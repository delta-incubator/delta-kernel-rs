//! Represents a segment of a delta log

use crate::expressions::column_expr;
use std::sync::{Arc, LazyLock};
use url::Url;

use crate::{
    actions::{get_log_schema, Metadata, Protocol, METADATA_NAME, PROTOCOL_NAME},
    schema::SchemaRef,
    DeltaResult, Engine, EngineData, Error, ExpressionRef, FileMeta,
};
use itertools::Itertools;

#[derive(Debug)]
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
#[cfg_attr(not(feature = "developer-visibility"), visibility::make(pub(crate)))]
pub(crate) struct LogSegment {
    log_root: Url,
    /// Reverse order sorted commit files in the log segment
    commit_files: Vec<FileMeta>,
    /// checkpoint files in the log segment.
    checkpoint_files: Vec<FileMeta>,
}

impl LogSegment {
    pub(crate) fn new(
        log_root: Url,
        commit_files: Vec<FileMeta>,
        checkpoint_files: Vec<FileMeta>,
    ) -> Self {
        LogSegment {
            log_root,
            commit_files,
            checkpoint_files,
        }
    }
    pub(crate) fn log_root(&self) -> &Url {
        &self.log_root
    }
    /// Read a stream of log data from this log segment.
    ///
    /// The log files will be read from most recent to oldest.
    /// The boolean flags indicates whether the data was read from
    /// a commit file (true) or a checkpoint file (false).
    ///
    /// `read_schema` is the schema to read the log files with. This can be used
    /// to project the log files to a subset of the columns.
    ///
    /// `meta_predicate` is an optional expression to filter the log files with. It is _NOT_ the
    /// query's predicate, but rather a predicate for filtering log files themselves.
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    #[cfg_attr(not(feature = "developer-visibility"), visibility::make(pub(crate)))]
    pub fn replay(
        &self,
        engine: &dyn Engine,
        commit_read_schema: SchemaRef,
        checkpoint_read_schema: SchemaRef,
        meta_predicate: Option<ExpressionRef>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send> {
        let json_client = engine.get_json_handler();
        let commit_stream = json_client
            .read_json_files(
                &self.commit_files,
                commit_read_schema,
                meta_predicate.clone(),
            )?
            .map_ok(|batch| (batch, true));

        let parquet_client = engine.get_parquet_handler();
        let checkpoint_stream = parquet_client
            .read_parquet_files(
                &self.checkpoint_files,
                checkpoint_read_schema,
                meta_predicate,
            )?
            .map_ok(|batch| (batch, false));

        let batches = commit_stream.chain(checkpoint_stream);

        Ok(batches)
    }

    pub(crate) fn read_metadata(&self, engine: &dyn Engine) -> DeltaResult<(Metadata, Protocol)> {
        let data_batches = self.replay_for_metadata(engine)?;
        let mut metadata_opt: Option<Metadata> = None;
        let mut protocol_opt: Option<Protocol> = None;
        for batch in data_batches {
            let (batch, _) = batch?;
            if metadata_opt.is_none() {
                metadata_opt = crate::actions::Metadata::try_new_from_data(batch.as_ref())?;
            }
            if protocol_opt.is_none() {
                protocol_opt = crate::actions::Protocol::try_new_from_data(batch.as_ref())?;
            }
            if metadata_opt.is_some() && protocol_opt.is_some() {
                // we've found both, we can stop
                break;
            }
        }
        match (metadata_opt, protocol_opt) {
            (Some(m), Some(p)) => Ok((m, p)),
            (None, Some(_)) => Err(Error::MissingMetadata),
            (Some(_), None) => Err(Error::MissingProtocol),
            _ => Err(Error::MissingMetadataAndProtocol),
        }
    }

    // Factored out to facilitate testing
    pub(crate) fn replay_for_metadata(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send> {
        let schema = get_log_schema().project(&[PROTOCOL_NAME, METADATA_NAME])?;
        // filter out log files that do not contain metadata or protocol information
        use crate::Expression as Expr;
        static META_PREDICATE: LazyLock<Option<ExpressionRef>> = LazyLock::new(|| {
            Some(Arc::new(Expr::or(
                column_expr!("metaData.id").is_not_null(),
                column_expr!("protocol.minReaderVersion").is_not_null(),
            )))
        });
        // read the same protocol and metadata schema for both commits and checkpoints
        self.replay(engine, schema.clone(), schema, META_PREDICATE.clone())
    }
}

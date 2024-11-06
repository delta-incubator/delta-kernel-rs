//! Represents a segment of a delta log. [`LogSegment`] wraps a set of  checkpoint and commit
//! files.

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
pub(crate) struct LogSegment {
    pub log_root: Url,
    /// Reverse order sorted commit files in the log segment
    pub commit_files: Vec<FileMeta>,
    /// checkpoint files in the log segment.
    pub checkpoint_files: Vec<FileMeta>,
}

impl LogSegment {
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
    pub(crate) fn replay(
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

        Ok(commit_stream.chain(checkpoint_stream))
    }

    // Get the most up-to-date Protocol and Metadata actions
    pub(crate) fn read_metadata(&self, engine: &dyn Engine) -> DeltaResult<(Metadata, Protocol)> {
        let data_batches = self.replay_for_metadata(engine)?;
        let mut metadata_opt = None;
        let mut protocol_opt = None;
        for batch in data_batches {
            let (batch, _) = batch?;
            if metadata_opt.is_none() {
                metadata_opt = Metadata::try_new_from_data(batch.as_ref())?;
            }
            if protocol_opt.is_none() {
                protocol_opt = Protocol::try_new_from_data(batch.as_ref())?;
            }
            if let (Some(m), Some(p)) = (metadata_opt, protocol_opt) {
                return Ok((m, p))
            }
        }
        match (metadata_opt, protocol_opt) {
            (_, Some(_)) => Err(Error::MissingMetadata),
            (Some(_), _) => Err(Error::MissingProtocol),
            _ => Err(Error::MissingMetadataAndProtocol),
        }
    }

    // Replay the commit log, projecting rows to only contain Protocol and Metadata action columns.
    fn replay_for_metadata(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send> {
        let schema = get_log_schema().project(&[PROTOCOL_NAME, METADATA_NAME])?;
        // filter out log files that do not contain metadata or protocol information
        use crate::Expression as Expr;
        static META_PREDICATE: LazyLock<Option<ExpressionRef>> = LazyLock::new(|| {
            Some(Arc::new(Expr::or(
                Expr::column([METADATA_NAME, "id"]).is_not_null(),
                Expr::column([PROTOCOL_NAME, "minReaderVersion"]).is_not_null(),
            )))
        });
        // read the same protocol and metadata schema for both commits and checkpoints
        self.replay(engine, schema.clone(), schema, META_PREDICATE.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use itertools::Itertools;

    use crate::{engine::sync::SyncEngine, Table};

    // NOTE: In addition to testing the meta-predicate for metadata replay, this test also verifies
    // that the parquet reader properly infers nullcount = rowcount for missing columns. The two
    // checkpoint part files that contain transaction app ids have truncated schemas that would
    // otherwise fail skipping due to their missing nullcount stat:
    //
    // Row group 0:  count: 1  total(compressed): 111 B total(uncompressed):107 B
    // --------------------------------------------------------------------------------
    //              type    nulls  min / max
    // txn.appId    BINARY  0      "3ae45b72-24e1-865a-a211-3..." / "3ae45b72-24e1-865a-a211-3..."
    // txn.version  INT64   0      "4390" / "4390"
    #[test]
    fn test_replay_for_metadata() {
        let path = std::fs::canonicalize(PathBuf::from("./tests/data/parquet_row_group_skipping/"));
        let url = url::Url::from_directory_path(path.unwrap()).unwrap();
        let engine = SyncEngine::new();

        let table = Table::new(url);
        let snapshot = table.snapshot(&engine, None).unwrap();
        let data: Vec<_> = snapshot
            .log_segment
            .replay_for_metadata(&engine)
            .unwrap()
            .try_collect()
            .unwrap();

        // The checkpoint has five parts, each containing one action:
        // 1. txn (physically missing P&M columns)
        // 2. metaData
        // 3. protocol
        // 4. add
        // 5. txn (physically missing P&M columns)
        //
        // The parquet reader should skip parts 1, 3, and 5. Note that the actual `read_metadata`
        // always skips parts 4 and 5 because it terminates the iteration after finding both P&M.
        //
        // NOTE: Each checkpoint part is a single-row file -- guaranteed to produce one row group.
        //
        // WARNING: https://github.com/delta-incubator/delta-kernel-rs/issues/434 -- We currently
        // read parts 1 and 5 (4 in all instead of 2) because row group skipping is disabled for
        // missing columns, but can still skip part 3 because has valid nullcount stats for P&M.
        assert_eq!(data.len(), 4);
    }
}

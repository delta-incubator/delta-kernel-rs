//! Represents a segment of a delta log. [`LogSegment`] wraps a set of  checkpoint and commit
//! files.

use crate::actions::{get_log_schema, Metadata, Protocol, METADATA_NAME, PROTOCOL_NAME};
use crate::path::ParsedLogPath;
use crate::schema::SchemaRef;
use crate::snapshot::CheckpointMetadata;
use crate::utils::require;
use crate::{
    DeltaResult, Engine, EngineData, Error, Expression, ExpressionRef, FileMeta, FileSystemClient,
    Version,
};
use itertools::Itertools;
use std::cmp::Ordering;
use std::sync::{Arc, LazyLock};
use tracing::warn;
use url::Url;

#[derive(Debug)]
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
pub(crate) struct LogSegment {
    pub end_version: Version,
    pub log_root: Url,
    /// Commit files in the log segment
    pub commit_files: Vec<ParsedLogPath>,
    /// Checkpoint files in the log segment.
    pub checkpoint_files: Vec<ParsedLogPath>,
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
    pub(crate) fn replay(
        &self,
        engine: &dyn Engine,
        commit_read_schema: SchemaRef,
        checkpoint_read_schema: SchemaRef,
        meta_predicate: Option<ExpressionRef>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send> {
        let commit_files: Vec<_> = self
            .commit_files
            .iter()
            .map(|f| f.location.clone())
            .collect();
        let commit_stream = engine
            .get_json_handler()
            .read_json_files(&commit_files, commit_read_schema, meta_predicate.clone())?
            .map_ok(|batch| (batch, true));

        let checkpoint_files: Vec<_> = self
            .checkpoint_files
            .iter()
            .map(|f| f.location.clone())
            .collect();
        let checkpoint_stream = engine
            .get_parquet_handler()
            .read_parquet_files(&checkpoint_files, checkpoint_read_schema, meta_predicate)?
            .map_ok(|batch| (batch, false));

        Ok(commit_stream.chain(checkpoint_stream))
    }

    // Get the most up-to-date Protocol and Metadata actions
    pub(crate) fn read_metadata(&self, engine: &dyn Engine) -> DeltaResult<(Metadata, Protocol)> {
        let data_batches = self.replay_for_metadata(engine)?;
        let (mut metadata_opt, mut protocol_opt) = (None, None);
        for batch in data_batches {
            let (batch, _) = batch?;
            if metadata_opt.is_none() {
                metadata_opt = Metadata::try_new_from_data(batch.as_ref())?;
            }
            if protocol_opt.is_none() {
                protocol_opt = Protocol::try_new_from_data(batch.as_ref())?;
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
            (None, None) => Err(Error::MissingMetadataAndProtocol),
        }
    }

    // Replay the commit log, projecting rows to only contain Protocol and Metadata action columns.
    fn replay_for_metadata(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send> {
        let schema = get_log_schema().project(&[PROTOCOL_NAME, METADATA_NAME])?;
        // filter out log files that do not contain metadata or protocol information
        static META_PREDICATE: LazyLock<Option<ExpressionRef>> = LazyLock::new(|| {
            Some(Arc::new(Expression::or(
                Expression::column([METADATA_NAME, "id"]).is_not_null(),
                Expression::column([PROTOCOL_NAME, "minReaderVersion"]).is_not_null(),
            )))
        });
        // read the same protocol and metadata schema for both commits and checkpoints
        self.replay(engine, schema.clone(), schema, META_PREDICATE.clone())
    }
}

/// Builder for [`LogSegment`] from from `start_version` to `end_version` inclusive
pub(crate) struct LogSegmentBuilder<'a> {
    fs_client: &'a dyn FileSystemClient,
    table_root: &'a Url,
    checkpoint: Option<CheckpointMetadata>,
    start_version: Option<Version>,
    end_version: Option<Version>,
    reversed_commit_files: bool,
}
impl<'a> LogSegmentBuilder<'a> {
    pub(crate) fn new(fs_client: &'a dyn FileSystemClient, table_root: &'a Url) -> Self {
        LogSegmentBuilder {
            fs_client,
            table_root,
            checkpoint: None,
            start_version: None,
            end_version: None,
            reversed_commit_files: false,
        }
    }
    /// Optionally provide a checkpoint hint that results from reading the `last_checkpoint` file.
    pub(crate) fn with_checkpoint(mut self, checkpoint: CheckpointMetadata) -> Self {
        self.checkpoint = Some(checkpoint);
        self
    }

    /// Optionally set the start version of the [`LogSegment`]. This ensures that all commit files
    /// are above this version. Checkpoint files will be omitted if this is specified.
    #[allow(unused)]
    pub(crate) fn with_start_version(mut self, version: Version) -> Self {
        self.start_version = Some(version);
        self
    }
    /// Optionally set the end version of the [`LogSegment`]. This ensures that all commit files
    /// and checkpoints are below the end version.
    pub(crate) fn with_end_version(mut self, version: Version) -> Self {
        self.end_version = Some(version);
        self
    }
    /// Optionally specify that the commits in the [`LogSegment`] will be in order. By default, the
    /// [`LogSegment`] will reverse the order of commit files, with the latest commit coming first.
    #[allow(unused)]
    pub(crate) fn with_reversed_commit_files(mut self) -> Self {
        self.reversed_commit_files = true;
        self
    }
    /// Build the [`LogSegment`]
    ///
    /// This fetches checkpoint and commit files using the `fs_client`.
    pub(crate) fn build(self) -> DeltaResult<LogSegment> {
        let Self {
            fs_client,
            table_root,
            checkpoint,
            start_version,
            end_version,
            reversed_commit_files,
        } = self;
        let log_root = table_root.join("_delta_log/").unwrap();
        let (mut commit_files, mut checkpoint_files) = match (checkpoint, end_version) {
            (Some(cp), None) => Self::list_log_files_with_checkpoint(&cp, fs_client, &log_root)?,
            (Some(cp), Some(version)) if cp.version >= version => {
                Self::list_log_files_with_checkpoint(&cp, fs_client, &log_root)?
            }
            _ => {
                let (commit_files, checkpoint_files, _) =
                    Self::list_log_files_from_version(fs_client, &log_root, None)?;

                (commit_files, checkpoint_files)
            }
        };

        // Commit file versions must satisfy the following:
        // - Be greater than the start version
        // - Be greater than the most recent checkpoint version if it exists
        // - Be less than or equal to the end version.
        if let Some(start_version) = start_version {
            checkpoint_files.clear();
            commit_files.retain(|log_path| start_version <= log_path.version);
        }
        if let Some(checkpoint_file) = checkpoint_files.first() {
            commit_files.retain(|log_path| checkpoint_file.version < log_path.version);
        }
        if let Some(end_version) = end_version {
            commit_files.retain(|log_path| log_path.version <= end_version);
        }

        // get the effective version from chosen files
        let version_eff = commit_files
            .last()
            .or(checkpoint_files.first())
            .ok_or(Error::MissingVersion)? // TODO: A more descriptive error
            .version;
        if let Some(end_version) = end_version {
            require!(
                version_eff == end_version,
                Error::MissingVersion // TODO more descriptive error
            );
        }

        // We assume listing returned ordered. If `in_order_commit_files` is false, we want reverse order.
        if reversed_commit_files {
            commit_files.reverse();
        }

        Ok(LogSegment {
            end_version: version_eff,
            log_root,
            commit_files,
            checkpoint_files,
        })
    }
    pub(crate) fn list_log_files_from_version(
        fs_client: &dyn FileSystemClient,
        log_root: &Url,
        version: Option<Version>,
    ) -> DeltaResult<(Vec<ParsedLogPath>, Vec<ParsedLogPath>, i64)> {
        let begin_version = version.unwrap_or(0);
        let version_prefix = format!("{:020}", begin_version);
        let start_from = log_root.join(&version_prefix)?;

        let mut max_checkpoint_version = version.map_or(-1, |x| x as i64);
        let mut checkpoint_files = vec![];
        // We expect 10 commit files per checkpoint, so start with that size. We could adjust this based
        // on config at some point
        let mut commit_files = Vec::with_capacity(10);

        for meta_res in fs_client.list_from(&start_from)? {
            let meta = meta_res?;
            let parsed_path = ParsedLogPath::try_from(meta)?;
            // TODO this filters out .crc files etc which start with "." - how do we want to use these kind of files?
            if let Some(parsed_path) = parsed_path {
                if parsed_path.is_commit() {
                    commit_files.push(parsed_path);
                } else if parsed_path.is_checkpoint() {
                    let path_version = parsed_path.version as i64;
                    match path_version.cmp(&max_checkpoint_version) {
                        Ordering::Greater => {
                            max_checkpoint_version = path_version;
                            checkpoint_files.clear();
                            checkpoint_files.push(parsed_path);
                        }
                        Ordering::Equal => checkpoint_files.push(parsed_path),
                        Ordering::Less => {}
                    }
                }
            }
        }

        debug_assert!(
            commit_files
                .windows(2)
                .all(|cfs| cfs[0].version <= cfs[1].version),
            "fs_client.list_from() didn't return a sorted listing! {:?}",
            commit_files
        );

        Ok((commit_files, checkpoint_files, max_checkpoint_version))
    }

    /// List all log files after a given checkpoint.
    pub(crate) fn list_log_files_with_checkpoint(
        checkpoint_metadata: &CheckpointMetadata,
        fs_client: &dyn FileSystemClient,
        log_root: &Url,
    ) -> DeltaResult<(Vec<ParsedLogPath>, Vec<ParsedLogPath>)> {
        let (commit_files, checkpoint_files, max_checkpoint_version) =
            Self::list_log_files_from_version(
                fs_client,
                log_root,
                Some(checkpoint_metadata.version),
            )?;

        if checkpoint_files.is_empty() {
            // TODO: We could potentially recover here
            return Err(Error::generic(
                "Had a _last_checkpoint hint but didn't find any checkpoints",
            ));
        }

        if max_checkpoint_version != checkpoint_metadata.version as i64 {
            warn!(
                "_last_checkpoint hint is out of date. _last_checkpoint version: {}. Using actual most recent: {}",
                checkpoint_metadata.version,
                max_checkpoint_version
            );
        } else if checkpoint_files.len() != checkpoint_metadata.parts.unwrap_or(1) {
            return Err(Error::Generic(format!(
                "_last_checkpoint indicated that checkpoint should have {} parts, but it has {}",
                checkpoint_metadata.parts.unwrap_or(1),
                checkpoint_files.len()
            )));
        }
        Ok((commit_files, checkpoint_files))
    }
}
#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc};

    use itertools::Itertools;
    use object_store::{memory::InMemory, path::Path, ObjectStore};
    use url::Url;

    use crate::{
        engine::{
            default::{
                executor::tokio::TokioBackgroundExecutor, filesystem::ObjectStoreFileSystemClient,
            },
            sync::SyncEngine,
        },
        log_segment::LogSegmentBuilder,
        snapshot::CheckpointMetadata,
        Table,
    };

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

    #[test]
    fn test_read_log_with_out_of_date_last_checkpoint() {
        let store = Arc::new(InMemory::new());

        fn get_path(index: usize, suffix: &str) -> Path {
            let path = format!("_delta_log/{index:020}.{suffix}");
            Path::from(path.as_str())
        }
        let data = bytes::Bytes::from("kernel-data");

        let checkpoint_metadata = CheckpointMetadata {
            version: 3,
            size: 10,
            parts: None,
            size_in_bytes: None,
            num_of_add_files: None,
            checkpoint_schema: None,
            checksum: None,
        };

        // add log files to store
        tokio::runtime::Runtime::new()
            .expect("create tokio runtime")
            .block_on(async {
                for path in [
                    get_path(0, "json"),
                    get_path(1, "checkpoint.parquet"),
                    get_path(2, "json"),
                    get_path(3, "checkpoint.parquet"),
                    get_path(4, "json"),
                    get_path(5, "checkpoint.parquet"),
                    get_path(6, "json"),
                    get_path(7, "json"),
                ] {
                    store
                        .put(&path, data.clone().into())
                        .await
                        .expect("put log file in store");
                }
                let checkpoint_str =
                    serde_json::to_string(&checkpoint_metadata).expect("Serialize checkpoint");
                store
                    .put(
                        &Path::from("_delta_log/_last_checkpoint"),
                        checkpoint_str.into(),
                    )
                    .await
                    .expect("Write _last_checkpoint");
            });

        let client = ObjectStoreFileSystemClient::new(
            store,
            false, // don't have ordered listing
            Path::from("/"),
            Arc::new(TokioBackgroundExecutor::new()),
        );

        let table_root = Url::parse("memory:///").expect("valid url");

        let log_segment = LogSegmentBuilder::new(&client, &table_root)
            .with_reversed_commit_files()
            .with_checkpoint(checkpoint_metadata)
            .build()
            .unwrap();
        let (commit_files, checkpoint_files) =
            (log_segment.commit_files, log_segment.checkpoint_files);

        assert_eq!(checkpoint_files.len(), 1);
        println!("checkpoint: {:?}", checkpoint_files);
        println!("commits: {:?}", commit_files);
        assert_eq!(commit_files.len(), 2);
        assert_eq!(checkpoint_files[0].version, 5);
        println!("commitfiles: {:?}", commit_files);
        assert_eq!(commit_files[0].version, 7);
        assert_eq!(commit_files[1].version, 6);
    }
}

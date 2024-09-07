//! In-memory representation of snapshots of tables (snapshot is a table at given point in time, it
//! has schema etc.)
//!

use std::cmp::Ordering;
use std::sync::Arc;

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};
use url::Url;

use crate::actions::{get_log_schema, Metadata, Protocol, METADATA_NAME, PROTOCOL_NAME};
use crate::features::{ColumnMappingMode, COLUMN_MAPPING_MODE_KEY};
use crate::path::{version_from_location, LogPath};
use crate::scan::ScanBuilder;
use crate::schema::{Schema, SchemaRef};
use crate::utils::require;
use crate::{DeltaResult, Engine, Error, FileMeta, FileSystemClient, Version};
use crate::{EngineData, Expression};

const LAST_CHECKPOINT_FILE_NAME: &str = "_last_checkpoint";

#[derive(Debug)]
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
#[cfg_attr(not(feature = "developer-visibility"), visibility::make(pub(crate)))]
struct LogSegment {
    log_root: Url,
    /// Reverse order sorted commit files in the log segment
    pub(crate) commit_files: Vec<FileMeta>,
    /// checkpoint files in the log segment.
    pub(crate) checkpoint_files: Vec<FileMeta>,
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
    /// `predicate` is an optional expression to filter the log files with.
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    #[cfg_attr(not(feature = "developer-visibility"), visibility::make(pub(crate)))]
    fn replay(
        &self,
        engine: &dyn Engine,
        commit_read_schema: SchemaRef,
        checkpoint_read_schema: SchemaRef,
        predicate: Option<Expression>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send> {
        let json_client = engine.get_json_handler();
        // TODO change predicate to: predicate AND add.path not null and remove.path not null
        let commit_stream = json_client
            .read_json_files(&self.commit_files, commit_read_schema, predicate.clone())?
            .map_ok(|batch| (batch, true));

        let parquet_client = engine.get_parquet_handler();
        // TODO change predicate to: predicate AND add.path not null
        let checkpoint_stream = parquet_client
            .read_parquet_files(&self.checkpoint_files, checkpoint_read_schema, predicate)?
            .map_ok(|batch| (batch, false));

        let batches = commit_stream.chain(checkpoint_stream);

        Ok(batches)
    }

    fn read_metadata(&self, engine: &dyn Engine) -> DeltaResult<Option<(Metadata, Protocol)>> {
        let schema = get_log_schema().project(&[PROTOCOL_NAME, METADATA_NAME])?;
        // read the same protocol and metadata schema for both commits and checkpoints
        // TODO add metadata.table_id is not null and protocol.something_required is not null
        let data_batches = self.replay(engine, schema.clone(), schema, None)?;
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
            (Some(m), Some(p)) => Ok(Some((m, p))),
            (None, Some(_)) => Err(Error::MissingMetadata),
            (Some(_), None) => Err(Error::MissingProtocol),
            _ => Err(Error::MissingMetadataAndProtocol),
        }
    }
}

// TODO expose methods for accessing the files of a table (with file pruning).
/// In-memory representation of a specific snapshot of a Delta table. While a `DeltaTable` exists
/// throughout time, `Snapshot`s represent a view of a table at a specific point in time; they
/// have a defined schema (which may change over time for any given table), specific version, and
/// frozen log segment.

pub struct Snapshot {
    pub(crate) table_root: Url,
    pub(crate) log_segment: LogSegment,
    version: Version,
    metadata: Metadata,
    protocol: Protocol,
    schema: Schema,
    pub(crate) column_mapping_mode: ColumnMappingMode,
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        debug!("Dropping snapshot");
    }
}

impl std::fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshot")
            .field("path", &self.log_segment.log_root.as_str())
            .field("version", &self.version)
            .field("metadata", &self.metadata)
            .finish()
    }
}

/*
Reasoning and Example:

Consider a Delta table with the following log structure:
Version | File
--------|---------------------
0       | 00000.json
1       | 00001.json
2       | 00002.json
3       | 00003.json
4       | 00004.checkpoint.parquet
4       | 00004.json
5       | 00005.json
6       | 00006.json
7       | 00007.json
8       | 00008.json
9       | 00009.checkpoint.parquet
9       | 00009.json

1. If requested_version is None (latest version):
   - We include all commit files after the last checkpoint (4).
   - Result: [9, 8, 7, 6, 5]

2. If requested_version is 6:
   - We include commit files after the last checkpoint (4) up to and including 6.
   - Result: [6, 5]

3. If requested_version is 4 (same as checkpoint):
   - We only include the commit file at version 4.
   - Result: [4]

4. If requested_version is 3 (before the checkpoint):
   - We include all commit files up to and including 3.
   - Result: [3, 2, 1, 0]

The logic ensures that:
a) We never include commit files beyond the requested version.
b) We use the most recent checkpoint as a starting point for efficiency.
c) We include the checkpoint version file only if it's explicitly requested.
d) We sort the files in reverse order for proper replay of the log.

This approach optimizes log replay by leveraging checkpoints while ensuring
correct behavior for all possible requested versions.
*/
impl Snapshot {
    /// Create a new [`Snapshot`] instance for the given version.
    ///
    /// # Parameters
    ///
    /// - `location`: url pointing at the table root (where `_delta_log` folder is located)
    /// - `engine`: Implementation of [`Engine`] apis.
    /// - `version`: target version of the [`Snapshot`]
    pub fn try_new(
        table_root: Url,
        engine: &dyn Engine,
        version: Option<Version>,
    ) -> DeltaResult<Self> {
        let fs_client = engine.get_file_system_client();
        let log_url = LogPath::new(&table_root).child("_delta_log/").unwrap();

        // List relevant files from log
        let (commit_files, checkpoint_files) =
            list_log_files(fs_client.as_ref(), &log_url, version)?;

        // print the commit_files and checkpoint_files
        println!("\n\ncommit_files_try_new: {:?}", commit_files);
        println!("checkpoint_files_try_new: {:?}\n\n", checkpoint_files);

        // get the effective version from chosen files
        let version_eff = commit_files
            .first()
            .or(checkpoint_files.first())
            .and_then(|f| LogPath::new(&f.location).version)
            .ok_or(Error::MissingVersion)?; // TODO: A more descriptive error

        if let Some(v) = version {
            require!(
                version_eff == v,
                Error::MissingVersion // TODO more descriptive error
            );
        }

        let log_segment = LogSegment {
            log_root: log_url,
            commit_files,
            checkpoint_files,
        };

        Self::try_new_from_log_segment(table_root, log_segment, version_eff, engine)
    }

    /// Create a new [`Snapshot`] instance.
    pub(crate) fn try_new_from_log_segment(
        location: Url,
        log_segment: LogSegment,
        version: Version,
        engine: &dyn Engine,
    ) -> DeltaResult<Self> {
        let (metadata, protocol) = log_segment
            .read_metadata(engine)?
            .ok_or(Error::MissingMetadata)?;
        let schema = metadata.schema()?;
        let column_mapping_mode = match metadata.configuration.get(COLUMN_MAPPING_MODE_KEY) {
            Some(mode) if protocol.min_reader_version >= 2 => mode.as_str().try_into(),
            _ => Ok(ColumnMappingMode::None),
        }?;
        Ok(Self {
            table_root: location,
            log_segment,
            version,
            metadata,
            protocol,
            schema,
            column_mapping_mode,
        })
    }

    /// Log segment this snapshot uses
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    fn _log_segment(&self) -> &LogSegment {
        &self.log_segment
    }

    pub fn table_root(&self) -> &Url {
        &self.table_root
    }

    /// Version of this `Snapshot` in the table.
    pub fn version(&self) -> Version {
        self.version
    }

    /// Table [`Schema`] at this `Snapshot`s version.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Table [`Metadata`] at this `Snapshot`s version.
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Table [`Protocol`] at this `Snapshot`s version.
    pub fn protocol(&self) -> &Protocol {
        &self.protocol
    }

    /// Get the [column mapping
    /// mode](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#column-mapping) at this
    /// `Snapshot`s version.
    pub fn column_mapping_mode(&self) -> ColumnMappingMode {
        self.column_mapping_mode
    }

    /// Create a [`ScanBuilder`] for an `Arc<Snapshot>`.
    pub fn scan_builder(self: Arc<Self>) -> ScanBuilder {
        ScanBuilder::new(self)
    }

    /// Consume this `Snapshot` to create a [`ScanBuilder`]
    pub fn into_scan_builder(self) -> ScanBuilder {
        ScanBuilder::new(self)
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
#[cfg_attr(not(feature = "developer-visibility"), visibility::make(pub(crate)))]
struct CheckpointMetadata {
    /// The version of the table when the last checkpoint was made.
    #[allow(unreachable_pub)] // used by acceptance tests (TODO make an fn accessor?)
    pub version: Version,
    /// The number of actions that are stored in the checkpoint.
    pub(crate) size: i64,
    /// The number of fragments if the last checkpoint was written in multiple parts.
    pub(crate) parts: Option<i32>,
    /// The number of bytes of the checkpoint.
    /// TODO: Temporary fix, checkout this issue for full details: https://github.com/delta-incubator/delta-kernel-rs/issues/326
    #[serde(alias = "size_in_bytes")]
    pub(crate) size_in_bytes: Option<i64>,
    /// The number of AddFile actions in the checkpoint.
    pub(crate) num_of_add_files: Option<i64>,
    /// The schema of the checkpoint file.
    pub(crate) checkpoint_schema: Option<Schema>,
    /// The checksum of the last checkpoint JSON.
    pub(crate) checksum: Option<String>,
}

/// Try reading the `_last_checkpoint` file.
///
/// Note that we typically want to ignore a missing/invalid `_last_checkpoint` file without failing
/// the read. Thus, the semantics of this function are to return `None` if the file is not found or
/// is invalid JSON. Unexpected/unrecoverable errors are returned as `Err` case and are assumed to
/// cause failure.
///
/// TODO: java kernel retries three times before failing, should we do the same?
fn read_last_checkpoint(
    fs_client: &dyn FileSystemClient,
    log_root: &Url,
) -> DeltaResult<Option<CheckpointMetadata>> {
    let file_path = LogPath::new(log_root).child(LAST_CHECKPOINT_FILE_NAME)?;
    debug!("Reading last checkpoint from: {}", file_path);
    match fs_client
        .read_files(vec![(file_path, None)])
        .and_then(|mut data| data.next().expect("read_files should return one file"))
    {
        Ok(data) => {
            // print the data in bytes as a string
            debug!("Data: {:?}", std::str::from_utf8(&data).unwrap());
            Ok(serde_json::from_slice(&data)
                .inspect_err(|e| warn!("invalid _last_checkpoint JSON: {e}"))
                .ok())
        }
        Err(Error::FileNotFound(_)) => Ok(None),
        Err(err) => Err(err),
    }
}

/// List relevant log files.
///
/// Relevant files are the max checkpoint found and all subsequent commits.
/// If a checkpoint is provided, it starts listing from there; otherwise, it starts from the beginning.
fn list_log_files(
    fs_client: &dyn FileSystemClient,
    log_root: &Url,
    requested_version: Option<Version>,
) -> DeltaResult<(Vec<FileMeta>, Vec<FileMeta>)> {
    let start_from = log_root.join(&format!("{:020}", 0))?;

    let mut commit_files = Vec::new();
    let mut checkpoint_files = Vec::new();
    let mut last_checkpoint_version = None;
    let mut next_checkpoint_version = None;

    // If no requested version, try to get the last checkpoint
    let last_checkpoint = if requested_version.is_none() {
        read_last_checkpoint(fs_client, log_root)?
    } else {
        None
    };

    // Iterate through files in the log directory
    for maybe_meta in fs_client.list_from(&start_from)? {
        let meta = maybe_meta?;
        let log_path = LogPath::new(&meta.location);
        let file_version = log_path.version.map(|v| v as i64).unwrap_or(0);

        // Stop if we've passed the requested version
        if let Some(req_version) = requested_version {
            if file_version > req_version as i64 {
                break;
            }
        }

        if log_path.is_checkpoint {
            if last_checkpoint_version.is_none() || file_version > last_checkpoint_version.unwrap()
            {
                last_checkpoint_version = Some(file_version);
                checkpoint_files.clear();
                checkpoint_files.push(meta.clone());
            }

            // If we're using last_checkpoint and we've found the next checkpoint, stop
            if requested_version.is_none()
                && last_checkpoint.is_some()
                && file_version > last_checkpoint.as_ref().unwrap().version as i64
            {
                next_checkpoint_version = Some(file_version);
                break;
            }
        } else if log_path.is_commit {
            commit_files.push(meta);
        }
    }

    println!(
        "\n\n\nlast_checkpoint_version: {:?}",
        last_checkpoint_version
    );
    println!("next_checkpoint_version: {:?}", next_checkpoint_version);
    println!("requested_version: {:?}", requested_version);
    println!("checkpoint_files: {:?}", checkpoint_files);
    println!("commit_files: {:?}\n\n\n", commit_files);

    // Filter commit files based on the last checkpoint version and requested version
    commit_files.retain(|f| {
        let version = LogPath::new(&f.location)
            .version
            .map(|v| v as i64)
            .unwrap_or(0);
        match (last_checkpoint_version, requested_version) {
            // Case: Checkpoint and requested version are the same
            // Occurs when: A specific version is requested that matches a checkpoint
            // Example: last_checkpoint_version = Some(10), requested_version = Some(10)
            // Keep only commit files with version 10
            (Some(lcv), Some(rv)) if lcv == rv as i64 => version == lcv,

            // Case: Normal range between checkpoint and requested version
            // Occurs when: A specific version is requested that's after a known checkpoint
            // Example: last_checkpoint_version = Some(10), requested_version = Some(15)
            // Keep commit files with versions 11, 12, 13, 14, 15
            (Some(lcv), Some(rv)) => version > lcv && version <= rv as i64,

            // Case: No requested version, using last known checkpoint
            // Occurs when: No version is requested, we use read_last_checkpoint result
            // Note: A new checkpoint might have been created during file listing
            // Example: last_checkpoint_version = Some(10), next_checkpoint_version = Some(20)
            // Keep commit files with versions 11, 12, ..., 19
            (Some(lcv), None) => {
                version > lcv && next_checkpoint_version.map_or(true, |ncv| version < ncv)
            }

            // Case: No last checkpoint, but we have a requested version
            // Occurs when: A specific version is requested, but no checkpoint was found before it
            // Example: requested_version = Some(15), no checkpoints found
            // Keep all commit files up to and including version 15
            (None, Some(rv)) => version <= rv as i64,

            // Case: No last checkpoint and no requested version
            // Occurs when: No version is requested and no checkpoints were found
            // Keep all commit files (this case should be rare in practice)
            (None, None) => true,
        }
    });

    // Sort commit files in reverse order (newest first)
    commit_files.sort_unstable_by(|a, b| b.location.cmp(&a.location));

    Ok((commit_files, checkpoint_files))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;
    use std::sync::Arc;
    use lazy_static::lazy_static;

    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;

    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
    use crate::engine::default::filesystem::ObjectStoreFileSystemClient;
    use crate::engine::sync::SyncEngine;
    use crate::schema::StructType;

    // Define a type for the test case elements
    type TestCase = (
        Version,         // expected<RequestedVersion>
        Version,         // expected<ActualVersion>
        Option<Version>, // expected<CheckpointVersion>
        Vec<Version>,    // expected<CommitFileVersions>
        Vec<Version>,    // expected<CheckpointFileVersions>
    );
    lazy_static! {
        static ref TEST_CASES: Vec<TestCase> = vec![
        // Version 0: No checkpoint, only the initial commit
        (0, 0, None, vec![0], vec![]),
        // Version 1-3: No checkpoint yet, accumulating commits
        (1, 1, None, vec![1, 0], vec![]),
        (2, 2, None, vec![2, 1, 0], vec![]),
        (3, 3, None, vec![3, 2, 1, 0], vec![]),
        // Version 4: First checkpoint, only includes its own commit
        (4, 4, Some(4), vec![4], vec![4]),
        // Version 5-8: After first checkpoint, accumulating new commits
        (5, 5, Some(4), vec![5], vec![4]),
        (6, 6, Some(4), vec![6, 5], vec![4]),
        (7, 7, Some(4), vec![7, 6, 5], vec![4]),
        (8, 8, Some(4), vec![8, 7, 6, 5], vec![4]),
        // Version 9: Second checkpoint, only includes its own commit
        (9, 9, Some(9), vec![9], vec![9]),
        // Version 10-13: After second checkpoint, accumulating new commits
        (10, 10, Some(9), vec![10], vec![9]),
        (11, 11, Some(9), vec![11, 10], vec![9]),
        (12, 12, Some(9), vec![12, 11, 10], vec![9]),
        (13, 13, Some(9), vec![13, 12, 11, 10], vec![9]),
        // Version 14: Third checkpoint, only includes its own commit
        (14, 14, Some(14), vec![14], vec![14]),
        // Version 15-18: After third checkpoint, accumulating new commits
        (15, 15, Some(14), vec![15], vec![14]),
        (16, 16, Some(14), vec![16, 15], vec![14]),
        (17, 17, Some(14), vec![17, 16, 15], vec![14]),
        (18, 18, Some(14), vec![18, 17, 16, 15], vec![14]),
        // Version 19: Fourth checkpoint, only includes its own commit
        (19, 19, Some(19), vec![19], vec![19]),
        // Version 20-23: After fourth checkpoint, accumulating new commits
        (20, 20, Some(19), vec![20], vec![19]),
        (21, 21, Some(19), vec![21, 20], vec![19]),
        (22, 22, Some(19), vec![22, 21, 20], vec![19]),
        (23, 23, Some(19), vec![23, 22, 21, 20], vec![19]),
        // Version 24: Fifth checkpoint, only includes its own commit
        (24, 24, Some(24), vec![24], vec![24]),
        // Version 25-28: After fifth checkpoint, accumulating new commits
        (25, 25, Some(24), vec![25], vec![24]),
        (26, 26, Some(24), vec![26, 25], vec![24]),
        (27, 27, Some(24), vec![27, 26, 25], vec![24]),
        (28, 28, Some(24), vec![28, 27, 26, 25], vec![24]),
    ];
    }

    #[test]
    fn test_snapshot_read_metadata() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let engine = SyncEngine::new();
        let snapshot = Snapshot::try_new(url, &engine, Some(1)).unwrap();

        let expected = Protocol {
            min_reader_version: 3,
            min_writer_version: 7,
            reader_features: Some(vec!["deletionVectors".into()]),
            writer_features: Some(vec!["deletionVectors".into()]),
        };
        assert_eq!(snapshot.protocol(), &expected);

        let schema_string = r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#;
        let expected: StructType = serde_json::from_str(schema_string).unwrap();
        assert_eq!(snapshot.schema(), &expected);
    }

    #[test]
    fn test_new_snapshot() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let engine = SyncEngine::new();
        let snapshot = Snapshot::try_new(url, &engine, None).unwrap();

        let expected = Protocol {
            min_reader_version: 3,
            min_writer_version: 7,
            reader_features: Some(vec!["deletionVectors".into()]),
            writer_features: Some(vec!["deletionVectors".into()]),
        };
        assert_eq!(snapshot.protocol(), &expected);

        let schema_string = r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#;
        let expected: StructType = serde_json::from_str(schema_string).unwrap();
        assert_eq!(snapshot.schema(), &expected);
    }

    #[test]
    fn test_read_table_with_last_checkpoint() {
        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/table-with-dv-small/_delta_log/",
        ))
        .unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let store = Arc::new(LocalFileSystem::new());
        let prefix = Path::from(url.path());
        let client = ObjectStoreFileSystemClient::new(
            store,
            prefix,
            Arc::new(TokioBackgroundExecutor::new()),
        );
        let cp = read_last_checkpoint(&client, &url).unwrap();
        assert!(cp.is_none())
    }

    fn valid_last_checkpoint() -> Vec<u8> {
        r#"{"size":8,"size_in_bytes":21857,"version":1}"#.as_bytes().to_vec()
    }

    #[test]
    fn test_read_table_with_invalid_last_checkpoint() {
        // in memory file system
        let store = Arc::new(InMemory::new());

        // put _last_checkpoint file
        let data = valid_last_checkpoint();
        let invalid_data = "invalid".as_bytes().to_vec();
        let path = Path::from("valid/_last_checkpoint");
        let invalid_path = Path::from("invalid/_last_checkpoint");

        tokio::runtime::Runtime::new()
            .expect("create tokio runtime")
            .block_on(async {
                store
                    .put(&path, data.into())
                    .await
                    .expect("put _last_checkpoint");
                store
                    .put(&invalid_path, invalid_data.into())
                    .await
                    .expect("put _last_checkpoint");
            });

        let client = ObjectStoreFileSystemClient::new(
            store,
            Path::from("/"),
            Arc::new(TokioBackgroundExecutor::new()),
        );
        let url = Url::parse("memory:///valid/").expect("valid url");
        let valid = read_last_checkpoint(&client, &url).expect("read last checkpoint");
        let url = Url::parse("memory:///invalid/").expect("valid url");
        let invalid = read_last_checkpoint(&client, &url).expect("read last checkpoint");
        assert!(valid.is_some());
        assert!(invalid.is_none())
    }

    #[test_log::test]
    fn test_read_table_with_checkpoint() {
        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/with_checkpoint_no_last_checkpoint/",
        ))
        .unwrap();
        let location = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();
        let snapshot = Snapshot::try_new(location, &engine, None).unwrap();

        assert_eq!(snapshot.log_segment.checkpoint_files.len(), 1);
        assert_eq!(
            LogPath::new(&snapshot.log_segment.checkpoint_files[0].location).version,
            Some(2)
        );
        assert_eq!(snapshot.log_segment.commit_files.len(), 1);
        assert_eq!(
            LogPath::new(&snapshot.log_segment.commit_files[0].location).version,
            Some(3)
        );
    }

    #[test]
    fn test_snapshot_version_0_with_checkpoint_at_version_1() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/app-txn-checkpoint/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let engine = SyncEngine::new();

        // First, let's verify the content of the _last_checkpoint file
        let fs_client = engine.get_file_system_client();
        let log_url = LogPath::new(&url).child("_delta_log/").unwrap();
        let last_checkpoint = read_last_checkpoint(fs_client.as_ref(), &log_url).unwrap();

        assert!(
            last_checkpoint.is_some(),
            "_last_checkpoint file should exist"
        );
        let checkpoint_meta = last_checkpoint.unwrap();
        debug!("Checkpoint metadata: {:#?}", checkpoint_meta);
        assert_eq!(
            checkpoint_meta.version, 1,
            "Last checkpoint should be at version 1"
        );
        assert_eq!(checkpoint_meta.size, 8, "Checkpoint size should be 8");
        assert_eq!(
            checkpoint_meta.size_in_bytes,
            Some(21857),
            "Checkpoint size in bytes should be 21857"
        );

        // Now, request snapshot at version 0
        let snapshot = Snapshot::try_new(url.clone(), &engine, Some(0));

        match snapshot {
            Ok(snap) => {
                assert_eq!(snap.version(), 0, "Snapshot version should be 0");

                // Verify that the snapshot contains the correct files
                assert_eq!(
                    snap.log_segment.commit_files.len(),
                    1,
                    "There should be one commit file"
                );
                assert_eq!(
                    LogPath::new(&snap.log_segment.commit_files[0].location).version,
                    Some(0),
                    "The commit file should be version 0"
                );

                assert!(
                    snap.log_segment.checkpoint_files.is_empty(),
                    "Snapshot for version 0 should not contain checkpoint files"
                );
            }
            Err(e) => {
                panic!("Failed to create snapshot for version 0: {:?}", e);
            }
        }

        // Verify the snapshot at version 1 (the checkpoint version)
        let snapshot_1 = Snapshot::try_new(url, &engine, Some(1)).unwrap();
        assert_eq!(snapshot_1.version(), 1, "Snapshot version should be 1");
        assert_eq!(
            snapshot_1.log_segment.checkpoint_files.len(),
            1,
            "There should be one checkpoint file for version 1"
        );
        assert_eq!(
            LogPath::new(&snapshot_1.log_segment.checkpoint_files[0].location).version,
            Some(1),
            "The checkpoint file should be version 1"
        );
    }

    #[test]
    fn test_snapshot_with_version_less_than_latest_checkpoint() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/multiple-checkpoint/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let engine = SyncEngine::new();

        // Attempt to create a snapshot at version 10
        let result = Snapshot::try_new(url, &engine, Some(10));

        // Check if the operation succeeded
        assert!(
            result.is_ok(),
            "Expected snapshot creation to succeed for version 10"
        );

        let snapshot = result.unwrap();

        // Verify the snapshot properties
        assert_eq!(snapshot.version(), 10, "Snapshot version should be 10");

        // Verify the checkpoint files
        let checkpoint_files = &snapshot.log_segment.checkpoint_files;
        assert_eq!(checkpoint_files.len(), 1, "Should have one checkpoint file");
        assert_eq!(
            LogPath::new(&checkpoint_files[0].location).version,
            Some(9),
            "Checkpoint should be version 9"
        );

        // Verify the commit files
        let commit_files = &snapshot.log_segment.commit_files;
        assert_eq!(commit_files.len(), 1, "Should have one commit file");

        let commit_version = LogPath::new(&commit_files[0].location).version.unwrap();
        assert_eq!(commit_version, 10, "Commit file should be version 10");

        // Verify that specific files are present
        let file_names: Vec<String> = checkpoint_files
            .iter()
            .chain(commit_files.iter())
            .map(|f| {
                Path::from(f.location.path())
                    .filename()
                    .unwrap()
                    .to_string()
            })
            .collect();

        assert!(
            file_names.contains(&"00000000000000000009.checkpoint.parquet".to_string()),
            "Checkpoint file 9 should be present"
        );
        assert!(
            file_names.contains(&"00000000000000000010.json".to_string()),
            "Commit file 10 should be present"
        );

        // Verify that specific files are not present
        assert!(
            !file_names.contains(&"00000000000000000009.json".to_string()),
            "Commit file 9 should not be present"
        );
        assert!(
            !file_names.contains(&"00000000000000000008.json".to_string()),
            "Commit file 8 should not be present"
        );
        assert!(
            !file_names.contains(&"00000000000000000011.json".to_string()),
            "Commit file 11 should not be present"
        );
        assert!(
            !file_names.contains(&"00000000000000000014.checkpoint.parquet".to_string()),
            "Checkpoint file 14 should not be present"
        );
    }

    #[test]
    fn test_snapshot_with_version_after_last_checkpoint() {
        // Setup
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/multiple-checkpoint/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();

        // Attempt to create a snapshot at version 26
        let result = Snapshot::try_new(url.clone(), &engine, Some(26));

        // Check if the operation succeeded
        assert!(
            result.is_ok(),
            "Expected snapshot creation to succeed for version 26"
        );

        let snapshot = result.unwrap();

        // Verify the snapshot properties
        assert_eq!(snapshot.version(), 26, "Snapshot version should be 26");

        // Verify that the commit files are correct
        let commit_versions: Vec<_> = snapshot
            .log_segment
            .commit_files
            .iter()
            .filter_map(|f| LogPath::new(&f.location).version)
            .collect();

        assert!(
            commit_versions.contains(&26),
            "Snapshot should include commit file for version 26"
        );
        assert!(
            commit_versions.contains(&25),
            "Snapshot should include commit file for version 25"
        );
        assert!(
            !commit_versions.contains(&27),
            "Snapshot should not include commit file for version 27"
        );
        assert!(
            !commit_versions.contains(&28),
            "Snapshot should not include commit file for version 28"
        );

        // Verify that the checkpoint file is correct
        assert_eq!(
            snapshot.log_segment.checkpoint_files.len(),
            1,
            "Snapshot should include one checkpoint file"
        );
        assert_eq!(
            LogPath::new(&snapshot.log_segment.checkpoint_files[0].location).version,
            Some(24),
            "Snapshot should use the checkpoint file for version 24"
        );

        // Verify that the log segment contains the correct range of files
        let min_version = commit_versions.iter().min().unwrap();
        let max_version = commit_versions.iter().max().unwrap();
        assert!(
            min_version >= &25,
            "Minimum commit version should be at least 25"
        );
        assert_eq!(max_version, &26, "Maximum commit version should be 26");

        // Verify that the effective version is correct
        assert_eq!(snapshot.version(), 26, "Effective version should be 26");
    }

    #[test]
    fn test_snapshot_at_latest_checkpoint_version() {
        // Setup
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/multiple-checkpoint/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();

        // Read the last checkpoint to get its version
        let fs_client = engine.get_file_system_client();
        let log_url = LogPath::new(&url).child("_delta_log/").unwrap();
        let last_checkpoint = read_last_checkpoint(fs_client.as_ref(), &log_url)
            .expect("Failed to read last checkpoint")
            .expect("No checkpoint found");

        let checkpoint_version = last_checkpoint.version;

        // Attempt to create a snapshot at the checkpoint version
        let result = Snapshot::try_new(url.clone(), &engine, Some(checkpoint_version));

        // Check if the operation succeeded
        assert!(
            result.is_ok(),
            "Expected snapshot creation to succeed for checkpoint version {}",
            checkpoint_version
        );

        let snapshot = result.unwrap();

        // Verify the snapshot properties
        assert_eq!(
            snapshot.version(),
            checkpoint_version,
            "Snapshot version should match checkpoint version"
        );

        // Verify that the checkpoint file is used
        assert_eq!(
            snapshot.log_segment.checkpoint_files.len(),
            1,
            "Snapshot should include one checkpoint file"
        );
        assert_eq!(
            LogPath::new(&snapshot.log_segment.checkpoint_files[0].location).version,
            Some(checkpoint_version),
            "Snapshot should use the checkpoint file for version {}",
            checkpoint_version
        );

        // Verify that no commit files after the checkpoint version are included
        let commit_versions: Vec<_> = snapshot
            .log_segment
            .commit_files
            .iter()
            .filter_map(|f| LogPath::new(&f.location).version)
            .collect();

        assert!(
            commit_versions.is_empty() || commit_versions.iter().all(|&v| v <= checkpoint_version),
            "Snapshot should not include commit files after checkpoint version"
        );

        // Verify that the effective version is correct
        assert_eq!(
            snapshot.version(),
            checkpoint_version,
            "Effective version should match checkpoint version"
        );
    }

    /*
    Reasoning and Example:

    Consider a Delta table with the following log structure:
    Version | File
    --------|---------------------
    0       | 00000.json
    1       | 00001.json
    2       | 00002.json
    3       | 00003.json
    4       | 00004.checkpoint.parquet
    4       | 00004.json
    5       | 00005.json
    6       | 00006.json
    7       | 00007.json
    8       | 00008.json
    9       | 00009.checkpoint.parquet
    9       | 00009.json

    1. If requested_version is None (latest version):
       - We include all commit files after the last checkpoint (4).
       - Result: [9, 8, 7, 6, 5]

    2. If requested_version is 6:
       - We include commit files after the last checkpoint (4) up to and including 6.
       - Result: [6, 5]

    3. If requested_version is 4 (same as checkpoint):
       - We only include the commit file at version 4.
       - Result: [4]

    4. If requested_version is 3 (before the checkpoint):
       - We include all commit files up to and including 3.
       - Result: [3, 2, 1, 0]

    The logic ensures that:
    a) We never include commit files beyond the requested version.
    b) We use the most recent checkpoint as a starting point for efficiency.
    c) We include the checkpoint version file only if it's explicitly requested.

    */
    #[test]
    fn test_snapshot_versions() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/multiple-checkpoint/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();

        for (
            requested_version,
            expected_version,
            expected_checkpoint,
            expected_commits,
            expected_checkpoints,
        ) in TEST_CASES.iter()
        {
            let snapshot = Snapshot::try_new(url.clone(), &engine, Some(*requested_version));

            assert!(
                snapshot.is_ok(),
                "Failed to create snapshot for version {}: {:?}",
                requested_version,
                snapshot.err()
            );
            let snapshot = snapshot.unwrap();

            assert_eq!(
                snapshot.version(),
                *expected_version,
                "For requested version {}, expected version {}, but got {}",
                requested_version,
                expected_version,
                snapshot.version()
            );

            // Check checkpoint version
            let actual_checkpoint = snapshot
                .log_segment
                .checkpoint_files
                .first()
                .and_then(|f| LogPath::new(&f.location).version);
            assert_eq!(
                actual_checkpoint, *expected_checkpoint,
                "For version {}, expected checkpoint {:?}, but got {:?}",
                requested_version, expected_checkpoint, actual_checkpoint
            );

            // Check commit files
            let commit_versions: Vec<_> = snapshot
                .log_segment
                .commit_files
                .iter()
                .filter_map(|f| LogPath::new(&f.location).version)
                .collect();
            assert_eq!(
                commit_versions, *expected_commits,
                "For version {}, expected commit files {:?}, but got {:?}",
                requested_version, expected_commits, commit_versions
            );

            // Check checkpoint files
            let checkpoint_versions: Vec<_> = snapshot
                .log_segment
                .checkpoint_files
                .iter()
                .filter_map(|f| LogPath::new(&f.location).version)
                .collect();
            assert_eq!(
                checkpoint_versions, *expected_checkpoints,
                "For version {}, expected checkpoint files {:?}, but got {:?}",
                requested_version, expected_checkpoints, checkpoint_versions
            );
        }
    }

    #[test]
    fn test_list_log_files() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/multiple-checkpoint/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();
        let fs_client = engine.get_file_system_client();
        let log_url = LogPath::new(&url).child("_delta_log/").unwrap();

        for (
            requested_version,
            _expected_version,
            _expected_checkpoint,
            expected_commits,
            expected_checkpoints,
        ) in TEST_CASES.iter()
        {
            println!(
                "Testing list_log_files with requested_version: {:?}",
                requested_version
            );

            let (commit_files, checkpoint_files) =
                list_log_files(fs_client.as_ref(), &log_url, Some(*requested_version)).unwrap();

            let commit_versions: Vec<_> = commit_files
                .iter()
                .filter_map(|f| LogPath::new(&f.location).version)
                .collect();
            let checkpoint_versions: Vec<_> = checkpoint_files
                .iter()
                .filter_map(|f| LogPath::new(&f.location).version)
                .collect();

            assert_eq!(
                commit_versions, *expected_commits,
                "For requested version {:?}, expected commit versions {:?}, but got {:?}",
                requested_version, expected_commits, commit_versions
            );
            assert_eq!(
                checkpoint_versions, *expected_checkpoints,
                "For requested version {:?}, expected checkpoint versions {:?}, but got {:?}",
                requested_version, expected_checkpoints, checkpoint_versions
            );
        }
    }

    #[test]
    fn test_snapshot_latest_version() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/multiple-checkpoint/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();

        // Create snapshot with None (latest version)
        let snapshot = Snapshot::try_new(url.clone(), &engine, None).unwrap();

        // Expected values
        let expected_version = 28;
        let expected_checkpoint_version = Some(24);
        let expected_commits = vec![28, 27, 26, 25];
        let expected_checkpoints = vec![24];

        // Assertions
        assert_eq!(
            snapshot.version(),
            expected_version,
            "Expected latest version to be {}, but got {}",
            expected_version,
            snapshot.version()
        );

        // Check checkpoint version
        let actual_checkpoint = snapshot
            .log_segment
            .checkpoint_files
            .first()
            .and_then(|f| LogPath::new(&f.location).version);
        assert_eq!(
            actual_checkpoint, expected_checkpoint_version,
            "Expected checkpoint version {:?}, but got {:?}",
            expected_checkpoint_version, actual_checkpoint
        );

        // Check commit files
        let commit_versions: Vec<_> = snapshot
            .log_segment
            .commit_files
            .iter()
            .filter_map(|f| LogPath::new(&f.location).version)
            .collect();
        assert_eq!(
            commit_versions, expected_commits,
            "Expected commit files {:?}, but got {:?}",
            expected_commits, commit_versions
        );

        // Check checkpoint files
        let checkpoint_versions: Vec<_> = snapshot
            .log_segment
            .checkpoint_files
            .iter()
            .filter_map(|f| LogPath::new(&f.location).version)
            .collect();
        assert_eq!(
            checkpoint_versions, expected_checkpoints,
            "Expected checkpoint files {:?}, but got {:?}",
            expected_checkpoints, checkpoint_versions
        );
    }

    #[test]
    fn test_list_log_files_scenarios() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/multiple-checkpoint/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let log_url = LogPath::new(&url).child("_delta_log/").unwrap();
        let fs_client = SyncEngine::new().get_file_system_client();

        // Scenario 1: requested_version is None
        let (commit_files, checkpoint_files) =
            list_log_files(fs_client.as_ref(), &log_url, None).unwrap();

        println!("Scenario 1 - commit_files: {:?}", commit_files);
        println!("Scenario 1 - checkpoint_files: {:?}", checkpoint_files);

        assert_eq!(
            commit_files.len(),
            4,
            "Expected 4 commit files when requested_version is None"
        );
        assert_eq!(
            checkpoint_files.len(),
            1,
            "Expected 1 checkpoint file when requested_version is None"
        );
        let commit_versions: Vec<_> = commit_files
            .iter()
            .filter_map(|f| LogPath::new(&f.location).version)
            .collect();
        assert_eq!(
            commit_versions,
            vec![28, 27, 26, 25],
            "Expected commit versions to be [28, 27, 26, 25]"
        );
        assert_eq!(
            LogPath::new(&checkpoint_files[0].location).version,
            Some(24),
            "Expected latest checkpoint version to be 24"
        );

        // Scenario 2: requested_version is Some(Version)
        let (commit_files, checkpoint_files) =
            list_log_files(fs_client.as_ref(), &log_url, Some(15)).unwrap();

        println!("Scenario 2 - commit_files: {:?}", commit_files);
        println!("Scenario 2 - checkpoint_files: {:?}", checkpoint_files);

        assert_eq!(
            commit_files.len(),
            1,
            "Expected 1 commit file for version 15"
        );
        assert_eq!(
            checkpoint_files.len(),
            1,
            "Expected 1 checkpoint file for version 15"
        );
        let commit_versions: Vec<_> = commit_files
            .iter()
            .filter_map(|f| LogPath::new(&f.location).version)
            .collect();
        assert_eq!(
            commit_versions,
            vec![15],
            "Expected commit version to be [15]"
        );
        assert_eq!(
            LogPath::new(&checkpoint_files[0].location).version,
            Some(14),
            "Expected checkpoint version to be 14"
        );

        // Additional test for Scenario 2 to ensure we don't include files after the requested version
        let (commit_files, checkpoint_files) =
            list_log_files(fs_client.as_ref(), &log_url, Some(22)).unwrap();
        let commit_versions: Vec<_> = commit_files
            .iter()
            .filter_map(|f| LogPath::new(&f.location).version)
            .collect();
        assert_eq!(
            commit_versions,
            vec![22, 21, 20],
            "Expected commit versions to be [22, 21, 20] for requested version 22"
        );
        assert_eq!(
            LogPath::new(&checkpoint_files[0].location).version,
            Some(19),
            "Expected checkpoint version to be 19 for requested version 22"
        );
        assert!(
            !commit_versions.contains(&23),
            "Should not include commit files after the requested version (22)"
        );

        // Scenario 3: requested_version is the same as a checkpoint version
        let (commit_files, checkpoint_files) =
            list_log_files(fs_client.as_ref(), &log_url, Some(24)).unwrap();

        println!("Scenario 3 - commit_files: {:?}", commit_files);
        println!("Scenario 3 - checkpoint_files: {:?}", checkpoint_files);

        let commit_versions: Vec<_> = commit_files
            .iter()
            .filter_map(|f| LogPath::new(&f.location).version)
            .collect();
        assert_eq!(
            commit_versions,
            vec![24],
            "Expected only commit version 24 when requested version is 24"
        );
        assert_eq!(
            checkpoint_files.len(),
            1,
            "Expected 1 checkpoint file when requested version is 24"
        );
        assert_eq!(
            LogPath::new(&checkpoint_files[0].location).version,
            Some(24),
            "Expected checkpoint version to be 24 when requested version is 24"
        );
    }
}

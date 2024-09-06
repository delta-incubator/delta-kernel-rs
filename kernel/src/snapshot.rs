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
        let (mut commit_files, checkpoint_files) =
            match (read_last_checkpoint(fs_client.as_ref(), &log_url)?, version) {
                (Some(cp), None) => {
                    list_log_files_with_checkpoint(&cp, fs_client.as_ref(), &log_url)?
                }
                (Some(cp), Some(version)) if cp.version >= version => {
                    list_log_files_with_checkpoint(&cp, fs_client.as_ref(), &log_url)?
                }
                _ => list_log_files(fs_client.as_ref(), &log_url)?,
            };

        debug!("Commit files: {:?}", commit_files.iter().map(|f| f.location.clone()).collect::<Vec<_>>());
        debug!("Checkpoint files: {:?}", checkpoint_files.iter().map(|f| f.location.clone()).collect::<Vec<_>>());
        // remove all files above requested version
        if let Some(version) = version {
            commit_files.retain(|meta| {
                if let Some(v) = LogPath::new(&meta.location).version {
                    v <= version
                } else {
                    false
                }
            });
        }

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
        },
        Err(Error::FileNotFound(_)) => Ok(None),
        Err(err) => Err(err),
    }
}

/// List all log files after a given checkpoint.
fn list_log_files_with_checkpoint(
    cp: &CheckpointMetadata,
    fs_client: &dyn FileSystemClient,
    log_root: &Url,
) -> DeltaResult<(Vec<FileMeta>, Vec<FileMeta>)> {
    let version_prefix = format!("{:020}", cp.version);
    let start_from = log_root.join(&version_prefix)?;

    let files = fs_client
        .list_from(&start_from)?
        .collect::<Result<Vec<_>, Error>>()?
        .into_iter()
        // TODO this filters out .crc files etc which start with "." - how do we want to use these kind of files?
        .filter(|f| version_from_location(&f.location).is_some())
        .collect::<Vec<_>>();

    let mut commit_files = files
        .iter()
        .filter_map(|f| {
            if LogPath::new(&f.location).is_commit {
                Some(f.clone())
            } else {
                None
            }
        })
        .collect_vec();
    // NOTE this will sort in reverse order
    commit_files.sort_unstable_by(|a, b| b.location.cmp(&a.location));

    let checkpoint_files = files
        .iter()
        .filter_map(|f| {
            if LogPath::new(&f.location).is_checkpoint {
                Some(f.clone())
            } else {
                None
            }
        })
        .collect_vec();

    // TODO raise a proper error
    assert_eq!(checkpoint_files.len(), cp.parts.unwrap_or(1) as usize);

    Ok((commit_files, checkpoint_files))
}

/// List relevant log files.
///
/// Relevant files are the max checkpoint found and all subsequent commits.
fn list_log_files(
    fs_client: &dyn FileSystemClient,
    log_root: &Url,
) -> DeltaResult<(Vec<FileMeta>, Vec<FileMeta>)> {
    let version_prefix = format!("{:020}", 0);
    let start_from = log_root.join(&version_prefix)?;

    let mut max_checkpoint_version = -1_i64;
    let mut commit_files = Vec::new();
    let mut checkpoint_files = Vec::with_capacity(10);

    for maybe_meta in fs_client.list_from(&start_from)? {
        let meta = maybe_meta?;
        let log_path = LogPath::new(&meta.location);
        if log_path.is_checkpoint {
            let version = log_path.version.unwrap_or(0) as i64;
            match version.cmp(&max_checkpoint_version) {
                Ordering::Greater => {
                    max_checkpoint_version = version;
                    checkpoint_files.clear();
                    checkpoint_files.push(meta);
                }
                Ordering::Equal => {
                    checkpoint_files.push(meta);
                }
                _ => {}
            }
        } else if log_path.is_commit {
            commit_files.push(meta);
        }
    }

    commit_files.retain(|f| {
        version_from_location(&f.location).unwrap_or(0) as i64 > max_checkpoint_version
    });
    // NOTE this will sort in reverse order
    commit_files.sort_unstable_by(|a, b| b.location.cmp(&a.location));

    Ok((commit_files, checkpoint_files))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;
    use std::sync::Arc;

    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;

    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
    use crate::engine::default::filesystem::ObjectStoreFileSystemClient;
    use crate::engine::sync::SyncEngine;
    use crate::schema::StructType;

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
        let path = std::fs::canonicalize(PathBuf::from("./tests/data/multiple-checkpoint/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let engine = SyncEngine::new();
        
        // Attempt to create a snapshot at version 10
        let result = Snapshot::try_new(url, &engine, Some(10));

        // Check if the operation succeeded
        assert!(result.is_ok(), "Expected snapshot creation to succeed for version 10");

        let snapshot = result.unwrap();

        // Verify the snapshot properties
        assert_eq!(snapshot.version(), 10, "Snapshot version should be 10");

    }
}

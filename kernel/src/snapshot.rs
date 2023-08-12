//! In-memory representation of snapshots of tables (snapshot is a table at given point in time, it
//! has schema etc.)
//!

use std::cmp::Ordering;
use std::sync::Arc;
use std::sync::RwLock;

use arrow_array::RecordBatch;
use arrow_schema::{Fields, Schema as ArrowSchema};
#[cfg(feature = "async")]
use futures::Stream;
#[cfg(feature = "async")]
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::actions::{parse_action, Action, ActionType, Metadata, Protocol};
use crate::path::LogPath;
use crate::scan::ScanBuilder;
use crate::schema::Schema;
use crate::Expression;
use crate::{DeltaResult, Error, FileMeta, TableClient, Version};

const LAST_CHECKPOINT_FILE_NAME: &str = "_last_checkpoint";

#[derive(Debug)]
pub struct LogSegment {
    log_root: Url,
    /// Reverse order soprted commit files in the log segment
    pub(crate) commit_files: Vec<FileMeta>,
    /// checkpoint files in the log segement.
    pub(crate) checkpoint_files: Vec<FileMeta>,
}

impl LogSegment {
    pub(crate) fn commit_files(&self) -> impl Iterator<Item = &FileMeta> {
        self.commit_files.iter()
    }

    /// Read a stream of log records from the log segment.
    ///
    /// Use `read_schema` to prune the fields to be read.
    /// Use `predicate` to filter the rows to be read.
    #[cfg(feature = "async")]
    pub fn replay<JRC: Send, PRC: Send>(
        &self,
        table_client: &dyn TableClient<JsonReadContext = JRC, ParquetReadContext = PRC>,
        read_schema: Arc<ArrowSchema>,
        predicate: Option<Expression>,
    ) -> DeltaResult<impl Stream<Item = DeltaResult<RecordBatch>>> {
        let mut commit_files: Vec<_> = self.commit_files().cloned().collect();

        // NOTE this will already sort in reverse order
        commit_files.sort_unstable_by(|a, b| b.location.cmp(&a.location));
        let json_client = table_client.get_json_handler();
        let read_contexts =
            json_client.contextualize_file_reads(commit_files, predicate.clone())?;
        let commit_stream = json_client
            .read_json_files(read_contexts, Arc::new(read_schema.as_ref().try_into()?))?;

        let parquet_client = table_client.get_parquet_handler();
        let read_contexts =
            parquet_client.contextualize_file_reads(self.checkpoint_files.clone(), predicate)?;
        let checkpoint_stream = parquet_client
            .read_parquet_files(read_contexts, Arc::new(read_schema.as_ref().try_into()?))?;

        let batches = commit_stream.chain(checkpoint_stream);

        Ok(batches)
    }

    #[cfg(feature = "sync")]
    pub fn replay_sync<JRC: Send, PRC: Send>(
        &self,
        table_client: &dyn TableClient<JsonReadContext = JRC, ParquetReadContext = PRC>,
        read_schema: Arc<ArrowSchema>,
        predicate: Option<Expression>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<RecordBatch>>> {
        let mut commit_files: Vec<_> = self.commit_files().cloned().collect();

        // NOTE this will already sort in reverse order
        commit_files.sort_unstable_by(|a, b| b.location.cmp(&a.location));
        let json_client = table_client.get_json_handler();
        let read_contexts =
            json_client.contextualize_file_reads(commit_files, predicate.clone())?;
        let commit_stream = json_client
            .read_json_files_sync(read_contexts, Arc::new(read_schema.as_ref().try_into()?))?;

        let parquet_client = table_client.get_parquet_handler();
        let read_contexts =
            parquet_client.contextualize_file_reads(self.checkpoint_files.clone(), predicate)?;
        let checkpoint_stream = parquet_client
            .read_parquet_files_sync(read_contexts, Arc::new(read_schema.as_ref().try_into()?))?;

        let batches = commit_stream.chain(checkpoint_stream);

        Ok(batches)
    }

    #[cfg(feature = "async")]
    async fn read_metadata<JRC: Send, PRC: Send>(
        &self,
        table_client: &dyn TableClient<JsonReadContext = JRC, ParquetReadContext = PRC>,
    ) -> DeltaResult<Option<(Metadata, Protocol)>> {
        let mut parser = MetadataParser::default();

        let read_schema = Arc::new(ArrowSchema {
            fields: Fields::from_iter([ActionType::Metadata.field(), ActionType::Protocol.field()]),
            metadata: Default::default(),
        });

        let mut batches = self.replay(table_client, read_schema, None)?;
        while let Some(batch) = batches.next().await {
            let batch = batch?;
            if let Some((metadata, protocol)) = parser.check_log_batch(&batch)? {
                return Ok(Some((metadata, protocol)));
            }
        }

        Ok(None)
    }

    #[cfg(feature = "sync")]
    fn read_metadata_sync<JRC: Send, PRC: Send>(
        &self,
        table_client: &dyn TableClient<JsonReadContext = JRC, ParquetReadContext = PRC>,
    ) -> DeltaResult<Option<(Metadata, Protocol)>> {
        let mut parser = MetadataParser::default();

        let read_schema = Arc::new(ArrowSchema {
            fields: Fields::from_iter([ActionType::Metadata.field(), ActionType::Protocol.field()]),
            metadata: Default::default(),
        });

        let batches = self.replay_sync(table_client, read_schema, None)?;
        for batch in batches {
            let batch = batch?;
            if let Some((metadata, protocol)) = parser.check_log_batch(&batch)? {
                return Ok(Some((metadata, protocol)));
            }
        }

        Ok(None)
    }
}

#[derive(Default)]
struct MetadataParser {
    metadata: Option<Metadata>,
    protocol: Option<Protocol>,
}

impl MetadataParser {
    fn check_log_batch(
        &mut self,
        log_batch: &RecordBatch,
    ) -> DeltaResult<Option<(Metadata, Protocol)>> {
        if let Ok(mut metas) = parse_action(&log_batch, &ActionType::Metadata) {
            match metas.next() {
                Some(Action::Metadata(meta)) => {
                    self.metadata = Some(meta.clone());
                }
                _ => (),
            }
        }

        if let Ok(mut protos) = parse_action(&log_batch, &ActionType::Protocol) {
            match protos.next() {
                Some(Action::Protocol(proto)) => {
                    self.protocol = Some(proto.clone());
                }
                _ => (),
            }
        }

        if let (Some(metadata), Some(protocol)) = (&self.metadata, &self.protocol) {
            Ok(Some((metadata.clone(), protocol.clone())))
        } else {
            Ok(None)
        }
    }
}

// TODO expose methods for accessing the files of a table (with file pruning).
/// In-memory representation of a specific snapshot of a Delta table. While a `DeltaTable` exists
/// throughout time, `Snapshot`s represent a view of a table at a specific point in time; they
/// have a defined schema (which may change over time for any given table), specific version, and
/// frozen log segment.
pub struct Snapshot<JRC: Send, PRC: Send> {
    table_root: Url,
    table_client: Arc<dyn TableClient<JsonReadContext = JRC, ParquetReadContext = PRC>>,
    log_segment: LogSegment,
    version: Version,
    metadata: Arc<RwLock<Option<(Metadata, Protocol)>>>,
}

impl<JRC: Send, PRC: Send> std::fmt::Debug for Snapshot<JRC, PRC> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshot")
            .field("path", &self.log_segment.log_root.as_str())
            .field("version", &self.version)
            .field("metadata", &self.metadata)
            .finish()
    }
}

impl<JRC: Send, PRC: Send + Sync> Snapshot<JRC, PRC> {
    /// Create a new [`Snapshot`] instance for the given version.
    ///
    /// # Parameters
    ///
    /// - `location`: url pointing at the table root (where `_delta_log` folder is located)
    /// - `table_client`: Implementation of [`TableClient`] apis.
    /// - `version`: target version of the [`Snapshot`]
    #[cfg(feature = "async")]
    pub async fn try_new(
        table_root: Url,
        table_client: Arc<dyn TableClient<JsonReadContext = JRC, ParquetReadContext = PRC>>,
        version: Option<Version>,
    ) -> DeltaResult<Self> {
        let mut builder = SnapshotBuilder::new(table_root, version);

        let fs_client = table_client.get_file_system_client();
        let path = builder.last_checkpoint_path();
        let last_checkpoint_data = match fs_client.read_files(vec![(path, None)]).await {
            Ok(data) => Some(data[0].clone()),
            Err(Error::FileNotFound(_)) => None,
            Err(err) => return Err(err),
        };

        let data_ref = last_checkpoint_data.as_deref().map(|bytes| bytes.as_ref());
        let start_from = builder.handle_checkpoint(data_ref)?;
        let mut file_stream = fs_client.list_from(&start_from).await?;

        while let Some(file) = file_stream.next().await {
            builder.receive_file(file?);
        }

        builder.build(table_client)
    }

    /// Create a new [`Snapshot`] instance for the given version.
    ///
    /// # Parameters
    ///
    /// - `location`: url pointing at the table root (where `_delta_log` folder is located)
    /// - `table_client`: Implementation of [`TableClient`] apis.
    /// - `version`: target version of the [`Snapshot`]
    #[cfg(feature = "sync")]
    pub fn try_new_sync(
        table_root: Url,
        table_client: Arc<dyn TableClient<JsonReadContext = JRC, ParquetReadContext = PRC>>,
        version: Option<Version>,
    ) -> DeltaResult<Self> {
        let mut builder = SnapshotBuilder::new(table_root, version);

        let fs_client = table_client.get_file_system_client();
        let path = builder.last_checkpoint_path();
        let last_checkpoint_data = match fs_client.read_files_sync(vec![(path, None)]) {
            Ok(data) => Some(data[0].clone()),
            Err(Error::FileNotFound(_)) => None,
            Err(err) => return Err(err),
        };

        let start_from = builder.handle_checkpoint(last_checkpoint_data.as_deref())?;
        let file_iter = fs_client.list_from_sync(&start_from)?;

        for file in file_iter {
            builder.receive_file(file?);
        }

        builder.build(table_client)
    }

    /// Create a new [`Snapshot`] instance.
    pub fn new(
        location: Url,
        client: Arc<dyn TableClient<JsonReadContext = JRC, ParquetReadContext = PRC>>,
        log_segment: LogSegment,
        version: Version,
    ) -> Self {
        Self {
            table_root: location,
            table_client: client,
            log_segment,
            version,
            metadata: Default::default(),
        }
    }

    /// Version of this [`Snapshot`] in the table.
    pub fn version(&self) -> Version {
        self.version
    }

    #[cfg(feature = "async")]
    async fn get_or_insert_metadata(&self) -> DeltaResult<(Metadata, Protocol)> {
        let read_lock = self
            .metadata
            .read()
            .map_err(|_| Error::Generic("filed to get read lock".into()))?;
        if let Some((metadata, protocol)) = read_lock.as_ref() {
            return Ok((metadata.clone(), protocol.clone()));
        }
        drop(read_lock);

        let (metadata, protocol) = self
            .log_segment
            .read_metadata(self.table_client.as_ref())
            .await?
            .ok_or(Error::MissingMetadata)?;
        let mut meta = self
            .metadata
            .write()
            .map_err(|_| Error::Generic("filed to get write lock".into()))?;
        *meta = Some((metadata.clone(), protocol.clone()));

        Ok((metadata, protocol))
    }

    /// Table [`Schema`] at this [`Snapshot`]s version.
    pub async fn schema(&self) -> DeltaResult<Schema> {
        self.metadata().await?.schema()
    }

    /// Table [`Metadata`] at this [`Snapshot`]s version.
    pub async fn metadata(&self) -> DeltaResult<Metadata> {
        let (metadata, _) = self.get_or_insert_metadata().await?;
        Ok(metadata)
    }

    pub async fn protocol(&self) -> DeltaResult<Protocol> {
        let (_, protocol) = self.get_or_insert_metadata().await?;
        Ok(protocol)
    }

    pub async fn scan(self) -> DeltaResult<ScanBuilder<JRC, PRC>> {
        let schema = Arc::new(self.schema().await?);
        Ok(ScanBuilder::new(
            self.table_root,
            schema,
            self.log_segment,
            self.table_client,
        ))
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointMetadata {
    /// The version of the table when the last checkpoint was made.
    pub version: Version,
    /// The number of actions that are stored in the checkpoint.
    pub size: i32,
    /// The number of fragments if the last checkpoint was written in multiple parts.
    pub parts: Option<i32>,
    /// The number of bytes of the checkpoint.
    pub size_in_bytes: Option<i32>,
    /// The number of AddFile actions in the checkpoint.
    pub num_of_add_files: Option<i32>,
    /// The schema of the checkpoint file.
    pub checkpoint_schema: Option<Schema>,
    /// The checksum of the last checkpoint JSON.
    pub checksum: Option<String>,
}

struct SnapshotBuilder {
    table_root: Url,
    version: Option<Version>,
    last_checkpoint: Option<CheckpointMetadata>,
    commit_files: Vec<FileMeta>,
    checkpoint_files: Vec<FileMeta>,
    max_checkpoint_version: i64,
}

impl SnapshotBuilder {
    fn new(table_root: Url, version: Option<Version>) -> Self {
        Self {
            table_root,
            version,
            last_checkpoint: None,
            commit_files: vec![],
            checkpoint_files: vec![],
            max_checkpoint_version: -1,
        }
    }

    /// Get the URL of the _last_checkpoint file.
    fn last_checkpoint_path(&self) -> Url {
        let log_root = LogPath(&self.table_root).child("_delta_log/").unwrap();
        LogPath(&log_root).child(LAST_CHECKPOINT_FILE_NAME).unwrap()
    }

    /// Receive the data from the _last_checkpoint file and return the URL to start
    /// listing log files from.
    fn handle_checkpoint(&mut self, checkpoint_data: Option<&[u8]>) -> DeltaResult<Url> {
        self.last_checkpoint = match checkpoint_data {
            Some(data) => Some(serde_json::from_slice(data)?),
            None => None,
        };

        let start_version = self
            .last_checkpoint
            .as_ref()
            .map(|cp| cp.version)
            .unwrap_or(0);
        let version_prefix = format!("{:020}", start_version);
        let log_root = LogPath(&self.table_root).child("_delta_log/")?;
        let start_from = log_root.join(&version_prefix)?;

        Ok(start_from)
    }

    /// Receive a file from the log and store it in the builder.
    fn receive_file(&mut self, file: FileMeta) {
        let path = LogPath(&file.location);
        if path.is_checkpoint_file() {
            let commit_version = path.commit_version().unwrap_or(0) as i64;
            match commit_version.cmp(&self.max_checkpoint_version) {
                Ordering::Greater => {
                    self.max_checkpoint_version = commit_version;
                    self.checkpoint_files.clear();
                    self.checkpoint_files.push(file);
                }
                Ordering::Equal => {
                    self.checkpoint_files.push(file);
                }
                Ordering::Less => {}
            }
        } else if path.is_commit_file() {
            self.commit_files.push(file);
        }
    }

    /// Build the snapshot based on the log files received so far.
    fn build<JRC: Send, PRC: Send>(
        mut self,
        table_client: Arc<dyn TableClient<JsonReadContext = JRC, ParquetReadContext = PRC>>,
    ) -> DeltaResult<Snapshot<JRC, PRC>> {
        // Build the snapshot
        // remove all files above requested version
        if let Some(version) = self.version {
            self.commit_files.retain(|meta| {
                if let Some(v) = LogPath(&meta.location).commit_version() {
                    v <= version
                } else {
                    false
                }
            });
        }

        // sort commit files in reverse chronological order
        self.commit_files
            .sort_unstable_by(|a, b| b.location.cmp(&a.location));

        if let Some(cp) = self.last_checkpoint {
            if self.checkpoint_files.len() != cp.parts.unwrap_or(1) as usize {
                // TODO more descriptive error
                return Err(Error::FileNotFound(format!("Expected to find {} checkpoint files but only found {}. The table may be corrupted.",
                    cp.parts.unwrap_or(1), self.checkpoint_files.len())));
            }
        }

        // Remove log files at or before the checkpoint
        self.commit_files.retain(|f| {
            LogPath(&f.location)
                .commit_version()
                .map(|v| v as i64 > self.max_checkpoint_version)
                .unwrap_or(false)
        });

        // get the effective version from chosen files
        let version_eff = if !self.commit_files.is_empty() {
            self.commit_files
                .first()
                .and_then(|f| LogPath(&f.location).commit_version())
                .unwrap()
        } else if !self.checkpoint_files.is_empty() {
            self.checkpoint_files
                .first()
                .and_then(|f| LogPath(&f.location).commit_version())
                .unwrap()
        } else {
            // TODO more descriptive error
            return Err(Error::MissingVersion);
        };

        if let Some(v) = self.version {
            if version_eff != v {
                // TODO more descriptive error
                return Err(Error::MissingVersion);
            }
        }

        let log_segment = LogSegment {
            log_root: LogPath(&self.table_root).child("_delta_log/")?,
            commit_files: self.commit_files,
            checkpoint_files: self.checkpoint_files,
        };

        Ok(Snapshot {
            table_root: self.table_root,
            table_client,
            log_segment,
            version: version_eff,
            metadata: Default::default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::path::PathBuf;

    use object_store::local::LocalFileSystem;
    use object_store::path::Path;

    use crate::client::DefaultTableClient;
    use crate::filesystem::ObjectStoreFileSystemClient;
    use crate::schema::StructType;

    #[tokio::test]
    async fn test_snapshot_read_metadata() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let client =
            Arc::new(DefaultTableClient::try_new(&url, HashMap::<String, String>::new()).unwrap());
        let snapshot = Snapshot::try_new(url, client, Some(1)).await.unwrap();

        let protocol = snapshot.protocol().await.unwrap();
        let expected = Protocol {
            min_reader_version: 3,
            min_wrriter_version: 7,
            reader_features: Some(vec!["deletionVectors".into()]),
            writer_features: Some(vec!["deletionVectors".into()]),
        };
        assert_eq!(protocol, expected);

        let schema_string = r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#;
        let expected: StructType = serde_json::from_str(schema_string).unwrap();
        let schema = snapshot.schema().await.unwrap();
        assert_eq!(schema, expected);
    }

    #[tokio::test]
    async fn test_new_snapshot() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let client =
            Arc::new(DefaultTableClient::try_new(&url, HashMap::<String, String>::new()).unwrap());
        let snapshot = Snapshot::try_new(url, client, None).await.unwrap();

        let protocol = snapshot.protocol().await.unwrap();
        let expected = Protocol {
            min_reader_version: 3,
            min_wrriter_version: 7,
            reader_features: Some(vec!["deletionVectors".into()]),
            writer_features: Some(vec!["deletionVectors".into()]),
        };
        assert_eq!(protocol, expected);

        let schema_string = r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#;
        let expected: StructType = serde_json::from_str(schema_string).unwrap();
        let schema = snapshot.schema().await.unwrap();
        assert_eq!(schema, expected);
    }

    #[tokio::test]
    async fn test_read_table_with_last_checkpoint() {
        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/table-with-dv-small/_delta_log/",
        ))
        .unwrap();
        let location = url::Url::from_directory_path(path).unwrap();

        let table_client = Arc::new(
            DefaultTableClient::try_new(&location, HashMap::<String, String>::new()).unwrap(),
        );
        let snapshot = Snapshot::try_new(location, table_client, None)
            .await
            .unwrap();

        assert_eq!(snapshot.log_segment.checkpoint_files.len(), 0);

        assert_eq!(
            snapshot.log_segment.commit_files.len(),
            2,
            "expected 1 commit file, but got {:?}",
            snapshot.log_segment.commit_files
        );
        assert_eq!(
            LogPath(&snapshot.log_segment.commit_files[0].location).commit_version(),
            Some(1)
        );
    }

    #[tokio::test]
    async fn test_read_table_with_checkpoint() {
        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/with_checkpoint_no_last_checkpoint/",
        ))
        .unwrap();
        let location = url::Url::from_directory_path(path).unwrap();
        let table_client = Arc::new(
            DefaultTableClient::try_new(&location, HashMap::<String, String>::new()).unwrap(),
        );
        let snapshot = Snapshot::try_new(location, table_client, None)
            .await
            .unwrap();

        assert_eq!(snapshot.log_segment.checkpoint_files.len(), 1);
        assert_eq!(
            LogPath(&snapshot.log_segment.checkpoint_files[0].location).commit_version(),
            Some(2)
        );
        assert_eq!(
            snapshot.log_segment.commit_files.len(),
            1,
            "expected 1 commit file, but got {:?}",
            snapshot.log_segment.commit_files
        );
        assert_eq!(
            LogPath(&snapshot.log_segment.commit_files[0].location).commit_version(),
            Some(3)
        );
    }
}

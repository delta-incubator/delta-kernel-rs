//! Delta Table properties. Note this module implements per-table configuration which governs how
//! table-level capabilities/properties are configured (turned on/off etc.). This is orthogonal to
//! protocol-level 'table features' which enable or disable reader/writer features (which then
//! usually must be enabled/configured by table properties).
//!
//! For example (from delta's protocol.md): A feature being supported does not imply that it is
//! active. For example, a table may have the `appendOnly` feature listed in writerFeatures, but it
//! does not have a table property delta.appendOnly that is set to `true`. In such a case the table
//! is not append-only, and writers are allowed to change, remove, and rearrange data. However,
//! writers must know that the table property delta.appendOnly should be checked before writing the
//! table.

use std::collections::HashMap;
use std::time::Duration;

use serde::Deserialize;

use crate::expressions::ColumnName;
use crate::table_features::ColumnMappingMode;
use crate::DeltaResult;

mod deserialize;
use deserialize::*;

/// Default num index cols
pub const DEFAULT_NUM_INDEX_COLS: i32 = 32;

/// Delta table properties. These are parsed from the 'configuration' map in the most recent
/// 'Metadata' action of a table.
#[derive(Deserialize, Debug, Clone, Eq, PartialEq, Default)]
#[serde(default)]
pub struct TableProperties {
    /// true for this Delta table to be append-only. If append-only,
    /// existing records cannot be deleted, and existing values cannot be updated.
    #[serde(rename = "delta.appendOnly")]
    #[serde(deserialize_with = "deserialize_bool")]
    pub append_only: Option<bool>,

    /// true for Delta Lake to automatically optimize the layout of the files for this Delta table.
    #[serde(rename = "delta.autoOptimize.autoCompact")]
    #[serde(deserialize_with = "deserialize_bool")]
    pub auto_compact: Option<bool>,

    /// true for Delta Lake to automatically optimize the layout of the files for this Delta table
    /// during writes.
    #[serde(rename = "delta.autoOptimize.optimizeWrite")]
    #[serde(deserialize_with = "deserialize_bool")]
    pub optimize_write: Option<bool>,

    /// Interval (expressed as number of commits) after which a new checkpoint should be created.
    /// E.g. if checkpoint interval = 10, then a checkpoint should be written every 10 commits.
    #[serde(rename = "delta.checkpointInterval")]
    #[serde(deserialize_with = "deserialize_pos_int")]
    pub checkpoint_interval: Option<u64>,

    /// true for Delta Lake to write file statistics in checkpoints in JSON format for the stats column.
    #[serde(rename = "delta.checkpoint.writeStatsAsJson")]
    #[serde(deserialize_with = "deserialize_bool")]
    pub checkpoint_write_stats_as_json: Option<bool>,

    /// true for Delta Lake to write file statistics to checkpoints in struct format for the
    /// stats_parsed column and to write partition values as a struct for partitionValues_parsed.
    #[serde(rename = "delta.checkpoint.writeStatsAsStruct")]
    #[serde(deserialize_with = "deserialize_bool")]
    pub checkpoint_write_stats_as_struct: Option<bool>,

    /// Whether column mapping is enabled for Delta table columns and the corresponding
    /// Parquet columns that use different names.
    #[serde(rename = "delta.columnMapping.mode")]
    #[serde(deserialize_with = "deserialize_option")]
    pub column_mapping_mode: Option<ColumnMappingMode>,

    /// The number of columns for Delta Lake to collect statistics about for data skipping.
    /// A value of -1 means to collect statistics for all columns. Updating this property does
    /// not automatically collect statistics again; instead, it redefines the statistics schema
    /// of the Delta table. Specifically, it changes the behavior of future statistics collection
    /// (such as during appends and optimizations) as well as data skipping (such as ignoring column
    /// statistics beyond this number, even when such statistics exist).
    #[serde(rename = "delta.dataSkippingNumIndexedCols")]
    #[serde(deserialize_with = "deserialize_option")]
    pub data_skipping_num_indexed_cols: Option<DataSkippingNumIndexedCols>,

    /// A comma-separated list of column names on which Delta Lake collects statistics to enhance
    /// data skipping functionality. This property takes precedence over
    /// [DataSkippingNumIndexedCols](DeltaConfigKey::DataSkippingNumIndexedCols).
    #[serde(rename = "delta.dataSkippingStatsColumns")]
    #[serde(deserialize_with = "deserialize_column_names")]
    pub data_skipping_stats_columns: Option<Vec<ColumnName>>,

    /// The shortest duration for Delta Lake to keep logically deleted data files before deleting
    /// them physically. This is to prevent failures in stale readers after compactions or partition
    /// overwrites.
    ///
    /// This value should be large enough to ensure that:
    ///
    /// * It is larger than the longest possible duration of a job if you run VACUUM when there are
    ///   concurrent readers or writers accessing the Delta table.
    /// * If you run a streaming query that reads from the table, that query does not stop for
    ///   longer than this value. Otherwise, the query may not be able to restart, as it must still
    ///   read old files.
    #[serde(rename = "delta.deletedFileRetentionDuration")]
    #[serde(deserialize_with = "deserialize_interval")]
    pub deleted_file_retention_duration: Option<Duration>,

    /// true to enable change data feed.
    #[serde(rename = "delta.enableChangeDataFeed")]
    #[serde(deserialize_with = "deserialize_bool")]
    pub enable_change_data_feed: Option<bool>,

    /// true to enable deletion vectors and predictive I/O for updates.
    #[serde(rename = "delta.enableDeletionVectors")]
    #[serde(deserialize_with = "deserialize_bool")]
    pub enable_deletion_vectors: Option<bool>,

    /// The degree to which a transaction must be isolated from modifications made by concurrent
    /// transactions.
    ///
    /// Valid values are `Serializable` and `WriteSerializable`.
    #[serde(rename = "delta.isolationLevel")]
    #[serde(deserialize_with = "deserialize_option")]
    pub isolation_level: Option<IsolationLevel>,

    /// How long the history for a Delta table is kept.
    ///
    /// Each time a checkpoint is written, Delta Lake automatically cleans up log entries older
    /// than the retention interval. If you set this property to a large enough value, many log
    /// entries are retained. This should not impact performance as operations against the log are
    /// constant time. Operations on history are parallel but will become more expensive as the log
    /// size increases.
    #[serde(rename = "delta.logRetentionDuration")]
    #[serde(deserialize_with = "deserialize_interval")]
    pub log_retention_duration: Option<Duration>,

    /// TODO docs
    #[serde(rename = "delta.enableExpiredLogCleanup")]
    #[serde(deserialize_with = "deserialize_bool")]
    pub enable_expired_log_cleanup: Option<bool>,

    // /// The minimum required protocol reader version for a reader that allows to read from this Delta table.
    // #[serde(rename = "delta.minReaderVersion")]
    // pub min_reader_version: Option<u8>,

    // /// The minimum required protocol writer version for a writer that allows to write to this Delta table.
    // #[serde(rename = "delta.minWriterVersion")]
    // pub min_writer_version: Option<u8>,
    //
    /// true for Delta to generate a random prefix for a file path instead of partition information.
    ///
    /// For example, this may improve Amazon S3 performance when Delta Lake needs to send very high
    /// volumes of Amazon S3 calls to better partition across S3 servers.
    #[serde(rename = "delta.randomizeFilePrefixes")]
    #[serde(deserialize_with = "deserialize_bool")]
    pub randomize_file_prefixes: Option<bool>,

    /// When delta.randomizeFilePrefixes is set to true, the number of characters that Delta
    /// generates for random prefixes.
    #[serde(rename = "delta.randomPrefixLength")]
    #[serde(deserialize_with = "deserialize_pos_int")]
    pub random_prefix_length: Option<u64>,

    /// The shortest duration within which new snapshots will retain transaction identifiers (for
    /// example, SetTransactions). When a new snapshot sees a transaction identifier older than or
    /// equal to the duration specified by this property, the snapshot considers it expired and
    /// ignores it. The SetTransaction identifier is used when making the writes idempotent.
    #[serde(rename = "delta.setTransactionRetentionDuration")]
    #[serde(deserialize_with = "deserialize_interval")]
    pub set_transaction_retention_duration: Option<Duration>,

    /// The target file size in bytes or higher units for file tuning. For example, 104857600
    /// (bytes) or 100mb.
    ///
    /// TODO: kernel doesn't govern file writes? should we pass this through just with a note that
    /// we don't pay attention to it? Scenario: engine calls
    /// snapshot.table_properties().target_file_size in order to know how big _it_ should write
    /// parquet files.
    #[serde(rename = "delta.targetFileSize")]
    #[serde(deserialize_with = "deserialize_pos_int")]
    pub target_file_size: Option<u64>,

    /// The target file size in bytes or higher units for file tuning. For example, 104857600
    /// (bytes) or 100mb.
    ///
    /// See TODO above
    #[serde(rename = "delta.tuneFileSizesForRewrites")]
    #[serde(deserialize_with = "deserialize_bool")]
    pub tune_file_sizes_for_rewrites: Option<bool>,

    /// 'classic' for classic Delta Lake checkpoints. 'v2' for v2 checkpoints.
    #[serde(rename = "delta.checkpointPolicy")]
    #[serde(deserialize_with = "deserialize_option")]
    pub checkpoint_policy: Option<CheckpointPolicy>,

    /// TODO
    #[serde(rename = "delta.enableRowTracking")]
    #[serde(deserialize_with = "deserialize_bool")]
    pub enable_row_tracking: Option<bool>,
}

impl TableProperties {
    pub(crate) fn new(config_map: &HashMap<String, String>) -> DeltaResult<Self> {
        let deserializer = StringMapDeserializer::new(config_map);
        // FIXME error
        TableProperties::deserialize(deserializer).map_err(|e| crate::Error::Generic(e.to_string()))
    }

    pub(crate) fn get_column_mapping_mode(&self) -> ColumnMappingMode {
        self.column_mapping_mode.unwrap_or(ColumnMappingMode::None)
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
#[serde(try_from = "String")]
pub enum DataSkippingNumIndexedCols {
    AllColumns,
    NumColumns(u64),
}

impl TryFrom<String> for DataSkippingNumIndexedCols {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let num: i64 = value
            .parse()
            .map_err(|_| format!("couldn't parse {value} as i64"))?; // FIXME
        match num {
            -1 => Ok(DataSkippingNumIndexedCols::AllColumns),
            x if x > -1 => Ok(DataSkippingNumIndexedCols::NumColumns(x as u64)),
            _ => Err(format!("Invalid value: {}", value)),
        }
    }
}

/// The isolation level applied during transaction
#[derive(Deserialize, Debug, Copy, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum IsolationLevel {
    /// The strongest isolation level. It ensures that committed write operations
    /// and all reads are Serializable. Operations are allowed as long as there
    /// exists a serial sequence of executing them one-at-a-time that generates
    /// the same outcome as that seen in the table. For the write operations,
    /// the serial sequence is exactly the same as that seen in the table’s history.
    Serializable,

    /// A weaker isolation level than Serializable. It ensures only that the write
    /// operations (that is, not reads) are serializable. However, this is still stronger
    /// than Snapshot isolation. WriteSerializable is the default isolation level because
    /// it provides great balance of data consistency and availability for most common operations.
    WriteSerializable,

    /// SnapshotIsolation is a guarantee that all reads made in a transaction will see a consistent
    /// snapshot of the database (in practice it reads the last committed values that existed at the
    /// time it started), and the transaction itself will successfully commit only if no updates
    /// it has made conflict with any concurrent updates made since that snapshot.
    SnapshotIsolation,
}

// Delta-Spark default isolation level is Serializable
impl Default for IsolationLevel {
    fn default() -> Self {
        Self::Serializable
    }
}

/// The checkpoint policy applied when writing checkpoints
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CheckpointPolicy {
    /// classic Delta Lake checkpoints
    Classic,
    /// v2 checkpoints
    V2,
}

impl Default for CheckpointPolicy {
    fn default() -> Self {
        Self::Classic
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::column_name;
    use std::collections::HashMap;

    #[test]
    fn fail_known_keys() {
        let properties = HashMap::from([("delta.appendOnly".to_string(), "wack".to_string())]);
        let de = StringMapDeserializer::new(&properties);
        assert!(TableProperties::deserialize(de).is_err());
    }

    #[test]
    fn allow_unknown_keys() {
        let properties =
            HashMap::from([("some_random_unknown_key".to_string(), "test".to_string())]);
        let de = StringMapDeserializer::new(&properties);
        let actual = TableProperties::deserialize(de).unwrap();
        let expected = TableProperties::default();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_empty_table_properties() {
        let map = HashMap::new();
        let de = StringMapDeserializer::new(&map);
        let actual = TableProperties::deserialize(de).unwrap();
        let default_table_properties = TableProperties::default();
        assert_eq!(actual, default_table_properties);
    }

    #[test]
    fn test_parse_table_properties() {
        let properties = [
            ("delta.appendOnly", "true"),
            ("delta.autoOptimize.optimizeWrite", "true"),
            ("delta.autoOptimize.autoCompact", "true"),
            ("delta.checkpointInterval", "101"),
            ("delta.checkpoint.writeStatsAsJson", "true"),
            ("delta.checkpoint.writeStatsAsStruct", "true"),
            ("delta.columnMapping.mode", "id"),
            ("delta.dataSkippingNumIndexedCols", "-1"),
            ("delta.dataSkippingStatsColumns", "col1,col2"),
            ("delta.deletedFileRetentionDuration", "interval 1 second"),
            ("delta.enableChangeDataFeed", "true"),
            ("delta.enableDeletionVectors", "true"),
            ("delta.isolationLevel", "snapshotIsolation"),
            ("delta.logRetentionDuration", "interval 2 seconds"),
            ("delta.enableExpiredLogCleanup", "true"),
            ("delta.randomizeFilePrefixes", "true"),
            ("delta.randomPrefixLength", "1001"),
            (
                "delta.setTransactionRetentionDuration",
                "interval 60 seconds",
            ),
            ("delta.targetFileSize", "1000000000"),
            ("delta.tuneFileSizesForRewrites", "true"),
            ("delta.checkpointPolicy", "v2"),
            ("delta.enableRowTracking", "true"),
        ];
        let properties = properties
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let de = StringMapDeserializer::new(&properties);
        let actual = TableProperties::deserialize(de).unwrap();
        let expected = TableProperties {
            append_only: Some(true),
            optimize_write: Some(true),
            auto_compact: Some(true),
            checkpoint_interval: Some(101),
            checkpoint_write_stats_as_json: Some(true),
            checkpoint_write_stats_as_struct: Some(true),
            column_mapping_mode: Some(ColumnMappingMode::Id),
            data_skipping_num_indexed_cols: Some(DataSkippingNumIndexedCols::AllColumns),
            data_skipping_stats_columns: Some(vec![column_name!("col1"), column_name!("col2")]),
            deleted_file_retention_duration: Some(Duration::new(1, 0)),
            enable_change_data_feed: Some(true),
            enable_deletion_vectors: Some(true),
            isolation_level: Some(IsolationLevel::SnapshotIsolation),
            log_retention_duration: Some(Duration::new(2, 0)),
            enable_expired_log_cleanup: Some(true),
            randomize_file_prefixes: Some(true),
            random_prefix_length: Some(1001),
            set_transaction_retention_duration: Some(Duration::new(60, 0)),
            target_file_size: Some(1_000_000_000),
            tune_file_sizes_for_rewrites: Some(true),
            checkpoint_policy: Some(CheckpointPolicy::V2),
            enable_row_tracking: Some(true),
        };
        assert_eq!(actual, expected);
    }
}

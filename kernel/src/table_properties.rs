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
#[derive(Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct TableProperties {
    /// true for this Delta table to be append-only. If append-only,
    /// existing records cannot be deleted, and existing values cannot be updated.
    #[serde(rename = "delta.appendOnly")]
    #[serde(deserialize_with = "deserialize_bool")]
    #[serde(default)]
    pub append_only: bool,

    /// true for Delta Lake to automatically optimize the layout of the files for this Delta table.
    #[serde(rename = "delta.autoOptimize.autoCompact")]
    #[serde(deserialize_with = "deserialize_bool")]
    #[serde(default)]
    pub auto_compact: bool,

    /// true for Delta Lake to automatically optimize the layout of the files for this Delta table
    /// during writes.
    #[serde(rename = "delta.autoOptimize.optimizeWrite")]
    #[serde(deserialize_with = "deserialize_bool")]
    #[serde(default)]
    pub optimize_write: bool,

    /// Interval (expressed as number of commits) after which a new checkpoint should be created.
    /// E.g. if checkpoint interval = 10, then a checkpoint should be written every 10 commits.
    #[serde(rename = "delta.checkpointInterval")]
    #[serde(deserialize_with = "deserialize_pos_int")]
    #[serde(default = "default_checkpoint_interval")]
    pub checkpoint_interval: u64,

    /// true for Delta Lake to write file statistics in checkpoints in JSON format for the stats column.
    #[serde(rename = "delta.checkpoint.writeStatsAsJson")]
    #[serde(deserialize_with = "deserialize_bool")]
    #[serde(default)]
    pub checkpoint_write_stats_as_json: bool,

    /// true for Delta Lake to write file statistics to checkpoints in struct format for the
    /// stats_parsed column and to write partition values as a struct for partitionValues_parsed.
    #[serde(rename = "delta.checkpoint.writeStatsAsStruct")]
    #[serde(deserialize_with = "deserialize_bool")]
    #[serde(default)]
    pub checkpoint_write_stats_as_struct: bool,

    /// Whether column mapping is enabled for Delta table columns and the corresponding
    /// Parquet columns that use different names.
    #[serde(rename = "delta.columnMapping.mode", default)]
    pub column_mapping_mode: ColumnMappingMode,

    /// The number of columns for Delta Lake to collect statistics about for data skipping.
    /// A value of -1 means to collect statistics for all columns. Updating this property does
    /// not automatically collect statistics again; instead, it redefines the statistics schema
    /// of the Delta table. Specifically, it changes the behavior of future statistics collection
    /// (such as during appends and optimizations) as well as data skipping (such as ignoring column
    /// statistics beyond this number, even when such statistics exist).
    #[serde(rename = "delta.dataSkippingNumIndexedCols", default, flatten)]
    pub data_skipping_num_indexed_cols: Option<DataSkippingNumIndexedCols>,

    /// A comma-separated list of column names on which Delta Lake collects statistics to enhance
    /// data skipping functionality. This property takes precedence over
    /// [DataSkippingNumIndexedCols](DeltaConfigKey::DataSkippingNumIndexedCols).
    #[serde(rename = "delta.dataSkippingStatsColumns")]
    #[serde(deserialize_with = "deserialize_column_names")]
    #[serde(default)]
    pub data_skipping_stats_columns: Vec<ColumnName>,

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
    #[serde(default)]
    pub deleted_file_retention_duration: Option<Duration>,

    /// true to enable change data feed.
    #[serde(rename = "delta.enableChangeDataFeed")]
    #[serde(deserialize_with = "deserialize_bool")]
    #[serde(default)]
    pub enable_change_data_feed: bool,

    /// true to enable deletion vectors and predictive I/O for updates.
    #[serde(rename = "delta.enableDeletionVectors")]
    #[serde(deserialize_with = "deserialize_bool")]
    #[serde(default)]
    pub enable_deletion_vectors: bool,

    /// The degree to which a transaction must be isolated from modifications made by concurrent
    /// transactions.
    ///
    /// Valid values are `Serializable` and `WriteSerializable`.
    #[serde(rename = "delta.isolationLevel")]
    #[serde(default)]
    pub isolation_level: IsolationLevel,

    /// How long the history for a Delta table is kept.
    ///
    /// Each time a checkpoint is written, Delta Lake automatically cleans up log entries older
    /// than the retention interval. If you set this property to a large enough value, many log
    /// entries are retained. This should not impact performance as operations against the log are
    /// constant time. Operations on history are parallel but will become more expensive as the log
    /// size increases.
    #[serde(rename = "delta.logRetentionDuration")]
    #[serde(deserialize_with = "deserialize_interval")]
    #[serde(default)]
    pub log_retention_duration: Option<Duration>,

    /// TODO docs
    #[serde(rename = "delta.enableExpiredLogCleanup")]
    #[serde(default)]
    pub enable_expired_log_cleanup: bool,

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
    #[serde(default)]
    pub randomize_file_prefixes: bool,

    /// When delta.randomizeFilePrefixes is set to true, the number of characters that Delta
    /// generates for random prefixes.
    #[serde(rename = "delta.randomPrefixLength")]
    #[serde(default)]
    pub random_prefix_length: Option<u32>,

    /// The shortest duration within which new snapshots will retain transaction identifiers (for
    /// example, SetTransactions). When a new snapshot sees a transaction identifier older than or
    /// equal to the duration specified by this property, the snapshot considers it expired and
    /// ignores it. The SetTransaction identifier is used when making the writes idempotent.
    #[serde(rename = "delta.setTransactionRetentionDuration")]
    #[serde(deserialize_with = "deserialize_interval")]
    #[serde(default)]
    pub set_transaction_retention_duration: Option<Duration>,

    /// The target file size in bytes or higher units for file tuning. For example, 104857600
    /// (bytes) or 100mb.
    ///
    /// TODO: kernel doesn't govern file writes? should we pass this through just with a note that
    /// we don't pay attention to it? Scenario: engine calls
    /// snapshot.table_properties().target_file_size in order to know how big _it_ should write
    /// parquet files.
    #[serde(rename = "delta.targetFileSize")]
    #[serde(default)]
    pub target_file_size: Option<u64>,

    /// The target file size in bytes or higher units for file tuning. For example, 104857600
    /// (bytes) or 100mb.
    ///
    /// See TODO above
    #[serde(rename = "delta.tuneFileSizesForRewrites")]
    #[serde(default)]
    pub tune_file_sizes_for_rewrites: Option<bool>,

    /// 'classic' for classic Delta Lake checkpoints. 'v2' for v2 checkpoints.
    #[serde(rename = "delta.checkpointPolicy")]
    #[serde(default)]
    pub checkpoint_policy: CheckpointPolicy,
}

impl TableProperties {
    pub(crate) fn new(config_map: &HashMap<String, String>) -> DeltaResult<Self> {
        let deserializer = StringMapDeserializer::new(config_map);
        // FIXME error
        TableProperties::deserialize(deserializer).map_err(|e| crate::Error::Generic(e.to_string()))
    }
}

fn default_checkpoint_interval() -> u64 {
    10
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

// /// Delta configuration error
// #[derive(thiserror::Error, Debug, PartialEq, Eq)]
// pub enum DeltaConfigError {
//     /// Error returned when configuration validation failed.
//     #[error("Validation failed - {0}")]
//     Validation(String),
// }
//
// impl From<DeltaConfigError> for Error {
//     fn from(e: DeltaConfigError) -> Self {
//         Error::InvalidConfiguration(e.to_string())
//     }
// }

//     /// The shortest duration for Delta Lake to keep logically deleted data files before deleting
//     /// them physically. This is to prevent failures in stale readers after compactions or partition
//     /// overwrites.
//     ///
//     /// This value should be large enough to ensure that:
//     ///
//     /// * It is larger than the longest possible duration of a job if you run VACUUM when there are
//     ///   concurrent readers or writers accessing the Delta table.
//     /// * If you run a streaming query that reads from the table, that query does not stop for
//     ///   longer than this value. Otherwise, the query may not be able to restart, as it must still
//     ///   read old files.
//     pub fn deleted_file_retention_duration(&self) -> Duration {
//         static DEFAULT_FILE_RETENTION_DURATION: LazyLock<Duration> =
//             LazyLock::new(|| parse_interval("interval 1 weeks").unwrap());
//         self.0
//             .get(DeltaTableProperty::DeletedFileRetentionDuration.as_ref())
//             .and_then(|v| parse_interval(v).ok())
//             .unwrap_or_else(|| DEFAULT_FILE_RETENTION_DURATION.to_owned())
//     }
//
//     /// How long the history for a Delta table is kept.
//     ///
//     /// Each time a checkpoint is written, Delta Lake automatically cleans up log entries older
//     /// than the retention interval. If you set this property to a large enough value, many log
//     /// entries are retained. This should not impact performance as operations against the log are
//     /// constant time. Operations on history are parallel but will become more expensive as the log
//     /// size increases.
//     pub fn log_retention_duration(&self) -> Duration {
//         static DEFAULT_LOG_RETENTION_DURATION: LazyLock<Duration> =
//             LazyLock::new(|| parse_interval("interval 30 days").unwrap());
//         self.0
//             .get(DeltaTableProperty::LogRetentionDuration.as_ref())
//             .and_then(|v| parse_interval(v).ok())
//             .unwrap_or_else(|| DEFAULT_LOG_RETENTION_DURATION.to_owned())
//     }
//
//     /// The degree to which a transaction must be isolated from modifications made by concurrent
//     /// transactions.
//     ///
//     /// Valid values are `Serializable` and `WriteSerializable`.
//     pub fn isolation_level(&self) -> IsolationLevel {
//         self.0
//             .get(DeltaTableProperty::IsolationLevel.as_ref())
//             .and_then(|v| v.parse().ok())
//             .unwrap_or_default()
//     }
//
//     /// Policy applied during chepoint creation
//     pub fn checkpoint_policy(&self) -> CheckpointPolicy {
//         self.0
//             .get(DeltaTableProperty::CheckpointPolicy.as_ref())
//             .and_then(|v| v.parse().ok())
//             .unwrap_or_default()
//     }
//
//     /// Return the column mapping mode according to delta.columnMapping.mode
//     pub fn column_mapping_mode(&self) -> ColumnMappingMode {
//         self.0
//             .get(DeltaTableProperty::ColumnMappingMode.as_ref())
//             .and_then(|v| v.parse().ok())
//             .unwrap_or_default()
//     }
//
//     /// Return the check constraints on the current table
//     pub fn get_constraints(&self) -> Vec<Constraint> {
//         self.0
//             .iter()
//             .filter_map(|(field, value)| {
//                 if field.starts_with("delta.constraints") {
//                     field
//                         .splitn(3, '.')
//                         .last()
//                         .map(|n| Constraint::new(n, value))
//                 } else {
//                     None
//                 }
//             })
//             .collect()
//     }
//
//     /// Column names on which Delta Lake collects statistics to enhance data skipping functionality.
//     /// This property takes precedence over [num_indexed_cols](Self::num_indexed_cols).
//     pub fn stats_columns(&self) -> Option<Vec<&str>> {
//         self.0
//             .get(DeltaTableProperty::DataSkippingStatsColumns.as_ref())
//             .map(|v| v.split(',').collect())
//     }

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

// Spark assumes Serializable as default isolation level
// https://github.com/delta-io/delta/blob/abb171c8401200e7772b27e3be6ea8682528ac72/core/src/main/scala/org/apache/spark/sql/delta/OptimisticTransaction.scala#L1023
impl Default for IsolationLevel {
    fn default() -> Self {
        Self::Serializable
    }
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
/// The checkpoint policy applied when writing checkpoints
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
    use crate::actions::{Format, Metadata};
    use crate::schema::StructType;
    use std::collections::HashMap;

    fn dummy_metadata() -> Metadata {
        let schema = StructType::new(Vec::new());
        Metadata {
            id: "id".into(),
            name: None,
            description: None,
            format: Format::default(),
            schema_string: serde_json::to_string(&schema).unwrap(),
            partition_columns: Vec::new(),
            configuration: HashMap::new(),
            created_time: None,
        }
    }

    #[test]
    fn fail_known_keys() {
        let properties = HashMap::from([("delta.appendOnly".to_string(), "wack".to_string())]);
        let de = StringMapDeserializer::new(&properties);
        assert!(TableProperties::deserialize(de).is_err());
    }

    // #[test]
    // fn get_interval_from_metadata_test() {
    //     let md = dummy_metadata();
    //     let config = TableConfig(&md.configuration);

    //     // default 1 week
    //     assert_eq!(
    //         config.deleted_file_retention_duration().as_secs(),
    //         SECONDS_PER_WEEK,
    //     );

    //     // change to 2 day
    //     let mut md = dummy_metadata();
    //     md.configuration.insert(
    //         DeltaTableProperty::DeletedFileRetentionDuration
    //             .as_ref()
    //             .to_string(),
    //         "interval 2 day".to_string(),
    //     );
    //     let config = TableConfig(&md.configuration);

    //     assert_eq!(
    //         config.deleted_file_retention_duration().as_secs(),
    //         2 * SECONDS_PER_DAY,
    //     );
    // }

    // #[test]
    // fn get_long_from_metadata_test() {
    //     let md = dummy_metadata();
    //     let config = TableConfig(&md.configuration);
    //     assert_eq!(config.checkpoint_interval(), 10,)
    // }

    // #[test]
    // fn get_boolean_from_metadata_test() {
    //     let md = dummy_metadata();
    //     let config = TableConfig(&md.configuration);

    //     // default value is true
    //     assert!(config.enable_expired_log_cleanup());

    //     // change to false
    //     let mut md = dummy_metadata();
    //     md.configuration.insert(
    //         DeltaTableProperty::EnableExpiredLogCleanup.as_ref().into(),
    //         "false".to_string(),
    //     );
    //     let config = TableConfig(&md.configuration);

    //     assert!(!config.enable_expired_log_cleanup());
    // }

    // #[test]
    // fn parse_interval_test() {
    //     assert_eq!(
    //         parse_interval("interval 123 nanosecond").unwrap(),
    //         Duration::from_nanos(123)
    //     );

    //     assert_eq!(
    //         parse_interval("interval 123 nanoseconds").unwrap(),
    //         Duration::from_nanos(123)
    //     );

    //     assert_eq!(
    //         parse_interval("interval 123 microsecond").unwrap(),
    //         Duration::from_micros(123)
    //     );

    //     assert_eq!(
    //         parse_interval("interval 123 microseconds").unwrap(),
    //         Duration::from_micros(123)
    //     );

    //     assert_eq!(
    //         parse_interval("interval 123 millisecond").unwrap(),
    //         Duration::from_millis(123)
    //     );

    //     assert_eq!(
    //         parse_interval("interval 123 milliseconds").unwrap(),
    //         Duration::from_millis(123)
    //     );

    //     assert_eq!(
    //         parse_interval("interval 123 second").unwrap(),
    //         Duration::from_secs(123)
    //     );

    //     assert_eq!(
    //         parse_interval("interval 123 seconds").unwrap(),
    //         Duration::from_secs(123)
    //     );

    //     assert_eq!(
    //         parse_interval("interval 123 minute").unwrap(),
    //         Duration::from_secs(123 * 60)
    //     );

    //     assert_eq!(
    //         parse_interval("interval 123 minutes").unwrap(),
    //         Duration::from_secs(123 * 60)
    //     );

    //     assert_eq!(
    //         parse_interval("interval 123 hour").unwrap(),
    //         Duration::from_secs(123 * 3600)
    //     );

    //     assert_eq!(
    //         parse_interval("interval 123 hours").unwrap(),
    //         Duration::from_secs(123 * 3600)
    //     );

    //     assert_eq!(
    //         parse_interval("interval 123 day").unwrap(),
    //         Duration::from_secs(123 * 86400)
    //     );

    //     assert_eq!(
    //         parse_interval("interval 123 days").unwrap(),
    //         Duration::from_secs(123 * 86400)
    //     );

    //     assert_eq!(
    //         parse_interval("interval 123 week").unwrap(),
    //         Duration::from_secs(123 * 604800)
    //     );

    //     assert_eq!(
    //         parse_interval("interval 123 week").unwrap(),
    //         Duration::from_secs(123 * 604800)
    //     );
    // }

    // #[test]
    // fn parse_interval_invalid_test() {
    //     assert_eq!(
    //         parse_interval("whatever").err().unwrap(),
    //         DeltaConfigError::Validation("'whatever' is not an interval".to_string())
    //     );

    //     assert_eq!(
    //         parse_interval("interval").err().unwrap(),
    //         DeltaConfigError::Validation("'interval' is not an interval".to_string())
    //     );

    //     assert_eq!(
    //         parse_interval("interval 2").err().unwrap(),
    //         DeltaConfigError::Validation("'interval 2' is not an interval".to_string())
    //     );

    //     assert_eq!(
    //         parse_interval("interval 2 years").err().unwrap(),
    //         DeltaConfigError::Validation("Unknown unit 'years'".to_string())
    //     );

    //     assert_eq!(
    //         parse_interval("interval two years").err().unwrap(),
    //         DeltaConfigError::Validation(
    //             "Cannot parse 'two' as integer: invalid digit found in string".to_string()
    //         )
    //     );

    //     assert_eq!(
    //         parse_interval("interval -25 hours").err().unwrap(),
    //         DeltaConfigError::Validation(
    //             "interval 'interval -25 hours' cannot be negative".to_string()
    //         )
    //     );
    // }

    // #[test]
    // fn test_constraint() {
    //     let md = dummy_metadata();
    //     let config = TableConfig(&md.configuration);

    //     assert_eq!(config.get_constraints().len(), 0);

    //     let mut md = dummy_metadata();
    //     md.configuration.insert(
    //         "delta.constraints.name".to_string(),
    //         "name = 'foo'".to_string(),
    //     );
    //     md.configuration
    //         .insert("delta.constraints.age".to_string(), "age > 10".to_string());
    //     let config = TableConfig(&md.configuration);

    //     let constraints = config.get_constraints();
    //     assert_eq!(constraints.len(), 2);
    //     assert!(constraints.contains(&Constraint::new("name", "name = 'foo'")));
    //     assert!(constraints.contains(&Constraint::new("age", "age > 10")));
    // }

    // #[test]
    // fn test_roundtrip_config_key() {
    //     let cases = [
    //         (DeltaTableProperty::AppendOnly, "delta.appendOnly"),
    //         (
    //             DeltaTableProperty::AutoOptimizeAutoCompact,
    //             "delta.autoOptimize.autoCompact",
    //         ),
    //         (
    //             DeltaTableProperty::AutoOptimizeOptimizeWrite,
    //             "delta.autoOptimize.optimizeWrite",
    //         ),
    //         (
    //             DeltaTableProperty::CheckpointInterval,
    //             "delta.checkpointInterval",
    //         ),
    //         (
    //             DeltaTableProperty::CheckpointWriteStatsAsJson,
    //             "delta.checkpoint.writeStatsAsJson",
    //         ),
    //         (
    //             DeltaTableProperty::CheckpointWriteStatsAsStruct,
    //             "delta.checkpoint.writeStatsAsStruct",
    //         ),
    //         (
    //             DeltaTableProperty::ColumnMappingMode,
    //             "delta.columnMapping.mode",
    //         ),
    //         (
    //             DeltaTableProperty::DataSkippingNumIndexedCols,
    //             "delta.dataSkippingNumIndexedCols",
    //         ),
    //         (
    //             DeltaTableProperty::DataSkippingStatsColumns,
    //             "delta.dataSkippingStatsColumns",
    //         ),
    //         (
    //             DeltaTableProperty::DeletedFileRetentionDuration,
    //             "delta.deletedFileRetentionDuration",
    //         ),
    //         (
    //             DeltaTableProperty::EnableChangeDataFeed,
    //             "delta.enableChangeDataFeed",
    //         ),
    //         (
    //             DeltaTableProperty::EnableDeletionVectors,
    //             "delta.enableDeletionVectors",
    //         ),
    //         (DeltaTableProperty::IsolationLevel, "delta.isolationLevel"),
    //         (
    //             DeltaTableProperty::LogRetentionDuration,
    //             "delta.logRetentionDuration",
    //         ),
    //         (
    //             DeltaTableProperty::MinReaderVersion,
    //             "delta.minReaderVersion",
    //         ),
    //         (
    //             DeltaTableProperty::MinWriterVersion,
    //             "delta.minWriterVersion",
    //         ),
    //         (
    //             DeltaTableProperty::RandomizeFilePrefixes,
    //             "delta.randomizeFilePrefixes",
    //         ),
    //         (
    //             DeltaTableProperty::RandomPrefixLength,
    //             "delta.randomPrefixLength",
    //         ),
    //         (
    //             DeltaTableProperty::SetTransactionRetentionDuration,
    //             "delta.setTransactionRetentionDuration",
    //         ),
    //         (
    //             DeltaTableProperty::EnableExpiredLogCleanup,
    //             "delta.enableExpiredLogCleanup",
    //         ),
    //         (DeltaTableProperty::TargetFileSize, "delta.targetFileSize"),
    //         (
    //             DeltaTableProperty::TuneFileSizesForRewrites,
    //             "delta.tuneFileSizesForRewrites",
    //         ),
    //         (
    //             DeltaTableProperty::CheckpointPolicy,
    //             "delta.checkpointPolicy",
    //         ),
    //     ];

    //     assert_eq!(DeltaTableProperty::VARIANTS.len(), cases.len());

    //     for (key, expected) in cases {
    //         assert_eq!(key.as_ref(), expected);

    //         let serialized = serde_json::to_string(&key).unwrap();
    //         assert_eq!(serialized, format!("\"{}\"", expected));

    //         let deserialized: DeltaTableProperty = serde_json::from_str(&serialized).unwrap();
    //         assert_eq!(deserialized, key);

    //         let from_str: DeltaTableProperty = expected.parse().unwrap();
    //         assert_eq!(from_str, key);
    //     }
    // }

    // #[test]
    // fn test_default_config() {
    //     let md = dummy_metadata();
    //     let config = TableConfig(&md.configuration);

    //     assert_eq!(config.append_only(), false);
    //     // assert_eq!(config.auto_optimize_auto_compact(), false);
    //     assert_eq!(config.auto_optimize_optimize_write(), false);
    //     assert_eq!(config.checkpoint_interval(), 10);
    //     assert_eq!(config.write_stats_as_json(), true);
    //     assert_eq!(config.write_stats_as_struct(), false);
    //     assert_eq!(config.target_file_size(), 104857600);
    //     assert_eq!(config.enable_change_data_feed(), false);
    //     assert_eq!(config.enable_deletion_vectors(), false);
    //     assert_eq!(config.num_indexed_cols(), 32);
    //     assert_eq!(config.enable_expired_log_cleanup(), true);
    // }

    #[test]
    fn it_works() {
        let properties = HashMap::from([("delta.appendOnly".to_string(), "true".to_string())]);
        let de = StringMapDeserializer::new(&properties);
        let actual = TableProperties::deserialize(de).unwrap();
        let expected = TableProperties {
            append_only: true,
            optimize_write: false,
            auto_compact: false,
            checkpoint_interval: 10,
            checkpoint_write_stats_as_json: false,
            checkpoint_write_stats_as_struct: false,
            column_mapping_mode: ColumnMappingMode::None,
            data_skipping_num_indexed_cols: None,
            data_skipping_stats_columns: vec![],
            deleted_file_retention_duration: None,
            enable_change_data_feed: false,
            enable_deletion_vectors: false,
            isolation_level: IsolationLevel::Serializable,
            log_retention_duration: None,
            enable_expired_log_cleanup: false,
            randomize_file_prefixes: false,
            random_prefix_length: None,
            set_transaction_retention_duration: None,
            target_file_size: None,
            tune_file_sizes_for_rewrites: None,
            checkpoint_policy: CheckpointPolicy::Classic,
        };
        assert_eq!(actual, expected);
    }
}

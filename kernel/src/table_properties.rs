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
//
// Delete me?
// Delta table properties are use to configure the table. They are defined in the 'Metadata' action
// under 'configuration' map. Every table must have at exactly one Protocol and one Metadata action.
// Only the most recent is used in the case of multiple P/M actions present in a log segment.
//
// ex: "configuration":{"delta.enableDeletionVectors":"true"}
// parsed into a map of DeltaTableProperty
// = DeltaTableProperty::EnableDeletionVectors -> true

use std::time::Duration;

use serde::Deserialize;
use serde::de::{self, Deserializer};

use crate::error::Error;
use crate::table_features::ColumnMappingMode;

/// Default num index cols
pub const DEFAULT_NUM_INDEX_COLS: i32 = 32;

// #[derive(Debug)]
// struct BoundedInt(i32);
//
// impl<'de> Deserialize<'de> for BoundedInt {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where
//         D: Deserializer<'de>,
//     {
//         let val = i32::deserialize(deserializer)?;
//         if val >= 1 && val <= 10 {
//             Ok(BoundedInt(val))
//         } else {
//             Err(de::Error::custom(format!(
//                 "Value {} is out of bounds (1-10)",
//                 val
//             )))
//         }
//     }
// }

/// Delta table properties
#[derive(Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct TableProperties {
    /// true for this Delta table to be append-only. If append-only,
    /// existing records cannot be deleted, and existing values cannot be updated.
    #[serde(rename = "delta.appendOnly")]
    pub append_only: Option<bool>,

    /// true for Delta Lake to automatically optimize the layout of the files for this Delta table.
    #[serde(rename = "delta.autoOptimize.autoCompact")]
    pub auto_compact: Option<bool>,

    /// true for Delta Lake to automatically optimize the layout of the files for this Delta table
    /// during writes.
    #[serde(rename = "delta.autoOptimize.optimizeWrite")]
    pub optimize_write: Option<bool>,

    /// Interval (expressed as number of commits) after which a new checkpoint should be created.
    /// E.g. if checkpoint interval = 10, then a checkpoint should be written every 10 commits.
    #[serde(rename = "delta.checkpointInterval")]
    pub checkpoint_interval: Option<u64>,

    /// true for Delta Lake to write file statistics in checkpoints in JSON format for the stats column.
    #[serde(rename = "delta.checkpoint.writeStatsAsJson")]
    pub checkpoint_write_stats_as_json: Option<bool>,

    /// true for Delta Lake to write file statistics to checkpoints in struct format for the
    /// stats_parsed column and to write partition values as a struct for partitionValues_parsed.
    #[serde(rename = "delta.checkpoint.writeStatsAsStruct")]
    pub checkpoint_write_stats_as_struct: Option<bool>,

    /// Whether column mapping is enabled for Delta table columns and the corresponding
    /// Parquet columns that use different names.
    #[serde(rename = "delta.columnMapping.mode")]
    pub column_mapping_mode: ColumnMappingMode,

    /// The number of columns for Delta Lake to collect statistics about for data skipping.
    /// A value of -1 means to collect statistics for all columns. Updating this property does
    /// not automatically collect statistics again; instead, it redefines the statistics schema
    /// of the Delta table. Specifically, it changes the behavior of future statistics collection
    /// (such as during appends and optimizations) as well as data skipping (such as ignoring column
    /// statistics beyond this number, even when such statistics exist).
    #[serde(rename = "delta.dataSkippingNumIndexedCols")]
    pub data_skipping_num_indexed_cols: Option<i64>,

    /// A comma-separated list of column names on which Delta Lake collects statistics to enhance
    /// data skipping functionality. This property takes precedence over
    /// [DataSkippingNumIndexedCols](DeltaConfigKey::DataSkippingNumIndexedCols).
    #[serde(rename = "delta.dataSkippingStatsColumns")]
    pub data_skipping_stats_columns: Option<u64>,

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
    pub enable_change_data_feed: Option<bool>,

    /// true to enable deletion vectors and predictive I/O for updates.
    #[serde(rename = "delta.enableDeletionVectors")]
    pub enable_deletion_vectors: Option<bool>,

    /// The degree to which a transaction must be isolated from modifications made by concurrent transactions.
    ///
    /// Valid values are `Serializable` and `WriteSerializable`.
    #[serde(rename = "delta.isolationLevel")]
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

    /// TODO I could not find this property in the documentation, but was defined here and makes sense..?
    #[serde(rename = "delta.enableExpiredLogCleanup")]
    pub enable_expired_log_cleanup: Option<bool>,

    /// The minimum required protocol reader version for a reader that allows to read from this Delta table.
    #[serde(rename = "delta.minReaderVersion")]
    pub min_reader_version: Option<u8>,

    /// The minimum required protocol writer version for a writer that allows to write to this Delta table.
    #[serde(rename = "delta.minWriterVersion")]
    pub min_writer_version: Option<u8>,

    /// true for Delta to generate a random prefix for a file path instead of partition information.
    ///
    /// For example, this may improve Amazon S3 performance when Delta Lake needs to send very high
    /// volumes of Amazon S3 calls to better partition across S3 servers.
    #[serde(rename = "delta.randomizeFilePrefixes")]
    pub randomize_file_prefixes: Option<bool>,

    /// When delta.randomizeFilePrefixes is set to true, the number of characters that Delta
    /// generates for random prefixes.
    #[serde(rename = "delta.randomPrefixLength")]
    pub random_prefix_length: Option<bool>,

    /// The shortest duration within which new snapshots will retain transaction identifiers (for
    /// example, SetTransactions). When a new snapshot sees a transaction identifier older than or
    /// equal to the duration specified by this property, the snapshot considers it expired and
    /// ignores it. The SetTransaction identifier is used when making the writes idempotent.
    #[serde(rename = "delta.setTransactionRetentionDuration")]
    #[serde(deserialize_with = "deserialize_interval")]
    pub set_transaction_retention_duration: Option<Duration>,

    /// The target file size in bytes or higher units for file tuning. For example, 104857600
    /// (bytes) or 100mb.
    #[serde(rename = "delta.targetFileSize")]
    pub target_file_size: Option<u64>,

    /// The target file size in bytes or higher units for file tuning. For example, 104857600
    /// (bytes) or 100mb.
    #[serde(rename = "delta.tuneFileSizesForRewrites")]
    pub tune_file_sizes_for_rewrites: Option<bool>,

    /// 'classic' for classic Delta Lake checkpoints. 'v2' for v2 checkpoints.
    #[serde(rename = "delta.checkpointPolicy")]
    pub checkpoint_policy: Option<CheckpointPolicy>,
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

const SECONDS_PER_MINUTE: u64 = 60;
const SECONDS_PER_HOUR: u64 = 60 * SECONDS_PER_MINUTE;
const SECONDS_PER_DAY: u64 = 24 * SECONDS_PER_HOUR;
const SECONDS_PER_WEEK: u64 = 7 * SECONDS_PER_DAY;

fn deserialize_interval<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    match opt {
        Some(s) => parse_interval(&s).map(Some).map_err(de::Error::custom),
        None => Ok(None),
    }
}

fn parse_interval(value: &str) -> Result<Duration, String> {
    let not_an_interval = || format!("'{value}' is not an interval");

    if !value.starts_with("interval ") {
        return Err(not_an_interval());
    }
    let mut it = value.split_whitespace();
    let _ = it.next(); // skip "interval"
    let number = parse_int(it.next().ok_or_else(not_an_interval)?)?;
    if number < 0 {
        return Err(format!("interval '{value}' cannot be negative"));
    }
    let number = number as u64;

    let duration = match it.next().ok_or_else(not_an_interval)? {
        "nanosecond" | "nanoseconds" => Duration::from_nanos(number),
        "microsecond" | "microseconds" => Duration::from_micros(number),
        "millisecond" | "milliseconds" => Duration::from_millis(number),
        "second" | "seconds" => Duration::from_secs(number),
        "minute" | "minutes" => Duration::from_secs(number * SECONDS_PER_MINUTE),
        "hour" | "hours" => Duration::from_secs(number * SECONDS_PER_HOUR),
        "day" | "days" => Duration::from_secs(number * SECONDS_PER_DAY),
        "week" | "weeks" => Duration::from_secs(number * SECONDS_PER_WEEK),
        unit => {
            return Err(format!("Unknown unit '{unit}'"));
        }
    };

    Ok(duration)
}

fn parse_int(value: &str) -> Result<i64, String> {
    value
        .parse()
        .map_err(|e| format!("Cannot parse '{value}' as integer: {e}"))
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
    fn zach() {

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
}

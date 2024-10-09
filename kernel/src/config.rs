//! Delta Table configuration
use std::collections::HashMap;
use std::time::Duration;

use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display as StrumDisplay, EnumString, VariantNames};

use crate::error::Error;
use crate::features::{ColumnMappingMode, Constraint};

/// Typed property keys that can be defined on a delta table
/// <https://docs.delta.io/latest/table-properties.html#delta-table-properties-reference>
/// <https://learn.microsoft.com/en-us/azure/databricks/delta/table-properties>
#[derive(
    Serialize,
    Deserialize,
    Debug,
    Clone,
    Eq,
    PartialEq,
    EnumString,
    StrumDisplay,
    AsRefStr,
    VariantNames,
    Hash,
)]
#[non_exhaustive]
pub enum DeltaConfigKey {
    /// true for this Delta table to be append-only. If append-only,
    /// existing records cannot be deleted, and existing values cannot be updated.
    #[strum(serialize = "delta.appendOnly")]
    #[serde(rename = "delta.appendOnly")]
    AppendOnly,

    /// true for Delta Lake to automatically optimize the layout of the files for this Delta table.
    #[strum(serialize = "delta.autoOptimize.autoCompact")]
    #[serde(rename = "delta.autoOptimize.autoCompact")]
    AutoOptimizeAutoCompact,

    /// true for Delta Lake to automatically optimize the layout of the files for this Delta table during writes.
    #[strum(serialize = "delta.autoOptimize.optimizeWrite")]
    #[serde(rename = "delta.autoOptimize.optimizeWrite")]
    AutoOptimizeOptimizeWrite,

    /// Interval (number of commits) after which a new checkpoint should be created
    #[strum(serialize = "delta.checkpointInterval")]
    #[serde(rename = "delta.checkpointInterval")]
    CheckpointInterval,

    /// true for Delta Lake to write file statistics in checkpoints in JSON format for the stats column.
    #[strum(serialize = "delta.checkpoint.writeStatsAsJson")]
    #[serde(rename = "delta.checkpoint.writeStatsAsJson")]
    CheckpointWriteStatsAsJson,

    /// true for Delta Lake to write file statistics to checkpoints in struct format for the
    /// stats_parsed column and to write partition values as a struct for partitionValues_parsed.
    #[strum(serialize = "delta.checkpoint.writeStatsAsStruct")]
    #[serde(rename = "delta.checkpoint.writeStatsAsStruct")]
    CheckpointWriteStatsAsStruct,

    /// Whether column mapping is enabled for Delta table columns and the corresponding
    /// Parquet columns that use different names.
    #[strum(serialize = "delta.columnMapping.mode")]
    #[serde(rename = "delta.columnMapping.mode")]
    ColumnMappingMode,

    /// The number of columns for Delta Lake to collect statistics about for data skipping.
    /// A value of -1 means to collect statistics for all columns. Updating this property does
    /// not automatically collect statistics again; instead, it redefines the statistics schema
    /// of the Delta table. Specifically, it changes the behavior of future statistics collection
    /// (such as during appends and optimizations) as well as data skipping (such as ignoring column
    /// statistics beyond this number, even when such statistics exist).
    #[strum(serialize = "delta.dataSkippingNumIndexedCols")]
    #[serde(rename = "delta.dataSkippingNumIndexedCols")]
    DataSkippingNumIndexedCols,

    /// A comma-separated list of column names on which Delta Lake collects statistics to enhance
    /// data skipping functionality. This property takes precedence over
    /// [DataSkippingNumIndexedCols](DeltaConfigKey::DataSkippingNumIndexedCols).
    #[strum(serialize = "delta.dataSkippingStatsColumns")]
    #[serde(rename = "delta.dataSkippingStatsColumns")]
    DataSkippingStatsColumns,

    /// The shortest duration for Delta Lake to keep logically deleted data files before deleting
    /// them physically. This is to prevent failures in stale readers after compactions or partition overwrites.
    ///
    /// This value should be large enough to ensure that:
    ///
    /// * It is larger than the longest possible duration of a job if you run VACUUM when there are
    ///   concurrent readers or writers accessing the Delta table.
    /// * If you run a streaming query that reads from the table, that query does not stop for longer
    ///   than this value. Otherwise, the query may not be able to restart, as it must still read old files.
    #[strum(serialize = "delta.deletedFileRetentionDuration")]
    #[serde(rename = "delta.deletedFileRetentionDuration")]
    DeletedFileRetentionDuration,

    /// true to enable change data feed.
    #[strum(serialize = "delta.enableChangeDataFeed")]
    #[serde(rename = "delta.enableChangeDataFeed")]
    EnableChangeDataFeed,

    /// true to enable deletion vectors and predictive I/O for updates.
    #[strum(serialize = "delta.enableDeletionVectors")]
    #[serde(rename = "delta.enableDeletionVectors")]
    EnableDeletionVectors,

    /// The degree to which a transaction must be isolated from modifications made by concurrent transactions.
    ///
    /// Valid values are `Serializable` and `WriteSerializable`.
    #[strum(serialize = "delta.isolationLevel")]
    #[serde(rename = "delta.isolationLevel")]
    IsolationLevel,

    /// How long the history for a Delta table is kept.
    ///
    /// Each time a checkpoint is written, Delta Lake automatically cleans up log entries older
    /// than the retention interval. If you set this property to a large enough value, many log
    /// entries are retained. This should not impact performance as operations against the log are
    /// constant time. Operations on history are parallel but will become more expensive as the log size increases.
    #[strum(serialize = "delta.logRetentionDuration")]
    #[serde(rename = "delta.logRetentionDuration")]
    LogRetentionDuration,

    /// TODO I could not find this property in the documentation, but was defined here and makes sense..?
    #[strum(serialize = "delta.enableExpiredLogCleanup")]
    #[serde(rename = "delta.enableExpiredLogCleanup")]
    EnableExpiredLogCleanup,

    /// The minimum required protocol reader version for a reader that allows to read from this Delta table.
    #[strum(serialize = "delta.minReaderVersion")]
    #[serde(rename = "delta.minReaderVersion")]
    MinReaderVersion,

    /// The minimum required protocol writer version for a writer that allows to write to this Delta table.
    #[strum(serialize = "delta.minWriterVersion")]
    #[serde(rename = "delta.minWriterVersion")]
    MinWriterVersion,

    /// true for Delta Lake to generate a random prefix for a file path instead of partition information.
    ///
    /// For example, this ma
    /// y improve Amazon S3 performance when Delta Lake needs to send very high volumes
    /// of Amazon S3 calls to better partition across S3 servers.
    #[strum(serialize = "delta.randomizeFilePrefixes")]
    #[serde(rename = "delta.randomizeFilePrefixes")]
    RandomizeFilePrefixes,

    /// When delta.randomizeFilePrefixes is set to true, the number of characters that Delta Lake generates for random prefixes.
    #[strum(serialize = "delta.randomPrefixLength")]
    #[serde(rename = "delta.randomPrefixLength")]
    RandomPrefixLength,

    /// The shortest duration within which new snapshots will retain transaction identifiers (for example, SetTransactions).
    /// When a new snapshot sees a transaction identifier older than or equal to the duration specified by this property,
    /// the snapshot considers it expired and ignores it. The SetTransaction identifier is used when making the writes idempotent.
    #[strum(serialize = "delta.setTransactionRetentionDuration")]
    #[serde(rename = "delta.setTransactionRetentionDuration")]
    SetTransactionRetentionDuration,

    /// The target file size in bytes or higher units for file tuning. For example, 104857600 (bytes) or 100mb.
    #[strum(serialize = "delta.targetFileSize")]
    #[serde(rename = "delta.targetFileSize")]
    TargetFileSize,

    /// The target file size in bytes or higher units for file tuning. For example, 104857600 (bytes) or 100mb.
    #[strum(serialize = "delta.tuneFileSizesForRewrites")]
    #[serde(rename = "delta.tuneFileSizesForRewrites")]
    TuneFileSizesForRewrites,

    /// 'classic' for classic Delta Lake checkpoints. 'v2' for v2 checkpoints.
    #[strum(serialize = "delta.checkpointPolicy")]
    #[serde(rename = "delta.checkpointPolicy")]
    CheckpointPolicy,
}

/// Delta configuration error
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum DeltaConfigError {
    /// Error returned when configuration validation failed.
    #[error("Validation failed - {0}")]
    Validation(String),
}

impl From<DeltaConfigError> for Error {
    fn from(e: DeltaConfigError) -> Self {
        Error::InvalidConfiguration(e.to_string())
    }
}

macro_rules! table_config {
    ($(($docs:literal, $key:expr, $name:ident, $ret:ty, $default:literal),)*) => {
        $(
            #[doc = $docs]
            pub fn $name(&self) -> $ret {
                self.0
                    .get($key.as_ref())
                    .and_then(|value| value.parse().ok())
                    .unwrap_or($default)
            }
        )*
    }
}

/// Well known delta table configuration
pub struct TableConfig<'a>(pub(crate) &'a HashMap<String, String>);

/// Default num index cols
pub const DEFAULT_NUM_INDEX_COLS: i32 = 32;

impl<'a> TableConfig<'a> {
    table_config!(
        (
            "true for this Delta table to be append-only",
            DeltaConfigKey::AppendOnly,
            append_only,
            bool,
            false
        ),
        (
            "true for this Delta table to be append-only",
            DeltaConfigKey::AutoOptimizeOptimizeWrite,
            auto_optimize_optimize_write,
            bool,
            false
        ),
        (
            "true for Delta Lake to write file statistics in checkpoints in JSON format for the stats column.",
            DeltaConfigKey::CheckpointWriteStatsAsJson,
            write_stats_as_json,
            bool,
            true
        ),
        (
            "true for Delta Lake to write file statistics to checkpoints in struct format",
            DeltaConfigKey::CheckpointWriteStatsAsStruct,
            write_stats_as_struct,
            bool,
            false
        ),
        (
            "The target file size in bytes or higher units for file tuning",
            DeltaConfigKey::TargetFileSize,
            target_file_size,
            i64,
            // Databricks / spark defaults to 104857600 (bytes) or 100mb
            104857600
        ),
        (
            "true to enable change data feed.",
            DeltaConfigKey::EnableChangeDataFeed,
            enable_change_data_feed,
            bool,
            false
        ),
        (
            "true to enable deletion vectors and predictive I/O for updates.",
            DeltaConfigKey::EnableDeletionVectors,
            enable_deletion_vectors,
            bool,
            // in databricks the default is dependent on the workspace settings and runtime version
            // https://learn.microsoft.com/en-us/azure/databricks/administration-guide/workspace-settings/deletion-vectors
            false
        ),
        (
            "The number of columns for Delta Lake to collect statistics about for data skipping.",
            DeltaConfigKey::DataSkippingNumIndexedCols,
            num_indexed_cols,
            i32,
            32
        ),
        (
            "whether to cleanup expired logs",
            DeltaConfigKey::EnableExpiredLogCleanup,
            enable_expired_log_cleanup,
            bool,
            true
        ),
        (
            "Interval (number of commits) after which a new checkpoint should be created",
            DeltaConfigKey::CheckpointInterval,
            checkpoint_interval,
            i32,
            10
        ),
    );

    /// The shortest duration for Delta Lake to keep logically deleted data files before deleting
    /// them physically. This is to prevent failures in stale readers after compactions or partition overwrites.
    ///
    /// This value should be large enough to ensure that:
    ///
    /// * It is larger than the longest possible duration of a job if you run VACUUM when there are
    ///   concurrent readers or writers accessing the Delta table.
    /// * If you run a streaming query that reads from the table, that query does not stop for longer
    ///   than this value. Otherwise, the query may not be able to restart, as it must still read old files.
    pub fn deleted_file_retention_duration(&self) -> Duration {
        lazy_static! {
            static ref DEFAULT_DURATION: Duration = parse_interval("interval 1 weeks").unwrap();
        }
        self.0
            .get(DeltaConfigKey::DeletedFileRetentionDuration.as_ref())
            .and_then(|v| parse_interval(v).ok())
            .unwrap_or_else(|| DEFAULT_DURATION.to_owned())
    }

    /// How long the history for a Delta table is kept.
    ///
    /// Each time a checkpoint is written, Delta Lake automatically cleans up log entries older
    /// than the retention interval. If you set this property to a large enough value, many log
    /// entries are retained. This should not impact performance as operations against the log are
    /// constant time. Operations on history are parallel but will become more expensive as the log size increases.
    pub fn log_retention_duration(&self) -> Duration {
        lazy_static! {
            static ref DEFAULT_DURATION: Duration = parse_interval("interval 30 days").unwrap();
        }
        self.0
            .get(DeltaConfigKey::LogRetentionDuration.as_ref())
            .and_then(|v| parse_interval(v).ok())
            .unwrap_or_else(|| DEFAULT_DURATION.to_owned())
    }

    /// The degree to which a transaction must be isolated from modifications made by concurrent transactions.
    ///
    /// Valid values are `Serializable` and `WriteSerializable`.
    pub fn isolation_level(&self) -> IsolationLevel {
        self.0
            .get(DeltaConfigKey::IsolationLevel.as_ref())
            .and_then(|v| v.parse().ok())
            .unwrap_or_default()
    }

    /// Policy applied during chepoint creation
    pub fn checkpoint_policy(&self) -> CheckpointPolicy {
        self.0
            .get(DeltaConfigKey::CheckpointPolicy.as_ref())
            .and_then(|v| v.parse().ok())
            .unwrap_or_default()
    }

    /// Return the column mapping mode according to delta.columnMapping.mode
    pub fn column_mapping_mode(&self) -> ColumnMappingMode {
        self.0
            .get(DeltaConfigKey::ColumnMappingMode.as_ref())
            .and_then(|v| v.parse().ok())
            .unwrap_or_default()
    }

    /// Return the check constraints on the current table
    pub fn get_constraints(&self) -> Vec<Constraint> {
        self.0
            .iter()
            .filter_map(|(field, value)| {
                if field.starts_with("delta.constraints") {
                    field
                        .splitn(3, '.')
                        .last()
                        .map(|n| Constraint::new(n, value))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Column names on which Delta Lake collects statistics to enhance data skipping functionality.
    /// This property takes precedence over [num_indexed_cols](Self::num_indexed_cols).
    pub fn stats_columns(&self) -> Option<Vec<&str>> {
        self.0
            .get(DeltaConfigKey::DataSkippingStatsColumns.as_ref())
            .map(|v| v.split(',').collect())
    }
}

#[derive(
    Serialize, Deserialize, Debug, Copy, Clone, PartialEq, EnumString, StrumDisplay, AsRefStr,
)]
/// The isolation level applied during transaction
pub enum IsolationLevel {
    /// The strongest isolation level. It ensures that committed write operations
    /// and all reads are Serializable. Operations are allowed as long as there
    /// exists a serial sequence of executing them one-at-a-time that generates
    /// the same outcome as that seen in the table. For the write operations,
    /// the serial sequence is exactly the same as that seen in the table’s history.
    #[strum(ascii_case_insensitive)]
    Serializable,

    /// A weaker isolation level than Serializable. It ensures only that the write
    /// operations (that is, not reads) are serializable. However, this is still stronger
    /// than Snapshot isolation. WriteSerializable is the default isolation level because
    /// it provides great balance of data consistency and availability for most common operations.
    #[strum(ascii_case_insensitive)]
    WriteSerializable,

    /// SnapshotIsolation is a guarantee that all reads made in a transaction will see a consistent
    /// snapshot of the database (in practice it reads the last committed values that existed at the
    /// time it started), and the transaction itself will successfully commit only if no updates
    /// it has made conflict with any concurrent updates made since that snapshot.
    #[strum(ascii_case_insensitive)]
    SnapshotIsolation,
}

// Spark assumes Serializable as default isolation level
// https://github.com/delta-io/delta/blob/abb171c8401200e7772b27e3be6ea8682528ac72/core/src/main/scala/org/apache/spark/sql/delta/OptimisticTransaction.scala#L1023
impl Default for IsolationLevel {
    fn default() -> Self {
        Self::Serializable
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, EnumString, StrumDisplay, AsRefStr)]
/// The checkpoint policy applied when writing checkpoints
#[serde(rename_all = "camelCase")]
#[strum(serialize_all = "camelCase")]
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

fn parse_interval(value: &str) -> Result<Duration, DeltaConfigError> {
    let not_an_interval = || DeltaConfigError::Validation(format!("'{value}' is not an interval"));

    if !value.starts_with("interval ") {
        return Err(not_an_interval());
    }
    let mut it = value.split_whitespace();
    let _ = it.next(); // skip "interval"
    let number = parse_int(it.next().ok_or_else(not_an_interval)?)?;
    if number < 0 {
        return Err(DeltaConfigError::Validation(format!(
            "interval '{value}' cannot be negative"
        )));
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
            return Err(DeltaConfigError::Validation(format!(
                "Unknown unit '{unit}'"
            )));
        }
    };

    Ok(duration)
}

fn parse_int(value: &str) -> Result<i64, DeltaConfigError> {
    value.parse().map_err(|e| {
        DeltaConfigError::Validation(format!("Cannot parse '{value}' as integer: {e}"))
    })
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
    fn get_interval_from_metadata_test() {
        let md = dummy_metadata();
        let config = TableConfig(&md.configuration);

        // default 1 week
        assert_eq!(
            config.deleted_file_retention_duration().as_secs(),
            SECONDS_PER_WEEK,
        );

        // change to 2 day
        let mut md = dummy_metadata();
        md.configuration.insert(
            DeltaConfigKey::DeletedFileRetentionDuration
                .as_ref()
                .to_string(),
            "interval 2 day".to_string(),
        );
        let config = TableConfig(&md.configuration);

        assert_eq!(
            config.deleted_file_retention_duration().as_secs(),
            2 * SECONDS_PER_DAY,
        );
    }

    #[test]
    fn get_long_from_metadata_test() {
        let md = dummy_metadata();
        let config = TableConfig(&md.configuration);
        assert_eq!(config.checkpoint_interval(), 10,)
    }

    #[test]
    fn get_boolean_from_metadata_test() {
        let md = dummy_metadata();
        let config = TableConfig(&md.configuration);

        // default value is true
        assert!(config.enable_expired_log_cleanup());

        // change to false
        let mut md = dummy_metadata();
        md.configuration.insert(
            DeltaConfigKey::EnableExpiredLogCleanup.as_ref().into(),
            "false".to_string(),
        );
        let config = TableConfig(&md.configuration);

        assert!(!config.enable_expired_log_cleanup());
    }

    #[test]
    fn parse_interval_test() {
        assert_eq!(
            parse_interval("interval 123 nanosecond").unwrap(),
            Duration::from_nanos(123)
        );

        assert_eq!(
            parse_interval("interval 123 nanoseconds").unwrap(),
            Duration::from_nanos(123)
        );

        assert_eq!(
            parse_interval("interval 123 microsecond").unwrap(),
            Duration::from_micros(123)
        );

        assert_eq!(
            parse_interval("interval 123 microseconds").unwrap(),
            Duration::from_micros(123)
        );

        assert_eq!(
            parse_interval("interval 123 millisecond").unwrap(),
            Duration::from_millis(123)
        );

        assert_eq!(
            parse_interval("interval 123 milliseconds").unwrap(),
            Duration::from_millis(123)
        );

        assert_eq!(
            parse_interval("interval 123 second").unwrap(),
            Duration::from_secs(123)
        );

        assert_eq!(
            parse_interval("interval 123 seconds").unwrap(),
            Duration::from_secs(123)
        );

        assert_eq!(
            parse_interval("interval 123 minute").unwrap(),
            Duration::from_secs(123 * 60)
        );

        assert_eq!(
            parse_interval("interval 123 minutes").unwrap(),
            Duration::from_secs(123 * 60)
        );

        assert_eq!(
            parse_interval("interval 123 hour").unwrap(),
            Duration::from_secs(123 * 3600)
        );

        assert_eq!(
            parse_interval("interval 123 hours").unwrap(),
            Duration::from_secs(123 * 3600)
        );

        assert_eq!(
            parse_interval("interval 123 day").unwrap(),
            Duration::from_secs(123 * 86400)
        );

        assert_eq!(
            parse_interval("interval 123 days").unwrap(),
            Duration::from_secs(123 * 86400)
        );

        assert_eq!(
            parse_interval("interval 123 week").unwrap(),
            Duration::from_secs(123 * 604800)
        );

        assert_eq!(
            parse_interval("interval 123 week").unwrap(),
            Duration::from_secs(123 * 604800)
        );
    }

    #[test]
    fn parse_interval_invalid_test() {
        assert_eq!(
            parse_interval("whatever").err().unwrap(),
            DeltaConfigError::Validation("'whatever' is not an interval".to_string())
        );

        assert_eq!(
            parse_interval("interval").err().unwrap(),
            DeltaConfigError::Validation("'interval' is not an interval".to_string())
        );

        assert_eq!(
            parse_interval("interval 2").err().unwrap(),
            DeltaConfigError::Validation("'interval 2' is not an interval".to_string())
        );

        assert_eq!(
            parse_interval("interval 2 years").err().unwrap(),
            DeltaConfigError::Validation("Unknown unit 'years'".to_string())
        );

        assert_eq!(
            parse_interval("interval two years").err().unwrap(),
            DeltaConfigError::Validation(
                "Cannot parse 'two' as integer: invalid digit found in string".to_string()
            )
        );

        assert_eq!(
            parse_interval("interval -25 hours").err().unwrap(),
            DeltaConfigError::Validation(
                "interval 'interval -25 hours' cannot be negative".to_string()
            )
        );
    }

    #[test]
    fn test_constraint() {
        let md = dummy_metadata();
        let config = TableConfig(&md.configuration);

        assert_eq!(config.get_constraints().len(), 0);

        let mut md = dummy_metadata();
        md.configuration.insert(
            "delta.constraints.name".to_string(),
            "name = 'foo'".to_string(),
        );
        md.configuration
            .insert("delta.constraints.age".to_string(), "age > 10".to_string());
        let config = TableConfig(&md.configuration);

        let constraints = config.get_constraints();
        assert_eq!(constraints.len(), 2);
        assert!(constraints.contains(&Constraint::new("name", "name = 'foo'")));
        assert!(constraints.contains(&Constraint::new("age", "age > 10")));
    }

    #[test]
    fn test_roundtrip_config_key() {
        let cases = [
            (DeltaConfigKey::AppendOnly, "delta.appendOnly"),
            (
                DeltaConfigKey::AutoOptimizeAutoCompact,
                "delta.autoOptimize.autoCompact",
            ),
            (
                DeltaConfigKey::AutoOptimizeOptimizeWrite,
                "delta.autoOptimize.optimizeWrite",
            ),
            (
                DeltaConfigKey::CheckpointInterval,
                "delta.checkpointInterval",
            ),
            (
                DeltaConfigKey::CheckpointWriteStatsAsJson,
                "delta.checkpoint.writeStatsAsJson",
            ),
            (
                DeltaConfigKey::CheckpointWriteStatsAsStruct,
                "delta.checkpoint.writeStatsAsStruct",
            ),
            (
                DeltaConfigKey::ColumnMappingMode,
                "delta.columnMapping.mode",
            ),
            (
                DeltaConfigKey::DataSkippingNumIndexedCols,
                "delta.dataSkippingNumIndexedCols",
            ),
            (
                DeltaConfigKey::DataSkippingStatsColumns,
                "delta.dataSkippingStatsColumns",
            ),
            (
                DeltaConfigKey::DeletedFileRetentionDuration,
                "delta.deletedFileRetentionDuration",
            ),
            (
                DeltaConfigKey::EnableChangeDataFeed,
                "delta.enableChangeDataFeed",
            ),
            (
                DeltaConfigKey::EnableDeletionVectors,
                "delta.enableDeletionVectors",
            ),
            (DeltaConfigKey::IsolationLevel, "delta.isolationLevel"),
            (
                DeltaConfigKey::LogRetentionDuration,
                "delta.logRetentionDuration",
            ),
            (DeltaConfigKey::MinReaderVersion, "delta.minReaderVersion"),
            (DeltaConfigKey::MinWriterVersion, "delta.minWriterVersion"),
            (
                DeltaConfigKey::RandomizeFilePrefixes,
                "delta.randomizeFilePrefixes",
            ),
            (
                DeltaConfigKey::RandomPrefixLength,
                "delta.randomPrefixLength",
            ),
            (
                DeltaConfigKey::SetTransactionRetentionDuration,
                "delta.setTransactionRetentionDuration",
            ),
            (
                DeltaConfigKey::EnableExpiredLogCleanup,
                "delta.enableExpiredLogCleanup",
            ),
            (DeltaConfigKey::TargetFileSize, "delta.targetFileSize"),
            (
                DeltaConfigKey::TuneFileSizesForRewrites,
                "delta.tuneFileSizesForRewrites",
            ),
            (DeltaConfigKey::CheckpointPolicy, "delta.checkpointPolicy"),
        ];

        assert_eq!(DeltaConfigKey::VARIANTS.len(), cases.len());

        for (key, expected) in cases {
            assert_eq!(key.as_ref(), expected);

            let serialized = serde_json::to_string(&key).unwrap();
            assert_eq!(serialized, format!("\"{}\"", expected));

            let deserialized: DeltaConfigKey = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, key);

            let from_str: DeltaConfigKey = expected.parse().unwrap();
            assert_eq!(from_str, key);
        }
    }

    #[test]
    fn test_default_config() {
        let md = dummy_metadata();
        let config = TableConfig(&md.configuration);

        assert_eq!(config.append_only(), false);
        // assert_eq!(config.auto_optimize_auto_compact(), false);
        assert_eq!(config.auto_optimize_optimize_write(), false);
        assert_eq!(config.checkpoint_interval(), 10);
        assert_eq!(config.write_stats_as_json(), true);
        assert_eq!(config.write_stats_as_struct(), false);
        assert_eq!(config.target_file_size(), 104857600);
        assert_eq!(config.enable_change_data_feed(), false);
        assert_eq!(config.enable_deletion_vectors(), false);
        assert_eq!(config.num_indexed_cols(), 32);
        assert_eq!(config.enable_expired_log_cleanup(), true);
    }
}

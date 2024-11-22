//! For now we just use simple functions to deserialize table properties from strings. This allows
//! us to relatively simply implement the functionality described in the protocol and expose
//! 'simple' types to the user in the [`TableProperties`] struct. E.g. we can expose a `bool`
//! directly instead of a `BoolConfig` type that we implement `Deserialize` for.
use std::time::Duration;

use super::*;
use crate::expressions::ColumnName;
use crate::table_features::ColumnMappingMode;
use crate::utils::require;

const SECONDS_PER_MINUTE: u64 = 60;
const SECONDS_PER_HOUR: u64 = 60 * SECONDS_PER_MINUTE;
const SECONDS_PER_DAY: u64 = 24 * SECONDS_PER_HOUR;
const SECONDS_PER_WEEK: u64 = 7 * SECONDS_PER_DAY;

impl<K, V, I> From<I> for TableProperties
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: AsRef<str>,
{
    fn from(unparsed: I) -> Self {
        let mut props = TableProperties::default();
        for (k, v) in unparsed {
            let parsed =
                match k.as_ref() {
                    "delta.appendOnly" => {
                        parse_bool(v.as_ref()).map(|val| props.append_only = Some(val))
                    }
                    "delta.autoOptimize.autoCompact" => {
                        parse_bool(v.as_ref()).map(|val| props.auto_compact = Some(val))
                    }
                    "delta.autoOptimize.optimizeWrite" => {
                        parse_bool(v.as_ref()).map(|val| props.optimize_write = Some(val))
                    }
                    "delta.checkpointInterval" => parse_positive_int(v.as_ref())
                        .map(|val| props.checkpoint_interval = Some(val)),
                    "delta.checkpoint.writeStatsAsJson" => parse_bool(v.as_ref())
                        .map(|val| props.checkpoint_write_stats_as_json = Some(val)),
                    "delta.checkpoint.writeStatsAsStruct" => parse_bool(v.as_ref())
                        .map(|val| props.checkpoint_write_stats_as_struct = Some(val)),
                    "delta.columnMapping.mode" => ColumnMappingMode::try_from(v.as_ref())
                        .map(|val| props.column_mapping_mode = Some(val))
                        .ok(),
                    "delta.dataSkippingNumIndexedCols" => {
                        DataSkippingNumIndexedCols::try_from(v.as_ref())
                            .map(|val| props.data_skipping_num_indexed_cols = Some(val))
                            .ok()
                    }
                    "delta.dataSkippingStatsColumns" => parse_column_names(v.as_ref())
                        .map(|val| props.data_skipping_stats_columns = Some(val)),
                    "delta.deletedFileRetentionDuration" => parse_interval(v.as_ref())
                        .map(|val| props.deleted_file_retention_duration = Some(val)),
                    "delta.enableChangeDataFeed" => {
                        parse_bool(v.as_ref()).map(|val| props.enable_change_data_feed = Some(val))
                    }
                    "delta.enableDeletionVectors" => {
                        parse_bool(v.as_ref()).map(|val| props.enable_deletion_vectors = Some(val))
                    }
                    "delta.isolationLevel" => IsolationLevel::try_from(v.as_ref())
                        .map(|val| props.isolation_level = Some(val))
                        .ok(),
                    "delta.logRetentionDuration" => parse_interval(v.as_ref())
                        .map(|val| props.log_retention_duration = Some(val)),
                    "delta.enableExpiredLogCleanup" => parse_bool(v.as_ref())
                        .map(|val| props.enable_expired_log_cleanup = Some(val)),
                    "delta.randomizeFilePrefixes" => {
                        parse_bool(v.as_ref()).map(|val| props.randomize_file_prefixes = Some(val))
                    }
                    "delta.randomPrefixLength" => parse_positive_int(v.as_ref())
                        .map(|val| props.random_prefix_length = Some(val)),
                    "delta.setTransactionRetentionDuration" => parse_interval(v.as_ref())
                        .map(|val| props.set_transaction_retention_duration = Some(val)),
                    "delta.targetFileSize" => {
                        parse_positive_int(v.as_ref()).map(|val| props.target_file_size = Some(val))
                    }
                    "delta.tuneFileSizesForRewrites" => parse_bool(v.as_ref())
                        .map(|val| props.tune_file_sizes_for_rewrites = Some(val)),
                    "delta.checkpointPolicy" => CheckpointPolicy::try_from(v.as_ref())
                        .map(|val| props.checkpoint_policy = Some(val))
                        .ok(),
                    "delta.enableRowTracking" => {
                        parse_bool(v.as_ref()).map(|val| props.enable_row_tracking = Some(val))
                    }
                    _ => None,
                };
            if parsed.is_none() {
                props
                    .unknown_properties
                    .insert(k.as_ref().to_string(), v.as_ref().to_string());
            }
        }
        props
    }
}

/// Deserialize a string representing a positive integer into an `Option<u64>`. Returns `Some` if
/// successfully parses, and `None` otherwise.
pub(crate) fn parse_positive_int(s: &str) -> Option<i64> {
    // parse to i64 (then check n > 0) since java doesn't even allow u64
    let n: i64 = s.parse().ok()?;
    if n > 0 {
        Some(n)
    } else {
        None
    }
}

/// Deserialize a string representing a boolean into an `Option<bool>`. Returns `Some` if
/// successfully parses, and `None` otherwise.
pub(crate) fn parse_bool(s: &str) -> Option<bool> {
    match s {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

/// Deserialize a comma-separated list of column names into an `Option<Vec<ColumnName>>`. Returns
/// `Some` if successfully parses, and `None` otherwise.
pub(crate) fn parse_column_names(s: &str) -> Option<Vec<ColumnName>> {
    ColumnName::parse_column_name_list(&s).ok()
}

/// Deserialize an interval string of the form "interval 5 days" into an `Option<Duration>`.
/// Returns `Some` if successfully parses, and `None` otherwise.
pub(crate) fn parse_interval(s: &str) -> Option<Duration> {
    parse_interval_impl(s).ok()
}

#[derive(thiserror::Error, Debug)]
pub enum ParseIntervalError {
    /// The input string is not a valid interval
    #[error("'{0}' is not an interval")]
    NotAnInterval(String),
    /// Couldn't parse the input string as an integer
    #[error("Unable to parse '{0}' as an integer")]
    ParseIntError(String),
    /// Negative intervals aren't supported
    #[error("Interval '{0}' cannot be negative")]
    NegativeInterval(String),
    /// Unsupported interval
    #[error("Unsupported interval '{0}'")]
    UnsupportedInterval(String),
    /// Unknown unit
    #[error("Unknown interval unit '{0}'")]
    UnknownUnit(String),
}

/// This is effectively a simpler version of spark's `CalendarInterval` parser. See spark's
/// `stringToInterval`:
/// https://github.com/apache/spark/blob/5a57efdcee9e6569d8de4272bda258788cf349e3/sql/api/src/main/scala/org/apache/spark/sql/catalyst/util/SparkIntervalUtils.scala#L134
///
/// Notably we don't support months nor years, nor do we support fractional values, and negative
/// intervals aren't supported.
///
/// For now this is adapted from delta-rs' `parse_interval` function:
/// https://github.com/delta-io/delta-rs/blob/d4f18b3ae9d616e771b5d0e0fa498d0086fd91eb/crates/core/src/table/config.rs#L474
///
/// See issue delta-kernel-rs/#507 for details: https://github.com/delta-io/delta-kernel-rs/issues/507
fn parse_interval_impl(value: &str) -> Result<Duration, ParseIntervalError> {
    let mut it = value.split_whitespace();
    if it.next() != Some("interval") {
        return Err(ParseIntervalError::NotAnInterval(value.to_string()));
    }
    let number = it
        .next()
        .ok_or_else(|| ParseIntervalError::NotAnInterval(value.to_string()))?;
    let number: i64 = number
        .parse()
        .map_err(|_| ParseIntervalError::ParseIntError(number.into()))?;

    // TODO(zach): spark allows negative intervals, but we don't
    require!(
        number >= 0,
        ParseIntervalError::NegativeInterval(value.to_string())
    );

    // convert to u64 since Duration expects it
    let number = number as u64; // non-negative i64 => always safe

    let duration = match it
        .next()
        .ok_or_else(|| ParseIntervalError::NotAnInterval(value.to_string()))?
    {
        "nanosecond" | "nanoseconds" => Duration::from_nanos(number),
        "microsecond" | "microseconds" => Duration::from_micros(number),
        "millisecond" | "milliseconds" => Duration::from_millis(number),
        "second" | "seconds" => Duration::from_secs(number),
        "minute" | "minutes" => Duration::from_secs(number * SECONDS_PER_MINUTE),
        "hour" | "hours" => Duration::from_secs(number * SECONDS_PER_HOUR),
        "day" | "days" => Duration::from_secs(number * SECONDS_PER_DAY),
        "week" | "weeks" => Duration::from_secs(number * SECONDS_PER_WEEK),
        unit @ ("month" | "months") => {
            return Err(ParseIntervalError::UnsupportedInterval(unit.to_string()));
        }
        unit => {
            return Err(ParseIntervalError::UnknownUnit(unit.to_string()));
        }
    };

    Ok(duration)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_interval() {
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
    fn test_invalid_parse_interval() {
        assert_eq!(
            parse_interval_impl("whatever").err().unwrap().to_string(),
            "'whatever' is not an interval".to_string()
        );

        assert_eq!(
            parse_interval_impl("interval").err().unwrap().to_string(),
            "'interval' is not an interval".to_string()
        );

        assert_eq!(
            parse_interval_impl("interval 2").err().unwrap().to_string(),
            "'interval 2' is not an interval".to_string()
        );

        assert_eq!(
            parse_interval_impl("interval 2 months")
                .err()
                .unwrap()
                .to_string(),
            "Unsupported interval 'months'".to_string()
        );

        assert_eq!(
            parse_interval_impl("interval 2 years")
                .err()
                .unwrap()
                .to_string(),
            "Unknown interval unit 'years'".to_string()
        );

        assert_eq!(
            parse_interval_impl("interval two years")
                .err()
                .unwrap()
                .to_string(),
            "Unable to parse 'two' as an integer".to_string()
        );

        assert_eq!(
            parse_interval_impl("interval -25 hours")
                .err()
                .unwrap()
                .to_string(),
            "Interval 'interval -25 hours' cannot be negative".to_string()
        );
    }
}

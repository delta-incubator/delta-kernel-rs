//! For now we just use simple functions to deserialize table properties from strings. This allows
//! us to relatively simply implement the functionality described in the protocol and expose
//! 'simple' types to the user in the [`TableProperties`] struct. E.g. we can expose a `bool`
//! directly instead of a `BoolConfig` type that we implement `Deserialize` for.

use crate::expressions::ColumnName;

use std::time::Duration;

use serde::de::{self, Deserializer};
use serde::Deserialize;

const SECONDS_PER_MINUTE: u64 = 60;
const SECONDS_PER_HOUR: u64 = 60 * SECONDS_PER_MINUTE;
const SECONDS_PER_DAY: u64 = 24 * SECONDS_PER_HOUR;
const SECONDS_PER_WEEK: u64 = 7 * SECONDS_PER_DAY;

/// Transparently pass through the option deserialization to the inner type.
pub(crate) fn deserialize_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

/// Deserialize a string representing a positive integer into an `Option<u64>`.
pub(crate) fn deserialize_pos_int<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    let n: u64 = s.parse().map_err(de::Error::custom)?;
    if n == 0 {
        return Err(de::Error::custom("expected a positive integer"));
    }
    Ok(Some(n))
}

/// Deserialize a string representing a boolean into an `Option<bool>`.
pub(crate) fn deserialize_bool<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    match s.as_str() {
        "true" => Ok(Some(true)),
        "false" => Ok(Some(false)),
        _ => Err(de::Error::unknown_variant(&s, &["true", "false"])),
    }
}

/// Deserialize a comma-separated list of column names into an `Option<Vec<ColumnName>>`.
pub(crate) fn deserialize_column_names<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<ColumnName>>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    Ok(Some(
        ColumnName::parse_column_name_list(&s).map_err(de::Error::custom)?,
    ))
}

/// Deserialize an interval string of the form "interval 5 days" into an `Option<Duration>`.
pub(crate) fn deserialize_interval<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    parse_interval(&s).map(Some).map_err(de::Error::custom)
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
fn parse_interval(value: &str) -> Result<Duration, ParseIntervalError> {
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
    if number < 0 {
        return Err(ParseIntervalError::NegativeInterval(value.to_string()));
    }
    let number: u64 = number
        .try_into()
        .expect("interval is non-negative and was i64, so it fits in u64");

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
        unit @ "month" | unit @ "months" => {
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
            parse_interval("whatever").err().unwrap().to_string(),
            "'whatever' is not an interval".to_string()
        );

        assert_eq!(
            parse_interval("interval").err().unwrap().to_string(),
            "'interval' is not an interval".to_string()
        );

        assert_eq!(
            parse_interval("interval 2").err().unwrap().to_string(),
            "'interval 2' is not an interval".to_string()
        );

        assert_eq!(
            parse_interval("interval 2 months")
                .err()
                .unwrap()
                .to_string(),
            "Unsupported interval 'months'".to_string()
        );

        assert_eq!(
            parse_interval("interval 2 years")
                .err()
                .unwrap()
                .to_string(),
            "Unknown interval unit 'years'".to_string()
        );

        assert_eq!(
            parse_interval("interval two years")
                .err()
                .unwrap()
                .to_string(),
            "Unable to parse 'two' as an integer".to_string()
        );

        assert_eq!(
            parse_interval("interval -25 hours")
                .err()
                .unwrap()
                .to_string(),
            "Interval 'interval -25 hours' cannot be negative".to_string()
        );
    }
}

use crate::expressions::ColumnName;

use std::collections::HashMap;
use std::time::Duration;

use serde::de::{self, DeserializeSeed, Deserializer, MapAccess, Visitor};
use serde::Deserialize;

const SECONDS_PER_MINUTE: u64 = 60;
const SECONDS_PER_HOUR: u64 = 60 * SECONDS_PER_MINUTE;
const SECONDS_PER_DAY: u64 = 24 * SECONDS_PER_HOUR;
const SECONDS_PER_WEEK: u64 = 7 * SECONDS_PER_DAY;

pub(crate) struct StringMapDeserializer<'de> {
    iter: std::collections::hash_map::Iter<'de, String, String>,
}

impl<'de> StringMapDeserializer<'de> {
    pub(crate) fn new(map: &'de HashMap<String, String>) -> Self {
        StringMapDeserializer { iter: map.iter() }
    }
}

impl<'de> Deserializer<'de> for StringMapDeserializer<'de> {
    type Error = de::value::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_map(HashMapMapAccess {
            iter: self.iter,
            value: None,
        })
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes
        byte_buf option unit unit_struct newtype_struct seq tuple tuple_struct
        map struct enum identifier ignored_any
    }
}

struct HashMapMapAccess<'de> {
    iter: std::collections::hash_map::Iter<'de, String, String>,
    value: Option<&'de String>,
}

impl<'de> MapAccess<'de> for HashMapMapAccess<'de> {
    type Error = de::value::Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        match self.iter.next() {
            Some((key, value)) => {
                self.value = Some(value);
                let de = de::value::StrDeserializer::new(key);
                seed.deserialize(de).map(Some)
            }
            None => Ok(None),
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let value = self.value.take().unwrap(); // FIXME
        let de = de::value::StrDeserializer::new(value);
        seed.deserialize(de)
    }
}

pub(crate) fn deserialize_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

pub(crate) fn deserialize_pos_int<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    let n: u64 = s.parse().unwrap(); // FIXME
    if n == 0 {
        panic!("FIXME");
        // return Err("something");
    }
    Ok(Some(n))
}

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

pub(crate) fn deserialize_column_names<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<ColumnName>>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    Ok(Some(
        s.split(',')
            .map(|name: &str| ColumnName::new([name]))
            .collect(),
    ))
}

pub(crate) fn deserialize_interval<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    parse_interval(&s).map(Some).map_err(de::Error::custom)
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
            parse_interval("whatever").err().unwrap(),
            "'whatever' is not an interval".to_string()
        );

        assert_eq!(
            parse_interval("interval").err().unwrap(),
            "'interval' is not an interval".to_string()
        );

        assert_eq!(
            parse_interval("interval 2").err().unwrap(),
            "'interval 2' is not an interval".to_string()
        );

        assert_eq!(
            parse_interval("interval 2 years").err().unwrap(),
            "Unknown unit 'years'".to_string()
        );

        assert_eq!(
            parse_interval("interval two years").err().unwrap(),
            "Cannot parse 'two' as integer: invalid digit found in string".to_string()
        );

        assert_eq!(
            parse_interval("interval -25 hours").err().unwrap(),
            "interval 'interval -25 hours' cannot be negative".to_string()
        );
    }
}

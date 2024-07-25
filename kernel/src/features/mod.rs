use serde::{Deserialize, Serialize};

use crate::Error;

pub use column_mapping::ColumnMappingMode;
pub(crate) use column_mapping::COLUMN_MAPPING_MODE_KEY;

mod column_mapping;

/// Features table readers can support as well as let users know
/// what is supported
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum ReaderFeatures {
    /// Mapping of one column to another
    ColumnMapping,
    /// Deletion vectors for merge, update, delete
    DeletionVectors,
    /// timestamps without timezone support
    #[serde(rename = "timestampNtz")]
    TimestampWithoutTimezone,
    /// version 2 of checkpointing
    V2Checkpoint,
}

impl TryFrom<String> for ReaderFeatures {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.as_str().try_into()
    }
}

impl TryFrom<&str> for ReaderFeatures {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let val = match value {
            "columnMapping" => ReaderFeatures::ColumnMapping,
            "deletionVectors" => ReaderFeatures::DeletionVectors,
            "timestampNtz" => ReaderFeatures::TimestampWithoutTimezone,
            "v2Checkpoint" => ReaderFeatures::V2Checkpoint,
            _ => return Err(Error::generic(format!("Unknown reader feature: {}", value))),
        };
        Ok(val)
    }
}

impl std::str::FromStr for ReaderFeatures {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.try_into()
    }
}

impl AsRef<str> for ReaderFeatures {
    fn as_ref(&self) -> &str {
        match self {
            ReaderFeatures::ColumnMapping => "columnMapping",
            ReaderFeatures::DeletionVectors => "deletionVectors",
            ReaderFeatures::TimestampWithoutTimezone => "timestampNtz",
            ReaderFeatures::V2Checkpoint => "v2Checkpoint",
        }
    }
}

impl std::fmt::Display for ReaderFeatures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

/// Features table writers can support as well as let users know
/// what is supported
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum WriterFeatures {
    /// Append Only Tables
    AppendOnly,
    /// Table invariants
    Invariants,
    /// Check constraints on columns
    CheckConstraints,
    /// CDF on a table
    ChangeDataFeed,
    /// Columns with generated values
    GeneratedColumns,
    /// Mapping of one column to another
    ColumnMapping,
    /// ID Columns
    IdentityColumns,
    /// Deletion vectors for merge, update, delete
    DeletionVectors,
    /// Row tracking on tables
    RowTracking,
    /// timestamps without timezone support
    #[serde(rename = "timestampNtz")]
    TimestampWithoutTimezone,
    /// domain specific metadata
    DomainMetadata,
    /// version 2 of checkpointing
    V2Checkpoint,
    /// Iceberg compatibility support
    IcebergCompatV1,
    /// Iceberg compatibility support
    IcebergCompatV2,
}

impl TryFrom<String> for WriterFeatures {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.as_str().try_into()
    }
}

impl std::str::FromStr for WriterFeatures {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.try_into()
    }
}

impl TryFrom<&str> for WriterFeatures {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let val = match value {
            "appendOnly" => WriterFeatures::AppendOnly,
            "invariants" => WriterFeatures::Invariants,
            "checkConstraints" => WriterFeatures::CheckConstraints,
            "changeDataFeed" => WriterFeatures::ChangeDataFeed,
            "generatedColumns" => WriterFeatures::GeneratedColumns,
            "columnMapping" => WriterFeatures::ColumnMapping,
            "identityColumns" => WriterFeatures::IdentityColumns,
            "deletionVectors" => WriterFeatures::DeletionVectors,
            "rowTracking" => WriterFeatures::RowTracking,
            "timestampNtz" => WriterFeatures::TimestampWithoutTimezone,
            "domainMetadata" => WriterFeatures::DomainMetadata,
            "v2Checkpoint" => WriterFeatures::V2Checkpoint,
            "icebergCompatV1" => WriterFeatures::IcebergCompatV1,
            "icebergCompatV2" => WriterFeatures::IcebergCompatV2,
            _ => return Err(Error::generic(format!("Unknown writer feature: {}", value))),
        };
        Ok(val)
    }
}

impl AsRef<str> for WriterFeatures {
    fn as_ref(&self) -> &str {
        match self {
            WriterFeatures::AppendOnly => "appendOnly",
            WriterFeatures::Invariants => "invariants",
            WriterFeatures::CheckConstraints => "checkConstraints",
            WriterFeatures::ChangeDataFeed => "changeDataFeed",
            WriterFeatures::GeneratedColumns => "generatedColumns",
            WriterFeatures::ColumnMapping => "columnMapping",
            WriterFeatures::IdentityColumns => "identityColumns",
            WriterFeatures::DeletionVectors => "deletionVectors",
            WriterFeatures::RowTracking => "rowTracking",
            WriterFeatures::TimestampWithoutTimezone => "timestampNtz",
            WriterFeatures::DomainMetadata => "domainMetadata",
            WriterFeatures::V2Checkpoint => "v2Checkpoint",
            WriterFeatures::IcebergCompatV1 => "icebergCompatV1",
            WriterFeatures::IcebergCompatV2 => "icebergCompatV2",
        }
    }
}

impl std::fmt::Display for WriterFeatures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_reader_features() {
        let cases = [
            (ReaderFeatures::ColumnMapping, "columnMapping"),
            (ReaderFeatures::DeletionVectors, "deletionVectors"),
            (ReaderFeatures::TimestampWithoutTimezone, "timestampNtz"),
            (ReaderFeatures::V2Checkpoint, "v2Checkpoint"),
        ];

        for (feature, expected) in cases.into_iter() {
            let serialized = serde_json::to_string(&feature).unwrap();
            assert_eq!(serialized, format!("\"{}\"", expected));

            let deserialized: ReaderFeatures = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, feature);

            let from_str: ReaderFeatures = expected.parse().unwrap();
            assert_eq!(from_str, feature);
        }
    }

    #[test]
    fn test_roundtrip_writer_features() {
        let cases = [
            (WriterFeatures::AppendOnly, "appendOnly"),
            (WriterFeatures::Invariants, "invariants"),
            (WriterFeatures::CheckConstraints, "checkConstraints"),
            (WriterFeatures::ChangeDataFeed, "changeDataFeed"),
            (WriterFeatures::GeneratedColumns, "generatedColumns"),
            (WriterFeatures::ColumnMapping, "columnMapping"),
            (WriterFeatures::IdentityColumns, "identityColumns"),
            (WriterFeatures::DeletionVectors, "deletionVectors"),
            (WriterFeatures::RowTracking, "rowTracking"),
            (WriterFeatures::TimestampWithoutTimezone, "timestampNtz"),
            (WriterFeatures::DomainMetadata, "domainMetadata"),
            (WriterFeatures::V2Checkpoint, "v2Checkpoint"),
            (WriterFeatures::IcebergCompatV1, "icebergCompatV1"),
            (WriterFeatures::IcebergCompatV2, "icebergCompatV2"),
        ];

        for (feature, expected) in cases.into_iter() {
            let serialized = serde_json::to_string(&feature).unwrap();
            assert_eq!(serialized, format!("\"{}\"", expected));

            let deserialized: WriterFeatures = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, feature);

            let from_str: WriterFeatures = expected.parse().unwrap();
            assert_eq!(from_str, feature);
        }
    }
}

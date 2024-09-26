use serde::{Deserialize, Serialize};

pub use column_mapping::ColumnMappingMode;
pub use constraints::Constraint;
use strum::{AsRefStr, Display as StrumDisplay, EnumString, VariantNames};

mod column_mapping;
mod constraints;

/// Features table readers can support as well as let users know
/// what is supported
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
)]
#[strum(serialize_all = "camelCase")]
#[serde(rename_all = "camelCase")]
pub enum ReaderFeatures {
    /// Mapping of one column to another
    ColumnMapping,
    /// Deletion vectors for merge, update, delete
    DeletionVectors,
    /// timestamps without timezone support
    #[strum(serialize = "timestampNtz")]
    #[serde(rename = "timestampNtz")]
    TimestampWithoutTimezone,
    // Allow columns to change type
    TypeWidening,
    #[strum(serialize = "typeWidening-preview")]
    #[serde(rename = "typeWidening-preview")]
    TypeWideningPreview,
    /// version 2 of checkpointing
    V2Checkpoint,
}

/// Features table writers can support as well as let users know
/// what is supported
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
)]
#[strum(serialize_all = "camelCase")]
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
    #[strum(serialize = "timestampNtz")]
    #[serde(rename = "timestampNtz")]
    TimestampWithoutTimezone,
    // Allow columns to change type
    TypeWidening,
    #[strum(serialize = "typeWidening-preview")]
    #[serde(rename = "typeWidening-preview")]
    TypeWideningPreview,
    /// domain specific metadata
    DomainMetadata,
    /// version 2 of checkpointing
    V2Checkpoint,
    /// Iceberg compatibility support
    IcebergCompatV1,
    /// Iceberg compatibility support
    IcebergCompatV2,
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
            (ReaderFeatures::TypeWidening, "typeWidening"),
            (ReaderFeatures::TypeWideningPreview, "typeWidening-preview"),
            (ReaderFeatures::V2Checkpoint, "v2Checkpoint"),
        ];

        assert_eq!(ReaderFeatures::VARIANTS.len(), cases.len());

        for ((feature, expected), name) in cases.into_iter().zip(ReaderFeatures::VARIANTS) {
            assert_eq!(*name, expected);

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
            (WriterFeatures::TypeWidening, "typeWidening"),
            (WriterFeatures::TypeWideningPreview, "typeWidening-preview"),
            (WriterFeatures::DomainMetadata, "domainMetadata"),
            (WriterFeatures::V2Checkpoint, "v2Checkpoint"),
            (WriterFeatures::IcebergCompatV1, "icebergCompatV1"),
            (WriterFeatures::IcebergCompatV2, "icebergCompatV2"),
        ];

        assert_eq!(WriterFeatures::VARIANTS.len(), cases.len());

        for ((feature, expected), name) in cases.into_iter().zip(WriterFeatures::VARIANTS) {
            assert_eq!(*name, expected);

            let serialized = serde_json::to_string(&feature).unwrap();
            assert_eq!(serialized, format!("\"{}\"", expected));

            let deserialized: WriterFeatures = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, feature);

            let from_str: WriterFeatures = expected.parse().unwrap();
            assert_eq!(from_str, feature);
        }
    }
}

//! Provides parsing and manipulation of the various actions defined in the [Delta
//! specification](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)

pub mod deletion_vector;
pub(crate) mod schemas;
pub mod visitors;

use std::collections::HashMap;
use std::fmt;

use delta_kernel_derive::Schema;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use visitors::{AddVisitor, MetadataVisitor, ProtocolVisitor};

use self::deletion_vector::DeletionVectorDescriptor;
use crate::actions::schemas::GetStructField;
use crate::{schema::StructType, DeltaResult, EngineData, Error};

pub(crate) const ADD_NAME: &str = "add";
pub(crate) const REMOVE_NAME: &str = "remove";
pub(crate) const METADATA_NAME: &str = "metaData";
pub(crate) const PROTOCOL_NAME: &str = "protocol";
pub(crate) const TRANSACTION_NAME: &str = "txn";

lazy_static! {
    static ref LOG_SCHEMA: StructType = StructType::new(
        vec![
            Option::<Add>::get_struct_field(ADD_NAME),
            Option::<Remove>::get_struct_field(REMOVE_NAME),
            Option::<Metadata>::get_struct_field(METADATA_NAME),
            Option::<Protocol>::get_struct_field(PROTOCOL_NAME),
            Option::<Transaction>::get_struct_field(TRANSACTION_NAME),
            // We don't support the following actions yet
            //Option<Cdc>::get_field(CDC_NAME),
            //Option<CommitInfo>::get_field(COMMIT_INFO_NAME),
            //Option<DomainMetadata>::get_field(DOMAIN_METADATA_NAME),
        ]
    );
}

#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
#[cfg_attr(not(feature = "developer-visibility"), visibility::make(pub(crate)))]
pub fn get_log_schema() -> &'static StructType {
    &LOG_SCHEMA
}

#[derive(Debug, Clone, PartialEq, Eq, Schema, Serialize, Deserialize)]
pub struct Format {
    /// Name of the encoding for files in this table
    pub provider: String,
    /// A map containingconfiguration options for the format
    pub options: HashMap<String, String>,
}

impl Default for Format {
    fn default() -> Self {
        Self {
            provider: String::from("parquet"),
            options: HashMap::new(),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Schema, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    /// Unique identifier for this table
    pub id: String,
    /// User-provided identifier for this table
    pub name: Option<String>,
    /// User-provided description for this table
    pub description: Option<String>,
    /// Specification of the encoding for the files stored in the table
    pub format: Format,
    /// Schema of the table
    pub schema_string: String,
    /// Column names by which the data should be partitioned
    pub partition_columns: Vec<String>,
    /// The time when this metadata action is created, in milliseconds since the Unix epoch
    pub created_time: Option<i64>,
    /// Configuration options for the metadata action
    pub configuration: HashMap<String, String>,
}

impl Metadata {
    pub fn try_new_from_data(data: &dyn EngineData) -> DeltaResult<Option<Metadata>> {
        let mut visitor = MetadataVisitor::default();
        data.extract(get_log_schema().project(&[METADATA_NAME])?, &mut visitor)?;
        Ok(visitor.metadata)
    }

    pub fn schema(&self) -> DeltaResult<StructType> {
        Ok(serde_json::from_str(&self.schema_string)?)
    }
}

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

impl fmt::Display for ReaderFeatures {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
}

impl TryFrom<String> for WriterFeatures {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.as_str().try_into()
    }
}

impl TryFrom<&str> for WriterFeatures {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let val = match value {
            "appendOnly" | "delta.appendOnly" => WriterFeatures::AppendOnly,
            "invariants" | "delta.invariants" => WriterFeatures::Invariants,
            "checkConstraints" | "delta.checkConstraints" => WriterFeatures::CheckConstraints,
            "changeDataFeed" | "delta.enableChangeDataFeed" => WriterFeatures::ChangeDataFeed,
            "generatedColumns" => WriterFeatures::GeneratedColumns,
            "columnMapping" => WriterFeatures::ColumnMapping,
            "identityColumns" => WriterFeatures::IdentityColumns,
            "deletionVectors" | "delta.enableDeletionVectors" => WriterFeatures::DeletionVectors,
            "rowTracking" | "delta.enableRowTracking" => WriterFeatures::RowTracking,
            "timestampNtz" => WriterFeatures::TimestampWithoutTimezone,
            "domainMetadata" => WriterFeatures::DomainMetadata,
            "v2Checkpoint" => WriterFeatures::V2Checkpoint,
            "icebergCompatV1" => WriterFeatures::IcebergCompatV1,
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
        }
    }
}

impl fmt::Display for WriterFeatures {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Schema, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Protocol {
    /// The minimum version of the Delta read protocol that a client must implement
    /// in order to correctly read this table
    pub min_reader_version: i32,
    /// The minimum version of the Delta write protocol that a client must implement
    /// in order to correctly write this table
    pub min_writer_version: i32,
    /// A collection of features that a client must implement in order to correctly
    /// read this table (exist only when minReaderVersion is set to 3)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reader_features: Option<Vec<String>>,
    /// A collection of features that a client must implement in order to correctly
    /// write this table (exist only when minWriterVersion is set to 7)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub writer_features: Option<Vec<String>>,
}

impl Protocol {
    pub fn try_new_from_data(data: &dyn EngineData) -> DeltaResult<Option<Protocol>> {
        let mut visitor = ProtocolVisitor::default();
        data.extract(get_log_schema().project(&[PROTOCOL_NAME])?, &mut visitor)?;
        Ok(visitor.protocol)
    }

    pub fn has_reader_feature(&self, feature: &ReaderFeatures) -> bool {
        self.reader_features
            .as_ref()
            .map(|features| features.iter().any(|f| f == feature.as_ref()))
            .unwrap_or_default()
    }

    pub fn has_writer_feature(&self, feature: &WriterFeatures) -> bool {
        self.writer_features
            .as_ref()
            .map(|features| features.iter().any(|f| f == feature.as_ref()))
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Schema, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Add {
    /// A relative path to a data file from the root of the table or an absolute path to a file
    /// that should be added to the table. The path is a URI as specified by
    /// [RFC 2396 URI Generic Syntax], which needs to be decoded to get the data file path.
    ///
    /// [RFC 2396 URI Generic Syntax]: https://www.ietf.org/rfc/rfc2396.txt
    pub path: String,

    /// A map from partition column to value for this logical file.
    pub partition_values: HashMap<String, String>,

    /// The size of this data file in bytes
    pub size: i64,

    /// The time this logical file was created, as milliseconds since the epoch.
    pub modification_time: i64,

    /// When `false` the logical file must already be present in the table or the records
    /// in the added file must be contained in one or more remove actions in the same version.
    pub data_change: bool,

    /// Contains [statistics] (e.g., count, min/max values for columns) about the data in this logical file.
    ///
    /// [statistics]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Per-file-Statistics
    pub stats: Option<String>,

    /// Map containing metadata about this logical file.
    pub tags: Option<HashMap<String, String>>,

    /// Information about deletion vector (DV) associated with this add action
    pub deletion_vector: Option<DeletionVectorDescriptor>,

    /// Default generated Row ID of the first row in the file. The default generated Row IDs
    /// of the other rows in the file can be reconstructed by adding the physical index of the
    /// row within the file to the base Row ID
    pub base_row_id: Option<i64>,

    /// First commit version in which an add action with the same path was committed to the table.
    pub default_row_commit_version: Option<i64>,

    /// The name of the clustering implementation
    pub clustering_provider: Option<String>,
}

impl Add {
    /// Since we always want to parse multiple adds from data, we return a `Vec<Add>`
    pub fn parse_from_data(data: &dyn EngineData) -> DeltaResult<Vec<Add>> {
        let mut visitor = AddVisitor::default();
        data.extract(get_log_schema().project(&[ADD_NAME])?, &mut visitor)?;
        Ok(visitor.adds)
    }

    pub fn dv_unique_id(&self) -> Option<String> {
        self.deletion_vector.as_ref().map(|dv| dv.unique_id())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Schema, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Remove {
    /// A relative path to a data file from the root of the table or an absolute path to a file
    /// that should be added to the table. The path is a URI as specified by
    /// [RFC 2396 URI Generic Syntax], which needs to be decoded to get the data file path.
    ///
    /// [RFC 2396 URI Generic Syntax]: https://www.ietf.org/rfc/rfc2396.txt
    pub path: String,

    /// The time this logical file was created, as milliseconds since the epoch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_timestamp: Option<i64>,

    /// When `false` the logical file must already be present in the table or the records
    /// in the added file must be contained in one or more remove actions in the same version.
    pub data_change: bool,

    /// When true the fields `partition_values`, `size`, and `tags` are present
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extended_file_metadata: Option<bool>,

    /// A map from partition column to value for this logical file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_values: Option<HashMap<String, String>>,

    /// The size of this data file in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,

    /// Map containing metadata about this logical file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, String>>,

    /// Information about deletion vector (DV) associated with this add action
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_vector: Option<DeletionVectorDescriptor>,

    /// Default generated Row ID of the first row in the file. The default generated Row IDs
    /// of the other rows in the file can be reconstructed by adding the physical index of the
    /// row within the file to the base Row ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_row_id: Option<i64>,

    /// First commit version in which an add action with the same path was committed to the table.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_row_commit_version: Option<i64>,
}

impl Remove {
    pub(crate) fn dv_unique_id(&self) -> Option<String> {
        self.deletion_vector.as_ref().map(|dv| dv.unique_id())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Schema, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    /// A unique identifier for the application performing the transaction.
    pub app_id: String,

    /// An application-specific numeric identifier for this transaction.
    pub version: i64,

    /// The time when this transaction action was created in milliseconds since the Unix epoch.
    pub last_updated: Option<i64>,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::schema::{ArrayType, DataType, MapType, StructField};

    #[test]
    fn test_metadata_schema() {
        let schema = get_log_schema()
            .project(&["metaData"])
            .expect("Couldn't get metaData field");

        let expected = Arc::new(StructType::new(vec![StructField::new(
            "metaData",
            StructType::new(vec![
                StructField::new("id", DataType::STRING, false),
                StructField::new("name", DataType::STRING, true),
                StructField::new("description", DataType::STRING, true),
                StructField::new(
                    "format",
                    StructType::new(vec![
                        StructField::new("provider", DataType::STRING, false),
                        StructField::new(
                            "options",
                            MapType::new(DataType::STRING, DataType::STRING, false),
                            false,
                        ),
                    ]),
                    false,
                ),
                StructField::new("schemaString", DataType::STRING, false),
                StructField::new(
                    "partitionColumns",
                    ArrayType::new(DataType::STRING, false),
                    false,
                ),
                StructField::new("createdTime", DataType::LONG, true),
                StructField::new(
                    "configuration",
                    MapType::new(DataType::STRING, DataType::STRING, false),
                    false,
                ),
            ]),
            true,
        )]));
        assert_eq!(schema, expected);
    }

    fn tags_field() -> StructField {
        StructField::new(
            "tags",
            MapType::new(DataType::STRING, DataType::STRING, false),
            true,
        )
    }

    fn partition_values_field() -> StructField {
        StructField::new(
            "partitionValues",
            MapType::new(DataType::STRING, DataType::STRING, false),
            true,
        )
    }

    fn deletion_vector_field() -> StructField {
        StructField::new(
            "deletionVector",
            DataType::Struct(Box::new(StructType::new(vec![
                StructField::new("storageType", DataType::STRING, false),
                StructField::new("pathOrInlineDv", DataType::STRING, false),
                StructField::new("offset", DataType::INTEGER, true),
                StructField::new("sizeInBytes", DataType::INTEGER, false),
                StructField::new("cardinality", DataType::LONG, false),
            ]))),
            true,
        )
    }

    #[test]
    fn test_remove_schema() {
        let schema = get_log_schema()
            .project(&["remove"])
            .expect("Couldn't get remove field");
        let expected = Arc::new(StructType::new(vec![StructField::new(
            "remove",
            StructType::new(vec![
                StructField::new("path", DataType::STRING, false),
                StructField::new("deletionTimestamp", DataType::LONG, true),
                StructField::new("dataChange", DataType::BOOLEAN, false),
                StructField::new("extendedFileMetadata", DataType::BOOLEAN, true),
                partition_values_field(),
                StructField::new("size", DataType::LONG, true),
                tags_field(),
                deletion_vector_field(),
                StructField::new("baseRowId", DataType::LONG, true),
                StructField::new("defaultRowCommitVersion", DataType::LONG, true),
            ]),
            true,
        )]));
        assert_eq!(schema, expected);
    }

    #[test]
    fn test_transaction_schema() {
        let schema = get_log_schema()
            .project(&["txn"])
            .expect("Couldn't get transaction field");

        let expected = Arc::new(StructType::new(vec![StructField::new(
            "txn",
            StructType::new(vec![
                StructField::new("appId", DataType::STRING, false),
                StructField::new("version", DataType::LONG, false),
                StructField::new("lastUpdated", DataType::LONG, true),
            ]),
            true,
        )]));
        assert_eq!(schema, expected);
    }
}

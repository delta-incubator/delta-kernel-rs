//! Provides parsing and manipulation of the various actions defined in the [Delta
//! specification](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)

use delta_kernel_derive::Schema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::LazyLock;
use visitors::{AddVisitor, MetadataVisitor, ProtocolVisitor};

use self::deletion_vector::DeletionVectorDescriptor;
use crate::actions::schemas::GetStructField;
use crate::features::{ReaderFeatures, WriterFeatures};
use crate::{schema::StructType, DeltaResult, EngineData};

pub mod deletion_vector;
pub mod set_transaction;

pub(crate) mod schemas;
#[cfg(feature = "developer-visibility")]
pub mod visitors;
#[cfg(not(feature = "developer-visibility"))]
pub(crate) mod visitors;

pub(crate) const ADD_NAME: &str = "add";
pub(crate) const REMOVE_NAME: &str = "remove";
pub(crate) const METADATA_NAME: &str = "metaData";
pub(crate) const PROTOCOL_NAME: &str = "protocol";
pub(crate) const SET_TRANSACTION_NAME: &str = "txn";
pub(crate) const COMMIT_INFO_NAME: &str = "commitInfo";

static LOG_SCHEMA: LazyLock<StructType> = LazyLock::new(|| {
    StructType::new([
        Option::<Add>::get_struct_field(ADD_NAME),
        Option::<Remove>::get_struct_field(REMOVE_NAME),
        Option::<Metadata>::get_struct_field(METADATA_NAME),
        Option::<Protocol>::get_struct_field(PROTOCOL_NAME),
        Option::<SetTransaction>::get_struct_field(SET_TRANSACTION_NAME),
        Option::<CommitInfo>::get_struct_field(COMMIT_INFO_NAME),
        // We don't support the following actions yet
        //Option::<Cdc>::get_struct_field(CDC_NAME),
        //Option::<DomainMetadata>::get_struct_field(DOMAIN_METADATA_NAME),
    ])
});

#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
#[cfg_attr(not(feature = "developer-visibility"), visibility::make(pub(crate)))]
fn get_log_schema() -> &'static StructType {
    &LOG_SCHEMA
}

#[derive(Debug, Clone, PartialEq, Eq, Schema)]
pub struct Format {
    /// Name of the encoding for files in this table
    pub provider: String,
    /// A map containing configuration options for the format
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

#[derive(Debug, Default, Clone, PartialEq, Eq, Schema)]
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
            .is_some_and(|features| features.iter().any(|f| f == feature.as_ref()))
    }

    pub fn has_writer_feature(&self, feature: &WriterFeatures) -> bool {
        self.writer_features
            .as_ref()
            .is_some_and(|features| features.iter().any(|f| f == feature.as_ref()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Schema)]
struct OperationParameters {
    operation_parameters: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Schema)]
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
#[cfg_attr(not(feature = "developer-visibility"), visibility::make(pub(crate)))]
struct CommitInfo {
    /// The time this logical file was created, as milliseconds since the epoch.
    pub(crate) timestamp: Option<i64>,
    /// An arbitrary string that identifies the operation associated with this commit. This is
    /// specified by the engine.
    pub(crate) operation: Option<String>,
    /// Map of arbitrary string key-value pairs that provide additional information about the
    /// operation. This is specified by the engine.
    ///
    /// For now this is always empty; and since we don't have the ability to construct an empty
    /// string-string map, we spoof the operation_parameters with an empty struct so it serializes
    /// the same as an empty map (as `{}`).
    operation_parameters: Option<OperationParameters>,
    /// The version of the delta_kernel crate used to write this commit. The kernel will always
    /// write this field, but it is optional since many tables will not have this field (i.e. any
    /// tables not written by kernel).
    pub(crate) kernel_version: Option<String>,
    /// A place for the engine to store additional metadata associated with this commit encoded as
    /// a map of strings.
    pub(crate) engine_commit_info: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Schema)]
pub struct Add {
    /// A relative path to a data file from the root of the table or an absolute path to a file
    /// that should be added to the table. The path is a URI as specified by
    /// [RFC 2396 URI Generic Syntax], which needs to be decoded to get the data file path.
    ///
    /// [RFC 2396 URI Generic Syntax]: https://www.ietf.org/rfc/rfc2396.txt
    pub path: String,

    /// A map from partition column to value for this logical file. This map can contain null in the
    /// values meaning a partition is null. We drop those values from this map, due to the
    /// `drop_null_container_values` annotation. This means an engine can assume that if a partition
    /// is found in [`Metadata`] `partition_columns`, but not in this map, its value is null.
    #[drop_null_container_values]
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

#[derive(Debug, Clone, PartialEq, Eq, Schema)]
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
#[cfg_attr(not(feature = "developer-visibility"), visibility::make(pub(crate)))]
struct Remove {
    /// A relative path to a data file from the root of the table or an absolute path to a file
    /// that should be added to the table. The path is a URI as specified by
    /// [RFC 2396 URI Generic Syntax], which needs to be decoded to get the data file path.
    ///
    /// [RFC 2396 URI Generic Syntax]: https://www.ietf.org/rfc/rfc2396.txt
    pub(crate) path: String,

    /// The time this logical file was created, as milliseconds since the epoch.
    pub(crate) deletion_timestamp: Option<i64>,

    /// When `false` the logical file must already be present in the table or the records
    /// in the added file must be contained in one or more remove actions in the same version.
    pub(crate) data_change: bool,

    /// When true the fields `partition_values`, `size`, and `tags` are present
    pub(crate) extended_file_metadata: Option<bool>,

    /// A map from partition column to value for this logical file.
    pub(crate) partition_values: Option<HashMap<String, String>>,

    /// The size of this data file in bytes
    pub(crate) size: Option<i64>,

    /// Map containing metadata about this logical file.
    pub(crate) tags: Option<HashMap<String, String>>,

    /// Information about deletion vector (DV) associated with this add action
    pub(crate) deletion_vector: Option<DeletionVectorDescriptor>,

    /// Default generated Row ID of the first row in the file. The default generated Row IDs
    /// of the other rows in the file can be reconstructed by adding the physical index of the
    /// row within the file to the base Row ID
    pub(crate) base_row_id: Option<i64>,

    /// First commit version in which an add action with the same path was committed to the table.
    pub(crate) default_row_commit_version: Option<i64>,
}

impl Remove {
    pub(crate) fn dv_unique_id(&self) -> Option<String> {
        self.deletion_vector.as_ref().map(|dv| dv.unique_id())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Schema)]
pub struct SetTransaction {
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

        let expected = Arc::new(StructType::new([StructField::new(
            "metaData",
            StructType::new([
                StructField::new("id", DataType::STRING, false),
                StructField::new("name", DataType::STRING, true),
                StructField::new("description", DataType::STRING, true),
                StructField::new(
                    "format",
                    StructType::new([
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

    #[test]
    fn test_add_schema() {
        let schema = get_log_schema()
            .project(&["add"])
            .expect("Couldn't get add field");

        let expected = Arc::new(StructType::new([StructField::new(
            "add",
            StructType::new([
                StructField::new("path", DataType::STRING, false),
                StructField::new(
                    "partitionValues",
                    MapType::new(DataType::STRING, DataType::STRING, true),
                    false,
                ),
                StructField::new("size", DataType::LONG, false),
                StructField::new("modificationTime", DataType::LONG, false),
                StructField::new("dataChange", DataType::BOOLEAN, false),
                StructField::new("stats", DataType::STRING, true),
                StructField::new(
                    "tags",
                    MapType::new(DataType::STRING, DataType::STRING, false),
                    true,
                ),
                deletion_vector_field(),
                StructField::new("baseRowId", DataType::LONG, true),
                StructField::new("defaultRowCommitVersion", DataType::LONG, true),
                StructField::new("clusteringProvider", DataType::STRING, true),
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
            DataType::struct_type([
                StructField::new("storageType", DataType::STRING, false),
                StructField::new("pathOrInlineDv", DataType::STRING, false),
                StructField::new("offset", DataType::INTEGER, true),
                StructField::new("sizeInBytes", DataType::INTEGER, false),
                StructField::new("cardinality", DataType::LONG, false),
            ]),
            true,
        )
    }

    #[test]
    fn test_remove_schema() {
        let schema = get_log_schema()
            .project(&["remove"])
            .expect("Couldn't get remove field");
        let expected = Arc::new(StructType::new([StructField::new(
            "remove",
            StructType::new([
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

        let expected = Arc::new(StructType::new([StructField::new(
            "txn",
            StructType::new([
                StructField::new("appId", DataType::STRING, false),
                StructField::new("version", DataType::LONG, false),
                StructField::new("lastUpdated", DataType::LONG, true),
            ]),
            true,
        )]));
        assert_eq!(schema, expected);
    }

    #[test]
    fn test_commit_info_schema() {
        let schema = get_log_schema()
            .project(&["commitInfo"])
            .expect("Couldn't get commitInfo field");

        let expected = Arc::new(StructType::new(vec![StructField::new(
            "commitInfo",
            StructType::new(vec![
                StructField::new("timestamp", DataType::LONG, true),
                StructField::new("operation", DataType::STRING, true),
                StructField::new(
                    "operationParameters",
                    StructType::new([StructField::new(
                        "operationParameters",
                        DataType::LONG,
                        true,
                    )]),
                    true,
                ),
                StructField::new("kernelVersion", DataType::STRING, true),
                StructField::new(
                    "engineCommitInfo",
                    MapType::new(DataType::STRING, DataType::STRING, false),
                    true,
                ),
            ]),
            true,
        )]));
        assert_eq!(schema, expected);
    }
}

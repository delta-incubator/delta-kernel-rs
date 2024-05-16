//! Code to handle column mapping, including modes and schema transforms

use crate::{
    schema::{ColumnMetadataKey, MetadataValue, StructField},
    DeltaResult, Error,
};
use serde::{Deserialize, Serialize};

/// Modes of column mapping a table can be in
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnMappingMode {
    None,
    Id,
    Name,
}

// key to look in metadata.configuration for to get column mapping mode
pub(crate) const COLUMN_MAPPING_MODE_KEY: &str = "delta.columnMapping.mode";

impl TryFrom<&str> for ColumnMappingMode {
    type Error = Error;

    fn try_from(mode: &str) -> DeltaResult<Self> {
        match mode {
            "none" => Ok(ColumnMappingMode::None),
            "id" => Ok(ColumnMappingMode::Id),
            "name" => Ok(ColumnMappingMode::Name),
            _ => Err(Error::invalid_column_mapping_mode(mode)),
        }
    }
}

/// Transform a logical `StructField` into the field as it should be read from parquet. *Only* valid
/// to call this from a scan operating in ColumnMappingMode::Name mode.
///
/// This returns both the field and the name of the field. The name has the same lifetime as the
/// logical field
pub(crate) fn get_name_mapped_physical_field(
    logical_field: &StructField,
) -> DeltaResult<(StructField, &str)> {
    match logical_field.metadata.get(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref()) {
        Some(val) => match val {
            MetadataValue::Number(_) => {
                Err(Error::generic("{ColumnMetadataKey::ColumnMappingPhysicalName} must be a string in name mapping mode"))
            }
            MetadataValue::String(name) => {
                Ok((
                    StructField::new(name, logical_field.data_type().clone(), logical_field.is_nullable()),
                    name
                ))
            }
        }
        None => {
            Err(Error::generic("fields MUST have a {ColumnMetadataKey::ColumnMappingPhysicalName} key in their metadata in name mapping mode"))
        }
    }
}

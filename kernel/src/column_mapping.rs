//! Code to handle column mapping, including modes and schema transforms

use std::sync::Arc;

use itertools::Itertools;

use crate::{
    schema::{ColumnMetadataKey, MetadataValue, SchemaRef, StructField, StructType},
    DeltaResult, Error,
};

/// Modes of column mapping a table can be in
#[derive(Debug)]
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

pub(crate) fn transform_to_parquet_schema(
    logical_schema: &SchemaRef,
    mapping_mode: ColumnMappingMode,
) -> DeltaResult<SchemaRef> {
    match mapping_mode {
        ColumnMappingMode::None => Ok(logical_schema.clone()),
        ColumnMappingMode::Id => Err(Error::generic("Don't support id mapping atm")),
        ColumnMappingMode::Name => {
            let parquet_fields: Vec<StructField> = logical_schema.fields().map(|field| {
                match field.metadata.get(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref()) {
                    Some(val) => match val {
                        MetadataValue::Number(_) => {
                            Err(Error::generic("{ColumnMetadataKey::ColumnMappingPhysicalName} must be a string in name mapping mode"))
                        }
                        MetadataValue::String(name) => {
                            Ok(StructField::new(name, field.data_type().clone(), field.is_nullable()))
                        }
                    }
                    None => {
                        Err(Error::generic("fields MUST have a {ColumnMetadataKey::ColumnMappingPhysicalName} key in their metadata in name mapping mode"))
                    }
                }
            }).try_collect()?;
            Ok(Arc::new(StructType::new(parquet_fields)))
        }
    }
}

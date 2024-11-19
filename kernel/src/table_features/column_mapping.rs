//! Code to handle column mapping, including modes and schema transforms
use std::borrow::Cow;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::actions::{Metadata, Protocol};
use crate::schema::{ColumnName, DataType, MetadataValue, Schema, SchemaTransform, StructField};
use crate::{DeltaResult, Error};

/// Modes of column mapping a table can be in
#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum ColumnMappingMode {
    /// No column mapping is applied
    None,
    /// Columns are mapped by their field_id in parquet
    Id,
    /// Columns are mapped to a physical name
    Name,
}

// key to look in metadata.configuration for to get column mapping mode
pub(crate) const COLUMN_MAPPING_MODE_KEY: &str = "delta.columnMapping.mode";

impl TryFrom<&str> for ColumnMappingMode {
    type Error = Error;

    fn try_from(s: &str) -> DeltaResult<Self> {
        match s.to_ascii_lowercase().as_str() {
            "none" => Ok(Self::None),
            "id" => Ok(Self::Id),
            "name" => Ok(Self::Name),
            _ => Err(Error::invalid_column_mapping_mode(s)),
        }
    }
}

impl FromStr for ColumnMappingMode {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.try_into()
    }
}

impl Default for ColumnMappingMode {
    fn default() -> Self {
        Self::None
    }
}

impl AsRef<str> for ColumnMappingMode {
    fn as_ref(&self) -> &str {
        match self {
            Self::None => "none",
            Self::Id => "id",
            Self::Name => "name",
        }
    }
}

/// Extract the schema and column mapping mode from the metadata. When column mapping mode is
/// enabled, verify that each field in the schema is annotated with a physical name and field_id;
/// when not enabled, verify that no fields are annotated.
pub(crate) fn get_validated_column_mapping_schema(
    metadata: &Metadata,
    protocol: &Protocol,
) -> DeltaResult<(Schema, ColumnMappingMode)> {
    let column_mapping_mode = match metadata.configuration.get(COLUMN_MAPPING_MODE_KEY) {
        Some(mode) if protocol.min_reader_version() >= 2 => mode.as_str().try_into()?,
        _ => ColumnMappingMode::None,
    };
    let schema = metadata.schema()?;
    validate_column_mapping_schema(&schema, column_mapping_mode)?;
    Ok((schema, column_mapping_mode))
}

pub(crate) fn validate_column_mapping_schema(
    schema: &Schema,
    mode: ColumnMappingMode,
) -> DeltaResult<()> {
    if mode == ColumnMappingMode::Id {
        // TODO: Support column mapping ID mode
        return Err(Error::unsupported("Column mapping ID mode not supported"));
    }

    let mut validator = ValidateColumnMappings {
        mode,
        path: vec![],
        err: None,
    };
    let _ = validator.transform_struct(schema);
    if let Some(err) = validator.err {
        Err(err)
    } else {
        Ok(())
    }
}

struct ValidateColumnMappings {
    mode: ColumnMappingMode,
    path: Vec<String>,
    err: Option<Error>,
}
impl ValidateColumnMappings {
    fn transform_inner_type<'a>(
        &mut self,
        data_type: &'a DataType,
        name: &str,
    ) -> Option<Cow<'a, DataType>> {
        if self.err.is_none() {
            self.path.push(name.to_string());
            let _ = self.transform(data_type);
            self.path.pop();
        }
        None
    }
    fn check_annotations(&mut self, field: &StructField) {
        let annotation = "delta.columnMapping.physicalName";
        match (self.mode, field.metadata.get(annotation)) {
            // Both Id and Name modes require a physical name; None mode requires no physical name.
            (ColumnMappingMode::Name | ColumnMappingMode::Id, Some(MetadataValue::String(_))) => {}
            (ColumnMappingMode::None, None) => {}
            (ColumnMappingMode::None, Some(_)) => {
                self.err = Some(Error::invalid_column_mapping_mode(format!(
                    "Column mapping is not enabled but field '{}' is annotated with {}",
                    ColumnName::new(&self.path),
                    annotation
                )));
            }
            (ColumnMappingMode::Name | ColumnMappingMode::Id, _) => {
                self.err = Some(Error::invalid_column_mapping_mode(format!(
                    "Column mapping is enabled but field '{}' lacks the string annotation {}",
                    ColumnName::new(&self.path),
                    annotation
                )));
            }
        }

        let annotation = "delta.columnMapping.id";
        match (self.mode, field.metadata.get(annotation)) {
            // Both Id and Name modes require a field ID; None mode requires no field id.
            (ColumnMappingMode::Name | ColumnMappingMode::Id, Some(MetadataValue::Number(_))) => {}
            (ColumnMappingMode::None, None) => {}
            (ColumnMappingMode::None, Some(_)) => {
                self.err = Some(Error::invalid_column_mapping_mode(format!(
                    "Column mapping is not enabled but field '{}' is annotated with {}",
                    ColumnName::new(&self.path),
                    annotation
                )));
            }
            (ColumnMappingMode::Name | ColumnMappingMode::Id, _) => {
                self.err = Some(Error::invalid_column_mapping_mode(format!(
                    "Column mapping is enabled but field '{}' lacks the numeric annotation {}",
                    ColumnName::new(&self.path),
                    annotation
                )));
            }
        }
    }
}
impl SchemaTransform for ValidateColumnMappings {
    // Override array element and map key/value for better error messages
    fn transform_array_element<'a>(&mut self, etype: &'a DataType) -> Option<Cow<'a, DataType>> {
        self.transform_inner_type(etype, "<array element>")
    }
    fn transform_map_key<'a>(&mut self, ktype: &'a DataType) -> Option<Cow<'a, DataType>> {
        self.transform_inner_type(ktype, "<map key>")
    }
    fn transform_map_value<'a>(&mut self, vtype: &'a DataType) -> Option<Cow<'a, DataType>> {
        self.transform_inner_type(vtype, "<map value>")
    }

    fn transform_struct_field<'a>(
        &mut self,
        field: &'a StructField,
    ) -> Option<Cow<'a, StructField>> {
        if self.err.is_none() {
            self.path.push(field.name.clone());
            self.check_annotations(field);
            let _ = self.recurse_into_struct_field(field);
            self.path.pop();
        }
        None
    }
}

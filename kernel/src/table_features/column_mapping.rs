//! Code to handle column mapping, including modes and schema transforms
use super::ReaderFeatures;
use crate::actions::Protocol;
use crate::schema::{ColumnName, DataType, MetadataValue, Schema, SchemaTransform, StructField};
use crate::table_properties::TableProperties;
use crate::{DeltaResult, Error};

use std::borrow::Cow;

use serde::{Deserialize, Serialize};
use strum::EnumString;

/// Modes of column mapping a table can be in
#[derive(Debug, Default, EnumString, Serialize, Deserialize, Copy, Clone, PartialEq, Eq)]
#[strum(serialize_all = "camelCase")]
#[serde(rename_all = "camelCase")]
pub enum ColumnMappingMode {
    /// No column mapping is applied
    None,
    /// Columns are mapped by their field_id in parquet
    Id,
    /// Columns are mapped to a physical name
    #[default]
    Name,
}

/// Determine the column mapping mode for a table based on the [`Protocol`] and [`TableProperties`]
pub(crate) fn column_mapping_mode(
    protocol: &Protocol,
    table_properties: &TableProperties,
) -> ColumnMappingMode {
    match (
        table_properties.column_mapping_mode,
        protocol.min_reader_version(),
    ) {
        // NOTE: The table property is optional even when the feature is supported, and should be
        // ignored when the feature is not supported. For details see
        // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#column-mapping
        (Some(mode), 2) => mode,
        (Some(mode), 3) if protocol.has_reader_feature(&ReaderFeatures::ColumnMapping) => mode,
        _ => ColumnMappingMode::None,
    }
}

/// When column mapping mode is enabled, verify that each field in the schema is annotated with a
/// physical name and field_id; when not enabled, verify that no fields are annotated.
pub fn validate_schema_column_mapping(schema: &Schema, mode: ColumnMappingMode) -> DeltaResult<()> {
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
    match validator.err {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

struct ValidateColumnMappings<'a> {
    mode: ColumnMappingMode,
    path: Vec<&'a str>,
    err: Option<Error>,
}

impl<'a> ValidateColumnMappings<'a> {
    fn transform_inner_type(
        &mut self,
        data_type: &'a DataType,
        name: &'a str,
    ) -> Option<Cow<'a, DataType>> {
        if self.err.is_none() {
            self.path.push(name);
            let _ = self.transform(data_type);
            self.path.pop();
        }
        None
    }
    fn check_annotations(&mut self, field: &StructField) {
        // The iterator yields `&&str` but `ColumnName::new` needs `&str`
        let column_name = || ColumnName::new(self.path.iter().copied());
        let annotation = "delta.columnMapping.physicalName";
        match (self.mode, field.metadata.get(annotation)) {
            // Both Id and Name modes require a physical name annotation; None mode forbids it.
            (ColumnMappingMode::None, None) => {}
            (ColumnMappingMode::Name | ColumnMappingMode::Id, Some(MetadataValue::String(_))) => {}
            (ColumnMappingMode::Name | ColumnMappingMode::Id, Some(_)) => {
                self.err = Some(Error::invalid_column_mapping_mode(format!(
                    "The {annotation} annotation on field '{}' must be a string",
                    column_name()
                )));
            }
            (ColumnMappingMode::None, Some(_)) => {
                self.err = Some(Error::invalid_column_mapping_mode(format!(
                    "Column mapping is not enabled but field '{annotation}' is annotated with {}",
                    column_name()
                )));
            }
            (ColumnMappingMode::Name | ColumnMappingMode::Id, None) => {
                self.err = Some(Error::invalid_column_mapping_mode(format!(
                    "Column mapping is enabled but field '{}' lacks the {annotation} annotation",
                    column_name()
                )));
            }
        }

        let annotation = "delta.columnMapping.id";
        match (self.mode, field.metadata.get(annotation)) {
            // Both Id and Name modes require a field ID annotation; None mode forbids it.
            (ColumnMappingMode::None, None) => {}
            (ColumnMappingMode::Name | ColumnMappingMode::Id, Some(MetadataValue::Number(_))) => {}
            (ColumnMappingMode::Name | ColumnMappingMode::Id, Some(_)) => {
                self.err = Some(Error::invalid_column_mapping_mode(format!(
                    "The {annotation} annotation on field '{}' must be a number",
                    column_name()
                )));
            }
            (ColumnMappingMode::None, Some(_)) => {
                self.err = Some(Error::invalid_column_mapping_mode(format!(
                    "Column mapping is not enabled but field '{}' is annotated with {annotation}",
                    column_name()
                )));
            }
            (ColumnMappingMode::Name | ColumnMappingMode::Id, None) => {
                self.err = Some(Error::invalid_column_mapping_mode(format!(
                    "Column mapping is enabled but field '{}' lacks the {annotation} annotation",
                    column_name()
                )));
            }
        }
    }
}

impl<'a> SchemaTransform<'a> for ValidateColumnMappings<'a> {
    // Override array element and map key/value for better error messages
    fn transform_array_element(&mut self, etype: &'a DataType) -> Option<Cow<'a, DataType>> {
        self.transform_inner_type(etype, "<array element>")
    }
    fn transform_map_key(&mut self, ktype: &'a DataType) -> Option<Cow<'a, DataType>> {
        self.transform_inner_type(ktype, "<map key>")
    }
    fn transform_map_value(&mut self, vtype: &'a DataType) -> Option<Cow<'a, DataType>> {
        self.transform_inner_type(vtype, "<map value>")
    }
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        if self.err.is_none() {
            self.path.push(&field.name);
            self.check_annotations(field);
            let _ = self.recurse_into_struct_field(field);
            self.path.pop();
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_column_mapping_mode() {
        let table_properties: HashMap<_, _> =
            [("delta.columnMapping.mode".to_string(), "id".to_string())]
                .into_iter()
                .collect();
        let table_properties = TableProperties::from(table_properties.iter());
        let empty_table_properties = TableProperties::from([] as [(String, String); 0]);

        let protocol = Protocol::try_new(2, 5, None::<Vec<String>>, None::<Vec<String>>).unwrap();

        assert_eq!(
            column_mapping_mode(&protocol, &table_properties),
            ColumnMappingMode::Id
        );

        assert_eq!(
            column_mapping_mode(&protocol, &empty_table_properties),
            ColumnMappingMode::None
        );

        let empty_features = Some::<[String; 0]>([]);
        let protocol =
            Protocol::try_new(3, 7, empty_features.clone(), empty_features.clone()).unwrap();

        assert_eq!(
            column_mapping_mode(&protocol, &table_properties),
            ColumnMappingMode::None
        );

        assert_eq!(
            column_mapping_mode(&protocol, &empty_table_properties),
            ColumnMappingMode::None
        );

        let protocol = Protocol::try_new(
            3,
            7,
            Some([ReaderFeatures::ColumnMapping]),
            empty_features.clone(),
        )
        .unwrap();

        assert_eq!(
            column_mapping_mode(&protocol, &table_properties),
            ColumnMappingMode::Id
        );

        assert_eq!(
            column_mapping_mode(&protocol, &empty_table_properties),
            ColumnMappingMode::None
        );

        let protocol = Protocol::try_new(
            3,
            7,
            Some([ReaderFeatures::DeletionVectors]),
            empty_features.clone(),
        )
        .unwrap();

        assert_eq!(
            column_mapping_mode(&protocol, &table_properties),
            ColumnMappingMode::None
        );

        assert_eq!(
            column_mapping_mode(&protocol, &empty_table_properties),
            ColumnMappingMode::None
        );

        let protocol = Protocol::try_new(
            3,
            7,
            Some([
                ReaderFeatures::DeletionVectors,
                ReaderFeatures::ColumnMapping,
            ]),
            empty_features,
        )
        .unwrap();

        assert_eq!(
            column_mapping_mode(&protocol, &table_properties),
            ColumnMappingMode::Id
        );

        assert_eq!(
            column_mapping_mode(&protocol, &empty_table_properties),
            ColumnMappingMode::None
        );
    }
}

//! Identity-preserving transitions for complete, column-mapped schemas.

use std::borrow::Cow;
use std::collections::HashMap;

use crate::actions::Metadata;
use crate::schema::diff::{field_paths_by_id, fields_by_id};
use crate::schema::validation::validate_schema;
use crate::schema::{
    normalize_column_names_to_schema_casing, ArrayType, ColumnMetadataKey, DataType, MapType,
    MetadataValue, PrimitiveType, SchemaRef, StructField, StructType,
};
use crate::table_configuration::TableConfiguration;
use crate::table_features::{
    assign_column_mapping_metadata, find_max_column_id_in_schema, is_iceberg_allowed_type_change,
    validate_column_mapping_id, ColumnMappingMode, TableFeature,
};
use crate::table_properties::{COLUMN_MAPPING_MAX_COLUMN_ID, DATA_SKIPPING_STATS_COLUMNS};
use crate::transforms::{transform_output_type, SchemaTransform};
use crate::{KernelError, KernelResult};

/// Validates a complete schema transition and returns its effective table configuration.
///
/// Assigns identities to new fields and restores omitted, Kernel-controlled metadata on surviving
/// fields. Returns an error for identity reuse, relocation, incompatible types, or invalid
/// defaults.
pub(crate) fn replace_schema(
    config: &TableConfiguration,
    requested: SchemaRef,
) -> KernelResult<TableConfiguration> {
    if config.column_mapping_mode() == ColumnMappingMode::None {
        return Err(KernelError::schema(
            "Complete schema updates require column mapping",
        ));
    }
    let old = config.logical_schema();
    let old_fields = fields_by_id(&old).map_err(|e| KernelError::schema(e.to_string()))?;
    let historical_max = config
        .table_properties()
        .column_mapping_max_column_id
        .ok_or_else(|| {
            KernelError::schema(
                "Complete schema updates require a valid delta.columnMapping.maxColumnId",
            )
        })?
        .max(find_max_column_id_in_schema(&old).unwrap_or(0));
    validate_column_mapping_id(historical_max)?;
    // Reserve explicitly supplied IDs before allocating any missing IDs, regardless of field order.
    let mut max_id = historical_max.max(find_max_column_id_in_schema(&requested).unwrap_or(0));
    validate_column_mapping_id(max_id)?;
    let mut mapping = PrepareMapping {
        old_fields: &old_fields,
        max_id: &mut max_id,
        nested_ids: config.is_feature_enabled(&TableFeature::IcebergCompatV2)
            || config.is_feature_enabled(&TableFeature::IcebergCompatV3),
    };
    let requested = mapping.transform_struct(&requested)?.into_owned();
    // The diff deliberately suppresses descendants of additions. Check every new identity here.
    for (id, field) in fields_by_id(&requested).map_err(|e| KernelError::schema(e.to_string()))? {
        super::reject_generated_or_identity_metadata(&field)?;
        if field.is_metadata_column() {
            return Err(KernelError::schema(
                "Metadata columns cannot be stored in a table schema",
            ));
        }
        if !old_fields.contains_key(&id) {
            if let Some(MetadataValue::String(name)) =
                field.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
            {
                let properties = config.table_properties();
                if [
                    properties.materialized_row_id_column_name.as_deref(),
                    properties
                        .materialized_row_commit_version_column_name
                        .as_deref(),
                ]
                .contains(&Some(name.as_str()))
                {
                    return Err(KernelError::schema(
                        "New field reuses a row-tracking materialized name",
                    ));
                }
            }
            if old_fields.values().any(|old| {
                old.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
                    == field.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
            }) {
                return Err(KernelError::schema(
                    "New field reuses an existing physical name",
                ));
            }
            if id <= historical_max {
                return Err(KernelError::schema(format!(
                    "New field '{}' reuses historical mapping ID {id}",
                    field.name()
                )));
            }
            if !field.is_nullable() || field.column_default()?.is_some() {
                return Err(KernelError::schema(format!(
                    "New field '{}' must be nullable and cannot have a default",
                    field.name()
                )));
            }
            if field
                .get_config_value(&ColumnMetadataKey::TypeChanges)
                .is_some()
            {
                return Err(KernelError::schema(
                    "New fields cannot supply type-change history",
                ));
            }
        }
    }
    let schema: SchemaRef = transition_struct(&old, &requested, config, &old_fields)?.into();
    validate_schema(
        &schema,
        config.column_mapping_mode(),
        config.is_feature_enabled(&TableFeature::ChangeDataFeed),
    )?;
    for name in config.metadata().partition_columns() {
        let previous = old
            .field(name)
            .ok_or_else(|| KernelError::schema("Missing partition column"))?;
        let current = schema
            .field(name)
            .ok_or_else(|| KernelError::schema("Cannot drop or rename a partition column"))?;
        if field_id(previous)? != field_id(current)? || previous.data_type() != current.data_type()
        {
            return Err(KernelError::schema(
                "Cannot replace or change the type of a partition column",
            ));
        }
    }
    let metadata = config
        .metadata()
        .clone()
        .with_schema(schema.clone())?
        .with_configuration_entry(COLUMN_MAPPING_MAX_COLUMN_ID, max_id.to_string());
    let metadata = update_stats_columns(config, &schema, metadata)?;
    TableConfiguration::try_new_with_schema(config, metadata, schema)
}

fn update_stats_columns(
    old: &TableConfiguration,
    schema: &StructType,
    metadata: Metadata,
) -> KernelResult<Metadata> {
    let Some(columns) = old.table_properties().data_skipping_stats_columns.as_ref() else {
        return Ok(metadata);
    };
    // An explicitly changed selection already uses the final schema's logical names.
    if metadata.configuration().get(DATA_SKIPPING_STATS_COLUMNS)
        != old
            .metadata()
            .configuration()
            .get(DATA_SKIPPING_STATS_COLUMNS)
    {
        return Ok(metadata);
    }
    let old_paths = field_paths_by_id(old.logical_schema_ref())
        .map_err(|e| KernelError::schema(e.to_string()))?
        .into_iter()
        .map(|(id, path)| (path, id))
        .collect::<HashMap<_, _>>();
    let new_paths = field_paths_by_id(schema).map_err(|e| KernelError::schema(e.to_string()))?;
    let normalized = normalize_column_names_to_schema_casing(old.logical_schema_ref(), columns);
    let updated = normalized
        .iter()
        .zip(columns)
        .filter_map(|(path, original)| match old_paths.get(path) {
            Some(id) => new_paths.get(id),
            None => Some(original),
        })
        .cloned()
        .collect::<Vec<_>>();
    if &updated == columns {
        return Ok(metadata);
    }
    Ok(metadata.with_configuration_entry(
        DATA_SKIPPING_STATS_COLUMNS,
        updated
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(","),
    ))
}

struct PrepareMapping<'a> {
    old_fields: &'a HashMap<i64, StructField>,
    max_id: &'a mut i64,
    nested_ids: bool,
}

impl<'a> SchemaTransform<'a> for PrepareMapping<'_> {
    transform_output_type!(|'a, T| KernelResult<Cow<'a, T>>);

    fn transform_variant(&mut self, stype: &'a StructType) -> KernelResult<Cow<'a, StructType>> {
        Ok(Cow::Borrowed(stype))
    }

    fn transform_struct_field(
        &mut self,
        field: &'a StructField,
    ) -> KernelResult<Cow<'a, StructField>> {
        let old = field
            .get_config_value(&ColumnMetadataKey::ColumnMappingId)
            .and_then(|id| match id {
                MetadataValue::Number(id) => self.old_fields.get(id),
                _ => None,
            });
        if let Some(old) = old {
            let mut result = self.recurse_into_struct_field(field)?.into_owned();
            for key in [
                ColumnMetadataKey::ColumnMappingPhysicalName,
                ColumnMetadataKey::ColumnMappingNestedIds,
                ColumnMetadataKey::TypeChanges,
            ] {
                let previous = old.get_config_value(&key);
                let supplied = result.get_config_value(&key);
                if supplied.is_some() && supplied != previous {
                    return Err(KernelError::schema(format!(
                        "Cannot modify {} for field '{}'",
                        key.as_ref(),
                        field.name()
                    )));
                }
                if let Some(value) = previous {
                    result
                        .metadata
                        .insert(key.as_ref().to_string(), value.clone());
                }
            }
            Ok(Cow::Owned(result))
        } else {
            // A snapshot cannot prove that a supplied name was never used by a dropped field.
            RejectNewPhysicalNames.transform_struct_field(field)?;
            let schema = StructType::try_new([field.clone()])?;
            let mapped = assign_column_mapping_metadata(&schema, self.max_id, self.nested_ids)?;
            mapped
                .into_fields()
                .next()
                .map(Cow::Owned)
                .ok_or_else(|| KernelError::internal_error("mapping discarded a field"))
        }
    }
}

struct RejectNewPhysicalNames;

impl<'a> SchemaTransform<'a> for RejectNewPhysicalNames {
    transform_output_type!(|'a, T| KernelResult<Cow<'a, T>>);

    fn transform_struct_field(
        &mut self,
        field: &'a StructField,
    ) -> KernelResult<Cow<'a, StructField>> {
        if field
            .get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
            .is_some()
        {
            return Err(KernelError::schema(
                "New fields cannot supply physical names; Kernel allocates fresh names",
            ));
        }
        self.recurse_into_struct_field(field)
    }

    fn transform_variant(&mut self, stype: &'a StructType) -> KernelResult<Cow<'a, StructType>> {
        Ok(Cow::Borrowed(stype))
    }
}

fn field_id(field: &StructField) -> KernelResult<i64> {
    match field.get_config_value(&ColumnMetadataKey::ColumnMappingId) {
        Some(MetadataValue::Number(id)) => Ok(*id),
        _ => Err(KernelError::schema("Missing column mapping ID")),
    }
}

fn transition_struct(
    old: &StructType,
    new: &StructType,
    config: &TableConfiguration,
    all_old: &HashMap<i64, StructField>,
) -> KernelResult<StructType> {
    let siblings = old
        .fields()
        .map(|field| Ok((field_id(field)?, field)))
        .collect::<KernelResult<HashMap<_, _>>>()?;
    new.fields()
        .map(|field| {
            let id = field_id(field)?;
            let Some(previous) = siblings.get(&id) else {
                if all_old.contains_key(&id) {
                    return Err(KernelError::schema(format!(
                        "Cannot relocate field '{}' between parents",
                        field.name()
                    )));
                }
                // A new parent must not contain a surviving field from another parent.
                for id in fields_by_id(&StructType::try_new([field.clone()])?)
                    .map_err(|e| KernelError::schema(e.to_string()))?
                    .keys()
                {
                    if all_old.contains_key(id) {
                        return Err(KernelError::schema(
                            "Cannot relocate a surviving field into a new parent",
                        ));
                    }
                }
                return Ok(field.clone());
            };
            if previous.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
                != field.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
            {
                return Err(KernelError::schema(format!(
                    "Cannot change physical name of '{}'",
                    field.name()
                )));
            }
            if previous.is_nullable() && !field.is_nullable() {
                return Err(KernelError::schema(format!(
                    "Cannot tighten nullability of '{}'",
                    field.name()
                )));
            }
            let mut result = field.clone();
            let mut changes = Vec::new();
            result.data_type = transition_type(
                previous.data_type(),
                field.data_type(),
                config,
                all_old,
                &mut changes,
                "",
            )?;
            if previous.get_config_value(&ColumnMetadataKey::CurrentDefault)
                != field.get_config_value(&ColumnMetadataKey::CurrentDefault)
                || previous.data_type() != field.data_type()
            {
                if let Some(default) = field.column_default()? {
                    if !config.is_feature_supported(&TableFeature::AllowColumnDefaults)
                        || !default.is_kernel_parsable_literal()
                    {
                        return Err(KernelError::schema(format!(
                            "Unsupported default change for '{}'",
                            field.name()
                        )));
                    }
                }
            }
            if !changes.is_empty() {
                let mut history = match previous.get_config_value(&ColumnMetadataKey::TypeChanges) {
                    None => Vec::new(),
                    Some(MetadataValue::Other(serde_json::Value::Array(values))) => values.clone(),
                    _ => return Err(KernelError::schema("Malformed type-change history")),
                };
                history.extend(changes);
                result.metadata.insert(
                    ColumnMetadataKey::TypeChanges.as_ref().to_string(),
                    MetadataValue::Other(history.into()),
                );
            }
            Ok(result)
        })
        .collect::<KernelResult<Vec<_>>>()
        .and_then(StructType::try_new)
}

fn transition_type(
    old: &DataType,
    new: &DataType,
    config: &TableConfiguration,
    all_old: &HashMap<i64, StructField>,
    changes: &mut Vec<serde_json::Value>,
    path: &str,
) -> KernelResult<DataType> {
    let child_path = |segment: &str| {
        if path.is_empty() {
            segment.to_string()
        } else {
            format!("{path}.{segment}")
        }
    };
    match (old, new) {
        (DataType::Struct(a), DataType::Struct(b)) => {
            Ok(transition_struct(a, b, config, all_old)?.into())
        }
        (DataType::Array(a), DataType::Array(b)) if !a.contains_null() || b.contains_null() => {
            Ok(ArrayType::new(
                transition_type(
                    a.element_type(),
                    b.element_type(),
                    config,
                    all_old,
                    changes,
                    &child_path("element"),
                )?,
                b.contains_null(),
            )
            .into())
        }
        (DataType::Map(a), DataType::Map(b))
            if !a.value_contains_null() || b.value_contains_null() =>
        {
            Ok(MapType::new(
                transition_type(
                    a.key_type(),
                    b.key_type(),
                    config,
                    all_old,
                    changes,
                    &child_path("key"),
                )?,
                transition_type(
                    a.value_type(),
                    b.value_type(),
                    config,
                    all_old,
                    changes,
                    &child_path("value"),
                )?,
                b.value_contains_null(),
            )
            .into())
        }
        _ if old == new => Ok(new.clone()),
        (DataType::Primitive(a), DataType::Primitive(b))
            if (config.is_feature_enabled(&TableFeature::TypeWidening)
                || config.is_feature_enabled(&TableFeature::TypeWideningPreview))
                && can_widen(a, b) =>
        {
            if [TableFeature::IcebergCompatV2, TableFeature::IcebergCompatV3]
                .iter()
                .any(|feature| config.is_feature_supported(feature))
                && !is_iceberg_allowed_type_change(old, new)
            {
                return Err(KernelError::schema(format!(
                    "Iceberg compatibility forbids type change from {old} to {new}"
                )));
            }
            let mut record = serde_json::json!({"fromType": old, "toType": new});
            if config.is_feature_supported(&TableFeature::TypeWideningPreview) {
                // Preview clients use the version to identify files that need rewriting.
                record["tableVersion"] = config
                    .version()
                    .checked_add(1)
                    .ok_or_else(|| KernelError::schema("Type-change version overflow"))?
                    .into();
            }
            if !path.is_empty() {
                record["fieldPath"] = path.into();
            }
            changes.push(record);
            Ok(new.clone())
        }
        _ => Err(KernelError::schema(format!(
            "Unsupported schema transition from {old} to {new} at '{path}'"
        ))),
    }
}

fn can_widen(from: &PrimitiveType, to: &PrimitiveType) -> bool {
    match (from, to) {
        (
            PrimitiveType::Byte,
            PrimitiveType::Short
            | PrimitiveType::Integer
            | PrimitiveType::Long
            | PrimitiveType::Double,
        )
        | (
            PrimitiveType::Short,
            PrimitiveType::Integer | PrimitiveType::Long | PrimitiveType::Double,
        )
        | (PrimitiveType::Integer, PrimitiveType::Long | PrimitiveType::Double)
        | (PrimitiveType::Float, PrimitiveType::Double)
        | (PrimitiveType::Date, PrimitiveType::TimestampNtz) => true,
        (PrimitiveType::Decimal(a), PrimitiveType::Decimal(b)) => {
            b.scale() >= a.scale() && b.precision() - b.scale() >= a.precision() - a.scale()
        }
        (
            PrimitiveType::Byte | PrimitiveType::Short | PrimitiveType::Integer,
            PrimitiveType::Decimal(b),
        ) => b.precision() - b.scale() >= 10,
        (PrimitiveType::Long, PrimitiveType::Decimal(b)) => b.precision() - b.scale() >= 20,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use url::Url;

    use super::*;
    use crate::actions::{Metadata, Protocol};
    use crate::schema::{schema, schema_ref};

    fn mapped(name: &str, id: i64, data_type: impl Into<DataType>) -> StructField {
        StructField::nullable(name, data_type).add_metadata([
            (
                ColumnMetadataKey::ColumnMappingId.as_ref(),
                MetadataValue::Number(id),
            ),
            (
                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                MetadataValue::String(format!("physical_{id}")),
            ),
        ])
    }

    fn config(schema: StructType, widening: bool) -> TableConfiguration {
        let mut properties = HashMap::from([
            ("delta.columnMapping.mode".into(), "name".into()),
            (COLUMN_MAPPING_MAX_COLUMN_ID.into(), "10".into()),
        ]);
        let mut readers = vec![TableFeature::ColumnMapping];
        let mut writers = vec![
            TableFeature::ColumnMapping,
            TableFeature::AllowColumnDefaults,
        ];
        if widening {
            properties.insert("delta.enableTypeWidening".into(), "true".into());
            readers.push(TableFeature::TypeWidening);
            writers.push(TableFeature::TypeWidening);
        }
        let metadata = Metadata::try_new(None, None, schema.into(), vec![], 0, properties).unwrap();
        TableConfiguration::try_new(
            metadata,
            Protocol::try_new_modern(readers, writers).unwrap(),
            Url::parse("memory:///").unwrap(),
            0,
        )
        .unwrap()
    }

    #[rstest]
    #[case::missing(None)]
    #[case::malformed(Some("invalid"))]
    fn complete_schema_rejects_unavailable_historical_max_id(#[case] watermark: Option<&str>) {
        let schema = schema! { (mapped("survivor", 1, DataType::INTEGER)) };
        let mut properties = HashMap::from([("delta.columnMapping.mode".into(), "name".into())]);
        if let Some(watermark) = watermark {
            properties.insert(COLUMN_MAPPING_MAX_COLUMN_ID.into(), watermark.into());
        }
        let metadata =
            Metadata::try_new(None, None, schema.clone().into(), vec![], 0, properties).unwrap();
        let config = TableConfiguration::try_new(
            metadata,
            Protocol::try_new_modern([TableFeature::ColumnMapping], [TableFeature::ColumnMapping])
                .unwrap(),
            Url::parse("memory:///").unwrap(),
            1,
        )
        .unwrap();
        // The surviving schema cannot establish the maximum ID of previously dropped fields.
        let requested =
            schema! { (mapped("survivor", 1, DataType::INTEGER)), nullable "new": STRING };
        let error = replace_schema(&config, requested.into()).unwrap_err();
        assert!(error.to_string().contains("maxColumnId"));
    }

    #[rstest]
    fn complete_schema_renames_drops_reorders_and_allocates(#[values(false, true)] nested: bool) {
        let old =
            schema! { (mapped("a", 1, DataType::INTEGER)), (mapped("b", 2, DataType::STRING)) };
        let new = schema! { nullable "new": STRING, (mapped("renamed", 1, DataType::INTEGER)) };
        let wrap = |s| {
            if nested {
                schema! { (mapped("parent", 3, ArrayType::new(s, true))) }
            } else {
                s
            }
        };
        let tc = config(wrap(old), false);
        let result = replace_schema(&tc, wrap(new).into()).unwrap();
        let fields = fields_by_id(result.logical_schema_ref()).unwrap();
        assert_eq!(fields[&1].name(), "renamed");
        assert!(!fields.contains_key(&2));
        assert_eq!(fields[&11].name(), "new");
        assert_eq!(
            result.table_properties().column_mapping_max_column_id,
            Some(11)
        );
    }

    #[rstest]
    #[case::historical_id(StructField::nullable("new", DataType::INTEGER).add_metadata([("delta.columnMapping.id", MetadataValue::Number(9))]), "historical")]
    #[case::physical_name(mapped("a", 1, DataType::INTEGER).add_metadata([(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(), MetadataValue::String("different".into()))]), "physical")]
    #[case::narrowing(mapped("a", 1, DataType::BYTE), "transition")]
    #[case::structural(mapped("a", 1, ArrayType::new(DataType::INTEGER, true)), "transition")]
    #[case::new_default(StructField::nullable("new", DataType::INTEGER).add_metadata([("CURRENT_DEFAULT", MetadataValue::String("1".into()))]), "cannot have a default")]
    #[case::unsupported_default(mapped("a", 1, DataType::INTEGER).add_metadata([("CURRENT_DEFAULT", MetadataValue::String("1 + 1".into()))]), "default change")]
    fn complete_schema_rejects_invalid_transition(
        #[case] field: StructField,
        #[case] message: &str,
    ) {
        let tc = config(schema! { (mapped("a", 1, DataType::INTEGER)) }, true);
        let error = replace_schema(&tc, schema_ref! { (field) }).unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
    }

    #[rstest]
    fn complete_schema_widening_authors_and_restores_history(#[values(false, true)] nested: bool) {
        let wrap = |ty| {
            if nested {
                DataType::from(ArrayType::new(
                    MapType::new(DataType::STRING, ty, true),
                    true,
                ))
            } else {
                ty
            }
        };
        let tc = config(schema! { (mapped("a", 1, wrap(DataType::INTEGER))) }, true);
        let next = schema_ref! { (mapped("a", 1, wrap(DataType::LONG))) };
        let result = replace_schema(&tc, next.clone()).unwrap();
        let field = result.logical_schema_ref().field("a").unwrap();
        let history = field
            .get_config_value(&ColumnMetadataKey::TypeChanges)
            .unwrap();
        let mut expected = serde_json::json!({"fromType": "integer", "toType": "long"});
        if nested {
            expected["fieldPath"] = "element.value".into();
        }
        assert_eq!(
            history,
            &MetadataValue::Other(serde_json::json!([expected]))
        );
        let restored = replace_schema(&result, next).unwrap();
        assert_eq!(restored.logical_schema(), result.logical_schema());
        let tampered = schema_ref! { (mapped("a", 1, wrap(DataType::LONG)).add_metadata([("delta.typeChanges", MetadataValue::Other(serde_json::json!([])))])) };
        assert!(replace_schema(&result, tampered).is_err());
    }

    #[test]
    fn complete_schema_preview_widening_records_commit_version() {
        let tc = config(schema! { (mapped("a", 1, DataType::INTEGER)) }, true);
        let protocol = Protocol::try_new_modern(
            vec![
                TableFeature::ColumnMapping,
                TableFeature::TypeWideningPreview,
            ],
            vec![
                TableFeature::ColumnMapping,
                TableFeature::TypeWideningPreview,
                TableFeature::AllowColumnDefaults,
            ],
        )
        .unwrap();
        let tc = TableConfiguration::try_new_from(&tc, None, Some(protocol), 42).unwrap();
        let updated =
            replace_schema(&tc, schema_ref! { (mapped("a", 1, DataType::LONG)) }).unwrap();
        assert_eq!(
            updated
                .logical_schema_ref()
                .field("a")
                .unwrap()
                .get_config_value(&ColumnMetadataKey::TypeChanges),
            Some(&MetadataValue::Other(
                serde_json::json!([{"fromType": "integer", "toType": "long", "tableVersion": 43}])
            ))
        );
    }

    #[test]
    fn complete_schema_rejects_relocation_and_nullability_tightening() {
        let tc = config(
            schema! { (mapped("parent", 1, schema! { (mapped("child", 2, DataType::INTEGER)) })) },
            false,
        );
        assert!(
            replace_schema(&tc, schema_ref! { (mapped("child", 2, DataType::INTEGER)) }).is_err()
        );
        let tc = config(schema! { (mapped("a", 1, DataType::INTEGER)) }, false);
        let mut required = mapped("a", 1, DataType::INTEGER);
        required.nullable = false;
        assert!(replace_schema(&tc, schema_ref! { (required) }).is_err());
        assert!(replace_schema(&tc, schema_ref! { (mapped("a", 1, DataType::LONG)) }).is_err());
    }

    #[rstest]
    #[case("reserved_row_id")]
    #[case("reserved_row_version")]
    fn complete_schema_rejects_new_field_reusing_row_tracking_name(#[case] name: &str) {
        let tc = config(schema! { (mapped("a", 1, DataType::INTEGER)) }, false);
        let metadata = tc
            .metadata()
            .clone()
            .with_configuration_entry("delta.enableRowTracking", "true")
            .with_configuration_entry(
                "delta.rowTracking.materializedRowIdColumnName",
                "reserved_row_id",
            )
            .with_configuration_entry(
                "delta.rowTracking.materializedRowCommitVersionColumnName",
                "reserved_row_version",
            );
        let protocol = Protocol::try_new_modern(
            vec![TableFeature::ColumnMapping],
            vec![
                TableFeature::ColumnMapping,
                TableFeature::AllowColumnDefaults,
                TableFeature::RowTracking,
                TableFeature::DomainMetadata,
            ],
        )
        .unwrap();
        let tc = TableConfiguration::try_new_from(&tc, Some(metadata), Some(protocol), 0).unwrap();
        let field = mapped("new", 11, DataType::INTEGER).add_metadata([(
            ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
            MetadataValue::String(name.into()),
        )]);
        assert!(replace_schema(&tc, schema_ref! { (field) }).is_err());
    }

    #[test]
    fn complete_schema_restores_omitted_physical_name() {
        let tc = config(schema! { (mapped("a", 1, DataType::INTEGER)) }, false);
        let renamed = StructField::nullable("renamed", DataType::INTEGER)
            .add_metadata([("delta.columnMapping.id", MetadataValue::Number(1))]);
        let updated = replace_schema(&tc, schema_ref! { (renamed) }).unwrap();
        assert_eq!(
            updated
                .logical_schema_ref()
                .field("renamed")
                .unwrap()
                .get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName),
            tc.logical_schema_ref()
                .field("a")
                .unwrap()
                .get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName),
        );
    }

    #[test]
    fn complete_schema_rejects_supplied_physical_name_inside_new_subtree() {
        let tc = config(schema! { (mapped("a", 1, DataType::INTEGER)) }, false);
        let requested = schema_ref! {
            nullable "parent": [nullable (schema! { (mapped("new", 11, DataType::STRING)) })],
        };
        let error = replace_schema(&tc, requested).unwrap_err();
        assert!(error
            .to_string()
            .contains("New fields cannot supply physical names"));
    }

    #[rstest]
    fn complete_schema_stats_columns_follow_identity(#[values(false, true)] nested: bool) {
        let old = schema! {
            (mapped("keep.with.dot", 1, DataType::INTEGER)),
            (mapped("drop", 2, DataType::STRING)),
        };
        let new = schema! {
            (mapped("renamed", 1, DataType::INTEGER)),
            nullable "drop": STRING,
        };
        let (old, new, selection, expected) = if nested {
            (
                schema! { (mapped("parent", 3, old)) },
                schema! { (mapped("renamed_parent", 3, new)) },
                "parent.`keep.with.dot`,parent.drop,missing",
                vec![
                    crate::expressions::column_name!("renamed_parent.renamed"),
                    crate::expressions::column_name!("missing"),
                ],
            )
        } else {
            (
                old,
                new,
                "`keep.with.dot`,drop,missing",
                vec![
                    crate::expressions::column_name!("renamed"),
                    crate::expressions::column_name!("missing"),
                ],
            )
        };
        let tc = config(old, false);
        let metadata = tc
            .metadata()
            .clone()
            .with_configuration_entry("delta.dataSkippingStatsColumns", selection);
        let tc = TableConfiguration::try_new_from(&tc, Some(metadata), None, 0).unwrap();
        let result = replace_schema(&tc, new.into()).unwrap();
        assert_eq!(
            result.table_properties().data_skipping_stats_columns,
            Some(expected)
        );
    }

    #[test]
    fn complete_schema_accepts_literal_defaults_and_preserves_max_id_on_drop() {
        let tc = config(
            schema! { (mapped("a", 1, DataType::INTEGER)), (mapped("b", 2, DataType::STRING)) },
            false,
        );
        let result = replace_schema(&tc, schema_ref! { (mapped("a", 1, DataType::INTEGER).add_metadata([("CURRENT_DEFAULT", MetadataValue::String("42".into()))])) }).unwrap();
        assert_eq!(
            result.table_properties().column_mapping_max_column_id,
            Some(10)
        );
        assert_eq!(
            result
                .logical_schema_ref()
                .field("a")
                .unwrap()
                .column_default()
                .unwrap()
                .unwrap()
                .raw_sql(),
            "42"
        );
    }
}

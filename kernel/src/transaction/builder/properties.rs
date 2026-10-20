//! Property and protocol preparation for existing-table transactions.

use std::collections::{HashMap, HashSet};

use super::create_table::{
    extract_table_features_and_properties, ALLOWED_DELTA_FEATURES, ALLOWED_DELTA_PROPERTIES,
};
use crate::actions::{Metadata, Protocol};
use crate::table_configuration::TableConfiguration;
use crate::table_features::{
    add_feature_to_lists, assign_nested_cm_ids, auto_enable_property_driven_features,
    ensure_domain_metadata_dependency, find_max_column_id_in_schema, ColumnMappingMode,
    FeatureRequirement, TableFeature, LEGACY_WRITER_FEATURES,
};
use crate::table_properties::*;
use crate::{KernelError, KernelResult, Version};

/// Merges replacement property collections and returns metadata and protocol for final validation.
///
/// `timestamp` is the actual ICT for a transaction enabling timestamps at `version`.
/// Rejects unsupported settings, overlap, and transitions that require migration or backfill.
pub(super) fn prepare_properties(
    old: &TableConfiguration,
    additions: Option<HashMap<String, String>>,
    removals: Option<Vec<String>>,
    version: Version,
    timestamp: i64,
) -> KernelResult<(Metadata, Protocol)> {
    if additions.is_none() && removals.is_none() {
        return Ok((old.metadata().clone(), old.protocol().clone()));
    }
    let mut allowed_features = ALLOWED_DELTA_FEATURES.to_vec();
    allowed_features.push(TableFeature::TimestampWithoutTimezone);
    allowed_features.push(TableFeature::AllowColumnDefaults);
    // Catalog ownership requires a separate integration with the catalog committer.
    allowed_features.retain(|feature| feature != &TableFeature::CatalogManaged);
    let added = normalize(additions.unwrap_or_default(), &allowed_features)?;
    let removed = removals.unwrap_or_default();
    for key in &removed {
        if key.to_ascii_lowercase().starts_with("delta.") {
            return Err(KernelError::invalid_transaction_state(
                "Unsetting delta.* table properties is unsupported",
            ));
        }
        if added.contains_key(key) {
            return Err(KernelError::invalid_transaction_state(format!(
                "Property '{key}' cannot be added and removed"
            )));
        }
    }
    let extracted =
        extract_table_features_and_properties(added, &allowed_features, "UPDATE TABLE")?;
    let disable_ict = extracted
        .properties
        .get(ENABLE_IN_COMMIT_TIMESTAMPS)
        .is_some_and(|v| v == "false");
    let mut configuration = old.metadata().configuration().clone();
    configuration.extend(extracted.properties);
    for key in removed {
        configuration.remove(&key);
    }
    let props = TableProperties::from(configuration.iter());
    let previous = old.table_properties();
    if props.enable_row_tracking.unwrap_or(false) != previous.enable_row_tracking.unwrap_or(false) {
        return Err(KernelError::unsupported(
            "Cannot enable or disable row tracking on an existing table",
        ));
    }
    if props.enable_iceberg_compat_v3.unwrap_or(false)
        != previous.enable_iceberg_compat_v3.unwrap_or(false)
    {
        return Err(KernelError::unsupported(
            "Cannot enable or disable IcebergCompatV3 on an existing table",
        ));
    }
    if props.column_mapping_mode.unwrap_or(ColumnMappingMode::None) != old.column_mapping_mode() {
        return Err(KernelError::unsupported(
            "Column mapping mode migrations are unsupported",
        ));
    }
    if disable_ict {
        if old.is_catalog_managed() {
            return Err(KernelError::unsupported(
                "Cannot disable in-commit timestamps on a catalog-managed table",
            ));
        }
        // End the enabled interval; re-enabling records the new commit version and timestamp.
        configuration.remove(IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION);
        configuration.remove(IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP);
    } else if props.enable_in_commit_timestamps == Some(true)
        && previous.enable_in_commit_timestamps != Some(true)
    {
        configuration.insert(
            IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.into(),
            version.to_string(),
        );
        configuration.insert(
            IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.into(),
            timestamp.to_string(),
        );
    }
    let mut readers = extracted.reader_features;
    let mut writers = extracted.writer_features;
    // The preview feature already satisfies property-driven enablement. A stable-feature
    // upgrade requires an explicit feature request, which remains in the extracted lists.
    if old.is_feature_supported(&TableFeature::TypeWideningPreview) {
        allowed_features.retain(|feature| feature != &TableFeature::TypeWidening);
    }
    auto_enable_property_driven_features(&allowed_features, &props, &mut readers, &mut writers);
    if props.checkpoint_policy == Some(CheckpointPolicy::V2) {
        add_feature_to_lists(TableFeature::V2Checkpoint, &mut readers, &mut writers);
    }
    let protocol = upgrade_protocol(old, writers)?;
    Ok((
        old.metadata().clone().with_configuration(configuration),
        protocol,
    ))
}

/// Adds requested features and their protocol dependencies, preserving all existing support.
/// Returns the original protocol when every requested feature is already supported.
pub(super) fn upgrade_protocol(
    old: &TableConfiguration,
    features: Vec<TableFeature>,
) -> KernelResult<Protocol> {
    let mut pending = features;
    let mut required = HashSet::new();
    while let Some(feature) = pending.pop() {
        if !required.insert(feature.clone()) {
            continue;
        }
        for dependency in feature.info().feature_requirements {
            if let FeatureRequirement::Supported(dep) | FeatureRequirement::Enabled(dep) =
                dependency
            {
                pending.push(dep.clone());
            }
        }
    }
    if required
        .iter()
        .all(|feature| old.is_feature_supported(feature))
    {
        return Ok(old.protocol().clone());
    }
    let mut readers = old
        .protocol()
        .reader_features()
        .unwrap_or_default()
        .to_vec();
    let mut writers = old
        .protocol()
        .writer_features()
        .unwrap_or_default()
        .to_vec();
    for feature in LEGACY_WRITER_FEATURES
        .iter()
        .filter(|feature| old.is_feature_supported(feature))
    {
        add_feature_to_lists(feature.clone(), &mut readers, &mut writers);
    }
    for feature in required {
        add_feature_to_lists(feature, &mut readers, &mut writers);
    }
    ensure_domain_metadata_dependency(&mut readers, &mut writers);
    readers.sort_by_key(ToString::to_string);
    writers.sort_by_key(ToString::to_string);
    let reader_version = if readers.is_empty() {
        old.protocol().min_reader_version()
    } else {
        3
    };
    Protocol::try_new(
        reader_version,
        7,
        (reader_version == 3).then_some(readers),
        Some(writers),
    )
}

/// Prepares nested identities when enabling Iceberg V2 and validates retained row-tracking names.
/// Returns the final configuration or a metadata validation error.
pub(super) fn finalize_properties(
    old: &TableConfiguration,
    mut final_config: TableConfiguration,
) -> KernelResult<TableConfiguration> {
    if final_config.table_properties().enable_iceberg_compat_v2 == Some(true)
        && old.table_properties().enable_iceberg_compat_v2 != Some(true)
    {
        let mut max_id = final_config
            .table_properties()
            .column_mapping_max_column_id
            .unwrap_or(0)
            .max(find_max_column_id_in_schema(final_config.logical_schema_ref()).unwrap_or(0));
        let schema = assign_nested_cm_ids(final_config.logical_schema_ref(), &mut max_id)?;
        let schema = std::sync::Arc::new(schema);
        let metadata = final_config
            .metadata()
            .clone()
            .with_schema(schema.clone())?
            .with_configuration_entry(COLUMN_MAPPING_MAX_COLUMN_ID, max_id.to_string());
        final_config = TableConfiguration::try_new_with_schema(&final_config, metadata, schema)?;
    }
    let props = final_config.table_properties();
    if props.enable_row_tracking == Some(true) {
        let (Some(row_id), Some(row_version)) = (
            &props.materialized_row_id_column_name,
            &props.materialized_row_commit_version_column_name,
        ) else {
            return Err(KernelError::invalid_protocol(
                "Row tracking requires materialized column names",
            ));
        };
        if row_id == row_version
            || row_id.is_empty()
            || row_version.is_empty()
            || final_config
                .physical_schema()
                .fields()
                .any(|field| field.name() == row_id || field.name() == row_version)
        {
            return Err(KernelError::schema("Row-tracking materialized names must be distinct and must not collide with data columns"));
        }
    }
    Ok(final_config)
}

fn normalize(
    properties: HashMap<String, String>,
    allowed_features: &[TableFeature],
) -> KernelResult<HashMap<String, String>> {
    let mut result = HashMap::new();
    for (key, mut value) in properties {
        let lower = key.to_ascii_lowercase();
        let key = if let Some(feature_name) = lower.strip_prefix("delta.feature.") {
            let feature = allowed_features
                .iter()
                .find(|f| f.to_string().eq_ignore_ascii_case(feature_name))
                .ok_or_else(|| {
                    KernelError::unsupported(format!("Unsupported table feature '{key}'"))
                })?;
            if !value.eq_ignore_ascii_case("supported") {
                return Err(KernelError::generic(format!(
                    "Invalid value for '{key}': expected supported"
                )));
            }
            value = "supported".into();
            format!("delta.feature.{feature}")
        } else if lower.starts_with("delta.") {
            let canonical = ALLOWED_DELTA_PROPERTIES
                .iter()
                .find(|candidate| candidate.eq_ignore_ascii_case(&key))
                .ok_or_else(|| {
                    KernelError::unsupported(format!(
                        "Unsupported or Kernel-controlled table property '{key}'"
                    ))
                })?;
            // Delta Boolean values are case-insensitive; preserve ordinary property values.
            if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false") {
                value = value.to_ascii_lowercase();
            }
            validate_value(canonical, &value)?;
            canonical.to_string()
        } else {
            key
        };
        if result.insert(key.clone(), value).is_some() {
            return Err(KernelError::generic(format!(
                "Duplicate normalized table property '{key}'"
            )));
        }
    }
    Ok(result)
}

fn validate_value(key: &str, value: &str) -> KernelResult<()> {
    let parsed = TableProperties::from([(key, value)]);
    let valid = !parsed.unknown_properties.contains_key(key)
        && match key {
            COLUMN_MAPPING_MODE => parsed.column_mapping_mode.is_some(),
            CHECKPOINT_POLICY => matches!(value, "classic" | "v2"),
            CHECKPOINT_INTERVAL => value.parse::<i32>().is_ok_and(|v| v > 0),
            DATA_SKIPPING_NUM_INDEXED_COLS => value.parse::<i32>().is_ok_and(|v| v >= -1),
            PARQUET_FORMAT_VERSION => {
                let parts: Vec<_> = value.split('.').collect();
                value == "1.0.0"
                    || parts.len() == 3
                        && parts[0] == "2"
                        && parts[1..].iter().all(|p| p.parse::<u32>().is_ok())
            }
            _ => true,
        };
    if !valid {
        return Err(KernelError::generic(format!(
            "Invalid value '{value}' for '{key}'"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::unit_test_utils::{load_test_table, MockTableConfigurationBuilder};

    #[rstest]
    #[case::ordinary("app.key", "value", false)]
    #[case::reassert("delta.enableTypeWidening", "true", false)]
    #[case::explicit_upgrade("delta.feature.typeWidening", "supported", true)]
    fn property_updates_preserve_preview_widening_unless_explicitly_upgraded(
        #[case] key: &str,
        #[case] value: &str,
        #[case] upgrades: bool,
    ) {
        let protocol = Protocol::try_new_modern(
            [TableFeature::TypeWideningPreview],
            [TableFeature::TypeWideningPreview],
        )
        .unwrap();
        let old = MockTableConfigurationBuilder::new()
            .with_protocol(protocol)
            .with_properties([(ENABLE_TYPE_WIDENING, "true")])
            .build();
        let (_, protocol) = prepare_properties(
            &old,
            Some(HashMap::from([(key.into(), value.into())])),
            None,
            1,
            42,
        )
        .unwrap();
        assert_eq!(protocol != *old.protocol(), upgrades);
        assert_eq!(
            protocol
                .writer_features()
                .unwrap()
                .contains(&TableFeature::TypeWidening),
            upgrades
        );
        assert!(protocol
            .writer_features()
            .unwrap()
            .contains(&TableFeature::TypeWideningPreview));
    }

    #[rstest]
    #[case("DELTA.EnableDeletionVectors", "TRUE", ENABLE_DELETION_VECTORS, "true")]
    #[case("delta.CHECKPOINTpolicy", "v2", CHECKPOINT_POLICY, "v2")]
    #[case("App.MixedCase", "TRUE", "App.MixedCase", "TRUE")]
    #[case("format-version", "3", "format-version", "3")]
    #[case(
        "databricks.delta.liquid.hierarchicalClusteringColumns",
        "a",
        "databricks.delta.liquid.hierarchicalClusteringColumns",
        "a"
    )]
    fn property_normalization_preserves_ordinary_keys(
        #[case] key: &str,
        #[case] value: &str,
        #[case] expected_key: &str,
        #[case] expected_value: &str,
    ) {
        let result = normalize(
            HashMap::from([(key.into(), value.into())]),
            ALLOWED_DELTA_FEATURES,
        )
        .unwrap();
        assert_eq!(
            result,
            HashMap::from([(expected_key.into(), expected_value.into())])
        );
    }

    #[rstest]
    #[case("delta.columnMapping.maxColumnId", "20")]
    #[case("DELTA.rowTracking.materializedRowIdColumnName", "new")]
    #[case("delta.inCommitTimestampEnablementTimestamp", "7")]
    #[case("delta.inCommitTimestampEnablementVersion", "7")]
    #[case("delta.enableIcebergWriterCompatV3", "true")]
    #[case("delta.universalFormat.enabledFormats", "iceberg")]
    #[case("delta.enableDeletionVectors", "yes")]
    #[case("delta.checkpointPolicy", "wrong")]
    #[case("delta.checkpointInterval", "0")]
    #[case("delta.checkpointInterval", "2147483648")]
    #[case("delta.dataSkippingNumIndexedCols", "2147483648")]
    #[case("delta.checkpointPolicy", "V2")]
    #[case("delta.dataSkippingNumIndexedCols", "-2")]
    #[case("delta.columnMapping.mode", "wrong")]
    #[case("delta.feature.timestampNtz", "false")]
    fn property_normalization_rejects_invalid_or_protected_settings(
        #[case] key: &str,
        #[case] value: &str,
    ) {
        assert!(normalize(
            HashMap::from([(key.into(), value.into())]),
            ALLOWED_DELTA_FEATURES
        )
        .is_err());
    }

    #[rstest]
    #[case("delta.enableRowTracking", "true")]
    #[case("delta.enableIcebergCompatV3", "true")]
    #[case("delta.columnMapping.mode", "name")]
    fn property_updates_reject_existing_table_migrations(
        #[case] key: &str,
        #[case] value: &str,
    ) -> KernelResult<()> {
        let (_engine, snapshot, _dir) = load_test_table("table-without-dv-small")?;
        assert!(prepare_properties(
            snapshot.table_configuration(),
            Some(HashMap::from([(key.into(), value.into())])),
            None,
            1,
            42
        )
        .is_err());
        Ok(())
    }

    #[rstest]
    #[case("DELTA.enableDeletionVectors")]
    #[case("delta.unknown")]
    #[case("App.Key")]
    fn property_removals_reject_delta_keys_and_overlap(#[case] key: &str) -> KernelResult<()> {
        let (_engine, snapshot, _dir) = load_test_table("table-without-dv-small")?;
        assert!(prepare_properties(
            snapshot.table_configuration(),
            Some(HashMap::from([("App.Key".into(), "value".into())])),
            Some(vec![key.into()]),
            1,
            42
        )
        .is_err());
        Ok(())
    }
    #[test]
    fn property_updates_reject_disabling_catalog_managed_ict() {
        let protocol = Protocol::try_new_modern(
            [TableFeature::CatalogManaged],
            [
                TableFeature::CatalogManaged,
                TableFeature::InCommitTimestamp,
            ],
        )
        .unwrap();
        let old = MockTableConfigurationBuilder::new()
            .with_protocol(protocol)
            .with_properties([(ENABLE_IN_COMMIT_TIMESTAMPS, "true")])
            .build();
        let error = prepare_properties(
            &old,
            Some(HashMap::from([(
                ENABLE_IN_COMMIT_TIMESTAMPS.into(),
                "false".into(),
            )])),
            None,
            1,
            42,
        )
        .unwrap_err();
        assert!(error.to_string().contains("catalog-managed"), "{error}");
    }
}

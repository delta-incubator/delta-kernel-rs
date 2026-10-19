//! Complete-schema and property updates through the public transaction builder.

use std::fs::File;
use std::time::{Duration, SystemTime};

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{schema_ref, ColumnMetadataKey, DataType, MapType, StructField};
use delta_kernel::table_features::TableFeature;
use delta_kernel::transaction::UpdateTableOperation;
use delta_kernel::{KernelResult, Snapshot};
use rstest::rstest;
use test_utils::{create_table_and_load_snapshot, test_table_setup, test_table_setup_mt};

#[tokio::test]
async fn table_property_updates_merge_remove_and_accept_noops() -> KernelResult<()> {
    let (_dir, path, engine) = test_table_setup()?;
    let original = create_table_and_load_snapshot(
        &path,
        schema_ref! { nullable "id": INTEGER },
        engine.as_ref(),
        &[("App.Keep", "old"), ("App.Remove", "old")],
    )?;
    let updated = original
        .update_table(UpdateTableOperation::AlterTable, "test", false)
        .with_table_properties_added([("discarded", "value")])
        .with_table_properties_added([("App.Keep", "new"), ("format-version", "3")])
        .with_table_properties_removed(["App.Keep"])
        .with_table_properties_removed(["App.Remove", "absent"])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let props = &updated.table_properties().unknown_properties;
    assert_eq!(props["App.Keep"], "new");
    assert_eq!(props["format-version"], "3");
    assert!(!props.contains_key("discarded"));
    assert!(!props.contains_key("App.Remove"));
    let noop = updated
        .update_table(UpdateTableOperation::AlterTable, "test", false)
        .with_table_properties_added([("App.Keep", "new")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    assert_eq!(noop.version(), 2);
    let fresh = Snapshot::builder_for(&path).build(engine.as_ref())?;
    assert_eq!(fresh.table_properties(), noop.table_properties());
    Ok(())
}

#[rstest]
#[tokio::test]
async fn property_only_update_accepts_map_key_and_value_structs(
    #[values("none", "name", "id")] mapping: &str,
) -> KernelResult<()> {
    let (_dir, path, engine) = test_table_setup()?;
    let child = delta_kernel::schema::schema! { nullable "id": INTEGER };
    let schema =
        schema_ref! { (StructField::nullable("m", MapType::new(child.clone(), child, true))) };
    let original = create_table_and_load_snapshot(
        &path,
        schema,
        engine.as_ref(),
        &[("delta.columnMapping.mode", mapping)],
    )?;
    let updated = original
        .clone()
        .update_table(UpdateTableOperation::AlterTable, "test", false)
        .with_table_properties_added([("app.key", "value")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let fresh = Snapshot::builder_for(&path).build(engine.as_ref())?;
    assert_eq!(fresh.schema(), original.schema());
    assert_eq!(fresh.table_properties(), updated.table_properties());
    assert_eq!(
        fresh.table_properties().unknown_properties["app.key"],
        "value"
    );
    Ok(())
}

#[rstest]
#[tokio::test]
async fn property_enablement_and_complete_schema_validate_together(
    #[values(false, true)] schema_first: bool,
) -> KernelResult<()> {
    let (_dir, path, engine) = test_table_setup()?;
    let original = create_table_and_load_snapshot(
        &path,
        schema_ref! { nullable "id": INTEGER },
        engine.as_ref(),
        &[("delta.columnMapping.mode", "name")],
    )?;
    let old_field = original.schema().field("id").unwrap().clone();
    let widened = StructField::nullable("renamed", DataType::LONG)
        .with_metadata(old_field.metadata().clone());
    let schema = schema_ref! { (widened), nullable "event_time": TIMESTAMP_NTZ };
    let props = [
        ("DELTA.enableTypeWidening", "TRUE"),
        ("delta.feature.timestampNTZ", "supported"),
    ];
    let builder = original.update_table(UpdateTableOperation::AlterTable, "test", false);
    let builder = if schema_first {
        builder
            .with_updated_schema(schema)
            .with_table_properties_added(props)
    } else {
        builder
            .with_table_properties_added(props)
            .with_updated_schema(schema)
    };
    let post = builder
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    assert_eq!(post.table_properties().enable_type_widening, Some(true));
    assert_eq!(
        post.schema().field("renamed").unwrap().column_mapping_id(),
        old_field.column_mapping_id()
    );
    assert!(post
        .schema()
        .field("renamed")
        .unwrap()
        .get_config_value(&ColumnMetadataKey::TypeChanges)
        .is_some());
    assert!(!post
        .table_properties()
        .unknown_properties
        .keys()
        .any(|key| key.starts_with("delta.feature.")));
    assert_eq!(
        Snapshot::builder_for(&path)
            .build(engine.as_ref())?
            .schema(),
        post.schema()
    );
    Ok(())
}

#[rstest]
#[tokio::test]
async fn property_ict_enable_reassert_disable_preserves_bookkeeping(
    #[values(false, true)] future_modification_time: bool,
) -> KernelResult<()> {
    let (_dir, path, engine) = test_table_setup()?;
    let original = create_table_and_load_snapshot(
        &path,
        schema_ref! { nullable "id": INTEGER },
        engine.as_ref(),
        &[],
    )?;
    let original = if future_modification_time {
        let commit = _dir.path().join("_delta_log/00000000000000000000.json");
        File::options()
            .write(true)
            .open(commit)?
            .set_modified(SystemTime::UNIX_EPOCH + Duration::from_millis(9_999_999_999_999))?;
        Snapshot::builder_for(&path).build(engine.as_ref())?
    } else {
        original
    };
    let old_timestamp = original.get_timestamp(engine.as_ref())?;
    let mut snapshot = original;
    let mut enabled_at = None;
    for (version, value) in [(1, "true"), (2, "true"), (3, "false")] {
        snapshot = snapshot
            .update_table(UpdateTableOperation::AlterTable, "test", false)
            .with_table_properties_added([("delta.enableInCommitTimestamps", value)])
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
            .commit(engine.as_ref())?
            .unwrap_post_commit_snapshot();
        let props = snapshot.table_properties();
        if value == "true" {
            assert_eq!(props.in_commit_timestamp_enablement_version, Some(1));
            if version == 1 {
                enabled_at = Some(snapshot.get_timestamp(engine.as_ref())?);
            }
            assert_eq!(props.in_commit_timestamp_enablement_timestamp, enabled_at);
            assert!(enabled_at.unwrap() > old_timestamp);
        } else {
            assert_eq!(props.in_commit_timestamp_enablement_version, None);
            assert_eq!(props.in_commit_timestamp_enablement_timestamp, None);
        }
        assert_eq!(
            Snapshot::builder_for(&path)
                .build(engine.as_ref())?
                .table_properties(),
            props
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn ict_enablement_reports_missing_previous_timestamp() -> KernelResult<()> {
    let (dir, path, engine) = test_table_setup_mt()?;
    let original = create_table_and_load_snapshot(
        &path,
        schema_ref! { nullable "id": INTEGER },
        engine.as_ref(),
        &[],
    )?;
    original.checkpoint(engine.as_ref(), None)?;
    std::fs::remove_file(dir.path().join("_delta_log/00000000000000000000.json"))?;
    let checkpoint_only = Snapshot::builder_for(&path).build(engine.as_ref())?;
    let result = checkpoint_only
        .update_table(UpdateTableOperation::AlterTable, "test", false)
        .with_table_properties_added([("delta.enableInCommitTimestamps", "true")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));
    test_utils::assert_result_error_with_message(
        result,
        "Cannot enable in-commit timestamps: previous commit timestamp unavailable",
    );
    assert!(!dir
        .path()
        .join("_delta_log/00000000000000000001.json")
        .exists());
    Ok(())
}

#[rstest]
#[case::deletion_vectors("delta.enableDeletionVectors", "true", TableFeature::DeletionVectors)]
#[case::checkpoint_v2("delta.checkpointPolicy", "v2", TableFeature::V2Checkpoint)]
#[case::vacuum_protocol(
    "delta.feature.vacuumProtocolCheck",
    "supported",
    TableFeature::VacuumProtocolCheck
)]
#[tokio::test]
async fn table_property_feature_requests_round_trip(
    #[case] key: &str,
    #[case] value: &str,
    #[case] feature: TableFeature,
) -> KernelResult<()> {
    let (_dir, path, engine) = test_table_setup()?;
    let original = create_table_and_load_snapshot(
        &path,
        schema_ref! { nullable "id": INTEGER },
        engine.as_ref(),
        &[],
    )?;
    let post = original
        .update_table(UpdateTableOperation::AlterTable, "test", false)
        .with_table_properties_added([(key, value)])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    assert_eq!(post.version(), 1);
    let fresh = Snapshot::builder_for(&path).build(engine.as_ref())?;
    assert_eq!(fresh.table_properties(), post.table_properties());
    assert!(fresh.table_configuration().is_feature_supported(&feature));
    assert_eq!(
        fresh.table_configuration().protocol(),
        post.table_configuration().protocol()
    );
    Ok(())
}

#[tokio::test]
async fn property_enabling_iceberg_v2_allocates_nested_ids_without_changing_fields(
) -> KernelResult<()> {
    let (_dir, path, engine) = test_table_setup()?;
    let original = create_table_and_load_snapshot(
        &path,
        schema_ref! { nullable "values": (delta_kernel::schema::ArrayType::new(DataType::INTEGER, true)) },
        engine.as_ref(),
        &[("delta.columnMapping.mode", "name")],
    )?;
    let old_field = original.schema().field("values").unwrap().clone();
    let old_max = original
        .table_properties()
        .column_mapping_max_column_id
        .unwrap();
    let post = original
        .update_table(UpdateTableOperation::AlterTable, "test", false)
        .with_table_properties_added([("delta.enableIcebergCompatV2", "true")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let new_schema = post.schema();
    let new_field = new_schema.field("values").unwrap();
    assert_eq!(new_field.column_mapping_id(), old_field.column_mapping_id());
    assert_eq!(
        new_field.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName),
        old_field.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
    );
    assert!(new_field
        .get_config_value(&ColumnMetadataKey::ColumnMappingNestedIds)
        .is_some());
    assert_eq!(
        post.table_properties().column_mapping_max_column_id,
        Some(old_max + 1)
    );
    let repeated = post
        .clone()
        .update_table(UpdateTableOperation::AlterTable, "test", false)
        .with_table_properties_added([("delta.enableIcebergCompatV2", "true")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    assert_eq!(post.schema(), repeated.schema());
    let invalid = repeated
        .update_table(UpdateTableOperation::AlterTable, "test", false)
        .with_table_properties_added([("delta.enableDeletionVectors", "true")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));
    assert!(invalid.is_err());
    assert_eq!(
        Snapshot::builder_for(&path)
            .build(engine.as_ref())?
            .version(),
        2
    );
    Ok(())
}

#[rstest]
#[case::row_tracking("delta.enableRowTracking")]
#[case::compat_v3("delta.enableIcebergCompatV3")]
#[tokio::test]
async fn property_reassertion_preserves_materialized_names_and_rejects_disable(
    #[case] key: &str,
) -> KernelResult<()> {
    let (_dir, path, engine) = test_table_setup()?;
    let original = create_table_and_load_snapshot(
        &path,
        schema_ref! { nullable "id": INTEGER },
        engine.as_ref(),
        &[(key, "true")],
    )?;
    let props = original.table_properties().clone();
    let post = original
        .update_table(UpdateTableOperation::AlterTable, "test", false)
        .with_table_properties_added([(key, "true")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    assert_eq!(
        post.table_properties().materialized_row_id_column_name,
        props.materialized_row_id_column_name
    );
    assert_eq!(
        post.table_properties()
            .materialized_row_commit_version_column_name,
        props.materialized_row_commit_version_column_name
    );
    assert!(post
        .update_table(UpdateTableOperation::AlterTable, "test", false)
        .with_table_properties_added([(key, "false")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
        .is_err());
    Ok(())
}

#[tokio::test]
async fn metadata_conflicts_require_preparation_from_a_fresh_snapshot() -> KernelResult<()> {
    let (_dir, path, engine) = test_table_setup()?;
    let original = create_table_and_load_snapshot(
        &path,
        schema_ref! { nullable "id": INTEGER },
        engine.as_ref(),
        &[],
    )?;
    let first = original
        .clone()
        .update_table(UpdateTableOperation::AlterTable, "test", false)
        .with_table_properties_added([("winner", "yes")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    let stale = original
        .update_table(UpdateTableOperation::AlterTable, "test", false)
        .with_table_properties_added([("other", "yes")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    first.commit(engine.as_ref())?.unwrap_committed();
    assert!(matches!(
        stale.commit(engine.as_ref())?,
        delta_kernel::transaction::CommitResult::Conflicted(_)
    ));
    let fresh = Snapshot::builder_for(&path).build(engine.as_ref())?;
    assert_eq!(fresh.version(), 1);
    assert!(!fresh
        .table_properties()
        .unknown_properties
        .contains_key("other"));
    let post = fresh
        .update_table(UpdateTableOperation::AlterTable, "test", false)
        .with_table_properties_added([("other", "yes")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    assert_eq!(post.table_properties().unknown_properties["winner"], "yes");
    assert_eq!(post.table_properties().unknown_properties["other"], "yes");
    Ok(())
}

#[rstest]
#[case::allowed(DataType::LONG, true)]
#[case::forbidden(DataType::DOUBLE, false)]
#[tokio::test]
async fn widening_preserves_iceberg_protocol_restrictions(
    #[case] data_type: DataType,
    #[case] accepted: bool,
    #[values(false, true)] schema_first: bool,
    #[values("true", "false")] iceberg_enabled: &str,
    #[values("true", "false")] initially_enabled: &str,
) -> KernelResult<()> {
    let (_dir, path, engine) = test_table_setup()?;
    let original = create_table_and_load_snapshot(
        &path,
        schema_ref! { nullable "id": INTEGER },
        engine.as_ref(),
        &[
            ("delta.columnMapping.mode", "name"),
            ("delta.enableIcebergCompatV2", initially_enabled),
            ("delta.feature.icebergCompatV2", "supported"),
        ],
    )?;
    let id = original.schema().field("id").unwrap().clone();
    let schema = schema_ref! { (StructField::nullable("id", data_type.clone()).with_metadata(id.metadata().clone())) };
    let props = [
        ("delta.enableIcebergCompatV2", iceberg_enabled),
        ("delta.enableTypeWidening", "true"),
    ];
    let builder = original
        .clone()
        .update_table(UpdateTableOperation::AlterTable, "test", false);
    let builder = if schema_first {
        builder
            .with_updated_schema(schema)
            .with_table_properties_added(props)
    } else {
        builder
            .with_table_properties_added(props)
            .with_updated_schema(schema)
    };
    let result = builder.build(engine.as_ref(), Box::new(FileSystemCommitter::new()));
    if accepted {
        let updated = result?
            .commit(engine.as_ref())?
            .unwrap_post_commit_snapshot();
        assert_eq!(
            updated.schema().field("id").unwrap().data_type(),
            &data_type
        );
        assert_eq!(
            updated.table_properties().enable_iceberg_compat_v2,
            Some(iceberg_enabled == "true")
        );
    } else {
        test_utils::assert_result_error_with_message(result, "Iceberg compatibility forbids");
        let fresh = Snapshot::builder_for(&path).build(engine.as_ref())?;
        assert_eq!(fresh.version(), original.version());
        assert_eq!(fresh.schema(), original.schema());
    }
    Ok(())
}

#[rstest]
#[tokio::test]
async fn schema_rename_respects_explicit_statistics_selection(
    #[values(false, true)] schema_first: bool,
    #[values("new", "old")] selection: &str,
    #[values("delta.dataSkippingStatsColumns", "DELTA.DATASKIPPINGSTATSCOLUMNS")] key: &str,
) -> KernelResult<()> {
    let (_dir, path, engine) = test_table_setup()?;
    let original = create_table_and_load_snapshot(
        &path,
        schema_ref! { nullable "old": INTEGER },
        engine.as_ref(),
        &[
            ("delta.columnMapping.mode", "name"),
            ("delta.dataSkippingStatsColumns", "old"),
        ],
    )?;
    let renamed = StructField::nullable("renamed", DataType::INTEGER)
        .with_metadata(original.schema().field("old").unwrap().metadata().clone());
    let schema = schema_ref! { (renamed), nullable "new": INTEGER, nullable "old": INTEGER };
    let properties = [(key, selection)];
    let builder = original.update_table(UpdateTableOperation::AlterTable, "test", false);
    let builder = if schema_first {
        builder
            .with_updated_schema(schema)
            .with_table_properties_added(properties)
    } else {
        builder
            .with_table_properties_added(properties)
            .with_updated_schema(schema)
    };
    let updated = builder
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    assert_eq!(
        updated.table_properties().data_skipping_stats_columns,
        Some(vec![delta_kernel::expressions::ColumnName::new([
            selection
        ])])
    );
    Ok(())
}

//! Schema, property, and clustering updates through the public transaction builder.

use std::fs::File;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use delta_kernel::arrow::array::{new_null_array, Array, Int32Array, StringArray, StructArray};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::{column_name, ColumnName};
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::schema::{
    schema_ref, ColumnMetadataKey, DataType, MapType, StructField, StructType,
};
use delta_kernel::table_features::TableFeature;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::transaction::UpdateTableOperation;
use delta_kernel::{KernelResult, Snapshot};
use rstest::rstest;
use test_utils::{
    add_commit, create_table_and_load_snapshot, read_actions_from_commit, read_add_infos,
    read_scan, test_table_setup, test_table_setup_mt, write_batch_to_table,
};

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
#[case::retain(None)]
#[case::clear(Some(vec![]))]
#[case::replace(Some(vec![column_name!("id")]))]
#[tokio::test]
async fn explicit_clustering_request_repairs_malformed_domain(
    #[case] columns: Option<Vec<ColumnName>>,
) -> KernelResult<()> {
    let (_dir, path, engine) = test_table_setup()?;
    let original = create_table(&path, schema_ref! { nullable "id": INTEGER }, "test", false)
        .with_data_layout(DataLayout::Clustered {
            columns: vec![column_name!("id")],
        })
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let malformed = serde_json::json!({"domainMetadata": {
        "domain": "delta.clustering", "configuration": "{", "removed": false
    }});
    add_commit(
        original.table_root().as_str(),
        &LocalFileSystem::new(),
        1,
        malformed.to_string(),
    )
    .await?;
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    assert!(snapshot
        .get_physical_clustering_columns(engine.as_ref())
        .is_err());
    let builder = snapshot
        .update_table(UpdateTableOperation::AlterTable, "test", false)
        .with_table_properties_added([("app.key", "value")]);
    let builder = match &columns {
        Some(columns) => builder.with_clustering_columns(columns.clone()),
        None => builder,
    };
    let result = builder.build(engine.as_ref(), Box::new(FileSystemCommitter::new()));
    if let Some(columns) = columns {
        let updated = result?
            .commit(engine.as_ref())?
            .unwrap_post_commit_snapshot();
        let fresh = Snapshot::builder_for(&path).build(engine.as_ref())?;
        assert_eq!(updated.version(), 2);
        assert_eq!(
            updated.get_physical_clustering_columns(engine.as_ref())?,
            Some(columns.clone())
        );
        assert_eq!(
            fresh.get_physical_clustering_columns(engine.as_ref())?,
            Some(columns)
        );
    } else {
        assert!(result.is_err());
        assert_eq!(
            Snapshot::builder_for(&path)
                .build(engine.as_ref())?
                .version(),
            1
        );
    }
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn clustering_enable_replace_clear_and_retain_round_trip(
    #[values("none", "name", "id")] mapping: &str,
    #[values("classic", "v2")] checkpoint: &str,
) -> KernelResult<()> {
    let (_dir, path, engine) = test_table_setup_mt()?;
    let original = create_table_and_load_snapshot(
        &path,
        schema_ref! { nullable "a": INTEGER, nullable "b": STRING },
        engine.as_ref(),
        &[
            ("delta.columnMapping.mode", mapping),
            ("delta.checkpointPolicy", checkpoint),
        ],
    )?;
    let batch = RecordBatch::try_new(
        Arc::new(original.schema().as_ref().try_into_arrow()?),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["value"])),
        ],
    )?;
    let original =
        write_batch_to_table(&original, engine.as_ref(), batch, Default::default()).await?;
    original.write_checksum(engine.as_ref())?;
    let original = Snapshot::builder_for(&path).build(engine.as_ref())?;
    let original_file_stats = original.get_file_stats_if_present().unwrap();
    let mut snapshot = original.clone();
    for columns in [
        vec![],
        vec![column_name!("B"), column_name!("A")],
        vec![column_name!("a")],
        vec![],
    ] {
        let post = snapshot
            .clone()
            .update_table(UpdateTableOperation::AlterTable, "test", false)
            .with_clustering_columns(vec![column_name!("discarded")])
            .with_clustering_columns(columns.clone())
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
            .commit(engine.as_ref())?
            .unwrap_post_commit_snapshot();
        assert_eq!(
            post.get_file_stats_if_present(),
            Some(original_file_stats.clone())
        );
        let infos = post.get_clustering_column_infos(engine.as_ref())?.unwrap();
        let logical: Vec<_> = infos
            .iter()
            .map(|info| info.logical_column.to_string())
            .collect();
        assert_eq!(
            logical,
            columns
                .iter()
                .map(|column| column.to_string().to_lowercase())
                .collect::<Vec<_>>()
        );
        assert_eq!(post.schema(), original.schema());
        assert!(
            read_actions_from_commit(post.table_root(), post.version(), "metaData")?.is_empty()
        );
        assert_eq!(
            read_actions_from_commit(post.table_root(), post.version(), "domainMetadata")?.len(),
            1
        );
        let retained = post
            .clone()
            .update_table(UpdateTableOperation::AlterTable, "test", false)
            .with_table_properties_removed(["absent"])
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
            .commit(engine.as_ref())?
            .unwrap_post_commit_snapshot();
        assert_eq!(
            retained.get_physical_clustering_columns(engine.as_ref())?,
            post.get_physical_clustering_columns(engine.as_ref())?
        );
        retained.clone().checkpoint(engine.as_ref(), None)?;
        retained.write_checksum(engine.as_ref())?;
        snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
        assert_eq!(
            snapshot.get_clustering_column_infos(engine.as_ref())?,
            Some(infos)
        );
    }
    assert!(snapshot
        .table_configuration()
        .is_feature_supported(&TableFeature::ClusteredTable));
    assert!(snapshot
        .table_configuration()
        .is_feature_supported(&TableFeature::DomainMetadata));
    assert!(Snapshot::builder_for(&path)
        .at_version(0)
        .build(engine.as_ref())?
        .get_physical_clustering_columns(engine.as_ref())?
        .is_none());
    Ok(())
}

#[rstest]
#[case::retain(None, false)]
#[case::replace(Some(vec![column_name!("a")]), true)]
#[case::clear(Some(vec![]), true)]
#[tokio::test]
async fn schema_drop_validates_final_clustering_selection(
    #[case] columns: Option<Vec<ColumnName>>,
    #[case] accepted: bool,
    #[values(false, true)] schema_first: bool,
) -> KernelResult<()> {
    let (_dir, path, engine) = test_table_setup()?;
    let original = create_table(
        &path,
        schema_ref! { nullable "a": INTEGER, nullable "b": INTEGER, nullable "c": INTEGER },
        "test",
        false,
    )
    .with_table_properties([("delta.columnMapping.mode", "name")])
    .with_data_layout(DataLayout::Clustered {
        columns: vec![column_name!("a"), column_name!("b")],
    })
    .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
    .commit(engine.as_ref())?
    .unwrap_post_commit_snapshot();
    let schema = StructType::try_new(
        original
            .schema()
            .fields()
            .filter(|field| field.name() != "b")
            .cloned(),
    )?
    .into();
    let mut builder =
        original
            .clone()
            .update_table(UpdateTableOperation::AlterTable, "test", false);
    if schema_first {
        builder = builder.with_updated_schema(schema);
    }
    if let Some(columns) = columns {
        builder = builder.with_clustering_columns(columns);
    }
    if !schema_first {
        builder = builder.with_updated_schema(
            StructType::try_new(
                original
                    .schema()
                    .fields()
                    .filter(|field| field.name() != "b")
                    .cloned(),
            )?
            .into(),
        );
    }
    let result = builder.build(engine.as_ref(), Box::new(FileSystemCommitter::new()));
    if accepted {
        let post = result?
            .commit(engine.as_ref())?
            .unwrap_post_commit_snapshot();
        assert!(!post.schema().contains("b"));
        assert_eq!(
            Snapshot::builder_for(&path)
                .build(engine.as_ref())?
                .schema(),
            post.schema()
        );
    } else {
        assert!(result.is_err());
        assert_eq!(
            Snapshot::builder_for(&path)
                .build(engine.as_ref())?
                .version(),
            0
        );
    }
    Ok(())
}

#[rstest]
#[case::duplicate(vec![column_name!("a"), column_name!("A")], false)]
#[case::missing(vec![column_name!("missing")], false)]
#[case::unsupported(vec![column_name!("flag")], false)]
#[case::empty_path(vec![ColumnName::new(Vec::<String>::new())], false)]
#[case::partitioned(vec![column_name!("a")], true)]
#[case::partitioned_clear(vec![], true)]
#[tokio::test]
async fn invalid_clustering_request_publishes_nothing(
    #[case] columns: Vec<ColumnName>,
    #[case] partitioned: bool,
) -> KernelResult<()> {
    let (_dir, path, engine) = test_table_setup()?;
    let mut builder = create_table(
        &path,
        schema_ref! { nullable "a": INTEGER, nullable "flag": BOOLEAN },
        "test",
        false,
    );
    if partitioned {
        builder = builder.with_data_layout(DataLayout::Partitioned {
            columns: vec![column_name!("a")],
        });
    }
    let snapshot = builder
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let result = snapshot
        .update_table(UpdateTableOperation::AlterTable, "test", false)
        .with_clustering_columns(columns)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));
    assert!(result.is_err());
    assert_eq!(
        Snapshot::builder_for(&path)
            .build(engine.as_ref())?
            .version(),
        0
    );
    Ok(())
}

#[tokio::test]
async fn nested_clustering_preserves_physical_identity_after_rename() -> KernelResult<()> {
    let (_dir, path, engine) = test_table_setup()?;
    let original = create_table_and_load_snapshot(
        &path,
        schema_ref! { (StructField::nullable("parent.with.dot", StructType::try_new([StructField::nullable("child", DataType::INTEGER)])?)) },
        engine.as_ref(),
        &[("delta.columnMapping.mode", "name")],
    )?;
    let clustered = original
        .update_table(UpdateTableOperation::AlterTable, "test", false)
        .with_clustering_columns(vec![ColumnName::new(["PARENT.WITH.DOT", "CHILD"])])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let parent = clustered.schema().field("parent.with.dot").unwrap().clone();
    let renamed = StructField::nullable("renamed", parent.data_type().clone())
        .with_metadata(parent.metadata().clone());
    let post = clustered
        .clone()
        .update_table(UpdateTableOperation::AlterTable, "test", false)
        .with_updated_schema(schema_ref! { (renamed) })
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    assert_eq!(
        post.get_physical_clustering_columns(engine.as_ref())?,
        clustered.get_physical_clustering_columns(engine.as_ref())?
    );
    assert_eq!(
        post.get_clustering_column_infos(engine.as_ref())?.unwrap()[0].logical_column,
        column_name!("renamed.child")
    );
    Ok(())
}

#[rstest]
#[tokio::test]
async fn schema_properties_and_clustering_commit_with_data_and_required_statistics(
    #[values(false, true)] missing_statistics: bool,
) -> KernelResult<()> {
    let (_dir, path, engine) = test_table_setup()?;
    let original = create_table_and_load_snapshot(
        &path,
        schema_ref! { nullable "a": INTEGER },
        engine.as_ref(),
        &[("delta.columnMapping.mode", "name")],
    )?;
    let schema =
        schema_ref! { (original.schema().field("a").unwrap().clone()), nullable "b": INTEGER };
    let mut transaction = original
        .update_table(UpdateTableOperation::Write, "test", true)
        .with_clustering_columns(vec![column_name!("b")])
        .with_table_properties_added([
            ("format-version", "3"),
            ("delta.dataSkippingNumIndexedCols", "0"),
        ])
        .with_updated_schema(schema)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    let context = transaction.write_state()?.write_context_builder().build()?;
    let batch = RecordBatch::try_new(
        Arc::new(context.logical_data_schema().as_ref().try_into_arrow()?),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![3, 4])),
        ],
    )?;
    let metadata = engine
        .write_parquet(&ArrowEngineData::new(batch), &context)
        .await?;
    if missing_statistics {
        let batch = test_utils::into_record_batch(metadata);
        let mut columns = batch.columns().to_vec();
        let stats_index = batch.schema().index_of("stats")?;
        let stats = columns[stats_index]
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let mut stats_columns = stats.columns().to_vec();
        let null_count = stats
            .fields()
            .iter()
            .position(|field| field.name() == "nullCount")
            .unwrap();
        stats_columns[null_count] =
            new_null_array(stats_columns[null_count].data_type(), batch.num_rows());
        columns[stats_index] = Arc::new(StructArray::new(
            stats.fields().clone(),
            stats_columns,
            stats.nulls().cloned(),
        ));
        transaction.add_files(Box::new(ArrowEngineData::new(RecordBatch::try_new(
            batch.schema(),
            columns,
        )?)));
        test_utils::assert_result_error_with_message(
            transaction.commit(engine.as_ref()),
            "Required column",
        );
        assert_eq!(
            Snapshot::builder_for(&path)
                .build(engine.as_ref())?
                .version(),
            0
        );
        return Ok(());
    }
    transaction.add_files(metadata);
    let post = transaction
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let physical = post
        .get_physical_clustering_columns(engine.as_ref())?
        .unwrap();
    let adds = read_add_infos(&post, engine.as_ref())?;
    assert_eq!(adds.len(), 1);
    assert_eq!(
        adds[0].stats.as_ref().unwrap()["minValues"][physical[0].path()[0].as_str()],
        3
    );
    assert_eq!(
        post.table_properties().unknown_properties["format-version"],
        "3"
    );
    let fresh = Snapshot::builder_for(&path).build(engine.as_ref())?;
    let batches = read_scan(&fresh.scan_builder().build()?, engine.clone())?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
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

//! Characterizes schema intent independently of the staged data-file actions.

use std::sync::Arc;

use delta_kernel::arrow::array::new_null_array;
use delta_kernel::arrow::datatypes::Schema as ArrowSchema;
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::column_name;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::schema::{schema_ref, DataType, StructField};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::TableFeature;
use delta_kernel::transaction::{SchemaOperation, UpdateTableOperation};
use delta_kernel::try_parse_uri;
use rstest::rstest;
use test_utils::{
    assert_result_error_with_message, create_table as create_test_table,
    create_table_and_load_snapshot, read_actions_from_commit, read_add_infos, test_table_setup_mt,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SchemaInput {
    Omitted,
    Empty,
    AddColumn,
    AlreadyNullable,
}

impl SchemaInput {
    fn changes(self) -> Vec<SchemaOperation> {
        match self {
            Self::Omitted | Self::Empty => vec![],
            Self::AddColumn => vec![SchemaOperation::AddColumn {
                parent: None,
                field: StructField::nullable("added", DataType::STRING),
            }],
            Self::AlreadyNullable => vec![SchemaOperation::SetNullable {
                column: column_name!("id"),
            }],
        }
    }

    fn is_nonempty(self) -> bool {
        matches!(self, Self::AddColumn | Self::AlreadyNullable)
    }
}

#[rstest]
#[case::plain(None, false, true, true)]
#[case::v2_enabled(Some(TableFeature::IcebergCompatV2), true, true, false)]
#[case::v2_disabled(Some(TableFeature::IcebergCompatV2), false, true, true)]
#[case::v3_enabled(Some(TableFeature::IcebergCompatV3), true, false, false)]
#[case::v3_disabled(Some(TableFeature::IcebergCompatV3), false, true, true)]
#[case::column_defaults(Some(TableFeature::AllowColumnDefaults), true, false, false)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iceberg_schema_evolution_operation_input_matrix(
    #[case] feature: Option<TableFeature>,
    #[case] enabled: bool,
    #[case] non_alter_changes_allowed: bool,
    #[case] alter_changes_allowed: bool,
    #[values(
        UpdateTableOperation::Write,
        UpdateTableOperation::StreamingUpdate,
        UpdateTableOperation::AlterTable,
        UpdateTableOperation::Delete,
        UpdateTableOperation::Update,
        UpdateTableOperation::Merge,
        UpdateTableOperation::Optimize,
        UpdateTableOperation::Custom("UNKNOWN".to_string())
    )]
    operation: UpdateTableOperation,
    #[values(
        SchemaInput::Omitted,
        SchemaInput::Empty,
        SchemaInput::AddColumn,
        SchemaInput::AlreadyNullable
    )]
    input: SchemaInput,
    #[values(false, true)] stage_data: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let schema = schema_ref! { nullable "id": INTEGER };
    let enablement = if enabled { "true" } else { "false" };
    let properties = match feature {
        Some(TableFeature::IcebergCompatV2) => vec![
            ("delta.feature.icebergCompatV2", "supported"),
            ("delta.enableIcebergCompatV2", enablement),
            ("delta.columnMapping.mode", "name"),
        ],
        Some(TableFeature::IcebergCompatV3) => vec![
            ("delta.feature.icebergCompatV3", "supported"),
            ("delta.enableIcebergCompatV3", enablement),
            ("delta.columnMapping.mode", "name"),
            ("delta.enableRowTracking", "true"),
        ],
        _ => vec![],
    };
    let snapshot = if feature == Some(TableFeature::AllowColumnDefaults) {
        create_test_table(
            Arc::new(LocalFileSystem::new()),
            try_parse_uri(&table_path)?,
            schema,
            &[],
            true,
            vec![],
            vec!["allowColumnDefaults"],
        )
        .await?;
        Snapshot::builder_for(&table_path).build(engine.as_ref())?
    } else {
        create_table_and_load_snapshot(&table_path, schema, engine.as_ref(), &properties)?
    };
    if let Some(feature) = feature {
        assert!(snapshot
            .table_configuration()
            .is_feature_supported(&feature));
        assert_eq!(
            snapshot.table_configuration().is_feature_enabled(&feature),
            enabled
        );
    }
    let is_alter = operation == UpdateTableOperation::AlterTable;
    let stage_data = stage_data && !is_alter;
    let builder = snapshot
        .clone()
        .transaction_builder()
        .with_operation(operation.clone());
    let result = if input == SchemaInput::Omitted {
        Ok(builder)
    } else {
        builder.with_schema_changes(input.changes())
    }
    .and_then(|builder| builder.build(engine.as_ref(), Box::new(FileSystemCommitter::new())));
    let expected_success = match input {
        SchemaInput::Omitted => !is_alter,
        SchemaInput::Empty => false,
        SchemaInput::AddColumn | SchemaInput::AlreadyNullable if is_alter => alter_changes_allowed,
        SchemaInput::AddColumn | SchemaInput::AlreadyNullable => non_alter_changes_allowed,
    };
    assert_eq!(result.is_ok(), expected_success, "{result:?}");
    if !expected_success {
        let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
        assert_eq!(reloaded.version(), 0);
        assert_eq!(reloaded.schema(), snapshot.schema());
        return Ok(());
    }

    let mut transaction = result?;
    if stage_data {
        let context = transaction.write_state()?.write_context_builder().build()?;
        assert_eq!(
            context.logical_data_schema().contains("added"),
            input == SchemaInput::AddColumn
        );
        let arrow_schema: Arc<ArrowSchema> =
            Arc::new(context.logical_data_schema().as_ref().try_into_arrow()?);
        let columns = arrow_schema
            .fields()
            .iter()
            .map(|field| new_null_array(field.data_type(), 1))
            .collect();
        let batch = RecordBatch::try_new(arrow_schema, columns)?;
        transaction.add_files(
            engine
                .write_parquet(&ArrowEngineData::new(batch), &context)
                .await?,
        );
    }
    let post = transaction
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert_eq!(reloaded.version(), 1);
    assert_eq!(reloaded.schema(), post.schema());
    assert_eq!(
        reloaded.schema().contains("added"),
        input == SchemaInput::AddColumn
    );
    assert_eq!(
        read_add_infos(&reloaded, engine.as_ref())?.len(),
        usize::from(stage_data)
    );
    assert_eq!(
        read_actions_from_commit(post.table_root(), 1, "metaData")?.len(),
        usize::from(input.is_nonempty())
    );
    let commit_info = read_actions_from_commit(post.table_root(), 1, "commitInfo")?;
    assert_eq!(commit_info[0]["operation"], operation.as_str());
    if input != SchemaInput::AddColumn {
        assert_eq!(reloaded.schema(), snapshot.schema());
    }
    Ok(())
}

#[rstest]
#[case::v2("delta.enableIcebergCompatV2")]
#[case::v3("delta.enableIcebergCompatV3")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iceberg_schema_evolution_rejects_empty_calls_even_with_queued_changes(
    #[case] enablement_property: &str,
    #[values(true, false)] enabled: bool,
    #[values(true, false)] individual_setter: bool,
    #[values(SchemaInput::AddColumn, SchemaInput::AlreadyNullable)] input: SchemaInput,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let snapshot = create_table_and_load_snapshot(
        &table_path,
        schema_ref! { nullable "id": INTEGER },
        engine.as_ref(),
        &[(enablement_property, if enabled { "true" } else { "false" })],
    )?;
    let builder = snapshot
        .transaction_builder()
        .with_operation(UpdateTableOperation::Write);
    let builder = match (individual_setter, input) {
        (true, SchemaInput::AddColumn) => {
            builder.add_column(StructField::nullable("added", DataType::STRING))
        }
        (true, SchemaInput::AlreadyNullable) => builder.set_nullable(column_name!("id")),
        _ => builder.with_schema_changes(input.changes())?,
    };
    assert_result_error_with_message(
        builder.with_schema_changes([]),
        "with_schema_changes requires at least one schema operation",
    );
    let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert_eq!(reloaded.version(), 0);
    assert!(!reloaded.schema().contains("added"));
    Ok(())
}

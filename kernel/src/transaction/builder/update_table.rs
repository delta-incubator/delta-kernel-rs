//! Builder for transactions against an existing table.

use delta_kernel_derive::internal_api;

use super::state::TransactionBuilderState;
use super::TransactionBuilder;
use crate::committer::Committer;
use crate::expressions::ColumnName;
use crate::schema::StructField;
use crate::snapshot::SnapshotRef;
use crate::table_configuration::TableConfiguration;
use crate::table_features::{
    validate_iceberg_compat_if_needed, IcebergCompatValidationContext, Operation, TableFeature,
    V2_VALIDATOR, V3_VALIDATOR,
};
use crate::transaction::domain_metadata::validate_unique_domains;
use crate::transaction::schema_evolution::{evolve_table_config, SchemaOperation};
use crate::transaction::{ExecutionMode, Transaction, UpdateTable, UpdateTableOperation};
use crate::utils::{current_time_ms, require};
use crate::{Engine, KernelError, KernelResult, Result};

/// Configures DML and schema-changing DDL transactions against an existing table.
///
/// This builder does not create or replace tables. The operation and engine info are required by
/// [`Snapshot::update_table`](crate::Snapshot::update_table) and cannot be changed on the builder.
/// [`build`](UpdateTableTransactionBuilder::build) validates the accumulated intent and constructs
/// the transaction.
///
/// cbindgen:ignore
pub type UpdateTableTransactionBuilder = TransactionBuilder<UpdateTable>;

/// Opaque state collected by update-table-specific builder setters.
#[doc(hidden)]
pub struct UpdateTableBuilderState {
    snapshot: SnapshotRef,
    operation: UpdateTableOperation,
    schema_changes: Vec<SchemaOperation>,
    domain_metadata_removals: Vec<String>,
    is_blind_append: bool,
}

impl std::fmt::Debug for TransactionBuilder<UpdateTable> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpdateTableTransactionBuilder")
            .field("snapshot_version", &self.local_state.snapshot.version())
            .field("operation", &self.local_state.operation)
            .field(
                "schema_change_count",
                &self.local_state.schema_changes.len(),
            )
            .finish()
    }
}

impl TransactionBuilder<UpdateTable> {
    pub(crate) fn new(
        snapshot: SnapshotRef,
        operation: UpdateTableOperation,
        engine_info: String,
    ) -> Self {
        Self {
            common_state: TransactionBuilderState::for_update_table(engine_info),
            local_state: UpdateTableBuilderState {
                snapshot,
                operation,
                schema_changes: Vec::new(),
                domain_metadata_removals: Vec::new(),
                is_blind_append: false,
            },
        }
    }

    /// Marks the transaction as a blind append assertion.
    ///
    /// Blind appends add new files without depending on existing table state. Commit validation
    /// requires staged Add metadata and rejects staged Remove metadata or deletion-vector batches.
    /// Blind append is also invalid for `ALTER TABLE`, schema changes, or `dataChange = false`.
    pub fn with_blind_append(mut self) -> Self {
        self.local_state.is_blind_append = true;
        self
    }

    /// Adds schema changes to validate and apply during [`build`](Self::build).
    ///
    /// Appends `changes` in call order and returns the updated builder. Each change observes the
    /// result of prior changes. Empty input is rejected immediately because appending it would
    /// erase the distinction between an explicit empty request and no request, including when
    /// other schema changes are already queued.
    ///
    /// # Errors
    ///
    /// Returns an error if `changes` is empty.
    #[internal_api]
    pub(crate) fn with_schema_changes(
        mut self,
        changes: impl IntoIterator<Item = SchemaOperation>,
    ) -> Result<Self> {
        let mut changes = changes.into_iter().peekable();
        require!(
            changes.peek().is_some(),
            KernelError::generic("with_schema_changes requires at least one schema operation")
        );
        self.local_state.schema_changes.extend(changes);
        Ok(self)
    }

    /// Adds a nullable top-level column to the table schema.
    ///
    /// The field must not already exist in the schema, using a case-insensitive comparison, and
    /// must be nullable because existing data files do not contain it. On column-mapping tables,
    /// Kernel assigns or preserves column-mapping IDs and physical names.
    ///
    /// Schema changes are applied in call order and validated by [`build`](Self::build).
    pub fn add_column(mut self, field: StructField) -> Self {
        self.local_state
            .schema_changes
            .push(SchemaOperation::add_column(None, field));
        self
    }

    /// Adds a nullable field under `parent` in the table schema.
    ///
    /// An empty `parent` targets the root schema. Path segments may traverse nested structs, array
    /// elements, map keys, and map values, but the resolved parent must be a struct. The field must
    /// be nullable, must not be a metadata column, and must not collide case-insensitively with a
    /// sibling. On column-mapping tables, Kernel assigns or preserves column-mapping IDs and
    /// physical names.
    ///
    /// Schema changes are applied in call order and validated by [`build`](Self::build).
    pub fn add_column_at(mut self, parent: ColumnName, field: StructField) -> Self {
        self.local_state
            .schema_changes
            .push(SchemaOperation::add_column(parent, field));
        self
    }

    /// Changes a possibly nested column from non-nullable to nullable.
    ///
    /// The column path is resolved after all preceding schema changes. An already-nullable column
    /// is unchanged, but the transaction still emits its metadata action.
    pub fn set_nullable(mut self, column: ColumnName) -> Self {
        self.local_state
            .schema_changes
            .push(SchemaOperation::SetNullable { column });
        self
    }

    /// Adds a user-controlled domain metadata removal to the transaction.
    ///
    /// If the domain exists, commit emits a tombstone that preserves its previous configuration.
    /// Removing a domain that does not exist is a no-op.
    ///
    /// Each domain may occur only once across additions and removals. Conflicts are rejected by
    /// [`build`](Self::build).
    pub fn with_domain_metadata_removed(mut self, domain: impl Into<String>) -> Self {
        self.local_state
            .domain_metadata_removals
            .push(domain.into());
        self
    }

    /// Validates the accumulated intent and builds a transaction.
    ///
    /// If [`with_data_change`](Self::with_data_change) is not set, updates without schema changes
    /// default to `true`. Updates with schema changes infer the value at commit: `false` without
    /// staged Add, Remove, or DV-update batches, and `true` otherwise, including empty or
    /// unselected batches.
    ///
    /// # Parameters
    ///
    /// - `engine`: Provides table-state reads needed during validation.
    /// - `committer`: Executes the eventual atomic commit.
    ///
    /// For clustered tables, building performs log replay to load clustering columns from domain
    /// metadata and may therefore incur additional I/O.
    ///
    /// # Errors
    ///
    /// Returns an error for:
    /// - Invalid operation intent, including incompatible blind-append or schema intent.
    /// - Unsupported table writes, schema evolution, or CDF validation failures.
    /// - Conflicting application ids or domain metadata, or reserved or unsupported domains.
    /// - Connector commit information that does not contain exactly one row.
    ///
    /// Schema evolution is rejected when `icebergCompatV3` or `allowColumnDefaults` is enabled.
    /// With `icebergCompatV2` enabled, only [`UpdateTableOperation::AlterTable`] schema evolution
    /// is rejected.
    pub fn build(self, engine: &dyn Engine, committer: Box<dyn Committer>) -> Result<Transaction> {
        self.validate()?;
        let transaction = try_new_update_table(self, committer, engine)?;
        transaction.validate_domain_metadata_operations()?;
        Ok(transaction)
    }

    fn validate(&self) -> Result<()> {
        let operation = &self.local_state.operation;
        operation
            .validate()
            .map_err(KernelError::invalid_transaction_state)?;
        if operation == &UpdateTableOperation::AlterTable {
            if self.local_state.schema_changes.is_empty() {
                return Err(KernelError::invalid_transaction_state(
                    "ALTER TABLE requires at least one schema change",
                ));
            }
            if self.local_state.is_blind_append {
                return Err(KernelError::invalid_transaction_state(
                    "ALTER TABLE cannot be marked as a blind append",
                ));
            }
        }
        if self.local_state.is_blind_append && self.common_state.data_change == Some(false) {
            return Err(KernelError::invalid_transaction_state(
                "blind append requires data_change to be true",
            ));
        }
        if self.local_state.is_blind_append && !self.local_state.schema_changes.is_empty() {
            return Err(KernelError::invalid_transaction_state(
                "blind append cannot include schema changes",
            ));
        }

        let removals = validate_unique_domains(
            self.local_state
                .domain_metadata_removals
                .iter()
                .map(String::as_str),
            |domain| {
                KernelError::invalid_transaction_state(format!(
                    "domain metadata '{domain}' is removed more than once"
                ))
            },
        )?;
        for metadata in &self.common_state.domain_metadata_additions {
            let domain = metadata.domain();
            if removals.contains(domain) {
                return Err(KernelError::invalid_transaction_state(format!(
                    "domain metadata '{domain}' cannot be added and removed in one transaction"
                )));
            }
        }
        // Duplicate additions are validated by the shared builder state.
        self.common_state.validate()?;
        Ok(())
    }
}

fn try_new_update_table<MODE: ExecutionMode>(
    builder: UpdateTableTransactionBuilder,
    committer: Box<dyn Committer>,
    engine: &dyn Engine,
) -> KernelResult<Transaction<UpdateTable, MODE>> {
    let TransactionBuilder {
        common_state,
        local_state,
    } = builder;
    let UpdateTableBuilderState {
        snapshot: read_snapshot,
        operation,
        schema_changes,
        domain_metadata_removals,
        is_blind_append,
    } = local_state;

    // important! before writing to the table we must check it is supported
    read_snapshot
        .table_configuration()
        .ensure_operation_supported(Operation::Write)?;

    // TODO(#3240): Validate that delta.enableRowTracking=true has the required protocol support
    // and materialized column-name properties. Materialized names must be distinct and must not
    // collide with physical data columns.

    // Read clustering columns from snapshot (returns None if clustering not enabled)
    let clustering_columns = read_snapshot.get_physical_clustering_columns(engine)?;

    let commit_timestamp = current_time_ms()?;

    let span = tracing::info_span!(
        "txn",
        path = %read_snapshot.table_root(),
        read_version = read_snapshot.version(),
    );

    let effective_table_config = read_snapshot.table_configuration().clone();

    validate_iceberg_compat_if_needed(
        &effective_table_config,
        &V2_VALIDATOR,
        IcebergCompatValidationContext::Write,
    )?;

    validate_iceberg_compat_if_needed(
        &effective_table_config,
        &V3_VALIDATOR,
        IcebergCompatValidationContext::Write,
    )?;

    let should_emit_metadata = !schema_changes.is_empty();
    let effective_table_config = if should_emit_metadata {
        apply_schema_changes(
            effective_table_config,
            schema_changes,
            operation.as_ref() == Some(&UpdateTableOperation::AlterTable),
        )?
    } else {
        effective_table_config
    };

    Ok(Transaction {
        read_snapshot_opt: Some(read_snapshot),
        should_emit_metadata,
        user_domain_removals: domain_metadata_removals,
        is_blind_append,
        physical_clustering_columns: clustering_columns,
        ..Transaction::new(
            common_state,
            effective_table_config,
            committer,
            operation.into(),
            span,
            commit_timestamp,
        )
    })
}

fn apply_schema_changes(
    table_config: TableConfiguration,
    changes: Vec<SchemaOperation>,
    is_alter_table: bool,
) -> KernelResult<TableConfiguration> {
    let unsupported_iceberg_compat = [TableFeature::IcebergCompatV2, TableFeature::IcebergCompatV3]
        .into_iter()
        .find(|feature| {
            table_config.is_feature_enabled(feature)
                && (is_alter_table || feature == &TableFeature::IcebergCompatV3)
        });
    if let Some(feature) = unsupported_iceberg_compat {
        return Err(KernelError::unsupported(format!(
            "Schema changes are not yet supported on tables with {feature} enabled"
        )));
    }
    if table_config.is_feature_enabled(&TableFeature::AllowColumnDefaults) {
        return Err(KernelError::unsupported(
            "Schema changes are not yet supported on tables with allowColumnDefaults enabled",
        ));
    }
    evolve_table_config(&table_config, changes)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;

    use crate::arrow::array::{ArrayRef, Int32Array, StringArray};
    use crate::arrow::datatypes::Schema as ArrowSchema;
    use crate::arrow::record_batch::RecordBatch;
    use crate::committer::FileSystemCommitter;
    use crate::engine::arrow_conversion::TryIntoArrow;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::sync::SyncEngine;
    use crate::expressions::column_name;
    #[cfg(feature = "adaptive-metadata-in-dev")]
    use crate::expressions::ColumnName;
    use crate::log_segment::LogSegment;
    use crate::schema::{schema_ref, DataType, StructField};
    use crate::table_features::{ColumnMappingMode, TableFeature};
    use crate::table_properties::{
        ENABLE_ICEBERG_COMPAT_V2, ENABLE_ICEBERG_COMPAT_V3, ENABLE_ROW_TRACKING,
    };
    use crate::transaction::{SchemaOperation, UpdateTableOperation};
    use crate::unit_test_utils::{
        create_log_path, load_test_table, test_schema_flat, test_schema_flat_with_column_mapping,
        MockProtocolBuilder, MockTableConfigurationBuilder,
    };
    use crate::{KernelError, Result, Snapshot, SnapshotRef};

    #[rstest]
    #[case::write(UpdateTableOperation::Write)]
    #[case::delete(UpdateTableOperation::Delete)]
    #[case::custom(UpdateTableOperation::Custom("vendor.write".to_string()))]
    fn builder_collects_existing_table_transaction_intent(
        #[case] operation: UpdateTableOperation,
    ) -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let transaction = snapshot
            .update_table(operation.clone(), "test-engine")
            .with_operation_parameters([("mode", Some("Append"))])
            .with_operation_metrics([("numFiles", Some("3"))])
            .with_transaction_id("app", 7)
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

        assert_eq!(transaction.operation, operation.into());
        assert_eq!(transaction.engine_info, "test-engine");
        assert_eq!(
            transaction.operation_parameters.as_ref().unwrap()["mode"].as_deref(),
            Some("Append")
        );
        assert_eq!(
            transaction.operation_metrics.as_ref().unwrap()["numFiles"].as_deref(),
            Some("3")
        );
        assert_eq!(transaction.set_transactions[0].app_id, "app");
        assert_eq!(transaction.set_transactions[0].version, 7);
        assert_eq!(
            transaction.set_transactions[0].last_updated,
            Some(transaction.commit_timestamp)
        );
        assert!(transaction.data_change);
        Ok(())
    }

    #[test]
    fn replacement_setters_use_the_last_value_and_empty_correlation_clears() -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let transaction = snapshot
            .update_table(UpdateTableOperation::Delete, "second-engine")
            .with_correlation_id("first-correlation")
            .with_correlation_id("")
            .with_operation_parameters([("first", Some("value"))])
            .with_operation_parameters([("second", Some("value"))])
            .with_operation_metrics([("first", Some("value"))])
            .with_operation_metrics([("second", Some("value"))])
            .with_data_change(false)
            .with_data_change(true)
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

        assert_eq!(transaction.operation, UpdateTableOperation::Delete.into());
        assert_eq!(transaction.engine_info, "second-engine");
        assert!(transaction.correlation_id.is_none());
        let parameters = transaction.operation_parameters.as_ref().unwrap();
        assert_eq!(parameters.len(), 1);
        assert_eq!(parameters["second"].as_deref(), Some("value"));
        let metrics = transaction.operation_metrics.as_ref().unwrap();
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics["second"].as_deref(), Some("value"));
        assert!(transaction.data_change);
        Ok(())
    }

    #[test]
    fn builder_debug_summarizes_intent() -> Result<()> {
        let (_engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let debug = format!(
            "{:?}",
            snapshot
                .update_table(UpdateTableOperation::Write, "test-engine")
                .add_column(StructField::nullable("new_column", DataType::STRING))
        );

        assert!(debug.contains("UpdateTableTransactionBuilder"));
        assert!(debug.contains("operation: Write"));
        assert!(debug.contains("schema_change_count: 1"));
        Ok(())
    }

    #[test]
    fn builder_applies_optional_transaction_intent() -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let commit_info_schema = schema_ref! { nullable "tag": STRING };
        let arrow_schema: ArrowSchema = commit_info_schema.as_ref().try_into_arrow()?;
        let commit_info = Box::new(ArrowEngineData::new(RecordBatch::try_new(
            Arc::new(arrow_schema),
            vec![Arc::new(StringArray::from(vec![Some("value")]))],
        )?));
        let mut transaction = snapshot
            .update_table(UpdateTableOperation::Write, "test-engine")
            .with_commit_info(commit_info, commit_info_schema)
            .with_correlation_id("correlation-id")
            .with_data_change(false)
            .with_schema_changes([SchemaOperation::add_column(
                None,
                StructField::nullable("new_column", DataType::STRING),
            )])
            .expect("non-empty schema changes must be accepted by the setter")
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
        transaction.ack_column_defaults();
        transaction.ack_row_tracking_preservation();
        transaction = transaction.with_row_tracking_high_water_mark(17)?;

        assert_eq!(
            transaction.correlation_id.as_deref(),
            Some("correlation-id")
        );
        assert!(!transaction.data_change);
        assert!(transaction.column_defaults_acknowledged);
        assert!(transaction.row_tracking_preservation_acknowledged);
        assert_eq!(transaction.provided_row_tracking_high_water_mark, Some(17));
        assert!(transaction.engine_commit_info.is_some());
        assert!(transaction.user_domain_removals.is_empty());
        Ok(())
    }

    #[rstest]
    #[case(0)]
    #[case(2)]
    fn builder_rejects_non_single_row_commit_info(#[case] row_count: usize) -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let commit_info_schema = schema_ref! { nullable "tag": STRING };
        let arrow_schema: ArrowSchema = commit_info_schema.as_ref().try_into_arrow()?;
        let commit_info = Box::new(ArrowEngineData::new(RecordBatch::try_new(
            Arc::new(arrow_schema),
            vec![Arc::new(StringArray::from(vec![Some("value"); row_count]))],
        )?));

        let error = snapshot
            .update_table(UpdateTableOperation::Write, "test-engine")
            .with_commit_info(commit_info, commit_info_schema)
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Connector commit info must contain exactly one row"),
            "{error}"
        );
        Ok(())
    }

    #[cfg(feature = "adaptive-metadata-in-dev")]
    #[test]
    fn builder_collects_nested_schema_intent() -> Result<()> {
        let (_engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let builder = snapshot
            .update_table(UpdateTableOperation::AlterTable, "test-engine")
            .add_column_at(
                ColumnName::new(["parent"]),
                StructField::nullable("child", DataType::STRING),
            );

        assert_eq!(builder.local_state.schema_changes.len(), 1);
        Ok(())
    }

    #[test]
    fn alter_table_operation_uses_unified_builder() -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let transaction = snapshot
            .update_table(UpdateTableOperation::AlterTable, "test-engine")
            .add_column(StructField::nullable("new_column", DataType::STRING))
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

        assert_eq!(
            transaction.operation,
            UpdateTableOperation::AlterTable.into()
        );
        assert!(transaction.should_emit_metadata);
        assert!(transaction
            .effective_table_config
            .logical_schema()
            .contains("new_column"));
        Ok(())
    }

    #[test]
    fn custom_operation_with_schema_change_uses_behavioral_validation() -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let transaction = snapshot
            .update_table(
                UpdateTableOperation::Custom("vendor schema write".to_string()),
                "test-engine",
            )
            .add_column(StructField::nullable("new_column", DataType::STRING))
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

        assert!(transaction.should_emit_metadata);
        Ok(())
    }

    #[test]
    fn custom_operation_rejects_reserved_known_name() -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let error = snapshot
            .update_table(
                UpdateTableOperation::Custom("ALTER TABLE".to_string()),
                "test-engine",
            )
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
            .unwrap_err();

        assert!(error.to_string().contains("reserved"));
        Ok(())
    }

    #[test]
    fn alter_table_requires_schema_changes() -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let error = snapshot
            .update_table(UpdateTableOperation::AlterTable, "test-engine")
            .with_data_change(false)
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("requires at least one schema change"));
        Ok(())
    }

    #[rstest]
    fn empty_schema_changes_are_rejected(
        #[values(false, true)] blind_append: bool,
        #[values(false, true)] unknown_length: bool,
    ) -> Result<()> {
        let (_engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let builder = snapshot
            .transaction_builder()
            .with_operation(UpdateTableOperation::Write);
        let builder = if blind_append {
            builder.with_blind_append()
        } else {
            builder
        };
        let result = if unknown_length {
            builder.with_schema_changes(std::iter::from_fn(|| None::<SchemaOperation>))
        } else {
            builder.with_schema_changes([])
        };
        let error = result.unwrap_err();

        assert!(matches!(&error, KernelError::Generic(_)));
        assert!(error
            .to_string()
            .contains("with_schema_changes requires at least one schema operation"));
        Ok(())
    }

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
                Self::AddColumn => vec![SchemaOperation::add_column(
                    None,
                    StructField::nullable("added", DataType::STRING),
                )],
                Self::AlreadyNullable => vec![SchemaOperation::SetNullable {
                    column: column_name!("id"),
                }],
            }
        }
    }

    #[rstest]
    #[case::plain(None, false, true, true)]
    #[case::v2_enabled(Some(TableFeature::IcebergCompatV2), true, true, false)]
    #[case::v2_disabled(Some(TableFeature::IcebergCompatV2), false, true, true)]
    #[case::v3_enabled(Some(TableFeature::IcebergCompatV3), true, false, false)]
    #[case::v3_disabled(Some(TableFeature::IcebergCompatV3), false, true, true)]
    #[case::column_defaults(Some(TableFeature::AllowColumnDefaults), true, false, false)]
    fn schema_change_validation_by_operation_and_feature(
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
    ) -> Result<()> {
        let snapshot = schema_validation_snapshot(feature.as_ref(), enabled);
        let is_alter = operation == UpdateTableOperation::AlterTable;
        let changes_allowed = if is_alter {
            alter_changes_allowed
        } else {
            non_alter_changes_allowed
        };
        let builder = snapshot
            .clone()
            .transaction_builder()
            .with_operation(operation.clone());
        let changes = input.changes();
        if input == SchemaInput::Empty {
            let error = builder.with_schema_changes(changes).unwrap_err();
            assert!(matches!(&error, KernelError::Generic(message)
                if message == "with_schema_changes requires at least one schema operation"));
            return Ok(());
        }
        let builder = if input == SchemaInput::Omitted {
            builder
        } else {
            builder
                .with_schema_changes(changes)
                .expect("non-empty schema changes must be accepted by the setter")
        };
        let result = builder.build(&SyncEngine::new(), Box::new(FileSystemCommitter::new()));
        if input == SchemaInput::Omitted && is_alter {
            let error = result.unwrap_err();
            assert!(
                matches!(&error, KernelError::InvalidTransactionState(message)
                if message == "ALTER TABLE requires at least one schema change")
            );
        } else if input != SchemaInput::Omitted && !changes_allowed {
            let error = result.unwrap_err();
            let expected = format!(
                "Schema changes are not yet supported on tables with {} enabled",
                feature.unwrap()
            );
            assert!(matches!(&error, KernelError::Unsupported(message) if message == &expected));
        } else {
            let transaction = result?;
            assert_eq!(transaction.operation, Some(operation.into()));
            assert_eq!(
                transaction.should_emit_metadata,
                input != SchemaInput::Omitted
            );
            assert!(!transaction.should_emit_protocol);
            let schema = transaction.effective_table_config.logical_schema();
            if input == SchemaInput::AddColumn {
                assert_eq!(
                    schema.field("added").unwrap().data_type(),
                    &DataType::STRING
                );
                assert!(schema.field("added").unwrap().is_nullable());
                assert_eq!(schema.num_fields(), snapshot.schema().num_fields() + 1);
            } else {
                assert_eq!(schema, snapshot.schema());
            }
        }
        Ok(())
    }

    #[rstest]
    fn empty_schema_changes_are_rejected_after_queued_changes(
        #[values(TableFeature::IcebergCompatV2, TableFeature::IcebergCompatV3)]
        feature: TableFeature,
        #[values(false, true)] enabled: bool,
        #[values(false, true)] individual_setter: bool,
        #[values(SchemaInput::AddColumn, SchemaInput::AlreadyNullable)] input: SchemaInput,
    ) -> Result<()> {
        let snapshot = schema_validation_snapshot(Some(&feature), enabled);
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
        assert_eq!(builder.local_state.schema_changes.len(), 1);
        let error = builder.with_schema_changes([]).unwrap_err();
        assert!(matches!(&error, KernelError::Generic(message)
            if message == "with_schema_changes requires at least one schema operation"));
        Ok(())
    }

    fn schema_validation_snapshot(feature: Option<&TableFeature>, enabled: bool) -> SnapshotRef {
        let mut config = MockTableConfigurationBuilder::new().with_schema(test_schema_flat());
        let mut features: Vec<_> = feature.into_iter().cloned().collect();
        if matches!(
            feature,
            Some(TableFeature::IcebergCompatV2 | TableFeature::IcebergCompatV3)
        ) {
            features.push(TableFeature::ColumnMapping);
            config = config
                .with_schema(test_schema_flat_with_column_mapping())
                .with_column_mapping(ColumnMappingMode::Name)
                .with_properties([("delta.columnMapping.maxColumnId", "2")]);
            let property = if feature == Some(&TableFeature::IcebergCompatV2) {
                ENABLE_ICEBERG_COMPAT_V2
            } else {
                features.extend([TableFeature::RowTracking, TableFeature::DomainMetadata]);
                config = config.with_properties([(ENABLE_ROW_TRACKING, "true")]);
                ENABLE_ICEBERG_COMPAT_V3
            };
            config = config.with_properties([(property, if enabled { "true" } else { "false" })]);
        }
        let config = config
            .with_protocol(MockProtocolBuilder::new().with_features(features).build())
            .build();
        if let Some(feature) = feature {
            assert!(config.is_feature_supported(feature));
            assert_eq!(config.is_feature_enabled(feature), enabled);
        }
        let log_segment = LogSegment::new_for_version_zero(
            config.table_root().join("_delta_log/").unwrap(),
            create_log_path("file:///_delta_log/00000000000000000000.json"),
        )
        .expect("valid version-zero log segment");
        Arc::new(Snapshot::new(log_segment, config).expect("valid schema-validation snapshot"))
    }

    #[test]
    fn nonempty_schema_change_calls_preserve_iterator_items_and_call_order() {
        let (engine, snapshot, _tempdir) =
            load_test_table("table-without-dv-small").expect("load test table");
        let mut changes = [SchemaOperation::add_column(
            None,
            StructField::nullable("new_column", DataType::STRING),
        )]
        .into_iter();
        let builder = snapshot
            .transaction_builder()
            .with_operation(UpdateTableOperation::Write)
            .with_schema_changes(std::iter::from_fn(move || changes.next()))
            .expect("non-empty iterator must be accepted")
            .with_schema_changes([SchemaOperation::SetNullable {
                column: column_name!("new_column"),
            }])
            .expect("subsequent non-empty schema changes must be accepted")
            .add_column(StructField::nullable("another_column", DataType::INTEGER));

        assert_eq!(builder.local_state.schema_changes.len(), 3);
        let transaction = builder
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
            .expect("schema changes must build in call order");
        assert!(transaction.should_emit_metadata);
        let schema = transaction.effective_table_config.logical_schema();
        assert!(schema.contains("new_column"));
        assert!(schema.contains("another_column"));
    }

    #[derive(Debug)]
    enum InvalidIntent {
        AlterTableBlindAppend,
        BlindAppendWithoutDataChange,
        BlindAppendWithSchemaChange,
    }

    #[rstest]
    #[case::alter_blind_append(
        InvalidIntent::AlterTableBlindAppend,
        "cannot be marked as a blind append"
    )]
    #[case::blind_append_without_data_change(
        InvalidIntent::BlindAppendWithoutDataChange,
        "requires data_change to be true"
    )]
    #[case::blind_append_with_schema_change(
        InvalidIntent::BlindAppendWithSchemaChange,
        "cannot include schema changes"
    )]
    fn invalid_intent_is_rejected(
        #[case] intent: InvalidIntent,
        #[case] expected: &str,
    ) -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let operation = match intent {
            InvalidIntent::AlterTableBlindAppend => UpdateTableOperation::AlterTable,
            _ => UpdateTableOperation::Write,
        };
        let builder = snapshot.update_table(operation, "test-engine");
        let builder = match intent {
            InvalidIntent::AlterTableBlindAppend => builder
                .add_column(StructField::nullable("new_column", DataType::STRING))
                .with_blind_append(),
            InvalidIntent::BlindAppendWithoutDataChange => {
                builder.with_blind_append().with_data_change(false)
            }
            InvalidIntent::BlindAppendWithSchemaChange => builder
                .add_column(StructField::nullable("new_column", DataType::STRING))
                .with_blind_append(),
        };

        let error = builder
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
            .unwrap_err();
        assert!(matches!(&error, KernelError::InvalidTransactionState(_)));
        assert!(error.to_string().contains(expected), "{error}");
        Ok(())
    }

    #[derive(Clone, Copy, Debug)]
    enum StagedFileAction {
        Add,
        Remove,
        DeletionVectorUpdate,
    }

    #[rstest]
    fn alter_table_infers_data_change_from_staged_file_batches(
        #[values(
            StagedFileAction::Add,
            StagedFileAction::Remove,
            StagedFileAction::DeletionVectorUpdate
        )]
        action: StagedFileAction,
    ) -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let mut transaction = snapshot
            .update_table(UpdateTableOperation::AlterTable, "test-engine")
            .add_column(StructField::nullable("new_column", DataType::STRING))
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

        transaction.resolve_data_change();
        assert!(!transaction.data_change);

        let data = || {
            let values = Arc::new(Int32Array::from_iter_values([1])) as ArrayRef;
            Box::new(ArrowEngineData::new(
                RecordBatch::try_from_iter([("value", values)]).unwrap(),
            ))
        };
        match action {
            StagedFileAction::Add => transaction
                .staged_data_changes
                .add_files_metadata
                .push(data()),
            StagedFileAction::Remove | StagedFileAction::DeletionVectorUpdate => {
                let data = crate::FilteredEngineData::with_all_rows_selected(data());
                match action {
                    StagedFileAction::Remove => transaction
                        .staged_data_changes
                        .remove_files_metadata
                        .push(data),
                    StagedFileAction::DeletionVectorUpdate => {
                        transaction.staged_data_changes.dv_matched_files.push(data)
                    }
                    StagedFileAction::Add => unreachable!(),
                }
            }
        }
        transaction.resolve_data_change();
        assert!(transaction.data_change);
        Ok(())
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn alter_table_preserves_explicit_data_change(#[case] data_change: bool) -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let mut transaction = snapshot
            .update_table(UpdateTableOperation::AlterTable, "test-engine")
            .add_column(StructField::nullable("new_column", DataType::STRING))
            .with_data_change(data_change)
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

        transaction.resolve_data_change();
        assert_eq!(transaction.data_change, data_change);
        Ok(())
    }

    #[test]
    fn schema_change_data_change_inference_does_not_depend_on_operation_name() -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let mut transaction = snapshot
            .update_table(
                UpdateTableOperation::Custom("CUSTOM".to_string()),
                "test-engine",
            )
            .add_column(StructField::nullable("new_column", DataType::STRING))
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

        transaction.resolve_data_change();
        assert!(!transaction.data_change);

        let values = Arc::new(Int32Array::from_iter_values([1])) as ArrayRef;
        transaction
            .staged_data_changes
            .add_files_metadata
            .push(Box::new(ArrowEngineData::new(RecordBatch::try_from_iter(
                [("value", values)],
            )?)));
        transaction.resolve_data_change();
        assert!(transaction.data_change);
        Ok(())
    }

    #[rstest]
    #[case::duplicate_app_id(&[], &[], &["app", "app"], "app_id app appears more than once")]
    #[case::duplicate_domain(
        &["domain", "domain"], &[], &[], "domain metadata 'domain' appears more than once"
    )]
    #[case::added_and_removed_domain(
        &["domain"], &["domain"], &[], "cannot be added and removed"
    )]
    #[case::duplicate_removals(
        &[], &["domain", "domain"], &[], "removed more than once"
    )]
    #[case::duplicate_removal_precedes_add_remove_conflict(
        &["domain"], &["domain", "domain"], &[], "removed more than once"
    )]
    #[case::add_remove_conflict_precedes_duplicate_addition(
        &["domain", "domain"], &["domain"], &[], "cannot be added and removed"
    )]
    #[case::duplicate_app_id_precedes_duplicate_addition(
        &["domain", "domain"], &[], &["app", "app"], "app_id app appears more than once"
    )]
    fn duplicate_builder_values_are_rejected(
        #[case] additions: &[&str],
        #[case] removals: &[&str],
        #[case] app_ids: &[&str],
        #[case] expected: &str,
    ) -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let mut builder = snapshot.update_table(UpdateTableOperation::Write, "test-engine");
        for domain in additions {
            builder = builder.with_domain_metadata(*domain, "value");
        }
        for domain in removals {
            builder = builder.with_domain_metadata_removed(*domain);
        }
        for app_id in app_ids {
            builder = builder.with_transaction_id(*app_id, 1);
        }

        let error = builder
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
            .unwrap_err();
        assert!(matches!(&error, KernelError::InvalidTransactionState(_)));
        assert!(error.to_string().contains(expected), "{error}");
        Ok(())
    }

    #[test]
    fn domain_metadata_without_table_feature_is_rejected_during_build() -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let error = snapshot
            .update_table(UpdateTableOperation::Write, "test-engine")
            .with_domain_metadata("app.config", "{}")
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
            .unwrap_err();

        assert!(error.to_string().contains("domainMetadata"), "{error}");
        Ok(())
    }
}

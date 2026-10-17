//! ALTER TABLE transaction FFI lifecycle.

use delta_kernel::transaction::builder::alter_table::{Modifying, Ready};

use super::committed::commit_result_to_committed_handle;
use super::deletion_vector::stage_deletion_vector_updates;
use super::update_table::decode_single_field;
use super::write_context::SharedWriteState;
use super::*;
use crate::scan::SharedScanMetadataIterator;

/// Opaque builder state for callers that cannot express Rust type-state transitions.
///
/// cbindgen:ignore
pub enum AlterTableBuilder {
    /// No schema operation has been queued.
    Ready(AlterTableTransactionBuilder<Ready>),
    /// At least one schema operation has been queued.
    Modifying(AlterTableTransactionBuilder<Modifying>),
}

impl From<AlterTableTransactionBuilder<Ready>> for AlterTableBuilder {
    fn from(builder: AlterTableTransactionBuilder<Ready>) -> Self {
        Self::Ready(builder)
    }
}

impl From<AlterTableTransactionBuilder<Modifying>> for AlterTableBuilder {
    fn from(builder: AlterTableTransactionBuilder<Modifying>) -> Self {
        Self::Modifying(builder)
    }
}

impl AlterTableBuilder {
    fn build(
        self,
        engine: &dyn delta_kernel::Engine,
        committer: Box<dyn delta_kernel::committer::Committer>,
    ) -> KernelResult<AlterTableTransaction> {
        match self {
            Self::Ready(_) => Err(delta_kernel::KernelError::invalid_transaction_state(
                "ALTER TABLE requires at least one schema change",
            )),
            Self::Modifying(builder) => builder.build(engine, committer),
        }
    }
}

macro_rules! with_alter_builder {
    ($value:expr, |$builder:ident| $body:expr) => {
        match $value {
            AlterTableBuilder::Ready($builder) => $body,
            AlterTableBuilder::Modifying($builder) => $body,
        }
    };
}

/// An owned ALTER builder. Build it or release it with [`free_alter_table_txn_builder`].
///
/// Setters consume the input handle on both success and error. Successful setters return its
/// replacement. The operation is fixed to ALTER TABLE and data-change intent is always false.
#[handle_descriptor(target=AlterTableBuilder, mutable=true, sized=true)]
pub struct ExclusiveAlterTableTransactionBuilder;

/// An owned ALTER transaction. Commit it or release it with [`free_alter_table_txn`].
///
/// Commit information, operation metrics, and user domain metadata can be supplied before commit.
/// File rewrites must preserve logical data; all Add and Remove actions use `dataChange = false`.
#[handle_descriptor(target=AlterTableTransaction, mutable=true, sized=true)]
pub struct ExclusiveAlterTableTransaction;

/// Creates an ALTER builder from a snapshot and connector name/version.
///
/// Returns an owned builder, or an error for invalid UTF-8 in `engine_info`.
///
/// # Safety
///
/// Both handles and `engine_info` must be valid and are borrowed for this call.
#[no_mangle]
pub unsafe extern "C" fn new_alter_table_txn_builder(
    snapshot: Handle<SharedSnapshot>,
    engine_info: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveAlterTableTransactionBuilder>> {
    let engine = unsafe { engine.as_ref() };
    let engine_info: KernelResult<String> =
        unsafe { TryFromStringSlice::try_from_slice(&engine_info) };
    engine_info
        .map(|engine_info| {
            let snapshot = unsafe { snapshot.clone_as_arc() };
            Box::new(AlterTableBuilder::from(snapshot.alter_table(engine_info))).into()
        })
        .into_extern_result(&engine)
}

/// Validates and builds an ALTER transaction with the filesystem committer.
///
/// Returns an error for missing or invalid schema operations, unsupported tables, or invalid
/// transaction metadata, or table-state I/O. On clustered tables, building may replay the log to
/// read clustering columns for write statistics.
///
/// # Safety
///
/// Both handles must be valid. Borrows `engine` and consumes `builder`, including on error.
#[no_mangle]
pub unsafe extern "C" fn alter_table_txn_builder_build(
    builder: Handle<ExclusiveAlterTableTransactionBuilder>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveAlterTableTransaction>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    builder
        .build(
            engine.engine().as_ref(),
            Box::new(FileSystemCommitter::new()),
        )
        .map(|transaction| Box::new(transaction).into())
        .into_extern_result(&engine)
}

/// Validates and builds an ALTER transaction with the supplied committer.
///
/// Returns the same validation errors as [`alter_table_txn_builder_build`].
///
/// # Safety
///
/// All handles must be valid. Borrows `engine` and consumes `builder` and `committer`, including
/// on error.
#[no_mangle]
pub unsafe extern "C" fn alter_table_txn_builder_build_with_committer(
    builder: Handle<ExclusiveAlterTableTransactionBuilder>,
    engine: Handle<SharedExternEngine>,
    committer: Handle<MutableCommitter>,
) -> ExternResult<Handle<ExclusiveAlterTableTransaction>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let committer = unsafe { committer.into_inner() };
    builder
        .build(engine.engine().as_ref(), committer)
        .map(|transaction| Box::new(transaction).into())
        .into_extern_result(&engine)
}

/// Releases an unused builder.
///
/// # Safety
///
/// `builder` must be valid and is consumed. Do not use or free it again.
#[no_mangle]
pub unsafe extern "C" fn free_alter_table_txn_builder(
    builder: Handle<ExclusiveAlterTableTransactionBuilder>,
) {
    builder.drop_handle();
}

/// Queues a nullable top-level field described by a single-field schema.
///
/// Returns an error if `field` is null or does not describe exactly one field.
///
/// # Safety
///
/// Handles must be valid. A non-null `field` and its nested pointers must be readable for this
/// call. Borrows `engine` and `field`; consumes `builder`, including on error.
#[no_mangle]
pub unsafe extern "C" fn alter_table_txn_builder_add_column(
    builder: Handle<ExclusiveAlterTableTransactionBuilder>,
    field: *const EngineSchema,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveAlterTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    unsafe { required_ref(field) }
        .and_then(decode_single_field)
        .map(|field| {
            with_alter_builder!(builder, |builder| {
                Box::new(AlterTableBuilder::from(builder.add_column(field))).into()
            })
        })
        .into_extern_result(&engine)
}

/// Queues a nullable field beneath `parent`.
///
/// Returns an error for null pointers, an invalid path, or a schema without exactly one field.
///
/// # Safety
///
/// Handles must be valid. Non-null pointers and all reachable data must be readable for this
/// call. Borrows `engine`, `parent`, and `field`; consumes `builder`, including on error.
#[no_mangle]
pub unsafe extern "C" fn alter_table_txn_builder_add_column_at(
    builder: Handle<ExclusiveAlterTableTransactionBuilder>,
    parent: *const FfiColumnName,
    field: *const EngineSchema,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveAlterTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let result: KernelResult<Handle<ExclusiveAlterTableTransactionBuilder>> = (|| {
        let parent = unsafe { required_ref(parent)?.try_to_column_name() }?;
        let field = decode_single_field(unsafe { required_ref(field) }?)?;
        Ok(with_alter_builder!(builder, |builder| {
            Box::new(AlterTableBuilder::from(
                builder.add_column_at(parent, field),
            ))
            .into()
        }))
    })();
    result.into_extern_result(&engine)
}

/// Queues a change making the field at `column` nullable.
///
/// Returns an error for a null pointer or an invalid path. Schema validation occurs at build.
///
/// # Safety
///
/// Handles must be valid. A non-null `column` and its path must be readable for this call.
/// Borrows `engine` and `column`; consumes `builder`, including on error.
#[no_mangle]
pub unsafe extern "C" fn alter_table_txn_builder_set_nullable(
    builder: Handle<ExclusiveAlterTableTransactionBuilder>,
    column: *const FfiColumnName,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveAlterTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    unsafe { required_ref(column).and_then(|column| column.try_to_column_name()) }
        .map(|column| {
            with_alter_builder!(builder, |builder| {
                Box::new(AlterTableBuilder::from(builder.set_nullable(column))).into()
            })
        })
        .into_extern_result(&engine)
}

/// Sets the correlation identifier on ALTER metric events.
///
/// Replaces the previous identifier; an empty string clears it. Invalid UTF-8 returns an error.
///
/// # Safety
///
/// Handles and `correlation_id` must be valid. Borrows `engine` and the string; consumes `builder`,
/// including on error.
#[no_mangle]
pub unsafe extern "C" fn alter_table_txn_builder_with_correlation_id(
    builder: Handle<ExclusiveAlterTableTransactionBuilder>,
    correlation_id: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveAlterTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let correlation_id: KernelResult<String> =
        unsafe { TryFromStringSlice::try_from_slice(&correlation_id) };
    correlation_id
        .map(|identifier| {
            with_alter_builder!(builder, |builder| {
                Box::new(AlterTableBuilder::from(
                    builder.with_correlation_id(identifier),
                ))
                .into()
            })
        })
        .into_extern_result(&engine)
}

/// Replaces the operation parameters recorded in commit history.
///
/// Returns an error for a null map, duplicate keys, or invalid UTF-8. The map is copied.
///
/// # Safety
///
/// Handles must be valid. A non-null `parameters` and its entries must be readable for this call.
/// Borrows `engine` and the map; consumes `builder`, including on error.
#[no_mangle]
pub unsafe extern "C" fn alter_table_txn_builder_with_operation_parameters(
    builder: Handle<ExclusiveAlterTableTransactionBuilder>,
    parameters: *const FfiNullableStringMap,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveAlterTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    unsafe {
        required_ref(parameters).and_then(|parameters| {
            with_alter_builder!(builder, |builder| apply_nullable_string_map(
                builder,
                parameters,
                TransactionBuilder::with_operation_parameters,
            )
            .map(AlterTableBuilder::from))
        })
    }
    .map(|builder| Box::new(builder).into())
    .into_extern_result(&engine)
}

/// Replaces the operation metrics recorded in commit history.
///
/// Returns an error for a null map, duplicate keys, or invalid UTF-8. The map is copied.
///
/// # Safety
///
/// Handles must be valid. A non-null `metrics` and its entries must be readable for this call.
/// Borrows `engine` and the map; consumes `builder`, including on error.
#[no_mangle]
pub unsafe extern "C" fn alter_table_txn_builder_with_operation_metrics(
    builder: Handle<ExclusiveAlterTableTransactionBuilder>,
    metrics: *const FfiNullableStringMap,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveAlterTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    unsafe {
        required_ref(metrics).and_then(|metrics| {
            with_alter_builder!(builder, |builder| apply_nullable_string_map(
                builder,
                metrics,
                TransactionBuilder::with_operation_metrics,
            )
            .map(AlterTableBuilder::from))
        })
    }
    .map(|builder| Box::new(builder).into())
    .into_extern_result(&engine)
}

/// Supplies one connector-defined commit-info row and its schema.
///
/// Replaces the previous row. Returns an error for a null or invalid schema; build checks that
/// the row count is exactly one. Kernel-owned history fields take precedence.
///
/// # Safety
///
/// Handles must be valid. A non-null `schema` and its reachable data must be readable for this
/// call. Borrows `engine` and `schema`; consumes `builder` and `commit_info`, including on error.
#[no_mangle]
pub unsafe extern "C" fn alter_table_txn_builder_with_commit_info(
    builder: Handle<ExclusiveAlterTableTransactionBuilder>,
    commit_info: Handle<ExclusiveEngineData>,
    schema: *const EngineSchema,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveAlterTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let commit_info = unsafe { commit_info.into_inner() };
    let engine = unsafe { engine.as_ref() };
    unsafe {
        required_ref(schema).and_then(|schema| {
            with_alter_builder!(builder, |builder| apply_commit_info(
                builder,
                commit_info,
                schema,
                TransactionBuilder::with_commit_info,
            )
            .map(AlterTableBuilder::from))
        })
    }
    .map(|builder| Box::new(builder).into())
    .into_extern_result(&engine)
}

/// Adds user-controlled domain metadata.
///
/// Returns an error for invalid UTF-8. Build validates uniqueness and domain eligibility.
///
/// # Safety
///
/// Handles and strings must be valid. Borrows `engine` and the strings; consumes `builder`,
/// including on error.
#[no_mangle]
pub unsafe extern "C" fn alter_table_txn_builder_with_domain_metadata(
    builder: Handle<ExclusiveAlterTableTransactionBuilder>,
    domain: KernelStringSlice,
    configuration: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveAlterTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    unsafe {
        with_alter_builder!(builder, |builder| apply_domain_metadata(
            builder,
            domain,
            configuration,
            TransactionBuilder::with_domain_metadata,
        )
        .map(AlterTableBuilder::from))
    }
    .map(|builder| Box::new(builder).into())
    .into_extern_result(&engine)
}

/// Adds an application transaction identifier to emit as a transaction action.
///
/// Returns an error for invalid UTF-8. Build rejects duplicate application identifiers.
///
/// # Safety
///
/// Handles and `app_id` must be valid. Borrows `engine` and the string; consumes `builder`,
/// including on error.
#[no_mangle]
pub unsafe extern "C" fn alter_table_txn_builder_with_transaction_id(
    builder: Handle<ExclusiveAlterTableTransactionBuilder>,
    app_id: KernelStringSlice,
    version: i64,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveAlterTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let app_id: KernelResult<String> = unsafe { TryFromStringSlice::try_from_slice(&app_id) };
    app_id
        .map(|app_id| {
            with_alter_builder!(builder, |builder| {
                Box::new(AlterTableBuilder::from(
                    builder.with_transaction_id(app_id, version),
                ))
                .into()
            })
        })
        .into_extern_result(&engine)
}

/// Replaces operation metrics before commit.
///
/// Returns a replacement transaction, or an error for null `metrics`, invalid UTF-8, or duplicate
/// map keys. Replaces builder metrics and the nested `operationMetrics` field in connector commit
/// information.
///
/// # Safety
///
/// Handles must be valid. A non-null `metrics` and its nested pointers must be readable for this
/// call. Borrows `engine` and `metrics`; consumes `txn`, including on error.
#[no_mangle]
pub unsafe extern "C" fn alter_table_txn_with_operation_metrics(
    txn: Handle<ExclusiveAlterTableTransaction>,
    metrics: *const FfiNullableStringMap,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveAlterTableTransaction>> {
    let txn = unsafe { *txn.into_inner() };
    let engine = unsafe { engine.as_ref() };
    unsafe {
        required_ref(metrics).and_then(|metrics| {
            apply_nullable_string_map(txn, metrics, Transaction::with_operation_metrics)
        })
    }
    .map(|txn| Box::new(txn).into())
    .into_extern_result(&engine)
}

/// Replaces the connector-defined `commitInfo` row before commit.
///
/// Returns a replacement transaction, or an error for null or invalid `schema`. Commit checks
/// that `commit_info` contains exactly one row. Kernel-owned fields and dedicated parameter or
/// metric maps take precedence over same-named fields in this row.
///
/// # Safety
///
/// Handles must be valid. A non-null `schema` and its reachable data must be readable for this
/// call. Borrows `engine` and `schema`; consumes `txn` and `commit_info`, including on error.
#[no_mangle]
pub unsafe extern "C" fn alter_table_txn_with_commit_info(
    txn: Handle<ExclusiveAlterTableTransaction>,
    commit_info: Handle<ExclusiveEngineData>,
    schema: *const EngineSchema,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveAlterTableTransaction>> {
    let txn = unsafe { *txn.into_inner() };
    let commit_info = unsafe { commit_info.into_inner() };
    let engine = unsafe { engine.as_ref() };
    unsafe {
        required_ref(schema).and_then(|schema| {
            apply_commit_info(txn, commit_info, schema, Transaction::with_commit_info)
        })
    }
    .map(|txn| Box::new(txn).into())
    .into_extern_result(&engine)
}

/// Adds user-controlled domain metadata before commit.
///
/// Returns a replacement transaction, or an error for invalid UTF-8. Commit validates domain
/// eligibility and uniqueness, including additions supplied through the builder.
///
/// # Safety
///
/// Handles and strings must be valid. Borrows `engine`, `domain`, and `configuration`; consumes
/// `txn`, including on error.
#[no_mangle]
pub unsafe extern "C" fn alter_table_txn_with_domain_metadata(
    txn: Handle<ExclusiveAlterTableTransaction>,
    domain: KernelStringSlice,
    configuration: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveAlterTableTransaction>> {
    let txn = unsafe { *txn.into_inner() };
    let engine = unsafe { engine.as_ref() };
    unsafe {
        apply_domain_metadata(
            txn,
            domain,
            configuration,
            Transaction::with_domain_metadata,
        )
    }
    .map(|txn| Box::new(txn).into())
    .into_extern_result(&engine)
}

/// Returns owned write state using the evolved schema, or an error if the transaction cannot write.
///
/// The state can outlive the transaction. Release it with `free_write_state`. The transaction
/// remains owned by the caller on success and error.
///
/// # Safety
///
/// `txn` must point to a valid live handle and is borrowed for this call. `engine` must be valid
/// and is borrowed. A null `txn` returns an error.
#[no_mangle]
pub unsafe extern "C" fn alter_table_txn_write_state(
    txn: *const Handle<ExclusiveAlterTableTransaction>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<SharedWriteState>> {
    let engine = unsafe { engine.as_ref() };
    unsafe { required_ref(txn) }
        .and_then(|txn| unsafe { txn.as_ref() }.write_state())
        .map(Into::into)
        .into_extern_result(&engine)
}

/// Stages replacement files written with the evolved schema. Returns true on success, or an error
/// for a null `txn`.
///
/// The connector must preserve logical data across the added and removed files.
///
/// # Safety
///
/// `txn` must point to a valid handle and is mutably borrowed; it remains owned by the caller.
/// `write_metadata` must be valid and is consumed on success and error. `engine` must be valid and
/// is borrowed. A null `txn` is allowed and returns an error.
#[no_mangle]
pub unsafe extern "C" fn alter_table_txn_add_files(
    txn: *mut Handle<ExclusiveAlterTableTransaction>,
    write_metadata: Handle<ExclusiveEngineData>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let write_metadata = unsafe { write_metadata.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let result = unsafe { required_mut(txn) }.map(|txn| {
        unsafe { txn.as_mut() }.add_files(write_metadata);
        true
    });
    result.into_extern_result(&engine)
}

/// Stages selected files for removal during a data-preserving rewrite.
///
/// `data` must use the scan-row schema and preserve scan metadata. Select only rows active in the
/// original scan. A null or empty selection vector selects every row; use it only when all rows are
/// active and intended for removal. Returns true on success, or an error for a null `txn` or an
/// invalid selection vector.
///
/// # Safety
///
/// `txn` must point to a valid handle and is mutably borrowed. `engine` must be valid and is
/// borrowed. `data` must be valid and is consumed on success and error. A non-null selection vector
/// with nonzero length must address that many readable bytes for this call. A null `txn` returns
/// an error. The transaction remains owned by the caller on success and error.
#[no_mangle]
pub unsafe extern "C" fn alter_table_txn_remove_files(
    txn: *mut Handle<ExclusiveAlterTableTransaction>,
    data: Handle<ExclusiveEngineData>,
    selection_vector: *const u8,
    selection_vector_len: usize,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let data = unsafe { data.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let result = unsafe { required_mut(txn) }.and_then(|txn| unsafe {
        stage_file_removals(txn.as_mut(), data, selection_vector, selection_vector_len)
    });
    result.into_extern_result(&engine)
}

/// Acknowledges preservation of Stable Row IDs and Stable Row Commit Versions in replacement files.
///
/// Required before committing removals on row-tracked tables. Kernel does not inspect file contents
/// to verify preservation. Returns true on success, or an error for a null `txn`.
///
/// # Safety
///
/// `txn` must point to a valid handle and is mutably borrowed. `engine` must be valid and is
/// borrowed. A null `txn` returns an error. The transaction remains owned by the caller.
#[no_mangle]
pub unsafe extern "C" fn alter_table_txn_ack_row_tracking_preservation(
    txn: *mut Handle<ExclusiveAlterTableTransaction>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let engine = unsafe { engine.as_ref() };
    unsafe { required_mut(txn) }
        .map(|txn| {
            unsafe { txn.as_mut() }.ack_row_tracking_preservation();
            true
        })
        .into_extern_result(&engine)
}

/// Stages replacement deletion-vector descriptors without changing logical data.
///
/// Each descriptor must match a selected scan row and preserve its logical rows, for example by
/// relocating the same bitmap. Kernel does not verify bitmap equivalence. Matched scan metadata
/// must have accurate `numRecords` and valid AddFile fields. An unmatched path or invalid metadata
/// returns an error without changing the transaction. Returns true on success.
///
/// # Safety
///
/// `txn` must point to a valid handle and is mutably borrowed. A null `txn` returns an error.
/// `dv_map` and `scan_iter` must be valid and are consumed on success and error. `engine` must be
/// valid and is borrowed. The transaction remains owned by the caller.
#[no_mangle]
pub unsafe extern "C" fn alter_table_txn_update_deletion_vectors(
    txn: *mut Handle<ExclusiveAlterTableTransaction>,
    dv_map: Handle<ExclusiveDvDescriptorMap>,
    scan_iter: Handle<SharedScanMetadataIterator>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let dv_map = unsafe { dv_map.into_inner() };
    let scan_iter = unsafe { scan_iter.into_inner() };
    let engine = unsafe { engine.as_ref() };
    unsafe { required_mut(txn) }
        .and_then(|txn| unsafe { stage_deletion_vector_updates(txn.as_mut(), *dv_map, &scan_iter) })
        .map(|_| true)
        .into_extern_result(&engine)
}

/// Commits an ALTER transaction and returns an owned committed-transaction handle.
///
/// Returns an error for validation, I/O, or commit conflicts. Release the result with
/// [`free_committed_transaction`]. Conflict/retry state is not preserved by this FFI.
///
/// # Safety
///
/// Both handles must be valid. Borrows `engine` and consumes `txn`, including on error.
#[no_mangle]
pub unsafe extern "C" fn alter_table_txn_commit(
    txn: Handle<ExclusiveAlterTableTransaction>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveCommittedTransaction>> {
    let transaction = unsafe { txn.into_inner() };
    let engine = unsafe { engine.as_ref() };
    commit_result_to_committed_handle(transaction.commit(engine.engine().as_ref()))
        .into_extern_result(&engine)
}

/// Releases an unused transaction.
///
/// # Safety
///
/// `txn` must be valid and is consumed. Do not use or free it again.
#[no_mangle]
pub unsafe extern "C" fn free_alter_table_txn(txn: Handle<ExclusiveAlterTableTransaction>) {
    txn.drop_handle();
}

unsafe fn required_ref<'a, T>(value: *const T) -> KernelResult<&'a T> {
    unsafe { value.as_ref() }.ok_or_else(|| {
        delta_kernel::KernelError::invalid_transaction_state(
            "ALTER TABLE argument must not be null",
        )
    })
}

unsafe fn required_mut<'a, T>(value: *mut T) -> KernelResult<&'a mut T> {
    unsafe { value.as_mut() }.ok_or_else(|| {
        delta_kernel::KernelError::invalid_transaction_state(
            "ALTER TABLE argument must not be null",
        )
    })
}

//! Compile-time boundaries for transaction states and execution modes.

/// Create-table transactions cannot remove files.
///
/// ```compile_fail,E0599
/// use delta_kernel::engine_data::FilteredEngineData;
/// use delta_kernel::transaction::{CreateTable, Transaction};
///
/// fn remove(txn: &mut Transaction<CreateTable>, data: FilteredEngineData) {
///     txn.remove_files(data);
/// }
/// ```
pub struct CreateRemove;

/// Shared configuration and write-state APIs are available for any execution mode.
///
/// ```no_run
/// use delta_kernel::transaction::{UpdateTable, Transaction, ExecutionMode};
/// use delta_kernel::Result;
///
/// fn configure<E: ExecutionMode>(txn: Transaction<UpdateTable, E>) -> Result<()> {
///     let txn = txn.with_operation_metrics([("numFiles", Some("1"))]);
///     let _state = txn.write_state()?;
///     Ok(())
/// }
/// ```
pub struct SharedConfiguration;

/// The default transaction mode is imperative, and its commit API remains available.
///
/// ```no_run
/// use delta_kernel::transaction::{CommitResult, UpdateTable, Imperative, Transaction};
/// use delta_kernel::{Result, Engine};
///
/// fn commit(txn: Transaction, engine: &dyn Engine) -> Result<CommitResult> {
///     let txn: Transaction<UpdateTable, Imperative> = txn;
///     txn.commit(engine)
/// }
/// ```
pub struct ImperativeCommit;

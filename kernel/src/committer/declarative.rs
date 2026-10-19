use delta_kernel_derive::internal_api;
use url::Url;

use crate::plans::{Operation, PlanBuilder, RelationRef, ScopedPlanExecutor};
use crate::{FileMeta, KernelError, KernelResult};

/// Writes out ordered [`RelationRef`] inputs as JSON and returns metadata for the written file.
///
/// - `executor` holds the relations and runs the write plan.
/// - `actions` yields the commit action relations in write order.
/// - `path` is the destination JSON file URL.
#[internal_api]
pub(crate) fn write_commit_json_declarative(
    executor: &dyn ScopedPlanExecutor,
    actions: impl IntoIterator<Item = RelationRef>,
    path: Url,
) -> KernelResult<FileMeta> {
    let plan = PlanBuilder::write_json(
        actions.into_iter().map(PlanBuilder::relation_source),
        path.clone(),
        false,
        true,
    )?
    .build()?;
    let file_meta = FileMeta::try_from(
        executor
            .execute_op(Operation::QueryPlan(plan))?
            .into_data()?,
    )?;
    if file_meta.location != path {
        return Err(KernelError::generic(format!(
            "Commit write returned metadata for {} instead of {path}",
            file_meta.location
        )));
    }
    Ok(file_meta)
}

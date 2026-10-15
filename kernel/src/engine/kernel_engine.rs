//! Engine state for cancellation-aware imperative reads.

use std::sync::Arc;

#[cfg(feature = "declarative-plans")]
use crate::plans::PlanExecutor;
use crate::{
    CancellationTokenRef, Engine, EvaluationHandler, JsonHandler, ParquetHandler, StorageHandler,
};

#[cfg(test)]
mod tests;

/// An engine adapter that supplies an operation's cancellation token to imperative reads.
///
/// Read helpers inherit this token unless a read supplies its own. Handler getters return the
/// original handlers, so direct handler calls do not inherit cancellation. Writes, evaluation,
/// and declarative execution retain their underlying behavior.
///
/// Reuse the adapter across calls in one operation, including metadata access after snapshot
/// construction. Snapshots retain neither the adapter nor its token; read iterators own their
/// cancellation state and can outlive the adapter.
///
/// Place this adapter outside other engine decorators, or make outer decorators forward the
/// cancellation-token accessor.
pub struct KernelEngine {
    evaluation: Arc<dyn EvaluationHandler>,
    storage: Arc<dyn StorageHandler>,
    json: Arc<dyn JsonHandler>,
    parquet: Arc<dyn ParquetHandler>,
    cancellation_token: CancellationTokenRef,
    #[cfg(feature = "declarative-plans")]
    plan_executor: Option<Arc<dyn PlanExecutor>>,
}

impl KernelEngine {
    /// Captures `engine`'s handlers and binds `cancellation_token` without issuing I/O.
    pub fn new(engine: &dyn Engine, cancellation_token: CancellationTokenRef) -> Self {
        Self {
            evaluation: engine.evaluation_handler(),
            storage: engine.storage_handler(),
            json: engine.json_handler(),
            parquet: engine.parquet_handler(),
            cancellation_token,
            #[cfg(feature = "declarative-plans")]
            plan_executor: engine.plan_executor(),
        }
    }
}

impl Engine for KernelEngine {
    /// Returns the operation's read cancellation token.
    fn cancellation_token(&self) -> Option<CancellationTokenRef> {
        Some(self.cancellation_token.clone())
    }

    /// Returns the original expression evaluator.
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.evaluation.clone()
    }

    /// Returns the original storage handler.
    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.storage.clone()
    }

    /// Returns the original JSON handler.
    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json.clone()
    }

    /// Returns the original Parquet handler.
    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.parquet.clone()
    }

    /// Returns the original declarative plan executor.
    #[cfg(feature = "declarative-plans")]
    fn plan_executor(&self) -> Option<Arc<dyn PlanExecutor>> {
        self.plan_executor.clone()
    }
}

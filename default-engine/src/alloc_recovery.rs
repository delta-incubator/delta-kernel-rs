//! Internal task-boundary support for experimental FFI allocation recovery.

use std::any::Any;
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Poll;

/// An unwind payload transported between a worker and its originating FFI call.
pub type PanicPayload = Box<dyn Any + Send>;

/// The source of an allocation refusal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum FailureReason {
    /// The configured budget cannot accommodate the allocation.
    Limit = 1,
    /// The underlying allocator refused the allocation.
    Allocator = 2,
}

/// Allocation-failure information carried without formatting a Rust error message.
#[derive(Clone, Copy, Debug)]
pub struct AllocationFailure {
    /// The source of the failure.
    pub reason: FailureReason,
    /// The requested allocation size in bytes.
    pub requested: usize,
}

const CONFIGURING: usize = usize::MAX;
static OPERATIONS: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static GUARDED: Cell<bool> = const { Cell::new(false) };
    static RECOVERING: Cell<bool> = const { Cell::new(false) };
    static REFUSAL: Cell<FailureReason> = const { Cell::new(FailureReason::Allocator) };
    static CURRENT: RefCell<Option<Arc<Operation>>> = const { RefCell::new(None) };
    static PAYLOAD: RefCell<Option<Box<AllocationFailure>>> = const { RefCell::new(None) };
}

/// Shared failure state for a native operation and its executor tasks.
#[derive(Debug)]
pub struct Operation {
    failure: AtomicU8,
}

impl Operation {
    /// Creates an operation, preventing configuration changes until all tasks release it.
    pub fn new() -> Arc<Self> {
        loop {
            let count = OPERATIONS.load(Ordering::Acquire);
            if count != CONFIGURING
                && OPERATIONS
                    .compare_exchange_weak(count, count + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                break;
            }
            std::hint::spin_loop();
        }
        Arc::new(Self {
            failure: AtomicU8::new(0),
        })
    }

    /// Returns the first allocation failure recorded by this operation or its tasks.
    pub fn failure(&self) -> Option<FailureReason> {
        match self.failure.load(Ordering::Acquire) {
            1 => Some(FailureReason::Limit),
            2 => Some(FailureReason::Allocator),
            _ => None,
        }
    }

    fn record(&self, reason: FailureReason) {
        let _ = self
            .failure
            .compare_exchange(0, reason as u8, Ordering::AcqRel, Ordering::Acquire);
    }
}

impl Drop for Operation {
    fn drop(&mut self) {
        OPERATIONS.fetch_sub(1, Ordering::Release);
    }
}

/// Runs a configuration update only when no protected operations or their tasks exist.
/// Returns false without executing `update` when an operation is active.
pub fn configure_if_idle(update: impl FnOnce() -> bool) -> bool {
    if OPERATIONS
        .compare_exchange(0, CONFIGURING, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return false;
    }
    let result = update();
    OPERATIONS.store(0, Ordering::Release);
    result
}

/// Whether allocations on this thread belong to protected application work.
pub fn is_guarded() -> bool {
    GUARDED.try_with(Cell::get).unwrap_or(false)
}

/// Whether this thread is unwinding or reporting an allocation failure.
pub fn is_recovering() -> bool {
    RECOVERING.try_with(Cell::get).unwrap_or(true)
}

/// Records a refusal without allocating; called before the allocator returns null.
pub fn record_refusal(reason: FailureReason) {
    let _ = REFUSAL.try_with(|slot| slot.set(reason));
}

/// Returns the operation inherited by newly submitted executor tasks.
pub fn current_operation() -> Option<Arc<Operation>> {
    CURRENT.with(|slot| slot.borrow().clone())
}

/// Enables bounded recovery allocations for the remainder of the current guard.
pub fn begin_recovery() {
    RECOVERING.with(|slot| slot.set(true));
}

struct Guard {
    previous: Option<Arc<Operation>>,
    guarded: bool,
    recovering: bool,
}

impl Guard {
    fn enter(operation: Arc<Operation>) -> Self {
        let recovering = RECOVERING.with(|slot| slot.replace(true));
        // Provision before arming the hook. A failed provision is deliberately fail-stop.
        let ready = PAYLOAD.with(|slot| slot.borrow().is_some());
        if !ready {
            let payload = Box::new(AllocationFailure {
                reason: FailureReason::Allocator,
                requested: 0,
            });
            PAYLOAD.with(|slot| *slot.borrow_mut() = Some(payload));
        }
        let previous = CURRENT.with(|slot| slot.replace(Some(operation)));
        let guarded = GUARDED.with(|slot| slot.replace(true));
        RECOVERING.with(|slot| slot.set(false));
        Self {
            previous,
            guarded,
            recovering,
        }
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        CURRENT.with(|slot| *slot.borrow_mut() = self.previous.take());
        GUARDED.with(|slot| slot.set(self.guarded));
        RECOVERING.with(|slot| slot.set(self.recovering));
    }
}

/// Starts allocation-failure unwinding outside the allocator.
/// Aborts if there is no prepared guard or if the thread is already unwinding.
pub fn unwind_allocation_failure(requested: usize) -> ! {
    if !is_guarded() || std::thread::panicking() {
        std::process::abort();
    }
    begin_recovery();
    let reason = REFUSAL.with(Cell::get);
    CURRENT.with(|slot| {
        if let Some(operation) = slot.borrow().as_ref() {
            operation.record(reason);
        }
    });
    let Some(mut payload) = PAYLOAD.with(|slot| slot.borrow_mut().take()) else {
        std::process::abort();
    };
    *payload = AllocationFailure { reason, requested };
    resume_unwind(payload)
}

/// Recovers the typed allocation payload for reuse on the receiving thread.
/// Returns unrelated panic payloads unchanged.
pub fn recycle_failure(payload: PanicPayload) -> Result<AllocationFailure, PanicPayload> {
    match payload.downcast::<AllocationFailure>() {
        Ok(payload) => {
            let failure = *payload;
            PAYLOAD.with(|slot| *slot.borrow_mut() = Some(payload));
            Ok(failure)
        }
        Err(payload) => Err(payload),
    }
}

/// Runs synchronous work inside a prepared allocation-recovery boundary.
/// The returned panic payload must be propagated or decoded by the caller.
pub fn run_sync<T>(operation: Arc<Operation>, task: impl FnOnce() -> T) -> Result<T, PanicPayload> {
    let _guard = Guard::enter(operation.clone());
    catch_unwind(AssertUnwindSafe(|| {
        if let Some(reason) = operation.failure() {
            record_refusal(reason);
            unwind_allocation_failure(0);
        }
        task()
    }))
}

/// Propagates a worker panic on the waiting thread without invoking the panic hook.
pub fn resume_task<T>(result: Result<T, PanicPayload>) -> T {
    match result {
        Ok(value) => value,
        Err(payload) => {
            begin_recovery();
            resume_unwind(payload)
        }
    }
}

/// Captures the submitting operation and catches failures at each worker poll boundary.
pub fn capture_future<F: Future + Send>(
    task: F,
) -> impl Future<Output = Result<F::Output, PanicPayload>> + Send {
    let operation = current_operation();
    let mut task = Box::pin(task);
    std::future::poll_fn(move |cx| {
        let poll = || task.as_mut().poll(cx);
        let result = match operation.as_ref() {
            Some(operation) => run_sync(operation.clone(), poll),
            None => catch_unwind(AssertUnwindSafe(poll)),
        };
        match result {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
            Err(payload) => Poll::Ready(Err(payload)),
        }
    })
}

/// Captures failures from a detached task; its operation retains allocation-failure state.
pub fn capture_spawn<F: Future<Output = ()> + Send>(task: F) -> impl Future<Output = ()> + Send {
    let task = capture_future(task);
    async move {
        if let Err(payload) = task.await {
            if let Err(payload) = recycle_failure(payload) {
                resume_unwind(payload);
            }
        }
    }
}

/// Captures the submitting operation for a blocking executor closure.
pub fn capture_blocking<F: FnOnce() -> R + Send, R>(
    task: F,
) -> impl FnOnce() -> Result<R, PanicPayload> + Send {
    let operation = current_operation();
    move || match operation {
        Some(operation) => run_sync(operation, task),
        None => catch_unwind(AssertUnwindSafe(task)),
    }
}

/// Runs reporting or cleanup with the bounded recovery allowance, restoring thread state afterward.
pub fn with_recovery<T>(body: impl FnOnce() -> T) -> T {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            RECOVERING.with(|slot| slot.set(self.0));
        }
    }
    let _restore = Restore(RECOVERING.with(|slot| slot.replace(true)));
    body()
}

use std::alloc::{GlobalAlloc, Layout, System};
#[cfg(feature = "test-ffi")]
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Once;

use delta_kernel::KernelResult;
use delta_kernel_default_engine::alloc_recovery::{self as recovery, FailureReason, Operation};

use crate::error::{AllocateError, ExternResult, FFIKernelError, IntoExternResult};
use crate::handle::{Handle, HandleDescriptor, HandleOps};
use crate::kernel_string_slice;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static ALLOCATING: AtomicUsize = AtomicUsize::new(0);

struct InFlight;
impl InFlight {
    fn enter() -> Self {
        loop {
            let count = ALLOCATING.load(Ordering::Acquire);
            if count != usize::MAX
                && ALLOCATING
                    .compare_exchange_weak(count, count + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                return Self;
            }
            std::hint::spin_loop();
        }
    }
}
impl Drop for InFlight {
    fn drop(&mut self) {
        ALLOCATING.fetch_sub(1, Ordering::Release);
    }
}

fn allocated(bytes: usize) {
    let live = CURRENT.fetch_add(bytes, Ordering::AcqRel) + bytes;
    PEAK.fetch_max(live, Ordering::Relaxed);
}
static LIMIT: AtomicUsize = AtomicUsize::new(usize::MAX);
static CEILING: AtomicUsize = AtomicUsize::new(usize::MAX);
static INSTALL_HOOK: Once = Once::new();

#[cfg(feature = "test-ffi")]
thread_local! {
    static FAIL_AFTER: Cell<Option<u64>> = const { Cell::new(None) };
}

pub(crate) struct BudgetAllocator;

impl BudgetAllocator {
    pub(crate) fn current_usage(&self) -> usize {
        CURRENT.load(Ordering::Acquire)
    }
    pub(crate) fn peak_usage(&self) -> usize {
        PEAK.load(Ordering::Relaxed)
    }
    pub(crate) fn reset_peak_usage(&self) {
        PEAK.store(CURRENT.load(Ordering::Acquire), Ordering::Relaxed);
    }
}

fn reserve(bytes: usize) -> bool {
    let limit = if recovery::is_guarded() && !recovery::is_recovering() {
        LIMIT.load(Ordering::Acquire)
    } else {
        CEILING.load(Ordering::Acquire)
    };
    let mut live = LIVE.load(Ordering::Acquire);
    loop {
        let Some(total) = live.checked_add(bytes).filter(|total| *total <= limit) else {
            recovery::record_refusal(FailureReason::Limit);
            return false;
        };
        match LIVE.compare_exchange_weak(live, total, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return true,
            Err(actual) => live = actual,
        }
    }
}

fn injected() -> bool {
    #[cfg(feature = "test-ffi")]
    if recovery::is_guarded() && !recovery::is_recovering() {
        return FAIL_AFTER
            .try_with(|slot| match slot.get() {
                Some(0) => {
                    slot.set(None);
                    true
                }
                Some(remaining) => {
                    slot.set(Some(remaining - 1));
                    false
                }
                None => false,
            })
            .unwrap_or(false);
    }
    false
}

// SAFETY: All methods reserve before allocation, roll back failures, and never unwind.
// Reallocation conservatively charges both blocks while the underlying allocator may move data.
unsafe impl GlobalAlloc for BudgetAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _in_flight = InFlight::enter();
        if !reserve(layout.size()) {
            return std::ptr::null_mut();
        }
        let ptr = if injected() {
            std::ptr::null_mut()
        } else {
            unsafe { System.alloc(layout) }
        };
        if ptr.is_null() {
            LIVE.fetch_sub(layout.size(), Ordering::AcqRel);
            recovery::record_refusal(FailureReason::Allocator);
        } else {
            allocated(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let _in_flight = InFlight::enter();
        if !reserve(layout.size()) {
            return std::ptr::null_mut();
        }
        let ptr = if injected() {
            std::ptr::null_mut()
        } else {
            unsafe { System.alloc_zeroed(layout) }
        };
        if ptr.is_null() {
            LIVE.fetch_sub(layout.size(), Ordering::AcqRel);
            recovery::record_refusal(FailureReason::Allocator);
        } else {
            allocated(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::AcqRel);
        CURRENT.fetch_sub(layout.size(), Ordering::AcqRel);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let _in_flight = InFlight::enter();
        if !reserve(new_size) {
            return std::ptr::null_mut();
        }
        let result = if injected() {
            std::ptr::null_mut()
        } else {
            unsafe { System.realloc(ptr, layout, new_size) }
        };
        if result.is_null() {
            LIVE.fetch_sub(new_size, Ordering::AcqRel);
            recovery::record_refusal(FailureReason::Allocator);
        } else {
            LIVE.fetch_sub(layout.size(), Ordering::AcqRel);
            if new_size >= layout.size() {
                allocated(new_size - layout.size());
            } else {
                CURRENT.fetch_sub(layout.size() - new_size, Ordering::AcqRel);
            }
        }
        result
    }
}

pub(super) fn configure(limit: u64, recovery_bytes: u64) -> bool {
    let Ok(limit) = usize::try_from(limit) else {
        return false;
    };
    let Ok(recovery_bytes) = usize::try_from(recovery_bytes) else {
        return false;
    };
    let Some(ceiling) = limit.checked_add(recovery_bytes) else {
        return false;
    };
    if (limit == 0) != (recovery_bytes == 0) {
        return false;
    }
    INSTALL_HOOK.call_once(|| {
        std::alloc::set_alloc_error_hook(|layout| {
            recovery::unwind_allocation_failure(layout.size());
        })
    });
    recovery::configure_if_idle(|| {
        if ALLOCATING
            .compare_exchange(0, usize::MAX, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        let valid = limit == 0 || ceiling >= LIVE.load(Ordering::Acquire);
        if valid {
            CEILING.store(
                if limit == 0 { usize::MAX } else { ceiling },
                Ordering::Release,
            );
            LIMIT.store(
                if limit == 0 { usize::MAX } else { limit },
                Ordering::Release,
            );
        }
        ALLOCATING.store(0, Ordering::Release);
        valid
    })
}

#[cfg(feature = "test-ffi")]
pub(super) fn inject_failure(count: u64) -> bool {
    // Provision the hook even when only synthetic failure injection is enabled.
    INSTALL_HOOK.call_once(|| {
        std::alloc::set_alloc_error_hook(|layout| {
            recovery::unwind_allocation_failure(layout.size());
        })
    });
    FAIL_AFTER.with(|slot| slot.set(Some(count)));
    true
}

pub(super) unsafe fn protect_handle<H>(
    allocator: &dyn AllocateError,
    body: impl FnOnce() -> KernelResult<Handle<H>>,
) -> ExternResult<Handle<H>>
where
    H: HandleDescriptor + HandleOps<H::Target, H::Mutable, H::Sized>,
{
    let operation = recovery::current_operation().unwrap_or_else(Operation::new);
    let result = recovery::run_sync(operation.clone(), || unsafe {
        let result = body();
        if operation.failure().is_some() {
            recovery::with_recovery(|| match result {
                Ok(handle) => handle.drop_handle(),
                Err(error) => drop(error),
            });
            None
        } else {
            Some(result.into_extern_result(allocator))
        }
    });
    recovery::with_recovery(|| {
        let reason = match result {
            Ok(Some(result)) => return result,
            Ok(None) => match operation.failure() {
                Some(reason) => reason,
                None => std::process::abort(),
            },
            Err(payload) => match recovery::recycle_failure(payload) {
                Ok(failure) => failure.reason,
                // Preserve fail-stop behavior for unrelated panics at a C ABI boundary.
                Err(_) => std::process::abort(),
            },
        };
        let (kind, message) = match reason {
            FailureReason::Limit => (
                FFIKernelError::AllocationLimitExceeded,
                "Rust allocation limit exceeded during snapshot loading",
            ),
            FailureReason::Allocator => (
                FFIKernelError::AllocationFailed,
                "Rust allocator returned null during snapshot loading",
            ),
        };
        ExternResult::Err(unsafe { allocator.allocate_error(kind, kernel_string_slice!(message)) })
    })
}

#[cfg(all(test, feature = "test-ffi", unix))]
mod tests {
    use std::os::unix::process::ExitStatusExt;
    use std::process::Command;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    use delta_kernel_default_engine::executor::tokio::TokioBackgroundExecutor;
    use delta_kernel_default_engine::executor::TaskExecutor;

    use super::*;

    const LARGE: usize = 16 * 1024 * 1024;

    #[test]
    fn recovery_scenarios_in_subprocesses() {
        if let Ok(scenario) = std::env::var("KERNEL_ALLOC_RECOVERY_SCENARIO") {
            scenario_body(&scenario);
            return;
        }
        for scenario in [
            "limit",
            "fallible",
            "underlying",
            "worker",
            "concurrent",
            "configuration",
            "accounting",
            "contention",
            "unprotected",
            "double_failure",
        ] {
            let status = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "alloc_recovery::enabled::tests::recovery_scenarios_in_subprocesses",
                    "--nocapture",
                ])
                .env("KERNEL_ALLOC_RECOVERY_SCENARIO", scenario)
                .status()
                .unwrap();
            if ["unprotected", "double_failure"].contains(&scenario) {
                assert_eq!(status.signal(), Some(6), "{scenario}: {status}");
            } else {
                assert!(status.success(), "{scenario}: {status}");
            }
        }
    }

    fn scenario_body(scenario: &str) {
        assert!(configure(0, 0));
        match scenario {
            "configuration" => {
                let operation = Operation::new();
                assert!(!configure(0, 0));
                drop(operation);
                assert!(configure(0, 0));
                assert!(!configure(u64::MAX, 1));
            }
            "limit" => {
                assert!(configure(
                    (LIVE.load(Ordering::Acquire) + 64 * 1024) as u64,
                    1024 * 1024
                ));
                let operation = Operation::new();
                let result = recovery::run_sync(operation.clone(), || {
                    std::hint::black_box(vec![0u8; LARGE])
                });
                assert_eq!(
                    recovery::recycle_failure(result.unwrap_err())
                        .unwrap()
                        .reason,
                    FailureReason::Limit
                );
                drop(operation);
                assert!(configure(0, 0));
                assert_eq!(std::hint::black_box(vec![1u8; LARGE]).len(), LARGE);
            }
            "fallible" => {
                let mut data = vec![7u8; 8192];
                let before = CURRENT.load(Ordering::Acquire);
                assert!(configure(
                    (LIVE.load(Ordering::Acquire) + 64 * 1024) as u64,
                    1024 * 1024
                ));
                let result = recovery::run_sync(Operation::new(), || data.try_reserve(LARGE));
                assert!(result.unwrap().is_err());
                assert_eq!(data, vec![7u8; 8192]);
                assert!(CURRENT.load(Ordering::Acquire) < before + 64 * 1024);
                assert!(configure(0, 0));
            }
            "underlying" => {
                assert!(inject_failure(0));
                let operation = Operation::new();
                let result =
                    recovery::run_sync(operation.clone(), || std::hint::black_box(vec![0u8; 8192]));
                assert_eq!(
                    recovery::recycle_failure(result.unwrap_err())
                        .unwrap()
                        .reason,
                    FailureReason::Allocator
                );
                drop(operation);
                assert!(configure(0, 0));
            }
            "worker" => {
                let executor = TokioBackgroundExecutor::new();
                let operation = Operation::new();
                let result = recovery::run_sync(operation.clone(), || {
                    executor.block_on(async {
                        assert!(inject_failure(0));
                        std::hint::black_box(vec![0u8; 8192])
                    })
                });
                assert_eq!(
                    recovery::recycle_failure(result.unwrap_err())
                        .unwrap()
                        .reason,
                    FailureReason::Allocator
                );
                assert_eq!(operation.failure(), Some(FailureReason::Allocator));
                drop(operation);
                drop(executor);
                assert!(configure(0, 0));
            }
            "concurrent" => {
                let barrier = Arc::new(std::sync::Barrier::new(4));
                let threads: Vec<_> = (0..4)
                    .map(|_| {
                        let barrier = barrier.clone();
                        std::thread::spawn(move || {
                            barrier.wait();
                            assert!(inject_failure(0));
                            let result = recovery::run_sync(Operation::new(), || {
                                std::hint::black_box(vec![0u8; 8192])
                            });
                            assert_eq!(
                                recovery::recycle_failure(result.unwrap_err())
                                    .unwrap()
                                    .reason,
                                FailureReason::Allocator
                            );
                        })
                    })
                    .collect();
                for thread in threads {
                    thread.join().unwrap();
                }
                assert!(configure(0, 0));
            }
            "accounting" => {
                recovery::run_sync(Operation::new(), || unsafe {
                    let before = CURRENT.load(Ordering::Acquire);
                    let small = Layout::from_size_align(4096, 8).unwrap();
                    let ptr = BudgetAllocator.alloc_zeroed(small);
                    assert!(!ptr.is_null());
                    assert_eq!(std::slice::from_raw_parts(ptr, 4096), &[0; 4096]);
                    assert_eq!(CURRENT.load(Ordering::Acquire), before + 4096);
                    *ptr = 7;
                    let grown = BudgetAllocator.realloc(ptr, small, 8192);
                    assert!(!grown.is_null());
                    assert_eq!(*grown, 7);
                    assert_eq!(CURRENT.load(Ordering::Acquire), before + 8192);
                    assert!(inject_failure(0));
                    let large = Layout::from_size_align(8192, 8).unwrap();
                    assert!(BudgetAllocator.realloc(grown, large, 16384).is_null());
                    assert_eq!(*grown, 7);
                    assert_eq!(CURRENT.load(Ordering::Acquire), before + 8192);
                    BudgetAllocator.dealloc(grown, large);
                    assert_eq!(CURRENT.load(Ordering::Acquire), before);
                })
                .unwrap();
            }
            "contention" => {
                const BLOCK: usize = 1024 * 1024;
                let ready = Arc::new(std::sync::Barrier::new(9));
                let start = Arc::new(std::sync::Barrier::new(9));
                let held = Arc::new(std::sync::Barrier::new(9));
                let release = Arc::new(std::sync::Barrier::new(9));
                let successes = Arc::new(AtomicUsize::new(0));
                let threads: Vec<_> = (0..8)
                    .map(|_| {
                        let (ready, start, held, release, successes) = (
                            ready.clone(),
                            start.clone(),
                            held.clone(),
                            release.clone(),
                            successes.clone(),
                        );
                        std::thread::spawn(move || {
                            ready.wait();
                            start.wait();
                            recovery::run_sync(Operation::new(), || unsafe {
                                let layout = Layout::from_size_align(BLOCK, 8).unwrap();
                                let ptr = BudgetAllocator.alloc(layout);
                                if !ptr.is_null() {
                                    successes.fetch_add(1, Ordering::AcqRel);
                                }
                                held.wait();
                                release.wait();
                                if !ptr.is_null() {
                                    BudgetAllocator.dealloc(ptr, layout);
                                }
                            })
                            .unwrap();
                        })
                    })
                    .collect();
                ready.wait();
                let limit = LIVE.load(Ordering::Acquire) + 2 * BLOCK + 64 * 1024;
                assert!(configure(limit as u64, BLOCK as u64));
                start.wait();
                held.wait();
                assert_eq!(successes.load(Ordering::Acquire), 2);
                assert!(LIVE.load(Ordering::Acquire) <= limit);
                release.wait();
                for thread in threads {
                    thread.join().unwrap();
                }
                assert!(configure(0, 0));
            }
            "unprotected" => {
                assert!(configure(
                    (LIVE.load(Ordering::Acquire) + 64 * 1024) as u64,
                    1024 * 1024
                ));
                std::hint::black_box(vec![0u8; LARGE]);
                panic!("unprotected failure must abort");
            }
            "double_failure" => {
                struct AllocatesOnDrop;
                impl Drop for AllocatesOnDrop {
                    fn drop(&mut self) {
                        std::hint::black_box(vec![0u8; LARGE]);
                    }
                }
                assert!(configure(
                    (LIVE.load(Ordering::Acquire) + 64 * 1024) as u64,
                    1024 * 1024
                ));
                let _ = recovery::run_sync(Operation::new(), || {
                    let _cleanup = AllocatesOnDrop;
                    std::hint::black_box(vec![0u8; LARGE]);
                });
                panic!("a second failure during unwinding must abort");
            }
            _ => panic!("unknown scenario"),
        }
    }
}

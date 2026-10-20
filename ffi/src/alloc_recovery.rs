//! Experimental allocation limits and snapshot FFI recovery boundaries.

use delta_kernel::KernelResult;

#[cfg(not(feature = "alloc-recovery"))]
use crate::error::IntoExternResult;
use crate::error::{AllocateError, ExternResult};
use crate::handle::{Handle, HandleDescriptor, HandleOps};

#[cfg(feature = "alloc-recovery")]
mod enabled;
#[cfg(feature = "alloc-recovery")]
pub(crate) use enabled::BudgetAllocator;

/// Whether this native library supports experimental allocation-limit recovery.
/// Returns false for stable, tracking-only builds.
#[no_mangle]
pub extern "C" fn alloc_recovery_enabled() -> bool {
    cfg!(feature = "alloc-recovery")
}

/// Initializes the immutable requested-byte budget for experimental snapshot recovery.
///
/// Call once during single-threaded startup, before creating engines or starting native work.
/// `limit_bytes` bounds protected application allocations. `recovery_bytes` provides additional,
/// bounded capacity for cleanup and runtime housekeeping. Zero for both keeps enforcement off.
/// Returns false for unsupported builds, invalid limits, a ceiling below charged allocations, or
/// an already initialized policy. Failed calls do not consume initialization. This is not an RSS
/// limit. Allocations before initialization are unbounded.
#[no_mangle]
pub extern "C" fn configure_native_allocation_limit(limit_bytes: u64, recovery_bytes: u64) -> bool {
    #[cfg(feature = "alloc-recovery")]
    return enabled::configure(limit_bytes, recovery_bytes);
    #[cfg(not(feature = "alloc-recovery"))]
    {
        let _ = (limit_bytes, recovery_bytes);
        false
    }
}

/// Installs a thread-scoped, one-shot underlying-allocation failure for prototype tests.
///
/// The failure occurs after `successful_allocations` allocator calls on this thread, and only
/// while protected work is running. Returns false unless both `test-ffi` and `alloc-recovery`
/// were enabled. This does not consume real machine memory.
#[no_mangle]
pub extern "C" fn test_fail_native_allocation_after(successful_allocations: u64) -> bool {
    #[cfg(all(feature = "alloc-recovery", feature = "test-ffi"))]
    return enabled::inject_failure(successful_allocations);
    #[cfg(not(all(feature = "alloc-recovery", feature = "test-ffi")))]
    {
        let _ = successful_allocations;
        false
    }
}

/// Runs a handle-producing operation and consumes a discarded success after worker failure.
///
/// # Safety
/// `allocator` and any handles captured by `body` must obey their FFI contracts.
pub(crate) unsafe fn protect_handle<H>(
    allocator: &dyn AllocateError,
    body: impl FnOnce() -> KernelResult<Handle<H>>,
) -> ExternResult<Handle<H>>
where
    H: HandleDescriptor + HandleOps<H::Target, H::Mutable, H::Sized>,
{
    #[cfg(feature = "alloc-recovery")]
    return unsafe { enabled::protect_handle(allocator, body) };
    #[cfg(not(feature = "alloc-recovery"))]
    unsafe {
        body().into_extern_result(allocator)
    }
}

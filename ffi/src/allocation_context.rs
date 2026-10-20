//! Diagnostic context handles shared with host allocation owners.
//!
//! Allocation hooks use only thread-local context lookup, ownership reference counts, and atomic
//! counters. Handle metadata contributes to untagged process instrumentation, outside query totals.

use std::ptr;

use delta_kernel_memory_tracking::{
    add_external, capture, domain_snapshot, process_snapshot, release_external, without_tracking,
    AccountingDomain, AllocationSource, Context, Scope, Snapshot,
};

/// Opaque, reference-counted allocation-origin context. Free with `memory_context_free`.
pub struct MemoryContext(Context);

/// Opaque thread-confined scope. Leave on the entering thread, in reverse entry order.
pub struct MemoryContextScope {
    _scope: Scope,
}

/// A diagnostic snapshot of requested allocation bytes, independent of process RSS.
#[derive(Clone, Copy, Default)]
#[repr(C)]
pub struct MemoryStatistics {
    /// Bytes not yet released by the allocating owner.
    pub live_bytes: u64,
    /// Maximum simultaneously live bytes, not a sum of component peaks.
    pub peak_bytes: u64,
    /// Cumulative allocation bytes including positive reallocation growth.
    pub allocated_bytes: u64,
    /// Successful allocation/reallocation operations.
    pub allocations: u64,
    /// Live tracking prefixes and layout padding; excludes allocator bookkeeping.
    pub tracking_bytes: u64,
}

/// Creates an owned context labeled `id`, optionally underneath borrowed `parent`.
///
/// # Safety
/// `parent` must be null or a live context. Free the result with `memory_context_free`.
#[no_mangle]
pub unsafe extern "C" fn memory_context_new(
    id: u64,
    parent: *const MemoryContext,
) -> *mut MemoryContext {
    let parent = unsafe { parent.as_ref() }.map(|p| p.0.clone());
    without_tracking(|| Box::into_raw(Box::new(MemoryContext(Context::new(id, parent)))))
}

/// Returns an independent owned handle to borrowed `context`, or null for null input.
/// Free the returned handle with `memory_context_free`.
///
/// # Safety
/// `context` must be null or live for this call.
#[no_mangle]
pub unsafe extern "C" fn memory_context_clone(context: *const MemoryContext) -> *mut MemoryContext {
    without_tracking(|| {
        unsafe { context.as_ref() }.map_or(ptr::null_mut(), |c| {
            Box::into_raw(Box::new(MemoryContext(c.0.clone())))
        })
    })
}

/// Captures an owned copy of the current context, or returns null for untagged work.
/// Free a non-null result with `memory_context_free`.
#[no_mangle]
pub extern "C" fn memory_context_capture() -> *mut MemoryContext {
    without_tracking(|| {
        capture().map_or(ptr::null_mut(), |c| {
            Box::into_raw(Box::new(MemoryContext(c)))
        })
    })
}

/// Releases an owned context handle; outstanding scopes and allocations retain their own
/// references.
///
/// # Safety
/// `context` must be null or an owned, unreleased context not being accessed by another call.
#[no_mangle]
pub unsafe extern "C" fn memory_context_free(context: *mut MemoryContext) {
    if !context.is_null() {
        unsafe { drop(Box::from_raw(context)) };
    }
}

/// Enters borrowed `context`, or untagged execution for null, until `memory_context_leave`.
///
/// # Safety
/// `context` must be null or live. Leave the returned scope on this thread in reverse entry order.
#[no_mangle]
pub unsafe extern "C" fn memory_context_enter(
    context: *const MemoryContext,
) -> *mut MemoryContextScope {
    let context = unsafe { context.as_ref() }.map(|c| c.0.clone());
    without_tracking(|| {
        Box::into_raw(Box::new(MemoryContextScope {
            _scope: Scope::enter(context),
        }))
    })
}

/// Leaves and consumes a scope returned by `memory_context_enter`. Null is a no-op.
///
/// # Safety
/// The scope must be live and left once, on its entering thread, in reverse entry order.
#[no_mangle]
pub unsafe extern "C" fn memory_context_leave(scope: *mut MemoryContextScope) {
    if !scope.is_null() {
        unsafe { drop(Box::from_raw(scope)) };
    }
}

/// Reads `context` (null means process total) into borrowed `out`.
/// `source` is extensible: 0=Rust, 1=host Arrow, 2=host ABI, 3=other native, 4=combined,
/// 5=instrumentation. Instrumentation is normally untagged and absent from query totals.
/// Returns false for an unsupported source or null output, leaving the output unchanged.
/// Concurrent reads can mix counter observation times; quiescent reads are consistent.
///
/// # Safety
/// `context` must be null or live. Non-null `out` must be aligned and writable for one record.
#[no_mangle]
pub unsafe extern "C" fn memory_context_read(
    context: *const MemoryContext,
    source: u32,
    out: *mut MemoryStatistics,
) -> bool {
    let snapshot = unsafe { context.as_ref() }.map_or_else(process_snapshot, |c| c.0.snapshot());
    unsafe { write_statistics(snapshot, source, out) }
}

/// Reads an aggregate without retaining individual contexts. `domain` is extensible:
/// 0=process, 1=tagged, 2=untagged. Source selectors match `memory_context_read`.
/// Tagged allocations are counted once regardless of parent hierarchy; untagged does not mean
/// uninstrumented. Returns false for unsupported selectors or null output, leaving it unchanged.
///
/// # Safety
/// Non-null `out` must be aligned and writable for one record.
#[no_mangle]
pub unsafe extern "C" fn memory_domain_read(
    domain: u32,
    source: u32,
    out: *mut MemoryStatistics,
) -> bool {
    let domain = match domain {
        0 => AccountingDomain::Process,
        1 => AccountingDomain::Tagged,
        2 => AccountingDomain::Untagged,
        _ => return false,
    };
    unsafe { write_statistics(domain_snapshot(domain), source, out) }
}

/// Adds/releases externally owned allocation bytes in borrowed `context`.
/// `source` uses the extensible values from `memory_context_read`, excluding combined and Rust.
/// Null context records untagged process bytes. Returns false for an unsupported source or a
/// byte count too large for this platform.
///
/// # Safety
/// The context must be null or live. Release exactly the bytes charged by this owner in the same
/// source; never charge imported buffers or release a logical reference before backing ownership
/// ends.
#[no_mangle]
pub unsafe extern "C" fn memory_context_record(
    context: *const MemoryContext,
    source: u32,
    bytes: u64,
    allocate: bool,
) -> bool {
    let context = unsafe { context.as_ref() }.map(|c| &c.0);
    let Ok(bytes) = usize::try_from(bytes) else {
        return false;
    };
    let source = match source {
        1 => AllocationSource::Arrow,
        2 => AllocationSource::Abi,
        3 => AllocationSource::Other,
        5 => AllocationSource::Instrumentation,
        _ => return false,
    };
    if allocate {
        add_external(context, source, bytes)
    } else {
        release_external(context, source, bytes)
    }
    true
}

unsafe fn write_statistics(snapshot: Snapshot, source: u32, out: *mut MemoryStatistics) -> bool {
    if source > 5 || out.is_null() {
        return false;
    }
    let statistics = match source {
        4 => snapshot.total,
        5 => snapshot.sources[AllocationSource::Instrumentation as usize],
        _ => snapshot.sources[source as usize],
    };
    unsafe {
        out.write(MemoryStatistics {
            live_bytes: statistics.live_bytes,
            peak_bytes: statistics.peak_bytes,
            allocated_bytes: statistics.allocated_bytes,
            allocations: statistics.allocations,
            tracking_bytes: statistics.tracking_bytes,
        })
    };
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handles_scopes_and_allocations_keep_the_origin_alive() {
        unsafe {
            let context = memory_context_new(17, ptr::null());
            let copy = memory_context_clone(context);
            let scope = memory_context_enter(context);
            memory_context_free(context);
            let bytes = vec![31u8; 8192];
            let captured = memory_context_capture();
            memory_context_leave(scope);
            let mut statistics = MemoryStatistics::default();
            assert!(memory_context_read(copy, 0, &mut statistics));
            assert_eq!(statistics.live_bytes, 8192);
            std::thread::spawn(move || drop(bytes)).join().unwrap();
            assert!(memory_context_read(captured, 0, &mut statistics));
            assert_eq!(statistics.live_bytes, 0);
            assert_eq!(statistics.peak_bytes, 8192);
            memory_context_free(copy);
            memory_context_free(captured);
        }
    }

    #[test]
    fn external_owners_share_a_combined_peak_without_summing_source_peaks() {
        unsafe {
            let parent = memory_context_new(1, ptr::null());
            let context = memory_context_new(2, parent);
            assert!(memory_context_record(context, 1, 100, true));
            assert!(memory_context_record(context, 1, 100, false));
            assert!(memory_context_record(context, 2, 200, true));
            let mut statistics = MemoryStatistics::default();
            assert!(memory_context_read(parent, 4, &mut statistics));
            assert_eq!(statistics.live_bytes, 200);
            assert_eq!(statistics.peak_bytes, 200);
            assert_eq!(statistics.allocated_bytes, 300);
            assert_eq!(statistics.allocations, 2);
            assert!(memory_context_record(context, 2, 200, false));
            assert!(memory_context_read(parent, 4, &mut statistics));
            assert_eq!(statistics.live_bytes, 0);
            memory_context_free(parent);
            memory_context_free(context);
        }
    }

    #[test]
    fn nulls_and_unknown_sources_have_defined_results() {
        unsafe {
            memory_context_free(ptr::null_mut());
            memory_context_leave(ptr::null_mut());
            assert!(memory_context_clone(ptr::null()).is_null());
            assert!(memory_context_record(ptr::null(), 1, 100, true));
            assert!(memory_context_record(ptr::null(), 1, 100, false));
            let context = memory_context_new(1, ptr::null());
            assert!(!memory_context_record(context, 0, 100, true));
            assert!(!memory_context_record(context, 4, 100, true));
            assert!(!memory_context_read(context, 4, ptr::null_mut()));
            let mut statistics = MemoryStatistics {
                live_bytes: 42,
                ..Default::default()
            };
            assert!(!memory_context_read(context, u32::MAX, &mut statistics));
            assert_eq!(statistics.live_bytes, 42);
            let scope = memory_context_enter(ptr::null());
            assert!(memory_context_capture().is_null());
            memory_context_leave(scope);
            memory_context_free(context);
        }
    }

    #[test]
    fn metadata_and_aggregate_domains_are_available_without_charging_the_query() {
        unsafe {
            let context = memory_context_new(37, ptr::null());
            let scope = memory_context_enter(context);
            let mut statistics = MemoryStatistics::default();
            assert!(memory_context_read(context, 5, &mut statistics));
            assert_eq!(statistics.live_bytes, 0);
            assert!(memory_domain_read(2, 5, &mut statistics));
            assert!(statistics.live_bytes > 0);
            assert!(memory_context_record(ptr::null(), 5, 40, true));
            assert!(memory_context_record(ptr::null(), 5, 40, false));
            statistics.live_bytes = 42;
            assert!(!memory_domain_read(u32::MAX, 4, &mut statistics));
            assert_eq!(statistics.live_bytes, 42);
            assert!(!memory_domain_read(0, u32::MAX, &mut statistics));
            assert_eq!(statistics.live_bytes, 42);
            memory_context_leave(scope);
            memory_context_free(context);
        }
    }
}

//! Experimental allocation-origin accounting, independent of an allocator installation.
//!
//! Install [`TrackingAllocator`] at the application or dynamic-library boundary. Contexts identify
//! allocation origin, not every object referencing the allocation. Borrowing or exporting a buffer
//! must not charge it again. External allocators can use [`Charge`] for backing allocations they
//! own. No allocation hook logs, invokes a callback, or allocates accounting metadata.
//! Tracking uses atomic counters and ownership prefixes, with no mutex or pointer registry on the
//! allocation path. This does not change synchronization inside the underlying system allocator.
//!
//! Counters measure requested bytes, not allocator usable size or RSS. Ancestors include
//! descendants; do not sum a parent and its children. Individual counters are atomic; multi-field
//! snapshots are consistent only when allocation activity is quiescent. Process totals are always
//! recorded alongside context counters. Peaks use live bytes, never by summing source peaks.
//! Accounting does not enforce a memory budget.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};

/// Allocation origin source. Imported buffers keep the source of their original allocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
pub enum AllocationSource {
    /// Allocations routed through Rust's global allocator.
    Rust,
    /// Backing buffers allocated by the host's Arrow allocator.
    Arrow,
    /// Host-allocated ABI structures and temporary marshaling buffers.
    Abi,
    /// Explicit C or other native allocations.
    Other,
    /// Accounting records, scopes, and host-side diagnostic scratch.
    Instrumentation,
}

/// Aggregate selection independent of an individual context's lifetime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccountingDomain {
    /// All instrumented allocations, including instrumentation metadata.
    Process,
    /// Allocations with an origin context, counted once regardless of context hierarchy.
    Tagged,
    /// Allocations without an origin context; this does not identify uninstrumented memory.
    Untagged,
}

/// Requested-byte statistics for one source or the combined live total.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(C)]
pub struct Statistics {
    /// Requested bytes not yet released by the underlying allocator.
    pub live_bytes: u64,
    /// Maximum simultaneously live requested bytes.
    pub peak_bytes: u64,
    /// Sum of successful allocation sizes and successful reallocation growth.
    pub allocated_bytes: u64,
    /// Successful allocations, including successful reallocations.
    pub allocations: u64,
    /// Live prefix/padding bytes added by the tracking allocator; excludes allocator bookkeeping.
    pub tracking_bytes: u64,
}

/// Inclusive statistics for an allocation context and its descendants.
#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct Snapshot {
    /// Caller-supplied context identifier; zero identifies the process aggregate.
    pub id: u64,
    /// Combined statistics, whose peak includes overlap across all sources.
    pub total: Statistics,
    /// AllocationSource statistics in [`AllocationSource`] discriminant order.
    pub sources: [Statistics; 5],
}

#[derive(Default)]
struct Counters {
    live: AtomicU64,
    peak: AtomicU64,
    allocated: AtomicU64,
    allocations: AtomicU64,
    tracking: AtomicU64,
}

impl Counters {
    const fn new() -> Self {
        Self {
            live: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            allocated: AtomicU64::new(0),
            allocations: AtomicU64::new(0),
            tracking: AtomicU64::new(0),
        }
    }

    fn add(&self, bytes: usize, tracking: usize) {
        let live = self.live.fetch_add(bytes as u64, Ordering::Relaxed) + bytes as u64;
        self.peak.fetch_max(live, Ordering::Relaxed);
        self.allocated.fetch_add(bytes as u64, Ordering::Relaxed);
        self.allocations.fetch_add(1, Ordering::Relaxed);
        self.tracking.fetch_add(tracking as u64, Ordering::Relaxed);
    }

    fn subtract(&self, bytes: usize, tracking: usize) {
        self.live.fetch_sub(bytes as u64, Ordering::Relaxed);
        self.tracking.fetch_sub(tracking as u64, Ordering::Relaxed);
    }

    fn snapshot(&self) -> Statistics {
        Statistics {
            live_bytes: self.live.load(Ordering::Relaxed),
            peak_bytes: self.peak.load(Ordering::Relaxed),
            allocated_bytes: self.allocated.load(Ordering::Relaxed),
            allocations: self.allocations.load(Ordering::Relaxed),
            tracking_bytes: self.tracking.load(Ordering::Relaxed),
        }
    }
}

struct State {
    id: u64,
    parent: Option<Context>,
    total: Counters,
    sources: [Counters; 5],
}

impl State {
    const fn new(id: u64, parent: Option<Context>) -> Self {
        Self {
            id,
            parent,
            total: Counters::new(),
            sources: [const { Counters::new() }; 5],
        }
    }

    fn add(&self, source: AllocationSource, bytes: usize, tracking: usize) {
        self.total.add(bytes, tracking);
        self.sources[source as usize].add(bytes, tracking);
    }

    fn subtract(&self, source: AllocationSource, bytes: usize, tracking: usize) {
        self.total.subtract(bytes, tracking);
        self.sources[source as usize].subtract(bytes, tracking);
    }

    fn snapshot(&self) -> Snapshot {
        Snapshot {
            id: self.id,
            total: self.total.snapshot(),
            sources: std::array::from_fn(|i| self.sources[i].snapshot()),
        }
    }
}

static PROCESS: State = State::new(0, None);
static TAGGED: State = State::new(0, None);
static UNTAGGED: State = State::new(0, None);

thread_local! {
    static ACTIVE: RefCell<Option<Context>> = const { RefCell::new(None) };
    static SUPPRESSED: Cell<bool> = const { Cell::new(false) };
}

/// Reference-counted allocation context. Allocations keep their origin alive until final release.
#[derive(Clone)]
pub struct Context(Arc<State>);

impl Context {
    /// Creates an allocation context with caller-supplied `id` and optional inclusive `parent`.
    ///
    /// IDs are diagnostic labels, not registry keys. Creation metadata is process instrumentation.
    /// This function does not enter the context and does not impose a memory limit.
    pub fn new(id: u64, parent: Option<Self>) -> Self {
        without_tracking(|| Self(Arc::new(State::new(id, parent))))
    }

    /// Enters this context on the current thread until the returned guard is dropped.
    /// Nested guards must be dropped in reverse order. The guard cannot be sent to another thread.
    pub fn enter(&self) -> Scope {
        Scope::enter(Some(self.clone()))
    }

    /// Reads this context's inclusive counters. Concurrent snapshots may mix observation times.
    pub fn snapshot(&self) -> Snapshot {
        self.0.snapshot()
    }

    /// Wraps `future`, restoring this context around every poll and around final destruction.
    /// Spawning a child task still requires wrapping that child at its own spawn boundary.
    pub fn track<F: Future>(&self, future: F) -> Tracked<F> {
        Tracked {
            future: ManuallyDrop::new(future),
            context: Some(self.clone()),
        }
    }
}

/// A thread-confined, nesting-aware active-context guard.
pub struct Scope {
    previous: Option<Context>,
    _thread_confined: PhantomData<*mut ()>,
}

impl Scope {
    /// Enters `context`, or the untagged process context for `None`.
    /// Guards must be dropped on this thread in reverse entry order.
    pub fn enter(context: Option<Context>) -> Self {
        let previous = ACTIVE.with(|active| active.replace(context));
        Self {
            previous,
            _thread_confined: PhantomData,
        }
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        let _ = ACTIVE.try_with(|active| active.replace(self.previous.take()));
    }
}

/// Captures the current allocation context for propagation to another thread or task.
/// Returns `None` when the thread is untagged or its thread-local state is being destroyed.
pub fn capture() -> Option<Context> {
    let pointer = active_pointer();
    if pointer.is_null() {
        None
    } else {
        // The active Scope retains the context for the duration of this call.
        unsafe { Arc::increment_strong_count(pointer) };
        Some(Context(unsafe { Arc::from_raw(pointer) }))
    }
}

/// Reads inclusive process counters, including allocations made outside a tagged context.
pub fn process_snapshot() -> Snapshot {
    domain_snapshot(AccountingDomain::Process)
}

/// Reads `domain` without retaining individual context handles.
/// Tagged/untagged partition live bytes, but their independent peaks do not add up
/// to the process peak. Concurrent multi-counter reads can mix observation times.
pub fn domain_snapshot(domain: AccountingDomain) -> Snapshot {
    match domain {
        AccountingDomain::Process => PROCESS.snapshot(),
        AccountingDomain::Tagged => TAGGED.snapshot(),
        AccountingDomain::Untagged => UNTAGGED.snapshot(),
    }
}

/// Resets the process Rust-source peak to its current live bytes, returning the previous peak.
/// This operation is not coordinated with allocations and must be serialized for reliable resets.
pub fn reset_process_rust_peak() -> u64 {
    let counters = &PROCESS.sources[AllocationSource::Rust as usize];
    counters
        .peak
        .swap(counters.live.load(Ordering::Relaxed), Ordering::Relaxed)
}

/// Runs `f` with allocations charged to process instrumentation, outside any query context.
/// Does not disable the allocator's prefixes or process accounting.
pub fn without_tracking<T>(f: impl FnOnce() -> T) -> T {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            let _ = SUPPRESSED.try_with(|s| s.set(self.0));
        }
    }
    let restore = Restore(SUPPRESSED.with(|s| s.replace(true)));
    let result = f();
    drop(restore);
    result
}

/// Future carrying an allocation context across task migration and cancellation.
pub struct Tracked<F> {
    future: ManuallyDrop<F>,
    context: Option<Context>,
}

impl<F> Tracked<F> {
    /// Captures the current context for `future`, including an untagged context.
    pub fn capture(future: F) -> Self {
        Self {
            future: ManuallyDrop::new(future),
            context: capture(),
        }
    }
}

impl<F: Future> Future for Tracked<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        // The future is structurally pinned; it is never moved through this projection.
        let this = unsafe { self.get_unchecked_mut() };
        let _scope = Scope::enter(this.context.clone());
        unsafe { Pin::new_unchecked(&mut *this.future) }.poll(cx)
    }
}

impl<F> Drop for Tracked<F> {
    fn drop(&mut self) {
        let _scope = Scope::enter(self.context.clone());
        // The pinned future is dropped in place and exactly once, including on cancellation.
        unsafe { ManuallyDrop::drop(&mut self.future) };
    }
}

/// Records newly allocated external backing in `source`, against `context` and process totals.
/// `None` identifies untagged work. Never charge an imported or retained allocation a second time.
pub fn add_external(context: Option<&Context>, source: AllocationSource, bytes: usize) {
    update(context.map(|c| c.0.as_ref()), source, bytes, 0, true);
}

/// Records final release of bytes charged with [`add_external`] to the same context and source.
/// The caller must balance allocations exactly; closing a logical reference is not a release.
pub fn release_external(context: Option<&Context>, source: AllocationSource, bytes: usize) {
    update(context.map(|c| c.0.as_ref()), source, bytes, 0, false);
}

/// A charge for one backing allocation owned by an external allocator.
///
/// Keep it until the actual backing allocation is released, not merely until a logical scope
/// closes. Never charge an imported or borrowed allocation already accounted by another allocator.
pub struct Charge {
    context: Option<Context>,
    source: AllocationSource,
    bytes: usize,
}

impl Charge {
    /// Charges `bytes` to `context` in `source`. `None` charges only the process aggregate.
    /// The caller must report a backing allocation exactly once and drop this charge exactly once.
    pub fn new(context: Option<Context>, source: AllocationSource, bytes: usize) -> Self {
        update(
            context.as_ref().map(|c| c.0.as_ref()),
            source,
            bytes,
            0,
            true,
        );
        Self {
            context,
            source,
            bytes,
        }
    }

    /// Charges a backing allocation to the currently active context.
    pub fn capture(source: AllocationSource, bytes: usize) -> Self {
        Self::new(capture(), source, bytes)
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        update(
            self.context.as_ref().map(|c| c.0.as_ref()),
            self.source,
            self.bytes,
            0,
            false,
        );
    }
}

/// Diagnostic allocator with an ownership prefix on every allocation.
///
/// Delegates to [`System`]. Prefixes preserve allocation origin across cross-thread frees and
/// reallocations. Enable only at the application or cdylib boundary, not in a general library.
pub struct TrackingAllocator;

#[derive(Clone, Copy)]
struct Header {
    owner: *const State,
    source: AllocationSource,
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        unsafe { allocate(layout, false) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        unsafe { allocate(layout, true) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        if let Some((backing, offset)) = extended(layout) {
            let base = unsafe { pointer.sub(offset) };
            let header = unsafe { base.cast::<Header>().read() };
            update(
                unsafe { header.owner.as_ref() },
                header.source,
                layout.size(),
                backing.size() - layout.size(),
                false,
            );
            unsafe { System.dealloc(pointer.sub(offset), backing) };
            if !header.owner.is_null() {
                unsafe { drop(Arc::from_raw(header.owner)) };
            }
        }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let Some((backing, offset)) = extended(layout) else {
            return ptr::null_mut();
        };
        let Ok(new_layout) = Layout::from_size_align(new_size, layout.align()) else {
            return ptr::null_mut();
        };
        let Some((new_backing, _)) = extended(new_layout) else {
            return ptr::null_mut();
        };
        let base = unsafe { pointer.sub(offset) };
        let header = unsafe { base.cast::<Header>().read() };
        let resized = unsafe { System.realloc(pointer.sub(offset), backing, new_backing.size()) };
        if resized.is_null() {
            return ptr::null_mut();
        }
        {
            let owner = unsafe { header.owner.as_ref() };
            // Reallocation retains the origin and charges only positive growth as allocation
            // volume.
            if new_size >= layout.size() {
                update(owner, header.source, new_size - layout.size(), 0, true);
            } else {
                update(owner, header.source, layout.size() - new_size, 0, false);
                for_state(owner, |state| {
                    state.total.allocations.fetch_add(1, Ordering::Relaxed);
                    state.sources[header.source as usize]
                        .allocations
                        .fetch_add(1, Ordering::Relaxed);
                });
            }
            let old_tracking = backing.size() - layout.size();
            let new_tracking = new_backing.size() - new_size;
            for_state(owner, |state| {
                for counters in [&state.total, &state.sources[header.source as usize]] {
                    if new_tracking >= old_tracking {
                        counters
                            .tracking
                            .fetch_add((new_tracking - old_tracking) as u64, Ordering::Relaxed);
                    } else {
                        counters
                            .tracking
                            .fetch_sub((old_tracking - new_tracking) as u64, Ordering::Relaxed);
                    }
                }
            });
        }
        unsafe { resized.add(offset) }
    }
}

fn active_pointer() -> *const State {
    ACTIVE
        .try_with(|active| {
            active
                .try_borrow()
                .ok()
                .and_then(|c| c.as_ref().map(|c| Arc::as_ptr(&c.0)))
                .unwrap_or(ptr::null())
        })
        .unwrap_or(ptr::null())
}

fn extended(layout: Layout) -> Option<(Layout, usize)> {
    Layout::new::<Header>()
        .extend(layout)
        .ok()
        .map(|(layout, offset)| (layout.pad_to_align(), offset))
}

unsafe fn allocate(layout: Layout, zeroed: bool) -> *mut u8 {
    let Some((backing, offset)) = extended(layout) else {
        return ptr::null_mut();
    };
    let base = if zeroed {
        unsafe { System.alloc_zeroed(backing) }
    } else {
        unsafe { System.alloc(backing) }
    };
    if base.is_null() {
        return base;
    }
    let source = if SUPPRESSED.try_with(Cell::get).unwrap_or(true) {
        AllocationSource::Instrumentation
    } else {
        AllocationSource::Rust
    };
    let owner = if source == AllocationSource::Rust {
        active_pointer()
    } else {
        ptr::null()
    };
    if !owner.is_null() {
        unsafe { Arc::increment_strong_count(owner) };
    }
    unsafe { base.cast::<Header>().write(Header { owner, source }) };
    update(
        unsafe { owner.as_ref() },
        source,
        layout.size(),
        backing.size() - layout.size(),
        true,
    );
    unsafe { base.add(offset) }
}

fn for_state(mut owner: Option<&State>, mut f: impl FnMut(&State)) {
    let tagged = owner.is_some();
    while let Some(state) = owner {
        f(state);
        owner = state.parent.as_ref().map(|parent| parent.0.as_ref());
    }
    f(if tagged { &TAGGED } else { &UNTAGGED });
    f(&PROCESS);
}

fn update(
    owner: Option<&State>,
    source: AllocationSource,
    bytes: usize,
    tracking: usize,
    add: bool,
) {
    for_state(owner, |state| {
        if add {
            state.add(source, bytes, tracking)
        } else {
            state.subtract(source, bytes, tracking)
        }
    });
}

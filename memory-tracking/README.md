# Experimental allocation context accounting

A context identifies a query or operation. Allocation source identifies its allocating owner:
Rust's global allocator, host Arrow, host ABI buffers, or other native memory. Sources are an
independent breakdown within each context; Rust is not the unit of query attribution.

This is a diagnostic prototype. It does not reserve memory, reject allocations, change OOM
handling, or promise RSS coverage.

## Attribution and lifetime

Enter a `Context` around synchronous work. Wrap futures with `Context::track` and propagate the
context at every thread/task boundary. The optional default-engine feature propagates it through
its executor's `block_on`, `spawn`, and `spawn_blocking`; arbitrary host executors and directly
spawned tasks need their own propagation. Each future restores the context for each poll and for
its destructor, including cancellation.

`TrackingAllocator` delegates to `System` and stores an origin reference in an allocation prefix.
Free/realloc uses that reference, independently of the freeing thread's active query. Outstanding
allocations keep the context alive after its original handle is released. Reallocations preserve
origin; successful growth contributes to cumulative allocated bytes. Hierarchical contexts charge
ancestors inclusively. Never sum a parent and its descendants.

There is no pointer map, tracking mutex, callback, logging, or metadata allocation in the allocator
hooks. The underlying system allocator still has its own synchronization. Thread-local scope setup
and context creation occur outside hooks. Context records, FFI handles, and explicitly suppressed
instrumentation are excluded from counters, although those allocations still carry prefixes.

Process-wide totals are enabled by default for metrics reporting, including untagged Rust work.
Disable `process-totals` only to isolate their cost in experiments. Context counters always remain
active. Multi-field snapshots can mix observation times while allocations are concurrent. Peaks
track simultaneously live bytes; a combined peak is not the sum of source peaks.

## Other allocation owners

`add_external`/`release_external` and `Charge` record backing ownership, not logical references.
The FFI exposes retained context handles, thread-local scopes, source/combined/process snapshots,
and external-owner accounting. `context-alloc-tracking` installs the allocator only at the FFI
library boundary; do not install a global allocator in a general-purpose library. When both FFI
tracking features are enabled, context tracking supersedes `peak_alloc`.

For host Arrow, attach a listener to a query's **producer** allocator and inherit it into child
allocators. Existing allocator live/peak accounting remains useful independently. Charge successful
allocation and final backing release; preserve the listener across any accounting-ownership
transfer. Do not attach the producer listener to foreign Rust-buffer imports: the Rust owner already
accounts those buffers. Retain/export is not allocation. Ownership transfers to an allocator with a
different listener require an explicit transfer policy.

For host ABI buffers, a scope's requested sizes do not establish the lifetime or retained size of a
shared pool. Measure logical demand separately, instrument the pool's backing allocator, or use an
explicit diagnostic owned-allocation backend and compare it against the uninstrumented owned
backend. Do not report scope close as a physical free for GC-managed or pooled backing.

C owners can capture the active context once, record a successful allocation, then release that
same charge and context on final free, including from another thread. Do not interpose on libc
allocations already routed through the Rust or Arrow trackers.

## Counters and coverage

- Live and peak bytes count requested sizes; `tracking_bytes` separately counts live prefix and
  alignment padding on counted Rust allocations, including trailing padding.
- Cumulative bytes include new allocations and positive realloc growth. Count includes successful
  reallocations, including shrinking or unchanged sizes.
- Host Arrow typically reports its rounded allocation-manager sizes. Reservations, allocator
  transfers, imported buffers, and underlying pool retention need separate interpretation.
- RSS, allocator usable size/bookkeeping, uninstrumented native libraries, direct `mmap`, native
  stacks, JVM heap, and backing retained by pools are outside these counters.
- Cache allocations stay attributed to their origin until freed. Attribution is not evidence that a
  query exclusively owns or can reclaim the memory. Shared caches require their own policy.
- Query retained bytes must be observed after work/cancellation has drained; immediate return can
  leave background cleanup in flight.

The unsafe prefix allocator is experimental. Miri tests use Tree Borrows, which supports accessing
an allocation prefix via the returned payload pointer. Stacked Borrows rejects narrowed payload
provenance in this allocator wrapper. No new unsafe test is ignored or run with alias checks off.

## Reproduce measurements

```sh
cargo test -p delta_kernel_memory_tracking
MIRIFLAGS=-Zmiri-tree-borrows cargo +nightly miri test -p delta_kernel_memory_tracking
TRACKING_ITERATIONS=200000 cargo bench -p delta_kernel_memory_tracking --bench allocation_tracking
TRACKING_ITERATIONS=200000 cargo bench -p delta_kernel_memory_tracking \
  --bench allocation_tracking --no-default-features
```

The benchmark warms each configuration once, rotates mode order across five reported repeats,
and tests independent contexts versus threads sharing a context. CSV timings are aggregate
nanoseconds per alloc/free pair, not individual request latency. `process` uses `peak_alloc`;
`header` adds context prefixes/counters; `header_external` adds **synthetic** Arrow and ABI charges,
not actual host allocations. The no-process-totals run still uses process counters in its `process`
comparator. Use `TRACKING_MODE` and `TRACKING_REPEATS` to select a mode and repetitions.

Recorded allocator results are in `measurements/` (Apple M5 Max, arm64, Darwin 25.5.0; release
build; first payload byte touched; no RSS measurement). For 64-byte allocations, medians in ns/pair:

| Threads / contexts | System | Process only | Context + process | Context without process |
| --- | ---: | ---: | ---: | ---: |
| 1 / independent | 9.11 | 10.54 | 22.79 | 15.95 |
| 4 / independent | 2.60 | 29.01 | 88.52 | 4.34 |
| 4 / shared | 3.04 | 29.22 | 136.18 | 106.17 |

These results demonstrate counter contention under allocator-heavy concurrency. They do not
predict query-level overhead. The separate runs and host scheduling add variability; use the raw
repeats, larger allocation sizes, and representative workloads before deciding on sharding or
sampling. Process totals remain enabled; their optional feature switch is an experimental control.

A local-table snapshot plus scan-metadata replay probe lives in `mem-test`:

```sh
python3 mem-test/generate-context-probe-table.py /tmp/context-memory-table
TRACKING_BENCH_ALLOCATOR=context cargo run --release -p mem-test --bin context-memory \
  --features context-alloc-tracking,context-process-totals -- file:///tmp/context-memory-table/ 10
```

Build separately with `TRACKING_BENCH_ALLOCATOR=system`, `process`, and `context`. The selection is
fixed at compile time so allocations made before `main` use the same allocator as later frees.
Each run warms once and reports live/peak/volume/count at scan completion, live after dropping
query objects, and retained bytes/final peak after the shared engine shuts down. The latter drains
executor cleanup. Compare elapsed time only between otherwise equivalent builds and inputs.

### Snapshot and log replay measurement

A Linux x86_64 release-build probe used a synthetic local Delta log with 100 commits and 10,000
active Add actions (one integer column, per-file min/max/null-count statistics), without a
checkpoint. Each iteration loads a new snapshot and consumes its complete scan-metadata stream;
no data files are read. The engine is shared between iterations, and the filesystem cache is warm.
Three runs per allocator mode each discard one warmup and record ten iterations; run order rotates.

| Installed allocator | Median elapsed | Observed range |
| --- | ---: | ---: |
| System | 39.00 ms | 37.96-46.83 ms |
| Process only | 45.70 ms | 40.07-50.03 ms |
| Context plus process | 59.06 ms | 56.34-71.55 ms |

Context tracking adds about 51% over System and 29% over process-only in this allocation-heavy
local replay probe. It is substantial diagnostic overhead, not an admission-path performance claim.
The context recorded about 252.1 MB cumulative requested bytes over 130,662 allocation/reallocation
operations per iteration. Median peak live requested bytes was 9.89 MB (6.71-15.42 MB across runs,
depending on background overlap); 43,663 requested bytes remained at scan completion. Every
measured context returned to zero live bytes after query objects were dropped and the shared
engine shut down. The raw CSVs preserve the immediate and drained observations.

Peak variation reflects concurrency and buffering: an operation's cumulative bytes are not its
required simultaneous capacity. This probe supports measuring estimates against per-query live
peaks; it does not establish an upfront reservation formula or realistic driver-wide coverage.

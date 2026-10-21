# Native allocation coverage audit

The tracker measures allocation requests through instrumented owners. It is not a libc interposer
or an inventory of the host process. `process_snapshot()` covers this allocator image plus explicit
external-owner records, not every library loaded into the process. No measured coverage percentage
is available.

This audit follows the normal-dependency graph of the default engine with Arrow/Parquet 60,
`zstd` 0.14.0 / `zstd-safe` 8.0.0 / `zstd-sys` 2.1.0+zstd.1.5.7, and `aws-lc-sys` 0.39.0.
Optional features, custom engines, and different dependency resolutions can change the graph.
The FFI must be built with `context-alloc-tracking` to install the context allocator; enabling
context propagation on the default-engine crate alone does not install it.

## Allocation coverage

| Allocation owner | Coverage | Evidence and limit |
| --- | --- | --- |
| Rust `Vec`, `String`, maps, `Box`, `Arc`, and ordinary dependency allocations | Counted when linked to the instrumented global allocator | Allocation prefixes retain origin through realloc and free. An arbitrary external library image is not automatically included. |
| Rust Arrow backing, ordinary Parquet input/output buffers, pure Rust codec buffers | Counted through Rust's allocator | Arrow `MutableBuffer` calls `std::alloc::alloc`, `alloc_zeroed`, `realloc`, and `dealloc`. Foreign backing adopted by Arrow keeps its external owner; importing is not a new allocation. |
| Zstd compression/decompression contexts, workspaces, and dictionaries allocated by C | **Not counted** | Parquet constructs `zstd::bulk` codecs; zstd-safe uses `ZSTD_createCCtx` / `ZSTD_createDCtx`. Their default custom-memory argument falls through to C `malloc`/`calloc`. Counting Rust compressed/decompressed buffers does not count those C allocations. |
| AWS-LC crypto internals | **Not counted** | Object storage and the rustls dependency graph include AWS-LC. `OPENSSL_malloc` calls C `malloc`. Rust request/response and wrapper allocations remain counted separately. Only code paths that actually allocate this state consume it. |
| Optional native TLS backends and operating-system services | **Not counted at their C/platform allocation boundary** | OpenSSL/platform TLS, certificate services, and libc name resolution can allocate outside Rust's global allocator. Exact owners depend on target, features, and exercised path. |
| Host/native memory explicitly recorded with `Charge` or external-owner FFI functions | Counted only for that owner | There is no automatic discovery. Record the successful backing request and debit the same origin only at final release. |
| Uninstrumented custom allocators, direct system allocator calls, or direct mappings | **Not counted automatically** | No direct `malloc`/`mmap` allocation sites were found in production `kernel`, `default-engine`, or `ffi` Rust sources. The confirmed C bypasses above are in dependencies. Host code may introduce others. |
| Tracking contexts, FFI context/scope handles, and suppressed metadata | Counted as untagged instrumentation | Kept outside query totals. Prefix and layout padding is reported separately as `tracking_bytes`. |
| Allocator size-class slack, arena metadata, retained free pages, stacks, and library/code mappings | **Not measured by requested-byte counters** | Neither query nor process snapshots are physical RSS measurements. Untouched allocations may not be resident, so requested bytes are not a guaranteed lower or upper bound on RSS. |

Rust-owned cached or pooled backing remains counted while its original allocation is live. Memory
retained below a matching global deallocation by the system allocator is outside the live count.
Do not treat these as the same kind of retention.

## Query attribution coverage

A counted allocation can still lack a reliable query label:

- The two default-engine executors wrap their `block_on`, `spawn`, and `spawn_blocking` work. This
  covers the wrapped future's polls/destruction and the wrapped blocking closure, not arbitrary
  tasks spawned by dependencies.
- `object_store::client::SpawnService::call` uses `self.runtime.spawn` to execute HTTP work and
  drain a response body on another runtime. When that connector is selected, it bypasses the
  default-engine wrapper.
- Hyper's `GaiResolver::call` uses `tokio::task::spawn_blocking` for name resolution. Its task is
  not wrapped by the default engine. Rust allocations in it can appear in untagged process totals;
  libc resolver allocations are a separate, genuinely uncounted C owner.
- Other library-owned connection tasks, custom executors, and host thread handoffs need their own
  propagation. A dependency's presence does not prove every optional task path is active.
- Engine setup before entering a context is untagged. Shared initialization/cache allocations
  made inside a context retain that origin; attribution does not imply exclusive ownership or
  that cancelling that query can reclaim them.

`Tagged` and `Untagged` partition instrumented live requested bytes at quiescence. Untagged bytes
are not an estimate of uninstrumented native allocations. Concurrent multi-counter reads can mix
observation times; independent peaks cannot be added together.

## Source evidence

- [Arrow MutableBuffer](https://github.com/apache/arrow-rs/blob/60.0.0/arrow-buffer/src/buffer/mutable.rs)
- [Parquet Zstd codec](https://github.com/apache/arrow-rs/blob/60.0.0/parquet/src/compression.rs)
- [zstd-safe context construction](https://docs.rs/crate/zstd-safe/8.0.0/source/src/lib.rs)
- [Zstd context construction](https://github.com/facebook/zstd/blob/v1.5.7/lib/decompress/zstd_decompress.c)
  and [allocation helpers](https://github.com/facebook/zstd/blob/v1.5.7/lib/common/allocations.h)
- [AWS-LC native allocation source](https://docs.rs/crate/aws-lc-sys/0.39.0/source/aws-lc/crypto/mem.c)
- [Object-store HTTP task handoff](https://docs.rs/crate/object_store/0.14.2/source/src/client/http/spawn.rs)
- [Hyper-util DNS task handoff](https://docs.rs/crate/hyper-util/0.1.20/source/src/client/legacy/connect/dns.rs)

The Rust buffer and native dependency paths above were checked in resolved source, not inferred
from crate names. This is a static ownership audit; it does not quantify native allocations from
Zstd, crypto, platform services, or a host's other libraries. Closing those gaps requires
instrumenting their allocator/owner APIs or separately measuring them. A global malloc interposer
would need an explicit double-counting policy for requests already recorded by Rust and hosts.

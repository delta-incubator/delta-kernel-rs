# Experimental allocation recovery

`alloc-recovery` enables a requested-byte budget and recovery at snapshot FFI boundaries.
It implies `alloc-tracking`. It requires `nightly-2026-09-02` and `panic=unwind`; tracking alone
uses stable Rust. `--all-features` includes recovery and therefore also requires nightly.
Cargo features do not select a compiler, so use the build wrapper:

```sh
# Stable, including allocation telemetry.
./ffi/build-native.sh

# Pinned nightly, including experimental recovery.
KERNEL_FFI_FEATURES=default-engine-rustls,alloc-recovery,vendored-protoc ./ffi/build-native.sh
```

Call `configure_native_allocation_limit(limit_bytes, recovery_bytes)` once during single-threaded
startup, before creating engines or starting native work. Both values must be positive, or both zero
to keep enforcement off. A successful call fixes the policy for the lifetime of the loaded library;
later calls are rejected, including attempts to disable enforcement. Failed calls do not consume
initialization. Configuration returns false if unsupported, invalid, already initialized, or if the
absolute ceiling would be below existing charged bytes. Allocations before initialization are
unbounded. Startup ordering is the caller's responsibility; no allocator-side reconfiguration gate
is installed. Zero does not remove the hook.

The allocator reserves requested bytes atomically before calling `System`. Protected work uses
`limit_bytes`; recovery and unguarded housekeeping use the absolute ceiling
`limit_bytes + recovery_bytes`. Reallocation conservatively reserves the entire replacement
while the original allocation remains charged. Failed reservations and underlying null returns
roll back the reservation. Deallocation releases it. Concurrent callers share one budget.

The allocator never unwinds. An infallible allocation receiving null invokes Rust's allocation
error handler, which calls the installed nightly hook. On a prepared thread, the hook resumes
unwinding with a preallocated typed payload, without invoking the ordinary panic hook. Default
engine executor boundaries transport that failure to the originating call. Rust catches it before
returning across C and reports `AllocationLimitExceeded` or `AllocationFailed` through the engine
error allocator. A caller can release query resources and retry a later operation after recovery.
Fallible allocation APIs such as `try_reserve` return their ordinary error without invoking the hook.

## Scope and limits

Protected entry points are `get_snapshot_builder`, `get_snapshot_builder_from`, and
`snapshot_builder_build`. Task context follows both provided Tokio executors, including blocking
work. Other entry points, arbitrary third-party thread pools, and foreign-language callbacks are
not a general recovery boundary. No unwind may cross a non-unwinding foreign ABI frame.
Unrelated panics retain fail-stop behavior at the protected FFI boundary.

This is a bound on requested Rust allocation sizes in this native library, including in-flight
reservations. It is not a process/container/RSS limit: allocator metadata, retained pages,
foreign allocators, mappings, stacks, and host-runtime allocations need separate headroom.
Recovery capacity is bounded, not physically reserved; it can still fail at the system allocator.
Failure while unwinding, exhaustion outside a prepared boundary, and kernel OOM kills cannot be
recovered by this mechanism. Shared failure state also cannot make detached work finish before its
originating call returns. This prototype is not a guarantee of recovery from arbitrary system OOM.

## Verification

```sh
cargo +nightly-2026-09-02 test --locked -p delta_kernel_ffi --lib \
  --features alloc-recovery,test-ffi,vendored-protoc \
  recovery_scenarios_in_subprocesses -- --nocapture
```

The subprocess suite covers cap refusal, underlying-null injection, fallible allocation, worker
propagation, concurrent failures, global cap contention, allocation accounting, and configuration
validation. Separate child processes verify deliberate aborts outside protection and on double failure.
`test_fail_native_allocation_after` requires `test-ffi` as well as recovery, is thread scoped, and
simulates null without exhausting the machine. It is not a production control.

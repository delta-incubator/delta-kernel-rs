#!/usr/bin/env bash
set -euo pipefail

# Select nightly only for the allocation-recovery experiment; tracking alone uses stable.
features="${KERNEL_FFI_FEATURES:-default-engine-rustls,declarative-plans,delta-kernel-unity-catalog,alloc-tracking,geo-type-in-dev,tracing,vendored-protoc}"
for arg in "$@"; do
  case "$arg" in
    --features|--features=*|-F|-F*|--all-features)
      echo "Select features with KERNEL_FFI_FEATURES so the compiler matches them." >&2
      exit 2
      ;;
  esac
done
toolchain=stable
case ",$features," in
  *,alloc-recovery,*) toolchain=nightly-2026-09-02 ;;
esac
cd "$(dirname "$0")/.."
export DELTA_KERNEL_GIT_SHA="$(git rev-parse HEAD)"
exec cargo "+$toolchain" build --locked -p delta_kernel_ffi --features "$features" "$@"

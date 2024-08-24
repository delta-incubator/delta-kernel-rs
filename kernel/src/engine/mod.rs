//! Provides engine implementation that implement the required traits. These engines can optionally
//! be built into the kernel by setting the `default-engine`, `sync-engine` or `wasm-engine` feature flags. See the
//! related modules for more information.

#[cfg(feature = "arrow-conversion")]
pub(crate) mod arrow_conversion;

#[cfg(feature = "arrow-expression")]
pub mod arrow_expression;

#[cfg(any(feature = "default-engine", feature = "sync-engine", feature = "wasm-engine"))]
pub mod arrow_data;

#[cfg(any(feature = "default-engine", feature = "sync-engine", feature = "wasm-engine"))]
pub(crate) mod arrow_get_data;

#[cfg(any(feature = "default-engine", feature = "sync-engine", feature = "wasm-engine"))]
pub(crate) mod arrow_utils;

#[cfg(feature = "default-engine")]
pub mod default;

#[cfg(feature = "sync-engine")]
pub mod sync;

#[cfg(feature = "wasm-engine")]
pub mod wasm;

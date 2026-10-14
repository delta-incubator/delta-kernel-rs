//! Reader-side protocol-feature integration tests.

mod alter_table;
mod cdf;
mod clustering_e2e;
mod column_mapping;
mod dv;
mod iceberg_compat;
#[cfg(feature = "internal-api")]
mod iceberg_schema_evolution;
mod interval;
mod maintenance_ops;
pub(crate) mod row_tracking;

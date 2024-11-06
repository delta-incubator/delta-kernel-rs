//! This module encapsulates the state of a scan

use std::collections::HashMap;

use crate::{
    actions::visitors::visit_deletion_vector_at,
    engine_data::{GetData, TypedGetData},
    features::ColumnMappingMode,
    scan::{
        log_replay::{self, SCAN_ROW_SCHEMA},
        state::{DvInfo, Stats},
    },
    schema::SchemaRef,
    DataVisitor, DeltaResult, EngineData, Error,
};
use serde::{Deserialize, Serialize};
use tracing::warn;

/// State that doesn't change between scans
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct GlobalScanState {
    pub table_root: String,
    pub partition_columns: Vec<String>,
    pub logical_schema: SchemaRef,
    pub read_schema: SchemaRef,
    pub column_mapping_mode: ColumnMappingMode,
}

pub(crate) type ScanCallback<T> = fn(
    context: &mut T,
    path: &str,
    size: i64,
    stats: Option<Stats>,
    dv_info: DvInfo,
    partition_values: HashMap<String, String>,
);

/// Request that the kernel call a callback on each valid file that needs to be read for the
/// scan.
///
/// The arguments to the callback are:
/// * `context`: an `&mut context` argument. this can be anything that engine needs to pass through to each call
/// * `path`: a `&str` which is the path to the file
/// * `size`: an `i64` which is the size of the file
/// * `dv_info`: a [`DvInfo`] struct, which allows getting the selection vector for this file
/// * `partition_values`: a `HashMap<String, String>` which are partition values
///
/// ## Context
/// A note on the `context`. This can be any value the engine wants. This function takes ownership
/// of the passed arg, but then returns it, so the engine can repeatedly call `visit_scan_files`
/// with the same context.
///
/// ## Example
/// ```ignore
/// let mut context = [my context];
/// for res in scan_data { // scan data from scan.get_scan_data()
///     let (data, vector) = res?;
///     context = delta_kernel::scan::state::visit_scan_files(
///        data.as_ref(),
///        vector,
///        context,
///        my_callback,
///     )?;
/// }
/// ```
pub(crate) fn visit_scan_files<T>(
    data: &dyn EngineData,
    selection_vector: &[bool],
    context: T,
    callback: ScanCallback<T>,
) -> DeltaResult<T> {
    let mut visitor = ScanFileVisitor {
        callback,
        selection_vector,
        context,
    };
    data.extract(log_replay::SCAN_ROW_SCHEMA.clone(), &mut visitor)?;
    Ok(visitor.context)
}

// add some visitor magic for engines
struct ScanFileVisitor<'a, T> {
    callback: ScanCallback<T>,
    selection_vector: &'a [bool],
    context: T,
}

impl<T> DataVisitor for ScanFileVisitor<'_, T> {
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for row_index in 0..row_count {
            if !self.selection_vector[row_index] {
                // skip skipped rows
                continue;
            }
            // Since path column is required, use it to detect presence of an Add action
            if let Some(path) = getters[0].get_opt(row_index, "scanFile.path")? {
                let size = getters[1].get(row_index, "scanFile.size")?;
                let stats: Option<String> = getters[3].get_opt(row_index, "scanFile.stats")?;
                let stats: Option<Stats> =
                    stats.and_then(|json| match serde_json::from_str(json.as_str()) {
                        Ok(stats) => Some(stats),
                        Err(e) => {
                            warn!("Invalid stats string in Add file {json}: {}", e);
                            None
                        }
                    });

                let dv_index = SCAN_ROW_SCHEMA
                    .index_of("deletionVector")
                    .ok_or_else(|| Error::missing_column("deletionVector"))?;
                let deletion_vector = visit_deletion_vector_at(row_index, &getters[dv_index..])?;
                let dv_info = DvInfo { deletion_vector };
                let partition_values =
                    getters[9].get(row_index, "scanFile.fileConstantValues.partitionValues")?;
                (self.callback)(
                    &mut self.context,
                    path,
                    size,
                    stats,
                    dv_info,
                    partition_values,
                )
            }
        }
        Ok(())
    }
}

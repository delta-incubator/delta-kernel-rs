//! This module encapsulates the state of a scan

use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
};

use crate::{
    actions::visitors::visit_deletion_vector_at,
    engine_data::{GetData, TypedGetData},
    features::ColumnMappingMode,
    scan::{
        log_replay::SCAN_ROW_SCHEMA,
        state::{DvInfo, Stats},
    },
    schema::{DataType, MapType, Schema, SchemaRef, StructField, StructType},
    DataVisitor, DeltaResult, EngineData,
};
use crate::{
    expressions::{column_expr, Expression},
    Version,
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

#[derive(Debug, Clone)]
pub(crate) enum ScanFileType {
    Add,
    Remove,
    Cdc,
}
#[derive(Debug)]
pub(crate) struct ScanFile {
    pub tpe: ScanFileType,
    pub path: String,
    pub size: i64,
    pub dv_info: DvInfo,
    pub partition_values: HashMap<String, String>,
    pub commit_version: i64,
    pub timestamp: i64,
}

pub(crate) type ScanCallback<T> = fn(context: &mut T, scan_file: ScanFile);

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

    data.extract(TABLE_CHANGES_SCAN_ROW_SCHEMA.clone(), &mut visitor)?;
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
            let timestamp = getters[21].get(row_index, "scanFile.timestamp")?;
            let commit_version = getters[22].get(row_index, "scanFile.commit_version")?;

            // Since path column is required, use it to detect presence of an Add action
            if let Some(path) = getters[0].get_opt(row_index, "scanFile.add.path")? {
                let size = getters[1].get(row_index, "scanFile.add.size")?;
                let dv_index = 4;
                let deletion_vector = visit_deletion_vector_at(row_index, &getters[dv_index..])?;
                let dv_info = DvInfo { deletion_vector };
                let partition_values =
                    getters[9].get(row_index, "scanFile.add.fileConstantValues.partitionValues")?;
                let scan_file = ScanFile {
                    tpe: ScanFileType::Add,
                    path,
                    size,
                    dv_info,
                    partition_values,
                    commit_version,
                    timestamp,
                };
                (self.callback)(&mut self.context, scan_file)
            } else if let Some(path) = getters[10].get_opt(row_index, "scanFile.remove.path")? {
                let size = getters[11].get(row_index, "scanFile.add.size")?;
                let dv_index = 12;
                let deletion_vector = visit_deletion_vector_at(row_index, &getters[dv_index..])?;
                let dv_info = DvInfo { deletion_vector };
                let partition_values = getters[17].get(
                    row_index,
                    "scanFile.remove.fileConstantValues.partitionValues",
                )?;
                let scan_file = ScanFile {
                    tpe: ScanFileType::Remove,
                    path,
                    size,
                    dv_info,
                    partition_values,
                    commit_version,
                    timestamp,
                };
                (self.callback)(&mut self.context, scan_file)
            } else if let Some(path) = getters[18].get_opt(row_index, "scanFile.cdc.path")? {
                let size = getters[19].get(row_index, "scanFile.add.size")?;
                let partition_values = getters[20].get(
                    row_index,
                    "scanFile.remove.fileConstantValues.partitionValues",
                )?;
                let scan_file = ScanFile {
                    tpe: ScanFileType::Cdc,
                    path,
                    size,
                    dv_info: DvInfo {
                        deletion_vector: None,
                    },
                    partition_values,
                    commit_version,
                    timestamp,
                };

                (self.callback)(&mut self.context, scan_file)
            } else {
                println!("Didn't find anything");
            }
        }
        Ok(())
    }
}

/// Get the schema that scan rows (from [`TableChanges::scan_data`]) will be returned with.
///
/// It is:
/// ```ignored
/// {
///    path: string,
///    size: long,
///    modificationTime: long,
///    stats: string,
///    deletionVector: {
///      storageType: string,
///      pathOrInlineDv: string,
///      offset: int,
///      sizeInBytes: int,
///      cardinality: long,
///    },
///    fileConstantValues: {
///      partitionValues: map<string, string>
///    }
/// }
/// ```
pub(crate) fn scan_row_schema() -> Schema {
    TABLE_CHANGES_SCAN_ROW_SCHEMA.as_ref().clone()
}

// TODO: Should unify with ADD SCAN_ROW_SCHEMA
static TABLE_CHANGES_LOG_ADD_SCHEMA: LazyLock<StructField> = LazyLock::new(|| {
    StructField::new(
        "add",
        StructType::new([
            StructField::new("path", DataType::STRING, true),
            StructField::new("size", DataType::LONG, true),
            StructField::new("modificationTime", DataType::LONG, true),
            StructField::new("stats", DataType::STRING, true),
            StructField::new(
                "deletionVector",
                StructType::new([
                    StructField::new("storageType", DataType::STRING, true),
                    StructField::new("pathOrInlineDv", DataType::STRING, true),
                    StructField::new("offset", DataType::INTEGER, true),
                    StructField::new("sizeInBytes", DataType::INTEGER, true),
                    StructField::new("cardinality", DataType::LONG, true),
                ]),
                true,
            ),
            StructField::new(
                "fileConstantValues",
                StructType::new([StructField::new(
                    "partitionValues",
                    MapType::new(DataType::STRING, DataType::STRING, true),
                    true,
                )]),
                true,
            ),
        ]),
        true,
    )
});

static TABLE_CHANGES_LOG_REMOVE_SCHEMA: LazyLock<StructField> = LazyLock::new(|| {
    StructField::new(
        "remove",
        StructType::new([
            StructField::new("path", DataType::STRING, true),
            StructField::new("size", DataType::LONG, true),
            StructField::new(
                "deletionVector",
                StructType::new([
                    StructField::new("storageType", DataType::STRING, true),
                    StructField::new("pathOrInlineDv", DataType::STRING, true),
                    StructField::new("offset", DataType::INTEGER, true),
                    StructField::new("sizeInBytes", DataType::INTEGER, true),
                    StructField::new("cardinality", DataType::LONG, true),
                ]),
                true,
            ),
            StructField::new(
                "fileConstantValues",
                StructType::new([StructField::new(
                    "partitionValues",
                    MapType::new(DataType::STRING, DataType::STRING, true),
                    true,
                )]),
                true,
            ),
        ]),
        true,
    )
});

static TABLE_CHANGES_LOG_CDC_SCHEMA: LazyLock<StructField> = LazyLock::new(|| {
    StructField::new(
        "cdc",
        StructType::new([
            StructField::new("path", DataType::STRING, true),
            StructField::new("size", DataType::LONG, true),
            StructField::new(
                "fileConstantValues",
                StructType::new([StructField::new(
                    "partitionValues",
                    MapType::new(DataType::STRING, DataType::STRING, true),
                    true,
                )]),
                true,
            ),
        ]),
        true,
    )
});

static TABLE_CHANGES_TIMESTAMP: LazyLock<StructField> =
    LazyLock::new(|| StructField::new("timestamp", DataType::LONG, true));

static TABLE_CHANGES_COMMIT_VERSION: LazyLock<StructField> =
    LazyLock::new(|| StructField::new("commit_version", DataType::LONG, true));

// NB: If you update this schema, ensure you update the comment describing it in the doc comment
// for `scan_row_schema` in table_changes/mod.rs! You'll also need to update ScanFileVisitor as the
// indexes will be off
pub(crate) static TABLE_CHANGES_SCAN_ROW_SCHEMA: LazyLock<Arc<StructType>> = LazyLock::new(|| {
    // Note that fields projected out of a nullable struct must be nullable
    Arc::new(StructType::new([
        // TODO: Look at log add schema, then project_as_struct, and use it to construct a schema
        TABLE_CHANGES_LOG_ADD_SCHEMA.clone(),
        TABLE_CHANGES_LOG_REMOVE_SCHEMA.clone(),
        TABLE_CHANGES_LOG_CDC_SCHEMA.clone(),
        TABLE_CHANGES_TIMESTAMP.clone(),
        TABLE_CHANGES_COMMIT_VERSION.clone(),
    ]))
});

pub(crate) static SCAN_ROW_DATATYPE: LazyLock<DataType> =
    LazyLock::new(|| SCAN_ROW_SCHEMA.clone().into());

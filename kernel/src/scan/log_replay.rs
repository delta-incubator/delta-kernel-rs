use std::clone::Clone;
use std::collections::HashSet;
use std::sync::{Arc, LazyLock};

use tracing::debug;

use super::data_skipping::DataSkippingFilter;
use super::ScanData;
use crate::actions::get_log_add_schema;
use crate::engine_data::{GetData, RowVisitorBase, TypedGetData};
use crate::expressions::{column_expr, column_name, ColumnName, Expression, ExpressionRef};
use crate::scan::DeletionVectorDescriptor;
use crate::schema::{ColumnNamesAndTypes, DataType, MapType, SchemaRef, StructField, StructType};
use crate::utils::require;
use crate::{DeltaResult, Engine, EngineData, Error, ExpressionHandler, RowVisitor as _};

/// The subset of file action fields that uniquely identifies it in the log, used for deduplication
/// of adds and removes during log replay.
#[derive(Debug, Hash, Eq, PartialEq)]
struct FileActionKey {
    path: String,
    dv_unique_id: Option<String>,
}
impl FileActionKey {
    fn new(path: impl Into<String>, dv_unique_id: Option<String>) -> Self {
        let path = path.into();
        Self { path, dv_unique_id }
    }
}

struct LogReplayScanner {
    filter: Option<DataSkippingFilter>,

    /// A set of (data file path, dv_unique_id) pairs that have been seen thus
    /// far in the log. This is used to filter out files with Remove actions as
    /// well as duplicate entries in the log.
    seen: HashSet<FileActionKey>,
}

struct AddRemoveDedupVisitor<'seen> {
    seen: &'seen mut HashSet<FileActionKey>,
    selection_vector: Vec<bool>,
    is_log_batch: bool,
}
impl<'seen> AddRemoveDedupVisitor<'seen> {
    fn filter_seen(&mut self, path: &str, dv_unique_id: Option<String>) -> bool {
        // Note: each (add.path + add.dv_unique_id()) pair has a
        // unique Add + Remove pair in the log. For example:
        // https://github.com/delta-io/delta/blob/master/spark/src/test/resources/delta/table-with-dv-large/_delta_log/00000000000000000001.json

        let key = FileActionKey::new(path, dv_unique_id);
        if self.seen.contains(&key) {
            debug!(
                "Ignoring duplicate ({}, {:?}) in scan, is log {}",
                key.path, key.dv_unique_id, self.is_log_batch
            );
            false
        } else {
            debug!(
                "Including ({}, {:?}) in scan, is log {}",
                key.path, key.dv_unique_id, self.is_log_batch
            );
            if self.is_log_batch {
                // Remember file actions from this batch so we can ignore duplicates as we process
                // batches from older commit and/or checkpoint files. We don't track checkpoint
                // batches because they are already the oldest actions and never replace anything.
                self.seen.insert(key);
            }
            true
        }
    }

    fn filter_row<'a>(&mut self, i: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<bool> {
        // Add will have a path at index 0 if it is valid; otherwise, if it is a log batch, we
        // may have a remove with a path at index 4.
        let (path, getters, keep) = if let Some(path) = getters[0].get_opt(i, "add.path")? {
            (path, &getters[1..4], true)
        } else if !self.is_log_batch {
            return Ok(false);
        } else if let Some(path) = getters[4].get_opt(i, "remove.path")? {
            (path, &getters[5..8], false)
        } else {
            return Ok(false);
        };

        let dv_unique_id = match getters[0].get_opt(i, "deletionVector.storageType")? {
            Some(storage_type) => Some(DeletionVectorDescriptor::unique_id_from_parts(
                storage_type,
                getters[1].get(i, "deletionVector.pathOrInlineDv")?,
                getters[2].get_opt(i, "deletionVector.offset")?,
            )),
            None => None,
        };
        // Process both adds and removes, but only keep the surviving adds
        Ok(self.filter_seen(path, dv_unique_id) && keep)
    }
}
impl<'seen> RowVisitorBase for AddRemoveDedupVisitor<'seen> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            use DataType::{INTEGER, STRING};
            let types_and_names = vec![
                (STRING, column_name!("add.path")),
                (STRING, column_name!("add.deletionVector.storageType")),
                (STRING, column_name!("add.deletionVector.pathOrInlineDv")),
                (INTEGER, column_name!("add.deletionVector.offset")),
                (STRING, column_name!("remove.path")),
                (STRING, column_name!("remove.deletionVector.storageType")),
                (STRING, column_name!("remove.deletionVector.pathOrInlineDv")),
                (INTEGER, column_name!("remove.deletionVector.offset")),
            ];
            let (types, names) = types_and_names.into_iter().unzip();
            (names, types).into()
        });
        let (names, types) = NAMES_AND_TYPES.as_ref();
        if self.is_log_batch {
            (names, types)
        } else {
            // All checkpoint actions are already reconciled and Remove actions in checkpoint files
            // only serve as tombstones for vacuum jobs. So we only need to examine the adds here.
            (&names[..4], &types[..4])
        }
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        let expected_getters = if self.is_log_batch { 8 } else { 4 };
        require!(
            getters.len() == expected_getters,
            Error::InternalError(format!(
                "Wrong number of AddRemoveDedupVisitor getters: {}",
                getters.len()
            ))
        );
        for i in 0..row_count {
            if self.selection_vector[i] {
                self.selection_vector[i] = self.filter_row(i, getters)?;
            }
        }
        Ok(())
    }
}

// NB: If you update this schema, ensure you update the comment describing it in the doc comment
// for `scan_row_schema` in scan/mod.rs! You'll also need to update ScanFileVisitor as the
// indexes will be off
pub(crate) static SCAN_ROW_SCHEMA: LazyLock<Arc<StructType>> = LazyLock::new(|| {
    // Note that fields projected out of a nullable struct must be nullable
    Arc::new(StructType::new([
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
    ]))
});

static SCAN_ROW_DATATYPE: LazyLock<DataType> = LazyLock::new(|| SCAN_ROW_SCHEMA.clone().into());

impl LogReplayScanner {
    /// Create a new [`LogReplayScanner`] instance
    fn new(
        engine: &dyn Engine,
        table_schema: &SchemaRef,
        predicate: Option<ExpressionRef>,
    ) -> Self {
        Self {
            filter: DataSkippingFilter::new(engine, table_schema, predicate),
            seen: Default::default(),
        }
    }

    fn get_add_transform_expr(&self) -> Expression {
        Expression::Struct(vec![
            column_expr!("add.path"),
            column_expr!("add.size"),
            column_expr!("add.modificationTime"),
            column_expr!("add.stats"),
            column_expr!("add.deletionVector"),
            Expression::Struct(vec![column_expr!("add.partitionValues")]),
        ])
    }

    fn process_scan_batch(
        &mut self,
        expression_handler: &dyn ExpressionHandler,
        actions: &dyn EngineData,
        is_log_batch: bool,
    ) -> DeltaResult<ScanData> {
        // apply data skipping to get back a selection vector for actions that passed skipping
        // note: None implies all files passed data skipping.
        let filter_vector = self
            .filter
            .as_ref()
            .map(|filter| filter.apply(actions))
            .transpose()?;

        // we start our selection vector based on what was filtered. we will add to this vector
        // below if a file has been removed
        let selection_vector = match filter_vector {
            Some(ref filter_vector) => filter_vector.clone(),
            None => vec![true; actions.len()],
        };

        assert_eq!(selection_vector.len(), actions.len());

        let mut visitor = AddRemoveDedupVisitor {
            seen: &mut self.seen,
            selection_vector,
            is_log_batch,
        };
        visitor.visit_rows_of(actions)?;
        let selection_vector = visitor.selection_vector;

        // TODO: Teach expression eval to respect the selection vector we just computed so carefully!
        let result = expression_handler
            .get_evaluator(
                get_log_add_schema().clone(),
                self.get_add_transform_expr(),
                SCAN_ROW_DATATYPE.clone(),
            )
            .evaluate(actions)?;
        Ok((result, selection_vector))
    }
}

/// Given an iterator of (engine_data, bool) tuples and a predicate, returns an iterator of
/// `(engine_data, selection_vec)`. Each row that is selected in the returned `engine_data` _must_
/// be processed to complete the scan. Non-selected rows _must_ be ignored. The boolean flag
/// indicates whether the record batch is a log or checkpoint batch.
pub fn scan_action_iter(
    engine: &dyn Engine,
    action_iter: impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>>,
    table_schema: &SchemaRef,
    predicate: Option<ExpressionRef>,
) -> impl Iterator<Item = DeltaResult<ScanData>> {
    let mut log_scanner = LogReplayScanner::new(engine, table_schema, predicate);
    let expression_handler = engine.get_expression_handler();
    action_iter
        .map(move |action_res| {
            action_res.and_then(|(batch, is_log_batch)| {
                log_scanner.process_scan_batch(
                    expression_handler.as_ref(),
                    batch.as_ref(),
                    is_log_batch,
                )
            })
        })
        .filter(|action_res| {
            match action_res {
                Ok((_, sel_vec)) => {
                    // don't bother returning it if everything is filtered out
                    sel_vec.contains(&true)
                }
                Err(_) => true, // just pass through errors
            }
        })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::scan::{
        state::{DvInfo, Stats},
        test_utils::{add_batch_simple, add_batch_with_remove, run_with_validate_callback},
    };

    // dv-info is more complex to validate, we validate that works in the test for visit_scan_files
    // in state.rs
    fn validate_simple(
        _: &mut (),
        path: &str,
        size: i64,
        stats: Option<Stats>,
        _: DvInfo,
        part_vals: HashMap<String, String>,
    ) {
        assert_eq!(
            path,
            "part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet"
        );
        assert_eq!(size, 635);
        assert!(stats.is_some());
        assert_eq!(stats.as_ref().unwrap().num_records, 10);
        assert_eq!(part_vals.get("date"), Some(&"2017-12-10".to_string()));
        assert_eq!(part_vals.get("non-existent"), None);
    }

    #[test]
    fn test_scan_action_iter() {
        run_with_validate_callback(
            vec![add_batch_simple()],
            &[true, false],
            (),
            validate_simple,
        );
    }

    #[test]
    fn test_scan_action_iter_with_remove() {
        run_with_validate_callback(
            vec![add_batch_with_remove()],
            &[false, false, true, false],
            (),
            validate_simple,
        );
    }
}

use std::{collections::HashMap, sync::Arc};

use itertools::Itertools;
use tracing::debug;

use crate::{
    actions::{
        deletion_vector::split_vector, get_log_add_schema, get_log_schema, ADD_NAME, REMOVE_NAME,
    },
    scan::{
        get_state_info,
        log_replay::scan_action_iter,
        state::{self, DvInfo, GlobalScanState, Stats},
        transform_to_logical_internal, ColumnType, ScanData, ScanResult,
    },
    schema::{SchemaRef, StructType},
    DeltaResult, Engine, EngineData, ExpressionRef, FileMeta,
};

use super::TableChanges;

/// Builder to scan a snapshot of a table.
pub struct TableChangesScanBuilder {
    table_changes: Arc<TableChanges>,
    schema: Option<SchemaRef>,
    predicate: Option<ExpressionRef>,
}

impl TableChangesScanBuilder {
    /// Create a new [`ScanBuilder`] instance.
    pub fn new(table_changes: impl Into<Arc<TableChanges>>) -> Self {
        Self {
            table_changes: table_changes.into(),
            schema: None,
            predicate: None,
        }
    }

    /// Provide [`Schema`] for columns to select from the [`Snapshot`].
    ///
    /// A table with columns `[a, b, c]` could have a scan which reads only the first
    /// two columns by using the schema `[a, b]`.
    ///
    /// [`Schema`]: crate::schema::Schema
    /// [`Snapshot`]: crate::snapshot::Snapshot
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Optionally provide a [`SchemaRef`] for columns to select from the [`Snapshot`]. See
    /// [`ScanBuilder::with_schema`] for details. If `schema_opt` is `None` this is a no-op.
    pub fn with_schema_opt(self, schema_opt: Option<SchemaRef>) -> Self {
        match schema_opt {
            Some(schema) => self.with_schema(schema),
            None => self,
        }
    }

    /// Optionally provide an expression to filter rows. For example, using the predicate `x <
    /// 4` to return a subset of the rows in the scan which satisfy the filter. If `predicate_opt`
    /// is `None`, this is a no-op.
    ///
    /// NOTE: The filtering is best-effort and can produce false positives (rows that should should
    /// have been filtered out but were kept).
    pub fn with_predicate(mut self, predicate: impl Into<Option<ExpressionRef>>) -> Self {
        self.predicate = predicate.into();
        self
    }

    /// Build the [`Scan`].
    ///
    /// This does not scan the table at this point, but does do some work to ensure that the
    /// provided schema make sense, and to prepare some metadata that the scan will need.  The
    /// [`Scan`] type itself can be used to fetch the files and associated metadata required to
    /// perform actual data reads.
    pub fn build(self) -> DeltaResult<TableChangesScan> {
        // if no schema is provided, use snapshot's entire schema (e.g. SELECT *)
        let logical_schema = self
            .schema
            .unwrap_or_else(|| self.table_changes.schema.clone().into());
        let (all_fields, read_fields, have_partition_cols) = get_state_info(
            logical_schema.as_ref(),
            &self.table_changes.metadata.partition_columns,
            self.table_changes.column_mapping_mode,
        )?;
        let physical_schema = Arc::new(StructType::new(read_fields));
        Ok(TableChangesScan {
            table_changes: self.table_changes,
            logical_schema,
            physical_schema,
            predicate: self.predicate,
            all_fields,
            have_partition_cols,
        })
    }
}
pub struct TableChangesScan {
    table_changes: Arc<TableChanges>,
    logical_schema: SchemaRef,
    physical_schema: SchemaRef,
    predicate: Option<ExpressionRef>,
    all_fields: Vec<ColumnType>,
    have_partition_cols: bool,
}

impl TableChangesScan {
    /// Get a shared reference to the [`Schema`] of the scan.
    ///
    /// [`Schema`]: crate::schema::Schema
    pub fn schema(&self) -> &SchemaRef {
        &self.logical_schema
    }

    /// Get the predicate [`Expression`] of the scan.
    pub fn predicate(&self) -> Option<ExpressionRef> {
        self.predicate.clone()
    }

    /// Get an iterator of [`EngineData`]s that should be included in scan for a query. This handles
    /// log-replay, reconciling Add and Remove actions, and applying data skipping (if
    /// possible). Each item in the returned iterator is a tuple of:
    /// - `Box<dyn EngineData>`: Data in engine format, where each row represents a file to be
    ///   scanned. The schema for each row can be obtained by calling [`scan_row_schema`].
    /// - `Vec<bool>`: A selection vector. If a row is at index `i` and this vector is `false` at
    ///   index `i`, then that row should *not* be processed (i.e. it is filtered out). If the vector
    ///   is `true` at index `i` the row *should* be processed. If the selector vector is *shorter*
    ///   than the number of rows returned, missing elements are considered `true`, i.e. included in
    ///   the query. NB: If you are using the default engine and plan to call arrow's
    ///   `filter_record_batch`, you _need_ to extend this vector to the full length of the batch or
    ///   arrow will drop the extra rows.
    pub fn scan_data(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanData>>> {
        Ok(scan_action_iter(
            engine,
            self.replay_for_scan_data(engine)?,
            &self.logical_schema,
            self.predicate(),
        ))
    }

    // Factored out to facilitate testing
    fn replay_for_scan_data(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send> {
        let commit_read_schema = get_log_schema().project(&[ADD_NAME, REMOVE_NAME])?;
        let checkpoint_read_schema = get_log_add_schema().clone();

        // NOTE: We don't pass any meta-predicate because we expect no meaningful row group skipping
        // when ~every checkpoint file will contain the adds and removes we are looking for.
        self.table_changes.log_segment.replay(
            engine,
            commit_read_schema,
            checkpoint_read_schema,
            None,
        )
    }

    /// Get global state that is valid for the entire scan. This is somewhat expensive so should
    /// only be called once per scan.
    pub fn global_scan_state(&self) -> GlobalScanState {
        GlobalScanState {
            table_root: self.table_changes.table_root.to_string(),
            partition_columns: self.table_changes.metadata.partition_columns.clone(),
            logical_schema: self.logical_schema.clone(),
            read_schema: self.physical_schema.clone(),
            column_mapping_mode: self.table_changes.column_mapping_mode,
        }
    }

    /// Perform an "all in one" scan. This will use the provided `engine` to read and
    /// process all the data for the query. Each [`ScanResult`] in the resultant iterator encapsulates
    /// the raw data and an optional boolean vector built from the deletion vector if it was
    /// present. See the documentation for [`ScanResult`] for more details. Generally
    /// connectors/engines will want to use [`Scan::scan_data`] so they can have more control over
    /// the execution of the scan.
    // This calls [`Scan::scan_data`] to get an iterator of `ScanData` actions for the scan, and then uses the
    // `engine`'s [`crate::ParquetHandler`] to read the actual table data.
    pub fn execute<'a>(
        &'a self,
        engine: &'a dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanResult>> + 'a> {
        struct ScanFile {
            path: String,
            size: i64,
            dv_info: DvInfo,
            partition_values: HashMap<String, String>,
        }
        fn scan_data_callback(
            batches: &mut Vec<ScanFile>,
            path: &str,
            size: i64,
            _: Option<Stats>,
            dv_info: DvInfo,
            partition_values: HashMap<String, String>,
        ) {
            batches.push(ScanFile {
                path: path.to_string(),
                size,
                dv_info,
                partition_values,
            });
        }

        debug!(
            "Executing scan with logical schema {:#?} and physical schema {:#?}",
            self.logical_schema, self.physical_schema
        );

        let global_state = Arc::new(self.global_scan_state());
        let scan_data = self.scan_data(engine)?;
        let scan_files_iter = scan_data
            .map(|res| {
                let (data, vec) = res?;
                let scan_files = vec![];
                state::visit_scan_files(data.as_ref(), &vec, scan_files, scan_data_callback)
            })
            // Iterator<DeltaResult<Vec<ScanFile>>> to Iterator<DeltaResult<ScanFile>>
            .flatten_ok();

        let result = scan_files_iter
            .map(move |scan_file| -> DeltaResult<_> {
                let scan_file = scan_file?;
                let file_path = self.table_changes.table_root.join(&scan_file.path)?;
                let mut selection_vector = scan_file
                    .dv_info
                    .get_selection_vector(engine, &self.table_changes.table_root)?;
                let meta = FileMeta {
                    last_modified: 0,
                    size: scan_file.size as usize,
                    location: file_path,
                };
                let read_result_iter = engine.get_parquet_handler().read_parquet_files(
                    &[meta],
                    global_state.read_schema.clone(),
                    self.predicate(),
                )?;
                let gs = global_state.clone(); // Arc clone
                Ok(read_result_iter.map(move |read_result| -> DeltaResult<_> {
                    let read_result = read_result?;
                    // to transform the physical data into the correct logical form
                    let logical = transform_to_logical_internal(
                        engine,
                        read_result,
                        &gs,
                        &scan_file.partition_values,
                        &self.all_fields,
                        self.have_partition_cols,
                    );
                    let len = logical.as_ref().map_or(0, |res| res.length());
                    // need to split the dv_mask. what's left in dv_mask covers this result, and rest
                    // will cover the following results. we `take()` out of `selection_vector` to avoid
                    // trying to return a captured variable. We're going to reassign `selection_vector`
                    // to `rest` in a moment anyway
                    let mut sv = selection_vector.take();
                    let rest = split_vector(sv.as_mut(), len, None);
                    let result = ScanResult {
                        raw_data: logical,
                        raw_mask: sv,
                    };
                    selection_vector = rest;
                    Ok(result)
                }))
            })
            // Iterator<DeltaResult<Iterator<DeltaResult<ScanResult>>>> to Iterator<DeltaResult<DeltaResult<ScanResult>>>
            .flatten_ok()
            // Iterator<DeltaResult<DeltaResult<ScanResult>>> to Iterator<DeltaResult<ScanResult>>
            .map(|x| x?);
        Ok(result)
    }
}

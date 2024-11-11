use std::{collections::HashMap, sync::Arc};

use itertools::{Either, Itertools};
use tracing::debug;

use crate::{
    actions::{
        deletion_vector::{split_vector, treemap_to_bools},
        get_log_schema, ADD_NAME, CDC_NAME, COMMIT_INFO_NAME, REMOVE_NAME,
    },
    scan::{
        data_skipping::DataSkippingFilter, get_state_info, state::DvInfo, ColumnType, ScanData,
        ScanResult,
    },
    schema::{SchemaRef, StructType},
    table_changes::{
        data_read::transform_to_logical_internal,
        replay_scanner::TableChangesMetadataScanner,
        state::{self, ScanFile, ScanFileType},
    },
    DeltaResult, Engine, EngineData, ExpressionRef, FileMeta,
};

use super::{TableChanges, TableChangesGlobalScanState, TableChangesScanData};

/// Builder to scan a snapshot of a table.
pub struct TableChangesScanBuilder {
    table_changes: Arc<TableChanges>,
    schema: Option<SchemaRef>,
    predicate: Option<ExpressionRef>,
}
impl TableChangesScanBuilder {
    /// Create a new [`TableChangesScanBuilder`] instance.
    pub fn new(table_changes: impl Into<Arc<TableChanges>>) -> Self {
        Self {
            table_changes: table_changes.into(),
            schema: None,
            predicate: None,
        }
    }

    /// Provide [`Schema`] for columns to select from the [`TableChanges`].
    ///
    /// A table with columns `[a, b, c]` could have a scan which reads only the first
    /// two columns by using the schema `[a, b]`.
    ///
    /// [`Schema`]: crate::schema::Schema
    /// [`TableChanges`]: crate::table_changes:TableChanges:
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Optionally provide a [`SchemaRef`] for columns to select from the [`TableChanges`]. See
    /// [`TableChangesScanBuilder::with_schema`] for details. If `schema_opt` is `None` this is a no-op.
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

    /// Build the [`TableChangesScan`].
    ///
    /// This does not scan the table at this point, but does do some work to ensure that the
    /// provided schema make sense, and to prepare some metadata that the scan will need.  The
    /// [`TableChangesScan`] type itself can be used to fetch the files and associated metadata required to
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
    pub table_changes: Arc<TableChanges>,
    pub logical_schema: SchemaRef,
    pub physical_schema: SchemaRef,
    pub predicate: Option<ExpressionRef>,
    pub all_fields: Vec<ColumnType>,
    pub have_partition_cols: bool,
}

/// Given an iterator of (engine_data, bool) tuples and a predicate, returns an iterator of
/// `(engine_data, selection_vec)`. Each row that is selected in the returned `engine_data` _must_
/// be processed to complete the scan. Non-selected rows _must_ be ignored. The boolean flag
/// indicates whether the record batch is a log or checkpoint batch.
pub fn table_changes_action_iter(
    engine: &dyn Engine,
    commit_iter: impl Iterator<
        Item = (
            DeltaResult<Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send>>,
            i64,
            i64,
        ),
    >,
    table_schema: &SchemaRef,
    predicate: Option<ExpressionRef>,
) -> DeltaResult<impl Iterator<Item = DeltaResult<TableChangesScanData>>> {
    let filter = DataSkippingFilter::new(engine, table_schema, predicate);
    let expression_handler = engine.get_expression_handler();
    let result = commit_iter
        .map(
            move |(action_iter, timestamp, commit_version)| -> DeltaResult<_> {
                let action_iter = action_iter?;
                let expression_handler = expression_handler.clone();
                let mut metadata_scanner =
                    TableChangesMetadataScanner::new(filter.clone(), timestamp, commit_version);

                // Find CDC, get commitInfo, and perform metadata scan
                let mut batches = vec![];
                for action_res in action_iter {
                    let batch = action_res?;
                    // TODO: Make this metadata iterator
                    metadata_scanner.process_scan_batch(batch.as_ref())?;
                    batches.push(batch);
                }

                let mut log_scanner = metadata_scanner.into_replay_scanner();
                // File metadata output scan
                let x: Vec<ScanData> = batches
                    .into_iter()
                    .map(|batch| {
                        log_scanner.process_scan_batch(expression_handler.as_ref(), batch.as_ref())
                    })
                    .try_collect()?;
                let remove_dvs = Arc::new(log_scanner.remove_dvs);
                let y = x.into_iter().map(move |(a, b)| {
                    let remove_dvs = remove_dvs.clone();
                    (a, b, remove_dvs)
                });
                Ok(y)
            },
        )
        .flatten_ok();
    Ok(result)
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
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<TableChangesScanData>>> {
        table_changes_action_iter(
            engine,
            self.replay_for_scan_data(engine)?,
            &self.logical_schema,
            self.predicate(),
        )
    }

    // Factored out to facilitate testing
    fn replay_for_scan_data(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<
        impl Iterator<
            Item = (
                DeltaResult<Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send>>,
                i64,
                i64,
            ),
        >,
    > {
        let commit_read_schema =
            get_log_schema().project(&[ADD_NAME, CDC_NAME, REMOVE_NAME, COMMIT_INFO_NAME])?;

        let json_client = engine.get_json_handler();

        // Collect so we don't return reference to self
        let commits = self
            .table_changes
            .log_segment
            .commit_files
            .iter()
            .map(move |log_path| {
                let file = log_path.location.clone();
                let version = log_path.version as i64;
                (file, version)
            })
            .collect_vec();

        let result = commits.into_iter().map(move |(file, version)| {
            let timestamp = file.last_modified;
            // NOTE: We don't pass any meta-predicate because we expect no meaningful row group skipping
            // when ~every checkpoint file will contain the adds and removes we are looking for.
            let commit = json_client.read_json_files(&[file], commit_read_schema.clone(), None);

            (commit, timestamp, version)
        });
        Ok(result)
    }

    /// Get global state that is valid for the entire scan. This is somewhat expensive so should
    /// only be called once per scan.
    pub(crate) fn global_scan_state(&self) -> TableChangesGlobalScanState {
        TableChangesGlobalScanState {
            table_root: self.table_changes.table_root.to_string(),
            partition_columns: self.table_changes.metadata.partition_columns.clone(),
            logical_schema: self.logical_schema.clone(),
            read_schema: self.physical_schema.clone(),
            column_mapping_mode: self.table_changes.column_mapping_mode,
            // TODO: Arc output_schema earlier
            output_schema: self.table_changes.output_schema.clone().into(),
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
        #[derive(Debug)]
        struct ScanFileContext {
            pub files: Vec<ScanFile>,
            pub remove_dv: Arc<HashMap<String, DvInfo>>,
        }
        fn scan_data_callback(context: &mut ScanFileContext, scan_file: ScanFile) {
            context.files.push(scan_file);
        }

        debug!(
            "Executing scan with logical schema {:#?} and physical schema {:#?}",
            self.logical_schema, self.physical_schema
        );
        debug!(
            "Executing scan with logical schema {:#?} and physical schema {:#?}",
            self.logical_schema, self.physical_schema
        );

        let global_state = Arc::new(self.global_scan_state());
        let scan_data = self.scan_data(engine)?;
        let scan_files_iter: Vec<_> = scan_data
            .map(|res| -> DeltaResult<_> {
                let (data, vec, remove_dv) = res?;
                let context = ScanFileContext {
                    files: vec![],
                    remove_dv,
                };

                let context =
                    state::visit_scan_files(data.as_ref(), &vec, context, scan_data_callback)?;

                Ok(context
                    .files
                    .into_iter()
                    .map(move |x| (x, context.remove_dv.clone())))
            })
            .flatten_ok()
            .collect_vec();

        let result = scan_files_iter
            .into_iter()
            .map(move |scan_res| -> DeltaResult<_> {
                let (scan_file, dv_map) = scan_res?;
                let ScanFile {
                    tpe,
                    path,
                    dv_info,
                    partition_values,
                    size,
                    commit_version,
                    timestamp,
                } = scan_file;
                let file_path = self.table_changes.table_root.join(&path)?;
                match (&tpe, dv_map.get(&path)) {
                    (state::ScanFileType::Add, Some(rm_dv)) => {
                        let meta = FileMeta {
                            last_modified: 0,
                            size: size as usize,
                            location: file_path,
                        };
                        let add_dv = dv_info
                            .get_treemap(engine, &self.table_changes.table_root)?
                            .unwrap_or(Default::default());
                        let rm_dv = rm_dv
                            .get_treemap(engine, &self.table_changes.table_root)?
                            .unwrap_or(Default::default());
                        let added = &rm_dv - &add_dv;
                        let added = treemap_to_bools(added);

                        let y = self.generate_output_rows(
                            engine,
                            meta.clone(),
                            global_state.clone(),
                            partition_values.clone(),
                            commit_version,
                            timestamp,
                            ScanFileType::Add,
                            Some(added),
                            Some(false),
                        )?;
                        let removed = add_dv - rm_dv;
                        let removed = treemap_to_bools(removed);

                        let x = self.generate_output_rows(
                            engine,
                            meta,
                            global_state.clone(),
                            partition_values.clone(),
                            commit_version,
                            timestamp,
                            ScanFileType::Remove,
                            Some(removed),
                            Some(false),
                        )?;

                        Ok(Either::Left(x.chain(y)))
                    }
                    _ => {
                        let selection_vector =
                            dv_info.get_selection_vector(engine, &self.table_changes.table_root)?;
                        let meta = FileMeta {
                            last_modified: 0,
                            size: size as usize,
                            location: file_path,
                        };
                        Ok(Either::Right(self.generate_output_rows(
                            engine,
                            meta,
                            global_state.clone(),
                            partition_values,
                            commit_version,
                            timestamp,
                            tpe,
                            selection_vector,
                            None,
                        )?))
                    }
                }
            })
            // // Iterator<DeltaResult<Iterator<DeltaResult<ScanResult>>>> to Iterator<DeltaResult<DeltaResult<ScanResult>>>
            .flatten_ok()
            // // Iterator<DeltaResult<DeltaResult<ScanResult>>> to Iterator<DeltaResult<ScanResult>>
            .map(|x| x?);
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    fn generate_output_rows<'a>(
        &'a self,
        engine: &'a dyn Engine,
        meta: FileMeta,
        global_state: Arc<TableChangesGlobalScanState>,
        partition_values: HashMap<String, String>,
        commit_version: i64,
        timestamp: i64,
        tpe: state::ScanFileType,
        mut selection_vector: Option<Vec<bool>>,
        extend: Option<bool>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanResult>> + 'a> {
        let read_result_iter = engine.get_parquet_handler().read_parquet_files(
            &[meta],
            global_state.read_schema.clone(),
            self.predicate(),
        )?;

        // Arc clone
        let gs = global_state.clone();

        Ok(read_result_iter.map(move |read_result| -> DeltaResult<_> {
            let read_result = read_result?;
            // to transform the physical data into the correct logical form
            let logical = transform_to_logical_internal(
                engine,
                read_result,
                &gs,
                &partition_values,
                &self.all_fields,
                self.have_partition_cols,
                commit_version,
                timestamp,
                tpe.clone(),
            );
            let len = logical.as_ref().map_or(0, |res| res.length());
            // need to split the dv_mask. what's left in dv_mask covers this result, and rest
            // will cover the following results. we `take()` out of `selection_vector` to avoid
            // trying to return a captured variable. We're going to reassign `selection_vector`
            // to `rest` in a moment anyway
            let mut sv = selection_vector.take();
            let rest = split_vector(sv.as_mut(), len, extend);
            let result = ScanResult {
                raw_data: logical,
                raw_mask: sv,
            };
            selection_vector = rest;
            Ok(result)
        }))
    }
}

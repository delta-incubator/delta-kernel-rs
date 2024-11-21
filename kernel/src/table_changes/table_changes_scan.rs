use std::{collections::HashMap, iter::once, sync::Arc};

use itertools::{Either, Itertools};
use tracing::debug;

use crate::{
    actions::deletion_vector::{split_vector, treemap_to_bools},
    scan::{
        state::{DvInfo, GlobalScanState},
        ColumnType, ScanResult,
    },
    schema::{DataType, SchemaRef, StructField, StructType},
    table_changes::{
        data_read::get_generated_columns,
        state::{ScanFile, ScanFileType},
    },
    DeltaResult, Engine, Expression, ExpressionRef, FileMeta,
};

use super::{
    data_read::transform_to_logical_internal, replay_scanner::table_changes_action_iter, state,
    TableChanges, TableChangesScanData, CDF_GENERATED_COLUMNS,
};

/// The result of building a [`TableChanges`] scan over a table. This can be used to get a change
/// data feed from the table
#[allow(unused)]
pub struct TableChangesScan {
    table_changes: Arc<TableChanges>,
    logical_schema: SchemaRef,
    physical_schema: SchemaRef,
    predicate: Option<ExpressionRef>,
    all_fields: Vec<ColumnType>,
    generated_columns: Vec<String>,
    have_partition_cols: bool,
}
/// Builder to read the `TableChanges` of a table.
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

    fn logical_schema(&self) -> Arc<StructType> {
        match &self.schema {
            Some(schema) => schema.clone(),
            None => self.table_changes.schema.clone().into(),
        }
    }

    /// Build the [`TableChangesScan`].
    ///
    /// This does not scan the table at this point, but does do some work to ensure that the
    /// provided schema make sense, and to prepare some metadata that the scan will need.  The
    /// [`TableChangesScan`] type itself can be used to fetch the files and associated metadata required to
    /// perform actual data reads.
    pub fn build(self) -> DeltaResult<TableChangesScan> {
        // if no schema is provided, use snapshot's entire schema (e.g. SELECT *)
        let logical_schema = self.logical_schema();
        let mut have_partition_cols = false;
        let mut read_fields = Vec::with_capacity(logical_schema.fields.len());

        // Loop over all selected fields and note if they are columns that will be read from the
        // parquet file ([`ColumnType::Selected`]) or if they are partition columns and will need to
        // be filled in by evaluating an expression ([`ColumnType::Partition`])
        let column_types = logical_schema
            .fields()
            .enumerate()
            .map(|(index, logical_field)| -> DeltaResult<_> {
                if self
                    .table_changes
                    .metadata
                    .partition_columns
                    .contains(logical_field.name())
                {
                    // Store the index into the schema for this field. When we turn it into an
                    // expression in the inner loop, we will index into the schema and get the name and
                    // data type, which we need to properly materialize the column.
                    have_partition_cols = true;
                    Ok(ColumnType::Partition(index))
                } else if CDF_GENERATED_COLUMNS.contains(&logical_field.name().as_str()) {
                    Ok(ColumnType::InsertedColumn(index))
                } else {
                    // Add to read schema, store field so we can build a `Column` expression later
                    // if needed (i.e. if we have partition columns)
                    let physical_field =
                        logical_field.make_physical(self.table_changes.column_mapping_mode)?;
                    debug!("\n\n{logical_field:#?}\nAfter mapping: {physical_field:#?}\n\n");
                    let physical_name = physical_field.name.clone();
                    read_fields.push(physical_field);
                    Ok(ColumnType::Selected(physical_name))
                }
            })
            .try_collect()?;
        let generated_columns = vec![];
        let physical_schema = Arc::new(StructType::new(read_fields));
        Ok(TableChangesScan {
            table_changes: self.table_changes,
            logical_schema,
            physical_schema,
            predicate: self.predicate,
            all_fields: column_types,
            generated_columns,
            have_partition_cols,
        })
    }
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
    /// log-replay, reconciling Add, Remove, and CDC actions, and applying data skipping (if
    /// possible). Each item in the returned iterator is a tuple of:
    /// - `Box<dyn EngineData>`: Data in engine format, where each row represents a file to be
    ///   scanned. The schema for each row can be obtained by calling [`scan_row_schema`]. The rows
    ///   are guaranteed to belong to the same commit.
    /// - `Vec<bool>`: A selection vector. If a row is at index `i` and this vector is `false` at
    ///   index `i`, then that row should *not* be processed (i.e. it is filtered out). If the vector
    ///   is `true` at index `i` the row *should* be processed. If the selector vector is *shorter*
    ///   than the number of rows returned, missing elements are considered `true`, i.e. included in
    ///   the query. NB: If you are using the default engine and plan to call arrow's
    ///   `filter_record_batch`, you _need_ to extend this vector to the full length of the batch or
    ///   arrow will drop the extra rows.
    /// - `Arc<HashMap<String, DvInfo>>`: A map from file path to the deletion vector belonging to a
    ///   remove action. It is guaranteed that there is an add action with the same path present in
    ///   the same commit. This is used to resolve the additions and removals when deletion vectors
    ///   are enabled.
    pub fn scan_data(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<TableChangesScanData>>> {
        let commit_files = self.table_changes.log_segment.sorted_commit_files.clone();
        table_changes_action_iter(engine, commit_files, &self.logical_schema, self.predicate())
    }

    /// Get global state that is valid for the entire scan. This is somewhat expensive so should
    /// only be called once per scan.
    pub(crate) fn global_scan_state(&self) -> GlobalScanState {
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
                let file = FileMeta {
                    last_modified: 0,
                    size: size as usize,
                    location: file_path,
                };
                println!(
                    "for version: {}, tpe: {:?}, and dv : {:?}",
                    commit_version,
                    tpe,
                    dv_map.get(&path),
                );
                match (&tpe, dv_map.get(&path)) {
                    (state::ScanFileType::Add, Some(rm_dv)) => {
                        let generated_columns = get_generated_columns(
                            timestamp,
                            state::ScanFileType::Add,
                            commit_version,
                        )?;

                        let add_dv = dv_info
                            .get_treemap(engine, &self.table_changes.table_root)?
                            .unwrap_or(Default::default());
                        let rm_dv = rm_dv
                            .get_treemap(engine, &self.table_changes.table_root)?
                            .unwrap_or(Default::default());

                        let added = treemap_to_bools(&rm_dv - &add_dv);
                        println!("added: {:?}", added);
                        let added_rows = self.generate_output_rows(
                            engine,
                            file.clone(),
                            global_state.clone(),
                            partition_values.clone(),
                            Some(added),
                            Some(false),
                            generated_columns.clone(),
                            self.global_scan_state().read_schema.clone(),
                        )?;

                        let removed = treemap_to_bools(add_dv - rm_dv);
                        println!("removed : {:?}", removed);
                        let generated_columns = get_generated_columns(
                            timestamp,
                            state::ScanFileType::Remove,
                            commit_version,
                        )?;
                        let removed_rows = self.generate_output_rows(
                            engine,
                            file,
                            global_state.clone(),
                            partition_values.clone(),
                            Some(removed),
                            Some(false),
                            generated_columns.clone(),
                            self.global_scan_state().read_schema.clone(),
                        )?;

                        Ok(Either::Left(added_rows.chain(removed_rows)))
                    }
                    (ScanFileType::Cdc, _) => {
                        let selection_vector =
                            dv_info.get_selection_vector(engine, &self.table_changes.table_root)?;

                        let generated_columns =
                            get_generated_columns(timestamp, tpe, commit_version)?;

                        let fields = self
                            .global_scan_state()
                            .read_schema
                            .fields()
                            .cloned()
                            .collect_vec();
                        let read_schema = StructType::new(fields.into_iter().chain(once(
                            StructField::new("_change_type", DataType::STRING, false),
                        )));
                        Ok(Either::Right(self.generate_output_rows(
                            engine,
                            file,
                            global_state.clone(),
                            partition_values,
                            selection_vector,
                            None,
                            generated_columns,
                            read_schema.into(),
                        )?))
                    }
                    _ => {
                        let selection_vector =
                            dv_info.get_selection_vector(engine, &self.table_changes.table_root)?;

                        let generated_columns =
                            get_generated_columns(timestamp, tpe, commit_version)?;
                        Ok(Either::Right(self.generate_output_rows(
                            engine,
                            file,
                            global_state.clone(),
                            partition_values,
                            selection_vector,
                            None,
                            generated_columns,
                            self.global_scan_state().read_schema.clone(),
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
        global_state: Arc<GlobalScanState>,
        partition_values: HashMap<String, String>,
        mut selection_vector: Option<Vec<bool>>,
        extend: Option<bool>,
        generated_columns: Arc<HashMap<String, Expression>>,
        read_schema: Arc<StructType>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanResult>> + 'a> {
        let read_result_iter = engine.get_parquet_handler().read_parquet_files(
            &[meta],
            read_schema.clone(),
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
                gs.as_ref(),
                &partition_values,
                &self.all_fields,
                self.have_partition_cols,
                generated_columns.as_ref(),
                read_schema.clone(),
            );
            let len = logical.as_ref().map_or(0, |res| res.len());
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

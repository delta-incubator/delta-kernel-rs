use std::collections::HashMap;
use std::iter;

use itertools::Itertools;
use url::Url;

use crate::actions::deletion_vector::split_vector;
use crate::expressions::{column_expr, Scalar};
use crate::scan::state::GlobalScanState;
use crate::scan::{parse_partition_value, ColumnType, ScanResult};
use crate::schema::{ColumnName, DataType, SchemaRef, StructField, StructType};
use crate::{DeltaResult, Engine, Error, Expression, ExpressionRef, FileMeta};

use super::resolve_dvs::ResolvedCdfScanFile;
use super::scan_file::{CdfScanFile, CdfScanFileType};

/// Returns a map from change data feed column name to an expression that generates the row data.
fn get_generated_columns(scan_file: &CdfScanFile) -> DeltaResult<HashMap<&str, Expression>> {
    let timestamp = Scalar::timestamp_from_millis(scan_file.commit_timestamp)?;
    let version = scan_file.commit_version;
    let change_type: Expression = match scan_file.scan_type {
        CdfScanFileType::Cdc => column_expr!("_change_type"),
        CdfScanFileType::Add => "insert".into(),

        CdfScanFileType::Remove => "delete".into(),
    };
    let expressions = [
        ("_change_type", change_type),
        ("_commit_version", Expression::literal(version)),
        ("_commit_timestamp", timestamp.into()),
    ];
    Ok(expressions.into_iter().collect())
}

/// Generates the expression used to convert physical data from the `scan_file` path into logical
/// data matching the `global_state.logical_schema`
fn get_expression(
    scan_file: &CdfScanFile,
    global_state: &GlobalScanState,
    all_fields: &[ColumnType],
) -> DeltaResult<Expression> {
    let mut generated_columns = get_generated_columns(scan_file)?;
    let all_fields = all_fields
        .iter()
        .map(|field| match field {
            ColumnType::Partition(field_idx) => {
                let field = global_state.logical_schema.fields.get_index(*field_idx);
                let Some((_, field)) = field else {
                    return Err(Error::generic(
                        "logical schema did not contain expected field, can't transform data",
                    ));
                };
                let name = field.physical_name();
                let value_expression =
                    parse_partition_value(scan_file.partition_values.get(name), field.data_type())?;
                Ok(value_expression.into())
            }
            ColumnType::Selected(field_name) => {
                // Remove to take ownership
                let generated_column = generated_columns.remove(field_name.as_str());
                Ok(generated_column.unwrap_or_else(|| ColumnName::new([field_name]).into()))
            }
        })
        .try_collect()?;
    Ok(Expression::Struct(all_fields))
}

/// Gets the physical schema that will be used to read data in the `scan_file` path.
fn get_read_schema(scan_file: &CdfScanFile, global_scan_state: &GlobalScanState) -> SchemaRef {
    if scan_file.scan_type == CdfScanFileType::Cdc {
        let change_type = StructField::new("_change_type", DataType::STRING, false);
        let fields = global_scan_state
            .read_schema
            .fields()
            .cloned()
            .chain(iter::once(change_type));
        StructType::new(fields).into()
    } else {
        global_scan_state.read_schema.clone()
    }
}

/// Reads the data at the `resolved_scan_file` and transforms the data from physical to logical.
/// The result is a fallible iterator of [`ScanResult`] containing the logical data.
pub(crate) fn read_scan_data(
    engine: &dyn Engine,
    resolved_scan_file: ResolvedCdfScanFile,
    global_state: &GlobalScanState,
    all_fields: &[ColumnType],
    predicate: Option<ExpressionRef>,
) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanResult>>> {
    let ResolvedCdfScanFile {
        scan_file,
        mut selection_vector,
    } = resolved_scan_file;

    let expression = get_expression(&scan_file, global_state, all_fields)?;
    let schema = get_read_schema(&scan_file, global_state);
    let evaluator = engine.get_expression_handler().get_evaluator(
        schema.clone(),
        expression,
        global_state.logical_schema.clone().into(),
    );

    let table_root = Url::parse(&global_state.table_root)?;
    let location = table_root.join(&scan_file.path)?;
    let file = FileMeta {
        last_modified: 0,
        size: 0,
        location,
    };
    let read_result_iter =
        engine
            .get_parquet_handler()
            .read_parquet_files(&[file], schema, predicate)?;

    let result = read_result_iter.map(move |batch| -> DeltaResult<_> {
        let batch = batch?;
        // to transform the physical data into the correct logical form
        let logical = evaluator.evaluate(batch.as_ref());
        let len = logical.as_ref().map_or(0, |res| res.len());
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
    });
    Ok(result)
}

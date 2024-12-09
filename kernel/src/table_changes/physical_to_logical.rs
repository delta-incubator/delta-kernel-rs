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
#[allow(unused)]
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
#[allow(unused)]
fn get_expression(
    scan_file: &CdfScanFile,
    logical_schema: &StructType,
    all_fields: &[ColumnType],
) -> DeltaResult<Expression> {
    let mut generated_columns = get_generated_columns(scan_file)?;
    let all_fields = all_fields
        .iter()
        .map(|field| match field {
            ColumnType::Partition(field_idx) => {
                let field = logical_schema.fields.get_index(*field_idx);
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
#[allow(unused)]
fn get_read_schema(scan_file: &CdfScanFile, read_schema: &StructType) -> SchemaRef {
    if scan_file.scan_type == CdfScanFileType::Cdc {
        let change_type = StructField::new("_change_type", DataType::STRING, false);
        let fields = read_schema.fields().cloned().chain(iter::once(change_type));
        StructType::new(fields).into()
    } else {
        read_schema.clone().into()
    }
}

/// Reads the data at the `resolved_scan_file` and transforms the data from physical to logical.
/// The result is a fallible iterator of [`ScanResult`] containing the logical data.
#[allow(unused)]
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

    let expression = get_expression(&scan_file, global_state.logical_schema.as_ref(), all_fields)?;
    let schema = get_read_schema(&scan_file, global_state.read_schema.as_ref());
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::expressions::{column_expr, Expression, Scalar};
    use crate::scan::ColumnType;
    use crate::schema::{DataType, StructField, StructType};
    use crate::table_changes::scan_file::{CdfScanFile, CdfScanFileType};

    use super::get_expression;

    #[test]
    fn add_get_expression() {
        let test = |scan_type, expected_expr| {
            let scan_file = CdfScanFile {
                scan_type,
                path: "fake_path".to_string(),
                dv_info: Default::default(),
                remove_dv: None,
                partition_values: HashMap::from([("age".to_string(), "20".to_string())]),
                commit_version: 42,
                commit_timestamp: 1234,
            };
            let logical_schema = StructType::new([
                StructField::new("id", DataType::STRING, true),
                StructField::new("age", DataType::LONG, false),
                StructField::new("_change_type", DataType::STRING, false),
                StructField::new("_commit_version", DataType::LONG, false),
                StructField::new("_commit_timestamp", DataType::TIMESTAMP, false),
            ]);
            let all_fields = vec![
                ColumnType::Selected("id".to_string()),
                ColumnType::Partition(1),
                ColumnType::Selected("_change_type".to_string()),
                ColumnType::Selected("_commit_version".to_string()),
                ColumnType::Selected("_commit_timestamp".to_string()),
            ];
            let expression = get_expression(&scan_file, &logical_schema, &all_fields).unwrap();
            let expected = Expression::struct_from([
                column_expr!("id"),
                Scalar::Long(20).into(),
                expected_expr,
                Expression::literal(42i64),
                Scalar::Timestamp(1234000).into(), // Microsecond is 1000x millisecond
            ]);

            assert_eq!(expression, expected)
        };

        test(CdfScanFileType::Add, "insert".into());
        test(CdfScanFileType::Remove, "delete".into());
        test(CdfScanFileType::Cdc, column_expr!("_change_type"));
    }
}

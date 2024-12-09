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

#[allow(unused)]
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
#[allow(unused)]
fn read_schema(scan_file: &CdfScanFile, global_scan_state: &GlobalScanState) -> SchemaRef {
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
    let schema = read_schema(&scan_file, global_state);
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

    use std::{collections::HashMap, error, sync::Arc};

    use arrow::compute::filter_record_batch;
    use arrow_array::RecordBatch;
    use arrow_cast::pretty::pretty_format_batches;
    use itertools::Itertools;

    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
    use crate::engine::default::DefaultEngine;
    use crate::{DeltaResult, Error, Table, Version};

    fn read_cdf_for_table(
        path: impl AsRef<str>,
        start_version: Version,
        end_version: impl Into<Option<Version>>,
    ) -> DeltaResult<Vec<RecordBatch>> {
        let table = Table::try_from_uri(path)?;
        let options = HashMap::from([("skip_signature", "true".to_string())]);
        let engine = Arc::new(DefaultEngine::try_new(
            table.location(),
            options,
            Arc::new(TokioBackgroundExecutor::new()),
        )?);
        let table_changes = table.table_changes(engine.as_ref(), start_version, end_version)?;

        let x = table_changes.into_scan_builder().build()?;
        let batches: Vec<RecordBatch> = x
            .execute(engine)?
            .map(|scan_result| -> DeltaResult<_> {
                let scan_result = scan_result?;
                let mask = scan_result.full_mask();
                let data = scan_result.raw_data?;
                let record_batch: RecordBatch = data
                    .into_any()
                    .downcast::<ArrowEngineData>()
                    .map_err(|_| Error::engine_data_type("ArrowEngineData".to_string()))?
                    .into();
                if let Some(mask) = mask {
                    Ok(filter_record_batch(&record_batch, &mask.into())?)
                } else {
                    Ok(record_batch)
                }
            })
            .try_collect()?;
        Ok(batches)
    }

    fn assert_batches_sorted_eq(expected_lines: &[impl ToString], batches: &[RecordBatch]) {
        let mut expected_lines: Vec<String> =
            expected_lines.iter().map(ToString::to_string).collect();

        // sort except for header + footer
        let num_lines = expected_lines.len();
        if num_lines > 3 {
            expected_lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
        }

        let formatted = arrow::util::pretty::pretty_format_batches(batches)
            .unwrap()
            .to_string();

        let mut actual_lines: Vec<&str> = formatted.trim().lines().collect();

        // sort except for header + footer
        let num_lines = actual_lines.len();
        if num_lines > 3 {
            actual_lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
        }

        let expected_table_str = expected_lines.join("\n");
        let actual_table_str = actual_lines.join("\n");

        assert_eq!(
            actual_lines.len(),
            expected_lines.len(),
            "Incorrect number of lines. Expected:\n{}\nbut got:\n{} ",
            expected_table_str,
            actual_table_str
        );
        for (expected, actual) in expected_lines.iter().zip(actual_lines) {
            assert_eq!(
                expected, actual,
                "Expected:\n{}\nbut got:\n{}",
                expected_table_str, actual_table_str
            );
        }
    }

    #[test]
    fn cdf_with_deletion_vector() -> Result<(), Box<dyn error::Error>> {
        let cdf = read_cdf_for_table("tests/data/table-with-cdf-and-dv", 0, None)?;
        assert_batches_sorted_eq(
            &[
                "+-------+--------------+-----------------+--------------------------+",
                "| value | _change_type | _commit_version | _commit_timestamp        |",
                "+-------+--------------+-----------------+--------------------------+",
                "| 0     | insert       | 0               | 1970-01-21T01:35:06.498Z |",
                "| 1     | insert       | 0               | 1970-01-21T01:35:06.498Z |",
                "| 2     | insert       | 0               | 1970-01-21T01:35:06.498Z |",
                "| 3     | insert       | 0               | 1970-01-21T01:35:06.498Z |",
                "| 4     | insert       | 0               | 1970-01-21T01:35:06.498Z |",
                "| 5     | insert       | 0               | 1970-01-21T01:35:06.498Z |",
                "| 6     | insert       | 0               | 1970-01-21T01:35:06.498Z |",
                "| 8     | insert       | 0               | 1970-01-21T01:35:06.498Z |",
                "| 7     | insert       | 0               | 1970-01-21T01:35:06.498Z |",
                "| 9     | insert       | 0               | 1970-01-21T01:35:06.498Z |",
                "| 0     | delete       | 1               | 1970-01-21T01:35:06.498Z |",
                "| 9     | delete       | 1               | 1970-01-21T01:35:06.498Z |",
                "| 0     | insert       | 2               | 1970-01-21T01:35:06.498Z |",
                "| 9     | insert       | 2               | 1970-01-21T01:35:06.498Z |",
                "+-------+--------------+-----------------+--------------------------+",
            ],
            &cdf,
        );
        Ok(())
    }

    #[test]
    fn basic_cdf() -> Result<(), Box<dyn error::Error>> {
        let batches = read_cdf_for_table("tests/data/cdf-table", 0, None)?;
        assert_batches_sorted_eq(&[
             "+----+--------+------------+------------------+-----------------+--------------------------+",
             "| id | name   | birthday   | _change_type     | _commit_version | _commit_timestamp        |",
             "+----+--------+------------+------------------+-----------------+--------------------------+",
             "| 1  | Steve  | 2023-12-22 | insert           | 0               | 2023-12-22T17:10:18.828  |",
             "| 2  | Bob    | 2023-12-23 | insert           | 0               | 2023-12-22T17:10:18.828  |",
             "| 3  | Dave   | 2023-12-23 | insert           | 0               | 2023-12-22T17:10:18.828  |",
             "| 4  | Kate   | 2023-12-23 | insert           | 0               | 2023-12-22T17:10:18.828  |",
             "| 5  | Emily  | 2023-12-24 | insert           | 0               | 2023-12-22T17:10:18.828  |",
             "| 6  | Carl   | 2023-12-24 | insert           | 0               | 2023-12-22T17:10:18.828  |",
             "| 7  | Dennis | 2023-12-24 | insert           | 0               | 2023-12-22T17:10:18.828  |",
             "| 8  | Claire | 2023-12-25 | insert           | 0               | 2023-12-22T17:10:18.828  |",
             "| 9  | Ada    | 2023-12-25 | insert           | 0               | 2023-12-22T17:10:18.828  |",
             "| 10 | Borb   | 2023-12-25 | insert           | 0               | 2023-12-22T17:10:18.828  |",
             "| 3  | Dave   | 2023-12-22 | update_postimage | 1               | 2023-12-22T17:10:21.675  |",
             "| 3  | Dave   | 2023-12-23 | update_preimage  | 1               | 2023-12-22T17:10:21.675  |",
             "| 4  | Kate   | 2023-12-22 | update_postimage | 1               | 2023-12-22T17:10:21.675  |",
             "| 4  | Kate   | 2023-12-23 | update_preimage  | 1               | 2023-12-22T17:10:21.675  |",
             "| 2  | Bob    | 2023-12-22 | update_postimage | 1               | 2023-12-22T17:10:21.675  |",
             "| 2  | Bob    | 2023-12-23 | update_preimage  | 1               | 2023-12-22T17:10:21.675  |",
             "| 7  | Dennis | 2023-12-24 | update_preimage  | 2               | 2023-12-29T21:41:33.785  |",
             "| 7  | Dennis | 2023-12-29 | update_postimage | 2               | 2023-12-29T21:41:33.785  |",
             "| 5  | Emily  | 2023-12-24 | update_preimage  | 2               | 2023-12-29T21:41:33.785  |",
             "| 5  | Emily  | 2023-12-29 | update_postimage | 2               | 2023-12-29T21:41:33.785  |",
             "| 6  | Carl   | 2023-12-24 | update_preimage  | 2               | 2023-12-29T21:41:33.785  |",
             "| 6  | Carl   | 2023-12-29 | update_postimage | 2               | 2023-12-29T21:41:33.785  |",
             "| 7  | Dennis | 2023-12-29 | delete           | 3               | 2024-01-06T16:44:59.570  |",
             "+----+--------+------------+------------------+-----------------+--------------------------+"],
            &batches
        );
        Ok(())
    }

    #[test]
    fn cdf_non_partitioned() -> Result<(), Box<dyn error::Error>> {
        let batches = read_cdf_for_table("tests/data/cdf-table-non-partitioned", 0, None)?;
        assert_batches_sorted_eq(&[
             "+----+--------+------------+-------------------+---------------+--------------+----------------+------------------+-----------------+--------------------------+",
             "| id | name   | birthday   | long_field        | boolean_field | double_field | smallint_field | _change_type     | _commit_version | _commit_timestamp        |",
             "+----+--------+------------+-------------------+---------------+--------------+----------------+------------------+-----------------+--------------------------+",
             "| 1  | Steve  | 2024-04-14 | 1                 | true          | 3.14         | 1              | insert           | 0               | 2024-04-14T15:58:26.249  |",
             "| 2  | Bob    | 2024-04-15 | 1                 | true          | 3.14         | 1              | insert           | 0               | 2024-04-14T15:58:26.249  |",
             "| 3  | Dave   | 2024-04-15 | 2                 | true          | 3.14         | 1              | insert           | 0               | 2024-04-14T15:58:26.249  |",
             "| 4  | Kate   | 2024-04-15 | 3                 | true          | 3.14         | 1              | insert           | 0               | 2024-04-14T15:58:26.249  |",
             "| 5  | Emily  | 2024-04-16 | 4                 | true          | 3.14         | 1              | insert           | 0               | 2024-04-14T15:58:26.249  |",
             "| 6  | Carl   | 2024-04-16 | 5                 | true          | 3.14         | 1              | insert           | 0               | 2024-04-14T15:58:26.249  |",
             "| 7  | Dennis | 2024-04-16 | 6                 | true          | 3.14         | 1              | insert           | 0               | 2024-04-14T15:58:26.249  |",
             "| 8  | Claire | 2024-04-17 | 7                 | true          | 3.14         | 1              | insert           | 0               | 2024-04-14T15:58:26.249  |",
             "| 9  | Ada    | 2024-04-17 | 8                 | true          | 3.14         | 1              | insert           | 0               | 2024-04-14T15:58:26.249  |",
             "| 10 | Borb   | 2024-04-17 | 99999999999999999 | true          | 3.14         | 1              | insert           | 0               | 2024-04-14T15:58:26.249  |",
             "| 3  | Dave   | 2024-04-15 | 2                 | true          | 3.14         | 1              | update_preimage  | 1               | 2024-04-14T15:58:29.393  |",
             "| 3  | Dave   | 2024-04-14 | 2                 | true          | 3.14         | 1              | update_postimage | 1               | 2024-04-14T15:58:29.393  |",
             "| 4  | Kate   | 2024-04-15 | 3                 | true          | 3.14         | 1              | update_preimage  | 1               | 2024-04-14T15:58:29.393  |",
             "| 4  | Kate   | 2024-04-14 | 3                 | true          | 3.14         | 1              | update_postimage | 1               | 2024-04-14T15:58:29.393  |",
             "| 2  | Bob    | 2024-04-15 | 1                 | true          | 3.14         | 1              | update_preimage  | 1               | 2024-04-14T15:58:29.393  |",
             "| 2  | Bob    | 2024-04-14 | 1                 | true          | 3.14         | 1              | update_postimage | 1               | 2024-04-14T15:58:29.393  |",
             "| 7  | Dennis | 2024-04-16 | 6                 | true          | 3.14         | 1              | update_preimage  | 2               | 2024-04-14T15:58:31.257  |",
             "| 7  | Dennis | 2024-04-14 | 6                 | true          | 3.14         | 1              | update_postimage | 2               | 2024-04-14T15:58:31.257  |",
             "| 5  | Emily  | 2024-04-16 | 4                 | true          | 3.14         | 1              | update_preimage  | 2               | 2024-04-14T15:58:31.257  |",
             "| 5  | Emily  | 2024-04-14 | 4                 | true          | 3.14         | 1              | update_postimage | 2               | 2024-04-14T15:58:31.257  |",
             "| 6  | Carl   | 2024-04-16 | 5                 | true          | 3.14         | 1              | update_preimage  | 2               | 2024-04-14T15:58:31.257  |",
             "| 6  | Carl   | 2024-04-14 | 5                 | true          | 3.14         | 1              | update_postimage | 2               | 2024-04-14T15:58:31.257  |",
             "| 7  | Dennis | 2024-04-14 | 6                 | true          | 3.14         | 1              | delete           | 3               | 2024-04-14T15:58:32.495  |",
             "| 1  | Alex   | 2024-04-14 | 1                 | true          | 3.14         | 1              | insert           | 4               | 2024-04-14T15:58:33.444  |",
             "| 2  | Alan   | 2024-04-15 | 1                 | true          | 3.14         | 1              | insert           | 4               | 2024-04-14T15:58:33.444  |",
             "+----+--------+------------+-------------------+---------------+--------------+----------------+------------------+-----------------+--------------------------+"
        ], &batches);
        Ok(())
    }
}

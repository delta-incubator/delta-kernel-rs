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
    ) -> DeltaResult<String> {
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
        let formatted = pretty_format_batches(&batches)?.to_string();
        Ok(formatted)
    }

    #[test]
    fn cdf_with_deletion_vector() -> Result<(), Box<dyn error::Error>> {
        let cdf = read_cdf_for_table("tests/data/table-with-cdf-and-dv", 0, None)?;
        let expected = concat!(
            "+-------+--------------+-----------------+--------------------------+\n",
            "| value | _change_type | _commit_version | _commit_timestamp        |\n",
            "+-------+--------------+-----------------+--------------------------+\n",
            "| 0     | insert       | 0               | 1970-01-21T01:35:06.498Z |\n",
            "| 1     | insert       | 0               | 1970-01-21T01:35:06.498Z |\n",
            "| 2     | insert       | 0               | 1970-01-21T01:35:06.498Z |\n",
            "| 3     | insert       | 0               | 1970-01-21T01:35:06.498Z |\n",
            "| 4     | insert       | 0               | 1970-01-21T01:35:06.498Z |\n",
            "| 5     | insert       | 0               | 1970-01-21T01:35:06.498Z |\n",
            "| 6     | insert       | 0               | 1970-01-21T01:35:06.498Z |\n",
            "| 7     | insert       | 0               | 1970-01-21T01:35:06.498Z |\n",
            "| 8     | insert       | 0               | 1970-01-21T01:35:06.498Z |\n",
            "| 9     | insert       | 0               | 1970-01-21T01:35:06.498Z |\n",
            "| 0     | delete       | 1               | 1970-01-21T01:35:06.498Z |\n",
            "| 9     | delete       | 1               | 1970-01-21T01:35:06.498Z |\n",
            "| 0     | insert       | 2               | 1970-01-21T01:35:06.498Z |\n",
            "| 9     | insert       | 2               | 1970-01-21T01:35:06.498Z |\n",
            "+-------+--------------+-----------------+--------------------------+"
        );
        assert_eq!(expected, cdf);
        Ok(())
    }
    #[test]
    fn basic_cdf() -> Result<(), Box<dyn error::Error>> {
        let cdf = read_cdf_for_table("tests/data/cdf-table", 0, None)?;
        let expected = r#"
             +----+--------+------------------+-----------------+-------------------------+------------+
             | id | name   | _change_type     | _commit_version | _commit_timestamp       | birthday   |
             +----+--------+------------------+-----------------+-------------------------+------------+
             | 7  | Dennis | delete           | 3               | 2024-01-06T16:44:59.570 | 2023-12-29 |
             | 3  | Dave   | update_preimage  | 1               | 2023-12-22T17:10:21.675 | 2023-12-23 |
             | 4  | Kate   | update_preimage  | 1               | 2023-12-22T17:10:21.675 | 2023-12-23 |
             | 2  | Bob    | update_preimage  | 1               | 2023-12-22T17:10:21.675 | 2023-12-23 |
             | 7  | Dennis | update_preimage  | 2               | 2023-12-29T21:41:33.785 | 2023-12-24 |
             | 5  | Emily  | update_preimage  | 2               | 2023-12-29T21:41:33.785 | 2023-12-24 |
             | 6  | Carl   | update_preimage  | 2               | 2023-12-29T21:41:33.785 | 2023-12-24 |
             | 7  | Dennis | update_postimage | 2               | 2023-12-29T21:41:33.785 | 2023-12-29 |
             | 5  | Emily  | update_postimage | 2               | 2023-12-29T21:41:33.785 | 2023-12-29 |
             | 6  | Carl   | update_postimage | 2               | 2023-12-29T21:41:33.785 | 2023-12-29 |
             | 3  | Dave   | update_postimage | 1               | 2023-12-22T17:10:21.675 | 2023-12-22 |
             | 4  | Kate   | update_postimage | 1               | 2023-12-22T17:10:21.675 | 2023-12-22 |
             | 2  | Bob    | update_postimage | 1               | 2023-12-22T17:10:21.675 | 2023-12-22 |
             | 2  | Bob    | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-23 |
             | 3  | Dave   | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-23 |
             | 4  | Kate   | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-23 |
             | 5  | Emily  | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-24 |
             | 6  | Carl   | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-24 |
             | 7  | Dennis | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-24 |
             | 1  | Steve  | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-22 |
             | 8  | Claire | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-25 |
             | 9  | Ada    | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-25 |
             | 10 | Borb   | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-25 |
             +----+--------+------------------+-----------------+-------------------------+------------+
    "#;
        //TODO
        Ok(())
    }
}

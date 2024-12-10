use std::{collections::HashMap, error, sync::Arc};

use arrow::compute::filter_record_batch;
use arrow_array::RecordBatch;
use itertools::Itertools;

use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::{DeltaResult, Error, Table, Version};

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
    let sort_rows = |lines: &mut Vec<String>| {
        let num_lines = lines.len();
        if num_lines > 3 {
            // sort except for header + footer
            lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
        }
    };

    let mut expected_lines: Vec<String> = expected_lines.iter().map(ToString::to_string).collect();
    sort_rows(&mut expected_lines);

    let formatted = arrow::util::pretty::pretty_format_batches(batches)
        .unwrap()
        .to_string();
    let mut actual_lines: Vec<String> = formatted.trim().lines().map(ToString::to_string).collect();
    sort_rows(&mut actual_lines);

    let expected_table_str = expected_lines.join("\n");
    let actual_table_str = actual_lines.join("\n");

    assert_eq!(
        expected_lines.len(),
        actual_lines.len(),
        "Incorrect number of lines. Expected {} lines:\n{}\nbut got {} lines:\n{} ",
        expected_lines.len(),
        expected_table_str,
        actual_lines.len(),
        actual_table_str
    );
    for (expected, actual) in expected_lines.iter().zip(&actual_lines) {
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
             "+----+--------+------------+------------------+-----------------+--------------------------+"
            ],
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
            ],
            &batches);
    Ok(())
}

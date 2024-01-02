#![cfg(feature = "test-utils")]
use std::collections::HashMap;

use arrow_select::concat::concat_batches;
use deltakernel::expressions::{BinaryOperator, Expression};
use deltakernel::scan::ScanBuilder;
use deltakernel::test_util::{assert_batches_sorted_eq, TestResult, TestTableFactory};

#[test]
fn single_commit_two_add_files() -> TestResult {
    let mut factory = TestTableFactory::default();
    let schema = factory.schemas().simple().clone();
    let tt = factory.get_table("stats");

    let protocol = factory.create_protocol(1, 2, None, None);
    let metadata = factory.create_metadata(&schema, None, None);
    let batch = factory.generate_batch(
        &schema,
        100,
        HashMap::from_iter([("id".to_string(), ("1", "3"))]),
    )?;
    tt.write_commit(0, &[protocol, metadata], &[batch.clone(), batch.clone()])?;

    let expected_data = vec![batch.clone(), batch];
    let (table, engine) = tt.table();
    let scan = table.scan(&engine, None)?.build();

    let mut files = 0;
    let stream = scan.execute(&engine)?.into_iter().zip(expected_data);
    for (data, expected) in stream {
        files += 1;
        assert_eq!(data, expected);
    }
    assert_eq!(2, files, "Expected to have scanned two files");
    Ok(())
}

#[test]
fn two_commits() -> TestResult {
    let mut factory = TestTableFactory::default();
    let schema = factory.schemas().simple().clone();
    let tt = factory.get_table("stats");

    let protocol = factory.create_protocol(1, 2, None, None);
    let metadata = factory.create_metadata(&schema, None, None);
    let batch1 = factory.generate_batch(
        &schema,
        100,
        HashMap::from_iter([("id".to_string(), ("1", "3"))]),
    )?;
    tt.write_commit(0, &[protocol, metadata], &[batch1.clone()])?;

    let batch2 = factory.generate_batch(
        &schema,
        100,
        HashMap::from_iter([("id".to_string(), ("5", "7"))]),
    )?;
    tt.write_commit(1, &[], &[batch2.clone()])?;

    let expected_data = vec![batch2, batch1];
    let (table, engine) = tt.table();
    let scan = table.scan(&engine, None)?.build();

    let mut files = 0;
    let stream = scan.execute(&engine)?.into_iter().zip(expected_data);

    for (data, expected) in stream {
        files += 1;
        assert_eq!(data, expected);
    }
    assert_eq!(2, files, "Expected to have scanned two files");

    Ok(())
}

#[test]
fn remove_action() -> Result<(), Box<dyn std::error::Error>> {
    let mut factory = TestTableFactory::default();
    let schema = factory.schemas().simple().clone();
    let tt = factory.get_table("stats");

    let protocol = factory.create_protocol(1, 2, None, None);
    let metadata = factory.create_metadata(&schema, None, None);
    let batch1 = factory.generate_batch(
        &schema,
        100,
        HashMap::from_iter([("id".to_string(), ("1", "3"))]),
    )?;
    tt.write_commit(0, &[protocol, metadata], &[batch1.clone()])?;

    let batch2 = factory.generate_batch(
        &schema,
        100,
        HashMap::from_iter([("id".to_string(), ("5", "7"))]),
    )?;
    let adds = tt.write_commit(1, &[], &[batch2])?;
    let removes = tt.get_remove_actions(&adds);
    tt.write_commit(2, &removes, &[])?;

    let expected_data = vec![batch1];

    let (table, engine) = tt.table();
    let scan = table.scan(&engine, None)?.build();
    let stream = scan.execute(&engine)?.into_iter().zip(expected_data);

    let mut files = 0;
    for (data, expected) in stream {
        files += 1;
        assert_eq!(data, expected);
    }
    assert_eq!(1, files, "Expected to have scanned one file");
    Ok(())
}

#[test]
fn stats() -> TestResult {
    let mut factory = TestTableFactory::default();
    let schema = factory.schemas().simple().clone();
    let tt = factory.get_table("stats");

    let protocol = factory.create_protocol(1, 2, None, None);
    let metadata = factory.create_metadata(&schema, None, None);
    tt.write_commit(0, &[protocol, metadata], &[])?;

    let batch1 = factory.generate_batch(
        &schema,
        100,
        HashMap::from_iter([("id".to_string(), ("1", "3"))]),
    )?;
    let batch2 = factory.generate_batch(
        &schema,
        100,
        HashMap::from_iter([("id".to_string(), ("5", "7"))]),
    )?;
    tt.write_commit(1, &[], &[batch1.clone()])?;
    tt.write_commit(2, &[], &[batch2.clone()])?;

    let (table, engine) = tt.table();

    // The first file has id between 1 and 3; the second has id between 5 and 7. For each operator,
    // we validate the boundary values where we expect the set of matched files to change.
    //
    // NOTE: For cases that match both batch1 and batch2, we list batch2 first because log replay
    // returns most recently added files first.
    use BinaryOperator::{
        Equal, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, NotEqual,
    };
    let test_cases: Vec<(_, i64, _)> = vec![
        (Equal, 0, vec![]),
        (Equal, 1, vec![&batch1]),
        (Equal, 3, vec![&batch1]),
        (Equal, 4, vec![]),
        (Equal, 5, vec![&batch2]),
        (Equal, 7, vec![&batch2]),
        (Equal, 8, vec![]),
        (LessThan, 1, vec![]),
        (LessThan, 2, vec![&batch1]),
        (LessThan, 5, vec![&batch1]),
        (LessThan, 6, vec![&batch2, &batch1]),
        (LessThanOrEqual, 0, vec![]),
        (LessThanOrEqual, 1, vec![&batch1]),
        (LessThanOrEqual, 4, vec![&batch1]),
        (LessThanOrEqual, 5, vec![&batch2, &batch1]),
        (GreaterThan, 2, vec![&batch2, &batch1]),
        (GreaterThan, 3, vec![&batch2]),
        (GreaterThan, 6, vec![&batch2]),
        (GreaterThan, 7, vec![]),
        (GreaterThanOrEqual, 3, vec![&batch2, &batch1]),
        (GreaterThanOrEqual, 4, vec![&batch2]),
        (GreaterThanOrEqual, 7, vec![&batch2]),
        (GreaterThanOrEqual, 8, vec![]),
        (NotEqual, 0, vec![&batch2, &batch1]),
        (NotEqual, 1, vec![&batch2]),
        (NotEqual, 3, vec![&batch2]),
        (NotEqual, 4, vec![&batch2, &batch1]),
        (NotEqual, 5, vec![&batch1]),
        (NotEqual, 7, vec![&batch1]),
        (NotEqual, 8, vec![&batch2, &batch1]),
    ];
    for (op, value, expected_batches) in test_cases {
        let predicate = Expression::BinaryOperation {
            op,
            left: Box::new(Expression::column("id")),
            right: Box::new(Expression::literal(value)),
        };
        let scan = table.scan(&engine, None)?.with_predicate(predicate).build();

        let expected_files = expected_batches.len();
        let mut files_scanned = 0;
        let stream = scan.execute(&engine)?.into_iter().zip(expected_batches);

        for (batch, expected) in stream {
            files_scanned += 1;
            assert_eq!(&batch, expected);
        }
        assert_eq!(expected_files, files_scanned);
    }
    Ok(())
}

fn read_table_data(path: &str, expected: Vec<&str>) -> TestResult {
    let mut factory = TestTableFactory::default();
    let tt = factory.load_location("test", path);

    let (table, engine) = tt.table();
    let snapshot = table.snapshot(&engine, None)?;
    let scan = ScanBuilder::new(snapshot).build();

    let batches = scan.execute(&engine)?;
    let schema = batches[0].schema();
    let batch = concat_batches(&schema, &batches)?;

    assert_batches_sorted_eq!(&expected, &[batch]);
    Ok(())
}

#[test]
fn data() -> TestResult {
    let expected = vec![
        "+--------+--------+---------+",
        "| letter | number | a_float |",
        "+--------+--------+---------+",
        "|        | 6      | 6.6     |",
        "| a      | 1      | 1.1     |",
        "| a      | 4      | 4.4     |",
        "| b      | 2      | 2.2     |",
        "| c      | 3      | 3.3     |",
        "| e      | 5      | 5.5     |",
        "+--------+--------+---------+",
    ];
    read_table_data("./tests/data/basic_partitioned", expected)?;

    Ok(())
}

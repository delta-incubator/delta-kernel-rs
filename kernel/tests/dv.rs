//! Read a small table with/without deletion vectors.
//! Must run at the root of the crate
use arrow::util::pretty::print_batches;
use deltakernel::test_util::{TestResult, TestTableFactory};

#[test]
fn dv_table() -> TestResult {
    let mut factory = TestTableFactory::default();
    let tt = factory.load_table("test", "./tests/data/table-with-dv-small/");
    let (table, engine) = tt.table();
    let scan = table.scan(&engine, None)?.build();

    let stream = scan.execute(&engine)?;
    for batch in stream {
        let rows = batch.num_rows();
        print_batches(&[batch])?;
        assert_eq!(rows, 8);
    }
    Ok(())
}

#[test]
fn non_dv_table() -> TestResult {
    let mut factory = TestTableFactory::default();
    let tt = factory.load_table("test", "./tests/data/table-without-dv-small/");
    let (table, engine) = tt.table();
    let scan = table.scan(&engine, None)?.build();

    let stream = scan.execute(&engine)?;
    for batch in stream {
        let rows = batch.num_rows();
        print_batches(&[batch]).unwrap();
        assert_eq!(rows, 10);
    }
    Ok(())
}

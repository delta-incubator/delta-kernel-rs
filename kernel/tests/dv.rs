//! Read a small table with/without deletion vectors.
//! Must run at the root of the crate
use std::ops::Add;
use std::path::PathBuf;

use delta_kernel::engine::sync::SyncEngine;
use delta_kernel::scan::ScanResult;
use delta_kernel::{DeltaResult, Table};

use itertools::Itertools;
use test_log::test;

fn count_valid_rows(stream: impl Iterator<Item = DeltaResult<ScanResult>>) -> DeltaResult<usize> {
    stream
        .map_ok(|res| -> DeltaResult<_> {
            let data = res.raw_data?;
            let rows = data.length();
            let valid_rows = match res.mask {
                None => rows,
                Some(ref mask) => (0..rows).filter(|i| mask[*i]).count(),
            };
            Ok(valid_rows)
        })
        .flatten_ok()
        .fold_ok(0, Add::add)
}

#[test]
fn dv_table() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/"))?;
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = SyncEngine::new();

    let table = Table::new(url);
    let snapshot = table.snapshot(&engine, None)?;
    let scan = snapshot.into_scan_builder().build()?;

    let stream = scan.execute(&engine)?;
    let total_rows = count_valid_rows(stream)?;
    assert_eq!(total_rows, 8);
    Ok(())
}

#[test]
fn non_dv_table() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/"))?;
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = SyncEngine::new();

    let table = Table::new(url);
    let snapshot = table.snapshot(&engine, None)?;
    let scan = snapshot.into_scan_builder().build()?;

    let stream = scan.execute(&engine)?;
    let total_rows = count_valid_rows(stream)?;
    assert_eq!(total_rows, 10);
    Ok(())
}

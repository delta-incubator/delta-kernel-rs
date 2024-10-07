//! Read a small table with/without deletion vectors.
//! Must run at the root of the crate
use std::ops::Add;
use std::path::PathBuf;

use delta_kernel::engine::sync::SyncEngine;
use delta_kernel::scan::ScanResult;
use delta_kernel::{DeltaResult, Table};

use itertools::Itertools;
use test_log::test;

fn count_total_scan_rows(
    stream: impl Iterator<Item = DeltaResult<ScanResult>>,
) -> DeltaResult<usize> {
    stream
        .map(|sr_res| {
            let sr = sr_res?;
            let data = sr.raw_data?;
            // NOTE: The mask only suppresses rows for which it is both present and false.
            let deleted_rows = sr.mask.as_ref().map_or(0, |mask| {
                mask.iter().filter(|&&m| !m).count()
            });
            Ok(data.length() - deleted_rows)
        })
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
    let total_rows = count_total_scan_rows(stream)?;
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
    let total_rows = count_total_scan_rows(stream)?;
    assert_eq!(total_rows, 10);
    Ok(())
}

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
        .map(|res| -> DeltaResult<_> {
            res.and_then(|res| {
                let data = res.raw_data?;
                let rows = data.length();
                let valid_rows = res.mask.as_ref().map_or(rows, |mask| {
                    // [`ScanResult`] states that the mask may be *shorter* than the number of
                    // rows. Missing elements are considered true, so we include them in the count.
                    let missing_elems = rows - mask.len();
                    mask.iter().filter(|&&m| m).count() + missing_elems
                });
                Ok(valid_rows)
            })
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

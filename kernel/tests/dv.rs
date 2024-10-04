//! Read a small table with/without deletion vectors.
//! Must run at the root of the crate
use std::path::PathBuf;

use delta_kernel::engine::sync::SyncEngine;
use delta_kernel::scan::ScanResult;
use delta_kernel::Table;

use test_log::test;

fn count_total_rows(scan: Vec<ScanResult>) -> Result<usize, Box<dyn std::error::Error>> {
    let mut total_rows = 0;
    for sr in scan {
        let deleted_rows = match sr.raw_mask() {
            Some(raw_mask) => raw_mask.iter().filter(|&&m| !m).count(),
            None => 0,
        };
        total_rows += sr.raw_data?.length() - deleted_rows;
    }
    Ok(total_rows)
}

#[test]
fn dv_table() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/"))?;
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = SyncEngine::new();

    let table = Table::new(url);
    let snapshot = table.snapshot(&engine, None)?;
    let scan = snapshot.into_scan_builder().build()?;

    let total_rows = count_total_rows(scan.execute(&engine)?)?;
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
    let total_rows = count_total_rows(scan.execute(&engine)?)?;
    assert_eq!(total_rows, 10);
    Ok(())
}

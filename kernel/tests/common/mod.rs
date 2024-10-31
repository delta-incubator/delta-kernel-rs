use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use itertools::Itertools;

use crate::ArrowEngineData;
use delta_kernel::scan::Scan;
use delta_kernel::{DeltaResult, Engine, EngineData, Table};

pub(crate) fn to_arrow(data: Box<dyn EngineData>) -> DeltaResult<RecordBatch> {
    Ok(data
        .into_any()
        .downcast::<ArrowEngineData>()
        .map_err(|_| delta_kernel::Error::EngineDataType("ArrowEngineData".to_string()))?
        .into())
}

pub(crate) fn test_read(
    expected: &ArrowEngineData,
    table: &Table,
    engine: &impl Engine,
) -> Result<(), Box<dyn std::error::Error>> {
    let snapshot = table.snapshot(engine, None)?;
    let scan = snapshot.into_scan_builder().build()?;
    let batches = read_scan(&scan, engine)?;
    let formatted = pretty_format_batches(&batches).unwrap().to_string();

    let expected = pretty_format_batches(&[expected.record_batch().clone()])
        .unwrap()
        .to_string();

    println!("actual:\n{formatted}");
    println!("expected:\n{expected}");
    assert_eq!(formatted, expected);

    Ok(())
}

pub(crate) fn read_scan(scan: &Scan, engine: &dyn Engine) -> DeltaResult<Vec<RecordBatch>> {
    let scan_results = scan.execute(engine)?;
    scan_results
        .map(|scan_result| -> DeltaResult<_> {
            let scan_result = scan_result?;
            let mask = scan_result.full_mask();
            let data = scan_result.raw_data?;
            let record_batch = to_arrow(data)?;
            if let Some(mask) = mask {
                Ok(filter_record_batch(&record_batch, &mask.into())?)
            } else {
                Ok(record_batch)
            }
        })
        .try_collect()
}

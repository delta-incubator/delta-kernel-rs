use crate::ArrowEngineData;
use arrow::record_batch::RecordBatch;
use delta_kernel::{DeltaResult, Engine, EngineData, Table};

pub(crate) fn to_arrow(data: Box<dyn EngineData>) -> DeltaResult<RecordBatch> {
    Ok(data
        .into_any()
        .downcast::<ArrowEngineData>()
        .map_err(|_| delta_kernel::Error::EngineDataType("ArrowEngineData".to_string()))?
        .into())
}

// going to unify across read/write
pub(crate) fn test_read(
    expected: &ArrowEngineData,
    table: &Table,
    engine: &impl Engine,
) -> Result<(), Box<dyn std::error::Error>> {
    let snapshot = table.snapshot(engine, None)?;
    let scan = snapshot.into_scan_builder().build()?;
    let actual = scan.execute(engine)?;
    let batches: Vec<RecordBatch> = actual
        .into_iter()
        .map(|res| {
            let data = res.unwrap().raw_data.unwrap();
            let record_batch: RecordBatch = data
                .into_any()
                .downcast::<ArrowEngineData>()
                .unwrap()
                .into();
            record_batch
        })
        .collect();

    let formatted = arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string();

    let expected = arrow::util::pretty::pretty_format_batches(&[expected.record_batch().clone()])
        .unwrap()
        .to_string();

    println!("actual:\n{formatted}");
    println!("expected:\n{expected}");
    assert_eq!(formatted, expected);

    Ok(())
}

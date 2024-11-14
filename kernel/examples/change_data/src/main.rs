use arrow::{compute::filter_record_batch, util::pretty::print_batches};
use arrow_array::RecordBatch;
use delta_kernel::{
    engine::{arrow_data::ArrowEngineData, sync::SyncEngine},
    DeltaResult, Table,
};
use itertools::Itertools;

fn main() -> DeltaResult<()> {
    let uri = "./table-with-dv-small";
    // build a table and get the lastest snapshot from it
    let table = Table::try_from_uri(uri)?;

    let engine = SyncEngine::new();

    let table_changes = table.table_changes(&engine, 0, None)?;
    //let schema = table_changes
    //    .schema
    //    .project(&["value", "_commit_timestamp", "_change_type"])?;
    let x = table_changes
        .into_scan_builder()
        //.with_schema(schema)
        .build()?;
    let batches: Vec<RecordBatch> = x
        .execute(&engine)?
        .map(|scan_result| -> DeltaResult<_> {
            let scan_result = scan_result?;
            let mask = scan_result.full_mask();
            let data = scan_result.raw_data?;
            let record_batch: RecordBatch = data
                .into_any()
                .downcast::<ArrowEngineData>()
                .map_err(|_| delta_kernel::Error::EngineDataType("ArrowEngineData".to_string()))?
                .into();
            if let Some(mask) = mask {
                Ok(filter_record_batch(&record_batch, &mask.into())?)
            } else {
                Ok(record_batch)
            }
        })
        .try_collect()?;
    print_batches(&batches)?;

    Ok(())
}

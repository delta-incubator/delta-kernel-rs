use std::{collections::HashMap, sync::Arc};

use arrow_array::RecordBatch;
use delta_kernel::{
    engine::{
        arrow_data::ArrowEngineData,
        default::{executor::tokio::TokioBackgroundExecutor, DefaultEngine},
    },
    scan::ScanResult,
    DeltaResult, EngineData, Table,
};
use itertools::Itertools;

fn into_record_batch(engine_data: DeltaResult<Box<dyn EngineData>>) -> DeltaResult<RecordBatch> {
    engine_data
        .and_then(ArrowEngineData::try_from_engine_data)
        .map(Into::into)
}
fn main() -> DeltaResult<()> {
    let uri =
        "/Users/oussama.saoudi/delta-kernel-rs/kernel/examples/change_data/deltalog-getChanges";
    // build a table and get the lastest snapshot from it
    let table = Table::try_from_uri(uri)?;

    let engine = DefaultEngine::try_new(
        table.location(),
        HashMap::<String, String>::new(),
        Arc::new(TokioBackgroundExecutor::new()),
    )?;

    let table_changes = table.table_changes(&engine, 0, None)?;
    let x = table_changes.into_scan_builder().build()?;
    let vec: Vec<ScanResult> = x.execute(&engine)?.try_collect()?;
    for res in vec {
        println!("{:?}", into_record_batch(res.raw_data)?)
    }

    Ok(())
}

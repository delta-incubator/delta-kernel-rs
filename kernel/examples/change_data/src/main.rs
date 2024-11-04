use std::{collections::HashMap, sync::Arc};

use delta_kernel::{
    engine::default::{executor::tokio::TokioBackgroundExecutor, DefaultEngine},
    DeltaResult, Table,
};

fn main() -> DeltaResult<()> {
    let uri = "/Users/oussama.saoudi/delta-kernel-rs/kernel/tests/data/table-with-dv-small/";
    // build a table and get the lastest snapshot from it
    let table = Table::try_from_uri(uri)?;

    let engine = DefaultEngine::try_new(
        table.location(),
        HashMap::<String, String>::new(),
        Arc::new(TokioBackgroundExecutor::new()),
    )?;

    let table_changes = table.table_changes(&engine, 0, None)?;
    println!("table_changes: {:?}", table_changes);

    Ok(())
}

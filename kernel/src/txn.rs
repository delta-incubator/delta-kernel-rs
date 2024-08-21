use std::sync::Arc;

use url::Url;

use crate::{DeltaResult, Engine, EngineData};
use crate::snapshot::Snapshot;

pub struct TransactionBuilder {
    table_location: Url,
}

impl TransactionBuilder {
    pub fn new(table_location: Url) -> Self {
        Self { table_location }
    }

    /// create the transaction. reads the latest snapshot of the table to get table metadata.
    pub fn build(self, engine: &dyn Engine) -> DeltaResult<Transaction> {
        let latest_snapshot = Snapshot::try_new(self.table_location.clone(), engine, None)?;
        Ok(Transaction::new(self.table_location, latest_snapshot))
    }
}

pub struct Transaction {
    table_location: Url,
    latest_snapshot: Arc<Snapshot>,
    write_metadata: Option<Box<dyn EngineData>>,
}

impl Transaction {
    pub fn new(table_location: Url, latest_snapshot: impl Into<Arc<Snapshot>>) -> Self {
        Self {
            table_location,
            latest_snapshot: latest_snapshot.into(),
            write_metadata: None,
        }
    }

    pub fn write(&mut self, engine: &dyn Engine, data: Box<dyn EngineData>) -> DeltaResult<()> {
        // transform to physical
        // let physical_data = transform_to_physical(engine, data.as_ref())?;
        // write the parquet file
        let parquet_handler = engine.get_parquet_handler();
        // parquet_handler.write_parquet_files(&self.table_location, physical_data)?;
        let write_metadata = parquet_handler.write_parquet_files(&self.table_location, data)?;
        // FIXME implement how to combine write metadatas
        self.write_metadata = Some(write_metadata.into());
        Ok(())
    }

    pub fn commit(self, engine: &dyn Engine) -> DeltaResult<()> {
        let json_handler = engine.get_json_handler();
        let commit_file_name = format!("{:020}", &self.latest_snapshot.version() + 1) + ".json";
        let commit_path = &self.table_location.join("_delta_log/")?.join(&commit_file_name)?;
        // fixme
        let write_metadata = self.write_metadata.expect("no write metadata");
        json_handler.put_json(commit_path, write_metadata)?;
        Ok(())
    }
}

// for now do the dumb thing and just copy the
fn transform_to_physical(
    engine: &dyn Engine,
    data: &dyn EngineData,
) -> DeltaResult<Box<dyn EngineData>> {
    // transform logical data to physical data
    // 1. remove partition columns
    // 2. column mapping
    // 3. generated columns, default values, row ids
    // let expr_handler = engine.get_expression_handler();
    // let physical_data = expr_handler.get_evaluator().evaluate(data)?;
    unimplemented!()
}

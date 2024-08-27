use std::sync::Arc;

// use delta_kernel_derive::Schema;
use url::Url;
use tracing::info;

use crate::snapshot::Snapshot;
use crate::{DeltaResult, Engine, EngineData};

// derive schema?
#[derive(Debug)]
pub struct WriteContext {
    pub target_directory: Url,
}

impl WriteContext {
    fn new(target_directory: &Url) -> Self {
        Self {
            target_directory: target_directory.clone()
        }
    }
}

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
        Ok(Transaction::new(latest_snapshot))
    }
}

pub struct Transaction {
    read_snapshot: Arc<Snapshot>,
    // TODO rename
    write_metadata: Option<Box<dyn EngineData>>
}

impl Transaction {
    pub fn new(snapshot: impl Into<Arc<Snapshot>>) -> Self {
        Self {
            read_snapshot: snapshot.into(),
            write_metadata: None,
        }
    }

    /// get the write context for this transaction
    // should this be WriteContext or DeltaResult<Box<dyn EngineData>>
    // TODO need engine?
    pub fn write_context(&self, _engine: &dyn Engine) -> WriteContext {
        let target_dir = self.read_snapshot.table_root();
        WriteContext::new(target_dir)
    }

    /// add write metadata to the transaction, where the metadata has specific schema (link)
    /// for now this will overwrite any previous write metadata
    /// TODO this should actually stream the data
    pub fn add_write_metadata(&mut self, data: Box<dyn EngineData>) {
        self.write_metadata = Some(data);
    }

    // TODO conflict resolution
    pub fn commit(self, engine: &dyn Engine) -> DeltaResult<()> {
        let json_handler = engine.get_json_handler();
        let commit_file_name = format!("{:020}", &self.read_snapshot.version() + 1) + ".json";
        let commit_path = &self
            .read_snapshot
            .table_root
            .join("_delta_log/")?
            .join(&commit_file_name)?;
        if let Some(write_metadata) = self.write_metadata {
            json_handler.put_json(commit_path, write_metadata)?;
        } else {
            info!("No writes to commit");
        }
        Ok(())
    }
}

// fn transform_to_physical(
//     engine: &dyn Engine,
//     data: &dyn EngineData,
// ) -> DeltaResult<Box<dyn EngineData>> {
//     // transform logical data to physical data
//     // 1. remove partition columns
//     // 2. column mapping
//     // 3. generated columns, default values, row ids
//     // let expr_handler = engine.get_expression_handler();
//     // let physical_data = expr_handler.get_evaluator().evaluate(data)?;
//     unimplemented!()
// }

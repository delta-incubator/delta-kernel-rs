use std::sync::Arc;

// use delta_kernel_derive::Schema;
use tracing::info;
use url::Url;

use crate::schema::SchemaRef;
use crate::snapshot::Snapshot;
use crate::Expression;
use crate::{DeltaResult, Engine, EngineData};

// derive schema?
#[derive(Debug)]
pub struct WriteContext {
    pub target_directory: Url,
    snapshot_schema: SchemaRef,
    partition_columns: Vec<String>,
}

impl WriteContext {
    fn new(
        target_directory: &Url,
        snapshot_schema: SchemaRef,
        partition_columns: Vec<String>,
    ) -> Self {
        Self {
            target_directory: target_directory.clone(),
            snapshot_schema,
            partition_columns,
        }
    }

    // or should we have this spit out an expression that the engine can apply themselves??
    pub fn transform_to_physical(
        &self,
        engine: &dyn Engine,
        data: &dyn EngineData,
    ) -> DeltaResult<Box<dyn EngineData>> {
        // transform logical data to physical data
        // 1. remove partition columns (or leave them for iceberg compat)
        // 2. TODO column mapping
        // 3. LATER generated columns, default values, row ids

        let fields = self
            .snapshot_schema
            .fields()
            .filter_map(|f| {
                if self.partition_columns.contains(&f.name().to_string()) {
                    None
                } else {
                    Some(Expression::Column(f.name().to_string()))
                }
            })
            .collect();
        let expr = Expression::Struct(fields);
        let cols = self
            .snapshot_schema
            .fields()
            .filter_map(|f| {
                if self.partition_columns.contains(&f.name().to_string()) {
                    None
                } else {
                    Some(f.name())
                }
            })
            .collect::<Vec<_>>();
        let physical_schema = self
            .snapshot_schema
            .project(&cols)
            .expect("schema projection");
        engine
            .get_expression_handler()
            .get_evaluator(self.snapshot_schema.clone(), expr, physical_schema.into())
            .evaluate(data)
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
    write_metadata: Option<Box<dyn EngineData>>,
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
    pub fn write_context(&self, _engine: &dyn Engine, partition_cols: Vec<String>) -> WriteContext {
        let target_dir = self.read_snapshot.table_root();
        let snapshot_schema = self.read_snapshot.schema();
        WriteContext::new(
            target_dir,
            Arc::new(snapshot_schema.clone()),
            partition_cols,
        )
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

use std::sync::Arc;

use arrow::array::StringArray;
use arrow::record_batch::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use arrow_schema::{DataType as ArrowDataType, Field};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;
use serde_json::{json, to_vec};
use url::Url;

use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use delta_kernel::{Error as KernelError, Table};

// setup default engine with in-memory object store.
fn setup(
    table_name: &str,
) -> (
    Arc<dyn ObjectStore>,
    DefaultEngine<TokioBackgroundExecutor>,
    Url,
) {
    let table_root_path = Path::from(format!("/{table_name}"));
    let url = Url::parse(&format!("memory:///{}/", table_root_path)).unwrap();
    let storage = Arc::new(InMemory::new());
    (
        storage.clone(),
        DefaultEngine::new(
            storage,
            table_root_path,
            Arc::new(TokioBackgroundExecutor::new()),
        ),
        url,
    )
}

// we provide this table creation function since we only do appends to existing tables for now.
// this will just create an empty table with the given schema. (just protocol + metadata actions)
async fn create_table(
    store: Arc<dyn ObjectStore>,
    table_path: Url,
    schema: SchemaRef,
    partition_columns: &[&str],
) -> Result<Table, Box<dyn std::error::Error>> {
    let table_id = "test_id";
    let schema = serde_json::to_string(&schema)?;

    let protocol = json!({
        "protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": [],
            "writerFeatures": []
        }
    });
    let metadata = json!({
        "metaData": {
            "id": table_id,
            "format": {
                "provider": "parquet",
                "options": {}
            },
            "schemaString": schema,
            "partitionColumns": partition_columns,
            "configuration": {},
            "createdTime": 1677811175819u64
        }
    });

    let data = [
        to_vec(&protocol).unwrap(),
        b"\n".to_vec(),
        to_vec(&metadata).unwrap(),
    ]
    .concat();

    // put 0.json with protocol + metadata
    let path = table_path.path();
    let path = format!("{path}_delta_log/00000000000000000000.json");
    store.put(&Path::from(path), data.into()).await?;
    Ok(Table::new(table_path))
}

#[tokio::test]
async fn test_commit_info() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();
    // setup in-memory object store and default engine
    let (store, engine, table_location) = setup("test_table");

    // create a simple table: one int column named 'number'
    let schema = Arc::new(StructType::new(vec![StructField::new(
        "number",
        DataType::INTEGER,
        true,
    )]));
    let table = create_table(store.clone(), table_location, schema, &[]).await?;

    // create a transaction
    let mut txn = table.new_transaction(&engine)?;

    // add commit info of the form {engineInfo: "default engine"}
    let commit_info_schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "engineInfo",
        ArrowDataType::Utf8,
        true,
    )]));
    let commit_info_batch = RecordBatch::try_new(
        commit_info_schema.clone(),
        vec![Arc::new(StringArray::from(vec!["default engine"]))],
    )?;
    txn.commit_info(
        Box::new(ArrowEngineData::new(commit_info_batch)),
        commit_info_schema.try_into()?,
    );

    // commit!
    txn.commit(&engine)?;

    let commit1 = store
        .get(&Path::from(
            "/test_table/_delta_log/00000000000000000001.json",
        ))
        .await?;
    assert_eq!(
        String::from_utf8(commit1.bytes().await?.to_vec())?,
        "{\"commitInfo\":{\"kernelVersion\":\"v0.3.1\",\"engineInfo\":\"default engine\"}}\n"
    );

    // one null row commit info
    let mut txn = table.new_transaction(&engine)?;
    let commit_info_schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "some_column_name",
        ArrowDataType::Utf8,
        true,
    )]));
    let commit_info_batch = RecordBatch::try_new(
        commit_info_schema.clone(),
        vec![Arc::new(StringArray::new_null(1))],
    )?;
    txn.commit_info(
        Box::new(ArrowEngineData::new(commit_info_batch)),
        commit_info_schema.try_into()?,
    );

    // commit!
    txn.commit(&engine)?;

    let commit1 = store
        .get(&Path::from(
            "/test_table/_delta_log/00000000000000000002.json",
        ))
        .await?;
    assert_eq!(
        String::from_utf8(commit1.bytes().await?.to_vec())?,
        "{\"commitInfo\":{\"kernelVersion\":\"v0.3.1\"}}\n"
    );
    Ok(())
}

#[tokio::test]
async fn test_empty_commit() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();
    // setup in-memory object store and default engine
    let (store, engine, table_location) = setup("test_table");

    // create a simple table: one int column named 'number'
    let schema = Arc::new(StructType::new(vec![StructField::new(
        "number",
        DataType::INTEGER,
        true,
    )]));
    let table = create_table(store.clone(), table_location, schema, &[]).await?;

    // create a transaction and commit
    table.new_transaction(&engine)?.commit(&engine)?;

    let commit1 = store
        .get(&Path::from(
            "/test_table/_delta_log/00000000000000000001.json",
        ))
        .await?;
    assert!(commit1.bytes().await?.to_vec().is_empty());
    Ok(())
}

#[tokio::test]
async fn test_invalid_commit_info() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();
    // setup in-memory object store and default engine
    let (store, engine, table_location) = setup("test_table");

    // create a simple table: one int column named 'number'
    let schema = Arc::new(StructType::new(vec![StructField::new(
        "number",
        DataType::INTEGER,
        true,
    )]));
    let table = create_table(store.clone(), table_location, schema, &[]).await?;

    // empty commit info test
    let mut txn = table.new_transaction(&engine)?;
    let commit_info_schema = Arc::new(ArrowSchema::empty());
    let commit_info_batch = RecordBatch::new_empty(commit_info_schema.clone());
    assert!(commit_info_batch.num_rows() == 0);
    txn.commit_info(
        Box::new(ArrowEngineData::new(commit_info_batch)),
        commit_info_schema.try_into()?,
    );

    // commit!
    assert!(matches!(
        txn.commit(&engine),
        Err(KernelError::InvalidCommitInfo(_))
    ));

    // two-row commit info test
    let mut txn = table.new_transaction(&engine)?;
    let commit_info_schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "engineInfo",
        ArrowDataType::Utf8,
        true,
    )]));
    let commit_info_batch = RecordBatch::try_new(
        commit_info_schema.clone(),
        vec![Arc::new(StringArray::from(vec![
            "row1: default engine",
            "row2: default engine",
        ]))],
    )?;

    txn.commit_info(
        Box::new(ArrowEngineData::new(commit_info_batch)),
        commit_info_schema.try_into()?,
    );

    // commit!
    assert!(matches!(
        txn.commit(&engine),
        Err(KernelError::InvalidCommitInfo(_))
    ));
    Ok(())
}

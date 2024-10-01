use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_schema::Field;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;
use url::Url;

use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use delta_kernel::{DeltaResult, Engine, EngineData, Table};

// fixme use in macro below
// const PROTOCOL_METADATA_TEMPLATE: &'static str = r#"{{"protocol":{{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":[],"writerFeatures":[]}}}}
// {{"metaData":{{"id":"{}","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{}","partitionColumns":[],"configuration":{{}},"createdTime":1677811175819}}}}"#;

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
    // fixme use this schema
    _schema: SchemaRef,
    schema_string: &str,
    partition_columns: &str,
) -> Result<Table, Box<dyn std::error::Error>> {
    // put 0.json with protocol + metadata
    let table_id = "test_id";
    let data = format!(
        r#"{{"protocol":{{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":[],"writerFeatures":[]}}}}
{{"metaData":{{"id":"{}","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{}","partitionColumns":[{}],"configuration":{{}},"createdTime":1677811175819}}}}"#,
        table_id, schema_string, partition_columns
    ).into_bytes();
    let path = table_path.path();
    let path = format!("{path}_delta_log/00000000000000000000.json");
    println!("putting to path: {}", path);
    store.put(&Path::from(path), data.into()).await?;
    Ok(Table::new(table_path))
}

// FIXME delete/unify from default/parquet.rs
fn get_metadata_schema() -> Arc<arrow_schema::Schema> {
    let path_field = Field::new("path", arrow_schema::DataType::Utf8, false);
    let size_field = Field::new("size", arrow_schema::DataType::Int64, false);
    let partition_field = Field::new(
        "partitionValues",
        arrow_schema::DataType::Map(
            Arc::new(Field::new(
                "entries",
                arrow_schema::DataType::Struct(
                    vec![
                        Field::new("keys", arrow_schema::DataType::Utf8, false),
                        Field::new("values", arrow_schema::DataType::Utf8, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        ),
        false,
    );
    let data_change_field = Field::new("dataChange", arrow_schema::DataType::Boolean, false);
    let modification_time_field =
        Field::new("modificationTime", arrow_schema::DataType::Int64, false);

    Arc::new(arrow_schema::Schema::new(vec![Field::new(
        "add",
        arrow_schema::DataType::Struct(
            vec![
                path_field.clone(),
                size_field.clone(),
                partition_field.clone(),
                data_change_field.clone(),
                modification_time_field.clone(),
            ]
            .into(),
        ),
        false,
    )]))
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
    let schema_string = r#"{\"type\":\"struct\",\"fields\":[{\"name\":\"number\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}"#;
    let table = create_table(
        store.clone(),
        table_location,
        schema.clone(),
        schema_string,
        "",
    )
    .await?;
    // println!(
    //     "{:?}",
    //     store
    //         .get(&Path::from(
    //             "/test_table/_delta_log/00000000000000000000.json"
    //         ))
    //         .await
    // );
    // append an arrow record batch (vec of record batches??)
    let mut txn = table.new_transaction(&engine)?;
    txn.commit(&engine)?;
    let commit1 = store
        .get(&Path::from(
            "/test_table/_delta_log/00000000000000000001.json",
        ))
        .await?;
    println!("commit1: {}", String::from_utf8(commit1.bytes().await?.to_vec())?);
    Ok(())
}

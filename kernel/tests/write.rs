use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_schema::Field;
use futures::future::join_all;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;
use url::Url;

use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::parquet::DefaultParquetHandler;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use delta_kernel::{Engine, Table};

// fixme use in macro below
// const PROTOCOL_METADATA_TEMPLATE: &'static str = r#"{{"protocol":{{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":[],"writerFeatures":[]}}}}
// {{"metaData":{{"id":"{}","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{}","partitionColumns":[],"configuration":{{}},"createdTime":1677811175819}}}}"#;

// setup default engine with in-memory object store.
fn setup(storage_prefix: Path) -> (Arc<dyn ObjectStore>, DefaultEngine<TokioBackgroundExecutor>) {
    let storage = Arc::new(InMemory::new());
    (
        storage.clone(),
        DefaultEngine::new(
            storage,
            storage_prefix,
            Arc::new(TokioBackgroundExecutor::new()),
        ),
    )
}

// we provide this table creation function since we only do appends to existing tables for now.
// this will just create an empty table with the given schema. (just protocol + metadata actions)
async fn create_table(
    store: Arc<dyn ObjectStore>,
    table_path: Url,
    // fixme use this schema
    _schema: SchemaRef,
) -> Result<Table, Box<dyn std::error::Error>> {
    // put 0.json with protocol + metadata
    let table_id = "test_id";
    let schema_string = r#"{\"type\":\"struct\",\"fields\":[{\"name\":\"number\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}"#;
    let data = format!(
        r#"{{"protocol":{{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":[],"writerFeatures":[]}}}}
{{"metaData":{{"id":"{}","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{}","partitionColumns":[],"configuration":{{}},"createdTime":1677811175819}}}}"#,
        table_id, schema_string
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

// tests
// 1. basic happy path append
// 2. append with schema mismatch
// 3. append with partitioned table

#[tokio::test]
async fn basic_append() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();

    // setup in-memory object store and default engine
    let table_location = Url::parse("memory:///test_table/").unwrap();
    println!("table_location: {}", table_location);
    // prefix is just "/" since we are using in-memory object store - don't really care about
    // collisions TODO check?? lol
    let (store, engine) = setup(Path::from("memory://"));

    // create a simple table: one int column named 'number'
    let schema = Arc::new(StructType::new(vec![StructField::new(
        "number",
        DataType::INTEGER,
        true,
    )]));
    let table = create_table(store.clone(), table_location, schema.clone()).await?;

    // println!(
    //     "{:?}",
    //     store
    //         .get(&Path::from(
    //             "/test_table/_delta_log/00000000000000000000.json"
    //         ))
    //         .await
    // );

    // append an arrow record batch (vec of record batches??)
    let append_data = RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into()?),
        vec![Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3]))],
    )?;
    let append_data = Box::new(ArrowEngineData::new(append_data));

    let append_data2 = RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into()?),
        vec![Arc::new(arrow::array::Int32Array::from(vec![4, 5, 6]))],
    )?;
    let append_data2 = Box::new(ArrowEngineData::new(append_data2));

    // create a new txn based on current table version
    let txn_builder = table.new_transaction_builder();
    let mut txn = txn_builder.build(&engine).expect("build txn");
    let write_context1 = txn.write_context(&engine);
    let write_context2 = txn.write_context(&engine);

    // create a new async task to do the write (simulate executors)
    let pq1 = engine.get_parquet_handler();
    let pq2 = pq1.clone(); // cheap
    let writers = [
        (append_data, write_context1, pq1),
        (append_data2, write_context2, pq2),
    ]
    .into_iter()
    .map(|(data, write_context, pq)| {
        tokio::task::spawn(async move {
            // transform to logical then write to object store and give back metadata
            let pq = pq
                .as_any()
                .downcast_ref::<DefaultParquetHandler<TokioBackgroundExecutor>>()
                .unwrap();
            pq.write_parquet_files(&write_context.target_directory, data)
                .await
                .expect("write parquet data")
        })
    })
    .collect::<Vec<_>>();

    // wait for writes to complete
    let metadata = join_all(writers).await;
    let metadata = metadata
        .into_iter()
        .map(|data_result| {
            RecordBatch::from(ArrowEngineData::try_from_engine_data(data_result.unwrap()).unwrap())
        })
        .collect::<Vec<_>>();

    // FIXME
    let metadata_schema = get_metadata_schema();

    // concat all the metadata into single record batch
    let metadata = arrow::compute::concat_batches(&metadata_schema, &metadata)?;

    txn.add_write_metadata(Box::new(ArrowEngineData::new(metadata)));
    txn.commit(&engine)?;

    // let commit1 = store
    //     .get(&Path::from(
    //         "/test_table/_delta_log/00000000000000000001.json",
    //     ))
    //     .await?;
    // println!("{:?}", commit1);
    // println!("{}", String::from_utf8(commit1.bytes().await?.to_vec())?);

    let expected_data = RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into()?),
        vec![Arc::new(arrow::array::Int32Array::from(vec![
            1, 2, 3, 4, 5, 6,
        ]))],
    )?;
    let expected = Box::new(ArrowEngineData::new(expected_data));
    test_read(expected, table, &engine)?;

    Ok(())
}

fn test_read(
    expected: Box<ArrowEngineData>,
    table: Table,
    engine: &impl Engine,
) -> Result<(), Box<dyn std::error::Error>> {
    let snapshot = table.snapshot(engine, None)?;

    // println!("{:?}", snapshot);

    let scan = snapshot.into_scan_builder().build()?;

    let actual = scan.execute(engine)?;
    let batches: Vec<RecordBatch> = actual
        .into_iter()
        .map(|res| {
            let data = res.raw_data.unwrap();
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

    assert_eq!(formatted, expected);
    println!("expected:\n{expected}");
    println!("got:\n{formatted}");

    Ok(())
}

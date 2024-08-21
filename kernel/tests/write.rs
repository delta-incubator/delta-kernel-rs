use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;
use url::Url;

use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use delta_kernel::{Engine, Table};

// fixme use in macro below
const PROTOCOL_METADATA_TEMPLATE: &'static str = r#"{{"protocol":{{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":[],"writerFeatures":[]}}}}
{{"metaData":{{"id":"{}","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{}","partitionColumns":[],"configuration":{{}},"createdTime":1677811175819}}}}"#;

// setup default engine with in-memory object store.
fn setup(storage_prefix: Path) -> (Arc<dyn ObjectStore>, impl Engine) {
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

    println!(
        "{:?}",
        store
            .get(&Path::from(
                "/test_table/_delta_log/00000000000000000000.json"
            ))
            .await
    );

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

    // // create a new async task to do the write (simulate executors)
    // let writer = tokio::task::spawn(async {
    //     // write will transform logical data to physical data and write to object store
    //     txn.write(append_data).expect("write data files");
    // });
    txn.write(&engine, append_data).expect("write append_data");
    txn.write(&engine, append_data2)
        .expect("write append_data2");

    // wait for writes to complete
    // let _ = writer.await?;

    txn.commit(&engine)?;

    let commit1 =
        store
            .get(&Path::from(
                "/test_table/_delta_log/00000000000000000001.json"
            ))
            .await?;
    println!(
        "{:?}", commit1
    );

    println!("{}", String::from_utf8(commit1.bytes().await?.to_vec())?);

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

    println!("{:?}", snapshot);
    // println!("snapshot files {:?}", snapshot);

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

    println!("expected:\n{expected}");
    println!("got:\n{formatted}");

    Ok(())
}

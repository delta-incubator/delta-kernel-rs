use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
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
use delta_kernel::{DeltaResult, Engine, EngineData, Table};

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

// tests
// 1. basic happy path append
// 2. partition append
// 3. append with schema mismatch
// 4. commit with conflict
/*
#[tokio::test]
async fn append_basic() -> Result<(), Box<dyn std::error::Error>> {
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
    let write_context1 = txn.write_context();
    let write_context2 = txn.write_context();

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
        .map(|write_metadata| {
            let write_metadata = write_metadata.unwrap();
            RecordBatch::from(
                ArrowEngineData::try_from_engine_data(
                    create_write_metadata(
                        &write_metadata.path,
                        write_metadata.size as i64,
                        vec![],
                        true,
                        1724265056,
                    )
                    .unwrap(),
                )
                .unwrap(),
            )
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
*/

#[tokio::test]
async fn append_partitioned() -> Result<(), Box<dyn std::error::Error>> {
    use futures::stream::StreamExt;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();

    // setup in-memory object store and default engine
    let table_location = Url::parse("memory:///test_table/").unwrap();
    let (store, engine) = setup(Path::from("memory://"));

    // create a simple table: int column named 'part', int col named 'number'
    let schema = Arc::new(StructType::new(vec![
        StructField::new("part", DataType::INTEGER, true),
        StructField::new("number", DataType::INTEGER, true),
    ]));
    let schema_string = r#"{\"type\":\"struct\",\"fields\":[{\"name\":\"part\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"number\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}"#;
    let table = create_table(
        store.clone(),
        table_location,
        schema.clone(),
        schema_string,
        "\"part\"",
    )
    .await?;

    // append an arrow record batch (vec of record batches??)
    let append_data = RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into()?),
        vec![
            Arc::new(arrow::array::Int32Array::from(vec![1, 1, 1])),
            Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3])),
        ],
    )?;
    let append_data = Box::new(ArrowEngineData::new(append_data));

    let append_data2 = RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into()?),
        vec![
            Arc::new(arrow::array::Int32Array::from(vec![2, 2, 2])),
            Arc::new(arrow::array::Int32Array::from(vec![4, 5, 6])),
        ],
    )?;
    let append_data2 = Box::new(ArrowEngineData::new(append_data2));

    // create a new txn based on current table version
    let txn_builder = table.new_transaction_builder();
    let mut txn = txn_builder.build(&engine).expect("build txn");
    let write_context1 = txn.write_context();
    let write_context2 = txn.write_context();

    // create a new async task to do the write (simulate executors)
    let pq1 = engine.get_parquet_handler();
    let pq2 = pq1.clone(); // cheap
    let e = Arc::new(engine);
    let writers = [
        (append_data, write_context1, pq1, e.clone(), ("part", 1)),
        (append_data2, write_context2, pq2, e.clone(), ("part", 2)),
    ]
    .into_iter()
    .map(|(data, write_context, pq, arc_engine, partition)| {
        tokio::task::spawn(async move {
            // transform to logical then write to object store and give back metadata
            let pq = pq
                .as_any()
                .downcast_ref::<DefaultParquetHandler<TokioBackgroundExecutor>>()
                .unwrap();
            // TODO or should we just give the engine an expression and let them do transform
            let data = write_context.transform_to_physical(arc_engine.as_ref(), data.as_ref());
            let metadata = pq
                .write_parquet_files(&write_context.target_directory, data.unwrap())
                .await
                .expect("write parquet data");

            (metadata, partition)
        })
    })
    .collect::<Vec<_>>();

    // wait for writes to complete
    let metadata = join_all(writers).await;

    let metadata_iter = metadata.into_iter().filter_map(|metadata_result| {
        let (write_metadata, partition) = match metadata_result {
            Ok(data) => data,
            Err(e) => {
                eprintln!("Error unwrapping metadata: {:?}", e);
                return None;
            }
        };

        let (partition_key, partition_val) = partition;
        let partition_key = partition_key.to_string();
        let partition_val = partition_val.to_string();

        let engine_data_result = create_write_metadata(
            &write_metadata.path,
            write_metadata.size as i64,
            vec![(partition_key, partition_val)],
            true,
            1724265056,
        );

        match engine_data_result {
            Ok(engine_data) => Some(engine_data),
            Err(e) => {
                eprintln!("Error creating write metadata: {:?}", e);
                None
            }
        }
    });

    let metadata_iter: Box<dyn Iterator<Item = Box<dyn EngineData>> + Send> =
        Box::new(metadata_iter);

    txn.add_write_metadata(metadata_iter);

    // make a simple record batch of {"engineInfo": "delta_kernel/default"}
    let commit_info = RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(vec![Field::new(
            "engineInfo",
            arrow_schema::DataType::Utf8,
            true,
        )])),
        vec![Arc::new(arrow::array::StringArray::from(vec![
            "delta_kernel/default",
        ]))],
    )?;
    txn.commit_info(Box::new(ArrowEngineData::new(commit_info)));
    txn.commit(e.clone().as_ref())?;
    let commit1 = store
        .get(&Path::from(
            "/test_table/_delta_log/00000000000000000001.json",
        ))
        .await?;
    println!(
        "COMMIT {}",
        String::from_utf8(commit1.bytes().await?.to_vec())?
    );

    // pick out a random parquet file and check that it doesn't have the 'part' column materialized
    let mut list = store.list(Some(&Path::from("/test_table/")));
    while let Some(meta) = list.next().await {
        let path = meta.unwrap().location;
        if path.extension() == Some("parquet") {
            let data = store.get(&path).await?.bytes().await?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(data).unwrap();
            assert!(builder
                .schema()
                .fields()
                .iter()
                .all(|field| { field.name() != "part" }));
            break;
        }
    }

    let expected_data = RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into()?),
        vec![
            Arc::new(arrow::array::Int32Array::from(vec![1, 1, 1, 2, 2, 2])),
            Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
        ],
    )?;
    let expected = Box::new(ArrowEngineData::new(expected_data));
    test_read(expected, table, e.as_ref())?;

    Ok(())
}

/*
#[tokio::test]
async fn commit_conflict() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();
    // setup in-memory object store and default engine
    let table_location = Url::parse("memory:///test_table/").unwrap();
    let (store, engine) = setup(Path::from("memory://"));

    // create a simple table: one int column named 'number'
    let schema = Arc::new(StructType::new(vec![StructField::new(
        "number",
        DataType::INTEGER,
        true,
    )]));
    let schema_string = r#"{\"type\":\"struct\",\"fields\":[{\"name\":\"number\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}"#;
    let loc = table_location.clone();
    let path = loc.path();
    let table = create_table(
        store.clone(),
        table_location,
        schema.clone(),
        schema_string,
        "",
    )
    .await?;

    // append an arrow record batch (vec of record batches??)
    let append_data = RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into()?),
        vec![Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3]))],
    )?;
    let append_data = Box::new(ArrowEngineData::new(append_data));

    // create a new txn based on current table version
    let txn_builder = table.new_transaction_builder();
    let mut txn = txn_builder.build(&engine).expect("build txn");
    let write_context = txn.write_context();

    // TEST: add a commit for 1.json to conflict
    let data = format!(r#"{{"add":{{}}}}"#).into_bytes();
    let path = format!("{path}_delta_log/00000000000000000001.json");
    println!("putting to path: {}", path);
    store.put(&Path::from(path), data.into()).await?;

    // create a new async task to do the write (simulate executors)
    let pq = engine.get_parquet_handler();
    let handle = tokio::task::spawn(async move {
        // transform to logical then write to object store and give back metadata
        let pq = pq
            .as_any()
            .downcast_ref::<DefaultParquetHandler<TokioBackgroundExecutor>>()
            .unwrap();
        pq.write_parquet_files(&write_context.target_directory, append_data)
            .await
            .expect("write parquet data")
    });

    // wait for writes to complete
    let metadata = handle.await;
    let metadata = metadata
        .into_iter()
        .map(|write_metadata| {
            RecordBatch::from(
                ArrowEngineData::try_from_engine_data(
                    create_write_metadata(
                        &write_metadata.path,
                        write_metadata.size as i64,
                        vec![],
                        true,
                        1724265056,
                    )
                    .unwrap(),
                )
                .unwrap(),
            )
        })
        .collect::<Vec<_>>();

    // FIXME
    let metadata_schema = get_metadata_schema();

    // concat all the metadata into single record batch
    let metadata = arrow::compute::concat_batches(&metadata_schema, &metadata)?;

    txn.add_write_metadata(Box::new(ArrowEngineData::new(metadata)));

    // TODO better error (make a new Error type) and check we get 'AlreadyExists'
    assert!(txn.commit(&engine).is_err_and(|e| {
        e.to_string().contains("already exists")
    }));

    Ok(())
}
*/
fn test_read(
    expected: Box<ArrowEngineData>,
    table: Table,
    engine: &impl Engine,
) -> Result<(), Box<dyn std::error::Error>> {
    let snapshot = table.snapshot(engine, None)?;

    println!("[test_read] snapshot: {:?}", snapshot);

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
    assert_eq!(formatted, expected);

    Ok(())
}

fn create_write_metadata(
    path: &str,
    size: i64,
    partitions: Vec<(String, String)>,
    data_change: bool,
    modification_time: i64,
) -> DeltaResult<Box<dyn EngineData>> {
    // FIXME unify with the metadata_schema above
    let path_field = Field::new("path", arrow_schema::DataType::Utf8, false);
    let size_field = Field::new("size", arrow_schema::DataType::Int64, false);
    let partition_field = Field::new(
        "partitionValues",
        arrow_schema::DataType::Map(
            Arc::new(Field::new(
                "key_value",
                arrow_schema::DataType::Struct(
                    vec![
                        Field::new("key", arrow_schema::DataType::Utf8, false),
                        Field::new("value", arrow_schema::DataType::Utf8, true),
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
    // Create the data for the record batch
    let path = Arc::new(StringArray::from(vec![path.to_string()])) as ArrayRef;
    let size = Arc::new(Int64Array::from(vec![size])) as ArrayRef;
    use arrow_array::builder::StringBuilder;
    let key_builder = StringBuilder::new();
    let val_builder = StringBuilder::new();
    let names = arrow_array::builder::MapFieldNames {
        entry: "key_value".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    };
    let mut builder = arrow_array::builder::MapBuilder::new(Some(names), key_builder, val_builder);
    if partitions.is_empty() {
        builder.append(true).unwrap();
    } else {
        for (k, v) in partitions {
            builder.keys().append_value(&k);
            builder.values().append_value(&v);
            builder.append(true).unwrap();
        }
    }
    let partitions = Arc::new(builder.finish()) as ArrayRef;
    let data_change = Arc::new(BooleanArray::from(vec![data_change])) as ArrayRef;
    let modification_time = Arc::new(Int64Array::from(vec![modification_time])) as ArrayRef;

    let schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
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
        true,
    )]));

    let add = Arc::new(arrow_array::StructArray::from(vec![
        (Arc::new(path_field), path),
        (Arc::new(size_field), size),
        (Arc::new(partition_field), partitions),
        (Arc::new(data_change_field), data_change),
        (Arc::new(modification_time_field), modification_time),
    ])) as ArrayRef;

    println!("ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ");
    println!("write metadata schema: {:#?}", schema);
    println!("ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ");

    // Create the record batch
    Ok(Box::new(ArrowEngineData::new(RecordBatch::try_new(
        schema,
        vec![add],
    )?)))
}

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int32Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use arrow_schema::{DataType as ArrowDataType, Field};
use itertools::Itertools;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;
use serde_json::Deserializer;
use serde_json::{json, to_vec};
use url::Url;

use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use delta_kernel::Error as KernelError;
use delta_kernel::{DeltaResult, Table};

mod common;
use common::test_read;

// setup default engine with in-memory (=true) or local fs (=false) object store.
fn setup(
    table_name: &str,
    in_memory: bool,
) -> (
    Arc<dyn ObjectStore>,
    DefaultEngine<TokioBackgroundExecutor>,
    Url,
) {
    let (storage, base_path, base_url): (Arc<dyn ObjectStore>, &str, &str) = if in_memory {
        (Arc::new(InMemory::new()), "/", "memory:///")
    } else {
        (
            Arc::new(LocalFileSystem::new()),
            "./kernel_write_tests/",
            "file://",
        )
    };

    let table_root_path = Path::from(format!("{base_path}{table_name}"));
    let url = Url::parse(&format!("{base_url}{table_root_path}/")).unwrap();
    let executor = Arc::new(TokioBackgroundExecutor::new());
    let engine = DefaultEngine::new(Arc::clone(&storage), table_root_path, executor);

    (storage, engine, url)
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

// create commit info in arrow of the form {engineInfo: "default engine"}
fn new_commit_info() -> DeltaResult<Box<ArrowEngineData>> {
    // create commit info of the form {engineCommitInfo: Map { "engineInfo": "default engine" } }
    let commit_info_schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "engineCommitInfo",
        ArrowDataType::Map(
            Arc::new(Field::new(
                "entries",
                ArrowDataType::Struct(
                    vec![
                        Field::new("key", ArrowDataType::Utf8, false),
                        Field::new("value", ArrowDataType::Utf8, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        ),
        false,
    )]));

    use arrow_array::builder::StringBuilder;
    let key_builder = StringBuilder::new();
    let val_builder = StringBuilder::new();
    let names = arrow_array::builder::MapFieldNames {
        entry: "entries".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    };
    let mut builder = arrow_array::builder::MapBuilder::new(Some(names), key_builder, val_builder);
    builder.keys().append_value("engineInfo");
    builder.values().append_value("default engine");
    builder.append(true).unwrap();
    let array = builder.finish();

    let commit_info_batch =
        RecordBatch::try_new(commit_info_schema.clone(), vec![Arc::new(array)])?;
    Ok(Box::new(ArrowEngineData::new(commit_info_batch)))
}

#[tokio::test]
async fn test_commit_info() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();
    // setup in-memory object store and default engine
    let (store, engine, table_location) = setup("test_table", true);

    // create a simple table: one int column named 'number'
    let schema = Arc::new(StructType::new(vec![StructField::new(
        "number",
        DataType::INTEGER,
        true,
    )]));
    let table = create_table(store.clone(), table_location, schema, &[]).await?;

    let commit_info = new_commit_info()?;

    // create a transaction
    let txn = table
        .new_transaction(&engine)?
        .with_commit_info(commit_info);

    // commit!
    txn.commit(&engine)?;

    let commit1 = store
        .get(&Path::from(
            "/test_table/_delta_log/00000000000000000001.json",
        ))
        .await?;

    let mut parsed_commit: serde_json::Value = serde_json::from_slice(&commit1.bytes().await?)?;
    *parsed_commit
        .get_mut("commitInfo")
        .unwrap()
        .get_mut("timestamp")
        .unwrap() = serde_json::Value::Number(0.into());

    let expected_commit = json!({
        "commitInfo": {
            "timestamp": 0,
            "operation": "UNKNOWN",
            "kernelVersion": format!("v{}", env!("CARGO_PKG_VERSION")),
            "operationParameters": {},
            "engineCommitInfo": {
                "engineInfo": "default engine"
            }
        }
    });

    assert_eq!(parsed_commit, expected_commit);
    Ok(())
}

#[tokio::test]
async fn test_empty_commit() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();
    // setup in-memory object store and default engine
    let (store, engine, table_location) = setup("test_table", true);

    // create a simple table: one int column named 'number'
    let schema = Arc::new(StructType::new(vec![StructField::new(
        "number",
        DataType::INTEGER,
        true,
    )]));
    let table = create_table(store.clone(), table_location, schema, &[]).await?;

    assert!(matches!(
        table.new_transaction(&engine)?.commit(&engine).unwrap_err(),
        KernelError::MissingCommitInfo
    ));

    Ok(())
}

#[tokio::test]
async fn test_invalid_commit_info() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();
    // setup in-memory object store and default engine
    let (store, engine, table_location) = setup("test_table", true);

    // create a simple table: one int column named 'number'
    let schema = Arc::new(StructType::new(vec![StructField::new(
        "number",
        DataType::INTEGER,
        true,
    )]));
    let table = create_table(store.clone(), table_location, schema, &[]).await?;

    // empty commit info test
    let commit_info_schema = Arc::new(ArrowSchema::empty());
    let commit_info_batch = RecordBatch::new_empty(commit_info_schema.clone());
    assert!(commit_info_batch.num_rows() == 0);
    let txn = table
        .new_transaction(&engine)?
        .with_commit_info(Box::new(ArrowEngineData::new(commit_info_batch)));

    // commit!
    assert!(matches!(
        txn.commit(&engine),
        Err(KernelError::InvalidCommitInfo(_))
    ));

    // two-row commit info test
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

    let txn = table
        .new_transaction(&engine)?
        .with_commit_info(Box::new(ArrowEngineData::new(commit_info_batch)));

    // commit!
    assert!(matches!(
        txn.commit(&engine),
        Err(KernelError::InvalidCommitInfo(_))
    ));
    Ok(())
}

// check that the timestamps in commit_info and add actions are within 10s of SystemTime::now()
fn check_action_timestamps<'a>(
    parsed_commits: impl Iterator<Item = &'a serde_json::Value>,
) -> Result<(), Box<dyn std::error::Error>> {
    let now: i64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis()
        .try_into()
        .unwrap();

    parsed_commits.for_each(|commit| {
        if let Some(commit_info_ts) = &commit.pointer("/commitInfo/timestamp") {
            assert!((now - commit_info_ts.as_i64().unwrap()).abs() < 10_000);
        }
        if let Some(add_ts) = &commit.pointer("/add/modificationTime") {
            assert!((now - add_ts.as_i64().unwrap()).abs() < 10_000);
        }
    });

    Ok(())
}

// update `value` at (.-separated) `path` to `new_value`
fn set_value(
    value: &mut serde_json::Value,
    path: &str,
    new_value: serde_json::Value,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut path_string = path.replace(".", "/");
    path_string.insert(0, '/');
    let v = value
        .pointer_mut(&path_string)
        .ok_or_else(|| format!("key '{path}' not found"))?;
    *v = new_value;
    Ok(())
}

#[tokio::test]
async fn test_append() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();
    // setup in-memory object store and default engine
    let (store, engine, table_location) = setup("test_table", true);

    // create a simple table: one int column named 'number'
    let schema = Arc::new(StructType::new(vec![StructField::new(
        "number",
        DataType::INTEGER,
        true,
    )]));
    let table = create_table(store.clone(), table_location, schema.clone(), &[]).await?;

    let commit_info = new_commit_info()?;

    let mut txn = table
        .new_transaction(&engine)?
        .with_commit_info(commit_info);

    // create two new arrow record batches to append
    let append_data = [[1, 2, 3], [4, 5, 6]].map(|data| -> DeltaResult<_> {
        let data = RecordBatch::try_new(
            Arc::new(schema.as_ref().try_into()?),
            vec![Arc::new(arrow::array::Int32Array::from(data.to_vec()))],
        )?;
        Ok(Box::new(ArrowEngineData::new(data)))
    });

    // write data out by spawning async tasks to simulate executors
    let engine = Arc::new(engine);
    let write_context = Arc::new(txn.get_write_context());
    let tasks = append_data.into_iter().map(|data| {
        // arc clones
        let engine = engine.clone();
        let write_context = write_context.clone();
        tokio::task::spawn(async move {
            engine
                .write_parquet(
                    data.as_ref().unwrap(),
                    write_context.as_ref(),
                    HashMap::new(),
                    true,
                )
                .await
        })
    });

    let write_metadata = futures::future::join_all(tasks).await.into_iter().flatten();
    for meta in write_metadata {
        txn.add_write_metadata(meta?);
    }

    // commit!
    txn.commit(engine.as_ref())?;

    let commit1 = store
        .get(&Path::from(
            "/test_table/_delta_log/00000000000000000001.json",
        ))
        .await?;

    let mut parsed_commits: Vec<_> = Deserializer::from_slice(&commit1.bytes().await?)
        .into_iter::<serde_json::Value>()
        .try_collect()?;

    // check that the timestamps in commit_info and add actions are within 10s of SystemTime::now()
    // before we clear them for comparison
    check_action_timestamps(parsed_commits.iter())?;

    // set timestamps to 0 and paths to known string values for comparison
    // (otherwise timestamps are non-deterministic and paths are random UUIDs)
    set_value(&mut parsed_commits[0], "commitInfo.timestamp", json!(0))?;
    set_value(&mut parsed_commits[1], "add.modificationTime", json!(0))?;
    set_value(&mut parsed_commits[1], "add.path", json!("first.parquet"))?;
    set_value(&mut parsed_commits[2], "add.modificationTime", json!(0))?;
    set_value(&mut parsed_commits[2], "add.path", json!("second.parquet"))?;

    let expected_commit = vec![
        json!({
            "commitInfo": {
                "timestamp": 0,
                "operation": "UNKNOWN",
                "kernelVersion": format!("v{}", env!("CARGO_PKG_VERSION")),
                "operationParameters": {},
                "engineCommitInfo": {
                    "engineInfo": "default engine"
                }
            }
        }),
        json!({
            "add": {
                "path": "first.parquet",
                "partitionValues": {},
                "size": 479,
                "modificationTime": 0,
                "dataChange": true
            }
        }),
        json!({
            "add": {
                "path": "second.parquet",
                "partitionValues": {},
                "size": 479,
                "modificationTime": 0,
                "dataChange": true
            }
        }),
    ];

    assert_eq!(parsed_commits, expected_commit);

    test_read(
        &ArrowEngineData::new(RecordBatch::try_new(
            Arc::new(schema.as_ref().try_into()?),
            vec![Arc::new(arrow::array::Int32Array::from(vec![
                1, 2, 3, 4, 5, 6,
            ]))],
        )?),
        &table,
        engine.as_ref(),
    )?;
    Ok(())
}

#[tokio::test]
async fn test_append_partitioned() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();
    // setup in-memory object store and default engine
    let (store, engine, table_location) = setup("test_table", true);
    let partition_col = "partition";

    // create a simple partitioned table: one int column named 'number', partitioned by string
    // column named 'partition'
    let table_schema = Arc::new(StructType::new(vec![
        StructField::new("number", DataType::INTEGER, true),
        StructField::new("partition", DataType::STRING, true),
    ]));
    let data_schema = Arc::new(StructType::new(vec![StructField::new(
        "number",
        DataType::INTEGER,
        true,
    )]));
    let table = create_table(
        store.clone(),
        table_location,
        table_schema.clone(),
        &[partition_col],
    )
    .await?;

    let commit_info = new_commit_info()?;

    let mut txn = table
        .new_transaction(&engine)?
        .with_commit_info(commit_info);

    // create two new arrow record batches to append
    let append_data = [[1, 2, 3], [4, 5, 6]].map(|data| -> DeltaResult<_> {
        let data = RecordBatch::try_new(
            Arc::new(data_schema.as_ref().try_into()?),
            vec![Arc::new(arrow::array::Int32Array::from(data.to_vec()))],
        )?;
        Ok(Box::new(ArrowEngineData::new(data)))
    });
    let partition_vals = vec!["a", "b"];

    // write data out by spawning async tasks to simulate executors
    let engine = Arc::new(engine);
    let write_context = Arc::new(txn.get_write_context());
    let tasks = append_data
        .into_iter()
        .zip(partition_vals)
        .map(|(data, partition_val)| {
            // arc clones
            let engine = engine.clone();
            let write_context = write_context.clone();
            tokio::task::spawn(async move {
                engine
                    .write_parquet(
                        data.as_ref().unwrap(),
                        write_context.as_ref(),
                        HashMap::from([(partition_col.to_string(), partition_val.to_string())]),
                        true,
                    )
                    .await
            })
        });

    let write_metadata = futures::future::join_all(tasks).await.into_iter().flatten();
    for meta in write_metadata {
        txn.add_write_metadata(meta?);
    }

    // commit!
    txn.commit(engine.as_ref())?;

    let commit1 = store
        .get(&Path::from(
            "/test_table/_delta_log/00000000000000000001.json",
        ))
        .await?;

    let mut parsed_commits: Vec<_> = Deserializer::from_slice(&commit1.bytes().await?)
        .into_iter::<serde_json::Value>()
        .try_collect()?;

    // check that the timestamps in commit_info and add actions are within 10s of SystemTime::now()
    // before we clear them for comparison
    check_action_timestamps(parsed_commits.iter())?;

    // set timestamps to 0 and paths to known string values for comparison
    // (otherwise timestamps are non-deterministic and paths are random UUIDs)
    set_value(&mut parsed_commits[0], "commitInfo.timestamp", json!(0))?;
    set_value(&mut parsed_commits[1], "add.modificationTime", json!(0))?;
    set_value(&mut parsed_commits[1], "add.path", json!("first.parquet"))?;
    set_value(&mut parsed_commits[2], "add.modificationTime", json!(0))?;
    set_value(&mut parsed_commits[2], "add.path", json!("second.parquet"))?;

    let expected_commit = vec![
        json!({
            "commitInfo": {
                "timestamp": 0,
                "operation": "UNKNOWN",
                "kernelVersion": format!("v{}", env!("CARGO_PKG_VERSION")),
                "operationParameters": {},
                "engineCommitInfo": {
                    "engineInfo": "default engine"
                }
            }
        }),
        json!({
            "add": {
                "path": "first.parquet",
                "partitionValues": {
                    "partition": "a"
                },
                "size": 479,
                "modificationTime": 0,
                "dataChange": true
            }
        }),
        json!({
            "add": {
                "path": "second.parquet",
                "partitionValues": {
                    "partition": "b"
                },
                "size": 479,
                "modificationTime": 0,
                "dataChange": true
            }
        }),
    ];

    assert_eq!(parsed_commits, expected_commit);

    test_read(
        &ArrowEngineData::new(RecordBatch::try_new(
            Arc::new(table_schema.as_ref().try_into()?),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
                Arc::new(StringArray::from(vec!["a", "a", "a", "b", "b", "b"])),
            ],
        )?),
        &table,
        engine.as_ref(),
    )?;
    Ok(())
}

#[tokio::test]
async fn test_append_invalid_schema() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();
    // setup in-memory object store and default engine
    let (store, engine, table_location) = setup("test_table", true);

    // create a simple table: one int column named 'number'
    let table_schema = Arc::new(StructType::new(vec![StructField::new(
        "number",
        DataType::INTEGER,
        true,
    )]));
    // incompatible data schema: one string column named 'string'
    let data_schema = Arc::new(StructType::new(vec![StructField::new(
        "string",
        DataType::STRING,
        true,
    )]));
    let table = create_table(store.clone(), table_location, table_schema.clone(), &[]).await?;

    let commit_info = new_commit_info()?;

    let txn = table
        .new_transaction(&engine)?
        .with_commit_info(commit_info);

    // create two new arrow record batches to append
    let append_data = [["a", "b"], ["c", "d"]].map(|data| -> DeltaResult<_> {
        let data = RecordBatch::try_new(
            Arc::new(data_schema.as_ref().try_into()?),
            vec![Arc::new(arrow::array::StringArray::from(data.to_vec()))],
        )?;
        Ok(Box::new(ArrowEngineData::new(data)))
    });

    // write data out by spawning async tasks to simulate executors
    let engine = Arc::new(engine);
    let write_context = Arc::new(txn.get_write_context());
    let tasks = append_data.into_iter().map(|data| {
        // arc clones
        let engine = engine.clone();
        let write_context = write_context.clone();
        tokio::task::spawn(async move {
            engine
                .write_parquet(
                    data.as_ref().unwrap(),
                    write_context.as_ref(),
                    HashMap::new(),
                    true,
                )
                .await
        })
    });

    let mut write_metadata = futures::future::join_all(tasks).await.into_iter().flatten();
    assert!(write_metadata.all(|res| match res {
        Err(KernelError::Arrow(arrow_schema::ArrowError::SchemaError(_))) => true,
        Err(KernelError::Backtraced { source, .. })
            if matches!(
                &*source,
                KernelError::Arrow(arrow_schema::ArrowError::SchemaError(_))
            ) =>
            true,
        _ => false,
    }));
    Ok(())
}

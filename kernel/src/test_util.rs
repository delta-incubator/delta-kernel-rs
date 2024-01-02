use std::collections::HashMap;
use std::sync::Arc;
use std::{env, path::PathBuf};

use arrow::array::*;
use arrow::compute::{max as arrow_max, min as arrow_min};
use arrow::datatypes::DataType as ArrowDataType;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chrono::Utc;
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::prefix::PrefixStore;
use object_store::{path::Path, ObjectMeta, ObjectStore};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rand::distributions::{Alphanumeric, DistString, Distribution, Uniform};
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use crate::actions::Action;
use crate::client::executor::{tokio::TokioBackgroundExecutor, TaskExecutor};
use crate::client::DefaultTableClient;
use crate::expressions::Scalar;
use crate::schema::{DataType, PrimitiveType, Schema, StructField, StructType};
use crate::table::Table;
use crate::{Add, FileMeta, Metadata, Protocol, Remove};

pub type TestResult<T = (), E = Box<dyn std::error::Error>> = std::result::Result<T, E>;

pub fn acceptance_test_data() -> String {
    match get_data_dir("DELTA_ACCEPTANCE_TEST_DATA", "../acceptance/tests/dat") {
        Ok(pb) => pb.display().to_string(),
        Err(err) => panic!("failed to get arrow data dir: {err}"),
    }
}

pub struct TestTableFactory {
    object_store: Arc<dyn ObjectStore>,
    tables: HashMap<String, Path>,
    executor: Arc<TokioBackgroundExecutor>,
}

impl Default for TestTableFactory {
    fn default() -> Self {
        Self {
            object_store: Arc::new(InMemory::new()),
            tables: HashMap::new(),
            executor: Arc::new(TokioBackgroundExecutor::new()),
        }
    }
}

pub struct Schemas {}

impl Schemas {
    /// A simple flat schema with a long, float and string column.
    ///
    /// ### Columns
    /// - id: long
    /// - float: double
    /// - string: string
    pub fn simple(&self) -> &'static StructType {
        lazy_static::lazy_static! {
            static ref _SIMPLE: StructType = StructType::new(vec![
                StructField::new("id", DataType::LONG, true),
                StructField::new("float", DataType::DOUBLE, true),
                StructField::new("string", DataType::STRING, true),
            ]);
        }
        &_SIMPLE
    }
}

impl TestTableFactory {
    pub fn new() -> Self {
        Self::default()
    }

    /// Accessor to some pre-defined schmeas for testing.
    /// See method doc on [`Schemas`] for more details
    /// on individual schemas.
    pub fn schemas(&self) -> &'static Schemas {
        lazy_static::lazy_static! {
            static ref _SCHEMAS: Schemas = Schemas {};
        }
        &_SCHEMAS
    }

    /// Load a location (folder) from a local directory into the internal in-memory store.
    /// Each loaded location will be assigned a unique name and can be accessed via
    /// getting a `TestTable` by its name which will be prefixed with the location.
    pub fn load_location(
        &mut self,
        name: impl Into<String>,
        path: impl AsRef<std::path::Path>,
    ) -> TestTable {
        let name = name.into();
        let tt = self.get_table(name.clone());

        let path = std::fs::canonicalize(path).unwrap();
        let location = Path::from(url::Url::from_file_path(path).unwrap().path());
        let store = PrefixStore::new(LocalFileSystem::new(), location.clone());

        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        self.executor.spawn(async move {
            let mut stream = store.list(None);
            while let Some(item) = stream.next().await {
                let item = item.unwrap();
                let data = store
                    .get(&item.location)
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap();
                tt.object_store.put(&item.location, data).await.unwrap();
                sender.send(()).ok();
            }
        });
        receiver.into_iter().for_each(drop);

        TestTable::new(
            Arc::new(PrefixStore::new(
                self.object_store.clone(),
                self.tables.get(&name).unwrap().clone(),
            )),
            self.executor.clone(),
        )
    }

    /// Get a table by its name from the in-memory store.
    pub fn get_table(&mut self, name: impl Into<String>) -> TestTable {
        let name = name.into();
        match self.tables.get(&name) {
            Some(root) => {
                let store = Arc::new(PrefixStore::new(self.object_store.clone(), root.clone()));
                TestTable::new(store, self.executor.clone())
            }
            None => {
                let root = Path::from(Uuid::new_v4().to_string());
                let store = Arc::new(PrefixStore::new(self.object_store.clone(), root.clone()));
                self.tables.insert(name, root.clone());
                TestTable::new(store, self.executor.clone())
            }
        }
    }

    pub fn create_metadata(
        &self,
        schema: &Schema,
        partiton_columns: Option<Vec<String>>,
        configuration: Option<HashMap<String, Option<String>>>,
    ) -> Action {
        create_metadata(schema, partiton_columns, configuration)
    }

    pub fn create_protocol(
        &self,
        min_reader_version: i32,
        min_writer_version: i32,
        reader_features: Option<Vec<String>>,
        writer_features: Option<Vec<String>>,
    ) -> Action {
        Action::Protocol(Protocol {
            min_reader_version,
            min_writer_version,
            reader_features,
            writer_features,
        })
    }

    pub fn generate_batch(
        &self,
        schema: &Schema,
        length: usize,
        bounds: HashMap<String, (&str, &str)>,
    ) -> TestResult<RecordBatch> {
        schema
            .fields()
            .iter()
            .map(|field| {
                let (min_val, max_val) = if let Some((min_val, max_val)) = bounds.get(field.name())
                {
                    (*min_val, *max_val)
                } else {
                    // NOTE providing illegal strings will resolve to default bounds,
                    // an empty string will resolve to null.
                    ("$%&", "$%&")
                };
                create_random_batch(
                    field.data_type().clone(),
                    min_val.to_string(),
                    max_val.to_string(),
                    length,
                )
            })
            .collect::<TestResult<Vec<_>>>()
            .map(|columns| {
                RecordBatch::try_new(Arc::new(schema.try_into().unwrap()), columns).unwrap()
            })
    }
}

pub struct TestTable {
    object_store: Arc<dyn ObjectStore>,
    executor: Arc<TokioBackgroundExecutor>,
}

impl TestTable {
    fn new(object_store: Arc<dyn ObjectStore>, executor: Arc<TokioBackgroundExecutor>) -> Self {
        Self {
            object_store,
            executor,
        }
    }

    /// Get the kernel table instance and corresponding default engine client
    pub fn table(&self) -> (Table, DefaultTableClient<TokioBackgroundExecutor>) {
        (
            Table::new(url::Url::parse("memory:///").unwrap()),
            DefaultTableClient::new(
                self.object_store.clone(),
                Path::default(),
                self.executor.clone(),
            ),
        )
    }

    pub fn list_files(&self, prefix: Option<&Path>) -> impl Iterator<Item = FileMeta> {
        let store = self.object_store.clone();
        let prefix = prefix.cloned();
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        self.executor.spawn(async move {
            let mut stream = store.list(prefix.as_ref());
            while let Some(item) = stream.next().await {
                sender.send(item.unwrap()).ok();
            }
        });
        receiver.into_iter().map(|meta| FileMeta {
            location: Url::parse(&format!("memory:///{}", meta.location)).unwrap(),
            last_modified: meta.last_modified.timestamp_millis(),
            size: meta.size,
        })
    }

    pub fn head(&self, path: &Path) -> TestResult<FileMeta> {
        let store = self.object_store.clone();
        let path = path.clone();
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        self.executor.spawn(async move {
            let meta = store.head(&path).await.unwrap();
            sender.send(meta).ok();
        });
        let meta = receiver.recv()?;
        Ok(FileMeta {
            location: Url::parse(&format!("memory:///{}", meta.location)).unwrap(),
            last_modified: meta.last_modified.timestamp_millis(),
            size: meta.size,
        })
    }

    pub fn write_commit<'a>(
        &self,
        version: u64,
        actions: impl IntoIterator<Item = &'a Action>,
        data: impl IntoIterator<Item = &'a RecordBatch>,
    ) -> TestResult<Vec<Action>> {
        let data: Vec<_> = data.into_iter().cloned().collect();
        let mut actions: Vec<_> = actions.into_iter().cloned().collect();

        let store = self.object_store.clone();
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        self.executor.spawn(async move {
            for batch in data {
                let file_name = Path::from(generate_file_name());
                let bytes = get_parquet_bytes(&batch).unwrap();
                store.put(&file_name, bytes).await.unwrap();
                let meta = store.head(&file_name).await.unwrap();
                let stats = get_stats(&batch).unwrap();
                actions.push(Action::Add(Add {
                    path: file_name.to_string(),
                    size: meta.size as i64,
                    partition_values: HashMap::new(),
                    data_change: true,
                    modification_time: meta.last_modified.timestamp_millis(),
                    stats: serde_json::to_string(&stats).ok(),
                    deletion_vector: None,
                    base_row_id: None,
                    default_row_commit_version: None,
                    tags: HashMap::new(),
                }));
            }

            let bytes = generate_commit_bytes(&actions).unwrap();
            let filename = format!("_delta_log/{:0>20}.json", version);
            store.put(&Path::from(filename), bytes).await.unwrap();

            sender.send(actions).ok();
        });

        Ok(receiver
            .recv()?
            .into_iter()
            .filter(|action| matches!(action, Action::Add(_)))
            .collect())
    }

    pub fn get_remove_actions<'a>(
        &self,
        adds: impl IntoIterator<Item = &'a Action>,
    ) -> Vec<Action> {
        adds.into_iter()
            .filter_map(|add| match add {
                Action::Add(add) => Some(create_remove_action(add)),
                _ => None,
            })
            .collect()
    }

    pub fn write_data_file(&self, batch: &RecordBatch) -> TestResult<ObjectMeta> {
        let store = self.object_store.clone();
        let data = get_parquet_bytes(batch)?;
        let file_name = object_store::path::Path::from(generate_file_name());
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        self.executor.spawn(async move {
            store.put(&file_name, data).await.unwrap();
            let meta = store.head(&file_name).await.unwrap();
            sender.send(meta).ok();
        });

        Ok(receiver.recv()?)
    }
}

fn create_random_batch(
    data_type: DataType,
    min_val: String,
    max_val: String,
    length: usize,
) -> TestResult<ArrayRef> {
    use PrimitiveType::*;
    let mut rng = rand::thread_rng();

    match data_type {
        DataType::Primitive(PrimitiveType::Integer) => {
            let min_val = Integer
                .parse_scalar(&min_val)
                .unwrap_or(Scalar::Integer(-10));
            let max_val = Integer
                .parse_scalar(&max_val)
                .unwrap_or(Scalar::Integer(10));
            let between = match (min_val, max_val) {
                (Scalar::Integer(min), Scalar::Integer(max)) => Uniform::from(min..=max),
                _ => unreachable!(),
            };
            let arr = Int32Array::from(
                (0..length)
                    .map(|_| between.sample(&mut rng))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr))
        }
        DataType::Primitive(PrimitiveType::Long) => {
            let min_val = PrimitiveType::Long
                .parse_scalar(&min_val)
                .unwrap_or(Scalar::Long(-10));
            let max_val = PrimitiveType::Long
                .parse_scalar(&max_val)
                .unwrap_or(Scalar::Long(10));
            let between = match (min_val, max_val) {
                (Scalar::Long(min), Scalar::Long(max)) => Uniform::from(min..=max),
                _ => unreachable!(),
            };
            let arr = Int64Array::from(
                (0..length)
                    .map(|_| between.sample(&mut rng))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr))
        }
        DataType::Primitive(PrimitiveType::Float) => {
            let min_val = PrimitiveType::Float
                .parse_scalar(&min_val)
                .unwrap_or(Scalar::Float(-10.1));
            let max_val = PrimitiveType::Float
                .parse_scalar(&max_val)
                .unwrap_or(Scalar::Float(10.1));
            let between = match (min_val, max_val) {
                (Scalar::Float(min), Scalar::Float(max)) => Uniform::from(min..=max),
                _ => unreachable!(),
            };
            let arr = Float32Array::from(
                (0..length)
                    .map(|_| between.sample(&mut rng))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr))
        }
        DataType::Primitive(PrimitiveType::Double) => {
            let min_val = PrimitiveType::Double
                .parse_scalar(&min_val)
                .unwrap_or(Scalar::Double(-10.1));
            let max_val = PrimitiveType::Double
                .parse_scalar(&max_val)
                .unwrap_or(Scalar::Double(10.1));
            let between = match (min_val, max_val) {
                (Scalar::Double(min), Scalar::Double(max)) => Uniform::from(min..=max),
                _ => unreachable!(),
            };
            let arr = Float64Array::from(
                (0..length)
                    .map(|_| between.sample(&mut rng))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr))
        }
        DataType::Primitive(PrimitiveType::String) => {
            let arr = StringArray::from(
                (0..length)
                    .map(|_| Alphanumeric.sample_string(&mut rng, 16))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr))
        }
        _ => todo!(),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileStats {
    pub num_records: i64,
    pub null_count: HashMap<String, String>,
    pub min_values: HashMap<String, String>,
    pub max_values: HashMap<String, String>,
}

impl FileStats {
    pub fn new(num_records: i64) -> Self {
        Self {
            num_records,
            null_count: HashMap::new(),
            min_values: HashMap::new(),
            max_values: HashMap::new(),
        }
    }
}

fn create_metadata(
    schema: &Schema,
    partition_columns: Option<Vec<String>>,
    configuration: Option<HashMap<String, Option<String>>>,
) -> Action {
    Action::Metadata(Metadata {
        id: Uuid::new_v4().hyphenated().to_string(),
        name: Some("test_table".to_string()),
        description: Some("test table".to_string()),
        format: crate::Format::default(),
        partition_columns: partition_columns.unwrap_or_default(),
        configuration: configuration.unwrap_or_default(),
        schema_string: serde_json::to_string(schema).unwrap(),
        created_time: Utc::now().timestamp_millis().into(),
    })
}

fn create_remove_action(add: &Add) -> Action {
    Action::Remove(Remove {
        path: add.path.clone(),
        data_change: true,
        deletion_timestamp: Some(Utc::now().timestamp_millis()),
        size: Some(add.size),
        extended_file_metadata: Some(true),
        partition_values: Some(add.partition_values.clone()),
        tags: Some(add.tags.clone()),
        deletion_vector: add.deletion_vector.clone(),
        base_row_id: add.base_row_id,
        default_row_commit_version: add.default_row_commit_version,
    })
}

fn get_stats(batch: &RecordBatch) -> TestResult<FileStats> {
    use ArrowDataType::*;

    let mut file_stats = FileStats::new(batch.num_rows() as i64);
    for (i, field) in batch.schema().fields().iter().enumerate() {
        let array = batch.column(i);
        let stats = match array.data_type() {
            Int8 => {
                let array = array.as_any().downcast_ref::<Int8Array>().unwrap();
                let min = Scalar::Byte(arrow_min(array).unwrap());
                let max = Scalar::Byte(arrow_max(array).unwrap());
                let null_count = Scalar::Long(array.null_count() as i64);
                Some((null_count, min, max))
            }
            Int16 => {
                let array = array.as_any().downcast_ref::<Int16Array>().unwrap();
                let min = Scalar::Short(arrow_min(array).unwrap());
                let max = Scalar::Short(arrow_max(array).unwrap());
                let null_count = Scalar::Long(array.null_count() as i64);
                Some((null_count, min, max))
            }
            Int32 => {
                let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                let min = Scalar::Integer(arrow_min(array).unwrap());
                let max = Scalar::Integer(arrow_max(array).unwrap());
                let null_count = Scalar::Long(array.null_count() as i64);
                Some((null_count, min, max))
            }
            Int64 => {
                let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                let min = Scalar::Long(arrow_min(array).unwrap());
                let max = Scalar::Long(arrow_max(array).unwrap());
                let null_count = Scalar::Long(array.null_count() as i64);
                Some((null_count, min, max))
            }
            Float32 => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                let min = Scalar::Float(arrow_min(array).unwrap());
                let max = Scalar::Float(arrow_max(array).unwrap());
                let null_count = Scalar::Long(array.null_count() as i64);
                Some((null_count, min, max))
            }
            Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                let min = Scalar::Double(arrow_min(array).unwrap());
                let max = Scalar::Double(arrow_max(array).unwrap());
                let null_count = Scalar::Long(array.null_count() as i64);
                Some((null_count, min, max))
            }
            Utf8 => None,
            Struct(_) => None,
            _ => todo!(),
        };
        if let Some((null_count, min, max)) = stats {
            file_stats
                .null_count
                .insert(field.name().to_string(), null_count.serialize());
            file_stats
                .min_values
                .insert(field.name().to_string(), min.serialize());
            file_stats
                .max_values
                .insert(field.name().to_string(), max.serialize());
        }
    }
    Ok(file_stats)
}

fn generate_file_name() -> String {
    let uuid = Uuid::new_v4();
    let file_name = uuid.hyphenated().to_string();
    format!("part-0001-{}.parquet", file_name)
}

fn get_parquet_bytes(batch: &RecordBatch) -> TestResult<Bytes> {
    let mut data: Vec<u8> = Vec::new();
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(&mut data, batch.schema(), Some(props))?;
    writer.write(batch)?;
    // writer must be closed to write footer
    writer.close()?;
    Ok(data.into())
}

fn generate_commit_bytes<'a>(actions: impl IntoIterator<Item = &'a Action>) -> TestResult<Bytes> {
    Ok(actions
        .into_iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()?
        .join("\n")
        .into())
}

/// Originaqlly lifted from arrow-rs crate
///
/// Returns a directory path for finding test data.
///
/// udf_env: name of an environment variable
///
/// submodule_dir: fallback path (relative to CARGO_MANIFEST_DIR)
///
///  Returns either:
/// The path referred to in `udf_env` if that variable is set and refers to a directory
/// The submodule_data directory relative to CARGO_MANIFEST_PATH
pub fn get_data_dir(udf_env: &str, submodule_data: &str) -> TestResult<PathBuf> {
    // Try user defined env.
    if let Ok(dir) = env::var(udf_env) {
        let trimmed = dir.trim().to_string();
        if !trimmed.is_empty() {
            let pb = PathBuf::from(trimmed);
            if pb.is_dir() {
                return Ok(pb);
            } else {
                return Err(format!(
                    "the data dir `{}` defined by env {} not found",
                    pb.display(),
                    udf_env
                )
                .into());
            }
        }
    }

    // The env is undefined or its value is trimmed to empty, let's try default dir.

    // env "CARGO_MANIFEST_DIR" is "the directory containing the manifest of your package",
    // set by `cargo run` or `cargo test`, see:
    // https://doc.rust-lang.org/cargo/reference/environment-variables.html
    let dir = env!("CARGO_MANIFEST_DIR");

    let pb = PathBuf::from(dir).join(submodule_data);
    if pb.is_dir() {
        Ok(pb)
    } else {
        Err(format!(
            "env `{}` is undefined or has empty value, and the pre-defined data dir `{}` not found.",
            udf_env,
            pb.display(),
        ).into())
    }
}

#[macro_export]
macro_rules! assert_batches_sorted_eq {
    ($EXPECTED_LINES: expr, $CHUNKS: expr) => {
        let mut expected_lines: Vec<String> = $EXPECTED_LINES.iter().map(|&s| s.into()).collect();

        // sort except for header + footer
        let num_lines = expected_lines.len();
        if num_lines > 3 {
            expected_lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
        }

        let formatted = arrow::util::pretty::pretty_format_batches($CHUNKS)
            .unwrap()
            .to_string();
        // fix for windows: \r\n -->

        let mut actual_lines: Vec<&str> = formatted.trim().lines().collect();

        // sort except for header + footer
        let num_lines = actual_lines.len();
        if num_lines > 3 {
            actual_lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
        }

        assert_eq!(
            expected_lines, actual_lines,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            expected_lines, actual_lines
        );
    };
}

pub use assert_batches_sorted_eq;

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;
    use crate::{
        schema::{DataType, StructField, StructType},
        Format, Metadata, Protocol,
    };

    #[test]
    fn test_acceptance_test_data() {
        let data_dir = super::acceptance_test_data();
        assert!(data_dir.ends_with("acceptance/tests/dat"));
        let path = std::fs::canonicalize(data_dir);
        assert!(path.is_ok());
    }

    #[test]
    fn test_write_commit() -> TestResult {
        let mut factory = TestTableFactory::default();

        let protocol = factory.create_protocol(1, 1, None, None);
        let schema = StructType::new(vec![
            StructField::new("number", DataType::LONG, true),
            StructField::new("a_float", DataType::DOUBLE, true),
        ]);
        let metadata = factory.create_metadata(&schema, None, None);
        let test_table = factory.get_table("test");
        test_table.write_commit(0, &[protocol, metadata], &[])?;

        let log_files = test_table
            .list_files(Some(&Path::from("_delta_log")))
            .collect_vec();
        assert_eq!(log_files.len(), 1);

        let (table, engine) = test_table.table();
        let snapshot = table.snapshot(&engine, None).unwrap();
        assert_eq!(snapshot.version(), 0);
        assert_eq!(snapshot.schema(), &schema);

        let batch = factory.generate_batch(
            &schema,
            10,
            HashMap::from_iter(vec![
                ("number".to_string(), ("-100", "100")),
                ("a_float".to_string(), ("-110.1", "222.2")),
            ]),
        )?;
        let actions = test_table.write_commit(1, &[], &[batch])?;
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], Action::Add(_)));

        let snapshot = table.snapshot(&engine, None).unwrap();
        assert_eq!(snapshot.version(), 1);

        Ok(())
    }

    #[test]
    fn test_load_table() -> TestResult {
        let mut factory = TestTableFactory::default();

        let test_table = factory.load_location("test", "./tests/data/basic_partitioned");

        let log_files = test_table
            .list_files(Some(&Path::from("_delta_log")))
            .collect_vec();
        assert_eq!(log_files.len(), 2);

        let (table, engine) = test_table.table();

        let snapshot = table.snapshot(&engine, None).unwrap();
        let expected = Protocol {
            min_reader_version: 1,
            min_writer_version: 2,
            reader_features: Some(Vec::new()),
            writer_features: Some(Vec::new()),
        };
        assert_eq!(snapshot.protocol(), &expected);

        let expected = Metadata {
            id: "ced0baf6-aa13-4871-af26-91e6e2787052".to_string(),
            name: None,
            description: None,
            format: Format::default(),
            partition_columns: vec!["letter".to_string()],
            configuration: HashMap::new(),
            created_time: Some(1674611426764),
            schema_string: "{\"type\":\"struct\",\"fields\":[{\"name\":\"letter\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"number\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"a_float\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]}".to_string(),

        };
        assert_eq!(snapshot.metadata(), &expected);

        let scan = table.scan(&engine, None)?.build();
        let batches = scan.execute(&engine)?;

        let expected = [
            "+--------+--------+---------+",
            "| letter | number | a_float |",
            "+--------+--------+---------+",
            "|        | 6      | 6.6     |",
            "| a      | 1      | 1.1     |",
            "| a      | 4      | 4.4     |",
            "| b      | 2      | 2.2     |",
            "| c      | 3      | 3.3     |",
            "| e      | 5      | 5.5     |",
            "+--------+--------+---------+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        // get the exisiting table from factory.
        let test_table = factory.get_table("test");
        let (table, engine) = test_table.table();
        let scan = table.scan(&engine, None)?.build();
        let batches = scan.execute(&engine)?;
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }
}

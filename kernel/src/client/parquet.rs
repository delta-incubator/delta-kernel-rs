//! Default Parquet handler implementation

use std::ops::Range;
use std::sync::Arc;

use arrow_schema::SchemaRef as ArrowSchemaRef;
use chrono::DateTime;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{DynObjectStore, ObjectMeta};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use parquet::arrow::ProjectionMask;

use super::file_handler::{FileOpenFuture, FileOpener, FileStream};
use super::schema_util::SchemaAdapter;
use crate::executor::TaskExecutor;
use crate::schema::SchemaRef;
use crate::{DeltaResult, Error, Expression, FileDataReadResultIterator, FileMeta, ParquetHandler};

#[derive(Debug)]
pub struct DefaultParquetHandler<E: TaskExecutor> {
    store: Arc<DynObjectStore>,
    task_executor: Arc<E>,
    readahead: usize,
    batch_size: usize,
}

impl<E: TaskExecutor> DefaultParquetHandler<E> {
    pub fn new(store: Arc<DynObjectStore>, task_executor: Arc<E>) -> Self {
        Self {
            store,
            task_executor,
            readahead: 10,
            batch_size: 1024,
        }
    }

    /// Max number of batches to read ahead while executing [Self::read_parquet_files()].
    ///
    /// Defaults to 10.
    pub fn with_readahead(mut self, readahead: usize) -> Self {
        self.readahead = readahead;
        self
    }

    /// Set the number of rows to read per batch during [Self::parse_json()].
    ///
    /// Defaults to 1024.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

impl<E: TaskExecutor> ParquetHandler for DefaultParquetHandler<E> {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        _predicate: Option<Expression>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        if files.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }

        let schema: ArrowSchemaRef = Arc::new(physical_schema.as_ref().try_into()?);
        let file_reader = ParquetOpener::new(self.batch_size, schema.clone(), self.store.clone());
        let stream = FileStream::new(files.to_vec(), schema, file_reader)?;

        // This channel will become the output iterator.
        // The stream will execute in the background and send results to this channel.
        // The channel will buffer up to `readahead` results, allowing the background
        // stream to get ahead of the consumer.
        let (sender, receiver) = std::sync::mpsc::sync_channel(self.readahead);

        self.task_executor.spawn(stream.for_each(move |res| {
            sender.send(res).ok();
            futures::future::ready(())
        }));

        Ok(Box::new(receiver.into_iter()))
    }
}

/// Implements [`FileOpener`] for a parquet file
struct ParquetOpener {
    batch_size: usize,
    limit: Option<usize>,
    table_schema: ArrowSchemaRef,
    store: Arc<DynObjectStore>,
}

impl ParquetOpener {
    pub(crate) fn new(
        batch_size: usize,
        schema: ArrowSchemaRef,
        store: Arc<DynObjectStore>,
    ) -> Self {
        Self {
            batch_size,
            table_schema: schema,
            limit: None,
            store,
        }
    }
}

impl FileOpener for ParquetOpener {
    fn open(&self, file_meta: FileMeta, _range: Option<Range<i64>>) -> DeltaResult<FileOpenFuture> {
        let seconds = file_meta.last_modified / 1000;
        let nanos = (file_meta.last_modified % 1000) * 1_000_000;
        let meta = ObjectMeta {
            // TODO handle paths relative to root
            location: Path::from(file_meta.location.path()),
            size: file_meta.size,
            last_modified: DateTime::from_timestamp(seconds, nanos as u32).ok_or(
                Error::Generic("Invalid timestamp in file metadata".to_string()),
            )?,
            e_tag: None,
            version: None,
        };
        let store = self.store.clone();

        let batch_size = self.batch_size;
        let limit = self.limit;

        let adapter = SchemaAdapter::try_new(self.table_schema.clone())?;

        Ok(Box::pin(async move {
            let reader = ParquetObjectReader::new(store, meta);
            let options = ArrowReaderOptions::new(); //.with_page_index(enable_page_index);
            let mut builder =
                ParquetRecordBatchStreamBuilder::new_with_options(reader, options).await?;

            let (mapping, projection) =
                adapter.map_schema(builder.schema(), builder.parquet_schema())?;

            let mask = ProjectionMask::leaves(builder.parquet_schema(), projection);
            if let Some(limit) = limit {
                builder = builder.with_limit(limit)
            }

            let stream = builder
                .with_projection(mask)
                .with_batch_size(batch_size)
                .build()?;

            let adapted = stream
                .map(move |batch| match batch {
                    Ok(batch) => mapping.map_batch(batch),
                    Err(e) => Err(e.into()),
                })
                .map_err(|e| Error::GenericError {
                    source: Box::new(e),
                });

            Ok(adapted.boxed())
        }))
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::RecordBatch;
    use itertools::Itertools;

    use super::*;
    use crate::schema::{DataType as DeltaDataType, StructField, StructType};
    use crate::test_util::{assert_batches_sorted_eq, TestResult, TestTableFactory};

    use crate::{ActionType, TableClient};

    #[test]
    fn test_read_parquet_files() -> TestResult {
        let mut factory = TestTableFactory::new();
        let path = "./tests/data/table-with-dv-small/";
        let file_path =
            Path::from("part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet");
        let tt = factory.load_table("test", path);
        let files = &[tt.head(&file_path)?];

        let (table, engine) = tt.table();
        let handler = engine.get_parquet_handler();
        let read_schema = table.snapshot(&engine, None)?.schema().clone();

        let data: Vec<RecordBatch> = handler
            .read_parquet_files(files, Arc::new(read_schema.try_into().unwrap()), None)?
            .try_collect()?;

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].num_rows(), 10);

        Ok(())
    }

    #[test]
    fn test_projection_pushdown() -> TestResult {
        let mut factory = TestTableFactory::new();
        let _test_table = factory.get_table("default");
        Ok(())
    }

    #[test]
    fn test_read_parquet_projection() -> TestResult {
        let mut factory = TestTableFactory::new();
        let path = "../acceptance/tests/dat/out/reader_tests/generated/with_checkpoint/delta/";
        let file_path = Path::from("_delta_log/00000000000000000002.checkpoint.parquet");
        let tt = factory.load_table("test", path);
        let checkpoint_file = &[tt.head(&file_path)?];

        let (_, engine) = tt.table();
        let handler = engine.get_parquet_handler();

        let read_schema = Arc::new(StructType::new(vec![ActionType::Metadata
            .schema_field()
            .clone()]));

        let data: Vec<_> = handler
            .read_parquet_files(checkpoint_file, read_schema, None)?
            .try_collect()?;

        let expected = [
            "+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| metaData                                                                                                                                                                                                                                                                                                                                                                                                                                       |",
            "+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| {id: , name: , description: , format: {provider: , options: {}}, schemaString: , partitionColumns: , createdTime: , configuration: {}}                                                                                                                                                                                                                                                                                                         |",
            "| {id: , name: , description: , format: {provider: , options: {}}, schemaString: , partitionColumns: , createdTime: , configuration: {}}                                                                                                                                                                                                                                                                                                         |",
            "| {id: , name: , description: , format: {provider: , options: {}}, schemaString: , partitionColumns: , createdTime: , configuration: {}}                                                                                                                                                                                                                                                                                                         |",
            "| {id: 84b09beb-329c-4b5e-b493-f58c6c78b8fd, name: , description: , format: {provider: parquet, options: {}}, schemaString: {\"type\":\"struct\",\"fields\":[{\"name\":\"letter\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"int\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}, partitionColumns: [], createdTime: 1674611455081, configuration: {delta.checkpointInterval: 2}} |",
            "+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
        ];

        assert_batches_sorted_eq!(expected, &data);

        let read_schema: Arc<StructType> = Arc::new(StructType::new(vec![ActionType::Protocol
            .schema_field()
            .clone()]));
        let data: Vec<RecordBatch> = handler
            .read_parquet_files(checkpoint_file, read_schema, None)?
            .try_collect()?;

        let expected = [
            "+--------------------------------------------------------------------------------+",
            "| protocol                                                                       |",
            "+--------------------------------------------------------------------------------+",
            "| {minReaderVersion: , minWriterVersion: , readerFeatures: , writerFeatures: }   |",
            "| {minReaderVersion: , minWriterVersion: , readerFeatures: , writerFeatures: }   |",
            "| {minReaderVersion: , minWriterVersion: , readerFeatures: , writerFeatures: }   |",
            "| {minReaderVersion: 1, minWriterVersion: 2, readerFeatures: , writerFeatures: } |",
            "+--------------------------------------------------------------------------------+",
        ];
        assert_batches_sorted_eq!(expected, &data);
        //
        let read_schema: Arc<StructType> = Arc::new(StructType::new(vec![StructField::new(
            "protocol",
            StructType::new(vec![StructField::new(
                "minWriterVersion",
                DeltaDataType::INTEGER,
                true,
            )]),
            true,
        )]));
        let data: Vec<RecordBatch> = handler
            .read_parquet_files(checkpoint_file, read_schema, None)?
            .try_collect()?;

        let expected = [
            "+-----------------------+",
            "| protocol              |",
            "+-----------------------+",
            "| {minWriterVersion: 2} |",
            "| {minWriterVersion: }  |",
            "| {minWriterVersion: }  |",
            "| {minWriterVersion: }  |",
            "+-----------------------+",
        ];
        assert_batches_sorted_eq!(expected, &data);

        Ok(())
    }
}

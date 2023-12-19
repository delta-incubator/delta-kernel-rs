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
    use std::path::PathBuf;

    use arrow_array::RecordBatch;
    use itertools::Itertools;
    use object_store::{local::LocalFileSystem, ObjectStore};

    use super::*;
    use crate::executor::tokio::TokioBackgroundExecutor;
    use crate::schema::{DataType as DeltaDataType, StructField, StructType};
    use crate::ActionType;

    macro_rules! assert_batches_sorted_eq {
        ($EXPECTED_LINES: expr, $CHUNKS: expr) => {
            let mut expected_lines: Vec<String> =
                $EXPECTED_LINES.iter().map(|&s| s.into()).collect();

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

    #[tokio::test]
    async fn test_read_parquet_files() {
        let store = Arc::new(LocalFileSystem::new());

        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/table-with-dv-small/part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet"
        )).unwrap();
        let url = url::Url::from_file_path(path).unwrap();
        let location = Path::from(url.path());
        let meta = store.head(&location).await.unwrap();

        let reader = ParquetObjectReader::new(store.clone(), meta.clone());
        let physical_schema = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .unwrap()
            .schema()
            .clone();

        let files = &[FileMeta {
            location: url.clone(),
            last_modified: meta.last_modified.timestamp_millis(),
            size: meta.size,
        }];

        let handler = DefaultParquetHandler::new(store, Arc::new(TokioBackgroundExecutor::new()));
        let data: Vec<RecordBatch> = handler
            .read_parquet_files(files, Arc::new(physical_schema.try_into().unwrap()), None)
            .unwrap()
            .try_collect()
            .unwrap();

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].num_rows(), 10);
    }

    async fn get_parquet_builder(
        path: impl AsRef<std::path::Path>,
    ) -> (
        ParquetRecordBatchStreamBuilder<ParquetObjectReader>,
        [FileMeta; 1],
        Arc<dyn ObjectStore>,
    ) {
        let store = Arc::new(LocalFileSystem::new());
        let path = std::fs::canonicalize(path).unwrap();
        let url = url::Url::from_file_path(path).unwrap();
        let location = Path::from(url.path());
        let meta = store.head(&location).await.unwrap();
        let reader = ParquetObjectReader::new(store.clone(), meta.clone());
        let files = [FileMeta {
            location: url.clone(),
            last_modified: meta.last_modified.timestamp_millis(),
            size: meta.size,
        }];
        (
            ParquetRecordBatchStreamBuilder::new(reader).await.unwrap(),
            files,
            store,
        )
    }

    #[tokio::test]
    async fn test_read_parquet_projection() {
        let path = "../acceptance/tests/dat/out/reader_tests/generated/with_checkpoint/delta/_delta_log/00000000000000000002.checkpoint.parquet";
        let (_, files, store) = get_parquet_builder(path).await;
        let handler = DefaultParquetHandler::new(store, Arc::new(TokioBackgroundExecutor::new()));

        let read_schema: Arc<StructType> = Arc::new(StructType::new(vec![ActionType::Metadata
            .schema_field()
            .clone()]));
        let data: Vec<RecordBatch> = handler
            .read_parquet_files(&files, read_schema, None)
            .unwrap()
            .try_collect()
            .unwrap();

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
            .read_parquet_files(&files, read_schema, None)
            .unwrap()
            .try_collect()
            .unwrap();

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
            .read_parquet_files(&files, read_schema, None)
            .unwrap()
            .try_collect()
            .unwrap();

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
    }
}

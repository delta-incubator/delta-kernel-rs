//! Default Parquet handler implementation

use std::ops::Range;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::{ArrayRef, BooleanArray, Int64Array, MapArray, StringArray};
use arrow_schema::Field;
use futures::StreamExt;
use object_store::path::Path;
use object_store::DynObjectStore;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use uuid::Uuid;

use super::file_stream::{FileOpenFuture, FileOpener, FileStream};
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::arrow_utils::{generate_mask, get_requested_indices, reorder_struct_array};
use crate::engine::default::executor::TaskExecutor;
use crate::schema::SchemaRef;
use crate::{
    DeltaResult, EngineData, Error, Expression, FileDataReadResultIterator, FileMeta,
    ParquetHandler,
};

#[derive(Debug)]
pub struct DefaultParquetHandler<E: TaskExecutor> {
    store: Arc<DynObjectStore>,
    task_executor: Arc<E>,
    readahead: usize,
}

impl<E: TaskExecutor> DefaultParquetHandler<E> {
    pub fn new(store: Arc<DynObjectStore>, task_executor: Arc<E>) -> Self {
        Self {
            store,
            task_executor,
            readahead: 10,
        }
    }

    /// Max number of batches to read ahead while executing [Self::read_parquet_files()].
    ///
    /// Defaults to 10.
    pub fn with_readahead(mut self, readahead: usize) -> Self {
        self.readahead = readahead;
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

        // get the first FileMeta to decide how to fetch the file.
        // NB: This means that every file in `FileMeta` _must_ have the same scheme or things will break
        // s3://    -> aws   (ParquetOpener)
        // nothing  -> local (ParquetOpener)
        // https:// -> assume presigned URL (and fetch without object_store)
        //   -> reqwest to get data
        //   -> parse to parquet
        // SAFETY: we did is_empty check above, this is ok.
        let file_opener: Box<dyn FileOpener> = match files[0].location.scheme() {
            "http" | "https" => Box::new(PresignedUrlOpener::new(1024, physical_schema.clone())),
            _ => Box::new(ParquetOpener::new(
                1024,
                physical_schema.clone(),
                self.store.clone(),
            )),
        };
        FileStream::new_async_read_iterator(
            self.task_executor.clone(),
            Arc::new(physical_schema.as_ref().try_into()?),
            file_opener,
            files,
            self.readahead,
        )
    }

    fn write_parquet_files(
        &self,
        path: &url::Url,
        data: Box<dyn EngineData>,
    ) -> DeltaResult<Box<dyn EngineData>> {
        let batch: Box<_> = ArrowEngineData::try_from_engine_data(data)?;
        let record_batch = batch.record_batch();
        let mut buffer = vec![];
        let mut writer = ArrowWriter::try_new(&mut buffer, record_batch.schema(), None).unwrap();

        writer.write(&record_batch).expect("Writing batch");

        // writer must be closed to write footer
        writer.close().unwrap();

        // FIXME safe cast?
        let size = buffer.len() as i64;

        let name: String = Uuid::new_v4().to_string() + ".parquet";
        let path = path.join(&name)?;
        println!("Writing parquet file to {:?}", path);

        futures::executor::block_on(async {
            self.store
                .put(&Path::from(path.path()), buffer.into())
                .await
        })?;

        // FIXME get schema from kernel?
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
        let modification_time_field = Field::new("modificationTime", arrow_schema::DataType::Int64, false);

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
            false,
        )]));

        // Create the data for the record batch
        let path = Arc::new(StringArray::from(vec![path.to_string()])) as ArrayRef;
        let size = Arc::new(Int64Array::from(vec![size])) as ArrayRef;
        use arrow_array::builder::StringBuilder;
        let mut builder =
            arrow_array::builder::MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        builder.append(true).unwrap();
        let partitions = Arc::new(builder.finish()) as ArrayRef;
        let data_change = Arc::new(BooleanArray::from(vec![true])) as ArrayRef;
        let modification_time = Arc::new(Int64Array::from(vec![1724265056])) as ArrayRef;

        let add = Arc::new(arrow_array::StructArray::from(vec![
            (Arc::new(path_field), path),
            (Arc::new(size_field), size),
            (Arc::new(partition_field), partitions),
            (Arc::new(data_change_field), data_change),
            (Arc::new(modification_time_field), modification_time),
        ])) as ArrayRef;

        // Create the record batch
        let batch = RecordBatch::try_new(schema, vec![add]).unwrap();

        Ok(Box::new(ArrowEngineData::new(batch)))
    }
}

/// Implements [`FileOpener`] for a parquet file
struct ParquetOpener {
    // projection: Arc<[usize]>,
    batch_size: usize,
    limit: Option<usize>,
    table_schema: SchemaRef,
    store: Arc<DynObjectStore>,
}

impl ParquetOpener {
    pub(crate) fn new(
        batch_size: usize,
        table_schema: SchemaRef,
        store: Arc<DynObjectStore>,
    ) -> Self {
        Self {
            batch_size,
            table_schema,
            limit: None,
            store,
        }
    }
}

impl FileOpener for ParquetOpener {
    fn open(&self, file_meta: FileMeta, _range: Option<Range<i64>>) -> DeltaResult<FileOpenFuture> {
        let path = Path::from_url_path(file_meta.location.path())?;
        let store = self.store.clone();

        let batch_size = self.batch_size;
        // let projection = self.projection.clone();
        let table_schema = self.table_schema.clone();
        let limit = self.limit;

        Ok(Box::pin(async move {
            // TODO avoid IO by converting passed file meta to ObjectMeta
            let meta = store.head(&path).await?;
            let mut reader = ParquetObjectReader::new(store, meta);
            let metadata = ArrowReaderMetadata::load_async(&mut reader, Default::default()).await?;
            let parquet_schema = metadata.schema();
            let (indicies, requested_ordering) =
                get_requested_indices(&table_schema, parquet_schema)?;
            let options = ArrowReaderOptions::new(); //.with_page_index(enable_page_index);
            let mut builder =
                ParquetRecordBatchStreamBuilder::new_with_options(reader, options).await?;
            if let Some(mask) = generate_mask(
                &table_schema,
                parquet_schema,
                builder.parquet_schema(),
                &indicies,
            ) {
                builder = builder.with_projection(mask)
            }

            if let Some(limit) = limit {
                builder = builder.with_limit(limit)
            }

            let stream = builder.with_batch_size(batch_size).build()?;

            let stream = stream.map(move |rbr| {
                // re-order each batch if needed
                rbr.map_err(Error::Parquet).and_then(|rb| {
                    reorder_struct_array(rb.into(), &requested_ordering).map(Into::into)
                })
            });
            Ok(stream.boxed())
        }))
    }
}

/// Implements [`FileOpener`] for a opening a parquet file from a presigned URL
struct PresignedUrlOpener {
    batch_size: usize,
    limit: Option<usize>,
    table_schema: SchemaRef,
    client: reqwest::Client,
}

impl PresignedUrlOpener {
    pub(crate) fn new(batch_size: usize, schema: SchemaRef) -> Self {
        Self {
            batch_size,
            table_schema: schema,
            limit: None,
            client: reqwest::Client::new(),
        }
    }
}

impl FileOpener for PresignedUrlOpener {
    fn open(&self, file_meta: FileMeta, _range: Option<Range<i64>>) -> DeltaResult<FileOpenFuture> {
        let batch_size = self.batch_size;
        let table_schema = self.table_schema.clone();
        let limit = self.limit;
        let client = self.client.clone(); // uses Arc internally according to reqwest docs

        Ok(Box::pin(async move {
            // fetch the file from the interweb
            let reader = client.get(file_meta.location).send().await?.bytes().await?;
            let metadata = ArrowReaderMetadata::load(&reader, Default::default())?;
            let parquet_schema = metadata.schema();
            let (indicies, requested_ordering) =
                get_requested_indices(&table_schema, parquet_schema)?;

            let options = ArrowReaderOptions::new();
            let mut builder =
                ParquetRecordBatchReaderBuilder::try_new_with_options(reader, options)?;
            if let Some(mask) = generate_mask(
                &table_schema,
                parquet_schema,
                builder.parquet_schema(),
                &indicies,
            ) {
                builder = builder.with_projection(mask)
            }

            if let Some(limit) = limit {
                builder = builder.with_limit(limit)
            }

            let reader = builder.with_batch_size(batch_size).build()?;

            let stream = futures::stream::iter(reader);
            let stream = stream.map(move |rbr| {
                // re-order each batch if needed
                rbr.map_err(Error::Arrow).and_then(|rb| {
                    reorder_struct_array(rb.into(), &requested_ordering).map(Into::into)
                })
            });
            Ok(stream.boxed())
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use arrow_array::RecordBatch;
    use object_store::{local::LocalFileSystem, ObjectStore};

    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
    use crate::EngineData;

    use itertools::Itertools;

    use super::*;

    fn into_record_batch(
        engine_data: DeltaResult<Box<dyn EngineData>>,
    ) -> DeltaResult<RecordBatch> {
        engine_data
            .and_then(ArrowEngineData::try_from_engine_data)
            .map(Into::into)
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
            last_modified: meta.last_modified.timestamp(),
            size: meta.size,
        }];

        let handler = DefaultParquetHandler::new(store, Arc::new(TokioBackgroundExecutor::new()));
        let data: Vec<RecordBatch> = handler
            .read_parquet_files(files, Arc::new(physical_schema.try_into().unwrap()), None)
            .unwrap()
            .map(into_record_batch)
            .try_collect()
            .unwrap();

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].num_rows(), 10);
    }
}

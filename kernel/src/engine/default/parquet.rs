//! Default Parquet handler implementation

use std::ops::Range;
use std::sync::Arc;

use futures::StreamExt;
use object_store::path::Path;
use object_store::DynObjectStore;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};

use super::file_stream::{FileOpenFuture, FileOpener, FileStream};
use crate::engine::arrow_expression::expression_to_row_filter;
use crate::engine::arrow_utils::{generate_mask, get_requested_indices, reorder_struct_array};
use crate::engine::default::executor::TaskExecutor;
use crate::schema::SchemaRef;
use crate::{DeltaResult, Error, Expression, FileDataReadResultIterator, FileMeta, ParquetHandler};

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
        predicate: Option<Expression>,
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
            "http" | "https" => Box::new(PresignedUrlOpener::new(
                1024,
                physical_schema.clone(),
                predicate,
            )),
            _ => Box::new(ParquetOpener::new(
                1024,
                physical_schema.clone(),
                self.store.clone(),
                predicate,
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
}

/// Implements [`FileOpener`] for a parquet file
struct ParquetOpener {
    // projection: Arc<[usize]>,
    batch_size: usize,
    limit: Option<usize>,
    table_schema: SchemaRef,
    store: Arc<DynObjectStore>,
    predicate: Option<Expression>,
}

impl ParquetOpener {
    pub(crate) fn new(
        batch_size: usize,
        table_schema: SchemaRef,
        store: Arc<DynObjectStore>,
        predicate: Option<Expression>,
    ) -> Self {
        Self {
            batch_size,
            table_schema,
            limit: None,
            store,
            predicate,
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
        let predicate = self.predicate.clone();
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
            if let Some(predicate) = predicate {
                let parquet_schema = metadata.schema();
                let parquet_physical_schema = metadata.parquet_schema();
                let row_filter = expression_to_row_filter(
                    predicate,
                    &table_schema,
                    parquet_schema,
                    parquet_physical_schema,
                )?;
                builder = builder.with_row_filter(row_filter);
            }
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
    predicate: Option<Expression>,
}

impl PresignedUrlOpener {
    pub(crate) fn new(batch_size: usize, schema: SchemaRef, predicate: Option<Expression>) -> Self {
        Self {
            batch_size,
            table_schema: schema,
            limit: None,
            client: reqwest::Client::new(),
            predicate,
        }
    }
}

impl FileOpener for PresignedUrlOpener {
    fn open(&self, file_meta: FileMeta, _range: Option<Range<i64>>) -> DeltaResult<FileOpenFuture> {
        let batch_size = self.batch_size;
        let table_schema = self.table_schema.clone();
        let limit = self.limit;
        let client = self.client.clone(); // uses Arc internally according to reqwest docs
        let predicate = self.predicate.clone();

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

            if let Some(predicate) = predicate {
                let parquet_schema = metadata.schema();
                let parquet_physical_schema = metadata.parquet_schema();
                let row_filter = expression_to_row_filter(
                    predicate,
                    &table_schema,
                    parquet_schema,
                    parquet_physical_schema,
                )?;
                builder = builder.with_row_filter(row_filter);
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
    use std::ops::Not;
    use std::path::PathBuf;

    use arrow_array::RecordBatch;
    use object_store::{local::LocalFileSystem, ObjectStore};

    use crate::actions::{Metadata, Protocol};
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
    use crate::EngineData;
    use Expression as Expr;

    use itertools::Itertools;

    use super::*;

    fn into_record_batch(
        engine_data: DeltaResult<Box<dyn EngineData>>,
    ) -> DeltaResult<RecordBatch> {
        engine_data
            .and_then(ArrowEngineData::try_from_engine_data)
            .map(Into::into)
    }

    async fn get_record_batch(path: &str, predicate: Option<Expression>) -> Vec<RecordBatch> {
        let store = Arc::new(LocalFileSystem::new());

        let path = std::fs::canonicalize(PathBuf::from(path)).unwrap();
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
        handler
            .read_parquet_files(
                files,
                Arc::new(physical_schema.try_into().unwrap()),
                predicate,
            )
            .unwrap()
            .map(into_record_batch)
            .try_collect()
            .unwrap()
    }

    #[tokio::test]
    async fn test_read_parquet_files_with_empty_output() {
        let predicate = Some(Expr::lt(Expr::column("value"), Expr::literal(0)));
        let path = "./tests/data/table-with-dv-small/part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet";
        let data = get_record_batch(path, predicate).await;
        assert_eq!(data.len(), 0);
    }

    #[tokio::test]
    async fn test_read_parquet_files_filter_data() {
        let cases = [
            (
                5,
                Some(Expr::gt_eq(Expr::column("value"), Expr::literal(5))),
            ),
            (1, Some(Expr::gt(Expr::column("value"), Expr::literal(8)))),
            (10, None),
        ];
        let path = "./tests/data/table-with-dv-small/part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet";
        for (num_rows, predicate) in cases {
            let data = get_record_batch(path, predicate).await;
            assert_eq!(data.len(), 1);
            assert_eq!(data[0].num_rows(), num_rows);
        }
    }

    #[tokio::test]
    async fn test_parquet_protocol_metadata_filter() {
        let predicate = Some(Expr::or(
            Expr::not(Expr::is_null(Expr::column("metaData.id"))),
            Expr::not(Expr::is_null(Expr::column("protocol.minReaderVersion"))),
        ));

        let path =
            "./tests/data/app-txn-checkpoint/_delta_log/00000000000000000001.checkpoint.parquet";
        let data_filtered = get_record_batch(path, predicate).await;
        let data = get_record_batch(path, None).await;

        let mut metadata_filtered: Vec<Metadata> = vec![];
        let mut metadata_expected: Vec<Metadata> = vec![];
        let mut protocol_filtered: Vec<Protocol> = vec![];
        let mut protocol_expected: Vec<Protocol> = vec![];

        for batch in data_filtered.into_iter().map(Into::<ArrowEngineData>::into) {
            if let Some(metadata) = Metadata::try_new_from_data(&batch).unwrap() {
                metadata_filtered.push(metadata);
            } else if let Some(protocol) = Protocol::try_new_from_data(&batch).unwrap() {
                protocol_filtered.push(protocol);
            } else {
                panic!("The filtered data must only have metadata or protocol entries");
            }
        }
        for batch in data.into_iter().map(Into::<ArrowEngineData>::into) {
            if let Some(metadata) = Metadata::try_new_from_data(&batch).unwrap() {
                metadata_expected.push(metadata);
            }
            if let Some(protocol) = Protocol::try_new_from_data(&batch).unwrap() {
                protocol_expected.push(protocol);
            }
        }

        assert_eq!(metadata_filtered, metadata_expected);
        assert_eq!(protocol_expected, protocol_expected);
    }
}

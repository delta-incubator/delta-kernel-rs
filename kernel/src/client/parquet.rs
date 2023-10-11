//! Default Parquet handler implementation

use std::ops::Range;
use std::sync::Arc;

use arrow_schema::SchemaRef as ArrowSchemaRef;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::DynObjectStore;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};

use super::file_handler::{FileOpenFuture, FileOpener};
use crate::executor::TaskExecutor;
use crate::file_handler::FileStream;
use crate::schema::SchemaRef;
use crate::{
    DeltaResult, Error, Expression, FileDataReadResultIterator, FileHandler, FileMeta,
    ParquetHandler,
};

pub struct ParquetReadContext {
    // pub(crate) reader: ParquetObjectReader,
    pub(crate) meta: FileMeta,
}

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

impl<E: TaskExecutor> FileHandler for DefaultParquetHandler<E> {
    type FileReadContext = ParquetReadContext;

    fn contextualize_file_reads(
        &self,
        files: &[FileMeta],
        _predicate: Option<Expression>,
    ) -> DeltaResult<Vec<ParquetReadContext>> {
        Ok(files
            .iter()
            .map(|meta| ParquetReadContext { meta: meta.clone() })
            .collect())
    }
}

impl<E: TaskExecutor> ParquetHandler for DefaultParquetHandler<E> {
    fn read_parquet_files(
        &self,
        files: Vec<<Self as FileHandler>::FileReadContext>,
        physical_schema: SchemaRef,
    ) -> DeltaResult<FileDataReadResultIterator> {
        // TODO at the very least load only required columns ...
        if files.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }

        let schema: ArrowSchemaRef = Arc::new(physical_schema.as_ref().try_into()?);
        let file_reader = ParquetOpener::new(1024, schema.clone(), self.store.clone());

        let files = files.into_iter().map(|f| f.meta).collect::<Vec<_>>();
        let stream = FileStream::new(files, schema, file_reader)?;

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
    // projection: Arc<[usize]>,
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
        let path = Path::from(file_meta.get_location().path());
        let store = self.store.clone();

        let batch_size = self.batch_size;
        // let projection = self.projection.clone();
        let _table_schema = self.table_schema.clone();
        let limit = self.limit;

        Ok(Box::pin(async move {
            // TODO avoid IO by converting passed file meta to ObjectMeta
            let meta = store.head(&path).await?;
            let reader = ParquetObjectReader::new(store, meta);
            let options = ArrowReaderOptions::new(); //.with_page_index(enable_page_index);
            let mut builder =
                ParquetRecordBatchStreamBuilder::new_with_options(reader, options).await?;

            // let mask = ProjectionMask::roots(builder.parquet_schema(), projection.iter().cloned());
            if let Some(limit) = limit {
                builder = builder.with_limit(limit)
            }

            let stream = builder
                // .with_projection(mask)
                .with_batch_size(batch_size)
                .build()?;

            let adapted = stream.map_err(|e| Error::GenericError {
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
    use object_store::{local::LocalFileSystem, ObjectStore};

    use crate::executor::tokio::TokioBackgroundExecutor;

    use itertools::Itertools;

    use super::*;

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
        let context = handler.contextualize_file_reads(files, None).unwrap();

        let data: Vec<RecordBatch> = handler
            .read_parquet_files(context, Arc::new(physical_schema.try_into().unwrap()))
            .unwrap()
            .try_collect()
            .unwrap();

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].num_rows(), 10);
    }
}

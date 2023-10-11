//! Default Json handler implementation

use std::io::{BufReader, Cursor};
use std::ops::Range;
use std::sync::Arc;
use std::task::{ready, Poll};

use arrow_array::{RecordBatch, StringArray};
use arrow_json::ReaderBuilder;
use arrow_schema::SchemaRef as ArrowSchemaRef;
use arrow_select::concat::concat_batches;
use bytes::{Buf, Bytes};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{DynObjectStore, GetResultPayload};

use super::file_handler::{FileOpenFuture, FileOpener};
use crate::executor::TaskExecutor;
use crate::file_handler::FileStream;
use crate::schema::SchemaRef;
use crate::{
    DeltaResult, Error, Expression, FileDataReadResultIterator, FileHandler, FileMeta, JsonHandler,
};

pub struct JsonReadContext {
    pub(crate) store: Arc<DynObjectStore>,
    pub(crate) meta: FileMeta,
}

#[derive(Debug)]
pub struct DefaultJsonHandler<E: TaskExecutor> {
    store: Arc<DynObjectStore>,
    task_executor: Arc<E>,
    readahead: usize,
}

impl<E: TaskExecutor> DefaultJsonHandler<E> {
    pub fn new(store: Arc<DynObjectStore>, task_executor: Arc<E>) -> Self {
        Self {
            store,
            task_executor,
            readahead: 10,
        }
    }

    /// Set the maximum number of batches to read ahead during [Self::read_json_files()].
    ///
    /// Defaults to 10.
    pub fn with_readahead(mut self, readahead: usize) -> Self {
        self.readahead = readahead;
        self
    }
}

impl<E: TaskExecutor> FileHandler for DefaultJsonHandler<E> {
    type FileReadContext = JsonReadContext;

    fn contextualize_file_reads(
        &self,
        files: &[FileMeta],
        _predicate: Option<Expression>,
    ) -> DeltaResult<Vec<JsonReadContext>> {
        Ok(files
            .iter()
            .map(|meta| JsonReadContext {
                store: self.store.clone(),
                meta: (*meta).into(),
            })
            .collect())
    }
}

impl<E: TaskExecutor> JsonHandler for DefaultJsonHandler<E> {
    fn parse_json(
        &self,
        json_strings: StringArray,
        output_schema: ArrowSchemaRef,
    ) -> DeltaResult<RecordBatch> {
        // TODO concatenating to a single string is probably not needed if we use the
        // lower level RawDecoder APIs
        let data = json_strings
            .into_iter()
            .filter_map(|d| {
                d.map(|dd| {
                    let mut data = dd.as_bytes().to_vec();
                    data.extend("\n".as_bytes());
                    data
                })
            })
            .flatten()
            .collect::<Vec<_>>();

        let batches = ReaderBuilder::new(output_schema.clone())
            .build(Cursor::new(data))?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(concat_batches(&output_schema, &batches)?)
    }

    fn read_json_files(
        &self,
        files: &[Self::FileReadContext],
        physical_schema: SchemaRef,
    ) -> DeltaResult<FileDataReadResultIterator> {
        if files.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }

        let schema: ArrowSchemaRef = Arc::new(physical_schema.as_ref().try_into()?);
        let store = files.first().unwrap().store.clone();
        let file_reader = JsonOpener::new(1024, schema.clone(), store);

        let files = files.iter().map(|f| f.meta.into()).collect::<Vec<_>>();
        let stream = FileStream::new(files, schema, file_reader)?;

        // This channel will become the output iterator
        // The stream will execute in the background, and we allow up to `readahead`
        // batches to be buffered in the channel.
        let (sender, receiver) = std::sync::mpsc::sync_channel(self.readahead);

        self.task_executor.spawn(stream.for_each(move |res| {
            sender.send(res).ok();
            futures::future::ready(())
        }));

        Ok(Box::new(receiver.into_iter()))
    }
}

/// A [`FileOpener`] that opens a JSON file and yields a [`FileOpenFuture`]
#[allow(missing_debug_implementations)]
pub struct JsonOpener {
    batch_size: usize,
    projected_schema: ArrowSchemaRef,
    object_store: Arc<DynObjectStore>,
}

impl JsonOpener {
    /// Returns a  [`JsonOpener`]
    pub fn new(
        batch_size: usize,
        projected_schema: ArrowSchemaRef,
        // file_compression_type: FileCompressionType,
        object_store: Arc<DynObjectStore>,
    ) -> Self {
        Self {
            batch_size,
            projected_schema,
            // file_compression_type,
            object_store,
        }
    }
}

impl FileOpener for JsonOpener {
    fn open(&self, file_meta: FileMeta, _: Option<Range<i64>>) -> DeltaResult<FileOpenFuture> {
        let store = self.object_store.clone();
        let schema = self.projected_schema.clone();
        let batch_size = self.batch_size;

        Ok(Box::pin(async move {
            let path = Path::from(file_meta.get_location().path());
            match store.get(&path).await?.payload {
                GetResultPayload::File(file, _) => {
                    let reader = ReaderBuilder::new(schema)
                        .with_batch_size(batch_size)
                        .build(BufReader::new(file))?;
                    Ok(futures::stream::iter(reader).map_err(Error::from).boxed())
                }
                GetResultPayload::Stream(s) => {
                    let mut decoder = ReaderBuilder::new(schema)
                        .with_batch_size(batch_size)
                        .build_decoder()?;

                    let mut input = s.map_err(Error::from);
                    let mut buffered = Bytes::new();

                    let s = futures::stream::poll_fn(move |cx| {
                        loop {
                            if buffered.is_empty() {
                                buffered = match ready!(input.poll_next_unpin(cx)) {
                                    Some(Ok(b)) => b,
                                    Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                                    None => break,
                                };
                            }
                            let read = buffered.len();

                            let decoded = match decoder.decode(buffered.as_ref()) {
                                Ok(decoded) => decoded,
                                Err(e) => return Poll::Ready(Some(Err(e.into()))),
                            };

                            buffered.advance(decoded);
                            if decoded != read {
                                break;
                            }
                        }

                        Poll::Ready(decoder.flush().map_err(Error::from).transpose())
                    });
                    Ok(s.map_err(Error::from).boxed())
                }
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::{actions::get_log_schema, executor::tokio::TokioBackgroundExecutor};
    use itertools::Itertools;
    use object_store::{local::LocalFileSystem, ObjectStore};

    #[test]
    fn test_parse_json() {
        let store = Arc::new(LocalFileSystem::new());
        let handler = DefaultJsonHandler::new(store, Arc::new(TokioBackgroundExecutor::new()));

        let json_strings: StringArray = vec![
            r#"{"add":{"path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet","partitionValues":{},"size":635,"modificationTime":1677811178336,"dataChange":true,"stats":"{\"numRecords\":10,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":9},\"nullCount\":{\"value\":0},\"tightBounds\":true}","tags":{"INSERTION_TIME":"1677811178336000","MIN_INSERTION_TIME":"1677811178336000","MAX_INSERTION_TIME":"1677811178336000","OPTIMIZE_TARGET_SIZE":"268435456"}}}"#,
            r#"{"commitInfo":{"timestamp":1677811178585,"operation":"WRITE","operationParameters":{"mode":"ErrorIfExists","partitionBy":"[]"},"isolationLevel":"WriteSerializable","isBlindAppend":true,"operationMetrics":{"numFiles":"1","numOutputRows":"10","numOutputBytes":"635"},"engineInfo":"Databricks-Runtime/<unknown>","txnId":"a6a94671-55ef-450e-9546-b8465b9147de"}}"#,
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#,
            r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableDeletionVectors":"true","delta.columnMapping.mode":"none"},"createdTime":1677811175819}}"#,
        ]
        .into();
        let output_schema = Arc::new(get_log_schema());

        let batch = handler.parse_json(json_strings, output_schema).unwrap();
        assert_eq!(batch.num_rows(), 4);
    }

    #[tokio::test]
    async fn test_read_json_files() {
        let store = Arc::new(LocalFileSystem::new());

        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/table-with-dv-small/_delta_log/00000000000000000000.json",
        ))
        .unwrap();
        let url = url::Url::from_file_path(path).unwrap();
        let location = Path::from(url.path());
        let meta = store.head(&location).await.unwrap();

        let files = &[FileMeta {
            location: url.clone(),
            last_modified: meta.last_modified.timestamp(),
            size: meta.size,
        }];

        let handler = DefaultJsonHandler::new(store, Arc::new(TokioBackgroundExecutor::new()));
        let physical_schema = Arc::new(get_log_schema());
        let context = handler.contextualize_file_reads(files, None).unwrap();
        let data: Vec<RecordBatch> = handler
            .read_json_files(&context, Arc::new(physical_schema.try_into().unwrap()))
            .unwrap()
            .try_collect()
            .unwrap();

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].num_rows(), 4);
    }
}

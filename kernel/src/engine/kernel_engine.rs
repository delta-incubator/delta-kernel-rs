//! Engine handlers bound to an operation's read cancellation scope.

use std::sync::Arc;

use bytes::Bytes;
use derive_more::Constructor;
use url::Url;

use crate::cancellation::check_cancelled;
#[cfg(feature = "declarative-plans")]
use crate::plans::PlanExecutor;
use crate::{
    CancellationTokenRef, Engine, EngineData, EvaluationHandler, FileDataReadResultIterator,
    FileMeta, FileSize, FileSlice, FilteredEngineData, JsonHandler, ParquetFooter, ParquetHandler,
    PredicateRef, Result, ResultIterator, ResultIteratorStatic, SchemaRef, StorageHandler,
};

#[cfg(test)]
mod tests;

/// An engine adapter that supplies an operation's cancellation token to read handlers.
///
/// Owns handles to the supplied engine's handlers. Plain reads and cancellation-aware reads with
/// no explicit token use this operation's token. An explicit per-read token overrides it and is
/// forwarded unchanged. Writes, expression evaluation, and declarative plan execution retain
/// their underlying behavior. HEAD requests check cancellation before starting but cannot be
/// interrupted through the handler API.
///
/// Reuse this adapter across calls that belong to the same operation, including metadata reads
/// after building a snapshot. Snapshots do not retain the adapter or its token. Returned read
/// iterators retain their own cancellation state and can outlive the adapter.
pub struct KernelEngine {
    evaluation: Arc<dyn EvaluationHandler>,
    storage: Arc<dyn StorageHandler>,
    json: Arc<dyn JsonHandler>,
    parquet: Arc<dyn ParquetHandler>,
    #[cfg(feature = "declarative-plans")]
    plan_executor: Option<Arc<dyn PlanExecutor>>,
}

impl KernelEngine {
    /// Creates an adapter over `engine` whose reads use `cancellation_token`.
    ///
    /// Captures the existing handler handles without retaining a borrow of `engine` or issuing I/O.
    pub fn new(engine: &dyn Engine, cancellation_token: CancellationTokenRef) -> Self {
        Self {
            evaluation: engine.evaluation_handler(),
            storage: Arc::new(ReadHandler::new(
                engine.storage_handler(),
                cancellation_token.clone(),
            )),
            json: Arc::new(ReadHandler::new(
                engine.json_handler(),
                cancellation_token.clone(),
            )),
            parquet: Arc::new(ReadHandler::new(
                engine.parquet_handler(),
                cancellation_token,
            )),
            #[cfg(feature = "declarative-plans")]
            plan_executor: engine.plan_executor(),
        }
    }
}

impl Engine for KernelEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.evaluation.clone()
    }

    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.storage.clone()
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json.clone()
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.parquet.clone()
    }

    #[cfg(feature = "declarative-plans")]
    fn plan_executor(&self) -> Option<Arc<dyn PlanExecutor>> {
        self.plan_executor.clone()
    }
}

/// Supplies a default cancellation scope at a handler's read boundary.
#[derive(Constructor)]
struct ReadHandler<H: ?Sized> {
    inner: Arc<H>,
    cancellation_token: CancellationTokenRef,
}

impl<H: ?Sized> ReadHandler<H> {
    fn read_token(&self, token: Option<CancellationTokenRef>) -> Option<CancellationTokenRef> {
        Some(token.unwrap_or_else(|| self.cancellation_token.clone()))
    }
}

impl StorageHandler for ReadHandler<dyn StorageHandler> {
    fn list_from(&self, path: &Url) -> Result<ResultIteratorStatic<FileMeta>> {
        self.list_from_with_cancellation(path, None)
    }

    fn list_from_with_cancellation(
        &self,
        path: &Url,
        token: Option<CancellationTokenRef>,
    ) -> Result<ResultIteratorStatic<FileMeta>> {
        self.inner
            .list_from_with_cancellation(path, self.read_token(token))
    }

    fn read_files(&self, files: Vec<FileSlice>) -> Result<ResultIteratorStatic<Bytes>> {
        self.read_files_with_cancellation(files, None)
    }

    fn read_files_with_cancellation(
        &self,
        files: Vec<FileSlice>,
        token: Option<CancellationTokenRef>,
    ) -> Result<ResultIteratorStatic<Bytes>> {
        self.inner
            .read_files_with_cancellation(files, self.read_token(token))
    }

    fn head(&self, path: &Url) -> Result<FileMeta> {
        check_cancelled(Some(&self.cancellation_token))?;
        self.inner.head(path)
    }

    fn put(&self, path: &Url, data: Bytes, overwrite: bool) -> Result<()> {
        self.inner.put(path, data, overwrite)
    }

    fn copy_atomic(&self, src: &Url, dest: &Url) -> Result<()> {
        self.inner.copy_atomic(src, dest)
    }

    fn delete(&self, path: &Url) -> Result<()> {
        self.inner.delete(path)
    }
}

impl JsonHandler for ReadHandler<dyn JsonHandler> {
    fn read_json_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> Result<FileDataReadResultIterator> {
        self.read_json_files_with_cancellation(files, schema, predicate, None)
    }

    fn read_json_files_with_cancellation(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
        token: Option<CancellationTokenRef>,
    ) -> Result<FileDataReadResultIterator> {
        self.inner.read_json_files_with_cancellation(
            files,
            schema,
            predicate,
            self.read_token(token),
        )
    }

    fn parse_json(
        &self,
        data: Box<dyn EngineData>,
        schema: SchemaRef,
    ) -> Result<Box<dyn EngineData>> {
        self.inner.parse_json(data, schema)
    }

    fn write_json_file(
        &self,
        path: &Url,
        data: ResultIterator<'_, FilteredEngineData>,
        overwrite: bool,
    ) -> Result<FileSize> {
        self.inner.write_json_file(path, data, overwrite)
    }
}

impl ParquetHandler for ReadHandler<dyn ParquetHandler> {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> Result<FileDataReadResultIterator> {
        self.read_parquet_files_with_cancellation(files, schema, predicate, None)
    }

    fn read_parquet_files_with_cancellation(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
        token: Option<CancellationTokenRef>,
    ) -> Result<FileDataReadResultIterator> {
        self.inner.read_parquet_files_with_cancellation(
            files,
            schema,
            predicate,
            self.read_token(token),
        )
    }

    fn read_parquet_footer(&self, file: &FileMeta) -> Result<ParquetFooter> {
        self.read_parquet_footer_with_cancellation(file, None)
    }

    fn read_parquet_footer_with_cancellation(
        &self,
        file: &FileMeta,
        token: Option<CancellationTokenRef>,
    ) -> Result<ParquetFooter> {
        self.inner
            .read_parquet_footer_with_cancellation(file, self.read_token(token))
    }

    fn write_parquet_file(
        &self,
        location: Url,
        data: ResultIteratorStatic<Box<dyn EngineData>>,
    ) -> Result<FileSize> {
        self.inner.write_parquet_file(location, data)
    }
}

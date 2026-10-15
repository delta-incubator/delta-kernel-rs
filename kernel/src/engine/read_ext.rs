//! Imperative read helpers that select cancellation at the I/O boundary.

use bytes::Bytes;
use url::Url;

use crate::cancellation::check_cancelled;
use crate::{
    CancellationTokenRef, Engine, FileDataReadResultIterator, FileMeta, FileSlice, ParquetFooter,
    PredicateRef, Result, ResultIteratorStatic, SchemaRef,
};

/// Routes imperative reads through the engine's cancellation-aware handlers.
///
/// An explicit token overrides the engine token; `None` inherits it. Handler errors and lazy
/// iterators are returned unchanged. HEAD checks cancellation only before issuing the request.
#[allow(
    clippy::disallowed_methods,
    reason = "Read helpers supply the engine token to handlers."
)]
pub(crate) trait EngineReadExt: Engine {
    /// Lists files after `path` using the engine's read token.
    fn list_from(&self, path: &Url) -> Result<ResultIteratorStatic<FileMeta>> {
        self.list_from_with_cancellation(path, None)
    }

    /// Lists files after `path` using an explicit or inherited read token.
    fn list_from_with_cancellation(
        &self,
        path: &Url,
        token: Option<CancellationTokenRef>,
    ) -> Result<ResultIteratorStatic<FileMeta>> {
        self.storage_handler()
            .list_from_with_cancellation(path, token.or_else(|| self.cancellation_token()))
    }

    /// Reads byte ranges from `files` using the engine's read token.
    fn read_files(&self, files: Vec<FileSlice>) -> Result<ResultIteratorStatic<Bytes>> {
        self.read_files_with_cancellation(files, None)
    }

    /// Reads byte ranges from `files` using an explicit or inherited read token.
    fn read_files_with_cancellation(
        &self,
        files: Vec<FileSlice>,
        token: Option<CancellationTokenRef>,
    ) -> Result<ResultIteratorStatic<Bytes>> {
        self.storage_handler()
            .read_files_with_cancellation(files, token.or_else(|| self.cancellation_token()))
    }

    /// Returns metadata for `path`, failing before the request if the engine token is cancelled.
    fn head(&self, path: &Url) -> Result<FileMeta> {
        check_cancelled(self.cancellation_token().as_ref())?;
        self.storage_handler().head(path)
    }

    /// Reads projected JSON batches from `files` using the engine's read token.
    fn read_json_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> Result<FileDataReadResultIterator> {
        self.read_json_files_with_cancellation(files, schema, predicate, None)
    }

    /// Reads projected JSON batches using an explicit or inherited read token.
    fn read_json_files_with_cancellation(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
        token: Option<CancellationTokenRef>,
    ) -> Result<FileDataReadResultIterator> {
        self.json_handler().read_json_files_with_cancellation(
            files,
            schema,
            predicate,
            token.or_else(|| self.cancellation_token()),
        )
    }

    /// Reads projected Parquet batches from `files` using the engine's read token.
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> Result<FileDataReadResultIterator> {
        self.read_parquet_files_with_cancellation(files, schema, predicate, None)
    }

    /// Reads projected Parquet batches using an explicit or inherited read token.
    fn read_parquet_files_with_cancellation(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
        token: Option<CancellationTokenRef>,
    ) -> Result<FileDataReadResultIterator> {
        self.parquet_handler().read_parquet_files_with_cancellation(
            files,
            schema,
            predicate,
            token.or_else(|| self.cancellation_token()),
        )
    }

    /// Reads the Parquet footer for `file` using the engine's read token.
    #[allow(
        dead_code,
        reason = "Provides the implicit-token counterpart to explicit footer reads."
    )]
    fn read_parquet_footer(&self, file: &FileMeta) -> Result<ParquetFooter> {
        self.read_parquet_footer_with_cancellation(file, None)
    }

    /// Reads the Parquet footer using an explicit or inherited read token.
    fn read_parquet_footer_with_cancellation(
        &self,
        file: &FileMeta,
        token: Option<CancellationTokenRef>,
    ) -> Result<ParquetFooter> {
        self.parquet_handler()
            .read_parquet_footer_with_cancellation(
                file,
                token.or_else(|| self.cancellation_token()),
            )
    }
}

impl<E: Engine + ?Sized> EngineReadExt for E {}

//! Imperative write helpers that preserve the underlying handler's behavior.

use bytes::Bytes;
use url::Url;

use crate::{
    Engine, EngineData, FileSize, FilteredEngineData, Result, ResultIterator, ResultIteratorStatic,
};

/// Routes imperative writes through the engine without applying its read cancellation token.
#[allow(
    clippy::disallowed_methods,
    reason = "Write helpers delegate to the original handlers."
)]
pub(crate) trait EngineWriteExt: Engine {
    /// Atomically copies `src` to `dest` without overwriting an existing destination.
    #[allow(dead_code, reason = "Exposes the complete storage write helper API.")]
    fn copy_atomic(&self, src: &Url, dest: &Url) -> Result<()> {
        self.storage_handler().copy_atomic(src, dest)
    }

    /// Deletes `path`, succeeding if the file is already absent.
    #[allow(dead_code, reason = "Exposes the complete storage write helper API.")]
    fn delete(&self, path: &Url) -> Result<()> {
        self.storage_handler().delete(path)
    }

    /// Writes `data` to `path`, replacing an existing file only when `overwrite` is true.
    fn put(&self, path: &Url, data: Bytes, overwrite: bool) -> Result<()> {
        self.storage_handler().put(path, data, overwrite)
    }

    /// Writes selected rows as JSON at `path` and returns the file size.
    fn write_json_file(
        &self,
        path: &Url,
        data: ResultIterator<'_, FilteredEngineData>,
        overwrite: bool,
    ) -> Result<FileSize> {
        self.json_handler().write_json_file(path, data, overwrite)
    }

    /// Writes batches as Parquet at `location` and returns the file size.
    fn write_parquet_file(
        &self,
        location: Url,
        data: ResultIteratorStatic<Box<dyn EngineData>>,
    ) -> Result<FileSize> {
        self.parquet_handler().write_parquet_file(location, data)
    }
}

impl<E: Engine + ?Sized> EngineWriteExt for E {}

#[cfg(test)]
mod tests;

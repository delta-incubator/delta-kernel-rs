//! Operation-scoped reads for snapshot loading.

use bytes::Bytes;
use url::Url;

use crate::log_replay::ActionsBatch;
use crate::log_segment::{ActionsWithCheckpointInfo, LogSegment};
use crate::log_segment_files::{CheckpointHandling, LogSegmentFiles};
use crate::metrics::SnapshotLoadMetricContext;
use crate::path::ParsedLogPath;
use crate::schema::SchemaRef;
use crate::{
    CancellationTokenRef, Engine, FileDataReadResultIterator, FileMeta, FileSlice, KernelResult,
    KernelResultIteratorStatic, Version,
};

#[cfg(test)]
mod tests;

/// Borrows a reusable engine and binds its reads to one operation.
pub(crate) struct KernelReader<'a> {
    engine: &'a dyn Engine,

    cancellation_token: Option<CancellationTokenRef>,
}

impl<'a> KernelReader<'a> {
    pub(crate) fn new(
        engine: &'a dyn Engine,
        cancellation_token: Option<CancellationTokenRef>,
    ) -> Self {
        Self {
            engine,
            cancellation_token,
        }
    }

    #[cfg(feature = "declarative-plans")]
    pub(crate) fn engine(&self) -> &'a dyn Engine {
        self.engine
    }

    pub(crate) fn discover_snapshot(
        &self,
        log_root: Url,
        log_tail: Vec<ParsedLogPath>,
        version: Option<Version>,
        metric_context: SnapshotLoadMetricContext,
    ) -> KernelResult<LogSegment> {
        LogSegment::for_snapshot(
            self.engine.storage_handler().as_ref(),
            log_root,
            log_tail,
            version,
            metric_context,
            self.cancellation_token.as_ref(),
        )
    }

    pub(crate) fn list_log_files(
        &self,
        log_root: &Url,
        log_tail: Vec<ParsedLogPath>,
        start_version: Option<Version>,
        end_version: Option<Version>,
        checkpoint_handling: CheckpointHandling,
    ) -> KernelResult<LogSegmentFiles> {
        LogSegmentFiles::list_with_checkpoint_handling(
            self.engine.storage_handler().as_ref(),
            log_root,
            log_tail,
            start_version,
            end_version,
            checkpoint_handling,
            self.cancellation_token.as_ref(),
        )
    }

    pub(crate) fn read_files(
        &self,
        files: Vec<FileSlice>,
    ) -> KernelResult<KernelResultIteratorStatic<Bytes>> {
        self.engine
            .storage_handler()
            .read_files_with_cancellation(files, self.cancellation_token.clone())
    }

    pub(crate) fn read_json_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
    ) -> KernelResult<FileDataReadResultIterator> {
        self.engine
            .json_handler()
            .read_json_files_with_cancellation(files, schema, None, self.cancellation_token.clone())
    }

    /// Reads commit and checkpoint actions with one scope, including any required sidecar reads.
    pub(crate) fn read_actions(
        &self,
        segment: &LogSegment,
        commit_schema: SchemaRef,
        checkpoint_schema: SchemaRef,
    ) -> KernelResult<
        ActionsWithCheckpointInfo<impl Iterator<Item = KernelResult<ActionsBatch>> + Send>,
    > {
        segment.read_actions_with_projected_checkpoint_actions(
            self.engine,
            commit_schema,
            checkpoint_schema,
            None,
            None,
            None,
            self.cancellation_token.as_ref(),
        )
    }
}

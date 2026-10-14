//! Snapshot-build cancellation through real engine read paths.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use rstest::rstest;
use url::Url;

use crate::engine::sync::SyncEngine;
use crate::engine::test_delegating::DelegatingEngine;
#[cfg(feature = "declarative-plans")]
use crate::kernel_reader::KernelReader;
use crate::schema::SchemaRef;
use crate::snapshot::IncrementalReplay;
use crate::unit_test_utils::TestCancellationToken;
use crate::{
    CancellationTokenRef, Engine, EngineData, FileDataReadResultIterator, FileMeta, FileSize,
    FileSlice, FilteredEngineData, JsonHandler, KernelError, KernelResult, ParquetFooter,
    ParquetHandler, PredicateRef, ResultIterator, ResultIteratorStatic, Snapshot, StorageHandler,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Read {
    List,
    Hint,
    Crc,
    Json,
    Parquet,
    Footer,
}

/// Records scopes at the real engine's read boundaries; all data comes from the inner handlers.
struct RecordingReads {
    inner: Arc<dyn Engine>,
    token: Arc<TestCancellationToken>,
    expected_token: Option<CancellationTokenRef>,
    cancel_on: Option<Read>,
    cancel_json_after_batch: bool,
    events: Mutex<Vec<Read>>,
}

impl RecordingReads {
    fn new(cancel_on: Option<Read>) -> Arc<Self> {
        let token = Arc::new(TestCancellationToken::default());
        Arc::new(Self {
            inner: Arc::new(SyncEngine::new()),
            expected_token: Some(token.clone()),
            token,
            cancel_on,
            cancel_json_after_batch: false,
            events: Mutex::new(Vec::new()),
        })
    }

    fn engine(self: &Arc<Self>) -> DelegatingEngine {
        let engine = DelegatingEngine::new(self.inner.clone())
            .with_storage_handler(self.clone())
            .with_json_handler(self.clone())
            .with_parquet_handler(self.clone());
        #[cfg(feature = "declarative-plans")]
        let engine = engine.without_plan_executor();
        engine
    }

    fn record(&self, read: Read, token: &Option<CancellationTokenRef>) -> KernelResult<()> {
        match (token, &self.expected_token) {
            (Some(token), Some(expected)) => assert!(Arc::ptr_eq(token, expected)),
            (None, None) => {}
            _ => panic!("missing or replaced token on {read:?}"),
        }
        self.events.lock().unwrap().push(read);
        if self.cancel_on == Some(read) {
            self.token.cancel();
            if token.is_none() {
                return Err(KernelError::Cancelled);
            }
        }
        Ok(())
    }
}

impl StorageHandler for RecordingReads {
    fn list_from(&self, _path: &Url) -> KernelResult<ResultIteratorStatic<FileMeta>> {
        panic!("unscoped listing");
    }

    fn list_from_with_cancellation(
        &self,
        path: &Url,
        token: Option<CancellationTokenRef>,
    ) -> KernelResult<ResultIteratorStatic<FileMeta>> {
        self.record(Read::List, &token)?;
        self.inner
            .storage_handler()
            .list_from_with_cancellation(path, token)
    }

    fn read_files(&self, _files: Vec<FileSlice>) -> KernelResult<ResultIteratorStatic<Bytes>> {
        panic!("unscoped raw read");
    }

    fn read_files_with_cancellation(
        &self,
        files: Vec<FileSlice>,
        token: Option<CancellationTokenRef>,
    ) -> KernelResult<ResultIteratorStatic<Bytes>> {
        let read = if files.iter().any(|(url, _)| url.path().ends_with(".crc")) {
            Read::Crc
        } else {
            Read::Hint
        };
        self.record(read, &token)?;
        self.inner
            .storage_handler()
            .read_files_with_cancellation(files, token)
    }

    fn put(&self, path: &Url, data: Bytes, overwrite: bool) -> KernelResult<()> {
        self.inner.storage_handler().put(path, data, overwrite)
    }

    fn head(&self, path: &Url) -> KernelResult<FileMeta> {
        self.inner.storage_handler().head(path)
    }

    fn delete(&self, path: &Url) -> KernelResult<()> {
        self.inner.storage_handler().delete(path)
    }

    fn copy_atomic(&self, src: &Url, dest: &Url) -> KernelResult<()> {
        self.inner.storage_handler().copy_atomic(src, dest)
    }
}

impl JsonHandler for RecordingReads {
    fn parse_json(
        &self,
        data: Box<dyn EngineData>,
        schema: SchemaRef,
    ) -> KernelResult<Box<dyn EngineData>> {
        self.inner.json_handler().parse_json(data, schema)
    }

    fn read_json_files(
        &self,
        _files: &[FileMeta],
        _schema: SchemaRef,
        _predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        panic!("unscoped JSON read");
    }

    fn read_json_files_with_cancellation(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
        token: Option<CancellationTokenRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        if !files.is_empty() {
            self.record(Read::Json, &token)?;
        }
        let batches = self
            .inner
            .json_handler()
            .read_json_files_with_cancellation(files, schema, predicate, token)?;
        if self.cancel_json_after_batch {
            let token = self.token.clone();
            Ok(Box::new(batches.inspect(move |_| token.cancel())))
        } else {
            Ok(batches)
        }
    }

    fn write_json_file(
        &self,
        path: &Url,
        data: ResultIterator<'_, FilteredEngineData>,
        overwrite: bool,
    ) -> KernelResult<FileSize> {
        self.inner
            .json_handler()
            .write_json_file(path, data, overwrite)
    }
}

impl ParquetHandler for RecordingReads {
    fn read_parquet_files(
        &self,
        _files: &[FileMeta],
        _schema: SchemaRef,
        _predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        panic!("unscoped Parquet read");
    }

    fn read_parquet_files_with_cancellation(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
        token: Option<CancellationTokenRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        self.record(Read::Parquet, &token)?;
        self.inner
            .parquet_handler()
            .read_parquet_files_with_cancellation(files, schema, predicate, token)
    }

    fn read_parquet_footer(&self, _file: &FileMeta) -> KernelResult<ParquetFooter> {
        panic!("unscoped footer read");
    }

    fn read_parquet_footer_with_cancellation(
        &self,
        file: &FileMeta,
        token: Option<CancellationTokenRef>,
    ) -> KernelResult<ParquetFooter> {
        self.record(Read::Footer, &token)?;
        self.inner
            .parquet_handler()
            .read_parquet_footer_with_cancellation(file, token)
    }

    fn write_parquet_file(
        &self,
        location: Url,
        data: ResultIteratorStatic<Box<dyn EngineData>>,
    ) -> KernelResult<FileSize> {
        self.inner
            .parquet_handler()
            .write_parquet_file(location, data)
    }
}

fn table_url(table: &str) -> Url {
    Url::from_directory_path(
        std::fs::canonicalize(PathBuf::from("tests/data").join(table)).unwrap(),
    )
    .unwrap()
}

#[rstest]
#[case("table-without-dv-small", Read::Json)]
#[case("crc-full", Read::Crc)]
#[case("parquet_row_group_skipping", Read::Parquet)]
fn snapshot_reads_keep_builder_token(
    #[case] table: &str,
    #[case] read: Read,
    #[values(false, true)] cancel: bool,
) {
    let reads = RecordingReads::new(cancel.then_some(read));
    let engine = reads.engine();
    let token: CancellationTokenRef = reads.token.clone();
    let result = Snapshot::builder_for(table_url(table))
        .with_cancellation_token(token)
        .build(&engine);
    if cancel {
        assert!(matches!(result, Err(KernelError::Cancelled)));
        assert_eq!(reads.events.lock().unwrap().last(), Some(&read));
    } else {
        let actual = result.unwrap();
        let expected = Snapshot::builder_for(table_url(table))
            .build(reads.inner.as_ref())
            .unwrap();
        assert_eq!(actual.as_ref(), expected.as_ref());
        assert!(reads.events.lock().unwrap().contains(&read));
    }
}

#[rstest]
#[case("table-with-dv-small", Read::Json)]
#[case("parquet_row_group_skipping", Read::Parquet)]
fn incremental_reads_keep_builder_token(
    #[case] table: &str,
    #[case] read: Read,
    #[values(false, true)] cancel: bool,
) {
    let reads = RecordingReads::new(cancel.then_some(read));
    let base = Snapshot::builder_for(table_url(table))
        .at_version(0)
        .build(reads.inner.as_ref())
        .unwrap();
    let token: CancellationTokenRef = reads.token.clone();
    let result = Snapshot::builder_from(base)
        .with_cancellation_token(token)
        .build(&reads.engine());
    if cancel {
        assert!(matches!(result, Err(KernelError::Cancelled)));
    } else {
        assert_eq!(result.unwrap().version(), 1);
    }
    assert!(reads.events.lock().unwrap().contains(&read));
}

#[rstest]
fn stale_crc_replay_keeps_builder_token(
    #[values(false, true)] incremental: bool,
    #[values(None, Some(false), Some(true))] cancel_after_batch: Option<bool>,
) {
    let temp = tempfile::tempdir().unwrap();
    let log = temp.path().join("_delta_log");
    std::fs::create_dir(&log).unwrap();
    for suffix in ["json", "crc"] {
        let file = format!("00000000000000000000.{suffix}");
        std::fs::copy(
            PathBuf::from("tests/data/crc-full/_delta_log").join(&file),
            log.join(file),
        )
        .unwrap();
    }
    std::fs::write(
        log.join("00000000000000000001.json"),
        r#"{"commitInfo":{"operation":"WRITE"}}"#,
    )
    .unwrap();
    let url = Url::from_directory_path(temp.path()).unwrap();
    let mut reads = RecordingReads::new((cancel_after_batch == Some(false)).then_some(Read::Json));
    Arc::get_mut(&mut reads).unwrap().cancel_json_after_batch = cancel_after_batch == Some(true);
    let engine = reads.engine();
    let token: CancellationTokenRef = reads.token.clone();
    let result = if incremental {
        let base = Snapshot::builder_for(&url)
            .at_version(0)
            .build(reads.inner.as_ref())
            .unwrap();
        Snapshot::builder_from(base)
            .with_incremental_crc_replay(IncrementalReplay::Unlimited)
            .with_cancellation_token(token)
            .build(&engine)
    } else {
        Snapshot::builder_for(&url)
            .with_incremental_crc_replay(IncrementalReplay::Unlimited)
            .with_cancellation_token(token)
            .build(&engine)
    };
    if cancel_after_batch.is_some() {
        assert!(matches!(result, Err(KernelError::Cancelled)));
    } else {
        assert_eq!(result.unwrap().crc_at_version().unwrap().version, 1);
    }
    let events = reads.events.lock().unwrap();
    assert_eq!(events.iter().filter(|read| **read == Read::Json).count(), 1);
}

#[rstest]
#[case(Read::Hint)]
#[case(Read::List)]
fn discovery_keeps_builder_token(#[case] read: Read) {
    let reads = RecordingReads::new(Some(read));
    let token: CancellationTokenRef = reads.token.clone();
    let result = Snapshot::builder_for(table_url("table-without-dv-small"))
        .with_cancellation_token(token)
        .build(&reads.engine());
    assert!(matches!(result, Err(KernelError::Cancelled)));
    assert_eq!(reads.events.lock().unwrap().last(), Some(&read));
}

#[test]
fn engine_reported_crc_cancellation_is_not_optional() {
    let mut reads = RecordingReads::new(Some(Read::Crc));
    Arc::get_mut(&mut reads).unwrap().expected_token = None;
    let result = Snapshot::builder_for(table_url("crc-full")).build(&reads.engine());
    assert!(matches!(result, Err(KernelError::Cancelled)));
    assert_eq!(reads.events.lock().unwrap().last(), Some(&Read::Crc));
}

#[test]
fn completed_snapshot_does_not_retain_build_scope() {
    let engine = SyncEngine::new();
    let cancelled: CancellationTokenRef = Arc::new(TestCancellationToken::cancelled());
    assert!(matches!(
        Snapshot::builder_for(table_url("table-without-dv-small"))
            .with_cancellation_token(cancelled)
            .build(&engine),
        Err(KernelError::Cancelled)
    ));
    let token: CancellationTokenRef = Arc::new(TestCancellationToken::default());
    let weak = Arc::downgrade(&token);
    let snapshot = Snapshot::builder_for(table_url("table-without-dv-small"))
        .with_cancellation_token(token)
        .build(&engine)
        .unwrap();
    assert!(weak.upgrade().is_none());
    assert_eq!(snapshot.version(), 0);
    Snapshot::builder_from(snapshot)
        .at_version(0)
        .with_cancellation_token(
            Arc::new(TestCancellationToken::cancelled()) as CancellationTokenRef
        )
        .build(&engine)
        .unwrap();
}

#[cfg(feature = "declarative-plans")]
#[test]
fn declarative_replay_does_not_use_reader_cancellation() {
    let engine = SyncEngine::new();
    let snapshot = Snapshot::builder_for(table_url("table-without-dv-small"))
        .build(&engine)
        .unwrap();
    let token: CancellationTokenRef = Arc::new(TestCancellationToken::cancelled());
    let reader = KernelReader::new(&engine, Some(token));
    let resolution = snapshot
        .log_segment()
        .read_protocol_metadata_opt(&reader, None)
        .unwrap();
    assert!(resolution.metadata.is_some());
    assert!(resolution.protocol.is_some());
}

//! Cancellation and ownership through real engine read paths.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use rstest::rstest;
use url::Url;

use crate::actions::LOG_METADATA_SCHEMA;
use crate::committer::FileSystemCommitter;
use crate::engine::sync::SyncEngine;
use crate::engine::test_delegating::DelegatingEngine;
use crate::engine::{EngineReadExt, KernelEngine};
use crate::metrics::MeteredDeltaEngine;
use crate::object_store::memory::InMemory;
use crate::schema::{schema_ref, ColumnName, SchemaRef};
use crate::snapshot::IncrementalReplay;
use crate::transaction::create_table::create_table;
use crate::transaction::data_layout::DataLayout;
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

#[allow(
    clippy::disallowed_methods,
    reason = "The test adapter exercises handler calls directly."
)]
impl StorageHandler for RecordingReads {
    fn list_from(&self, path: &Url) -> KernelResult<ResultIteratorStatic<FileMeta>> {
        self.list_from_with_cancellation(path, None)
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

    fn read_files(&self, files: Vec<FileSlice>) -> KernelResult<ResultIteratorStatic<Bytes>> {
        self.read_files_with_cancellation(files, None)
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

#[allow(
    clippy::disallowed_methods,
    reason = "The test adapter exercises handler calls directly."
)]
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
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        self.read_json_files_with_cancellation(files, schema, predicate, None)
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

#[allow(
    clippy::disallowed_methods,
    reason = "The test adapter exercises handler calls directly."
)]
impl ParquetHandler for RecordingReads {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        self.read_parquet_files_with_cancellation(files, schema, predicate, None)
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

    fn read_parquet_footer(&self, file: &FileMeta) -> KernelResult<ParquetFooter> {
        self.read_parquet_footer_with_cancellation(file, None)
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

#[derive(Clone, Copy)]
enum ReadApi {
    Plain,
    WithCancellation,
}

fn start_read(
    engine: &dyn Engine,
    read: Read,
    api: ReadApi,
    token: Option<CancellationTokenRef>,
) -> KernelResult<ResultIteratorStatic<()>> {
    let log = table_url("parquet_row_group_skipping").join("_delta_log/")?;
    let file_name = match read {
        Read::List => "00000000000000000000",
        Read::Hint => "_last_checkpoint",
        Read::Crc => "00000000000000000000.crc",
        Read::Json => "00000000000000000000.json",
        Read::Parquet | Read::Footer => {
            "00000000000000000001.checkpoint.0000000001.0000000005.parquet"
        }
    };
    let path = if read == Read::Crc {
        table_url("crc-full").join("_delta_log/00000000000000000000.crc")?
    } else {
        log.join(file_name)?
    };
    let file = FileMeta::new(path.clone(), 0, 0);
    let schema = LOG_METADATA_SCHEMA.clone();
    match (read, api) {
        (Read::List, ReadApi::Plain) => {
            Ok(Box::new(engine.list_from(&path)?.map(|r| r.map(|_| ()))))
        }
        (Read::List, ReadApi::WithCancellation) => Ok(Box::new(
            engine
                .list_from_with_cancellation(&path, token)?
                .map(|r| r.map(|_| ())),
        )),
        (Read::Hint | Read::Crc, ReadApi::Plain) => Ok(Box::new(
            engine
                .read_files(vec![(path, None)])?
                .map(|r| r.map(|_| ())),
        )),
        (Read::Hint | Read::Crc, ReadApi::WithCancellation) => Ok(Box::new(
            engine
                .read_files_with_cancellation(vec![(path, None)], token)?
                .map(|r| r.map(|_| ())),
        )),
        (Read::Json, ReadApi::Plain) => Ok(Box::new(
            engine
                .read_json_files(&[file], schema, None)?
                .map(|r| r.map(|_| ())),
        )),
        (Read::Json, ReadApi::WithCancellation) => Ok(Box::new(
            engine
                .read_json_files_with_cancellation(&[file], schema, None, token)?
                .map(|r| r.map(|_| ())),
        )),
        (Read::Parquet, ReadApi::Plain) => Ok(Box::new(
            engine
                .read_parquet_files(&[file], schema, None)?
                .map(|r| r.map(|_| ())),
        )),
        (Read::Parquet, ReadApi::WithCancellation) => Ok(Box::new(
            engine
                .read_parquet_files_with_cancellation(&[file], schema, None, token)?
                .map(|r| r.map(|_| ())),
        )),
        (Read::Footer, ReadApi::Plain) => {
            engine.read_parquet_footer(&file)?;
            Ok(Box::new(std::iter::once(Ok(()))))
        }
        (Read::Footer, ReadApi::WithCancellation) => {
            engine.read_parquet_footer_with_cancellation(&file, token)?;
            Ok(Box::new(std::iter::once(Ok(()))))
        }
    }
}

#[rstest]
fn engine_reads_inherit_scope_and_explicit_tokens_override_it(
    #[values(
        Read::List,
        Read::Hint,
        Read::Crc,
        Read::Json,
        Read::Parquet,
        Read::Footer
    )]
    read: Read,
    #[values(ReadApi::Plain, ReadApi::WithCancellation)] api: ReadApi,
    #[values(false, true)] cancelled: bool,
) {
    let reads = RecordingReads::new(None);
    if cancelled {
        reads.token.cancel();
    }
    let engine = KernelEngine::new(&reads.engine(), reads.token.clone());
    let result = start_read(&engine, read, api, None)
        .and_then(|mut iter| iter.next().expect("fixture must produce a result"));
    if cancelled {
        assert!(matches!(result, Err(KernelError::Cancelled)));
    } else {
        result.unwrap();
    }
    assert_eq!(*reads.events.lock().unwrap(), [read]);

    let explicit = RecordingReads::new(None);
    let engine = KernelEngine::new(
        &explicit.engine(),
        Arc::new(TestCancellationToken::cancelled()),
    );
    start_read(
        &engine,
        read,
        ReadApi::WithCancellation,
        Some(explicit.token.clone()),
    )
    .unwrap()
    .next()
    .unwrap()
    .unwrap();
    assert_eq!(*explicit.events.lock().unwrap(), [read]);
}

#[rstest]
fn iterators_keep_scope_after_engine_is_dropped(
    #[values(Read::List, Read::Hint, Read::Crc, Read::Json, Read::Parquet)] read: Read,
    #[values(false, true)] cancel: bool,
) {
    let token = Arc::new(TestCancellationToken::default());
    let mut iter = {
        let engine = KernelEngine::new(&SyncEngine::new(), token.clone());
        start_read(&engine, read, ReadApi::Plain, None).unwrap()
    };
    if cancel {
        token.cancel();
        assert!(matches!(iter.next(), Some(Err(KernelError::Cancelled))));
    } else {
        iter.next().unwrap().unwrap();
    }
}

#[rstest]
fn unscoped_engine_reads_pass_none(
    #[values(
        Read::List,
        Read::Hint,
        Read::Crc,
        Read::Json,
        Read::Parquet,
        Read::Footer
    )]
    read: Read,
) {
    let mut reads = RecordingReads::new(None);
    Arc::get_mut(&mut reads).unwrap().expected_token = None;
    start_read(&reads.engine(), read, ReadApi::Plain, None)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(*reads.events.lock().unwrap(), [read]);
}

#[test]
fn adapter_preserves_handler_identity_and_builder_token_overrides_engine_token() {
    let reads = RecordingReads::new(None);
    let original = reads.engine();
    let engine = KernelEngine::new(&original, Arc::new(TestCancellationToken::cancelled()));
    assert!(Arc::ptr_eq(
        &engine.storage_handler(),
        &original.storage_handler()
    ));
    assert!(Arc::ptr_eq(
        &engine.json_handler(),
        &original.json_handler()
    ));
    assert!(Arc::ptr_eq(
        &engine.parquet_handler(),
        &original.parquet_handler()
    ));
    assert!(Arc::ptr_eq(
        &engine.evaluation_handler(),
        &original.evaluation_handler()
    ));
    Snapshot::builder_for(table_url("table-without-dv-small"))
        .with_cancellation_token(reads.token.clone() as CancellationTokenRef)
        .build(&engine)
        .unwrap();
    let events = reads.events.lock().unwrap();
    assert!(events.contains(&Read::Hint));
    assert!(events.contains(&Read::List));
    assert!(events.contains(&Read::Json));
}

#[test]
fn metered_engine_preserves_operation_token() {
    let token = Arc::new(TestCancellationToken::default());
    let engine = MeteredDeltaEngine::new(Arc::new(KernelEngine::new(
        &SyncEngine::new(),
        token.clone(),
    )));
    let mut iter = start_read(&engine, Read::Json, ReadApi::Plain, None).unwrap();
    token.cancel();
    assert!(matches!(iter.next(), Some(Err(KernelError::Cancelled))));
}

#[test]
fn nested_scope_uses_inner_operation_token_and_keeps_writes_unscoped() {
    let engine = SyncEngine::new();
    let cancelled: CancellationTokenRef = Arc::new(TestCancellationToken::cancelled());
    let outer = KernelEngine::new(&engine, cancelled);
    let inner = KernelEngine::new(&outer, Arc::new(TestCancellationToken::default()));
    start_read(&inner, Read::Json, ReadApi::Plain, None)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();

    let temp = tempfile::tempdir().unwrap();
    let path = Url::from_file_path(temp.path().join("data")).unwrap();
    outer
        .storage_handler()
        .put(&path, Bytes::from_static(b"data"), false)
        .unwrap();
    assert!(matches!(outer.head(&path), Err(KernelError::Cancelled)));
    assert_eq!(
        engine
            .read_files(vec![(path.clone(), None)])
            .unwrap()
            .next()
            .unwrap()
            .unwrap(),
        Bytes::from_static(b"data")
    );
    outer.storage_handler().delete(&path).unwrap();
}

#[rstest]
#[case("table-without-dv-small", Read::Json)]
#[case("crc-full", Read::Crc)]
#[case("parquet_row_group_skipping", Read::Parquet)]
fn snapshot_reads_keep_builder_token(
    #[case] table: &str,
    #[case] read: Read,
    #[values(false, true)] cancel: bool,
    #[values(false, true)] bind_in_builder: bool,
) {
    let reads = RecordingReads::new(cancel.then_some(read));
    let engine = reads.engine();
    let token: CancellationTokenRef = reads.token.clone();
    let builder = Snapshot::builder_for(table_url(table));
    let result = if bind_in_builder {
        builder.with_cancellation_token(token).build(&engine)
    } else {
        builder.build(&KernelEngine::new(&engine, token))
    };
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
    #[values(false, true)] bind_in_builder: bool,
) {
    let reads = RecordingReads::new(cancel.then_some(read));
    let base = Snapshot::builder_for(table_url(table))
        .at_version(0)
        .build(reads.inner.as_ref())
        .unwrap();
    let token: CancellationTokenRef = reads.token.clone();
    let builder = Snapshot::builder_from(base);
    let result = if bind_in_builder {
        builder
            .with_cancellation_token(token)
            .build(&reads.engine())
    } else {
        builder.build(&KernelEngine::new(&reads.engine(), token))
    };
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

/// Lazy snapshot metadata that requires log replay when no CRC is available.
#[derive(Clone, Copy, Debug)]
enum MetadataRead {
    Domain,
    Transaction,
    Clustering,
}

#[rstest]
fn post_build_metadata_reads_inherit_operation_token(
    #[values(
        MetadataRead::Domain,
        MetadataRead::Transaction,
        MetadataRead::Clustering
    )]
    read: MetadataRead,
    #[values(false, true)] cancelled: bool,
) {
    let original = SyncEngine::new_with_store(Arc::new(InMemory::new()));
    create_table("memory:///", schema_ref! { nullable "id": INTEGER }, "test")
        .with_domain_metadata("test", "value")
        .with_transaction_id("app", 7)
        .with_data_layout(DataLayout::Clustered {
            columns: vec![ColumnName::new(["id"])],
        })
        .build(&original, Box::new(FileSystemCommitter::new()))
        .unwrap()
        .commit(&original)
        .unwrap()
        .unwrap_committed();
    let mut reads = RecordingReads::new(None);
    Arc::get_mut(&mut reads).unwrap().inner = Arc::new(original);
    let engine = KernelEngine::new(&reads.engine(), reads.token.clone());
    let snapshot = Snapshot::builder_for("memory:///").build(&engine).unwrap();
    assert!(snapshot.crc_at_version().is_none());
    reads.events.lock().unwrap().clear();
    if cancelled {
        reads.token.cancel();
    }
    let load = |engine: &dyn Engine| -> KernelResult<()> {
        match read {
            MetadataRead::Domain => assert_eq!(
                snapshot.get_domain_metadata("test", engine)?,
                Some("value".into())
            ),
            MetadataRead::Transaction => {
                assert_eq!(snapshot.get_app_id_version("app", engine)?, Some(7));
            }
            MetadataRead::Clustering => assert_eq!(
                snapshot.get_physical_clustering_columns(engine)?,
                Some(vec![ColumnName::new(["id"])])
            ),
        }
        Ok(())
    };
    let result = load(&engine);
    if cancelled {
        assert!(matches!(result, Err(KernelError::Cancelled)), "{result:?}");
    } else {
        result.unwrap();
    }
    assert_eq!(*reads.events.lock().unwrap(), [Read::Json]);
    load(reads.inner.as_ref()).unwrap();
}

#[cfg(feature = "declarative-plans")]
#[test]
fn declarative_replay_does_not_use_handler_cancellation() {
    let engine = SyncEngine::new();
    let snapshot = Snapshot::builder_for(table_url("table-without-dv-small"))
        .build(&engine)
        .unwrap();
    let token: CancellationTokenRef = Arc::new(TestCancellationToken::cancelled());
    let engine = KernelEngine::new(&engine, token);
    let resolution = snapshot
        .log_segment()
        .read_protocol_metadata_opt(&engine, None)
        .unwrap();
    assert!(resolution.metadata.is_some());
    assert!(resolution.protocol.is_some());
}

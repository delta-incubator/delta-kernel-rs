//! Integration tests for the checkpoint schema that `Snapshot::checkpoint` records in the
//! `_last_checkpoint` hint and keeps in the snapshot it returns.

use std::sync::{Arc, Mutex};

use delta_kernel::arrow::array::{Int32Array, RecordBatch};
use delta_kernel::checkpoint::{CheckpointSpec, V2CheckpointConfig};
use delta_kernel::schema::{schema_ref, SchemaRef};
use delta_kernel::snapshot::CheckpointWriteResult;
use delta_kernel::{
    Engine, EngineData, EvaluationHandler, FileDataReadResultIterator, FileMeta, FileSize,
    JsonHandler, KernelResult, KernelResultIteratorStatic, ParquetFooter, ParquetHandler,
    PredicateRef, Snapshot, SnapshotRef, StorageHandler,
};
use rstest::rstest;
use test_utils::delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use test_utils::delta_kernel_default_engine::{DefaultEngine, DefaultEngineBuilder};
use test_utils::table_builder::{
    checkpoint_struct_stats, partitioned, test_table, FeatureSet, LogState,
};
use test_utils::{create_table_and_load_snapshot, insert_data, read_scan, test_table_setup_mt};
use url::Url;

/// Delegates to `inner` and records the file of each `read_parquet_footer` call.
struct FooterCountingParquetHandler {
    inner: Arc<dyn ParquetHandler>,
    footer_reads: Mutex<Vec<Url>>,
}

impl ParquetHandler for FooterCountingParquetHandler {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        self.inner
            .read_parquet_files(files, physical_schema, predicate)
    }

    fn write_parquet_file(
        &self,
        location: Url,
        data: KernelResultIteratorStatic<Box<dyn EngineData>>,
    ) -> KernelResult<FileSize> {
        self.inner.write_parquet_file(location, data)
    }

    fn read_parquet_footer(&self, file: &FileMeta) -> KernelResult<ParquetFooter> {
        self.footer_reads
            .lock()
            .unwrap()
            .push(file.location.clone());
        self.inner.read_parquet_footer(file)
    }
}

/// Delegates to `inner`, but its parquet handler counts footer reads.
struct FooterCountingEngine {
    inner: Arc<dyn Engine>,
    parquet: Arc<FooterCountingParquetHandler>,
}

impl FooterCountingEngine {
    fn new(inner: Arc<dyn Engine>) -> Self {
        let parquet = Arc::new(FooterCountingParquetHandler {
            inner: inner.parquet_handler(),
            footer_reads: Mutex::default(),
        });
        Self { inner, parquet }
    }

    /// Returns the number of footer reads of top-level checkpoint files and of sidecar files.
    fn footer_reads(&self) -> (usize, usize) {
        let reads = self.parquet.footer_reads.lock().unwrap();
        let sidecar_reads = reads
            .iter()
            .filter(|location| location.path().contains("/_sidecars/"))
            .count();
        (reads.len() - sidecar_reads, sidecar_reads)
    }
}

impl Engine for FooterCountingEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.inner.evaluation_handler()
    }
    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.inner.storage_handler()
    }
    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.inner.json_handler()
    }
    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.parquet.clone()
    }
}

fn v2_with_sidecars() -> CheckpointSpec {
    CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
        file_actions_per_sidecar_hint: None,
    })
}

/// The `checkpointSchema` that kernel writes is the footer schema of the checkpoint file it wrote
/// (the top-level file of a V2 checkpoint), and the snapshot that `Snapshot::checkpoint` returns
/// keeps the same hint that a fresh load reads from `_last_checkpoint`. Struct stats on a
/// partitioned table put `stats_parsed` and `partitionValues_parsed` into the schema.
#[rstest]
#[case::v1(FeatureSet::new(), None)]
#[case::v2(FeatureSet::new().v2_checkpoint(), None)]
#[case::v2_with_sidecars(FeatureSet::new().v2_checkpoint(), Some(v2_with_sidecars()))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_schema_hint_matches_checkpoint_footer(
    #[case] features: FeatureSet,
    #[case] spec: Option<CheckpointSpec>,
) -> KernelResult<()> {
    let table = test_table(
        LogState::with_latest_version(1),
        features,
        partitioned(),
        checkpoint_struct_stats(),
    );
    let engine = DefaultEngineBuilder::new(table.store().clone())
        .with_task_executor(Arc::new(TokioMultiThreadExecutor::new(
            tokio::runtime::Handle::current(),
        )))
        .build();
    let snapshot = Snapshot::builder_for(table.table_root()).build(&engine)?;
    let (_, checkpointed) = snapshot.checkpoint(&engine, spec.as_ref())?;

    let fresh = Snapshot::builder_for(table.table_root()).build(&engine)?;
    let hint = fresh
        .log_segment()
        .checkpoint_hint()
        .expect("_last_checkpoint describes the new checkpoint");
    assert_eq!(
        checkpointed.log_segment().checkpoint_hint(),
        Some(hint),
        "{table}"
    );
    let checkpoint = &fresh.log_segment().listed.checkpoint_parts[0];
    let footer = engine
        .parquet_handler()
        .read_parquet_footer(&checkpoint.location)?;
    assert_eq!(hint.checkpoint_schema(), Some(&footer.schema), "{table}");
    Ok(())
}

/// How the test gets the snapshot that reads a kernel-written checkpoint.
#[derive(Debug, Clone, Copy, PartialEq)]
enum ReadPath {
    CheckpointResult,
    PostCommit,
    IncrementalUpdate,
    FreshLoad,
    TimeTravel,
    FreshLoadWithoutHint,
}

/// The checkpoint that the test writes with `Snapshot::checkpoint`.
#[derive(Debug, Clone, Copy, PartialEq)]
enum CheckpointKind {
    V1,
    V2,
    V2WithSidecars,
}

/// A table with a kernel-written checkpoint at version 1 and one more commit.
struct CheckpointedTable {
    _tmp_dir: tempfile::TempDir,
    table_path: String,
    engine: Arc<DefaultEngine<TokioMultiThreadExecutor>>,
    spec: Option<CheckpointSpec>,
    /// The snapshot that `Snapshot::checkpoint` returns.
    checkpointed: SnapshotRef,
    /// The post-commit snapshot of the commit on top of `checkpointed`.
    post_commit: SnapshotRef,
}

impl CheckpointedTable {
    async fn new(kind: CheckpointKind) -> KernelResult<Self> {
        let (tmp_dir, table_path, engine) = test_table_setup_mt()?;
        let properties: &[(&str, &str)] = match kind {
            CheckpointKind::V1 => &[],
            CheckpointKind::V2 | CheckpointKind::V2WithSidecars => {
                &[("delta.feature.v2Checkpoint", "supported")]
            }
        };
        let spec = (kind == CheckpointKind::V2WithSidecars).then(v2_with_sidecars);
        let schema = schema_ref! { nullable "id": INTEGER };
        let snapshot =
            create_table_and_load_snapshot(&table_path, schema, engine.as_ref(), properties)?;
        let snapshot = insert_data(snapshot, &engine, vec![Arc::new(Int32Array::from(vec![1]))])
            .await?
            .unwrap_post_commit_snapshot();
        let (_, checkpointed) = snapshot.checkpoint(engine.as_ref(), spec.as_ref())?;
        let post_commit = insert_data(
            checkpointed.clone(),
            &engine,
            vec![Arc::new(Int32Array::from(vec![2]))],
        )
        .await?
        .unwrap_post_commit_snapshot();
        Ok(Self {
            _tmp_dir: tmp_dir,
            table_path,
            engine,
            spec,
            checkpointed,
            post_commit,
        })
    }
}

fn count_rows(snapshot: SnapshotRef, engine: Arc<dyn Engine>) -> KernelResult<usize> {
    let scan = snapshot.scan_builder().build()?;
    Ok(read_scan(&scan, engine)?
        .iter()
        .map(RecordBatch::num_rows)
        .sum())
}

/// Every read path that has the hint of a kernel-written checkpoint takes the checkpoint schema
/// from the hint, so it reads no checkpoint footer. The footer of the first sidecar is still read.
#[rstest]
#[case::checkpoint_result(ReadPath::CheckpointResult, 1)]
#[case::post_commit(ReadPath::PostCommit, 2)]
#[case::incremental_update(ReadPath::IncrementalUpdate, 2)]
#[case::fresh_load(ReadPath::FreshLoad, 2)]
#[case::time_travel(ReadPath::TimeTravel, 1)]
#[case::fresh_load_without_hint(ReadPath::FreshLoadWithoutHint, 2)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_footer_reads_by_read_path(
    #[case] read_path: ReadPath,
    #[case] expected_rows: usize,
    #[values(CheckpointKind::V1, CheckpointKind::V2, CheckpointKind::V2WithSidecars)]
    kind: CheckpointKind,
) -> Result<(), Box<dyn std::error::Error>> {
    let table = CheckpointedTable::new(kind).await?;
    let counting = Arc::new(FooterCountingEngine::new(table.engine.clone()));
    let snapshot = match read_path {
        ReadPath::CheckpointResult => table.checkpointed,
        ReadPath::PostCommit => table.post_commit,
        ReadPath::IncrementalUpdate => {
            Snapshot::builder_from(table.checkpointed).build(counting.as_ref())?
        }
        ReadPath::FreshLoad => Snapshot::builder_for(&table.table_path).build(counting.as_ref())?,
        ReadPath::TimeTravel => Snapshot::builder_for(&table.table_path)
            .at_version(table.checkpointed.version())
            .build(counting.as_ref())?,
        ReadPath::FreshLoadWithoutHint => {
            let hint_path =
                std::path::Path::new(&table.table_path).join("_delta_log/_last_checkpoint");
            std::fs::remove_file(hint_path)?;
            Snapshot::builder_for(&table.table_path).build(counting.as_ref())?
        }
    };

    assert_eq!(count_rows(snapshot, counting.clone())?, expected_rows);
    let checkpoint_footer_reads = usize::from(read_path == ReadPath::FreshLoadWithoutHint);
    let sidecar_footer_reads = usize::from(kind == CheckpointKind::V2WithSidecars);
    assert_eq!(
        counting.footer_reads(),
        (checkpoint_footer_reads, sidecar_footer_reads)
    );
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn next_checkpoint_reads_no_checkpoint_footer(
    #[values(CheckpointKind::V1, CheckpointKind::V2, CheckpointKind::V2WithSidecars)]
    kind: CheckpointKind,
) -> Result<(), Box<dyn std::error::Error>> {
    let table = CheckpointedTable::new(kind).await?;
    let counting = Arc::new(FooterCountingEngine::new(table.engine.clone()));
    let (result, checkpointed) = table
        .post_commit
        .checkpoint(counting.as_ref(), table.spec.as_ref())?;

    assert_eq!(result, CheckpointWriteResult::Written);
    let sidecar_footer_reads = usize::from(kind == CheckpointKind::V2WithSidecars);
    assert_eq!(counting.footer_reads(), (0, sidecar_footer_reads));
    assert_eq!(count_rows(checkpointed, counting.clone())?, 2);
    assert_eq!(counting.footer_reads(), (0, 2 * sidecar_footer_reads));
    let fresh = Snapshot::builder_for(&table.table_path).build(table.engine.as_ref())?;
    assert_eq!(
        fresh
            .log_segment()
            .checkpoint_hint()
            .map(|hint| hint.version()),
        Some(2)
    );
    Ok(())
}

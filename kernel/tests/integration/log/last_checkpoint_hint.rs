//! Integration tests for the checkpoint schema that `Snapshot::checkpoint` records in the
//! `_last_checkpoint` hint and keeps in the snapshot it returns.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use delta_kernel::arrow::array::{Int32Array, RecordBatch};
use delta_kernel::checkpoint::{CheckpointSpec, V2CheckpointConfig};
use delta_kernel::schema::{schema_ref, SchemaRef};
use delta_kernel::{
    Engine, EngineData, EvaluationHandler, FileDataReadResultIterator, FileMeta, FileSize,
    JsonHandler, KernelResult, KernelResultIteratorStatic, ParquetFooter, ParquetHandler,
    PredicateRef, Snapshot, StorageHandler,
};
use rstest::rstest;
use test_utils::delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use test_utils::delta_kernel_default_engine::DefaultEngineBuilder;
use test_utils::table_builder::{
    checkpoint_json_stats, checkpoint_struct_stats, clustered, partitioned, unpartitioned,
    DataLayoutConfig, FeatureSet, LogState, TableConfig, TestTableBuilder,
};
use test_utils::{create_table_and_load_snapshot, insert_data, read_scan, test_table_setup_mt};
use url::Url;

/// Delegates to `inner` and counts `read_parquet_footer` calls.
struct FooterCountingParquetHandler {
    inner: Arc<dyn ParquetHandler>,
    footer_reads: AtomicUsize,
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
        self.footer_reads.fetch_add(1, Ordering::Relaxed);
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
            footer_reads: AtomicUsize::new(0),
        });
        Self { inner, parquet }
    }

    fn footer_reads(&self) -> usize {
        self.parquet.footer_reads.load(Ordering::Relaxed)
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
/// keeps the same hint that a fresh load reads from `_last_checkpoint`.
#[rstest]
#[case::v1(FeatureSet::new(), None)]
#[case::v2(FeatureSet::new().v2_checkpoint(), None)]
#[case::v2_with_sidecars(FeatureSet::new().v2_checkpoint(), Some(v2_with_sidecars()))]
#[case::column_mapping_id(FeatureSet::new().column_mapping("id"), None)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_schema_hint_matches_checkpoint_footer(
    #[case] features: FeatureSet,
    #[case] spec: Option<CheckpointSpec>,
    #[values(checkpoint_json_stats(), checkpoint_struct_stats())] table_config: TableConfig,
    #[values(unpartitioned(), partitioned(), clustered())] data_layout: DataLayoutConfig,
) -> KernelResult<()> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(1))
        .with_features(features)
        .with_table_config(table_config)
        .with_data_layout(data_layout)
        .build()?;
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
    /// The snapshot that `Snapshot::checkpoint` returns.
    CheckpointResult,
    /// The post-commit snapshot of a commit on top of the checkpoint result.
    PostCommit,
    /// `Snapshot::builder_from` the checkpoint result.
    IncrementalUpdate,
    /// A new snapshot that reads `_last_checkpoint`.
    FreshLoad,
    /// A new snapshot at the checkpoint version.
    TimeTravel,
    /// A new snapshot after `_last_checkpoint` is deleted.
    FreshLoadWithoutHint,
}

/// The checkpoint that the test writes with `Snapshot::checkpoint`.
#[derive(Debug, Clone, Copy, PartialEq)]
enum CheckpointKind {
    V1,
    V2,
    V2WithSidecars,
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
    let (_tmp_dir, table_path, engine) = test_table_setup_mt()?;
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

    let counting = Arc::new(FooterCountingEngine::new(engine.clone()));
    let snapshot = match read_path {
        ReadPath::CheckpointResult => checkpointed,
        ReadPath::PostCommit => post_commit,
        ReadPath::IncrementalUpdate => {
            Snapshot::builder_from(checkpointed).build(counting.as_ref())?
        }
        ReadPath::FreshLoad => Snapshot::builder_for(&table_path).build(counting.as_ref())?,
        ReadPath::TimeTravel => Snapshot::builder_for(&table_path)
            .at_version(checkpointed.version())
            .build(counting.as_ref())?,
        ReadPath::FreshLoadWithoutHint => {
            let hint_path = std::path::Path::new(&table_path).join("_delta_log/_last_checkpoint");
            std::fs::remove_file(hint_path)?;
            Snapshot::builder_for(&table_path).build(counting.as_ref())?
        }
    };
    let scan = snapshot.scan_builder().build()?;
    let rows: usize = read_scan(&scan, counting.clone())?
        .iter()
        .map(RecordBatch::num_rows)
        .sum();

    assert_eq!(rows, expected_rows);
    let checkpoint_footer_reads = usize::from(read_path == ReadPath::FreshLoadWithoutHint);
    let sidecar_footer_reads = usize::from(kind == CheckpointKind::V2WithSidecars);
    assert_eq!(
        counting.footer_reads(),
        checkpoint_footer_reads + sidecar_footer_reads
    );
    Ok(())
}

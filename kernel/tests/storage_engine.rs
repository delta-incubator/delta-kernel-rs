#![cfg(feature = "default-engine-base")]

use delta_kernel::engine::storage::StorageEngine;
use delta_kernel::{Engine, ParquetHandler, Result, Snapshot};
use rstest::rstest;
use test_utils::engine_contract::{
    test_json_handler_file_path_contract, test_parquet_handler_footer_errors_on_missing_file,
    test_parquet_handler_footer_preserves_field_ids,
    test_parquet_handler_reads_file_with_arrow_schema,
    test_parquet_handler_write_always_overwrites, test_parquet_handler_write_omits_arrow_schema,
};
use url::Url;

fn engine() -> StorageEngine {
    let inner = test_utils::create_default_engine(&Url::parse("file:///").unwrap()).unwrap();
    StorageEngine::new(inner.storage_handler())
}

#[test]
fn json_preserves_file_path_metadata() {
    test_json_handler_file_path_contract(engine().json_handler().as_ref());
}

#[rstest]
#[case(test_parquet_handler_footer_errors_on_missing_file)]
#[case(test_parquet_handler_footer_preserves_field_ids)]
#[case(test_parquet_handler_reads_file_with_arrow_schema)]
#[case(test_parquet_handler_write_always_overwrites)]
#[case(test_parquet_handler_write_omits_arrow_schema)]
fn parquet_contract(#[case] contract: fn(&dyn ParquetHandler)) {
    contract(engine().parquet_handler().as_ref());
}

#[test]
fn snapshot_replays_checkpoint_and_json_without_declarative_executor() {
    let engine = engine();
    #[cfg(feature = "declarative-plans")]
    assert!(engine.plan_executor().is_none());
    let root = Url::from_directory_path(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/data/with_checkpoint_no_last_checkpoint"),
    )
    .unwrap();
    let snapshot = Snapshot::builder_for(root).build(&engine).unwrap();
    assert_eq!(snapshot.version(), 3);
    let scan = snapshot.scan_builder().build().unwrap();
    let batches: Vec<_> = scan
        .scan_metadata(&engine)
        .unwrap()
        .collect::<Result<_>>()
        .unwrap();
    assert!(!batches.is_empty());
}

#[test]
fn malformed_json_yields_one_error_and_terminates() {
    use std::sync::Arc;

    use delta_kernel::schema::{DataType, StructType};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("malformed.json");
    std::fs::write(&path, br#"{"value": ["#).unwrap();
    let url = Url::from_file_path(path).unwrap();
    let engine = engine();
    let file = engine.storage_handler().head(&url).unwrap();
    let schema = Arc::new(StructType::new_unchecked([
        delta_kernel::schema::StructField::nullable("value", DataType::LONG),
    ]));
    let mut batches = engine
        .json_handler()
        .read_json_files(&[file], schema, None)
        .unwrap();
    assert!(batches.next().unwrap().is_err());
    assert!(batches.next().is_none());
}

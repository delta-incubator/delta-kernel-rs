//! Write-helper delegation through real storage and serialization handlers.

use std::sync::Arc;

use bytes::Bytes;
use rstest::rstest;
use url::Url;

use crate::arrow::array::{Array, Int32Array, RecordBatch};
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::sync::SyncEngine;
use crate::engine::{EngineReadExt, EngineWriteExt, KernelEngine};
use crate::schema::schema_ref;
use crate::unit_test_utils::TestCancellationToken;
use crate::{EngineData, FilteredEngineData, KernelError};

/// Returns a batch containing two integer rows.
fn batch() -> Box<dyn EngineData> {
    let values = Arc::new(Int32Array::from(vec![1, 2])) as Arc<dyn Array>;
    let batch = RecordBatch::try_from_iter(vec![("value", values)]).unwrap();
    Box::new(ArrowEngineData::new(batch))
}

#[rstest]
fn storage_writes_preserve_overwrite_copy_and_delete_contracts(
    #[values(false, true)] cancelled: bool,
) {
    let temp = tempfile::tempdir().unwrap();
    let source = Url::from_file_path(temp.path().join("source")).unwrap();
    let destination = Url::from_file_path(temp.path().join("destination")).unwrap();
    let token = Arc::new(TestCancellationToken::default());
    if cancelled {
        token.cancel();
    }
    let engine = KernelEngine::new(&SyncEngine::new(), token);
    engine
        .put(&source, Bytes::from_static(b"first"), false)
        .unwrap();
    assert!(matches!(
        engine.put(&source, Bytes::from_static(b"second"), false),
        Err(KernelError::FileAlreadyExists(_))
    ));
    engine
        .put(&source, Bytes::from_static(b"second"), true)
        .unwrap();
    engine.copy_atomic(&source, &destination).unwrap();
    assert_eq!(
        std::fs::read(temp.path().join("destination")).unwrap(),
        b"second"
    );
    assert_eq!(
        std::fs::read(temp.path().join("source")).unwrap(),
        b"second"
    );
    assert!(matches!(
        engine.copy_atomic(&source, &destination),
        Err(KernelError::FileAlreadyExists(_))
    ));
    engine.delete(&destination).unwrap();
    engine.delete(&destination).unwrap();
    assert!(!temp.path().join("destination").exists());
    assert!(matches!(
        engine.copy_atomic(&destination, &source),
        Err(KernelError::FileNotFound(_))
    ));
    assert_eq!(
        std::fs::read(temp.path().join("source")).unwrap(),
        b"second"
    );
}

#[rstest]
fn file_writes_preserve_rows_sizes_and_errors(#[values(false, true)] cancelled: bool) {
    let temp = tempfile::tempdir().unwrap();
    let json = Url::from_file_path(temp.path().join("data.json")).unwrap();
    let parquet = Url::from_file_path(temp.path().join("data.parquet")).unwrap();
    let token = Arc::new(TestCancellationToken::default());
    if cancelled {
        token.cancel();
    }
    let original = SyncEngine::new();
    let engine = KernelEngine::new(&original, token);
    let selected = FilteredEngineData::try_new(batch(), vec![true, false]).unwrap();
    let mut batches = vec![Ok(selected)].into_iter();
    let json_size = engine
        .write_json_file(&json, Box::new(batches.by_ref()), false)
        .unwrap();
    assert!(batches.next().is_none());
    assert_eq!(
        std::fs::read(temp.path().join("data.json")).unwrap(),
        b"{\"value\":1}\n"
    );
    assert_eq!(json_size, original.head(&json).unwrap().size);
    assert!(matches!(
        engine.write_json_file(&json, Box::new(std::iter::empty()), false),
        Err(KernelError::FileAlreadyExists(_))
    ));
    let parquet_size = engine
        .write_parquet_file(parquet.clone(), Box::new(std::iter::once(Ok(batch()))))
        .unwrap();
    let meta = original.head(&parquet).unwrap();
    assert_eq!(parquet_size, meta.size);
    let schema = schema_ref! { not_null "value": INTEGER };
    let rows = original
        .read_parquet_files(&[meta], schema, None)
        .unwrap()
        .map(|batch| batch.unwrap().len())
        .sum::<usize>();
    assert_eq!(rows, 2);

    let failure = || Err(KernelError::generic("write input failure"));
    let json_error = engine
        .write_json_file(&json, Box::new(std::iter::once(failure())), true)
        .unwrap_err();
    let parquet_error = engine
        .write_parquet_file(
            parquet,
            Box::new(std::iter::once(Err(KernelError::generic(
                "write input failure",
            )))),
        )
        .unwrap_err();
    assert!(json_error.to_string().contains("write input failure"));
    assert!(parquet_error.to_string().contains("write input failure"));
}

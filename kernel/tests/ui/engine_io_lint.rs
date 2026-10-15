//! Negative Clippy fixture for handler calls that bypass the engine read helpers.

#![allow(dead_code)]

use crate::{Engine, FileMeta, JsonHandler, ParquetHandler, SchemaRef, StorageHandler};

/// Exercises both method-call and fully qualified handler reads.
fn direct_reads(engine: &dyn Engine, file: &FileMeta, schema: SchemaRef) {
    let storage = engine.storage_handler();
    let json = engine.json_handler();
    let parquet = engine.parquet_handler();
    let _ = storage.list_from(&file.location);
    let _ = storage.list_from_with_cancellation(&file.location, None);
    let _ = storage.read_files(vec![]);
    let _ = storage.read_files_with_cancellation(vec![], None);
    let _ = storage.head(&file.location);
    let _ = json.read_json_files(&[], schema.clone(), None);
    let _ = json.read_json_files_with_cancellation(&[], schema.clone(), None, None);
    let _ = parquet.read_parquet_files(&[], schema.clone(), None);
    let _ = parquet.read_parquet_files_with_cancellation(&[], schema.clone(), None, None);
    let _ = parquet.read_parquet_footer(file);
    let _ = parquet.read_parquet_footer_with_cancellation(file, None);
    let _ = StorageHandler::list_from(storage.as_ref(), &file.location);
    let _ = StorageHandler::list_from_with_cancellation(storage.as_ref(), &file.location, None);
    let _ = StorageHandler::read_files(storage.as_ref(), vec![]);
    let _ = StorageHandler::read_files_with_cancellation(storage.as_ref(), vec![], None);
    let _ = StorageHandler::head(storage.as_ref(), &file.location);
    let _ = JsonHandler::read_json_files(json.as_ref(), &[], schema.clone(), None);
    let _ = JsonHandler::read_json_files_with_cancellation(
        json.as_ref(),
        &[],
        schema.clone(),
        None,
        None,
    );
    let _ = ParquetHandler::read_parquet_files(parquet.as_ref(), &[], schema.clone(), None);
    let _ = ParquetHandler::read_parquet_files_with_cancellation(
        parquet.as_ref(),
        &[],
        schema,
        None,
        None,
    );
    let _ = ParquetHandler::read_parquet_footer(parquet.as_ref(), file);
    let _ = ParquetHandler::read_parquet_footer_with_cancellation(parquet.as_ref(), file, None);
}

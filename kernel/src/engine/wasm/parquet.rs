use wasm_bindgen::prelude::*;
use crate::ParquetHandler;
use crate::{DeltaResult, Expression, FileDataReadResultIterator, FileMeta, SchemaRef};

pub struct WasmParquetHandler(JsValue);

impl WasmParquetHandler {
    pub fn new(js_value: JsValue) -> Self {
        WasmParquetHandler(js_value)
    }
}

impl ParquetHandler for WasmParquetHandler {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<Expression>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        self.0.read_parquet_files(files, physical_schema, predicate)
    }
}

// SAFETY: We're assuming a single-threaded environment for WASM
unsafe impl Send for WasmParquetHandler {}
unsafe impl Sync for WasmParquetHandler {}
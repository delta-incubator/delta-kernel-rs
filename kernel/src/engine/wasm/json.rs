use wasm_bindgen::prelude::*;
use crate::JsonHandler;
use crate::{DeltaResult, EngineData, SchemaRef, FileMeta, Expression, FileDataReadResultIterator};

pub struct WasmJsonHandler(JsValue);

impl WasmJsonHandler {
    pub fn new(js_value: JsValue) -> Self {
        WasmJsonHandler(js_value)
    }
}

impl JsonHandler for WasmJsonHandler {
    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> DeltaResult<Box<dyn EngineData>> {
        self.0.parse_json(json_strings, output_schema)
    }

    fn read_json_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<Expression>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        self.0.read_json_files(files, physical_schema, predicate)
    }
}

// SAFETY: We're assuming a single-threaded environment for WASM
unsafe impl Send for WasmJsonHandler {}
unsafe impl Sync for WasmJsonHandler {}
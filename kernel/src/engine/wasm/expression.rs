use wasm_bindgen::prelude::*;
use crate::ExpressionHandler;
use arrow::datatypes::{DataType, SchemaRef};
use std::sync::Arc;
use crate::Expression;
use crate::ExpressionEvaluator;

pub struct WasmExpressionHandler(JsValue);

impl WasmExpressionHandler {
    pub fn new(js_value: JsValue) -> Self {
        WasmExpressionHandler(js_value)
    }
}

impl ExpressionHandler for WasmExpressionHandler {
    fn get_evaluator(
        &self,
        schema: SchemaRef,
        expression: Expression,
        output_type: DataType,
    ) -> Arc<dyn ExpressionEvaluator> {
        self.0.get_evaluator(schema, expression, output_type)
    }
}

// SAFETY: We're assuming a single-threaded environment for WASM
unsafe impl Send for WasmExpressionHandler {}
unsafe impl Sync for WasmExpressionHandler {}
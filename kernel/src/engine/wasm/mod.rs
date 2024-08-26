use wasm_bindgen::prelude::*;
use std::sync::Arc;
use crate::{Engine, ExpressionHandler, FileSystemClient, JsonHandler, ParquetHandler};

mod fs_client;
mod json;
mod parquet;
mod expression;

#[wasm_bindgen]
pub struct WasmEngine {
    fs_client: Arc<fs_client::WasmFileSystemClient>,
    json_handler: Arc<json::WasmJsonHandler>,
    parquet_handler: Arc<parquet::WasmParquetHandler>,
    expression_handler: Arc<expression::WasmExpressionHandler>,
}

#[wasm_bindgen]
impl WasmEngine {
    #[wasm_bindgen(constructor)]
    pub fn new(
        fs_client: JsValue,
        json_handler: JsValue,
        parquet_handler: JsValue,
        expression_handler: JsValue
    ) -> Self {
        WasmEngine {
            fs_client: Arc::new(fs_client::WasmFileSystemClient::new(fs_client)),
            json_handler: Arc::new(json::WasmJsonHandler::new(json_handler)),
            parquet_handler: Arc::new(parquet::WasmParquetHandler::new(parquet_handler)),
            expression_handler: Arc::new(expression::WasmExpressionHandler::new(expression_handler)),
        }
    }
}

impl Engine for WasmEngine {
    fn get_file_system_client(&self) -> Arc<dyn FileSystemClient> {
        self.fs_client.clone()
    }

    fn get_json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json_handler.clone()
    }

    fn get_parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.parquet_handler.clone()
    }

    fn get_expression_handler(&self) -> Arc<dyn ExpressionHandler> {
        self.expression_handler.clone()
    }
}
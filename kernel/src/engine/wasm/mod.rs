use wasm_bindgen::prelude::*;
use std::sync::Arc;
use crate::{Engine, ExpressionHandler, FileSystemClient, JsonHandler, ParquetHandler};

#[wasm_bindgen]
pub struct WasmEngine {
    fs_client: Option<Arc<dyn FileSystemClient>>,
    json_handler: Option<Arc<dyn JsonHandler>>,
    parquet_handler: Option<Arc<dyn ParquetHandler>>,
    expression_handler: Option<Arc<dyn ExpressionHandler>>,
}

#[wasm_bindgen]
impl WasmEngine {
    #[wasm_bindgen(constructor)]
    pub fn new() -> WasmEngine {
        WasmEngine {
            fs_client: None,
            json_handler: None,
            parquet_handler: None,
            expression_handler: None,
        }
    }

    #[wasm_bindgen]
    pub fn set_fs_client(&mut self, fs_client: JsValue) -> Result<(), JsValue> {
        self.fs_client = Some(Arc::new(fs_client.into_serde().map_err(|e| e.to_string())?));
        Ok(())
    }

    #[wasm_bindgen]
    pub fn set_json_handler(&mut self, json_handler: JsValue) -> Result<(), JsValue> {
        self.json_handler = Some(Arc::new(json_handler.into_serde().map_err(|e| e.to_string())?));
        Ok(())
    }

    #[wasm_bindgen]
    pub fn set_parquet_handler(&mut self, parquet_handler: JsValue) -> Result<(), JsValue> {
        self.parquet_handler = Some(Arc::new(parquet_handler.into_serde().map_err(|e| e.to_string())?));
        Ok(())
    }

    #[wasm_bindgen]
    pub fn set_expression_handler(&mut self, expression_handler: JsValue) -> Result<(), JsValue> {
        self.expression_handler = Some(Arc::new(expression_handler.into_serde().map_err(|e| e.to_string())?));
        Ok(())
    }
}

impl Engine for WasmEngine {
    fn get_expression_handler(&self) -> Arc<dyn ExpressionHandler> {
        self.expression_handler.clone().expect("Expression handler not set")
    }

    fn get_file_system_client(&self) -> Arc<dyn FileSystemClient> {
        self.fs_client.clone().expect("File system client not set")
    }

    fn get_parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.parquet_handler.clone().expect("Parquet handler not set")
    }

    fn get_json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json_handler.clone().expect("JSON handler not set")
    }
}
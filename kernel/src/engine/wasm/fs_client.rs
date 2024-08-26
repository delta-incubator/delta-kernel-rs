use wasm_bindgen::prelude::*;
use crate::FileSystemHandler;

pub struct WasmFileSystemClient(JsValue);

impl WasmFileSystemClient {
    pub fn new(js_value: JsValue) -> Self {
        WasmFileSystemClient(js_value)
    }
}

impl FileSystemHandler for WasmFileSystemClient {
    // Implement FileSystemHandler methods here, using self.0 to call JavaScript functions
}

// SAFETY: We're assuming a single-threaded environment for WASM
unsafe impl Send for WasmFileSystemClient {}
unsafe impl Sync for WasmFileSystemClient {}
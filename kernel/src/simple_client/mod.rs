/// This module implements a simple, single threaded, EngineClient

use crate::{DeltaResult, schema::Schema};
use crate::engine_data::EngineData;

use std::sync::Arc;
use url::Url;

mod data;

struct SimpleJsonHandler {

}

impl JsonHandler for SimpleJsonHandler {
   fn read_json_files(
       &self,
       files: dyn IntoIterator<Item = Url, IntoIter = dyn Iterator<Item = Url>>,
       schema: &Schema,
   ) -> DeltaResult<FileReadResultIt> {
       files.map(|file| {
           
       })
   }
}

struct SimpleClient {}

// impl EngineClient for SimpleClient {
    
// }


// Everything below will be moved to ../../lib.rs when we switch to EngineClient from TableClient

pub type FileReadResultIt = Box<dyn Iterator<Item = dyn EngineData>>;

pub trait JsonHandler {
    fn read_json_files(
        &self,
        files: dyn IntoIterator<Item = Url, IntoIter = dyn Iterator<Item = Url>>,
        schema: &Schema,
    ) -> DeltaResult<FileReadResultIt>;
}
pub trait EngineClient {
    fn get_json_handler(&self) -> Arc<dyn JsonHandler>;
}

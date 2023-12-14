use crate::engine_data::{DataExtractor, DataVisitor, EngineData, TypeTag};
/// This module implements a simple, single threaded, EngineClient
use crate::{schema::SchemaRef, DeltaResult};

use std::sync::Arc;
use url::Url;

pub mod data;

struct SimpleJsonHandler {}
impl JsonHandler for SimpleJsonHandler {
    fn read_json_files(&self, files: Vec<Url>, schema: SchemaRef) -> DeltaResult<FileReadResult> {
        files
            .into_iter()
            .map(|file| {
                let d = data::SimpleData::try_create_from_json(schema.clone(), file);
                d.map(|d| {
                    let b: Box<dyn EngineData> = Box::new(d);
                    b
                })
            })
            .collect()
    }
}

struct SimpleDataExtractor {
    expected_tag: data::SimpleDataTypeTag,
}
impl DataExtractor for SimpleDataExtractor {
    fn extract(&self, blob: &dyn EngineData, schema: SchemaRef, visitor: &mut dyn DataVisitor) {
        assert!(self.expected_tag.eq(blob.type_tag()));
        let data: &data::SimpleData = blob
            .as_any()
            .downcast_ref::<data::SimpleData>()
            .expect("extract called on blob that isn't SimpleData");
        data.extract(schema, visitor);
    }

    fn length(&self, blob: &dyn EngineData) -> usize {
        assert!(self.expected_tag.eq(blob.type_tag()));
        let data: &data::SimpleData = blob
            .as_any()
            .downcast_ref::<data::SimpleData>()
            .expect("length called on blob that isn't SimpleData");
        data.length()
    }
}

pub struct SimpleClient {
    json_handler: Arc<SimpleJsonHandler>,
    data_extractor: Arc<SimpleDataExtractor>,
}

impl SimpleClient {
    pub fn new() -> Self {
        SimpleClient {
            json_handler: Arc::new(SimpleJsonHandler {}),
            data_extractor: Arc::new(SimpleDataExtractor {
                expected_tag: data::SimpleDataTypeTag,
            }),
        }
    }
}

impl EngineClient for SimpleClient {
    fn get_json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json_handler.clone()
    }

    fn get_data_extactor(&self) -> Arc<dyn DataExtractor> {
        self.data_extractor.clone()
    }
}

// Everything below will be moved to ../../lib.rs when we switch to EngineClient from TableClient

pub type FileReadResult = Vec<Box<dyn EngineData>>;

pub trait JsonHandler {
    fn read_json_files(&self, files: Vec<Url>, schema: SchemaRef) -> DeltaResult<FileReadResult>;
}
pub trait EngineClient {
    fn get_json_handler(&self) -> Arc<dyn JsonHandler>;
    fn get_data_extactor(&self) -> Arc<dyn DataExtractor>;
}

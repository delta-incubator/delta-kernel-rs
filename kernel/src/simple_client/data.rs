use crate::engine_data::{DataExtractor, DataVisitor, EngineData, TypeTag};
use crate::schema::Schema;

use arrow_array::{RecordBatch, StringArray};
use arrow_json::ReaderBuilder;
use std::fs::File;
use std::io::BufReader;

struct SimpleDataTypeTag;
impl TypeTag for SimpleDataTypeTag {}

// need to convert our schema to arrow schema

/// SimpleData holds a RecordBatch
struct SimpleData {
    data: RecordBatch,
}

impl EngineData for SimpleData {
   fn type_tag(&self) -> &dyn TypeTag {
       &SimpleDataTypeTag
   }
}

impl SimpleData {
    
}

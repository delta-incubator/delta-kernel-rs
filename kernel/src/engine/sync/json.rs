use std::{fs::File, io::BufReader};

use arrow_schema::SchemaRef as ArrowSchemaRef;
use url::Url;

use super::read_files;
use crate::{
    engine::arrow_data::ArrowEngineData, engine::arrow_utils::parse_json as arrow_parse_json,
    schema::SchemaRef, DeltaResult, EngineData, Expression, FileDataReadResultIterator, FileMeta,
    JsonHandler,
};

pub(crate) struct SyncJsonHandler;

fn try_create_from_json(
    file: File,
    _schema: SchemaRef,
    arrow_schema: ArrowSchemaRef,
    _predicate: Option<&Expression>,
) -> DeltaResult<impl Iterator<Item = DeltaResult<ArrowEngineData>>> {
    let json = arrow_json::ReaderBuilder::new(arrow_schema)
        .build(BufReader::new(file))?
        .map(|data| Ok(ArrowEngineData::new(data?)));
    Ok(json)
}

impl JsonHandler for SyncJsonHandler {
    fn read_json_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<Expression>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        read_files(files, schema, predicate, try_create_from_json)
    }

    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> DeltaResult<Box<dyn EngineData>> {
        arrow_parse_json(json_strings, output_schema)
    }

    fn write_json_file<'a>(
        &self,
        _path: &Url,
        _data: Box<dyn Iterator<Item = Box<dyn EngineData>> + Send + 'a>,
        _overwrite: bool,
    ) -> DeltaResult<()> {
        unimplemented!()
    }
}

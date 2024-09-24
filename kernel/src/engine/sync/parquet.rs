use std::fs::File;

use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder};
use tracing::debug;
use url::Url;

use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::arrow_expression::expression_to_row_filter;
use crate::engine::arrow_utils::{generate_mask, get_requested_indices, reorder_struct_array};
use crate::schema::SchemaRef;
use crate::{DeltaResult, Error, Expression, FileDataReadResultIterator, FileMeta, ParquetHandler};

pub(crate) struct SyncParquetHandler;

fn try_create_from_parquet(
    schema: SchemaRef,
    location: Url,
    predicate: Option<Expression>,
) -> DeltaResult<ArrowEngineData> {
    let file = File::open(
        location
            .to_file_path()
            .map_err(|_| Error::generic("can only read local files"))?,
    )?;
    let metadata = ArrowReaderMetadata::load(&file, Default::default())?;
    let parquet_schema = metadata.schema();
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let (indicies, requested_ordering) = get_requested_indices(&schema, parquet_schema)?;
    if let Some(mask) = generate_mask(&schema, parquet_schema, builder.parquet_schema(), &indicies)
    {
        builder = builder.with_projection(mask);
    }
    if let Some(predicate) = predicate {
        let parquet_schema = metadata.schema();
        let parquet_physical_schema = metadata.parquet_schema();
        let row_filter = expression_to_row_filter(predicate, &schema, parquet_schema, parquet_physical_schema)?;
        builder = builder.with_row_filter(row_filter);
    }
    let mut reader = builder.build()?;
    let data = reader
        .next()
        .ok_or_else(|| Error::generic("No data found reading parquet file"))?;
    let reordered = reorder_struct_array(data?.into(), &requested_ordering).map(Into::into)?;
    Ok(ArrowEngineData::new(reordered))
}

impl ParquetHandler for SyncParquetHandler {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<Expression>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        debug!("Reading parquet files: {files:#?} with schema {schema:#?} and predicate {predicate:#?}");
        if files.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }
        let locations: Vec<_> = files.iter().map(|file| file.location.clone()).collect();
        Ok(Box::new(locations.into_iter().map(move |location| {
            try_create_from_parquet(schema.clone(), location, predicate.clone())
                .map(|d| Box::new(d) as _)
        })))
    }
}

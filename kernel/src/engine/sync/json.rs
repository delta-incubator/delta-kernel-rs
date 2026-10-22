use std::io::{BufReader, Cursor};
use std::num::NonZero;
use std::sync::Arc;

use bytes::Bytes;
#[cfg(test)]
use url::Url;

#[cfg(test)]
use super::{put_bytes, read_files_arrow};
use crate::arrow::json::ReaderBuilder;
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::arrow_utils::{build_json_reorder_indices, fixup_json_read, json_arrow_schema};
#[cfg(test)]
use crate::engine::arrow_utils::{parse_json as arrow_parse_json, to_json_bytes};
#[cfg(test)]
use crate::engine_data::FilteredEngineData;
#[cfg(test)]
use crate::object_store::DynObjectStore;
use crate::schema::SchemaRef;
#[cfg(test)]
use crate::{
    EngineData, FileDataReadResultIterator, FileMeta, FileSize, JsonHandler, KernelResultIterator,
};
use crate::{KernelResult, PredicateRef};

#[cfg(test)]
pub(crate) struct SyncJsonHandler {
    store: Option<Arc<DynObjectStore>>,
    batch_size: Option<NonZero<usize>>,
}

#[cfg(test)]
impl SyncJsonHandler {
    /// Create a JSON handler backed by `store`, or the local filesystem when absent.
    pub(crate) fn new(store: Option<Arc<DynObjectStore>>) -> Self {
        Self {
            store,
            batch_size: None,
        }
    }

    /// Set the maximum number of rows per JSON file read batch.
    #[cfg(test)]
    pub(crate) fn with_batch_size(mut self, batch_size: NonZero<usize>) -> Self {
        self.batch_size = Some(batch_size);
        self
    }
}

pub(super) fn try_create_from_json(
    data: Bytes,
    schema: SchemaRef,
    _predicate: Option<PredicateRef>,
    file_location: String,
) -> KernelResult<impl Iterator<Item = KernelResult<ArrowEngineData>>> {
    try_create_from_json_with_batch_size(data, schema, file_location, None)
}

fn try_create_from_json_with_batch_size(
    data: Bytes,
    schema: SchemaRef,
    file_location: String,
    batch_size: Option<NonZero<usize>>,
) -> KernelResult<impl Iterator<Item = KernelResult<ArrowEngineData>>> {
    let json_schema = Arc::new(json_arrow_schema(&schema)?);
    let reorder_indices = build_json_reorder_indices(&schema)?;
    let mut reader_builder = ReaderBuilder::new(json_schema).with_coerce_primitive(true);
    if let Some(batch_size) = batch_size {
        reader_builder = reader_builder.with_batch_size(batch_size.get());
    }
    let json = reader_builder
        .build(BufReader::new(Cursor::new(data)))?
        .map(move |data| fixup_json_read(data?, &reorder_indices, &file_location));
    Ok(json)
}

#[cfg(test)]
impl JsonHandler for SyncJsonHandler {
    fn read_json_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        let iter = read_files_arrow(self.store.as_ref(), files, schema, predicate, {
            let batch_size = self.batch_size;
            move |data, schema, _predicate, file_location| {
                try_create_from_json_with_batch_size(data, schema, file_location, batch_size)
            }
        });
        Ok(Box::new(iter.map(|data| Ok(Box::new(data?) as _))))
    }

    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> KernelResult<Box<dyn EngineData>> {
        arrow_parse_json(json_strings, output_schema)
    }

    fn write_json_file(
        &self,
        path: &Url,
        data: KernelResultIterator<'_, FilteredEngineData>,
        overwrite: bool,
    ) -> KernelResult<FileSize> {
        let buf = to_json_bytes(data)?;
        let size = buf.len() as FileSize;
        put_bytes(self.store.as_ref(), path, buf.into(), overwrite)?;
        Ok(size)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use serde_json::json;
    use tempfile::TempDir;
    use url::Url;

    use super::*;
    use crate::arrow::array::{RecordBatch, StringArray};
    use crate::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use crate::KernelError;

    // Helper function to create test data
    fn create_test_data(values: Vec<&str>) -> KernelResult<Box<dyn EngineData>> {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "dog",
            ArrowDataType::Utf8,
            true,
        )]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(values))])?;
        Ok(Box::new(ArrowEngineData::new(batch)))
    }

    // Helper function to read and parse JSON file
    fn read_json_file(path: &Path) -> KernelResult<Vec<serde_json::Value>> {
        let file = std::fs::read_to_string(path)?;
        let json: Vec<_> = serde_json::Deserializer::from_str(&file)
            .into_iter::<serde_json::Value>()
            .flatten()
            .collect();
        Ok(json)
    }

    #[test]
    fn test_write_json_file_without_overwrite() -> KernelResult<()> {
        do_test_write_json_file(false)
    }

    #[test]
    fn test_write_json_file_overwrite() -> KernelResult<()> {
        do_test_write_json_file(true)
    }

    fn do_test_write_json_file(overwrite: bool) -> KernelResult<()> {
        let test_dir = TempDir::new().unwrap();
        let path = test_dir.path().join("00000000000000000001.json");
        let handler = SyncJsonHandler::new(None);
        let url = Url::from_file_path(&path).unwrap();

        // First write with no existing file
        let data = create_test_data(vec!["remi", "wilson"])?;
        let filtered_data = Ok(FilteredEngineData::with_all_rows_selected(data));
        let result =
            handler.write_json_file(&url, Box::new(std::iter::once(filtered_data)), overwrite);

        let written_size = result.unwrap();
        assert_eq!(written_size, 32);
        assert_eq!(written_size, std::fs::metadata(&path).unwrap().len());
        let json = read_json_file(&path)?;
        assert_eq!(json, vec![json!({"dog": "remi"}), json!({"dog": "wilson"})]);

        // Second write with existing file
        let data = create_test_data(vec!["seb", "tia"])?;
        let filtered_data = Ok(FilteredEngineData::with_all_rows_selected(data));
        let result =
            handler.write_json_file(&url, Box::new(std::iter::once(filtered_data)), overwrite);

        if overwrite {
            let written_size = result.unwrap();
            assert_eq!(written_size, 28);
            assert_eq!(written_size, std::fs::metadata(&path).unwrap().len());
            let json = read_json_file(&path)?;
            assert_eq!(json, vec![json!({"dog": "seb"}), json!({"dog": "tia"})]);
        } else {
            // Verify the second write fails with FileAlreadyExists error
            assert!(matches!(result, Err(KernelError::FileAlreadyExists(_))));
        }

        Ok(())
    }

    #[test]
    fn test_write_empty_json_file_reports_zero_size() -> KernelResult<()> {
        let test_dir = TempDir::new().unwrap();
        let path = test_dir.path().join("empty.json");
        let handler = SyncJsonHandler::new(None);
        let url = Url::from_file_path(&path).unwrap();

        let written_size = handler
            .write_json_file(&url, Box::new(std::iter::empty()), false)
            .unwrap();

        assert_eq!(written_size, 0);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
        Ok(())
    }

    // TODO(#2618): Restore once the engine contract helpers move to test_utils and SyncEngine can
    // call them without the kernel-cfg-test cycle issue.
    //
    // #[test]
    // fn json_handler_file_path_contract() {
    //     test_json_handler_file_path_contract(&SyncJsonHandler::new(None));
    // }
}

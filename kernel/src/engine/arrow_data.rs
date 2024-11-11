use crate::engine_data::{EngineData, EngineList, EngineMap, GetData};
use crate::expressions::ColumnName;
use crate::utils::require;
use crate::{RowVisitor, DeltaResult, Error};

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{Array, ArrayRef, GenericListArray, MapArray, OffsetSizeTrait, RecordBatch, StructArray};
use arrow_schema::{FieldRef, DataType as ArrowDataType};
use tracing::{debug};

use std::any::Any;
use std::collections::{HashMap, HashSet};

/// ArrowEngineData holds an Arrow RecordBatch, implements `EngineData` so the kernel can extract from it.
pub struct ArrowEngineData {
    data: RecordBatch,
}

impl ArrowEngineData {
    /// Create a new `ArrowEngineData` from a `RecordBatch`
    pub fn new(data: RecordBatch) -> Self {
        ArrowEngineData { data }
    }

    /// Utility constructor to get a `Box<ArrowEngineData>` out of a `Box<dyn EngineData>`
    pub fn try_from_engine_data(engine_data: Box<dyn EngineData>) -> DeltaResult<Box<Self>> {
        engine_data
            .into_any()
            .downcast::<ArrowEngineData>()
            .map_err(|_| Error::engine_data_type("ArrowEngineData"))
    }

    /// Get a reference to the `RecordBatch` this `ArrowEngineData` is wrapping
    pub fn record_batch(&self) -> &RecordBatch {
        &self.data
    }
}

impl EngineData for ArrowEngineData {
    fn visit_rows(&self, leaf_columns: &[ColumnName], visitor: &mut dyn RowVisitor) -> DeltaResult<()> {
        // Collect the set of all leaf columns we want to extract, along with their parents, for
        // efficient depth-first extraction. If the list contains any non-leaf, duplicate, or
        // missing column references, the extracted column list will be too short (error out below).
        let mut mask = HashSet::new();
        for column in leaf_columns {
            for i in 0..column.len() {
                mask.insert(&column[..i+1]);
            }
        }
        println!("Column mask for selected columns {leaf_columns:?} is {mask:#?}");

        let mut getters = vec![];
        self.extract_columns(&mut getters, &mask)?;
        if getters.len() != leaf_columns.len() {
            return Err(Error::MissingColumn(
                format!(
                    "Expected {} leaf columns, but only found {}",
                    leaf_columns.len(), getters.len()
                )
            ));
        }
        visitor.visit(self.length(), &getters)
    }

    fn length(&self) -> usize {
        self.data.num_rows()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl From<RecordBatch> for ArrowEngineData {
    fn from(value: RecordBatch) -> Self {
        ArrowEngineData::new(value)
    }
}

impl From<ArrowEngineData> for RecordBatch {
    fn from(value: ArrowEngineData) -> Self {
        value.data
    }
}

impl From<Box<ArrowEngineData>> for RecordBatch {
    fn from(value: Box<ArrowEngineData>) -> Self {
        value.data
    }
}

/// This is a trait that allows us to enumerate a set of Arrow `Array` and `Field` pairs, in schema
/// order. Both `RecordBatch` and `StructArray` can do this. By having our `extract_*` functions
/// just take anything that implements this trait we can use the same function to drill into either
/// one. This is useful because when we're recursing into data we start with a `RecordBatch`, but if
/// we encounter a Struct column, it will be a `StructArray`.
trait ProvidesColumnsAndFields {
    fn columns(&self) -> &[ArrayRef];
    fn fields(&self) -> &[FieldRef];
}

impl ProvidesColumnsAndFields for RecordBatch {
    fn columns(&self) -> &[ArrayRef] {
        self.columns()
    }
    fn fields(&self) -> &[FieldRef] {
        self.schema_ref().fields()
    }
}

impl ProvidesColumnsAndFields for StructArray {
    fn columns(&self) -> &[ArrayRef] {
        self.columns()
    }
    fn fields(&self) -> &[FieldRef] {
        self.fields()
    }
}

impl<OffsetSize> EngineList for GenericListArray<OffsetSize>
where
    OffsetSize: OffsetSizeTrait,
{
    fn len(&self, row_index: usize) -> usize {
        self.value(row_index).len()
    }

    fn get(&self, row_index: usize, index: usize) -> String {
        let arry = self.value(row_index);
        let sarry = arry.as_string::<i32>();
        sarry.value(index).to_string()
    }

    fn materialize(&self, row_index: usize) -> Vec<String> {
        let mut result = vec![];
        for i in 0..EngineList::len(self, row_index) {
            result.push(self.get(row_index, i));
        }
        result
    }
}

impl EngineMap for MapArray {
    fn get<'a>(&'a self, row_index: usize, key: &str) -> Option<&'a str> {
        let offsets = self.offsets();
        let start_offset = offsets[row_index] as usize;
        let count = offsets[row_index + 1] as usize - start_offset;
        let keys = self.keys().as_string::<i32>();
        for (idx, map_key) in keys.iter().enumerate().skip(start_offset).take(count) {
            if let Some(map_key) = map_key {
                if key == map_key {
                    // found the item
                    let vals = self.values().as_string::<i32>();
                    return Some(vals.value(idx));
                }
            }
        }
        None
    }

    fn materialize(&self, row_index: usize) -> HashMap<String, String> {
        let mut ret = HashMap::new();
        let map_val = self.value(row_index);
        let keys = map_val.column(0).as_string::<i32>();
        let values = map_val.column(1).as_string::<i32>();
        for (key, value) in keys.iter().zip(values.iter()) {
            if let (Some(key), Some(value)) = (key, value) {
                ret.insert(key.into(), value.into());
            }
        }
        ret
    }
}

impl ArrowEngineData {
    /// Extracts an exploded view (all leaf values), in schema order of that data contained
    /// within. `out_col_array` is filled with [`GetData`] items that can be used to get at the
    /// actual primitive types.
    ///
    /// # Arguments
    ///
    /// * `out_col_array` - the vec that leaf values will be pushed onto. it is passed as an arg to
    ///   make the recursion below easier. if we returned a [`Vec`] we would have to `extend` it each
    ///   time we encountered a struct and made the recursive call.
    /// * `schema` - the schema to extract getters for
    pub fn extract_columns<'a>(
        &'a self,
        getters: &mut Vec<&dyn GetData<'a>>,
        column_mask: &HashSet<&[String]>,
    ) -> DeltaResult<()> {
        Self::extract_columns_from_array(getters, &mut vec![], column_mask, &self.data)
    }

    fn extract_columns_from_array<'a>(
        getters: &mut Vec<&dyn GetData<'a>>,
        path: &mut Vec<String>,
        column_mask: &HashSet<&[String]>,
        data: &'a dyn ProvidesColumnsAndFields,
    ) -> DeltaResult<()> {
        for (column, field) in data.columns().iter().zip(data.fields()) {
            path.push(field.name().to_string());
            let col_name = ColumnName::new(path.iter());
            if column_mask.contains(&path[..]) {
                Self::extract_column(getters, path, column_mask, col_name, column)?;
            } else {
                println!("Skipping unmasked path {col_name}");
            }
            path.pop();
        }
        Ok(())
    }

    fn extract_column<'a>(
        out_col_array: &mut Vec<&dyn GetData<'a>>,
        path: &mut Vec<String>,
        column_mask: &HashSet<&[String]>,
        field_name: ColumnName,
        col: &'a dyn Array,
    ) -> DeltaResult<()> {
        match col.data_type() {
            &ArrowDataType::Null => {
                println!("Pushing a null array for {field_name}");
                out_col_array.push(&());
            }
            &ArrowDataType::Struct(_) => {
                // both structs, so recurse into col
                let struct_array = col.as_struct();
                ArrowEngineData::extract_columns_from_array(
                    out_col_array,
                    path,
                    column_mask,
                    struct_array,
                )?;
            }
            &ArrowDataType::Boolean => {
                println!("Pushing boolean array for {field_name}");
                out_col_array.push(col.as_boolean());
            }
            &ArrowDataType::Utf8 => {
                println!("Pushing string array for {field_name}");
                out_col_array.push(col.as_string());
            }
            &ArrowDataType::Int32 => {
                println!("Pushing int32 array for {field_name}");
                out_col_array.push(col.as_primitive::<Int32Type>());
            }
            &ArrowDataType::Int64 => {
                println!("Pushing int64 array for {field_name}");
                out_col_array.push(col.as_primitive::<Int64Type>());
            }
            ArrowDataType::List(arrow_field) => match arrow_field.data_type() {
                ArrowDataType::Utf8 => {
                    println!("Pushing list for {field_name}");
                    let list: &GenericListArray<i32> = col.as_list();
                    out_col_array.push(list);
                }
                _ => {
                    return Err(Error::UnexpectedColumnType(format!(
                        "On {field_name}: Only support lists that contain strings"
                    )))
                }
            }
            ArrowDataType::LargeList(arrow_field) => match arrow_field.data_type() {
                ArrowDataType::Utf8 => {
                    println!("Pushing large list for {field_name}");
                    let list: &GenericListArray<i64> = col.as_list();
                    out_col_array.push(list);
                }
                _ => {
                    return Err(Error::UnexpectedColumnType(format!(
                        "On {field_name}: Only support lists that contain strings"
                    )))
                }
            }
            &ArrowDataType::Map(ref map_field, _sorted_keys) => {
                if let ArrowDataType::Struct(fields) = map_field.data_type() {
                    let mut fcount = 0;
                    for field in fields {
                        require!(
                            field.data_type() == &ArrowDataType::Utf8,
                            Error::UnexpectedColumnType(format!(
                                "On {field_name}: Only support maps of String->String"
                            ))
                        );
                        fcount += 1;
                    }
                    require!(
                        fcount == 2,
                        Error::UnexpectedColumnType(format!(
                            "On {field_name}: Expect map field struct to have two fields"
                        ))
                    );
                    println!("Pushing map for {field_name}");
                    out_col_array.push(col.as_map());
                } else {
                    return Err(Error::UnexpectedColumnType(format!(
                        "On {field_name}: Expect arrow maps to have struct field in DataType"
                    )));
                }
            }
            arrow_data_type => {
                return Err(Error::UnexpectedColumnType(format!(
                    "On {field_name}: Can't extract columns of type {arrow_data_type}"
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};

    use crate::{
        actions::{get_log_schema, Metadata, Protocol},
        engine::sync::SyncEngine,
        DeltaResult, Engine, EngineData,
    };

    use super::ArrowEngineData;

    fn string_array_to_engine_data(string_array: StringArray) -> Box<dyn EngineData> {
        let string_field = Arc::new(Field::new("a", DataType::Utf8, true));
        let schema = Arc::new(ArrowSchema::new(vec![string_field]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(string_array)])
            .expect("Can't convert to record batch");
        Box::new(ArrowEngineData::new(batch))
    }

    #[test]
    fn test_md_extract() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let handler = engine.get_json_handler();
        let json_strings: StringArray = vec![
            r#"{"metaData":{"id":"aff5cb91-8cd9-4195-aef9-446908507302","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"c1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c2\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c3\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["c1","c2"],"configuration":{},"createdTime":1670892997849}}"#,
        ]
        .into();
        let output_schema = get_log_schema().clone();
        let parsed = handler
            .parse_json(string_array_to_engine_data(json_strings), output_schema)
            .unwrap();
        let metadata = Metadata::try_new_from_data(parsed.as_ref())?.unwrap();
        assert_eq!(metadata.id, "aff5cb91-8cd9-4195-aef9-446908507302");
        assert_eq!(metadata.created_time, Some(1670892997849));
        assert_eq!(metadata.partition_columns, vec!("c1", "c2"));
        Ok(())
    }

    #[test]
    fn test_protocol_extract() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let handler = engine.get_json_handler();
        let json_strings: StringArray = vec![
            r#"{"protocol": {"minReaderVersion": 3, "minWriterVersion": 7, "readerFeatures": ["rw1"], "writerFeatures": ["rw1", "w2"]}}"#,
        ]
        .into();
        let output_schema = get_log_schema().project(&["protocol"])?;
        let parsed = handler
            .parse_json(string_array_to_engine_data(json_strings), output_schema)
            .unwrap();
        let protocol = Protocol::try_new_from_data(parsed.as_ref())?.unwrap();
        assert_eq!(protocol.min_reader_version, 3);
        assert_eq!(protocol.min_writer_version, 7);
        assert_eq!(protocol.reader_features, Some(vec!["rw1".into()]));
        assert_eq!(protocol.writer_features, Some(vec!["rw1".into(), "w2".into()]));
        Ok(())
    }
}

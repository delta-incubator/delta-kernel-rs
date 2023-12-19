use crate::engine_data::{DataItem, DataVisitor, EngineData, TypeTag};
use crate::schema::SchemaRef;
use crate::DeltaResult;

use arrow_array::{array, RecordBatch, StringArray};
use arrow_schema::{DataType, Schema as ArrowSchema};
use url::Url;

use std::any::Any;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

pub struct SimpleDataTypeTag;
impl TypeTag for SimpleDataTypeTag {}

/// SimpleData holds a RecordBatch
pub struct SimpleData {
    data: RecordBatch,
}

impl EngineData for SimpleData {
    fn type_tag(&self) -> &dyn TypeTag {
        &SimpleDataTypeTag
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

macro_rules! push_item {
    ($field: expr, $col: expr, $prim_type: expr, $arry_type: ty, $result_arry: expr, $enum_typ: expr) => {
        if $field.data_type() != &crate::schema::DataType::Primitive($prim_type) {
            panic!("Schema's don't match");
        }
        let arry = $col
            .as_any()
            .downcast_ref::<$arry_type>()
            .expect("Failed to downcast");
        for item in arry.iter() {
            if let Some(i) = item {
                $result_arry.push(Some($enum_typ(i)));
            }
        }
    };
}

impl SimpleData {
    pub fn try_create_from_json(schema: SchemaRef, location: Url) -> DeltaResult<Self> {
        let arrow_schema: ArrowSchema = (&*schema).try_into()?;
        // todo: Check scheme of url
        let file = File::open(location.to_file_path().unwrap()).unwrap(); // todo: fix to_file_path.unwrap()
        let mut json = arrow_json::ReaderBuilder::new(Arc::new(arrow_schema))
            .build(BufReader::new(file))
            .unwrap();
        let data = json.next().unwrap().unwrap();
        Ok(SimpleData { data })
    }

    pub fn extract(&self, schema: SchemaRef, visitor: &mut dyn DataVisitor) {
        use crate::schema::PrimitiveType;
        let arrow_schema = self.data.schema();
        let mut res_arry: Vec<Option<DataItem<'_>>> = vec![];
        for field in schema.fields.iter() {
            let name = field.name();
            if let Some((arrow_index, arrow_field)) = arrow_schema.column_with_name(name) {
                let col = self.data.column(arrow_index);
                match arrow_field.data_type() {
                    DataType::Boolean => {
                        push_item!(
                            field,
                            col,
                            PrimitiveType::Boolean,
                            array::BooleanArray,
                            res_arry,
                            DataItem::Bool
                        );
                    }
                    DataType::Int64 => {
                        push_item!(
                            field,
                            col,
                            PrimitiveType::Long,
                            array::Int64Array,
                            res_arry,
                            DataItem::I64
                        );
                    }
                    DataType::Utf8 => {
                        push_item!(
                            field,
                            col,
                            PrimitiveType::String,
                            StringArray,
                            res_arry,
                            DataItem::Str
                        );
                    }
                    _ => unimplemented!(),
                }
            }
        }
        visitor.visit(&res_arry);
    }

    pub fn length(&self) -> usize {
        self.data.num_rows()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn create_batch() -> RecordBatch {
        let id_array = StringArray::from(vec![Some("id")]);
        let ct_array = Int64Array::from(vec![1]);
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("created_time", DataType::Int64, false),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(id_array), Arc::new(ct_array)],
        )
        .unwrap()
    }

    #[test]
    fn test_md_extract() {
        use crate::schema::{DataType, PrimitiveType, StructField, StructType};
        let s = SimpleData {
            data: create_batch(),
        };
        let metadata_test_schema = StructType::new(vec![
            StructField::new("id", DataType::Primitive(PrimitiveType::String), false),
            StructField::new(
                "created_time",
                DataType::Primitive(PrimitiveType::Long),
                false,
            ),
        ]);
        let mut metadata_visitor = crate::actions::types::MetadataVisitor::default();
        s.extract(Arc::new(metadata_test_schema), &mut metadata_visitor);

        println!("Got: {:?}", metadata_visitor.extracted);

        assert!(metadata_visitor.extracted.is_some());
        let metadata = metadata_visitor.extracted.unwrap();
        assert!(metadata.id == "id");
        assert!(metadata.created_time == Some(1));
    }
}

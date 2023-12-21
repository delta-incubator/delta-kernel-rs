use crate::engine_data::{DataItem, DataVisitor, EngineData, TypeTag};
use crate::schema::{Schema, SchemaRef, StructField};
use crate::DeltaResult;

use arrow_array::cast::AsArray;
use arrow_array::{array, ArrayRef, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
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
                println!("Pushed {:#?}", i);
                $result_arry.push(Some($enum_typ(i)));
            } else {
                if $result_arry.len() > 0 {
                    println!("Pushing None");
                    $result_arry.push(None);
                }
            }
        }
    };
}

impl SimpleData {
    pub fn try_create_from_json(schema: SchemaRef, location: Url) -> DeltaResult<Self> {
        let arrow_schema: ArrowSchema = (&*schema).try_into()?;
        println!("Reading {:#?} with schema: {:#?}", location, arrow_schema);
        // todo: Check scheme of url
        let file = File::open(location.to_file_path().unwrap()).unwrap(); // todo: fix to_file_path.unwrap()
        let mut json = arrow_json::ReaderBuilder::new(Arc::new(arrow_schema))
            .build(BufReader::new(file))
            .unwrap();
        let data = json.next().unwrap().unwrap();
        Ok(SimpleData { data })
    }

    fn extract_field<'a>(
        &'a self,
        arry: &'a StructArray,
        arrow_schema: &ArrowSchemaRef,
        field: &StructField,
        res_arry: &mut Vec<Option<DataItem<'a>>>,
    ) {
        use crate::schema::PrimitiveType;
        let name = field.name();
        println!("Extracting {}", name);
        if let Some((arrow_index, arrow_field)) = arrow_schema.column_with_name(name) {
            let col = arry.column(arrow_index);
            match arrow_field.data_type() {
                DataType::Struct(arrow_fields) => {
                    match &field.data_type {
                        crate::schema::DataType::Struct(field_struct) => {
                            let inner_schema = Arc::new(ArrowSchema::new(arrow_fields.clone()));
                            let struct_array = col.as_struct();
                            self.extract_inner(struct_array, field_struct, &inner_schema, res_arry);
                        }
                        _ => panic!("schema mismatch")
                    }
                }
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
                DataType::List(_) => {
                    println!("ignoring list");
                    if res_arry.len() > 0 {
                        res_arry.push(None);
                    }
                }
                DataType::Map(_,_) => {
                    println!("ignoring map");
                    if res_arry.len() > 0 {
                        res_arry.push(None);
                    }
                }
                _ => {
                    println!("CAN'T EXTRACT: {}", arrow_field.data_type());
                    unimplemented!()
                }
            }
        }
    }

    // extract fields specified in `schema` from arry. We pass the arrow_schema just as validation
    fn extract_inner<'a>(&'a self, arry: &'a StructArray, schema: &Schema, arrow_schema: &ArrowSchemaRef, res_arry: &mut Vec<Option<DataItem<'a>>>) {
        for field in schema.fields.iter() {
            self.extract_field(arry, &arrow_schema, field, res_arry);
        }
    }

    pub fn extract(&self, schema: SchemaRef, visitor: &mut dyn DataVisitor) {
        let arrow_schema = self.data.schema();
        let mut res_arry: Vec<Option<DataItem<'_>>> = vec![];

        let sa: &StructArray = &self.data.clone().into();
        self.extract_inner(sa, &schema, &arrow_schema, &mut res_arry);

        println!("extracted on: {:#?}", self.data);
        println!("result array len: {}", res_arry.len());
        if res_arry.len() > 0 {
            visitor.visit(&res_arry);
        }
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

    // #[test]
    // fn test_md_extract() {
    //     use crate::schema::{DataType, PrimitiveType, StructField, StructType};
    //     let s = SimpleData {
    //         data: create_batch(),
    //     };
    //     let metadata_test_schema = StructType::new(vec![
    //         StructField::new("id", DataType::Primitive(PrimitiveType::String), false),
    //         StructField::new(
    //             "created_time",
    //             DataType::Primitive(PrimitiveType::Long),
    //             false,
    //         ),
    //     ]);
    //     let mut metadata_visitor = crate::actions::types::MetadataVisitor::default();
    //     s.extract(Arc::new(metadata_test_schema), &mut metadata_visitor);

    //     println!("Got: {:?}", metadata_visitor.extracted);

    //     assert!(metadata_visitor.extracted.is_some());
    //     let metadata = metadata_visitor.extracted.unwrap();
    //     assert!(metadata.id == "id");
    //     assert!(metadata.created_time == Some(1));
    // }
}

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{new_null_array, RecordBatch, RecordBatchOptions};
use arrow_cast::{can_cast_types, cast};
use arrow_schema::{ArrowError, FieldRef, Schema, SchemaRef};
use parquet::schema::types::SchemaDescriptor;

use super::super::util::extract_column;
use super::fields::*;
use crate::error::DeltaResult;

/// A utility which can adapt file-level record batches to a table schema which may have a schema
/// obtained from merging multiple file-level schemas.
///
/// This is useful for enabling schema evolution in partitioned datasets.
///
/// This has to be done in two stages.
///
/// 1. Before reading the file, we have to map projected column indexes from the table schema to
///    the file schema.
///
/// 2. After reading a record batch we need to map the read columns back to the expected columns
///    indexes and insert null-valued columns wherever the file schema was missing a colum present
///    in the table schema.
#[derive(Clone, Debug)]
pub(crate) struct SchemaAdapter {
    /// Schema for the table
    table_schema: SchemaRef,
    table_leaf_map: Arc<Vec<(ColumnPath, (usize, FieldRef))>>,
}

impl SchemaAdapter {
    pub(crate) fn try_new(table_schema: SchemaRef) -> Result<Self, ArrowError> {
        let table_leaf_map = Arc::new(table_schema.fields().leaf_map()?);
        Ok(Self {
            table_schema,
            table_leaf_map,
        })
    }

    pub(crate) fn map_schema(
        &self,
        file_schema: &Schema,
        schema_descriptor: &SchemaDescriptor,
    ) -> DeltaResult<(SchemaMapping, Vec<usize>)> {
        // Create a map parquet column names (paths) to table column names (paths).
        let flm = file_schema.fields().leaf_map()?;
        let key_map = flm
            .iter()
            .map(|(k, _)| (k.parquet_names(), k))
            .collect::<HashMap<_, _>>();
        let pq_leaf_keys = schema_descriptor
            .columns()
            .iter()
            .enumerate()
            .filter_map(|(idx, c)| {
                let key = c.path().parts().to_vec();
                key_map.get(&key).map(|k| (*k, idx))
            })
            .collect::<HashMap<_, _>>();

        // Create a map of file column paths to table column paths as well as a projection
        // vector to read the parquet file with.
        let mut file_leaf_map = HashMap::with_capacity(file_schema.fields().len());
        let mut projection = Vec::with_capacity(file_schema.fields().len());
        let visit = |_: usize, path: &mut ColumnPath, _: &FieldRef| {
            if let Some((tp, _)) = self.table_leaf_map.iter().find(|(k, _)| k == path) {
                file_leaf_map.insert(tp.clone(), path.clone());
                projection.push(*pq_leaf_keys.get(path).unwrap());
            }
            Ok(())
        };

        file_schema.fields().visit_fields(visit)?;

        // map all actual table paths to their corresponding file paths, if present
        let leaf_field_mappings = self
            .table_leaf_map
            .iter()
            .map(|(table_path, (_table_idx, _table_field))| {
                (table_path.clone(), file_leaf_map.get(table_path).cloned())
            })
            .collect();

        Ok((
            SchemaMapping {
                table_schema: self.table_schema.clone(),
                table_path_map: leaf_field_mappings,
            },
            projection,
        ))
    }
}

/// The SchemaMapping struct holds a mapping from the file schema to the table schema
/// and any necessary type conversions that need to be applied.
#[derive(Debug)]
pub(crate) struct SchemaMapping {
    /// The schema of the table. This is the expected schema after conversion and it should match the schema of the query result.
    table_schema: SchemaRef,
    /// A sorted array (in reverse order of table leaf index) of tuples
    /// (table_path, (file_path, field)) where table_path is the path of the leaf field in the table schema
    /// and file_path is the path of the leaf field in the file schema. field is the corresponding field in the
    /// table schema, the file array sould be cases to.
    pub(crate) table_path_map: HashMap<ColumnPath, Option<ColumnPath>>,
}

impl SchemaMapping {
    /// Adapts a `RecordBatch` to match the `table_schema` using the stored mapping and conversions.
    pub(crate) fn map_batch(&self, batch: RecordBatch) -> Result<RecordBatch, ArrowError> {
        let num_rows = batch.num_rows();

        let visit =
            |_: usize, path: &ColumnPath, field: &FieldRef| match self.table_path_map.get(path) {
                Some(Some(file_path)) => {
                    let names = file_path.names();
                    let mut field_idents = names.iter().map(|n| n.as_str());
                    if let Some(next_path_step) = field_idents.next() {
                        let array = extract_column(&batch, next_path_step, &mut field_idents)?;
                        if can_cast_types(array.data_type(), field.data_type()) {
                            cast(&array, field.data_type())
                        } else {
                            Ok(new_null_array(field.data_type(), num_rows))
                        }
                    } else {
                        Ok(new_null_array(field.data_type(), num_rows))
                    }
                }
                Some(None) => Ok(new_null_array(field.data_type(), num_rows)),
                _ => Err(ArrowError::InvalidArgumentError(format!(
                    "Field {} not found in table schema",
                    field.name()
                ))),
            };

        // Pick an array from the batch for the given field. This is mainly used for non-leaf arrays
        // we need to construct, to get the corrext null buffers etc.
        let pick = |path: &ColumnPath, field: &FieldRef| {
            let names = path.names();
            let mut field_idents = names.iter().map(|n| n.as_str());
            if let Some(next_path_step) = field_idents.next() {
                let array = extract_column(&batch, next_path_step, &mut field_idents).cloned();
                if array.is_ok() {
                    array
                } else {
                    Ok(new_null_array(field.data_type(), num_rows))
                }
            } else {
                Ok(new_null_array(field.data_type(), num_rows))
            }
        };

        let arrays = self.table_schema.fields().pick_arrays(visit, pick)?;
        let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
        RecordBatch::try_new_with_options(self.table_schema.clone(), arrays, &options)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Fields};

    use super::*;
    use crate::ActionType;

    lazy_static::lazy_static! {
        static ref STRUCT_FIELDS: Fields = Fields::from(vec![
            Field::new("a", DataType::Float32, true),
            Field::new("b", DataType::Float32, true),
        ]);
        static ref STRUCT_FIELDS_SEL: Fields = Fields::from(vec![
            Field::new("a", DataType::Float32, true),
        ]);
        static ref SIMPLE_MAP: FieldRef = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("keys", DataType::Utf8, false),
                Field::new("values", DataType::Int32, true),
            ])),
            false,
        ));
        static ref COMPLEX_MAP_ENTRIES: FieldRef = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("keys", DataType::Utf8, false),
                Field::new("values", DataType::Struct(STRUCT_FIELDS.clone()), true),
            ])),
            false,
        ));
        static ref COMPLEX_MAP_ENTRIES_SEL: FieldRef = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("keys", DataType::Utf8, false),
                Field::new("values", DataType::Struct(STRUCT_FIELDS_SEL.clone()), true),
            ])),
            false,
        ));
        static ref SIMPLE_LIST: FieldRef = Arc::new(Field::new(
            "list",
            DataType::List(Arc::new(Field::new("element", DataType::Utf8, false))),
            true,
        ));
        static ref COMPLEX_LIST: FieldRef = Arc::new(Field::new(
            "list_complex",
            DataType::List(Arc::new(Field::new("element", DataType::Struct(STRUCT_FIELDS.clone()), false))),
            true,
        ));
        static ref COMPLEX_LIST_SEL: FieldRef = Arc::new(Field::new(
            "list_complex",
            DataType::List(Arc::new(Field::new("element", DataType::Struct(STRUCT_FIELDS_SEL.clone()), false))),
            true,
        ));
    }

    // #[test]
    // fn schema_mapping_map_batch() {
    //     let table_schema = Arc::new(Schema::new(vec![
    //         Field::new("c1", DataType::Utf8, true),
    //         Field::new("c2", DataType::UInt32, true),
    //         Field::new("c3", DataType::Float64, true),
    //     ]));
    //
    //     let adapter = SchemaAdapter::new(table_schema.clone());
    //
    //     let file_schema = Schema::new(vec![
    //         Field::new("c1", DataType::Utf8, true),
    //         Field::new("c2", DataType::UInt64, true),
    //         Field::new("c3", DataType::Float32, true),
    //     ]);
    //
    //     let (mapping, _) = adapter.map_schema(&file_schema).expect("map schema failed");
    //
    //     let c1 = StringArray::from(vec!["hello", "world"]);
    //     let c2 = UInt64Array::from(vec![9_u64, 5_u64]);
    //     let c3 = Float32Array::from(vec![2.0_f32, 7.0_f32]);
    //     let batch = RecordBatch::try_new(
    //         Arc::new(file_schema),
    //         vec![Arc::new(c1), Arc::new(c2), Arc::new(c3)],
    //     )
    //     .unwrap();
    //
    //     let mapped_batch = mapping.map_batch(batch).unwrap();
    //
    //     assert_eq!(mapped_batch.schema(), table_schema);
    //     assert_eq!(mapped_batch.num_columns(), 3);
    //     assert_eq!(mapped_batch.num_rows(), 2);
    //
    //     let c1 = mapped_batch.column(0).as_string::<i32>();
    //     let c2 = mapped_batch.column(1).as_primitive::<UInt32Type>();
    //     let c3 = mapped_batch.column(2).as_primitive::<Float64Type>();
    //
    //     assert_eq!(c1.value(0), "hello");
    //     assert_eq!(c1.value(1), "world");
    //     assert_eq!(c2.value(0), 9_u32);
    //     assert_eq!(c2.value(1), 5_u32);
    //     assert_eq!(c3.value(0), 2.0_f64);
    //     assert_eq!(c3.value(1), 7.0_f64);
    // }

    // #[test]
    // fn schema_adapter_map_schema_with_projection() {
    //     let table_schema = Arc::new(Schema::new(vec![
    //         Field::new("c0", DataType::Utf8, true),
    //         Field::new("c1", DataType::Utf8, true),
    //         Field::new("c2", DataType::Float64, true),
    //         Field::new("c3", DataType::Int32, true),
    //         Field::new("c4", DataType::Float32, true),
    //     ]));
    //
    //     let file_schema = Schema::new(vec![
    //         Field::new("id", DataType::Int32, true),
    //         Field::new("c1", DataType::Boolean, true),
    //         Field::new("c2", DataType::Float32, true),
    //         Field::new("c3", DataType::Binary, true),
    //         Field::new("c4", DataType::Int64, true),
    //     ]);
    //
    //     let indices = vec![1, 2, 4];
    //     let schema = SchemaRef::from(table_schema.project(&indices).unwrap());
    //     let adapter = SchemaAdapter::new(schema);
    //     let (mapping, projection) = adapter.map_schema(&file_schema).unwrap();
    //
    //     let id = Int32Array::from(vec![Some(1), Some(2), Some(3)]);
    //     let c1 = BooleanArray::from(vec![Some(true), Some(false), Some(true)]);
    //     let c2 = Float32Array::from(vec![Some(2.0_f32), Some(7.0_f32), Some(3.0_f32)]);
    //     let c3 = BinaryArray::from_opt_vec(vec![Some(b"hallo"), Some(b"danke"), Some(b"super")]);
    //     let c4 = Int64Array::from(vec![1, 2, 3]);
    //     let batch = RecordBatch::try_new(
    //         Arc::new(file_schema),
    //         vec![
    //             Arc::new(id),
    //             Arc::new(c1),
    //             Arc::new(c2),
    //             Arc::new(c3),
    //             Arc::new(c4),
    //         ],
    //     )
    //     .unwrap();
    //     let rows_num = batch.num_rows();
    //     let projected = batch.project(&projection).unwrap();
    //     let mapped_batch = mapping.map_batch(projected).unwrap();
    //
    //     assert_eq!(
    //         mapped_batch.schema(),
    //         Arc::new(table_schema.project(&indices).unwrap())
    //     );
    //     assert_eq!(mapped_batch.num_columns(), indices.len());
    //     assert_eq!(mapped_batch.num_rows(), rows_num);
    //
    //     let c1 = mapped_batch.column(0).as_string::<i32>();
    //     let c2 = mapped_batch.column(1).as_primitive::<Float64Type>();
    //     let c4 = mapped_batch.column(2).as_primitive::<Float32Type>();
    //
    //     assert_eq!(c1.value(0), "true");
    //     assert_eq!(c1.value(1), "false");
    //     assert_eq!(c1.value(2), "true");
    //
    //     assert_eq!(c2.value(0), 2.0_f64);
    //     assert_eq!(c2.value(1), 7.0_f64);
    //     assert_eq!(c2.value(2), 3.0_f64);
    //
    //     assert_eq!(c4.value(0), 1.0_f32);
    //     assert_eq!(c4.value(1), 2.0_f32);
    //     assert_eq!(c4.value(2), 3.0_f32);
    // }

    fn validate_mapping(
        leaf_field_mappings: &[Option<(ColumnPath, FieldRef)>],
        expected: &[(ColumnPath, &str)],
    ) {
        assert_eq!(
            leaf_field_mappings
                .iter()
                .filter_map(|o| o.as_ref())
                .count(),
            expected.len(),
            "leaf_field_mappings: {:?}",
            leaf_field_mappings
        );
        for (path, name) in expected {
            let (idx, field) = leaf_field_mappings
                .iter()
                .find(|opt| match opt {
                    Some((p, _)) => p == path,
                    _ => false,
                })
                .unwrap()
                .as_ref()
                .unwrap();
            assert_eq!(path, idx);
            assert_eq!(field.name(), name);
        }
    }

    #[test]
    fn test_map_schema_leaves_maps() {
        // let table_schema = Arc::new(Schema::new(vec![
        //     Field::new("c1", DataType::Utf8, true),
        //     Field::new(
        //         "c2",
        //         DataType::Map(COMPLEX_MAP_ENTRIES.clone(), false),
        //         true,
        //     ),
        //     Field::new("c3", DataType::Utf8, true),
        // ]));
        //
        // let leaf_map = table_schema.fields().leaf_map();
        // leaf_map.iter().for_each(|f| {
        //     println!("1: {:?}", f);
        // });

        let table_schema: Field = ActionType::Metadata.try_into().unwrap();
        let table_schema = Arc::new(Schema::new(vec![table_schema]));

        let leaf_map = table_schema.fields().leaf_map().unwrap();
        let mut leaf_map = leaf_map.iter().collect::<Vec<_>>();
        leaf_map.sort_by(|a, b| (a.1).0.cmp(&(b.1).0));

        leaf_map.iter().for_each(|f| {
            println!("2: {:?}", f);
        });

        //     let adapter = SchemaAdapter::new(table_schema.clone());
        //
        //     let file_schema = Arc::new(Schema::new(vec![
        //         Field::new("c1", DataType::Utf8, true),
        //         Field::new(
        //             "c2",
        //             DataType::Map(COMPLEX_MAP_ENTRIES.clone(), false),
        //             true,
        //         ),
        //     ]));
        //     let (mapping, projection) = adapter.map_schema(&file_schema).expect("map failed");
        //
        //     assert_eq!(projection, [0, 1, 2, 3]);
        //     let fields = [
        //         (ColumnPath(vec![ColumnSegment::field("c1")]), "c1"),
        //         (
        //             ColumnPath(vec![
        //                 ColumnSegment::field("c2"),
        //                 ColumnSegment::map_root("entries"),
        //                 ColumnSegment::map_key("keys"),
        //             ]),
        //             "keys",
        //         ),
        //         (
        //             ColumnPath(vec![
        //                 ColumnSegment::field("c2"),
        //                 ColumnSegment::map_root("entries"),
        //                 ColumnSegment::map_value("values"),
        //                 ColumnSegment::field("a"),
        //             ]),
        //             "a",
        //         ),
        //         (
        //             ColumnPath(vec![
        //                 ColumnSegment::field("c2"),
        //                 ColumnSegment::map_root("entries"),
        //                 ColumnSegment::map_value("values"),
        //                 ColumnSegment::field("b"),
        //             ]),
        //             "b",
        //         ),
        //     ];
        //     validate_mapping(&mapping, &fields);
        //
        //     let file_schema = Arc::new(Schema::new(vec![
        //         Field::new("c1", DataType::Utf8, true),
        //         Field::new(
        //             "c2",
        //             DataType::Map(COMPLEX_MAP_ENTRIES_SEL.clone(), false),
        //             true,
        //         ),
        //     ]));
        //     let (mapping, projection) = adapter.map_schema(&file_schema).expect("map failed");
        //
        //     assert_eq!(projection, [0, 1, 2]);
        //     let fields = [
        //         (ColumnPath(vec![ColumnSegment::field("c1")]), "c1"),
        //         (
        //             ColumnPath(vec![
        //                 ColumnSegment::field("c2"),
        //                 ColumnSegment::map_root("entries"),
        //                 ColumnSegment::map_key("keys"),
        //             ]),
        //             "keys",
        //         ),
        //         (
        //             ColumnPath(vec![
        //                 ColumnSegment::field("c2"),
        //                 ColumnSegment::map_root("entries"),
        //                 ColumnSegment::map_value("values"),
        //                 ColumnSegment::field("a"),
        //             ]),
        //             "a",
        //         ),
        //     ];
        //     validate_mapping(&mapping, &fields);
        //
        //     let table_schema = Arc::new(Schema::new(vec![
        //         Field::new("c1", DataType::Utf8, true),
        //         COMPLEX_LIST.as_ref().clone(),
        //     ]));
        //     let adapter = SchemaAdapter::new(table_schema.clone());
        //
        //     let file_schema = Arc::new(Schema::new(vec![
        //         Field::new("c1", DataType::Utf8, true),
        //         COMPLEX_LIST_SEL.as_ref().clone(),
        //     ]));
        //     let (mapping, projection) = adapter.map_schema(&file_schema).expect("map failed");
        //     mapping.leaf_field_mappings.iter().for_each(|f| {
        //         println!("leaf_field_mappings: {:?}", f);
        //     });
    }

    // #[test]
    // fn test_map_schema_leafs() {
    //     let floats = Fields::from(vec![
    //         Field::new("a", DataType::Float32, true),
    //         Field::new("b", DataType::Float32, true),
    //     ]);
    //     let map_field = Arc::new(Field::new(
    //         "entries",
    //         DataType::Struct(Fields::from(vec![
    //             Field::new("keys", DataType::Utf8, false),
    //             Field::new("values", DataType::Int32, true),
    //         ])),
    //         false,
    //     ));
    //     let table_schema = Arc::new(Schema::new(vec![
    //         Field::new("c3", DataType::Struct(floats.clone()), true),
    //         Field::new("c1", DataType::Utf8, true),
    //         Field::new("c2", DataType::Int32, true),
    //         Field::new("c4", DataType::Map(map_field.clone(), false), true),
    //     ]));
    //     let adapter = SchemaAdapter::new(table_schema.clone());
    //
    //     let file_schema: SchemaRef = Arc::new(Schema::new(vec![
    //         Field::new("c0", DataType::Utf8, true),
    //         Field::new("c1", DataType::Utf8, true),
    //         Field::new("c3", DataType::Struct(floats.clone()), true),
    //         Field::new("c4", DataType::Map(map_field.clone(), false), true),
    //     ]));
    //     let (mapping, projection) = adapter.map_schema(&file_schema).expect("map failed");
    //
    //     // assert_eq!(projection, [1, 2, 3]);
    //     println!("projection: {:?}", projection);
    //     mapping.leaf_field_mappings.iter().for_each(|f| {
    //         println!("leaf_field_mappings: {:?}", f);
    //     });
    //
    //     let c1 = Arc::new(StringArray::from(vec![Some("hello"), Some("world")]));
    //     let c2 = Arc::new(Int32Array::from(vec![Some(9_i32), None]));
    //     let c3_a = Arc::new(Float32Array::from(vec![Some(1.0_f32), Some(2.0_f32)]));
    //     let c3_b = Arc::new(Float32Array::from(vec![Some(10.0_f32), Some(20.0_f32)]));
    //     let c3 = Arc::new(StructArray::try_new(floats, vec![c3_a, c3_b], None).unwrap());
    //
    //     let string_builder = StringBuilder::new();
    //     let int_builder = Int32Builder::with_capacity(4);
    //     let mut builder = MapBuilder::new(None, string_builder, int_builder);
    //
    //     builder.keys().append_value("joe");
    //     builder.values().append_value(1);
    //     builder.append(true).unwrap();
    //
    //     builder.keys().append_value("blogs");
    //     builder.values().append_value(2);
    //     builder.append(true).unwrap();
    //
    //     let array = Arc::new(builder.finish());
    //
    //     let file_data =
    //         RecordBatch::try_new(file_schema, vec![c1.clone(), c1.clone(), c3, array]).unwrap();
    // }

    // #[test]
    // fn test_leaf_filter() {
    //     let c1 = Arc::new(StringArray::from(vec![Some("hello"), Some("world")]));
    //     let c2 = Arc::new(Int32Array::from(vec![Some(9_i32), None]));
    //     let c3_a = Arc::new(Float32Array::from(vec![Some(1.0_f32), Some(2.0_f32)]));
    //     let c3_b = Arc::new(Float32Array::from(vec![Some(10.0_f32), Some(20.0_f32)]));
    //
    //     let floats = Fields::from(vec![
    //         Field::new("a", DataType::Float32, true),
    //         Field::new("b", DataType::Float32, true),
    //     ]);
    //     let table_schema = Arc::new(Schema::new(vec![
    //         Field::new("c3", DataType::Struct(floats.clone()), true),
    //         Field::new("c1", DataType::Utf8, true),
    //         Field::new("c2", DataType::Int32, true),
    //     ]));
    //     let adapter = SchemaAdapter::new(table_schema.clone());
    //
    //     let file_schema: SchemaRef = Arc::new(Schema::new(vec![
    //         Field::new("c1", DataType::Utf8, true),
    //         Field::new("c2", DataType::Int32, true),
    //     ]));
    //     let (_, projection) = adapter
    //         .map_schema(&file_schema)
    //         .expect("map leaf schema failed");
    //     assert_eq!(projection, [0, 1]);
    //
    //     let file_floats = Fields::from(vec![
    //         Field::new("a", DataType::Float64, true),
    //         Field::new("b", DataType::Float64, true),
    //     ]);
    //     let file_schema: SchemaRef = Arc::new(Schema::new(vec![
    //         Field::new("c3", DataType::Struct(file_floats.clone()), true),
    //         Field::new("c1", DataType::Utf8, true),
    //         Field::new("c2", DataType::Int32, true),
    //     ]));
    //     let (_, projection) = adapter.map_schema(&file_schema).expect("map failed");
    //     assert_eq!(projection, [0, 1, 2, 3]);
    //
    //     let file_schema: SchemaRef = Arc::new(Schema::new(vec![
    //         Field::new("c0", DataType::Utf8, true),
    //         Field::new("c1", DataType::Utf8, true),
    //         Field::new("c3", DataType::Struct(file_floats), true),
    //         Field::new("c4", DataType::Int32, true),
    //     ]));
    //     let (mapping, projection) = adapter.map_schema(&file_schema).expect("map failed");
    //
    //     assert_eq!(projection, [1, 2, 3]);
    //     mapping.leaf_field_mappings.iter().for_each(|f| {
    //         println!("leaf_field_mappings: {:?}", f);
    //     });
    // }
}

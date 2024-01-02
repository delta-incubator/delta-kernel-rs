use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{new_null_array, RecordBatch, RecordBatchOptions};
use arrow_cast::{cast_with_options, CastOptions};
use arrow_schema::{ArrowError, FieldRef, Schema, SchemaRef};
use parquet::schema::types::SchemaDescriptor;

use super::super::util::extract_column;
use super::fields::*;
use crate::error::DeltaResult;

/// A utility which can adapt file-level record batches to a table schema.
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
    safe_cast: bool,
}

impl SchemaAdapter {
    pub(crate) fn try_new(table_schema: SchemaRef) -> Result<Self, ArrowError> {
        let table_leaf_map = Arc::new(table_schema.fields().leaf_map()?);
        Ok(Self {
            table_schema,
            table_leaf_map,
            safe_cast: false,
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
                if let Some(pq_idx) = pq_leaf_keys.get(path) {
                    projection.push(*pq_idx);
                } else {
                    println!("path: {:?}", path);
                }
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
                safe_cast: self.safe_cast,
            },
            projection,
        ))
    }
}

/// The SchemaMapping struct holds a mapping from the file schema to the table schema
/// and any necessary type conversions that need to be applied.
#[derive(Debug)]
pub(crate) struct SchemaMapping {
    /// The schema of the table. This is the expected schema after conversion.
    /// The schema mapper will try to cast leaf columns to match this schema.
    table_schema: SchemaRef,
    pub(crate) table_path_map: HashMap<ColumnPath, Option<ColumnPath>>,
    safe_cast: bool,
}

impl SchemaMapping {
    /// Adapts a `RecordBatch` to match the `table_schema` using the stored mapping and conversions.
    pub(crate) fn map_batch(&self, batch: RecordBatch) -> Result<RecordBatch, ArrowError> {
        let num_rows = batch.num_rows();
        let cast_options = CastOptions {
            safe: self.safe_cast,
            ..Default::default()
        };

        // Visit a field in the table schema and try to find the corresponding column in the data.
        let visit =
            |_: usize, path: &ColumnPath, field: &FieldRef| match self.table_path_map.get(path) {
                Some(Some(file_path)) => {
                    let names = file_path.names();
                    let mut field_idents = names.iter().map(|n| n.as_str());
                    if let Some(next_path_step) = field_idents.next() {
                        let array = extract_column(&batch, next_path_step, &mut field_idents)?;
                        cast_with_options(&array, field.data_type(), &cast_options)
                    } else {
                        Ok(new_null_array(field.data_type(), num_rows))
                    }
                }
                Some(None) => Ok(new_null_array(field.data_type(), num_rows)),
                // NOTE: this should never happen as we are only mapping fields that are present in
                // the table schema.
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

        // NOTE: required to handle empty batches
        let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
        let arrays = self.table_schema.fields().pick_arrays(visit, pick)?;
        RecordBatch::try_new_with_options(self.table_schema.clone(), arrays, &options)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use object_store::path::Path;

    use super::*;
    use crate::schema::{ArrayType, DataType, MapType, StructField, StructType};
    use crate::test_util::{assert_batches_sorted_eq, TestResult, TestTableFactory};
    use crate::{ActionType, TableClient};

    // TODO
    // - handle updating nullability of columns

    #[allow(dead_code)]
    enum Scenario {
        /// A table with a schema that is a superset of the file schema.
        Superset,
        /// A table with a schema that is a subset of the file schema.
        Subset,
        /// A table with a schema that matches the file schema.
        Match,
    }

    fn get_parquet_schema<'a>(
        fields: impl IntoIterator<Item = &'a str>,
        scenario: Scenario,
    ) -> Arc<StructType> {
        let mut columns = Vec::new();
        for field in fields {
            match field {
                "int" => {
                    columns.push(StructField::new("int", DataType::LONG, true));
                }
                "float" => {
                    columns.push(StructField::new("float", DataType::DOUBLE, true));
                }
                "struct" => {
                    let struct_fields = match scenario {
                        Scenario::Match => StructType::new(vec![
                            StructField::new("a", DataType::LONG, true),
                            StructField::new(
                                "b",
                                DataType::Array(Box::new(ArrayType::new(DataType::LONG, true))),
                                true,
                            ),
                            StructField::new("c", DataType::LONG, true),
                        ]),
                        Scenario::Subset => StructType::new(vec![
                            StructField::new("a", DataType::LONG, true),
                            StructField::new("c", DataType::LONG, true),
                        ]),
                        Scenario::Superset => StructType::new(vec![
                            StructField::new("a", DataType::LONG, true),
                            StructField::new("c", DataType::LONG, true),
                            StructField::new("d", DataType::LONG, true),
                        ]),
                    };
                    columns.push(StructField::new(
                        "struct",
                        DataType::Struct(Box::new(struct_fields)),
                        true,
                    ));
                }
                "list" => {
                    let list_fields = match scenario {
                        Scenario::Match => StructType::new(vec![
                            StructField::new("a", DataType::LONG, false),
                            StructField::new("b", DataType::LONG, true),
                        ]),
                        Scenario::Subset => {
                            StructType::new(vec![StructField::new("b", DataType::LONG, true)])
                        }
                        Scenario::Superset => StructType::new(vec![
                            StructField::new("a", DataType::LONG, false),
                            StructField::new("b", DataType::LONG, true),
                            StructField::new("c", DataType::LONG, true),
                        ]),
                    };
                    columns.push(StructField::new(
                        "list",
                        DataType::Array(Box::new(ArrayType::new(
                            DataType::Struct(Box::new(list_fields)),
                            true,
                        ))),
                        true,
                    ));
                }
                "map" => {
                    columns.push(StructField::new(
                        "map",
                        DataType::Map(Box::new(MapType::new(
                            DataType::STRING,
                            DataType::INTEGER,
                            true,
                        ))),
                        true,
                    ));
                }
                _ => (),
            }
        }

        Arc::new(StructType::new(columns))
    }

    #[test]
    fn test_load_complex_parquet() -> TestResult {
        let mut factory = TestTableFactory::new();
        let path = "./tests/parquet/";
        let tt = factory.load_location("parquet", path);
        let (_, engine) = tt.table();
        let handler = engine.get_parquet_handler();

        // Reading all fields from a complex parquet file written by different engines.
        // NOTE: It seems that the parquet writer in polars does not write maps as pyarrow and spark do.
        //       Specifically the map seems to be written as a list of structs with two fields, key and value
        //
        // spark / pyarrow:
        //   map: map<string, int32 ('map')>
        //     child 0, map: struct<key: string not null, value: int32> not null
        //       child 0, key: string not null
        //       child 1, value: int32
        //
        // polars:
        //   map: large_list<item: struct<key: large_string, value: int32>>
        //     child 0, item: struct<key: large_string, value: int32>
        //       child 0, key: large_string
        //       child 1, value: int32

        let variants = ["pyarrow", "spark", "duckdb"];
        let read_schema =
            get_parquet_schema(["int", "float", "struct", "list", "map"], Scenario::Match);
        let expected = &[
            "+-----+-------+------------------------+------------------------------+--------------+",
            "| int | float | struct                 | list                         | map          |",
            "+-----+-------+------------------------+------------------------------+--------------+",
            "| 1   | 1.0   |                        |                              | {a: 1, b: 2} |",
            "| 2   |       | {a: 1, b: [2, 3], c: } | [{a: 1, b: 2}, {a: 3, b: 4}] |              |",
            "| 3   | 3.0   | {a: 3, b: [4], c: 5}   | [{a: 5, b: }, {a: 6, b: 7}]  | {d: 4}       |",
            "+-----+-------+------------------------+------------------------------+--------------+",
          ];
        for variant in variants {
            let files = &[tt.head(&Path::from(format!("{}.parquet", variant)))?];
            let data = handler
                .read_parquet_files(files, read_schema.clone(), None)?
                .collect::<Result<Vec<_>, _>>()?;
            assert_batches_sorted_eq!(expected, &data);
        }

        let variants = ["pyarrow", "spark", "duckdb"];
        let read_schema = get_parquet_schema(["int", "float", "struct", "list"], Scenario::Match);
        let expected = &[
            "+-----+-------+------------------------+------------------------------+",
            "| int | float | struct                 | list                         |",
            "+-----+-------+------------------------+------------------------------+",
            "| 1   | 1.0   |                        |                              |",
            "| 2   |       | {a: 1, b: [2, 3], c: } | [{a: 1, b: 2}, {a: 3, b: 4}] |",
            "| 3   | 3.0   | {a: 3, b: [4], c: 5}   | [{a: 5, b: }, {a: 6, b: 7}]  |",
            "+-----+-------+------------------------+------------------------------+",
        ];
        for variant in variants {
            let files = &[tt.head(&Path::from(format!("{}.parquet", variant)))?];
            let data = handler
                .read_parquet_files(files, read_schema.clone(), None)?
                .collect::<Result<Vec<_>, _>>()?;
            assert_batches_sorted_eq!(expected, &data);
        }

        Ok(())
    }

    #[test]
    fn test_project_nested_fields() -> TestResult {
        let mut factory = TestTableFactory::new();
        let path = "./tests/parquet/";
        let tt = factory.load_location("parquet", path);
        let (_, engine) = tt.table();
        let handler = engine.get_parquet_handler();

        let variants = ["pyarrow", "spark", "duckdb"];
        let read_schema =
            get_parquet_schema(["int", "float", "struct", "list", "map"], Scenario::Subset);
        let expected = &[
            "+-----+-------+--------------+------------------+--------------+",
            "| int | float | struct       | list             | map          |",
            "+-----+-------+--------------+------------------+--------------+",
            "| 1   | 1.0   |              |                  | {a: 1, b: 2} |",
            "| 2   |       | {a: 1, c: }  | [{b: 2}, {b: 4}] |              |",
            "| 3   | 3.0   | {a: 3, c: 5} | [{b: }, {b: 7}]  | {d: 4}       |",
            "+-----+-------+--------------+------------------+--------------+",
        ];
        for variant in variants {
            let files = &[tt.head(&Path::from(format!("{}.parquet", variant)))?];
            let data = handler
                .read_parquet_files(files, read_schema.clone(), None)?
                .collect::<Result<Vec<_>, _>>()?;
            assert_batches_sorted_eq!(expected, &data);
        }

        Ok(())
    }

    #[test]
    fn test_read_checkpoints() -> TestResult {
        let mut factory = TestTableFactory::new();
        let path = "./tests/parquet/checkpoints/";
        let tt = factory.load_location("parquet", path);
        let (_, engine) = tt.table();
        let handler = engine.get_parquet_handler();

        let files = &[tt.head(&Path::from("d121_only_struct_stats.checkpoint.parquet"))?];

        let read_schema = Arc::new(StructType::new(vec![ActionType::Add
            .schema_field()
            .clone()]));
        let expected = &[
            "+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| add                                                                                                                                                                                                                                                                                                                                  |",
            "+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "|                                                                                                                                                                                                                                                                                                                                      |",
            "|                                                                                                                                                                                                                                                                                                                                      |",
            "| {path: part-00000-1c2d1a32-02dc-484f-87ff-4328ea56045d-c000.snappy.parquet, partitionValues: {}, size: 5488, modificationTime: 1666652376000, dataChange: false, stats: , tags: {INSERTION_TIME: 1666652376000000, OPTIMIZE_TARGET_SIZE: 268435456}, deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: } |",
            "| {path: part-00000-28925d3a-bdf2-411e-bca9-b067444cbcb0-c000.snappy.parquet, partitionValues: {}, size: 5489, modificationTime: 1666652374000, dataChange: false, stats: , tags: {INSERTION_TIME: 1666652374000000, OPTIMIZE_TARGET_SIZE: 268435456}, deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: } |",
            "| {path: part-00000-6630b7c4-0aca-405b-be86-68a812f2e4c8-c000.snappy.parquet, partitionValues: {}, size: 5489, modificationTime: 1666652378000, dataChange: false, stats: , tags: {INSERTION_TIME: 1666652378000000, OPTIMIZE_TARGET_SIZE: 268435456}, deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: } |",
            "| {path: part-00000-74151571-7ec6-4bd6-9293-b5daab2ce667-c000.snappy.parquet, partitionValues: {}, size: 5489, modificationTime: 1666652377000, dataChange: false, stats: , tags: {INSERTION_TIME: 1666652377000000, OPTIMIZE_TARGET_SIZE: 268435456}, deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: } |",
            "| {path: part-00000-7a509247-4f58-4453-9202-51d75dee59af-c000.snappy.parquet, partitionValues: {}, size: 5489, modificationTime: 1666652373000, dataChange: false, stats: , tags: {INSERTION_TIME: 1666652373000000, OPTIMIZE_TARGET_SIZE: 268435456}, deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: } |",
            "| {path: part-00000-b26ba634-874c-45b0-a7ff-2f0395a53966-c000.snappy.parquet, partitionValues: {}, size: 5489, modificationTime: 1666652375000, dataChange: false, stats: , tags: {INSERTION_TIME: 1666652375000000, OPTIMIZE_TARGET_SIZE: 268435456}, deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: } |",
            "| {path: part-00000-c4c8caec-299d-42a4-b50c-5a4bf724c037-c000.snappy.parquet, partitionValues: {}, size: 5489, modificationTime: 1666652379000, dataChange: false, stats: , tags: {INSERTION_TIME: 1666652379000000, OPTIMIZE_TARGET_SIZE: 268435456}, deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: } |",
            "| {path: part-00000-ce300400-58ff-4b8f-8ba9-49422fdf9f2e-c000.snappy.parquet, partitionValues: {}, size: 5489, modificationTime: 1666652382000, dataChange: false, stats: , tags: {INSERTION_TIME: 1666652382000000, OPTIMIZE_TARGET_SIZE: 268435456}, deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: } |",
            "| {path: part-00000-e8e3753f-e2f6-4c9f-98f9-8f3d346727ba-c000.snappy.parquet, partitionValues: {}, size: 5489, modificationTime: 1666652380000, dataChange: false, stats: , tags: {INSERTION_TIME: 1666652380000000, OPTIMIZE_TARGET_SIZE: 268435456}, deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: } |",
            "| {path: part-00000-f73ff835-0571-4d67-ac43-4fbf948bfb9b-c000.snappy.parquet, partitionValues: {}, size: 5731, modificationTime: 1666652383000, dataChange: false, stats: , tags: {INSERTION_TIME: 1666652383000000, OPTIMIZE_TARGET_SIZE: 268435456}, deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: } |",
            "+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
        ];
        let data = handler
            .read_parquet_files(files, read_schema.clone(), None)?
            .collect::<Result<Vec<_>, _>>()?;
        assert_batches_sorted_eq!(expected, &data);

        let read_schema = Arc::new(StructType::new(vec![ActionType::Protocol
            .schema_field()
            .clone()]));
        let expected = &[
            "+--------------------------------------------------------------------------------+",
            "| protocol                                                                       |",
            "+--------------------------------------------------------------------------------+",
            "|                                                                                |",
            "|                                                                                |",
            "|                                                                                |",
            "|                                                                                |",
            "|                                                                                |",
            "|                                                                                |",
            "|                                                                                |",
            "|                                                                                |",
            "|                                                                                |",
            "|                                                                                |",
            "|                                                                                |",
            "| {minReaderVersion: 1, minWriterVersion: 2, readerFeatures: , writerFeatures: } |",
            "+--------------------------------------------------------------------------------+",
        ];
        let data = handler
            .read_parquet_files(files, read_schema.clone(), None)?
            .collect::<Result<Vec<_>, _>>()?;
        assert_batches_sorted_eq!(expected, &data);

        Ok(())
    }

    // #[test]
    // fn test_fill_missing_nested_fields() -> TestResult {
    //     let mut factory = TestTableFactory::new();
    //     let path = "./tests/parquet/";
    //     let tt = factory.load_location("parquet", path);
    //     let (_, engine) = tt.table();
    //     let handler = engine.get_parquet_handler();
    //
    //     let variants = ["pyarrow", "spark"];
    //     let read_schema = get_parquet_schema(
    //         ["int", "float", "struct", "list", "map"],
    //         Scenbario::Superset,
    //     );
    //     let expected = &[
    //         "+-----+-------+--------------+------------------+--------------+",
    //         "| int | float | struct       | list             | map          |",
    //         "+-----+-------+--------------+------------------+--------------+",
    //         "| 1   | 1.0   | {a: , c: }   |                  | {a: 1, b: 2} |",
    //         "| 2   |       | {a: 1, c: }  | [{b: 2}, {b: 4}] | {}           |",
    //         "| 3   | 3.0   | {a: 3, c: 5} | [{b: }, {b: 7}]  | {d: 4}       |",
    //         "+-----+-------+--------------+------------------+--------------+",
    //     ];
    //     for variant in variants {
    //         let files = &[tt.head(&Path::from(format!("{}.parquet", variant)))?];
    //         let data = handler
    //             .read_parquet_files(files, read_schema.clone(), None)?
    //             .collect::<Result<Vec<_>, _>>()?;
    //         assert_batches_sorted_eq!(expected, &data);
    //     }
    //
    //     Ok(())
    // }

    #[test]
    fn test_map_schema_leaves_maps() {
        let table_schema: Field = ActionType::Metadata.try_into().unwrap();
        let table_schema = Arc::new(Schema::new(vec![table_schema]));

        let leaf_map = table_schema.fields().leaf_map().unwrap();
        let mut leaf_map = leaf_map.iter().collect::<Vec<_>>();
        leaf_map.sort_by(|a, b| (a.1).0.cmp(&(b.1).0));

        leaf_map.iter().for_each(|f| {
            println!("2: {:?}", f);
        });
    }
}

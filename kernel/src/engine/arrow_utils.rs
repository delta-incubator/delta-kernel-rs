//! Some utilities for working with arrow data types

use std::sync::Arc;

use crate::{
    schema::{DataType, Schema, SchemaRef, StructField, StructType},
    utils::require,
    DeltaResult, Error,
};

use arrow_array::RecordBatch;
use arrow_schema::{
    DataType as ArrowDataType, Fields, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use parquet::{arrow::ProjectionMask, schema::types::SchemaDescriptor};

/// Reordering is specified as a tree. Each level is a vec of `ReorderIndex`s. Each element's index
/// represents a column that will be in the read parquet data at that level and index. If the value
/// stored is an `Index` variant, the associated `usize` is the position that the column should
/// appear in the final output. If it is a `Child` variant, then at that index there is a `Struct`
/// whose ordering is specified by the values in the associated `Vec` according to these same rules.
#[derive(Debug, PartialEq)]
pub(crate) enum ReorderIndex {
    Index(usize),
    Child(Vec<ReorderIndex>),
}

/// helper function, does the same as `get_requested_indices` but at an offset. used to recurse into
/// structs. this is called recursively to traverse into structs and lists. `parquet_offset` is how
/// many parquet fields exist before processing this potentially nested schema. `reorder_offset` is
/// how many fields we've found so far before processing at this nested schema. returns the number
/// of parquet fields and the number of requested fields processed
fn get_indices(
    start_parquet_offset: usize,
    requested_schema: &Schema,
    fields: &Fields,
    mask_indices: &mut Vec<usize>,
) -> DeltaResult<(usize, Vec<ReorderIndex>)> {
    let mut found_fields = 0;
    let mut parquet_offset = start_parquet_offset;
    let mut reorder_indices = vec![];
    //println!("at top with parquet_offset {parquet_offset}");
    for (parquet_index, field) in fields.iter().enumerate() {
        //println!("looking at field {}", field.name());
        match field.data_type() {
            ArrowDataType::Struct(fields) => {
                if let Some((_index, _, requested_field)) =
                    requested_schema.fields.get_full(field.name())
                {
                    match requested_field.data_type {
                        DataType::Struct(ref requested_schema) => {
                            let (parquet_advance, child_indices) = get_indices(
                                found_fields + parquet_offset,
                                requested_schema.as_ref(),
                                fields,
                                mask_indices,
                            )?;
                            // advance the number of parquet fields, but subtract 1 because the
                            // struct will be counted by the `enumerate` call but doesn't count as
                            // an actual index.
                            //println!("here:\n parquet_offset: {parquet_offset}\n parquet_advance: {parquet_advance}");
                            parquet_offset += parquet_advance - 1;
                            // also increase found_fields because the struct is a field we've found
                            // and will count in the `requested_schema.fields.len()` call below
                            found_fields += 1;
                            // push the child reorder on
                            reorder_indices.push(ReorderIndex::Child(child_indices));
                        }
                        _ => {
                            return Err(Error::unexpected_column_type(field.name()));
                        }
                    }
                }
            }
            ArrowDataType::List(list_field) | ArrowDataType::ListView(list_field) => {
                if let Some((index, _, requested_field)) =
                    requested_schema.fields.get_full(field.name())
                {
                    // we just want to transparently recurse into lists, need to transform the kernel
                    // list data type into a schema
                    match requested_field.data_type() {
                        DataType::Array(array_type) => {
                            let requested_schema = StructType::new(vec![StructField::new(
                                list_field.name().clone(), // so we find it in the inner call
                                array_type.element_type.clone(),
                                array_type.contains_null,
                            )]);
                            let (parquet_advance, child_indices) = get_indices(
                                found_fields + parquet_offset,
                                &requested_schema,
                                &[list_field.clone()].into(),
                                mask_indices,
                            )?;
                            // see comment above in struct match arm
                            //println!("here list:\n parquet_offset: {parquet_offset}\n parquet_advance: {parquet_advance}");
                            parquet_offset += parquet_advance - 1;
                            found_fields += 1;
                            // we have to recurse to find the type, but for reordering a list we
                            // only need a child reordering if the inner type is a struct
                            if let ArrowDataType::Struct(_) = list_field.data_type() {
                                if child_indices.len() != 1 {
                                    return Err(Error::generic("List call should not have generated more than one reorder index"));
                                }
                                // safety, checked that we have 1 element
                                let child_indices = child_indices.into_iter().next().unwrap();
                                reorder_indices.push(child_indices);
                            } else {
                                reorder_indices.push(ReorderIndex::Index(index));
                            }
                        }
                        _ => {
                            return Err(Error::unexpected_column_type(list_field.name()));
                        }
                    }
                }
            }
            _ => {
                if let Some(index) = requested_schema.index_of(field.name()) {
                    found_fields += 1;
                    mask_indices.push(parquet_offset + parquet_index);
                    reorder_indices.push(ReorderIndex::Index(index));
                }
            }
        }
    }
    //println!("found {found_fields}");
    //println!("found {found_fields}, requested {}. req schema: {:?}", requested_schema.fields.len(), requested_schema);
    require!(
        found_fields == requested_schema.fields.len(),
        Error::generic("Didn't find all requested columns in parquet schema")
    );
    Ok((
        parquet_offset + fields.len() - start_parquet_offset,
        reorder_indices,
    ))
}

/// Get the indices in `parquet_schema` of the specified columns in `requested_schema`. This
/// returns a tuples of (mask_indices: Vec<parquet_schema_index>, reorder_indices:
/// Vec<requested_index>). `mask_indices` is used for generating the mask for reading from the
/// parquet file, and simply contains an entry for each index we wish to select from the parquet
/// file set to the index of the requested column in the parquet. `reorder_indices` is used for
/// re-ordering and will be the same size as `requested_schema`. Each index in `reorder_indices`
/// represents a column that will be in the read parquet data at that index. The value stored in
/// `reorder_indices` is the position that the column should appear in the final output. For
/// example, if `reorder_indices` is `[2,0,1]`, then the re-ordering code should take the third
/// column in the raw-read parquet data, and move it to the first column in the final output, the
/// first column to the second, and the second to the third.
pub(crate) fn get_requested_indices(
    requested_schema: &SchemaRef,
    parquet_schema: &ArrowSchemaRef,
) -> DeltaResult<(Vec<usize>, Vec<ReorderIndex>)> {
    let mut mask_indices = vec![];
    let (_, reorder_indexes) = get_indices(
        0,
        requested_schema,
        parquet_schema.fields(),
        &mut mask_indices,
    )?;
    Ok((mask_indices, reorder_indexes)) // FIX ME
}

/// Create a mask that will only select the specified indices from the parquet. Currently we only
/// handle "root" level columns, and hence use `ProjectionMask::roots`, but will support leaf
/// selection in the future. See issues #86 and #96 as well.
pub(crate) fn generate_mask(
    requested_schema: &SchemaRef,
    parquet_schema: &ArrowSchemaRef,
    parquet_physical_schema: &SchemaDescriptor,
    indices: &[usize],
) -> Option<ProjectionMask> {
    if parquet_schema.fields.size() == requested_schema.fields.len() {
        // we assume that in get_requested_indices we will have caught any column name mismatches,
        // so here we can just say that if we request the same # of columns as the parquet file
        // actually has, we don't need to mask anything out
        None
    } else {
        Some(ProjectionMask::roots(
            parquet_physical_schema,
            indices.to_owned(),
        ))
    }
}

/// Reorder a RecordBatch to match `requested_ordering`. For each non-zero value in
/// `requested_ordering`, the column at that index will be added in order to returned batch
pub(crate) fn reorder_record_batch(
    input_data: RecordBatch,
    requested_ordering: &[usize],
) -> DeltaResult<RecordBatch> {
    if requested_ordering.windows(2).all(|is| is[0] < is[1]) {
        // indices is already sorted, meaning we requested in the order that the columns were
        // stored in the parquet
        Ok(input_data)
    } else {
        // requested an order different from the parquet, reorder
        let input_schema = input_data.schema();
        let mut fields = Vec::with_capacity(requested_ordering.len());
        let reordered_columns = requested_ordering
            .iter()
            .map(|index| {
                fields.push(input_schema.field(*index).clone());
                input_data.column(*index).clone() // cheap Arc clone
            })
            .collect();
        let schema = Arc::new(ArrowSchema::new(fields));
        Ok(RecordBatch::try_new(schema, reordered_columns)?)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};

    use crate::schema::{ArrayType, DataType, StructField, StructType};

    use super::{get_requested_indices, ReorderIndex};

    macro_rules! rii {
        ($index: expr) => {{
            ReorderIndex::Index($index)
        }};
    }

    fn nested_arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i", ArrowDataType::Int32, false),
            ArrowField::new(
                "nested",
                ArrowDataType::Struct(
                    vec![
                        ArrowField::new("int32", ArrowDataType::Int32, false),
                        ArrowField::new("string", ArrowDataType::Utf8, false),
                    ]
                    .into(),
                ),
                false,
            ),
            ArrowField::new("j", ArrowDataType::Int32, false),
        ]))
    }

    #[test]
    fn simple_mask_indices() {
        let kernel_schema = Arc::new(StructType::new(vec![
            StructField::new("i", DataType::INTEGER, false),
            StructField::new("s", DataType::STRING, true),
            StructField::new("i2", DataType::INTEGER, true),
        ]));
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i", ArrowDataType::Int32, false),
            ArrowField::new("s", ArrowDataType::Utf8, true),
            ArrowField::new("i2", ArrowDataType::Int32, true),
        ]));
        let (mask_indices, reorder_indices) =
            get_requested_indices(&kernel_schema, &arrow_schema).unwrap();
        let expect_mask = vec![0, 1, 2];
        let expect_reorder = vec![rii!(0), rii!(1), rii!(2)];
        assert_eq!(mask_indices, expect_mask);
        assert_eq!(reorder_indices, expect_reorder);
    }

    #[test]
    fn simple_reorder_indices() {
        let kernel_schema = Arc::new(StructType::new(vec![
            StructField::new("i", DataType::INTEGER, false),
            StructField::new("s", DataType::STRING, true),
            StructField::new("i2", DataType::INTEGER, true),
        ]));
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i2", ArrowDataType::Int32, true),
            ArrowField::new("i", ArrowDataType::Int32, false),
            ArrowField::new("s", ArrowDataType::Utf8, true),
        ]));
        let (mask_indices, reorder_indices) =
            get_requested_indices(&kernel_schema, &arrow_schema).unwrap();
        let expect_mask = vec![0, 1, 2];
        let expect_reorder = vec![rii!(2), rii!(0), rii!(1)];
        assert_eq!(mask_indices, expect_mask);
        assert_eq!(reorder_indices, expect_reorder);
    }

    #[test]
    fn nested_indices() {
        let kernel_schema = Arc::new(StructType::new(vec![
            StructField::new("i", DataType::INTEGER, false),
            StructField::new(
                "nested",
                StructType::new(vec![
                    StructField::new("int32", DataType::INTEGER, false),
                    StructField::new("string", DataType::STRING, false),
                ]),
                false,
            ),
            StructField::new("j", DataType::INTEGER, false),
        ]));
        let arrow_schema = nested_arrow_schema();
        let (mask_indices, reorder_indices) =
            get_requested_indices(&kernel_schema, &arrow_schema).unwrap();
        let expect_mask = vec![0, 1, 2, 3];
        let expect_reorder = vec![
            rii!(0),
            ReorderIndex::Child(vec![rii!(0), rii!(1)]),
            rii!(2),
        ];
        assert_eq!(mask_indices, expect_mask);
        assert_eq!(reorder_indices, expect_reorder);
    }

    #[test]
    fn nested_indices_reorder() {
        let kernel_schema = Arc::new(StructType::new(vec![
            StructField::new("j", DataType::INTEGER, false),
            StructField::new(
                "nested",
                StructType::new(vec![
                    StructField::new("string", DataType::STRING, false),
                    StructField::new("int32", DataType::INTEGER, false),
                ]),
                false,
            ),
            StructField::new("i", DataType::INTEGER, false),
        ]));
        let arrow_schema = nested_arrow_schema();
        let (mask_indices, reorder_indices) =
            get_requested_indices(&kernel_schema, &arrow_schema).unwrap();
        let expect_mask = vec![0, 1, 2, 3];
        let expect_reorder = vec![
            rii!(2),
            ReorderIndex::Child(vec![rii!(1), rii!(0)]),
            rii!(0),
        ];
        assert_eq!(mask_indices, expect_mask);
        assert_eq!(reorder_indices, expect_reorder);
    }

    #[test]
    fn nested_indices_mask_inner() {
        let kernel_schema = Arc::new(StructType::new(vec![
            StructField::new("i", DataType::INTEGER, false),
            StructField::new(
                "nested",
                StructType::new(vec![StructField::new("int32", DataType::INTEGER, false)]),
                false,
            ),
            StructField::new("j", DataType::INTEGER, false),
        ]));
        let arrow_schema = nested_arrow_schema();
        let (mask_indices, reorder_indices) =
            get_requested_indices(&kernel_schema, &arrow_schema).unwrap();
        let expect_mask = vec![0, 1, 3];
        let expect_reorder = vec![rii!(0), ReorderIndex::Child(vec![rii!(0)]), rii!(2)];
        assert_eq!(mask_indices, expect_mask);
        assert_eq!(reorder_indices, expect_reorder);
    }

    #[test]
    fn simple_list_mask() {
        let kernel_schema = Arc::new(StructType::new(vec![
            StructField::new("i", DataType::INTEGER, false),
            StructField::new("list", ArrayType::new(DataType::INTEGER, false), false),
            StructField::new("j", DataType::INTEGER, false),
        ]));
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i", ArrowDataType::Int32, false),
            ArrowField::new(
                "list",
                ArrowDataType::List(Arc::new(ArrowField::new(
                    "nested",
                    ArrowDataType::Int32,
                    false,
                ))),
                false,
            ),
            ArrowField::new("j", ArrowDataType::Int32, false),
        ]));
        let (mask_indices, reorder_indices) =
            get_requested_indices(&kernel_schema, &arrow_schema).unwrap();
        let expect_mask = vec![0, 1, 2];
        let expect_reorder = vec![rii!(0), rii!(1), rii!(2)];
        assert_eq!(mask_indices, expect_mask);
        assert_eq!(reorder_indices, expect_reorder);
    }

    #[test]
    fn nested_indices_list() {
        let kernel_schema = Arc::new(StructType::new(vec![
            StructField::new("i", DataType::INTEGER, false),
            StructField::new(
                "list",
                ArrayType::new(
                    StructType::new(vec![
                        StructField::new("int32", DataType::INTEGER, false),
                        StructField::new("string", DataType::STRING, false),
                    ])
                    .into(),
                    false,
                ),
                false,
            ),
            StructField::new("j", DataType::INTEGER, false),
        ]));
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i", ArrowDataType::Int32, false),
            ArrowField::new(
                "list",
                ArrowDataType::List(Arc::new(ArrowField::new(
                    "nested",
                    ArrowDataType::Struct(
                        vec![
                            ArrowField::new("int32", ArrowDataType::Int32, false),
                            ArrowField::new("string", ArrowDataType::Utf8, false),
                        ]
                        .into(),
                    ),
                    false,
                ))),
                false,
            ),
            ArrowField::new("j", ArrowDataType::Int32, false),
        ]));
        let (mask_indices, reorder_indices) =
            get_requested_indices(&kernel_schema, &arrow_schema).unwrap();
        let expect_mask = vec![0, 1, 2, 3];
        let expect_reorder = vec![
            rii!(0),
            ReorderIndex::Child(vec![rii!(0), rii!(1)]),
            rii!(2),
        ];
        assert_eq!(mask_indices, expect_mask);
        assert_eq!(reorder_indices, expect_reorder);
    }

    #[test]
    fn nested_indices_list_mask_inner() {
        let kernel_schema = Arc::new(StructType::new(vec![
            StructField::new("i", DataType::INTEGER, false),
            StructField::new(
                "list",
                ArrayType::new(
                    StructType::new(vec![StructField::new("int32", DataType::INTEGER, false)])
                        .into(),
                    false,
                ),
                false,
            ),
            StructField::new("j", DataType::INTEGER, false),
        ]));
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i", ArrowDataType::Int32, false),
            ArrowField::new(
                "list",
                ArrowDataType::List(Arc::new(ArrowField::new(
                    "nested",
                    ArrowDataType::Struct(
                        vec![
                            ArrowField::new("int32", ArrowDataType::Int32, false),
                            ArrowField::new("string", ArrowDataType::Utf8, false),
                        ]
                        .into(),
                    ),
                    false,
                ))),
                false,
            ),
            ArrowField::new("j", ArrowDataType::Int32, false),
        ]));
        let (mask_indices, reorder_indices) =
            get_requested_indices(&kernel_schema, &arrow_schema).unwrap();
        let expect_mask = vec![0, 1, 3];
        let expect_reorder = vec![rii!(0), ReorderIndex::Child(vec![rii!(0)]), rii!(2)];
        assert_eq!(mask_indices, expect_mask);
        assert_eq!(reorder_indices, expect_reorder);
    }

    #[test]
    fn nested_indices_list_mask_inner_reorder() {
        let kernel_schema = Arc::new(StructType::new(vec![
            StructField::new("i", DataType::INTEGER, false),
            StructField::new(
                "list",
                ArrayType::new(
                    StructType::new(vec![
                        StructField::new("string", DataType::INTEGER, false),
                        StructField::new("int2", DataType::INTEGER, false),
                    ])
                    .into(),
                    false,
                ),
                false,
            ),
            StructField::new("j", DataType::INTEGER, false),
        ]));
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i", ArrowDataType::Int32, false), // field 0
            ArrowField::new(
                "list",
                ArrowDataType::List(Arc::new(ArrowField::new(
                    "nested",
                    ArrowDataType::Struct(
                        vec![
                            ArrowField::new("int1", ArrowDataType::Int32, false), // field 1
                            ArrowField::new("int2", ArrowDataType::Int32, false), // field 2
                            ArrowField::new("string", ArrowDataType::Utf8, false), // field 3
                        ]
                        .into(),
                    ),
                    false,
                ))),
                false,
            ),
            ArrowField::new("j", ArrowDataType::Int32, false), // field 4
        ]));
        let (mask_indices, reorder_indices) =
            get_requested_indices(&kernel_schema, &arrow_schema).unwrap();
        let expect_mask = vec![0, 2, 3, 4];
        let expect_reorder = vec![
            rii!(0),
            ReorderIndex::Child(vec![rii!(1), rii!(0)]),
            rii!(2),
        ];
        assert_eq!(mask_indices, expect_mask);
        assert_eq!(reorder_indices, expect_reorder);
    }
}

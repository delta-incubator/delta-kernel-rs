//! Some utilities for working with arrow data types

use std::{collections::HashSet, sync::Arc};

use crate::{
    schema::{DataType, Schema, SchemaRef, StructField, StructType},
    DeltaResult, Error,
};

use arrow_array::{cast::AsArray, new_null_array, Array as ArrowArray, StructArray};
use arrow_schema::{
    DataType as ArrowDataType, Field as ArrowField, FieldRef as ArrowFieldRef, Fields,
    SchemaRef as ArrowSchemaRef,
};
use parquet::{arrow::ProjectionMask, schema::types::SchemaDescriptor};
use tracing::debug;

/// Reordering is specified as a tree. Each level is a vec of `ReorderIndex`s. Each element's index
/// represents a column that will be in the read parquet data at that level and index. The `index()`
/// of the element is the position that the column should appear in the final output. If it is a
/// `Child` variant, then at that index there is a `Struct` whose ordering is specified by the
/// values in the associated `Vec` according to these same rules.
#[derive(Debug, PartialEq)]
pub(crate) enum ReorderIndex {
    Child {
        index: usize,
        children: Vec<ReorderIndex>,
    },
    Index {
        index: usize,
    },
    Null {
        index: usize,
        field: ArrowFieldRef,
    },
}

impl ReorderIndex {
    fn index(&self) -> usize {
        match self {
            ReorderIndex::Child { index, .. } => *index,
            ReorderIndex::Index { index, .. } => *index,
            ReorderIndex::Null { index, .. } => *index,
        }
    }

    /// check if this indexing is ordered. an `Index` variant is ordered by definition. a `Null`
    /// variant is not because if we have a `Null` variant we need to do work in
    /// reorder_struct_array to insert the null col
    fn is_ordered(&self) -> bool {
        match self {
            ReorderIndex::Child { ref children, .. } => is_ordered(children),
            ReorderIndex::Index { .. } => true,
            ReorderIndex::Null { .. } => false,
        }
    }
}

// count the number of physical columns, including nested ones in an `ArrowField`
fn count_cols(field: &ArrowField) -> usize {
    _count_cols(field.data_type())
}

fn _count_cols(dt: &ArrowDataType) -> usize {
    match dt {
        ArrowDataType::Struct(fields) => fields.iter().fold(0, |acc, f| acc + count_cols(f)),
        ArrowDataType::Union(fields, _) => fields.iter().fold(0, |acc, (_, f)| acc + count_cols(f)),
        ArrowDataType::List(field)
        | ArrowDataType::LargeList(field)
        | ArrowDataType::FixedSizeList(field, _)
        | ArrowDataType::Map(field, _) => count_cols(field),
        ArrowDataType::Dictionary(_, value_field) => _count_cols(value_field.as_ref()),
        _ => 1, // other types are "real" fields, so count
    }
}

/// helper function, does the same as `get_requested_indices` but at an offset. used to recurse into
/// structs, lists, and maps. `parquet_offset` is how many parquet fields exist before processing
/// this potentially nested schema. returns the number of parquet fields in `fields` (regardless of
/// if they are selected or not) and reordering information for the requested fields.
fn get_indices(
    start_parquet_offset: usize,
    requested_schema: &Schema,
    fields: &Fields,
    mask_indices: &mut Vec<usize>,
) -> DeltaResult<(usize, Vec<ReorderIndex>)> {
    let mut found_fields = HashSet::with_capacity(requested_schema.fields.len());
    let mut reorder_indices = Vec::with_capacity(requested_schema.fields.len());
    let mut parquet_offset = start_parquet_offset;
    for (parquet_index, field) in fields.iter().enumerate() {
        debug!(
            "Getting indices for field {} with offset {parquet_offset}, with index {parquet_index}",
            field.name()
        );
        match field.data_type() {
            ArrowDataType::Struct(fields) => {
                if let Some((index, _, requested_field)) =
                    requested_schema.fields.get_full(field.name())
                {
                    match requested_field.data_type {
                        DataType::Struct(ref requested_schema) => {
                            let (parquet_advance, children) = get_indices(
                                parquet_index + parquet_offset,
                                requested_schema.as_ref(),
                                fields,
                                mask_indices,
                            )?;
                            // advance the number of parquet fields, but subtract 1 because the
                            // struct will be counted by the `enumerate` call but doesn't count as
                            // an actual index.
                            parquet_offset += parquet_advance - 1;
                            // note that we found this field
                            found_fields.insert(requested_field.name());
                            // push the child reorder on
                            reorder_indices.push(ReorderIndex::Child { index, children });
                        }
                        _ => {
                            return Err(Error::unexpected_column_type(field.name()));
                        }
                    }
                } else {
                    // We're NOT selecting this field, but we still need to update how much we skip
                    debug!("Skipping over un-selected struct: {}", field.name());
                    // offset by number of inner fields. subtract one, because the enumerate still
                    // counts this field
                    parquet_offset += count_cols(field) - 1;
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
                            let (parquet_advance, children) = get_indices(
                                found_fields.len() + parquet_offset,
                                &requested_schema,
                                &[list_field.clone()].into(),
                                mask_indices,
                            )?;
                            // see comment above in struct match arm
                            parquet_offset += parquet_advance - 1;
                            found_fields.insert(requested_field.name());
                            // we have to recurse to find the type, but for reordering a list we
                            // only need a child reordering if the inner type is a struct
                            if let ArrowDataType::Struct(_) = list_field.data_type() {
                                if children.len() != 1 {
                                    return Err(
                                        Error::generic(
                                            "List call should not have generated more than one reorder index"
                                        )
                                    );
                                }
                                // safety, checked that we have 1 element
                                let mut children = children.into_iter().next().unwrap();
                                // the index is wrong though, as it's the index from the inner
                                // schema. Adjust it to be our index
                                if let ReorderIndex::Child {
                                    index: ref mut child_index,
                                    ..
                                } = children
                                {
                                    *child_index = index;
                                } else {
                                    return Err(
                                        Error::generic(
                                            "List call should have returned a ReorderIndex::Child variant"
                                        )
                                    );
                                }
                                reorder_indices.push(children);
                            } else {
                                reorder_indices.push(ReorderIndex::Index { index });
                            }
                        }
                        _ => {
                            return Err(Error::unexpected_column_type(list_field.name()));
                        }
                    }
                }
            }
            ArrowDataType::Map(key_val_field, _) => {
                if let Some((index, _, requested_field)) =
                    requested_schema.fields.get_full(field.name())
                {
                    match (key_val_field.data_type(), requested_field.data_type()) {
                        (ArrowDataType::Struct(inner_fields), DataType::Map(map_type)) => {
                            let mut key_val_names =
                                inner_fields.iter().map(|f| f.name().to_string());
                            let key_name = if let Some(key_name) = key_val_names.next() {
                                key_name
                            } else {
                                return Err(Error::generic("map fields didn't include a key col"));
                            };
                            let val_name = if let Some(val_name) = key_val_names.next() {
                                val_name
                            } else {
                                return Err(Error::generic("map fields didn't include a val col"));
                            };
                            if key_val_names.next().is_some() {
                                return Err(Error::generic("map fields had more than 2 members"));
                            }
                            let inner_schema = map_type.as_struct_schema(key_name, val_name);
                            let (parquet_advance, _children) = get_indices(
                                parquet_index + parquet_offset,
                                &inner_schema,
                                inner_fields,
                                mask_indices,
                            )?;
                            // advance the number of parquet fields, but subtract 1 because the
                            // map will be counted by the `enumerate` call but doesn't count as
                            // an actual index.
                            parquet_offset += parquet_advance - 1;
                            // note that we found this field
                            found_fields.insert(requested_field.name());
                            // push the child reorder on, currently no reordering for maps
                            reorder_indices.push(ReorderIndex::Index { index });
                        }
                        _ => {
                            return Err(Error::unexpected_column_type(field.name()));
                        }
                    }
                }
            }
            _ => {
                if let Some((index, _, requested_field)) =
                    requested_schema.fields.get_full(field.name())
                {
                    found_fields.insert(requested_field.name());
                    mask_indices.push(parquet_offset + parquet_index);
                    reorder_indices.push(ReorderIndex::Index { index });
                }
            }
        }
    }
    if found_fields.len() != requested_schema.fields.len() {
        // some fields are missing, but they might be nullable, need to insert them into the reorder_indices
        for (requested_position, field) in requested_schema.fields().enumerate() {
            if !found_fields.contains(field.name()) {
                if field.nullable {
                    debug!("Inserting missing and nullable field: {}", field.name());
                    reorder_indices.push(ReorderIndex::Null {
                        index: requested_position,
                        field: Arc::new(field.try_into()?),
                    });
                } else {
                    return Err(Error::Generic(format!(
                        "Requested field not found in parquet schema, and field is not nullable: {}",
                        field.name()
                    )));
                }
            }
        }
    }
    Ok((
        parquet_offset + fields.len() - start_parquet_offset,
        reorder_indices,
    ))
}

/// Get the indices in `parquet_schema` of the specified columns in `requested_schema`. This returns
/// a tuples of (mask_indices: Vec<parquet_schema_index>, reorder_indices:
/// Vec<requested_index>). `mask_indices` is used for generating the mask for reading from the
/// parquet file, and simply contains an entry for each index we wish to select from the parquet
/// file set to the index of the requested column in the parquet. `reorder_indices` is used for
/// re-ordering. See the documentation for [`ReorderIndex`] to understand what each element in the
/// returned array means
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
    Ok((mask_indices, reorder_indexes))
}

/// Create a mask that will only select the specified indices from the parquet. Currently we only
/// handle "root" level columns, and hence use `ProjectionMask::roots`, but will support leaf
/// selection in the future. See issues #86 and #96 as well.
pub(crate) fn generate_mask(
    _requested_schema: &SchemaRef,
    _parquet_schema: &ArrowSchemaRef,
    parquet_physical_schema: &SchemaDescriptor,
    indices: &[usize],
) -> Option<ProjectionMask> {
    // TODO: Determine if it's worth checking if we're selecting everything and returning None in
    // that case
    Some(ProjectionMask::leaves(
        parquet_physical_schema,
        indices.to_owned(),
    ))
}

/// Check if an ordering is already ordered
fn is_ordered(requested_ordering: &[ReorderIndex]) -> bool {
    if requested_ordering.is_empty() {
        return true;
    }
    // we have >=1 element. check that the first element is ordered
    if !requested_ordering[0].is_ordered() {
        return false;
    }
    // now check that all elements are ordered wrt. each other, and are internally ordered
    requested_ordering
        .windows(2)
        .all(|ri| (ri[0].index() < ri[1].index()) && ri[1].is_ordered())
}

// we use this as a placeholder for an array and its associated field. We can fill in a Vec of None
// of this type and then set elements of the Vec to Some(FieldArrayOpt) for each column
type FieldArrayOpt = Option<(Arc<ArrowField>, Arc<dyn ArrowArray>)>;

/// Reorder a RecordBatch to match `requested_ordering`. For each non-zero value in
/// `requested_ordering`, the column at that index will be added in order to returned batch
pub(crate) fn reorder_struct_array(
    input_data: StructArray,
    requested_ordering: &[ReorderIndex],
) -> DeltaResult<StructArray> {
    if is_ordered(requested_ordering) {
        // indices is already sorted, meaning we requested in the order that the columns were
        // stored in the parquet
        Ok(input_data)
    } else {
        // requested an order different from the parquet, reorder
        debug!("Have requested reorder {requested_ordering:#?} on {input_data:?}");
        let num_rows = input_data.len();
        let num_cols = requested_ordering.len();
        let (input_fields, input_cols, null_buffer) = input_data.into_parts();
        let mut final_fields_cols: Vec<FieldArrayOpt> = vec![None; num_cols];
        for (parquet_position, reorder_index) in requested_ordering.iter().enumerate() {
            // for each item, reorder_index.index() tells us where to put it, and its position in
            // requested_ordering tells us where it is in the parquet data
            match reorder_index {
                ReorderIndex::Child { index, children } => {
                    let struct_array = input_cols[parquet_position].as_struct().clone();
                    let result_array = reorder_struct_array(struct_array, children)?;
                    // create the new field specifying the correct order for the struct
                    let new_field = Arc::new(ArrowField::new_struct(
                        input_fields[parquet_position].name(),
                        result_array.fields().clone(),
                        input_fields[parquet_position].is_nullable(),
                    ));
                    final_fields_cols[*index] = Some((new_field, Arc::new(result_array)));
                }
                ReorderIndex::Index { index } => {
                    final_fields_cols[*index] = Some((
                        input_fields[parquet_position].clone(), // cheap Arc clone
                        input_cols[parquet_position].clone(),   // cheap Arc clone
                    ));
                }
                ReorderIndex::Null { index, field } => {
                    let null_arry = Arc::new(new_null_array(field.data_type(), num_rows));
                    let field = field.clone(); // cheap Arc clone
                    final_fields_cols[*index] = Some((field, null_arry));
                }
            }
        }
        let mut field_vec = Vec::with_capacity(num_cols);
        let mut reordered_columns = Vec::with_capacity(num_cols);
        for field_array_opt in final_fields_cols.into_iter() {
            let (field, array) = field_array_opt.ok_or_else(|| {
                Error::generic(
                    "Found a None in final_fields_cols. This is a kernel bug, please report.",
                )
            })?;
            field_vec.push(field);
            reordered_columns.push(array);
        }
        Ok(StructArray::try_new(
            field_vec.into(),
            reordered_columns,
            null_buffer,
        )?)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::AsArray;
    use arrow_array::{ArrayRef as ArrowArrayRef, BooleanArray, Int32Array, StructArray};
    use arrow_schema::{
        DataType as ArrowDataType, Field as ArrowField, Fields, Schema as ArrowSchema,
        SchemaRef as ArrowSchemaRef,
    };

    use crate::schema::{ArrayType, DataType, StructField, StructType};

    use super::{get_requested_indices, reorder_struct_array, ReorderIndex};

    macro_rules! rii {
        ($index: expr) => {{
            ReorderIndex::Index { index: $index }
        }};
    }

    fn nested_arrow_schema() -> ArrowSchemaRef {
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
    fn simple_nullable_field_missing() {
        let kernel_schema = Arc::new(StructType::new(vec![
            StructField::new("i", DataType::INTEGER, false),
            StructField::new("s", DataType::STRING, true),
            StructField::new("i2", DataType::INTEGER, true),
        ]));
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i", ArrowDataType::Int32, false),
            ArrowField::new("i2", ArrowDataType::Int32, true),
        ]));
        let (mask_indices, reorder_indices) =
            get_requested_indices(&kernel_schema, &arrow_schema).unwrap();
        let expect_mask = vec![0, 1];
        let expect_reorder = vec![
            rii!(0),
            rii!(2),
            ReorderIndex::Null {
                index: 1,
                field: Arc::new(ArrowField::new("s", ArrowDataType::Utf8, true)),
            },
        ];
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
            ReorderIndex::Child {
                index: 1,
                children: vec![rii!(0), rii!(1)],
            },
            rii!(2),
        ];
        assert_eq!(mask_indices, expect_mask);
        assert_eq!(reorder_indices, expect_reorder);
    }

    #[test]
    fn nested_indices_reorder() {
        let kernel_schema = Arc::new(StructType::new(vec![
            StructField::new(
                "nested",
                StructType::new(vec![
                    StructField::new("string", DataType::STRING, false),
                    StructField::new("int32", DataType::INTEGER, false),
                ]),
                false,
            ),
            StructField::new("j", DataType::INTEGER, false),
            StructField::new("i", DataType::INTEGER, false),
        ]));
        let arrow_schema = nested_arrow_schema();
        let (mask_indices, reorder_indices) =
            get_requested_indices(&kernel_schema, &arrow_schema).unwrap();
        let expect_mask = vec![0, 1, 2, 3];
        let expect_reorder = vec![
            rii!(2),
            ReorderIndex::Child {
                index: 0,
                children: vec![rii!(1), rii!(0)],
            },
            rii!(1),
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
        let expect_reorder = vec![
            rii!(0),
            ReorderIndex::Child {
                index: 1,
                children: vec![rii!(0)],
            },
            rii!(2),
        ];
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
            ReorderIndex::Child {
                index: 1,
                children: vec![rii!(0), rii!(1)],
            },
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
        let expect_reorder = vec![
            rii!(0),
            ReorderIndex::Child {
                index: 1,
                children: vec![rii!(0)],
            },
            rii!(2),
        ];
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
            ReorderIndex::Child {
                index: 1,
                children: vec![rii!(1), rii!(0)],
            },
            rii!(2),
        ];
        assert_eq!(mask_indices, expect_mask);
        assert_eq!(reorder_indices, expect_reorder);
    }

    #[test]
    fn skipped_struct() {
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
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(
                "skipped",
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
            ArrowField::new("i", ArrowDataType::Int32, false),
        ]));
        let (mask_indices, reorder_indices) =
            get_requested_indices(&kernel_schema, &arrow_schema).unwrap();
        let expect_mask = vec![2, 3, 4, 5];
        let expect_reorder = vec![
            rii!(2),
            ReorderIndex::Child {
                index: 1,
                children: vec![rii!(0), rii!(1)],
            },
            rii!(0),
        ];
        assert_eq!(mask_indices, expect_mask);
        assert_eq!(reorder_indices, expect_reorder);
    }

    fn make_struct_array() -> StructArray {
        let boolean = Arc::new(BooleanArray::from(vec![false, false, true, true]));
        let int = Arc::new(Int32Array::from(vec![42, 28, 19, 31]));
        StructArray::from(vec![
            (
                Arc::new(ArrowField::new("b", ArrowDataType::Boolean, false)),
                boolean.clone() as ArrowArrayRef,
            ),
            (
                Arc::new(ArrowField::new("c", ArrowDataType::Int32, false)),
                int.clone() as ArrowArrayRef,
            ),
        ])
    }

    #[test]
    fn simple_reorder_struct() {
        let arry = make_struct_array();
        let reorder = vec![rii!(1), rii!(0)];
        let ordered = reorder_struct_array(arry, &reorder).unwrap();
        assert_eq!(ordered.column_names(), vec!["c", "b"]);
    }

    #[test]
    fn nested_reorder_struct() {
        let arry1 = Arc::new(make_struct_array());
        let arry2 = Arc::new(make_struct_array());
        let fields: Fields = vec![
            Arc::new(ArrowField::new("b", ArrowDataType::Boolean, false)),
            Arc::new(ArrowField::new("c", ArrowDataType::Int32, false)),
        ]
        .into();
        let nested = StructArray::from(vec![
            (
                Arc::new(ArrowField::new(
                    "struct1",
                    ArrowDataType::Struct(fields.clone()),
                    false,
                )),
                arry1 as ArrowArrayRef,
            ),
            (
                Arc::new(ArrowField::new(
                    "struct2",
                    ArrowDataType::Struct(fields),
                    false,
                )),
                arry2 as ArrowArrayRef,
            ),
        ]);
        let reorder = vec![
            ReorderIndex::Child {
                index: 1,
                children: vec![rii!(1), rii!(0)],
            },
            ReorderIndex::Child {
                index: 0,
                children: vec![
                    rii!(0),
                    rii!(1),
                    ReorderIndex::Null {
                        index: 2,
                        field: Arc::new(ArrowField::new("s", ArrowDataType::Utf8, true)),
                    },
                ],
            },
        ];
        let ordered = reorder_struct_array(nested, &reorder).unwrap();
        assert_eq!(ordered.column_names(), vec!["struct2", "struct1"]);
        let ordered_s2 = ordered.column(0).as_struct();
        assert_eq!(ordered_s2.column_names(), vec!["b", "c", "s"]);
        let ordered_s1 = ordered.column(1).as_struct();
        assert_eq!(ordered_s1.column_names(), vec!["c", "b"]);
    }
}

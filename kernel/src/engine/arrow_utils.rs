//! Some utilities for working with arrow data types

use std::{collections::HashSet, sync::Arc};

use crate::{
    schema::{DataType, PrimitiveType, Schema, SchemaRef, StructField, StructType},
    utils::require,
    DeltaResult, Error,
};

use arrow_array::{
    cast::AsArray, new_null_array, Array as ArrowArray, GenericListArray, OffsetSizeTrait,
    StructArray,
};
use arrow_schema::{
    DataType as ArrowDataType, Field as ArrowField, FieldRef as ArrowFieldRef, Fields,
    SchemaRef as ArrowSchemaRef,
};
use itertools::Itertools;
use parquet::{arrow::ProjectionMask, schema::types::SchemaDescriptor};
use tracing::debug;

fn make_arrow_error(s: String) -> Error {
    Error::Arrow(arrow_schema::ArrowError::InvalidArgumentError(s))
}

/// Ensure a kernel data type matches an arrow data type. This only ensures that the actual "type"
/// is the same, but does so recursively into structs, and ensures lists and maps have the correct
/// associated types as well. This returns an `Ok(())` if the types are compatible, or an error if
/// the types do not match. If there is a `struct` type included, we only ensure that the named
/// fields that the kernel is asking for exist, and that for those fields the types
/// match. Un-selected fields are ignored.
pub(crate) fn ensure_data_types(
    kernel_type: &DataType,
    arrow_type: &ArrowDataType,
) -> DeltaResult<()> {
    match (kernel_type, arrow_type) {
        (DataType::Primitive(_), _) if arrow_type.is_primitive() => {
            let converted_type: ArrowDataType = kernel_type.try_into()?;
            if &converted_type == arrow_type {
                Ok(())
            } else {
                Err(make_arrow_error(format!(
                    "Incorrect datatype. Expected {}, got {}",
                    converted_type, arrow_type
                )))
            }
        }
        (DataType::Primitive(PrimitiveType::Boolean), ArrowDataType::Boolean)
        | (DataType::Primitive(PrimitiveType::String), ArrowDataType::Utf8)
        | (DataType::Primitive(PrimitiveType::Binary), ArrowDataType::Binary) => {
            // strings, bools, and binary  aren't primitive in arrow
            Ok(())
        }
        (
            DataType::Primitive(PrimitiveType::Decimal(kernel_prec, kernel_scale)),
            ArrowDataType::Decimal128(arrow_prec, arrow_scale),
        ) if arrow_prec == kernel_prec && *arrow_scale == *kernel_scale as i8 => {
            // decimal isn't primitive in arrow. cast above is okay as we limit range
            Ok(())
        }
        (DataType::Array(inner_type), ArrowDataType::List(arrow_list_type)) => {
            let kernel_array_type = &inner_type.element_type;
            let arrow_list_type = arrow_list_type.data_type();
            ensure_data_types(kernel_array_type, arrow_list_type)
        }
        (DataType::Map(kernel_map_type), ArrowDataType::Map(arrow_map_type, _)) => {
            if let ArrowDataType::Struct(fields) = arrow_map_type.data_type() {
                let mut fiter = fields.iter();
                if let Some(key_type) = fiter.next() {
                    ensure_data_types(&kernel_map_type.key_type, key_type.data_type())?;
                } else {
                    return Err(make_arrow_error(
                        "Arrow map struct didn't have a key type".to_string(),
                    ));
                }
                if let Some(value_type) = fiter.next() {
                    ensure_data_types(&kernel_map_type.value_type, value_type.data_type())?;
                } else {
                    return Err(make_arrow_error(
                        "Arrow map struct didn't have a value type".to_string(),
                    ));
                }
                Ok(())
            } else {
                Err(make_arrow_error(
                    "Arrow map type wasn't a struct.".to_string(),
                ))
            }
        }
        (DataType::Struct(kernel_fields), ArrowDataType::Struct(arrow_fields)) => {
            // build a list of kernel fields that matches the order of the arrow fields
            let mapped_fields = arrow_fields
                .iter()
                .flat_map(|f| kernel_fields.fields.get(f.name()));

            // keep track of how many fields we matched up
            let mut found_fields = 0;
            // ensure that for the fields that we found, the types match
            for (kernel_field, arrow_field) in mapped_fields.zip(arrow_fields) {
                ensure_data_types(&kernel_field.data_type, arrow_field.data_type())?;
                found_fields += 1;
            }

            // require that we found the number of fields that we requested.
            require!(kernel_fields.fields.len() == found_fields, {
                let kernel_field_names = kernel_fields.fields.keys().join(", ");
                let arrow_field_names = arrow_fields.iter().map(|f| f.name()).join(", ");
                make_arrow_error(format!(
                    "Missing Struct fields. Requested: {}, found: {}",
                    kernel_field_names, arrow_field_names,
                ))
            });
            Ok(())
        }
        _ => Err(make_arrow_error(format!(
            "Incorrect datatype. Expected {}, got {}",
            kernel_type, arrow_type
        ))),
    }
}

/// Reordering is specified as a tree. Each level is a vec of `ReorderIndex`s. Each element's index
/// represents a column that will be in the read parquet data at that level and index. The `index()`
/// of the element is the position that the column should appear in the final output. If it is a
/// `Child` variant, then at that index there is a `Struct` whose ordering is specified by the
/// values in the associated `Vec` according to these same rules.
#[derive(Debug, PartialEq)]
pub(crate) struct ReorderIndex {
    pub(crate) index: usize,
    kind: ReorderIndexKind,
}

#[derive(Debug, PartialEq)]
pub(crate) enum ReorderIndexKind {
    Child(Vec<ReorderIndex>),
    Index,
    Missing(ArrowFieldRef),
}

impl ReorderIndex {
    fn new_child(index: usize, children: Vec<ReorderIndex>) -> Self {
        ReorderIndex {
            index,
            kind: ReorderIndexKind::Child(children),
        }
    }

    fn new_index(index: usize) -> Self {
        ReorderIndex {
            index,
            kind: ReorderIndexKind::Index,
        }
    }

    fn new_missing(index: usize, field: ArrowFieldRef) -> Self {
        ReorderIndex {
            index,
            kind: ReorderIndexKind::Missing(field),
        }
    }

    /// Check if this reordering contains a `Missing` variant anywhere. See comment below on
    /// [`is_ordered`] to understand why this is needed.
    fn contains_missing(&self) -> bool {
        match self.kind {
            ReorderIndexKind::Child(ref children) => is_ordered(children),
            ReorderIndexKind::Index => true,
            ReorderIndexKind::Missing(_) => false,
        }
    }
}

// count the number of physical columns, including nested ones in an `ArrowField`
fn count_cols(field: &ArrowField) -> usize {
    _count_cols(field.data_type())
}

fn _count_cols(dt: &ArrowDataType) -> usize {
    match dt {
        ArrowDataType::Struct(fields) => fields.iter().map(|f| count_cols(f)).sum(),
        ArrowDataType::Union(fields, _) => fields.iter().map(|(_, f)| count_cols(f)).sum(),
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
                    if let DataType::Struct(ref requested_schema) = requested_field.data_type {
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
                        reorder_indices.push(ReorderIndex::new_child(index, children));
                    } else {
                        return Err(Error::unexpected_column_type(field.name()));
                    }
                } else {
                    // We're NOT selecting this field, but we still need to update how much we skip
                    debug!("Skipping over un-selected struct: {}", field.name());
                    // offset by number of inner fields. subtract one, because the enumerate still
                    // counts this field
                    parquet_offset += count_cols(field) - 1;
                }
            }
            ArrowDataType::List(list_field)
            | ArrowDataType::LargeList(list_field)
            | ArrowDataType::ListView(list_field) => {
                if let Some((index, _, requested_field)) =
                    requested_schema.fields.get_full(field.name())
                {
                    // we just want to transparently recurse into lists, need to transform the kernel
                    // list data type into a schema
                    if let DataType::Array(array_type) = requested_field.data_type() {
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
                        if children.len() != 1 {
                            return Err(Error::generic(
                                "List call should not have generated more than one reorder index",
                            ));
                        }
                        // safety, checked that we have 1 element
                        let mut children = children.into_iter().next().unwrap();
                        // the index is wrong, as it's the index from the inner schema. Adjust
                        // it to be our index
                        children.index = index;
                        reorder_indices.push(children);
                    } else {
                        return Err(Error::unexpected_column_type(list_field.name()));
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
                            let key_name = key_val_names.next().ok_or_else(|| {
                                Error::generic("map fields didn't include a key col")
                            })?;
                            let val_name = key_val_names.next().ok_or_else(|| {
                                Error::generic("map fields didn't include a val col")
                            })?;
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
                            reorder_indices.push(ReorderIndex::new_index(index));
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
                    ensure_data_types(&requested_field.data_type, field.data_type())?;
                    found_fields.insert(requested_field.name());
                    mask_indices.push(parquet_offset + parquet_index);
                    reorder_indices.push(ReorderIndex::new_index(index));
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
                    reorder_indices.push(ReorderIndex::new_missing(
                        requested_position,
                        Arc::new(field.try_into()?),
                    ));
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
/// a tuple of (mask_indices: Vec<parquet_schema_index>, reorder_indices:
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

/// Create a mask that will only select the specified indices from the parquet. `indices` can be
/// computed from a [`Schema`] using [`get_requested_indices`]
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

/// Check if an ordering is already ordered. We check if the indices are in ascending order. That's
/// enough to ensure we don't need to do any transformation on the data read from parquet _iff_
/// there are no `null` columns to insert. If we _do_ need to insert a null column then we need to
/// transform the data. Therefore we also call [`contains_missing`] to ensure both the ascending
/// nature of the indices AND that no `Missing` variants exist, and only if both are true do we
/// consider an ordering "ordered".
fn is_ordered(requested_ordering: &[ReorderIndex]) -> bool {
    if requested_ordering.is_empty() {
        return true;
    }
    // we have >=1 element. check that the first element is ordered
    if !requested_ordering[0].contains_missing() {
        return false;
    }
    // now check that all elements are ordered wrt. each other, and are internally ordered
    requested_ordering
        .windows(2)
        .all(|ri| (ri[0].index < ri[1].index) && ri[1].contains_missing())
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
            match &reorder_index.kind {
                ReorderIndexKind::Child(children) => {
                    match input_cols[parquet_position].data_type() {
                        ArrowDataType::Struct(_) => {
                            let struct_array = input_cols[parquet_position].as_struct().clone();
                            let result_array = reorder_struct_array(struct_array, children)?;
                            // create the new field specifying the correct order for the struct
                            let new_field = Arc::new(ArrowField::new_struct(
                                input_fields[parquet_position].name(),
                                result_array.fields().clone(),
                                input_fields[parquet_position].is_nullable(),
                            ));
                            final_fields_cols[reorder_index.index] =
                                Some((new_field, Arc::new(result_array)));
                        }
                        ArrowDataType::List(_) => {
                            let list_array = input_cols[parquet_position].as_list::<i32>().clone();
                            final_fields_cols[reorder_index.index] = reorder_list(
                                list_array,
                                input_fields[parquet_position].name(),
                                children,
                            )?;
                        }
                        ArrowDataType::LargeList(_) => {
                            let list_array = input_cols[parquet_position].as_list::<i64>().clone();
                            final_fields_cols[reorder_index.index] = reorder_list(
                                list_array,
                                input_fields[parquet_position].name(),
                                children,
                            )?;
                        }
                        // TODO: MAP
                        _ => {
                            return Err(Error::generic(
                            "Child reorder can only apply to struct/list/map. This is a kernel bug, please report"
                        ));
                        }
                    }
                }
                ReorderIndexKind::Index => {
                    final_fields_cols[reorder_index.index] = Some((
                        input_fields[parquet_position].clone(), // cheap Arc clone
                        input_cols[parquet_position].clone(),   // cheap Arc clone
                    ));
                }
                ReorderIndexKind::Missing(field) => {
                    let null_array = Arc::new(new_null_array(field.data_type(), num_rows));
                    let field = field.clone(); // cheap Arc clone
                    final_fields_cols[reorder_index.index] = Some((field, null_array));
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

fn reorder_list<O: OffsetSizeTrait>(
    list_array: GenericListArray<O>,
    input_field_name: &str,
    children: &[ReorderIndex],
) -> DeltaResult<FieldArrayOpt> {
    let (list_field, offset_buffer, maybe_sa, null_buf) = list_array.into_parts();
    if let Some(struct_array) = maybe_sa.as_struct_opt() {
        let struct_array = struct_array.clone();
        let result_array = Arc::new(reorder_struct_array(struct_array, children)?);
        let new_list_field = Arc::new(ArrowField::new_struct(
            list_field.name(),
            result_array.fields().clone(),
            result_array.is_nullable(),
        ));
        let new_field = Arc::new(ArrowField::new_list(
            input_field_name,
            new_list_field.clone(),
            list_field.is_nullable(),
        ));
        let list = Arc::new(GenericListArray::try_new(
            new_list_field,
            offset_buffer,
            result_array,
            null_buf,
        )?);
        Ok(Some((new_field, list)))
    } else {
        Err(Error::generic(
            "Child reorder of list should have had struct child. This is a kernel bug, please report"
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::AsArray,
        buffer::{OffsetBuffer, ScalarBuffer},
    };
    use arrow_array::{
        Array, ArrayRef as ArrowArrayRef, BooleanArray, GenericListArray, Int32Array, StructArray,
    };
    use arrow_schema::{
        DataType as ArrowDataType, Field as ArrowField, Fields, Schema as ArrowSchema,
        SchemaRef as ArrowSchemaRef,
    };

    use crate::schema::{ArrayType, DataType, MapType, StructField, StructType};

    use super::{get_requested_indices, reorder_struct_array, ReorderIndex};

    fn nested_parquet_schema() -> ArrowSchemaRef {
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
        let requested_schema = Arc::new(StructType::new(vec![
            StructField::new("i", DataType::INTEGER, false),
            StructField::new("s", DataType::STRING, true),
            StructField::new("i2", DataType::INTEGER, true),
        ]));
        let parquet_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i", ArrowDataType::Int32, false),
            ArrowField::new("s", ArrowDataType::Utf8, true),
            ArrowField::new("i2", ArrowDataType::Int32, true),
        ]));
        let (mask_indices, reorder_indices) =
            get_requested_indices(&requested_schema, &parquet_schema).unwrap();
        let expect_mask = vec![0, 1, 2];
        let expect_reorder = vec![
            ReorderIndex::new_index(0),
            ReorderIndex::new_index(1),
            ReorderIndex::new_index(2),
        ];
        assert_eq!(mask_indices, expect_mask);
        assert_eq!(reorder_indices, expect_reorder);
    }

    #[test]
    fn ensure_data_types_fails_correctly() {
        let requested_schema = Arc::new(StructType::new(vec![
            StructField::new("i", DataType::INTEGER, false),
            StructField::new("s", DataType::INTEGER, true),
        ]));
        let parquet_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i", ArrowDataType::Int32, false),
            ArrowField::new("s", ArrowDataType::Utf8, true),
        ]));
        let res = get_requested_indices(&requested_schema, &parquet_schema);
        assert!(res.is_err());

        let requested_schema = Arc::new(StructType::new(vec![
            StructField::new("i", DataType::INTEGER, false),
            StructField::new("s", DataType::STRING, true),
        ]));
        let parquet_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i", ArrowDataType::Int32, false),
            ArrowField::new("s", ArrowDataType::Int32, true),
        ]));
        let res = get_requested_indices(&requested_schema, &parquet_schema);
        println!("{res:#?}");
        assert!(res.is_err());
    }

    #[test]
    fn mask_with_map() {
        let requested_schema = Arc::new(StructType::new(vec![StructField::new(
            "map",
            DataType::Map(Box::new(MapType::new(
                DataType::INTEGER,
                DataType::STRING,
                false,
            ))),
            false,
        )]));
        let parquet_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new_map(
            "map",
            "entries",
            ArrowField::new("i", ArrowDataType::Int32, false),
            ArrowField::new("s", ArrowDataType::Utf8, false),
            false,
            false,
        )]));
        let (mask_indices, reorder_indices) =
            get_requested_indices(&requested_schema, &parquet_schema).unwrap();
        let expect_mask = vec![0, 1];
        let expect_reorder = vec![ReorderIndex::new_index(0)];
        assert_eq!(mask_indices, expect_mask);
        assert_eq!(reorder_indices, expect_reorder);
    }

    #[test]
    fn simple_reorder_indices() {
        let requested_schema = Arc::new(StructType::new(vec![
            StructField::new("i", DataType::INTEGER, false),
            StructField::new("s", DataType::STRING, true),
            StructField::new("i2", DataType::INTEGER, true),
        ]));
        let parquet_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i2", ArrowDataType::Int32, true),
            ArrowField::new("i", ArrowDataType::Int32, false),
            ArrowField::new("s", ArrowDataType::Utf8, true),
        ]));
        let (mask_indices, reorder_indices) =
            get_requested_indices(&requested_schema, &parquet_schema).unwrap();
        let expect_mask = vec![0, 1, 2];
        let expect_reorder = vec![
            ReorderIndex::new_index(2),
            ReorderIndex::new_index(0),
            ReorderIndex::new_index(1),
        ];
        assert_eq!(mask_indices, expect_mask);
        assert_eq!(reorder_indices, expect_reorder);
    }

    #[test]
    fn simple_nullable_field_missing() {
        let requested_schema = Arc::new(StructType::new(vec![
            StructField::new("i", DataType::INTEGER, false),
            StructField::new("s", DataType::STRING, true),
            StructField::new("i2", DataType::INTEGER, true),
        ]));
        let parquet_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i", ArrowDataType::Int32, false),
            ArrowField::new("i2", ArrowDataType::Int32, true),
        ]));
        let (mask_indices, reorder_indices) =
            get_requested_indices(&requested_schema, &parquet_schema).unwrap();
        let expect_mask = vec![0, 1];
        let expect_reorder = vec![
            ReorderIndex::new_index(0),
            ReorderIndex::new_index(2),
            ReorderIndex::new_missing(1, Arc::new(ArrowField::new("s", ArrowDataType::Utf8, true))),
        ];
        assert_eq!(mask_indices, expect_mask);
        assert_eq!(reorder_indices, expect_reorder);
    }

    #[test]
    fn nested_indices() {
        let requested_schema = Arc::new(StructType::new(vec![
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
        let parquet_schema = nested_parquet_schema();
        let (mask_indices, reorder_indices) =
            get_requested_indices(&requested_schema, &parquet_schema).unwrap();
        let expect_mask = vec![0, 1, 2, 3];
        let expect_reorder = vec![
            ReorderIndex::new_index(0),
            ReorderIndex::new_child(
                1,
                vec![ReorderIndex::new_index(0), ReorderIndex::new_index(1)],
            ),
            ReorderIndex::new_index(2),
        ];
        assert_eq!(mask_indices, expect_mask);
        assert_eq!(reorder_indices, expect_reorder);
    }

    #[test]
    fn nested_indices_reorder() {
        let requested_schema = Arc::new(StructType::new(vec![
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
        let parquet_schema = nested_parquet_schema();
        let (mask_indices, reorder_indices) =
            get_requested_indices(&requested_schema, &parquet_schema).unwrap();
        let expect_mask = vec![0, 1, 2, 3];
        let expect_reorder = vec![
            ReorderIndex::new_index(2),
            ReorderIndex::new_child(
                0,
                vec![ReorderIndex::new_index(1), ReorderIndex::new_index(0)],
            ),
            ReorderIndex::new_index(1),
        ];
        assert_eq!(mask_indices, expect_mask);
        assert_eq!(reorder_indices, expect_reorder);
    }

    #[test]
    fn nested_indices_mask_inner() {
        let requested_schema = Arc::new(StructType::new(vec![
            StructField::new("i", DataType::INTEGER, false),
            StructField::new(
                "nested",
                StructType::new(vec![StructField::new("int32", DataType::INTEGER, false)]),
                false,
            ),
            StructField::new("j", DataType::INTEGER, false),
        ]));
        let parquet_schema = nested_parquet_schema();
        let (mask_indices, reorder_indices) =
            get_requested_indices(&requested_schema, &parquet_schema).unwrap();
        let expect_mask = vec![0, 1, 3];
        let expect_reorder = vec![
            ReorderIndex::new_index(0),
            ReorderIndex::new_child(1, vec![ReorderIndex::new_index(0)]),
            ReorderIndex::new_index(2),
        ];
        assert_eq!(mask_indices, expect_mask);
        assert_eq!(reorder_indices, expect_reorder);
    }

    #[test]
    fn simple_list_mask() {
        let requested_schema = Arc::new(StructType::new(vec![
            StructField::new("i", DataType::INTEGER, false),
            StructField::new("list", ArrayType::new(DataType::INTEGER, false), false),
            StructField::new("j", DataType::INTEGER, false),
        ]));
        let parquet_schema = Arc::new(ArrowSchema::new(vec![
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
            get_requested_indices(&requested_schema, &parquet_schema).unwrap();
        let expect_mask = vec![0, 1, 2];
        let expect_reorder = vec![
            ReorderIndex::new_index(0),
            ReorderIndex::new_index(1),
            ReorderIndex::new_index(2),
        ];
        assert_eq!(mask_indices, expect_mask);
        assert_eq!(reorder_indices, expect_reorder);
    }

    #[test]
    fn nested_indices_list() {
        let requested_schema = Arc::new(StructType::new(vec![
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
        let parquet_schema = Arc::new(ArrowSchema::new(vec![
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
            get_requested_indices(&requested_schema, &parquet_schema).unwrap();
        let expect_mask = vec![0, 1, 2, 3];
        let expect_reorder = vec![
            ReorderIndex::new_index(0),
            ReorderIndex::new_child(
                1,
                vec![ReorderIndex::new_index(0), ReorderIndex::new_index(1)],
            ),
            ReorderIndex::new_index(2),
        ];
        assert_eq!(mask_indices, expect_mask);
        assert_eq!(reorder_indices, expect_reorder);
    }

    #[test]
    fn nested_indices_list_mask_inner() {
        let requested_schema = Arc::new(StructType::new(vec![
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
        let parquet_schema = Arc::new(ArrowSchema::new(vec![
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
            get_requested_indices(&requested_schema, &parquet_schema).unwrap();
        let expect_mask = vec![0, 1, 3];
        let expect_reorder = vec![
            ReorderIndex::new_index(0),
            ReorderIndex::new_child(1, vec![ReorderIndex::new_index(0)]),
            ReorderIndex::new_index(2),
        ];
        assert_eq!(mask_indices, expect_mask);
        assert_eq!(reorder_indices, expect_reorder);
    }

    #[test]
    fn nested_indices_list_mask_inner_reorder() {
        let requested_schema = Arc::new(StructType::new(vec![
            StructField::new("i", DataType::INTEGER, false),
            StructField::new(
                "list",
                ArrayType::new(
                    StructType::new(vec![
                        StructField::new("string", DataType::STRING, false),
                        StructField::new("int2", DataType::INTEGER, false),
                    ])
                    .into(),
                    false,
                ),
                false,
            ),
            StructField::new("j", DataType::INTEGER, false),
        ]));
        let parquet_schema = Arc::new(ArrowSchema::new(vec![
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
            get_requested_indices(&requested_schema, &parquet_schema).unwrap();
        let expect_mask = vec![0, 2, 3, 4];
        let expect_reorder = vec![
            ReorderIndex::new_index(0),
            ReorderIndex::new_child(
                1,
                vec![ReorderIndex::new_index(1), ReorderIndex::new_index(0)],
            ),
            ReorderIndex::new_index(2),
        ];
        assert_eq!(mask_indices, expect_mask);
        assert_eq!(reorder_indices, expect_reorder);
    }

    #[test]
    fn skipped_struct() {
        let requested_schema = Arc::new(StructType::new(vec![
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
        let parquet_schema = Arc::new(ArrowSchema::new(vec![
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
            get_requested_indices(&requested_schema, &parquet_schema).unwrap();
        let expect_mask = vec![2, 3, 4, 5];
        let expect_reorder = vec![
            ReorderIndex::new_index(2),
            ReorderIndex::new_child(
                1,
                vec![ReorderIndex::new_index(0), ReorderIndex::new_index(1)],
            ),
            ReorderIndex::new_index(0),
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
        let reorder = vec![ReorderIndex::new_index(1), ReorderIndex::new_index(0)];
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
            ReorderIndex::new_child(
                1,
                vec![ReorderIndex::new_index(1), ReorderIndex::new_index(0)],
            ),
            ReorderIndex::new_child(
                0,
                vec![
                    ReorderIndex::new_index(0),
                    ReorderIndex::new_index(1),
                    ReorderIndex::new_missing(
                        2,
                        Arc::new(ArrowField::new("s", ArrowDataType::Utf8, true)),
                    ),
                ],
            ),
        ];
        let ordered = reorder_struct_array(nested, &reorder).unwrap();
        assert_eq!(ordered.column_names(), vec!["struct2", "struct1"]);
        let ordered_s2 = ordered.column(0).as_struct();
        assert_eq!(ordered_s2.column_names(), vec!["b", "c", "s"]);
        let ordered_s1 = ordered.column(1).as_struct();
        assert_eq!(ordered_s1.column_names(), vec!["c", "b"]);
    }

    #[test]
    fn reorder_list_of_struct() {
        let boolean = Arc::new(BooleanArray::from(vec![
            false, false, true, true, false, true,
        ]));
        let int = Arc::new(Int32Array::from(vec![42, 28, 19, 31, 0, 3]));
        let list_sa = StructArray::from(vec![
            (
                Arc::new(ArrowField::new("b", ArrowDataType::Boolean, false)),
                boolean.clone() as ArrowArrayRef,
            ),
            (
                Arc::new(ArrowField::new("c", ArrowDataType::Int32, false)),
                int.clone() as ArrowArrayRef,
            ),
        ]);
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 3, 6]));
        let list_field = ArrowField::new("item", list_sa.data_type().clone(), false);
        let list = Arc::new(GenericListArray::new(
            Arc::new(list_field),
            offsets,
            Arc::new(list_sa),
            None,
        ));
        let fields: Fields = vec![
            Arc::new(ArrowField::new("b", ArrowDataType::Boolean, false)),
            Arc::new(ArrowField::new("c", ArrowDataType::Int32, false)),
        ]
        .into();
        let list_dt = Arc::new(ArrowField::new(
            "list",
            ArrowDataType::new_list(ArrowDataType::Struct(fields), false),
            false,
        ));
        let struct_array = StructArray::from(vec![(list_dt, list as ArrowArrayRef)]);
        let reorder = vec![ReorderIndex::new_child(
            0,
            vec![ReorderIndex::new_index(1), ReorderIndex::new_index(0)],
        )];
        let ordered = reorder_struct_array(struct_array, &reorder).unwrap();
        let ordered_list_col = ordered.column(0).as_list::<i32>();
        for i in 0..ordered_list_col.len() {
            let array_item = ordered_list_col.value(i);
            let struct_item = array_item.as_struct();
            assert_eq!(struct_item.column_names(), vec!["c", "b"]);
        }
    }
}

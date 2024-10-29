use std::os::raw::c_void;

use crate::{handle::Handle, kernel_string_slice, KernelStringSlice, SharedSnapshot};
use delta_kernel::schema::{ArrayType, DataType, MapType, PrimitiveType, StructType};
/// The `EngineSchemaVisitor` defines a visitor system to allow engines to build their own
/// representation of a schema from a particular schema within kernel.
///
/// The model is list based. When the kernel needs a list, it will ask engine to allocate one of a
/// particular size. Once allocated the engine returns an `id`, which can be any integer identifier
/// ([`usize`]) the engine wants, and will be passed back to the engine to identify the list in the
/// future.
///
/// Every schema element the kernel visits belongs to some list of "sibling" elements. The schema
/// itself is a list of schema elements, and every complex type (struct, map, array) contains a list
/// of "child" elements.
///  1. Before visiting schema or any complex type, the kernel asks the engine to allocate a list to
///     hold its children
///  2. When visiting any schema element, the kernel passes its parent's "child list" as the
///     "sibling list" the element should be appended to:
///      - For the top-level schema, visit each top-level column, passing the column's name and type
///      - For a struct, first visit each struct field, passing the field's name, type, nullability,
///        and metadata
///      - For a map, visit the key and value, passing its special name ("map_key" or "map_value"),
///        type, and value nullability (keys are never nullable)
///      - For a list, visit the element, passing its special name ("array_element"), type, and
///        nullability
///  3. When visiting a complex schema element, the kernel also passes the "child list" containing
///     that element's (already-visited) children.
///  4. The [`visit_schema`] method returns the id of the list of top-level columns
// WARNING: the visitor MUST NOT retain internal references to the string slices passed to visitor methods
// TODO: struct nullability and field metadata
#[repr(C)]
pub struct EngineSchemaVisitor {
    /// opaque state pointer
    pub data: *mut c_void,
    /// Creates a new field list, optionally reserving capacity up front
    pub make_field_list: extern "C" fn(data: *mut c_void, reserve: usize) -> usize,

    // visitor methods that should instantiate and append the appropriate type to the field list
    /// Indicate that the schema contains a `Struct` type. The top level of a Schema is always a
    /// `Struct`. The fields of the `Struct` are in the list identified by `child_list_id`.
    pub visit_struct: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        child_list_id: usize,
    ),

    /// Indicate that the schema contains an Array type. `child_list_id` will be a _one_ item list
    /// with the array's element type
    pub visit_array: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        contains_null: bool, // if this array can contain null values
        child_list_id: usize,
    ),

    /// Indicate that the schema contains an Map type. `child_list_id` will be a _two_ item list
    /// where the first element is the map's key type and the second element is the
    /// map's value type
    pub visit_map: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        value_contains_null: bool, // if this map can contain null values
        child_list_id: usize,
    ),

    /// visit a `decimal` with the specified `precision` and `scale`
    pub visit_decimal: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        precision: u8,
        scale: u8,
    ),

    /// Visit a `string` belonging to the list identified by `sibling_list_id`.
    pub visit_string:
        extern "C" fn(data: *mut c_void, sibling_list_id: usize, name: KernelStringSlice),

    /// Visit a `long` belonging to the list identified by `sibling_list_id`.
    pub visit_long:
        extern "C" fn(data: *mut c_void, sibling_list_id: usize, name: KernelStringSlice),

    /// Visit an `integer` belonging to the list identified by `sibling_list_id`.
    pub visit_integer:
        extern "C" fn(data: *mut c_void, sibling_list_id: usize, name: KernelStringSlice),

    /// Visit a `short` belonging to the list identified by `sibling_list_id`.
    pub visit_short:
        extern "C" fn(data: *mut c_void, sibling_list_id: usize, name: KernelStringSlice),

    /// Visit a `byte` belonging to the list identified by `sibling_list_id`.
    pub visit_byte:
        extern "C" fn(data: *mut c_void, sibling_list_id: usize, name: KernelStringSlice),

    /// Visit a `float` belonging to the list identified by `sibling_list_id`.
    pub visit_float:
        extern "C" fn(data: *mut c_void, sibling_list_id: usize, name: KernelStringSlice),

    /// Visit a `double` belonging to the list identified by `sibling_list_id`.
    pub visit_double:
        extern "C" fn(data: *mut c_void, sibling_list_id: usize, name: KernelStringSlice),

    /// Visit a `boolean` belonging to the list identified by `sibling_list_id`.
    pub visit_boolean:
        extern "C" fn(data: *mut c_void, sibling_list_id: usize, name: KernelStringSlice),

    /// Visit `binary` belonging to the list identified by `sibling_list_id`.
    pub visit_binary:
        extern "C" fn(data: *mut c_void, sibling_list_id: usize, name: KernelStringSlice),

    /// Visit a `date` belonging to the list identified by `sibling_list_id`.
    pub visit_date:
        extern "C" fn(data: *mut c_void, sibling_list_id: usize, name: KernelStringSlice),

    /// Visit a `timestamp` belonging to the list identified by `sibling_list_id`.
    pub visit_timestamp:
        extern "C" fn(data: *mut c_void, sibling_list_id: usize, name: KernelStringSlice),

    /// Visit a `timestamp` with no timezone belonging to the list identified by `sibling_list_id`.
    pub visit_timestamp_ntz:
        extern "C" fn(data: *mut c_void, sibling_list_id: usize, name: KernelStringSlice),
}

/// Visit the schema of the passed `SnapshotHandle`, using the provided `visitor`. See the
/// documentation of [`EngineSchemaVisitor`] for a description of how this visitor works.
///
/// This method returns the id of the list allocated to hold the top level schema columns.
///
/// # Safety
///
/// Caller is responsible for passing a valid snapshot handle and schema visitor.
#[no_mangle]
pub unsafe extern "C" fn visit_schema(
    snapshot: Handle<SharedSnapshot>,
    visitor: &mut EngineSchemaVisitor,
) -> usize {
    let snapshot = unsafe { snapshot.as_ref() };
    // Visit all the fields of a struct and return the list of children
    fn visit_struct_fields(visitor: &EngineSchemaVisitor, s: &StructType) -> usize {
        let child_list_id = (visitor.make_field_list)(visitor.data, s.fields.len());
        for field in s.fields() {
            visit_schema_item(field.data_type(), field.name(), visitor, child_list_id);
        }
        child_list_id
    }

    fn visit_array_item(visitor: &EngineSchemaVisitor, at: &ArrayType) -> usize {
        let child_list_id = (visitor.make_field_list)(visitor.data, 1);
        visit_schema_item(&at.element_type, "array_element", visitor, child_list_id);
        child_list_id
    }

    fn visit_map_types(visitor: &EngineSchemaVisitor, mt: &MapType) -> usize {
        let child_list_id = (visitor.make_field_list)(visitor.data, 2);
        visit_schema_item(&mt.key_type, "map_key", visitor, child_list_id);
        visit_schema_item(&mt.value_type, "map_value", visitor, child_list_id);
        child_list_id
    }

    // Visit a struct field (recursively) and add the result to the list of siblings.
    fn visit_schema_item(
        data_type: &DataType,
        name: &str,
        visitor: &EngineSchemaVisitor,
        sibling_list_id: usize,
    ) {
        macro_rules! call {
            ( $visitor_fn:ident $(, $extra_args:expr) *) => {
                (visitor.$visitor_fn)(
                    visitor.data,
                    sibling_list_id,
                    kernel_string_slice!(name)
                    $(, $extra_args) *
                )
            };
        }
        match data_type {
            DataType::Struct(st) => call!(visit_struct, visit_struct_fields(visitor, st)),
            DataType::Map(mt) => {
                call!(
                    visit_map,
                    mt.value_contains_null,
                    visit_map_types(visitor, mt)
                )
            }
            DataType::Array(at) => {
                call!(visit_array, at.contains_null, visit_array_item(visitor, at))
            }
            DataType::Primitive(PrimitiveType::Decimal(precision, scale)) => {
                call!(visit_decimal, *precision, *scale)
            }
            &DataType::STRING => call!(visit_string),
            &DataType::LONG => call!(visit_long),
            &DataType::INTEGER => call!(visit_integer),
            &DataType::SHORT => call!(visit_short),
            &DataType::BYTE => call!(visit_byte),
            &DataType::FLOAT => call!(visit_float),
            &DataType::DOUBLE => call!(visit_double),
            &DataType::BOOLEAN => call!(visit_boolean),
            &DataType::BINARY => call!(visit_binary),
            &DataType::DATE => call!(visit_date),
            &DataType::TIMESTAMP => call!(visit_timestamp),
            &DataType::TIMESTAMP_NTZ => call!(visit_timestamp_ntz),
        }
    }

    visit_struct_fields(visitor, snapshot.schema())
}

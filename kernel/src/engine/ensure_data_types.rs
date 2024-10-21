//! Helpers to ensure that kernel data types match arrow data types

use std::{collections::{HashMap, HashSet}, ops::Deref};

use arrow_schema::{DataType as ArrowDataType, Field as ArrowField};
use itertools::Itertools;

use crate::{
    engine::arrow_utils::make_arrow_error,
    schema::{DataType, MetadataValue, StructField},
    utils::require,
    DeltaResult, Error,
};

/// Ensure a kernel data type matches an arrow data type. This only ensures that the actual "type"
/// is the same, but does so recursively into structs, and ensures lists and maps have the correct
/// associated types as well.
///
/// If `check_nullability_and_metadata` is true, this will also return an error if it finds a struct
/// field that differs in nullability or metadata between the kernel and arrow schema. If it is
/// false, no checks on nullability or metadata are performed.
///
/// This returns an `Ok(DataTypeCompat)` if the types are compatible, and
/// will indicate what kind of compatibility they have, or an error if the types do not match. If
/// there is a `struct` type included, we only ensure that the named fields that the kernel is
/// asking for exist, and that for those fields the types match. Un-selected fields are ignored.
pub(crate) fn ensure_data_types(
    kernel_type: &DataType,
    arrow_type: &ArrowDataType,
    check_nullability_and_metadata: bool,
) -> DeltaResult<DataTypeCompat> {
    let check = EnsureDataTypes { check_nullability_and_metadata };
    check.ensure_data_types(kernel_type, arrow_type)
}

struct EnsureDataTypes {
    check_nullability_and_metadata: bool,
}

/// Capture the compatibility between two data-types, as passed to [`ensure_data_types`]
pub(crate) enum DataTypeCompat {
    /// The two types are the same
    Identical,
    /// What is read from parquet needs to be cast to the associated type
    NeedsCast(ArrowDataType),
    /// Types are compatible, but are nested types. This is used when comparing types where casting
    /// is not desired (i.e. in the expression evaluator)
    Nested,
}

impl EnsureDataTypes {
    // Perform the check. See documentation for `ensure_data_types` entry point method above
    fn ensure_data_types(
        &self,
        kernel_type: &DataType,
        arrow_type: &ArrowDataType,
    ) -> DeltaResult<DataTypeCompat> {
        match (kernel_type, arrow_type) {
            (DataType::Primitive(_), _) if arrow_type.is_primitive() => {
                check_cast_compat(kernel_type.try_into()?, arrow_type)
            }
            // strings, bools, and binary  aren't primitive in arrow
            (&DataType::BOOLEAN, ArrowDataType::Boolean)
                | (&DataType::STRING, ArrowDataType::Utf8)
                | (&DataType::BINARY, ArrowDataType::Binary) => {
                    Ok(DataTypeCompat::Identical)
                }
            (DataType::Array(inner_type), ArrowDataType::List(arrow_list_field)) => {
                self.ensure_nullability(
                    "List",
                    inner_type.contains_null,
                    arrow_list_field.is_nullable(),
                )?;
                self.ensure_data_types(
                    &inner_type.element_type,
                    arrow_list_field.data_type(),
                )
            }
            (DataType::Map(kernel_map_type), ArrowDataType::Map(arrow_map_type, _)) => {
                let ArrowDataType::Struct(fields) = arrow_map_type.data_type() else {
                    return Err(make_arrow_error("Arrow map type wasn't a struct."));
                };
                let [key_type, value_type] = fields.deref() else {
                    return Err(make_arrow_error("Arrow map type didn't have expected key/value fields"));
                };
                self.ensure_data_types(
                    &kernel_map_type.key_type,
                    key_type.data_type(),
                )?;
                self.ensure_nullability(
                    "Map",
                    kernel_map_type.value_contains_null,
                    value_type.is_nullable(),
                )?;
                self.ensure_data_types(
                    &kernel_map_type.value_type,
                    value_type.data_type(),
                )?;
                Ok(DataTypeCompat::Nested)
            }
            (DataType::Struct(kernel_fields), ArrowDataType::Struct(arrow_fields)) => {
                // build a list of kernel fields that matches the order of the arrow fields
                let mapped_fields = arrow_fields
                    .iter()
                    .filter_map(|f| kernel_fields.fields.get(f.name()));

                // keep track of how many fields we matched up
                let mut found_fields = 0;
                // ensure that for the fields that we found, the types match
                for (kernel_field, arrow_field) in mapped_fields.zip(arrow_fields) {
                    self.ensure_nullability_and_metadata(kernel_field, arrow_field)?;
                    self.ensure_data_types(
                        &kernel_field.data_type,
                        arrow_field.data_type(),
                    )?;
                    found_fields += 1;
                }

                // require that we found the number of fields that we requested.
                require!(kernel_fields.fields.len() == found_fields, {
                    let arrow_field_map: HashSet<&String> =
                        HashSet::from_iter(arrow_fields.iter().map(|f| f.name()));
                    let missing_field_names = kernel_fields
                        .fields
                        .keys()
                        .filter(|kernel_field| !arrow_field_map.contains(kernel_field))
                        .take(5)
                        .join(", ");
                    make_arrow_error(format!(
                        "Missing Struct fields {} (Up to five missing fields shown)",
                        missing_field_names
                    ))
                });
                Ok(DataTypeCompat::Nested)
            }
            _ => Err(make_arrow_error(format!(
                "Incorrect datatype. Expected {}, got {}",
                kernel_type, arrow_type
            ))),
        }
    }

    fn ensure_nullability(
        &self,
        desc: &str,
        kernel_field_is_nullable: bool,
        arrow_field_is_nullable: bool,
    ) -> DeltaResult<()> {
        if self.check_nullability_and_metadata && kernel_field_is_nullable != arrow_field_is_nullable {
            Err(Error::Generic(format!(
                "{desc} has nullablily {} in kernel and {} in arrow",
                kernel_field_is_nullable,
                arrow_field_is_nullable,
            )))
        } else {
            Ok(())
        }
    }

    fn ensure_nullability_and_metadata(
        &self,
        kernel_field: &StructField,
        arrow_field: &ArrowField
    ) -> DeltaResult<()> {
        self.ensure_nullability(&kernel_field.name, kernel_field.nullable, arrow_field.is_nullable())?;
        if self.check_nullability_and_metadata && !metadata_eq(&kernel_field.metadata, arrow_field.metadata()) {
            Err(Error::Generic(format!(
                "Field {} has metadata {:?} in kernel and {:?} in arrow",
                kernel_field.name,
                kernel_field.metadata,
                arrow_field.metadata(),
            )))
        } else {
            Ok(())
        }
    }
}

// Check if two types can be cast
fn check_cast_compat(
    target_type: ArrowDataType,
    source_type: &ArrowDataType,
) -> DeltaResult<DataTypeCompat> {
    use ArrowDataType::*;

    match (source_type, &target_type) {
        (source_type, target_type) if source_type == target_type => Ok(DataTypeCompat::Identical),
        (&ArrowDataType::Timestamp(_, _), &ArrowDataType::Timestamp(_, _)) => {
            // timestamps are able to be cast between each other
            Ok(DataTypeCompat::NeedsCast(target_type))
        }
        // Allow up-casting to a larger type if it's safe and can't cause overflow or loss of precision.
        (Int8, Int16 | Int32 | Int64 | Float64) => Ok(DataTypeCompat::NeedsCast(target_type)),
        (Int16, Int32 | Int64 | Float64) => Ok(DataTypeCompat::NeedsCast(target_type)),
        (Int32, Int64 | Float64) => Ok(DataTypeCompat::NeedsCast(target_type)),
        (Float32, Float64) => Ok(DataTypeCompat::NeedsCast(target_type)),
        (_, Decimal128(p, s)) if can_upcast_to_decimal(source_type, *p, *s) => {
            Ok(DataTypeCompat::NeedsCast(target_type))
        }
        (Date32, Timestamp(_, None)) => Ok(DataTypeCompat::NeedsCast(target_type)),
        _ => Err(make_arrow_error(format!(
            "Incorrect datatype. Expected {}, got {}",
            target_type, source_type
        ))),
    }
}

// Returns whether the given source type can be safely cast to a decimal with the given precision and scale without
// loss of information.
fn can_upcast_to_decimal(
    source_type: &ArrowDataType,
    target_precision: u8,
    target_scale: i8,
) -> bool {
    use ArrowDataType::*;

    let (source_precision, source_scale) = match source_type {
        Decimal128(p, s) => (*p, *s),
        // Allow converting integers to a decimal that can hold all possible values.
        Int8 => (3u8, 0i8),
        Int16 => (5u8, 0i8),
        Int32 => (10u8, 0i8),
        Int64 => (20u8, 0i8),
        _ => return false,
    };

    target_precision >= source_precision
        && target_scale >= source_scale
        && target_precision - source_precision >= (target_scale - source_scale) as u8
}

impl PartialEq<String> for MetadataValue {
    fn eq(&self, other: &String) -> bool {
        self.to_string().eq(other)
    }
}

// allow for comparing our metadata maps to arrow ones. We can't implement PartialEq because both
// are HashMaps which aren't defined in this crate
fn metadata_eq(
    kernel_metadata: &HashMap<String, MetadataValue>,
    arrow_metadata: &HashMap<String, String>,
) -> bool {
    let kernel_len = kernel_metadata.len();
    if kernel_len != arrow_metadata.len() {
        return false;
    }
    if kernel_len == 0 {
        // lens are equal, so two empty maps are equal
        return true;
    }
    kernel_metadata
        .iter()
        .all(|(key, value)| arrow_metadata.get(key).is_some_and(|v| *value == *v))
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Fields};

    use crate::{
        engine::ensure_data_types::ensure_data_types,
        schema::{ArrayType, DataType, MapType, StructField},
    };

    #[test]
    fn accepts_safe_decimal_casts() {
        use super::can_upcast_to_decimal;
        use ArrowDataType::*;

        assert!(can_upcast_to_decimal(&Decimal128(1, 0), 2u8, 0i8));
        assert!(can_upcast_to_decimal(&Decimal128(1, 0), 2u8, 1i8));
        assert!(can_upcast_to_decimal(&Decimal128(5, -2), 6u8, -2i8));
        assert!(can_upcast_to_decimal(&Decimal128(5, -2), 6u8, -1i8));
        assert!(can_upcast_to_decimal(&Decimal128(5, 1), 6u8, 1i8));
        assert!(can_upcast_to_decimal(&Decimal128(5, 1), 6u8, 2i8));
        assert!(can_upcast_to_decimal(
            &Decimal128(10, 5),
            arrow_schema::DECIMAL128_MAX_PRECISION,
            arrow_schema::DECIMAL128_MAX_SCALE - 5
        ));

        assert!(can_upcast_to_decimal(&Int8, 3u8, 0i8));
        assert!(can_upcast_to_decimal(&Int8, 4u8, 0i8));
        assert!(can_upcast_to_decimal(&Int8, 4u8, 1i8));
        assert!(can_upcast_to_decimal(&Int8, 7u8, 2i8));

        assert!(can_upcast_to_decimal(&Int16, 5u8, 0i8));
        assert!(can_upcast_to_decimal(&Int16, 6u8, 0i8));
        assert!(can_upcast_to_decimal(&Int16, 6u8, 1i8));
        assert!(can_upcast_to_decimal(&Int16, 9u8, 2i8));

        assert!(can_upcast_to_decimal(&Int32, 10u8, 0i8));
        assert!(can_upcast_to_decimal(&Int32, 11u8, 0i8));
        assert!(can_upcast_to_decimal(&Int32, 11u8, 1i8));
        assert!(can_upcast_to_decimal(&Int32, 14u8, 2i8));

        assert!(can_upcast_to_decimal(&Int64, 20u8, 0i8));
        assert!(can_upcast_to_decimal(&Int64, 21u8, 0i8));
        assert!(can_upcast_to_decimal(&Int64, 21u8, 1i8));
        assert!(can_upcast_to_decimal(&Int64, 24u8, 2i8));
    }

    #[test]
    fn rejects_unsafe_decimal_casts() {
        use super::can_upcast_to_decimal;
        use ArrowDataType::*;

        assert!(!can_upcast_to_decimal(&Decimal128(2, 0), 2u8, 1i8));
        assert!(!can_upcast_to_decimal(&Decimal128(2, 0), 2u8, -1i8));
        assert!(!can_upcast_to_decimal(&Decimal128(5, 2), 6u8, 4i8));

        assert!(!can_upcast_to_decimal(&Int8, 2u8, 0i8));
        assert!(!can_upcast_to_decimal(&Int8, 3u8, 1i8));
        assert!(!can_upcast_to_decimal(&Int16, 4u8, 0i8));
        assert!(!can_upcast_to_decimal(&Int16, 5u8, 1i8));
        assert!(!can_upcast_to_decimal(&Int32, 9u8, 0i8));
        assert!(!can_upcast_to_decimal(&Int32, 10u8, 1i8));
        assert!(!can_upcast_to_decimal(&Int64, 19u8, 0i8));
        assert!(!can_upcast_to_decimal(&Int64, 20u8, 1i8));
    }

    #[test]
    fn ensure_decimals() {
        assert!(ensure_data_types(
            &DataType::decimal_unchecked(5, 2),
            &ArrowDataType::Decimal128(5, 2),
            false
        )
        .is_ok());
        assert!(ensure_data_types(
            &DataType::decimal_unchecked(5, 2),
            &ArrowDataType::Decimal128(5, 3),
            false
        )
        .is_err());
    }

    #[test]
    fn ensure_map() {
        let arrow_field = ArrowField::new_map(
            "map",
            "entries",
            ArrowField::new("key", ArrowDataType::Int64, false),
            ArrowField::new("val", ArrowDataType::Utf8, true),
            false,
            false,
        );
        assert!(ensure_data_types(
            &DataType::Map(Box::new(MapType::new(
                DataType::LONG,
                DataType::STRING,
                true
            ))),
            arrow_field.data_type(),
            false
        )
        .is_ok());

        assert!(ensure_data_types(
            &DataType::Map(Box::new(MapType::new(
                DataType::LONG,
                DataType::STRING,
                false
            ))),
            arrow_field.data_type(),
            true
        )
        .is_err());

        assert!(ensure_data_types(
            &DataType::Map(Box::new(MapType::new(DataType::LONG, DataType::LONG, true))),
            arrow_field.data_type(),
            false
        )
        .is_err());
    }

    #[test]
    fn ensure_list() {
        assert!(ensure_data_types(
            &DataType::Array(Box::new(ArrayType::new(DataType::LONG, true))),
            &ArrowDataType::new_list(ArrowDataType::Int64, true),
            false
        )
        .is_ok());
        assert!(ensure_data_types(
            &DataType::Array(Box::new(ArrayType::new(DataType::STRING, true))),
            &ArrowDataType::new_list(ArrowDataType::Int64, true),
            false
        )
        .is_err());
        assert!(ensure_data_types(
            &DataType::Array(Box::new(ArrayType::new(DataType::LONG, true))),
            &ArrowDataType::new_list(ArrowDataType::Int64, false),
            true
        )
        .is_err());
    }

    #[test]
    fn ensure_struct() {
        let schema = DataType::struct_type([StructField::new(
            "a",
            ArrayType::new(
                DataType::struct_type([
                    StructField::new("w", DataType::LONG, true),
                    StructField::new("x", ArrayType::new(DataType::LONG, true), true),
                    StructField::new(
                        "y",
                        MapType::new(DataType::LONG, DataType::STRING, true),
                        true,
                    ),
                    StructField::new(
                        "z",
                        DataType::struct_type([
                            StructField::new("n", DataType::LONG, true),
                            StructField::new("m", DataType::STRING, true),
                        ]),
                        true,
                    ),
                ]),
                true,
            ),
            true,
        )]);
        let arrow_struct: ArrowDataType = (&schema).try_into().unwrap();
        assert!(ensure_data_types(&schema, &arrow_struct, true).is_ok());

        let kernel_simple = DataType::struct_type([
            StructField::new("w", DataType::LONG, true),
            StructField::new("x", DataType::LONG, true),
        ]);

        let arrow_simple_ok = ArrowField::new_struct(
            "arrow_struct",
            Fields::from(vec![
                ArrowField::new("w", ArrowDataType::Int64, true),
                ArrowField::new("x", ArrowDataType::Int64, true),
            ]),
            true,
        );
        assert!(ensure_data_types(&kernel_simple, arrow_simple_ok.data_type(), true).is_ok());

        let arrow_missing_simple = ArrowField::new_struct(
            "arrow_struct",
            Fields::from(vec![ArrowField::new("w", ArrowDataType::Int64, true)]),
            true,
        );
        assert!(ensure_data_types(&kernel_simple, arrow_missing_simple.data_type(), true).is_err());

        let arrow_nullable_mismatch_simple = ArrowField::new_struct(
            "arrow_struct",
            Fields::from(vec![
                ArrowField::new("w", ArrowDataType::Int64, false),
                ArrowField::new("x", ArrowDataType::Int64, true),
            ]),
            true,
        );
        assert!(ensure_data_types(
            &kernel_simple,
            arrow_nullable_mismatch_simple.data_type(),
            true
        )
        .is_err());
    }
}

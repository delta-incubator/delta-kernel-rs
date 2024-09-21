//! Schema definitions for action types

use std::collections::{HashMap, HashSet};

use crate::schema::{ArrayType, DataType, MapType, StructField};

pub(crate) trait ToDataType {
    fn to_data_type() -> DataType;
    fn nullable() -> bool;
}

macro_rules! impl_to_data_type {
    ( $(($rust_type: ty, $kernel_type: expr, $nullable: expr)), * ) => {
        $(
            impl ToDataType for $rust_type {
                fn to_data_type() -> DataType {
                    $kernel_type
                }

                fn nullable() -> bool {
                    $nullable
                }
            }
        )*
    };
}

impl_to_data_type!(
    (String, DataType::STRING, false),
    (i64, DataType::LONG, false),
    (i32, DataType::INTEGER, false),
    (i16, DataType::SHORT, false),
    (char, DataType::BYTE, false),
    (f32, DataType::FLOAT, false),
    (f64, DataType::DOUBLE, false),
    (bool, DataType::BOOLEAN, false)
);

// ToDataType impl for non-nullable array types
impl<T: ToDataType> ToDataType for Vec<T> {
    fn to_data_type() -> DataType {
        ArrayType::new(T::to_data_type(), T::nullable()).into()
    }

    fn nullable() -> bool {
        T::nullable()
    }
}

impl<T: ToDataType> ToDataType for HashSet<T> {
    fn to_data_type() -> DataType {
        ArrayType::new(T::to_data_type(), T::nullable()).into()
    }

    fn nullable() -> bool {
        T::nullable()
    }
}

// ToDataType impl for non-nullable map types
impl<K: ToDataType, V: ToDataType> ToDataType for HashMap<K, V> {
    fn to_data_type() -> DataType {
        MapType::new(K::to_data_type(), V::to_data_type(), V::nullable()).into()
    }

    fn nullable() -> bool {
        V::nullable()
    }
}

impl<T: ToDataType> ToDataType for Option<T> {
    fn to_data_type() -> DataType {
        T::to_data_type()
    }

    fn nullable() -> bool {
        true
    }
}

pub(crate) trait GetStructField {
    fn get_struct_field(name: impl Into<String>) -> StructField;
}

// Normal types produce non-nullable fields
impl<T: ToDataType> GetStructField for T {
    fn get_struct_field(name: impl Into<String>) -> StructField {
        StructField::new(name, T::to_data_type(), T::nullable())
    }
}

// Option types produce nullable fields
// impl<T: ToDataType> GetStructField for Option<T> {
//     fn get_struct_field(name: impl Into<String>) -> StructField {
//         StructField::new(name, T::to_data_type(), T::nullable())
//     }
// }

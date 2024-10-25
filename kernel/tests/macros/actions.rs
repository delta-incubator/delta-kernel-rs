pub mod schemas {
    use crate::schema::DataType;
    use crate::schema::MapType;
    use crate::schema::StructField;
    use std::collections::HashMap;

    pub trait ToDataType {
        fn to_data_type() -> DataType;
    }

    pub(crate) trait ToNullableContainerType {
        fn to_nullable_container_type() -> DataType;
    }

    pub trait GetStructField {
        fn get_struct_field(name: impl Into<String>) -> StructField;
    }

    pub trait GetNullableContainerStructField {
        fn get_nullable_container_struct_field(name: impl Into<String>) -> StructField;
    }

    impl<T: ToNullableContainerType> GetNullableContainerStructField for T {
        fn get_nullable_container_struct_field(name: impl Into<String>) -> StructField {
            StructField::new(name, T::to_nullable_container_type(), false)
        }
    }

    macro_rules! impl_to_data_type {
        ( $(($rust_type: ty, $kernel_type: expr)), * ) => {
            $(
                impl ToDataType for $rust_type {
                    fn to_data_type() -> DataType {
                        $kernel_type
                    }
                }
            )*
        };
    }

    impl_to_data_type!((String, DataType::STRING));

    impl<T: ToDataType> GetStructField for T {
        fn get_struct_field(name: impl Into<String>) -> StructField {
            StructField::new(name, T::to_data_type(), false)
        }
    }

    impl<K: ToDataType, V: ToDataType> ToDataType for HashMap<K, V> {
        fn to_data_type() -> DataType {
            MapType::new(K::to_data_type(), V::to_data_type(), false).into()
        }
    }

    impl<A: ToDataType, B: ToDataType, C: ToDataType> ToDataType for Box<dyn Fn(A, B) -> C> {
        fn to_data_type() -> DataType {
            MapType::new(A::to_data_type(), B::to_data_type(), false).into()
        }
    }

    impl<K: ToDataType, V: ToDataType> ToNullableContainerType for HashMap<K, V> {
        fn to_nullable_container_type() -> DataType {
            MapType::new(K::to_data_type(), V::to_data_type(), true).into()
        }
    }
}

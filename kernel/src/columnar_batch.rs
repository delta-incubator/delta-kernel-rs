use std::sync::Arc;

use crate::{
    schema::{DataType, Schema, StructField},
    DeltaResult,
};

pub trait ColumnarBatch {
    /// Get the schema of the batch.
    fn schema(&self) -> DeltaResult<Schema>;

    /// Get the column at the specified index.
    fn column(&self, index: usize) -> &dyn ColumnVector;

    /// Number of rows in the batch.
    fn size(&self) -> usize;

    fn with_column(
        &self,
        index: usize,
        field: StructField,
        column: Arc<dyn ColumnVector>,
    ) -> DeltaResult<Self>
    where
        Self: Sized;

    fn with_deleted_column_at(&self, index: usize) -> DeltaResult<Self>
    where
        Self: Sized;

    fn with_schema(&self, schema: Schema) -> DeltaResult<Self>
    where
        Self: Sized;

    fn slice(&self, offset: usize, length: usize) -> DeltaResult<Self>
    where
        Self: Sized;

    fn rows(&self) -> Box<dyn Iterator<Item = &dyn Row>>;
}

// TODO: do all these methods do bounds checking? Should we offer alternative
// methods that don't require it (i.e. iterators)?

// TODO: should these methods type check?

pub trait ColumnVector {
    fn data_type(&self) -> DeltaResult<DataType>;
    fn size(&self) -> usize;
    fn is_null(&self, i: usize) -> bool;
    fn get_i32(&self, i: usize) -> DeltaResult<Option<i32>>;
    fn get_string(&self, i: usize) -> DeltaResult<Option<&str>>;
    fn get_struct(&self, i: usize) -> DeltaResult<Option<&dyn Row>>;
    fn get_array(&self, i: usize) -> DeltaResult<Option<&dyn ArrayValue>>;
    fn get_map(&self, i: usize) -> DeltaResult<Option<&dyn MapValue>>;
}

pub trait Row {
    fn schema(&self) -> DeltaResult<Schema>;
    fn is_null(&self, i: usize) -> bool;
    fn get_i32(&self, i: usize) -> DeltaResult<Option<i32>>;
    fn get_string(&self, i: usize) -> DeltaResult<Option<&str>>;
    fn get_struct(&self, i: usize) -> DeltaResult<Option<&dyn Row>>;
    fn get_array(&self, i: usize) -> DeltaResult<Option<&dyn ArrayValue>>;
    fn get_map(&self, i: usize) -> DeltaResult<Option<&dyn MapValue>>;
}

// Based on: https://github.com/delta-io/delta/pull/2087

pub trait ArrayValue {
    /// Return the number of elements in the array
    fn size(&self) -> usize;

    /// Get the elements in the array
    fn elements(&self) -> &dyn ColumnVector;
}

pub trait MapValue {
    /// Return the number of elements in the map
    fn size(&self) -> usize;

    /// Get the keys in the map
    fn keys(&self) -> &dyn ColumnVector;

    /// Get the values in the map
    fn values(&self) -> &dyn ColumnVector;
}

mod arrow {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int32Type;
    use arrow_array::Array as ArrowArray;
    use arrow_array::RecordBatch;
    use arrow_array::StructArray;
    use arrow_schema::{DataType as ArrowDataType, Schema as ArrowSchema};

    use crate::Error;

    use super::*;

    impl ColumnarBatch for RecordBatch {
        fn schema(&self) -> DeltaResult<Schema> {
            Ok(self.schema().as_ref().try_into()?)
        }

        fn column(&self, index: usize) -> &dyn ColumnVector {
            &self.column(index).as_ref() as &dyn ColumnVector
        }
    }

    impl ColumnVector for dyn ArrowArray {
        fn data_type(&self) -> DeltaResult<DataType> {
            Ok(self.data_type().try_into()?)
        }

        fn size(&self) -> usize {
            self.len()
        }

        fn is_null(&self, i: usize) -> bool {
            self.is_null(i)
        }

        /// Get the i32 value at the specified index.
        ///
        /// This will panic if the column is not boolean or if the index is out of bounds.
        fn get_i32(&self, i: usize) -> DeltaResult<Option<i32>> {
            if self.is_null(i) {
                Ok(None)
            } else {
                Ok(Some(self.as_primitive::<Int32Type>().value(i)))
            }
        }

        /// Get the string value at the specified index.
        ///
        /// This will panic if the column is not string or if the index is out of bounds.
        fn get_string(&self, i: usize) -> DeltaResult<Option<&str>> {
            if self.is_null(i) {
                Ok(None)
            } else {
                match self.data_type() {
                    ArrowDataType::Utf8 => Ok(Some(self.as_string::<i32>().value(i))),
                    ArrowDataType::LargeUtf8 => Ok(Some(self.as_string::<i64>().value(i))),
                    _ => panic!("get_string called on non-string column"),
                }
            }
        }

        /// Get the struct value at the specified index.
        ///
        /// This will panic if the column is not struct or if the index is out of bounds.
        fn get_struct(&self, i: usize) -> DeltaResult<Option<&dyn Row>> {
            if self.is_null(i) {
                Ok(None)
            } else {
                let batch = self
                    .as_struct_opt()
                    .expect("get_struct called on non-struct column");
                let row = ArrowRow {
                    batch,
                    row_index: i,
                };
                Ok(Some(&row))
            }
        }

        /// Get the array value at the specified index.
        ///
        /// This will panic if the column is not array or if the index is out of bounds.
        fn get_array(&self, i: usize) -> DeltaResult<Option<&dyn ArrayValue>> {
            if self.is_null(i) {
                Ok(None)
            } else {
                let batch = self
                    .as_list_opt()
                    .expect("get_array called on non-array column");
                let array = ArrowArray {
                    array: batch.value(i),
                };
                Ok(Some(&array))
            }
        }
    }

    trait ArrowTabular {
        fn schema(&self) -> &ArrowSchema;
        fn column(&self, index: usize) -> &dyn ArrowArray;
    }

    impl ArrowTabular for RecordBatch {
        fn schema(&self) -> &ArrowSchema {
            self.schema().as_ref()
        }

        fn column(&self, index: usize) -> &dyn ArrowArray {
            self.column(index).as_ref()
        }
    }

    impl ArrowTabular for StructArray {
        fn schema(&self) -> &ArrowSchema {
            self.schema()
        }

        fn column(&self, index: usize) -> &dyn ArrowArray {
            self.column(index).as_ref()
        }
    }

    /// A reference to a row in a RecordBatch or StructArray.
    pub struct ArrowRow<'a, T: ArrowTabular> {
        batch: &'a T,
        row_index: usize,
    }

    impl<'a, T: ArrowTabular> Row for ArrowRow<'a, T> {
        fn schema(&self) -> DeltaResult<Schema> {
            ArrowTabular::schema(self.batch)
                .try_into()
                .map_err(|err| Error::Arrow(err))
        }

        fn is_null(&self, i: usize) -> bool {
            self.batch.column(i).is_null(self.row_index)
        }

        fn get_i32(&self, i: usize) -> DeltaResult<Option<i32>> {
            self.batch.column(i).get_i32(self.row_index)
        }

        fn get_string(&self, i: usize) -> DeltaResult<Option<&str>> {
            self.batch.column(i).get_string(self.row_index)
        }

        fn get_struct(&self, i: usize) -> DeltaResult<Option<&dyn Row>> {
            self.batch.column(i).get_struct(self.row_index)
        }

        fn get_array(&self, i: usize) -> DeltaResult<Option<&dyn ArrayValue>> {
            self.batch.column(i).get_array(self.row_index)
        }

        fn get_map(&self, i: usize) -> DeltaResult<Option<&dyn MapValue>> {
            self.batch.column(i).get_map(self.row_index)
        }
    }

    pub struct ArrowArraySlice<'a> {
        batch: &'a dyn ArrowArray,
        offset: usize,
        length: usize,
    }

    
}

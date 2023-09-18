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

pub trait ColumnVector {
    fn data_type(&self) -> DeltaResult<DataType>;
    fn size(&self) -> usize;
    fn is_null(&self, i: usize) -> bool;

    fn get_bool(&self, i: usize) -> DeltaResult<Option<bool>>;
    fn get_i8(&self, i: usize) -> DeltaResult<Option<i8>>;
    fn get_i16(&self, i: usize) -> DeltaResult<Option<i16>>;
    fn get_i32(&self, i: usize) -> DeltaResult<Option<i32>>;
    fn get_i64(&self, i: usize) -> DeltaResult<Option<i64>>;
    fn get_f32(&self, i: usize) -> DeltaResult<Option<f32>>;
    fn get_f64(&self, i: usize) -> DeltaResult<Option<f64>>;
    fn get_binary(&self, i: usize) -> DeltaResult<Option<&[u8]>>;
    fn get_string(&self, i: usize) -> DeltaResult<Option<&str>>;
    // TODO: decimal
    fn get_struct(&self, i: usize) -> DeltaResult<Option<&dyn Row>>;
    // TODO: map, list
}

pub trait Row {
    fn schema(&self) -> &Schema;
    fn is_null(&self, i: usize) -> bool;

    fn get_bool(&self, i: usize) -> DeltaResult<bool>;
    fn get_i8(&self, i: usize) -> DeltaResult<i8>;
    fn get_i16(&self, i: usize) -> DeltaResult<i16>;
    fn get_i32(&self, i: usize) -> DeltaResult<i32>;
    fn get_i64(&self, i: usize) -> DeltaResult<i64>;
    fn get_f32(&self, i: usize) -> DeltaResult<f32>;
    fn get_f64(&self, i: usize) -> DeltaResult<f64>;
    fn get_binary(&self, i: usize) -> DeltaResult<&[u8]>;
    fn get_string(&self, i: usize) -> DeltaResult<&str>;
    fn get_struct(&self, i: usize) -> DeltaResult<&dyn Row>;
    // TODO: decimal
    // TODO: map, list
}

mod arrow {
    use arrow_array::Array as ArrowArray;
    use arrow_array::RecordBatch;

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

        /// Get the boolean value at the specified index.
        ///
        /// This will panic if the column is not boolean or if the index is out of bounds.
        fn get_bool(&self, i: usize) -> DeltaResult<Option<bool>> {
            if self.is_null(i) {
                Ok(None)
            } else {
                Ok(Some(
                    self.as_any()
                        .downcast_ref::<arrow_array::BooleanArray>()
                        .unwrap()
                        .value(i),
                ))
            }
        }
    }

    pub struct ArrowRow<'a> {
        schema: Schema,
        batch: &'a RecordBatch,
        row_index: usize,
    }

    impl Row for ArrowRow {
        fn schema(&self) -> &Schema {
            &self.schema
        }

        fn is_null(&self, i: usize) -> bool {
            self.batch.column(i).is_null(self.row_index)
        }

        fn get_bool(&self, i: usize) -> DeltaResult<bool> {
            self.batch.column(i).get_bool(self.row_index)
        }

        fn get_i8(&self, i: usize) -> DeltaResult<i8> {
            self.batch.column(i).get_i8(self.row_index)
        }

        fn get_i16(&self, i: usize) -> DeltaResult<i16> {
            self.batch.column(i).get_i16(self.row_index)
        }

        fn get_i32(&self, i: usize) -> DeltaResult<i32> {
            self.batch.column(i).get_i32(self.row_index)
        }

        fn get_i64(&self, i: usize) -> DeltaResult<i64> {
            self.batch.column(i).get_i64(self.row_index)
        }

        fn get_f32(&self, i: usize) -> DeltaResult<f32> {
            self.batch.column(i).get_f32(self.row_index)
        }

        fn get_f64(&self, i: usize) -> DeltaResult<f64> {
            self.batch.column(i).get_f64(self.row_index)
        }

        fn get_binary(&self, i: usize) -> DeltaResult<&[u8]> {
            self.batch.column(i).get_binary(self.row_index)
        }

        fn get_string(&self, i: usize) -> DeltaResult<&str> {
            self.batch.column(i).get_string(self.row_index)
        }

        fn get_struct(&self, i: usize) -> DeltaResult<&dyn Row> {
            self.batch.column(i).get_struct(self.row_index)
        }
    }
}

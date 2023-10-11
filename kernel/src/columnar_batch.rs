use crate::{
    schema::{DataType, Schema, StructField},
    DeltaResult,
};

pub trait ColumnarBatch {
    type Column: ColumnVector;

    /// Get the schema of the batch.
    fn schema(&self) -> DeltaResult<Schema>;

    /// Get the column at the specified index.
    fn column(&self, index: usize) -> DeltaResult<Self::Column>;

    /// Number of rows in the batch.
    fn size(&self) -> usize;

    /// Insert a column at the specified index.
    fn with_column(
        &self,
        index: usize,
        field: StructField,
        column: Self::Column,
    ) -> DeltaResult<Self>
    where
        Self: Sized;

    /// Remove the column at the specified index.
    fn with_deleted_column_at(&self, index: usize) -> DeltaResult<Self>
    where
        Self: Sized;

    // fn with_schema(&self, schema: Schema) -> DeltaResult<Self>
    // where
    //     Self: Sized;

    fn slice(&self, offset: usize, length: usize) -> DeltaResult<Self>
    where
        Self: Sized;

    fn rows(&self) -> Box<dyn Iterator<Item = Box<dyn Row<Column = Self::Column>>>>;
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
    // TODO: add other primitive types
    fn get_struct(&self, i: usize) -> DeltaResult<Option<Box<dyn Row<Column = Self>>>>;
    fn get_array(&self, i: usize) -> DeltaResult<Option<Box<dyn ArrayValue<Column = Self>>>>;
    fn get_map(&self, i: usize) -> DeltaResult<Option<Box<dyn MapValue<Column = Self>>>>;
}

pub trait Row {
    type Column: ColumnVector;

    fn schema(&self) -> DeltaResult<Schema>;
    fn is_null(&self, i: usize) -> bool;
    fn get_i32(&self, i: usize) -> DeltaResult<Option<i32>>;
    fn get_string(&self, i: usize) -> DeltaResult<Option<&str>>;
    // TODO: add other primitive types
    fn get_struct(&self, i: usize) -> DeltaResult<Option<Box<dyn Row<Column = Self::Column>>>>;
    fn get_array(
        &self,
        i: usize,
    ) -> DeltaResult<Option<Box<dyn ArrayValue<Column = Self::Column>>>>;
    fn get_map(&self, i: usize) -> DeltaResult<Option<Box<dyn MapValue<Column = Self::Column>>>>;
}

// Based on: https://github.com/delta-io/delta/pull/2087

pub trait ArrayValue {
    type Column: ColumnVector;

    /// Return the number of elements in the array
    fn size(&self) -> usize;

    /// Get the elements in the array
    fn elements(&self) -> Self::Column;
}

pub trait MapValue {
    type Column: ColumnVector;

    /// Return the number of elements in the map
    fn size(&self) -> usize;

    /// Get the keys in the map
    fn keys(&self) -> Self::Column;

    /// Get the values in the map
    fn values(&self) -> Self::Column;
}

pub mod arrow {
    use std::sync::Arc;

    use arrow_array::cast::AsArray;
    use arrow_array::types::Int32Type;
    use arrow_array::Array as ArrowArray;
    use arrow_array::RecordBatch;
    use arrow_array::StructArray;
    use arrow_schema::{DataType as ArrowDataType, Schema as ArrowSchema};

    use crate::Error;

    use super::*;

    impl ColumnarBatch for RecordBatch {
        type Column = Arc<dyn ArrowArray>;

        fn schema(&self) -> DeltaResult<Schema> {
            Ok(self.schema().as_ref().try_into()?)
        }

        fn column(&self, index: usize) -> DeltaResult<Self::Column> {
            if index < self.num_columns() {
                Ok(self.column(index).clone())
            } else {
                Err(Error::Generic(format!(
                    "Column index {} out of bounds",
                    index
                )))
            }
        }

        fn size(&self) -> usize {
            self.num_rows()
        }

        fn with_column(
            &self,
            index: usize,
            field: StructField,
            column: Self::Column,
        ) -> DeltaResult<Self>
        where
            Self: Sized,
        {
            todo!()
        }

        fn with_deleted_column_at(&self, index: usize) -> DeltaResult<Self>
        where
            Self: Sized,
        {
            let indices = (0..self.num_columns())
                .filter(|i| *i != index)
                .collect::<Vec<_>>();
            RecordBatch::project(&self, &indices).map_err(|err| Error::Arrow(err))
        }

        fn slice(&self, offset: usize, length: usize) -> DeltaResult<Self>
        where
            Self: Sized,
        {
            Ok(RecordBatch::slice(self, offset, length))
        }

        fn rows(&self) -> Box<dyn Iterator<Item = Box<dyn Row<Column = Self::Column>>>> {
            let batch = self.clone();
            Box::new((0..self.size()).into_iter().map(move |i| {
                let row = Box::new(ArrowRow {
                    batch: batch.clone(),
                    row_index: i,
                });
                row as Box<dyn Row<Column = Self::Column>>
            }))
        }
    }

    impl ColumnVector for Arc<dyn ArrowArray> {
        fn data_type(&self) -> DeltaResult<DataType> {
            Ok(self.as_ref().data_type().try_into()?)
        }

        fn size(&self) -> usize {
            self.len()
        }

        fn is_null(&self, i: usize) -> bool {
            self.as_ref().is_null(i)
        }

        /// Get the i32 value at the specified index.
        ///
        /// This will panic if the column is not boolean or if the index is out of bounds.
        fn get_i32(&self, i: usize) -> DeltaResult<Option<i32>> {
            if self.as_ref().is_null(i) {
                Ok(None)
            } else {
                Ok(Some(self.as_primitive::<Int32Type>().value(i)))
            }
        }

        /// Get the string value at the specified index.
        ///
        /// This will panic if the column is not string or if the index is out of bounds.
        fn get_string(&self, i: usize) -> DeltaResult<Option<&str>> {
            if self.as_ref().is_null(i) {
                Ok(None)
            } else {
                match self.as_ref().data_type() {
                    ArrowDataType::Utf8 => Ok(Some(self.as_string::<i32>().value(i))),
                    ArrowDataType::LargeUtf8 => Ok(Some(self.as_string::<i64>().value(i))),
                    _ => panic!("get_string called on non-string column"),
                }
            }
        }

        /// Get the struct value at the specified index.
        ///
        /// This will panic if the column is not struct or if the index is out of bounds.
        fn get_struct(&self, i: usize) -> DeltaResult<Option<Box<dyn Row<Column = Self>>>> {
            if self.as_ref().is_null(i) {
                Ok(None)
            } else {
                let batch = self
                    .as_struct_opt()
                    .expect("get_struct called on non-struct column")
                    .clone();
                let row = ArrowRow {
                    batch,
                    row_index: i,
                };
                Ok(Some(Box::new(row)))
            }
        }

        /// Get the array value at the specified index.
        ///
        /// This will panic if the column is not array or if the index is out of bounds.
        fn get_array(&self, i: usize) -> DeltaResult<Option<Box<dyn ArrayValue<Column = Self>>>> {
            if self.as_ref().is_null(i) {
                Ok(None)
            } else {
                let sub_array = match self.as_ref().data_type() {
                    ArrowDataType::List(_) => self.as_list_opt::<i32>().unwrap().value(i),
                    ArrowDataType::LargeList(_) => self.as_list_opt::<i32>().unwrap().value(i),
                    _ => panic!("get_array called on non-array column"),
                };
                Ok(Some(Box::new(ArrowArraySlice(sub_array))))
            }
        }

        fn get_map(&self, i: usize) -> DeltaResult<Option<Box<dyn MapValue<Column = Self>>>> {
            if self.as_ref().is_null(i) {
                Ok(None)
            } else {
                let arr = self.as_map().value(i);
                let map_array = ArrowMapValue {
                    keys: arr.column(0).clone(),
                    values: arr.column(1).clone(),
                };
                Ok(Some(Box::new(map_array)))
            }
        }
    }

    pub trait ArrowTabular {
        fn schema(&self) -> Arc<ArrowSchema>;
        fn column(&self, index: usize) -> &Arc<dyn ArrowArray>;
    }

    impl ArrowTabular for RecordBatch {
        fn schema(&self) -> Arc<ArrowSchema> {
            self.schema()
        }

        fn column(&self, index: usize) -> &Arc<dyn ArrowArray> {
            &self.column(index)
        }
    }

    impl ArrowTabular for StructArray {
        fn schema(&self) -> Arc<ArrowSchema> {
            Arc::new(ArrowSchema::new(self.fields().clone()))
        }

        fn column(&self, index: usize) -> &Arc<dyn ArrowArray> {
            &self.column(index)
        }
    }

    /// A reference to a row in a RecordBatch or StructArray.
    #[derive(Debug)]
    pub struct ArrowRow<T: ArrowTabular> {
        batch: T,
        row_index: usize,
    }

    impl<T: ArrowTabular> Row for ArrowRow<T> {
        type Column = Arc<dyn ArrowArray>;

        fn schema(&self) -> DeltaResult<Schema> {
            ArrowTabular::schema(&self.batch)
                .try_into()
                .map_err(|err| Error::Arrow(err))
        }

        fn is_null(&self, i: usize) -> bool {
            ArrowArray::is_null(self.batch.column(i), self.row_index)
        }

        fn get_i32(&self, i: usize) -> DeltaResult<Option<i32>> {
            self.batch.column(i).get_i32(self.row_index)
        }

        fn get_string(&self, i: usize) -> DeltaResult<Option<&str>> {
            self.batch.column(i).get_string(self.row_index)
        }

        fn get_struct(&self, i: usize) -> DeltaResult<Option<Box<dyn Row<Column = Self::Column>>>> {
            self.batch.column(i).get_struct(self.row_index)
        }

        fn get_array(
            &self,
            i: usize,
        ) -> DeltaResult<Option<Box<dyn ArrayValue<Column = Self::Column>>>> {
            self.batch.column(i).get_array(self.row_index)
        }

        fn get_map(
            &self,
            i: usize,
        ) -> DeltaResult<Option<Box<dyn MapValue<Column = Self::Column>>>> {
            self.batch.column(i).get_map(self.row_index)
        }
    }

    #[derive(Debug)]
    pub struct ArrowArraySlice(Arc<dyn ArrowArray>);

    impl ArrayValue for ArrowArraySlice {
        type Column = Arc<dyn ArrowArray>;

        fn size(&self) -> usize {
            self.0.len()
        }

        fn elements(&self) -> Self::Column {
            self.0.clone()
        }
    }

    #[derive(Debug)]
    pub struct ArrowMapValue {
        keys: Arc<dyn ArrowArray>,
        values: Arc<dyn ArrowArray>,
    }

    impl MapValue for ArrowMapValue {
        type Column = Arc<dyn ArrowArray>;

        fn size(&self) -> usize {
            self.keys.len()
        }

        fn keys(&self) -> Self::Column {
            self.keys.clone()
        }

        fn values(&self) -> Self::Column {
            self.values.clone()
        }
    }

    #[cfg(test)]
    mod tests {
        use arrow_array::{Int32Array, StringArray};
        use arrow_schema::Field as ArrowField;

        use crate::schema::{PrimitiveType, StructType};

        use super::*;

        #[test]
        fn test_recordbatch_basics() {
            let schema = ArrowSchema::new(vec![
                ArrowField::new("a", ArrowDataType::Int32, true),
                ArrowField::new("b", ArrowDataType::Utf8, true),
            ]);

            let batch = RecordBatch::try_new(
                Arc::new(schema),
                vec![
                    Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
                    Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
                ],
            )
            .unwrap();

            // We use ColumnarBatch::{method}() syntax to make sure we dispatch
            // to ColumnarBatch methods and not RecordBatch methods.
            assert_eq!(ColumnarBatch::size(&batch), 3);

            let schema = ColumnarBatch::schema(&batch).unwrap();
            let expected_schema = StructType::new(vec![
                StructField::new("a", DataType::Primitive(PrimitiveType::Integer), true),
                StructField::new("b", DataType::Primitive(PrimitiveType::String), true),
            ]);
            assert_eq!(schema, expected_schema);

            let column = ColumnarBatch::column(&batch, 0).unwrap();
            assert_eq!(ColumnVector::size(&column), 3);
            assert_eq!(
                ColumnVector::data_type(&column).unwrap(),
                DataType::Primitive(PrimitiveType::Integer)
            );
            assert_eq!(ColumnVector::get_i32(&column, 0).unwrap(), Some(1));
            assert_eq!(ColumnVector::get_i32(&column, 1).unwrap(), None);
            assert_eq!(ColumnVector::get_i32(&column, 2).unwrap(), Some(3));
            assert!(ColumnVector::is_null(&column, 1));
            assert!(!ColumnVector::is_null(&column, 0));

            let column = ColumnarBatch::column(&batch, 1).unwrap();
            assert_eq!(ColumnVector::size(&column), 3);
            assert_eq!(
                ColumnVector::data_type(&column).unwrap(),
                DataType::Primitive(PrimitiveType::String)
            );
            assert_eq!(ColumnVector::get_string(&column, 0).unwrap(), Some("a"));
            assert_eq!(ColumnVector::get_string(&column, 1).unwrap(), Some("b"));
            assert_eq!(ColumnVector::get_string(&column, 2).unwrap(), None);
            assert!(ColumnVector::is_null(&column, 2));
            assert!(!ColumnVector::is_null(&column, 0));

            assert!(ColumnarBatch::column(&batch, 2).is_err());

            let mut rows = ColumnarBatch::rows(&batch);

            let row = rows.next().unwrap();
            assert!(!row.is_null(0));
            assert!(!row.is_null(1));
            assert_eq!(row.get_i32(0).unwrap(), Some(1));
            assert_eq!(row.get_string(1).unwrap(), Some("a"));

            let row = rows.next().unwrap();
            assert!(row.is_null(0));
            assert!(!row.is_null(1));
            assert_eq!(row.get_i32(0).unwrap(), None);
            assert_eq!(row.get_string(1).unwrap(), Some("b"));

            let row = rows.next().unwrap();
            assert!(!row.is_null(0));
            assert!(row.is_null(1));
            assert_eq!(row.get_i32(0).unwrap(), Some(3));
            assert_eq!(row.get_string(1).unwrap(), None);

            assert!(rows.next().is_none());
        }

        #[test]
        fn test_recordbatch_slice() {
            todo!()
        }

        #[test]
        fn test_recordbatch_modification() {
            todo!()
        }

        #[test]
        fn test_struct_array() {
            todo!()
        }

        #[test]
        fn test_array_array() {
            todo!()
        }

        #[test]
        fn test_map_array() {
            todo!()
        }
    }
}

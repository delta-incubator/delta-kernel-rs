use crate::{
    schema::{DataType, Schema, StructField},
    DeltaResult,
};

// TODO: Should all these methods perform bounds checking? Should they return
// errors for out of bounds?

/// A columnar batch of data.
///
/// There should be a single implementation of this trait and the associated
/// [ColumnVector] trait. This trait is used to abstract over different in-memory
/// columnar formats.
///
/// An implementation is provided for [arrow_array::RecordBatch] in the [arrow]
/// sub-module. Engines may provide their own implementations optimized for their
/// in-memory format.
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
        field: &StructField,
        column: Self::Column,
    ) -> DeltaResult<Self>
    where
        Self: Sized;

    /// Remove the column at the specified index.
    fn with_deleted_column_at(&self, index: usize) -> DeltaResult<Self>
    where
        Self: Sized;

    /// Get a new columnar batch containing a slice of the rows in this batch.
    fn slice(&self, offset: usize, length: usize) -> DeltaResult<Self>
    where
        Self: Sized;

    /// Iterate over the rows in the batch.
    fn rows(&self) -> Box<dyn Iterator<Item = Box<dyn Row<Column = Self::Column>>>>;
}

/// A column in a [ColumnarBatch].
pub trait ColumnVector {
    /// Get the data type of the column.
    fn data_type(&self) -> DeltaResult<DataType>;

    /// Get the number of elements in the column.
    fn size(&self) -> usize;

    /// Check if the element at the specified index is null.
    fn is_null(&self, i: usize) -> bool;

    // TODO: should these methods type check?

    /// Get the i32 value at the specified index.
    fn get_i32(&self, i: usize) -> DeltaResult<Option<i32>>;

    /// Get the string value at the specified index.
    fn get_string(&self, i: usize) -> DeltaResult<Option<&str>>;

    // TODO: add other primitive types

    /// Get the struct value at the specified index.
    fn get_struct(&self, i: usize) -> DeltaResult<Option<Box<dyn Row<Column = Self>>>>;

    /// Get the array value at the specified index.
    fn get_array(&self, i: usize) -> DeltaResult<Option<Box<dyn ArrayValue<Column = Self>>>>;

    /// Get the map value at the specified index.
    fn get_map(&self, i: usize) -> DeltaResult<Option<Box<dyn MapValue<Column = Self>>>>;
}

/// A row reference in a [ColumnarBatch].
pub trait Row {
    /// The column implementation associated with this Row.
    type Column: ColumnVector;

    /// Get the schema of the row.
    fn schema(&self) -> DeltaResult<Schema>;

    /// Check if the element at the specified column index is null.
    fn is_null(&self, i: usize) -> bool;

    /// Get the i32 value in the specified column index.
    fn get_i32(&self, i: usize) -> DeltaResult<Option<i32>>;

    /// Get the string value in the specified column index.
    fn get_string(&self, i: usize) -> DeltaResult<Option<&str>>;

    // TODO: add other primitive types

    /// Get the struct value in the specified column index.
    fn get_struct(&self, i: usize) -> DeltaResult<Option<Box<dyn Row<Column = Self::Column>>>>;

    /// Get the array value in the specified column index.
    fn get_array(
        &self,
        i: usize,
    ) -> DeltaResult<Option<Box<dyn ArrayValue<Column = Self::Column>>>>;

    /// Get the map value in the specified column index.
    fn get_map(&self, i: usize) -> DeltaResult<Option<Box<dyn MapValue<Column = Self::Column>>>>;
}

/// An array value in a [ColumnarBatch].
pub trait ArrayValue {
    /// The column implementation associated with this ArrayValue. This is returned
    /// by the [elements] method.
    type Column: ColumnVector;

    /// Return the number of elements in the array
    fn size(&self) -> usize;

    /// Get the elements in the array
    fn elements(&self) -> Self::Column;
}

pub trait MapValue {
    /// The column implementation associated with this MapValue. This is returned
    /// by the [keys] and [values] methods.
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
    use arrow_array::{Array as ArrowArray, RecordBatch, StructArray};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};

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
            field: &StructField,
            column: Self::Column,
        ) -> DeltaResult<Self>
        where
            Self: Sized,
        {
            let arrow_field = Arc::new(ArrowField::try_from(field)?);
            let mut fields = self.schema().as_ref().clone().fields().to_vec();
            fields.insert(index, arrow_field);
            let schema = Arc::new(ArrowSchema::new(fields));
            let mut columns = self.columns().to_vec();
            columns.insert(index, column);
            RecordBatch::try_new(schema, columns).map_err(Error::Arrow)
        }

        fn with_deleted_column_at(&self, index: usize) -> DeltaResult<Self>
        where
            Self: Sized,
        {
            let indices = (0..self.num_columns())
                .filter(|i| *i != index)
                .collect::<Vec<_>>();
            RecordBatch::project(self, &indices).map_err(Error::Arrow)
        }

        fn slice(&self, offset: usize, length: usize) -> DeltaResult<Self>
        where
            Self: Sized,
        {
            Ok(RecordBatch::slice(self, offset, length))
        }

        fn rows(&self) -> Box<dyn Iterator<Item = Box<dyn Row<Column = Self::Column>>>> {
            let batch = self.clone();
            Box::new((0..self.size()).map(move |i| {
                let row: Box<dyn Row<Column = Self::Column>> = Box::new(ArrowRow {
                    batch: batch.clone(),
                    row_index: i,
                });
                row
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
            self.column(index)
        }
    }

    impl ArrowTabular for StructArray {
        fn schema(&self) -> Arc<ArrowSchema> {
            Arc::new(ArrowSchema::new(self.fields().clone()))
        }

        fn column(&self, index: usize) -> &Arc<dyn ArrowArray> {
            self.column(index)
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
                .map_err(Error::Arrow)
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
        use arrow_array::{Int32Array, ListArray, MapArray, StringArray};
        use arrow_schema::Field as ArrowField;

        use crate::schema::{ArrayType, MapType, PrimitiveType, StructType};

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

            let batch = ColumnarBatch::slice(&batch, 1, 2).unwrap();
            assert_eq!(ColumnarBatch::size(&batch), 2);

            let column = ColumnarBatch::column(&batch, 0).unwrap();
            assert_eq!(ColumnVector::size(&column), 2);
            assert_eq!(ColumnVector::get_i32(&column, 0).unwrap(), None);
            assert_eq!(ColumnVector::get_i32(&column, 1).unwrap(), Some(3));

            let column = ColumnarBatch::column(&batch, 1).unwrap();
            assert_eq!(ColumnVector::size(&column), 2);
            assert_eq!(ColumnVector::get_string(&column, 0).unwrap(), Some("b"));
            assert_eq!(ColumnVector::get_string(&column, 1).unwrap(), None);

            assert!(ColumnarBatch::column(&batch, 2).is_err());
        }

        #[test]
        fn test_recordbatch_modification() {
            let schema = ArrowSchema::new(vec![ArrowField::new("a", ArrowDataType::Int32, true)]);

            let batch = RecordBatch::try_new(
                Arc::new(schema),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )
            .unwrap();

            // Can add a new column
            let new_column = Arc::new(Int32Array::from(vec![4, 5, 6]));
            let batch = ColumnarBatch::with_column(
                &batch,
                0,
                &StructField::new("b", DataType::Primitive(PrimitiveType::Integer), true),
                new_column,
            )
            .unwrap();

            // Can delete a column
            let batch = ColumnarBatch::with_deleted_column_at(&batch, 1).unwrap();
            assert_eq!(
                ColumnarBatch::schema(&batch).unwrap(),
                StructType::new(vec![StructField::new(
                    "b",
                    DataType::Primitive(PrimitiveType::Integer),
                    true
                ),])
            );
        }

        #[test]
        fn test_struct_array() {
            let inner_fields = vec![
                ArrowField::new("a", ArrowDataType::Int32, true),
                ArrowField::new("b", ArrowDataType::Utf8, true),
            ];
            let schema = ArrowSchema::new(vec![ArrowField::new(
                "struct",
                ArrowDataType::Struct(inner_fields.clone().into()),
                true,
            )]);

            let batch = RecordBatch::try_new(
                Arc::new(schema),
                vec![Arc::new(StructArray::new(
                    inner_fields.into(),
                    vec![
                        Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
                        Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
                    ],
                    None,
                ))],
            )
            .unwrap();

            // Get as a struct column.
            let struct_column = ColumnarBatch::column(&batch, 0).unwrap();
            assert_eq!(ColumnVector::size(&struct_column), 3);
            assert_eq!(
                ColumnVector::data_type(&struct_column).unwrap(),
                DataType::Struct(Box::new(StructType::new(vec![
                    StructField::new("a", DataType::Primitive(PrimitiveType::Integer), true),
                    StructField::new("b", DataType::Primitive(PrimitiveType::String), true),
                ])))
            );
            let struct_row = ColumnVector::get_struct(&struct_column, 0)
                .unwrap()
                .unwrap();
            assert_eq!(
                DataType::Struct(Box::new(struct_row.schema().unwrap())),
                ColumnVector::data_type(&struct_column).unwrap()
            );
            assert_eq!(struct_row.get_i32(0).unwrap(), Some(1));
            assert_eq!(struct_row.get_string(1).unwrap(), Some("a"));

            // Get as rows.
            let mut rows = ColumnarBatch::rows(&batch);
            let row = rows.next().unwrap();
            let struct_row = row.get_struct(0).unwrap().unwrap();
            assert_eq!(struct_row.get_i32(0).unwrap(), Some(1));
            assert_eq!(struct_row.get_string(1).unwrap(), Some("a"));
        }

        #[test]
        fn test_array_array() {
            let arrow_schema = ArrowSchema::new(vec![ArrowField::new(
                "array",
                ArrowDataType::List(Arc::new(ArrowField::new(
                    "item",
                    ArrowDataType::Int32,
                    true,
                ))),
                true,
            )]);

            let data = vec![
                Some(vec![Some(1), Some(2), Some(3)]),
                Some(vec![Some(4), Some(5)]),
                Some(vec![Some(6), Some(7)]),
            ];
            let batch = RecordBatch::try_new(
                Arc::new(arrow_schema),
                vec![Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
                    data,
                ))],
            )
            .unwrap();

            // Get as a column
            let array_column = ColumnarBatch::column(&batch, 0).unwrap();
            assert_eq!(ColumnVector::size(&array_column), 3);
            assert_eq!(
                ColumnVector::data_type(&array_column).unwrap(),
                DataType::Array(Box::new(ArrayType::new(
                    DataType::Primitive(PrimitiveType::Integer),
                    true
                )))
            );
            let inner0 = ColumnVector::get_array(&array_column, 0).unwrap().unwrap();
            assert_eq!(inner0.size(), 3);
            assert_eq!(ColumnVector::size(&inner0.elements()), 3);
            assert_eq!(inner0.elements().get_i32(0).unwrap(), Some(1));
            assert_eq!(inner0.elements().get_i32(2).unwrap(), Some(3));
            let inner2 = ColumnVector::get_array(&array_column, 2).unwrap().unwrap();
            assert_eq!(inner2.size(), 2);
            assert_eq!(ColumnVector::size(&inner2.elements()), 2);
            assert_eq!(inner2.elements().get_i32(0).unwrap(), Some(6));
            assert_eq!(inner2.elements().get_i32(1).unwrap(), Some(7));

            // Get as rows
            let mut rows = ColumnarBatch::rows(&batch);
            let row = rows.next().unwrap();
            let array_row = row.get_array(0).unwrap().unwrap();
            assert_eq!(array_row.size(), 3);
            assert_eq!(array_row.elements().get_i32(0).unwrap(), Some(1));
            assert_eq!(array_row.elements().get_i32(2).unwrap(), Some(3));
            let row = rows.next().unwrap();
            let array_row = row.get_array(0).unwrap().unwrap();
            assert_eq!(array_row.size(), 2);
            assert_eq!(array_row.elements().get_i32(0).unwrap(), Some(4));
            assert_eq!(array_row.elements().get_i32(1).unwrap(), Some(5));
        }

        #[test]
        fn test_map_array() {
            let arrow_schema = ArrowSchema::new(vec![ArrowField::new_map(
                "map",
                "entries",
                Arc::new(ArrowField::new("keys", ArrowDataType::Utf8, false)),
                Arc::new(ArrowField::new("values", ArrowDataType::Int32, false)),
                false,
                true,
            )]);

            let array = MapArray::new_from_strings(
                vec!["key1", "key2", "key1", "key3"].into_iter(),
                &Int32Array::from_iter_values(0..4),
                &[0, 2, 4],
            )
            .unwrap();

            let batch =
                RecordBatch::try_new(Arc::new(arrow_schema), vec![Arc::new(array)]).unwrap();

            // Get as a column
            let map_column = ColumnarBatch::column(&batch, 0).unwrap();
            assert_eq!(ColumnVector::size(&map_column), 2);
            assert_eq!(
                ColumnVector::data_type(&map_column).unwrap(),
                DataType::Map(Box::new(MapType::new(
                    DataType::Primitive(PrimitiveType::String),
                    DataType::Primitive(PrimitiveType::Integer),
                    false
                )))
            );
            let map_value = ColumnVector::get_map(&map_column, 0).unwrap().unwrap();
            assert_eq!(map_value.size(), 2);
            assert_eq!(map_value.keys().get_string(0).unwrap(), Some("key1"));
            assert_eq!(map_value.keys().get_string(1).unwrap(), Some("key2"));
            assert_eq!(map_value.values().get_i32(0).unwrap(), Some(0));
            assert_eq!(map_value.values().get_i32(1).unwrap(), Some(1));
            let map_value = ColumnVector::get_map(&map_column, 1).unwrap().unwrap();
            assert_eq!(map_value.size(), 2);
            assert_eq!(map_value.keys().get_string(0).unwrap(), Some("key1"));
            assert_eq!(map_value.keys().get_string(1).unwrap(), Some("key3"));
            assert_eq!(map_value.values().get_i32(0).unwrap(), Some(2));
            assert_eq!(map_value.values().get_i32(1).unwrap(), Some(3));

            // Get as rows
            let mut rows = ColumnarBatch::rows(&batch);
            let row = rows.next().unwrap();
            let map_row = row.get_map(0).unwrap().unwrap();
            assert_eq!(map_row.size(), 2);
            assert_eq!(map_row.keys().get_string(0).unwrap(), Some("key1"));
            assert_eq!(map_row.keys().get_string(1).unwrap(), Some("key2"));
            assert_eq!(map_row.values().get_i32(0).unwrap(), Some(0));
            assert_eq!(map_row.values().get_i32(1).unwrap(), Some(1));
        }
    }
}

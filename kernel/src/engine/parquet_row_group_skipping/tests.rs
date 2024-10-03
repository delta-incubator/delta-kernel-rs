use super::*;
use crate::Expression;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use std::fs::File;

#[test]
fn test_get_stat_values() {
    let file = File::open("./tests/data/all_primitive_types/part-00000-b5953e03-5673-45f9-9ac5-78bedb3a17fe-c000.snappy.parquet").unwrap();
    let metadata = ArrowReaderMetadata::load(&file, Default::default()).unwrap();

    // The expression doesn't matter -- it just needs to mention all the columns we care about.
    let columns = Expression::and_from(vec![
        Expression::column("utf8"),
        Expression::column("int64"),
        Expression::column("int32"),
        Expression::column("int16"),
        Expression::column("int8"),
        Expression::column("float32"),
        Expression::column("float64"),
        Expression::column("bool"),
        Expression::column("binary"),
        Expression::column("decimal32"),
        Expression::column("decimal64"),
        Expression::column("decimal128"),
        Expression::column("date32"),
        Expression::column("timestamp"),
        Expression::column("timestamp_ntz"),
    ]);
    let filter = RowGroupFilter::new(metadata.metadata().row_group(0), &columns);

    assert_eq!(
        filter.get_min_stat_value(&ColumnPath::from("utf8"), &DataType::STRING),
        Some("0".into())
    );

    assert_eq!(
        filter.get_min_stat_value(&ColumnPath::from("int64"), &DataType::LONG),
        Some(0i64.into())
    );

    assert_eq!(
        filter.get_min_stat_value(&ColumnPath::from("int32"), &DataType::INTEGER),
        Some(0i32.into())
    );

    assert_eq!(
        filter.get_min_stat_value(&ColumnPath::from("int16"), &DataType::SHORT),
        Some(0i16.into())
    );

    assert_eq!(
        filter.get_min_stat_value(&ColumnPath::from("int8"), &DataType::BYTE),
        Some(0i8.into())
    );

    assert_eq!(
        filter.get_min_stat_value(&ColumnPath::from("float64"), &DataType::DOUBLE),
        Some(0f64.into())
    );

    assert_eq!(
        filter.get_min_stat_value(&ColumnPath::from("float32"), &DataType::FLOAT),
        Some(0f32.into())
    );

    assert_eq!(
        filter.get_min_stat_value(&ColumnPath::from("bool"), &DataType::BOOLEAN),
        Some(false.into())
    );

    assert_eq!(
        filter.get_min_stat_value(&ColumnPath::from("binary"), &DataType::BINARY),
        Some([].as_slice().into())
    );

    assert_eq!(
        filter.get_min_stat_value(
            &ColumnPath::from("decimal32"),
            &DataType::decimal(8, 3).unwrap()
        ),
        Some(Scalar::Decimal(10000, 8, 3).into())
    );

    assert_eq!(
        filter.get_min_stat_value(
            &ColumnPath::from("decimal64"),
            &DataType::decimal(16, 3).unwrap()
        ),
        Some(Scalar::Decimal(10000, 16, 3).into())
    );

    assert_eq!(
        filter.get_min_stat_value(
            &ColumnPath::from("decimal128"),
            &DataType::decimal(32, 3).unwrap()
        ),
        Some(Scalar::Decimal(10000, 32, 3).into())
    );

    assert_eq!(
        filter.get_min_stat_value(&ColumnPath::from("timestamp"), &DataType::TIMESTAMP),
        None // Timestamp defaults to 96-bit, which doesn't get stats
    );

    assert_eq!(
        filter.get_min_stat_value(&ColumnPath::from("timestamp_ntz"), &DataType::TIMESTAMP_NTZ),
        Some(
            PrimitiveType::TimestampNtz
                .parse_scalar("1970-01-01 00:00:00.000000")
                .unwrap()
        ),
    );

    assert_eq!(
        filter.get_max_stat_value(&ColumnPath::from("utf8"), &DataType::STRING),
        Some("4".into())
    );

    assert_eq!(
        filter.get_max_stat_value(&ColumnPath::from("int64"), &DataType::LONG),
        Some(4i64.into())
    );

    assert_eq!(
        filter.get_max_stat_value(&ColumnPath::from("int32"), &DataType::INTEGER),
        Some(4.into())
    );

    assert_eq!(
        filter.get_max_stat_value(&ColumnPath::from("int16"), &DataType::SHORT),
        Some(4i16.into())
    );

    assert_eq!(
        filter.get_max_stat_value(&ColumnPath::from("int8"), &DataType::BYTE),
        Some(4i8.into())
    );

    assert_eq!(
        filter.get_max_stat_value(&ColumnPath::from("float64"), &DataType::DOUBLE),
        Some(4f64.into())
    );

    assert_eq!(
        filter.get_max_stat_value(&ColumnPath::from("float32"), &DataType::FLOAT),
        Some(4f32.into())
    );

    assert_eq!(
        filter.get_max_stat_value(&ColumnPath::from("bool"), &DataType::BOOLEAN),
        Some(true.into())
    );

    assert_eq!(
        filter.get_max_stat_value(&ColumnPath::from("binary"), &DataType::BINARY),
        Some([0, 0, 0, 0].as_slice().into())
    );

    assert_eq!(
        filter.get_max_stat_value(
            &ColumnPath::from("decimal32"),
            &DataType::decimal(8, 3).unwrap()
        ),
        Some(Scalar::Decimal(14000, 8, 3).into())
    );

    assert_eq!(
        filter.get_max_stat_value(
            &ColumnPath::from("decimal64"),
            &DataType::decimal(16, 3).unwrap()
        ),
        Some(Scalar::Decimal(14000, 16, 3).into())
    );

    assert_eq!(
        filter.get_max_stat_value(
            &ColumnPath::from("decimal128"),
            &DataType::decimal(32, 3).unwrap()
        ),
        Some(Scalar::Decimal(14000, 32, 3).into())
    );

    assert_eq!(
        filter.get_max_stat_value(&ColumnPath::from("timestamp"), &DataType::TIMESTAMP),
        None // Timestamp defaults to 96-bit, which doesn't get stats
    );

    assert_eq!(
        filter.get_max_stat_value(&ColumnPath::from("timestamp_ntz"), &DataType::TIMESTAMP_NTZ),
        Some(
            PrimitiveType::TimestampNtz
                .parse_scalar("1970-01-01 04:00:00.000000")
                .unwrap()
        ),
    );
}

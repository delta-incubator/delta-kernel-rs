//! Utility functions used for testing ffi code

use std::{ops::Not, sync::Arc};

use crate::{expressions::SharedExpression, handle::Handle};
use delta_kernel::{
    expressions::{ArrayData, BinaryOperator, Expression, Scalar, StructData},
    schema::{ArrayType, DataType, StructField, StructType},
};

/// Constructs a kernel expression that is passed back as a SharedExpression handle. The expected
/// output expression can be found in `ffi/tests/test_expression_visitor/expected.txt`.
///
/// # Safety
/// The caller is responsible for freeing the retured memory, either by calling
/// [`free_kernel_predicate`], or [`Handle::drop_handle`]
#[no_mangle]
pub unsafe extern "C" fn get_testing_kernel_expression() -> Handle<SharedExpression> {
    use Expression as Expr;

    let array_type = ArrayType::new(
        DataType::Primitive(delta_kernel::schema::PrimitiveType::Short),
        false,
    );
    let array_data = ArrayData::new(array_type.clone(), vec![Scalar::Short(5), Scalar::Short(0)]);

    let nested_fields = vec![
        StructField::new("a", DataType::INTEGER, false),
        StructField::new("b", array_type, false),
    ];
    let nested_values = vec![Scalar::Integer(500), Scalar::Array(array_data.clone())];
    let nested_struct = StructData::try_new(nested_fields.clone(), nested_values).unwrap();
    let nested_struct_type = StructType::new(nested_fields);

    let top_level_struct = StructData::try_new(
        vec![StructField::new(
            "top",
            DataType::Struct(Box::new(nested_struct_type)),
            true,
        )],
        vec![Scalar::Struct(nested_struct)],
    )
    .unwrap();

    let mut sub_exprs = vec![
        Expr::literal(i8::MAX),
        Expr::literal(i8::MIN),
        Expr::literal(f32::MAX),
        Expr::literal(f32::MIN),
        Expr::literal(f64::MAX),
        Expr::literal(f64::MIN),
        Expr::literal(i32::MAX),
        Expr::literal(i32::MIN),
        Expr::literal(i64::MAX),
        Expr::literal(i64::MIN),
        Expr::literal("hello expressions"),
        Expr::literal(true),
        Expr::literal(false),
        Scalar::Timestamp(50).into(),
        Scalar::TimestampNtz(100).into(),
        Scalar::Date(32).into(),
        Scalar::Binary(0x0000deadbeefcafeu64.to_be_bytes().to_vec()).into(),
        // Both the most and least significant u64 of the Decimal value will be 1
        Scalar::Decimal((1 << 64) + 1, 2, 3).into(),
        Expr::null_literal(DataType::SHORT),
        Scalar::Struct(top_level_struct).into(),
        Scalar::Array(array_data).into(),
        Expr::struct_from(vec![Expr::or_from(vec![
            Scalar::Integer(5).into(),
            Scalar::Long(20).into(),
        ])]),
        Expr::not(Expr::is_null(Expr::column("col"))),
    ];
    sub_exprs.extend(
        [
            BinaryOperator::In,
            BinaryOperator::Plus,
            BinaryOperator::Minus,
            BinaryOperator::Equal,
            BinaryOperator::NotEqual,
            BinaryOperator::NotIn,
            BinaryOperator::Divide,
            BinaryOperator::Multiply,
            BinaryOperator::LessThan,
            BinaryOperator::LessThanOrEqual,
            BinaryOperator::GreaterThan,
            BinaryOperator::GreaterThanOrEqual,
            BinaryOperator::Distinct,
        ]
        .iter()
        .map(|op| Expr::binary(*op, Scalar::Integer(0), Scalar::Long(0))),
    );

    Arc::new(Expr::and_from(sub_exprs)).into()
}

//! Defines [`EngineExpressionVisitor`]. This is a visitor  that can be used to convert the kernel's [`Expression`] to an engine's expression format.
use crate::expressions::SharedExpression;
use std::ffi::c_void;

use crate::{handle::Handle, KernelStringSlice};
use delta_kernel::expressions::{
    ArrayData, BinaryOperator, Expression, Scalar, StructData, UnaryOperator, VariadicOperator,
};

/// Free the memory the passed SharedExpression
///
/// # Safety
/// Engine is responsible for passing a valid SharedExpression
#[no_mangle]
pub unsafe extern "C" fn free_kernel_predicate(data: Handle<SharedExpression>) {
    data.drop_handle();
}

type VisitLiteralFn<T> = extern "C" fn(data: *mut c_void, sibling_list_id: usize, value: T);
type VisitBinaryOpFn =
    extern "C" fn(data: *mut c_void, sibling_list_id: usize, child_list_id: usize);
type VisitVariadicFn =
    extern "C" fn(data: *mut c_void, sibling_list_id: usize, child_list_id: usize);
type VisitUnaryFn = extern "C" fn(data: *mut c_void, sibling_list_id: usize, child_list_id: usize);

/// The [`EngineExpressionVisitor`] defines a visitor system to allow engines to build their own
/// representation of a kernel expression.
///
/// The model is list based. When the kernel needs a list, it will ask engine to allocate one of a
/// particular size. Once allocated the engine returns an `id`, which can be any integer identifier
/// ([`usize`]) the engine wants, and will be passed back to the engine to identify the list in the
/// future.
///
/// Every expression the kernel visits belongs to some list of "sibling" elements. The schema
/// itself is a list of schema elements, and every complex type (struct expression, array, variadic, etc)
/// contains a list of "child" elements.
///  1. Before visiting any complex expression type, the kernel asks the engine to allocate a list to
///     hold its children
///  2. When visiting any expression element, the kernel passes its parent's "child list" as the
///     "sibling list" the element should be appended to:
///      - For a struct literal, first visit each struct field and visit each value
///      - For a struct expression, visit each sub expression.
///      - For an array literal, visit each of the elements.
///      - For a variadic `and` or `or` expression, visit each sub-expression.
///      - For a binary operator expression, visit the left and right operands.
///      - For a unary `is null` or `not` expression, visit the sub-expression.
///  3. When visiting a complex expression, the kernel also passes the "child list" containing
///     that element's (already-visited) children.
///  4. The [`visit_expression`] method returns the id of the list of top-level columns
///
/// WARNING: The visitor MUST NOT retain internal references to string slices or binary data passed
/// to visitor methods
/// TODO: Visit type information in struct field and null. This will likely involve using the schema
/// visitor. Note that struct literals are currently in flux, and may change significantly. Here is the relevant
/// issue: https://github.com/delta-incubator/delta-kernel-rs/issues/412
#[repr(C)]
pub struct EngineExpressionVisitor {
    /// An opaque engine state pointer
    pub data: *mut c_void,
    /// Creates a new expression list, optionally reserving capacity up front
    pub make_field_list: extern "C" fn(data: *mut c_void, reserve: usize) -> usize,
    /// Visit a 32bit `integer` belonging to the list identified by `sibling_list_id`.
    pub visit_int_literal: VisitLiteralFn<i32>,
    /// Visit a 64bit `long`  belonging to the list identified by `sibling_list_id`.
    pub visit_long_literal: VisitLiteralFn<i64>,
    /// Visit a 16bit `short` belonging to the list identified by `sibling_list_id`.
    pub visit_short_literal: VisitLiteralFn<i16>,
    /// Visit an 8bit `byte` belonging to the list identified by `sibling_list_id`.
    pub visit_byte_literal: VisitLiteralFn<i8>,
    /// Visit a 32bit `float` belonging to the list identified by `sibling_list_id`.
    pub visit_float_literal: VisitLiteralFn<f32>,
    /// Visit a 64bit `double` belonging to the list identified by `sibling_list_id`.
    pub visit_double_literal: VisitLiteralFn<f64>,
    /// Visit a `string` belonging to the list identified by `sibling_list_id`.
    pub visit_string_literal: VisitLiteralFn<KernelStringSlice>,
    /// Visit a `boolean` belonging to the list identified by `sibling_list_id`.
    pub visit_bool_literal: VisitLiteralFn<bool>,
    /// Visit a 64bit timestamp belonging to the list identified by `sibling_list_id`.
    /// The timestamp is microsecond precision and adjusted to UTC.
    pub visit_timestamp_literal: VisitLiteralFn<i64>,
    /// Visit a 64bit timestamp belonging to the list identified by `sibling_list_id`.
    /// The timestamp is microsecond precision with no timezone.
    pub visit_timestamp_ntz_literal: VisitLiteralFn<i64>,
    /// Visit a 32bit intger `date` representing days since UNIX epoch 1970-01-01.  The `date` belongs
    /// to the list identified by `sibling_list_id`.
    pub visit_date_literal: VisitLiteralFn<i32>,
    /// Visit binary data at the `buffer` with length `len` belonging to the list identified by
    /// `sibling_list_id`.
    pub visit_binary_literal:
        extern "C" fn(data: *mut c_void, sibling_list_id: usize, buffer: *const u8, len: usize),
    /// Visit a 128bit `decimal` value with the given precision and scale. The 128bit integer
    /// is split into the most significant 64 bits in `value_ms`, and the least significant 64
    /// bits in `value_ls`. The `decimal` belongs to the list identified by `sibling_list_id`.
    pub visit_decimal_literal: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        value_ms: u64,
        value_ls: u64,
        precision: u8,
        scale: u8,
    ),
    /// Visit a struct literal belonging to the list identified by `sibling_list_id`.
    /// The field names of the struct are in a list identified by `child_field_list_id`.
    /// The values of the struct are in a list identified by `child_value_list_id`.
    pub visit_struct_literal: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        child_field_list_value: usize,
        child_value_list_id: usize,
    ),
    /// Visit an array literal belonging to the list identified by `sibling_list_id`.
    /// The values of the array are in a list identified by `child_list_id`.
    pub visit_array_literal:
        extern "C" fn(data: *mut c_void, sibling_list_id: usize, child_list_id: usize),
    /// Visits a null value belonging to the list identified by `sibling_list_id.
    pub visit_null_literal: extern "C" fn(data: *mut c_void, sibling_list_id: usize),
    /// Visits an `and` expression belonging to the list identified by `sibling_list_id`.
    /// The sub-expressions of the array are in a list identified by `child_list_id`
    pub visit_and: VisitVariadicFn,
    /// Visits an `or` expression belonging to the list identified by `sibling_list_id`.
    /// The sub-expressions of the array are in a list identified by `child_list_id`
    pub visit_or: VisitVariadicFn,
    /// Visits a `not` expression belonging to the list identified by `sibling_list_id`.
    /// The sub-expression will be in a _one_ item list identified by `child_list_id`
    pub visit_not: VisitUnaryFn,
    /// Visits a `is_null` expression belonging to the list identified by `sibling_list_id`.
    /// The sub-expression will be in a _one_ item list identified by `child_list_id`
    pub visit_is_null: VisitUnaryFn,
    /// Visits the `LessThan` binary operator belonging to the list identified by `sibling_list_id`.
    /// The operands will be in a _two_ item list identified by `child_list_id`
    pub visit_lt: VisitBinaryOpFn,
    /// Visits the `LessThanOrEqual` binary operator belonging to the list identified by `sibling_list_id`.
    /// The operands will be in a _two_ item list identified by `child_list_id`
    pub visit_le: VisitBinaryOpFn,
    /// Visits the `GreaterThan` binary operator belonging to the list identified by `sibling_list_id`.
    /// The operands will be in a _two_ item list identified by `child_list_id`
    pub visit_gt: VisitBinaryOpFn,
    /// Visits the `GreaterThanOrEqual` binary operator belonging to the list identified by `sibling_list_id`.
    /// The operands will be in a _two_ item list identified by `child_list_id`
    pub visit_ge: VisitBinaryOpFn,
    /// Visits the `Equal` binary operator belonging to the list identified by `sibling_list_id`.
    /// The operands will be in a _two_ item list identified by `child_list_id`
    pub visit_eq: VisitBinaryOpFn,
    /// Visits the `NotEqual` binary operator belonging to the list identified by `sibling_list_id`.
    /// The operands will be in a _two_ item list identified by `child_list_id`
    pub visit_ne: VisitBinaryOpFn,
    /// Visits the `Distinct` binary operator belonging to the list identified by `sibling_list_id`.
    /// The operands will be in a _two_ item list identified by `child_list_id`
    pub visit_distinct: VisitBinaryOpFn,
    /// Visits the `In` binary operator belonging to the list identified by `sibling_list_id`.
    /// The operands will be in a _two_ item list identified by `child_list_id`
    pub visit_in: VisitBinaryOpFn,
    /// Visits the `NotIn` binary operator belonging to the list identified by `sibling_list_id`.
    /// The operands will be in a _two_ item list identified by `child_list_id`
    pub visit_not_in: VisitBinaryOpFn,
    /// Visits the `Add` binary operator belonging to the list identified by `sibling_list_id`.
    /// The operands will be in a _two_ item list identified by `child_list_id`
    pub visit_add: VisitBinaryOpFn,
    /// Visits the `Minus` binary operator belonging to the list identified by `sibling_list_id`.
    /// The operands will be in a _two_ item list identified by `child_list_id`
    pub visit_minus: VisitBinaryOpFn,
    /// Visits the `Multiply` binary operator belonging to the list identified by `sibling_list_id`.
    /// The operands will be in a _two_ item list identified by `child_list_id`
    pub visit_multiply: VisitBinaryOpFn,
    /// Visits the `Divide` binary operator belonging to the list identified by `sibling_list_id`.
    /// The operands will be in a _two_ item list identified by `child_list_id`
    pub visit_divide: VisitBinaryOpFn,
    /// Visits the `column` belonging to the list identified by `sibling_list_id`.
    pub visit_column:
        extern "C" fn(data: *mut c_void, sibling_list_id: usize, name: KernelStringSlice),
    /// Visits a `StructExpression` belonging to the list identified by `sibling_list_id`.
    /// The sub-expressions of the `StructExpression` are in a list identified by `child_list_id`
    pub visit_struct_expr:
        extern "C" fn(data: *mut c_void, sibling_list_id: usize, child_list_id: usize),
}

/// Visit the expression of the passed [`SharedExpression`] Handle using the provided `visitor`.
/// See the documentation of [`EngineExpressionVisitor`] for a description of how this visitor
/// works.
///
/// This method returns the id that the engine generated for the top level expression
///
/// # Safety
///
/// The caller must pass a valid SharedExpression Handle and expression visitor
#[no_mangle]
pub unsafe extern "C" fn visit_expression(
    expression: &Handle<SharedExpression>,
    visitor: &mut EngineExpressionVisitor,
) -> usize {
    macro_rules! call {
        ( $visitor:ident, $visitor_fn:ident $(, $extra_args:expr) *) => {
            ($visitor.$visitor_fn)($visitor.data $(, $extra_args) *)
        };
    }
    fn visit_expression_array(
        visitor: &mut EngineExpressionVisitor,
        array: &ArrayData,
        sibling_list_id: usize,
    ) {
        #[allow(deprecated)]
        let elements = array.array_elements();
        let child_list_id = call!(visitor, make_field_list, elements.len());
        for scalar in elements {
            visit_expression_scalar(visitor, scalar, child_list_id);
        }
        call!(visitor, visit_array_literal, sibling_list_id, child_list_id);
    }
    fn visit_expression_struct_literal(
        visitor: &mut EngineExpressionVisitor,
        struct_data: &StructData,
        sibling_list_id: usize,
    ) {
        let child_value_list_id = call!(visitor, make_field_list, struct_data.fields().len());
        let child_field_list_id = call!(visitor, make_field_list, struct_data.fields().len());
        for (field, value) in struct_data.fields().iter().zip(struct_data.values()) {
            call!(
                visitor,
                visit_string_literal,
                child_field_list_id,
                field.name().into()
            );
            visit_expression_scalar(visitor, value, child_value_list_id);
        }
        call!(
            visitor,
            visit_struct_literal,
            sibling_list_id,
            child_field_list_id,
            child_value_list_id
        )
    }
    fn visit_expression_struct_expr(
        visitor: &mut EngineExpressionVisitor,
        exprs: &[Expression],
        sibling_list_id: usize,
    ) {
        let child_list_id = call!(visitor, make_field_list, exprs.len());
        for expr in exprs {
            visit_expression_impl(visitor, expr, child_list_id);
        }
        call!(visitor, visit_struct_expr, sibling_list_id, child_list_id)
    }
    fn visit_expression_variadic(
        visitor: &mut EngineExpressionVisitor,
        op: &VariadicOperator,
        exprs: &[Expression],
        sibling_list_id: usize,
    ) {
        let child_list_id = call!(visitor, make_field_list, exprs.len());
        for expr in exprs {
            visit_expression_impl(visitor, expr, child_list_id);
        }

        let visit_fn = match op {
            VariadicOperator::And => &visitor.visit_and,
            VariadicOperator::Or => &visitor.visit_or,
        };
        visit_fn(visitor.data, sibling_list_id, child_list_id);
    }
    fn visit_expression_scalar(
        visitor: &mut EngineExpressionVisitor,
        scalar: &Scalar,
        sibling_list_id: usize,
    ) {
        match scalar {
            Scalar::Integer(val) => call!(visitor, visit_int_literal, sibling_list_id, *val),
            Scalar::Long(val) => call!(visitor, visit_long_literal, sibling_list_id, *val),
            Scalar::Short(val) => call!(visitor, visit_short_literal, sibling_list_id, *val),
            Scalar::Byte(val) => call!(visitor, visit_byte_literal, sibling_list_id, *val),
            Scalar::Float(val) => call!(visitor, visit_float_literal, sibling_list_id, *val),
            Scalar::Double(val) => call!(visitor, visit_double_literal, sibling_list_id, *val),
            Scalar::String(val) => {
                call!(visitor, visit_string_literal, sibling_list_id, val.into())
            }
            Scalar::Boolean(val) => call!(visitor, visit_bool_literal, sibling_list_id, *val),
            Scalar::Timestamp(val) => {
                call!(visitor, visit_timestamp_literal, sibling_list_id, *val)
            }
            Scalar::TimestampNtz(val) => {
                call!(visitor, visit_timestamp_ntz_literal, sibling_list_id, *val)
            }
            Scalar::Date(val) => call!(visitor, visit_date_literal, sibling_list_id, *val),
            Scalar::Binary(buf) => call!(
                visitor,
                visit_binary_literal,
                sibling_list_id,
                buf.as_ptr(),
                buf.len()
            ),
            Scalar::Decimal(value, precision, scale) => {
                let ms: u64 = (value >> 64) as u64;
                let ls: u64 = *value as u64;
                call!(
                    visitor,
                    visit_decimal_literal,
                    sibling_list_id,
                    ms,
                    ls,
                    *precision,
                    *scale
                )
            }
            Scalar::Null(_) => call!(visitor, visit_null_literal, sibling_list_id),
            Scalar::Struct(struct_data) => {
                visit_expression_struct_literal(visitor, struct_data, sibling_list_id)
            }
            Scalar::Array(array) => visit_expression_array(visitor, array, sibling_list_id),
        }
    }
    fn visit_expression_impl(
        visitor: &mut EngineExpressionVisitor,
        expression: &Expression,
        sibling_list_id: usize,
    ) {
        match expression {
            Expression::Literal(scalar) => {
                visit_expression_scalar(visitor, scalar, sibling_list_id)
            }
            Expression::Column(name) => call!(visitor, visit_column, sibling_list_id, name.into()),
            Expression::Struct(exprs) => {
                visit_expression_struct_expr(visitor, exprs, sibling_list_id)
            }
            Expression::BinaryOperation { op, left, right } => {
                let child_list_id = call!(visitor, make_field_list, 2);
                visit_expression_impl(visitor, left, child_list_id);
                visit_expression_impl(visitor, right, child_list_id);
                let op = match op {
                    BinaryOperator::Plus => visitor.visit_add,
                    BinaryOperator::Minus => visitor.visit_minus,
                    BinaryOperator::Multiply => visitor.visit_multiply,
                    BinaryOperator::Divide => visitor.visit_divide,
                    BinaryOperator::LessThan => visitor.visit_lt,
                    BinaryOperator::LessThanOrEqual => visitor.visit_le,
                    BinaryOperator::GreaterThan => visitor.visit_gt,
                    BinaryOperator::GreaterThanOrEqual => visitor.visit_ge,
                    BinaryOperator::Equal => visitor.visit_eq,
                    BinaryOperator::NotEqual => visitor.visit_ne,
                    BinaryOperator::Distinct => visitor.visit_distinct,
                    BinaryOperator::In => visitor.visit_in,
                    BinaryOperator::NotIn => visitor.visit_not_in,
                };
                op(visitor.data, sibling_list_id, child_list_id);
            }
            Expression::UnaryOperation { op, expr } => {
                let child_id_list = call!(visitor, make_field_list, 1);
                visit_expression_impl(visitor, expr, child_id_list);
                let op = match op {
                    UnaryOperator::Not => visitor.visit_not,
                    UnaryOperator::IsNull => visitor.visit_is_null,
                };
                op(visitor.data, sibling_list_id, child_id_list);
            }
            Expression::VariadicOperation { op, exprs } => {
                visit_expression_variadic(visitor, op, exprs, sibling_list_id)
            }
        }
    }
    let top_level = call!(visitor, make_field_list, 1);
    visit_expression_impl(visitor, expression.as_ref(), top_level);
    top_level
}

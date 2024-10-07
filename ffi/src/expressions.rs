use std::{ffi::c_void, sync::Arc};

use crate::{
    handle::Handle, AllocateErrorFn, EngineIterator, ExternResult, IntoExternResult,
    KernelPredicate, KernelStringSlice, ReferenceSet, TryFromStringSlice,
};
use delta_kernel::{
    expressions::{
        ArrayData, BinaryOperator, Expression, Scalar, StructData, UnaryOperator, VariadicOperator,
    },
    schema::{ArrayType, DataType, PrimitiveType, StructField, StructType},
    DeltaResult,
};

#[derive(Default)]
pub struct KernelExpressionVisitorState {
    // TODO: ReferenceSet<Box<dyn MetadataFilterFn>> instead?
    inflight_expressions: ReferenceSet<Expression>,
}
impl KernelExpressionVisitorState {
    pub fn new() -> Self {
        Self {
            inflight_expressions: Default::default(),
        }
    }
}

/// A predicate that can be used to skip data when scanning.
///
/// When invoking [`scan::scan`], The engine provides a pointer to the (engine's native) predicate,
/// along with a visitor function that can be invoked to recursively visit the predicate. This
/// engine state must be valid until the call to `scan::scan` returns. Inside that method, the
/// kernel allocates visitor state, which becomes the second argument to the predicate visitor
/// invocation along with the engine-provided predicate pointer. The visitor state is valid for the
/// lifetime of the predicate visitor invocation. Thanks to this double indirection, engine and
/// kernel each retain ownership of their respective objects, with no need to coordinate memory
/// lifetimes with the other.
#[repr(C)]
pub struct EnginePredicate {
    pub predicate: *mut c_void,
    pub visitor:
        extern "C" fn(predicate: *mut c_void, state: &mut KernelExpressionVisitorState) -> usize,
}

fn wrap_expression(state: &mut KernelExpressionVisitorState, expr: Expression) -> usize {
    state.inflight_expressions.insert(expr)
}

pub fn unwrap_kernel_expression(
    state: &mut KernelExpressionVisitorState,
    exprid: usize,
) -> Option<Expression> {
    state.inflight_expressions.take(exprid)
}

fn visit_expression_binary(
    state: &mut KernelExpressionVisitorState,
    op: BinaryOperator,
    a: usize,
    b: usize,
) -> usize {
    let left = unwrap_kernel_expression(state, a).map(Box::new);
    let right = unwrap_kernel_expression(state, b).map(Box::new);
    match left.zip(right) {
        Some((left, right)) => {
            wrap_expression(state, Expression::BinaryOperation { op, left, right })
        }
        None => 0, // invalid child => invalid node
    }
}

fn visit_expression_unary(
    state: &mut KernelExpressionVisitorState,
    op: UnaryOperator,
    inner_expr: usize,
) -> usize {
    unwrap_kernel_expression(state, inner_expr).map_or(0, |expr| {
        wrap_expression(state, Expression::unary(op, expr))
    })
}

// The EngineIterator is not thread safe, not reentrant, not owned by callee, not freed by callee.
#[no_mangle]
pub extern "C" fn visit_expression_and(
    state: &mut KernelExpressionVisitorState,
    children: &mut EngineIterator,
) -> usize {
    let result = Expression::and_from(
        children.flat_map(|child| unwrap_kernel_expression(state, child as usize)),
    );
    wrap_expression(state, result)
}

#[no_mangle]
pub extern "C" fn visit_expression_lt(
    state: &mut KernelExpressionVisitorState,
    a: usize,
    b: usize,
) -> usize {
    visit_expression_binary(state, BinaryOperator::LessThan, a, b)
}

#[no_mangle]
pub extern "C" fn visit_expression_le(
    state: &mut KernelExpressionVisitorState,
    a: usize,
    b: usize,
) -> usize {
    visit_expression_binary(state, BinaryOperator::LessThanOrEqual, a, b)
}

#[no_mangle]
pub extern "C" fn visit_expression_gt(
    state: &mut KernelExpressionVisitorState,
    a: usize,
    b: usize,
) -> usize {
    visit_expression_binary(state, BinaryOperator::GreaterThan, a, b)
}

#[no_mangle]
pub extern "C" fn visit_expression_ge(
    state: &mut KernelExpressionVisitorState,
    a: usize,
    b: usize,
) -> usize {
    visit_expression_binary(state, BinaryOperator::GreaterThanOrEqual, a, b)
}

#[no_mangle]
pub extern "C" fn visit_expression_eq(
    state: &mut KernelExpressionVisitorState,
    a: usize,
    b: usize,
) -> usize {
    visit_expression_binary(state, BinaryOperator::Equal, a, b)
}

/// # Safety
/// The string slice must be valid
#[no_mangle]
pub unsafe extern "C" fn visit_expression_column(
    state: &mut KernelExpressionVisitorState,
    name: KernelStringSlice,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name = unsafe { String::try_from_slice(&name) };
    visit_expression_column_impl(state, name).into_extern_result(&allocate_error)
}
fn visit_expression_column_impl(
    state: &mut KernelExpressionVisitorState,
    name: DeltaResult<String>,
) -> DeltaResult<usize> {
    Ok(wrap_expression(state, Expression::Column(name?)))
}

#[no_mangle]
pub extern "C" fn visit_expression_not(
    state: &mut KernelExpressionVisitorState,
    inner_expr: usize,
) -> usize {
    visit_expression_unary(state, UnaryOperator::Not, inner_expr)
}

#[no_mangle]
pub extern "C" fn visit_expression_is_null(
    state: &mut KernelExpressionVisitorState,
    inner_expr: usize,
) -> usize {
    visit_expression_unary(state, UnaryOperator::IsNull, inner_expr)
}

/// # Safety
/// The string slice must be valid
#[no_mangle]
pub unsafe extern "C" fn visit_expression_literal_string(
    state: &mut KernelExpressionVisitorState,
    value: KernelStringSlice,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let value = unsafe { String::try_from_slice(&value) };
    visit_expression_literal_string_impl(state, value).into_extern_result(&allocate_error)
}
fn visit_expression_literal_string_impl(
    state: &mut KernelExpressionVisitorState,
    value: DeltaResult<String>,
) -> DeltaResult<usize> {
    Ok(wrap_expression(
        state,
        Expression::Literal(Scalar::from(value?)),
    ))
}

// We need to get parse.expand working to be able to macro everything below, see issue #255

#[no_mangle]
pub extern "C" fn visit_expression_literal_int(
    state: &mut KernelExpressionVisitorState,
    value: i32,
) -> usize {
    wrap_expression(state, Expression::literal(value))
}

#[no_mangle]
pub extern "C" fn visit_expression_literal_long(
    state: &mut KernelExpressionVisitorState,
    value: i64,
) -> usize {
    wrap_expression(state, Expression::literal(value))
}

#[no_mangle]
pub extern "C" fn visit_expression_literal_short(
    state: &mut KernelExpressionVisitorState,
    value: i16,
) -> usize {
    wrap_expression(state, Expression::literal(value))
}

#[no_mangle]
pub extern "C" fn visit_expression_literal_byte(
    state: &mut KernelExpressionVisitorState,
    value: i8,
) -> usize {
    wrap_expression(state, Expression::literal(value))
}

#[no_mangle]
pub extern "C" fn visit_expression_literal_float(
    state: &mut KernelExpressionVisitorState,
    value: f32,
) -> usize {
    wrap_expression(state, Expression::literal(value))
}

#[no_mangle]
pub extern "C" fn visit_expression_literal_double(
    state: &mut KernelExpressionVisitorState,
    value: f64,
) -> usize {
    wrap_expression(state, Expression::literal(value))
}

#[no_mangle]
pub extern "C" fn visit_expression_literal_bool(
    state: &mut KernelExpressionVisitorState,
    value: bool,
) -> usize {
    wrap_expression(state, Expression::literal(value))
}

#[no_mangle]
pub unsafe extern "C" fn get_kernel_expression() -> Handle<KernelPredicate> {
    use Expression as Expr;

    let array_type = ArrayType::new(
        DataType::Primitive(delta_kernel::schema::PrimitiveType::Short),
        false,
    );
    let array_data = ArrayData::new(array_type.clone(), vec![Scalar::Short(5), Scalar::Short(0)]);
    let nested_fields = vec![
        StructField::new("a", DataType::Primitive(PrimitiveType::Integer), false),
        StructField::new("b", DataType::Array(Box::new(array_type)), false),
    ];
    let nested_values = vec![Scalar::Integer(500), Scalar::Array(array_data)];
    let nested = StructData::try_new(nested_fields.clone(), nested_values).unwrap();
    let nested_type = StructType::new(nested_fields);
    let top = StructData::try_new(
        vec![StructField::new(
            "top",
            DataType::Struct(Box::new(nested_type)),
            true,
        )],
        vec![Scalar::Struct(nested)],
    )
    .unwrap();
    Arc::new(Expr::and_from(vec![
        Expr::and_from(vec![
            Expr::literal(Scalar::Integer(5)),
            Expr::literal(Scalar::Long(20)),
        ]),
        Expr::literal(Scalar::Integer(10)),
        Expr::literal(Scalar::Struct(top)),
    ]))
    .into()
}

/// Kernel Expression to Engine Expression
///
#[repr(C)]
pub struct EngineExpressionVisitor {
    /// opaque state pointer
    pub data: *mut c_void,
    /// Visit an `integer` belonging to the list identified by `sibling_list_id`.
    pub visit_int: extern "C" fn(data: *mut c_void, value: i32) -> usize,
    pub visit_long: extern "C" fn(data: *mut c_void, value: i64) -> usize,
    pub visit_short: extern "C" fn(data: *mut c_void, value: i16) -> usize,
    pub visit_byte: extern "C" fn(data: *mut c_void, value: i8) -> usize,
    pub visit_float: extern "C" fn(data: *mut c_void, value: f32) -> usize,
    pub visit_double: extern "C" fn(data: *mut c_void, value: f64) -> usize,
    pub visit_string: extern "C" fn(data: *mut c_void, value: KernelStringSlice) -> usize,
    pub visit_bool: extern "C" fn(data: *mut c_void, value: bool) -> usize,
    pub visit_timestamp: extern "C" fn(data: *mut c_void, value: i64) -> usize,
    pub visit_timestamp_ntz: extern "C" fn(data: *mut c_void, value: i64) -> usize,
    pub visit_date: extern "C" fn(data: *mut c_void, value: i32) -> usize,
    pub visit_binary: extern "C" fn(data: *mut c_void, buf: *const u8, len: usize) -> usize,
    pub visit_decimal: extern "C" fn(
        data: *mut c_void,
        value_ms: u64, // Most significant 64 bits of decimal value
        value_ls: u64, // Least significant 64 bits of decimal value
        precision: u8,
        scale: u8,
    ) -> usize,

    pub visit_and: extern "C" fn(data: *mut c_void, len: usize) -> usize,
    pub visit_or: extern "C" fn(data: *mut c_void, len: usize) -> usize,
    pub visit_variadic_item:
        extern "C" fn(data: *mut c_void, variadic_id: usize, sub_expr_id: usize),
    pub visit_not: extern "C" fn(data: *mut c_void, inner_expr: usize) -> usize,
    pub visit_is_null: extern "C" fn(data: *mut c_void, inner_expr: usize) -> usize,

    pub visit_lt: extern "C" fn(data: *mut c_void, a: usize, b: usize) -> usize,
    pub visit_le: extern "C" fn(data: *mut c_void, a: usize, b: usize) -> usize,
    pub visit_gt: extern "C" fn(data: *mut c_void, a: usize, b: usize) -> usize,
    pub visit_ge: extern "C" fn(data: *mut c_void, a: usize, b: usize) -> usize,
    pub visit_eq: extern "C" fn(data: *mut c_void, a: usize, b: usize) -> usize,
    pub visit_ne: extern "C" fn(data: *mut c_void, a: usize, b: usize) -> usize,
    pub visit_distinct: extern "C" fn(data: *mut c_void, a: usize, b: usize) -> usize,
    pub visit_in: extern "C" fn(data: *mut c_void, a: usize, b: usize) -> usize,
    pub visit_not_in: extern "C" fn(data: *mut c_void, a: usize, b: usize) -> usize,

    pub visit_add: extern "C" fn(data: *mut c_void, a: usize, b: usize) -> usize,
    pub visit_minus: extern "C" fn(data: *mut c_void, a: usize, b: usize) -> usize,
    pub visit_multiply: extern "C" fn(data: *mut c_void, a: usize, b: usize) -> usize,
    pub visit_divide: extern "C" fn(data: *mut c_void, a: usize, b: usize) -> usize,

    pub visit_column: extern "C" fn(data: *mut c_void, name: KernelStringSlice) -> usize,

    pub visit_struct: extern "C" fn(data: *mut c_void, len: usize) -> usize,
    pub visit_struct_item: extern "C" fn(data: *mut c_void, struct_id: usize, expr_id: usize),

    pub visit_struct_literal: extern "C" fn(data: *mut c_void, num_fields: usize) -> usize,
    pub visit_struct_literal_field: extern "C" fn(
        data: *mut c_void,
        struct_id: usize,
        field_name: KernelStringSlice,
        field_value: usize,
    ),
    pub visit_null: extern "C" fn(data: *mut c_void) -> usize,
    pub visit_array: extern "C" fn(data: *mut c_void, len: usize) -> usize,
    pub visit_array_item: extern "C" fn(data: *mut c_void, array_id: usize, item_id: usize),
}

#[no_mangle]
pub unsafe extern "C" fn visit_expression(
    expression: &Handle<KernelPredicate>, // TODO: This will likely be some kind of Handle<Expression>
    visitor: &mut EngineExpressionVisitor,
) -> usize {
    macro_rules! call {
        ( $visitor:ident, $visitor_fn:ident $(, $extra_args:expr) *) => {
            ($visitor.$visitor_fn)($visitor.data $(, $extra_args) *)
        };
    }
    fn visit_array(visitor: &mut EngineExpressionVisitor, array: &ArrayData) -> usize {
        #[allow(deprecated)]
        let elements = array.array_elements();
        let array_id = call!(visitor, visit_array, elements.len());
        for scalar in elements {
            let scalar_id = visit_scalar(visitor, scalar);
            call!(visitor, visit_array_item, array_id, scalar_id);
        }
        array_id
    }
    fn visit_struct(visitor: &mut EngineExpressionVisitor, struct_data: &StructData) -> usize {
        let struct_id = call!(visitor, visit_struct_literal, struct_data.fields().len());
        for (field, value) in struct_data.fields().iter().zip(struct_data.values()) {
            let value_id = visit_scalar(visitor, value);
            call!(
                visitor,
                visit_struct_literal_field,
                struct_id,
                field.name().into(),
                value_id
            );
        }
        struct_id
    }
    fn visit_expr_struct(visitor: &mut EngineExpressionVisitor, exprs: &Vec<Expression>) -> usize {
        let expr_struct_id = call!(visitor, visit_struct, exprs.len());
        for expr in exprs {
            let expr_id = visit_expression(visitor, expr);
            call!(visitor, visit_struct_item, expr_struct_id, expr_id)
        }
        expr_struct_id
    }
    fn visit_variadic(
        visitor: &mut EngineExpressionVisitor,
        op: &VariadicOperator,
        exprs: &Vec<Expression>,
    ) -> usize {
        let visit_fn = match op {
            VariadicOperator::And => &visitor.visit_and,
            VariadicOperator::Or => &visitor.visit_or,
        };
        let variadic_id = visit_fn(visitor.data, exprs.len());
        for expr in exprs {
            let expr_id = visit_expression(visitor, expr);
            call!(visitor, visit_variadic_item, variadic_id, expr_id)
        }
        variadic_id
    }
    fn visit_scalar(visitor: &mut EngineExpressionVisitor, scalar: &Scalar) -> usize {
        match scalar {
            Scalar::Integer(val) => call!(visitor, visit_int, *val),
            Scalar::Long(val) => call!(visitor, visit_long, *val),
            Scalar::Short(val) => call!(visitor, visit_short, *val),
            Scalar::Byte(val) => call!(visitor, visit_byte, *val),
            Scalar::Float(val) => call!(visitor, visit_float, *val),
            Scalar::Double(val) => call!(visitor, visit_double, *val),
            Scalar::String(val) => call!(visitor, visit_string, val.into()),
            Scalar::Boolean(val) => call!(visitor, visit_bool, *val),
            Scalar::Timestamp(val) => call!(visitor, visit_timestamp, *val),
            Scalar::TimestampNtz(val) => call!(visitor, visit_timestamp_ntz, *val),
            Scalar::Date(val) => call!(visitor, visit_date, *val),
            Scalar::Binary(buf) => call!(visitor, visit_binary, buf.as_ptr(), buf.len()),
            Scalar::Decimal(value, precision, scale) => {
                let ms: u64 = (value >> 64) as u64;
                let ls: u64 = *value as u64;
                call!(visitor, visit_decimal, ms, ls, *precision, *scale)
            }
            Scalar::Null(_) => call!(visitor, visit_null),
            Scalar::Struct(struct_data) => visit_struct(visitor, struct_data),
            Scalar::Array(array) => visit_array(visitor, array),
        }
    }
    fn visit_expression(visitor: &mut EngineExpressionVisitor, expression: &Expression) -> usize {
        match expression {
            Expression::Literal(scalar) => visit_scalar(visitor, scalar),
            Expression::Column(name) => call!(visitor, visit_column, name.into()),
            Expression::Struct(exprs) => visit_expr_struct(visitor, exprs),
            Expression::BinaryOperation { op, left, right } => {
                let left_id = visit_expression(visitor, left);
                let right_id = visit_expression(visitor, right);
                match op {
                    BinaryOperator::Plus => call!(visitor, visit_add, left_id, right_id),
                    BinaryOperator::Minus => call!(visitor, visit_minus, left_id, right_id),
                    BinaryOperator::Multiply => call!(visitor, visit_multiply, left_id, right_id),
                    BinaryOperator::Divide => call!(visitor, visit_divide, left_id, right_id),
                    BinaryOperator::LessThan => call!(visitor, visit_lt, left_id, right_id),
                    BinaryOperator::LessThanOrEqual => call!(visitor, visit_le, left_id, right_id),
                    BinaryOperator::GreaterThan => call!(visitor, visit_gt, left_id, right_id),
                    BinaryOperator::GreaterThanOrEqual => {
                        call!(visitor, visit_ge, left_id, right_id)
                    }
                    BinaryOperator::Equal => call!(visitor, visit_eq, left_id, right_id),
                    BinaryOperator::NotEqual => call!(visitor, visit_ne, left_id, right_id),
                    BinaryOperator::Distinct => call!(visitor, visit_distinct, left_id, right_id),
                    BinaryOperator::In => call!(visitor, visit_in, left_id, right_id),
                    BinaryOperator::NotIn => call!(visitor, visit_not_in, left_id, right_id),
                }
            }
            Expression::UnaryOperation { op, expr } => {
                let expr_id = visit_expression(visitor, expr);
                match op {
                    UnaryOperator::Not => call!(visitor, visit_not, expr_id),
                    UnaryOperator::IsNull => call!(visitor, visit_is_null, expr_id),
                }
            }
            Expression::VariadicOperation { op, exprs } => visit_variadic(visitor, op, exprs),
        }
    }
    visit_expression(visitor, expression.as_ref())
}

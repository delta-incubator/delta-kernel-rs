use std::{ffi::c_void, io::Read, ops::Add, sync::Arc};

use crate::{
    handle::Handle, AllocateErrorFn, EngineIterator, ExternResult, IntoExternResult,
    KernelPredicate, KernelStringSlice, ReferenceSet, TryFromStringSlice,
};
use delta_kernel::{
    expressions::{BinaryOperator, Expression, Scalar, UnaryOperator, VariadicOperator},
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
    Arc::new(Expr::and_from(vec![
        Expr::and_from(vec![
            Expr::literal(Scalar::Integer(5)),
            Expr::literal(Scalar::Integer(20)),
        ]),
        Expr::literal(Scalar::Integer(10)),
        Expr::literal(Scalar::Integer(10)),
    ]))
    .into()
}

/// Kernel Expression to Engine Expression
///
#[repr(C)]
pub struct EngineExpressionVisitor {
    /// opaque state pointer
    pub data: *mut c_void,
    /// Creates a new field list, optionally reserving capacity up front
    pub make_expr_list: extern "C" fn(data: *mut c_void, reserve: usize) -> usize,

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
        value_ms: u64, // Most significant half of decimal value
        value_ls: u64, // Least significant half of decimal value
        precision: u8,
        scale: u8,
    ) -> usize,
    // Scalar::Null(_) => todo!(),
    // Scalar::Struct(_) => todo!(),
    // Scalar::Array(_) => todo!(),
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

    pub visit_expr_struct: extern "C" fn(data: *mut c_void, len: usize) -> usize,
    pub visit_expr_struct_item: extern "C" fn(data: *mut c_void, struct_id: usize, expr_id: usize),
}

#[no_mangle]
pub unsafe extern "C" fn visit_expression(
    expression: &Handle<KernelPredicate>, // TODO: This will likely be some kind of Handle<Expression>
    visitor: &mut EngineExpressionVisitor,
) -> usize {
    fn visit_expr_struct(visitor: &mut EngineExpressionVisitor, exprs: &Vec<Expression>) -> usize {
        let expr_struct_id = (visitor.visit_expr_struct)(visitor.data, exprs.len());
        for expr in exprs {
            let expr_id = visit_expression(visitor, expr);
            (visitor.visit_expr_struct_item)(visitor.data, expr_struct_id, expr_id)
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
            (visitor.visit_variadic_item)(visitor.data, variadic_id, expr_id)
        }
        variadic_id
    }
    fn visit_expression(visitor: &mut EngineExpressionVisitor, expression: &Expression) -> usize {
        macro_rules! call {
            ( $visitor_fn:ident $(, $extra_args:expr) *) => {
                (visitor.$visitor_fn)(visitor.data $(, $extra_args) *)
            };
        }
        match expression {
            Expression::Literal(lit) => match lit {
                Scalar::Integer(val) => call!(visit_int, *val),
                Scalar::Long(val) => call!(visit_long, *val),
                Scalar::Short(val) => call!(visit_short, *val),
                Scalar::Byte(val) => call!(visit_byte, *val),
                Scalar::Float(val) => call!(visit_float, *val),
                Scalar::Double(val) => call!(visit_double, *val),
                Scalar::String(val) => call!(visit_string, val.into()),
                Scalar::Boolean(val) => call!(visit_bool, *val),
                Scalar::Timestamp(val) => call!(visit_timestamp, *val),
                Scalar::TimestampNtz(val) => call!(visit_timestamp_ntz, *val),
                Scalar::Date(val) => call!(visit_date, *val),
                Scalar::Binary(buf) => call!(visit_binary, buf.as_ptr(), buf.len()),
                Scalar::Decimal(value, precision, scale) => {
                    let ms: u64 = (value >> 64) as u64;
                    let ls: u64 = *value as u64;
                    call!(visit_decimal, ms, ls, *precision, *scale)
                }
                Scalar::Null(_) => todo!(),
                Scalar::Struct(_) => todo!(),
                Scalar::Array(_) => todo!(),
            },
            Expression::Column(name) => call!(visit_column, name.into()),
            Expression::Struct(exprs) => visit_expr_struct(visitor, exprs),
            Expression::BinaryOperation { op, left, right } => {
                let left_id = visit_expression(visitor, left);
                let right_id = visit_expression(visitor, right);
                match op {
                    BinaryOperator::Plus => call!(visit_add, left_id, right_id),
                    BinaryOperator::Minus => call!(visit_minus, left_id, right_id),
                    BinaryOperator::Multiply => call!(visit_multiply, left_id, right_id),
                    BinaryOperator::Divide => call!(visit_divide, left_id, right_id),
                    BinaryOperator::LessThan => call!(visit_lt, left_id, right_id),
                    BinaryOperator::LessThanOrEqual => call!(visit_le, left_id, right_id),
                    BinaryOperator::GreaterThan => call!(visit_gt, left_id, right_id),
                    BinaryOperator::GreaterThanOrEqual => call!(visit_ge, left_id, right_id),
                    BinaryOperator::Equal => call!(visit_eq, left_id, right_id),
                    BinaryOperator::NotEqual => call!(visit_ne, left_id, right_id),
                    BinaryOperator::Distinct => call!(visit_distinct, left_id, right_id),
                    BinaryOperator::In => call!(visit_in, left_id, right_id),
                    BinaryOperator::NotIn => call!(visit_not_in, left_id, right_id),
                }
            }
            Expression::UnaryOperation { op, expr } => {
                let expr_id = visit_expression(visitor, expr);
                match op {
                    UnaryOperator::Not => call!(visit_not, expr_id),
                    UnaryOperator::IsNull => call!(visit_is_null, expr_id),
                }
            }
            Expression::VariadicOperation { op, exprs } => visit_variadic(visitor, op, exprs),
        }
    }
    visit_expression(visitor, expression.as_ref())
}

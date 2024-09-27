use std::ffi::c_void;

use crate::{
    handle::Handle, AllocateErrorFn, EngineIterator, ExternResult, IntoExternResult,
    KernelStringSlice, ReferenceSet, TryFromStringSlice,
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
    pub visit_bool: extern "C" fn(data: *mut c_void, value: bool) -> usize,
    pub visit_string: extern "C" fn(data: *mut c_void, value: KernelStringSlice) -> usize,

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

    pub visit_column: extern "C" fn(data: *mut c_void, name: KernelStringSlice) -> usize,
}

#[no_mangle]
pub unsafe extern "C" fn visit_expression(
    expression: &Expression,
    visitor: &mut EngineExpressionVisitor,
) -> usize {
    fn visit_variadic(
        visitor: &mut EngineExpressionVisitor,
        op: &VariadicOperator,
        exprs: &Vec<Expression>,
    ) -> usize {
        let variadic_id = match op {
            VariadicOperator::And => (visitor.visit_and)(visitor.data, exprs.len()),
            VariadicOperator::Or => (visitor.visit_or)(visitor.data, exprs.len()),
        };
        for expr in exprs {
            let expr_id = visit_expression(visitor, expr);
            (visitor.visit_variadic_item)(visitor.data, variadic_id, expr_id)
        }
        variadic_id
    }
    fn visit_binary_op(
        visitor: &mut EngineExpressionVisitor,
        op: &BinaryOperator,
        a: &Expression,
        b: &Expression,
    ) -> usize {
        let a_id = visit_expression(visitor, a);
        let b_id = visit_expression(visitor, b);
        match op {
            BinaryOperator::Plus => todo!(),
            BinaryOperator::Minus => todo!(),
            BinaryOperator::Multiply => todo!(),
            BinaryOperator::Divide => todo!(),
            BinaryOperator::LessThan => todo!(),
            BinaryOperator::LessThanOrEqual => todo!(),
            BinaryOperator::GreaterThan => todo!(),
            BinaryOperator::GreaterThanOrEqual => todo!(),
            BinaryOperator::Equal => todo!(),
            BinaryOperator::NotEqual => todo!(),
            BinaryOperator::Distinct => todo!(),
            BinaryOperator::In => todo!(),
            BinaryOperator::NotIn => todo!(),
        }
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
                Scalar::Timestamp(val) => todo!(),
                Scalar::TimestampNtz(_) => todo!(),
                Scalar::Date(_) => todo!(),
                Scalar::Binary(_) => todo!(),
                Scalar::Decimal(_, _, _) => todo!(),
                Scalar::Null(_) => todo!(),
                Scalar::Struct(_) => todo!(),
                Scalar::Array(_) => todo!(),
            },
            Expression::Column(name) => call!(visit_column, name.into()),
            Expression::Struct(_) => todo!(),
            Expression::BinaryOperation { op, left, right } => todo!(),
            Expression::UnaryOperation { op, expr } => todo!(),
            Expression::VariadicOperation { op, exprs } => visit_variadic(visitor, op, exprs),
        }
    }
    visit_expression(visitor, expression)
}

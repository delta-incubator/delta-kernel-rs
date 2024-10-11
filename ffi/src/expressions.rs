use std::{ffi::c_void, ops::Not, sync::Arc};

use crate::{
    handle::Handle, AllocateErrorFn, EngineIterator, ExternResult, IntoExternResult,
    KernelStringSlice, ReferenceSet, TryFromStringSlice,
};
use delta_kernel::{
    expressions::{
        ArrayData, BinaryOperator, Expression, Scalar, StructData, UnaryOperator, VariadicOperator,
    },
    schema::{ArrayType, DataType, PrimitiveType, StructField, StructType},
    DeltaResult,
};
use delta_kernel_ffi_macros::handle_descriptor;

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

#[handle_descriptor(target=Expression, mutable=false, sized=true)]
pub struct SharedExpression;

/// Free the memory the passed SharedExpression
///
/// # Safety
/// Engine is responsible for passing a valid SharedExpression
#[no_mangle]
pub unsafe extern "C" fn free_kernel_predicate(data: Handle<SharedExpression>) {
    data.drop_handle();
}

/// Constructs a kernel expression that is passed back as a SharedExpression handle
///
/// # Safety
/// The caller is responsible for freeing the retured memory, either by calling
/// [`free_kernel_predicate`], or [`Handle::drop_handle`]
#[no_mangle]
pub unsafe extern "C" fn get_kernel_expression() -> Handle<SharedExpression> {
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
        Expr::literal(Scalar::Byte(i8::MAX)),
        Expr::literal(Scalar::Byte(i8::MIN)),
        Expr::literal(Scalar::Float(f32::MAX)),
        Expr::literal(Scalar::Float(f32::MIN)),
        Expr::literal(Scalar::Double(f64::MAX)),
        Expr::literal(Scalar::Double(f64::MIN)),
        Expr::literal(Scalar::Integer(i32::MAX)),
        Expr::literal(Scalar::Integer(i32::MIN)),
        Expr::literal(Scalar::Long(i64::MAX)),
        Expr::literal(Scalar::Long(i64::MIN)),
        Expr::literal(Scalar::String("hello expressions".into())),
        Expr::literal(Scalar::Boolean(true)),
        Expr::literal(Scalar::Boolean(false)),
        Expr::literal(Scalar::Timestamp(50)),
        Expr::literal(Scalar::TimestampNtz(100)),
        Expr::literal(Scalar::Date(32)),
        Expr::literal(Scalar::Binary(b"0xdeadbeefcafe".to_vec())),
        // Both the most and least significant u64 of the Decimal value will be 1
        Expr::literal(Scalar::Decimal((1 << 64) + 1, 2, 3)),
        Expr::literal(Scalar::Null(DataType::Primitive(PrimitiveType::Short))),
        Expr::literal(Scalar::Struct(top_level_struct)),
        Expr::literal(Scalar::Array(array_data)),
        Expr::struct_expr(vec![Expr::or_from(vec![
            Expr::literal(Scalar::Integer(5)),
            Expr::literal(Scalar::Long(20)),
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
        .map(|op| {
            Expr::binary(
                *op,
                Expr::literal(Scalar::Integer(0)),
                Expr::literal(Scalar::Long(0)),
            )
        }),
    );

    Arc::new(Expr::and_from(sub_exprs)).into()
}

/// The [`EngineExpressionVisitor`] defines a visitor system to allow engines to build their own
/// representation of an expression from a particular expression within the kernel.
///
/// Visit operations where the engine allocates an expression must return an associated `id`, which is an integer
/// identifier ([`usize`]). This identifier can be passed back to the engine to identify the expression.
/// The [`EngineExpressionVisitor`] handles both simple and complex types.
/// 1. For simple types, the engine is expected to allocate that data and return its identifier.
/// 2. For complex types such as structs, arrays, and variadic expressions, there will be a call to
///     construct the expression, and populate sub-expressions. For instance, [`visit_and`] recieves
///     the expected number of sub-expressions and must return an identifier. The kernel will
///     subsequently call [`visit_variadic_sub_expr`] with the identifier of the And expression, and the
///     identifier for a sub-expression.
///
/// WARNING: The visitor MUST NOT retain internal references to string slices or binary data passed
/// to visitor methods
/// TODO: Add type information in struct field and null. This will likely involve using the schema visitor.
#[repr(C)]
pub struct EngineExpressionVisitor {
    /// An opaque state pointer
    pub data: *mut c_void,
    /// Creates a new expression list, optionally reserving capacity up front
    pub make_field_list: extern "C" fn(data: *mut c_void, reserve: usize) -> usize,
    /// Visit a 32bit `integer
    pub visit_int_literal: extern "C" fn(data: *mut c_void, value: i32, sibling_list_id: usize),
    /// Visit a 64bit `long`.
    pub visit_long_literal: extern "C" fn(data: *mut c_void, value: i64, sibling_list_id: usize),
    /// Visit a 16bit `short`.
    pub visit_short_literal: extern "C" fn(data: *mut c_void, value: i16, sibling_list_id: usize),
    /// Visit an 8bit `byte`.
    pub visit_byte_literal: extern "C" fn(data: *mut c_void, value: i8, sibling_list_id: usize),
    /// Visit a 32bit `float`.
    pub visit_float_literal: extern "C" fn(data: *mut c_void, value: f32, sibling_list_id: usize),
    /// Visit a 64bit `double`.
    pub visit_double_literal: extern "C" fn(data: *mut c_void, value: f64, sibling_list_id: usize),
    /// Visit a `string`.
    pub visit_string_literal:
        extern "C" fn(data: *mut c_void, value: KernelStringSlice, sibling_list_id: usize),
    /// Visit a `boolean`.
    pub visit_bool_literal: extern "C" fn(data: *mut c_void, value: bool, sibling_list_id: usize),
    /// Visit a 64bit timestamp. The timestamp is microsecond precision and adjusted to UTC.
    pub visit_timestamp_literal:
        extern "C" fn(data: *mut c_void, value: i64, sibling_list_id: usize),
    /// Visit a 64bit timestamp. The timestamp is microsecond precision with no timezone.
    pub visit_timestamp_ntz_literal:
        extern "C" fn(data: *mut c_void, value: i64, sibling_list_id: usize),
    /// Visit a 32bit int date representing days since UNIX epoch 1970-01-01.
    pub visit_date_literal: extern "C" fn(data: *mut c_void, value: i32, sibling_list_id: usize),
    /// Visit binary data at the `buffer` with length `len`.
    pub visit_binary_literal:
        extern "C" fn(data: *mut c_void, buffer: *const u8, len: usize, sibling_list_id: usize),
    /// Visit a 128bit `decimal` value with the given precision and scale. The 128bit integer
    /// is split into the most significant 64 bits in `value_ms`, and the least significant 64
    /// bits in `value_ls`.
    pub visit_decimal_literal: extern "C" fn(
        data: *mut c_void,
        value_ms: u64, // Most significant 64 bits of decimal value
        value_ls: u64, // Least significant 64 bits of decimal value
        precision: u8,
        scale: u8,
        sibling_list_id: usize,
    ),
    /// Visit a struct literal which is made up of a list of field names and values. This declares
    /// the number of fields that the struct will have. The visitor will populate the struct fields
    /// using the [`visit_struct_literal_field`] method.
    pub visit_struct_literal: extern "C" fn(
        data: *mut c_void,
        child_field_list_value: usize,
        child_value_list_id: usize,
        sibling_list_id: usize,
    ),
    /// Visit an `arary`, declaring the length `len`. The visitor will populate the array
    /// elements using the [`visit_array_element`] method.
    pub visit_array_literal:
        extern "C" fn(data: *mut c_void, child_list_id: usize, sibling_list_id: usize),
    /// Visits a null value.
    pub visit_null_literal: extern "C" fn(data: *mut c_void, sibling_list_id: usize),
    /// Visits an `and` expression which is made of a list of sub-expressions. This declares the
    /// number of sub-expressions that the `and` expression will be made of. The visitor will populate
    /// the list of expressions using the [`visit_variadic_sub_expr`] method.
    pub visit_and: extern "C" fn(data: *mut c_void, child_list_id: usize, sibling_list_id: usize),
    /// Visits an `or` expression which is made of a list of sub-expressions. This declares the
    /// number of sub-expressions that the `or` expression will be made of. The visitor will populate
    /// the list of expressions using the [`visit_variadic_sub_expr`] method.
    pub visit_or: extern "C" fn(data: *mut c_void, child_list_id: usize, sibling_list_id: usize),
    ///Visits a `not` expression, bulit using the sub-expression `inner_expr`.
    pub visit_not: extern "C" fn(data: *mut c_void, chilrd_list_id: usize, sibling_list_id: usize),
    ///Visits an `is_null` expression, built using the sub-expression `inner_expr`.
    pub visit_is_null:
        extern "C" fn(data: *mut c_void, child_list_id: usize, sibling_list_id: usize),
    /// Visit the `less than` binary operation, which takes the left sub expression id `a` and the
    /// right sub-expression id `b`.
    pub visit_lt: extern "C" fn(data: *mut c_void, child_list_id: usize, sibling_list_id: usize),
    /// Visit the `less than or equal` binary operation, which takes the left sub expression id `a`
    /// and the right sub-expression id `b`.
    pub visit_le: extern "C" fn(data: *mut c_void, child_list_id: usize, sibling_list_id: usize),
    /// Visit the `greater than` binary operation, which takes the left sub expression id `a`
    /// and the right sub-expression id `b`.
    pub visit_gt: extern "C" fn(data: *mut c_void, child_list_id: usize, sibling_list_id: usize),
    /// Visit the `greater than or equal` binary operation, which takes the left sub expression id `a`
    /// and the right sub-expression id `b`.
    pub visit_ge: extern "C" fn(data: *mut c_void, child_list_id: usize, sibling_list_id: usize),
    /// Visit the `equal` binary operation, which takes the left sub expression id `a`
    /// and the right sub-expression id `b`.
    pub visit_eq: extern "C" fn(data: *mut c_void, child_list_id: usize, sibling_list_id: usize),
    /// Visit the `not equal` binary operation, which takes the left sub expression id `a`
    /// and the right sub-expression id `b`.
    pub visit_ne: extern "C" fn(data: *mut c_void, child_list_id: usize, sibling_list_id: usize),
    /// Visit the `distinct` binary operation, which takes the left sub expression id `a`
    /// and the right sub-expression id `b`.
    pub visit_distinct:
        extern "C" fn(data: *mut c_void, child_list_id: usize, sibling_list_id: usize),
    /// Visit the `in` binary operation, which takes the left sub expression id `a`
    /// and the right sub-expression id `b`.
    pub visit_in: extern "C" fn(data: *mut c_void, child_list_id: usize, sibling_list_id: usize),
    /// Visit the `not in` binary operation, which takes the left sub expression id `a`
    /// and the right sub-expression id `b`.
    pub visit_not_in:
        extern "C" fn(data: *mut c_void, child_list_id: usize, sibling_list_id: usize),
    /// Visit the `add` binary operation, which takes the left sub expression id `a`
    /// and the right sub-expression id `b`.
    pub visit_add: extern "C" fn(data: *mut c_void, child_list_id: usize, sibling_list_id: usize),
    /// Visit the `minus` binary operation, which takes the left sub expression id `a`
    /// and the right sub-expression id `b`.
    pub visit_minus: extern "C" fn(data: *mut c_void, child_list_id: usize, sibling_list_id: usize),
    /// Visit the `multiply` binary operation, which takes the left sub expression id `a`
    /// and the right sub-expression id `b`.
    pub visit_multiply:
        extern "C" fn(data: *mut c_void, child_list_id: usize, sibling_list_id: usize),
    /// Visit the `divide` binary operation, which takes the left sub expression id `a`
    /// and the right sub-expression id `b`.
    pub visit_divide:
        extern "C" fn(data: *mut c_void, child_list_id: usize, sibling_list_id: usize),
    /// Visit the `colmun` identified by the `name` string.
    pub visit_column:
        extern "C" fn(data: *mut c_void, name: KernelStringSlice, sibling_list_id: usize),
    /// Visit a `struct` which is constructed from an ordered list of expressions. This declares
    /// the number of expressions that the struct will be made of. The visitor will populate the
    /// list of expressions using the [`visit_struct_sub_expr`] method.
    pub visit_struct_expr:
        extern "C" fn(data: *mut c_void, child_list_id: usize, sibling_list_id: usize),
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
    fn visit_array(
        visitor: &mut EngineExpressionVisitor,
        array: &ArrayData,
        sibling_list_id: usize,
    ) {
        #[allow(deprecated)]
        let elements = array.array_elements();
        let child_list_id = call!(visitor, make_field_list, elements.len());
        for scalar in elements {
            visit_scalar(visitor, scalar, child_list_id);
        }
        call!(visitor, visit_array_literal, child_list_id, sibling_list_id);
    }
    fn visit_struct_literal(
        visitor: &mut EngineExpressionVisitor,
        struct_data: &StructData,
        sibling_list_id: usize,
    ) {
        let child_value_list_id = call!(visitor, make_field_list, struct_data.fields().len());
        let child_field_list_id = call!(visitor, make_field_list, struct_data.fields().len());
        for (field, value) in struct_data.fields().iter().zip(struct_data.values()) {
            visit_scalar(
                visitor,
                &Scalar::String(field.name.clone()),
                child_field_list_id,
            );
            visit_scalar(visitor, value, child_value_list_id);
        }
        call!(
            visitor,
            visit_struct_literal,
            child_field_list_id,
            child_value_list_id,
            sibling_list_id
        )
    }
    fn visit_struct_expr(
        visitor: &mut EngineExpressionVisitor,
        exprs: &Vec<Expression>,
        sibling_list_id: usize,
    ) {
        let child_list_id = call!(visitor, make_field_list, exprs.len());
        for expr in exprs {
            visit_expression_impl(visitor, expr, child_list_id);
        }
        call!(visitor, visit_struct_expr, child_list_id, sibling_list_id)
    }
    fn visit_variadic(
        visitor: &mut EngineExpressionVisitor,
        op: &VariadicOperator,
        exprs: &Vec<Expression>,
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
        visit_fn(visitor.data, child_list_id, sibling_list_id);
    }
    fn visit_scalar(
        visitor: &mut EngineExpressionVisitor,
        scalar: &Scalar,
        sibling_list_id: usize,
    ) {
        match scalar {
            Scalar::Integer(val) => call!(visitor, visit_int_literal, *val, sibling_list_id),
            Scalar::Long(val) => call!(visitor, visit_long_literal, *val, sibling_list_id),
            Scalar::Short(val) => call!(visitor, visit_short_literal, *val, sibling_list_id),
            Scalar::Byte(val) => call!(visitor, visit_byte_literal, *val, sibling_list_id),
            Scalar::Float(val) => call!(visitor, visit_float_literal, *val, sibling_list_id),
            Scalar::Double(val) => call!(visitor, visit_double_literal, *val, sibling_list_id),
            Scalar::String(val) => {
                call!(visitor, visit_string_literal, val.into(), sibling_list_id)
            }
            Scalar::Boolean(val) => call!(visitor, visit_bool_literal, *val, sibling_list_id),
            Scalar::Timestamp(val) => {
                call!(visitor, visit_timestamp_literal, *val, sibling_list_id)
            }
            Scalar::TimestampNtz(val) => {
                call!(visitor, visit_timestamp_ntz_literal, *val, sibling_list_id)
            }
            Scalar::Date(val) => call!(visitor, visit_date_literal, *val, sibling_list_id),
            Scalar::Binary(buf) => call!(
                visitor,
                visit_binary_literal,
                buf.as_ptr(),
                buf.len(),
                sibling_list_id
            ),
            Scalar::Decimal(value, precision, scale) => {
                let ms: u64 = (value >> 64) as u64;
                let ls: u64 = *value as u64;
                call!(
                    visitor,
                    visit_decimal_literal,
                    ms,
                    ls,
                    *precision,
                    *scale,
                    sibling_list_id
                )
            }
            Scalar::Null(_) => call!(visitor, visit_null_literal, sibling_list_id),
            Scalar::Struct(struct_data) => {
                visit_struct_literal(visitor, struct_data, sibling_list_id)
            }
            Scalar::Array(array) => visit_array(visitor, array, sibling_list_id),
        }
    }
    fn visit_expression_impl(
        visitor: &mut EngineExpressionVisitor,
        expression: &Expression,
        sibling_list_id: usize,
    ) {
        match expression {
            Expression::Literal(scalar) => visit_scalar(visitor, scalar, sibling_list_id),
            Expression::Column(name) => call!(visitor, visit_column, name.into(), sibling_list_id),
            Expression::Struct(exprs) => visit_struct_expr(visitor, exprs, sibling_list_id),
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
                op(visitor.data, child_list_id, sibling_list_id);
            }
            Expression::UnaryOperation { op, expr } => {
                let child_id_list = call!(visitor, make_field_list, 1);
                visit_expression_impl(visitor, expr, child_id_list);
                let op = match op {
                    UnaryOperator::Not => visitor.visit_not,
                    UnaryOperator::IsNull => visitor.visit_is_null,
                };
                op(visitor.data, child_id_list, sibling_list_id);
            }
            Expression::VariadicOperation { op, exprs } => {
                visit_variadic(visitor, op, exprs, sibling_list_id)
            }
        }
    }
    let top_level = call!(visitor, make_field_list, 1);
    visit_expression_impl(visitor, expression.as_ref(), top_level);
    top_level
}

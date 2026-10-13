use delta_kernel_derive::pub_macro;

/// Builds an [`Expression`](crate::expressions::Expression) from nested constructor calls.
///
/// Supports `lit(value)`, typed NULLs such as `null(LONG)`, `col("a.b")`, explicit column
/// segments such as `path("a.b", "c")`, COALESCE, AND/OR/NOT, and unary/binary predicate
/// builders such as `is_null`, `is_not_null`, `distinct`, and `gt`. Every call returns an
/// `Expression`, including predicate calls.
///
/// Literals retain their Rust type: `lit(1)` is INTEGER and `lit(1_i64)` is LONG.
/// `null` takes a [`DataType`](crate::schema::DataType) associated constant name, such as `LONG`.
/// `col` takes one string literal and follows [`col!`](crate::expressions::col) path rules.
/// `col("a.b")` addresses nested fields; `path("a.b")` addresses one field whose name contains a
/// dot. Use `path` for multiple explicit segments or computed names. Literal arguments and path
/// segments are evaluated once, in order.
///
/// Calls delegate to the existing builders without resolving types or evaluating rows.
/// AND/OR preserve [`Predicate::junction`](crate::expressions::Predicate::junction)
/// normalization: empty AND is TRUE, empty OR is FALSE, and single-input junctions unwrap.
/// Invalid call shapes, unknown predicate builders, and unsupported literal types fail to compile.
///
/// ```
/// use delta_kernel::expressions::{col, expr, lit, Expression, Predicate};
///
/// assert_eq!(
///     expr!(and(
///         is_not_null(col("path")),
///         le(coalesce(col("size"), lit(0_i64)), lit(100_i64)),
///     )),
///     Expression::from_pred(Predicate::and(
///         col!("path").is_not_null(),
///         Expression::coalesce([col!("size"), lit(0_i64)]).le(lit(100_i64)),
///     )),
/// );
/// ```
///
/// Macro and crate imports can be aliased:
///
/// ```
/// use delta_kernel as kernel;
/// use kernel::expressions::{expr as kernel_expr, null_lit, Expression};
/// use kernel::schema::DataType;
///
/// assert_eq!(
///     kernel_expr!(coalesce(path("a.b"), null(LONG))),
///     Expression::coalesce([Expression::column(["a.b"]), null_lit(DataType::LONG)]),
/// );
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::expr;
/// let _ = expr!(a + b);
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::expr;
/// let _ = expr!(not(col("a"), col("b"), col("c")));
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::expr;
/// let _ = expr!(unknown_predicate(col("a")));
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::{expr, Expression, Predicate};
/// trait CustomPredicate {
///     fn custom_predicate(self) -> Predicate;
/// }
/// impl CustomPredicate for Expression {
///     fn custom_predicate(self) -> Predicate { Predicate::TRUE }
/// }
/// let _ = expr!(custom_predicate(col("a")));
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::expr;
/// let _ = expr!(not());
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::expr;
/// let _ = expr!(not(col("a"), col("b")));
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::{expr, Expression, Predicate};
/// trait BinaryNot {
///     fn not(self, other: Expression) -> Predicate;
/// }
/// impl BinaryNot for Expression {
///     fn not(self, _other: Expression) -> Predicate { Predicate::TRUE }
/// }
/// let _ = expr!(not(col("a"), col("b")));
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::expr;
/// let _ = expr!(gt(col("a")));
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::expr;
/// let _ = expr!(is_null(col("a"), col("b")));
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::expr;
/// let _ = expr!(path());
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::expr;
/// let _ = expr!(col("a", "b"));
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::expr;
/// const COLUMN: &str = "a";
/// let _ = expr!(col(COLUMN));
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::expr;
/// let _ = expr!(null(decimal(10, 2)));
/// ```
///
/// ```compile_fail,E0277
/// use delta_kernel::expressions::expr;
/// let _ = expr!(lit(1_u64));
/// ```
#[pub_macro]
macro_rules! expr {
    (col($path:literal)) => { $crate::expressions::col!($path) };
    (col($($other:tt)*)) => {
        ::core::compile_error!(
            "col() requires one string literal; use path() for explicit or computed column segments"
        )
    };
    (path($($segment:expr),+ $(,)?)) => {
        $crate::expressions::Expression::column([$($segment),+])
    };
    (lit($value:expr)) => { $crate::expressions::lit($value) };
    (null($ty:ident)) => {
        $crate::expressions::null_lit($crate::schema::DataType::$ty)
    };
    (@call coalesce; $($arg:expr),*) => {
        $crate::expressions::Expression::coalesce(
            ::std::vec::Vec::<$crate::expressions::Expression>::from([$($arg),*]),
        )
    };
    (@call and; $($arg:expr),*) => {
        $crate::expressions::Expression::from_pred(
            $crate::expressions::Predicate::and_from([
                $($crate::expressions::Predicate::from_expr($arg)),*
            ]),
        )
    };
    (@call or; $($arg:expr),*) => {
        $crate::expressions::Expression::from_pred(
            $crate::expressions::Predicate::or_from([
                $($crate::expressions::Predicate::from_expr($arg)),*
            ]),
        )
    };
    (@call not; $arg:expr) => {
        $crate::expressions::Expression::from_pred(
            $crate::expressions::Predicate::not(
                $crate::expressions::Predicate::from_expr($arg),
            ),
        )
    };
    (@call is_null; $arg:expr) => {
        $crate::expressions::Expression::from_pred(
            $crate::expressions::Predicate::is_null($arg),
        )
    };
    (@call is_not_null; $arg:expr) => {
        $crate::expressions::Expression::from_pred(
            $crate::expressions::Predicate::is_not_null($arg),
        )
    };
    (@call eq; $left:expr, $right:expr) => {
        $crate::expressions::Expression::from_pred(
            $crate::expressions::Predicate::eq($left, $right),
        )
    };
    (@call ne; $left:expr, $right:expr) => {
        $crate::expressions::Expression::from_pred(
            $crate::expressions::Predicate::ne($left, $right),
        )
    };
    (@call lt; $left:expr, $right:expr) => {
        $crate::expressions::Expression::from_pred(
            $crate::expressions::Predicate::lt($left, $right),
        )
    };
    (@call le; $left:expr, $right:expr) => {
        $crate::expressions::Expression::from_pred(
            $crate::expressions::Predicate::le($left, $right),
        )
    };
    (@call gt; $left:expr, $right:expr) => {
        $crate::expressions::Expression::from_pred(
            $crate::expressions::Predicate::gt($left, $right),
        )
    };
    (@call ge; $left:expr, $right:expr) => {
        $crate::expressions::Expression::from_pred(
            $crate::expressions::Predicate::ge($left, $right),
        )
    };
    (@call distinct; $left:expr, $right:expr) => {
        $crate::expressions::Expression::from_pred(
            $crate::expressions::Predicate::distinct($left, $right),
        )
    };
    (@call $op:ident; $($arg:expr),*) => {
        ::core::compile_error!(::core::concat!(
            "unsupported expression call or arity: ", ::core::stringify!($op)
        ))
    };
    ($op:ident($($child:ident($($args:tt)*)),* $(,)?)) => {
        $crate::expressions::expr!(@call $op; $($crate::expressions::expr!($child($($args)*))),*)
    };
    ($($other:tt)*) => {
        ::core::compile_error!("expected nested calls, e.g. coalesce(col(\"a\"), lit(0_i64))")
    };
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use crate::expressions::{
        col, expr, lit, null_lit, BinaryPredicateOp, Expression, Predicate, Scalar,
        UnaryPredicateOp,
    };
    use crate::schema::DataType;

    #[test]
    fn nested_predicate_preserves_boolean_structure_and_coalesce_order() {
        let actual = expr!(and(
            is_not_null(col("path")),
            not(or(
                distinct(col("deleted"), lit(false)),
                le(
                    coalesce(col("size"), coalesce(col("fallback_size"), lit(0_i64))),
                    lit(15_i64),
                ),
            )),
        ));
        let expected = Expression::from_pred(Predicate::and(
            col!("path").is_not_null(),
            Predicate::not(Predicate::or(
                col!("deleted").distinct(lit(false)),
                Expression::coalesce([
                    col!("size"),
                    Expression::coalesce([col!("fallback_size"), lit(0_i64)]),
                ])
                .le(lit(15_i64)),
            )),
        ));
        assert_eq!(actual, expected);
    }

    #[rstest]
    #[case::coalesce_empty(expr!(coalesce()), Expression::coalesce(Vec::<Expression>::new()))]
    #[case::coalesce_single(expr!(coalesce(col("a"))), Expression::coalesce([col!("a")]))]
    #[case::coalesce_three(
        expr!(coalesce(col("a"), col("b"), col("c"))),
        Expression::coalesce([col!("a"), col!("b"), col!("c")]),
    )]
    #[case::and_empty(expr!(and()), Expression::from_pred(Predicate::TRUE))]
    #[case::and_single(expr!(and(col("a"))), col!("a"))]
    #[case::and_three(
        expr!(and(col("a"), col("b"), col("c"))),
        Expression::from_pred(Predicate::and_from([
            Predicate::column(["a"]), Predicate::column(["b"]), Predicate::column(["c"]),
        ])),
    )]
    #[case::or_empty(expr!(or()), Expression::from_pred(Predicate::FALSE))]
    #[case::or_single(expr!(or(col("a"))), col!("a"))]
    #[case::or_three(
        expr!(or(col("a"), col("b"), col("c"))),
        Expression::from_pred(Predicate::or_from([
            Predicate::column(["a"]), Predicate::column(["b"]), Predicate::column(["c"]),
        ])),
    )]
    #[case::boolean_expressions(
        expr!(and(col("a"), not(col("b")), gt(col("size"), lit(0_i64)))),
        Expression::from_pred(Predicate::and_from([
            Predicate::column(["a"]),
            Predicate::not(Predicate::column(["b"])),
            col!("size").gt(lit(0_i64)),
        ])),
    )]
    fn variadic_calls_preserve_operands_and_junction_identities(
        #[case] actual: Expression,
        #[case] expected: Expression,
    ) {
        assert_eq!(actual, expected);
    }

    #[rstest]
    #[case::nested(expr!(col("a.b")), &["a", "b"])]
    #[case::literal_dot(expr!(path("a.b")), &["a.b"])]
    #[case::explicit_segments(expr!(path("outer", "inner.with.dot", "leaf")),
        &["outer", "inner.with.dot", "leaf"])]
    fn column_paths_distinguish_literal_dots_from_nested_fields(
        #[case] actual: Expression,
        #[case] segments: &[&str],
    ) {
        assert_eq!(actual, Expression::column(segments.iter().copied()));
    }

    #[rstest]
    #[case::lt(expr!(lt(col("left"), lit(7_i64))), BinaryPredicateOp::LessThan, false)]
    #[case::gt(expr!(gt(col("left"), lit(7_i64))), BinaryPredicateOp::GreaterThan, false)]
    #[case::eq(expr!(eq(col("left"), lit(7_i64))), BinaryPredicateOp::Equal, false)]
    #[case::distinct(expr!(distinct(col("left"), lit(7_i64))), BinaryPredicateOp::Distinct, false)]
    #[case::le(expr!(le(col("left"), lit(7_i64))), BinaryPredicateOp::GreaterThan, true)]
    #[case::ge(expr!(ge(col("left"), lit(7_i64))), BinaryPredicateOp::LessThan, true)]
    #[case::ne(expr!(ne(col("left"), lit(7_i64))), BinaryPredicateOp::Equal, true)]
    fn comparisons_preserve_operators_operand_order_and_negation(
        #[case] actual: Expression,
        #[case] op: BinaryPredicateOp,
        #[case] negated: bool,
    ) {
        let expected = Predicate::binary(op, col!("left"), lit(7_i64));
        let expected = if negated {
            Predicate::not(expected)
        } else {
            expected
        };
        assert_eq!(actual, Expression::from_pred(expected));
    }

    #[rstest]
    #[case::is_null(expr!(is_null(col("a"))), false)]
    #[case::is_not_null(expr!(is_not_null(col("a"))), true)]
    fn null_predicates_preserve_is_null_and_negation(
        #[case] actual: Expression,
        #[case] negated: bool,
    ) {
        let expected = Predicate::unary(UnaryPredicateOp::IsNull, col!("a"));
        let expected = if negated {
            Predicate::not(expected)
        } else {
            expected
        };
        assert_eq!(actual, Expression::from_pred(expected));
    }

    #[rstest]
    #[case::unsuffixed_integer(expr!(lit(1)), Scalar::Integer(1))]
    #[case::long(expr!(lit(1_i64)), Scalar::Long(1))]
    #[case::byte(expr!(lit(i8::MIN)), Scalar::Byte(i8::MIN))]
    #[case::short(expr!(lit(i16::MIN)), Scalar::Short(i16::MIN))]
    #[case::integer_min(expr!(lit(i32::MIN)), Scalar::Integer(i32::MIN))]
    #[case::long_max(expr!(lit(i64::MAX)), Scalar::Long(i64::MAX))]
    #[case::true_literal(expr!(lit(true)), Scalar::Boolean(true))]
    #[case::false_literal(expr!(lit(false)), Scalar::Boolean(false))]
    #[case::float(expr!(lit(1.5_f32)), Scalar::Float(1.5))]
    #[case::double(expr!(lit(-2.5_f64)), Scalar::Double(-2.5))]
    #[case::empty_string(expr!(lit("")), Scalar::String(String::new()))]
    #[case::owned_string(expr!(lit(String::from("a\0b\n"))), Scalar::String("a\0b\n".into()))]
    #[case::binary(expr!(lit(vec![0_u8, 255])), Scalar::Binary(vec![0, 255]))]
    #[case::scalar(expr!(lit(Scalar::Date(-1))), Scalar::Date(-1))]
    fn literals_preserve_exact_scalar_variants(
        #[case] actual: Expression,
        #[case] expected: Scalar,
    ) {
        assert_eq!(actual, Expression::Literal(expected));
    }

    #[rstest]
    #[case::integer(expr!(null(INTEGER)), DataType::INTEGER)]
    #[case::long(expr!(null(LONG)), DataType::LONG)]
    #[case::boolean(expr!(null(BOOLEAN)), DataType::BOOLEAN)]
    #[case::string(expr!(null(STRING)), DataType::STRING)]
    #[case::double(expr!(null(DOUBLE)), DataType::DOUBLE)]
    fn nulls_preserve_data_types(#[case] actual: Expression, #[case] data_type: DataType) {
        assert_eq!(actual, null_lit(data_type));
    }

    #[test]
    fn literal_arguments_are_evaluated_once_in_order() {
        let mut calls = 0_i64;
        let mut next = || {
            calls += 1;
            calls
        };
        let actual = expr!(coalesce(lit(next()), lit(next())));
        assert_eq!(calls, 2);
        assert_eq!(actual, Expression::coalesce([lit(1_i64), lit(2_i64)]));
    }

    #[test]
    fn path_segments_accept_owned_names_and_are_evaluated_once_in_order() {
        let mut calls = 0;
        let mut next = || {
            calls += 1;
            format!("field.{calls}")
        };
        let actual = expr!(path(next(), next(),));
        assert_eq!(calls, 2);
        assert_eq!(actual, Expression::column(["field.1", "field.2"]));
    }

    #[test]
    fn binary_predicate_arguments_are_evaluated_once_in_order() {
        let mut calls = 0_i64;
        let mut next = || {
            calls += 1;
            calls
        };
        let actual = expr!(gt(lit(next()), lit(next()),));
        assert_eq!(calls, 2);
        assert_eq!(actual, Expression::from_pred(lit(1_i64).gt(lit(2_i64))));
    }

    mod macro_hygiene {
        type Vec = ();
        type Expression = ();
        type Predicate = ();

        #[test]
        fn expressions_resolve_types_independently_of_caller_names() {
            let _caller_types: (Vec, Expression, Predicate) = ((), (), ());
            assert_eq!(
                crate::expressions::expr!(coalesce(lit(false), null(BOOLEAN))),
                crate::expressions::Expression::coalesce([
                    crate::expressions::lit(false),
                    crate::expressions::null_lit(crate::schema::DataType::BOOLEAN),
                ]),
            );
        }
    }
}

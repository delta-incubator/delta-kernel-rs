use super::*;
use crate::expressions::{ArrayData, Expression, StructData, UnaryOperator};
use crate::predicates::PredicateEvaluator;
use crate::schema::ArrayType;
use crate::DataType;

macro_rules! expect_eq {
    ( $expr: expr, $expect: expr, $fmt: literal ) => {
        let expect = ($expect);
        let result = ($expr);
        assert!(
            result == expect,
            "Expected {} = {:?}, got {:?}",
            format!($fmt),
            expect,
            result
        );
    };
}

struct UnimplementedColumnResolver;
impl ResolveColumnAsScalar for UnimplementedColumnResolver {
    fn resolve_column(&self, _col: &str) -> Option<Scalar> {
        unimplemented!()
    }
}

impl ResolveColumnAsScalar for Scalar {
    fn resolve_column(&self, _col: &str) -> Option<Scalar> {
        Some(self.clone())
    }
}

#[test]
fn test_default_eval_scalar() {
    let test_cases = [
        (Scalar::Boolean(true), false, Some(true)),
        (Scalar::Boolean(true), true, Some(false)),
        (Scalar::Boolean(false), false, Some(false)),
        (Scalar::Boolean(false), true, Some(true)),
        (Scalar::Long(1), false, None),
        (Scalar::Long(1), true, None),
        (Scalar::Null(DataType::BOOLEAN), false, None),
        (Scalar::Null(DataType::BOOLEAN), true, None),
        (Scalar::Null(DataType::LONG), false, None),
        (Scalar::Null(DataType::LONG), true, None),
    ];
    for (value, inverted, expect) in test_cases.into_iter() {
        assert_eq!(
            PredicateEvaluatorDefaults::eval_scalar(&value, inverted),
            expect,
            "value: {value:?} inverted: {inverted}"
        );
    }
}

// verifies that partial orderings behave as excpected for all Scalar types
#[test]
fn test_default_partial_cmp_scalars() {
    use Ordering::*;
    use Scalar::*;

    let smaller_values = &[
        Integer(1),
        Long(1),
        Short(1),
        Byte(1),
        Float(1.0),
        Double(1.0),
        String("1".into()),
        Boolean(false),
        Timestamp(1),
        TimestampNtz(1),
        Date(1),
        Binary(vec![1]),
        Decimal(1, 10, 10), // invalid value,
        Null(DataType::LONG),
        Struct(StructData::try_new(vec![], vec![]).unwrap()),
        Array(ArrayData::new(
            ArrayType::new(DataType::LONG, false),
            vec![],
        )),
    ];
    let larger_values = &[
        Integer(10),
        Long(10),
        Short(10),
        Byte(10),
        Float(10.0),
        Double(10.0),
        String("10".into()),
        Boolean(true),
        Timestamp(10),
        TimestampNtz(10),
        Date(10),
        Binary(vec![10]),
        Decimal(10, 10, 10), // invalid value
        Null(DataType::LONG),
        Struct(StructData::try_new(vec![], vec![]).unwrap()),
        Array(ArrayData::new(
            ArrayType::new(DataType::LONG, false),
            vec![],
        )),
    ];

    // scalars of different types are always incomparable
    let compare = PredicateEvaluatorDefaults::partial_cmp_scalars;
    for (i, a) in smaller_values.iter().enumerate() {
        for b in smaller_values.iter().skip(i + 1) {
            for op in [Less, Equal, Greater] {
                for inverted in [true, false] {
                    assert!(
                        compare(op, a, b, inverted).is_none(),
                        "{:?} should not be comparable to {:?}",
                        a.data_type(),
                        b.data_type()
                    );
                }
            }
        }
    }

    let expect_if_comparable_type = |s: &_, expect| match s {
        Null(_) | Decimal(..) | Struct(_) | Array(_) => None,
        _ => Some(expect),
    };

    // Test same-type comparisons where a == b
    for (a, b) in smaller_values.iter().zip(smaller_values) {
        for inverted in [true, false] {
            expect_eq!(
                compare(Less, a, b, inverted),
                expect_if_comparable_type(a, inverted),
                "{a:?} < {b:?} (inverted: {inverted})"
            );

            expect_eq!(
                compare(Equal, a, b, inverted),
                expect_if_comparable_type(a, !inverted),
                "{a:?} == {b:?} (inverted: {inverted})"
            );

            expect_eq!(
                compare(Greater, a, b, inverted),
                expect_if_comparable_type(a, inverted),
                "{a:?} > {b:?} (inverted: {inverted})"
            );
        }
    }

    // Test same-type comparisons where a < b
    for (a, b) in smaller_values.iter().zip(larger_values) {
        for inverted in [true, false] {
            expect_eq!(
                compare(Less, a, b, inverted),
                expect_if_comparable_type(a, !inverted),
                "{a:?} < {b:?} (inverted: {inverted})"
            );

            expect_eq!(
                compare(Equal, a, b, inverted),
                expect_if_comparable_type(a, inverted),
                "{a:?} == {b:?} (inverted: {inverted})"
            );

            expect_eq!(
                compare(Greater, a, b, inverted),
                expect_if_comparable_type(a, inverted),
                "{a:?} < {b:?} (inverted: {inverted})"
            );

            expect_eq!(
                compare(Less, b, a, inverted),
                expect_if_comparable_type(a, inverted),
                "{b:?} < {a:?} (inverted: {inverted})"
            );

            expect_eq!(
                compare(Equal, b, a, inverted),
                expect_if_comparable_type(a, inverted),
                "{b:?} == {a:?} (inverted: {inverted})"
            );

            expect_eq!(
                compare(Greater, b, a, inverted),
                expect_if_comparable_type(a, !inverted),
                "{b:?} < {a:?} (inverted: {inverted})"
            );
        }
    }
}

// Verifies that eval_binary_scalars uses partial_cmp_scalars correctly
#[test]
fn test_eval_binary_scalars() {
    use BinaryOperator::*;
    let smaller_value = Scalar::Long(1);
    let larger_value = Scalar::Long(10);
    for inverted in [true, false] {
        let compare = PredicateEvaluatorDefaults::eval_binary_scalars;
        expect_eq!(
            compare(Equal, &smaller_value, &smaller_value, inverted),
            Some(!inverted),
            "{smaller_value} == {smaller_value} (inverted: {inverted})"
        );
        expect_eq!(
            compare(Equal, &smaller_value, &larger_value, inverted),
            Some(inverted),
            "{smaller_value} == {larger_value} (inverted: {inverted})"
        );

        expect_eq!(
            compare(NotEqual, &smaller_value, &smaller_value, inverted),
            Some(inverted),
            "{smaller_value} != {smaller_value} (inverted: {inverted})"
        );
        expect_eq!(
            compare(NotEqual, &smaller_value, &larger_value, inverted),
            Some(!inverted),
            "{smaller_value} != {larger_value} (inverted: {inverted})"
        );

        expect_eq!(
            compare(LessThan, &smaller_value, &smaller_value, inverted),
            Some(inverted),
            "{smaller_value} < {smaller_value} (inverted: {inverted})"
        );
        expect_eq!(
            compare(LessThan, &smaller_value, &larger_value, inverted),
            Some(!inverted),
            "{smaller_value} < {larger_value} (inverted: {inverted})"
        );

        expect_eq!(
            compare(GreaterThan, &smaller_value, &smaller_value, inverted),
            Some(inverted),
            "{smaller_value} > {smaller_value} (inverted: {inverted})"
        );
        expect_eq!(
            compare(GreaterThan, &smaller_value, &larger_value, inverted),
            Some(inverted),
            "{smaller_value} > {larger_value} (inverted: {inverted})"
        );

        expect_eq!(
            compare(LessThanOrEqual, &smaller_value, &smaller_value, inverted),
            Some(!inverted),
            "{smaller_value} <= {smaller_value} (inverted: {inverted})"
        );
        expect_eq!(
            compare(LessThanOrEqual, &smaller_value, &larger_value, inverted),
            Some(!inverted),
            "{smaller_value} <= {larger_value} (inverted: {inverted})"
        );

        expect_eq!(
            compare(GreaterThanOrEqual, &smaller_value, &smaller_value, inverted),
            Some(!inverted),
            "{smaller_value} >= {smaller_value} (inverted: {inverted})"
        );
        expect_eq!(
            compare(GreaterThanOrEqual, &smaller_value, &larger_value, inverted),
            Some(inverted),
            "{smaller_value} >= {larger_value} (inverted: {inverted})"
        );
    }
}

#[test]
fn test_eval_variadic() {
    let test_cases: Vec<(&[_], _, _)> = vec![
        // input, AND expect, OR expect
        (&[], Some(true), Some(false)),
        (&[Some(true)], Some(true), Some(true)),
        (&[Some(false)], Some(false), Some(false)),
        (&[None], None, None),
        (&[Some(true), Some(false)], Some(false), Some(true)),
        (&[Some(false), Some(true)], Some(false), Some(true)),
        (&[Some(true), None], None, Some(true)),
        (&[None, Some(true)], None, Some(true)),
        (&[Some(false), None], Some(false), None),
        (&[None, Some(false)], Some(false), None),
        (&[None, Some(false), Some(true)], Some(false), Some(true)),
        (&[None, Some(true), Some(false)], Some(false), Some(true)),
        (&[Some(false), None, Some(true)], Some(false), Some(true)),
        (&[Some(true), None, Some(false)], Some(false), Some(true)),
        (&[Some(false), Some(true), None], Some(false), Some(true)),
        (&[Some(true), Some(false), None], Some(false), Some(true)),
    ];
    let filter = DefaultPredicateEvaluator::from(UnimplementedColumnResolver);
    for (inputs, expect_and, expect_or) in test_cases.iter() {
        let inputs: Vec<_> = inputs
            .iter()
            .cloned()
            .map(|v| match v {
                Some(v) => Expression::literal(v),
                None => Expression::null_literal(DataType::BOOLEAN),
            })
            .collect();
        for inverted in [true, false] {
            let invert_if_needed = |v: &Option<_>| v.clone().map(|v| v != inverted);
            expect_eq!(
                filter.eval_variadic(VariadicOperator::And, &inputs, inverted),
                invert_if_needed(expect_and),
                "AND({inputs:?}) (inverted: {inverted})"
            );
            expect_eq!(
                filter.eval_variadic(VariadicOperator::Or, &inputs, inverted),
                invert_if_needed(expect_or),
                "OR({inputs:?}) (inverted: {inverted})"
            );
        }
    }
}

#[test]
fn test_eval_column() {
    let test_cases = [
        (Scalar::from(true), Some(true)),
        (Scalar::from(false), Some(false)),
        (Scalar::Null(DataType::BOOLEAN), None),
        (Scalar::from(1), None),
    ];
    let col = "x";
    for (input, expect) in &test_cases {
        let filter = DefaultPredicateEvaluator::from(input.clone());
        for inverted in [true, false] {
            expect_eq!(
                filter.eval_column(col, inverted),
                expect.map(|v| v != inverted),
                "{input:?} (inverted: {inverted})"
            );
        }
    }
}

#[test]
fn test_eval_not() {
    let test_cases = [
        (Scalar::Boolean(true), Some(false)),
        (Scalar::Boolean(false), Some(true)),
        (Scalar::Null(DataType::BOOLEAN), None),
        (Scalar::Long(1), None),
    ];
    let filter = DefaultPredicateEvaluator::from(UnimplementedColumnResolver);
    for (input, expect) in test_cases {
        let input = input.into();
        for inverted in [true, false] {
            expect_eq!(
                filter.eval_unary(UnaryOperator::Not, &input, inverted),
                expect.map(|v| v != inverted),
                "NOT({input:?}) (inverted: {inverted})"
            );
        }
    }
}

#[test]
fn test_eval_is_null() {
    let expr = Expression::column("x");
    let filter = DefaultPredicateEvaluator::from(Scalar::from(1));
    expect_eq!(
        filter.eval_unary(UnaryOperator::IsNull, &expr, true),
        Some(true),
        "x IS NOT NULL"
    );
    expect_eq!(
        filter.eval_unary(UnaryOperator::IsNull, &expr, false),
        Some(false),
        "x IS NULL"
    );

    let expr = Expression::literal(1);
    expect_eq!(
        filter.eval_unary(UnaryOperator::IsNull, &expr, true),
        None,
        "1 IS NOT NULL"
    );
    expect_eq!(
        filter.eval_unary(UnaryOperator::IsNull, &expr, false),
        None,
        "1 IS NULL"
    );
}

#[test]
fn test_eval_distinct() {
    let one = &Scalar::from(1);
    let two = &Scalar::from(2);
    let null = &Scalar::Null(DataType::INTEGER);
    let filter = DefaultPredicateEvaluator::from(one.clone());
    let col = "x";
    expect_eq!(
        filter.eval_distinct(col, one, true),
        Some(true),
        "NOT DISTINCT(x, 1) (x = 1)"
    );
    expect_eq!(
        filter.eval_distinct(col, one, false),
        Some(false),
        "DISTINCT(x, 1) (x = 1)"
    );
    expect_eq!(
        filter.eval_distinct(col, two, true),
        Some(false),
        "NOT DISTINCT(x, 2) (x = 1)"
    );
    expect_eq!(
        filter.eval_distinct(col, two, false),
        Some(true),
        "DISTINCT(x, 2) (x = 1)"
    );
    expect_eq!(
        filter.eval_distinct(col, null, true),
        Some(false),
        "NOT DISTINCT(x, NULL) (x = 1)"
    );
    expect_eq!(
        filter.eval_distinct(col, null, false),
        Some(true),
        "DISTINCT(x, NULL) (x = 1)"
    );

    let filter = DefaultPredicateEvaluator::from(null.clone());
    expect_eq!(
        filter.eval_distinct(col, one, true),
        Some(false),
        "NOT DISTINCT(x, 1) (x = NULL)"
    );
    expect_eq!(
        filter.eval_distinct(col, one, false),
        Some(true),
        "DISTINCT(x, 1) (x = NULL)"
    );
    expect_eq!(
        filter.eval_distinct(col, null, true),
        Some(true),
        "NOT DISTINCT(x, NULL) (x = NULL)"
    );
    expect_eq!(
        filter.eval_distinct(col, null, false),
        Some(false),
        "DISTINCT(x, NULL) (x = NULL)"
    );
}

// NOTE: We're testing routing here -- the actual comparisons are already validated by
// test_eval_binary_scalars.
#[test]
fn eval_binary() {
    let col = Expression::column("x");
    let val = Expression::literal(10);
    let filter = DefaultPredicateEvaluator::from(Scalar::from(1));

    // unsupported
    expect_eq!(
        filter.eval_binary(BinaryOperator::Plus, &col, &val, false),
        None,
        "x + 10"
    );
    expect_eq!(
        filter.eval_binary(BinaryOperator::Minus, &col, &val, false),
        None,
        "x - 10"
    );
    expect_eq!(
        filter.eval_binary(BinaryOperator::Multiply, &col, &val, false),
        None,
        "x * 10"
    );
    expect_eq!(
        filter.eval_binary(BinaryOperator::Divide, &col, &val, false),
        None,
        "x / 10"
    );

    // supported
    for inverted in [true, false] {
        expect_eq!(
            filter.eval_binary(BinaryOperator::LessThan, &col, &val, inverted),
            Some(!inverted),
            "x < 10 (inverted: {inverted})"
        );
        expect_eq!(
            filter.eval_binary(BinaryOperator::LessThanOrEqual, &col, &val, inverted),
            Some(!inverted),
            "x <= 10 (inverted: {inverted})"
        );
        expect_eq!(
            filter.eval_binary(BinaryOperator::Equal, &col, &val, inverted),
            Some(inverted),
            "x = 10 (inverted: {inverted})"
        );
        expect_eq!(
            filter.eval_binary(BinaryOperator::NotEqual, &col, &val, inverted),
            Some(!inverted),
            "x != 10 (inverted: {inverted})"
        );
        expect_eq!(
            filter.eval_binary(BinaryOperator::GreaterThanOrEqual, &col, &val, inverted),
            Some(inverted),
            "x >= 10 (inverted: {inverted})"
        );
        expect_eq!(
            filter.eval_binary(BinaryOperator::GreaterThan, &col, &val, inverted),
            Some(inverted),
            "x > 10 (inverted: {inverted})"
        );
        expect_eq!(
            filter.eval_binary(BinaryOperator::Distinct, &col, &val, inverted),
            Some(!inverted),
            "DISTINCT(x, 10) (inverted: {inverted})"
        );

        expect_eq!(
            filter.eval_binary(BinaryOperator::LessThan, &val, &col, inverted),
            Some(inverted),
            "10 < x (inverted: {inverted})"
        );
        expect_eq!(
            filter.eval_binary(BinaryOperator::LessThanOrEqual, &val, &col, inverted),
            Some(inverted),
            "10 <= x (inverted: {inverted})"
        );
        expect_eq!(
            filter.eval_binary(BinaryOperator::Equal, &val, &col, inverted),
            Some(inverted),
            "10 = x (inverted: {inverted})"
        );
        expect_eq!(
            filter.eval_binary(BinaryOperator::NotEqual, &val, &col, inverted),
            Some(!inverted),
            "10 != x (inverted: {inverted})"
        );
        expect_eq!(
            filter.eval_binary(BinaryOperator::GreaterThanOrEqual, &val, &col, inverted),
            Some(!inverted),
            "10 >= x (inverted: {inverted})"
        );
        expect_eq!(
            filter.eval_binary(BinaryOperator::GreaterThan, &val, &col, inverted),
            Some(!inverted),
            "10 > x (inverted: {inverted})"
        );
        expect_eq!(
            filter.eval_binary(BinaryOperator::Distinct, &val, &col, inverted),
            Some(!inverted),
            "DISTINCT(10, x) (inverted: {inverted})"
        );
    }
}

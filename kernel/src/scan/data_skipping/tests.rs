use super::*;

use crate::predicates::{DefaultPredicateEvaluator, UnimplementedColumnResolver};
use std::collections::HashMap;

const TRUE: Option<bool> = Some(true);
const FALSE: Option<bool> = Some(false);
const NULL: Option<bool> = None;

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

#[test]
fn test_eval_is_null() {
    let col = &Expr::column("x");
    let expressions = [Expr::is_null(col.clone()), !Expr::is_null(col.clone())];

    let do_test = |nullcount: i64, expected: &[Option<bool>]| {
        let resolver = HashMap::from_iter([
            ("numRecords", Scalar::from(2i64)),
            ("nullCount.x", Scalar::from(nullcount)),
        ]);
        let filter = DefaultPredicateEvaluator::from(resolver);
        for (expr, expect) in expressions.iter().zip(expected) {
            let pred = as_data_skipping_predicate(&expr, false).unwrap();
            expect_eq!(
                filter.eval_expr(&pred, false),
                *expect,
                "{expr:#?} became {pred:#?} ({nullcount} nulls)"
            );
        }
    };

    // no nulls
    do_test(0, &[FALSE, TRUE]);

    // some nulls
    do_test(1, &[TRUE, TRUE]);

    // all nulls
    do_test(2, &[TRUE, FALSE]);
}

#[test]
fn test_eval_binary_comparisons() {
    let col = &Expr::column("x");
    let five = &Scalar::from(5);
    let ten = &Scalar::from(10);
    let fifteen = &Scalar::from(15);
    let null = &Scalar::Null(DataType::INTEGER);

    let expressions = [
        Expr::lt(col.clone(), ten.clone()),
        Expr::le(col.clone(), ten.clone()),
        Expr::eq(col.clone(), ten.clone()),
        Expr::ne(col.clone(), ten.clone()),
        Expr::gt(col.clone(), ten.clone()),
        Expr::ge(col.clone(), ten.clone()),
    ];

    let do_test = |min: &Scalar, max: &Scalar, expected: &[Option<bool>]| {
        let resolver =
            HashMap::from_iter([("minValues.x", min.clone()), ("maxValues.x", max.clone())]);
        let filter = DefaultPredicateEvaluator::from(resolver);
        for (expr, expect) in expressions.iter().zip(expected.iter()) {
            let pred = as_data_skipping_predicate(expr, false).unwrap();
            expect_eq!(
                filter.eval_expr(&pred, false),
                *expect,
                "{expr:#?} became {pred:#?} with [{min}..{max}]"
            );
        }
    };

    // value < min = max (15..15 = 10, 15..15 <= 10, etc)
    do_test(fifteen, fifteen, &[FALSE, FALSE, FALSE, TRUE, TRUE, TRUE]);

    // min = max = value (10..10 = 10, 10..10 <= 10, etc)
    //
    // NOTE: missing min or max stat produces NULL output if the expression needed it.
    do_test(ten, ten, &[FALSE, TRUE, TRUE, FALSE, FALSE, TRUE]);
    do_test(null, ten, &[NULL, NULL, NULL, NULL, FALSE, TRUE]);
    do_test(ten, null, &[FALSE, TRUE, NULL, NULL, NULL, NULL]);

    // min = max < value (5..5 = 10, 5..5 <= 10, etc)
    do_test(five, five, &[TRUE, TRUE, FALSE, TRUE, FALSE, FALSE]);

    // value = min < max (5..15 = 10, 5..15 <= 10, etc)
    do_test(ten, fifteen, &[FALSE, TRUE, TRUE, TRUE, TRUE, TRUE]);

    // min < value < max (5..15 = 10, 5..15 <= 10, etc)
    do_test(five, fifteen, &[TRUE, TRUE, TRUE, TRUE, TRUE, TRUE]);
}

#[test]
fn test_eval_variadic() {
    // NOTE: The data skipping logic can produce TRUE instead of NULL in some cases. This is
    // harmless, because data skipping only cares about FALSE (NULL and TRUE are treated the same).
    let test_cases = &[
        // Every combo of 0, 1 and 2 inputs
        (&[] as &[Option<bool>], TRUE, FALSE, FALSE, TRUE),
        (&[TRUE], TRUE, TRUE, FALSE, FALSE),
        (&[FALSE], FALSE, FALSE, TRUE, TRUE),
        (&[NULL], TRUE, NULL, NULL, TRUE), // NULL -> TRUE
        (&[TRUE, TRUE], TRUE, TRUE, FALSE, FALSE),
        (&[TRUE, FALSE], FALSE, TRUE, TRUE, FALSE),
        (&[TRUE, NULL], TRUE, NULL, NULL, FALSE), // NULL -> TRUE
        (&[FALSE, TRUE], FALSE, TRUE, TRUE, FALSE),
        (&[FALSE, FALSE], FALSE, FALSE, TRUE, TRUE),
        (&[FALSE, NULL], FALSE, NULL, NULL, TRUE), // NULL -> TRUE
        (&[NULL, TRUE], TRUE, NULL, NULL, FALSE),  // NULL -> TRUE
        (&[NULL, FALSE], FALSE, NULL, NULL, TRUE), // NULL -> TRUE
        (&[NULL, NULL], TRUE, NULL, NULL, TRUE),   // NULL -> TRUE
        // Every combo of 1:2
        (&[TRUE, FALSE, FALSE], FALSE, TRUE, TRUE, FALSE),
        (&[FALSE, TRUE, FALSE], FALSE, TRUE, TRUE, FALSE),
        (&[FALSE, FALSE, TRUE], FALSE, TRUE, TRUE, FALSE),
        (&[TRUE, NULL, NULL], TRUE, NULL, NULL, FALSE), // NULL -> TRUE
        (&[NULL, TRUE, NULL], TRUE, NULL, NULL, FALSE), // NULL -> TRUE
        (&[NULL, NULL, TRUE], TRUE, NULL, NULL, FALSE), // NULL -> TRUE
        (&[FALSE, TRUE, TRUE], FALSE, TRUE, TRUE, FALSE),
        (&[TRUE, FALSE, TRUE], FALSE, TRUE, TRUE, FALSE),
        (&[TRUE, TRUE, FALSE], FALSE, TRUE, TRUE, FALSE),
        (&[FALSE, NULL, NULL], FALSE, NULL, NULL, TRUE), // NULL -> TRUE
        (&[NULL, FALSE, NULL], FALSE, NULL, NULL, TRUE), // NULL -> TRUE
        (&[NULL, NULL, FALSE], FALSE, NULL, NULL, TRUE), // NULL -> TRUE
        (&[NULL, TRUE, TRUE], TRUE, NULL, NULL, FALSE),  // NULL -> TRUE
        (&[TRUE, NULL, TRUE], TRUE, NULL, NULL, FALSE),  // NULL -> TRUE
        (&[TRUE, TRUE, NULL], TRUE, NULL, NULL, FALSE),  // NULL -> TRUE
        (&[NULL, FALSE, FALSE], FALSE, NULL, NULL, TRUE), // NULL -> TRUE
        (&[FALSE, NULL, FALSE], FALSE, NULL, NULL, TRUE), // NULL -> TRUE
        (&[FALSE, FALSE, NULL], FALSE, NULL, NULL, TRUE), // NULL -> TRUE
        // Every unique ordering of 3
        (&[TRUE, FALSE, NULL], FALSE, NULL, NULL, FALSE),
        (&[TRUE, NULL, FALSE], FALSE, NULL, NULL, FALSE),
        (&[FALSE, TRUE, NULL], FALSE, NULL, NULL, FALSE),
        (&[FALSE, NULL, TRUE], FALSE, NULL, NULL, FALSE),
        (&[NULL, TRUE, FALSE], FALSE, NULL, NULL, FALSE),
        (&[NULL, FALSE, TRUE], FALSE, NULL, NULL, FALSE),
    ];
    let filter = DefaultPredicateEvaluator::from(UnimplementedColumnResolver);
    for (inputs, expect_and, expect_or, expect_not_and, expect_not_or) in test_cases {
        let inputs: Vec<_> = inputs
            .iter()
            .map(|val| match val {
                Some(v) => Expr::literal(v),
                None => Expr::null_literal(DataType::BOOLEAN),
            })
            .collect();

        let expr = Expr::and_from(inputs.clone());
        let pred = as_data_skipping_predicate(&expr, false);
        expect_eq!(
            pred.and_then(|p| filter.eval_expr(&p, false)),
            *expect_and,
            "AND({inputs:?})"
        );

        let expr = Expr::or_from(inputs.clone());
        let pred = as_data_skipping_predicate(&expr, false);
        expect_eq!(
            pred.and_then(|p| filter.eval_expr(&p, false)),
            *expect_or,
            "OR({inputs:?})"
        );

        let expr = Expr::and_from(inputs.clone());
        let pred = as_data_skipping_predicate(&expr, true);
        expect_eq!(
            pred.and_then(|p| filter.eval_expr(&p, false)),
            *expect_not_and,
            "NOT AND({inputs:?})"
        );

        let expr = Expr::or_from(inputs.clone());
        let pred = as_data_skipping_predicate(&expr, true);
        expect_eq!(
            pred.and_then(|p| filter.eval_expr(&p, false)),
            *expect_not_or,
            "NOT OR({inputs:?})"
        );
    }
}

// DISTINCT is actually quite complex internally. It indirectly exercises IS [NOT] NULL and
// AND/OR. We already validated min/max comparisons, so we're mostly worried about NULL vs. non-NULL
// literals, tight bounds, and nullcount/rowcount stats here.
#[test]
fn test_eval_distinct() {
    let col = &Expr::column("x");
    let five = &Scalar::from(5);
    let ten = &Scalar::from(10);
    let fifteen = &Scalar::from(15);
    let null = &Scalar::Null(DataType::INTEGER);

    let expressions = [
        Expr::distinct(col.clone(), ten.clone()),
        !Expr::distinct(col.clone(), ten.clone()),
        Expr::distinct(col.clone(), null.clone()),
        !Expr::distinct(col.clone(), null.clone()),
    ];

    let do_test = |min: &Scalar, max: &Scalar, nullcount: i64, expected: &[Option<bool>]| {
        let resolver = HashMap::from_iter([
            ("numRecords", Scalar::from(2i64)),
            ("nullCount.x", Scalar::from(nullcount)),
            ("minValues.x", min.clone()),
            ("maxValues.x", max.clone()),
        ]);
        let filter = DefaultPredicateEvaluator::from(resolver);
        for (expr, expect) in expressions.iter().zip(expected) {
            let pred = as_data_skipping_predicate(&expr, false).unwrap();
            expect_eq!(
                filter.eval_expr(&pred, false),
                *expect,
                "{expr:#?} became {pred:#?} ({min}..{max}, {nullcount} nulls)"
            );
        }
    };

    // min = max = value, no nulls
    do_test(ten, ten, 0, &[FALSE, TRUE, TRUE, FALSE]);

    // min = max = value, some nulls
    do_test(ten, ten, 1, &[TRUE, TRUE, TRUE, TRUE]);

    // min = max = value, all nulls
    do_test(ten, ten, 2, &[TRUE, FALSE, FALSE, TRUE]);

    // value < min = max, no nulls
    do_test(fifteen, fifteen, 0, &[TRUE, FALSE, TRUE, FALSE]);

    // value < min = max, some nulls
    do_test(fifteen, fifteen, 1, &[TRUE, FALSE, TRUE, TRUE]);

    // value < min = max, all nulls
    do_test(fifteen, fifteen, 2, &[TRUE, FALSE, FALSE, TRUE]);

    // min < value < max, no nulls
    do_test(five, fifteen, 0, &[TRUE, TRUE, TRUE, FALSE]);

    // min < value < max, some nulls
    do_test(five, fifteen, 1, &[TRUE, TRUE, TRUE, TRUE]);

    // min < value < max, all nulls
    do_test(five, fifteen, 2, &[TRUE, FALSE, FALSE, TRUE]);
}

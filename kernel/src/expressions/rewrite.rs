//! rewirite expression to data skipping expression

use crate::expressions::{BinaryOperator, Expression};

pub(crate) fn rewite(expr: &Expression) -> Option<Expression> {
    use BinaryOperator::*;
    use Expression::*;

    match expr {
        BinaryOperation {
            op: And,
            left,
            right,
        } => match (rewite(left), rewite(right)) {
            (Some(left), Some(right)) => Some(BinaryOperation {
                op: And,
                left: Box::new(left),
                right: Box::new(right),
            }),
            (left, right) => left.or(right),
        },
        BinaryOperation {
            op: Or,
            left,
            right,
        } => Some(BinaryOperation {
            op: Or,
            left: Box::new(rewite(left)?),
            right: Box::new(rewite(right)?),
        }),
        BinaryOperation { op, left, right } => match op {
            LessThan | LessThanOrEqual => match (left.as_ref(), right.as_ref()) {
                // column <lt | lt_eq> value
                (Column(col), Literal(value)) => Some(BinaryOperation {
                    op: op.clone(),
                    left: Box::new(Column(format!("maxValues.{}", col))),
                    right: Box::new(Literal(value.clone())),
                }),
                // value <lt | lt_eq> column
                (Literal(value), Column(col)) => Some(BinaryOperation {
                    op: match op {
                        LessThan => GreaterThan,
                        LessThanOrEqual => GreaterThanOrEqual,
                        _ => unreachable!(),
                    },
                    left: Box::new(Column(format!("minValues.{}", col))),
                    right: Box::new(Literal(value.clone())),
                }),
                _ => None,
            },
            GreaterThan | GreaterThanOrEqual => match (left.as_ref(), right.as_ref()) {
                // column <gt | gt_eq> value
                (Column(col), Literal(value)) => Some(BinaryOperation {
                    op: op.clone(),
                    left: Box::new(Column(format!("minValues.{}", col))),
                    right: Box::new(Literal(value.clone())),
                }),
                // value <gt | gt_eq> column
                (Literal(value), Column(col)) => Some(BinaryOperation {
                    op: match op {
                        GreaterThan => LessThan,
                        GreaterThanOrEqual => LessThanOrEqual,
                        _ => unreachable!(),
                    },
                    left: Box::new(Column(format!("maxValues.{}", col))),
                    right: Box::new(Literal(value.clone())),
                }),
                _ => None,
            },
            // column == value
            // column == value is equivalent to min values being less than or equal to value
            // and max values being greater than or equal to value
            Equal => match (left.as_ref(), right.as_ref()) {
                (Column(col), Literal(value)) | (Literal(value), Column(col)) => {
                    Some(BinaryOperation {
                        op: And,
                        left: Box::new(BinaryOperation {
                            op: GreaterThanOrEqual,
                            left: Box::new(Column(format!("maxValues.{}", col))),
                            right: Box::new(Literal(value.clone())),
                        }),
                        right: Box::new(BinaryOperation {
                            op: LessThanOrEqual,
                            left: Box::new(Column(format!("minValues.{}", col))),
                            right: Box::new(Literal(value.clone())),
                        }),
                    })
                }
                _ => None,
            },
            // column != value
            // column != value is equivalent to min values being strictly greater than value
            // or max values being strictly less than value
            NotEqual => match (left.as_ref(), right.as_ref()) {
                (Column(col), Literal(value)) | (Literal(value), Column(col)) => {
                    Some(BinaryOperation {
                        op: Or,
                        left: Box::new(BinaryOperation {
                            op: LessThan,
                            left: Box::new(Column(format!("maxValues.{}", col))),
                            right: Box::new(Literal(value.clone())),
                        }),
                        right: Box::new(BinaryOperation {
                            op: GreaterThan,
                            left: Box::new(Column(format!("minValues.{}", col))),
                            right: Box::new(Literal(value.clone())),
                        }),
                    })
                }
                _ => None,
            },
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::expressions::Scalar;

    #[test]
    fn test_rewrite_basic_comparison() {
        let column = Expression::Column("a".to_string());
        let lit_int = Expression::Literal(Scalar::Integer(1));

        let cases = [
            (
                column.clone().lt(lit_int.clone()),
                Expression::BinaryOperation {
                    op: BinaryOperator::LessThan,
                    left: Box::new(Expression::Column("maxValues.a".to_string())),
                    right: Box::new(lit_int.clone()),
                },
            ),
            (
                lit_int.clone().lt(column.clone()),
                Expression::BinaryOperation {
                    op: BinaryOperator::GreaterThan,
                    left: Box::new(Expression::Column("minValues.a".to_string())),
                    right: Box::new(lit_int.clone()),
                },
            ),
            (
                column.clone().gt(lit_int.clone()),
                Expression::BinaryOperation {
                    op: BinaryOperator::GreaterThan,
                    left: Box::new(Expression::Column("minValues.a".to_string())),
                    right: Box::new(lit_int.clone()),
                },
            ),
            (
                lit_int.clone().gt(column.clone()),
                Expression::BinaryOperation {
                    op: BinaryOperator::LessThan,
                    left: Box::new(Expression::Column("maxValues.a".to_string())),
                    right: Box::new(lit_int.clone()),
                },
            ),
            (
                column.clone().lt_eq(lit_int.clone()),
                Expression::BinaryOperation {
                    op: BinaryOperator::LessThanOrEqual,
                    left: Box::new(Expression::Column("maxValues.a".to_string())),
                    right: Box::new(lit_int.clone()),
                },
            ),
            (
                lit_int.clone().lt_eq(column.clone()),
                Expression::BinaryOperation {
                    op: BinaryOperator::GreaterThanOrEqual,
                    left: Box::new(Expression::Column("minValues.a".to_string())),
                    right: Box::new(lit_int.clone()),
                },
            ),
            (
                column.clone().gt_eq(lit_int.clone()),
                Expression::BinaryOperation {
                    op: BinaryOperator::GreaterThanOrEqual,
                    left: Box::new(Expression::Column("minValues.a".to_string())),
                    right: Box::new(lit_int.clone()),
                },
            ),
            (
                lit_int.clone().gt_eq(column.clone()),
                Expression::BinaryOperation {
                    op: BinaryOperator::LessThanOrEqual,
                    left: Box::new(Expression::Column("maxValues.a".to_string())),
                    right: Box::new(lit_int.clone()),
                },
            ),
            (
                column.clone().eq(lit_int.clone()),
                Expression::BinaryOperation {
                    op: BinaryOperator::And,
                    left: Box::new(Expression::BinaryOperation {
                        op: BinaryOperator::GreaterThanOrEqual,
                        left: Box::new(Expression::Column("maxValues.a".to_string())),
                        right: Box::new(lit_int.clone()),
                    }),
                    right: Box::new(Expression::BinaryOperation {
                        op: BinaryOperator::LessThanOrEqual,
                        left: Box::new(Expression::Column("minValues.a".to_string())),
                        right: Box::new(lit_int.clone()),
                    }),
                },
            ),
            (
                lit_int.clone().eq(column.clone()),
                Expression::BinaryOperation {
                    op: BinaryOperator::And,
                    left: Box::new(Expression::BinaryOperation {
                        op: BinaryOperator::GreaterThanOrEqual,
                        left: Box::new(Expression::Column("maxValues.a".to_string())),
                        right: Box::new(lit_int.clone()),
                    }),
                    right: Box::new(Expression::BinaryOperation {
                        op: BinaryOperator::LessThanOrEqual,
                        left: Box::new(Expression::Column("minValues.a".to_string())),
                        right: Box::new(lit_int.clone()),
                    }),
                },
            ),
            (
                column.clone().ne(lit_int.clone()),
                Expression::BinaryOperation {
                    op: BinaryOperator::Or,
                    left: Box::new(Expression::BinaryOperation {
                        op: BinaryOperator::LessThan,
                        left: Box::new(Expression::Column("maxValues.a".to_string())),
                        right: Box::new(lit_int.clone()),
                    }),
                    right: Box::new(Expression::BinaryOperation {
                        op: BinaryOperator::GreaterThan,
                        left: Box::new(Expression::Column("minValues.a".to_string())),
                        right: Box::new(lit_int.clone()),
                    }),
                },
            ),
            (
                lit_int.clone().ne(column.clone()),
                Expression::BinaryOperation {
                    op: BinaryOperator::Or,
                    left: Box::new(Expression::BinaryOperation {
                        op: BinaryOperator::LessThan,
                        left: Box::new(Expression::Column("maxValues.a".to_string())),
                        right: Box::new(lit_int.clone()),
                    }),
                    right: Box::new(Expression::BinaryOperation {
                        op: BinaryOperator::GreaterThan,
                        left: Box::new(Expression::Column("minValues.a".to_string())),
                        right: Box::new(lit_int.clone()),
                    }),
                },
            ),
        ];

        for (input, expected) in cases {
            let rewritten = rewite(&input).unwrap();
            assert_eq!(rewritten, expected)
        }
    }
}

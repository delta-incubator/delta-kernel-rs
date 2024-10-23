use crate::expressions::{
    BinaryOperator, Expression as Expr, Scalar, UnaryOperator, VariadicOperator,
};
use crate::schema::DataType;

use std::cmp::Ordering;
use tracing::debug;

/// Evaluates a predicate expression tree against column names that resolve as scalars. Useful for
/// testing/debugging but also serves as a reference implementation that documents the expression
/// semantics that kernel relies on for data skipping.
pub(crate) trait PredicateEvaluator {
    type Output;

    fn eval_scalar(&self, val: &Scalar, inverted: bool) -> Option<Self::Output>;

    fn eval_column(&self, col: &str, inverted: bool) -> Option<Self::Output> {
        // The expression <col> is equivalent to <col> != FALSE, and the expression NOT <col> is
        // equivalent to <col> != TRUE.
        self.eval_eq(col, &Scalar::from(inverted), true)
    }

    fn eval_not(&self, expr: &Expr, inverted: bool) -> Option<Self::Output> {
        self.eval_expr(expr, !inverted)
    }

    fn eval_is_null(&self, col: &str, inverted: bool) -> Option<Self::Output>;

    fn eval_unary(&self, op: UnaryOperator, expr: &Expr, inverted: bool) -> Option<Self::Output> {
        match op {
            UnaryOperator::Not => self.eval_not(expr, inverted),
            UnaryOperator::IsNull => {
                // Data skipping only supports IS [NOT] NULL over columns (not expressions)
                let Expr::Column(col) = expr else {
                    debug!("Unsupported operand: IS [NOT] NULL: {expr:?}");
                    return None;
                };
                self.eval_is_null(col, inverted)
            }
        }
    }

    fn eval_lt(&self, col: &str, val: &Scalar) -> Option<Self::Output>;

    fn eval_le(&self, col: &str, val: &Scalar) -> Option<Self::Output>;

    fn eval_gt(&self, col: &str, val: &Scalar) -> Option<Self::Output>;

    fn eval_ge(&self, col: &str, val: &Scalar) -> Option<Self::Output>;

    fn eval_eq(&self, col: &str, val: &Scalar, inverted: bool) -> Option<Self::Output>;

    fn eval_distinct(&self, col: &str, val: &Scalar, inverted: bool) -> Option<Self::Output> {
        if let Scalar::Null(_) = val {
            // DISTINCT(<col>, NULL) is the same as <col> IS NOT NULL
            self.eval_is_null(col, !inverted)
        } else if inverted {
            // NOT DISTINCT(<col>, <val>) is the same as <col> = <val>
            self.eval_eq(col, val, false)
        } else {
            // DISTINCT(<col>, <val>) is AND(<col> IS NOT NULL, <col> != <val>)
            let args = [self.eval_is_null(col, true), self.eval_eq(col, val, true)];
            self.finish_eval_variadic(VariadicOperator::And, args, false)
        }
    }

    fn eval_binary_scalars(
        &self,
        op: BinaryOperator,
        left: &Scalar,
        right: &Scalar,
        inverted: bool,
    ) -> Option<Self::Output>;

    fn eval_binary(
        &self,
        op: BinaryOperator,
        left: &Expr,
        right: &Expr,
        inverted: bool,
    ) -> Option<Self::Output> {
        use BinaryOperator::*;
        use Expr::{Column, Literal};

        // NOTE: We rely on the literal values to provide logical type hints. That means we cannot
        // perform column-column comparisons, because we cannot infer the logical type to use.
        let (op, col, val) = match (left, right) {
            (Column(col), Literal(val)) => (op, col, val),
            (Literal(val), Column(col)) => (op.commute()?, col, val),
            (Literal(a), Literal(b)) => return self.eval_binary_scalars(op, a, b, inverted),
            _ => {
                debug!("Unsupported binary operand(s): {left:?} {op:?} {right:?}");
                return None;
            }
        };
        match (op, inverted) {
            (Plus | Minus | Multiply | Divide, _) => None, // Unsupported
            (LessThan, false) | (GreaterThanOrEqual, true) => self.eval_lt(col, val),
            (LessThanOrEqual, false) | (GreaterThan, true) => self.eval_le(col, val),
            (GreaterThan, false) | (LessThanOrEqual, true) => self.eval_gt(col, val),
            (GreaterThanOrEqual, false) | (LessThan, true) => self.eval_ge(col, val),
            (Equal, _) => self.eval_eq(col, val, inverted),
            (NotEqual, _) => self.eval_eq(col, val, !inverted),
            (Distinct, _) => self.eval_distinct(col, val, inverted),
            (In | NotIn, _) => None, // TODO?
        }
    }

    fn finish_eval_variadic(
        &self,
        op: VariadicOperator,
        exprs: impl IntoIterator<Item = Option<Self::Output>>,
        inverted: bool,
    ) -> Option<Self::Output>;

    fn eval_variadic(
        &self,
        op: VariadicOperator,
        exprs: &[Expr],
        inverted: bool,
    ) -> Option<Self::Output> {
        // Evaluate the input expressions, inverting each as needed and tracking whether we've seen
        // any NULL result. Stop immediately (short circuit) if we see a dominant value.
        let exprs = exprs.iter().map(|expr| self.eval_expr(expr, inverted));
        self.finish_eval_variadic(op, exprs, inverted)
    }

    fn eval_expr(&self, expr: &Expr, inverted: bool) -> Option<Self::Output> {
        use Expr::*;
        match expr {
            Literal(val) => self.eval_scalar(val, inverted),
            Column(col) => self.eval_column(col, inverted),
            Struct(_) => None, // not supported
            UnaryOperation { op, expr } => self.eval_unary(*op, expr, inverted),
            BinaryOperation { op, left, right } => self.eval_binary(*op, left, right, inverted),
            VariadicOperation { op, exprs } => self.eval_variadic(*op, exprs, inverted),
        }
    }
}

/// Directly evaluates predicates, resolving columns directly as scalars.
pub(crate) struct DefaultPredicateEvaluator;
impl DefaultPredicateEvaluator {
    pub(crate) fn resolve_column(&self, _col: &str) -> Option<Scalar> {
        todo!()
    }

    pub(crate) fn partial_cmp_scalars(
        a: &Scalar,
        b: &Scalar,
        ord: Ordering,
        inverted: bool,
    ) -> Option<bool> {
        let result = a.partial_cmp(b)? == ord;
        Some(result != inverted)
    }

    pub(crate) fn eval_scalar(val: &Scalar, inverted: bool) -> Option<bool> {
        match val {
            Scalar::Boolean(val) => Some(*val != inverted),
            _ => None,
        }
    }

    pub(crate) fn eval_binary_scalars(
        op: BinaryOperator,
        left: &Scalar,
        right: &Scalar,
        inverted: bool,
    ) -> Option<bool> {
        use BinaryOperator::*;
        match op {
            Equal => Self::partial_cmp_scalars(left, right, Ordering::Equal, inverted),
            NotEqual => Self::partial_cmp_scalars(left, right, Ordering::Equal, !inverted),
            LessThan => Self::partial_cmp_scalars(left, right, Ordering::Less, inverted),
            LessThanOrEqual => Self::partial_cmp_scalars(left, right, Ordering::Greater, !inverted),
            GreaterThan => Self::partial_cmp_scalars(left, right, Ordering::Greater, inverted),
            GreaterThanOrEqual => Self::partial_cmp_scalars(left, right, Ordering::Less, !inverted),
            _ => {
                debug!("Unsupported binary operator: {left:?} {op:?} {right:?}");
                None
            }
        }
    }

    pub(crate) fn finish_eval_variadic(
        op: VariadicOperator,
        exprs: impl IntoIterator<Item = Option<bool>>,
        inverted: bool,
    ) -> Option<bool> {
        // With AND (OR), any FALSE (TRUE) input forces FALSE (TRUE) output.  If there was no
        // dominating input, then any NULL input forces NULL output.  Otherwise, return the
        // non-dominant value. Inverting the operation also inverts the dominant value.
        let dominator = match op {
            VariadicOperator::And => inverted,
            VariadicOperator::Or => !inverted,
        };
        let result = exprs.into_iter().try_fold(false, |found_null, val| {
            match val {
                Some(val) if val == dominator => None, // (1) short circuit, dominant found
                Some(_) => Some(found_null),
                None => Some(true), // (2) null found (but keep looking for a dominant value)
            }
        });

        match result {
            None => Some(dominator), // (1) short circuit, dominant found
            Some(false) => Some(!dominator),
            Some(true) => None, // (2) null found, dominant not found
        }
    }
}
impl PredicateEvaluator for DefaultPredicateEvaluator {
    type Output = bool;

    fn eval_scalar(&self, val: &Scalar, inverted: bool) -> Option<bool> {
        Self::eval_scalar(val, inverted)
    }

    fn eval_is_null(&self, col: &str, inverted: bool) -> Option<bool> {
        let is_null = matches!(self.resolve_column(col)?, Scalar::Null(_));
        Some(is_null != inverted)
    }

    fn eval_lt(&self, col: &str, val: &Scalar) -> Option<bool> {
        self.eval_binary_scalars(
            BinaryOperator::LessThan,
            &self.resolve_column(col)?,
            val,
            false,
        )
    }

    fn eval_le(&self, col: &str, val: &Scalar) -> Option<bool> {
        self.eval_binary_scalars(
            BinaryOperator::LessThanOrEqual,
            &self.resolve_column(col)?,
            val,
            false,
        )
    }

    fn eval_gt(&self, col: &str, val: &Scalar) -> Option<bool> {
        self.eval_binary_scalars(
            BinaryOperator::GreaterThan,
            &self.resolve_column(col)?,
            val,
            false,
        )
    }

    fn eval_ge(&self, col: &str, val: &Scalar) -> Option<bool> {
        self.eval_binary_scalars(
            BinaryOperator::GreaterThanOrEqual,
            &self.resolve_column(col)?,
            val,
            false,
        )
    }

    fn eval_eq(&self, col: &str, val: &Scalar, inverted: bool) -> Option<bool> {
        self.eval_binary_scalars(
            BinaryOperator::Equal,
            &self.resolve_column(col)?,
            val,
            inverted,
        )
    }

    fn eval_binary_scalars(
        &self,
        op: BinaryOperator,
        left: &Scalar,
        right: &Scalar,
        inverted: bool,
    ) -> Option<bool> {
        Self::eval_binary_scalars(op, left, right, inverted)
    }

    fn finish_eval_variadic(
        &self,
        op: VariadicOperator,
        exprs: impl IntoIterator<Item = Option<bool>>,
        inverted: bool,
    ) -> Option<bool> {
        Self::finish_eval_variadic(op, exprs, inverted)
    }
}

pub(crate) trait DataSkippingStatsProvider {
    type TypedOutput;
    type IntOutput;
    type BoolOutput;

    /// Retrieves the minimum value of a column, if it exists and has the requested type.
    fn get_min_stat(&self, col: &str, data_type: &DataType) -> Option<Self::TypedOutput>;

    /// Retrieves the maximum value of a column, if it exists and has the requested type.
    fn get_max_stat(&self, col: &str, data_type: &DataType) -> Option<Self::TypedOutput>;

    /// Retrieves the null count of a column, if it exists.
    fn get_nullcount_stat(&self, col: &str) -> Option<Self::IntOutput>;

    /// Retrieves the row count of a column (parquet footers always include this stat).
    fn get_rowcount_stat(&self) -> Option<Self::IntOutput>;

    fn eval_partial_cmp(
        &self,
        col: Self::TypedOutput,
        val: &Scalar,
        ord: Ordering,
        inverted: bool,
    ) -> Option<Self::BoolOutput>;

    /// Performs a partial comparison against a column min-stat. See [`partial_cmp_scalars`] for
    /// details of the comparison semantics.
    fn partial_cmp_min_stat(
        &self,
        col: &str,
        val: &Scalar,
        ord: Ordering,
        inverted: bool,
    ) -> Option<Self::BoolOutput> {
        let min = self.get_min_stat(col, &val.data_type())?;
        self.eval_partial_cmp(min, val, ord, inverted)
    }

    /// Performs a partial comparison against a column max-stat. See [`partial_cmp_scalars`] for
    /// details of the comparison semantics.
    fn partial_cmp_max_stat(
        &self,
        col: &str,
        val: &Scalar,
        ord: Ordering,
        inverted: bool,
    ) -> Option<Self::BoolOutput> {
        let max = self.get_max_stat(col, &val.data_type())?;
        self.eval_partial_cmp(max, val, ord, inverted)
    }
}

pub(crate) trait DataSkippingPredicateEvaluator: DataSkippingStatsProvider {
    fn eval_scalar(&self, val: &Scalar, inverted: bool) -> Option<Self::BoolOutput>;

    fn eval_is_null(&self, col: &str, inverted: bool) -> Option<Self::BoolOutput>;

    fn eval_binary_scalars(
        &self,
        op: BinaryOperator,
        left: &Scalar,
        right: &Scalar,
        inverted: bool,
    ) -> Option<Self::BoolOutput>;

    fn finish_eval_variadic(
        &self,
        op: VariadicOperator,
        exprs: impl IntoIterator<Item = Option<Self::BoolOutput>>,
        inverted: bool,
    ) -> Option<Self::BoolOutput>;
}

impl<T: DataSkippingPredicateEvaluator> PredicateEvaluator for T {
    type Output = T::BoolOutput;

    fn eval_scalar(&self, val: &Scalar, inverted: bool) -> Option<Self::Output> {
        DataSkippingPredicateEvaluator::eval_scalar(self, val, inverted)
    }

    fn eval_is_null(&self, col: &str, inverted: bool) -> Option<Self::Output> {
        DataSkippingPredicateEvaluator::eval_is_null(self, col, inverted)
    }

    fn eval_lt(&self, col: &str, val: &Scalar) -> Option<Self::Output> {
        // Given `col < val`:
        // Skip if `val` is not greater than _all_ values in [min, max], implies
        // Skip if `val <= min AND val <= max` implies
        // Skip if `val <= min` implies
        // Keep if `NOT(val <= min)` implies
        // Keep if `val > min` implies
        // Keep if `min < val`
        self.partial_cmp_min_stat(col, val, Ordering::Less, false)
    }

    fn eval_le(&self, col: &str, val: &Scalar) -> Option<Self::Output> {
        // Given `col <= val`:
        // Skip if `val` is less than _all_ values in [min, max], implies
        // Skip if `val < min AND val < max` implies
        // Skip if `val < min` implies
        // Keep if `NOT(val < min)` implies
        // Keep if `NOT(min > val)`
        self.partial_cmp_min_stat(col, val, Ordering::Greater, true)
    }

    fn eval_gt(&self, col: &str, val: &Scalar) -> Option<Self::Output> {
        // Given `col > val`:
        // Skip if `val` is not less than _all_ values in [min, max], implies
        // Skip if `val >= min AND val >= max` implies
        // Skip if `val >= max` implies
        // Keep if `NOT(val >= max)` implies
        // Keep if `NOT(max <= val)` implies
        // Keep if `max > val`
        self.partial_cmp_max_stat(col, val, Ordering::Greater, false)
    }

    fn eval_ge(&self, col: &str, val: &Scalar) -> Option<Self::Output> {
        // Given `col >= val`:
        // Skip if `val is greater than _every_ value in [min, max], implies
        // Skip if `val > min AND val > max` implies
        // Skip if `val > max` implies
        // Keep if `NOT(val > max)` implies
        // Keep if `NOT(max < val)`
        self.partial_cmp_max_stat(col, val, Ordering::Less, true)
    }

    fn eval_eq(&self, col: &str, val: &Scalar, inverted: bool) -> Option<Self::Output> {
        let (op, exprs) = if inverted {
            // Column could compare not-equal if min or max value differs from the literal.
            let exprs = [
                self.partial_cmp_min_stat(col, val, Ordering::Equal, true),
                self.partial_cmp_max_stat(col, val, Ordering::Equal, true),
            ];
            (VariadicOperator::Or, exprs)
        } else {
            // Column could compare equal if its min/max values bracket the literal.
            let exprs = [
                self.partial_cmp_min_stat(col, val, Ordering::Greater, true),
                self.partial_cmp_max_stat(col, val, Ordering::Less, true),
            ];
            (VariadicOperator::And, exprs)
        };
        self.finish_eval_variadic(op, exprs, false)
    }

    fn eval_binary_scalars(
        &self,
        op: BinaryOperator,
        left: &Scalar,
        right: &Scalar,
        inverted: bool,
    ) -> Option<Self::Output> {
        DataSkippingPredicateEvaluator::eval_binary_scalars(self, op, left, right, inverted)
    }

    fn finish_eval_variadic(
        &self,
        op: VariadicOperator,
        exprs: impl IntoIterator<Item = Option<Self::Output>>,
        inverted: bool,
    ) -> Option<Self::Output> {
        DataSkippingPredicateEvaluator::finish_eval_variadic(self, op, exprs, inverted)
    }
}

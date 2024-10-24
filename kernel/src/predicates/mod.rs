use crate::expressions::{
    BinaryOperator, Expression as Expr, Scalar, UnaryOperator, VariadicOperator,
};
use crate::schema::DataType;

use std::cmp::Ordering;
use tracing::debug;

#[cfg(test)]
mod tests;

/// Evaluates a predicate expression tree against column names that resolve as scalars. Useful for
/// testing/debugging but also serves as a reference implementation that documents the expression
/// semantics that kernel relies on for data skipping.
///
/// # Inverted expression semantics
///
/// Because inversion(`NOT` operator) has special semantics and can often be optimized away by
/// pushing it down, most methods take an `inverted` flag. That allows operations like
/// [`UnaryOperator::Not`] to simply evaluate their operand with a flipped `inverted` flag,
///
/// # NULL and error semantics
///
/// Literal NULL values almost always produce cascading changes in the predicate's structure, so we
/// represent them by `Option::None` rather than `Scalar::Null`. This allows e.g. `<A> < NULL` to be
/// rewritten as `NULL`, or `AND(<A>, NULL, <B>)` to be rewritten as `AND(<A>, <B>)`.
///
/// With the exception of `IS [NOT] NULL`, all operations produce NULL output if any input is
/// `NULL`. Any resolution failures also produce NULL (such as missing columns or type mismatch
/// between a column and the scalar it is compared against).
///
/// For safety reasons, this evaluator only accepts expressions of the form `<col> IS [NOT]
/// NULL`. This is because stats-based skipping is only well-defined for that restricted case --
/// there is no easy way to distinguish whether NULL was due to missing stats vs. an operation that
/// produced NULL for other reasons.
///
/// NOTE: The error-handling semantics of this trait's scalar-based predicate evaluation may differ
/// from those of the engine's expression evaluation, because kernel expressions don't include the
/// necessary type information to reliably detect all type errors.
pub(crate) trait PredicateEvaluator {
    type Output;

    /// A (possibly inverted) boolean scalar value, e.g. `[NOT] <value>`.
    fn eval_scalar(&self, val: &Scalar, inverted: bool) -> Option<Self::Output>;

    /// A (possibly inverted) NULL check, e.g. `<expr> IS [NOT] NULL`.
    fn eval_is_null(&self, col: &str, inverted: bool) -> Option<Self::Output>;

    /// A less-than comparison, e.g. `<col> < <value>`.
    ///
    /// NOTE: Caller is responsible to commute and/or invert the operation if needed,
    /// e.g. `NOT(<value> < <col>` becomes `<col> <= <value>`.
    fn eval_lt(&self, col: &str, val: &Scalar) -> Option<Self::Output>;

    /// A less-than-or-equal comparison, e.g. `<col> <= <value>`
    ///
    /// NOTE: Caller is responsible to commute and/or invert the operation if needed,
    /// e.g. `NOT(<value> <= <col>` becomes `<col> > <value>`.
    fn eval_le(&self, col: &str, val: &Scalar) -> Option<Self::Output>;

    /// A greater-than comparison, e.g. `<col> > <value>`
    ///
    /// NOTE: Caller is responsible to commute and/or invert the operation if needed,
    /// e.g. `NOT(<value> > <col>` becomes `<col> >= <value>`.
    fn eval_gt(&self, col: &str, val: &Scalar) -> Option<Self::Output>;

    /// A greater-than-or-equal comparison, e.g. `<col> >= <value>`
    ///
    /// NOTE: Caller is responsible to commute and/or invert the operation if needed,
    /// e.g. `NOT(<value> >= <col>` becomes `<col> > <value>`.
    fn eval_ge(&self, col: &str, val: &Scalar) -> Option<Self::Output>;

    /// A (possibly inverted) equality comparison, e.g. `<col> = <value>` or `<col> != <value>`.
    ///
    /// NOTE: Caller is responsible to commute the operation if needed, e.g. `<value> != <col>`
    /// becomes `<col> != <value>`.
    fn eval_eq(&self, col: &str, val: &Scalar, inverted: bool) -> Option<Self::Output>;

    /// A (possibly inverted) binary comparison between two scalars, e.g. `<value> != <value>`.
    fn eval_binary_scalars(
        &self,
        op: BinaryOperator,
        left: &Scalar,
        right: &Scalar,
        inverted: bool,
    ) -> Option<Self::Output>;

    /// Completes evaluation of a variadic expression.
    ///
    /// AND and OR are implemented by first evaluating its inputs (always the same, provided by
    /// [`eval_variadic`]), and then assembling the results back into a variadic expression in some
    /// implementation-defined way (this method).
    fn finish_eval_variadic(
        &self,
        op: VariadicOperator,
        exprs: impl IntoIterator<Item = Option<Self::Output>>,
        inverted: bool,
    ) -> Option<Self::Output>;

    // ==================== PROVIDED METHODS ====================

    /// A (possibly inverted) boolean column access, e.g. `[NOT] <col>`.
    fn eval_column(&self, col: &str, inverted: bool) -> Option<Self::Output> {
        // The expression <col> is equivalent to <col> != FALSE, and the expression NOT <col> is
        // equivalent to <col> != TRUE.
        self.eval_eq(col, &Scalar::from(inverted), true)
    }

    /// Inverts a (possibly already inverted) expression, e.g. `[NOT] NOT <expr>`.
    fn eval_not(&self, expr: &Expr, inverted: bool) -> Option<Self::Output> {
        self.eval_expr(expr, !inverted)
    }

    /// Dispatches a (possibly inverted) unary expression to each operator's specific implementation.
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

    /// A (possibly inverted) DISTINCT test, e.g. `[NOT] DISTINCT(<col>, false)`. DISTINCT can be
    /// seen as one of two operations, depending on the input:
    ///
    /// 1. DISTINCT(<col>, NULL) is equivalent to `<col> IS NOT NULL`
    /// 2. DISTINCT(<col>, <value>) is equivalent to `OR(<col> IS NULL, <col> != <value>)`
    fn eval_distinct(&self, col: &str, val: &Scalar, inverted: bool) -> Option<Self::Output> {
        if let Scalar::Null(_) = val {
            self.eval_is_null(col, !inverted)
        } else {
            let args = [
                self.eval_is_null(col, inverted),
                self.eval_eq(col, val, !inverted),
            ];
            self.finish_eval_variadic(VariadicOperator::Or, args, inverted)
        }
    }

    /// A (possibly inverted) IN-list check, e.g. `<col> [NOT] IN <array-value>`.
    ///
    /// Unsupported by default, but implementations can override it if they wish.
    fn eval_in(&self, _col: &str, _val: &Scalar, _inverted: bool) -> Option<Self::Output> {
        None // TODO?
    }

    /// Dispatches a (possibly inverted) binary expression to each operator's specific implementation.
    ///
    /// NOTE: Only binary operators that produce boolean outputs are supported.
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
            (Plus | Minus | Multiply | Divide, _) => None, // Unsupported - not boolean output
            (LessThan, false) | (GreaterThanOrEqual, true) => self.eval_lt(col, val),
            (LessThanOrEqual, false) | (GreaterThan, true) => self.eval_le(col, val),
            (GreaterThan, false) | (LessThanOrEqual, true) => self.eval_gt(col, val),
            (GreaterThanOrEqual, false) | (LessThan, true) => self.eval_ge(col, val),
            (Equal, _) => self.eval_eq(col, val, inverted),
            (NotEqual, _) => self.eval_eq(col, val, !inverted),
            (Distinct, _) => self.eval_distinct(col, val, inverted),
            (In, _) => self.eval_in(col, val, inverted),
            (NotIn, _) => self.eval_in(col, val, !inverted),
        }
    }

    /// Dispatches a variadic operation, leveraging each implementation's [`finish_eval_variadic`].
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

    /// Dispatches an expression to the specific implementation for each expression variant.
    ///
    /// NOTE: [`Expression::Struct`] is not supported and always evaluates to `None`.
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

pub(crate) struct PredicateEvaluatorDefaults;
impl PredicateEvaluatorDefaults {
    pub(crate) fn eval_scalar(val: &Scalar, inverted: bool) -> Option<bool> {
        match val {
            Scalar::Boolean(val) => Some(*val != inverted),
            _ => None,
        }
    }

    pub(crate) fn partial_cmp_scalars(
        ord: Ordering,
        a: &Scalar,
        b: &Scalar,
        inverted: bool,
    ) -> Option<bool> {
        let cmp = a.partial_cmp(b)?;
        let matched = cmp == ord;
        Some(matched != inverted)
    }

    pub(crate) fn eval_binary_scalars(
        op: BinaryOperator,
        left: &Scalar,
        right: &Scalar,
        inverted: bool,
    ) -> Option<bool> {
        use BinaryOperator::*;
        match op {
            Equal => Self::partial_cmp_scalars(Ordering::Equal, left, right, inverted),
            NotEqual => Self::partial_cmp_scalars(Ordering::Equal, left, right, !inverted),
            LessThan => Self::partial_cmp_scalars(Ordering::Less, left, right, inverted),
            LessThanOrEqual => Self::partial_cmp_scalars(Ordering::Greater, left, right, !inverted),
            GreaterThan => Self::partial_cmp_scalars(Ordering::Greater, left, right, inverted),
            GreaterThanOrEqual => Self::partial_cmp_scalars(Ordering::Less, left, right, !inverted),
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

/// Resolves columns as scalars, as a building block for [`DefaultPredicateEvaluator`].
pub(crate) trait ResolveColumnAsScalar {
    fn resolve_column(&self, col: &str) -> Option<Scalar>;
}
/// A predicate evaluator that directly evaluates the predicate to produce an `Option<bool>`
/// result. Column resolution is handled by an embedded [`ResolveColumnAsScalar`] instance.
pub(crate) struct DefaultPredicateEvaluator {
    resolver: Box<dyn ResolveColumnAsScalar>,
}
impl DefaultPredicateEvaluator {
    fn resolve_column(&self, col: &str) -> Option<Scalar> {
        self.resolver.resolve_column(col)
    }
}
impl<T: ResolveColumnAsScalar + 'static> From<T> for DefaultPredicateEvaluator {
    fn from(resolver: T) -> Self {
        Self {
            resolver: Box::new(resolver),
        }
    }
}
impl PredicateEvaluator for DefaultPredicateEvaluator {
    type Output = bool;

    fn eval_scalar(&self, val: &Scalar, inverted: bool) -> Option<bool> {
        PredicateEvaluatorDefaults::eval_scalar(val, inverted)
    }

    fn eval_is_null(&self, col: &str, inverted: bool) -> Option<bool> {
        let col = self.resolve_column(col)?;
        Some(matches!(col, Scalar::Null(_)) != inverted)
    }

    fn eval_lt(&self, col: &str, val: &Scalar) -> Option<bool> {
        let col = self.resolve_column(col)?;
        self.eval_binary_scalars(BinaryOperator::LessThan, &col, val, false)
    }

    fn eval_le(&self, col: &str, val: &Scalar) -> Option<bool> {
        let col = self.resolve_column(col)?;
        self.eval_binary_scalars(BinaryOperator::LessThanOrEqual, &col, val, false)
    }

    fn eval_gt(&self, col: &str, val: &Scalar) -> Option<bool> {
        let col = self.resolve_column(col)?;
        self.eval_binary_scalars(BinaryOperator::GreaterThan, &col, val, false)
    }

    fn eval_ge(&self, col: &str, val: &Scalar) -> Option<bool> {
        let col = self.resolve_column(col)?;
        self.eval_binary_scalars(BinaryOperator::GreaterThanOrEqual, &col, val, false)
    }

    fn eval_eq(&self, col: &str, val: &Scalar, inverted: bool) -> Option<bool> {
        let col = self.resolve_column(col)?;
        self.eval_binary_scalars(BinaryOperator::Equal, &col, val, inverted)
    }

    fn eval_binary_scalars(
        &self,
        op: BinaryOperator,
        left: &Scalar,
        right: &Scalar,
        inverted: bool,
    ) -> Option<Self::Output> {
        PredicateEvaluatorDefaults::eval_binary_scalars(op, left, right, inverted)
    }

    fn finish_eval_variadic(
        &self,
        op: VariadicOperator,
        exprs: impl IntoIterator<Item = Option<bool>>,
        inverted: bool,
    ) -> Option<bool> {
        PredicateEvaluatorDefaults::finish_eval_variadic(op, exprs, inverted)
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
        ord: Ordering,
        col: Self::TypedOutput,
        val: &Scalar,
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
        self.eval_partial_cmp(ord, min, val, inverted)
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
        self.eval_partial_cmp(ord, max, val, inverted)
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

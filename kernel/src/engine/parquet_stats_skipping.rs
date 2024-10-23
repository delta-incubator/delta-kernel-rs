//! An implementation of data skipping that leverages parquet stats from the file footer.
use crate::expressions::{
    BinaryOperator, Expression as Expr, Scalar, UnaryOperator, VariadicOperator,
};
use crate::predicates::{
    DataSkippingPredicateEvaluator, DataSkippingStatsProvider, DefaultPredicateEvaluator,
    PredicateEvaluator,
};
use crate::schema::DataType;
use std::cmp::Ordering;

#[cfg(test)]
mod tests;

/// A helper trait (mostly exposed for testing). It provides the four stats getters needed by
/// [`DataSkippingStatsProvider`]. From there, we can automatically derive a
/// [`DataSkippingPredicateEvaluator`].
pub(crate) trait ParquetStatsProvider {
    fn get_parquet_min_stat(&self, _col: &str, _data_type: &DataType) -> Option<Scalar>;

    fn get_parquet_max_stat(&self, _col: &str, _data_type: &DataType) -> Option<Scalar>;

    fn get_parquet_nullcount_stat(&self, _col: &str) -> Option<i64>;

    fn get_parquet_rowcount_stat(&self) -> i64;
}

/// Blanket implementation that converts a [`ParquetStatsProvider`] into a [`DataSkippingStatsProvider`].
impl<T: ParquetStatsProvider> DataSkippingStatsProvider for T {
    type TypedOutput = Scalar;
    type IntOutput = i64;
    type BoolOutput = bool;

    fn get_min_stat(&self, col: &str, data_type: &DataType) -> Option<Scalar> {
        self.get_parquet_min_stat(col, data_type)
    }

    fn get_max_stat(&self, col: &str, data_type: &DataType) -> Option<Scalar> {
        self.get_parquet_max_stat(col, data_type)
    }

    fn get_nullcount_stat(&self, col: &str) -> Option<i64> {
        self.get_parquet_nullcount_stat(col)
    }

    fn get_rowcount_stat(&self) -> Option<i64> {
        Some(self.get_parquet_rowcount_stat())
    }

    fn eval_partial_cmp(
        &self,
        col: Scalar,
        val: &Scalar,
        ord: Ordering,
        inverted: bool,
    ) -> Option<bool> {
        DefaultPredicateEvaluator::partial_cmp_scalars(&col, val, ord, inverted)
    }
}

/// Blanket implementation that converts a [`ParquetStatsProvider`] into a
/// [`DataSkippingPredicateEvaluator`].
impl<T: ParquetStatsProvider> DataSkippingPredicateEvaluator for T {
    fn eval_scalar(&self, val: &Scalar, inverted: bool) -> Option<bool> {
        DefaultPredicateEvaluator::eval_scalar(val, inverted)
    }

    fn eval_is_null(&self, col: &str, inverted: bool) -> Option<bool> {
        let sentinel = match inverted {
            // IS NOT NULL - skip if all-null
            true => self.get_rowcount_stat()?,
            // IS NULL - skip if no-null
            false => 0,
        };
        Some(self.get_nullcount_stat(col)? != sentinel)
    }

    fn eval_binary_scalars(
        &self,
        op: BinaryOperator,
        left: &Scalar,
        right: &Scalar,
        inverted: bool,
    ) -> Option<bool> {
        DefaultPredicateEvaluator::eval_binary_scalars(op, left, right, inverted)
    }

    fn finish_eval_variadic(
        &self,
        op: VariadicOperator,
        exprs: impl IntoIterator<Item = Option<bool>>,
        inverted: bool,
    ) -> Option<bool> {
        DefaultPredicateEvaluator::finish_eval_variadic(op, exprs, inverted)
    }
}

/// Data skipping based on parquet footer stats (e.g. row group skipping). The required methods
/// fetch stats values for requested columns (if available and with compatible types), and the
/// provided methods implement the actual skipping logic.
///
/// NOTE: We are given a row-based filter, but stats-based predicate evaluation -- which applies to
/// a SET of rows -- has different semantics than row-based predicate evaluation. The provided
/// methods of this class convert various supported expressions into data skipping predicates, and
/// then return the result of evaluating the translated filter.
pub(crate) trait ParquetStatsSkippingFilter {
    /// Attempts to filter using SQL WHERE semantics.
    ///
    /// By default, [`apply_expr`] can produce unwelcome behavior for comparisons involving all-NULL
    /// columns (e.g. `a == 10`), because the (legitimately NULL) min/max stats are interpreted as
    /// stats-missing that produces a NULL data skipping result). The resulting NULL can "poison"
    /// the entire expression, causing it to return NULL instead of FALSE that would allow skipping.
    ///
    /// Meanwhile, SQL WHERE semantics only keep rows for which the filter evaluates to TRUE --
    /// effectively turning `<expr>` into the null-safe predicate `AND(<expr> IS NOT NULL, <expr>)`.
    ///
    /// We cannot safely evaluate an arbitrary data skipping expression with null-safe semantics
    /// (because NULL could also mean missing-stats), but we CAN safely turn a column reference in a
    /// comparison into a null-safe comparison, as long as the comparison's parent expressions are
    /// all AND. To see why, consider a WHERE clause filter of the form:
    ///
    /// ```text
    /// AND(..., a {cmp} b, ...)
    /// ```
    ///
    /// In order allow skipping based on the all-null `a` or `b`, we want to actually evaluate:
    /// ```text
    /// AND(..., AND(a IS NOT NULL, b IS NOT NULL, a {cmp} b), ...)
    /// ```
    ///
    /// This optimization relies on the fact that we only support IS [NOT] NULL skipping for
    /// columns, and we only support skipping for comparisons between columns and literals. Thus, a
    /// typical case such as: `AND(..., x < 10, ...)` would in the all-null case be evaluated as:
    /// ```text
    /// AND(..., AND(x IS NOT NULL, 10 IS NOT NULL, x < 10), ...)
    /// AND(..., AND(FALSE, NULL, NULL), ...)
    /// AND(..., FALSE, ...)
    /// FALSE
    /// ```
    ///
    /// In the not all-null case, it would instead evaluate as:
    /// ```text
    /// AND(..., AND(x IS NOT NULL, 10 IS NOT NULL, x < 10), ...)
    /// AND(..., AND(TRUE, NULL, <result>), ...)
    /// ```
    ///
    /// If the result was FALSE, it forces both inner and outer AND to FALSE, as desired. If the
    /// result was TRUE or NULL, then it does not contribute to data skipping but also does not
    /// block it if other legs of the AND evaluate to FALSE.
    fn eval_sql_where(&self, filter: &Expr) -> Option<bool>;
    fn eval_binary_nullsafe(&self, op: BinaryOperator, left: &Expr, right: &Expr) -> Option<bool>;
}

impl<T: DataSkippingPredicateEvaluator<BoolOutput = bool>> ParquetStatsSkippingFilter for T {
    fn eval_sql_where(&self, filter: &Expr) -> Option<bool> {
        use Expr::*;
        use VariadicOperator::And;
        match filter {
            VariadicOperation { op: And, exprs } => {
                let exprs: Vec<_> = exprs
                    .iter()
                    .map(|expr| self.eval_sql_where(expr))
                    .map(|result| match result {
                        Some(value) => Expr::literal(value),
                        None => Expr::null_literal(DataType::BOOLEAN),
                    })
                    .collect();
                self.eval_variadic(And, &exprs, false)
            }
            BinaryOperation { op, left, right } => self.eval_binary_nullsafe(*op, left, right),
            _ => self.eval_expr(filter, false),
        }
    }

    /// Helper method for [`apply_sql_where`], that evaluates `{a} {cmp} {b}` as
    /// ```text
    /// AND({a} IS NOT NULL, {b} IS NOT NULL, {a} {cmp} {b})
    /// ```
    ///
    /// The null checks only apply to column expressions, so at least one of them will always be
    /// NULL (since we don't support skipping over column-column comparisons). If any NULL check
    /// fails (producing FALSE), it short-circuits the entire AND without ever evaluating the
    /// comparison. Otherwise, the original comparison will run and -- if FALSE -- can cause data
    /// skipping as usual.
    fn eval_binary_nullsafe(&self, op: BinaryOperator, left: &Expr, right: &Expr) -> Option<bool> {
        use UnaryOperator::IsNull;
        // Convert `a {cmp} b` to `AND(a IS NOT NULL, b IS NOT NULL, a {cmp} b)`,
        // and only evaluate the comparison if the null checks don't short circuit.
        if let Some(false) = self.eval_unary(IsNull, left, true) {
            return Some(false);
        }
        if let Some(false) = self.eval_unary(IsNull, right, true) {
            return Some(false);
        }
        self.eval_binary(op, left, right, false)
    }
}

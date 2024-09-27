//! An implementation of data skipping that leverages parquet stats from the file footer.
use crate::expressions::{BinaryOperator, Expression, Scalar, UnaryOperator, VariadicOperator};
use crate::schema::DataType;
use parquet::schema::types::ColumnPath;
use std::cmp::Ordering;

/// Data skipping based on parquet footer stats (e.g. row group skipping). The required methods
/// fetch stats values for requested columns (if available and with compatible types), and the
/// provided methods implement the actual skipping logic.
///
/// NOTE: We are given a row-based filter, but stats-based predicate evaluation -- which applies to
/// a SET of rows -- has different semantics than row-based predicate evaluation. The provided
/// methods of this class convert various supported expressions into data skipping predicates, and
/// then return the result of evaluating the translated filter.
pub(crate) trait ParquetStatsSkippingFilter {
    /// Retrieves the minimum value of a column, if it exists and has the requested type.
    fn get_min_stat_value(&self, col: &ColumnPath, data_type: &DataType) -> Option<Scalar>;

    /// Retrieves the maximum value of a column, if it exists and has the requested type.
    fn get_max_stat_value(&self, col: &ColumnPath, data_type: &DataType) -> Option<Scalar>;

    /// Retrieves the null count of a column, if it exists.
    fn get_nullcount_stat_value(&self, col: &ColumnPath) -> Option<i64>;

    /// Retrieves the row count of a column (parquet footers always include this stat).
    fn get_rowcount_stat_value(&self) -> i64;

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
    fn apply_sql_where(&self, filter: &Expression) -> Option<bool> {
        use Expression::*;
        use VariadicOperator::And;
        match filter {
            VariadicOperation { op: And, exprs } => {
                let exprs: Vec<_> = exprs
                    .iter()
                    .map(|expr| self.apply_sql_where(expr))
                    .map(|result| match result {
                        Some(value) => Expression::literal(value),
                        None => Expression::null_literal(DataType::BOOLEAN),
                    })
                    .collect();
                self.apply_variadic(And, &exprs, false)
            }
            BinaryOperation { op, left, right } => self.apply_binary_nullsafe(*op, left, right),
            _ => self.apply_expr(filter, false),
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
    fn apply_binary_nullsafe(
        &self,
        op: BinaryOperator,
        left: &Expression,
        right: &Expression,
    ) -> Option<bool> {
        use UnaryOperator::IsNull;
        // Convert `a {cmp} b` to `AND(a IS NOT NULL, b IS NOT NULL, a {cmp} b)`,
        // and only evaluate the comparison if the null checks don't short circuit.
        if matches!(self.apply_unary(IsNull, left, true), Some(false)) {
            return Some(false);
        }
        if matches!(self.apply_unary(IsNull, right, true), Some(false)) {
            return Some(false);
        }
        self.apply_binary(op, left, right, false)
    }

    /// Evaluates a predicate over stats instead of rows. Evaluation is a depth-first traversal over
    /// all supported subexpressions; unsupported expressions (or expressions that rely on missing
    /// stats) are replaced with NULL (`None`) values, which then propagate upward following the
    /// NULL semantics of their parent expressions. If stats prove the filter would eliminate ALL
    /// rows from the result, then this method returns `Some(false)` and those rows can be skipped
    /// without inspecting them individually. A return value of `Some(true)` means the filter does
    /// not reliably eliminate all rows, and `None` indicates the needed stats were not available.
    ///
    /// If `inverted`, the caller requests to evaluate `NOT(expression)` instead of evaluating
    /// `expression` directly. This is important because `NOT(data_skipping(expr))` is NOT
    /// `equivalent to data_skipping(NOT(expr))`, so we need to "push down" the NOT in order to
    /// ensure correct semantics. For example, given the expression `x == 10`, and min-max stats
    /// 1..100, `NOT(x == 10)` and `x == 10` both evaluate to TRUE (because neither filter can
    /// provably eliminate all rows).
    fn apply_expr(&self, expression: &Expression, inverted: bool) -> Option<bool> {
        use Expression::*;
        match expression {
            VariadicOperation { op, exprs } => self.apply_variadic(*op, exprs, inverted),
            BinaryOperation { op, left, right } => self.apply_binary(*op, left, right, inverted),
            UnaryOperation { op, expr } => self.apply_unary(*op, expr, inverted),
            Literal(value) => Self::apply_scalar(value, inverted),
            Column(col) => self.apply_column(col, inverted),
            Struct(_) => None, // not supported
        }
    }

    /// Evaluates AND/OR expressions with Kleene semantics and short circuit behavior.
    ///
    /// Short circuiting is based on the observation that each operator has a "dominant" boolean
    /// value that forces the output to match regardless of any other input. For example, a single
    /// FALSE input forces AND to FALSE, and a single TRUE input forces OR to TRUE.
    ///
    /// Kleene semantics mean that -- in the absence of any dominant input -- a single NULL input
    /// forces the output to NULL. If no NULL nor dominant input is seen, then the operator's output
    /// "defaults" to the non-dominant value (and we can actually just ignore non-dominant inputs).
    ///
    /// If the filter is inverted, use de Morgan's laws to push the inversion down into the inputs
    /// (e.g. `NOT(AND(a, b))` becomes `OR(NOT(a), NOT(b))`).
    fn apply_variadic(
        &self,
        op: VariadicOperator,
        exprs: &[Expression],
        inverted: bool,
    ) -> Option<bool> {
        // With AND (OR), any FALSE (TRUE) input forces FALSE (TRUE) output.  If there was no
        // dominating input, then any NULL input forces NULL output.  Otherwise, return the
        // non-dominant value. Inverting the operation also inverts the dominant value.
        let dominator = match op {
            VariadicOperator::And => inverted,
            VariadicOperator::Or => !inverted,
        };

        // Evaluate the input expressions, inverting each as needed and tracking whether we've seen
        // any NULL result. Stop immediately (short circuit) if we see a dominant value.
        let result = exprs.iter().try_fold(false, |found_null, expr| {
            match self.apply_expr(expr, inverted) {
                Some(v) if v == dominator => None, // (1) short circuit, dominant found
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

    /// Evaluates binary comparisons. Any NULL input produces a NULL output. If `inverted`, the
    /// opposite operation is performed, e.g. `<` evaluates as if `>=` had been requested instead.
    fn apply_binary(
        &self,
        op: BinaryOperator,
        left: &Expression,
        right: &Expression,
        inverted: bool,
    ) -> Option<bool> {
        use BinaryOperator::*;
        use Expression::{Column, Literal};

        // Min/Max stats don't allow us to push inversion down into the comparison. Instead, we
        // invert the comparison itself when needed and compute normally after that.
        let op = match inverted {
            true => op.invert()?,
            false => op,
        };

        // NOTE: We rely on the literal values to provide logical type hints. That means we cannot
        // perform column-column comparisons, because we cannot infer the logical type to use.
        let (op, col, val) = match (left, right) {
            (Column(col), Literal(val)) => (op, col, val),
            (Literal(val), Column(col)) => (op.commute()?, col, val),
            (Literal(a), Literal(b)) => return Self::apply_binary_scalars(op, a, b),
            _ => None?, // unsupported combination of operands
        };
        let col = col_name_to_path(col);
        let min_max_disjunct = |min_ord, max_ord, inverted| -> Option<bool> {
            let skip_lo = self.partial_cmp_min_stat(&col, val, min_ord, false)?;
            let skip_hi = self.partial_cmp_max_stat(&col, val, max_ord, false)?;
            let skip = skip_lo || skip_hi;
            Some(skip != inverted)
        };
        match op {
            // Given `col == val`:
            // skip if `val` cannot equal _any_ value in [min, max], implies
            // skip if `NOT(val BETWEEN min AND max)` implies
            // skip if `NOT(min <= val AND val <= max)` implies
            // skip if `min > val OR max < val`
            // keep if `NOT(min > val OR max < val)`
            Equal => min_max_disjunct(Ordering::Greater, Ordering::Less, true),
            // Given `col != val`:
            // skip if `val` equals _every_ value in [min, max], implies
            // skip if `val == min AND val == max` implies
            // skip if `val <= min AND min <= val AND val <= max AND max <= val` implies
            // skip if `val <= min AND max <= val` implies
            // keep if `NOT(val <= min AND max <= val)` implies
            // keep if `val > min OR max > val` implies
            // keep if `min < val OR max > val`
            NotEqual => min_max_disjunct(Ordering::Less, Ordering::Greater, false),
            // Given `col < val`:
            // Skip if `val` is not greater than _all_ values in [min, max], implies
            // Skip if `val <= min AND val <= max` implies
            // Skip if `val <= min` implies
            // Keep if `NOT(val <= min)` implies
            // Keep if `val > min` implies
            // Keep if `min < val`
            LessThan => self.partial_cmp_min_stat(&col, val, Ordering::Less, false),
            // Given `col <= val`:
            // Skip if `val` is less than _all_ values in [min, max], implies
            // Skip if `val < min AND val < max` implies
            // Skip if `val < min` implies
            // Keep if `NOT(val < min)` implies
            // Keep if `NOT(min > val)`
            LessThanOrEqual => self.partial_cmp_min_stat(&col, val, Ordering::Greater, true),
            // Given `col > val`:
            // Skip if `val` is not less than _all_ values in [min, max], implies
            // Skip if `val >= min AND val >= max` implies
            // Skip if `val >= max` implies
            // Keep if `NOT(val >= max)` implies
            // Keep if `NOT(max <= val)` implies
            // Keep if `max > val`
            GreaterThan => self.partial_cmp_max_stat(&col, val, Ordering::Greater, false),
            // Given `col >= val`:
            // Skip if `val is greater than _every_ value in [min, max], implies
            // Skip if `val > min AND val > max` implies
            // Skip if `val > max` implies
            // Keep if `NOT(val > max)` implies
            // Keep if `NOT(max < val)`
            GreaterThanOrEqual => self.partial_cmp_max_stat(&col, val, Ordering::Less, true),
            _ => None, // unsupported operation
        }
    }

    /// Helper method, invoked by [`apply_binary`], for constant comparisons. Query planner constant
    /// folding optimizationss SHOULD eliminate such patterns, but we implement the support anyway
    /// because propagating a NULL in its place could disable skipping entirely, e.g. an expression
    /// such as `OR(10 == 20, <false expression>)`.
    fn apply_binary_scalars(op: BinaryOperator, left: &Scalar, right: &Scalar) -> Option<bool> {
        use BinaryOperator::*;
        match op {
            Equal => partial_cmp_scalars(left, right, Ordering::Equal, false),
            NotEqual => partial_cmp_scalars(left, right, Ordering::Equal, true),
            LessThan => partial_cmp_scalars(left, right, Ordering::Less, false),
            LessThanOrEqual => partial_cmp_scalars(left, right, Ordering::Greater, true),
            GreaterThan => partial_cmp_scalars(left, right, Ordering::Greater, false),
            GreaterThanOrEqual => partial_cmp_scalars(left, right, Ordering::Less, true),
            _ => None, // unsupported operation
        }
    }

    /// Applies unary NOT and IS [NOT] NULL. Null inputs to NOT produce NULL output. The null checks
    /// are only defined for columns (not expressions), and they ony produce NULL output if the
    /// necessary nullcount stats are missing.
    fn apply_unary(&self, op: UnaryOperator, expr: &Expression, inverted: bool) -> Option<bool> {
        match op {
            UnaryOperator::Not => self.apply_expr(expr, !inverted),
            UnaryOperator::IsNull => match expr {
                Expression::Column(col) => {
                    let skip = match inverted {
                        // IS NOT NULL - skip if all-null
                        true => self.get_rowcount_stat_value(),
                        // IS NULL - skip if no-null
                        false => 0,
                    };
                    let col = col_name_to_path(col);
                    Some(self.get_nullcount_stat_value(&col)? != skip)
                }
                _ => None,
            },
        }
    }

    /// Propagates a boolean-typed column, allowing e.g. `flag OR ...`.
    /// Columns of other types are ignored (NULL result).
    fn apply_column(&self, col: &str, inverted: bool) -> Option<bool> {
        let col = col_name_to_path(col);
        let as_boolean = |get: &dyn Fn(_, _, _) -> _| match get(self, &col, &DataType::BOOLEAN) {
            Some(Scalar::Boolean(value)) => Some(value),
            _ => None,
        };
        let min = as_boolean(&Self::get_min_stat_value)?;
        let max = as_boolean(&Self::get_max_stat_value)?;
        Some(min != inverted || max != inverted)
    }

    /// Propagates a boolean literal, allowing e.g. `FALSE OR ...`.
    /// Literals of other types are ignored (NULL result).
    fn apply_scalar(value: &Scalar, inverted: bool) -> Option<bool> {
        match value {
            Scalar::Boolean(value) => Some(*value != inverted),
            _ => None,
        }
    }

    /// Performs a partial comparison against a column min-stat. See [`partial_cmp_scalars`] for
    /// details of the comparison semantics.
    fn partial_cmp_min_stat(
        &self,
        col: &ColumnPath,
        val: &Scalar,
        ord: Ordering,
        inverted: bool,
    ) -> Option<bool> {
        let min = self.get_min_stat_value(col, &val.data_type())?;
        partial_cmp_scalars(&min, val, ord, inverted)
    }

    /// Performs a partial comparison against a column max-stat. See [`partial_cmp_scalars`] for
    /// details of the comparison semantics.
    fn partial_cmp_max_stat(
        &self,
        col: &ColumnPath,
        val: &Scalar,
        ord: Ordering,
        inverted: bool,
    ) -> Option<bool> {
        let max = self.get_max_stat_value(col, &val.data_type())?;
        partial_cmp_scalars(&max, val, ord, inverted)
    }
}

/// Compares two scalar values, returning Some(true) if the result matches the target `Ordering`. If
/// an inverted comparison is requested, then return Some(false) on match instead. For example,
/// requesting an inverted `Ordering::Less` matches both `Ordering::Greater` and `Ordering::Equal`,
/// corresponding to a logical `>=` comparison. Returns `None` if the values are incomparable, which
/// can occur because the types differ or because the type itself is incomparable.
pub(crate) fn partial_cmp_scalars(
    a: &Scalar,
    b: &Scalar,
    ord: Ordering,
    inverted: bool,
) -> Option<bool> {
    let result = a.partial_cmp(b)? == ord;
    Some(result != inverted)
}

pub(crate) fn col_name_to_path(col: &str) -> ColumnPath {
    // TODO: properly handle nested columns
    // https://github.com/delta-incubator/delta-kernel-rs/issues/86
    ColumnPath::new(col.split('.').map(|s| s.to_string()).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{ArrayData, StructData};
    use crate::schema::ArrayType;
    use crate::DataType;

    struct UnimplementedTestFilter;
    impl ParquetStatsSkippingFilter for UnimplementedTestFilter {
        fn get_min_stat_value(&self, _col: &ColumnPath, _data_type: &DataType) -> Option<Scalar> {
            unimplemented!()
        }

        fn get_max_stat_value(&self, _col: &ColumnPath, _data_type: &DataType) -> Option<Scalar> {
            unimplemented!()
        }

        fn get_nullcount_stat_value(&self, _col: &ColumnPath) -> Option<i64> {
            unimplemented!()
        }

        fn get_rowcount_stat_value(&self) -> i64 {
            unimplemented!()
        }
    }

    struct JunctionTest {
        inputs: &'static [Option<bool>],
        expect_and: Option<bool>,
        expect_or: Option<bool>,
    }

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
    impl JunctionTest {
        fn new(
            inputs: &'static [Option<bool>],
            expect_and: Option<bool>,
            expect_or: Option<bool>,
        ) -> Self {
            Self {
                inputs,
                expect_and,
                expect_or,
            }
        }
        fn do_test(&self) {
            use VariadicOperator::*;
            let filter = UnimplementedTestFilter;
            let inputs: Vec<_> = self
                .inputs
                .iter()
                .map(|val| match val {
                    Some(v) => Expression::literal(v),
                    None => Expression::null_literal(DataType::BOOLEAN),
                })
                .collect();

            expect_eq!(
                filter.apply_variadic(And, &inputs, false),
                self.expect_and,
                "AND({inputs:?})"
            );
            expect_eq!(
                filter.apply_variadic(Or, &inputs, false),
                self.expect_or,
                "OR({inputs:?})"
            );
            expect_eq!(
                filter.apply_variadic(And, &inputs, true),
                self.expect_and.map(|val| !val),
                "NOT(AND({inputs:?}))"
            );
            expect_eq!(
                filter.apply_variadic(Or, &inputs, true),
                self.expect_or.map(|val| !val),
                "NOT(OR({inputs:?}))"
            );
        }
    }

    /// Tests apply_variadic and apply_scalar
    #[test]
    fn test_junctions() {
        let t = JunctionTest::new;
        const TRUE: Option<bool> = Some(true);
        const FALSE: Option<bool> = Some(false);
        const NULL: Option<bool> = None;
        let test_cases = &[
            // Every combo of 0, 1 and 2 inputs
            t(&[], TRUE, FALSE),
            t(&[TRUE], TRUE, TRUE),
            t(&[FALSE], FALSE, FALSE),
            t(&[NULL], NULL, NULL),
            t(&[TRUE, TRUE], TRUE, TRUE),
            t(&[TRUE, FALSE], FALSE, TRUE),
            t(&[TRUE, NULL], NULL, TRUE),
            t(&[FALSE, TRUE], FALSE, TRUE),
            t(&[FALSE, FALSE], FALSE, FALSE),
            t(&[FALSE, NULL], FALSE, NULL),
            t(&[NULL, TRUE], NULL, TRUE),
            t(&[NULL, FALSE], FALSE, NULL),
            t(&[NULL, NULL], NULL, NULL),
            // Every combo of 1:2
            t(&[TRUE, FALSE, FALSE], FALSE, TRUE),
            t(&[FALSE, TRUE, FALSE], FALSE, TRUE),
            t(&[FALSE, FALSE, TRUE], FALSE, TRUE),
            t(&[TRUE, NULL, NULL], NULL, TRUE),
            t(&[NULL, TRUE, NULL], NULL, TRUE),
            t(&[NULL, NULL, TRUE], NULL, TRUE),
            t(&[FALSE, TRUE, TRUE], FALSE, TRUE),
            t(&[TRUE, FALSE, TRUE], FALSE, TRUE),
            t(&[TRUE, TRUE, FALSE], FALSE, TRUE),
            t(&[FALSE, NULL, NULL], FALSE, NULL),
            t(&[NULL, FALSE, NULL], FALSE, NULL),
            t(&[NULL, NULL, FALSE], FALSE, NULL),
            t(&[NULL, TRUE, TRUE], NULL, TRUE),
            t(&[TRUE, NULL, TRUE], NULL, TRUE),
            t(&[TRUE, TRUE, NULL], NULL, TRUE),
            t(&[NULL, FALSE, FALSE], FALSE, NULL),
            t(&[FALSE, NULL, FALSE], FALSE, NULL),
            t(&[FALSE, FALSE, NULL], FALSE, NULL),
            // Every unique ordering of 3
            t(&[TRUE, FALSE, NULL], FALSE, TRUE),
            t(&[TRUE, NULL, FALSE], FALSE, TRUE),
            t(&[FALSE, TRUE, NULL], FALSE, TRUE),
            t(&[FALSE, NULL, TRUE], FALSE, TRUE),
            t(&[NULL, TRUE, FALSE], FALSE, TRUE),
            t(&[NULL, FALSE, TRUE], FALSE, TRUE),
        ];
        for test_case in test_cases {
            test_case.do_test();
        }
    }

    // tests apply_binary_scalars
    #[test]
    fn test_binary_scalars() {
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
        use BinaryOperator::*;
        let binary_ops = [
            Equal,
            NotEqual,
            LessThan,
            LessThanOrEqual,
            GreaterThan,
            GreaterThanOrEqual,
        ];
        let compare = UnimplementedTestFilter::apply_binary_scalars;
        for (i, a) in smaller_values.iter().enumerate() {
            for b in smaller_values.iter().skip(i + 1) {
                for op in binary_ops {
                    let result = compare(op, a, b);
                    let a_type = a.data_type();
                    let b_type = b.data_type();
                    assert!(
                        result.is_none(),
                        "{a_type:?} should not be comparable to {b_type:?}"
                    );
                }
            }
        }

        let expect_if_comparable_type = |s: &_, expect| match s {
            Null(_) | Decimal(..) | Struct(_) | Array(_) => None,
            _ => Some(expect),
        };

        // Test same-type comparisons where a == b
        for (a, b) in smaller_values.iter().zip(smaller_values.iter()) {
            expect_eq!(
                compare(Equal, a, b),
                expect_if_comparable_type(a, true),
                "{a:?} == {b:?}"
            );

            expect_eq!(
                compare(NotEqual, a, b),
                expect_if_comparable_type(a, false),
                "{a:?} != {b:?}"
            );

            expect_eq!(
                compare(LessThan, a, b),
                expect_if_comparable_type(a, false),
                "{a:?} < {b:?}"
            );

            expect_eq!(
                compare(GreaterThan, a, b),
                expect_if_comparable_type(a, false),
                "{a:?} > {b:?}"
            );

            expect_eq!(
                compare(LessThanOrEqual, a, b),
                expect_if_comparable_type(a, true),
                "{a:?} <= {b:?}"
            );

            expect_eq!(
                compare(GreaterThanOrEqual, a, b),
                expect_if_comparable_type(a, true),
                "{a:?} >= {b:?}"
            );
        }

        // Test same-type comparisons where a < b
        for (a, b) in smaller_values.iter().zip(larger_values.iter()) {
            expect_eq!(
                compare(Equal, a, b),
                expect_if_comparable_type(a, false),
                "{a:?} == {b:?}"
            );

            expect_eq!(
                compare(NotEqual, a, b),
                expect_if_comparable_type(a, true),
                "{a:?} != {b:?}"
            );

            expect_eq!(
                compare(LessThan, a, b),
                expect_if_comparable_type(a, true),
                "{a:?} < {b:?}"
            );

            expect_eq!(
                compare(GreaterThan, a, b),
                expect_if_comparable_type(a, false),
                "{a:?} > {b:?}"
            );

            expect_eq!(
                compare(LessThanOrEqual, a, b),
                expect_if_comparable_type(a, true),
                "{a:?} <= {b:?}"
            );

            expect_eq!(
                compare(GreaterThanOrEqual, a, b),
                expect_if_comparable_type(a, false),
                "{a:?} >= {b:?}"
            );
        }
    }

    struct MinMaxTestFilter {
        min: Option<Scalar>,
        max: Option<Scalar>,
    }
    impl MinMaxTestFilter {
        fn new(min: Option<Scalar>, max: Option<Scalar>) -> Self {
            Self { min, max }
        }
        fn get_stat_value(stat: &Option<Scalar>, data_type: &DataType) -> Option<Scalar> {
            stat.as_ref()
                .filter(|v| v.data_type() == *data_type)
                .cloned()
        }
    }
    impl ParquetStatsSkippingFilter for MinMaxTestFilter {
        fn get_min_stat_value(&self, _col: &ColumnPath, data_type: &DataType) -> Option<Scalar> {
            Self::get_stat_value(&self.min, data_type)
        }

        fn get_max_stat_value(&self, _col: &ColumnPath, data_type: &DataType) -> Option<Scalar> {
            Self::get_stat_value(&self.max, data_type)
        }

        fn get_nullcount_stat_value(&self, _col: &ColumnPath) -> Option<i64> {
            unimplemented!()
        }

        fn get_rowcount_stat_value(&self) -> i64 {
            unimplemented!()
        }
    }

    #[test]
    fn test_binary_eq_ne() {
        use BinaryOperator::*;

        const LO: Scalar = Scalar::Long(1);
        const MID: Scalar = Scalar::Long(10);
        const HI: Scalar = Scalar::Long(100);
        let col = &Expression::column("x");

        for inverted in [false, true] {
            // quick test for literal-literal comparisons
            expect_eq!(
                MinMaxTestFilter::new(MID.into(), MID.into()).apply_binary(
                    Equal,
                    &MID.into(),
                    &MID.into(),
                    inverted
                ),
                Some(!inverted),
                "{MID} == {MID} (min: {MID}, max: {MID}, inverted: {inverted})"
            );

            // quick test for literal-column comparisons
            expect_eq!(
                MinMaxTestFilter::new(MID.into(), MID.into()).apply_binary(
                    Equal,
                    &MID.into(),
                    col,
                    inverted
                ),
                Some(!inverted),
                "{MID} == {col} (min: {MID}, max: {MID}, inverted: {inverted})"
            );

            expect_eq!(
                MinMaxTestFilter::new(MID.into(), MID.into()).apply_binary(
                    Equal,
                    col,
                    &MID.into(),
                    inverted
                ),
                Some(!inverted),
                "{col} == {MID} (min: {MID}, max: {MID}, inverted: {inverted})"
            );

            expect_eq!(
                MinMaxTestFilter::new(LO.into(), HI.into()).apply_binary(
                    Equal,
                    col,
                    &MID.into(),
                    inverted
                ),
                Some(true), // min..max range includes both EQ and NE
                "{col} == {MID} (min: {LO}, max: {HI}, inverted: {inverted})"
            );

            expect_eq!(
                MinMaxTestFilter::new(LO.into(), MID.into()).apply_binary(
                    Equal,
                    col,
                    &HI.into(),
                    inverted
                ),
                Some(inverted),
                "{col} == {HI} (min: {LO}, max: {MID}, inverted: {inverted})"
            );

            expect_eq!(
                MinMaxTestFilter::new(MID.into(), HI.into()).apply_binary(
                    Equal,
                    col,
                    &LO.into(),
                    inverted
                ),
                Some(inverted),
                "{col} == {LO} (min: {MID}, max: {HI}, inverted: {inverted})"
            );

            expect_eq!(
                MinMaxTestFilter::new(MID.into(), MID.into()).apply_binary(
                    NotEqual,
                    col,
                    &MID.into(),
                    inverted
                ),
                Some(inverted),
                "{col} != {MID} (min: {MID}, max: {MID}, inverted: {inverted})"
            );

            expect_eq!(
                MinMaxTestFilter::new(LO.into(), HI.into()).apply_binary(
                    NotEqual,
                    col,
                    &MID.into(),
                    inverted
                ),
                Some(true), // min..max range includes both EQ and NE
                "{col} != {MID} (min: {LO}, max: {HI}, inverted: {inverted})"
            );

            expect_eq!(
                MinMaxTestFilter::new(LO.into(), MID.into()).apply_binary(
                    NotEqual,
                    col,
                    &HI.into(),
                    inverted
                ),
                Some(!inverted),
                "{col} != {HI} (min: {LO}, max: {MID}, inverted: {inverted})"
            );

            expect_eq!(
                MinMaxTestFilter::new(MID.into(), HI.into()).apply_binary(
                    NotEqual,
                    col,
                    &LO.into(),
                    inverted
                ),
                Some(!inverted),
                "{col} != {LO} (min: {MID}, max: {HI}, inverted: {inverted})"
            );
        }
    }

    #[test]
    fn test_binary_lt_ge() {
        use BinaryOperator::*;

        const LO: Scalar = Scalar::Long(1);
        const MID: Scalar = Scalar::Long(10);
        const HI: Scalar = Scalar::Long(100);
        let col = &Expression::column("x");

        for inverted in [false, true] {
            expect_eq!(
                MinMaxTestFilter::new(MID.into(), MID.into()).apply_binary(
                    LessThan,
                    col,
                    &MID.into(),
                    inverted
                ),
                Some(inverted),
                "{col} < {MID} (min: {MID}, max: {MID}, inverted: {inverted})"
            );

            expect_eq!(
                MinMaxTestFilter::new(LO.into(), HI.into()).apply_binary(
                    LessThan,
                    col,
                    &MID.into(),
                    inverted
                ),
                Some(true), // min..max range includes both LT and GE
                "{col} < {MID} (min: {LO}, max: {HI}, inverted: {inverted})"
            );

            expect_eq!(
                MinMaxTestFilter::new(LO.into(), MID.into()).apply_binary(
                    LessThan,
                    col,
                    &HI.into(),
                    inverted
                ),
                Some(!inverted),
                "{col} < {HI} (min: {LO}, max: {MID}, inverted: {inverted})"
            );

            expect_eq!(
                MinMaxTestFilter::new(MID.into(), HI.into()).apply_binary(
                    LessThan,
                    col,
                    &LO.into(),
                    inverted
                ),
                Some(inverted),
                "{col} < {LO} (min: {MID}, max: {HI}, inverted: {inverted})"
            );

            expect_eq!(
                MinMaxTestFilter::new(MID.into(), MID.into()).apply_binary(
                    GreaterThanOrEqual,
                    col,
                    &MID.into(),
                    inverted
                ),
                Some(!inverted),
                "{col} >= {MID} (min: {MID}, max: {MID}, inverted: {inverted})"
            );

            expect_eq!(
                MinMaxTestFilter::new(LO.into(), HI.into()).apply_binary(
                    GreaterThanOrEqual,
                    col,
                    &MID.into(),
                    inverted
                ),
                Some(true), // min..max range includes both EQ and NE
                "{col} >= {MID} (min: {LO}, max: {HI}, inverted: {inverted})"
            );

            expect_eq!(
                MinMaxTestFilter::new(LO.into(), MID.into()).apply_binary(
                    GreaterThanOrEqual,
                    col,
                    &HI.into(),
                    inverted
                ),
                Some(inverted),
                "{col} >= {HI} (min: {LO}, max: {MID}, inverted: {inverted})"
            );

            expect_eq!(
                MinMaxTestFilter::new(MID.into(), HI.into()).apply_binary(
                    GreaterThanOrEqual,
                    col,
                    &LO.into(),
                    inverted
                ),
                Some(!inverted),
                "{col} >= {LO} (min: {MID}, max: {HI}, inverted: {inverted})"
            );
        }
    }

    #[test]
    fn test_binary_le_gt() {
        use BinaryOperator::*;

        const LO: Scalar = Scalar::Long(1);
        const MID: Scalar = Scalar::Long(10);
        const HI: Scalar = Scalar::Long(100);
        let col = &Expression::column("x");

        for inverted in [false, true] {
            expect_eq!(
                MinMaxTestFilter::new(MID.into(), MID.into()).apply_binary(
                    LessThanOrEqual,
                    col,
                    &MID.into(),
                    inverted
                ),
                Some(!inverted),
                "{col} <= {MID} (min: {MID}, max: {MID}, inverted: {inverted})"
            );

            expect_eq!(
                MinMaxTestFilter::new(LO.into(), HI.into()).apply_binary(
                    LessThanOrEqual,
                    col,
                    &MID.into(),
                    inverted
                ),
                Some(true), // min..max range includes both LT and GE
                "{col} <= {MID} (min: {LO}, max: {HI}, inverted: {inverted})"
            );

            expect_eq!(
                MinMaxTestFilter::new(LO.into(), MID.into()).apply_binary(
                    LessThanOrEqual,
                    col,
                    &HI.into(),
                    inverted
                ),
                Some(!inverted),
                "{col} <= {HI} (min: {LO}, max: {MID}, inverted: {inverted})"
            );

            expect_eq!(
                MinMaxTestFilter::new(MID.into(), HI.into()).apply_binary(
                    LessThanOrEqual,
                    col,
                    &LO.into(),
                    inverted
                ),
                Some(inverted),
                "{col} <= {LO} (min: {MID}, max: {HI}, inverted: {inverted})"
            );

            expect_eq!(
                MinMaxTestFilter::new(MID.into(), MID.into()).apply_binary(
                    GreaterThan,
                    col,
                    &MID.into(),
                    inverted
                ),
                Some(inverted),
                "{col} > {MID} (min: {MID}, max: {MID}, inverted: {inverted})"
            );

            expect_eq!(
                MinMaxTestFilter::new(LO.into(), HI.into()).apply_binary(
                    GreaterThan,
                    col,
                    &MID.into(),
                    inverted
                ),
                Some(true), // min..max range includes both EQ and NE
                "{col} > {MID} (min: {LO}, max: {HI}, inverted: {inverted})"
            );

            expect_eq!(
                MinMaxTestFilter::new(LO.into(), MID.into()).apply_binary(
                    GreaterThan,
                    col,
                    &HI.into(),
                    inverted
                ),
                Some(inverted),
                "{col} > {HI} (min: {LO}, max: {MID}, inverted: {inverted})"
            );

            expect_eq!(
                MinMaxTestFilter::new(MID.into(), HI.into()).apply_binary(
                    GreaterThan,
                    col,
                    &LO.into(),
                    inverted
                ),
                Some(!inverted),
                "{col} > {LO} (min: {MID}, max: {HI}, inverted: {inverted})"
            );
        }
    }

    struct NullCountTestFilter {
        nullcount: Option<i64>,
        rowcount: i64,
    }
    impl NullCountTestFilter {
        fn new(nullcount: Option<i64>, rowcount: i64) -> Self {
            Self {
                nullcount,
                rowcount,
            }
        }
    }
    impl ParquetStatsSkippingFilter for NullCountTestFilter {
        fn get_min_stat_value(&self, _col: &ColumnPath, _data_type: &DataType) -> Option<Scalar> {
            unimplemented!()
        }

        fn get_max_stat_value(&self, _col: &ColumnPath, _data_type: &DataType) -> Option<Scalar> {
            unimplemented!()
        }

        fn get_nullcount_stat_value(&self, _col: &ColumnPath) -> Option<i64> {
            self.nullcount
        }

        fn get_rowcount_stat_value(&self) -> i64 {
            self.rowcount
        }
    }

    #[test]
    fn test_not_null() {
        use UnaryOperator::IsNull;

        let col = &Expression::column("x");
        for inverted in [false, true] {
            expect_eq!(
                NullCountTestFilter::new(None, 10).apply_unary(IsNull, col, inverted),
                None,
                "{col} IS NULL (nullcount: None, rowcount: 10, inverted: {inverted})"
            );

            expect_eq!(
                NullCountTestFilter::new(Some(0), 10).apply_unary(IsNull, col, inverted),
                Some(inverted),
                "{col} IS NULL (nullcount: 0, rowcount: 10, inverted: {inverted})"
            );

            expect_eq!(
                NullCountTestFilter::new(Some(5), 10).apply_unary(IsNull, col, inverted),
                Some(true),
                "{col} IS NULL (nullcount: 5, rowcount: 10, inverted: {inverted})"
            );

            expect_eq!(
                NullCountTestFilter::new(Some(10), 10).apply_unary(IsNull, col, inverted),
                Some(!inverted),
                "{col} IS NULL (nullcount: 10, rowcount: 10, inverted: {inverted})"
            );
        }
    }

    #[test]
    fn test_bool_col() {
        use Scalar::Boolean;
        const TRUE: Scalar = Boolean(true);
        const FALSE: Scalar = Boolean(false);
        for inverted in [false, true] {
            expect_eq!(
                MinMaxTestFilter::new(TRUE.into(), TRUE.into()).apply_column("x", inverted),
                Some(!inverted),
                "x as boolean (min: TRUE, max: TRUE, inverted: {inverted})"
            );
            expect_eq!(
                MinMaxTestFilter::new(FALSE.into(), TRUE.into()).apply_column("x", inverted),
                Some(true),
                "x as boolean (min: FALSE, max: TRUE, inverted: {inverted})"
            );
            expect_eq!(
                MinMaxTestFilter::new(FALSE.into(), FALSE.into()).apply_column("x", inverted),
                Some(inverted),
                "x as boolean (min: FALSE, max: FALSE, inverted: {inverted})"
            );
        }
    }

    struct AllNullTestFilter;
    impl ParquetStatsSkippingFilter for AllNullTestFilter {
        fn get_min_stat_value(&self, _col: &ColumnPath, _data_type: &DataType) -> Option<Scalar> {
            None
        }

        fn get_max_stat_value(&self, _col: &ColumnPath, _data_type: &DataType) -> Option<Scalar> {
            None
        }

        fn get_nullcount_stat_value(&self, _col: &ColumnPath) -> Option<i64> {
            Some(self.get_rowcount_stat_value())
        }

        fn get_rowcount_stat_value(&self) -> i64 {
            10
        }
    }

    #[test]
    fn test_sql_where() {
        let col = &Expression::column("x");
        let val = &Expression::literal(1);
        const NULL: Expression = Expression::Literal(Scalar::Null(DataType::BOOLEAN));
        const FALSE: Expression = Expression::Literal(Scalar::Boolean(false));
        const TRUE: Expression = Expression::Literal(Scalar::Boolean(true));

        // Basic sanity checks
        expect_eq!(AllNullTestFilter.apply_sql_where(val), None, "WHERE {val}");
        expect_eq!(AllNullTestFilter.apply_sql_where(col), None, "WHERE {col}");
        expect_eq!(
            AllNullTestFilter.apply_sql_where(&Expression::is_null(col.clone())),
            Some(true), // No injected NULL checks
            "WHERE {col} IS NULL"
        );
        expect_eq!(
            AllNullTestFilter.apply_sql_where(&Expression::lt(TRUE, FALSE)),
            Some(false), // Injected NULL checks don't short circuit when inputs are NOT NULL
            "WHERE {TRUE} < {FALSE}"
        );

        // Constrast normal vs SQL WHERE semantics - comparison
        expect_eq!(
            AllNullTestFilter.apply_expr(&Expression::lt(col.clone(), val.clone()), false),
            None,
            "{col} < {val}"
        );
        expect_eq!(
            AllNullTestFilter.apply_sql_where(&Expression::lt(col.clone(), val.clone())),
            Some(false),
            "WHERE {col} < {val}"
        );
        expect_eq!(
            AllNullTestFilter.apply_expr(&Expression::lt(val.clone(), col.clone()), false),
            None,
            "{val} < {col}"
        );
        expect_eq!(
            AllNullTestFilter.apply_sql_where(&Expression::lt(val.clone(), col.clone())),
            Some(false),
            "WHERE {val} < {col}"
        );

        // Constrast normal vs SQL WHERE semantics - comparison inside AND
        expect_eq!(
            AllNullTestFilter.apply_expr(
                &Expression::and_from([NULL, Expression::lt(col.clone(), val.clone()),]),
                false
            ),
            None,
            "{NULL} AND {col} < {val}"
        );
        expect_eq!(
            AllNullTestFilter.apply_sql_where(&Expression::and_from([
                NULL,
                Expression::lt(col.clone(), val.clone()),
            ])),
            Some(false),
            "WHERE {NULL} AND {col} < {val}"
        );

        expect_eq!(
            AllNullTestFilter.apply_expr(
                &Expression::and_from([TRUE, Expression::lt(col.clone(), val.clone()),]),
                false
            ),
            None, // NULL (from the NULL check) is stronger than TRUE
            "{TRUE} AND {col} < {val}"
        );
        expect_eq!(
            AllNullTestFilter.apply_sql_where(&Expression::and_from([
                TRUE,
                Expression::lt(col.clone(), val.clone()),
            ])),
            Some(false), // FALSE (from the NULL check) is stronger than TRUE
            "WHERE {TRUE} AND {col} < {val}"
        );

        // Contrast normal vs. SQL WHERE semantics - comparison inside AND inside AND
        expect_eq!(
            AllNullTestFilter.apply_expr(
                &Expression::and_from([
                    TRUE,
                    Expression::and_from([NULL, Expression::lt(col.clone(), val.clone()),]),
                ]),
                false,
            ),
            None,
            "{TRUE} AND ({NULL} AND {col} < {val})"
        );
        expect_eq!(
            AllNullTestFilter.apply_sql_where(&Expression::and_from([
                TRUE,
                Expression::and_from([NULL, Expression::lt(col.clone(), val.clone()),]),
            ])),
            Some(false),
            "WHERE {TRUE} AND ({NULL} AND {col} < {val})"
        );

        // Semantics are the same for comparison inside OR inside AND
        expect_eq!(
            AllNullTestFilter.apply_expr(
                &Expression::or_from([
                    FALSE,
                    Expression::and_from([NULL, Expression::lt(col.clone(), val.clone()),]),
                ]),
                false,
            ),
            None,
            "{FALSE} OR ({NULL} AND {col} < {val})"
        );
        expect_eq!(
            AllNullTestFilter.apply_sql_where(&Expression::or_from([
                FALSE,
                Expression::and_from([NULL, Expression::lt(col.clone(), val.clone()),]),
            ])),
            None,
            "WHERE {FALSE} OR ({NULL} AND {col} < {val})"
        );
    }
}

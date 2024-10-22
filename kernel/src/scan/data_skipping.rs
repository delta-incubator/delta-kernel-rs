use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::{Arc, LazyLock};

use tracing::debug;

use crate::actions::visitors::SelectionVectorVisitor;
use crate::actions::{get_log_schema, ADD_NAME};
use crate::error::DeltaResult;
use crate::expressions::{
    BinaryOperator, Expression as Expr, ExpressionRef, Scalar, UnaryOperator, VariadicOperator,
};
use crate::schema::{DataType, PrimitiveType, SchemaRef, SchemaTransform, StructField, StructType};
use crate::{Engine, EngineData, ExpressionEvaluator, JsonHandler};

/// Rewrites a predicate to a predicate that can be used to skip files based on their stats.
/// Returns `None` if the predicate is not eligible for data skipping.
///
/// We normalize each binary operation to a comparison between a column and a literal value and
/// rewite that in terms of the min/max values of the column.
/// For example, `1 < a` is rewritten as `minValues.a > 1`.
///
/// For Unary `Not`, we push the Not down using De Morgan's Laws to invert everything below the Not.
///
/// Unary `IsNull` checks if the null counts indicate that the column could contain a null.
///
/// The variadic operations are rewritten as follows:
/// - `AND` is rewritten as a conjunction of the rewritten operands where we just skip operands that
///         are not eligible for data skipping.
/// - `OR` is rewritten only if all operands are eligible for data skipping. Otherwise, the whole OR
///        expression is dropped.
fn as_data_skipping_predicate(expr: &Expr, inverted: bool) -> Option<Expr> {
    DataSkippingPredicateCreator.eval_expr(expr, inverted)
}

pub(crate) struct DataSkippingFilter {
    stats_schema: SchemaRef,
    select_stats_evaluator: Arc<dyn ExpressionEvaluator>,
    skipping_evaluator: Arc<dyn ExpressionEvaluator>,
    filter_evaluator: Arc<dyn ExpressionEvaluator>,
    json_handler: Arc<dyn JsonHandler>,
}

impl DataSkippingFilter {
    /// Creates a new data skipping filter. Returns None if there is no predicate, or the predicate
    /// is ineligible for data skipping.
    ///
    /// NOTE: None is equivalent to a trivial filter that always returns TRUE (= keeps all files),
    /// but using an Option lets the engine easily avoid the overhead of applying trivial filters.
    pub(crate) fn new(
        engine: &dyn Engine,
        table_schema: &SchemaRef,
        predicate: Option<ExpressionRef>,
    ) -> Option<Self> {
        static PREDICATE_SCHEMA: LazyLock<DataType> = LazyLock::new(|| {
            DataType::struct_type([StructField::new("predicate", DataType::BOOLEAN, true)])
        });
        static STATS_EXPR: LazyLock<Expr> = LazyLock::new(|| Expr::column("add.stats"));
        static FILTER_EXPR: LazyLock<Expr> =
            LazyLock::new(|| Expr::column("predicate").distinct(false));

        let predicate = predicate.as_deref()?;
        debug!("Creating a data skipping filter for {}", &predicate);
        let field_names: HashSet<_> = predicate.references();

        // Build the stats read schema by extracting the column names referenced by the predicate,
        // extracting the corresponding field from the table schema, and inserting that field.
        let data_fields: Vec<_> = table_schema
            .fields()
            .filter(|field| field_names.contains(&field.name.as_str()))
            .cloned()
            .collect();
        if data_fields.is_empty() {
            // The predicate didn't reference any eligible stats columns, so skip it.
            return None;
        }
        let minmax_schema = StructType::new(data_fields);

        // Convert a min/max stats schema into a nullcount schema (all leaf fields are LONG)
        struct NullCountStatsTransform;
        impl SchemaTransform for NullCountStatsTransform {
            fn transform_primitive<'a>(
                &mut self,
                _ptype: Cow<'a, PrimitiveType>,
            ) -> Option<Cow<'a, PrimitiveType>> {
                Some(Cow::Owned(PrimitiveType::Long))
            }
        }
        let nullcount_schema = NullCountStatsTransform
            .transform_struct(Cow::Borrowed(&minmax_schema))?
            .into_owned();
        let stats_schema = Arc::new(StructType::new([
            StructField::new("numRecords", DataType::LONG, true),
            StructField::new("tightBounds", DataType::BOOLEAN, true),
            StructField::new("nullCount", nullcount_schema, true),
            StructField::new("minValues", minmax_schema.clone(), true),
            StructField::new("maxValues", minmax_schema, true),
        ]));

        // Skipping happens in several steps:
        //
        // 1. The stats selector fetches add.stats from the metadata
        //
        // 2. The predicate (skipping evaluator) produces false for any file whose stats prove we
        //    can safely skip it. A value of true means the stats say we must keep the file, and
        //    null means we could not determine whether the file is safe to skip, because its stats
        //    were missing/null.
        //
        // 3. The selection evaluator does DISTINCT(col(predicate), 'false') to produce true (= keep) when
        //    the predicate is true/null and false (= skip) when the predicate is false.
        let select_stats_evaluator = engine.get_expression_handler().get_evaluator(
            // safety: kernel is very broken if we don't have the schema for Add actions
            get_log_schema().project(&[ADD_NAME]).unwrap(),
            STATS_EXPR.clone(),
            DataType::STRING,
        );

        let skipping_evaluator = engine.get_expression_handler().get_evaluator(
            stats_schema.clone(),
            Expr::struct_from([as_data_skipping_predicate(predicate, false)?]),
            PREDICATE_SCHEMA.clone(),
        );

        let filter_evaluator = engine.get_expression_handler().get_evaluator(
            stats_schema.clone(),
            FILTER_EXPR.clone(),
            DataType::BOOLEAN,
        );

        Some(Self {
            stats_schema,
            select_stats_evaluator,
            skipping_evaluator,
            filter_evaluator,
            json_handler: engine.get_json_handler(),
        })
    }

    /// Apply the DataSkippingFilter to an EngineData batch of actions. Returns a selection vector
    /// which can be applied to the actions to find those that passed data skipping.
    pub(crate) fn apply(&self, actions: &dyn EngineData) -> DeltaResult<Vec<bool>> {
        // retrieve and parse stats from actions data
        let stats = self.select_stats_evaluator.evaluate(actions)?;
        assert_eq!(stats.length(), actions.length());
        let parsed_stats = self
            .json_handler
            .parse_json(stats, self.stats_schema.clone())?;
        assert_eq!(parsed_stats.length(), actions.length());

        // evaluate the predicate on the parsed stats, then convert to selection vector
        let skipping_predicate = self.skipping_evaluator.evaluate(&*parsed_stats)?;
        assert_eq!(skipping_predicate.length(), actions.length());
        let selection_vector = self
            .filter_evaluator
            .evaluate(skipping_predicate.as_ref())?;
        assert_eq!(selection_vector.length(), actions.length());

        // visit the engine's selection vector to produce a Vec<bool>
        let mut visitor = SelectionVectorVisitor::default();
        let schema = StructType::new([StructField::new("output", DataType::BOOLEAN, false)]);
        selection_vector
            .as_ref()
            .extract(Arc::new(schema), &mut visitor)?;
        Ok(visitor.selection_vector)

        // TODO(zach): add some debug info about data skipping that occurred
        // let before_count = actions.length();
        // debug!(
        //     "number of actions before/after data skipping: {before_count} / {}",
        //     filtered_actions.num_rows()
        // );
    }
}
/// Evaluates a predicate expression tree against column names that resolve as scalars. Useful for
/// testing/debugging but also serves as a reference implementation that documents the expression
/// semantics that kernel relies on for data skipping.
#[allow(unused, clippy::unreachable)]
trait PredicateEvaluator {
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

    fn eval_binary_scalars(&self, op: BinaryOperator, left: &Scalar, right: &Scalar, inverted: bool) -> Option<Self::Output>;

    fn eval_binary(&self, op: BinaryOperator, left: &Expr, right: &Expr, inverted: bool) -> Option<Self::Output> {
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

    fn finish_eval_variadic(&self, op: VariadicOperator, exprs: impl IntoIterator<Item = Option<Self::Output>>, inverted: bool) -> Option<Self::Output>;

    fn eval_variadic(&self, op: VariadicOperator, exprs: &[Expr], inverted: bool) -> Option<Self::Output> {

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
#[allow(unused)]
struct DefaultPredicateEvaluator;
#[allow(unused)]
impl DefaultPredicateEvaluator {

    fn resolve_column(&self, _col: &str) -> Option<Scalar> {
        todo!()
    }

    fn partial_cmp_scalars(
        a: &Scalar,
        b: &Scalar,
        ord: Ordering,
        inverted: bool,
    ) -> Option<bool> {
        let result = a.partial_cmp(b)? == ord;
        Some(result != inverted)
    }

    fn eval_scalar(val: &Scalar, inverted: bool) -> Option<bool> {
        match val {
            Scalar::Boolean(val) => Some(*val != inverted),
            _ => None,
        }
    }

    fn eval_binary_scalars(op: BinaryOperator, left: &Scalar, right: &Scalar, inverted: bool) -> Option<bool> {
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

    fn finish_eval_variadic(op: VariadicOperator, exprs: impl IntoIterator<Item = Option<bool>>, inverted: bool) -> Option<bool> {
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
        self.eval_binary_scalars(BinaryOperator::LessThan, &self.resolve_column(col)?, val, false)
    }

    fn eval_le(&self, col: &str, val: &Scalar) -> Option<bool> {
        self.eval_binary_scalars(BinaryOperator::LessThanOrEqual, &self.resolve_column(col)?, val, false)
    }

    fn eval_gt(&self, col: &str, val: &Scalar) -> Option<bool> {
        self.eval_binary_scalars(BinaryOperator::GreaterThan, &self.resolve_column(col)?, val, false)
    }

    fn eval_ge(&self, col: &str, val: &Scalar) -> Option<bool> {
        self.eval_binary_scalars(BinaryOperator::GreaterThanOrEqual, &self.resolve_column(col)?, val, false)
    }

    fn eval_eq(&self, col: &str, val: &Scalar, inverted: bool) -> Option<bool> {
        self.eval_binary_scalars(BinaryOperator::Equal, &self.resolve_column(col)?, val, inverted)
    }

    fn eval_binary_scalars(&self, op: BinaryOperator, left: &Scalar, right: &Scalar, inverted: bool) -> Option<bool> {
        Self::eval_binary_scalars(op, left, right, inverted)
    }

    fn finish_eval_variadic(&self, op: VariadicOperator, exprs: impl IntoIterator<Item = Option<bool>>, inverted: bool) -> Option<bool> {
        Self::finish_eval_variadic(op, exprs, inverted)
    }
}

trait StatsColumnProvider {
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

    fn eval_partial_cmp(&self, col: Self::TypedOutput, val: &Scalar, ord: Ordering, inverted: bool) -> Option<Self::BoolOutput>;

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

#[allow(unused)]
struct ParquetStatsSkippingPredicateEvaluator;

#[allow(unused)]
impl ParquetStatsSkippingPredicateEvaluator {
}
impl StatsColumnProvider for ParquetStatsSkippingPredicateEvaluator {
    type TypedOutput = Scalar;
    type IntOutput = i64;
    type BoolOutput = bool;

    /// Retrieves the minimum value of a column, if it exists and has the requested type.
    fn get_min_stat(&self, _col: &str, _data_type: &DataType) -> Option<Scalar> {
        todo!()
    }

    /// Retrieves the maximum value of a column, if it exists and has the requested type.
    fn get_max_stat(&self, _col: &str, _data_type: &DataType) -> Option<Scalar> {
        todo!()
    }

    /// Retrieves the null count of a column, if it exists.
    fn get_nullcount_stat(&self, _col: &str) -> Option<i64> {
        todo!()
    }

    /// Retrieves the row count of a column (parquet footers always include this stat).
    fn get_rowcount_stat(&self) -> Option<i64> {
        todo!()
    }

    fn eval_partial_cmp(&self, col: Scalar, val: &Scalar, ord: Ordering, inverted: bool) -> Option<bool> {
        DefaultPredicateEvaluator::partial_cmp_scalars(&col, val, ord, inverted)
    }

}
impl DataSkippingPredicateEvaluator for ParquetStatsSkippingPredicateEvaluator {

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

    fn eval_binary_scalars(&self, op: BinaryOperator, left: &Scalar, right: &Scalar, inverted: bool) -> Option<bool> {
        DefaultPredicateEvaluator::eval_binary_scalars(op, left, right, inverted)
    }

    fn finish_eval_variadic(&self, op: VariadicOperator, exprs: impl IntoIterator<Item = Option<bool>>, inverted: bool) -> Option<bool> {
        DefaultPredicateEvaluator::finish_eval_variadic(op, exprs, inverted)
    }
}

#[allow(unused)]
struct DataSkippingPredicateCreator;

#[allow(unused)]
impl DataSkippingPredicateCreator {
    /// Get the expression that checks IS [NOT] NULL for a column with tight bounds. A column may
    /// satisfy IS NULL if `nullCount != 0`, and may satisfy IS NOT NULL if `nullCount !=
    /// numRecords`. Note that a the bounds are tight unless `tightBounds` is neither TRUE nor NULL.
    fn get_tight_is_null_expr(nullcount_col: Expr, inverted: bool) -> Expr {
        let sentinel_value = if inverted {
            Expr::column("numRecords")
        } else {
            Expr::literal(0i64)
        };
        Expr::and(
            Expr::distinct(Expr::column("tightBounds"), false),
            Expr::ne(nullcount_col, sentinel_value),
        )
    }

    /// Get the expression that checks if a col could be null, assuming tight_bounds = false. In this
    /// case, we can only check whether the WHOLE column is null or not, by checking if the number of
    /// records is equal to the null count. Any other values of nullCount must be ignored (except 0,
    /// which doesn't help us).
    fn get_wide_is_null_expr(nullcount_col: Expr, inverted: bool) -> Expr {
        let op = if inverted {
            BinaryOperator::NotEqual
        } else {
            BinaryOperator::Equal
        };
        Expr::and(
            Expr::eq(Expr::column("tightBounds"), false),
            Expr::binary(op, Expr::column("numRecords"), nullcount_col),
        )
    }

}

impl StatsColumnProvider for DataSkippingPredicateCreator {
    type TypedOutput = Expr;
    type IntOutput = Expr;
    type BoolOutput = Expr;

    /// Retrieves the minimum value of a column, if it exists and has the requested type.
    fn get_min_stat(&self, col: &str, _data_type: &DataType) -> Option<Expr> {
        let col = format!("minValues.{}", col);
        Some(Expr::column(col))
    }

    /// Retrieves the maximum value of a column, if it exists and has the requested type.
    fn get_max_stat(&self, col: &str, _data_type: &DataType) -> Option<Expr> {
        let col = format!("maxValues.{}", col);
        Some(Expr::column(col))
    }

    /// Retrieves the null count of a column, if it exists.
    fn get_nullcount_stat(&self, col: &str) -> Option<Expr> {
        let col = format!("nullCount.{}", col);
        Some(Expr::column(col))
    }

    /// Retrieves the row count of a column (parquet footers always include this stat).
    fn get_rowcount_stat(&self) -> Option<Expr> {
        let col = "minValues";
        Some(Expr::column(col))
    }

    fn eval_partial_cmp(&self, col: Expr, val: &Scalar, ord: Ordering, inverted: bool) -> Option<Expr> {
        let op = match (ord, inverted) {
            (Ordering::Less, false) => BinaryOperator::LessThan,
            (Ordering::Less, true) => BinaryOperator::GreaterThanOrEqual,
            (Ordering::Equal, false) => BinaryOperator::Equal,
            (Ordering::Equal, true) => BinaryOperator::NotEqual,
            (Ordering::Greater, false) => BinaryOperator::GreaterThan,
            (Ordering::Greater, true) => BinaryOperator::LessThanOrEqual,
        };
        Some(Expr::binary(op, col, val.clone()))
    }
}

trait DataSkippingPredicateEvaluator : StatsColumnProvider {

    fn eval_scalar(&self, val: &Scalar, inverted: bool) -> Option<Self::BoolOutput>;

    fn eval_is_null(&self, col: &str, inverted: bool) -> Option<Self::BoolOutput>;

    fn eval_binary_scalars(&self, op: BinaryOperator, left: &Scalar, right: &Scalar, inverted: bool) -> Option<Self::BoolOutput>;

    fn finish_eval_variadic(&self, op: VariadicOperator, exprs: impl IntoIterator<Item = Option<Self::BoolOutput>>, inverted: bool) -> Option<Self::BoolOutput>;

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

    fn eval_binary_scalars(&self, op: BinaryOperator, left: &Scalar, right: &Scalar, inverted: bool) -> Option<Self::Output> {
        DataSkippingPredicateEvaluator::eval_binary_scalars(self, op, left, right, inverted)
    }

    fn finish_eval_variadic(&self, op: VariadicOperator, exprs: impl IntoIterator<Item = Option<Self::Output>>, inverted: bool) -> Option<Self::Output> {
        DataSkippingPredicateEvaluator::finish_eval_variadic(self, op, exprs, inverted)
    }

}

impl DataSkippingPredicateEvaluator for DataSkippingPredicateCreator {
    fn eval_scalar(&self, val: &Scalar, inverted: bool) -> Option<Expr> {
        DefaultPredicateEvaluator::eval_scalar(val, inverted).map(Expr::literal)
    }

    fn eval_is_null(&self, col: &str, inverted: bool) -> Option<Expr> {
        // to check if a column could have a null, we need two different checks, to see if
        // the bounds are tight and then to actually do the check
        let nullcount = self.get_nullcount_stat(col)?;
        Some(Expr::or(
            Self::get_tight_is_null_expr(nullcount.clone(), inverted),
            Self::get_wide_is_null_expr(nullcount, inverted),
        ))
    }

    fn eval_binary_scalars(&self, op: BinaryOperator, left: &Scalar, right: &Scalar, inverted: bool) -> Option<Expr> {
        DefaultPredicateEvaluator::eval_binary_scalars(op, left, right, inverted).map(Expr::literal)
    }

    fn finish_eval_variadic(&self, mut op: VariadicOperator, exprs: impl IntoIterator<Item = Option<Expr>>, inverted: bool) -> Option<Expr> {
        if inverted {
            op = op.invert();
        }
        let exprs = exprs.into_iter();
        let exprs: Vec<_> = match op {
            VariadicOperator::And => exprs.flatten().collect(),
            VariadicOperator::Or => exprs.collect::<Option<_>>()?,
        };
        Some(Expr::variadic(op, exprs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rewrite_basic_comparison() {
        let column = Expr::column("a");
        let lit_int = Expr::literal(1_i32);
        let min_col = Expr::column("minValues.a");
        let max_col = Expr::column("maxValues.a");

        let cases = [
            (
                column.clone().lt(lit_int.clone()),
                Expr::lt(min_col.clone(), lit_int.clone()),
            ),
            (
                lit_int.clone().lt(column.clone()),
                Expr::gt(max_col.clone(), lit_int.clone()),
            ),
            (
                column.clone().gt(lit_int.clone()),
                Expr::gt(max_col.clone(), lit_int.clone()),
            ),
            (
                lit_int.clone().gt(column.clone()),
                Expr::lt(min_col.clone(), lit_int.clone()),
            ),
            (
                column.clone().lt_eq(lit_int.clone()),
                Expr::le(min_col.clone(), lit_int.clone()),
            ),
            (
                lit_int.clone().lt_eq(column.clone()),
                Expr::ge(max_col.clone(), lit_int.clone()),
            ),
            (
                column.clone().gt_eq(lit_int.clone()),
                Expr::ge(max_col.clone(), lit_int.clone()),
            ),
            (
                lit_int.clone().gt_eq(column.clone()),
                Expr::le(min_col.clone(), lit_int.clone()),
            ),
            (
                column.clone().eq(lit_int.clone()),
                Expr::and_from([
                    Expr::le(min_col.clone(), lit_int.clone()),
                    Expr::ge(max_col.clone(), lit_int.clone()),
                ]),
            ),
            (
                lit_int.clone().eq(column.clone()),
                Expr::and_from([
                    Expr::le(min_col.clone(), lit_int.clone()),
                    Expr::ge(max_col.clone(), lit_int.clone()),
                ]),
            ),
            (
                column.clone().ne(lit_int.clone()),
                Expr::or_from([
                    Expr::ne(min_col.clone(), lit_int.clone()),
                    Expr::ne(max_col.clone(), lit_int.clone()),
                ]),
            ),
            (
                lit_int.clone().ne(column.clone()),
                Expr::or_from([
                    Expr::ne(min_col.clone(), lit_int.clone()),
                    Expr::ne(max_col.clone(), lit_int.clone()),
                ]),
            ),
        ];

        for (input, expected) in cases {
            let rewritten = as_data_skipping_predicate(&input, false).unwrap();
            assert_eq!(rewritten, expected, "input was {input}")
        }
    }
}

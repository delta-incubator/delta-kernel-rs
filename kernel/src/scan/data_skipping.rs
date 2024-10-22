use std::borrow::Cow;
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

/// Get the expression that checks IS [NOT] NULL for a column with tight bounds. A column may
/// satisfy IS NULL if `nullCount != 0`, and may satisfy IS NOT NULL if `nullCount !=
/// numRecords`. Note that a the bounds are tight unless `tightBounds` is neither TRUE nor NULL.
fn get_tight_is_null_expr(null_col: String, inverted: bool) -> Expr {
    let sentinel_value = if inverted {
        Expr::column("numRecords")
    } else {
        Expr::literal(0i64)
    };
    Expr::and(
        Expr::distinct(Expr::column("tightBounds"), false),
        Expr::ne(Expr::column(null_col), sentinel_value),
    )
}

/// Get the expression that checks if a col could be null, assuming tight_bounds = false. In this
/// case, we can only check whether the WHOLE column is null or not, by checking if the number of
/// records is equal to the null count. Any other values of nullCount must be ignored (except 0,
/// which doesn't help us).
fn get_wide_is_null_expr(null_col: String, inverted: bool) -> Expr {
    let op = if inverted {
        BinaryOperator::NotEqual
    } else {
        BinaryOperator::Equal
    };
    Expr::and(
        Expr::eq(Expr::column("tightBounds"), false),
        Expr::binary(op, Expr::column("numRecords"), Expr::column(null_col)),
    )
}

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
    use BinaryOperator::*;
    use Expr::*;
    use UnaryOperator::*;

    match expr {
        Literal(Scalar::Boolean(value)) => Some(Expr::literal(*value != inverted)),
        Literal(_) => None, // Non-boolean literals unsupported
        // The expression <col> is equivalent to <col> != FALSE. Resembling NotEqual case below.
        Column(col) => Some(Expr::or(
            Expr::ne(Column(format!("minValues.{}", col)), inverted),
            Expr::ne(Column(format!("maxValues.{}", col)), inverted),
        )),
        // push down Not by inverting everything below it
        UnaryOperation { op: Not, expr } => as_data_skipping_predicate(expr, !inverted),
        UnaryOperation { op: IsNull, expr } => {
            // to check if a column could have a null, we need two different checks, to see if
            // the bounds are tight and then to actually do the check
            let Column(col) = expr.as_ref() else {
                return None;
            };
            let null_col = format!("nullCount.{col}");
            Some(Expr::or(
                get_tight_is_null_expr(null_col.clone(), inverted),
                get_wide_is_null_expr(null_col, inverted),
            ))
        }
        BinaryOperation { op, left, right } => {
            // Reduce the number of cases by forcing column on left and literal on right.
            let (op, col, val) = match (left.as_ref(), right.as_ref()) {
                (Column(col), Literal(val)) => (*op, col, val),
                (Literal(val), Column(col)) => (op.commute()?, col, val),
                _ => return None, // unsupported combination of operands
            };

            // Min/Max stats don't allow us to push inversion down into the comparison. Instead, we
            // invert the comparison itself when needed and compute normally after that.
            let op = match inverted {
                true => op.invert()?,
                false => op,
            };

            let stats_col = match op {
                LessThan | LessThanOrEqual => "minValues",
                GreaterThan | GreaterThanOrEqual => "maxValues",
                // Column could compare equal if its min/max values bracket the literal.
                Equal => {
                    return Some(Expr::and(
                        Expr::le(Column(format!("minValues.{}", col)), val.clone()),
                        Expr::ge(Column(format!("maxValues.{}", col)), val.clone()),
                    ))
                }
                // Column could compare not-equal if min and/or max value differs from the literal.
                NotEqual => {
                    return Some(Expr::or(
                        Expr::ne(Column(format!("minValues.{}", col)), val.clone()),
                        Expr::ne(Column(format!("maxValues.{}", col)), val.clone()),
                    ))
                }
                _ => return None, // unsupported operation
            };
            Some(Expr::binary(
                op,
                Column(format!("{}.{}", stats_col, col)),
                val.clone(),
            ))
        }
        VariadicOperation { op, exprs } => {
            // Invert the operator (here) and also invert its arguments (below).
            let op = match inverted {
                true => op.invert(),
                false => *op,
            };
            let exprs = exprs
                .iter()
                .map(|e| as_data_skipping_predicate(e, inverted));
            let exprs: Vec<_> = match op {
                VariadicOperator::And => exprs.flatten().collect(),
                VariadicOperator::Or => exprs.collect::<Option<_>>()?,
            };
            Some(Expr::variadic(op, exprs))
        }
        Struct(_) => None, // Unsupported
    }
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
            assert_eq!(rewritten, expected)
        }
    }
}

//! An implementation of parquet row group skipping using data skipping predicates over footer stats.
use crate::engine::parquet_stats_skipping::{col_name_to_path, ParquetStatsSkippingFilter};
use crate::expressions::{Expression, Scalar};
use crate::schema::{DataType, PrimitiveType};
use parquet::arrow::arrow_reader::ArrowReaderBuilder;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;
use parquet::schema::types::{ColumnDescPtr, ColumnPath};
use std::collections::{HashMap, HashSet};

/// Given an [`ArrowReaderBuilder`] and predicate [`Expression`], use parquet footer stats to filter
/// out any row group that provably contains no rows which satisfy the predicate.
pub fn filter_row_groups<T>(
    reader: ArrowReaderBuilder<T>,
    filter: &Expression,
) -> ArrowReaderBuilder<T> {
    let indices = reader
        .metadata()
        .row_groups()
        .iter()
        .enumerate()
        .filter_map(|(index, row_group)| RowGroupFilter::apply(filter, row_group).then_some(index))
        .collect();
    reader.with_row_groups(indices)
}

/// A ParquetStatsSkippingFilter for row group skipping. It obtains stats from a parquet
/// [`RowGroupMetaData`] and pre-computes the mapping of each referenced column path to its
/// corresponding field index, for O(1) stats lookups.
struct RowGroupFilter<'a> {
    row_group: &'a RowGroupMetaData,
    field_indices: HashMap<ColumnPath, usize>,
}

impl<'a> RowGroupFilter<'a> {
    /// Applies a filtering expression to a row group. Return value false means to skip it.
    fn apply(filter: &Expression, row_group: &'a RowGroupMetaData) -> bool {
        let field_indices = compute_field_indices(row_group.schema_descr().columns(), filter);
        let result = Self {
            row_group,
            field_indices,
        }
        .apply_sql_where(filter);
        !matches!(result, Some(false))
    }

    fn get_stats(&self, col: &ColumnPath) -> Option<&Statistics> {
        let field_index = self.field_indices.get(col)?;
        self.row_group.column(*field_index).statistics()
    }
}

impl<'a> ParquetStatsSkippingFilter for RowGroupFilter<'a> {
    // Extracts a stat value, converting from its physical type to the requested logical type.
    //
    // NOTE: This code is highly redundant with [`get_min_stat_value`], but parquet
    // ValueStatistics<T> requires T to impl a private trait, so we can't factor out any kind of
    // helper method. And macros are hard enough to read that it's not worth defining one.
    fn get_min_stat_value(&self, col: &ColumnPath, data_type: &DataType) -> Option<Scalar> {
        use PrimitiveType::*;
        let value = match (data_type.as_primitive_opt()?, self.get_stats(col)?) {
            (String, Statistics::ByteArray(s)) => s.min_opt()?.as_utf8().ok()?.into(),
            (String, Statistics::FixedLenByteArray(s)) => s.min_opt()?.as_utf8().ok()?.into(),
            (String, _) => None?,
            (Long, Statistics::Int64(s)) => s.min_opt()?.into(),
            (Long, Statistics::Int32(s)) => (*s.min_opt()? as i64).into(),
            (Long, _) => None?,
            (Integer, Statistics::Int32(s)) => s.min_opt()?.into(),
            (Integer, _) => None?,
            (Short, Statistics::Int32(s)) => (*s.min_opt()? as i16).into(),
            (Short, _) => None?,
            (Byte, Statistics::Int32(s)) => (*s.min_opt()? as i8).into(),
            (Byte, _) => None?,
            (Float, Statistics::Float(s)) => s.min_opt()?.into(),
            (Float, _) => None?,
            (Double, Statistics::Double(s)) => s.min_opt()?.into(),
            (Double, _) => None?,
            (Boolean, Statistics::Boolean(s)) => s.min_opt()?.into(),
            (Boolean, _) => None?,
            (Binary, Statistics::ByteArray(s)) => s.min_opt()?.data().into(),
            (Binary, Statistics::FixedLenByteArray(s)) => s.min_opt()?.data().into(),
            (Binary, _) => None?,
            (Date, Statistics::Int32(s)) => Scalar::Date(*s.min_opt()?),
            (Date, _) => None?,
            (Timestamp, Statistics::Int64(s)) => Scalar::Timestamp(*s.min_opt()?),
            (Timestamp, _) => None?, // TODO: Int96 timestamps
            (TimestampNtz, Statistics::Int64(s)) => Scalar::TimestampNtz(*s.min_opt()?),
            (TimestampNtz, _) => None?, // TODO: Int96 timestamps
            (Decimal(..), _) => None?,  // TODO: Decimal (Int32, Int64, FixedLenByteArray)
        };
        Some(value)
    }

    fn get_max_stat_value(&self, col: &ColumnPath, data_type: &DataType) -> Option<Scalar> {
        use PrimitiveType::*;
        let value = match (data_type.as_primitive_opt()?, self.get_stats(col)?) {
            (String, Statistics::ByteArray(s)) => s.max_opt()?.as_utf8().ok()?.into(),
            (String, Statistics::FixedLenByteArray(s)) => s.max_opt()?.as_utf8().ok()?.into(),
            (String, _) => None?,
            (Long, Statistics::Int64(s)) => s.max_opt()?.into(),
            (Long, Statistics::Int32(s)) => (*s.max_opt()? as i64).into(),
            (Long, _) => None?,
            (Integer, Statistics::Int32(s)) => s.max_opt()?.into(),
            (Integer, _) => None?,
            (Short, Statistics::Int32(s)) => (*s.max_opt()? as i16).into(),
            (Short, _) => None?,
            (Byte, Statistics::Int32(s)) => (*s.max_opt()? as i8).into(),
            (Byte, _) => None?,
            (Float, Statistics::Float(s)) => s.max_opt()?.into(),
            (Float, _) => None?,
            (Double, Statistics::Double(s)) => s.max_opt()?.into(),
            (Double, _) => None?,
            (Boolean, Statistics::Boolean(s)) => s.max_opt()?.into(),
            (Boolean, _) => None?,
            (Binary, Statistics::ByteArray(s)) => s.max_opt()?.data().into(),
            (Binary, Statistics::FixedLenByteArray(s)) => s.max_opt()?.data().into(),
            (Binary, _) => None?,
            (Date, Statistics::Int32(s)) => Scalar::Date(*s.max_opt()?),
            (Date, _) => None?,
            (Timestamp, Statistics::Int64(s)) => Scalar::Timestamp(*s.max_opt()?),
            (Timestamp, _) => None?, // TODO: Int96 timestamps
            (TimestampNtz, Statistics::Int64(s)) => Scalar::TimestampNtz(*s.max_opt()?),
            (TimestampNtz, _) => None?, // TODO: Int96 timestamps
            (Decimal(..), _) => None?,  // TODO: Decimal (Int32, Int64, FixedLenByteArray)
        };
        Some(value)
    }

    // Parquet nullcount stats always have the same type (u64), so we can directly return the value
    // instead of wrapping it in a Scalar. We can safely cast it from u64 to i64, because the
    // nullcount can never be larger than the rowcount, and the parquet rowcount stat is i64.
    fn get_nullcount_stat_value(&self, col: &ColumnPath) -> Option<i64> {
        Some(self.get_stats(col)?.null_count_opt()? as i64)
    }

    fn get_rowcount_stat_value(&self) -> i64 {
        self.row_group.num_rows()
    }
}

/// Given a filter expression of interest and a set of parquet column descriptors, build a column ->
/// index mapping for columns the expression references. This ensures O(1) lookup times, for an
/// overall O(n) cost to evaluate an expression tree with n nodes.
pub(crate) fn compute_field_indices(
    fields: &[ColumnDescPtr],
    expression: &Expression,
) -> HashMap<ColumnPath, usize> {
    fn recurse(expression: &Expression, columns: &mut HashSet<ColumnPath>) {
        match expression {
            Expression::Literal(_) => {}
            Expression::Column(name) => {
                columns.insert(col_name_to_path(name));
            }
            Expression::Struct(fields) => {
                for field in fields {
                    recurse(field, columns);
                }
            }
            Expression::UnaryOperation { expr, .. } => recurse(expr, columns),
            Expression::BinaryOperation { left, right, .. } => {
                recurse(left, columns);
                recurse(right, columns);
            }
            Expression::VariadicOperation { exprs, .. } => {
                for expr in exprs {
                    recurse(expr, columns);
                }
            }
        }
    }

    // Build up a set of requested column paths, then take each found path as the corresponding map
    // key (avoids unnecessary cloning).
    //
    // NOTE: If a requested column was not available, it is silently ignored.
    let mut requested_columns = HashSet::new();
    recurse(expression, &mut requested_columns);
    fields
        .iter()
        .enumerate()
        .filter_map(|(i, f)| requested_columns.take(f.path()).map(|path| (path, i)))
        .collect()
}

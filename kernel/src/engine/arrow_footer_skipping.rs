//! An implementation of parquet row group skipping using data skipping predicates.
use crate::expressions::{BinaryOperator, Expression, Scalar, UnaryOperator, VariadicOperator};
use crate::schema::{DataType, PrimitiveType};
use parquet::arrow::arrow_reader::ArrowReaderBuilder;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;
use parquet::schema::types::{ColumnDescPtr, ColumnPath};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

pub fn filter_row_groups<T>(
    reader: ArrowReaderBuilder<T>,
    filter: &Expression,
) -> ArrowReaderBuilder<T> {
    let indices = reader
        .metadata()
        .row_groups()
        .iter()
        .enumerate()
        .filter_map(|(index, row_group)| {
            // We can only skip a row group if the filter is false (true/null means keep)
            let keep = !matches!(RowGroupFilter::apply(filter, row_group), Some(false));
            keep.then(|| index)
        })
        .collect();
    reader.with_row_groups(indices)
}

struct RowGroupFilter<'a> {
    row_group: &'a RowGroupMetaData,
    field_indices: HashMap<ColumnPath, usize>,
}

impl<'a> RowGroupFilter<'a> {
    fn apply(filter: &Expression, row_group: &'a RowGroupMetaData) -> Option<bool> {
        let field_indices = compute_field_indices(row_group.schema_descr().columns(), filter);
        Self {
            row_group,
            field_indices,
        }.apply_expr(filter, false)
    }

    fn apply_expr(&self, expression: &Expression, inverted: bool) -> Option<bool> {
        use Expression::*;
        match expression {
            VariadicOperation { op, exprs } => self.apply_variadic(op, exprs, inverted),
            BinaryOperation { op, left, right } => self.apply_binary(op, left, right, inverted),
            UnaryOperation { op, expr } => self.apply_unary(op, expr, inverted),
            // How to handle a leaf expression depends on the parent expression that embeds it
            Literal(_) | Column(_) => None,
            // We don't support skipping over complex types
            Struct(_) => None,
        }
    }

    fn apply_variadic(
        &self,
        op: &VariadicOperator,
        exprs: &[Expression],
        inverted: bool,
    ) -> Option<bool> {
        let exprs: Vec<_> = exprs
            .iter()
            .map(|expr| self.apply_expr(expr, inverted))
            .collect();

        // With AND (OR), any FALSE (TRUE) input forces FALSE (TRUE) output.  If there was no
        // dominating value, then any NULL input forces NULL output.  Otherwise, return the
        // non-dominant value. Inverting the operation also inverts the dominant value.
        let dominator = match op {
            VariadicOperator::And => inverted,
            VariadicOperator::Or => !inverted,
        };
        if exprs.iter().any(|v| v.is_some_and(|v| v == dominator)) {
            Some(dominator)
        } else if exprs.iter().any(|e| e.is_none()) {
            None
        } else {
            Some(!dominator)
        }
    }

    fn apply_binary(
        &self,
        op: &BinaryOperator,
        left: &Expression,
        right: &Expression,
        inverted: bool,
    ) -> Option<bool> {
        use BinaryOperator::*;
        use Expression::{Column, Literal};

        let (op, col, val) = match (left, right) {
            (Column(col), Literal(val)) => (*op, col, val),
            (Literal(val), Column(col)) => (op.commute()?, col, val),
            _ => None?, // unsupported combination of operands
        };
        let col = col_name_to_path(col);
        let skipping_eq = |inverted| -> Option<bool> {
            let below_lo = self.partial_cmp_min_stat(&col, val, Ordering::Less, inverted)?;
            let above_hi = self.partial_cmp_max_stat(&col, val, Ordering::Greater, inverted)?;
            let out_of_bounds = below_lo || above_hi;
            Some(out_of_bounds == inverted)
        };
        match op {
            Equal => skipping_eq(inverted),
            NotEqual => skipping_eq(!inverted),
            LessThan => self.partial_cmp_min_stat(&col, val, Ordering::Less, inverted),
            LessThanOrEqual => self.partial_cmp_min_stat(&col, val, Ordering::Greater, !inverted),
            GreaterThan => self.partial_cmp_max_stat(&col, val, Ordering::Greater, inverted),
            GreaterThanOrEqual => self.partial_cmp_max_stat(&col, val, Ordering::Less, !inverted),
            _ => None, // unsupported operation
        }
    }

    fn apply_unary(&self, op: &UnaryOperator, expr: &Expression, inverted: bool) -> Option<bool> {
        match op {
            UnaryOperator::Not => self.apply_expr(expr, !inverted),
            UnaryOperator::IsNull => {
                if let Expression::Column(col) = expr {
                    let expect = if inverted {
                        // IS NOT NULL => null count equals zero
                        0
                    } else {
                        // IS NULL => null count equals row count
                        self.get_rowcount_stat_value()
                    };
                    let col = col_name_to_path(col);
                    Some(self.get_nullcount_stat_value(&col)? == expect)
                } else {
                    None
                }
            }
        }
    }

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
            (TimestampNtz, _) => None?,  // TODO: Int96 timestamps
            (Decimal(_, _), _) => None?, // TODO: Decimal (Int32, Int64, FixedLenByteArray)
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
            (TimestampNtz, _) => None?,  // TODO: Int96 timestamps
            (Decimal(_, _), _) => None?, // TODO: Decimal (Int32, Int64, FixedLenByteArray)
        };
        Some(value)
    }

    fn get_nullcount_stat_value(&self, col: &ColumnPath) -> Option<i64> {
        // Null stats always have the same type (u64), so we can handle them directly. Further,
        // the rowcount stat is i64 so we can safely cast this to i64 to match
        Some(self.get_stats(col)?.null_count_opt()? as i64)
    }

    fn get_rowcount_stat_value(&self) -> i64 {
        self.row_group.num_rows()
    }

    fn get_stats(&self, col: &ColumnPath) -> Option<&Statistics> {
        let field_index = self.field_indices.get(col)?;
        self.row_group.column(*field_index).statistics()
    }
}

fn partial_cmp_scalars(a: &Scalar, b: &Scalar, ord: Ordering, inverted: bool) -> Option<bool> {
    let result = a.partial_cmp(b)? == ord;
    Some(result != inverted)
}

fn col_name_to_path(col: &str) -> ColumnPath {
    // TODO: properly handle nested columns
    // https://github.com/delta-incubator/delta-kernel-rs/issues/86
    ColumnPath::new(col.split('.').map(|s| s.to_string()).collect())
}

fn compute_field_indices(
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

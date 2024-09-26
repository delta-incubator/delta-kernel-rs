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
            keep.then_some(index)
        })
        .collect();
    reader.with_row_groups(indices)
}

struct RowGroupFilter<'a> {
    row_group: &'a RowGroupMetaData,
    field_indices: HashMap<ColumnPath, usize>,
}

impl BinaryOperator {
    fn try_invert_if(&self, invert: bool) -> Option<Self> {
        match invert {
            true => self.invert(),
            false => Some(*self),
        }
    }
}

// TODO: Unit tests can implement this trait in order to easily validate the skipping logic
pub(crate) trait ParquetFooterSkippingFilter {
    fn get_min_stat_value(&self, col: &ColumnPath, data_type: &DataType) -> Option<Scalar>;

    fn get_max_stat_value(&self, col: &ColumnPath, data_type: &DataType) -> Option<Scalar>;

    fn get_nullcount_stat_value(&self, col: &ColumnPath) -> Option<i64>;

    fn get_rowcount_stat_value(&self) -> i64;

    fn apply_expr(&self, expression: &Expression, inverted: bool) -> Option<bool> {
        use Expression::*;
        match expression {
            VariadicOperation { op, exprs } => self.apply_variadic(*op, exprs, inverted),
            BinaryOperation { op, left, right } => self.apply_binary(*op, left, right, inverted),
            UnaryOperation { op, expr } => self.apply_unary(*op, expr, inverted),
            Literal(value) => Self::apply_scalar(value, inverted),
            Column(col) => self.apply_column(col, inverted),
            // We don't support skipping over complex types
            Struct(_) => None,
        }
    }

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

        // Evaluate the input expressions. tracking whether we've seen any NULL result. Stop
        // immediately (short circuit) if we see a dominant value.
        let result = exprs.iter().try_fold(false, |found_null, expr| {
            match self.apply_expr(expr, inverted) {
                Some(v) if v == dominator => None,
                Some(_) => Some(found_null),
                None => Some(true),
            }
        });

        match result {
            None => Some(dominator),
            Some(false) => Some(!dominator),
            Some(true) => None,
        }
    }

    fn apply_binary(
        &self,
        op: BinaryOperator,
        left: &Expression,
        right: &Expression,
        inverted: bool,
    ) -> Option<bool> {
        use BinaryOperator::*;
        use Expression::{Column, Literal};

        let op = op.try_invert_if(inverted)?;

        // NOTE: We rely on the literal values to provide logical type hints. That means we cannot
        // perform column-column comparisons, because we cannot infer the logical type to use.
        let (op, col, val) = match (left, right) {
            (Column(col), Literal(val)) => (op, col, val),
            (Literal(val), Column(col)) => (op.commute()?, col, val),
            (Literal(a), Literal(b)) => return Self::apply_binary_scalars(op, a, b),
            _ => None?, // unsupported combination of operands
        };
        let col = col_name_to_path(col);
        let skipping_eq = |inverted| -> Option<bool> {
            // Given `col == val`:
            // skip if `val` cannot equal _any_ value in [min, max], implies
            // skip if `NOT(val BETWEEN min AND max)` implies
            // skip if `NOT(min <= val AND val <= max)` implies
            // skip if `min > val OR max < val`
            //
            // Given `col != val`:
            // skip if `val` equals _every_ value in [min, max], implies
            // skip if `val == min AND val == max` implies
            // skip if `val <= min AND min <= val AND val <= max AND max <= val` implies
            // skip if `val <= min AND max <= val` implies
            // keep if `NOT(val <= min AND max <= val)` implies
            // keep if `val > min OR max > val` implies
            // keep if `min < val OR max > val`
            let (min_ord, max_ord) = match inverted {
                false => (Ordering::Greater, Ordering::Less),
                true => (Ordering::Less, Ordering::Greater),
            };
            let skip_lo = self.partial_cmp_min_stat(&col, val, min_ord, false)?;
            let skip_hi = self.partial_cmp_max_stat(&col, val, max_ord, false)?;
            let skip = skip_lo || skip_hi;
            println!("skip_lo: {skip_lo}, skip_hi: {skip_hi}");
            Some(skip == inverted)
        };
        match op {
            Equal => skipping_eq(false),
            NotEqual => skipping_eq(true),
            // Given `col < val`:
            // Skip if `val` is not greater than _all_ values in [min, max], implies
            // Skip if `val <= min AND val <= max` implies
            // Skip if `val <= min` implies
            // Keep if `NOT(val <= min)` implies
            // Keep if `val > min` implies
            // Keep if `min < val`
            LessThan => self.partial_cmp_min_stat(&col, val, Ordering::Less, false),
            LessThanOrEqual => self.partial_cmp_min_stat(&col, val, Ordering::Greater, true),
            GreaterThan => self.partial_cmp_max_stat(&col, val, Ordering::Greater, false),
            // Given `col >= val`:
            // Skip if `val is greater than _every_ value in [min, max], implies
            // Skip if `val > min AND val > max` implies
            // Skip if `val > max` implies
            // Keep if `NOT(val > max)` implies
            // Keep if `val <= max` implies
            // Keep if `max >= val`
            GreaterThanOrEqual => self.partial_cmp_max_stat(&col, val, Ordering::Less, true),
            _ => None, // unsupported operation
        }
    }

    // Support e.g. `10 == 20 OR ...`
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

    fn apply_unary(&self, op: UnaryOperator, expr: &Expression, inverted: bool) -> Option<bool> {
        match op {
            UnaryOperator::Not => self.apply_expr(expr, !inverted),
            UnaryOperator::IsNull => match expr {
                Expression::Column(col) => {
                    let skip = match inverted {
                        // IS NOT NULL - only skip if all-null
                        true => self.get_rowcount_stat_value(),
                        // IS NULL - only skip if no-null
                        false => 0,
                    };
                    let col = col_name_to_path(col);
                    Some(self.get_nullcount_stat_value(&col)? != skip)
                }
                _ => None,
            },
        }
    }

    // handle e.g. `flag OR ...`
    fn apply_column(&self, col: &str, inverted: bool) -> Option<bool> {
        let col = col_name_to_path(col);
        let boolean_stat = |get_stat_value: &dyn Fn(_, _, _) -> _| {
            match get_stat_value(self, &col, &DataType::BOOLEAN) {
                Some(Scalar::Boolean(value)) => Some(value),
                _ => None,
            }
        };
        let min = boolean_stat(&Self::get_min_stat_value)?;
        let max = boolean_stat(&Self::get_max_stat_value)?;
        Some(min != inverted || max != inverted)
    }

    // handle e.g. `FALSE OR ...`
    fn apply_scalar(value: &Scalar, inverted: bool) -> Option<bool> {
        match value {
            Scalar::Boolean(value) => Some(*value != inverted),
            _ => None,
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
}

impl<'a> RowGroupFilter<'a> {
    fn apply(filter: &Expression, row_group: &'a RowGroupMetaData) -> Option<bool> {
        let field_indices = compute_field_indices(row_group.schema_descr().columns(), filter);
        Self {
            row_group,
            field_indices,
        }
        .apply_expr(filter, false)
    }

    fn get_stats(&self, col: &ColumnPath) -> Option<&Statistics> {
        let field_index = self.field_indices.get(col)?;
        self.row_group.column(*field_index).statistics()
    }
}

impl<'a> ParquetFooterSkippingFilter for RowGroupFilter<'a> {
    // Extracts a stat value, converting from its physical to the requested logical type.
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

    fn get_nullcount_stat_value(&self, col: &ColumnPath) -> Option<i64> {
        // Null stats always have the same type (u64), so we can handle them directly. Further,
        // the rowcount stat is i64 so we can safely cast this to i64 to match
        Some(self.get_stats(col)?.null_count_opt()? as i64)
    }

    fn get_rowcount_stat_value(&self) -> i64 {
        self.row_group.num_rows()
    }
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

pub(crate) fn col_name_to_path(col: &str) -> ColumnPath {
    // TODO: properly handle nested columns
    // https://github.com/delta-incubator/delta-kernel-rs/issues/86
    ColumnPath::new(col.split('.').map(|s| s.to_string()).collect())
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{ArrayData, StructData};
    use crate::schema::ArrayType;
    use crate::DataType;

    struct UnimplementedTestFilter;
    impl ParquetFooterSkippingFilter for UnimplementedTestFilter {
        fn get_min_stat_value(&self, col: &ColumnPath, data_type: &DataType) -> Option<Scalar> {
            unimplemented!()
        }

        fn get_max_stat_value(&self, col: &ColumnPath, data_type: &DataType) -> Option<Scalar> {
            unimplemented!()
        }

        fn get_nullcount_stat_value(&self, col: &ColumnPath) -> Option<i64> {
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
    impl ParquetFooterSkippingFilter for MinMaxTestFilter {
        fn get_min_stat_value(&self, col: &ColumnPath, data_type: &DataType) -> Option<Scalar> {
            Self::get_stat_value(&self.min, data_type)
        }

        fn get_max_stat_value(&self, col: &ColumnPath, data_type: &DataType) -> Option<Scalar> {
            Self::get_stat_value(&self.max, data_type)
        }

        fn get_nullcount_stat_value(&self, col: &ColumnPath) -> Option<i64> {
            unimplemented!()
        }

        fn get_rowcount_stat_value(&self) -> i64 {
            unimplemented!()
        }
    }

    #[test]
    fn test_binary_eq_ne() {
        use BinaryOperator::*;
        use Scalar::{Boolean, Long};

        const LO: Scalar = Long(1);
        const MID: Scalar = Long(10);
        const HI: Scalar = Long(100);
        let col = &Expression::column("x");

        for inverted in [false, true] {
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
        use Scalar::{Boolean, Long};

        const LO: Scalar = Long(1);
        const MID: Scalar = Long(10);
        const HI: Scalar = Long(100);
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
}

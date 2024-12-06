use std::collections::HashMap;
use std::iter;

use crate::expressions::{column_expr, Scalar};
use crate::scan::{parse_partition_value, ColumnType};
use crate::schema::{ColumnName, DataType, SchemaRef, StructField, StructType};
use crate::{DeltaResult, Error, Expression};

use super::scan_file::{CDFScanFile, CDFScanFileType};

pub(crate) fn get_generated_columns() -> DeltaResult<HashMap<String, Expression>> {
    // Both in-commit timestamps and file metadata are in milliseconds
    //
    // See:
    // [`FileMeta`]
    // [In-Commit Timestamps] : https://github.com/delta-io/delta/blob/master/PROTOCOL.md#writer-requirements-for-in-commit-timestampsa
    let timestamp = Scalar::timestamp_from_millis(self.scan_file.timestamp)?;
    let commit_version: i64 = self
        .scan_file
        .commit_version
        .try_into()
        .map_err(Error::generic)?;
    let cols = ["_change_type", "_commit_version", "_commit_timestamp"];
    let expressions = match self.scan_file.tpe {
        CDFScanFileType::Cdc => [
            column_expr!("_change_type"),
            Expression::literal(commit_version),
            timestamp.into(),
        ],
        CDFScanFileType::Add => [
            "insert".into(),
            Expression::literal(commit_version),
            timestamp.into(),
        ],

        CDFScanFileType::Remove => [
            "delete".into(),
            Expression::literal(commit_version),
            timestamp.into(),
        ],
    };
    let generated_columns: HashMap<String, Expression> = cols
        .iter()
        .map(ToString::to_string)
        .zip(expressions)
        .collect();
    Ok(generated_columns)
}
fn get_expression(scan_file: &CDFScanFile) -> DeltaResult<Expression> {
    let mut generated_columns = get_generated_columns()?;
    let all_fields = global_scan_state
        .all_fields
        .iter()
        .map(|field| match field {
            ColumnType::Partition(field_idx) => {
                let field = self
                    .global_scan_state
                    .logical_schema
                    .fields
                    .get_index(*field_idx);
                let Some((_, field)) = field else {
                    return Err(Error::generic(
                        "logical schema did not contain expected field, can't transform data",
                    ));
                };
                let name = field.physical_name(global_scan_state.column_mapping_mode)?;
                let value_expression =
                    parse_partition_value(scan_file.partition_values.get(name), field.data_type())?;
                Ok(value_expression.into())
            }
            ColumnType::Selected(field_name) =>
            // We take the expression from the map
            {
                Ok(generated_columns
                    .remove(&field_name)
                    .unwrap_or_else(|| ColumnName::new([field_name]).into()))
            }
        })
        .try_collect()?;
    Ok(Expression::Struct(all_fields))
}
fn read_schema() -> SchemaRef {
    if scan_file.tpe == CDFScanFileType::Cdc {
        let fields = self
            .global_scan_state
            .read_schema
            .fields()
            .cloned()
            .chain(iter::once(StructField::new(
                "_change_type",
                DataType::STRING,
                false,
            )));
        StructType::new(fields).into()
    } else {
        global_scan_state.read_schema.clone()
    }
}

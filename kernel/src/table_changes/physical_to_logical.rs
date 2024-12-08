use std::collections::HashMap;
use std::iter;

use itertools::Itertools;

use crate::expressions::{column_expr, Scalar};
use crate::scan::state::GlobalScanState;
use crate::scan::{parse_partition_value, ColumnType};
use crate::schema::{ColumnName, DataType, SchemaRef, StructField, StructType};
use crate::{DeltaResult, Error, Expression};

use super::scan_file::{CdfScanFile, CdfScanFileType};

#[allow(unused)]
fn get_generated_columns(scan_file: &CdfScanFile) -> DeltaResult<HashMap<String, Expression>> {
    let timestamp = Scalar::timestamp_from_millis(scan_file.commit_timestamp)?;
    let version = scan_file.commit_version;
    let change_type: Expression = match scan_file.scan_type {
        CdfScanFileType::Cdc => column_expr!("_change_type"),
        CdfScanFileType::Add => "insert".into(),

        CdfScanFileType::Remove => "delete".into(),
    };
    let expressions = [
        ("_change_type".to_string(), change_type),
        ("_commit_version".to_string(), Expression::literal(version)),
        ("_commit_timestamp".to_string(), timestamp.into()),
    ];
    Ok(expressions.into_iter().collect())
}

#[allow(unused)]
fn get_expression(
    scan_file: &CdfScanFile,
    global_state: GlobalScanState,
    all_fields: &[ColumnType],
) -> DeltaResult<Expression> {
    let mut generated_columns = get_generated_columns(scan_file)?;
    let all_fields = all_fields
        .iter()
        .map(|field| match field {
            ColumnType::Partition(field_idx) => {
                let field = global_state.logical_schema.fields.get_index(*field_idx);
                let Some((_, field)) = field else {
                    return Err(Error::generic(
                        "logical schema did not contain expected field, can't transform data",
                    ));
                };
                let name = field.physical_name();
                let value_expression =
                    parse_partition_value(scan_file.partition_values.get(name), field.data_type())?;
                Ok(value_expression.into())
            }
            ColumnType::Selected(field_name) => {
                // Remove to take ownership
                let generated_column = generated_columns.remove(field_name);
                Ok(generated_column.unwrap_or_else(|| ColumnName::new([field_name]).into()))
            }
        })
        .try_collect()?;
    Ok(Expression::Struct(all_fields))
}
#[allow(unused)]
fn read_schema(scan_file: &CdfScanFile, global_scan_state: GlobalScanState) -> SchemaRef {
    if scan_file.scan_type == CdfScanFileType::Cdc {
        let change_type = StructField::new("_change_type", DataType::STRING, false);
        let fields = global_scan_state
            .read_schema
            .fields()
            .cloned()
            .chain(iter::once(change_type));
        StructType::new(fields).into()
    } else {
        global_scan_state.read_schema.clone()
    }
}

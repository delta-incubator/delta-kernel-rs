use std::{collections::HashMap, sync::Arc};

use itertools::Itertools;

use crate::{
    expressions::{column_expr, ColumnName, Scalar},
    scan::{parse_partition_value, state::GlobalScanState, ColumnType},
    schema::StructType,
    DeltaResult, Engine, EngineData, Error, Expression,
};

use super::{state::ScanFileType, CDF_GENERATED_COLUMNS};

// We have this function because `execute` can save `all_fields` and `have_partition_cols` in the
// scan, and then reuse them for each batch transform
#[allow(clippy::too_many_arguments)] // TEMPORARY
pub(crate) fn transform_to_logical_internal(
    engine: &dyn Engine,
    data: Box<dyn EngineData>,
    global_state: &GlobalScanState,
    partition_values: &std::collections::HashMap<String, String>,
    all_fields: &[ColumnType],
    _have_partition_cols: bool,
    generated_columns: &HashMap<String, Expression>,
    read_schema: Arc<StructType>,
) -> DeltaResult<Box<dyn EngineData>> {
    // need to add back partition cols and/or fix-up mapped columns
    let all_fields: Vec<Expression> = all_fields
        .iter()
        .map(|field| match field {
            ColumnType::Partition(field_idx) => {
                let field = global_state.logical_schema.fields.get_index(*field_idx);
                let Some((_, field)) = field else {
                    return Err(Error::generic(
                        "logical schema did not contain expected field, can't transform data",
                    ));
                };
                let name = field.physical_name(global_state.column_mapping_mode)?;
                let value_expression =
                    parse_partition_value(partition_values.get(name), field.data_type())?;
                Ok(value_expression.into())
            }
            ColumnType::Selected(field_name) => Ok(ColumnName::new([field_name]).into()),
            ColumnType::InsertedColumn(field_idx) => {
                let field = global_state.logical_schema.fields.get_index(*field_idx);
                let Some((_, field)) = field else {
                    return Err(Error::generic(
                        "logical schema did not contain expected field, can't transform data",
                    ));
                };
                let Some(expr) = generated_columns.get(field.name()) else {
                    return Err(Error::generic(
                        "Got unexpected inserted field , can't transform data",
                    ));
                };
                Ok(expr.clone())
            }
        })
        .try_collect()?;
    let read_expression = Expression::Struct(all_fields);

    let result = engine
        .get_expression_handler()
        .get_evaluator(
            read_schema,
            read_expression,
            global_state.logical_schema.clone().into(),
        )
        .evaluate(data.as_ref())?;
    Ok(result)
}

pub(crate) fn get_generated_columns(
    timestamp: i64,
    tpe: ScanFileType,
    commit_version: i64,
) -> Result<Arc<HashMap<String, Expression>>, crate::Error> {
    // Both in-commit timestamps and file metadata are in milliseconds
    //
    // See:
    // [`FileMeta`]
    // [In-Commit Timestamps] : https://github.com/delta-io/delta/blob/master/PROTOCOL.md#writer-requirements-for-in-commit-timestampsa
    let timestamp = Scalar::timestamp_from_millis(timestamp)?;
    let expressions = match tpe {
        ScanFileType::Cdc => [
            column_expr!("_change_type"),
            Expression::literal(commit_version),
            timestamp.into(),
        ],
        ScanFileType::Add => [
            "insert".into(),
            Expression::literal(commit_version),
            timestamp.into(),
        ],

        ScanFileType::Remove => [
            "delete".into(),
            Expression::literal(commit_version),
            timestamp.into(),
        ],
    };
    let generated_columns: Arc<HashMap<String, Expression>> = Arc::new(
        CDF_GENERATED_COLUMNS
            .iter()
            .map(ToString::to_string)
            .zip(expressions)
            .collect(),
    );
    Ok(generated_columns)
}

use std::collections::HashMap;

use itertools::Itertools;

use crate::{
    expressions::ColumnName,
    scan::{parse_partition_value, state::GlobalScanState, ColumnType},
    DeltaResult, Engine, EngineData, Error, Expression,
};

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
    mut generated_columns: HashMap<String, Expression>,
) -> DeltaResult<Box<dyn EngineData>> {
    let read_schema = global_state.read_schema.clone();
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
                let Some(expr) = generated_columns.remove(field.name()) else {
                    return Err(Error::generic(
                        "logical schema did not contain expected field, can't transform data",
                    ));
                };
                Ok(expr)
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

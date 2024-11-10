use itertools::Itertools;

use crate::{
    expressions::{ColumnName, Scalar},
    features::ColumnMappingMode,
    scan::{parse_partition_value, ColumnType},
    DeltaResult, Engine, EngineData, Error, Expression,
};

use super::TableChangesGlobalScanState;

// We have this function because `execute` can save `all_fields` and `have_partition_cols` in the
// scan, and then reuse them for each batch transform
pub(crate) fn transform_to_logical_internal(
    engine: &dyn Engine,
    data: Box<dyn EngineData>,
    global_state: &TableChangesGlobalScanState,
    partition_values: &std::collections::HashMap<String, String>,
    all_fields: &[ColumnType],
    have_partition_cols: bool,
) -> DeltaResult<Box<dyn EngineData>> {
    let read_schema = global_state.read_schema.clone();
    println!("Read schemA: {:?}", read_schema);
    // need to add back partition cols and/or fix-up mapped columns
    let mut all_fields: Vec<Expression> = all_fields
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
        })
        .try_collect()?;
    all_fields.extend([
        Expression::literal(42i64),
        Scalar::Timestamp(50).into(),
        "insert".into(),
    ]);
    let read_expression = Expression::Struct(all_fields);
    println!("Final schema: {:?}", global_state.output_schema);
    let result = engine
        .get_expression_handler()
        .get_evaluator(
            read_schema,
            read_expression,
            global_state.output_schema.clone().into(),
        )
        .evaluate(data.as_ref())?;
    Ok(result)
}

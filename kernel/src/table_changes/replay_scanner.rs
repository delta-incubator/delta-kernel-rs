use std::collections::{HashMap, HashSet};

use crate::actions::visitors::{AddVisitor, RemoveVisitor};
use crate::actions::{get_log_schema, Add, Remove, ADD_NAME, REMOVE_NAME};
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine_data::{GetData, TypedGetData};
use crate::expressions::{column_expr, Expression};
use crate::scan::data_skipping::DataSkippingFilter;
use crate::scan::state::DvInfo;
use crate::scan::ScanData;
use crate::table_changes::state::TABLE_CHANGES_SCAN_ROW_SCHEMA;
use crate::{DataVisitor, DeltaResult, EngineData, ExpressionHandler};
use arrow_array::RecordBatch;
use tracing::debug;

use super::state::TABLE_CHANGES_SCAN_ROW_EXPR;
use super::TableChangesScanData;

#[derive(Default)]
pub(crate) struct AddRemoveCdcVisitor {
    pub adds: Vec<(Add, usize)>,
    pub removes: Vec<(Remove, usize)>,
    selection_vector: Option<Vec<bool>>,
}

const ADD_FIELD_COUNT: usize = 15;

impl AddRemoveCdcVisitor {
    pub(crate) fn new(selection_vector: Option<Vec<bool>>) -> Self {
        AddRemoveCdcVisitor {
            selection_vector,
            ..Default::default()
        }
    }
}

impl DataVisitor for AddRemoveCdcVisitor {
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            // Add will have a path at index 0 if it is valid
            if let Some(path) = getters[0].get_opt(i, "add.path")? {
                // Keep the file unless the selection vector is present and is false for this row
                if !self
                    .selection_vector
                    .as_ref()
                    .is_some_and(|selection| !selection[i])
                {
                    self.adds.push((
                        AddVisitor::visit_add(i, path, &getters[..ADD_FIELD_COUNT])?,
                        i,
                    ))
                }
            }
            // Remove will have a path at index 15 if it is valid
            // TODO(nick): Should count the fields in Add to ensure we don't get this wrong if more
            // are added
            // TODO(zach): add a check for selection vector that we never skip a remove
            else if let Some(path) = getters[ADD_FIELD_COUNT].get_opt(i, "remove.path")? {
                let remove_getters = &getters[ADD_FIELD_COUNT..];
                self.removes
                    .push((RemoveVisitor::visit_remove(i, path, remove_getters)?, i));
            }
        }
        Ok(())
    }
}

pub(crate) struct TableChangesLogReplayScanner {
    filter: Option<DataSkippingFilter>,
    pub remove_dvs: HashMap<String, DvInfo>,
}

impl TableChangesLogReplayScanner {
    /// Create a new [`LogReplayScanner`] instance
    pub(crate) fn new(filter: Option<DataSkippingFilter>) -> Self {
        Self {
            filter,
            remove_dvs: Default::default(),
        }
    }

    fn get_add_transform_expr(&self) -> Expression {
        TABLE_CHANGES_SCAN_ROW_EXPR.as_ref().clone()
    }

    pub(crate) fn process_scan_batch(
        &mut self,
        expression_handler: &dyn ExpressionHandler,
        actions: &dyn EngineData,
    ) -> DeltaResult<ScanData> {
        // apply data skipping to get back a selection vector for actions that passed skipping
        // note: None implies all files passed data skipping.
        let filter_vector = self
            .filter
            .as_ref()
            .map(|filter| filter.apply(actions))
            .transpose()?;

        // we start our selection vector based on what was filtered. we will add to this vector
        // below if a file has been removed
        let mut selection_vector = match filter_vector {
            Some(ref filter_vector) => filter_vector.clone(),
            None => vec![false; actions.length()],
        };

        assert_eq!(selection_vector.len(), actions.length());
        let AddRemoveCdcVisitor {
            adds,
            removes,
            selection_vector: _,
        } = self.setup_batch_process(filter_vector, actions)?;
        for (add, index) in adds.into_iter() {
            // Note: each (add.path + add.dv_unique_id()) pair has a
            // unique Add + Remove pair in the log. For example:
            // https://github.com/delta-io/delta/blob/master/spark/src/test/resources/delta/table-with-dv-large/_delta_log/00000000000000000001.json
            selection_vector[index] = true;
            debug!(
                "Including file in scan: ({}, {:?})",
                add.path,
                add.dv_unique_id(),
            );
        }
        for (remove, index) in removes.into_iter() {
            debug!(
                "Including file in scan: ({}, {:?})",
                remove.path,
                remove.dv_unique_id(),
            );
            if let Some(dv) = remove.deletion_vector {
                let dv_info = DvInfo {
                    deletion_vector: Some(dv),
                };
                self.remove_dvs.insert(remove.path.clone(), dv_info);
            }
            selection_vector[index] = true;
        }

        // Downcast actions to arrow_engine data, then print out record batches
        // let record_batch: &RecordBatch = actions
        //     .as_any()
        //     .downcast_ref::<ArrowEngineData>()
        //     .unwrap()
        //     .record_batch();
        // println!("\n\n\n==============================================================================================\nRecord batch: {:?}", record_batch);
        let result = expression_handler
            .get_evaluator(
                get_log_schema().clone(),
                self.get_add_transform_expr(),
                TABLE_CHANGES_SCAN_ROW_SCHEMA.clone().into(),
            )
            .evaluate(actions)?;
        // let record_batch: &RecordBatch = result
        //     .as_any()
        //     .downcast_ref::<ArrowEngineData>()
        //     .unwrap()
        //     .record_batch();
        // println!("\n\n\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nOutput batch: {:?}", record_batch);
        // println!("**************************************************************************************************************");
        println!("evaluation successful...");
        Ok((result, selection_vector))
    }

    // work shared between process_batch and process_scan_batch
    fn setup_batch_process(
        &mut self,
        selection_vector: Option<Vec<bool>>,
        actions: &dyn EngineData,
    ) -> DeltaResult<AddRemoveCdcVisitor> {
        let schema_to_use =
            // NB: We _must_ pass these in the order `ADD_NAME, REMOVE_NAME` as the visitor assumes
            // the Add action comes first. The [`project`] method honors this order, so this works
            // as long as we keep this order here.
            get_log_schema().project(&[ADD_NAME, REMOVE_NAME])?;
        let mut visitor = AddRemoveCdcVisitor::new(selection_vector);
        actions.extract(schema_to_use, &mut visitor)?;

        Ok(visitor)
    }
}

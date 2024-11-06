use std::collections::HashSet;

use tracing::debug;

use crate::actions::visitors::{AddVisitor, RemoveVisitor};
use crate::actions::{get_log_schema, Add, Remove, ADD_NAME, REMOVE_NAME};
use crate::engine_data::{GetData, TypedGetData};
use crate::expressions::{column_expr, Expression};
use crate::scan::data_skipping::DataSkippingFilter;
use crate::scan::log_replay::SCAN_ROW_DATATYPE;
use crate::scan::ScanData;
use crate::{DataVisitor, DeltaResult, EngineData, ExpressionHandler};

#[derive(Default)]
pub(crate) struct CdcVisitor {
    pub adds: Vec<(Add, usize)>,
    pub removes: Vec<Remove>,
    selection_vector: Option<Vec<bool>>,
}

const ADD_FIELD_COUNT: usize = 15;

impl CdcVisitor {
    pub(crate) fn new(selection_vector: Option<Vec<bool>>) -> Self {
        CdcVisitor {
            selection_vector,
            ..Default::default()
        }
    }
}

impl DataVisitor for CdcVisitor {
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
                    .push(RemoveVisitor::visit_remove(i, path, remove_getters)?);
            }
        }
        Ok(())
    }
}

pub(crate) struct CdcLogReplayScanner<'a> {
    filter: Option<&'a DataSkippingFilter>,

    /// A set of (data file path, dv_unique_id) pairs that have been seen thus
    /// far in the log. This is used to filter out files with Remove actions as
    /// well as duplicate entries in the log.
    seen: HashSet<(String, Option<String>)>,
}

impl<'a> CdcLogReplayScanner<'a> {
    /// Create a new [`LogReplayScanner`] instance
    pub(crate) fn new(filter: Option<&'a DataSkippingFilter>) -> Self {
        Self {
            filter,
            seen: Default::default(),
        }
    }

    fn get_add_transform_expr(&self) -> Expression {
        Expression::Struct(vec![
            column_expr!("add.path"),
            column_expr!("add.size"),
            column_expr!("add.modificationTime"),
            column_expr!("add.stats"),
            column_expr!("add.deletionVector"),
            Expression::Struct(vec![column_expr!("add.partitionValues")]),
        ])
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
            .map(|filter| filter.apply(actions))
            .transpose()?;

        // we start our selection vector based on what was filtered. we will add to this vector
        // below if a file has been removed
        let mut selection_vector = match filter_vector {
            Some(ref filter_vector) => filter_vector.clone(),
            None => vec![false; actions.length()],
        };

        assert_eq!(selection_vector.len(), actions.length());
        let adds = self.setup_batch_process(filter_vector, actions)?;

        for (add, index) in adds.into_iter() {
            // Note: each (add.path + add.dv_unique_id()) pair has a
            // unique Add + Remove pair in the log. For example:
            // https://github.com/delta-io/delta/blob/master/spark/src/test/resources/delta/table-with-dv-large/_delta_log/00000000000000000001.json
            if !self.seen.contains(&(add.path.clone(), add.dv_unique_id())) {
                debug!(
                    "Including file in scan: ({}, {:?})",
                    add.path,
                    add.dv_unique_id(),
                );
                // Remember file actions from this batch so we can ignore duplicates
                // as we process batches from older commit and/or checkpoint files. We
                // don't need to track checkpoint batches because they are already the
                // oldest actions and can never replace anything.
                self.seen.insert((add.path.clone(), add.dv_unique_id()));
                selection_vector[index] = true;
            } else {
                debug!("Filtering out Add due to it being removed {}", add.path);
                // we may have a true here because the data-skipping predicate included the file
                selection_vector[index] = false;
            }
        }

        let result = expression_handler
            .get_evaluator(
                get_log_schema().project(&[ADD_NAME])?,
                self.get_add_transform_expr(),
                SCAN_ROW_DATATYPE.clone(),
            )
            .evaluate(actions)?;
        Ok((result, selection_vector))
    }

    // work shared between process_batch and process_scan_batch
    fn setup_batch_process(
        &mut self,
        selection_vector: Option<Vec<bool>>,
        actions: &dyn EngineData,
    ) -> DeltaResult<Vec<(Add, usize)>> {
        let schema_to_use =
            // NB: We _must_ pass these in the order `ADD_NAME, REMOVE_NAME` as the visitor assumes
            // the Add action comes first. The [`project`] method honors this order, so this works
            // as long as we keep this order here.
            get_log_schema().project(&[ADD_NAME, REMOVE_NAME])?;
        let mut visitor = CdcVisitor::new(selection_vector);
        actions.extract(schema_to_use, &mut visitor)?;

        for remove in visitor.removes.into_iter() {
            let dv_id = remove.dv_unique_id();
            self.seen.insert((remove.path, dv_id));
        }

        Ok(visitor.adds)
    }
}

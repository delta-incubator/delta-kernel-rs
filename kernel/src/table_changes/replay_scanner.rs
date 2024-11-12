use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::actions::visitors::{AddVisitor, CdcVisitor, RemoveVisitor};
use crate::actions::{
    get_log_schema, Add, Cdc, Remove, ADD_NAME, CDC_NAME, COMMIT_INFO_NAME, REMOVE_NAME,
};
use crate::engine_data::{GetData, TypedGetData};
use crate::expressions::{column_expr, Expression};
use crate::path::ParsedLogPath;
use crate::scan::data_skipping::DataSkippingFilter;
use crate::scan::state::DvInfo;
use crate::scan::ScanData;
use crate::schema::SchemaRef;
use crate::table_changes::state::scan_row_schema;
use crate::{DataVisitor, DeltaResult, Engine, EngineData, ExpressionHandler, ExpressionRef};
use itertools::Itertools;
use tracing::debug;

use super::TableChangesScanData;

#[derive(Default)]
pub(crate) struct AddRemoveCdcVisitor {
    pub adds: Vec<(Add, usize)>,
    pub removes: Vec<(Remove, usize)>,
    pub cdcs: Vec<(Cdc, usize)>,
    selection_vector: Option<Vec<bool>>,
}

const ADD_FIELD_COUNT: usize = 15;
const CDC_FIELD_COUNT: usize = 5;

impl AddRemoveCdcVisitor {
    pub(crate) fn new(selection_vector: Option<Vec<bool>>) -> Self {
        AddRemoveCdcVisitor {
            selection_vector,
            ..Default::default()
        }
    }
}

/// Given an iterator of (engine_data, bool) tuples and a predicate, returns an iterator of
/// `(engine_data, selection_vec)`. Each row that is selected in the returned `engine_data` _must_
/// be processed to complete the scan. Non-selected rows _must_ be ignored. The boolean flag
/// indicates whether the record batch is a log or checkpoint batch.
pub(crate) fn table_changes_action_iter(
    engine: &dyn Engine,
    commit_files: impl IntoIterator<Item = ParsedLogPath>,
    table_schema: &SchemaRef,
    predicate: Option<ExpressionRef>,
) -> DeltaResult<impl Iterator<Item = DeltaResult<TableChangesScanData>>> {
    let commit_read_schema =
        get_log_schema().project(&[ADD_NAME, CDC_NAME, REMOVE_NAME, COMMIT_INFO_NAME])?;

    let json_client = engine.get_json_handler();

    let filter = DataSkippingFilter::new(engine, table_schema, predicate);
    let expression_handler = engine.get_expression_handler();
    let result = commit_files
        .into_iter()
        .map(move |commit_path| -> DeltaResult<_> {
            let timestamp = commit_path.location.last_modified;
            let commit_version = commit_path.version as i64;
            let action_iter = json_client.read_json_files(
                &[commit_path.location.clone()],
                commit_read_schema.clone(),
                None,
            )?;
            let expression_handler = expression_handler.clone();
            let mut metadata_scanner =
                TableChangesMetadataScanner::new(filter.clone(), timestamp, commit_version);

            // Find CDC, get commitInfo, and perform metadata scan
            for action_batch in action_iter {
                metadata_scanner.process_scan_batch(action_batch?.as_ref())?;
            }

            let action_iter = json_client.read_json_files(
                &[commit_path.location.clone()],
                commit_read_schema.clone(),
                None,
            )?;

            let mut log_scanner = metadata_scanner.into_replay_scanner();
            // File metadata output scan
            let x: Vec<ScanData> = action_iter
                .into_iter()
                .map(|action_batch| {
                    log_scanner
                        .process_scan_batch(expression_handler.as_ref(), action_batch?.as_ref())
                })
                .try_collect()?;
            let remove_dvs = Arc::new(log_scanner.remove_dvs);
            let y = x.into_iter().map(move |(a, b)| {
                let remove_dvs = remove_dvs.clone();
                (a, b, remove_dvs)
            });
            Ok(y)
        })
        .flatten_ok();
    Ok(result)
}

impl DataVisitor for AddRemoveCdcVisitor {
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            if self
                .selection_vector
                .as_ref()
                .is_some_and(|selection| !selection[i])
            {
                continue;
            }

            static CDC_FIELD_END: usize = ADD_FIELD_COUNT + CDC_FIELD_COUNT;
            // Add will have a path at index 0 if it is valid
            if let Some(path) = getters[0].get_opt(i, "add.path")? {
                // Keep the file unless the selection vector is present and is false for this row
                self.adds.push((
                    AddVisitor::visit_add(i, path, &getters[..ADD_FIELD_COUNT])?,
                    i,
                ))
            } else if let Some(path) = getters[ADD_FIELD_COUNT].get_opt(i, "cdc.path")? {
                self.cdcs.push((
                    CdcVisitor::visit_cdc(i, path, &getters[ADD_FIELD_COUNT..CDC_FIELD_END])?,
                    i,
                ))
            }
            // Remove will have a path at index 15 if it is valid
            // TODO(nick): Should count the fields in Add to ensure we don't get this wrong if more
            // are added
            // TODO(zach): add a check for selection vector that we never skip a remove
            else if let Some(path) = getters[CDC_FIELD_END].get_opt(i, "remove.path")? {
                let remove_getters = &getters[CDC_FIELD_END..];
                self.removes
                    .push((RemoveVisitor::visit_remove(i, path, remove_getters)?, i));
            }
        }
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct MetadataVisitor {
    pub adds: Vec<Add>,
    pub cdcs: Vec<Cdc>,
    pub timestamp: Option<i64>,
    selection_vector: Option<Vec<bool>>,
}
impl MetadataVisitor {
    pub(crate) fn new(selection_vector: Option<Vec<bool>>) -> Self {
        MetadataVisitor {
            selection_vector,
            ..Default::default()
        }
    }
}

impl DataVisitor for MetadataVisitor {
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            static CDC_FIELD_END: usize = ADD_FIELD_COUNT + CDC_FIELD_COUNT;
            // Add will have a path at index 0 if it is valid
            if let Some(path) = getters[0].get_opt(i, "add.path")? {
                // Keep the file unless the selection vector is present and is false for this row
                if !self
                    .selection_vector
                    .as_ref()
                    .is_some_and(|selection| !selection[i])
                {
                    self.adds
                        .push(AddVisitor::visit_add(i, path, &getters[..ADD_FIELD_COUNT])?)
                }
            } else if let Some(path) = getters[ADD_FIELD_COUNT].get_opt(i, "cdc.path")? {
                if !self
                    .selection_vector
                    .as_ref()
                    .is_some_and(|selection| !selection[i])
                {
                    self.cdcs.push(CdcVisitor::visit_cdc(
                        i,
                        path,
                        &getters[ADD_FIELD_COUNT..CDC_FIELD_END],
                    )?)
                }
            }
            // Remove will have a path at index 15 if it is valid
            // TODO(nick): Should count the fields in Add to ensure we don't get this wrong if more
            // are added
            // TODO(zach): add a check for selection vector that we never skip a remove
            else if let Some(timestamp) =
                getters[CDC_FIELD_END].get_long(i, "commitInfo.timestamp")?
            {
                self.timestamp = Some(timestamp);
            }
        }
        Ok(())
    }
}

pub(crate) struct TableChangesMetadataScanner {
    filter: Option<DataSkippingFilter>,
    pub add_files: HashSet<String>,
    pub has_cdcs: bool,
    pub timestamp: i64,
    pub commit_version: i64,
}
impl TableChangesMetadataScanner {
    pub(crate) fn new(
        filter: Option<DataSkippingFilter>,
        timestamp: i64,
        commit_version: i64,
    ) -> Self {
        TableChangesMetadataScanner {
            filter,
            add_files: Default::default(),
            has_cdcs: false,
            timestamp,
            commit_version,
        }
    }
    pub(crate) fn process_scan_batch(&mut self, actions: &dyn EngineData) -> DeltaResult<()> {
        // apply data skipping to get back a selection vector for actions that passed skipping
        // note: None implies all files passed data skipping.
        let filter_vector = self
            .filter
            .as_ref()
            .map(|filter| filter.apply(actions))
            .transpose()?;

        // we start our selection vector based on what was filtered. we will add to this vector
        // below if a file has been removed
        let selection_vector = match filter_vector {
            Some(ref filter_vector) => filter_vector.clone(),
            None => vec![false; actions.length()],
        };

        assert_eq!(selection_vector.len(), actions.length());
        let MetadataVisitor {
            timestamp,
            adds,
            cdcs,
            selection_vector: _,
        } = self.setup_batch_process(filter_vector, actions)?;
        // If a timestamp from `CommitInfo` action is found, use that
        if let Some(timestamp) = timestamp {
            self.timestamp = timestamp;
        }
        self.has_cdcs = self.has_cdcs || !cdcs.is_empty();
        if !self.has_cdcs {
            for add in adds.into_iter() {
                self.add_files.insert(add.path);
            }
        }
        Ok(())
    }

    // work shared between process_batch and process_scan_batch
    fn setup_batch_process(
        &mut self,
        selection_vector: Option<Vec<bool>>,
        actions: &dyn EngineData,
    ) -> DeltaResult<MetadataVisitor> {
        let schema_to_use =
            // NB: We _must_ pass these in the order `ADD_NAME, REMOVE_NAME` as the visitor assumes
            // the Add action comes first. The [`project`] method honors this order, so this works
            // as long as we keep this order here.
            get_log_schema().project(&[ADD_NAME, CDC_NAME, COMMIT_INFO_NAME,])?;
        let mut visitor = MetadataVisitor::new(selection_vector);
        actions.extract(schema_to_use, &mut visitor)?;

        Ok(visitor)
    }

    /// Create a new [`LogReplayScanner`] instance
    pub(crate) fn into_replay_scanner(self) -> TableChangesLogReplayScanner {
        TableChangesLogReplayScanner {
            filter: self.filter,
            remove_dvs: Default::default(),
            add_files: self.add_files,
            has_cdcs: self.has_cdcs,
            timestamp: self.timestamp,
            commit_version: self.commit_version,
        }
    }
}

pub(crate) struct TableChangesLogReplayScanner {
    filter: Option<DataSkippingFilter>,
    pub remove_dvs: HashMap<String, DvInfo>,
    pub add_files: HashSet<String>,
    pub has_cdcs: bool,
    pub timestamp: i64,
    pub commit_version: i64,
}

impl TableChangesLogReplayScanner {
    fn get_add_transform_expr(&self, timestamp: i64, commit_number: i64) -> Expression {
        Expression::struct_from([
            Expression::struct_from([
                column_expr!("add.path"),
                column_expr!("add.size"),
                column_expr!("add.modificationTime"),
                column_expr!("add.stats"),
                column_expr!("add.deletionVector"),
                Expression::struct_from([column_expr!("add.partitionValues")]),
            ]),
            Expression::struct_from([
                column_expr!("remove.path"),
                column_expr!("remove.size"),
                column_expr!("remove.deletionVector"),
                Expression::struct_from([column_expr!("remove.partitionValues")]),
            ]),
            Expression::struct_from([
                column_expr!("cdc.path"),
                column_expr!("cdc.size"),
                Expression::struct_from([column_expr!("cdc.partitionValues")]),
            ]),
            timestamp.into(),
            commit_number.into(),
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
            cdcs,
            selection_vector: _,
        } = self.setup_batch_process(filter_vector, actions)?;
        for (add, index) in adds.into_iter() {
            // Note: each (add.path + add.dv_unique_id()) pair has a
            // unique Add + Remove pair in the log. For example:
            // https://github.com/delta-io/delta/blob/master/spark/src/test/resources/delta/table-with-dv-large/_delta_log/00000000000000000001.json
            selection_vector[index] = !self.has_cdcs;
            debug!(
                "Including file in scan: ({}, {:?})",
                add.path,
                add.dv_unique_id(),
            );
        }
        for (remove, index) in removes.into_iter() {
            if !self.add_files.contains(&remove.path) {
                debug!(
                    "Including file in scan: ({}, {:?})",
                    remove.path,
                    remove.dv_unique_id(),
                );
                selection_vector[index] = !self.has_cdcs;
            } else {
                let dv_info = DvInfo {
                    deletion_vector: remove.deletion_vector,
                };
                self.remove_dvs.insert(remove.path.clone(), dv_info);
                selection_vector[index] = false;
            }
        }
        for (_cdc, index) in cdcs.into_iter() {
            selection_vector[index] = true;
        }

        let result = expression_handler
            .get_evaluator(
                get_log_schema().clone(),
                self.get_add_transform_expr(self.timestamp, self.commit_version),
                scan_row_schema().into(),
            )
            .evaluate(actions)?;

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
            get_log_schema().project(&[ADD_NAME, CDC_NAME, REMOVE_NAME])?;
        let mut visitor = AddRemoveCdcVisitor::new(selection_vector);
        actions.extract(schema_to_use, &mut visitor)?;

        Ok(visitor)
    }
}

use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::Arc;

use crate::actions::visitors::{AddVisitor, CdcVisitor, RemoveVisitor};
use crate::actions::{
    get_log_schema, Add, Cdc, Remove, ADD_NAME, CDC_NAME, COMMIT_INFO_NAME, REMOVE_NAME,
};
use crate::engine_data::{GetData, TypedGetData};
use crate::path::ParsedLogPath;
use crate::scan::data_skipping::DataSkippingFilter;
use crate::scan::state::DvInfo;
use crate::scan::ScanData;
use crate::schema::SchemaRef;
use crate::table_changes::state::{scan_row_schema, transform_to_scan_row_expression};
use crate::{
    DataVisitor, DeltaResult, Engine, EngineData, Error, ExpressionHandler, ExpressionRef,
};
use itertools::Itertools;
use tracing::debug;

use super::TableChangesScanData;
/// Given an iterator of ParsedLogPath and a predicate, returns an iterator of
/// [`TableChangesScanData`] = `(engine_data, selection_vec, remove_deletion_vectors)`.
/// Each row that is selected in the returned `engine_data` _must_ be processed to
/// complete the scan. Non-selected rows _must_ be ignored.
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
            let expression_handler = expression_handler.clone();
            let timestamp = commit_path.location.last_modified;
            let commit_version = commit_path.version as i64;

            let action_iter = json_client.read_json_files(
                &[commit_path.location.clone()],
                commit_read_schema.clone(),
                None,
            )?;

            let metadata_scanner = TableChangesMetadataScanner::new(
                filter.clone(),
                timestamp,
                commit_version,
                action_iter,
            );
            let metadata_scanner = metadata_scanner.run()?;

            // Get another action iterator for the second pass through the actions
            let action_iter = json_client.read_json_files(
                &[commit_path.location.clone()],
                commit_read_schema.clone(),
                None,
            )?;

            let log_scanner = metadata_scanner.into_replay_scanner(action_iter, expression_handler);
            // File metadata output scan
            log_scanner.into_scan_files()
        })
        .flatten_ok();
    Ok(result)
}

trait PhaseStatus {}
struct Uninitialized;
struct Initialized;
impl PhaseStatus for Uninitialized {}
impl PhaseStatus for Initialized {}

struct TableChangesMetadataScanner<
    I: Iterator<Item = DeltaResult<Box<dyn EngineData>>>,
    S: PhaseStatus,
> {
    action_iter: Option<I>,
    filter: Option<DataSkippingFilter>,
    pub add_files: HashSet<String>,
    pub has_cdcs: bool,
    pub timestamp: i64,
    pub commit_version: i64,
    _phase: PhantomData<S>,
}
impl<I: Iterator<Item = DeltaResult<Box<dyn EngineData>>>>
    TableChangesMetadataScanner<I, Uninitialized>
{
    pub(crate) fn new(
        filter: Option<DataSkippingFilter>,
        timestamp: i64,
        commit_version: i64,
        action_iter: I,
    ) -> Self {
        TableChangesMetadataScanner {
            filter,
            add_files: Default::default(),
            has_cdcs: false,
            timestamp,
            commit_version,
            action_iter: Some(action_iter),
            _phase: Default::default(),
        }
    }
    pub(crate) fn run(mut self) -> DeltaResult<TableChangesMetadataScanner<I, Initialized>> {
        let Some(action_iter) = self.action_iter.take() else {
            return Err(Error::generic("Log replay failed"));
        };
        // Find CDC, get commitInfo, and perform metadata scan
        for action_batch in action_iter.into_iter() {
            self.process_scan_batch(action_batch?.as_ref())?;
        }
        Ok(TableChangesMetadataScanner {
            _phase: Default::default(),
            action_iter: None,
            filter: self.filter,
            add_files: self.add_files,
            has_cdcs: self.has_cdcs,
            timestamp: self.timestamp,
            commit_version: self.commit_version,
        })
    }
    fn process_scan_batch(&mut self, actions: &dyn EngineData) -> DeltaResult<()> {
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
}

impl<I: Iterator<Item = DeltaResult<Box<dyn EngineData>>>>
    TableChangesMetadataScanner<I, Initialized>
{
    /// Create a new [`LogReplayScanner`] instance
    pub(crate) fn into_replay_scanner(
        self,
        action_iter: I,
        expression_handler: Arc<dyn ExpressionHandler>,
    ) -> TableChangesLogReplayScanner<I> {
        TableChangesLogReplayScanner {
            filter: self.filter,
            remove_dvs: Default::default(),
            add_files: self.add_files,
            has_cdcs: self.has_cdcs,
            timestamp: self.timestamp,
            commit_version: self.commit_version,
            expression_handler,
            action_iter: Some(action_iter),
        }
    }
}

struct TableChangesLogReplayScanner<I: Iterator<Item = DeltaResult<Box<dyn EngineData>>>> {
    filter: Option<DataSkippingFilter>,
    action_iter: Option<I>,
    remove_dvs: HashMap<String, DvInfo>,
    add_files: HashSet<String>,
    has_cdcs: bool,
    timestamp: i64,
    commit_version: i64,
    expression_handler: Arc<dyn ExpressionHandler>,
}

impl<I: Iterator<Item = DeltaResult<Box<dyn EngineData>>>> TableChangesLogReplayScanner<I> {
    pub(crate) fn into_scan_files(
        mut self,
    ) -> DeltaResult<impl Iterator<Item = TableChangesScanData>> {
        let Some(action_iter) = self.action_iter.take() else {
            return Err(Error::generic("Log replay failed"));
        };

        let expression_handler = self.expression_handler.clone();
        let data: Vec<_> = action_iter
            .into_iter()
            .map(|action_batch| {
                self.process_scan_batch(expression_handler.as_ref(), action_batch?.as_ref())
            })
            .try_collect()?;

        let remove_dvs = Arc::new(self.remove_dvs);
        Ok(data.into_iter().map(move |(a, b)| {
            let remove_dvs = remove_dvs.clone();
            (a, b, remove_dvs)
        }))
    }

    fn process_scan_batch(
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
                transform_to_scan_row_expression(self.timestamp, self.commit_version),
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
            } else if let Some(path) = getters[CDC_FIELD_END].get_opt(i, "remove.path")? {
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
            } else if let Some(timestamp) =
                getters[CDC_FIELD_END].get_long(i, "commitInfo.timestamp")?
            {
                self.timestamp = Some(timestamp);
            }
        }
        Ok(())
    }
}

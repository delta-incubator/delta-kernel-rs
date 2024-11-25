use std::collections::{HashMap, HashSet};
use std::iter::{empty, once};
use std::sync::{Arc, LazyLock};

use crate::actions::visitors::{visit_deletion_vector_at, ProtocolVisitor};
use crate::actions::{
    get_log_add_schema, get_log_schema, ADD_NAME, CDC_NAME, COMMIT_INFO_NAME, METADATA_NAME,
    PROTOCOL_NAME, REMOVE_NAME,
};
use crate::expressions::column_name;
use crate::expressions::{column_expr, Expression};
use crate::path::ParsedLogPath;
use crate::scan::data_skipping::DataSkippingFilter;
use crate::scan::log_replay::SCAN_ROW_DATATYPE;
use crate::scan::state::DvInfo;
use crate::schema::{ArrayType, ColumnNamesAndTypes, DataType, SchemaRef, StructType};
use crate::utils::require;
use crate::{
    DeltaResult, Engine, EngineData, Error, ExpressionEvaluator, ExpressionRef, JsonHandler,
    RowVisitor,
};
use itertools::Itertools;

type TableChangesScanData = (Box<dyn EngineData>, Vec<bool>, Arc<HashMap<String, DvInfo>>);

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
    let json_client = engine.get_json_handler();
    let filter = DataSkippingFilter::new(engine, table_schema, predicate);

    let expression_evaluator = engine.get_expression_handler().get_evaluator(
        get_log_add_schema().clone(),
        get_add_transform_expr(),
        SCAN_ROW_DATATYPE.clone(),
    );

    let result = commit_files
        .into_iter()
        .map(move |commit_file| -> DeltaResult<_> {
            let mut scanner = LogReplayScanner::new(
                commit_file,
                json_client.clone(),
                filter.clone(),
                expression_evaluator.clone(),
            );
            scanner.phase_1()?;
            scanner.into_scan_batches()
        })
        .flatten_ok()
        .map(|res| res?);
    Ok(result)
}

fn get_add_transform_expr() -> Expression {
    Expression::Struct(vec![
        column_expr!("add.path"),
        column_expr!("add.size"),
        column_expr!("add.modificationTime"),
        column_expr!("add.stats"),
        column_expr!("add.deletionVector"),
        Expression::Struct(vec![column_expr!("add.partitionValues")]),
    ])
}

struct LogReplayScanner {
    has_cdc_action: bool,
    // A map from path to the deletion vector from the remove action. It is guaranteed that there
    // is an add action with the same path in this commit
    remove_dvs: HashMap<String, DvInfo>,
    commit_file: ParsedLogPath,
    json_client: Arc<dyn JsonHandler>,
    timestamp: i64,
    filter: Option<DataSkippingFilter>,
    expression_evaluator: Arc<dyn ExpressionEvaluator>,
}

impl LogReplayScanner {
    fn new(
        commit_file: ParsedLogPath,
        json_client: Arc<dyn JsonHandler>,
        filter: Option<DataSkippingFilter>,
        expression_evaluator: Arc<dyn ExpressionEvaluator>,
    ) -> Self {
        Self {
            timestamp: commit_file.location.last_modified,
            commit_file,
            json_client,
            filter,
            has_cdc_action: Default::default(),
            remove_dvs: Default::default(),
            expression_evaluator,
        }
    }
    fn phase_1(&mut self) -> DeltaResult<()> {
        let schema = Phase1Visitor::schema()?;
        let action_iter =
            self.json_client
                .read_json_files(&[self.commit_file.location.clone()], schema, None)?;

        let mut add_set: HashSet<String> = Default::default();

        for actions in action_iter {
            let actions = actions?;
            // apply data skipping to get back a selection vector for actions that passed skipping
            // note: None implies all files passed data skipping.
            let filter_vector = self
                .filter
                .as_ref()
                .map(|filter| filter.apply(actions.as_ref()))
                .transpose()?;

            // we start our selection vector based on what was filtered. we will add to this vector
            // below if a file has been removed
            let selection_vector = match filter_vector {
                Some(ref filter_vector) => filter_vector.clone(),
                None => vec![false; actions.len()],
            };
            let mut visitor = Phase1Visitor::new(self, selection_vector, &mut add_set);
            visitor.visit_rows_of(actions.as_ref())?;
        }
        self.remove_dvs
            .retain(|rm_path, _| add_set.contains(rm_path));
        Ok(())
    }

    fn into_scan_batches(
        self,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<TableChangesScanData>>> {
        let Self {
            has_cdc_action,
            remove_dvs,
            commit_file,
            json_client,
            timestamp: _,
            filter,
            expression_evaluator,
        } = self;
        let remove_dvs = Arc::new(remove_dvs);

        let schema = Phase2Visitor::schema()?;
        let action_iter =
            json_client.read_json_files(&[commit_file.location.clone()], schema, None)?;

        let result = action_iter.map(move |actions| -> DeltaResult<_> {
            let actions = actions?;
            // apply data skipping to get back a selection vector for actions that passed skipping
            // note: None implies all files passed data skipping.
            let filter_vector = filter
                .as_ref()
                .map(|filter| filter.apply(actions.as_ref()))
                .transpose()?;

            // we start our selection vector based on what was filtered. we will add to this vector
            // below if a file has been removed
            let selection_vector = match filter_vector {
                Some(ref filter_vector) => filter_vector.clone(),
                None => vec![false; actions.len()],
            };
            let mut visitor = Phase2Visitor::new(&remove_dvs, selection_vector, has_cdc_action);
            visitor.visit_rows_of(actions.as_ref())?;

            let result = expression_evaluator.evaluate(actions.as_ref())?;
            Ok((result, visitor.selection_vector, remove_dvs.clone()))
        });
        Ok(result)
    }
}

struct Phase1Visitor<'a> {
    scanner: &'a mut LogReplayScanner,
    selection_vector: Vec<bool>,
    add_set: &'a mut HashSet<String>,
}
impl<'a> Phase1Visitor<'a> {
    fn new(
        scanner: &'a mut LogReplayScanner,
        selection_vector: Vec<bool>,
        add_set: &'a mut HashSet<String>,
    ) -> Self {
        Phase1Visitor {
            scanner,
            selection_vector,
            add_set,
        }
    }
    fn schema() -> DeltaResult<Arc<StructType>> {
        get_log_schema().project(&[
            ADD_NAME,
            CDC_NAME,
            COMMIT_INFO_NAME,
            METADATA_NAME,
            PROTOCOL_NAME,
        ])
    }
}

impl<'a> RowVisitor for Phase1Visitor<'a> {
    fn selected_column_names_and_types(
        &self,
    ) -> (
        &'static [crate::expressions::ColumnName],
        &'static [crate::schema::DataType],
    ) {
        // NOTE: The order of the names and types is based on [`Phase1Visitor::schema`]
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            const STRING: DataType = DataType::STRING;
            const INTEGER: DataType = DataType::INTEGER;
            const LONG: DataType = DataType::LONG;
            let string_list: DataType = ArrayType::new(STRING, false).into();
            let types_and_names = vec![
                (STRING, column_name!("add.path")),
                (STRING, column_name!("remove.path")),
                (STRING, column_name!("remove.deletionVector.storageType")),
                (STRING, column_name!("remove.deletionVector.pathOrInlineDv")),
                (INTEGER, column_name!("remove.deletionVector.offset")),
                (STRING, column_name!("cdc.path")),
                (LONG, column_name!("commitInfo.timestamp")),
                (STRING, column_name!("metaData.schemaString")),
                (INTEGER, column_name!("protocol.minReaderVersion")),
                (INTEGER, column_name!("protocol.minWriterVersion")),
                (string_list.clone(), column_name!("protocol.readerFeatures")),
                (string_list, column_name!("protocol.writerFeatures")),
            ];
            let (types, names) = types_and_names.into_iter().unzip();
            (names, types).into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'b>(
        &mut self,
        row_count: usize,
        getters: &[&'b dyn crate::engine_data::GetData<'b>],
    ) -> DeltaResult<()> {
        let expected_getters = 8;
        require!(
            getters.len() == expected_getters,
            Error::InternalError(format!(
                "Wrong number of AddRemoveDedupVisitor getters: {}",
                getters.len()
            ))
        );

        for i in 0..row_count {
            if !self.selection_vector[i] {
                continue;
            }
            if !self.scanner.has_cdc_action {
                if let Some(path) = getters[0].get_str(i, "add.path")? {
                    self.add_set.insert(path.to_string());
                } else if let Some(path) = getters[1].get_str(i, "remove.path")? {
                    let deletion_vector = visit_deletion_vector_at(i, &getters[5..8])?;
                    self.scanner
                        .remove_dvs
                        .insert(path.to_string(), DvInfo { deletion_vector });
                } else if getters[5].get_str(i, "cdc.path")?.is_some() {
                    self.scanner.has_cdc_action = true;
                }
            }
            if let Some(timestamp) = getters[6].get_long(i, "commitInfo.timestamp")? {
                self.scanner.timestamp = timestamp;
            } else if let Some(schema) = getters[7].get_str(i, "metaData.schemaString")? {
                // TODO: Validate that the schema is as expected
            } else if let Some(min_reader_version) =
                getters[8].get_int(i, "protocol.min_reader_version")?
            {
                let protocol =
                    ProtocolVisitor::visit_protocol(i, min_reader_version, &getters[4..8])?;
                protocol.ensure_read_supported()?;
            }
        }
        Ok(())
    }
}

struct Phase2Visitor<'a> {
    selection_vector: Vec<bool>,
    has_cdc_action: bool,
    remove_dvs: &'a HashMap<String, DvInfo>,
}

impl<'a> Phase2Visitor<'a> {
    fn new(
        remove_dvs: &'a HashMap<String, DvInfo>,
        selection_vector: Vec<bool>,
        has_cdc_action: bool,
    ) -> Self {
        Phase2Visitor {
            selection_vector,
            has_cdc_action,
            remove_dvs,
        }
    }
    fn schema() -> DeltaResult<Arc<StructType>> {
        get_log_schema().project(&[ADD_NAME, REMOVE_NAME, CDC_NAME])
    }
}

impl<'a> RowVisitor for Phase2Visitor<'a> {
    fn selected_column_names_and_types(
        &self,
    ) -> (
        &'static [crate::expressions::ColumnName],
        &'static [crate::schema::DataType],
    ) {
        // NOTE: The order of the names and types is based on [`Phase2Visitor::schema`]
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            const STRING: DataType = DataType::STRING;
            const INTEGER: DataType = DataType::INTEGER;
            const LONG: DataType = DataType::LONG;
            let types_and_names = vec![
                (STRING, column_name!("add.path")),
                (STRING, column_name!("remove.path")),
                (STRING, column_name!("cdc.path")),
            ];
            let (types, names) = types_and_names.into_iter().unzip();
            (names, types).into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'b>(
        &mut self,
        row_count: usize,
        getters: &[&'b dyn crate::engine_data::GetData<'b>],
    ) -> DeltaResult<()> {
        let expected_getters = 8;
        require!(
            getters.len() == expected_getters,
            Error::InternalError(format!(
                "Wrong number of AddRemoveDedupVisitor getters: {}",
                getters.len()
            ))
        );

        for i in 0..row_count {
            if !self.selection_vector[i] {
                continue;
            }
            if getters[0].get_str(i, "add.path")?.is_some() {
                self.selection_vector[i] = !self.has_cdc_action;
            } else if let Some(path) = getters[1].get_str(i, "remove.path")? {
                self.selection_vector[i] =
                    !self.has_cdc_action && !self.remove_dvs.contains_key(path)
            } else {
                self.selection_vector[i] = getters[2].get_str(i, "cdc.path")?.is_some()
            };
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use crate::actions::get_log_add_schema;
    use crate::engine::sync::SyncEngine;
    use crate::log_segment::LogSegment;
    use crate::path::ParsedLogPath;
    use crate::scan::log_replay::SCAN_ROW_DATATYPE;
    use crate::{scan, DeltaResult, Engine, Version};

    use super::{get_add_transform_expr, LogReplayScanner};

    fn get_segment(
        engine: &dyn Engine,
        path: &str,
        start_version: Version,
        end_version: impl Into<Option<Version>>,
    ) -> DeltaResult<Vec<ParsedLogPath>> {
        let table_root = url::Url::from_directory_path(path).unwrap();

        let log_root = table_root.join("_delta_log/")?;
        let log_segment = LogSegment::for_table_changes(
            engine.get_file_system_client().as_ref(),
            log_root,
            start_version,
            end_version,
        )?;
        Ok(log_segment.ascending_commit_files)
    }

    fn get_commit_log_scanner(engine: &dyn Engine, commit: ParsedLogPath) -> LogReplayScanner {
        let expression_evaluator = engine.get_expression_handler().get_evaluator(
            get_log_add_schema().clone(),
            get_add_transform_expr(),
            SCAN_ROW_DATATYPE.clone(),
        );
        LogReplayScanner::new(
            commit,
            engine.get_json_handler(),
            None,
            expression_evaluator.clone(),
        )
    }

    #[test]
    fn simple_log_replay() {
        let engine = SyncEngine::new();
        let path = "./tests/data/table-with-cdf";
        let mut commits = get_segment(&engine, path, 0, None).unwrap().into_iter();

        let mut scanner = get_commit_log_scanner(&engine, commits.next().unwrap());
        scanner.phase_1().unwrap();
        assert!(!scanner.has_cdc_action);
        assert!(scanner.remove_dvs.is_empty());
    }
}

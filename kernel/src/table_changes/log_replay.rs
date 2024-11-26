use std::collections::{HashMap, HashSet};
use std::iter::{empty, once};
use std::sync::{Arc, LazyLock};

use crate::actions::visitors::{visit_deletion_vector_at, ProtocolVisitor};
use crate::actions::{
    get_log_add_schema, get_log_schema, Metadata, Protocol, ADD_NAME, CDC_NAME, COMMIT_INFO_NAME,
    METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME,
};
use crate::engine::arrow_data::ArrowEngineData;
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

struct TableChangesScanData {
    data: Box<dyn EngineData>,
    selection_vector: Vec<bool>,
    remove_dv: Option<Arc<HashMap<String, DvInfo>>>,
}

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
            scanner.visit_commit()?;
            scanner.resolve_dvs();
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
    add_paths: HashSet<String>,
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
            add_paths: Default::default(),
        }
    }
    fn visit_commit(&mut self) -> DeltaResult<()> {
        let schema = Phase1Visitor::schema()?;
        let action_iter =
            self.json_client
                .read_json_files(&[self.commit_file.location.clone()], schema, None)?;

        for actions in action_iter {
            let actions = actions?;

            //let actions_arrow: &ArrowEngineData =
            //    actions.as_ref().any_ref().downcast_ref().unwrap();
            //println!("actions: {:?}", actions_arrow.record_batch());
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
                None => vec![true; actions.len()],
            };
            let mut visitor = Phase1Visitor::new(self, selection_vector);
            visitor.visit_rows_of(actions.as_ref())?;
            if let Some(protocol) = visitor.protocol {
                protocol.ensure_read_supported()?;
            }
            if let Some(schema) = visitor.schema {
                // TODO: Ensure schema is compatible
            }
        }
        Ok(())
    }

    fn resolve_dvs(&mut self) {
        if self.has_cdc_action {
            self.remove_dvs.clear();
        } else {
            self.remove_dvs
                .retain(|rm_path, _| self.add_paths.contains(rm_path));
        }
        self.add_paths.clear()
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
            add_paths: _,
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
                None => vec![true; actions.len()],
            };
            let mut visitor = Phase2Visitor::new(&remove_dvs, selection_vector, has_cdc_action);
            visitor.visit_rows_of(actions.as_ref())?;

            let data = expression_evaluator.evaluate(actions.as_ref())?;
            Ok(TableChangesScanData {
                data,
                selection_vector: visitor.selection_vector,
                remove_dv: Some(remove_dvs.clone()),
            })
        });
        Ok(result)
    }
}

struct Phase1Visitor<'a> {
    scanner: &'a mut LogReplayScanner,
    selection_vector: Vec<bool>,
    protocol: Option<Protocol>,
    schema: Option<String>,
}
impl<'a> Phase1Visitor<'a> {
    fn new(scanner: &'a mut LogReplayScanner, selection_vector: Vec<bool>) -> Self {
        Phase1Visitor {
            scanner,
            selection_vector,
            protocol: None,
            schema: None,
        }
    }
    fn schema() -> DeltaResult<Arc<StructType>> {
        get_log_schema().project(&[
            ADD_NAME,
            REMOVE_NAME,
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
                (INTEGER, column_name!("remove.deletionVector.sizeInBytes")),
                (LONG, column_name!("remove.deletionVector.cardinality")),
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
        let expected_getters = 14;
        require!(
            getters.len() == expected_getters,
            Error::InternalError(format!(
                "Wrong number of Phase1Visitor getters: {}",
                getters.len()
            ))
        );

        for i in 0..row_count {
            if !self.selection_vector[i] {
                continue;
            }
            if let Some(path) = getters[0].get_str(i, "add.path")? {
                self.scanner.add_paths.insert(path.to_string());
            } else if let Some(path) = getters[1].get_str(i, "remove.path")? {
                let deletion_vector = visit_deletion_vector_at(i, &getters[2..=6])?;
                self.scanner
                    .remove_dvs
                    .insert(path.to_string(), DvInfo { deletion_vector });
            } else if getters[7].get_str(i, "cdc.path")?.is_some() {
                self.scanner.has_cdc_action = true;
            } else if let Some(timestamp) = getters[8].get_long(i, "commitInfo.timestamp")? {
                self.scanner.timestamp = timestamp;
            } else if let Some(schema) = getters[9].get_str(i, "metaData.schemaString")? {
                self.schema = Some(schema.to_string());
                // TODO: Validate that the schema is as expected
            } else if let Some(min_reader_version) =
                getters[10].get_int(i, "protocol.min_reader_version")?
            {
                let protocol =
                    ProtocolVisitor::visit_protocol(i, min_reader_version, &getters[10..=13])?;
                self.protocol = Some(protocol);
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
        let expected_getters = 3;
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

    use crate::actions::deletion_vector::DeletionVectorDescriptor;
    use crate::scan::state::DvInfo;
    use crate::schema::{DataType, StructField, StructType};
    use itertools::Itertools;
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    use serde::Serialize;
    use std::collections::{HashMap, HashSet};
    use std::path::Path;
    use std::sync::Arc;
    use tempfile::TempDir;

    use super::{get_add_transform_expr, LogReplayScanner, TableChangesScanData};
    use crate::actions::{get_log_add_schema, Add, Cdc, CommitInfo, Metadata, Protocol, Remove};
    use crate::engine::sync::SyncEngine;
    use crate::log_segment::LogSegment;
    use crate::path::ParsedLogPath;
    use crate::scan::log_replay::SCAN_ROW_DATATYPE;
    use crate::{DeltaResult, Engine, Version};
    use test_utils::delta_path_for_version;

    #[derive(Serialize)]
    #[allow(unused)]
    enum Action {
        #[serde(rename = "add")]
        Add(Add),
        #[serde(rename = "remove")]
        Remove(Remove),
        #[serde(rename = "cdc")]
        Cdc(Cdc),
        #[serde(rename = "metaData")]
        Metadata(Metadata),
        #[serde(rename = "protocol")]
        Protocol(Protocol),
        #[serde(rename = "commitInfo")]
        CommitInfo(CommitInfo),
    }

    impl From<Add> for Action {
        fn from(value: Add) -> Self {
            Self::Add(value)
        }
    }
    impl From<Remove> for Action {
        fn from(value: Remove) -> Self {
            Self::Remove(value)
        }
    }
    impl From<Cdc> for Action {
        fn from(value: Cdc) -> Self {
            Self::Cdc(value)
        }
    }
    impl From<Metadata> for Action {
        fn from(value: Metadata) -> Self {
            Self::Metadata(value)
        }
    }
    impl From<Protocol> for Action {
        fn from(value: Protocol) -> Self {
            Self::Protocol(value)
        }
    }
    impl From<CommitInfo> for Action {
        fn from(value: CommitInfo) -> Self {
            Self::CommitInfo(value)
        }
    }

    struct MockTable {
        commit_num: u64,
        store: Arc<LocalFileSystem>,
        dir: TempDir,
    }

    impl MockTable {
        pub(crate) fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
            Self {
                commit_num: 0,
                store,
                dir,
            }
        }
        pub(crate) async fn commit(&mut self, actions: &[Action]) {
            let data = actions
                .iter()
                .map(|action| serde_json::to_string(&action).unwrap())
                .join("\n");
            println!("Writing data: {}", data);

            let path = delta_path_for_version(self.commit_num, "json");
            self.commit_num += 1;
            // add log files to store

            self.store
                .put(&path, data.into())
                .await
                .expect("put log file in store");
        }
        pub(crate) fn table_root(&self) -> &Path {
            self.dir.path()
        }
    }
    fn get_init_commit() -> Vec<Action> {
        let schema = StructType::new([
            StructField::new("id", DataType::LONG, true),
            StructField::new("value", DataType::STRING, true),
        ]);
        let schema_string = serde_json::to_string(&schema).unwrap();
        vec![
            Metadata {
                schema_string,
                configuration: HashMap::from([
                    ("enableChangeDataFeed".to_string(), "true".to_string()),
                    ("enableDeletionVectors".to_string(), "true".to_string()),
                ]),
                ..Default::default()
            }
            .into(),
            Protocol::try_new(3, 7, Some(["deletionVectors"]), Some(["deletionVectors"]))
                .unwrap()
                .into(),
        ]
    }

    fn get_segment(
        engine: &dyn Engine,
        path: &Path,
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
    fn result_to_sv(iter: impl Iterator<Item = DeltaResult<TableChangesScanData>>) -> Vec<bool> {
        iter.map(|scan_data| -> DeltaResult<_> { Ok(scan_data?.selection_vector) })
            .flatten_ok()
            .try_collect()
            .unwrap()
    }

    #[tokio::test]
    async fn metadata_protocol() {
        let engine = SyncEngine::new();
        let mut mock_table = MockTable::new();
        mock_table.commit(&get_init_commit()).await;

        let mut commits = get_segment(&engine, mock_table.table_root(), 0, None)
            .unwrap()
            .into_iter();

        let mut scanner = get_commit_log_scanner(&engine, commits.next().unwrap());

        scanner.visit_commit().unwrap();
        assert!(!scanner.has_cdc_action);
        assert!(scanner.remove_dvs.is_empty());
        assert!(scanner.add_paths.is_empty());

        scanner.resolve_dvs();

        assert_eq!(
            result_to_sv(scanner.into_scan_batches().unwrap()),
            &[false, false]
        );
    }

    #[tokio::test]
    async fn table_changes_add_remove() {
        let engine = SyncEngine::new();
        let mut mock_table = MockTable::new();
        mock_table
            .commit(&[
                Add {
                    path: "fake_path_1".into(),
                    ..Default::default()
                }
                .into(),
                Remove {
                    path: "fake_path_2".into(),
                    ..Default::default()
                }
                .into(),
            ])
            .await;

        let mut commits = get_segment(&engine, mock_table.table_root(), 0, None)
            .unwrap()
            .into_iter();

        let commit = commits.next().unwrap();
        let mut scanner = get_commit_log_scanner(&engine, commit);

        scanner.visit_commit().unwrap();
        assert!(!scanner.has_cdc_action);
        assert_eq!(
            scanner.add_paths,
            HashSet::from(["fake_path_1".to_string()])
        );
        assert_eq!(
            scanner.remove_dvs,
            HashMap::from([(
                "fake_path_2".to_string(),
                DvInfo {
                    deletion_vector: None
                }
            )])
        );

        scanner.resolve_dvs();
        assert_eq!(scanner.remove_dvs, HashMap::new());

        assert_eq!(
            result_to_sv(scanner.into_scan_batches().unwrap()),
            &[true, true]
        );
    }

    #[tokio::test]
    async fn table_changes_cdc() {
        let engine = SyncEngine::new();
        let mut mock_table = MockTable::new();
        mock_table.commit(&get_init_commit()).await;
        mock_table
            .commit(&[
                Add {
                    path: "fake_path_1".into(),
                    ..Default::default()
                }
                .into(),
                Remove {
                    path: "fake_path_2".into(),
                    ..Default::default()
                }
                .into(),
                Cdc {
                    path: "fake_path_3".into(),
                    ..Default::default()
                }
                .into(),
            ])
            .await;

        let mut commits = get_segment(&engine, mock_table.table_root(), 0, None)
            .unwrap()
            .into_iter();

        let commit = commits.nth(1).unwrap();
        let mut scanner = get_commit_log_scanner(&engine, commit);

        scanner.visit_commit().unwrap();
        assert!(scanner.has_cdc_action);
        assert_eq!(
            scanner.add_paths,
            HashSet::from(["fake_path_1".to_string()])
        );
        assert_eq!(
            scanner.remove_dvs,
            HashMap::from([(
                "fake_path_2".to_string(),
                DvInfo {
                    deletion_vector: None
                }
            )])
        );

        scanner.resolve_dvs();
        assert_eq!(scanner.remove_dvs, HashMap::new());

        assert_eq!(
            result_to_sv(scanner.into_scan_batches().unwrap()),
            &[false, false, true]
        );
    }

    #[tokio::test]
    async fn table_changes_dv() {
        let engine = SyncEngine::new();
        let mut mock_table = MockTable::new();

        let deletion_vector1 = DeletionVectorDescriptor {
            storage_type: "u".to_string(),
            path_or_inline_dv: "vBn[lx{q8@P<9BNH/isA".to_string(),
            offset: Some(1),
            size_in_bytes: 36,
            cardinality: 2,
        };
        let deletion_vector2 = DeletionVectorDescriptor {
            storage_type: "u".to_string(),
            path_or_inline_dv: "U5OWRz5k%CFT.Td}yCPW".to_string(),
            offset: Some(1),
            size_in_bytes: 38,
            cardinality: 3,
        };
        mock_table
            .commit(&[
                Add {
                    path: "fake_path_1".into(),
                    ..Default::default()
                }
                .into(),
                Remove {
                    path: "fake_path_1".into(),
                    deletion_vector: Some(deletion_vector1.clone()),
                    ..Default::default()
                }
                .into(),
                Remove {
                    path: "fake_path_2".into(),
                    deletion_vector: Some(deletion_vector2.clone()),
                    ..Default::default()
                }
                .into(),
            ])
            .await;

        let mut commits = get_segment(&engine, mock_table.table_root(), 0, None)
            .unwrap()
            .into_iter();

        let commit = commits.next().unwrap();
        let mut scanner = get_commit_log_scanner(&engine, commit);

        scanner.visit_commit().unwrap();
        assert!(!scanner.has_cdc_action);
        assert_eq!(
            scanner.add_paths,
            HashSet::from(["fake_path_1".to_string()])
        );
        assert_eq!(
            scanner.remove_dvs,
            HashMap::from([
                (
                    "fake_path_1".to_string(),
                    DvInfo {
                        deletion_vector: Some(deletion_vector1.clone())
                    }
                ),
                (
                    "fake_path_2".to_string(),
                    DvInfo {
                        deletion_vector: Some(deletion_vector2.clone())
                    }
                )
            ])
        );

        scanner.resolve_dvs();
        assert_eq!(
            scanner.remove_dvs,
            HashMap::from([(
                "fake_path_1".to_string(),
                DvInfo {
                    deletion_vector: Some(deletion_vector1.clone())
                }
            )])
        );

        assert_eq!(
            result_to_sv(scanner.into_scan_batches().unwrap()),
            &[true, false, true]
        );
    }
    #[tokio::test]
    async fn table_changes_protocol() {
        let engine = SyncEngine::new();
        let mut mock_table = MockTable::new();

        let protocol = Protocol::try_new(
            3,
            1,
            ["fake_feature".to_string()].into(),
            ["fake_feature".to_string()].into(),
        )
        .unwrap();

        mock_table
            .commit(&[
                Add {
                    path: "fake_path_1".into(),
                    ..Default::default()
                }
                .into(),
                Remove {
                    path: "fake_path_2".into(),
                    ..Default::default()
                }
                .into(),
                protocol.into(),
            ])
            .await;

        let mut commits = get_segment(&engine, mock_table.table_root(), 0, None)
            .unwrap()
            .into_iter();

        let commit = commits.next().unwrap();
        let mut scanner = get_commit_log_scanner(&engine, commit);

        assert!(scanner.visit_commit().is_err());
    }
}

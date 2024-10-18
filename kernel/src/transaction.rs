use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::actions::get_log_schema;
use crate::error::Error;
use crate::path::ParsedLogPath;
use crate::schema::{MapType, StructField, StructType};
use crate::snapshot::Snapshot;
use crate::{DataType, DeltaResult, Engine, EngineData, Expression, Version};

const KERNEL_VERSION: &str = env!("CARGO_PKG_VERSION");
const UNKNOWN_OPERATION: &str = "UNKNOWN";

/// A transaction represents an in-progress write to a table.
pub struct Transaction {
    read_snapshot: Arc<Snapshot>,
    operation: Option<String>,
    commit_info: Option<Box<dyn EngineData>>,
}

impl std::fmt::Debug for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!(
            "Transaction {{ read_snapshot version: {}, commit_info: {} }}",
            self.read_snapshot.version(),
            self.commit_info.is_some()
        ))
    }
}

impl Transaction {
    /// Create a new transaction from a snapshot. The snapshot will be used to read the current
    /// state of the table (e.g. to read the current version).
    ///
    /// Instead of using this API, the more typical (user-facing) API is
    /// [Table::new_transaction](crate::table::Table::new_transaction) to create a transaction from
    /// a table automatically backed by the latest snapshot.
    pub(crate) fn new(snapshot: impl Into<Arc<Snapshot>>) -> Self {
        Transaction {
            read_snapshot: snapshot.into(),
            operation: None,
            commit_info: None,
        }
    }

    /// Consume the transaction and commit the in-progress write to the table.
    pub fn commit(self, engine: &dyn Engine) -> DeltaResult<CommitResult> {
        // step one: construct the iterator of actions we want to commit
        // note: only support commit_info right now.
        let actions: Box<dyn Iterator<Item = Box<dyn EngineData>> + Send> = match self.commit_info {
            Some(engine_commit_info) => {
                let actions =
                    generate_commit_info(engine, self.operation.as_deref(), engine_commit_info)?;
                Box::new(std::iter::once(actions))
            }
            None => {
                // if there is no commit info, actions start empty
                Box::new(std::iter::empty())
            }
        };

        // step two: set new commit version (current_version + 1) and path to write
        let commit_version = self.read_snapshot.version() + 1;
        let commit_path =
            ParsedLogPath::new_commit(self.read_snapshot.table_root(), commit_version)?;

        // step three: commit the actions as a json file in the log
        let json_handler = engine.get_json_handler();
        match json_handler.write_json_file(&commit_path.location, Box::new(actions), false) {
            Ok(()) => Ok(CommitResult::Committed(commit_version)),
            Err(Error::FileAlreadyExists(_)) => Err(Error::generic("fixme")), // Ok(CommitResult::Conflict(self, commit_version)),
            Err(e) => Err(e),
        }
    }

    /// Set the operation that this transaction is performing. This string will be persisted in the
    /// commitInfo action in the log. <- TODO include this bit?
    pub fn operation(&mut self, operation: String) {
        self.operation = Some(operation);
    }

    /// WARNING: This is an unstable API and will likely change in the future.
    ///
    /// Add commit info to the transaction. This is commit-wide metadata that is written as the
    /// first action in the commit. The engine data passed here must have exactly one row, and we
    /// only read one column: `engineCommitInfo` which must be a map<string, string> encoding the
    /// metadata.
    ///
    /// Note that there are two main pieces of information included in commit info: (1) custom
    /// engine commit info (specified via this API) and (2) delta's own commit info which is
    /// effectively appended to the engine-specific commit info.
    ///
    /// If the engine elects to omit commit info, it can do so in two ways:
    /// 1. skip this API entirely - this will omit the commitInfo action from the commit (that is,
    ///    it will prevent the kernel from writing delta-specific commitInfo). This precludes usage
    ///    of table features which require commitInfo like in-commit timestamps.
    /// 2. pass a commit_info data chunk with one row of `engineCommitInfo` with an empty map. This
    ///    allows kernel to include delta-specific commit info, resulting in a commitInfo action in
    ///    the log, just without any engine-specific additions.
    pub fn commit_info(&mut self, commit_info: Box<dyn EngineData>) {
        self.commit_info = Some(commit_info);
    }
}

/// Result after committing a transaction. If 'committed', the version is the new version written
/// to the log. If 'conflict', the transaction is returned so the caller can resolve the conflict
/// (along with the version which conflicted).
pub enum CommitResult {
    /// The transaction was successfully committed at the version.
    Committed(Version),
    /// The transaction conflicted with an existing version (at the version given).
    Conflict(Transaction, Version),
}

// given the engine's commit info we want to create commitInfo action to commit (and append more actions to)
fn generate_commit_info(
    engine: &dyn Engine,
    operation: Option<&str>,
    engine_commit_info: Box<dyn EngineData>,
) -> DeltaResult<Box<dyn EngineData>> {
    if engine_commit_info.length() != 1 {
        return Err(Error::InvalidCommitInfo(format!(
            "Engine commit info should have exactly one row, found {}",
            engine_commit_info.length()
        )));
    }

    let commit_info_exprs = [
        // FIXME we should take a timestamp closer to commit time?
        Expression::literal(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time went backwards..?")
                .as_millis() as i64, // FIXME safe cast
        ),
        Expression::literal(operation.unwrap_or(UNKNOWN_OPERATION)),
        // Expression::struct_expr([Expression::null_literal(DataType::LONG)]),
        Expression::null_literal(DataType::Map(Box::new(MapType::new(
            DataType::STRING,
            DataType::STRING,
            true,
        )))),
        Expression::literal(format!("v{}", KERNEL_VERSION)),
        Expression::column("engineCommitInfo"),
    ];
    let commit_info_expr = Expression::struct_expr([Expression::struct_expr(commit_info_exprs)]);
    // TODO probably just create a static
    let commit_info_schema = get_log_schema().project_as_struct(&["commitInfo"])?;

    let engine_commit_info_schema = StructType::new(vec![StructField::new(
        "engineCommitInfo",
        MapType::new(DataType::STRING, DataType::STRING, true),
        false,
    )]);

    let commit_info_evaluator = engine.get_expression_handler().get_evaluator(
        engine_commit_info_schema.into(),
        commit_info_expr,
        commit_info_schema.into(),
    );

    commit_info_evaluator.evaluate(engine_commit_info.as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::arrow_expression::ArrowExpressionHandler;
    use crate::{ExpressionHandler, FileSystemClient, JsonHandler, ParquetHandler};

    use arrow::record_batch::RecordBatch;
    use arrow_schema::Schema as ArrowSchema;
    use arrow_schema::{DataType as ArrowDataType, Field};

    struct ExprEngine(Arc<dyn ExpressionHandler>);

    impl ExprEngine {
        fn new() -> Self {
            ExprEngine(Arc::new(ArrowExpressionHandler))
        }
    }

    impl Engine for ExprEngine {
        fn get_expression_handler(&self) -> Arc<dyn ExpressionHandler> {
            self.0.clone()
        }

        fn get_json_handler(&self) -> Arc<dyn JsonHandler> {
            unimplemented!()
        }

        fn get_parquet_handler(&self) -> Arc<dyn ParquetHandler> {
            unimplemented!()
        }

        fn get_file_system_client(&self) -> Arc<dyn FileSystemClient> {
            unimplemented!()
        }
    }

    // simple test for generating commit info
    #[test]
    fn test_generate_commit_info() -> DeltaResult<()> {
        let engine = ExprEngine::new();
        let engine_commit_info_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "engineCommitInfo",
            ArrowDataType::Map(
                Arc::new(Field::new(
                    "entries",
                    ArrowDataType::Struct(
                        vec![
                            Field::new("key", ArrowDataType::Utf8, false),
                            Field::new("value", ArrowDataType::Utf8, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            false,
        )]));

        use arrow_array::builder::StringBuilder;
        let key_builder = StringBuilder::new();
        let val_builder = StringBuilder::new();
        let names = arrow_array::builder::MapFieldNames {
            entry: "entries".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        };
        let mut builder =
            arrow_array::builder::MapBuilder::new(Some(names), key_builder, val_builder);
        builder.keys().append_value("engineInfo");
        builder.values().append_value("default engine");
        builder.append(true).unwrap();
        let array = builder.finish();

        let commit_info_batch =
            RecordBatch::try_new(engine_commit_info_schema, vec![Arc::new(array)])?;

        let actions = generate_commit_info(
            &engine,
            Some("test operation"),
            Box::new(ArrowEngineData::new(commit_info_batch)),
        )?;

        // TODO test it lol: more test cases, assert
        Ok(())
    }
}

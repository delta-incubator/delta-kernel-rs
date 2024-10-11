use std::sync::Arc;

use crate::actions::get_log_schema;
use crate::path::ParsedLogPath;
use crate::schema::{Schema, SchemaRef, StructType};
use crate::snapshot::Snapshot;
use crate::{DataType, Expression};
use crate::{DeltaResult, Engine, EngineData};

const KERNEL_VERSION: &str = env!("CARGO_PKG_VERSION");

/// A transaction represents an in-progress write to a table.
pub struct Transaction {
    read_snapshot: Arc<Snapshot>,
    commit_info: Option<EngineCommitInfo>,
}

// Since the engine can include any commit info it likes, we unify the data/schema pair as a single
// struct with Arc semantics.
#[derive(Clone)]
struct EngineCommitInfo {
    data: Arc<dyn EngineData>,
    schema: SchemaRef,
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
            commit_info: None,
        }
    }

    /// Consume the transaction and commit the in-progress write to the table.
    pub fn commit(self, engine: &dyn Engine) -> DeltaResult<CommitResult> {
        // step one: construct the iterator of actions we want to commit
        // note: only support commit_info right now.
        let (actions, _actions_schema): (
            Box<dyn Iterator<Item = Box<dyn EngineData>> + Send>,
            SchemaRef,
        ) = match self.commit_info {
            Some(ref engine_commit_info) => {
                let (actions, schema) = generate_commit_info(engine, engine_commit_info)?;
                (Box::new(std::iter::once(actions)), schema)
            }
            None => {
                (
                    // if there is no commit info, actions are empty iterator and schema is just our
                    // known log schema.
                    Box::new(std::iter::empty()),
                    get_log_schema().clone().into(),
                )
            }
        };

        // step two: set new commit version (current_version + 1) and path to write
        let commit_version = self.read_snapshot.version() + 1;
        let commit_path =
            ParsedLogPath::new_commit(self.read_snapshot.table_root(), commit_version)?
                .expect("valid commit path");
        assert!(
            commit_path.is_commit(),
            "commit_path should be a commit path"
        );

        // step three: commit the actions as a json file in the log
        let json_handler = engine.get_json_handler();
        match json_handler.write_json_file(&commit_path.location, Box::new(actions), false) {
            Ok(()) => Ok(CommitResult::Committed(commit_version)),
            Err(crate::error::Error::ObjectStore(object_store::Error::AlreadyExists {
                ..
            })) => Ok(CommitResult::Conflict(self, commit_version)),
            Err(e) => Err(e),
        }
    }

    /// Add commit info to the transaction. This is commit-wide metadata that is written as the
    /// first action in the commit. Note it is required in order to commit. If the engine does not
    /// require any commit info, pass an empty `EngineData`.
    pub fn commit_info(&mut self, commit_info: Box<dyn EngineData>, schema: Schema) {
        self.commit_info = Some(EngineCommitInfo {
            data: commit_info.into(),
            schema: schema.into(),
        });
    }
}

/// Result after committing a transaction. If 'committed', the version is the new version written
/// to the log. If 'conflict', the transaction is returned so the caller can resolve the conflict
/// (along with the version which conflicted).
pub enum CommitResult {
    /// The transaction was successfully committed at the version.
    Committed(crate::Version),
    /// The transaction conflicted with an existing version (at the version given).
    Conflict(Transaction, crate::Version),
}

// given the engine's commit info (data and schema as [EngineCommitInfo]) we want to create both:
// (1) the commitInfo action to commit (and append more actions to) and
// (2) the schema for the actions to commit (this is determined by the engine's commit info schema)
fn generate_commit_info(
    engine: &dyn Engine,
    engine_commit_info: &EngineCommitInfo,
) -> DeltaResult<(Box<dyn EngineData>, SchemaRef)> {
    use crate::actions::{
        ADD_NAME, COMMIT_INFO_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME, TRANSACTION_NAME,
    };

    if engine_commit_info.data.length() != 1 {
        return Err(crate::error::Error::InvalidCommitInfo(format!(
            "Engine commit info should have exactly one row, found {}",
            engine_commit_info.data.length()
        )));
    }

    let mut action_fields = get_log_schema().fields().collect::<Vec<_>>();
    let commit_info_field = action_fields
        .pop()
        .expect("last field is commit_info in action schema");
    let DataType::Struct(commit_info_schema) = commit_info_field.data_type() else {
        unreachable!("commit_info_field is a struct");
    };
    let commit_info_fields = commit_info_schema
        .fields()
        .chain(engine_commit_info.schema.fields())
        .cloned()
        .collect();
    let commit_info_schema = StructType::new(commit_info_fields);
    let action_fields =
        action_fields
            .into_iter()
            .cloned()
            .chain(std::iter::once(crate::schema::StructField::new(
                COMMIT_INFO_NAME,
                commit_info_schema,
                true,
            )));
    let action_schema = StructType::new(action_fields.collect());

    // nullable = true
    // println!("action_schema: {:#?}", action_schema);

    let commit_info_expr = std::iter::once(Expression::literal(format!("v{}", KERNEL_VERSION)))
        .chain(
            engine_commit_info
                .schema
                .fields()
                .map(|f| Expression::column(f.name())),
        );

    // generate expression with null for all the fields except the commit_info field, and
    // append the commit_info to the end.
    let commit_info_expr_fields = [
        ADD_NAME,
        REMOVE_NAME,
        METADATA_NAME,
        PROTOCOL_NAME,
        TRANSACTION_NAME,
    ]
    .iter()
    .map(|name| {
        Expression::null_literal(
            action_schema
                .field(name)
                .expect("find field in action schema")
                .data_type()
                .clone(),
        )
    })
    .chain(std::iter::once(Expression::struct_expr(commit_info_expr)));
    let commit_info_expr = Expression::struct_expr(commit_info_expr_fields);

    // commit info has arbitrary schema ex: {engineInfo: string, operation: string}
    // we want to bundle it up and put it in the commit_info field of the actions.
    let commit_info_evaluator = engine.get_expression_handler().get_evaluator(
        engine_commit_info.schema.clone(),
        commit_info_expr,
        action_schema.clone().into(),
    );

    let actions = commit_info_evaluator.evaluate(engine_commit_info.data.as_ref())?;
    Ok((actions, action_schema.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::arrow_data::ArrowEngineData;
    use arrow::array::{Int32Array, StringArray};
    use arrow::record_batch::RecordBatch;

    struct ExprEngine(Arc<dyn crate::ExpressionHandler>);

    impl ExprEngine {
        fn new() -> Self {
            ExprEngine(Arc::new(
                crate::engine::arrow_expression::ArrowExpressionHandler,
            ))
        }
    }

    impl Engine for ExprEngine {
        fn get_expression_handler(&self) -> Arc<dyn crate::ExpressionHandler> {
            self.0.clone()
        }

        fn get_json_handler(&self) -> Arc<dyn crate::JsonHandler> {
            unimplemented!()
        }

        fn get_parquet_handler(&self) -> Arc<dyn crate::ParquetHandler> {
            unimplemented!()
        }

        fn get_file_system_client(&self) -> Arc<dyn crate::FileSystemClient> {
            unimplemented!()
        }
    }

    // simple test for generating commit info
    #[test]
    fn test_generate_commit_info() {
        let engine = ExprEngine::new();
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("engine_info", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("operation", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("int", arrow_schema::DataType::Int32, true),
        ]));
        let data = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["test engine info"])),
                Arc::new(StringArray::from(vec!["append"])),
                Arc::new(Int32Array::from(vec![42])),
            ],
        );
        let engine_commit_info = EngineCommitInfo {
            data: Arc::new(ArrowEngineData::new(data.unwrap())),
            schema: Arc::new(schema.try_into().unwrap()),
        };
        let (actions, actions_schema) = generate_commit_info(&engine, &engine_commit_info).unwrap();

        // FIXME actual assertions
        assert_eq!(actions_schema.fields().collect::<Vec<_>>().len(), 6);
        let DataType::Struct(struct_type) = actions_schema.field("commitInfo").unwrap().data_type()
        else {
            unreachable!("commitInfo is a struct");
        };
        assert_eq!(struct_type.fields().collect::<Vec<_>>().len(), 4);
        assert_eq!(actions.length(), 1);
    }
}

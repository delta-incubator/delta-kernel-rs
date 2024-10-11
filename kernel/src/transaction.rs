use std::mem;
use std::sync::Arc;

use crate::actions::get_log_schema;
use crate::path::ParsedLogPath;
use crate::schema::{Schema, SchemaRef, StructField, StructType};
use crate::snapshot::Snapshot;
use crate::{DataType, Expression};
use crate::{DeltaResult, Engine, EngineData};

use itertools::chain;
use url::Url;

const KERNEL_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Get the expected schema for [`write_metadata`].
///
/// [`write_metadata`]: crate::transaction::Transaction::write_metadata
pub fn get_write_metadata_schema() -> &'static StructType {
    &crate::actions::WRITE_METADATA_SCHEMA
}

/// A transaction represents an in-progress write to a table.
pub struct Transaction {
    read_snapshot: Arc<Snapshot>,
    commit_info: Option<EngineCommitInfo>,
    write_metadata: Box<dyn Iterator<Item = Box<dyn EngineData>> + Send>,
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
            write_metadata: Box::new(std::iter::empty()),
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

        // TODO consider IntoIterator so we can have multiple write_metadata iterators (and return
        // self in the conflict case for retries)
        let adds = generate_adds(engine, self.write_metadata);
        let actions = chain(actions, adds);

        // step two: set new commit version (current_version + 1) and path to write
        let commit_version = self.read_snapshot.version() + 1;
        let commit_path =
            ParsedLogPath::new_commit(self.read_snapshot.table_root(), commit_version)?;

        // step three: commit the actions as a json file in the log
        let json_handler = engine.get_json_handler();
        match json_handler.write_json_file(&commit_path.location, Box::new(actions), false) {
            Ok(()) => Ok(CommitResult::Committed(commit_version)),
            Err(crate::error::Error::ObjectStore(object_store::Error::AlreadyExists {
                ..
            })) => Err(crate::Error::generic("fixme")), // Ok(CommitResult::Conflict(self, commit_version)),
            Err(e) => Err(e),
        }
    }

    /// Add commit info to the transaction. This is commit-wide metadata that is written as the
    /// first action in the commit.
    ///
    /// Note that there are two main pieces of information included in commit info: (1) custom
    /// engine commit info (specified via this API) and (2) delta's own commit info which is
    /// effectively appended to the engine-specific commit info.
    ///
    /// If the engine elects to omit commit info, it can do so in two ways:
    /// 1. skip this API entirely - this will omit the commitInfo action from the commit (that is,
    ///    it will prevent the kernel from writing delta-specific commitInfo). This precludes usage
    ///    of table features which require commitInfo like in-commit timestamps.
    /// 2. pass an empty commit_info data chunk - this will allow kernel to include delta-specific
    ///    commit info, resulting in a commitInfo action in the log, just without any
    ///    engine-specific additions.
    pub fn commit_info(&mut self, commit_info: Box<dyn EngineData>, schema: Schema) {
        self.commit_info = Some(EngineCommitInfo {
            data: commit_info.into(),
            schema: schema.into(),
        });
    }

    // Generate the logical-to-physical transform expression for this transaction. At the moment,
    // this is a transaction-wide expression.
    fn generate_logical_to_physical(&self) -> Expression {
        // for now, we just pass through all the columns except partition columns.
        // note this is _incorrect_ if table config deems we need partition columns.
        Expression::struct_expr(self.read_snapshot.schema().fields().filter_map(|f| {
            if self
                .read_snapshot
                .metadata()
                .partition_columns
                .contains(&f.name())
            {
                None
            } else {
                Some(Expression::column(f.name()))
            }
        }))
    }

    /// Get the write context for this transaction. At the moment, this is constant for the whole
    /// transaction.
    pub fn write_context(&self) -> WriteContext {
        let target_dir = self.read_snapshot.table_root();
        let snapshot_schema = self.read_snapshot.schema();
        let partition_cols = self.read_snapshot.metadata().partition_columns.clone();
        let logical_to_physical = self.generate_logical_to_physical();
        WriteContext::new(
            target_dir.clone(),
            Arc::new(snapshot_schema.clone()),
            partition_cols,
            logical_to_physical,
        )
    }

    /// Add write metadata about files to include in the transaction. This API can be called
    /// multiple times to add multiple iterators.
    ///
    /// TODO what is expected schema for the batches?
    pub fn add_write_metadata(
        &mut self,
        data: Box<dyn Iterator<Item = Box<dyn EngineData>> + Send>,
    ) {
        let write_metadata = mem::replace(&mut self.write_metadata, Box::new(std::iter::empty()));
        self.write_metadata = Box::new(chain(write_metadata, data));
    }
}

// this does something similar to adding top-level 'commitInfo' named struct. we should unify.
fn generate_adds(
    engine: &dyn Engine,
    write_metadata: Box<dyn Iterator<Item = Box<dyn EngineData>> + Send>,
) -> Box<dyn Iterator<Item = Box<dyn EngineData>> + Send> {
    let expression_handler = engine.get_expression_handler();
    let write_metadata_schema = crate::transaction::get_write_metadata_schema();
    let log_schema: DataType = DataType::struct_type(vec![StructField::new(
        crate::actions::ADD_NAME,
        write_metadata_schema.clone(),
        true,
    )]);

    Box::new(write_metadata.map(move |write_metadata_batch| {
        let adds_expr = Expression::struct_expr([Expression::struct_expr(
            write_metadata_schema
                .fields()
                .map(|f| Expression::column(f.name())),
        )]);
        let adds_evaluator = expression_handler.get_evaluator(
            write_metadata_schema.clone().into(),
            adds_expr,
            log_schema.clone(),
        );
        adds_evaluator
            .evaluate(write_metadata_batch.as_ref())
            .expect("fixme")
    }))
}
/// WriteContext is data derived from a [`Transaction`] that can be provided to writers in order to
/// write table data.
///
/// [`Transaction`]: struct.Transaction.html
pub struct WriteContext {
    pub target_dir: Url,
    pub schema: SchemaRef,
    pub partition_cols: Vec<String>,
    pub logical_to_physical: Expression,
}

impl WriteContext {
    fn new(
        target_dir: Url,
        schema: SchemaRef,
        partition_cols: Vec<String>,
        logical_to_physical: Expression,
    ) -> Self {
        WriteContext {
            target_dir,
            schema,
            partition_cols,
            logical_to_physical,
        }
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

    let mut action_schema = get_log_schema().clone();
    let commit_info_field = action_schema
        .fields
        .get_mut(COMMIT_INFO_NAME)
        .ok_or_else(|| crate::Error::missing_column(COMMIT_INFO_NAME))?;
    let DataType::Struct(mut commit_info_data_type) = commit_info_field.data_type().clone() else {
        return Err(crate::error::Error::internal_error(
            "commit_info_field is a struct",
        ));
    };
    commit_info_data_type.fields.extend(
        engine_commit_info
            .schema
            .fields()
            .map(|f| (f.name.clone(), f.clone())),
    );
    commit_info_field.data_type = DataType::Struct(commit_info_data_type);

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

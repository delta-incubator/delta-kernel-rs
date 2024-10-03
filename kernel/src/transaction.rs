use std::sync::Arc;

use crate::actions::get_log_schema;
use crate::expressions::Scalar;
use crate::schema::{Schema, StructType};
use crate::snapshot::Snapshot;
use crate::{DataType, Expression};
use crate::{DeltaResult, Engine, EngineData};

pub struct Transaction {
    read_snapshot: Arc<Snapshot>,
    commit_info: Option<CommitInfoData>,
}

pub struct CommitInfoData {
    data: Box<dyn EngineData>,
    schema: Schema,
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
    pub fn new(snapshot: impl Into<Arc<Snapshot>>) -> Self {
        Transaction {
            read_snapshot: snapshot.into(),
            commit_info: None,
        }
    }

    pub fn commit(self, engine: &dyn Engine) -> DeltaResult<CommitResult> {
        use crate::actions::{
            ADD_NAME, COMMIT_INFO_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME, TRANSACTION_NAME,
        };

        // step one: construct the iterator of actions we want to commit
        let action_schema = get_log_schema();

        let actions = self.commit_info.into_iter().map(|commit_info| {
            // expression to select all the columns
            let mut commit_info_expr = vec![Expression::literal("v0.3.1")];
            commit_info_expr.extend(
                commit_info
                    .schema
                    .fields()
                    .map(|f| Expression::column(f.name()))
                    .collect::<Vec<_>>(),
            );
            let commit_info_expr = Expression::Struct(vec![
                Expression::Literal(Scalar::Null(
                    action_schema.field(ADD_NAME).unwrap().data_type().clone(),
                )),
                Expression::Literal(Scalar::Null(
                    action_schema
                        .field(REMOVE_NAME)
                        .unwrap()
                        .data_type()
                        .clone(),
                )),
                Expression::Literal(Scalar::Null(
                    action_schema
                        .field(METADATA_NAME)
                        .unwrap()
                        .data_type()
                        .clone(),
                )),
                Expression::Literal(Scalar::Null(
                    action_schema
                        .field(PROTOCOL_NAME)
                        .unwrap()
                        .data_type()
                        .clone(),
                )),
                Expression::Literal(Scalar::Null(
                    action_schema
                        .field(TRANSACTION_NAME)
                        .unwrap()
                        .data_type()
                        .clone(),
                )),
                Expression::Struct(commit_info_expr),
            ]);

            // add the commit info fields to the action schema.
            // e.g. if engine's commit info is {engineInfo: string, operation: string}
            // then the 'commit_info' field in the actions will be:
            // {kernelVersion: string, engineInfo: string, operation: string}
            // let action_fields = action_schema
            //     .project_as_struct(&[
            //         ADD_NAME,
            //         REMOVE_NAME,
            //         METADATA_NAME,
            //         PROTOCOL_NAME,
            //         TRANSACTION_NAME,
            //     ])
            //     .unwrap()
            //     .fields();
            // let kernel_commit_info_fields = action_schema
            //     .project_as_struct(&[COMMIT_INFO_NAME])
            //     .unwrap()
            //     .fields();
            // let engine_commit_info_fields = commit_info.schema.fields();
            // let commit_info_fields = kernel_commit_info_fields.chain(engine_commit_info_fields);
            // let action_schema = StructType::new(
            //     std::iter::once(commit_info_fields)
            //         .chain(action_fields)
            //         .collect::Vec<_>()
            // );

            let mut action_fields = action_schema.fields().collect::<Vec<_>>();
            let commit_info_field = action_fields.pop().unwrap();
            let mut commit_info_fields =
                if let DataType::Struct(commit_info_schema) = commit_info_field.data_type() {
                    commit_info_schema.fields().collect::<Vec<_>>()
                } else {
                    unreachable!()
                };
            commit_info_fields.extend(commit_info.schema.fields());
            let commit_info_schema =
                StructType::new(commit_info_fields.into_iter().map(|f| f.clone()).collect());
            let mut action_fields = action_fields
                .into_iter()
                .map(|f| f.clone())
                .collect::<Vec<_>>();
            action_fields.push(crate::schema::StructField::new(
                COMMIT_INFO_NAME,
                commit_info_schema,
                true,
            ));
            let action_schema = StructType::new(action_fields);

            println!("commit_info schema: {:#?}", commit_info.schema);
            println!("action_schema: {:#?}", action_schema);

            // commit info has arbitrary schema ex: {engineInfo: string, operation: string}
            // we want to bundle it up and put it in the commit_info field of the actions.
            let commit_info_evaluator = engine.get_expression_handler().get_evaluator(
                commit_info.schema.into(),
                commit_info_expr,
                action_schema.into(),
            );
            commit_info_evaluator
                .evaluate(commit_info.data.as_ref())
                .unwrap()
        });

        // step two: figure out the commit version and path to write
        let commit_version = &self.read_snapshot.version() + 1;
        let commit_file_name = format!("{:020}", commit_version) + ".json";
        let commit_path = &self
            .read_snapshot
            .table_root
            .join("_delta_log/")?
            .join(&commit_file_name)?;

        // step three: commit the actions as a json file in the log
        let json_handler = engine.get_json_handler();

        json_handler.write_json_file(commit_path, Box::new(actions), false)?;
        Ok(CommitResult::Committed(commit_version))
    }

    /// Add commit info to the transaction. This is commit-wide metadata that is written as the
    /// first action in the commit. Note it is required in order to commit. If the engine does not
    /// require any commit info, pass an empty `EngineData`.
    pub fn commit_info(&mut self, commit_info: Box<dyn EngineData>, schema: Schema) {
        self.commit_info = Some(CommitInfoData {
            data: commit_info,
            schema,
        });
    }
}

pub enum CommitResult {
    Committed(crate::Version),
}

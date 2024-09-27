use std::mem;
use std::sync::Arc;

// use delta_kernel_derive::Schema;
use itertools::chain;
use url::Url;

use crate::schema::SchemaRef;
use crate::schema::{DataType, StructField};
use crate::snapshot::Snapshot;
use crate::{DeltaResult, Engine, EngineData, Expression, Version};

// derive schema?
#[derive(Debug)]
pub struct WriteContext {
    pub target_directory: Url,
    snapshot_schema: SchemaRef,
    partition_columns: Vec<String>,
}

impl WriteContext {
    fn new(
        target_directory: &Url,
        snapshot_schema: SchemaRef,
        partition_columns: Vec<String>,
    ) -> Self {
        Self {
            target_directory: target_directory.clone(),
            snapshot_schema,
            partition_columns,
        }
    }

    // or should we have this spit out an expression that the engine can apply themselves??
    pub fn transform_to_physical(
        &self,
        engine: &dyn Engine,
        data: &dyn EngineData,
    ) -> DeltaResult<Box<dyn EngineData>> {
        // transform logical data to physical data
        // 1. remove partition columns (or leave them for iceberg compat)
        // 2. TODO column mapping
        // 3. LATER generated columns, default values, row ids

        let fields = self
            .snapshot_schema
            .fields()
            .filter_map(|f| {
                if self.partition_columns.contains(&f.name().to_string()) {
                    None
                } else {
                    Some(Expression::Column(f.name().to_string()))
                }
            })
            .collect();
        let expr = Expression::Struct(fields);
        let cols = self
            .snapshot_schema
            .fields()
            .filter_map(|f| {
                if self.partition_columns.contains(&f.name().to_string()) {
                    None
                } else {
                    Some(f.name())
                }
            })
            .collect::<Vec<_>>();
        let physical_schema = self
            .snapshot_schema
            .project(&cols)
            .expect("schema projection");
        engine
            .get_expression_handler()
            .get_evaluator(self.snapshot_schema.clone(), expr, physical_schema.into())
            .evaluate(data)
    }
}

pub struct TransactionBuilder {
    table_location: Url,
}

impl TransactionBuilder {
    pub fn new(table_location: Url) -> Self {
        Self { table_location }
    }

    /// create the transaction. reads the latest snapshot of the table to get table metadata.
    pub fn build(self, engine: &dyn Engine) -> DeltaResult<Transaction> {
        let latest_snapshot = Snapshot::try_new(self.table_location.clone(), engine, None)?;
        Ok(Transaction::new(latest_snapshot))
    }
}

/// A transaction is an in-progress write operation on a Delta table. It provides a consistent view
/// of the table during the transaction.
pub struct Transaction {
    read_snapshot: Arc<Snapshot>,
    commit_info: Option<Box<dyn EngineData>>,
    // iterator of write metadata
    write_metadata: Box<dyn Iterator<Item = Box<dyn EngineData>> + Send>,
}

/// Result of committing a transaction.
// TODO should we expose this to the user or handle retries ourselves and only expose true failure?
pub enum CommitResult {
    /// Successfully committed the transaction at version `Version`
    Committed(Version),
    /// Nothing to commit
    NoCommit,
    /// Failed transaction but can be retried with the latest version + 1
    // or should we keep a retry_version which is latest_version + 1?
    Retry {
        latest_version: Version,
        transaction: Transaction,
    },
    /// The commit failed due to a logical conflict and must undergo some form of 'replay' before
    /// it can be retried.
    Failed {
        latest_version: Version,
        conflict: CommitConflict,
        transaction: Transaction,
    },
}

pub enum CommitConflict {
    ConcurrentAppend,
    ConcurrentDelete,
    ConcurrentUpdate,
    ConcurrentMetadata,
    ConcurrentSchema,
    ConcurrentTable,
}

impl Transaction {
    pub fn new(snapshot: impl Into<Arc<Snapshot>>) -> Self {
        Self {
            read_snapshot: snapshot.into(),
            write_metadata: Box::new(std::iter::empty::<Box<dyn EngineData>>()),
            commit_info: None,
        }
    }

    /// get the write context for this transaction
    // should this be WriteContext or DeltaResult<Box<dyn EngineData>>
    // TODO need engine?, _engine: &dyn Engine
    pub fn write_context(&self) -> WriteContext {
        let target_dir = self.read_snapshot.table_root();
        let snapshot_schema = self.read_snapshot.schema();
        let partition_cols = self.read_snapshot.metadata().partition_columns.clone();
        WriteContext::new(
            target_dir,
            Arc::new(snapshot_schema.clone()),
            partition_cols,
        )
    }

    /// add write metadata to the transaction, where the metadata has specific schema (link)
    /// for now this will overwrite any previous write metadata
    /// TODO this should actually stream the data
    pub fn add_write_metadata(
        &mut self,
        data: Box<dyn Iterator<Item = Box<dyn EngineData>> + Send>,
    ) {
        let write_metadata = mem::replace(&mut self.write_metadata, Box::new(std::iter::empty()));
        self.write_metadata = Box::new(chain(write_metadata, data));
    }

    /// Add commit info to the transaction. This is commit-wide metadata that is written as the
    /// first action in the commit. Note it is required in order to commit. If the engine does not
    /// require any commit info, pass an empty `EngineData`.
    pub fn commit_info(&mut self, commit_info: Box<dyn EngineData>) {
        self.commit_info = Some(commit_info);
    }

    fn generate_commit_info(
        &self,
        action_schema: crate::schema::Schema,
        engine: &dyn Engine,
    ) -> Box<dyn EngineData> {
        // augment commit info so that it looks like: {"commitInfo": {<engine's commit info>}}
        let expr = Expression::Struct(vec![
            Expression::Literal(crate::expressions::Scalar::Null(add_schema())),
            Expression::Struct(vec![
                Expression::Column("engineInfo".to_owned()),
                Expression::literal("delta_kernel v0.3.1".to_owned()),
            ]),
        ]);

        let engine_commit_info_schema = Box::new(crate::schema::StructType::new(vec![
            StructField::new("engineInfo", DataType::STRING, true),
            // StructField::new("kernelVersion", DataType::STRING, true),
        ]));
        // let commit_info_schema = Arc::new(crate::schema::StructType::new(vec![StructField::new(
        //     "commitInfo",
        //     DataType::Struct(engine_commit_info_schema.clone()),
        //     true,
        // )]));

        println!("action schema {:#?}", action_schema);

        let r = engine
            .get_expression_handler()
            .get_evaluator(
                Arc::from(engine_commit_info_schema),
                expr,
                action_schema.into(),
            )
            .evaluate(
                self.commit_info
                    .as_ref()
                    .expect("commit info not set")
                    .as_ref(),
            )
            .unwrap();

        // convert r to ArrowEngineData
        let a = r
            .into_any()
            .downcast::<crate::engine::arrow_data::ArrowEngineData>()
            .unwrap();
        println!("-------------------------------------");
        println!("SCHEMA {:#?}", a.record_batch().schema());
        println!("-------------------------------------");
        let s = a.record_batch().schema();
        let mut i = s.fields().into_iter();
        let f1 = i.next();
        let f2 = i.next();
        // make everything nullable in f2
        let f2: Option<_> = f2.map(|f| {
            let f = <arrow_schema::Field as Clone>::clone(f).with_nullable(true);
            // struct with engineInfo and kernelVersion
            f.with_data_type(arrow_schema::DataType::Struct(
                vec![
                    arrow_schema::Field::new("engineInfo", arrow_schema::DataType::Utf8, true),
                    arrow_schema::Field::new("kernelVersion", arrow_schema::DataType::Utf8, true),
                ]
                .into(),
            ))
        });
        let schema =
            arrow_schema::Schema::new(vec![f1.unwrap().clone(), f2.unwrap().clone().into()]);
        let rb = a.record_batch();
        let rb = rb.clone().with_schema(schema.into());
        Box::new(crate::engine::arrow_data::ArrowEngineData::new(rb.unwrap()))
    }

    // TODO conflict resolution
    #[must_use]
    pub fn commit(self, engine: &dyn Engine) -> DeltaResult<CommitResult> {
        let json_handler = engine.get_json_handler();
        let commit_version = &self.read_snapshot.version() + 1;
        let commit_file_name = format!("{:020}", commit_version) + ".json";
        let commit_path = &self
            .read_snapshot
            .table_root
            .join("_delta_log/")?
            .join(&commit_file_name)?;
        let action_schema = schema();
        let commit_info = self.generate_commit_info(action_schema.clone(), engine);
        // rewrite write_metadata to be add file actions
        let commit_info_type = DataType::Struct(Box::new(crate::schema::StructType::new(vec![
            StructField::new("engineInfo", DataType::STRING, true),
            StructField::new("kernelVersion", DataType::STRING, true),
        ])));
        let write_expr = Expression::Struct(vec![
            Expression::Column("add".to_owned()),
            Expression::Literal(crate::expressions::Scalar::Null(commit_info_type)),
        ]);
        let expression = Box::new(engine.get_expression_handler().get_evaluator(
            schema().into(),
            // Arc::new(crate::schema::StructType::new(vec![
            //     StructField::new("add", DataType::Struct(Box::new(schema())), true),
            //     StructField::new(
            //         "commitInfo",
            //         DataType::Struct(Box::new(crate::schema::StructType::new(vec![
            //             StructField::new("engineInfo", DataType::STRING, true),
            //             StructField::new("kernelVersion", DataType::STRING, true),
            //         ]))),
            //         true,
            //     ),
            // ])),
            write_expr,
            action_schema.into(),
        ));
        // FIXME leak
        let expression = Box::leak(expression);
        let write_metadata = self
            .write_metadata
            .map(|write_meta| expression.evaluate(write_meta.as_ref()).unwrap());

        let actions = Box::new(chain(std::iter::once(commit_info), write_metadata));
        json_handler.write_json(commit_path, actions)?;
        Ok(CommitResult::Committed(commit_version))
    }
}

fn add_schema() -> crate::schema::DataType {
    let path_field = StructField::new("path", DataType::STRING, false);
    let size_field = StructField::new("size", DataType::LONG, false);
    let partition_field = StructField::new(
        "partitionValues",
        DataType::Map(Box::new(crate::schema::MapType::new(
            DataType::STRING,
            DataType::STRING,
            true,
        ))),
        false,
    );
    let data_change_field = StructField::new("dataChange", DataType::BOOLEAN, false);
    let modification_time_field = StructField::new("modificationTime", DataType::LONG, false);
    DataType::Struct(Box::new(crate::schema::StructType::new(vec![
        path_field.clone(),
        size_field.clone(),
        partition_field.clone(),
        data_change_field.clone(),
        modification_time_field.clone(),
    ])))
}

fn schema() -> crate::schema::Schema {
    let path_field = StructField::new("path", DataType::STRING, false);
    let size_field = StructField::new("size", DataType::LONG, false);
    let partition_field = StructField::new(
        "partitionValues",
        DataType::Map(Box::new(crate::schema::MapType::new(
            DataType::STRING,
            DataType::STRING,
            true,
        ))),
        false,
    );
    let data_change_field = StructField::new("dataChange", DataType::BOOLEAN, false);
    let modification_time_field = StructField::new("modificationTime", DataType::LONG, false);

    crate::schema::Schema::new(vec![
        StructField::new(
            "add",
            DataType::Struct(Box::new(crate::schema::StructType::new(vec![
                path_field.clone(),
                size_field.clone(),
                partition_field.clone(),
                data_change_field.clone(),
                modification_time_field.clone(),
            ]))),
            true,
        ),
        StructField::new(
            "commitInfo",
            DataType::Struct(Box::new(crate::schema::StructType::new(vec![
                StructField::new("engineInfo", DataType::STRING, true),
                StructField::new("kernelVersion", DataType::STRING, true),
            ]))),
            true,
        ),
    ])
}

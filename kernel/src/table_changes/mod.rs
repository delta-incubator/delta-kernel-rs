//! In-memory representation of a change data feed table.

use std::{collections::HashMap, sync::Arc};
<<<<<<< HEAD
=======

use table_changes_scan::TableChangesScanBuilder;
use url::Url;
>>>>>>> 4d81c52 (Basic iteration for CDF)

use crate::{
    actions::{Metadata, Protocol},
    features::ColumnMappingMode,
    log_segment::{LogSegment, LogSegmentBuilder},
    path::AsUrl,
    scan::{get_state_info, state::DvInfo, ColumnType},
    schema::{Schema, SchemaRef, StructType},
    snapshot::Snapshot,
<<<<<<< HEAD
    DeltaResult, Engine, EngineData, Error, ExpressionRef, Version,
=======
    DeltaResult, Engine, EngineData, Error, Version,
>>>>>>> 4d81c52 (Basic iteration for CDF)
};
use url::Url;

<<<<<<< HEAD
pub type TableChangesScanData = (Box<dyn EngineData>, Vec<bool>, Arc<HashMap<String, DvInfo>>);
=======
mod metadata_scanner;
mod replay_scanner;
pub mod table_changes_scan;
>>>>>>> 4d81c52 (Basic iteration for CDF)

pub type TableChangesScanData = (Box<dyn EngineData>, Vec<bool>, Arc<HashMap<String, String>>);

static CDF_ENABLE_FLAG: &str = "delta.enableChangeDataFeed";

#[derive(Debug)]
pub struct TableChanges {
    pub snapshot: Snapshot,
    #[allow(unused)]
    pub(crate) log_segment: LogSegment,
    pub schema: Schema,
    pub version: Version,
    pub metadata: Metadata,
    pub protocol: Protocol,
    pub(crate) column_mapping_mode: ColumnMappingMode,
    pub table_root: Url,
}

impl TableChanges {
    pub fn try_new(
        table_root: Url,
        engine: &dyn Engine,
        start_version: Version,
        end_version: Option<Version>,
    ) -> DeltaResult<Self> {
        let start_snapshot =
            Snapshot::try_new(table_root.as_url().clone(), engine, Some(start_version))?;
        let end_snapshot = Snapshot::try_new(table_root.as_url().clone(), engine, end_version)?;

        let start_flag = start_snapshot.metadata().configuration.get(CDF_ENABLE_FLAG);
        let end_flag = end_snapshot.metadata().configuration.get(CDF_ENABLE_FLAG);

        // Verify CDF is enabled at the beginning and end of the interval
        let is_cdf_enabled = |flag_res: Option<&String>| flag_res.is_some_and(|val| val == "true");
        if !is_cdf_enabled(start_flag) || !is_cdf_enabled(end_flag) {
            return Err(Error::TableChangesDisabled(start_version, end_version));
        }

        // Get a log segment for the CDF range
        let fs_client = engine.get_file_system_client();
        let mut builder = LogSegmentBuilder::new(fs_client, &table_root);
        builder = builder.with_start_version(start_version);
        if let Some(end_version) = end_version {
            builder = builder.with_end_version(end_version);
        }
        builder = builder
            .with_no_checkpoint_files()
            .with_in_order_commit_files();
        let log_segment = builder.build()?;

        Ok(TableChanges {
            snapshot: start_snapshot,
            log_segment,
            schema: end_snapshot.schema().clone(),
            column_mapping_mode: end_snapshot.column_mapping_mode,
            version: end_snapshot.version(),
            protocol: end_snapshot.protocol().clone(),
            metadata: end_snapshot.metadata().clone(),
            table_root,
        })
    }
    pub fn into_scan_builder(self) -> TableChangesScanBuilder {
        TableChangesScanBuilder::new(self)
    }
}

/// Builder to read the `TableChanges` of a table.
pub struct TableChangesScanBuilder {
    table_changes: Arc<TableChanges>,
    schema: Option<SchemaRef>,
    predicate: Option<ExpressionRef>,
}

impl TableChangesScanBuilder {
    /// Create a new [`TableChangesScanBuilder`] instance.
    pub fn new(table_changes: impl Into<Arc<TableChanges>>) -> Self {
        Self {
            table_changes: table_changes.into(),
            schema: None,
            predicate: None,
        }
    }

    /// Provide [`Schema`] for columns to select from the [`TableChanges`].
    ///
    /// A table with columns `[a, b, c]` could have a scan which reads only the first
    /// two columns by using the schema `[a, b]`.
    ///
    /// [`Schema`]: crate::schema::Schema
    /// [`TableChanges`]: crate::table_changes:TableChanges:
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Optionally provide a [`SchemaRef`] for columns to select from the [`TableChanges`]. See
    /// [`TableChangesScanBuilder::with_schema`] for details. If `schema_opt` is `None` this is a no-op.
    pub fn with_schema_opt(self, schema_opt: Option<SchemaRef>) -> Self {
        match schema_opt {
            Some(schema) => self.with_schema(schema),
            None => self,
        }
    }

    /// Optionally provide an expression to filter rows. For example, using the predicate `x <
    /// 4` to return a subset of the rows in the scan which satisfy the filter. If `predicate_opt`
    /// is `None`, this is a no-op.
    ///
    /// NOTE: The filtering is best-effort and can produce false positives (rows that should should
    /// have been filtered out but were kept).
    pub fn with_predicate(mut self, predicate: impl Into<Option<ExpressionRef>>) -> Self {
        self.predicate = predicate.into();
        self
    }

    /// Build the [`TableChangesScan`].
    ///
    /// This does not scan the table at this point, but does do some work to ensure that the
    /// provided schema make sense, and to prepare some metadata that the scan will need.  The
    /// [`TableChangesScan`] type itself can be used to fetch the files and associated metadata required to
    /// perform actual data reads.
    pub fn build(self) -> DeltaResult<TableChangesScan> {
        // if no schema is provided, use snapshot's entire schema (e.g. SELECT *)
        let logical_schema = self
            .schema
            .unwrap_or_else(|| self.table_changes.schema.clone().into());
        let (all_fields, read_fields, have_partition_cols) = get_state_info(
            logical_schema.as_ref(),
            &self.table_changes.metadata.partition_columns,
            self.table_changes.column_mapping_mode,
        )?;
        let physical_schema = Arc::new(StructType::new(read_fields));
        Ok(TableChangesScan {
            table_changes: self.table_changes,
            logical_schema,
            physical_schema,
            predicate: self.predicate,
            all_fields,
            have_partition_cols,
        })
    }
}

/// The result of building a [`TableChanges`] scan over a table. This can be used to get a change
/// data feed from the table
#[allow(unused)]
pub struct TableChangesScan {
    table_changes: Arc<TableChanges>,
    logical_schema: SchemaRef,
    physical_schema: SchemaRef,
    predicate: Option<ExpressionRef>,
    all_fields: Vec<ColumnType>,
    have_partition_cols: bool,
}

#[cfg(test)]
mod tests {
    use crate::Error;
    use crate::{engine::sync::SyncEngine, Table};

    #[test]
    fn get_cdf_ranges() {
        let path = "./tests/data/table-with-cdf";
        let engine = Box::new(SyncEngine::new());
        let table = Table::try_from_uri(path).unwrap();

        let valid_ranges = [(0, Some(1)), (0, Some(0)), (1, Some(1))];
        for (start_version, end_version) in valid_ranges {
            assert!(table
                .table_changes(engine.as_ref(), start_version, end_version)
                .is_ok())
        }

        let invalid_ranges = [
            (0, None),
            (0, Some(2)),
            (1, Some(2)),
            (2, None),
            (2, Some(2)),
        ];
        for (start_version, end_version) in invalid_ranges {
            let res = table.table_changes(engine.as_ref(), start_version, end_version);
            assert!(matches!(res, Err(Error::TableChangesDisabled(_, _))))
        }
    }
}

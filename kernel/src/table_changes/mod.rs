//! In-memory representation of a change data feed table.

use std::{collections::HashMap, sync::Arc};

use crate::{
    actions::{Metadata, Protocol},
    features::ColumnMappingMode,
    log_segment::{LogSegment, LogSegmentBuilder},
    path::AsUrl,
    scan::{state::DvInfo, ColumnType},
    schema::{Schema, SchemaRef},
    snapshot::Snapshot,
    DeltaResult, Engine, EngineData, Error, ExpressionRef, Version,
};
use table_changes_scan::TableChangesScanBuilder;
use url::Url;

pub type TableChangesScanData = (Box<dyn EngineData>, Vec<bool>, Arc<HashMap<String, DvInfo>>);
mod metadata_scanner;
mod replay_scanner;
mod state;
pub mod table_changes_scan;

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

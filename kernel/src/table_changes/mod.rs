//! In-memory representation of a change data feed table.

use std::{collections::HashMap, sync::Arc};

use crate::{
    actions::{Metadata, Protocol},
    log_segment::LogSegment,
    path::AsUrl,
    scan::state::DvInfo,
    schema::{DataType, Schema, StructField, StructType},
    snapshot::Snapshot,
    table_features::ColumnMappingMode,
    DeltaResult, Engine, EngineData, Error, Version,
};
use table_changes_scan::TableChangesScanBuilder;
use url::Url;
mod data_read;
mod replay_scanner;
mod state;
pub mod table_changes_scan;

pub type TableChangesScanData = (Box<dyn EngineData>, Vec<bool>, Arc<HashMap<String, DvInfo>>);

static CDF_GENERATED_COLUMNS: [&str; 3] = ["_change_type", "_commit_version", "_commit_timestamp"];
static CDF_ENABLE_FLAG: &str = "delta.enableChangeDataFeed";

#[derive(Debug)]
pub struct TableChanges {
    #[allow(unused)]
    pub(crate) log_segment: LogSegment,
    pub schema: Schema,
    pub start_version: Version,
    pub end_version: Version,
    pub metadata: Metadata,
    pub protocol: Protocol,
    pub column_mapping_mode: ColumnMappingMode,
    pub table_root: Url,
    pub output_schema: Schema,
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
        let log_segment = LogSegment::for_table_changes(
            fs_client.as_ref(),
            &table_root,
            start_version,
            end_version,
        )?;

        let output_schema = Self::transform_logical_schema(end_snapshot.schema());

        Ok(TableChanges {
            log_segment,
            schema: output_schema.clone(),
            column_mapping_mode: end_snapshot.column_mapping_mode,
            start_version,
            end_version: end_snapshot.version(),
            protocol: end_snapshot.protocol().clone(),
            metadata: end_snapshot.metadata().clone(),
            table_root,
            output_schema,
        })
    }

    fn transform_logical_schema(schema: &Schema) -> Schema {
        let cdf_fields = [
            StructField::new("_change_type", DataType::STRING, false),
            StructField::new("_commit_version", DataType::LONG, false),
            StructField::new("_commit_timestamp", DataType::TIMESTAMP, false),
        ];
        StructType::new(schema.fields().cloned().chain(cdf_fields))
    }
    pub fn into_scan_builder(self) -> TableChangesScanBuilder {
        TableChangesScanBuilder::new(self)
    }
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
            let table_changes = table
                .table_changes(engine.as_ref(), start_version, end_version)
                .unwrap();
            assert_eq!(table_changes.start_version, start_version);
            if let Some(end_version) = end_version {
                assert_eq!(table_changes.end_version, end_version);
            }
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

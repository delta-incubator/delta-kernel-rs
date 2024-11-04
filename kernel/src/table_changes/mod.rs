//! In-memory representation of a change data feed table.

use table_changes_scan::TableChangesScan;
use url::Url;

use crate::{
    actions::{Metadata, Protocol},
    features::ColumnMappingMode,
    log_segment::{LogSegment, LogSegmentBuilder},
    path::AsUrl,
    schema::Schema,
    snapshot::Snapshot,
    DeltaResult, Engine, Error, Version,
};

pub mod table_changes_scan;

static CDF_ENABLE_FLAG: &str = "delta.enableChangeDataFeed";

#[derive(Debug)]
pub struct TableChanges {
    pub snapshot: Snapshot,
    pub log_segment: LogSegment,
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
        let is_valid_flag = |flag_res: Option<&String>| flag_res.is_some_and(|val| val == "true");
        if !is_valid_flag(start_flag) || !is_valid_flag(end_flag) {
            return Err(Error::TableChangesDisabled(start_version, end_version));
        }

        let fs_client = engine.get_file_system_client();
        let mut builder = LogSegmentBuilder::new(fs_client, &table_root);
        builder = builder.with_start_version(start_version);
        if let Some(end_version) = end_version {
            builder = builder.with_end_version(end_version);
        }
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
    pub fn into_scan_builder(self) -> TableChangesScan {
        todo!()
    }
}

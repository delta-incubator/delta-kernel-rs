//! In-memory representation of a change data feed table.

use std::sync::Arc;

use url::Url;

use crate::{
    features::ColumnMappingMode,
    log_segment::{LogSegment, LogSegmentBuilder},
    path::AsUrl,
    scan::{ScanBuilder, Scannable},
    schema::Schema,
    snapshot::Snapshot,
    DeltaResult, Engine, Version,
};

static CDF_ENABLE_FLAG: &str = "delta.enableChangeDataFeed";

#[derive(Debug)]
pub struct TableChanges {
    snapshot: Snapshot,
    cdf_range: LogSegment,
    schema: Schema,
    column_mapping_mode: ColumnMappingMode,
}

impl TableChanges {
    pub fn try_new(
        table_root: Url,
        engine: &dyn Engine,
        start_version: Version,
        end_version: Option<Version>,
    ) -> DeltaResult<Self> {
        let fs_client = engine.get_file_system_client();
        let snapshot = Snapshot::try_new(table_root.as_url().clone(), engine, Some(start_version))?;
        let mut builder = LogSegmentBuilder::new(fs_client, &table_root);
        builder = builder.with_start_version(start_version);
        if let Some(end_version) = end_version {
            builder = builder.with_start_version(end_version);
        }
        let log_segment = builder.build()?;

        Ok(TableChanges {
            snapshot,
            cdf_range: log_segment,
            column_mapping_mode: end_snapshot.column_mapping_mode,
        })
    }
    pub fn into_scan_builder(self) -> ScanBuilder {
        ScanBuilder::new(Arc::new(self) as Arc<dyn Scannable>)
    }
}

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
    DeltaResult, Engine, Error, Version,
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
        let start_snapshot =
            Snapshot::try_new(table_root.as_url().clone(), engine, Some(start_version))?;

        // Get the end snapshot to fetch schema
        let end_snapshot = Snapshot::try_new(table_root.as_url().clone(), engine, end_version)?;
        let schema = end_snapshot.schema();

        let mut builder = LogSegmentBuilder::new(fs_client, &table_root);
        builder = builder.with_start_version(start_version);
        if let Some(end_version) = end_version {
            builder = builder.with_start_version(end_version);
        }
        builder = builder.set_omit_checkpoints();
        let log_segment = builder.build()?;

        match (
            start_snapshot.metadata().configuration.get(CDF_ENABLE_FLAG),
            end_snapshot.metadata().configuration.get(CDF_ENABLE_FLAG),
        ) {
            (Some(start_flag), Some(end_flag)) if start_flag == "true" && end_flag == "true" => {}
            _ => {
                return Err(Error::InvalidChangeDataFeedRange(
                    start_version,
                    end_version,
                ))
            }
        }

        Ok(TableChanges {
            schema: schema.clone(),
            snapshot: start_snapshot,
            cdf_range: log_segment,
            column_mapping_mode: end_snapshot.column_mapping_mode,
        })
    }
    pub fn into_scan_builder(self) -> ScanBuilder {
        ScanBuilder::new(Arc::new(self) as Arc<dyn Scannable>)
    }
}

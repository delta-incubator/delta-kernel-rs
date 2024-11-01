//! In-memory representation of a change data feed table.

use url::Url;

use crate::{
    log_segment::{LogSegment, LogSegmentBuilder},
    path::AsUrl,
    schema::Schema,
    snapshot::Snapshot,
    DeltaResult, Engine, Version,
};

#[derive(Debug)]
pub struct TableChanges {
    snapshot: Snapshot,
    cdf_range: LogSegment,
    schema: Schema,
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
        })
    }
}

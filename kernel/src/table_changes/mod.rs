//! Provides an API to read the table's change data feed between two versions.

use url::Url;

use crate::log_segment::LogSegment;
use crate::path::AsUrl;
use crate::schema::Schema;
use crate::snapshot::Snapshot;
use crate::table_features::ColumnMappingMode;
use crate::{DeltaResult, Engine, Error, Version};

/// Represents a call to read the Change Data Feed between two versions of a table. The schema of
/// `TableChanges` will be the schema of the table with three additional columns:
/// - `_change_type`: String representing the type of change that for that commit. This may be one
///   of `delete`, `insert`, `update_preimage`, or `update_postimage`.
/// - `_commit_version`: Long representing the commit the change occurred in.
/// - `_commit_timestamp`: Time at which the commit occurred. If In-commit timestamps is enabled,
///   this is retrieved from the [`CommitInfo`] action. Otherwise, the timestamp is the same as the
///   commit file's modification timestamp.
///
/// For more details, see the following sections of the protocol:
/// - [Add CDC File](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-cdc-file)
/// - [Change Data Files](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#change-data-files).
///
/// [`CommitInfo`]: crate::actions::CommitInfo
#[derive(Debug)]
pub struct TableChanges {
    pub log_segment: LogSegment,
    table_root: Url,
    end_snapshot: Snapshot,
    start_version: Version,
}

impl TableChanges {
    /// Creates a new [`TableChanges`] instance for the given version range. This function checks
    /// these two properties:
    /// - The change data feed table feature must be enabled in both the start or end versions.
    /// - The schema at the start and end versions are the same.
    ///
    /// Note that this does not check that change data feed is enabled for every commit in the
    /// range. It also does not check that the schema remains the same for the entire range.
    ///
    /// # Parameters
    /// - `table_root`: url pointing at the table root (where `_delta_log` folder is located)
    /// - `engine`: Implementation of [`Engine`] apis.
    /// - `start_version`: The start version of the change data feed
    /// - `end_version`: The end version of the change data feed. If this is none, this defaults to
    ///   the newest table version.
    pub fn try_new(
        table_root: Url,
        engine: &dyn Engine,
        start_version: Version,
        end_version: Option<Version>,
    ) -> DeltaResult<Self> {
        let start_snapshot =
            Snapshot::try_new(table_root.as_url().clone(), engine, Some(start_version))?;
        let end_snapshot = Snapshot::try_new(table_root.as_url().clone(), engine, end_version)?;

        // Verify CDF is enabled at the beginning and end of the interval
        let is_cdf_enabled = |snapshot: &Snapshot| {
            static ENABLE_CDF_FLAG: &str = "delta.enableChangeDataFeed";
            snapshot
                .metadata()
                .configuration
                .get(ENABLE_CDF_FLAG)
                .is_some_and(|val| val == "true")
        };
        if !is_cdf_enabled(&start_snapshot) {
            return Err(Error::table_changes_disabled(start_version));
        } else if !is_cdf_enabled(&end_snapshot) {
            return Err(Error::table_changes_disabled(end_snapshot.version()));
        }
        if start_snapshot.schema() != end_snapshot.schema() {
            return Err(Error::generic(
                "Failed to build TableChanges: Start and end version schemas are different.",
            ));
        }

        let log_root = table_root.join("_delta_log/")?;
        let log_segment = LogSegment::for_table_changes(
            engine.get_file_system_client().as_ref(),
            log_root,
            start_version,
            end_version,
        )?;

        Ok(TableChanges {
            table_root,
            end_snapshot,
            log_segment,
            start_version,
        })
    }
    pub fn start_version(&self) -> Version {
        self.start_version
    }
    /// The end version of the `TableChanges`. If no end_version was specified in
    /// [`TableChanges::try_new`], this returns the newest version as of the call to `try_new`.
    pub fn end_version(&self) -> Version {
        self.log_segment.end_version
    }
    /// The logical schema of the change data feed. For details on the shape of the schema, see
    /// [`TableChanges`].
    pub fn schema(&self) -> &Schema {
        self.end_snapshot.schema()
    }
    pub fn table_root(&self) -> &Url {
        &self.table_root
    }
    #[allow(unused)]
    pub(crate) fn partition_columns(&self) -> &Vec<String> {
        &self.end_snapshot.metadata().partition_columns
    }
    #[allow(unused)]
    pub(crate) fn column_mapping_mode(&self) -> &ColumnMappingMode {
        &self.end_snapshot.column_mapping_mode
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::sync::SyncEngine;
    use crate::Error;
    use crate::Table;

    #[test]
    fn test_enable_cdf_flag() {
        let path = "./tests/data/table-with-cdf";
        let engine = Box::new(SyncEngine::new());
        let table = Table::try_from_uri(path).unwrap();

        let valid_ranges = [(0, 1), (0, 0), (1, 1)];
        for (start_version, end_version) in valid_ranges {
            let table_changes = table
                .table_changes(engine.as_ref(), start_version, end_version)
                .unwrap();
            assert_eq!(table_changes.start_version, start_version);
            assert_eq!(table_changes.end_version(), end_version);
        }

        let invalid_ranges = [(0, Some(2)), (1, Some(2)), (2, Some(2))];
        for (start_version, end_version) in invalid_ranges {
            let res = table.table_changes(engine.as_ref(), start_version, end_version);
            assert!(matches!(res, Err(Error::TableChangesDisabled(_))))
        }
    }
    #[test]
    fn test_schema_evolution_fails() {
        let path = "./tests/data/table-with-cdf";
        let engine = Box::new(SyncEngine::new());
        let table = Table::try_from_uri(path).unwrap();

        // A field in the schema goes from being nullable to non-nullable
        let table_changes_res = table.table_changes(engine.as_ref(), 3, 4);
        assert!(matches!(table_changes_res, Err(Error::Generic(_))));
    }
}

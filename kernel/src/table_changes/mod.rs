//! Reads a table's change data feed between two versions.
//!
//! [`TableChanges::try_new`] reads change data recorded by writers and produces data through a
//! [`TableChangesScanBuilder`]. When the `internal-api` feature is enabled,
//! [`TableChanges::try_new_row_tracking_cdf_listing`] instead lists the files and row-tracking
//! metadata that a connector needs to reconstruct changes while reading the data files itself.
//!
//! # Example
//! ```rust
//! # use std::sync::Arc;
//! # use test_utils::delta_kernel_default_engine::{DefaultEngine, DefaultEngineBuilder};
//! # use delta_kernel::expressions::{col, lit};
//! # use delta_kernel::{Predicate, Snapshot, SnapshotRef, KernelError, Engine};
//! # use delta_kernel::table_changes::TableChanges;
//! # let path = "./tests/data/table-with-cdf";
//! let url = delta_kernel::try_parse_uri(path)?;
//! # use test_utils::delta_kernel_default_engine::storage::store_from_url;
//! # let engine = std::sync::Arc::new(DefaultEngineBuilder::new(store_from_url(&url)?).build());
//! // Get the table changes (change data feed) between version 0 and 1
//! let table_changes = TableChanges::try_new(url, engine.as_ref(), 0, Some(1))?;
//!
//! // Optionally specify a schema and predicate to apply to the table changes scan
//! let schema = table_changes
//!     .schema()
//!     .project(&["id", "_commit_version"])?;
//! let predicate = Arc::new(Predicate::gt(col!("id"), lit(10)));
//!
//! // Construct the table changes scan
//! let table_changes_scan = table_changes
//!     .into_scan_builder()
//!     .with_schema(schema)
//!     .with_predicate(predicate.clone())
//!     .build()?;
//!
//! // Execute the table changes scan to get a fallible iterator of `Box<dyn EngineData>`s
//! let table_change_batches = table_changes_scan.execute(engine.clone())?;
//! # Ok::<(), KernelError>(())
//! ```
use std::sync::{Arc, LazyLock};

use delta_kernel_derive::internal_api;
use log_replay::table_changes_action_iter_with_mode;
use scan::TableChangesScanBuilder;
use scan_file::scan_metadata_to_scan_file;
use url::Url;

use crate::commit_range::{CommitRange, CommitRangeBuilder};
use crate::path::LogPathFileType;
use crate::schema::compare::SchemaComparison as _;
use crate::schema::{try_schema, DataType, Schema, StructField, StructType};
use crate::snapshot::{Snapshot, SnapshotRef};
use crate::table_configuration::TableConfiguration;
use crate::table_features::{Operation, TableFeature};
use crate::table_properties::{
    MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME, MATERIALIZED_ROW_ID_COLUMN_NAME,
};
use crate::utils::{require, FoldWithOption as _};
use crate::{Engine, KernelError, KernelResult, LogPath, Version};

mod log_replay;
mod net_changes;
mod physical_to_logical;
mod resolve_dvs;
pub mod scan;
mod scan_file;
#[cfg(test)]
mod test_utils;

#[cfg_attr(not(feature = "internal-api"), allow(unused_imports))]
#[internal_api]
pub(crate) use scan_file::{TableChangesFileAction, TableChangesScanFile};

/// Selects the history represented by [`TableChanges::scan_file_listing`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
#[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
#[internal_api]
pub(crate) enum TableChangesListingMode {
    /// Preserves each commit's file actions.
    ///
    /// An `add` and `remove` for the same path in one commit are grouped. Actions for the same
    /// path in different commits remain separate and require reconciliation by stable row ID.
    AllChanges,
    /// Reduces each path to its net effect between the start and end of the range.
    ///
    /// The remove side represents the earliest pre-image and the add side represents the latest
    /// post-image. Intermediate actions are omitted, but each surviving action still requires
    /// row-level reconciliation. Computing net changes buffers the full range.
    NetChanges,
}

/// Identifies the table feature that must remain enabled across the change-feed range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CdfMode {
    /// Uses change data recorded by writers.
    ///
    /// This mode requires `delta.enableChangeDataFeed`. It reads `_change_data` files when a
    /// commit contains them and otherwise derives changes from `add` and `remove` actions. The
    /// data-reading [`scan`] and `execute` paths use these semantics.
    ChangeDataFeed,
    /// Uses row lineage to reconstruct changes while reading data files.
    ///
    /// This mode requires `delta.enableRowTracking`. It ignores `_change_data` files and returns
    /// metadata from `add` and `remove` actions for [`TableChanges::scan_file_listing`].
    RowTracking,
}

impl CdfMode {
    /// The table feature that must be enabled across the entire change-feed range for this mode.
    pub(crate) fn required_feature(self) -> TableFeature {
        match self {
            CdfMode::ChangeDataFeed => TableFeature::ChangeDataFeed,
            CdfMode::RowTracking => TableFeature::RowTracking,
        }
    }

    /// Returns the mode-specific error when [`CdfMode::required_feature`] is disabled at `version`.
    pub(crate) fn feature_disabled_error(self, version: Version) -> KernelError {
        match self {
            CdfMode::ChangeDataFeed => KernelError::change_data_feed_unsupported(version),
            CdfMode::RowTracking => KernelError::row_tracking_change_feed_unsupported(version),
        }
    }

    /// The read operation whose feature support is required by this mode.
    pub(crate) fn read_operation(self) -> Operation {
        match self {
            CdfMode::ChangeDataFeed => Operation::Cdf,
            CdfMode::RowTracking => Operation::Scan,
        }
    }

    /// Whether data written with `candidate` can be read using the change feed's `read_schema`.
    ///
    /// [`CdfMode::ChangeDataFeed`] requires exact equality. Row-tracking CDF accepts additive
    /// nullable fields and wider nullability, but not datatype changes. Both modes require exact
    /// UDT equality, including annotations.
    pub(crate) fn schemas_compatible(
        self,
        candidate: &StructType,
        read_schema: &StructType,
    ) -> bool {
        match self {
            CdfMode::ChangeDataFeed => candidate == read_schema,
            CdfMode::RowTracking => candidate
                .can_read_as_without_type_widening(read_schema)
                .is_ok(),
        }
    }

    /// Whether `AddCDCFile` actions supersede `add` and `remove` actions.
    pub(crate) fn uses_change_data_files(self) -> bool {
        self == CdfMode::ChangeDataFeed
    }

    /// Returns the mode-specific error for incompatible range-boundary schemas.
    pub(crate) fn boundary_schema_error(self, start: &StructType, end: &StructType) -> KernelError {
        match self {
            CdfMode::ChangeDataFeed => KernelError::generic(format!(
                "Failed to build TableChanges: Start and end version schemas are different. Found start version schema {start:?} and end version schema {end:?}",
            )),
            CdfMode::RowTracking => KernelError::change_data_feed_incompatible_schema(end, start),
        }
    }

    /// Maps a read-support failure after a metadata or protocol update to the mode-specific error.
    pub(crate) fn read_support_error(
        self,
        underlying: KernelError,
        version: Version,
    ) -> KernelError {
        match self {
            CdfMode::ChangeDataFeed => KernelError::change_data_feed_unsupported(version),
            CdfMode::RowTracking => underlying,
        }
    }
}

pub(crate) const CHANGE_TYPE_COL_NAME: &str = "_change_type";
pub(crate) const COMMIT_VERSION_COL_NAME: &str = "_commit_version";
pub(crate) const COMMIT_TIMESTAMP_COL_NAME: &str = "_commit_timestamp";
static ADD_CHANGE_TYPE: &str = "insert";
static REMOVE_CHANGE_TYPE: &str = "delete";
static CDF_FIELDS: LazyLock<[StructField; 3]> = LazyLock::new(|| {
    [
        StructField::not_null(CHANGE_TYPE_COL_NAME, DataType::STRING),
        StructField::not_null(COMMIT_VERSION_COL_NAME, DataType::LONG),
        StructField::not_null(COMMIT_TIMESTAMP_COL_NAME, DataType::TIMESTAMP),
    ]
});

/// Represents a call to read the Change Data Feed (CDF) between two versions of a table. The schema
/// of `TableChanges` will be the schema of the table at the end version with three additional
/// columns:
/// - `_change_type`: String representing the type of change that for that commit. This may be one
///   of `delete`, `insert`, `update_preimage`, or `update_postimage`.
/// - `_commit_version`: Long representing the commit the change occurred in.
/// - `_commit_timestamp`: Commit time from `CommitInfo.inCommitTimestamp` when in-commit timestamps
///   are enabled, otherwise from the log file's modification time. The value is a UTC timestamp
///   with microsecond precision.
///
/// Three properties must hold for the entire CDF range:
/// - Reading must be supported for every commit in the range: every enabled reader feature must be
///   supported by the kernel. The supported read features will be expanded in the future to cover
///   more delta table features.
/// - [`TableChanges::try_new`] requires Change Data Feed to remain enabled and requires exact
///   schema equality.
/// - [`TableChanges::try_new_row_tracking_cdf_listing`] requires row tracking to remain enabled. It
///   allows additive nullable columns and relaxed nullability, but rejects datatype changes.
///
/// Construction validates the range boundaries. Intermediate metadata and protocol updates are
/// validated when the transaction log is replayed by the scan or listing operation.
///
///  # Examples
///  Get `TableChanges` for versions 0 to 1 (inclusive)
///  ```rust
///  # use test_utils::delta_kernel_default_engine::{storage::store_from_url, DefaultEngineBuilder};
///  # use delta_kernel::{SnapshotRef, KernelError};
///  # use delta_kernel::table_changes::TableChanges;
///  # let path = "./tests/data/table-with-cdf";
///  let url = delta_kernel::try_parse_uri(path)?;
///  # let engine = DefaultEngineBuilder::new(store_from_url(&url)?).build();
///  let table_changes = TableChanges::try_new(url, &engine, 0, Some(1))?;
///  # Ok::<(), KernelError>(())
///  ````
/// For more details, see the following sections of the protocol:
/// - [Add CDC File](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-cdc-file)
/// - [Change Data Files](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#change-data-files).
#[derive(Debug)]
pub struct TableChanges {
    commit_range: CommitRange,
    table_root: Url,
    end_snapshot: SnapshotRef,
    schema: Schema,
    start_table_config: TableConfiguration,
    // Both feed types share range loading and log replay. The mode is fixed at construction, and
    // each consumer method verifies that it supports that mode before doing work.
    mode: CdfMode,
}

impl TableChanges {
    /// Creates a new [`TableChanges`] instance for the given version range. This function checks
    /// these properties:
    /// - The change data feed table feature must be enabled in both the start or end versions.
    /// - Every enabled reader feature must be supported by the kernel.
    /// - The schemas at the start and end versions are the same.
    ///
    /// Note that this does not check that change data feed is enabled for every commit in the
    /// range. It also does not check that the schema remains the same for the entire range.
    ///
    /// # Parameters
    /// - `table_root`: url pointing at the table root (where `_delta_log` folder is located)
    /// - `engine`: Implementation of [`Engine`] apis.
    /// - `start_version`: The start version of the change data feed
    /// - `end_version`: The end version (inclusive) of the change data feed. If this is none, this
    ///   defaults to the newest table version.
    pub fn try_new(
        table_root: Url,
        engine: &dyn Engine,
        start_version: Version,
        end_version: Option<Version>,
    ) -> KernelResult<Self> {
        Self::try_new_internal(
            table_root,
            engine,
            start_version,
            end_version,
            CdfMode::ChangeDataFeed,
            None,
        )
    }

    /// Creates a listing-only change feed from row-tracking metadata.
    ///
    /// This path requires `delta.enableRowTracking` and ignores `_change_data` files.
    /// [`TableChanges::scan_file_listing`] returns the `add` and `remove` actions whose data must
    /// be reconciled by row ID.
    ///
    /// # Parameters
    ///
    /// - `table_root`: URL of the table root containing `_delta_log`.
    /// - `engine`: Engine used to read the transaction log.
    /// - `start_version`: First version in the change feed; must not exceed a supplied catalog
    ///   maximum.
    /// - `end_version`: Optional inclusive end version; must not exceed a supplied catalog maximum.
    ///   When omitted, uses that maximum, or the newest version when no maximum is supplied.
    /// - `catalog_log_tail`: Catalog-ratified staged commits and maximum ratified version, required
    ///   for catalog-managed tables. The tail must be in contiguous ascending version order. A
    ///   nonempty tail must end at the maximum when `end_version` is omitted, or at or beyond an
    ///   explicit `end_version`. An empty tail is allowed when the commits are published.
    ///
    /// # Errors
    ///
    /// Returns an error if the range or catalog inputs are invalid, a catalog maximum is supplied
    /// for a non-catalog-managed end snapshot, or a boundary has unavailable row tracking,
    /// unsupported reader features, or an incompatible schema. A published start may predate
    /// catalog activation. Errors from intermediate versions are returned by
    /// [`TableChanges::scan_file_listing`].
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    #[internal_api]
    pub(crate) fn try_new_row_tracking_cdf_listing(
        table_root: Url,
        engine: &dyn Engine,
        start_version: Version,
        end_version: Option<Version>,
        catalog_log_tail: Option<(Vec<LogPath>, Version)>,
    ) -> KernelResult<Self> {
        Self::try_new_internal(
            table_root,
            engine,
            start_version,
            end_version,
            CdfMode::RowTracking,
            catalog_log_tail,
        )
    }

    fn try_new_internal(
        table_root: Url,
        engine: &dyn Engine,
        start_version: Version,
        end_version: Option<Version>,
        mode: CdfMode,
        catalog_log_tail: Option<(Vec<LogPath>, Version)>,
    ) -> KernelResult<Self> {
        let commit_range = CommitRange::builder_for(table_root.as_str(), start_version)
            .fold_with(end_version, CommitRangeBuilder::with_end_version)
            .fold_with(catalog_log_tail.as_ref(), |builder, (tail, maximum)| {
                builder.with_log_tail(tail.clone(), *maximum)
            })
            .build(engine)?;

        let start_snapshot = load_start_snapshot(engine, &commit_range, catalog_log_tail.as_ref())?;
        start_snapshot
            .table_configuration()
            .ensure_operation_supported(mode.read_operation())?;

        let end_snapshot = Snapshot::builder_from(start_snapshot.clone())
            .at_version(commit_range.end_version())
            .fold_with(catalog_log_tail, |builder, (tail, maximum)| {
                builder
                    .with_log_tail(tail)
                    .with_max_catalog_version(maximum)
            })
            .build(engine)?;
        end_snapshot
            .table_configuration()
            .ensure_operation_supported(mode.read_operation())?;

        // Verify the change feed is enabled at the beginning and end of the interval to fail early.
        // The `ensure_operation_supported` calls above already validate that every enabled reader
        // feature is supported by the kernel (e.g. deletion vectors and column mapping). The
        // feature that must additionally be *enabled* depends on the mode: ChangeDataFeed
        // for the cdc-file path, RowTracking for the row-tracking path.
        //
        // Note: We must still check each metadata and protocol action in the CDF range.
        let check_table_config = |snapshot: &Snapshot| -> KernelResult<()> {
            require!(
                snapshot
                    .table_configuration()
                    .is_feature_enabled(&mode.required_feature()),
                mode.feature_disabled_error(snapshot.version())
            );
            Ok(())
        };
        check_table_config(&start_snapshot)?;
        check_table_config(&end_snapshot)?;

        if mode == CdfMode::RowTracking {
            let properties = end_snapshot.table_properties();
            require!(
                properties.materialized_row_id_column_name.is_some(),
                KernelError::generic(format!(
                    "Row tracking is enabled at version {}, but metadata property {} is missing",
                    end_snapshot.version(),
                    MATERIALIZED_ROW_ID_COLUMN_NAME
                ))
            );
            require!(
                properties
                    .materialized_row_commit_version_column_name
                    .is_some(),
                KernelError::generic(format!(
                    "Row tracking is enabled at version {}, but metadata property {} is missing",
                    end_snapshot.version(),
                    MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME
                ))
            );
        }

        // Log replay validates only commits that contain metadata, so validate the range boundary
        // separately. Compatibility is evaluated against the logical schemas.
        let start_schema = start_snapshot.schema();
        let end_schema = end_snapshot.schema();
        if !mode.schemas_compatible(start_schema.as_ref(), end_schema.as_ref()) {
            return Err(mode.boundary_schema_error(start_schema.as_ref(), end_schema.as_ref()));
        }

        let schema = try_schema! {
            ..(end_schema.fields()),
            ..(CDF_FIELDS.clone()),
        }?;

        Ok(TableChanges {
            table_root,
            end_snapshot,
            commit_range,
            schema,
            start_table_config: start_snapshot.table_configuration().clone(),
            mode,
        })
    }

    /// The start version of the `TableChanges`.
    pub fn start_version(&self) -> Version {
        self.commit_range.start_version()
    }
    /// The end version (inclusive) of the [`TableChanges`]. If no `end_version` was specified in
    /// [`TableChanges::try_new`], this returns the newest version as of the call to `try_new`.
    pub fn end_version(&self) -> Version {
        self.commit_range.end_version()
    }
    /// The logical schema of the change data feed. For details on the shape of the schema, see
    /// [`TableChanges`].
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
    /// Path to the root of the table that is being read.
    pub fn table_root(&self) -> &Url {
        &self.table_root
    }

    /// Returns the physical Parquet column that stores materialized row IDs.
    ///
    /// # Errors
    ///
    /// Returns an error unless this value was created by
    /// [`TableChanges::try_new_row_tracking_cdf_listing`].
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    #[internal_api]
    pub(crate) fn materialized_row_id_column_name(&self) -> KernelResult<&str> {
        self.row_tracking_table_properties()?
            .materialized_row_id_column_name
            .as_deref()
            .ok_or_else(|| {
                KernelError::internal_error(
                    "A row-tracking TableChanges is missing its materialized row ID column name",
                )
            })
    }

    /// Returns the physical Parquet column that stores materialized row commit versions.
    ///
    /// # Errors
    ///
    /// Returns an error unless this value was created by
    /// [`TableChanges::try_new_row_tracking_cdf_listing`].
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    #[internal_api]
    pub(crate) fn materialized_row_commit_version_column_name(&self) -> KernelResult<&str> {
        self.row_tracking_table_properties()?
            .materialized_row_commit_version_column_name
            .as_deref()
            .ok_or_else(|| {
                KernelError::internal_error(
                    "A row-tracking TableChanges is missing its materialized row commit version \
                     column name",
                )
            })
    }

    fn row_tracking_table_properties(
        &self,
    ) -> KernelResult<&crate::table_properties::TableProperties> {
        require!(
            self.mode == CdfMode::RowTracking,
            KernelError::unsupported(
                "Row-tracking column names are only available for row-tracking change feeds"
            )
        );
        Ok(self.end_snapshot.table_properties())
    }

    /// Create a [`TableChangesScanBuilder`] for an `Arc<TableChanges>`.
    pub fn scan_builder(self: Arc<Self>) -> TableChangesScanBuilder {
        TableChangesScanBuilder::new(self)
    }

    /// Consume this `TableChanges` to create a [`TableChangesScanBuilder`]
    pub fn into_scan_builder(self) -> TableChangesScanBuilder {
        TableChangesScanBuilder::new(self)
    }

    /// Lists the files required to reconstruct a row-tracking change feed without reading data
    /// files.
    ///
    /// Each [`TableChangesFileAction`] contains an add side, a remove side, or both. Read each side
    /// using its own deletion vector. For every selected physical row, reconstruct the row ID as
    /// `coalesce(materialized_row_id, base_row_id + physical_row_index)`. Reconstruct the row
    /// commit version as
    /// `coalesce(materialized_row_commit_version, default_row_commit_version)`. The materialized
    /// column names are returned by [`TableChanges::materialized_row_id_column_name`] and
    /// [`TableChanges::materialized_row_commit_version_column_name`]. Assign physical row indexes
    /// before applying deletion vectors.
    ///
    /// Rows present only on the add side are inserts, rows present only on the remove side are
    /// deletes, and matching row IDs with different row commit versions are updates. Matching both
    /// the row ID and row commit version identifies a row carried forward without change.
    /// `AddCDCFile` actions are ignored.
    ///
    /// Both listing modes buffer the selected actions before returning. The iterator therefore
    /// reports preparation errors immediately and uses memory proportional to the range's action
    /// count. This method requires a [`TableChanges`] created by
    /// [`TableChanges::try_new_row_tracking_cdf_listing`].
    ///
    /// # Errors
    ///
    /// Returns an error if this value was not created by
    /// [`TableChanges::try_new_row_tracking_cdf_listing`] or if the log actions cannot be replayed
    /// into a valid listing.
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    #[internal_api]
    pub(crate) fn scan_file_listing(
        self: Arc<Self>,
        engine: Arc<dyn Engine>,
        mode: TableChangesListingMode,
    ) -> KernelResult<impl Iterator<Item = KernelResult<TableChangesFileAction>>> {
        if self.mode != CdfMode::RowTracking {
            return Err(KernelError::unsupported(
                "scan_file_listing is only supported for row-tracking change feeds; construct \
                 the TableChanges with TableChanges::try_new_row_tracking_cdf_listing",
            ));
        }

        let commits = self.commit_range.commit_files().to_vec();
        let schema = self.end_snapshot.schema();
        let scan_metadata = table_changes_action_iter_with_mode(
            engine,
            &self.start_table_config,
            commits,
            schema,
            None,
            self.mode,
        )?;
        // Any listing error surfaces here rather than mid-iteration.
        let actions = scan_metadata_to_scan_file(scan_metadata)
            .map(|scan_file| TableChangesFileAction::try_from_scan_file(scan_file?))
            .collect::<KernelResult<Vec<_>>>()?;
        let actions = match mode {
            TableChangesListingMode::AllChanges => actions,
            TableChangesListingMode::NetChanges => net_changes::collapse_net_changes(actions)?,
        };
        Ok(actions.into_iter().map(Ok))
    }
}

fn load_start_snapshot(
    engine: &dyn Engine,
    range: &CommitRange,
    catalog_log_tail: Option<&(Vec<LogPath>, Version)>,
) -> KernelResult<SnapshotRef> {
    let builder = || Snapshot::builder_for(range.table_root()).at_version(range.start_version());
    let result = builder()
        .fold_with(catalog_log_tail, |builder, (tail, maximum)| {
            builder
                .with_log_tail(tail.clone())
                .with_max_catalog_version(*maximum)
        })
        .build(engine);

    // Published history may predate catalog activation; staged starts cannot fall back.
    match result {
        Err(KernelError::MaxCatalogVersion(_))
            if catalog_log_tail.is_some()
                && range
                    .commit_files()
                    .first()
                    .is_some_and(|commit| commit.file_type == LogPathFileType::Commit) =>
        {
            builder().build(engine)
        }
        result => result,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};

    use ::test_utils::{add_commit, add_staged_commit};
    use bytes::Bytes;
    use itertools::{assert_equal, Itertools};

    use super::*;
    use crate::actions::deletion_vector::DeletionVectorDescriptor;
    use crate::actions::{Add, CommitInfo, DomainMetadata, Metadata, Protocol, Remove};
    use crate::engine::sync::SyncEngine;
    use crate::engine::test_delegating::DelegatingEngine;
    use crate::object_store::memory::InMemory;
    use crate::schema::{schema_ref, DataType, StructField, StructType};
    use crate::table_changes::test_utils::{
        row_tracking_metadata, row_tracking_properties, row_tracking_protocol,
        row_tracking_setup_actions, test_deletion_vector,
        TEST_MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME, TEST_MATERIALIZED_ROW_ID_COLUMN_NAME,
    };
    use crate::table_changes::CDF_FIELDS;
    use crate::table_features::TableFeature;
    use crate::table_properties::{
        COLUMN_MAPPING_MODE, ENABLE_CHANGE_DATA_FEED, ENABLE_IN_COMMIT_TIMESTAMPS,
        IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP, IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION,
        MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME, MATERIALIZED_ROW_ID_COLUMN_NAME,
    };
    use crate::unit_test_utils::{
        assert_result_error_with_message, create_log_path, serialize_actions,
        test_schema_flat_with_column_mapping, Action, LocalMockTable,
    };
    use crate::{
        Engine, FileMeta, FileSlice, KernelError, KernelResultIteratorStatic, LogPath,
        StorageHandler,
    };

    fn listing_test_schema() -> Arc<StructType> {
        schema_ref! {
            nullable "id": INTEGER,
            nullable "value": STRING,
        }
    }

    fn catalog_managed_row_tracking_setup_actions() -> Vec<Action> {
        let metadata = row_tracking_metadata(listing_test_schema())
            .with_configuration_entry(ENABLE_IN_COMMIT_TIMESTAMPS, "true");
        let protocol = Protocol::try_new_modern(
            [TableFeature::CatalogManaged],
            [
                TableFeature::CatalogManaged,
                TableFeature::InCommitTimestamp,
                TableFeature::RowTracking,
                TableFeature::DomainMetadata,
            ],
        )
        .unwrap();
        vec![Action::Protocol(protocol), Action::Metadata(metadata)]
    }

    fn commit_info_action(version: Version) -> Action {
        let timestamp = 1_000 + version as i64;
        Action::CommitInfo(CommitInfo::new(
            timestamp,
            Some(timestamp),
            None,
            None,
            true,
        ))
    }

    fn row_tracking_add_action(path: &str, version: Version) -> Action {
        Action::Add(Add {
            path: path.to_string(),
            data_change: true,
            size: 100,
            stats: Some(serde_json::json!({"numRecords": 1}).to_string()),
            base_row_id: Some(version as i64 * 10),
            default_row_commit_version: Some(version as i64),
            ..Default::default()
        })
    }

    async fn write_catalog_managed_row_tracking_base(
        store: &dyn crate::object_store::ObjectStore,
        table_root: &Url,
    ) {
        let actions = [commit_info_action(0)]
            .into_iter()
            .chain(catalog_managed_row_tracking_setup_actions())
            .chain([row_tracking_add_action("path_0", 0)]);
        add_commit(table_root, store, 0, serialize_actions(actions))
            .await
            .unwrap();
    }

    #[test]
    fn table_changes_checks_enable_cdf_flag() {
        // Table with CDF enabled, then disabled at version 2 and enabled at version 3
        let path = "./tests/data/table-with-cdf";
        let engine = Box::new(SyncEngine::new());
        let url = delta_kernel::try_parse_uri(path).unwrap();

        let valid_ranges = [(0, 1), (0, 0), (1, 1)];
        for (start_version, end_version) in valid_ranges {
            let table_changes = TableChanges::try_new(
                url.clone(),
                engine.as_ref(),
                start_version,
                end_version.into(),
            )
            .unwrap();
            assert_eq!(table_changes.start_version(), start_version);
            assert_eq!(table_changes.end_version(), end_version);
        }

        let invalid_ranges = [(0, 2), (1, 2), (2, 2), (2, 3)];
        for (start_version, end_version) in invalid_ranges {
            let res = TableChanges::try_new(
                url.clone(),
                engine.as_ref(),
                start_version,
                end_version.into(),
            );
            assert!(matches!(
                res,
                Err(KernelError::ChangeDataFeedUnsupported(_))
            ))
        }
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn cdf_static_false_skips_invalid_intermediate_metadata(
        #[values(false, true)] statically_false: bool,
    ) {
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new());
        let mut table = LocalMockTable::new();
        let metadata = |enabled: bool| {
            Action::Metadata(
                Metadata::try_new(
                    None,
                    None,
                    listing_test_schema(),
                    vec![],
                    0,
                    HashMap::from([(ENABLE_CHANGE_DATA_FEED.to_string(), enabled.to_string())]),
                )
                .unwrap(),
            )
        };
        table
            .commit([
                Action::Protocol(Protocol::try_new_legacy(1, 4).unwrap()),
                metadata(true),
            ])
            .await;
        table.commit([metadata(false)]).await;
        table.commit([metadata(true)]).await;

        let root = Url::from_directory_path(table.table_root()).unwrap();
        let changes = TableChanges::try_new(root, engine.as_ref(), 0, Some(2)).unwrap();
        let builder = changes.into_scan_builder();
        let builder = if statically_false {
            builder.with_predicate(Arc::new(crate::Predicate::FALSE))
        } else {
            builder
        };
        let scan = builder.build().unwrap();
        let result: KernelResult<Vec<_>> = scan.execute(engine).unwrap().try_collect();
        if statically_false {
            assert!(result.unwrap().is_empty());
        } else {
            assert!(matches!(
                result,
                Err(KernelError::ChangeDataFeedUnsupported(1))
            ));
        }
    }

    #[rstest::rstest]
    #[case::cdf_disabled_after_listing(CdfMode::ChangeDataFeed)]
    #[case::row_tracking_schema_added_after_listing(CdfMode::RowTracking)]
    #[tokio::test]
    async fn omitted_end_snapshot_stays_at_the_resolved_range_end(#[case] mode: CdfMode) {
        let store = Arc::new(InMemory::new());
        let table_root = Url::parse("memory:///").unwrap();
        let schema = listing_test_schema();
        let mut properties = row_tracking_properties();
        properties.insert(ENABLE_CHANGE_DATA_FEED.to_string(), "true".to_string());
        let metadata =
            Metadata::try_new(None, None, schema.clone(), vec![], 0, properties.clone()).unwrap();
        let protocol = Protocol::try_new_modern(
            TableFeature::EMPTY_LIST,
            [
                TableFeature::ChangeDataFeed,
                TableFeature::RowTracking,
                TableFeature::DomainMetadata,
            ],
        )
        .unwrap();
        add_commit(
            &table_root,
            store.as_ref(),
            0,
            serialize_actions([
                Action::Protocol(protocol),
                Action::Metadata(metadata),
                row_tracking_add_action("path_0", 0),
            ]),
        )
        .await
        .unwrap();

        let new_schema = match mode {
            CdfMode::ChangeDataFeed => {
                properties.insert(ENABLE_CHANGE_DATA_FEED.to_string(), "false".to_string());
                schema.clone()
            }
            CdfMode::RowTracking => schema_ref! {
                ..(schema.fields()),
                nullable "extra": STRING,
            },
        };
        let next_metadata =
            Metadata::try_new(None, None, new_schema, vec![], 0, properties).unwrap();
        let inner = Arc::new(SyncEngine::new_with_store(store));
        let storage = Arc::new(PublishAfterFirstList {
            inner: inner.storage_handler(),
            next_commit: Mutex::new(Some((
                table_root
                    .join("_delta_log/00000000000000000001.json")
                    .unwrap(),
                serialize_actions([
                    Action::Metadata(next_metadata),
                    row_tracking_add_action("path_1", 1),
                ])
                .into(),
            ))),
        });
        let engine = Arc::new(DelegatingEngine::new(inner).with_storage_handler(storage.clone()));
        let table_changes = match mode {
            CdfMode::ChangeDataFeed => TableChanges::try_new(table_root, engine.as_ref(), 0, None),
            CdfMode::RowTracking => TableChanges::try_new_row_tracking_cdf_listing(
                table_root,
                engine.as_ref(),
                0,
                None,
                None,
            ),
        }
        .unwrap();

        assert!(storage.next_commit.lock().unwrap().is_none());
        assert_eq!(table_changes.end_version(), 0);
        assert_eq!(table_changes.end_snapshot.version(), 0);
        assert_eq!(table_changes.end_snapshot.schema(), schema);
        assert_equal(
            schema.fields().cloned().chain(CDF_FIELDS.clone()),
            table_changes.schema().fields().cloned(),
        );
        if mode == CdfMode::RowTracking {
            let listing: Vec<_> = Arc::new(table_changes)
                .scan_file_listing(engine, TableChangesListingMode::AllChanges)
                .unwrap()
                .try_collect()
                .unwrap();
            assert_eq!(listing.len(), 1);
            assert_eq!(listing[0].add.as_ref().unwrap().path, "path_0");
        }
    }
    #[test]
    fn schema_evolution_fails() {
        let path = "./tests/data/table-with-cdf";
        let engine = Box::new(SyncEngine::new());
        let url = delta_kernel::try_parse_uri(path).unwrap();
        let expected_msg = "Failed to build TableChanges: Start and end version schemas are different. Found start version schema StructType { type_name: \"struct\", fields: {\"part\": StructField { name: \"part\", data_type: Primitive(Integer), nullable: true, metadata: {} }, \"id\": StructField { name: \"id\", data_type: Primitive(Integer), nullable: true, metadata: {} }}, metadata_columns: {} } and end version schema StructType { type_name: \"struct\", fields: {\"part\": StructField { name: \"part\", data_type: Primitive(Integer), nullable: true, metadata: {} }, \"id\": StructField { name: \"id\", data_type: Primitive(Integer), nullable: false, metadata: {} }}, metadata_columns: {} }";

        // A field in the schema goes from being nullable to non-nullable
        let table_changes_res = TableChanges::try_new(url, engine.as_ref(), 3, Some(4));
        assert!(matches!(table_changes_res, Err(KernelError::Generic(msg)) if msg == expected_msg));
    }

    #[test]
    fn table_changes_has_cdf_schema() {
        let path = "./tests/data/table-with-cdf";
        let engine = Box::new(SyncEngine::new());
        let url = delta_kernel::try_parse_uri(path).unwrap();
        let expected_schema = [
            StructField::nullable("part", DataType::INTEGER),
            StructField::nullable("id", DataType::INTEGER),
        ]
        .into_iter()
        .chain(CDF_FIELDS.clone());

        let table_changes =
            TableChanges::try_new(url.clone(), engine.as_ref(), 0, 0.into()).unwrap();
        assert_equal(expected_schema, table_changes.schema().fields().cloned());
    }

    #[test]
    fn scan_file_listing_rejects_cdc_file_table_changes() {
        // A change-data-file `TableChanges` (built via `try_new`) must not be consumed through the
        // row-tracking listing API.
        let path = "./tests/data/table-with-cdf";
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new());
        let url = delta_kernel::try_parse_uri(path).unwrap();
        let table_changes =
            Arc::new(TableChanges::try_new(url, engine.as_ref(), 0, Some(1)).unwrap());
        assert_result_error_with_message(
            table_changes.materialized_row_id_column_name(),
            "only available for row-tracking change feeds",
        );
        assert_result_error_with_message(
            table_changes.materialized_row_commit_version_column_name(),
            "only available for row-tracking change feeds",
        );
        let res = table_changes.scan_file_listing(engine, TableChangesListingMode::AllChanges);
        assert!(
            matches!(res, Err(KernelError::Unsupported(_))),
            "scan_file_listing on a cdc-file TableChanges must return an unsupported error"
        );
    }

    #[rstest::rstest]
    #[case::row_id(MATERIALIZED_ROW_ID_COLUMN_NAME)]
    #[case::row_commit_version(MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME)]
    #[tokio::test]
    async fn try_new_row_tracking_cdf_listing_rejects_missing_materialized_column_name(
        #[case] missing_property: &str,
    ) {
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new());
        let mut mock_table = LocalMockTable::new();
        let mut properties = row_tracking_properties();
        properties.remove(missing_property);
        let metadata =
            Metadata::try_new(None, None, listing_test_schema(), vec![], 0, properties).unwrap();
        mock_table
            .commit([
                Action::Protocol(row_tracking_protocol()),
                Action::Metadata(metadata),
            ])
            .await;

        let table_root = url::Url::from_directory_path(mock_table.table_root()).unwrap();
        assert_result_error_with_message(
            TableChanges::try_new_row_tracking_cdf_listing(
                table_root,
                engine.as_ref(),
                0,
                Some(0),
                None,
            ),
            missing_property,
        );
    }

    #[test]
    fn try_new_row_tracking_cdf_listing_fails_when_row_tracking_disabled() {
        // table-with-cdf enables change data feed but not row tracking, so the row-tracking feed
        // must be rejected at construction.
        let path = "./tests/data/table-with-cdf";
        let engine = Box::new(SyncEngine::new());
        let url = delta_kernel::try_parse_uri(path).unwrap();
        let res =
            TableChanges::try_new_row_tracking_cdf_listing(url, engine.as_ref(), 0, Some(1), None);
        assert!(
            matches!(&res, Err(KernelError::RowTrackingChangeFeedUnsupported(_))),
            "expected a row-tracking-disabled error, got {res:?}"
        );
    }

    #[tokio::test]
    async fn try_new_row_tracking_cdf_listing_rejects_incompatible_start_schema() {
        // The start-version schema (in effect for in-range commits before any metadata update)
        // carries a column the end-version schema drops. No in-range commit re-declares the start
        // schema, so this boundary incompatibility is caught only by the start-vs-end check, not
        // the per-commit one.
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new());
        let mut mock_table = LocalMockTable::new();
        let start_schema = schema_ref! {
            nullable "id": INTEGER,
            nullable "value": STRING,
            nullable "extra": INTEGER,
        };
        let end_schema = schema_ref! {
            nullable "id": INTEGER,
            nullable "value": STRING,
        };
        let rt_config = row_tracking_properties();

        // v0: start schema + row tracking. v1: a data commit (no metadata) using the start schema.
        // v2: a metadata commit that drops `extra` -- never re-declaring the start schema in range.
        mock_table
            .commit(row_tracking_setup_actions(start_schema))
            .await;
        mock_table
            .commit([Action::Add(Add {
                path: "f1".into(),
                data_change: true,
                size: 10,
                base_row_id: Some(0),
                default_row_commit_version: Some(1),
                ..Default::default()
            })])
            .await;
        mock_table
            .commit([Action::Metadata(
                Metadata::try_new(None, None, end_schema, vec![], 0, rt_config).unwrap(),
            )])
            .await;

        let table_root = url::Url::from_directory_path(mock_table.table_root()).unwrap();
        let res = TableChanges::try_new_row_tracking_cdf_listing(
            table_root,
            engine.as_ref(),
            1,
            Some(2),
            None,
        );
        assert!(
            matches!(
                &res,
                Err(KernelError::ChangeDataFeedIncompatibleSchema(_, _))
            ),
            "expected an incompatible start schema to be rejected, got {res:?}"
        );
    }

    #[tokio::test]
    async fn scan_file_listing_surfaces_row_tracking_files_end_to_end() {
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new());
        let mut mock_table = LocalMockTable::new();

        // version 0: protocol + metadata (row tracking enabled) + an insert.
        let add_v0 = Add {
            path: "path_0".into(),
            data_change: true,
            size: 100,
            base_row_id: Some(0),
            default_row_commit_version: Some(0),
            ..Default::default()
        };
        let add_v1 = Add {
            path: "path_1".into(),
            data_change: true,
            size: 100,
            base_row_id: Some(5),
            default_row_commit_version: Some(1),
            ..Default::default()
        };
        mock_table
            .commit(
                row_tracking_setup_actions(listing_test_schema())
                    .into_iter()
                    .chain([Action::Add(add_v0)]),
            )
            .await;
        mock_table.commit([Action::Add(add_v1)]).await;

        let table_root = url::Url::from_directory_path(mock_table.table_root()).unwrap();
        let table_changes = Arc::new(
            TableChanges::try_new_row_tracking_cdf_listing(
                table_root,
                engine.as_ref(),
                0,
                Some(1),
                None,
            )
            .unwrap(),
        );
        let listing: Vec<TableChangesFileAction> = table_changes
            .scan_file_listing(engine, TableChangesListingMode::AllChanges)
            .unwrap()
            .try_collect()
            .unwrap();

        // Both inserts surface (no cdc files) as single-sided add actions, each carrying its base
        // row id.
        assert_eq!(listing.len(), 2);
        let add_side = |path: &str| {
            listing
                .iter()
                .filter_map(|fa| fa.add.as_ref())
                .find(|a| a.path == path)
                .unwrap_or_else(|| panic!("{path} add side present"))
                .clone()
        };
        assert!(listing.iter().all(|fa| fa.remove.is_none()));
        assert_eq!(add_side("path_0").base_row_id, Some(0));
        assert_eq!(add_side("path_1").base_row_id, Some(5));
    }

    #[rstest::rstest]
    #[case::gap_outside_range(
        &[(0, true), (2, true)],
        2,
        "Log tail versions 0 and 2 are not contiguous",
    )]
    #[case::tail_ends_before_target(
        &[(0, true), (1, true)],
        0,
        "Log tail version 1 is less than requested version 2",
    )]
    #[case::numbered_tail(
        &[(2, false)],
        0,
        "Commit range log tail must contain only staged commits",
    )]
    #[case::mixed_tail(
        &[(1, false), (2, true)],
        0,
        "Commit range log tail must contain only staged commits",
    )]
    #[test]
    fn row_tracking_listing_validates_catalog_tail_before_listing(
        #[case] tail_entries: &[(Version, bool)],
        #[case] start_version: Version,
        #[case] expected_error: &str,
    ) {
        let table_root = Url::parse("memory:///").unwrap();
        let log_tail = tail_entries
            .iter()
            .map(|(version, staged)| {
                if *staged {
                    LogPath::staged_commit(
                        table_root.clone(),
                        &format!("{version:020}.00000000-0000-4000-8000-{version:012}.json"),
                        0,
                        1,
                    )
                    .unwrap()
                } else {
                    LogPath::try_new(FileMeta {
                        location: table_root
                            .join(&format!("_delta_log/{version:020}.json"))
                            .unwrap(),
                        last_modified: 0,
                        size: 1,
                    })
                    .unwrap()
                }
            })
            .collect();
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        let result = TableChanges::try_new_row_tracking_cdf_listing(
            table_root,
            &engine,
            start_version,
            Some(2),
            Some((log_tail, 2)),
        );
        assert_result_error_with_message(result, expected_error);
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn row_tracking_listing_rejects_mismatched_catalog_inputs(
        #[values(false, true)] catalog_managed: bool,
    ) {
        let store = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());
        let table_root = Url::parse("memory:///").unwrap();
        if catalog_managed {
            write_catalog_managed_row_tracking_base(store.as_ref(), &table_root).await;
        } else {
            add_commit(
                &table_root,
                store.as_ref(),
                0,
                serialize_actions(row_tracking_setup_actions(listing_test_schema())),
            )
            .await
            .unwrap();
        }
        let result = TableChanges::try_new_row_tracking_cdf_listing(
            table_root,
            &engine,
            0,
            Some(0),
            (!catalog_managed).then(|| (Vec::new(), 0)),
        );
        assert!(matches!(result, Err(KernelError::MaxCatalogVersion(_))));
    }

    #[tokio::test]
    async fn catalog_managed_row_tracking_listing_rejects_metadata_that_disables_ict() {
        let store = Arc::new(InMemory::new());
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new_with_store(store.clone()));
        let table_root = Url::parse("memory:///").unwrap();
        write_catalog_managed_row_tracking_base(store.as_ref(), &table_root).await;

        for (version, ict_enabled) in [(1, false), (2, true)] {
            let metadata = row_tracking_metadata(listing_test_schema())
                .with_configuration_entry(ENABLE_IN_COMMIT_TIMESTAMPS, ict_enabled.to_string());
            add_commit(
                &table_root,
                store.as_ref(),
                version,
                serialize_actions([commit_info_action(version), Action::Metadata(metadata)]),
            )
            .await
            .unwrap();
        }

        let table_changes = TableChanges::try_new_row_tracking_cdf_listing(
            table_root,
            engine.as_ref(),
            0,
            Some(2),
            Some((Vec::new(), 2)),
        )
        .unwrap();
        let result = Arc::new(table_changes)
            .scan_file_listing(engine, TableChangesListingMode::AllChanges)
            .map(|_| ());
        assert_result_error_with_message(
            result,
            "Feature 'catalogManaged' requires 'inCommitTimestamp' to be enabled",
        );
    }

    #[rstest::rstest]
    #[case::cross_activation_explicit(0, Some(1), true, &["path_0", "path_1"])]
    #[case::cross_activation_latest(0, None, true, &["path_0", "path_1"])]
    #[case::already_managed_start(1, Some(1), true, &["path_1"])]
    #[case::entirely_before_activation(0, Some(0), false, &[])]
    #[tokio::test]
    async fn catalog_managed_row_tracking_listing_can_start_before_catalog_activation(
        #[case] start_version: Version,
        #[case] end_version: Option<Version>,
        #[case] succeeds: bool,
        #[case] expected_paths: &[&str],
        #[values(false, true)] staged: bool,
        #[values(TableFeature::CatalogManaged, TableFeature::CatalogOwnedPreview)]
        catalog_feature: TableFeature,
    ) {
        let store = Arc::new(InMemory::new());
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new_with_store(store.clone()));
        let table_root = Url::parse("memory:///").unwrap();
        let metadata = row_tracking_metadata(listing_test_schema());
        add_commit(
            &table_root,
            store.as_ref(),
            0,
            serialize_actions([
                Action::Protocol(row_tracking_protocol()),
                Action::Metadata(metadata.clone()),
                row_tracking_add_action("path_0", 0),
                Action::DomainMetadata(DomainMetadata::new(
                    "delta.rowTracking".to_string(),
                    serde_json::json!({"rowIdHighWaterMark": 0}).to_string(),
                )),
            ]),
        )
        .await
        .unwrap();
        let timestamp = engine
            .storage_handler()
            .head(
                &table_root
                    .join("_delta_log/00000000000000000000.json")
                    .unwrap(),
            )
            .unwrap()
            .last_modified
            + 1;
        let metadata = metadata
            .with_configuration_entry(ENABLE_IN_COMMIT_TIMESTAMPS, "true")
            .with_configuration_entry(IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION, "1")
            .with_configuration_entry(
                IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP,
                timestamp.to_string(),
            );
        let protocol = Protocol::try_new_modern(
            [catalog_feature.clone()],
            [
                catalog_feature,
                TableFeature::InCommitTimestamp,
                TableFeature::RowTracking,
                TableFeature::DomainMetadata,
            ],
        )
        .unwrap();
        let commit = |path| {
            serialize_actions([
                Action::CommitInfo(CommitInfo::new(
                    timestamp,
                    Some(timestamp),
                    None,
                    None,
                    true,
                )),
                Action::Protocol(protocol.clone()),
                Action::Metadata(metadata.clone()),
                row_tracking_add_action(path, 1),
                Action::DomainMetadata(DomainMetadata::new(
                    "delta.rowTracking".to_string(),
                    serde_json::json!({"rowIdHighWaterMark": 10}).to_string(),
                )),
            ])
        };
        let mut log_tail = Vec::new();
        if staged {
            add_commit(&table_root, store.as_ref(), 1, commit("published_shadow_1"))
                .await
                .unwrap();
            let path = add_staged_commit(&table_root, store.as_ref(), 1, commit("path_1"))
                .await
                .unwrap();
            let path = create_log_path(table_root.join(path.as_ref()).unwrap().as_str());
            log_tail.push(LogPath::try_new(path.location).unwrap());
        } else {
            add_commit(&table_root, store.as_ref(), 1, commit("path_1"))
                .await
                .unwrap();
        }
        let table_changes = TableChanges::try_new_row_tracking_cdf_listing(
            table_root,
            engine.as_ref(),
            start_version,
            end_version,
            Some((log_tail, 1)),
        );
        if !succeeds {
            assert!(matches!(
                table_changes,
                Err(KernelError::MaxCatalogVersion(_))
            ));
            return;
        }
        let table_changes = Arc::new(table_changes.unwrap());
        assert_eq!(table_changes.start_version(), start_version);
        assert_eq!(table_changes.end_version(), 1);
        assert_eq!(table_changes.end_snapshot.version(), 1);
        assert_eq!(
            table_changes.start_table_config.is_catalog_managed(),
            start_version == 1,
        );
        let listing: Vec<_> = table_changes
            .scan_file_listing(engine, TableChangesListingMode::AllChanges)
            .unwrap()
            .try_collect()
            .unwrap();
        assert_eq!(
            listing
                .iter()
                .map(|action| action.add.as_ref().unwrap().path.as_str())
                .collect::<Vec<_>>(),
            expected_paths,
        );
    }

    #[tokio::test]
    async fn staged_boundary_metadata_supersedes_numbered_shadows() {
        let store = Arc::new(InMemory::new());
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new_with_store(store.clone()));
        let table_root = Url::parse("memory:///").unwrap();
        write_catalog_managed_row_tracking_base(store.as_ref(), &table_root).await;

        let base_schema = listing_test_schema();
        let start_schema = schema_ref! {
            ..(base_schema.fields()),
            nullable "start_only": LONG,
        };
        let end_schema = schema_ref! {
            ..(start_schema.fields()),
            nullable "end_only": STRING,
        };
        let mut tail = Vec::new();
        for (version, schema) in [(1, start_schema.clone()), (2, end_schema.clone())] {
            let staged_metadata = row_tracking_metadata(schema)
                .with_configuration_entry(ENABLE_IN_COMMIT_TIMESTAMPS, "true");
            let shadow_metadata = row_tracking_metadata(base_schema.clone())
                .with_configuration_entry(ENABLE_IN_COMMIT_TIMESTAMPS, "true");
            add_commit(
                &table_root,
                store.as_ref(),
                version,
                serialize_actions([
                    commit_info_action(version),
                    Action::Metadata(shadow_metadata),
                    row_tracking_add_action(&format!("shadow_{version}"), version),
                ]),
            )
            .await
            .unwrap();
            let path = add_staged_commit(
                &table_root,
                store.as_ref(),
                version,
                serialize_actions([
                    commit_info_action(version),
                    Action::Metadata(staged_metadata),
                    row_tracking_add_action(&format!("path_{version}"), version),
                ]),
            )
            .await
            .unwrap();
            let path = create_log_path(table_root.join(path.as_ref()).unwrap().as_str());
            tail.push(LogPath::try_new(path.location).unwrap());
        }

        let changes = Arc::new(
            TableChanges::try_new_row_tracking_cdf_listing(
                table_root,
                engine.as_ref(),
                1,
                Some(2),
                Some((tail, 2)),
            )
            .unwrap(),
        );
        assert_eq!(changes.start_table_config.logical_schema(), start_schema);
        assert_eq!(changes.end_snapshot.schema(), end_schema);
        assert_equal(
            end_schema.fields().cloned().chain(CDF_FIELDS.clone()),
            changes.schema().fields().cloned(),
        );
        let listing: Vec<_> = changes
            .scan_file_listing(engine, TableChangesListingMode::AllChanges)
            .unwrap()
            .try_collect()
            .unwrap();
        assert_eq!(
            listing
                .iter()
                .map(|action| {
                    let add = action.add.as_ref().unwrap();
                    (add.path.as_str(), add.commit_version)
                })
                .collect::<Vec<_>>(),
            vec![("path_1", 1), ("path_2", 2)],
        );
    }

    #[rstest::rstest]
    #[case::published_start(0, true)]
    #[case::staged_start(1, false)]
    #[tokio::test]
    async fn catalog_snapshot_fallback_only_applies_to_published_starts(
        #[case] start_version: Version,
        #[case] succeeds: bool,
    ) {
        let store = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());
        let table_root = Url::parse("memory:///").unwrap();
        let ordinary = serialize_actions(row_tracking_setup_actions(listing_test_schema()));
        for version in [0, 1] {
            add_commit(&table_root, store.as_ref(), version, ordinary.clone())
                .await
                .unwrap();
        }
        let staged_start = add_staged_commit(&table_root, store.as_ref(), 1, ordinary)
            .await
            .unwrap();
        let timestamp = engine
            .storage_handler()
            .head(&table_root.join(staged_start.as_ref()).unwrap())
            .unwrap()
            .last_modified
            + 1;
        let activation = catalog_managed_row_tracking_setup_actions()
            .into_iter()
            .map(|action| match action {
                Action::Metadata(metadata) => Action::Metadata(
                    metadata
                        .with_configuration_entry(IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION, "2")
                        .with_configuration_entry(
                            IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP,
                            timestamp.to_string(),
                        ),
                ),
                other => other,
            });
        let body = serialize_actions(
            [Action::CommitInfo(CommitInfo::new(
                timestamp,
                Some(timestamp),
                None,
                None,
                true,
            ))]
            .into_iter()
            .chain(activation),
        );
        let staged_end = add_staged_commit(&table_root, store.as_ref(), 2, body)
            .await
            .unwrap();
        let tail = [staged_start, staged_end]
            .into_iter()
            .map(|path| {
                let path = create_log_path(table_root.join(path.as_ref()).unwrap().as_str());
                LogPath::try_new(path.location).unwrap()
            })
            .collect();
        let result = TableChanges::try_new_row_tracking_cdf_listing(
            table_root,
            &engine,
            start_version,
            Some(2),
            Some((tail, 2)),
        );
        if succeeds {
            let changes = result.unwrap();
            assert_eq!(changes.start_version(), 0);
            assert_eq!(changes.end_snapshot.version(), 2);
            assert!(!changes.start_table_config.is_catalog_managed());
            assert!(changes
                .end_snapshot
                .table_configuration()
                .is_catalog_managed());
        } else {
            assert!(matches!(result, Err(KernelError::MaxCatalogVersion(_))));
        }
    }

    #[rstest::rstest]
    #[case::catalog_latest(0, None, Some(2), &["path_0", "path_1", "path_2"])]
    #[case::explicit_end(0, Some(1), Some(1), &["path_0", "path_1"])]
    #[case::end_above_catalog_maximum(0, Some(3), None, &[])]
    #[case::start_above_catalog_maximum(3, Some(3), None, &[])]
    #[tokio::test]
    async fn catalog_managed_row_tracking_listing_respects_version_bounds(
        #[case] start_version: Version,
        #[case] end_version: Option<Version>,
        #[case] expected_end_version: Option<Version>,
        #[case] expected_paths: &[&str],
        #[values(false, true)] staged: bool,
    ) {
        let store = Arc::new(InMemory::new());
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new_with_store(store.clone()));
        let table_root = Url::parse("memory:///").unwrap();
        write_catalog_managed_row_tracking_base(store.as_ref(), &table_root).await;
        let mut log_tail = Vec::new();
        for version in 1..=3 {
            let body = serialize_actions([
                commit_info_action(version),
                row_tracking_add_action(&format!("path_{version}"), version),
            ]);
            if staged && version <= 2 {
                let shadow = serialize_actions([
                    commit_info_action(version),
                    row_tracking_add_action(&format!("published_shadow_{version}"), version),
                ]);
                add_commit(&table_root, store.as_ref(), version, shadow)
                    .await
                    .unwrap();
                let path = add_staged_commit(&table_root, store.as_ref(), version, body)
                    .await
                    .unwrap();
                let path = create_log_path(table_root.join(path.as_ref()).unwrap().as_str());
                log_tail.push(LogPath::try_new(path.location).unwrap());
            } else {
                add_commit(&table_root, store.as_ref(), version, body)
                    .await
                    .unwrap();
            }
        }
        let table_changes = TableChanges::try_new_row_tracking_cdf_listing(
            table_root,
            engine.as_ref(),
            start_version,
            end_version,
            Some((log_tail, 2)),
        );
        let Some(expected_end_version) = expected_end_version else {
            assert!(matches!(
                table_changes,
                Err(KernelError::MaxCatalogVersion(_))
            ));
            return;
        };
        let table_changes = Arc::new(table_changes.unwrap());
        assert_eq!(table_changes.end_version(), expected_end_version);
        assert_eq!(table_changes.end_snapshot.version(), expected_end_version);
        let listing: Vec<TableChangesFileAction> = table_changes
            .scan_file_listing(engine, TableChangesListingMode::AllChanges)
            .unwrap()
            .try_collect()
            .unwrap();
        let paths = listing
            .iter()
            .filter_map(|action| action.add.as_ref().map(|add| add.path.as_str()))
            .collect::<HashSet<_>>();
        assert_eq!(paths, expected_paths.iter().copied().collect());
        assert_eq!(listing.len(), expected_paths.len());
        assert_eq!(
            listing
                .iter()
                .map(|action| action.add.as_ref().unwrap().commit_version)
                .collect::<Vec<_>>(),
            (start_version..=expected_end_version)
                .map(|version| version as i64)
                .collect::<Vec<_>>(),
        );
    }

    #[tokio::test]
    async fn scan_file_listing_preserves_column_mapped_partition_values() {
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new());
        let mut mock_table = LocalMockTable::new();
        let schema = test_schema_flat_with_column_mapping();
        let mut properties = row_tracking_properties();
        properties.insert(COLUMN_MAPPING_MODE.to_string(), "name".to_string());
        let metadata =
            Metadata::try_new(None, None, schema, vec!["name".to_string()], 0, properties).unwrap();
        let protocol = Protocol::try_new_modern(
            [TableFeature::ColumnMapping],
            [
                TableFeature::ColumnMapping,
                TableFeature::RowTracking,
                TableFeature::DomainMetadata,
            ],
        )
        .unwrap();
        let partition_values = HashMap::from([("phys_name".to_string(), "north".to_string())]);
        mock_table
            .commit([
                Action::Protocol(protocol),
                Action::Metadata(metadata),
                Action::Add(Add {
                    path: "name=north/file.parquet".into(),
                    partition_values: partition_values.clone(),
                    data_change: true,
                    size: 100,
                    base_row_id: Some(0),
                    default_row_commit_version: Some(0),
                    ..Default::default()
                }),
            ])
            .await;

        let table_root = url::Url::from_directory_path(mock_table.table_root()).unwrap();
        let table_changes = Arc::new(
            TableChanges::try_new_row_tracking_cdf_listing(
                table_root,
                engine.as_ref(),
                0,
                Some(0),
                None,
            )
            .unwrap(),
        );
        assert_eq!(
            table_changes.materialized_row_id_column_name().unwrap(),
            TEST_MATERIALIZED_ROW_ID_COLUMN_NAME
        );
        assert_eq!(
            table_changes
                .materialized_row_commit_version_column_name()
                .unwrap(),
            TEST_MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME
        );
        let listing: Vec<TableChangesFileAction> = table_changes
            .scan_file_listing(engine, TableChangesListingMode::AllChanges)
            .unwrap()
            .try_collect()
            .unwrap();

        assert_eq!(listing.len(), 1);
        assert_eq!(
            listing[0].add.as_ref().unwrap().partition_values,
            partition_values
        );
    }

    #[derive(Clone, Copy)]
    enum DvUpdateLayout {
        SameCommit,
        CrossCommit,
    }

    enum Expected {
        Grouped {
            remove_version: i64,
            add_version: i64,
        },
        TwoSingleSided,
    }

    #[rstest::rstest]
    #[case::same_commit_all_changes(
        DvUpdateLayout::SameCommit,
        TableChangesListingMode::AllChanges,
        Expected::Grouped { remove_version: 1, add_version: 1 }
    )]
    #[case::same_commit_net_changes(
        DvUpdateLayout::SameCommit,
        TableChangesListingMode::NetChanges,
        Expected::Grouped { remove_version: 1, add_version: 1 }
    )]
    #[case::cross_commit_all_changes(
        DvUpdateLayout::CrossCommit,
        TableChangesListingMode::AllChanges,
        Expected::TwoSingleSided
    )]
    #[case::cross_commit_net_changes(
        DvUpdateLayout::CrossCommit,
        TableChangesListingMode::NetChanges,
        Expected::Grouped { remove_version: 1, add_version: 2 }
    )]
    #[tokio::test]
    async fn scan_file_listing_groups_dv_update(
        #[case] layout: DvUpdateLayout,
        #[case] mode: TableChangesListingMode,
        #[case] expected: Expected,
    ) {
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new());
        let mut mock_table = LocalMockTable::new();
        let dv1 = test_deletion_vector("vBn[lx{q8@P<9BNH/isA", 2);
        let dv2 = test_deletion_vector("U5OWRz5k%CFT.Td}yCPW", 3);
        let remove_f = |dv: &DeletionVectorDescriptor| {
            Action::Remove(Remove {
                path: "f".into(),
                data_change: true,
                deletion_vector: Some(dv.clone()),
                ..Default::default()
            })
        };
        let add_f = |dv: &DeletionVectorDescriptor, commit_version: i64| {
            Action::Add(Add {
                path: "f".into(),
                data_change: true,
                size: 100,
                deletion_vector: Some(dv.clone()),
                base_row_id: Some(0),
                default_row_commit_version: Some(commit_version),
                ..Default::default()
            })
        };

        mock_table
            .commit(
                row_tracking_setup_actions(listing_test_schema())
                    .into_iter()
                    .chain([add_f(&dv1, 0)]),
            )
            .await;
        let end_version = match layout {
            DvUpdateLayout::SameCommit => {
                mock_table.commit([remove_f(&dv1), add_f(&dv2, 1)]).await;
                1
            }
            DvUpdateLayout::CrossCommit => {
                mock_table.commit([remove_f(&dv1)]).await;
                mock_table.commit([add_f(&dv2, 2)]).await;
                2
            }
        };

        let table_root = url::Url::from_directory_path(mock_table.table_root()).unwrap();
        let table_changes = Arc::new(
            TableChanges::try_new_row_tracking_cdf_listing(
                table_root,
                engine.as_ref(),
                1,
                Some(end_version),
                None,
            )
            .unwrap(),
        );
        let listing: Vec<TableChangesFileAction> = table_changes
            .scan_file_listing(engine, mode)
            .unwrap()
            .try_collect()
            .unwrap();

        match expected {
            Expected::Grouped {
                remove_version,
                add_version,
            } => {
                assert_eq!(listing.len(), 1, "expected one grouped action: {listing:?}");
                let update = &listing[0];
                let remove = update.remove.as_ref().expect("remove side");
                assert_eq!(remove.commit_version, remove_version);
                assert_eq!(remove.deletion_vector, Some(dv1));
                let add = update.add.as_ref().expect("add side");
                assert_eq!(add.commit_version, add_version);
                assert_eq!(add.deletion_vector, Some(dv2));
            }
            Expected::TwoSingleSided => {
                assert_eq!(
                    listing.len(),
                    2,
                    "expected two ungrouped single-sided actions: {listing:?}"
                );
                let all_remove = listing
                    .iter()
                    .find(|fa| fa.remove.is_some())
                    .expect("a standalone remove side");
                assert!(all_remove.add.is_none(), "remove must be single-sided");
                let all_remove = all_remove.remove.as_ref().unwrap();
                assert_eq!(all_remove.commit_version, 1);
                assert_eq!(all_remove.deletion_vector, Some(dv1));
                let all_add = listing
                    .iter()
                    .find(|fa| fa.add.is_some())
                    .expect("a standalone add side");
                assert!(all_add.remove.is_none(), "add must be single-sided");
                let all_add = all_add.add.as_ref().unwrap();
                assert_eq!(all_add.commit_version, 2);
                assert_eq!(all_add.deletion_vector, Some(dv2));
            }
        }
    }

    #[tokio::test]
    async fn scan_builder_rejects_row_tracking_table_changes() {
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new());
        let mut mock_table = LocalMockTable::new();
        mock_table
            .commit(row_tracking_setup_actions(listing_test_schema()))
            .await;

        let table_root = url::Url::from_directory_path(mock_table.table_root()).unwrap();
        let table_changes = TableChanges::try_new_row_tracking_cdf_listing(
            table_root,
            engine.as_ref(),
            0,
            Some(0),
            None,
        )
        .unwrap();
        let res = table_changes.into_scan_builder().build();
        assert_result_error_with_message(
            res,
            "A row-tracking TableChanges cannot be scanned for data",
        );
    }

    #[rstest::rstest]
    #[case::all_changes(TableChangesListingMode::AllChanges)]
    #[case::net_changes(TableChangesListingMode::NetChanges)]
    #[tokio::test]
    async fn scan_file_listing_empty_range_yields_no_actions(
        #[case] mode: TableChangesListingMode,
    ) {
        // A range whose only commit is the row-tracking setup (protocol + metadata, no data files)
        // has no add/remove actions to list, so both modes must return an empty listing.
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new());
        let mut mock_table = LocalMockTable::new();
        mock_table
            .commit(row_tracking_setup_actions(listing_test_schema()))
            .await;

        let table_root = url::Url::from_directory_path(mock_table.table_root()).unwrap();
        let table_changes = Arc::new(
            TableChanges::try_new_row_tracking_cdf_listing(
                table_root,
                engine.as_ref(),
                0,
                Some(0),
                None,
            )
            .unwrap(),
        );
        let listing: Vec<TableChangesFileAction> = table_changes
            .scan_file_listing(engine, mode)
            .unwrap()
            .try_collect()
            .unwrap();
        assert!(listing.is_empty(), "expected an empty listing: {listing:?}");
    }

    struct PublishAfterFirstList {
        inner: Arc<dyn StorageHandler>,
        next_commit: Mutex<Option<(Url, Bytes)>>,
    }

    impl StorageHandler for PublishAfterFirstList {
        fn list_from(&self, path: &Url) -> KernelResult<KernelResultIteratorStatic<FileMeta>> {
            let files: Vec<_> = self.inner.list_from(path)?.try_collect()?;
            let next_commit = self.next_commit.lock().unwrap().take();
            if let Some((path, data)) = next_commit {
                self.inner.put(&path, data, false)?;
            }
            Ok(Box::new(files.into_iter().map(Ok)))
        }

        fn read_files(
            &self,
            files: Vec<FileSlice>,
        ) -> KernelResult<KernelResultIteratorStatic<Bytes>> {
            self.inner.read_files(files)
        }

        fn copy_atomic(&self, src: &Url, dest: &Url) -> KernelResult<()> {
            self.inner.copy_atomic(src, dest)
        }

        fn put(&self, path: &Url, data: Bytes, overwrite: bool) -> KernelResult<()> {
            self.inner.put(path, data, overwrite)
        }

        fn head(&self, path: &Url) -> KernelResult<FileMeta> {
            self.inner.head(path)
        }

        fn delete(&self, path: &Url) -> KernelResult<()> {
            self.inner.delete(path)
        }
    }
}

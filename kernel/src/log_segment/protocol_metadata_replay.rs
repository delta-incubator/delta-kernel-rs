//! Protocol and Metadata replay logic for [`LogSegment`].
//!
//! This module contains the methods that perform a lightweight log replay to extract the latest
//! Protocol and Metadata actions from a [`LogSegment`].

use std::sync::{Arc, LazyLock};

use tracing::{info, instrument};
use url::Url;

use super::LogSegment;
#[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
use super::{inconsistent_pointer, require_pointed_checkpoint_action};
#[cfg(feature = "adaptive-metadata-in-dev")]
use crate::actions::visitors::LastManifestCommitVisitor;
#[cfg(feature = "adaptive-metadata-in-dev")]
use crate::actions::{CheckpointAction, LastManifestCommit, CHECKPOINT_ACTION_FIELD};
use crate::actions::{Metadata, Protocol, METADATA_FIELD, PROTOCOL_FIELD};
#[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
use crate::actions::{CHECKPOINT_ACTION_NAME, COMMIT_INFO_NAME};
#[cfg(feature = "declarative-plans")]
use crate::actions::{METADATA_NAME, PROTOCOL_NAME};
use crate::crc::Crc;
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
#[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
use crate::expressions::lit;
#[cfg(feature = "declarative-plans")]
use crate::expressions::{col, Predicate};
use crate::log_replay::ActionsBatch;
use crate::metrics::ProtocolMetadataSource;
use crate::path::ParsedLogPath;
#[cfg(feature = "declarative-plans")]
use crate::plans::ir::nodes::Agg;
#[cfg(feature = "declarative-plans")]
use crate::plans::ir::nodes::FileType;
#[cfg(feature = "declarative-plans")]
use crate::plans::ir::plan::Plan;
#[cfg(feature = "declarative-plans")]
use crate::plans::{Operation, PlanBuilder, PlanExecutor};
#[cfg(feature = "declarative-plans")]
use crate::schema::SchemaRef;
use crate::schema::{
    column_name, schema_ref, ColumnName, ColumnNamesAndTypes, DataType, MetadataColumnSpec,
    StructField, StructType,
};
use crate::{
    version_as_i64, Engine, EngineData, KernelError, KernelResult, KernelResultIterator, Version,
};

impl LogSegment {
    /// Read the latest Protocol and Metadata from this log segment, using CRC when available.
    /// The result's `metadata` and `protocol` are `None` if not found.
    ///
    /// The `crc` parameter is the CRC eagerly resolved by the caller; it is used to
    /// short-circuit or seed the replay.
    ///
    /// Under `adaptive-metadata-in-dev`, the result's `captured_checkpoint_action` is the latest
    /// AMT checkpoint action when replay captured it, for the caller to store on the
    /// [`Snapshot`](crate::Snapshot).
    #[instrument(name = "log_seg.load_p_m", skip_all, fields(enable_call_frame), err)]
    pub(crate) fn read_protocol_metadata_opt(
        &self,
        engine: &dyn Engine,
        crc: Option<&Arc<Crc>>,
    ) -> KernelResult<PmResolution> {
        // Case 1: If CRC at target version, use it directly and exit early.
        if let Some(crc) = crc.filter(|c| c.version == self.end_version) {
            info!("P&M from CRC at target version {}", self.end_version);
            return Ok(PmResolution::from_crc(
                crc,
                ProtocolMetadataSource::CrcAtTarget,
            ));
        }

        // We didn't return above, so we need to do log replay to find P&M.
        //
        // Case 2: CRC exists at an earlier version => Prune the log segment to only replay
        //         commits *after* the CRC version.
        //   (a) If we find new P&M in the pruned replay, return it.
        //   (b) If we don't find new P&M, fall back to the CRC.
        //
        // Case 3: No CRC exists => Full P&M log replay.

        if let Some(crc) = crc.filter(|c| c.version < self.end_version) {
            // Case 2(a): Replay only commits after CRC version
            info!(
                "Pruning log segment to commits after CRC version {}",
                crc.version
            );
            let pruned = self.segment_after_version(crc.version);
            let candidate = pruned.replay_for_pm(
                engine,
                #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
                true,
            )?;
            // Ignore pruned P&M at or below the CRC version: a lagging AMT checkpoint action can
            // carry it, and the CRC's P&M is at least as new.
            let (protocol_opt, metadata_opt) =
                candidate.pm_newer_than(version_as_i64(crc.version)?);
            let (protocol_opt, metadata_opt) = (protocol_opt.cloned(), metadata_opt.cloned());
            // The pruned replay only reads commits after the CRC version, but a hit there is still
            // the latest action: any action at or below the CRC is older.
            #[cfg(feature = "adaptive-metadata-in-dev")]
            let (captured_checkpoint_action, last_manifest_commit) = candidate.amt_resolutions();

            if metadata_opt.is_some() && protocol_opt.is_some() {
                info!("Found P&M from pruned log replay");
                return Ok(PmResolution {
                    metadata: metadata_opt,
                    protocol: protocol_opt,
                    source: ProtocolMetadataSource::CrcSeededPmOnlyReplay,
                    #[cfg(feature = "adaptive-metadata-in-dev")]
                    captured_checkpoint_action,
                    #[cfg(feature = "adaptive-metadata-in-dev")]
                    last_manifest_commit,
                });
            }

            // Case 2(b): P&M incomplete or older than the CRC, use the CRC.
            // Use `or_else` so any newer P or M found in the pruned replay takes priority
            // over the (older) CRC values.
            info!("P&M fallback to CRC (no P&M changes after CRC version)");
            return Ok(PmResolution {
                metadata: metadata_opt.or_else(|| Some(crc.metadata.clone())),
                protocol: protocol_opt.or_else(|| Some(crc.protocol.clone())),
                source: ProtocolMetadataSource::CrcSeededPmOnlyReplay,
                #[cfg(feature = "adaptive-metadata-in-dev")]
                captured_checkpoint_action,
                #[cfg(feature = "adaptive-metadata-in-dev")]
                last_manifest_commit,
            });
        }

        // Case 3: Full P&M log replay.
        let candidate = self.replay_for_pm(
            engine,
            #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
            self.checkpoint_version.is_some(),
        )?;
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let (captured_checkpoint_action, last_manifest_commit) = candidate.amt_resolutions();
        Ok(PmResolution {
            metadata: candidate.metadata.map(|(_, m)| m),
            protocol: candidate.protocol.map(|(_, p)| p),
            source: ProtocolMetadataSource::FullReplay,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            captured_checkpoint_action,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            last_manifest_commit,
        })
    }

    /// The commit at `end_version`, if this segment lists it (it does not when the segment ends
    /// on a checkpoint).
    #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
    fn newest_commit(&self) -> Option<&ParsedLogPath> {
        self.listed
            .ascending_commit_files
            .last()
            .filter(|commit| commit.version == self.end_version)
    }

    /// Replays the log segment for the latest Protocol and Metadata, each with its version.
    /// `bounded` says whether a CRC or a checkpoint file bounds the replay.
    fn replay_for_pm(
        &self,
        engine: &dyn Engine,
        #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
        bounded: bool,
    ) -> KernelResult<PmCandidate> {
        #[cfg(feature = "declarative-plans")]
        if let Some(executor) = engine.plan_executor() {
            return self.replay_for_pm_via_plan(
                executor.as_ref(),
                #[cfg(feature = "adaptive-metadata-in-dev")]
                bounded,
            );
        }
        let candidate = resolve_pm_batches(self.read_pm_batches(engine)?)?;
        // Only a malformed log has a pointer past its own commit; reject it before it is cached.
        #[cfg(feature = "adaptive-metadata-in-dev")]
        candidate.ensure_pointer_at_or_before(self.end_version)?;
        Ok(candidate)
    }

    /// [`Self::replay_for_pm`] through declarative plans. The workflow is meant not to regress
    /// existing P&M replay: a replay bounded by a CRC or a checkpoint file runs one P&M plan, which
    /// also reads the newest commit's `commitInfo`. Only an unbounded replay reads the newest
    /// commit first, so its `lastManifestCommit` can bound the rest.
    #[cfg(feature = "declarative-plans")]
    fn replay_for_pm_via_plan(
        &self,
        executor: &dyn PlanExecutor,
        #[cfg(feature = "adaptive-metadata-in-dev")] bounded: bool,
    ) -> KernelResult<PmCandidate> {
        #[cfg(feature = "adaptive-metadata-in-dev")]
        if let Some(newest_commit) = self.newest_commit().filter(|_| !bounded) {
            return self.replay_for_pm_from_newest_commit(executor, newest_commit);
        }
        resolve_pm_batches(self.read_pm_batches_via_plan(
            executor,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            None,
        )?)
    }

    /// Replays P&M by first reading the newest commit in its own plan, then at most one more plan
    /// when that commit's P&M are not final. The newest commit's `lastManifestCommit` bounds that
    /// plan: a checkpoint action holds the P&M as of its `checkpointMetadata.version`, which is at
    /// least its content root version, so the plan reads only commits after that version (after the
    /// pointer's content root version when the action is not yet read). The plan also captures the
    /// pointed checkpoint action from the manifest commit itself when the newest commit lacks it
    /// and this segment lists that commit.
    ///
    /// # Errors
    /// Returns an error if the newest commit's `lastManifestCommit` names a later commit, if the
    /// newest commit carries a checkpoint action without a pointer naming it, or if replay needs
    /// the pointed action's P&M and the pointed commit lacks a matching checkpoint action.
    #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
    fn replay_for_pm_from_newest_commit(
        &self,
        executor: &dyn PlanExecutor,
        newest_commit: &ParsedLogPath,
    ) -> KernelResult<PmCandidate> {
        let newest =
            resolve_pm_batches(Self::read_newest_commit_via_plan(executor, newest_commit)?)?;
        newest.ensure_pointer_at_or_before(self.end_version)?;
        let pointer = newest.last_manifest_commit.clone();
        let end_version = version_as_i64(self.end_version)?;
        // A commit carries a checkpoint action exactly when it is the manifest commit its pointer
        // names.
        let newest = match &pointer {
            Some(pointer) if pointer.version == end_version => {
                newest.require_pointed_checkpoint(pointer)?
            }
            Some(pointer) if newest.checkpoint.is_some() => {
                return Err(inconsistent_pointer(
                    pointer,
                    &format!("commit {end_version} carries a checkpoint action"),
                ))
            }
            None if newest.checkpoint.is_some() => {
                return Err(KernelError::invalid_log_segment(format!(
                    "commit {end_version} carries a checkpoint action but no lastManifestCommit"
                )))
            }
            _ => newest,
        };
        if is_final(&newest.protocol, Some(end_version))
            && is_final(&newest.metadata, Some(end_version))
        {
            return Ok(newest);
        }
        // An unlisted pointed commit is at or below where this segment starts, so whatever covers
        // that start has P&M at least as new, and the action is left to the lazy pointer read.
        let (capture, checkpointed_version) = match (&newest.checkpoint, &pointer) {
            (Some((version, _)), _) => (None, Some(*version)),
            (None, Some(pointer)) => match self.find_pointed_commit(pointer) {
                Some(commit) => (Some((commit, pointer)), Some(pointer.content_root_version)),
                None => (None, None),
            },
            (None, None) => (None, None),
        };
        let checkpointed_version = checkpointed_version
            .map(|version| {
                Version::try_from(version).map_err(|_| {
                    KernelError::internal_error(format!("negative checkpoint version {version}"))
                })
            })
            .transpose()?;
        let trimmed = checkpointed_version.map(|version| self.segment_after_version(version));
        let replayed = trimmed.as_ref().unwrap_or(self);
        let rest = resolve_pm_batches(
            replayed.read_pm_batches_via_plan(executor, capture.map(|(commit, _)| commit))?,
        )?;
        let resolved = newest.merge(rest);
        match capture {
            // The trimmed replay skips P&M at or below the content root, so the action is needed
            // only when replay lacks Protocol or Metadata without it.
            Some((_, pointer))
                if resolved.checkpoint.is_some()
                    || resolved.protocol.is_none()
                    || resolved.metadata.is_none() =>
            {
                resolved.require_pointed_checkpoint(pointer)
            }
            _ => Ok(resolved),
        }
    }

    /// Reads the newest `commit`'s Protocol, Metadata, checkpoint action, and `lastManifestCommit`
    /// with a single-file plan.
    #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
    fn read_newest_commit_via_plan(
        executor: &dyn PlanExecutor,
        commit: &ParsedLogPath,
    ) -> KernelResult<impl Iterator<Item = KernelResult<VersionedBatch>> + Send> {
        let captured = [&*CHECKPOINT_ACTION_FIELD];
        let scan = PlanBuilder::scan_json(
            Self::version_tagged_scan_files([commit])?,
            &["version"],
            pm_plan_schema(&captured)?,
        )?;
        let plan = newest_pm_plan(scan, &captured)?;
        execute_pm_plan(executor, plan, true, commit.version)
    }

    /// Builds the declarative plan that selects the latest Protocol and Metadata actions. Under
    /// `adaptive-metadata-in-dev`, it also selects the newest `commitInfo`, and `capture` names the
    /// manifest commit whose checkpoint action the plan also selects.
    #[cfg(feature = "declarative-plans")]
    fn build_pm_plan(
        &self,
        #[cfg(feature = "adaptive-metadata-in-dev")] capture: Option<&ParsedLogPath>,
    ) -> KernelResult<Plan> {
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let (mut commit_paths, captured, checkpoint_arm) = match capture {
            Some(manifest_commit) => {
                // A compaction spanning the manifest commit is tagged with one version for all its
                // actions and may drop the checkpoint action, so read the commits around it one
                // by one.
                let mut paths = self.find_commit_cover_paths_reading(manifest_commit.version);
                // The manifest commit can sit just before a segment trimmed to its content root.
                if !paths.contains(manifest_commit) {
                    paths.push(manifest_commit.clone());
                }
                // No compaction spans the manifest commit, so only its own file has its version.
                let checkpoint_arm = Predicate::and(
                    col!(CHECKPOINT_ACTION_NAME).is_not_null(),
                    col!("version").eq(lit(manifest_commit.version_as_i64()?)),
                );
                (paths, vec![&*CHECKPOINT_ACTION_FIELD], Some(checkpoint_arm))
            }
            None => (self.find_commit_cover_paths(), vec![], None),
        };
        // Log compactions drop commitInfo, so read the newest commit itself when one covers it.
        #[cfg(feature = "adaptive-metadata-in-dev")]
        if let Some(newest_commit) = self.newest_commit() {
            if !commit_paths.contains(newest_commit) {
                commit_paths.push(newest_commit.clone());
            }
        }
        #[cfg(not(feature = "adaptive-metadata-in-dev"))]
        let (commit_paths, captured, checkpoint_arm) = (
            self.find_commit_cover_paths(),
            Vec::new(),
            None::<Predicate>,
        );
        let schema = pm_plan_schema(&captured)?;

        let commits = PlanBuilder::scan_json(
            Self::version_tagged_scan_files(&commit_paths)?,
            &["version"],
            schema.clone(),
        )?;

        // A checkpoint's parts share one format; scan them with the matching operator.
        let checkpoint = self
            .checkpoint_version_tagged_scan_files()?
            .map(|(file_type, checkpoint_files)| {
                let scan = match file_type {
                    FileType::Json => PlanBuilder::scan_json,
                    FileType::Parquet => PlanBuilder::scan_parquet,
                };
                scan(checkpoint_files, &["version"], schema.clone())
            })
            .transpose()?;

        // Required fields are non-null exactly when their Protocol or Metadata action is present.
        // Filter on required leaf fields so readers can use row group skipping.
        let relevant_action = Predicate::or(
            col!(PROTOCOL_NAME, "minReaderVersion").is_not_null(),
            col!(METADATA_NAME, "id").is_not_null(),
        );
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let relevant_action = Predicate::or(relevant_action, col!(COMMIT_INFO_NAME).is_not_null());
        let relevant_action = match checkpoint_arm {
            Some(checkpoint_arm) => Predicate::or(relevant_action, checkpoint_arm),
            None => relevant_action,
        };

        let relevant = PlanBuilder::union_all(std::iter::once(commits).chain(checkpoint))?
            .filter(relevant_action)?;
        newest_pm_plan(relevant, &captured)
    }

    /// Reads the P&M commit cover and checkpoint via the declarative plan, tagging each batch with
    /// its version. `capture` is as for [`Self::build_pm_plan`].
    #[cfg(feature = "declarative-plans")]
    fn read_pm_batches_via_plan(
        &self,
        executor: &dyn PlanExecutor,
        #[cfg(feature = "adaptive-metadata-in-dev")] capture: Option<&ParsedLogPath>,
    ) -> KernelResult<impl Iterator<Item = KernelResult<VersionedBatch>> + Send> {
        let plan = self.build_pm_plan(
            #[cfg(feature = "adaptive-metadata-in-dev")]
            capture,
        )?;
        execute_pm_plan(
            executor,
            plan,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            capture.is_some(),
            #[cfg(not(feature = "adaptive-metadata-in-dev"))]
            false,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            self.end_version,
        )
    }

    /// Reads the P&M commit cover and checkpoint, tagging each batch with its version.
    fn read_pm_batches(
        &self,
        engine: &dyn Engine,
    ) -> KernelResult<impl KernelResultIterator<VersionedBatch>> {
        let (commit_schema, checkpoint_schema) = pm_replay_schemas();
        // Commit schema only: `_file` in the checkpoint schema would break its skipping predicate.
        let file_column =
            StructField::create_metadata_column("_file", MetadataColumnSpec::FilePath);
        let commit_schema = Arc::new(StructType::try_new(
            commit_schema.fields().cloned().chain([file_column]),
        )?);
        let checkpoint_version = self.checkpoint_version.map(|v| v as i64);
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let end_version = version_as_i64(self.end_version)?;
        let batches = self
            .read_actions_with_projected_checkpoint_actions(
                engine,
                commit_schema,
                checkpoint_schema,
                None,
                None,
                None,
                None,
            )?
            .actions;
        Ok(batches.map(move |batch| {
            let batch = batch?;
            // A commit's version is parsed from its `_file`; a checkpoint batch uses the constant.
            let version = if batch.is_log_batch {
                batch_version(batch.actions.as_ref())? as i64
            } else {
                checkpoint_version.ok_or_else(|| {
                    KernelError::internal_error("checkpoint batch without a version")
                })?
            };
            Ok(VersionedBatch {
                protocol_version: Some(version),
                metadata_version: Some(version),
                #[cfg(feature = "adaptive-metadata-in-dev")]
                is_newest_commit: batch.is_log_batch && version == end_version,
                batch,
            })
        }))
    }
}

/// How a snapshot's `lastManifestCommit` was resolved at build time, stored on the
/// [`Snapshot`](crate::Snapshot) so consumers can read it without opening the log.
#[cfg(feature = "adaptive-metadata-in-dev")]
#[derive(Debug, Clone)]
pub(crate) enum LastManifestCommitResolution {
    /// Read from the CRC at the snapshot version, the newest commit's commitInfo, or the
    /// committing transaction. `None` means the table has no manifest commit at this version.
    Resolved(Option<LastManifestCommit>),
    /// The build did not find the snapshot version's `lastManifestCommit`; consumers read it from
    /// that version's commit file.
    Unresolved,
}

#[cfg(feature = "adaptive-metadata-in-dev")]
impl LastManifestCommitResolution {
    /// Resolves the `lastManifestCommit` that P&M replay read from the newest commit. A miss is
    /// `Unresolved` because replay may not have read that commit (e.g. the segment ends on a
    /// checkpoint).
    fn from_replay(captured: Option<LastManifestCommit>) -> Self {
        match captured {
            Some(pointer) => Self::Resolved(Some(pointer)),
            None => Self::Unresolved,
        }
    }
}

/// Result of a P&M resolution (see [`LogSegment::read_protocol_metadata_opt`]).
pub(crate) struct PmResolution {
    pub(crate) metadata: Option<Metadata>,
    pub(crate) protocol: Option<Protocol>,
    /// How the Protocol and Metadata were resolved.
    pub(crate) source: ProtocolMetadataSource,
    /// The latest AMT `checkpoint` action, when replay happened to read it. This only saves a
    /// read: `lastManifestCommit`, which the CRC or the snapshot version's `commitInfo` always
    /// carries, is the authority on the action and the snapshot follows it when this is `None`.
    /// A hit is the latest action, since replay keeps the highest `checkpointMetadata.version`
    /// it reaches and checkpoint versions strictly increase.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    pub(crate) captured_checkpoint_action: Option<Arc<CheckpointAction>>,
    /// How the segment's `lastManifestCommit` was resolved. See [`LastManifestCommitResolution`].
    #[cfg(feature = "adaptive-metadata-in-dev")]
    pub(crate) last_manifest_commit: LastManifestCommitResolution,
}

impl PmResolution {
    /// P&M taken from a CRC that short-circuits replay. With no replay to capture the checkpoint
    /// action, the CRC's [`LastManifestCommit`] is what later locates it.
    pub(crate) fn from_crc(crc: &Crc, source: ProtocolMetadataSource) -> Self {
        Self {
            metadata: Some(crc.metadata.clone()),
            protocol: Some(crc.protocol.clone()),
            source,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            captured_checkpoint_action: None,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            last_manifest_commit: LastManifestCommitResolution::Resolved(
                crc.last_manifest_commit_opt.clone(),
            ),
        }
    }
}

/// Protocol and Metadata, each tagged with the version it was found at. Holds both a single
/// batch's parse and the newest resolved across batches.
struct PmCandidate {
    protocol: Option<(i64, Protocol)>,
    metadata: Option<(i64, Metadata)>,
    /// The AMT checkpoint action found in this batch, or the latest one across batches, tagged
    /// with its `checkpointMetadata.version` so it ranks the same way as Protocol and Metadata.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    checkpoint: Option<(i64, CheckpointAction)>,
    /// The newest commit's `commitInfo.lastManifestCommit`, if replay read it.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    last_manifest_commit: Option<LastManifestCommit>,
}

impl PmCandidate {
    /// The Protocol and Metadata found above `version`, each `None` when it is at or below it.
    fn pm_newer_than(&self, version: i64) -> (Option<&Protocol>, Option<&Metadata>) {
        let protocol = self.protocol.as_ref().filter(|(v, _)| *v > version);
        let metadata = self.metadata.as_ref().filter(|(v, _)| *v > version);
        (protocol.map(|(_, p)| p), metadata.map(|(_, m)| m))
    }

    /// The AMT checkpoint action replay captured, if any, and how it resolved `lastManifestCommit`,
    /// for the snapshot.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    fn amt_resolutions(&self) -> (Option<Arc<CheckpointAction>>, LastManifestCommitResolution) {
        (
            self.checkpoint.as_ref().map(|(_, c)| Arc::new(c.clone())),
            LastManifestCommitResolution::from_replay(self.last_manifest_commit.clone()),
        )
    }

    /// Errors unless the `lastManifestCommit` read from the newest commit, at `version`, names a
    /// manifest commit at or before it.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    fn ensure_pointer_at_or_before(&self, version: Version) -> KernelResult<()> {
        self.last_manifest_commit
            .as_ref()
            .map_or(Ok(()), |pointer| pointer.ensure_at_or_before(version))
    }

    /// Combines two replay results, keeping each field's higher-versioned value and the first
    /// `lastManifestCommit`.
    #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
    fn merge(self, other: PmCandidate) -> Self {
        PmCandidate {
            protocol: newer(self.protocol, other.protocol),
            metadata: newer(self.metadata, other.metadata),
            checkpoint: newer(self.checkpoint, other.checkpoint),
            last_manifest_commit: self.last_manifest_commit.or(other.last_manifest_commit),
        }
    }

    /// Checks the captured checkpoint action against `pointer`, the `lastManifestCommit` naming
    /// the commit replay read it from.
    ///
    /// # Errors
    /// Returns an error if replay captured no checkpoint action or one with another content root
    /// version.
    #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
    fn require_pointed_checkpoint(mut self, pointer: &LastManifestCommit) -> KernelResult<Self> {
        let action = require_pointed_checkpoint_action(
            pointer,
            self.checkpoint.take().map(|(_, action)| action),
        )?;
        self.checkpoint = Some((action.version(), action));
        Ok(self)
    }
}

/// A P&M-projected batch with the versions to rank its Protocol and Metadata at.
struct VersionedBatch {
    protocol_version: Option<i64>,
    metadata_version: Option<i64>,
    /// Whether the batch comes from the segment's newest commit, whose `lastManifestCommit`
    /// describes the segment's version.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    is_newest_commit: bool,
    batch: ActionsBatch,
}

/// The newest Protocol and Metadata across `batches`.
fn resolve_pm_batches(
    batches: impl KernelResultIterator<VersionedBatch>,
) -> KernelResult<PmCandidate> {
    let mut metadata: Option<(i64, Metadata)> = None;
    let mut protocol: Option<(i64, Protocol)> = None;
    #[cfg(feature = "adaptive-metadata-in-dev")]
    let mut checkpoint: Option<(i64, CheckpointAction)> = None;
    #[cfg(feature = "adaptive-metadata-in-dev")]
    let mut last_manifest_commit: Option<LastManifestCommit> = None;
    for batch in batches {
        let VersionedBatch {
            protocol_version,
            metadata_version,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            is_newest_commit,
            batch,
        } = batch?;
        let batch_version = protocol_version.max(metadata_version);
        let candidate = pm_candidate(
            &batch,
            protocol_version,
            metadata_version,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            is_newest_commit,
        )?;
        metadata = newer(metadata, candidate.metadata);
        protocol = newer(protocol, candidate.protocol);
        // Keep the highest-versioned checkpoint action, ranked by version like P&M (not by
        // iteration order). Best-effort: if the loop breaks below before reaching one, it stays
        // `None`.
        #[cfg(feature = "adaptive-metadata-in-dev")]
        {
            checkpoint = newer(checkpoint, candidate.checkpoint);
            last_manifest_commit = last_manifest_commit.or(candidate.last_manifest_commit);
        }
        // A checkpoint action's P&M can be older than its commit, so check version not presence.
        if is_final(&protocol, batch_version) && is_final(&metadata, batch_version) {
            break;
        }
    }
    Ok(PmCandidate {
        protocol,
        metadata,
        #[cfg(feature = "adaptive-metadata-in-dev")]
        checkpoint,
        #[cfg(feature = "adaptive-metadata-in-dev")]
        last_manifest_commit,
    })
}

/// Parses the log version from a batch's `_file` metadata column.
fn batch_version(data: &dyn EngineData) -> KernelResult<Version> {
    #[derive(Default)]
    struct FilePathVisitor {
        file: Option<String>,
    }
    impl RowVisitor for FilePathVisitor {
        fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
            static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
                LazyLock::new(|| (vec![column_name!("_file")], vec![DataType::STRING]).into());
            NAMES_AND_TYPES.as_ref()
        }
        fn visit<'a>(
            &mut self,
            row_count: usize,
            getters: &[&'a dyn GetData<'a>],
        ) -> KernelResult<()> {
            if self.file.is_none() && row_count > 0 {
                self.file = getters[0].get_opt(0, "_file")?;
            }
            Ok(())
        }
    }
    let mut visitor = FilePathVisitor::default();
    visitor.visit_rows_of(data)?;
    let file = visitor
        .file
        .ok_or_else(|| KernelError::internal_error("commit batch missing _file column"))?;
    let url = Url::parse(&file)
        .map_err(|e| KernelError::internal_error(format!("batch has invalid _file {file}: {e}")))?;
    ParsedLogPath::try_from(url)?
        .map(|path| path.version)
        .ok_or_else(|| KernelError::internal_error(format!("batch from non-log file {file}")))
}

/// Whether `winner` is set at a version at least `batch_version`.
fn is_final<T>(winner: &Option<(i64, T)>, batch_version: Option<i64>) -> bool {
    match (winner, batch_version) {
        (Some((version, _)), Some(bv)) => *version >= bv,
        _ => false,
    }
}

/// The Protocol and Metadata nested in a checkpoint action, each tagged with the action's
/// `checkpointMetadata.version`.
#[cfg(feature = "adaptive-metadata-in-dev")]
fn checkpoint_pm(checkpoint: &CheckpointAction) -> ((i64, Protocol), (i64, Metadata)) {
    let version = checkpoint.version();
    (
        (version, checkpoint.protocol().clone()),
        (version, checkpoint.metadata().clone()),
    )
}

/// The higher-versioned of `a` and `b`; on a tie, `b` wins.
fn newer<T>(a: Option<(i64, T)>, b: Option<(i64, T)>) -> Option<(i64, T)> {
    match (a, b) {
        (Some(a), Some(b)) => {
            if b.0 >= a.0 {
                Some(b)
            } else {
                Some(a)
            }
        }
        (a, b) => a.or(b),
    }
}

/// The commit and checkpoint read schemas for P&M replay; the commit schema also reads the AMT
/// `checkpoint` action and `commitInfo.lastManifestCommit`.
fn pm_replay_schemas() -> (Arc<StructType>, Arc<StructType>) {
    let checkpoint_schema = schema_ref! {
        (&PROTOCOL_FIELD),
        (&METADATA_FIELD),
    };
    #[cfg(feature = "adaptive-metadata-in-dev")]
    let commit_schema = schema_ref! {
        (&PROTOCOL_FIELD),
        (&METADATA_FIELD),
        (&CHECKPOINT_ACTION_FIELD),
        (LastManifestCommitVisitor::commit_info_field()),
    };
    #[cfg(not(feature = "adaptive-metadata-in-dev"))]
    let commit_schema = checkpoint_schema.clone();
    (commit_schema, checkpoint_schema)
}

/// The Protocol and Metadata in `batch`, tagged with the given versions, plus any AMT checkpoint
/// action's nested P&M at its own version and, for the newest commit, its `lastManifestCommit`.
fn pm_candidate(
    batch: &ActionsBatch,
    protocol_version: Option<i64>,
    metadata_version: Option<i64>,
    #[cfg(feature = "adaptive-metadata-in-dev")] is_newest_commit: bool,
) -> KernelResult<PmCandidate> {
    let actions = batch.actions.as_ref();
    let protocol = protocol_version.zip(Protocol::try_new_from_data(actions)?);
    let metadata = metadata_version.zip(Metadata::try_new_from_data(actions)?);

    #[cfg(feature = "adaptive-metadata-in-dev")]
    {
        // A checkpoint action's nested P&M is at its own `checkpointMetadata.version`. Only parse
        // it from batches projected with the checkpoint-action column: `is_log_batch` marks
        // exactly those. The non-plan read projects the column only for commits, not checkpoint
        // parts, and the plan reads mark their output by whether their schema includes it.
        let checkpoint = if batch.is_log_batch {
            CheckpointAction::try_new_from_data(actions)?
        } else {
            None
        };
        let (checkpoint_protocol, checkpoint_metadata) =
            checkpoint.as_ref().map(checkpoint_pm).unzip();
        let last_manifest_commit = if is_newest_commit {
            LastManifestCommit::try_new_from_data(actions)?
        } else {
            None
        };
        Ok(PmCandidate {
            protocol: newer(protocol, checkpoint_protocol),
            metadata: newer(metadata, checkpoint_metadata),
            checkpoint: checkpoint.map(|c| (c.version(), c)),
            last_manifest_commit,
        })
    }

    #[cfg(not(feature = "adaptive-metadata-in-dev"))]
    Ok(PmCandidate { protocol, metadata })
}

/// The P&M plan read schema: Protocol, Metadata, under `adaptive-metadata-in-dev` `commitInfo`,
/// the `captured` actions, and the file-constant `version`.
#[cfg(feature = "declarative-plans")]
fn pm_plan_schema(captured: &[&StructField]) -> KernelResult<SchemaRef> {
    let actions = [
        &*PROTOCOL_FIELD,
        &*METADATA_FIELD,
        #[cfg(feature = "adaptive-metadata-in-dev")]
        LastManifestCommitVisitor::commit_info_field(),
    ]
    .into_iter()
    .chain(captured.iter().copied())
    .cloned();
    let version = StructField::not_null("version", DataType::LONG);
    Ok(Arc::new(StructType::try_new(actions.chain([version]))?))
}

/// Aggregates `input` to its newest Protocol and Metadata, with their versions, under
/// `adaptive-metadata-in-dev` its newest `commitInfo` with its version, and the newest of each
/// `captured` action.
#[cfg(feature = "declarative-plans")]
fn newest_pm_plan(input: PlanBuilder, captured: &[&StructField]) -> KernelResult<Plan> {
    input
        .aggregate_ungrouped(|a| {
            let protocol = || column_name!(PROTOCOL_NAME);
            let metadata = || column_name!(METADATA_NAME);
            let version = || column_name!("version");
            // The version aggregates are aliased; unaliased both would be named `version`.
            let a = a
                .max_non_null_by(protocol(), protocol(), version())
                .max_non_null_by(metadata(), metadata(), version())
                .aggregate_as(
                    Agg::max_non_null_by(version(), protocol(), version()),
                    "protocol_version",
                )
                .aggregate_as(
                    Agg::max_non_null_by(version(), metadata(), version()),
                    "metadata_version",
                );
            #[cfg(feature = "adaptive-metadata-in-dev")]
            let a = {
                let commit_info = || column_name!(COMMIT_INFO_NAME);
                a.max_non_null_by(commit_info(), commit_info(), version())
                    .aggregate_as(
                        Agg::max_non_null_by(version(), commit_info(), version()),
                        "commit_info_version",
                    )
            };
            captured.iter().fold(a, |a, field| {
                let action = || ColumnName::new([field.name()]);
                a.max_non_null_by(action(), action(), version())
            })
        })?
        .build()
}

/// Executes a P&M `plan`, tagging its output with the aggregated P&M versions. `has_checkpoint`
/// says whether the plan reads the checkpoint action column. Under `adaptive-metadata-in-dev`, the
/// output's `commitInfo` is taken only when it comes from the commit at `end_version`.
#[cfg(feature = "declarative-plans")]
fn execute_pm_plan(
    executor: &dyn PlanExecutor,
    plan: Plan,
    has_checkpoint: bool,
    #[cfg(feature = "adaptive-metadata-in-dev")] end_version: Version,
) -> KernelResult<impl Iterator<Item = KernelResult<VersionedBatch>> + Send> {
    #[cfg(feature = "adaptive-metadata-in-dev")]
    let end_version = version_as_i64(end_version)?;
    let batches = executor
        .execute_op(Operation::QueryPlan(plan))?
        .into_data()?
        .map(move |batch| {
            // `pm_candidate` parses the checkpoint action only from log batches.
            let batch = ActionsBatch::new(batch?, has_checkpoint);
            let versions = PlanOutputVersions::read(batch.actions.as_ref())?;
            Ok(VersionedBatch {
                protocol_version: versions.protocol,
                metadata_version: versions.metadata,
                #[cfg(feature = "adaptive-metadata-in-dev")]
                is_newest_commit: versions.commit_info == Some(end_version),
                batch,
            })
        });
    Ok(batches)
}

/// The version columns a P&M plan's aggregate emits.
#[cfg(feature = "declarative-plans")]
#[derive(Default)]
struct PlanOutputVersions {
    protocol: Option<i64>,
    metadata: Option<i64>,
    #[cfg(feature = "adaptive-metadata-in-dev")]
    commit_info: Option<i64>,
}

#[cfg(feature = "declarative-plans")]
impl PlanOutputVersions {
    fn read(actions: &dyn EngineData) -> KernelResult<Self> {
        let mut versions = Self::default();
        versions.visit_rows_of(actions)?;
        Ok(versions)
    }
}

#[cfg(feature = "declarative-plans")]
impl RowVisitor for PlanOutputVersions {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            let names = vec![
                column_name!("protocol_version"),
                column_name!("metadata_version"),
                #[cfg(feature = "adaptive-metadata-in-dev")]
                column_name!("commit_info_version"),
            ];
            let types = vec![DataType::LONG; names.len()];
            (names, types).into()
        });
        NAMES_AND_TYPES.as_ref()
    }
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        if row_count > 0 {
            self.protocol = getters[0].get_opt(0, "protocol_version")?;
            self.metadata = getters[1].get_opt(0, "metadata_version")?;
            #[cfg(feature = "adaptive-metadata-in-dev")]
            {
                self.commit_info = getters[2].get_opt(0, "commit_info_version")?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    #[cfg(feature = "declarative-plans")]
    use std::sync::Arc;

    use itertools::Itertools;
    use test_log::test;

    use crate::engine::sync::SyncEngine;
    #[cfg(feature = "declarative-plans")]
    use crate::engine::test_delegating::DelegatingEngine;
    #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
    use crate::expressions::lit;
    #[cfg(feature = "declarative-plans")]
    use crate::expressions::{col, Predicate};
    #[cfg(feature = "declarative-plans")]
    use crate::log_segment::LogSegment;
    #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
    use crate::path::ParsedLogPath;
    #[cfg(feature = "declarative-plans")]
    use crate::plans::ir::nodes::Operator;
    #[cfg(feature = "declarative-plans")]
    use crate::plans::{Operation, PlanExecutor, PlanResult};
    use crate::Snapshot;
    #[cfg(feature = "declarative-plans")]
    use crate::SnapshotRef;
    #[cfg(feature = "declarative-plans")]
    use crate::{KernelError, KernelResult};

    #[cfg(feature = "declarative-plans")]
    struct FailingPlanExecutor;

    #[cfg(feature = "declarative-plans")]
    impl PlanExecutor for FailingPlanExecutor {
        fn execute_op(&self, _op: Operation) -> KernelResult<PlanResult> {
            Err(KernelError::generic("plan executor deliberately failed"))
        }
    }

    // NOTE: In addition to testing the meta-predicate for metadata replay, this test also verifies
    // that the parquet reader properly infers nullcount = rowcount for missing columns. The two
    // checkpoint part files that contain transaction app ids have truncated schemas that would
    // otherwise fail skipping due to their missing nullcount stat:
    //
    // Row group 0:  count: 1  total(compressed): 111 B total(uncompressed):107 B
    // --------------------------------------------------------------------------------
    //              type    nulls  min / max
    // txn.appId    BINARY  0      "3ae45b72-24e1-865a-a211-3..." / "3ae45b72-24e1-865a-a211-3..."
    // txn.version  INT64   0      "4390" / "4390"
    #[test]
    fn test_replay_for_metadata() {
        let path = std::fs::canonicalize(PathBuf::from("./tests/data/parquet_row_group_skipping/"));
        let url = url::Url::from_directory_path(path.unwrap()).unwrap();
        let engine = SyncEngine::new();

        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
        let data: Vec<_> = snapshot
            .log_segment()
            .read_pm_batches(&engine)
            .unwrap()
            .try_collect()
            .unwrap();

        // The checkpoint has five parts, each containing one action:
        // 1. txn (physically missing P&M columns)
        // 2. metaData
        // 3. protocol
        // 4. add
        // 5. txn (physically missing P&M columns)
        //
        // The parquet reader should skip parts 1, 3, and 5. Note that the actual `read_metadata`
        // always skips parts 4 and 5 because it terminates the iteration after finding both P&M.
        //
        // NOTE: Each checkpoint part is a single-row file -- guaranteed to produce one row group.
        //
        // WARNING: https://github.com/delta-io/delta-kernel-rs/issues/434 -- We currently
        // read parts 1 and 5 (4 in all instead of 2) because row group skipping is disabled for
        // missing columns, but can still skip part 3 because has valid nullcount stats for P&M.
        assert_eq!(data.len(), 4);
    }

    /// The filter of `segment`'s P&M plan, built with `capture` under `adaptive-metadata-in-dev`.
    #[cfg(feature = "declarative-plans")]
    fn pm_plan_filter(
        segment: &LogSegment,
        #[cfg(feature = "adaptive-metadata-in-dev")] capture: Option<&ParsedLogPath>,
    ) -> Predicate {
        let plan = segment
            .build_pm_plan(
                #[cfg(feature = "adaptive-metadata-in-dev")]
                capture,
            )
            .unwrap();
        plan.nodes
            .iter()
            .find_map(|node| match &node.op {
                Operator::Filter(filter) => Some(filter.predicate.as_ref().clone()),
                _ => None,
            })
            .expect("P&M plan must filter irrelevant actions")
    }

    #[cfg(feature = "declarative-plans")]
    fn app_txn_checkpoint_snapshot() -> SnapshotRef {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/app-txn-checkpoint/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        Snapshot::builder_for(url)
            .build(&SyncEngine::new())
            .unwrap()
    }

    #[cfg(feature = "declarative-plans")]
    fn pm_predicate() -> Predicate {
        let pm = Predicate::or(
            col!("protocol.minReaderVersion").is_not_null(),
            col!("metaData.id").is_not_null(),
        );
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let pm = Predicate::or(pm, col!("commitInfo").is_not_null());
        pm
    }

    #[cfg(feature = "declarative-plans")]
    #[test]
    fn test_declarative_pm_plan_filters_irrelevant_actions() {
        let snapshot = app_txn_checkpoint_snapshot();
        let filter = pm_plan_filter(
            snapshot.log_segment(),
            #[cfg(feature = "adaptive-metadata-in-dev")]
            None,
        );
        assert_eq!(filter, pm_predicate());
    }

    #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
    #[test]
    fn test_declarative_pm_plan_capturing_checkpoint_keeps_checkpoint_actions() {
        let snapshot = app_txn_checkpoint_snapshot();
        let manifest_commit =
            ParsedLogPath::create_parsed_published_commit(snapshot.table_root(), 1);
        let filter = pm_plan_filter(snapshot.log_segment(), Some(&manifest_commit));
        let expected = Predicate::or(
            pm_predicate(),
            Predicate::and(
                col!("checkpoint").is_not_null(),
                col!("version").eq(lit(1i64)),
            ),
        );
        assert_eq!(filter, expected);
    }

    // With the `declarative-plans` feature flag on, `SyncEngine` resolves P&M through the
    // declarative plan.
    //
    // This fixture's checkpoint names its map entry fields `entries` where kernel expects
    // `key_value`. Parquet takes that name from the writer's Arrow schema unless the writer sets
    // `WriterProperties::coerce_types`, which is off by default, and Arrow's own
    // `MapFieldNames::default()` is `entries`. So a writer that builds its maps from Arrow defaults
    // produces a file kernel must translate on read. Spark and kernel both write `key_value`,
    // covered by
    // `scan_plan::execution_tests::declarative_metadata_reconciles_checkpoint_with_later_commits`.
    #[test]
    fn test_snapshot_build_via_plan_over_parquet_checkpoint_with_entries_named_maps() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/app-txn-checkpoint/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();

        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();

        assert_eq!(snapshot.version(), 1);
        assert_eq!(snapshot.schema().fields().count(), 3);
    }

    // The array counterpart of the test above. This fixture's checkpoint names its array element
    // fields `item` where kernel expects `element`, so it covers the other half of the naming
    // disagreement. `metaData.partitionColumns` is the array in question, and it is present in
    // every `metaData` action, so its element name is checked on every P&M replay.
    #[test]
    fn test_snapshot_build_via_plan_over_parquet_checkpoint_with_item_named_arrays() {
        let path = std::fs::canonicalize(PathBuf::from("./tests/data/parsed-stats/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();

        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();

        assert_eq!(snapshot.version(), 5);
        assert_eq!(snapshot.schema().fields().count(), 5);
    }

    #[cfg(feature = "declarative-plans")]
    #[test]
    fn test_snapshot_build_via_failing_plan_executor_surfaces_error_without_fallback() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/app-txn-checkpoint/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = DelegatingEngine::new(Arc::new(SyncEngine::new()))
            .with_plan_executor(Arc::new(FailingPlanExecutor));

        let result = Snapshot::builder_for(url).build(&engine);

        assert!(
            result.is_err(),
            "plan failure must surface, not fall back to legacy replay"
        );
    }
}

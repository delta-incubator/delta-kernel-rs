//! Declarative metadata scan plans.
//!
//! [`Scan::build_metadata_scan_plan`] reconciles checkpoint and commit actions into live adds,
//! applying metadata pruning before newest-action-wins replay.

use std::borrow::Cow;
use std::sync::{Arc, LazyLock};

use url::Url;

use super::data_skipping::as_sql_data_skipping_predicate_with_stats_columns;
use super::state_info::StateInfo;
use super::{PhysicalPredicate, Scan};
use crate::actions::{
    ADD_FIELD, ADD_NAME, ADD_SCHEMA, REMOVE_FIELD, SIDECAR_FIELD, SIDECAR_NAME, STATS_PARSED,
};
use crate::checkpoint::{CheckpointShape, CheckpointType};
use crate::expressions::{
    col, column_name, joined_column_expr, lit, null_lit, ColumnName, Expression as Expr,
    ExpressionRef, MapToStructOptions, Predicate, UnaryExpressionOp,
};
use crate::plans::ir::nodes::{DynamicScan, FileType, ScanFile};
use crate::plans::ir::plan::Plan;
use crate::scan::log_replay::{PARTITION_VALUES_PARSED_NAME, STATS_PARSED_NAME};
use crate::schema::{
    lazy_schema_ref, schema, schema_ref, CheckpointFieldKind, DataType, SchemaRef,
    SchemaStructPatchBuilder, StructField, StructType,
};
use crate::struct_patch::{project_struct_preserving_nulls, ProjectionStructPatchBuilder};
use crate::transforms::{transform_output_type, ExpressionTransform};
use crate::utils::FoldWithOption as _;
use crate::{KernelError, KernelResult, PlanBuilder};

// Preserve the complete native stats shape for JSON output, while retaining the physical
// date/timestamp reinterpretations that a typed checkpoint read would otherwise perform.
// Unannotated integer fields must be read as temporal values before JSON serialization.
fn checkpoint_json_stats_schema(native: &StructType, requested: &StructType) -> StructType {
    StructType::new_unchecked(native.fields().map(|field| {
        let mut field = field.clone();
        if let Some(target) = requested.field(field.name()) {
            match (field.data_type(), target.data_type()) {
                (DataType::Struct(source), DataType::Struct(target)) => {
                    field.data_type = checkpoint_json_stats_schema(source, target).into();
                }
                (DataType::Primitive(source), DataType::Primitive(target))
                    if source.is_checkpoint_cast_compatible(target, CheckpointFieldKind::Stats) =>
                {
                    field.data_type = target.clone().into();
                }
                _ => {}
            }
        }
        field
    }))
}

// Variant and geospatial min/max require Delta-specific JSON encodings (Z85 and WKT), which
// generic ToJson does not supply. nullCount for these columns is an ordinary LONG statistic.
fn stats_need_alternate_json(stats: &StructType, table: &StructType) -> bool {
    fn contains_special_type(native: &StructType, table: Option<&StructType>) -> bool {
        native.fields().any(|field| {
            let table_type = table
                .and_then(|s| s.field(field.name()))
                .map(StructField::data_type);
            match (field.data_type(), table_type) {
                (DataType::Variant(_), _) | (DataType::Struct(_), Some(DataType::Variant(_))) => {
                    true
                }
                #[cfg(feature = "geo-type-in-dev")]
                (
                    DataType::Primitive(
                        crate::schema::PrimitiveType::Geometry(_)
                        | crate::schema::PrimitiveType::Geography(_),
                    ),
                    _,
                )
                | (
                    DataType::Primitive(crate::schema::PrimitiveType::Binary),
                    Some(DataType::Primitive(
                        crate::schema::PrimitiveType::Geometry(_)
                        | crate::schema::PrimitiveType::Geography(_),
                    )),
                ) => true,
                (DataType::Struct(native), table_type) => contains_special_type(
                    native,
                    match table_type {
                        Some(DataType::Struct(table)) => Some(table),
                        _ => None,
                    },
                ),
                _ => false,
            }
        })
    }
    ["minValues", "maxValues"].iter().any(|name| {
        stats
            .field(name)
            .is_some_and(|field| match field.data_type() {
                DataType::Struct(values) => contains_special_type(values, Some(table)),
                _ => false,
            })
    })
}

fn checkpoint_json_stats_source(
    shape: &CheckpointShape,
    table: &StructType,
) -> KernelResult<Option<SchemaRef>> {
    if shape.has_json_stats() == Some(true) {
        return Ok(None);
    }
    let Some(leaf) = shape.leaf_checkpoint_schema.as_ref() else {
        return Ok(None);
    };
    let Ok(field) = leaf.field_at(&column_name!("add.stats_parsed")) else {
        return Ok(None);
    };
    let DataType::Struct(stats) = field.data_type() else {
        return Ok(None);
    };
    if stats_need_alternate_json(stats, table) {
        return Err(KernelError::unsupported(
            "Cannot synthesize JSON stats for VARIANT or geospatial min/max; use structured stats or a \
             checkpoint with JSON stats",
        ));
    }
    // JSON_ONLY has no typed output schema. Resolve temporal types from the full table, including
    // columns outside the requested/indexed set, while preserving the native field set.
    let stats_types = schema! {
        nullable "minValues": (table.clone()),
        nullable "maxValues": (table.clone()),
    };
    Ok(Some(Arc::new(checkpoint_json_stats_schema(
        stats,
        &stats_types,
    ))))
}

// Align compatible checkpoint fields with the requested types. The read schema includes missing
// fields so Parquet can null-fill them; the JSON projection keeps only the native field set.
// ALL's synthesized JSON may use these widened types; JSON_ONLY retains the native types.
// Variant is atomic here: its physical struct must not be expanded as a user-defined struct.
fn merge_checkpoint_stats_schema(
    native: &StructType,
    requested: &StructType,
    include_missing: bool,
) -> StructType {
    let mut fields: Vec<_> = native
        .fields()
        .cloned()
        .map(|mut field| {
            if let Some(target) = requested.field(field.name()) {
                field.data_type = match (field.data_type(), target.data_type()) {
                    (DataType::Struct(source), DataType::Struct(target)) => {
                        merge_checkpoint_stats_schema(source, target, include_missing).into()
                    }
                    (_, target) => target.clone(),
                };
            }
            field
        })
        .collect();
    if include_missing {
        fields.extend(
            requested
                .fields()
                .filter(|f| native.field(f.name()).is_none())
                .cloned(),
        );
    }
    StructType::new_unchecked(fields)
}

// === Internal column names ===

// Both add and remove provide path + DV (storageType, pathOrInlineDv, offset) columns. We
// materialize them as one top-level `file_action_key` column that are used by the plan's
// aggregate and anti-join operators.
const FILE_ACTION_KEY: &str = "file_action_key";
const STATS: &str = "stats";
const JSON_STATS_SOURCE: &str = "_json_stats_source";
const PARTITION_VALUES: &str = "partitionValues";
const PARTITION_VALUES_PARSED: &str = "partitionValues_parsed";
// Generated partition pruning predicates reference this to retain removes.
const IS_ADD: &str = "is_add";
const VERSION: &str = "version";

impl Scan {
    /// Build the live-add metadata plan from checkpoint and commit actions.
    ///
    /// Returns `None` for an empty result or a statically false predicate.
    #[tracing::instrument(
        name = "scan_plan.build_metadata_scan_plan",
        skip_all,
        fields(enable_call_frame),
        err
    )]
    pub(super) fn build_metadata_scan_plan(
        &self,
        shape: &CheckpointShape,
    ) -> KernelResult<Option<Plan>> {
        let state = &self.state_info;
        // A statically-unsatisfiable predicate (e.g. `x > 10 AND FALSE`) skips the whole table.
        if state.physical_predicate == PhysicalPredicate::StaticSkipAll {
            return Ok(None);
        }

        let source_json_stats = if self.stats.synthesize_json {
            checkpoint_json_stats_source(
                shape,
                &self.snapshot.table_configuration().physical_schema(),
            )?
        } else {
            None
        };

        let prune = stats_skipping_predicate(state);
        let prune = prune.as_ref();

        // The output `add` after reparsing `stats`/`partitionValues`: shared by the commit arm's
        // dedup carrier and both terminal `{ add }` projections, so every arm agrees on the
        // union schema.
        let add_field = self.normalized_add_field()?;
        let (output_expr, output_schema) = self.metadata_output_projection(&add_field)?;

        let commit_actions = self.commit_arm()?.try_fold_with(prune, |p, prune| {
            // We filter so that:
            // * All remove actions are kept
            // * Add actions that do not match the partition pruning or stats predicate are removed.
            //
            // NOTE: It is important that add actions are filtered by the partition predicate
            // because partition filtering may not be applied on data rows. On the other
            // hand, failing to skip based on data columns is safe because the data
            // predicate will also be evaluated on data rows. Thus it is crucial that we partition
            // prune adds here.
            //
            // NOTE: It is not safe to prune remove actions using the partition filter. This is
            // because a NULL result for `remove.partitionValues.partCol` may be due to
            // `remove.partitionValues` being NULL, or it may be from `partCol` being
            // NULL. Thus, we simply do not prune removes.
            p.filter(Predicate::or(col!("add").is_null(), prune.clone()))
        })?;

        let deduped_commit = commit_actions.aggregate_by([column_name!(FILE_ACTION_KEY)], |a| {
            // Each group with a non-null FILE_ACTION_KEY contains the adds and removes for a given
            // file; winning adds pass through unchanged while winning removes produce NULL. Non-
            // file actions have NULL FILE_ACTION_KEY and map to their own NULL group.
            a.max_non_null_by(
                column_name!(ADD_NAME),
                column_name!(FILE_ACTION_KEY),
                column_name!(VERSION),
            )
        })?;

        let checkpoint_adds = self
            .checkpoint_arm(shape, source_json_stats.as_ref())?
            .try_fold_with(prune, |p, prune| p.filter(prune.clone()))?;

        let checkpoint_live_adds = checkpoint_adds
            .anti_join(
                deduped_commit.clone(),
                [column_name!(FILE_ACTION_KEY)],
                [column_name!(FILE_ACTION_KEY)],
            )?
            .try_fold_with(source_json_stats.as_ref(), |plan, _| {
                plan.project_patch(|patch| {
                    patch
                        .replace_at(
                            [ADD_NAME],
                            STATS,
                            StructField::nullable(STATS, DataType::STRING),
                            Expr::unary(UnaryExpressionOp::ToJson, col!(JSON_STATS_SOURCE)),
                        )
                        .drop(JSON_STATS_SOURCE)
                })
            })?
            .project(output_expr.clone(), output_schema.clone())?;

        let commit_live_adds = deduped_commit
            .filter(col!("add").is_not_null())?
            .project(output_expr, output_schema)?;

        PlanBuilder::union_all([commit_live_adds, checkpoint_live_adds])?.build_opt()
    }

    /// Build normalized checkpoint adds. Returns an empty relation when no checkpoint exists.
    ///
    /// ## SQL equivalent:
    //
    /// SELECT STRUCT(
    ///          add.* EXCEPT (
    ///            stats_parsed, partitionValues_parsed
    ///          ),
    ///          add.stats_parsed AS stats_parsed,
    ///          add.partitionValues_parsed AS partitionValues_parsed
    ///        ) AS add,
    ///        version, add.path IS NOT NULL AS is_add, file_key(add) AS key
    /// FROM checkpoint_actions
    /// WHERE add.path IS NOT NULL
    ///
    /// When the checkpoint lacks native parsed metadata, `FROM_JSON(add.stats, physical_stats)`
    /// and `MAP_TO_STRUCT(add.partitionValues, physical_partitions)` replace the corresponding
    /// fields above. A parsed field is omitted when its schema is absent.
    fn checkpoint_arm(
        &self,
        shape: &CheckpointShape,
        source_json_stats: Option<&SchemaRef>,
    ) -> KernelResult<PlanBuilder> {
        let log_segment = self.snapshot.log_segment();
        let physical_stats = self.state_info.physical_stats_read_schema();
        let physical_partitions = self.state_info.physical_partition_schema.as_ref();
        let source_physical_stats =
            physical_stats.and_then(|schema| shape.compatible_stats_parsed_schema(schema));
        // JSON output must retain every stat stored in the checkpoint, even when pruning or
        // structured output requests only a subset of columns.
        let combined_stats =
            source_json_stats
                .as_ref()
                .zip(source_physical_stats)
                .map(|(native, requested)| {
                    Arc::new(merge_checkpoint_stats_schema(native, requested, true))
                });
        let stats_to_read = combined_stats
            .as_ref()
            .or(source_json_stats)
            .or(source_physical_stats);
        let source_physical_partitions = physical_partitions
            .and_then(|schema| shape.compatible_partition_values_parsed_schema(schema));
        let checkpoint = log_segment.checkpoint_version_tagged_scan_files()?;
        let read_schema = || -> KernelResult<SchemaRef> {
            let schema = parquet_read_schema(stats_to_read, source_physical_partitions)?;
            if shape.has_json_stats() == Some(false) {
                Ok(Arc::new(
                    SchemaStructPatchBuilder::new()
                        .drop_at([ADD_NAME], STATS)
                        .build(&schema)?,
                ))
            } else {
                Ok(schema)
            }
        };

        let actions = match (&shape.checkpoint_type, checkpoint) {
            (CheckpointType::Leaf, Some((FileType::Parquet, parts))) => {
                let schema = read_schema()?;
                PlanBuilder::scan_parquet(parts, &[VERSION], schema)
            }
            (CheckpointType::Leaf, Some((FileType::Json, parts))) => {
                PlanBuilder::scan_json(
                    parts,
                    &[VERSION],
                    json_read_schema(/* include_remove */ false),
                )
            }
            (CheckpointType::Manifest, Some((file_type, parts))) => {
                let schema = read_schema()?;
                match log_segment.checkpoint_hint_version_tagged_sidecar_scan_files()? {
                    Some(sidecars) => PlanBuilder::scan_parquet(sidecars, &[VERSION], schema),
                    // Without a complete hint, load the sidecars referenced by the manifest.
                    None => sidecar_actions(file_type, parts, schema, &log_segment.log_root),
                }
            }
            (CheckpointType::None, _) | (_, None) => {
                PlanBuilder::values(json_read_schema(/* include_remove */ false), vec![])
            }
        }?;

        let actions = actions.filter(col!("add.path").is_not_null())?;
        // Keep the normalized add schema stable without reading a missing Parquet field.
        let actions = if shape.has_json_stats() == Some(false) {
            actions.project_patch(|patch| {
                patch.insert_after_at(
                    [ADD_NAME],
                    "dataChange",
                    StructField::nullable(STATS, DataType::STRING),
                    null_lit(DataType::STRING),
                )
            })?
        } else {
            actions
        };
        let actions = if let Some(native_json_schema) = source_json_stats {
            let json_schema = combined_stats
                .as_ref()
                .map(|read| {
                    Arc::new(merge_checkpoint_stats_schema(
                        native_json_schema,
                        read,
                        false,
                    ))
                })
                .unwrap_or_else(|| native_json_schema.clone());
            let json_source = if combined_stats.is_some() {
                project_struct_preserving_nulls([ADD_NAME, STATS_PARSED], &json_schema)
            } else {
                col!("add.stats_parsed")
            };
            // Materialize the native-shaped struct with its schema before ToJson. Keep the typed
            // output independent: ToJson intentionally truncates timestamp stats to milliseconds.
            actions.project_patch(|patch| {
                let patch = patch.append(
                    StructField::nullable(JSON_STATS_SOURCE, json_schema.as_ref().clone()),
                    json_source,
                );
                match physical_stats {
                    Some(schema) => {
                        let typed_stats = if source_physical_stats.is_some() {
                            project_struct_preserving_nulls([ADD_NAME, STATS_PARSED], schema)
                        } else {
                            // An incompatible native struct cannot safely supply typed stats.
                            // Retain the existing JSON fallback, without synthesizing it first.
                            Expr::parse_json(col!("add.stats"), Arc::clone(schema))
                        };
                        patch.replace_at(
                            [ADD_NAME],
                            STATS_PARSED,
                            StructField::nullable(STATS_PARSED, schema.as_ref().clone()),
                            typed_stats,
                        )
                    }
                    None => patch.drop_at([ADD_NAME], STATS_PARSED),
                }
            })?
        } else {
            actions
        };

        actions.project_patch(|patch| {
            patch
                .with_parsed_add_stats(physical_stats)
                .with_parsed_add_partition_values(physical_partitions)
                .append(
                    StructField::not_null(IS_ADD, DataType::BOOLEAN),
                    Expr::from(col!("add.path").is_not_null()),
                )
                .append(
                    FILE_ACTION_KEY_FIELD.clone(),
                    file_action_key_expr(|col| joined_column_expr!("add", col)),
                )
        })
    }

    /// Build the normalized commit JSON arm.
    ///
    /// ## SQL equivalent:
    ///
    /// SELECT STRUCT(
    ///          add.* EXCEPT (stats_parsed, partitionValues_parsed),
    ///          FROM_JSON(add.stats, physical_stats) AS stats_parsed,
    ///          MAP_TO_STRUCT(add.partitionValues, physical_partitions) AS partitionValues_parsed
    ///        ) AS add,
    ///        remove, version, add.path IS NOT NULL AS is_add,
    ///        file_key(COALESCE(add, remove)) AS key
    /// FROM json_commits
    /// WHERE add.path IS NOT NULL OR remove.path IS NOT NULL
    ///
    /// A parsed field is omitted when its schema is absent.
    fn commit_arm(&self) -> KernelResult<PlanBuilder> {
        let log_segment = self.snapshot.log_segment();
        let commit_files = log_segment.commit_cover_version_tagged_scan_files()?;
        PlanBuilder::scan_json(commit_files, &[VERSION], json_read_schema(true))?
            .filter(Predicate::or(
                col!("add.path").is_not_null(),
                col!("remove.path").is_not_null(),
            ))?
            .project_patch(|patch| {
                // Commits never carry source-native parsed columns, so normalize from the raw
                // encodings.
                patch
                    .with_parsed_add_stats(self.state_info.physical_stats_read_schema())
                    .with_parsed_add_partition_values(
                        self.state_info.physical_partition_schema.as_ref(),
                    )
                    .append(
                        StructField::not_null(IS_ADD, DataType::BOOLEAN),
                        Expr::from(col!("add.path").is_not_null()),
                    )
                    .append(
                        FILE_ACTION_KEY_FIELD.clone(),
                        file_action_key_expr(|col| {
                            Expr::coalesce([
                                joined_column_expr!("add", col),
                                joined_column_expr!("remove", col),
                            ])
                        }),
                    )
            })
    }

    fn normalized_add_field(&self) -> KernelResult<StructField> {
        let physical_stats_read_schema = self.state_info.physical_stats_read_schema();
        let physical_partition_schema = self.state_info.physical_partition_schema.as_ref();
        let patch = SchemaStructPatchBuilder::new()
            .fold_with(physical_stats_read_schema, |patch, schema| {
                patch.append(StructField::nullable(STATS_PARSED, schema.as_ref().clone()))
            })
            .fold_with(physical_partition_schema, |patch, schema| {
                patch.append(StructField::nullable(
                    PARTITION_VALUES_PARSED,
                    schema.as_ref().clone(),
                ))
            });
        Ok(StructField::nullable(ADD_NAME, patch.build(&ADD_SCHEMA)?))
    }

    /// Builds the output projection for requested stats and partition values. The base of this
    /// transformation is constructed by [`Self::normalized_add_field`].
    ///
    /// The output schema is:
    /// ```text
    /// add: struct<
    ///   path: string,
    ///   partitionValues: map<string, string>,
    ///   size: long,
    ///   modificationTime: long,
    ///   dataChange: boolean,
    ///   stats: string,                         // when JSON stats are requested
    ///   tags: map<string, string>,
    ///   deletionVector: struct<...>,
    ///   baseRowId: long,
    ///   defaultRowCommitVersion: long,
    ///   clusteringProvider: string,
    ///   stats_parsed: struct<...>,             // when parsed stats are requested
    ///   partitionValues_parsed: struct<...>,   // when parsed partition values are requested
    /// >
    /// ```
    /// Stats output may contain neither representation, JSON only, parsed only, or both. Parsed
    /// partition values are selected independently and omitted for unpartitioned tables. Fields
    /// needed only for pruning are omitted.
    fn metadata_output_projection(
        &self,
        add_field: &StructField,
    ) -> KernelResult<(ExpressionRef, SchemaRef)> {
        let input_schema = schema_ref! { (add_field.clone()) };
        let has_stats_parsed = input_schema.contains_col([ADD_NAME, STATS_PARSED_NAME]);
        let projection = ProjectionStructPatchBuilder::new_nested(&input_schema, [ADD_NAME]);

        // JSON stats output. `StatsOptions` allows JSON only, parsed only, both, or neither.
        let has_json_stats = input_schema.contains_col([ADD_NAME, STATS]);
        let projection = match (self.stats.synthesize_json, has_json_stats) {
            (true, true) | (false, false) => projection,
            (true, false) => {
                return Err(KernelError::internal_error(
                    "JSON stats were requested, but add.stats is missing from the metadata schema",
                ));
            }
            (false, true) => projection.drop(STATS),
        };

        // Parsed stats output.
        let projection = match (
            self.state_info.physical_stats_output_schema(),
            has_stats_parsed,
        ) {
            (Some(physical_stats), _) => projection.replace(
                STATS_PARSED,
                StructField::nullable(STATS_PARSED, physical_stats.as_ref().clone()),
                project_struct_preserving_nulls([ADD_NAME, STATS_PARSED_NAME], physical_stats),
            ),
            (None, true) => projection.drop(STATS_PARSED),
            (None, false) => projection,
        };

        // Parsed partition-values output.
        let has_partition_values_parsed =
            input_schema.contains_col([ADD_NAME, PARTITION_VALUES_PARSED_NAME]);
        let physical_partitions = self
            .partition_values
            .parsed_struct
            .then_some(self.state_info.physical_partition_schema.as_ref())
            .flatten();
        let projection = match (physical_partitions, has_partition_values_parsed) {
            (Some(schema), true) => projection.replace(
                PARTITION_VALUES_PARSED,
                StructField::nullable(PARTITION_VALUES_PARSED, schema.as_ref().clone()),
                project_struct_preserving_nulls([ADD_NAME, PARTITION_VALUES_PARSED_NAME], schema),
            ),
            (Some(_), false) => {
                return Err(KernelError::internal_error(
                    "parsed partition values were requested, but add.partitionValues_parsed is \
                     missing",
                ));
            }
            (None, true) => projection.drop(PARTITION_VALUES_PARSED),
            (None, false) => projection,
        };

        let (add_schema, add_expr) = projection.build()?;
        let schema = schema_ref! { nullable ADD_NAME: (add_schema.as_ref().clone()) };
        Ok((Arc::new(Expr::struct_from([add_expr])), schema))
    }
}

/// Read actions from V2 checkpoint sidecars.
fn sidecar_actions(
    file_type: FileType,
    root_parts: Vec<ScanFile>,
    action_schema: SchemaRef,
    log_root: &Url,
) -> KernelResult<PlanBuilder> {
    const FILE_PATH: &str = "path";
    const FILE_SIZE: &str = "size";
    const FILE_MOD: &str = "filemod";
    const SIDECAR_SIZE: &str = "sizeInBytes";
    const SIDECAR_FILE_MOD: &str = "modificationTime";

    static SIDECAR_FILE_META_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
        not_null FILE_PATH: STRING,
        not_null FILE_SIZE: LONG,
        not_null FILE_MOD: LONG,
        nullable VERSION: LONG,
    };

    static SIDECAR_READ_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
        (&SIDECAR_FIELD),
        nullable VERSION: LONG,
    };

    let scan = match file_type {
        FileType::Json => PlanBuilder::scan_json,
        FileType::Parquet => PlanBuilder::scan_parquet,
    };
    let sidecar_files = scan(root_parts, &[VERSION], SIDECAR_READ_SCHEMA.clone())?
        .filter(col!(SIDECAR_NAME, FILE_PATH).is_not_null())?
        .project(
            Expr::struct_from([
                col!(SIDECAR_NAME, FILE_PATH),
                col!(SIDECAR_NAME, SIDECAR_SIZE),
                col!(SIDECAR_NAME, SIDECAR_FILE_MOD),
                col!(VERSION),
            ]),
            SIDECAR_FILE_META_SCHEMA.clone(),
        )?;

    let dynamic_scan = DynamicScan::try_new(
        &SIDECAR_FILE_META_SCHEMA,
        action_schema,
        FileType::Parquet,
        log_root.join("_sidecars/")?,
        [VERSION],
        column_name!(FILE_PATH),
        column_name!(FILE_SIZE),
        column_name!(FILE_MOD),
        None,
    )?;

    sidecar_files.dynamic_scan(dynamic_scan)
}

// === Helpers ===

/// Read schema for JSON actions tagged with their log version.
/// Commits include removes; JSON checkpoint leaves do not.
fn json_read_schema(include_remove: bool) -> SchemaRef {
    schema_ref! {
        (&ADD_FIELD),
        ..(include_remove.then_some(&REMOVE_FIELD)),
        nullable VERSION: LONG,
    }
}

/// Read schema for parquet add actions.
fn parquet_read_schema(
    physical_stats: Option<&SchemaRef>,
    physical_partitions: Option<&SchemaRef>,
) -> KernelResult<SchemaRef> {
    let add_patch = SchemaStructPatchBuilder::new()
        .fold_with(physical_stats, |patch, schema| {
            patch.append(StructField::nullable(STATS_PARSED, schema.as_ref().clone()))
        })
        .fold_with(physical_partitions, |patch, schema| {
            patch.append(StructField::nullable(
                PARTITION_VALUES_PARSED,
                schema.as_ref().clone(),
            ))
        });
    Ok(schema_ref! {
        nullable ADD_NAME: (add_patch.build(&ADD_SCHEMA)?),
        nullable VERSION: LONG,
    })
}

/// File identity used for replay.
static FILE_ACTION_KEY_FIELD: LazyLock<StructField> = LazyLock::new(|| {
    let schema = schema! {
        nullable "path": STRING,
        nullable "deletionVector": {
            not_null "storageType": STRING,
            not_null "pathOrInlineDv": STRING,
            nullable "offset": INTEGER,
        },
    };
    StructField::nullable(FILE_ACTION_KEY, schema)
});

/// Build a file identity from path and deletion vector.
fn file_action_key_expr(key_col_expr: impl Fn(ColumnName) -> Expr) -> Expr {
    let storage_type = key_col_expr(column_name!("deletionVector.storageType"));
    Expr::struct_from([
        key_col_expr(column_name!("path")),
        Expr::struct_with_nullability_from(
            [
                storage_type.clone(),
                key_col_expr(column_name!("deletionVector.pathOrInlineDv")),
                key_col_expr(column_name!("deletionVector.offset")),
            ],
            Expr::from_pred(storage_type.is_not_null()),
        ),
    ])
}

trait ProjectionStructPatchBuilderExt<'a> {
    /// Parses add stats, preferring a compatible parsed field.
    ///
    /// When `physical_stats` is present, the input must contain either
    /// `add.stats_parsed` or the fallback `add.stats` JSON field.
    fn with_parsed_add_stats(self, physical_stats: Option<&SchemaRef>) -> Self;

    /// Parses add partition values when a compatible parsed field is not already present.
    fn with_parsed_add_partition_values(self, physical_partitions: Option<&SchemaRef>) -> Self;
}

impl<'a> ProjectionStructPatchBuilderExt<'a> for ProjectionStructPatchBuilder<'a> {
    fn with_parsed_add_stats(self, physical_stats: Option<&SchemaRef>) -> Self {
        let has_stats_parsed = self
            .input_schema()
            .contains_col([ADD_NAME, STATS_PARSED_NAME]);
        let add = [ADD_NAME];
        match physical_stats {
            Some(schema) => {
                let field = StructField::nullable(STATS_PARSED, schema.as_ref().clone());
                let expr = Expr::parse_json(col!("add.stats"), Arc::clone(schema));
                if has_stats_parsed {
                    self
                } else {
                    self.append_at(add, field, expr)
                }
            }
            None => self,
        }
    }

    fn with_parsed_add_partition_values(self, physical_partitions: Option<&SchemaRef>) -> Self {
        let has_partition_values_parsed = self
            .input_schema()
            .contains_col([ADD_NAME, PARTITION_VALUES_PARSED_NAME]);
        let add = [ADD_NAME];
        match (physical_partitions, has_partition_values_parsed) {
            (Some(schema), false) => self.append_at(
                add,
                StructField::nullable(PARTITION_VALUES_PARSED, schema.as_ref().clone()),
                Expr::map_to_struct(
                    col!(ADD_NAME, PARTITION_VALUES),
                    MapToStructOptions::default(),
                ),
            ),
            (Some(_), true) | (None, _) => self,
        }
    }
}

/// Build the metadata pruning predicate, or `None` when no pruning is possible.
fn stats_skipping_predicate(state: &StateInfo) -> Option<Predicate> {
    /// Re-roots metadata columns under `add`.
    struct MetadataSkippingColumnPrefixer;

    impl<'a> ExpressionTransform<'a> for MetadataSkippingColumnPrefixer {
        transform_output_type!(|'a, T| Cow<'a, T>);

        fn transform_expr_column(&mut self, name: &'a ColumnName) -> Cow<'a, ColumnName> {
            let path = name.path();
            let replacement_root = match path.first().map(String::as_str) {
                Some(STATS_PARSED) => [ADD_NAME, STATS_PARSED],
                Some(PARTITION_VALUES_PARSED) => [ADD_NAME, PARTITION_VALUES_PARSED],
                _ => return Cow::Borrowed(name),
            };
            Cow::Owned(ColumnName::new(
                replacement_root
                    .into_iter()
                    .map(str::to_string)
                    .chain(path.iter().skip(1).cloned()),
            ))
        }
    }

    let PhysicalPredicate::Some(pred, _) = &state.physical_predicate else {
        return None;
    };
    let partition_column_names = state
        .physical_partition_schema
        .iter()
        .flat_map(|s| s.fields().map(|f| ColumnName::new([f.name()])))
        .collect();
    let skipping = as_sql_data_skipping_predicate_with_stats_columns(
        pred,
        &partition_column_names,
        &state.eligible_physical_stats_columns,
    )?;
    // A null skipping verdict means the available metadata cannot prove the file is skippable.
    let skipping = Predicate::distinct(skipping, lit(false));
    let mut prefixer = MetadataSkippingColumnPrefixer;
    Some(prefixer.transform_pred(&skipping).into_owned())
}

#[cfg(test)]
#[path = "scan_plan/tests.rs"]
mod execution_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::{
        Array as _, AsArray as _, Date32Array, Float64Array, Int64Array, StringArray, StructArray,
        TimestampMicrosecondArray,
    };
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::engine::arrow_utils::parse_json_impl;
    use crate::engine::sync::SyncEngine;
    use crate::log_segment::LogSegment;
    use crate::log_segment_files::LogSegmentFiles;
    use crate::object_store::memory::InMemory;
    use crate::object_store::path::Path;
    use crate::object_store::ObjectStoreExt as _;
    use crate::parquet::arrow::arrow_writer::ArrowWriter;
    use crate::plans::ir::nodes::Operator;
    use crate::plans::Operation as PlanOperation;
    use crate::scan::{PartitionValuesOptions, StatsOptions};
    use crate::snapshot::Snapshot;
    use crate::table_features::TableFeature;
    use crate::unit_test_utils::{
        create_log_path, MockProtocolBuilder, MockTableConfigurationBuilder,
    };
    use crate::{Engine as _, KernelResult};

    fn mock_snapshot(log_segment: LogSegment) -> KernelResult<Arc<Snapshot>> {
        let table_configuration = MockTableConfigurationBuilder::new()
            .with_schema(partitioned_schema())
            .with_partition_columns(["p"])
            .with_protocol(MockProtocolBuilder::new().with_versions(2, 5).build())
            .with_table_root("memory:///")
            .try_build()?;
        Ok(Arc::new(Snapshot::new(log_segment, table_configuration)?))
    }

    fn partitioned_schema() -> SchemaRef {
        schema_ref! {
            nullable "x": LONG,
            nullable "p": STRING,
        }
    }

    fn log_root() -> Url {
        Url::parse("file:///_delta_log/").unwrap()
    }

    fn log_segment(log_root: Url, commits: &[&str], checkpoint: Option<&str>) -> LogSegment {
        let ascending_commit_files: Vec<_> =
            commits.iter().map(|path| create_log_path(path)).collect();
        let checkpoint_parts: Vec<_> = checkpoint.into_iter().map(create_log_path).collect();
        let checkpoint_version = checkpoint_parts.first().map(|path| path.version);
        let latest_commit_file = ascending_commit_files.last().cloned();
        let end_version = latest_commit_file
            .as_ref()
            .map(|path| path.version)
            .or(checkpoint_version)
            .unwrap_or_default();
        LogSegment {
            end_version,
            checkpoint_version,
            log_root,
            listed: LogSegmentFiles {
                ascending_commit_files,
                checkpoint_parts,
                latest_commit_file,
                max_published_version: Some(end_version),
                ..Default::default()
            },
            last_checkpoint_metadata: None,
        }
    }

    fn checkpoint_path(file_type: FileType) -> &'static str {
        match file_type {
            FileType::Json => concat!(
                "file:///_delta_log/00000000000000000000.checkpoint.",
                "11111111-1111-1111-1111-111111111111.json"
            ),
            FileType::Parquet => "file:///_delta_log/00000000000000000000.checkpoint.parquet",
        }
    }

    fn shape(checkpoint_type: CheckpointType, parsed_stats: Option<SchemaRef>) -> CheckpointShape {
        let leaf_checkpoint_schema = parsed_stats
            .as_ref()
            .map(|stats| parquet_read_schema(Some(stats), None).unwrap());
        CheckpointShape {
            checkpoint_type,
            leaf_checkpoint_schema,
        }
    }

    fn no_checkpoint() -> CheckpointShape {
        shape(CheckpointType::None, None)
    }

    fn tags(plan: &Plan) -> Vec<String> {
        plan.nodes.iter().map(|node| node.op.to_string()).collect()
    }

    fn add_struct(schema: &SchemaRef) -> &StructType {
        let DataType::Struct(add_struct) = schema
            .field(ADD_NAME)
            .expect("schema should contain add")
            .data_type()
        else {
            panic!("add should be a struct");
        };
        add_struct
    }

    // One add with JSON stats and no `stats_parsed`.
    fn write_parquet_checkpoint(store: &Arc<InMemory>, path: &str) -> KernelResult<()> {
        use crate::arrow::array::builder::{MapBuilder, MapFieldNames, StringBuilder};
        use crate::arrow::array::{
            Array, BooleanArray, Int64Array, RecordBatch, StringArray as SA,
        };
        use crate::arrow::datatypes::{DataType as ADT, Field, Fields, Schema};
        use crate::parquet::arrow::arrow_writer::ArrowWriter;

        // An empty (non-null) `partitionValues` map for the single row; the canonical add schema
        // requires the field, so the checkpoint file must physically carry it. The inner field
        // names must match kernel's map convention (`key_value` / `key` / `value`).
        let map_names = MapFieldNames {
            entry: "key_value".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        };
        let mut map = MapBuilder::new(Some(map_names), StringBuilder::new(), StringBuilder::new());
        map.append(true).unwrap();
        let partition_values = map.finish();

        // The reader null-fills missing *nullable* add fields, but the canonical add schema's
        // non-null scalars (`path`, `size`, `modificationTime`, `dataChange`) and `partitionValues`
        // must be present in the file. `stats` carries the JSON string parsed in the
        // no-parsed-stats path; there is deliberately no `stats_parsed` column.
        let add_fields = Fields::from(vec![
            Field::new("path", ADT::Utf8, true),
            Field::new("stats", ADT::Utf8, true),
            Field::new(
                "partitionValues",
                partition_values.data_type().clone(),
                true,
            ),
            Field::new("size", ADT::Int64, true),
            Field::new("modificationTime", ADT::Int64, true),
            Field::new("dataChange", ADT::Boolean, true),
        ]);
        let schema = Arc::new(Schema::new(vec![
            Field::new(ADD_NAME, ADT::Struct(add_fields.clone()), true),
            Field::new(VERSION, ADT::Int64, true),
        ]));
        let add = StructArray::new(
            add_fields,
            vec![
                Arc::new(SA::from(vec!["c.parquet"])),
                Arc::new(SA::from(vec![
                    r#"{"numRecords":1,"minValues":{"x":10},"maxValues":{"x":10}}"#,
                ])),
                Arc::new(partition_values),
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(BooleanArray::from(vec![true])),
            ],
            None,
        );
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(add), Arc::new(Int64Array::from(vec![0i64]))],
        )?;

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        futures::executor::block_on(store.put(&Path::from(path), buf.into()))?;
        Ok(())
    }

    fn struct_stats_schema() -> SchemaRef {
        let segment = log_segment(log_root(), &[], None);
        mock_snapshot(segment)
            .unwrap()
            .scan_builder()
            .with_predicate(Arc::new(col!("x").gt(lit(5i64))))
            .with_stats(StatsOptions::all())
            .build()
            .unwrap()
            .state_info
            .physical_stats_read_schema()
            .cloned()
            .expect("stats schema")
    }

    // Commit-arm operator sequence without optional pruning.
    const COMMIT_ARM_TAGS: &[&str] = &[
        "scan_json", // commits
        "filter",    // keep file actions
        "project",   // normalize
        "aggregate", // newest-action-per-key
        "filter",    // live commit adds
        "project",   // extract add
    ];

    #[rstest::rstest]
    #[case::leaf_parquet(shape(CheckpointType::Leaf, None), FileType::Parquet,
        vec!["scan_parquet", "filter", "project", "semi_join", "project"])]
    #[case::leaf_json(shape(CheckpointType::Leaf, None), FileType::Json,
        vec!["scan_json", "filter", "project", "semi_join", "project"])]
    #[case::manifest(shape(CheckpointType::Manifest, None), FileType::Parquet,
        vec!["scan_parquet", "filter", "project", "dynamic_scan", "filter", "project", "semi_join", "project"])]
    fn metadata_plan_checkpoint_arm_shape(
        #[case] shape: CheckpointShape,
        #[case] file_type: FileType,
        #[case] checkpoint_arm_tags: Vec<&'static str>,
    ) -> KernelResult<()> {
        let segment = log_segment(
            log_root(),
            &["file:///_delta_log/00000000000000000001.json"],
            Some(checkpoint_path(file_type)),
        );
        let scan = mock_snapshot(segment)?.scan_builder().build()?;
        let plan = scan.build_metadata_scan_plan(&shape)?.expect("non-empty");

        let mut expected: Vec<&str> = COMMIT_ARM_TAGS.to_vec();
        expected.extend(checkpoint_arm_tags);
        expected.push("union_all"); // terminal
        assert_eq!(tags(&plan), expected);
        Ok(())
    }

    #[rstest::rstest]
    #[case::without_parsed_partitions(None, false)]
    #[case::compatible_partitions(Some(schema_ref! { nullable "p": STRING }), true)]
    // The table's partition column p is STRING, so native LONG values are incompatible.
    #[case::incompatible_partitions(Some(schema_ref! { nullable "p": LONG }), false)]
    fn metadata_plan_checkpoint_metadata_columns(
        #[case] parsed_partitions: Option<SchemaRef>,
        #[case] expect_native_partitions: bool,
        #[values(None, Some(struct_stats_schema()))] parsed_stats: Option<SchemaRef>,
        #[values(CheckpointType::Leaf, CheckpointType::Manifest)] checkpoint_type: CheckpointType,
    ) -> KernelResult<()> {
        let stats = StatsOptions::all();
        let partition_values = PartitionValuesOptions::with_struct();
        let segment = log_segment(log_root(), &[], Some(checkpoint_path(FileType::Parquet)));
        let scan = mock_snapshot(segment)?
            .scan_builder()
            .with_stats(stats)
            .with_partition_values(partition_values)
            .build()?;
        let shape = CheckpointShape {
            checkpoint_type,
            leaf_checkpoint_schema: Some(parquet_read_schema(
                parsed_stats.as_ref(),
                parsed_partitions.as_ref(),
            )?),
        };
        let plan = scan.build_metadata_scan_plan(&shape)?.expect("non-empty");

        let checkpoint_schema = plan
            .nodes
            .iter()
            .find_map(|node| match &node.op {
                Operator::ScanParquet(scan) if shape.checkpoint_type == CheckpointType::Leaf => {
                    Some(&scan.schema)
                }
                Operator::DynamicScan(scan) => {
                    assert!(scan.dv_column.is_none(), "sidecar scan sets no dv column");
                    Some(&scan.schema)
                }
                _ => None,
            })
            .expect("checkpoint leaf scan");
        assert_eq!(
            add_struct(checkpoint_schema).field(STATS_PARSED).is_some(),
            parsed_stats.is_some(),
        );
        assert_eq!(
            add_struct(checkpoint_schema)
                .field(PARTITION_VALUES_PARSED)
                .is_some(),
            expect_native_partitions,
        );

        let normalization = plan
            .nodes
            .iter()
            .rev()
            .find_map(|node| match &node.op {
                Operator::Project(project) if project.schema.field(IS_ADD).is_some() => {
                    Some(project.expr.to_string())
                }
                _ => None,
            })
            .expect("checkpoint normalization project");
        assert_eq!(
            normalization.contains("MAP_TO_STRUCT"),
            !expect_native_partitions
        );
        assert!(!normalization.contains("COALESCE"));
        Ok(())
    }

    #[test]
    fn metadata_plan_commits_only() -> KernelResult<()> {
        let segment = log_segment(
            log_root(),
            &["file:///_delta_log/00000000000000000001.json"],
            None,
        );
        let scan = mock_snapshot(segment)?.scan_builder().build()?;
        let plan = scan
            .build_metadata_scan_plan(&no_checkpoint())?
            .expect("non-empty");
        assert_eq!(tags(&plan), COMMIT_ARM_TAGS.to_vec());
        Ok(())
    }

    #[rstest::rstest]
    #[case::leaf_parquet(shape(CheckpointType::Leaf, None), FileType::Parquet,
        vec!["scan_parquet", "filter", "project", "project"])]
    #[case::manifest(shape(CheckpointType::Manifest, None), FileType::Parquet,
        vec!["scan_parquet", "filter", "project", "dynamic_scan", "filter", "project", "project"])]
    fn metadata_plan_checkpoint_only(
        #[case] shape: CheckpointShape,
        #[case] file_type: FileType,
        #[case] checkpoint_arm_tags: Vec<&'static str>,
    ) -> KernelResult<()> {
        let segment = log_segment(log_root(), &[], Some(checkpoint_path(file_type)));
        let scan = mock_snapshot(segment)?.scan_builder().build()?;
        let plan = scan.build_metadata_scan_plan(&shape)?.expect("non-empty");
        assert_eq!(tags(&plan), checkpoint_arm_tags);
        Ok(())
    }

    #[test]
    fn metadata_plan_empty_is_none() -> KernelResult<()> {
        let segment = log_segment(log_root(), &[], None);
        let scan = mock_snapshot(segment)?.scan_builder().build()?;
        assert!(scan.build_metadata_scan_plan(&no_checkpoint())?.is_none());
        Ok(())
    }

    #[test]
    fn metadata_plan_static_skip_all_is_none() -> KernelResult<()> {
        let segment = log_segment(log_root(), &[], None);
        let scan = mock_snapshot(segment)?
            .scan_builder()
            .with_predicate(Arc::new(Predicate::FALSE))
            .build()?;
        assert_eq!(
            scan.state_info.physical_predicate,
            PhysicalPredicate::StaticSkipAll
        );
        assert!(scan
            .build_metadata_scan_plan(&shape(CheckpointType::Leaf, None))?
            .is_none());
        Ok(())
    }

    #[test]
    fn metadata_plan_executes_commit_dedup_with_sync_executor() -> KernelResult<()> {
        let store = Arc::new(InMemory::new());
        futures::executor::block_on(async {
            store
                .put(
                    &Path::from("_delta_log/00000000000000000000.json"),
                    r#"{"add":{"path":"a.parquet","size":1,"modificationTime":1,"dataChange":true,"partitionValues":{}}}
{"add":{"path":"b.parquet","size":1,"modificationTime":1,"dataChange":true,"partitionValues":{}}}
"#
                    .into(),
                )
                .await?;
            store
                .put(
                    &Path::from("_delta_log/00000000000000000001.json"),
                    r#"{"remove":{"path":"a.parquet","deletionTimestamp":2,"dataChange":true}}
"#
                    .into(),
                )
                .await?;
            KernelResult::<()>::Ok(())
        })?;

        let segment = log_segment(
            Url::parse("memory:///_delta_log/").unwrap(),
            &[
                "memory:///_delta_log/00000000000000000000.json",
                "memory:///_delta_log/00000000000000000001.json",
            ],
            None,
        );
        let scan = mock_snapshot(segment)?.scan_builder().build()?;
        let plan = scan
            .build_metadata_scan_plan(&no_checkpoint())?
            .expect("non-empty");

        let engine = SyncEngine::new_with_store(store);
        let mut batches = engine
            .plan_executor()
            .unwrap()
            .execute_op(PlanOperation::QueryPlan(plan))?
            .into_data()?;
        let batch = batches
            .next()
            .expect("one batch")?
            .try_into_record_batch()?;
        assert!(batches.next().is_none());
        assert_eq!(batch.num_rows(), 1);

        let add = batch
            .column_by_name(ADD_NAME)
            .expect("add column")
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("add struct");
        let paths = add
            .column_by_name("path")
            .expect("add.path")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("path string");
        assert_eq!(paths.value(0), "b.parquet");
        Ok(())
    }

    #[rstest::rstest]
    #[case::native_variant(schema! {
        nullable "minValues": { nullable "removed": (DataType::unshredded_variant()) },
    }, true)]
    #[case::physical_variant(schema! {
        nullable "minValues": { nullable "v": { nullable "metadata": BINARY, nullable "value": BINARY } },
    }, true)]
    #[case::nested_physical_variant(schema! {
        nullable "maxValues": { nullable "col-outer": {
            nullable "col-variant": { nullable "metadata": BINARY, nullable "value": BINARY },
        } },
    }, true)]
    #[case::null_count_only(schema! { nullable "nullCount": { nullable "v": LONG } }, false)]
    #[case::encoded_string(schema! { nullable "minValues": { nullable "v": STRING } }, false)]
    #[cfg_attr(feature = "geo-type-in-dev", case::native_geometry(schema! {
        nullable "minValues": { nullable "removed": (crate::schema::GeometryType::try_new("EPSG:4326").unwrap()) },
    }, true))]
    #[cfg_attr(feature = "geo-type-in-dev", case::physical_geometry(schema! {
        nullable "minValues": { nullable "g": BINARY },
    }, true))]
    #[cfg_attr(feature = "geo-type-in-dev", case::nested_physical_geometry(schema! {
        nullable "maxValues": { nullable "col-outer": { nullable "col-geometry": BINARY } },
    }, true))]
    #[cfg_attr(feature = "geo-type-in-dev", case::geometry_null_count(schema! {
        nullable "nullCount": { nullable "g": LONG },
    }, false))]
    #[cfg_attr(feature = "geo-type-in-dev", case::encoded_wkt(schema! {
        nullable "minValues": { nullable "g": STRING },
    }, false))]
    fn checkpoint_alternate_json_encoding_policy(
        #[case] stats: StructType,
        #[case] needs_encoding: bool,
        #[values(false, true)] has_json: bool,
    ) -> KernelResult<()> {
        let table = schema! {
            nullable "v": (DataType::unshredded_variant()),
            nullable "col-outer": { nullable "col-variant": (DataType::unshredded_variant()) },
        };
        #[cfg(feature = "geo-type-in-dev")]
        let table = SchemaStructPatchBuilder::new()
            .append(StructField::nullable(
                "g",
                crate::schema::GeometryType::try_new("EPSG:4326")?,
            ))
            .append_at(
                ["col-outer"],
                StructField::nullable(
                    "col-geometry",
                    crate::schema::GeometryType::try_new("EPSG:4326")?,
                ),
            )
            .build(&table)?;
        let mut shape = shape(CheckpointType::Leaf, Some(Arc::new(stats)));
        if !has_json {
            shape.leaf_checkpoint_schema = Some(Arc::new(
                SchemaStructPatchBuilder::new()
                    .drop_at([ADD_NAME], STATS)
                    .build(shape.leaf_checkpoint_schema.as_ref().unwrap())?,
            ));
        }
        let result = checkpoint_json_stats_source(&shape, &table);
        if needs_encoding && !has_json {
            assert!(matches!(result, Err(KernelError::Unsupported(_))));
        } else {
            assert_eq!(result?.is_some(), !has_json && !needs_encoding);
        }
        Ok(())
    }

    #[rstest::rstest]
    fn metadata_plan_preserves_native_typed_stats(
        #[values(StatsOptions::json_only(), StatsOptions::all())] stats: StatsOptions,
        #[values(1, -1)] indexed_columns: i32,
        #[values(1_000_001, -1)] micros: i64,
        #[values(false, true)] has_json: bool,
    ) -> KernelResult<()> {
        const RAW_STATS: &str = r#" { "numRecords" : 999, "custom" : 42 } "#;

        let values = schema! {
            nullable "ts": LONG,
            nullable "d": INTEGER,
            nullable "ntz": LONG,
            nullable "nested": { nullable "f": FLOAT, nullable "i": INTEGER },
        };
        let native = schema_ref! {
            nullable "numRecords": LONG,
            nullable "minValues": (values.clone()),
            nullable "maxValues": (values),
            nullable "extra": LONG,
        };
        let checkpoint_schema = parquet_read_schema(Some(&native), None)?;
        let checkpoint_schema = if has_json {
            checkpoint_schema
        } else {
            Arc::new(
                SchemaStructPatchBuilder::new()
                    .drop_at([ADD_NAME], STATS)
                    .build(&checkpoint_schema)?,
            )
        };
        let checkpoint_shape = CheckpointShape {
            checkpoint_type: CheckpointType::Leaf,
            leaf_checkpoint_schema: Some(checkpoint_schema.clone()),
        };
        let values = serde_json::json!({
            "d": 1, "ts": micros, "ntz": micros, "nested": { "i": 7, "f": 0.1 }
        });
        let mut null_nested = values.clone();
        null_nested["nested"] = serde_json::Value::Null;
        let json: Vec<_> = [
            (
                "values.parquet",
                serde_json::json!({
                    "numRecords": 1, "extra": 42, "minValues": values, "maxValues": values
                }),
            ),
            (
                "null-nested.parquet",
                serde_json::json!({
                    "numRecords": 1, "extra": 42, "minValues": null_nested, "maxValues": null
                }),
            ),
            ("null-stats.parquet", serde_json::Value::Null),
        ]
        .into_iter()
        .map(|(path, stats)| {
            serde_json::json!({
                "add": {
                    "path": path, "size": 1, "modificationTime": 1, "dataChange": true,
                    "partitionValues": {}, "stats_parsed": stats,
                    "stats": (path == "values.parquet").then_some(RAW_STATS)
                }
            })
            .to_string()
        })
        .collect();
        let batch = parse_json_impl(&StringArray::from(json), checkpoint_schema)?;
        let mut bytes = vec![];
        let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), None)?;
        writer.write(&batch)?;
        writer.close()?;
        let store = Arc::new(InMemory::new());
        futures::executor::block_on(store.put(
            &Path::from("_delta_log/00000000000000000000.checkpoint.parquet"),
            bytes.into(),
        ))?;
        let segment = log_segment(
            Url::parse("memory:///_delta_log/").unwrap(),
            &[],
            Some("memory:///_delta_log/00000000000000000000.checkpoint.parquet"),
        );
        let config = MockTableConfigurationBuilder::new()
            .with_schema(schema_ref! {
                nullable "d": DATE,
                nullable "ts": TIMESTAMP,
                nullable "ntz": TIMESTAMP_NTZ,
                nullable "nested": {
                    nullable "i": LONG, nullable "f": DOUBLE, nullable "missing": STRING,
                },
                nullable "missing": STRING,
            })
            .with_properties([(
                "delta.dataSkippingNumIndexedCols",
                indexed_columns.to_string(),
            )])
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::TimestampWithoutTimezone])
                    .build(),
            )
            .with_table_root("memory:///")
            .try_build()?;
        let scan = Arc::new(Snapshot::new(segment, config)?)
            .scan_builder()
            .with_stats(stats)
            .build()?;
        let plan = scan
            .build_metadata_scan_plan(&checkpoint_shape)?
            .expect("non-empty");
        let engine = SyncEngine::new_with_store(store);
        let batches = engine
            .plan_executor()
            .unwrap()
            .execute_op(PlanOperation::QueryPlan(plan))?
            .into_data()?;
        let mut rows = 0;
        for batch in batches {
            let batch = batch?.try_into_record_batch()?;
            rows += batch.num_rows();
            let add = batch.column(0).as_struct();
            let paths = add.column_by_name("path").unwrap().as_string::<i32>();
            let json = add.column_by_name(STATS).unwrap().as_string::<i32>();
            for row in 0..batch.num_rows() {
                let path = paths.value(row);
                let null_stats = path == "null-stats.parquet";
                let null_nested = path == "null-nested.parquet";
                if let Some(stats) = add.column_by_name(STATS_PARSED) {
                    let stats = stats.as_struct();
                    assert_eq!(stats.is_null(row), null_stats);
                    if !null_stats {
                        assert!(stats.column_by_name("tightBounds").unwrap().is_null(row));
                        assert!(stats.column_by_name("nullCount").unwrap().is_null(row));
                        let min = stats.column_by_name("minValues").unwrap().as_struct();
                        assert!(!min.is_null(row));
                        let date = min
                            .column_by_name("d")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<Date32Array>()
                            .unwrap();
                        assert!(!date.is_null(row));
                        assert_eq!(date.value(row), 1);
                        if indexed_columns == -1 {
                            for name in ["ts", "ntz"] {
                                let timestamp = min
                                    .column_by_name(name)
                                    .unwrap()
                                    .as_any()
                                    .downcast_ref::<TimestampMicrosecondArray>()
                                    .unwrap();
                                assert!(!timestamp.is_null(row));
                                assert_eq!(timestamp.value(row), micros, "{name}");
                            }
                            assert!(min.column_by_name("missing").unwrap().is_null(row));
                            let nested = min.column_by_name("nested").unwrap().as_struct();
                            assert_eq!(nested.is_null(row), null_nested);
                            if !null_nested {
                                let integer = nested
                                    .column_by_name("i")
                                    .unwrap()
                                    .as_any()
                                    .downcast_ref::<Int64Array>()
                                    .unwrap();
                                let float = nested
                                    .column_by_name("f")
                                    .unwrap()
                                    .as_any()
                                    .downcast_ref::<Float64Array>()
                                    .unwrap();
                                assert_eq!(integer.value(row), 7);
                                assert_eq!(float.value(row), f64::from(0.1f32));
                                assert!(nested.column_by_name("missing").unwrap().is_null(row));
                            }
                        }
                        assert_eq!(
                            stats.column_by_name("maxValues").unwrap().is_null(row),
                            null_nested
                        );
                    }
                }
                if has_json {
                    // Schema-level selection preserves native JSON verbatim, including NULL even
                    // when the same row has populated stats_parsed.
                    assert_eq!(json.is_null(row), path != "values.parquet");
                    if path == "values.parquet" {
                        assert_eq!(json.value(row), RAW_STATS);
                    }
                    continue;
                }
                assert_eq!(json.is_null(row), null_stats);
                if null_stats {
                    continue;
                }
                let json: serde_json::Value = serde_json::from_str(json.value(row)).unwrap();
                assert_eq!(json["extra"], 42);
                assert!(json.get("tightBounds").is_none());
                assert!(json.get("nullCount").is_none());
                assert!(json["minValues"].get("missing").is_none());
                assert_eq!(json["minValues"]["d"], "1970-01-02");
                let timestamp = if micros > 0 {
                    "1970-01-01T00:00:01.000"
                } else {
                    "1969-12-31T23:59:59.999"
                };
                assert_eq!(json["minValues"]["ts"], format!("{timestamp}Z"));
                assert_eq!(json["minValues"]["ntz"], timestamp);
                if !null_nested {
                    assert!(json["minValues"]["nested"].get("missing").is_none());
                    assert_eq!(json["minValues"]["nested"]["i"], 7);
                }
            }
        }
        assert_eq!(rows, 3);
        Ok(())
    }

    #[rstest::rstest]
    #[case::keeps_matching_file(5, 1)]
    #[case::prunes_non_matching_file(20, 0)]
    fn metadata_plan_executes_leaf_without_stats_parsed(
        #[case] lower_bound: i64,
        #[case] expected_rows: usize,
    ) -> KernelResult<()> {
        let store = Arc::new(InMemory::new());
        // A single-row parquet checkpoint carrying an `add` with a JSON `stats` string but no
        // `stats_parsed` column.
        write_parquet_checkpoint(&store, "_delta_log/00000000000000000000.checkpoint.parquet")?;

        let segment = log_segment(
            Url::parse("memory:///_delta_log/").unwrap(),
            &[],
            Some("memory:///_delta_log/00000000000000000000.checkpoint.parquet"),
        );
        let scan = mock_snapshot(segment)?
            .scan_builder()
            .with_stats(StatsOptions::all())
            .with_predicate(Arc::new(col!("x").gt(lit(lower_bound))))
            .build()?;
        let plan = scan
            // Leaf with no compatible parsed stats -> parse add.stats instead.
            .build_metadata_scan_plan(&shape(CheckpointType::Leaf, None))?
            .expect("non-empty");

        let engine = SyncEngine::new_with_store(store);
        let mut batches = engine
            .plan_executor()
            .unwrap()
            .execute_op(PlanOperation::QueryPlan(plan))?
            .into_data()?;
        let actual_rows = batches.try_fold(0, |rows, batch| {
            Ok::<_, crate::KernelError>(rows + batch?.try_into_record_batch()?.num_rows())
        })?;
        assert_eq!(actual_rows, expected_rows);
        Ok(())
    }
}

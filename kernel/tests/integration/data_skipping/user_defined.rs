//! Data skipping with UDT null counts and ordinary-column min/max statistics.

use std::sync::Arc;

use delta_kernel::expressions::{col, lit, Predicate};
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::scan::StatsOptions;
use delta_kernel::schema::{schema_ref, DataType, UserDefinedType};
use delta_kernel::table_features::{assign_column_mapping_metadata, ColumnMappingMode};
use delta_kernel::Snapshot;
use rstest::rstest;
use serde_json::json;
use test_utils::{add_commit, test_table_setup_mt};
use url::Url;

use super::{surviving_paths_with_stats, AllNullSource};

#[rstest]
#[case::comparison(Predicate::eq(col!("value"), lit(42i64)), vec!["mixed.parquet", "non_null.parquet"])]
#[case::is_null(Predicate::is_null(col!("value")), vec!["all_null.parquet", "mixed.parquet"])]
#[case::is_not_null(Predicate::is_not_null(col!("value")), vec!["mixed.parquet", "non_null.parquet"])]
#[case::ordinary_range(Predicate::gt(col!("other"), lit(15i64)), vec!["all_null.parquet", "mixed.parquet"])]
#[case::ordinary_out_of_range(Predicate::gt(col!("other"), lit(1000i64)), vec![])]
#[case::mixed_and(Predicate::and(Predicate::eq(col!("value"), lit(42i64)), Predicate::gt(col!("other"), lit(15i64))), vec!["mixed.parquet"])]
#[case::mixed_or(Predicate::or(Predicate::eq(col!("value"), lit(42i64)), Predicate::gt(col!("other"), lit(1000i64))), vec!["mixed.parquet", "non_null.parquet"])]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn udt_skipping_ignores_min_max_and_reads_leaf_null_count(
    #[case] predicate: Predicate,
    #[case] expected: Vec<&str>,
    #[values(
        AllNullSource::CommitOnly,
        AllNullSource::CheckpointJsonStats,
        AllNullSource::CheckpointStructStats
    )]
    source: AllNullSource,
    #[values(false, true)] parallel: bool,
    #[values(
        ColumnMappingMode::None,
        ColumnMappingMode::Name,
        ColumnMappingMode::Id
    )]
    mapping_mode: ColumnMappingMode,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, table_path, engine) = test_table_setup_mt()?;
    let table_root = Url::from_directory_path(&table_path).unwrap().to_string();
    let udt = UserDefinedType::try_new(
        DataType::LONG,
        [("class".into(), Some("example.Value".into()))].into(),
    )?;
    let schema = schema_ref! { nullable "value": (udt), nullable "other": LONG };
    let mut max_column_id = 0;
    let schema = if mapping_mode == ColumnMappingMode::None {
        schema
    } else {
        Arc::new(assign_column_mapping_metadata(
            &schema,
            &mut max_column_id,
            false,
        )?)
    };
    let physical_name = schema.field("value").unwrap().physical_name(mapping_mode);
    let other_physical_name = schema.field("other").unwrap().physical_name(mapping_mode);
    let struct_only = source == AllNullSource::CheckpointStructStats;
    let mut configuration = if struct_only {
        json!({"delta.checkpoint.writeStatsAsStruct":"true",
            "delta.checkpoint.writeStatsAsJson":"false"})
    } else {
        json!({})
    };
    let protocol = if mapping_mode == ColumnMappingMode::None {
        json!({"minReaderVersion":1,"minWriterVersion":2})
    } else {
        configuration["delta.columnMapping.mode"] = json!(mapping_mode);
        configuration["delta.columnMapping.maxColumnId"] = json!(max_column_id.to_string());
        json!({"minReaderVersion":3,"minWriterVersion":7,
            "readerFeatures":["columnMapping"], "writerFeatures":["columnMapping"]})
    };
    let mut actions = vec![
        json!({"protocol":protocol}),
        json!({"metaData":{
            "id":"udt-stats", "format":{"provider":"parquet","options":{}},
            "schemaString":serde_json::to_string(&schema)?, "partitionColumns":[],
            "configuration":configuration,
            "createdTime":0,
        }}),
    ];
    for (path, null_count, other_value) in [
        ("non_null.parquet", 0, 10),
        ("mixed.parquet", 1, 20),
        ("all_null.parquet", 2, 30),
    ] {
        // A reader must ignore UDT min/max even when a log contains them.
        let stats = json!({"numRecords":2,
            "nullCount":{(physical_name):null_count, (other_physical_name):0},
            "minValues":{(physical_name):100, (other_physical_name):other_value},
            "maxValues":{(physical_name):100, (other_physical_name):other_value},
            "tightBounds":true});
        actions.push(json!({"add":{
            "path":path, "size":100, "partitionValues":{}, "modificationTime":0,
            "dataChange":true, "stats":stats.to_string(),
        }}));
    }
    add_commit(
        &table_root,
        &LocalFileSystem::new(),
        0,
        actions
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .await?;
    if source != AllNullSource::CommitOnly {
        let snapshot = Snapshot::builder_for(&table_root).build(engine.as_ref())?;
        snapshot.checkpoint(engine.as_ref(), None)?;
    }
    // TODO(#2832): parallel scans need stats_parsed to prune struct-only checkpoint files.
    let expected = if parallel && struct_only {
        vec!["all_null.parquet", "mixed.parquet", "non_null.parquet"]
    } else {
        expected
    };
    assert_eq!(
        surviving_paths_with_stats(
            &table_path,
            engine,
            Arc::new(predicate),
            StatsOptions::default(),
            parallel,
        )?,
        expected
    );
    Ok(())
}

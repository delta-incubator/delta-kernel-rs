//! AMT (adaptiveMetadata) tests for [`IncrementalScanBuilder`]: ranges containing a manifest
//! commit fall back (`None`) because its file actions may live only in the metadata tree.

use std::collections::HashSet;
use std::sync::Arc;

use rstest::rstest;
use test_utils::add_commit;

use crate::engine::sync::SyncEngine;
use crate::log_replay::FileActionKey;
use crate::object_store::memory::InMemory;
use crate::unit_test_utils::{
    adaptive_metadata_table_configuration, test_schema_flat_with_column_mapping,
};
use crate::{Snapshot, Version};

const TABLE_ROOT: &str = "memory:///";

fn protocol_and_metadata_commit() -> String {
    let config = adaptive_metadata_table_configuration(test_schema_flat_with_column_mapping(), &[]);
    format!(
        "{}\n{}",
        serde_json::json!({ "protocol": config.protocol() }),
        serde_json::json!({ "metaData": config.metadata() }),
    )
}

// A commit adding `path`, followed by a `commitInfo` carrying `last_manifest_commit` as
// `(version, contentRootVersion)` when set. The `commitInfo` row comes after the `add` to check
// that it is found anywhere in the commit.
fn add_commit_with_last_manifest_commit(
    path: &str,
    last_manifest_commit: Option<(i64, i64)>,
) -> String {
    let mut commit_info = serde_json::json!({ "operation": "WRITE" });
    if let Some((version, content_root_version)) = last_manifest_commit {
        commit_info["lastManifestCommit"] = serde_json::json!({
            "version": version,
            "contentRootVersion": content_root_version,
        });
    }
    format!(
        "{}\n{}",
        serde_json::json!({ "add": {
            "path": path,
            "partitionValues": {},
            "size": 1,
            "modificationTime": 0,
            "dataChange": true,
        } }),
        serde_json::json!({ "commitInfo": commit_info }),
    )
}

#[rstest]
#[case::no_manifest_commit(None, None, 0, true)]
#[case::manifest_commit_at_base(Some((1, 1)), Some((1, 1)), 1, true)]
#[case::manifest_commit_is_target(None, Some((2, 2)), 1, false)]
#[case::manifest_commit_carried_forward_into_range(Some((1, 1)), Some((1, 1)), 0, false)]
#[case::lagging_content_root_in_range(None, Some((2, 1)), 1, false)]
#[tokio::test]
async fn manifest_commit_in_range_falls_back(
    #[case] v1_last_manifest_commit: Option<(i64, i64)>,
    #[case] v2_last_manifest_commit: Option<(i64, i64)>,
    #[case] base_version: Version,
    #[case] expect_stream: bool,
) {
    let store = Arc::new(InMemory::new());
    let commits = [
        protocol_and_metadata_commit(),
        add_commit_with_last_manifest_commit("a.parquet", v1_last_manifest_commit),
        add_commit_with_last_manifest_commit("b.parquet", v2_last_manifest_commit),
    ];
    for (version, commit) in commits.into_iter().enumerate() {
        add_commit(TABLE_ROOT, store.as_ref(), version as Version, commit)
            .await
            .unwrap();
    }
    let engine = SyncEngine::new_with_store(store);
    let snapshot = Snapshot::builder_for(TABLE_ROOT).build(&engine).unwrap();

    let stream = snapshot
        .incremental_scan_builder(base_version)
        .build(&engine)
        .unwrap();

    assert_eq!(stream.is_some(), expect_stream);
    if let Some(stream) = stream {
        let expected: HashSet<_> = ["a.parquet", "b.parquet"]
            .into_iter()
            .skip(base_version as usize)
            .map(|path| FileActionKey::new(path, None))
            .collect();
        assert_eq!(stream.into_summary().unwrap().live_adds, expected);
    }
}

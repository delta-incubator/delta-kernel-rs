use std::ops::Not;

use url::Url;

use super::scan_file::CdfScanFileType;
use crate::actions::deletion_vector::{deletion_treemap_to_bools, selection_treemap_to_bools};
use crate::table_changes::scan_file::CdfScanFile;
use crate::{DeltaResult, Engine, Error};

struct ResolvedCdfScanFile {
    scan_file: CdfScanFile,
    selection_vector: Vec<bool>,
}

/// Resolves the deletion vectors for an [`UnresolvedCdfScanFile`]. This function handles two
/// types of `CdfScanFile`s:
/// 1. The first case is a [`CdfScanFile`] paired with a remove deletion vector. The `scan_type`
///    must be [`CdfScanFileType::Add`]. In this case, both the add and remove deletion vectors are
///    read if they exist. Then, we find the set of rows in the scan file that have been added, and
///    the set of rows that have been removed. The set of removed rows (if any) will be represented
///    by a [`ResolvedCdfScanFile`] with `scan_type` = [`CdfScanFileType::Remove`]. The set of
///    added rows (if any) will be represented by a [`ResolvedCdfScanFile`] with `scan_type` =
///    [`CdfScanFileType::Add`].
///
///    Note: We allow the possibility for there to be both added rows, and removed rows for a
///    single add/remove pair.
/// 2. The second case handles all other add, remove, and cdc [`CdfScanFile`]s. These will simply
///    have the deletion vector read (if present), and each is converted into a [`ResolvedCdfScanFile`].
///    No changes are made to the `scan_type`.
#[allow(unused)]
pub(crate) fn resolve_scan_file_dv(
    engine: &dyn Engine,
    table_root: &Url,
    scan_file: CdfScanFile,
) -> DeltaResult<impl Iterator<Item = ResolvedCdfScanFile>> {
    let add_dv = scan_file.add_dv.get_treemap(engine, table_root)?;
    let rm_dv = scan_file
        .remove_dv
        .as_ref()
        .map(|rm_dv| rm_dv.get_treemap(engine, table_root))
        .transpose()?;
    println!(
        "initial {add_dv:?}, {rm_dv:?}, scan type: {:?}",
        scan_file.scan_type
    );
    let (add_dv, rm_dv) = match (add_dv, rm_dv, &scan_file.scan_type) {
        (_, Some(_), CdfScanFileType::Remove) => {
            return Err(Error::generic(
                "CdfScanFile with type remove cannot have a remove deletion vector",
            ));
        }
        (_, Some(_), CdfScanFileType::Cdc) => {
            return Err(Error::generic(
                "CdfScanFile with type cdccannot have a remove deletion vector",
            ));
        }
        (add_dv, Some(rm_dv), CdfScanFileType::Add) => {
            let add_dv = add_dv.unwrap_or_else(Default::default);
            let rm_dv = rm_dv.unwrap_or_else(Default::default);
            // Take the symmetric difference so we don't double count rows
            let adds = &rm_dv - &add_dv;
            let removes = add_dv - rm_dv;
            (
                adds.is_empty().not().then_some(adds),
                removes.is_empty().not().then_some(removes),
            )
        }
        (add_dv, None, CdfScanFileType::Add | CdfScanFileType::Cdc) => {
            (Some(add_dv.unwrap_or_else(Default::default)), None)
        }
        (rm_dv, None, CdfScanFileType::Remove) => {
            (None, Some(rm_dv.unwrap_or_else(Default::default)))
        }
    };
    let treemap_to_bools = if scan_file.remove_dv.is_some() {
        selection_treemap_to_bools
    } else {
        deletion_treemap_to_bools
    };

    let rm_scan_file = CdfScanFile {
        scan_type: CdfScanFileType::Remove,
        ..scan_file.clone()
    };
    let adds = add_dv
        .map(treemap_to_bools)
        .map(|selection_vector| ResolvedCdfScanFile {
            scan_file,
            selection_vector,
        });
    let removes = rm_dv
        .map(treemap_to_bools)
        .map(|selection_vector| ResolvedCdfScanFile {
            scan_file: rm_scan_file,
            selection_vector,
        });
    Ok([adds, removes].into_iter().flatten())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, path::PathBuf};

    use itertools::Itertools;

    use crate::{
        actions::deletion_vector::DeletionVectorDescriptor,
        engine::sync::SyncEngine,
        scan::state::DvInfo,
        table_changes::scan_file::{CdfScanFile, CdfScanFileType},
    };

    use super::resolve_scan_file_dv;

    #[test]
    fn add_with_dv() {
        let engine = SyncEngine::new();
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let table_root = url::Url::from_directory_path(path).unwrap();

        let commit_version = 42_i64;
        let commit_timestamp = 1234_i64;
        let deletion_vector = Some(DeletionVectorDescriptor {
            storage_type: "u".to_string(),
            path_or_inline_dv: "vBn[lx{q8@P<9BNH/isA".to_string(),
            offset: Some(1),
            size_in_bytes: 36,
            cardinality: 2,
        });
        let path = "fake_path".to_string();
        let add_dv = DvInfo { deletion_vector };
        let remove_dv = Some(DvInfo {
            deletion_vector: None,
        });
        let scan_file = CdfScanFile {
            scan_type: CdfScanFileType::Add,
            path: path.clone(),
            add_dv,
            remove_dv,
            partition_values: HashMap::new(),
            commit_version,
            commit_timestamp,
        };

        // Remove: None deleted
        // Add: DV with 0th and 9th bit set (ie deleted)
        let mut expected_sv = vec![false; 10];
        expected_sv[0] = true;
        expected_sv[9] = true;
        let resolved = resolve_scan_file_dv(&engine, &table_root, scan_file)
            .unwrap()
            .map(|file| (file.scan_file.scan_type, file.selection_vector))
            .collect_vec();
        assert_eq!(resolved, vec![(CdfScanFileType::Remove, expected_sv)]);
    }

    #[test]
    fn rm_with_dv() {
        let engine = SyncEngine::new();
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let table_root = url::Url::from_directory_path(path).unwrap();

        let deletion_vector = Some(DeletionVectorDescriptor {
            storage_type: "u".to_string(),
            path_or_inline_dv: "vBn[lx{q8@P<9BNH/isA".to_string(),
            offset: Some(1),
            size_in_bytes: 36,
            cardinality: 2,
        });
        let commit_version = 42_i64;
        let commit_timestamp = 1234_i64;

        let path = "fake_path".to_string();
        let add_dv = DvInfo {
            deletion_vector: None,
        };
        let remove_dv = Some(DvInfo { deletion_vector });
        let scan_file = CdfScanFile {
            scan_type: CdfScanFileType::Add,
            path: path.clone(),
            add_dv,
            remove_dv,
            partition_values: HashMap::new(),
            commit_version,
            commit_timestamp,
        };

        // Remove: DV with 0th and 9th bit set (ie deleted)
        // Add: No rows deleted
        let mut expected_sv = vec![false; 10];
        expected_sv[0] = true;
        expected_sv[9] = true;
        let resolved = resolve_scan_file_dv(&engine, &table_root, scan_file)
            .unwrap()
            .map(|file| (file.scan_file.scan_type, file.selection_vector))
            .collect_vec();
        assert_eq!(resolved, vec![(CdfScanFileType::Add, expected_sv)]);
    }
}

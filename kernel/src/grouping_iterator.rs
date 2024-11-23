use crate::path::ParsedLogPath;
use std::iter::Peekable;
use crate::path::AsUrl;
use crate::FileMeta;

// type to group the delta log files by version
// example delta log files and their grouping by version:
// 1. 0000000001.json
// 2. 0000000001.checkpoint.parquet
// 3. 0000000002.json
// 4. 0000000002.checkpoint.0000000001.0000000002.parquet
// 5. 0000000003.json
//
// The version groups are:
// 1. [1, 2]
// 2. [3, 4]
// 3. [5]
pub struct VersionGroup<L: AsUrl = FileMeta> {
    version: u64,
    files: Vec<ParsedLogPath<L>>,
}

// Files are implementation of an Iterator that yields ParsedLogPath
// So it can be any type that implements Iterator<Item = ParsedLogPath<L>>
// Hence their size is not known at compile time, so we use a Box<dyn Iterator<Item = ParsedLogPath<L>>>
pub struct VersionGroupingIterator<L: AsUrl = FileMeta> {
    files: Peekable<Box<dyn Iterator<Item = ParsedLogPath<L>>>>,
}

// We use a type conversion to allow the caller to pass any iterator that yields ParsedLogPath
// This gives an advantage to group files by version in a streaming fashion if we can assume that 
// the input iterator is already sorted by version, like an S3 listing of delta log files.
impl<T, L> From<T> for VersionGroupingIterator<L>
where
    L: AsUrl + 'static,
    T: Iterator<Item = ParsedLogPath<L>> + 'static,
{
    fn from(value: T) -> Self {
        let files: Box<dyn Iterator<Item = ParsedLogPath<L>>> = Box::new(value);
        VersionGroupingIterator { files: files.peekable() }
    }
}

// By assuming that the input iterator is already sorted by version, we can group the files by version in a streaming fashion
// This assuming is very important, if the input is not sorted by version, the grouping will not be correct
impl<L: AsUrl> Iterator for VersionGroupingIterator<L> {
    type Item = VersionGroup<L>;

    fn next(&mut self) -> Option<VersionGroup<L>> {
        while let Some(logpath) = self.files.next() {
            let version: u64 = logpath.version;
            let mut files = vec![logpath];
            // this is where we look ahead for the next file and check if it has the same version
            // if it does, we add it to the current group
            // if it doesn't, we return the current group and start a new one
            // this is why we need to assume that the input iterator is already sorted by version, because we only check the next file
            while let Some(parsed_logpath) = self.files.next_if(|v| v.version == version) {
                files.push(parsed_logpath)
            }
            return Some(VersionGroup { version, files });
        }
        None
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use url::Url;
    /// Returns a URL pointing to the test data directory containing Delta table log files.
    /// The path is relative to the project root and points to a small test Delta table.
    /// 
    /// # Returns
    /// A URL object representing the canonicalized path to the test Delta log directory
    fn table_log_dir_url() -> Url {
        let path = PathBuf::from("./tests/data/table-with-dv-small/_delta_log/");
        let path = std::fs::canonicalize(path).unwrap();
        Url::from_directory_path(path).unwrap()
    }

    /// Creates a ParsedLogPath for testing by constructing a Delta log file path with the given version and type.
    /// 
    /// # Arguments
    /// * `version` - The version number to use in the filename (will be zero-padded to 20 digits)
    /// * `file_type` - The type of log file to create. Valid options are:
    ///   * "commit" - Creates a commit file (.json)
    ///   * "checkpoint" - Creates a single checkpoint file (.checkpoint.parquet)
    ///   * "multipart1" - Creates part 1 of a 2-part checkpoint
    ///   * "multipart2" - Creates part 2 of a 2-part checkpoint
    ///
    /// # Returns
    /// A ParsedLogPath containing the constructed URL and parsed metadata
    ///
    /// # Panics
    /// Panics if an invalid file_type is provided
    fn create_log_path(version: u64, file_type: &str) -> ParsedLogPath<Url> {
        let base_url = table_log_dir_url();
        let filename = match file_type {
            "commit" => format!("{:020}.json", version),
            "checkpoint" => format!("{:020}.checkpoint.parquet", version),
            "multipart1" => format!("{:020}.checkpoint.0000000001.0000000002.parquet", version),
            "multipart2" => format!("{:020}.checkpoint.0000000002.0000000002.parquet", version),
            _ => panic!("Unknown file type"),
        };
        let url = base_url.join(&filename).unwrap();
        ParsedLogPath::try_from(url).unwrap().unwrap()
    }

    #[test]
    /// Tests the basic functionality of VersionGroupingIterator with a single commit file
    /// 
    /// This test verifies that:
    /// 1. The iterator correctly processes a single commit file
    /// 2. The version group contains the expected version number (1)
    /// 3. The group contains exactly one file
    /// 4. The file is correctly identified as a commit file
    /// 5. After consuming the single group, the iterator is exhausted
    /// 
    /// This represents the simplest possible case for the iterator - a single file
    /// that needs to be grouped by version.
    #[test]
    fn test_single_commit() {
        let paths = vec![create_log_path(1, "commit")];
        let mut iter: VersionGroupingIterator<Url> = VersionGroupingIterator::from(paths.into_iter());
        
        if let Some(group) = iter.next() {
            assert_eq!(group.version, 1);
            assert_eq!(group.files.len(), 1);
            assert!(group.files[0].is_commit());
        } else {
            panic!("Expected a group");
        }
        
        assert!(iter.next().is_none());
    }

    #[test]
    /// Tests that VersionGroupingIterator correctly handles multiple sequential versions
    /// 
    /// This test verifies several critical aspects of version grouping:
    /// 1. The iterator can process multiple commit files with different versions
    /// 2. Files are grouped correctly by their version numbers
    /// 3. The groups are returned in sequential version order (1, 2, 3)
    /// 4. Each group contains exactly one file when there is one file per version
    /// 5. The files in each group are correctly identified as commit files
    /// 6. The iterator is exhausted after processing all versions
    /// 
    /// This test is important because it validates the core functionality of streaming
    /// version-based grouping when processing a Delta table's log directory. The sequential
    /// version ordering is especially critical since the Delta protocol relies on processing
    /// log files in version order to reconstruct table state.
    ///
    /// Example Delta Log Directory:
    /// ```text
    /// _delta_log/
    ///   00000000000000000001.json        -> Group 1: [00000000000000000001.json]
    ///   00000000000000000002.json        -> Group 2: [00000000000000000002.json]
    ///   00000000000000000003.json        -> Group 3: [00000000000000000003.json]
    /// ```
    /// 
    /// The test verifies that the iterator yields three groups, each containing
    /// exactly one commit file, in version order 1->2->3.
    fn test_multiple_versions() {
        let paths = vec![
            create_log_path(1, "commit"),
            create_log_path(2, "commit"),
            create_log_path(3, "commit"),
        ];
        let mut iter: VersionGroupingIterator<Url> = VersionGroupingIterator::from(paths.into_iter());
        
        for expected_version in 1..=3 {
            let group = iter.next().expect("Should have a group");
            assert_eq!(group.version, expected_version);
            assert_eq!(group.files.len(), 1);
            assert!(group.files[0].is_commit());
        }
        assert!(iter.next().is_none());
    }


    #[test]
    /// Tests that VersionGroupingIterator correctly groups a commit file with its checkpoint file
    /// 
    /// This test verifies that:
    /// 1. Files with the same version are grouped together
    /// 2. Both commit and checkpoint files are included in the group
    /// 3. The group has the correct version number
    /// 4. The group contains exactly 2 files (1 commit + 1 checkpoint)
    /// 5. The files are correctly identified as commit and checkpoint types
    /// 6. The iterator is exhausted after the single group
    ///
    /// Example Delta Log Directory:
    /// _delta_log/
    ///   00000000000000000001.json
    ///   00000000000000000001.checkpoint.parquet
    ///
    /// VersionGroup {
    ///   version: 1,
    ///   files: [
    ///     00000000000000000001.json,
    ///     00000000000000000001.checkpoint.parquet
    ///   ]
    /// }
    fn test_version_with_checkpoint() {
        let paths = vec![
            create_log_path(1, "commit"),
            create_log_path(1, "checkpoint"),
        ];
        let mut iter: VersionGroupingIterator<Url> = VersionGroupingIterator::from(paths.into_iter());
        
        let group = iter.next().expect("Should have a group");
        assert_eq!(group.version, 1);
        assert_eq!(group.files.len(), 2);
        assert!(group.files.iter().any(|f| f.is_commit()));
        assert!(group.files.iter().any(|f| f.is_checkpoint()));
        
        assert!(iter.next().is_none());
    }

    #[test]
    /// Tests that VersionGroupingIterator correctly handles multi-part checkpoint files
    /// 
    /// This test verifies that:
    /// 1. All files with the same version are grouped together
    /// 2. The group includes both parts of a multi-part checkpoint
    /// 3. The commit file is included in the same group
    /// 4. The group has the correct version number
    /// 5. The group contains exactly 3 files (1 commit + 2 checkpoint parts)
    /// 6. Files are correctly identified as commit vs checkpoint types
    /// 7. The iterator is exhausted after processing the single group
    ///
    /// Example Delta Log Directory:
    /// _delta_log/
    ///   00000000000000000001.json
    ///   00000000000000000001.checkpoint.0000000001.0000000002.parquet
    ///   00000000000000000001.checkpoint.0000000002.0000000002.parquet
    ///
    /// VersionGroup {
    ///   version: 1,
    ///   files: [
    ///     00000000000000000001.json,
    ///     00000000000000000001.checkpoint.0000000001.0000000002.parquet,
    ///     00000000000000000001.checkpoint.0000000002.0000000002.parquet
    ///   ]
    /// }
    fn test_multipart_checkpoint() {
        let paths = vec![
            create_log_path(1, "commit"),
            create_log_path(1, "multipart1"),
            create_log_path(1, "multipart2"),
        ];
        let mut iter: VersionGroupingIterator<Url> = VersionGroupingIterator::from(paths.into_iter());
        
        let group = iter.next().expect("Should have a group");
        assert_eq!(group.version, 1);
        assert_eq!(group.files.len(), 3);
        
        let (commits, checkpoints): (Vec<_>, Vec<_>) = group.files
            .iter()
            .partition(|f| f.is_commit());
        
        assert_eq!(commits.len(), 1, "Should have one commit");
        assert_eq!(checkpoints.len(), 2, "Should have two checkpoint parts");
        
        assert!(iter.next().is_none());
    }

    #[test]
    /// Tests that VersionGroupingIterator correctly handles a mix of versions and file types
    /// 
    /// This test verifies that:
    /// 1. Files are correctly grouped by version number
    /// 2. Each group contains the right number and types of files
    /// 3. Groups are returned in version order
    /// 4. The iterator processes all groups and terminates properly
    ///
    /// Test Data Structure:
    /// Version 1:
    ///   - One commit file
    ///   - Two parts of a multi-part checkpoint
    /// Version 2:
    ///   - One commit file
    ///   - One single checkpoint file
    /// Version 3:
    ///   - One commit file only
    ///
    /// Example Delta Log Directory:
    /// _delta_log/
    ///   00000000000000000001.json
    ///   00000000000000000001.checkpoint.0000000001.0000000002.parquet
    ///   00000000000000000001.checkpoint.0000000002.0000000002.parquet
    ///   00000000000000000002.json
    ///   00000000000000000002.checkpoint.parquet
    ///   00000000000000000003.json
    ///
    /// Expected Version Groups:
    /// VersionGroup {
    ///   version: 1,
    ///   files: [
    ///     00000000000000000001.json,
    ///     00000000000000000001.checkpoint.0000000001.0000000002.parquet,
    ///     00000000000000000001.checkpoint.0000000002.0000000002.parquet
    ///   ]
    /// }
    /// VersionGroup {
    ///   version: 2,
    ///   files: [
    ///     00000000000000000002.json,
    ///     00000000000000000002.checkpoint.parquet
    ///   ]
    /// }
    /// VersionGroup {
    ///   version: 3,
    ///   files: [
    ///     00000000000000000003.json
    ///   ]
    /// }
    fn test_mixed_versions_and_types() {
        let paths = vec![
            create_log_path(1, "commit"),
            create_log_path(1, "multipart1"),
            create_log_path(1, "multipart2"),
            create_log_path(2, "commit"),
            create_log_path(2, "checkpoint"),
            create_log_path(3, "commit"),
        ];
        let mut iter: VersionGroupingIterator<Url> = VersionGroupingIterator::from(paths.into_iter());
        
        // Version 1 group
        let group = iter.next().expect("Should have version 1");
        assert_eq!(group.version, 1);
        assert_eq!(group.files.len(), 3);
        assert_eq!(
            group.files.iter().filter(|f| f.is_commit()).count(),
            1,
            "Should have one commit"
        );
        assert_eq!(
            group.files.iter().filter(|f| f.is_checkpoint()).count(),
            2,
            "Should have two checkpoint parts"
        );
        
        // Version 2 group
        let group = iter.next().expect("Should have version 2");
        assert_eq!(group.version, 2);
        assert_eq!(group.files.len(), 2);
        assert_eq!(
            group.files.iter().filter(|f| f.is_commit()).count(),
            1,
            "Should have one commit"
        );
        assert_eq!(
            group.files.iter().filter(|f| f.is_checkpoint()).count(),
            1,
            "Should have one checkpoint"
        );
        
        // Version 3 group
        let group = iter.next().expect("Should have version 3");
        assert_eq!(group.version, 3);
        assert_eq!(group.files.len(), 1);
        assert!(group.files[0].is_commit(), "Should be a commit file");
        
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_empty_iterator() {
        // Test that an empty input iterator returns no groups
        let paths: Vec<ParsedLogPath<Url>> = vec![];
        let mut iter: VersionGroupingIterator<Url> = VersionGroupingIterator::from(paths.into_iter());
        // Verify that next() returns None when there are no items
        assert!(iter.next().is_none());
    }
    // We expect the caller to sort the input before passing it to the iterator
    // hence the test is not needed, if uncommented and run, it will fail
    // #[test]
    // fn test_unsorted_input() {
    //     let paths = vec![
    //         create_log_path(2, "commit"),
    //         create_log_path(1, "commit"),
    //         create_log_path(1, "checkpoint"),
    //         create_log_path(2, "checkpoint"),
    //     ];
    //     let mut iter: VersionGroupingIterator<Url> = VersionGroupingIterator::from(paths.into_iter());
        
    //     // Should still group by version regardless of input order
    //     for version in 1..=2 {
    //         let group = iter.next().expect("Should have a group");
    //         assert_eq!(group.version, version);
    //         assert_eq!(group.files.len(), 2);
    //         assert!(group.files.iter().any(|f| f.is_commit()));
    //         assert!(group.files.iter().any(|f| f.is_checkpoint()));
    //     }
        
    //     assert!(iter.next().is_none());
    // }
}


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
}


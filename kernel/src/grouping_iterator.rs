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


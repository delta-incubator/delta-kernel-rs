use crate::path::ParsedLogPath;
use std::iter::Peekable;

pub struct VersionGroup {
    version: u64,
    files: Vec<ParsedLogPath>,
}

pub struct VersionGroupingIterator {
    // Peekable iterator that yields (Version, ParsedLogPath) pairs
    // Peekable is needed to look ahead for grouping files with same version
    files: Peekable<Box<dyn Iterator<Item = VersionGroup>>>,
}

impl<T: Iterator<Item = ParsedLogPath> + 'static> From<T> for VersionGroupingIterator {
    fn from(value: T) -> Self {
        // No need for map_while since version is already parsed
        let files: Box<dyn Iterator<Item = _>> = Box::new(value);
        VersionGroupingIterator { files: files.peekable() }
    }
}


//! Utilities to make working with directory and file paths easier

use std::str::FromStr;
use url::Url;

use crate::{DeltaResult, Error, FileMeta, Version};

/// How many characters a version tag has
const VERSION_LEN: usize = 20;

/// How many characters a part specifier on a multipart checkpoint has
const MULTIPART_PART_LEN: usize = 10;

/// The number of characters in the uuid part of a uuid checkpoint
const UUID_PART_LEN: usize = 36;

#[derive(Debug)]
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
#[cfg_attr(not(feature = "developer-visibility"), visibility::make(pub(crate)))]
enum LogPathFileType {
    Commit,
    SinglePartCheckpoint,
    #[allow(unused)]
    UuidCheckpoint(String),
    // NOTE: Delta spec doesn't actually say, but checkpoint part numbers are effectively 31-bit
    // unsigned integers: Negative values are never allowed, but Java integer types are always
    // signed. Approximate that as u32 here.
    #[allow(unused)]
    MultiPartCheckpoint {
        part_num: u32,
        num_parts: u32,
    },
    #[allow(unused)]
    CompactedCommit {
        hi: Version,
    },
    Unknown,
}

#[derive(Debug)]
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
#[cfg_attr(not(feature = "developer-visibility"), visibility::make(pub(crate)))]
struct ParsedLogPath<Location: AsUrl = FileMeta> {
    pub location: Location,
    #[allow(unused)]
    pub filename: String,
    #[allow(unused)]
    pub extension: String,
    pub version: Version,
    pub file_type: LogPathFileType,
}

// Internal helper used by TryFrom<FileMeta> below. It parses a fixed-length string into the numeric
// type expected by the caller. A wrong length produces an error, even if the parse succeeded.
fn parse_path_part<T: FromStr>(value: &str, expect_len: usize, location: &Url) -> DeltaResult<T> {
    match value.parse() {
        Ok(result) if value.len() == expect_len => Ok(result),
        _ => Err(Error::invalid_log_path(location)),
    }
}

// We normally construct ParsedLogPath from FileMeta, but in testing it's convenient to use
// a Url directly instead. This trait decouples the two.
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
#[cfg_attr(not(feature = "developer-visibility"), visibility::make(pub(crate)))]
trait AsUrl {
    fn as_url(&self) -> &Url;
}

impl AsUrl for FileMeta {
    fn as_url(&self) -> &Url {
        &self.location
    }
}

impl<Location: AsUrl> ParsedLogPath<Location> {
    // NOTE: We can't actually impl TryFrom because Option<T> is a foreign struct even if T is local.
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    #[cfg_attr(not(feature = "developer-visibility"), visibility::make(pub(crate)))]
    fn try_from(location: Location) -> DeltaResult<Option<ParsedLogPath<Location>>> {
        let url = location.as_url();
        let filename = url
            .path_segments()
            .ok_or_else(|| Error::invalid_log_path(url))?
            .last()
            .unwrap() // "the iterator always contains at least one string (which may be empty)"
            .to_string();
        if filename.is_empty() {
            return Err(Error::invalid_log_path(url));
        }

        let mut split = filename.split('.');

        // NOTE: str::split always returns at least one item, even for the empty string.
        let version = split.next().unwrap();

        // Every valid log path starts with a numeric version part. If version parsing fails, it
        // must not be a log path and we simply return None. However, it is an error if version
        // parsing succeeds for a wrong-length numeric string.
        let version = match version.parse().ok() {
            Some(v) if version.len() == VERSION_LEN => v,
            Some(_) => return Err(Error::invalid_log_path(url)),
            None => return Ok(None),
        };

        // Every valid log path has a file extension as its last part. Return None if it's missing.
        let split: Vec<_> = split.collect();
        let extension = match split.last() {
            Some(extension) => extension.to_string(),
            None => return Ok(None),
        };

        // Parse the file type, based on the number of remaining parts
        let file_type = match split.as_slice() {
            ["json"] => LogPathFileType::Commit,
            ["checkpoint", "parquet"] => LogPathFileType::SinglePartCheckpoint,
            ["checkpoint", uuid, "json" | "parquet"] => {
                let uuid = parse_path_part(uuid, UUID_PART_LEN, url)?;
                LogPathFileType::UuidCheckpoint(uuid)
            }
            [hi, "compacted", "json"] => {
                let hi = parse_path_part(hi, VERSION_LEN, url)?;
                LogPathFileType::CompactedCommit { hi }
            }
            ["checkpoint", part_num, num_parts, "parquet"] => {
                let part_num = parse_path_part(part_num, MULTIPART_PART_LEN, url)?;
                let num_parts = parse_path_part(num_parts, MULTIPART_PART_LEN, url)?;

                // A valid part_num must be in the range [1, num_parts]
                if !(0 < part_num && part_num <= num_parts) {
                    return Err(Error::invalid_log_path(url));
                }
                LogPathFileType::MultiPartCheckpoint {
                    part_num,
                    num_parts,
                }
            }

            // Unrecognized log paths are allowed, so long as they have a valid version.
            _ => LogPathFileType::Unknown,
        };
        Ok(Some(ParsedLogPath {
            location,
            filename,
            extension,
            version,
            file_type,
        }))
    }

    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    #[cfg_attr(not(feature = "developer-visibility"), visibility::make(pub(crate)))]
    fn is_commit(&self) -> bool {
        matches!(self.file_type, LogPathFileType::Commit)
    }

    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    #[cfg_attr(not(feature = "developer-visibility"), visibility::make(pub(crate)))]
    fn is_checkpoint(&self) -> bool {
        // TODO: Include UuidCheckpoint once we actually support v2 checkpoints
        matches!(
            self.file_type,
            LogPathFileType::SinglePartCheckpoint | LogPathFileType::MultiPartCheckpoint { .. }
        )
    }

    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    #[cfg_attr(not(feature = "developer-visibility"), visibility::make(pub(crate)))]
    #[allow(dead_code)] // currently only used in tests, which don't "count"
    fn is_unknown(&self) -> bool {
        // TODO: Stop treating UuidCheckpoint as unknown once we support v2 checkpoints
        matches!(
            self.file_type,
            LogPathFileType::Unknown | LogPathFileType::UuidCheckpoint(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    // Easier to test directly with Url instead of FileMeta!
    impl AsUrl for Url {
        fn as_url(&self) -> &Url {
            self
        }
    }

    fn table_log_dir_url() -> Url {
        let path = PathBuf::from("./tests/data/table-with-dv-small/_delta_log/");
        let path = std::fs::canonicalize(path).unwrap();
        assert!(path.is_dir());
        let url = url::Url::from_directory_path(path).unwrap();
        assert!(url.path().ends_with('/'));
        url
    }

    fn return_table_log_dir_url(path: &str) -> Url {
        let path = PathBuf::from(path);
        let path = std::fs::canonicalize(path).unwrap();
        assert!(path.is_dir());
        let url = url::Url::from_directory_path(path).unwrap();
        assert!(url.path().ends_with('/'));
        url
    }
    #[test]
    fn test_invalid_version_and_part_lengths() {
        let table_log_dir =
            return_table_log_dir_url("./tests/data/multiple-checkpoint-faulty-1/_delta_log/");

        // Test cases for VERSION_LEN (20 digits)
        let test_cases = vec![
            // Error expected: Version number is 19 digits, but should be 20
            ("000000000000000006.json", "short version number"),
            // Error expected: Version number is 21 digits, but should be 20
            ("000000000000000000006.json", "long version number"),
        ];

        for (file_name, error_msg) in test_cases {
            let log_path = table_log_dir.join(file_name).unwrap();
            let result = ParsedLogPath::try_from(log_path);
            assert!(result.is_err(), "Expected an error for {}", error_msg);
            assert!(matches!(result, Err(Error::InvalidLogPath(_))), "Expected InvalidLogPath error for {}", error_msg);
        }

        // Test cases for MULTIPART_PART_LEN (10 digits)
        let test_cases = vec![
            // Error expected: Part number is 9 digits, but should be 10
            ("00000000000000000010.checkpoint.000000001.0000000002.parquet", "short part number"),
            // Error expected: Part number is 11 digits, but should be 10
            ("00000000000000000010.checkpoint.00000000001.0000000002.parquet", "long part number"),
            // Error expected: Total parts is 9 digits, but should be 10
            ("00000000000000000010.checkpoint.0000000001.000000002.parquet", "short total parts"),
            // Error expected: Total parts is 11 digits, but should be 10
            ("00000000000000000010.checkpoint.0000000001.00000000002.parquet", "long total parts"),
        ];

        for (file_name, error_msg) in test_cases {
            let log_path = table_log_dir.join(file_name).unwrap();
            let result = ParsedLogPath::try_from(log_path);
            assert!(result.is_err(), "Expected an error for {}", error_msg);
            assert!(matches!(result, Err(Error::InvalidLogPath(_))), "Expected InvalidLogPath error for {}", error_msg);
        }

        // Test cases for UUID_PART_LEN (36 characters)
        let test_cases = vec![
            // Error expected: UUID is 35 characters, but should be 36
            ("00000000000000000010.checkpoint.3a0d65cd-4056-49b8-937b-95f9e3ee90.parquet", "short UUID"),
            // Error expected: UUID is 37 characters, but should be 36
            ("00000000000000000010.checkpoint.3a0d65cd-4056-49b8-937b-95f9e3ee90e5a.parquet", "long UUID"),
        ];

        for (file_name, error_msg) in test_cases {
            let log_path = table_log_dir.join(file_name).unwrap();
            let result = ParsedLogPath::try_from(log_path);
            assert!(result.is_err(), "Expected an error for {}", error_msg);
            assert!(matches!(result, Err(Error::InvalidLogPath(_))), "Expected InvalidLogPath error for {}", error_msg);
        }
    }
    fn test_unknown_invalid_patterns() {
        let table_log_dir = table_log_dir_url();

        // invalid -- not a file
        let log_path = table_log_dir.join("subdir/").unwrap();
        assert!(log_path
            .path()
            .ends_with("/tests/data/table-with-dv-small/_delta_log/subdir/"));
        ParsedLogPath::try_from(log_path).expect_err("directory path");

        // ignored - not versioned
        let log_path = table_log_dir.join("_last_checkpoint").unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap();
        assert!(log_path.is_none());

        // ignored - no extension
        let log_path = table_log_dir.join("00000000000000000010").unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap();
        assert!(log_path.is_none());

        // ignored - version fails to parse
        let log_path = table_log_dir.join("abc.json").unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap();
        assert!(log_path.is_none());

        // invalid - version has too many digits
        let log_path = table_log_dir.join("000000000000000000010.json").unwrap();
        ParsedLogPath::try_from(log_path).expect_err("too many digits");

        // invalid - version has too few digits
        let log_path = table_log_dir.join("0000000000000000010.json").unwrap();
        ParsedLogPath::try_from(log_path).expect_err("too few digits");

        // unknown - two parts
        let log_path = table_log_dir.join("00000000000000000010.foo").unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(log_path.filename, "00000000000000000010.foo");
        assert_eq!(log_path.extension, "foo");
        assert_eq!(log_path.version, 10);
        assert!(matches!(log_path.file_type, LogPathFileType::Unknown));
        assert!(log_path.is_unknown());

        // unknown - many parts
        let log_path = table_log_dir
            .join("00000000000000000010.a.b.c.foo")
            .unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(log_path.filename, "00000000000000000010.a.b.c.foo");
        assert_eq!(log_path.extension, "foo");
        assert_eq!(log_path.version, 10);
        assert!(log_path.is_unknown());
    }

    #[test]
    fn test_commit_patterns() {
        let table_log_dir = table_log_dir_url();

        let log_path = table_log_dir.join("00000000000000000000.json").unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(log_path.filename, "00000000000000000000.json");
        assert_eq!(log_path.extension, "json");
        assert_eq!(log_path.version, 0);
        assert!(matches!(log_path.file_type, LogPathFileType::Commit));
        assert!(log_path.is_commit());
        assert!(!log_path.is_checkpoint());

        let log_path = table_log_dir.join("00000000000000000005.json").unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(log_path.version, 5);
        assert!(log_path.is_commit());
    }

    #[test]
    fn test_single_part_checkpoint_patterns() {
        let table_log_dir = table_log_dir_url();

        let log_path = table_log_dir
            .join("00000000000000000002.checkpoint.parquet")
            .unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(log_path.filename, "00000000000000000002.checkpoint.parquet");
        assert_eq!(log_path.extension, "parquet");
        assert_eq!(log_path.version, 2);
        assert!(matches!(
            log_path.file_type,
            LogPathFileType::SinglePartCheckpoint
        ));
        assert!(!log_path.is_commit());
        assert!(log_path.is_checkpoint());

        // invalid file extension
        let log_path = table_log_dir
            .join("00000000000000000002.checkpoint.json")
            .unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(log_path.filename, "00000000000000000002.checkpoint.json");
        assert_eq!(log_path.extension, "json");
        assert_eq!(log_path.version, 2);
        assert!(!log_path.is_commit());
        assert!(!log_path.is_checkpoint());
        assert!(log_path.is_unknown());
    }

    #[test]
    fn test_uuid_checkpoint_patterns() {
        let table_log_dir = table_log_dir_url();

        let log_path = table_log_dir
            .join("00000000000000000002.checkpoint.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.parquet")
            .unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(
            log_path.filename,
            "00000000000000000002.checkpoint.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.parquet"
        );
        assert_eq!(log_path.extension, "parquet");
        assert_eq!(log_path.version, 2);
        assert!(matches!(
            log_path.file_type,
            LogPathFileType::UuidCheckpoint(ref u) if u == "3a0d65cd-4056-49b8-937b-95f9e3ee90e5",
        ));
        assert!(!log_path.is_commit());

        // TODO: Support v2 checkpoints! Until then we can't treat these as checkpoint files.
        assert!(!log_path.is_checkpoint());
        assert!(log_path.is_unknown());

        let log_path = table_log_dir
            .join("00000000000000000002.checkpoint.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json")
            .unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(
            log_path.filename,
            "00000000000000000002.checkpoint.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json"
        );
        assert_eq!(log_path.extension, "json");
        assert_eq!(log_path.version, 2);
        assert!(matches!(
            log_path.file_type,
            LogPathFileType::UuidCheckpoint(ref u) if u == "3a0d65cd-4056-49b8-937b-95f9e3ee90e5",
        ));
        assert!(!log_path.is_commit());

        // TODO: Support v2 checkpoints! Until then we can't treat these as checkpoint files.
        assert!(!log_path.is_checkpoint());
        assert!(log_path.is_unknown());

        let log_path = table_log_dir
            .join("00000000000000000002.checkpoint.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.foo")
            .unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(
            log_path.filename,
            "00000000000000000002.checkpoint.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.foo"
        );
        assert_eq!(log_path.extension, "foo");
        assert_eq!(log_path.version, 2);
        assert!(!log_path.is_commit());
        assert!(!log_path.is_checkpoint());
        assert!(log_path.is_unknown());

        let log_path = table_log_dir
            .join("00000000000000000002.checkpoint.foo.parquet")
            .unwrap();
        ParsedLogPath::try_from(log_path).expect_err("not a uuid");

        // invalid file extension
        let log_path = table_log_dir
            .join("00000000000000000002.checkpoint.foo")
            .unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(log_path.filename, "00000000000000000002.checkpoint.foo");
        assert_eq!(log_path.extension, "foo");
        assert_eq!(log_path.version, 2);
        assert!(!log_path.is_commit());
        assert!(!log_path.is_checkpoint());
        assert!(log_path.is_unknown());
    }

    #[test]
    fn test_multi_part_checkpoint_patterns() {
        let table_log_dir = table_log_dir_url();

        let log_path = table_log_dir
            .join("00000000000000000008.checkpoint.0000000000.0000000002.json")
            .unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(
            log_path.filename,
            "00000000000000000008.checkpoint.0000000000.0000000002.json"
        );
        assert_eq!(log_path.extension, "json");
        assert_eq!(log_path.version, 8);
        assert!(!log_path.is_commit());
        assert!(!log_path.is_checkpoint());
        assert!(log_path.is_unknown());

        let log_path = table_log_dir
            .join("00000000000000000008.checkpoint.0000000000.0000000002.parquet")
            .unwrap();
        ParsedLogPath::try_from(log_path).expect_err("invalid part 0");

        let log_path = table_log_dir
            .join("00000000000000000008.checkpoint.0000000001.0000000002.parquet")
            .unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(
            log_path.filename,
            "00000000000000000008.checkpoint.0000000001.0000000002.parquet"
        );
        assert_eq!(log_path.extension, "parquet");
        assert_eq!(log_path.version, 8);
        assert!(matches!(
            log_path.file_type,
            LogPathFileType::MultiPartCheckpoint {
                part_num: 1,
                num_parts: 2
            }
        ));
        assert!(!log_path.is_commit());
        assert!(log_path.is_checkpoint());

        let log_path = table_log_dir
            .join("00000000000000000008.checkpoint.0000000002.0000000002.parquet")
            .unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(
            log_path.filename,
            "00000000000000000008.checkpoint.0000000002.0000000002.parquet"
        );
        assert_eq!(log_path.extension, "parquet");
        assert_eq!(log_path.version, 8);
        assert!(matches!(
            log_path.file_type,
            LogPathFileType::MultiPartCheckpoint {
                part_num: 2,
                num_parts: 2
            }
        ));
        assert!(!log_path.is_commit());
        assert!(log_path.is_checkpoint());

        let log_path = table_log_dir
            .join("00000000000000000008.checkpoint.0000000003.0000000002.parquet")
            .unwrap();
        ParsedLogPath::try_from(log_path).expect_err("invalid part 3");

        let log_path = table_log_dir
            .join("00000000000000000008.checkpoint.000000001.0000000002.parquet")
            .unwrap();
        ParsedLogPath::try_from(log_path).expect_err("invalid part_num");

        let log_path = table_log_dir
            .join("00000000000000000008.checkpoint.0000000001.000000002.parquet")
            .unwrap();
        ParsedLogPath::try_from(log_path).expect_err("invalid num_parts");

        let log_path = table_log_dir
            .join("00000000000000000008.checkpoint.00000000x1.0000000002.parquet")
            .unwrap();
        ParsedLogPath::try_from(log_path).expect_err("invalid part_num");

        let log_path = table_log_dir
            .join("00000000000000000008.checkpoint.0000000001.00000000x2.parquet")
            .unwrap();
        ParsedLogPath::try_from(log_path).expect_err("invalid num_parts");
    }

    #[test]
    fn test_compacted_delta_patterns() {
        let table_log_dir = table_log_dir_url();

        let log_path = table_log_dir
            .join("00000000000000000008.00000000000000000015.compacted.json")
            .unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(
            log_path.filename,
            "00000000000000000008.00000000000000000015.compacted.json"
        );
        assert_eq!(log_path.extension, "json");
        assert_eq!(log_path.version, 8);
        assert!(matches!(
            log_path.file_type,
            LogPathFileType::CompactedCommit { hi: 15 },
        ));
        assert!(!log_path.is_commit());
        assert!(!log_path.is_checkpoint());

        // invalid extension
        let log_path = table_log_dir
            .join("00000000000000000008.00000000000000000015.compacted.parquet")
            .unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(
            log_path.filename,
            "00000000000000000008.00000000000000000015.compacted.parquet"
        );
        assert_eq!(log_path.extension, "parquet");
        assert_eq!(log_path.version, 8);
        assert!(!log_path.is_commit());
        assert!(!log_path.is_checkpoint());
        assert!(log_path.is_unknown());

        let log_path = table_log_dir
            .join("00000000000000000008.0000000000000000015.compacted.json")
            .unwrap();
        ParsedLogPath::try_from(log_path).expect_err("too few digits in hi");

        let log_path = table_log_dir
            .join("00000000000000000008.000000000000000000015.compacted.json")
            .unwrap();
        ParsedLogPath::try_from(log_path).expect_err("too many digits in hi");

        let log_path = table_log_dir
            .join("00000000000000000008.00000000000000000a15.compacted.json")
            .unwrap();
        ParsedLogPath::try_from(log_path).expect_err("non-numeric hi");
    }

    #[test]
    fn test_empty_filename() {
        let table_log_dir = table_log_dir_url();
        
        // Test URL ending with a slash
        let log_path = table_log_dir.join("").unwrap();
        let result = ParsedLogPath::try_from(log_path);
        assert!(result.is_err(), "Expected an error for empty filename");
        assert!(matches!(result, Err(Error::InvalidLogPath(_))), "Expected InvalidLogPath error for empty filename");
    }

    #[test]
    fn test_ok_none_and_unknown_cases() {
        let table_log_dir = return_table_log_dir_url("./tests/data/multiple-checkpoint-faulty-1/_delta_log/");

        // Test cases for non-numeric version (should return Ok(None))
        let test_cases = vec![
            "_last_checkpoint",
            "abc.json",
            "version_001.json",
        ];

        for file_name in test_cases {
            let log_path = table_log_dir.join(file_name).unwrap();
            let result = ParsedLogPath::try_from(log_path);
            assert!(matches!(result, Ok(None)), "Expected Ok(None) for non-numeric version: {}", file_name);
        }

        // Test case for missing file extension (should return Ok(None))
        let log_path = table_log_dir.join("00000000000000000010").unwrap();
        let result = ParsedLogPath::try_from(log_path);
        assert!(matches!(result, Ok(None)), "Expected Ok(None) for missing file extension");

        // Test case for empty extension (should return Ok(Some) with Unknown file type)
        let log_path = table_log_dir.join("00000000000000000011.").unwrap();
        let result = ParsedLogPath::try_from(log_path);
        assert!(matches!(result, Ok(Some(_))), "Expected Ok(Some) for empty extension");
        if let Ok(Some(parsed)) = result {
            assert!(parsed.is_unknown(), "Expected Unknown file type for empty extension");
        }
    }
}
//! Utilities to make working with directory and file paths easier

use url::Url;

use crate::{DeltaResult, Version};

#[derive(Debug)]
pub(crate) struct LogPath<'a> {
    url: &'a Url,
    path: &'a str,
    version: Option<Version>,
    is_commit: bool,
    is_checkpoint: bool,
}

fn get_filename(path: &str) -> Option<&str> {
    if path.is_empty() || path.ends_with('/') {
        None
    } else {
        path.rsplit(std::path::MAIN_SEPARATOR_STR).next()
    }
}

fn try_get_version(version_str_opt: Option<&str>, expected_digits: usize) -> Option<Version> {
    version_str_opt.and_then(|version_str| {
        if version_str.len() == expected_digits {
            version_str.parse().ok()
        } else {
            None
        }
    })
}

impl<'a> LogPath<'a> {
    pub(crate) fn new(url: &'a Url) -> Self {
        let path = url.path();
        let version_str = get_filename(path).and_then(|f| f.split_once('.'));
        let version = version_str.and_then(|(name, _)| try_get_version(Some(name), 20));

        let mut is_commit = false;
        let mut is_checkpoint = false;
        if version.is_some() {
            // could be a checkpoint or commit file, let's check
            let (_, suffix) = version_str.unwrap(); // safe, version.is_some()
            is_commit = suffix == "json"; // if we were just [version].json, we're a commit file

            if !is_commit && suffix.starts_with("checkpoint.") {
                let rest = &suffix[11..];
                // check if name is just [version].checkpoint.parquet, i.e. we have a classic checkpoint
                is_checkpoint = rest == "parquet";
                if !is_checkpoint {
                    // test if we're a multipart checkpoint
                    let mut split = rest.splitn(3, '.');
                    let (checkpoint_index, checkpoint_max, ext) = (
                        try_get_version(split.next(), 10),
                        try_get_version(split.next(), 10),
                        split.next(),
                    );
                    is_checkpoint = checkpoint_index.is_some()
                        && checkpoint_max.is_some()
                        && ext == Some("parquet");
                }
            }
        }
        LogPath {
            url,
            path,
            version,
            is_commit,
            is_checkpoint,
        }
    }

    pub(crate) fn child(&self, path: impl AsRef<str>) -> DeltaResult<Url> {
        Ok(self.url.join(path.as_ref())?)
    }

    /// Returns the last path segment containing the filename stored in this [`LogPath`]
    pub(crate) fn filename(&self) -> Option<&str> {
        get_filename(self.path)
    }

    /// Returns the extension of the file stored in this [`LogPath`], if any
    #[allow(unused)]
    pub(crate) fn extension(&self) -> Option<&str> {
        self.filename()
            .and_then(|f| f.rsplit_once('.'))
            .and_then(|(_, extension)| {
                if extension.is_empty() {
                    None
                } else {
                    Some(extension)
                }
            })
    }

    pub(crate) fn is_checkpoint_file(&self) -> bool {
        self.is_checkpoint
    }

    pub(crate) fn is_commit_file(&self) -> bool {
        self.is_commit
    }

    /// Parse the version number assuming a commit json or checkpoint parquet file
    pub(crate) fn commit_version(&self) -> Option<Version> {
        self.version
    }
}

impl<'a> AsRef<Url> for LogPath<'a> {
    fn as_ref(&self) -> &Url {
        self.url
    }
}

impl<'a> AsRef<str> for LogPath<'a> {
    fn as_ref(&self) -> &str {
        self.url.as_str()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn table_url() -> Url {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        url::Url::from_file_path(path).unwrap()
    }

    #[test]
    fn test_file_patterns() {
        let table_url = table_url();
        let log_path = LogPath::new(&table_url)
            .child("_delta_log/00000000000000000000.json")
            .unwrap();
        let log_path = LogPath::new(&log_path);

        assert_eq!("00000000000000000000.json", log_path.filename().unwrap());
        assert_eq!("json", log_path.extension().unwrap());
        assert!(log_path.is_commit_file());
        assert!(!log_path.is_checkpoint_file());
        assert_eq!(log_path.commit_version(), Some(0));

        let log_path = log_path.child("00000000000000000005.json").unwrap();
        let log_path = LogPath::new(&log_path);

        assert_eq!(log_path.commit_version(), Some(5));

        let log_path = log_path
            .child("00000000000000000002.checkpoint.parquet")
            .unwrap();
        let log_path = LogPath::new(&log_path);

        assert_eq!(
            "00000000000000000002.checkpoint.parquet",
            log_path.filename().unwrap()
        );
        assert_eq!("parquet", log_path.extension().unwrap());
        assert!(!log_path.is_commit_file());
        assert!(log_path.is_checkpoint_file());
        assert_eq!(log_path.commit_version(), Some(2));
    }

    fn test_child_is_multi(log_path: &LogPath<'_>, child: &str, is_checkpoint: bool) {
        let path = log_path.child(child).unwrap();
        let to_test = LogPath::new(&path);
        assert_eq!(to_test.is_checkpoint_file(), is_checkpoint);
    }

    #[test]
    fn test_multipart_parsing() {
        let table_url = table_url();
        let log_path = LogPath::new(&table_url);

        for good_path in [
            "_delta_log/00000000000000000001.checkpoint.0000000001.0000000002.parquet",
            "_delta_log/00000000000000000021.checkpoint.0000000003.0000000010.parquet",
        ] {
            test_child_is_multi(&log_path, good_path, true);
        }

        for bad_path in [
            // `o` value is not 10 digits
            "_delta_log/00000000000000000001.checkpoint.00000001.0000000002.parquet",
            // `p` value is not 10 digits
            "_delta_log/00000000000000000001.checkpoint.0000000001.00000002.parquet",
            // `o` not a number
            "_delta_log/00000000000000000001.checkpoint.000000000a.0000000002.parquet",
            // `p` not a number
            "_delta_log/00000000000000000001.checkpoint.0000000001.000000000x.parquet",
            // doesn't say 'checkpoint'
            "_delta_log/00000000000000000001.checkpoinx.00000001.0000000002.parquet",
            // not .parquet
            "_delta_log/00000000000000000001.checkpoint.00000001.0000000002.json",
        ] {
            test_child_is_multi(&log_path, bad_path, false);
        }
    }
}

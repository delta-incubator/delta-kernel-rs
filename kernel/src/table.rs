//! In-memory representation of a Delta table, which acts as an immutable root entity for reading
//! the different versions

use std::path::PathBuf;
use std::sync::Arc;

use url::Url;

use crate::snapshot::Snapshot;
use crate::{DeltaResult, Engine, Error, Version};

/// In-memory representation of a Delta table, which acts as an immutable root entity for reading
/// the different versions (see [`Snapshot`]) of the table located in storage.
#[derive(Clone)]
pub struct Table {
    location: Url,
}

impl std::fmt::Debug for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("Table")
            .field("location", &self.location)
            .finish()
    }
}

impl Table {
    /// Create a new Delta table with the given parameters
    pub fn new(location: Url) -> Self {
        Self { location }
    }

    /// Try to create a new table from a string uri. This will do it's best to handle things like
    /// `/local/paths`, and even `../relative/paths`.
    pub fn try_from_uri(uri: impl AsRef<str>) -> DeltaResult<Self> {
        let uri = uri.as_ref();
        let uri_type: UriType = resolve_uri_type(uri)?;
        let url = match uri_type {
            UriType::LocalPath(path) => {
                if !path.exists() {
                    // When we support writes, create a directory if we can
                    return Err(Error::InvalidTableLocation(format!(
                        "Path does not exist: {path:?}"
                    )));
                }
                if !path.is_dir() {
                    return Err(Error::InvalidTableLocation(format!(
                        "{path:?} is not a directory"
                    )));
                }
                let path = std::fs::canonicalize(path).map_err(|err| {
                    let msg = format!("Invalid table location: {} Error: {:?}", uri, err);
                    Error::InvalidTableLocation(msg)
                })?;
                Url::from_directory_path(path.clone()).map_err(|_| {
                    let msg = format!(
                        "Could not construct a URL from canonicalized path: {:?}.\n\
                         Something must be very wrong with the table path.",
                        path
                    );
                    Error::InvalidTableLocation(msg)
                })?
            }
            UriType::Url(url) => url,
        };
        Ok(Self::new(url))
    }

    /// Fully qualified location of the Delta table.
    pub fn location(&self) -> &Url {
        &self.location
    }

    /// Create a [`Snapshot`] of the table corresponding to `version`.
    ///
    /// If no version is supplied, a snapshot for the latest version will be created.
    pub fn snapshot(
        &self,
        engine: &dyn Engine,
        version: Option<Version>,
    ) -> DeltaResult<Arc<Snapshot>> {
        Snapshot::try_new(self.location.clone(), engine, version)
    }
}

#[derive(Debug)]
enum UriType {
    LocalPath(PathBuf),
    Url(Url),
}

/// Utility function to figure out whether string representation of the path is either local path or
/// some kind or URL.
///
/// Will return an error if the path is not valid.
fn resolve_uri_type(table_uri: impl AsRef<str>) -> DeltaResult<UriType> {
    let table_uri = table_uri.as_ref();
    let known_schemes = [
        "file", "memory", "s3", "s3a", "az", "adl", "azure", "abfs", "abfss", "gs", "http", "https",
    ]; // hard-code for now, TODO: pull these out of object_store

    if let Ok(url) = Url::parse(table_uri) {
        let scheme = url.scheme().to_string();
        if url.scheme() == "file" {
            Ok(UriType::LocalPath(
                url.to_file_path()
                    .map_err(|_| Error::invalid_table_location(table_uri))?,
            ))
        } else if known_schemes.contains(&scheme.as_str()) {
            Ok(UriType::Url(url))
        // NOTE this check is required to support absolute windows paths which may properly parse as url
        // we assume here that a single character scheme is a windows drive letter
        } else if scheme.len() == 1 {
            Ok(UriType::LocalPath(PathBuf::from(table_uri)))
        } else {
            Err(Error::InvalidTableLocation(format!(
                "Unknown scheme: {}",
                scheme
            )))
        }
    } else {
        Ok(UriType::LocalPath(table_uri.into()))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::engine::sync::SyncEngine;

    #[test]
    fn test_table() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();
        let table = Table::new(url);
        let snapshot = table.snapshot(&engine, None).unwrap();
        assert_eq!(snapshot.version(), 1)
    }

    #[test]
    fn test_path_parsing() {
        for x in [
            "file:///foo/bar",
            "file:///foo/bar/",
            "/foo/bar",
            "/foo/bar/",
            "../foo/bar",
            "../foo/bar/",
            "c:/foo/bar",
            r"e:\foo\bar",
        ] {
            match resolve_uri_type(x) {
                Ok(UriType::LocalPath(_)) => {}
                x => panic!("Should have parsed as a local path {x:?}"),
            }
        }

        for x in [
            "s3://foo/bar",
            "s3a://foo/bar",
            "memory://foo/bar",
            "gs://foo/bar",
            "https://foo/bar/",
        ] {
            match resolve_uri_type(x) {
                Ok(UriType::Url(_)) => {}
                x => panic!("Should have parsed as a url {x:?}"),
            }
        }

        for x in ["unknown://foo/bar", "file://foo/bar", "s2://foo/bar"] {
            resolve_uri_type(x).expect_err(format!("Should not have parsed: {x}").as_str());
        }
    }
}

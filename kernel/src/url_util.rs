use std::path::{Path, PathBuf};
pub use url::Url;  // Re-export Url from the url crate

pub trait UrlExt {
    fn to_file_path(&self) -> Result<PathBuf, &'static str>;
    fn from_file_path(path: impl AsRef<Path>) -> Result<Self, url::ParseError> where Self: Sized;
    fn from_directory_path(path: impl AsRef<Path>) -> Result<Self, url::ParseError> where Self: Sized;
}

impl UrlExt for Url {
    fn to_file_path(&self) -> Result<PathBuf, &'static str> {
        #[cfg(feature = "wasm-engine")]
        {
            tracing::debug!("Converting URL to file path in WebAssembly context: {}", self);
            let path = self.path();
            tracing::debug!("Extracted path: {}", path);
            Ok(path.into())
        }

        #[cfg(not(feature = "wasm-engine"))]
        self.to_file_path()
    }

    fn from_file_path(path: impl AsRef<Path>) -> Result<Self, url::ParseError> {
        #[cfg(feature = "wasm-engine")]
        {
            let path = path.as_ref();
            let path_str = path.to_str().ok_or_else(|| url::ParseError::IdnaError)?;
            Url::parse(&format!(path_str))
        }

        #[cfg(not(feature = "wasm-engine"))]
        Url::from_file_path(path)
    }

    fn from_directory_path(path: impl AsRef<Path>) -> Result<Self, url::ParseError> {
        #[cfg(feature = "wasm-engine")]
        {
            let path = path.as_ref();
            let mut path_str = path.to_str().ok_or_else(|| url::ParseError::IdnaError)?.to_string();
            if !path_str.ends_with('/') {
                path_str.push('/');
            }
            Url::parse(&format!(path_str))
        }

        #[cfg(not(feature = "wasm-engine"))]
        Url::from_directory_path(path)
    }
}
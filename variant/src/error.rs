use crate::utils::Type;
use parquet::errors::ParquetError;
use std::array::TryFromSliceError;
use std::str::Utf8Error;
use std::string::FromUtf8Error;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Parsing error: {0}")]
    ParsingError(String),
    #[error("Utf8 parsing error: {0}")]
    Utf8Error(#[from] Utf8Error),
    #[error("Utf8 parsing error: {0}")]
    FromUtf8Error(#[from] FromUtf8Error),
    #[error("Serde parsing error: {0}")]
    SerdeError(#[from] serde_json::Error),
    #[error("Key: {0} does not exist in variant")]
    KeyNotFound(String),
    #[error("Incorrect type called: {0}, expected: {1}")]
    IncorrectType(Type, Type),
    #[error("Bad Byte cast: {0}")]
    BadBytes(#[from] TryFromSliceError),
    #[error("Bad Slice Length: {0} to {1}")]
    BadSlice(usize, usize),
    #[error("Parquet error: {0}")]
    ParquetError(#[from] ParquetError),
    #[error("Invalid Array Index: {0}")]
    InvalidArrayIndex(usize),
}

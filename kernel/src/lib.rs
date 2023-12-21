//! # TableClient interfaces
//!
//! The TableClient interfaces allow connectors to bring their own implementation of functionality
//! such as reading parquet files, listing files in a file system, parsing a JSON string etc.
//!
//! The [`TableClient`] trait exposes methods to get sub-clients which expose the core
//! functionalities customizable by connectors.
//!
//! ## Expression handling
//!
//! Expression handling is done via the [`ExpressionHandler`], which in turn allows the creation
//! of [`ExpressionEvaluator`]s. These evaluators are created for a specific predicate [`Expression`]
//! and allow evaluation of that predicate for a specific batches of data.
//!
//! ## File system interactions
//!
//! Delta Kernel needs to perform some basic operations against file systems like listing and reading files.
//! These interactions are encapsulated in the [`FileSystemClient`] trait. Implementors must take take
//! care that all assumptions on the behavior if the functions - like sorted results - are respected.
//!
//! ## Reading log and data files
//!
//! Delta Kernel requires the capability to read json and parquet files, which is exposed via the
//! [`JsonHandler`] and [`ParquetHandler`] respectively. When reading files, connectors are asked
//! to provide the context information it requires to execute the actual read. This is done by
//! invoking methods on the [`FileHandler`] trait. All specific file handlers must also provide
//! the contextualization APis.
//!

#![warn(
    unreachable_pub,
    trivial_casts,
    trivial_numeric_casts,
    unused_extern_crates,
    rust_2018_idioms,
    rust_2021_compatibility
)]

use std::ops::Range;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use bytes::Bytes;
use url::Url;

use self::schema::{DataType, SchemaRef};

pub mod actions;
pub mod error;
pub mod expressions;
pub mod path;
pub mod scan;
pub mod schema;
pub mod snapshot;
pub mod table;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_util;

pub use actions::{types::*, ActionType};
pub use error::{DeltaResult, Error};
pub use expressions::Expression;
pub use scan::{Scan, ScanBuilder};
pub use table::Table;

#[cfg(feature = "default-client")]
pub mod client;
#[cfg(feature = "default-client")]
pub use client::*;

/// Delta table version is 8 byte unsigned int
pub type Version = u64;

pub type FileSlice = (Url, Option<Range<usize>>);

/// Data read from a Delta table file and the corresponding scan file information.
pub type FileDataReadResult = (FileMeta, RecordBatch);
pub type FileDataReadResultIterator = Box<dyn Iterator<Item = DeltaResult<RecordBatch>> + Send>;

/// The metadata that describes an object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMeta {
    /// The fully qualified path to the object
    pub location: Url,
    /// The last modified time
    pub last_modified: i64,
    /// The size in bytes of the object
    pub size: usize,
}

/// Interface for implementing an Expression evaluator.
///
/// It contains one Expression which can be evaluated on multiple ColumnarBatches.
/// Connectors can implement this interface to optimize the evaluation using the
/// connector specific capabilities.
pub trait ExpressionEvaluator {
    /// Evaluate the expression on given ColumnarBatch data.
    ///
    /// Contains one value for each row of the input.
    /// The data type of the output is same as the type output of the expression this evaluator is using.
    fn evaluate(&self, batch: &RecordBatch) -> DeltaResult<ArrayRef>;
}

/// Provides expression evaluation capability to Delta Kernel.
///
/// Delta Kernel can use this client to evaluate predicate on partition filters,
/// fill up partition column values and any computation on data using Expressions.
pub trait ExpressionHandler {
    /// Create an [`ExpressionEvaluator`] that can evaluate the given [`Expression`]
    /// on columnar batches with the given [`Schema`] to produce data of [`DataType`].
    ///
    /// # Parameters
    ///
    /// - `schema`: Schema of the input data.
    /// - `expression`: Expression to evaluate.
    /// - `output_type`: Expected result data type.
    ///
    /// [`Schema`]: crate::schema::StructType
    /// [`DataType`]: crate::schema::DataType
    fn get_evaluator(
        &self,
        schema: SchemaRef,
        expression: Expression,
        output_type: DataType,
    ) -> Arc<dyn ExpressionEvaluator>;
}

/// Provides file system related functionalities to Delta Kernel.
///
/// Delta Kernel uses this client whenever it needs to access the underlying
/// file system where the Delta table is present. Connector implementation of
/// this interface can hide filesystem specific details from Delta Kernel.
pub trait FileSystemClient: Send + Sync {
    /// List the paths in the same directory that are lexicographically greater or equal to
    /// (UTF-8 sorting) the given `path`. The result should also be sorted by the file name.
    fn list_from(&self, path: &Url)
        -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>>;

    /// Read data specified by the start and end offset from the file.
    fn read_files(
        &self,
        files: Vec<FileSlice>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<Bytes>>>>;
}

/// Provides JSON handling functionality to Delta Kernel.
///
/// Delta Kernel can use this client to parse JSON strings into Row or read content from JSON files.
/// Connectors can leverage this interface to provide their best implementation of the JSON parsing
/// capability to Delta Kernel.
pub trait JsonHandler {
    /// Parse the given json strings and return the fields requested by output schema as columns in a [`RecordBatch`].
    fn parse_json(
        &self,
        json_strings: ArrayRef,
        output_schema: SchemaRef,
    ) -> DeltaResult<RecordBatch>;

    /// Read and parse the JSON format file at given locations and return
    /// the data as a RecordBatch with the columns requested by physical schema.
    ///
    /// # Parameters
    ///
    /// - `files` - File metadata for files to be read.
    /// - `physical_schema` - Select list of columns to read from the JSON file.
    /// - `predicate` - Optional push-down predicate hint (engine is free to ignore it).
    fn read_json_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<Expression>,
    ) -> DeltaResult<FileDataReadResultIterator>;
}

/// Provides Parquet file related functionalities to Delta Kernel.
///
/// Connectors can leverage this interface to provide their own custom
/// implementation of Parquet data file functionalities to Delta Kernel.
pub trait ParquetHandler: Send + Sync {
    /// Read and parse the JSON format file at given locations and return
    /// the data as a RecordBatch with the columns requested by physical schema.
    ///
    /// # Parameters
    ///
    /// - `files` - File metadata for files to be read.
    /// - `physical_schema` - Select list of columns to read from the JSON file.
    /// - `predicate` - Optional push-down predicate hint (engine is free to ignore it).
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<Expression>,
    ) -> DeltaResult<FileDataReadResultIterator>;
}

/// Interface encapsulating all clients needed by the Delta Kernel in order to read the Delta table.
///
/// Connectors are expected to pass an implementation of this interface when reading a Delta table.
pub trait TableClient {
    /// Get the connector provided [`ExpressionHandler`].
    fn get_expression_handler(&self) -> Arc<dyn ExpressionHandler>;

    /// Get the connector provided [`FileSystemClient`]
    fn get_file_system_client(&self) -> Arc<dyn FileSystemClient>;

    /// Get the connector provided [`JsonHandler`].
    fn get_json_handler(&self) -> Arc<dyn JsonHandler>;

    /// Get the connector provided [`ParquetHandler`].
    fn get_parquet_handler(&self) -> Arc<dyn ParquetHandler>;
}

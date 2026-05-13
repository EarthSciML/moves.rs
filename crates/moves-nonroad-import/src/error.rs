//! Error type for the Nonroad input-database importer.
//!
//! The error variants pair an absolute file path with either a 1-based line
//! number (for CSV parse failures) or a column name (for type / validation
//! failures), so the diagnostic identifies the offending cell without forcing
//! the caller to keep a separate lookup table.

use std::path::PathBuf;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error reading {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("{path}:{line}: {message}")]
    Parse {
        path: PathBuf,
        line: usize,
        message: String,
    },

    #[error("{path}: column '{column}' missing from header")]
    MissingColumn { path: PathBuf, column: String },

    #[error(
        "{path}: header has {actual} columns but the {table} importer expects {expected} \
         (extra or reordered columns trip this — re-export the template)"
    )]
    HeaderShape {
        path: PathBuf,
        table: String,
        expected: usize,
        actual: usize,
    },

    #[error("{path}:{line}: row has {actual} cells, header has {expected}")]
    RowWidth {
        path: PathBuf,
        line: usize,
        expected: usize,
        actual: usize,
    },

    #[error("{path}:{line}: duplicate primary key for {table} — key {key} appears more than once")]
    DuplicateKey {
        path: PathBuf,
        line: usize,
        table: String,
        key: String,
    },

    #[error(
        "{path}: {table} allocation invariant — sum of {column} for key {key} is {actual}, \
         expected {expected} within tolerance {tolerance}"
    )]
    AllocationSum {
        path: PathBuf,
        table: String,
        column: String,
        key: String,
        actual: f64,
        expected: f64,
        tolerance: f64,
    },

    #[error("internal: {message}")]
    Internal { message: String },

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("manifest serialisation: {0}")]
    Manifest(#[from] serde_json::Error),
}

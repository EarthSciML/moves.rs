//! Crate-local error type.

use std::path::PathBuf;

use thiserror::Error;

/// Errors returned by the PDB importer.
#[derive(Debug, Error)]
pub enum Error {
    #[error("io error reading {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("csv error in {path} (line {line}): {message}")]
    Csv {
        path: PathBuf,
        line: u64,
        message: String,
    },

    /// A required column is absent from the input file's header row.
    #[error("table '{table}' is missing required column '{column}' (file: {path})")]
    MissingColumn {
        table: String,
        column: String,
        path: PathBuf,
    },

    /// A non-nullable cell was empty. Mirrors Java's "Missing required data" error.
    #[error(
        "table '{table}' line {line}: missing required data for column '{column}' (file: {path})"
    )]
    MissingRequiredCell {
        table: String,
        line: u64,
        column: String,
        path: PathBuf,
    },

    /// A cell could not be parsed into the column's declared type.
    #[error(
        "table '{table}' line {line}: column '{column}' expected {expected}, got '{value}' (file: {path})"
    )]
    Parse {
        table: String,
        line: u64,
        column: String,
        expected: &'static str,
        value: String,
        path: PathBuf,
    },

    /// A cross-row invariant failed (e.g. fractions don't sum to one,
    /// off-network linkID present in linkSourceTypeHour, etc.). Mirrors
    /// Java's `addQualityMessage("ERROR: …")` lines that flip the
    /// importer to NOT_READY.
    #[error("validation error in '{table}': {message}")]
    Validation { table: String, message: String },

    /// Pass-through for arrow-internal errors.
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// Pass-through for parquet-internal errors.
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    /// Pass-through for `serde_json`.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Crate-local result alias.
pub type Result<T> = std::result::Result<T, Error>;

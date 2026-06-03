//! Error types for the importer framework.

use std::path::PathBuf;

/// Crate error.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Filesystem IO failure with an attached path for context.
    #[error("I/O error on {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// CSV / TSV parsing failure raised by the Polars reader.
    #[error("CSV parse error on {path}: {message}")]
    CsvParse { path: PathBuf, message: String },

    /// The CSV header is missing a column the descriptor requires, or
    /// contains a duplicate that cannot be resolved by case folding.
    #[error("missing required column '{column}' in {path}")]
    MissingColumn { path: PathBuf, column: String },

    /// A column failed type coercion (e.g., a string in a numeric
    /// column) before per-row validation could run.
    #[error("type coercion failed for column '{column}' in {path}: {message}")]
    TypeCoercion {
        path: PathBuf,
        column: String,
        message: String,
    },

    /// Polars engine error raised while pulling decode tables from
    /// the default-DB reader during foreign-key validation.
    #[error("polars error: {0}")]
    Polars(#[from] polars::error::PolarsError),

    /// `moves-data-default` reader error (e.g., manifest missing,
    /// schema-only table requested for validation).
    #[error("default-DB reader error: {0}")]
    DefaultDbReader(#[from] moves_data_default::Error),

    /// Parquet writer error.
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    /// Arrow error during type conversion.
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// Validation context did not load a referenced default-DB table.
    #[error("default-DB lookup failed for table '{table}': {message}")]
    DefaultDb { table: String, message: String },
}

/// Crate result alias.
pub type Result<T> = std::result::Result<T, Error>;

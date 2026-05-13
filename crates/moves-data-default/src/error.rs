//! Crate-local error type.

use std::path::PathBuf;

use thiserror::Error;

/// Errors returned by the lazy-loading reader.
#[derive(Debug, Error)]
pub enum Error {
    /// Failure reading a file on disk (manifest, sidecar, or Parquet).
    #[error("io error reading {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// `manifest.json` could not be parsed as JSON.
    #[error("manifest parse error at {path}: {source}")]
    ManifestParse {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },

    /// `manifest.json` was parsed but carries an unexpected `schema_version`.
    #[error("manifest schema_version mismatch at {path}: expected '{expected}', got '{found}'")]
    ManifestVersion {
        path: PathBuf,
        expected: String,
        found: String,
    },

    /// The caller asked for a table the manifest does not list.
    #[error("unknown table '{0}'")]
    UnknownTable(String),

    /// The caller's filter referenced a column that is not one of the
    /// table's partition columns. Filtering on a non-partition column is
    /// not a partition-pruning predicate — it should be expressed against
    /// the returned [`polars::lazy::frame::LazyFrame`] instead.
    #[error(
        "table '{table}' has no partition column named '{column}' (partition columns: {partition_columns:?})"
    )]
    UnknownPartitionColumn {
        table: String,
        column: String,
        partition_columns: Vec<String>,
    },

    /// The caller asked to scan a `schema_only` table. These tables ship
    /// empty in the default DB and are populated by the runtime — there is
    /// no Parquet to read.
    #[error("table '{table}' is schema-only (populated at runtime); use schema_sidecar() instead")]
    SchemaOnly { table: String },

    /// Pass-through for Polars-internal errors (file open, scan, collect).
    #[error("polars error: {0}")]
    Polars(#[from] polars::error::PolarsError),
}

/// Crate-local result alias.
pub type Result<T> = std::result::Result<T, Error>;

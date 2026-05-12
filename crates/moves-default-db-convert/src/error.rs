//! Error types for the default-DB conversion pipeline.

use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io: {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("json: {path}: {source}")]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },

    #[error("parse: {path}:{line}: {message}")]
    Parse {
        path: PathBuf,
        line: usize,
        message: String,
    },

    #[error("row width mismatch in {path}: expected {expected} cols, got {actual}")]
    RowWidthMismatch {
        path: PathBuf,
        expected: usize,
        actual: usize,
    },

    #[error("plan: {0}")]
    Plan(String),

    #[error("partition: table '{table}' has strategy '{strategy}' but no usable partition column in PK {pk:?}")]
    NoPartitionColumn {
        table: String,
        strategy: String,
        pk: Vec<String>,
    },

    #[error(
        "row count mismatch for {table} partition {partition}: expected {expected}, wrote {actual}"
    )]
    RowCountMismatch {
        table: String,
        partition: String,
        expected: u64,
        actual: u64,
    },

    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
}

pub type Result<T> = std::result::Result<T, Error>;

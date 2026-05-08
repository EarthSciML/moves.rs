use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("json error at {path}: {source}")]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },

    #[error("table {table:?}: natural-key column {column:?} not in schema")]
    NaturalKeyColumnMissing { table: String, column: String },

    #[error(
        "table {table:?}: column {column:?} has unsupported arrow type {dtype}; \
         supported types are Int64, Float64, Utf8, Boolean"
    )]
    UnsupportedColumnType {
        table: String,
        column: String,
        dtype: String,
    },

    #[error(
        "table {table:?}: column {column:?} declared as {declared} but has actual type {actual}"
    )]
    ColumnTypeMismatch {
        table: String,
        column: String,
        declared: String,
        actual: String,
    },

    #[error("table {table:?}: row {row} has {actual} columns but schema has {expected}")]
    RowWidthMismatch {
        table: String,
        row: usize,
        expected: usize,
        actual: usize,
    },

    #[error("table {table:?}: schema declares column {column:?} more than once")]
    DuplicateColumn { table: String, column: String },

    #[error("snapshot has duplicate table {table:?}")]
    DuplicateTable { table: String },

    #[error("snapshot at {path}: manifest is missing")]
    ManifestMissing { path: PathBuf },

    #[error("snapshot at {path}: format_version {actual:?} not supported (expected {expected:?})")]
    UnsupportedFormatVersion {
        path: PathBuf,
        actual: String,
        expected: String,
    },

    #[error(
        "snapshot at {path}: aggregate hash mismatch (manifest: {manifest_hash}, computed: {computed_hash})"
    )]
    AggregateHashMismatch {
        path: PathBuf,
        manifest_hash: String,
        computed_hash: String,
    },

    #[error(
        "snapshot at {path}: table {table:?} content hash mismatch (manifest: {manifest_hash}, computed: {computed_hash})"
    )]
    ContentHashMismatch {
        path: PathBuf,
        table: String,
        manifest_hash: String,
        computed_hash: String,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

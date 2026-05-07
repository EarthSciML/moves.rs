//! Error types for fixture capture.

use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("parse error at {path}:{line}: {message}")]
    Parse {
        path: PathBuf,
        line: usize,
        message: String,
    },

    #[error("RunSpec at {path} is missing required element <{element}>")]
    RunSpecMissing { path: PathBuf, element: String },

    #[error("RunSpec at {path}: {message}")]
    RunSpecInvalid { path: PathBuf, message: String },

    #[error("XML error at {path}: {source}")]
    Xml {
        path: PathBuf,
        #[source]
        source: quick_xml::Error,
    },

    #[error("snapshot error: {0}")]
    Snapshot(#[from] moves_snapshot::Error),

    #[error("json error at {path}: {source}")]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },

    #[error("captures directory at {path} is missing required subdirectory '{subdir}'")]
    CapturesMissing { path: PathBuf, subdir: String },

    #[error("schema sidecar at {path} declares column '{column}' but the row data has only {actual} columns")]
    SchemaWidthMismatch {
        path: PathBuf,
        column: String,
        actual: usize,
    },

    #[error("schema sidecar at {path} declares {expected} columns but the row data has {actual}")]
    RowWidthMismatch {
        path: PathBuf,
        expected: usize,
        actual: usize,
    },

    #[error("table name '{name}' collides — captures must produce unique table names; conflict from {path}")]
    DuplicateTableName { name: String, path: PathBuf },
}

pub type Result<T> = std::result::Result<T, Error>;

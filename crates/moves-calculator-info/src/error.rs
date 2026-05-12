//! Error types for `moves-calculator-info`.

use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("{path}:{line}: malformed directive: {message}")]
    Directive {
        path: PathBuf,
        line: usize,
        message: String,
    },

    #[error("{path}:{line}: unknown granularity '{value}'")]
    UnknownGranularity {
        path: PathBuf,
        line: usize,
        value: String,
    },

    #[error("{path}:{line}: unknown priority '{value}'")]
    UnknownPriority {
        path: PathBuf,
        line: usize,
        value: String,
    },

    #[error("dag construction error: {0}")]
    DagBuild(String),

    #[error("json error at {path}: {source}")]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

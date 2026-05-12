//! Error types for `moves-sql-macros`.

use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("config error at {path}: {source}")]
    Config {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },

    #[error("data set '{id}' row {row}: width {width} does not match column count {columns}")]
    RowWidthMismatch {
        id: String,
        row: usize,
        width: usize,
        columns: usize,
    },

    #[error("data set '{id}': at least one column name is required")]
    EmptyColumns { id: String },
}

pub type Result<T> = std::result::Result<T, Error>;

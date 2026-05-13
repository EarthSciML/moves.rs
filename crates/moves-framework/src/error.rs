//! Crate-local error type.

use std::path::PathBuf;

use thiserror::Error;

/// Crate result alias.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors surfaced by `moves-framework`. Phase 2 starts narrow; per-task
/// variants (RunSpec loading, location iteration, calculator dispatch) are
/// added as the framework crate fills out.
#[derive(Debug, Error)]
pub enum Error {
    /// Returned by trait impls whose body hasn't been ported yet.
    #[error("master loopable not yet implemented")]
    NotImplemented,

    /// I/O failure while writing output files. The path identifies the
    /// target (parquet file, partition directory, temporary sibling).
    #[error("output i/o error at {path}: {source}")]
    Io {
        /// Path the writer was operating on.
        path: PathBuf,
        /// Underlying OS error.
        #[source]
        source: std::io::Error,
    },

    /// Arrow record-batch construction failure — almost always indicates
    /// a mismatch between the declared schema and the columns the writer
    /// built.
    #[error("arrow: {0}")]
    Arrow(#[source] arrow::error::ArrowError),

    /// Parquet encoder failure.
    #[error("parquet: {0}")]
    Parquet(#[source] parquet::errors::ParquetError),

    /// The writer encountered a column name not in the static output
    /// schema — should be unreachable in correct code, but a diagnosable
    /// failure mode is better than a panic if a schema constant drifts
    /// out of sync with the record-batch builder.
    #[error("output schema mismatch: unknown column '{0}'")]
    OutputSchemaMismatch(String),
}

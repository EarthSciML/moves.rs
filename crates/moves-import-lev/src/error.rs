//! Crate-local error type.

use std::path::PathBuf;

use thiserror::Error;

/// Errors returned by the LEV/NLEV importer.
#[derive(Debug, Error)]
pub enum Error {
    /// Filesystem I/O failed.
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// The input CSV header is missing one of the columns marked
    /// `required = true` in [`crate::schema::COLUMNS`].
    #[error("input CSV at {path} is missing required column '{column}'")]
    MissingRequiredColumn { path: PathBuf, column: String },

    /// The input CSV header contains a column name that is not part of
    /// the LEV/NLEV schema.
    #[error("input CSV at {path} has unknown column '{column}'")]
    UnknownColumn { path: PathBuf, column: String },

    /// The input CSV header repeats a column name.
    #[error("input CSV at {path} has duplicate column '{column}'")]
    DuplicateColumn { path: PathBuf, column: String },

    /// A data row has a different number of fields than the header.
    #[error(
        "input CSV at {path}, line {line}: row has {actual} fields but header declared {expected}"
    )]
    RowWidthMismatch {
        path: PathBuf,
        line: usize,
        expected: usize,
        actual: usize,
    },

    /// A cell could not be parsed as the column's declared type.
    #[error(
        "input CSV at {path}, line {line}, column '{column}': could not parse {value:?} as {expected_type}"
    )]
    ParseCell {
        path: PathBuf,
        line: usize,
        column: String,
        expected_type: &'static str,
        value: String,
    },

    /// A required column has a blank or `NULL` cell.
    #[error("input CSV at {path}, line {line}: required column '{column}' is empty")]
    MissingRequiredValue {
        path: PathBuf,
        line: usize,
        column: String,
    },

    /// A rate column has a negative value. Emission rates and their CVs
    /// are non-negative by definition.
    #[error("input CSV at {path}, line {line}, column '{column}': rate is negative ({value})")]
    NegativeRate {
        path: PathBuf,
        line: usize,
        column: String,
        value: f64,
    },

    /// A rate column has a non-finite (`NaN` / `±Inf`) value.
    #[error("input CSV at {path}, line {line}, column '{column}': rate is not finite ({value})")]
    NonFiniteRate {
        path: PathBuf,
        line: usize,
        column: String,
        value: f64,
    },

    /// Two rows share the same primary key. The default-DB declares
    /// `(sourceBinID, polProcessID, opModeID, ageGroupID)` unique;
    /// importing duplicates would silently overwrite on insert.
    #[error(
        "input CSV at {path}: duplicate primary key (sourceBinID={source_bin_id}, polProcessID={pol_process_id}, opModeID={op_mode_id}, ageGroupID={age_group_id}) appears at lines {first} and {second}"
    )]
    DuplicatePrimaryKey {
        path: PathBuf,
        source_bin_id: i64,
        pol_process_id: i64,
        op_mode_id: i64,
        age_group_id: i64,
        first: usize,
        second: usize,
    },

    /// Arrow/Parquet encoding produced an error.
    #[error("parquet encode error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    /// Arrow encoding produced an error.
    #[error("arrow encode error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}

/// Crate-local result alias.
pub type Result<T> = std::result::Result<T, Error>;

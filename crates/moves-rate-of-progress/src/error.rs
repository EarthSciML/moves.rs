//! Error type for `moves-rate-of-progress`.

use std::path::PathBuf;

use thiserror::Error;

/// Errors returned by the I/O and construction paths.
#[derive(Debug, Error)]
pub enum Error {
    /// File I/O problem (open, read, write).
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// The input CSV file is missing a required header column or has
    /// an unrecognised column set.
    #[error("CSV at {path} has malformed header (got {got:?}, want a permutation of {want:?})")]
    BadCsvHeader {
        path: PathBuf,
        got: Vec<String>,
        want: Vec<&'static str>,
    },

    /// CSV parse failure (missing field, unparseable number, ragged row).
    #[error("CSV at {path} line {line}: {message}")]
    CsvParse {
        path: PathBuf,
        line: u64,
        message: String,
    },

    /// A `reductionFraction` value is outside the valid range [0.0, 1.0].
    #[error(
        "ROP row (pollutant={pollutant_id}, sourceType={source_type_id}, \
        regClass={reg_class_id}, modelYear={model_year_id}) has \
        reductionFraction={reduction_fraction} outside [0.0, 1.0]"
    )]
    InvalidReductionFraction {
        pollutant_id: i32,
        source_type_id: i32,
        reg_class_id: i32,
        model_year_id: i32,
        reduction_fraction: f64,
    },
}

impl Error {
    pub(crate) fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Error::Io {
            path: path.into(),
            source,
        }
    }
}

/// Crate-local result alias.
pub type Result<T> = std::result::Result<T, Error>;

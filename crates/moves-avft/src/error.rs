//! Error type for `moves-avft`.

use std::path::PathBuf;

use thiserror::Error;

use crate::model::{ModelYearId, SourceTypeId};

/// Errors returned by the importer, tool, and I/O paths.
#[derive(Debug, Error)]
pub enum Error {
    /// File I/O problem (open, read, write).
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// The input CSV file is missing one of the required header columns,
    /// or the columns are not the canonical AVFT names.
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

    /// TOML parse failure for the tool spec.
    #[error("TOML spec parse error at {path}: {source}")]
    TomlParse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },

    /// Negative `fuelEngFraction` cell — rejected by the Java importer's
    /// validator.
    #[error(
        "AVFT row (sourceType={source_type_id}, modelYear={model_year_id}, fuel={fuel_type_id}, eng={eng_tech_id}) has negative fuelEngFraction={fuel_eng_fraction}"
    )]
    NegativeFraction {
        source_type_id: i64,
        model_year_id: i64,
        fuel_type_id: i64,
        eng_tech_id: i64,
        fuel_eng_fraction: f64,
    },

    /// The sum of `fuelEngFraction` for a (sourceType, modelYear) group
    /// exceeds 1.0 (with the same 4-decimal-place rounding the SQL check
    /// uses).
    #[error(
        "AVFT (sourceType={source_type_id}, modelYear={model_year_id}) fuelEngFraction sums to {sum} > 1.0"
    )]
    FractionSumExceedsOne {
        source_type_id: i64,
        model_year_id: i64,
        sum: f64,
    },

    /// The TOML tool spec failed internal validation — e.g.
    /// `analysis_year` precedes `last_complete_model_year`,
    /// `last_complete_model_year` predates the AVFT default DB's 1950
    /// floor, or a `sourceTypeID` appears twice in the method list.
    /// Raised by [`ToolSpec::validate`](crate::spec::ToolSpec::validate).
    #[error("tool spec error: {0}")]
    ToolSpec(String),

    /// Tool reported an error condition that prevents producing a valid
    /// output (e.g., a source type's distribution does not sum to 1
    /// after gap-filling, or the known-fractions input is missing
    /// entries the projection requires).
    #[error(
        "AVFT tool error: {message} (sourceType={source_type_id}, modelYear={model_year_id:?})"
    )]
    ToolFailure {
        source_type_id: SourceTypeId,
        model_year_id: Option<ModelYearId>,
        message: String,
    },

    /// Arrow / Parquet write failure.
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    /// Arrow schema / batch failure.
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
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

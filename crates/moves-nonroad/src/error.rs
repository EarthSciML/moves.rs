//! Error type for `moves-nonroad`.
//!
//! Per the error-handling policy in `ARCHITECTURE.md` (§ 4.2),
//! Fortran's integer return-code convention becomes Rust
//! [`Result`]; the [`Error`] enum carries source-location and
//! input-context information sufficient to identify the offending
//! file, line, or computation.
//!
//! The skeleton declares the four variants needed by the plumbing:
//! [`Error::Io`] for I/O failures with a path; [`Error::Parse`] for
//! input-record-level parse failures (path + 1-based line number);
//! [`Error::Config`] for option-file or invariant violations; and
//! [`Error::NonFinite`] for finite-value invariants violated during
//! emissions computation. Subsequent porting tasks add domain-specific
//! variants as they encounter fault modes that don't fit the existing
//! surface; new variants must continue to encode enough context to
//! identify the source of the fault.

use std::path::PathBuf;

/// Errors produced by `moves-nonroad` routines.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// I/O failure with the path that triggered it.
    #[error("io error at {path}: {source}")]
    Io {
        /// Path being read or written when the I/O error occurred.
        path: PathBuf,
        /// Underlying [`std::io::Error`].
        #[source]
        source: std::io::Error,
    },

    /// An input record could not be parsed.
    ///
    /// `line` is 1-based and refers to the input file's line number.
    #[error("parse error in {file:?} at line {line}: {message}")]
    Parse {
        /// Input file containing the bad record.
        file: PathBuf,
        /// 1-based line number of the bad record.
        line: usize,
        /// Human-readable description of what went wrong.
        message: String,
    },

    /// A configured input value violated a domain invariant.
    ///
    /// Used for `.opt`-file values and cross-input consistency
    /// checks that are not localized to a single record.
    #[error("invalid configuration: {0}")]
    Config(String),

    /// A computation produced a non-finite value where a finite was
    /// required (NaN or infinity in an emissions accumulator).
    ///
    /// Routines that detect non-finite values mid-computation surface
    /// them via this variant rather than propagating silently. See
    /// the numerical-fidelity discussion in Tasks 104, 115, and 116.
    #[error("non-finite value in {context}: {value}")]
    NonFinite {
        /// What was being computed when the bad value appeared.
        context: String,
        /// The non-finite [`f64`] that violated the invariant.
        value: f64,
    },

    /// A spatial-allocation indicator value was not found for the
    /// requested geography. Mirrors the `IEOF` return from `getind.f`
    /// that the allocation routines (`alocty.f`, `alosta.f`,
    /// `alosub.f`) treat as fatal (`7002` error path).
    #[error(
        "no spatial-indicator data for code={code:?} fips={fips:?} \
         subcounty={subcounty:?} year={year}"
    )]
    IndicatorMissing {
        /// 3-character allocation indicator code (e.g. `"POP"`).
        code: String,
        /// 5-character FIPS code being looked up.
        fips: String,
        /// Subcounty identifier (empty string for state- and
        /// national-level lookups).
        subcounty: String,
        /// Evaluation year passed to the lookup.
        year: i32,
    },

    /// A retrofit record specified an absolute number of units
    /// retrofitted (`annual_frac_or_count > 1.0`) that exceeds the
    /// engine population available for the current model iteration.
    /// Mirrors the `7000` error path in `clcrtrft.f` (:171, :273).
    #[error(
        "retrofit {retrofit_id} ({pollutant}) requests {n_units_requested} units \
         but only {n_units_existing} engines exist \
         (scc={scc:?} hp_avg={hp_avg} model_year={model_year} tech_type={tech_type:?})"
    )]
    RetrofitNUnitsExceedPopulation {
        /// Retrofit ID from `rtrftid`.
        retrofit_id: i32,
        /// Pollutant whose accumulator triggered the check.
        pollutant: String,
        /// 10-character SCC of the iteration that hit the error.
        scc: String,
        /// HP-average of the iteration.
        hp_avg: f32,
        /// Model year of the iteration.
        model_year: i32,
        /// Tech type of the iteration.
        tech_type: String,
        /// Units requested (the offending product `frac * pop`).
        n_units_requested: f32,
        /// Engines available for this iteration (`pop`).
        n_units_existing: f32,
    },
}

/// Crate-local [`Result`] alias.
pub type Result<T> = std::result::Result<T, Error>;

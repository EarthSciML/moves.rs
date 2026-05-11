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

    /// A spatial-allocation indicator value was required but not
    /// present in the loaded `.IND` records.
    ///
    /// Mirrors the `IEOF` branch of `getind.f` (NONROAD source),
    /// which the allocation routines (`alocty.f`, `alosta.f`,
    /// `alosub.f`) surface as a fatal "Could not find any spatial
    /// indicator data" message.
    #[error(
        "spatial-allocation indicator {code:?} missing for FIPS {fips:?}, \
         subregion {subregion:?}, year {year}"
    )]
    IndicatorMissing {
        /// 3-character indicator code (e.g. `POP`, `HHS`).
        code: String,
        /// 5-character FIPS code being looked up.
        fips: String,
        /// 5-character subregion code (blanks for state/county-level
        /// lookups).
        subregion: String,
        /// Episode year supplied to the lookup.
        year: i32,
    },
}

/// Crate-local [`Result`] alias.
pub type Result<T> = std::result::Result<T, Error>;

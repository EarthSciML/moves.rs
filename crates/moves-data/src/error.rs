//! Crate-local error type.

use std::num::ParseIntError;

use thiserror::Error;

/// Crate result alias.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors surfaced by `moves-data`. Lookup misses are represented by
/// `Option::None`, not errors — only operations with a meaningful failure
/// mode (parsing, illegal id arithmetic) return `Error`.
#[derive(Debug, Error)]
pub enum Error {
    /// A string could not be parsed as the expected integer id.
    #[error("could not parse {kind} id from {input:?}: {source}")]
    ParseId {
        /// What kind of id was being parsed (e.g., `"pollutant"`).
        kind: &'static str,
        /// The input string.
        input: String,
        /// Underlying integer-parse error.
        #[source]
        source: ParseIntError,
    },
}

//! Crate error type for `moves-calculators`.
//!
//! Follows the workspace convention (see `CONTRIBUTING.md`): a crate-local
//! `thiserror` enum re-exported at the crate root alongside a `Result` alias.

use thiserror::Error;

/// Errors produced by the `moves-calculators` generators and calculators.
#[derive(Debug, Error)]
pub enum Error {
    /// A generator was handed a malformed external-parameter list.
    ///
    /// Ports the failure paths of the Go `readExternalFlags` function: too
    /// few CSV parameters, or a trailing identifier that does not parse as
    /// an integer.
    #[error("base rate generator parameters: {0}")]
    Parameters(String),
}

/// Convenience alias for results carrying the crate [`enum@Error`].
pub type Result<T> = std::result::Result<T, Error>;

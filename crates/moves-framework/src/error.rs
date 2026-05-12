//! Crate-local error type.

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
}

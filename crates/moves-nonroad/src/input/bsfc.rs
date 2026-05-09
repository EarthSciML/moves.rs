//! BSFC (brake-specific fuel consumption) parser stub (`rdbsfc.f`).
//!
//! Task 97. The Fortran `rdbsfc.f` opens the BSFC file and dispatches
//! to `rdemfc` (the generic emission-factor reader), since BSFC files
//! share the `.EMF` format. The Rust port honours that delegation:
//!
//! - [`BsfcSource`] records where the BSFC bundle should be loaded
//!   from (path / reader).
//! - The actual record-level parser lives in `super::emfc` (Task 96)
//!   when wired up. Until that module ships, [`load`] returns a
//!   [`Error::Config`] with a clear "not yet implemented" message,
//!   so that callers receive a deterministic signal rather than
//!   silently falling through.
//!
//! # Fortran source
//!
//! Ports `rdbsfc.f` (109 lines).

use crate::{Error, Result};
use std::path::PathBuf;

/// Where to find the BSFC bundle.
#[derive(Debug, Clone)]
pub struct BsfcSource {
    /// Path on disk.
    pub path: PathBuf,
}

/// Placeholder loader. Will dispatch to the EMF parser (Task 96)
/// once that lands. Returns `Error::Config` until then so the
/// integration path produces a deterministic error rather than a
/// silent success.
pub fn load(source: &BsfcSource) -> Result<()> {
    Err(Error::Config(format!(
        "BSFC parser not yet wired to /EMF/ reader (Task 96); requested {:?}",
        source.path
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_config_error_until_emfc_lands() {
        let src = BsfcSource {
            path: PathBuf::from("foo.bsf"),
        };
        let err = load(&src).unwrap_err();
        match err {
            Error::Config(msg) => assert!(msg.contains("Task 96")),
            other => panic!("unexpected: {other:?}"),
        }
    }
}

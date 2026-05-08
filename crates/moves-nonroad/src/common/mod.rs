//! Replacement for the 65 Fortran COMMON blocks that hold NONROAD's
//! global state.
//!
//! The Fortran source uses 11 include files (`*.inc`) declaring 65
//! named COMMON blocks; these are imported into routines and act as
//! shared mutable state. The Rust port replaces them with typed
//! sub-structs grouped by purpose, owned by a top-level
//! [`NonroadContext`] passed explicitly between modules.
//!
//! Task 92 owns the design that splits state across the typed
//! sub-structs (one per include file is the starting heuristic);
//! Task 93 ports the parameter and chemistry constants from
//! `nonrdprm.inc` into [`consts`]. Until those tasks land,
//! [`NonroadContext`] is an empty placeholder so that downstream
//! signatures (`fn ...(ctx: &NonroadContext, ...)`) can be sketched
//! without a structural rewrite when the real fields land.

pub mod consts;

/// Top-level container for all NONROAD execution state.
///
/// Replaces the implicit-global-via-COMMON pattern in the Fortran
/// source. Concrete fields are added by Task 92 (COMMON-block
/// replacement design) once the include-file audit is complete.
///
/// # Construction
///
/// Until Task 92 lands, the only constructor is [`NonroadContext::new`],
/// which produces an empty context. Real construction will move into
/// `input::nropt` (Task 99) once option-file parsing is in place.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct NonroadContext {
    // Fields added by Task 92 (one sub-struct per include file is
    // the starting heuristic; see ARCHITECTURE.md § 6).
}

impl NonroadContext {
    /// Create an empty execution context.
    ///
    /// Once Task 92 populates the real fields, this constructor stays
    /// available for unit tests that build a "just enough" context;
    /// the production path (Task 99) will use a builder constructed
    /// from a parsed `.opt` file plus the loaded input bundle.
    pub fn new() -> Self {
        Self::default()
    }
}

//! Calculator integration-validation harness — Task 73 (`mo-fvuf`).
//!
//! The bead: *run every Phase 0 fixture through the Rust calculators
//! (Tasks 45–72) and diff each calculator's output against the
//! canonical-MOVES captures from Phase 0, within explicit tolerance
//! budgets.* This module tree is that harness, split into four concerns:
//!
//! | Module          | Concern                                                  |
//! |-----------------|----------------------------------------------------------|
//! | [`fixtures`]    | The 23 Phase 0 onroad RunSpec fixtures                   |
//! | [`calculators`] | The 38 Phase 3 calculator implementations (Tasks 45–72)  |
//! | [`coverage`]    | The fixture × calculator coverage matrix                 |
//! | [`compare`]     | Diff produced tables against canonical snapshots         |
//!
//! The harness tests themselves live in the sibling integration test
//! `tests/calculator_integration.rs`.
//!
//! # Scope: onroad hot-path calculators
//!
//! Tasks 45–72 port the MOVES onroad emission calculators for the
//! hot-path processes. Phase 0 ships 33 fixtures; the ten `nr-*.xml`
//! NONROAD fixtures drive a separate calculation path and are covered by
//! the Task 115 NONROAD gate (`characterization/nonroad-fidelity/`).
//! This harness scopes to the **23 onroad fixtures**.
//!
//! # What runs today, and what is gated
//!
//! Three things the harness does *now*, on every `cargo test`:
//!
//! 1. **Validates its own machinery** — the fixture-catalogue parse,
//!    the calculator catalogue, the coverage-matrix derivation, and
//!    the snapshot-diff engine are exercised end to end on the real
//!    fixtures and on synthetic snapshots (the co-located tests).
//! 2. **Pins the catalogues** — 23 onroad fixtures present and
//!    well-formed, 38 calculators registered with stable names.
//! 3. **Fixes the coverage matrix** — every onroad fixture is reached
//!    by at least one calculator; the matrix shape is asserted.
//!
//! One thing is **gated** behind infrastructure the repository does
//! not hold yet: the end-to-end *canonical-capture diff*. It needs
//! two inputs —
//!
//! - the canonical-MOVES captures, one `moves_snapshot`-format
//!   snapshot per fixture under `characterization/snapshots/<fixture>/`.
//!   Phase 0 ships the fixture RunSpecs but the snapshots are "pending
//!   compute-node run" (`characterization/fixtures/README.md`); and
//! - the Rust calculators' own per-fixture output. Every calculator's
//!   `Calculator::execute` returns `CalculatorOutput::empty()` today
//!   because `CalculatorContext` exposes no row storage until the
//!   data plane lands. The calculators' *numeric compute cores* are
//!   complete and callable — the per-calculator unit tests exercise
//!   them — but the materialisation that feeds them per-fixture inputs
//!   and collects their output rows does not exist.
//!
//! When a snapshot directory is supplied (default [`snapshots_root`],
//! overridable via [`SNAPSHOTS_DIR_ENV`]), the harness loads and
//! structurally validates it. The actual canonical-vs-port diff
//! activates with no further harness change once the data plane lands
//! the calculator-output side — [`compare::compare_table`] is the
//! diff contract that wiring builds to.
//!
//! This split mirrors the Task 44 generator-validation gate: Task 73
//! builds the *gate*; Phase 0's compute-node run and the data plane
//! supply its two inputs.

use std::path::PathBuf;

pub mod calculators;
pub mod compare;
pub mod coverage;
pub mod fixtures;

/// Environment variable naming the directory of canonical-MOVES
/// snapshots — one `moves_snapshot`-format sub-directory per fixture.
///
/// Unset (the common case) the harness falls back to the in-repo
/// [`snapshots_root`]. Set it to point the gate at a fresh capture
/// run produced off-repo.
pub const SNAPSHOTS_DIR_ENV: &str = "CALCULATOR_VALIDATION_SNAPSHOTS";

/// The repository root, derived from the crate's manifest directory.
///
/// `CARGO_MANIFEST_DIR` is `<repo>/crates/moves-calculators`; its
/// grandparent is the repository root.
pub fn repo_root() -> PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crate manifest dir has a repo-root grandparent")
        .to_path_buf()
}

/// The directory holding the canonical-MOVES snapshots.
///
/// [`SNAPSHOTS_DIR_ENV`] overrides it when set to a non-empty value;
/// otherwise it is the in-repo `characterization/snapshots/`.
pub fn snapshots_root() -> PathBuf {
    std::env::var_os(SNAPSHOTS_DIR_ENV)
        .map(PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| repo_root().join("characterization").join("snapshots"))
}

/// The tolerance-budget config — the per-(table, column) absolute
/// tolerances the canonical-capture diff applies.
///
/// Lives in-repo so the budget is version-controlled alongside the
/// harness; [`compare::tolerance_options`] reads it.
pub fn tolerance_config_path() -> PathBuf {
    repo_root()
        .join("characterization")
        .join("calculator-validation")
        .join("tolerance.toml")
}

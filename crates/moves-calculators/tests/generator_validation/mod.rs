//! Generator integration-validation harness — ().
//!
//! The work item: *run every fixture through the Rust generators
//! with calculators still stubbed, and diff each
//! generator's output against the canonical-MOVES intermediate
//! captures from within explicit tolerance budgets.* This
//! module tree is that harness, split into five concerns:
//!
//! | Module | Concern |
//! |----------------|--------------------------------------------------------|
//! | [`fixtures`] | The 23 onroad RunSpec fixtures |
//! | [`generators`] | The 16 generator implementations |
//! | [`coverage`] | The fixture × generator coverage matrix |
//! | [`adapter`] | Generator compute-core output → `moves_snapshot` table |
//! | [`compare`] | Diff produced tables against canonical snapshots |
//!
//! The harness tests themselves live in the sibling integration test
//! `tests/generator_integration.rs`.
//!
//! # Scope: onroad only
//!
//! The generators ported in are MOVES *onroad*
//! generators. The catalogue ships 33 fixtures; the ten
//! `nr-*.xml` NONROAD fixtures drive a separate calculation path
//! (the `moves-nonroad` Fortran port) and never instantiate these
//! generators — they are covered by the NONROAD
//! numerical-fidelity gate (`characterization/nonroad-fidelity/`).
//! This harness therefore scopes to the **23 onroad fixtures**.
//!
//! # What runs today, and what is gated
//!
//! Four things the harness does *now*, on every `cargo test`:
//!
//! 1. **Validates its own machinery** — the fixture catalogue parse,
//! the generator catalogue, the coverage-matrix derivation, and
//! the snapshot-diff engine are exercised end to end on the real
//! fixtures and on synthetic snapshots (the co-located tests).
//! 2. **Exercises the live port** — it calls the real
//! `MeteorologyGenerator` numeric compute core, routes its output
//! through [`adapter`] into a `moves_snapshot` table, and confirms
//! the comparison machinery composes with genuine port output.
//! 3. **Pins the catalogues** — 23 onroad fixtures present and
//! well-formed, 16 generators registered with stable names.
//! 4. **Fixes the tolerance budget** — including the one *expected*
//! divergence the generator port already documents (the
//! `MeteorologyGenerator` `5/9` artifact — see
//! `characterization/generator-validation/tolerance.toml`).
//!
//! One thing is **gated** behind infrastructure the repository does
//! not hold yet: the end-to-end *canonical-capture diff*. It needs
//! two inputs//!
//! - the canonical-MOVES intermediate captures, one
//! `moves_snapshot`-format snapshot per fixture under
//! `characterization/snapshots/<fixture>/`. ships the
//! fixture RunSpecs but the snapshots are "pending compute-node
//! run" (`characterization/fixtures/README.md`); and
//! - the Rust generators' own per-fixture output. Every generator's
//! `Generator::execute` returns `CalculatorOutput::empty()` today
//! because `CalculatorContext` exposes no row storage until the
//! data plane (`DataFrameStore`) lands. The generators'
//! *numeric compute cores* are complete and callable — that is
//! what [`adapter`] and the live-port test exercise — but the
//! materialisation that feeds them per-fixture inputs and collects
//! their output rows does not exist.
//!
//! When a snapshot directory is supplied (default
//! [`snapshots_root`], overridable via [`SNAPSHOTS_DIR_ENV`]), the
//! harness loads and structurally validates it. The actual
//! canonical-vs-port diff activates with no further harness change
//! once the data plane lands the generator-output side//! [`adapter`] is the contract that wiring builds to, and
//! [`compare::compare_table`] is the diff.
//!
//! This split is deliberate and mirrors the NONROAD
//! fidelity gate: builds the *gate*;'s compute-node
//! run and data plane supply its two inputs.

use std::path::PathBuf;

pub mod adapter;
pub mod compare;
pub mod coverage;
pub mod fixtures;
pub mod generators;

/// Environment variable naming the directory of canonical-MOVES
/// snapshots — one `moves_snapshot`-format sub-directory per fixture.
///
/// Unset (the common case) the harness falls back to the in-repo
/// [`snapshots_root`]. Set it to point the gate at a fresh capture
/// run produced off-repo (`characterization/snapshots/README.md`
/// § "Producing the full fixture suite").
pub const SNAPSHOTS_DIR_ENV: &str = "GENERATOR_VALIDATION_SNAPSHOTS";

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
        .join("generator-validation")
        .join("tolerance.toml")
}

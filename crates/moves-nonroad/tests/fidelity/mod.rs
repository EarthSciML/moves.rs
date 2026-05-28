//! NONROAD numerical-fidelity validation harness — Task 115
//! (`mo-065ko`).
//!
//! The bead: *run every Phase 0 NONROAD fixture through the Rust
//! port and diff the result against the locally-fixed gfortran
//! NONROAD reference, within a fixed tolerance budget.* This module
//! tree is that harness, split into five concerns:
//!
//! | Module          | Concern                                            |
//! |-----------------|----------------------------------------------------|
//! | [`reference`]   | Parse the `dbgemit` capture TSV into records       |
//! | [`tolerance`]   | The `1e-9` / `1e-12` / exact tolerance policy      |
//! | [`divergence`]  | Diff reference vs. port output → a triage report   |
//! | [`fixtures`]    | The ten Phase 0 `nr-*.xml` fixtures                |
//! | [`adapter`]     | `moves-nonroad` output types → reference records   |
//!
//! The harness tests themselves live in the sibling integration
//! test `tests/nonroad_fidelity.rs`.
//!
//! # What runs today
//!
//! Four things the harness does on every `cargo test`:
//!
//! 1. **Validates its own machinery** — the parser, the tolerance
//!    rules, and the divergence engine are exercised end to end on
//!    synthetic captures (the co-located unit tests in each module).
//! 2. **Exercises the live port** — it calls real `moves-nonroad`
//!    functions (`age_distribution`, `growth_factor`), routes their
//!    output through [`adapter`] and [`divergence`], and confirms
//!    the machinery composes with genuine port output.
//! 3. **Pins the fixture catalogue** — it confirms all ten Phase 0
//!    NONROAD fixtures are present and well-formed.
//! 4. **Runs the end-to-end diff** (when `NONROAD_FIDELITY_REFERENCE`
//!    is set) — loads and structurally validates the gfortran baseline
//!    corpus, then runs each fixture through
//!    `run_simulation` with
//!    `ProductionExecutor` wrapped in [`adapter::InstrumentingExecutor`]
//!    to capture the port-side records, and reports a
//!    [`divergence::DivergenceReport`] per fixture.
//!
//! Note: until fixture-data file loaders (NR*.ACT, NR*.GRW, …) are
//! ported, the port-side capture runs over an empty input bundle, so
//! the diff shows all reference records "missing from port" — the
//! report is printed but not asserted.  The full numerical comparison
//! activates as loaders land and `NonroadInputs` is populated.
//!
//! Task 116 (`mo-490cm`) consumes the [`divergence::DivergenceReport`]
//! to triage divergences.

use std::path::{Path, PathBuf};

pub mod adapter;
pub mod divergence;
pub mod fixtures;
pub mod reference;
pub mod tolerance;

/// Environment variable naming a directory of captured gfortran
/// `dbgemit` baselines — one `<fixture>.tsv` per Phase 0 NONROAD
/// fixture (see [`fixtures::NonroadFixture::reference_filename`]).
///
/// The name mirrors the `NRDBG_FILE` convention the instrumentation
/// itself uses (`characterization/nonroad-build/README.md`).
pub const REFERENCE_DIR_ENV: &str = "NONROAD_FIDELITY_REFERENCE";

/// The reference-baseline directory, if [`REFERENCE_DIR_ENV`] is set
/// to a non-empty value.
///
/// Relative paths are resolved against the repository root (the
/// grandparent of this crate's `CARGO_MANIFEST_DIR`), so callers may
/// pass either an absolute path or a repo-relative path like
/// `characterization/nonroad-fidelity/baselines`.
pub fn reference_dir() -> Option<PathBuf> {
    let raw = std::env::var_os(REFERENCE_DIR_ENV)?;
    let p = PathBuf::from(&raw);
    if p.as_os_str().is_empty() {
        return None;
    }
    if p.is_absolute() {
        Some(p)
    } else {
        Some(fixtures::repo_root().join(p))
    }
}

/// Path to `MANIFEST.toml` within `dir`.
pub fn manifest_path(dir: &Path) -> PathBuf {
    dir.join("MANIFEST.toml")
}

/// One fixture entry in the corpus manifest.
#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
pub struct ManifestEntry {
    /// Fixture name (e.g. `nr-construction-state`), must match a `FIXTURE_NAMES` entry.
    pub name: String,
    /// Relative path to the TSV within the baseline directory.
    pub path: String,
    /// Lowercase hex SHA256 of the TSV file bytes.
    pub sha256: String,
    /// Byte count of the TSV file.
    pub bytes: u64,
    /// Row count (non-comment lines) in the TSV file.
    pub rows: u64,
    /// Capture wall time in seconds (optional provenance field).
    #[serde(default)]
    pub wall_seconds: Option<u64>,
}

/// The corpus manifest (`MANIFEST.toml`) produced by `generate-corpus.sh`.
#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
pub struct CorpusManifest {
    /// SHA256 of the SIF used to generate the corpus (optional provenance).
    #[serde(default)]
    pub sif_sha256: Option<String>,
    /// One entry per fixture, in any order.
    pub fixtures: Vec<ManifestEntry>,
}

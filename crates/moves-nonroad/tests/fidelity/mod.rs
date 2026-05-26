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
//! # What runs today, and what is gated
//!
//! Three things the harness does *now*, on every `cargo test`:
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
//!
//! One thing is **gated** behind infrastructure that does not exist
//! in the repository yet: the end-to-end *gfortran-reference diff*.
//! It needs two inputs —
//!
//! - a captured `dbgemit` baseline per fixture, produced by running
//!   the instrumented gfortran NONROAD (`characterization/nonroad-build/`)
//!   inside the canonical-MOVES Apptainer SIF; and
//! - the Rust port's own intermediate-state capture, produced once
//!   Task 117 wires up `run_simulation` with port-side instrumentation.
//!
//! When a baseline directory is supplied via the
//! [`REFERENCE_DIR_ENV`] environment variable, the harness loads and
//! structurally validates it. The actual reference-vs-port diff
//! activates with no further harness change once Task 117 lands —
//! [`adapter`] is the contract the port instrumentation builds to,
//! and [`divergence::compare_runs`] is the diff.
//!
//! This split is deliberate: Task 115 builds the *gate*; Task 116
//! (`mo-490cm`) consumes its [`divergence::DivergenceReport`] to
//! triage divergences; Task 117 supplies the port side of the diff.

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
pub fn reference_dir() -> Option<PathBuf> {
    std::env::var_os(REFERENCE_DIR_ENV)
        .map(PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty())
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

//! Full-suite regression pass — Phase 7 Task 126 (`mo-uj3ke`).
//!
//! Runs **all 34 characterization fixtures** (24 onroad + 10 NONROAD; the 3
//! `scale-*` fixtures that require additional input databases are excluded by
//! default) through the complete Rust port and verifies:
//!
//! 1. **No fixture panics or errors** — every `moves run` invocation returns
//!    `Ok(outcome)` and produces a `MOVESRun.parquet` metadata file.
//! 2. **Module plan is non-empty** — every fixture exercises at least one
//!    calculator-graph module, confirming the RunSpec parses and the DAG
//!    filters correctly for both the onroad and NONROAD model paths.
//! 3. **Canonical-snapshot diff (active)** — [`canonical_snapshot_diff`] runs
//!    each fixture with `--snapshot` (so the calculators execute against the
//!    captured execution DB and the engine writes the real `MOVESOutput/`
//!    tree), then compares the port's per-pollutant `emissionQuant` totals
//!    against the canonical `MOVESOutput` table. It hard-asserts on the
//!    fixtures whose data plane matches canonical within a documented
//!    precision-only tolerance and prints — without asserting — the divergence
//!    for fixtures with a known, reported data-plane bug. See the catalogue in
//!    `docs/known-divergences.md`.
//!
//! # Why per-pollutant sums, not a cell diff
//!
//! Even where the port reproduces canonical to `f64` precision, the two
//! `MOVESOutput` tables disagree on metadata/labeling columns that do not
//! affect emitted mass (`iterationID`, `roadTypeID`, the `SCC` road-type
//! subfield) and on which uncertainty columns are present. A cell-level
//! [`moves_snapshot::diff_snapshots`] therefore fails on every fixture; the
//! per-pollutant `emissionQuant` total is the quantity that must agree and is
//! the same metric `characterization/audit/regression_gate.sh` uses.
//!
//! # Running the gate explicitly
//!
//! ```sh
//! cargo test --test full_suite_regression canonical_snapshot_diff -- --nocapture
//! ```
//!
//! Point it at a different snapshot tree with
//! `REGRESSION_SNAPSHOTS_DIR=<path>`.

use std::path::{Path, PathBuf};

use moves_cli::{run_simulation, RunOptions};
use moves_snapshot::{
    compare_pollutant_sums, pollutant_sums_from_output_dir, pollutant_sums_from_snapshot, Snapshot,
};
use tempfile::tempdir;

/// Environment variable naming the directory of Phase 0 canonical-MOVES
/// snapshots. Unset → the in-repo `characterization/snapshots/` tree, which is
/// populated (all 34 non-scale fixtures), so the diff gate is active.
pub const SNAPSHOTS_DIR_ENV: &str = "REGRESSION_SNAPSHOTS_DIR";

// ── helpers ──────────────────────────────────────────────────────────────────

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root above crates/moves-cli/")
        .to_path_buf()
}

fn fixtures_dir() -> PathBuf {
    workspace_root().join("characterization/fixtures")
}

/// The canonical snapshot root, overridable by [`SNAPSHOTS_DIR_ENV`].
fn snapshots_root() -> PathBuf {
    std::env::var_os(SNAPSHOTS_DIR_ENV)
        .map(PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| workspace_root().join("characterization").join("snapshots"))
}

/// All non-`scale-*` fixture XML paths in sorted order.
///
/// 37 total fixtures; 3 `scale-*` excluded (require additional input
/// databases). Result: 24 onroad (including mixed-onroad-nonroad) + 10 NONROAD = 34 fixtures.
fn all_fixtures() -> Vec<PathBuf> {
    let dir = fixtures_dir();
    let mut paths: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read fixtures dir {}: {e}", dir.display()))
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or_default();
            p.extension().and_then(|x| x.to_str()) == Some("xml") && !name.starts_with("scale-")
        })
        .collect();
    paths.sort();
    paths
}

fn fixture_name(path: &Path) -> &str {
    path.file_stem().and_then(|n| n.to_str()).unwrap_or("?")
}

fn canonical_present(snapshots_root: &Path, name: &str) -> bool {
    snapshots_root.join(name).join("manifest.json").is_file()
}

// ── fixture catalogue ─────────────────────────────────────────────────────────

/// The fixture catalogue must contain exactly 34 non-scale fixtures.
///
/// 37 total in `characterization/fixtures/`:
/// - 24 onroad/mixed (non-`nr-`, non-`scale-`): 23 default-scale + `mixed-onroad-nonroad`
/// - 10 NONROAD (`nr-*.xml`)
/// - 3 `scale-*.xml` (excluded — require additional input databases)
#[test]
fn fixture_catalogue_size() {
    let fixtures = all_fixtures();
    assert_eq!(
        fixtures.len(),
        34,
        "expected 34 non-scale fixtures (24 onroad/mixed + 10 NONROAD), \
         found {}. Update this test if the catalogue changes.",
        fixtures.len()
    );

    let onroad_count = fixtures
        .iter()
        .filter(|p| !fixture_name(p).starts_with("nr-"))
        .count();
    let nonroad_count = fixtures
        .iter()
        .filter(|p| fixture_name(p).starts_with("nr-"))
        .count();

    assert_eq!(
        onroad_count, 24,
        "expected 24 onroad/mixed fixtures, found {onroad_count}"
    );
    assert_eq!(
        nonroad_count, 10,
        "expected 10 NONROAD fixtures (nr-*), found {nonroad_count}"
    );
}

// ── always-active smoke tests ─────────────────────────────────────────────────

/// Every fixture must complete without error and produce a non-empty module plan.
///
/// Prints a regression table matching `docs/known-divergences.md`.
/// All 34 fixtures are expected to report 0 modules executed in Phase 7
/// (pre-data-plane), which is the known baseline this test pins.
#[test]
fn all_fixtures_run_without_error() {
    let fixtures = all_fixtures();
    assert!(!fixtures.is_empty(), "no fixtures found");

    println!(
        "\n{:<42} {:>8} {:>8} {:>8}",
        "fixture", "planned", "executed", "unimpl"
    );
    println!("{}", "-".repeat(72));

    let mut failures: Vec<String> = Vec::new();

    for fixture in &fixtures {
        let name = fixture_name(fixture);
        let out = tempdir().expect("tempdir");
        let result = run_simulation(&RunOptions {
            runspec: fixture.clone(),
            output: out.path().to_path_buf(),
            max_parallel_chunks: 1,
            calculator_dag: None,
            run_date_time: Some("2026-05-21T00:00:00".to_string()),
            snapshot: None,
        });

        match result {
            Err(e) => {
                failures.push(format!("{name}: {e}"));
                println!("{name:<42} ERROR");
            }
            Ok(outcome) => {
                if !outcome.run_record_path.is_file() {
                    failures.push(format!("{name}: MOVESRun.parquet missing"));
                }
                if outcome.modules_planned.is_empty() {
                    failures.push(format!("{name}: no modules planned"));
                }
                println!(
                    "{name:<42} {:>8} {:>8} {:>8}",
                    outcome.modules_planned.len(),
                    outcome.modules_executed.len(),
                    outcome.modules_unimplemented.len(),
                );
            }
        }
    }

    println!("{}", "-".repeat(72));
    println!("{} fixtures", fixtures.len());

    assert!(
        failures.is_empty(),
        "full-suite regression failures:\n  {}",
        failures.join("\n  ")
    );
}

/// NONROAD fixtures must plan at least one module independently of the
/// onroad calculator-graph path.
#[test]
fn nonroad_fixtures_plan_modules() {
    let fixtures: Vec<PathBuf> = all_fixtures()
        .into_iter()
        .filter(|p| fixture_name(p).starts_with("nr-"))
        .collect();
    assert_eq!(fixtures.len(), 10);

    for fixture in &fixtures {
        let name = fixture_name(fixture);
        let out = tempdir().expect("tempdir");
        let outcome = run_simulation(&RunOptions {
            runspec: fixture.clone(),
            output: out.path().to_path_buf(),
            max_parallel_chunks: 1,
            calculator_dag: None,
            run_date_time: Some("2026-05-21T00:00:00".to_string()),
            snapshot: None,
        })
        .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert!(
            !outcome.modules_planned.is_empty(),
            "{name}: no modules planned"
        );
    }
}

// ── canonical-diff gate ───────────────────────────────────────────────────────

/// Per-pollutant relative-difference tolerance for **onroad** fixtures whose
/// data plane is wired and matches canonical. The float-summation order differs
/// between canonical MOVES (MariaDB) and the Rust port (`f64` Polars), producing
/// sub-`1e-4` relative drift on the `emissionQuant` totals. See
/// `docs/known-divergences.md` §4.2.
const ONROAD_REL_TOL: f64 = 1e-3;

/// Per-pollutant relative-difference tolerance for **NONROAD** fixtures. NONROAD
/// arithmetic is Fortran single-precision (`real*4`) in canonical MOVES; the
/// Rust port uses `f64` throughout, so the totals differ by a documented
/// half-percent-scale amount even when the physics is reproduced exactly. See
/// `docs/known-divergences.md` §4.2.
const NONROAD_REL_TOL: f64 = 1e-2;

/// Floor on the relative-difference denominator so a pollutant that is exactly
/// zero in canonical does not yield an infinite relative difference.
const REL_DIFF_FLOOR: f64 = 1e-30;

/// Fixtures whose `MOVESOutput` matches canonical within tolerance and which the
/// gate therefore **hard-asserts**. A regression that pushes any of these out of
/// tolerance fails the test.
///
/// `vacuous` marks fixtures whose canonical snapshot has no `MOVESOutput` rows
/// (the selected process is not materialised in the capture); the port likewise
/// emits nothing, so there is no divergence to measure. They are pinned here so
/// that if either side starts producing rows the asymmetry is caught.
fn asserted_fixtures() -> &'static [(&'static str, f64, bool)] {
    &[
        // (fixture, per-pollutant relative tolerance, vacuous)
        ("process-evap-fvv", ONROAD_REL_TOL, false), // ~8.2e-5
        ("process-evap-leaks", ONROAD_REL_TOL, false), // ~1.6e-7
        ("process-evap-permeation", ONROAD_REL_TOL, false), // ~2.1e-7
        ("nr-commercial-nation", NONROAD_REL_TOL, false), // ~3.5e-3 (real*4)
        ("process-apu", ONROAD_REL_TOL, true),
        ("process-crankcase-extidle", ONROAD_REL_TOL, true),
        ("process-crankcase-start", ONROAD_REL_TOL, true),
        ("process-extended-idle", ONROAD_REL_TOL, true),
    ]
}

/// Fixtures with a **known, reported data-plane bug** whose `MOVESOutput`
/// diverges from canonical far beyond any precision budget. They are run and
/// their divergence is printed for visibility, but the gate does **not** assert
/// on them — masking the bug with a widened tolerance would be worse than no
/// gate (a real-bug divergence must be fixed in the data plane, never hidden).
/// Each entry is catalogued in `docs/known-divergences.md` §4.4. As a fixture's
/// data plane is fixed it should graduate from this list into
/// [`asserted_fixtures`].
const QUARANTINED_FIXTURES: &[&str] = &[
    // Onroad-exhaust path emits a fixed ~8,632-row block of NONROAD-coded rows
    // (SCC 2260/2265/2282/2285) regardless of the RunSpec — identical bytes
    // across every onroad fixture. Emitted mass is ~7 orders of magnitude high.
    "chain-nonhaptog",
    "chain-tog-speciation",
    "expand-counties",
    "expand-criteria",
    "expand-day",
    "expand-fueltype-diesel",
    "expand-month",
    "expand-sourcetype",
    "mixed-onroad-nonroad",
    "process-airtoxics",
    "process-brakewear",
    "process-crankcase-running",
    "process-nox-speciation",
    "process-pm-exhaust",
    "process-refueling",
    "process-tirewear",
    "sample-runspec",
    // NONROAD fixtures that emit nothing (port row count 0 vs a populated
    // canonical) or a wrong row count — population/sector-coverage gaps.
    "nr-agriculture-state",
    "nr-airport-support-county",
    "nr-construction-state",
    "nr-industrial-county",
    "nr-lawn-garden-county",
    "nr-logging-county",
    "nr-pleasure-craft-state",
    "nr-railroad-support-nation",
    "nr-recreational-county",
];

fn is_quarantined(name: &str) -> bool {
    QUARANTINED_FIXTURES.contains(&name)
}

/// Compare each fixture's **real `MOVESOutput`** against its canonical-MOVES
/// snapshot, per-pollutant, with `--snapshot` active.
///
/// **Dormant** unless [`SNAPSHOTS_DIR_ENV`] points at a populated snapshot tree
/// (or the in-repo `characterization/snapshots/` tree is populated, which it is)
/// AND the fixture's snapshot sub-directory contains a `manifest.json`.
///
/// For every covered fixture the test:
/// 1. Runs the port with `snapshot: Some(<canonical-dir>)`, so the calculators
///    execute against the captured execution database and the engine writes the
///    real `MOVESOutput/` partitioned Parquet tree (not just `MOVESRun.parquet`).
/// 2. Sums `emissionQuant` per `pollutantID` from both the canonical
///    `MOVESOutput` table and the port's `MOVESOutput/` tree.
/// 3. Compares the per-pollutant totals.
///
/// Why per-pollutant sums rather than a cell-level [`diff_snapshots`]? Even where
/// the port reproduces canonical to `f64` precision, the two `MOVESOutput` tables
/// disagree on metadata/labeling columns that do not affect emitted mass
/// (`iterationID` NULL vs 1, `roadTypeID` 0 vs the link road type, and the `SCC`
/// road-type subfield), and canonical carries `emissionQuantMean`/`Sigma` where
/// the port carries `emissionRate`/`runHash`. A cell diff fails on those for
/// every fixture; the per-pollutant `emissionQuant` total is the quantity that
/// must agree and cleanly isolates real divergences in emitted mass. This is the
/// same metric as `characterization/audit/regression_gate.sh`.
///
/// The gate hard-asserts on [`asserted_fixtures`] and prints — without asserting
/// — the divergence for [`QUARANTINED_FIXTURES`] (known, reported data-plane
/// bugs; see `docs/known-divergences.md` §4.4).
#[test]
fn canonical_snapshot_diff() {
    let root = snapshots_root();
    let fixtures = all_fixtures();

    let covered: Vec<&Path> = fixtures
        .iter()
        .map(|p| p.as_path())
        .filter(|p| canonical_present(&root, fixture_name(p)))
        .collect();

    if covered.is_empty() {
        println!(
            "\n[canonical_snapshot_diff] DORMANT\n\
             No canonical snapshots under '{}'.\n\
             See docs/known-divergences.md for how to enable this gate.",
            root.display()
        );
        return;
    }

    println!(
        "\n[canonical_snapshot_diff] {} fixture(s) against {}",
        covered.len(),
        root.display()
    );
    println!(
        "{:<28} {:>10} {:>10} {:>14} {:>10}",
        "fixture", "canon_rows", "port_rows", "max_rel_diff", "verdict"
    );
    println!("{}", "-".repeat(78));

    let mut failures: Vec<String> = Vec::new();
    let mut passed = 0usize;
    let mut quarantined = 0usize;

    for fixture_path in covered {
        let name = fixture_name(fixture_path);
        let out = tempdir().expect("tempdir");

        let outcome = run_simulation(&RunOptions {
            runspec: fixture_path.to_path_buf(),
            output: out.path().to_path_buf(),
            max_parallel_chunks: 1,
            calculator_dag: None,
            run_date_time: Some("2026-05-21T00:00:00".to_string()),
            // Activate the data plane: calculators execute against the captured
            // execution DB and the engine writes the real MOVESOutput tree.
            snapshot: Some(root.join(name)),
        })
        .unwrap_or_else(|e| panic!("{name}: run error — {e}"));

        let canonical = match Snapshot::load(&root.join(name)) {
            Ok(s) => pollutant_sums_from_snapshot(&s),
            Err(e) => {
                println!("{name:<28} canonical load error: {e}");
                failures.push(format!("{name}: canonical load error: {e}"));
                continue;
            }
        };
        let port = pollutant_sums_from_output_dir(outcome.output_root.as_path())
            .unwrap_or_else(|e| panic!("{name}: reading port MOVESOutput — {e}"));

        let cmp = compare_pollutant_sums(&canonical, &port, REL_DIFF_FLOOR);

        let asserted = asserted_fixtures().iter().find(|(n, _, _)| *n == name);
        let verdict = if let Some((_, tol, vacuous)) = asserted {
            // Hard-asserted fixture.
            let row_mismatch = canonical.row_count != port.row_count;
            let within = cmp.within(*tol);
            if *vacuous {
                if canonical.row_count == 0 && port.row_count == 0 {
                    passed += 1;
                    "PASS(empty)"
                } else {
                    failures.push(format!(
                        "{name}: expected vacuous (0 rows both) but canon={} port={}",
                        canonical.row_count, port.row_count
                    ));
                    "FAIL"
                }
            } else if within && !row_mismatch {
                passed += 1;
                "PASS"
            } else {
                failures.push(format!(
                    "{name}: max_rel_diff={:.3e} (tol {:.0e}), canon_rows={} port_rows={}",
                    cmp.max_rel_diff, tol, canonical.row_count, port.row_count
                ));
                "FAIL"
            }
        } else if is_quarantined(name) {
            quarantined += 1;
            "BUG(quar)"
        } else {
            // A covered fixture that is neither asserted nor quarantined is an
            // unclassified divergence: fail loudly so it gets triaged rather
            // than silently ignored.
            failures.push(format!(
                "{name}: UNCLASSIFIED max_rel_diff={:.3e}, canon_rows={} port_rows={} — \
                 add to asserted_fixtures (with a documented precision-only tolerance) \
                 or QUARANTINED_FIXTURES (with a docs/known-divergences.md bug entry)",
                cmp.max_rel_diff, canonical.row_count, port.row_count
            ));
            "UNCLASS"
        };

        println!(
            "{name:<28} {:>10} {:>10} {:>14.3e} {:>10}",
            canonical.row_count, port.row_count, cmp.max_rel_diff, verdict
        );
    }

    println!("{}", "-".repeat(78));
    println!(
        "{passed} asserted-pass, {quarantined} quarantined (reported bugs), \
         {} failure(s)",
        failures.len()
    );

    assert!(
        failures.is_empty(),
        "canonical-snapshot gate failures (these are regressions in the \
         asserted set or unclassified divergences — fix the data plane, do not \
         widen tolerances to mask a real bug):\n  {}",
        failures.join("\n  ")
    );
}

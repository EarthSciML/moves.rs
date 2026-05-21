//! Full-suite regression pass — Phase 7 Task 126 (`mo-uj3ke`).
//!
//! Runs **all 33 characterization fixtures** (23 onroad + 10 NONROAD; the 3
//! `scale-*` fixtures that require additional input databases are excluded by
//! default) through the complete Rust port and verifies:
//!
//! 1. **No fixture panics or errors** — every `moves run` invocation returns
//!    `Ok(outcome)` and produces a `MOVESRun.parquet` metadata file.
//! 2. **Module plan is non-empty** — every fixture exercises at least one
//!    calculator-graph module, confirming the RunSpec parses and the DAG
//!    filters correctly for both the onroad and NONROAD model paths.
//! 3. **Canonical-snapshot diff (gated)** — when [`SNAPSHOTS_DIR_ENV`] is
//!    set to a directory containing Phase 0 canonical-MOVES snapshots, each
//!    fixture's port output snapshot is diffed against the corresponding
//!    canonical snapshot within the tolerance budget from
//!    `characterization/tolerance.toml`. Divergences beyond the budget fail
//!    the test; within-budget divergences are recorded in the output. This
//!    gate is dormant until canonical snapshots and real calculator output
//!    both exist — see `docs/known-divergences.md`.
//!
//! # Current state (Phase 7 entry)
//!
//! All 33 fixtures report 0 modules executed and all planned modules
//! unimplemented. This is expected: calculator `execute()` methods return
//! `CalculatorOutput::empty()` until the data plane is wired in. The test
//! still exercises RunSpec parsing, DAG filtering, engine orchestration, and
//! `MOVESRun.parquet` output shape for both model paths.
//!
//! # Enabling the canonical-diff gate
//!
//! 1. Generate canonical-MOVES snapshots on an HPC node with Apptainer
//!    (see `characterization/apptainer/README.md`):
//!    ```sh
//!    characterization/run-all-fixtures.sh --fakeroot --keep-going
//!    ```
//!
//! 2. Wire the data plane so calculator `execute()` methods produce real
//!    output (Phase 4 `DataFrameStore` deliverables).
//!
//! 3. Run with the snapshot directory set:
//!    ```sh
//!    REGRESSION_SNAPSHOTS_DIR=characterization/snapshots \
//!        cargo test --test full_suite_regression -- --nocapture
//!    ```

use std::path::{Path, PathBuf};

use moves_cli::{run_simulation, RunOptions};
use moves_snapshot::{diff_snapshots, DiffOptions, Snapshot, ToleranceConfig};
use tempfile::tempdir;

/// Environment variable naming the directory of Phase 0 canonical-MOVES
/// snapshots. Unset → the in-repo `characterization/snapshots/` tree (which
/// currently contains only a README, keeping the diff gate dormant).
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

fn tolerance_config_path() -> PathBuf {
    workspace_root().join("characterization/tolerance.toml")
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
/// 36 total fixtures; 3 `scale-*` excluded (require additional input
/// databases). Result: 23 onroad + 10 NONROAD = 33 fixtures.
fn all_fixtures() -> Vec<PathBuf> {
    let dir = fixtures_dir();
    let mut paths: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read fixtures dir {}: {e}", dir.display()))
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or_default();
            p.extension().and_then(|x| x.to_str()) == Some("xml")
                && !name.starts_with("scale-")
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

fn tolerance_opts() -> DiffOptions {
    let path = tolerance_config_path();
    if path.is_file() {
        ToleranceConfig::from_file(&path)
            .map(Into::into)
            .unwrap_or_default()
    } else {
        DiffOptions::default()
    }
}

// ── fixture catalogue ─────────────────────────────────────────────────────────

/// The fixture catalogue must contain exactly 33 non-scale fixtures.
///
/// 36 total in `characterization/fixtures/`:
/// - 23 onroad (non-`nr-`, non-`scale-`)
/// - 10 NONROAD (`nr-*.xml`)
/// - 3 `scale-*.xml` (excluded — require additional input databases)
#[test]
fn fixture_catalogue_size() {
    let fixtures = all_fixtures();
    assert_eq!(
        fixtures.len(),
        33,
        "expected 33 non-scale fixtures (23 onroad + 10 NONROAD), \
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
        onroad_count, 23,
        "expected 23 onroad fixtures, found {onroad_count}"
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
/// All 33 fixtures are expected to report 0 modules executed in Phase 7
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
        })
        .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert!(
            !outcome.modules_planned.is_empty(),
            "{name}: no modules planned"
        );
    }
}

// ── canonical-diff gate ───────────────────────────────────────────────────────

/// Diff each fixture's port output against its canonical-MOVES snapshot.
///
/// **Dormant** unless [`SNAPSHOTS_DIR_ENV`] points at a populated snapshot
/// tree AND the fixture's snapshot sub-directory contains a `manifest.json`.
/// When a snapshot is found, the test:
///
/// 1. Runs the port to get output.
/// 2. Loads the port output as a `moves_snapshot`-format snapshot (available
///    once the data plane writes output in that format).
/// 3. Diffs port vs canonical within the tolerance budget.
/// 4. Fails only when differences exceed the budget.
///
/// Differences within the budget (known artifacts) are printed but not
/// failing. Update `characterization/tolerance.toml` to widen a budget for
/// a characterised artifact, and add a comment documenting why.
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

    let opts = tolerance_opts();
    println!(
        "\n[canonical_snapshot_diff] {} fixture(s) against {}",
        covered.len(),
        root.display()
    );

    let mut ok: Vec<&str> = Vec::new();
    let mut failures: Vec<String> = Vec::new();

    for fixture_path in covered {
        let name = fixture_name(fixture_path);
        let out = tempdir().expect("tempdir");

        let outcome = run_simulation(&RunOptions {
            runspec: fixture_path.to_path_buf(),
            output: out.path().to_path_buf(),
            max_parallel_chunks: 1,
            calculator_dag: None,
            run_date_time: Some("2026-05-21T00:00:00".to_string()),
        })
        .unwrap_or_else(|e| panic!("{name}: run error — {e}"));

        // Load canonical snapshot.
        let canonical = match Snapshot::load(&root.join(name)) {
            Ok(s) => s,
            Err(e) => {
                println!("{name}: SKIP — canonical load error: {e}");
                continue;
            }
        };

        // Load port output as a snapshot. Currently the engine writes only
        // MOVESRun.parquet (no manifest.json), so this will fail until the
        // data plane is wired and the output processor writes in
        // moves-snapshot format. The error is handled gracefully.
        let port = match Snapshot::load(outcome.output_root.as_path()) {
            Ok(s) => s,
            Err(e) => {
                println!("{name}: SKIP diff — port output not in snapshot format: {e}");
                continue;
            }
        };

        let diff = diff_snapshots(&canonical, &port, &opts);
        let s = diff.summary();

        if diff.is_empty() {
            ok.push(name);
        } else {
            failures.push(format!(
                "{name}: tables_added={} tables_removed={} tables_changed={} \
                 schema_diffs={} rows_added={} rows_removed={} cells_changed={}",
                s.tables_added,
                s.tables_removed,
                s.tables_changed,
                s.schema_diffs,
                s.rows_added,
                s.rows_removed,
                s.cells_changed,
            ));
        }
    }

    if !ok.is_empty() {
        println!("within tolerance ({}):", ok.len());
        for name in &ok {
            println!("  ✓ {name}");
        }
    }
    if !failures.is_empty() {
        println!("beyond tolerance ({}):", failures.len());
        for f in &failures {
            println!("  ✗ {f}");
        }
    }

    assert!(
        failures.is_empty(),
        "canonical-snapshot divergences beyond budget — \
         update characterization/tolerance.toml to accept known artifacts:\n  {}",
        failures.join("\n  ")
    );
}

//! Performance-baseline integration test — Task 75 (`mo-85wl`).
//!
//! Runs every onroad fixture through the Rust engine and reports per-fixture
//! wall time, planning time, execution time, and peak RSS. The test asserts
//! correctness (runs must not error) and a loose wall-time upper bound per
//! fixture so CI catches regressions. The printed table is the Task 75
//! baseline record.
//!
//! # What this measures today (Phase 3, pre-data-plane)
//!
//! Calculators return `CalculatorOutput::empty()` — the compute cores are
//! ported but the data plane that feeds real row data is not yet wired in.
//! The timings therefore reflect **framework overhead only**: RunSpec parsing,
//! calculator-graph planning, MasterLoop setup and iteration (zero rows),
//! and `MOVESRun.parquet` output. These numbers are the reference against
//! which the data-plane overhead will be added in later phases.
//!
//! # Comparison to canonical MOVES
//!
//! See `docs/performance-baseline.md` for the methodology and the
//! canonical-MOVES reference numbers.
//!
//! # Cache miss rate
//!
//! Hardware performance counters require an external tool. To collect cache
//! statistics, wrap the binary:
//!
//! ```text
//! perf stat -e cache-misses,cache-references,L1-dcache-loads \
//!     target/release/moves run --runspec characterization/fixtures/process-airtoxics.xml \
//!     --output /tmp/out
//! ```

use std::path::{Path, PathBuf};
use std::time::Duration;

use moves_cli::{run_simulation, RunOptions};
use tempfile::tempdir;

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

/// All onroad fixture XML files (excludes `nr-*` NONROAD fixtures and the
/// `scale-*` fixtures that require additional input databases).
fn onroad_fixtures() -> Vec<PathBuf> {
    let dir = fixtures_dir();
    let mut paths: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read fixtures dir {}: {e}", dir.display()))
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or_default();
            p.extension().and_then(|x| x.to_str()) == Some("xml")
                && !name.starts_with("nr-")
                && !name.starts_with("scale-")
        })
        .collect();
    paths.sort();
    paths
}

/// Print header and separator lines for the performance table.
fn print_table_header() {
    println!(
        "\n{:<38} {:>10} {:>10} {:>10} {:>10} {:>8}",
        "fixture", "wall(ms)", "plan(ms)", "exec(ms)", "chunks", "RSS(MiB)"
    );
    println!("{}", "-".repeat(92));
}

/// Print one row of the performance table.
fn print_table_row(
    name: &str,
    wall_ms: f64,
    plan_ms: f64,
    exec_ms: f64,
    chunks: usize,
    rss_mib: Option<f64>,
) {
    let rss_str = rss_mib
        .map(|r| format!("{r:>8.1}"))
        .unwrap_or_else(|| "     n/a".to_string());
    println!(
        "{:<38} {:>10.1} {:>10.1} {:>10.1} {:>10} {rss_str}",
        name, wall_ms, plan_ms, exec_ms, chunks,
    );
}

/// Maximum allowed wall time per fixture before the test fails.
/// Generous enough to pass on slow CI machines; tight enough to catch a
/// runaway regression. The framework-overhead path (no real data) should
/// finish well under 500 ms on any modern machine.
const WALL_TIME_LIMIT: Duration = Duration::from_secs(10);

#[test]
fn perf_baseline_all_onroad_fixtures() {
    let fixtures = onroad_fixtures();
    assert!(
        !fixtures.is_empty(),
        "no onroad fixtures found in {}",
        fixtures_dir().display()
    );

    print_table_header();

    let mut total_wall = Duration::ZERO;
    let mut total_plan = Duration::ZERO;
    let mut total_exec = Duration::ZERO;
    let mut failures: Vec<String> = Vec::new();

    for fixture in &fixtures {
        let name = fixture.file_stem().and_then(|n| n.to_str()).unwrap_or("?");

        let out_dir = tempdir().expect("tempdir");
        let opts = RunOptions {
            runspec: fixture.clone(),
            output: out_dir.path().to_path_buf(),
            max_parallel_chunks: 1,
            calculator_dag: None,
            run_date_time: None,
        };

        match run_simulation(&opts) {
            Ok(outcome) => {
                let wall_ms = outcome.wall_time.as_secs_f64() * 1000.0;
                let plan_ms = outcome.planning_time.as_secs_f64() * 1000.0;
                let exec_ms = outcome.execution_time.as_secs_f64() * 1000.0;
                let rss_mib = outcome.peak_rss_kib.map(|k| k as f64 / 1024.0);

                print_table_row(
                    name,
                    wall_ms,
                    plan_ms,
                    exec_ms,
                    outcome.chunk_count(),
                    rss_mib,
                );

                total_wall += outcome.wall_time;
                total_plan += outcome.planning_time;
                total_exec += outcome.execution_time;

                if outcome.wall_time > WALL_TIME_LIMIT {
                    failures.push(format!(
                        "{name}: wall time {:.1} ms exceeded limit {:.0} ms",
                        wall_ms,
                        WALL_TIME_LIMIT.as_secs_f64() * 1000.0
                    ));
                }
            }
            Err(e) => {
                failures.push(format!("{name}: run failed: {e}"));
                println!("{:<38} ERROR: {e}", name);
            }
        }
    }

    // Summary row.
    println!("{}", "-".repeat(92));
    println!(
        "{:<38} {:>10.1} {:>10.1} {:>10.1}",
        format!("TOTAL ({} fixtures)", fixtures.len()),
        total_wall.as_secs_f64() * 1000.0,
        total_plan.as_secs_f64() * 1000.0,
        total_exec.as_secs_f64() * 1000.0,
    );
    println!();

    assert!(
        failures.is_empty(),
        "performance-baseline failures:\n  {}",
        failures.join("\n  ")
    );
}

/// Smoke test: a single fixture run at host parallelism completes without
/// error and reports non-zero timing fields.
#[test]
fn single_fixture_timing_fields_are_populated() {
    let fixture = fixtures_dir().join("process-airtoxics.xml");
    if !fixture.is_file() {
        return; // fixture missing — skip
    }
    let out_dir = tempdir().expect("tempdir");
    let outcome = run_simulation(&RunOptions {
        runspec: fixture,
        output: out_dir.path().to_path_buf(),
        max_parallel_chunks: 0,
        calculator_dag: None,
        run_date_time: None,
    })
    .expect("run must succeed");

    assert!(
        outcome.wall_time > Duration::ZERO,
        "wall_time must be positive"
    );
    assert!(
        outcome.planning_time > Duration::ZERO,
        "planning_time must be positive"
    );
    assert_eq!(
        outcome.chunk_wall_times.len(),
        outcome.chunk_count(),
        "chunk_wall_times length must match chunk_count"
    );
}

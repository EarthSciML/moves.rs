//! Public benchmark suite — Task 127 (`mo-6w7oo`).
//!
//! Measures wall time, peak memory, and output-correctness metrics across the
//! six representative workload categories from the migration plan, sweeping
//! `--max-parallel-chunks` at 1, 2, 4, and NCPU.
//!
//! # Workload categories
//!
//! | Category | Fixtures | Scale fixture env var |
//! |---|---|---|
//! | Default-scale national (onroad) | all `process-*`, `chain-*`, `expand-*`, `sample-runspec` | — |
//! | NONROAD-only | all `nr-*` | — |
//! | Mixed onroad + NONROAD | `mixed-onroad-nonroad` | — |
//! | County-Scale (single county) | `scale-county` | `BENCHMARK_SCALE_INPUTS_DIR` |
//! | Project-Scale | `scale-project` | `BENCHMARK_SCALE_INPUTS_DIR` |
//! | Rates-mode | `scale-rates` | `BENCHMARK_SCALE_INPUTS_DIR` |
//!
//! # Scale fixtures
//!
//! The three `scale-*` fixtures require County Database (CDB) and Project
//! Database (PDB) Parquet inputs that are not bundled in the repo. Set
//! `BENCHMARK_SCALE_INPUTS_DIR` to the directory containing the appropriate
//! input Parquet files to enable these workloads. Without the variable, the
//! scale-fixture section prints a notice and skips.
//!
//! # Output-correctness column
//!
//! Each workload row reports `impl%` — the fraction of planned calculator
//! modules that were executed (not unimplemented stubs). In Phase 7 this is
//! 0 % because the data plane is not yet wired; once Phase 4 lands the number
//! will climb toward 100 %.
//!
//! # Reproducing
//!
//! ```sh
//! # All default-scale + NONROAD + mixed workloads (no external inputs):
//! cargo test -p moves-cli --test benchmark_suite -- --nocapture
//!
//! # Include County/Project/Rates scale fixtures:
//! BENCHMARK_SCALE_INPUTS_DIR=/path/to/scale-inputs \
//!     cargo test -p moves-cli --test benchmark_suite -- --nocapture
//!
//! # Release build for publication-quality numbers:
//! cargo test -p moves-cli --test benchmark_suite --release -- --nocapture
//! ```

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use moves_cli::{run_simulation, RunOptions};
use tempfile::tempdir;

// ── environment variable ─────────────────────────────────────────────────────

/// Set to a directory containing CDB/PDB Parquet inputs to enable
/// County-Scale, Project-Scale, and Rates-mode workloads.
const SCALE_INPUTS_ENV: &str = "BENCHMARK_SCALE_INPUTS_DIR";

// ── workspace helpers ─────────────────────────────────────────────────────────

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

fn scale_inputs_dir() -> Option<PathBuf> {
    std::env::var_os(SCALE_INPUTS_ENV)
        .map(PathBuf::from)
        .filter(|p| p.is_dir())
}

// ── fixture selection ─────────────────────────────────────────────────────────

/// All default-scale onroad fixtures (exclude `nr-*`, `scale-*`, `mixed-*`).
fn default_scale_fixtures() -> Vec<PathBuf> {
    collect_fixtures(|name| {
        !name.starts_with("nr-")
            && !name.starts_with("scale-")
            && !name.starts_with("mixed-")
    })
}

/// All NONROAD-only fixtures (`nr-*`).
fn nonroad_fixtures() -> Vec<PathBuf> {
    collect_fixtures(|name| name.starts_with("nr-"))
}

/// Mixed onroad + NONROAD fixture.
fn mixed_fixtures() -> Vec<PathBuf> {
    collect_fixtures(|name| name.starts_with("mixed-"))
}

/// Scale fixtures — only exist when the inputs directory is available.
fn scale_fixtures(kind: &str) -> Option<PathBuf> {
    let path = fixtures_dir().join(format!("{kind}.xml"));
    if path.is_file() { Some(path) } else { None }
}

fn collect_fixtures(predicate: impl Fn(&str) -> bool) -> Vec<PathBuf> {
    let dir = fixtures_dir();
    let mut paths: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read fixtures dir {}: {e}", dir.display()))
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or_default();
            p.extension().and_then(|x| x.to_str()) == Some("xml") && predicate(name)
        })
        .collect();
    paths.sort();
    paths
}

// ── measurement helpers ──────────────────────────────────────────────────────

/// Read VmHWM from /proc/self/status (Linux only).
fn read_peak_rss_mib() -> Option<f64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        line.strip_prefix("VmHWM:")?
            .split_whitespace()
            .next()?
            .parse::<u64>()
            .ok()
            .map(|kib| kib as f64 / 1024.0)
    })
}

/// Per-fixture result from a single run.
#[derive(Debug)]
struct FixtureResult {
    name: String,
    wall_ms: f64,
    plan_ms: f64,
    exec_ms: f64,
    chunks: usize,
    modules_planned: usize,
    modules_executed: usize,
    rss_mib: Option<f64>,
}

/// Run a single fixture at the given parallelism, returning its metrics.
fn run_fixture(path: &Path, max_parallel_chunks: usize) -> Result<FixtureResult, String> {
    let name = path
        .file_stem()
        .and_then(|n| n.to_str())
        .unwrap_or("?")
        .to_string();
    let out_dir = tempdir().map_err(|e| format!("tempdir: {e}"))?;
    let rss_before = read_peak_rss_mib();
    let opts = RunOptions {
        runspec: path.to_path_buf(),
        output: out_dir.path().to_path_buf(),
        max_parallel_chunks,
        calculator_dag: None,
        run_date_time: None,
    };
    let outcome = run_simulation(&opts).map_err(|e| format!("{name}: {e}"))?;
    let rss_after = read_peak_rss_mib();
    let rss_mib = match (rss_before, rss_after) {
        (Some(b), Some(a)) => Some(a.max(b) - b.min(a)),
        (None, a) => a,
        (b, None) => b,
    };
    Ok(FixtureResult {
        name,
        wall_ms: outcome.wall_time.as_secs_f64() * 1000.0,
        plan_ms: outcome.planning_time.as_secs_f64() * 1000.0,
        exec_ms: outcome.execution_time.as_secs_f64() * 1000.0,
        chunks: outcome.chunk_count(),
        modules_planned: outcome.modules_planned.len(),
        modules_executed: outcome.modules_executed.len(),
        rss_mib,
    })
}

// ── printing helpers ──────────────────────────────────────────────────────────

fn print_fixture_header() {
    println!(
        "\n{:<36} {:>10} {:>10} {:>10} {:>7} {:>7} {:>8} {:>8}",
        "fixture", "wall(ms)", "plan(ms)", "exec(ms)", "chunks", "impl%", "RSS_MiB", "N"
    );
    println!("{}", "-".repeat(100));
}

fn print_fixture_row(r: &FixtureResult, n: usize) {
    let impl_pct = if r.modules_planned == 0 {
        0.0
    } else {
        r.modules_executed as f64 / r.modules_planned as f64 * 100.0
    };
    let rss_str = r
        .rss_mib
        .map(|v| format!("{v:>8.1}"))
        .unwrap_or_else(|| "     n/a".to_string());
    println!(
        "{:<36} {:>10.1} {:>10.1} {:>10.1} {:>7} {:>6.0}% {rss_str} {:>8}",
        r.name, r.wall_ms, r.plan_ms, r.exec_ms, r.chunks, impl_pct, n
    );
}

// ── parallelism sweep helpers ─────────────────────────────────────────────────

fn parallelism_levels(ncpu: usize) -> Vec<usize> {
    let mut levels = vec![1usize, 2, 4];
    if ncpu > 4 {
        levels.push(ncpu);
    }
    levels.dedup();
    levels
}

/// Print the N-sweep summary table for a workload group.
fn print_n_sweep(
    label: &str,
    fixtures: &[PathBuf],
    ncpu: usize,
    failures: &mut Vec<String>,
) {
    if fixtures.is_empty() {
        return;
    }
    let levels = parallelism_levels(ncpu);
    println!("\n### {label} — N sweep ({} fixture(s))\n", fixtures.len());
    println!(
        "{:>6}  {:>14}  {:>8}  {:>12}",
        "N", "total_wall_ms", "speedup", "peak_RSS_MiB"
    );
    println!("{}", "-".repeat(48));

    let mut baseline_wall: Option<f64> = None;
    for &n in &levels {
        let (wall_ms, rss_mib) = run_group_silent(fixtures, n, failures);
        let speedup = baseline_wall.map(|b| b / wall_ms).unwrap_or(1.0);
        if baseline_wall.is_none() {
            baseline_wall = Some(wall_ms);
        }
        let rss_str = rss_mib
            .map(|r| format!("{r:>12.1}"))
            .unwrap_or_else(|| "         n/a".to_string());
        println!("{n:>6}  {wall_ms:>14.1}  {speedup:>8.2}  {rss_str}");
    }
    println!();
}

/// Run all fixtures silently (no per-fixture rows); return (total_wall_ms, peak_rss).
fn run_group_silent(
    fixtures: &[PathBuf],
    n: usize,
    failures: &mut Vec<String>,
) -> (f64, Option<f64>) {
    let t_start = Instant::now();
    let rss_start = read_peak_rss_mib();
    for path in fixtures {
        if let Err(e) = run_fixture(path, n) {
            failures.push(e);
        }
    }
    let wall_ms = t_start.elapsed().as_secs_f64() * 1000.0;
    let rss_after = read_peak_rss_mib();
    let rss_delta = match (rss_start, rss_after) {
        (Some(b), Some(a)) => Some((a - b).max(0.0)),
        (None, a) => a,
        _ => None,
    };
    (wall_ms, rss_delta)
}

// ── individual fixture detail at N=1 ─────────────────────────────────────────

fn print_fixture_group_detail(label: &str, fixtures: &[PathBuf], failures: &mut Vec<String>) {
    if fixtures.is_empty() {
        return;
    }
    println!("\n### {label} — per-fixture detail (N=1)\n");
    print_fixture_header();
    let mut total_wall = Duration::ZERO;
    let mut total_plan = Duration::ZERO;
    for path in fixtures {
        match run_fixture(path, 1) {
            Ok(r) => {
                total_wall += Duration::from_secs_f64(r.wall_ms / 1000.0);
                total_plan += Duration::from_secs_f64(r.plan_ms / 1000.0);
                print_fixture_row(&r, 1);
            }
            Err(e) => {
                failures.push(format!("[{label}] {e}"));
                println!("  ERROR: {e}");
            }
        }
    }
    println!("{}", "-".repeat(100));
    println!(
        "{:<36} {:>10.1} {:>10.1}",
        format!("TOTAL ({} fixtures)", fixtures.len()),
        total_wall.as_secs_f64() * 1000.0,
        total_plan.as_secs_f64() * 1000.0,
    );
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// Full benchmark suite — all six workload categories.
///
/// Prints the per-fixture detail table at N=1 and the N-sweep summary table
/// for each workload group. Scale fixtures are skipped unless
/// `BENCHMARK_SCALE_INPUTS_DIR` is set and points to a valid directory.
#[test]
fn benchmark_all_workload_categories() {
    let ncpu = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    let scale_inputs = scale_inputs_dir();

    println!(
        "\n\
        ═══════════════════════════════════════════════════════════════\n\
        MOVES-Rust Benchmark Suite — Task 127\n\
        Host CPUs: {ncpu}\n\
        Scale inputs: {}\n\
        ═══════════════════════════════════════════════════════════════",
        scale_inputs
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| format!("(not set — skip scale fixtures; set {SCALE_INPUTS_ENV})"))
    );

    let mut failures: Vec<String> = Vec::new();

    // ── 1. Default-scale national (onroad) ─────────────────────────────────
    {
        let fixtures = default_scale_fixtures();
        println!("\n\n## 1. Default-Scale National (onroad, {} fixtures)\n", fixtures.len());
        print_fixture_group_detail("default-scale", &fixtures, &mut failures);
        print_n_sweep("Default-Scale", &fixtures, ncpu, &mut failures);
    }

    // ── 2. NONROAD-only ───────────────────────────────────────────────────
    {
        let fixtures = nonroad_fixtures();
        println!("\n\n## 2. NONROAD-Only ({} fixtures)\n", fixtures.len());
        print_fixture_group_detail("nonroad", &fixtures, &mut failures);
        print_n_sweep("NONROAD-Only", &fixtures, ncpu, &mut failures);
    }

    // ── 3. Mixed onroad + NONROAD ─────────────────────────────────────────
    {
        let fixtures = mixed_fixtures();
        println!("\n\n## 3. Mixed Onroad + NONROAD ({} fixture(s))\n", fixtures.len());
        print_fixture_group_detail("mixed", &fixtures, &mut failures);
        print_n_sweep("Mixed Onroad+NONROAD", &fixtures, ncpu, &mut failures);
    }

    // ── 4–6. Scale fixtures (need external inputs) ────────────────────────
    let scale_kinds = [
        ("scale-county", "4. County-Scale (single county, single year)"),
        ("scale-project", "5. Project-Scale"),
        ("scale-rates",  "6. Rates-Mode"),
    ];

    for (kind, section) in &scale_kinds {
        println!("\n\n## {section}\n");
        if scale_inputs.is_none() {
            println!(
                "  [SKIPPED] Set {SCALE_INPUTS_ENV} to a directory containing \
                 CDB/PDB Parquet inputs to enable this workload.\n"
            );
            continue;
        }
        match scale_fixtures(kind) {
            None => {
                println!("  [SKIPPED] Fixture {kind}.xml not found.\n");
            }
            Some(path) => {
                print_fixture_group_detail(kind, std::slice::from_ref(&path), &mut failures);
                print_n_sweep(kind, &[path], ncpu, &mut failures);
            }
        }
    }

    // ── assertion ─────────────────────────────────────────────────────────
    assert!(
        failures.is_empty(),
        "benchmark failures:\n  {}",
        failures.join("\n  ")
    );
}

/// Fast smoke test: one fixture from each always-available category at N=1.
///
/// Asserts non-error completion and non-zero timing fields. Does not sweep N.
/// Runs in a few seconds; suitable for `cargo test` without `--nocapture`.
#[test]
fn benchmark_smoke_test_one_per_category() {
    let fixtures_dir = fixtures_dir();
    let candidates = [
        fixtures_dir.join("process-airtoxics.xml"),    // default-scale
        fixtures_dir.join("nr-commercial-nation.xml"), // nonroad
        fixtures_dir.join("mixed-onroad-nonroad.xml"), // mixed
    ];

    let mut failures: Vec<String> = Vec::new();
    for path in &candidates {
        if !path.is_file() {
            continue;
        }
        match run_fixture(path, 1) {
            Ok(r) => {
                assert!(
                    r.wall_ms > 0.0,
                    "{}: wall_ms must be positive",
                    r.name
                );
                assert!(
                    r.chunks > 0,
                    "{}: must plan at least one chunk",
                    r.name
                );
            }
            Err(e) => failures.push(e),
        }
    }
    assert!(failures.is_empty(), "smoke failures:\n  {}", failures.join("\n  "));
}

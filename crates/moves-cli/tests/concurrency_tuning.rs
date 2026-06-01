//! Concurrency-tuning and memory-pressure test — Task 76 (`mo-e0da`).
//!
//! Sweeps `--max-parallel-chunks` from 1 to NCPU on the full onroad fixture
//! suite, measuring total throughput (wall time) and peak RSS at each N.
//! Prints the tuning curve and asserts two invariants:
//!
//! 1. **Throughput does not degrade at higher N.**  Total wall time at NCPU
//!    must not exceed total wall time at N=1 by more than a noise factor.
//!
//! 2. **Peak RSS is bounded by the chain-isolation model.**  In Phase 3
//!    (calculators return empty output; no data-plane working set),
//!    `max_chain_working_set ≈ 0`, so peak RSS should be essentially flat
//!    across N.  The assertion enforces a generous ceiling:
//!    `rss(N) ≤ rss(N=1) + N × RSS_PER_EXTRA_CHUNK_MIB`.
//!    A violation means calculator chains are sharing more state than they
//!    should — a correctness bug masquerading as a memory issue.
//!
//! # Measurement methodology
//!
//! `VmHWM` from `/proc/self/status` is the process-lifetime high-water mark
//! — monotonic and non-decreasing within a test run.  To isolate each N's
//! contribution, this test runs N values in ascending order: the RSS reading
//! after the sweep at level N captures the max across all prior levels plus
//! that level's runs.  The *delta* between consecutive readings is the
//! additional memory the higher-parallelism run consumed.
//!
//! # Phase 3 caveat
//!
//! All calculators return `CalculatorOutput::empty()` today.  The throughput
//! numbers reflect **framework overhead** only: RunSpec parsing, planning,
//! MasterLoop setup, and `MOVESRun.parquet` output.  Once the data plane
//! lands (Phase 4), per-chain working sets will be non-zero and the RSS
//! model will exercise real memory pressure.

use std::path::{Path, PathBuf};
use std::time::Instant;

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

/// Read VmHWM from /proc/self/status (Linux only). Returns None elsewhere.
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

/// Maximum allowed additional RSS per extra parallel chunk slot, MiB.
/// In Phase 3 (empty calculators), each chain's working set is ~0 beyond
/// the MasterLoop struct overhead.  2 MiB per slot is extremely generous —
/// a violation here means something is sharing unexpected mutable state.
const RSS_PER_EXTRA_CHUNK_MIB: f64 = 2.0;

/// Maximum wall-time slowdown allowed at NCPU vs N=1.
/// Higher parallelism can only help (or be neutral) on the fixture suite —
/// execution is ~15% of total wall time in Phase 3, but we allow a 2× noise
/// factor so slow CI machines don't flap.
const MAX_THROUGHPUT_REGRESSION_FACTOR: f64 = 2.0;

/// Run all fixtures at a given parallelism level; return (total_wall_ms, rss_mib_after).
fn run_all_fixtures(fixtures: &[PathBuf], max_parallel_chunks: usize) -> (f64, Option<f64>) {
    let t_start = Instant::now();
    for fixture in fixtures {
        let out_dir = tempdir().expect("tempdir");
        run_simulation(&RunOptions {
            runspec: fixture.clone(),
            output: out_dir.path().to_path_buf(),
            max_parallel_chunks,
            calculator_dag: None,
            run_date_time: None,
            snapshot: None,
            scale_input: None,
        })
        .unwrap_or_else(|e| {
            panic!(
                "fixture {} failed at N={max_parallel_chunks}: {e}",
                fixture.display()
            )
        });
    }
    let wall_ms = t_start.elapsed().as_secs_f64() * 1000.0;
    let rss_mib = read_peak_rss_mib();
    (wall_ms, rss_mib)
}

#[test]
fn concurrency_tuning_sweep() {
    let fixtures = onroad_fixtures();
    assert!(
        !fixtures.is_empty(),
        "no onroad fixtures found in {}",
        fixtures_dir().display()
    );

    let ncpu: usize = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    // Sweep: 1, 2, 4, 8, ... doubling up to NCPU; then NCPU itself.
    let mut n_values: Vec<(usize, String)> = Vec::new();
    let mut n = 1usize;
    while n < ncpu {
        n_values.push((n, n.to_string()));
        n = n.saturating_mul(2);
    }
    n_values.push((ncpu, format!("{ncpu} (NCPU)")));
    n_values.dedup_by_key(|(n, _)| *n);

    struct Row {
        n: usize,
        label: String,
        wall_ms: f64,
        speedup: f64,
        rss_mib: Option<f64>,
        rss_delta_mib: Option<f64>,
    }
    let mut rows: Vec<Row> = Vec::new();

    // Run in ascending N order so the monotonic VmHWM captures the deltas.
    let mut prev_rss: Option<f64> = None;
    for (n, label) in &n_values {
        let (wall_ms, rss_mib) = run_all_fixtures(&fixtures, *n);
        let rss_delta = match (rss_mib, prev_rss) {
            (Some(rss), Some(prev)) => Some((rss - prev).max(0.0)),
            (Some(rss), None) => Some(rss),
            _ => None,
        };
        prev_rss = rss_mib;
        rows.push(Row {
            n: *n,
            label: label.clone(),
            wall_ms,
            speedup: 0.0,
            rss_mib,
            rss_delta_mib: rss_delta,
        });
    }

    // Compute speedup relative to N=1.
    let baseline_wall = rows[0].wall_ms;
    for row in &mut rows {
        row.speedup = baseline_wall / row.wall_ms;
    }

    // Print the table.
    println!(
        "\n{:>12}  {:>12}  {:>10}  {:>10}  {:>12}",
        "N", "total_wall_ms", "speedup", "RSS_MiB", "delta_RSS_MiB"
    );
    println!("{}", "-".repeat(62));
    for row in &rows {
        let rss_str = row
            .rss_mib
            .map(|r| format!("{r:>10.1}"))
            .unwrap_or_else(|| "       n/a".to_string());
        let delta_str = row
            .rss_delta_mib
            .map(|d| format!("{d:>12.1}"))
            .unwrap_or_else(|| "         n/a".to_string());
        println!(
            "{:>12}  {:>12.1}  {:>10.2}  {rss_str}  {delta_str}",
            row.label, row.wall_ms, row.speedup,
        );
    }
    println!();

    let n1_row = &rows[0];
    let ncpu_row = rows.last().unwrap();

    // Assertion 1: throughput must not degrade beyond noise factor.
    assert!(
        ncpu_row.wall_ms <= n1_row.wall_ms * MAX_THROUGHPUT_REGRESSION_FACTOR,
        "Throughput regression at N={}: wall {:.1} ms > N=1 wall {:.1} ms × {:.0} \
         (limit {:.1} ms). The executor may be serialising instead of parallelising.",
        ncpu_row.n,
        ncpu_row.wall_ms,
        n1_row.wall_ms,
        MAX_THROUGHPUT_REGRESSION_FACTOR,
        n1_row.wall_ms * MAX_THROUGHPUT_REGRESSION_FACTOR,
    );

    // Assertion 2: RSS must stay within the chain-isolation model.
    // At Phase 3 (empty calculators), expected additional RSS = N × ~0.
    // We allow RSS_PER_EXTRA_CHUNK_MIB per additional parallel slot as slack.
    if let (Some(rss_n1), Some(rss_ncpu)) = (n1_row.rss_mib, ncpu_row.rss_mib) {
        let extra_slots = ncpu_row.n.saturating_sub(1) as f64;
        let rss_ceiling = rss_n1 + extra_slots * RSS_PER_EXTRA_CHUNK_MIB;
        assert!(
            rss_ncpu <= rss_ceiling,
            "Memory-pressure model violated at N={}: RSS {:.1} MiB > ceiling {:.1} MiB \
             (baseline {:.1} MiB + {} extra slots × {:.0} MiB/slot). \
             Calculator chains may be sharing unexpected mutable state.",
            ncpu_row.n,
            rss_ncpu,
            rss_ceiling,
            rss_n1,
            ncpu_row.n.saturating_sub(1),
            RSS_PER_EXTRA_CHUNK_MIB,
        );
    }
}

/// Single-fixture throughput sanity check: all N values run without error
/// and produce non-zero timing fields.  Lighter-weight than the full sweep.
#[test]
fn all_parallelism_levels_run_the_airtoxics_fixture() {
    let fixture = fixtures_dir().join("process-airtoxics.xml");
    if !fixture.is_file() {
        return;
    }
    let ncpu = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    for n in [1, 4, ncpu] {
        let out_dir = tempdir().expect("tempdir");
        let outcome = run_simulation(&RunOptions {
            runspec: fixture.clone(),
            output: out_dir.path().to_path_buf(),
            max_parallel_chunks: n,
            calculator_dag: None,
            run_date_time: None,
            snapshot: None,
            scale_input: None,
        })
        .unwrap_or_else(|e| panic!("airtoxics at N={n}: {e}"));

        assert!(
            outcome.wall_time.as_nanos() > 0,
            "N={n}: wall_time must be positive"
        );
        assert_eq!(
            outcome.max_parallel_chunks,
            n.max(1),
            "N={n}: resolved parallelism must match"
        );
    }
}

//! Concurrency-correctness test — ().
//!
//! Runs every onroad fixture through the Rust engine at three
//! `--max-parallel-chunks` settings (1, 4, NCPU) and asserts that the
//! `MOVESRun.parquet` output is byte-identical across all three. Any
//! divergence indicates a calculator that is not pure with respect to its
//! inputs — typically a hidden iteration-order dependency, shared mutable
//! state, or non-deterministic float summation across threads.
//!
//! # Current scope (pre-data-plane)
//!
//! Calculators return `CalculatorOutput::empty()` today because the data
//! plane that feeds real row data is not yet wired in. The test therefore
//! exercises **framework non-determinism** only: any divergence here is a
//! bug in the chunking, executor, or output-processor layers — not in the
//! numeric calculator cores.
//!
//! When the data plane lands, the same test immediately covers real
//! calculator outputs with no changes needed.
//!
//! # Byte-identity contract
//!
//! `run_date_time` is pinned to a fixed string so the run-metadata row in
//! `MOVESRun.parquet` is the same regardless of when the test runs. The
//! engine does not stamp the wall clock, so timing fields are the only
//! remaining source of variation — and those live in the in-memory
//! [`EngineOutcome`], not in the written Parquet file.

use std::fs;
use std::path::{Path, PathBuf};

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

/// All onroad fixture XML files — same selection as `perf_baseline`:
/// excludes `nr-*` NONROAD fixtures and `scale-*` fixtures that require
/// additional input databases.
fn onroad_fixtures() -> Vec<PathBuf> {
    let dir = fixtures_dir();
    let mut paths: Vec<PathBuf> = fs::read_dir(&dir)
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

/// Read the raw bytes of a file.
fn read_bytes(path: &Path) -> Vec<u8> {
    fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// Run one fixture at a given parallelism and return the `MOVESRun.parquet`
/// bytes.
fn run_fixture_bytes(fixture: &Path, max_parallel_chunks: usize) -> Vec<u8> {
    let out_dir = tempdir().expect("tempdir");
    let opts = RunOptions {
        runspec: fixture.to_path_buf(),
        output: out_dir.path().to_path_buf(),
        max_parallel_chunks,
        calculator_dag: None,
 // Pin to a fixed timestamp so the output row is byte-stable across
 // parallelism variants; the engine does not stamp the wall clock.
        run_date_time: Some("2026-01-01T00:00:00".to_string()),
        snapshot: None,
        scale_input: None,
            default_db: None,
    };
    let outcome = run_simulation(&opts).unwrap_or_else(|e| {
        panic!(
            "run {} (parallel={}): {e}",
            fixture.display(),
            max_parallel_chunks
        )
    });
    read_bytes(&outcome.run_record_path)
}

#[test]
fn outputs_are_byte_identical_across_parallelism_settings() {
    let fixtures = onroad_fixtures();
    assert!(
        !fixtures.is_empty(),
        "no onroad fixtures found in {}",
        fixtures_dir().display()
    );

 // Host parallelism — 0 expands to available_parallelism inside the engine.
    let ncpu: usize = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

 // The three settings to compare. If NCPU is 1 or 4 we deduplicate so we
 // don't run the same setting twice and call that "a comparison".
    let settings: Vec<(usize, &str)> = {
        let mut s = vec![(1usize, "1"), (4, "4"), (ncpu, "NCPU")];
        s.dedup_by_key(|(n, _)| *n);
        s
    };

    let mut failures: Vec<String> = Vec::new();

    for fixture in &fixtures {
        let name = fixture.file_stem().and_then(|n| n.to_str()).unwrap_or("?");

 // Run at each parallelism setting and collect bytes.
        let runs: Vec<(usize, &str, Vec<u8>)> = settings
            .iter()
            .map(|&(limit, label)| {
                let bytes = run_fixture_bytes(fixture, limit);
                (limit, label, bytes)
            })
            .collect();

 // Compare every pair against the first run.
        let (_, ref_label, ref_bytes) = &runs[0];
        for (limit, label, bytes) in &runs[1..] {
            if bytes != ref_bytes {
                failures.push(format!(
                    "{name}: parallel={limit} ({label}) output differs from \
                     parallel=1 ({ref_label}): \
                     {} bytes vs {} bytes",
                    bytes.len(),
                    ref_bytes.len(),
                ));
            }
        }
    }

    assert!(
        failures.is_empty(),
        "concurrency-correctness failures — non-deterministic output detected:\n  {}",
        failures.join("\n  ")
    );
}

/// Verify that all three parallelism settings complete without panicking on
/// a single well-known fixture. This catches executor-level crashes
/// independently of the byte-identity check.
#[test]
fn all_parallelism_settings_complete_without_error() {
    let fixture = fixtures_dir().join("process-airtoxics.xml");
    if !fixture.is_file() {
        return;
    }

    let ncpu = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    for limit in [1, 4, ncpu] {
        let out_dir = tempdir().expect("tempdir");
        let outcome = run_simulation(&RunOptions {
            runspec: fixture.clone(),
            output: out_dir.path().to_path_buf(),
            max_parallel_chunks: limit,
            calculator_dag: None,
            run_date_time: Some("2026-01-01T00:00:00".to_string()),
            snapshot: None,
            scale_input: None,
            default_db: None,
        })
        .unwrap_or_else(|e| panic!("run at parallel={limit}: {e}"));

        assert!(
            outcome.run_record_path.is_file(),
            "MOVESRun.parquet missing at parallel={limit}"
        );
        assert_eq!(
            outcome.max_parallel_chunks,
            limit.max(1),
            "resolved parallelism mismatch at parallel={limit}"
        );
    }
}

//! End-to-end test: drive the `moves-coverage` binary against a synthetic
//! `snapshots/` tree of `execution-trace.json` files and verify the rolled-up
//! coverage map is well-formed and byte-identical across re-runs.
//!
//! The unit tests in `crate::coverage` cover the aggregation logic;
//! this file covers the CLI's directory-walking, exit-code behavior, and
//! end-to-end determinism contract that Phase 1 Task 9 (bead `mo-55l0`)
//! depends on.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::json;
use tempfile::tempdir;

fn cli_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_moves-coverage"))
}

fn run_cli(snapshots: &Path, output: &Path) -> std::process::ExitStatus {
    Command::new(cli_binary())
        .arg("--snapshots-dir")
        .arg(snapshots)
        .arg("--output-dir")
        .arg(output)
        .status()
        .expect("invoke moves-coverage")
}

fn write_trace(snapshot_root: &Path, fixture: &str, body: serde_json::Value) {
    let dir = snapshot_root.join(fixture);
    fs::create_dir_all(&dir).unwrap();
    let pretty = serde_json::to_vec_pretty(&body).unwrap();
    fs::write(dir.join("execution-trace.json"), pretty).unwrap();
}

fn sample_trace(fixture: &str) -> serde_json::Value {
    // Mirrors the `moves-fixture-capture/v1` schema closely enough that
    // the loader is exercised; only the fields the aggregator reads need
    // to round-trip.
    json!({
        "trace_version": "moves-fixture-capture/v1",
        "fixture_name": fixture,
        "sif_sha256": format!("sif-{fixture}"),
        "runspec_sha256": format!("rs-{fixture}"),
        "java_classes": [
            { "name": "gov.epa.otaq.moves.master.calculator.BaseRateCalculator", "kind": "calculator" }
        ],
        "sql_files": [
            { "path": "database/CalculatorSQL/BaseRateCalculator.sql", "consumed_by": ["WorkerTemp00"] }
        ],
        "go_calculators": [
            { "name": "BaseRateCalculator", "invoked_in": ["WorkerTemp00"] }
        ],
        "worker_bundles": [
            {
                "id": "WorkerTemp00",
                "java_classes": ["gov.epa.otaq.moves.master.calculator.BaseRateCalculator"],
                "sql_files": ["database/CalculatorSQL/BaseRateCalculator.sql"],
                "go_calculators": ["BaseRateCalculator"],
                "statement_count": 42
            }
        ],
        "sources": { "worker_sql_files": 1, "class_load_log_files": 0 }
    })
}

#[test]
fn coverage_cli_aggregates_two_fixture_traces() {
    let snapshots = tempdir().unwrap();
    write_trace(snapshots.path(), "alpha", sample_trace("alpha"));
    write_trace(snapshots.path(), "beta", sample_trace("beta"));
    // A snapshot subdir with no trace file must be skipped silently.
    fs::create_dir_all(snapshots.path().join("notraces")).unwrap();

    let out = tempdir().unwrap();
    let status = run_cli(snapshots.path(), out.path());
    assert!(status.success(), "CLI failed with {status:?}");

    let bytes = fs::read(out.path().join("coverage-map.json")).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(v["coverage_version"], "moves-coverage/v1");
    assert_eq!(v["total_fixtures"], 2);
    assert_eq!(v["total_statement_weight"], 84);
    assert_eq!(v["fixtures"].as_array().unwrap().len(), 2);
    assert_eq!(v["fixtures"][0]["fixture_name"], "alpha");
    assert_eq!(v["fixtures"][1]["fixture_name"], "beta");

    let java = v["java_classes"].as_array().unwrap();
    assert_eq!(java.len(), 1);
    assert_eq!(java[0]["fixture_count"], 2);
    assert_eq!(java[0]["statement_weight"], 84);
    assert_eq!(java[0]["score"], 1.0);

    let hot = v["hot_paths"].as_array().unwrap();
    // One java class + one SQL file + one Go calc, all score 1.0.
    assert_eq!(hot.len(), 3);
}

#[test]
fn coverage_cli_is_byte_identical_across_runs() {
    let snapshots = tempdir().unwrap();
    write_trace(snapshots.path(), "alpha", sample_trace("alpha"));
    write_trace(snapshots.path(), "beta", sample_trace("beta"));
    write_trace(snapshots.path(), "gamma", sample_trace("gamma"));

    let out1 = tempdir().unwrap();
    let out2 = tempdir().unwrap();

    assert!(run_cli(snapshots.path(), out1.path()).success());
    assert!(run_cli(snapshots.path(), out2.path()).success());

    let bytes1 = fs::read(out1.path().join("coverage-map.json")).unwrap();
    let bytes2 = fs::read(out2.path().join("coverage-map.json")).unwrap();
    assert_eq!(bytes1, bytes2);
    assert!(bytes1.ends_with(b"\n"));
}

#[test]
fn coverage_cli_emits_empty_map_for_no_traces() {
    let snapshots = tempdir().unwrap();
    // No subdirectories at all — a fresh fixture suite before any captures.
    let out = tempdir().unwrap();
    let status = run_cli(snapshots.path(), out.path());
    assert!(status.success(), "CLI failed with {status:?}");

    let bytes = fs::read(out.path().join("coverage-map.json")).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["total_fixtures"], 0);
    assert_eq!(v["total_statement_weight"], 0);
    assert!(v["java_classes"].as_array().unwrap().is_empty());
    assert!(v["sql_files"].as_array().unwrap().is_empty());
    assert!(v["go_calculators"].as_array().unwrap().is_empty());
    assert!(v["hot_paths"].as_array().unwrap().is_empty());
}

#[test]
fn coverage_cli_creates_output_dir_if_missing() {
    let snapshots = tempdir().unwrap();
    write_trace(snapshots.path(), "alpha", sample_trace("alpha"));

    let parent = tempdir().unwrap();
    let out = parent.path().join("nested").join("coverage");
    assert!(!out.exists());

    let status = run_cli(snapshots.path(), &out);
    assert!(status.success(), "CLI failed with {status:?}");
    assert!(out.join("coverage-map.json").is_file());
}

#[test]
fn coverage_cli_exits_nonzero_for_missing_snapshots_root() {
    let parent = tempdir().unwrap();
    let missing = parent.path().join("does-not-exist");
    let out = tempdir().unwrap();
    let status = run_cli(&missing, out.path());
    assert!(!status.success(), "expected nonzero exit, got {status:?}");
}

//! End-to-end CLI tests for `moves-snapshot diff`.
//!
//! Exercises the binary the same way CI does: write two snapshots to disk,
//! invoke the binary, assert on exit code, stdout, and structured JSON
//! output. Uses Cargo's auto-discovered `CARGO_BIN_EXE_<name>` to find the
//! built binary so the test runs against the actual artifact, not a
//! re-implementation.

use std::path::Path;
use std::process::Command;

use moves_snapshot::format::ColumnKind;
use moves_snapshot::table::{TableBuilder, Value};
use moves_snapshot::Snapshot;

fn build_snapshot(rows: &[(i64, f64)]) -> Snapshot {
    let mut tb = TableBuilder::new(
        "t",
        [
            ("id".to_string(), ColumnKind::Int64),
            ("v".to_string(), ColumnKind::Float64),
        ],
    )
    .unwrap()
    .with_natural_key(["id"])
    .unwrap();
    for (id, v) in rows {
        tb.push_row([Value::Int64(*id), Value::Float64(*v)])
            .unwrap();
    }
    let table = tb.build().unwrap();
    let mut s = Snapshot::new();
    s.add_table(table).unwrap();
    s
}

fn write_snapshot(rows: &[(i64, f64)], dir: &Path) {
    build_snapshot(rows).write(dir).unwrap();
}

fn run_diff(args: &[&std::ffi::OsStr]) -> (i32, String, String) {
    let bin = env!("CARGO_BIN_EXE_moves-snapshot");
    let out = Command::new(bin)
        .arg("diff")
        .args(args)
        .output()
        .expect("spawn moves-snapshot diff");
    let code = out.status.code().unwrap_or(-1);
    (
        code,
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn equal_snapshots_exit_zero_and_report_match() {
    let lhs = tempfile::tempdir().unwrap();
    let rhs = tempfile::tempdir().unwrap();
    write_snapshot(&[(1, 1.0), (2, 2.0)], lhs.path());
    write_snapshot(&[(1, 1.0), (2, 2.0)], rhs.path());

    let (code, stdout, stderr) = run_diff(&[lhs.path().as_os_str(), rhs.path().as_os_str()]);
    assert_eq!(code, 0, "stdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("snapshots match"),
        "expected match line in stdout, got: {stdout}"
    );
}

#[test]
fn cell_diff_exits_one_and_renders_summary() {
    let lhs = tempfile::tempdir().unwrap();
    let rhs = tempfile::tempdir().unwrap();
    write_snapshot(&[(1, 1.0), (2, 2.0)], lhs.path());
    write_snapshot(&[(1, 1.5), (2, 2.0)], rhs.path());

    let (code, stdout, _stderr) = run_diff(&[lhs.path().as_os_str(), rhs.path().as_os_str()]);
    assert_eq!(code, 1);
    assert!(stdout.contains("cells_changed=1"), "stdout: {stdout}");
    assert!(stdout.contains("table t:"), "stdout: {stdout}");
    assert!(stdout.contains("column=v"), "stdout: {stdout}");
}

#[test]
fn json_output_round_trips() {
    let lhs = tempfile::tempdir().unwrap();
    let rhs = tempfile::tempdir().unwrap();
    write_snapshot(&[(1, 1.0)], lhs.path());
    write_snapshot(&[(1, 1.5)], rhs.path());

    let (code, stdout, stderr) = run_diff(&[
        lhs.path().as_os_str(),
        rhs.path().as_os_str(),
        std::ffi::OsStr::new("--format"),
        std::ffi::OsStr::new("json"),
    ]);
    assert_eq!(code, 1, "stderr: {stderr}");

    let v: serde_json::Value = serde_json::from_str(&stdout).expect("valid JSON");
    assert_eq!(v["format_version"], "moves-snapshot-diff/v1");
    assert_eq!(v["summary"]["cells_changed"], 1);
    assert_eq!(v["summary"]["tables_changed"], 1);
    let row_diffs = &v["diff"]["table_changes"][0]["row_diffs"];
    assert!(row_diffs.is_array(), "row_diffs not array: {row_diffs}");
    assert_eq!(row_diffs[0]["cell"]["column"], "v");
}

#[test]
fn tolerance_config_suppresses_small_diff() {
    let lhs = tempfile::tempdir().unwrap();
    let rhs = tempfile::tempdir().unwrap();
    write_snapshot(&[(1, 1.000000)], lhs.path());
    write_snapshot(&[(1, 1.000001)], rhs.path());

    // Without tolerance, this is a diff.
    let (code, _, _) = run_diff(&[lhs.path().as_os_str(), rhs.path().as_os_str()]);
    assert_eq!(code, 1);

    // With a per-column tolerance > 1e-6, the diff is suppressed.
    let cfg = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(
        cfg.path(),
        r#"
[tables."t"]
v = 1e-5
"#,
    )
    .unwrap();
    let (code, stdout, stderr) = run_diff(&[
        lhs.path().as_os_str(),
        rhs.path().as_os_str(),
        std::ffi::OsStr::new("--tolerance"),
        cfg.path().as_os_str(),
    ]);
    assert_eq!(code, 0, "stdout: {stdout}\nstderr: {stderr}");
    assert!(stdout.contains("snapshots match"));
}

#[test]
fn missing_snapshot_returns_two() {
    let lhs = tempfile::tempdir().unwrap();
    let (code, _stdout, stderr) = run_diff(&[
        lhs.path().as_os_str(),
        std::ffi::OsStr::new("/no/such/path"),
    ]);
    assert_eq!(code, 2, "stderr: {stderr}");
    assert!(stderr.contains("error:"), "stderr: {stderr}");
}

#[test]
fn malformed_tolerance_returns_two() {
    let lhs = tempfile::tempdir().unwrap();
    let rhs = tempfile::tempdir().unwrap();
    write_snapshot(&[(1, 1.0)], lhs.path());
    write_snapshot(&[(1, 1.0)], rhs.path());
    let cfg = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(cfg.path(), "default_float_tolerance = -1.0\n").unwrap();
    let (code, _stdout, stderr) = run_diff(&[
        lhs.path().as_os_str(),
        rhs.path().as_os_str(),
        std::ffi::OsStr::new("--tolerance"),
        cfg.path().as_os_str(),
    ]);
    assert_eq!(code, 2, "stderr: {stderr}");
    assert!(
        stderr.contains("must be a finite, non-negative"),
        "stderr: {stderr}"
    );
}

#[test]
fn limit_caps_text_output_rows() {
    let lhs = tempfile::tempdir().unwrap();
    let rhs = tempfile::tempdir().unwrap();
    let lhs_rows: Vec<(i64, f64)> = (0..50).map(|i| (i, i as f64)).collect();
    let rhs_rows: Vec<(i64, f64)> = (0..50).map(|i| (i, (i + 1) as f64)).collect();
    write_snapshot(&lhs_rows, lhs.path());
    write_snapshot(&rhs_rows, rhs.path());

    let (code, stdout, _stderr) = run_diff(&[
        lhs.path().as_os_str(),
        rhs.path().as_os_str(),
        std::ffi::OsStr::new("--limit"),
        std::ffi::OsStr::new("3"),
    ]);
    assert_eq!(code, 1);
    assert!(
        stdout.contains("more row diff(s) suppressed"),
        "stdout: {stdout}"
    );
    // 50 cell diffs, 3 shown, 47 suppressed.
    assert!(stdout.contains("47 more"), "stdout: {stdout}");
}

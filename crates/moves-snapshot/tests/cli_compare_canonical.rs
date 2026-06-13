//! End-to-end CLI tests for `compare-canonical`.
//!
//! Creates a synthetic snapshot containing a `db__movesoutput__movesoutput`
//! table (matching the real MOVES output schema: Int64 pollutantID, Float64
//! emissionQuant) and verifies that `compare-canonical` reads it correctly.

use std::process::Command;

use moves_snapshot::format::ColumnKind;
use moves_snapshot::table::{TableBuilder, Value};
use moves_snapshot::Snapshot;
use tempfile::tempdir;

fn build_movesoutput_snapshot(rows: &[(i64, f64)]) -> Snapshot {
    let mut tb = TableBuilder::new(
        "db__movesoutput__movesoutput",
        [
            ("pollutantID".to_string(), ColumnKind::Int64),
            ("emissionQuant".to_string(), ColumnKind::Float64),
        ],
    )
    .unwrap()
    .with_natural_key(["pollutantID"])
    .unwrap();
    for (pid, eq) in rows {
        tb.push_row([Value::Int64(*pid), Value::Float64(*eq)])
            .unwrap();
    }
    let table = tb.build().unwrap();
    let mut s = Snapshot::new();
    s.add_table(table).unwrap();
    s
}

fn run_compare(args: &[&str]) -> (i32, String, String) {
    let bin = env!("CARGO_BIN_EXE_compare-canonical");
    let out = Command::new(bin)
        .args(args)
        .output()
        .expect("spawn compare-canonical");
    let code = out.status.code().unwrap_or(-1);
    (
        code,
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn no_snapshot_no_moves_rs_produces_empty_table() {
    // A genuinely-absent canonical snapshot (path does not exist) is the
    // "no canonical data" case → empty comparison, exit 0. A *present* path
    // that fails to load is a hard error (covered separately); see the
    // `args.canonical.exists()` branch in compare-canonical.
    let empty = tempdir().unwrap();
    let absent = empty.path().join("no-such-snapshot");
    let mrs = tempdir().unwrap();
    let (code, stdout, _stderr) = run_compare(&[
        "--canonical",
        absent.to_str().unwrap(),
        "--moves-rs",
        mrs.path().to_str().unwrap(),
        "--fixture",
        "test-fixture",
        "--format",
        "text",
    ]);
    assert_eq!(code, 0, "exit code should be 0");
    assert!(
        stdout.contains("no emission data"),
        "empty table placeholder expected in: {stdout}"
    );
}

#[test]
fn canonical_data_appears_in_text_output() {
    let snap_dir = tempdir().unwrap();
    build_movesoutput_snapshot(&[(2, 100.0), (3, 50.0)])
        .write(snap_dir.path())
        .unwrap();
    let mrs = tempdir().unwrap();

    let (code, stdout, _) = run_compare(&[
        "--canonical",
        snap_dir.path().to_str().unwrap(),
        "--moves-rs",
        mrs.path().to_str().unwrap(),
        "--fixture",
        "smoke",
        "--format",
        "text",
    ]);
    assert_eq!(code, 0);
    assert!(
        stdout.contains("| 2 |"),
        "pollutant 2 row missing: {stdout}"
    );
    assert!(
        stdout.contains("| 3 |"),
        "pollutant 3 row missing: {stdout}"
    );
    // moves.rs produced no output → delta equals -canonical
    assert!(
        stdout.contains("-1.0e2") || stdout.contains("-100"),
        "expected negative delta for pollutant 2: {stdout}"
    );
}

#[test]
fn canonical_data_json_output_has_correct_fields() {
    let snap_dir = tempdir().unwrap();
    build_movesoutput_snapshot(&[(1, 42.0)])
        .write(snap_dir.path())
        .unwrap();
    let mrs = tempdir().unwrap();

    let (code, stdout, _) = run_compare(&[
        "--canonical",
        snap_dir.path().to_str().unwrap(),
        "--moves-rs",
        mrs.path().to_str().unwrap(),
        "--fixture",
        "json-test",
        "--canonical-wall",
        "10.0",
        "--moves-rs-wall",
        "2.0",
        "--format",
        "json",
    ]);
    assert_eq!(code, 0);

    let v: serde_json::Value = serde_json::from_str(&stdout).expect("valid JSON");
    assert_eq!(v["fixture"], "json-test");
    assert_eq!(v["pollutant_count"], 1);
    assert!((v["speedup"].as_f64().unwrap() - 5.0).abs() < 1e-9);
    assert_eq!(
        v["canonical_row_count"], 1,
        "canonical_row_count missing or wrong"
    );
    assert_eq!(
        v["moves_rs_row_count"], 0,
        "moves_rs_row_count missing or wrong"
    );
    assert!(
        v["row_count_ratio"].as_f64().unwrap().abs() < 1e-9,
        "row_count_ratio should be 0 when no moves.rs output"
    );

    let row = &v["rows"][0];
    assert_eq!(row["pollutant_id"], 1);
    assert!((row["canonical_emission_quant"].as_f64().unwrap() - 42.0).abs() < 1e-9);
    assert!((row["moves_rs_emission_quant"].as_f64().unwrap()).abs() < 1e-9);
    assert!((row["delta"].as_f64().unwrap() + 42.0).abs() < 1e-9);
}

#[test]
fn timing_columns_populated_in_text_output() {
    let snap_dir = tempdir().unwrap();
    build_movesoutput_snapshot(&[(1, 10.0)])
        .write(snap_dir.path())
        .unwrap();
    let mrs = tempdir().unwrap();

    let (code, stdout, _) = run_compare(&[
        "--canonical",
        snap_dir.path().to_str().unwrap(),
        "--moves-rs",
        mrs.path().to_str().unwrap(),
        "--fixture",
        "timing-test",
        "--canonical-wall",
        "30.0",
        "--moves-rs-wall",
        "1.5",
        "--format",
        "text",
    ]);
    assert_eq!(code, 0);
    assert!(
        stdout.contains("30.0"),
        "canonical wall time missing: {stdout}"
    );
    assert!(
        stdout.contains("1.5"),
        "moves.rs wall time missing: {stdout}"
    );
    assert!(stdout.contains("20.0"), "speedup 20.0× missing: {stdout}");
    assert!(
        stdout.contains("Canonical rows:"),
        "row count line missing: {stdout}"
    );
}

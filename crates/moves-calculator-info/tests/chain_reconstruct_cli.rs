//! End-to-end test: drive the `moves-chain-reconstruct` binary against a
//! synthetic `CalculatorInfo.txt` + Java source tree and verify the DAG
//! JSON is well-formed and byte-identical across re-runs.
//!
//! The unit tests in `crate::chain` cover the DAG construction logic;
//! this file covers the CLI's argument parsing, source-dir walking, exit
//! codes, and the end-to-end determinism contract that Phase 1 Task 10
//! (bead `mo-78un`) depends on.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;
use tempfile::tempdir;

fn cli_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_moves-chain-reconstruct"))
}

fn run_cli(
    calculator_info: &Path,
    source_dir: Option<&Path>,
    output: &Path,
) -> std::process::ExitStatus {
    let mut cmd = Command::new(cli_binary());
    cmd.arg("--calculator-info")
        .arg(calculator_info)
        .arg("--output-dir")
        .arg(output);
    if let Some(d) = source_dir {
        cmd.arg("--source-dir").arg(d);
    }
    cmd.status().expect("invoke moves-chain-reconstruct")
}

const SAMPLE_INFO: &str =
    "// Registration\tOutputPollutantName\tOutputPollutantID\tProcessName\tProcessID\tModuleName
// Subscribe\tModuleName\tProcessName\tProcessID\tGranularity\tPriority
// Chain\tOutputModuleName\tInputModuleName
Registration\tTotal Gaseous Hydrocarbons\t1\tRunning Exhaust\t1\tBaseRateCalculator
Registration\tCarbon Monoxide (CO)\t2\tRunning Exhaust\t1\tBaseRateCalculator
Subscribe\tBaseRateCalculator\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR
Chain\tHCSpeciationCalculator\tBaseRateCalculator
";

#[test]
fn writes_dag_json_for_minimal_input() {
    let dir = tempdir().unwrap();
    let info_path = dir.path().join("CalculatorInfo.txt");
    fs::write(&info_path, SAMPLE_INFO).unwrap();
    let out_dir = dir.path().join("out");

    let status = run_cli(&info_path, None, &out_dir);
    assert!(status.success(), "exit status: {status:?}");

    let out_path = out_dir.join("calculator-dag.json");
    assert!(out_path.exists(), "expected DAG JSON at {out_path:?}");
    let body: Value = serde_json::from_slice(&fs::read(&out_path).unwrap()).unwrap();
    assert_eq!(body["schema"], "moves-calculator-dag/v1");
    assert_eq!(body["counts"]["registrations"], 2);
    assert_eq!(body["counts"]["subscriptions"], 1);
    assert_eq!(body["counts"]["chains"], 1);
    assert_eq!(body["counts"]["modules"], 2);
    // BaseRateCalculator subscribes, HCSpeciation chains.
    let modules = body["modules"].as_array().unwrap();
    let names: Vec<&str> = modules
        .iter()
        .map(|m| m["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["BaseRateCalculator", "HCSpeciationCalculator"]);
    // chain_templates rolls up BaseRateCalculator's chain into one entry.
    assert_eq!(body["chain_templates"].as_array().unwrap().len(), 1);
}

#[test]
fn byte_identical_across_re_runs() {
    let dir = tempdir().unwrap();
    let info_path = dir.path().join("CalculatorInfo.txt");
    fs::write(&info_path, SAMPLE_INFO).unwrap();
    let out_dir = dir.path().join("out");

    let status1 = run_cli(&info_path, None, &out_dir);
    assert!(status1.success());
    let bytes1 = fs::read(out_dir.join("calculator-dag.json")).unwrap();

    let status2 = run_cli(&info_path, None, &out_dir);
    assert!(status2.success());
    let bytes2 = fs::read(out_dir.join("calculator-dag.json")).unwrap();
    assert_eq!(bytes1, bytes2);
}

#[test]
fn missing_calculator_info_is_error_exit() {
    let dir = tempdir().unwrap();
    let out_dir = dir.path().join("out");
    let status = run_cli(&dir.path().join("does-not-exist.txt"), None, &out_dir);
    assert!(!status.success(), "expected non-zero exit, got: {status:?}");
}

#[test]
fn malformed_directive_is_error_exit() {
    let dir = tempdir().unwrap();
    let info_path = dir.path().join("CalculatorInfo.txt");
    fs::write(&info_path, "Registration\ttoo\tfew\tfields\n").unwrap();
    let out_dir = dir.path().join("out");
    let status = run_cli(&info_path, None, &out_dir);
    assert!(!status.success(), "expected non-zero exit, got: {status:?}");
}

#[test]
fn source_dir_fills_in_missing_subscription() {
    // Build a tiny "MOVES source" tree with a calculator that registers
    // but does NOT appear in CalculatorInfo.txt's Subscribe directives.
    // The Java scan should fill in the missing subscription.
    let dir = tempdir().unwrap();
    let info_path = dir.path().join("CalculatorInfo.txt");
    fs::write(
        &info_path,
        "Registration\tCO\t2\tStart Exhaust\t2\tBasicStartCalc\n",
    )
    .unwrap();

    let src_dir = dir.path().join("src");
    let calc_dir = src_dir.join("gov/epa/otaq/moves/master/implementation/ghg");
    fs::create_dir_all(&calc_dir).unwrap();
    fs::write(
        calc_dir.join("BasicStartCalc.java"),
        r#"
package gov.epa.otaq.moves.master.implementation.ghg;
public class BasicStartCalc extends GenericCalculatorBase {
    public BasicStartCalc() {
        super(new String[]{"202"},
              MasterLoopGranularity.YEAR,
              0,
              "database/Foo.sql",
              null);
    }
}
"#,
    )
    .unwrap();

    let out_dir = dir.path().join("out");
    let status = run_cli(&info_path, Some(&src_dir), &out_dir);
    assert!(status.success());

    let body: Value =
        serde_json::from_slice(&fs::read(out_dir.join("calculator-dag.json")).unwrap()).unwrap();
    let modules = body["modules"].as_array().unwrap();
    let basic = modules
        .iter()
        .find(|m| m["name"] == "BasicStartCalc")
        .expect("module entry");
    assert_eq!(basic["subscribes_directly"], true);
    let subs = basic["subscriptions"].as_array().unwrap();
    assert_eq!(subs.len(), 1);
    assert_eq!(subs[0]["granularity"], "YEAR");
    assert_eq!(subs[0]["priority"], "EMISSION_CALCULATOR");
    assert_eq!(subs[0]["source"], "JavaSource");
    // Path is relative to source-dir.
    assert_eq!(
        basic["java_path"],
        "gov/epa/otaq/moves/master/implementation/ghg/BasicStartCalc.java"
    );
}

#[test]
fn output_dir_is_created_if_missing() {
    let dir = tempdir().unwrap();
    let info_path = dir.path().join("CalculatorInfo.txt");
    fs::write(&info_path, SAMPLE_INFO).unwrap();
    // Three levels deep, no parent dirs exist yet.
    let nested = dir.path().join("a/b/c");
    let status = run_cli(&info_path, None, &nested);
    assert!(status.success());
    assert!(nested.join("calculator-dag.json").exists());
}

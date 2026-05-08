//! End-to-end test: drive the `moves-fixture-capture` binary against a
//! synthetic captures directory and verify the resulting snapshot is
//! byte-identical across two independent invocations.
//!
//! This proves the bead's "deterministic given the same inputs" acceptance
//! criterion at the host-side level — the orchestration scripts that drive
//! the SIF live one layer up, and behavioral validation of MOVES execution
//! itself requires an HPC compute node (per mo-1s9o).

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::tempdir;

const CAPTURE_FILES: &[(&str, &[u8])] = &[
    (
        "databases/movesoutput/movesactivityoutput.schema.tsv",
        b"yearid\tint\tPRI\nmonthid\tint\tPRI\nactivity\tdouble\t\n",
    ),
    (
        "databases/movesoutput/movesactivityoutput.tsv",
        b"2020\t1\t100.5\n2020\t2\t150.25\n2020\t3\tNULL\n",
    ),
    (
        "databases/movesoutput/movesoutput.schema.tsv",
        b"id\tint\tPRI\nrate\tdecimal\t\n",
    ),
    (
        "databases/movesoutput/movesoutput.tsv",
        b"1\t0.001\n2\t0.002\n",
    ),
    // Default DB — should be excluded based on the RunSpec's <scaleinputdatabase>.
    (
        "databases/movesdb20241112/sourceusetype.schema.tsv",
        b"sourcetypeid\tint\tPRI\nname\tvarchar\t\n",
    ),
    (
        "databases/movesdb20241112/sourceusetype.tsv",
        b"21\tpassengercar\n",
    ),
    (
        "moves-temporary/SourceTypeYearVMT_2020.tbl",
        b"sourcetypeid\tyear\tvmt\n21\t2020\t1500000000\n",
    ),
    ("worker-folder/WorkerTemp00/Output.tbl", b"a\tb\n1\t2\n"),
];

const RUNSPEC_BODY: &[u8] = br#"<?xml version="1.0" encoding="UTF-8"?>
<runspec version="MOVES5">
  <description>Synthetic test RunSpec</description>
  <scaleinputdatabase servername="" databasename="movesdb20241112"/>
  <outputdatabase servername="" databasename="movesoutput"/>
</runspec>
"#;

const FIXTURE_LOCK: &[u8] = br#"# fixture-image.lock - synthetic for tests.
sif_path           = "characterization/apptainer/moves-fixture.sif"
sif_sha256         = "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100"
sif_bytes          = 1024
"#;

fn populate_captures(root: &Path) {
    for (rel, body) in CAPTURE_FILES {
        let path = root.join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&path, body).unwrap();
    }
}

fn cli_binary() -> PathBuf {
    // CARGO_BIN_EXE_<name> is set by Cargo for integration tests.
    PathBuf::from(env!("CARGO_BIN_EXE_moves-fixture-capture"))
}

fn run_cli(captures: &Path, runspec: &Path, lock: &Path, output: &Path) {
    let status = Command::new(cli_binary())
        .arg("--captures-dir")
        .arg(captures)
        .arg("--runspec")
        .arg(runspec)
        .arg("--sif-lockfile")
        .arg(lock)
        .arg("--output-dir")
        .arg(output)
        .status()
        .expect("invoke moves-fixture-capture");
    assert!(status.success(), "CLI failed with {status:?}");
}

fn collect_files(root: &Path) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn walk(root: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) {
    let mut entries: Vec<_> = fs::read_dir(dir).unwrap().filter_map(|e| e.ok()).collect();
    entries.sort_by_key(|e| e.path());
    for entry in entries {
        let path = entry.path();
        let ft = entry.file_type().unwrap();
        if ft.is_dir() {
            walk(root, &path, out);
        } else if ft.is_file() {
            let rel = path
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .into_owned();
            let bytes = fs::read(&path).unwrap();
            out.push((rel, bytes));
        }
    }
}

#[test]
fn cli_produces_byte_identical_snapshot_across_runs() {
    let captures1 = tempdir().unwrap();
    let captures2 = tempdir().unwrap();
    populate_captures(captures1.path());
    populate_captures(captures2.path());

    let scratch = tempdir().unwrap();
    let runspec = scratch.path().join("SampleRunSpec.xml");
    fs::write(&runspec, RUNSPEC_BODY).unwrap();
    let lock = scratch.path().join("fixture-image.lock");
    fs::write(&lock, FIXTURE_LOCK).unwrap();

    let out1 = tempdir().unwrap();
    let out2 = tempdir().unwrap();
    run_cli(captures1.path(), &runspec, &lock, out1.path());
    run_cli(captures2.path(), &runspec, &lock, out2.path());

    let files1 = collect_files(out1.path());
    let files2 = collect_files(out2.path());
    let names1: Vec<&str> = files1.iter().map(|(n, _)| n.as_str()).collect();
    let names2: Vec<&str> = files2.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names1, names2, "snapshot file lists differ");
    for ((n1, b1), (_, b2)) in files1.iter().zip(files2.iter()) {
        assert_eq!(b1, b2, "snapshot bytes differ at {n1}");
    }
}

#[test]
fn cli_includes_provenance_with_sif_and_runspec_hashes() {
    let captures = tempdir().unwrap();
    populate_captures(captures.path());
    let scratch = tempdir().unwrap();
    let runspec = scratch.path().join("SampleRunSpec.xml");
    fs::write(&runspec, RUNSPEC_BODY).unwrap();
    let lock = scratch.path().join("fixture-image.lock");
    fs::write(&lock, FIXTURE_LOCK).unwrap();
    let out = tempdir().unwrap();

    run_cli(captures.path(), &runspec, &lock, out.path());

    let prov_bytes = fs::read(out.path().join("provenance.json")).unwrap();
    let prov: serde_json::Value = serde_json::from_slice(&prov_bytes).unwrap();
    assert_eq!(prov["fixture_name"], "samplerunspec");
    assert_eq!(
        prov["sif_sha256"],
        "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100"
    );
    assert_eq!(prov["output_database"], "movesoutput");
    assert_eq!(prov["scale_input_database"], "movesdb20241112");
    // RunSpec sha256 must be a 64-char lowercase hex string.
    let rs_sha = prov["runspec_sha256"].as_str().unwrap();
    assert_eq!(rs_sha.len(), 64);
    assert!(rs_sha
        .chars()
        .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    // Aggregate hash matches the manifest.
    let manifest_bytes = fs::read(out.path().join("manifest.json")).unwrap();
    let manifest: serde_json::Value = serde_json::from_slice(&manifest_bytes).unwrap();
    assert_eq!(
        prov["snapshot_aggregate_sha256"],
        manifest["aggregate_sha256"]
    );
}

#[test]
fn cli_excludes_default_db_from_snapshot() {
    let captures = tempdir().unwrap();
    populate_captures(captures.path());
    let scratch = tempdir().unwrap();
    let runspec = scratch.path().join("SampleRunSpec.xml");
    fs::write(&runspec, RUNSPEC_BODY).unwrap();
    let lock = scratch.path().join("fixture-image.lock");
    fs::write(&lock, FIXTURE_LOCK).unwrap();
    let out = tempdir().unwrap();

    run_cli(captures.path(), &runspec, &lock, out.path());

    let manifest_bytes = fs::read(out.path().join("manifest.json")).unwrap();
    let manifest: serde_json::Value = serde_json::from_slice(&manifest_bytes).unwrap();
    let names: Vec<&str> = manifest["tables"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert!(!names.iter().any(|n| n.starts_with("db__movesdb20241112__")));
    assert!(names.iter().any(|n| n.starts_with("db__movesoutput__")));
}

#[test]
fn cli_emits_execution_trace_alongside_snapshot() {
    // Phase 0 Task 8 (mo-d7or): the CLI must produce execution-trace.json
    // next to provenance.json. Populate a captures dir with a worker.sql
    // that names a calculator + an SQL file, plus a JVM class-load log
    // under moves-temporary/instrumentation/, and verify the resulting
    // trace lists each.
    let captures = tempdir().unwrap();
    populate_captures(captures.path());

    // worker.sql in WorkerTemp00 (replacing the bare Output.tbl-only
    // bundle the canonical population creates).
    fs::write(
        captures
            .path()
            .join("worker-folder/WorkerTemp00/worker.sql"),
        b"-- @@@ Calculator: gov.epa.otaq.moves.master.calculator.CriteriaRunningCalculator\n\
          -- @@@ source: database/CalculatorSQL/CriteriaRunningCalculator.sql\n\
          SELECT 1;\n",
    )
    .unwrap();
    // A Go calculator artifact dropped into the same bundle.
    fs::write(
        captures
            .path()
            .join("worker-folder/WorkerTemp00/BaseRateCalculator.go.input"),
        b"go-calc input\n",
    )
    .unwrap();
    // JVM class-load log under moves-temporary/instrumentation/.
    let instr_dir = captures.path().join("moves-temporary/instrumentation");
    fs::create_dir_all(&instr_dir).unwrap();
    fs::write(
        instr_dir.join("class-load-12345.log"),
        b"[0.001s][info][class,load] java.lang.Object source: shared\n\
          [0.123s][info][class,load] gov.epa.otaq.moves.master.framework.Generator source: file:/opt/moves/...\n",
    )
    .unwrap();

    let scratch = tempdir().unwrap();
    let runspec = scratch.path().join("SampleRunSpec.xml");
    fs::write(&runspec, RUNSPEC_BODY).unwrap();
    let lock = scratch.path().join("fixture-image.lock");
    fs::write(&lock, FIXTURE_LOCK).unwrap();
    let out = tempdir().unwrap();

    run_cli(captures.path(), &runspec, &lock, out.path());

    let trace_bytes = fs::read(out.path().join("execution-trace.json")).unwrap();
    let trace: serde_json::Value = serde_json::from_slice(&trace_bytes).unwrap();

    assert_eq!(trace["fixture_name"], "samplerunspec");
    assert_eq!(
        trace["sif_sha256"],
        "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100"
    );
    assert_eq!(trace["trace_version"], "moves-fixture-capture/v1");

    // Java classes from both worker.sql and the JVM log, sorted.
    let class_names: Vec<&str> = trace["java_classes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        class_names,
        vec![
            "gov.epa.otaq.moves.master.calculator.CriteriaRunningCalculator",
            "gov.epa.otaq.moves.master.framework.Generator",
        ]
    );

    // SQL file from worker.sql attributed to WorkerTemp00.
    let sql_files = trace["sql_files"].as_array().unwrap();
    assert_eq!(sql_files.len(), 1);
    assert_eq!(
        sql_files[0]["path"],
        "database/CalculatorSQL/CriteriaRunningCalculator.sql"
    );
    let consumers: Vec<&str> = sql_files[0]["consumed_by"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s.as_str().unwrap())
        .collect();
    assert_eq!(consumers, vec!["WorkerTemp00"]);

    // Go calc detected from the BaseRateCalculator.go.input filename.
    let go = trace["go_calculators"].as_array().unwrap();
    assert_eq!(go.len(), 1);
    assert_eq!(go[0]["name"], "BaseRateCalculator");

    // Source counts.
    assert_eq!(trace["sources"]["worker_sql_files"], 1);
    assert_eq!(trace["sources"]["class_load_log_files"], 1);

    // The trace file must be byte-stable across re-runs of the same
    // captures (same determinism contract as snapshot + provenance).
    let out2 = tempdir().unwrap();
    run_cli(captures.path(), &runspec, &lock, out2.path());
    let trace_bytes2 = fs::read(out2.path().join("execution-trace.json")).unwrap();
    assert_eq!(trace_bytes, trace_bytes2);
}

#[test]
fn cli_emits_empty_but_valid_trace_when_no_worker_or_instrumentation() {
    // A capture dir with only databases (no worker bundles, no
    // instrumentation log) must still produce a syntactically valid
    // trace — an empty trace, but parseable. Phase 1 consumers can rely
    // on the file existing.
    let captures = tempdir().unwrap();
    populate_captures(captures.path());

    let scratch = tempdir().unwrap();
    let runspec = scratch.path().join("SampleRunSpec.xml");
    fs::write(&runspec, RUNSPEC_BODY).unwrap();
    let lock = scratch.path().join("fixture-image.lock");
    fs::write(&lock, FIXTURE_LOCK).unwrap();
    let out = tempdir().unwrap();

    run_cli(captures.path(), &runspec, &lock, out.path());

    let trace_bytes = fs::read(out.path().join("execution-trace.json")).unwrap();
    let trace: serde_json::Value = serde_json::from_slice(&trace_bytes).unwrap();
    assert!(trace["java_classes"].as_array().unwrap().is_empty());
    assert!(trace["sql_files"].as_array().unwrap().is_empty());
    // The fixture identity fields must still be populated.
    assert_eq!(trace["fixture_name"], "samplerunspec");
    assert_eq!(trace["sources"]["worker_sql_files"], 0);
}

#[test]
fn cli_handles_pending_first_build_lockfile() {
    // A lockfile that hasn't been refreshed by build-fixture-sif.sh should
    // not abort the capture — the snapshot is still valid, it just records
    // the pending state for downstream tooling to flag.
    let captures = tempdir().unwrap();
    populate_captures(captures.path());
    let scratch = tempdir().unwrap();
    let runspec = scratch.path().join("SampleRunSpec.xml");
    fs::write(&runspec, RUNSPEC_BODY).unwrap();
    let lock = scratch.path().join("fixture-image.lock");
    fs::write(&lock, b"sif_sha256 = \"PENDING_FIRST_BUILD\"\n").unwrap();
    let out = tempdir().unwrap();

    run_cli(captures.path(), &runspec, &lock, out.path());

    let prov_bytes = fs::read(out.path().join("provenance.json")).unwrap();
    let prov: serde_json::Value = serde_json::from_slice(&prov_bytes).unwrap();
    assert_eq!(prov["sif_sha256"], "PENDING_FIRST_BUILD");
}

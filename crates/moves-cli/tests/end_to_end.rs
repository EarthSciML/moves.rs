//! End-to-end tests for the `moves` CLI (migration-plan Task 28).
//!
//! The headline test — `run_sample_runspec_walks_the_graph` — is the Phase 2
//! closing smoke test: load `characterization/fixtures/sample-runspec.xml`,
//! walk the real calculator graph (no calculators ported yet, so every
//! module reports unimplemented), and confirm the engine still produces an
//! empty-but-correctly-shaped `MOVESRun.parquet`.
//!
//! The remaining tests exercise the `convert-runspec` and `import-cdb`
//! subcommands through the same library entry points the `moves` binary
//! calls.

use std::fs;
use std::path::{Path, PathBuf};

use moves_cli::{
    convert_runspec, import_cdb, load_run_spec, run_simulation, ConvertOptions, ImportOptions,
    ImportStatus, RunOptions, RunSpecFormat,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// Workspace root — the tests resolve `characterization/` fixtures relative
/// to it. `CARGO_MANIFEST_DIR` is `crates/moves-cli` during `cargo test`.
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent() // crates/
        .and_then(Path::parent) // workspace root
        .expect("workspace root above crates/moves-cli/")
        .to_path_buf()
}

fn sample_runspec() -> PathBuf {
    workspace_root().join("characterization/fixtures/sample-runspec.xml")
}

/// Read a Parquet file, returning `(row_count, column_names)`.
fn read_parquet(path: &Path) -> (usize, Vec<String>) {
    let file = fs::File::open(path).unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap_or_else(|e| panic!("parquet open {}: {e}", path.display()));
    let columns: Vec<String> = builder
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    let rows: usize = builder
        .build()
        .expect("build parquet reader")
        .map(|batch| batch.expect("parquet batch").num_rows())
        .sum();
    (rows, columns)
}

// ---- `run` -----------------------------------------------------------------

#[test]
fn run_sample_runspec_walks_the_graph_and_writes_shaped_output() {
    let runspec = sample_runspec();
    assert!(runspec.is_file(), "fixture missing: {}", runspec.display());

    let out = tempfile::tempdir().unwrap();
    let opts = RunOptions {
        runspec: runspec.clone(),
        output: out.path().join("run-output"),
        max_parallel_chunks: 1,
        calculator_dag: None, // the embedded Phase 1 DAG
        run_date_time: Some("2026-05-17T00:00:00".to_string()),
    };
    let outcome = run_simulation(&opts).expect("run should succeed");

    // The engine walked the real calculator graph: sample-runspec selects
    // energy-consumption pollutants for running / start / extended-idle
    // exhaust, which the DAG registers to BaseRateCalculator.
    assert!(
        !outcome.modules_planned.is_empty(),
        "expected a non-empty calculator plan"
    );
    assert!(
        outcome
            .modules_planned
            .iter()
            .any(|m| m == "BaseRateCalculator"),
        "expected BaseRateCalculator in the plan, got {:?}",
        outcome.modules_planned
    );
    assert!(outcome.chunk_count() >= 1);

    // Phase 2: no calculators are ported, so every planned module is
    // unimplemented and nothing executes.
    assert!(outcome.modules_executed.is_empty());
    assert_eq!(outcome.modules_unimplemented, outcome.modules_planned);
    assert!(!outcome.is_fully_implemented());
    assert_eq!(outcome.iterations, 1);

    // The output is empty-but-correctly-shaped: a one-row MOVESRun.parquet
    // carrying the canonical output schema.
    assert!(outcome.run_record_path.is_file());
    assert_eq!(
        outcome.run_record_path.file_name().unwrap(),
        "MOVESRun.parquet"
    );
    let (rows, columns) = read_parquet(&outcome.run_record_path);
    assert_eq!(rows, 1, "MOVESRun.parquet should hold exactly one run row");
    assert!(
        columns.iter().any(|c| c == "MOVESRunID"),
        "MOVESRun.parquet should carry the output schema, got columns {columns:?}"
    );
}

#[test]
fn run_accepts_a_toml_runspec() {
    // Convert the XML fixture to TOML, then drive a run from the TOML —
    // exercises the `.toml` branch of the RunSpec loader.
    let dir = tempfile::tempdir().unwrap();
    let toml = dir.path().join("sample.toml");
    convert_runspec(&ConvertOptions {
        input: sample_runspec(),
        output: Some(toml.clone()),
    })
    .expect("xml -> toml conversion");

    let opts = RunOptions {
        runspec: toml,
        output: dir.path().join("run-output"),
        max_parallel_chunks: 0,
        calculator_dag: None,
        run_date_time: None,
    };
    let outcome = run_simulation(&opts).expect("run from TOML should succeed");
    assert!(outcome.run_record_path.is_file());
    assert!(!outcome.modules_planned.is_empty());
}

#[test]
fn run_reports_a_missing_runspec() {
    let opts = RunOptions {
        runspec: PathBuf::from("/nonexistent/spec.xml"),
        output: tempfile::tempdir().unwrap().path().join("o"),
        max_parallel_chunks: 1,
        calculator_dag: None,
        run_date_time: None,
    };
    let err = run_simulation(&opts).unwrap_err();
    assert!(err.to_string().contains("reading RunSpec"), "got: {err}");
}

// ---- `convert-runspec` -----------------------------------------------------

#[test]
fn convert_runspec_round_trips_xml_through_toml() {
    let dir = tempfile::tempdir().unwrap();
    let toml = dir.path().join("spec.toml");
    let back = dir.path().join("spec.xml");

    let to_toml = convert_runspec(&ConvertOptions {
        input: sample_runspec(),
        output: Some(toml.clone()),
    })
    .expect("xml -> toml");
    assert_eq!(to_toml.from, RunSpecFormat::Xml);
    assert_eq!(to_toml.to, RunSpecFormat::Toml);
    assert!(toml.is_file());

    let to_xml = convert_runspec(&ConvertOptions {
        input: toml.clone(),
        output: Some(back.clone()),
    })
    .expect("toml -> xml");
    assert_eq!(to_xml.from, RunSpecFormat::Toml);
    assert_eq!(to_xml.to, RunSpecFormat::Xml);

    // The model survives the XML -> TOML -> XML round trip.
    let original = load_run_spec(&sample_runspec()).unwrap();
    let round_tripped = load_run_spec(&back).unwrap();
    assert_eq!(original, round_tripped);
}

#[test]
fn convert_runspec_derives_the_output_path_from_the_target_format() {
    let dir = tempfile::tempdir().unwrap();
    let xml = dir.path().join("spec.xml");
    fs::copy(sample_runspec(), &xml).unwrap();

    // No --output: the converter writes alongside the input with the
    // opposite extension.
    let outcome = convert_runspec(&ConvertOptions {
        input: xml.clone(),
        output: None,
    })
    .expect("conversion");
    assert_eq!(outcome.output, dir.path().join("spec.toml"));
    assert!(outcome.output.is_file());
}

// ---- `import-cdb` ----------------------------------------------------------

fn write_file(dir: &Path, name: &str, contents: &str) {
    fs::write(dir.join(name), contents).unwrap();
}

#[test]
fn import_cdb_validates_and_writes_parquet() {
    let input = tempfile::tempdir().unwrap();
    write_file(
        input.path(),
        "SourceTypeYear.csv",
        "yearID,sourceTypeID,sourceTypePopulation\n2020,21,1500000\n2021,21,1525000\n",
    );
    let output = tempfile::tempdir().unwrap();

    let outcome = import_cdb(&ImportOptions {
        input: input.path().to_path_buf(),
        output: output.path().to_path_buf(),
        default_db: None,
    })
    .expect("import should succeed");

    assert!(!outcome.has_errors());
    assert_eq!(outcome.written(), 1);
    let written = outcome
        .tables
        .iter()
        .find(|t| t.table == "SourceTypeYear" && t.status == ImportStatus::Written)
        .expect("SourceTypeYear should be written");
    assert_eq!(written.row_count, 2);

    let destination = written.destination.as_ref().expect("destination set");
    assert!(destination.is_file());
    let (rows, _columns) = read_parquet(destination);
    assert_eq!(rows, 2);

    // The other declared County tables had no CSV, so they are reported
    // missing rather than failing the import.
    assert!(outcome.missing() > 0);
}

#[test]
fn import_cdb_rejects_a_table_that_fails_validation() {
    let input = tempfile::tempdir().unwrap();
    // A negative population violates the `NonNegative` column filter.
    write_file(
        input.path(),
        "SourceTypeYear.csv",
        "yearID,sourceTypeID,sourceTypePopulation\n2020,21,-100\n",
    );
    let output = tempfile::tempdir().unwrap();

    let outcome = import_cdb(&ImportOptions {
        input: input.path().to_path_buf(),
        output: output.path().to_path_buf(),
        default_db: None,
    })
    .expect("import call itself should succeed");

    assert!(outcome.has_errors());
    let rejected = outcome
        .tables
        .iter()
        .find(|t| t.table == "SourceTypeYear")
        .expect("SourceTypeYear reported");
    assert_eq!(rejected.status, ImportStatus::Rejected);
    assert!(!rejected.errors.is_empty());
    assert!(rejected.destination.is_none());
    // A rejected table writes no Parquet.
    assert!(!output.path().join("SourceTypeYear.parquet").exists());
}

#[test]
fn import_cdb_errors_when_no_csv_is_present() {
    let input = tempfile::tempdir().unwrap();
    let output = tempfile::tempdir().unwrap();
    let err = import_cdb(&ImportOptions {
        input: input.path().to_path_buf(),
        output: output.path().to_path_buf(),
        default_db: None,
    })
    .unwrap_err();
    assert!(
        err.to_string().contains("no County-database CSV files"),
        "got: {err}"
    );
}

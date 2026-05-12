//! End-to-end tests that exercise the CLI binary against synthetic
//! `tables.json` plans and TSV fixtures.

use std::path::Path;
use std::process::Command;

use arrow::array::Array;
use bytes::Bytes;
use moves_default_db_convert::{convert, ConvertOptions, Manifest};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn write(path: &Path, body: &[u8]) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, body).unwrap();
}

fn read_back(bytes: &[u8]) -> Vec<arrow::record_batch::RecordBatch> {
    let bytes = Bytes::from(bytes.to_vec());
    ParquetRecordBatchReaderBuilder::try_new(bytes)
        .unwrap()
        .build()
        .unwrap()
        .map(|b| b.unwrap())
        .collect()
}

fn count_parquet_rows(path: &Path) -> usize {
    let bytes = std::fs::read(path).unwrap();
    read_back(&bytes).iter().map(|b| b.num_rows()).sum()
}

/// Walks the production audit (`tables.json`) and confirms the conversion
/// crate's [`PartitionPlan`] parser accepts every committed table.
#[test]
fn audit_tables_json_parses_cleanly() {
    let repo_root = repo_root();
    let plan_path = repo_root
        .join("characterization")
        .join("default-db-schema")
        .join("tables.json");
    assert!(plan_path.exists(), "expected {}", plan_path.display());
    let bytes = std::fs::read(&plan_path).unwrap();
    let plan = moves_default_db_convert::PartitionPlan::from_bytes(&plan_path, &bytes).unwrap();
    assert!(
        plan.tables.len() >= 200,
        "too few tables: {}",
        plan.tables.len()
    );

    // Spot-check known strategy assignments from the partitioning plan
    // doc — surfaces drift between audit and converter early.
    let year = plan.get("Year").expect("Year is in the audit");
    assert_eq!(
        year.partition.strategy,
        moves_default_db_convert::PartitionStrategy::Monolithic
    );
    let sho = plan.get("SHO").expect("SHO is in the audit");
    assert_eq!(
        sho.partition.strategy,
        moves_default_db_convert::PartitionStrategy::SchemaOnly
    );
    let imc = plan.get("IMCoverage").expect("IMCoverage is in the audit");
    assert_eq!(
        imc.partition.strategy,
        moves_default_db_convert::PartitionStrategy::YearXCounty
    );
}

/// Each table-entry in the audit must resolve to a partition spec; if any
/// large table is missing a usable PK column for its strategy the audit
/// has drifted and the conversion would fail at runtime.
#[test]
fn every_audit_entry_resolves_to_a_partition_spec() {
    let repo_root = repo_root();
    let plan_path = repo_root
        .join("characterization")
        .join("default-db-schema")
        .join("tables.json");
    let bytes = std::fs::read(&plan_path).unwrap();
    let plan = moves_default_db_convert::PartitionPlan::from_bytes(&plan_path, &bytes).unwrap();
    for table in &plan.tables {
        moves_default_db_convert::partition::resolve(table)
            .unwrap_or_else(|e| panic!("audit table '{}' failed: {e}", table.name));
    }
}

/// Full pipeline run with a mixed plan: monolithic, schema-only, and
/// county-partitioned. Confirms manifest aggregation, partition layout,
/// row-count validation, and Parquet round-trip in one go.
#[test]
fn pipeline_writes_mixed_strategies_with_valid_parquet() {
    let dir = tempfile::tempdir().unwrap();
    let tsv_dir = dir.path().join("dump");
    let out_dir = dir.path().join("out");
    let plan_path = dir.path().join("tables.json");

    let plan = br#"{
        "schema_version": "moves-default-db-schema/v1",
        "moves_commit": "deadbeefcafe",
        "sources": {},
        "table_count": 3,
        "tables": [
            {
                "name": "Year",
                "primary_key": ["yearID"],
                "columns": [
                    {"name": "yearID", "type": "smallint"},
                    {"name": "isBaseYear", "type": "char"}
                ],
                "indexes": [],
                "estimated_rows_upper_bound": 50,
                "size_bucket": "small",
                "filter_columns": [],
                "partition": {"strategy": "monolithic", "rationale": ""}
            },
            {
                "name": "Link",
                "primary_key": ["linkID"],
                "columns": [{"name": "linkID", "type": "int"}],
                "indexes": [],
                "estimated_rows_upper_bound": 0,
                "size_bucket": "empty",
                "filter_columns": [],
                "partition": {"strategy": "schema_only", "rationale": "empty"}
            },
            {
                "name": "Surrogate",
                "primary_key": ["stateID", "metric"],
                "columns": [
                    {"name": "stateID", "type": "smallint"},
                    {"name": "metric", "type": "varchar"},
                    {"name": "value", "type": "double"}
                ],
                "indexes": [],
                "estimated_rows_upper_bound": 1000000,
                "size_bucket": "large",
                "filter_columns": [],
                "partition": {"strategy": "county", "rationale": ""}
            }
        ]
    }"#;
    write(&plan_path, plan);

    write(
        &tsv_dir.join("Year.schema.tsv"),
        b"yearID\tsmallint\tPRI\nisBaseYear\tchar\t\n",
    );
    write(&tsv_dir.join("Year.tsv"), b"1990\tY\n2000\tN\n");

    write(
        &tsv_dir.join("Surrogate.schema.tsv"),
        b"stateID\tsmallint\tPRI\nmetric\tvarchar\tPRI\nvalue\tdouble\t\n",
    );
    write(
        &tsv_dir.join("Surrogate.tsv"),
        b"06\talpha\t1.5\n06\tbeta\t2.5\n17\talpha\t3.5\n17\tbeta\t4.5\n17\tgamma\tNULL\n",
    );

    let opts = ConvertOptions {
        tsv_dir,
        plan_path,
        output_root: out_dir.clone(),
        moves_db_version: "movesdb20241112".into(),
        generated_at_utc: Some("1970-01-01T00:00:00Z".into()),
        require_every_table: false,
    };
    let (manifest, report) = convert(&opts).unwrap();

    // Table list is sorted by case-folded name.
    let names: Vec<&str> = manifest.tables.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(names, vec!["Link", "Surrogate", "Year"]);

    // Year is monolithic with 2 rows.
    let year = manifest.tables.iter().find(|t| t.name == "Year").unwrap();
    assert_eq!(year.partition_strategy, "monolithic");
    assert_eq!(year.row_count, 2);
    assert_eq!(year.partitions.len(), 1);
    assert_eq!(
        count_parquet_rows(&out_dir.join("Year.parquet")),
        2,
        "Year parquet"
    );

    // Link is schema-only with a sidecar.
    let link = manifest.tables.iter().find(|t| t.name == "Link").unwrap();
    assert_eq!(link.partition_strategy, "schema_only");
    assert_eq!(link.row_count, 0);
    assert_eq!(link.schema_only_path.as_deref(), Some("Link.schema.json"));
    assert!(out_dir.join("Link.schema.json").exists());
    assert!(!out_dir.join("Link.parquet").exists());

    // Surrogate is county-strategy with stateID fallback => `state=` partitions.
    let sur = manifest
        .tables
        .iter()
        .find(|t| t.name == "Surrogate")
        .unwrap();
    assert_eq!(sur.partition_strategy, "county");
    assert_eq!(sur.partition_columns, vec!["stateID".to_string()]);
    assert_eq!(sur.partitions.len(), 2);
    let by_path: std::collections::HashMap<
        &str,
        &moves_default_db_convert::manifest::PartitionManifest,
    > = sur
        .partitions
        .iter()
        .map(|p| (p.path.as_str(), p))
        .collect();
    let p06 = by_path["Surrogate/state=06/part.parquet"];
    let p17 = by_path["Surrogate/state=17/part.parquet"];
    assert_eq!(p06.row_count, 2);
    assert_eq!(p17.row_count, 3);
    assert_eq!(p06.values, vec!["06".to_string()]);
    assert_eq!(p17.values, vec!["17".to_string()]);

    // Round-trip a partition and confirm NULL preserved.
    let bytes = std::fs::read(out_dir.join(&p17.path)).unwrap();
    let batches = read_back(&bytes);
    assert_eq!(batches.len(), 1);
    let value_col = batches[0]
        .column_by_name("value")
        .expect("value column")
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("Float64Array");
    assert_eq!(value_col.len(), 3);
    assert!(!value_col.is_null(0)); // 3.5
    assert!(!value_col.is_null(1)); // 4.5
    assert!(value_col.is_null(2)); // NULL

    // Report aggregates match.
    assert_eq!(report.tables_written, 3);
    assert_eq!(report.partitions_written, 3); // Year + 2 Surrogate partitions
    assert_eq!(report.total_rows, 7);
    assert!(report.warnings.is_empty());
}

/// Re-running the converter on identical inputs must produce identical
/// Parquet bytes (no statistics, no compression, deterministic grouping).
#[test]
fn deterministic_re_run() {
    let dir = tempfile::tempdir().unwrap();
    let tsv_dir = dir.path().join("dump");
    let plan_path = dir.path().join("tables.json");

    write(
        &plan_path,
        br#"{
            "schema_version": "moves-default-db-schema/v1",
            "moves_commit": "x",
            "sources": {},
            "table_count": 1,
            "tables": [{
                "name": "T",
                "primary_key": ["id"],
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "label", "type": "varchar"}
                ],
                "indexes": [],
                "estimated_rows_upper_bound": 10,
                "size_bucket": "small",
                "filter_columns": [],
                "partition": {"strategy": "monolithic", "rationale": ""}
            }]
        }"#,
    );
    write(
        &tsv_dir.join("T.schema.tsv"),
        b"id\tint\tPRI\nlabel\tvarchar\t\n",
    );
    write(&tsv_dir.join("T.tsv"), b"1\talpha\n2\tbeta\n3\tNULL\n");

    let mut opts = ConvertOptions {
        tsv_dir: tsv_dir.clone(),
        plan_path: plan_path.clone(),
        output_root: dir.path().join("out1"),
        moves_db_version: "movesdb20241112".into(),
        generated_at_utc: Some("1970-01-01T00:00:00Z".into()),
        require_every_table: true,
    };
    let (m1, _) = convert(&opts).unwrap();
    let h1 = m1.tables[0].partitions[0].sha256.clone();

    opts.output_root = dir.path().join("out2");
    let (m2, _) = convert(&opts).unwrap();
    let h2 = m2.tables[0].partitions[0].sha256.clone();

    assert_eq!(h1, h2, "Parquet hashes must be byte-stable");
    let bytes1 = std::fs::read(dir.path().join("out1").join("T.parquet")).unwrap();
    let bytes2 = std::fs::read(dir.path().join("out2").join("T.parquet")).unwrap();
    assert_eq!(bytes1, bytes2, "Parquet bytes must be identical");
}

/// CLI binary exists and prints help.
#[test]
fn cli_help_works() {
    let bin = env!("CARGO_BIN_EXE_moves-default-db-convert");
    let out = Command::new(bin).arg("--help").output().unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert!(stdout.contains("--tsv-dir"));
    assert!(stdout.contains("--plan"));
    assert!(stdout.contains("--output"));
    assert!(stdout.contains("--moves-db-version"));
}

/// CLI binary runs end-to-end and writes manifest + parquet.
#[test]
fn cli_runs_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let tsv_dir = dir.path().join("dump");
    let out_dir = dir.path().join("out");
    let plan_path = dir.path().join("tables.json");

    write(
        &plan_path,
        br#"{
            "schema_version": "moves-default-db-schema/v1",
            "moves_commit": "deadbeef",
            "sources": {},
            "table_count": 1,
            "tables": [{
                "name": "Year",
                "primary_key": ["yearID"],
                "columns": [
                    {"name": "yearID", "type": "smallint"},
                    {"name": "isBaseYear", "type": "char"}
                ],
                "indexes": [],
                "estimated_rows_upper_bound": 100,
                "size_bucket": "small",
                "filter_columns": [],
                "partition": {"strategy": "monolithic", "rationale": ""}
            }]
        }"#,
    );
    write(
        &tsv_dir.join("Year.schema.tsv"),
        b"yearID\tsmallint\tPRI\nisBaseYear\tchar\t\n",
    );
    write(&tsv_dir.join("Year.tsv"), b"1990\tY\n2000\tN\n");

    let bin = env!("CARGO_BIN_EXE_moves-default-db-convert");
    let out = Command::new(bin)
        .args([
            "--tsv-dir",
            tsv_dir.to_str().unwrap(),
            "--plan",
            plan_path.to_str().unwrap(),
            "--output",
            out_dir.to_str().unwrap(),
            "--moves-db-version",
            "movesdb20241112",
            "--generated-at-utc",
            "1970-01-01T00:00:00Z",
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let manifest_path = out_dir.join("manifest.json");
    assert!(manifest_path.exists());

    let manifest: Manifest =
        serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
    assert_eq!(manifest.tables.len(), 1);
    assert_eq!(manifest.tables[0].name, "Year");
    assert_eq!(manifest.tables[0].row_count, 2);
}

fn repo_root() -> std::path::PathBuf {
    // Tests run with CARGO_MANIFEST_DIR set to the crate dir; the workspace
    // root is its grandparent (../..).
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

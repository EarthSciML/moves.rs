//! End-to-end conversion: read TSV dumps, apply partition strategy, write
//! Parquet, build manifest.
//!
//! ## Input layout
//!
//! ```text
//! <tsv-dir>/
//!   <Table>.tsv            # rows, ORDER BY all columns, mariadb -B -N format
//!   <Table>.schema.tsv     # column metadata (name, mysql_type, column_key)
//! ```
//!
//! ## Output layout
//!
//! ```text
//! <output-root>/
//!   manifest.json
//!   <Table>.parquet                                 # monolithic
//!   <Table>.schema.json                             # schema_only
//!   <Table>/county=<id>/part.parquet                # county/zone/state
//!   <Table>/year=<y>/county=<id>/part.parquet       # year_x_county
//!   <Table>/modelYear=<y>/part.parquet              # model_year
//! ```
//!
//! ## Behaviour
//!
//! * Monolithic: read all rows, write one Parquet file.
//! * Schema-only: read the schema, write a `*.schema.json` sidecar. If the
//!   TSV is unexpectedly non-empty (a future EPA release that populates a
//!   previously-empty table), the row count is recorded and the conversion
//!   keeps going, but the rows are NOT serialised — the caller is expected
//!   to update the audit (`tables.json`) and re-run. We emit a warning
//!   via the [`ConvertReport`] so the caller can surface it.
//! * Partitioned: load all rows, group by partition values, write one
//!   Parquet file per partition. The grouping uses a `BTreeMap` keyed on
//!   the partition values' string form so partition iteration order is
//!   deterministic.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::manifest::{
    ColumnManifest, Manifest, PartitionManifest, SchemaOnlySidecar, TableManifest,
    MANIFEST_FILENAME, SCHEMA_ONLY_VERSION,
};
use crate::parquet_writer::{encode_parquet, sha256_hex, write_atomic, Row};
use crate::partition::{render_path, resolve, PartitionSpec};
use crate::plan::{PartitionPlan, PartitionStrategy, TableEntry};
use crate::tsv::{count_rows, read_schema_tsv, SchemaColumn, TsvRows};

/// Configuration for a conversion run.
#[derive(Debug, Clone)]
pub struct ConvertOptions {
    pub tsv_dir: PathBuf,
    pub plan_path: PathBuf,
    pub output_root: PathBuf,
    pub moves_db_version: String,
    /// Optional override for the "generated at" stamp written to the
    /// manifest. Default: ISO-8601 UTC of the current wall clock. Tests
    /// pass a fixed string for byte-stable assertions.
    pub generated_at_utc: Option<String>,
    /// If `false`, skip tables present in the plan but absent from the
    /// TSV directory. If `true`, error out so silent omissions surface.
    pub require_every_table: bool,
}

/// Summary of a conversion run. Returned for telemetry / CI logging.
#[derive(Debug, Clone, Default)]
pub struct ConvertReport {
    pub tables_written: usize,
    pub partitions_written: usize,
    pub total_rows: u64,
    pub skipped_tables: Vec<String>,
    pub warnings: Vec<String>,
}

/// Run the conversion pipeline end-to-end.
pub fn convert(opts: &ConvertOptions) -> Result<(Manifest, ConvertReport)> {
    let plan_bytes = std::fs::read(&opts.plan_path).map_err(|source| Error::Io {
        path: opts.plan_path.clone(),
        source,
    })?;
    let plan = PartitionPlan::from_bytes(&opts.plan_path, &plan_bytes)?;
    let plan_sha = sha256_hex(&plan_bytes);
    let generated_at = opts
        .generated_at_utc
        .clone()
        .unwrap_or_else(now_iso8601_utc);

    std::fs::create_dir_all(&opts.output_root).map_err(|source| Error::Io {
        path: opts.output_root.clone(),
        source,
    })?;

    let mut manifest = Manifest::new(
        opts.moves_db_version.clone(),
        plan.moves_commit.clone(),
        plan_sha,
        generated_at,
    );
    let mut report = ConvertReport::default();

    for table in &plan.tables {
        match convert_table(table, opts, &mut report)? {
            ConvertTableOutcome::Written(entry) => {
                report.tables_written += 1;
                report.partitions_written += entry.partitions.len();
                report.total_rows += entry.row_count;
                if let Some(w) = entry_warning(table, &entry) {
                    report.warnings.push(w);
                }
                manifest.push(entry);
            }
            ConvertTableOutcome::Skipped(reason) => {
                if opts.require_every_table {
                    return Err(Error::Plan(format!(
                        "table '{}' missing from TSV dir: {}",
                        table.name, reason
                    )));
                }
                report.skipped_tables.push(table.name.clone());
            }
        }
    }

    manifest.finalize();

    let manifest_path = opts.output_root.join(MANIFEST_FILENAME);
    let manifest_json = manifest.to_pretty_json().map_err(|source| Error::Json {
        path: manifest_path.clone(),
        source,
    })?;
    write_atomic(&manifest_path, manifest_json.as_bytes())?;

    Ok((manifest, report))
}

enum ConvertTableOutcome {
    Written(TableManifest),
    Skipped(String),
}

fn convert_table(
    table: &TableEntry,
    opts: &ConvertOptions,
    report: &mut ConvertReport,
) -> Result<ConvertTableOutcome> {
    let strategy = table.partition.strategy;
    let spec = resolve(table)?;

    match strategy {
        PartitionStrategy::SchemaOnly => Ok(ConvertTableOutcome::Written(write_schema_only(
            table, opts,
        )?)),
        _ => {
            // All other strategies need the TSV pair. Missing TSV means the
            // dumper skipped this table — surface vs. silently omit per
            // ConvertOptions.require_every_table. Lookup is case-insensitive
            // because the MariaDB on Linux normalizes the original Windows
            // dump's CamelCase table names to lowercase on disk, while the
            // audit (`tables.json`) preserves the canonical CamelCase from
            // the MOVES Java schema.
            let schema_path =
                match find_tsv_case_insensitive(&opts.tsv_dir, &table.name, ".schema.tsv")? {
                    Some(p) => p,
                    None => {
                        return Ok(ConvertTableOutcome::Skipped(format!(
                            "schema TSV not found for table '{}' in {}",
                            table.name,
                            opts.tsv_dir.display()
                        )));
                    }
                };
            let columns = read_schema_tsv(&schema_path)?;
            check_columns_drift(table, &columns, opts, report)?;
            let tsv_path = match find_tsv_case_insensitive(&opts.tsv_dir, &table.name, ".tsv")? {
                Some(p) => p,
                None => {
                    return Ok(ConvertTableOutcome::Skipped(format!(
                        "row TSV not found for table '{}' in {}",
                        table.name,
                        opts.tsv_dir.display()
                    )));
                }
            };
            let rows = read_all_rows(&tsv_path, &columns)?;
            let source_count = rows.len() as u64;
            let entry = write_partitioned(table, &columns, &spec, rows, opts)?;
            verify_row_count(&entry, source_count, &tsv_path)?;
            Ok(ConvertTableOutcome::Written(entry))
        }
    }
}

/// Public re-export of the case-insensitive TSV lookup used internally
/// by [`convert`]. Sibling modules — notably `validate` — need the same
/// lookup semantics so a validation run sees the same TSV files the
/// conversion did. Kept as a thin wrapper so the private helper can
/// evolve without breaking external callers.
pub fn find_tsv_case_insensitive_pub(
    dir: &Path,
    table_name: &str,
    suffix: &str,
) -> Result<Option<PathBuf>> {
    find_tsv_case_insensitive(dir, table_name, suffix)
}

/// Locate `<table_name><suffix>` under `dir` with a case-insensitive match
/// on the file stem. The dumper's filenames track MariaDB's on-disk case
/// (which lower-cases the original Windows dump's names), while the audit
/// uses CamelCase. Without this, ~90% of tables get silently skipped.
///
/// Returns `Ok(None)` if no matching file exists — including the case
/// where `dir` itself is missing (a wholly-absent TSV directory means no
/// table can be found, which is treated as a soft miss).
fn find_tsv_case_insensitive(
    dir: &Path,
    table_name: &str,
    suffix: &str,
) -> Result<Option<PathBuf>> {
    let target = format!("{}{}", table_name, suffix);
    let exact = dir.join(&target);
    if exact.exists() {
        return Ok(Some(exact));
    }
    let entries = match std::fs::read_dir(dir) {
        Ok(it) => it,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(Error::Io {
                path: dir.to_path_buf(),
                source,
            })
        }
    };
    for entry in entries {
        let entry = entry.map_err(|source| Error::Io {
            path: dir.to_path_buf(),
            source,
        })?;
        if let Some(name) = entry.file_name().to_str() {
            if name.eq_ignore_ascii_case(&target) {
                return Ok(Some(entry.path()));
            }
        }
    }
    Ok(None)
}

fn write_schema_only(table: &TableEntry, opts: &ConvertOptions) -> Result<TableManifest> {
    let schema_path = find_tsv_case_insensitive(&opts.tsv_dir, &table.name, ".schema.tsv")?;
    let columns = if let Some(p) = &schema_path {
        read_schema_tsv(p)?
    } else {
        // Fall back to the audit's columns when the dumper didn't ship
        // anything (the table was empty in MariaDB and a future toolchain
        // may not even emit a schema TSV). The audit is authoritative for
        // these schema-only tables since they ship empty in the default DB.
        synthesize_columns(table)
    };
    let row_tsv = find_tsv_case_insensitive(&opts.tsv_dir, &table.name, ".tsv")?;
    let mut row_count: u64 = 0;
    if let Some(p) = &row_tsv {
        row_count = count_rows(p)?;
    }

    let sidecar_path = format!("{}.schema.json", table.name);
    let column_manifests: Vec<ColumnManifest> = columns
        .iter()
        .map(|c| ColumnManifest {
            name: c.name.clone(),
            mysql_type: c.mysql_type.clone(),
            arrow_type: format!("{:?}", c.arrow_type),
            primary_key: c.primary_key,
        })
        .collect();
    let pk: Vec<String> = if !table.primary_key.is_empty() {
        table.primary_key.clone()
    } else {
        columns
            .iter()
            .filter(|c| c.primary_key)
            .map(|c| c.name.clone())
            .collect()
    };

    let sidecar = SchemaOnlySidecar {
        schema_version: SCHEMA_ONLY_VERSION.to_string(),
        name: table.name.clone(),
        columns: column_manifests.clone(),
        primary_key: pk.clone(),
    };
    let sidecar_bytes = serde_json::to_vec_pretty(&sidecar).map_err(|source| Error::Json {
        path: opts.output_root.join(&sidecar_path),
        source,
    })?;
    write_atomic(&opts.output_root.join(&sidecar_path), &sidecar_bytes)?;

    Ok(TableManifest {
        name: table.name.clone(),
        partition_strategy: PartitionStrategy::SchemaOnly.as_str().to_string(),
        partition_columns: vec![],
        row_count,
        columns: column_manifests,
        primary_key: pk,
        partitions: vec![],
        schema_only_path: Some(sidecar_path),
    })
}

fn synthesize_columns(table: &TableEntry) -> Vec<SchemaColumn> {
    use crate::types::{mysql_to_arrow, normalize_mysql_type};
    let pk: std::collections::HashSet<String> = table
        .primary_key
        .iter()
        .map(|c| c.to_ascii_lowercase())
        .collect();
    table
        .columns
        .iter()
        .map(|c| {
            let mysql_type = normalize_mysql_type(&c.ty);
            SchemaColumn {
                name: c.name.clone(),
                arrow_type: mysql_to_arrow(&mysql_type),
                mysql_type,
                primary_key: pk.contains(&c.name.to_ascii_lowercase()),
            }
        })
        .collect()
}

/// Compare the audit's column list against the dump's schema and either
/// warn or error on drift, based on `opts.require_every_table`.
///
/// Drift is real and expected: Task 79's audit parses the canonical MOVES
/// DDL (`CreateDefault.sql`), while the actual `movesdb20241112.zip` ships
/// with extra metadata columns (e.g. `isUserInput`) added in later
/// releases. The dump's schema is authoritative for what we actually write
/// to Parquet; the audit's column list is informational. In strict mode
/// (`--require-every-table`) we still escalate drift to an error so a
/// regenerated audit catches the drift in CI.
fn check_columns_drift(
    table: &TableEntry,
    columns: &[SchemaColumn],
    opts: &ConvertOptions,
    report: &mut ConvertReport,
) -> Result<()> {
    if table.columns.is_empty() {
        return Ok(());
    }
    if table.columns.len() != columns.len() {
        let msg = format!(
            "table '{}': audit lists {} columns, dump schema has {}",
            table.name,
            table.columns.len(),
            columns.len()
        );
        if opts.require_every_table {
            return Err(Error::Plan(msg));
        }
        report.warnings.push(msg);
    }
    Ok(())
}

fn read_all_rows(tsv_path: &Path, columns: &[SchemaColumn]) -> Result<Vec<Row>> {
    let iter = TsvRows::read(tsv_path, columns.len())?;
    let mut rows: Vec<Row> = Vec::new();
    for r in iter {
        rows.push(r?);
    }
    Ok(rows)
}

fn write_partitioned(
    table: &TableEntry,
    columns: &[SchemaColumn],
    spec: &PartitionSpec,
    rows: Vec<Row>,
    opts: &ConvertOptions,
) -> Result<TableManifest> {
    let mut column_indices: Vec<usize> = Vec::with_capacity(spec.columns.len());
    for pc in &spec.columns {
        let idx = columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&pc.column))
            .ok_or_else(|| Error::NoPartitionColumn {
                table: table.name.clone(),
                strategy: spec.strategy.as_str().to_string(),
                pk: table.primary_key.clone(),
            })?;
        column_indices.push(idx);
    }

    let column_manifests: Vec<ColumnManifest> = columns
        .iter()
        .map(|c| ColumnManifest {
            name: c.name.clone(),
            mysql_type: c.mysql_type.clone(),
            arrow_type: format!("{:?}", c.arrow_type),
            primary_key: c.primary_key,
        })
        .collect();

    let partition_column_names: Vec<String> =
        spec.columns.iter().map(|c| c.column.clone()).collect();

    let total_rows = rows.len() as u64;

    let partitions = if spec.is_partitioned() {
        write_partitions_grouped(table, columns, spec, &column_indices, rows, opts)?
    } else {
        let out = encode_parquet(columns, &rows)?;
        let rel_path = render_path(&table.name, &[]);
        let abs_path = opts.output_root.join(&rel_path);
        write_atomic(&abs_path, &out.bytes)?;
        vec![PartitionManifest {
            path: rel_path,
            values: vec![],
            row_count: out.row_count,
            sha256: out.sha256,
            bytes: out.bytes.len() as u64,
        }]
    };

    let pk: Vec<String> = if !table.primary_key.is_empty() {
        table.primary_key.clone()
    } else {
        columns
            .iter()
            .filter(|c| c.primary_key)
            .map(|c| c.name.clone())
            .collect()
    };

    Ok(TableManifest {
        name: table.name.clone(),
        partition_strategy: spec.strategy.as_str().to_string(),
        partition_columns: partition_column_names,
        row_count: total_rows,
        columns: column_manifests,
        primary_key: pk,
        partitions,
        schema_only_path: None,
    })
}

fn write_partitions_grouped(
    table: &TableEntry,
    columns: &[SchemaColumn],
    spec: &PartitionSpec,
    column_indices: &[usize],
    rows: Vec<Row>,
    opts: &ConvertOptions,
) -> Result<Vec<PartitionManifest>> {
    // Group rows by their partition value tuple. BTreeMap so iteration is
    // deterministic across runs; the key is the joined-with-NUL string of
    // the partition values so a tuple with embedded `=` or `/` does not
    // collide with another tuple after path sanitisation.
    let mut groups: BTreeMap<String, (Vec<String>, Vec<Row>)> = BTreeMap::new();
    for row in rows {
        let mut key = String::new();
        let mut values = Vec::with_capacity(column_indices.len());
        for (i, &idx) in column_indices.iter().enumerate() {
            let v = row[idx].clone().unwrap_or_else(|| "__NULL__".to_string());
            if i > 0 {
                key.push('\0');
            }
            key.push_str(&v);
            values.push(v);
        }
        groups
            .entry(key)
            .or_insert_with(|| (values, Vec::new()))
            .1
            .push(row);
    }

    let mut out = Vec::with_capacity(groups.len());
    for (_, (values, rows)) in groups {
        let parts: Vec<(String, String)> = spec
            .columns
            .iter()
            .zip(values.iter())
            .map(|(spec_col, v)| (spec_col.label.clone(), v.clone()))
            .collect();
        let rel_path = render_path(&table.name, &parts);
        let parquet = encode_parquet(columns, &rows)?;
        let abs_path = opts.output_root.join(&rel_path);
        write_atomic(&abs_path, &parquet.bytes)?;
        out.push(PartitionManifest {
            path: rel_path,
            values,
            row_count: parquet.row_count,
            sha256: parquet.sha256,
            bytes: parquet.bytes.len() as u64,
        });
    }
    Ok(out)
}

fn verify_row_count(entry: &TableManifest, source_count: u64, tsv_path: &Path) -> Result<()> {
    let written: u64 = entry.partitions.iter().map(|p| p.row_count).sum();
    if written != source_count {
        return Err(Error::RowCountMismatch {
            table: entry.name.clone(),
            partition: tsv_path.display().to_string(),
            expected: source_count,
            actual: written,
        });
    }
    Ok(())
}

fn entry_warning(table: &TableEntry, manifest: &TableManifest) -> Option<String> {
    if table.partition.strategy == PartitionStrategy::SchemaOnly && manifest.row_count > 0 {
        return Some(format!(
            "table '{}' is marked schema_only but the TSV dump carries {} rows — \
             re-run the schema audit; this conversion did not write Parquet for them",
            table.name, manifest.row_count
        ));
    }
    None
}

fn now_iso8601_utc() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format_iso8601_utc(now)
}

/// Format a unix-epoch second count as `YYYY-MM-DDTHH:MM:SSZ`. Vendoring
/// this avoids pulling `chrono` for one timestamp. Algorithm: Howard
/// Hinnant's days-from-civil, adapted from C++.
pub fn format_iso8601_utc(unix_secs: u64) -> String {
    let secs = unix_secs % 86_400;
    let days = (unix_secs / 86_400) as i64;
    let hour = secs / 3600;
    let minute = (secs / 60) % 60;
    let second = secs % 60;

    // Days from civil epoch (1970-01-01) to a (year, month, day).
    let z = days + 719_468; // shift to civil epoch 0000-03-01
    let era = z.div_euclid(146_097);
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        y, m, d, hour, minute, second
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn write_file(path: &Path, body: &[u8]) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, body).unwrap();
    }

    fn minimal_plan(extra_table: Option<&str>) -> Vec<u8> {
        let mut s = String::from(
            r#"{
            "schema_version": "moves-default-db-schema/v1",
            "moves_commit": "deadbeef",
            "sources": {},
            "table_count": "#,
        );
        s += if extra_table.is_some() { "2" } else { "1" };
        s += r#",
            "tables": [
                {
                    "name": "Year",
                    "primary_key": ["yearID"],
                    "columns": [
                        {"name": "yearID", "type": "smallint"},
                        {"name": "isBaseYear", "type": "char"}
                    ],
                    "indexes": [],
                    "estimated_rows_upper_bound": 100,
                    "size_bucket": "small",
                    "filter_columns": ["yearID"],
                    "partition": {"strategy": "monolithic", "rationale": "lookup"}
                }"#;
        if let Some(name) = extra_table {
            s += &format!(
                r#",
                {{
                    "name": "{name}",
                    "primary_key": ["yearID", "countyID"],
                    "columns": [
                        {{"name": "yearID", "type": "int"}},
                        {{"name": "countyID", "type": "int"}},
                        {{"name": "value", "type": "double"}}
                    ],
                    "indexes": [],
                    "estimated_rows_upper_bound": 1000000,
                    "size_bucket": "large",
                    "filter_columns": ["yearID","countyID"],
                    "partition": {{"strategy": "year_x_county", "rationale": ""}}
                }}"#
            );
        }
        s += "\n            ]\n        }";
        s.into_bytes()
    }

    #[test]
    fn convert_monolithic_table() {
        let dir = tempdir().unwrap();
        let tsv_dir = dir.path().join("dump");
        let out_dir = dir.path().join("out");
        let plan_path = dir.path().join("tables.json");

        write_file(&plan_path, &minimal_plan(None));
        write_file(
            &tsv_dir.join("Year.schema.tsv"),
            b"yearID\tsmallint\tPRI\nisBaseYear\tchar\t\n",
        );
        write_file(
            &tsv_dir.join("Year.tsv"),
            b"1990\tY\n1995\tN\n2000\tY\n2005\tN\n",
        );

        let opts = ConvertOptions {
            tsv_dir: tsv_dir.clone(),
            plan_path: plan_path.clone(),
            output_root: out_dir.clone(),
            moves_db_version: "movesdb20241112".into(),
            generated_at_utc: Some("1970-01-01T00:00:00Z".into()),
            require_every_table: true,
        };
        let (manifest, report) = convert(&opts).unwrap();
        assert_eq!(manifest.tables.len(), 1);
        assert_eq!(manifest.tables[0].name, "Year");
        assert_eq!(manifest.tables[0].partition_strategy, "monolithic");
        assert_eq!(manifest.tables[0].row_count, 4);
        assert_eq!(manifest.tables[0].partitions.len(), 1);
        assert_eq!(manifest.tables[0].partitions[0].path, "Year.parquet");
        assert!(out_dir.join("Year.parquet").exists());
        assert!(out_dir.join("manifest.json").exists());
        assert_eq!(report.tables_written, 1);
        assert_eq!(report.partitions_written, 1);
        assert_eq!(report.total_rows, 4);
    }

    #[test]
    fn convert_year_x_county_partitions() {
        let dir = tempdir().unwrap();
        let tsv_dir = dir.path().join("dump");
        let out_dir = dir.path().join("out");
        let plan_path = dir.path().join("tables.json");

        write_file(&plan_path, &minimal_plan(Some("YxC")));
        // Year (monolithic)
        write_file(
            &tsv_dir.join("Year.schema.tsv"),
            b"yearID\tsmallint\tPRI\nisBaseYear\tchar\t\n",
        );
        write_file(&tsv_dir.join("Year.tsv"), b"2020\tY\n");
        // YxC (year_x_county partitioned)
        write_file(
            &tsv_dir.join("YxC.schema.tsv"),
            b"yearID\tint\tPRI\ncountyID\tint\tPRI\nvalue\tdouble\t\n",
        );
        write_file(
            &tsv_dir.join("YxC.tsv"),
            b"2020\t17031\t1.5\n2020\t17031\t2.5\n2020\t06037\t3.5\n2021\t17031\t4.5\n",
        );

        let opts = ConvertOptions {
            tsv_dir,
            plan_path,
            output_root: out_dir.clone(),
            moves_db_version: "movesdb20241112".into(),
            generated_at_utc: Some("1970-01-01T00:00:00Z".into()),
            require_every_table: true,
        };
        let (manifest, _) = convert(&opts).unwrap();
        // sorted lower-case: yxc before year? "yxc" > "year". So Year first.
        let yxc = manifest.tables.iter().find(|t| t.name == "YxC").unwrap();
        assert_eq!(yxc.partitions.len(), 3);
        let mut paths: Vec<&str> = yxc.partitions.iter().map(|p| p.path.as_str()).collect();
        paths.sort();
        assert_eq!(paths[0], "YxC/year=2020/county=06037/part.parquet");
        assert_eq!(paths[1], "YxC/year=2020/county=17031/part.parquet");
        assert_eq!(paths[2], "YxC/year=2021/county=17031/part.parquet");
        // Row count for the 2020/17031 group should be 2.
        let p = yxc
            .partitions
            .iter()
            .find(|p| p.path.contains("year=2020/county=17031"))
            .unwrap();
        assert_eq!(p.row_count, 2);
        // All partitions exist on disk.
        for p in &yxc.partitions {
            assert!(out_dir.join(&p.path).exists(), "missing {}", p.path);
        }
    }

    #[test]
    fn schema_only_writes_sidecar_only() {
        let dir = tempdir().unwrap();
        let tsv_dir = dir.path().join("dump");
        let out_dir = dir.path().join("out");
        let plan_path = dir.path().join("tables.json");

        let plan_body = br#"{
            "schema_version": "moves-default-db-schema/v1",
            "moves_commit": "deadbeef",
            "sources": {},
            "table_count": 1,
            "tables": [{
                "name": "Link",
                "primary_key": ["linkID"],
                "columns": [{"name":"linkID","type":"int"}],
                "indexes": [],
                "estimated_rows_upper_bound": 0,
                "size_bucket": "empty",
                "filter_columns": [],
                "partition": {"strategy": "schema_only", "rationale": "empty"}
            }]
        }"#;
        write_file(&plan_path, plan_body);
        // No TSV files written — schema_only ships empty in default DB.

        let opts = ConvertOptions {
            tsv_dir,
            plan_path,
            output_root: out_dir.clone(),
            moves_db_version: "movesdb20241112".into(),
            generated_at_utc: Some("1970-01-01T00:00:00Z".into()),
            require_every_table: false,
        };
        let (manifest, _report) = convert(&opts).unwrap();
        assert_eq!(manifest.tables.len(), 1);
        assert_eq!(manifest.tables[0].partition_strategy, "schema_only");
        assert_eq!(manifest.tables[0].row_count, 0);
        assert_eq!(
            manifest.tables[0].schema_only_path.as_deref(),
            Some("Link.schema.json")
        );
        assert!(out_dir.join("Link.schema.json").exists());
        assert!(!out_dir.join("Link.parquet").exists());
    }

    #[test]
    fn schema_only_with_unexpected_rows_emits_warning() {
        let dir = tempdir().unwrap();
        let tsv_dir = dir.path().join("dump");
        let out_dir = dir.path().join("out");
        let plan_path = dir.path().join("tables.json");

        let plan_body = br#"{
            "schema_version": "moves-default-db-schema/v1",
            "moves_commit": "deadbeef",
            "sources": {},
            "table_count": 1,
            "tables": [{
                "name": "Link",
                "primary_key": ["linkID"],
                "columns": [{"name":"linkID","type":"int"}],
                "indexes": [],
                "estimated_rows_upper_bound": 0,
                "size_bucket": "empty",
                "filter_columns": [],
                "partition": {"strategy": "schema_only", "rationale": "empty"}
            }]
        }"#;
        write_file(&plan_path, plan_body);
        write_file(&tsv_dir.join("Link.schema.tsv"), b"linkID\tint\tPRI\n");
        write_file(&tsv_dir.join("Link.tsv"), b"1\n2\n3\n");

        let opts = ConvertOptions {
            tsv_dir,
            plan_path,
            output_root: out_dir,
            moves_db_version: "movesdb20241112".into(),
            generated_at_utc: Some("1970-01-01T00:00:00Z".into()),
            require_every_table: false,
        };
        let (manifest, report) = convert(&opts).unwrap();
        assert_eq!(manifest.tables[0].row_count, 3);
        assert_eq!(report.warnings.len(), 1);
        assert!(report.warnings[0].contains("Link"));
    }

    #[test]
    fn column_drift_warns_in_lenient_mode_errors_in_strict() {
        // The audit's column list is from the canonical DDL; the actual
        // dump may have additional columns. Lenient mode warns and writes
        // using the dump's schema; strict mode errors so a CI audit catches
        // the drift.
        let dir = tempdir().unwrap();
        let tsv_dir = dir.path().join("dump");
        let out_dir = dir.path().join("out");
        let plan_path = dir.path().join("tables.json");

        write_file(&plan_path, &minimal_plan(None));
        // Audit lists 2 columns; dump emits 3 (extra `isUserInput`).
        write_file(
            &tsv_dir.join("Year.schema.tsv"),
            b"yearID\tsmallint\tPRI\nisBaseYear\tchar\t\nisUserInput\tchar\t\n",
        );
        write_file(&tsv_dir.join("Year.tsv"), b"2020\tY\tN\n");

        let lenient = ConvertOptions {
            tsv_dir: tsv_dir.clone(),
            plan_path: plan_path.clone(),
            output_root: out_dir.clone(),
            moves_db_version: "movesdb20241112".into(),
            generated_at_utc: Some("1970-01-01T00:00:00Z".into()),
            require_every_table: false,
        };
        let (_, report) = convert(&lenient).unwrap();
        assert_eq!(report.tables_written, 1);
        assert!(
            report
                .warnings
                .iter()
                .any(|w| w.contains("audit lists 2") && w.contains("dump schema has 3")),
            "expected drift warning, got: {:?}",
            report.warnings
        );

        let strict = ConvertOptions {
            require_every_table: true,
            ..lenient
        };
        let err = convert(&strict).unwrap_err();
        match err {
            Error::Plan(msg) => assert!(msg.contains("audit lists 2")),
            other => panic!("expected Error::Plan, got {other:?}"),
        }
    }

    #[test]
    fn case_insensitive_tsv_lookup_picks_lowercased_dump() {
        // The dump from a MariaDB-on-Linux load of a Windows-originated dump
        // emits lower-cased filenames (e.g. `year.tsv` / `year.schema.tsv`)
        // while the audit names the table `Year`. The converter must match.
        let dir = tempdir().unwrap();
        let tsv_dir = dir.path().join("dump");
        let out_dir = dir.path().join("out");
        let plan_path = dir.path().join("tables.json");

        write_file(&plan_path, &minimal_plan(None));
        // Note the lower-case filenames here.
        write_file(
            &tsv_dir.join("year.schema.tsv"),
            b"yearID\tsmallint\tPRI\nisBaseYear\tchar\t\n",
        );
        write_file(&tsv_dir.join("year.tsv"), b"2020\tY\n");

        let opts = ConvertOptions {
            tsv_dir,
            plan_path,
            output_root: out_dir.clone(),
            moves_db_version: "movesdb20241112".into(),
            generated_at_utc: Some("1970-01-01T00:00:00Z".into()),
            require_every_table: true,
        };
        let (manifest, report) = convert(&opts).unwrap();
        assert_eq!(report.tables_written, 1);
        // Output uses the audit's canonical CamelCase.
        assert_eq!(manifest.tables[0].name, "Year");
        assert!(out_dir.join("Year.parquet").exists());
    }

    #[test]
    fn missing_table_skipped_or_errored() {
        let dir = tempdir().unwrap();
        let tsv_dir = dir.path().join("dump");
        let out_dir = dir.path().join("out");
        let plan_path = dir.path().join("tables.json");
        write_file(&plan_path, &minimal_plan(None));
        // No TSV files written.

        let opts = ConvertOptions {
            tsv_dir: tsv_dir.clone(),
            plan_path: plan_path.clone(),
            output_root: out_dir.clone(),
            moves_db_version: "movesdb20241112".into(),
            generated_at_utc: Some("1970-01-01T00:00:00Z".into()),
            require_every_table: false,
        };
        let (manifest, report) = convert(&opts).unwrap();
        assert_eq!(manifest.tables.len(), 0);
        assert_eq!(report.skipped_tables, vec!["Year".to_string()]);

        let strict = ConvertOptions {
            require_every_table: true,
            ..opts
        };
        let err = convert(&strict).unwrap_err();
        assert!(matches!(err, Error::Plan(_)));
    }

    #[test]
    fn iso8601_format_known_vectors() {
        // 1970-01-01T00:00:00Z
        assert_eq!(format_iso8601_utc(0), "1970-01-01T00:00:00Z");
        // 2025-01-01T00:00:00Z = 1735689600
        assert_eq!(format_iso8601_utc(1_735_689_600), "2025-01-01T00:00:00Z");
        // 2000-02-29T12:34:56Z (leap day) — 951_827_696
        assert_eq!(format_iso8601_utc(951_827_696), "2000-02-29T12:34:56Z");
    }
}

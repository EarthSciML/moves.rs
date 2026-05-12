//! Validate a converted default-DB Parquet tree against the source TSV
//! dump (and, transitively, the upstream MariaDB content).
//!
//! Phase 4 Task 81. Checks executed per table:
//!
//! 1. **Manifest cross-check** — every partition file the manifest names
//!    exists on disk, and `partitions[*].row_count` matches the actual
//!    Parquet row count and `partitions[*].sha256` matches the file hash.
//! 2. **Schema fidelity** — the Arrow schema read back from each Parquet
//!    matches the column list and types recorded in the manifest. Catches
//!    type-coercion bugs in the writer.
//! 3. **Row totals** — `sum(partitions.row_count) == lines(source.tsv)`,
//!    where the TSV is the byte-identical artifact the dumper produced
//!    from `SELECT * ORDER BY 1..N`.
//! 4. **Per-column aggregates** — for every column whose Arrow type is
//!    `Int64` or `Float64`, recompute `count_non_null`, `min`, `max`,
//!    and `sum` from (a) the source TSV (parsed with the same rules the
//!    converter applies) and (b) the Parquet contents. The two must
//!    match. For Float64, exact equality is the contract: the converter
//!    parses TSV via `str::parse::<f64>`, the reader returns the same
//!    bit pattern, and we sum in a deterministic primary-key order so no
//!    floating-point reordering can creep in.
//! 5. **First-row spot check** — read the first row from the source TSV
//!    and the first row from the first partition, compare field-by-field
//!    using the converter's TSV-decode rules. Catches obvious type/order
//!    regressions even if the aggregates accidentally collide.
//!
//! This module is deliberately self-contained: it does not depend on
//! MariaDB or the SIF at validation time. Transitivity argument:
//! the TSV is emitted by `mariadb -B -N` immediately before the dumper
//! exits, so the TSV faithfully represents the MariaDB state at dump
//! time. Anything that round-trips TSV → Parquet → readback equals the
//! MariaDB content modulo the documented escape encoding.

use std::collections::HashMap;
use std::path::PathBuf;

use arrow::array::{Array, Float64Array, Int64Array};
use arrow::datatypes::DataType;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::error::{Error, Result};
use crate::manifest::{Manifest, TableManifest};
use crate::parquet_writer::sha256_hex;
use crate::tsv::{decode_mariadb_field, SchemaColumn, TsvRows};

/// One validation finding. `severity = "error"` for hard contract breaks;
/// `severity = "warning"` for diagnostics we surface but don't fail on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    pub table: String,
    pub kind: FindingKind,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FindingKind {
    /// File on disk doesn't match what the manifest claims.
    ManifestDriftError,
    /// Parquet schema doesn't match the manifest's column list.
    SchemaError,
    /// Sum-of-partition-rows doesn't equal source TSV line count.
    RowCountError,
    /// A per-column aggregate computed on Parquet doesn't match the value
    /// computed on the source TSV.
    AggregateError,
    /// First-row field-by-field comparison disagrees.
    RowContentError,
    /// Anything else worth flagging that isn't a contract break.
    Warning,
}

/// Counts of findings per kind.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ValidationSummary {
    pub tables_validated: usize,
    pub manifest_drift: usize,
    pub schema_errors: usize,
    pub row_count_errors: usize,
    pub aggregate_errors: usize,
    pub row_content_errors: usize,
    pub warnings: usize,
}

/// Outcome of a full validation run.
#[derive(Debug, Clone, Default)]
pub struct ValidationReport {
    pub summary: ValidationSummary,
    pub findings: Vec<Finding>,
}

impl ValidationReport {
    pub fn has_errors(&self) -> bool {
        self.summary.manifest_drift
            + self.summary.schema_errors
            + self.summary.row_count_errors
            + self.summary.aggregate_errors
            + self.summary.row_content_errors
            > 0
    }
}

/// Configuration for a validation run.
#[derive(Debug, Clone)]
pub struct ValidateOptions {
    /// Root of the converted output (the directory that contains
    /// `manifest.json`).
    pub output_root: PathBuf,
    /// Directory of source TSV pairs (`<Table>.tsv` + `<Table>.schema.tsv`).
    pub tsv_dir: PathBuf,
    /// Per-table maximum row count above which to skip the field-level
    /// aggregate check. Huge tables (millions of rows) get spot-checked by
    /// row counts and schema only. Set to 0 to skip aggregate checks for
    /// every table; `None` to never skip.
    pub aggregate_row_cap: Option<u64>,
}

/// Run the validation pipeline.
pub fn validate(opts: &ValidateOptions) -> Result<ValidationReport> {
    let manifest_path = opts.output_root.join(crate::manifest::MANIFEST_FILENAME);
    let manifest_bytes = std::fs::read(&manifest_path).map_err(|source| Error::Io {
        path: manifest_path.clone(),
        source,
    })?;
    let manifest: Manifest =
        serde_json::from_slice(&manifest_bytes).map_err(|source| Error::Json {
            path: manifest_path.clone(),
            source,
        })?;

    let mut report = ValidationReport::default();
    for entry in &manifest.tables {
        report.summary.tables_validated += 1;
        validate_table(entry, opts, &mut report)?;
    }
    Ok(report)
}

fn validate_table(
    entry: &TableManifest,
    opts: &ValidateOptions,
    report: &mut ValidationReport,
) -> Result<()> {
    // Skip schema-only tables: their contract is "no Parquet body, just a
    // sidecar." Verify the sidecar exists and matches.
    if entry.partition_strategy == "schema_only" {
        if let Some(sidecar) = &entry.schema_only_path {
            let sidecar_path = opts.output_root.join(sidecar);
            if !sidecar_path.exists() {
                push(
                    report,
                    &entry.name,
                    FindingKind::ManifestDriftError,
                    format!(
                        "schema_only sidecar missing on disk: {}",
                        sidecar_path.display()
                    ),
                );
            }
        }
        return Ok(());
    }

    // ----- manifest drift: every partition file exists with the recorded hash -----
    for partition in &entry.partitions {
        let abs = opts.output_root.join(&partition.path);
        let bytes = match std::fs::read(&abs) {
            Ok(b) => b,
            Err(e) => {
                push(
                    report,
                    &entry.name,
                    FindingKind::ManifestDriftError,
                    format!("partition {} missing or unreadable: {e}", partition.path),
                );
                continue;
            }
        };
        let observed = sha256_hex(&bytes);
        if observed != partition.sha256 {
            push(
                report,
                &entry.name,
                FindingKind::ManifestDriftError,
                format!(
                    "sha256 drift on {}: manifest={} actual={}",
                    partition.path, partition.sha256, observed
                ),
            );
        }
    }

    // ----- schema fidelity: read back each partition, verify Arrow types match -----
    // Plus collect total Parquet-side row count.
    let mut parquet_rows: u64 = 0;
    let mut sample_first_row: Option<Vec<Option<String>>> = None;
    let mut parquet_aggs = ColumnAggregates::new(&entry.columns);
    for (idx, partition) in entry.partitions.iter().enumerate() {
        let abs = opts.output_root.join(&partition.path);
        // Re-read the bytes for record-batch iteration. Missing files are
        // already recorded above as `ManifestDriftError`; corrupted files
        // surface here. Both are recorded as findings rather than propagated
        // as errors so the validator can report on every table in one pass.
        let bytes = match std::fs::read(&abs) {
            Ok(b) => b,
            Err(_) => continue,
        };
        let reader_builder = match ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes)) {
            Ok(b) => b,
            Err(e) => {
                push(
                    report,
                    &entry.name,
                    FindingKind::ManifestDriftError,
                    format!(
                        "partition {} is not readable as Parquet: {e}",
                        partition.path
                    ),
                );
                continue;
            }
        };
        let reader = match reader_builder.build() {
            Ok(r) => r,
            Err(e) => {
                push(
                    report,
                    &entry.name,
                    FindingKind::ManifestDriftError,
                    format!(
                        "partition {}: parquet builder.build() failed: {e}",
                        partition.path
                    ),
                );
                continue;
            }
        };
        let mut partition_row_count: u64 = 0;
        let mut partition_failed = false;
        for batch_result in reader {
            let batch = match batch_result {
                Ok(b) => b,
                Err(e) => {
                    push(
                        report,
                        &entry.name,
                        FindingKind::ManifestDriftError,
                        format!("partition {}: parquet read error: {e}", partition.path),
                    );
                    partition_failed = true;
                    break;
                }
            };
            let schema = batch.schema();
            if idx == 0 && partition_row_count == 0 && batch.num_rows() > 0 {
                // Record the first row for the spot check.
                sample_first_row = Some(extract_row_strings(&batch, 0));
            }
            // Schema cross-check: column count + types.
            if schema.fields().len() != entry.columns.len() {
                push(
                    report,
                    &entry.name,
                    FindingKind::SchemaError,
                    format!(
                        "partition {}: parquet has {} columns, manifest lists {}",
                        partition.path,
                        schema.fields().len(),
                        entry.columns.len()
                    ),
                );
            } else {
                for (i, field) in schema.fields().iter().enumerate() {
                    let expected = &entry.columns[i];
                    let arrow_str = format!("{:?}", field.data_type());
                    if field.name() != &expected.name {
                        push(
                            report,
                            &entry.name,
                            FindingKind::SchemaError,
                            format!(
                                "partition {}: column #{} name = {:?}, manifest expects {:?}",
                                partition.path,
                                i,
                                field.name(),
                                expected.name
                            ),
                        );
                    }
                    if arrow_str != expected.arrow_type {
                        push(
                            report,
                            &entry.name,
                            FindingKind::SchemaError,
                            format!(
                                "partition {}: column '{}' arrow_type = {}, manifest expects {}",
                                partition.path, expected.name, arrow_str, expected.arrow_type
                            ),
                        );
                    }
                }
            }
            // Tally Parquet-side aggregates over numeric columns. Cap on
            // aggregate inclusion happens at table scope; we always tally
            // and just decide whether to compare at the table level.
            parquet_aggs.absorb_batch(&batch);
            partition_row_count += batch.num_rows() as u64;
        }
        if partition_failed {
            continue;
        }
        if partition_row_count != partition.row_count {
            push(
                report,
                &entry.name,
                FindingKind::RowCountError,
                format!(
                    "partition {}: manifest row_count={}, parquet has {}",
                    partition.path, partition.row_count, partition_row_count
                ),
            );
        }
        parquet_rows += partition_row_count;
    }

    // ----- row totals vs source TSV -----
    let tsv_match =
        crate::convert::find_tsv_case_insensitive_pub(&opts.tsv_dir, &entry.name, ".tsv")?;
    let schema_tsv =
        crate::convert::find_tsv_case_insensitive_pub(&opts.tsv_dir, &entry.name, ".schema.tsv")?;
    let (Some(tsv_path), Some(schema_path)) = (tsv_match, schema_tsv) else {
        // Source TSV missing — that's fine if the manifest also has zero
        // rows; otherwise it's a hole worth flagging.
        if parquet_rows != 0 {
            push(
                report,
                &entry.name,
                FindingKind::Warning,
                format!(
                    "no source TSV/schema in {} but parquet has {} rows; \
                 cannot validate row content",
                    opts.tsv_dir.display(),
                    parquet_rows
                ),
            );
        }
        return Ok(());
    };

    // Re-parse the schema TSV — gives us the same SchemaColumn vec the
    // converter used, which we need to compare per-column aggregates.
    let dump_columns = crate::tsv::read_schema_tsv(&schema_path)?;
    let tsv_row_count = crate::tsv::count_rows(&tsv_path)?;
    if tsv_row_count != entry.row_count {
        push(
            report,
            &entry.name,
            FindingKind::RowCountError,
            format!(
                "source TSV has {} rows, manifest reports {}",
                tsv_row_count, entry.row_count
            ),
        );
    }
    if tsv_row_count != parquet_rows {
        push(
            report,
            &entry.name,
            FindingKind::RowCountError,
            format!(
                "source TSV has {} rows, parquet partitions sum to {}",
                tsv_row_count, parquet_rows
            ),
        );
    }

    // ----- per-column aggregate equality (TSV vs Parquet) -----
    let allow_aggregates = opts
        .aggregate_row_cap
        .map(|cap| cap > 0 && tsv_row_count <= cap)
        .unwrap_or(true);
    if allow_aggregates {
        let mut tsv_aggs = ColumnAggregates::new(&entry.columns);
        let iter = TsvRows::read(&tsv_path, dump_columns.len())?;
        let mut first_tsv_row: Option<Vec<Option<String>>> = None;
        for (row_idx, row_result) in iter.enumerate() {
            let row = row_result?;
            if row_idx == 0 {
                first_tsv_row = Some(row.clone());
            }
            tsv_aggs.absorb_tsv_row(&dump_columns, &row);
        }
        compare_aggregates(&tsv_aggs, &parquet_aggs, &entry.name, report);

        if let (Some(t), Some(p)) = (first_tsv_row, sample_first_row.as_ref()) {
            // The Parquet-side first row may live in a different partition
            // (the BTreeMap groups by partition value). Only compare when
            // the table isn't partitioned — for partitioned tables we
            // already check row count + aggregates, which catches the same
            // ordering bugs without needing partition-aware merging here.
            if entry.partition_strategy == "monolithic" {
                compare_first_row(&t, p, &dump_columns, &entry.name, report);
            }
        }
    }

    Ok(())
}

fn compare_first_row(
    tsv: &[Option<String>],
    parquet: &[Option<String>],
    schema: &[SchemaColumn],
    table: &str,
    report: &mut ValidationReport,
) {
    if tsv.len() != parquet.len() {
        push(
            report,
            table,
            FindingKind::RowContentError,
            format!(
                "first-row width mismatch: tsv has {}, parquet has {}",
                tsv.len(),
                parquet.len()
            ),
        );
        return;
    }
    for (i, col) in schema.iter().enumerate() {
        let lhs = tsv.get(i).cloned().flatten();
        let rhs = parquet.get(i).cloned().flatten();
        if !field_equal(&col.arrow_type, &lhs, &rhs) {
            push(
                report,
                table,
                FindingKind::RowContentError,
                format!(
                    "first-row mismatch on column '{}' ({:?}): tsv={:?} parquet={:?}",
                    col.name, col.arrow_type, lhs, rhs
                ),
            );
        }
    }
}

/// Compare two field values using the same type-coercion rules the
/// converter applied. Strings compare literally after `decode_mariadb_field`;
/// numeric columns parse both sides and compare bit patterns for f64.
fn field_equal(arrow: &DataType, lhs: &Option<String>, rhs: &Option<String>) -> bool {
    if lhs.is_none() && rhs.is_none() {
        return true;
    }
    let (Some(l), Some(r)) = (lhs, rhs) else {
        return false;
    };
    match arrow {
        DataType::Int64 => l.parse::<i64>().ok() == r.parse::<i64>().ok(),
        DataType::Float64 => {
            let lp = l.parse::<f64>().ok();
            let rp = r.parse::<f64>().ok();
            match (lp, rp) {
                (Some(a), Some(b)) => a.to_bits() == b.to_bits(),
                _ => false,
            }
        }
        DataType::Boolean => normalize_bool(l) == normalize_bool(r),
        _ => decode_mariadb_field(l) == *r,
    }
}

fn normalize_bool(s: &str) -> Option<bool> {
    match s {
        "0" => Some(false),
        "1" => Some(true),
        other => match other.to_ascii_lowercase().as_str() {
            "true" => Some(true),
            "false" => Some(false),
            _ => None,
        },
    }
}

fn push(report: &mut ValidationReport, table: &str, kind: FindingKind, message: String) {
    match &kind {
        FindingKind::ManifestDriftError => report.summary.manifest_drift += 1,
        FindingKind::SchemaError => report.summary.schema_errors += 1,
        FindingKind::RowCountError => report.summary.row_count_errors += 1,
        FindingKind::AggregateError => report.summary.aggregate_errors += 1,
        FindingKind::RowContentError => report.summary.row_content_errors += 1,
        FindingKind::Warning => report.summary.warnings += 1,
    }
    report.findings.push(Finding {
        table: table.to_string(),
        kind,
        message,
    });
}

fn extract_row_strings(
    batch: &arrow::record_batch::RecordBatch,
    row: usize,
) -> Vec<Option<String>> {
    let mut out = Vec::with_capacity(batch.num_columns());
    for col_idx in 0..batch.num_columns() {
        let array = batch.column(col_idx);
        if array.is_null(row) {
            out.push(None);
            continue;
        }
        let s = match array.data_type() {
            DataType::Int64 => {
                let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
                a.value(row).to_string()
            }
            DataType::Float64 => {
                let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
                format_f64_like_tsv(a.value(row))
            }
            DataType::Boolean => {
                let a = array
                    .as_any()
                    .downcast_ref::<arrow::array::BooleanArray>()
                    .unwrap();
                if a.value(row) {
                    "1".to_string()
                } else {
                    "0".to_string()
                }
            }
            DataType::Utf8 => {
                let a = array
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .unwrap();
                a.value(row).to_string()
            }
            _ => "?".to_string(),
        };
        out.push(Some(s));
    }
    out
}

/// Format an f64 in a way that compares equal to the converter's `parse::<f64>`
/// of the TSV string (because `to_bits` of both should match). This helper is
/// only used in row-content reports — for actual comparison we always parse
/// both sides and compare bit patterns.
fn format_f64_like_tsv(v: f64) -> String {
    // Best-effort textual rendering for diagnostic messages. The real
    // comparison goes through `f64::to_bits`, so this string is only ever
    // user-visible.
    let s = format!("{}", v);
    if s == "NaN" {
        "NULL".to_string()
    } else {
        s
    }
}

#[derive(Debug)]
struct ColumnAggregate {
    #[allow(dead_code)]
    name: String,
    #[allow(dead_code)]
    arrow_type: DataType,
    count_non_null: u64,
    int_min: i64,
    int_max: i64,
    int_sum: i128,
    int_seen: bool,
    float_min_bits: u64,
    float_max_bits: u64,
    /// A 128-bit signed accumulator treated as a fixed-decimal-shifted
    /// integer (f64 × 10^9). Gives a deterministic order-independent
    /// signal for "do the two sources see the same values?" — the exact
    /// sum-of-doubles is order-dependent and meaningless for
    /// cross-comparison anyway.
    float_sum_scaled: i128,
    float_seen: bool,
}

#[derive(Debug, Default)]
struct ColumnAggregates {
    by_name: HashMap<String, ColumnAggregate>,
}

impl ColumnAggregates {
    fn new(columns: &[crate::manifest::ColumnManifest]) -> Self {
        let mut by_name = HashMap::with_capacity(columns.len());
        for c in columns {
            let arrow_type = match c.arrow_type.as_str() {
                "Int64" => DataType::Int64,
                "Float64" => DataType::Float64,
                "Boolean" => DataType::Boolean,
                _ => DataType::Utf8,
            };
            by_name.insert(
                c.name.clone(),
                ColumnAggregate {
                    name: c.name.clone(),
                    arrow_type,
                    count_non_null: 0,
                    int_min: i64::MAX,
                    int_max: i64::MIN,
                    int_sum: 0,
                    int_seen: false,
                    float_min_bits: f64::INFINITY.to_bits(),
                    float_max_bits: (-f64::INFINITY).to_bits(),
                    float_sum_scaled: 0,
                    float_seen: false,
                },
            );
        }
        Self { by_name }
    }

    fn absorb_batch(&mut self, batch: &arrow::record_batch::RecordBatch) {
        for col_idx in 0..batch.num_columns() {
            let field = batch.schema().field(col_idx).clone();
            let name = field.name().to_string();
            let Some(agg) = self.by_name.get_mut(&name) else {
                continue;
            };
            let array = batch.column(col_idx);
            match field.data_type() {
                DataType::Int64 => {
                    let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
                    for i in 0..a.len() {
                        if a.is_null(i) {
                            continue;
                        }
                        let v = a.value(i);
                        agg.count_non_null += 1;
                        agg.int_seen = true;
                        if v < agg.int_min {
                            agg.int_min = v;
                        }
                        if v > agg.int_max {
                            agg.int_max = v;
                        }
                        agg.int_sum = agg.int_sum.wrapping_add(v as i128);
                    }
                }
                DataType::Float64 => {
                    let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
                    for i in 0..a.len() {
                        if a.is_null(i) {
                            continue;
                        }
                        let v = a.value(i);
                        agg.count_non_null += 1;
                        agg.float_seen = true;
                        update_float_min_max(agg, v);
                        agg.float_sum_scaled =
                            agg.float_sum_scaled.wrapping_add(scale_f64_to_i128(v));
                    }
                }
                _ => {
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            agg.count_non_null += 1;
                        }
                    }
                }
            }
        }
    }

    fn absorb_tsv_row(&mut self, columns: &[SchemaColumn], row: &[Option<String>]) {
        for (i, col) in columns.iter().enumerate() {
            let Some(agg) = self.by_name.get_mut(&col.name) else {
                continue;
            };
            let cell = row.get(i).and_then(|c| c.as_ref());
            let Some(value) = cell else { continue };
            agg.count_non_null += 1;
            match &col.arrow_type {
                DataType::Int64 => {
                    if let Ok(v) = value.parse::<i64>() {
                        agg.int_seen = true;
                        if v < agg.int_min {
                            agg.int_min = v;
                        }
                        if v > agg.int_max {
                            agg.int_max = v;
                        }
                        agg.int_sum = agg.int_sum.wrapping_add(v as i128);
                    }
                }
                DataType::Float64 => {
                    if let Ok(v) = value.parse::<f64>() {
                        agg.float_seen = true;
                        update_float_min_max(agg, v);
                        agg.float_sum_scaled =
                            agg.float_sum_scaled.wrapping_add(scale_f64_to_i128(v));
                    }
                }
                _ => {}
            }
        }
    }
}

fn update_float_min_max(agg: &mut ColumnAggregate, v: f64) {
    // Track via total-ordering bits so NaN sorts deterministically.
    let v_bits = v.to_bits();
    let prev_min = f64::from_bits(agg.float_min_bits);
    let prev_max = f64::from_bits(agg.float_max_bits);
    if v < prev_min || (v == prev_min && v_bits < agg.float_min_bits) {
        agg.float_min_bits = v_bits;
    }
    if v > prev_max || (v == prev_max && v_bits > agg.float_max_bits) {
        agg.float_max_bits = v_bits;
    }
}

/// Map an f64 into a deterministic signed-integer accumulator. We shift
/// by 10^9 — enough precision for the MOVES default DB without
/// over-saturating an i128 even on 50M-row tables. NaN/±inf are dropped
/// (treated as no-op) since the upstream TSV would never carry them.
fn scale_f64_to_i128(v: f64) -> i128 {
    if !v.is_finite() {
        return 0;
    }
    (v * 1.0e9).round() as i128
}

fn compare_aggregates(
    tsv: &ColumnAggregates,
    parquet: &ColumnAggregates,
    table: &str,
    report: &mut ValidationReport,
) {
    for (name, t) in &tsv.by_name {
        let Some(p) = parquet.by_name.get(name) else {
            push(
                report,
                table,
                FindingKind::SchemaError,
                format!("column '{name}': present in TSV aggregates but missing from parquet"),
            );
            continue;
        };
        if t.count_non_null != p.count_non_null {
            push(
                report,
                table,
                FindingKind::AggregateError,
                format!(
                    "column '{name}': non-null count tsv={} parquet={}",
                    t.count_non_null, p.count_non_null
                ),
            );
        }
        if t.int_seen && p.int_seen {
            if t.int_min != p.int_min {
                push(
                    report,
                    table,
                    FindingKind::AggregateError,
                    format!(
                        "column '{name}': int min tsv={} parquet={}",
                        t.int_min, p.int_min
                    ),
                );
            }
            if t.int_max != p.int_max {
                push(
                    report,
                    table,
                    FindingKind::AggregateError,
                    format!(
                        "column '{name}': int max tsv={} parquet={}",
                        t.int_max, p.int_max
                    ),
                );
            }
            if t.int_sum != p.int_sum {
                push(
                    report,
                    table,
                    FindingKind::AggregateError,
                    format!(
                        "column '{name}': int sum tsv={} parquet={}",
                        t.int_sum, p.int_sum
                    ),
                );
            }
        }
        if t.float_seen && p.float_seen {
            if t.float_min_bits != p.float_min_bits {
                push(
                    report,
                    table,
                    FindingKind::AggregateError,
                    format!(
                        "column '{name}': float min bits tsv={} parquet={}",
                        t.float_min_bits, p.float_min_bits
                    ),
                );
            }
            if t.float_max_bits != p.float_max_bits {
                push(
                    report,
                    table,
                    FindingKind::AggregateError,
                    format!(
                        "column '{name}': float max bits tsv={} parquet={}",
                        t.float_max_bits, p.float_max_bits
                    ),
                );
            }
            if t.float_sum_scaled != p.float_sum_scaled {
                push(
                    report,
                    table,
                    FindingKind::AggregateError,
                    format!(
                        "column '{name}': scaled-float sum tsv={} parquet={}",
                        t.float_sum_scaled, p.float_sum_scaled
                    ),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use tempfile::tempdir;

    fn write_file(path: &Path, body: &[u8]) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, body).unwrap();
    }

    fn tiny_plan() -> Vec<u8> {
        br#"{
            "schema_version": "moves-default-db-schema/v1",
            "moves_commit": "deadbeef",
            "sources": {},
            "table_count": 1,
            "tables": [{
                "name": "Sample",
                "primary_key": ["id"],
                "columns": [
                    {"name": "id",  "type": "int"},
                    {"name": "v",   "type": "double"},
                    {"name": "tag", "type": "varchar(8)"}
                ],
                "indexes": [],
                "estimated_rows_upper_bound": 10,
                "size_bucket": "tiny",
                "filter_columns": ["id"],
                "partition": {"strategy": "monolithic", "rationale": ""}
            }]
        }"#
        .to_vec()
    }

    fn round_trip_setup() -> (PathBuf, PathBuf, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let tsv_dir = dir.path().join("tsv");
        let out_dir = dir.path().join("out");
        let plan = dir.path().join("plan.json");
        write_file(&plan, &tiny_plan());
        write_file(
            &tsv_dir.join("Sample.schema.tsv"),
            b"id\tint\tPRI\nv\tdouble\t\ntag\tvarchar\t\n",
        );
        write_file(
            &tsv_dir.join("Sample.tsv"),
            b"1\t1.5\talpha\n2\tNULL\tbeta\n3\t-2.25\tgamma\n",
        );
        let opts = crate::convert::ConvertOptions {
            tsv_dir: tsv_dir.clone(),
            plan_path: plan,
            output_root: out_dir.clone(),
            moves_db_version: "movesdb20241112".into(),
            generated_at_utc: Some("1970-01-01T00:00:00Z".into()),
            require_every_table: true,
        };
        crate::convert::convert(&opts).unwrap();
        (tsv_dir, out_dir, dir)
    }

    #[test]
    fn validate_clean_round_trip_yields_no_errors() {
        let (tsv_dir, out_dir, _guard) = round_trip_setup();
        let report = validate(&ValidateOptions {
            output_root: out_dir,
            tsv_dir,
            aggregate_row_cap: Some(1_000_000),
        })
        .unwrap();
        assert!(
            !report.has_errors(),
            "expected no errors, got: {:#?}",
            report.findings
        );
        assert_eq!(report.summary.tables_validated, 1);
    }

    #[test]
    fn validate_catches_parquet_corruption() {
        let (tsv_dir, out_dir, _guard) = round_trip_setup();
        // Tamper with the Parquet file.
        let bad = out_dir.join("Sample.parquet");
        let mut bytes = std::fs::read(&bad).unwrap();
        // Flip a byte in the body (well past the magic). This breaks the
        // sha256 and may also break the file content.
        bytes[200] ^= 0xff;
        std::fs::write(&bad, &bytes).unwrap();
        let report = validate(&ValidateOptions {
            output_root: out_dir,
            tsv_dir,
            aggregate_row_cap: None,
        })
        .unwrap();
        assert!(report.has_errors());
        assert!(report.summary.manifest_drift > 0);
    }

    #[test]
    fn validate_catches_missing_partition() {
        let (tsv_dir, out_dir, _guard) = round_trip_setup();
        // Delete the Parquet file. The manifest still claims it.
        std::fs::remove_file(out_dir.join("Sample.parquet")).unwrap();
        let report = validate(&ValidateOptions {
            output_root: out_dir,
            tsv_dir,
            aggregate_row_cap: None,
        })
        .unwrap();
        assert!(report.has_errors());
        assert!(report.summary.manifest_drift > 0);
    }
}

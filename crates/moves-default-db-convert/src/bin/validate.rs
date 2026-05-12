//! `moves-default-db-validate` CLI.
//!
//! Validate a converted default-DB Parquet tree against the source TSV
//! dump (Phase 4 Task 81). Reports manifest drift, schema regressions,
//! row-count mismatches, and per-column aggregate disagreements between
//! the source TSV and the readback of the Parquet partitions.
//!
//! Usage:
//!
//! ```sh
//! moves-default-db-validate \
//!   --output-root default-db/movesdb20241112 \
//!   --tsv-dir     default-db/movesdb20241112/_tsv \
//!   [--aggregate-row-cap 2000000]
//! ```

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

use moves_default_db_convert::{validate, ValidateOptions};

#[derive(Debug, Parser)]
#[command(
    name = "moves-default-db-validate",
    about = "Validate a converted default-DB Parquet tree against its source TSV dump (Phase 4 Task 81).",
    version
)]
struct Args {
    /// Path to the converted output root (contains `manifest.json`).
    #[arg(long, value_name = "DIR")]
    output_root: PathBuf,

    /// Path to the source TSV dump directory (contains `<Table>.tsv` and
    /// `<Table>.schema.tsv` pairs). Typically `<output>/_tsv`.
    #[arg(long, value_name = "DIR")]
    tsv_dir: PathBuf,

    /// Per-table row count above which we skip the aggregate cross-check.
    /// `0` disables aggregate validation entirely (row counts + schema
    /// only). Omit to validate every row regardless of table size.
    #[arg(long, value_name = "ROWS")]
    aggregate_row_cap: Option<u64>,

    /// Emit findings as JSON instead of pretty-printed text.
    #[arg(long, default_value_t = false)]
    json: bool,

    /// Maximum number of findings to print per kind before truncating.
    /// Default 20. Does not affect the JSON output.
    #[arg(long, default_value_t = 20, value_name = "N")]
    max_findings_per_kind: usize,
}

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::from(2)
        }
    }
}

fn run() -> moves_default_db_convert::Result<ExitCode> {
    let args = Args::parse();
    let opts = ValidateOptions {
        output_root: args.output_root,
        tsv_dir: args.tsv_dir,
        aggregate_row_cap: args.aggregate_row_cap,
    };
    let report = validate(&opts)?;

    if args.json {
        emit_json(&report);
    } else {
        emit_text(&report, args.max_findings_per_kind);
    }

    Ok(if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    })
}

fn emit_text(report: &moves_default_db_convert::ValidationReport, max_per_kind: usize) {
    let s = &report.summary;
    println!(
        "validated {} tables: manifest_drift={} schema={} row_count={} aggregate={} row_content={} warnings={}",
        s.tables_validated,
        s.manifest_drift,
        s.schema_errors,
        s.row_count_errors,
        s.aggregate_errors,
        s.row_content_errors,
        s.warnings,
    );

    if report.findings.is_empty() {
        println!("clean: all checks passed");
        return;
    }

    use std::collections::BTreeMap;
    let mut grouped: BTreeMap<&'static str, Vec<&moves_default_db_convert::validate::Finding>> =
        BTreeMap::new();
    for f in &report.findings {
        let bucket = match f.kind {
            moves_default_db_convert::validate::FindingKind::ManifestDriftError => "manifest_drift",
            moves_default_db_convert::validate::FindingKind::SchemaError => "schema",
            moves_default_db_convert::validate::FindingKind::RowCountError => "row_count",
            moves_default_db_convert::validate::FindingKind::AggregateError => "aggregate",
            moves_default_db_convert::validate::FindingKind::RowContentError => "row_content",
            moves_default_db_convert::validate::FindingKind::Warning => "warning",
        };
        grouped.entry(bucket).or_default().push(f);
    }

    for (kind, findings) in grouped {
        println!("\n[{kind}] {} findings:", findings.len());
        for f in findings.iter().take(max_per_kind) {
            println!("  - {}: {}", f.table, f.message);
        }
        if findings.len() > max_per_kind {
            println!("  ... ({} more truncated)", findings.len() - max_per_kind);
        }
    }
}

fn emit_json(report: &moves_default_db_convert::ValidationReport) {
    // Roll our own simple JSON to keep the dependency surface tiny.
    let summary = &report.summary;
    let mut s = String::new();
    s.push_str("{\n");
    s.push_str("  \"summary\": {\n");
    s.push_str(&format!(
        "    \"tables_validated\": {},\n",
        summary.tables_validated
    ));
    s.push_str(&format!(
        "    \"manifest_drift\": {},\n",
        summary.manifest_drift
    ));
    s.push_str(&format!(
        "    \"schema_errors\": {},\n",
        summary.schema_errors
    ));
    s.push_str(&format!(
        "    \"row_count_errors\": {},\n",
        summary.row_count_errors
    ));
    s.push_str(&format!(
        "    \"aggregate_errors\": {},\n",
        summary.aggregate_errors
    ));
    s.push_str(&format!(
        "    \"row_content_errors\": {},\n",
        summary.row_content_errors
    ));
    s.push_str(&format!("    \"warnings\": {}\n", summary.warnings));
    s.push_str("  },\n  \"findings\": [\n");
    for (i, f) in report.findings.iter().enumerate() {
        let kind = match f.kind {
            moves_default_db_convert::validate::FindingKind::ManifestDriftError => "manifest_drift",
            moves_default_db_convert::validate::FindingKind::SchemaError => "schema",
            moves_default_db_convert::validate::FindingKind::RowCountError => "row_count",
            moves_default_db_convert::validate::FindingKind::AggregateError => "aggregate",
            moves_default_db_convert::validate::FindingKind::RowContentError => "row_content",
            moves_default_db_convert::validate::FindingKind::Warning => "warning",
        };
        let msg = f
            .message
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n");
        let comma = if i + 1 == report.findings.len() {
            ""
        } else {
            ","
        };
        s.push_str(&format!(
            "    {{\"table\": \"{}\", \"kind\": \"{kind}\", \"message\": \"{msg}\"}}{comma}\n",
            f.table
        ));
    }
    s.push_str("  ]\n}\n");
    print!("{s}");
}

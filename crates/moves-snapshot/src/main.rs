//! `moves-snapshot` CLI.
//!
//! Currently exposes a single subcommand:
//!
//! ```sh
//! moves-snapshot diff <a> <b> [--tolerance file.toml] [--format text|json] [--limit N]
//! ```
//!
//! Loads two snapshot directories and emits a structured table-row-cell diff.
//! Float columns honour an absolute, per-(table, column) tolerance loaded from
//! a TOML config (see `crates/moves-snapshot/src/tolerance.rs`).
//!
//! Exit codes:
//!
//! | code | meaning                                  |
//! |------|------------------------------------------|
//! | 0    | snapshots are equivalent within tolerance|
//! | 1    | differences detected                     |
//! | 2    | error (unreadable inputs, bad config…)   |
//!
//! The 0/1 split makes `moves-snapshot diff` a drop-in CI gate: any nonzero
//! exit short-circuits the pipeline.

use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;

use moves_snapshot::diff::DiffOptions;
use moves_snapshot::tolerance::ToleranceConfig;
use moves_snapshot::{diff_snapshots, Diff, DiffSummary, RowDiff, SchemaDiff, Snapshot};

const DIFF_REPORT_VERSION: &str = "moves-snapshot-diff/v1";

#[derive(Debug, Parser)]
#[command(
    name = "moves-snapshot",
    about = "Tools for the moves-snapshot canonical fixture-output format.",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Compute a structured diff between two snapshot directories.
    Diff(DiffArgs),
}

#[derive(Debug, Parser)]
struct DiffArgs {
    /// Left-hand snapshot directory (the baseline / expected snapshot).
    #[arg(value_name = "LHS")]
    lhs: PathBuf,

    /// Right-hand snapshot directory (the candidate / observed snapshot).
    #[arg(value_name = "RHS")]
    rhs: PathBuf,

    /// TOML tolerance config. See module docs for the file shape.
    #[arg(long, value_name = "PATH")]
    tolerance: Option<PathBuf>,

    /// Output format. `text` is the human-readable summary; `json` emits the
    /// full structured diff suitable for piping into `jq`.
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    format: OutputFormat,

    /// Cap the per-table row-diff list rendered in `text` output. `0` means
    /// unlimited. Has no effect on `json` output.
    #[arg(long, default_value_t = 25)]
    limit: usize,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum OutputFormat {
    Text,
    Json,
}

/// JSON-serializable wrapper around a [`Diff`]. Carries header information so
/// downstream tools (CI, jq scripts, the witness) can identify the inputs
/// without separately parsing CLI flags out of logs.
#[derive(Debug, Serialize)]
struct DiffReport<'a> {
    format_version: &'static str,
    lhs: &'a Path,
    rhs: &'a Path,
    tolerance_config: Option<&'a Path>,
    summary: DiffSummary,
    diff: &'a Diff,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Diff(args) => match run_diff(&args) {
            Ok(code) => code,
            Err(err) => {
                eprintln!("error: {err}");
                ExitCode::from(2)
            }
        },
    }
}

fn run_diff(args: &DiffArgs) -> Result<ExitCode, CliError> {
    let lhs = Snapshot::load(&args.lhs).map_err(|source| CliError::LoadSnapshot {
        path: args.lhs.clone(),
        source: Box::new(source),
    })?;
    let rhs = Snapshot::load(&args.rhs).map_err(|source| CliError::LoadSnapshot {
        path: args.rhs.clone(),
        source: Box::new(source),
    })?;

    let opts: DiffOptions = match &args.tolerance {
        Some(path) => ToleranceConfig::from_file(path)
            .map_err(|e| CliError::Tolerance(Box::new(e)))?
            .into(),
        None => DiffOptions::default(),
    };

    let diff = diff_snapshots(&lhs, &rhs, &opts);
    let summary = diff.summary();

    let stdout = io::stdout();
    let mut out = stdout.lock();
    match args.format {
        OutputFormat::Text => render_text(&mut out, args, &diff, &summary)?,
        OutputFormat::Json => render_json(&mut out, args, &diff, &summary)?,
    }

    Ok(if diff.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    })
}

fn render_json(
    out: &mut impl Write,
    args: &DiffArgs,
    diff: &Diff,
    summary: &DiffSummary,
) -> Result<(), CliError> {
    let report = DiffReport {
        format_version: DIFF_REPORT_VERSION,
        lhs: &args.lhs,
        rhs: &args.rhs,
        tolerance_config: args.tolerance.as_deref(),
        summary: summary.clone(),
        diff,
    };
    let mut bytes = serde_json::to_vec_pretty(&report).map_err(CliError::Json)?;
    bytes.push(b'\n');
    out.write_all(&bytes).map_err(CliError::Output)?;
    Ok(())
}

fn render_text(
    out: &mut impl Write,
    args: &DiffArgs,
    diff: &Diff,
    summary: &DiffSummary,
) -> Result<(), CliError> {
    writeln!(out, "moves-snapshot diff").map_err(CliError::Output)?;
    writeln!(out, "  lhs:  {}", args.lhs.display()).map_err(CliError::Output)?;
    writeln!(out, "  rhs:  {}", args.rhs.display()).map_err(CliError::Output)?;
    if let Some(p) = &args.tolerance {
        writeln!(out, "  tolerance: {}", p.display()).map_err(CliError::Output)?;
    }
    writeln!(out).map_err(CliError::Output)?;

    if diff.is_empty() {
        writeln!(out, "snapshots match (within configured tolerance)").map_err(CliError::Output)?;
        return Ok(());
    }

    writeln!(
        out,
        "summary: tables_added={} tables_removed={} tables_changed={} \
         schema_diffs={} rows_added={} rows_removed={} cells_changed={}",
        summary.tables_added,
        summary.tables_removed,
        summary.tables_changed,
        summary.schema_diffs,
        summary.rows_added,
        summary.rows_removed,
        summary.cells_changed
    )
    .map_err(CliError::Output)?;
    writeln!(out).map_err(CliError::Output)?;

    if !diff.tables_added.is_empty() {
        writeln!(out, "tables added (only in rhs):").map_err(CliError::Output)?;
        for t in &diff.tables_added {
            writeln!(out, "  + {t}").map_err(CliError::Output)?;
        }
        writeln!(out).map_err(CliError::Output)?;
    }
    if !diff.tables_removed.is_empty() {
        writeln!(out, "tables removed (only in lhs):").map_err(CliError::Output)?;
        for t in &diff.tables_removed {
            writeln!(out, "  - {t}").map_err(CliError::Output)?;
        }
        writeln!(out).map_err(CliError::Output)?;
    }

    for tc in &diff.table_changes {
        writeln!(
            out,
            "table {}: {} schema diff(s), {} row diff(s)",
            tc.table,
            tc.schema_diffs.len(),
            tc.row_diffs.len()
        )
        .map_err(CliError::Output)?;

        for sd in &tc.schema_diffs {
            writeln!(out, "  schema: {}", format_schema_diff(sd)).map_err(CliError::Output)?;
        }

        let cap = if args.limit == 0 {
            tc.row_diffs.len()
        } else {
            args.limit.min(tc.row_diffs.len())
        };
        for rd in tc.row_diffs.iter().take(cap) {
            writeln!(out, "  row:    {}", format_row_diff(rd)).map_err(CliError::Output)?;
        }
        if tc.row_diffs.len() > cap {
            writeln!(
                out,
                "  ... {} more row diff(s) suppressed (raise with --limit, --limit 0 = unlimited)",
                tc.row_diffs.len() - cap
            )
            .map_err(CliError::Output)?;
        }
        writeln!(out).map_err(CliError::Output)?;
    }
    Ok(())
}

fn format_schema_diff(d: &SchemaDiff) -> String {
    match d {
        SchemaDiff::ColumnAdded(c) => format!("column added: {} ({})", c.name, c.kind.as_str()),
        SchemaDiff::ColumnRemoved(c) => {
            format!("column removed: {} ({})", c.name, c.kind.as_str())
        }
        SchemaDiff::ColumnTypeChanged { name, lhs, rhs } => format!(
            "column type changed: {} ({} -> {})",
            name,
            lhs.as_str(),
            rhs.as_str()
        ),
        SchemaDiff::NaturalKeyChanged { lhs, rhs } => {
            format!("natural key changed: {lhs:?} -> {rhs:?}")
        }
    }
}

fn format_row_diff(d: &RowDiff) -> String {
    match d {
        RowDiff::Added { key } => format!("added   key={}", join_key(key)),
        RowDiff::Removed { key } => format!("removed key={}", join_key(key)),
        RowDiff::Cell {
            key,
            column,
            lhs,
            rhs,
        } => format!(
            "cell    key={} column={} lhs={} rhs={}",
            join_key(key),
            column,
            lhs.as_deref().unwrap_or("<null>"),
            rhs.as_deref().unwrap_or("<null>"),
        ),
    }
}

fn join_key(parts: &[String]) -> String {
    if parts.len() == 1 {
        parts[0].clone()
    } else {
        format!("({})", parts.join(", "))
    }
}

#[derive(Debug, thiserror::Error)]
enum CliError {
    #[error("failed to load snapshot at {path}: {source}")]
    LoadSnapshot {
        path: PathBuf,
        #[source]
        source: Box<moves_snapshot::Error>,
    },

    #[error(transparent)]
    Tolerance(#[from] Box<moves_snapshot::ToleranceError>),

    #[error("failed to serialize JSON output: {0}")]
    Json(#[source] serde_json::Error),

    #[error("failed to write output: {0}")]
    Output(#[source] std::io::Error),
}

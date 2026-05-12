//! `moves-default-db-convert` CLI.
//!
//! Reads a TSV dump directory (produced by
//! `characterization/default-db-conversion/dump-default-db.sh`), applies the
//! partitioning plan from `tables.json`, and writes the versioned Parquet
//! layout plus `manifest.json`.
//!
//! Usage:
//!
//! ```sh
//! moves-default-db-convert \
//!   --tsv-dir   /tmp/dump/movesdb20241112 \
//!   --plan      characterization/default-db-schema/tables.json \
//!   --output    default-db/movesdb20241112 \
//!   --moves-db-version movesdb20241112
//! ```

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

use moves_default_db_convert::{convert, ConvertOptions};

#[derive(Debug, Parser)]
#[command(
    name = "moves-default-db-convert",
    about = "Convert a MOVES default-DB TSV dump into partitioned Parquet (Phase 4 Task 80).",
    version
)]
struct Args {
    /// Directory containing the `<Table>.tsv` and `<Table>.schema.tsv`
    /// pairs produced by the dump stage.
    #[arg(long, value_name = "DIR")]
    tsv_dir: PathBuf,

    /// Path to `characterization/default-db-schema/tables.json`.
    #[arg(long, value_name = "PATH")]
    plan: PathBuf,

    /// Output root. The converter writes the versioned subtree plus
    /// `manifest.json` under this directory.
    #[arg(long, value_name = "DIR")]
    output: PathBuf,

    /// MOVES default DB version label (used for the manifest's
    /// `moves_db_version` field). Match the EPA release naming, e.g.
    /// `movesdb20241112`.
    #[arg(long, value_name = "LABEL")]
    moves_db_version: String,

    /// If set, error out when a table from the plan is missing from the
    /// TSV directory. Default: skip silently and report in the summary.
    #[arg(long, default_value_t = false)]
    require_every_table: bool,

    /// Override the manifest's `generated_at_utc` field. Use for
    /// reproducible diffs across runs.
    #[arg(long, value_name = "ISO8601")]
    generated_at_utc: Option<String>,
}

fn main() -> ExitCode {
    match run() {
        Ok(_) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::from(1)
        }
    }
}

fn run() -> moves_default_db_convert::Result<()> {
    let args = Args::parse();
    let opts = ConvertOptions {
        tsv_dir: args.tsv_dir,
        plan_path: args.plan,
        output_root: args.output,
        moves_db_version: args.moves_db_version,
        generated_at_utc: args.generated_at_utc,
        require_every_table: args.require_every_table,
    };
    let (_manifest, report) = convert(&opts)?;
    eprintln!(
        "[moves-default-db-convert] wrote {} tables, {} partitions, {} total rows",
        report.tables_written, report.partitions_written, report.total_rows
    );
    if !report.skipped_tables.is_empty() {
        eprintln!(
            "  skipped {} tables (not present in TSV dir): {}",
            report.skipped_tables.len(),
            report.skipped_tables.join(", ")
        );
    }
    for w in &report.warnings {
        eprintln!("  warning: {w}");
    }
    eprintln!(
        "  manifest: {}",
        opts.output_root
            .join(moves_default_db_convert::MANIFEST_FILENAME)
            .display()
    );
    Ok(())
}

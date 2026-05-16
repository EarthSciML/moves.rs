//! `moves-avft` CLI — Rust port of the Java `AVFTToolRunner`.
//!
//! Two subcommands:
//!
//! * `validate` — check that a user CSV matches the AVFT importer's rules.
//! * `tool` — gap-fill + project a user AVFT into a complete output.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use moves_avft::{csv_io, import, parquet_io, tool, AvftTable, ToolSpec};

#[derive(Debug, Parser)]
#[command(
    name = "moves-avft",
    version,
    about = "AVFT importer + AVFT Tool (Phase 4 Task 86)"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Validate a user AVFT CSV against the canonical importer rules.
    Validate {
        /// Path to the user AVFT CSV (columns: sourceTypeID, modelYearID,
        /// fuelTypeID, engTechID, fuelEngFraction).
        #[arg(long)]
        input: PathBuf,
    },
    /// Run the AVFT Tool: gap-fill + project the user AVFT into a
    /// complete table. Emits CSV and/or Parquet output.
    Tool {
        /// Path to the TOML tool spec.
        #[arg(long)]
        spec: PathBuf,
        /// Path to the user AVFT CSV.
        #[arg(long)]
        input: PathBuf,
        /// Path to the default AVFT CSV. Typically derived from the
        /// default DB's `samplevehiclepopulation` table — see
        /// `gov/epa/otaq/moves/master/gui/avfttool/AVFTTool.sql`'s
        /// `AVFTTool_CreateDefaultAVFT` procedure for the canonical
        /// computation.
        #[arg(long = "default-avft")]
        default_avft: PathBuf,
        /// Optional path to a known-fractions CSV (required only when
        /// the spec selects projection = "known-fractions").
        #[arg(long = "known-fractions")]
        known_fractions: Option<PathBuf>,
        /// Write the completed AVFT to this CSV path.
        #[arg(long = "output-csv")]
        output_csv: Option<PathBuf>,
        /// Write the completed AVFT to this Parquet path.
        #[arg(long = "output-parquet")]
        output_parquet: Option<PathBuf>,
    },
}

fn main() -> ExitCode {
    match Cli::parse().command {
        Command::Validate { input } => match cmd_validate(&input) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("error: {e}");
                ExitCode::FAILURE
            }
        },
        Command::Tool {
            spec,
            input,
            default_avft,
            known_fractions,
            output_csv,
            output_parquet,
        } => match cmd_tool(
            &spec,
            &input,
            &default_avft,
            known_fractions.as_deref(),
            output_csv.as_deref(),
            output_parquet.as_deref(),
        ) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("error: {e}");
                ExitCode::FAILURE
            }
        },
    }
}

fn cmd_validate(input: &std::path::Path) -> moves_avft::Result<()> {
    let read = csv_io::read_csv(input)?;
    if !read.duplicate_keys.is_empty() {
        eprintln!(
            "warning: {} duplicate primary key(s) in {} — last-wins applied",
            read.duplicate_keys.len(),
            input.display()
        );
    }
    let report = import::validate(&read.table)?;
    for w in &report.warnings {
        eprintln!("warning: {w:?}");
    }
    println!(
        "OK: {} rows validated from {}",
        read.table.len(),
        input.display()
    );
    Ok(())
}

fn cmd_tool(
    spec_path: &std::path::Path,
    input_path: &std::path::Path,
    default_path: &std::path::Path,
    known_path: Option<&std::path::Path>,
    output_csv: Option<&std::path::Path>,
    output_parquet: Option<&std::path::Path>,
) -> moves_avft::Result<()> {
    let spec = ToolSpec::from_toml_file(spec_path)?;
    let input = csv_io::read_csv(input_path)?.table;
    let default = csv_io::read_csv(default_path)?.table;
    let known: AvftTable = match known_path {
        Some(p) => csv_io::read_csv(p)?.table,
        None => AvftTable::new(),
    };

    let inputs = tool::ToolInputs {
        spec: &spec,
        input: &input,
        default: &default,
        known_fractions: &known,
    };
    let report = tool::run(&inputs)?;
    for m in &report.messages {
        eprintln!("note: {m:?}");
    }

    if let Some(p) = output_csv {
        csv_io::write_csv(&report.output, p)?;
        println!("wrote {} rows to {}", report.output.len(), p.display());
    }
    if let Some(p) = output_parquet {
        parquet_io::write_parquet(&report.output, p)?;
        println!("wrote {} rows to {}", report.output.len(), p.display());
    }
    if output_csv.is_none() && output_parquet.is_none() {
        println!(
            "tool ran successfully: {} rows (no --output-csv or --output-parquet specified)",
            report.output.len()
        );
    }
    Ok(())
}

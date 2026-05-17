//! `moves` — the MOVES command-line entry point (migration-plan Task 28).
//!
//! Three subcommands:
//!
//! * `moves run --runspec <path>` — parse a RunSpec, walk the calculator
//!   graph, and write output Parquet.
//! * `moves import-cdb --input <dir> --output <dir>` — validate County-scale
//!   input CSVs and write them as Parquet.
//! * `moves convert-runspec --input <path>` — convert a RunSpec between XML
//!   and TOML.
//!
//! The command logic lives in the `moves-cli` library so it is unit- and
//! integration-testable without spawning a subprocess; this file is just the
//! `clap` front end.
//!
//! Exit codes:
//!
//! | code | meaning                                                          |
//! |------|------------------------------------------------------------------|
//! | 0    | success                                                          |
//! | 1    | a command-level failure, or `import-cdb` rejected a table        |
//! | 2    | argument-parsing error (emitted by `clap`)                       |

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use moves_cli::{
    convert_runspec, import_cdb, run_simulation, ConvertOptions, ImportOptions, ImportStatus,
    RunOptions,
};

#[derive(Debug, Parser)]
#[command(
    name = "moves",
    about = "Pure-Rust port of EPA's MOVES on-road and NONROAD emissions model.",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run a MOVES simulation from a RunSpec.
    Run {
        /// RunSpec to execute (`.xml`, `.mrs`, or `.toml`).
        #[arg(long, value_name = "PATH")]
        runspec: PathBuf,

        /// Directory output Parquet is written to. Created if absent.
        #[arg(long, value_name = "DIR", default_value = "moves-output")]
        output: PathBuf,

        /// Maximum calculator chains run concurrently (0 = host parallelism).
        #[arg(long, value_name = "N", default_value_t = 0)]
        max_parallel_chunks: usize,

        /// Override calculator-chain DAG (default: the embedded Phase 1 DAG).
        #[arg(long, value_name = "PATH")]
        calculator_dag: Option<PathBuf>,

        /// Value for the `MOVESRun.runDateTime` output column. Left unset by
        /// default, which keeps the run's output byte-stable.
        #[arg(long, value_name = "ISO8601")]
        run_date_time: Option<String>,
    },

    /// Import County-database (CDB) input CSV files into Parquet.
    ImportCdb {
        /// Directory holding the `<TableName>.csv` files.
        #[arg(long, value_name = "DIR")]
        input: PathBuf,

        /// Directory the validated `<TableName>.parquet` files go to.
        #[arg(long, value_name = "DIR")]
        output: PathBuf,

        /// Converted default-DB Parquet tree, for foreign-key validation.
        /// Without it, FK checks degrade to warnings.
        #[arg(long, value_name = "DIR")]
        default_db: Option<PathBuf>,
    },

    /// Convert a RunSpec between XML and TOML.
    ConvertRunspec {
        /// RunSpec to convert (`.xml`, `.mrs`, or `.toml`).
        #[arg(long, value_name = "PATH")]
        input: PathBuf,

        /// Output path. Defaults to the input path with the format's
        /// extension swapped (`.xml` ↔ `.toml`).
        #[arg(long, value_name = "PATH")]
        output: Option<PathBuf>,
    },
}

fn main() -> ExitCode {
    match dispatch(Cli::parse().command) {
        Ok(code) => code,
        Err(err) => {
            eprintln!("error: {err:#}");
            ExitCode::FAILURE
        }
    }
}

fn dispatch(command: Command) -> anyhow::Result<ExitCode> {
    match command {
        Command::Run {
            runspec,
            output,
            max_parallel_chunks,
            calculator_dag,
            run_date_time,
        } => cmd_run(RunOptions {
            runspec,
            output,
            max_parallel_chunks,
            calculator_dag,
            run_date_time,
        }),
        Command::ImportCdb {
            input,
            output,
            default_db,
        } => cmd_import_cdb(ImportOptions {
            input,
            output,
            default_db,
        }),
        Command::ConvertRunspec { input, output } => {
            cmd_convert_runspec(ConvertOptions { input, output })
        }
    }
}

fn cmd_run(opts: RunOptions) -> anyhow::Result<ExitCode> {
    let outcome = run_simulation(&opts)?;
    println!("[moves run] {}", opts.runspec.display());
    println!(
        "  calculator graph : {} module(s) planned across {} chunk(s)",
        outcome.modules_planned.len(),
        outcome.chunk_count()
    );
    println!("  executed         : {}", outcome.modules_executed.len());
    if !outcome.modules_unimplemented.is_empty() {
        println!(
            "  not yet ported   : {} module(s) — Phase 2: no calculators ported yet",
            outcome.modules_unimplemented.len()
        );
    }
    println!("  iterations       : {}", outcome.iterations);
    println!("  max parallelism  : {}", outcome.max_parallel_chunks);
    println!("  output directory : {}", outcome.output_root.display());
    println!("  run record       : {}", outcome.run_record_path.display());
    Ok(ExitCode::SUCCESS)
}

fn cmd_import_cdb(opts: ImportOptions) -> anyhow::Result<ExitCode> {
    let outcome = import_cdb(&opts)?;
    println!("[moves import-cdb] {}", opts.input.display());
    if !outcome.default_db_used {
        println!("  note: no --default-db given — foreign-key checks are warnings only");
    }
    for table in &outcome.tables {
        match table.status {
            ImportStatus::Written => {
                println!(
                    "  ok       {:<28} {:>9} row(s)",
                    table.table, table.row_count
                );
                if let Some(destination) = &table.destination {
                    println!("             -> {}", destination.display());
                }
            }
            ImportStatus::Rejected => {
                println!(
                    "  REJECTED {:<28} {} validation error(s)",
                    table.table,
                    table.errors.len()
                );
                for error in &table.errors {
                    println!("             - {error}");
                }
            }
            ImportStatus::Missing => {
                println!(
                    "  --       {:<28} no {}.csv in input directory",
                    table.table, table.table
                );
            }
        }
        for warning in &table.warnings {
            println!("             ! {warning}");
        }
    }
    println!(
        "  {} written, {} rejected, {} missing",
        outcome.written(),
        outcome.rejected(),
        outcome.missing()
    );
    Ok(if outcome.has_errors() {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    })
}

fn cmd_convert_runspec(opts: ConvertOptions) -> anyhow::Result<ExitCode> {
    let outcome = convert_runspec(&opts)?;
    println!(
        "[moves convert-runspec] {} -> {}",
        outcome.from.label(),
        outcome.to.label()
    );
    println!("  {}", outcome.input.display());
    println!("  -> {}", outcome.output.display());
    Ok(ExitCode::SUCCESS)
}

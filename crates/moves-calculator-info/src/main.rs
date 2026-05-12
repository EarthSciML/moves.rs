//! `moves-chain-reconstruct` — Phase 1 Task 10 deliverable.
//!
//! Parses a `CalculatorInfo.txt` (and, optionally, the MOVES source tree
//! it came from) into the calculator-chain DAG JSON that Phase 2 Task 19
//! consumes.
//!
//! ```sh
//! moves-chain-reconstruct \
//!     --calculator-info /path/to/CalculatorInfo.txt \
//!     --source-dir      /path/to/EPA_MOVES_Model \
//!     --output-dir      characterization/calculator-chains
//! ```
//!
//! Exit codes:
//!
//! | code | meaning                                                            |
//! |------|--------------------------------------------------------------------|
//! | 0    | DAG JSON written successfully                                       |
//! | 1    | error (unreadable input, malformed directive, missing module, …)    |
//!
//! See the crate-level documentation for the JSON schema and the
//! determinism contract.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

use moves_calculator_info::{
    build_dag, parse_calculator_info, scan_source_dir, write_dag_json, DAG_FILE,
};

#[derive(Debug, Parser)]
#[command(
    name = "moves-chain-reconstruct",
    about = "Parse CalculatorInfo.txt (optionally with MOVES Java source) into the calculator-chain DAG JSON.",
    version
)]
struct Args {
    /// Path to `CalculatorInfo.txt` (the runtime log emitted by
    /// `InterconnectionTracker` when MOVES is built with
    /// `GENERATE_CALCULATOR_INFO_DOCUMENTATION`).
    #[arg(long, value_name = "FILE")]
    calculator_info: PathBuf,

    /// Optional path to the MOVES source tree
    /// (e.g. a clone of `https://github.com/USEPA/EPA_MOVES_Model`).
    /// When supplied, the tool scans `.java` files under it to fill in
    /// subscription metadata for calculators that didn't fire during the
    /// run that produced `CalculatorInfo.txt`.
    #[arg(long, value_name = "DIR")]
    source_dir: Option<PathBuf>,

    /// Output directory. Will be created if absent. The file is written
    /// as `<output-dir>/calculator-dag.json`.
    #[arg(long, value_name = "DIR")]
    output_dir: PathBuf,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::from(1)
        }
    }
}

fn run() -> moves_calculator_info::Result<()> {
    let args = Args::parse();
    let info = parse_calculator_info(&args.calculator_info)?;
    let java_subscriptions = match &args.source_dir {
        Some(dir) => scan_source_dir(dir)?,
        None => Vec::new(),
    };
    let dag = build_dag(&info, &java_subscriptions)?;
    let path = write_dag_json(&args.output_dir, &dag)?;

    eprintln!(
        "[moves-chain-reconstruct] wrote {} ({} regs, {} subs, {} chains, {} modules, {} (process,pollutant) keys, {} java records)",
        path.display(),
        dag.counts.registrations,
        dag.counts.subscriptions,
        dag.counts.chains,
        dag.counts.modules,
        dag.counts.unique_process_pollutant_pairs,
        java_subscriptions.len(),
    );
    eprintln!("  output file: {}", DAG_FILE);
    Ok(())
}

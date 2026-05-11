//! `moves-coverage` CLI — Phase 1 Task 9 deliverable.
//!
//! Reads every `execution-trace.json` under a snapshots root and writes
//! a suite-wide coverage rollup to `<output-dir>/coverage-map.json`.
//!
//! ```sh
//! moves-coverage \
//!   --snapshots-dir characterization/snapshots \
//!   --output-dir characterization/coverage
//! ```
//!
//! Exit codes:
//!
//! | code | meaning                                              |
//! |------|------------------------------------------------------|
//! | 0    | coverage map written successfully                    |
//! | 1    | error (unreadable snapshots root, malformed trace, …) |
//!
//! See `crate::coverage` for the schema of the emitted JSON and the
//! determinism contract.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

use moves_fixture_capture::{
    build_coverage_map, read_traces_dir, write_coverage_map, COVERAGE_FILE,
};

#[derive(Debug, Parser)]
#[command(
    name = "moves-coverage",
    about = "Aggregate per-fixture execution traces into a Phase-1 coverage map.",
    version
)]
struct Args {
    /// Snapshots root, typically `characterization/snapshots`. Each
    /// immediate subdirectory is treated as one fixture; subdirectories
    /// without an `execution-trace.json` are skipped silently.
    #[arg(long, value_name = "DIR")]
    snapshots_dir: PathBuf,

    /// Output directory. Will be created if absent. The file is written
    /// as `<output-dir>/coverage-map.json`.
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

fn run() -> moves_fixture_capture::Result<()> {
    let args = Args::parse();
    let traces = read_traces_dir(&args.snapshots_dir)?;
    let map = build_coverage_map(&traces);
    let path = write_coverage_map(&args.output_dir, &map)?;

    eprintln!(
        "[moves-coverage] wrote {} ({} fixtures, {} java classes, {} sql files, {} go calcs)",
        path.display(),
        map.total_fixtures,
        map.java_classes.len(),
        map.sql_files.len(),
        map.go_calculators.len(),
    );
    eprintln!("  total_statement_weight: {}", map.total_statement_weight);
    eprintln!("  output file:            {}", COVERAGE_FILE);
    Ok(())
}

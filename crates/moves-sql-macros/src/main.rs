//! `moves-sql-expand` — Phase 2 Task 22 deliverable.
//!
//! Reads a raw MOVES SQL script and a TOML configuration file describing the
//! macro value sets, enabled sections, and `##context.*##` replacements that
//! canonical MOVES would have applied at runtime, then prints the fully
//! expanded SQL to stdout (or `--output`).
//!
//! ```sh
//! moves-sql-expand \
//!     --script /path/to/database/BaseRateCalculator.sql \
//!     --config /path/to/runspec-bundle.toml
//! ```
//!
//! Exit codes:
//!
//! | code | meaning                                                            |
//! |------|--------------------------------------------------------------------|
//! | 0    | expanded SQL written successfully                                   |
//! | 1    | error (unreadable input, malformed TOML, set-shape mismatch, …)     |
//!
//! See the [crate-level documentation][crate] for the configuration TOML
//! schema and the macro / section semantics.

use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

use moves_sql_macros::{
    error::{Error, Result},
    ExpandConfig,
};

#[derive(Debug, Parser)]
#[command(
    name = "moves-sql-expand",
    about = "Expand a MOVES SQL script with the macro values, enabled sections, and context replacements described in a TOML config.",
    version
)]
struct Args {
    /// Path to the raw SQL script (e.g. one of the files under
    /// `database/` in the upstream `EPA_MOVES_Model` repo).
    #[arg(long, value_name = "FILE")]
    script: PathBuf,

    /// Path to the TOML configuration file. See the crate-level docs for
    /// the schema. Pass `--config /dev/null` (or an empty file) for a
    /// pass-through with no macros, no sections, no replacements.
    #[arg(long, value_name = "FILE")]
    config: PathBuf,

    /// Output file. Omit to print to stdout.
    #[arg(long, value_name = "FILE")]
    output: Option<PathBuf>,

    /// Emit a one-line summary on stderr (sections seen / kept / dropped).
    /// Off by default so the tool can be used in shell pipelines.
    #[arg(long)]
    summary: bool,
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

fn run() -> Result<()> {
    let args = Args::parse();

    let script = std::fs::read_to_string(&args.script).map_err(|source| Error::Io {
        path: args.script.clone(),
        source,
    })?;
    let raw_lines: Vec<String> = script.lines().map(|s| s.to_string()).collect();

    let cfg = ExpandConfig::load(&args.config)?;
    let expander = cfg.build_expander()?;

    let mut macro_expanded: Vec<String> = Vec::with_capacity(raw_lines.len());
    for line in &raw_lines {
        expander.expand_and_add(line, &mut macro_expanded);
    }

    let enabled: Vec<&str> = cfg.enabled_sections.iter().map(String::as_str).collect();
    let replacements = cfg.replacement_pairs();
    let out = moves_sql_macros::process_sections(&macro_expanded, &enabled, &replacements);

    write_output(&args, &out.lines)?;

    if args.summary {
        eprintln!(
            "[moves-sql-expand] {} raw line(s) -> {} macro-expanded line(s) -> {} after section filter ({} sections kept, {} dropped)",
            raw_lines.len(),
            macro_expanded.len(),
            out.lines.len(),
            out.sections_kept,
            out.sections_dropped,
        );
    }

    Ok(())
}

fn write_output(args: &Args, lines: &[String]) -> Result<()> {
    match &args.output {
        Some(path) => {
            let mut f = std::fs::File::create(path).map_err(|source| Error::Io {
                path: path.clone(),
                source,
            })?;
            for line in lines {
                writeln!(f, "{line}").map_err(|source| Error::Io {
                    path: path.clone(),
                    source,
                })?;
            }
            Ok(())
        }
        None => {
            let mut stdout = std::io::stdout().lock();
            for line in lines {
                // stdout write errors generally indicate a closed pipe;
                // there's no useful Error variant for them in the doc tool,
                // so map them to the script path for context.
                writeln!(stdout, "{line}").map_err(|source| Error::Io {
                    path: args.script.clone(),
                    source,
                })?;
            }
            Ok(())
        }
    }
}

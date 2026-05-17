//! `moves run` — load a RunSpec, walk the calculator graph, write output.
//!
//! Thin wrapper over [`moves_framework::MOVESEngine`] (migration-plan
//! Task 27): it parses the RunSpec, builds the [`CalculatorRegistry`] from
//! the Phase 1 calculator-chain DAG, hands both to the engine, and returns
//! the engine's [`EngineOutcome`].
//!
//! # The calculator DAG
//!
//! The engine needs the calculator-graph DAG that Phase 1 Task 10
//! reconstructed. The committed artifact lives at
//! `characterization/calculator-chains/calculator-dag.json`; `moves run`
//! embeds it at compile time so the binary is self-contained — running
//! `moves run` walks the real 63-module / 960-pair MOVES calculator graph
//! out of the box. A caller can still point at a different DAG with
//! `--calculator-dag` (e.g. to test against a regenerated graph).

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use moves_calculator_info::CalculatorDag;
use moves_framework::{CalculatorRegistry, EngineConfig, EngineOutcome, MOVESEngine};

use crate::load_run_spec;

/// The Phase 1 calculator-chain DAG, embedded at compile time.
///
/// `moves run` uses this whenever `--calculator-dag` is not supplied. The
/// source artifact is the byte-stable JSON written by the Phase 1
/// `moves-chain-reconstruct` tool.
const EMBEDDED_CALCULATOR_DAG: &str =
    include_str!("../../../characterization/calculator-chains/calculator-dag.json");

/// Inputs for one `moves run` invocation.
#[derive(Debug, Clone)]
pub struct RunOptions {
    /// Path to the RunSpec file (`.xml`, `.mrs`, or `.toml`).
    pub runspec: PathBuf,
    /// Directory the engine writes output Parquet into. Created if absent.
    pub output: PathBuf,
    /// `--max-parallel-chunks`: the maximum number of calculator chains run
    /// concurrently. `0` selects the host's available parallelism.
    pub max_parallel_chunks: usize,
    /// Optional override for the calculator-chain DAG. `None` uses the
    /// Phase 1 DAG embedded in the binary at compile time.
    pub calculator_dag: Option<PathBuf>,
    /// Optional value for the `MOVESRun.runDateTime` output column. `None`
    /// leaves it null, which keeps the run's output byte-stable — the
    /// engine deliberately does not stamp the wall clock itself.
    pub run_date_time: Option<String>,
}

/// Run a MOVES simulation: parse the RunSpec, build the calculator
/// registry, drive the [`MOVESEngine`], and report the outcome.
///
/// # Errors
///
/// Surfaces RunSpec load/parse failures, calculator-DAG load failures, and
/// any error the engine raises while planning, executing, or writing
/// output.
pub fn run_simulation(opts: &RunOptions) -> Result<EngineOutcome> {
    let run_spec = load_run_spec(&opts.runspec)?;
    let registry = load_registry(opts.calculator_dag.as_deref())?;
    let config = EngineConfig {
        output_root: opts.output.clone(),
        max_parallel_chunks: opts.max_parallel_chunks,
        run_spec_file_name: opts
            .runspec
            .file_name()
            .map(|name| name.to_string_lossy().into_owned()),
        run_date_time: opts.run_date_time.clone(),
    };
    let engine = MOVESEngine::new(run_spec, registry, config);
    let outcome = engine.run().context("MOVES engine run failed")?;
    Ok(outcome)
}

/// Build the [`CalculatorRegistry`] — from `path` if given, otherwise from
/// the embedded Phase 1 DAG.
fn load_registry(path: Option<&Path>) -> Result<CalculatorRegistry> {
    match path {
        Some(path) => CalculatorRegistry::load_from_json(path)
            .with_context(|| format!("loading calculator DAG from {}", path.display())),
        None => {
            let dag: CalculatorDag = serde_json::from_str(EMBEDDED_CALCULATOR_DAG)
                .context("parsing the embedded calculator-chain DAG")?;
            Ok(CalculatorRegistry::new(dag))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_dag_parses_into_a_registry() {
        let registry = load_registry(None).expect("embedded DAG should parse");
        // The Phase 1 reconstruction recovers ~63 calculator-graph modules.
        assert!(
            registry.dag().modules.len() >= 60,
            "expected ~63 modules, got {}",
            registry.dag().modules.len()
        );
    }

    #[test]
    fn load_registry_reports_a_missing_dag_file() {
        let err = load_registry(Some(Path::new("/nonexistent/dag.json"))).unwrap_err();
        assert!(
            err.to_string().contains("loading calculator DAG from"),
            "got: {err}"
        );
    }
}

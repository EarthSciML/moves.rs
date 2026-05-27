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

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use moves_calculator_info::CalculatorDag;
use moves_calculators::generators::totalactivitygenerator::allocation::{
    self as sho_alloc, ShoRow,
};
use moves_calculators::generators::totalactivitygenerator::inputs::{
    HourDayRow, LinkRow as ShoLinkRow,
};
use moves_calculators::generators::totalactivitygenerator::model::AverageSpeedRow;
use moves_framework::{
    CalculatorRegistry, CountyRow, DataFrameStore, DataFrameStoreParquet, DataFrameStoreTyped,
    EngineConfig, EngineOutcome, GeographyTables, InMemoryStore, LinkRow, MOVESEngine, TableRow,
};

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
    /// Path to a canonical MOVES snapshot directory (as written by the Phase 1
    /// capture harness). When present, the `db__movesexecution*` Parquet
    /// files under `<snapshot>/tables/` are loaded as the execution-database
    /// slow tier and made available to calculators via `ctx.tables()`.
    pub snapshot: Option<PathBuf>,
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
    let registry = load_registry(opts.calculator_dag.as_deref(), opts.snapshot.is_some())?;
    let config = EngineConfig {
        output_root: opts.output.clone(),
        max_parallel_chunks: opts.max_parallel_chunks,
        run_spec_file_name: opts
            .runspec
            .file_name()
            .map(|name| name.to_string_lossy().into_owned()),
        run_date_time: opts.run_date_time.clone(),
        collect_output_in_memory: false,
    };
    let mut engine = MOVESEngine::new(run_spec, registry, config);
    if let Some(snapshot_dir) = &opts.snapshot {
        let store = load_execution_db(snapshot_dir)
            .with_context(|| format!("loading execution DB from {}", snapshot_dir.display()))?;
        let geography = load_geography_from_store(&store)
            .with_context(|| format!("building geography from {}", snapshot_dir.display()))?;
        engine = engine.with_slow_store(store);
        engine
            .execution_run_spec_mut()
            .build_execution_locations(&geography);
    }
    let outcome = engine.run().context("MOVES engine run failed")?;
    Ok(outcome)
}

/// Build the [`CalculatorRegistry`] — from `path` if given, otherwise from
/// the embedded Phase 1 DAG.
/// Load execution-database tables from a snapshot directory.
///
/// Scans `snapshot_dir/tables/` for Parquet files whose names begin with
/// `db__movesexecution` (the canonical MOVES execution-DB prefix written by
/// the Phase 1 capture harness).  Each matching file is read and inserted
/// into the returned [`InMemoryStore`] under the table name extracted from
/// the filename suffix (the part after the last `__`, without the `.parquet`
/// extension, e.g. `samplevehicletrip`).
///
/// The [`InMemoryStore`] uses case-insensitive lookup, so calculators that
/// call `ctx.tables().iter_typed("SampleVehicleTrip")` will find the entry
/// stored as `samplevehicletrip`.
fn load_execution_db(snapshot_dir: &Path) -> Result<InMemoryStore> {
    let tables_dir = snapshot_dir.join("tables");
    let mut store = InMemoryStore::new();
    let dir = fs::read_dir(&tables_dir)
        .with_context(|| format!("reading snapshot tables dir {}", tables_dir.display()))?;
    for entry in dir {
        let entry = entry.context("reading directory entry")?;
        let file_name = entry.file_name();
        let name_str = file_name.to_string_lossy();
        // Only the execution-DB tables; skip output-DB and other prefixes.
        if !name_str.starts_with("db__movesexecution") || !name_str.ends_with(".parquet") {
            continue;
        }
        // Extract the table name: last `__`-separated segment, strip `.parquet`.
        let table_name = name_str
            .rsplit("__")
            .next()
            .unwrap_or(&name_str)
            .trim_end_matches(".parquet");
        let bytes = fs::read(entry.path())
            .with_context(|| format!("reading {}", entry.path().display()))?;
        store
            .read_parquet(table_name, &bytes)
            .with_context(|| format!("parsing {}", entry.path().display()))?;
    }
    // If the SHO table has null distances (MOVES inserts them before calculateDistance
    // runs), compute distance = SHO * averageSpeed and write back to the store.
    populate_sho_distances(&mut store).context("populating SHO distances from snapshot")?;
    Ok(store)
}

/// Compute `SHO.distance = SHO * averageSpeed` from snapshot tables and write
/// back, so downstream calculators find non-null distances in the slow store.
fn populate_sho_distances(store: &mut InMemoryStore) -> Result<()> {
    if !store.contains("SHO") {
        return Ok(());
    }
    let sho_rows: Vec<ShoRow> = store
        .iter_typed("SHO")
        .context("reading SHO from snapshot")?;
    // If distances are already non-zero, nothing to do.
    if sho_rows.iter().any(|r| r.distance != 0.0) {
        return Ok(());
    }
    let link: Vec<ShoLinkRow> = if store.contains("Link") {
        store
            .iter_typed("Link")
            .context("reading Link from snapshot")?
    } else {
        return Ok(());
    };
    let average_speed: Vec<AverageSpeedRow> = if store.contains("AverageSpeed") {
        store
            .iter_typed("AverageSpeed")
            .context("reading AverageSpeed from snapshot")?
    } else {
        return Ok(());
    };
    let hour_day: Vec<HourDayRow> = if store.contains("HourDay") {
        store
            .iter_typed("HourDay")
            .context("reading HourDay from snapshot")?
    } else {
        return Ok(());
    };
    let with_distance = sho_alloc::calculate_distance(&sho_rows, &link, &average_speed, &hour_day);
    if with_distance.is_empty() {
        return Ok(());
    }
    let df = ShoRow::into_dataframe(with_distance).context("serialising SHO with distances")?;
    store.insert("SHO", df);
    Ok(())
}

/// Cast an integer column to i32, accepting both Int32 and Int64 sources.
fn col_as_i32(df: &polars::prelude::DataFrame, name: &str) -> Result<Vec<Option<i32>>> {
    use polars::prelude::DataType;
    let s = df
        .column(name)
        .with_context(|| format!("{name} not found"))?;
    let casted = if *s.dtype() == DataType::Int64 {
        s.cast(&DataType::Int32)
            .with_context(|| format!("{name}: cast i64→i32 failed"))?
    } else {
        s.clone()
    };
    Ok(casted
        .i32()
        .with_context(|| format!("{name} not i32"))?
        .into_iter()
        .collect())
}

/// Build a [`GeographyTables`] by joining the `Link` and `County` DataFrames
/// from the slow store.
fn load_geography_from_store(store: &InMemoryStore) -> Result<GeographyTables> {
    let links: Vec<LinkRow> = if let Some(arc_df) = store.get("link") {
        let df = &*arc_df;
        let link_id = col_as_i32(df, "linkID")?;
        let county_id = col_as_i32(df, "countyID")?;
        let zone_id = col_as_i32(df, "zoneID")?;
        let road_type_id = col_as_i32(df, "roadTypeID")?;
        // Load county stateID for join
        let county_state: std::collections::HashMap<i32, i32> =
            if let Some(arc_county) = store.get("county") {
                let cdf = &*arc_county;
                let cids = col_as_i32(cdf, "countyID").ok();
                let sids = col_as_i32(cdf, "stateID").ok();
                cids.iter()
                    .zip(sids.iter())
                    .flat_map(|(c, s)| c.iter().zip(s.iter()))
                    .filter_map(|(c, s)| Some(((*c)?, (*s)?)))
                    .collect()
            } else {
                Default::default()
            };
        (0..df.height())
            .filter_map(|i| {
                let link_id = link_id[i]? as u32;
                let county_id = county_id[i]? as u32;
                let zone_id = zone_id[i]? as u32;
                let road_type_id = road_type_id[i]? as u32;
                let state_id = *county_state.get(&(county_id as i32))? as u32;
                Some(LinkRow {
                    state_id,
                    county_id,
                    zone_id,
                    link_id,
                    road_type_id,
                })
            })
            .collect()
    } else {
        Vec::new()
    };

    let counties: Vec<CountyRow> = if let Some(arc_df) = store.get("county") {
        let df = &*arc_df;
        let county_ids = col_as_i32(df, "countyID").ok();
        let state_ids = col_as_i32(df, "stateID").ok();
        match (county_ids, state_ids) {
            (Some(cids), Some(sids)) => cids
                .into_iter()
                .zip(sids)
                .filter_map(|(c, s)| {
                    Some(CountyRow {
                        state_id: s? as u32,
                        county_id: c? as u32,
                    })
                })
                .collect(),
            _ => Vec::new(),
        }
    } else {
        Vec::new()
    };

    Ok(GeographyTables::new(links, counties))
}

fn load_registry(path: Option<&Path>, with_calculators: bool) -> Result<CalculatorRegistry> {
    let mut registry = match path {
        Some(path) => CalculatorRegistry::load_from_json(path)
            .with_context(|| format!("loading calculator DAG from {}", path.display()))?,
        None => {
            let dag: CalculatorDag = serde_json::from_str(EMBEDDED_CALCULATOR_DAG)
                .context("parsing the embedded calculator-chain DAG")?;
            CalculatorRegistry::new(dag)
        }
    };
    if with_calculators {
        moves_calculators::register_all(&mut registry)
            .context("registering calculator and generator factories")?;
    }
    Ok(registry)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_dag_parses_into_a_registry() {
        let registry = load_registry(None, false).expect("embedded DAG should parse");
        // The Phase 1 reconstruction recovers ~63 calculator-graph modules.
        assert!(
            registry.dag().modules.len() >= 60,
            "expected ~63 modules, got {}",
            registry.dag().modules.len()
        );
    }

    #[test]
    fn load_registry_reports_a_missing_dag_file() {
        let err = load_registry(Some(Path::new("/nonexistent/dag.json")), false).unwrap_err();
        assert!(
            err.to_string().contains("loading calculator DAG from"),
            "got: {err}"
        );
    }
}

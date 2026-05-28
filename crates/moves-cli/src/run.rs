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

use std::collections::BTreeMap;
use std::fs;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use moves_calculator_info::CalculatorDag;
use moves_calculators::generators::totalactivitygenerator::inputs::{
    HourDayRow, LinkRow as ShoLinkRow,
};
use moves_calculators::generators::totalactivitygenerator::model::AverageSpeedRow;
use moves_framework::{
    read_execution_bundle, CalculatorRegistry, CountyRow, DataFrameStore, DataFrameStoreParquet,
    DataFrameStoreTyped, EngineConfig, EngineOutcome, GeographyTables, InMemoryStore, LinkRow,
    MOVESEngine,
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
/// Prefers the Arrow-IPC bundle at `<snapshot>/tables/execution-db.bundle` when
/// it exists (written by `moves-snapshot::Snapshot::write` since format v2).
/// Falls back to scanning `<snapshot>/tables/` for individual Parquet files
/// whose names begin with `db__movesexecution` for snapshots captured before
/// the bundle format was introduced.
///
/// All tables are stored in the returned [`InMemoryStore`] under their *short*
/// name (the last `__`-separated segment, lower-cased). The store uses
/// case-insensitive lookup, so calculators calling
/// `ctx.tables().iter_typed("SampleVehicleTrip")` will find the entry stored
/// as `samplevehicletrip`.
fn load_execution_db(snapshot_dir: &Path) -> Result<InMemoryStore> {
    let bundle_path = snapshot_dir.join("tables").join("execution-db.bundle");
    let mut store = if bundle_path.exists() {
        read_execution_bundle(&bundle_path)
            .with_context(|| format!("loading execution-DB bundle {}", bundle_path.display()))?
    } else {
        load_execution_db_from_parquet(snapshot_dir)?
    };
    // If the SHO table has null distances (MOVES inserts them before calculateDistance
    // runs), compute distance = SHO * averageSpeed and write back to the store.
    populate_sho_distances(&mut store).context("populating SHO distances from snapshot")?;
    Ok(store)
}

/// Fall-back loader: scan `<snapshot>/tables/` for individual `db__movesexecution*.parquet`
/// files (snapshots captured before the bundle format was introduced).
fn load_execution_db_from_parquet(snapshot_dir: &Path) -> Result<InMemoryStore> {
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
        let file = fs::File::open(entry.path())
            .with_context(|| format!("opening {}", entry.path().display()))?;
        store
            .read_parquet(table_name, BufReader::new(file))
            .with_context(|| format!("parsing {}", entry.path().display()))?;
    }
    Ok(store)
}

/// Compute `SHO.distance = SHO * averageSpeed` from snapshot tables and write
/// back, so downstream calculators find non-null distances in the slow store.
///
/// The implementation avoids materialising `Vec<ShoRow>` for the full SHO
/// table (which can exceed 1 M rows).  Instead it:
///   1. Reads only the four columns needed for distance arithmetic into owned
///      `Vec<i32>` / `Vec<f64>` buffers, then releases the `Arc<DataFrame>`.
///   2. Uses [`DataFrameStoreTyped::iter_typed`] for the small lookup tables
///      (Link, AverageSpeed, HourDay), which are typically <1000 rows.
///   3. Computes a `Vec<f64>` distance column and writes it back in-place via
///      [`InMemoryStore::get_mut`], which avoids cloning the Arrow buffers
///      because the outer `Arc<DataFrame>` refcount is 1 at that point.
fn populate_sho_distances(store: &mut InMemoryStore) -> Result<()> {
    use polars::prelude::{DataType, NamedFrom, Series};

    if !store.contains("SHO") {
        return Ok(());
    }

    // --- Phase 1: read SHO column data ---
    // We extract only the columns needed for the computation and collect them
    // into owned Vecs so the Arc<DataFrame> borrow is released before we need
    // &mut InMemoryStore below.
    let (link_ids, hour_day_ids, source_type_ids, sho_vals, n) = {
        let sho_arc = store
            .get("SHO")
            .context("SHO not in store after contains check")?;
        let df = &*sho_arc;

        // Case-insensitive column finder for snapshot tables (all-lowercase MySQL names).
        let find_col = |want: &str| -> Result<polars::prelude::Column> {
            let lower = want.to_ascii_lowercase();
            df.columns()
                .iter()
                .find(|c| c.name().to_ascii_lowercase() == lower)
                .cloned()
                .with_context(|| format!("SHO column '{want}' not found"))
        };

        // Early exit: if any distance is already non-zero, skip.
        let dist_nonzero = find_col("distance")
            .ok()
            .and_then(|c| c.f64().ok().cloned())
            .is_some_and(|ca| ca.into_iter().any(|v| v.is_some_and(|d| d != 0.0)));
        if dist_nonzero {
            return Ok(());
        }

        // Cast helper: accepts Int32 or Int64 snapshot columns.
        let to_i32_vec = |col: polars::prelude::Column| -> Result<Vec<i32>> {
            let casted = if *col.dtype() == DataType::Int64 {
                col.cast(&DataType::Int32)
                    .map_err(|e| anyhow::anyhow!("{e}"))?
            } else {
                col
            };
            Ok(casted
                .i32()
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .into_no_null_iter()
                .collect())
        };

        let link_ids = to_i32_vec(find_col("linkID")?)?;
        let hour_day_ids = to_i32_vec(find_col("hourDayID")?)?;
        let source_type_ids = to_i32_vec(find_col("sourceTypeID")?)?;
        let sho_vals: Vec<f64> = find_col("SHO")?
            .cast(&DataType::Float64)
            .map_err(|e| anyhow::anyhow!("SHO column: cast to f64 failed: {e}"))?
            .f64()
            .map_err(|e| anyhow::anyhow!("SHO column is not f64 after cast: {e}"))?
            .into_no_null_iter()
            .collect();
        let n = df.height();
        (link_ids, hour_day_ids, source_type_ids, sho_vals, n)
    }; // sho_arc is dropped; Arc<DataFrame> refcount returns to 1

    // --- Phase 2: build lookup tables from small reference tables ---
    if !store.contains("Link") || !store.contains("AverageSpeed") || !store.contains("HourDay") {
        return Ok(());
    }
    let link: Vec<ShoLinkRow> = store
        .iter_typed("Link")
        .context("reading Link from snapshot")?;
    let average_speed: Vec<AverageSpeedRow> = store
        .iter_typed("AverageSpeed")
        .context("reading AverageSpeed from snapshot")?;
    let hour_day: Vec<HourDayRow> = store
        .iter_typed("HourDay")
        .context("reading HourDay from snapshot")?;

    let road_type_of: BTreeMap<i32, i32> =
        link.iter().map(|l| (l.link_id, l.road_type_id)).collect();
    let day_hour_of: BTreeMap<i32, (i32, i32)> = hour_day
        .iter()
        .map(|hd| (hd.hour_day_id, (hd.day_id, hd.hour_id)))
        .collect();
    let speed_of: BTreeMap<(i32, i32, i32, i32), f64> = average_speed
        .iter()
        .map(|a| {
            (
                (a.road_type_id, a.source_type_id, a.day_id, a.hour_id),
                a.average_speed,
            )
        })
        .collect();

    // --- Phase 3: compute distance column (same formula as calculate_distance) ---
    let distances: Vec<f64> = (0..n)
        .map(|i| {
            (|| {
                let &road_type_id = road_type_of.get(&link_ids[i])?;
                let &(day_id, hour_id) = day_hour_of.get(&hour_day_ids[i])?;
                let &speed = speed_of.get(&(road_type_id, source_type_ids[i], day_id, hour_id))?;
                Some(sho_vals[i] * speed)
            })()
            .unwrap_or(0.0)
        })
        .collect();

    if distances.iter().all(|&d| d == 0.0) {
        return Ok(());
    }

    // --- Phase 4: update distance column in-place ---
    // Arc<DataFrame> refcount is 1 (sho_arc was dropped); no DataFrame clone occurs.
    let sho_mut = store.get_mut("SHO").expect("SHO was present above");
    sho_mut
        .with_column(Series::new("distance".into(), distances).into())
        .map_err(|e| anyhow::anyhow!("replacing SHO.distance: {e}"))?;
    Ok(())
}

/// Cast a DataFrame column to Int32, returning the Column for row-wise access.
///
/// Accepts Int32 (identity cast) and Int64 (narrowing cast). Returning the
/// Column instead of a Vec lets callers use `col.i32()?.get(i)` without
/// materialising an intermediate allocation.
fn cast_to_i32(df: &polars::prelude::DataFrame, name: &str) -> Result<polars::prelude::Column> {
    df.column(name)
        .with_context(|| format!("{name} not found"))?
        .cast(&polars::prelude::DataType::Int32)
        .with_context(|| format!("{name}: cast to i32 failed"))
}

/// Build a [`GeographyTables`] by joining the `Link` and `County` DataFrames
/// from the slow store.
fn load_geography_from_store(store: &InMemoryStore) -> Result<GeographyTables> {
    let links: Vec<LinkRow> = if let Some(arc_df) = store.get("link") {
        let df = &*arc_df;
        // Cast all four columns once; iterate row-wise to avoid 4 intermediate Vecs.
        let link_id_s = cast_to_i32(df, "linkID")?;
        let county_id_s = cast_to_i32(df, "countyID")?;
        let zone_id_s = cast_to_i32(df, "zoneID")?;
        let road_type_id_s = cast_to_i32(df, "roadTypeID")?;
        let link_ids = link_id_s.i32().context("linkID not i32 after cast")?;
        let county_ids = county_id_s.i32().context("countyID not i32 after cast")?;
        let zone_ids = zone_id_s.i32().context("zoneID not i32 after cast")?;
        let road_type_ids = road_type_id_s
            .i32()
            .context("roadTypeID not i32 after cast")?;
        let county_state: std::collections::HashMap<i32, i32> =
            if let Some(arc_county) = store.get("county") {
                let cdf = &*arc_county;
                let cid_s = cast_to_i32(cdf, "countyID").ok();
                let sid_s = cast_to_i32(cdf, "stateID").ok();
                match (cid_s, sid_s) {
                    (Some(cs), Some(ss)) => {
                        let cids = cs.i32().unwrap();
                        let sids = ss.i32().unwrap();
                        (0..cdf.height())
                            .filter_map(|i| Some((cids.get(i)?, sids.get(i)?)))
                            .collect()
                    }
                    _ => Default::default(),
                }
            } else {
                Default::default()
            };
        (0..df.height())
            .filter_map(|i| {
                let link_id = link_ids.get(i)? as u32;
                let county_id = county_ids.get(i)? as u32;
                let zone_id = zone_ids.get(i)? as u32;
                let road_type_id = road_type_ids.get(i)? as u32;
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
        let cid_s = cast_to_i32(df, "countyID").ok();
        let sid_s = cast_to_i32(df, "stateID").ok();
        match (cid_s, sid_s) {
            (Some(cs), Some(ss)) => {
                let cids = cs.i32().unwrap();
                let sids = ss.i32().unwrap();
                (0..df.height())
                    .filter_map(|i| {
                        Some(CountyRow {
                            state_id: sids.get(i)? as u32,
                            county_id: cids.get(i)? as u32,
                        })
                    })
                    .collect()
            }
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
    use moves_calculators::generators::totalactivitygenerator::allocation::ShoRow;
    use moves_calculators::generators::totalactivitygenerator::inputs::{
        HourDayRow as TagHourDayRow, LinkRow as TagLinkRow,
    };
    use moves_calculators::generators::totalactivitygenerator::model::AverageSpeedRow as TagAvgSpeedRow;
    use moves_framework::TableRow;
    use polars::prelude::NamedFrom;

    use super::*;

    fn make_execdb_snapshot() -> (tempfile::TempDir, moves_snapshot::Snapshot) {
        use moves_snapshot::format::ColumnKind;
        use moves_snapshot::table::{TableBuilder, Value};
        use moves_snapshot::Snapshot;

        let mut tb = TableBuilder::new(
            "db__movesexecution1__activitytype",
            [
                ("activitytypeid".to_string(), ColumnKind::Int64),
                ("activitytype".to_string(), ColumnKind::Utf8),
            ],
        )
        .unwrap()
        .with_natural_key(["activitytypeid"])
        .unwrap();
        tb.push_row([Value::Int64(1), Value::Utf8("Running Exhaust".into())])
            .unwrap();
        tb.push_row([Value::Int64(2), Value::Utf8("Start Exhaust".into())])
            .unwrap();
        let table = tb.build().unwrap();

        let mut snap = Snapshot::new();
        snap.add_table(table).unwrap();

        let dir = tempfile::tempdir().unwrap();
        snap.write(dir.path()).unwrap();
        (dir, snap)
    }

    #[test]
    fn load_execution_db_prefers_bundle_when_present() {
        let (dir, _snap) = make_execdb_snapshot();
        // The bundle should have been written by Snapshot::write.
        let bundle_path = dir.path().join("tables").join("execution-db.bundle");
        assert!(
            bundle_path.exists(),
            "bundle must exist after Snapshot::write"
        );

        let store = load_execution_db(dir.path()).expect("load must succeed from bundle");
        assert!(
            store.contains("activitytype"),
            "activitytype table must be present in store"
        );
        let df = store.get("activitytype").unwrap();
        assert_eq!(df.height(), 2, "both rows must be loaded");
    }

    #[test]
    fn load_execution_db_fallback_works_without_bundle() {
        let (dir, _snap) = make_execdb_snapshot();
        // Remove the bundle to force the per-file Parquet fallback.
        let bundle_path = dir.path().join("tables").join("execution-db.bundle");
        std::fs::remove_file(&bundle_path).unwrap();

        let store = load_execution_db(dir.path()).expect("fallback load must succeed");
        assert!(
            store.contains("activitytype"),
            "activitytype table must be present in fallback store"
        );
        let df = store.get("activitytype").unwrap();
        assert_eq!(df.height(), 2, "both rows must be loaded via fallback");
    }

    fn make_sho_store() -> InMemoryStore {
        let sho_rows = vec![
            ShoRow {
                hour_day_id: 85,
                month_id: 1,
                year_id: 2020,
                age_id: 0,
                link_id: 1001,
                source_type_id: 21,
                sho: 10.0,
                distance: 0.0,
            },
            ShoRow {
                hour_day_id: 85,
                month_id: 1,
                year_id: 2020,
                age_id: 0,
                link_id: 1000,
                source_type_id: 21,
                sho: 5.0,
                distance: 0.0,
            },
        ];
        let link_rows = vec![
            TagLinkRow {
                link_id: 1001,
                zone_id: 100,
                road_type_id: 2,
                county_id: 9,
            },
            TagLinkRow {
                link_id: 1000,
                zone_id: 100,
                road_type_id: 1,
                county_id: 9,
            },
        ];
        let avg_speed_rows = vec![TagAvgSpeedRow {
            road_type_id: 2,
            source_type_id: 21,
            day_id: 5,
            hour_id: 8,
            average_speed: 55.0,
        }];
        let hour_day_rows = vec![TagHourDayRow {
            hour_day_id: 85,
            hour_id: 8,
            day_id: 5,
        }];
        let mut store = InMemoryStore::new();
        store.insert("SHO", ShoRow::into_dataframe(sho_rows).unwrap());
        store.insert("Link", TagLinkRow::into_dataframe(link_rows).unwrap());
        store.insert(
            "AverageSpeed",
            TagAvgSpeedRow::into_dataframe(avg_speed_rows).unwrap(),
        );
        store.insert(
            "HourDay",
            TagHourDayRow::into_dataframe(hour_day_rows).unwrap(),
        );
        store
    }

    #[test]
    fn populate_sho_distances_matches_calculate_distance() {
        let mut store = make_sho_store();
        populate_sho_distances(&mut store).expect("populate_sho_distances failed");

        let sho_df = store.get("SHO").expect("SHO table missing");
        let dist = sho_df.column("distance").unwrap().f64().unwrap();
        // link 1001 (road 2): 10.0 * 55.0 = 550.0
        assert!(
            (dist.get(0).unwrap() - 550.0).abs() < 1e-9,
            "dist[0]={}",
            dist.get(0).unwrap()
        );
        // link 1000 (road 1, off-network): no AverageSpeed → 0.0
        assert_eq!(dist.get(1).unwrap(), 0.0);
    }

    #[test]
    fn populate_sho_distances_skips_when_already_set() {
        let mut store = make_sho_store();
        // Pre-populate distance on the first row.
        {
            let sho_mut = store.get_mut("SHO").unwrap();
            sho_mut
                .with_column(
                    polars::prelude::Series::new("distance".into(), vec![999.0f64, 0.0]).into(),
                )
                .unwrap();
        }
        populate_sho_distances(&mut store).expect("populate_sho_distances failed");
        // Distance should be unchanged because at least one was non-zero.
        let dist = store
            .get("SHO")
            .unwrap()
            .column("distance")
            .unwrap()
            .f64()
            .unwrap()
            .clone();
        assert!((dist.get(0).unwrap() - 999.0).abs() < 1e-9);
    }

    #[test]
    fn populate_sho_distances_noop_when_no_sho() {
        let mut store = InMemoryStore::new();
        populate_sho_distances(&mut store).expect("should be noop");
    }

    #[test]
    fn populate_sho_distances_handles_str_sho_column() {
        // Regression: snapshots from canonical MOVES sometimes serialize the
        // SHO column as Utf8/String rather than Float64. Verify the cast path
        // handles this without error and produces correct distances.
        let mut store = make_sho_store();
        {
            let df = store.get_mut("SHO").unwrap();
            let sho_as_str = df
                .column("SHO")
                .unwrap()
                .cast(&polars::prelude::DataType::String)
                .unwrap();
            df.with_column(sho_as_str).unwrap();
        }
        populate_sho_distances(&mut store).expect("populate_sho_distances failed on str SHO");

        let sho_df = store.get("SHO").expect("SHO table missing");
        let dist = sho_df.column("distance").unwrap().f64().unwrap();
        assert!(
            (dist.get(0).unwrap() - 550.0).abs() < 1e-9,
            "dist[0]={}",
            dist.get(0).unwrap()
        );
        assert_eq!(dist.get(1).unwrap(), 0.0);
    }

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
    fn evap_op_mode_generator_is_registered() {
        let registry = load_registry(None, true).expect("registry should load with calculators");
        assert!(
            registry.has_factory("EvaporativeEmissionsOperatingModeDistributionGenerator"),
            "EvaporativeEmissionsOperatingModeDistributionGenerator must have a factory registered"
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

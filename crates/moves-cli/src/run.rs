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

use std::collections::{BTreeMap, BTreeSet, HashMap};
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
    read_execution_bundle, read_execution_bundle_filtered, CalculatorRegistry, CountyRow,
    DataFrameStore, DataFrameStoreTyped, EngineConfig, EngineOutcome, GeographyTables,
    InMemoryStore, LinkRow, MOVESEngine,
};
use moves_runspec::{GeoKind, RunSpec};
use polars::prelude::{
    col, lit, Expr, LazyFrame, NamedFrom, PlRefPath, ScanArgsParquet, SerReader, Series,
};

use crate::load_run_spec;

/// The Phase 1 calculator-chain DAG, embedded at compile time.
///
/// `moves run` uses this whenever `--calculator-dag` is not supplied. The
/// source artifact is the byte-stable JSON written by the Phase 1
/// `moves-chain-reconstruct` tool.
const EMBEDDED_CALCULATOR_DAG: &str =
    include_str!("../../../characterization/calculator-chains/calculator-dag.json");

/// Per-run geographic and temporal filter values for snapshot predicate pushdown.
///
/// Derived from a [`RunSpec`] before loading the execution-DB snapshot. Only the
/// Parquet fallback path uses this; the Arrow-IPC bundle path loads tables whole.
struct SnapshotFilter {
    /// Zone IDs the run touches. Derived from County geographic selections using
    /// the MOVES convention `zone_id = county_id * 10`, plus any Zone selections.
    zone_ids: Vec<i64>,
    /// County IDs from the RunSpec's geographic selections.
    county_ids: Vec<i64>,
    /// Calendar year IDs from the RunSpec's timespan.
    year_ids: Vec<i64>,
    /// Month IDs from the RunSpec's timespan.
    month_ids: Vec<i64>,
}

impl SnapshotFilter {
    fn from_run_spec(run_spec: &RunSpec) -> Self {
        let mut county_set: BTreeSet<i64> = BTreeSet::new();
        let mut zone_set: BTreeSet<i64> = BTreeSet::new();
        for sel in &run_spec.geographic_selections {
            match sel.kind {
                GeoKind::County => {
                    let county = i64::from(sel.key);
                    county_set.insert(county);
                    // MOVES convention: the default zone for each county is county_id * 10.
                    zone_set.insert(county * 10);
                }
                GeoKind::Zone => {
                    zone_set.insert(i64::from(sel.key));
                    county_set.insert(i64::from(sel.key) / 10);
                }
                _ => {}
            }
        }
        Self {
            zone_ids: zone_set.into_iter().collect(),
            county_ids: county_set.into_iter().collect(),
            year_ids: run_spec
                .timespan
                .years
                .iter()
                .map(|&y| i64::from(y))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            // MOVES XML months are 0-indexed (key=7 → monthID=8 / August);
            // add 1 to convert from RunSpec key to MOVES internal monthID.
            month_ids: run_spec
                .timespan
                .months
                .iter()
                .map(|&m| i64::from(m) + 1)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
        }
    }
}

/// Build a Polars filter expression for a named snapshot table.
///
/// Returns `None` when no filter applies (table not recognised, or all
/// filter sets are empty — e.g. a nation-scale run has no county IDs).
fn table_filter_expr(table_name: &str, filter: &SnapshotFilter) -> Option<Expr> {
    fn ids_in(column: &str, ids: &[i64]) -> Option<Expr> {
        if ids.is_empty() {
            return None;
        }
        let s = Series::new(column.into(), ids);
        Some(col(column).is_in(lit(s), false))
    }

    fn and_opt(a: Option<Expr>, b: Option<Expr>) -> Option<Expr> {
        match (a, b) {
            (Some(x), Some(y)) => Some(x.and(y)),
            (Some(x), None) | (None, Some(x)) => Some(x),
            (None, None) => None,
        }
    }

    match table_name {
        "zonemonthhour" => and_opt(
            ids_in("zoneID", &filter.zone_ids),
            ids_in("monthID", &filter.month_ids),
        ),
        "countyyear" => and_opt(
            ids_in("countyID", &filter.county_ids),
            ids_in("yearID", &filter.year_ids),
        ),
        _ => None,
    }
}

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
    let snap_filter = opts
        .snapshot
        .is_some()
        .then(|| SnapshotFilter::from_run_spec(&run_spec));
    // Compute the allowed table set before the registry moves into the engine.
    let allowed_tables = if opts.snapshot.is_some() {
        Some(registry.required_input_tables())
    } else {
        None
    };
    let mut engine = MOVESEngine::new(run_spec, registry, config);
    if let Some(snapshot_dir) = &opts.snapshot {
        let filter = snap_filter
            .as_ref()
            .expect("built above when snapshot is Some");
        let store = load_execution_db(snapshot_dir, filter, allowed_tables.as_ref())
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
///
/// When `allowed_tables` is `Some`, only tables whose lowercased short name
/// appears in the set are loaded. Tables absent from the set are silently
/// skipped. Pass [`CalculatorRegistry::required_input_tables`] here to avoid
/// materialising tables that no registered calculator or generator consumes.
fn load_execution_db(
    snapshot_dir: &Path,
    filter: &SnapshotFilter,
    allowed_tables: Option<&BTreeSet<String>>,
) -> Result<InMemoryStore> {
    let bundle_path = snapshot_dir.join("tables").join("execution-db.bundle");
    let mut store = if bundle_path.exists() {
        match allowed_tables {
            Some(allowed) => {
                read_execution_bundle_filtered(&bundle_path, allowed).with_context(|| {
                    format!("loading execution-DB bundle {}", bundle_path.display())
                })?
            }
            None => read_execution_bundle(&bundle_path).with_context(|| {
                format!("loading execution-DB bundle {}", bundle_path.display())
            })?,
        }
    } else {
        load_execution_db_from_parquet(snapshot_dir, filter, allowed_tables)?
    };
    // If the SHO table has null distances (MOVES inserts them before calculateDistance
    // runs), compute distance = SHO * averageSpeed and write back to the store.
    populate_sho_distances(&mut store).context("populating SHO distances from snapshot")?;
    // Apply the AirToxicsDistanceCalculator.sql Section Extract Data transforms
    // to dioxinEmissionRate and metalEmissionRate: split polProcessID into
    // (processID, pollutantID) and expand modelYearGroupID to individual
    // modelYearID rows.
    transform_airtoxics_rate_tables(&mut store)
        .context("transforming airToxics emission-rate tables from snapshot")?;
    Ok(store)
}

/// If `name` ends with exactly four ASCII decimal digits (a year suffix like `2020`),
/// return the prefix without them; otherwise return `name` unchanged.
fn strip_year_suffix(name: &str) -> &str {
    let bytes = name.as_bytes();
    if bytes.len() > 4 && bytes[bytes.len() - 4..].iter().all(|b| b.is_ascii_digit()) {
        &name[..name.len() - 4]
    } else {
        name
    }
}

/// Fall-back loader: scan `<snapshot>/tables/` for individual `db__movesexecution*.parquet`
/// files (snapshots captured before the bundle format was introduced).
///
/// For tables listed in [`table_filter_expr`] (currently `ZoneMonthHour` and
/// `CountyYear`) a Polars `LazyFrame` predicate is pushed into the Parquet decoder
/// so only matching row groups are decoded.  All other tables are read whole.
///
/// When `allowed_tables` is `Some`, only tables whose lowercased short name
/// appears in the set are loaded; others are skipped before opening the file.
fn load_execution_db_from_parquet(
    snapshot_dir: &Path,
    filter: &SnapshotFilter,
    allowed_tables: Option<&BTreeSet<String>>,
) -> Result<InMemoryStore> {
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
            .trim_end_matches(".parquet")
            .to_owned();
        // Skip tables not needed by any registered calculator/generator.
        // Year-suffixed tables (e.g. `stmyTVVCoeffs2020`) are admitted when
        // their base name (e.g. `stmytvvcoeffs`) appears in the allowed set,
        // because some generators read them dynamically and declare no static
        // INPUT_TABLES entry.
        if let Some(allowed) = allowed_tables {
            let lower = table_name.to_ascii_lowercase();
            if !allowed.contains(&lower) && !allowed.contains(strip_year_suffix(&lower)) {
                continue;
            }
        }
        let path = entry.path();
        let df = if let Some(pred) = table_filter_expr(&table_name, filter) {
            let pl_path = PlRefPath::try_from_pathbuf(path.clone())
                .with_context(|| format!("building Polars path for {}", path.display()))?;
            LazyFrame::scan_parquet(pl_path, ScanArgsParquet::default())
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .filter(pred)
                .collect()
                .with_context(|| format!("filtered scan of {}", path.display()))?
        } else {
            let file =
                fs::File::open(&path).with_context(|| format!("opening {}", path.display()))?;
            polars::prelude::ParquetReader::new(BufReader::new(file))
                .finish()
                .map_err(|e| anyhow::anyhow!("parsing {}: {e}", path.display()))?
        };
        store.insert(table_name, df);
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

/// Port of `AirToxicsDistanceCalculator.sql` `Section Extract Data` for the
/// `dioxinEmissionRate` and `metalEmissionRate` tables.
///
/// Raw snapshot tables carry the default-DB schema — `polProcessID` (composite)
/// and `modelYearGroupID` (group key). The `AirToxicsDistanceCalculator` expects
/// both already split and expanded. This function applies two transforms so the
/// calculator sees the worker-extracted schema it requires:
///
/// 1. **polProcessID split**: `processID = polProcessID % 100`,
///    `pollutantID = polProcessID / 100`.
/// 2. **modelYearGroupID expansion**: each rate row fans out to one row per
///    individual model year in the group, resolved through
///    `PollutantProcessMappedModelYear`.
///
/// No-ops when `PollutantProcessMappedModelYear` is absent from the store or
/// when both rate tables are absent. Tables already in the worker-extracted
/// schema (presence of a `processid` column detected) are left unchanged.
fn transform_airtoxics_rate_tables(store: &mut InMemoryStore) -> Result<()> {
    if !store.contains("PollutantProcessMappedModelYear") {
        return Ok(());
    }
    let ppmy_map = build_pol_process_model_year_map(store)?;
    for &table in &["dioxinEmissionRate", "metalEmissionRate"] {
        if !store.contains(table) {
            continue;
        }
        // Already transformed: worker-extracted schema has processID, not polProcessID.
        let already_split = store
            .get(table)
            .map(|arc| {
                arc.columns()
                    .iter()
                    .any(|c| c.name().to_ascii_lowercase() == "processid")
            })
            .unwrap_or(false);
        if already_split {
            continue;
        }
        let expanded = expand_rate_table_rows(store, table, &ppmy_map)
            .with_context(|| format!("expanding {table}"))?;
        store.insert(table, expanded);
    }
    Ok(())
}

/// Build a `(polProcessID, modelYearGroupID) -> Vec<modelYearID>` expansion
/// map from the `PollutantProcessMappedModelYear` snapshot table.
///
/// `PollutantProcessMappedModelYear` has one row per `(polProcessID,
/// modelYearID)` pair — the unique key. Inverting to
/// `(polProcessID, modelYearGroupID)` yields the set of model years each
/// group spans, which is the join direction the rate-table expansion needs.
fn build_pol_process_model_year_map(
    store: &InMemoryStore,
) -> Result<HashMap<(i32, i32), Vec<i32>>> {
    let arc = store
        .get("PollutantProcessMappedModelYear")
        .context("PollutantProcessMappedModelYear not in store")?;
    let df = &*arc;
    let n = df.height();
    if n == 0 {
        return Ok(HashMap::new());
    }
    let col_i32 = |name: &str| -> Result<Vec<i32>> {
        let col = df
            .columns()
            .iter()
            .find(|c| c.name().to_ascii_lowercase() == name.to_ascii_lowercase())
            .with_context(|| {
                format!("column '{name}' not found in PollutantProcessMappedModelYear")
            })?
            .cast(&polars::prelude::DataType::Int32)
            .with_context(|| format!("{name}: cast to i32 failed"))?;
        Ok(col
            .i32()
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .into_no_null_iter()
            .collect())
    };
    let pol_proc_ids = col_i32("polProcessID")?;
    let my_group_ids = col_i32("modelYearGroupID")?;
    let my_ids = col_i32("modelYearID")?;
    let mut map: HashMap<(i32, i32), Vec<i32>> = HashMap::new();
    for i in 0..n {
        map.entry((pol_proc_ids[i], my_group_ids[i]))
            .or_default()
            .push(my_ids[i]);
    }
    Ok(map)
}

/// Expand one rate table from the raw snapshot schema to the worker-extracted
/// schema the `AirToxicsDistanceCalculator` expects.
///
/// **Input** (`dioxinEmissionRate`): `polProcessID`, `fuelTypeID`,
/// `modelYearGroupID`, `meanBaseRate`, …
/// **Input** (`metalEmissionRate`): same plus `sourceTypeID`.
///
/// **Output** (`dioxinEmissionRate`): `processID`, `pollutantID`, `fuelTypeID`,
/// `modelYearID`, `meanBaseRate`.
/// **Output** (`metalEmissionRate`): same plus `sourceTypeID`.
///
/// Rows whose `(polProcessID, modelYearGroupID)` key has no entry in
/// `ppmy_map` are dropped — mirroring the SQL `INNER JOIN` semantics.
fn expand_rate_table_rows(
    store: &InMemoryStore,
    table: &str,
    ppmy_map: &HashMap<(i32, i32), Vec<i32>>,
) -> Result<polars::prelude::DataFrame> {
    use polars::prelude::{DataFrame, DataType, NamedFrom, Series};

    let arc = store.get(table).context("table not in store")?;
    let df = &*arc;
    let n = df.height();

    let is_metal = table.to_ascii_lowercase() == "metalemissionrate";

    // Short-circuit: an empty table (0 rows) is common for snapshots captured
    // when AirToxicsDistanceCalculator is not active — the Parquet schema may
    // type meanBaseRate as String rather than Float64, which would cause the
    // col_f64 cast below to fail. There is nothing to expand.
    if n == 0 {
        let mut cols = vec![
            Series::new("processID".into(), Vec::<i32>::new()).into(),
            Series::new("pollutantID".into(), Vec::<i32>::new()).into(),
            Series::new("fuelTypeID".into(), Vec::<i32>::new()).into(),
        ];
        if is_metal {
            cols.push(Series::new("sourceTypeID".into(), Vec::<i32>::new()).into());
        }
        cols.extend([
            Series::new("modelYearID".into(), Vec::<i32>::new()).into(),
            Series::new("meanBaseRate".into(), Vec::<f64>::new()).into(),
        ]);
        return DataFrame::new(0, cols)
            .map_err(|e| anyhow::anyhow!("{table}: building empty DataFrame: {e}"));
    }

    let col_i32 = |name: &str| -> Result<Vec<i32>> {
        let col = df
            .columns()
            .iter()
            .find(|c| c.name().to_ascii_lowercase() == name.to_ascii_lowercase())
            .with_context(|| format!("column '{name}' not found in {table}"))?
            .cast(&DataType::Int32)
            .with_context(|| format!("{name}: cast to i32 failed"))?;
        Ok(col
            .i32()
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .into_no_null_iter()
            .collect())
    };
    let col_f64 = |name: &str| -> Result<Vec<f64>> {
        let col = df
            .columns()
            .iter()
            .find(|c| c.name().to_ascii_lowercase() == name.to_ascii_lowercase())
            .with_context(|| format!("column '{name}' not found in {table}"))?;
        let casted = if *col.dtype() == DataType::Float32 || *col.dtype() == DataType::String {
            col.cast(&DataType::Float64)
                .map_err(|e| anyhow::anyhow!("{e}"))?
        } else {
            col.clone()
        };
        Ok(casted
            .f64()
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .into_no_null_iter()
            .collect())
    };

    let pol_proc_ids = col_i32("polProcessID")?;
    let fuel_type_ids = col_i32("fuelTypeID")?;
    let my_group_ids = col_i32("modelYearGroupID")?;
    let mean_base_rates = col_f64("meanBaseRate")?;
    let source_type_ids: Option<Vec<i32>> = if is_metal {
        Some(col_i32("sourceTypeID")?)
    } else {
        None
    };

    let mut out_process_ids: Vec<i32> = Vec::new();
    let mut out_pollutant_ids: Vec<i32> = Vec::new();
    let mut out_fuel_type_ids: Vec<i32> = Vec::new();
    let mut out_source_type_ids: Vec<i32> = Vec::new();
    let mut out_model_year_ids: Vec<i32> = Vec::new();
    let mut out_mean_base_rates: Vec<f64> = Vec::new();

    for i in 0..n {
        let pol_proc = pol_proc_ids[i];
        let my_grp = my_group_ids[i];
        let Some(model_years) = ppmy_map.get(&(pol_proc, my_grp)) else {
            continue; // No expansion entry — INNER JOIN drops the row.
        };
        let process_id = pol_proc % 100;
        let pollutant_id = pol_proc / 100;
        for &my in model_years {
            out_process_ids.push(process_id);
            out_pollutant_ids.push(pollutant_id);
            out_fuel_type_ids.push(fuel_type_ids[i]);
            if is_metal {
                out_source_type_ids.push(source_type_ids.as_ref().unwrap()[i]);
            }
            out_model_year_ids.push(my);
            out_mean_base_rates.push(mean_base_rates[i]);
        }
    }

    let n_out = out_process_ids.len();
    let mut cols = vec![
        Series::new("processID".into(), out_process_ids).into(),
        Series::new("pollutantID".into(), out_pollutant_ids).into(),
        Series::new("fuelTypeID".into(), out_fuel_type_ids).into(),
    ];
    if is_metal {
        cols.push(Series::new("sourceTypeID".into(), out_source_type_ids).into());
    }
    cols.extend([
        Series::new("modelYearID".into(), out_model_year_ids).into(),
        Series::new("meanBaseRate".into(), out_mean_base_rates).into(),
    ]);
    DataFrame::new(n_out, cols)
        .map_err(|e| anyhow::anyhow!("{table}: building transformed DataFrame: {e}"))
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

        let filter = SnapshotFilter::from_run_spec(&moves_runspec::RunSpec::default());
        let store =
            load_execution_db(dir.path(), &filter, None).expect("load must succeed from bundle");
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

        let filter = SnapshotFilter::from_run_spec(&moves_runspec::RunSpec::default());
        let store =
            load_execution_db(dir.path(), &filter, None).expect("fallback load must succeed");
        assert!(
            store.contains("activitytype"),
            "activitytype table must be present in fallback store"
        );
        let df = store.get("activitytype").unwrap();
        assert_eq!(df.height(), 2, "both rows must be loaded via fallback");
    }

    #[test]
    fn load_execution_db_bundle_skips_table_not_in_allowed_set() {
        let (dir, _snap) = make_execdb_snapshot();
        let bundle_path = dir.path().join("tables").join("execution-db.bundle");
        assert!(bundle_path.exists(), "bundle must exist");

        // Build an allowed set that does NOT include "activitytype".
        let mut allowed: BTreeSet<String> = BTreeSet::new();
        allowed.insert("sometable".to_string());

        let filter = SnapshotFilter::from_run_spec(&moves_runspec::RunSpec::default());
        let store = load_execution_db(dir.path(), &filter, Some(&allowed))
            .expect("filtered load must succeed");
        assert!(
            !store.contains("activitytype"),
            "activitytype must be absent when not in allowed set"
        );
    }

    #[test]
    fn load_execution_db_bundle_loads_table_when_in_allowed_set() {
        let (dir, _snap) = make_execdb_snapshot();
        let bundle_path = dir.path().join("tables").join("execution-db.bundle");
        assert!(bundle_path.exists(), "bundle must exist");

        let mut allowed: BTreeSet<String> = BTreeSet::new();
        allowed.insert("activitytype".to_string());

        let filter = SnapshotFilter::from_run_spec(&moves_runspec::RunSpec::default());
        let store = load_execution_db(dir.path(), &filter, Some(&allowed))
            .expect("filtered load must succeed");
        assert!(
            store.contains("activitytype"),
            "activitytype must be present when in allowed set"
        );
        let df = store.get("activitytype").unwrap();
        assert_eq!(df.height(), 2);
    }

    #[test]
    fn load_execution_db_parquet_skips_table_not_in_allowed_set() {
        let (dir, _snap) = make_execdb_snapshot();
        // Remove bundle to force Parquet path.
        let bundle_path = dir.path().join("tables").join("execution-db.bundle");
        std::fs::remove_file(&bundle_path).unwrap();

        let mut allowed: BTreeSet<String> = BTreeSet::new();
        allowed.insert("sometable".to_string()); // activitytype excluded

        let filter = SnapshotFilter::from_run_spec(&moves_runspec::RunSpec::default());
        let store = load_execution_db(dir.path(), &filter, Some(&allowed))
            .expect("filtered Parquet load must succeed");
        assert!(
            !store.contains("activitytype"),
            "activitytype must be absent in Parquet path when not in allowed set"
        );
    }

    #[test]
    fn load_execution_db_parquet_loads_table_when_in_allowed_set() {
        let (dir, _snap) = make_execdb_snapshot();
        let bundle_path = dir.path().join("tables").join("execution-db.bundle");
        std::fs::remove_file(&bundle_path).unwrap();

        let mut allowed: BTreeSet<String> = BTreeSet::new();
        allowed.insert("activitytype".to_string());

        let filter = SnapshotFilter::from_run_spec(&moves_runspec::RunSpec::default());
        let store = load_execution_db(dir.path(), &filter, Some(&allowed))
            .expect("filtered Parquet load must succeed");
        assert!(
            store.contains("activitytype"),
            "activitytype must be present when in allowed set"
        );
        let df = store.get("activitytype").unwrap();
        assert_eq!(df.height(), 2);
    }

    #[test]
    fn load_execution_db_parquet_admits_year_suffixed_table_when_base_in_allowed() {
        use moves_snapshot::format::ColumnKind;
        use moves_snapshot::table::{TableBuilder, Value};
        use moves_snapshot::Snapshot;

        // Build a snapshot with a year-suffixed table (stmytvvcoeffs2020).
        let mut tb = TableBuilder::new(
            "db__movesexecution1__stmytvvcoeffs2020",
            [
                ("sourcetypeid".to_string(), ColumnKind::Int64),
                ("coeff".to_string(), ColumnKind::Float64),
            ],
        )
        .unwrap()
        .with_natural_key(["sourcetypeid"])
        .unwrap();
        tb.push_row([Value::Int64(21), Value::Float64(1.5)])
            .unwrap();
        let table = tb.build().unwrap();
        let mut snap = Snapshot::new();
        snap.add_table(table).unwrap();
        let dir = tempfile::tempdir().unwrap();
        snap.write(dir.path()).unwrap();

        // Remove bundle to force Parquet path.
        let bundle_path = dir.path().join("tables").join("execution-db.bundle");
        std::fs::remove_file(&bundle_path).unwrap();

        // Allowed set contains only the base name — no year suffix.
        let mut allowed: BTreeSet<String> = BTreeSet::new();
        allowed.insert("stmytvvcoeffs".to_string());

        let filter = SnapshotFilter::from_run_spec(&moves_runspec::RunSpec::default());
        let store = load_execution_db(dir.path(), &filter, Some(&allowed))
            .expect("year-suffixed Parquet load must succeed");
        assert!(
            store.contains("stmytvvcoeffs2020"),
            "year-suffixed table must be admitted when base name is in allowed set"
        );
        assert_eq!(store.get("stmytvvcoeffs2020").unwrap().height(), 1);
    }

    #[test]
    fn strip_year_suffix_strips_four_digits() {
        assert_eq!(strip_year_suffix("stmytvvcoeffs2020"), "stmytvvcoeffs");
        assert_eq!(strip_year_suffix("activitytype"), "activitytype");
        assert_eq!(strip_year_suffix("table202"), "table202");
        assert_eq!(strip_year_suffix("table20a0"), "table20a0");
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

    // Helper: build a minimal store for airtoxics rate-table transform tests.
    //
    // PollutantProcessMappedModelYear: polProcessID=13001 (pollutantID=130,
    // processID=1), modelYearGroupID=30140010, modelYearID in {2010, 2015}.
    // dioxinEmissionRate raw row: (polProcessID=13001, fuelTypeID=2,
    //   modelYearGroupID=30140010, meanBaseRate=0.05).
    // metalEmissionRate raw row: (polProcessID=6301, fuelTypeID=2,
    //   sourceTypeID=21, modelYearGroupID=30140010, meanBaseRate=0.02).
    fn make_airtoxics_raw_store() -> InMemoryStore {
        use polars::prelude::{DataFrame, NamedFrom, Series};

        let mut store = InMemoryStore::new();

        // PollutantProcessMappedModelYear: two model years for group 30140010.
        let ppmy_df = DataFrame::new(
            2,
            vec![
                Series::new("polProcessID".into(), vec![13001i64, 13001i64]).into(),
                Series::new("modelYearID".into(), vec![2010i64, 2015i64]).into(),
                Series::new("modelYearGroupID".into(), vec![30140010i64, 30140010i64]).into(),
                Series::new("fuelMYGroupID".into(), vec![0i64, 0i64]).into(),
                Series::new("IMModelYearGroupID".into(), vec![0i64, 0i64]).into(),
            ],
        )
        .unwrap();
        store.insert("PollutantProcessMappedModelYear", ppmy_df);

        // Raw dioxinEmissionRate (one row: polProcessID=13001, modelYearGroupID=30140010).
        let dioxin_df = DataFrame::new(
            1,
            vec![
                Series::new("polProcessID".into(), vec![13001i64]).into(),
                Series::new("fuelTypeID".into(), vec![2i64]).into(),
                Series::new("modelYearGroupID".into(), vec![30140010i64]).into(),
                Series::new("units".into(), vec!["g/mile".to_string()]).into(),
                Series::new("meanBaseRate".into(), vec![0.05f64]).into(),
                Series::new("meanBaseRateCV".into(), vec![0.0f64]).into(),
                Series::new("dataSourceId".into(), vec![1i64]).into(),
            ],
        )
        .unwrap();
        store.insert("dioxinEmissionRate", dioxin_df);

        // Raw metalEmissionRate (one row: polProcessID=6301, modelYearGroupID=30140010).
        // polProcessID=6301: pollutantID=63, processID=1.
        // PPMY for polProcessID=6301 not in store — we add it below.
        let ppmy_df2 = {
            let ppmy = store.get("PollutantProcessMappedModelYear").unwrap();
            let mut ppmy2 = (*ppmy).clone();
            // Append two more rows for polProcessID=6301, same group.
            let new_df = DataFrame::new(
                2,
                vec![
                    Series::new("polProcessID".into(), vec![6301i64, 6301i64]).into(),
                    Series::new("modelYearID".into(), vec![2010i64, 2015i64]).into(),
                    Series::new("modelYearGroupID".into(), vec![30140010i64, 30140010i64]).into(),
                    Series::new("fuelMYGroupID".into(), vec![0i64, 0i64]).into(),
                    Series::new("IMModelYearGroupID".into(), vec![0i64, 0i64]).into(),
                ],
            )
            .unwrap();
            ppmy2.vstack_mut(&new_df).unwrap();
            ppmy2
        };
        store.insert("PollutantProcessMappedModelYear", ppmy_df2);

        let metal_df = DataFrame::new(
            1,
            vec![
                Series::new("polProcessID".into(), vec![6301i64]).into(),
                Series::new("fuelTypeID".into(), vec![2i64]).into(),
                Series::new("sourceTypeID".into(), vec![21i64]).into(),
                Series::new("modelYearGroupID".into(), vec![30140010i64]).into(),
                Series::new("units".into(), vec!["g/mile".to_string()]).into(),
                Series::new("meanBaseRate".into(), vec![0.02f64]).into(),
                Series::new("meanBaseRateCV".into(), vec![0.0f64]).into(),
                Series::new("dataSourceId".into(), vec![1i64]).into(),
            ],
        )
        .unwrap();
        store.insert("metalEmissionRate", metal_df);

        store
    }

    #[test]
    fn transform_dioxin_splits_pol_process_and_expands_model_years() {
        let mut store = make_airtoxics_raw_store();
        transform_airtoxics_rate_tables(&mut store).expect("transform failed");

        let df = store
            .get("dioxinemissionrate")
            .expect("table must be present");
        // One raw row × two model years in the group → two output rows.
        assert_eq!(
            df.height(),
            2,
            "expected 2 expanded rows, got {}",
            df.height()
        );

        let col_i32 = |name: &str| -> Vec<i32> {
            df.columns()
                .iter()
                .find(|c| c.name().to_ascii_lowercase() == name)
                .unwrap()
                .cast(&polars::prelude::DataType::Int32)
                .unwrap()
                .i32()
                .unwrap()
                .into_no_null_iter()
                .collect()
        };
        let process_ids = col_i32("processid");
        let pollutant_ids = col_i32("pollutantid");
        let fuel_type_ids = col_i32("fueltypeid");
        let model_year_ids = col_i32("modelyearid");
        let rates: Vec<f64> = df
            .columns()
            .iter()
            .find(|c| c.name().to_ascii_lowercase() == "meanbaserate")
            .unwrap()
            .f64()
            .unwrap()
            .into_no_null_iter()
            .collect();

        // polProcessID=13001: processID=13001%100=1, pollutantID=13001/100=130.
        assert!(process_ids.iter().all(|&p| p == 1), "processID must be 1");
        assert!(
            pollutant_ids.iter().all(|&p| p == 130),
            "pollutantID must be 130"
        );
        assert!(
            fuel_type_ids.iter().all(|&f| f == 2),
            "fuelTypeID must be 2"
        );
        // Both model years from the group must appear.
        let mut my_sorted = model_year_ids.clone();
        my_sorted.sort();
        assert_eq!(my_sorted, vec![2010, 2015]);
        assert!(rates.iter().all(|&r| (r - 0.05).abs() < 1e-12));
        // Transformed table must NOT have polProcessID or modelYearGroupID columns.
        assert!(
            !df.columns()
                .iter()
                .any(|c| c.name().to_ascii_lowercase() == "polprocessid"),
            "polProcessID must be absent after transform"
        );
    }

    #[test]
    fn transform_metal_includes_source_type_id() {
        let mut store = make_airtoxics_raw_store();
        transform_airtoxics_rate_tables(&mut store).expect("transform failed");

        let df = store
            .get("metalemissionrate")
            .expect("table must be present");
        assert_eq!(df.height(), 2);

        let col_i32 = |name: &str| -> Vec<i32> {
            df.columns()
                .iter()
                .find(|c| c.name().to_ascii_lowercase() == name)
                .unwrap()
                .cast(&polars::prelude::DataType::Int32)
                .unwrap()
                .i32()
                .unwrap()
                .into_no_null_iter()
                .collect()
        };
        // polProcessID=6301: processID=1, pollutantID=63.
        assert!(col_i32("processid").iter().all(|&p| p == 1));
        assert!(col_i32("pollutantid").iter().all(|&p| p == 63));
        assert!(col_i32("sourcetypeid").iter().all(|&s| s == 21));
        let mut my_sorted = col_i32("modelyearid");
        my_sorted.sort();
        assert_eq!(my_sorted, vec![2010, 2015]);
    }

    #[test]
    fn transform_noop_when_ppmy_absent() {
        let mut store = InMemoryStore::new();
        // Add raw dioxin rate but no PPMY — transform should silently skip.
        store.insert(
            "dioxinEmissionRate",
            polars::prelude::DataFrame::new(
                1,
                vec![
                    polars::prelude::Series::new("polProcessID".into(), vec![13001i64]).into(),
                    polars::prelude::Series::new("fuelTypeID".into(), vec![2i64]).into(),
                    polars::prelude::Series::new("modelYearGroupID".into(), vec![30140010i64])
                        .into(),
                    polars::prelude::Series::new("meanBaseRate".into(), vec![0.05f64]).into(),
                ],
            )
            .unwrap(),
        );
        transform_airtoxics_rate_tables(&mut store).expect("noop should not error");
        // Table should be unchanged — still has polProcessID.
        let df = store.get("dioxinemissionrate").unwrap();
        assert!(
            df.columns()
                .iter()
                .any(|c| c.name().to_ascii_lowercase() == "polprocessid"),
            "polProcessID should still be present when PPMY is absent"
        );
    }

    #[test]
    fn transform_noop_when_already_transformed() {
        let mut store = make_airtoxics_raw_store();
        // Apply once to get the transformed version.
        transform_airtoxics_rate_tables(&mut store).expect("first transform failed");
        // Apply again — must be idempotent (no error, same row count).
        transform_airtoxics_rate_tables(&mut store).expect("second transform failed");
        let df = store.get("dioxinemissionrate").unwrap();
        assert_eq!(
            df.height(),
            2,
            "row count must not change on second transform"
        );
    }

    #[test]
    fn transform_empty_rate_table_produces_empty_output() {
        use polars::prelude::{DataFrame, NamedFrom, Series};
        let mut store = InMemoryStore::new();
        // PPMY with two model years for one group.
        store.insert(
            "PollutantProcessMappedModelYear",
            DataFrame::new(
                2,
                vec![
                    Series::new("polProcessID".into(), vec![13001i64, 13001i64]).into(),
                    Series::new("modelYearID".into(), vec![2010i64, 2015i64]).into(),
                    Series::new("modelYearGroupID".into(), vec![30140010i64, 30140010i64]).into(),
                    Series::new("fuelMYGroupID".into(), vec![0i64, 0i64]).into(),
                    Series::new("IMModelYearGroupID".into(), vec![0i64, 0i64]).into(),
                ],
            )
            .unwrap(),
        );
        // dioxinEmissionRate with 0 rows.
        store.insert(
            "dioxinEmissionRate",
            DataFrame::new(
                0,
                vec![
                    Series::new("polProcessID".into(), Vec::<i64>::new()).into(),
                    Series::new("fuelTypeID".into(), Vec::<i64>::new()).into(),
                    Series::new("modelYearGroupID".into(), Vec::<i64>::new()).into(),
                    Series::new("meanBaseRate".into(), Vec::<f64>::new()).into(),
                ],
            )
            .unwrap(),
        );
        transform_airtoxics_rate_tables(&mut store).expect("transform on empty table failed");
        let df = store.get("dioxinemissionrate").unwrap();
        assert_eq!(df.height(), 0, "empty input yields empty output");
    }

    #[test]
    fn transform_empty_rate_table_string_dtype_mean_base_rate() {
        // Regression for mo-81nw: Polars infers meanBaseRate as String dtype
        // when the snapshot Parquet was captured from an empty MariaDB table.
        // expand_rate_table_rows must short-circuit and not attempt a
        // String → Float64 cast that would crash on the empty series.
        use polars::prelude::{DataFrame, DataType, NamedFrom, Series};
        let mut store = InMemoryStore::new();
        store.insert(
            "PollutantProcessMappedModelYear",
            DataFrame::new(
                1,
                vec![
                    Series::new("polProcessID".into(), vec![13001i64]).into(),
                    Series::new("modelYearID".into(), vec![2010i64]).into(),
                    Series::new("modelYearGroupID".into(), vec![30140010i64]).into(),
                    Series::new("fuelMYGroupID".into(), vec![0i64]).into(),
                    Series::new("IMModelYearGroupID".into(), vec![0i64]).into(),
                ],
            )
            .unwrap(),
        );
        // meanBaseRate has String dtype — as inferred from an empty Parquet table.
        store.insert(
            "dioxinEmissionRate",
            DataFrame::new(
                0,
                vec![
                    Series::new("polProcessID".into(), Vec::<i64>::new()).into(),
                    Series::new("fuelTypeID".into(), Vec::<i64>::new()).into(),
                    Series::new("modelYearGroupID".into(), Vec::<i64>::new()).into(),
                    Series::new("meanBaseRate".into(), Vec::<String>::new()).into(),
                ],
            )
            .unwrap(),
        );
        transform_airtoxics_rate_tables(&mut store)
            .expect("empty String-dtype meanBaseRate must not crash");
        let df = store.get("dioxinemissionrate").unwrap();
        assert_eq!(df.height(), 0, "empty input yields empty output");
        assert_eq!(
            *df.column("meanBaseRate").unwrap().dtype(),
            DataType::Float64,
            "output meanBaseRate must be Float64"
        );
    }

    #[test]
    fn new_tvv_year_generator_is_registered() {
        let registry = load_registry(None, true).expect("registry should load with calculators");
        assert!(
            registry.has_factory("NewTvvYearGenerator"),
            "NewTvvYearGenerator must have a factory registered"
        );
    }

    // ---- SnapshotFilter and table_filter_expr tests ----

    fn county_run_spec(county_id: u32, year: u32, month: u32) -> moves_runspec::RunSpec {
        use moves_runspec::{GeoKind, GeographicSelection, Timespan};
        moves_runspec::RunSpec {
            geographic_selections: vec![GeographicSelection {
                kind: GeoKind::County,
                key: county_id,
                description: String::new(),
            }],
            timespan: Timespan {
                years: vec![year],
                months: vec![month],
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn snapshot_filter_derives_zone_from_county() {
        let rs = county_run_spec(26161, 2020, 7);
        let f = SnapshotFilter::from_run_spec(&rs);
        assert_eq!(f.county_ids, vec![26161i64]);
        assert_eq!(f.zone_ids, vec![261610i64], "zone = county * 10");
        assert_eq!(f.year_ids, vec![2020i64]);
        assert_eq!(
            f.month_ids,
            vec![8i64],
            "month key=7 → monthID=8 (MOVES XML months are 0-indexed)"
        );
    }

    #[test]
    fn snapshot_filter_empty_for_nation_scale() {
        let f = SnapshotFilter::from_run_spec(&moves_runspec::RunSpec::default());
        assert!(f.county_ids.is_empty());
        assert!(f.zone_ids.is_empty());
        assert!(f.year_ids.is_empty());
        assert!(f.month_ids.is_empty());
    }

    #[test]
    fn table_filter_expr_zonemonthhour_returns_expr() {
        let rs = county_run_spec(26161, 2020, 7);
        let f = SnapshotFilter::from_run_spec(&rs);
        assert!(
            table_filter_expr("zonemonthhour", &f).is_some(),
            "should produce a filter when zone_ids and month_ids are non-empty"
        );
    }

    #[test]
    fn table_filter_expr_countyyear_returns_expr() {
        let rs = county_run_spec(26161, 2020, 7);
        let f = SnapshotFilter::from_run_spec(&rs);
        assert!(
            table_filter_expr("countyyear", &f).is_some(),
            "should produce a filter when county_ids and year_ids are non-empty"
        );
    }

    #[test]
    fn table_filter_expr_none_for_unrecognised_table() {
        let rs = county_run_spec(26161, 2020, 7);
        let f = SnapshotFilter::from_run_spec(&rs);
        assert!(table_filter_expr("emissionrate", &f).is_none());
        assert!(table_filter_expr("activitytype", &f).is_none());
    }

    #[test]
    fn table_filter_expr_none_when_ids_empty() {
        let f = SnapshotFilter::from_run_spec(&moves_runspec::RunSpec::default());
        assert!(
            table_filter_expr("zonemonthhour", &f).is_none(),
            "no expr when all filter sets are empty (nation-scale run)"
        );
        assert!(table_filter_expr("countyyear", &f).is_none());
    }

    #[test]
    fn parquet_fallback_applies_filter_to_zonemonthhour() {
        use moves_snapshot::format::ColumnKind;
        use moves_snapshot::table::{TableBuilder, Value};
        use moves_snapshot::Snapshot;

        // Build a snapshot with a small ZoneMonthHour table: 3 zones × 2 months × 1 hour.
        let mut tb = TableBuilder::new(
            "db__movesexecution1__zonemonthhour",
            [
                ("monthID".to_string(), ColumnKind::Int64),
                ("zoneID".to_string(), ColumnKind::Int64),
                ("hourID".to_string(), ColumnKind::Int64),
                ("temperature".to_string(), ColumnKind::Float64),
            ],
        )
        .unwrap()
        .with_natural_key(["monthID", "zoneID", "hourID"])
        .unwrap();
        // zone 100 (county 10), month 7
        tb.push_row([
            Value::Int64(7),
            Value::Int64(100),
            Value::Int64(8),
            Value::Float64(75.0),
        ])
        .unwrap();
        // zone 100, month 8
        tb.push_row([
            Value::Int64(8),
            Value::Int64(100),
            Value::Int64(8),
            Value::Float64(80.0),
        ])
        .unwrap();
        // zone 200 (different county), month 7
        tb.push_row([
            Value::Int64(7),
            Value::Int64(200),
            Value::Int64(8),
            Value::Float64(70.0),
        ])
        .unwrap();
        let table = tb.build().unwrap();
        let mut snap = Snapshot::new();
        snap.add_table(table).unwrap();
        let dir = tempfile::tempdir().unwrap();
        snap.write(dir.path()).unwrap();
        // Remove bundle to force Parquet fallback.
        let bundle = dir.path().join("tables").join("execution-db.bundle");
        if bundle.exists() {
            std::fs::remove_file(&bundle).unwrap();
        }

        // Filter: county 10 → zone 100, month 7.
        use moves_runspec::{GeoKind, GeographicSelection, Timespan};
        let rs = moves_runspec::RunSpec {
            geographic_selections: vec![GeographicSelection {
                kind: GeoKind::County,
                key: 10,
                description: String::new(),
            }],
            timespan: Timespan {
                months: vec![7],
                ..Default::default()
            },
            ..Default::default()
        };
        let filter = SnapshotFilter::from_run_spec(&rs);
        let store = load_execution_db_from_parquet(dir.path(), &filter, None)
            .expect("filtered load must succeed");
        let df = store.get("zonemonthhour").unwrap();
        // Only the row for zone=100, month=7 should survive.
        assert_eq!(df.height(), 1, "filter should keep only 1 matching row");
        let zone_col = df
            .column("zoneID")
            .unwrap()
            .cast(&polars::prelude::DataType::Int64)
            .unwrap();
        assert_eq!(
            zone_col.i64().unwrap().get(0).unwrap(),
            100,
            "surviving row must be zone 100"
        );
    }
}

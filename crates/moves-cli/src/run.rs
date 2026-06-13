//! `moves run` — load a RunSpec, walk the calculator graph, write output.
//!
//! Thin wrapper over [`moves_framework::MOVESEngine`] (
//!): it parses the RunSpec, builds the [`CalculatorRegistry`] from
//! the calculator-chain DAG, hands both to the engine, and returns
//! the engine's [`EngineOutcome`].
//!
//! # The calculator DAG
//!
//! The engine needs the calculator-graph DAG that
//! reconstructed. The committed artifact lives at
//! `characterization/calculator-chains/calculator-dag.json`; `moves run`
//! embeds it at compile time so the binary is self-contained — running
//! `moves run` walks the real 63-module / 960-pair MOVES calculator graph
//! out of the box. A caller can still point at a different DAG with
//! `--calculator-dag` (e.g. to test against a regenerated graph).

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use moves_calculator_info::CalculatorDag;
use moves_calculators::generators::totalactivitygenerator::inputs::{
    HourDayRow, LinkRow as ShoLinkRow,
};
use moves_calculators::generators::totalactivitygenerator::model::AverageSpeedRow;
use moves_data_default::DefaultDb;
use moves_framework::{
    default_tables, read_execution_bundle, read_execution_bundle_filtered, CalculatorRegistry,
    CountyRow, DataFrameStore, DataFrameStoreTyped, EngineConfig, EngineOutcome, GeographyTables,
    InMemoryStore, InputDataManager, LinkRow, MOVESEngine, MergeTableSpec, RunSpecFilters,
};
use moves_runspec::{GeoKind, RunSpec};
use polars::prelude::{
    col, lit, Expr, LazyFrame, NamedFrom, PlRefPath, ScanArgsParquet, SerReader, Series,
};

use crate::load_run_spec;

/// The calculator-chain DAG, embedded at compile time.
///
/// `moves run` uses this whenever `--calculator-dag` is not supplied. The
/// source artifact is the byte-stable JSON written by the
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
            // `timespan.months` already holds the real MOVES `monthID` (the
            // `<month key="N"/>` 0-based index → ID conversion happens once, in
            // the XML parser; see `XmlIndexedId`). Use it verbatim — adding 1
            // here would double-shift (e.g. August → September).
            month_ids: run_spec
                .timespan
                .months
                .iter()
                .map(|&m| i64::from(m))
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
    /// DAG embedded in the binary at compile time.
    pub calculator_dag: Option<PathBuf>,
    /// Optional value for the `MOVESRun.runDateTime` output column. `None`
    /// leaves it null, which keeps the run's output byte-stable — the
    /// engine deliberately does not stamp the wall clock itself.
    pub run_date_time: Option<String>,
    /// Path to a canonical MOVES snapshot directory (as written by the
    /// capture harness). When present, the `db__movesexecution*` Parquet
    /// files under `<snapshot>/tables/` are loaded as the execution-database
    /// slow tier and made available to calculators via `ctx.tables()`.
    pub snapshot: Option<PathBuf>,
    /// Path to a County/Project-scale input directory produced by
    /// `moves import-cdb` (CDB) or the PDB importer. When present, every
    /// `*.parquet` file in the directory is loaded and inserted into the
    /// execution-database slow tier, **overriding** any same-named table
    /// that the snapshot (or default-DB) already loaded. This implements
    /// the MOVES County/Project-scale data-preference rule: user-supplied
    /// CDB/PDB tables take precedence over the default database for the
    /// tables they cover, while all other tables continue to come from the
    /// snapshot/default-DB.
    ///
    /// Applies when the RunSpec sets `<modeldomain>` to `SINGLE` (County)
    /// or `PROJECT`.
    pub scale_input: Option<PathBuf>,
    /// Path to a converted default-DB Parquet tree (as written by
    /// `moves-default-db-convert`). When present, the default database is
    /// loaded and filtered to the RunSpec's geography/time/pollutant/process
    /// dimensions, providing the execution-database slow tier without a
    /// captured snapshot. The `Link` table and all `RunSpec*` tables are
    /// synthesised from the RunSpec and the loaded default-DB tables.
    pub default_db: Option<PathBuf>,
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
    let has_slow_store =
        opts.snapshot.is_some() || opts.scale_input.is_some() || opts.default_db.is_some();
    let registry = load_registry(opts.calculator_dag.as_deref(), has_slow_store)?;
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
        let mut tables = registry.required_input_tables();
        // `populate_sho_distances` recomputes the NULL `SHO.distance` column
        // from these reference tables. They are not declared inputs of any
        // calculator (MOVES populates distance inside the activity generator,
        // which does not re-run against a snapshot), so they must be admitted
        // explicitly or the load filter drops them and distance stays NULL.
        tables.extend(SHO_DISTANCE_INPUT_TABLES.iter().map(|t| (*t).to_owned()));
        // `populate_source_use_type_physics_mapping` derives the missing
        // `sourceUseTypePhysicsMapping` table from `sourceUseTypePhysics` when a
        // snapshot omits the runtime-built mapping; admit both so neither is
        // filtered out before the synthesis runs.
        tables.extend(
            SOURCE_TYPE_PHYSICS_MAPPING_INPUT_TABLES
                .iter()
                .map(|t| (*t).to_owned()),
        );
        // The BaseRateGenerator's SB-weighting reads the fuel-usage-remapped
        // source-bin distribution under a dynamic per-(process,county,year) name
        // (`sourceBinDistributionFuelUsage_1_26161_2020`). It declares no static
        // INPUT_TABLES entry, so admit its base name explicitly; the loader
        // strips the numeric suffix to match (see `strip_numeric_index_suffix`).
        // Without this the SB-weighting falls back to the raw SourceBinDistribution
        // and over-weights flex-fuel (E85) energy ~50×.
        tables.insert("sourcebindistributionfuelusage".to_owned());
        // The same SB-weighting applies the canonical EV-sales ICE back-scaling
        // (step 010) from these three tables; they are not declared static
        // calculator inputs either, so admit them or recent-model-year ICE
        // energy rates come out ~2-3% low.
        tables.insert("evsalesfraction".to_owned());
        tables.insert("fleetavgadjustment".to_owned());
        tables.insert("regulatoryclass".to_owned());
        Some(tables)
    } else {
        None
    };
    let mut engine = MOVESEngine::new(run_spec.clone(), registry, config);
    // Attach the internal control strategies this RunSpec enables. Canonical
    // MOVES subscribes each strategy only when its RunSpec predicate is set;
    // `register_strategies` mirrors that gating, so a strategy the run does not
    // use never runs (and an unported-but-requested strategy fails loudly in
    // `pre_run` instead of silently dropping its control effect).
    if has_slow_store {
        let mut strategies = moves_framework::ControlStrategyRegistry::new();
        moves_calculators::register_strategies(&mut strategies, &run_spec);
        engine = engine.with_strategy_registry(strategies);
    }
    if let Some(snapshot_dir) = &opts.snapshot {
        let filter = snap_filter
            .as_ref()
            .expect("built above when snapshot is Some");
        let mut store = load_execution_db(snapshot_dir, filter, allowed_tables.as_ref())
            .with_context(|| format!("loading execution DB from {}", snapshot_dir.display()))?;
        // Overlay any County/Project-scale Parquet tables on top of the snapshot
        // tables. CDB/PDB tables take precedence for the tables they supply.
        if let Some(scale_dir) = &opts.scale_input {
            overlay_scale_input_db(&mut store, scale_dir)
                .with_context(|| format!("loading scale-input DB from {}", scale_dir.display()))?;
        }
        let geography = load_geography_from_store(&store)
            .with_context(|| format!("building geography from {}", snapshot_dir.display()))?;
        // Canonical MOVES runs `DO_RATES_FIRST` (the released default): only the
        // BaseRate + chained-calculator whitelist produces emissions, and the
        // legacy inventory calculators (CriteriaStart/CriteriaRunning/BasicPm/…)
        // are cleared. A captured snapshot is, by construction, the execution DB
        // of such a rates-first canonical run (every snapshot carries a
        // `baseRateOutput`), so the snapshot replay must mirror the same plan.
        // Without this, the port runs BOTH pipelines and double-counts — e.g.
        // start exhaust (process 2) is emitted by the legacy CriteriaStart
        // calculator on off-network road type 1 even when the RunSpec selects
        // only an on-road type, where canonical's rates-first plan emits nothing.
        engine = engine.with_rates_first(true).with_slow_store(store);
        engine
            .execution_run_spec_mut()
            .build_execution_locations(&geography);
    } else if let Some(scale_dir) = &opts.scale_input {
        // Scale-only path: no snapshot or default DB — build a store from CDB/PDB
        // Parquet alone, then run the same synthesis steps as the default-DB path.
        // CDB provides county-scale tables (ZoneRoadType, SourceTypePopulation, …);
        // the synthesis steps fill in derived tables that are not in the CDB:
        //   - Link: synthesised from ZoneRoadType when CDB omits it
        //   - RunSpec*: derived from the RunSpec (not a DB table)
        //   - sourceUseTypePhysicsMapping: derived from sourceUseTypePhysics if present
        //   - ZoneMonthHour meteorology columns: filled from temperature/relHumidity
        // Each synthesis function is a no-op when its input table is absent, so it
        // is safe to call even for minimal CDB dirs that omit optional tables.
        let mut store = InMemoryStore::new();
        overlay_scale_input_db(&mut store, scale_dir)
            .with_context(|| format!("loading scale-input DB from {}", scale_dir.display()))?;
        populate_link_from_zone_road_type(&mut store)
            .context("synthesising Link from ZoneRoadType")?;
        build_runspec_tables(&run_spec, &mut store)
            .context("building RunSpec tables from RunSpec")?;
        populate_source_use_type_physics_mapping(&mut store)
            .context("synthesising sourceUseTypePhysicsMapping")?;
        populate_zone_month_hour_meteorology(&mut store)
            .context("populating ZoneMonthHour meteorology")?;
        let geography = load_geography_from_store(&store)
            .with_context(|| format!("building geography from {}", scale_dir.display()))?;
        engine = engine.with_slow_store(store);
        engine
            .execution_run_spec_mut()
            .build_execution_locations(&geography);
    } else if let Some(default_db_path) = &opts.default_db {
        // Default-DB path: load the Parquet default database, synthesise the Link
        // table from ZoneRoadType, build RunSpec* tables from the RunSpec, and
        // optionally overlay CDB/PDB tables on top before wiring into the engine.
        let store = build_default_db_store(default_db_path, &run_spec, opts.scale_input.as_deref())
            .with_context(|| {
                format!(
                    "building default-DB store from {}",
                    default_db_path.display()
                )
            })?;
        let geography = load_geography_from_store(&store).with_context(|| {
            format!(
                "building geography from default DB at {}",
                default_db_path.display()
            )
        })?;
        // Execution day types: canonical iterates every DayOfAnyWeek day (not the
        // runspec `<day>` selection). The load filter above loaded the day-keyed
        // tables for all day types; read them back here (store is moved into the
        // engine below) so the execution day set can be expanded to match.
        let execution_day_ids: Vec<u32> = day_ids_from_store(&store);
        // The default DB carries both the rates and inventory execution tables,
        // so follow canonical MOVESInstantiator DO_RATES_FIRST (the released
        // default) and run only the BaseRate + chained pipeline — otherwise the
        // legacy inventory calculators double-count and fan a single run-month
        // out across all 12 months.
        engine = engine.with_rates_first(true).with_slow_store(store);
        engine
            .execution_run_spec_mut()
            .build_execution_locations(&geography);
        engine
            .execution_run_spec_mut()
            .set_execution_days(execution_day_ids);
    }
    let outcome = engine.run().context("MOVES engine run failed")?;
    Ok(outcome)
}

/// Build the [`CalculatorRegistry`] — from `path` if given, otherwise from
/// the embedded DAG.
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
    // Synthesise sourceUseTypePhysicsMapping from sourceUseTypePhysics when the
    // snapshot omits the runtime-built mapping (MOVES builds it inside
    // SourceUseTypePhysics.setup, which does not re-run against a snapshot).
    populate_source_use_type_physics_mapping(&mut store)
        .context("synthesising sourceUseTypePhysicsMapping from snapshot")?;
    // Fill the NULL ZoneMonthHour meteorology columns (heatIndex,
    // specificHumidity, molWaterFraction) that MeteorologyGenerator computes at
    // runtime but some snapshots capture empty.
    populate_zone_month_hour_meteorology(&mut store)
        .context("populating ZoneMonthHour meteorology from snapshot")?;
    // Union process/year-indexed table variants (e.g. baserate_1_2001, baserate_2_2001)
    // into their canonical names (e.g. baserate) so calculators can read real data.
    merge_process_year_variants(&mut store)
        .context("merging process/year-indexed table variants into canonical names")?;
    Ok(store)
}

/// Overlay County/Project-scale (CDB/PDB) Parquet files from `scale_dir` into
/// an existing [`InMemoryStore`].
///
/// Scans `scale_dir` for every `*.parquet` file, reads each, and inserts it
/// into `store` using the stem (filename without `.parquet`) as the table name,
/// **replacing** any same-named table that was already in the store. This
/// implements the MOVES County/Project-scale data preference rule: user-supplied
/// CDB/PDB tables override the default-database tables for the tables they cover;
/// tables not present in the CDB/PDB directory continue to come from the
/// snapshot or default DB.
///
/// The table name inserted is the **stem as-is** (preserving the original case
/// the importer wrote, e.g. `"Link"` rather than `"link"`). The store uses
/// case-insensitive lookup, so calculators that ask for `"link"` or `"LINK"` will
/// find the value regardless.
fn overlay_scale_input_db(store: &mut InMemoryStore, scale_dir: &Path) -> Result<()> {
    let dir = fs::read_dir(scale_dir)
        .with_context(|| format!("reading scale-input directory {}", scale_dir.display()))?;
    for entry in dir {
        let entry = entry.context("reading scale-input directory entry")?;
        let file_name = entry.file_name();
        let name_str = file_name.to_string_lossy();
        if !name_str.ends_with(".parquet") {
            continue;
        }
        let table_name = name_str.trim_end_matches(".parquet").to_owned();
        let path = entry.path();
        let file = fs::File::open(&path).with_context(|| format!("opening {}", path.display()))?;
        let df = polars::prelude::ParquetReader::new(BufReader::new(file))
            .finish()
            .map_err(|e| anyhow::anyhow!("parsing {}: {e}", path.display()))?;
        store.insert(table_name, df);
    }
    Ok(())
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

/// Strip all trailing `_<digits>` segments from a lowercased table name, returning
/// the base name that MOVES uses for the canonical (unpartitioned) form.
///
/// MOVES partitions some execution-DB tables by process, county, and/or year into
/// separate MySQL tables (e.g. `baserate_1_2001`, `baseratebyage_90_2020`). The
/// calculator reads the canonical name (`baserate`, `baseratebyage`). This helper
/// strips those numeric-index suffixes so the allow-filter admits the variants and
/// the merge step can union them back under the canonical name.
///
/// Examples: `"baserate_1_2001"` → `"baserate"`, `"baserate"` → `"baserate"`,
/// `"stmytvvcoeffs2020"` → `"stmytvvcoeffs2020"` (no underscore separator, unchanged).
fn strip_numeric_index_suffix(name: &str) -> &str {
    let mut end = name.len();
    while let Some(pos) = name[..end].rfind('_') {
        let suffix = &name[pos + 1..end];
        if !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()) {
            end = pos;
        } else {
            break;
        }
    }
    &name[..end]
}

/// Union all process/year-indexed table variants into their canonical names.
///
/// MOVES splits some execution-DB tables (e.g. `BaseRate`, `BaseRateByAge`) into
/// separate per-process-per-year MySQL tables (`baserate_1_2001`, `baserate_2_2001`).
/// The snapshot captures those variants alongside an empty canonical stub (`baserate`).
/// Calculators read only the canonical name, so this step unions all variants into
/// that name, replacing the empty stub with the full merged table.
fn merge_process_year_variants(store: &mut InMemoryStore) -> Result<()> {
    // Delegates to the single shared implementation in
    // `moves_calculators::default_db_setup` (see that module — both the native
    // CLI and the wasm path call it, so the synthesis can no longer drift).
    moves_calculators::default_db_setup::merge_store_variants_eager(store)
        .map_err(|e| anyhow::anyhow!(e))
}

/// Fall-back loader: scan `<snapshot>/tables/` for individual `db__movesexecution*.parquet`
/// files (snapshots captured before the bundle format was introduced).
///
/// For tables listed in [`table_filter_expr`] (currently `ZoneMonthHour` and
/// `CountyYear`) a Polars `LazyFrame` predicate is pushed into the Parquet decoder
/// so only matching row groups are decoded. All other tables are read whole.
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
        // Process/year-indexed tables (e.g. `baserate_1_2001`) are admitted when
        // their canonical base name (e.g. `baserate`) appears in the allowed set;
        // they are merged into that canonical name by `merge_process_year_variants`
        // after loading.
        if let Some(allowed) = allowed_tables {
            let lower = table_name.to_ascii_lowercase();
            if !allowed.contains(&lower)
                && !allowed.contains(strip_year_suffix(&lower))
                && !allowed.contains(strip_numeric_index_suffix(&lower))
            {
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

/// Reference tables [`populate_sho_distances`] needs to recompute the NULL
/// `SHO.distance` column. Snapshot loading filters tables down to the registered
/// calculators' declared inputs; these are not among them, so they are added to
/// the allowed set explicitly. Lower-cased short names, matching the load filter.
const SHO_DISTANCE_INPUT_TABLES: &[&str] = &["sho", "link", "averagespeed", "hourday"];

/// Tables [`populate_source_use_type_physics_mapping`] needs to synthesise the
/// `sourceUseTypePhysicsMapping` table when a snapshot captured the source
/// `sourceUseTypePhysics` table but not the runtime-derived mapping. Neither is
/// declared as an input by any calculator (calculators read the *mapping*,
/// which MOVES builds in `SourceUseTypePhysics.setup()`), so they must be
/// admitted explicitly or the load filter drops them.
const SOURCE_TYPE_PHYSICS_MAPPING_INPUT_TABLES: &[&str] =
    &["sourceusetypephysics", "sourceusetypephysicsmapping"];

/// Compute `SHO.distance = SHO * averageSpeed` from snapshot tables and write
/// back, so downstream calculators find non-null distances in the slow store.
///
/// The implementation avoids materialising `Vec<ShoRow>` for the full SHO
/// table (which can exceed 1 M rows). Instead it:
/// 1. Reads only the four columns needed for distance arithmetic into owned
/// `Vec<i32>` / `Vec<f64>` buffers, then releases the `Arc<DataFrame>`.
/// 2. Uses [`DataFrameStoreTyped::iter_typed`] for the small lookup tables
/// (Link, AverageSpeed, HourDay), which are typically <1000 rows.
/// 3. Computes a `Vec<f64>` distance column and writes it back in-place via
/// [`InMemoryStore::get_mut`], which avoids cloning the Arrow buffers
/// because the outer `Arc<DataFrame>` refcount is 1 at that point.
fn populate_sho_distances(store: &mut InMemoryStore) -> Result<()> {
    use polars::prelude::{DataType, NamedFrom, Series};

    if !store.contains("SHO") {
        return Ok(());
    }

    // ---: read SHO column data ---
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

    // ---: build lookup tables from small reference tables ---
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

    // ---: compute distance column (same formula as calculate_distance) ---
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

    // ---: update distance column in-place ---
    // Arc<DataFrame> refcount is 1 (sho_arc was dropped); no DataFrame clone occurs.
    let sho_mut = store.get_mut("SHO").expect("SHO was present above");
    sho_mut
        .with_column(Series::new("distance".into(), distances).into())
        .map_err(|e| anyhow::anyhow!("replacing SHO.distance: {e}"))?;
    Ok(())
}

/// Synthesise `sourceUseTypePhysicsMapping` from `sourceUseTypePhysics` when a
/// snapshot omits it.
///
/// MOVES builds `sourceUseTypePhysicsMapping` at runtime in
/// `SourceUseTypePhysics.setup()`; some execution-DB snapshots capture the
/// source `sourceUseTypePhysics` table but not the derived mapping. When the
/// mapping is absent (and the source table present), synthesise the **identity
/// mapping** — the MOVES default when no alternate vehicle physics is
/// configured: `realSourceTypeID = tempSourceTypeID = sourceTypeID` and
/// `opModeIDOffset = 0`, carrying the road-load terms (`regClassID`,
/// `beginModelYearID`, `endModelYearID`, `rollingTermA`, `rotatingTermB`,
/// `dragTermC`, `sourceMass`, `fixedMassFactor`) through unchanged.
///
/// The operating-mode-distribution correctors key on
/// `realSourceTypeID <> tempSourceTypeID`, so the identity rows are inert and
/// leave emission results unchanged — they only let calculators that read the
/// mapping table run instead of erroring on a missing table.
fn populate_source_use_type_physics_mapping(store: &mut InMemoryStore) -> Result<()> {
    // Delegates to the single shared implementation in
    // `moves_calculators::default_db_setup` (see that module — both the native
    // CLI and the wasm path call it, so the synthesis can no longer drift).
    moves_calculators::default_db_setup::populate_source_use_type_physics_mapping(store)
        .map_err(|e| anyhow::anyhow!(e))
}

/// Fill the NULL `ZoneMonthHour` meteorology columns (`heatIndex`,
/// `specificHumidity`, `molWaterFraction`) from `temperature` / `relHumidity`.
///
/// MOVES' `MeteorologyGenerator` (`doHeatIndex`) computes these three columns at
/// runtime and writes them back into `ZoneMonthHour`; some execution-DB
/// snapshots capture the raw table with `temperature` and `relHumidity` present
/// but the three derived columns left NULL. Downstream calculators read
/// `ZoneMonthHour` directly and abort on the NULLs, so reproduce the generator's
/// computation here via [`build_meteorology_table`] (joining `Zone` → `County`
/// for each county's barometric pressure / altitude) and write the results back
/// into the slow store. Rows whose zone/county is missing — which MOVES drops in
/// its inner-join — keep `heatIndex = temperature` (the `<78 °F` passthrough)
/// with zero humidity terms so every row stays readable.
///
/// No-op when `ZoneMonthHour` is absent, when `heatIndex` already holds a
/// non-NULL value (snapshot captured the computed table), or when `Zone` /
/// `County` are unavailable.
fn populate_zone_month_hour_meteorology(store: &mut InMemoryStore) -> Result<()> {
    // Delegates to the single shared implementation in
    // `moves_calculators::default_db_setup` (see that module — both the native
    // CLI and the wasm path call it, so the synthesis can no longer drift).
    moves_calculators::default_db_setup::populate_zone_month_hour_meteorology(store)
        .map_err(|e| anyhow::anyhow!(e))
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

/// Synthesise the `Link` table from `ZoneRoadType` + `RoadType` when the
/// default DB provides no Link rows.
///
/// The default DB ships `Link` as schema-only (zero rows). For county-scale
/// runs the engine needs at least one `LinkRow` per (zone, road-type) to
/// iterate over execution locations. This function reads the `ZoneRoadType`
/// table (which does have rows — one per zone × road-type combination) and
/// constructs synthetic `Link` rows using the MOVES county-scale convention:
///
/// * `countyID  = zoneID / 10`  (default zone = county × 10)
/// * `linkID    = zoneID * 10 + roadTypeID`
///
/// The result is inserted into the store under the canonical name `"Link"`.
/// The function is a no-op when `ZoneRoadType` is absent from the store, or
/// when a non-empty `Link` table is already present (user-supplied CDB/PDB
/// data takes precedence).
fn populate_link_from_zone_road_type(store: &mut InMemoryStore) -> Result<()> {
    // Delegates to the single shared implementation in
    // `moves_calculators::default_db_setup` (see that module — both the native
    // CLI and the wasm path call it, so the synthesis can no longer drift).
    moves_calculators::default_db_setup::populate_link_from_zone_road_type(store)
        .map_err(|e| anyhow::anyhow!(e))
}

/// Build the `RunSpec*` tables that generators read from the execution-DB
/// slow tier, synthesising them from the parsed [`RunSpec`] and the already-
/// loaded default-DB tables in `store`.
///
/// The default DB does not contain RunSpec-specific selection tables — they
/// are normally written by Java MOVES into `MOVESExecution` just before the
/// generators run. This function replicates that step in pure Rust, producing:
///
/// | Table | Column | Source |
/// |-------|--------|--------|
/// | `RunSpecSourceType` | `sourceTypeID` (i32) | `onroad_vehicle_selections` |
/// | `RunSpecPollutantProcess` | `polProcessID` (i32) | `pollutant_process_associations` |
/// | `RunSpecDay` | `dayID` (i32) | `timespan.days` |
/// | `RunSpecHour` | `hourID` (i32) | `timespan.begin_hour..=end_hour` |
/// | `RunSpecHourDay` | `hourDayID` (i32) | hours × days cross-product |
/// | `RunSpecMonth` | `monthID` (i32) | `timespan.months` |
/// | `RunSpecYear` | `yearID` (i32) | `timespan.years` |
/// | `RunSpecRoadType` | `roadTypeID` (i32) | `road_types` |
/// | `RunSpecMonthGroup` | `monthGroupID` (i32) | `MonthGroupOfAnyYear` join |
/// | `RunSpecSourceFuelType` | `sourceTypeID`, `fuelTypeID` (i64) | onroad selections |
///
/// `RunSpecHourDay` uses the MOVES packed-key formula `hourDayID = hourID × 10 + dayID`.
///
/// `RunSpecMonthGroup` is derived by joining the RunSpec months against the
/// `MonthGroupOfAnyYear` table in the store; if that table is absent the
/// month ID is used as the month group ID directly (safe fallback — in the
/// default MOVES configuration each month belongs to its own group).
fn build_runspec_tables(runspec: &RunSpec, store: &mut InMemoryStore) -> Result<()> {
    // Delegates to the single shared implementation in
    // `moves_calculators::default_db_setup` (see that module — both the native
    // CLI and the wasm path call it, so the synthesis can no longer drift).
    moves_calculators::default_db_setup::build_runspec_tables(runspec, store)
        .map_err(|e| anyhow::anyhow!(e))
}

/// Derive `fuelYearID` values from the already-loaded `Year` table.
///
/// Ports the first half of `ExecutionRunSpec.initializeAfterShallowTables`
/// (lines 223–241): `SELECT DISTINCT fuelYearID FROM year WHERE yearID IN
/// (<runspec years>)`. The `Year` table is unfiltered in the default-DB
/// registry, so all year→fuelYear mappings are available here.
#[cfg(not(target_arch = "wasm32"))]
fn derive_fuel_years_from_store(store: &InMemoryStore, year_ids: &[i64]) -> Vec<i64> {
    let Some(arc) = store.get("Year") else {
        return Vec::new();
    };
    let df = &*arc;
    let find = |want: &str| -> Option<polars::prelude::Column> {
        let target = want.to_ascii_lowercase();
        df.columns()
            .iter()
            .find(|c| c.name().to_ascii_lowercase() == target)
            .cloned()
    };
    let (Some(yid_col), Some(fyid_col)) = (find("yearID"), find("fuelYearID")) else {
        return Vec::new();
    };
    let Ok(yids) = yid_col
        .cast(&polars::prelude::DataType::Int64)
        .and_then(|c| c.i64().cloned())
    else {
        return Vec::new();
    };
    let Ok(fyids) = fyid_col
        .cast(&polars::prelude::DataType::Int64)
        .and_then(|c| c.i64().cloned())
    else {
        return Vec::new();
    };
    let year_set: BTreeSet<i64> = year_ids.iter().copied().collect();
    let mut fuel_years: BTreeSet<i64> = BTreeSet::new();
    for i in 0..df.height() {
        if let (Some(y), Some(fy)) = (yids.get(i), fyids.get(i)) {
            if year_set.contains(&y) {
                fuel_years.insert(fy);
            }
        }
    }
    fuel_years.into_iter().collect()
}

/// Derive `regionID` values from the county-filtered `regionCounty` table.
///
/// Ports the second half of `ExecutionRunSpec.initializeAfterShallowTables`
/// (lines 243–251): `{0} ∪ SELECT DISTINCT regionID FROM regionCounty`.
/// Region 0 is the MOVES "all-regions" wildcard and is always included
/// (canonical Java line 245: `regions.add(Integer.valueOf(0))`).
#[cfg(not(target_arch = "wasm32"))]
fn derive_region_ids_from_store(store: &InMemoryStore) -> Vec<i64> {
    let mut regions: BTreeSet<i64> = BTreeSet::new();
    regions.insert(0); // wildcard — always present
    let Some(arc) = store.get("regionCounty") else {
        return regions.into_iter().collect();
    };
    let df = &*arc;
    let Some(rid_col) = df
        .columns()
        .iter()
        .find(|c| c.name().to_ascii_lowercase() == "regionid")
        .cloned()
    else {
        return regions.into_iter().collect();
    };
    if let Ok(casted) = rid_col.cast(&polars::prelude::DataType::Int64) {
        if let Ok(rids) = casted.i64() {
            for v in rids.into_iter().flatten() {
                regions.insert(v);
            }
        }
    }
    regions.into_iter().collect()
}

/// Build the in-memory execution store from a converted default-DB Parquet
/// tree, applying RunSpec-scoped filters via [`InputDataManager`] and then
/// synthesising the `Link`, `RunSpec*`, and meteorology tables.
///
/// This is the data-setup half of the `--default-db` run path, exposed as a
/// standalone function so integration tests can inspect or validate the
/// generated execution tables without driving the full calculator engine.
///
/// The returned [`InMemoryStore`] contains:
/// * All default-DB tables loaded by [`InputDataManager`] (filtered to the
///   RunSpec's geography, time, pollutant, and vehicle dimensions).
/// * An optional County/Project-scale overlay from `scale_dir`.
/// * A synthesised `Link` table derived from `ZoneRoadType`.
/// * Synthesised `RunSpec*` tables (year, month, day, hour, etc.) derived
///   from the RunSpec.
/// * Synthesised `sourceUseTypePhysicsMapping` (identity mapping from
///   `sourceUseTypePhysics`).
/// * Filled-in `ZoneMonthHour` meteorology columns (`heatIndex`,
///   `specificHumidity`, `molWaterFraction`).
/// * Process/year-indexed table variants merged into their canonical names.
///
/// # Errors
///
/// Fails if the default-DB cannot be opened, a Polars scan fails during
/// table loading, or a synthesis step returns an error.
#[cfg(not(target_arch = "wasm32"))]
pub fn build_default_db_store(
    default_db_path: &Path,
    run_spec: &RunSpec,
    scale_dir: Option<&Path>,
) -> Result<InMemoryStore> {
    let db = DefaultDb::open(default_db_path)
        .with_context(|| format!("opening default DB at {}", default_db_path.display()))?;

    // Phase 1: load all default-DB tables with base filters.
    // `fuel_years` and `region_ids` are empty at this stage — they require
    // DB lookups (Year → fuelYearID mapping, regionCounty → regionID set)
    // that are only valid after Phase 1 completes. Year loads unfiltered
    // (no year_column in the registry); regionCounty loads county-filtered
    // but without the fuelYear filter (which isn't known yet).
    let mut base_filters = RunSpecFilters::from_runspec(run_spec);
    // Expand the fuelType load filter to the selected source types' full fleet
    // fuel mix. A runspec `onroadvehicleselection` names one (sourceType,
    // fuelType), but canonical runs the source type's whole fleet (its
    // `RunSpecSourceFuelType` is `FuelType ⋈ SourceUseType ⋈ FuelEngTechAssoc`
    // for the selected source types — see build_runspec_tables). Without
    // matching that here, the fuel-sensitive tables (FuelSupply, …) load only
    // the literal selection fuel, so the BaseRate/activity for the other fleet
    // fuels canonical emits have no fuel supply ("missing fuel supply for
    // fuel_type_id=2"). Union the fleet fuels into the load filter.
    expand_fuel_filter_to_fleet(&db, &mut base_filters)
        .context("expanding fuelType filter to the selected sources' fleet fuels")?;
    // Expand the dayID load filter to all DayOfAnyWeek day types. Canonical's
    // execution time span (buildExecutionTimeSpan useRunSpec=false) iterates
    // every DayOfAnyWeek day, not the runspec `<day>` selection, so the
    // day-keyed tables (DayOfAnyWeek, HourDay, HourVMTFraction, …) must load for
    // both weekend (2) and weekday (5). The execution-day set is expanded to
    // match after load (set_execution_days). Without this, selecting weekday
    // only would drop the weekend day type's activity — ~half the output rows.
    expand_day_filter_to_all_day_types(&db, &mut base_filters)
        .context("expanding dayID filter to all DayOfAnyWeek day types")?;
    let plan = InputDataManager::plan(&base_filters, &default_tables());
    let mut store = InputDataManager::execute(&plan, &db)
        .map_err(|e| anyhow::anyhow!("loading default DB: {e}"))?;

    // Phase 2: derive fuel_years and region_ids from the Phase 1 tables,
    // then re-execute only the fuel/region-sensitive tables with the correct
    // filters. Mirrors `ExecutionRunSpec.initializeAfterShallowTables`
    // (Java lines 217–251).
    let fuel_years = derive_fuel_years_from_store(&store, &base_filters.years);
    let region_ids = derive_region_ids_from_store(&store);
    let full_filters = RunSpecFilters {
        fuel_years,
        region_ids,
        ..base_filters
    };
    // Select only the tables that carry a fuel_year or region column
    // annotation — those are the tables whose Phase 1 load lacked these
    // filters. Year and regionCounty are the source tables for the
    // derivation above and are intentionally left as loaded in Phase 1
    // (Year is registry-unfiltered; regionCounty is county-filtered only).
    let fuel_region_specs: Vec<MergeTableSpec> = default_tables()
        .into_iter()
        .filter(|t| t.fuel_year_column.is_some() || t.region_column.is_some())
        .filter(|t| !matches!(t.table_name, "Year" | "regionCounty"))
        .collect();
    let replan = InputDataManager::plan(&full_filters, &fuel_region_specs);
    let replenished = InputDataManager::execute(&replan, &db)
        .map_err(|e| anyhow::anyhow!("loading default DB (fuel/region re-pass): {e}"))?;
    replenished.copy_into(&mut store);

    if let Some(sd) = scale_dir {
        overlay_scale_input_db(&mut store, sd)
            .with_context(|| format!("loading scale-input DB from {}", sd.display()))?;
    }
    // Single shared post-load synthesis (merge variants, prune geography to the
    // run's counties, synthesise Link + RunSpec* tables, fill meteorology, scope
    // PollutantProcessModelYear, …) — the same routine the wasm path runs, so the
    // native default-DB path can no longer drift behind it. This also gives the
    // native path the geography pruning + pol-process scoping it previously
    // lacked (memory containment for multi-county runs).
    moves_calculators::default_db_setup::setup_execution_store(run_spec, &mut store)
        .map_err(|e| anyhow::anyhow!(e))
        .context("default-DB execution-store synthesis")?;
    Ok(store)
}

/// Expand `filters.fuel_type_ids` to include every fuel type the selected
/// source types use, read from the default DB's `FuelEngTechAssoc`.
///
/// Mirrors canonical's `RunSpecSourceFuelType` derivation (the selected source
/// type's whole fleet fuel mix, not the literal per-selection fuel). Only
/// *adds* fuels — the resulting filter is a superset, so a fixture that already
/// listed every fleet fuel is unchanged. A no-op (leaving the filter as-is) if
/// `FuelEngTechAssoc` is absent or carries neither key column.
#[cfg(not(target_arch = "wasm32"))]
fn expand_fuel_filter_to_fleet(db: &DefaultDb, filters: &mut RunSpecFilters) -> Result<()> {
    use moves_data_default::TableFilter;
    use polars::prelude::DataType;

    if filters.source_type_ids.is_empty() {
        return Ok(());
    }
    let Ok(lf) = db.scan("FuelEngTechAssoc", &TableFilter::new()) else {
        return Ok(());
    };
    let df = lf
        .collect()
        .context("scanning FuelEngTechAssoc for fleet fuel expansion")?;
    let (Ok(st), Ok(ft)) = (
        df.column("sourceTypeID")
            .and_then(|c| c.cast(&DataType::Int64)),
        df.column("fuelTypeID")
            .and_then(|c| c.cast(&DataType::Int64)),
    ) else {
        return Ok(());
    };
    let (Ok(st), Ok(ft)) = (st.i64(), ft.i64()) else {
        return Ok(());
    };
    let selected: BTreeSet<i64> = filters.source_type_ids.iter().copied().collect();
    let mut fuels: BTreeSet<i64> = filters.fuel_type_ids.iter().copied().collect();
    for i in 0..df.height() {
        if let (Some(s), Some(f)) = (st.get(i), ft.get(i)) {
            if selected.contains(&s) {
                fuels.insert(f);
            }
        }
    }
    filters.fuel_type_ids = fuels.into_iter().collect();
    Ok(())
}

/// Replace `filters.days` with all `DayOfAnyWeek` day types from the default DB.
///
/// Canonical's execution time span iterates every `DayOfAnyWeek` day (not the
/// runspec selection); the day-keyed input tables must therefore load for all
/// day types. No-op (filter unchanged) if `DayOfAnyWeek` is absent or yields no
/// day IDs.
#[cfg(not(target_arch = "wasm32"))]
fn expand_day_filter_to_all_day_types(db: &DefaultDb, filters: &mut RunSpecFilters) -> Result<()> {
    let days = day_ids_from_default_db(db)?;
    if !days.is_empty() {
        filters.days = days;
    }
    Ok(())
}

/// Read the distinct `dayID`s from the loaded `DayOfAnyWeek` table in the store
/// as `u32` (for the execution day set). Empty if absent / no `dayID` column.
fn day_ids_from_store(store: &InMemoryStore) -> Vec<u32> {
    use polars::prelude::DataType;
    let Some(arc) = store.get("DayOfAnyWeek") else {
        return Vec::new();
    };
    let Ok(col) = arc.column("dayID").and_then(|c| c.cast(&DataType::Int64)) else {
        return Vec::new();
    };
    let Ok(ca) = col.i64() else {
        return Vec::new();
    };
    let set: BTreeSet<i64> = ca.into_iter().flatten().collect();
    set.into_iter()
        .filter_map(|d| u32::try_from(d).ok())
        .collect()
}

/// Read the distinct `dayID`s from the default DB's `DayOfAnyWeek` table
/// (unfiltered). Empty if the table is absent or carries no `dayID` column.
#[cfg(not(target_arch = "wasm32"))]
fn day_ids_from_default_db(db: &DefaultDb) -> Result<Vec<i64>> {
    use moves_data_default::TableFilter;
    use polars::prelude::DataType;

    let Ok(lf) = db.scan("DayOfAnyWeek", &TableFilter::new()) else {
        return Ok(Vec::new());
    };
    let df = lf
        .collect()
        .context("scanning DayOfAnyWeek for day-type expansion")?;
    let Ok(col) = df.column("dayID").and_then(|c| c.cast(&DataType::Int64)) else {
        return Ok(Vec::new());
    };
    let Ok(ca) = col.i64() else {
        return Ok(Vec::new());
    };
    let set: BTreeSet<i64> = ca.into_iter().flatten().collect();
    Ok(set.into_iter().collect())
}

/// The pollutants the default (embedded-DAG) calculator set consumes and
/// replaces — the union of every registered calculator's
/// [`Calculator::replaced_pollutants`](moves_framework::Calculator::replaced_pollutants)
/// (e.g. SulfatePM's EC 112 / NonECPM 118).
///
/// Exposed for the canonical-snapshot regression gate, which asserts canonical
/// never emits a zero-valued row for one of these — the premise that makes the
/// engine's zero-row drop for replaced pollutants safe.
///
/// # Errors
///
/// Propagates a failure to parse the embedded DAG or register the default
/// calculator factories.
pub fn default_replaced_pollutants() -> Result<std::collections::BTreeSet<i32>> {
    Ok(load_registry(None, true)?.replaced_pollutants().clone())
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
    fn load_execution_db_parquet_admits_and_merges_indexed_tables() {
        use moves_snapshot::format::ColumnKind;
        use moves_snapshot::table::{TableBuilder, Value};
        use moves_snapshot::Snapshot;

        // Build a snapshot with two process/year-indexed baserate variants.
        let make_table = |name: &str, id_val: i64| {
            let mut tb = TableBuilder::new(
                name,
                [
                    ("sourcetypeid".to_string(), ColumnKind::Int64),
                    ("processid".to_string(), ColumnKind::Int64),
                ],
            )
            .unwrap()
            .with_natural_key(["sourcetypeid", "processid"])
            .unwrap();
            tb.push_row([Value::Int64(21), Value::Int64(id_val)])
                .unwrap();
            tb.build().unwrap()
        };

        let mut snap = Snapshot::new();
        snap.add_table(make_table("db__movesexecution1__baserate_1_2001", 1))
            .unwrap();
        snap.add_table(make_table("db__movesexecution1__baserate_2_2001", 2))
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        snap.write(dir.path()).unwrap();

        // Remove bundle to force Parquet path.
        let bundle_path = dir.path().join("tables").join("execution-db.bundle");
        std::fs::remove_file(&bundle_path).unwrap();

        // Allowed set contains only the canonical base name.
        let mut allowed: BTreeSet<String> = BTreeSet::new();
        allowed.insert("baserate".to_string());

        let filter = SnapshotFilter::from_run_spec(&moves_runspec::RunSpec::default());
        let store = load_execution_db(dir.path(), &filter, Some(&allowed))
            .expect("indexed Parquet load must succeed");

        // The canonical "baserate" must exist and have 2 merged rows.
        assert!(
            store.contains("baserate"),
            "canonical baserate must exist after merge"
        );
        let merged = store.get("baserate").unwrap();
        assert_eq!(
            merged.height(),
            2,
            "merged baserate must have 2 rows (one from each variant)"
        );
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

    #[test]
    fn strip_numeric_index_suffix_strips_process_year() {
        assert_eq!(strip_numeric_index_suffix("baserate_1_2001"), "baserate");
        assert_eq!(
            strip_numeric_index_suffix("baseratebyage_90_2020"),
            "baseratebyage"
        );
        assert_eq!(
            strip_numeric_index_suffix("sourcebindistributionfuelusage_1_26161_2001"),
            "sourcebindistributionfuelusage"
        );
    }

    #[test]
    fn strip_numeric_index_suffix_no_change_plain_name() {
        assert_eq!(strip_numeric_index_suffix("baserate"), "baserate");
        assert_eq!(strip_numeric_index_suffix("activitytype"), "activitytype");
        // year-only suffix (no underscore separator) is unchanged
        assert_eq!(
            strip_numeric_index_suffix("stmytvvcoeffs2020"),
            "stmytvvcoeffs2020"
        );
    }

    #[test]
    fn merge_process_year_variants_unions_rows() {
        use moves_framework::{DataFrameStore, InMemoryStore};
        use polars::prelude::{DataFrame, NamedFrom, Series};

        let make_df = |row_val: i64| {
            let s = Series::new("id".into(), [row_val]);
            DataFrame::new(1, vec![s.into()]).unwrap()
        };

        let mut store = InMemoryStore::new();
        // Two variants for "baserate", plus an empty canonical stub.
        store.insert("baserate", DataFrame::default());
        store.insert("baserate_1_2001", make_df(1));
        store.insert("baserate_2_2001", make_df(2));
        // Unrelated table that must not be touched.
        store.insert("activitytype", make_df(99));

        merge_process_year_variants(&mut store).expect("merge must succeed");

        // Canonical "baserate" must now have 2 rows (union of the two variants).
        let merged = store.get("baserate").expect("baserate must exist");
        assert_eq!(merged.height(), 2, "merged baserate must have 2 rows");
        // Unrelated table must be untouched.
        assert_eq!(
            store.get("activitytype").unwrap().height(),
            1,
            "activitytype must be unchanged"
        );
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

    /// Build a one-row `sourceUseTypePhysics` DataFrame mirroring the snapshot
    /// schema (int64 IDs, string road-load terms).
    fn make_physics_df() -> polars::prelude::DataFrame {
        use polars::prelude::*;
        df!(
            "sourceTypeID" => &[21i64],
            "regClassID" => &[20i64],
            "beginModelYearID" => &[1950i64],
            "endModelYearID" => &[2060i64],
            "rollingTermA" => &["0.156461000000"],
            "rotatingTermB" => &["0.002001930000"],
            "dragTermC" => &["0.000492646000"],
            "sourceMass" => &["1.478800000000"],
            "fixedMassFactor" => &["1.478800000000"],
        )
        .unwrap()
    }

    #[test]
    fn synthesises_identity_physics_mapping_when_absent() {
        let mut store = InMemoryStore::new();
        store.insert("sourceUseTypePhysics".to_string(), make_physics_df());
        populate_source_use_type_physics_mapping(&mut store).expect("synthesis should succeed");

        let df = store
            .get("sourceUseTypePhysicsMapping")
            .expect("mapping table must be synthesised");
        // All 11 mapping columns present.
        for col in [
            "realSourceTypeID",
            "tempSourceTypeID",
            "regClassID",
            "beginModelYearID",
            "endModelYearID",
            "opModeIDOffset",
            "rollingTermA",
            "rotatingTermB",
            "dragTermC",
            "sourceMass",
            "fixedMassFactor",
        ] {
            assert!(df.column(col).is_ok(), "missing column {col}");
        }
        // Identity mapping: real == temp == sourceTypeID, offset 0.
        let real = df.column("realSourceTypeID").unwrap().i64().unwrap();
        let temp = df.column("tempSourceTypeID").unwrap().i64().unwrap();
        let offset = df.column("opModeIDOffset").unwrap().i64().unwrap();
        assert_eq!(real.get(0), Some(21));
        assert_eq!(temp.get(0), Some(21));
        assert_eq!(offset.get(0), Some(0));
        // The original sourceTypeID column is renamed, not duplicated.
        assert!(df.column("sourceTypeID").is_err());
    }

    #[test]
    fn physics_mapping_synthesis_is_noop_when_mapping_present() {
        let mut store = InMemoryStore::new();
        store.insert("sourceUseTypePhysics".to_string(), make_physics_df());
        // Pre-existing mapping (single sentinel column) must be left untouched.
        let sentinel = polars::prelude::df!("realSourceTypeID" => &[99i64]).unwrap();
        store.insert("sourceUseTypePhysicsMapping".to_string(), sentinel);
        populate_source_use_type_physics_mapping(&mut store).expect("noop");
        let df = store.get("sourceUseTypePhysicsMapping").unwrap();
        assert_eq!(df.width(), 1, "existing mapping must not be rebuilt");
        assert_eq!(
            df.column("realSourceTypeID").unwrap().i64().unwrap().get(0),
            Some(99)
        );
    }

    #[test]
    fn physics_mapping_synthesis_is_noop_without_source_table() {
        let mut store = InMemoryStore::new();
        populate_source_use_type_physics_mapping(&mut store).expect("noop");
        assert!(!store.contains("sourceUseTypePhysicsMapping"));
    }

    /// One-row `ZoneMonthHour` with the three derived columns left NULL, plus
    /// the `Zone`/`County` rows meteorology needs to resolve pressure.
    fn make_raw_meteorology_store() -> InMemoryStore {
        use polars::prelude::*;
        let mut store = InMemoryStore::new();
        let zmh = df!(
            "zoneID" => &[100i64],
            "monthID" => &[7i64],
            "hourID" => &[12i64],
            "temperature" => &[90.0f64],
            "relHumidity" => &[60.0f64],
            "heatIndex" => &[None::<f64>],
            "specificHumidity" => &[None::<f64>],
            "molWaterFraction" => &[None::<f64>],
        )
        .unwrap();
        let zone = df!("zoneID" => &[100i64], "countyID" => &[26161i64]).unwrap();
        let county = df!(
            "countyID" => &[26161i64],
            "barometricPressure" => &[29.92f64],
            "altitude" => &[None::<&str>],
        )
        .unwrap();
        store.insert("ZoneMonthHour".to_string(), zmh);
        store.insert("Zone".to_string(), zone);
        store.insert("County".to_string(), county);
        store
    }

    #[test]
    fn fills_null_meteorology_columns() {
        let mut store = make_raw_meteorology_store();
        populate_zone_month_hour_meteorology(&mut store).expect("fill should succeed");
        let df = store.get("ZoneMonthHour").unwrap();
        // 90 °F / 60 % RH is above the 78 °F threshold, so the regression makes
        // heatIndex exceed the dry-bulb temperature, and the humidity terms are
        // populated (non-null).
        let hi = df
            .column("heatIndex")
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();
        assert!(hi > 90.0, "heatIndex {hi} should exceed temperature");
        assert!(df
            .column("specificHumidity")
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .is_some());
        assert!(df
            .column("molWaterFraction")
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .is_some());
    }

    #[test]
    fn meteorology_fill_noop_when_already_populated() {
        use polars::prelude::*;
        let mut store = InMemoryStore::new();
        let zmh = df!(
            "zoneID" => &[100i64], "monthID" => &[7i64], "hourID" => &[12i64],
            "temperature" => &[90.0f64], "relHumidity" => &[60.0f64],
            "heatIndex" => &[Some(123.0f64)],
            "specificHumidity" => &[Some(1.0f64)],
            "molWaterFraction" => &[Some(0.1f64)],
        )
        .unwrap();
        store.insert("ZoneMonthHour".to_string(), zmh);
        populate_zone_month_hour_meteorology(&mut store).expect("noop");
        let hi = store
            .get("ZoneMonthHour")
            .unwrap()
            .column("heatIndex")
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(hi, 123.0, "pre-populated heatIndex must be preserved");
    }

    #[test]
    fn meteorology_fill_noop_when_no_zmh() {
        let mut store = InMemoryStore::new();
        populate_zone_month_hour_meteorology(&mut store).expect("noop");
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
        // The reconstruction recovers ~63 calculator-graph modules.
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
            vec![7i64],
            "the model `months` already holds the real monthID; SnapshotFilter \
             uses it verbatim (key→ID conversion happens in the XML parser)"
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
        // The model `months` holds the real monthID (here 7) — the key→ID
        // conversion is the XML parser's job, so SnapshotFilter uses it
        // verbatim. The fixture has rows for monthID 7 and 8 at zone=100; the
        // filter is zone=100 AND monthID=7, so only the month-7 row survives
        // and zone=200 is dropped.
        assert_eq!(
            df.height(),
            1,
            "filter should keep only the month=7 row for zone 100"
        );
        let zone_col = df
            .column("zoneID")
            .unwrap()
            .cast(&polars::prelude::DataType::Int64)
            .unwrap();
        assert!(
            zone_col.i64().unwrap().into_iter().all(|v| v == Some(100)),
            "all surviving rows must be zone 100"
        );
    }

    // ---- populate_link_from_zone_road_type tests ----

    fn make_zone_road_type_store() -> InMemoryStore {
        use polars::prelude::*;
        let mut store = InMemoryStore::new();
        let zrt = df!(
            "zoneID"       => &[261610i32, 261610i32, 261610i32],
            "roadTypeID"   => &[2i32, 4i32, 5i32],
            "SHOAllocFactor" => &[0.5f64, 0.3f64, 0.2f64],
        )
        .unwrap();
        store.insert("ZoneRoadType".to_string(), zrt);
        store
    }

    #[test]
    fn link_synthesised_from_zone_road_type() {
        let mut store = make_zone_road_type_store();
        populate_link_from_zone_road_type(&mut store).expect("should succeed");

        let link = store.get("link").expect("Link must be in store");
        assert_eq!(link.height(), 3, "one Link row per (zone, road-type) pair");

        let link_ids: Vec<i32> = link
            .column("linkID")
            .unwrap()
            .i32()
            .unwrap()
            .into_no_null_iter()
            .collect();
        // linkID = zoneID * 10 + roadTypeID
        assert!(link_ids.contains(&(261610 * 10 + 2)), "linkID for road 2");
        assert!(link_ids.contains(&(261610 * 10 + 4)), "linkID for road 4");
        assert!(link_ids.contains(&(261610 * 10 + 5)), "linkID for road 5");

        let county_ids: Vec<i32> = link
            .column("countyID")
            .unwrap()
            .i32()
            .unwrap()
            .into_no_null_iter()
            .collect();
        // countyID = zoneID / 10
        assert!(
            county_ids.iter().all(|&c| c == 26161),
            "countyID = zoneID/10"
        );
    }

    #[test]
    fn link_synthesis_noop_when_zone_road_type_absent() {
        let mut store = InMemoryStore::new();
        populate_link_from_zone_road_type(&mut store).expect("noop");
        assert!(!store.contains("link"), "no Link should be created");
    }

    #[test]
    fn link_synthesis_noop_when_link_already_populated() {
        use polars::prelude::*;
        let mut store = make_zone_road_type_store();
        // Pre-populate a Link row — synthesis must not overwrite it.
        let existing_link =
            df!("linkID" => &[999i32], "countyID" => &[99i32], "zoneID" => &[990i32], "roadTypeID" => &[1i32]).unwrap();
        store.insert("Link".to_string(), existing_link);
        populate_link_from_zone_road_type(&mut store).expect("noop");
        let link = store.get("link").unwrap();
        assert_eq!(link.height(), 1, "pre-existing Link must be preserved");
        assert_eq!(
            link.column("linkID")
                .unwrap()
                .i32()
                .unwrap()
                .get(0)
                .unwrap(),
            999
        );
    }

    #[test]
    fn link_synthesis_deduplicates_zone_road_type_pairs() {
        use polars::prelude::*;
        let mut store = InMemoryStore::new();
        // Duplicate rows — should produce only 1 unique link.
        let zrt = df!(
            "zoneID"       => &[100i32, 100i32],
            "roadTypeID"   => &[2i32, 2i32],
            "SHOAllocFactor" => &[0.5f64, 0.5f64],
        )
        .unwrap();
        store.insert("ZoneRoadType".to_string(), zrt);
        populate_link_from_zone_road_type(&mut store).expect("should succeed");
        let link = store.get("link").unwrap();
        assert_eq!(link.height(), 1, "duplicates must be merged");
    }

    // ---- build_runspec_tables tests ----

    fn fixture_runspec() -> RunSpec {
        use moves_runspec::{
            GeoKind, GeographicSelection, OnroadVehicleSelection, PollutantProcessAssociation,
            RoadType, Timespan,
        };
        RunSpec {
            geographic_selections: vec![GeographicSelection {
                kind: GeoKind::County,
                key: 26161,
                description: String::new(),
            }],
            timespan: Timespan {
                years: vec![2020],
                months: vec![7],
                days: vec![5],
                begin_hour: Some(6),
                end_hour: Some(6),
                ..Default::default()
            },
            onroad_vehicle_selections: vec![OnroadVehicleSelection {
                fuel_type_id: 1,
                fuel_type_name: "Gasoline".into(),
                source_type_id: 21,
                source_type_name: "Passenger Car".into(),
            }],
            road_types: vec![RoadType {
                road_type_id: 4,
                road_type_name: "Urban Restricted Access".into(),
                model_combination: None,
            }],
            pollutant_process_associations: vec![PollutantProcessAssociation {
                pollutant_id: 3,
                pollutant_name: "NOx".into(),
                process_id: 1,
                process_name: "Running Exhaust".into(),
            }],
            ..Default::default()
        }
    }

    #[test]
    fn build_runspec_tables_produces_all_tables() {
        let runspec = fixture_runspec();
        let mut store = InMemoryStore::new();
        build_runspec_tables(&runspec, &mut store).expect("should succeed");

        for table in &[
            "RunSpecSourceType",
            "RunSpecPollutantProcess",
            "RunSpecDay",
            "RunSpecHour",
            "RunSpecHourDay",
            "RunSpecMonth",
            "RunSpecYear",
            "RunSpecRoadType",
            "RunSpecMonthGroup",
            "RunSpecSourceFuelType",
        ] {
            assert!(
                store.contains(table),
                "table '{table}' must be present after build_runspec_tables"
            );
        }
    }

    #[test]
    fn build_runspec_tables_source_type_values() {
        let runspec = fixture_runspec();
        let mut store = InMemoryStore::new();
        build_runspec_tables(&runspec, &mut store).expect("should succeed");

        let df = store.get("runspecsourcetype").unwrap();
        assert_eq!(df.height(), 1);
        assert_eq!(
            df.column("sourceTypeID").unwrap().i32().unwrap().get(0),
            Some(21)
        );
    }

    #[test]
    fn build_runspec_tables_hour_day_packed_key() {
        // hourID=6, dayID=5 → hourDayID = 6*10+5 = 65
        let runspec = fixture_runspec();
        let mut store = InMemoryStore::new();
        build_runspec_tables(&runspec, &mut store).expect("should succeed");

        let df = store.get("runspechourday").unwrap();
        assert_eq!(df.height(), 1, "one hourDay for (hour=6, day=5)");
        assert_eq!(
            df.column("hourDayID").unwrap().i32().unwrap().get(0),
            Some(65),
            "hourDayID = 6*10+5 = 65"
        );
    }

    #[test]
    fn build_runspec_tables_pol_process_id() {
        // pollutant=3, process=1 → polProcessID = 301
        let runspec = fixture_runspec();
        let mut store = InMemoryStore::new();
        build_runspec_tables(&runspec, &mut store).expect("should succeed");

        let df = store.get("runspecpollutantprocess").unwrap();
        assert_eq!(df.height(), 1);
        assert_eq!(
            df.column("polProcessID").unwrap().i32().unwrap().get(0),
            Some(301)
        );
    }

    #[test]
    fn build_runspec_tables_source_fuel_type_int64() {
        // RunSpecSourceFuelType uses Int64 per SourceBinDistributionGenerator schema.
        let runspec = fixture_runspec();
        let mut store = InMemoryStore::new();
        build_runspec_tables(&runspec, &mut store).expect("should succeed");

        let df = store.get("runspecsourcefueltype").unwrap();
        assert_eq!(df.height(), 1);
        assert_eq!(
            df.column("sourceTypeID").unwrap().i64().unwrap().get(0),
            Some(21)
        );
        assert_eq!(
            df.column("fuelTypeID").unwrap().i64().unwrap().get(0),
            Some(1)
        );
    }

    #[test]
    fn build_runspec_tables_month_group_fallback_to_month_id() {
        // No MonthGroupOfAnyYear in store → monthGroupID == monthID.
        let runspec = fixture_runspec();
        let mut store = InMemoryStore::new();
        build_runspec_tables(&runspec, &mut store).expect("should succeed");

        let df = store.get("runspecmonthgroup").unwrap();
        assert_eq!(df.height(), 1);
        assert_eq!(
            df.column("monthGroupID").unwrap().i32().unwrap().get(0),
            Some(7), // same as monthID
        );
    }

    #[test]
    fn build_runspec_tables_month_group_from_lookup_table() {
        use polars::prelude::*;
        let runspec = fixture_runspec(); // month=7
        let mut store = InMemoryStore::new();
        // MonthGroupOfAnyYear: monthID=7 → monthGroupID=3 (synthetic mapping).
        let mgoay = df!(
            "monthID"      => &[7i32, 8i32],
            "monthGroupID" => &[3i32, 4i32],
        )
        .unwrap();
        store.insert("MonthGroupOfAnyYear".to_string(), mgoay);
        build_runspec_tables(&runspec, &mut store).expect("should succeed");

        let df = store.get("runspecmonthgroup").unwrap();
        assert_eq!(df.height(), 1);
        assert_eq!(
            df.column("monthGroupID").unwrap().i32().unwrap().get(0),
            Some(3), // looked up from MonthGroupOfAnyYear
        );
    }

    // ---------- derive_fuel_years_from_store ---------------------------------

    #[test]
    fn derive_fuel_years_extracts_matching_year_rows() {
        use polars::prelude::*;
        let mut store = InMemoryStore::new();
        // Year table: 2019→2018 (historical boundary), 2020→2019, 2021→2020.
        let year_df = df!(
            "yearID"     => &[2019i32, 2020i32, 2021i32],
            "isBaseYear" => &[0i32, 0i32, 0i32],
            "fuelYearID" => &[2018i32, 2019i32, 2020i32],
        )
        .unwrap();
        store.insert("Year", year_df);

        // RunSpec years = [2020] → fuelYearID should be [2019].
        let fuel_years = derive_fuel_years_from_store(&store, &[2020]);
        assert_eq!(fuel_years, vec![2019i64]);
    }

    #[test]
    fn derive_fuel_years_multi_year_deduplicates() {
        use polars::prelude::*;
        let mut store = InMemoryStore::new();
        // Two calendar years map to the same fuelYearID (many-to-one).
        let year_df = df!(
            "yearID"     => &[2020i32, 2021i32, 2022i32],
            "isBaseYear" => &[0i32, 0i32, 0i32],
            "fuelYearID" => &[2019i32, 2019i32, 2021i32],
        )
        .unwrap();
        store.insert("Year", year_df);

        let fuel_years = derive_fuel_years_from_store(&store, &[2020, 2021]);
        assert_eq!(fuel_years, vec![2019i64]); // deduplicated
    }

    #[test]
    fn derive_fuel_years_returns_empty_when_year_table_absent() {
        let store = InMemoryStore::new();
        let fuel_years = derive_fuel_years_from_store(&store, &[2020]);
        assert!(fuel_years.is_empty());
    }

    // ---------- derive_region_ids_from_store ---------------------------------

    #[test]
    fn derive_region_ids_always_includes_wildcard_zero() {
        // Even with an empty regionCounty table, region 0 must be present.
        let mut store = InMemoryStore::new();
        use polars::prelude::*;
        let rc_df = df!(
            "regionID" => Vec::<i32>::new(),
            "countyID" => Vec::<i32>::new(),
            "fuelYearID" => Vec::<i32>::new(),
        )
        .unwrap();
        store.insert("regionCounty", rc_df);
        let region_ids = derive_region_ids_from_store(&store);
        assert_eq!(region_ids, vec![0i64]);
    }

    #[test]
    fn derive_region_ids_unions_table_values_with_zero() {
        use polars::prelude::*;
        let mut store = InMemoryStore::new();
        let rc_df = df!(
            "regionID"   => &[100000000i32, 200000000i32, 100000000i32],
            "countyID"   => &[40001i32, 40003i32, 40005i32],
            "fuelYearID" => &[2019i32, 2019i32, 2019i32],
        )
        .unwrap();
        store.insert("regionCounty", rc_df);
        let region_ids = derive_region_ids_from_store(&store);
        // 0 (wildcard) + unique regionIDs from the table.
        assert_eq!(region_ids, vec![0i64, 100000000i64, 200000000i64]);
    }

    #[test]
    fn derive_region_ids_returns_only_zero_when_table_absent() {
        let store = InMemoryStore::new();
        let region_ids = derive_region_ids_from_store(&store);
        assert_eq!(region_ids, vec![0i64]);
    }

    // ---------- overlay_scale_input_db ----------------------------------------

    /// Write `df` to `<dir>/<name>.parquet`.
    fn write_parquet_to_dir(
        dir: &std::path::Path,
        name: &str,
        df: &mut polars::prelude::DataFrame,
    ) {
        use polars::prelude::{ParquetCompression, ParquetWriter, StatisticsOptions};
        let path = dir.join(format!("{name}.parquet"));
        let file = std::fs::File::create(&path).expect("create parquet file");
        ParquetWriter::new(file)
            .with_compression(ParquetCompression::Uncompressed)
            .with_statistics(StatisticsOptions::empty())
            .finish(df)
            .expect("write parquet");
    }

    #[test]
    fn overlay_scale_input_db_loads_parquet_into_store() {
        use polars::prelude::*;
        let dir = tempfile::tempdir().unwrap();

        let mut df = df!(
            "sourceTypeID" => &[21i32],
            "population"   => &[1000i32],
        )
        .unwrap();
        write_parquet_to_dir(dir.path(), "SourceTypePopulation", &mut df);

        let mut store = InMemoryStore::new();
        overlay_scale_input_db(&mut store, dir.path()).expect("overlay must succeed");

        assert!(
            store.contains("SourceTypePopulation"),
            "SourceTypePopulation must be in store after overlay"
        );
        let loaded = store.get("SourceTypePopulation").unwrap();
        assert_eq!(loaded.height(), 1, "one row should be loaded");
    }

    #[test]
    fn overlay_scale_input_db_overrides_existing_table() {
        // CDB/PDB tables take precedence over same-named tables already in the store.
        use polars::prelude::*;
        let dir = tempfile::tempdir().unwrap();

        // Write a CDB SourceTypePopulation with count=999.
        let mut cdb_df = df!(
            "sourceTypeID" => &[21i32],
            "population"   => &[999i32],
        )
        .unwrap();
        write_parquet_to_dir(dir.path(), "SourceTypePopulation", &mut cdb_df);

        // Pre-seed the store with a same-named table (default DB value = 100).
        let default_df = df!(
            "sourceTypeID" => &[21i32],
            "population"   => &[100i32],
        )
        .unwrap();
        let mut store = InMemoryStore::new();
        store.insert("SourceTypePopulation".to_string(), default_df);

        overlay_scale_input_db(&mut store, dir.path()).expect("overlay must succeed");

        // CDB value (999) should win over the prior default (100).
        let result = store.get("SourceTypePopulation").unwrap();
        assert_eq!(result.height(), 1);
        let pop = result
            .column("population")
            .unwrap()
            .i32()
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(pop, 999, "CDB population (999) must override default (100)");
    }

    #[test]
    fn overlay_scale_input_db_skips_non_parquet_files() {
        use polars::prelude::*;
        let dir = tempfile::tempdir().unwrap();

        // Write a JSON file that should be ignored.
        std::fs::write(dir.path().join("manifest.json"), b"{}").unwrap();

        // Write one real Parquet table.
        let mut df = df!("zoneID" => &[261610i32], "roadTypeID" => &[4i32]).unwrap();
        write_parquet_to_dir(dir.path(), "ZoneRoadType", &mut df);

        let mut store = InMemoryStore::new();
        overlay_scale_input_db(&mut store, dir.path()).expect("overlay must succeed");

        assert!(
            store.contains("ZoneRoadType"),
            "ZoneRoadType must be loaded"
        );
        // The JSON file must not produce any table.
        assert_eq!(
            store.names().len(),
            1,
            "only ZoneRoadType should be in store, not the .json file"
        );
    }

    #[test]
    fn scale_only_path_synthesises_runspec_tables_and_link() {
        // Verifies that the scale-only code path (no snapshot, no default DB)
        // runs the same synthesis steps as the default-DB path: RunSpec* tables
        // are built from the RunSpec, and Link is synthesised from ZoneRoadType
        // when CDB provides ZoneRoadType but no explicit Link table.
        use polars::prelude::*;
        let dir = tempfile::tempdir().unwrap();

        // Minimal CDB: ZoneRoadType (county 26161, zone 261610, Urban Restricted = 4).
        let mut zrt_df = df!(
            "zoneID"     => &[261610i32],
            "roadTypeID" => &[4i32],
            "SHOAllocFactor" => &[1.0f64],
        )
        .unwrap();
        write_parquet_to_dir(dir.path(), "ZoneRoadType", &mut zrt_df);

        // Build a minimal RunSpec matching scale-county.xml.
        use moves_runspec::{
            GeoKind, GeographicSelection, ModelDomain, OnroadVehicleSelection, RunSpec, Timespan,
        };
        let run_spec = RunSpec {
            domain: Some(ModelDomain::Single),
            geographic_selections: vec![GeographicSelection {
                kind: GeoKind::County,
                key: 26161,
                description: "Washtenaw County".to_string(),
            }],
            timespan: Timespan {
                years: vec![2020],
                months: vec![7],
                days: vec![5],
                begin_hour: Some(6),
                end_hour: Some(6),
                ..Default::default()
            },
            onroad_vehicle_selections: vec![OnroadVehicleSelection {
                fuel_type_id: 1,
                source_type_id: 21,
                fuel_type_name: "Gasoline".to_string(),
                source_type_name: "Passenger Car".to_string(),
            }],
            ..Default::default()
        };

        // Run the synthesis steps against a store seeded from the CDB dir.
        let mut store = InMemoryStore::new();
        overlay_scale_input_db(&mut store, dir.path()).expect("overlay");
        populate_link_from_zone_road_type(&mut store).expect("link synthesis");
        build_runspec_tables(&run_spec, &mut store).expect("runspec tables");

        // Link must have been synthesised from ZoneRoadType.
        assert!(
            store.contains("link"),
            "Link must be synthesised from ZoneRoadType"
        );
        let link = store.get("link").unwrap();
        assert_eq!(
            link.height(),
            1,
            "one link row for zone 261610 + road type 4"
        );

        // RunSpec* tables must be present (calculators read these from the slow tier).
        assert!(
            store.contains("RunSpecSourceType"),
            "RunSpecSourceType must be built from RunSpec"
        );
        assert!(
            store.contains("RunSpecPollutantProcess"),
            "RunSpecPollutantProcess must be built from RunSpec"
        );
        let src_types: Vec<i32> = store
            .get("RunSpecSourceType")
            .unwrap()
            .column("sourceTypeID")
            .unwrap()
            .i32()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(src_types, vec![21i32], "source type 21 from RunSpec");
    }

    #[test]
    fn overlay_scale_input_db_preference_over_default_db_tables() {
        // Integration-level check: when a default-DB store is seeded with a table
        // and the CDB dir contains a same-named file, the CDB value wins.
        // This is the canonical acceptance test for Task 144 /
        // bead mo-2yx: "a SINGLE-scale run with a minimal county input DB reads
        // the user-supplied tables over defaults."
        use polars::prelude::*;
        let dir = tempfile::tempdir().unwrap();

        // CDB SourceTypePopulation: population=500 (user-supplied).
        let mut cdb_stp = df!(
            "sourceTypeID" => &[21i32],
            "yearID"       => &[2020i32],
            "population"   => &[500i32],
        )
        .unwrap();
        write_parquet_to_dir(dir.path(), "SourceTypePopulation", &mut cdb_stp);

        // Simulate a default-DB store that has population=100 for the same row.
        let default_stp = df!(
            "sourceTypeID" => &[21i32],
            "yearID"       => &[2020i32],
            "population"   => &[100i32],
        )
        .unwrap();
        let mut store = InMemoryStore::new();
        store.insert("SourceTypePopulation".to_string(), default_stp);

        // Overlay the CDB — mirrors what build_default_db_store does for path C
        // and what run_simulation now does for path B (scale-only).
        overlay_scale_input_db(&mut store, dir.path()).expect("overlay");

        let result = store.get("SourceTypePopulation").unwrap();
        let pop = result
            .column("population")
            .unwrap()
            .i32()
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(
            pop, 500,
            "user-supplied CDB population (500) must override default (100)"
        );
    }
}

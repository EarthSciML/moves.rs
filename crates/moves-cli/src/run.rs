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
use moves_framework::{
    read_execution_bundle, read_execution_bundle_filtered, CalculatorRegistry, CountyRow,
    DataFrameStore, DataFrameStoreTyped, EngineConfig, EngineOutcome, GeographyTables,
    InMemoryStore, LinkRow, MOVESEngine,
};
use moves_runspec::{GeoKind, RunSpec};
use polars::prelude::{
    col, concat, lit, Expr, IntoLazy, LazyFrame, NamedFrom, PlRefPath, ScanArgsParquet, SerReader,
    Series, UnionArgs,
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
    let has_slow_store = opts.snapshot.is_some() || opts.scale_input.is_some();
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
        Some(tables)
    } else {
        None
    };
    let mut engine = MOVESEngine::new(run_spec, registry, config);
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
        engine = engine.with_slow_store(store);
        engine
            .execution_run_spec_mut()
            .build_execution_locations(&geography);
    } else if let Some(scale_dir) = &opts.scale_input {
        // Scale-only path: no snapshot — build a store from CDB/PDB Parquet alone.
        let mut store = InMemoryStore::new();
        overlay_scale_input_db(&mut store, scale_dir)
            .with_context(|| format!("loading scale-input DB from {}", scale_dir.display()))?;
        let geography = load_geography_from_store(&store)
            .with_context(|| format!("building geography from {}", scale_dir.display()))?;
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
/// **replacing** any same-named table that was already in the store.  This
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
/// separate MySQL tables (e.g. `baserate_1_2001`, `baseratebyage_90_2020`).  The
/// calculator reads the canonical name (`baserate`, `baseratebyage`).  This helper
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
    let all_names: Vec<String> = store.names().into_iter().map(|s| s.to_string()).collect();
    let mut by_base: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();
    for name in &all_names {
        let base = strip_numeric_index_suffix(name);
        if base != *name {
            by_base
                .entry(base.to_string())
                .or_default()
                .push(name.clone());
        }
    }
    for (base, variant_names) in by_base {
        let frames: Vec<LazyFrame> = variant_names
            .iter()
            .filter_map(|vname| store.get(vname))
            .filter(|df| df.height() > 0)
            .map(|df| df.as_ref().clone().lazy())
            .collect();
        if frames.is_empty() {
            continue;
        }
        let merged = if frames.len() == 1 {
            frames.into_iter().next().unwrap().collect()
        } else {
            concat(frames, UnionArgs::default())
                .map_err(|e| anyhow::anyhow!("concat {base} variants: {e}"))?
                .collect()
        }
        .map_err(|e| anyhow::anyhow!("collecting merged {base}: {e}"))?;
        store.insert(base, merged);
    }
    Ok(())
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
    use polars::prelude::{NamedFrom, Series};

    // Nothing to do if the mapping is already present, or there is no source
    // physics table to derive it from.
    if store.contains("sourceUseTypePhysicsMapping") || !store.contains("sourceUseTypePhysics") {
        return Ok(());
    }

    let physics = store
        .get("sourceUseTypePhysics")
        .context("sourceUseTypePhysics not in store after contains check")?;
    let mut mapping: polars::prelude::DataFrame = (*physics).clone();
    drop(physics); // release the Arc clone before mutating the store below

    // Resolve the source-type column case-insensitively (snapshot column
    // casings vary), then rename it to the mapping's `realSourceTypeID`.
    let src_col = mapping
        .get_column_names()
        .iter()
        .find(|n| n.as_str().eq_ignore_ascii_case("sourceTypeID"))
        .map(|n| n.to_string())
        .context("sourceUseTypePhysics has no sourceTypeID column")?;
    mapping
        .rename(&src_col, "realSourceTypeID".into())
        .map_err(|e| anyhow::anyhow!("renaming sourceTypeID -> realSourceTypeID: {e}"))?;

    // tempSourceTypeID is a copy of realSourceTypeID for the identity mapping.
    let mut temp = mapping
        .column("realSourceTypeID")
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .clone();
    temp.rename("tempSourceTypeID".into());
    let n = mapping.height();
    mapping
        .with_column(temp)
        .map_err(|e| anyhow::anyhow!("adding tempSourceTypeID: {e}"))?;
    mapping
        .with_column(Series::new("opModeIDOffset".into(), vec![0i64; n]).into())
        .map_err(|e| anyhow::anyhow!("adding opModeIDOffset: {e}"))?;

    store.insert("sourceUseTypePhysicsMapping".to_string(), mapping);
    Ok(())
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
    use moves_calculators::generators::meteorology::{build_meteorology_table, MeteorologyInputs};
    use polars::prelude::{DataType, NamedFrom, Series};

    if !store.contains("ZoneMonthHour") {
        return Ok(());
    }

    // Early exit: if heatIndex already carries any non-null value, the snapshot
    // captured the computed columns — leave the table untouched.
    {
        let zmh = store
            .get("ZoneMonthHour")
            .context("ZoneMonthHour not in store after contains check")?;
        let already_filled = zmh
            .columns()
            .iter()
            .find(|c| c.name().eq_ignore_ascii_case("heatIndex"))
            .and_then(|c| c.cast(&DataType::Float64).ok())
            .and_then(|c| c.f64().ok().cloned())
            .is_some_and(|ca| ca.into_iter().any(|v| v.is_some()));
        if already_filled {
            return Ok(());
        }
    }

    // Zone and County are needed to resolve each county's barometric pressure.
    if !store.contains("Zone") || !store.contains("County") {
        return Ok(());
    }

    let inputs = MeteorologyInputs {
        zone_month_hour: store
            .iter_typed("ZoneMonthHour")
            .context("reading ZoneMonthHour for meteorology")?,
        zone: store.iter_typed("Zone").context("reading Zone")?,
        county: store.iter_typed("County").context("reading County")?,
    };
    let computed = build_meteorology_table(&inputs);

    // Index the computed meteorology by (zoneID, monthID, hourID).
    let mut by_key: std::collections::HashMap<(i32, i32, i32), (f64, f64, f64)> =
        std::collections::HashMap::with_capacity(computed.len());
    for r in &computed {
        by_key.insert(
            (r.zone_id, r.month_id, r.hour_id),
            (r.heat_index, r.specific_humidity, r.mol_water_fraction),
        );
    }

    // Read the existing key columns + temperature (the unmatched-row fallback)
    // in DataFrame row order, then release the Arc before mutating the store.
    let (heat, spec, mol) = {
        let zmh = store.get("ZoneMonthHour").expect("ZoneMonthHour present");
        let df = &*zmh;
        let find = |want: &str| -> Result<polars::prelude::Column> {
            let lower = want.to_ascii_lowercase();
            df.columns()
                .iter()
                .find(|c| c.name().to_ascii_lowercase() == lower)
                .cloned()
                .with_context(|| format!("ZoneMonthHour column '{want}' not found"))
        };
        let to_i32 = |c: polars::prelude::Column| -> Result<Vec<i32>> {
            Ok(c.cast(&DataType::Int32)
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .i32()
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .into_iter()
                .map(|v| v.unwrap_or(0))
                .collect())
        };
        let zone = to_i32(find("zoneID")?)?;
        let month = to_i32(find("monthID")?)?;
        let hour = to_i32(find("hourID")?)?;
        let temps: Vec<f64> = find("temperature")?
            .cast(&DataType::Float64)
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .f64()
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .into_iter()
            .map(|v| v.unwrap_or(0.0))
            .collect();
        let n = df.height();
        let mut heat = Vec::with_capacity(n);
        let mut spec = Vec::with_capacity(n);
        let mut mol = Vec::with_capacity(n);
        for i in 0..n {
            match by_key.get(&(zone[i], month[i], hour[i])) {
                Some(&(hi, sh, mwf)) => {
                    heat.push(hi);
                    spec.push(sh);
                    mol.push(mwf);
                }
                None => {
                    heat.push(temps[i]);
                    spec.push(0.0);
                    mol.push(0.0);
                }
            }
        }
        (heat, spec, mol)
    };

    let zmh_mut = store
        .get_mut("ZoneMonthHour")
        .expect("ZoneMonthHour present");
    zmh_mut
        .with_column(Series::new("heatIndex".into(), heat).into())
        .map_err(|e| anyhow::anyhow!("replacing ZoneMonthHour.heatIndex: {e}"))?;
    zmh_mut
        .with_column(Series::new("specificHumidity".into(), spec).into())
        .map_err(|e| anyhow::anyhow!("replacing ZoneMonthHour.specificHumidity: {e}"))?;
    zmh_mut
        .with_column(Series::new("molWaterFraction".into(), mol).into())
        .map_err(|e| anyhow::anyhow!("replacing ZoneMonthHour.molWaterFraction: {e}"))?;
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

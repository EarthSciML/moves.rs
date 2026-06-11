//! Shared post-load execution-DB store synthesis for the default-DB path.
//!
//! Both entry points — `moves-cli`'s `build_default_db_store` (native, loads via
//! `InputDataManager` + `DefaultDb`) and `moves-wasm`'s default-DB flow (browser,
//! loads from an Arrow-IPC bundle) — must apply the *same* post-load synthesis
//! before running the engine: merge process/year variant tables, prune
//! geography-keyed national tables to the run's counties, synthesise `Link` and
//! the `RunSpec*` tables, fill derived meteorology columns, and so on.
//!
//! These functions previously lived in two hand-maintained copies (one in each
//! crate). They drifted, which silently broke the `default_db_snapshot_diff`
//! gate (the native path fell behind the browser path). They now live here, in
//! the single crate both callers depend on, so the gate validates the exact
//! synthesis the demo runs.
//!
//! All polars operations are polars-core only (no lazy / parquet), so this
//! module compiles for `wasm32-unknown-unknown`.

use std::collections::{BTreeMap, BTreeSet};

use moves_framework::{DataFrameStore, DataFrameStoreTyped, InMemoryStore};
use moves_runspec::RunSpec;
use polars::prelude::{BooleanChunked, Column, DataFrame, DataType, NamedFrom, Series};

use crate::generators::meteorology::{build_meteorology_table, MeteorologyInputs};

pub fn setup_execution_store(runspec: &RunSpec, store: &mut InMemoryStore) -> Result<(), String> {
    macro_rules! synth_step {
        ($label:expr, $call:expr) => {{
            if std::env::var("MOVES_DEBUG_LOAD").is_ok() {
                use std::io::Write;
                let _ = writeln!(std::io::stderr(), "[synth] {}", $label);
                let _ = std::io::stderr().flush();
            }
            $call?;
        }};
    }
    synth_step!("merge_store_variants_eager", merge_store_variants_eager(store));
    synth_step!(
        "prune_geographic",
        prune_geographic_tables_to_runspec(runspec, store)
    );
    synth_step!(
        "source_use_type_physics",
        populate_source_use_type_physics_mapping(store)
    );
    synth_step!(
        "pollutant_process_mapped",
        populate_pollutant_process_mapped_model_year(store)
    );
    synth_step!(
        "zone_month_hour_meteorology",
        populate_zone_month_hour_meteorology(store)
    );
    synth_step!("link_from_zone_road_type", populate_link_from_zone_road_type(store));
    synth_step!("fill_fuel_supply", fill_fuel_supply_placeholder_nulls(store));
    synth_step!(
        "high_ethanol_fuel_props",
        transform_high_ethanol_fuel_properties(store)
    );
    synth_step!("build_runspec_tables", build_runspec_tables(runspec, store));
    synth_step!(
        "scope_pollutant_process_model_year",
        scope_pollutant_process_model_year_to_runspec(store)
    );
    Ok(())
}

/// Filter `PollutantProcessModelYear` to the `polProcessID`s the run selects
/// (`RunSpecPollutantProcess`). No-op if either table is absent or the keep-set
/// is empty; `prune_table_by_id`'s own guard keeps the full table if nothing
/// matches (so a column/convention mismatch can't empty it).
pub fn scope_pollutant_process_model_year_to_runspec(
    store: &mut InMemoryStore,
) -> Result<(), String> {
    let keep: BTreeSet<i64> = {
        let Some(arc) = store.get("RunSpecPollutantProcess") else {
            return Ok(());
        };
        let df = &*arc;
        let Some(name) = df
            .columns()
            .iter()
            .find(|c| c.name().eq_ignore_ascii_case("polProcessID"))
            .map(|c| c.name().to_string())
        else {
            return Ok(());
        };
        let col = df
            .column(&name)
            .and_then(|c| c.cast(&DataType::Int32))
            .map_err(|e| format!("RunSpecPollutantProcess.polProcessID cast: {e}"))?;
        let ca = col.i32().map_err(|e| format!("{e}"))?;
        ca.into_iter().flatten().map(i64::from).collect()
    };
    if keep.is_empty() {
        return Ok(());
    }
    prune_table_by_id(store, "PollutantProcessModelYear", "polProcessID", &keep)
}

/// Drop rows of geography-keyed national tables that fall outside the runspec's
/// county selections, so the in-browser store build and the calculator chain
/// don't carry ~3000× the data a single-county run needs.
///
/// The default DB ships `ZoneMonthHour` (≈930k rows = 3232 zones × 288
/// month/hours) and `CountyYear` (≈204k rows) as single unpartitioned files.
/// For a county-scoped onroad run only the run's own county/zone is ever read,
/// so the rest is dead weight that dominates the WASM runtime (the
/// `ZoneMonthHour` meteorology synthesis re-materialises every row, and each
/// met-reading calculator rescans the table per chunk).
///
/// Conservative by design:
/// * No-op unless the run is *purely* county-scoped (any state/nation selection
///   leaves the tables intact — we can't enumerate their zones here).
/// * The keep-set of zone IDs is taken from the loaded `Zone` table
///   (`countyID → zoneID`), which is authoritative, falling back to the MOVES
///   `zoneID = countyID × 10` convention only if `Zone` is absent.
/// * Per table, a filter that would drop *every* row is skipped (a column or
///   convention mismatch must not silently empty a table — keep it full and
///   correct, just slow).
pub fn prune_geographic_tables_to_runspec(
    runspec: &RunSpec,
    store: &mut InMemoryStore,
) -> Result<(), String> {
    use moves_runspec::GeoKind;

    let mut county_ids: BTreeSet<i64> = BTreeSet::new();
    let mut has_broader_scope = false;
    for sel in &runspec.geographic_selections {
        match sel.kind {
            GeoKind::County => {
                county_ids.insert(sel.key as i64);
            }
            // State / Nation (or anything else): can't safely enumerate zones.
            _ => has_broader_scope = true,
        }
    }
    if county_ids.is_empty() || has_broader_scope {
        return Ok(());
    }

    // Authoritative zoneIDs for the selected counties, from the Zone table.
    let zone_ids = zone_ids_for_counties(store, &county_ids);

    prune_table_by_id(store, "ZoneMonthHour", "zoneID", &zone_ids)?;
    prune_table_by_id(store, "CountyYear", "countyID", &county_ids)?;
    // County ships as the full national table (3232 rows). Several onroad
    // calculators (e.g. BasicRunningPmEmissionCalculator's fuel_supply_adjustment)
    // iterate `inputs.county` directly, expecting only the run's county; the
    // national table turns that into a 3232x cartesian blow-up (and double-counts
    // the gpa-blended fuel adjustment across every county). Prune to the run's
    // counties.
    prune_table_by_id(store, "County", "countyID", &county_ids)?;
    // Link ships as the full national table (22610 rows across 3232 counties).
    // OperatingModeDistributionGenerator cross-joins it against the op-mode
    // fractions on roadTypeID (`for fraction { for link { if road match }}`),
    // so a national Link turns ~62k fractions into ~200M OpModeDistribution
    // rows — of which only the run county's handful of links are ever consumed.
    // Prune to the run's counties (links carry a countyID column).
    prune_table_by_id(store, "Link", "countyID", &county_ids)?;
    // FuelSupply ships national — every fuelRegionID (~105k rows). The onroad
    // fuel-effect calculators cross-join it (BasicRunningPmEmissionCalculator's
    // fuel_supply_with_fuel_type × fuel_supply_adjustment), so the national
    // table both explodes the join (100k × the rest) and double-counts market
    // share across regions. Resolve the run's fuel regions from regionCounty
    // (countyID → regionID) and prune FuelSupply to them. `regionCounty` maps a
    // county to DIFFERENT regions across fuel years (e.g. 26161 → 200000000 only
    // for fuelYear 1990, → 270000000 for 1999+), so the resolution MUST be scoped
    // to the run's fuel year — otherwise both regions survive and every fuelType's
    // market share sums to ~2, doubling the inventory.
    let fuel_years = fuel_years_for_runspec(store, runspec);
    let region_ids = fuel_region_ids_for_counties(store, &county_ids, &fuel_years);
    if !region_ids.is_empty() {
        prune_table_by_id(store, "FuelSupply", "fuelRegionID", &region_ids)?;
    }
    Ok(())
}

/// Resolve the fuel-region IDs serving `county_ids` for the run's fuel
/// year(s), from `regionCounty` (`countyID`, `regionID`, `fuelYearID`). A
/// county maps to different regions across fuel years, so rows are restricted
/// to `fuel_years` (when non-empty and the column is present) — otherwise the
/// historical regions (e.g. a 1990-only region) survive and double the
/// FuelSupply market share. Returns empty if the table/columns are absent, in
/// which case the caller leaves `FuelSupply` unpruned.
fn fuel_region_ids_for_counties(
    store: &InMemoryStore,
    county_ids: &BTreeSet<i64>,
    fuel_years: &BTreeSet<i64>,
) -> BTreeSet<i64> {
    let Some(arc) = store.get("regionCounty") else {
        return BTreeSet::new();
    };
    let df = &*arc;
    let col = |name: &str| {
        df.columns()
            .iter()
            .find(|c| c.name().eq_ignore_ascii_case(name))
            .and_then(|c| c.cast(&DataType::Int64).ok())
    };
    let (Some(region_col), Some(county_col)) = (col("regionID"), col("countyID")) else {
        return BTreeSet::new();
    };
    let (Ok(rids), Ok(cids)) = (region_col.i64(), county_col.i64()) else {
        return BTreeSet::new();
    };
    // Optional fuelYearID column for year-scoped resolution.
    let fy_ids = col("fuelYearID").and_then(|c| c.i64().ok().cloned());
    let mut out: BTreeSet<i64> = BTreeSet::new();
    for i in 0..df.height() {
        if let (Some(r), Some(c)) = (rids.get(i), cids.get(i)) {
            if !county_ids.contains(&c) {
                continue;
            }
            // Scope to the run's fuel year(s) when known.
            if !fuel_years.is_empty() {
                match fy_ids.as_ref().and_then(|fc| fc.get(i)) {
                    Some(fy) if fuel_years.contains(&fy) => {}
                    Some(_) => continue,
                    None => {}
                }
            }
            out.insert(r);
        }
    }
    out
}

/// Map the runspec's calendar years to `fuelYearID`s via the loaded `Year`
/// table (`yearID` → `fuelYearID`). Empty if `Year` is absent or carries
/// neither column — the caller then leaves the region resolution year-agnostic.
fn fuel_years_for_runspec(store: &InMemoryStore, runspec: &RunSpec) -> BTreeSet<i64> {
    let year_set: BTreeSet<i64> = runspec
        .timespan
        .years
        .iter()
        .map(|&y| i64::from(y))
        .collect();
    if year_set.is_empty() {
        return BTreeSet::new();
    }
    let Some(arc) = store.get("Year") else {
        return BTreeSet::new();
    };
    let df = &*arc;
    let col = |name: &str| {
        df.columns()
            .iter()
            .find(|c| c.name().eq_ignore_ascii_case(name))
            .and_then(|c| c.cast(&DataType::Int64).ok())
    };
    let (Some(yid_col), Some(fyid_col)) = (col("yearID"), col("fuelYearID")) else {
        return BTreeSet::new();
    };
    let (Ok(yids), Ok(fyids)) = (yid_col.i64(), fyid_col.i64()) else {
        return BTreeSet::new();
    };
    let mut out: BTreeSet<i64> = BTreeSet::new();
    for i in 0..df.height() {
        if let (Some(y), Some(fy)) = (yids.get(i), fyids.get(i)) {
            if year_set.contains(&y) {
                out.insert(fy);
            }
        }
    }
    out
}

/// Resolve the set of zone IDs belonging to `county_ids` from the `Zone` table
/// (`zoneID`, `countyID`). Falls back to the MOVES `zoneID = countyID × 10`
/// convention if `Zone` is missing or carries neither column.
fn zone_ids_for_counties(store: &InMemoryStore, county_ids: &BTreeSet<i64>) -> BTreeSet<i64> {
    let fallback = || county_ids.iter().map(|&c| c * 10).collect::<BTreeSet<i64>>();

    let Some(arc) = store.get("Zone") else {
        return fallback();
    };
    let df = &*arc;
    let col = |name: &str| {
        df.columns()
            .iter()
            .find(|c| c.name().eq_ignore_ascii_case(name))
            .and_then(|c| c.cast(&DataType::Int32).ok())
    };
    let (Some(zone_col), Some(county_col)) = (col("zoneID"), col("countyID")) else {
        return fallback();
    };
    let (Ok(zids), Ok(cids)) = (zone_col.i32(), county_col.i32()) else {
        return fallback();
    };
    let mut out: BTreeSet<i64> = BTreeSet::new();
    for i in 0..df.height() {
        if let (Some(z), Some(c)) = (zids.get(i), cids.get(i)) {
            if county_ids.contains(&(c as i64)) {
                out.insert(z as i64);
            }
        }
    }
    if out.is_empty() {
        fallback()
    } else {
        out
    }
}

/// Filter `table` in place to rows whose `id_col` value is in `keep`. No-op if
/// the table or column is absent, if nothing would be dropped, or if the filter
/// would keep zero rows (treated as a convention mismatch: leave the table full
/// so the run stays correct).
fn prune_table_by_id(
    store: &mut InMemoryStore,
    table: &str,
    id_col: &str,
    keep: &BTreeSet<i64>,
) -> Result<(), String> {
    let Some(arc) = store.get(table) else {
        return Ok(());
    };
    let df = (*arc).clone();
    drop(arc);

    let Some(name) = df
        .columns()
        .iter()
        .find(|c| c.name().eq_ignore_ascii_case(id_col))
        .map(|c| c.name().to_string())
    else {
        return Ok(());
    };
    let col = df
        .column(&name)
        .and_then(|c| c.cast(&DataType::Int32))
        .map_err(|e| format!("{table}.{id_col} cast: {e}"))?;
    let ca = col.i32().map_err(|e| format!("{table}.{id_col}: {e}"))?;

    let mut mask: Vec<bool> = Vec::with_capacity(ca.len());
    let mut kept = 0usize;
    for v in ca {
        let b = v.is_some_and(|x| keep.contains(&(x as i64)));
        kept += usize::from(b);
        mask.push(b);
    }
    // Nothing to drop, or a mismatch that would empty the table: leave it.
    if kept == df.height() || kept == 0 {
        return Ok(());
    }
    let mask: BooleanChunked = mask.into_iter().collect();
    let filtered = df
        .filter(&mask)
        .map_err(|e| format!("filtering {table}: {e}"))?;
    store.insert(table.to_string(), filtered);
    Ok(())
}

/// Zero-fill the NULL `marketShare`/`marketShareCV` of the FuelSupply
/// `fuelFormulationID = 0` placeholder row(s) only, in place.
///
/// The default DB ships a single all-zero placeholder row (fuelFormulationID=0)
/// whose market-share columns are NULL and that never joins real data; that row
/// is the only legitimate NULL. A NULL `marketShare` on any *real*
/// (fuelFormulationID != 0) row is a genuine data gap — the native strict
/// per-row extractor (criteria_running_calculator.rs `FuelSupplyRow::extract`
/// errors via `ok_or_else(|| null("marketShare"))`), so we must surface it as an
/// error here rather than coerce it to 0.0 and silently zero out that
/// formulation's blend-weighted contribution. No-op if the table is absent.
/// Uses polars-core only.
pub fn fill_fuel_supply_placeholder_nulls(store: &mut InMemoryStore) -> Result<(), String> {
    const TABLE: &str = "FuelSupply";
    const COLS: &[&str] = &["marketShare", "marketShareCV"];

    let Some(arc) = store.get(TABLE) else {
        return Ok(());
    };
    let mut df = (*arc).clone();
    drop(arc);

    // Locate the fuelFormulationID column so the NULL fill can be restricted to
    // the placeholder row(s). If it is missing we cannot distinguish placeholder
    // from real rows, so leave the data untouched and let the strict extractor
    // decide.
    let ffid_name = df
        .columns()
        .iter()
        .find(|c| c.name().eq_ignore_ascii_case("fuelFormulationID"))
        .map(|c| c.name().to_string());
    let Some(ffid_name) = ffid_name else {
        return Ok(());
    };
    let ffid = df
        .column(&ffid_name)
        .and_then(|c| c.cast(&DataType::Int32))
        .map_err(|e| format!("FuelSupply.fuelFormulationID cast: {e}"))?;
    let ffid = ffid.i32().map_err(|e| format!("{e}"))?.clone();
    let is_placeholder = |i: usize| ffid.get(i) == Some(0);

    let mut changed = false;
    for &want in COLS {
        let actual = df
            .columns()
            .iter()
            .find(|c| c.name().to_ascii_lowercase() == want.to_ascii_lowercase())
            .map(|c| c.name().to_string());
        let Some(name) = actual else { continue };
        let casted = df
            .column(&name)
            .and_then(|c| c.cast(&DataType::Float64))
            .map_err(|e| format!("FuelSupply.{want} cast: {e}"))?;
        let ca = casted.f64().map_err(|e| format!("{e}"))?;
        if ca.null_count() == 0 {
            continue;
        }
        let mut filled: Vec<f64> = Vec::with_capacity(ca.len());
        for i in 0..ca.len() {
            match ca.get(i) {
                Some(v) => filled.push(v),
                // A NULL on a real row is a data gap the native path would
                // surface; only the fuelFormulationID=0 placeholder may be 0.0.
                None if is_placeholder(i) => filled.push(0.0),
                None => {
                    return Err(format!(
                        "FuelSupply.{want} is NULL for fuelFormulationID={} (row {i}): \
                         a real fuel-supply row is missing its market share",
                        ffid.get(i)
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "NULL".to_string()),
                    ));
                }
            }
        }
        let series: Column = Series::new(name.as_str().into(), filled).into();
        if df.with_column(series).is_ok() {
            changed = true;
        }
    }
    if changed {
        store.insert(TABLE.to_string(), df);
    }
    Ok(())
}

/// Port of `FuelEffectsGenerator.setup()`'s high-ethanol (E85/E70) fuel-property
/// transformation (`cloneEthanolFuelsForRegions` +
/// `alterHighEthanolFuelProperties`, steps 005/020/025).
///
/// The default DB ships high-ethanol formulations (`fuelSubtypeID` 51/52) with
/// raw E85 distillation sentinels (`T50=999`, `T90=999`, `ETOHVolume≈74`). The
/// BaseRate general-fuel-ratio THC expression contains
/// `exp(5.58e-5*T50*T50 - 0.0195*T50 + …)`, so `T50=999` explodes the ratio to
/// ~1.67e17 and produces garbage THC. Canonical `FuelEffectsGenerator.setup()`
/// fixes this by overwriting each high-ethanol formulation's *combustion*
/// properties with the matching **E10 base-fuel** values from the
/// `e10FuelProperties` table before any fuel-effect math runs. The captured
/// snapshot/`canonical_snapshot_diff` path already has this baked into its
/// `fuelFormulation`; the default-DB path did not — this synthesises it.
///
/// Steps (citing `FuelEffectsGenerator.java`):
///   - **005** (`cloneEthanolFuelsForRegions`): when one high-ethanol
///     `fuelFormulationID` is used by multiple distinct
///     `(fuelRegionID, fuelYearID, monthGroupID)` usages, each usage after the
///     first must get its own clone so region/month-specific E10 props don't
///     collide. **Cloning is intentionally skipped here** (see below); we apply
///     the per-formulation substitution using the formulation's single usage.
///     If a multi-usage formulation is found, we log via `MOVES_DEBUG_LOAD`
///     rather than silently mis-handle.
///   - **020** (`alterHighEthanolFuelProperties`): for each high-ethanol
///     formulation `(f, region, year, monthGroup)`, set each property to
///     `coalesce(e1.<col>, e0.<col>, ff.<col>)` where
///     `e0 = e10FuelProperties[region=0, year, monthGroup]` (nation) and
///     `e1 = e10FuelProperties[region=usage region, year, monthGroup]` (region;
///     may be absent). `coalesce` uses e1's value if its row exists and the
///     column is non-NULL, else e0's if non-NULL, else keeps the formulation's
///     existing value. Also adds an `altRVP` column (defaulted to `RVP` for all
///     rows) and sets it to `coalesce(e1.RVP, e0.RVP, ff.RVP)` for high-ethanol
///     formulations. The port's `FuelEffectsGenerator` reads `altRVP`.
///   - **025** (`DefaultDataMaker.calculateVolToWtPercentOxy`): recompute
///     `volToWtPercentOxy` over the *whole* table from the (now-altered)
///     oxygenate volumes.
///
/// No-op if any of `FuelFormulation`, `FuelSupply`, `e10FuelProperties` is
/// absent (some paths lack them). Polars-core only (wasm32-safe).
///
/// **Cloning skipped, by design**: single-county / single-month fixtures use
/// each high-ethanol formulationID in exactly one `(region, year, monthGroup)`,
/// so cloning is a no-op for them. Faithfully porting the multi-usage clone in
/// polars-core (insert-new-row + repoint FuelSupply) is non-trivial; for the
/// rare multi-usage case we log and apply the first usage's substitution rather
/// than silently producing region-incorrect props.
pub fn transform_high_ethanol_fuel_properties(
    store: &mut InMemoryStore,
) -> Result<(), String> {
    // The combustion-property columns altered in step 020 (altRVP is handled
    // separately because it is a *new* column sourced from RVP).
    const PROP_COLS: &[&str] = &[
        "sulfurLevel",
        "ETOHVolume",
        "MTBEVolume",
        "ETBEVolume",
        "TAMEVolume",
        "aromaticContent",
        "olefinContent",
        "benzeneContent",
        "e200",
        "e300",
        "BioDieselEsterVolume",
        "CetaneIndex",
        "PAHContent",
        "T50",
        "T90",
    ];

    let (Some(ff_arc), Some(fs_arc), Some(e10_arc)) = (
        store.get("FuelFormulation"),
        store.get("FuelSupply"),
        store.get("e10FuelProperties"),
    ) else {
        return Ok(());
    };
    let mut ff = (*ff_arc).clone();
    let fs = &*fs_arc;
    let e10 = &*e10_arc;
    drop(ff_arc);

    let log = |msg: &str| {
        if std::env::var("MOVES_DEBUG_LOAD").is_ok() {
            use std::io::Write;
            let _ = writeln!(std::io::stderr(), "[synth] high_ethanol_fuel_props: {msg}");
            let _ = std::io::stderr().flush();
        }
    };

    // Case-insensitive column lookup → owned actual name.
    let col_name = |df: &DataFrame, want: &str| -> Option<String> {
        df.get_column_names()
            .iter()
            .find(|c| c.eq_ignore_ascii_case(want))
            .map(|c| c.to_string())
    };
    // Read an Int64 column as a Vec<i64>, erroring on NULL keys.
    let i64_col = |df: &DataFrame, want: &str, ctx: &str| -> Result<Vec<i64>, String> {
        let name = col_name(df, want)
            .ok_or_else(|| format!("{ctx}: column {want} missing"))?;
        let casted = df
            .column(&name)
            .and_then(|c| c.cast(&DataType::Int64))
            .map_err(|e| format!("{ctx}.{want} cast: {e}"))?;
        let ca = casted.i64().map_err(|e| format!("{ctx}.{want}: {e}"))?;
        ca.into_iter()
            .map(|v| v.ok_or_else(|| format!("{ctx}.{want} has a NULL key")))
            .collect()
    };
    // Read a Float64 column as Vec<Option<f64>> (NULLs preserved for coalesce).
    let f64_opt_col = |df: &DataFrame, want: &str, ctx: &str| -> Result<Vec<Option<f64>>, String> {
        let name = col_name(df, want)
            .ok_or_else(|| format!("{ctx}: column {want} missing"))?;
        let casted = df
            .column(&name)
            .and_then(|c| c.cast(&DataType::Float64))
            .map_err(|e| format!("{ctx}.{want} cast: {e}"))?;
        let ca = casted.f64().map_err(|e| format!("{ctx}.{want}: {e}"))?;
        Ok(ca.into_iter().collect())
    };

    // ---- Build the e10FuelProperties lookup, keyed (region, year, monthGroup).
    let e10_region = i64_col(e10, "fuelRegionID", "e10FuelProperties")?;
    let e10_year = i64_col(e10, "fuelYearID", "e10FuelProperties")?;
    let e10_month = i64_col(e10, "monthGroupID", "e10FuelProperties")?;
    // altRVP is sourced from e10's RVP; PROP_COLS map directly.
    let mut e10_cols: BTreeMap<&str, Vec<Option<f64>>> = BTreeMap::new();
    for &c in PROP_COLS {
        e10_cols.insert(c, f64_opt_col(e10, c, "e10FuelProperties")?);
    }
    let e10_rvp = f64_opt_col(e10, "RVP", "e10FuelProperties")?;
    // (region, year, month) -> row index. A duplicate key keeps the first row
    // (canonical relies on uniqueness of (region,year,month) here).
    let mut e10_index: BTreeMap<(i64, i64, i64), usize> = BTreeMap::new();
    for i in 0..e10_region.len() {
        e10_index
            .entry((e10_region[i], e10_year[i], e10_month[i]))
            .or_insert(i);
    }

    // ---- FuelSupply usages, keyed by fuelFormulationID.
    let fs_ffid = i64_col(fs, "fuelFormulationID", "FuelSupply")?;
    let fs_region = i64_col(fs, "fuelRegionID", "FuelSupply")?;
    let fs_year = i64_col(fs, "fuelYearID", "FuelSupply")?;
    let fs_month = i64_col(fs, "monthGroupID", "FuelSupply")?;
    // fuelFormulationID -> set of distinct (region, year, month) usages.
    let mut usages: BTreeMap<i64, BTreeSet<(i64, i64, i64)>> = BTreeMap::new();
    for i in 0..fs_ffid.len() {
        usages
            .entry(fs_ffid[i])
            .or_default()
            .insert((fs_region[i], fs_year[i], fs_month[i]));
    }

    // ---- FuelFormulation columns we mutate.
    let ff_ffid = i64_col(&ff, "fuelFormulationID", "FuelFormulation")?;
    let ff_subtype = i64_col(&ff, "fuelSubtypeID", "FuelFormulation")?;
    let n = ff_ffid.len();

    // Existing FuelFormulation property values (Option for NULL-aware coalesce).
    let mut ff_vals: BTreeMap<&str, Vec<Option<f64>>> = BTreeMap::new();
    for &c in PROP_COLS {
        ff_vals.insert(c, f64_opt_col(&ff, c, "FuelFormulation")?);
    }
    let ff_rvp = f64_opt_col(&ff, "RVP", "FuelFormulation")?;
    // altRVP starts as a copy of RVP for every row (canonical: add column,
    // `update set altRVP=RVP`).
    let mut alt_rvp: Vec<Option<f64>> = ff_rvp.clone();

    // coalesce(e1.col, e0.col, existing)
    let coalesce = |e1: Option<usize>, e0: Option<usize>, src: &[Option<f64>], existing: Option<f64>| -> Option<f64> {
        if let Some(i1) = e1 {
            if let Some(v) = src[i1] {
                return Some(v);
            }
        }
        if let Some(i0) = e0 {
            if let Some(v) = src[i0] {
                return Some(v);
            }
        }
        existing
    };

    // ---- Step 005/020: alter high-ethanol formulations in place.
    let mut altered = 0usize;
    for row in 0..n {
        let subtype = ff_subtype[row];
        if subtype != 51 && subtype != 52 {
            continue;
        }
        let ffid = ff_ffid[row];
        let Some(usage_set) = usages.get(&ffid) else {
            // High-ethanol formulation not referenced by FuelSupply — nothing
            // to key the E10 lookup on, so leave it untouched.
            log(&format!(
                "formulation {ffid} (subtype {subtype}) not used in FuelSupply; left unaltered"
            ));
            continue;
        };
        if usage_set.len() > 1 {
            // cloneEthanolFuelsForRegions would split this into one formulation
            // per usage; we do not clone. Apply the first usage and warn.
            log(&format!(
                "formulation {ffid} has {} distinct (region,year,month) usages; \
                 cloning SKIPPED — applying first usage's E10 props only",
                usage_set.len()
            ));
        }
        let &(region, year, month) = usage_set.iter().next().expect("usage_set non-empty");

        // e0 = nation (region 0), e1 = usage region (may be absent).
        let e0 = e10_index.get(&(0, year, month)).copied();
        let e1 = e10_index.get(&(region, year, month)).copied();
        if e0.is_none() && e1.is_none() {
            log(&format!(
                "formulation {ffid}: no e10FuelProperties row for (year {year}, month {month}); \
                 properties unchanged"
            ));
            continue;
        }

        for &c in PROP_COLS {
            let src = &e10_cols[c];
            let existing = ff_vals[c][row];
            let new = coalesce(e1, e0, src, existing);
            ff_vals.get_mut(c).expect("prop col present")[row] = new;
        }
        alt_rvp[row] = coalesce(e1, e0, &e10_rvp, ff_rvp[row]);
        altered += 1;
    }
    log(&format!("altered {altered} high-ethanol formulation row(s)"));

    // ---- Step 025: recompute volToWtPercentOxy over the whole table from the
    // (now-altered) oxygenate volumes. Denominator <= 0 → 0.
    let etoh = &ff_vals["ETOHVolume"];
    let mtbe = &ff_vals["MTBEVolume"];
    let etbe = &ff_vals["ETBEVolume"];
    let tame = &ff_vals["TAMEVolume"];
    let mut vol_to_wt: Vec<Option<f64>> = Vec::with_capacity(n);
    for row in 0..n {
        let e = etoh[row].unwrap_or(0.0);
        let m = mtbe[row].unwrap_or(0.0);
        let eb = etbe[row].unwrap_or(0.0);
        let t = tame[row].unwrap_or(0.0);
        let denom = e + m + eb + t;
        let v = if denom > 0.0 {
            (e * 0.3653 + m * 0.1792 + eb * 0.1537 + t * 0.1651) / denom
        } else {
            0.0
        };
        vol_to_wt.push(Some(v));
    }

    // ---- Write the altered property columns + altRVP + volToWtPercentOxy back.
    let set_col = |df: &mut DataFrame, want: &str, vals: &[Option<f64>]| -> Result<(), String> {
        let name = col_name(df, want).unwrap_or_else(|| want.to_string());
        let owned: Vec<Option<f64>> = vals.to_vec();
        let s: Series = Series::new(name.as_str().into(), owned);
        df.with_column(Column::from(s))
            .map_err(|e| format!("FuelFormulation.{want} write: {e}"))?;
        Ok(())
    };
    for &c in PROP_COLS {
        set_col(&mut ff, c, &ff_vals[c])?;
    }
    set_col(&mut ff, "volToWtPercentOxy", &vol_to_wt)?;
    // altRVP is a NEW column; with_column adds it if absent.
    set_col(&mut ff, "altRVP", &alt_rvp)?;

    store.insert("FuelFormulation".to_string(), ff);
    Ok(())
}

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

/// Merge process/year-indexed variant tables into their canonical names using
/// `DataFrame::vstack` (polars-core, wasm32-compatible).
///
/// This is the wasm32-safe equivalent of `merge_process_year_variants` in
/// `moves-cli/src/run.rs`, which uses `LazyFrame + concat` (polars-lazy,
/// not available on wasm32).
pub fn merge_store_variants_eager(store: &mut InMemoryStore) -> Result<(), String> {
    let all_names: Vec<String> = store.names().iter().map(|s| s.to_string()).collect();
    let mut by_base: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for name in &all_names {
        let base = strip_numeric_index_suffix(name);
        if base != name.as_str() {
            by_base
                .entry(base.to_string())
                .or_default()
                .push(name.clone());
        }
    }
    for (base, variant_names) in by_base {
        let mut dfs: Vec<DataFrame> = variant_names
            .iter()
            .filter_map(|vname| store.get(vname))
            .filter(|df| df.height() > 0)
            .map(|df| df.as_ref().clone())
            .collect();
        if dfs.is_empty() {
            continue;
        }
        let merged = if dfs.len() == 1 {
            dfs.remove(0)
        } else {
            let mut base_df = dfs[0].clone();
            for df in &dfs[1..] {
                base_df = base_df
                    .vstack(df)
                    .map_err(|e| format!("vstacking {base} variants: {e}"))?;
            }
            base_df
        };
        store.insert(base, merged);
    }
    Ok(())
}

/// Synthesise `Link` from `ZoneRoadType` when `Link` is absent or empty.
///
/// Port of `populate_link_from_zone_road_type` in `moves-cli/src/run.rs`.
/// Uses polars-core only.
pub fn populate_link_from_zone_road_type(store: &mut InMemoryStore) -> Result<(), String> {
    if !store.contains("ZoneRoadType") {
        return Ok(());
    }
    if store.get("link").is_some_and(|df| df.height() > 0) {
        return Ok(());
    }

    let (zone_ids, road_type_ids) = {
        let arc = store
            .get("ZoneRoadType")
            .expect("ZoneRoadType present after contains check");
        let df = &*arc;
        let find = |want: &str| -> Result<polars::prelude::Column, String> {
            let lower = want.to_ascii_lowercase();
            df.columns()
                .iter()
                .find(|c| c.name().to_ascii_lowercase() == lower)
                .cloned()
                .ok_or_else(|| format!("ZoneRoadType column '{want}' not found"))
        };
        let zone_col = find("zoneID")?
            .cast(&DataType::Int32)
            .map_err(|e| format!("ZoneRoadType.zoneID cast: {e}"))?;
        let road_col = find("roadTypeID")?
            .cast(&DataType::Int32)
            .map_err(|e| format!("ZoneRoadType.roadTypeID cast: {e}"))?;
        let zids: Vec<i32> = zone_col
            .i32()
            .map_err(|e| format!("{e}"))?
            .into_no_null_iter()
            .collect();
        let rids: Vec<i32> = road_col
            .i32()
            .map_err(|e| format!("{e}"))?
            .into_no_null_iter()
            .collect();
        (zids, rids)
    };

    let mut seen: BTreeSet<(i32, i32)> = BTreeSet::new();
    let mut link_ids: Vec<i32> = Vec::new();
    let mut county_ids: Vec<i32> = Vec::new();
    let mut out_zone_ids: Vec<i32> = Vec::new();
    let mut out_road_type_ids: Vec<i32> = Vec::new();
    for (&zone_id, &road_type_id) in zone_ids.iter().zip(road_type_ids.iter()) {
        if seen.insert((zone_id, road_type_id)) {
            link_ids.push(zone_id * 10 + road_type_id);
            county_ids.push(zone_id / 10);
            out_zone_ids.push(zone_id);
            out_road_type_ids.push(road_type_id);
        }
    }
    if link_ids.is_empty() {
        return Ok(());
    }

    let n = link_ids.len();
    let df = DataFrame::new(
        n,
        vec![
            Series::new("linkID".into(), link_ids).into(),
            Series::new("countyID".into(), county_ids).into(),
            Series::new("zoneID".into(), out_zone_ids).into(),
            Series::new("roadTypeID".into(), out_road_type_ids).into(),
        ],
    )
    .map_err(|e| format!("building Link DataFrame: {e}"))?;
    store.insert("Link".to_string(), df);
    Ok(())
}

/// Build all `RunSpec*` tables that generators read from the execution-DB slow
/// tier, synthesised from the parsed [`RunSpec`].
///
/// Port of `build_runspec_tables` in `moves-cli/src/run.rs`.
/// Uses polars-core only.
/// Distinct `dayID`s from the store's `DayOfAnyWeek` table (the run's full set
/// of day types). Empty if the table is absent or carries no `dayID` column.
fn day_ids_from_day_of_any_week(store: &InMemoryStore) -> BTreeSet<i32> {
    let Some(arc) = store.get("DayOfAnyWeek") else {
        return BTreeSet::new();
    };
    let Ok(col) = arc.column("dayID").and_then(|c| c.cast(&DataType::Int32)) else {
        return BTreeSet::new();
    };
    let Ok(ca) = col.i32() else {
        return BTreeSet::new();
    };
    ca.into_iter().flatten().collect()
}

pub fn build_runspec_tables(runspec: &RunSpec, store: &mut InMemoryStore) -> Result<(), String> {
    let insert_i32 = |store: &mut InMemoryStore, name: &str, col: &str, vals: Vec<i32>| {
        let n = vals.len();
        let df = DataFrame::new(n, vec![Series::new(col.into(), vals).into()])
            .expect("single-column DataFrame should never fail");
        store.insert(name.to_string(), df);
    };

    // RunSpecSourceType.
    let source_type_ids: Vec<i32> = {
        let mut ids: BTreeSet<i32> = BTreeSet::new();
        for sel in &runspec.onroad_vehicle_selections {
            ids.insert(sel.source_type_id as i32);
        }
        ids.into_iter().collect()
    };
    insert_i32(
        store,
        "RunSpecSourceType",
        "sourceTypeID",
        source_type_ids.clone(),
    );

    // RunSpecPollutantProcess.
    let pol_process_ids: Vec<i32> = {
        let mut ids: BTreeSet<i32> = BTreeSet::new();
        for assoc in &runspec.pollutant_process_associations {
            ids.insert((assoc.pollutant_id * 100 + assoc.process_id) as i32);
        }
        ids.into_iter().collect()
    };
    insert_i32(
        store,
        "RunSpecPollutantProcess",
        "polProcessID",
        pol_process_ids,
    );

    // RunSpecDay. Canonical's execution time span iterates EVERY DayOfAnyWeek
    // day type, not the runspec `<day>` selection (buildExecutionTimeSpan
    // useRunSpec=false) — so RunSpecDay / RunSpecHourDay (and the activity those
    // drive) must cover both weekend (2) and weekday (5). The store's
    // DayOfAnyWeek is the authority (loaded for all day types via the expanded
    // day filter); fall back to the runspec days only when it is absent.
    let day_ids: Vec<i32> = {
        let from_store: BTreeSet<i32> = day_ids_from_day_of_any_week(store);
        if from_store.is_empty() {
            runspec.timespan.days.iter().map(|&d| d as i32).collect()
        } else {
            from_store.into_iter().collect()
        }
    };
    insert_i32(store, "RunSpecDay", "dayID", day_ids.clone());

    // RunSpecHour.
    let hour_ids: Vec<i32> = match (runspec.timespan.begin_hour, runspec.timespan.end_hour) {
        (Some(b), Some(e)) if b <= e => (b..=e).map(|h| h as i32).collect(),
        (Some(h), _) | (_, Some(h)) => vec![h as i32],
        (None, None) => Vec::new(),
    };
    insert_i32(store, "RunSpecHour", "hourID", hour_ids.clone());

    // RunSpecHourDay.
    let hour_day_ids: Vec<i32> = {
        let mut ids: BTreeSet<i32> = BTreeSet::new();
        for &h in &hour_ids {
            for &d in &day_ids {
                ids.insert(h * 10 + d);
            }
        }
        ids.into_iter().collect()
    };
    insert_i32(store, "RunSpecHourDay", "hourDayID", hour_day_ids);

    // RunSpecMonth (months are 1-indexed in MOVES internal representation).
    let month_ids: Vec<i32> = {
        let mut ids: BTreeSet<i32> = BTreeSet::new();
        for &m in &runspec.timespan.months {
            ids.insert(m as i32);
        }
        ids.into_iter().collect()
    };
    insert_i32(store, "RunSpecMonth", "monthID", month_ids.clone());

    // RunSpecYear.
    let year_ids: Vec<i32> = {
        let mut ids: BTreeSet<i32> = BTreeSet::new();
        for &y in &runspec.timespan.years {
            ids.insert(y as i32);
        }
        ids.into_iter().collect()
    };
    insert_i32(store, "RunSpecYear", "yearID", year_ids);

    // RunSpecModelYear: the fleet model years covered by the run — each analysis
    // year minus every age in `AgeCategory` (ages 0..=40). SO2 / SulfatePM filter
    // their per-`modelYearID` rates to this set; canonical MOVES populates
    // `RunSpecModelYear` the same way (the run's years crossed with the age range).
    // Mirrors the `modelYearID = year - ageID` derivation already used by the
    // BaseRate SBWeighted port.
    let model_year_ids: Vec<i32> = {
        let age_ids: Vec<i32> = store
            .get("AgeCategory")
            .and_then(|arc| {
                let df = &*arc;
                let col = df
                    .columns()
                    .iter()
                    .find(|c| c.name().eq_ignore_ascii_case("ageID"))?;
                let casted = col.cast(&DataType::Int32).ok()?;
                let ca = casted.i32().ok()?;
                Some(ca.into_iter().flatten().collect::<Vec<i32>>())
            })
            .unwrap_or_default();
        let mut ids: BTreeSet<i32> = BTreeSet::new();
        for &y in &runspec.timespan.years {
            for &a in &age_ids {
                ids.insert(y as i32 - a);
            }
        }
        ids.into_iter().collect()
    };
    insert_i32(store, "RunSpecModelYear", "modelYearID", model_year_ids);

    // RunSpecRoadType.
    let road_type_ids: Vec<i32> = {
        let mut ids: BTreeSet<i32> = BTreeSet::new();
        for rt in &runspec.road_types {
            ids.insert(rt.road_type_id as i32);
        }
        ids.into_iter().collect()
    };
    insert_i32(store, "RunSpecRoadType", "roadTypeID", road_type_ids);

    // RunSpecMonthGroup: derive from MonthGroupOfAnyYear if present.
    let month_group_ids: Vec<i32> = if store.contains("MonthGroupOfAnyYear") {
        let arc = store
            .get("MonthGroupOfAnyYear")
            .expect("MonthGroupOfAnyYear present after contains check");
        let df = &*arc;
        let find = |want: &str| {
            let lower = want.to_ascii_lowercase();
            df.columns()
                .iter()
                .find(|c| c.name().to_ascii_lowercase() == lower)
                .cloned()
        };
        let mut month_to_group: BTreeMap<i32, i32> = BTreeMap::new();
        if let (Some(mid_col), Some(mgid_col)) = (find("monthID"), find("monthGroupID")) {
            let mids = mid_col
                .cast(&DataType::Int32)
                .ok()
                .and_then(|c| c.i32().ok().cloned());
            let mgids = mgid_col
                .cast(&DataType::Int32)
                .ok()
                .and_then(|c| c.i32().ok().cloned());
            if let (Some(mids), Some(mgids)) = (mids, mgids) {
                for i in 0..df.height() {
                    if let (Some(mid), Some(mgid)) = (mids.get(i), mgids.get(i)) {
                        month_to_group.insert(mid, mgid);
                    }
                }
            }
        }
        let mut groups: BTreeSet<i32> = BTreeSet::new();
        for &m in &month_ids {
            groups.insert(*month_to_group.get(&m).unwrap_or(&m));
        }
        groups.into_iter().collect()
    } else {
        month_ids.clone()
    };
    insert_i32(store, "RunSpecMonthGroup", "monthGroupID", month_group_ids);

    // RunSpecSourceFuelType (Int64 pairs per SourceBinDistributionGenerator schema).
    //
    // Canonical does NOT restrict to the per-selection fuelType. A runspec
    // `onroadvehicleselection` names one (sourceType, fuelType), but MOVES runs
    // the selected source type's WHOLE fleet fuel mix: the GUI's
    // `loadValidFuelSourceCombinations`
    // (gui/OnRoadVehicleEquipment.java) populates the run from
    // `FuelType ⋈ SourceUseType ⋈ FuelEngTechAssoc`, so the captured execution
    // `RunSpecSourceFuelType` holds e.g. (21,{1,2,5,9}) for a single (21,1)
    // selection. Mirror that: expand each SELECTED sourceType to all
    // `(sourceTypeID, fuelTypeID)` pairs FuelEngTechAssoc lists for it.
    // Restricting to the literal selection fuel (the prior behaviour) dropped
    // every non-selected fuel bin's activity — a uniform under-emission across
    // all default-DB criteria fixtures. Falls back to the raw selection pairs
    // only if FuelEngTechAssoc is absent (no default-DB load).
    let selected_source_types: BTreeSet<i64> = runspec
        .onroad_vehicle_selections
        .iter()
        .map(|sel| sel.source_type_id as i64)
        .collect();
    let source_fuel_pairs: Vec<(i64, i64)> = {
        let mut pairs: BTreeSet<(i64, i64)> = BTreeSet::new();
        let from_feta = store.get("FuelEngTechAssoc").and_then(|arc| {
            let st = arc
                .column("sourceTypeID")
                .and_then(|c| c.cast(&DataType::Int64))
                .ok()?;
            let ft = arc
                .column("fuelTypeID")
                .and_then(|c| c.cast(&DataType::Int64))
                .ok()?;
            let st = st.i64().ok()?.clone();
            let ft = ft.i64().ok()?.clone();
            let mut found = false;
            for i in 0..arc.height() {
                if let (Some(s), Some(f)) = (st.get(i), ft.get(i)) {
                    if selected_source_types.contains(&s) {
                        pairs.insert((s, f));
                        found = true;
                    }
                }
            }
            // Only treat FuelEngTechAssoc as authoritative when it actually
            // covered the selected source types; an empty/irrelevant table
            // falls through to the literal-selection pairs below.
            found.then_some(())
        });
        if from_feta.is_none() {
            for sel in &runspec.onroad_vehicle_selections {
                pairs.insert((sel.source_type_id as i64, sel.fuel_type_id as i64));
            }
        }
        pairs.into_iter().collect()
    };
    let (sf_source_ids, sf_fuel_ids): (Vec<i64>, Vec<i64>) = source_fuel_pairs.into_iter().unzip();
    let n = sf_source_ids.len();
    let sf_df = DataFrame::new(
        n,
        vec![
            Series::new("sourceTypeID".into(), sf_source_ids).into(),
            Series::new("fuelTypeID".into(), sf_fuel_ids).into(),
        ],
    )
    .map_err(|e| format!("building RunSpecSourceFuelType: {e}"))?;
    store.insert("RunSpecSourceFuelType".to_string(), sf_df);

    Ok(())
}

/// Synthesise `PollutantProcessMappedModelYear` from `PollutantProcessModelYear`.
///
/// MOVES builds this table during execution-DB setup by mapping each
/// `(polProcessID, modelYearID)` through `modelYearMapping` (a user→standard
/// model-year remap). The default DB ships an empty `modelYearMapping`, so the
/// mapping is the identity and the result is a direct projection of
/// `PollutantProcessModelYear`'s `(polProcessID, modelYearID, IMModelYearGroupID)`
/// columns. Calculators (BaseRate, criteria, NOx, …) read this table to expand
/// per-pollutant-process ratios across model years; without it they fail with
/// "table 'PollutantProcessMappedModelYear' not found in store".
///
/// No-op when the table already exists or the source table is absent. Uses
/// polars-core only (wasm32-compatible).
pub fn populate_pollutant_process_mapped_model_year(store: &mut InMemoryStore) -> Result<(), String> {
    if store.contains("PollutantProcessMappedModelYear")
        || !store.contains("PollutantProcessModelYear")
    {
        return Ok(());
    }

    // With an identity model-year mapping the mapped table carries exactly the
    // source table's columns (polProcessID, modelYearID, modelYearGroupID,
    // fuelMYGroupID, IMModelYearGroupID) — different calculators read different
    // subsets — so copy the source wholesale under the mapped name.
    let mapped: DataFrame = (*store
        .get("PollutantProcessModelYear")
        .expect("present after contains check"))
    .clone();
    store.insert("PollutantProcessMappedModelYear".to_string(), mapped);
    Ok(())
}

/// Synthesise `sourceUseTypePhysicsMapping` from `sourceUseTypePhysics` when
/// the table is absent.
///
/// Port of `populate_source_use_type_physics_mapping` in `moves-cli/src/run.rs`.
pub fn populate_source_use_type_physics_mapping(store: &mut InMemoryStore) -> Result<(), String> {
    if store.contains("sourceUseTypePhysicsMapping") || !store.contains("sourceUseTypePhysics") {
        return Ok(());
    }

    let physics = store
        .get("sourceUseTypePhysics")
        .expect("present after contains check");
    let mut mapping: DataFrame = (*physics).clone();
    drop(physics);

    let src_col = mapping
        .get_column_names()
        .iter()
        .find(|n| n.as_str().eq_ignore_ascii_case("sourceTypeID"))
        .map(|n| n.to_string())
        .ok_or("sourceUseTypePhysics has no sourceTypeID column")?;
    mapping
        .rename(&src_col, "realSourceTypeID".into())
        .map_err(|e| format!("renaming sourceTypeID → realSourceTypeID: {e}"))?;

    let mut temp = mapping
        .column("realSourceTypeID")
        .map_err(|e| format!("{e}"))?
        .clone();
    temp.rename("tempSourceTypeID".into());
    let n = mapping.height();
    mapping
        .with_column(temp)
        .map_err(|e| format!("adding tempSourceTypeID: {e}"))?;
    mapping
        .with_column(Series::new("opModeIDOffset".into(), vec![0i64; n]).into())
        .map_err(|e| format!("adding opModeIDOffset: {e}"))?;

    store.insert("sourceUseTypePhysicsMapping".to_string(), mapping);
    Ok(())
}

/// Fill derived `ZoneMonthHour` meteorology columns from `temperature` and
/// `relHumidity`, when those derived columns are NULL in the store.
///
/// Port of `populate_zone_month_hour_meteorology` in `moves-cli/src/run.rs`.
/// Uses `build_meteorology_table` from the `meteorology` generator.
pub fn populate_zone_month_hour_meteorology(store: &mut InMemoryStore) -> Result<(), String> {
    if !store.contains("ZoneMonthHour") {
        return Ok(());
    }

    // Early exit if heatIndex is already populated.
    {
        let zmh = store
            .get("ZoneMonthHour")
            .expect("ZoneMonthHour not in store after contains check");
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

    if !store.contains("Zone") || !store.contains("County") {
        return Ok(());
    }

    let inputs = MeteorologyInputs {
        zone_month_hour: store
            .iter_typed("ZoneMonthHour")
            .map_err(|e| format!("reading ZoneMonthHour: {e}"))?,
        zone: store
            .iter_typed("Zone")
            .map_err(|e| format!("reading Zone: {e}"))?,
        county: store
            .iter_typed("County")
            .map_err(|e| format!("reading County: {e}"))?,
    };
    let computed = build_meteorology_table(&inputs);

    let mut by_key: std::collections::HashMap<(i32, i32, i32), (f64, f64, f64)> =
        std::collections::HashMap::with_capacity(computed.len());
    for r in &computed {
        by_key.insert(
            (r.zone_id, r.month_id, r.hour_id),
            (r.heat_index, r.specific_humidity, r.mol_water_fraction),
        );
    }

    // Re-read ZoneMonthHour and annotate with computed columns.
    let zmh_arc = store
        .get("ZoneMonthHour")
        .expect("ZoneMonthHour present after contains check");
    let zmh = &*zmh_arc;

    let find = |want: &str| -> Result<polars::prelude::Column, String> {
        let lower = want.to_ascii_lowercase();
        zmh.columns()
            .iter()
            .find(|c| c.name().to_ascii_lowercase() == lower)
            .cloned()
            .ok_or_else(|| format!("ZoneMonthHour column '{want}' not found"))
    };
    let zone_ids_col = find("zoneID")?
        .cast(&DataType::Int32)
        .map_err(|e| format!("zoneID cast: {e}"))?;
    let month_ids_col = find("monthID")?
        .cast(&DataType::Int32)
        .map_err(|e| format!("monthID cast: {e}"))?;
    let hour_ids_col = find("hourID")?
        .cast(&DataType::Int32)
        .map_err(|e| format!("hourID cast: {e}"))?;

    // temperature is the heatIndex fallback for unmatched rows. Canonical MOVES
    // (MeteorologyGenerator.java:151-156) sets `heatIndex = temperature` when
    // temperature < 78F (the no-humidity-polynomial path), so an unmatched
    // ZoneMonthHour row must inherit its own ambient temperature, NOT 0.0.
    // (matches the CLI port: moves-cli/src/run.rs uses `heat.push(temps[i])`.)
    let temps_col = find("temperature")?
        .cast(&DataType::Float64)
        .map_err(|e| format!("temperature cast: {e}"))?;
    let temps_ca = temps_col.f64().map_err(|e| format!("{e}"))?;

    let zids = zone_ids_col.i32().map_err(|e| format!("{e}"))?;
    let mids = month_ids_col.i32().map_err(|e| format!("{e}"))?;
    let hids = hour_ids_col.i32().map_err(|e| format!("{e}"))?;
    let n = zmh.height();

    let mut heat_index: Vec<f64> = Vec::with_capacity(n);
    let mut specific_humidity: Vec<f64> = Vec::with_capacity(n);
    let mut mol_water_fraction: Vec<f64> = Vec::with_capacity(n);

    for i in 0..n {
        let key = (
            zids.get(i).unwrap_or(0),
            mids.get(i).unwrap_or(0),
            hids.get(i).unwrap_or(0),
        );
        match by_key.get(&key).copied() {
            Some((hi, sh, mwf)) => {
                heat_index.push(hi);
                specific_humidity.push(sh);
                mol_water_fraction.push(mwf);
            }
            None => {
                heat_index.push(temps_ca.get(i).unwrap_or(0.0));
                specific_humidity.push(0.0);
                mol_water_fraction.push(0.0);
            }
        }
    }

    let mut updated = zmh.clone();
    drop(zmh_arc);

    updated
        .with_column(Series::new("heatIndex".into(), heat_index).into())
        .map_err(|e| format!("writing heatIndex: {e}"))?;
    updated
        .with_column(Series::new("specificHumidity".into(), specific_humidity).into())
        .map_err(|e| format!("writing specificHumidity: {e}"))?;
    updated
        .with_column(Series::new("molWaterFraction".into(), mol_water_fraction).into())
        .map_err(|e| format!("writing molWaterFraction: {e}"))?;
    store.insert("ZoneMonthHour".to_string(), updated);
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------


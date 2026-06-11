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
    merge_store_variants_eager(store)?;
    // Several geography-keyed tables ship as a single unpartitioned national
    // file (ZoneMonthHour alone is ~930k rows). A county-scoped onroad run only
    // ever reads its own county/zone, so pruning the rest up front shrinks the
    // meteorology synthesis below and every downstream calculator's
    // ZoneMonthHour scan — the WASM analogue of the gate's zone-filter. Must run
    // before populate_zone_month_hour_meteorology so the synthesis sees the
    // already-pruned table.
    prune_geographic_tables_to_runspec(runspec, store)?;
    populate_source_use_type_physics_mapping(store)?;
    populate_pollutant_process_mapped_model_year(store)?;
    populate_zone_month_hour_meteorology(store)?;
    populate_link_from_zone_road_type(store)?;
    // The default DB ships an all-zero "placeholder" row (fuelFormulationID=0)
    // in FuelSupply with NULL market-share columns. Real fuel supplies always
    // carry a value; the placeholder never joins real data, so fill its NULLs
    // with 0.0 rather than have the strict per-row extractors error on it.
    fill_fuel_supply_placeholder_nulls(store)?;
    build_runspec_tables(runspec, store)?;
    // PollutantProcessModelYear ships as the full national table (~15.6k rows);
    // MOVES's execution-DB copy is run-scoped. Several calculators iterate it
    // (notably BasicRunningPmEmissionCalculator's fuel_supply_adjustment), so
    // scope it to the run's pol-process pairs from RunSpecPollutantProcess (just
    // synthesised by build_runspec_tables). Every PollutantProcessModelYear
    // reader is a primary calculator processing the run's pp pairs, so this
    // drops only rows no calculator reads.
    scope_pollutant_process_model_year_to_runspec(store)?;
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
    // (countyID → regionID) and prune FuelSupply to them.
    let region_ids = fuel_region_ids_for_counties(store, &county_ids);
    if !region_ids.is_empty() {
        prune_table_by_id(store, "FuelSupply", "fuelRegionID", &region_ids)?;
    }
    Ok(())
}

/// Resolve the fuel-region IDs serving `county_ids`, from `regionCounty`
/// (`countyID`, `regionID`). Returns empty if the table or its columns are
/// absent, in which case the caller leaves `FuelSupply` unpruned.
fn fuel_region_ids_for_counties(
    store: &InMemoryStore,
    county_ids: &BTreeSet<i64>,
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
    let mut out: BTreeSet<i64> = BTreeSet::new();
    for i in 0..df.height() {
        if let (Some(r), Some(c)) = (rids.get(i), cids.get(i)) {
            if county_ids.contains(&c) {
                out.insert(r);
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

    // RunSpecDay.
    let day_ids: Vec<i32> = {
        let mut ids: BTreeSet<i32> = BTreeSet::new();
        for &d in &runspec.timespan.days {
            ids.insert(d as i32);
        }
        ids.into_iter().collect()
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
    let source_fuel_pairs: Vec<(i64, i64)> = {
        let mut pairs: BTreeSet<(i64, i64)> = BTreeSet::new();
        for sel in &runspec.onroad_vehicle_selections {
            pairs.insert((sel.source_type_id as i64, sel.fuel_type_id as i64));
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


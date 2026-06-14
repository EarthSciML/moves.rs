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
    synth_step!(
        "merge_store_variants_eager",
        merge_store_variants_eager(store)
    );
    synth_step!(
        "build_ev_sales_fraction",
        build_ev_sales_fraction(runspec, store)
    );
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
    synth_step!(
        "link_from_zone_road_type",
        populate_link_from_zone_road_type(store)
    );
    synth_step!(
        "fill_fuel_supply",
        fill_fuel_supply_placeholder_nulls(store)
    );
    synth_step!(
        "high_ethanol_fuel_props",
        transform_high_ethanol_fuel_properties(store)
    );
    synth_step!("build_runspec_tables", build_runspec_tables(runspec, store));
    synth_step!(
        "preaggregate_activity_to_month",
        preaggregate_activity_to_month(runspec, store)
    );
    synth_step!(
        "build_regclass_source_type_fraction",
        build_regclass_source_type_fraction(store)
    );
    synth_step!(
        "scope_pollutant_process_model_year",
        scope_pollutant_process_model_year_to_runspec(store)
    );
    synth_step!(
        "prune_integrated_species",
        prune_integrated_species_to_run_mechanisms(store)
    );
    synth_step!("build_criteria_ratio", build_criteria_ratio(store));
    synth_step!("build_alt_criteria_ratio", build_alt_criteria_ratio(store));
    synth_step!("build_at_ratio", build_at_ratio(store));
    synth_step!("build_runspec_chained_to", build_runspec_chained_to(store));
    Ok(())
}

/// Prune `integratedSpeciesSet` to the chemical mechanisms whose mechanism
/// pseudo-pollutant the run actually selected — the data-plane equivalent of
/// `TOGSpeciationCalculator.doExecute`'s `if(mechanismIDs.length()<=0) return null`
/// gate, which yields **zero** `NonHAPTOG` (pollutant 88) output.
///
/// Canonical builds its `##mechanismIDs##` filter from the run's selected
/// 'Mechanisms'-display-group pollutants and narrows the `integratedSpeciesSet`
/// extract to them. The captured snapshot ships `integratedSpeciesSet` already
/// filtered (empty for a run that selects no mechanism), but the default DB
/// ships the full table — so without this prune `TOGSpeciationCalculator` finds
/// integrated species to fan out, has nothing to subtract for a non-mechanism
/// run, and emits a spurious `NonHAPTOG = NMOG` residual. The default-DB
/// `chain-nonhaptog` fixture over-emitted exactly 208 pol-88 rows this way
/// (canonical emits none).
///
/// A mechanism `m`'s pollutant has databaseKey `1000 + (m-1)*500` (inverting
/// canonical's `mechanismID = 1 + (databaseKey-1000)/500`); the mechanism is
/// kept iff that pollutant is in the run's pollutant/process set. The result
/// may legitimately be empty — unlike [`prune_table_by_id`], which refuses to
/// empty a table.
fn prune_integrated_species_to_run_mechanisms(store: &mut InMemoryStore) -> Result<(), String> {
    let Some(iss_arc) = store.get("integratedSpeciesSet") else {
        return Ok(());
    };
    if iss_arc.height() == 0 {
        return Ok(());
    }
    let iss = (*iss_arc).clone();
    drop(iss_arc);

    // Selected pollutants — pollutantID = polProcessID / 100. (The default-DB
    // path synthesises `RunSpecPollutantProcess`, not `RunSpecPollutant`.)
    let mut selected: BTreeSet<i64> = BTreeSet::new();
    if let Some(arc) = store.get("RunSpecPollutantProcess") {
        if let Some(col) = arc
            .columns()
            .iter()
            .find(|c| c.name().eq_ignore_ascii_case("polProcessID"))
        {
            let casted = col
                .cast(&DataType::Int64)
                .map_err(|e| format!("RunSpecPollutantProcess.polProcessID cast: {e}"))?;
            let ca = casted
                .i64()
                .map_err(|e| format!("RunSpecPollutantProcess.polProcessID: {e}"))?;
            for v in ca.into_iter().flatten() {
                selected.insert(v / 100);
            }
        }
    }

    let Some(mech_name) = iss
        .columns()
        .iter()
        .find(|c| c.name().eq_ignore_ascii_case("mechanismID"))
        .map(|c| c.name().to_string())
    else {
        return Ok(());
    };
    let casted = iss
        .column(&mech_name)
        .and_then(|c| c.cast(&DataType::Int32))
        .map_err(|e| format!("integratedSpeciesSet.mechanismID cast: {e}"))?;
    let ca = casted
        .i32()
        .map_err(|e| format!("integratedSpeciesSet.mechanismID: {e}"))?;

    let mut mask: Vec<bool> = Vec::with_capacity(ca.len());
    for v in ca {
        let keep = v.is_some_and(|m| {
            let mech_pollutant = 1000 + (i64::from(m) - 1) * 500;
            selected.contains(&mech_pollutant)
        });
        mask.push(keep);
    }
    // Already complete? Skip the rebuild. (An all-keep mask is a no-op.)
    if mask.iter().all(|&b| b) {
        return Ok(());
    }
    let mask: BooleanChunked = mask.into_iter().collect();
    let filtered = iss
        .filter(&mask)
        .map_err(|e| format!("filtering integratedSpeciesSet: {e}"))?;
    store.insert("integratedSpeciesSet".to_string(), filtered);
    Ok(())
}

/// Build the MY ≤ 2000 gasoline `criteriaRatio` table (the EPA Complex + sulfur
/// fuel-effects model) for the default-DB path.
///
/// `generalFuelRatioExpression` ships the criteria (THC/CO/NOx running+start)
/// fuel effects only for model years ≥ 2001; for MY ≤ 2000 the canonical
/// `FuelEffectsGenerator` runs the live Complex Model + sulfur model into
/// `criteriaRatio`. Without it a default-DB run applies no criteria fuel
/// reduction for MY ≤ 2000 and over-emits (NOx ~1.27×, THC ~1.21×, CO ~1.03×).
///
/// No-op when `criteriaRatio` is already populated (it is execution-time derived
/// and the default DB ships it empty, but guard anyway) or when the
/// complex-model input tables are absent. The MY ≥ 2001 criteria effects stay in
/// `generalFuelRatio`; the `FuelEffectsGenerator` drops only the
/// model-year-overlapping criteria rows, so the two tables apply each effect
/// exactly once. See [`crate::generators::fueleffectsgenerator::criteria`].
fn build_criteria_ratio(store: &mut InMemoryStore) -> Result<(), String> {
    use crate::generators::fueleffectsgenerator::criteria;

    let rows = criteria::build_criteria_ratio_rows(store);
    if rows.is_empty() {
        return Ok(());
    }
    let n = rows.len();
    let icol = |name: &str, f: &dyn Fn(&criteria::CriteriaRatioOutRow) -> i32| -> Column {
        Series::new(name.into(), rows.iter().map(f).collect::<Vec<i32>>()).into()
    };
    let fcol = |name: &str, f: &dyn Fn(&criteria::CriteriaRatioOutRow) -> f64| -> Column {
        Series::new(name.into(), rows.iter().map(f).collect::<Vec<f64>>()).into()
    };
    let df = DataFrame::new(
        n,
        vec![
            icol("fuelTypeID", &|r| r.fuel_type_id),
            icol("fuelFormulationID", &|r| r.fuel_formulation_id),
            icol("polProcessID", &|r| r.pol_process_id),
            icol("pollutantID", &|r| r.pollutant_id),
            icol("processID", &|r| r.process_id),
            icol("sourceTypeID", &|r| r.source_type_id),
            icol("modelYearID", &|r| r.model_year_id),
            icol("ageID", &|r| r.age_id),
            fcol("ratio", &|r| r.ratio),
            fcol("ratioGPA", &|r| r.ratio_gpa),
            fcol("ratioNoSulfur", &|r| r.ratio_no_sulfur),
        ],
    )
    .map_err(|e| format!("building criteriaRatio: {e}"))?;
    store.insert("criteriaRatio".to_string(), df);
    Ok(())
}

/// Build the default-DB `altCriteriaRatio` table — the E85 "alternate" (E10-RVP)
/// THC fuel-effect ratios the `HCSpeciationCalculator` needs to speciate ethanol
/// E70/E85 2001+ running/start NMOG and VOC.
///
/// Without it the `BaseRateCalculator`'s `build_e85_block` finds no
/// `altCriteriaRatio` row, never emits the `altTHC` (10001) tally, and HC
/// speciation produces no NMOG (pollutant 80) for E85 model years ≥ 2001 — the
/// default-DB `chain-tog-speciation` fixture was short exactly those rows.
///
/// Must run after `high_ethanol_fuel_props` (which derives the `altRVP` column
/// the pseudo-THC expressions reference). No-op when `altCriteriaRatio` is
/// already populated or the inputs are absent. See
/// [`crate::generators::fueleffectsgenerator::criteria::build_alt_criteria_ratio_rows`].
fn build_alt_criteria_ratio(store: &mut InMemoryStore) -> Result<(), String> {
    use crate::generators::fueleffectsgenerator::criteria;

    let rows = criteria::build_alt_criteria_ratio_rows(store);
    if rows.is_empty() {
        return Ok(());
    }
    let n = rows.len();
    let icol = |name: &str, f: &dyn Fn(&criteria::CriteriaRatioOutRow) -> i32| -> Column {
        Series::new(name.into(), rows.iter().map(f).collect::<Vec<i32>>()).into()
    };
    let fcol = |name: &str, f: &dyn Fn(&criteria::CriteriaRatioOutRow) -> f64| -> Column {
        Series::new(name.into(), rows.iter().map(f).collect::<Vec<f64>>()).into()
    };
    let df = DataFrame::new(
        n,
        vec![
            icol("fuelTypeID", &|r| r.fuel_type_id),
            icol("fuelFormulationID", &|r| r.fuel_formulation_id),
            icol("polProcessID", &|r| r.pol_process_id),
            icol("pollutantID", &|r| r.pollutant_id),
            icol("processID", &|r| r.process_id),
            icol("sourceTypeID", &|r| r.source_type_id),
            icol("modelYearID", &|r| r.model_year_id),
            icol("ageID", &|r| r.age_id),
            fcol("ratio", &|r| r.ratio),
            fcol("ratioGPA", &|r| r.ratio_gpa),
            fcol("ratioNoSulfur", &|r| r.ratio_no_sulfur),
        ],
    )
    .map_err(|e| format!("building altCriteriaRatio: {e}"))?;
    store.insert("altCriteriaRatio".to_string(), df);
    Ok(())
}

/// Build the default-DB `ATRatio` (gaseous air-toxics-to-VOC ratio) table — the
/// runtime output of `FuelEffectsGenerator.doAirToxicsCalculations` +
/// `copyAirToxicsToATRatio`, which the default DB ships empty. Without it a
/// default-DB run emits **no** gaseous air toxics (Benzene/1,3-Butadiene/
/// Formaldehyde, pollutants 20/24/25), since `AirToxicsCalculator` scales them
/// from VOC via `ATRatio`.
///
/// No-op when `ATRatio` is already populated (snapshot/onroad path ships it
/// captured) or the model inputs are absent (nonroad / non-toxics runs).
/// Delegates to
/// [`crate::generators::fueleffectsgenerator::criteria::build_at_ratio_rows`].
fn build_at_ratio(store: &mut InMemoryStore) -> Result<(), String> {
    use crate::generators::fueleffectsgenerator::criteria;

    let rows = criteria::build_at_ratio_rows(store);
    if rows.is_empty() {
        return Ok(());
    }
    let n = rows.len();
    let icol = |name: &str, f: &dyn Fn(&criteria::AtRatioOutRow) -> i32| -> Column {
        Series::new(name.into(), rows.iter().map(f).collect::<Vec<i32>>()).into()
    };
    let fcol = |name: &str, f: &dyn Fn(&criteria::AtRatioOutRow) -> f64| -> Column {
        Series::new(name.into(), rows.iter().map(f).collect::<Vec<f64>>()).into()
    };
    let df = DataFrame::new(
        n,
        vec![
            icol("fuelTypeID", &|r| r.fuel_type_id),
            icol("fuelFormulationID", &|r| r.fuel_formulation_id),
            icol("polProcessID", &|r| r.pol_process_id),
            icol("minModelYearID", &|r| r.min_model_year_id),
            icol("maxModelYearID", &|r| r.max_model_year_id),
            icol("ageID", &|r| r.age_id),
            icol("monthGroupID", &|r| r.month_group_id),
            fcol("atRatio", &|r| r.at_ratio),
        ],
    )
    .map_err(|e| format!("building ATRatio: {e}"))?;
    store.insert("ATRatio".to_string(), df);
    Ok(())
}

/// Build the default-DB `RunSpecChainedTo` table — the chained-calculator
/// input→output map (`ExecutionRunSpec.buildRunSpecFilterTables`). The default
/// DB ships it empty, so a default-DB run never tells the chained calculators
/// (air toxics, NMOG/VOC speciation, …) which input pollutant feeds which
/// output, and they emit nothing.
///
/// Canonical builds it from the *execution* (run-scoped) `PollutantProcessAssoc`
/// self-joined on `chainedTo1 / chainedTo2`. The default-DB `PollutantProcessAssoc`
/// is the full table, so both the output and the input pol-process are scoped
/// here to the run's `RunSpecPollutantProcess`, reproducing the execution DB's
/// pre-scoped join.
///
/// No-op when `RunSpecChainedTo` is already populated (snapshot/onroad path).
fn build_runspec_chained_to(store: &mut InMemoryStore) -> Result<(), String> {
    if store
        .get("RunSpecChainedTo")
        .is_some_and(|df| df.height() > 0)
    {
        return Ok(());
    }
    let Some(ppa) = store.get("PollutantProcessAssoc") else {
        return Ok(());
    };
    let read = |df: &DataFrame, name: &str| -> Result<Vec<Option<i32>>, String> {
        let col = df
            .column(name)
            .and_then(|c| c.cast(&DataType::Int32))
            .map_err(|e| format!("PollutantProcessAssoc.{name}: {e}"))?;
        Ok(col
            .i32()
            .map_err(|e| format!("PollutantProcessAssoc.{name}: {e}"))?
            .into_iter()
            .collect())
    };
    let pp = read(&ppa, "polProcessID")?;
    let pol = read(&ppa, "pollutantID")?;
    let proc = read(&ppa, "processID")?;
    let c1 = read(&ppa, "chainedto1")?;
    let c2 = read(&ppa, "chainedto2")?;

    // Run-scoped pol-processes; without them we cannot reproduce the execution
    // DB's pre-scoped join, so emit nothing rather than the full default chain.
    let Some(rspp) = store.get("RunSpecPollutantProcess") else {
        return Ok(());
    };
    let selected: BTreeSet<i32> = rspp
        .column("polProcessID")
        .and_then(|c| c.cast(&DataType::Int32))
        .map_err(|e| format!("RunSpecPollutantProcess.polProcessID: {e}"))?
        .i32()
        .map_err(|e| format!("RunSpecPollutantProcess.polProcessID: {e}"))?
        .into_iter()
        .flatten()
        .collect();

    // polProcessID → (pollutantID, processID) for the selected, scoped set.
    let mut info: BTreeMap<i32, (i32, i32)> = BTreeMap::new();
    for i in 0..pp.len() {
        if let (Some(p), Some(po), Some(pr)) = (pp[i], pol[i], proc[i]) {
            if selected.contains(&p) {
                info.insert(p, (po, pr));
            }
        }
    }

    let (mut out_pp, mut out_pol, mut out_proc) = (Vec::new(), Vec::new(), Vec::new());
    let (mut in_pp, mut in_pol, mut in_proc) = (Vec::new(), Vec::new(), Vec::new());
    for i in 0..pp.len() {
        let Some(opp) = pp[i] else { continue };
        let (Some(&(opol, oproc)), true) = (info.get(&opp), selected.contains(&opp)) else {
            continue;
        };
        for chained in [c1[i], c2[i]].into_iter().flatten() {
            // chainedTo 0 / absent → no chain link.
            let Some(&(ipol, iproc)) = info.get(&chained) else {
                continue;
            };
            out_pp.push(opp);
            out_pol.push(opol);
            out_proc.push(oproc);
            in_pp.push(chained);
            in_pol.push(ipol);
            in_proc.push(iproc);
        }
    }
    let n = out_pp.len();
    let df = DataFrame::new(
        n,
        vec![
            Series::new("outputPolProcessID".into(), out_pp).into(),
            Series::new("outputPollutantID".into(), out_pol).into(),
            Series::new("outputProcessID".into(), out_proc).into(),
            Series::new("inputPolProcessID".into(), in_pp).into(),
            Series::new("inputPollutantID".into(), in_pol).into(),
            Series::new("inputProcessID".into(), in_proc).into(),
        ],
    )
    .map_err(|e| format!("building RunSpecChainedTo: {e}"))?;
    store.insert("RunSpecChainedTo".to_string(), df);
    Ok(())
}

/// Read an `Int32`-castable column from a store table into a `Vec<Option<i32>>`.
fn col_i32(df: &DataFrame, name: &str) -> Result<Vec<Option<i32>>, String> {
    Ok(df
        .column(name)
        .and_then(|c| c.cast(&DataType::Int32))
        .map_err(|e| format!("{name}: {e}"))?
        .i32()
        .map_err(|e| format!("{name}: {e}"))?
        .into_iter()
        .collect())
}

/// Read a `Float64`-castable column from a store table into a `Vec<Option<f64>>`.
fn col_f64(df: &DataFrame, name: &str) -> Result<Vec<Option<f64>>, String> {
    Ok(df
        .column(name)
        .and_then(|c| c.cast(&DataType::Float64))
        .map_err(|e| format!("{name}: {e}"))?
        .f64()
        .map_err(|e| format!("{name}: {e}"))?
        .into_iter()
        .collect())
}

/// Preaggregate the activity tables to a single monthly time cell when the
/// runspec's `aggregateBy = Month` — the default-DB port of canonical
/// `InputDataManager.preAggregateExecutionDB` (`database/PreAggDAY.sql` +
/// `PreAggMONTH.sql`).
///
/// With `aggregateBy = Month` the canonical execution DB collapses the hour and
/// day dimensions of the activity tables to a single `(hourDayID, dayID, hourID)
/// = (0,0,0)` "Entire Day / Whole Week" cell *before* the rate calc, so the full
/// month's activity lands in one row. The default-DB path never ran that step,
/// so `TotalActivityGenerator` kept hour/day resolution and emitted per-(dayType,
/// selected-hour) rows at ~1/15 of the monthly total. The snapshot path is
/// unaffected (it ships the already-collapsed tables — this synth reproduces
/// them), and every other fixture is unaffected (no other run sets
/// `aggregateBy = Month`).
///
/// Collapses: `HourDay`, `HourOfAnyDay`, `DayOfAnyWeek`, `HourVMTFraction`
/// (→1.0), `DayVMTFraction` (→1.0) and `AvgSpeedDistribution` (activity-weighted
/// over the run's day types). The weight mirrors the canonical two-stage
/// `HourWeighting × DayWeighting`: `W(st,rt,day,hour) = hourVMTFraction ·
/// (Σ_month dayVMTFraction·noOfRealDays·monthVMTFraction / Σ_month
/// monthVMTFraction)`.
fn preaggregate_activity_to_month(
    runspec: &RunSpec,
    store: &mut InMemoryStore,
) -> Result<(), String> {
    if runspec.timespan.aggregate_by.as_deref() != Some("Month") {
        return Ok(());
    }
    // Only collapse if the time dimension is still expanded (default-DB path).
    // The snapshot path ships HourDay already collapsed to the single (0,0,0)
    // cell, so skip there to avoid clobbering captured data.
    let Some(hour_day) = store.get("HourDay") else {
        return Ok(());
    };
    if hour_day.height() <= 1 {
        return Ok(());
    }

    // Run's day types, captured before RunSpecDay is collapsed below — the
    // AvgSpeedDistribution weighting scopes to these.
    let run_days: BTreeSet<i32> = match store.get("RunSpecDay") {
        Some(rd) => col_i32(&rd, "dayID")?.into_iter().flatten().collect(),
        None => BTreeSet::new(),
    };

    // OldHourDay: hourDayID → (dayID, hourID), captured before the collapse.
    let hd_id = col_i32(&hour_day, "hourDayID")?;
    let hd_day = col_i32(&hour_day, "dayID")?;
    let hd_hour = col_i32(&hour_day, "hourID")?;
    let mut hour_day_of: BTreeMap<i32, (i32, i32)> = BTreeMap::new();
    for i in 0..hd_id.len() {
        if let (Some(id), Some(d), Some(h)) = (hd_id[i], hd_day[i], hd_hour[i]) {
            hour_day_of.insert(id, (d, h));
        }
    }

    // noOfRealDays per dayID (DayOfAnyWeek, before collapse).
    let no_of_real_days: BTreeMap<i32, f64> = match store.get("DayOfAnyWeek") {
        Some(df) => {
            let day = col_i32(&df, "dayID")?;
            let nord = col_f64(&df, "noOfRealDays")?;
            (0..day.len())
                .filter_map(|i| Some((day[i]?, nord[i]?)))
                .collect()
        }
        None => return Ok(()),
    };

    // monthVMTFraction: (sourceTypeID, monthID) → fraction.
    let mut month_vmt: BTreeMap<(i32, i32), f64> = BTreeMap::new();
    if let Some(df) = store.get("MonthVMTFraction") {
        let st = col_i32(&df, "sourceTypeID")?;
        let mo = col_i32(&df, "monthID")?;
        let fr = col_f64(&df, "monthVMTFraction")?;
        for i in 0..st.len() {
            if let (Some(s), Some(m), Some(f)) = (st[i], mo[i], fr[i]) {
                month_vmt.insert((s, m), f);
            }
        }
    }

    // DayWeighting1(st, rt, day) = Σ_month dayVMT·noOfRealDays·monthVMT
    //                              / Σ_month monthVMT.
    let Some(day_vmt_df) = store.get("DayVMTFraction") else {
        return Ok(());
    };
    let (dv_st, dv_mo, dv_rt, dv_day) = (
        col_i32(&day_vmt_df, "sourceTypeID")?,
        col_i32(&day_vmt_df, "monthID")?,
        col_i32(&day_vmt_df, "roadTypeID")?,
        col_i32(&day_vmt_df, "dayID")?,
    );
    let dv_frac = col_f64(&day_vmt_df, "dayVMTFraction")?;
    // (st, rt, day) → (numerator, denominator)
    let mut dw1_acc: BTreeMap<(i32, i32, i32), (f64, f64)> = BTreeMap::new();
    for i in 0..dv_st.len() {
        let (Some(s), Some(m), Some(rt), Some(d), Some(f)) =
            (dv_st[i], dv_mo[i], dv_rt[i], dv_day[i], dv_frac[i])
        else {
            continue;
        };
        let mvf = month_vmt.get(&(s, m)).copied().unwrap_or(0.0);
        let nord = no_of_real_days.get(&d).copied().unwrap_or(0.0);
        let e = dw1_acc.entry((s, rt, d)).or_insert((0.0, 0.0));
        e.0 += f * nord * mvf;
        e.1 += mvf;
    }
    let day_weighting1: BTreeMap<(i32, i32, i32), f64> = dw1_acc
        .into_iter()
        .map(|(k, (num, den))| (k, if den > 0.0 { num / den } else { 0.0 }))
        .collect();

    // Original hourVMTFraction (st, rt, day, hour) → fraction, captured before
    // the in-place collapse below. Used as the hour-stage weight for the
    // AvgSpeedDistribution collapse (canonical PreAggDAY's HourWeighting1).
    let mut hour_vmt: BTreeMap<(i32, i32, i32, i32), f64> = BTreeMap::new();
    if let Some(df) = store.get("HourVMTFraction") {
        let st = col_i32(&df, "sourceTypeID")?;
        let rt = col_i32(&df, "roadTypeID")?;
        let day = col_i32(&df, "dayID")?;
        let hour = col_i32(&df, "hourID")?;
        let fr = col_f64(&df, "hourVMTFraction")?;
        for i in 0..st.len() {
            if let (Some(s), Some(r), Some(d), Some(h), Some(f)) =
                (st[i], rt[i], day[i], hour[i], fr[i])
            {
                hour_vmt.insert((s, r, d, h), f);
            }
        }
    }

    // roadTypeVMTFraction (st, rt) → fraction, for the HourWeighting2 collapse.
    let mut road_type_vmt: BTreeMap<(i32, i32), f64> = BTreeMap::new();
    if let Some(df) = store.get("RoadTypeDistribution") {
        let st = col_i32(&df, "sourceTypeID")?;
        let rt = col_i32(&df, "roadTypeID")?;
        let fr = col_f64(&df, "roadTypeVMTFraction")?;
        for i in 0..st.len() {
            if let (Some(s), Some(r), Some(f)) = (st[i], rt[i], fr[i]) {
                road_type_vmt.insert((s, r), f);
            }
        }
    }

    // --- HourDay → single (0,0,0) ---
    let collapsed_hour_day = DataFrame::new(
        1,
        vec![
            Series::new("hourDayID".into(), vec![0i32]).into(),
            Series::new("dayID".into(), vec![0i32]).into(),
            Series::new("hourID".into(), vec![0i32]).into(),
        ],
    )
    .map_err(|e| format!("collapsing HourDay: {e}"))?;
    store.insert("HourDay".to_string(), collapsed_hour_day);

    // --- HourOfAnyDay → single (0, "Entire Day") ---
    let collapsed_hoad = DataFrame::new(
        1,
        vec![
            Series::new("hourID".into(), vec![0i32]).into(),
            Series::new("hourName".into(), vec!["Entire Day"]).into(),
        ],
    )
    .map_err(|e| format!("collapsing HourOfAnyDay: {e}"))?;
    store.insert("HourOfAnyDay".to_string(), collapsed_hoad);

    // --- DayOfAnyWeek → single (0, "Whole Week", 7.0) ---
    let collapsed_doaw = DataFrame::new(
        1,
        vec![
            Series::new("dayID".into(), vec![0i32]).into(),
            Series::new("dayName".into(), vec!["Whole Week"]).into(),
            Series::new("noOfRealDays".into(), vec![7.0f64]).into(),
        ],
    )
    .map_err(|e| format!("collapsing DayOfAnyWeek: {e}"))?;
    store.insert("DayOfAnyWeek".to_string(), collapsed_doaw);

    // --- RunSpec time tables → the single collapsed (0) cell ---
    // The activity is allocated across `RunSpecHourDay`; without collapsing these
    // the SHO would still key on the original hourDayIDs (now missing from the
    // collapsed HourDay/DayOfAnyWeek), breaking the universalActivity divisor.
    for (table, col) in [
        ("RunSpecHourDay", "hourDayID"),
        ("RunSpecDay", "dayID"),
        ("RunSpecHour", "hourID"),
    ] {
        if store.get(table).is_some() {
            let df = DataFrame::new(1, vec![Series::new(col.into(), vec![0i32]).into()])
                .map_err(|e| format!("collapsing {table}: {e}"))?;
            store.insert(table.to_string(), df);
        }
    }

    // --- HourVMTFraction → (st, rt, 0, 0, 1.0) per distinct (st, rt) ---
    if let Some(df) = store.get("HourVMTFraction") {
        let st = col_i32(&df, "sourceTypeID")?;
        let rt = col_i32(&df, "roadTypeID")?;
        let pairs: BTreeSet<(i32, i32)> = (0..st.len())
            .filter_map(|i| Some((st[i]?, rt[i]?)))
            .collect();
        let n = pairs.len();
        let (st_c, rt_c): (Vec<i32>, Vec<i32>) = pairs.into_iter().unzip();
        let hv = DataFrame::new(
            n,
            vec![
                Series::new("sourceTypeID".into(), st_c).into(),
                Series::new("roadTypeID".into(), rt_c).into(),
                Series::new("dayID".into(), vec![0i32; n]).into(),
                Series::new("hourID".into(), vec![0i32; n]).into(),
                Series::new("hourVMTFraction".into(), vec![1.0f64; n]).into(),
            ],
        )
        .map_err(|e| format!("collapsing HourVMTFraction: {e}"))?;
        store.insert("HourVMTFraction".to_string(), hv);
    }

    // --- DayVMTFraction → (st, month, rt, 0, 1.0) per distinct (st, month, rt) ---
    {
        let triples: BTreeSet<(i32, i32, i32)> = (0..dv_st.len())
            .filter_map(|i| Some((dv_st[i]?, dv_mo[i]?, dv_rt[i]?)))
            .collect();
        let n = triples.len();
        let (mut st_c, mut mo_c, mut rt_c) = (Vec::new(), Vec::new(), Vec::new());
        for (s, m, rt) in triples {
            st_c.push(s);
            mo_c.push(m);
            rt_c.push(rt);
        }
        let dv = DataFrame::new(
            n,
            vec![
                Series::new("sourceTypeID".into(), st_c).into(),
                Series::new("monthID".into(), mo_c).into(),
                Series::new("roadTypeID".into(), rt_c).into(),
                Series::new("dayID".into(), vec![0i32; n]).into(),
                Series::new("dayVMTFraction".into(), vec![1.0f64; n]).into(),
            ],
        )
        .map_err(|e| format!("collapsing DayVMTFraction: {e}"))?;
        store.insert("DayVMTFraction".to_string(), dv);
    }

    // --- AvgSpeedDistribution → activity-weighted collapse to hourDayID = 0 ---
    if let Some(df) = store.get("AvgSpeedDistribution") {
        let st = col_i32(&df, "sourceTypeID")?;
        let rt = col_i32(&df, "roadTypeID")?;
        let hdid = col_i32(&df, "hourDayID")?;
        let bin = col_i32(&df, "avgSpeedBinID")?;
        let frac = col_f64(&df, "avgSpeedFraction")?;
        // (st, rt, bin) → (Σ frac·W, Σ W); combined weight
        // W = hourVMTFraction(st,rt,day,hour) · DayWeighting1(st,rt,day),
        // mirroring the canonical two-stage HourWeighting × DayWeighting collapse.
        let mut acc: BTreeMap<(i32, i32, i32), (f64, f64)> = BTreeMap::new();
        for i in 0..st.len() {
            let (Some(s), Some(r), Some(hid), Some(b), Some(fr)) =
                (st[i], rt[i], hdid[i], bin[i], frac[i])
            else {
                continue;
            };
            let Some(&(day, hour)) = hour_day_of.get(&hid) else {
                continue;
            };
            if !run_days.is_empty() && !run_days.contains(&day) {
                continue;
            }
            let dw = day_weighting1.get(&(s, r, day)).copied().unwrap_or(0.0);
            let hw = hour_vmt.get(&(s, r, day, hour)).copied().unwrap_or(0.0);
            let w = dw * hw;
            let e = acc.entry((s, r, b)).or_insert((0.0, 0.0));
            e.0 += fr * w;
            e.1 += w;
        }
        let n = acc.len();
        let (mut st_c, mut rt_c, mut hd_c, mut bin_c, mut fr_c) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for ((s, r, b), (num, den)) in acc {
            st_c.push(s);
            rt_c.push(r);
            hd_c.push(0i32);
            bin_c.push(b);
            fr_c.push(if den > 0.0 { num / den } else { 0.0 });
        }
        let asd = DataFrame::new(
            n,
            vec![
                Series::new("sourceTypeID".into(), st_c).into(),
                Series::new("roadTypeID".into(), rt_c).into(),
                Series::new("hourDayID".into(), hd_c).into(),
                Series::new("avgSpeedBinID".into(), bin_c).into(),
                Series::new("avgSpeedFraction".into(), fr_c).into(),
            ],
        )
        .map_err(|e| format!("collapsing AvgSpeedDistribution: {e}"))?;
        store.insert("AvgSpeedDistribution".to_string(), asd);
    }

    // HourWeighting3(hour) for the temperature / AC-term hour collapse:
    // `HW3(hour) = avg_day HW2(preferredSourceType, day, hour)`, where
    // `HW2(st,day,hour) = Σ_rt hourVMTFraction · roadTypeVMTFraction
    // / Σ_rt roadTypeVMTFraction` and the preferred sourceType is the first
    // present in the canonical SourceTypeOrdering (PreAggDAY HourWeighting2/3).
    let hw3: BTreeMap<i32, f64> = {
        let mut hw2_num: BTreeMap<(i32, i32, i32), f64> = BTreeMap::new();
        let mut hw2_den: BTreeMap<(i32, i32, i32), f64> = BTreeMap::new();
        for (&(s, r, d, h), &hv) in &hour_vmt {
            let rtv = road_type_vmt.get(&(s, r)).copied().unwrap_or(0.0);
            *hw2_num.entry((s, d, h)).or_insert(0.0) += hv * rtv;
            *hw2_den.entry((s, d, h)).or_insert(0.0) += rtv;
        }
        let hw2: BTreeMap<(i32, i32, i32), f64> = hw2_num
            .iter()
            .map(|(&k, &num)| {
                let den = hw2_den.get(&k).copied().unwrap_or(0.0);
                (k, if den != 0.0 { num / den } else { 0.0 })
            })
            .collect();
        const SOURCE_TYPE_ORDER: [i32; 13] = [21, 31, 32, 52, 61, 54, 62, 43, 53, 41, 42, 51, 11];
        let mut st_total: BTreeMap<i32, f64> = BTreeMap::new();
        for (&(s, _, _), &v) in &hw2 {
            *st_total.entry(s).or_insert(0.0) += v;
        }
        let preferred_st = SOURCE_TYPE_ORDER
            .into_iter()
            .find(|s| st_total.get(s).is_some_and(|&t| t > 0.0))
            .or_else(|| st_total.keys().next().copied());
        let mut hw3_sum: BTreeMap<i32, f64> = BTreeMap::new();
        let mut hw3_cnt: BTreeMap<i32, i32> = BTreeMap::new();
        if let Some(ps) = preferred_st {
            for (&(s, _d, h), &v) in &hw2 {
                if s == ps {
                    *hw3_sum.entry(h).or_insert(0.0) += v;
                    *hw3_cnt.entry(h).or_insert(0) += 1;
                }
            }
        }
        hw3_sum
            .iter()
            .map(|(&h, &sum)| {
                (
                    h,
                    sum / f64::from(hw3_cnt.get(&h).copied().unwrap_or(1).max(1)),
                )
            })
            .collect()
    };

    // --- MonthGroupHour → HW3-weighted AC terms collapsed to hourID = 0 ---
    // compute_zone_ac_factor joins ZoneMonthHour.hourID with MonthGroupHour.hourID;
    // both must collapse to hour 0 together or the AC energy term is dropped
    // (summer-month under-count). Carries the A/B/C terms (the CV columns are
    // null in canonical) weighted by HW3.
    if let Some(df) = store.get("MonthGroupHour") {
        let grp = col_i32(&df, "monthGroupID")?;
        let hour = col_i32(&df, "hourID")?;
        let term_cols = ["ACActivityTermA", "ACActivityTermB", "ACActivityTermC"];
        let terms: Vec<Vec<Option<f64>>> = term_cols
            .iter()
            .map(|c| col_f64(&df, c))
            .collect::<Result<_, _>>()?;
        let mut acc: BTreeMap<i32, (f64, [f64; 3])> = BTreeMap::new();
        for i in 0..grp.len() {
            let (Some(g), Some(h)) = (grp[i], hour[i]) else {
                continue;
            };
            let w = hw3.get(&h).copied().unwrap_or(0.0);
            let e = acc.entry(g).or_insert((0.0, [0.0; 3]));
            e.0 += w;
            for (k, tc) in terms.iter().enumerate() {
                e.1[k] += tc[i].unwrap_or(0.0) * w;
            }
        }
        let n = acc.len();
        let mut g_c = Vec::new();
        let mut a_c = Vec::new();
        let mut b_c = Vec::new();
        let mut c_c = Vec::new();
        for (g, (wsum, sums)) in acc {
            g_c.push(g);
            let norm = |x: f64| if wsum != 0.0 { x / wsum } else { 0.0 };
            a_c.push(norm(sums[0]));
            b_c.push(norm(sums[1]));
            c_c.push(norm(sums[2]));
        }
        let null_cv: Vec<Option<f64>> = vec![None; n];
        let mgh = DataFrame::new(
            n,
            vec![
                Series::new("monthGroupID".into(), g_c).into(),
                Series::new("hourID".into(), vec![0i32; n]).into(),
                Series::new("ACActivityTermA".into(), a_c).into(),
                Series::new("ACActivityTermACV".into(), null_cv.clone()).into(),
                Series::new("ACActivityTermB".into(), b_c).into(),
                Series::new("ACActivityTermBCV".into(), null_cv.clone()).into(),
                Series::new("ACActivityTermC".into(), c_c).into(),
                Series::new("ACActivityTermCCV".into(), null_cv).into(),
            ],
        )
        .map_err(|e| format!("collapsing MonthGroupHour: {e}"))?;
        store.insert("MonthGroupHour".to_string(), mgh);
    }

    // --- ZoneMonthHour → activity-weighted collapse to hourID = 0 ---
    if let Some(df) = store.get("ZoneMonthHour") {
        // Collapse each met column to a single hourID = 0 row per (month, zone),
        // weighting by HW3(hour). heatIndex is set equal to the collapsed
        // temperature (matching canonical, which leaves it == temperature below
        // the heat-index threshold).
        let month = col_i32(&df, "monthID")?;
        let zone = col_i32(&df, "zoneID")?;
        let hour = col_i32(&df, "hourID")?;
        let met_cols = [
            "temperature",
            "relHumidity",
            "heatIndex",
            "specificHumidity",
            "molWaterFraction",
        ];
        let met: Vec<Vec<Option<f64>>> = met_cols
            .iter()
            .map(|c| col_f64(&df, c))
            .collect::<Result<_, _>>()?;
        // (month, zone) → (Σ HW3, [Σ col·HW3 per met column])
        let mut acc: BTreeMap<(i32, i32), (f64, [f64; 5])> = BTreeMap::new();
        for i in 0..month.len() {
            let (Some(m), Some(z), Some(h)) = (month[i], zone[i], hour[i]) else {
                continue;
            };
            let w = hw3.get(&h).copied().unwrap_or(0.0);
            let e = acc.entry((m, z)).or_insert((0.0, [0.0; 5]));
            e.0 += w;
            for (k, mc) in met.iter().enumerate() {
                e.1[k] += mc[i].unwrap_or(0.0) * w;
            }
        }
        let n = acc.len();
        let (mut m_c, mut z_c, mut h_c) = (Vec::new(), Vec::new(), Vec::new());
        let mut col_out: [Vec<f64>; 5] = Default::default();
        for ((m, z), (wsum, sums)) in acc {
            m_c.push(m);
            z_c.push(z);
            h_c.push(0i32);
            let temp = if wsum != 0.0 { sums[0] / wsum } else { 0.0 };
            for k in 0..5 {
                let v = if wsum != 0.0 { sums[k] / wsum } else { 0.0 };
                // heatIndex (k == 2) tracks temperature in the collapsed cell.
                col_out[k].push(if k == 2 { temp } else { v });
            }
        }
        let zmh = DataFrame::new(
            n,
            vec![
                Series::new("monthID".into(), m_c).into(),
                Series::new("zoneID".into(), z_c).into(),
                Series::new("hourID".into(), h_c).into(),
                Series::new("temperature".into(), std::mem::take(&mut col_out[0])).into(),
                Series::new("relHumidity".into(), std::mem::take(&mut col_out[1])).into(),
                Series::new("heatIndex".into(), std::mem::take(&mut col_out[2])).into(),
                Series::new("specificHumidity".into(), std::mem::take(&mut col_out[3])).into(),
                Series::new("molWaterFraction".into(), std::mem::take(&mut col_out[4])).into(),
            ],
        )
        .map_err(|e| format!("collapsing ZoneMonthHour: {e}"))?;
        store.insert("ZoneMonthHour".to_string(), zmh);
    }

    Ok(())
}

/// Build the runtime-derived `evSalesFraction` table the SBWeighted EV-sales
/// ICE back-scaling consumes (BaseRateGenerator SBWeightedRate step 010).
///
/// Canonical `BaseRateGenerator` (`BaseRateGenerator.java:315-341`) builds it as
/// `evFraction[modelYearID, fleetAvgGroupID] = evsales / sales` over the
/// **age-0 slice** (so `modelYearID = yearID`) of the default
/// `SourceTypeYear ⋈ SourceTypeAgeDistribution ⋈ SampleVehiclePopulation ⋈
/// RegulatoryClass`:
///
/// * `evsales = Σ sourceTypePopulation · ageFraction · stmyFraction` with
///   `fuelTypeID = 9`
/// * `sales   = Σ sourceTypePopulation · ageFraction · stmyFraction` (all fuels)
///
/// restricted to `modelYearID ∈ [analysisYear-40, analysisYear]`. The two
/// subqueries are INNER-joined on `(modelYearID, fleetAvgGroupID)`, so a row is
/// emitted for every group that has *any* fuelType-9 sample-vehicle row (its
/// `evFraction` may be `0`).
///
/// The raw default DB does not ship `evSalesFraction` (it is execution-time
/// derived), so without this step the back-scaling silently no-ops and
/// recent-model-year ICE energy rates come out ~2-3% low (`fuelType 9`
/// electricity ~12% low). No-op if any input table is absent — e.g. a partial
/// wasm partition load — so it can never empty an already-correct store.
fn build_ev_sales_fraction(runspec: &RunSpec, store: &mut InMemoryStore) -> Result<(), String> {
    use std::collections::{HashMap, HashSet};

    // Analysis year drives the [year-40, year] model-year window (canonical uses
    // the run year). A run with no years selected has nothing to build.
    let Some(&year) = runspec.timespan.years.iter().max() else {
        return Ok(());
    };
    let (year, lo) = (year as i64, year as i64 - 40);

    // Case-insensitive column readers (cast to the requested type), matching the
    // idiom used elsewhere in this module.
    let icol = |df: &DataFrame, name: &str| -> Option<Vec<Option<i64>>> {
        let c = df
            .columns()
            .iter()
            .find(|c| c.name().eq_ignore_ascii_case(name))?
            .cast(&DataType::Int64)
            .ok()?;
        let ca = c.i64().ok()?;
        Some((0..ca.len()).map(|i| ca.get(i)).collect())
    };
    let fcol = |df: &DataFrame, name: &str| -> Option<Vec<Option<f64>>> {
        let c = df
            .columns()
            .iter()
            .find(|c| c.name().eq_ignore_ascii_case(name))?
            .cast(&DataType::Float64)
            .ok()?;
        let ca = c.f64().ok()?;
        Some((0..ca.len()).map(|i| ca.get(i)).collect())
    };

    let (Some(sty), Some(stad), Some(svp), Some(rc)) = (
        store.get("SourceTypeYear"),
        store.get("SourceTypeAgeDistribution"),
        store.get("SampleVehiclePopulation"),
        store.get("RegulatoryClass"),
    ) else {
        return Ok(());
    };

    // regClassID → fleetAvgGroupID.
    let (Some(rc_id), Some(rc_grp)) = (icol(&rc, "regClassID"), icol(&rc, "fleetAvgGroupID"))
    else {
        return Ok(());
    };
    let reg_to_group: HashMap<i64, i64> = (0..rc_id.len())
        .filter_map(|i| Some((rc_id[i]?, rc_grp[i]?)))
        .collect();

    // sourceTypePopulation[(sourceTypeID, yearID)].
    let (Some(sty_st), Some(sty_yr), Some(sty_pop)) = (
        icol(&sty, "sourceTypeID"),
        icol(&sty, "yearID"),
        fcol(&sty, "sourceTypePopulation"),
    ) else {
        return Ok(());
    };
    let mut pop: HashMap<(i64, i64), f64> = HashMap::new();
    for i in 0..sty_st.len() {
        if let (Some(s), Some(y), Some(p)) = (sty_st[i], sty_yr[i], sty_pop[i]) {
            pop.insert((s, y), p);
        }
    }

    // age-0 ageFraction[(sourceTypeID, yearID)].
    let (Some(ad_st), Some(ad_yr), Some(ad_age), Some(ad_frac)) = (
        icol(&stad, "sourceTypeID"),
        icol(&stad, "yearID"),
        icol(&stad, "ageID"),
        fcol(&stad, "ageFraction"),
    ) else {
        return Ok(());
    };
    let mut age0: HashMap<(i64, i64), f64> = HashMap::new();
    for i in 0..ad_st.len() {
        if ad_age[i] == Some(0) {
            if let (Some(s), Some(y), Some(f)) = (ad_st[i], ad_yr[i], ad_frac[i]) {
                age0.insert((s, y), f);
            }
        }
    }

    // SampleVehiclePopulation rows. age-0 join ⇒ yearID == modelYearID.
    let (Some(svp_st), Some(svp_my), Some(svp_ft), Some(svp_rc), Some(svp_frac)) = (
        icol(&svp, "sourceTypeID"),
        icol(&svp, "modelYearID"),
        icol(&svp, "fuelTypeID"),
        icol(&svp, "regClassID"),
        fcol(&svp, "stmyFraction"),
    ) else {
        return Ok(());
    };

    let mut sales: HashMap<(i64, i64), f64> = HashMap::new();
    let mut evsales: HashMap<(i64, i64), f64> = HashMap::new();
    let mut ev_present: HashSet<(i64, i64)> = HashSet::new();
    for i in 0..svp_st.len() {
        let (Some(s), Some(my), Some(ft), Some(rcid), Some(frac)) =
            (svp_st[i], svp_my[i], svp_ft[i], svp_rc[i], svp_frac[i])
        else {
            continue;
        };
        if my < lo || my > year {
            continue;
        }
        let Some(&grp) = reg_to_group.get(&rcid) else {
            continue;
        };
        // age-0 ⇒ yearID = modelYearID for the population / age-fraction lookup.
        let (Some(&p), Some(&af)) = (pop.get(&(s, my)), age0.get(&(s, my))) else {
            continue;
        };
        let contrib = p * af * frac;
        *sales.entry((my, grp)).or_insert(0.0) += contrib;
        if ft == 9 {
            ev_present.insert((my, grp));
            *evsales.entry((my, grp)).or_insert(0.0) += contrib;
        }
    }

    // INNER JOIN t1(evsales) ⋈ t2(sales): emit every group with a fuelType-9 row
    // and positive total sales; evFraction may be 0.
    let mut keys: Vec<(i64, i64)> = ev_present.into_iter().collect();
    keys.sort_unstable();
    let mut out_my = Vec::with_capacity(keys.len());
    let mut out_grp = Vec::with_capacity(keys.len());
    let mut out_frac = Vec::with_capacity(keys.len());
    for (my, grp) in keys {
        let total = sales.get(&(my, grp)).copied().unwrap_or(0.0);
        if total <= 0.0 {
            continue;
        }
        out_my.push(my);
        out_grp.push(grp);
        out_frac.push(evsales.get(&(my, grp)).copied().unwrap_or(0.0) / total);
    }

    let n = out_my.len();
    let df = DataFrame::new(
        n,
        vec![
            Series::new("modelYearID".into(), out_my).into(),
            Series::new("fleetAvgGroupID".into(), out_grp).into(),
            Series::new("evFraction".into(), out_frac).into(),
        ],
    )
    .map_err(|e| format!("building evSalesFraction: {e}"))?;
    store.insert("evSalesFraction".to_string(), df);
    Ok(())
}

/// Build the runtime-derived `RegClassSourceTypeFraction` table the activity and
/// evaporative calculators consume (canonical `database/UpdateExecution.sql`,
/// "Create RegClassSourceTypeFraction").
///
/// `regClassFraction` is the share of a `(sourceType, fuelSupplyFuelType,
/// modelYear)` a regClass covers, weighted by fuel-usage fraction:
///
/// * `num[st, fsFT, my, rc] = Σ usageFraction · stmyFraction` over the sample-
///   vehicle rows of `(st, my, rc)` joined to their fuel-usage rows
/// * `denom[st, fsFT, my]   = Σ num` over all regClasses
/// * `regClassFraction = num / denom`
///
/// joining `fuelUsageFraction (fuf) ⋈ SampleVehiclePopulation (svp)` on
/// `fuf.sourceBinFuelTypeID = svp.fuelTypeID`, restricted to the runspec's
/// (sourceType × modelYear) set. The output `fuelTypeID` is the **supply** fuel
/// type (`fuelSupplyFuelTypeID`); zero fractions are dropped.
///
/// In the store `fuelUsageFraction` is already county+fuelYear scoped and the
/// default DB's `modelYearGroupID` is uniformly 0, so the canonical join (which
/// keys only on fuel type) is unambiguous. The raw default DB does not ship this
/// table (it is execution-time derived), so without this step the activity /
/// evaporative-permeation / liquid-leaking calculators error on a missing
/// `RegClassSourceTypeFraction`. No-op if any input table is absent.
fn build_regclass_source_type_fraction(store: &mut InMemoryStore) -> Result<(), String> {
    use std::collections::{HashMap, HashSet};

    let icol = |df: &DataFrame, name: &str| -> Option<Vec<Option<i64>>> {
        let c = df
            .columns()
            .iter()
            .find(|c| c.name().eq_ignore_ascii_case(name))?
            .cast(&DataType::Int64)
            .ok()?;
        let ca = c.i64().ok()?;
        Some((0..ca.len()).map(|i| ca.get(i)).collect())
    };
    let fcol = |df: &DataFrame, name: &str| -> Option<Vec<Option<f64>>> {
        let c = df
            .columns()
            .iter()
            .find(|c| c.name().eq_ignore_ascii_case(name))?
            .cast(&DataType::Float64)
            .ok()?;
        let ca = c.f64().ok()?;
        Some((0..ca.len()).map(|i| ca.get(i)).collect())
    };

    let (Some(fuf), Some(svp), Some(rst), Some(rmy)) = (
        store.get("fuelUsageFraction"),
        store.get("SampleVehiclePopulation"),
        store.get("RunSpecSourceType"),
        store.get("RunSpecModelYear"),
    ) else {
        return Ok(());
    };

    // Runspec (sourceType × modelYear) scope = canonical runspecSourceTypeModelYearID.
    let (Some(rst_ids), Some(rmy_ids)) = (icol(&rst, "sourceTypeID"), icol(&rmy, "modelYearID"))
    else {
        return Ok(());
    };
    let src_set: HashSet<i64> = rst_ids.into_iter().flatten().collect();
    let my_set: HashSet<i64> = rmy_ids.into_iter().flatten().collect();

    // fuelUsageFraction: sourceBinFuelTypeID → [(fuelSupplyFuelTypeID, usageFraction)].
    let (Some(fuf_bin), Some(fuf_sup), Some(fuf_use)) = (
        icol(&fuf, "sourceBinFuelTypeID"),
        icol(&fuf, "fuelSupplyFuelTypeID"),
        fcol(&fuf, "usageFraction"),
    ) else {
        return Ok(());
    };
    let mut usage: HashMap<i64, Vec<(i64, f64)>> = HashMap::new();
    for i in 0..fuf_bin.len() {
        if let (Some(b), Some(s), Some(u)) = (fuf_bin[i], fuf_sup[i], fuf_use[i]) {
            usage.entry(b).or_default().push((s, u));
        }
    }

    let (Some(svp_st), Some(svp_my), Some(svp_ft), Some(svp_rc), Some(svp_frac)) = (
        icol(&svp, "sourceTypeID"),
        icol(&svp, "modelYearID"),
        icol(&svp, "fuelTypeID"),
        icol(&svp, "regClassID"),
        fcol(&svp, "stmyFraction"),
    ) else {
        return Ok(());
    };

    let mut num: HashMap<(i64, i64, i64, i64), f64> = HashMap::new();
    let mut denom: HashMap<(i64, i64, i64), f64> = HashMap::new();
    for i in 0..svp_st.len() {
        let (Some(st), Some(my), Some(bin), Some(rc), Some(frac)) =
            (svp_st[i], svp_my[i], svp_ft[i], svp_rc[i], svp_frac[i])
        else {
            continue;
        };
        if !src_set.contains(&st) || !my_set.contains(&my) {
            continue;
        }
        let Some(supplies) = usage.get(&bin) else {
            continue;
        };
        for &(fs, u) in supplies {
            let contrib = u * frac;
            *num.entry((st, fs, my, rc)).or_insert(0.0) += contrib;
            *denom.entry((st, fs, my)).or_insert(0.0) += contrib;
        }
    }

    let mut keys: Vec<(i64, i64, i64, i64)> = num.keys().copied().collect();
    keys.sort_unstable();
    let (mut o_ft, mut o_my, mut o_st, mut o_rc, mut o_fr) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for (st, fs, my, rc) in keys {
        let d = denom.get(&(st, fs, my)).copied().unwrap_or(0.0);
        if d == 0.0 {
            continue;
        }
        let frac = num[&(st, fs, my, rc)] / d;
        if frac == 0.0 {
            continue;
        }
        o_ft.push(fs);
        o_my.push(my);
        o_st.push(st);
        o_rc.push(rc);
        o_fr.push(frac);
    }

    let n = o_ft.len();
    let df = DataFrame::new(
        n,
        vec![
            Series::new("fuelTypeID".into(), o_ft).into(),
            Series::new("modelYearID".into(), o_my).into(),
            Series::new("sourceTypeID".into(), o_st).into(),
            Series::new("regClassID".into(), o_rc).into(),
            Series::new("regClassFraction".into(), o_fr).into(),
        ],
    )
    .map_err(|e| format!("building RegClassSourceTypeFraction: {e}"))?;
    store.insert("RegClassSourceTypeFraction".to_string(), df);
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
    // FuelSupply also ships every monthGroupID (0–12). Canonical extracts it
    // per calculator filtered to the run's month (`FuelSupply INNER JOIN
    // MonthOfAnyYear WHERE monthID = context.monthID`), so each iteration sees a
    // single monthGroup. The port loads the whole table, and the fuel-adjustment
    // chains join it WITHOUT a monthGroup key (e.g. EvaporativePermeationCalculator
    // PC-4 joins WeightedFuelAdjustment → SBWeightedPermeationRate on
    // (polProcessID, modelYearID, fuelTypeID) only). With all 12 month groups
    // present, every emission rate is multiplied by the month count — a clean
    // 12× over-count on a single-month run. Prune FuelSupply to the run's
    // month group(s), mirroring the canonical extraction. (In the snapshot path
    // FuelSupply is already captured filtered to the run's month, so this is a
    // no-op there.)
    let month_groups = month_groups_for_runspec(store, runspec);
    if !month_groups.is_empty() {
        prune_table_by_id(store, "FuelSupply", "monthGroupID", &month_groups)?;
    }
    Ok(())
}

/// Map the runspec's selected calendar months to `monthGroupID`s via the loaded
/// `MonthOfAnyYear` table (`monthID` → `monthGroupID`). Falls back to the
/// identity mapping (`monthGroupID == monthID`, which holds in the default DB)
/// when `MonthOfAnyYear` is absent or lacks the columns. Empty when the runspec
/// selects no months — the caller then leaves `FuelSupply` unpruned by month.
fn month_groups_for_runspec(store: &InMemoryStore, runspec: &RunSpec) -> BTreeSet<i64> {
    let month_set: BTreeSet<i64> = runspec
        .timespan
        .months
        .iter()
        .map(|&m| i64::from(m))
        .collect();
    if month_set.is_empty() {
        return BTreeSet::new();
    }
    let Some(arc) = store.get("MonthOfAnyYear") else {
        return month_set; // identity fallback (monthGroupID == monthID)
    };
    let df = &*arc;
    let col = |name: &str| {
        df.columns()
            .iter()
            .find(|c| c.name().eq_ignore_ascii_case(name))
            .and_then(|c| c.cast(&DataType::Int64).ok())
    };
    let (Some(mid_col), Some(mg_col)) = (col("monthID"), col("monthGroupID")) else {
        return month_set; // identity fallback
    };
    let (Ok(mids), Ok(mgs)) = (mid_col.i64(), mg_col.i64()) else {
        return month_set;
    };
    let mut out: BTreeSet<i64> = BTreeSet::new();
    for i in 0..df.height() {
        if let (Some(m), Some(g)) = (mids.get(i), mgs.get(i)) {
            if month_set.contains(&m) {
                out.insert(g);
            }
        }
    }
    if out.is_empty() {
        month_set
    } else {
        out
    }
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
    let fallback = || {
        county_ids
            .iter()
            .map(|&c| c * 10)
            .collect::<BTreeSet<i64>>()
    };

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
pub fn transform_high_ethanol_fuel_properties(store: &mut InMemoryStore) -> Result<(), String> {
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
        let name = col_name(df, want).ok_or_else(|| format!("{ctx}: column {want} missing"))?;
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
        let name = col_name(df, want).ok_or_else(|| format!("{ctx}: column {want} missing"))?;
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
    let coalesce = |e1: Option<usize>,
                    e0: Option<usize>,
                    src: &[Option<f64>],
                    existing: Option<f64>|
     -> Option<f64> {
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
    log(&format!(
        "altered {altered} high-ethanol formulation row(s)"
    ));

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

    // RunSpecPollutantProcess. Includes the silently-required chain
    // prerequisites (canonical `ExecutionRunSpec.flagRequiredPollutantProcesses`)
    // — e.g. refueling (processes 18/19) chains off Total Energy Consumption
    // (pollutant 91) for Running/Start/Extended-Idle Exhaust (processes 1/2/90).
    // `scope_pollutant_process_model_year_to_runspec` prunes
    // `PollutantProcessModelYear` to this table's polProcessIDs; without the
    // prerequisites the energy model-year-group rows are dropped, so the
    // SourceBinDistribution generator's `aggregate_svp` inner-join to PPMY yields
    // no energy distribution, `BaseRate` emits no energy, and the chained
    // refueling calculator sees no input (gate `process-refueling`: 0 rows). The
    // refueling/CO2AE calculators that also read this table intersect it with
    // their own (pollutant, process) pairs, so the extra energy rows are inert
    // there. For every non-chained runspec this is a no-op (the execution set
    // equals the raw set).
    let pol_process_ids: Vec<i32> = {
        let exec = moves_framework::execution::ExecutionRunSpec::new(runspec.clone());
        let mut ids: BTreeSet<i32> = BTreeSet::new();
        for assoc in &exec.pollutant_process_associations {
            ids.insert(i32::from(assoc.pollutant_id.0) * 100 + i32::from(assoc.process_id.0));
        }
        ids.into_iter().collect()
    };
    insert_i32(
        store,
        "RunSpecPollutantProcess",
        "polProcessID",
        pol_process_ids,
    );

    // RunSpecDay. `<day key>` is a 0-based INDEX into the sorted DayOfAnyWeek
    // dayID list (canonical `TimeSpan.getDayByIndex` over `order by dayID` =
    // [2 weekend, 5 weekday]): key 0 -> day 2, key 1 -> day 5, key >= 2 -> out
    // of range. An out-of-range or empty selection means canonical adds no day
    // and the execution time span falls back to ALL day types — the common
    // `<day key="5"/>` fixtures, and `expand-day`'s keys 2/5. So convert the
    // selection through the sorted DayOfAnyWeek list and use all day types only
    // when nothing valid is selected. The port's RunSpec model stores the
    // literal `<day>` key (unlike months/hours, which `xml_format` already
    // index-converts), so the index->dayID conversion happens here.
    //
    // This restricts the activity (RunSpecHourDay -> SHO) to the selected day,
    // so the output is day-filtered without a separate output pass: the
    // captured snapshot's SHO carries only the selected `hourDayID` (e.g.
    // sample-runspec: hourDay 72 = hour 7 / day 2), and the default-DB path
    // previously over-emitted both day types (1000/168 rows vs canonical
    // 500/84).
    let day_ids: Vec<i32> = {
        let all_sorted: Vec<i32> = day_ids_from_day_of_any_week(store).into_iter().collect();
        if all_sorted.is_empty() {
            // DayOfAnyWeek absent — fall back to the literal runspec keys.
            runspec.timespan.days.iter().map(|&d| d as i32).collect()
        } else {
            let selected: BTreeSet<i32> = runspec
                .timespan
                .days
                .iter()
                .filter_map(|&k| all_sorted.get(k as usize).copied())
                .collect();
            if selected.is_empty() {
                all_sorted
            } else {
                selected.into_iter().collect()
            }
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
pub fn populate_pollutant_process_mapped_model_year(
    store: &mut InMemoryStore,
) -> Result<(), String> {
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

//! Data-plane loader: builds the `moves-nonroad` engine's reference
//! tables from the MOVES `nr*` execution-DB tables.
//!
//! Phase 5 / Task 119. In canonical MOVES the `NonroadEmissionCalculator`
//! Java class wrote the `nr*` database tables out to NONROAD's ~30
//! fixed-width input files (`.EMF`, `.POP`, `.GRW`, …) and spawned
//! `nonroad.exe`. This module is the in-process replacement for that
//! input-generation half: it reads the same `nr*` tables straight from
//! the [`InMemoryStore`](moves_framework::data::InMemoryStore) and
//! assembles them into the [`ReferenceData`] / [`NonroadInputs`] values
//! that [`moves_nonroad::run_simulation`] consumes.
//!
//! The loader is split into focused builders (emission factors,
//! population, activity, growth, …) so each can be unit-tested against
//! small synthetic tables. The top-level [`load_nonroad_reference`] wires
//! them together.
//!
//! # Column conventions
//!
//! The snapshot Parquet stores integer keys as `int64` and *decimal*
//! columns (rates, fractions, populations) as zero-padded **strings**.
//! Column casing also varies between sibling tables (`nrdayallocation`
//! uses lowercase `scc`; `nrhpcategory` is fully lowercase). The
//! [`col`]-family helpers below resolve names case-insensitively and
//! accept either a numeric or a string-decimal physical type.

use std::collections::BTreeMap;

use moves_framework::data::DataFrameStore;
use moves_nonroad::common::consts::{MXHPC, MXPOL, MXTECH};
use moves_nonroad::driver::{DriverRecord, RegionLevel, RunRegions};
use moves_nonroad::emissions::exhaust::EmissionUnitCode;
use moves_nonroad::geography::common::ActivityUnit;
use moves_nonroad::input::scrappage::ScrappagePoint;
use moves_nonroad::population::{AgeAdjustmentTable, GrowthIndicatorRecord};
use moves_nonroad::simulation::{
    ActivityTableEntry, EvapTechEntry, ExhaustTechEntry, NonroadInputs, NonroadOptions,
    ProductionExecutor, ReferenceData, SimEmissionRow,
};
use polars::prelude::*;

/// Pseudo-county FIPS used to drive the working County-dispatch path for
/// a NATION-level run. The snapshot's only "county" is `countyID = 0`
/// ("Nation"), but `00000` classifies as a *national* region shape and
/// would match no County dispatch branch — so we run the national
/// population through a single non-zero pseudo-county and null the
/// `countyID` back out when emitting (the output is a national aggregate).
const PSEUDO_COUNTY: &str = "00001";

/// Standard NONROAD HP representative levels (`hpclev` / `HPCAT`), used by
/// [`ProductionExecutor`] when partitioning equipment by HP.
const HP_LEVELS: [f32; MXHPC] = [
    3.0, 6.0, 11.0, 16.0, 25.0, 40.0, 50.0, 75.0, 100.0, 175.0, 300.0, 600.0, 750.0, 1000.0,
    1200.0, 1500.0, 1800.0, 2000.0,
];

/// MOVES `polProcessID` for the nonroad base rates we map onto engine
/// pollutant slots. `polProcessID = pollutantID * 100 + processID`; all
/// nonroad emission rates are at process 1.
const PP_THC: i64 = 101;
const PP_CO: i64 = 201;
const PP_NOX: i64 = 301;
const PP_PM: i64 = 10001;
/// Brake-specific fuel consumption carrier (pollutant 99, process 1).
/// Not an emitted pollutant — feeds the per-tech BSFC array the CO2/SOx
/// branches of `clcems` rely on.
const PP_BSFC: i64 = 9901;

/// Map a nonroad `polProcessID` to the engine's 0-based pollutant slot
/// ([`moves_nonroad::emissions::exhaust::PollutantIndex::slot`]).
/// Returns `None` for `polProcessID`s that are not exhaust pollutants
/// with a reserved slot (e.g. the BSFC carrier, handled separately).
fn pollutant_slot_for(pol_process_id: i64) -> Option<usize> {
    match pol_process_id {
        PP_THC => Some(0), // PollutantIndex::Thc
        PP_CO => Some(1),  // PollutantIndex::Co
        PP_NOX => Some(2), // PollutantIndex::Nox
        PP_PM => Some(5),  // PollutantIndex::Pm
        _ => None,
    }
}

/// Resolve a column name case-insensitively, returning the canonical name
/// the DataFrame actually stores it under.
fn resolve<'a>(df: &'a DataFrame, name: &str) -> Option<&'a str> {
    df.get_column_names()
        .into_iter()
        .map(|s| s.as_str())
        .find(|c| c.eq_ignore_ascii_case(name))
}

/// Extract an integer column as `Vec<i64>`, tolerating either a true
/// integer physical type or a string-encoded integer. Missing/unparseable
/// cells become `0`.
fn int_col(df: &DataFrame, name: &str) -> Vec<i64> {
    let Some(actual) = resolve(df, name) else {
        return vec![0; df.height()];
    };
    let Ok(col) = df.column(actual) else {
        return vec![0; df.height()];
    };
    if let Ok(ca) = col.i64() {
        return (0..df.height()).map(|i| ca.get(i).unwrap_or(0)).collect();
    }
    if let Ok(ca) = col.i32() {
        return (0..df.height())
            .map(|i| ca.get(i).map(i64::from).unwrap_or(0))
            .collect();
    }
    if let Ok(ca) = col.str() {
        return (0..df.height())
            .map(|i| {
                ca.get(i)
                    .and_then(|s| s.trim().parse::<f64>().ok())
                    .map(|v| v as i64)
                    .unwrap_or(0)
            })
            .collect();
    }
    vec![0; df.height()]
}

/// Extract a decimal column as `Vec<f64>`, tolerating a string-encoded
/// decimal (the snapshot's storage form), a native float, or an integer.
/// Missing/unparseable cells become `0.0`.
fn float_col(df: &DataFrame, name: &str) -> Vec<f64> {
    let Some(actual) = resolve(df, name) else {
        return vec![0.0; df.height()];
    };
    let Ok(col) = df.column(actual) else {
        return vec![0.0; df.height()];
    };
    if let Ok(ca) = col.f64() {
        return (0..df.height()).map(|i| ca.get(i).unwrap_or(0.0)).collect();
    }
    if let Ok(ca) = col.str() {
        return (0..df.height())
            .map(|i| {
                ca.get(i)
                    .and_then(|s| s.trim().parse::<f64>().ok())
                    .unwrap_or(0.0)
            })
            .collect();
    }
    if let Ok(ca) = col.i64() {
        return (0..df.height())
            .map(|i| ca.get(i).map(|v| v as f64).unwrap_or(0.0))
            .collect();
    }
    if let Ok(ca) = col.i32() {
        return (0..df.height())
            .map(|i| ca.get(i).map(f64::from).unwrap_or(0.0))
            .collect();
    }
    vec![0.0; df.height()]
}

/// Extract a string column as `Vec<String>`. Numeric physical types are
/// stringified. Missing cells become the empty string.
fn str_col(df: &DataFrame, name: &str) -> Vec<String> {
    let Some(actual) = resolve(df, name) else {
        return vec![String::new(); df.height()];
    };
    let Ok(col) = df.column(actual) else {
        return vec![String::new(); df.height()];
    };
    if let Ok(ca) = col.str() {
        return (0..df.height())
            .map(|i| ca.get(i).unwrap_or("").trim().to_string())
            .collect();
    }
    if let Ok(ca) = col.i64() {
        return (0..df.height())
            .map(|i| ca.get(i).map(|v| v.to_string()).unwrap_or_default())
            .collect();
    }
    vec![String::new(); df.height()]
}

/// Map a nonroad emission-rate `units` string to the engine's
/// [`EmissionUnitCode`]. Unknown/blank units default to `g/HP-hr`, the
/// dominant nonroad exhaust unit.
fn unit_code_for(units: &str) -> EmissionUnitCode {
    match units.trim().to_ascii_lowercase().as_str() {
        "g/hp-hr" | "g/hphr" | "" => EmissionUnitCode::GramsPerHpHour,
        "g/gallon" | "g/gal" => EmissionUnitCode::GramsPerGallon,
        "g/hr" => EmissionUnitCode::GramsPerHour,
        "g/day" => EmissionUnitCode::GramsPerDay,
        "g/start" => EmissionUnitCode::GramsPerStart,
        "mult" => EmissionUnitCode::Multiplier,
        _ => EmissionUnitCode::GramsPerHpHour,
    }
}

/// One (SCC, HP-bin) bucket's worth of per-engine-tech, per-pollutant
/// exhaust rates, accumulated while scanning `nremissionrate`.
#[derive(Default)]
struct RateBucket {
    /// Distinct engine-tech IDs in first-seen order; defines the tech-slot
    /// ordering for every parallel array on the emitted entry.
    tech_ids: Vec<i64>,
    /// `(pollutant_slot, tech_slot) -> base rate`.
    rates: BTreeMap<(usize, usize), f32>,
    /// `(pollutant_slot, tech_slot) -> unit code`.
    units: BTreeMap<(usize, usize), EmissionUnitCode>,
    /// `tech_slot -> BSFC (from polProcessID 9901)`.
    bsfc: BTreeMap<usize, f32>,
}

impl RateBucket {
    /// Index of `tech_id`, inserting it if new.
    fn tech_slot(&mut self, tech_id: i64) -> usize {
        if let Some(i) = self.tech_ids.iter().position(|&t| t == tech_id) {
            i
        } else {
            self.tech_ids.push(tech_id);
            self.tech_ids.len() - 1
        }
    }
}

/// Deterioration coefficients keyed by `(polProcessID, engTechID)`.
type DetMap = BTreeMap<(i64, i64), (f32, f32, f32)>;

/// Read `nrdeterioration` into a `(polProcessID, engTechID) -> (A, B, cap)`
/// lookup. Empty when the table is absent.
fn load_deterioration<S: DataFrameStore + ?Sized>(store: &S) -> DetMap {
    let Some(df) = store.get("nrdeterioration") else {
        return DetMap::new();
    };
    let pp = int_col(&df, "polProcessID");
    let tech = int_col(&df, "engTechID");
    let a = float_col(&df, "DFCoefficient");
    let b = float_col(&df, "DFAgeExponent");
    let cap = float_col(&df, "emissionCap");
    let mut map = DetMap::new();
    for i in 0..df.height() {
        map.insert((pp[i], tech[i]), (a[i] as f32, b[i] as f32, cap[i] as f32));
    }
    map
}

/// Build the per-`(SCC, HP-bin)` exhaust-tech entries — emission factors,
/// units, deterioration, and BSFC — from `nremissionrate` +
/// `nrdeterioration`.
///
/// The returned entries carry `emission_factors` / `emission_units` /
/// `det_*` laid out `[pollutant_slot * n_tech + tech]` exactly as
/// [`ExhaustTechEntry`] documents, so `compute_exhaust_factors` can expand
/// them into the engine's `[year][pollutant][tech]` arrays. `tech_names`
/// is the engine-tech ID list (stringified) in first-seen order;
/// `tech_fractions` is left empty here and filled by the tech-fraction
/// builder (it varies by model year, unlike the rates).
pub fn build_exhaust_tech_entries<S: DataFrameStore + ?Sized>(store: &S) -> Vec<ExhaustTechEntry> {
    let Some(df) = store.get("nremissionrate") else {
        return Vec::new();
    };
    let det = load_deterioration(store);

    let scc = str_col(&df, "SCC");
    let pp = int_col(&df, "polProcessID");
    let hp_min = int_col(&df, "hpMin");
    let hp_max = int_col(&df, "hpMax");
    let tech = int_col(&df, "engTechID");
    let rate = float_col(&df, "meanBaseRate");
    let units = str_col(&df, "units");

    // Group rate rows by (SCC, hpMin, hpMax). BTreeMap keeps a stable
    // deterministic order for reproducible output.
    let mut buckets: BTreeMap<(String, i64, i64), RateBucket> = BTreeMap::new();
    for i in 0..df.height() {
        let key = (scc[i].clone(), hp_min[i], hp_max[i]);
        let bucket = buckets.entry(key).or_default();
        let tslot = bucket.tech_slot(tech[i]);
        if pp[i] == PP_BSFC {
            bucket.bsfc.insert(tslot, rate[i] as f32);
            continue;
        }
        let Some(pslot) = pollutant_slot_for(pp[i]) else {
            continue;
        };
        bucket.rates.insert((pslot, tslot), rate[i] as f32);
        bucket
            .units
            .insert((pslot, tslot), unit_code_for(&units[i]));
    }

    // Index the rate buckets by their rate SCC so a population SCC can be
    // resolved to its most-specific available rate SCC.
    let mut hp_bins_by_scc: BTreeMap<String, Vec<(i64, i64)>> = BTreeMap::new();
    for (scc, hp_min, hp_max) in buckets.keys() {
        hp_bins_by_scc
            .entry(scc.clone())
            .or_default()
            .push((*hp_min, *hp_max));
    }

    // The SCCs the population (and driver records) actually use are the
    // full 10-digit codes in `nrsourceusetype`. Emission rates, however,
    // are keyed at a coarser level: some 10-digit SCCs have specific rows,
    // the rest fall back to the engine-family root `22XX000000`. Emit one
    // entry per (population SCC, HP bin) drawing on the resolved rate SCC,
    // so `find_exhaust_tech(full_scc, hp)` matches.
    let mut target_sccs = population_sccs(store);
    if target_sccs.is_empty() {
        // No source-use-type table (e.g. unit tests): key entries by the
        // rate SCCs directly.
        target_sccs = hp_bins_by_scc.keys().cloned().collect();
    }

    let mut entries = Vec::new();
    for pop_scc in target_sccs {
        let rate_scc = if hp_bins_by_scc.contains_key(&pop_scc) {
            pop_scc.clone()
        } else {
            family_root(&pop_scc)
        };
        let Some(bins) = hp_bins_by_scc.get(&rate_scc) else {
            continue;
        };
        for &(hp_min, hp_max) in bins {
            let Some(bucket) = buckets.get(&(rate_scc.clone(), hp_min, hp_max)) else {
                continue;
            };
            entries.push(make_exhaust_entry(
                pop_scc.clone(),
                hp_min,
                hp_max,
                bucket,
                &det,
            ));
        }
    }
    entries
}

/// The engine-family root SCC for a full 10-digit nonroad SCC — the first
/// four digits followed by `000000` (e.g. `2260006005` → `2260000000`).
fn family_root(scc: &str) -> String {
    if scc.len() >= 10 {
        format!("{}000000", &scc[..4])
    } else {
        scc.to_string()
    }
}

/// Most-specific lookup of a 10-digit SCC in a key set: try the full SCC,
/// then progressively zero trailing digit groups (subtype → equipment →
/// family), since different nr* tables key at different SCC aggregation
/// levels (e.g. `nrmonthallocation` keys at the equipment level
/// `2260001000`, not the family root `2260000000`).
fn scc_lookup<'a, V>(map: &'a BTreeMap<String, V>, scc: &str) -> Option<&'a V> {
    if let Some(v) = map.get(scc) {
        return Some(v);
    }
    if scc.len() == 10 {
        for k in [2usize, 4, 6] {
            let mut key = scc[..10 - k].to_string();
            key.push_str(&"0".repeat(k));
            if let Some(v) = map.get(&key) {
                return Some(v);
            }
        }
    }
    None
}

/// Distinct full SCCs present in `nrsourceusetype` (the population /
/// driver-record SCCs). Empty when the table is absent.
fn population_sccs<S: DataFrameStore + ?Sized>(store: &S) -> Vec<String> {
    let Some(df) = store.get("nrsourceusetype") else {
        return Vec::new();
    };
    let mut set: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for s in str_col(&df, "SCC") {
        if !s.is_empty() {
            set.insert(s);
        }
    }
    set.into_iter().collect()
}

/// Materialise one [`ExhaustTechEntry`] for `(scc, hp bin)` from a rate
/// bucket, expanding its `(pollutant, tech)` rates / units / deterioration
/// into the entry's flat `[pollutant_slot * n_tech + tech]` arrays.
fn make_exhaust_entry(
    scc: String,
    hp_min: i64,
    hp_max: i64,
    bucket: &RateBucket,
    det: &DetMap,
) -> ExhaustTechEntry {
    // The engine indexes per-tech arrays with an `MXTECH` stride and caps
    // the tech dimension at `MXTECH`; never emit more tech slots than that.
    let n_tech = bucket.tech_ids.len().clamp(1, MXTECH);
    let mut emission_factors = vec![0.0_f32; MXPOL * n_tech];
    let mut emission_units = vec![EmissionUnitCode::GramsPerHpHour; MXPOL * n_tech];
    let mut det_a = vec![0.0_f32; MXPOL * n_tech];
    let mut det_b = vec![0.0_f32; MXPOL * n_tech];
    let mut det_cap = vec![0.0_f32; MXPOL * n_tech];
    let mut bsfc = vec![0.0_f32; n_tech];

    for (&(pslot, tslot), &r) in &bucket.rates {
        if tslot >= n_tech {
            continue; // tech slot beyond the MXTECH cap
        }
        let idx = pslot * n_tech + tslot;
        emission_factors[idx] = r;
        if let Some(u) = bucket.units.get(&(pslot, tslot)) {
            emission_units[idx] = *u;
        }
        // Deterioration is keyed by (polProcessID, engTechID). Recover the
        // polProcessID from the pollutant slot.
        let pp_for_slot = match pslot {
            0 => PP_THC,
            1 => PP_CO,
            2 => PP_NOX,
            5 => PP_PM,
            _ => continue,
        };
        let tech_id = bucket.tech_ids[tslot];
        if let Some(&(a, b, cap)) = det.get(&(pp_for_slot, tech_id)) {
            det_a[idx] = a;
            det_b[idx] = b;
            det_cap[idx] = cap;
        }
    }
    for (&tslot, &v) in &bucket.bsfc {
        if tslot < n_tech {
            bsfc[tslot] = v;
        }
    }

    ExhaustTechEntry {
        scc,
        hp_min: hp_min as f32,
        hp_max: hp_max as f32,
        tech_names: bucket
            .tech_ids
            .iter()
            .take(n_tech)
            .map(|t| t.to_string())
            .collect(),
        // Filled by the tech-fraction builder; the rates above are
        // model-year independent but the tech mix is not.
        tech_fractions: vec![0.0; n_tech],
        bsfc,
        emission_factors,
        emission_units,
        det_a,
        det_b,
        det_cap,
        tech_fractions_by_year: BTreeMap::new(),
    }
}

/// One source-use-type row joined to its population: the unit of nonroad
/// equipment the driver loop iterates.
struct SourceUnit {
    scc: String,
    hp_avg: f32,
    hours_used_per_year: f32,
    load_factor: f32,
    population: f32,
    /// `nrsourceusetype.medianLifeFullLoad` — the NONROAD `.POP` "usage"
    /// field that drives scrptime's equipment lifespan.
    median_life: f32,
}

/// The sectors the runspec selected (`runspecsector`). `None` ⇒ no
/// selection table, so no sector filtering is applied.
fn selected_sectors<S: DataFrameStore + ?Sized>(
    store: &S,
) -> Option<std::collections::BTreeSet<i64>> {
    let df = store.get("runspecsector")?;
    let set: std::collections::BTreeSet<i64> = int_col(&df, "sectorID").into_iter().collect();
    (!set.is_empty()).then_some(set)
}

/// The fuel types the runspec selected (`runspecfueltype`). `None` ⇒ no
/// selection table. These are MOVES fuelTypeIDs; nonroad shares
/// `1 = Gasoline` with onroad but uses `23/24` for diesel — so an onroad
/// diesel selection (`2`) matches no nonroad SCC, which is exactly how
/// canonical produces gasoline-only nonroad output for a gas+diesel
/// runspec.
fn selected_fuels<S: DataFrameStore + ?Sized>(
    store: &S,
) -> Option<std::collections::BTreeSet<i64>> {
    let df = store.get("runspecfueltype")?;
    let set: std::collections::BTreeSet<i64> = int_col(&df, "fuelTypeID").into_iter().collect();
    (!set.is_empty()).then_some(set)
}

/// Map each full SCC to its nonroad fuel type via `nrscc`.
fn scc_fuel_map<S: DataFrameStore + ?Sized>(store: &S) -> BTreeMap<String, i64> {
    let mut map = BTreeMap::new();
    if let Some(df) = store.get("nrscc") {
        let scc = str_col(&df, "SCC");
        let fuel = int_col(&df, "fuelTypeID");
        for i in 0..df.height() {
            map.insert(scc[i].clone(), fuel[i]);
        }
    }
    map
}

/// Map each full SCC to its sector via `nrscc` (SCC → NREquipTypeID) and
/// `nrequipmenttype` (NREquipTypeID → sectorID).
fn scc_sector_map<S: DataFrameStore + ?Sized>(store: &S) -> BTreeMap<String, i64> {
    let mut equip_sector: BTreeMap<i64, i64> = BTreeMap::new();
    if let Some(df) = store.get("nrequipmenttype") {
        let id = int_col(&df, "NREquipTypeID");
        let sec = int_col(&df, "sectorID");
        for i in 0..df.height() {
            equip_sector.insert(id[i], sec[i]);
        }
    }
    let mut map = BTreeMap::new();
    if let Some(df) = store.get("nrscc") {
        let scc = str_col(&df, "SCC");
        let eq = int_col(&df, "NREquipTypeID");
        for i in 0..df.height() {
            if let Some(&sec) = equip_sector.get(&eq[i]) {
                map.insert(scc[i].clone(), sec);
            }
        }
    }
    map
}

/// Join `nrsourceusetype` (SCC, hp, activity by `sourceTypeID`) to
/// `nrbaseyearequippopulation` (population by `sourceTypeID`), yielding one
/// [`SourceUnit`] per source type with non-zero population, **restricted to
/// the runspec's selected sectors** (the snapshot carries every nonroad
/// sector; the runspec may select only some — e.g. commercial).
fn load_source_units<S: DataFrameStore + ?Sized>(store: &S) -> Vec<SourceUnit> {
    let Some(sut) = store.get("nrsourceusetype") else {
        return Vec::new();
    };
    let Some(pop) = store.get("nrbaseyearequippopulation") else {
        return Vec::new();
    };
    let sectors = selected_sectors(store);
    let scc_sector = if sectors.is_some() {
        scc_sector_map(store)
    } else {
        BTreeMap::new()
    };
    let fuels = selected_fuels(store);
    let scc_fuel = if fuels.is_some() {
        scc_fuel_map(store)
    } else {
        BTreeMap::new()
    };

    // population by sourceTypeID (summed across any state rows; the fixture
    // carries a single national stateID = 0).
    let pop_src = int_col(&pop, "sourceTypeID");
    let pop_val = float_col(&pop, "population");
    let mut pop_by_src: BTreeMap<i64, f64> = BTreeMap::new();
    for i in 0..pop.height() {
        *pop_by_src.entry(pop_src[i]).or_default() += pop_val[i];
    }

    let src = int_col(&sut, "sourceTypeID");
    let scc = str_col(&sut, "SCC");
    let hp_avg = float_col(&sut, "hpAvg");
    let hours = float_col(&sut, "hoursUsedPerYear");
    let load = float_col(&sut, "loadFactor");
    let median_life = float_col(&sut, "medianLifeFullLoad");

    let mut units = Vec::new();
    for i in 0..sut.height() {
        let population = pop_by_src.get(&src[i]).copied().unwrap_or(0.0);
        if population <= 0.0 {
            continue;
        }
        // Skip equipment outside the runspec's selected sectors.
        if let Some(sel) = &sectors {
            match scc_sector.get(&scc[i]) {
                Some(sec) if sel.contains(sec) => {}
                _ => continue,
            }
        }
        // Skip equipment whose fuel is not in the runspec's fuel selection.
        if let Some(sel) = &fuels {
            match scc_fuel.get(&scc[i]) {
                Some(fuel) if sel.contains(fuel) => {}
                _ => continue,
            }
        }
        units.push(SourceUnit {
            scc: scc[i].clone(),
            hp_avg: hp_avg[i] as f32,
            hours_used_per_year: hours[i] as f32,
            load_factor: load[i] as f32,
            population: population as f32,
            median_life: median_life[i] as f32,
        });
    }
    units
}

/// Build the [`NonroadInputs`] population bundle: one [`DriverRecord`] per
/// source unit, grouped by SCC, all assigned to [`PSEUDO_COUNTY`]. The
/// population is the base-year (`NRBaseYearID`) snapshot; the engine's
/// `age_distribution` projects it forward to the analysis (growth) year.
pub fn build_nonroad_inputs<S: DataFrameStore + ?Sized>(
    store: &S,
    _analysis_year: i32,
) -> NonroadInputs {
    let units = load_source_units(store);
    let base = base_year(store);
    let mut by_scc: BTreeMap<String, Vec<DriverRecord>> = BTreeMap::new();
    for u in &units {
        by_scc.entry(u.scc.clone()).or_default().push(DriverRecord {
            region_code: PSEUDO_COUNTY.to_string(),
            hp_avg: u.hp_avg,
            population: u.population,
            // Base-year (e.g. 1990) population; the engine grows it to the
            // analysis/growth year via the growth records.
            pop_year: base,
            // Median life at full load drives scrptime's lifespan.
            median_life: u.median_life,
        });
    }

    let mut inputs = NonroadInputs::new();
    for (scc, records) in by_scc {
        inputs.push_group(scc, records);
    }
    inputs.regions = RunRegions {
        selected_counties: vec![PSEUDO_COUNTY.to_string()],
        ..RunRegions::default()
    };
    inputs
}

/// Build the activity table — one [`ActivityTableEntry`] per SCC. The
/// engine's `find_activity` matches by SCC (and FIPS) only, so a single
/// representative `(hoursUsedPerYear, loadFactor)` per SCC is used (the
/// first source unit seen). Activity actually varies by HP bin; a
/// per-HP-bin refinement is a follow-up.
pub fn build_activity_entries<S: DataFrameStore + ?Sized>(store: &S) -> Vec<ActivityTableEntry> {
    let units = load_source_units(store);
    let mut seen: BTreeMap<String, ActivityTableEntry> = BTreeMap::new();
    for u in units {
        seen.entry(u.scc.clone())
            .or_insert_with(|| ActivityTableEntry {
                scc: u.scc.clone(),
                fips: String::new(), // match any FIPS
                starts: 0.0,
                activity_level: u.hours_used_per_year,
                activity_unit: ActivityUnit::HoursPerYear,
                load_factor: u.load_factor,
                age_code: "DEFAULT".to_string(),
            });
    }
    seen.into_values().collect()
}

/// Fill the per-model-year tech fractions on each exhaust-tech entry from
/// `nrengtechfraction`, aligned to the entry's `tech_names` (engTechID)
/// ordering. Restricted to **processGroupID = 1 (EXHAUST)** — group 2 is
/// EVAP and uses different HP binning (canonical `NonroadDataFileHelper`
/// writes the two groups to separate tech files).
///
/// The base emission rates are model-year independent, but the tech mix
/// phases cleaner technology in over model years. Populates
/// `tech_fractions_by_year` (per model year) and sets the scalar
/// `tech_fractions` to the analysis-year mix as a fallback.
pub fn fill_tech_fractions<S: DataFrameStore + ?Sized>(
    entries: &mut [ExhaustTechEntry],
    store: &S,
    analysis_year: i32,
) {
    // (scc, hpMin, hpMax, modelYear) -> { engTechID -> fraction }, exhaust
    // process group only.
    let mut by_key: BTreeMap<(String, i64, i64, i64), BTreeMap<i64, f32>> = BTreeMap::new();
    if let Some(df) = store.get("nrengtechfraction") {
        let scc = str_col(&df, "SCC");
        let hp_min = int_col(&df, "hpMin");
        let hp_max = int_col(&df, "hpMax");
        let model_year = int_col(&df, "modelYearID");
        let tech = int_col(&df, "engTechID");
        let frac = float_col(&df, "NREngTechFraction");
        let pgroup = int_col(&df, "processGroupID");
        for i in 0..df.height() {
            if pgroup[i] != 1 {
                continue; // exhaust only
            }
            by_key
                .entry((scc[i].clone(), hp_min[i], hp_max[i], model_year[i]))
                .or_default()
                .insert(tech[i], frac[i] as f32);
        }
    }
    let present_sccs: std::collections::BTreeSet<&String> =
        by_key.keys().map(|(s, _, _, _)| s).collect();

    for e in entries.iter_mut() {
        let n_tech = e.tech_names.len();
        if n_tech == 0 {
            continue;
        }
        let tech_ids: Vec<Option<i64>> =
            e.tech_names.iter().map(|n| n.parse::<i64>().ok()).collect();
        // Tech fractions may be keyed at the family-root SCC.
        let eff_scc = if present_sccs.contains(&e.scc) {
            e.scc.clone()
        } else {
            family_root(&e.scc)
        };
        let (hp_min, hp_max) = (e.hp_min as i64, e.hp_max as i64);

        let mut by_year: BTreeMap<i32, Vec<f32>> = BTreeMap::new();
        for ((kscc, kmin, kmax, kmy), techmap) in &by_key {
            if *kscc != eff_scc || *kmin != hp_min || *kmax != hp_max {
                continue;
            }
            let v: Vec<f32> = tech_ids
                .iter()
                .map(|tid| tid.and_then(|t| techmap.get(&t)).copied().unwrap_or(0.0))
                .collect();
            if v.iter().any(|&f| f > 0.0) {
                by_year.insert(*kmy as i32, v);
            }
        }

        e.tech_fractions = by_year
            .range(..=analysis_year)
            .next_back()
            .or_else(|| by_year.iter().next_back())
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| {
                let mut d = vec![0.0_f32; n_tech];
                d[0] = 1.0;
                d
            });
        e.tech_fractions_by_year = by_year;
    }
}

/// The base year of the equipment population (`nrbaseyearequippopulation.
/// NRBaseYearID`). Defaults to 1990 when absent.
fn base_year<S: DataFrameStore + ?Sized>(store: &S) -> i32 {
    if let Some(df) = store.get("nrbaseyearequippopulation") {
        if let Some(&y) = int_col(&df, "NRBaseYearID").iter().find(|&&y| y > 0) {
            return y as i32;
        }
    }
    1990
}

/// Build the growth cross-reference (SCC → growth-pattern indicator) and
/// the growth-index records the engine uses to project the base-year
/// population to the analysis year (canonical `grwfac.f`). Indicator =
/// `growthPatternID` (stringified); records carry the per-year
/// `growthIndex` keyed by the pseudo-county FIPS.
fn build_growth<S: DataFrameStore + ?Sized>(
    store: &S,
) -> (BTreeMap<String, String>, Vec<GrowthIndicatorRecord>) {
    let mut scc_pattern: BTreeMap<String, String> = BTreeMap::new();
    if let Some(df) = store.get("nrgrowthpatternfinder") {
        let scc = str_col(&df, "SCC");
        let pat = int_col(&df, "growthPatternID");
        for i in 0..df.height() {
            scc_pattern.insert(scc[i].clone(), pat[i].to_string());
        }
    }
    let mut records = Vec::new();
    if let Some(df) = store.get("nrgrowthindex") {
        let pat = int_col(&df, "growthPatternID");
        let year = int_col(&df, "yearID");
        let idx = float_col(&df, "growthIndex");
        for i in 0..df.height() {
            records.push(GrowthIndicatorRecord {
                indicator: pat[i].to_string(),
                fips: PSEUDO_COUNTY.to_string(),
                subregion: String::new(),
                year: year[i] as i32,
                value: idx[i] as f32,
            });
        }
    }
    (scc_pattern, records)
}

/// Build the global scrappage curve from `nrscrappagecurve` — the default
/// `NREquipTypeID = 0` curve, matching canonical (`NonroadDataFileHelper`
/// writes only `WHERE NREquipTypeID = 0`; alternates are deferred). Falls
/// back to a degenerate 0→100% curve when absent so `scrptime` always has
/// points.
pub fn build_scrappage_curve<S: DataFrameStore + ?Sized>(store: &S) -> Vec<ScrappagePoint> {
    let Some(df) = store.get("nrscrappagecurve") else {
        return vec![
            ScrappagePoint {
                bin: 0.0,
                percent: 0.0,
            },
            ScrappagePoint {
                bin: 1.0,
                percent: 100.0,
            },
        ];
    };
    let equip = int_col(&df, "NREquipTypeID");
    let frac = float_col(&df, "fractionLifeUsed");
    let pct = float_col(&df, "percentageScrapped");
    // Default curve only (NREquipTypeID = 0); dedupe by fraction breakpoint.
    let mut acc: BTreeMap<i64, f64> = BTreeMap::new();
    for i in 0..df.height() {
        if equip[i] != 0 {
            continue;
        }
        let key = (frac[i] * 1.0e6).round() as i64;
        acc.insert(key, pct[i]);
    }
    let mut points: Vec<ScrappagePoint> = acc
        .into_iter()
        .map(|(k, pct)| ScrappagePoint {
            bin: (k as f64 / 1.0e6) as f32,
            percent: pct as f32,
        })
        .collect();
    if points.is_empty() {
        points = vec![
            ScrappagePoint {
                bin: 0.0,
                percent: 0.0,
            },
            ScrappagePoint {
                bin: 1.0,
                percent: 100.0,
            },
        ];
    }
    points
}

/// Assemble the full [`ReferenceData`] the [`ProductionExecutor`] needs
/// from the `nr*` tables. Growth, scrappage, and evap are left at their
/// neutral defaults for this first end-to-end pass (no growth, default
/// scrappage); the exhaust rate + activity + tech-fraction path is fully
/// populated.
pub fn load_nonroad_reference<S: DataFrameStore + ?Sized>(
    store: &S,
    analysis_year: i32,
) -> ReferenceData {
    let mut exhaust_tech_entries = build_exhaust_tech_entries(store);
    fill_tech_fractions(&mut exhaust_tech_entries, store, analysis_year);
    let activity_entries = build_activity_entries(store);

    // The county routine requires an *evap* tech lookup to succeed before
    // it computes exhaust (it skips the record otherwise). We don't yet
    // compute evaporative emissions, so mirror each exhaust (SCC, HP bin)
    // with a zero-fraction evap entry: the lookup succeeds and the per-tech
    // loop contributes nothing.
    let evap_tech_entries = exhaust_tech_entries
        .iter()
        .map(|e| EvapTechEntry {
            scc: e.scc.clone(),
            hp_min: e.hp_min,
            hp_max: e.hp_max,
            tech_names: vec!["1".to_string()],
            tech_fractions: vec![0.0],
        })
        .collect();

    // Growth cross-reference per (SCC, HP bin): indicator = the SCC's
    // growth-pattern id (most-specific match). Unmatched SCCs get "DEF",
    // which selects no growth record ⇒ no growth for that SCC.
    let (scc_pattern, growth_records) = build_growth(store);
    let growth_xref_entries = exhaust_tech_entries
        .iter()
        .map(|e| moves_nonroad::simulation::GrowthXrefEntry {
            fips: PSEUDO_COUNTY.to_string(),
            scc: e.scc.clone(),
            hp_min: e.hp_min,
            hp_max: e.hp_max,
            indicator: scc_lookup(&scc_pattern, &e.scc)
                .cloned()
                .unwrap_or_else(|| "DEF".to_string()),
        })
        .collect();

    ReferenceData {
        exhaust_tech_entries,
        evap_tech_entries,
        activity_entries,
        growth_xref_entries,
        growth_records,
        scrappage_curve: build_scrappage_curve(store),
        age_adjustment_table: AgeAdjustmentTable::default(),
        ..ReferenceData::default()
    }
}

/// Build the [`ProductionExecutor`] for the national pseudo-county run.
pub fn build_production_executor<S: DataFrameStore + ?Sized>(
    store: &S,
    analysis_year: i32,
) -> ProductionExecutor {
    let reference = load_nonroad_reference(store, analysis_year);
    ProductionExecutor {
        county_fips: vec![PSEUDO_COUNTY.to_string()],
        hp_levels: HP_LEVELS,
        reference,
        ..ProductionExecutor::default()
    }
}

/// Build the [`NonroadOptions`] for a county-level (national pseudo-county)
/// run at `analysis_year`.
pub fn build_options(analysis_year: i32) -> NonroadOptions {
    let mut opts = NonroadOptions::new(RegionLevel::County, analysis_year);
    opts.growth_loaded = true;
    opts
}

/// Time-dimension keys stamped onto every emitted output row, taken from
/// the master-loop iteration position.
pub struct EmissionTimeKeys {
    pub year: i32,
    pub month: Option<i32>,
    pub day: Option<i32>,
    pub hour: Option<i32>,
}

/// Grams per short ton — the inverse of the engine's `CVTTON`
/// (`1.102311e-6` short-tons per gram). Engine `SimEmissionRow.emissions`
/// are short tons; MOVESOutput `emissionQuant` is grams.
const GRAMS_PER_SHORT_TON: f64 = 1.0 / 1.102_311e-6;

/// Engine pollutant slot → MOVES `pollutantID` for the emitted nonroad
/// exhaust pollutants (THC, CO, NOx, PM10).
const SLOT_POLLUTANT: [(usize, i32); 4] = [(0, 1), (1, 2), (2, 3), (5, 100)];

/// Days in a (non-leap) calendar month — NONROAD's `modays`, used as the
/// typical-day divisor.
fn days_in_month(month: i32) -> f64 {
    match month {
        2 => 28.0,
        4 | 6 | 9 | 11 => 30.0,
        _ => 31.0,
    }
}

/// Per-SCC temporal allocation factor that converts the engine's *annual*
/// emissions to the runspec's typical-day slice, exactly per canonical
/// NONROAD: `factor = monthFraction × dayf ÷ ndays`, where
/// `dayf = 7 × dayFraction` (`daymthf.f:177`), `ndays` = days in the month
/// (`adjtime = 1/ndays`, `prccty.f:304`). So
/// `factor = monthFraction × dayFraction × 7 / ndays`.
///
/// `monthFraction` is keyed `(SCC, stateID, monthID)` in `nrmonthallocation`;
/// `dayFraction` is keyed `(scc, dayID)` in `nrdayallocation` (lowercase
/// column). Missing dimensions default to 1.0 (family-root fallback first).
pub fn build_temporal_factors<S: DataFrameStore + ?Sized>(
    store: &S,
    month: i32,
    day: i32,
) -> BTreeMap<String, f64> {
    let mut month_by_scc: BTreeMap<String, f64> = BTreeMap::new();
    if let Some(df) = store.get("nrmonthallocation") {
        let scc = str_col(&df, "SCC");
        let m = int_col(&df, "monthID");
        let f = float_col(&df, "monthFraction");
        for i in 0..df.height() {
            if m[i] == month as i64 {
                month_by_scc.insert(scc[i].clone(), f[i]);
            }
        }
    }
    let mut day_by_scc: BTreeMap<String, f64> = BTreeMap::new();
    if let Some(df) = store.get("nrdayallocation") {
        let scc = str_col(&df, "scc");
        let d = int_col(&df, "dayID");
        let f = float_col(&df, "dayFraction");
        for i in 0..df.height() {
            if d[i] == day as i64 {
                day_by_scc.insert(scc[i].clone(), f[i]);
            }
        }
    }

    let lookup = |map: &BTreeMap<String, f64>, scc: &str| -> f64 {
        map.get(scc)
            .or_else(|| map.get(&family_root(scc)))
            .copied()
            .unwrap_or(1.0)
    };

    let ndays = days_in_month(month);
    let mut factors = BTreeMap::new();
    let sccs: std::collections::BTreeSet<&String> =
        month_by_scc.keys().chain(day_by_scc.keys()).collect();
    for scc in sccs {
        // monthFraction × (7 × dayFraction) ÷ ndays  (canonical typical-day).
        let f = lookup(&month_by_scc, scc) * (7.0 * lookup(&day_by_scc, scc)) / ndays;
        factors.insert(scc.clone(), f);
    }
    factors
}

/// Convert the engine's [`SimEmissionRow`]s into a MOVESOutput-shaped
/// DataFrame the framework's `frame_to_emission_records` consumes.
///
/// Emits one row per `(SimEmissionRow, emitted pollutant)` with non-zero
/// emissions, converting short tons → grams and applying the per-SCC
/// `temporal` allocation factor (annual → the runspec month/day slice;
/// pass an empty map for no allocation). Returns `Ok(None)` when no
/// non-zero emissions were produced. Integer columns are `i32` (the
/// physical type the framework reads via `.i32()`).
pub fn emissions_to_dataframe(
    rows: &[SimEmissionRow],
    keys: &EmissionTimeKeys,
    temporal: &BTreeMap<String, f64>,
) -> PolarsResult<Option<DataFrame>> {
    let mut year = Vec::new();
    let mut month = Vec::new();
    let mut day = Vec::new();
    let mut hour = Vec::new();
    let mut pollutant = Vec::new();
    let mut process = Vec::new();
    let mut quant = Vec::new();

    for row in rows {
        let tfac = scc_lookup(temporal, &row.scc).copied().unwrap_or(1.0);
        for (slot, pid) in SLOT_POLLUTANT {
            let e = row.emissions.get(slot).copied().unwrap_or(0.0);
            if e == 0.0 {
                continue;
            }
            year.push(keys.year);
            month.push(keys.month.unwrap_or(0));
            day.push(keys.day.unwrap_or(0));
            hour.push(keys.hour.unwrap_or(0));
            pollutant.push(pid);
            process.push(1_i32); // nonroad emission process
            quant.push(e as f64 * GRAMS_PER_SHORT_TON * tfac);
        }
    }

    if quant.is_empty() {
        return Ok(None);
    }
    let df = df!(
        "yearID" => year,
        "monthID" => month,
        "dayID" => day,
        "hourID" => hour,
        "pollutantID" => pollutant,
        "processID" => process,
        "emissionQuant" => quant,
    )?;
    Ok(Some(df))
}

#[cfg(test)]
mod tests {
    use super::*;
    use moves_framework::data::InMemoryStore;

    /// Diagnostic against the real nr-commercial-nation snapshot. Ignored
    /// by default (depends on the checked-in Parquet snapshot); run with
    /// `cargo test -p moves-calculators diag_snapshot -- --ignored --nocapture`.
    #[test]
    #[ignore]
    fn diag_snapshot_exhaust_coverage() {
        use std::fs;
        use std::path::PathBuf;
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../characterization/snapshots/nr-commercial-nation/tables");
        if !dir.exists() {
            eprintln!("snapshot not present at {dir:?}; skipping diagnostic");
            return;
        }
        let load = |table: &str| -> Option<DataFrame> {
            let entry = fs::read_dir(&dir).ok()?.filter_map(|e| e.ok()).find(|e| {
                e.file_name()
                    .to_string_lossy()
                    .to_ascii_lowercase()
                    .ends_with(&format!("__{}.parquet", table))
            })?;
            let file = std::fs::File::open(entry.path()).ok()?;
            ParquetReader::new(file).finish().ok()
        };
        let mut store = InMemoryStore::new();
        for t in [
            "nremissionrate",
            "nrdeterioration",
            "nrengtechfraction",
            "nrsourceusetype",
            "nrbaseyearequippopulation",
            "nrscrappagecurve",
            "nrmonthallocation",
            "nrdayallocation",
            "runspecsector",
            "runspecfueltype",
            "nrequipmenttype",
            "nrscc",
            "nrgrowthpatternfinder",
            "nrgrowthindex",
        ] {
            if let Some(df) = load(t) {
                store.insert(t, df);
            } else {
                eprintln!("MISSING table {t}");
            }
        }
        let entries = build_exhaust_tech_entries(&store);
        eprintln!("exhaust_tech_entries = {}", entries.len());
        for e in entries.iter().take(5) {
            eprintln!(
                "  entry scc={} hp=[{},{}] techs={:?} ef0={:?}",
                e.scc,
                e.hp_min,
                e.hp_max,
                e.tech_names,
                &e.emission_factors.iter().take(3).collect::<Vec<_>>()
            );
        }
        let inputs = build_nonroad_inputs(&store, 2020);
        eprintln!(
            "inputs groups={} records={}",
            inputs.group_count(),
            inputs.record_count()
        );
        // For the first few driver records, does an entry match by SCC+hp?
        let mut matched = 0;
        let mut total = 0;
        for g in &inputs.scc_groups {
            for r in &g.records {
                total += 1;
                let hit = entries
                    .iter()
                    .any(|e| e.scc == g.scc && e.hp_min <= r.hp_avg && r.hp_avg <= e.hp_max);
                if hit {
                    matched += 1;
                } else if matched + (total - matched) <= 8 {
                    eprintln!("  NO MATCH scc={} hp_avg={}", g.scc, r.hp_avg);
                }
            }
        }
        eprintln!("driver records matched-by-exhaust-entry: {matched}/{total}");

        // Reproduce the engine run in-process.
        let options = build_options(2020);
        let mut executor = build_production_executor(&store, 2020);
        eprintln!("executor.county_fips = {:?}", executor.county_fips);
        eprintln!(
            "inputs.regions.selected_counties = {:?}",
            inputs.regions.selected_counties
        );
        if let Some(g) = inputs.scc_groups.first() {
            if let Some(r) = g.records.first() {
                eprintln!(
                    "first record: scc={} region_code={:?} hp_avg={} pop={}",
                    g.scc, r.region_code, r.hp_avg, r.population
                );
            }
        }
        // Growth diagnostics: is growth actually matching/applying?
        eprintln!(
            "growth_records={} growth_xref[0].indicator={:?} options(epi={},grw={},tech={})",
            executor.reference.growth_records.len(),
            executor
                .reference
                .growth_xref_entries
                .first()
                .map(|e| &e.indicator),
            options.episode_year,
            options.growth_year,
            options.tech_year,
        );
        {
            use moves_nonroad::population::growth::{growth_factor, select_for_indicator};
            // 2265006030 -> growthPatternID 1063.
            let recs = select_for_indicator(&executor.reference.growth_records, "1063");
            match growth_factor(&recs, 1990, 2020, "00001") {
                Ok(gf) => eprintln!(
                    "growth_factor(1063,1990->2020,00001): annualized={:.5} base_ind={:.1} grow_ind={:.1}",
                    gf.factor, gf.base_indicator, gf.growth_indicator
                ),
                Err(e) => eprintln!("growth_factor ERR: {e:?}"),
            }
        }
        // Sample exhaust entry: nyrlif span proxy + per-MY tech presence.
        if let Some(e) = executor
            .reference
            .exhaust_tech_entries
            .iter()
            .find(|e| e.scc == "2265006030")
        {
            eprintln!(
                "entry 2265006030 hp=[{},{}] tech_ids={:?} per_my_years={}",
                e.hp_min,
                e.hp_max,
                e.tech_names,
                e.tech_fractions_by_year.len(),
            );
            for (yr, v) in &e.tech_fractions_by_year {
                eprintln!("   MY {yr}: sum={:.3} {:?}", v.iter().sum::<f32>(), v);
            }
        }
        let out = moves_nonroad::run_simulation(&options, &inputs, &mut executor).unwrap();
        eprintln!("sim counters = {:?}", out.counters);
        eprintln!("sim rows = {}", out.rows.len());
        let out_pop: f64 = out.rows.iter().map(|r| r.population as f64).sum();
        let in_pop: f64 = inputs
            .scc_groups
            .iter()
            .flat_map(|g| g.records.iter())
            .map(|r| r.population as f64)
            .sum();
        eprintln!(
            "POP in(base)={in_pop:.3e} out={out_pop:.3e} ratio={:.3}",
            out_pop / in_pop
        );
        let nonzero = out
            .rows
            .iter()
            .filter(|r| r.emissions.iter().any(|&e| e != 0.0))
            .count();
        eprintln!("nonzero rows = {nonzero}");
        let temporal = build_temporal_factors(&store, 8, 5);
        let g = 1.0 / 1.102_311e-6; // short tons -> grams
        let mut tot = [0.0f64; 4];
        for r in &out.rows {
            let tf = scc_lookup(&temporal, &r.scc).copied().unwrap_or(1.0);
            tot[0] += r.emissions[0] as f64 * tf;
            tot[1] += r.emissions[1] as f64 * tf;
            tot[2] += r.emissions[2] as f64 * tf;
            tot[3] += r.emissions[5] as f64 * tf;
        }
        eprintln!(
            "TOTALS grams: THC={:.3e} CO={:.3e} NOx={:.3e} PM={:.3e}",
            tot[0] * g,
            tot[1] * g,
            tot[2] * g,
            tot[3] * g
        );
        eprintln!("canonical     grams: THC=1.414e8 CO=6.508e9 NOx=4.947e7 PM=7.818e6");

        // Per-SCC NOx breakdown with fuel type, to test whether NOx is
        // dominated by diesel commercial equipment (canonical output is
        // gasoline-only, fuelTypeID=1).
        let fuel_df = load("nrscc").unwrap();
        let fscc = str_col(&fuel_df, "SCC");
        let ftype = int_col(&fuel_df, "fuelTypeID");
        let fuel_of: BTreeMap<String, i64> = fscc.into_iter().zip(ftype).collect();
        let mut per_scc: BTreeMap<String, (f64, f64, i64)> = BTreeMap::new(); // scc -> (nox, co, fuel)
        for r in &out.rows {
            let tf = scc_lookup(&temporal, &r.scc).copied().unwrap_or(1.0);
            let fuel = fuel_of.get(&r.scc).copied().unwrap_or(0);
            let e = per_scc.entry(r.scc.clone()).or_insert((0.0, 0.0, fuel));
            e.0 += r.emissions[2] as f64 * tf * g;
            e.1 += r.emissions[1] as f64 * tf * g;
        }
        // Per-SCC ratio vs canonical (CO, NOx) — is the under-prediction
        // uniform (global per-unit factor) or distributional?
        let can_co: BTreeMap<&str, f64> = [
            ("2260006005", 1.3237e7),
            ("2260006010", 8.5710e7),
            ("2260006015", 3.7159e4),
            ("2260006035", 5.7706e5),
            ("2265006005", 3.2478e9),
            ("2265006010", 6.4285e8),
            ("2265006015", 3.0661e8),
            ("2265006025", 8.4463e8),
            ("2265006030", 1.2998e9),
            ("2265006035", 6.6452e7),
        ]
        .into_iter()
        .collect();
        let can_nox: BTreeMap<&str, f64> = [
            ("2260006005", 1.4337e5),
            ("2260006010", 9.7700e5),
            ("2260006015", 3.7905e2),
            ("2260006035", 5.8865e3),
            ("2265006005", 2.2496e7),
            ("2265006010", 6.0662e6),
            ("2265006015", 3.0281e6),
            ("2265006025", 6.4103e6),
            ("2265006030", 9.8728e6),
            ("2265006035", 4.7077e5),
        ]
        .into_iter()
        .collect();
        let mut rows: Vec<_> = per_scc.into_iter().collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        eprintln!("per-SCC: scc  CO mine/canon   NOx mine/canon");
        for (scc, (nox, co, _)) in &rows {
            let cc = can_co.get(scc.as_str()).copied().unwrap_or(f64::NAN);
            let cn = can_nox.get(scc.as_str()).copied().unwrap_or(f64::NAN);
            eprintln!("  {scc}  CO {:.3}   NOx {:.3}", co / cc, nox / cn);
        }
    }

    /// Build a tiny in-memory store mimicking the nr* rate tables.
    fn store_with(emission: DataFrame, deterioration: Option<DataFrame>) -> InMemoryStore {
        let mut store = InMemoryStore::new();
        store.insert("nremissionrate", emission);
        if let Some(d) = deterioration {
            store.insert("nrdeterioration", d);
        }
        store
    }

    /// Emission rates stored as strings (the snapshot's physical form),
    /// keys as int64 — the loader must parse both.
    fn emission_df() -> DataFrame {
        df!(
            "polProcessID" => [101i64, 201, 301, 10001, 9901, 101],
            "SCC" => ["2260000000", "2260000000", "2260000000", "2260000000", "2260000000", "2260000000"],
            "hpMin" => [0i64, 0, 0, 0, 0, 1],
            "hpMax" => [1i64, 1, 1, 1, 1, 3],
            "modelYearID" => [1900i64, 1900, 1900, 1900, 1900, 1900],
            "engTechID" => [105i64, 105, 105, 105, 105, 112],
            "meanBaseRate" => ["261.000000", "733.000000", "4.500000", "1.250000", "0.660000", "180.000000"],
            "units" => ["g/hp-hr", "g/hp-hr", "g/hp-hr", "g/hp-hr", "", "g/hp-hr"],
        )
        .unwrap()
    }

    fn deterioration_df() -> DataFrame {
        df!(
            "polProcessID" => [101i64, 201, 301, 10001],
            "engTechID" => [105i64, 105, 105, 105],
            "DFCoefficient" => ["0.201000", "0.101000", "0.024000", "0.473000"],
            "DFAgeExponent" => ["1.000000", "1.000000", "1.000000", "1.000000"],
            "emissionCap" => [1i64, 1, 1, 1],
        )
        .unwrap()
    }

    #[test]
    fn reads_base_rate_into_thc_slot() {
        let store = store_with(emission_df(), Some(deterioration_df()));
        let entries = build_exhaust_tech_entries(&store);
        // Two HP bins -> two entries.
        assert_eq!(entries.len(), 2);

        let bin0 = entries
            .iter()
            .find(|e| e.hp_min == 0.0 && e.hp_max == 1.0)
            .expect("hp (0,1] entry");
        // One engine tech (105).
        assert_eq!(bin0.tech_names, vec!["105".to_string()]);
        let n_tech = 1;
        // THC (slot 0) base rate.
        assert_eq!(bin0.emission_factors[0 * n_tech], 261.0);
        // CO (slot 1), NOx (slot 2), PM (slot 5).
        assert_eq!(bin0.emission_factors[1 * n_tech], 733.0);
        assert_eq!(bin0.emission_factors[2 * n_tech], 4.5);
        assert_eq!(bin0.emission_factors[5 * n_tech], 1.25);
        // BSFC (polProcess 9901) lands in the per-tech bsfc array, not EF.
        assert_eq!(bin0.bsfc, vec![0.66_f32]);
        // Units parsed.
        assert_eq!(bin0.emission_units[0], EmissionUnitCode::GramsPerHpHour);
    }

    #[test]
    fn attaches_deterioration_by_polprocess_and_tech() {
        let store = store_with(emission_df(), Some(deterioration_df()));
        let entries = build_exhaust_tech_entries(&store);
        let bin0 = entries
            .iter()
            .find(|e| e.hp_min == 0.0 && e.hp_max == 1.0)
            .unwrap();
        // THC deterioration A = 0.201, B = 1.0, cap = 1.
        assert!((bin0.det_a[0] - 0.201).abs() < 1e-6);
        assert!((bin0.det_b[0] - 1.0).abs() < 1e-6);
        assert!((bin0.det_cap[0] - 1.0).abs() < 1e-6);
        // NOx deterioration A = 0.024 at slot 2.
        assert!((bin0.det_a[2] - 0.024).abs() < 1e-6);
    }

    #[test]
    fn second_hp_bin_is_isolated() {
        let store = store_with(emission_df(), None);
        let entries = build_exhaust_tech_entries(&store);
        let bin1 = entries
            .iter()
            .find(|e| e.hp_min == 1.0 && e.hp_max == 3.0)
            .expect("hp (1,3] entry");
        assert_eq!(bin1.tech_names, vec!["112".to_string()]);
        assert_eq!(bin1.emission_factors[0], 180.0);
        // No deterioration table -> zeros (no deterioration).
        assert_eq!(bin1.det_a[0], 0.0);
    }

    #[test]
    fn missing_table_yields_no_entries() {
        let store = InMemoryStore::new();
        assert!(build_exhaust_tech_entries(&store).is_empty());
    }
}

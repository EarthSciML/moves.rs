//! Data-plane loader: builds the `moves-nonroad` engine's reference
//! tables from the MOVES `nr*` execution-DB tables.
//!
//! /. In canonical MOVES the `NonroadEmissionCalculator`
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
use moves_nonroad::common::consts::{MXEVTECH, MXHPC, MXPOL, MXTECH};
use moves_nonroad::driver::{DriverRecord, RegionLevel, RunRegions};
use moves_nonroad::emissions::exhaust::EmissionUnitCode;
use moves_nonroad::geography::common::ActivityUnit;
use moves_nonroad::input::scrappage::ScrappagePoint;
use moves_nonroad::population::{AgeAdjustmentTable, GrowthIndicatorRecord};
use moves_nonroad::simulation::{
    ActivityTableEntry, EvapTechEntry, ExhaustTechEntry, NonroadInputs, NonroadOptions,
    ProductionExecutor, ReferenceData, SimEmissionRow, TemporalProfile,
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
/// integer physical type or a string-encoded integer.
///
/// A structurally *missing* column is a schema/naming mismatch, not a data
/// value: these columns (`polProcessID`, `engTechID`, `sourceTypeID`,
/// `hpMin`/`hpMax`, `modelYearID`, `monthID`, …) are mandatory join/lookup
/// keys. Silently substituting `0` would collapse every key and mis-build the
/// reference tables (e.g. every row maps to `sourceTypeID 0`). So a missing
/// column panics rather than zeroing the key. Individual unparseable *cells*
/// still tolerate the snapshot's mixed numeric/string-decimal storage and fall
/// back to `0`.
fn int_col(df: &DataFrame, name: &str) -> Vec<i64> {
    let Some(actual) = resolve(df, name) else {
        panic!(
            "required integer column {name:?} is absent from the snapshot table \
             (columns present: {:?}); this is a schema/naming mismatch, not a \
             zero value",
            df.get_column_names()
        );
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
///
/// A structurally *missing* column is a schema/naming mismatch, not a `0.0`
/// value: these columns carry every numeric quantity the engine consumes
/// (`meanBaseRate`, `population`, deterioration coefficients, `marketShare`,
/// `monthFraction`/`dayFraction`, `hpAvg`, `loadFactor`, `temperature`). A
/// silent `0.0` rate or `0.0` population would zero emissions for that
/// equipment with no diagnostic — canonical NONROAD treats a missing rate in a
/// fixed-format input file as a hard error. So a missing column panics rather
/// than fabricating zeros. Individual unparseable *cells* still fall back to
/// `0.0` to tolerate the snapshot's mixed numeric/string-decimal storage.
fn float_col(df: &DataFrame, name: &str) -> Vec<f64> {
    let Some(actual) = resolve(df, name) else {
        panic!(
            "required numeric column {name:?} is absent from the snapshot table \
             (columns present: {:?}); this is a schema/naming mismatch, not a \
             zero value",
            df.get_column_names()
        );
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
/// [`EmissionUnitCode`].
///
/// The units field selects the activity basis (HP-hours vs gallons vs hours
/// vs starts vs a multiplier), so a blank or unrecognized value is *not* a
/// safe default — canonical NONROAD aborts on it. `rdemfc.f:159-160` looks the
/// units keyword up via `fndchr`; an unmatched keyword falls through to error
/// label 7005 ("Missing or invalid tech type or units type"). We mirror that
/// hard failure rather than silently assuming `g/HP-hr` and applying the wrong
/// activity multiplier.
fn unit_code_for(units: &str) -> EmissionUnitCode {
    match units.trim().to_ascii_lowercase().as_str() {
        "g/hp-hr" | "g/hphr" => EmissionUnitCode::GramsPerHpHour,
        "g/gallon" | "g/gal" => EmissionUnitCode::GramsPerGallon,
        "g/hr" => EmissionUnitCode::GramsPerHour,
        "g/day" => EmissionUnitCode::GramsPerDay,
        "g/start" => EmissionUnitCode::GramsPerStart,
        "mult" => EmissionUnitCode::Multiplier,
        // Evap permeation species: grams per square metre per day.
        "g/m2/day" => EmissionUnitCode::GramsPerM2Day,
        other => panic!(
            "nremissionrate: missing or invalid emission-rate units {other:?} \
             (canonical rdemfc.f errors on an unrecognized units keyword); \
             the units field determines the activity basis and cannot be defaulted"
        ),
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

/// hp-binned rate entries `[(hp_min, hp_max, (rate, unit))]` for one pollutant slot.
type HpBinnedRates = Vec<(i64, i64, (f32, EmissionUnitCode))>;

/// Per-`(SCC, engTechID)` emission rates, hp-binned. The `.EMF` side of the
/// canonical model — looked up by hp containment, with SCC family-root
/// fallback applied by the caller.
#[derive(Default, Clone)]
struct TechRate {
    /// `pollutant_slot -> [(hp_min, hp_max, (rate, unit))]`.
    by_pollutant: BTreeMap<usize, HpBinnedRates>,
    /// `[(hp_min, hp_max, bsfc)]` from polProcessID 9901.
    bsfc: Vec<(i64, i64, f32)>,
}

/// Read `nremissionrate` into `(SCC, engTechID) -> TechRate` (the `.EMF`).
fn load_rate_lookup<S: DataFrameStore + ?Sized>(store: &S) -> BTreeMap<(String, i64), TechRate> {
    let mut map: BTreeMap<(String, i64), TechRate> = BTreeMap::new();
    let Some(df) = store.get("nremissionrate") else {
        return map;
    };
    let scc = str_col(&df, "SCC");
    let pp = int_col(&df, "polProcessID");
    let hmin = int_col(&df, "hpMin");
    let hmax = int_col(&df, "hpMax");
    let tech = int_col(&df, "engTechID");
    let rate = float_col(&df, "meanBaseRate");
    let units = str_col(&df, "units");
    for i in 0..df.height() {
        let e = map.entry((scc[i].clone(), tech[i])).or_default();
        if pp[i] == PP_BSFC {
            e.bsfc.push((hmin[i], hmax[i], rate[i] as f32));
            continue;
        }
        if let Some(ps) = pollutant_slot_for(pp[i]) {
            e.by_pollutant.entry(ps).or_default().push((
                hmin[i],
                hmax[i],
                (rate[i] as f32, unit_code_for(&units[i])),
            ));
        }
    }
    map
}

/// Read `nrengtechfraction` (processGroupID = 1, exhaust) into
/// `(SCC, hpMin, hpMax) -> { engTechID -> { modelYear -> fraction } }` — the
/// `.TECH` side, which defines the per-model-year tech mix.
type TechMix = BTreeMap<(String, i64, i64), BTreeMap<i64, BTreeMap<i64, f32>>>;
fn load_tech_mix<S: DataFrameStore + ?Sized>(store: &S) -> TechMix {
    let mut map = TechMix::new();
    let Some(df) = store.get("nrengtechfraction") else {
        return map;
    };
    let scc = str_col(&df, "SCC");
    let hmin = int_col(&df, "hpMin");
    let hmax = int_col(&df, "hpMax");
    let my = int_col(&df, "modelYearID");
    let tech = int_col(&df, "engTechID");
    let frac = float_col(&df, "NREngTechFraction");
    let pg = int_col(&df, "processGroupID");
    for i in 0..df.height() {
        if pg[i] != 1 {
            continue;
        }
        map.entry((scc[i].clone(), hmin[i], hmax[i]))
            .or_default()
            .entry(tech[i])
            .or_default()
            .insert(my[i], frac[i] as f32);
    }
    map
}

/// First payload whose `(hp_min, hp_max)` bin contains `hp`.
fn hp_pick<T>(bins: &[(i64, i64, T)], hp: f32) -> Option<&T> {
    bins.iter()
        .find(|(lo, hi, _)| (*lo as f32) <= hp && hp <= (*hi as f32))
        .map(|(_, _, v)| v)
}

/// Build exhaust-tech entries the canonical way: the tech *mix* comes from
/// `nrengtechfraction` (the `.TECH` file, per model year), and each tech's
/// emission factor is looked up from `nremissionrate` (the `.EMF` file) by
/// engTechID with **independent hp binning** and SCC family-root fallback.
/// One entry per `(SCC, equipment hpAvg)` from `nrsourceusetype`, with a
/// point HP range so `find_exhaust_tech(scc, hpAvg)` matches it exactly.
///
/// This reproduces NONROAD's two-separate-files model, which the older
/// rate-driven builder could not: for specific-rate SCCs the rate and
/// tech-fraction tables use different hp bins and engTechID sets, so a
/// rate-keyed tech list dropped the mix techs entirely.
fn build_entries_from_mix<S: DataFrameStore + ?Sized>(store: &S) -> Vec<ExhaustTechEntry> {
    let mix = load_tech_mix(store);
    if mix.is_empty() {
        return Vec::new();
    }
    let rates = load_rate_lookup(store);
    let det = load_deterioration(store);

    let Some(sut) = store.get("nrsourceusetype") else {
        return Vec::new();
    };
    // Distinct (SCC, hpAvg) equipment points.
    let su_scc = str_col(&sut, "SCC");
    let su_hp = float_col(&sut, "hpAvg");
    let mut pairs: std::collections::BTreeSet<(String, i64)> = std::collections::BTreeSet::new();
    for i in 0..sut.height() {
        pairs.insert((su_scc[i].clone(), (su_hp[i] * 1.0e3).round() as i64));
    }

    let mut entries = Vec::new();
    for (scc, hp_milli) in pairs {
        let hp_avg = hp_milli as f32 / 1.0e3;
        // Canonical SCC fallback chain (`fndtch`/`fndefc` `ascglb`): exact,
        // then the 7-digit equipment root (`xxxxxxx000`), then the 4-digit
        // family root (`xxxx000000`). Mixes and rates key at DIFFERENT
        // levels per SCC — diesel lawn&garden rates live at 2270004000
        // while their tech mix lives at 2270000000 — so each lookup must
        // walk the full chain independently.
        let equip = equip_root(&scc);
        let root = family_root(&scc);
        let chain: [&String; 3] = [&scc, &equip, &root];
        // The tech-mix hp bin containing hp_avg. NONROAD's .TECH lookup uses
        // the most-specific SCC that has a bin covering this hp, then falls
        // back to the root (default) tech files. A specific SCC's tech
        // rows need not span every hp bin its population uses: e.g.
        // 2265006010 has tech rows only for 0-25 hp, but its population
        // extends to 175 hp — those high-hp points are served by the
        // 2265000000 root's 25-9999 bin. Without the root fallback those
        // long-lived (~20 yr) high-hp points are dropped, truncating the
        // model-year span (and losing ~30 old model years for that SCC).
        let find_bin = |target: &str| {
            mix.iter().find(|((s, lo, hi), _)| {
                s.as_str() == target && (*lo as f32) <= hp_avg && hp_avg <= (*hi as f32)
            })
        };
        let found = chain.iter().find_map(|s| find_bin(s));
        let Some((_, tech_map)) = found else {
            continue;
        };

        let mut tech_ids: Vec<i64> = tech_map.keys().copied().collect();
        tech_ids.truncate(MXTECH);
        let n_tech = tech_ids.len().max(1);

        let mut emission_factors = vec![0.0_f32; MXPOL * n_tech];
        let mut emission_units = vec![EmissionUnitCode::GramsPerHpHour; MXPOL * n_tech];
        let mut det_a = vec![0.0_f32; MXPOL * n_tech];
        let mut det_b = vec![0.0_f32; MXPOL * n_tech];
        let mut det_cap = vec![0.0_f32; MXPOL * n_tech];
        let mut bsfc = vec![0.0_f32; n_tech];
        let mut by_year: BTreeMap<i32, Vec<f32>> = BTreeMap::new();

        for (t, &tid) in tech_ids.iter().enumerate() {
            // BSFC: rate lookup over the canonical SCC fallback chain.
            bsfc[t] = chain
                .iter()
                .find_map(|s| {
                    rates
                        .get(&((*s).clone(), tid))
                        .and_then(|tr| hp_pick(&tr.bsfc, hp_avg).copied())
                })
                .unwrap_or(0.0);

            for (pslot, pp_for) in [(0, PP_THC), (1, PP_CO), (2, PP_NOX), (5, PP_PM)] {
                let picked = chain.iter().find_map(|s| {
                    rates
                        .get(&((*s).clone(), tid))
                        .and_then(|tr| tr.by_pollutant.get(&pslot))
                        .and_then(|v| hp_pick(v, hp_avg).copied())
                });
                let idx = pslot * n_tech + t;
                if let Some((r, u)) = picked {
                    emission_factors[idx] = r;
                    emission_units[idx] = u;
                }
                if let Some(&(a, b, c)) = det.get(&(pp_for, tid)) {
                    det_a[idx] = a;
                    det_b[idx] = b;
                    det_cap[idx] = c;
                }
            }

            // Per-model-year fractions for this tech.
            for (&yr, &f) in &tech_map[&tid] {
                by_year
                    .entry(yr as i32)
                    .or_insert_with(|| vec![0.0_f32; n_tech])[t] = f;
            }
        }

        // Scalar fallback = the latest model-year mix.
        let tech_fractions = by_year
            .iter()
            .next_back()
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| {
                let mut d = vec![0.0_f32; n_tech];
                d[0] = 1.0;
                d
            });

        entries.push(ExhaustTechEntry {
            scc,
            hp_min: hp_avg,
            hp_max: hp_avg,
            tech_names: tech_ids.iter().map(|t| t.to_string()).collect(),
            tech_fractions,
            bsfc,
            emission_factors,
            emission_units,
            det_a,
            det_b,
            det_cap,
            tech_fractions_by_year: by_year,
        });
    }
    entries
}

/// Build the per-`(SCC, HP-bin)` exhaust-tech entries — emission factors,
/// units, deterioration, and BSFC. Prefers the canonical
/// `nrengtechfraction`-driven builder (`build_entries_from_mix`); falls
/// back to the legacy rate-driven path when the tech-mix / source-use-type
/// tables are absent (e.g. unit tests with synthetic rate tables only).
pub fn build_exhaust_tech_entries<S: DataFrameStore + ?Sized>(store: &S) -> Vec<ExhaustTechEntry> {
    let from_mix = build_entries_from_mix(store);
    if !from_mix.is_empty() {
        return from_mix;
    }
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

// =============================================================================
// Evaporative tech entries and emission rates
// =============================================================================

/// Map a nonroad `polProcessID` from `nrevapemissionrate` to the engine's
/// 0-based evap-species slot (`EvapSpecies::slot()`).
///
/// `polProcessID = pollutantID × 100 + processID`. The evap process IDs in
/// the MOVES database and their corresponding `EvapSpecies` slots are:
///
/// | processID | Species               | Slot |
/// |-----------|----------------------|------|
/// | 20        | TankPermeation       | 8    |
/// | 21        | HosePermeation       | 9    |
/// | 22        | NeckPermeation       | 10   |
/// | 23        | SupplyReturnPermeation | 11 |
/// | 24        | VentPermeation       | 12   |
/// | 30        | Diurnal              | 7    |
/// | 31        | HotSoak              | 13   |
/// | 32        | RunningLoss          | 16   |
fn evap_pollutant_slot_for(pol_process_id: i64) -> Option<usize> {
    match pol_process_id % 100 {
        20 => Some(8),  // TankPermeation
        21 => Some(9),  // HosePermeation
        22 => Some(10), // NeckPermeation
        23 => Some(11), // SupplyReturnPermeation
        24 => Some(12), // VentPermeation
        30 => Some(7),  // Diurnal
        31 => Some(13), // HotSoak
        32 => Some(16), // RunningLoss
        _ => None,
    }
}

/// Read `nrevapemissionrate` into `(SCC, engTechID) -> TechRate` (the evap
/// equivalent of the `.EMF` exhaust rate table). Slots in `TechRate.by_pollutant`
/// map to evap-species slots (7-16) via [`evap_pollutant_slot_for`].
fn load_evap_rate_lookup<S: DataFrameStore + ?Sized>(
    store: &S,
) -> BTreeMap<(String, i64), TechRate> {
    let mut map: BTreeMap<(String, i64), TechRate> = BTreeMap::new();
    let Some(df) = store.get("nrevapemissionrate") else {
        return map;
    };
    let scc = str_col(&df, "SCC");
    let pp = int_col(&df, "polProcessID");
    let hmin = int_col(&df, "hpMin");
    let hmax = int_col(&df, "hpMax");
    let tech = int_col(&df, "engTechID");
    let rate = float_col(&df, "meanBaseRate");
    let units = str_col(&df, "units");
    for i in 0..df.height() {
        let Some(pslot) = evap_pollutant_slot_for(pp[i]) else {
            continue;
        };
        let e = map.entry((scc[i].clone(), tech[i])).or_default();
        e.by_pollutant.entry(pslot).or_default().push((
            hmin[i],
            hmax[i],
            (rate[i] as f32, unit_code_for(&units[i])),
        ));
    }
    map
}

/// Read `nrengtechfraction` (processGroupID = 2, evap) into a
/// `(SCC, hpMin, hpMax) -> { engTechID -> { modelYear -> fraction } }` map.
fn load_evap_tech_mix<S: DataFrameStore + ?Sized>(store: &S) -> TechMix {
    let mut map = TechMix::new();
    let Some(df) = store.get("nrengtechfraction") else {
        return map;
    };
    let scc = str_col(&df, "SCC");
    let hmin = int_col(&df, "hpMin");
    let hmax = int_col(&df, "hpMax");
    let my = int_col(&df, "modelYearID");
    let tech = int_col(&df, "engTechID");
    let frac = float_col(&df, "NREngTechFraction");
    let pg = int_col(&df, "processGroupID");
    for i in 0..df.height() {
        if pg[i] != 2 {
            continue; // evap only
        }
        map.entry((scc[i].clone(), hmin[i], hmax[i]))
            .or_default()
            .entry(tech[i])
            .or_default()
            .insert(my[i], frac[i] as f32);
    }
    map
}

/// Build the per-`(SCC, HP-bin)` evap-tech entries — real tech fractions from
/// `nrengtechfraction` (processGroupID = 2) and emission rates from
/// `nrevapemissionrate`. Returns an empty vec when either table is absent.
pub fn build_evap_tech_entries<S: DataFrameStore + ?Sized>(
    store: &S,
    analysis_year: i32,
) -> Vec<EvapTechEntry> {
    let mix = load_evap_tech_mix(store);
    if mix.is_empty() {
        return Vec::new();
    }
    let rates = load_evap_rate_lookup(store);
    let det = load_deterioration(store);

    let Some(sut) = store.get("nrsourceusetype") else {
        return Vec::new();
    };
    let su_scc = str_col(&sut, "SCC");
    let su_hp = float_col(&sut, "hpAvg");
    let mut pairs: std::collections::BTreeSet<(String, i64)> = std::collections::BTreeSet::new();
    for i in 0..sut.height() {
        pairs.insert((su_scc[i].clone(), (su_hp[i] * 1.0e3).round() as i64));
    }

    let mut entries = Vec::new();
    for (scc, hp_milli) in pairs {
        let hp_avg = hp_milli as f32 / 1.0e3;
        // Canonical `fndevtch` SCC fallback: exact → 7-digit equipment root
        // → 4-digit family root (marine-diesel evap mixes key at
        // 2282020000, for example).
        let equip = equip_root(&scc);
        let root = family_root(&scc);
        let chain: [&String; 3] = [&scc, &equip, &root];

        let find_bin = |target: &str| {
            mix.iter().find(|((s, lo, hi), _)| {
                s.as_str() == target && (*lo as f32) <= hp_avg && hp_avg <= (*hi as f32)
            })
        };
        let found = chain.iter().find_map(|s| find_bin(s));
        let Some((_, tech_map)) = found else {
            continue; // no evap tech mix for this SCC (e.g. diesel)
        };

        let mut tech_ids: Vec<i64> = tech_map.keys().copied().collect();
        tech_ids.truncate(MXEVTECH);
        let n_tech = tech_ids.len().max(1);

        let mut emission_factors = vec![0.0_f32; MXPOL * n_tech];
        let mut unit_codes = vec![EmissionUnitCode::GramsPerHour; MXPOL * n_tech];
        let mut det_a = vec![0.0_f32; MXPOL * n_tech];
        let mut det_b = vec![0.0_f32; MXPOL * n_tech];
        let mut det_cap = vec![0.0_f32; MXPOL * n_tech];
        let mut by_year: BTreeMap<i32, Vec<f32>> = BTreeMap::new();

        // Map from evap species slot to polProcessID (for deterioration lookup).
        // Deterioration is keyed by (polProcessID, engTechID). polProcessID is
        // pollutantID × 100 + processID; for THC (pollutantID=1) and the evap
        // process IDs (20-24, 30-32) this gives the values below.
        let evap_slot_to_pp: [(usize, i64); 8] = [
            (7, 130),  // Diurnal
            (8, 120),  // TankPermeation
            (9, 121),  // HosePermeation
            (10, 122), // NeckPermeation
            (11, 123), // SupplyReturnPermeation
            (12, 124), // VentPermeation
            (13, 131), // HotSoak
            (16, 132), // RunningLoss
        ];

        for (t, &tid) in tech_ids.iter().enumerate() {
            for &(pslot, pp_for) in &evap_slot_to_pp {
                let picked = chain.iter().find_map(|s| {
                    rates
                        .get(&((*s).clone(), tid))
                        .and_then(|tr| tr.by_pollutant.get(&pslot))
                        .and_then(|v| hp_pick(v, hp_avg).copied())
                });
                let idx = pslot * n_tech + t;
                if let Some((r, u)) = picked {
                    emission_factors[idx] = r;
                    unit_codes[idx] = u;
                }
                if let Some(&(a, b, c)) = det.get(&(pp_for, tid)) {
                    det_a[idx] = a;
                    det_b[idx] = b;
                    det_cap[idx] = c;
                }
            }

            // Per-model-year tech fractions for this tech slot.
            for (&yr, &f) in &tech_map[&tid] {
                by_year
                    .entry(yr as i32)
                    .or_insert_with(|| vec![0.0_f32; n_tech])[t] = f;
            }
        }

        // Scalar tech fractions = latest model-year mix (fallback to first).
        let tech_fractions = by_year
            .range(..=analysis_year)
            .next_back()
            .or_else(|| by_year.iter().next_back())
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| {
                let mut d = vec![0.0_f32; n_tech];
                d[0] = 1.0;
                d
            });

        entries.push(EvapTechEntry {
            scc,
            hp_min: hp_avg,
            hp_max: hp_avg,
            tech_names: tech_ids.iter().map(|t| t.to_string()).collect(),
            tech_fractions,
            emission_factors,
            unit_codes,
            det_a,
            det_b,
            det_cap,
        });
    }
    entries
}

/// The equipment-type root SCC for a full 10-digit nonroad SCC — the first
/// seven digits followed by `000` (e.g. `2270004071` → `2270004000`). The
/// middle step of the canonical `fndtch`/`fndefc` `ascglb` fallback chain.
fn equip_root(scc: &str) -> String {
    if scc.len() >= 10 {
        format!("{}000", &scc[..7])
    } else {
        scc.to_string()
    }
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

/// Map each full SCC to its spatial-allocation surrogate via `nrscc`
/// (SCC → NREquipTypeID) and `nrequipmenttype` (NREquipTypeID →
/// surrogateID). This is the DB form of NONROAD's `ALLOCATE.XRF`
/// (canonical generates it from exactly this join —
/// `NonroadDataFileHelper.java:186-188` — with `coeff = 1.0`).
fn scc_surrogate_map<S: DataFrameStore + ?Sized>(store: &S) -> BTreeMap<String, i64> {
    let mut equip_surrogate: BTreeMap<i64, i64> = BTreeMap::new();
    if let Some(df) = store.get("nrequipmenttype") {
        let id = int_col(&df, "NREquipTypeID");
        let sur = int_col(&df, "surrogateID");
        for i in 0..df.height() {
            equip_surrogate.insert(id[i], sur[i]);
        }
    }
    let mut map = BTreeMap::new();
    if let Some(df) = store.get("nrscc") {
        let scc = str_col(&df, "SCC");
        let eq = int_col(&df, "NREquipTypeID");
        for i in 0..df.height() {
            if let Some(&sur) = equip_surrogate.get(&eq[i]) {
                map.insert(scc[i].clone(), sur);
            }
        }
    }
    map
}

/// Per-surrogate state→county allocation fractions for `county_fips`,
/// porting NONROAD's `alocty.f` + `getind.f` over `nrstatesurrogate`
/// (the DB source of the `.ALO` files):
///
///   fraction(surrogateID) = quant(county) / quant(state total row)
///
/// where the state total row is `countyID = stateID * 1000` and each
/// FIPS's quant is taken at the **latest `surrogateYearID` ≤ the episode
/// year**, falling back to the earliest year above it (`getind.f`'s
/// `vallow`-else-`valhi` rule — no interpolation). A zero or missing
/// state total yields fraction 0 (`alocty.f` line 40 guards
/// `valsta > 0`).
fn surrogate_fractions<S: DataFrameStore + ?Sized>(
    store: &S,
    county_fips: i64,
    year: i64,
) -> BTreeMap<i64, f64> {
    let mut out = BTreeMap::new();
    let Some(df) = store.get("nrstatesurrogate") else {
        return out;
    };
    let state_row = county_fips / 1000 * 1000;
    let sur = int_col(&df, "surrogateID");
    let cty = int_col(&df, "countyID");
    let yr = int_col(&df, "surrogateYearID");
    let quant = float_col(&df, "surrogatequant");

    // (surrogateID, fips) → (best year at-or-below, best year above).
    type Pick = (Option<(i64, f64)>, Option<(i64, f64)>);
    let mut picks: BTreeMap<(i64, i64), Pick> = BTreeMap::new();
    for i in 0..df.height() {
        if cty[i] != county_fips && cty[i] != state_row {
            continue;
        }
        let entry = picks.entry((sur[i], cty[i])).or_default();
        if yr[i] <= year {
            if entry.0.is_none_or(|(y, _)| yr[i] > y) {
                entry.0 = Some((yr[i], quant[i]));
            }
        } else if entry.1.is_none_or(|(y, _)| yr[i] < y) {
            entry.1 = Some((yr[i], quant[i]));
        }
    }
    let value = |sur_id: i64, fips: i64| -> Option<f64> {
        let (low, hi) = picks.get(&(sur_id, fips))?;
        low.or(*hi).map(|(_, v)| v)
    };
    let surrogate_ids: std::collections::BTreeSet<i64> = sur.iter().copied().collect();
    for sur_id in surrogate_ids {
        let qs = value(sur_id, state_row).unwrap_or(0.0);
        let qc = value(sur_id, county_fips).unwrap_or(0.0);
        out.insert(sur_id, if qs > 0.0 { qc / qs } else { 0.0 });
    }
    out
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

/// The set of SCCs the runspec's (fuel × sector) selection matches, mirroring
/// canonical's `/SOURCE CATEGORY/` packet query
/// (`NonroadOptFileSqlHelper.getSCCs`): `SELECT SCC FROM nrscc WHERE
/// fuelTypeID IN (runspec fuels) AND NREquipTypeID IN (runspec sectors)`.
///
/// `None` ⇒ run with **no SCC restriction**. That happens in two cases:
/// - neither selection table is present (unit-test stores), or
/// - the selection matches NO `nrscc` row. This is a load-bearing canonical
///   quirk: the runspec stores the *onroad* fuelTypeID (diesel = 2) but
///   `nrscc` uses nonroad fuel IDs (diesel = 23/24), so a diesel-only
///   nonroad runspec yields an EMPTY `/SOURCE CATEGORY/` packet — and the
///   NONROAD Fortran treats an empty packet exactly like a missing one,
///   running the ENTIRE inventory (all sectors, all fuels). Verified by
///   running the canonical NONROAD binary with an empty vs absent packet
///   (identical full output) and by the nr-agriculture-state /
///   nr-airport-support-county / nr-industrial-county /
///   nr-railroad-support-nation snapshots, whose diesel-only runspecs
///   produced every sector's SCCs.
fn selected_sccs<S: DataFrameStore + ?Sized>(
    store: &S,
) -> Option<std::collections::BTreeSet<String>> {
    let sectors = selected_sectors(store);
    let fuels = selected_fuels(store);
    if sectors.is_none() && fuels.is_none() {
        return None;
    }
    let scc_sector = scc_sector_map(store);
    let scc_fuel = scc_fuel_map(store);
    let df = store.get("nrscc")?;
    let scc = str_col(&df, "SCC");
    let mut allowed = std::collections::BTreeSet::new();
    for s in &scc {
        if let Some(sel) = &sectors {
            match scc_sector.get(s) {
                Some(sec) if sel.contains(sec) => {}
                _ => continue,
            }
        }
        if let Some(sel) = &fuels {
            match scc_fuel.get(s) {
                Some(fuel) if sel.contains(fuel) => {}
                _ => continue,
            }
        }
        allowed.insert(s.clone());
    }
    // Empty selection ⇒ empty /SOURCE CATEGORY/ packet ⇒ canonical runs
    // everything.
    (!allowed.is_empty()).then_some(allowed)
}

/// Geographic scope of one master-loop firing: the county FIPS the loop
/// is iterating plus the analysis year (which picks the surrogate year).
/// `None` ⇒ national (the captured population is summed across all
/// states, as for a NATION-scale run). A FIPS of `XX000` is a state-level
/// pseudo-county (NONROAD's state-total region): the population is
/// filtered to that state but not county-allocated.
#[derive(Clone, Copy)]
pub struct GeoScope {
    pub county_fips: u32,
    pub year: i32,
}

/// Join `nrsourceusetype` (SCC, hp, activity by `sourceTypeID`) to
/// `nrbaseyearequippopulation` (population by `sourceTypeID`), yielding one
/// [`SourceUnit`] per source type with non-zero population, **restricted to
/// the runspec's selected SCCs** (see [`selected_sccs`]; the snapshot
/// carries every nonroad sector — the runspec may select only some, or its
/// selection may match nothing, in which case everything runs).
///
/// `scope` narrows the captured national population to the firing
/// location: state runs keep only that state's rows; county runs
/// additionally scale each unit by its surrogate's state→county fraction
/// ([`surrogate_fractions`], NONROAD's `alocty.f`).
fn load_source_units<S: DataFrameStore + ?Sized>(
    store: &S,
    scope: Option<GeoScope>,
) -> Vec<SourceUnit> {
    let Some(sut) = store.get("nrsourceusetype") else {
        return Vec::new();
    };
    let Some(pop) = store.get("nrbaseyearequippopulation") else {
        return Vec::new();
    };
    let allowed_sccs = selected_sccs(store);
    // Geographic narrowing: state filter + (for a real county) the
    // per-surrogate allocation fractions.
    let (state_filter, county_alloc) = match scope {
        None => (None, None),
        Some(GeoScope { county_fips, year }) => {
            let state = i64::from(county_fips) / 1000;
            let alloc = (county_fips % 1000 != 0).then(|| {
                surrogate_fractions(store, i64::from(county_fips), i64::from(year))
            });
            (Some(state), alloc)
        }
    };
    let scc_surrogate = if county_alloc.is_some() {
        scc_surrogate_map(store)
    } else {
        BTreeMap::new()
    };

    // population by sourceTypeID. Unscoped: summed across all state rows
    // (a NATION capture carries either per-state rows or a single
    // stateID = 0 aggregate; either way the sum is the national total).
    // Scoped: only the firing state's rows.
    let pop_src = int_col(&pop, "sourceTypeID");
    let pop_val = float_col(&pop, "population");
    let pop_state = if state_filter.is_some() {
        int_col(&pop, "stateID")
    } else {
        Vec::new()
    };
    let mut pop_by_src: BTreeMap<i64, f64> = BTreeMap::new();
    for i in 0..pop.height() {
        if let Some(state) = state_filter {
            if pop_state[i] != state {
                continue;
            }
        }
        // Canonical writes each state population row to the NONROAD .POP
        // file with `%17.1f` (NonroadDataFileHelper.generatePopFile), so
        // the engine only ever sees populations rounded to ONE decimal.
        // Sub-0.05 rows (e.g. a 0.0285-unit long-lived hp bin) round to
        // zero and contribute nothing — without this, the port emits
        // model-year tails canonical never produces.
        *pop_by_src.entry(pop_src[i]).or_default() += (pop_val[i] * 10.0).round() / 10.0;
    }

    let src = int_col(&sut, "sourceTypeID");
    let scc = str_col(&sut, "SCC");
    let hp_avg = float_col(&sut, "hpAvg");
    let hours = float_col(&sut, "hoursUsedPerYear");
    let load = float_col(&sut, "loadFactor");
    let median_life = float_col(&sut, "medianLifeFullLoad");

    let mut units = Vec::new();
    for i in 0..sut.height() {
        let mut population = pop_by_src.get(&src[i]).copied().unwrap_or(0.0);
        // State→county allocation by the SCC's surrogate share (alocty.f).
        if let Some(fractions) = &county_alloc {
            let frac = scc_surrogate
                .get(&scc[i])
                .and_then(|sur| fractions.get(sur))
                .copied()
                .unwrap_or(0.0);
            population *= frac;
        }
        if population <= 0.0 {
            continue;
        }
        // Skip equipment outside the runspec's matched SCC selection (the
        // canonical /SOURCE CATEGORY/ packet).
        if let Some(allowed) = &allowed_sccs {
            if !allowed.contains(&scc[i]) {
                continue;
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
/// source unit, grouped by SCC, all assigned to `PSEUDO_COUNTY`. The
/// population is the base-year (`NRBaseYearID`) snapshot; the engine's
/// `age_distribution` projects it forward to the analysis (growth) year.
///
/// `county_fips` is the master loop's current county (`None`/`0` ⇒
/// national, `XX000` ⇒ state level, else a real county allocated via
/// `surrogate_fractions`).
pub fn build_nonroad_inputs<S: DataFrameStore + ?Sized>(
    store: &S,
    analysis_year: i32,
    county_fips: Option<u32>,
) -> NonroadInputs {
    let scope = match county_fips {
        None | Some(0) => None,
        Some(c) => Some(GeoScope {
            county_fips: c,
            year: analysis_year,
        }),
    };
    let units = load_source_units(store, scope);
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

/// Build the activity table — one [`ActivityTableEntry`] per
/// `(SCC, hpAvg)` equipment point, with a point HP range so the
/// engine's `find_activity(scc, hp)` resolves the same row canonical
/// `fndact` does (its HP-category check matters: hours/year vary by HP
/// bin within an SCC, e.g. marine gasoline 2282010005 uses 47.6 hr/yr
/// below the 750-hp bin but 30 hr/yr inside it — feeding the wrong
/// hours into `scrptime` shifts that bin's whole model-year span).
///
/// Built unscoped (national): the `(hoursUsedPerYear, loadFactor)`
/// values come from `nrsourceusetype`, which does not vary by
/// geography — only the population (handled in
/// [`build_nonroad_inputs`]) is allocated.
pub fn build_activity_entries<S: DataFrameStore + ?Sized>(store: &S) -> Vec<ActivityTableEntry> {
    let units = load_source_units(store, None);
    let mut seen: BTreeMap<(String, i64), ActivityTableEntry> = BTreeMap::new();
    for u in units {
        seen.entry((u.scc.clone(), (u.hp_avg * 1.0e3).round() as i64))
            .or_insert_with(|| ActivityTableEntry {
                scc: u.scc.clone(),
                fips: String::new(), // match any FIPS
                hp_min: u.hp_avg,
                hp_max: u.hp_avg,
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
        // Entries from the canonical mix-driven builder already carry their
        // per-model-year fractions; don't overwrite them.
        if !e.tech_fractions_by_year.is_empty() {
            continue;
        }
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
/// The `stateID` column of a state-keyed `nr*` table, or `None` when the
/// table has no such column (unit-test stores; treated as stateID = 0
/// defaults).
fn opt_state_col(df: &DataFrame) -> Option<Vec<i64>> {
    resolve(df, "stateID").is_some().then(|| int_col(df, "stateID"))
}

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
/// `state` scopes the pattern choice: `nrgrowthpatternfinder` is keyed
/// (SCC, stateID) — canonical's `.GRW` `/INDICATORS/` packet emits one
/// pattern per (state FIPS, SCC) (`NonroadDataFileHelper.java:1536`), so a
/// scoped run must use its own state's pattern. stateID = 0 rows (the only
/// kind in a NATION capture) are the fallback.
fn build_growth<S: DataFrameStore + ?Sized>(
    store: &S,
    state: Option<i64>,
) -> (BTreeMap<String, String>, Vec<GrowthIndicatorRecord>) {
    let mut scc_pattern: BTreeMap<String, String> = BTreeMap::new();
    if let Some(df) = store.get("nrgrowthpatternfinder") {
        let scc = str_col(&df, "SCC");
        let pat = int_col(&df, "growthPatternID");
        let st = opt_state_col(&df);
        let mut state_specific: std::collections::BTreeSet<String> =
            std::collections::BTreeSet::new();
        for i in 0..df.height() {
            let row_state = st.as_ref().map_or(0, |v| v[i]);
            match state {
                Some(s) if row_state == s => {
                    state_specific.insert(scc[i].clone());
                    scc_pattern.insert(scc[i].clone(), pat[i].to_string());
                }
                Some(_) if row_state == 0 => {
                    if !state_specific.contains(&scc[i]) {
                        scc_pattern.insert(scc[i].clone(), pat[i].to_string());
                    }
                }
                Some(_) => {}
                None => {
                    scc_pattern.insert(scc[i].clone(), pat[i].to_string());
                }
            }
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
                // Canonical writes the /GROWTH/ packet with
                // `results.getInt(growthindex)` (NonroadDataFileHelper.
                // generateGrowthFile `%20d`), truncating the fractional
                // index toward zero. The year-over-year growth factors sit
                // near zero, so the fraction decides the SIGN — and with it
                // which sale-years agedist's max(0,…) clamp zeroes out.
                value: (idx[i] as i64) as f32,
            });
        }
    }
    (scc_pattern, records)
}

/// Build the global scrappage curve from `nrscrappagecurve` — the default
/// `NREquipTypeID = 0` curve, matching canonical (`NonroadDataFileHelper`
/// writes only `WHERE NREquipTypeID = 0`; alternates are deferred).
///
/// The scrappage curve is a *required* input: canonical NONROAD reads it from
/// the mandatory `/SCRAPPAGE/` packet and aborts if the packet is missing
/// (`rdscrp.f:80-86 goto 7000`), and the EPA default database always ships the
/// `NREquipTypeID = 0` curve. A degenerate fabricated `0→100%` line is *not*
/// the canonical default curve (which is a sigmoid-like shape) and would
/// mis-age the fleet, so an absent/empty curve is surfaced as a hard error
/// rather than silently substituting points.
pub fn build_scrappage_curve<S: DataFrameStore + ?Sized>(store: &S) -> Vec<ScrappagePoint> {
    let Some(df) = store.get("nrscrappagecurve") else {
        panic!(
            "required table nrscrappagecurve is absent; canonical NONROAD aborts \
             when the /SCRAPPAGE/ packet is missing (rdscrp.f). The fleet-aging \
             scrappage curve cannot be fabricated."
        );
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
    let points: Vec<ScrappagePoint> = acc
        .into_iter()
        .map(|(k, pct)| ScrappagePoint {
            bin: (k as f64 / 1.0e6) as f32,
            percent: pct as f32,
        })
        .collect();
    if points.is_empty() {
        panic!(
            "nrscrappagecurve has no default (NREquipTypeID = 0) rows; canonical \
             NONROAD requires the default scrappage curve (rdscrp.f). The \
             fleet-aging curve cannot be fabricated."
        );
    }
    points
}

/// Average ambient temperature (°F) for the `emsadj.f` exhaust temperature
/// correction — the canonical `.opt` `Average temper.` value
/// (`NonroadOptFileSqlHelper`, "averageTemperature"):
///
/// ```sql
/// SELECT avg(temperature) FROM zonemonthhour t
/// INNER JOIN zone z ON t.zoneid = z.zoneid
/// WHERE z.countyid = ? AND t.monthid = ? AND hourid >= 6 AND hourid <= 18
/// ```
///
/// One **daytime (hourID 6–18)** mean per bundle county, applied to every
/// SCC in the region. The bundle county is exactly the master loop's
/// `county_fips` — a NATION capture has the pseudo county/zone 0, a state
/// run the `XX000` pseudo county (zone `XX0000`), a county run the real
/// county — so the same join covers all three scopes. `month = 0`
/// (annual run) averages all months. Returns `None` when the tables are
/// absent or no row matches.
fn build_ambient_temp<S: DataFrameStore + ?Sized>(
    store: &S,
    month: i64,
    county_fips: Option<u32>,
) -> Option<f32> {
    let df = store.get("zonemonthhour")?;
    let m = int_col(&df, "monthID");
    let t = float_col(&df, "temperature");
    let h = int_col(&df, "hourID");
    let z = resolve(&df, "zoneID").map(|_| int_col(&df, "zoneID"));
    // zone.countyID per zoneID for the county join; absent table (unit
    // tests) ⇒ no geographic restriction.
    let zone_county: Option<BTreeMap<i64, i64>> = store.get("zone").map(|zdf| {
        let zid = int_col(&zdf, "zoneID");
        let cid = int_col(&zdf, "countyID");
        (0..zdf.height()).map(|i| (zid[i], cid[i])).collect()
    });
    let county = county_fips.map(i64::from);
    let (mut sum, mut n) = (0.0_f64, 0_u32);
    for i in 0..df.height() {
        if month != 0 && m[i] != month {
            continue;
        }
        if !(6..=18).contains(&h[i]) {
            continue;
        }
        if let (Some(c), Some(zc), Some(z)) = (county, &zone_county, &z) {
            if zc.get(&z[i]).copied() != Some(c) {
                continue;
            }
        }
        sum += t[i];
        n += 1;
    }
    if n > 0 {
        Some((sum / n as f64) as f32)
    } else {
        None
    }
}

/// Per-tech sulfur alternates from `nrsulfuradjustment` — the canonical
/// `/PM BASE SULFUR/` packet (`NonroadOptFileSqlHelper.getPMBasedSulfur`:
/// `SELECT DISTINCT engtechname, pmbasesulfur, sulfatepmconversionfactor
/// FROM nrsulfuradjustment WHERE fueltypeid IN (23,24)`). Keyed by the
/// engTechID string (the port's tech names are engTechIDs, 1:1 with
/// engTechName). Drives the per-tech base/conversion override in the
/// diesel PM sulfur correction and the SOx EF rewrite (`clcems.f`
/// `sulalt`/`sulcnv`).
fn build_sulfur_alternates<S: DataFrameStore + ?Sized>(
    store: &S,
) -> BTreeMap<String, moves_nonroad::emissions::exhaust::SulfurAlternate> {
    let mut out = BTreeMap::new();
    let Some(df) = store.get("nrsulfuradjustment") else {
        return out;
    };
    let fuel = int_col(&df, "fuelTypeID");
    let tech = int_col(&df, "engTechID");
    let base = float_col(&df, "PMBaseSulfur");
    let conv = float_col(&df, "sulfatePMConversionFactor");
    for i in 0..df.height() {
        if fuel[i] == 23 || fuel[i] == 24 {
            out.insert(
                tech[i].to_string(),
                moves_nonroad::emissions::exhaust::SulfurAlternate {
                    alternate_base: base[i] as f32,
                    alternate_conversion: conv[i] as f32,
                },
            );
        }
    }
    out
}

/// The run's in-use fuel properties — the canonical `.opt` OPTIONS-packet
/// values (`NonroadOptFileSqlHelper.getOptionsParameters`).
struct NonroadFuelProperties {
    /// Gasoline oxygen content (weight %), `emsadj.f` oxygenate correction.
    oxygen_pct: f32,
    /// Whether the gasoline supply is predominantly RFG (subtype 11).
    rfg: bool,
    /// In-use sulfur weight % per engine fuel slot `[gas-2str, gas-4str,
    /// diesel, LPG, CNG]`; `None` when no fuel-supply data is loaded.
    sulfur_pct: Option<[f32; 5]>,
    /// In-use marine-diesel sulfur weight % (`soxdsm`).
    sulfur_marine: f32,
}

/// Port of the canonical OPTIONS-packet fuel queries: every value is a
/// `marketShare`-weighted sum over the **bundle county's nonroad fuel
/// region and run month-group** —
///
/// ```sql
/// ... FROM nrfuelsupply fs
/// INNER JOIN fuelformulation ff ON fs.fuelformulationid = ff.fuelformulationid
/// INNER JOIN year y ON y.fuelYearID = fs.fuelYearID
/// INNER JOIN regionCounty rc ON (rc.regionID = fs.fuelRegionID
///       AND rc.regionCodeID = 2 AND rc.fuelYearID = y.fuelYearID)
/// WHERE rc.countyid = ? AND fs.monthgroupid = ? AND y.yearID = ?
/// ```
///
/// - oxygen weight % (func A2): Σ (ETOH+MTBE+ETBE+TAME volumes) ×
///   volToWtPercentOxy × share over gasoline subtypes — **no fallback**:
///   an empty join yields 0.0. This is load-bearing: a state-scale run's
///   `regionCounty.regionID` (e.g. Iowa = 19) matches no
///   `nrfuelsupply.fuelRegionID`, so canonical runs with oxygen 0 while
///   the national supply would give ~3.6 wt% (a ~22% CO difference).
/// - gas sulfur (func A): Σ sulfurLevel × share / 10000; when the join
///   matches nothing, falls back to fuelFormulationID = 10's sulfurLevel.
/// - diesel / marine-diesel sulfur (funcs C/D): subtypes 23 / 24, raw sums.
/// - CNG/LPG sulfur (func E): subtypes 30/40, normalized by Σ share.
fn build_fuel_properties<S: DataFrameStore + ?Sized>(
    store: &S,
    county_fips: Option<u32>,
    month: u8,
    year: i32,
) -> NonroadFuelProperties {
    let none = NonroadFuelProperties {
        oxygen_pct: 0.0,
        rfg: false,
        sulfur_pct: None,
        sulfur_marine: 0.0,
    };
    let (Some(sup), Some(form)) = (store.get("nrfuelsupply"), store.get("fuelformulation")) else {
        return none;
    };

    // fuelformulation: per-formulation oxygen %, sulfur %, subtype.
    let f_id = int_col(&form, "fuelFormulationID");
    let f_sub = int_col(&form, "fuelSubtypeID");
    let f_v2w = float_col(&form, "volToWtPercentOxy");
    let f_sulfur = float_col(&form, "sulfurLevel");
    // Oxygenate volumes; columns may be absent from minimal test frames.
    let vol = |name: &str| -> Vec<f64> {
        if resolve(&form, name).is_some() {
            float_col(&form, name)
        } else {
            vec![0.0; form.height()]
        }
    };
    let (etoh, mtbe, etbe, tame) = (
        vol("ETOHVolume"),
        vol("MTBEVolume"),
        vol("ETBEVolume"),
        vol("TAMEVolume"),
    );
    let mut oxy_by_form: BTreeMap<i64, f64> = BTreeMap::new();
    let mut sulfur_by_form: BTreeMap<i64, f64> = BTreeMap::new();
    let mut sub_by_form: BTreeMap<i64, i64> = BTreeMap::new();
    for i in 0..form.height() {
        oxy_by_form.insert(f_id[i], (etoh[i] + mtbe[i] + etbe[i] + tame[i]) * f_v2w[i]);
        sulfur_by_form.insert(f_id[i], f_sulfur[i]);
        sub_by_form.insert(f_id[i], f_sub[i]);
    }
    let mut fueltype_by_sub: BTreeMap<i64, i64> = BTreeMap::new();
    if let Some(st) = store.get("nrfuelsubtype") {
        let sid = int_col(&st, "fuelSubtypeID");
        let ft = int_col(&st, "fuelTypeID");
        for i in 0..st.height() {
            fueltype_by_sub.insert(sid[i], ft[i]);
        }
    }

    // The bundle county's nonroad fuel region(s): regionCounty rows with
    // regionCodeID = 2, countyID = bundle county, fuelYearID = the run
    // year's fuelYearID. `None` (no regioncounty table, or no county —
    // unit-test stores) ⇒ no region restriction.
    let fuel_year: i64 = store
        .get("year")
        .and_then(|y| {
            let yid = int_col(&y, "yearID");
            let fy = int_col(&y, "fuelYearID");
            (0..y.height()).find(|&i| yid[i] == i64::from(year)).map(|i| fy[i])
        })
        .unwrap_or(i64::from(year));
    let regions: Option<std::collections::BTreeSet<i64>> = match (store.get("regioncounty"), county_fips) {
        (Some(rc), Some(c)) => {
            let rid = int_col(&rc, "regionID");
            let cid = int_col(&rc, "countyID");
            let code = int_col(&rc, "regionCodeID");
            let fy = int_col(&rc, "fuelYearID");
            Some(
                (0..rc.height())
                    .filter(|&i| code[i] == 2 && cid[i] == i64::from(c) && fy[i] == fuel_year)
                    .map(|i| rid[i])
                    .collect(),
            )
        }
        _ => None,
    };

    let s_form = int_col(&sup, "fuelFormulationID");
    let s_share = float_col(&sup, "marketShare");
    let s_region = resolve(&sup, "fuelRegionID").map(|_| int_col(&sup, "fuelRegionID"));
    let s_month = resolve(&sup, "monthGroupID").map(|_| int_col(&sup, "monthGroupID"));
    // The canonical `year` join also restricts the SUPPLY rows to the run's
    // fuelYearID — a county capture carries every fuel year (1990–2060,
    // shares summing to ~1 per fuel type per year), so skipping this filter
    // overcounts the weighted sums ~60x (oxygen ≈ 140 wt% → the oxygenate
    // correction zeroes CO/THC and inflates NOx ~17x).
    let s_fuelyear = resolve(&sup, "fuelYearID").map(|_| int_col(&sup, "fuelYearID"));

    let mut oxy = 0.0_f64;
    let (mut gas_sulfur, mut gas_rows) = (0.0_f64, 0u32);
    let (mut gas_share, mut rfg_share) = (0.0_f64, 0.0_f64);
    let mut dsl_sulfur = 0.0_f64;
    let mut marine_sulfur = 0.0_f64;
    let (mut cnglpg_sulfur, mut cnglpg_share) = (0.0_f64, 0.0_f64);
    let mut any_row = false;
    for i in 0..sup.height() {
        if let (Some(regions), Some(r)) = (&regions, &s_region) {
            if !regions.contains(&r[i]) {
                continue;
            }
        }
        if month != 0 {
            if let Some(m) = &s_month {
                if m[i] != i64::from(month) {
                    continue;
                }
            }
        }
        if let Some(fy) = &s_fuelyear {
            if fy[i] != fuel_year {
                continue;
            }
        }
        any_row = true;
        let share = s_share[i];
        let sub = sub_by_form.get(&s_form[i]).copied().unwrap_or(0);
        let sulfur = sulfur_by_form.get(&s_form[i]).copied().unwrap_or(0.0);
        if fueltype_by_sub.get(&sub).copied().unwrap_or(0) == 1 {
            oxy += share * oxy_by_form.get(&s_form[i]).copied().unwrap_or(0.0);
            gas_sulfur += share * sulfur / 10_000.0;
            gas_rows += 1;
            gas_share += share;
            if sub == 11 {
                rfg_share += share; // fuelSubtypeID 11 = Reformulated Gasoline
            }
        }
        match sub {
            23 => dsl_sulfur += share * sulfur / 10_000.0,
            24 => marine_sulfur += share * sulfur / 10_000.0,
            30 | 40 => {
                cnglpg_sulfur += share * sulfur / 10_000.0;
                cnglpg_share += share;
            }
            _ => {}
        }
    }
    if !any_row && regions.is_none() {
        // No supply rows at all and no scoping — keep the legacy
        // "no fuel data" result.
        return none;
    }

    // func A fallback: no gasoline row matched the region join ⇒ default
    // fuelFormulationID 10 sulfur (canonical's RVP==0 && sulfur==0 branch).
    if gas_rows == 0 {
        if let Some(i) = f_id.iter().position(|&id| id == 10) {
            gas_sulfur = f_sulfur[i] / 10_000.0;
        }
    }
    let cnglpg = if cnglpg_share > 0.0 {
        cnglpg_sulfur / cnglpg_share
    } else {
        0.0
    };
    NonroadFuelProperties {
        oxygen_pct: oxy as f32,
        rfg: gas_share > 0.0 && rfg_share / gas_share > 0.5,
        sulfur_pct: Some([
            gas_sulfur as f32,
            gas_sulfur as f32,
            dsl_sulfur as f32,
            cnglpg as f32,
            cnglpg as f32,
        ]),
        sulfur_marine: marine_sulfur as f32,
    }
}

/// Build temporal profiles from `nrmonthallocation` + `nrdayallocation`.
///
/// Returns `(profiles, months_selected, weekday_selected)` where
/// `months_selected[i]` is true for the active month(s) and
/// `weekday_selected` is true when `day_id == 5` (MOVES weekday).
/// `selected_month = 0` means annual (all 12 months selected).
fn build_temporal_profiles_for_period<S: DataFrameStore + ?Sized>(
    store: &S,
    selected_month: u8,
    day_id: u8,
    state: Option<i64>,
) -> (BTreeMap<String, TemporalProfile>, [bool; 12], bool) {
    // Collect monthly fractions per SCC: index 0=Jan..11=Dec.
    // `nrmonthallocation` is keyed (SCC, stateID, monthID) — canonical's
    // SEASON.DAT /MONTHLY/ packet is per state region
    // (`NonroadDataFileHelper` groups by `m.stateID, SCC`), so a scoped run
    // must read its own state's fractions: August agriculture activity in
    // Iowa is very different from the last-sorted state's. stateID = 0 rows
    // (the only kind in a NATION capture) act as the fallback default.
    let mut monthly_by_scc: BTreeMap<String, [f32; 12]> = BTreeMap::new();
    if let Some(df) = store.get("nrmonthallocation") {
        let scc = str_col(&df, "SCC");
        let month = int_col(&df, "monthID");
        let frac = float_col(&df, "monthFraction");
        let st = opt_state_col(&df);
        let mut state_specific: std::collections::BTreeSet<String> =
            std::collections::BTreeSet::new();
        for i in 0..df.height() {
            let m = month[i] as usize;
            if !(1..=12).contains(&m) {
                continue;
            }
            let row_state = st.as_ref().map_or(0, |v| v[i]);
            match state {
                Some(s) if row_state == s => {
                    // State-specific row: wins over any default already seen.
                    if state_specific.insert(scc[i].clone()) {
                        monthly_by_scc.insert(scc[i].clone(), [0.0; 12]);
                    }
                    monthly_by_scc.get_mut(&scc[i]).unwrap()[m - 1] = frac[i] as f32;
                }
                Some(_) if row_state == 0 => {
                    if !state_specific.contains(&scc[i]) {
                        monthly_by_scc.entry(scc[i].clone()).or_insert([0.0; 12])[m - 1] =
                            frac[i] as f32;
                    }
                }
                Some(_) => {} // another state's row — ignore
                None => {
                    monthly_by_scc.entry(scc[i].clone()).or_insert([0.0; 12])[m - 1] =
                        frac[i] as f32;
                }
            }
        }
    }
    // Collect daily fractions per SCC: [weekday, weekend].
    let mut daily_by_scc: BTreeMap<String, [f32; 2]> = BTreeMap::new();
    if let Some(df) = store.get("nrdayallocation") {
        let scc = str_col(&df, "scc");
        let day = int_col(&df, "dayID");
        let frac = float_col(&df, "dayFraction");
        for i in 0..df.height() {
            // MOVES dayID: 5 = weekday (slot 0), 2 = weekend (slot 1).
            let slot = match day[i] {
                5 => 0usize,
                2 => 1usize,
                _ => continue,
            };
            daily_by_scc.entry(scc[i].clone()).or_insert([1.0 / 7.0; 2])[slot] = frac[i] as f32;
        }
    }
    // Merge into TemporalProfile. Canonical NONROAD searches the /MONTHLY/
    // and /DAILY/ packets INDEPENDENTLY, each with its own global-code
    // fallback (exact SCC → 7-digit+000 → 4-digit+000000 → defmth=1/12 /
    // defday=1/7). The two tables key at different granularities (e.g.
    // nrdayallocation has an exact `2265004071` row while
    // nrmonthallocation only has `2265004000`), so a naive per-SCC merge
    // would pin the month dimension of an exact-day SCC to the 1/12
    // default — halving August lawn/garden activity. Resolve each
    // dimension with `scc_lookup` before merging.
    let all_sccs: std::collections::BTreeSet<&String> =
        monthly_by_scc.keys().chain(daily_by_scc.keys()).collect();
    let result: BTreeMap<String, TemporalProfile> = all_sccs
        .into_iter()
        .map(|scc| {
            let monthly = scc_lookup(&monthly_by_scc, scc)
                .copied()
                .unwrap_or([1.0 / 12.0; 12]);
            let daily = scc_lookup(&daily_by_scc, scc)
                .copied()
                .unwrap_or([1.0 / 7.0; 2]);
            (scc.clone(), TemporalProfile { monthly, daily })
        })
        .collect();

    let months_selected = if selected_month == 0 {
        [true; 12]
    } else {
        let mut m = [false; 12];
        if (1..=12).contains(&selected_month) {
            m[selected_month as usize - 1] = true;
        }
        m
    };

    let weekday_selected = day_id == 5;

    (result, months_selected, weekday_selected)
}

/// Assemble the full [`ReferenceData`] the [`ProductionExecutor`] needs
/// from the `nr*` tables.
///
/// `county_fips` is the master loop's current county (`None`/`0` ⇒
/// national, `XX000` ⇒ state level): it scopes the state-keyed reference
/// tables — month allocation, growth pattern, ZoneMonthHour ambient
/// temperature — to the run's geography.
pub fn load_nonroad_reference<S: DataFrameStore + ?Sized>(
    store: &S,
    analysis_year: i32,
    selected_month: u8,
    day_id: u8,
    county_fips: Option<u32>,
) -> ReferenceData {
    let state = match county_fips {
        None | Some(0) => None,
        Some(c) => Some(i64::from(c) / 1000),
    };
    let mut exhaust_tech_entries = build_exhaust_tech_entries(store);
    fill_tech_fractions(&mut exhaust_tech_entries, store, analysis_year);
    let activity_entries = build_activity_entries(store);

    // Build real evap tech entries from nrengtechfraction (processGroupID=2) and
    // nrevapemissionrate. Falls back to zero-fraction mirror entries so the
    // county routine's evap tech lookup always succeeds (it skips the record when
    // no evap entry is found) — the zero-fraction fallback contributes nothing to
    // the per-tech-type emission loop.
    let mut evap_tech_entries = build_evap_tech_entries(store, analysis_year);
    if evap_tech_entries.is_empty() {
        evap_tech_entries = exhaust_tech_entries
            .iter()
            .map(|e| EvapTechEntry {
                scc: e.scc.clone(),
                hp_min: e.hp_min,
                hp_max: e.hp_max,
                tech_names: vec!["1".to_string()],
                tech_fractions: vec![0.0],
                ..Default::default()
            })
            .collect();
    }

    // Growth cross-reference per (SCC, HP bin): indicator = the SCC's
    // growth-pattern id (most-specific match). Unmatched SCCs get
    // indicator=None; the engine errors on None when growth is active
    // (canonical prccty.f label 7001 / fndgxf no-match path).
    let (scc_pattern, growth_records) = build_growth(store, state);
    let growth_xref_entries = exhaust_tech_entries
        .iter()
        .map(|e| moves_nonroad::simulation::GrowthXrefEntry {
            fips: PSEUDO_COUNTY.to_string(),
            scc: e.scc.clone(),
            hp_min: e.hp_min,
            hp_max: e.hp_max,
            indicator: scc_lookup(&scc_pattern, &e.scc).cloned(),
        })
        .collect();

    let fuel = build_fuel_properties(store, county_fips, selected_month, analysis_year);

    let (temporal_profiles, months_selected, weekday_selected) =
        build_temporal_profiles_for_period(store, selected_month, day_id, state);

    ReferenceData {
        exhaust_tech_entries,
        evap_tech_entries,
        activity_entries,
        growth_xref_entries,
        growth_records,
        scrappage_curve: build_scrappage_curve(store),
        age_adjustment_table: AgeAdjustmentTable::default(),
        // NR*.TMF temporal profiles + period flags.
        temporal_profiles,
        months_selected,
        weekday_selected,
        // emsadj.f oxygenate + temperature corrections.
        // `selected_month` is the MOVES monthID (1–12), or 0 for an annual run
        // (see `period_flags` / `build_options`). The exhaust temperature
        // correction EXP(acoeff*(tamb-75)) is highly month-sensitive, so the
        // ambient temperature MUST be the run month's, not an annual mean: a
        // captured `zonemonthhour` carries ALL 12 months (it is NOT pre-filtered),
        // so passing `month = 0` would average to the cold annual mean (~57 °F vs
        // ~76 °F in August) and inflate 4-stroke-gasoline NOx by ~18%.
        //
        // Canonical applies ONE temperature per bundle region — the daytime
        // (hourID 6–18) county mean from the .opt OPTIONS packet — to every
        // SCC, so `ambient_temp_by_scc` stays empty (the engine falls back
        // to the scalar).
        fuel_oxygen_pct: fuel.oxygen_pct,
        fuel_rfg: fuel.rfg,
        fuel_sulfur_pct: fuel.sulfur_pct,
        fuel_sulfur_marine: fuel.sulfur_marine,
        sulfur_alternates: build_sulfur_alternates(store),
        ambient_temp_f: build_ambient_temp(store, i64::from(selected_month), county_fips),
        alloc_fractions: build_alloc_fractions(store, county_fips, analysis_year),
        ..ReferenceData::default()
    }
}

/// Per-SCC state→county allocation fraction for a county-scoped run
/// (empty for state/national scopes). The loader pre-allocates the
/// population in [`load_source_units`], but canonical `prcsta.f` grows
/// the age distribution on the **state** population and only then
/// allocates to counties (`alocty`) — `agedist`'s `MINGRWIND` clamp is
/// magnitude-sensitive, so a pre-allocated sub-0.0001 county population
/// balloons cohorts canonical leaves untouched. The engine divides this
/// fraction back out around its `age_distribution` call.
fn build_alloc_fractions<S: DataFrameStore + ?Sized>(
    store: &S,
    county_fips: Option<u32>,
    analysis_year: i32,
) -> BTreeMap<String, f32> {
    let Some(c) = county_fips else {
        return BTreeMap::new();
    };
    if c == 0 || c % 1000 == 0 {
        return BTreeMap::new();
    }
    let fractions = surrogate_fractions(store, i64::from(c), i64::from(analysis_year));
    scc_surrogate_map(store)
        .into_iter()
        .map(|(scc, sur)| (scc, fractions.get(&sur).copied().unwrap_or(0.0) as f32))
        .collect()
}

/// Build the [`ProductionExecutor`] for the national pseudo-county run.
///
/// `selected_month` (1–12) and `day_id` (MOVES dayID: 5=weekday, 2=weekend)
/// are used to load temporal profiles and set the period flags the
/// `day_month_factor(s)` callbacks need. Pass `selected_month=0` for
/// an annual (all-months) run.
pub fn build_production_executor<S: DataFrameStore + ?Sized>(
    store: &S,
    analysis_year: i32,
    selected_month: u8,
    day_id: u8,
    county_fips: Option<u32>,
) -> ProductionExecutor {
    let reference =
        load_nonroad_reference(store, analysis_year, selected_month, day_id, county_fips);
    let (months_selected, weekday_selected) = period_flags(selected_month, day_id);
    ProductionExecutor {
        county_fips: vec![PSEUDO_COUNTY.to_string()],
        hp_levels: HP_LEVELS,
        reference,
        months_selected,
        weekday_selected,
        total_mode: false, // MOVES always uses typical-day
        ..ProductionExecutor::default()
    }
}

/// Derive `(months_selected, weekday_selected)` from MOVES period identifiers.
fn period_flags(selected_month: u8, day_id: u8) -> ([bool; 12], bool) {
    let months = if selected_month == 0 {
        [true; 12]
    } else {
        let mut m = [false; 12];
        if (1..=12).contains(&selected_month) {
            m[selected_month as usize - 1] = true;
        }
        m
    };
    (months, day_id == 5)
}

/// Build the [`NonroadOptions`] for a county-level (national pseudo-county)
/// run at `analysis_year` for the given period.
///
/// `selected_month` (1–12 or 0 for annual) and `day_id` (5=weekday, 2=weekend)
/// mirror the MOVES runspec month/day selection.
pub fn build_options(analysis_year: i32, selected_month: u8, day_id: u8) -> NonroadOptions {
    let mut opts = NonroadOptions::new(RegionLevel::County, analysis_year);
    opts.growth_loaded = true;
    // Emit by-model-year exhaust rows so the output matches canonical's
    // per-(SCC, modelYear) structure. `emissions_to_dataframe` uses only the
    // by-model-year rows (model_year = Some) to avoid double-counting the
    // per-record totals the engine also emits.
    opts.emit_bmy_exhaust = true;
    opts.selected_month = selected_month;
    opts.weekday_selected = day_id == 5;
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
/// exhaust pollutants (THC, CO, NOx, CO2, SO2, PM10), mirroring canonical
/// `NonroadOutputDataLoader` (thc/co/nox/co2/so2/pmExhaust → 1/2/3/90/31/100).
/// Which of these actually reach the output is gated per-run by
/// [`selected_output_pollutants`] (canonical gates on the bundle SQL's
/// polProcessIDs — `need3101`/`need9001` flags).
const SLOT_POLLUTANT: [(usize, i32); 6] =
    [(0, 1), (1, 2), (2, 3), (3, 90), (4, 31), (5, 100)];

/// The runspec's selected output pollutants, from the execution DB's
/// `runspecpollutantprocess` (`pollutantID = polProcessID / 100`).
/// `None` ⇒ table absent (unit-test stores): emit the legacy exhaust set
/// (THC/CO/NOx/PM) and not the always-computed BSFC species (CO2/SO2),
/// matching the pre-gating behaviour.
pub fn selected_output_pollutants<S: DataFrameStore + ?Sized>(
    store: &S,
) -> Option<std::collections::BTreeSet<i32>> {
    let df = store.get("runspecpollutantprocess")?;
    let ids = int_col(&df, "polProcessID");
    Some(ids.iter().map(|&pp| (pp / 100) as i32).collect())
}

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
/// column). A missing dimension (after family-root fallback) defaults to its
/// canonical seasonality default — `monthFraction = 1/12`, `dayFraction = 1/7`
/// (`daymthf.f` / `rdseas.f`) — not a neutral 1.0.
pub fn build_temporal_factors<S: DataFrameStore + ?Sized>(
    store: &S,
    month: i32,
    day: i32,
    state: Option<i64>,
) -> BTreeMap<String, f64> {
    // (SCC, stateID, monthID)-keyed; state-specific rows win over the
    // stateID = 0 defaults (see build_temporal_profiles_for_period).
    let mut month_by_scc: BTreeMap<String, f64> = BTreeMap::new();
    if let Some(df) = store.get("nrmonthallocation") {
        let scc = str_col(&df, "SCC");
        let m = int_col(&df, "monthID");
        let f = float_col(&df, "monthFraction");
        let st = opt_state_col(&df);
        let mut state_specific: std::collections::BTreeSet<String> =
            std::collections::BTreeSet::new();
        for i in 0..df.height() {
            if m[i] != month as i64 {
                continue;
            }
            let row_state = st.as_ref().map_or(0, |v| v[i]);
            match state {
                Some(s) if row_state == s => {
                    state_specific.insert(scc[i].clone());
                    month_by_scc.insert(scc[i].clone(), f[i]);
                }
                Some(_) if row_state == 0 => {
                    if !state_specific.contains(&scc[i]) {
                        month_by_scc.insert(scc[i].clone(), f[i]);
                    }
                }
                Some(_) => {}
                None => {
                    month_by_scc.insert(scc[i].clone(), f[i]);
                }
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

    // Canonical NONROAD loads the *default* seasonality factors for an SCC
    // with no allocation match, not 1.0: `defmth = 1/12` and `defday = 1/7`
    // (`daymthf.f:99-119` loads `defmth`/`defday`; `rdseas.f:215-221` sets
    // them to `1.0/12.0` and `1./7.`). A neutral 1.0 here would over-allocate
    // (monthFraction 1.0 dumps the whole year into one month; dayFraction 1.0
    // makes `7 × dayFraction = 7`), so default each dimension to its canonical
    // value instead.
    // Per-dimension global-code fallback (exact → 7-digit+000 →
    // 4-digit+000000), mirroring canonical's independent /MONTHLY/ and
    // /DAILY/ searches.
    let lookup = |map: &BTreeMap<String, f64>, scc: &str, default: f64| -> f64 {
        scc_lookup(map, scc).copied().unwrap_or(default)
    };

    let ndays = days_in_month(month);
    let mut factors = BTreeMap::new();
    let sccs: std::collections::BTreeSet<&String> =
        month_by_scc.keys().chain(day_by_scc.keys()).collect();
    for scc in sccs {
        // monthFraction × (7 × dayFraction) ÷ ndays (canonical typical-day).
        let f = lookup(&month_by_scc, scc, 1.0 / 12.0)
            * (7.0 * lookup(&day_by_scc, scc, 1.0 / 7.0))
            / ndays;
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
///
/// `selected` restricts the emitted pollutants to the runspec's selection
/// (see [`selected_output_pollutants`]); `None` emits the legacy
/// THC/CO/NOx/PM exhaust set.
pub fn emissions_to_dataframe(
    rows: &[SimEmissionRow],
    keys: &EmissionTimeKeys,
    temporal: &BTreeMap<String, f64>,
    selected: Option<&std::collections::BTreeSet<i32>>,
) -> PolarsResult<Option<DataFrame>> {
    let mut year = Vec::new();
    let mut month = Vec::new();
    let mut day = Vec::new();
    let mut hour = Vec::new();
    let mut pollutant = Vec::new();
    let mut process = Vec::new();
    let mut model_year = Vec::new();
    let mut scc_out: Vec<String> = Vec::new();
    let mut quant = Vec::new();

    // When by-model-year output is on, the engine emits BOTH per-record-total
    // rows (model_year = None) and per-model-year rows (model_year = Some).
    // Use only the by-model-year rows to avoid double-counting — they carry
    // the SCC × modelYear structure that matches canonical's output.
    let has_bmy = rows.iter().any(|r| r.model_year.is_some());

    for row in rows {
        if has_bmy && row.model_year.is_none() {
            continue;
        }
        let tfac = scc_lookup(temporal, &row.scc).copied().unwrap_or(1.0);
        for (slot, pid) in SLOT_POLLUTANT {
            match selected {
                // Gate on the runspec's pollutant selection.
                Some(sel) if !sel.contains(&pid) => continue,
                // No selection table: legacy exhaust set only (CO2/SO2 are
                // always computed by the engine but were never emitted).
                None if pid == 90 || pid == 31 => continue,
                _ => {}
            }
            // Canonical clamps every loaded value at zero
            // (`NonroadOutputDataLoader`: `Math.max(0, …) * USTON_TO_GRAM`) —
            // the NONROAD SOx balance can go negative for low-sulfur fuels —
            // and KEEPS the zero-valued row (every selected pollutant of
            // every .BMY record gets a MOVESOutput row), so zero is not
            // skipped here.
            let e = row.emissions.get(slot).copied().unwrap_or(0.0).max(0.0);
            year.push(keys.year);
            month.push(keys.month.unwrap_or(0));
            day.push(keys.day.unwrap_or(0));
            hour.push(keys.hour.unwrap_or(0));
            pollutant.push(pid);
            process.push(1_i32); // nonroad emission process
            model_year.push(row.model_year.unwrap_or(0));
            scc_out.push(row.scc.clone());
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
        "modelYearID" => model_year,
        "SCC" => scc_out,
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
            "nrfuelsupply",
            "fuelformulation",
            "nrfuelsubtype",
            "zonemonthhour",
            "runspecmonth",
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
        let inputs = build_nonroad_inputs(&store, 2020, None);
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

        // Reproduce the engine run in-process (August weekday, typical-day).
        let options = build_options(2020, 8, 5);
        let mut executor = build_production_executor(&store, 2020, 8, 5, None);
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
        let temporal = build_temporal_factors(&store, 8, 5, None);
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

        // Per-model-year CO for 2265006030 vs canonical (recent years dominate).
        let can_my_co: BTreeMap<i32, f64> = [
            (2015, 4.37054e7),
            (2016, 1.22951e8),
            (2017, 1.76723e8),
            (2018, 2.66209e8),
            (2019, 3.07514e8),
            (2020, 3.31445e8),
            (2010, 0.0),
            (1990, 0.0),
            (1980, 0.0),
        ]
        .into_iter()
        .collect();
        let tf = scc_lookup(&temporal, "2265006030").copied().unwrap_or(1.0);
        let mut my_co: BTreeMap<i32, f64> = BTreeMap::new();
        for r in &out.rows {
            if r.scc == "2265006030" {
                if let Some(my) = r.model_year {
                    *my_co.entry(my).or_default() += r.emissions[1] as f64 * tf * g;
                }
            }
        }
        eprintln!("2265006030 per-MY CO (mine vs canonical):");
        for my in [1990, 2010, 2015, 2016, 2017, 2018, 2019, 2020] {
            let mine = my_co.get(&my).copied().unwrap_or(0.0);
            let canon = can_my_co.get(&my).copied();
            eprintln!("  MY {my}: mine={mine:.3e} canon={canon:?}");
        }
        eprintln!("2265006030 distinct model years emitted: {}", my_co.len());
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
        assert_eq!(bin0.emission_factors[0], 261.0);
        // CO (slot 1), NOx (slot 2), PM (slot 5).
        assert_eq!(bin0.emission_factors[n_tech], 733.0);
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

    // ---- build_temporal_profiles_for_period --------------------------------

    fn month_alloc_df() -> DataFrame {
        df!(
            "SCC"           => ["2260001000", "2260001000", "2260002000"],
            "stateID"       => [0i64, 0i64, 0i64],
            "monthID"       => [8i64, 9i64, 8i64],
            "monthFraction" => ["0.1", "0.09", "0.08"]
        )
        .unwrap()
    }

    fn day_alloc_df() -> DataFrame {
        df!(
            "scc"         => ["2260001000", "2260001000"],
            "dayID"       => [5i64, 2i64],
            "dayFraction" => ["0.166667", "0.083333"]
        )
        .unwrap()
    }

    #[test]
    fn temporal_profiles_loaded_from_month_and_day_tables() {
        let mut store = InMemoryStore::new();
        store.insert("nrmonthallocation", month_alloc_df());
        store.insert("nrdayallocation", day_alloc_df());

        let (profiles, months_selected, weekday_selected) =
            build_temporal_profiles_for_period(&store, 8, 5, None);

        let p = profiles.get("2260001000").expect("profile for 2260001000");
        assert!((p.monthly[7] - 0.1).abs() < 1e-5, "August fraction");
        assert!((p.monthly[8] - 0.09).abs() < 1e-5, "September fraction");
        assert!((p.daily[0] - 0.166667).abs() < 1e-4, "weekday fraction");
        assert!((p.daily[1] - 0.083333).abs() < 1e-4, "weekend fraction");

        assert!(months_selected[7], "August must be selected");
        assert!(!months_selected[0], "January must not be selected");
        assert!(weekday_selected, "dayID=5 means weekday");
    }

    #[test]
    fn period_flags_all_months_for_annual_run() {
        let store = InMemoryStore::new();
        let (_, months_selected, weekday_selected) =
            build_temporal_profiles_for_period(&store, 0, 2, None);
        assert!(
            months_selected.iter().all(|&m| m),
            "month=0 means all 12 selected"
        );
        assert!(!weekday_selected, "dayID=2 means weekend");
    }
}

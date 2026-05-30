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

use moves_framework::data::{DataFrameStore, InMemoryStore};
use moves_nonroad::emissions::exhaust::EmissionUnitCode;
use moves_nonroad::simulation::ExhaustTechEntry;
use polars::prelude::*;

use moves_nonroad::common::consts::MXPOL;

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

    let mut entries = Vec::with_capacity(buckets.len());
    for ((scc, hp_min, hp_max), bucket) in buckets {
        let n_tech = bucket.tech_ids.len().max(1);
        let mut emission_factors = vec![0.0_f32; MXPOL * n_tech];
        let mut emission_units = vec![EmissionUnitCode::GramsPerHpHour; MXPOL * n_tech];
        let mut det_a = vec![0.0_f32; MXPOL * n_tech];
        let mut det_b = vec![0.0_f32; MXPOL * n_tech];
        let mut det_cap = vec![0.0_f32; MXPOL * n_tech];
        let mut bsfc = vec![0.0_f32; n_tech];

        for (&(pslot, tslot), &r) in &bucket.rates {
            let idx = pslot * n_tech + tslot;
            emission_factors[idx] = r;
            if let Some(u) = bucket.units.get(&(pslot, tslot)) {
                emission_units[idx] = *u;
            }
            // Deterioration is keyed by (polProcessID, engTechID). Recover
            // the polProcessID from the pollutant slot.
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
            bsfc[tslot] = v;
        }

        entries.push(ExhaustTechEntry {
            scc,
            hp_min: hp_min as f32,
            hp_max: hp_max as f32,
            tech_names: bucket.tech_ids.iter().map(|t| t.to_string()).collect(),
            // Filled by the tech-fraction builder; the rates above are
            // model-year independent but the tech mix is not.
            tech_fractions: vec![0.0; n_tech],
            bsfc,
            emission_factors,
            emission_units,
            det_a,
            det_b,
            det_cap,
        });
    }
    entries
}

#[cfg(test)]
mod tests {
    use super::*;

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

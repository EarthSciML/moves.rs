//! Source-bin-weighted emission rates — the port of
//! `BaseRateGenerator.generateSBWeightedEmissionRates`
//! (`gov/epa/otaq/moves/master/implementation/ghg/BaseRateGenerator.java`).
//!
//! Canonical MOVES weights the raw per-source-bin emission rates
//! (`EmissionRate` / `EmissionRateByAge`) by the runtime `SourceBinDistribution`
//! activity fractions, producing `SBWeightedEmissionRate` /
//! `SBWeightedEmissionRateByAge`. Those weighted tables are what the rest of the
//! Base Rate path consumes. In the Rust port the computation was previously
//! missing for the default-DB path: the generator read the (never-produced)
//! `SBWeighted*` tables, got empty input, and emitted zero base rates — so the
//! whole onroad inventory came out empty. This module computes them in-process
//! from the tables the `SourceBinDistributionGenerator` and the default DB
//! already provide.
//!
//! Inventory scale uses `normalizationFactor = 1` (no division); only
//! Mesoscale-Lookup normalises by `SUM(sourceBinActivityFraction)`. The EV-sales
//! `evMultiplier` adjustment (canonical step 010) is a follow-up refinement and
//! is not yet applied here.

use std::collections::BTreeMap;

use polars::prelude::DataType;
use rustc_hash::{FxHashMap, FxHashSet};

use moves_framework::{DataFrameStoreTyped, Error};

use super::model::SbWeightedRateDetail;

/// Extract one integer column, casting i16/i32/i64 uniformly to `i64`. A NULL
/// becomes `0` (these are key columns — the canonical tables carry no NULLs).
fn col_i64<S: DataFrameStoreTyped + ?Sized>(
    store: &S,
    table: &str,
    column: &str,
) -> Result<Vec<i64>, Error> {
    let views = store.column_views(table, &[column])?;
    let s = views[0]
        .cast(&DataType::Int64)
        .map_err(|e| Error::Polars(e.to_string()))?;
    let ca = s.i64().map_err(|e| Error::Polars(e.to_string()))?;
    Ok(ca.into_iter().map(|v| v.unwrap_or(0)).collect())
}

/// Extract one floating column, casting f32/f64 uniformly to `f64`. NULL → 0.0.
fn col_f64<S: DataFrameStoreTyped + ?Sized>(
    store: &S,
    table: &str,
    column: &str,
) -> Result<Vec<f64>, Error> {
    let views = store.column_views(table, &[column])?;
    let s = views[0]
        .cast(&DataType::Float64)
        .map_err(|e| Error::Polars(e.to_string()))?;
    let ca = s.f64().map_err(|e| Error::Polars(e.to_string()))?;
    Ok(ca.into_iter().map(|v| v.unwrap_or(0.0)).collect())
}

/// Accumulator for one output group (the `SUM(...)` aggregates).
#[derive(Default, Clone, Copy)]
struct Acc {
    mean_base_rate: f64,
    mean_base_rate_im: f64,
    mean_base_rate_ac_adj: f64,
    mean_base_rate_im_ac_adj: f64,
    sum_sbd: f64,
}

/// One raw `EmissionRate[ByAge]` row, normalised to the columns the weighting
/// needs. `age_group_id` is `0` for the non-age `EmissionRate` table.
#[derive(Clone, Copy)]
struct RateRow {
    op_mode_id: i64,
    age_group_id: i64,
    mean_base_rate: f64,
    mean_base_rate_im: f64,
}

/// Output of [`compute_sb_weighted_rates`].
pub struct SbWeightedRates {
    /// `SBWeightedEmissionRateByAge` rows (carry `ageGroupID`).
    pub by_age: Vec<SbWeightedRateDetail>,
    /// `SBWeightedEmissionRate` rows (`ageGroupID = 0`).
    pub non_age: Vec<SbWeightedRateDetail>,
}

/// Read the `EmissionRate[ByAge]` table and index it by `(polProcessID,
/// sourceBinID)`. `with_age` selects the age-based table/column set.
fn index_rates<S: DataFrameStoreTyped + ?Sized>(
    store: &S,
    table: &str,
    with_age: bool,
) -> Result<FxHashMap<(i64, i64), Vec<RateRow>>, Error> {
    let pol = col_i64(store, table, "polProcessID")?;
    let bin = col_i64(store, table, "sourceBinID")?;
    let opm = col_i64(store, table, "opModeID")?;
    let mbr = col_f64(store, table, "meanBaseRate")?;
    let mbr_im = col_f64(store, table, "meanBaseRateIM")?;
    let age = if with_age {
        col_i64(store, table, "ageGroupID")?
    } else {
        vec![0; pol.len()]
    };
    let mut index: FxHashMap<(i64, i64), Vec<RateRow>> = FxHashMap::default();
    for i in 0..pol.len() {
        index.entry((pol[i], bin[i])).or_default().push(RateRow {
            op_mode_id: opm[i],
            age_group_id: age[i],
            mean_base_rate: mbr[i],
            mean_base_rate_im: mbr_im[i],
        });
    }
    Ok(index)
}

/// Compute `SBWeightedEmissionRate` and `SBWeightedEmissionRateByAge` for one
/// `(processID, year)` — the port of
/// `BaseRateGenerator.generateSBWeightedEmissionRates` at inventory scale.
///
/// Joins `EmissionRate[ByAge]` ⋈ `SourceBin` ⋈ `SourceBinDistribution` ⋈
/// `SourceTypeModelYear` ⋈ `PollutantProcessModelYear` ⋈ `PollutantProcessAssoc`,
/// left-joins `fullACAdjustment`, and sums the activity-weighted rates grouped by
/// the output key. The canonical `RunspecModelYear[AgeGroup]` filter is derived
/// from `AgeCategory` + `year` (`modelYearID = year - ageID`,
/// `ageGroupID = AgeCategory[ageID]`), since those runtime tables are not
/// materialised in the port.
pub fn compute_sb_weighted_rates<S: DataFrameStoreTyped + ?Sized>(
    store: &S,
    process_id: i64,
    county_id: i64,
    year: i64,
) -> Result<SbWeightedRates, Error> {
    // Canonical `BaseRateGenerator.generateSBWeightedEmissionRates` weights the
    // raw rates by the FUEL-USAGE-remapped source-bin distribution
    // (`sourceBinDistributionFuelUsage_<process>_<county>_<year>`, built by
    // SourceBinDistributionGenerator.doCountyYear), NOT the raw
    // `SourceBinDistribution`. The remap reattributes each flex-fuel vehicle's
    // activity from its *equipped* fuel bin (e.g. E85) to the fuel it actually
    // *burns* (mostly gasoline) via `fuelUsageFraction`. Using the raw
    // (equipped) distribution over-weights E85 energy ~50× and under-weights
    // gasoline (growing with model year as flex-fuel penetration rises). Prefer
    // the fuel-usage table; fall back to raw `SourceBinDistribution` only when
    // it is absent (unit-test contexts that seed the raw table directly).
    let fuel_usage_table = format!("sourceBinDistributionFuelUsage_{process_id}_{county_id}_{year}");
    let sbd_table: &str = if store.get(&fuel_usage_table).is_some() {
        &fuel_usage_table
    } else {
        "SourceBinDistribution"
    };

    // Nothing to weight when the driver tables are absent (a context that
    // supplies the `SBWeighted*` tables directly), rather than erroring on a
    // missing column.
    if store.get(sbd_table).is_none() || store.get("EmissionRateByAge").is_none() {
        return Ok(SbWeightedRates {
            by_age: Vec::new(),
            non_age: Vec::new(),
        });
    }

    // PollutantProcessAssoc: polProcessIDs belonging to this process.
    let ppa_pol = col_i64(store, "PollutantProcessAssoc", "polProcessID")?;
    let ppa_proc = col_i64(store, "PollutantProcessAssoc", "processID")?;
    let process_pol_procs: FxHashSet<i64> = ppa_pol
        .iter()
        .zip(&ppa_proc)
        .filter(|(_, &p)| p == process_id)
        .map(|(&pp, _)| pp)
        .collect();

    // SourceBin: sourceBinID → (fuelTypeID, regClassID, modelYearGroupID).
    let sb_id = col_i64(store, "SourceBin", "sourceBinID")?;
    let sb_fuel = col_i64(store, "SourceBin", "fuelTypeID")?;
    let sb_reg = col_i64(store, "SourceBin", "regClassID")?;
    let sb_myg = col_i64(store, "SourceBin", "modelYearGroupID")?;
    let mut source_bin: FxHashMap<i64, (i64, i64, i64)> = FxHashMap::default();
    for i in 0..sb_id.len() {
        source_bin.insert(sb_id[i], (sb_fuel[i], sb_reg[i], sb_myg[i]));
    }

    // SourceTypeModelYear: sourceTypeModelYearID → (sourceTypeID, modelYearID).
    let stmy_id = col_i64(store, "SourceTypeModelYear", "sourceTypeModelYearID")?;
    let stmy_st = col_i64(store, "SourceTypeModelYear", "sourceTypeID")?;
    let stmy_my = col_i64(store, "SourceTypeModelYear", "modelYearID")?;
    let mut stmy: FxHashMap<i64, (i64, i64)> = FxHashMap::default();
    for i in 0..stmy_id.len() {
        stmy.insert(stmy_id[i], (stmy_st[i], stmy_my[i]));
    }

    // PollutantProcessModelYear: valid (polProcessID, modelYearID, modelYearGroupID).
    let ppmy_pol = col_i64(store, "PollutantProcessModelYear", "polProcessID")?;
    let ppmy_my = col_i64(store, "PollutantProcessModelYear", "modelYearID")?;
    let ppmy_myg = col_i64(store, "PollutantProcessModelYear", "modelYearGroupID")?;
    let mut ppmy: FxHashSet<(i64, i64, i64)> = FxHashSet::default();
    for i in 0..ppmy_pol.len() {
        ppmy.insert((ppmy_pol[i], ppmy_my[i], ppmy_myg[i]));
    }

    // fullACAdjustment: (sourceTypeID, polProcessID, opModeID) → factor.
    let fac_st = col_i64(store, "fullACAdjustment", "sourceTypeID")?;
    let fac_pol = col_i64(store, "fullACAdjustment", "polProcessID")?;
    let fac_opm = col_i64(store, "fullACAdjustment", "opModeID")?;
    let fac_val = col_f64(store, "fullACAdjustment", "fullACAdjustment")?;
    let mut fac: FxHashMap<(i64, i64, i64), f64> = FxHashMap::default();
    for i in 0..fac_st.len() {
        fac.insert((fac_st[i], fac_pol[i], fac_opm[i]), fac_val[i]);
    }

    // RunspecModelYear[AgeGroup] derived from AgeCategory + year:
    //   valid modelYears        = { year - ageID }
    //   valid (modelYear, group) = { (year - ageID, AgeCategory[ageID].ageGroupID) }
    let age_id = col_i64(store, "AgeCategory", "ageID")?;
    let age_grp = col_i64(store, "AgeCategory", "ageGroupID")?;
    let mut valid_model_years: FxHashSet<i64> = FxHashSet::default();
    let mut valid_my_group: FxHashSet<(i64, i64)> = FxHashSet::default();
    for i in 0..age_id.len() {
        let my = year - age_id[i];
        valid_model_years.insert(my);
        valid_my_group.insert((my, age_grp[i]));
    }

    // EmissionRate[ByAge] indexed by (polProcessID, sourceBinID).
    let rates_by_age = index_rates(store, "EmissionRateByAge", true)?;
    let rates_non_age = index_rates(store, "EmissionRate", false)?;

    // The fuel-usage-remapped SBD (or raw, as fallback) — the driver (small: one
    // row per (stmy, polProcess, bin) for the run's source bins).
    let sbd_stmy = col_i64(store, sbd_table, "sourceTypeModelYearID")?;
    let sbd_pol = col_i64(store, sbd_table, "polProcessID")?;
    let sbd_bin = col_i64(store, sbd_table, "sourceBinID")?;
    let sbd_frac = col_f64(store, sbd_table, "sourceBinActivityFraction")?;

    // Output group keys: byAge carries ageGroupID, non-age does not.
    let mut by_age: BTreeMap<(i64, i64, i64, i64, i64, i64, i64), Acc> = BTreeMap::new();
    let mut non_age: BTreeMap<(i64, i64, i64, i64, i64, i64), Acc> = BTreeMap::new();

    for i in 0..sbd_stmy.len() {
        let pol_process_id = sbd_pol[i];
        if !process_pol_procs.contains(&pol_process_id) {
            continue;
        }
        let source_bin_id = sbd_bin[i];
        let sbaf = sbd_frac[i];

        let Some(&(source_type_id, model_year_id)) = stmy.get(&sbd_stmy[i]) else {
            continue;
        };
        let Some(&(fuel_type_id, reg_class_id, model_year_group_id)) = source_bin.get(&source_bin_id)
        else {
            continue;
        };
        // ppmy.modelYearGroupID = sb.modelYearGroupID AND ppmy.modelYearID = stmy.modelYearID.
        if !ppmy.contains(&(pol_process_id, model_year_id, model_year_group_id)) {
            continue;
        }

        // --- age-based weighting ---------------------------------------------
        if valid_model_years.contains(&model_year_id) {
            if let Some(rows) = rates_by_age.get(&(pol_process_id, source_bin_id)) {
                for r in rows {
                    if !valid_my_group.contains(&(model_year_id, r.age_group_id)) {
                        continue;
                    }
                    let ac = fac
                        .get(&(source_type_id, pol_process_id, r.op_mode_id))
                        .copied()
                        .unwrap_or(1.0);
                    let acc = by_age
                        .entry((
                            source_type_id,
                            pol_process_id,
                            model_year_id,
                            fuel_type_id,
                            r.op_mode_id,
                            r.age_group_id,
                            reg_class_id,
                        ))
                        .or_default();
                    accumulate(acc, sbaf, r, ac);
                }
            }
        }

        // --- non-age weighting -----------------------------------------------
        if valid_model_years.contains(&model_year_id) {
            if let Some(rows) = rates_non_age.get(&(pol_process_id, source_bin_id)) {
                for r in rows {
                    let ac = fac
                        .get(&(source_type_id, pol_process_id, r.op_mode_id))
                        .copied()
                        .unwrap_or(1.0);
                    let acc = non_age
                        .entry((
                            source_type_id,
                            pol_process_id,
                            model_year_id,
                            fuel_type_id,
                            r.op_mode_id,
                            reg_class_id,
                        ))
                        .or_default();
                    accumulate(acc, sbaf, r, ac);
                }
            }
        }
    }

    // Emit groups with SUM(sourceBinActivityFraction) > 0 (the SQL HAVING clause).
    let by_age_out = by_age
        .into_iter()
        .filter(|(_, a)| a.sum_sbd > 0.0)
        .map(|(k, a)| SbWeightedRateDetail {
            source_type_id: k.0 as i32,
            pol_process_id: k.1 as i32,
            model_year_id: k.2 as i32,
            fuel_type_id: k.3 as i32,
            op_mode_id: k.4 as i32,
            age_group_id: k.5 as i32,
            reg_class_id: k.6 as i32,
            sum_sbd: a.sum_sbd,
            sum_sbd_raw: a.sum_sbd,
            mean_base_rate: a.mean_base_rate,
            mean_base_rate_im: a.mean_base_rate_im,
            mean_base_rate_ac_adj: a.mean_base_rate_ac_adj,
            mean_base_rate_im_ac_adj: a.mean_base_rate_im_ac_adj,
        })
        .collect();
    let non_age_out = non_age
        .into_iter()
        .filter(|(_, a)| a.sum_sbd > 0.0)
        .map(|(k, a)| SbWeightedRateDetail {
            source_type_id: k.0 as i32,
            pol_process_id: k.1 as i32,
            model_year_id: k.2 as i32,
            fuel_type_id: k.3 as i32,
            op_mode_id: k.4 as i32,
            age_group_id: 0,
            reg_class_id: k.5 as i32,
            sum_sbd: a.sum_sbd,
            sum_sbd_raw: a.sum_sbd,
            mean_base_rate: a.mean_base_rate,
            mean_base_rate_im: a.mean_base_rate_im,
            mean_base_rate_ac_adj: a.mean_base_rate_ac_adj,
            mean_base_rate_im_ac_adj: a.mean_base_rate_im_ac_adj,
        })
        .collect();

    Ok(SbWeightedRates {
        by_age: by_age_out,
        non_age: non_age_out,
    })
}

/// Apply one `(sbaf, rate, acFactor)` contribution to a group accumulator,
/// matching the canonical `SUM(...)` expressions (`normalizationFactor = 1`).
fn accumulate(acc: &mut Acc, sbaf: f64, r: &RateRow, ac_factor: f64) {
    acc.mean_base_rate += sbaf * r.mean_base_rate;
    acc.mean_base_rate_im += sbaf * r.mean_base_rate_im;
    acc.mean_base_rate_ac_adj += sbaf * r.mean_base_rate * (ac_factor - 1.0);
    acc.mean_base_rate_im_ac_adj += sbaf * r.mean_base_rate_im * (ac_factor - 1.0);
    acc.sum_sbd += sbaf;
}

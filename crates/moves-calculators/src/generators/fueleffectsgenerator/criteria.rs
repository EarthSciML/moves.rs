//! Default-DB `criteriaRatio` — the criteria-pollutant (THC/CO/NOx running +
//! start) fuel-effect ratios the canonical `FuelEffectsGenerator` builds and the
//! general-fuel-ratio port omits.
//!
//! # Why this exists
//!
//! The `generalFuelRatioExpression` table carries the criteria fuel effects only
//! for model years ≥ 2001 (the sulfur model is pre-baked into closed-form
//! expressions there). For MY ≤ 2000 the canonical
//! `FuelEffectsGenerator.doNOxCalculations` / `doHCCalculations` /
//! `doCOCalculations` instead run the live Complex Model
//! (`complexModelParameters` polynomials over fuel properties) followed by the
//! sulfur model (`database/SulfurModel.sql`). Because the general-fuel-ratio
//! port reproduces neither — and because `generalFuelRatio` holds only one
//! model-year range per `(formulation, polProcess, sourceType)` so the per-year
//! criteria ratios cannot live there — a default-DB run applies **no** criteria
//! fuel reduction for MY ≤ 2000 and over-emits (NOx ~1.27×, THC ~1.21×,
//! CO ~1.03×).
//!
//! [`build_criteria_ratio_rows`] builds the full `criteriaRatio` table for the
//! default-DB path, per `(formulation, criteria polProcess, sourceType,
//! modelYear, ageID)`:
//!
//! - MY ≤ 2000 gasoline (fuelType 1): the live Complex + sulfur model below.
//! - Every other `(fuelType, modelYear)`: evaluate the matching
//!   `generalFuelRatioExpression` (the same closed-form the generator uses for
//!   `generalFuelRatio`).
//!
//! It runs only on the default-DB path (gated on `criteriaRatio` being empty;
//! the snapshot/onroad path ships it captured, nonroad lacks the model tables).
//! The `FuelEffectsGenerator` then drops the model-year-overlapping criteria
//! rows from `generalFuelRatio`, so each effect applies exactly once.
//!
//! # Complex + sulfur model (verified bit-exact against `expand-criteria`)
//!
//! `ratio = ratioNoSulfur · sulfEff`, where
//!
//! - `ratioNoSulfur`: NOx/HC use the *predictive* form
//!   `Σ_fm w·exp(target) / Σ_fm w·exp(base)`; CO uses the *atDifferenceFraction*
//!   form `1 + Σ_fm w·(exp(target)/exp(base) − 1)`. Each fuel-model sum is
//!   `Σ_cmp coeff · Π_param ((fp − center)/stdDev)` over the calculation
//!   engine's fuel models. CO weights are age-specific at `modelYearGroupID
//!   19502000`; NOx/HC fall back to the uniform `@0` weights. CO aliases the
//!   target's sulfur to the base's; NOx/HC do not.
//! - `sulfEff`: `blend(targetSulfur)/blend(baseSulfur=30)`, with
//!   `blend(s) = ½·sa3(s, Normal) + ½·sa3(s, High)` and
//!   `sa3(s) = max(sulfShort(s)/sulfShort(30), minSulfAdjust=0.5)`. For MY ≤ 2000
//!   there is no `M6SulfurCoeff` row (`sulfurIRFactor 0`, `sulfurLongCoeff 1`),
//!   so the IR/GPA terms collapse to this short form.

use std::collections::{BTreeMap, BTreeSet};

use moves_framework::{DataFrameStore, DataFrameStoreTyped, InMemoryStore};
use polars::prelude::{DataFrame, DataType};

use super::expression::Expression;
use super::model::FuelFormulation;
use super::{FuelFormulationRow, GeneralFuelRatioExpressionRow};

/// The CMP (Complex Model Parameter) polynomial table: `cmpID` → the fuel
/// property name(s) whose product (after centering/standardizing) the term
/// multiplies. A single-element entry is a linear term, a two-element entry a
/// product (or, when both names are equal, a square). Ports the
/// `complexModelParameterName` expression forms, which are fixed model
/// structure rather than run data.
fn cmp_params(cmp_id: i32) -> Option<&'static [&'static str]> {
    Some(match cmp_id {
        1 => &["Oxygen"],
        2 => &["Sulfur"],
        3 => &["RVP"],
        4 => &["E200"],
        5 => &["E300"],
        6 => &["Aromatics"],
        7 => &["Olefins"],
        8 => &["Benzene"],
        9 => &["Oxygen", "Oxygen"],
        10 => &["Sulfur", "Sulfur"],
        11 => &["RVP", "RVP"],
        12 => &["E200", "E200"],
        13 => &["E300", "E300"],
        14 => &["Aromatics", "Aromatics"],
        15 => &["Olefins", "Olefins"],
        16 => &["Benzene", "Benzene"],
        17 => &["Oxygen", "Sulfur"],
        18 => &["Oxygen", "RVP"],
        19 => &["Oxygen", "E200"],
        20 => &["Oxygen", "E300"],
        21 => &["Oxygen", "Aromatics"],
        22 => &["Oxygen", "Olefins"],
        23 => &["Oxygen", "Benzene"],
        24 => &["Sulfur", "RVP"],
        25 => &["Sulfur", "E200"],
        26 => &["Sulfur", "E300"],
        27 => &["Sulfur", "Aromatics"],
        28 => &["Sulfur", "Olefins"],
        29 => &["Sulfur", "Benzene"],
        30 => &["RVP", "E200"],
        31 => &["RVP", "E300"],
        32 => &["RVP", "Aromatics"],
        33 => &["RVP", "Olefins"],
        34 => &["RVP", "Benzene"],
        35 => &["E200", "E300"],
        36 => &["E200", "Aromatics"],
        37 => &["E200", "Olefins"],
        38 => &["E200", "Benzene"],
        39 => &["E300", "Aromatics"],
        40 => &["E300", "Olefins"],
        41 => &["E300", "Benzene"],
        42 => &["Aromatics", "Olefins"],
        43 => &["Aromatics", "Benzene"],
        44 => &["Olefins", "Benzene"],
        45 => &["MTBE"],
        46 => &["ETBE"],
        47 => &["Ethanol"],
        48 => &["TAME"],
        49 => &["MTBE", "MTBE"],
        50 => &["ETBE", "ETBE"],
        51 => &["Ethanol", "Ethanol"],
        52 => &["Intercept"],
        53 => &["Hi"],
        54 => &["T50"],
        55 => &["T90"],
        56 => &["T90", "T90"],
        57 => &["T50", "T50"],
        58 => &["Oxygen", "T90"],
        59 => &["Sulfur", "Hi"],
        60 => &["Aromatics", "T90"],
        61 => &["T50", "Hi"],
        62 => &["Olefins", "T90"],
        63 => &["Oxygen", "T50"],
        64 => &["Sulfur", "T90"],
        _ => return None,
    })
}

/// The criteria pollutant/process configuration. `pol_process_id` is
/// `pollutantID·100 + processID`; the predictive flag selects the
/// makeRatioNoSulfur vs makeAtDifferenceFraction form; the sulfur-alias flag
/// (CO only) replaces the target's sulfur with the base's in the complex model.
#[derive(Clone, Copy, Debug)]
pub struct CriteriaConfig {
    pub pol_process_id: i32,
    pub pollutant_id: i32,
    pub process_id: i32,
    /// `calculationEngines` substring used to select fuel models from
    /// `fuelModelName` (e.g. `predictNOx`, `predictHC`, `co`).
    pub engine: &'static str,
    /// Predictive (`Σw·exp/Σw·exp`) vs atDifferenceFraction (`1+Σw·(t/b−1)`).
    pub predictive: bool,
    /// CO aliases the target sulfur to the base's; NOx/HC do not.
    pub sulfur_alias: bool,
    /// modelYearGroupID for the `fuelModelWtFactor` lookup (falls back to `@0`).
    pub weight_model_year_group: i32,
}

/// The six criteria polProcesses, with their model configuration.
///
/// `weight_model_year_group` mirrors the canonical
/// `setFuelModelWtFactorVariables` lookup, which queries
/// `fuelModelWtFactor@modelYearGroupID` then falls back to `@0`. The verified
/// reference passes `19502000` for every pollutant; NOx/HC carry no `19502000`
/// weight rows and so resolve to the uniform `@0` weights, while CO's
/// age-specific weights live at `19502000`.
pub const CRITERIA_CONFIGS: [CriteriaConfig; 6] = [
    // NOx running (301) / start (302) — predictive.
    CriteriaConfig {
        pol_process_id: 301,
        pollutant_id: 3,
        process_id: 1,
        engine: "predictNOx",
        predictive: true,
        sulfur_alias: false,
        weight_model_year_group: 19502000,
    },
    CriteriaConfig {
        pol_process_id: 302,
        pollutant_id: 3,
        process_id: 2,
        engine: "predictNOx",
        predictive: true,
        sulfur_alias: false,
        weight_model_year_group: 19502000,
    },
    // THC running (101) / start (102) — predictive.
    CriteriaConfig {
        pol_process_id: 101,
        pollutant_id: 1,
        process_id: 1,
        engine: "predictHC",
        predictive: true,
        sulfur_alias: false,
        weight_model_year_group: 19502000,
    },
    CriteriaConfig {
        pol_process_id: 102,
        pollutant_id: 1,
        process_id: 2,
        engine: "predictHC",
        predictive: true,
        sulfur_alias: false,
        weight_model_year_group: 19502000,
    },
    // CO running (201) / start (202) — atDifferenceFraction, age-specific weights.
    CriteriaConfig {
        pol_process_id: 201,
        pollutant_id: 2,
        process_id: 1,
        engine: "co",
        predictive: false,
        sulfur_alias: true,
        weight_model_year_group: 19502000,
    },
    CriteriaConfig {
        pol_process_id: 202,
        pollutant_id: 2,
        process_id: 2,
        engine: "co",
        predictive: false,
        sulfur_alias: true,
        weight_model_year_group: 19502000,
    },
];

/// The complex-model modelYearGroupID gasoline criteria fuel effects key on:
/// the predictive model covers MY 1950–2000 (`baseFuel` group 19502000).
pub const COMPLEX_MODEL_YEAR_GROUP: i32 = 19502000;

/// The reference data the model reads from the default DB. All maps are built
/// once per run and shared across every `(ff, sourceType, MY, age)` evaluation.
pub struct CriteriaReference {
    /// `complexModelParameters`: polProcessID → list of (fuelModelID, cmpID, coeff).
    pub cmp: BTreeMap<i32, Vec<(i32, i32, f64)>>,
    /// `meanFuelParameters`: (polProcessID, fuelTypeID, modelYearGroupID, paramName)
    /// → (centeringValue, stdDevValue).
    pub mean_fuel: BTreeMap<(i32, i32, i32, String), (f64, f64)>,
    /// `fuelModelName`: engine substring → sorted fuelModelIDs.
    pub fuel_models: BTreeMap<String, Vec<i32>>,
    /// `fuelModelWtFactor`: (fuelModelID, modelYearGroupID, ageID) → weight.
    pub weights: BTreeMap<(i32, i32, i32), f64>,
    /// `baseFuel`: (engine, fuelTypeID, modelYearGroupID) → base fuelFormulationID.
    pub base_fuel: BTreeMap<(String, i32, i32), i32>,
    /// `sulfurModelCoeff`: (pollutantID, processID, sourceTypeID, M6emitterID,
    /// fuelMYGroupID) → (sulfurFunctionID, sulfurCoeff).
    pub sulfur_coeff: BTreeMap<(i32, i32, i32, i32, i32), (i32, f64)>,
    /// `sulfurModelName`: (M6EmitterID, sulfurFunctionID) → is log-log.
    pub sulfur_loglog: BTreeMap<(i32, i32), bool>,
}

/// Fuel properties standardized into the Complex Model's named variables.
/// Built per fuel formulation (and once for the base fuel).
struct Props {
    values: BTreeMap<&'static str, f64>,
}

impl Props {
    /// Derive the named complex-model properties from a fuel formulation's raw
    /// columns, mirroring `fuelParameterName`'s expressions: T50/T90 come from
    /// E200/E300, Oxygen from ethanol volume, and the oxygenate terms apply
    /// their volume-to-mass factors.
    fn new(ff: &FuelProps) -> Self {
        let e200 = ff.e200;
        let e300 = ff.e300;
        let mut values = BTreeMap::new();
        values.insert("Oxygen", 0.3653 * ff.etoh_volume);
        values.insert("Sulfur", ff.sulfur_level);
        values.insert("RVP", ff.rvp);
        values.insert("E200", e200);
        values.insert("E300", e300);
        values.insert("Aromatics", ff.aromatic_content);
        values.insert("Olefins", ff.olefin_content);
        values.insert("Benzene", ff.benzene_content);
        values.insert("T50", 2.0408163 * (147.91 - e200));
        values.insert("T90", 4.5454545 * (155.47 - e300));
        values.insert("Hi", 1.0);
        values.insert("Intercept", 1.0);
        values.insert("MTBE", 0.1786 * ff.mtbe_volume);
        values.insert("ETBE", 0.1533 * ff.etbe_volume);
        values.insert("Ethanol", 0.3488 * ff.etoh_volume);
        values.insert("TAME", 0.1636 * ff.tame_volume);
        Props { values }
    }

    fn get(&self, name: &str) -> f64 {
        self.values.get(name).copied().unwrap_or(0.0)
    }
}

impl FuelProps {
    fn from_model(ff: &FuelFormulation) -> Self {
        FuelProps {
            sulfur_level: f64::from(ff.sulfur_level),
            rvp: f64::from(ff.rvp),
            etoh_volume: f64::from(ff.etoh_volume),
            mtbe_volume: f64::from(ff.mtbe_volume),
            etbe_volume: f64::from(ff.etbe_volume),
            tame_volume: f64::from(ff.tame_volume),
            aromatic_content: f64::from(ff.aromatic_content),
            olefin_content: f64::from(ff.olefin_content),
            benzene_content: f64::from(ff.benzene_content),
            e200: f64::from(ff.e200),
            e300: f64::from(ff.e300),
        }
    }
}

/// The raw fuel-formulation columns the model reads. A projection of the
/// generator's `FuelFormulation` so the math module stays independent of it.
#[derive(Clone, Copy, Debug, Default)]
pub struct FuelProps {
    pub sulfur_level: f64,
    pub rvp: f64,
    pub etoh_volume: f64,
    pub mtbe_volume: f64,
    pub etbe_volume: f64,
    pub tame_volume: f64,
    pub aromatic_content: f64,
    pub olefin_content: f64,
    pub benzene_content: f64,
    pub e200: f64,
    pub e300: f64,
}

/// Map a model year to the `sulfurModelCoeff.fuelMYGroupID` whose range
/// `[round(g/10000)|1950, g%10000]` contains it.
fn sulfur_my_group(model_year: i32, groups: &[i32]) -> Option<i32> {
    groups.iter().copied().find(|&g| {
        let lo = if g / 10000 == 0 { 1950 } else { g / 10000 };
        let hi = g % 10000;
        model_year >= lo && model_year <= hi
    })
}

impl CriteriaReference {
    /// Distinct `sulfurModelCoeff.fuelMYGroupID`s, for the MY→group mapping.
    fn sulfur_groups(&self) -> Vec<i32> {
        let mut g: Vec<i32> = self.sulfur_coeff.keys().map(|k| k.4).collect();
        g.sort_unstable();
        g.dedup();
        g
    }
}

/// Compute `Σ_cmp coeff · Π_param ((prop − center)/stdDev)` for one fuel model.
fn fuel_model_sum(
    reference: &CriteriaReference,
    cfg: &CriteriaConfig,
    fuel_type_id: i32,
    fuel_model_id: i32,
    props: &Props,
    centers: &BTreeMap<String, (f64, f64)>,
) -> f64 {
    let Some(terms) = reference.cmp.get(&cfg.pol_process_id) else {
        return 0.0;
    };
    let mut sum = 0.0;
    for &(fm, cmp_id, coeff) in terms {
        if fm != fuel_model_id || coeff == 0.0 {
            continue;
        }
        let Some(params) = cmp_params(cmp_id) else {
            continue;
        };
        let mut v = 1.0;
        for &name in params {
            let (center, std) = centers.get(name).copied().unwrap_or((0.0, 1.0));
            v *= (props.get(name) - center) / std;
        }
        sum += coeff * v;
    }
    let _ = fuel_type_id;
    sum
}

/// `ratioNoSulfur` for a single target formulation (the complex/predictive
/// model). Returns `None` when the model has no data for this configuration
/// (e.g. the base fuel is undefined), in which case the caller emits no row.
fn ratio_no_sulfur(
    reference: &CriteriaReference,
    cfg: &CriteriaConfig,
    fuel_type_id: i32,
    age_id: i32,
    target: &FuelProps,
    base: &FuelProps,
) -> Option<f64> {
    let models = reference.fuel_models.get(cfg.engine)?;
    if models.is_empty() {
        return None;
    }

    // center/stdDev per property name, from meanFuelParameters@(polProc, ft,
    // COMPLEX_MODEL_YEAR_GROUP). Missing → (0, 1) (no centering/standardizing).
    let mut centers: BTreeMap<String, (f64, f64)> = BTreeMap::new();
    for ((pp, ft, grp, name), &(c, s)) in &reference.mean_fuel {
        if *pp == cfg.pol_process_id && *ft == fuel_type_id && *grp == COMPLEX_MODEL_YEAR_GROUP {
            let std = if s > 0.0 { s } else { 1.0 };
            centers.insert(name.clone(), (c, std));
        }
    }

    let base_props = Props::new(base);
    let mut target_props = Props::new(target);
    if cfg.sulfur_alias {
        // CO: alias the target sulfur to the base's (doCOCalculations).
        let bs = base_props.get("Sulfur");
        target_props.values.insert("Sulfur", bs);
    }

    let mut num = 0.0;
    let mut den = 0.0;
    for &fm in models {
        let w = reference
            .weights
            .get(&(fm, cfg.weight_model_year_group, age_id))
            .copied()
            .or_else(|| reference.weights.get(&(fm, 0, age_id)).copied())
            .unwrap_or(0.0);
        let ts = fuel_model_sum(reference, cfg, fuel_type_id, fm, &target_props, &centers);
        let bs = fuel_model_sum(reference, cfg, fuel_type_id, fm, &base_props, &centers);
        if cfg.predictive {
            num += w * ts.exp();
            den += w * bs.exp();
        } else {
            // atDifferenceFraction accumulates 1 + Σ w·(exp(t)/exp(b) − 1).
            num += w * ((ts - bs).exp() - 1.0);
        }
    }
    if cfg.predictive {
        if den == 0.0 {
            return None;
        }
        Some(num / den)
    } else {
        Some(1.0 + num)
    }
}

/// `sulfShort(s)` for one emitter: `exp(coeff·ln s)` (log-log) or
/// `exp(coeff·s)` (log-linear).
fn sulf_short(coeff: f64, log_log: bool, s: f64) -> f64 {
    if log_log {
        if s > 0.0 {
            (coeff * s.ln()).exp()
        } else {
            0.0
        }
    } else {
        (coeff * s).exp()
    }
}

/// The sulfur-model multiplier `blend(targetSulfur)/blend(baseSulfur=30)` for
/// MY ≤ 2000 (no `M6SulfurCoeff` row: `sulfurIRFactor 0`, `sulfurLongCoeff 1`,
/// so `sa3` collapses to `max(sulfShort(s)/sulfShort(30), 0.5)`).
fn sulf_eff(
    reference: &CriteriaReference,
    cfg: &CriteriaConfig,
    source_type_id: i32,
    sulfur_my_group_id: i32,
    target_sulfur: f64,
    base_sulfur: f64,
) -> Option<f64> {
    const MIN_SULF_ADJUST: f64 = 0.50;
    const SULFUR_BASIS: f64 = 30.0;
    // Emitters: 1 = Normal, 2 = High; blended 50/50.
    let sa3 = |s: f64| -> Option<f64> {
        let mut acc = 0.0;
        for emitter in [1, 2] {
            let (fn_id, coeff) = reference
                .sulfur_coeff
                .get(&(
                    cfg.pollutant_id,
                    cfg.process_id,
                    source_type_id,
                    emitter,
                    sulfur_my_group_id,
                ))
                .copied()?;
            let log_log = reference
                .sulfur_loglog
                .get(&(emitter, fn_id))
                .copied()
                .unwrap_or(false);
            let short_s = sulf_short(coeff, log_log, s);
            let short_30 = sulf_short(coeff, log_log, SULFUR_BASIS);
            let v = if short_30 != 0.0 {
                (short_s / short_30).max(MIN_SULF_ADJUST)
            } else {
                MIN_SULF_ADJUST
            };
            acc += 0.5 * v;
        }
        Some(acc)
    };
    let bt = sa3(target_sulfur)?;
    let bb = sa3(base_sulfur)?;
    if bb == 0.0 {
        return None;
    }
    Some(bt / bb)
}

/// Compute the MY ≤ 2000 gasoline criteria fuel-effect ratio for one
/// `(polProcess, sourceType, modelYear, ageID, target formulation)`.
///
/// Returns `(ratio, ratioNoSulfur)`, or `None` (emit no row, i.e. ratio 1.0)
/// when any required model input is absent for this configuration.
#[must_use]
pub fn criteria_ratio_my_le_2000(
    reference: &CriteriaReference,
    cfg: &CriteriaConfig,
    fuel_type_id: i32,
    source_type_id: i32,
    model_year: i32,
    age_id: i32,
    target: &FuelProps,
    base_formulation: &FuelProps,
) -> Option<(f64, f64)> {
    let rns = ratio_no_sulfur(
        reference,
        cfg,
        fuel_type_id,
        age_id,
        target,
        base_formulation,
    )?;
    let groups = reference.sulfur_groups();
    let group = sulfur_my_group(model_year, &groups)?;
    let se = sulf_eff(
        reference,
        cfg,
        source_type_id,
        group,
        target.sulfur_level,
        base_formulation.sulfur_level,
    )?;
    Some((rns * se, rns))
}

/// One output row of the default-DB `criteriaRatio` table.
#[derive(Clone, Copy, Debug)]
pub struct CriteriaRatioOutRow {
    pub fuel_type_id: i32,
    pub fuel_formulation_id: i32,
    pub pol_process_id: i32,
    pub pollutant_id: i32,
    pub process_id: i32,
    pub source_type_id: i32,
    pub model_year_id: i32,
    pub age_id: i32,
    pub ratio: f64,
    pub ratio_gpa: f64,
    pub ratio_no_sulfur: f64,
}

/// The base `fuelFormulationID` for a configuration's calculation engine
/// (predictNOx/predictHC/co), fuel type and the complex-model year group.
#[must_use]
pub fn base_formulation_id(
    reference: &CriteriaReference,
    cfg: &CriteriaConfig,
    fuel_type_id: i32,
) -> Option<i32> {
    reference
        .base_fuel
        .get(&(
            cfg.engine.to_string(),
            fuel_type_id,
            COMPLEX_MODEL_YEAR_GROUP,
        ))
        .copied()
}

// =============================================================================
// Default-DB reference-table loading
// =============================================================================

/// Read an `Int64`-castable column into `Vec<Option<i64>>` (case-insensitive
/// name match), or `None` when the column is absent.
fn icol(df: &DataFrame, name: &str) -> Option<Vec<Option<i64>>> {
    let c = df
        .columns()
        .iter()
        .find(|c| c.name().eq_ignore_ascii_case(name))?
        .cast(&DataType::Int64)
        .ok()?;
    let ca = c.i64().ok()?;
    Some((0..ca.len()).map(|i| ca.get(i)).collect())
}

/// Read a `Float64`-castable column into `Vec<Option<f64>>`.
fn fcol(df: &DataFrame, name: &str) -> Option<Vec<Option<f64>>> {
    let c = df
        .columns()
        .iter()
        .find(|c| c.name().eq_ignore_ascii_case(name))?
        .cast(&DataType::Float64)
        .ok()?;
    let ca = c.f64().ok()?;
    Some((0..ca.len()).map(|i| ca.get(i)).collect())
}

/// Read a string column into `Vec<Option<String>>`.
fn scol(df: &DataFrame, name: &str) -> Option<Vec<Option<String>>> {
    let c = df
        .columns()
        .iter()
        .find(|c| c.name().eq_ignore_ascii_case(name))?
        .cast(&DataType::String)
        .ok()?;
    let ca = c.str().ok()?;
    Some(
        (0..ca.len())
            .map(|i| ca.get(i).map(str::to_string))
            .collect(),
    )
}

/// Build the [`CriteriaReference`] from the default-DB tables in `store`.
/// Returns `None` (the model cannot run) when a required table is absent — e.g.
/// a nonroad run or a partial wasm partition.
#[must_use]
pub fn load_reference(store: &InMemoryStore) -> Option<CriteriaReference> {
    // complexModelParameters: coeff = coeff1+coeff2+coeff3.
    let cmp_df = store.get("complexModelParameters")?;
    let (pp, fm, cid) = (
        icol(&cmp_df, "polProcessID")?,
        icol(&cmp_df, "fuelModelID")?,
        icol(&cmp_df, "cmpID")?,
    );
    let (c1, c2, c3) = (
        fcol(&cmp_df, "coeff1")?,
        fcol(&cmp_df, "coeff2")?,
        fcol(&cmp_df, "coeff3")?,
    );
    let mut cmp: BTreeMap<i32, Vec<(i32, i32, f64)>> = BTreeMap::new();
    for i in 0..pp.len() {
        let (Some(p), Some(f), Some(c)) = (pp[i], fm[i], cid[i]) else {
            continue;
        };
        let coeff = c1[i].unwrap_or(0.0) + c2[i].unwrap_or(0.0) + c3[i].unwrap_or(0.0);
        cmp.entry(p as i32)
            .or_default()
            .push((f as i32, c as i32, coeff));
    }

    // meanFuelParameters ⋈ fuelParameterName → (polProc, ft, group, name) → (center, std).
    let fpn_df = store.get("fuelParameterName")?;
    let (fpn_id, fpn_name) = (
        icol(&fpn_df, "fuelParameterID")?,
        scol(&fpn_df, "fuelParameterName")?,
    );
    let mut param_name: BTreeMap<i64, String> = BTreeMap::new();
    for i in 0..fpn_id.len() {
        if let (Some(id), Some(n)) = (fpn_id[i], fpn_name[i].clone()) {
            param_name.insert(id, n);
        }
    }
    let mfp_df = store.get("meanFuelParameters")?;
    let (mpp, mft, mgrp, mpid) = (
        icol(&mfp_df, "polProcessID")?,
        icol(&mfp_df, "fuelTypeID")?,
        icol(&mfp_df, "modelYearGroupID")?,
        icol(&mfp_df, "fuelParameterID")?,
    );
    let (mctr, mstd) = (
        fcol(&mfp_df, "centeringValue")?,
        fcol(&mfp_df, "stdDevValue")?,
    );
    let mut mean_fuel: BTreeMap<(i32, i32, i32, String), (f64, f64)> = BTreeMap::new();
    for i in 0..mpp.len() {
        let (Some(p), Some(ft), Some(g), Some(pid)) = (mpp[i], mft[i], mgrp[i], mpid[i]) else {
            continue;
        };
        let Some(name) = param_name.get(&pid) else {
            continue;
        };
        let center = mctr[i].unwrap_or(0.0);
        let std = mstd[i].unwrap_or(1.0);
        mean_fuel.insert((p as i32, ft as i32, g as i32, name.clone()), (center, std));
    }

    // fuelModelName: engine substring → sorted fuelModelIDs.
    let fmn_df = store.get("fuelModelName")?;
    let (fmn_id, fmn_eng) = (
        icol(&fmn_df, "fuelModelID")?,
        scol(&fmn_df, "calculationEngines")?,
    );
    let mut fuel_models: BTreeMap<String, Vec<i32>> = BTreeMap::new();
    for cfg in &CRITERIA_CONFIGS {
        let needle = format!("|{}|", cfg.engine);
        let mut ids: Vec<i32> = (0..fmn_id.len())
            .filter_map(|i| {
                let id = fmn_id[i]?;
                let eng = fmn_eng[i].as_ref()?;
                eng.contains(&needle).then_some(id as i32)
            })
            .collect();
        ids.sort_unstable();
        ids.dedup();
        fuel_models.entry(cfg.engine.to_string()).or_insert(ids);
    }

    // fuelModelWtFactor: (fuelModelID, modelYearGroupID, ageID) → weight.
    let wt_df = store.get("fuelModelWtFactor")?;
    let (wfm, wgrp, wage) = (
        icol(&wt_df, "fuelModelID")?,
        icol(&wt_df, "modelYearGroupID")?,
        icol(&wt_df, "ageID")?,
    );
    let wval = fcol(&wt_df, "fuelModelWtFactor")?;
    let mut weights: BTreeMap<(i32, i32, i32), f64> = BTreeMap::new();
    for i in 0..wfm.len() {
        if let (Some(f), Some(g), Some(a), Some(v)) = (wfm[i], wgrp[i], wage[i], wval[i]) {
            weights.insert((f as i32, g as i32, a as i32), v);
        }
    }

    // baseFuel: (engine, fuelType, group) → fuelFormulationID.
    let bf_df = store.get("baseFuel")?;
    let (bf_eng, bf_ft, bf_grp, bf_ff) = (
        scol(&bf_df, "calculationEngine")?,
        icol(&bf_df, "fuelTypeID")?,
        icol(&bf_df, "modelYearGroupID")?,
        icol(&bf_df, "fuelFormulationID")?,
    );
    let mut base_fuel: BTreeMap<(String, i32, i32), i32> = BTreeMap::new();
    for i in 0..bf_eng.len() {
        if let (Some(e), Some(ft), Some(g), Some(ff)) =
            (bf_eng[i].clone(), bf_ft[i], bf_grp[i], bf_ff[i])
        {
            base_fuel.insert((e, ft as i32, g as i32), ff as i32);
        }
    }

    // sulfurModelCoeff: (pollutant, process, sourceType, M6emitter, fuelMYGroup)
    // → (sulfurFunctionID, sulfurCoeff). sulfurModelName: (emitter, fn) → log-log.
    let smc_df = store.get("sulfurModelCoeff")?;
    let (s_pol, s_proc, s_em, s_st, s_grp, s_fn) = (
        icol(&smc_df, "pollutantID")?,
        icol(&smc_df, "processID")?,
        icol(&smc_df, "M6emitterID")?,
        icol(&smc_df, "sourceTypeID")?,
        icol(&smc_df, "fuelMYGroupID")?,
        icol(&smc_df, "sulfurFunctionID")?,
    );
    let s_coeff = fcol(&smc_df, "sulfurCoeff")?;
    let mut sulfur_coeff: BTreeMap<(i32, i32, i32, i32, i32), (i32, f64)> = BTreeMap::new();
    for i in 0..s_pol.len() {
        if let (Some(pol), Some(proc), Some(em), Some(st), Some(g), Some(f), Some(co)) = (
            s_pol[i], s_proc[i], s_em[i], s_st[i], s_grp[i], s_fn[i], s_coeff[i],
        ) {
            sulfur_coeff.insert(
                (pol as i32, proc as i32, st as i32, em as i32, g as i32),
                (f as i32, co),
            );
        }
    }
    let smn_df = store.get("sulfurModelName")?;
    let (n_em, n_fn, n_name) = (
        icol(&smn_df, "M6EmitterID")?,
        icol(&smn_df, "sulfurFunctionID")?,
        scol(&smn_df, "sulfurFunctionName")?,
    );
    let mut sulfur_loglog: BTreeMap<(i32, i32), bool> = BTreeMap::new();
    for i in 0..n_em.len() {
        if let (Some(em), Some(f), Some(name)) = (n_em[i], n_fn[i], n_name[i].clone()) {
            sulfur_loglog.insert((em as i32, f as i32), name.eq_ignore_ascii_case("log-log"));
        }
    }

    Some(CriteriaReference {
        cmp,
        mean_fuel,
        fuel_models,
        weights,
        base_fuel,
        sulfur_coeff,
        sulfur_loglog,
    })
}

/// Per-(fuelType) supplied formulations: the full [`FuelFormulation`] model
/// keyed by ID, the fuelType of each ID, and the supplied set.
struct Formulations {
    by_id: BTreeMap<i32, FuelFormulation>,
    fuel_type: BTreeMap<i32, i32>,
    supplied: BTreeSet<i32>,
}

/// Read `FuelFormulation` (full models) + `FuelSubtype` (fuelType map) +
/// `FuelSupply` (supplied set) from the store.
fn load_formulations(store: &InMemoryStore) -> Option<Formulations> {
    let fst_df = store.get("FuelSubtype")?;
    let (fst_sub, fst_ft) = (
        icol(&fst_df, "fuelSubtypeID")?,
        icol(&fst_df, "fuelTypeID")?,
    );
    let mut subtype_to_type: BTreeMap<i32, i32> = BTreeMap::new();
    for i in 0..fst_sub.len() {
        if let (Some(s), Some(t)) = (fst_sub[i], fst_ft[i]) {
            subtype_to_type.insert(s as i32, t as i32);
        }
    }

    let ff_rows: Vec<FuelFormulationRow> = store.iter_typed("FuelFormulation").ok()?;
    let mut by_id: BTreeMap<i32, FuelFormulation> = BTreeMap::new();
    let mut fuel_type: BTreeMap<i32, i32> = BTreeMap::new();
    for r in ff_rows {
        let id = r.fuel_formulation_id;
        let Some(&ft) = subtype_to_type.get(&r.fuel_subtype_id) else {
            continue;
        };
        fuel_type.insert(id, ft);
        by_id.insert(id, r.into_model());
    }

    let fs_df = store.get("FuelSupply")?;
    let fs_ff = icol(&fs_df, "fuelFormulationID")?;
    let supplied: BTreeSet<i32> = fs_ff
        .into_iter()
        .flatten()
        .map(|v| v as i32)
        .filter(|f| by_id.contains_key(f))
        .collect();
    Some(Formulations {
        by_id,
        fuel_type,
        supplied,
    })
}

/// A criteria `generalFuelRatioExpression` row with its expressions pre-parsed.
struct ParsedExpr {
    min_model_year_id: i32,
    max_model_year_id: i32,
    min_age_id: i32,
    max_age_id: i32,
    ratio: Expression,
    gpa: Expression,
}

/// Pre-parsed criteria expressions, indexed by `(fuelTypeID, polProcessID,
/// sourceTypeID)`. Returns `None` only on a parse error (a malformed default DB).
fn load_criteria_expressions(
    store: &InMemoryStore,
) -> Option<BTreeMap<(i32, i32, i32), Vec<ParsedExpr>>> {
    let rows: Vec<GeneralFuelRatioExpressionRow> =
        store.iter_typed("generalFuelRatioExpression").ok()?;
    let mut map: BTreeMap<(i32, i32, i32), Vec<ParsedExpr>> = BTreeMap::new();
    for r in rows {
        if !CRITERIA_CONFIGS
            .iter()
            .any(|c| c.pol_process_id == r.pol_process_id)
        {
            continue;
        }
        let ratio_text = if r.fuel_effect_ratio_expression.is_empty() {
            "1"
        } else {
            &r.fuel_effect_ratio_expression
        };
        let gpa_text = if r.fuel_effect_ratio_gpa_expression.is_empty() {
            "1"
        } else {
            &r.fuel_effect_ratio_gpa_expression
        };
        let ratio = Expression::parse(ratio_text).ok()?;
        let gpa = Expression::parse(gpa_text).ok()?;
        map.entry((r.fuel_type_id, r.pol_process_id, r.source_type_id))
            .or_default()
            .push(ParsedExpr {
                min_model_year_id: r.min_model_year_id,
                max_model_year_id: r.max_model_year_id,
                min_age_id: r.min_age_id,
                max_age_id: r.max_age_id,
                ratio,
                gpa,
            });
    }
    Some(map)
}

/// Build the default-DB `criteriaRatio` table — the criteria (THC/CO/NOx
/// running+start) fuel-effect ratios for every supplied formulation.
///
/// MY ≤ 2000 gasoline (fuelType 1) uses the live EPA Complex + sulfur model
/// (`generalFuelRatioExpression` omits those years). Every other
/// `(fuelType, modelYear)` evaluates the matching `generalFuelRatioExpression`
/// (the same closed-form the generator uses for `generalFuelRatio`), so the
/// criteria effect is computed once, per model year, and the
/// `FuelEffectsGenerator` drops the model-year-overlapping criteria rows from
/// `generalFuelRatio`. Emits one row per `(supplied formulation, criteria
/// polProcess, runspec sourceType, modelYear, ageID)`.
///
/// Returns an empty vec (a no-op) when `criteriaRatio` is already populated (the
/// snapshot/onroad path) or when the model inputs are absent (nonroad).
#[must_use]
pub fn build_criteria_ratio_rows(store: &InMemoryStore) -> Vec<CriteriaRatioOutRow> {
    const FUEL_TYPE_GASOLINE: i32 = 1;

    // Gate: only the default-DB path (criteriaRatio empty) gets these rows.
    if store.get("criteriaRatio").is_some_and(|df| df.height() > 0) {
        return Vec::new();
    }
    let Some(reference) = load_reference(store) else {
        return Vec::new();
    };
    let (Some(forms), Some(expressions)) =
        (load_formulations(store), load_criteria_expressions(store))
    else {
        return Vec::new();
    };

    // Runspec sourceTypes.
    let Some(rsst) = store.get("RunSpecSourceType") else {
        return Vec::new();
    };
    let Some(st_ids) = icol(&rsst, "sourceTypeID") else {
        return Vec::new();
    };
    let source_types: Vec<i32> = st_ids.into_iter().flatten().map(|v| v as i32).collect();

    // Runspec (modelYear, ageID) pairs. The store carries no
    // `RunSpecModelYearAge` table; reconstruct it as canonical does — the cross
    // of `RunSpecYear` × `RunSpecModelYear` with `ageID = yearID − modelYearID`
    // restricted to ages [0, 40] (`modelYearID ∈ [yearID-40, yearID]`).
    let Some(rsy) = store.get("RunSpecYear") else {
        return Vec::new();
    };
    let Some(year_ids) = icol(&rsy, "yearID") else {
        return Vec::new();
    };
    let years: Vec<i32> = year_ids.into_iter().flatten().map(|v| v as i32).collect();
    let Some(rsmy) = store.get("RunSpecModelYear") else {
        return Vec::new();
    };
    let Some(rsmy_ids) = icol(&rsmy, "modelYearID") else {
        return Vec::new();
    };
    let model_years: Vec<i32> = rsmy_ids.into_iter().flatten().map(|v| v as i32).collect();
    let mut my_age: BTreeSet<(i32, i32)> = BTreeSet::new();
    for &year in &years {
        for &my in &model_years {
            let age = year - my;
            if (0..=40).contains(&age) {
                my_age.insert((my, age));
            }
        }
    }
    if my_age.is_empty() {
        return Vec::new();
    }
    let my_age: Vec<(i32, i32)> = my_age.into_iter().collect();

    // Complex-model base props per criteria config (gasoline MY ≤ 2000).
    let base_props: BTreeMap<i32, FuelProps> = CRITERIA_CONFIGS
        .iter()
        .filter_map(|cfg| {
            let ff = base_formulation_id(&reference, cfg, FUEL_TYPE_GASOLINE)?;
            Some((
                cfg.pol_process_id,
                FuelProps::from_model(forms.by_id.get(&ff)?),
            ))
        })
        .collect();

    let mut rows = Vec::new();
    for cfg in &CRITERIA_CONFIGS {
        for &target_id in &forms.supplied {
            let Some(ff_model) = forms.by_id.get(&target_id) else {
                continue;
            };
            let ft = forms.fuel_type.get(&target_id).copied().unwrap_or(0);
            let target_props = FuelProps::from_model(ff_model);
            for &st in &source_types {
                let exprs = expressions.get(&(ft, cfg.pol_process_id, st));
                for &(my, age) in &my_age {
                    // `(ratio, ratioGPA, ratioNoSulfur)`.
                    let result: Option<(f64, f64, f64)> = if ft == FUEL_TYPE_GASOLINE && my <= 2000
                    {
                        // Live Complex + sulfur model. No GPA phase-in for
                        // MY ≤ 2000, so ratioGPA == ratio.
                        base_props.get(&cfg.pol_process_id).and_then(|bp| {
                            criteria_ratio_my_le_2000(
                                &reference,
                                cfg,
                                ft,
                                st,
                                my,
                                age,
                                &target_props,
                                bp,
                            )
                            .map(|(ratio, rns)| (ratio, ratio, rns))
                        })
                    } else {
                        // Evaluate the matching closed-form expression. The
                        // sulfur model is baked into the expression, so
                        // ratioNoSulfur = 1.
                        exprs.and_then(|list| {
                            list.iter()
                                .find(|e| {
                                    my >= e.min_model_year_id
                                        && my <= e.max_model_year_id
                                        && age >= e.min_age_id
                                        && age <= e.max_age_id
                                })
                                .and_then(|e| {
                                    let r = e.ratio.evaluate(ff_model).ok()?;
                                    let g = e.gpa.evaluate(ff_model).ok()?;
                                    Some((r, g, 1.0))
                                })
                        })
                    };
                    let Some((ratio, ratio_gpa, ratio_no_sulfur)) = result else {
                        continue;
                    };
                    rows.push(CriteriaRatioOutRow {
                        fuel_type_id: ft,
                        fuel_formulation_id: target_id,
                        pol_process_id: cfg.pol_process_id,
                        pollutant_id: cfg.pollutant_id,
                        process_id: cfg.process_id,
                        source_type_id: st,
                        model_year_id: my,
                        age_id: age,
                        ratio,
                        ratio_gpa,
                        ratio_no_sulfur,
                    });
                }
            }
        }
    }
    rows
}

/// `fuelTypeID` for ethanol (E85) fuel.
const ETHANOL_FUEL_TYPE_ID: i32 = 5;
/// THC running/start `polProcessID`s the E85 pseudo-THC adjustment derives from.
const THC_PSEUDO_POL_PROCESS_IDS: [i32; 2] = [101, 102];
/// First model year the E85 pseudo-THC adjustment applies to.
const E85_MIN_MODEL_YEAR: i32 = 2001;
/// The E70/E85 fuel subtypes the pseudo-THC adjustment is restricted to.
const E85_FUEL_SUBTYPES: [i32; 2] = [51, 52];

/// Build the default-DB `altCriteriaRatio` table — the E85 "alternate" (E10-RVP)
/// THC fuel-effect ratios that the `HCSpeciationCalculator` needs to speciate
/// ethanol E70/E85 2001+ running/start NMOG and VOC.
///
/// Ports `FuelEffectsGenerator.copyGeneralFuelRatioToAltCriteriaRatio` (the
/// `altCriteriaRatio` half of the E85 "Pseudo-THC" path): the THC
/// `generalFuelRatioExpression` rows for ethanol (fuelType 5) running/start are
/// re-evaluated with `RVP` replaced by `altRVP` (the E10-equivalent RVP that
/// `transform_high_ethanol_fuel_properties` derives from `e10FuelProperties`),
/// restricted to the E70/E85 fuel subtypes, and expanded across the runspec
/// `(modelYear ≥ 2001, ageID)` grid. The result is keyed by the *normal* THC
/// `polProcessID`/`pollutantID` (1) — exactly the key `build_e85_block`
/// (`BaseRateCalculator`) looks up to emit the `altTHC` (10001) tally.
///
/// Emits one row per `(supplied E70/E85 formulation, THC polProcess, runspec
/// sourceType, modelYear ≥ 2001, ageID)`. Returns an empty vec (no-op) when
/// `altCriteriaRatio` is already populated (snapshot/onroad path) or the inputs
/// are absent (nonroad / non-ethanol runs).
#[must_use]
pub fn build_alt_criteria_ratio_rows(store: &InMemoryStore) -> Vec<CriteriaRatioOutRow> {
    // Gate: only the default-DB path (altCriteriaRatio empty) gets these rows.
    if store
        .get("altCriteriaRatio")
        .is_some_and(|df| df.height() > 0)
    {
        return Vec::new();
    }
    let Some(forms) = load_formulations(store) else {
        return Vec::new();
    };

    // Runspec sourceTypes — the pseudo-THC rows are scoped to them exactly as
    // `generalFuelRatioExpression` is filtered to the run's sourceTypes.
    let Some(rsst) = store.get("RunSpecSourceType") else {
        return Vec::new();
    };
    let Some(st_ids) = icol(&rsst, "sourceTypeID") else {
        return Vec::new();
    };
    let source_types: BTreeSet<i32> = st_ids.into_iter().flatten().map(|v| v as i32).collect();

    // Runspec (modelYear, ageID) grid — same reconstruction as
    // `build_criteria_ratio_rows`: RunSpecYear × RunSpecModelYear with
    // ageID = yearID − modelYearID restricted to [0, 40].
    let (Some(rsy), Some(rsmy)) = (store.get("RunSpecYear"), store.get("RunSpecModelYear")) else {
        return Vec::new();
    };
    let (Some(year_ids), Some(rsmy_ids)) = (icol(&rsy, "yearID"), icol(&rsmy, "modelYearID"))
    else {
        return Vec::new();
    };
    let years: Vec<i32> = year_ids.into_iter().flatten().map(|v| v as i32).collect();
    let model_years: Vec<i32> = rsmy_ids.into_iter().flatten().map(|v| v as i32).collect();
    let mut my_age: BTreeSet<(i32, i32)> = BTreeSet::new();
    for &year in &years {
        for &my in &model_years {
            let age = year - my;
            if (0..=40).contains(&age) {
                my_age.insert((my, age));
            }
        }
    }
    if my_age.is_empty() {
        return Vec::new();
    }

    // Ethanol THC running/start expressions, with RVP → altRVP (the pseudo-THC
    // derivation). Pre-parse once; index by sourceTypeID.
    let Ok(expr_rows) =
        store.iter_typed::<GeneralFuelRatioExpressionRow>("generalFuelRatioExpression")
    else {
        return Vec::new();
    };
    struct PseudoExpr {
        process_id: i32,
        source_type_id: i32,
        min_model_year_id: i32,
        max_model_year_id: i32,
        min_age_id: i32,
        max_age_id: i32,
        ratio: Expression,
        gpa: Expression,
    }
    let mut pseudo: Vec<PseudoExpr> = Vec::new();
    for r in &expr_rows {
        if r.fuel_type_id != ETHANOL_FUEL_TYPE_ID
            || !THC_PSEUDO_POL_PROCESS_IDS.contains(&r.pol_process_id)
            || r.max_model_year_id < E85_MIN_MODEL_YEAR
            || !source_types.contains(&r.source_type_id)
        {
            continue;
        }
        // Java replaces `RVP` with `altRVP`; `str::replace` never re-scans its
        // own output, matching the guarded canonical replace.
        let ratio_text = r.fuel_effect_ratio_expression.replace("RVP", "altRVP");
        let gpa_text = r.fuel_effect_ratio_gpa_expression.replace("RVP", "altRVP");
        let ratio_text = if ratio_text.is_empty() {
            "1".into()
        } else {
            ratio_text
        };
        let gpa_text = if gpa_text.is_empty() {
            "1".into()
        } else {
            gpa_text
        };
        let (Ok(ratio), Ok(gpa)) = (Expression::parse(&ratio_text), Expression::parse(&gpa_text))
        else {
            return Vec::new();
        };
        pseudo.push(PseudoExpr {
            process_id: r.pol_process_id % 100,
            source_type_id: r.source_type_id,
            min_model_year_id: r.min_model_year_id.max(E85_MIN_MODEL_YEAR),
            max_model_year_id: r.max_model_year_id,
            min_age_id: r.min_age_id,
            max_age_id: r.max_age_id,
            ratio,
            gpa,
        });
    }

    let mut rows = Vec::new();
    for &ffid in &forms.supplied {
        let Some(ff_model) = forms.by_id.get(&ffid) else {
            continue;
        };
        if !E85_FUEL_SUBTYPES.contains(&ff_model.fuel_subtype_id) {
            continue;
        }
        if forms.fuel_type.get(&ffid).copied() != Some(ETHANOL_FUEL_TYPE_ID) {
            continue;
        }
        for e in &pseudo {
            let (Ok(ratio), Ok(ratio_gpa)) = (e.ratio.evaluate(ff_model), e.gpa.evaluate(ff_model))
            else {
                continue;
            };
            for &(my, age) in &my_age {
                if my < e.min_model_year_id
                    || my > e.max_model_year_id
                    || age < e.min_age_id
                    || age > e.max_age_id
                {
                    continue;
                }
                rows.push(CriteriaRatioOutRow {
                    fuel_type_id: ETHANOL_FUEL_TYPE_ID,
                    fuel_formulation_id: ffid,
                    // Re-keyed to the normal THC pollutant for fast joins
                    // (canonical: `if(pollutantID>10000,pollutantID-10000,…)`).
                    pol_process_id: 100 + e.process_id,
                    pollutant_id: 1,
                    process_id: e.process_id,
                    source_type_id: e.source_type_id,
                    model_year_id: my,
                    age_id: age,
                    ratio,
                    ratio_gpa,
                    // Sulfur is baked into the expression; ratioNoSulfur = 1.
                    ratio_no_sulfur: 1.0,
                });
            }
        }
    }
    rows
}

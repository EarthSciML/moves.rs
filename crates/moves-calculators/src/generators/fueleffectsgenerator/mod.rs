//! Fuel Effects Generator —.
//!
//! Pure-Rust port of the general-fuel-ratio path of
//! `gov/epa/otaq/moves/master/implementation/ghg/FuelEffectsGenerator.java`
//! (4,435 lines). The generator apportions emission rates across the
//! fuel-formulation distribution, applying fuel adjustments via
//! `generalFuelRatioExpression` table entries.
//!
//! # Module map
//!
//! | Module | Ports |
//! |--------|-------|
//! | [`text`] | the `static` string helpers `rewriteCmpExpressionToIncludeStdDev`, `getCSV`, `getPolProcessIDsNotAlreadyDone` |
//! | [`model`] | the `FuelFormulation`, `GeneralFuelRatioExpression` and `IntegerPair` inner classes |
//! | [`expression`] | a recursive-descent evaluator for the SQL arithmetic in a `fuelEffectRatioExpression` |
//! | [`generalfuelratio`] | `doGeneralFuelRatio` — the headline compute path |
//!
//! # Scope
//!
//! `FuelEffectsGenerator` is one of the largest single classes in MOVES.
//! Its work splits into a general-fuel-ratio path (`doGeneralFuelRatio`,
//! driven by `generalFuelRatioExpression`) and a family of
//! predictive/complex-model paths (`doAirToxicsCalculations`,
//! `doCOCalculations`, `doHCCalculations`, `doNOxCalculations`,
//! `doMTBECalculations`, …) that depend on the MOVES expression engine and
//! a dozen further default-DB tables.
//!
//! This port covers the **general-fuel-ratio path** — the one the
//! names — together with the three pure `static` helpers,
//! and ports `FuelEffectsGeneratorTest`'s database-independent assertions
//! as the regression baseline. The predictive/complex-model paths are left
//! for a follow-up port; they need the complex-model expression engine,
//! which is a separate subsystem.
//!
//! # Data-plane wiring
//!
//! [`do_general_fuel_ratio`] is the numerical entry point. [`execute`] reads
//! `generalFuelRatioExpression`, `FuelFormulation`, `FuelSubtype`, and
//! `FuelSupply` from the context, assembles a [`GeneralFuelRatioInputs`],
//! calls the kernel, and writes the resulting rows to the scratch
//! `generalFuelRatio` table via `crate::wiring::write_scratch_table`.
//!
//! [`execute`]: Generator::execute

pub mod expression;
pub mod generalfuelratio;
pub mod model;
pub mod text;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

use moves_calculator_info::{Granularity, Priority};
use moves_data::ProcessId;
use moves_framework::{
    CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped, Error,
    Generator, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Series};

pub use expression::{Expression, ExpressionError, VariableSource};
pub use generalfuelratio::{
    derive_pseudo_thc_expressions, do_general_fuel_ratio, GeneralFuelRatioInputs,
};
pub use model::{
    contains, FuelFormulation, GeneralFuelRatioExpression, GeneralFuelRatioRow, IntegerPair,
};
pub use text::{
    get_csv, get_pol_process_ids_not_already_done, rewrite_cmp_expression_to_include_std_dev,
};

/// Stable module name in the calculator-chain DAG.
const GENERATOR_NAME: &str = "FuelEffectsGenerator";

/// Processes the generator subscribes to, from
/// `characterization/calculator-chains/calculator-dag.json`. The Java
/// `subscribeToMe` subscribes to every process in the RunSpec; the DAG
/// records the fourteen the generator participates in.
const SUBSCRIBED_PROCESSES: [u16; 14] = [1, 2, 9, 10, 11, 12, 13, 15, 16, 17, 18, 19, 90, 91];

/// Default-DB tables the general-fuel-ratio path reads. Names match the
/// casing of `characterization/default-db-schema/tables.json`.
///
/// `FuelSubtype` is needed to map `fuelSubtypeID` → `fuelTypeID`, which
/// `FuelFormulation` does not carry directly. `FuelSupply` supplies the
/// set of in-use `fuelFormulationID`s per fuel type.
static INPUT_TABLES: &[&str] = &[
    "generalFuelRatioExpression",
    "FuelFormulation",
    "FuelSubtype",
    "FuelSupply",
];

/// Scratch tables the general-fuel-ratio path writes for downstream
/// calculators. The full Java generator additionally writes `ATRatio`,
/// `criteriaRatio`, `MTBERatio` and related tables through the
/// predictive/complex-model paths not covered by this port.
static OUTPUT_TABLES: &[&str] = &["generalFuelRatio"];

/// The Fuel Effects Generator.
///
/// A zero-sized value type: the generator owns no per-run state, as the
/// [`Generator`] trait contract requires. All run-varying input flows
/// through [`do_general_fuel_ratio`]'s [`GeneralFuelRatioInputs`] argument.
#[derive(Debug, Clone, Copy, Default)]
pub struct FuelEffectsGenerator;

impl Generator for FuelEffectsGenerator {
    fn name(&self) -> &'static str {
        GENERATOR_NAME
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
 // Built once: `Priority::parse` is not a `const fn`, so the slice
 // cannot be a plain `static`. The generator subscribes at PROCESS
 // granularity, priority GENERATOR-1 — it runs just after the
 // TankFuelGenerator, which modifies fuel-formulation parameters.
        static SUBS: OnceLock<Vec<CalculatorSubscription>> = OnceLock::new();
        SUBS.get_or_init(|| {
            let priority = Priority::parse("GENERATOR-1").expect("GENERATOR-1 is a valid priority");
            SUBSCRIBED_PROCESSES
                .into_iter()
                .map(|process| {
                    CalculatorSubscription::new(ProcessId(process), Granularity::Process, priority)
                })
                .collect()
        })
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn output_tables(&self) -> &[&'static str] {
        OUTPUT_TABLES
    }

    fn execute(&self, ctx: &mut CalculatorContext) -> Result<CalculatorOutput, Error> {
 // Read `generalFuelRatioExpression`.
        let expr_rows: Vec<GeneralFuelRatioExpressionRow> =
            ctx.tables().iter_typed("generalFuelRatioExpression")?;
        let expressions: Vec<GeneralFuelRatioExpression> = expr_rows
            .into_iter()
            .map(|r| {
                GeneralFuelRatioExpression::new(
                    r.fuel_type_id,
                    r.pol_process_id,
                    r.min_model_year_id,
                    r.max_model_year_id,
                    r.min_age_id,
                    r.max_age_id,
                    r.source_type_id,
                    r.fuel_effect_ratio_expression,
                    r.fuel_effect_ratio_gpa_expression,
                )
            })
            .collect();

 // Read `FuelSubtype` to map fuelSubtypeID → fuelTypeID.
        let subtype_rows: Vec<FuelSubtypeRow> = ctx.tables().iter_typed("FuelSubtype")?;
        let subtype_to_type: std::collections::HashMap<i32, i32> = subtype_rows
            .into_iter()
            .map(|r| (r.fuel_subtype_id, r.fuel_type_id))
            .collect();

 // Read `FuelFormulation` and group by fuelTypeID via the subtype map.
        let ff_rows: Vec<FuelFormulationRow> = ctx.tables().iter_typed("FuelFormulation")?;
        let mut formulations_by_fuel_type: BTreeMap<i32, Vec<FuelFormulation>> = BTreeMap::new();
        for r in ff_rows {
            let fuel_type_id = *subtype_to_type.get(&r.fuel_subtype_id).unwrap_or(&0);
            formulations_by_fuel_type
                .entry(fuel_type_id)
                .or_default()
                .push(r.into_model());
        }

 // Read `FuelSupply` and build supplied_by_fuel_type (fuelFormulationID
 // sets per fuelTypeID). We join through the subtype map by way of
 // FuelFormulation: a formulation's fuelTypeID determines its bucket.
        let fs_rows: Vec<FuelSupplyRow> = ctx.tables().iter_typed("FuelSupply")?;
 // Build a fast lookup: fuelFormulationID → fuelTypeID.
        let formulation_to_type: std::collections::HashMap<i32, i32> = formulations_by_fuel_type
            .iter()
            .flat_map(|(&ft, ffs)| ffs.iter().map(move |ff| (ff.fuel_formulation_id, ft)))
            .collect();
        let mut supplied_by_fuel_type: BTreeMap<i32, BTreeSet<i32>> = BTreeMap::new();
        for r in fs_rows {
            if let Some(&fuel_type_id) = formulation_to_type.get(&r.fuel_formulation_id) {
                supplied_by_fuel_type
                    .entry(fuel_type_id)
                    .or_default()
                    .insert(r.fuel_formulation_id);
            }
        }

        let inputs = GeneralFuelRatioInputs {
            expressions,
            formulations_by_fuel_type,
            supplied_by_fuel_type,
            already_ratioed: BTreeSet::new(),
        };
        let rows = do_general_fuel_ratio(&inputs)
            .map_err(|e| Error::Polars(format!("FuelEffectsGenerator expression error: {e}")))?;
        let output_rows: Vec<GeneralFuelRatioOutputRow> = rows
            .into_iter()
            .map(GeneralFuelRatioOutputRow::from)
            .collect();
        crate::wiring::write_scratch_table(ctx, OUTPUT_TABLES[0], output_rows)
    }
}

// =============================================================================
// Row-extraction error helper
// =============================================================================

fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::RowExtraction {
        table: table.into(),
        row,
        column: column.into(),
        message: msg,
    }
}

// =============================================================================
// Input table row types and TableRow impls
// =============================================================================

/// One `generalFuelRatioExpression` row — all columns the kernel reads.
struct GeneralFuelRatioExpressionRow {
    fuel_type_id: i32,
    pol_process_id: i32,
    min_model_year_id: i32,
    max_model_year_id: i32,
    min_age_id: i32,
    max_age_id: i32,
    source_type_id: i32,
    fuel_effect_ratio_expression: String,
    fuel_effect_ratio_gpa_expression: String,
}

impl TableRow for GeneralFuelRatioExpressionRow {
    fn table_name() -> &'static str {
        "generalFuelRatioExpression"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([
            ("fuelTypeID".into(), DataType::Int32),
            ("polProcessID".into(), DataType::Int32),
            ("minModelYearID".into(), DataType::Int32),
            ("maxModelYearID".into(), DataType::Int32),
            ("minAgeID".into(), DataType::Int32),
            ("maxAgeID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("fuelEffectRatioExpression".into(), DataType::String),
            ("fuelEffectRatioGPAExpression".into(), DataType::String),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "polProcessID".into(),
                    rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "minModelYearID".into(),
                    rows.iter()
                        .map(|r| r.min_model_year_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "maxModelYearID".into(),
                    rows.iter()
                        .map(|r| r.max_model_year_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "minAgeID".into(),
                    rows.iter().map(|r| r.min_age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "maxAgeID".into(),
                    rows.iter().map(|r| r.max_age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelEffectRatioExpression".into(),
                    rows.iter()
                        .map(|r| r.fuel_effect_ratio_expression.as_str())
                        .collect::<Vec<&str>>(),
                )
                .into(),
                Series::new(
                    "fuelEffectRatioGPAExpression".into(),
                    rows.iter()
                        .map(|r| r.fuel_effect_ratio_gpa_expression.as_str())
                        .collect::<Vec<&str>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "generalFuelRatioExpression";
        let fuel_type_id_col = df
            .column("fuelTypeID")
            .map_err(|e| row_err(t, 0, "fuelTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "fuelTypeID", e.to_string()))?;
        let pol_process_id_col = df
            .column("polProcessID")
            .map_err(|e| row_err(t, 0, "polProcessID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "polProcessID", e.to_string()))?;
        let min_model_year_id_col = df
            .column("minModelYearID")
            .map_err(|e| row_err(t, 0, "minModelYearID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "minModelYearID", e.to_string()))?;
        let max_model_year_id_col = df
            .column("maxModelYearID")
            .map_err(|e| row_err(t, 0, "maxModelYearID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "maxModelYearID", e.to_string()))?;
        let min_age_id_col = df
            .column("minAgeID")
            .map_err(|e| row_err(t, 0, "minAgeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "minAgeID", e.to_string()))?;
        let max_age_id_col = df
            .column("maxAgeID")
            .map_err(|e| row_err(t, 0, "maxAgeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "maxAgeID", e.to_string()))?;
        let source_type_id_col = df
            .column("sourceTypeID")
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?;
        let expr_col = df
            .column("fuelEffectRatioExpression")
            .map_err(|e| row_err(t, 0, "fuelEffectRatioExpression", e.to_string()))?
            .str()
            .map_err(|e| row_err(t, 0, "fuelEffectRatioExpression", e.to_string()))?;
        let gpa_expr_col = df
            .column("fuelEffectRatioGPAExpression")
            .map_err(|e| row_err(t, 0, "fuelEffectRatioGPAExpression", e.to_string()))?
            .str()
            .map_err(|e| row_err(t, 0, "fuelEffectRatioGPAExpression", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(GeneralFuelRatioExpressionRow {
                    fuel_type_id: fuel_type_id_col.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    pol_process_id: pol_process_id_col
                        .get(i)
                        .ok_or_else(|| null("polProcessID"))?,
                    min_model_year_id: min_model_year_id_col
                        .get(i)
                        .ok_or_else(|| null("minModelYearID"))?,
                    max_model_year_id: max_model_year_id_col
                        .get(i)
                        .ok_or_else(|| null("maxModelYearID"))?,
                    min_age_id: min_age_id_col.get(i).ok_or_else(|| null("minAgeID"))?,
                    max_age_id: max_age_id_col.get(i).ok_or_else(|| null("maxAgeID"))?,
                    source_type_id: source_type_id_col
                        .get(i)
                        .ok_or_else(|| null("sourceTypeID"))?,
                    fuel_effect_ratio_expression: expr_col
                        .get(i)
                        .ok_or_else(|| null("fuelEffectRatioExpression"))?
                        .to_owned(),
                    fuel_effect_ratio_gpa_expression: gpa_expr_col
                        .get(i)
                        .ok_or_else(|| null("fuelEffectRatioGPAExpression"))?
                        .to_owned(),
                })
            })
            .collect()
    }
}

// =============================================================================
// FuelFormulation TableRow
// =============================================================================

/// All columns of `FuelFormulation` read by the general-fuel-ratio path.
///
/// The model type [`FuelFormulation`] lives in [`model`] and carries `f32`
/// fields (matching the MySQL `float` columns). This local wrapper provides
/// a `TableRow` impl that reads the Polars `Float64` columns (the store
/// always promotes to `f64`) and converts them back to `f32`.
struct FuelFormulationRow {
    fuel_formulation_id: i32,
    fuel_subtype_id: i32,
    rvp: f32,
    sulfur_level: f32,
    etoh_volume: f32,
    mtbe_volume: f32,
    etbe_volume: f32,
    tame_volume: f32,
    aromatic_content: f32,
    olefin_content: f32,
    benzene_content: f32,
    e200: f32,
    e300: f32,
    vol_to_wt_percent_oxy: f32,
    bio_diesel_ester_volume: f32,
    cetane_index: f32,
    pah_content: f32,
    t50: f32,
    t90: f32,
}

impl FuelFormulationRow {
    fn into_model(self) -> FuelFormulation {
        FuelFormulation {
            fuel_formulation_id: self.fuel_formulation_id,
            fuel_subtype_id: self.fuel_subtype_id,
            rvp: self.rvp,
            sulfur_level: self.sulfur_level,
            etoh_volume: self.etoh_volume,
            mtbe_volume: self.mtbe_volume,
            etbe_volume: self.etbe_volume,
            tame_volume: self.tame_volume,
            aromatic_content: self.aromatic_content,
            olefin_content: self.olefin_content,
            benzene_content: self.benzene_content,
            e200: self.e200,
            e300: self.e300,
            vol_to_wt_percent_oxy: self.vol_to_wt_percent_oxy,
            bio_diesel_ester_volume: self.bio_diesel_ester_volume,
            cetane_index: self.cetane_index,
            pah_content: self.pah_content,
            t50: self.t50,
            t90: self.t90,
            alt_rvp: 0.0, // added by TankFuelGenerator.setup(); zero at read time
        }
    }
}

impl TableRow for FuelFormulationRow {
    fn table_name() -> &'static str {
        "FuelFormulation"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([
            ("fuelFormulationID".into(), DataType::Int32),
            ("fuelSubtypeID".into(), DataType::Int32),
            ("RVP".into(), DataType::Float64),
            ("sulfurLevel".into(), DataType::Float64),
            ("ETOHVolume".into(), DataType::Float64),
            ("MTBEVolume".into(), DataType::Float64),
            ("ETBEVolume".into(), DataType::Float64),
            ("TAMEVolume".into(), DataType::Float64),
            ("aromaticContent".into(), DataType::Float64),
            ("olefinContent".into(), DataType::Float64),
            ("benzeneContent".into(), DataType::Float64),
            ("e200".into(), DataType::Float64),
            ("e300".into(), DataType::Float64),
            ("volToWtPercentOxy".into(), DataType::Float64),
            ("BioDieselEsterVolume".into(), DataType::Float64),
            ("CetaneIndex".into(), DataType::Float64),
            ("PAHContent".into(), DataType::Float64),
            ("T50".into(), DataType::Float64),
            ("T90".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "fuelFormulationID".into(),
                    rows.iter()
                        .map(|r| r.fuel_formulation_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelSubtypeID".into(),
                    rows.iter().map(|r| r.fuel_subtype_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "RVP".into(),
                    rows.iter().map(|r| r.rvp as f64).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "sulfurLevel".into(),
                    rows.iter()
                        .map(|r| r.sulfur_level as f64)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "ETOHVolume".into(),
                    rows.iter()
                        .map(|r| r.etoh_volume as f64)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "MTBEVolume".into(),
                    rows.iter()
                        .map(|r| r.mtbe_volume as f64)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "ETBEVolume".into(),
                    rows.iter()
                        .map(|r| r.etbe_volume as f64)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "TAMEVolume".into(),
                    rows.iter()
                        .map(|r| r.tame_volume as f64)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "aromaticContent".into(),
                    rows.iter()
                        .map(|r| r.aromatic_content as f64)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "olefinContent".into(),
                    rows.iter()
                        .map(|r| r.olefin_content as f64)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "benzeneContent".into(),
                    rows.iter()
                        .map(|r| r.benzene_content as f64)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "e200".into(),
                    rows.iter().map(|r| r.e200 as f64).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "e300".into(),
                    rows.iter().map(|r| r.e300 as f64).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "volToWtPercentOxy".into(),
                    rows.iter()
                        .map(|r| r.vol_to_wt_percent_oxy as f64)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "BioDieselEsterVolume".into(),
                    rows.iter()
                        .map(|r| r.bio_diesel_ester_volume as f64)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "CetaneIndex".into(),
                    rows.iter()
                        .map(|r| r.cetane_index as f64)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "PAHContent".into(),
                    rows.iter()
                        .map(|r| r.pah_content as f64)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "T50".into(),
                    rows.iter().map(|r| r.t50 as f64).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "T90".into(),
                    rows.iter().map(|r| r.t90 as f64).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "FuelFormulation";
        macro_rules! i32_col {
            ($name:expr) => {{
                df.column($name)
                    .map_err(|e| row_err(t, 0, $name, e.to_string()))?
                    .i32()
                    .map_err(|e| row_err(t, 0, $name, e.to_string()))?
            }};
        }
        macro_rules! f64_col {
            ($name:expr) => {{
                df.column($name)
                    .map_err(|e| row_err(t, 0, $name, e.to_string()))?
                    .f64()
                    .map_err(|e| row_err(t, 0, $name, e.to_string()))?
            }};
        }
        let fuel_formulation_id_col = i32_col!("fuelFormulationID");
        let fuel_subtype_id_col = i32_col!("fuelSubtypeID");
        let rvp_col = f64_col!("RVP");
        let sulfur_level_col = f64_col!("sulfurLevel");
        let etoh_volume_col = f64_col!("ETOHVolume");
        let mtbe_volume_col = f64_col!("MTBEVolume");
        let etbe_volume_col = f64_col!("ETBEVolume");
        let tame_volume_col = f64_col!("TAMEVolume");
        let aromatic_content_col = f64_col!("aromaticContent");
        let olefin_content_col = f64_col!("olefinContent");
        let benzene_content_col = f64_col!("benzeneContent");
        let e200_col = f64_col!("e200");
        let e300_col = f64_col!("e300");
        let vol_to_wt_percent_oxy_col = f64_col!("volToWtPercentOxy");
        let bio_diesel_ester_volume_col = f64_col!("BioDieselEsterVolume");
        let cetane_index_col = f64_col!("CetaneIndex");
        let pah_content_col = f64_col!("PAHContent");
        let t50_col = f64_col!("T50");
        let t90_col = f64_col!("T90");
        // Fuel-property columns are NULL in the default DB for formulations that
        // do not carry the property (e.g. diesel/electric have no RVP, ethanol
        // volume, or aromatic content; the formulation-0 placeholder is all
        // NULL). MOVES treats an absent fuel property as zero — those rows are
        // excluded from the property-based calculations by fuel-type joins — so
        // a missing value lowers to 0.0 rather than failing extraction. Only the
        // identity columns remain strictly required.
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(FuelFormulationRow {
                    fuel_formulation_id: fuel_formulation_id_col
                        .get(i)
                        .ok_or_else(|| null("fuelFormulationID"))?,
                    fuel_subtype_id: fuel_subtype_id_col
                        .get(i)
                        .ok_or_else(|| null("fuelSubtypeID"))?,
                    rvp: rvp_col.get(i).unwrap_or(0.0) as f32,
                    sulfur_level: sulfur_level_col.get(i).unwrap_or(0.0) as f32,
                    etoh_volume: etoh_volume_col.get(i).unwrap_or(0.0) as f32,
                    mtbe_volume: mtbe_volume_col.get(i).unwrap_or(0.0) as f32,
                    etbe_volume: etbe_volume_col.get(i).unwrap_or(0.0) as f32,
                    tame_volume: tame_volume_col.get(i).unwrap_or(0.0) as f32,
                    aromatic_content: aromatic_content_col.get(i).unwrap_or(0.0) as f32,
                    olefin_content: olefin_content_col.get(i).unwrap_or(0.0) as f32,
                    benzene_content: benzene_content_col.get(i).unwrap_or(0.0) as f32,
                    e200: e200_col.get(i).unwrap_or(0.0) as f32,
                    e300: e300_col.get(i).unwrap_or(0.0) as f32,
                    vol_to_wt_percent_oxy: vol_to_wt_percent_oxy_col.get(i).unwrap_or(0.0) as f32,
                    bio_diesel_ester_volume: bio_diesel_ester_volume_col.get(i).unwrap_or(0.0)
                        as f32,
                    cetane_index: cetane_index_col.get(i).unwrap_or(0.0) as f32,
                    pah_content: pah_content_col.get(i).unwrap_or(0.0) as f32,
                    t50: t50_col.get(i).unwrap_or(0.0) as f32,
                    t90: t90_col.get(i).unwrap_or(0.0) as f32,
                })
            })
            .collect()
    }
}

// =============================================================================
// FuelSubtype TableRow (local wrapper — orphan rule)
// =============================================================================

struct FuelSubtypeRow {
    fuel_subtype_id: i32,
    fuel_type_id: i32,
}

impl TableRow for FuelSubtypeRow {
    fn table_name() -> &'static str {
        "FuelSubtype"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([
            ("fuelSubtypeID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "fuelSubtypeID".into(),
                    rows.iter().map(|r| r.fuel_subtype_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "FuelSubtype";
        let fuel_subtype_id_col = df
            .column("fuelSubtypeID")
            .map_err(|e| row_err(t, 0, "fuelSubtypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "fuelSubtypeID", e.to_string()))?;
        let fuel_type_id_col = df
            .column("fuelTypeID")
            .map_err(|e| row_err(t, 0, "fuelTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "fuelTypeID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(FuelSubtypeRow {
                    fuel_subtype_id: fuel_subtype_id_col
                        .get(i)
                        .ok_or_else(|| null("fuelSubtypeID"))?,
                    fuel_type_id: fuel_type_id_col.get(i).ok_or_else(|| null("fuelTypeID"))?,
                })
            })
            .collect()
    }
}

// =============================================================================
// FuelSupply TableRow (local wrapper — only columns needed here)
// =============================================================================

struct FuelSupplyRow {
    fuel_formulation_id: i32,
}

impl TableRow for FuelSupplyRow {
    fn table_name() -> &'static str {
        "FuelSupply"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([
            ("fuelRegionID".into(), DataType::Int32),
            ("fuelYearID".into(), DataType::Int32),
            ("monthGroupID".into(), DataType::Int32),
            ("fuelFormulationID".into(), DataType::Int32),
            ("marketShare".into(), DataType::Float64),
            ("marketShareCV".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
 // Emit all required schema columns; we only carry fuelFormulationID.
        DataFrame::new(
            n,
            vec![
                Series::new("fuelRegionID".into(), vec![0i32; n]).into(),
                Series::new("fuelYearID".into(), vec![0i32; n]).into(),
                Series::new("monthGroupID".into(), vec![0i32; n]).into(),
                Series::new(
                    "fuelFormulationID".into(),
                    rows.iter()
                        .map(|r| r.fuel_formulation_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new("marketShare".into(), vec![1.0f64; n]).into(),
                Series::new("marketShareCV".into(), vec![0.0f64; n]).into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "FuelSupply";
        let fuel_formulation_id_col = df
            .column("fuelFormulationID")
            .map_err(|e| row_err(t, 0, "fuelFormulationID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "fuelFormulationID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(FuelSupplyRow {
                    fuel_formulation_id: fuel_formulation_id_col
                        .get(i)
                        .ok_or_else(|| null("fuelFormulationID"))?,
                })
            })
            .collect()
    }
}

// =============================================================================
// generalFuelRatio output TableRow
// =============================================================================

/// One `generalFuelRatio` scratch-table row — mirrors [`GeneralFuelRatioRow`]
/// with a `TableRow` impl for serialisation into the scratch store.
struct GeneralFuelRatioOutputRow {
    fuel_type_id: i32,
    fuel_formulation_id: i32,
    pol_process_id: i32,
    pollutant_id: i32,
    process_id: i32,
    min_model_year_id: i32,
    max_model_year_id: i32,
    min_age_id: i32,
    max_age_id: i32,
    source_type_id: i32,
    fuel_effect_ratio: f64,
    fuel_effect_ratio_gpa: f64,
}

impl From<GeneralFuelRatioRow> for GeneralFuelRatioOutputRow {
    fn from(r: GeneralFuelRatioRow) -> Self {
        GeneralFuelRatioOutputRow {
            fuel_type_id: r.fuel_type_id,
            fuel_formulation_id: r.fuel_formulation_id,
            pol_process_id: r.pol_process_id,
            pollutant_id: r.pollutant_id,
            process_id: r.process_id,
            min_model_year_id: r.min_model_year_id,
            max_model_year_id: r.max_model_year_id,
            min_age_id: r.min_age_id,
            max_age_id: r.max_age_id,
            source_type_id: r.source_type_id,
            fuel_effect_ratio: r.fuel_effect_ratio,
            fuel_effect_ratio_gpa: r.fuel_effect_ratio_gpa,
        }
    }
}

impl TableRow for GeneralFuelRatioOutputRow {
    fn table_name() -> &'static str {
        "generalFuelRatio"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([
            ("fuelTypeID".into(), DataType::Int32),
            ("fuelFormulationID".into(), DataType::Int32),
            ("polProcessID".into(), DataType::Int32),
            ("pollutantID".into(), DataType::Int32),
            ("processID".into(), DataType::Int32),
            ("minModelYearID".into(), DataType::Int32),
            ("maxModelYearID".into(), DataType::Int32),
            ("minAgeID".into(), DataType::Int32),
            ("maxAgeID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("fuelEffectRatio".into(), DataType::Float64),
            ("fuelEffectRatioGPA".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelFormulationID".into(),
                    rows.iter()
                        .map(|r| r.fuel_formulation_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "polProcessID".into(),
                    rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "pollutantID".into(),
                    rows.iter().map(|r| r.pollutant_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "processID".into(),
                    rows.iter().map(|r| r.process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "minModelYearID".into(),
                    rows.iter()
                        .map(|r| r.min_model_year_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "maxModelYearID".into(),
                    rows.iter()
                        .map(|r| r.max_model_year_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "minAgeID".into(),
                    rows.iter().map(|r| r.min_age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "maxAgeID".into(),
                    rows.iter().map(|r| r.max_age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelEffectRatio".into(),
                    rows.iter()
                        .map(|r| r.fuel_effect_ratio)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "fuelEffectRatioGPA".into(),
                    rows.iter()
                        .map(|r| r.fuel_effect_ratio_gpa)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "generalFuelRatio";
        macro_rules! i32_col {
            ($name:expr) => {{
                df.column($name)
                    .map_err(|e| row_err(t, 0, $name, e.to_string()))?
                    .i32()
                    .map_err(|e| row_err(t, 0, $name, e.to_string()))?
            }};
        }
        macro_rules! f64_col {
            ($name:expr) => {{
                df.column($name)
                    .map_err(|e| row_err(t, 0, $name, e.to_string()))?
                    .f64()
                    .map_err(|e| row_err(t, 0, $name, e.to_string()))?
            }};
        }
        let fuel_type_id_col = i32_col!("fuelTypeID");
        let fuel_formulation_id_col = i32_col!("fuelFormulationID");
        let pol_process_id_col = i32_col!("polProcessID");
        let pollutant_id_col = i32_col!("pollutantID");
        let process_id_col = i32_col!("processID");
        let min_model_year_id_col = i32_col!("minModelYearID");
        let max_model_year_id_col = i32_col!("maxModelYearID");
        let min_age_id_col = i32_col!("minAgeID");
        let max_age_id_col = i32_col!("maxAgeID");
        let source_type_id_col = i32_col!("sourceTypeID");
        let fuel_effect_ratio_col = f64_col!("fuelEffectRatio");
        let fuel_effect_ratio_gpa_col = f64_col!("fuelEffectRatioGPA");
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(GeneralFuelRatioOutputRow {
                    fuel_type_id: fuel_type_id_col.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    fuel_formulation_id: fuel_formulation_id_col
                        .get(i)
                        .ok_or_else(|| null("fuelFormulationID"))?,
                    pol_process_id: pol_process_id_col
                        .get(i)
                        .ok_or_else(|| null("polProcessID"))?,
                    pollutant_id: pollutant_id_col.get(i).ok_or_else(|| null("pollutantID"))?,
                    process_id: process_id_col.get(i).ok_or_else(|| null("processID"))?,
                    min_model_year_id: min_model_year_id_col
                        .get(i)
                        .ok_or_else(|| null("minModelYearID"))?,
                    max_model_year_id: max_model_year_id_col
                        .get(i)
                        .ok_or_else(|| null("maxModelYearID"))?,
                    min_age_id: min_age_id_col.get(i).ok_or_else(|| null("minAgeID"))?,
                    max_age_id: max_age_id_col.get(i).ok_or_else(|| null("maxAgeID"))?,
                    source_type_id: source_type_id_col
                        .get(i)
                        .ok_or_else(|| null("sourceTypeID"))?,
                    fuel_effect_ratio: fuel_effect_ratio_col
                        .get(i)
                        .ok_or_else(|| null("fuelEffectRatio"))?,
                    fuel_effect_ratio_gpa: fuel_effect_ratio_gpa_col
                        .get(i)
                        .ok_or_else(|| null("fuelEffectRatioGPA"))?,
                })
            })
            .collect()
    }
}

pub fn factory() -> Box<dyn Generator> {
    Box::new(FuelEffectsGenerator)
}

#[cfg(test)]
mod tests {
    use super::*;
    use moves_framework::{
        DataFrameStore, DataFrameStoreTyped, ExecutionLocation, ExecutionTime, InMemoryStore,
        IterationPosition,
    };

    #[test]
    fn generator_metadata_matches_calculator_dag() {
        let generator = FuelEffectsGenerator;
        assert_eq!(generator.name(), "FuelEffectsGenerator");

        let subs = generator.subscriptions();
 // The DAG records fourteen process subscriptions.
        assert_eq!(subs.len(), 14);
        let processes: Vec<u16> = subs.iter().map(|s| s.process_id.0).collect();
        assert_eq!(
            processes,
            vec![1, 2, 9, 10, 11, 12, 13, 15, 16, 17, 18, 19, 90, 91]
        );
 // All at PROCESS granularity, priority GENERATOR-1.
        assert!(subs.iter().all(|s| s.granularity == Granularity::Process));
        assert!(subs.iter().all(|s| s.priority.display() == "GENERATOR-1"));

        assert!(generator.upstream().is_empty());
        assert_eq!(generator.output_tables(), &["generalFuelRatio"]);
        assert!(generator
            .input_tables()
            .contains(&"generalFuelRatioExpression"));
    }

    #[test]
    fn generator_is_object_safe() {
 // The registry stores generators as `Box<dyn Generator>`.
        let generators: Vec<Box<dyn Generator>> = vec![Box::new(FuelEffectsGenerator)];
        assert_eq!(generators[0].name(), "FuelEffectsGenerator");
    }

 /// Build a minimal `InMemoryStore` with one expression, one formulation,
 /// one subtype mapping, and one fuel-supply row. The expression evaluates
 /// `MTBEVolume + 7` and `MTBEVolume * 2` against the formulation
 /// (MTBEVolume = 10.0), so the expected ratios are 17.0 and 20.0.
    fn make_store() -> InMemoryStore {
        let mut store = InMemoryStore::default();

 // generalFuelRatioExpression
        store.insert(
            "generalFuelRatioExpression",
            GeneralFuelRatioExpressionRow::into_dataframe(vec![GeneralFuelRatioExpressionRow {
                fuel_type_id: 1,
                pol_process_id: -101,
                min_model_year_id: 1960,
                max_model_year_id: 2060,
                min_age_id: 0,
                max_age_id: 30,
                source_type_id: 0,
                fuel_effect_ratio_expression: "MTBEVolume+7".into(),
                fuel_effect_ratio_gpa_expression: "MTBEVolume*2".into(),
            }])
            .unwrap(),
        );

 // FuelFormulation (fuelSubtypeID 10 → fuelTypeID 1 via FuelSubtype)
        store.insert(
            "FuelFormulation",
            FuelFormulationRow::into_dataframe(vec![FuelFormulationRow {
                fuel_formulation_id: 100,
                fuel_subtype_id: 10,
                rvp: 0.0,
                sulfur_level: 0.0,
                etoh_volume: 0.0,
                mtbe_volume: 10.0,
                etbe_volume: 0.0,
                tame_volume: 0.0,
                aromatic_content: 0.0,
                olefin_content: 0.0,
                benzene_content: 0.0,
                e200: 0.0,
                e300: 0.0,
                vol_to_wt_percent_oxy: 0.0,
                bio_diesel_ester_volume: 0.0,
                cetane_index: 0.0,
                pah_content: 0.0,
                t50: 0.0,
                t90: 0.0,
            }])
            .unwrap(),
        );

 // FuelSubtype: subtype 10 → fuel type 1
        store.insert(
            "FuelSubtype",
            FuelSubtypeRow::into_dataframe(vec![FuelSubtypeRow {
                fuel_subtype_id: 10,
                fuel_type_id: 1,
            }])
            .unwrap(),
        );

 // FuelSupply: formulation 100 is in the supply
        store.insert(
            "FuelSupply",
            FuelSupplyRow::into_dataframe(vec![FuelSupplyRow {
                fuel_formulation_id: 100,
            }])
            .unwrap(),
        );

        store
    }

    #[test]
    fn execute_writes_general_fuel_ratio_to_scratch() {
        let store = make_store();
        let position = IterationPosition {
            iteration: 0,
            process_id: Some(ProcessId(1)),
            location: ExecutionLocation::link(1, 1, 1, 1),
            time: ExecutionTime {
                year: Some(2020),
                month: None,
                day_id: None,
                hour: None,
            },
        };

        let generator = FuelEffectsGenerator;
        let mut ctx = CalculatorContext::with_position_and_tables(position, store);
        generator.execute(&mut ctx).unwrap();

        let out: Vec<GeneralFuelRatioOutputRow> =
            ctx.scratch().store.iter_typed("generalFuelRatio").unwrap();
        assert_eq!(out.len(), 1, "expected one generalFuelRatio row");
        let row = &out[0];
        assert_eq!(row.fuel_type_id, 1);
        assert_eq!(row.fuel_formulation_id, 100);
        assert_eq!(row.pol_process_id, -101);
        assert!((row.fuel_effect_ratio - 17.0).abs() < 1e-12);
        assert!((row.fuel_effect_ratio_gpa - 20.0).abs() < 1e-12);
    }

    #[test]
    fn execute_empty_supply_produces_no_rows() {
 // No FuelSupply rows → no supplied formulations → kernel returns empty.
        let mut store = InMemoryStore::default();
        store.insert(
            "generalFuelRatioExpression",
            GeneralFuelRatioExpressionRow::into_dataframe(vec![GeneralFuelRatioExpressionRow {
                fuel_type_id: 1,
                pol_process_id: 201,
                min_model_year_id: 1960,
                max_model_year_id: 2060,
                min_age_id: 0,
                max_age_id: 30,
                source_type_id: 0,
                fuel_effect_ratio_expression: "1".into(),
                fuel_effect_ratio_gpa_expression: "1".into(),
            }])
            .unwrap(),
        );
        store.insert(
            "FuelFormulation",
            FuelFormulationRow::into_dataframe(vec![FuelFormulationRow {
                fuel_formulation_id: 100,
                fuel_subtype_id: 10,
                rvp: 0.0,
                sulfur_level: 0.0,
                etoh_volume: 0.0,
                mtbe_volume: 0.0,
                etbe_volume: 0.0,
                tame_volume: 0.0,
                aromatic_content: 0.0,
                olefin_content: 0.0,
                benzene_content: 0.0,
                e200: 0.0,
                e300: 0.0,
                vol_to_wt_percent_oxy: 0.0,
                bio_diesel_ester_volume: 0.0,
                cetane_index: 0.0,
                pah_content: 0.0,
                t50: 0.0,
                t90: 0.0,
            }])
            .unwrap(),
        );
        store.insert(
            "FuelSubtype",
            FuelSubtypeRow::into_dataframe(vec![FuelSubtypeRow {
                fuel_subtype_id: 10,
                fuel_type_id: 1,
            }])
            .unwrap(),
        );
 // No FuelSupply rows.
        store.insert("FuelSupply", FuelSupplyRow::into_dataframe(vec![]).unwrap());

        let position = IterationPosition {
            iteration: 0,
            process_id: Some(ProcessId(1)),
            location: ExecutionLocation::link(1, 1, 1, 1),
            time: ExecutionTime {
                year: Some(2020),
                month: None,
                day_id: None,
                hour: None,
            },
        };
        let generator = FuelEffectsGenerator;
        let mut ctx = CalculatorContext::with_position_and_tables(position, store);
        generator.execute(&mut ctx).unwrap();

        let out: Vec<GeneralFuelRatioOutputRow> =
            ctx.scratch().store.iter_typed("generalFuelRatio").unwrap();
        assert!(out.is_empty(), "no supply → no output rows");
    }
}

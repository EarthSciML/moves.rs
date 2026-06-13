//! `NewTvvYearGenerator` — builds (or promotes) the year-specific TVV
//! coefficient tables to canonical scratch names once per calendar year.
//!
//! Ports the `NewTVVYear` section of
//! `database/MultidayTankVaporVentingCalculator.sql` (lines 336-435). That
//! section creates `regClassFractionOfSTMY{year}`, `stmyTVVEquations{year}`
//! and `stmyTVVCoeffs{year}` in the execution database, then caches the last
//! two under the unsuffixed names `stmyTVVEquations` / `stmyTVVCoeffs` for the
//! downstream Processing section.
//!
//! Two source paths:
//!
//! * **Snapshot path** — the snapshot pipeline captures the year-suffixed
//!   execution-DB tables (e.g. `stmyTVVCoeffs2020`). When present this
//!   generator just promotes them to the canonical names in scratch so the
//!   multiday-TVV calculator finds them via its scratch-first lookup.
//! * **Default-DB path** — the default DB ships only the raw `CumTVVCoeffs`
//!   table (no year-suffixed build). When the suffixed tables are absent this
//!   generator builds `stmyTVVEquations` / `stmyTVVCoeffs` from the raw tables
//!   (`CumTVVCoeffs`, `SampleVehiclePopulation`,
//!   `PollutantProcessMappedModelYear`, `AgeCategory`), exactly as the SQL
//!   does, and writes them into scratch under the unsuffixed names.

use std::collections::HashMap;

use moves_calculator_info::{Granularity, Priority};
use moves_data::ProcessId;
use moves_framework::{
    CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStore,
    DataFrameStoreTyped, Error, Generator, TableRow,
};
use polars::prelude::*;

use crate::calculators::multiday_tank_vapor_venting_calculator::{
    AgeCategoryRow, StmyTvvCoeffsRow, StmyTvvEquationsRow,
};

const NAME: &str = "NewTvvYearGenerator";

static INPUT_TABLES: &[&str] = &[];

static OUTPUT_TABLES: &[&str] = &["stmyTVVCoeffs", "stmyTVVEquations"];

/// Build a [`moves_framework::Error`] describing a failed column read, matching
/// the `from_dataframe` convention used across the calculator modules.
fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::Polars(format!("{table} row {row} column {column}: {msg}"))
}

/// One `CumTVVCoeffs` row — the per-regClass / modelYearGroup / ageGroup
/// cumulative-TVV coefficients shipped in the default database. Only the
/// columns the `NewTVVYear` aggregation reads are modelled.
///
/// `leakFractionIM` is schema-nullable (`double DEFAULT NULL`); modelled as
/// `Option<f64>` so MySQL's NULL-skipping `sum()` is reproduced faithfully.
#[derive(Debug, Clone, PartialEq)]
struct CumTvvCoeffsRow {
    /// `regClassID` — regulatory class.
    reg_class_id: i32,
    /// `modelYearGroupID` — the model-year group the coefficients apply to.
    model_year_group_id: i32,
    /// `ageGroupID` — the age-group bucket the coefficients apply to.
    age_group_id: i32,
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pol_process_id: i32,
    /// `backPurgeFactor`.
    back_purge_factor: f64,
    /// `averageCanisterCapacity`.
    average_canister_capacity: f64,
    /// `leakFraction`.
    leak_fraction: f64,
    /// `leakFractionIM` — schema-nullable; `None` is skipped in the sum.
    leak_fraction_im: Option<f64>,
    /// `tankSize`.
    tank_size: f64,
    /// `tankFillFraction`.
    tank_fill_fraction: f64,
    /// `tvvEquation` — the cumulative-TVV expression name (GROUP BY key).
    tvv_equation: String,
    /// `leakEquation` — the leaking-canister expression name (GROUP BY key).
    leak_equation: String,
}

impl TableRow for CumTvvCoeffsRow {
    fn table_name() -> &'static str {
        "CumTVVCoeffs"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("regClassID".into(), DataType::Int32),
            ("modelYearGroupID".into(), DataType::Int32),
            ("ageGroupID".into(), DataType::Int32),
            ("polProcessID".into(), DataType::Int32),
            ("backPurgeFactor".into(), DataType::Float64),
            ("averageCanisterCapacity".into(), DataType::Float64),
            ("leakFraction".into(), DataType::Float64),
            ("leakFractionIM".into(), DataType::Float64),
            ("tankSize".into(), DataType::Float64),
            ("tankFillFraction".into(), DataType::Float64),
            ("tvvEquation".into(), DataType::String),
            ("leakEquation".into(), DataType::String),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "regClassID".into(),
                    rows.iter().map(|r| r.reg_class_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "modelYearGroupID".into(),
                    rows.iter()
                        .map(|r| r.model_year_group_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageGroupID".into(),
                    rows.iter().map(|r| r.age_group_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "polProcessID".into(),
                    rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "backPurgeFactor".into(),
                    rows.iter()
                        .map(|r| r.back_purge_factor)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "averageCanisterCapacity".into(),
                    rows.iter()
                        .map(|r| r.average_canister_capacity)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "leakFraction".into(),
                    rows.iter().map(|r| r.leak_fraction).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "leakFractionIM".into(),
                    rows.iter()
                        .map(|r| r.leak_fraction_im.unwrap_or(f64::NAN))
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "tankSize".into(),
                    rows.iter().map(|r| r.tank_size).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "tankFillFraction".into(),
                    rows.iter()
                        .map(|r| r.tank_fill_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "tvvEquation".into(),
                    rows.iter()
                        .map(|r| r.tvv_equation.clone())
                        .collect::<Vec<String>>(),
                )
                .into(),
                Series::new(
                    "leakEquation".into(),
                    rows.iter()
                        .map(|r| r.leak_equation.clone())
                        .collect::<Vec<String>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "CumTVVCoeffs";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_str = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .str()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let rc = get_i32("regClassID")?;
        let myg = get_i32("modelYearGroupID")?;
        let ag = get_i32("ageGroupID")?;
        let pp = get_i32("polProcessID")?;
        let bp = get_f64("backPurgeFactor")?;
        let acc = get_f64("averageCanisterCapacity")?;
        let lf = get_f64("leakFraction")?;
        let lfim = get_f64("leakFractionIM")?;
        let ts = get_f64("tankSize")?;
        let tff = get_f64("tankFillFraction")?;
        let tvveq = get_str("tvvEquation")?;
        let leakeq = get_str("leakEquation")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(CumTvvCoeffsRow {
                    reg_class_id: rc.get(i).ok_or_else(|| null("regClassID"))?,
                    model_year_group_id: myg.get(i).ok_or_else(|| null("modelYearGroupID"))?,
                    age_group_id: ag.get(i).ok_or_else(|| null("ageGroupID"))?,
                    pol_process_id: pp.get(i).ok_or_else(|| null("polProcessID"))?,
                    back_purge_factor: bp.get(i).ok_or_else(|| null("backPurgeFactor"))?,
                    average_canister_capacity: acc
                        .get(i)
                        .ok_or_else(|| null("averageCanisterCapacity"))?,
                    leak_fraction: lf.get(i).ok_or_else(|| null("leakFraction"))?,
                    // Schema-nullable; NaN (written by into_dataframe) and
                    // genuine NULL both collapse to None so the sum skips them.
                    leak_fraction_im: lfim.get(i).filter(|v| v.is_finite()),
                    tank_size: ts.get(i).ok_or_else(|| null("tankSize"))?,
                    tank_fill_fraction: tff.get(i).ok_or_else(|| null("tankFillFraction"))?,
                    tvv_equation: tvveq.get(i).ok_or_else(|| null("tvvEquation"))?.to_string(),
                    leak_equation: leakeq
                        .get(i)
                        .ok_or_else(|| null("leakEquation"))?
                        .to_string(),
                })
            })
            .collect()
    }
}

/// One `SampleVehiclePopulation` row — the sample-vehicle stock fractions used
/// to derive `regClassFractionOfSTMY{year}`. Only the columns the
/// `NewTVVYear` aggregation reads are modelled (the existing
/// `SampleVehiclePopulationRow` in `activitycalculator/inputs.rs` omits the
/// `(sourceTypeID, fuelTypeID, regClassID)` keys this section needs).
#[derive(Debug, Clone, Copy, PartialEq)]
struct SampleVehiclePopulationRow {
    /// `sourceTypeModelYearID` — the `(sourceType, modelYear)` GROUP BY key.
    source_type_model_year_id: i32,
    /// `sourceTypeID`.
    source_type_id: i32,
    /// `modelYearID`.
    model_year_id: i32,
    /// `fuelTypeID`.
    fuel_type_id: i32,
    /// `regClassID` — regulatory class.
    reg_class_id: i32,
    /// `stmyFraction` — the sample-vehicle fraction.
    stmy_fraction: f64,
}

impl TableRow for SampleVehiclePopulationRow {
    fn table_name() -> &'static str {
        "SampleVehiclePopulation"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeModelYearID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("regClassID".into(), DataType::Int32),
            ("stmyFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeModelYearID".into(),
                    rows.iter()
                        .map(|r| r.source_type_model_year_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "modelYearID".into(),
                    rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "regClassID".into(),
                    rows.iter().map(|r| r.reg_class_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "stmyFraction".into(),
                    rows.iter().map(|r| r.stmy_fraction).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SampleVehiclePopulation";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let stmy = get_i32("sourceTypeModelYearID")?;
        let st = get_i32("sourceTypeID")?;
        let my = get_i32("modelYearID")?;
        let ft = get_i32("fuelTypeID")?;
        let rc = get_i32("regClassID")?;
        let frac = get_f64("stmyFraction")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SampleVehiclePopulationRow {
                    source_type_model_year_id: stmy
                        .get(i)
                        .ok_or_else(|| null("sourceTypeModelYearID"))?,
                    source_type_id: st.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    model_year_id: my.get(i).ok_or_else(|| null("modelYearID"))?,
                    fuel_type_id: ft.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    reg_class_id: rc.get(i).ok_or_else(|| null("regClassID"))?,
                    stmy_fraction: frac.get(i).ok_or_else(|| null("stmyFraction"))?,
                })
            })
            .collect()
    }
}

/// One `PollutantProcessMappedModelYear` row — maps a `(polProcessID,
/// modelYearGroupID)` onto its model years. The default-DB path synthesises
/// this table as a wholesale copy of `PollutantProcessModelYear`, so only the
/// `(polProcessID, modelYearID, modelYearGroupID)` columns the `NewTVVYear`
/// join needs are modelled.
#[derive(Debug, Clone, Copy, PartialEq)]
struct PollutantProcessMappedModelYearRow {
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pol_process_id: i32,
    /// `modelYearID` — the model year.
    model_year_id: i32,
    /// `modelYearGroupID` — the model-year group.
    model_year_group_id: i32,
}

impl TableRow for PollutantProcessMappedModelYearRow {
    fn table_name() -> &'static str {
        "PollutantProcessMappedModelYear"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("modelYearGroupID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "polProcessID".into(),
                    rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "modelYearID".into(),
                    rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "modelYearGroupID".into(),
                    rows.iter()
                        .map(|r| r.model_year_group_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "PollutantProcessMappedModelYear";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pp = get_i32("polProcessID")?;
        let my = get_i32("modelYearID")?;
        let mg = get_i32("modelYearGroupID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(PollutantProcessMappedModelYearRow {
                    pol_process_id: pp.get(i).ok_or_else(|| null("polProcessID"))?,
                    model_year_id: my.get(i).ok_or_else(|| null("modelYearID"))?,
                    model_year_group_id: mg.get(i).ok_or_else(|| null("modelYearGroupID"))?,
                })
            })
            .collect()
    }
}

/// The `regClassFractionOfSTMY{year}` intermediate (SQL lines 336-353): the
/// `(sourceType, modelYear, fuelType, regClass)` share of a sample-vehicle
/// `(sourceType, modelYear)` group, summed over `stmyFraction`.
#[derive(Debug, Clone, Copy)]
struct RegClassFractionRow {
    source_type_id: i32,
    model_year_id: i32,
    fuel_type_id: i32,
    reg_class_id: i32,
    fraction: f64,
}

/// Build `regClassFractionOfSTMY{year}` (SQL lines 347-353).
///
/// `select sourceTypeID, modelYearID, fuelTypeID, regClassID,
/// sum(stmyFraction) ... group by sourceTypeModelYearID, fuelTypeID, regClassID
/// having sum(stmyFraction) > 0`. The GROUP BY key is the
/// `sourceTypeModelYearID` (each `(sourceType, modelYear)` pair) plus
/// `fuelTypeID`, `regClassID`.
fn build_reg_class_fractions(svp: &[SampleVehiclePopulationRow]) -> Vec<RegClassFractionRow> {
    // Accumulate by the canonical GROUP BY key (sourceTypeModelYearID,
    // fuelTypeID, regClassID), retaining the row's sourceTypeID/modelYearID for
    // the projection (they are functionally determined by sourceTypeModelYearID).
    let mut acc: HashMap<(i32, i32, i32), RegClassFractionRow> = HashMap::new();
    for r in svp {
        let entry = acc
            .entry((r.source_type_model_year_id, r.fuel_type_id, r.reg_class_id))
            .or_insert(RegClassFractionRow {
                source_type_id: r.source_type_id,
                model_year_id: r.model_year_id,
                fuel_type_id: r.fuel_type_id,
                reg_class_id: r.reg_class_id,
                fraction: 0.0,
            });
        entry.fraction += r.stmy_fraction;
    }
    // having sum(stmyFraction) > 0
    acc.into_values().filter(|r| r.fraction > 0.0).collect()
}

/// Accumulator for one `stmyTVVEquations` GROUP BY bucket (SQL lines 400-403):
/// `(sourceTypeID, modelYearID, fuelTypeID, polProcessID, regClassID,
/// tvvEquation, leakEquation)`. Each coefficient column is a NULL-skipping
/// weighted sum; `leakFractionIM` tracks whether any contributing row was
/// non-NULL so the all-NULL bucket yields `None` (matching MySQL `sum()`).
#[derive(Default)]
struct EquationAccumulator {
    back_purge_factor: f64,
    average_canister_capacity: f64,
    reg_class_fraction: f64,
    leak_fraction: f64,
    leak_fraction_im: f64,
    leak_fraction_im_present: bool,
    tank_size: f64,
    tank_fill_fraction: f64,
}

/// Build `stmyTVVEquations{year}` and `stmyTVVCoeffs{year}` from the raw
/// default-DB tables, porting SQL lines 355-431.
fn build_tvv_tables(
    cum: &[CumTvvCoeffsRow],
    svp: &[SampleVehiclePopulationRow],
    ppmy: &[PollutantProcessMappedModelYearRow],
    age: &[AgeCategoryRow],
    year: i32,
) -> (Vec<StmyTvvEquationsRow>, Vec<StmyTvvCoeffsRow>) {
    // -- regClassFractionOfSTMY{year} (lines 336-353)
    let reg_fractions = build_reg_class_fractions(svp);
    // Index by (modelYearID, regClassID) for the rf join (lines 395-397). The
    // join also keys sourceTypeID/fuelTypeID into the GROUP BY, so retain all
    // matching rows per (modelYear, regClass).
    let mut rf_index: HashMap<(i32, i32), Vec<RegClassFractionRow>> = HashMap::new();
    for rf in reg_fractions {
        rf_index
            .entry((rf.model_year_id, rf.reg_class_id))
            .or_default()
            .push(rf);
    }

    // ageCategory: a.ageGroupID → set of a.ageID (line 393-394). The join keeps
    // every (ageGroup, ageID) pair; the WHERE then fixes modelYearID = year -
    // ageID (line 398).
    let mut age_index: HashMap<i32, Vec<i32>> = HashMap::new();
    for a in age {
        age_index.entry(a.age_group_id).or_default().push(a.age_id);
    }

    // ppmy keyed by (polProcessID, modelYearGroupID) → modelYearID (lines
    // 390-392). The WHERE then requires ppmy.modelYearID = year - ageID.
    let mut ppmy_index: HashMap<(i32, i32), Vec<i32>> = HashMap::new();
    for p in ppmy {
        ppmy_index
            .entry((p.pol_process_id, p.model_year_group_id))
            .or_default()
            .push(p.model_year_id);
    }

    // -- stmyTVVEquations{year} (lines 355-403): join + grouped weighted sums.
    // GROUP BY key: (sourceTypeID, modelYearID, fuelTypeID, polProcessID,
    // regClassID, tvvEquation, leakEquation).
    type EqKey = (i32, i32, i32, i32, i32, String, String);
    let mut eq_acc: HashMap<EqKey, EquationAccumulator> = HashMap::new();

    for c in cum {
        // ppmy join on (polProcessID, modelYearGroupID)
        let Some(ppmy_mys) = ppmy_index.get(&(c.pol_process_id, c.model_year_group_id)) else {
            continue;
        };
        // ageCategory join on ageGroupID
        let Some(age_ids) = age_index.get(&c.age_group_id) else {
            continue;
        };
        for &age_id in age_ids {
            // WHERE ppmy.modelYearID = year - ageID
            let target_my = year - age_id;
            // ppmy must contain that modelYearID for this (polProcess, mYGroup)
            if !ppmy_mys.contains(&target_my) {
                continue;
            }
            // rf join: rf.modelYearID = ppmy.modelYearID AND rf.regClassID =
            // c.regClassID
            let Some(rfs) = rf_index.get(&(target_my, c.reg_class_id)) else {
                continue;
            };
            for rf in rfs {
                let w = rf.fraction;
                let key: EqKey = (
                    rf.source_type_id,
                    rf.model_year_id,
                    rf.fuel_type_id,
                    c.pol_process_id,
                    c.reg_class_id,
                    c.tvv_equation.clone(),
                    c.leak_equation.clone(),
                );
                let e = eq_acc.entry(key).or_default();
                e.back_purge_factor += c.back_purge_factor * w;
                e.average_canister_capacity += c.average_canister_capacity * w;
                e.reg_class_fraction += w;
                e.leak_fraction += c.leak_fraction * w;
                // sum(leakFractionIM * fraction) — MySQL skips NULL operands.
                if let Some(lfim) = c.leak_fraction_im {
                    e.leak_fraction_im += lfim * w;
                    e.leak_fraction_im_present = true;
                }
                e.tank_size += c.tank_size * w;
                e.tank_fill_fraction += c.tank_fill_fraction * w;
            }
        }
    }

    let equations: Vec<StmyTvvEquationsRow> = eq_acc
        .into_iter()
        .map(|(k, e)| StmyTvvEquationsRow {
            source_type_id: k.0,
            model_year_id: k.1,
            fuel_type_id: k.2,
            pol_process_id: k.3,
            reg_class_id: k.4,
            tvv_equation: k.5,
            leak_equation: k.6,
            back_purge_factor: e.back_purge_factor,
            average_canister_capacity: e.average_canister_capacity,
            reg_class_fraction_of_source_type_model_year_fuel: e.reg_class_fraction,
            leak_fraction: e.leak_fraction,
            // All-NULL bucket → None (MySQL sum() of all-NULL is NULL).
            leak_fraction_im: if e.leak_fraction_im_present {
                Some(e.leak_fraction_im)
            } else {
                None
            },
            tank_size: e.tank_size,
            tank_fill_fraction: e.tank_fill_fraction,
        })
        .collect();

    // -- stmyTVVCoeffs{year} (lines 405-431): sum the equation rows across
    // regClass / equation. GROUP BY (sourceTypeID, modelYearID, fuelTypeID,
    // polProcessID).
    #[derive(Default)]
    struct CoeffAccumulator {
        back_purge_factor: f64,
        average_canister_capacity: f64,
        leak_fraction: f64,
        leak_fraction_im: f64,
        tank_size: f64,
        tank_fill_fraction: f64,
    }
    let mut coeff_acc: HashMap<(i32, i32, i32, i32), CoeffAccumulator> = HashMap::new();
    for eq in &equations {
        let c = coeff_acc
            .entry((
                eq.source_type_id,
                eq.model_year_id,
                eq.fuel_type_id,
                eq.pol_process_id,
            ))
            .or_default();
        c.back_purge_factor += eq.back_purge_factor;
        c.average_canister_capacity += eq.average_canister_capacity;
        c.leak_fraction += eq.leak_fraction;
        // sum(leakFractionIM) over equation rows; None contributes nothing.
        if let Some(lfim) = eq.leak_fraction_im {
            c.leak_fraction_im += lfim;
        }
        c.tank_size += eq.tank_size;
        c.tank_fill_fraction += eq.tank_fill_fraction;
    }

    let coeffs: Vec<StmyTvvCoeffsRow> = coeff_acc
        .into_iter()
        .map(|(k, c)| StmyTvvCoeffsRow {
            source_type_id: k.0,
            model_year_id: k.1,
            fuel_type_id: k.2,
            pol_process_id: k.3,
            back_purge_factor: c.back_purge_factor,
            average_canister_capacity: c.average_canister_capacity,
            leak_fraction: c.leak_fraction,
            leak_fraction_im: c.leak_fraction_im,
            tank_size: c.tank_size,
            tank_fill_fraction: c.tank_fill_fraction,
        })
        .collect();

    (equations, coeffs)
}

#[derive(Debug)]
pub struct NewTvvYearGenerator {
    subscriptions: Vec<CalculatorSubscription>,
}

impl NewTvvYearGenerator {
    pub const NAME: &'static str = NAME;

    #[must_use]
    pub fn new() -> Self {
        let priority =
            Priority::parse("GENERATOR").expect("\"GENERATOR\" is a canonical MasterLoopPriority");
        Self {
            subscriptions: vec![CalculatorSubscription::new(
                ProcessId(12),
                Granularity::Year,
                priority,
            )],
        }
    }
}

impl Default for NewTvvYearGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl Generator for NewTvvYearGenerator {
    fn name(&self) -> &'static str {
        NAME
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        &self.subscriptions
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn output_tables(&self) -> &[&'static str] {
        OUTPUT_TABLES
    }

    fn execute(&self, ctx: &mut CalculatorContext) -> Result<CalculatorOutput, Error> {
        let year = ctx
            .position()
            .time
            .year
            .ok_or_else(|| Error::Polars("no year in iteration position".into()))?;

        // Snapshot path: the year-suffixed execution-DB tables are present —
        // promote them to the canonical scratch names verbatim.
        let have_suffixed = ctx
            .tables()
            .get(&format!("stmyTVVCoeffs{year}"))
            .is_some()
            || ctx
                .tables()
                .get(&format!("stmyTVVEquations{year}"))
                .is_some();
        if have_suffixed {
            for base in ["stmyTVVCoeffs", "stmyTVVEquations"] {
                let year_name = format!("{base}{year}");
                if let Some(arc_df) = ctx.tables().get(&year_name) {
                    let df_owned = (*arc_df).clone();
                    ctx.scratch_mut().insert(base, df_owned);
                }
            }
            return Ok(CalculatorOutput::empty());
        }

        // Default-DB path: build stmyTVVEquations / stmyTVVCoeffs from the raw
        // tables, porting the NewTVVYear section (SQL lines 336-431).
        let cum: Vec<CumTvvCoeffsRow> = ctx.tables().iter_typed("CumTVVCoeffs")?;
        let svp: Vec<SampleVehiclePopulationRow> =
            ctx.tables().iter_typed_or_empty("SampleVehiclePopulation")?;
        let ppmy: Vec<PollutantProcessMappedModelYearRow> = ctx
            .tables()
            .iter_typed("PollutantProcessMappedModelYear")?;
        let age: Vec<AgeCategoryRow> = ctx.tables().iter_typed("AgeCategory")?;

        let (equations, coeffs) = build_tvv_tables(&cum, &svp, &ppmy, &age, i32::from(year));

        crate::wiring::write_scratch_table(ctx, "stmyTVVEquations", equations)?;
        crate::wiring::write_scratch_table(ctx, "stmyTVVCoeffs", coeffs)?;

        Ok(CalculatorOutput::empty())
    }
}

pub fn factory() -> Box<dyn Generator> {
    Box::new(NewTvvYearGenerator::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use moves_framework::execution::execution_db::{
        ExecutionLocation, ExecutionTime, IterationPosition,
    };
    use moves_framework::{CalculatorContext, DataFrameStore, InMemoryStore};

    fn make_df(col: &str) -> DataFrame {
        let s = Series::new(col.into(), [1i32]);
        DataFrame::new(1, vec![s.into()]).unwrap()
    }

    fn position_year_2020() -> IterationPosition {
        IterationPosition {
            iteration: 0,
            process_id: Some(ProcessId(12)),
            location: ExecutionLocation::link(26, 26_161, 90, 5001),
            time: ExecutionTime::year(2020),
        }
    }

    #[test]
    fn generator_metadata() {
        let gen = NewTvvYearGenerator::new();
        assert_eq!(gen.name(), NAME);
        assert_eq!(gen.output_tables(), OUTPUT_TABLES);
        assert_eq!(gen.subscriptions().len(), 1);
        assert_eq!(gen.subscriptions()[0].process_id, ProcessId(12));
    }

    #[test]
    fn factory_builds_generator() {
        let gen = factory();
        assert_eq!(gen.name(), NAME);
    }

    #[test]
    fn execute_copies_year_suffixed_tables_to_scratch() {
        let mut store = InMemoryStore::new();
        store.insert("stmyTVVCoeffs2020", make_df("sourceTypeID"));
        store.insert("stmyTVVEquations2020", make_df("tvvEquation"));

        let mut ctx = CalculatorContext::with_position_and_tables(position_year_2020(), store);
        NewTvvYearGenerator::new()
            .execute(&mut ctx)
            .expect("execute ok");

        assert!(
            ctx.scratch().store.contains("stmyTVVCoeffs"),
            "stmyTVVCoeffs must be in scratch after execute"
        );
        assert!(
            ctx.scratch().store.contains("stmyTVVEquations"),
            "stmyTVVEquations must be in scratch after execute"
        );
    }

    /// With no year-suffixed table and no raw tables either, the build path
    /// runs and writes empty (but present) canonical tables to scratch.
    #[test]
    fn execute_builds_empty_when_no_sources() {
        let mut store = InMemoryStore::new();
        // Provide the raw tables empty so iter_typed sees a schema'd frame.
        store.insert(
            "CumTVVCoeffs",
            CumTvvCoeffsRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert(
            "PollutantProcessMappedModelYear",
            PollutantProcessMappedModelYearRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert("AgeCategory", AgeCategoryRow::into_dataframe(vec![]).unwrap());

        let mut ctx = CalculatorContext::with_position_and_tables(position_year_2020(), store);
        NewTvvYearGenerator::new()
            .execute(&mut ctx)
            .expect("execute ok");

        assert!(
            ctx.scratch().store.contains("stmyTVVCoeffs"),
            "stmyTVVCoeffs must be built into scratch"
        );
        assert!(
            ctx.scratch().store.contains("stmyTVVEquations"),
            "stmyTVVEquations must be built into scratch"
        );
    }

    /// End-to-end build: a single CumTVVCoeffs row weighted by two regClass
    /// fractions of the same (sourceType, modelYear, fuelType) collapses to one
    /// coeffs row whose coefficient = coeff * fraction.
    #[test]
    fn build_weights_and_sums() {
        let year = 2020;
        // age 0 → modelYear 2020; ageGroup 1.
        let age = vec![AgeCategoryRow {
            age_id: 0,
            age_group_id: 1,
        }];
        // ppmy: polProcess 8412, modelYearGroup 0 → modelYear 2020.
        let ppmy = vec![PollutantProcessMappedModelYearRow {
            pol_process_id: 8412,
            model_year_id: 2020,
            model_year_group_id: 0,
        }];
        // CumTVVCoeffs: regClass 10, ageGroup 1, mYGroup 0, polProcess 8412.
        let cum = vec![CumTvvCoeffsRow {
            reg_class_id: 10,
            model_year_group_id: 0,
            age_group_id: 1,
            pol_process_id: 8412,
            back_purge_factor: 2.0,
            average_canister_capacity: 4.0,
            leak_fraction: 0.5,
            leak_fraction_im: None,
            tank_size: 60.0,
            tank_fill_fraction: 0.4,
            tvv_equation: "eq".to_string(),
            leak_equation: "leq".to_string(),
        }];
        // SVP: two rows, same (sourceType 21, modelYear 2020, fuel 1, regClass
        // 10), fractions 0.3 + 0.2 = 0.5.
        let svp = vec![
            SampleVehiclePopulationRow {
                source_type_model_year_id: 212020,
                source_type_id: 21,
                model_year_id: 2020,
                fuel_type_id: 1,
                reg_class_id: 10,
                stmy_fraction: 0.3,
            },
            SampleVehiclePopulationRow {
                source_type_model_year_id: 212020,
                source_type_id: 21,
                model_year_id: 2020,
                fuel_type_id: 1,
                reg_class_id: 10,
                stmy_fraction: 0.2,
            },
        ];

        let (equations, coeffs) = build_tvv_tables(&cum, &svp, &ppmy, &age, year);
        assert_eq!(equations.len(), 1, "one equation bucket");
        assert_eq!(coeffs.len(), 1, "one coeffs bucket");
        let eq = &equations[0];
        assert!((eq.reg_class_fraction_of_source_type_model_year_fuel - 0.5).abs() < 1e-12);
        // backPurgeFactor = 2.0 * 0.5 = 1.0
        assert!((eq.back_purge_factor - 1.0).abs() < 1e-12);
        assert_eq!(eq.leak_fraction_im, None, "all-NULL leakFractionIM → None");
        let co = &coeffs[0];
        assert_eq!(co.source_type_id, 21);
        assert_eq!(co.pol_process_id, 8412);
        assert!((co.back_purge_factor - 1.0).abs() < 1e-12);
        // leakFraction = 0.5 * 0.5 = 0.25
        assert!((co.leak_fraction - 0.25).abs() < 1e-12);
        assert_eq!(co.leak_fraction_im, 0.0, "all-None coeffs leakFractionIM → 0");
    }
}

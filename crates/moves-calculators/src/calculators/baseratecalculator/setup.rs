//! Input tables and the setup step for the Base Rate Calculator.
//!
//! Ports the Go `StartSetup` function from `baseratecalculator.go`. The Go
//! worker reads roughly twenty tab-delimited files into package-level hash
//! maps; the pure port instead takes a [`BaseRateCalculatorInputs`] holding
//! those tables as plain row vectors and derives a [`PreparedTables`] from it.
//!
//! [`BaseRateCalculatorInputs`] is the data-plane contract: a future Task 50
//! (`DataFrameStore`) wiring populates it from the scratch / default-DB
//! `DataFrame`s. [`PreparedTables::from_inputs`] reproduces the keying, the
//! `TemperatureAdjustment` model-year expansion, and the `IMCoverage` join
//! the Go `StartSetup` performed.
//!
//! # Fidelity note — duplicate keys
//!
//! Several Go loaders print `"ERROR: Already exists"` on a duplicate key but
//! then overwrite the entry anyway. The port reproduces the overwrite
//! ([`BTreeMap::insert`] semantics) and drops the diagnostic print.

use std::collections::{BTreeMap, BTreeSet};

use moves_framework::{data::TableRow, Error as MfError};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

use super::model::{
    ActivityWeightKey, CountyDetail, CriteriaRatioDetail, CriteriaRatioKey, EvEfficiencyDetail,
    FuelSupplyDetail, GeneralFuelRatioDetail, GeneralFuelRatioInnerDetail, ImCoverageKey,
    ImFactorKey, ModelYearFuelKey, NoxHumidityAdjustDetail, PolProcSourceRegFuelMyKey,
    PollutantProcessMappedModelYearDetail, PollutantProcessMappedModelYearKey, RunConstants,
    StartTempAdjustmentDetail, StartTempAdjustmentKey, TemperatureAdjustmentDetail,
    TemperatureAdjustmentKey, UniversalActivityKey, ZoneAcFactorKey, ZoneMonthHourDetail,
    ZoneMonthHourKey,
};

/// One `BaseRate` / `BaseRateByAge` file row — the rates the Base Rate
/// Generator (Task 42) produced for this calculator to adjust.
///
/// The Go `streamBaseRateByAge` reads a 22-column row and `streamBaseRate` a
/// 21-column row; the only difference is an `ageGroupID` column the Go reads
/// but never stores (`MWOKey.CalcIDs` re-derives age data). The port carries
/// one row type for both: every field below is read by the Go, `ageGroupID`
/// is not.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct BaseRateRow {
    /// Source type id.
    pub source_type_id: i32,
    /// Road type id.
    pub road_type_id: i32,
    /// Average-speed bin id.
    pub avg_speed_bin_id: i32,
    /// Hour/day id.
    pub hour_day_id: i32,
    /// Pollutant id.
    pub pollutant_id: i32,
    /// Process id.
    pub process_id: i32,
    /// Model year id.
    pub model_year_id: i32,
    /// Fuel type id.
    pub fuel_type_id: i32,
    /// Regulatory class id.
    pub reg_class_id: i32,
    /// Operating-mode id.
    pub op_mode_id: i32,
    /// Mean base rate.
    pub mean_base_rate: f64,
    /// Mean base rate, I/M adjusted.
    pub mean_base_rate_im: f64,
    /// Emission rate.
    pub emission_rate: f64,
    /// Emission rate, I/M adjusted.
    pub emission_rate_im: f64,
    /// Mean base rate, air-conditioning adjusted.
    pub mean_base_rate_ac_adj: f64,
    /// Mean base rate, I/M and air-conditioning adjusted.
    pub mean_base_rate_im_ac_adj: f64,
    /// Emission rate, air-conditioning adjusted.
    pub emission_rate_ac_adj: f64,
    /// Emission rate, I/M and air-conditioning adjusted.
    pub emission_rate_im_ac_adj: f64,
    /// Operating-mode fraction (inventory weighting).
    pub op_mode_fraction: f64,
    /// Operating-mode fraction (rate weighting).
    pub op_mode_fraction_rate: f64,
}

/// One row of an extended-idle / APU / shorepower hourly-fraction table.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct ModelYearFuelFractionRow {
    /// Model year id.
    pub model_year_id: i32,
    /// Fuel type id.
    pub fuel_type_id: i32,
    /// Hourly operating-mode fraction adjustment.
    pub hour_fraction_adjust: f64,
}

/// One `ZoneMonthHour` meteorology row.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct ZoneMonthHourRow {
    /// Month id.
    pub month_id: i32,
    /// Zone id.
    pub zone_id: i32,
    /// Hour id.
    pub hour_id: i32,
    /// Temperature (°F).
    pub temperature: f64,
    /// Relative humidity (%).
    pub rel_humidity: f64,
    /// Heat index (°F).
    pub heat_index: f64,
    /// Specific humidity (g H₂O per kg dry air).
    pub specific_humidity: f64,
    /// Water mole fraction.
    pub mol_water_fraction: f64,
}

/// One `PollutantProcessMappedModelYear` row.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct PollutantProcessMappedModelYearRow {
    /// Pollutant/process id.
    pub pol_process_id: i32,
    /// Model year id.
    pub model_year_id: i32,
    /// Model year group id.
    pub model_year_group_id: i32,
    /// Fuel model year group id.
    pub fuel_my_group_id: i32,
    /// I/M model year group id.
    pub im_model_year_group_id: i32,
}

/// One `StartTempAdjustment` row.
///
/// The file also carries coefficient-of-variation columns (`tempAdjustTermACV`
/// …); the Go does not read them and neither does the port.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct StartTempAdjustmentRow {
    /// Fuel type id.
    pub fuel_type_id: i32,
    /// Pollutant/process id.
    pub pol_process_id: i32,
    /// Model year group id.
    pub model_year_group_id: i32,
    /// Operating-mode id.
    pub op_mode_id: i32,
    /// Term `A`.
    pub term_a: f64,
    /// Term `B`.
    pub term_b: f64,
    /// Term `C`.
    pub term_c: f64,
    /// `startTempEquationType` — `"LOG"`, `"POLY"`, or other.
    pub equation_type: String,
}

/// One `County` row. The file carries seven columns; only these three are
/// read by the Go.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct CountyRow {
    /// County id.
    pub county_id: i32,
    /// Geographic-phase-in area fraction.
    pub gpa_fract: f64,
    /// Barometric pressure.
    pub barometric_pressure: f64,
}

/// One `GeneralFuelRatio` row.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct GeneralFuelRatioRow {
    /// Fuel formulation id.
    pub fuel_formulation_id: i32,
    /// Pollutant/process id.
    pub pol_process_id: i32,
    /// Source type id.
    pub source_type_id: i32,
    /// First model year the ratio applies to.
    pub min_model_year_id: i32,
    /// Last model year the ratio applies to.
    pub max_model_year_id: i32,
    /// First age the ratio applies to.
    pub min_age_id: i32,
    /// Last age the ratio applies to.
    pub max_age_id: i32,
    /// Fuel effect ratio (normal area).
    pub fuel_effect_ratio: f64,
    /// Fuel effect ratio (geographic-phase-in area).
    pub fuel_effect_ratio_gpa: f64,
}

/// One `CriteriaRatio` / `AltCriteriaRatio` row.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct CriteriaRatioRow {
    /// Fuel formulation id.
    pub fuel_formulation_id: i32,
    /// Pollutant/process id.
    pub pol_process_id: i32,
    /// Source type id.
    pub source_type_id: i32,
    /// Model year id.
    pub model_year_id: i32,
    /// Age id.
    pub age_id: i32,
    /// Ratio (normal area).
    pub ratio: f64,
    /// Ratio (geographic-phase-in area).
    pub ratio_gpa: f64,
    /// Ratio with no sulfur effect.
    pub ratio_no_sulfur: f64,
}

/// One `TemperatureAdjustment` row. The Go expands each row across the
/// model-year range `[minModelYearID, maxModelYearID]`.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct TemperatureAdjustmentRow {
    /// Pollutant/process id.
    pub pol_process_id: i32,
    /// Fuel type id.
    pub fuel_type_id: i32,
    /// Regulatory class id (`0` is the wildcard).
    pub reg_class_id: i32,
    /// First model year the row applies to (clamped up to `1950`).
    pub min_model_year_id: i32,
    /// Last model year the row applies to (clamped down to `2060`).
    pub max_model_year_id: i32,
    /// Term `A`.
    pub term_a: f64,
    /// Term `B`.
    pub term_b: f64,
    /// Term `C` — nullable in MOVES; `None` (SQL NULL) coalesces to `0.0`.
    pub term_c: Option<f64>,
}

/// One `NOxHumidityAdjust` row, keyed by fuel type.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct NoxHumidityAdjustRow {
    /// Fuel type id.
    pub fuel_type_id: i32,
    /// Equation name.
    pub humidity_nox_eq: String,
    /// Term `A`.
    pub humidity_term_a: f64,
    /// Term `B` — nullable in MOVES; `None` (SQL NULL) coalesces to `0.0`.
    pub humidity_term_b: Option<f64>,
    /// Lower bound on the humidity input.
    pub humidity_low_bound: f64,
    /// Upper bound on the humidity input.
    pub humidity_up_bound: f64,
    /// Humidity units label.
    pub humidity_units: String,
}

/// One `zoneACFactor` row.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct ZoneAcFactorRow {
    /// Hour id.
    pub hour_id: i32,
    /// Source type id.
    pub source_type_id: i32,
    /// Model year id.
    pub model_year_id: i32,
    /// Air-conditioning factor.
    pub ac_factor: f64,
}

/// One `IMFactor` row.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct ImFactorRow {
    /// Pollutant/process id.
    pub pol_process_id: i32,
    /// Inspection frequency.
    pub inspect_freq: i32,
    /// Test standards id.
    pub test_standards_id: i32,
    /// Source type id.
    pub source_type_id: i32,
    /// Fuel type id.
    pub fuel_type_id: i32,
    /// I/M model year group id.
    pub im_model_year_group_id: i32,
    /// Age group id.
    pub age_group_id: i32,
    /// I/M factor.
    pub im_factor: f64,
}

/// One `IMCoverage` row. The Go expands each row across the model-year range
/// `[begModelYearID, endModelYearID]`, joining against
/// `PollutantProcessMappedModelYear`, `AgeCategory`, and `IMFactor`.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct ImCoverageRow {
    /// Pollutant/process id.
    pub pol_process_id: i32,
    /// Source type id.
    pub source_type_id: i32,
    /// Fuel type id.
    pub fuel_type_id: i32,
    /// First model year of the program (clamped up to `1950`).
    pub beg_model_year_id: i32,
    /// Last model year of the program (clamped down to `2060`).
    pub end_model_year_id: i32,
    /// Inspection frequency.
    pub inspect_freq: i32,
    /// Test standards id.
    pub test_standards_id: i32,
    /// Compliance factor, as a percentage (the Go scales it by `0.01`).
    pub compliance_factor: f64,
}

/// One `EmissionRateAdjustmentWorker` row.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct EmissionRateAdjustmentRow {
    /// Pollutant/process id.
    pub pol_process_id: i32,
    /// Source type id.
    pub source_type_id: i32,
    /// Regulatory class id.
    pub reg_class_id: i32,
    /// Fuel type id.
    pub fuel_type_id: i32,
    /// First model year the row applies to (inclusive).
    pub begin_model_year_id: i32,
    /// Last model year the row applies to (inclusive).
    pub end_model_year_id: i32,
    /// Emission rate adjustment factor.
    pub emission_rate_adjustment: f64,
}

/// One `evefficiencyWorker` row.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct EvEfficiencyRow {
    /// Pollutant/process id.
    pub pol_process_id: i32,
    /// Source type id.
    pub source_type_id: i32,
    /// Regulatory class id.
    pub reg_class_id: i32,
    /// Fuel type id.
    pub fuel_type_id: i32,
    /// Model year id.
    pub model_year_id: i32,
    /// Battery efficiency.
    pub battery_efficiency: f64,
    /// Charging efficiency.
    pub charging_efficiency: f64,
}

/// One `universalActivity` row.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct UniversalActivityRow {
    /// Hour/day id.
    pub hour_day_id: i32,
    /// Model year id.
    pub model_year_id: i32,
    /// Source type id.
    pub source_type_id: i32,
    /// Activity.
    pub activity: f64,
}

/// One `smfrSBDSummary` row — a source-bin-distribution total.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct SmfrSbdSummaryRow {
    /// Source type id.
    pub source_type_id: i32,
    /// Model year id.
    pub model_year_id: i32,
    /// Fuel type id.
    pub fuel_type_id: i32,
    /// Regulatory class id.
    pub reg_class_id: i32,
    /// Source-bin distribution total.
    pub sbd_total: f64,
}

/// One `AgeCategory` row — the age-id → age-group-id mapping the Go `mwo`
/// package keeps in its `AgeGroups` global.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct AgeCategoryRow {
    /// Age id.
    pub age_id: i32,
    /// Age group id.
    pub age_group_id: i32,
}

/// One `FuelFormulation` row. The Go `mwo.FuelFormulation` struct carries two
/// dozen fuel-chemistry columns; the Base Rate Calculator reads only
/// `fuelSubTypeID` (in the E85 THC step).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct FuelFormulationRow {
    /// Fuel formulation id.
    pub fuel_formulation_id: i32,
    /// Fuel subtype id.
    pub fuel_sub_type_id: i32,
}

/// One `FuelSupply` row — a fuel formulation supplied to a
/// `(county, year, month, fuelType)` cell with its market share.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct FuelSupplyRow {
    /// County id.
    pub county_id: i32,
    /// Calendar year id.
    pub year_id: i32,
    /// Month id.
    pub month_id: i32,
    /// Fuel type id.
    pub fuel_type_id: i32,
    /// Fuel subtype id.
    pub fuel_sub_type_id: i32,
    /// Fuel formulation id.
    pub fuel_formulation_id: i32,
    /// Market share of this formulation within the fuel type.
    pub market_share: f64,
}

/// One `FuelType` row — a valid fuel-type id.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct FuelTypeRow {
    /// Fuel type id.
    pub fuel_type_id: i32,
}

fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> MfError {
    MfError::RowExtraction {
        table: table.to_string(),
        row,
        column: column.to_string(),
        message: msg,
    }
}

impl TableRow for BaseRateRow {
    fn table_name() -> &'static str {
        "BaseRate"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("avgSpeedBinID".into(), DataType::Int32),
            ("hourDayID".into(), DataType::Int32),
            ("pollutantID".into(), DataType::Int32),
            ("processID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("regClassID".into(), DataType::Int32),
            ("opModeID".into(), DataType::Int32),
            ("meanBaseRate".into(), DataType::Float64),
            ("meanBaseRateIM".into(), DataType::Float64),
            ("emissionRate".into(), DataType::Float64),
            ("emissionRateIM".into(), DataType::Float64),
            ("meanBaseRateACAdj".into(), DataType::Float64),
            ("meanBaseRateIMACAdj".into(), DataType::Float64),
            ("emissionRateACAdj".into(), DataType::Float64),
            ("emissionRateIMACAdj".into(), DataType::Float64),
            ("opModeFraction".into(), DataType::Float64),
            ("opModeFractionRate".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeID".into(),
                    rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "avgSpeedBinID".into(),
                    rows.iter()
                        .map(|r| r.avg_speed_bin_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourDayID".into(),
                    rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
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
                    "opModeID".into(),
                    rows.iter().map(|r| r.op_mode_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "meanBaseRate".into(),
                    rows.iter().map(|r| r.mean_base_rate).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "meanBaseRateIM".into(),
                    rows.iter()
                        .map(|r| r.mean_base_rate_im)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "emissionRate".into(),
                    rows.iter().map(|r| r.emission_rate).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "emissionRateIM".into(),
                    rows.iter()
                        .map(|r| r.emission_rate_im)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "meanBaseRateACAdj".into(),
                    rows.iter()
                        .map(|r| r.mean_base_rate_ac_adj)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "meanBaseRateIMACAdj".into(),
                    rows.iter()
                        .map(|r| r.mean_base_rate_im_ac_adj)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "emissionRateACAdj".into(),
                    rows.iter()
                        .map(|r| r.emission_rate_ac_adj)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "emissionRateIMACAdj".into(),
                    rows.iter()
                        .map(|r| r.emission_rate_im_ac_adj)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "opModeFraction".into(),
                    rows.iter()
                        .map(|r| r.op_mode_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "opModeFractionRate".into(),
                    rows.iter()
                        .map(|r| r.op_mode_fraction_rate)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "BaseRate";
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
        let source_type_id = get_i32("sourceTypeID")?;
        let road_type_id = get_i32("roadTypeID")?;
        let avg_speed_bin_id = get_i32("avgSpeedBinID")?;
        let hour_day_id = get_i32("hourDayID")?;
        let pollutant_id = get_i32("pollutantID")?;
        let process_id = get_i32("processID")?;
        let model_year_id = get_i32("modelYearID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        let reg_class_id = get_i32("regClassID")?;
        let op_mode_id = get_i32("opModeID")?;
        let mean_base_rate = get_f64("meanBaseRate")?;
        let mean_base_rate_im = get_f64("meanBaseRateIM")?;
        let emission_rate = get_f64("emissionRate")?;
        let emission_rate_im = get_f64("emissionRateIM")?;
        let mean_base_rate_ac_adj = get_f64("meanBaseRateACAdj")?;
        let mean_base_rate_im_ac_adj = get_f64("meanBaseRateIMACAdj")?;
        let emission_rate_ac_adj = get_f64("emissionRateACAdj")?;
        let emission_rate_im_ac_adj = get_f64("emissionRateIMACAdj")?;
        let op_mode_fraction = get_f64("opModeFraction")?;
        let op_mode_fraction_rate = get_f64("opModeFractionRate")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(BaseRateRow {
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
                    avg_speed_bin_id: avg_speed_bin_id
                        .get(i)
                        .ok_or_else(|| null("avgSpeedBinID"))?,
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    pollutant_id: pollutant_id.get(i).ok_or_else(|| null("pollutantID"))?,
                    process_id: process_id.get(i).ok_or_else(|| null("processID"))?,
                    model_year_id: model_year_id.get(i).ok_or_else(|| null("modelYearID"))?,
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    reg_class_id: reg_class_id.get(i).ok_or_else(|| null("regClassID"))?,
                    op_mode_id: op_mode_id.get(i).ok_or_else(|| null("opModeID"))?,
                    mean_base_rate: mean_base_rate.get(i).ok_or_else(|| null("meanBaseRate"))?,
                    mean_base_rate_im: mean_base_rate_im
                        .get(i)
                        .ok_or_else(|| null("meanBaseRateIM"))?,
                    emission_rate: emission_rate.get(i).ok_or_else(|| null("emissionRate"))?,
                    emission_rate_im: emission_rate_im
                        .get(i)
                        .ok_or_else(|| null("emissionRateIM"))?,
                    mean_base_rate_ac_adj: mean_base_rate_ac_adj
                        .get(i)
                        .ok_or_else(|| null("meanBaseRateACAdj"))?,
                    mean_base_rate_im_ac_adj: mean_base_rate_im_ac_adj
                        .get(i)
                        .ok_or_else(|| null("meanBaseRateIMACAdj"))?,
                    emission_rate_ac_adj: emission_rate_ac_adj
                        .get(i)
                        .ok_or_else(|| null("emissionRateACAdj"))?,
                    emission_rate_im_ac_adj: emission_rate_im_ac_adj
                        .get(i)
                        .ok_or_else(|| null("emissionRateIMACAdj"))?,
                    op_mode_fraction: op_mode_fraction
                        .get(i)
                        .ok_or_else(|| null("opModeFraction"))?,
                    op_mode_fraction_rate: op_mode_fraction_rate
                        .get(i)
                        .ok_or_else(|| null("opModeFractionRate"))?,
                })
            })
            .collect()
    }
}

impl TableRow for ModelYearFuelFractionRow {
    fn table_name() -> &'static str {
        "ExtendedIdleEmissionRateFraction"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("modelYearID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("hourFractionAdjust".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
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
                    "hourFractionAdjust".into(),
                    rows.iter()
                        .map(|r| r.hour_fraction_adjust)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "ExtendedIdleEmissionRateFraction";
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
        let model_year_id = get_i32("modelYearID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        let hour_fraction_adjust = get_f64("hourFractionAdjust")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(ModelYearFuelFractionRow {
                    model_year_id: model_year_id.get(i).ok_or_else(|| null("modelYearID"))?,
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    hour_fraction_adjust: hour_fraction_adjust
                        .get(i)
                        .ok_or_else(|| null("hourFractionAdjust"))?,
                })
            })
            .collect()
    }
}

impl TableRow for ZoneMonthHourRow {
    fn table_name() -> &'static str {
        "ZoneMonthHour"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("monthID".into(), DataType::Int32),
            ("zoneID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
            ("temperature".into(), DataType::Float64),
            ("relHumidity".into(), DataType::Float64),
            ("heatIndex".into(), DataType::Float64),
            ("specificHumidity".into(), DataType::Float64),
            ("molWaterFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "zoneID".into(),
                    rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourID".into(),
                    rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "temperature".into(),
                    rows.iter().map(|r| r.temperature).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "relHumidity".into(),
                    rows.iter().map(|r| r.rel_humidity).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "heatIndex".into(),
                    rows.iter().map(|r| r.heat_index).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "specificHumidity".into(),
                    rows.iter()
                        .map(|r| r.specific_humidity)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "molWaterFraction".into(),
                    rows.iter()
                        .map(|r| r.mol_water_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "ZoneMonthHour";
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
        let month_id = get_i32("monthID")?;
        let zone_id = get_i32("zoneID")?;
        let hour_id = get_i32("hourID")?;
        let temperature = get_f64("temperature")?;
        let rel_humidity = get_f64("relHumidity")?;
        let heat_index = get_f64("heatIndex")?;
        let specific_humidity = get_f64("specificHumidity")?;
        let mol_water_fraction = get_f64("molWaterFraction")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(ZoneMonthHourRow {
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    zone_id: zone_id.get(i).ok_or_else(|| null("zoneID"))?,
                    hour_id: hour_id.get(i).ok_or_else(|| null("hourID"))?,
                    temperature: temperature.get(i).ok_or_else(|| null("temperature"))?,
                    rel_humidity: rel_humidity.get(i).ok_or_else(|| null("relHumidity"))?,
                    heat_index: heat_index.get(i).ok_or_else(|| null("heatIndex"))?,
                    specific_humidity: specific_humidity
                        .get(i)
                        .ok_or_else(|| null("specificHumidity"))?,
                    mol_water_fraction: mol_water_fraction
                        .get(i)
                        .ok_or_else(|| null("molWaterFraction"))?,
                })
            })
            .collect()
    }
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
            ("fuelMYGroupID".into(), DataType::Int32),
            ("IMModelYearGroupID".into(), DataType::Int32),
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
                Series::new(
                    "fuelMYGroupID".into(),
                    rows.iter()
                        .map(|r| r.fuel_my_group_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "IMModelYearGroupID".into(),
                    rows.iter()
                        .map(|r| r.im_model_year_group_id)
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
        let pol_process_id = get_i32("polProcessID")?;
        let model_year_id = get_i32("modelYearID")?;
        let model_year_group_id = get_i32("modelYearGroupID")?;
        let fuel_my_group_id = get_i32("fuelMYGroupID")?;
        let im_model_year_group_id = get_i32("IMModelYearGroupID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(PollutantProcessMappedModelYearRow {
                    pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                    model_year_id: model_year_id.get(i).ok_or_else(|| null("modelYearID"))?,
                    model_year_group_id: model_year_group_id
                        .get(i)
                        .ok_or_else(|| null("modelYearGroupID"))?,
                    fuel_my_group_id: fuel_my_group_id
                        .get(i)
                        .ok_or_else(|| null("fuelMYGroupID"))?,
                    im_model_year_group_id: im_model_year_group_id
                        .get(i)
                        .ok_or_else(|| null("IMModelYearGroupID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for StartTempAdjustmentRow {
    fn table_name() -> &'static str {
        "StartTempAdjustment"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelTypeID".into(), DataType::Int32),
            ("polProcessID".into(), DataType::Int32),
            ("modelYearGroupID".into(), DataType::Int32),
            ("opModeID".into(), DataType::Int32),
            ("tempAdjustTermA".into(), DataType::Float64),
            ("tempAdjustTermB".into(), DataType::Float64),
            ("tempAdjustTermC".into(), DataType::Float64),
            ("startTempEquationType".into(), DataType::String),
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
                    "modelYearGroupID".into(),
                    rows.iter()
                        .map(|r| r.model_year_group_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "opModeID".into(),
                    rows.iter().map(|r| r.op_mode_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "tempAdjustTermA".into(),
                    rows.iter().map(|r| r.term_a).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "tempAdjustTermB".into(),
                    rows.iter().map(|r| r.term_b).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "tempAdjustTermC".into(),
                    rows.iter().map(|r| r.term_c).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "startTempEquationType".into(),
                    rows.iter()
                        .map(|r| r.equation_type.as_str())
                        .collect::<Vec<&str>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "StartTempAdjustment";
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
        let fuel_type_id = get_i32("fuelTypeID")?;
        let pol_process_id = get_i32("polProcessID")?;
        let model_year_group_id = get_i32("modelYearGroupID")?;
        let op_mode_id = get_i32("opModeID")?;
        let term_a = get_f64("tempAdjustTermA")?;
        let term_b = get_f64("tempAdjustTermB")?;
        let term_c = get_f64("tempAdjustTermC")?;
        let equation_type = get_str("startTempEquationType")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(StartTempAdjustmentRow {
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                    model_year_group_id: model_year_group_id
                        .get(i)
                        .ok_or_else(|| null("modelYearGroupID"))?,
                    op_mode_id: op_mode_id.get(i).ok_or_else(|| null("opModeID"))?,
                    term_a: term_a.get(i).ok_or_else(|| null("tempAdjustTermA"))?,
                    term_b: term_b.get(i).ok_or_else(|| null("tempAdjustTermB"))?,
                    term_c: term_c.get(i).ok_or_else(|| null("tempAdjustTermC"))?,
                    equation_type: equation_type
                        .get(i)
                        .ok_or_else(|| null("startTempEquationType"))?
                        .to_string(),
                })
            })
            .collect()
    }
}

impl TableRow for CountyRow {
    fn table_name() -> &'static str {
        "County"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("countyID".into(), DataType::Int32),
            ("GPAFract".into(), DataType::Float64),
            ("barometricPressure".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "countyID".into(),
                    rows.iter().map(|r| r.county_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "GPAFract".into(),
                    rows.iter().map(|r| r.gpa_fract).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "barometricPressure".into(),
                    rows.iter()
                        .map(|r| r.barometric_pressure)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "County";
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
        let county_id = get_i32("countyID")?;
        let gpa_fract = get_f64("GPAFract")?;
        let barometric_pressure = get_f64("barometricPressure")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(CountyRow {
                    county_id: county_id.get(i).ok_or_else(|| null("countyID"))?,
                    gpa_fract: gpa_fract.get(i).ok_or_else(|| null("GPAFract"))?,
                    barometric_pressure: barometric_pressure
                        .get(i)
                        .ok_or_else(|| null("barometricPressure"))?,
                })
            })
            .collect()
    }
}

impl TableRow for GeneralFuelRatioRow {
    fn table_name() -> &'static str {
        "GeneralFuelRatio"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelFormulationID".into(), DataType::Int32),
            ("polProcessID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("minModelYearID".into(), DataType::Int32),
            ("maxModelYearID".into(), DataType::Int32),
            ("minAgeID".into(), DataType::Int32),
            ("maxAgeID".into(), DataType::Int32),
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
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
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
        let t = "GeneralFuelRatio";
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
        let fuel_formulation_id = get_i32("fuelFormulationID")?;
        let pol_process_id = get_i32("polProcessID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let min_model_year_id = get_i32("minModelYearID")?;
        let max_model_year_id = get_i32("maxModelYearID")?;
        let min_age_id = get_i32("minAgeID")?;
        let max_age_id = get_i32("maxAgeID")?;
        let fuel_effect_ratio = get_f64("fuelEffectRatio")?;
        let fuel_effect_ratio_gpa = get_f64("fuelEffectRatioGPA")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(GeneralFuelRatioRow {
                    fuel_formulation_id: fuel_formulation_id
                        .get(i)
                        .ok_or_else(|| null("fuelFormulationID"))?,
                    pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    min_model_year_id: min_model_year_id
                        .get(i)
                        .ok_or_else(|| null("minModelYearID"))?,
                    max_model_year_id: max_model_year_id
                        .get(i)
                        .ok_or_else(|| null("maxModelYearID"))?,
                    min_age_id: min_age_id.get(i).ok_or_else(|| null("minAgeID"))?,
                    max_age_id: max_age_id.get(i).ok_or_else(|| null("maxAgeID"))?,
                    fuel_effect_ratio: fuel_effect_ratio
                        .get(i)
                        .ok_or_else(|| null("fuelEffectRatio"))?,
                    fuel_effect_ratio_gpa: fuel_effect_ratio_gpa
                        .get(i)
                        .ok_or_else(|| null("fuelEffectRatioGPA"))?,
                })
            })
            .collect()
    }
}

impl TableRow for CriteriaRatioRow {
    fn table_name() -> &'static str {
        "criteriaRatio"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelFormulationID".into(), DataType::Int32),
            ("polProcessID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("ratio".into(), DataType::Float64),
            ("ratioGPA".into(), DataType::Float64),
            ("ratioNoSulfur".into(), DataType::Float64),
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
                    "polProcessID".into(),
                    rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>(),
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
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ratio".into(),
                    rows.iter().map(|r| r.ratio).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "ratioGPA".into(),
                    rows.iter().map(|r| r.ratio_gpa).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "ratioNoSulfur".into(),
                    rows.iter().map(|r| r.ratio_no_sulfur).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "criteriaRatio";
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
        let fuel_formulation_id = get_i32("fuelFormulationID")?;
        let pol_process_id = get_i32("polProcessID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let model_year_id = get_i32("modelYearID")?;
        let age_id = get_i32("ageID")?;
        let ratio = get_f64("ratio")?;
        let ratio_gpa = get_f64("ratioGPA")?;
        let ratio_no_sulfur = get_f64("ratioNoSulfur")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(CriteriaRatioRow {
                    fuel_formulation_id: fuel_formulation_id
                        .get(i)
                        .ok_or_else(|| null("fuelFormulationID"))?,
                    pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    model_year_id: model_year_id.get(i).ok_or_else(|| null("modelYearID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    ratio: ratio.get(i).ok_or_else(|| null("ratio"))?,
                    ratio_gpa: ratio_gpa.get(i).ok_or_else(|| null("ratioGPA"))?,
                    ratio_no_sulfur: ratio_no_sulfur
                        .get(i)
                        .ok_or_else(|| null("ratioNoSulfur"))?,
                })
            })
            .collect()
    }
}

impl TableRow for TemperatureAdjustmentRow {
    fn table_name() -> &'static str {
        "TemperatureAdjustment"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("regClassID".into(), DataType::Int32),
            ("minModelYearID".into(), DataType::Int32),
            ("maxModelYearID".into(), DataType::Int32),
            ("tempAdjustTermA".into(), DataType::Float64),
            ("tempAdjustTermB".into(), DataType::Float64),
            ("tempAdjustTermC".into(), DataType::Float64),
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
                    "tempAdjustTermA".into(),
                    rows.iter().map(|r| r.term_a).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "tempAdjustTermB".into(),
                    rows.iter().map(|r| r.term_b).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "tempAdjustTermC".into(),
                    rows.iter().map(|r| r.term_c).collect::<Vec<Option<f64>>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "TemperatureAdjustment";
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
        let pol_process_id = get_i32("polProcessID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        let reg_class_id = get_i32("regClassID")?;
        let min_model_year_id = get_i32("minModelYearID")?;
        let max_model_year_id = get_i32("maxModelYearID")?;
        let term_a = get_f64("tempAdjustTermA")?;
        let term_b = get_f64("tempAdjustTermB")?;
        let term_c = get_f64("tempAdjustTermC")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(TemperatureAdjustmentRow {
                    pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    reg_class_id: reg_class_id.get(i).ok_or_else(|| null("regClassID"))?,
                    min_model_year_id: min_model_year_id
                        .get(i)
                        .ok_or_else(|| null("minModelYearID"))?,
                    max_model_year_id: max_model_year_id
                        .get(i)
                        .ok_or_else(|| null("maxModelYearID"))?,
                    term_a: term_a.get(i).ok_or_else(|| null("tempAdjustTermA"))?,
                    term_b: term_b.get(i).ok_or_else(|| null("tempAdjustTermB"))?,
                    term_c: term_c.get(i), // nullable: SQL NULL coalesces to 0.0
                })
            })
            .collect()
    }
}

impl TableRow for NoxHumidityAdjustRow {
    fn table_name() -> &'static str {
        "NOxHumidityAdjust"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelTypeID".into(), DataType::Int32),
            ("humidityNOxEq".into(), DataType::String),
            ("humidityTermA".into(), DataType::Float64),
            ("humidityTermB".into(), DataType::Float64),
            ("humidityLowBound".into(), DataType::Float64),
            ("humidityUpBound".into(), DataType::Float64),
            ("humidityUnits".into(), DataType::String),
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
                    "humidityNOxEq".into(),
                    rows.iter()
                        .map(|r| r.humidity_nox_eq.as_str())
                        .collect::<Vec<&str>>(),
                )
                .into(),
                Series::new(
                    "humidityTermA".into(),
                    rows.iter().map(|r| r.humidity_term_a).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "humidityTermB".into(),
                    rows.iter()
                        .map(|r| r.humidity_term_b)
                        .collect::<Vec<Option<f64>>>(),
                )
                .into(),
                Series::new(
                    "humidityLowBound".into(),
                    rows.iter()
                        .map(|r| r.humidity_low_bound)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "humidityUpBound".into(),
                    rows.iter()
                        .map(|r| r.humidity_up_bound)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "humidityUnits".into(),
                    rows.iter()
                        .map(|r| r.humidity_units.as_str())
                        .collect::<Vec<&str>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "NOxHumidityAdjust";
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
        let fuel_type_id = get_i32("fuelTypeID")?;
        let humidity_nox_eq = get_str("humidityNOxEq")?;
        let humidity_term_a = get_f64("humidityTermA")?;
        let humidity_term_b = get_f64("humidityTermB")?;
        let humidity_low_bound = get_f64("humidityLowBound")?;
        let humidity_up_bound = get_f64("humidityUpBound")?;
        let humidity_units = get_str("humidityUnits")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(NoxHumidityAdjustRow {
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    humidity_nox_eq: humidity_nox_eq
                        .get(i)
                        .ok_or_else(|| null("humidityNOxEq"))?
                        .to_string(),
                    humidity_term_a: humidity_term_a
                        .get(i)
                        .ok_or_else(|| null("humidityTermA"))?,
                    humidity_term_b: humidity_term_b.get(i), // nullable: None coalesces to 0.0
                    humidity_low_bound: humidity_low_bound
                        .get(i)
                        .ok_or_else(|| null("humidityLowBound"))?,
                    humidity_up_bound: humidity_up_bound
                        .get(i)
                        .ok_or_else(|| null("humidityUpBound"))?,
                    humidity_units: humidity_units
                        .get(i)
                        .ok_or_else(|| null("humidityUnits"))?
                        .to_string(),
                })
            })
            .collect()
    }
}

impl TableRow for ZoneAcFactorRow {
    fn table_name() -> &'static str {
        "zoneACFactor"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hourID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("ACFactor".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "hourID".into(),
                    rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>(),
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
                    "ACFactor".into(),
                    rows.iter().map(|r| r.ac_factor).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "zoneACFactor";
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
        let hour_id = get_i32("hourID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let model_year_id = get_i32("modelYearID")?;
        let ac_factor = get_f64("ACFactor")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(ZoneAcFactorRow {
                    hour_id: hour_id.get(i).ok_or_else(|| null("hourID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    model_year_id: model_year_id.get(i).ok_or_else(|| null("modelYearID"))?,
                    ac_factor: ac_factor.get(i).ok_or_else(|| null("ACFactor"))?,
                })
            })
            .collect()
    }
}

impl TableRow for ImFactorRow {
    fn table_name() -> &'static str {
        "IMFactor"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("inspectFreq".into(), DataType::Int32),
            ("testStandardsID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("IMModelYearGroupID".into(), DataType::Int32),
            ("ageGroupID".into(), DataType::Int32),
            ("IMFactor".into(), DataType::Float64),
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
                    "inspectFreq".into(),
                    rows.iter().map(|r| r.inspect_freq).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "testStandardsID".into(),
                    rows.iter()
                        .map(|r| r.test_standards_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "IMModelYearGroupID".into(),
                    rows.iter()
                        .map(|r| r.im_model_year_group_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageGroupID".into(),
                    rows.iter().map(|r| r.age_group_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "IMFactor".into(),
                    rows.iter().map(|r| r.im_factor).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "IMFactor";
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
        let pol_process_id = get_i32("polProcessID")?;
        let inspect_freq = get_i32("inspectFreq")?;
        let test_standards_id = get_i32("testStandardsID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        let im_model_year_group_id = get_i32("IMModelYearGroupID")?;
        let age_group_id = get_i32("ageGroupID")?;
        let im_factor = get_f64("IMFactor")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(ImFactorRow {
                    pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                    inspect_freq: inspect_freq.get(i).ok_or_else(|| null("inspectFreq"))?,
                    test_standards_id: test_standards_id
                        .get(i)
                        .ok_or_else(|| null("testStandardsID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    im_model_year_group_id: im_model_year_group_id
                        .get(i)
                        .ok_or_else(|| null("IMModelYearGroupID"))?,
                    age_group_id: age_group_id.get(i).ok_or_else(|| null("ageGroupID"))?,
                    im_factor: im_factor.get(i).ok_or_else(|| null("IMFactor"))?,
                })
            })
            .collect()
    }
}

impl TableRow for ImCoverageRow {
    fn table_name() -> &'static str {
        "IMCoverage"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("begModelYearID".into(), DataType::Int32),
            ("endModelYearID".into(), DataType::Int32),
            ("inspectFreq".into(), DataType::Int32),
            ("testStandardsID".into(), DataType::Int32),
            ("complianceFactor".into(), DataType::Float64),
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
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "begModelYearID".into(),
                    rows.iter()
                        .map(|r| r.beg_model_year_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "endModelYearID".into(),
                    rows.iter()
                        .map(|r| r.end_model_year_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "inspectFreq".into(),
                    rows.iter().map(|r| r.inspect_freq).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "testStandardsID".into(),
                    rows.iter()
                        .map(|r| r.test_standards_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "complianceFactor".into(),
                    rows.iter()
                        .map(|r| r.compliance_factor)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "IMCoverage";
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
        let pol_process_id = get_i32("polProcessID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        let beg_model_year_id = get_i32("begModelYearID")?;
        let end_model_year_id = get_i32("endModelYearID")?;
        let inspect_freq = get_i32("inspectFreq")?;
        let test_standards_id = get_i32("testStandardsID")?;
        let compliance_factor = get_f64("complianceFactor")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(ImCoverageRow {
                    pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    beg_model_year_id: beg_model_year_id
                        .get(i)
                        .ok_or_else(|| null("begModelYearID"))?,
                    end_model_year_id: end_model_year_id
                        .get(i)
                        .ok_or_else(|| null("endModelYearID"))?,
                    inspect_freq: inspect_freq.get(i).ok_or_else(|| null("inspectFreq"))?,
                    test_standards_id: test_standards_id
                        .get(i)
                        .ok_or_else(|| null("testStandardsID"))?,
                    compliance_factor: compliance_factor
                        .get(i)
                        .ok_or_else(|| null("complianceFactor"))?,
                })
            })
            .collect()
    }
}

impl TableRow for EmissionRateAdjustmentRow {
    fn table_name() -> &'static str {
        "EmissionRateAdjustment"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("regClassID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("beginModelYearID".into(), DataType::Int32),
            ("endModelYearID".into(), DataType::Int32),
            ("emissionRateAdjustment".into(), DataType::Float64),
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
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "regClassID".into(),
                    rows.iter().map(|r| r.reg_class_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "beginModelYearID".into(),
                    rows.iter()
                        .map(|r| r.begin_model_year_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "endModelYearID".into(),
                    rows.iter()
                        .map(|r| r.end_model_year_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "emissionRateAdjustment".into(),
                    rows.iter()
                        .map(|r| r.emission_rate_adjustment)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "EmissionRateAdjustment";
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
        let pol_process_id = get_i32("polProcessID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let reg_class_id = get_i32("regClassID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        let begin_model_year_id = get_i32("beginModelYearID")?;
        let end_model_year_id = get_i32("endModelYearID")?;
        let emission_rate_adjustment = get_f64("emissionRateAdjustment")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(EmissionRateAdjustmentRow {
                    pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    reg_class_id: reg_class_id.get(i).ok_or_else(|| null("regClassID"))?,
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    begin_model_year_id: begin_model_year_id
                        .get(i)
                        .ok_or_else(|| null("beginModelYearID"))?,
                    end_model_year_id: end_model_year_id
                        .get(i)
                        .ok_or_else(|| null("endModelYearID"))?,
                    emission_rate_adjustment: emission_rate_adjustment
                        .get(i)
                        .ok_or_else(|| null("emissionRateAdjustment"))?,
                })
            })
            .collect()
    }
}

impl TableRow for EvEfficiencyRow {
    fn table_name() -> &'static str {
        "EVEfficiency"
    }
    fn polars_schema() -> Schema {
        // DB schema: polProcessID, sourceTypeID, regClassID, ageGroupID,
        // beginModelYearID, endModelYearID, batteryEfficiency, chargingEfficiency.
        // ageGroupID maps to fuel_type_id and beginModelYearID to model_year_id
        // as a placeholder; the ev_efficiency flag defaults to false so the
        // look-up is never applied in standard runs.
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("regClassID".into(), DataType::Int32),
            ("ageGroupID".into(), DataType::Int32),
            ("beginModelYearID".into(), DataType::Int32),
            ("batteryEfficiency".into(), DataType::Float64),
            ("chargingEfficiency".into(), DataType::Float64),
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
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "regClassID".into(),
                    rows.iter().map(|r| r.reg_class_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageGroupID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "beginModelYearID".into(),
                    rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "batteryEfficiency".into(),
                    rows.iter()
                        .map(|r| r.battery_efficiency)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "chargingEfficiency".into(),
                    rows.iter()
                        .map(|r| r.charging_efficiency)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "EVEfficiency";
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
        let pol_process_id = get_i32("polProcessID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let reg_class_id = get_i32("regClassID")?;
        let age_group_id = get_i32("ageGroupID")?; // stored as fuel_type_id placeholder
        let begin_model_year_id = get_i32("beginModelYearID")?; // stored as model_year_id
        let battery_efficiency = get_f64("batteryEfficiency")?;
        let charging_efficiency = get_f64("chargingEfficiency")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(EvEfficiencyRow {
                    pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    reg_class_id: reg_class_id.get(i).ok_or_else(|| null("regClassID"))?,
                    fuel_type_id: age_group_id.get(i).ok_or_else(|| null("ageGroupID"))?,
                    model_year_id: begin_model_year_id
                        .get(i)
                        .ok_or_else(|| null("beginModelYearID"))?,
                    battery_efficiency: battery_efficiency
                        .get(i)
                        .ok_or_else(|| null("batteryEfficiency"))?,
                    charging_efficiency: charging_efficiency
                        .get(i)
                        .ok_or_else(|| null("chargingEfficiency"))?,
                })
            })
            .collect()
    }
}

impl TableRow for UniversalActivityRow {
    fn table_name() -> &'static str {
        "universalActivity"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("activity".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "hourDayID".into(),
                    rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "modelYearID".into(),
                    rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "activity".into(),
                    rows.iter().map(|r| r.activity).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "universalActivity";
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
        let hour_day_id = get_i32("hourDayID")?;
        let model_year_id = get_i32("modelYearID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let activity = get_f64("activity")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(UniversalActivityRow {
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    model_year_id: model_year_id.get(i).ok_or_else(|| null("modelYearID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    activity: activity.get(i).ok_or_else(|| null("activity"))?,
                })
            })
            .collect()
    }
}

impl TableRow for SmfrSbdSummaryRow {
    fn table_name() -> &'static str {
        "smfrSBDSummary"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("regClassID".into(), DataType::Int32),
            ("SBDTotal".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
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
                    "SBDTotal".into(),
                    rows.iter().map(|r| r.sbd_total).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "smfrSBDSummary";
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
        let source_type_id = get_i32("sourceTypeID")?;
        let model_year_id = get_i32("modelYearID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        let reg_class_id = get_i32("regClassID")?;
        let sbd_total = get_f64("SBDTotal")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SmfrSbdSummaryRow {
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    model_year_id: model_year_id.get(i).ok_or_else(|| null("modelYearID"))?,
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    reg_class_id: reg_class_id.get(i).ok_or_else(|| null("regClassID"))?,
                    sbd_total: sbd_total.get(i).ok_or_else(|| null("SBDTotal"))?,
                })
            })
            .collect()
    }
}

impl TableRow for AgeCategoryRow {
    fn table_name() -> &'static str {
        "AgeCategory"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("ageID".into(), DataType::Int32),
            ("ageGroupID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageGroupID".into(),
                    rows.iter().map(|r| r.age_group_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "AgeCategory";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let age_id = get_i32("ageID")?;
        let age_group_id = get_i32("ageGroupID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(AgeCategoryRow {
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    age_group_id: age_group_id.get(i).ok_or_else(|| null("ageGroupID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for FuelTypeRow {
    fn table_name() -> &'static str {
        "FuelType"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([("fuelTypeID".into(), DataType::Int32)])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![Series::new(
                "fuelTypeID".into(),
                rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
            )
            .into()],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "FuelType";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let fuel_type_id = get_i32("fuelTypeID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(FuelTypeRow {
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for FuelFormulationRow {
    fn table_name() -> &'static str {
        "FuelFormulation"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelFormulationID".into(), DataType::Int32),
            ("fuelSubTypeID".into(), DataType::Int32),
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
                    "fuelSubTypeID".into(),
                    rows.iter()
                        .map(|r| r.fuel_sub_type_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "FuelFormulation";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let fuel_formulation_id = get_i32("fuelFormulationID")?;
        let fuel_sub_type_id = get_i32("fuelSubTypeID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(FuelFormulationRow {
                    fuel_formulation_id: fuel_formulation_id
                        .get(i)
                        .ok_or_else(|| null("fuelFormulationID"))?,
                    fuel_sub_type_id: fuel_sub_type_id
                        .get(i)
                        .ok_or_else(|| null("fuelSubTypeID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for FuelSupplyRow {
    fn table_name() -> &'static str {
        "FuelSupply"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("countyID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("fuelSubTypeID".into(), DataType::Int32),
            ("fuelFormulationID".into(), DataType::Int32),
            ("marketShare".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "countyID".into(),
                    rows.iter().map(|r| r.county_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelSubTypeID".into(),
                    rows.iter()
                        .map(|r| r.fuel_sub_type_id)
                        .collect::<Vec<i32>>(),
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
                    "marketShare".into(),
                    rows.iter().map(|r| r.market_share).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "FuelSupply";
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
        let county_id = get_i32("countyID")?;
        let year_id = get_i32("yearID")?;
        let month_id = get_i32("monthID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        let fuel_sub_type_id = get_i32("fuelSubTypeID")?;
        let fuel_formulation_id = get_i32("fuelFormulationID")?;
        let market_share = get_f64("marketShare")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(FuelSupplyRow {
                    county_id: county_id.get(i).ok_or_else(|| null("countyID"))?,
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    fuel_sub_type_id: fuel_sub_type_id
                        .get(i)
                        .ok_or_else(|| null("fuelSubTypeID"))?,
                    fuel_formulation_id: fuel_formulation_id
                        .get(i)
                        .ok_or_else(|| null("fuelFormulationID"))?,
                    market_share: market_share.get(i).ok_or_else(|| null("marketShare"))?,
                })
            })
            .collect()
    }
}

/// Key for the [`PreparedTables::fuel_supply`] lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct FuelSupplyKey {
    /// County id.
    pub county_id: i32,
    /// Calendar year id.
    pub year_id: i32,
    /// Month id.
    pub month_id: i32,
    /// Fuel type id.
    pub fuel_type_id: i32,
}

/// All execution-database tables the Base Rate Calculator reads.
///
/// The data-plane contract: each field mirrors one table the Go worker
/// loaded. Tables are held as plain row vectors; [`PreparedTables::from_inputs`]
/// applies the keying and joins. `base_rate` / `base_rate_by_age` are the
/// Base Rate Generator's output — the rows this calculator adjusts.
#[derive(Debug, Clone, Default)]
pub struct BaseRateCalculatorInputs {
    /// `BaseRateByAge` rows (age-based pass).
    pub base_rate_by_age: Vec<BaseRateRow>,
    /// `BaseRate` rows (non-age-based pass).
    pub base_rate: Vec<BaseRateRow>,
    /// `ExtendedIdleEmissionRateFraction` rows.
    pub extended_idle_emission_rate_fraction: Vec<ModelYearFuelFractionRow>,
    /// `apuEmissionRateFraction` rows.
    pub apu_emission_rate_fraction: Vec<ModelYearFuelFractionRow>,
    /// `ShorepowerEmissionRateFraction` rows.
    pub shorepower_emission_rate_fraction: Vec<ModelYearFuelFractionRow>,
    /// `ZoneMonthHour` meteorology rows.
    pub zone_month_hour: Vec<ZoneMonthHourRow>,
    /// `PollutantProcessMappedModelYear` rows.
    pub pollutant_process_mapped_model_year: Vec<PollutantProcessMappedModelYearRow>,
    /// `StartTempAdjustment` rows.
    pub start_temp_adjustment: Vec<StartTempAdjustmentRow>,
    /// `County` rows.
    pub county: Vec<CountyRow>,
    /// `GeneralFuelRatio` rows.
    pub general_fuel_ratio: Vec<GeneralFuelRatioRow>,
    /// `CriteriaRatio` rows.
    pub criteria_ratio: Vec<CriteriaRatioRow>,
    /// `AltCriteriaRatio` rows.
    pub alt_criteria_ratio: Vec<CriteriaRatioRow>,
    /// `TemperatureAdjustment` rows.
    pub temperature_adjustment: Vec<TemperatureAdjustmentRow>,
    /// `NOxHumidityAdjust` rows.
    pub nox_humidity_adjust: Vec<NoxHumidityAdjustRow>,
    /// `zoneACFactor` rows.
    pub zone_ac_factor: Vec<ZoneAcFactorRow>,
    /// `IMFactor` rows.
    pub im_factor: Vec<ImFactorRow>,
    /// `IMCoverage` rows.
    pub im_coverage: Vec<ImCoverageRow>,
    /// `EmissionRateAdjustmentWorker` rows.
    pub emission_rate_adjustment: Vec<EmissionRateAdjustmentRow>,
    /// `evefficiencyWorker` rows.
    pub ev_efficiency: Vec<EvEfficiencyRow>,
    /// `universalActivity` rows.
    pub universal_activity: Vec<UniversalActivityRow>,
    /// `smfrSBDSummary` rows.
    pub smfr_sbd_summary: Vec<SmfrSbdSummaryRow>,
    /// `AgeCategory` rows (the `mwo.AgeGroups` source).
    pub age_category: Vec<AgeCategoryRow>,
    /// Valid fuel-type ids (the Go `mwo.FuelTypes` keys).
    pub fuel_types: Vec<i32>,
    /// `FuelFormulation` rows.
    pub fuel_formulations: Vec<FuelFormulationRow>,
    /// `FuelSupply` rows.
    pub fuel_supply: Vec<FuelSupplyRow>,
}

/// The Go package-level lookup maps after `StartSetup`.
///
/// Built by [`PreparedTables::from_inputs`]. The Go used hash maps; the port
/// uses [`BTreeMap`] / [`BTreeSet`] so accumulation order is deterministic
/// (see the module docs of [`super::model`]).
#[derive(Debug, Clone, Default)]
pub struct PreparedTables {
    /// `ExtendedIdleEmissionRateFraction` — opMode-200 usage fraction.
    pub extended_idle_emission_rate_fraction: BTreeMap<ModelYearFuelKey, f64>,
    /// `apuEmissionRateFraction` — opMode-201 usage fraction.
    pub apu_emission_rate_fraction: BTreeMap<ModelYearFuelKey, f64>,
    /// `ShorepowerEmissionRateFraction` — opMode-203 usage fraction.
    pub shorepower_emission_rate_fraction: BTreeMap<ModelYearFuelKey, f64>,
    /// `ZoneMonthHour` meteorology.
    pub zone_month_hour: BTreeMap<ZoneMonthHourKey, ZoneMonthHourDetail>,
    /// `PollutantProcessMappedModelYear`.
    pub pollutant_process_mapped_model_year:
        BTreeMap<PollutantProcessMappedModelYearKey, PollutantProcessMappedModelYearDetail>,
    /// `StartTempAdjustment`.
    pub start_temp_adjustment: BTreeMap<StartTempAdjustmentKey, StartTempAdjustmentDetail>,
    /// `County`, keyed by county id.
    pub county: BTreeMap<i32, CountyDetail>,
    /// `GeneralFuelRatio`.
    pub general_fuel_ratio: BTreeMap<super::model::GeneralFuelRatioKey, GeneralFuelRatioDetail>,
    /// `CriteriaRatio`.
    pub criteria_ratio: BTreeMap<CriteriaRatioKey, CriteriaRatioDetail>,
    /// `AltCriteriaRatio`.
    pub alt_criteria_ratio: BTreeMap<CriteriaRatioKey, CriteriaRatioDetail>,
    /// `TemperatureAdjustment` (model-year-expanded).
    pub temperature_adjustment: BTreeMap<TemperatureAdjustmentKey, TemperatureAdjustmentDetail>,
    /// `NOxHumidityAdjust`, keyed by fuel type id.
    pub nox_humidity_adjust: BTreeMap<i32, NoxHumidityAdjustDetail>,
    /// `zoneACFactor`.
    pub zone_ac_factor: BTreeMap<ZoneAcFactorKey, f64>,
    /// `IMFactor`.
    pub im_factor: BTreeMap<ImFactorKey, f64>,
    /// `IMCoverage` (built by the model-year-expanding join).
    pub im_coverage: BTreeMap<ImCoverageKey, f64>,
    /// `EmissionRateAdjustment`.
    pub emission_rate_adjustment: BTreeMap<PolProcSourceRegFuelMyKey, f64>,
    /// `EVEfficiency`.
    pub ev_efficiency: BTreeMap<PolProcSourceRegFuelMyKey, EvEfficiencyDetail>,
    /// `universalActivity`.
    pub universal_activity: BTreeMap<UniversalActivityKey, f64>,
    /// All `hourDayID` values seen in `universalActivity`.
    pub universal_activity_hour_day_ids: BTreeSet<i32>,
    /// `AgeGroups` — age id → age group id.
    pub age_groups: BTreeMap<i32, i32>,
    /// Valid fuel-type ids.
    pub fuel_types: BTreeSet<i32>,
    /// `FuelFormulation` — formulation id → fuel subtype id.
    pub fuel_formulations: BTreeMap<i32, i32>,
    /// `FuelSupply`, keyed by county / year / month / fuel type.
    pub fuel_supply: BTreeMap<FuelSupplyKey, Vec<FuelSupplyDetail>>,
}

impl PreparedTables {
    /// Build the prepared tables from raw [`BaseRateCalculatorInputs`].
    ///
    /// Ports the Go `StartSetup`. `constants` supplies `Constants.YearID`,
    /// which the `IMCoverage` join needs to compute an age from each model
    /// year. Maps are built in dependency order: `PollutantProcessMappedModelYear`,
    /// `IMFactor` and `AgeGroups` before `IMCoverage`.
    #[must_use]
    #[allow(clippy::too_many_lines)] // One straight-line port of the ~20-table Go `StartSetup`.
    pub fn from_inputs(inputs: BaseRateCalculatorInputs, constants: &RunConstants) -> Self {
        let mut prepared = PreparedTables::default();

        // The three hourly-fraction tables share a key shape.
        for row in inputs.extended_idle_emission_rate_fraction {
            prepared.extended_idle_emission_rate_fraction.insert(
                ModelYearFuelKey {
                    model_year_id: row.model_year_id,
                    fuel_type_id: row.fuel_type_id,
                },
                row.hour_fraction_adjust,
            );
        }
        for row in inputs.apu_emission_rate_fraction {
            prepared.apu_emission_rate_fraction.insert(
                ModelYearFuelKey {
                    model_year_id: row.model_year_id,
                    fuel_type_id: row.fuel_type_id,
                },
                row.hour_fraction_adjust,
            );
        }
        for row in inputs.shorepower_emission_rate_fraction {
            prepared.shorepower_emission_rate_fraction.insert(
                ModelYearFuelKey {
                    model_year_id: row.model_year_id,
                    fuel_type_id: row.fuel_type_id,
                },
                row.hour_fraction_adjust,
            );
        }

        for row in inputs.zone_month_hour {
            prepared.zone_month_hour.insert(
                ZoneMonthHourKey {
                    month_id: row.month_id,
                    zone_id: row.zone_id,
                    hour_id: row.hour_id,
                },
                ZoneMonthHourDetail {
                    temperature: row.temperature,
                    rel_humidity: row.rel_humidity,
                    heat_index: row.heat_index,
                    specific_humidity: row.specific_humidity,
                    mol_water_fraction: row.mol_water_fraction,
                },
            );
        }

        for row in inputs.pollutant_process_mapped_model_year {
            prepared.pollutant_process_mapped_model_year.insert(
                PollutantProcessMappedModelYearKey {
                    pol_process_id: row.pol_process_id,
                    model_year_id: row.model_year_id,
                },
                PollutantProcessMappedModelYearDetail {
                    model_year_group_id: row.model_year_group_id,
                    fuel_my_group_id: row.fuel_my_group_id,
                    im_model_year_group_id: row.im_model_year_group_id,
                },
            );
        }

        for row in inputs.start_temp_adjustment {
            prepared.start_temp_adjustment.insert(
                StartTempAdjustmentKey {
                    fuel_type_id: row.fuel_type_id,
                    pol_process_id: row.pol_process_id,
                    model_year_group_id: row.model_year_group_id,
                    op_mode_id: row.op_mode_id,
                },
                StartTempAdjustmentDetail {
                    term_a: row.term_a,
                    term_b: row.term_b,
                    term_c: row.term_c,
                    is_log: row.equation_type == "LOG",
                    is_poly: row.equation_type == "POLY",
                },
            );
        }

        for row in inputs.county {
            prepared.county.insert(
                row.county_id,
                CountyDetail {
                    gpa_fract: row.gpa_fract,
                    barometric_pressure: row.barometric_pressure,
                },
            );
        }

        // GeneralFuelRatio — fidelity note: the Go declares `details` as a
        // slice but each file row overwrites the map entry with a fresh
        // single-element detail (`GeneralFuelRatio[k] = v`), so the last row
        // for a key wins and `details` always holds exactly one range. The
        // port reproduces that; Task 44 decides whether it must accumulate.
        for row in inputs.general_fuel_ratio {
            prepared.general_fuel_ratio.insert(
                super::model::GeneralFuelRatioKey {
                    fuel_formulation_id: row.fuel_formulation_id,
                    pol_process_id: row.pol_process_id,
                    source_type_id: row.source_type_id,
                },
                GeneralFuelRatioDetail {
                    details: vec![GeneralFuelRatioInnerDetail {
                        min_model_year_id: row.min_model_year_id,
                        max_model_year_id: row.max_model_year_id,
                        min_age_id: row.min_age_id,
                        max_age_id: row.max_age_id,
                        fuel_effect_ratio: row.fuel_effect_ratio,
                        fuel_effect_ratio_gpa: row.fuel_effect_ratio_gpa,
                    }],
                },
            );
        }

        for row in inputs.criteria_ratio {
            prepared.criteria_ratio.insert(
                CriteriaRatioKey {
                    fuel_formulation_id: row.fuel_formulation_id,
                    pol_process_id: row.pol_process_id,
                    source_type_id: row.source_type_id,
                    model_year_id: row.model_year_id,
                    age_id: row.age_id,
                },
                CriteriaRatioDetail {
                    ratio: row.ratio,
                    ratio_gpa: row.ratio_gpa,
                    ratio_no_sulfur: row.ratio_no_sulfur,
                },
            );
        }
        for row in inputs.alt_criteria_ratio {
            prepared.alt_criteria_ratio.insert(
                CriteriaRatioKey {
                    fuel_formulation_id: row.fuel_formulation_id,
                    pol_process_id: row.pol_process_id,
                    source_type_id: row.source_type_id,
                    model_year_id: row.model_year_id,
                    age_id: row.age_id,
                },
                CriteriaRatioDetail {
                    ratio: row.ratio,
                    ratio_gpa: row.ratio_gpa,
                    ratio_no_sulfur: row.ratio_no_sulfur,
                },
            );
        }

        // TemperatureAdjustment — one file row expands across its (clamped)
        // model-year range, one map entry per year.
        for row in inputs.temperature_adjustment {
            let min_my = row.min_model_year_id.max(1950);
            let max_my = row.max_model_year_id.min(2060);
            for model_year_id in min_my..=max_my {
                prepared.temperature_adjustment.insert(
                    TemperatureAdjustmentKey {
                        pol_process_id: row.pol_process_id,
                        fuel_type_id: row.fuel_type_id,
                        reg_class_id: row.reg_class_id,
                        model_year_id,
                    },
                    TemperatureAdjustmentDetail {
                        term_a: row.term_a,
                        term_b: row.term_b,
                        term_c: row.term_c.unwrap_or(0.0),
                    },
                );
            }
        }

        for row in inputs.nox_humidity_adjust {
            prepared.nox_humidity_adjust.insert(
                row.fuel_type_id,
                NoxHumidityAdjustDetail {
                    humidity_nox_eq: row.humidity_nox_eq,
                    humidity_term_a: row.humidity_term_a,
                    humidity_term_b: row.humidity_term_b.unwrap_or(0.0),
                    humidity_low_bound: row.humidity_low_bound,
                    humidity_up_bound: row.humidity_up_bound,
                    humidity_units: row.humidity_units,
                },
            );
        }

        for row in inputs.zone_ac_factor {
            prepared.zone_ac_factor.insert(
                ZoneAcFactorKey {
                    hour_id: row.hour_id,
                    source_type_id: row.source_type_id,
                    model_year_id: row.model_year_id,
                },
                row.ac_factor,
            );
        }

        for row in inputs.im_factor {
            prepared.im_factor.insert(
                ImFactorKey {
                    pol_process_id: row.pol_process_id,
                    inspect_freq: row.inspect_freq,
                    test_standards_id: row.test_standards_id,
                    source_type_id: row.source_type_id,
                    fuel_type_id: row.fuel_type_id,
                    im_model_year_group_id: row.im_model_year_group_id,
                    age_group_id: row.age_group_id,
                },
                row.im_factor,
            );
        }

        for row in inputs.age_category {
            prepared.age_groups.insert(row.age_id, row.age_group_id);
        }

        // IMCoverage — depends on the three maps above. For each program row
        // and each model year it covers, join through
        // PollutantProcessMappedModelYear → AgeGroups → IMFactor, then
        // accumulate `IMFactor * complianceFactor` into the coverage entry.
        for row in inputs.im_coverage {
            let beg_my = row.beg_model_year_id.max(1950);
            let end_my = row.end_model_year_id.min(2060);
            let compliance_factor = 0.01 * row.compliance_factor;
            for model_year_id in beg_my..=end_my {
                let Some(ppa) = prepared.pollutant_process_mapped_model_year.get(
                    &PollutantProcessMappedModelYearKey {
                        pol_process_id: row.pol_process_id,
                        model_year_id,
                    },
                ) else {
                    continue;
                };
                let age_group_id = prepared
                    .age_groups
                    .get(&(constants.year_id - model_year_id))
                    .copied()
                    .unwrap_or(0);
                let Some(&imf) = prepared.im_factor.get(&ImFactorKey {
                    pol_process_id: row.pol_process_id,
                    inspect_freq: row.inspect_freq,
                    test_standards_id: row.test_standards_id,
                    source_type_id: row.source_type_id,
                    fuel_type_id: row.fuel_type_id,
                    im_model_year_group_id: ppa.im_model_year_group_id,
                    age_group_id,
                }) else {
                    continue;
                };
                let imk = ImCoverageKey {
                    pol_process_id: row.pol_process_id,
                    model_year_id,
                    source_type_id: row.source_type_id,
                    fuel_type_id: row.fuel_type_id,
                };
                let entry = prepared.im_coverage.entry(imk).or_insert(0.0);
                *entry += imf * compliance_factor;
            }
        }

        for row in inputs.emission_rate_adjustment {
            for model_year_id in row.begin_model_year_id..=row.end_model_year_id {
                prepared.emission_rate_adjustment.insert(
                    PolProcSourceRegFuelMyKey {
                        pol_process_id: row.pol_process_id,
                        source_type_id: row.source_type_id,
                        reg_class_id: row.reg_class_id,
                        fuel_type_id: row.fuel_type_id,
                        model_year_id,
                    },
                    row.emission_rate_adjustment,
                );
            }
        }

        for row in inputs.ev_efficiency {
            prepared.ev_efficiency.insert(
                PolProcSourceRegFuelMyKey {
                    pol_process_id: row.pol_process_id,
                    source_type_id: row.source_type_id,
                    reg_class_id: row.reg_class_id,
                    fuel_type_id: row.fuel_type_id,
                    model_year_id: row.model_year_id,
                },
                EvEfficiencyDetail {
                    battery_efficiency: row.battery_efficiency,
                    charging_efficiency: row.charging_efficiency,
                },
            );
        }

        for row in inputs.universal_activity {
            prepared.universal_activity.insert(
                UniversalActivityKey {
                    hour_day_id: row.hour_day_id,
                    model_year_id: row.model_year_id,
                    source_type_id: row.source_type_id,
                },
                row.activity,
            );
            prepared
                .universal_activity_hour_day_ids
                .insert(row.hour_day_id);
        }

        prepared.fuel_types = inputs.fuel_types.into_iter().collect();

        for row in inputs.fuel_formulations {
            prepared
                .fuel_formulations
                .insert(row.fuel_formulation_id, row.fuel_sub_type_id);
        }

        for row in inputs.fuel_supply {
            prepared
                .fuel_supply
                .entry(FuelSupplyKey {
                    county_id: row.county_id,
                    year_id: row.year_id,
                    month_id: row.month_id,
                    fuel_type_id: row.fuel_type_id,
                })
                .or_default()
                .push(FuelSupplyDetail {
                    fuel_sub_type_id: row.fuel_sub_type_id,
                    fuel_formulation_id: row.fuel_formulation_id,
                    market_share: row.market_share,
                });
        }

        prepared
    }
}

/// The `activityWeight` accumulator — the mutable map `calculate_activity_weight`
/// fills. Held as its own alias so the aggregate step's signature reads
/// clearly.
pub type ActivityWeights = BTreeMap<ActivityWeightKey, super::model::ActivityWeightDetail>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temperature_adjustment_expands_and_clamps_model_year_range() {
        let inputs = BaseRateCalculatorInputs {
            temperature_adjustment: vec![TemperatureAdjustmentRow {
                pol_process_id: 101,
                fuel_type_id: 2,
                reg_class_id: 0,
                min_model_year_id: 1940, // clamps up to 1950
                max_model_year_id: 1953,
                term_a: 1.0,
                term_b: 2.0,
                term_c: Some(3.0),
            }],
            ..BaseRateCalculatorInputs::default()
        };
        let prepared = PreparedTables::from_inputs(inputs, &RunConstants::default());
        // 1950..=1953 -> four entries; 1940..1949 clamped away.
        assert_eq!(prepared.temperature_adjustment.len(), 4);
        assert!(prepared
            .temperature_adjustment
            .contains_key(&TemperatureAdjustmentKey {
                pol_process_id: 101,
                fuel_type_id: 2,
                reg_class_id: 0,
                model_year_id: 1950,
            }));
        assert!(!prepared
            .temperature_adjustment
            .contains_key(&TemperatureAdjustmentKey {
                pol_process_id: 101,
                fuel_type_id: 2,
                reg_class_id: 0,
                model_year_id: 1949,
            }));
    }

    #[test]
    fn im_coverage_join_accumulates_factor_times_compliance() {
        // One IM program covering model years 2018..=2019, joined through
        // PPMMY (-> IMModelYearGroupID 7) and AgeGroups, summing IMFactor *
        // 0.01 * complianceFactor.
        let constants = RunConstants {
            year_id: 2020,
            ..RunConstants::default()
        };
        let inputs = BaseRateCalculatorInputs {
            pollutant_process_mapped_model_year: vec![
                PollutantProcessMappedModelYearRow {
                    pol_process_id: 301,
                    model_year_id: 2018,
                    model_year_group_id: 0,
                    fuel_my_group_id: 0,
                    im_model_year_group_id: 7,
                },
                PollutantProcessMappedModelYearRow {
                    pol_process_id: 301,
                    model_year_id: 2019,
                    model_year_group_id: 0,
                    fuel_my_group_id: 0,
                    im_model_year_group_id: 7,
                },
            ],
            age_category: vec![
                AgeCategoryRow {
                    age_id: 1,
                    age_group_id: 4,
                },
                AgeCategoryRow {
                    age_id: 2,
                    age_group_id: 4,
                },
            ],
            im_factor: vec![ImFactorRow {
                pol_process_id: 301,
                inspect_freq: 1,
                test_standards_id: 2,
                source_type_id: 21,
                fuel_type_id: 1,
                im_model_year_group_id: 7,
                age_group_id: 4,
                im_factor: 0.5,
            }],
            im_coverage: vec![ImCoverageRow {
                pol_process_id: 301,
                source_type_id: 21,
                fuel_type_id: 1,
                beg_model_year_id: 2018,
                end_model_year_id: 2019,
                inspect_freq: 1,
                test_standards_id: 2,
                compliance_factor: 80.0, // percent
            }],
            ..BaseRateCalculatorInputs::default()
        };
        let prepared = PreparedTables::from_inputs(inputs, &constants);
        // model year 2018 -> age 2 -> age group 4; 2019 -> age 1 -> age group 4.
        // Each: 0.5 * (0.01 * 80) = 0.4.
        assert_eq!(
            prepared.im_coverage.get(&ImCoverageKey {
                pol_process_id: 301,
                model_year_id: 2018,
                source_type_id: 21,
                fuel_type_id: 1,
            }),
            Some(&0.4)
        );
        assert_eq!(prepared.im_coverage.len(), 2);
    }

    #[test]
    fn fuel_supply_groups_formulations_by_cell() {
        let inputs = BaseRateCalculatorInputs {
            fuel_supply: vec![
                FuelSupplyRow {
                    county_id: 1,
                    year_id: 2020,
                    month_id: 7,
                    fuel_type_id: 1,
                    fuel_sub_type_id: 10,
                    fuel_formulation_id: 100,
                    market_share: 0.75,
                },
                FuelSupplyRow {
                    county_id: 1,
                    year_id: 2020,
                    month_id: 7,
                    fuel_type_id: 1,
                    fuel_sub_type_id: 11,
                    fuel_formulation_id: 101,
                    market_share: 0.25,
                },
            ],
            ..BaseRateCalculatorInputs::default()
        };
        let prepared = PreparedTables::from_inputs(inputs, &RunConstants::default());
        let cell = prepared
            .fuel_supply
            .get(&FuelSupplyKey {
                county_id: 1,
                year_id: 2020,
                month_id: 7,
                fuel_type_id: 1,
            })
            .expect("fuel supply cell present");
        assert_eq!(cell.len(), 2);
        assert_eq!(cell[0].market_share, 0.75);
    }
}

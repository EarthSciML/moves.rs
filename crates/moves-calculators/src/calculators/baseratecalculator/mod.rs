//! Base Rate Calculator — Phase 3 Task 45.
//!
//! Pure-Rust port of `calc/baseratecalculator/baseratecalculator.go`
//! (1,694 lines), the largest single calculator in the MOVES worker. It
//! implements the rates-first methodology: it takes the `BaseRate` /
//! `BaseRateByAge` tables the Base Rate Generator (Task 42) produced and
//! applies the temperature, humidity, fuel-effect, I/M, air-conditioning and
//! activity adjustments that turn them into the emission rates every
//! downstream criteria/GHG calculator chains from.
//!
//! # Module map
//!
//! | Module | Ports |
//! |--------|-------|
//! | [`model`] | the Go struct declarations and the `mwo`-package subset |
//! | [`setup`] | `StartSetup` — the ~20 lookup-table loaders and joins |
//! | [`adjust`] | `streamBaseRate*` row expansion + the `calculateAndAccumulate` adjustment sequence |
//! | [`aggregate`] | `aggregateOpModes`, `calculateActivityWeight`, `aggregateAndApplyActivity` |
//!
//! # What the Go did, and what this port keeps
//!
//! The Go worker read its inputs from a MariaDB execution database, ran the
//! computation across a goroutine pipeline connected by channels, and
//! streamed the result into temporary files. The port keeps the
//! **computation** verbatim — the per-operating-mode adjustment sequence, the
//! operating-mode aggregation, the activity weighting — and replaces the I/O
//! boundary with plain values: a [`BaseRateCalculatorInputs`] in, a
//! [`BaseRateCalculatorOutput`] out.
//!
//! The Go pipeline runs `calculateAndAccumulate` across several goroutines,
//! so its accumulation order — and therefore its floating-point sum order —
//! is already non-deterministic. The port collapses the pipeline to
//! sequential calls over deterministic ordered maps; the computed values are
//! identical within the tolerance that non-determinism already implies.
//!
//! # Data-plane status
//!
//! [`BaseRateCalculator::run`] is the numerical entry point and is fully
//! exercised by the crate's tests. The [`Calculator`] trait's
//! [`execute`](Calculator::execute) method is a shell: the
//! [`CalculatorContext`] it receives exposes only the Phase 2 placeholder
//! `ExecutionTables` / `ScratchNamespace`, which have no row storage yet.
//! Task 50 (`DataFrameStore`) lands that storage; the `execute` body then
//! materialises a [`BaseRateCalculatorInputs`] from the context, calls
//! [`BaseRateCalculator::run`], and writes the [`BaseRateCalculatorOutput`]
//! back. Until then `execute` returns an empty [`CalculatorOutput`] and the
//! metadata methods carry the real wiring information the registry needs.

pub mod adjust;
pub mod aggregate;
pub mod model;
pub mod setup;

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock};

use moves_calculator_info::{Granularity, Priority};
use moves_data::{PollutantId, PollutantProcessAssociation, ProcessId};
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped,
    Error, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

pub use model::{
    BlockKey, FuelBlock, ModuleFlags, RunConstants, ZoneMonthHourDetail, ZoneMonthHourKey,
};
pub use setup::{BaseRateCalculatorInputs, PreparedTables};

use adjust::{build_fuel_blocks, process_fuel_block};
use aggregate::{aggregate_and_apply_activity, calculate_activity_weight};

/// Stable module name in the calculator-chain DAG.
const CALCULATOR_NAME: &str = "BaseRateCalculator";

/// Default-DB and scratch tables the calculator reads. `BaseRate` and
/// `BaseRateByAge` are the Base Rate Generator's output tables; the rest are
/// the lookup tables `StartSetup` loaded.
static INPUT_TABLES: &[&str] = &[
    "BaseRate",
    "BaseRateByAge",
    "ExtendedIdleEmissionRateFraction",
    "apuEmissionRateFraction",
    "ShorepowerEmissionRateFraction",
    "ZoneMonthHour",
    "PollutantProcessMappedModelYear",
    "StartTempAdjustment",
    "County",
    "GeneralFuelRatio",
    "criteriaRatio",
    "altCriteriaRatio",
    "TemperatureAdjustment",
    "NOxHumidityAdjust",
    "zoneACFactor",
    "IMFactor",
    "IMCoverage",
    "EmissionRateAdjustment",
    "EVEfficiency",
    "universalActivity",
    "smfrSBDSummary",
    "AgeCategory",
    "FuelType",
    "FuelFormulation",
    "FuelSupply",
    "FuelSubtype",
    "MonthOfAnyYear",
];

/// One flattened output record — a [`BlockKey`] paired with one fuel
/// formulation's aggregated emission.
///
/// The Go streamed `FuelBlock`s, each carrying an `MWOKey` and a list of
/// `MWOEmission`s, to the worker's output writer. [`BaseRateCalculatorOutput::rows`]
/// produces the equivalent flat row form.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EmissionOutputRow {
    /// Identifying key of the block this emission belongs to.
    pub key: BlockKey,
    /// Fuel subtype id.
    pub fuel_sub_type_id: i32,
    /// Fuel formulation id.
    pub fuel_formulation_id: i32,
    /// Emission quantity.
    pub emission_quant: f64,
    /// Emission rate.
    pub emission_rate: f64,
}

/// The output of one Base Rate Calculator run.
///
/// Each [`FuelBlock`] carries its [`BlockKey`] and the per-fuel-formulation
/// [`Emission`](model::Emission)s `aggregate_op_modes` produced. The
/// operating-mode detail has been collapsed away — `op_mode` is `None` on
/// every block here.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BaseRateCalculatorOutput {
    /// The aggregated, activity-weighted fuel blocks.
    pub blocks: Vec<FuelBlock>,
}

impl BaseRateCalculatorOutput {
    /// Flatten the blocks into one [`EmissionOutputRow`] per
    /// `(block, fuel formulation)` pair.
    #[must_use]
    pub fn rows(&self) -> Vec<EmissionOutputRow> {
        let mut rows = Vec::new();
        for block in &self.blocks {
            for emission in &block.emissions {
                rows.push(EmissionOutputRow {
                    key: block.key,
                    fuel_sub_type_id: emission.fuel_sub_type_id,
                    fuel_formulation_id: emission.fuel_formulation_id,
                    emission_quant: emission.emission_quant,
                    emission_rate: emission.emission_rate,
                });
            }
        }
        rows
    }
}

fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::RowExtraction {
        table: table.to_string(),
        row,
        column: column.to_string(),
        message: msg,
    }
}

impl TableRow for EmissionOutputRow {
    fn table_name() -> &'static str {
        "MOVESWorkerOutput"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("yearID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
            ("stateID".into(), DataType::Int32),
            ("countyID".into(), DataType::Int32),
            ("zoneID".into(), DataType::Int32),
            ("linkID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("regClassID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("avgSpeedBinID".into(), DataType::Int32),
            ("pollutantID".into(), DataType::Int32),
            ("processID".into(), DataType::Int32),
            ("polProcessID".into(), DataType::Int32),
            ("hourDayID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("fuelSubTypeID".into(), DataType::Int32),
            ("fuelFormulationID".into(), DataType::Int32),
            ("emissionQuant".into(), DataType::Float64),
            ("emissionRate".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.key.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.key.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "dayID".into(),
                    rows.iter().map(|r| r.key.day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourID".into(),
                    rows.iter().map(|r| r.key.hour_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "stateID".into(),
                    rows.iter().map(|r| r.key.state_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "countyID".into(),
                    rows.iter().map(|r| r.key.county_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "zoneID".into(),
                    rows.iter().map(|r| r.key.zone_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "linkID".into(),
                    rows.iter().map(|r| r.key.link_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeID".into(),
                    rows.iter()
                        .map(|r| r.key.road_type_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter()
                        .map(|r| r.key.source_type_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "regClassID".into(),
                    rows.iter()
                        .map(|r| r.key.reg_class_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter()
                        .map(|r| r.key.fuel_type_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "modelYearID".into(),
                    rows.iter()
                        .map(|r| r.key.model_year_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "avgSpeedBinID".into(),
                    rows.iter()
                        .map(|r| r.key.avg_speed_bin_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "pollutantID".into(),
                    rows.iter()
                        .map(|r| r.key.pollutant_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "processID".into(),
                    rows.iter().map(|r| r.key.process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "polProcessID".into(),
                    rows.iter()
                        .map(|r| r.key.pol_process_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourDayID".into(),
                    rows.iter().map(|r| r.key.hour_day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.key.age_id).collect::<Vec<i32>>(),
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
                    "emissionQuant".into(),
                    rows.iter().map(|r| r.emission_quant).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "emissionRate".into(),
                    rows.iter().map(|r| r.emission_rate).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "MOVESWorkerOutput";
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
        let year_id = get_i32("yearID")?;
        let month_id = get_i32("monthID")?;
        let day_id = get_i32("dayID")?;
        let hour_id = get_i32("hourID")?;
        let state_id = get_i32("stateID")?;
        let county_id = get_i32("countyID")?;
        let zone_id = get_i32("zoneID")?;
        let link_id = get_i32("linkID")?;
        let road_type_id = get_i32("roadTypeID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let reg_class_id = get_i32("regClassID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        let model_year_id = get_i32("modelYearID")?;
        let avg_speed_bin_id = get_i32("avgSpeedBinID")?;
        let pollutant_id = get_i32("pollutantID")?;
        let process_id = get_i32("processID")?;
        let pol_process_id = get_i32("polProcessID")?;
        let hour_day_id = get_i32("hourDayID")?;
        let age_id = get_i32("ageID")?;
        let fuel_sub_type_id = get_i32("fuelSubTypeID")?;
        let fuel_formulation_id = get_i32("fuelFormulationID")?;
        let emission_quant = get_f64("emissionQuant")?;
        let emission_rate = get_f64("emissionRate")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(EmissionOutputRow {
                    key: BlockKey {
                        year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                        month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                        day_id: day_id.get(i).ok_or_else(|| null("dayID"))?,
                        hour_id: hour_id.get(i).ok_or_else(|| null("hourID"))?,
                        state_id: state_id.get(i).ok_or_else(|| null("stateID"))?,
                        county_id: county_id.get(i).ok_or_else(|| null("countyID"))?,
                        zone_id: zone_id.get(i).ok_or_else(|| null("zoneID"))?,
                        link_id: link_id.get(i).ok_or_else(|| null("linkID"))?,
                        road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
                        source_type_id: source_type_id
                            .get(i)
                            .ok_or_else(|| null("sourceTypeID"))?,
                        reg_class_id: reg_class_id.get(i).ok_or_else(|| null("regClassID"))?,
                        fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                        model_year_id: model_year_id.get(i).ok_or_else(|| null("modelYearID"))?,
                        avg_speed_bin_id: avg_speed_bin_id
                            .get(i)
                            .ok_or_else(|| null("avgSpeedBinID"))?,
                        pollutant_id: pollutant_id.get(i).ok_or_else(|| null("pollutantID"))?,
                        process_id: process_id.get(i).ok_or_else(|| null("processID"))?,
                        pol_process_id: pol_process_id
                            .get(i)
                            .ok_or_else(|| null("polProcessID"))?,
                        hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                        age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    },
                    fuel_sub_type_id: fuel_sub_type_id
                        .get(i)
                        .ok_or_else(|| null("fuelSubTypeID"))?,
                    fuel_formulation_id: fuel_formulation_id
                        .get(i)
                        .ok_or_else(|| null("fuelFormulationID"))?,
                    emission_quant: emission_quant.get(i).ok_or_else(|| null("emissionQuant"))?,
                    emission_rate: emission_rate.get(i).ok_or_else(|| null("emissionRate"))?,
                })
            })
            .collect()
    }
}

/// The Base Rate Calculator.
///
/// Holds a per-location [`PreparedTables`] cache keyed by [`RunConstants`].
/// `PreparedTables::from_inputs` rebuilds ~20 BTreeMaps on every call; for
/// multi-iteration runs the same (location, time) fires the calculator
/// multiple times. The cache amortises that cost across iterations — the
/// maps are built once per location and reused thereafter.
#[derive(Debug, Default)]
pub struct BaseRateCalculator {
    cache: Mutex<HashMap<RunConstants, Arc<PreparedTables>>>,
}

/// Run one accumulation pass — the Go `streamBaseRate*` → `calculateAndAccumulate`
/// → `disburseAccumulatedBlocks` sequence for one input table.
///
/// Rows are expanded into fuel blocks, each block is run through the
/// adjustment sequence, and the results are accumulated into a map keyed by
/// [`BlockKey`] (operating mode is *not* part of the key): blocks sharing a
/// key have their base-rate lists concatenated, exactly as the Go
/// `uniqueFuelBlocks` map did.
fn process_pass(
    rows: &[setup::BaseRateRow],
    prepared: &PreparedTables,
    constants: &RunConstants,
    flags: &ModuleFlags,
    gpa_fract: f64,
) -> Vec<FuelBlock> {
    let mut unique: BTreeMap<BlockKey, FuelBlock> = BTreeMap::new();
    for fb in build_fuel_blocks(rows, prepared, constants) {
        for processed in process_fuel_block(fb, prepared, flags, gpa_fract) {
            match unique.get_mut(&processed.key) {
                Some(existing) => {
                    if let (Some(existing_op), Some(processed_op)) =
                        (existing.op_mode.as_mut(), processed.op_mode)
                    {
                        existing_op.base_rates.extend(processed_op.base_rates);
                    }
                }
                None => {
                    unique.insert(processed.key, processed);
                }
            }
        }
    }
    unique.into_values().collect()
}

impl BaseRateCalculator {
    /// Stable module name — matches the Go source and the chain-DAG entry.
    pub const NAME: &'static str = CALCULATOR_NAME;

    /// Run the calculator over a fully materialised set of input tables.
    ///
    /// Ports `StartCalculating` / `doCalculationPipeline`. The Go processes
    /// the age-based (`BaseRateByAge`) and non-age-based (`BaseRate`) tables
    /// in two independent accumulation passes, then aggregates the
    /// operating-mode detail and applies the activity weighting; the port
    /// follows the same order.
    #[must_use]
    pub fn run(
        mut inputs: BaseRateCalculatorInputs,
        constants: &RunConstants,
        flags: &ModuleFlags,
    ) -> BaseRateCalculatorOutput {
        let smfr_sbd_summary = std::mem::take(&mut inputs.smfr_sbd_summary);
        let base_rate_by_age = std::mem::take(&mut inputs.base_rate_by_age);
        let base_rate = std::mem::take(&mut inputs.base_rate);
        let prepared = PreparedTables::from_inputs(inputs, constants);
        Self::run_with_prepared(
            smfr_sbd_summary,
            base_rate_by_age,
            base_rate,
            &prepared,
            constants,
            flags,
        )
    }

    fn run_with_prepared(
        smfr_sbd_summary: Vec<setup::SmfrSbdSummaryRow>,
        base_rate_by_age: Vec<setup::BaseRateRow>,
        base_rate: Vec<setup::BaseRateRow>,
        prepared: &PreparedTables,
        constants: &RunConstants,
        flags: &ModuleFlags,
    ) -> BaseRateCalculatorOutput {
        // The Go indexes `County[CountyID]` per row; the run processes a
        // single county, so the GPA fraction is resolved once. A county
        // absent from the table yields `0.0` (the Go would have panicked —
        // the county table always holds the run's one county).
        let gpa_fract = prepared
            .county
            .get(&constants.county_id)
            .map_or(0.0, |c| c.gpa_fract);

        // calculateActivityWeight runs once, ahead of the aggregation tail.
        let activity_weights = calculate_activity_weight(&smfr_sbd_summary, prepared, flags);

        // Two accumulation passes: age-based then non-age-based.
        let mut blocks = process_pass(&base_rate_by_age, prepared, constants, flags, gpa_fract);
        blocks.extend(process_pass(
            &base_rate, prepared, constants, flags, gpa_fract,
        ));

        // Aggregate operating modes and apply the activity weighting.
        for block in &mut blocks {
            aggregate_and_apply_activity(block, prepared, flags, &activity_weights);
        }

        BaseRateCalculatorOutput { blocks }
    }
}

/// The `(process, granularity, priority)` subscriptions — six processes at
/// `MONTH` granularity, `EMISSION_CALCULATOR` priority.
///
/// Matches the `Subscribe` directives recorded for `BaseRateCalculator` in
/// `CalculatorInfo.txt` and the `calculator-dag.json` entry.
fn subscriptions() -> &'static [CalculatorSubscription] {
    static SUBS: OnceLock<Vec<CalculatorSubscription>> = OnceLock::new();
    SUBS.get_or_init(|| {
        let priority = Priority::parse("EMISSION_CALCULATOR")
            .expect("EMISSION_CALCULATOR is a valid priority");
        // Running, Start, Brakewear, Tirewear, Extended Idle, Aux Power.
        [1_u16, 2, 9, 10, 90, 91]
            .into_iter()
            .map(|process| {
                CalculatorSubscription::new(ProcessId(process), Granularity::Month, priority)
            })
            .collect()
    })
}

/// The `(pollutant, process)` pairs the calculator registers.
///
/// The 96 pairs are the `Registration` directives recorded for
/// `BaseRateCalculator` in `CalculatorInfo.txt` (`registrations_count: 96` in
/// `calculator-dag.json`):
///
/// * twelve exhaust pollutants × six processes — 72 pairs. The Java
///   constructor's static `pollutantIDs` list holds ten of these; the run
///   that produced `CalculatorInfo.txt` also resolved pollutants 92 and 93
///   through calculator chaining.
/// * twenty-four distance-based pollutant/process pairs, all process 1. The
///   Java `distancePolProcessIDs` list holds twenty-five; pollutant 64 did
///   not resolve in that run.
fn registrations() -> &'static [PollutantProcessAssociation] {
    static REGS: OnceLock<Vec<PollutantProcessAssociation>> = OnceLock::new();
    REGS.get_or_init(|| {
        let mut regs = Vec::with_capacity(96);
        // Exhaust pollutants × processes.
        const EXHAUST_POLLUTANTS: [u16; 12] = [1, 2, 3, 6, 30, 91, 92, 93, 112, 116, 117, 118];
        const PROCESSES: [u16; 6] = [1, 2, 9, 10, 90, 91];
        for pollutant in EXHAUST_POLLUTANTS {
            for process in PROCESSES {
                regs.push(PollutantProcessAssociation {
                    pollutant_id: PollutantId(pollutant),
                    process_id: ProcessId(process),
                });
            }
        }
        // Distance-based pollutants, all process 1.
        let distance_pollutants = (60_u16..=67).filter(|p| *p != 64).chain(130_u16..=146);
        for pollutant in distance_pollutants {
            regs.push(PollutantProcessAssociation {
                pollutant_id: PollutantId(pollutant),
                process_id: ProcessId(1),
            });
        }
        regs
    })
}

impl Calculator for BaseRateCalculator {
    fn name(&self) -> &'static str {
        CALCULATOR_NAME
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        subscriptions()
    }

    fn registrations(&self) -> &[PollutantProcessAssociation] {
        registrations()
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let tables = ctx.tables();
        let pos = ctx.position();
        let constants = RunConstants {
            state_id: pos.location.state_id.map(|s| s as i32).unwrap_or(0),
            county_id: pos.location.county_id.map(|c| c as i32).unwrap_or(0),
            zone_id: pos.location.zone_id.map(|z| z as i32).unwrap_or(0),
            link_id: pos.location.link_id.map(|l| l as i32).unwrap_or(0),
            year_id: pos.time.year.map(|y| y as i32).unwrap_or(0),
            month_id: pos.time.month.map(|m| m as i32).unwrap_or(0),
        };
        let mut inputs = BaseRateCalculatorInputs {
            base_rate_by_age: tables.iter_typed("BaseRateByAge")?,
            base_rate: tables.iter_typed("BaseRate")?,
            extended_idle_emission_rate_fraction: tables
                .iter_typed_or_empty("ExtendedIdleEmissionRateFraction")?,
            apu_emission_rate_fraction: tables.iter_typed_or_empty("apuEmissionRateFraction")?,
            shorepower_emission_rate_fraction: tables
                .iter_typed_or_empty("ShorepowerEmissionRateFraction")?,
            pollutant_process_mapped_model_year: tables
                .iter_typed("PollutantProcessMappedModelYear")?,
            start_temp_adjustment: tables.iter_typed("StartTempAdjustment")?,
            county: tables.iter_typed("County")?,
            general_fuel_ratio: tables.iter_typed("GeneralFuelRatio")?,
            criteria_ratio: tables.iter_typed("criteriaRatio")?,
            alt_criteria_ratio: tables.iter_typed("altCriteriaRatio")?,
            temperature_adjustment: tables.iter_typed("TemperatureAdjustment")?,
            nox_humidity_adjust: tables.iter_typed("NOxHumidityAdjust")?,
            zone_ac_factor: tables.iter_typed_or_empty("zoneACFactor")?,
            im_factor: tables.iter_typed("IMFactor")?,
            im_coverage: tables.iter_typed("IMCoverage")?,
            emission_rate_adjustment: tables.iter_typed("EmissionRateAdjustment")?,
            ev_efficiency: tables.iter_typed("EVEfficiency")?,
            universal_activity: tables.iter_typed_or_empty("universalActivity")?,
            smfr_sbd_summary: tables.iter_typed_or_empty("smfrSBDSummary")?,
            age_category: tables.iter_typed("AgeCategory")?,
            fuel_types: tables
                .iter_typed::<setup::FuelTypeRow>("FuelType")?
                .into_iter()
                .map(|r| r.fuel_type_id)
                .collect(),
            fuel_formulations: tables.iter_typed("FuelFormulation")?,
            zone_month_hour: tables.iter_typed("ZoneMonthHour")?,
            fuel_supply: build_fuel_supply(tables, &constants)?,
        };
        let smfr_sbd_summary = std::mem::take(&mut inputs.smfr_sbd_summary);
        let base_rate_by_age = std::mem::take(&mut inputs.base_rate_by_age);
        let base_rate = std::mem::take(&mut inputs.base_rate);
        let prepared = {
            let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(hit) = cache.get(&constants) {
                Arc::clone(hit)
            } else {
                let p = Arc::new(PreparedTables::from_inputs(inputs, &constants));
                cache.insert(constants, Arc::clone(&p));
                p
            }
        };
        let output = Self::run_with_prepared(
            smfr_sbd_summary,
            base_rate_by_age,
            base_rate,
            &prepared,
            &constants,
            &ModuleFlags::default(),
        );
        let rows = output.rows();
        crate::wiring::emit_rows(rows)
    }
}

/// Construct the calculator as a boxed trait object — matches the engine's
/// calculator-factory signature so the registry can register it.
#[must_use]
pub fn factory() -> Box<dyn Calculator> {
    Box::new(BaseRateCalculator::default())
}

// =============================================================================
// FuelSupply join helpers — read the raw DB schema and join with FuelFormulation,
// FuelSubtype, and MonthOfAnyYear to produce the FuelSupplyRow the calculator
// expects. The Java extract step did this join; the snapshot stores the raw tables.
// =============================================================================

/// One raw `FuelSupply` row — the actual DB schema (no countyID, no monthID).
struct RawFuelSupplyRow {
    fuel_region_id: i32,
    fuel_year_id: i32,
    month_group_id: i32,
    fuel_formulation_id: i32,
    market_share: f64,
}

impl TableRow for RawFuelSupplyRow {
    fn table_name() -> &'static str {
        "FuelSupply"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelRegionID".into(), DataType::Int32),
            ("fuelYearID".into(), DataType::Int32),
            ("monthGroupID".into(), DataType::Int32),
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
                    "fuelRegionID".into(),
                    rows.iter().map(|r| r.fuel_region_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelYearID".into(),
                    rows.iter().map(|r| r.fuel_year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthGroupID".into(),
                    rows.iter().map(|r| r.month_group_id).collect::<Vec<i32>>(),
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
                .map_err(|e| moves_framework::Error::RowExtraction {
                    table: t.into(),
                    row: 0,
                    column: col.into(),
                    message: e.to_string(),
                })?
                .i32()
                .map_err(|e| moves_framework::Error::RowExtraction {
                    table: t.into(),
                    row: 0,
                    column: col.into(),
                    message: e.to_string(),
                })
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| moves_framework::Error::RowExtraction {
                    table: t.into(),
                    row: 0,
                    column: col.into(),
                    message: e.to_string(),
                })?
                .f64()
                .map_err(|e| moves_framework::Error::RowExtraction {
                    table: t.into(),
                    row: 0,
                    column: col.into(),
                    message: e.to_string(),
                })
        };
        let fuel_region_id = get_i32("fuelRegionID")?;
        let fuel_year_id = get_i32("fuelYearID")?;
        let month_group_id = get_i32("monthGroupID")?;
        let fuel_formulation_id = get_i32("fuelFormulationID")?;
        let market_share = get_f64("marketShare")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| moves_framework::Error::RowExtraction {
                    table: t.into(),
                    row: i,
                    column: col.into(),
                    message: "null value".into(),
                };
                Ok(RawFuelSupplyRow {
                    fuel_region_id: fuel_region_id.get(i).ok_or_else(|| null("fuelRegionID"))?,
                    fuel_year_id: fuel_year_id.get(i).ok_or_else(|| null("fuelYearID"))?,
                    month_group_id: month_group_id.get(i).ok_or_else(|| null("monthGroupID"))?,
                    fuel_formulation_id: fuel_formulation_id
                        .get(i)
                        .ok_or_else(|| null("fuelFormulationID"))?,
                    market_share: market_share.get(i).ok_or_else(|| null("marketShare"))?,
                })
            })
            .collect()
    }
}

/// One `FuelSubtype` row — only the two columns needed for the join.
struct LocalFuelSubtypeRow {
    fuel_subtype_id: i32,
    fuel_type_id: i32,
}

impl TableRow for LocalFuelSubtypeRow {
    fn table_name() -> &'static str {
        "FuelSubtype"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
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
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| moves_framework::Error::RowExtraction {
                    table: t.into(),
                    row: 0,
                    column: col.into(),
                    message: e.to_string(),
                })?
                .i32()
                .map_err(|e| moves_framework::Error::RowExtraction {
                    table: t.into(),
                    row: 0,
                    column: col.into(),
                    message: e.to_string(),
                })
        };
        let fuel_subtype_id = get_i32("fuelSubtypeID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| moves_framework::Error::RowExtraction {
                    table: t.into(),
                    row: i,
                    column: col.into(),
                    message: "null value".into(),
                };
                Ok(LocalFuelSubtypeRow {
                    fuel_subtype_id: fuel_subtype_id
                        .get(i)
                        .ok_or_else(|| null("fuelSubtypeID"))?,
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                })
            })
            .collect()
    }
}

/// One `MonthOfAnyYear` row — `monthGroupID → monthID` mapping.
struct LocalMonthGroupRow {
    month_group_id: i32,
    month_id: i32,
}

impl TableRow for LocalMonthGroupRow {
    fn table_name() -> &'static str {
        "MonthOfAnyYear"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("monthGroupID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "monthGroupID".into(),
                    rows.iter().map(|r| r.month_group_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "MonthOfAnyYear";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| moves_framework::Error::RowExtraction {
                    table: t.into(),
                    row: 0,
                    column: col.into(),
                    message: e.to_string(),
                })?
                .i32()
                .map_err(|e| moves_framework::Error::RowExtraction {
                    table: t.into(),
                    row: 0,
                    column: col.into(),
                    message: e.to_string(),
                })
        };
        let month_group_id = get_i32("monthGroupID")?;
        let month_id = get_i32("monthID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| moves_framework::Error::RowExtraction {
                    table: t.into(),
                    row: i,
                    column: col.into(),
                    message: "null value".into(),
                };
                Ok(LocalMonthGroupRow {
                    month_group_id: month_group_id.get(i).ok_or_else(|| null("monthGroupID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                })
            })
            .collect()
    }
}

/// Build the `FuelSupplyRow` list by joining raw DB tables.
///
/// The Java extract step joined `FuelSupply` with `FuelFormulation`,
/// `FuelSubtype`, county/region, and month mappings to produce a denormalized
/// view with `countyID`, `yearID`, `monthID`, `fuelTypeID`, and
/// `fuelSubTypeID`. This function reproduces that join from the snapshot tables.
fn build_fuel_supply(
    tables: &moves_framework::InMemoryStore,
    constants: &RunConstants,
) -> moves_framework::Result<Vec<setup::FuelSupplyRow>> {
    use setup::FuelFormulationRow;
    use setup::FuelSupplyRow;

    let raw: Vec<RawFuelSupplyRow> = tables.iter_typed("FuelSupply")?;
    let ff: Vec<FuelFormulationRow> = tables.iter_typed("FuelFormulation")?;
    let fs: Vec<LocalFuelSubtypeRow> = tables.iter_typed_or_empty("FuelSubtype")?;
    let mg: Vec<LocalMonthGroupRow> = tables.iter_typed_or_empty("MonthOfAnyYear")?;

    let formulation_to_subtype: HashMap<i32, i32> = ff
        .iter()
        .map(|r| (r.fuel_formulation_id, r.fuel_sub_type_id))
        .collect();
    let subtype_to_type: HashMap<i32, i32> = fs
        .iter()
        .map(|r| (r.fuel_subtype_id, r.fuel_type_id))
        .collect();
    let group_to_month: HashMap<i32, i32> =
        mg.iter().map(|r| (r.month_group_id, r.month_id)).collect();

    let rows = raw
        .iter()
        .filter_map(|r| {
            let fuel_sub_type_id = formulation_to_subtype
                .get(&r.fuel_formulation_id)
                .copied()?;
            let fuel_type_id = subtype_to_type.get(&fuel_sub_type_id).copied()?;
            let month_id = group_to_month
                .get(&r.month_group_id)
                .copied()
                .unwrap_or(r.month_group_id);
            Some(FuelSupplyRow {
                county_id: constants.county_id,
                year_id: r.fuel_year_id,
                month_id,
                fuel_type_id,
                fuel_sub_type_id,
                fuel_formulation_id: r.fuel_formulation_id,
                market_share: r.market_share,
            })
        })
        .collect();
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calculator_metadata_matches_calculator_info() {
        let calc = BaseRateCalculator::default();
        assert_eq!(calc.name(), "BaseRateCalculator");

        let subs = calc.subscriptions();
        assert_eq!(subs.len(), 6);
        let processes: Vec<u16> = subs.iter().map(|s| s.process_id.0).collect();
        assert_eq!(processes, vec![1, 2, 9, 10, 90, 91]);
        assert!(subs.iter().all(|s| s.granularity == Granularity::Month));
        assert!(subs
            .iter()
            .all(|s| s.priority.display() == "EMISSION_CALCULATOR"));
    }

    #[test]
    fn registrations_match_the_96_calculator_info_directives() {
        let calc = BaseRateCalculator::default();
        let regs = calc.registrations();
        assert_eq!(regs.len(), 96);

        // Twelve exhaust pollutants each appear for all six processes.
        for pollutant in [1_u16, 2, 3, 6, 30, 91, 92, 93, 112, 116, 117, 118] {
            let count = regs
                .iter()
                .filter(|r| r.pollutant_id == PollutantId(pollutant))
                .count();
            assert_eq!(count, 6, "pollutant {pollutant} should have six processes");
        }
        // Distance pollutant 64 did not resolve; 65 did.
        assert!(!regs.iter().any(|r| r.pollutant_id == PollutantId(64)));
        assert!(regs
            .iter()
            .any(|r| r.pollutant_id == PollutantId(65) && r.process_id == ProcessId(1)));
        // Distance pollutants are process 1 only.
        for pollutant in (130_u16..=146).chain(60..=63) {
            let procs: Vec<u16> = regs
                .iter()
                .filter(|r| r.pollutant_id == PollutantId(pollutant))
                .map(|r| r.process_id.0)
                .collect();
            assert_eq!(procs, vec![1], "pollutant {pollutant} is distance-only");
        }
    }

    #[test]
    fn input_tables_name_the_base_rate_generator_output() {
        // The two tables linking this calculator to Task 42's generator.
        let calc = BaseRateCalculator::default();
        assert!(calc.input_tables().contains(&"BaseRate"));
        assert!(calc.input_tables().contains(&"BaseRateByAge"));
        assert!(calc.upstream().is_empty());
    }

    #[test]
    fn calculator_is_object_safe() {
        // The registry stores calculators as `Box<dyn Calculator>`.
        let calcs: Vec<Box<dyn Calculator>> = vec![Box::new(BaseRateCalculator::default())];
        assert_eq!(calcs[0].name(), "BaseRateCalculator");
    }

    #[test]
    fn execute_returns_nonempty_dataframe_for_minimal_inputs() {
        use moves_framework::execution::execution_db::{
            ExecutionLocation, ExecutionTime, IterationPosition,
        };
        use moves_framework::{DataFrameStore, InMemoryStore};
        use setup::{AgeCategoryRow, BaseRateRow, FuelFormulationRow, FuelTypeRow};
        let base_rate_row = BaseRateRow {
            source_type_id: 21,
            road_type_id: 4,
            avg_speed_bin_id: 0,
            hour_day_id: 85,
            pollutant_id: 1,
            process_id: 1,
            model_year_id: 2018,
            fuel_type_id: 1,
            reg_class_id: 30,
            op_mode_id: 1,
            mean_base_rate: 1.0,
            mean_base_rate_im: 1.0,
            emission_rate: 1.0,
            emission_rate_im: 1.0,
            mean_base_rate_ac_adj: 1.0,
            mean_base_rate_im_ac_adj: 1.0,
            emission_rate_ac_adj: 1.0,
            emission_rate_im_ac_adj: 1.0,
            op_mode_fraction: 1.0,
            op_mode_fraction_rate: 1.0,
        };
        // Raw DB schema for FuelSupply (joined in build_fuel_supply).
        let raw_fuel_supply = RawFuelSupplyRow {
            fuel_region_id: 270000000, // placeholder region
            fuel_year_id: 2020,
            month_group_id: 7,
            fuel_formulation_id: 100,
            market_share: 1.0,
        };
        let fuel_formulation = FuelFormulationRow {
            fuel_formulation_id: 100,
            fuel_sub_type_id: 10,
        };
        let fuel_type = FuelTypeRow { fuel_type_id: 1 };
        let age_category = AgeCategoryRow {
            age_id: 2,
            age_group_id: 1,
        };

        let mut store = InMemoryStore::new();
        store.insert(
            "BaseRateByAge",
            BaseRateRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert(
            "BaseRate",
            BaseRateRow::into_dataframe(vec![base_rate_row]).unwrap(),
        );
        store.insert(
            "ExtendedIdleEmissionRateFraction",
            setup::ModelYearFuelFractionRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert(
            "apuEmissionRateFraction",
            setup::ModelYearFuelFractionRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert(
            "ShorepowerEmissionRateFraction",
            setup::ModelYearFuelFractionRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert(
            "ZoneMonthHour",
            setup::ZoneMonthHourRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert(
            "PollutantProcessMappedModelYear",
            setup::PollutantProcessMappedModelYearRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert(
            "StartTempAdjustment",
            setup::StartTempAdjustmentRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert("County", setup::CountyRow::into_dataframe(vec![]).unwrap());
        store.insert(
            "GeneralFuelRatio",
            setup::GeneralFuelRatioRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert(
            "criteriaRatio",
            setup::CriteriaRatioRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert(
            "altCriteriaRatio",
            setup::CriteriaRatioRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert(
            "TemperatureAdjustment",
            setup::TemperatureAdjustmentRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert(
            "NOxHumidityAdjust",
            setup::NoxHumidityAdjustRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert(
            "zoneACFactor",
            setup::ZoneAcFactorRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert(
            "IMFactor",
            setup::ImFactorRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert(
            "IMCoverage",
            setup::ImCoverageRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert(
            "EmissionRateAdjustment",
            setup::EmissionRateAdjustmentRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert(
            "EVEfficiency",
            setup::EvEfficiencyRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert(
            "universalActivity",
            setup::UniversalActivityRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert(
            "smfrSBDSummary",
            setup::SmfrSbdSummaryRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert(
            "AgeCategory",
            AgeCategoryRow::into_dataframe(vec![age_category]).unwrap(),
        );
        store.insert(
            "FuelType",
            FuelTypeRow::into_dataframe(vec![fuel_type]).unwrap(),
        );
        store.insert(
            "FuelFormulation",
            FuelFormulationRow::into_dataframe(vec![fuel_formulation]).unwrap(),
        );
        store.insert(
            "FuelSupply",
            RawFuelSupplyRow::into_dataframe(vec![raw_fuel_supply]).unwrap(),
        );
        store.insert(
            "FuelSubtype",
            LocalFuelSubtypeRow::into_dataframe(vec![LocalFuelSubtypeRow {
                fuel_subtype_id: 10,
                fuel_type_id: 1,
            }])
            .unwrap(),
        );
        store.insert(
            "MonthOfAnyYear",
            LocalMonthGroupRow::into_dataframe(vec![LocalMonthGroupRow {
                month_group_id: 7,
                month_id: 7,
            }])
            .unwrap(),
        );

        let position = IterationPosition {
            iteration: 0,
            process_id: None,
            location: ExecutionLocation::link(26, 26_161, 90, 5001),
            time: ExecutionTime::hour(2020, 7, 5, 8),
        };
        let ctx = CalculatorContext::with_position_and_tables(position, store);
        let calc = BaseRateCalculator::default();
        let out = calc.execute(&ctx).expect("execute ok");
        assert!(out.dataframe().is_some(), "expected non-empty DataFrame");
        assert!(
            out.dataframe().unwrap().height() > 0,
            "expected at least one row"
        );
    }

    #[test]
    fn run_on_empty_inputs_yields_empty_output() {
        let inputs = BaseRateCalculatorInputs::default();
        let output =
            BaseRateCalculator::run(inputs, &RunConstants::default(), &ModuleFlags::default());
        assert!(output.blocks.is_empty());
        assert!(output.rows().is_empty());
    }

    #[test]
    fn prepared_tables_cache_is_populated_on_first_call_and_reused_on_second() {
        use moves_framework::execution::execution_db::{
            ExecutionLocation, ExecutionTime, IterationPosition,
        };
        use moves_framework::{DataFrameStore, InMemoryStore};
        use setup::{AgeCategoryRow, BaseRateRow, FuelFormulationRow, FuelTypeRow};

        fn minimal_store() -> InMemoryStore {
            let mut store = InMemoryStore::new();
            store.insert(
                "BaseRateByAge",
                BaseRateRow::into_dataframe(vec![]).unwrap(),
            );
            store.insert("BaseRate", BaseRateRow::into_dataframe(vec![]).unwrap());
            store.insert(
                "ExtendedIdleEmissionRateFraction",
                setup::ModelYearFuelFractionRow::into_dataframe(vec![]).unwrap(),
            );
            store.insert(
                "apuEmissionRateFraction",
                setup::ModelYearFuelFractionRow::into_dataframe(vec![]).unwrap(),
            );
            store.insert(
                "ShorepowerEmissionRateFraction",
                setup::ModelYearFuelFractionRow::into_dataframe(vec![]).unwrap(),
            );
            store.insert(
                "ZoneMonthHour",
                setup::ZoneMonthHourRow::into_dataframe(vec![]).unwrap(),
            );
            store.insert(
                "PollutantProcessMappedModelYear",
                setup::PollutantProcessMappedModelYearRow::into_dataframe(vec![]).unwrap(),
            );
            store.insert(
                "StartTempAdjustment",
                setup::StartTempAdjustmentRow::into_dataframe(vec![]).unwrap(),
            );
            store.insert("County", setup::CountyRow::into_dataframe(vec![]).unwrap());
            store.insert(
                "GeneralFuelRatio",
                setup::GeneralFuelRatioRow::into_dataframe(vec![]).unwrap(),
            );
            store.insert(
                "criteriaRatio",
                setup::CriteriaRatioRow::into_dataframe(vec![]).unwrap(),
            );
            store.insert(
                "altCriteriaRatio",
                setup::CriteriaRatioRow::into_dataframe(vec![]).unwrap(),
            );
            store.insert(
                "TemperatureAdjustment",
                setup::TemperatureAdjustmentRow::into_dataframe(vec![]).unwrap(),
            );
            store.insert(
                "NOxHumidityAdjust",
                setup::NoxHumidityAdjustRow::into_dataframe(vec![]).unwrap(),
            );
            store.insert(
                "zoneACFactor",
                setup::ZoneAcFactorRow::into_dataframe(vec![]).unwrap(),
            );
            store.insert(
                "IMFactor",
                setup::ImFactorRow::into_dataframe(vec![]).unwrap(),
            );
            store.insert(
                "IMCoverage",
                setup::ImCoverageRow::into_dataframe(vec![]).unwrap(),
            );
            store.insert(
                "EmissionRateAdjustment",
                setup::EmissionRateAdjustmentRow::into_dataframe(vec![]).unwrap(),
            );
            store.insert(
                "EVEfficiency",
                setup::EvEfficiencyRow::into_dataframe(vec![]).unwrap(),
            );
            store.insert(
                "universalActivity",
                setup::UniversalActivityRow::into_dataframe(vec![]).unwrap(),
            );
            store.insert(
                "smfrSBDSummary",
                setup::SmfrSbdSummaryRow::into_dataframe(vec![]).unwrap(),
            );
            store.insert(
                "AgeCategory",
                AgeCategoryRow::into_dataframe(vec![]).unwrap(),
            );
            store.insert("FuelType", FuelTypeRow::into_dataframe(vec![]).unwrap());
            store.insert(
                "FuelFormulation",
                FuelFormulationRow::into_dataframe(vec![]).unwrap(),
            );
            store.insert(
                "FuelSupply",
                RawFuelSupplyRow::into_dataframe(vec![]).unwrap(),
            );
            store.insert(
                "FuelSubtype",
                LocalFuelSubtypeRow::into_dataframe(vec![]).unwrap(),
            );
            store.insert(
                "MonthOfAnyYear",
                LocalMonthGroupRow::into_dataframe(vec![]).unwrap(),
            );
            store
        }

        let store = minimal_store();
        let pos_a = IterationPosition {
            iteration: 0,
            process_id: None,
            location: ExecutionLocation::link(26, 26_161, 90, 5001),
            time: ExecutionTime::hour(2020, 7, 5, 8),
        };
        let pos_b = IterationPosition {
            iteration: 1,
            process_id: None,
            location: ExecutionLocation::link(26, 26_161, 90, 5001),
            time: ExecutionTime::hour(2020, 7, 5, 8),
        };
        let pos_c = IterationPosition {
            iteration: 0,
            process_id: None,
            location: ExecutionLocation::link(26, 26_999, 91, 5002),
            time: ExecutionTime::hour(2020, 7, 5, 8),
        };
        use std::sync::Arc;
        let slow = Arc::new(store);

        let calc = BaseRateCalculator::default();
        assert_eq!(calc.cache.lock().unwrap().len(), 0, "cache starts empty");

        let ctx_a = CalculatorContext::with_slow(Arc::clone(&slow));
        // pos_a and pos_b share the same county/zone/link/year/month — same RunConstants.
        let mut ctx_a = ctx_a;
        ctx_a.set_position(pos_a);
        calc.execute(&ctx_a).expect("first execute ok");
        assert_eq!(calc.cache.lock().unwrap().len(), 1, "one location cached");

        let mut ctx_b = CalculatorContext::with_slow(Arc::clone(&slow));
        ctx_b.set_position(pos_b);
        calc.execute(&ctx_b)
            .expect("second execute ok — same location, cache hit");
        assert_eq!(
            calc.cache.lock().unwrap().len(),
            1,
            "cache still has one entry after same-position second call"
        );

        let mut ctx_c = CalculatorContext::with_slow(Arc::clone(&slow));
        ctx_c.set_position(pos_c);
        calc.execute(&ctx_c)
            .expect("third execute ok — different location");
        assert_eq!(
            calc.cache.lock().unwrap().len(),
            2,
            "two entries after a different-location call"
        );
    }
}

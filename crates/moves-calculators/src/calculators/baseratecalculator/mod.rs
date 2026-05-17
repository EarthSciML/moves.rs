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

use std::collections::BTreeMap;
use std::sync::OnceLock;

use moves_calculator_info::{Granularity, Priority};
use moves_data::{PollutantId, PollutantProcessAssociation, ProcessId};
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, Error,
};

pub use model::{BlockKey, FuelBlock, ModuleFlags, RunConstants};
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

/// The Base Rate Calculator.
///
/// A zero-sized value type: the calculator owns no per-run state, exactly as
/// the [`Calculator`] trait contract requires. All run-varying input flows
/// through [`BaseRateCalculator::run`]'s arguments.
#[derive(Debug, Clone, Copy, Default)]
pub struct BaseRateCalculator;

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
    /// Run the calculator over a fully materialised set of input tables.
    ///
    /// Ports `StartCalculating` / `doCalculationPipeline`. The Go processes
    /// the age-based (`BaseRateByAge`) and non-age-based (`BaseRate`) tables
    /// in two independent accumulation passes, then aggregates the
    /// operating-mode detail and applies the activity weighting; the port
    /// follows the same order.
    #[must_use]
    pub fn run(
        inputs: &BaseRateCalculatorInputs,
        constants: &RunConstants,
        flags: &ModuleFlags,
    ) -> BaseRateCalculatorOutput {
        let prepared = PreparedTables::from_inputs(inputs, constants);

        // The Go indexes `County[CountyID]` per row; the run processes a
        // single county, so the GPA fraction is resolved once. A county
        // absent from the table yields `0.0` (the Go would have panicked —
        // the county table always holds the run's one county).
        let gpa_fract = prepared
            .county
            .get(&constants.county_id)
            .map_or(0.0, |c| c.gpa_fract);

        // calculateActivityWeight runs once, ahead of the aggregation tail.
        let activity_weights =
            calculate_activity_weight(&inputs.smfr_sbd_summary, &prepared, flags);

        // Two accumulation passes: age-based then non-age-based.
        let mut blocks = process_pass(
            &inputs.base_rate_by_age,
            &prepared,
            constants,
            flags,
            gpa_fract,
        );
        blocks.extend(process_pass(
            &inputs.base_rate,
            &prepared,
            constants,
            flags,
            gpa_fract,
        ));

        // Aggregate operating modes and apply the activity weighting.
        for block in &mut blocks {
            aggregate_and_apply_activity(block, &prepared, flags, &activity_weights);
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

    fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        // Shell pending the Task 50 data plane — see the module docs. The
        // numerical core is `BaseRateCalculator::run`; once `ExecutionTables`
        // and `ScratchNamespace` carry real rows, this body materialises a
        // `BaseRateCalculatorInputs` from `_ctx` and calls it.
        Ok(CalculatorOutput::empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calculator_metadata_matches_calculator_info() {
        let calc = BaseRateCalculator;
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
        let calc = BaseRateCalculator;
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
        let calc = BaseRateCalculator;
        assert!(calc.input_tables().contains(&"BaseRate"));
        assert!(calc.input_tables().contains(&"BaseRateByAge"));
        assert!(calc.upstream().is_empty());
    }

    #[test]
    fn calculator_is_object_safe() {
        // The registry stores calculators as `Box<dyn Calculator>`.
        let calcs: Vec<Box<dyn Calculator>> = vec![Box::new(BaseRateCalculator)];
        assert_eq!(calcs[0].name(), "BaseRateCalculator");
    }

    #[test]
    fn execute_is_a_shell_until_the_data_plane_lands() {
        let calc = BaseRateCalculator;
        let ctx = CalculatorContext::new();
        assert!(calc.execute(&ctx).is_ok());
    }

    #[test]
    fn run_on_empty_inputs_yields_empty_output() {
        let inputs = BaseRateCalculatorInputs::default();
        let output =
            BaseRateCalculator::run(&inputs, &RunConstants::default(), &ModuleFlags::default());
        assert!(output.blocks.is_empty());
        assert!(output.rows().is_empty());
    }
}

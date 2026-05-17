//! Base Rate Generator — Phase 3 Task 42.
//!
//! Pure-Rust port of `generators/baserategenerator/baserategenerator.go`
//! (2,391 lines), one of the two largest pieces of Go in the MOVES worker.
//! The generator builds the `BaseRate` and `BaseRateByAge` tables that the
//! `BaseRateCalculator` (Task 45) consumes — its output is the input to
//! every running-emission calculation, so numerical fidelity is critical.
//!
//! # Module map
//!
//! | Module | Ports |
//! |--------|-------|
//! | [`model`] | the Go struct declarations and `readExternalFlags` |
//! | [`inputs`] | `setupTables` and the thirteen `read*` table loaders |
//! | [`aggregate`] | `coreBaseRateGeneratorFromRatesOpModeDistribution` and the three `makeBaseRate*` aggregators |
//! | [`drivecycle`] | `findDriveCycles`, `calculateDriveCycleOpModeDistribution`, `processDriveCycles` |
//!
//! # What the Go did, and what this port keeps
//!
//! The Go worker read its inputs from a MariaDB execution database, ran the
//! computation across goroutines connected by channels, and streamed the
//! result into temporary files `LOAD DATA INFILE`'d back into MariaDB. The
//! port keeps the **computation** verbatim — the physics-mapping source-type
//! swaps, the weighted-rate aggregation, the vehicle-specific-power
//! operating-mode binning — and replaces the I/O boundary with plain values:
//! a [`BaseRateInputs`] in, a [`BaseRateOutput`] out. The goroutine pipeline
//! collapses to sequential calls because every stage is pure; the producer
//! yields its [`RomdBlock`](model::RomdBlock) stream in exactly the order the
//! Go SQL `ORDER BY` / nested-loop enumeration produced, so the streaming
//! aggregators accumulate identical sums.
//!
//! # Data-plane status
//!
//! [`BaseRateGenerator::run`] is the numerical entry point and is fully
//! exercised by the crate's tests. The [`Generator`] trait's
//! [`execute`](Generator::execute) method is a shell: the
//! [`CalculatorContext`] it receives exposes only the Phase 2 placeholder
//! [`ExecutionTables`] / [`ScratchNamespace`], which have no row storage
//! yet. Task 50 (`DataFrameStore`) lands that storage; the `execute` body
//! then materialises a [`BaseRateInputs`] from the context, calls
//! [`BaseRateGenerator::run`], and writes the [`BaseRateOutput`] back into
//! the scratch namespace. Until then `execute` returns an empty
//! [`CalculatorOutput`] and the metadata methods carry the real wiring
//! information the registry needs.
//!
//! [`ExecutionTables`]: moves_framework::ExecutionTables
//! [`ScratchNamespace`]: moves_framework::ScratchNamespace

pub mod aggregate;
pub mod drivecycle;
pub mod inputs;
pub mod model;

use std::sync::OnceLock;

use moves_calculator_info::{Granularity, Priority};
use moves_data::ProcessId;
use moves_framework::{
    CalculatorContext, CalculatorOutput, CalculatorSubscription, Error, Generator,
};

pub use inputs::{BaseRateInputs, PreparedTables};
pub use model::{
    BaseRateOutputRecord, DrivingIdleFractionRow, ExternalFlags, ALWAYS_USE_ROMD_TABLE,
};

use aggregate::{
    core_base_rate_generator_from_romd, make_base_rate_by_age_from_source_bin_rates,
    make_base_rate_from_distance_rates, make_base_rate_from_source_bin_rates,
};
use drivecycle::process_drive_cycles;

/// Stable module name in the calculator-chain DAG.
const GENERATOR_NAME: &str = "BaseRateGenerator";

/// Default-DB and scratch tables the generator reads. Names match the
/// casing used in the MOVES default database.
static INPUT_TABLES: &[&str] = &[
    "RatesOpModeDistribution",
    "SBWeightedEmissionRateByAge",
    "SBWeightedEmissionRate",
    "SBWeightedDistanceRate",
    "avgSpeedBin",
    "avgSpeedDistribution",
    "driveSchedule",
    "driveScheduleAssoc",
    "driveScheduleSecond",
    "operatingMode",
    "sourceUseTypePhysicsMapping",
    "runSpecRoadType",
    "runSpecHourDay",
    "runSpecSourceType",
    "runSpecPollutantProcess",
    "opModePolProcAssoc",
];

/// Scratch tables the generator writes for downstream calculators.
static OUTPUT_TABLES: &[&str] = &["BaseRate", "BaseRateByAge", "DrivingIdleFraction"];

/// The tables the Base Rate Generator produces in one run.
///
/// `base_rate` is the union of the source-bin-weighted and distance-weighted
/// contributions — the Go loaded both into the same `baseRate_<p>_<y>`
/// table. `driving_idle_fraction` is populated only on the drive-cycle path.
#[derive(Debug, Clone, Default)]
pub struct BaseRateOutput {
    /// Rows of the `BaseRate` table.
    pub base_rate: Vec<BaseRateOutputRecord>,
    /// Rows of the `BaseRateByAge` table.
    pub base_rate_by_age: Vec<BaseRateOutputRecord>,
    /// Rows of the `DrivingIdleFraction` table (drive-cycle path only).
    pub driving_idle_fraction: Vec<DrivingIdleFractionRow>,
}

/// The Base Rate Generator.
///
/// A zero-sized value type: the generator owns no per-run state, exactly as
/// the [`Generator`] trait contract requires. All run-varying input flows
/// through [`BaseRateGenerator::run`]'s arguments.
#[derive(Debug, Clone, Copy, Default)]
pub struct BaseRateGenerator;

impl BaseRateGenerator {
    /// Run the generator over a fully materialised set of input tables.
    ///
    /// Ports `BaseRateGeneratorFromRatesOpModeDistribution`. The drive-cycle
    /// fast path is taken for non-Project Running Exhaust (`processID == 1`)
    /// and Brakewear (`processID == 9`); every other process, and every
    /// Project-domain run, takes the `RatesOpModeDistribution` core path.
    /// Distance-based rates are always folded into the `BaseRate` output.
    #[must_use]
    pub fn run(inputs: &BaseRateInputs, flags: &ExternalFlags) -> BaseRateOutput {
        let prepared = PreparedTables::from_inputs(inputs, flags);

        // shouldProcessDriveCycles — EMT-633 added processID 9 (Brakewear)
        // alongside processID 1 (Running Exhaust). ALWAYS_USE_ROMD_TABLE
        // forces the core path; it is `false` in normal operation.
        let should_process_drive_cycles = !inputs.is_project
            && (flags.process_id == 1 || flags.process_id == 9)
            && !ALWAYS_USE_ROMD_TABLE;

        let (romd_blocks, driving_idle_fraction) = if should_process_drive_cycles {
            let drive = process_drive_cycles(inputs, &prepared, flags);
            (drive.romd_blocks, drive.driving_idle_fraction)
        } else {
            (
                core_base_rate_generator_from_romd(inputs, &prepared, flags),
                Vec::new(),
            )
        };

        let mut base_rate = make_base_rate_from_source_bin_rates(&romd_blocks, &prepared, flags);
        base_rate.extend(make_base_rate_from_distance_rates(inputs, &prepared, flags));
        let base_rate_by_age =
            make_base_rate_by_age_from_source_bin_rates(&romd_blocks, &prepared, flags);

        BaseRateOutput {
            base_rate,
            base_rate_by_age,
            driving_idle_fraction,
        }
    }
}

impl Generator for BaseRateGenerator {
    fn name(&self) -> &'static str {
        GENERATOR_NAME
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        // Built once: `Priority::parse` is not a `const fn`, so the slice
        // cannot be a plain `static`. The Base Rate Generator subscribes for
        // every exhaust process at YEAR granularity, priority GENERATOR-2 —
        // the rows recorded in `CalculatorInfo.txt`.
        static SUBS: OnceLock<Vec<CalculatorSubscription>> = OnceLock::new();
        SUBS.get_or_init(|| {
            let priority = Priority::parse("GENERATOR-2").expect("GENERATOR-2 is a valid priority");
            // Running, Start, Brakewear, Tirewear, Extended Idle, Aux Power.
            [1_u16, 2, 9, 10, 90, 91]
                .into_iter()
                .map(|process| {
                    CalculatorSubscription::new(ProcessId(process), Granularity::Year, priority)
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

    fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        // Shell pending the Task 50 data plane — see the module docs. The
        // numerical core is `BaseRateGenerator::run`; once `ScratchNamespace`
        // and `ExecutionTables` carry real rows, this body materialises a
        // `BaseRateInputs` from `_ctx` and calls it.
        Ok(CalculatorOutput::empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generator_metadata_matches_calculator_info() {
        let generator = BaseRateGenerator;
        assert_eq!(generator.name(), "BaseRateGenerator");

        let subs = generator.subscriptions();
        assert_eq!(subs.len(), 6);
        let processes: Vec<u16> = subs.iter().map(|s| s.process_id.0).collect();
        assert_eq!(processes, vec![1, 2, 9, 10, 90, 91]);
        assert!(subs.iter().all(|s| s.granularity == Granularity::Year));
        assert!(subs.iter().all(|s| s.priority.display() == "GENERATOR-2"));

        // Generators register no (pollutant, process) output pairs.
        assert!(generator.upstream().is_empty());
        assert!(generator
            .input_tables()
            .contains(&"RatesOpModeDistribution"));
        assert_eq!(
            generator.output_tables(),
            &["BaseRate", "BaseRateByAge", "DrivingIdleFraction"]
        );
    }

    #[test]
    fn generator_is_object_safe() {
        // The registry stores generators as `Box<dyn Generator>`.
        let generators: Vec<Box<dyn Generator>> = vec![Box::new(BaseRateGenerator)];
        assert_eq!(generators[0].name(), "BaseRateGenerator");
    }

    #[test]
    fn execute_is_a_shell_until_the_data_plane_lands() {
        let generator = BaseRateGenerator;
        let ctx = CalculatorContext::new();
        // The shell must not error — the registry may still call it.
        assert!(generator.execute(&ctx).is_ok());
    }

    #[test]
    fn run_on_empty_inputs_yields_empty_output() {
        let inputs = BaseRateInputs::default();
        let flags = ExternalFlags {
            process_id: 2,
            year_id: 2020,
            ..ExternalFlags::default()
        };
        let output = BaseRateGenerator::run(&inputs, &flags);
        assert!(output.base_rate.is_empty());
        assert!(output.base_rate_by_age.is_empty());
        assert!(output.driving_idle_fraction.is_empty());
    }
}

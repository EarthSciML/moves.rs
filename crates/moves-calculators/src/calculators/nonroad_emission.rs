//! Adapter bridging the onroad master loop to `moves-nonroad`'s
//! in-process simulation API — port of
//! `gov/epa/otaq/moves/master/nonroad/NonroadEmissionCalculator.java`.
//!
//! Migration plan: Phase 5, Task 119 (`mo-y4bs`).
//!
//! # What this adapter does
//!
//! In canonical MOVES, `NonroadEmissionCalculator` was the onroad DAG's
//! hook into the `nonroad.exe` subprocess: whenever the master loop
//! reached a nonroad source at a subscribed process/granularity, the
//! Java class generated the NONROAD input files, spawned `nonroad.exe`,
//! and ingested its output back into MariaDB.
//!
//! This Rust adapter replaces that subprocess boundary with a direct
//! call to [`moves_nonroad::run_simulation`]. When the master loop
//! fires the adapter for a DAY-granularity iteration over processes
//! 1, 15, 18–21, or 30–32, the adapter:
//!
//! 1. Extracts the episode year from the iteration position.
//! 2. Builds a [`NonroadOptions`] for a county-level run.
//! 3. Creates a [`NonroadInputs`] bundle (currently empty — the
//!    data-plane wiring that populates it from execution-DB tables
//!    is a follow-on task).
//! 4. Calls [`run_simulation`] with a [`ProductionExecutor`] backed
//!    by default (empty) [`ReferenceData`].
//! 5. Returns [`CalculatorOutput::empty()`] — the output-to-DataFrame
//!    conversion (mapping [`SimEmissionRow`] onto the unified Parquet
//!    output schema) is likewise deferred until the data-plane wiring
//!    lands.
//!
//! # Why empty inputs are safe for now
//!
//! `NonroadInputs::new()` creates an SCC-group list with no records
//! and an unconstrained region selection. `run_simulation` exits its
//! outer SCC-group loop immediately and returns an empty
//! `NonroadOutputs`. The call is a pure no-op — no I/O, no panics —
//! so the adapter can be registered and exercised by the
//! mixed-onroad-nonroad fixture today, even though the actual nonroad
//! emission numbers will only appear once the population-data wiring
//! is done.
//!
//! # Process subscription note
//!
//! `calculator-dag.json` records subscriptions to processes 1, 15,
//! 18, 19, 20, 21, 30, 31, and 32 at `DAY` granularity with
//! `EMISSION_CALCULATOR` priority — exactly matching
//! `CalculatorInfo.txt`. These are the nonroad emission processes;
//! the same process IDs appear in the onroad DAG because MOVES shares
//! the process-ID namespace between onroad and nonroad sources.
//!
//! [`NonroadOptions`]: moves_nonroad::NonroadOptions
//! [`NonroadInputs`]: moves_nonroad::NonroadInputs
//! [`run_simulation`]: moves_nonroad::run_simulation
//! [`ProductionExecutor`]: moves_nonroad::simulation::ProductionExecutor
//! [`ReferenceData`]: moves_nonroad::simulation::ReferenceData
//! [`SimEmissionRow`]: moves_nonroad::simulation::outputs::SimEmissionRow

use moves_calculator_info::{Granularity, Priority};
use moves_data::{PollutantProcessAssociation, ProcessId};
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, Error,
};

use moves_framework::data::DataFrameStore;

use crate::calculators::nonroad_loader::{self, EmissionTimeKeys};

/// MOVES default episode year used when the iteration position carries
/// no year (e.g. when the master loop fires before entering YEAR scope).
const DEFAULT_YEAR: i32 = 2020;

/// Subscribed process IDs — the nine nonroad emission processes the
/// `calculator-dag.json` records for `NonroadEmissionCalculator`.
const SUBSCRIBED_PROCESS_IDS: &[u16] = &[1, 15, 18, 19, 20, 21, 30, 31, 32];

/// The `nr*` execution-DB tables the data-plane loader reads to build the
/// NONROAD engine inputs. Declared via [`Calculator::input_tables`] so the
/// snapshot loader admits them (the loader only loads tables in the union
/// of every calculator's declared inputs).
const NONROAD_INPUT_TABLES: &[&str] = &[
    "nremissionrate",
    "nrdeterioration",
    "nrengtechfraction",
    "nrsourceusetype",
    "nrbaseyearequippopulation",
    "nrmonthallocation",
    "nrdayallocation",
    "nrhourallocation",
    "nrhourallocpattern",
    "nrhourpatternfinder",
    "nrgrowthindex",
    "nrgrowthpattern",
    "nrgrowthpatternfinder",
    "nrscrappagecurve",
    "nrscc",
    "nrhprangebin",
    "nrhpcategory",
    "nrsulfuradjustment",
    "nrstatesurrogate",
    "nrsourceusetypephysicsmapping",
    "runspecsector",
    "runspecfueltype",
    "nrfuelsupply",
    "fuelformulation",
    "nrfuelsubtype",
    "nrequipmenttype",
    "nragecategory",
    "nrmodelyear",
];

/// Adapter that routes onroad master-loop notifications for nonroad
/// emission processes into `moves-nonroad`'s [`run_simulation`] API.
///
/// This is a zero-sized type: all state it needs at runtime is derived
/// from its constructor arguments (the subscriptions) or extracted
/// from the [`CalculatorContext`] at execution time. The nonroad
/// reference data and population inputs are built fresh on every
/// `execute` call until the data-plane wiring provides a cached
/// `ReferenceData` from the execution DB.
///
/// [`run_simulation`]: moves_nonroad::run_simulation
#[derive(Debug)]
pub struct NonroadEmissionCalculator {
    subscriptions: Vec<CalculatorSubscription>,
}

impl NonroadEmissionCalculator {
    /// DAG name — matches the Java class and the `calculator-dag.json` entry.
    pub const NAME: &'static str = "NonroadEmissionCalculator";

    /// Construct the adapter, building one [`CalculatorSubscription`]
    /// per subscribed process at `DAY` granularity /
    /// `EMISSION_CALCULATOR` priority.
    #[must_use]
    pub fn new() -> Self {
        let priority = Priority::parse("EMISSION_CALCULATOR")
            .expect("\"EMISSION_CALCULATOR\" is a valid MasterLoop priority");
        let subscriptions = SUBSCRIBED_PROCESS_IDS
            .iter()
            .map(|&pid| CalculatorSubscription::new(ProcessId(pid), Granularity::Day, priority))
            .collect();
        Self { subscriptions }
    }
}

impl Default for NonroadEmissionCalculator {
    fn default() -> Self {
        Self::new()
    }
}

impl Calculator for NonroadEmissionCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        &self.subscriptions
    }

    /// Declares the `nr*` execution-DB tables the data-plane loader reads,
    /// so the snapshot loader admits them into the in-memory store.
    fn input_tables(&self) -> &[&'static str] {
        NONROAD_INPUT_TABLES
    }

    /// `NonroadEmissionCalculator` emits no `MOVESWorkerOutput` rows of its
    /// own: the Java class delegated to `nonroad.exe`, which wrote to separate
    /// output tables. In `moves-nonroad`, the simulation output flows through
    /// `NonroadOutputs` and will be mapped to the unified Parquet schema by
    /// the data-plane wiring (a follow-on task). Until that wiring lands,
    /// this slice is empty — consistent with `calculator-dag.json`'s
    /// `registrations_count: 0`.
    fn registrations(&self) -> &[PollutantProcessAssociation] {
        &[]
    }

    /// Run the NONROAD simulation for the current master-loop iteration.
    ///
    /// Extracts the episode year from `ctx.position().time.year`, falls
    /// back to [`DEFAULT_YEAR`] when the position carries none (should
    /// not happen at `DAY` granularity, but defensively handled).
    /// Calls [`moves_nonroad::run_simulation`] with empty inputs and
    /// default reference data, returning [`CalculatorOutput::empty()`]
    /// until the output-mapping wiring is in place.
    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let time = &ctx.position().time;
        let year = time.year.map(i32::from).unwrap_or(DEFAULT_YEAR);

        // Build the NONROAD engine inputs from the nr* execution-DB tables
        // (the in-process replacement for the Java input-file generator).
        let store = ctx.tables();
        let options = nonroad_loader::build_options(year);
        let inputs = nonroad_loader::build_nonroad_inputs(store, year);
        let debug = std::env::var("MOVES_NR_DEBUG").is_ok();
        if debug {
            eprintln!(
                "[nr] execute year={year} groups={} records={} has_nremissionrate={} has_pop={}",
                inputs.group_count(),
                inputs.record_count(),
                store.contains("nremissionrate"),
                store.contains("nrbaseyearequippopulation"),
            );
        }
        if inputs.is_empty() {
            // No nonroad population in this run — nothing to compute.
            return Ok(CalculatorOutput::empty());
        }
        let mut executor = nonroad_loader::build_production_executor(store, year);

        let outputs = moves_nonroad::run_simulation(&options, &inputs, &mut executor)
            .map_err(|e| Error::Nonroad(e.to_string()))?;
        if debug {
            let nonzero = outputs
                .rows
                .iter()
                .filter(|r| r.emissions.iter().any(|&e| e != 0.0))
                .count();
            eprintln!(
                "[nr] sim rows={} nonzero_rows={} counters={:?}",
                outputs.rows.len(),
                nonzero,
                outputs.counters,
            );
        }

        // Map the engine's SimEmissionRows onto the MOVESOutput schema,
        // allocating the engine's annual emissions onto this iteration's
        // month/day slice.
        let month = time.month.map(i32::from);
        let day = time.day_id.map(i32::from);
        let keys = EmissionTimeKeys {
            year,
            month,
            day,
            hour: time.hour.map(i32::from),
        };
        let temporal =
            nonroad_loader::build_temporal_factors(store, month.unwrap_or(0), day.unwrap_or(0));
        match nonroad_loader::emissions_to_dataframe(&outputs.rows, &keys, &temporal)
            .map_err(|e| Error::Polars(e.to_string()))?
        {
            Some(df) => Ok(CalculatorOutput::with_dataframe(df)),
            None => Ok(CalculatorOutput::empty()),
        }
    }
}

/// Construct the calculator as a boxed trait object — matches the
/// engine's calculator-factory signature so the registry can register it.
#[must_use]
pub fn factory() -> Box<dyn Calculator> {
    Box::new(NonroadEmissionCalculator::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use moves_framework::CalculatorContext;

    #[test]
    fn name_matches_dag() {
        assert_eq!(NonroadEmissionCalculator::NAME, "NonroadEmissionCalculator");
        assert_eq!(
            NonroadEmissionCalculator::new().name(),
            "NonroadEmissionCalculator"
        );
    }

    #[test]
    fn subscriptions_match_dag() {
        let calc = NonroadEmissionCalculator::new();
        let subs = calc.subscriptions();
        assert_eq!(
            subs.len(),
            9,
            "expected 9 subscriptions from calculator-dag.json"
        );

        let process_ids: Vec<u16> = subs.iter().map(|s| s.process_id.0).collect();
        assert_eq!(process_ids, vec![1, 15, 18, 19, 20, 21, 30, 31, 32]);

        for sub in subs {
            assert_eq!(
                sub.granularity,
                Granularity::Day,
                "all subscriptions at DAY granularity"
            );
        }
    }

    #[test]
    fn registrations_are_empty() {
        let calc = NonroadEmissionCalculator::new();
        assert!(
            calc.registrations().is_empty(),
            "NonroadEmissionCalculator has no direct (pollutant, process) registrations"
        );
    }

    #[test]
    fn execute_with_empty_context_succeeds() {
        let calc = NonroadEmissionCalculator::new();
        let ctx = CalculatorContext::new();
        let result = calc.execute(&ctx);
        assert!(result.is_ok(), "execute with empty context must not error");
        assert!(
            result.unwrap().dataframe().is_none(),
            "output must be empty (no DataFrame) until data-plane wiring lands"
        );
    }

    #[test]
    fn factory_produces_boxed_calculator() {
        let calc: Box<dyn Calculator> = factory();
        assert_eq!(calc.name(), "NonroadEmissionCalculator");
        assert_eq!(calc.subscriptions().len(), 9);
    }
}

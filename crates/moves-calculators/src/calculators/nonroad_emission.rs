//! Adapter bridging the onroad master loop to `moves-nonroad`'s
//! in-process simulation API — port of
//! `gov/epa/otaq/moves/master/nonroad/NonroadEmissionCalculator.java`.
//!
//!
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
//! 1. Checks the iteration position carries a full Year-Month-Day key
//! (mirroring canonical `doesProcessContext`); a context missing any
//! of those is skipped rather than run with a fabricated year.
//! 2. Builds a [`NonroadOptions`] for a county-level run at that year.
//! 3. Builds the [`NonroadInputs`] population bundle from the `nr*`
//! execution-DB tables via [`nonroad_loader::build_nonroad_inputs`].
//! 4. Calls [`run_simulation`] with a [`ProductionExecutor`] whose
//! [`ReferenceData`] is loaded from the same `nr*` tables.
//! 5. Maps the engine's [`SimEmissionRow`]s onto the MOVESOutput schema
//! and returns the resulting [`CalculatorOutput`].
//!
//! # Empty inputs: no-op vs. data-load failure
//!
//! An empty [`NonroadInputs`] bundle is treated as a *legitimate
//! nonroad-free slice* (no equipment after the runspec's sector/fuel
//! selection) **only** when the population-defining tables
//! (`nrsourceusetype`, `nrbaseyearequippopulation`) are actually present
//! in the execution store. If those tables are absent, the empty bundle
//! is instead a data-load failure — the snapshot loader was expected to
//! admit them (this calculator only fires for nonroad processes the
//! runspec selected) — and the adapter returns an [`Error::Nonroad`]
//! rather than a silent, successful-looking zero-emissions result.
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
    "zonemonthhour",
    "runspecmonth",
    "nrequipmenttype",
    "nragecategory",
    "nrmodelyear",
    // Evaporative emission rates by (SCC, HP-bin, polProcessID, engTechID).
    // Consumed by build_evap_tech_entries to populate EvapTechEntry emission
    // factors; drives compute_evap_factors / compute_evap_iteration in the
    // county path. g/hr and g/start species are computed; g/m2/day
    // (permeation) and Mult (diurnal) return RMISS pending spillage data.
    "nrevapemissionrate",
    // Retrofit annual/effective fractions by (SCC, engTech, hp, pollutant, retrofitID).
    // The engine's ReferenceData carries a retrofit_records slot; this declaration
    // ensures the table is available in-store for the retrofit loader.
    "nrretrofitfactors",
    // The runspec's pollutant×process selection — gates which engine
    // pollutant slots are emitted onto MOVESOutput (canonical's
    // need3101/need9001 bundle-SQL flags in NonroadOutputDataLoader).
    "runspecpollutantprocess",
    // zone (zoneID → countyID) joins zonemonthhour to the bundle county for
    // the OPTIONS-packet daytime ambient temperature; regioncounty + year
    // scope nrfuelsupply to the county's nonroad fuel region (the
    // OPTIONS-packet oxygen/sulfur queries).
    "zone",
    "regioncounty",
    "year",
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
    /// Canonical `NonroadEmissionCalculator.doesProcessContext`
    /// (`NonroadEmissionCalculator.java:457-481`) returns `false` for any
    /// context with `year <= 0`, `monthID <= 0`, or `dayID <= 0`. In the
    /// Rust port there is no separate `doesProcessContext` gate — the
    /// master loop guarantees that year, month, and day are all `Some` when
    /// dispatching at `DAY` granularity (the granularity this calculator
    /// subscribes at). A `None` here therefore means the engine failed to
    /// populate the position, which is a programming error — surface it as
    /// [`Error::MissingContext`] rather than silently returning empty output.
    ///
    /// Once a valid temporal position is present, builds the NONROAD engine
    /// inputs from the `nr*` execution-DB tables, calls
    /// [`moves_nonroad::run_simulation`], and maps the result onto the
    /// MOVESOutput schema.
    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let time = &ctx.position().time;
        // The master loop guarantees year/month/day are Some at DAY granularity.
        // A None here is a programming error — the engine should have filled the
        // position before dispatch.
        let year = time.year.ok_or_else(|| Error::MissingContext {
            what: "context.year".into(),
        })?;
        let _ = time.month.ok_or_else(|| Error::MissingContext {
            what: "context.month".into(),
        })?;
        let _ = time.day_id.ok_or_else(|| Error::MissingContext {
            what: "context.day_id".into(),
        })?;
        let year = i32::from(year);

        // Build the NONROAD engine inputs from the nr* execution-DB tables
        // (the in-process replacement for the Java input-file generator).
        let store = ctx.tables();
        let month = time.month.unwrap_or(0);
        let day_id = time.day_id.unwrap_or(0);
        // The master loop's current county scopes the national population
        // capture: None/0 ⇒ national, XX000 ⇒ state, else surrogate-allocated
        // county (NONROAD runs one region per bundle; the loop fires this
        // calculator once per county).
        let county_fips = ctx.position().location.county_id;
        let options = nonroad_loader::build_options(year, month, day_id);
        let inputs = nonroad_loader::build_nonroad_inputs(store, year, county_fips);
        let debug = std::env::var("MOVES_NR_DEBUG").is_ok();
        if debug {
            eprintln!(
                "[nr] execute year={year} county={county_fips:?} state={:?} groups={} records={} has_nremissionrate={} has_pop={}",
                ctx.position().location.state_id,
                inputs.group_count(),
                inputs.record_count(),
                store.contains("nremissionrate"),
                store.contains("nrbaseyearequippopulation"),
            );
        }
        if inputs.is_empty() {
            // An empty inputs bundle has two very different causes:
            //
            //  (a) the population-defining tables are *present* but produced no
            //      driver records (zero population, or every source type filtered
            //      out by the runspec's sector/fuel selection). This is a
            //      legitimate nonroad-free slice — canonical NONROAD simply emits
            //      no equipment for it — so we no-op.
            //
            //  (b) the population-defining tables are *absent* from the store. This
            //      calculator only fires for nonroad processes that are in the
            //      runspec (canonical `subscribeToMe` subscribes only when
            //      `doesHavePollutantAndProcess` holds — NonroadEmissionCalculator
            //      .java:108-134), so the snapshot loader was expected to admit
            //      these `nr*` tables. Their absence is a data-load failure, and a
            //      silent zero-emissions run would be indistinguishable from (a).
            //      Surface it as a hard error instead of fabricating an empty,
            //      successful-looking result.
            let has_source_use_type = store.contains("nrsourceusetype");
            let has_population = store.contains("nrbaseyearequippopulation");
            if !has_source_use_type || !has_population {
                let mut missing = Vec::new();
                if !has_source_use_type {
                    missing.push("nrsourceusetype");
                }
                if !has_population {
                    missing.push("nrbaseyearequippopulation");
                }
                return Err(Error::Nonroad(format!(
                    "nonroad population tables missing from the execution store \
                     ({}); cannot build NONROAD inputs for a nonroad-enabled run. \
                     A successful zero-emissions result would silently hide this \
                     missing-data condition.",
                    missing.join(", ")
                )));
            }
            // Tables present but empty population — a genuine nonroad-free slice.
            return Ok(CalculatorOutput::empty());
        }
        let mut executor =
            nonroad_loader::build_production_executor(store, year, month, day_id, county_fips);

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

        // Map the engine's SimEmissionRows onto the MOVESOutput schema.
        // The engine now applies temporal scaling internally (daymthf.f port)
        // when temporal profiles are loaded, so post-processing uses a neutral
        // map to avoid double-counting.
        let month_i = time.month.map(i32::from);
        let day_i = time.day_id.map(i32::from);
        let keys = EmissionTimeKeys {
            year,
            month: month_i,
            day: day_i,
            hour: time.hour.map(i32::from),
        };
        let temporal = if executor.reference.temporal_profiles.is_empty() {
            nonroad_loader::build_temporal_factors(
                store,
                month_i.unwrap_or(0),
                day_i.unwrap_or(0),
                county_fips.filter(|&c| c > 0).map(|c| i64::from(c) / 1000),
            )
        } else {
            std::collections::BTreeMap::new()
        };
        let selected_pollutants = nonroad_loader::selected_output_pollutants(store);
        match nonroad_loader::emissions_to_dataframe(
            &outputs.rows,
            &keys,
            &temporal,
            selected_pollutants.as_ref(),
        )
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
    fn execute_without_year_returns_missing_context() {
        // The master loop guarantees year/month/day are set at DAY granularity.
        // Calling execute() with an empty context (no position) is a programming
        // error — the calculator must surface it as MissingContext, not silently
        // return empty output.
        let calc = NonroadEmissionCalculator::new();
        let ctx = CalculatorContext::new();
        let result = calc.execute(&ctx);
        assert!(
            matches!(result, Err(Error::MissingContext { .. })),
            "expected Err(MissingContext) for empty context, got {:?}",
            result
        );
    }

    #[test]
    fn factory_produces_boxed_calculator() {
        let calc: Box<dyn Calculator> = factory();
        assert_eq!(calc.name(), "NonroadEmissionCalculator");
        assert_eq!(calc.subscriptions().len(), 9);
    }
}

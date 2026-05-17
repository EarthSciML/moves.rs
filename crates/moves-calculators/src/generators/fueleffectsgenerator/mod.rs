//! Fuel Effects Generator — Phase 3 Task 40.
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
//! migration plan names — together with the three pure `static` helpers,
//! and ports `FuelEffectsGeneratorTest`'s database-independent assertions
//! as the regression baseline. The predictive/complex-model paths are left
//! for a follow-up port; they need the complex-model expression engine,
//! which is a separate subsystem.
//!
//! # Data-plane status
//!
//! [`do_general_fuel_ratio`] is the numerical entry point and is fully
//! exercised by the crate's tests. The [`Generator`] trait's
//! [`execute`](Generator::execute) method is a shell: the
//! [`CalculatorContext`] it receives exposes only the Phase 2 placeholder
//! `ExecutionTables` / `ScratchNamespace`, which have no row storage yet.
//! Task 50 (`DataFrameStore`) lands that storage; `execute` then
//! materialises a [`GeneralFuelRatioInputs`] from the context, calls
//! [`do_general_fuel_ratio`], and writes the rows into the scratch
//! namespace. Until then `execute` returns an empty [`CalculatorOutput`]
//! and the metadata methods carry the real wiring information the registry
//! needs.

pub mod expression;
pub mod generalfuelratio;
pub mod model;
pub mod text;

use std::sync::OnceLock;

use moves_calculator_info::{Granularity, Priority};
use moves_data::ProcessId;
use moves_framework::{
    CalculatorContext, CalculatorOutput, CalculatorSubscription, Error, Generator,
};

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
static INPUT_TABLES: &[&str] = &[
    "generalFuelRatioExpression",
    "FuelFormulation",
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

    fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        // Shell pending the Task 50 data plane — see the module docs. The
        // numerical core is `do_general_fuel_ratio`; once `ExecutionTables`
        // and `ScratchNamespace` carry real rows, this body materialises a
        // `GeneralFuelRatioInputs` from `_ctx` and calls it.
        Ok(CalculatorOutput::empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        // The registry (Task 19) stores generators as `Box<dyn Generator>`.
        let generators: Vec<Box<dyn Generator>> = vec![Box::new(FuelEffectsGenerator)];
        assert_eq!(generators[0].name(), "FuelEffectsGenerator");
    }

    #[test]
    fn execute_is_a_shell_until_the_data_plane_lands() {
        let generator = FuelEffectsGenerator;
        let ctx = CalculatorContext::new();
        // The shell must not error — the registry may still call it.
        assert!(generator.execute(&ctx).is_ok());
    }
}

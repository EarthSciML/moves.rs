//! Port of `DummyCalculator.java` — migration plan Phase 3, Task 78
//! (Phase 3 closing checkpoint).
//!
//! `DummyCalculator` is a no-op placeholder that lives in MOVES's production
//! `implementation/ghg/` package but performs no computation. In canonical
//! Java MOVES it subscribed to every emission process at YEAR granularity
//! via `MasterLoopContext.ALL_PROCESSES` (process ID 0) and produced no
//! output — `registrations_count: 0` in `calculator-dag.json`.
//!
//! The Rust port carries empty `subscriptions()` and `registrations()` slices
//! because the Rust framework schedules calculators by real emission process
//! IDs (1–99); there is no ALL_PROCESSES sentinel. The Java subscription was
//! a testing artifact that triggered the calculator once per run regardless of
//! which process was executing; omitting it has no effect on outputs since the
//! calculator produced nothing.

use moves_data::PollutantProcessAssociation;
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, Error,
};

/// No-op placeholder — port of Java `DummyCalculator`.
///
/// Subscriptions and registrations are both empty; `execute` always returns
/// an empty output. Exists to satisfy the "every module in
/// `CalculatorInfo.txt` is represented in the Rust crate" completeness
/// criterion of Task 78.
#[derive(Debug)]
pub struct DummyCalculator;

impl Calculator for DummyCalculator {
    fn name(&self) -> &'static str {
        "DummyCalculator"
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        &[]
    }

    fn registrations(&self) -> &[PollutantProcessAssociation] {
        &[]
    }

    fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        Ok(CalculatorOutput::empty())
    }
}

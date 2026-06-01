//! `moves-fuel-control` — FuelControlStrategy internal control strategy.
//!
//! Ports `gov.epa.otaq.moves.master.implementation.general.FuelControlStrategy`
//! (MOVES5.0.1 commit 25dc6c83).
//!
//! # What this does
//!
//! The canonical Java implementation is a no-op stub — all lifecycle methods
//! (`subscribeToMe`, `executeLoop`, `cleanDataLoop`) have empty bodies, and the
//! JUnit test class declares a single `testNothing()` method that also does nothing.
//! This Rust port faithfully matches that behavior: the strategy registers under the
//! name `"FuelControlStrategy"` but leaves all input tables and emission rates
//! unchanged.
//!
//! It exists so that run configurations referencing `FuelControlStrategy` are
//! recognized by the framework without error. Actual fuel-supply adjustment logic
//! was never implemented in MOVES5 upstream.

use moves_framework::InternalControlStrategy;

/// FuelControlStrategy — no-op internal control strategy.
///
/// Ports `gov.epa.otaq.moves.master.implementation.general.FuelControlStrategy`.
///
/// All lifecycle hooks are no-ops. The canonical Java source is itself a skeleton:
/// `subscribeToMe`, `executeLoop`, and `cleanDataLoop` have empty bodies, and the
/// accompanying JUnit test (`testNothing`) verifies nothing. Zero emission-rate
/// adjustments are applied; running with this strategy active is equivalent to
/// running without it (`pct_diff = 0`).
#[derive(Debug, Default, Clone, Copy)]
pub struct FuelControlStrategy;

impl FuelControlStrategy {
 /// Construct a new `FuelControlStrategy`.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl InternalControlStrategy for FuelControlStrategy {
    fn name(&self) -> &'static str {
        "FuelControlStrategy"
    }
}

#[cfg(test)]
mod tests {
    use moves_framework::{DataFrameStore, InMemoryStore};
    use polars::prelude::{DataFrame, NamedFrom, Series};

    use super::*;

    #[test]
    fn name_is_stable() {
        assert_eq!(FuelControlStrategy::new().name(), "FuelControlStrategy");
    }

    #[test]
    fn subscriptions_is_empty() {
        assert!(FuelControlStrategy::new().subscriptions().is_empty());
    }

    #[test]
    fn modified_tables_is_empty() {
        assert!(
            FuelControlStrategy::new().modified_tables().is_empty(),
            "no-op strategy must not declare modified tables"
        );
    }

    #[test]
    fn pre_run_does_not_modify_store() {
        let s = FuelControlStrategy::new();
        let mut store = InMemoryStore::new();
        s.pre_run(&mut store).expect("pre_run must not fail");
        assert!(
            store.names().is_empty(),
            "no-op strategy must not insert any tables (pct_diff = 0 vs canonical)"
        );
    }

 /// Fixture: a store pre-populated with a fuel-supply table is unchanged after
 /// FuelControlStrategy runs — matching the canonical Java no-op behavior with
 /// pct_diff = 0% < 1%.
    #[test]
    fn pre_run_leaves_existing_tables_unchanged() {
        let s = FuelControlStrategy::new();
        let mut store = InMemoryStore::new();
        let df = DataFrame::new(
            2,
            vec![
                Series::new("fuelTypeID".into(), [1i32, 2]).into(),
                Series::new("marketShare".into(), [0.6_f64, 0.4]).into(),
            ],
        )
        .unwrap();
        store.insert("fuelSupply", df.clone());

        s.pre_run(&mut store).expect("pre_run must not fail");

        let after = store
            .get("fuelSupply")
            .expect("fuelSupply must still exist");
        assert_eq!(
            after.shape(),
            df.shape(),
            "fuelSupply table must be unchanged after FuelControlStrategy (pct_diff = 0)"
        );
    }

    #[test]
    fn is_trait_object_safe() {
        let s: Box<dyn InternalControlStrategy> = Box::new(FuelControlStrategy::new());
        assert_eq!(s.name(), "FuelControlStrategy");
    }
}

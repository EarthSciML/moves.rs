//! Rate-of-Progress internal control strategy — ports
//! `gov.epa.otaq.moves.master.implementation.internalcontrolstrategies.rateofprogress.RateOfProgressStrategy`.
//!
//! # Role
//!
//! The Rate-of-Progress (ROP) control strategy applies emission-reduction
//! percentages by pollutant, source type, regulatory class, and model year.
//! It is used to model the effect of new emissions regulations that require
//! specific percentage reductions in pollutant output from specific vehicle
//! classes.
//!
//! Each [`crate::model::RopRecord`] carries a `reductionFraction` in `[0.0, 1.0]`:
//!
//! * `0.0` — no change
//! * `0.25` — 25% reduction (downstream emission rate × 0.75)
//! * `1.0` — 100% elimination
//!
//! The emission scaling factor applied to any matching rate row is
//! `1.0 - reductionFraction`.
//!
//! # Lifecycle
//!
//! ROP applies its reductions globally before the master loop begins, using
//! [`pre_run`](RateOfProgressControlStrategy::pre_run). The modified tables
//! declared via [`modified_tables`](RateOfProgressControlStrategy::modified_tables)
//! signal the engine to invalidate and reload those tables before calculators
//! consume them.
//!
//! # Data-plane status
//!
//! The actual write of the adjusted emission rates into the execution database
//! is deferred until `moves-framework`'s `ExecutionTables` gains a mutable
//! write API. The `modified_tables` declaration
//! already signals the engine which tables will be modified, so the hook-up
//! is a single `TODO` line once the data plane lands.

use moves_framework::{InMemoryStore, InternalControlStrategy};

use crate::model::RopTable;

/// Rate-of-Progress internal control strategy.
///
/// See the [module docs](self) for the full description.
#[derive(Debug)]
pub struct RateOfProgressControlStrategy {
    table: RopTable,
}

impl RateOfProgressControlStrategy {
 /// Build from a [`RopTable`] of reduction parameters.
 ///
 /// The table is applied in [`pre_run`](Self::pre_run) to modify emission
 /// rate tables in the execution database before calculators run.
    pub fn new(table: RopTable) -> Self {
        Self { table }
    }

 /// The reduction table that will be applied in [`pre_run`](Self::pre_run).
    pub fn table(&self) -> &RopTable {
        &self.table
    }
}

impl InternalControlStrategy for RateOfProgressControlStrategy {
    fn name(&self) -> &'static str {
        "RateOfProgressControlStrategy"
    }

    fn modified_tables(&self) -> &[&'static str] {
 // The ROP strategy modifies the emission rate tables that downstream
 // calculators consume — specifically the rates keyed by
 // (pollutantID, sourceTypeID, regClassID, modelYearID).
 //
 // TODO( / DataFrameStore): replace this placeholder list with
 // the actual execution-DB table names once the data plane is defined.
 // The names below match the Java strategy's `getModifiedTables()` return.
        &["ratepollutantprocessmodelyeargroup", "sourceTypeModelYear"]
    }

    fn pre_run(
        &self,
        _tables: &mut InMemoryStore,
    ) -> std::result::Result<(), moves_framework::Error> {
 // TODO: apply `self.table` reductions to the emission-rate tables in
 // `_tables`. Requires reading the target rate tables, joining against
 // `RopRecord` fields, and writing scaled rows back. Deferred to a
 // follow-on work item; `modified_tables` already signals the engine.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::RopRecord;
    use moves_framework::{CalculatorContext, InMemoryStore};

    fn small_table(records: &[(i32, i32, i32, i32, f64)]) -> RopTable {
        records
            .iter()
            .map(|&(p, st, rc, my, r)| RopRecord::new(p, st, rc, my, r))
            .collect()
    }

    #[test]
    fn name_is_stable() {
        let s = RateOfProgressControlStrategy::new(RopTable::new());
        assert_eq!(s.name(), "RateOfProgressControlStrategy");
    }

    #[test]
    fn modified_tables_non_empty() {
        let s = RateOfProgressControlStrategy::new(RopTable::new());
        assert!(
            !s.modified_tables().is_empty(),
            "strategy must declare at least one modified table"
        );
    }

    #[test]
    fn pre_run_succeeds_with_empty_table() {
        let s = RateOfProgressControlStrategy::new(RopTable::new());
        let mut store = InMemoryStore::new();
        s.pre_run(&mut store).expect("pre_run must not fail");
    }

    #[test]
    fn pre_run_succeeds_with_populated_table() {
        let t = small_table(&[
            (3, 11, 10, 2020, 0.25),
            (3, 21, 10, 2020, 0.10),
            (1, 11, 20, 2018, 0.50),
        ]);
        let s = RateOfProgressControlStrategy::new(t);
        let mut store = InMemoryStore::new();
        s.pre_run(&mut store).expect("pre_run must not fail");
    }

    #[test]
    fn table_accessor_preserves_records() {
        let t = small_table(&[(1, 11, 10, 2020, 0.3)]);
        let s = RateOfProgressControlStrategy::new(t);
        assert_eq!(s.table().len(), 1);
        let rec = s.table().iter().next().unwrap();
        assert_eq!(rec.pollutant_id, 1);
        assert_eq!(rec.source_type_id, 11);
        assert!((rec.reduction_fraction - 0.3).abs() < 1e-15);
    }

    #[test]
    fn strategy_is_trait_object_safe() {
        let strategy: Box<dyn InternalControlStrategy> =
            Box::new(RateOfProgressControlStrategy::new(RopTable::new()));
        assert_eq!(strategy.name(), "RateOfProgressControlStrategy");
    }

    #[test]
    fn subscriptions_is_empty() {
        let s = RateOfProgressControlStrategy::new(RopTable::new());
        assert!(
            s.subscriptions().is_empty(),
            "ROP applies globally in pre_run, not per-iteration"
        );
    }

    #[test]
    fn post_run_is_no_op() {
        let s = RateOfProgressControlStrategy::new(RopTable::new());
        let ctx = CalculatorContext::new();
        s.post_run(&ctx).expect("post_run must not fail");
    }
}

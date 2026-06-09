//! Rate-of-Progress internal control strategy — ports
//! `gov.epa.otaq.moves.master.implementation.ghg.internalcontrolstrategies.rateofprogress.RateOfProgressStrategy`.
//!
//! # Role
//!
//! In canonical MOVES the Rate-of-Progress (ROP) strategy does **not** apply
//! per-row percentage reductions. When enabled it runs
//! `database/RateOfProgressStrategy.sql`, which "removes the effects of the
//! Clean Air Act by propagating 1993 emission rates into the future": it locates
//! the `RateOfProgress` model-year cut point (default 1993), rewrites future
//! calendar fuel years, and then propagates the cut-point model-year-group
//! values forward across roughly two dozen execution-DB tables (`baseFuel`,
//! `crankcaseEmissionRatio`, `cumTVVCoeffs`, `fuelSupply`, `fuelUsageFraction`,
//! `generalFuelRatioExpression`, `meanFuelParameters`, `noNO2Ratio`,
//! `pollutantProcessModelYear`, `sourceTypeModelYear`,
//! `sourceTypeModelYearGroup`, `startTempAdjustment`, the various emission-rate
//! and air-toxic ratio tables, etc.). See
//! `RateOfProgressStrategy.runScript()` and the `spDoRateOfProgress` procedure
//! in `database/RateOfProgressStrategy.sql`.
//!
//! # Implementation status
//!
//! That transformation has not been ported. It depends on default-DB tables
//! and helper structures (`modelYearCutPoints`, `modelYearGroup` decoding, the
//! fuel-year ripple logic) that this crate's in-memory data plane does not yet
//! expose, so [`pre_run`](RateOfProgressControlStrategy::pre_run) cannot
//! faithfully reproduce it. Until those tables and the model-year-group decode
//! step land, the strategy reports the unported condition as an explicit error
//! rather than silently returning success and leaving every advertised
//! [`modified_tables`](RateOfProgressControlStrategy::modified_tables) entry
//! untouched.
//!
//! Note: the [`crate::model::RopRecord`] / `reductionFraction` data model in
//! this crate is a placeholder that does not correspond to any field used by
//! the canonical strategy; it must not be wired into a per-row rate scaling,
//! which canonical MOVES never performs.

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
        // Tables the canonical `spDoRateOfProgress` procedure rewrites when the
        // Clean-Air-Act effect is removed (see `database/RateOfProgressStrategy.sql`).
        // This is the subset most material to downstream calculators; the full
        // procedure touches additional fuel and air-toxic ratio tables. The list
        // is advisory metadata for engine invalidation — `pre_run` does not yet
        // perform these rewrites (see below).
        &[
            "baseFuel",
            "fuelSupply",
            "fuelUsageFraction",
            "pollutantProcessModelYear",
            "sourceTypeModelYear",
            "sourceTypeModelYearGroup",
        ]
    }

    fn pre_run(
        &self,
        _tables: &mut InMemoryStore,
    ) -> std::result::Result<(), moves_framework::Error> {
        // Canonical MOVES runs `spDoRateOfProgress` here, propagating the
        // RateOfProgress cut-point (1993) model-year-group data forward across
        // the execution-DB tables listed in `modified_tables`. That port depends
        // on default-DB structures this crate does not yet expose
        // (`modelYearCutPoints`, model-year-group decoding, the fuel-year ripple).
        //
        // Rather than silently returning success — which would leave every
        // advertised modified table untouched and drop the entire control effect
        // while the run reports OK — surface the unported path as an explicit
        // error so callers cannot mistake a no-op for an applied strategy.
        Err(moves_framework::Error::NotImplemented)
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
    fn pre_run_reports_unported_with_empty_table() {
        // The Clean-Air-Act removal procedure is not ported; `pre_run` must
        // surface that explicitly rather than silently returning success.
        let s = RateOfProgressControlStrategy::new(RopTable::new());
        let mut store = InMemoryStore::new();
        let err = s
            .pre_run(&mut store)
            .expect_err("pre_run must report the unported path, not silently succeed");
        assert!(matches!(err, moves_framework::Error::NotImplemented));
    }

    #[test]
    fn pre_run_reports_unported_with_populated_table() {
        let t = small_table(&[
            (3, 11, 10, 2020, 0.25),
            (3, 21, 10, 2020, 0.10),
            (1, 11, 20, 2018, 0.50),
        ]);
        let s = RateOfProgressControlStrategy::new(t);
        let mut store = InMemoryStore::new();
        let err = s
            .pre_run(&mut store)
            .expect_err("pre_run must report the unported path, not silently succeed");
        assert!(matches!(err, moves_framework::Error::NotImplemented));
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

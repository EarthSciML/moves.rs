//! `InternalControlStrategy` trait — the contract MOVES control strategies implement.
//!
//! Ports `gov.epa.otaq.moves.master.framework.InternalControlStrategy` from the Java
//! MOVES source.
//!
//! # Control strategies vs. calculators
//!
//! Control strategies modify input tables before emission calculators consume them.
//! They run at `INTERNAL_CONTROL_STRATEGY` priority (1000), which is higher than
//! `GENERATOR` (100) and `EMISSION_CALCULATOR` (10), so they fire first at every
//! shared granularity bucket. Each strategy declares the tables it modifies via
//! [`modified_tables`](InternalControlStrategy::modified_tables) so the input data
//! manager can invalidate and reload those tables after
//! [`pre_run`](InternalControlStrategy::pre_run) completes.
//!
//! # Lifecycle
//!
//! The engine calls lifecycle hooks in this order:
//!
//! 1. [`pre_run`](InternalControlStrategy::pre_run) — once before the first
//!    master-loop iteration. Strategies use this to load and transform global input
//!    tables (e.g. AVFT applies fleet-composition changes to the `AVFT` table here).
//!
//! 2. [`execute`](InternalControlStrategy::execute) — once per subscribed master-loop
//!    iteration, at the granularity and process each [`StrategySubscription`] specifies.
//!    Strategies use this for per-location or per-time modifications (e.g. retrofit
//!    reduction factors applied per county per year).
//!
//! 3. [`post_run`](InternalControlStrategy::post_run) — once after the last master-loop
//!    iteration completes. Typically used for cleanup or summary reporting.
//!
//! `pre_run` and `post_run` run outside the parallel-chunk section of the engine —
//! they are called from a single thread. `execute` may be called concurrently from
//! multiple threads when the engine runs calculator chains in parallel; strategy
//! implementations must be `Sync` (already enforced by the trait bound) and must
//! internally synchronise any mutable state they access from `execute`.

use moves_calculator_info::{Granularity, Priority};
use moves_data::ProcessId;

use crate::calculator::CalculatorContext;
use crate::data::InMemoryStore;
use crate::error::Error;

/// One master-loop subscription declared by a control strategy.
///
/// Mirrors [`crate::CalculatorSubscription`] but always runs at
/// `INTERNAL_CONTROL_STRATEGY` priority. The `priority_offset` field
/// supports fine-tuning within the band (e.g. `INTERNAL_CONTROL_STRATEGY+5`
/// for a strategy that must run before others at the same granularity).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StrategySubscription {
    /// MOVES process this subscription is gated to.
    pub process_id: ProcessId,
    /// Granularity bucket the subscription fires in.
    pub granularity: Granularity,
    /// Offset added to the `INTERNAL_CONTROL_STRATEGY` base (1000).
    /// Use `0` for the default; positive values run before others
    /// in the same granularity band.
    pub priority_offset: i32,
}

impl StrategySubscription {
    /// Construct with explicit priority offset.
    #[must_use]
    pub fn new(process_id: ProcessId, granularity: Granularity, priority_offset: i32) -> Self {
        Self {
            process_id,
            granularity,
            priority_offset,
        }
    }

    /// Construct with zero priority offset — the most common case.
    #[must_use]
    pub fn at(process_id: ProcessId, granularity: Granularity) -> Self {
        Self::new(process_id, granularity, 0)
    }

    /// Computed MasterLoop priority integer value.
    ///
    /// Equals `INTERNAL_CONTROL_STRATEGY` base (1000) plus any offset. The
    /// master loop sorts higher values first within a granularity bucket.
    #[must_use]
    pub fn priority(&self) -> i32 {
        // INTERNAL_CONTROL_STRATEGY base = 1000 (from PriorityBase::base_value).
        Priority::parse("INTERNAL_CONTROL_STRATEGY")
            .expect("known-good constant")
            .value()
            + self.priority_offset
    }
}

/// Contract every MOVES internal control strategy implements.
///
/// Ports `gov.epa.otaq.moves.master.framework.InternalControlStrategy`.
///
/// An `InternalControlStrategy` is a stateless value type — the registry
/// instantiates one per run and reuses it across all iterations. All
/// run-varying inputs flow in via the [`CalculatorContext`] argument; any
/// mutable per-run bookkeeping must be held behind interior mutability with
/// appropriate synchronisation (see lifecycle notes above).
pub trait InternalControlStrategy: Send + Sync + std::fmt::Debug {
    /// Stable identifier used for registration and diagnostic logging.
    fn name(&self) -> &'static str;

    /// Master-loop subscriptions this strategy fires at per-iteration.
    ///
    /// Returns an empty slice for strategies that only use `pre_run` / `post_run`
    /// and do not need per-iteration callbacks.
    fn subscriptions(&self) -> &[StrategySubscription] {
        &[]
    }

    /// Default-DB tables this strategy reads from or writes to.
    ///
    /// The engine inspects this list after calling `pre_run` to know which
    /// tables must be invalidated and reloaded before calculators see them.
    /// Tables absent from this list are assumed unmodified.
    fn modified_tables(&self) -> &[&'static str] {
        &[]
    }

    /// Called once before the master loop begins.
    ///
    /// Use this for global input-table transformations that apply for the
    /// entire run — for example, the AVFT strategy writes its completed
    /// fleet-composition table into `tables` as `"AVFT"` here so downstream
    /// calculators see the user-specified fractions instead of the defaults.
    ///
    /// `tables` is the mutable slow-tier execution database. Write to it via
    /// `InMemoryStore::insert` or the [`crate::DataFrameStoreTyped`] helpers.
    ///
    /// Default: no-op.
    fn pre_run(&self, _tables: &mut InMemoryStore) -> Result<(), Error> {
        Ok(())
    }

    /// Called once per subscribed master-loop iteration at the granularity
    /// and process registered in [`subscriptions`](Self::subscriptions).
    ///
    /// Use this for per-location or per-time table modifications. May be
    /// called concurrently when the engine runs calculator chains in
    /// parallel — implementations that touch shared state must synchronise.
    ///
    /// Default: no-op.
    fn execute(&self, _ctx: &CalculatorContext) -> Result<(), Error> {
        Ok(())
    }

    /// Called once after all master-loop iterations complete.
    ///
    /// Use for cleanup or summary output. Default: no-op.
    fn post_run(&self, _ctx: &CalculatorContext) -> Result<(), Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use moves_calculator_info::Granularity;
    use moves_data::ProcessId;

    #[derive(Debug)]
    struct NoOpStrategy;

    impl InternalControlStrategy for NoOpStrategy {
        fn name(&self) -> &'static str {
            "NoOpStrategy"
        }
    }

    #[test]
    fn no_op_strategy_default_methods() {
        let s = NoOpStrategy;
        assert_eq!(s.name(), "NoOpStrategy");
        assert!(s.subscriptions().is_empty());
        assert!(s.modified_tables().is_empty());
        let mut store = InMemoryStore::new();
        s.pre_run(&mut store).expect("pre_run ok");
        let ctx = CalculatorContext::new();
        s.execute(&ctx).expect("execute ok");
        s.post_run(&ctx).expect("post_run ok");
    }

    #[derive(Debug)]
    struct FullStrategy;

    static FULL_SUBS: &[StrategySubscription] = &[StrategySubscription {
        process_id: ProcessId(1),
        granularity: Granularity::County,
        priority_offset: 0,
    }];
    static FULL_TABLES: &[&str] = &["AVFT", "sourceTypeYear"];

    impl InternalControlStrategy for FullStrategy {
        fn name(&self) -> &'static str {
            "FullStrategy"
        }
        fn subscriptions(&self) -> &[StrategySubscription] {
            FULL_SUBS
        }
        fn modified_tables(&self) -> &[&'static str] {
            FULL_TABLES
        }
    }

    #[test]
    fn full_strategy_metadata() {
        let s = FullStrategy;
        assert_eq!(s.subscriptions().len(), 1);
        assert_eq!(s.subscriptions()[0].process_id, ProcessId(1));
        assert_eq!(s.subscriptions()[0].granularity, Granularity::County);
        assert_eq!(s.modified_tables(), &["AVFT", "sourceTypeYear"]);
    }

    #[test]
    fn strategy_subscription_priority_default_offset() {
        let sub = StrategySubscription::at(ProcessId(2), Granularity::Hour);
        assert_eq!(sub.process_id, ProcessId(2));
        assert_eq!(sub.granularity, Granularity::Hour);
        assert_eq!(sub.priority_offset, 0);
        assert_eq!(sub.priority(), 1000);
    }

    #[test]
    fn strategy_subscription_priority_with_positive_offset() {
        let sub = StrategySubscription::new(ProcessId(1), Granularity::County, 5);
        assert_eq!(sub.priority(), 1005);
    }

    #[test]
    fn strategy_subscription_is_copy_and_eq() {
        let a = StrategySubscription::at(ProcessId(1), Granularity::County);
        let b = a;
        assert_eq!(a, b);
    }

    #[test]
    fn strategy_can_be_trait_object() {
        let strategies: Vec<Box<dyn InternalControlStrategy>> =
            vec![Box::new(NoOpStrategy), Box::new(FullStrategy)];
        assert_eq!(strategies[0].name(), "NoOpStrategy");
        assert_eq!(strategies[1].name(), "FullStrategy");
    }
}

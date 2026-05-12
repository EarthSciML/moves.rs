//! MasterLoop subscription model — granularity, subscriptions, and the
//! [`MasterLoopable`] trait.
//!
//! Ports the trio of Java types from
//! `gov.epa.otaq.moves.master.framework`:
//!
//! * `MasterLoopGranularity` → [`Granularity`], re-exported from
//!   [`moves_calculator_info`]. Phase 1 already encoded the constants and
//!   the [`Granularity::execution_index`] sort key.
//! * `MasterLoopable` → [`MasterLoopable`] trait.
//! * `MasterLoopableSubscription` → [`MasterLoopableSubscription`] record
//!   with [`Ord`] matching Java's `compareTo`.
//!
//! The execution dispatch loop (`MasterLoop`, `notifyLoopablesOfLoopChange`)
//! lands in Task 20 / Task 21. This module defines the foundation those
//! tasks build on, and is sufficient to drive `MasterLoopableSubscription`
//! ordering — the regression check called out in the migration plan.
//!
//! # Ordering invariants
//!
//! [`MasterLoopableSubscription::cmp`] reproduces
//! [`MasterLoopableSubscription.compareTo`](https://github.com/USEPA/EPA_MOVES_Model/blob/HEAD/gov/epa/otaq/moves/master/framework/MasterLoopableSubscription.java)
//! exactly, except for the identity tie-break (Java uses
//! `Object.hashCode()`; we use the loopable's `Arc` data-pointer address,
//! which gives the same "arbitrary-but-stable within one run" semantics).
//!
//! Sort order, smallest first:
//!
//! 1. Granularity, coarse → fine. PROCESS first; HOUR next-to-last;
//!    MATCH_FINEST last. See [`Granularity::execution_index`].
//! 2. Priority, high → low. Higher integer priorities fire earlier inside
//!    the same granularity bucket.
//! 3. Loopable identity. Two subscriptions of the same [`Arc`]ed loopable
//!    compare equal; subscriptions of different loopables get a stable
//!    non-equal ordering. The relative order between different loopables
//!    is unspecified (same as Java).

use std::cmp::Ordering;
use std::sync::Arc;

use crate::error::Error;

pub use moves_calculator_info::Granularity;

/// Snapshot of where the MasterLoop is currently iterating — process,
/// location, year/month/day/hour, granularity, and priority.
/// [`MasterLoopable::execute_at_granularity`] receives one of these so the
/// subscriber knows the context for its current invocation.
///
/// **Phase 2 skeleton.** Only the fields needed to define the trait
/// signature are populated here; the rest land in Task 20 (MasterLoop
/// iteration state) and Task 23 (`CalculatorContext`).
#[derive(Debug, Clone, Default)]
pub struct MasterLoopContext {
    /// Granularity bucket the loop is currently firing. `None` while the
    /// loop is initialising.
    pub execution_granularity: Option<Granularity>,
    /// Priority of the subscription firing this context. Higher integers
    /// fire earlier inside a granularity bucket.
    pub execution_priority: i32,
    /// `false` for the forward pass (calls
    /// [`MasterLoopable::execute_at_granularity`]); `true` for the reverse
    /// pass (calls [`MasterLoopable::clean_data_loop`]).
    pub is_clean_up: bool,
}

/// Implemented by every object that participates in MasterLoop iteration.
/// Ports [`MasterLoopable`](https://github.com/USEPA/EPA_MOVES_Model/blob/HEAD/gov/epa/otaq/moves/master/framework/MasterLoopable.java).
///
/// Generators and emission calculators (Phase 2 Task 18) are the principal
/// implementors. Java's `subscribeToMe(MasterLoop)` is intentionally not
/// part of the trait: the Rust port drives subscription through
/// `CalculatorRegistry` (Task 19), not by asking each loopable to
/// self-register.
///
/// Java's `executeLoop` is renamed [`MasterLoopable::execute_at_granularity`]
/// per the migration plan (Task 21).
pub trait MasterLoopable: Send + Sync + std::fmt::Debug {
    /// Run the subscriber for the current loop context. Fires once per
    /// iteration at the granularity the subscription was registered at.
    fn execute_at_granularity(&self, context: &MasterLoopContext) -> Result<(), Error>;

    /// Cleanup pass for data created during
    /// [`MasterLoopable::execute_at_granularity`]. Most emission calculators
    /// have an empty implementation; generators override this to drop their
    /// per-iteration scratch tables. Default no-op, matching the Java-side
    /// convention.
    fn clean_data_loop(&self, _context: &MasterLoopContext) -> Result<(), Error> {
        Ok(())
    }
}

/// One registration of a [`MasterLoopable`] at a specific
/// `(granularity, priority)`. A loopable may register multiple
/// subscriptions — for example, an emission calculator that handles two
/// pollutants at the same granularity but different priorities holds two
/// subscriptions.
///
/// Ports `gov.epa.otaq.moves.master.framework.MasterLoopableSubscription`.
///
/// The `priority` field stores the raw integer value (matching the Java
/// field). The canonical named values
/// ([`moves_calculator_info::Priority`]) are decoded at the call site
/// when subscribing.
#[derive(Debug, Clone)]
pub struct MasterLoopableSubscription {
    /// Granularity bucket this subscription fires in.
    pub granularity: Granularity,
    /// Priority within the granularity bucket. Higher fires earlier.
    pub priority: i32,
    /// The loopable to fire.
    pub loopable: Arc<dyn MasterLoopable>,
}

impl MasterLoopableSubscription {
    /// Construct a subscription from its three components.
    pub fn new(granularity: Granularity, priority: i32, loopable: Arc<dyn MasterLoopable>) -> Self {
        Self {
            granularity,
            priority,
            loopable,
        }
    }
}

impl PartialEq for MasterLoopableSubscription {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for MasterLoopableSubscription {}

impl PartialOrd for MasterLoopableSubscription {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MasterLoopableSubscription {
    /// Java-faithful ordering. See module docs for the full invariant.
    fn cmp(&self, other: &Self) -> Ordering {
        let g = self
            .granularity
            .execution_index()
            .cmp(&other.granularity.execution_index());
        if g != Ordering::Equal {
            return g;
        }
        // Higher priority sorts first → reverse natural integer order.
        // Java: `other.priority - this.priority`.
        let p = other.priority.cmp(&self.priority);
        if p != Ordering::Equal {
            return p;
        }
        // Identity tie-break. Java compares `Object.hashCode()`, which for
        // objects without an overridden `hashCode` is a JVM-assigned
        // identity hash; the Arc data-pointer address gives the equivalent
        // stable-within-a-run semantics in Rust. Cast to `*const ()` to
        // strip the vtable half of the `dyn` fat pointer — fat pointers
        // don't impl `Ord` directly.
        let lhs = Arc::as_ptr(&self.loopable) as *const ();
        let rhs = Arc::as_ptr(&other.loopable) as *const ();
        lhs.cmp(&rhs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// A minimal `MasterLoopable` used to drive subscription-ordering
    /// tests. The body never runs — these tests exercise the `compareTo`
    /// port, not the iteration loop (Task 20).
    #[derive(Debug)]
    struct DummyLoopable;

    impl MasterLoopable for DummyLoopable {
        fn execute_at_granularity(&self, _ctx: &MasterLoopContext) -> Result<(), Error> {
            Ok(())
        }
    }

    /// Java's helper: maps a signed `int` to `-1 / 0 / 1`. Lets us assert
    /// `compareTo` polarity without depending on the specific magnitude
    /// the implementation returns.
    fn unit(ord: Ordering) -> i32 {
        match ord {
            Ordering::Less => -1,
            Ordering::Equal => 0,
            Ordering::Greater => 1,
        }
    }

    fn dummy() -> Arc<dyn MasterLoopable> {
        Arc::new(DummyLoopable) as Arc<dyn MasterLoopable>
    }

    // ---- Direct ports of MasterLoopableSubscriptionTest.testCompareTo ----

    #[test]
    fn loopable_equals_itself() {
        let l = dummy();
        let a = MasterLoopableSubscription::new(Granularity::Hour, 10, l.clone());
        let b = MasterLoopableSubscription::new(Granularity::Hour, 10, l);
        assert_eq!(
            a.cmp(&b),
            Ordering::Equal,
            "subscriptions of the same loopable at the same (granularity, priority) should compare equal"
        );
        // Symmetry (trivially, since both are Equal).
        assert_eq!(unit(a.cmp(&b)), -unit(b.cmp(&a)));
    }

    #[test]
    fn different_loopables_compare_non_equal_and_antisymmetric() {
        let a = MasterLoopableSubscription::new(Granularity::Hour, 10, dummy());
        let b = MasterLoopableSubscription::new(Granularity::Hour, 10, dummy());
        assert_ne!(
            a.cmp(&b),
            Ordering::Equal,
            "different loopables should never compare equal even at matching (granularity, priority)"
        );
        assert_eq!(unit(a.cmp(&b)), -unit(b.cmp(&a)), "antisymmetry");
    }

    #[test]
    fn day_sorts_before_hour() {
        // Java: subscriptionA.granularity = HOUR, subscriptionB.granularity = DAY
        //       expected: A.compareTo(B) = +1 (A "greater", so B sorts first).
        let l = dummy();
        let hour = MasterLoopableSubscription::new(Granularity::Hour, 10, l.clone());
        let day = MasterLoopableSubscription::new(Granularity::Day, 10, l);
        assert_eq!(
            unit(hour.cmp(&day)),
            1,
            "HOUR sub should compare greater than DAY sub (DAY fires first)"
        );
        assert_eq!(unit(hour.cmp(&day)), -unit(day.cmp(&hour)));
    }

    #[test]
    fn higher_priority_sorts_first() {
        // Java: subscriptionA.priority = 100, subscriptionB.priority = 10
        //       expected: A.compareTo(B) = -1 (A "less", A fires first).
        let l = dummy();
        let hi = MasterLoopableSubscription::new(Granularity::Hour, 100, l.clone());
        let lo = MasterLoopableSubscription::new(Granularity::Hour, 10, l);
        assert_eq!(
            unit(hi.cmp(&lo)),
            -1,
            "priority 100 should sort before priority 10"
        );
        assert_eq!(unit(hi.cmp(&lo)), -unit(lo.cmp(&hi)));
    }

    // ---- Additional invariants the Java test commented out covers ----

    #[test]
    fn process_granularity_fires_before_hour() {
        // Java iteration order: PROCESS (coarsest) first, HOUR (finest)
        // near-last.
        let l = dummy();
        let process = MasterLoopableSubscription::new(Granularity::Process, 10, l.clone());
        let hour = MasterLoopableSubscription::new(Granularity::Hour, 10, l);
        assert_eq!(unit(process.cmp(&hour)), -1);
    }

    #[test]
    fn match_finest_sorts_after_every_real_granularity() {
        // MATCH_FINEST is the sentinel — it pins a calculator to the finest
        // granularity in play, firing after everything else at its level.
        let l = dummy();
        let mf = MasterLoopableSubscription::new(Granularity::MatchFinest, 10, l.clone());
        for g in [
            Granularity::Process,
            Granularity::State,
            Granularity::County,
            Granularity::Zone,
            Granularity::Link,
            Granularity::Year,
            Granularity::Month,
            Granularity::Day,
            Granularity::Hour,
        ] {
            let sub = MasterLoopableSubscription::new(g, 10, l.clone());
            assert_eq!(
                unit(sub.cmp(&mf)),
                -1,
                "{g:?} subscription should sort before MATCH_FINEST"
            );
        }
    }

    #[test]
    fn sorting_yields_coarse_first_iteration_order() {
        // Build one subscription per granularity, shuffle (by construction
        // order), sort, and assert we get the Java MasterLoop iteration
        // sequence: PROCESS → STATE → COUNTY → ZONE → LINK → YEAR →
        // MONTH → DAY → HOUR → MATCH_FINEST.
        let l = dummy();
        let mut subs: Vec<_> = [
            Granularity::Hour,
            Granularity::Day,
            Granularity::Month,
            Granularity::Year,
            Granularity::Link,
            Granularity::Zone,
            Granularity::County,
            Granularity::State,
            Granularity::Process,
            Granularity::MatchFinest,
        ]
        .into_iter()
        .map(|g| MasterLoopableSubscription::new(g, 10, l.clone()))
        .collect();
        subs.sort();
        let order: Vec<Granularity> = subs.iter().map(|s| s.granularity).collect();
        assert_eq!(
            order,
            vec![
                Granularity::Process,
                Granularity::State,
                Granularity::County,
                Granularity::Zone,
                Granularity::Link,
                Granularity::Year,
                Granularity::Month,
                Granularity::Day,
                Granularity::Hour,
                Granularity::MatchFinest,
            ],
        );
    }

    #[test]
    fn within_one_granularity_higher_priority_first() {
        // Two subscriptions at the same granularity, different priorities.
        // Sort should put the high-priority one first.
        let l = dummy();
        let mut subs = [
            MasterLoopableSubscription::new(Granularity::Month, 10, l.clone()),
            MasterLoopableSubscription::new(Granularity::Month, 100, l.clone()),
            MasterLoopableSubscription::new(Granularity::Month, 1000, l.clone()),
            MasterLoopableSubscription::new(Granularity::Month, 50, l),
        ];
        subs.sort();
        let prios: Vec<i32> = subs.iter().map(|s| s.priority).collect();
        assert_eq!(prios, vec![1000, 100, 50, 10]);
    }

    #[test]
    fn cross_granularity_priority_does_not_invert_granularity_order() {
        // Granularity always wins over priority. A high-priority HOUR
        // subscription still sorts after a low-priority PROCESS one.
        let l = dummy();
        let hi_hour = MasterLoopableSubscription::new(Granularity::Hour, 10_000, l.clone());
        let lo_process = MasterLoopableSubscription::new(Granularity::Process, 1, l);
        assert_eq!(unit(lo_process.cmp(&hi_hour)), -1);
    }

    // ---- Trait sanity ----

    #[test]
    fn default_clean_data_loop_is_no_op() {
        let dummy = DummyLoopable;
        let ctx = MasterLoopContext::default();
        // Should not panic; should return Ok.
        dummy.clean_data_loop(&ctx).unwrap();
    }
}

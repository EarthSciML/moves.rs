//! MasterLoop subscription model and iteration engine.
//!
//! Ports the quartet of Java types from
//! `gov.epa.otaq.moves.master.framework`:
//!
//! * `MasterLoopGranularity` → [`Granularity`], re-exported from
//!   [`moves_calculator_info`]. Phase 1 already encoded the constants and
//!   the [`Granularity::execution_index`] sort key.
//! * `MasterLoopable` → [`MasterLoopable`] trait.
//! * `MasterLoopableSubscription` → [`MasterLoopableSubscription`] record
//!   with [`Ord`] matching Java's `compareTo`.
//! * `MasterLoop` → [`MasterLoop`] iteration engine (Task 20) walking
//!   `iteration → process → state → county → zone → link → year → month →
//!   day → hour` and dispatching subscribers on entry (forward pass) and
//!   exit (cleanup pass) at each level. The engine consumes
//!   [`crate::ExecutionLocation`] / [`crate::ExecutionTime`] values
//!   (Task 23) and writes the firing position into
//!   [`MasterLoopContext::position`].
//!
//! Task 21 (`notifyLoopablesOfLoopChange` / `hasLoopables`) refines the
//! dispatch inside [`MasterLoop::run`] with the `hasLoopables`
//! short-circuit: the `year → month → day → hour` nest descends only as
//! deep as the finest registered subscription, so a run whose
//! subscribers all sit above the time nest skips it entirely.
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
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::error::Error;
use crate::execution_db::{ExecutionLocation, ExecutionTime, IterationPosition};

pub use moves_calculator_info::Granularity;

/// Snapshot of where the MasterLoop is currently iterating — the
/// `(iteration, process, location, time)` triple plus the granularity,
/// priority, and forward-pass / cleanup-pass flag.
///
/// Updated in-place by [`MasterLoop`] before every notification fires.
/// Subscribers receive a borrowed `&MasterLoopContext` and read whichever
/// fields apply to their granularity — for example a `STATE`-granularity
/// subscriber reads `position.process_id` and `position.location.state_id`
/// and ignores the rest. Location / time components below the firing
/// granularity hold `None`, matching the [`ExecutionLocation`] /
/// [`ExecutionTime`] convention from Task 23.
///
/// The Task 19 adapter that bridges [`MasterLoopable`] to
/// [`crate::Calculator`] reads `position` here and builds the
/// corresponding [`crate::CalculatorContext`] for the calculator
/// callback.
#[derive(Debug, Clone, Default)]
pub struct MasterLoopContext {
    /// Current iteration counter, process, location, and time. Updated
    /// in place as [`MasterLoop`] descends and ascends each level.
    pub position: IterationPosition,
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

/// Nested-loop iteration engine driving a sorted list of
/// [`MasterLoopableSubscription`]s through every
/// `(iteration, process, location, time)` combination.
///
/// Ports the iteration scaffolding of
/// [`MasterLoop.java`](https://github.com/USEPA/EPA_MOVES_Model/blob/HEAD/gov/epa/otaq/moves/master/framework/MasterLoop.java).
/// The Java code interleaves the iteration scaffolding with `MOVESThread`-
/// based parallel bundle execution; the Rust port keeps only the
/// scaffolding. The migration plan calls that split out explicitly: the
/// bundle-level parallelism is Phase 4 territory and is replaced by
/// structured concurrency at the engine layer (Task 25), not here.
///
/// # Iteration order
///
/// [`MasterLoop::run`] walks the nested loop:
///
/// 1. `iteration` — `0..self.iterations` (rare; almost always one
///    iteration, matching `MOVESEngine.numIterations`).
/// 2. `process` — every [`moves_data::ProcessId`] in
///    [`MasterLoop::processes`].
/// 3. `state` → `county` → `zone` → `link` — grouped from
///    [`MasterLoop::locations`] by leading IDs.
/// 4. `year` → `month` → `day_id` → `hour` — grouped from
///    [`MasterLoop::times`] by leading components.
///
/// Each location / time level fires its subscribers on entry (forward
/// pass, [`MasterLoopable::execute_at_granularity`]) and on exit, after
/// all nested sub-iterations complete, in reverse priority order
/// (cleanup pass, [`MasterLoopable::clean_data_loop`]). `MatchFinest`
/// subscriptions pin to the deepest level in play (`HOUR` for inventory
/// runs) and fire after the `HOUR` subscriptions on each `HOUR`
/// iteration. The `hasLoopables` short-circuit (Task 21) trims the
/// time nest to the finest granularity anyone subscribes at: a run
/// whose subscribers all sit at `PROCESS` never enters the
/// `year → month → day → hour` nest at all. See the private
/// `MasterLoop::time_nest_depth` for the depth decision.
///
/// # Inputs and per-level views
///
/// Callers populate [`locations`](Self::locations) with fully-specified
/// [`ExecutionLocation`] values (every id `Some`, typically built via
/// [`ExecutionLocation::link`]) and [`times`](Self::times) with
/// fully-specified [`ExecutionTime`] values (every field `Some`, via
/// [`ExecutionTime::hour`]). The engine groups them by leading
/// components and writes a *narrowed* view into [`MasterLoopContext`]
/// at each level — a `STATE`-bucket subscriber sees
/// `ctx.position.location.state_id == Some(_)` with the other three ids
/// `None`, matching the [`ExecutionLocation`] convention. Groups iterate
/// in numeric ID order ([`std::collections::BTreeMap`] key order), which
/// gives a stable per-run sequence independent of input order. Duplicate
/// entries inside the same leaf bucket fire once per duplicate, matching
/// the Java behaviour when the location producer emits a repeated tuple.
///
/// # Error handling
///
/// If any subscription returns [`Err`], `run` returns immediately with
/// that error and runs no further notifications — including the paired
/// cleanup passes for levels already entered. This matches Java:
/// `MasterLoop.threadIterationGo` puts no `try`/`finally` around the
/// loop body, so a loopable exception propagates uncaught and the
/// cleanup notifications below it never fire. The Rust port needs no
/// cleanup-on-error safety net of its own — per-iteration scratch state
/// lives in [`crate::CalculatorContext`] `DataFrame`s that drop when the
/// erroring `run` unwinds.
#[derive(Debug)]
pub struct MasterLoop {
    /// Number of top-level iterations. Defaults to `1`. `0` causes
    /// [`run`](Self::run) to be a no-op.
    pub iterations: u32,
    /// Processes to iterate; the full `location × time` nest runs once
    /// per process.
    pub processes: Vec<moves_data::ProcessId>,
    /// Fully-specified locations to iterate (every id `Some`). Grouped
    /// at runtime into the `state → county → zone → link` nest.
    pub locations: Vec<ExecutionLocation>,
    /// Fully-specified times to iterate (every field `Some`). Grouped
    /// at runtime into the `year → month → day_id → hour` nest.
    pub times: Vec<ExecutionTime>,
    /// Sorted subscriptions. Maintained in
    /// [`MasterLoopableSubscription::cmp`] order by
    /// [`subscribe`](Self::subscribe).
    subscriptions: Vec<MasterLoopableSubscription>,
}

impl Default for MasterLoop {
    fn default() -> Self {
        Self::new()
    }
}

impl MasterLoop {
    /// Empty loop with `iterations = 1`. Populate `processes`,
    /// `locations`, `times`, and add subscriptions before calling
    /// [`run`](Self::run).
    #[must_use]
    pub fn new() -> Self {
        Self {
            iterations: 1,
            processes: Vec::new(),
            locations: Vec::new(),
            times: Vec::new(),
            subscriptions: Vec::new(),
        }
    }

    /// Register a subscription. The list stays sorted by
    /// [`MasterLoopableSubscription::cmp`] so the engine iterates
    /// subscribers in execution order without re-sorting per level.
    pub fn subscribe(&mut self, sub: MasterLoopableSubscription) {
        self.subscriptions.push(sub);
        self.subscriptions.sort();
    }

    /// Currently registered subscriptions in execution order. Mostly
    /// useful in tests and diagnostic dumps.
    #[must_use]
    pub fn subscriptions(&self) -> &[MasterLoopableSubscription] {
        &self.subscriptions
    }

    /// Drive the full nested iteration. Returns the first error any
    /// subscription's [`MasterLoopable::execute_at_granularity`] or
    /// [`MasterLoopable::clean_data_loop`] surfaces.
    ///
    /// `iterations = 0`, empty `processes`, empty `locations`, or empty
    /// `times` all collapse the nest at the corresponding level — the
    /// engine simply doesn't visit anything below that point.
    ///
    /// Input [`ExecutionLocation`]s and [`ExecutionTime`]s with `None`
    /// leading components are skipped at the grouping step (we can't
    /// place them in the nest without knowing where they go).
    pub fn run(&self) -> Result<(), Error> {
        let location_groups = group_locations(&self.locations);
        let time_groups = group_times(&self.times);
        // `hasLoopables` short-circuit (Task 21): probe the subscription
        // set once up front so `run_times` can skip every time level
        // below the finest subscriber. Subscriptions are fixed for the
        // life of a `run` (it borrows `&self`), so a single probe is
        // exact for every process and location.
        let depth = self.time_nest_depth();
        let mut ctx = MasterLoopContext::default();

        for iter_idx in 0..self.iterations {
            ctx.position = IterationPosition::start();
            ctx.position.iteration = iter_idx;
            for &process in &self.processes {
                ctx.position.process_id = Some(process);
                ctx.position.location = ExecutionLocation::none();
                ctx.position.time = ExecutionTime::none();
                self.notify_at(Granularity::Process, &mut ctx)?;
                self.run_locations(&location_groups, &time_groups, depth, &mut ctx)?;
                self.cleanup_at(Granularity::Process, &mut ctx)?;
            }
        }
        Ok(())
    }

    /// Walk the `state → county → zone → link` location nest, firing
    /// each level's subscribers and descending into `run_times` at the
    /// innermost `link`. `depth` is forwarded untouched to `run_times`;
    /// the location nest itself is never short-circuited, matching Java's
    /// `loopThroughProcess`, which always iterates every execution
    /// location.
    fn run_locations(
        &self,
        location_groups: &LocationGroups,
        time_groups: &TimeGroups,
        depth: TimeNestDepth,
        ctx: &mut MasterLoopContext,
    ) -> Result<(), Error> {
        for (&state_id, counties) in location_groups {
            ctx.position.location = ExecutionLocation::state(state_id);
            self.notify_at(Granularity::State, ctx)?;
            for (&county_id, zones) in counties {
                ctx.position.location = ExecutionLocation::county(state_id, county_id);
                self.notify_at(Granularity::County, ctx)?;
                for (&zone_id, links) in zones {
                    ctx.position.location = ExecutionLocation {
                        state_id: Some(state_id),
                        county_id: Some(county_id),
                        zone_id: Some(zone_id),
                        link_id: None,
                    };
                    self.notify_at(Granularity::Zone, ctx)?;
                    for &link_id in links {
                        ctx.position.location =
                            ExecutionLocation::link(state_id, county_id, zone_id, link_id);
                        self.notify_at(Granularity::Link, ctx)?;
                        self.run_times(time_groups, depth, ctx)?;
                        self.cleanup_at(Granularity::Link, ctx)?;
                    }
                    self.cleanup_at(Granularity::Zone, ctx)?;
                }
                self.cleanup_at(Granularity::County, ctx)?;
            }
            self.cleanup_at(Granularity::State, ctx)?;
        }
        Ok(())
    }

    /// Forward + cleanup walk of the `year → month → day → hour` time
    /// nest, gated by the `hasLoopables` short-circuit.
    ///
    /// `depth` (from `time_nest_depth`) caps how far the nest descends —
    /// a level runs only when some subscriber fires there or finer. This
    /// ports the `mustLoopOver*` guards in `MasterLoop.loopThroughTime`:
    /// Java wraps each `for` in `if (mustLoopOver…)`, the port wraps each
    /// in `if depth >= …`. A level's cleanup notification sits inside
    /// that level's guard, so a skipped level fires neither its forward
    /// nor its cleanup pass — matching Java, where
    /// `notifyLoopablesOfLoopChange(…, true)` is itself inside the
    /// `if (mustLoopOver…)` block.
    fn run_times(
        &self,
        time_groups: &TimeGroups,
        depth: TimeNestDepth,
        ctx: &mut MasterLoopContext,
    ) -> Result<(), Error> {
        if depth == TimeNestDepth::Skip {
            return Ok(());
        }
        for (&year, months) in time_groups {
            ctx.position.time = ExecutionTime::year(year);
            self.notify_at(Granularity::Year, ctx)?;
            if depth >= TimeNestDepth::Month {
                for (&month, days) in months {
                    ctx.position.time = ExecutionTime {
                        year: Some(year),
                        month: Some(month),
                        day_id: None,
                        hour: None,
                    };
                    self.notify_at(Granularity::Month, ctx)?;
                    if depth >= TimeNestDepth::Day {
                        for (&day_id, hours) in days {
                            ctx.position.time = ExecutionTime {
                                year: Some(year),
                                month: Some(month),
                                day_id: Some(day_id),
                                hour: None,
                            };
                            self.notify_at(Granularity::Day, ctx)?;
                            if depth >= TimeNestDepth::Hour {
                                for &hour in hours {
                                    ctx.position.time =
                                        ExecutionTime::hour(year, month, day_id, hour);
                                    self.notify_at(Granularity::Hour, ctx)?;
                                    self.notify_at(Granularity::MatchFinest, ctx)?;
                                    self.cleanup_at(Granularity::MatchFinest, ctx)?;
                                    self.cleanup_at(Granularity::Hour, ctx)?;
                                }
                            }
                            self.cleanup_at(Granularity::Day, ctx)?;
                        }
                    }
                    self.cleanup_at(Granularity::Month, ctx)?;
                }
            }
            self.cleanup_at(Granularity::Year, ctx)?;
        }
        Ok(())
    }

    /// Forward pass: invoke
    /// [`MasterLoopable::execute_at_granularity`] on every subscription
    /// matching `granularity`, in execution order (priority high-to-low
    /// inside the granularity bucket, with identity tie-break).
    fn notify_at(
        &self,
        granularity: Granularity,
        ctx: &mut MasterLoopContext,
    ) -> Result<(), Error> {
        ctx.execution_granularity = Some(granularity);
        ctx.is_clean_up = false;
        for sub in &self.subscriptions {
            if sub.granularity != granularity {
                continue;
            }
            ctx.execution_priority = sub.priority;
            sub.loopable.execute_at_granularity(ctx)?;
        }
        Ok(())
    }

    /// Cleanup pass: invoke [`MasterLoopable::clean_data_loop`] on every
    /// subscription matching `granularity`, in reverse execution order.
    fn cleanup_at(
        &self,
        granularity: Granularity,
        ctx: &mut MasterLoopContext,
    ) -> Result<(), Error> {
        ctx.execution_granularity = Some(granularity);
        ctx.is_clean_up = true;
        for sub in self.subscriptions.iter().rev() {
            if sub.granularity != granularity {
                continue;
            }
            ctx.execution_priority = sub.priority;
            sub.loopable.clean_data_loop(ctx)?;
        }
        Ok(())
    }

    /// Port of `MasterLoop.hasLoopables`: whether any registered
    /// subscription fires at exactly `granularity`.
    ///
    /// Java keys subscriptions by `EmissionProcess` in a `TreeMap` and
    /// answers per process; this port keeps one global subscription list
    /// — a subscriber that cares about a single process filters on
    /// `ctx.position.process_id` itself — so the answer is
    /// process-independent. Java walks its per-process `TreeSet`,
    /// skipping coarser entries and stopping at the first granularity
    /// `>=` the target; an exact-match scan is equivalent here because
    /// `subscribe` keeps `subscriptions` sorted and `time_nest_depth`
    /// needs only the yes/no answer.
    fn has_loopables(&self, granularity: Granularity) -> bool {
        self.subscriptions
            .iter()
            .any(|sub| sub.granularity == granularity)
    }

    /// Decide how deep `run` drives the time nest, porting the
    /// `mustLoopOver*` cascade in `MasterLoop.loopThroughProcess`: the
    /// finest granularity anyone subscribes to sets the depth, and every
    /// coarser time level above it is implied.
    ///
    /// Java probes `hasLoopables(HOUR / DAY / MONTH / YEAR)`. This port
    /// also folds `MATCH_FINEST` into the `HOUR` probe. Java can leave it
    /// out because its `notifyLoopablesOfLoopChange` never dispatches a
    /// `MATCH_FINEST` subscription (it nulls them out); this port *does*
    /// dispatch them — Task 20 fires them at the `HOUR` level, right
    /// after the `HOUR` subscribers — so the `HOUR` loop must stay alive
    /// whenever a `MATCH_FINEST` subscription exists, or it would
    /// silently never fire.
    fn time_nest_depth(&self) -> TimeNestDepth {
        if self.has_loopables(Granularity::Hour) || self.has_loopables(Granularity::MatchFinest) {
            TimeNestDepth::Hour
        } else if self.has_loopables(Granularity::Day) {
            TimeNestDepth::Day
        } else if self.has_loopables(Granularity::Month) {
            TimeNestDepth::Month
        } else if self.has_loopables(Granularity::Year) {
            TimeNestDepth::Year
        } else {
            TimeNestDepth::Skip
        }
    }
}

/// How deep the `year → month → day → hour` time nest descends on a
/// `MasterLoop::run`, chosen by `MasterLoop::time_nest_depth` from a
/// coarse-to-fine `hasLoopables` probe.
///
/// Ports the `mustLoopOverYears / Months / Days / Hours` boolean cascade
/// in `MasterLoop.loopThroughProcess`. Those four Java booleans always
/// form a prefix — `hours ⇒ days ⇒ months ⇒ years` — so one
/// coarse-to-fine level captures them and makes the invalid mixes (say,
/// "days but not months") unrepresentable.
///
/// Variants are declared coarse-to-fine, so the derived `Ord` turns a
/// level test into a plain `depth >= TimeNestDepth::Month` comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum TimeNestDepth {
    /// No `YEAR` / `MONTH` / `DAY` / `HOUR` subscriber: the whole time
    /// nest is skipped and `run_times` returns immediately.
    Skip,
    /// Iterate `year` only.
    Year,
    /// Iterate `year` then `month`.
    Month,
    /// Iterate `year`, `month`, then `day`.
    Day,
    /// Iterate the full `year → month → day → hour` nest. Also chosen
    /// when only a `MATCH_FINEST` subscriber is registered.
    Hour,
}

/// `state_id → county_id → zone_id → [link_id]` nest produced by
/// [`group_locations`]. Outer-to-inner `BTreeMap` keys give deterministic
/// per-run iteration order; the leaf `Vec` preserves insertion order so
/// repeated link IDs fire repeated iterations.
type LocationGroups = BTreeMap<u32, BTreeMap<u32, BTreeMap<u32, Vec<u32>>>>;

/// `year → month → day_id → [hour]` nest produced by [`group_times`].
type TimeGroups = BTreeMap<u16, BTreeMap<u8, BTreeMap<u8, Vec<u8>>>>;

fn group_locations(locations: &[ExecutionLocation]) -> LocationGroups {
    let mut out: LocationGroups = BTreeMap::new();
    for l in locations {
        // Locations with any None leading id can't be placed in the nest
        // without inventing a key — skip them. Callers are expected to
        // pass fully-populated link-granularity tuples.
        let (Some(s), Some(c), Some(z), Some(k)) = (l.state_id, l.county_id, l.zone_id, l.link_id)
        else {
            continue;
        };
        out.entry(s)
            .or_default()
            .entry(c)
            .or_default()
            .entry(z)
            .or_default()
            .push(k);
    }
    out
}

fn group_times(times: &[ExecutionTime]) -> TimeGroups {
    let mut out: TimeGroups = BTreeMap::new();
    for t in times {
        let (Some(y), Some(m), Some(d), Some(h)) = (t.year, t.month, t.day_id, t.hour) else {
            continue;
        };
        out.entry(y)
            .or_default()
            .entry(m)
            .or_default()
            .entry(d)
            .or_default()
            .push(h);
    }
    out
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

    // ---- MasterLoop iteration engine (Task 20) -------------------------

    use moves_data::ProcessId;
    use std::sync::Mutex;

    /// Snapshot of one `MasterLoopContext` callback for assertion. We
    /// can't store `&MasterLoopContext` borrows in the recorder, and we
    /// don't want to clone the entire context for every callback — keep
    /// the fields we actually assert on.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct CallRecord {
        tag: &'static str,
        iteration: u32,
        process_id: Option<ProcessId>,
        location: ExecutionLocation,
        time: ExecutionTime,
        granularity: Option<Granularity>,
        priority: i32,
        is_clean_up: bool,
    }

    /// `MasterLoopable` that appends one [`CallRecord`] per invocation
    /// to a shared `Vec`. Lets tests assert the full call sequence.
    #[derive(Debug)]
    struct RecordingLoopable {
        tag: &'static str,
        log: Arc<Mutex<Vec<CallRecord>>>,
    }

    impl RecordingLoopable {
        fn new(tag: &'static str, log: Arc<Mutex<Vec<CallRecord>>>) -> Arc<Self> {
            Arc::new(Self { tag, log })
        }

        fn record(&self, ctx: &MasterLoopContext) {
            self.log.lock().unwrap().push(CallRecord {
                tag: self.tag,
                iteration: ctx.position.iteration,
                process_id: ctx.position.process_id,
                location: ctx.position.location,
                time: ctx.position.time,
                granularity: ctx.execution_granularity,
                priority: ctx.execution_priority,
                is_clean_up: ctx.is_clean_up,
            });
        }
    }

    impl MasterLoopable for RecordingLoopable {
        fn execute_at_granularity(&self, ctx: &MasterLoopContext) -> Result<(), Error> {
            self.record(ctx);
            Ok(())
        }
        fn clean_data_loop(&self, ctx: &MasterLoopContext) -> Result<(), Error> {
            self.record(ctx);
            Ok(())
        }
    }

    /// `MasterLoopable` whose `execute_at_granularity` returns
    /// `Err(NotImplemented)` after the first call. Used to assert that
    /// the loop aborts on the first error.
    #[derive(Debug)]
    struct FailingLoopable {
        log: Arc<Mutex<Vec<CallRecord>>>,
    }

    impl MasterLoopable for FailingLoopable {
        fn execute_at_granularity(&self, ctx: &MasterLoopContext) -> Result<(), Error> {
            self.log.lock().unwrap().push(CallRecord {
                tag: "fail",
                iteration: ctx.position.iteration,
                process_id: ctx.position.process_id,
                location: ctx.position.location,
                time: ctx.position.time,
                granularity: ctx.execution_granularity,
                priority: ctx.execution_priority,
                is_clean_up: ctx.is_clean_up,
            });
            Err(Error::NotImplemented)
        }
    }

    fn shared_log() -> Arc<Mutex<Vec<CallRecord>>> {
        Arc::new(Mutex::new(Vec::new()))
    }

    fn snapshot(log: &Arc<Mutex<Vec<CallRecord>>>) -> Vec<CallRecord> {
        log.lock().unwrap().clone()
    }

    fn loc(state: u32, county: u32, zone: u32, link: u32) -> ExecutionLocation {
        ExecutionLocation::link(state, county, zone, link)
    }

    fn et(year: u16, month: u8, day_id: u8, hour: u8) -> ExecutionTime {
        ExecutionTime::hour(year, month, day_id, hour)
    }

    #[test]
    fn new_initialises_with_iterations_one_and_empty_inputs() {
        let ml = MasterLoop::new();
        assert_eq!(ml.iterations, 1);
        assert!(ml.processes.is_empty());
        assert!(ml.locations.is_empty());
        assert!(ml.times.is_empty());
        assert!(ml.subscriptions().is_empty());
    }

    #[test]
    fn default_matches_new() {
        let a = MasterLoop::default();
        let b = MasterLoop::new();
        assert_eq!(a.iterations, b.iterations);
        assert_eq!(a.processes.len(), b.processes.len());
        assert_eq!(a.subscriptions().len(), b.subscriptions().len());
    }

    #[test]
    fn subscribe_keeps_subscriptions_sorted() {
        let log = shared_log();
        let lo = RecordingLoopable::new("lo", log.clone());
        let hi = RecordingLoopable::new("hi", log.clone());
        let mut ml = MasterLoop::new();
        // Insert low-priority first, then high-priority. After both
        // inserts, subscriptions() should yield highest-priority first
        // at the same granularity.
        ml.subscribe(MasterLoopableSubscription::new(Granularity::Hour, 10, lo));
        ml.subscribe(MasterLoopableSubscription::new(Granularity::Hour, 100, hi));
        let prios: Vec<i32> = ml.subscriptions().iter().map(|s| s.priority).collect();
        assert_eq!(
            prios,
            vec![100, 10],
            "highest priority first inside Hour bucket"
        );
    }

    #[test]
    fn empty_loop_runs_no_callbacks() {
        let ml = MasterLoop::new();
        // No processes/locations/times and no subscribers. Should be a no-op.
        ml.run().unwrap();
    }

    #[test]
    fn empty_processes_skips_entire_nest() {
        let log = shared_log();
        let probe = RecordingLoopable::new("probe", log.clone());
        let mut ml = MasterLoop::new();
        ml.locations.push(loc(1, 1, 1, 1));
        ml.times.push(et(2020, 1, 5, 8));
        ml.subscribe(MasterLoopableSubscription::new(
            Granularity::Hour,
            10,
            probe,
        ));
        ml.run().unwrap();
        // No processes ⇒ the outer loop body never runs ⇒ no callbacks.
        assert!(snapshot(&log).is_empty());
    }

    #[test]
    fn process_subscription_fires_once_per_process() {
        let log = shared_log();
        let probe = RecordingLoopable::new("proc", log.clone());
        let mut ml = MasterLoop::new();
        ml.processes.push(ProcessId(1));
        ml.processes.push(ProcessId(2));
        // No locations/times — but PROCESS-granularity subs still fire
        // before the location loop is entered.
        ml.subscribe(MasterLoopableSubscription::new(
            Granularity::Process,
            10,
            probe,
        ));
        ml.run().unwrap();
        let calls = snapshot(&log);
        // Two processes × (forward + cleanup) = 4 calls.
        assert_eq!(calls.len(), 4);
        assert_eq!(calls[0].granularity, Some(Granularity::Process));
        assert_eq!(calls[0].process_id, Some(ProcessId(1)));
        assert!(!calls[0].is_clean_up);
        // Second call is cleanup for the same process (no nested loop ⇒
        // we go straight from forward to cleanup).
        assert_eq!(calls[1].process_id, Some(ProcessId(1)));
        assert!(calls[1].is_clean_up);
        // Then process 2: forward + cleanup.
        assert_eq!(calls[2].process_id, Some(ProcessId(2)));
        assert!(!calls[2].is_clean_up);
        assert_eq!(calls[3].process_id, Some(ProcessId(2)));
        assert!(calls[3].is_clean_up);
    }

    #[test]
    fn single_hour_path_visits_every_granularity_top_to_bottom() {
        // One process × one location × one time. Subscribe at every
        // granularity. Forward pass should visit them coarse-to-fine
        // exactly once.
        let log = shared_log();
        let mut ml = MasterLoop::new();
        ml.processes.push(ProcessId(1));
        ml.locations.push(loc(10, 100, 1000, 10_000));
        ml.times.push(et(2020, 6, 5, 12));
        let grans = [
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
        ];
        for g in grans {
            let probe = RecordingLoopable::new(g.as_str(), log.clone());
            ml.subscribe(MasterLoopableSubscription::new(g, 10, probe));
        }
        ml.run().unwrap();
        let calls = snapshot(&log);

        // Filter to forward-pass calls only — cleanup interleaves.
        let forward: Vec<_> = calls.iter().filter(|c| !c.is_clean_up).collect();
        let forward_grans: Vec<_> = forward.iter().map(|c| c.granularity.unwrap()).collect();
        assert_eq!(
            forward_grans,
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
            "forward pass must walk coarse→fine, MATCH_FINEST last",
        );

        // Cleanup forms a balanced unwinding: MATCH_FINEST→HOUR at the
        // innermost level, then Day→Month→Year as we exit the time loop,
        // then Link→Zone→County→State as we exit the location loop, then
        // Process at the very end.
        let cleanup: Vec<_> = calls.iter().filter(|c| c.is_clean_up).collect();
        let cleanup_grans: Vec<_> = cleanup.iter().map(|c| c.granularity.unwrap()).collect();
        assert_eq!(
            cleanup_grans,
            vec![
                Granularity::MatchFinest,
                Granularity::Hour,
                Granularity::Day,
                Granularity::Month,
                Granularity::Year,
                Granularity::Link,
                Granularity::Zone,
                Granularity::County,
                Granularity::State,
                Granularity::Process,
            ],
            "cleanup pass must unwind in reverse-nest order",
        );
    }

    #[test]
    fn context_carries_correct_location_and_time_at_each_level() {
        // Subscribe only at HOUR — verify position fields are all set.
        let log = shared_log();
        let probe = RecordingLoopable::new("hour", log.clone());
        let mut ml = MasterLoop::new();
        ml.processes.push(ProcessId(2));
        ml.locations.push(loc(48, 201, 9, 123));
        ml.times.push(et(2022, 7, 5, 18));
        ml.subscribe(MasterLoopableSubscription::new(
            Granularity::Hour,
            10,
            probe,
        ));
        ml.run().unwrap();
        let calls = snapshot(&log);
        let fwd = &calls[0];
        assert_eq!(fwd.iteration, 0);
        assert_eq!(fwd.process_id, Some(ProcessId(2)));
        assert_eq!(fwd.location, ExecutionLocation::link(48, 201, 9, 123));
        assert_eq!(fwd.time, ExecutionTime::hour(2022, 7, 5, 18));
        assert_eq!(fwd.granularity, Some(Granularity::Hour));
        assert!(!fwd.is_clean_up);
    }

    #[test]
    fn state_level_only_carries_state_id_in_location() {
        // Subscribe at STATE — verify county/zone/link components are
        // `None` since the loop hasn't entered them yet.
        let log = shared_log();
        let probe = RecordingLoopable::new("state", log.clone());
        let mut ml = MasterLoop::new();
        ml.processes.push(ProcessId(1));
        ml.locations.push(loc(48, 201, 9, 123));
        ml.times.push(et(2020, 1, 5, 8));
        ml.subscribe(MasterLoopableSubscription::new(
            Granularity::State,
            10,
            probe,
        ));
        ml.run().unwrap();
        let calls = snapshot(&log);
        let fwd = calls.iter().find(|c| !c.is_clean_up).unwrap();
        assert_eq!(fwd.granularity, Some(Granularity::State));
        assert_eq!(fwd.location.state_id, Some(48));
        assert!(fwd.location.county_id.is_none(), "county not yet entered");
        assert!(fwd.location.zone_id.is_none());
        assert!(fwd.location.link_id.is_none());
    }

    #[test]
    fn year_level_only_carries_year_in_time() {
        let log = shared_log();
        let probe = RecordingLoopable::new("year", log.clone());
        let mut ml = MasterLoop::new();
        ml.processes.push(ProcessId(1));
        ml.locations.push(loc(1, 1, 1, 1));
        ml.times.push(et(2022, 7, 5, 18));
        ml.subscribe(MasterLoopableSubscription::new(
            Granularity::Year,
            10,
            probe,
        ));
        ml.run().unwrap();
        let calls = snapshot(&log);
        let fwd = calls.iter().find(|c| !c.is_clean_up).unwrap();
        assert_eq!(fwd.time.year, Some(2022));
        assert!(
            fwd.time.month.is_none(),
            "month not yet entered at YEAR level"
        );
        assert!(fwd.time.day_id.is_none());
        assert!(fwd.time.hour.is_none());
    }

    #[test]
    fn locations_group_by_leading_ids() {
        // Two counties under the same state, each with one link. STATE
        // subscriber should fire ONCE per unique state (here 1 state),
        // COUNTY subscriber should fire ONCE per unique (state, county)
        // (here 2 counties).
        let log = shared_log();
        let state_sub = RecordingLoopable::new("state", log.clone());
        let county_sub = RecordingLoopable::new("county", log.clone());
        let mut ml = MasterLoop::new();
        ml.processes.push(ProcessId(1));
        ml.locations.push(loc(1, 1, 1, 1));
        ml.locations.push(loc(1, 2, 1, 1));
        ml.times.push(et(2020, 1, 5, 8));
        ml.subscribe(MasterLoopableSubscription::new(
            Granularity::State,
            10,
            state_sub,
        ));
        ml.subscribe(MasterLoopableSubscription::new(
            Granularity::County,
            10,
            county_sub,
        ));
        ml.run().unwrap();
        let calls = snapshot(&log);
        let state_fwd: Vec<_> = calls
            .iter()
            .filter(|c| c.tag == "state" && !c.is_clean_up)
            .collect();
        let county_fwd: Vec<_> = calls
            .iter()
            .filter(|c| c.tag == "county" && !c.is_clean_up)
            .collect();
        assert_eq!(
            state_fwd.len(),
            1,
            "one unique state → one STATE forward call"
        );
        assert_eq!(
            county_fwd.len(),
            2,
            "two unique (state, county) → two COUNTY forward calls",
        );
        // Verify the county IDs visited cover both inputs (BTreeMap
        // iteration is by numeric ID, so order is deterministic 1, 2).
        assert_eq!(county_fwd[0].location.county_id, Some(1));
        assert_eq!(county_fwd[1].location.county_id, Some(2));
    }

    #[test]
    fn times_group_by_leading_components() {
        // Two months under the same year. YEAR sub fires once, MONTH sub
        // fires twice.
        let log = shared_log();
        let year_sub = RecordingLoopable::new("year", log.clone());
        let month_sub = RecordingLoopable::new("month", log.clone());
        let mut ml = MasterLoop::new();
        ml.processes.push(ProcessId(1));
        ml.locations.push(loc(1, 1, 1, 1));
        ml.times.push(et(2020, 1, 5, 8));
        ml.times.push(et(2020, 2, 5, 8));
        ml.subscribe(MasterLoopableSubscription::new(
            Granularity::Year,
            10,
            year_sub,
        ));
        ml.subscribe(MasterLoopableSubscription::new(
            Granularity::Month,
            10,
            month_sub,
        ));
        ml.run().unwrap();
        let calls = snapshot(&log);
        let year_fwd: Vec<_> = calls
            .iter()
            .filter(|c| c.tag == "year" && !c.is_clean_up)
            .collect();
        let month_fwd: Vec<_> = calls
            .iter()
            .filter(|c| c.tag == "month" && !c.is_clean_up)
            .collect();
        assert_eq!(year_fwd.len(), 1);
        assert_eq!(month_fwd.len(), 2);
        assert_eq!(month_fwd[0].time.month, Some(1));
        assert_eq!(month_fwd[1].time.month, Some(2));
    }

    #[test]
    fn within_granularity_high_priority_fires_first_in_forward_and_last_in_cleanup() {
        let log = shared_log();
        let hi = RecordingLoopable::new("hi", log.clone());
        let lo = RecordingLoopable::new("lo", log.clone());
        let mut ml = MasterLoop::new();
        ml.processes.push(ProcessId(1));
        ml.locations.push(loc(1, 1, 1, 1));
        ml.times.push(et(2020, 1, 5, 8));
        // Register low first, high second, to make sure subscribe()'s
        // sort drives execution order rather than insertion order.
        ml.subscribe(MasterLoopableSubscription::new(Granularity::Hour, 10, lo));
        ml.subscribe(MasterLoopableSubscription::new(Granularity::Hour, 100, hi));
        ml.run().unwrap();
        let calls = snapshot(&log);
        let hour_calls: Vec<_> = calls
            .iter()
            .filter(|c| c.granularity == Some(Granularity::Hour))
            .collect();
        // Hour forward: hi then lo. Hour cleanup: lo then hi (reverse).
        assert_eq!(hour_calls.len(), 4);
        assert_eq!(hour_calls[0].tag, "hi");
        assert!(!hour_calls[0].is_clean_up);
        assert_eq!(hour_calls[1].tag, "lo");
        assert!(!hour_calls[1].is_clean_up);
        assert_eq!(hour_calls[2].tag, "lo");
        assert!(hour_calls[2].is_clean_up);
        assert_eq!(hour_calls[3].tag, "hi");
        assert!(hour_calls[3].is_clean_up);
    }

    #[test]
    fn execution_priority_records_subscription_priority() {
        let log = shared_log();
        let p1 = RecordingLoopable::new("p1", log.clone());
        let p2 = RecordingLoopable::new("p2", log.clone());
        let mut ml = MasterLoop::new();
        ml.processes.push(ProcessId(1));
        ml.locations.push(loc(1, 1, 1, 1));
        ml.times.push(et(2020, 1, 5, 8));
        ml.subscribe(MasterLoopableSubscription::new(Granularity::Hour, 10, p1));
        ml.subscribe(MasterLoopableSubscription::new(Granularity::Hour, 100, p2));
        ml.run().unwrap();
        let calls = snapshot(&log);
        let by_tag: BTreeMap<&str, i32> = calls
            .iter()
            .filter(|c| !c.is_clean_up)
            .map(|c| (c.tag, c.priority))
            .collect();
        assert_eq!(by_tag.get("p1"), Some(&10));
        assert_eq!(by_tag.get("p2"), Some(&100));
    }

    #[test]
    fn match_finest_fires_after_hour_each_iteration() {
        let log = shared_log();
        let hour_sub = RecordingLoopable::new("hour", log.clone());
        let mf_sub = RecordingLoopable::new("mf", log.clone());
        let mut ml = MasterLoop::new();
        ml.processes.push(ProcessId(1));
        ml.locations.push(loc(1, 1, 1, 1));
        // Two hours so we observe per-hour interleaving.
        ml.times.push(et(2020, 1, 5, 1));
        ml.times.push(et(2020, 1, 5, 2));
        ml.subscribe(MasterLoopableSubscription::new(
            Granularity::Hour,
            10,
            hour_sub,
        ));
        ml.subscribe(MasterLoopableSubscription::new(
            Granularity::MatchFinest,
            10,
            mf_sub,
        ));
        ml.run().unwrap();
        let calls = snapshot(&log);
        let fwd: Vec<_> = calls.iter().filter(|c| !c.is_clean_up).collect();
        assert_eq!(fwd.len(), 4, "two hours × (hour + match_finest)");
        // Hour 1: hour then match_finest.
        assert_eq!(fwd[0].tag, "hour");
        assert_eq!(fwd[0].time.hour, Some(1));
        assert_eq!(fwd[1].tag, "mf");
        assert_eq!(fwd[1].time.hour, Some(1));
        // Hour 2: hour then match_finest.
        assert_eq!(fwd[2].tag, "hour");
        assert_eq!(fwd[2].time.hour, Some(2));
        assert_eq!(fwd[3].tag, "mf");
        assert_eq!(fwd[3].time.hour, Some(2));
    }

    #[test]
    fn multiple_iterations_repeat_the_full_nest() {
        let log = shared_log();
        let probe = RecordingLoopable::new("p", log.clone());
        let mut ml = MasterLoop::new();
        ml.iterations = 3;
        ml.processes.push(ProcessId(1));
        ml.locations.push(loc(1, 1, 1, 1));
        ml.times.push(et(2020, 1, 5, 8));
        ml.subscribe(MasterLoopableSubscription::new(
            Granularity::Hour,
            10,
            probe,
        ));
        ml.run().unwrap();
        let calls = snapshot(&log);
        let fwd: Vec<_> = calls.iter().filter(|c| !c.is_clean_up).collect();
        assert_eq!(fwd.len(), 3, "one forward call per iteration");
        for (i, c) in fwd.iter().enumerate() {
            assert_eq!(c.iteration, i as u32);
        }
    }

    #[test]
    fn iterations_zero_runs_no_callbacks() {
        let log = shared_log();
        let probe = RecordingLoopable::new("p", log.clone());
        let mut ml = MasterLoop::new();
        ml.iterations = 0;
        ml.processes.push(ProcessId(1));
        ml.locations.push(loc(1, 1, 1, 1));
        ml.times.push(et(2020, 1, 5, 8));
        ml.subscribe(MasterLoopableSubscription::new(
            Granularity::Hour,
            10,
            probe,
        ));
        ml.run().unwrap();
        assert!(snapshot(&log).is_empty());
    }

    #[test]
    fn forward_error_aborts_remaining_iteration() {
        // Two subscribers at HOUR. The high-priority one errors; the
        // low-priority one must never fire.
        let log = shared_log();
        let high = Arc::new(FailingLoopable { log: log.clone() }) as Arc<dyn MasterLoopable>;
        let low = RecordingLoopable::new("low", log.clone());
        let mut ml = MasterLoop::new();
        ml.processes.push(ProcessId(1));
        ml.locations.push(loc(1, 1, 1, 1));
        ml.times.push(et(2020, 1, 5, 8));
        ml.subscribe(MasterLoopableSubscription::new(
            Granularity::Hour,
            100,
            high,
        ));
        ml.subscribe(MasterLoopableSubscription::new(Granularity::Hour, 10, low));
        let err = ml.run().expect_err("should propagate the loopable's error");
        match err {
            Error::NotImplemented => {}
            other => panic!("expected NotImplemented, got {other:?}"),
        }
        let calls = snapshot(&log);
        let by_tag: Vec<&str> = calls.iter().map(|c| c.tag).collect();
        // Forward at coarser levels fires first; then the failing hour
        // subscriber. The low-priority hour subscriber never gets a turn,
        // and no cleanup runs.
        assert!(by_tag.contains(&"fail"));
        assert!(
            !by_tag.contains(&"low"),
            "should abort before lower-priority hour sub"
        );
    }

    #[test]
    fn loopable_with_two_subscriptions_fires_at_each() {
        // One loopable subscribes at PROCESS and at HOUR. It should fire
        // at both levels.
        let log = shared_log();
        let probe = RecordingLoopable::new("multi", log.clone());
        let mut ml = MasterLoop::new();
        ml.processes.push(ProcessId(1));
        ml.locations.push(loc(1, 1, 1, 1));
        ml.times.push(et(2020, 1, 5, 8));
        ml.subscribe(MasterLoopableSubscription::new(
            Granularity::Process,
            10,
            probe.clone(),
        ));
        ml.subscribe(MasterLoopableSubscription::new(
            Granularity::Hour,
            10,
            probe,
        ));
        ml.run().unwrap();
        let calls = snapshot(&log);
        let grans: Vec<_> = calls
            .iter()
            .filter(|c| !c.is_clean_up)
            .map(|c| c.granularity.unwrap())
            .collect();
        assert!(grans.contains(&Granularity::Process));
        assert!(grans.contains(&Granularity::Hour));
    }

    #[test]
    fn duplicate_link_in_input_fires_twice() {
        // Locations: (1,1,1,1) appears twice. LINK subscriber should fire
        // twice. This matches Java behaviour when the producer emits
        // repeated tuples.
        let log = shared_log();
        let probe = RecordingLoopable::new("link", log.clone());
        let mut ml = MasterLoop::new();
        ml.processes.push(ProcessId(1));
        ml.locations.push(loc(1, 1, 1, 1));
        ml.locations.push(loc(1, 1, 1, 1));
        ml.times.push(et(2020, 1, 5, 8));
        ml.subscribe(MasterLoopableSubscription::new(
            Granularity::Link,
            10,
            probe,
        ));
        ml.run().unwrap();
        let calls = snapshot(&log);
        let link_fwd_count = calls
            .iter()
            .filter(|c| c.granularity == Some(Granularity::Link) && !c.is_clean_up)
            .count();
        assert_eq!(link_fwd_count, 2);
    }

    #[test]
    fn groups_iterate_in_numeric_id_order_regardless_of_input_order() {
        // Submit locations in descending state order; STATE forward calls
        // should fire in ASCENDING numeric order (BTreeMap key order).
        let log = shared_log();
        let probe = RecordingLoopable::new("state", log.clone());
        let mut ml = MasterLoop::new();
        ml.processes.push(ProcessId(1));
        ml.locations.push(loc(3, 1, 1, 1));
        ml.locations.push(loc(1, 1, 1, 1));
        ml.locations.push(loc(2, 1, 1, 1));
        ml.times.push(et(2020, 1, 5, 8));
        ml.subscribe(MasterLoopableSubscription::new(
            Granularity::State,
            10,
            probe,
        ));
        ml.run().unwrap();
        let states: Vec<u32> = snapshot(&log)
            .iter()
            .filter(|c| c.granularity == Some(Granularity::State) && !c.is_clean_up)
            .filter_map(|c| c.location.state_id)
            .collect();
        assert_eq!(states, vec![1, 2, 3]);
    }

    #[test]
    fn partially_populated_locations_are_skipped_by_grouping() {
        // ExecutionLocation::state(1) has county/zone/link None — can't
        // place it in the nest. group_locations skips it; no callbacks
        // for that location.
        let log = shared_log();
        let probe = RecordingLoopable::new("link", log.clone());
        let mut ml = MasterLoop::new();
        ml.processes.push(ProcessId(1));
        ml.locations.push(ExecutionLocation::state(1));
        ml.locations.push(loc(2, 1, 1, 1));
        ml.times.push(et(2020, 1, 5, 8));
        ml.subscribe(MasterLoopableSubscription::new(
            Granularity::Link,
            10,
            probe,
        ));
        ml.run().unwrap();
        let states: Vec<u32> = snapshot(&log)
            .iter()
            .filter(|c| c.granularity == Some(Granularity::Link) && !c.is_clean_up)
            .filter_map(|c| c.location.state_id)
            .collect();
        // Only the fully-populated location (state 2) fires.
        assert_eq!(states, vec![2]);
    }

    #[test]
    fn group_locations_handles_empty_input() {
        let g = group_locations(&[]);
        assert!(g.is_empty());
    }

    #[test]
    fn group_times_handles_empty_input() {
        let g = group_times(&[]);
        assert!(g.is_empty());
    }

    // ---- hasLoopables short-circuit (Task 21) --------------------------

    #[test]
    fn has_loopables_reports_exact_granularity_membership() {
        let mut ml = MasterLoop::new();
        ml.subscribe(MasterLoopableSubscription::new(
            Granularity::Day,
            10,
            dummy(),
        ));
        // Exact match only — neither coarser, finer, nor the MatchFinest
        // sentinel counts as a Day subscriber.
        assert!(ml.has_loopables(Granularity::Day));
        assert!(!ml.has_loopables(Granularity::Hour));
        assert!(!ml.has_loopables(Granularity::Month));
        assert!(!ml.has_loopables(Granularity::Year));
        assert!(!ml.has_loopables(Granularity::MatchFinest));
        assert!(!ml.has_loopables(Granularity::Process));
    }

    #[test]
    fn time_nest_depth_tracks_the_finest_time_subscription() {
        // No subscriptions at all → the whole time nest is skipped.
        assert_eq!(MasterLoop::new().time_nest_depth(), TimeNestDepth::Skip);

        // Each finer subscription deepens the nest; coarser ones already
        // registered never shrink it back.
        let mut ml = MasterLoop::new();
        ml.subscribe(MasterLoopableSubscription::new(
            Granularity::Year,
            10,
            dummy(),
        ));
        assert_eq!(ml.time_nest_depth(), TimeNestDepth::Year);
        ml.subscribe(MasterLoopableSubscription::new(
            Granularity::Month,
            10,
            dummy(),
        ));
        assert_eq!(ml.time_nest_depth(), TimeNestDepth::Month);
        ml.subscribe(MasterLoopableSubscription::new(
            Granularity::Day,
            10,
            dummy(),
        ));
        assert_eq!(ml.time_nest_depth(), TimeNestDepth::Day);
        ml.subscribe(MasterLoopableSubscription::new(
            Granularity::Hour,
            10,
            dummy(),
        ));
        assert_eq!(ml.time_nest_depth(), TimeNestDepth::Hour);
    }

    #[test]
    fn time_nest_depth_skips_when_only_process_or_location_subscribers() {
        // PROCESS / STATE / COUNTY / ZONE / LINK are not time
        // granularities; none of them should pull the time nest open.
        let mut ml = MasterLoop::new();
        for g in [
            Granularity::Process,
            Granularity::State,
            Granularity::County,
            Granularity::Zone,
            Granularity::Link,
        ] {
            ml.subscribe(MasterLoopableSubscription::new(g, 10, dummy()));
        }
        assert_eq!(ml.time_nest_depth(), TimeNestDepth::Skip);
    }

    #[test]
    fn time_nest_depth_treats_match_finest_as_hour_depth() {
        // A MatchFinest subscriber fires at the HOUR level in this port
        // (Java never dispatches it), so it must keep the full time nest
        // alive even with no real HOUR subscriber.
        let mut ml = MasterLoop::new();
        ml.subscribe(MasterLoopableSubscription::new(
            Granularity::MatchFinest,
            10,
            dummy(),
        ));
        assert_eq!(ml.time_nest_depth(), TimeNestDepth::Hour);
    }

    #[test]
    fn match_finest_only_subscription_still_fires() {
        // No real-granularity subscriber, only MatchFinest. The
        // short-circuit must still open the HOUR loop so the subscriber
        // is dispatched — otherwise a registered subscription would
        // silently never fire.
        let log = shared_log();
        let mf = RecordingLoopable::new("mf", log.clone());
        let mut ml = MasterLoop::new();
        ml.processes.push(ProcessId(1));
        ml.locations.push(loc(1, 1, 1, 1));
        ml.times.push(et(2020, 1, 5, 8));
        ml.subscribe(MasterLoopableSubscription::new(
            Granularity::MatchFinest,
            10,
            mf,
        ));
        ml.run().unwrap();
        let calls = snapshot(&log);
        let fwd: Vec<_> = calls.iter().filter(|c| !c.is_clean_up).collect();
        assert_eq!(fwd.len(), 1, "the lone MatchFinest subscriber fires once");
        assert_eq!(fwd[0].granularity, Some(Granularity::MatchFinest));
        assert_eq!(fwd[0].time, ExecutionTime::hour(2020, 1, 5, 8));
    }

    #[test]
    fn day_subscription_fires_under_short_circuit() {
        // DAY is the finest subscription → depth Day; the hour loop is
        // short-circuited away. Two days under one month fire the DAY
        // subscriber twice, each with the hour component still None.
        let log = shared_log();
        let day_sub = RecordingLoopable::new("day", log.clone());
        let mut ml = MasterLoop::new();
        ml.processes.push(ProcessId(1));
        ml.locations.push(loc(1, 1, 1, 1));
        ml.times.push(et(2020, 3, 5, 8));
        ml.times.push(et(2020, 3, 6, 8));
        ml.subscribe(MasterLoopableSubscription::new(
            Granularity::Day,
            10,
            day_sub,
        ));
        ml.run().unwrap();
        let calls = snapshot(&log);
        let fwd: Vec<_> = calls.iter().filter(|c| !c.is_clean_up).collect();
        assert_eq!(fwd.len(), 2, "two days → two DAY forward calls");
        assert_eq!(fwd[0].time.day_id, Some(5));
        assert_eq!(fwd[1].time.day_id, Some(6));
        assert!(
            fwd[0].time.hour.is_none(),
            "hour loop short-circuited — no hour component at the DAY level"
        );
        assert!(fwd[1].time.hour.is_none());
    }
}

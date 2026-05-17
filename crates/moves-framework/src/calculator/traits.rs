//! Calculator and Generator base traits — the contract Phase 3
//! calculators and Phase 2 generators implement.
//!
//! Ports `EmissionCalculator.java`, `Generator.java`, and the abstract base
//! `GenericCalculatorBase.java` from `gov.epa.otaq.moves.master.framework`.
//!
//! # The two traits
//!
//! [`Calculator`] and [`Generator`] are parallel traits with overlapping
//! shape:
//!
//! * Both expose **metadata methods** — [`name`](Calculator::name),
//!   [`subscriptions`](Calculator::subscriptions),
//!   [`upstream`](Calculator::upstream),
//!   [`input_tables`](Calculator::input_tables). Task 19's
//!   `CalculatorRegistry` reads these to wire the MasterLoop without
//!   running any calculator body.
//! * Both expose a **work method** —
//!   `execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput>`.
//!   Task 21's loop notifier invokes this once per iteration at the
//!   registered granularity.
//!
//! Calculator-specific:
//! * [`Calculator::registrations`] — the `(pollutant, process)` pairs the
//!   calculator advertises in `Registration` directives.
//!
//! Generator-specific:
//! * [`Generator::output_tables`] — scratch-namespace tables this generator
//!   writes for downstream calculators.
//!
//! # Relationship to [`MasterLoopable`](crate::MasterLoopable)
//!
//! Neither [`Calculator`] nor [`Generator`] inherit
//! [`MasterLoopable`](crate::MasterLoopable). Doing so would force a
//! default `execute_at_granularity` body, but the
//! [`MasterLoopContext`](crate::MasterLoopContext) the trait method receives
//! does not yet hold a [`CalculatorContext`] — that pairing is owned by
//! Task 19's registry, which holds the per-run state needed to materialise
//! the calculator context from a master-loop callback.
//!
//! Phase 2 Task 19 will instead provide an adapter (`CalculatorMasterLoopable`
//! or a generic equivalent) that wraps a [`Calculator`]/[`Generator`] plus a
//! handle to the run state, implements [`MasterLoopable`](crate::MasterLoopable)
//! for that wrapper, and translates each loop callback into a
//! [`Calculator::execute`] / [`Generator::execute`] invocation against a
//! freshly built [`CalculatorContext`].
//!
//! Keeping `Calculator` decoupled from `MasterLoopable` means Phase 3
//! calculator authors only need to think about "given this context, produce
//! this output" — the dispatch plumbing stays inside the framework.
//!
//! # Context shape
//!
//! [`CalculatorContext`] is the runtime view of a calculator's inputs —
//! per-run filtered default-DB tables ([`ExecutionTables`]), inter-calculator
//! scratch ([`ScratchNamespace`]), and the master loop's current
//! [`IterationPosition`] (process, location, time). The component types
//! live in [`crate::execution::execution_db`]; this module re-binds them onto the
//! trait signatures.
//!
//! [`CalculatorOutput`] is the per-invocation result type. **Phase 2
//! skeleton.** Task 50 (`DataFrameStore`) replaces it with a Polars
//! `DataFrame` once the data plane lands. Sealing the trait signature
//! around the named placeholder type lets Phase 3 calculators start
//! porting against a stable API today; widening the placeholder later
//! does not break implementors.

use moves_calculator_info::{Granularity, Priority};
use moves_data::{PollutantProcessAssociation, ProcessId};

use crate::error::Error;
use crate::execution::execution_db::{ExecutionTables, IterationPosition, ScratchNamespace};

/// Runtime view of a calculator's inputs and scratch space — the in-memory
/// equivalent of MOVES's MariaDB execution database.
///
/// Owns the three pieces Task 23 specifies:
///
/// * [`tables`](Self::tables) — per-run filtered default-DB tables,
///   loaded once per run (slow tier).
/// * [`scratch`](Self::scratch) — name-keyed inter-calculator scratch
///   space (fast tier).
/// * [`position`](Self::position) — the current MasterLoop iteration /
///   location / time triple.
///
/// Calculators don't construct [`CalculatorContext`] themselves; Task 19's
/// registry materialises a fresh one on each
/// [`MasterLoopable::execute_at_granularity`](crate::MasterLoopable::execute_at_granularity)
/// callback and passes it by reference to [`Calculator::execute`] /
/// [`Generator::execute`].
///
/// The slow / scratch tiers are storage-shape placeholders today; Task 50
/// (`DataFrameStore`) replaces their internals. The position triple is
/// concrete: Phase 3 authors can read `ctx.position().location.county_id`,
/// `ctx.position().time.hour`, etc. immediately.
#[derive(Debug, Default)]
pub struct CalculatorContext {
    tables: ExecutionTables,
    scratch: ScratchNamespace,
    position: IterationPosition,
}

impl CalculatorContext {
    /// Construct an empty context with start-of-run position. Used by
    /// tests and by Task 19's registry stub until full per-run
    /// materialisation lands.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct a context with the given position, leaving tables and
    /// scratch empty. Convenient for tests that exercise position-aware
    /// logic without touching the data plane.
    #[must_use]
    pub fn with_position(position: IterationPosition) -> Self {
        Self {
            tables: ExecutionTables::empty(),
            scratch: ScratchNamespace::empty(),
            position,
        }
    }

    /// Per-run filtered default-DB tables. Calculators read from this in
    /// their [`Calculator::execute`] body, indexing by the canonical
    /// table names declared in [`Calculator::input_tables`].
    #[must_use]
    pub fn tables(&self) -> &ExecutionTables {
        &self.tables
    }

    /// Inter-calculator scratch namespace. Generators write here in their
    /// [`Generator::execute`] body; downstream calculators read.
    #[must_use]
    pub fn scratch(&self) -> &ScratchNamespace {
        &self.scratch
    }

    /// Current MasterLoop iteration / location / time triple. Concrete in
    /// this commit (placeholder data plane notwithstanding) — Phase 3
    /// calculators can read e.g. `ctx.position().time.hour` directly.
    #[must_use]
    pub fn position(&self) -> &IterationPosition {
        &self.position
    }
}

/// Value returned by [`Calculator::execute`] / [`Generator::execute`].
///
/// **Phase 2 skeleton.** Task 50 (`DataFrameStore`) replaces this with a
/// Polars `DataFrame`. Fixing the placeholder type here lets Phase 3
/// calculators commit to a result type that the registry can store, even
/// before the data plane has materialised.
#[derive(Debug, Default)]
pub struct CalculatorOutput {
    // Task 50 replaces with a real DataFrame.
    _private: (),
}

impl CalculatorOutput {
    /// Construct an empty output. Stand-in until [`CalculatorOutput`] wraps
    /// a real Polars `DataFrame` (Task 50).
    #[must_use]
    pub fn empty() -> Self {
        Self { _private: () }
    }
}

/// One MasterLoop subscription declared by a calculator or generator —
/// the Rust analogue of one `Subscribe` directive in `CalculatorInfo.txt`.
///
/// A single module may carry multiple subscriptions: e.g. a calculator that
/// covers both running and start exhaust processes at the same granularity
/// would declare two `CalculatorSubscription` rows.
///
/// Priority is stored as the rich [`Priority`] type so the source reads as
/// `Priority::parse("EMISSION_CALCULATOR+1").unwrap()` rather than as a
/// magic-number `11`. Task 19's registry calls [`Priority::value`] when
/// composing the underlying [`MasterLoopableSubscription`](crate::MasterLoopableSubscription).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CalculatorSubscription {
    /// MOVES process this subscription is gated to.
    pub process_id: ProcessId,
    /// Granularity bucket the subscription fires in.
    pub granularity: Granularity,
    /// Priority within the granularity bucket.
    pub priority: Priority,
}

impl CalculatorSubscription {
    /// Construct a subscription from its three components.
    #[must_use]
    pub fn new(process_id: ProcessId, granularity: Granularity, priority: Priority) -> Self {
        Self {
            process_id,
            granularity,
            priority,
        }
    }
}

/// Contract every MOVES emission calculator implements.
///
/// Ports `EmissionCalculator.java` plus the metadata stubs in
/// `GenericCalculatorBase.java`. Phase 3 (Tasks 30–88) lands one
/// implementor per Java calculator class.
///
/// A `Calculator` is a value type that owns no per-run state — Task 19's
/// registry instantiates one per chain-DAG entry and reuses it across the
/// run. All run-varying inputs flow through the [`CalculatorContext`]
/// argument to [`execute`](Self::execute).
pub trait Calculator: Send + Sync + std::fmt::Debug {
    /// Stable identifier matching the calculator's name in the chain DAG
    /// (the `ModuleEntry::name` from
    /// [`moves_calculator_info::CalculatorDag::modules`]).
    /// Used by the registry to wire chain edges between calculators.
    fn name(&self) -> &'static str;

    /// `(process, granularity, priority)` triples this calculator subscribes
    /// to. Most calculators return a single-entry slice; a few subscribe
    /// at multiple granularities for different processes — those return one
    /// row per `Subscribe` directive recorded in `CalculatorInfo.txt`.
    fn subscriptions(&self) -> &[CalculatorSubscription];

    /// `(pollutant, process)` pairs this calculator produces output for —
    /// the `Registration` directives in `CalculatorInfo.txt`.
    ///
    /// Returned slice is allowed to be empty for the rare calculator that
    /// only produces intermediate scratch data and registers nothing for
    /// direct output. Most calculators register at least one pair.
    fn registrations(&self) -> &[PollutantProcessAssociation];

    /// Names of upstream calculators/generators whose output this
    /// calculator's chain consumes. Each name matches a `ModuleEntry::name`
    /// elsewhere in the registry — the registry validates closure during
    /// startup.
    ///
    /// Default empty: root subscribers don't depend on anything upstream.
    fn upstream(&self) -> &[&'static str] {
        &[]
    }

    /// Default-DB / per-run scratch tables this calculator reads from
    /// [`CalculatorContext`]. Task 24 (`InputDataManager`) uses these to
    /// drive lazy loading and dependency analysis.
    ///
    /// Names are the canonical default-DB table names (e.g.
    /// `"sourceUseTypePopulation"`); the registry maps them onto Parquet
    /// snapshot files when materialising the context.
    ///
    /// Default empty: calculator authors fill this in as they port.
    fn input_tables(&self) -> &[&'static str] {
        &[]
    }

    /// Run the calculator. Called once per iteration at the registered
    /// granularity. Returns a [`CalculatorOutput`] (Task 50: a Polars
    /// `DataFrame`) of emission rows ready to merge into the master output.
    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error>;
}

/// Contract every MOVES generator implements.
///
/// Ports `Generator.java`. Generators produce upstream activity, fuel-effect,
/// or operating-mode data that emission calculators consume from the scratch
/// namespace. They subscribe to the MasterLoop like calculators but do not
/// register `(pollutant, process)` pairs — their output is intermediate,
/// not part of the emission tally.
///
/// Phase 3 generator implementations live in `moves-calculators` alongside
/// the calculators (the chain DAG groups them together).
pub trait Generator: Send + Sync + std::fmt::Debug {
    /// Stable identifier matching the generator's name in the chain DAG.
    fn name(&self) -> &'static str;

    /// `(process, granularity, priority)` triples this generator subscribes
    /// to. Generators typically subscribe at a coarser granularity than the
    /// calculators that consume their output (e.g. PROCESS or COUNTY), so
    /// the generated tables are reused across many inner-loop iterations.
    fn subscriptions(&self) -> &[CalculatorSubscription];

    /// Names of upstream generators/calculators this generator depends on.
    /// Defaults to empty.
    fn upstream(&self) -> &[&'static str] {
        &[]
    }

    /// Default-DB / per-run scratch tables this generator reads from
    /// [`CalculatorContext`]. Defaults to empty.
    fn input_tables(&self) -> &[&'static str] {
        &[]
    }

    /// Scratch-namespace tables this generator writes. Downstream
    /// calculators name these in their own [`Calculator::input_tables`] to
    /// declare the dependency. Defaults to empty.
    fn output_tables(&self) -> &[&'static str] {
        &[]
    }

    /// Run the generator. Called once per iteration at the registered
    /// granularity. The returned [`CalculatorOutput`] is stored under the
    /// generator's [`output_tables`](Self::output_tables) names by the
    /// registry (Task 19).
    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use moves_data::PollutantId;

    /// Minimal `Calculator` implementor exercising every trait method —
    /// shape check for the API. The body returns
    /// [`CalculatorOutput::empty`] so this is a compile-and-call smoke test,
    /// not a numerical check.
    #[derive(Debug)]
    struct DummyCalculator;

    static DUMMY_CALC_SUBS: &[CalculatorSubscription] = &[];
    static DUMMY_CALC_REGS: &[PollutantProcessAssociation] = &[];
    static DUMMY_CALC_UPSTREAM: &[&str] = &["UpstreamCalculator"];
    static DUMMY_CALC_INPUTS: &[&str] = &["sourceUseTypePopulation"];

    impl Calculator for DummyCalculator {
        fn name(&self) -> &'static str {
            "DummyCalculator"
        }
        fn subscriptions(&self) -> &[CalculatorSubscription] {
            DUMMY_CALC_SUBS
        }
        fn registrations(&self) -> &[PollutantProcessAssociation] {
            DUMMY_CALC_REGS
        }
        fn upstream(&self) -> &[&'static str] {
            DUMMY_CALC_UPSTREAM
        }
        fn input_tables(&self) -> &[&'static str] {
            DUMMY_CALC_INPUTS
        }
        fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
            Ok(CalculatorOutput::empty())
        }
    }

    /// Minimal `Generator` implementor exercising every trait method.
    #[derive(Debug)]
    struct DummyGenerator;

    static DUMMY_GEN_SUBS: &[CalculatorSubscription] = &[];
    static DUMMY_GEN_OUTPUTS: &[&str] = &["sourceBinDistribution"];

    impl Generator for DummyGenerator {
        fn name(&self) -> &'static str {
            "DummyGenerator"
        }
        fn subscriptions(&self) -> &[CalculatorSubscription] {
            DUMMY_GEN_SUBS
        }
        fn output_tables(&self) -> &[&'static str] {
            DUMMY_GEN_OUTPUTS
        }
        fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
            Ok(CalculatorOutput::empty())
        }
    }

    #[test]
    fn calculator_metadata_and_execute_compile_and_call() {
        let calc = DummyCalculator;
        assert_eq!(calc.name(), "DummyCalculator");
        assert!(calc.subscriptions().is_empty());
        assert!(calc.registrations().is_empty());
        assert_eq!(calc.upstream(), &["UpstreamCalculator"]);
        assert_eq!(calc.input_tables(), &["sourceUseTypePopulation"]);
        let ctx = CalculatorContext::new();
        let _out: CalculatorOutput = calc.execute(&ctx).expect("execute ok");
    }

    #[test]
    fn generator_metadata_and_execute_compile_and_call() {
        let gen = DummyGenerator;
        assert_eq!(gen.name(), "DummyGenerator");
        assert!(gen.subscriptions().is_empty());
        // Default-impl defaults exercise — upstream + input_tables not overridden.
        assert!(gen.upstream().is_empty());
        assert!(gen.input_tables().is_empty());
        assert_eq!(gen.output_tables(), &["sourceBinDistribution"]);
        let ctx = CalculatorContext::new();
        let _out: CalculatorOutput = gen.execute(&ctx).expect("execute ok");
    }

    #[test]
    fn calculator_can_be_held_as_trait_object() {
        // The registry (Task 19) will store calculators as `Box<dyn Calculator>`;
        // verify the trait is object-safe and a calculator value can be coerced.
        let calcs: Vec<Box<dyn Calculator>> = vec![Box::new(DummyCalculator)];
        assert_eq!(calcs[0].name(), "DummyCalculator");
    }

    #[test]
    fn generator_can_be_held_as_trait_object() {
        let gens: Vec<Box<dyn Generator>> = vec![Box::new(DummyGenerator)];
        assert_eq!(gens[0].name(), "DummyGenerator");
    }

    #[test]
    fn calculator_subscription_round_trip() {
        // Build a subscription with realistic process + granularity + priority
        // and assert the components stick.
        let sub = CalculatorSubscription::new(
            ProcessId(1), // Running Exhaust
            Granularity::Hour,
            Priority::parse("EMISSION_CALCULATOR+1").unwrap(),
        );
        assert_eq!(sub.process_id, ProcessId(1));
        assert_eq!(sub.granularity, Granularity::Hour);
        assert_eq!(sub.priority.display(), "EMISSION_CALCULATOR+1");
    }

    #[test]
    fn calculator_subscription_is_copy_and_eq() {
        // Subscriptions need `Copy` for the slice-returning trait method API:
        // calculator structs hold them in `static` arrays without `Clone` boilerplate.
        let a = CalculatorSubscription::new(
            ProcessId(2),
            Granularity::Hour,
            Priority::parse("EMISSION_CALCULATOR").unwrap(),
        );
        let b = a;
        assert_eq!(a, b);
    }

    /// Realistic shape test: a calculator with non-empty registrations
    /// and a multi-process subscription set, mirroring how Phase 3
    /// calculators will look once they land. Uses canonical `Pollutant` /
    /// `Process` ids from the MOVES default DB.
    #[derive(Debug)]
    struct ExampleRealisticCalculator;

    static EX_SUBS: &[CalculatorSubscription] = &[];
    static EX_REGS: &[PollutantProcessAssociation] = &[
        PollutantProcessAssociation {
            pollutant_id: PollutantId(2),
            process_id: ProcessId(1),
        }, // CO2 Running Exhaust
        PollutantProcessAssociation {
            pollutant_id: PollutantId(2),
            process_id: ProcessId(2),
        }, // CO2 Start Exhaust
    ];
    static EX_UPSTREAM: &[&str] = &["TotalActivityGenerator"];
    static EX_INPUTS: &[&str] = &["sourceUseTypePopulation", "emissionRateByAge"];

    impl Calculator for ExampleRealisticCalculator {
        fn name(&self) -> &'static str {
            "ExampleRealisticCalculator"
        }
        fn subscriptions(&self) -> &[CalculatorSubscription] {
            EX_SUBS
        }
        fn registrations(&self) -> &[PollutantProcessAssociation] {
            EX_REGS
        }
        fn upstream(&self) -> &[&'static str] {
            EX_UPSTREAM
        }
        fn input_tables(&self) -> &[&'static str] {
            EX_INPUTS
        }
        fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
            Ok(CalculatorOutput::empty())
        }
    }

    #[test]
    fn realistic_calculator_shape() {
        let calc = ExampleRealisticCalculator;
        let regs = calc.registrations();
        assert_eq!(regs.len(), 2);
        assert!(regs.iter().all(|r| r.pollutant_id == PollutantId(2)));
        let procs: Vec<ProcessId> = regs.iter().map(|r| r.process_id).collect();
        assert_eq!(procs, vec![ProcessId(1), ProcessId(2)]);
        assert_eq!(calc.upstream().len(), 1);
        assert_eq!(calc.input_tables().len(), 2);
    }

    #[test]
    fn context_default_position_is_start_of_run() {
        // The default-constructed context puts the master loop at the
        // pre-iteration state — no process, no location, no time.
        let ctx = CalculatorContext::new();
        let pos = ctx.position();
        assert_eq!(pos.iteration, 0);
        assert!(pos.process_id.is_none());
        assert!(pos.location.county_id.is_none());
        assert!(pos.time.year.is_none());
    }

    #[test]
    fn context_with_position_carries_through_execute() {
        // Calculators reach the position through `ctx.position()`. Build a
        // context at HOUR granularity and verify a Calculator body can
        // read each component from the accessor chain.
        use crate::execution::execution_db::{ExecutionLocation, ExecutionTime, IterationPosition};
        let pos = IterationPosition {
            iteration: 0,
            process_id: Some(ProcessId(1)),
            location: ExecutionLocation::link(40, 40_001, 400_011, 4_000_111),
            time: ExecutionTime::hour(2020, 7, 5, 8),
        };
        let ctx = CalculatorContext::with_position(pos);
        let calc = DummyCalculator;
        // Calculator body doesn't read the context — just verify it can
        // be called with the populated context.
        let _out = calc.execute(&ctx).expect("execute ok");
        // And verify the position is readable through the public accessor
        // (matches what Phase 3 calculator bodies will do).
        assert_eq!(ctx.position().process_id, Some(ProcessId(1)));
        assert_eq!(ctx.position().location.state_id, Some(40));
        assert_eq!(ctx.position().location.link_id, Some(4_000_111));
        assert_eq!(ctx.position().time.hour, Some(8));
    }
}

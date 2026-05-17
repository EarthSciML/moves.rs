//! `moves-framework` — execution runtime that wires RunSpecs to calculators.
//!
//! Will port the master-loop infrastructure from
//! `gov/epa/otaq/moves/master/framework/` (Java) and the granularity
//! hierarchy already characterized in `moves-calculator-info`. The crate
//! owns the runtime view of a RunSpec (`ExecutionRunSpec`), the
//! `(county, zone, link)` location iterator, the calculator registry and
//! scheduler, and the per-iteration `DataFrameStore` lifecycle.
//!
//! See `moves-rust-migration-plan.md`:
//!
//! * Task 15 — `ExecutionRunSpec` (this commit)
//! * Task 16 — Location iterator (this commit)
//! * Task 17 — MasterLoop subscription model
//! * Task 18 — Calculator and Generator base traits
//! * Task 19 — `CalculatorRegistry`
//! * Task 20 — MasterLoop core iteration (this commit)
//! * Task 21 — Granularity-based loop notification (refines Task 20 dispatch)
//! * Task 23 — `ExecutionDatabaseSchema` and `CalculatorContext`
//! * Task 24 — `InputDataManager` (this commit)
//! * Task 25 — Output aggregation planning (this commit)
//! * Task 26 — `OutputProcessor` plan-driven output aggregation (this commit)
//! * Task 27 — `MOVESEngine` and the bounded-concurrency executor (this commit)
//! * Task 50 — `DataFrameStore` (shared with `moves-data`)
//! * Task 89 — Unified Parquet output writer (this commit)
//!
//! # Phase 2 status
//!
//! Tasks 15, 16, 17, 18, 19, 20, 21, 23, 24, 25, 26, 27, and 89 are in place:
//!
//! * Task 15 — [`ExecutionRunSpec`] derives the run-time view of a
//!   [`moves_runspec::RunSpec`]: target pollutants / processes, timespan
//!   sets, vehicle selections, and the refueling-process dependency
//!   closure. Database-dependent state (locations, fuel years, regions)
//!   stays empty until Tasks 16 / 24 populate it.
//! * Task 16 — [`ExecutionLocationProducer`] expands a RunSpec's
//!   geographic selections into the sorted [`ExecutionLocation`] set the
//!   MasterLoop iterates; [`ExecutionRunSpec::build_execution_locations`]
//!   drives it from a [`GeographyTables`] value and re-derives the
//!   `states` / `counties` / `zones` / `links` projections.
//! * Task 17 — [`MasterLoopableSubscription`] ordering matches Java exactly.
//! * Task 18 — [`Calculator`] / [`Generator`] traits plus
//!   [`CalculatorSubscription`].
//! * Task 19 — [`CalculatorRegistry`] for chain-DAG factory bindings,
//!   RunSpec filtering, and topological ordering.
//! * Task 20 — [`MasterLoop`] iteration engine walking
//!   `iteration → process → state → county → zone → link → year → month →
//!   day → hour`, with forward/cleanup dispatch around the Task 23
//!   [`IterationPosition`] triple.
//! * Task 21 — `notifyLoopablesOfLoopChange` / `hasLoopables` dispatch:
//!   priority-ordered notification at each level (live since Task 20),
//!   plus the `hasLoopables` short-circuit that descends the time nest
//!   only as deep as the finest registered subscription.
//! * Task 23 — [`CalculatorContext`] owns [`ExecutionTables`],
//!   [`ScratchNamespace`], and the [`IterationPosition`] triple; the
//!   [`ExecutionDatabaseSchema`] registry defines which tables may appear
//!   in the execution database.
//! * Task 24 — [`InputDataManager`] ports the default-DB-to-execution-DB
//!   filter logic: [`RunSpecFilters`] projects RunSpec selections,
//!   [`WhereClauseBuilder`] emits structured per-dimension predicates,
//!   and [`InputDataManager::plan`] walks the
//!   [`default_tables`] registry to produce a [`MergePlan`].
//! * Task 25 — [`emission_aggregation`] / [`activity_aggregation`] /
//!   [`base_rate_aggregation`] derive the column-shape [`AggregationPlan`]
//!   (group-by keys, collapsed columns, `SUM` metric) from a RunSpec.
//! * Task 26 — [`aggregate_emissions`] / [`aggregate_activity`] apply an
//!   [`AggregationPlan`] to a record batch (group-by + `SUM` + temporal
//!   rescaling); [`OutputProcessor::write_aggregated_emissions`] composes
//!   the roll-up with the Task 89 writer.
//! * Task 27 — [`MOVESEngine`] is the entry point that ties the framework
//!   together: it builds an [`ExecutionRunSpec`], filters and topologically
//!   orders the calculator graph through the [`CalculatorRegistry`], splits
//!   it into independent chains with [`chunk_chains`], runs one
//!   [`MasterLoop`] per chain through the [`BoundedExecutor`] (a
//!   `rayon::ThreadPool` + [`Semaphore`] sized by `--max-parallel-chunks`),
//!   and finalises the [`OutputProcessor`]. Peak memory scales linearly
//!   with the parallelism limit; see [`crate::execution::executor`] for the
//!   memory model.
//! * Task 89 — [`OutputProcessor`], the strongly-typed Parquet writer for
//!   the three output tables defined by [`moves_data::output_schema`].
//!   Phase 3 calculators feed it [`moves_data::EmissionRecord`] /
//!   [`moves_data::ActivityRecord`] batches; Task 26 ([`aggregate_emissions`])
//!   rolls those batches up through an [`AggregationPlan`] first.
//!
//! Storage internals for [`ExecutionTables`] / [`ScratchNamespace`] stay
//! placeholder until Task 50 lands the concrete `DataFrameStore`. The
//! [`InputDataManager`] plan is consumed by that data plane to populate
//! [`ExecutionTables`] from Parquet snapshots.

mod error;

pub mod aggregation;
pub mod calculator;
pub mod execution;
pub mod input;
pub mod masterloop;

pub use error::{Error, Result};

pub use aggregation::*;
pub use calculator::*;
pub use execution::*;
pub use input::*;
pub use masterloop::*;

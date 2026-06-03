//! `moves-framework` — execution runtime that wires RunSpecs to calculators.
//!
//! Will port the master-loop infrastructure from
//! `gov/epa/otaq/moves/master/framework/` (Java) and the granularity
//! hierarchy already characterized in `moves-calculator-info`. The crate
//! owns the runtime view of a RunSpec (`ExecutionRunSpec`), the
//! `(county, zone, link)` location iterator, the calculator registry and
//! scheduler, and the per-iteration `DataFrameStore` lifecycle.
//!
//! See `moves-rust-.md`:
//!
//! * — `ExecutionRunSpec` (this commit)
//! * — Location iterator (this commit)
//! * — MasterLoop subscription model
//! * — Calculator and Generator base traits
//! * — `CalculatorRegistry`
//! * — MasterLoop core iteration (this commit)
//! * — Granularity-based loop notification (refines dispatch)
//! * — `ExecutionDatabaseSchema` and `CalculatorContext`
//! * — `InputDataManager` (this commit)
//! * — Output aggregation planning (this commit)
//! * — `OutputProcessor` plan-driven output aggregation (this commit)
//! * — `MOVESEngine` and the bounded-concurrency executor (this commit)
//! * — `DataFrameStore` (shared with `moves-data`)
//! * — Unified Parquet output writer (this commit)
//!
//! # status
//!
//!, 16, 17, 18, 19, 20, 21, 23, 24, 25, 26, 27, 50, and 89 are in place:
//!
//! * — [`ExecutionRunSpec`] derives the run-time view of a
//! [`moves_runspec::RunSpec`]: target pollutants / processes, timespan
//! sets, vehicle selections, and the refueling-process dependency
//! closure. Database-dependent state (locations, fuel years, regions)
//! stays empty until / 24 populate it.
//! * — [`ExecutionLocationProducer`] expands a RunSpec's
//! geographic selections into the sorted [`ExecutionLocation`] set the
//! MasterLoop iterates; [`ExecutionRunSpec::build_execution_locations`]
//! drives it from a [`GeographyTables`] value and re-derives the
//! `states` / `counties` / `zones` / `links` projections.
//! * — [`MasterLoopableSubscription`] ordering matches Java exactly.
//! * — [`Calculator`] / [`Generator`] traits plus
//! [`CalculatorSubscription`].
//! * — [`CalculatorRegistry`] for chain-DAG factory bindings,
//! RunSpec filtering, and topological ordering.
//! * — [`MasterLoop`] iteration engine walking
//! `iteration → process → state → county → zone → link → year → month →
//! day → hour`, with forward/cleanup dispatch around the
//! [`IterationPosition`] triple.
//! * — `notifyLoopablesOfLoopChange` / `hasLoopables` dispatch:
//! priority-ordered notification at each level (live since),
//! plus the `hasLoopables` short-circuit that descends the time nest
//! only as deep as the finest registered subscription.
//! * — [`CalculatorContext`] owns [`ExecutionTables`],
//! [`ScratchNamespace`], and the [`IterationPosition`] triple; the
//! [`ExecutionDatabaseSchema`] registry defines which tables may appear
//! in the execution database.
//! * — [`InputDataManager`] ports the default-DB-to-execution-DB
//! filter logic: [`RunSpecFilters`] projects RunSpec selections,
//! [`WhereClauseBuilder`] emits structured per-dimension predicates,
//! and [`InputDataManager::plan`] walks the
//! [`default_tables`] registry to produce a [`MergePlan`].
//! * — [`emission_aggregation`] / [`activity_aggregation`] /
//! [`base_rate_aggregation`] derive the column-shape [`AggregationPlan`]
//! (group-by keys, collapsed columns, `SUM` metric) from a RunSpec.
//! * — [`aggregate_emissions`] / [`aggregate_activity`] apply an
//! [`AggregationPlan`] to a record batch (group-by + `SUM` + temporal
//! rescaling); [`OutputProcessor::write_aggregated_emissions`] composes
//! the roll-up with the writer.
//! * — [`MOVESEngine`] is the entry point that ties the framework
//! together: it builds an [`ExecutionRunSpec`], filters and topologically
//! orders the calculator graph through the [`CalculatorRegistry`], splits
//! it into independent chains with [`chunk_chains`], runs one
//! [`MasterLoop`] per chain through the [`BoundedExecutor`] (a
//! `rayon::ThreadPool` + [`Semaphore`] sized by `--max-parallel-chunks`),
//! and finalises the [`OutputProcessor`]. Peak memory scales linearly
//! with the parallelism limit; see [`crate::execution::executor`] for the
//! memory model.
//! * — [`DataFrameStore`] trait + [`InMemoryStore`] implementation
//! back [`ExecutionTables`] and [`ScratchNamespace`]; both tiers are wired
//! through [`CalculatorContext`]. [`InternalControlStrategy::pre_run`]
//! receives `&mut InMemoryStore` so strategies can write to the slow tier
//! before the master loop begins. `DistanceCalculator::execute` is the
//! pilot that reads all seven input tables and emits a distance activity
//! `DataFrame` end-to-end. `AvftControlStrategy::pre_run` writes the
//! completed AVFT fleet-composition table into the slow tier.
//! * — [`OutputProcessor`], the strongly-typed Parquet writer for
//! the three output tables defined by [`moves_data::output_schema`].
//! calculators feed it [`moves_data::EmissionRecord`] /
//! [`moves_data::ActivityRecord`] batches; ([`aggregate_emissions`])
//! rolls those batches up through an [`AggregationPlan`] first.

mod error;

pub mod aggregation;
pub mod calculator;
pub mod control_strategy;
pub mod data;
pub mod execution;
pub mod input;
pub mod masterloop;

pub use error::{Error, Result};

// `ModelScale` and `ModelDomain` appear in the public `CalculatorContext` API
// (`model_scale()`, `model_domain()`, `is_project()`), so downstream calculator
// crates need to name them without taking a direct `moves-runspec` dependency.
pub use moves_runspec::model::{ModelDomain, ModelScale};

pub use aggregation::*;
pub use calculator::*;
pub use control_strategy::{
    ControlStrategyFactory, ControlStrategyRegistry, InternalControlStrategy, StrategySubscription,
};
#[cfg(not(target_arch = "wasm32"))]
pub use data::DataFrameStoreParquet;
#[cfg(not(target_arch = "wasm32"))]
pub use data::{read_execution_bundle, read_execution_bundle_filtered};
pub use data::{
    schema_registry, DataFrameStore, DataFrameStoreTyped, InMemoryStore, IntoDataFrame,
    TableHandle, TableRow, TableSchema, KNOWN_CALCULATOR_INPUT_TABLES,
};
pub use execution::*;
pub use input::*;
pub use masterloop::*;

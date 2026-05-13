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
//! * Task 15 — `ExecutionRunSpec`
//! * Task 16 — Location iterator
//! * Task 17 — MasterLoop subscription model
//! * Task 18 — Calculator and Generator base traits
//! * Task 19 — `CalculatorRegistry`
//! * Task 20 — MasterLoop core iteration (this commit)
//! * Task 21 — Granularity-based loop notification (refines Task 20 dispatch)
//! * Task 23 — `ExecutionDatabaseSchema` and `CalculatorContext`
//! * Task 24 — `InputDataManager` (this commit)
//! * Task 25 — Output aggregation planning (this commit)
//! * Task 26 — `OutputProcessor` plan-driven output aggregation (this commit)
//! * Task 50 — `DataFrameStore` (shared with `moves-data`)
//! * Task 89 — Unified Parquet output writer (this commit)
//!
//! # Phase 2 status
//!
//! Tasks 17, 18, 19, 20, 21, 23, 24, 25, 26, and 89 are in place:
//!
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
//! * Task 89 — [`OutputProcessor`], the strongly-typed Parquet writer for
//!   the three output tables defined by [`moves_data::output_schema`].
//!   Phase 3 calculators feed it [`moves_data::EmissionRecord`] /
//!   [`moves_data::ActivityRecord`] batches; Task 26 ([`output_aggregate`])
//!   rolls those batches up through an [`AggregationPlan`] first.
//!
//! Storage internals for [`ExecutionTables`] / [`ScratchNamespace`] stay
//! placeholder until Task 50 lands the concrete `DataFrameStore`. The
//! [`InputDataManager`] plan is consumed by that data plane to populate
//! [`ExecutionTables`] from Parquet snapshots.

pub mod aggregation;
pub mod calculator;
mod error;
pub mod execution_db;
pub mod input_data_manager;
pub mod master_loop;
pub mod output_aggregate;
pub mod output_processor;
pub mod registry;

pub use aggregation::{
    activity_aggregation, base_rate_aggregation, emission_aggregation, AggregationColumn,
    AggregationInputs, AggregationPlan, AggregationTable, TemporalScaling,
};
pub use calculator::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, Generator,
};
pub use error::{Error, Result};
pub use execution_db::{
    ExecutionDatabaseSchema, ExecutionLocation, ExecutionTableSpec, ExecutionTables, ExecutionTime,
    IterationPosition, ScratchNamespace, TableSource,
};
pub use input_data_manager::{
    default_tables, InputDataManager, MergePlan, MergeTableSpec, RunSpecFilters, TableMergePlan,
    WhereClause, WhereClauseBuilder,
};
pub use master_loop::{
    Granularity, MasterLoop, MasterLoopContext, MasterLoopable, MasterLoopableSubscription,
};
pub use output_aggregate::{
    aggregate_activity, aggregate_emissions, TemporalScalingFactors, UnitScaling,
};
pub use output_processor::{OutputProcessor, NULL_PARTITION, PARQUET_CREATED_BY};
pub use registry::{CalculatorFactory, CalculatorRegistry, GeneratorFactory, ModuleFactory};

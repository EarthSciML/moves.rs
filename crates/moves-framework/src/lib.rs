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
//! * Task 26 — `OutputProcessor` (Phase 2 skeleton, this commit via Task 89)
//! * Task 50 — `DataFrameStore` (shared with `moves-data`)
//! * Task 89 — Unified Parquet output writer (this commit)
//!
//! # Phase 2 status
//!
//! Tasks 17, 18, 19, 20, 23, and 89 are in place:
//!
//! * Task 17 — [`MasterLoopableSubscription`] ordering matches Java exactly.
//! * Task 18 — [`Calculator`] / [`Generator`] traits plus
//!   [`CalculatorSubscription`].
//! * Task 19 — [`CalculatorRegistry`] for chain-DAG factory bindings,
//!   RunSpec filtering, and topological ordering.
//! * Task 20 — [`MasterLoop`] iteration engine walking
//!   `iteration → process → state → county → zone → link → year → month →
//!   day → hour`, with forward/cleanup dispatch around the Task 23
//!   [`IterationPosition`] triple. The basic priority-ordered walk is
//!   live; Task 21 will refine it with the `hasLoopables` short-circuit
//!   and try-finally cleanup semantics from `notifyLoopablesOfLoopChange`.
//! * Task 23 — [`CalculatorContext`] owns [`ExecutionTables`],
//!   [`ScratchNamespace`], and the [`IterationPosition`] triple; the
//!   [`ExecutionDatabaseSchema`] registry defines which tables may appear
//!   in the execution database.
//! * Task 89 — [`OutputProcessor`], the strongly-typed Parquet writer for
//!   the three output tables defined by [`moves_data::output_schema`].
//!   Phase 3 calculators feed it [`moves_data::EmissionRecord`] /
//!   [`moves_data::ActivityRecord`] batches; Task 26 widens the API to
//!   accept Polars `DataFrame`s once Task 50 lands the data plane.
//!
//! Storage internals for [`ExecutionTables`] / [`ScratchNamespace`] stay
//! placeholder until Task 50 lands the concrete `DataFrameStore`.

pub mod calculator;
mod error;
pub mod execution_db;
pub mod master_loop;
pub mod output_processor;
pub mod registry;

pub use calculator::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, Generator,
};
pub use error::{Error, Result};
pub use execution_db::{
    ExecutionDatabaseSchema, ExecutionLocation, ExecutionTableSpec, ExecutionTables, ExecutionTime,
    IterationPosition, ScratchNamespace, TableSource,
};
pub use master_loop::{
    Granularity, MasterLoop, MasterLoopContext, MasterLoopable, MasterLoopableSubscription,
};
pub use output_processor::{OutputProcessor, NULL_PARTITION, PARQUET_CREATED_BY};
pub use registry::{CalculatorFactory, CalculatorRegistry, GeneratorFactory, ModuleFactory};

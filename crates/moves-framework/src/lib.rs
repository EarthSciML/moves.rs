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
//! * Task 20 — MasterLoop core iteration
//! * Task 23 — `ExecutionDatabaseSchema` and `CalculatorContext` (this commit)
//! * Task 50 — `DataFrameStore` (shared with `moves-data`)
//!
//! # Phase 2 status
//!
//! Tasks 17, 18, and 23 are in place:
//!
//! * Task 17 — [`MasterLoopableSubscription`] ordering matches Java exactly.
//! * Task 18 — [`Calculator`] / [`Generator`] traits plus
//!   [`CalculatorSubscription`].
//! * Task 23 — [`CalculatorContext`] widened to own [`ExecutionTables`],
//!   [`ScratchNamespace`], and an [`IterationPosition`] triple; the
//!   [`ExecutionDatabaseSchema`] registry defines which tables may appear
//!   in the execution database.
//!
//! Storage internals for [`ExecutionTables`] / [`ScratchNamespace`] stay
//! placeholder until Task 50 lands the concrete `DataFrameStore`.

pub mod calculator;
mod error;
pub mod execution_db;
pub mod master_loop;
pub mod registry;

pub use calculator::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, Generator,
};
pub use error::{Error, Result};
pub use execution_db::{
    ExecutionDatabaseSchema, ExecutionLocation, ExecutionTableSpec, ExecutionTables, ExecutionTime,
    IterationPosition, ScratchNamespace, TableSource,
};
pub use master_loop::{Granularity, MasterLoopContext, MasterLoopable, MasterLoopableSubscription};
pub use registry::{CalculatorFactory, CalculatorRegistry, GeneratorFactory, ModuleFactory};

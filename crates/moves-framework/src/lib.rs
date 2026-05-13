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
//! * Task 23 — `ExecutionDatabaseSchema` and `CalculatorContext`
//! * Task 26 — `OutputProcessor` (Phase 2 skeleton, this commit via Task 89)
//! * Task 50 — `DataFrameStore` (shared with `moves-data`)
//! * Task 89 — Unified Parquet output writer (this commit)
//!
//! # Phase 2 status
//!
//! Task 18 lands: the [`Calculator`] and [`Generator`] traits, the
//! [`CalculatorSubscription`] declaration record, and skeleton
//! [`CalculatorContext`] / [`CalculatorOutput`] placeholder types that
//! Task 23 / Task 50 will widen. Task 17 (subscription ordering) is in
//! place; the rest is still skeleton.
//!
//! Task 89 lands the [`OutputProcessor`] — the strongly-typed Parquet
//! writer for the three output tables defined by
//! [`moves_data::output_schema`]. Phase 3 calculators feed it
//! [`moves_data::EmissionRecord`] / [`moves_data::ActivityRecord`]
//! batches; Task 26 widens the API to accept Polars `DataFrame`s once
//! Task 50 lands the data plane.

pub mod calculator;
mod error;
pub mod master_loop;
pub mod output_processor;

pub use calculator::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, Generator,
};
pub use error::{Error, Result};
pub use master_loop::{Granularity, MasterLoopContext, MasterLoopable, MasterLoopableSubscription};
pub use output_processor::{OutputProcessor, NULL_PARTITION, PARQUET_CREATED_BY};

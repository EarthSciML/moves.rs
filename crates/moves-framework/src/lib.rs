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
//! * Task 17 — MasterLoop scheduler
//! * Task 19 — `CalculatorRegistry`
//! * Task 50 — `DataFrameStore` (shared with `moves-data`)
//!
//! # Phase 2 status
//!
//! Skeleton crate. Implementation lands in Tasks 15–19.

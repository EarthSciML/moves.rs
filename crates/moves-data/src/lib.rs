//! `moves-data` — schema-aware data plane backing the calculator chain.
//!
//! Replaces the MariaDB-backed `Connection` threading in legacy MOVES with a
//! Polars-based `DataFrameStore`: input tables loaded lazily from MOVES default
//! databases (now Parquet snapshots), intermediate tables produced by
//! calculators, and final output tables written to disk via `moves-framework`'s
//! output writer. Schema definitions and the pollutant/process/source-type/
//! road-type enums live here so every consumer agrees on column names and types.
//!
//! See `moves-rust-migration-plan.md`:
//!
//! * Task 14 — Pollutant/process/source-type/road-type enums
//! * Task 50 — `DataFrameStore`
//! * Task 89 — Unified Parquet output schema
//!
//! # Phase 2 status
//!
//! Skeleton crate. Implementation begins in Task 14.

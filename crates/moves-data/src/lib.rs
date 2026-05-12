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
//! # Definitional types (Task 14)
//!
//! [`Pollutant`], [`EmissionProcess`], [`RoadType`], [`SourceType`], and
//! [`PollutantProcessAssociation`] port the small Java classes of the same
//! names from `gov/epa/otaq/moves/master/framework/`. Each one is a `Copy`
//! value type (id + static name); a `phf`-backed `find_by_id` /
//! `find_by_name` pair replaces the Java mutable-`TreeSet` registry.
//!
//! The canonical entries are derived from the MOVES default database at
//! commit `25dc6c833dd8c88198f82cee93ca30be1456df8b` (MOVES5.0.1, default
//! DB `movesdb20241112`). Pollutant and process names come from the
//! `characterization/calculator-chains/calculator-dag.json` execution-chain
//! enumeration; road-type and source-type names come from the MOVES5
//! technical reference (the `RoadType` and `SourceUseType` tables). The
//! runtime DB layer (Task 50, Phase 4) reconciles these against the live
//! default DB at startup; any drift is a deliberate cutover.

pub mod error;
pub mod pollutant;
pub mod pollutant_process;
pub mod process;
pub mod road_type;
pub mod source_type;

pub use error::{Error, Result};
pub use pollutant::{Pollutant, PollutantId};
pub use pollutant_process::{PolProcessId, PollutantProcessAssociation};
pub use process::{EmissionProcess, ProcessId};
pub use road_type::{RoadType, RoadTypeId};
pub use source_type::{SourceType, SourceTypeId};

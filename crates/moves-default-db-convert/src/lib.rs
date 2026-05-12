//! Phase 4 Task 80: convert a MOVES default-DB MariaDB dump into the
//! versioned Parquet layout consumed by the lazy-loading reader.
//!
//! The conversion pipeline is split into two stages so the SIF-bound MariaDB
//! step and the pure-Rust Parquet write are independently testable:
//!
//! 1. **Dump stage** (shell, runs inside `canonical-moves.sif`): start
//!    MariaDB, dump every default-DB table to TSV plus a column-schema
//!    sidecar, exit. Lives at
//!    `characterization/default-db-conversion/dump-default-db.sh`.
//! 2. **Convert stage** (this crate, runs on the host): read the TSV
//!    pairs, apply the partition plan from
//!    `characterization/default-db-schema/tables.json`, write Parquet to
//!    `<output-root>/movesdb<YYYYMMDD>/<table>/...`, and produce
//!    `manifest.json`.
//!
//! Re-runnability for future EPA releases comes for free: bump the
//! `movesdb<DATE>` directory, re-run the dumper against the new SIF, point
//! the converter at the new TSV directory.
//!
//! See `characterization/default-db-conversion/README.md` for the
//! orchestration recipe, and `moves-rust-migration-plan.md` Task 80.

pub mod convert;
pub mod error;
pub mod manifest;
pub mod parquet_writer;
pub mod partition;
pub mod plan;
pub mod tsv;
pub mod types;
pub mod validate;

pub use convert::{convert, ConvertOptions, ConvertReport};
pub use error::{Error, Result};
pub use manifest::{Manifest, TableManifest, MANIFEST_FILENAME};
pub use plan::{PartitionPlan, PartitionStrategy, TableEntry};
pub use validate::{validate, ValidateOptions, ValidationReport, ValidationSummary};

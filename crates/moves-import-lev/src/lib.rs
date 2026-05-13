//! `moves-import-lev` — LEV/NLEV alternative-rate input importer.
//!
//! Phase 4 Task 87 of the `moves.rs` migration. Reads a user-supplied
//! CSV of Low-Emission-Vehicle or National-LEV emission rates by age
//! and writes a byte-deterministic Parquet file shaped like the
//! corresponding default-DB table.
//!
//! The two target tables —
//! [`EmissionRateByAgeLEV`](LevKind::Lev) and
//! [`EmissionRateByAgeNLEV`](LevKind::Nlev) — share an identical column
//! schema, so a single code path handles both; only the destination
//! filename differs. See [`schema::COLUMNS`] for the canonical column
//! layout, sourced from `characterization/default-db-schema/tables.json`.
//!
//! ## CSV format
//!
//! * Comma-separated. First non-empty, non-`#`-comment line is the
//!   header.
//! * Required columns (must appear in the header and have a value in
//!   every row):
//!   - `sourceBinID` (integer)
//!   - `polProcessID` (integer)
//!   - `opModeID` (integer)
//!   - `ageGroupID` (integer)
//!   - `meanBaseRate` (non-negative float)
//! * Optional columns (any subset; missing columns become SQL NULL):
//!   - `meanBaseRateCV` (non-negative float)
//!   - `meanBaseRateIM` (non-negative float)
//!   - `meanBaseRateIMCV` (non-negative float)
//!   - `dataSourceId` (integer)
//! * Header column order is free; the reader matches by name.
//! * Empty cells, blank cells, and the literal `NULL` (any case) all
//!   represent SQL NULL on output.
//!
//! ## Validation
//!
//! Beyond shape and type checks, the importer rejects:
//!
//! * Unknown header columns (catches typos).
//! * Duplicate header columns.
//! * Empty cells in required columns.
//! * Negative or non-finite rate values.
//! * Duplicate primary-key tuples
//!   `(sourceBinID, polProcessID, opModeID, ageGroupID)`.
//!
//! ## Example
//!
//! ```no_run
//! use std::path::Path;
//! use moves_import_lev::{import_lev, ImportReport};
//!
//! let report: ImportReport = import_lev(
//!     Path::new("inputs/lev-rates.csv"),
//!     Path::new("inputs/parquet"),
//! )?;
//! println!(
//!     "wrote {} rows to {} (sha256 {})",
//!     report.row_count,
//!     report.output_path.display(),
//!     report.sha256
//! );
//! # Ok::<(), moves_import_lev::Error>(())
//! ```
//!
//! ## Determinism
//!
//! The Parquet writer is pinned to the same settings as
//! `moves-default-db-convert` (uncompressed, no dictionary, no
//! statistics, `PARQUET_1_0` writer version). Combined with the
//! validator preserving input row order, the same CSV always hashes to
//! the same Parquet output — exactly what the Phase 4 Task 88
//! validation suite needs.
//!
//! See `moves-rust-migration-plan.md` Task 87.

pub mod csv_reader;
pub mod error;
pub mod importer;
pub mod parquet_writer;
pub mod schema;
pub mod validate;

pub use csv_reader::{Csv, CsvRow};
pub use error::{Error, Result};
pub use importer::{import, import_lev, import_nlev, parquet_path_for, ImportReport};
pub use parquet_writer::{encode, write_atomic, ParquetOutput, PARQUET_CREATED_BY};
pub use schema::{arrow_schema, column_index, Column, ColumnKind, LevKind, COLUMNS};
pub use validate::{validate, TypedRow, TypedValue};

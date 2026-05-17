//! `moves-nonroad-import` — Phase 4 Task 85.
//!
//! Convert user-supplied Nonroad-input CSV templates into the same
//! Parquet layout the default-DB converter (`moves-default-db-convert`)
//! produces. The two crates serve distinct sources (canonical default DB
//! vs. user override) but share the same downstream consumer
//! (`moves-data-default`) and the same determinism contract.
//!
//! ## Pipeline
//!
//! Per importer, in order:
//!
//! 1. **Read.** [`csv::read_csv`] parses the user's `<table>.csv` into a
//!    [`csv::CsvFile`].
//! 2. **Validate.** [`convert::convert_table`] verifies the header,
//!    coerces every cell into a typed [`parquet_writer::Cell`], runs
//!    per-cell rules (see [`schema::Rule`]), checks for duplicate
//!    primary keys, and applies cross-row invariants (currently
//!    [`schema::CrossRowInvariant::FractionSum`]).
//! 3. **Write.** [`parquet_writer::encode_parquet`] serialises the
//!    typed rows into a deterministic Parquet byte buffer; the
//!    orchestrator atomically writes it to disk.
//! 4. **Manifest.** A [`manifest::Manifest`] (schema tag
//!    `moves-nonroad-import-manifest/v1`) sits next to the Parquet
//!    files so a downstream loader can discover what's present without
//!    walking the directory.
//!
//! ## Built-in importers
//!
//! Phase 4 Task 85 names four importers:
//!
//! | Importer (table)        | What it carries                                |
//! |-------------------------|------------------------------------------------|
//! | `nrbaseyearequippopulation` | Base-year equipment population by source × state |
//! | `nrengtechfraction`        | Engine-technology fraction by model year (the Nonroad analogue of on-road `AgeDistribution`) |
//! | `nrretrofitfactors`        | Retrofit annual & effective fractions by SCC × engTech × hp × pollutant × retrofitID |
//! | `nrmonthallocation`        | Per-equipment monthly allocation fractions by state |
//!
//! ## Caller
//!
//! ```no_run
//! use moves_nonroad_import::{import, ImportOptions};
//!
//! let (_manifest, report) = import(&ImportOptions::new("input/", "output/"))?;
//! eprintln!("wrote {} tables ({} rows)", report.tables_written.len(), report.total_rows);
//! # Ok::<(), moves_nonroad_import::Error>(())
//! ```
//!
//! See `moves-rust-migration-plan.md` Task 85 and the sibling crate
//! `moves-default-db-convert` (Task 80) for the matching default-DB
//! pipeline.

pub mod convert;
pub mod csv;
pub mod error;
pub mod importer;
pub mod manifest;
pub mod parquet_writer;
pub mod schema;
pub mod tables;

pub use convert::{convert_table, Converted};
pub use csv::{read_csv, CsvFile, CsvRow};
pub use error::{Error, Result};
pub use importer::{import, read_manifest, ImportOptions, ImportReport};
pub use manifest::{ColumnManifest, Manifest, TableManifest, MANIFEST_FILENAME};
pub use parquet_writer::{encode_parquet, sha256_hex, Cell, ParquetOutput, TypedRow};
pub use schema::{Column, CrossRowInvariant, Rule, TableSchema};
pub use tables::{find as find_table, ImporterEntry};

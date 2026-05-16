//! `moves-importer` — shared framework for porting MOVES input-database
//! importers to Rust (Phase 4 Tasks 83-87).
//!
//! Canonical MOVES has 26 importers under
//! `gov/epa/otaq/moves/master/implementation/importers/` plus framework
//! glue in `gov/epa/otaq/moves/master/framework/importers/`. Every one
//! follows the same shape:
//!
//! 1. A `dataTableDescriptor` declares one or more tables, each as a
//!    sequence of `(column_name, decode_table, filter_constant)` triples.
//! 2. A SQL validation script under `database/<XYZ>Importer.sql`
//!    populates `importTempMessages` with row-level error messages,
//!    plus a `case when ... 'OK' else 'NOT_READY'` sentinel.
//! 3. The runtime loads user CSV/XLS into a MariaDB scratch table, runs
//!    the validation script, and gates the rest of the run on the
//!    sentinel.
//!
//! This crate factors that shape into Rust types so the County (CDB),
//! Project (PDB), and Nonroad importers don't each reinvent the
//! reader/validator/writer plumbing:
//!
//! * [`Filter`] enumerates the validation constraints MOVES uses
//!   (foreign-key tables, numeric ranges, year ranges). See the
//!   `FILTER_*` constants in `ImporterManager.java` for the canonical
//!   list — we port them one-for-one as variants.
//! * [`ColumnDescriptor`] pairs a column name with the [`Filter`] that
//!   constrains its values and the [`arrow::datatypes::DataType`] it
//!   maps to in the default-DB schema.
//! * [`TableDescriptor`] groups columns for one logical table together
//!   with the primary-key columns used by Parquet sort order.
//! * [`Importer`] is the per-importer trait: bind a name, list tables,
//!   and (optionally) override [`Importer::validate_imported`] to add
//!   the cross-row checks the SQL script does (allocation-factor sums,
//!   coverage of required (year, sourceTypeID) tuples, etc.).
//!
//! Concrete importers live in sibling crates: `moves-importer-county`
//! for Task 83, `moves-importer-project` for Task 84 (TBD), and
//! `moves-importer-nonroad` for Task 85 (TBD).
//!
//! ## Read → validate → write
//!
//! The end-to-end import path is:
//!
//! 1. [`reader::read_csv_table`] — parse a header-bearing CSV into an
//!    Arrow [`RecordBatch`](arrow::record_batch::RecordBatch) typed
//!    against the column types declared by the [`TableDescriptor`].
//!    Headers match case-insensitively; empty cells and the literal
//!    `NULL` token become Arrow nulls.
//! 2. [`validator::validate_table`] — run the [`Filter`] for each
//!    column and any [`Importer::validate_imported`] hook the importer
//!    overrides. Errors and warnings flow back as
//!    [`validator::ValidationMessage`] values without aborting; the
//!    caller decides which severity is fatal.
//! 3. [`writer::write_table_parquet`] — sort by the descriptor's
//!    primary key and serialize to byte-deterministic Parquet using the
//!    same writer settings the `moves-default-db-convert` pipeline
//!    uses, so importer output and default-DB output can be diff'd by
//!    SHA-256.
//!
//! Cross-table validation that needs the default DB (foreign-key
//! lookups, decode-table coverage) takes a [`ValidationContext`] holding
//! a [`moves_data_default::DefaultDb`] handle.

pub mod descriptor;
pub mod error;
pub mod filter;
pub mod importer;
pub mod reader;
pub mod validator;
pub mod writer;

pub use descriptor::{ColumnDescriptor, TableDescriptor};
pub use error::{Error, Result};
pub use filter::Filter;
pub use importer::Importer;
pub use reader::read_csv_table;
pub use validator::{
    validate_table, ImportedTable, Severity, ValidationContext, ValidationMessage,
};
pub use writer::{write_table_parquet, TableOutput};

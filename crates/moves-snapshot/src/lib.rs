//! Canonical snapshot format for MOVES fixture outputs.
//!
//! A snapshot is a directory layout of normalized tables plus a manifest
//! sidecar:
//!
//! ```text
//! <snapshot-dir>/
//!   manifest.json                ← format version, table list, aggregate hash
//!   tables/
//!     <name>.parquet             ← row-stable, fixed-decimal-encoded floats
//!     <name>.meta.json           ← schema, natural key, per-table content hash
//! ```
//!
//! Determinism guarantees:
//! * Rows are sorted lexicographically on the declared natural-key columns.
//! * Float columns are rounded to [`FLOAT_DECIMALS`] places and stored as
//!   fixed-decimal strings, eliminating float-formatting drift across
//!   platforms and stdlib versions.
//! * Parquet output is uncompressed, dictionary-disabled, statistics-disabled,
//!   and stamped with a fixed `created_by` — so the same [`Table`] always
//!   serializes to the same bytes.
//! * The manifest enumerates tables in lexicographic order; per-table metadata
//!   serializes via `serde_json` with a stable struct order, and JSON files
//!   are written with a trailing newline.

pub mod diff;
pub mod error;
pub mod format;
pub mod manifest;
pub mod snapshot;
pub mod table;
pub mod tolerance;

pub use diff::{diff_snapshots, Diff, DiffOptions, DiffSummary, RowDiff, SchemaDiff, TableChange};
pub use error::{Error, Result};
pub use format::{ColumnKind, ColumnSpec, FLOAT_DECIMALS, FORMAT_VERSION};
pub use manifest::{compute_aggregate_hash, sha256_hex, Manifest, ManifestEntry, TableMetadata};
pub use snapshot::Snapshot;
pub use table::{NormalizedColumn, Table, TableBuilder, Value};
pub use tolerance::{ToleranceConfig, ToleranceError};

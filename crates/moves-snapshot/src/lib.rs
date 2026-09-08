//! Canonical snapshot format for MOVES fixture outputs.
//!
//! A snapshot is a directory layout of normalized tables plus a manifest
//! sidecar:
//!
//! ```text
//! <snapshot-dir>/
//! manifest.json ← format version, table list, aggregate hash
//! tables/
//! <name>.parquet ← row-stable, canonically-encoded floats
//! <name>.meta.json ← schema, natural key, float encoding, content hash
//! ```
//!
//! Format versions:
//! * `moves-snapshot/v1` stored floats as fixed-decimal strings with twelve
//!   places after the point. Twelve decimal places is not twelve significant
//!   digits, so small-magnitude columns were truncated at capture time —
//!   `nrdioxinemissionrate.meanBaseRate` kept four significant digits for one
//!   pollutant and two for another. Still readable; no longer written.
//! * `moves-snapshot/v2` stores the shortest correctly-rounded decimal that
//!   parses back to a bit-identical f64, in normalized scientific notation
//!   ([`format::float_to_canonical`]). Lossless at every magnitude.
//!
//! [`Snapshot::load`] accepts every version in
//! [`SUPPORTED_FORMAT_VERSIONS`] and remembers which one it read, so an
//! older snapshot rewritten by a newer binary keeps its original bytes.
//! Each table's `.meta.json` records the rule that produced its cells: v1
//! writes `float_decimals`, v2 writes a tagged `float_encoding` object, and
//! [`TableMetadata::float_encoding`] resolves either into a
//! [`FloatEncoding`] that can report the storage quantum a consumer should
//! floor its tolerances at.
//!
//! Determinism guarantees:
//! * Rows are sorted on the declared natural-key columns — float keys by IEEE
//!   total order, every other kind lexicographically.
//! * Float columns are stored as canonical decimal strings, eliminating
//!   float-formatting drift across platforms and stdlib versions. The v2 rule
//!   is defined as a property of the *number*, not of any formatter, so it
//!   does not depend on locale, float-formatting mode, or the tie-breaking
//!   choices of a shortest-representation printer.
//! * Parquet output is uncompressed, dictionary-disabled, statistics-disabled,
//!   and stamped with a fixed `created_by` — so the same [`Table`] always
//!   serializes to the same bytes.
//! * The manifest enumerates tables in lexicographic order; per-table metadata
//!   serializes via `serde_json` with a stable struct order, and JSON files
//!   are written with a trailing newline.

pub mod bundle;
pub mod diff;
pub mod error;
pub mod format;
pub mod manifest;
pub mod output_compare;
pub mod snapshot;
pub mod table;
pub mod tolerance;

pub use bundle::{write_execution_bundle, BUNDLE_FILE, BUNDLE_MAGIC};
pub use diff::{diff_snapshots, Diff, DiffOptions, DiffSummary, RowDiff, SchemaDiff, TableChange};
pub use error::{Error, Result};
pub use format::{
    ColumnKind, ColumnSpec, FloatEncoding, FormatProfile, FORMAT_VERSION, MAX_SIGNIFICANT_DIGITS,
    SUPPORTED_FORMAT_VERSIONS, V1_FLOAT_DECIMALS,
};
pub use manifest::{compute_aggregate_hash, sha256_hex, Manifest, ManifestEntry, TableMetadata};
pub use output_compare::{
    compare_pollutant_sums, pollutant_sums_from_output_dir, pollutant_sums_from_snapshot,
    zero_valued_replaced_rows, PollutantComparison, PollutantRow, PollutantSums,
};
pub use snapshot::Snapshot;
pub use table::{NormalizedColumn, Table, TableBuilder, Value};
pub use tolerance::{ToleranceConfig, ToleranceError};

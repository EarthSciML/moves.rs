//! `moves-data-default` — lazy-loading reader over the converted MOVES
//! default-DB Parquet layout (Phase 4 Task 82).
//!
//! Pairs with the Phase 4 Task 80 conversion pipeline in
//! [`moves_default_db_convert`]. The converter writes a versioned
//! Parquet tree plus a `manifest.json`; this crate reads the manifest
//! and exposes the tree as Polars [`LazyFrame`](polars::prelude::LazyFrame)
//! values with partition pruning driven by typed filters.
//!
//! ```no_run
//! use moves_data_default::{DefaultDb, TableFilter};
//!
//! let db = DefaultDb::open("default-db/movesdb20241112")?;
//! // Monolithic lookup: no filter needed.
//! let source_use_type = db.scan("SourceUseType", &TableFilter::new())?
//!     .collect()?;
//! // Partitioned: filter by county.
//! let im_coverage_la = db
//!     .scan(
//!         "IMCoverage",
//!         &TableFilter::new()
//!             .partition_eq("countyID", 6037i64)
//!             .partition_in("yearID", [2020i64, 2025]),
//!     )?
//!     .collect()?;
//! # Ok::<(), moves_data_default::Error>(())
//! ```
//!
//! ## Design
//!
//! * **Manifest-first.** The reader trusts the `manifest.json` written
//!   by `moves-default-db-convert`. The manifest lists every Parquet
//!   file with its partition values; the reader's job is to filter that
//!   list before handing paths to Polars.
//! * **Polars `LazyFrame` everywhere.** Every read path returns a
//!   `LazyFrame` so callers can fuse selections / aggregations through
//!   the engine. `collect()` is left to the consumer (typically the
//!   `InputDataManager` in `moves-framework`).
//! * **Partition pruning only.** The reader doesn't (yet) push column
//!   predicates into the Parquet reader — the converter writes files
//!   with statistics disabled for byte-determinism, so row-group
//!   pushdown wouldn't help anyway. Express column predicates against
//!   the returned `LazyFrame`.
//! * **Schema-only sidecars.** Tables that ship empty in the default DB
//!   (`Link`, `SHO`, `SourceHours`, `Starts`) have no Parquet body.
//!   Calling [`DefaultDb::scan`] on them returns
//!   [`Error::SchemaOnly`]; use [`DefaultDb::schema_sidecar`] for the
//!   column types.
//!
//! ## Wiring into `moves-framework`
//!
//! The Phase 2 `InputDataManager` (Task 24) is the eventual consumer:
//! it walks the RunSpec, builds [`TableFilter`] values per active
//! selection, and calls [`DefaultDb::scan`] for each input table. Until
//! Task 24 lands, this crate is the de-facto entry point that downstream
//! Phase 4 importer crates (`CDB`, `PDB`, `Nonroad`, `AVFT`, `LEV/NLEV`)
//! can call directly.
//!
//! See `moves-rust-migration-plan.md` Tasks 80-82, and
//! `characterization/default-db-schema/partitioning-plan.md` for the
//! partition-strategy reference.

pub mod error;
pub mod filter;
pub mod manifest;
pub mod scan;
pub mod typed;

pub use error::{Error, Result};
pub use filter::{PartitionPredicate, PartitionValue, TableFilter};
pub use manifest::{
    find_table, load as load_manifest, ColumnManifest, Manifest, PartitionManifest,
    SchemaOnlySidecar, TableManifest, EXPECTED_SCHEMA_VERSION, MANIFEST_FILENAME,
};
pub use scan::DefaultDb;

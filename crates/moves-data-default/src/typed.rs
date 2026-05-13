//! Typed convenience accessors on top of the generic [`DefaultDb::scan`].
//!
//! The task spec for Phase 4 Task 82 calls for a typed API along the
//! shape of `DefaultDb::source_use_type_population(filter) -> Result<DataFrame>`.
//! Most tables don't need a hand-written wrapper — callers pass the
//! table name to [`DefaultDb::scan`] and get a [`LazyFrame`] — but a
//! handful of high-traffic lookup tables benefit from a typed-up
//! signature that names the columns and returns a materialized
//! [`DataFrame`].
//!
//! Add new wrappers here as downstream consumers (the InputDataManager,
//! per-importer crates) accumulate them. Keep them thin: the generic
//! [`DefaultDb::scan`] path remains the source of truth for partition
//! pruning, schema-only handling, and error reporting.
//!
//! [`DefaultDb::scan`]: crate::DefaultDb::scan

use polars::prelude::*;

use crate::error::Result;
use crate::filter::TableFilter;
use crate::scan::DefaultDb;

/// Convenience column-name constants for [`DefaultDb::source_use_type`].
/// Match the MOVES default-DB `SourceUseType` schema.
pub mod source_use_type {
    /// Source-type primary key (`smallint`).
    pub const SOURCE_TYPE_ID: &str = "sourceTypeID";
    /// HPMS vehicle category id (`smallint`).
    pub const HPMS_VTYPE_ID: &str = "HPMSVtypeID";
    /// Display name (`char(50)`).
    pub const SOURCE_TYPE_NAME: &str = "sourceTypeName";
}

impl DefaultDb {
    /// Load the `SourceUseType` lookup table as a materialized
    /// [`DataFrame`].
    ///
    /// `SourceUseType` is a 13-row monolithic dimension table — every
    /// MOVES run loads it whole. Returning a [`DataFrame`] (not
    /// [`LazyFrame`]) reflects that: the caller is going to `collect()`
    /// immediately anyway, and the typed accessor makes the call site
    /// read like a record lookup rather than a SQL scan.
    ///
    /// Columns (see the [`source_use_type`] sub-module for the
    /// stringly-typed constants):
    ///
    /// * `sourceTypeID` — `Int64`
    /// * `HPMSVtypeID` — `Int64`
    /// * `sourceTypeName` — `Utf8`
    pub fn source_use_type(&self) -> Result<DataFrame> {
        let lf = self.scan("SourceUseType", &TableFilter::new())?;
        Ok(lf.collect()?)
    }
}

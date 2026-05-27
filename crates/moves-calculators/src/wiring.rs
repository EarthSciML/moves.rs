//! Shared wiring helpers for calculator and generator `execute` bodies.
//!
//! Every bucket-A calculator follows the same three-step shape:
//!
//! ```text
//! 1. build_inputs(ctx)        — read tables, apply position filters
//! 2. kernel(&inputs)          — run the calculator algorithm
//! 3. emit_rows(rows)          — convert output rows to CalculatorOutput
//! ```
//!
//! Every generator follows a parallel shape:
//!
//! ```text
//! 1. build_inputs(ctx)        — read tables from ctx.tables()
//! 2. kernel(&inputs)          — run the generator algorithm
//! 3. write_scratch_table(ctx) — write output rows to ctx.scratch()
//! ```
//!
//! This module provides the non-calculator-specific glue:
//!
//! * [`position_filter`] — extract the master-loop position as a set of
//!   optional `i32` column predicates.
//! * [`PositionFilter::matches`] — test a `(year, county, process)` triple
//!   against the extracted filters.
//! * [`emit_rows`] — convert any `IntoDataFrame` row vector into a
//!   [`CalculatorOutput`] wrapping a `MOVESWorkerOutput` `DataFrame`.
//! * [`write_scratch_table`] — convert a row vector to a `DataFrame` and
//!   store it in the scratch namespace.
//!
//! All helpers are `pub(crate)` — they are implementation detail shared across
//! calculators and generators, not part of the public API.

use moves_framework::{CalculatorContext, CalculatorOutput, Error, IntoDataFrame, TableRow};

/// Position filters extracted from the current [`CalculatorContext`].
///
/// Each field is `None` when the master-loop iteration has not yet descended
/// to that dimension (e.g. at `COUNTY` granularity `process_id` is `None`).
/// A `None` field matches every row; a `Some(v)` field matches only rows
/// whose corresponding `INT` column equals `v`.
///
/// Build one with [`position_filter`], then use [`matches`](Self::matches) to
/// test individual rows from an iterator.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct PositionFilter {
    /// `yearID` predicate, derived from `ctx.position().time.year`.
    pub year: Option<i32>,
    /// `processID` predicate, derived from `ctx.position().process_id`.
    pub process_id: Option<i32>,
    /// `countyID` predicate, derived from `ctx.position().location.county_id`.
    pub county_id: Option<i32>,
}

impl PositionFilter {
    /// Returns `true` when a row with the given `(year_id, county_id,
    /// process_id)` values satisfies every concrete predicate.
    ///
    /// A `None` field is treated as a wildcard — the row always passes on
    /// that dimension. Call once per row in the `MOVESWorkerOutput` position
    /// filter inside each calculator's `build_inputs`.
    pub(crate) fn matches(&self, year_id: i32, county_id: i32, process_id: i32) -> bool {
        self.year.map_or(true, |y| y == year_id)
            && self.county_id.map_or(true, |c| c == county_id)
            && self.process_id.map_or(true, |p| p == process_id)
    }
}

/// Extract the master-loop position predicates from `ctx`.
///
/// Converts `u16` year, `u16` process-id value, and `u32` county-id to `i32`
/// to match the `INT` column type used in every calculator input table. The
/// resulting [`PositionFilter`] mirrors the SQL `##context.X##` macro
/// substitutions the Java calculators apply when extracting their input tables.
pub(crate) fn position_filter(ctx: &CalculatorContext) -> PositionFilter {
    let pos = ctx.position();
    PositionFilter {
        year: pos.time.year.map(i32::from),
        process_id: pos.process_id.map(|p| i32::from(p.0)),
        county_id: pos.location.county_id.map(|c| c as i32),
    }
}

/// Convert `rows` into a [`CalculatorOutput`] carrying a `MOVESWorkerOutput`
/// `DataFrame`.
///
/// Every bucket-A/B/C calculator ends its `execute` body the same way: convert
/// the emission row vector to a Polars `DataFrame` and wrap it in an output.
/// This helper folds that boilerplate so the per-calculator `execute` reads
/// "build inputs → run kernel → emit rows" with no repeated conversion code.
///
/// `T` is the caller's emission-row type; it must implement [`TableRow`], which
/// gives `Vec<T>` the [`IntoDataFrame`] blanket impl used to build the
/// `DataFrame`.
pub(crate) fn emit_rows<T: TableRow>(rows: Vec<T>) -> Result<CalculatorOutput, Error> {
    let df = rows
        .into_dataframe()
        .map_err(|e| Error::Polars(e.to_string()))?;
    Ok(CalculatorOutput::with_dataframe(df))
}

/// Write `rows` to the scratch namespace under `name` and return an empty
/// [`CalculatorOutput`].
///
/// The generator-side counterpart of [`emit_rows`]: every generator ends its
/// `execute` body the same way — convert the output row vector to a Polars
/// `DataFrame` and store it in the scratch namespace for downstream calculators.
/// This helper folds that boilerplate so the per-generator `execute` reads
/// "build inputs → run kernel → write scratch table" with no repeated conversion
/// code.
///
/// `T` is the caller's output-row type; it must implement [`TableRow`], which
/// gives `Vec<T>` the [`IntoDataFrame`] blanket impl used to build the
/// `DataFrame`. The write goes through [`crate::CalculatorContext::scratch_mut`]
/// raw insert, bypassing schema-registry validation — the scratch tier is
/// ephemeral and columns need not match the default-DB table shape.
pub(crate) fn write_scratch_table<T: TableRow>(
    ctx: &mut CalculatorContext,
    name: &str,
    rows: Vec<T>,
) -> Result<CalculatorOutput, Error> {
    let df = rows
        .into_dataframe()
        .map_err(|e| Error::Polars(e.to_string()))?;
    ctx.scratch_mut().insert(name, df);
    Ok(CalculatorOutput::empty())
}

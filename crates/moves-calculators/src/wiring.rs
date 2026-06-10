//! Shared wiring helpers for calculator and generator `execute` bodies.
//!
//! Every bucket-A calculator follows the same three-step shape:
//!
//! ```text
//! 1. build_inputs(ctx) — read tables, apply position filters
//! 2. kernel(&inputs) — run the calculator algorithm
//! 3. emit_rows(rows) — convert output rows to CalculatorOutput
//! ```
//!
//! Every generator follows a parallel shape:
//!
//! ```text
//! 1. build_inputs(ctx) — read tables from ctx.tables()
//! 2. kernel(&inputs) — run the generator algorithm
//! 3. write_scratch_table(ctx) — write output rows to ctx.scratch()
//! ```
//!
//! This module provides the non-calculator-specific glue:
//!
//! * [`position_filter`] — extract the master-loop position as a set of
//! optional `i32` column predicates.
//! * [`PositionFilter::matches`] — test a `(year, county, process)` triple
//! against the extracted filters.
//! * [`emit_rows`] — convert any `IntoDataFrame` row vector into a
//! [`CalculatorOutput`] wrapping a `MOVESWorkerOutput` `DataFrame`.
//! * [`write_scratch_table`] — convert a row vector to a `DataFrame` and
//! store it in the scratch namespace.
//!
//! All helpers are `pub(crate)` — they are implementation detail shared across
//! calculators and generators, not part of the public API.

use std::collections::HashSet;

use polars::prelude::{Column, DataFrame, DataType, PolarsResult};

use moves_framework::{
    CalculatorContext, CalculatorOutput, DataFrameStore, Error, IntoDataFrame, TableRow,
};

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

/// The shared `OpModeDistribution` scratch table and its canonical superset
/// schema, in column order. Every OMD generator's frame is projected onto this
/// so frames from different producers concatenate cleanly.
const OMD_TABLE: &str = "OpModeDistribution";
const OMD_COLUMNS: [&str; 7] = [
    "sourceTypeID",
    "roadTypeID",
    "linkID",
    "hourDayID",
    "polProcessID",
    "opModeID",
    "opModeFraction",
];

fn polars_err(e: impl std::fmt::Display) -> Error {
    Error::Polars(e.to_string())
}

/// Project an OMD producer's frame onto [`OMD_COLUMNS`]: synthesize a
/// zero-filled `roadTypeID` if the producer doesn't emit one (the Start and Evap
/// distributions are off-network and no `OpModeDistribution` reader extracts
/// `roadTypeID`), reorder to the canonical column order, and cast to the
/// canonical dtypes (`Int32` ids + `Float64` fraction). The cast matters because
/// frames must vstack against each other AND against a *provided* slow-tier
/// `OpModeDistribution`, which the default DB ships with `Int64` columns.
fn normalize_omd(mut df: DataFrame) -> PolarsResult<DataFrame> {
    if df.column("roadTypeID").is_err() {
        let n = df.height();
        df.with_column(Column::new("roadTypeID".into(), vec![0i32; n]))?;
    }
    let height = df.height();
    let cols = OMD_COLUMNS
        .iter()
        .map(|&name| {
            let dtype = if name == "opModeFraction" {
                DataType::Float64
            } else {
                DataType::Int32
            };
            df.column(name)?.cast(&dtype)
        })
        .collect::<PolarsResult<Vec<Column>>>()?;
    DataFrame::new(height, cols)
}

/// The distinct `polProcessID`s present in an OMD frame.
fn omd_pol_procs(df: &DataFrame) -> PolarsResult<HashSet<i32>> {
    Ok(df
        .column("polProcessID")?
        .i32()?
        .into_iter()
        .flatten()
        .collect())
}

/// Whether an OMD generator has nothing left to contribute this chunk, so it
/// should return without rebuilding its (expensive) distribution.
///
/// True when either its own `marker` is already in scratch (a later firing of
/// the same generator), or `OpModeDistribution` is a *provided* slow-tier table
/// (a snapshot's pre-computed distribution) that no generator has augmented in
/// scratch yet. The engine's `promote_scratch` copies each generator's scratch
/// output into the slow tier after every firing but leaves scratch intact, so
/// "scratch holds no `OpModeDistribution`" is what distinguishes a provided
/// table from one an earlier generator in this same chunk already wrote.
pub(crate) fn op_mode_distribution_already_built(ctx: &CalculatorContext, marker: &str) -> bool {
    ctx.scratch().store.contains(marker)
        || (ctx.scratch().store.get(OMD_TABLE).is_none()
            && ctx.tables().get(OMD_TABLE).is_some_and(|df| df.height() > 0))
}

/// Merge one OMD producer's rows into the single shared `OpModeDistribution`
/// scratch table instead of replacing it.
///
/// Canonical MOVES keeps `OpModeDistribution` as ONE process-keyed table that
/// every OMD generator writes by `INSERT`. In the Rust port the OMD generators
/// are co-chunked (every consumer reads the bare `OpModeDistribution` name) and
/// share one per-chunk scratch table, so a plain replace lets the last-firing
/// generator clobber the other processes' rows. This appends each producer's
/// (process-disjoint) rows: if the table already holds rows for any of this
/// producer's `polProcessID`s — a provided snapshot, or an earlier firing of the
/// same generator — it is left untouched; otherwise the normalized rows are
/// concatenated. `marker` is a per-generator scratch key set on completion so a
/// later firing returns early via [`op_mode_distribution_already_built`].
pub(crate) fn merge_op_mode_distribution<T: TableRow>(
    ctx: &mut CalculatorContext,
    marker: &str,
    rows: Vec<T>,
) -> Result<CalculatorOutput, Error> {
    let incoming = normalize_omd(rows.into_dataframe().map_err(polars_err)?).map_err(polars_err)?;
    let my_pps = omd_pol_procs(&incoming).map_err(polars_err)?;
    // A prior generator's scratch output overrides a provided slow-tier table.
    let existing = ctx
        .scratch()
        .store
        .get(OMD_TABLE)
        .or_else(|| ctx.tables().get(OMD_TABLE));
    let merged = match existing {
        // No table yet: create it (even when empty — downstream readers
        // `iter_typed` the table and expect it to be present).
        None => Some(incoming),
        Some(existing) => {
            let covered = existing
                .column("polProcessID")
                .ok()
                .and_then(|c| {
                    c.i32()
                        .ok()
                        .map(|c| c.into_iter().flatten().any(|v| my_pps.contains(&v)))
                })
                .unwrap_or(false);
            if covered || incoming.height() == 0 {
                // Already present for these processes (a provided snapshot or an
                // earlier firing), or nothing to add — leave the table as is.
                None
            } else {
                Some(
                    normalize_omd((*existing).clone())
                        .map_err(polars_err)?
                        .vstack(&incoming)
                        .map_err(polars_err)?,
                )
            }
        }
    };
    if let Some(df) = merged {
        ctx.scratch_mut().insert(OMD_TABLE, df);
    }
    // Mark this generator done even when it added nothing, so a repeat firing in
    // the same chunk early-returns before rebuilding.
    ctx.scratch_mut().insert(marker, DataFrame::empty());
    Ok(CalculatorOutput::empty())
}

#[cfg(test)]
mod omd_merge_tests {
    use super::*;
    use moves_framework::InMemoryStore;
    use polars::prelude::{DataType, NamedFrom, Schema, Series};

    /// Minimal `OpModeDistribution`-shaped row (no `roadTypeID`, like Start/Evap)
    /// for exercising the cross-process merge directly.
    struct OmdTestRow {
        pol_process_id: i32,
        op_mode_id: i32,
    }

    impl TableRow for OmdTestRow {
        fn table_name() -> &'static str {
            "OpModeDistribution"
        }
        fn polars_schema() -> Schema {
            Schema::from_iter([
                ("sourceTypeID".into(), DataType::Int32),
                ("linkID".into(), DataType::Int32),
                ("hourDayID".into(), DataType::Int32),
                ("polProcessID".into(), DataType::Int32),
                ("opModeID".into(), DataType::Int32),
                ("opModeFraction".into(), DataType::Float64),
            ])
        }
        fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
            let n = rows.len();
            DataFrame::new(
                n,
                vec![
                    Series::new("sourceTypeID".into(), vec![21i32; n]).into(),
                    Series::new("linkID".into(), vec![1i32; n]).into(),
                    Series::new("hourDayID".into(), vec![1i32; n]).into(),
                    Series::new(
                        "polProcessID".into(),
                        rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>(),
                    )
                    .into(),
                    Series::new(
                        "opModeID".into(),
                        rows.iter().map(|r| r.op_mode_id).collect::<Vec<i32>>(),
                    )
                    .into(),
                    Series::new("opModeFraction".into(), vec![1.0f64; n]).into(),
                ],
            )
        }
        fn from_dataframe(_df: &DataFrame) -> Result<Vec<Self>, Error> {
            Ok(Vec::new())
        }
    }

    fn pol_procs(ctx: &CalculatorContext) -> HashSet<i32> {
        let df = ctx
            .scratch()
            .store
            .get(OMD_TABLE)
            .expect("OpModeDistribution present");
        omd_pol_procs(&df).unwrap()
    }

    #[test]
    fn merge_accumulates_disjoint_processes_and_is_idempotent() {
        let mut ctx = CalculatorContext::new();

        // Producer A (process 101) then producer B (process 201): both survive.
        merge_op_mode_distribution(
            &mut ctx,
            "__m_a",
            vec![OmdTestRow { pol_process_id: 101, op_mode_id: 0 }],
        )
        .unwrap();
        merge_op_mode_distribution(
            &mut ctx,
            "__m_b",
            vec![
                OmdTestRow { pol_process_id: 201, op_mode_id: 0 },
                OmdTestRow { pol_process_id: 201, op_mode_id: 1 },
            ],
        )
        .unwrap();

        assert_eq!(pol_procs(&ctx), HashSet::from([101, 201]));
        let height = ctx.scratch().store.get(OMD_TABLE).unwrap().height();
        assert_eq!(height, 3, "1 row from A + 2 from B");
        // roadTypeID synthesized for the schemaless rows.
        assert!(ctx
            .scratch()
            .store
            .get(OMD_TABLE)
            .unwrap()
            .column("roadTypeID")
            .is_ok());

        // Re-running a producer whose processes are already present adds nothing.
        merge_op_mode_distribution(
            &mut ctx,
            "__m_a_again",
            vec![OmdTestRow { pol_process_id: 101, op_mode_id: 9 }],
        )
        .unwrap();
        assert_eq!(
            ctx.scratch().store.get(OMD_TABLE).unwrap().height(),
            3,
            "process 101 already covered — no duplicate rows"
        );
    }

    #[test]
    fn already_built_detects_marker_and_provided_table() {
        // Marker set → already built.
        let mut ctx = CalculatorContext::new();
        ctx.scratch_mut().insert("__m", DataFrame::empty());
        assert!(op_mode_distribution_already_built(&ctx, "__m"));
        assert!(!op_mode_distribution_already_built(&ctx, "__other"));

        // Provided slow-tier table (no scratch OMD yet) → already built.
        let mut store = InMemoryStore::new();
        store.insert(
            "OpModeDistribution",
            OmdTestRow::into_dataframe(vec![OmdTestRow { pol_process_id: 101, op_mode_id: 0 }])
                .unwrap(),
        );
        let provided = CalculatorContext::with_tables(store);
        assert!(op_mode_distribution_already_built(&provided, "__unset"));
    }
}

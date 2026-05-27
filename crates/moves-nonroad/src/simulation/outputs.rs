//! Structured result of [`run_simulation`](super::run_simulation) —
//! the in-memory replacement for NONROAD's text output and the
//! MariaDB ingestion step.
//!
//! In the Java↔Fortran bridge this task replaces, `nonroad.exe` wrote
//! a fixed-width `.OUT` file and the worker-side
//! `NonroadOutputDataLoader` / `NonroadPostProcessor` parsed it back
//! and loaded it into MariaDB. The Rust port instead returns a
//! [`NonroadOutputs`] value directly: the orchestrator merges it into
//! the unified Phase 4 Parquet output (`moves-data`'s `output_schema`,
//! Task 89) with no intermediate text format.
//!
//! # The output row shape
//!
//! NONROAD's geography routines emit two record granularities — a
//! per-`(FIPS, SCC, HP)` total (`wrtdat`) and a per-`(…, model-year,
//! tech)` breakdown (`wrtbmy`) — across two channels (exhaust and
//! evaporative). [`SimEmissionRow`] is the single flat shape all of
//! them collapse onto: a per-model-year breakdown row sets
//! [`model_year`](SimEmissionRow::model_year) and
//! [`tech_type`](SimEmissionRow::tech_type); a per-record total leaves
//! them `None`. One flat row type — rather than four output structs —
//! is what makes the downstream map onto the unified Parquet schema a
//! straight field copy.

use super::executor::GeographyExecution;

/// Which emission channel a [`SimEmissionRow`] belongs to — the
/// Fortran `iexev` argument (`1` exhaust, `2` evaporative).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmissionChannel {
    /// Exhaust emissions — `clcems.f`. Fortran `iexev = 1`.
    Exhaust,
    /// Evaporative emissions — `clcevems.f`. Fortran `iexev = 2`.
    Evaporative,
}

impl EmissionChannel {
    /// The Fortran `iexev` integer code (`1` exhaust, `2` evap).
    pub fn iexev(self) -> u8 {
        match self {
            EmissionChannel::Exhaust => 1,
            EmissionChannel::Evaporative => 2,
        }
    }
}

/// One emission output row, flattened from a geography routine's
/// `wrtdat` / `wrtbmy` record.
///
/// The per-pollutant [`emissions`](Self::emissions) vector is in tons,
/// matching the Fortran post-`CVTTON` scaling; its slots are indexed
/// by pollutant in the canonical NONROAD pollutant order, length
/// [`MXPOL`](crate::common::consts::MXPOL).
#[derive(Debug, Clone, PartialEq)]
pub struct SimEmissionRow {
    /// 5-character FIPS code — state or county for the geography
    /// level, `"00000"` for a US-total row.
    pub fips: String,
    /// 5-character subcounty marker; blank (`"     "`) at the county,
    /// state, and national levels.
    pub subcounty: String,
    /// 10-character SCC code.
    pub scc: String,
    /// HP-level representative for the row — Fortran `hplev`.
    pub hp_level: f32,
    /// Model year for a by-model-year breakdown row; `None` for a
    /// per-record total row (`wrtdat`-shaped).
    pub model_year: Option<i32>,
    /// Technology-type identifier for a by-model-year breakdown row;
    /// `None` for a per-record total row.
    pub tech_type: Option<String>,
    /// Exhaust or evaporative — see [`EmissionChannel`].
    pub channel: EmissionChannel,
    /// Equipment population for the row.
    pub population: f32,
    /// Activity (hours or gallons, per the activity unit) for the row.
    pub activity: f32,
    /// Fuel consumption for the row.
    pub fuel_consumption: f32,
    /// Per-pollutant emissions in tons. Length
    /// [`MXPOL`](crate::common::consts::MXPOL).
    pub emissions: Vec<f32>,
}

/// Run-level tallies collected while the driver loop executes.
///
/// These mirror the bookkeeping counters `nonroad.f` keeps (`nnatrc`,
/// the per-county `nctyrc`, …) and give the orchestrator a cheap
/// at-a-glance summary without re-walking [`NonroadOutputs::rows`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RunCounters {
    /// SCC groups whose record loop the driver planned (`getpop`
    /// returned them and they cleared the record-1 pre-check).
    pub scc_groups_planned: usize,
    /// SCC groups rejected wholesale by `nonroad.f`'s record-1 region
    /// pre-check (`SccGroupPlan::group_skipped`).
    pub scc_groups_skipped: usize,
    /// Population records the inner loop visited across all planned
    /// groups (one per [`DriverStep`](crate::driver::DriverStep)).
    pub records_visited: usize,
    /// Records skipped by the per-record region-selection filter
    /// (`StepOutcome::NotSelected`).
    pub records_not_selected: usize,
    /// Records whose region shape / run level matched no dispatch
    /// branch (`StepOutcome::Dispatched` with an empty list).
    pub records_no_dispatch: usize,
    /// Geography-routine dispatch calls made — one per
    /// `(record, Dispatch)` pair. A subcounty record can dispatch
    /// twice, so this can exceed [`records_visited`](Self::records_visited).
    pub dispatch_calls: usize,
    /// Dispatch calls whose geography routine returned an `ISKIP`
    /// (the executor reported [`GeographyExecution::skipped`]).
    pub geography_skips: usize,
}

/// The structured result of one [`run_simulation`](super::run_simulation).
///
/// The orchestrator consumes this directly: [`rows`](Self::rows) feed
/// the unified Parquet writer, [`counters`](Self::counters) and
/// [`completion_message`](Self::completion_message) feed the run log,
/// and [`warnings`](Self::warnings) surface non-fatal diagnostics.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NonroadOutputs {
    /// Every emission row the geography routines produced, in the
    /// order the driver loop dispatched them.
    pub rows: Vec<SimEmissionRow>,
    /// Non-fatal warnings, in production order. Mirrors the Fortran
    /// `chkwrn` warning channel; the count drives the completion
    /// banner.
    pub warnings: Vec<String>,
    /// Run-completion banner — [`completion_message`](crate::driver::completion_message)
    /// applied to the warning count. Empty until the run finishes.
    pub completion_message: String,
    /// Run-level tallies — see [`RunCounters`].
    pub counters: RunCounters,
    /// National-level records processed — Fortran `nnatrc`. Summed
    /// from the geography executions; meaningful at the national /
    /// US-total levels and `0` otherwise.
    pub national_record_count: i32,
}

/// Column-oriented store trait — Phase 4 unified-schema stand-in for
/// `moves_framework::DataFrameStore` (test-only; `moves-nonroad` carries
/// no polars / arrow dependency). The orchestrator crate owns the real
/// write path; this trait exists so the field-copy logic can be exercised
/// without adding a cross-crate dependency.
///
/// One call per row per column; the concrete mock appends to per-column
/// `Vec`s so the test can assert positional equality.
#[cfg(test)]
pub trait TestStore {
    /// Append a string value to the named column.
    fn push_str_col(&mut self, col: &str, val: String);
    /// Append an `f32` value to the named column.
    fn push_f32_col(&mut self, col: &str, val: f32);
    /// Append an optional `i32` value to the named column.
    fn push_opt_i32_col(&mut self, col: &str, val: Option<i32>);
    /// Append an optional string value to the named column.
    fn push_opt_str_col(&mut self, col: &str, val: Option<String>);
}

/// Field-copy from a [`SimEmissionRow`] onto the Phase 4 unified-schema
/// columns of a test store.
///
/// Column names match the Phase 4 unified Parquet schema (`moves-data`
/// Task 89): `fips`, `subcounty`, `scc`, `hp_level`, `model_year`,
/// `tech_type`, `channel`, `population`, `activity`, `fuel_consumption`,
/// and `emissions_N` for each pollutant index `N` in `0..MXPOL`.
///
/// The production write path lives in the orchestrator crate and uses the
/// real `DataFrameStore` from `moves-framework`; this method is the
/// reference specification of the field → column mapping.
#[cfg(test)]
impl SimEmissionRow {
    pub fn write_to_store_columns(&self, store: &mut dyn TestStore) {
        store.push_str_col("fips", self.fips.clone());
        store.push_str_col("subcounty", self.subcounty.clone());
        store.push_str_col("scc", self.scc.clone());
        store.push_f32_col("hp_level", self.hp_level);
        store.push_opt_i32_col("model_year", self.model_year);
        store.push_opt_str_col("tech_type", self.tech_type.clone());
        store.push_str_col(
            "channel",
            match self.channel {
                EmissionChannel::Exhaust => "exhaust".to_string(),
                EmissionChannel::Evaporative => "evaporative".to_string(),
            },
        );
        store.push_f32_col("population", self.population);
        store.push_f32_col("activity", self.activity);
        store.push_f32_col("fuel_consumption", self.fuel_consumption);
        for (i, &e) in self.emissions.iter().enumerate() {
            store.push_f32_col(&format!("emissions_{i}"), e);
        }
    }
}

impl NonroadOutputs {
    /// Fold one geography-routine execution into the run output:
    /// append its rows and warnings and add its national-record count.
    ///
    /// The `dispatch_calls` / `geography_skips` counters are *not*
    /// touched here — [`run_simulation`](super::run_simulation) owns
    /// them because it alone knows a call was made.
    pub fn absorb(&mut self, exec: GeographyExecution) {
        self.rows.extend(exec.rows);
        self.warnings.extend(exec.warnings);
        self.national_record_count += exec.national_record_count;
    }

    /// Total emission rows produced — a shorthand for `rows.len()`.
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::consts::MXPOL;
    use std::collections::HashMap;

    /// Concrete [`TestStore`] for unit tests — appends values to
    /// per-column `Vec`s keyed by column name.
    #[derive(Default)]
    struct MockStore {
        str_cols: HashMap<String, Vec<String>>,
        f32_cols: HashMap<String, Vec<f32>>,
        opt_i32_cols: HashMap<String, Vec<Option<i32>>>,
        opt_str_cols: HashMap<String, Vec<Option<String>>>,
    }

    impl TestStore for MockStore {
        fn push_str_col(&mut self, col: &str, val: String) {
            self.str_cols.entry(col.to_string()).or_default().push(val);
        }
        fn push_f32_col(&mut self, col: &str, val: f32) {
            self.f32_cols.entry(col.to_string()).or_default().push(val);
        }
        fn push_opt_i32_col(&mut self, col: &str, val: Option<i32>) {
            self.opt_i32_cols
                .entry(col.to_string())
                .or_default()
                .push(val);
        }
        fn push_opt_str_col(&mut self, col: &str, val: Option<String>) {
            self.opt_str_cols
                .entry(col.to_string())
                .or_default()
                .push(val);
        }
    }

    fn row(fips: &str, channel: EmissionChannel) -> SimEmissionRow {
        SimEmissionRow {
            fips: fips.to_string(),
            subcounty: "     ".to_string(),
            scc: "2270001010".to_string(),
            hp_level: 50.0,
            model_year: None,
            tech_type: None,
            channel,
            population: 100.0,
            activity: 200.0,
            fuel_consumption: 30.0,
            emissions: vec![0.0; MXPOL],
        }
    }

    #[test]
    fn iexev_codes_match_fortran() {
        assert_eq!(EmissionChannel::Exhaust.iexev(), 1);
        assert_eq!(EmissionChannel::Evaporative.iexev(), 2);
    }

    #[test]
    fn absorb_appends_rows_and_warnings_and_sums_counts() {
        let mut out = NonroadOutputs::default();
        out.absorb(GeographyExecution {
            rows: vec![row("06037", EmissionChannel::Exhaust)],
            warnings: vec!["first warning".to_string()],
            skipped: false,
            national_record_count: 2,
        });
        out.absorb(GeographyExecution {
            rows: vec![
                row("06038", EmissionChannel::Exhaust),
                row("06038", EmissionChannel::Evaporative),
            ],
            warnings: vec!["second warning".to_string()],
            skipped: false,
            national_record_count: 3,
        });
        assert_eq!(out.row_count(), 3);
        assert_eq!(out.warnings.len(), 2);
        assert_eq!(out.national_record_count, 5);
        assert_eq!(out.rows[0].fips, "06037");
        assert_eq!(out.rows[2].channel, EmissionChannel::Evaporative);
    }

    #[test]
    fn absorb_of_an_empty_execution_is_a_no_op() {
        let mut out = NonroadOutputs::default();
        out.absorb(GeographyExecution::default());
        assert_eq!(out.row_count(), 0);
        assert!(out.warnings.is_empty());
        assert_eq!(out.national_record_count, 0);
    }

    #[test]
    fn default_output_is_empty() {
        let out = NonroadOutputs::default();
        assert_eq!(out.row_count(), 0);
        assert!(out.warnings.is_empty());
        assert!(out.completion_message.is_empty());
        assert_eq!(out.counters, RunCounters::default());
        assert_eq!(out.national_record_count, 0);
    }

    #[test]
    fn write_to_store_columns_maps_every_field() {
        let mut emissions = vec![0.0_f32; MXPOL];
        emissions[0] = 1.5;
        emissions[5] = 2.7;
        let sample = SimEmissionRow {
            fips: "06037".to_string(),
            subcounty: "12345".to_string(),
            scc: "2270001010".to_string(),
            hp_level: 75.0,
            model_year: Some(2019),
            tech_type: Some("T1".to_string()),
            channel: EmissionChannel::Evaporative,
            population: 42.0,
            activity: 99.5,
            fuel_consumption: 11.1,
            emissions,
        };

        let mut store = MockStore::default();
        sample.write_to_store_columns(&mut store);

        assert_eq!(store.str_cols["fips"][0], "06037");
        assert_eq!(store.str_cols["subcounty"][0], "12345");
        assert_eq!(store.str_cols["scc"][0], "2270001010");
        assert!((store.f32_cols["hp_level"][0] - 75.0).abs() < f32::EPSILON);
        assert_eq!(store.opt_i32_cols["model_year"][0], Some(2019));
        assert_eq!(store.opt_str_cols["tech_type"][0], Some("T1".to_string()));
        assert_eq!(store.str_cols["channel"][0], "evaporative");
        assert!((store.f32_cols["population"][0] - 42.0).abs() < f32::EPSILON);
        assert!((store.f32_cols["activity"][0] - 99.5).abs() < f32::EPSILON);
        assert!((store.f32_cols["fuel_consumption"][0] - 11.1).abs() < f32::EPSILON);
        assert!((store.f32_cols["emissions_0"][0] - 1.5).abs() < f32::EPSILON);
        assert!((store.f32_cols["emissions_5"][0] - 2.7).abs() < f32::EPSILON);
        for i in 0..MXPOL {
            assert!(
                store.f32_cols.contains_key(&format!("emissions_{i}")),
                "missing column emissions_{i}"
            );
        }
    }

    #[test]
    fn write_to_store_columns_handles_none_optional_fields() {
        let sample = SimEmissionRow {
            fips: "00000".to_string(),
            subcounty: "     ".to_string(),
            scc: "2270001010".to_string(),
            hp_level: 50.0,
            model_year: None,
            tech_type: None,
            channel: EmissionChannel::Exhaust,
            population: 100.0,
            activity: 200.0,
            fuel_consumption: 30.0,
            emissions: vec![0.0; MXPOL],
        };

        let mut store = MockStore::default();
        sample.write_to_store_columns(&mut store);

        assert_eq!(store.opt_i32_cols["model_year"][0], None);
        assert_eq!(store.opt_str_cols["tech_type"][0], None);
        assert_eq!(store.str_cols["channel"][0], "exhaust");
    }
}

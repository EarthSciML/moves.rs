//! Data model for the `onRoadRetrofit` input table.
//!
//! Each record describes one retrofit program: for vehicles of a specific
//! source type whose model year falls in a given range, the program specifies
//! what fraction of the fleet has been retrofitted by a certain year and how
//! effective the retrofit is at reducing emissions for a given
//! pollutant/process pair.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Source-use-type identifier (`onRoadRetrofit.sourceTypeID`, `smallint`).
pub type SourceTypeId = i32;
/// Model-year identifier (`smallint unsigned`).
pub type ModelYearId = i32;
/// Calendar year a retrofit program applied through (`smallint unsigned`).
pub type RetrofitYearId = i32;
/// Pollutant identifier (`onRoadRetrofit.pollutantID`, `smallint unsigned`).
pub type PollutantId = u16;
/// Emission-process identifier (`onRoadRetrofit.processID`, `smallint unsigned`).
pub type ProcessId = u16;

/// One row of the `onRoadRetrofit` table.
///
/// Ports the canonical `onRoadRetrofit` MOVES input table. The Java source
/// stores `modelYearGroupID` as an integer-encoded range; the Rust port
/// uses explicit `start_model_year` / `end_model_year` fields for clarity.
///
/// The effective emission adjustment factor for a (sourceType, modelYear,
/// pollutant, process) combination in analysis year Y is:
///
/// ```text
/// factor = ∏ over matching records (r where retrofitYearID ≤ Y) of
/// (1 - r.cumulative_retrofit_fraction * r.retrofit_effectiveness)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct RetrofitRecord {
    /// Vehicle source-use type.
    #[serde(rename = "sourceTypeID")]
    pub source_type_id: SourceTypeId,
    /// First model year in the range this program applies to (inclusive).
    #[serde(rename = "startModelYear")]
    pub start_model_year: ModelYearId,
    /// Last model year in the range this program applies to (inclusive).
    #[serde(rename = "endModelYear")]
    pub end_model_year: ModelYearId,
    /// Calendar year through which this cumulative fraction applies.
    #[serde(rename = "retrofitYearID")]
    pub retrofit_year_id: RetrofitYearId,
    /// Pollutant this program reduces.
    #[serde(rename = "pollutantID")]
    pub pollutant_id: PollutantId,
    /// Emission process this program reduces.
    #[serde(rename = "processID")]
    pub process_id: ProcessId,
    /// Cumulative fraction of matching fleet that has been retrofitted by
    /// `retrofit_year_id`. Range: `[0.0, 1.0]`.
    #[serde(rename = "cumulativeRetrofitFraction")]
    pub cumulative_retrofit_fraction: f64,
    /// Emission reduction effectiveness for retrofitted vehicles.
    /// `0.0` = no reduction; `1.0` = complete elimination. Range: `[0.0, 1.0]`.
    #[serde(rename = "retrofitEffectiveness")]
    pub retrofit_effectiveness: f64,
}

impl RetrofitRecord {
    /// Construct a record from individual fields.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source_type_id: SourceTypeId,
        start_model_year: ModelYearId,
        end_model_year: ModelYearId,
        retrofit_year_id: RetrofitYearId,
        pollutant_id: PollutantId,
        process_id: ProcessId,
        cumulative_retrofit_fraction: f64,
        retrofit_effectiveness: f64,
    ) -> Self {
        Self {
            source_type_id,
            start_model_year,
            end_model_year,
            retrofit_year_id,
            pollutant_id,
            process_id,
            cumulative_retrofit_fraction,
            retrofit_effectiveness,
        }
    }

    /// `true` if `model_year` falls within `[start_model_year, end_model_year]`.
    pub fn covers_model_year(&self, model_year: ModelYearId) -> bool {
        model_year >= self.start_model_year && model_year <= self.end_model_year
    }

    /// Emission-reduction factor contributed by this single record:
    /// `1.0 - cumulative_retrofit_fraction * retrofit_effectiveness`.
    ///
    /// Values are clamped to `[0.0, 1.0]` as a safety net for
    /// floating-point edge cases.
    pub fn emission_factor(&self) -> f64 {
        (1.0 - self.cumulative_retrofit_fraction * self.retrofit_effectiveness).clamp(0.0, 1.0)
    }
}

/// Natural sort key for a [`RetrofitRecord`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RetrofitKey {
    pub source_type_id: SourceTypeId,
    pub start_model_year: ModelYearId,
    pub end_model_year: ModelYearId,
    pub retrofit_year_id: RetrofitYearId,
    pub pollutant_id: PollutantId,
    pub process_id: ProcessId,
}

impl From<&RetrofitRecord> for RetrofitKey {
    fn from(r: &RetrofitRecord) -> Self {
        Self {
            source_type_id: r.source_type_id,
            start_model_year: r.start_model_year,
            end_model_year: r.end_model_year,
            retrofit_year_id: r.retrofit_year_id,
            pollutant_id: r.pollutant_id,
            process_id: r.process_id,
        }
    }
}

/// In-memory `onRoadRetrofit` table.
///
/// Backed by a `BTreeMap` keyed on [`RetrofitKey`] so iteration order is
/// deterministic. Multiple programs may share the same
/// `(sourceType, modelYearRange, retrofitYear, pollutant, process)` key;
/// the last-write-wins semantics match the Java source's `INSERT IGNORE`
/// against a unique constraint — callers may validate uniqueness upstream.
#[derive(Debug, Default, Clone)]
pub struct RetrofitTable {
    records: BTreeMap<RetrofitKey, RetrofitRecord>,
}

impl RetrofitTable {
    /// Create an empty table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of rows in the table.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// `true` if the table has no rows.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Insert a record. Duplicate keys replace the previous entry.
    pub fn insert(&mut self, record: RetrofitRecord) {
        self.records.insert(RetrofitKey::from(&record), record);
    }

    /// Iterate all records in key order.
    pub fn iter(&self) -> impl Iterator<Item = &RetrofitRecord> {
        self.records.values()
    }

    /// All records whose `(sourceType, modelYear, pollutant, process)`
    /// match and whose `retrofit_year_id ≤ analysis_year`.
    ///
    /// These are the programs active for the given vehicle / year / pollutant
    /// combination during an analysis run.
    pub fn active_records(
        &self,
        source_type_id: SourceTypeId,
        model_year_id: ModelYearId,
        pollutant_id: PollutantId,
        process_id: ProcessId,
        analysis_year: i32,
    ) -> impl Iterator<Item = &RetrofitRecord> {
        self.records.values().filter(move |r| {
            r.source_type_id == source_type_id
                && r.covers_model_year(model_year_id)
                && r.pollutant_id == pollutant_id
                && r.process_id == process_id
                && r.retrofit_year_id <= analysis_year
        })
    }

    /// Compute the combined emission adjustment factor for a given
    /// `(sourceType, modelYear, pollutant, process)` combination in
    /// `analysis_year`.
    ///
    /// Returns the product of [`RetrofitRecord::emission_factor`] over all
    /// active records, or `1.0` if no programs apply (no adjustment). A
    /// factor of `0.5` means 50% of baseline emissions remain.
    pub fn combined_factor(
        &self,
        source_type_id: SourceTypeId,
        model_year_id: ModelYearId,
        pollutant_id: PollutantId,
        process_id: ProcessId,
        analysis_year: i32,
    ) -> f64 {
        self.active_records(
            source_type_id,
            model_year_id,
            pollutant_id,
            process_id,
            analysis_year,
        )
        .map(|r| r.emission_factor())
        .fold(1.0, |acc, f| acc * f)
    }
}

impl FromIterator<RetrofitRecord> for RetrofitTable {
    fn from_iter<I: IntoIterator<Item = RetrofitRecord>>(iter: I) -> Self {
        let mut t = Self::new();
        for r in iter {
            t.insert(r);
        }
        t
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(clippy::too_many_arguments)]
    fn make_record(
        source_type: i32,
        start_my: i32,
        end_my: i32,
        retrofit_year: i32,
        pollutant: u16,
        process: u16,
        fraction: f64,
        effectiveness: f64,
    ) -> RetrofitRecord {
        RetrofitRecord::new(
            source_type,
            start_my,
            end_my,
            retrofit_year,
            pollutant,
            process,
            fraction,
            effectiveness,
        )
    }

    #[test]
    fn covers_model_year_boundary_inclusive() {
        let r = make_record(11, 2005, 2015, 2020, 98, 1, 0.5, 0.8);
        assert!(r.covers_model_year(2005));
        assert!(r.covers_model_year(2015));
        assert!(r.covers_model_year(2010));
        assert!(!r.covers_model_year(2004));
        assert!(!r.covers_model_year(2016));
    }

    #[test]
    fn emission_factor_single_record() {
        let r = make_record(11, 2005, 2015, 2020, 98, 1, 0.5, 0.8);
        let expected = 1.0 - 0.5 * 0.8;
        assert!((r.emission_factor() - expected).abs() < 1e-12);
    }

    #[test]
    fn emission_factor_full_retrofit() {
        let r = make_record(11, 2005, 2015, 2020, 98, 1, 1.0, 1.0);
        assert!((r.emission_factor() - 0.0).abs() < 1e-12);
    }

    #[test]
    fn emission_factor_no_retrofit() {
        let r = make_record(11, 2005, 2015, 2020, 98, 1, 0.0, 0.8);
        assert!((r.emission_factor() - 1.0).abs() < 1e-12);
    }

    #[test]
    fn combined_factor_no_programs_is_one() {
        let t = RetrofitTable::new();
        let f = t.combined_factor(11, 2010, 98, 1, 2025);
        assert!((f - 1.0).abs() < 1e-12);
    }

    #[test]
    fn combined_factor_single_program() {
        let t: RetrofitTable = [make_record(11, 2005, 2015, 2020, 98, 1, 0.5, 0.8)]
            .into_iter()
            .collect();
        let f = t.combined_factor(11, 2010, 98, 1, 2025);
        let expected = 1.0 - 0.5 * 0.8;
        assert!((f - expected).abs() < 1e-12);
    }

    #[test]
    fn combined_factor_two_independent_programs_multiply() {
        let t: RetrofitTable = [
            make_record(11, 2000, 2015, 2010, 98, 1, 0.3, 0.6),
            make_record(11, 2000, 2015, 2015, 98, 1, 0.2, 0.5),
        ]
        .into_iter()
        .collect();
        let f = t.combined_factor(11, 2010, 98, 1, 2020);
        let expected = (1.0 - 0.3 * 0.6) * (1.0 - 0.2 * 0.5);
        assert!((f - expected).abs() < 1e-12);
    }

    #[test]
    fn combined_factor_skips_future_retrofit_year() {
        let t: RetrofitTable = [make_record(11, 2005, 2015, 2030, 98, 1, 0.5, 0.8)]
            .into_iter()
            .collect();
        let f = t.combined_factor(11, 2010, 98, 1, 2025);
        assert!((f - 1.0).abs() < 1e-12, "program not yet applied");
    }

    #[test]
    fn combined_factor_skips_wrong_model_year() {
        let t: RetrofitTable = [make_record(11, 2005, 2010, 2020, 98, 1, 0.5, 0.8)]
            .into_iter()
            .collect();
        let f = t.combined_factor(11, 2012, 98, 1, 2025);
        assert!((f - 1.0).abs() < 1e-12, "model year outside range");
    }

    #[test]
    fn combined_factor_skips_wrong_source_type() {
        let t: RetrofitTable = [make_record(11, 2005, 2015, 2020, 98, 1, 0.5, 0.8)]
            .into_iter()
            .collect();
        let f = t.combined_factor(21, 2010, 98, 1, 2025);
        assert!((f - 1.0).abs() < 1e-12);
    }

    #[test]
    fn iteration_is_deterministic() {
        let t: RetrofitTable = [
            make_record(21, 2005, 2015, 2020, 98, 1, 0.3, 0.5),
            make_record(11, 2005, 2015, 2020, 98, 1, 0.2, 0.6),
        ]
        .into_iter()
        .collect();
        let ids: Vec<_> = t.iter().map(|r| r.source_type_id).collect();
        assert_eq!(ids, vec![11, 21]);
    }

    #[test]
    fn duplicate_key_last_write_wins() {
        let mut t = RetrofitTable::new();
        t.insert(make_record(11, 2005, 2015, 2020, 98, 1, 0.3, 0.5));
        t.insert(make_record(11, 2005, 2015, 2020, 98, 1, 0.7, 0.9));
        assert_eq!(t.len(), 1);
        let r = t.iter().next().unwrap();
        assert!((r.cumulative_retrofit_fraction - 0.7).abs() < 1e-12);
    }
}

//! Subcounty-level processing — port of `prcsub.f` (Task 109).
//!
//! `prcsub.f` (829 lines) does the county-to-subregion allocation of
//! populations and applies emission + seasonality factors. Task 112
//! merged it with `prccty.f` into the shared, parameterised
//! [`process_geography`] routine; [`process_subcounty`] is now a thin
//! wrapper that selects [`ProcessLevel::Subcounty`]. The
//! subcounty-specific `fndasc` + subcounty-marker + `alosub`
//! allocation chain (`prcsub.f` :240–:266) — the only part of
//! `prcsub.f` that diverges from `prccty.f` — lives in that routine,
//! gated on the [`ProcessLevel`] selector.

use super::common::{GeographyCallbacks, PopulationRecord, ProcessOutcome, RunOptions};
use super::process::{process_geography, ProcessLevel};
use crate::Result;

/// Per-subcounty record index passed to [`process_subcounty`].
///
/// `prcsub.f` accepts a single `icurec` argument; the allocation
/// callback (`alosub`) uses it to look up the per-record population
/// data. The Rust port carries the index alongside the record so the
/// allocation method can resolve it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubcountyRecordIndex(pub usize);

/// Process one subcounty-level population record. Ports `prcsub.f`.
///
/// Thin wrapper over [`process_geography`] with
/// [`ProcessLevel::Subcounty`]. See [`process_geography`] for the
/// shared orchestration, the `ProcessOutcome` mapping, and the
/// failure modes; the subcounty-specific allocation step it runs is
/// described on [`ProcessLevel::Subcounty`].
///
/// # Arguments
///
/// - `record_index`: original record index `icurec`, threaded
///   through to `alosub` in the callback. The Fortran source uses it
///   as an index into the population COMMON arrays.
/// - `record`: per-record COMMON-block reads. For subcounty
///   processing, `region_code` is the 5-character FIPS prefix of the
///   matched `reglst` entry (the trailing 5 hold the subcounty
///   marker).
/// - `cached_growth`: the `growth` argument to `prcsub.f`. A sentinel
///   value `< 0` (Fortran's `-9`) means "not yet computed"; pass
///   `None` in that case and the callback's `allocate_subcounty`
///   produces a real value.
/// - `options`: run-level settings.
/// - `callbacks`: dependency surface (same trait as the county
///   processor).
pub fn process_subcounty<C: GeographyCallbacks + ?Sized>(
    record_index: SubcountyRecordIndex,
    record: &PopulationRecord<'_>,
    cached_growth: Option<f32>,
    options: &RunOptions,
    callbacks: &mut C,
) -> Result<ProcessOutcome> {
    process_geography(
        ProcessLevel::Subcounty {
            record_index: record_index.0,
            cached_growth,
        },
        record,
        options,
        callbacks,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::consts::MXHPC;
    use crate::emissions::exhaust::FuelKind;
    use crate::geography::common::{
        ActivityRecord, ActivityUnit, EmissionsIterationResult, EvapFactorsLookup,
        ExhaustFactorsLookup, ModelYearAgedistResult, NoopCallbacks, RefuelingData, RetrofitFilter,
        SumType, TechLookup,
    };
    use crate::Error;

    fn default_options() -> RunOptions {
        let mut hp_levels = [0.0_f32; MXHPC];
        let vs: [f32; MXHPC] = [
            3.0, 6.0, 11.0, 16.0, 25.0, 40.0, 50.0, 75.0, 100.0, 175.0, 300.0, 600.0, 750.0,
            1000.0, 1200.0, 1500.0, 1800.0, 2000.0,
        ];
        hp_levels.copy_from_slice(&vs);
        RunOptions {
            tech_year: 2020,
            episode_year: 2020,
            growth_year: 2020,
            fuel: FuelKind::Diesel,
            sum_type: SumType::Total,
            daily_mode: false,
            write_bmy_exhaust: false,
            write_bmy_evap: false,
            write_si: false,
            retrofit_enabled: false,
            spillage_enabled: false,
            growth_enabled: true,
            hp_levels,
        }
    }

    fn default_record() -> PopulationRecord<'static> {
        PopulationRecord {
            region_code: "17001",
            population: 100.0,
            hp_range: (50.0, 100.0),
            hp_avg: 75.0,
            use_hours: 1000.0,
            disc_code: "DEFAULT",
            base_pop_year: 2020,
            scc: "2270001000",
        }
    }

    #[test]
    fn process_subcounty_skips_when_fips_not_found() {
        let mut cb = NoopCallbacks;
        let options = default_options();
        let record = default_record();
        let outcome =
            process_subcounty(SubcountyRecordIndex(0), &record, None, &options, &mut cb).unwrap();
        assert!(outcome.is_skipped());
    }

    // ---- Custom callback that finds FIPS but not allocation; should be a fatal Config. ----
    struct FipsButNoAlloc;
    impl GeographyCallbacks for FipsButNoAlloc {
        fn find_fips(&self, _: &str) -> Option<usize> {
            Some(0)
        }
        fn tally_county_record(&mut self, _: usize) {}
        fn find_exhaust_tech(&self, _: &str, _: f32, _: i32) -> Option<TechLookup> {
            None
        }
        fn find_evap_tech(&self, _: &str, _: f32, _: i32) -> Option<TechLookup> {
            None
        }
        fn find_refueling(&self, _: &str, _: f32, _: &str) -> Option<RefuelingData> {
            None
        }
        fn find_growth_xref(&self, _: &str, _: &str, _: f32) -> Option<usize> {
            None
        }
        fn find_activity(&self, _: &str, _: &str, _: f32) -> Option<usize> {
            None
        }
        fn filter_retrofits(
            &mut self,
            _: RetrofitFilter,
            _: &str,
            _: f32,
            _: i32,
            _: &str,
        ) -> Result<()> {
            Ok(())
        }
        fn surviving_retrofits(&self) -> Vec<&crate::population::retrofit::RetrofitRecord> {
            Vec::new()
        }
        fn day_month_factors(
            &self,
            _: &str,
            _: &str,
        ) -> ([f32; crate::common::consts::MXDAYS], f32, f32, i32) {
            ([0.0; crate::common::consts::MXDAYS], 1.0, 1.0, 30)
        }
        fn emission_adjustments(
            &self,
            _: &str,
            _: &str,
            _: &[f32; crate::common::consts::MXDAYS],
        ) -> crate::emissions::exhaust::AdjustmentTable {
            crate::emissions::exhaust::AdjustmentTable::new(crate::common::consts::MXDAYS)
        }
        fn model_year_and_agedist(
            &mut self,
            _: usize,
            _: &PopulationRecord<'_>,
            _: &str,
            _: usize,
            _: i32,
            _: i32,
            _: f32,
        ) -> Result<ModelYearAgedistResult> {
            Err(Error::Config("n/a".into()))
        }
        fn compute_exhaust_factors(
            &mut self,
            _: &str,
            _: &[String],
            _: &[f32],
            _: i32,
            _: usize,
            _: usize,
        ) -> Result<ExhaustFactorsLookup> {
            Err(Error::Config("n/a".into()))
        }
        fn compute_evap_factors(
            &mut self,
            _: &str,
            _: &[String],
            _: &[f32],
            _: i32,
            _: usize,
            _: usize,
        ) -> Result<EvapFactorsLookup> {
            Err(Error::Config("n/a".into()))
        }
        fn compute_exhaust_iteration(
            &mut self,
            _: &PopulationRecord<'_>,
            _: &RunOptions,
            _: &ExhaustFactorsLookup,
            _: &crate::emissions::exhaust::AdjustmentTable,
            _: usize,
            _: usize,
            _: usize,
            _: f32,
            _: f32,
            _: f32,
            _: f32,
            _: f32,
            _: f32,
            _: f32,
            _: i32,
            _: usize,
        ) -> Result<EmissionsIterationResult> {
            Err(Error::Config("n/a".into()))
        }
        fn compute_evap_iteration(
            &mut self,
            _: &PopulationRecord<'_>,
            _: &RunOptions,
            _: &EvapFactorsLookup,
            _: &crate::emissions::exhaust::AdjustmentTable,
            _: &RefuelingData,
            _: usize,
            _: usize,
            _: usize,
            _: f32,
            _: f32,
            _: &str,
            _: f32,
            _: f32,
            _: f32,
            _: f32,
            _: f32,
            _: i32,
            _: f32,
        ) -> Result<EmissionsIterationResult> {
            Err(Error::Config("n/a".into()))
        }
        fn activity_record(&self, _: usize) -> ActivityRecord {
            ActivityRecord {
                starts: 0.0,
                activity_level: 0.0,
                activity_unit: ActivityUnit::HoursPerYear,
                load_factor: 0.0,
                age_code: String::new(),
            }
        }
        // Default `find_allocation` returns None — this is the
        // 7000-path trigger.
    }

    #[test]
    fn process_subcounty_missing_allocation_is_fatal() {
        let mut cb = FipsButNoAlloc;
        let options = default_options();
        let record = default_record();
        let err = process_subcounty(SubcountyRecordIndex(0), &record, None, &options, &mut cb)
            .unwrap_err();
        match err {
            Error::Config(m) => assert!(
                m.contains("Could not find any allocation coefficients"),
                "got: {m}"
            ),
            other => panic!("unexpected error: {other:?}"),
        }
    }
}

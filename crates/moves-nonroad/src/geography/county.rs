//! County-level processing — port of `prccty.f` (Task 109).
//!
//! `prccty.f` (790 lines) does the state-to-county allocation of
//! populations and applies emission + seasonality factors. Task 112
//! merged it with `prcsub.f` into the shared, parameterised
//! [`process_geography`] routine; [`process_county`] is now a thin
//! wrapper that selects [`ProcessLevel::County`].

use super::common::{GeographyCallbacks, PopulationRecord, ProcessOutcome, RunOptions};
use super::process::{process_geography, ProcessLevel};
use crate::Result;

/// Process one county-level population record. Ports `prccty.f`.
///
/// Thin wrapper over [`process_geography`] with
/// [`ProcessLevel::County`] — county processing reads the population
/// straight from the record, with no subcounty allocation step. See
/// [`process_geography`] for the shared orchestration, the
/// `ProcessOutcome` mapping, and the failure modes.
///
/// # Arguments
///
/// - `record`: per-record COMMON-block reads (`/popdat/` slot).
/// - `options`: run-level settings (`/optdat/`, `/eqpdat/`, `/io/`).
/// - `callbacks`: dependency surface that supplies the helpers
///   `prccty.f` calls (`fndtch`, `fndevtch`, `emfclc`, `evemfclc`,
///   `daymthf`, `emsadj`, `getgrw`, `grwfac`, `modyr`, `agedist`,
///   `clcems`, `clcevems`, `fndrtrft`, `fndrfm`, `fndchr`, …).
pub fn process_county<C: GeographyCallbacks + ?Sized>(
    record: &PopulationRecord<'_>,
    options: &RunOptions,
    callbacks: &mut C,
) -> Result<ProcessOutcome> {
    process_geography(ProcessLevel::County, record, options, callbacks)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::consts::{MXHPC, MXPOL};
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
    fn process_county_skips_when_fips_not_found() {
        let mut cb = NoopCallbacks;
        let options = default_options();
        let record = default_record();
        let outcome = process_county(&record, &options, &mut cb).unwrap();
        assert!(outcome.is_skipped(), "fips-not-found should yield ISKIP");
        let out = outcome.into_output();
        assert_eq!(out.fips, "17001");
        assert_eq!(out.emissions_day.len(), MXPOL);
        // No FIPS, so no emsams fold-in.
        assert!(out.emsams_fips_index.is_none());
        // No dat record emitted.
        assert!(out.dat_records.is_empty());
    }

    #[test]
    fn process_county_growth_disabled_is_fatal() {
        // Use a callback that DOES find the FIPS so we get past the
        // skip-early-out.
        struct OnlyFindFips;
        impl GeographyCallbacks for OnlyFindFips {
            fn find_fips(&self, _: &str) -> Option<usize> {
                Some(0)
            }
            fn tally_county_record(&mut self, _: usize) {}
            fn find_exhaust_tech(&self, _: &str, _: f32, _: i32) -> Option<TechLookup> {
                Some(TechLookup {
                    scc_tech_index: 0,
                    tech_names: vec!["T001".into()],
                    tech_fractions: vec![1.0],
                })
            }
            fn find_evap_tech(&self, _: &str, _: f32, _: i32) -> Option<TechLookup> {
                Some(TechLookup {
                    scc_tech_index: 0,
                    tech_names: vec!["E001".into()],
                    tech_fractions: vec![1.0],
                })
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
                Err(Error::Config("not used".into()))
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
                Err(Error::Config("not used".into()))
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
                Err(Error::Config("not used".into()))
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
                Err(Error::Config("not used".into()))
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
                Err(Error::Config("not used".into()))
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
        }
        let mut cb = OnlyFindFips;
        let mut options = default_options();
        options.growth_enabled = false;
        let record = default_record();
        let err = process_county(&record, &options, &mut cb).unwrap_err();
        match err {
            Error::Config(m) => assert!(m.contains("GROWTH FILES")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn process_county_zero_population_emits_zero_dat() {
        struct FipsOnly;
        impl GeographyCallbacks for FipsOnly {
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
        }
        let mut cb = FipsOnly;
        let options = default_options();
        let mut record = default_record();
        record.population = 0.0;
        let outcome = process_county(&record, &options, &mut cb).unwrap();
        match outcome {
            ProcessOutcome::Success(out) => {
                assert_eq!(out.dat_records.len(), 1);
                let dat = &out.dat_records[0];
                assert_eq!(dat.fips, "17001");
                assert_eq!(dat.population_total, 0.0);
                assert_eq!(dat.activity_total, 0.0);
                assert_eq!(dat.fuel_consumption, 0.0);
                // emsday is all zeros — none populated.
                assert!(dat.emissions.iter().all(|&v| v == 0.0));
            }
            other => panic!("expected Success, got {other:?}"),
        }
    }

    // -------------------------------------------------------------
    // Happy-path test: stub callback drives the full model-year loop
    // for a single-year, single-tech equipment record and asserts
    // that the per-(year, tech) iteration ran exactly once, with the
    // expected slot values folded into the running totals.
    // -------------------------------------------------------------

    struct HappyPathCallbacks {
        iter_count: std::cell::RefCell<u32>,
    }

    impl HappyPathCallbacks {
        fn new() -> Self {
            Self {
                iter_count: std::cell::RefCell::new(0),
            }
        }
    }

    impl GeographyCallbacks for HappyPathCallbacks {
        fn find_fips(&self, _: &str) -> Option<usize> {
            Some(3)
        }
        fn tally_county_record(&mut self, _: usize) {}
        fn find_exhaust_tech(&self, _: &str, _: f32, _: i32) -> Option<TechLookup> {
            Some(TechLookup {
                scc_tech_index: 0,
                tech_names: vec!["T001      ".into()],
                tech_fractions: vec![1.0],
            })
        }
        fn find_evap_tech(&self, _: &str, _: f32, _: i32) -> Option<TechLookup> {
            Some(TechLookup {
                scc_tech_index: 0,
                tech_names: vec!["EV9XX     ".into()],
                tech_fractions: vec![1.0],
            })
        }
        fn find_refueling(&self, _: &str, _: f32, _: &str) -> Option<RefuelingData> {
            None
        }
        fn find_growth_xref(&self, _: &str, _: &str, _: f32) -> Option<usize> {
            Some(7)
        }
        fn find_activity(&self, _: &str, _: &str, _: f32) -> Option<usize> {
            Some(2)
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
            pop: f32,
        ) -> Result<ModelYearAgedistResult> {
            // Single-year lifetime: nyrlif=1, all single-element vectors.
            Ok(ModelYearAgedistResult {
                yryrfrcscrp: vec![0.0],
                modfrc: vec![1.0],
                stradj: vec![0.5],
                actadj: vec![0.7],
                detage: vec![0.0],
                nyrlif: 1,
                population: pop,
            })
        }
        fn compute_exhaust_factors(
            &mut self,
            _: &str,
            tech_names: &[String],
            _: &[f32],
            _: i32,
            _: usize,
            _: usize,
        ) -> Result<ExhaustFactorsLookup> {
            // BSFC for the single year × single tech slot, with 0.4
            // so we can verify the fulbmy multiplication.
            let n = tech_names.len();
            Ok(ExhaustFactorsLookup {
                emission_factors: vec![
                    0.0;
                    crate::common::consts::MXAGYR
                        * crate::common::consts::MXPOL
                        * crate::common::consts::MXTECH
                ],
                bsfc: vec![0.4; n],
                unit_codes: vec![
                    crate::emissions::exhaust::EmissionUnitCode::GramsPerHour;
                    crate::common::consts::MXPOL * crate::common::consts::MXTECH
                ],
                adetcf: vec![0.0; crate::common::consts::MXPOL * crate::common::consts::MXTECH],
                bdetcf: vec![0.0; crate::common::consts::MXPOL * crate::common::consts::MXTECH],
                detcap: vec![0.0; crate::common::consts::MXPOL * crate::common::consts::MXTECH],
            })
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
            Ok(EvapFactorsLookup::default())
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
            *self.iter_count.borrow_mut() += 1;
            // Emit a known per-pollutant payload: 5.0 in pollutant
            // slot 0 (THC). The orchestrator adds this into
            // output.emissions_day.
            let mut emsday_delta = vec![0.0_f32; crate::common::consts::MXPOL];
            emsday_delta[0] = 5.0;
            let mut emsbmy = vec![0.0_f32; crate::common::consts::MXPOL];
            emsbmy[0] = 5.0;
            Ok(EmissionsIterationResult {
                emsday_delta,
                emsbmy,
                fulbmy: 1.0,
            })
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
            // Evap returns zero so we only see exhaust in the totals.
            Ok(EmissionsIterationResult {
                emsday_delta: vec![0.0; crate::common::consts::MXPOL],
                emsbmy: vec![0.0; crate::common::consts::MXPOL],
                fulbmy: 0.0,
            })
        }
        fn activity_record(&self, _: usize) -> ActivityRecord {
            ActivityRecord {
                starts: 1.0,
                activity_level: 1.0,
                activity_unit: ActivityUnit::HoursPerYear,
                load_factor: 0.5,
                age_code: "DEFAULT".into(),
            }
        }
    }

    #[test]
    fn process_county_runs_model_year_loop_once_for_single_year_record() {
        let mut cb = HappyPathCallbacks::new();
        let options = default_options();
        let record = default_record();
        let outcome = process_county(&record, &options, &mut cb).unwrap();
        let out = outcome.into_output();
        // FIPS lookup succeeded and we got the index back for the
        // emsams fold-in.
        assert_eq!(out.emsams_fips_index, Some(3));
        assert_eq!(out.fips, "17001");
        assert_eq!(out.hp_level, 100.0); // (50+100)/2 = 75 → first boundary > 75 = 100.
                                         // One exhaust iteration ran (single year × single tech).
        assert_eq!(*cb.iter_count.borrow(), 1);
        // emsday[0] = 5.0 from the exhaust iteration; emsams_delta
        // mirrors it (positive values are folded).
        assert_eq!(out.emissions_day[0], 5.0);
        assert_eq!(out.emsams_delta[0], 5.0);
        // The final wrtdat record carries the totals.
        assert_eq!(out.dat_records.len(), 1);
        let dat = &out.dat_records[0];
        // poptot = popcty * modfrc(0) = 100 * 1.0 = 100.0.
        assert_eq!(dat.population_total, 100.0);
        // fracretro = 0 since retrofit is disabled.
        assert_eq!(dat.frac_retrofitted, 0.0);
        assert_eq!(dat.units_retrofitted, 0.0);
    }
}

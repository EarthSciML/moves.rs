//! NONROAD–MOVES integration — the [`run_simulation`] entry point.
//!
//! Phase 5, Task 117. This module replaces the Java↔Fortran bridge —
//! `gov/epa/otaq/moves/master/nonroad/` (input generation) and the
//! worker-side `Nonroad{OutputDataLoader,PostProcessor}.java` (output
//! ingestion) — with a single in-process Rust function call. The
//! orchestrator builds a [`NonroadOptions`] and a [`NonroadInputs`],
//! calls [`run_simulation`], and gets a [`NonroadOutputs`] back. No
//! subprocess, no scratch files, no MariaDB ingestion step.
//!
//! # The three integration types
//!
//! | Java↔Fortran bridge stage      | Rust replacement      |
//! |--------------------------------|-----------------------|
//! | generate the `.opt` file       | [`NonroadOptions`]    |
//! | generate ~30 input data files  | [`NonroadInputs`]     |
//! | parse `.OUT`, ingest to MariaDB | [`NonroadOutputs`]    |
//!
//! [`NonroadOutputs::rows`] is a flat [`SimEmissionRow`] list shaped
//! for a straight field-copy onto the unified Phase 4 Parquet schema
//! (`moves-data`'s `output_schema`, Task 89) — the point at which the
//! migration plan places the onroad/nonroad output-schema
//! convergence. The cross-crate mapping itself lives on the
//! orchestrator side, which keeps `moves-nonroad` free of the
//! `arrow` / `parquet` dependency and preserves the crate's
//! WASM-compatibility posture (`ARCHITECTURE.md` § 4.4).
//!
//! # `run_simulation` is the driver-loop executor
//!
//! Task 113 ported `nonroad.f`'s record loop as a pure *planner*
//! ([`plan_scc_group`]) and named its
//! consumer: "the executor that runs each decision against the
//! geography routines … is the Task 117 integration layer."
//! [`run_simulation`] is that executor. It runs `nonroad.f`'s two-level
//! loop:
//!
//! - the **outer loop** walks [`NonroadInputs::scc_groups`] — one
//!   group is what `getpop` returns per pass (`nonroad.f` label `111`);
//! - the **inner loop** plans each group with
//!   [`plan_scc_group`] and dispatches
//!   every resulting [`DriverStep`](crate::driver::DriverStep) to a
//!   geography routine.
//!
//! The geography routines are reached through the
//! [`GeographyExecutor`] seam — see the [`executor`] module docs for
//! why the driver loop and the routines' numerical evaluation are kept
//! separable. [`PlanRecordingExecutor`] is the reference executor: it
//! records the dispatch plan and evaluates nothing, which makes
//! [`run_simulation`] a complete, exercised driver loop today and
//! leaves the production [`GeographyExecutor`] (which builds the
//! routines' callback contexts from loaded reference data) as a
//! cleanly-bounded following increment.
//!
//! # Signature note
//!
//! `ARCHITECTURE.md` § 7 sketches `run_simulation(&options, &inputs)`.
//! The realized signature adds a third parameter, the
//! [`GeographyExecutor`]. This is a deliberate, recorded deviation
//! (`ARCHITECTURE.md` § 4 requires deviations to be justified):
//! NONROAD's per-record emission evaluation needs the four geography-
//! routine callback families, and assembling those from loaded inputs
//! is substantial enough to be its own increment. A two-argument
//! `run_simulation` would either hide a half-built callback context
//! behind the signature or block this task on all of it. The explicit
//! third parameter makes the seam testable now (via
//! [`PlanRecordingExecutor`]) and is also the instrumentation hook the
//! numerical-fidelity harness needs.

pub mod executor;
pub mod inputs;
pub mod options;
pub mod outputs;

pub use executor::{
    DispatchContext, GeographyExecution, GeographyExecutor, PlanRecordingExecutor, RecordedDispatch,
};
pub use inputs::{NonroadInputs, SccGroup};
pub use options::{NonroadOptions, MAX_YEAR, MIN_YEAR};
pub use outputs::{EmissionChannel, NonroadOutputs, RunCounters, SimEmissionRow};

use crate::driver::{completion_message, plan_scc_group, StepOutcome};
use crate::Result;

/// Run one NONROAD simulation in-process.
///
/// This is the entry point the moves-rs orchestrator calls in place of
/// the old `nonroad.exe` subprocess. It validates `options`, walks
/// `nonroad.f`'s outer SCC-group loop and inner record loop over
/// `inputs`, dispatches every planned record to `geography`, and
/// returns the collected [`NonroadOutputs`].
///
/// # Parameters
///
/// - `options` — the run-global configuration (the in-memory `.opt`
///   file). [Validated](NonroadOptions::validate) before any work.
/// - `inputs` — the pre-loaded population groups and region selection.
/// - `geography` — the [`GeographyExecutor`] that evaluates (or
///   records) each geography-routine dispatch. Pass a
///   [`PlanRecordingExecutor`] for a dry run that produces the full
///   run structure (counters, completion banner, dispatch plan) with
///   no emission rows; pass a production executor for a numerical run.
///   Accepted as `&mut` so the executor accumulates state, and bounded
///   `?Sized` so a `&mut dyn GeographyExecutor` works too.
///
/// # Loop semantics
///
/// The outer loop preserves [`NonroadInputs::scc_groups`] order. For
/// each group, [`plan_scc_group`]
/// reproduces `nonroad.f`'s record-1 region pre-check and inner record
/// loop; a group the pre-check rejects contributes only to
/// [`RunCounters::scc_groups_skipped`]. Each surviving
/// [`DriverStep`](crate::driver::DriverStep) either is filtered out by
/// region selection ([`RunCounters::records_not_selected`]) or
/// dispatches to zero, one, or — for a subcounty record — two
/// geography routines, one [`GeographyExecutor::execute`] call each.
///
/// # Errors
///
/// - [`Error::Config`](crate::Error::Config) when
///   [`NonroadOptions::validate`] fails.
/// - Any error a geography routine raises through
///   [`GeographyExecutor::execute`]. Like the Fortran source, the port
///   has no per-record error recovery: the first error aborts the run.
///
/// # Examples
///
/// A dry run over an empty input bundle succeeds and produces the
/// "successful completion" banner:
///
/// ```
/// use moves_nonroad::driver::RegionLevel;
/// use moves_nonroad::simulation::{
///     run_simulation, NonroadInputs, NonroadOptions, PlanRecordingExecutor,
/// };
///
/// let options = NonroadOptions::new(RegionLevel::County, 2020);
/// let inputs = NonroadInputs::new();
/// let mut executor = PlanRecordingExecutor::new();
///
/// let outputs = run_simulation(&options, &inputs, &mut executor).unwrap();
/// assert!(outputs.rows.is_empty());
/// assert!(outputs.completion_message.starts_with("Successful completion"));
/// ```
pub fn run_simulation<G>(
    options: &NonroadOptions,
    inputs: &NonroadInputs,
    geography: &mut G,
) -> Result<NonroadOutputs>
where
    G: GeographyExecutor + ?Sized,
{
    options.validate()?;

    let mut outputs = NonroadOutputs::default();

    // nonroad.f label 111 — the outer getpop loop. One SccGroup is
    // one getpop pass.
    for group in &inputs.scc_groups {
        let plan = plan_scc_group(
            &group.scc,
            &group.records,
            options.region_level,
            &inputs.regions,
        );

        // nonroad.f :165–177 — the record-1 region pre-check can
        // reject the whole group before the record loop runs.
        if plan.group_skipped {
            outputs.counters.scc_groups_skipped += 1;
            continue;
        }
        outputs.counters.scc_groups_planned += 1;

        // nonroad.f label 333 — the inner record loop.
        for step in &plan.steps {
            outputs.counters.records_visited += 1;
            let dispatches = match &step.outcome {
                StepOutcome::NotSelected => {
                    outputs.counters.records_not_selected += 1;
                    continue;
                }
                StepOutcome::Dispatched(dispatches) => dispatches,
            };
            if dispatches.is_empty() {
                // Region shape / run level matched no dispatch branch
                // — nonroad.f falls straight through to the next
                // record.
                outputs.counters.records_no_dispatch += 1;
                continue;
            }

            let record = &group.records[step.record_index];
            for &dispatch in dispatches {
                let ctx = DispatchContext {
                    dispatch,
                    scc: &group.scc,
                    fuel: plan.fuel,
                    record,
                    growth: step.growth,
                };
                let execution = geography.execute(&ctx, options)?;
                outputs.counters.dispatch_calls += 1;
                if execution.skipped {
                    outputs.counters.geography_skips += 1;
                }
                outputs.absorb(execution);
            }
        }
    }

    // nonroad.f :346–356 — the closing banner, keyed off the warning
    // tally the geography routines accumulated.
    outputs.completion_message = completion_message(outputs.warnings.len() as i32);
    Ok(outputs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{Dispatch, DriverRecord, RegionLevel, RunRegions};
    use crate::emissions::exhaust::FuelKind;
    use crate::{Error, Result};

    fn rec(region: &str, hp: f32, pop: f32, year: i32) -> DriverRecord {
        DriverRecord {
            region_code: region.to_string(),
            hp_avg: hp,
            population: pop,
            pop_year: year,
        }
    }

    fn county_inputs() -> NonroadInputs {
        let mut inputs = NonroadInputs::new();
        inputs.regions = RunRegions {
            selected_counties: vec!["06037".to_string(), "06038".to_string()],
            ..Default::default()
        };
        inputs.push_group(
            "2270001010",
            vec![
                rec("06037", 25.0, 100.0, 2020),
                rec("06038", 25.0, 200.0, 2020),
            ],
        );
        inputs
    }

    /// A [`GeographyExecutor`] that returns a configurable canned
    /// [`GeographyExecution`] for every call. The driver loop's own
    /// [`RunCounters::dispatch_calls`] is the invocation count.
    struct CannedExecutor {
        /// When `Some`, every call returns this clone.
        canned: Option<GeographyExecution>,
    }

    impl GeographyExecutor for CannedExecutor {
        fn execute(
            &mut self,
            _ctx: &DispatchContext<'_>,
            _options: &NonroadOptions,
        ) -> Result<GeographyExecution> {
            Ok(self.canned.clone().unwrap_or_default())
        }
    }

    /// A [`GeographyExecutor`] that fails on its `fail_on`-th call
    /// (1-based).
    struct FailingExecutor {
        calls: usize,
        fail_on: usize,
    }

    impl GeographyExecutor for FailingExecutor {
        fn execute(
            &mut self,
            _ctx: &DispatchContext<'_>,
            _options: &NonroadOptions,
        ) -> Result<GeographyExecution> {
            self.calls += 1;
            if self.calls == self.fail_on {
                Err(Error::Config(format!(
                    "forced failure on call {}",
                    self.calls
                )))
            } else {
                Ok(GeographyExecution::default())
            }
        }
    }

    fn sample_row() -> SimEmissionRow {
        SimEmissionRow {
            fips: "06037".to_string(),
            subcounty: "     ".to_string(),
            scc: "2270001010".to_string(),
            hp_level: 50.0,
            model_year: None,
            tech_type: None,
            channel: EmissionChannel::Exhaust,
            population: 100.0,
            activity: 200.0,
            fuel_consumption: 30.0,
            emissions: vec![0.0; crate::common::consts::MXPOL],
        }
    }

    #[test]
    fn empty_run_succeeds_with_no_rows() {
        let options = NonroadOptions::new(RegionLevel::County, 2020);
        let inputs = NonroadInputs::new();
        let mut exec = PlanRecordingExecutor::new();
        let out = run_simulation(&options, &inputs, &mut exec).unwrap();
        assert!(out.rows.is_empty());
        assert_eq!(out.counters, RunCounters::default());
        assert!(out.completion_message.starts_with("Successful completion"));
        assert!(exec.is_empty());
    }

    #[test]
    fn invalid_options_abort_before_any_dispatch() {
        let mut options = NonroadOptions::new(RegionLevel::County, 2020);
        options.episode_year = 0;
        let inputs = county_inputs();
        let mut exec = PlanRecordingExecutor::new();
        let err = run_simulation(&options, &inputs, &mut exec).unwrap_err();
        match err {
            Error::Config(msg) => assert!(msg.contains("episode_year")),
            other => panic!("unexpected error: {other:?}"),
        }
        // Nothing dispatched — validation runs first.
        assert!(exec.is_empty());
    }

    #[test]
    fn simple_county_run_dispatches_each_record() {
        let options = NonroadOptions::new(RegionLevel::County, 2020);
        let inputs = county_inputs();
        let mut exec = PlanRecordingExecutor::new();
        let out = run_simulation(&options, &inputs, &mut exec).unwrap();

        assert_eq!(out.counters.scc_groups_planned, 1);
        assert_eq!(out.counters.scc_groups_skipped, 0);
        assert_eq!(out.counters.records_visited, 2);
        assert_eq!(out.counters.dispatch_calls, 2);
        assert_eq!(out.counters.records_not_selected, 0);
        assert_eq!(out.counters.records_no_dispatch, 0);

        assert_eq!(exec.len(), 2);
        for d in &exec.dispatches {
            assert_eq!(d.dispatch, Dispatch::County);
            assert_eq!(d.scc, "2270001010");
            assert_eq!(d.fuel, Some(FuelKind::Diesel));
        }
        assert_eq!(exec.dispatches[0].region_code, "06037");
        assert_eq!(exec.dispatches[1].region_code, "06038");
    }

    #[test]
    fn record_one_precheck_skips_the_whole_group() {
        let options = NonroadOptions::new(RegionLevel::County, 2020);
        let mut inputs = NonroadInputs::new();
        // First record's county is not selected ⇒ nonroad.f :165–177
        // rejects the entire SCC group.
        inputs.regions = RunRegions {
            selected_counties: vec!["06038".to_string()],
            ..Default::default()
        };
        inputs.push_group(
            "2270001010",
            vec![
                rec("06037", 25.0, 100.0, 2020), // not selected
                rec("06038", 25.0, 200.0, 2020),
            ],
        );
        let mut exec = PlanRecordingExecutor::new();
        let out = run_simulation(&options, &inputs, &mut exec).unwrap();
        assert_eq!(out.counters.scc_groups_skipped, 1);
        assert_eq!(out.counters.scc_groups_planned, 0);
        assert_eq!(out.counters.records_visited, 0);
        assert!(exec.is_empty());
    }

    #[test]
    fn unselected_records_are_counted_not_dispatched() {
        let options = NonroadOptions::new(RegionLevel::County, 2020);
        let mut inputs = NonroadInputs::new();
        inputs.regions = RunRegions {
            selected_counties: vec!["06037".to_string(), "06039".to_string()],
            ..Default::default()
        };
        inputs.push_group(
            "2270001010",
            vec![
                rec("06037", 25.0, 100.0, 2020),
                rec("06099", 25.0, 200.0, 2020), // not selected
                rec("06039", 25.0, 300.0, 2020),
            ],
        );
        let mut exec = PlanRecordingExecutor::new();
        let out = run_simulation(&options, &inputs, &mut exec).unwrap();
        assert_eq!(out.counters.records_visited, 3);
        assert_eq!(out.counters.records_not_selected, 1);
        assert_eq!(out.counters.dispatch_calls, 2);
        assert_eq!(exec.len(), 2);
    }

    #[test]
    fn growth_pair_threads_the_rate_into_the_dispatch() {
        let options = NonroadOptions::new(RegionLevel::County, 2020);
        let mut inputs = NonroadInputs::new();
        inputs.regions = RunRegions {
            selected_counties: vec!["06037".to_string(), "06038".to_string()],
            ..Default::default()
        };
        // Records 0 and 1 form a growth pair (same region + HP, years
        // differ); record 1 is consumed as the partner.
        inputs.push_group(
            "2270001010",
            vec![
                rec("06037", 25.0, 100.0, 2020),
                rec("06037", 25.0, 120.0, 2022),
                rec("06038", 25.0, 300.0, 2020),
            ],
        );
        let mut exec = PlanRecordingExecutor::new();
        let out = run_simulation(&options, &inputs, &mut exec).unwrap();
        // Two visited steps: record 0 (with growth), record 2.
        assert_eq!(out.counters.records_visited, 2);
        assert_eq!(exec.len(), 2);
        // Record 0 carries the growth rate (120-100)/(100*2) = 0.1.
        let g = exec.dispatches[0].growth.expect("growth on the pair");
        assert!((g - 0.1).abs() < 1e-6);
        assert_eq!(exec.dispatches[0].region_code, "06037");
        // Record 2 is not a growth record.
        assert_eq!(exec.dispatches[1].growth, None);
        assert_eq!(exec.dispatches[1].region_code, "06038");
    }

    #[test]
    fn subcounty_record_dispatches_to_two_routines() {
        let options = NonroadOptions::new(RegionLevel::Subcounty, 2020);
        let mut inputs = NonroadInputs::new();
        // A whole-county region entry triggers both prccty and prcsub
        // — one record, two dispatch calls.
        inputs.regions = RunRegions {
            selected_counties: vec!["06037".to_string()],
            region_list: vec!["06037".to_string()],
            ..Default::default()
        };
        inputs.push_group("2270001010", vec![rec("06037", 25.0, 100.0, 2020)]);
        let mut exec = PlanRecordingExecutor::new();
        let out = run_simulation(&options, &inputs, &mut exec).unwrap();
        assert_eq!(out.counters.records_visited, 1);
        assert_eq!(out.counters.dispatch_calls, 2);
        assert_eq!(exec.len(), 2);
        assert_eq!(exec.dispatches[0].dispatch, Dispatch::County);
        assert_eq!(exec.dispatches[1].dispatch, Dispatch::Subcounty);
    }

    #[test]
    fn no_dispatch_branch_is_counted() {
        // A national record on a county-level run matches no dispatch
        // branch — nonroad.f falls straight through.
        let options = NonroadOptions::new(RegionLevel::County, 2020);
        let mut inputs = NonroadInputs::new();
        inputs.push_group("2270001010", vec![rec("00000", 25.0, 100.0, 2020)]);
        let mut exec = PlanRecordingExecutor::new();
        let out = run_simulation(&options, &inputs, &mut exec).unwrap();
        assert_eq!(out.counters.records_visited, 1);
        assert_eq!(out.counters.records_no_dispatch, 1);
        assert_eq!(out.counters.dispatch_calls, 0);
        assert!(exec.is_empty());
    }

    #[test]
    fn executor_rows_are_absorbed_into_the_output() {
        let options = NonroadOptions::new(RegionLevel::County, 2020);
        let inputs = county_inputs();
        let mut exec = CannedExecutor {
            canned: Some(GeographyExecution {
                rows: vec![sample_row()],
                warnings: vec!["a warning".to_string()],
                skipped: false,
                national_record_count: 0,
            }),
        };
        let out = run_simulation(&options, &inputs, &mut exec).unwrap();
        // Two records, one canned row + one warning each.
        assert_eq!(out.counters.dispatch_calls, 2);
        assert_eq!(out.rows.len(), 2);
        assert_eq!(out.warnings.len(), 2);
        // Two warnings ⇒ the "Completion" (not "Successful") banner.
        assert!(out.completion_message.starts_with("Completion"));
        assert!(out.completion_message.contains("2 warnings"));
    }

    #[test]
    fn skipped_executions_are_tallied() {
        let options = NonroadOptions::new(RegionLevel::County, 2020);
        let inputs = county_inputs();
        let mut exec = CannedExecutor {
            canned: Some(GeographyExecution::skipped()),
        };
        let out = run_simulation(&options, &inputs, &mut exec).unwrap();
        assert_eq!(out.counters.dispatch_calls, 2);
        assert_eq!(out.counters.geography_skips, 2);
        assert!(out.rows.is_empty());
    }

    #[test]
    fn national_record_counts_accumulate() {
        let options = NonroadOptions::new(RegionLevel::County, 2020);
        let inputs = county_inputs();
        let mut exec = CannedExecutor {
            canned: Some(GeographyExecution {
                national_record_count: 4,
                ..Default::default()
            }),
        };
        let out = run_simulation(&options, &inputs, &mut exec).unwrap();
        // Two dispatches × 4 ⇒ 8.
        assert_eq!(out.national_record_count, 8);
    }

    #[test]
    fn a_geography_error_aborts_the_run() {
        let options = NonroadOptions::new(RegionLevel::County, 2020);
        let inputs = county_inputs();
        // Fail on the second dispatch call.
        let mut exec = FailingExecutor {
            calls: 0,
            fail_on: 2,
        };
        let err = run_simulation(&options, &inputs, &mut exec).unwrap_err();
        match err {
            Error::Config(msg) => assert!(msg.contains("call 2")),
            other => panic!("unexpected error: {other:?}"),
        }
        // The run stopped at the failing call.
        assert_eq!(exec.calls, 2);
    }

    #[test]
    fn outer_loop_preserves_scc_group_order() {
        let options = NonroadOptions::new(RegionLevel::County, 2020);
        let mut inputs = NonroadInputs::new();
        inputs.regions = RunRegions {
            selected_counties: vec!["06037".to_string()],
            ..Default::default()
        };
        inputs
            .push_group("2270001010", vec![rec("06037", 25.0, 100.0, 2020)])
            .push_group("2265001010", vec![rec("06037", 10.0, 50.0, 2020)]);
        let mut exec = PlanRecordingExecutor::new();
        let out = run_simulation(&options, &inputs, &mut exec).unwrap();
        assert_eq!(out.counters.scc_groups_planned, 2);
        assert_eq!(exec.len(), 2);
        // Diesel SCC first, then 4-stroke gas — input order.
        assert_eq!(exec.dispatches[0].scc, "2270001010");
        assert_eq!(exec.dispatches[0].fuel, Some(FuelKind::Diesel));
        assert_eq!(exec.dispatches[1].scc, "2265001010");
        assert_eq!(exec.dispatches[1].fuel, Some(FuelKind::Gasoline4Stroke));
    }

    #[test]
    fn run_simulation_accepts_a_trait_object() {
        // `?Sized` bound ⇒ a `&mut dyn GeographyExecutor` works.
        let options = NonroadOptions::new(RegionLevel::County, 2020);
        let inputs = county_inputs();
        let mut concrete = PlanRecordingExecutor::new();
        let dynamic: &mut dyn GeographyExecutor = &mut concrete;
        let out = run_simulation(&options, &inputs, dynamic).unwrap();
        assert_eq!(out.counters.dispatch_calls, 2);
        assert_eq!(concrete.len(), 2);
    }
}

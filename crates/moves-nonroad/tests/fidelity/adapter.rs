//! Adapters from `moves-nonroad` output types to [`ReferenceRecord`]s.
//!
//! The `dbgemit` instrumentation captures the gfortran reference's
//! intermediate state at four call sites. To diff the Rust port
//! against that capture, the port's output has to be expressed in
//! the *same* record shape — the same phase, the same labels, the
//! same value-vector lengths. These adapters are that bridge.
//!
//! | Phase | `moves-nonroad` output / source | Emitted labels |
//! |----------|---------------------------------------------------|------------------------------------------|
//! | `GETPOP` | `&[SelectedPopulation]` | `popeqp`, `avghpc`, `usehrs`, `ipopyr` |
//! | `GETPOP` | [`DispatchContext`] (executor boundary) | `popeqp`, `avghpc`, `ipopyr` |
//! | `AGEDIST`| `AgeDistributionResult` | `mdyrfrc`, `baspop` |
//! | `GRWFAC` | `GrowthFactor` | `factor`, `baseyearind`, `growthyearind` |
//! | `CLCEMS` | `ExhaustCalcOutputs` | `emsday`, `emsbmy` |
//! | `CLCEMS` | [`GeographyExecution`] rows (executor boundary) | `emsday` |
//!
//! The labels match the `dbgemit` patch table in
//! `characterization/nonroad-build/README.md`.
//!
//! # [`InstrumentingExecutor`]
//!
//! [`InstrumentingExecutor<G>`] wraps any [`GeographyExecutor`] and
//! records port-side [`ReferenceRecord`]s at the executor boundary.
//! It delegates every dispatch to the inner executor, then appends
//! [`Phase::Getpop`] records built from the [`DispatchContext`] and
//! [`Phase::Clcems`] records built from the returned
//! [`GeographyExecution`] rows. Intermediate AGEDIST and GRWFAC
//! state is not visible at this boundary and is therefore omitted;
//! those phases are captured only by the fine-grained adapters above.
//!
//! # Scope boundary
//!
//! The `CLCEMS` patch *also* emits the calculation's input-context
//! scalars `pop`, `mfrac`, `afac`, and `dage`. Those are inputs, not
//! fields of [`ExhaustCalcOutputs`], so they have no adapter here.
//! [`tolerance::classify`] already classifies all four so the
//! comparison engine handles them when a future hook emits them.
//!
//! [`tolerance::classify`]: super::tolerance::classify
//! [`DispatchContext`]: moves_nonroad::simulation::DispatchContext
//! [`GeographyExecution`]: moves_nonroad::simulation::GeographyExecution
//! [`GeographyExecutor`]: moves_nonroad::simulation::GeographyExecutor

use moves_nonroad::emissions::ExhaustCalcOutputs;
use moves_nonroad::population::{AgeDistributionResult, GrowthFactor, SelectedPopulation};
use moves_nonroad::simulation::{
    DispatchContext, EmissionChannel, GeographyExecution, GeographyExecutor, NonroadOptions,
};
use moves_nonroad::Result;

use super::reference::{Context, Phase, ReferenceRecord};

/// Widen a `real*4` slice to the `f64` values a [`ReferenceRecord`]
/// carries. The widening is lossless — every `f32` is exactly
/// representable as an `f64`.
fn widen(values: &[f32]) -> Vec<f64> {
    values.iter().map(|&v| v as f64).collect()
}

/// `AGEDIST` — turn an [`AgeDistributionResult`] into the `mdyrfrc`
/// and `baspop` records the `agedist.f` patch emits.
pub fn agedist_records(ctx: &Context, result: &AgeDistributionResult) -> Vec<ReferenceRecord> {
    vec![
        ReferenceRecord::new(
            Phase::Agedist,
            ctx.clone(),
            "mdyrfrc",
            widen(&result.mdyrfrc),
        ),
        ReferenceRecord::new(
            Phase::Agedist,
            ctx.clone(),
            "baspop",
            vec![result.base_population as f64],
        ),
    ]
}

/// `GRWFAC` — turn a [`GrowthFactor`] into the `factor`,
/// `baseyearind`, and `growthyearind` records the `grwfac.f` patch
/// emits.
pub fn grwfac_records(ctx: &Context, gf: &GrowthFactor) -> Vec<ReferenceRecord> {
    vec![
        ReferenceRecord::new(Phase::Grwfac, ctx.clone(), "factor", vec![gf.factor as f64]),
        ReferenceRecord::new(
            Phase::Grwfac,
            ctx.clone(),
            "baseyearind",
            vec![gf.base_indicator as f64],
        ),
        ReferenceRecord::new(
            Phase::Grwfac,
            ctx.clone(),
            "growthyearind",
            vec![gf.growth_indicator as f64],
        ),
    ]
}

/// `GETPOP` — turn the selected populations into the per-record
/// arrays the `getpop.f` patch emits (`popeqp`, `avghpc`, `usehrs`,
/// `ipopyr`), each of length `npoprc`.
pub fn getpop_records(ctx: &Context, pops: &[SelectedPopulation]) -> Vec<ReferenceRecord> {
    vec![
        ReferenceRecord::new(
            Phase::Getpop,
            ctx.clone(),
            "popeqp",
            pops.iter().map(|p| p.population as f64).collect(),
        ),
        ReferenceRecord::new(
            Phase::Getpop,
            ctx.clone(),
            "avghpc",
            pops.iter().map(|p| p.hp_avg as f64).collect(),
        ),
        ReferenceRecord::new(
            Phase::Getpop,
            ctx.clone(),
            "usehrs",
            pops.iter().map(|p| p.usage as f64).collect(),
        ),
        ReferenceRecord::new(
            Phase::Getpop,
            ctx.clone(),
            "ipopyr",
            pops.iter().map(|p| f64::from(p.year)).collect(),
        ),
    ]
}

/// `CLCEMS` — turn [`ExhaustCalcOutputs`] into the `emsday` and
/// `emsbmy` records the `clcems.f` patch emits. See the module-level
/// "Scope boundary" note for the `pop`/`mfrac`/`afac`/`dage`
/// input-context scalars.
pub fn clcems_records(ctx: &Context, out: &ExhaustCalcOutputs) -> Vec<ReferenceRecord> {
    vec![
        ReferenceRecord::new(
            Phase::Clcems,
            ctx.clone(),
            "emsday",
            widen(&out.emissions_day),
        ),
        ReferenceRecord::new(
            Phase::Clcems,
            ctx.clone(),
            "emsbmy",
            widen(&out.emissions_by_model_year),
        ),
    ]
}

// =============================================================================
// InstrumentingExecutor — executor-boundary port-side capture
// =============================================================================

/// Wraps any [`GeographyExecutor`] and collects per-dispatch
/// [`ReferenceRecord`]s from the executor boundary.
///
/// On each [`execute`](GeographyExecutor::execute) call the executor:
/// 1. delegates to the inner executor;
/// 2. appends [`Phase::Getpop`] records from the [`DispatchContext`]
/// via [`getpop_records_from_driver`]; and
/// 3. appends [`Phase::Clcems`] records from the returned
/// [`GeographyExecution`] rows via [`clcems_records_from_rows`].
///
/// The accumulated [`captured`](Self::captured) records are the
/// port-side input to [`super::divergence::compare_runs`].
pub struct InstrumentingExecutor<G: GeographyExecutor> {
 /// The wrapped executor that evaluates the geography routines.
    pub inner: G,
 /// Port-side records accumulated across all dispatches.
    pub captured: Vec<ReferenceRecord>,
    call_counter: usize,
}

impl<G: GeographyExecutor> InstrumentingExecutor<G> {
 /// Wrap `inner` in a new instrumenting executor with an empty
 /// capture log.
    pub fn new(inner: G) -> Self {
        Self {
            inner,
            captured: Vec::new(),
            call_counter: 0,
        }
    }
}

impl<G: GeographyExecutor> GeographyExecutor for InstrumentingExecutor<G> {
    fn execute(
        &mut self,
        ctx: &DispatchContext<'_>,
        options: &NonroadOptions,
    ) -> Result<GeographyExecution> {
        self.call_counter += 1;
        let execution = self.inner.execute(ctx, options)?;

        let adapter_ctx = Context::parse(&format!(
            "call={},fips={},year={}",
            self.call_counter, ctx.record.region_code, ctx.record.pop_year
        ));
        self.captured
            .extend(getpop_records_from_driver(&adapter_ctx, ctx));
        self.captured
            .extend(clcems_records_from_rows(&adapter_ctx, &execution));

        Ok(execution)
    }
}

/// `GETPOP`-phase adapter: build [`ReferenceRecord`]s from a
/// [`DispatchContext`] at the executor boundary.
///
/// Emits `popeqp`, `avghpc`, and `ipopyr`. The `usehrs` field from
/// the gfortran `getpop.f` patch is not carried in [`DispatchContext`]
/// and is omitted.
pub fn getpop_records_from_driver(
    ctx: &Context,
    dispatch: &DispatchContext<'_>,
) -> Vec<ReferenceRecord> {
    vec![
        ReferenceRecord::new(
            Phase::Getpop,
            ctx.clone(),
            "popeqp",
            vec![dispatch.record.population as f64],
        ),
        ReferenceRecord::new(
            Phase::Getpop,
            ctx.clone(),
            "avghpc",
            vec![dispatch.record.hp_avg as f64],
        ),
        ReferenceRecord::new(
            Phase::Getpop,
            ctx.clone(),
            "ipopyr",
            vec![dispatch.record.pop_year as f64],
        ),
    ]
}

/// `CLCEMS`-phase adapter: build [`ReferenceRecord`]s from
/// [`GeographyExecution`] rows.
///
/// Emits one `emsday` record per exhaust row. The values are the
/// row's per-pollutant emission totals widened to `f64`.
pub fn clcems_records_from_rows(
    ctx: &Context,
    execution: &GeographyExecution,
) -> Vec<ReferenceRecord> {
    execution
        .rows
        .iter()
        .filter(|row| row.channel == EmissionChannel::Exhaust)
        .map(|row| {
            ReferenceRecord::new(
                Phase::Clcems,
                ctx.clone(),
                "emsday",
                row.emissions.iter().map(|&v| v as f64).collect(),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> Context {
        Context::parse("call=1,fips=26000")
    }

    #[test]
    fn agedist_adapter_emits_mdyrfrc_and_baspop() {
        let result = AgeDistributionResult {
            base_population: 1234.5,
            mdyrfrc: vec![0.1, 0.5, 0.4],
            warnings: Vec::new(),
        };
        let records = agedist_records(&ctx(), &result);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].phase, Phase::Agedist);
        assert_eq!(records[0].label, "mdyrfrc");
        assert_eq!(
            records[0].values,
            vec![0.1f32 as f64, 0.5f32 as f64, 0.4f32 as f64]
        );
        assert_eq!(records[1].label, "baspop");
        assert_eq!(records[1].values, vec![1234.5]);
    }

    #[test]
    fn grwfac_adapter_emits_three_scalars() {
        let gf = GrowthFactor {
            factor: 0.025,
            base_indicator: 1.0,
            growth_indicator: 1.025,
            warning: None,
        };
        let records = grwfac_records(&ctx(), &gf);
        assert_eq!(records.len(), 3);
        let labels: Vec<&str> = records.iter().map(|r| r.label.as_str()).collect();
        assert_eq!(labels, ["factor", "baseyearind", "growthyearind"]);
        for r in &records {
            assert_eq!(r.phase, Phase::Grwfac);
            assert_eq!(r.values.len(), 1);
        }
        assert_eq!(records[0].values[0], 0.025f32 as f64);
    }

    #[test]
    fn getpop_adapter_emits_four_parallel_arrays() {
        let pops = vec![
            SelectedPopulation {
                fips: "26000".to_string(),
                subregion: String::new(),
                year: 2018,
                hp_avg: 50.0,
                hp_min: 25.0,
                hp_max: 75.0,
                usage: 400.0,
                tech_code: "T".to_string(),
                population: 100.0,
            },
            SelectedPopulation {
                fips: "26000".to_string(),
                subregion: String::new(),
                year: 2021,
                hp_avg: 60.0,
                hp_min: 50.0,
                hp_max: 100.0,
                usage: 500.0,
                tech_code: "T".to_string(),
                population: 250.0,
            },
        ];
        let records = getpop_records(&ctx(), &pops);
        assert_eq!(records.len(), 4);
        let popeqp = records.iter().find(|r| r.label == "popeqp").unwrap();
        assert_eq!(popeqp.values, vec![100.0, 250.0]);
        let ipopyr = records.iter().find(|r| r.label == "ipopyr").unwrap();
        assert_eq!(ipopyr.values, vec![2018.0, 2021.0]);
 // Every array has one entry per selected population.
        for r in &records {
            assert_eq!(r.values.len(), pops.len());
        }
    }

    #[test]
    fn getpop_adapter_handles_empty_selection() {
        let records = getpop_records(&ctx(), &[]);
        assert_eq!(records.len(), 4);
        for r in &records {
            assert!(r.values.is_empty());
        }
    }

    #[test]
    fn clcems_adapter_emits_emsday_and_emsbmy() {
        let out = ExhaustCalcOutputs {
            emissions_day: vec![1.0, 2.0, 3.0],
            emissions_by_model_year: vec![10.0, 20.0, 30.0],
        };
        let records = clcems_records(&ctx(), &out);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].label, "emsday");
        assert_eq!(records[0].values, vec![1.0, 2.0, 3.0]);
        assert_eq!(records[1].label, "emsbmy");
        assert_eq!(records[1].values, vec![10.0, 20.0, 30.0]);
    }

    #[test]
    fn adapter_records_carry_the_supplied_context() {
        let records = clcems_records(&ctx(), &ExhaustCalcOutputs::default());
        for r in &records {
            assert_eq!(r.context.get("fips"), Some("26000"));
            assert_eq!(r.context.call(), Some(1));
        }
    }

    #[test]
    fn getpop_from_driver_emits_three_fields() {
        use moves_nonroad::driver::{Dispatch, DriverRecord};
        let record = DriverRecord {
            region_code: "26000".to_string(),
            hp_avg: 50.0,
            population: 100.0,
            pop_year: 2021,
            median_life: 0.0,
        };
        let dispatch_ctx = DispatchContext {
            dispatch: Dispatch::County,
            scc: "2270001010",
            fuel: None,
            record: &record,
            growth: None,
        };
        let ctx = Context::parse("call=1,fips=26000");
        let records = getpop_records_from_driver(&ctx, &dispatch_ctx);
        assert_eq!(records.len(), 3);
        let labels: Vec<&str> = records.iter().map(|r| r.label.as_str()).collect();
        assert_eq!(labels, ["popeqp", "avghpc", "ipopyr"]);
        assert_eq!(records[0].values, vec![100.0]);
        assert_eq!(records[1].values, vec![50.0f32 as f64]);
        assert_eq!(records[2].values, vec![2021.0]);
        for r in &records {
            assert_eq!(r.phase, Phase::Getpop);
        }
    }

    #[test]
    fn clcems_from_rows_emits_exhaust_only() {
        use moves_nonroad::simulation::{EmissionChannel, SimEmissionRow};
        fn row(channel: EmissionChannel) -> SimEmissionRow {
            SimEmissionRow {
                fips: "26000".to_string(),
                subcounty: "     ".to_string(),
                scc: "2270001010".to_string(),
                hp_level: 50.0,
                model_year: None,
                tech_type: None,
                channel,
                population: 100.0,
                activity: 200.0,
                fuel_consumption: 30.0,
                emissions: vec![1.0, 2.0, 3.0],
            }
        }
        let execution = GeographyExecution {
            rows: vec![
                row(EmissionChannel::Exhaust),
                row(EmissionChannel::Evaporative),
            ],
            warnings: Vec::new(),
            skipped: false,
            national_record_count: 0,
        };
        let ctx = Context::parse("call=1,fips=26000");
        let records = clcems_records_from_rows(&ctx, &execution);
        assert_eq!(records.len(), 1, "only exhaust rows become CLCEMS records");
        assert_eq!(records[0].phase, Phase::Clcems);
        assert_eq!(records[0].label, "emsday");
        assert_eq!(records[0].values, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn clcems_from_empty_execution_is_empty() {
        let records = clcems_records_from_rows(&ctx(), &GeographyExecution::default());
        assert!(records.is_empty());
    }

    #[test]
    fn instrumenting_executor_records_getpop_per_dispatch() {
        use moves_nonroad::driver::{Dispatch, DriverRecord, RegionLevel};
        use moves_nonroad::simulation::{NonroadOptions, PlanRecordingExecutor};
        let record = DriverRecord {
            region_code: "26000".to_string(),
            hp_avg: 50.0,
            population: 100.0,
            pop_year: 2021,
            median_life: 0.0,
        };
        let options = NonroadOptions::new(RegionLevel::County, 2021);
        let dispatch_ctx = DispatchContext {
            dispatch: Dispatch::County,
            scc: "2270001010",
            fuel: None,
            record: &record,
            growth: None,
        };
        let inner = PlanRecordingExecutor::new();
        let mut instr = InstrumentingExecutor::new(inner);
        instr.execute(&dispatch_ctx, &options).unwrap();

 // Three GETPOP records per dispatch (popeqp, avghpc, ipopyr).
        let getpop: Vec<_> = instr
            .captured
            .iter()
            .filter(|r| r.phase == Phase::Getpop)
            .collect();
        assert_eq!(getpop.len(), 3);
        let labels: Vec<&str> = getpop.iter().map(|r| r.label.as_str()).collect();
        assert!(labels.contains(&"popeqp"));
        assert!(labels.contains(&"ipopyr"));

 // PlanRecordingExecutor returns no rows → no CLCEMS records.
        let clcems_count = instr
            .captured
            .iter()
            .filter(|r| r.phase == Phase::Clcems)
            .count();
        assert_eq!(clcems_count, 0);

 // Call counter is reflected in the context.
        assert_eq!(instr.captured[0].context.call(), Some(1));
    }

    #[test]
    fn instrumenting_executor_increments_call_counter() {
        use moves_nonroad::driver::{Dispatch, DriverRecord, RegionLevel};
        use moves_nonroad::simulation::{NonroadOptions, PlanRecordingExecutor};
        let record = DriverRecord {
            region_code: "26000".to_string(),
            hp_avg: 50.0,
            population: 100.0,
            pop_year: 2021,
            median_life: 0.0,
        };
        let options = NonroadOptions::new(RegionLevel::County, 2021);
        let dispatch_ctx = DispatchContext {
            dispatch: Dispatch::County,
            scc: "2270001010",
            fuel: None,
            record: &record,
            growth: None,
        };
        let inner = PlanRecordingExecutor::new();
        let mut instr = InstrumentingExecutor::new(inner);
        instr.execute(&dispatch_ctx, &options).unwrap();
        instr.execute(&dispatch_ctx, &options).unwrap();

 // Two dispatches × 3 GETPOP records each = 6 total.
        assert_eq!(instr.captured.len(), 6);
 // Second dispatch has call=2.
        let second_call_records: Vec<_> = instr
            .captured
            .iter()
            .filter(|r| r.context.call() == Some(2))
            .collect();
        assert_eq!(second_call_records.len(), 3);
    }
}

//! Adapters from `moves-nonroad` output types to [`ReferenceRecord`]s.
//!
//! The `dbgemit` instrumentation captures the gfortran reference's
//! intermediate state at four call sites. To diff the Rust port
//! against that capture, the port's output has to be expressed in
//! the *same* record shape — the same phase, the same labels, the
//! same value-vector lengths. These adapters are that bridge.
//!
//! | Phase    | `moves-nonroad` output      | Emitted labels                       |
//! |----------|-----------------------------|--------------------------------------|
//! | `GETPOP` | `&[SelectedPopulation]`     | `popeqp`, `avghpc`, `usehrs`, `ipopyr` |
//! | `AGEDIST`| `AgeDistributionResult`     | `mdyrfrc`, `baspop`                  |
//! | `GRWFAC` | `GrowthFactor`              | `factor`, `baseyearind`, `growthyearind` |
//! | `CLCEMS` | `ExhaustCalcOutputs`        | `emsday`, `emsbmy`                   |
//!
//! The labels match the `dbgemit` patch table in
//! `characterization/nonroad-build/README.md`.
//!
//! # Scope boundary
//!
//! The `CLCEMS` patch *also* emits the calculation's input-context
//! scalars `pop`, `mfrac`, `afac`, and `dage`. Those are inputs, not
//! fields of [`ExhaustCalcOutputs`], so they have no adapter here —
//! when Task 117 wires `run_simulation` with port-side
//! instrumentation it emits them inline at the call site (they are
//! scalars already in scope: `ReferenceRecord::new(Phase::Clcems,
//! ctx, "pop", vec![pop])`). [`tolerance::classify`] already
//! classifies all four so the comparison engine handles them.
//!
//! [`tolerance::classify`]: super::tolerance::classify

use moves_nonroad::emissions::ExhaustCalcOutputs;
use moves_nonroad::population::{AgeDistributionResult, GrowthFactor, SelectedPopulation};

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
}

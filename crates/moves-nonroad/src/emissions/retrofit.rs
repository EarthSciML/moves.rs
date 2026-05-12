//! Retrofit-emission calculation (Task 108).
//!
//! Ports `clcrtrft.f` (309 lines) and the four retrofit validators
//! (`vldrtrftrecs.f`, `vldrtrfthp.f`, `vldrtrftscc.f`,
//! `vldrtrfttchtyp.f`). The validators run upstream in the input
//! parser; the calculator below consumes the filtered records they
//! produced.

use crate::common::consts::NRTRFTPLLTNT;
use crate::population::retrofit::{RetrofitPollutant, RetrofitRecord};
use crate::{Error, Result};

/// The four pollutants tracked by retrofit records, in
/// `rtrftplltnt` order. This corresponds to the 0..3 slot used by
/// the per-retrofit accumulator arrays (Fortran `plltnt = 1..4`).
const RETROFIT_POLLUTANTS: [RetrofitPollutant; NRTRFTPLLTNT] = [
    RetrofitPollutant::Hc,
    RetrofitPollutant::Co,
    RetrofitPollutant::Nox,
    RetrofitPollutant::Pm,
];

/// Identifying context for warnings and errors emitted by
/// [`calculate_retrofit_reduction`].
///
/// Mirrors the `scc`/`hpavg`/`mdlyr`/`techtype` arguments of
/// `clcrtrft` (the Fortran routine uses them only in its
/// `IOWSTD`/`IOWMSG` log statements; the Rust port preserves them
/// on the warning/error variants so callers can format equivalent
/// log entries).
#[derive(Debug, Clone)]
pub struct RetrofitCalcContext<'a> {
    /// 10-character SCC for the current model iteration.
    pub scc: &'a str,
    /// HP-average for the current model iteration.
    pub hp_avg: f32,
    /// Model year for the current model iteration.
    pub model_year: i32,
    /// 10-character tech type for the current model iteration.
    pub tech_type: &'a str,
}

/// Outcome of one [`calculate_retrofit_reduction`] call. Replaces
/// the Fortran `fracretro` / `unitsretro` output arguments and adds
/// the side-channel for non-fatal warnings.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RetrofitReductionOutcome {
    /// Total fraction of the current iteration's engine population
    /// that is retrofitted (Fortran `fracretro`). Clamped to
    /// `[0.0, 1.0]`.
    pub frac_retro: f32,
    /// Number of engines retrofitted, `pop * frac_retro` (Fortran
    /// `unitsretro`).
    pub units_retro: f32,
    /// Non-fatal warnings produced while computing reductions.
    pub warnings: Vec<RetrofitCalcWarning>,
}

/// Non-fatal warnings emitted by [`calculate_retrofit_reduction`].
///
/// The Fortran source writes these to `IOWSTD` / `IOWMSG`; the Rust
/// port returns them as data so callers can log, count, or surface
/// them in whatever way fits the wider system.
#[derive(Debug, Clone, PartialEq)]
pub enum RetrofitCalcWarning {
    /// One retrofit-pollutant accumulator exceeded `1.0`; the value
    /// was clamped to `1.0` before being folded into the per-pollutant
    /// reduction. Mirrors `clcrtrft.f` :199–224 (`8001` format).
    FractionExceedsOne {
        /// Retrofit ID from `rtrftid` whose accumulator overflowed.
        retrofit_id: i32,
        /// Pollutant whose accumulator overflowed.
        pollutant: RetrofitPollutant,
        /// Value before clamping (`rtrftplltntfracretro(rtrft, plltnt)`).
        frac_retro: f32,
        /// Iteration context (SCC, HP, model year, tech type),
        /// preserved so callers can format the original Fortran-style
        /// log entry.
        context: RetrofitCalcContextOwned,
    },
    /// The summed per-pollutant reduction fraction exceeded `1.0`;
    /// the value was clamped to `1.0`. Mirrors `clcrtrft.f` :238–260
    /// (`8002` format).
    ReductionFractionExceedsOne {
        /// Pollutant whose reduction fraction overflowed.
        pollutant: RetrofitPollutant,
        /// Iteration context.
        context: RetrofitCalcContextOwned,
    },
}

/// Owned copy of [`RetrofitCalcContext`] (the borrowed form would
/// tie the warning's lifetime to the caller's strings, which is
/// awkward when warnings outlive the call).
#[derive(Debug, Clone, PartialEq)]
pub struct RetrofitCalcContextOwned {
    /// SCC of the iteration.
    pub scc: String,
    /// HP-average of the iteration.
    pub hp_avg: f32,
    /// Model year of the iteration.
    pub model_year: i32,
    /// Tech type of the iteration.
    pub tech_type: String,
}

impl<'a> RetrofitCalcContext<'a> {
    fn to_owned_context(&self) -> RetrofitCalcContextOwned {
        RetrofitCalcContextOwned {
            scc: self.scc.to_string(),
            hp_avg: self.hp_avg,
            model_year: self.model_year,
            tech_type: self.tech_type.to_string(),
        }
    }
}

/// Compute the retrofit reductions for the current model iteration.
/// Ports `clcrtrft.f`.
///
/// `filtered_records` is the slice of [`RetrofitRecord`]s that
/// affect the current `(SCC, HP, model_year, tech_type)` tuple — the
/// `rtrftfltr3` array in the Fortran source, materialised by
/// `fndrtrft(fltrtyp=3, …)` upstream. `episode_year` is the run's
/// evaluation year (`iepyr` in `nonrdusr.inc`); it caps each
/// record's retrofit-year span via `min(year_retrofit_end,
/// episode_year)`.
///
/// `pop` is the engine population for the current iteration. It is
/// used both to convert "N units retrofitted" records into a
/// fraction (`tmpfrac = N / pop`) and to scale `frac_retro` into the
/// output `units_retro = pop * frac_retro`.
///
/// `pollutant_reduction_fraction` is the persistent
/// `rtrftplltntrdfrc` array from [`RetrofitState`]. The four
/// retrofit-pollutant slots are zeroed and then summed over the
/// surviving retrofits; other slots are left untouched. Indexing is
/// 0-based with main-pollutant-index minus one, matching
/// [`RetrofitPollutant::pollutant_index`]: HC at slot 0, CO at 1,
/// NOX at 2, PM at 5.
///
/// Returns the [`RetrofitReductionOutcome`] holding `frac_retro`,
/// `units_retro`, and any non-fatal warnings. Returns
/// [`Error::RetrofitNUnitsExceedPopulation`] when a retrofit
/// requesting an absolute number of units would exceed the
/// available population (the Fortran `7000` error path).
///
/// [`RetrofitState`]: crate::population::retrofit::RetrofitState
pub fn calculate_retrofit_reduction(
    filtered_records: &[&RetrofitRecord],
    pop: f32,
    episode_year: i32,
    pollutant_reduction_fraction: &mut [f32],
    ctx: &RetrofitCalcContext<'_>,
) -> Result<RetrofitReductionOutcome> {
    // Per-retrofit accumulators keyed by the order in which a new
    // retrofit ID first appears in `filtered_records` (the Fortran
    // source threads the same ordering via the `rtrftidxid` array
    // and the `numrtrft` counter; `clcrtrft.f` :104–:133).
    let mut retrofit_ids: Vec<i32> = Vec::new();
    let mut frac_retro: Vec<[f32; NRTRFTPLLTNT]> = Vec::new();
    let mut has_n_units: Vec<[bool; NRTRFTPLLTNT]> = Vec::new();
    let mut effect: Vec<[f32; NRTRFTPLLTNT]> = Vec::new();

    // --- Phase 1: accumulate fraction and effect per retrofit-pollutant
    //              (clcrtrft.f :115–:156) ---
    for record in filtered_records {
        let rtrft_slot = match retrofit_ids.iter().position(|&id| id == record.id) {
            Some(idx) => idx,
            None => {
                retrofit_ids.push(record.id);
                frac_retro.push([0.0; NRTRFTPLLTNT]);
                has_n_units.push([false; NRTRFTPLLTNT]);
                effect.push([0.0; NRTRFTPLLTNT]);
                retrofit_ids.len() - 1
            }
        };

        // rycount = min(rtrftryen, iepyr) - rtrftryst + 1
        //   (clcrtrft.f :137–:139).
        let ry_end = record.year_retrofit_end.min(episode_year);
        let ry_count = (ry_end - record.year_retrofit_start + 1) as f32;

        // The record carries `pollutant_idx` as the main pollutant
        // index (1, 2, 3, 6); map it down to the 0..3 slot used by
        // the local accumulators.
        let pollutant =
            RetrofitPollutant::from_pollutant_index(record.pollutant_idx).ok_or_else(|| {
                Error::Config(format!(
                    "retrofit record {} (id={}) has invalid pollutant_idx={}",
                    record.record_index, record.id, record.pollutant_idx
                ))
            })?;
        let plltnt_slot = pollutant.slot();

        // If `annual_frac_or_count > 1`, the value is an absolute
        // count of engines, not a fraction. Convert to a fraction of
        // `pop` and mark the slot so phase 2 can catch "asks for more
        // engines than exist" (clcrtrft.f :145–:150).
        let tmp_frac = if record.annual_frac_or_count > 1.0 {
            has_n_units[rtrft_slot][plltnt_slot] = true;
            record.annual_frac_or_count / pop
        } else {
            record.annual_frac_or_count
        };

        frac_retro[rtrft_slot][plltnt_slot] += tmp_frac * ry_count;
        // Effect is constant per (retrofit, pollutant); the Fortran
        // assigns it on every iteration (`clcrtrft.f` :154) for the
        // same reason — last-writer-wins on identical values.
        effect[rtrft_slot][plltnt_slot] = record.effectiveness;
    }

    let num_retrofits = retrofit_ids.len();
    let mut total_frac_retro = 0.0_f32;

    // --- Phase 2: total fraction retrofitted, with N-units guard
    //              (clcrtrft.f :161–:184) ---
    for rtrft_slot in 0..num_retrofits {
        for &pollutant in &RETROFIT_POLLUTANTS {
            let plltnt_slot = pollutant.slot();
            let f = frac_retro[rtrft_slot][plltnt_slot];
            if f > 0.0 {
                if has_n_units[rtrft_slot][plltnt_slot] && f > 1.0 {
                    return Err(Error::RetrofitNUnitsExceedPopulation {
                        retrofit_id: retrofit_ids[rtrft_slot],
                        pollutant: pollutant.canonical_name().to_string(),
                        scc: ctx.scc.to_string(),
                        hp_avg: ctx.hp_avg,
                        model_year: ctx.model_year,
                        tech_type: ctx.tech_type.to_string(),
                        n_units_requested: f * pop,
                        n_units_existing: pop,
                    });
                }
                // "Fraction retrofitted always same for all pollutants
                // for a given retrofit and must only be added once,
                // so once a non-zero value is found exit the loop."
                // (clcrtrft.f :177–:182).
                total_frac_retro += f;
                break;
            }
        }
    }
    let total_frac_retro = total_frac_retro.min(1.0);
    let units_retro = pop * total_frac_retro;

    // --- Phase 3: per-pollutant reduction fractions (clcrtrft.f :188–:233) ---
    // Zero out the four retrofit-pollutant slots of the output array
    // (other slots stay untouched, matching the Fortran COMMON-block
    // behaviour where `rtrftplltntrdfrc(idxmp(plltnt))` is the only
    // index written here).
    let mut warnings = Vec::new();
    for &pollutant in &RETROFIT_POLLUTANTS {
        let main_idx = (pollutant.pollutant_index() - 1) as usize;
        pollutant_reduction_fraction[main_idx] = 0.0;
    }
    for rtrft_slot in 0..num_retrofits {
        for &pollutant in &RETROFIT_POLLUTANTS {
            let plltnt_slot = pollutant.slot();
            let mut f = frac_retro[rtrft_slot][plltnt_slot];
            if f == 0.0 {
                continue;
            }
            if f > 1.0 {
                warnings.push(RetrofitCalcWarning::FractionExceedsOne {
                    retrofit_id: retrofit_ids[rtrft_slot],
                    pollutant,
                    frac_retro: f,
                    context: ctx.to_owned_context(),
                });
                f = 1.0;
            }
            let main_idx = (pollutant.pollutant_index() - 1) as usize;
            pollutant_reduction_fraction[main_idx] += f * effect[rtrft_slot][plltnt_slot];
        }
    }

    // --- Phase 4: clamp the per-pollutant reduction fractions to 1
    //              (clcrtrft.f :237–:261) ---
    for &pollutant in &RETROFIT_POLLUTANTS {
        let main_idx = (pollutant.pollutant_index() - 1) as usize;
        if pollutant_reduction_fraction[main_idx] > 1.0 {
            warnings.push(RetrofitCalcWarning::ReductionFractionExceedsOne {
                pollutant,
                context: ctx.to_owned_context(),
            });
            pollutant_reduction_fraction[main_idx] = 1.0;
        }
    }

    Ok(RetrofitReductionOutcome {
        frac_retro: total_frac_retro,
        units_retro,
        warnings,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::consts::MXPOL;
    use crate::population::retrofit::init_retrofit_state;

    fn ctx_default() -> RetrofitCalcContext<'static> {
        RetrofitCalcContext {
            scc: "2270002003",
            hp_avg: 75.0,
            model_year: 2010,
            tech_type: "T2",
        }
    }

    fn make_record(
        id: i32,
        pollutant: RetrofitPollutant,
        ry_start: i32,
        ry_end: i32,
        annual_frac_or_count: f32,
        effectiveness: f32,
    ) -> RetrofitRecord {
        RetrofitRecord {
            record_index: 0,
            id,
            year_retrofit_start: ry_start,
            year_retrofit_end: ry_end,
            year_model_start: 2000,
            year_model_end: 2010,
            scc: "ALL".to_string(),
            tech_type: "ALL".to_string(),
            hp_min: 0.0,
            hp_max: 100.0,
            annual_frac_or_count,
            effectiveness,
            pollutant: pollutant.canonical_name().to_string(),
            pollutant_idx: pollutant.pollutant_index(),
        }
    }

    #[test]
    fn empty_filter_produces_zero_output_and_zeros_retrofit_slots() {
        let mut state = init_retrofit_state();
        // Seed the four retrofit slots and one unrelated slot to
        // confirm only the four are touched.
        state.pollutant_reduction_fraction[RetrofitPollutant::Hc.pollutant_index() as usize - 1] =
            0.5;
        state.pollutant_reduction_fraction[10] = 0.42;

        let outcome = calculate_retrofit_reduction(
            &[],
            100.0,
            2020,
            &mut state.pollutant_reduction_fraction,
            &ctx_default(),
        )
        .unwrap();
        assert_eq!(outcome.frac_retro, 0.0);
        assert_eq!(outcome.units_retro, 0.0);
        assert!(outcome.warnings.is_empty());

        for p in RETROFIT_POLLUTANTS {
            let idx = (p.pollutant_index() - 1) as usize;
            assert_eq!(state.pollutant_reduction_fraction[idx], 0.0);
        }
        assert_eq!(state.pollutant_reduction_fraction[10], 0.42);
    }

    #[test]
    fn single_record_yields_frac_times_rycount_and_units() {
        let record = make_record(7, RetrofitPollutant::Hc, 2010, 2014, 0.1, 0.5);
        let recs: Vec<&RetrofitRecord> = vec![&record];
        let mut state = init_retrofit_state();
        let outcome = calculate_retrofit_reduction(
            &recs,
            200.0,
            2020,
            &mut state.pollutant_reduction_fraction,
            &ctx_default(),
        )
        .unwrap();
        // 5 retrofit years (2010..=2014 capped by iepyr=2020), 0.1 each.
        assert!((outcome.frac_retro - 0.5).abs() < 1e-6);
        assert!((outcome.units_retro - 100.0).abs() < 1e-4);

        let hc_idx = (RetrofitPollutant::Hc.pollutant_index() - 1) as usize;
        // reduction = 0.5 * 0.5 = 0.25
        assert!((state.pollutant_reduction_fraction[hc_idx] - 0.25).abs() < 1e-6);
        // Other retrofit pollutants stay zeroed.
        for &p in &[
            RetrofitPollutant::Co,
            RetrofitPollutant::Nox,
            RetrofitPollutant::Pm,
        ] {
            let idx = (p.pollutant_index() - 1) as usize;
            assert_eq!(state.pollutant_reduction_fraction[idx], 0.0);
        }
    }

    #[test]
    fn episode_year_caps_retrofit_year_end() {
        // ry_end = 2030, but iepyr = 2012, so rycount = 2012 - 2010 + 1 = 3.
        let record = make_record(1, RetrofitPollutant::Co, 2010, 2030, 0.2, 1.0);
        let recs: Vec<&RetrofitRecord> = vec![&record];
        let mut state = init_retrofit_state();
        let outcome = calculate_retrofit_reduction(
            &recs,
            10.0,
            2012,
            &mut state.pollutant_reduction_fraction,
            &ctx_default(),
        )
        .unwrap();
        // 3 years × 0.2 = 0.6
        assert!((outcome.frac_retro - 0.6).abs() < 1e-6);
        assert!((outcome.units_retro - 6.0).abs() < 1e-4);
    }

    #[test]
    fn multiple_pollutants_one_retrofit_counts_frac_once() {
        // Same retrofit id=1 spans HC and NOX with the same annual
        // fraction per pollutant — the total fraction is counted once,
        // but reductions apply per pollutant.
        let r_hc = make_record(1, RetrofitPollutant::Hc, 2010, 2012, 0.1, 0.5);
        let r_nox = make_record(1, RetrofitPollutant::Nox, 2010, 2012, 0.1, 0.8);
        let recs: Vec<&RetrofitRecord> = vec![&r_hc, &r_nox];
        let mut state = init_retrofit_state();
        let outcome = calculate_retrofit_reduction(
            &recs,
            100.0,
            2020,
            &mut state.pollutant_reduction_fraction,
            &ctx_default(),
        )
        .unwrap();
        // rycount = 3, total fraction = 0.3 (counted once even though two pollutants)
        assert!((outcome.frac_retro - 0.3).abs() < 1e-6);
        assert!((outcome.units_retro - 30.0).abs() < 1e-4);

        let hc = (RetrofitPollutant::Hc.pollutant_index() - 1) as usize;
        let nox = (RetrofitPollutant::Nox.pollutant_index() - 1) as usize;
        // Reductions: HC = 0.3*0.5 = 0.15; NOX = 0.3*0.8 = 0.24
        assert!((state.pollutant_reduction_fraction[hc] - 0.15).abs() < 1e-6);
        assert!((state.pollutant_reduction_fraction[nox] - 0.24).abs() < 1e-6);
    }

    #[test]
    fn distinct_retrofit_ids_each_contribute_to_total_fraction() {
        let r1 = make_record(1, RetrofitPollutant::Hc, 2010, 2010, 0.2, 0.5);
        let r2 = make_record(2, RetrofitPollutant::Hc, 2010, 2010, 0.3, 0.5);
        let recs: Vec<&RetrofitRecord> = vec![&r1, &r2];
        let mut state = init_retrofit_state();
        let outcome = calculate_retrofit_reduction(
            &recs,
            100.0,
            2020,
            &mut state.pollutant_reduction_fraction,
            &ctx_default(),
        )
        .unwrap();
        // rycount=1 for each; total = 0.2 + 0.3 = 0.5
        assert!((outcome.frac_retro - 0.5).abs() < 1e-6);
        let hc = (RetrofitPollutant::Hc.pollutant_index() - 1) as usize;
        // Reduction sum: 0.2*0.5 + 0.3*0.5 = 0.25
        assert!((state.pollutant_reduction_fraction[hc] - 0.25).abs() < 1e-6);
    }

    #[test]
    fn n_units_converts_to_fraction_via_pop() {
        // annual_frac_or_count > 1 → count, not fraction.
        // tmpfrac = 25 / 100 = 0.25, rycount = 1.
        let record = make_record(1, RetrofitPollutant::Hc, 2010, 2010, 25.0, 1.0);
        let recs: Vec<&RetrofitRecord> = vec![&record];
        let mut state = init_retrofit_state();
        let outcome = calculate_retrofit_reduction(
            &recs,
            100.0,
            2020,
            &mut state.pollutant_reduction_fraction,
            &ctx_default(),
        )
        .unwrap();
        assert!((outcome.frac_retro - 0.25).abs() < 1e-6);
        assert!((outcome.units_retro - 25.0).abs() < 1e-4);
    }

    #[test]
    fn n_units_exceeds_pop_returns_error() {
        // 25 units / 10 engines → tmpfrac = 2.5, hasnunits set, > 1
        // triggers the 7000 error path.
        let record = make_record(42, RetrofitPollutant::Nox, 2010, 2010, 25.0, 1.0);
        let recs: Vec<&RetrofitRecord> = vec![&record];
        let mut state = init_retrofit_state();
        let err = calculate_retrofit_reduction(
            &recs,
            10.0,
            2020,
            &mut state.pollutant_reduction_fraction,
            &ctx_default(),
        )
        .unwrap_err();
        match err {
            Error::RetrofitNUnitsExceedPopulation {
                retrofit_id,
                pollutant,
                n_units_requested,
                n_units_existing,
                ..
            } => {
                assert_eq!(retrofit_id, 42);
                assert_eq!(pollutant, "NOX");
                assert!((n_units_requested - 25.0).abs() < 1e-4);
                assert!((n_units_existing - 10.0).abs() < 1e-4);
            }
            other => panic!("expected RetrofitNUnitsExceedPopulation, got {other:?}"),
        }
    }

    #[test]
    fn fraction_over_one_warns_and_clamps() {
        // Single-pollutant fraction > 1 (e.g. high annual fraction
        // times many retrofit years): 0.4 * 4 years = 1.6 → clamp to 1
        // for the per-pollutant reduction, and total frac_retro is
        // also clamped at 1.
        let record = make_record(1, RetrofitPollutant::Pm, 2010, 2013, 0.4, 0.5);
        let recs: Vec<&RetrofitRecord> = vec![&record];
        let mut state = init_retrofit_state();
        let outcome = calculate_retrofit_reduction(
            &recs,
            100.0,
            2020,
            &mut state.pollutant_reduction_fraction,
            &ctx_default(),
        )
        .unwrap();
        assert!((outcome.frac_retro - 1.0).abs() < 1e-6);
        assert!((outcome.units_retro - 100.0).abs() < 1e-4);

        assert_eq!(outcome.warnings.len(), 1);
        match &outcome.warnings[0] {
            RetrofitCalcWarning::FractionExceedsOne {
                retrofit_id,
                pollutant,
                frac_retro,
                ..
            } => {
                assert_eq!(*retrofit_id, 1);
                assert_eq!(*pollutant, RetrofitPollutant::Pm);
                assert!((frac_retro - 1.6).abs() < 1e-5);
            }
            other => panic!("expected FractionExceedsOne, got {other:?}"),
        }

        // Per-pollutant reduction: clamped fraction (1.0) * effect (0.5) = 0.5
        let pm = (RetrofitPollutant::Pm.pollutant_index() - 1) as usize;
        assert!((state.pollutant_reduction_fraction[pm] - 0.5).abs() < 1e-6);
    }

    #[test]
    fn reduction_over_one_warns_and_clamps() {
        // Two retrofits each contributing 0.5*1.0 = 0.5 reduction →
        // sum = 1.0 (exactly at limit, no warning). Push to 0.6 each
        // → sum = 1.2 → warning + clamp to 1.0.
        let r1 = make_record(1, RetrofitPollutant::Co, 2010, 2010, 0.6, 1.0);
        let r2 = make_record(2, RetrofitPollutant::Co, 2010, 2010, 0.6, 1.0);
        let recs: Vec<&RetrofitRecord> = vec![&r1, &r2];
        let mut state = init_retrofit_state();
        let outcome = calculate_retrofit_reduction(
            &recs,
            100.0,
            2020,
            &mut state.pollutant_reduction_fraction,
            &ctx_default(),
        )
        .unwrap();
        // Total frac_retro = 0.6 + 0.6 = 1.2 → clamp to 1
        assert!((outcome.frac_retro - 1.0).abs() < 1e-6);

        // ReductionFractionExceedsOne warning fired for CO.
        let reduction_warns: Vec<_> = outcome
            .warnings
            .iter()
            .filter(|w| matches!(w, RetrofitCalcWarning::ReductionFractionExceedsOne { .. }))
            .collect();
        assert_eq!(reduction_warns.len(), 1);
        match reduction_warns[0] {
            RetrofitCalcWarning::ReductionFractionExceedsOne { pollutant, .. } => {
                assert_eq!(*pollutant, RetrofitPollutant::Co);
            }
            _ => unreachable!(),
        }

        let co = (RetrofitPollutant::Co.pollutant_index() - 1) as usize;
        assert!((state.pollutant_reduction_fraction[co] - 1.0).abs() < 1e-6);
    }

    #[test]
    fn reduction_at_or_below_one_does_not_warn() {
        // 0.5 + 0.5 = 1.0 (exactly at limit, no clamp warning).
        let r1 = make_record(1, RetrofitPollutant::Co, 2010, 2010, 0.5, 1.0);
        let r2 = make_record(2, RetrofitPollutant::Co, 2010, 2010, 0.5, 1.0);
        let recs: Vec<&RetrofitRecord> = vec![&r1, &r2];
        let mut state = init_retrofit_state();
        let outcome = calculate_retrofit_reduction(
            &recs,
            100.0,
            2020,
            &mut state.pollutant_reduction_fraction,
            &ctx_default(),
        )
        .unwrap();
        assert!((outcome.frac_retro - 1.0).abs() < 1e-6);
        assert!(outcome.warnings.is_empty());
        let co = (RetrofitPollutant::Co.pollutant_index() - 1) as usize;
        assert!((state.pollutant_reduction_fraction[co] - 1.0).abs() < 1e-6);
    }

    #[test]
    fn record_order_stable_for_distinct_ids() {
        // Order: id=2 first, then id=1. Both should accumulate
        // independently; output should be deterministic regardless of
        // how the linear-search-insert behaves.
        let r2 = make_record(2, RetrofitPollutant::Hc, 2010, 2010, 0.2, 0.5);
        let r1 = make_record(1, RetrofitPollutant::Hc, 2010, 2010, 0.1, 1.0);
        let recs: Vec<&RetrofitRecord> = vec![&r2, &r1];
        let mut state = init_retrofit_state();
        let outcome = calculate_retrofit_reduction(
            &recs,
            100.0,
            2020,
            &mut state.pollutant_reduction_fraction,
            &ctx_default(),
        )
        .unwrap();
        // Total fraction = 0.2 + 0.1 = 0.3
        assert!((outcome.frac_retro - 0.3).abs() < 1e-6);
        // Reduction: 0.2*0.5 + 0.1*1.0 = 0.2
        let hc = (RetrofitPollutant::Hc.pollutant_index() - 1) as usize;
        assert!((state.pollutant_reduction_fraction[hc] - 0.2).abs() < 1e-6);
    }

    #[test]
    fn pm_slot_distinct_from_other_pollutants() {
        // PM lives at main pollutant index 6 (slot 5 in 0-based),
        // distinct from HC/CO/NOX at 0/1/2.
        let record = make_record(1, RetrofitPollutant::Pm, 2010, 2010, 0.1, 0.5);
        let recs: Vec<&RetrofitRecord> = vec![&record];
        let mut state = init_retrofit_state();
        let outcome = calculate_retrofit_reduction(
            &recs,
            100.0,
            2020,
            &mut state.pollutant_reduction_fraction,
            &ctx_default(),
        )
        .unwrap();
        assert!((outcome.frac_retro - 0.1).abs() < 1e-6);
        let pm = (RetrofitPollutant::Pm.pollutant_index() - 1) as usize;
        assert_eq!(pm, 5);
        assert!((state.pollutant_reduction_fraction[pm] - 0.05).abs() < 1e-6);
        for &p in &[
            RetrofitPollutant::Hc,
            RetrofitPollutant::Co,
            RetrofitPollutant::Nox,
        ] {
            let idx = (p.pollutant_index() - 1) as usize;
            assert_eq!(state.pollutant_reduction_fraction[idx], 0.0);
        }
    }

    #[test]
    fn unrelated_pollutant_slots_are_preserved() {
        // Pre-populate slots that are not in the four retrofit
        // pollutants and verify they survive the call. CO2 (IDXCO2=4)
        // is one such slot; main index 4 → 0-based 3.
        let record = make_record(1, RetrofitPollutant::Hc, 2010, 2010, 0.1, 0.5);
        let recs: Vec<&RetrofitRecord> = vec![&record];
        let mut state = init_retrofit_state();
        state.pollutant_reduction_fraction[3] = 0.99; // CO2 slot
        state.pollutant_reduction_fraction[7] = 0.42; // arbitrary
        let _ = calculate_retrofit_reduction(
            &recs,
            100.0,
            2020,
            &mut state.pollutant_reduction_fraction,
            &ctx_default(),
        )
        .unwrap();
        assert_eq!(state.pollutant_reduction_fraction[3], 0.99);
        assert_eq!(state.pollutant_reduction_fraction[7], 0.42);
    }

    #[test]
    fn warning_carries_iteration_context() {
        let record = make_record(99, RetrofitPollutant::Pm, 2010, 2015, 0.5, 0.5);
        let recs: Vec<&RetrofitRecord> = vec![&record];
        let mut state = init_retrofit_state();
        let ctx = RetrofitCalcContext {
            scc: "2270002099",
            hp_avg: 50.0,
            model_year: 2008,
            tech_type: "T1",
        };
        let outcome = calculate_retrofit_reduction(
            &recs,
            100.0,
            2020,
            &mut state.pollutant_reduction_fraction,
            &ctx,
        )
        .unwrap();
        assert_eq!(outcome.warnings.len(), 1);
        match &outcome.warnings[0] {
            RetrofitCalcWarning::FractionExceedsOne { context, .. } => {
                assert_eq!(context.scc, "2270002099");
                assert_eq!(context.hp_avg, 50.0);
                assert_eq!(context.model_year, 2008);
                assert_eq!(context.tech_type, "T1");
            }
            other => panic!("unexpected warning {other:?}"),
        }
    }

    #[test]
    fn invalid_pollutant_index_returns_config_error() {
        let mut record = make_record(1, RetrofitPollutant::Hc, 2010, 2010, 0.1, 0.5);
        record.pollutant_idx = 99; // out of the valid retrofit set
        let recs: Vec<&RetrofitRecord> = vec![&record];
        let mut state = init_retrofit_state();
        let err = calculate_retrofit_reduction(
            &recs,
            100.0,
            2020,
            &mut state.pollutant_reduction_fraction,
            &ctx_default(),
        )
        .unwrap_err();
        match err {
            Error::Config(msg) => assert!(msg.contains("pollutant_idx=99")),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn pollutant_reduction_fraction_array_sized_to_mxpol() {
        // Sanity check: the RetrofitState's accumulator is sized
        // MXPOL; PM lives at slot 5, well inside.
        let state = init_retrofit_state();
        assert_eq!(state.pollutant_reduction_fraction.len(), MXPOL);
        assert!(MXPOL > (RetrofitPollutant::Pm.pollutant_index() - 1) as usize);
    }
}

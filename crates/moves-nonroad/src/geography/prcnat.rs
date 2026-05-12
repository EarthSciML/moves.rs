//! National-level processing — `prcnat.f` (943 lines).
//!
//! Drives the nation-to-state allocation when called with a "national"
//! record (`idxsta <= 0`) and the direct per-state processing when
//! called with a "state" record. For each requested state the routine
//! threads:
//!
//! 1. Per-state population (`popsta(idx)`) — either allocated from the
//!    national total (via [`crate::allocation::allocate_state`]) or
//!    set directly from `popeqp(icurec)` when `idxsta > 0`.
//! 2. Per-state model-year × tech × exhaust + evap iterations,
//!    identical in structure to `prcus.f`.
//!
//! The Rust port shares the structured-output and callback-trait
//! design from [`super::prcus`]. The trait is renamed to
//! [`NationalCallbacks`] to keep the per-routine method names
//! self-documenting; the inner-iteration signatures are otherwise
//! identical.
//!
//! Compared to `prcus.f`:
//!
//! - `popus -> popsta(idx)` everywhere.
//! - State loop wraps everything from `daymthf` onward; lookups that
//!   take a FIPS code are now per-state (`statcd(idx)`).
//! - The per-state-loop body precomputes per-model-year evap data
//!   (rfmode, tank, tfull, …) before the state loop so it stays
//!   independent of state — see `prcnat.f` :363–:478.
//! - The model-year inner loop is essentially the same as `prcus.f`'s
//!   except for the state index in `popsta(idx)`.
//! - A "state has its own records" skip (`lstlev(idx)`) early-returns
//!   from the per-state body without emitting output (see
//!   `prcnat.f` :490–:492).
//! - The allocation step at the top (`alosta`) is skipped when
//!   `idxsta > 0`; `nstarc(idxsta)` is bumped instead and the state
//!   gets `grwsta = 1.0` (see `prcnat.f` :311–:315).
//!
//! Numerical-fidelity policy: same as [`super::prcus`] — all
//! arithmetic stays in `f32` and the order of operations preserves
//! the Fortran expression evaluation.

use super::prcus::{
    DayMonthFactor, EvapCallInputs, EvapResult, EvapTechLookup, ExhaustCallInputs, ExhaustResult,
    ExhaustTechLookup, ModelYearOutput, RetrofitResult,
};
use super::{
    blank_subcounty, fuel_density, hp_level_for_midpoint, missing_emissions,
    temporal_adjustment_for_unit, zero_emissions, ActivityLookup, ByModelYearOutput,
    EquipmentRecord, GeographyError, GeographyOutput, GeographyWarning, RunOptions, SiAggregate,
    StateDescriptor, StateOutput,
};
use crate::common::consts::{MXPOL, RMISS};
use crate::{Error, Result};

/// Output of one `alosta` step. Mirrors the
/// `(popsta(idxsta), grwsta(idxsta), used_flag)` tuple the Fortran
/// source materialises. Only populated when [`NationalCallbacks::allocate_to_states`]
/// is called with a national-record marker.
#[derive(Debug, Clone, PartialEq)]
pub struct StateAllocationOutcome {
    /// Per-state population allocations. Indexed parallel to the
    /// `StateDescriptor` slice supplied to the routine.
    pub populations: Vec<f32>,
    /// Per-state growth values. Same indexing.
    pub growth: Vec<f32>,
    /// `true` iff at least one state got non-zero population
    /// (Fortran `luse`); the Fortran source uses it to gate counting
    /// the allocation as "used."
    pub used: bool,
}

/// Callbacks the [`process_national_record`] routine uses for the
/// per-iteration lookups and computations.
///
/// Method names mirror the Fortran routines they replace; the
/// per-state lookups take an explicit FIPS argument.
pub trait NationalCallbacks {
    /// `fndasc(asccod, ascalo, nalorc)` — national-to-state
    /// allocation entry. `None` triggers
    /// [`GeographyError::AllocationNotFound`].
    fn find_allocation(&mut self, scc: &str) -> Option<()>;
    /// `alosta(..)` — perform the national-to-state allocation. The
    /// callback owns the allocation table; the result is returned as
    /// [`StateAllocationOutcome`].
    fn allocate_to_states(
        &mut self,
        scc: &str,
        states: &[StateDescriptor],
        national_population: f32,
        growth: f32,
    ) -> Result<StateAllocationOutcome>;
    /// `fndtch(scc, hp_avg, tech_year)`.
    fn find_exhaust_tech(&mut self, scc: &str, hp_avg: f32, year: i32)
        -> Option<ExhaustTechLookup>;
    /// `fndevtch(scc, hp_avg, tech_year)`.
    fn find_evap_tech(&mut self, scc: &str, hp_avg: f32, year: i32) -> Option<EvapTechLookup>;
    /// `fndgxf(state_fips, scc, hp_avg)`.
    fn find_growth_xref(&mut self, fips: &str, scc: &str, hp_avg: f32) -> Option<i32>;
    /// `fndact(scc, state_fips, hp_avg)`.
    fn find_activity(&mut self, scc: &str, fips: &str, hp_avg: f32) -> Option<ActivityLookup>;
    /// `daymthf(scc, state_fips)`.
    fn day_month_factor(&mut self, scc: &str, fips: &str) -> DayMonthFactor;
    /// `getgrw(indcod)`.
    fn load_growth(&mut self, _indcod: i32) -> Result<()> {
        Ok(())
    }
    /// `grwfac(year1, year2, fips, indcod)`.
    fn growth_factor(&mut self, year1: i32, year2: i32, fips: &str, indcod: i32) -> Result<f32>;
    /// `modyr(..)` — per-state initial age-distribution.
    fn model_year(
        &mut self,
        eq: &EquipmentRecord,
        activity: &ActivityLookup,
        growth_factor: f32,
    ) -> Result<ModelYearOutput>;
    /// `agedist(..)` — grow the age distribution + base population.
    #[allow(clippy::too_many_arguments)]
    fn age_distribution(
        &mut self,
        base_pop: f32,
        modfrc: &[f32],
        base_year: i32,
        growth_year: i32,
        yryrfrcscrp: &[f32],
        fips: &str,
        indcod: i32,
    ) -> Result<f32>;
    /// `fndrtrft(1, scc, hp_avg)`.
    fn filter_retrofits_by_scc_hp(&mut self, scc: &str, hp_avg: f32) -> Result<()>;
    /// `fndrtrft(2, year)`.
    fn filter_retrofits_by_year(&mut self, year: i32) -> Result<()>;
    /// `fndrtrft(3, tech)`.
    fn filter_retrofits_by_tech(&mut self, tech: &str) -> Result<()>;
    /// `clcrtrft(..)`.
    fn calculate_retrofit(
        &mut self,
        pop: f32,
        scc: &str,
        hp_avg: f32,
        model_year: i32,
        tech: &str,
    ) -> Result<RetrofitResult>;
    /// `clcems(..)` for exhaust emissions.
    fn calculate_exhaust(&mut self, inputs: &ExhaustCallInputs<'_>) -> Result<ExhaustResult>;
    /// `clcevems(..)` for evap emissions.
    fn calculate_evap(&mut self, inputs: &EvapCallInputs<'_>) -> Result<EvapResult>;
}

/// Per-call context bundle for [`process_national_record`]. Owns
/// the inputs that don't depend on per-state state.
#[derive(Debug, Clone)]
pub struct NationalContext<'a> {
    /// Per-record equipment data.
    pub equipment: EquipmentRecord,
    /// Run-time options.
    pub run_options: RunOptions,
    /// 10-character SCC for the current iteration.
    pub scc: &'a str,
    /// HP-level table (`hpclev`).
    pub hp_levels: &'a [f32],
    /// State descriptors (FIPS + `lstacd` + `lstlev` parallel).
    pub states: &'a [StateDescriptor],
    /// 1-based state index (`idxsta`). `0` or negative encodes
    /// "national record" — triggers the `alosta` allocation; positive
    /// encodes "state record" — only the given state is processed.
    pub state_index: i32,
    /// Pre-fetched growth value passed by the caller (`growth`
    /// argument in Fortran). `-9.0` means "not yet computed" and is
    /// passed through to the allocation callback.
    pub growth_hint: f32,
}

/// Process one national-level record. Ports `prcnat.f`.
///
/// Returns a [`GeographyOutput`] holding one [`StateOutput`] per
/// requested state. The vector is parallel to
/// [`NationalContext::states`].
///
/// Errors:
///
/// - [`GeographyError::AllocationNotFound`] when the SCC lacks
///   national-to-state allocation coefficients.
/// - [`GeographyError::GrowthFileMissing`] when the run requested
///   growth-dependent processing without loading `/GROWTH FILES/`.
/// - [`GeographyError::GrowthIndicatorNotFound`] when a state's
///   growth cross-reference lookup fails.
/// - any error propagated from a callback.
pub fn process_national_record(
    ctx: &NationalContext<'_>,
    callbacks: &mut dyn NationalCallbacks,
) -> Result<GeographyOutput> {
    let eq = &ctx.equipment;
    let opt = &ctx.run_options;
    let scc = ctx.scc;
    let subcur = blank_subcounty();

    let mut output = GeographyOutput {
        state_record_counts: vec![0; ctx.states.len()],
        ..Default::default()
    };

    // --- HP level (prcnat.f :252–:265) ---
    let hpmid = (eq.hp_range_min + eq.hp_range_max) / 2.0;
    let hplev = hp_level_for_midpoint(hpmid, ctx.hp_levels);
    let hpval = eq.hp_avg;

    let popus = eq.population;

    // --- zero-population: emit a zero record per selected state and
    //     return (prcnat.f :268–:280) ---
    if popus <= 0.0 {
        for state in ctx.states {
            if !state.selected {
                continue;
            }
            output.state_outputs.push(StateOutput {
                fips: state.fips.clone(),
                subcounty: subcur.clone(),
                scc: scc.to_string(),
                hp_level: hplev,
                population: 0.0,
                activity: 0.0,
                fuel_consumption: 0.0,
                load_factor: 0.0,
                hp_avg: 0.0,
                frac_retrofitted: 0.0,
                units_retrofitted: 0.0,
                emissions_day: zero_emissions(),
                missing: false,
            });
        }
        return Ok(output);
    }

    // --- per-state population + growth setup (prcnat.f :283–:315) ---
    // Either allocate from the national total via `alosta`, or assign
    // the full population to a single state.
    let mut popsta = vec![0.0_f32; ctx.states.len()];
    let mut grwsta = vec![1.0_f32; ctx.states.len()];

    if ctx.state_index <= 0 {
        // National record — call alosta to spread popus across states.
        if callbacks.find_allocation(scc).is_none() {
            return Err(Error::Config(
                GeographyError::AllocationNotFound {
                    scc: scc.to_string(),
                }
                .to_string(),
            ));
        }
        output.national_record_count = 1;
        let alloc = callbacks.allocate_to_states(scc, ctx.states, popus, ctx.growth_hint)?;
        if alloc.populations.len() != ctx.states.len() || alloc.growth.len() != ctx.states.len() {
            return Err(Error::Config(format!(
                "alosta callback returned {} populations / {} growth values \
                 for {} states",
                alloc.populations.len(),
                alloc.growth.len(),
                ctx.states.len(),
            )));
        }
        popsta = alloc.populations;
        grwsta = alloc.growth;
    } else {
        let idx = (ctx.state_index - 1) as usize;
        if idx >= ctx.states.len() {
            return Err(Error::Config(format!(
                "state_index {} out of bounds for {} states",
                ctx.state_index,
                ctx.states.len()
            )));
        }
        popsta[idx] = popus;
        grwsta[idx] = 1.0;
        output.state_record_counts[idx] += 1;
    }

    // --- tech sanity check at the run's tech_year (prcnat.f :320–:345) ---
    let tech_at_tech_year = callbacks.find_exhaust_tech(scc, hpval, opt.tech_year);
    if tech_at_tech_year.is_none() {
        output.warnings.push(GeographyWarning::MissingExhaustTech {
            scc: scc.to_string(),
            hp_avg: hpval,
            year: opt.tech_year,
        });
        return Ok(output);
    }
    let evtech_at_tech_year = callbacks.find_evap_tech(scc, hpval, opt.tech_year);
    if evtech_at_tech_year.is_none() {
        output.warnings.push(GeographyWarning::MissingEvapTech {
            scc: scc.to_string(),
            hp_avg: hpval,
            year: opt.tech_year,
        });
        return Ok(output);
    }

    // --- fuel density (prcnat.f :350–:358) ---
    let denful = fuel_density(opt.fuel);

    // --- growth-file packet required (prcnat.f :543 / :7003) ---
    if !opt.growth_loaded {
        return Err(Error::Config(GeographyError::GrowthFileMissing.to_string()));
    }

    // --- state loop (prcnat.f :482–:889) ---
    for (idx, state) in ctx.states.iter().enumerate() {
        // Skip if state not selected, or if doing national record
        // but state has its own records (prcnat.f :487–:492).
        if !state.selected {
            continue;
        }
        if ctx.state_index <= 0 && state.has_state_records {
            continue;
        }

        let fips = state.fips.as_str();
        let pop_state = popsta[idx];

        // --- zero state population — write zero record, continue
        //     (prcnat.f :497–:502) ---
        if pop_state <= 0.0 {
            output.state_outputs.push(StateOutput {
                fips: fips.to_string(),
                subcounty: subcur.clone(),
                scc: scc.to_string(),
                hp_level: hplev,
                population: 0.0,
                activity: 0.0,
                fuel_consumption: 0.0,
                load_factor: 0.0,
                hp_avg: 0.0,
                frac_retrofitted: 0.0,
                units_retrofitted: 0.0,
                emissions_day: zero_emissions(),
                missing: false,
            });
            continue;
        }

        let mut emsday = zero_emissions();

        // --- daymthf + adjtime/tplfac/tplful (prcnat.f :514–:534) ---
        let dmf = callbacks.day_month_factor(scc, fips);
        let ndays = dmf.n_days;
        let adjtime: f32 = if opt.total_mode {
            1.0
        } else if ndays > 0 {
            1.0 / ndays as f32
        } else {
            0.0
        };
        let tplfac: f32 = if opt.daily_output {
            dmf.dayf
        } else {
            dmf.mthf * dmf.dayf
        };
        let tplful: f32 = dmf.mthf * dmf.dayf;

        // --- growth Xref + activity (prcnat.f :544–:565) ---
        let Some(indcod) = callbacks.find_growth_xref(fips, scc, hpval) else {
            return Err(Error::Config(
                GeographyError::GrowthIndicatorNotFound {
                    fips: fips.to_string(),
                    scc: scc.to_string(),
                    hp_avg: hpval,
                    hp_min: eq.hp_range_min,
                    hp_max: eq.hp_range_max,
                }
                .to_string(),
            ));
        };
        let Some(activity) = callbacks.find_activity(scc, fips, hpval) else {
            output.warnings.push(GeographyWarning::MissingActivity {
                scc: scc.to_string(),
                fips: fips.to_string(),
                hp_min: eq.hp_range_min,
                hp_max: eq.hp_range_max,
            });
            output.state_outputs.push(StateOutput {
                fips: fips.to_string(),
                subcounty: subcur.clone(),
                scc: scc.to_string(),
                hp_level: hplev,
                population: RMISS,
                activity: RMISS,
                fuel_consumption: RMISS,
                load_factor: RMISS,
                hp_avg: RMISS,
                frac_retrofitted: RMISS,
                units_retrofitted: RMISS,
                emissions_day: missing_emissions(),
                missing: true,
            });
            // The Fortran source `goto 9999`s here, ending the entire
            // routine. The Rust port mirrors that — once a state hits
            // missing activity, the run aborts (early return).
            return Ok(output);
        };

        // --- load growth + base-year growth factor (prcnat.f :569–:581) ---
        callbacks.load_growth(indcod)?;
        let grwtmp = callbacks.growth_factor(eq.pop_year, eq.pop_year + 1, fips, indcod)?;
        // Track the per-state growth for downstream use.
        grwsta[idx] = grwtmp;

        // --- modyr -> initial age distribution (prcnat.f :586–:589) ---
        let my_out = callbacks.model_year(eq, &activity, grwsta[idx])?;
        let nyrlif = my_out.nyrlif;

        // --- agedist -> grown population (prcnat.f :595–:598) ---
        let pop_state = callbacks
            .age_distribution(
                pop_state,
                &my_out.modfrc,
                eq.pop_year,
                opt.growth_year,
                &my_out.yryrfrcscrp,
                fips,
                indcod,
            )?
            .max(0.0);

        // --- initialise totals (prcnat.f :603–:621) ---
        let mut poptot: f32 = 0.0;
        let mut acttot: f32 = 0.0;
        let mut strtot: f32 = 0.0;
        let mut fulcsm: f32 = 0.0;
        let mut fracretro: f32 = 0.0;
        let mut unitsretro: f32 = 0.0;
        let mut evpoptot: f32 = 0.0;
        let mut evacttot: f32 = 0.0;
        let mut evstrtot: f32 = 0.0;

        // --- filter retrofits by SCC + HP (prcnat.f :612–:615) ---
        if opt.retrofit_loaded {
            callbacks.filter_retrofits_by_scc_hp(scc, hpval)?;
        }

        // --- model-year loop (prcnat.f :625–:866) ---
        let iepyr = opt.episode_year;
        let lo = iepyr - (nyrlif as i32) + 1;
        let hi = iepyr;
        for iyr in lo..=hi {
            let idxyr_one_based = (iepyr - iyr + 1) as usize;
            let idxyr = idxyr_one_based - 1;

            let modfrc = my_out.modfrc.get(idxyr).copied().unwrap_or(0.0);
            if modfrc <= 0.0 {
                continue;
            }
            let actadj = my_out.actadj.get(idxyr).copied().unwrap_or(0.0);
            let stradj = my_out.stradj.get(idxyr).copied().unwrap_or(0.0);
            let detage = my_out.detage.get(idxyr).copied().unwrap_or(0.0);

            let tchmdyr = iyr.min(opt.tech_year);

            // --- exhaust tech for this model year (prcnat.f :377–:399 / :659) ---
            let Some(tech) = callbacks.find_exhaust_tech(scc, hpval, tchmdyr) else {
                output.warnings.push(GeographyWarning::MissingExhaustTech {
                    scc: scc.to_string(),
                    hp_avg: hpval,
                    year: tchmdyr,
                });
                return Ok(output);
            };

            // --- filter retrofits by model year (prcnat.f :650–:655) ---
            if opt.retrofit_loaded {
                callbacks.filter_retrofits_by_year(iyr)?;
            }

            let mut fulbmytot: f32 = 0.0;

            // --- exhaust tech-type loop (prcnat.f :659–:757) ---
            for (tech_i, tech_name) in tech.tech_names.iter().enumerate() {
                let tfrac = tech.fractions[tech_i];
                if tfrac <= 0.0 {
                    continue;
                }

                let popbmy = pop_state * modfrc * tfrac;

                let mut frac_retro_bmy = 0.0_f32;
                let mut units_retro_bmy = 0.0_f32;
                if opt.retrofit_loaded {
                    callbacks.filter_retrofits_by_tech(tech_name)?;
                    let r = callbacks.calculate_retrofit(popbmy, scc, hpval, iyr, tech_name)?;
                    frac_retro_bmy = r.frac_retro;
                    units_retro_bmy = r.units_retro;
                    unitsretro += units_retro_bmy;
                }

                let tpltmp = temporal_adjustment_for_unit(activity.units, tplfac);

                let er = callbacks.calculate_exhaust(&ExhaustCallInputs {
                    scc,
                    activity: &activity,
                    hp_avg: hpval,
                    fuel_density: denful,
                    year_index: idxyr,
                    tech_index: tech_i,
                    tech_name,
                    tech_fraction: tfrac,
                    population: pop_state,
                    model_year_fraction: modfrc,
                    activity_adjustment: actadj,
                    starts_adjustment: stradj,
                    deterioration_age: detage,
                    temporal_adjustment: tpltmp,
                    adjustment_time: adjtime,
                    n_days: ndays,
                })?;

                accumulate_emissions(&mut emsday, &er.ems_day_delta);

                let actbmy = actadj * pop_state * modfrc * tplful * tfrac * adjtime;
                let fulbmy = tplful
                    * pop_state
                    * actadj
                    * modfrc
                    * tfrac
                    * (hpval * activity.load_factor / denful.max(f32::MIN_POSITIVE))
                    * adjtime;

                fulcsm += fulbmy;
                fulbmytot += fulbmy;

                if opt.emit_bmy {
                    output.bmy_outputs.push(ByModelYearOutput {
                        fips: fips.to_string(),
                        subcounty: subcur.clone(),
                        scc: scc.to_string(),
                        hp_level: hplev,
                        tech_type: tech_name.clone(),
                        model_year: iyr,
                        population: popbmy,
                        emissions: er.ems_bmy.clone(),
                        fuel_consumption: fulbmy,
                        activity: actbmy,
                        load_factor: activity.load_factor,
                        hp_avg: hpval,
                        frac_retrofitted: frac_retro_bmy,
                        units_retrofitted: units_retro_bmy,
                        channel: 1,
                    });
                }
                if opt.emit_si {
                    output.si_aggregates.push(SiAggregate {
                        fips: fips.to_string(),
                        scc: scc.to_string(),
                        tech_type: tech_name.clone(),
                        population: popbmy,
                        activity: actbmy,
                        fuel_consumption: fulbmy,
                        emissions: er.ems_bmy,
                        channel: 1,
                    });
                }
            }

            // --- population / activity / starts totals (prcnat.f :761–:765) ---
            poptot += pop_state * modfrc;
            acttot += actadj * pop_state * modfrc * tplful * adjtime;
            strtot += stradj * pop_state * modfrc * tplful * adjtime;

            // --- evap tech for this model year (prcnat.f :412 / :774) ---
            let Some(evtech) = callbacks.find_evap_tech(scc, hpval, tchmdyr) else {
                output.warnings.push(GeographyWarning::MissingEvapTech {
                    scc: scc.to_string(),
                    hp_avg: hpval,
                    year: tchmdyr,
                });
                return Ok(output);
            };

            // --- evap tech-type loop (prcnat.f :774–:854) ---
            for (evtech_i, evtech_name) in evtech.tech_names.iter().enumerate() {
                let evfrac = evtech.fractions[evtech_i];
                if evfrac <= 0.0 {
                    continue;
                }
                let tpltmp = temporal_adjustment_for_unit(activity.units, tplfac);
                let fulbmy_evap = fulbmytot * evfrac;

                let er = callbacks.calculate_evap(&EvapCallInputs {
                    scc,
                    activity: &activity,
                    hp_avg: hpval,
                    year_index: idxyr,
                    evap_tech_index: evtech_i,
                    evap_tech_name: evtech_name,
                    evap_tech_fraction: evfrac,
                    population: pop_state,
                    model_year_fraction: modfrc,
                    activity_adjustment: actadj,
                    starts_adjustment: stradj,
                    deterioration_age: detage,
                    temporal_adjustment: tpltmp,
                    adjustment_time: adjtime,
                    n_days: ndays,
                    fips,
                    fuel_consumption: fulbmy_evap,
                })?;

                accumulate_emissions(&mut emsday, &er.ems_day_delta);

                let popbmy = pop_state * modfrc * evfrac;
                let actbmy = actadj * pop_state * modfrc * tplful * evfrac * adjtime;

                if opt.emit_bmy_evap {
                    output.bmy_outputs.push(ByModelYearOutput {
                        fips: fips.to_string(),
                        subcounty: subcur.clone(),
                        scc: scc.to_string(),
                        hp_level: hplev,
                        tech_type: evtech_name.clone(),
                        model_year: iyr,
                        population: popbmy,
                        emissions: er.ems_bmy.clone(),
                        fuel_consumption: fulbmy_evap,
                        activity: actbmy,
                        load_factor: RMISS,
                        hp_avg: RMISS,
                        frac_retrofitted: RMISS,
                        units_retrofitted: RMISS,
                        channel: 2,
                    });
                }
                if opt.emit_si {
                    output.si_aggregates.push(SiAggregate {
                        fips: fips.to_string(),
                        scc: scc.to_string(),
                        tech_type: evtech_name.clone(),
                        population: popbmy,
                        activity: actbmy,
                        fuel_consumption: fulbmy_evap,
                        emissions: er.ems_bmy,
                        channel: 2,
                    });
                }
            }

            evpoptot += pop_state * modfrc;
            evacttot += actadj * pop_state * modfrc * tplful * adjtime;
            evstrtot += stradj * pop_state * modfrc * tplful * adjtime;
        }

        if poptot > 0.0 {
            fracretro = unitsretro / poptot;
        }

        output.state_outputs.push(StateOutput {
            fips: fips.to_string(),
            subcounty: subcur.clone(),
            scc: scc.to_string(),
            hp_level: hplev,
            population: poptot,
            activity: acttot,
            fuel_consumption: fulcsm,
            load_factor: activity.load_factor,
            hp_avg: hpval,
            frac_retrofitted: fracretro,
            units_retrofitted: unitsretro,
            emissions_day: emsday,
            missing: false,
        });

        // Suppress unused warnings — these counters are reported in
        // the Fortran SI report (see `wrtsi.f`); deferred to Task 114.
        let _ = (strtot, evpoptot, evacttot, evstrtot);
    }

    Ok(output)
}

fn accumulate_emissions(target: &mut [f32], delta: &[f32]) {
    let n = target.len().min(delta.len()).min(MXPOL);
    for i in 0..n {
        target[i] += delta[i];
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::emissions::exhaust::{ActivityUnit, FuelKind};

    fn sample_options() -> RunOptions {
        RunOptions {
            episode_year: 2020,
            growth_year: 2020,
            tech_year: 2020,
            fuel: FuelKind::Diesel,
            total_mode: true,
            daily_output: false,
            emit_bmy: false,
            emit_bmy_evap: false,
            emit_si: false,
            growth_loaded: true,
            retrofit_loaded: false,
            spillage_loaded: false,
        }
    }

    fn sample_equipment(pop: f32) -> EquipmentRecord {
        EquipmentRecord {
            hp_range_min: 25.0,
            hp_range_max: 50.0,
            hp_avg: 37.0,
            population: pop,
            pop_year: 2020,
            use_hours: 250.0,
            discharge_code: 0,
            starts_hours: 0.0,
        }
    }

    fn sample_hp_levels() -> Vec<f32> {
        vec![11.0, 25.0, 50.0, 100.0, 175.0]
    }

    fn sample_states() -> Vec<StateDescriptor> {
        vec![
            StateDescriptor {
                fips: "06000".to_string(),
                selected: true,
                has_state_records: false,
            },
            StateDescriptor {
                fips: "17000".to_string(),
                selected: true,
                has_state_records: false,
            },
            // Unselected state — should be skipped.
            StateDescriptor {
                fips: "36000".to_string(),
                selected: false,
                has_state_records: false,
            },
        ]
    }

    /// Reusable happy-path stub. Each state runs through the
    /// model-year loop with a single (modfrc=1.0) year.
    struct HappyCallbacks {
        states_seen: Vec<String>,
        exhaust_calls: usize,
        evap_calls: usize,
    }
    impl HappyCallbacks {
        fn new() -> Self {
            Self {
                states_seen: Vec::new(),
                exhaust_calls: 0,
                evap_calls: 0,
            }
        }
    }
    impl NationalCallbacks for HappyCallbacks {
        fn find_allocation(&mut self, _scc: &str) -> Option<()> {
            Some(())
        }
        fn allocate_to_states(
            &mut self,
            _scc: &str,
            states: &[StateDescriptor],
            national_population: f32,
            growth: f32,
        ) -> Result<StateAllocationOutcome> {
            // Equal split across selected states.
            let selected: Vec<_> = states
                .iter()
                .enumerate()
                .filter(|(_, s)| s.selected)
                .collect();
            let per = if selected.is_empty() {
                0.0
            } else {
                national_population / selected.len() as f32
            };
            let mut populations = vec![0.0_f32; states.len()];
            let mut growth_v = vec![1.0_f32; states.len()];
            for (i, s) in states.iter().enumerate() {
                if s.selected {
                    populations[i] = per;
                    growth_v[i] = growth.max(1.0);
                }
            }
            Ok(StateAllocationOutcome {
                populations,
                growth: growth_v,
                used: true,
            })
        }
        fn find_exhaust_tech(
            &mut self,
            _scc: &str,
            _hp_avg: f32,
            _year: i32,
        ) -> Option<ExhaustTechLookup> {
            Some(ExhaustTechLookup {
                tech_names: vec!["BASE".to_string()],
                fractions: vec![1.0],
            })
        }
        fn find_evap_tech(
            &mut self,
            _scc: &str,
            _hp_avg: f32,
            _year: i32,
        ) -> Option<EvapTechLookup> {
            Some(EvapTechLookup {
                tech_names: vec!["EVAP_BASE".to_string()],
                fractions: vec![1.0],
            })
        }
        fn find_growth_xref(&mut self, _: &str, _: &str, _: f32) -> Option<i32> {
            Some(101)
        }
        fn find_activity(&mut self, _: &str, fips: &str, _: f32) -> Option<ActivityLookup> {
            self.states_seen.push(fips.to_string());
            Some(ActivityLookup {
                load_factor: 0.5,
                units: ActivityUnit::HoursPerYear,
                activity_level: 250.0,
                starts_value: 1.0,
                age_curve_id: "DEFAULT".to_string(),
            })
        }
        fn day_month_factor(&mut self, _: &str, _: &str) -> DayMonthFactor {
            DayMonthFactor {
                day_month_fac: vec![1.0; 365],
                mthf: 1.0,
                dayf: 1.0,
                n_days: 30,
            }
        }
        fn growth_factor(&mut self, _: i32, _: i32, _: &str, _: i32) -> Result<f32> {
            Ok(0.0)
        }
        fn model_year(
            &mut self,
            _: &EquipmentRecord,
            _: &ActivityLookup,
            _: f32,
        ) -> Result<ModelYearOutput> {
            let mut modfrc = vec![0.0_f32; 51];
            modfrc[0] = 1.0;
            Ok(ModelYearOutput {
                yryrfrcscrp: vec![0.0; 51],
                modfrc,
                stradj: vec![1.0; 1],
                actadj: vec![1.0; 1],
                detage: vec![0.0; 1],
                nyrlif: 1,
            })
        }
        fn age_distribution(
            &mut self,
            base_pop: f32,
            _: &[f32],
            _: i32,
            _: i32,
            _: &[f32],
            _: &str,
            _: i32,
        ) -> Result<f32> {
            Ok(base_pop)
        }
        fn filter_retrofits_by_scc_hp(&mut self, _: &str, _: f32) -> Result<()> {
            Ok(())
        }
        fn filter_retrofits_by_year(&mut self, _: i32) -> Result<()> {
            Ok(())
        }
        fn filter_retrofits_by_tech(&mut self, _: &str) -> Result<()> {
            Ok(())
        }
        fn calculate_retrofit(
            &mut self,
            _: f32,
            _: &str,
            _: f32,
            _: i32,
            _: &str,
        ) -> Result<RetrofitResult> {
            Ok(RetrofitResult::default())
        }
        fn calculate_exhaust(&mut self, _: &ExhaustCallInputs<'_>) -> Result<ExhaustResult> {
            self.exhaust_calls += 1;
            Ok(ExhaustResult {
                ems_day_delta: vec![0.0; MXPOL],
                ems_bmy: vec![0.0; MXPOL],
            })
        }
        fn calculate_evap(&mut self, _: &EvapCallInputs<'_>) -> Result<EvapResult> {
            self.evap_calls += 1;
            Ok(EvapResult {
                ems_day_delta: vec![0.0; MXPOL],
                ems_bmy: vec![0.0; MXPOL],
            })
        }
    }

    #[test]
    fn zero_population_emits_one_zero_record_per_selected_state() {
        let states = sample_states();
        let ctx = NationalContext {
            equipment: sample_equipment(0.0),
            run_options: sample_options(),
            scc: "2270002000",
            hp_levels: &sample_hp_levels(),
            states: &states,
            state_index: -1,
            growth_hint: -9.0,
        };
        let mut cb = HappyCallbacks::new();
        let out = process_national_record(&ctx, &mut cb).unwrap();
        // 2 selected states -> 2 outputs.
        assert_eq!(out.state_outputs.len(), 2);
        assert!(out.state_outputs.iter().all(|s| s.population == 0.0));
        assert!(out.state_outputs.iter().all(|s| !s.missing));
    }

    #[test]
    fn national_record_invokes_alosta_and_processes_each_selected_state() {
        let states = sample_states();
        let ctx = NationalContext {
            equipment: sample_equipment(2000.0),
            run_options: sample_options(),
            scc: "2270002000",
            hp_levels: &sample_hp_levels(),
            states: &states,
            state_index: -1, // national record
            growth_hint: -9.0,
        };
        let mut cb = HappyCallbacks::new();
        let out = process_national_record(&ctx, &mut cb).unwrap();
        assert_eq!(out.state_outputs.len(), 2);
        // Per-state pop = 2000/2 = 1000.
        assert!((out.state_outputs[0].population - 1000.0).abs() < 1e-3);
        assert!((out.state_outputs[1].population - 1000.0).abs() < 1e-3);
        // Two states × (1 exhaust + 1 evap) = 2 of each calculator call.
        assert_eq!(cb.exhaust_calls, 2);
        assert_eq!(cb.evap_calls, 2);
        // Unselected state ("36000") should never have been queried
        // for activity.
        assert!(!cb.states_seen.iter().any(|s| s == "36000"));
        assert_eq!(out.national_record_count, 1);
    }

    #[test]
    fn state_record_skips_allocation_and_only_processes_its_state() {
        let states = sample_states();
        let ctx = NationalContext {
            equipment: sample_equipment(2000.0),
            run_options: sample_options(),
            scc: "2270002000",
            hp_levels: &sample_hp_levels(),
            states: &states,
            state_index: 2, // process state #2 only
            growth_hint: -9.0,
        };
        let mut cb = HappyCallbacks::new();
        let out = process_national_record(&ctx, &mut cb).unwrap();
        // State #1 (0-based idx 0) has population 0 -> zero record;
        // state #2 (idx 1) has full population -> processed.
        assert_eq!(out.state_outputs.len(), 2);
        let s1 = out
            .state_outputs
            .iter()
            .find(|s| s.fips == "06000")
            .unwrap();
        let s2 = out
            .state_outputs
            .iter()
            .find(|s| s.fips == "17000")
            .unwrap();
        assert_eq!(s1.population, 0.0);
        assert!((s2.population - 2000.0).abs() < 1e-3);
        assert_eq!(out.national_record_count, 0);
        assert_eq!(out.state_record_counts[1], 1);
    }

    #[test]
    fn unselected_states_are_skipped_entirely() {
        let states = sample_states();
        let ctx = NationalContext {
            equipment: sample_equipment(1000.0),
            run_options: sample_options(),
            scc: "2270002000",
            hp_levels: &sample_hp_levels(),
            states: &states,
            state_index: -1,
            growth_hint: -9.0,
        };
        let mut cb = HappyCallbacks::new();
        let out = process_national_record(&ctx, &mut cb).unwrap();
        // Only 06000 and 17000 should be in the output.
        let fipses: Vec<&str> = out.state_outputs.iter().map(|s| s.fips.as_str()).collect();
        assert!(fipses.contains(&"06000"));
        assert!(fipses.contains(&"17000"));
        assert!(!fipses.contains(&"36000"));
    }

    #[test]
    fn missing_allocation_returns_error() {
        struct NoAlloc(HappyCallbacks);
        impl NationalCallbacks for NoAlloc {
            fn find_allocation(&mut self, _scc: &str) -> Option<()> {
                None
            }
            fn allocate_to_states(
                &mut self,
                scc: &str,
                states: &[StateDescriptor],
                pop: f32,
                growth: f32,
            ) -> Result<StateAllocationOutcome> {
                self.0.allocate_to_states(scc, states, pop, growth)
            }
            fn find_exhaust_tech(
                &mut self,
                scc: &str,
                hp_avg: f32,
                year: i32,
            ) -> Option<ExhaustTechLookup> {
                self.0.find_exhaust_tech(scc, hp_avg, year)
            }
            fn find_evap_tech(
                &mut self,
                scc: &str,
                hp_avg: f32,
                year: i32,
            ) -> Option<EvapTechLookup> {
                self.0.find_evap_tech(scc, hp_avg, year)
            }
            fn find_growth_xref(&mut self, f: &str, s: &str, h: f32) -> Option<i32> {
                self.0.find_growth_xref(f, s, h)
            }
            fn find_activity(&mut self, s: &str, f: &str, h: f32) -> Option<ActivityLookup> {
                self.0.find_activity(s, f, h)
            }
            fn day_month_factor(&mut self, s: &str, f: &str) -> DayMonthFactor {
                self.0.day_month_factor(s, f)
            }
            fn growth_factor(&mut self, a: i32, b: i32, c: &str, d: i32) -> Result<f32> {
                self.0.growth_factor(a, b, c, d)
            }
            fn model_year(
                &mut self,
                e: &EquipmentRecord,
                a: &ActivityLookup,
                g: f32,
            ) -> Result<ModelYearOutput> {
                self.0.model_year(e, a, g)
            }
            fn age_distribution(
                &mut self,
                a: f32,
                b: &[f32],
                c: i32,
                d: i32,
                e: &[f32],
                f: &str,
                g: i32,
            ) -> Result<f32> {
                self.0.age_distribution(a, b, c, d, e, f, g)
            }
            fn filter_retrofits_by_scc_hp(&mut self, s: &str, h: f32) -> Result<()> {
                self.0.filter_retrofits_by_scc_hp(s, h)
            }
            fn filter_retrofits_by_year(&mut self, y: i32) -> Result<()> {
                self.0.filter_retrofits_by_year(y)
            }
            fn filter_retrofits_by_tech(&mut self, t: &str) -> Result<()> {
                self.0.filter_retrofits_by_tech(t)
            }
            fn calculate_retrofit(
                &mut self,
                p: f32,
                s: &str,
                h: f32,
                y: i32,
                t: &str,
            ) -> Result<RetrofitResult> {
                self.0.calculate_retrofit(p, s, h, y, t)
            }
            fn calculate_exhaust(
                &mut self,
                inputs: &ExhaustCallInputs<'_>,
            ) -> Result<ExhaustResult> {
                self.0.calculate_exhaust(inputs)
            }
            fn calculate_evap(&mut self, inputs: &EvapCallInputs<'_>) -> Result<EvapResult> {
                self.0.calculate_evap(inputs)
            }
        }
        let states = sample_states();
        let ctx = NationalContext {
            equipment: sample_equipment(1000.0),
            run_options: sample_options(),
            scc: "2270002000",
            hp_levels: &sample_hp_levels(),
            states: &states,
            state_index: -1,
            growth_hint: -9.0,
        };
        let mut cb = NoAlloc(HappyCallbacks::new());
        let err = process_national_record(&ctx, &mut cb).unwrap_err();
        assert!(format!("{err}").contains("allocation"));
    }
}

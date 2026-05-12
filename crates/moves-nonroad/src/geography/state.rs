//! State-level processing — ports `prcsta.f` (1,034 lines) and
//! `prc1st.f` (785 lines).
//!
//! Two Fortran routines share the same per-state aggregate
//! computation but differ in how they emit it:
//!
//! | Source | Role | Top-level function | Output |
//! |---|---|---|---|
//! | `prcsta.f` | State-to-county allocation | [`process_state_to_county_record`] | One record per selected county in the state |
//! | `prc1st.f` | State-from-national derivation | [`process_state_from_national_record`] | One record per state |
//!
//! Both build identical state-level aggregates (population, activity,
//! fuel, retrofit, exhaust-/evap-emissions) and then either:
//!
//! - **`prcsta.f`**: loop over the state's counties and allocate
//!   `popcty(idxfip) / popsta` of every aggregate to each county;
//!   the per-(model-year, tech) by-model-year records are also
//!   scaled by that fraction. Retrofit-fraction is state-level and
//!   pass-through unchanged (matches Fortran `prcsta.f` :954).
//! - **`prc1st.f`**: emit the aggregate at the state FIPS directly.
//!
//! The shared backbone is factored into [`compute_state_aggregate`],
//! which mirrors the body of `prcus.f` against the state FIPS. The
//! two top-level functions then run that backbone and turn the
//! result into the variant-appropriate
//! [`super::StateOutput`] / [`super::ByModelYearOutput`] /
//! [`super::SiAggregate`] records.
//!
//! # Callbacks
//!
//! State processing uses the same per-iteration callback shape as
//! [`super::prcus::UsTotalCallbacks`] / [`super::prcnat::NationalCallbacks`].
//! The Rust port defines [`StateCallbacks`] with the same signature.
//! Implementations of the various traits typically share the
//! underlying lookup tables; the trait separation exists so that
//! per-routine doc comments name the Fortran behaviour exactly.

use super::prcus::{
    DayMonthFactor, EvapCallInputs, EvapResult, EvapTechLookup, ExhaustCallInputs, ExhaustResult,
    ExhaustTechLookup, ModelYearOutput, RetrofitResult,
};
use super::{
    blank_subcounty, fuel_density, hp_level_for_midpoint, missing_emissions,
    temporal_adjustment_for_unit, zero_emissions, ActivityLookup, ByModelYearOutput,
    EquipmentRecord, GeographyError, GeographyOutput, GeographyWarning, RunOptions, SiAggregate,
    StateOutput,
};
use crate::common::consts::{MXPOL, RMISS};
use crate::{Error, Result};

// =============================================================================
// Public types
// =============================================================================

/// One county descriptor used by [`process_state_to_county_record`].
/// Mirrors the Fortran `(fipcod(idxfip), lfipcd(idxfip),
/// popcty(idxfip))` triple read from `nonrdreg.inc` + `nonrdalo.inc`
/// after [`crate::allocation::allocate_county`] populated the per-county
/// populations.
#[derive(Debug, Clone)]
pub struct CountyInput {
    /// 5-character county FIPS code (`fipcod`).
    pub fips: String,
    /// `true` iff the county is in the run (`lfipcd`).
    pub selected: bool,
    /// County population from `alocty.f` (`popcty(idxfip)`).
    pub population: f32,
}

/// Per-call context bundle for [`process_state_to_county_record`]
/// and [`process_state_from_national_record`]. Owns the inputs that
/// don't depend on per-iteration state.
#[derive(Debug, Clone)]
pub struct StateContext<'a> {
    /// Per-record equipment data.
    pub equipment: EquipmentRecord,
    /// Run-time options.
    pub run_options: RunOptions,
    /// 10-character SCC for the current iteration.
    pub scc: &'a str,
    /// 5-character state FIPS code (`fipsta`).
    pub state_fips: &'a str,
    /// HP-level table (`hpclev`).
    pub hp_levels: &'a [f32],
}

/// Callbacks the state-processing routines use for the per-iteration
/// lookups and computations. Identical in shape to
/// [`super::prcus::UsTotalCallbacks`]; trait separation keeps the
/// per-routine Fortran provenance explicit.
pub trait StateCallbacks {
    /// `fndtch(scc, hp_avg, tech_year)`.
    fn find_exhaust_tech(&mut self, scc: &str, hp_avg: f32, year: i32)
        -> Option<ExhaustTechLookup>;
    /// `fndevtch(scc, hp_avg, tech_year)`.
    fn find_evap_tech(&mut self, scc: &str, hp_avg: f32, year: i32) -> Option<EvapTechLookup>;
    /// `fndgxf(fips, scc, hp_avg)`.
    fn find_growth_xref(&mut self, fips: &str, scc: &str, hp_avg: f32) -> Option<i32>;
    /// `fndact(scc, fips, hp_avg)`.
    fn find_activity(&mut self, scc: &str, fips: &str, hp_avg: f32) -> Option<ActivityLookup>;
    /// `daymthf(scc, fips)`.
    fn day_month_factor(&mut self, scc: &str, fips: &str) -> DayMonthFactor;
    /// `getgrw(indcod)`.
    fn load_growth(&mut self, _indcod: i32) -> Result<()> {
        Ok(())
    }
    /// `grwfac(year1, year2, fips, indcod)`.
    fn growth_factor(&mut self, year1: i32, year2: i32, fips: &str, indcod: i32) -> Result<f32>;
    /// `modyr(..)`.
    fn model_year(
        &mut self,
        eq: &EquipmentRecord,
        activity: &ActivityLookup,
        growth_factor: f32,
    ) -> Result<ModelYearOutput>;
    /// `agedist(..)`.
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
    /// `fndrtrft(filter_type=1, scc, hp)`.
    fn filter_retrofits_by_scc_hp(&mut self, scc: &str, hp_avg: f32) -> Result<()>;
    /// `fndrtrft(filter_type=2, year)`.
    fn filter_retrofits_by_year(&mut self, year: i32) -> Result<()>;
    /// `fndrtrft(filter_type=3, tech)`.
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
    /// `clcems(..)`.
    fn calculate_exhaust(&mut self, inputs: &ExhaustCallInputs<'_>) -> Result<ExhaustResult>;
    /// `clcevems(..)`.
    fn calculate_evap(&mut self, inputs: &EvapCallInputs<'_>) -> Result<EvapResult>;
}

// =============================================================================
// State-level aggregate
// =============================================================================

/// Outcome of [`compute_state_aggregate`].
///
/// The state-level computation can short-circuit on three documented
/// paths the Fortran source spells out; each is surfaced as a
/// variant rather than mutating an output buffer.
#[derive(Debug, Clone)]
pub enum StateAggregateOutcome {
    /// Full state-level computation succeeded.
    Ok(StateAggregate),
    /// State population was zero (`popeqp(icurec) <= 0`). Caller
    /// emits the zero-record output and returns.
    ZeroPopulation { hp_level: f32 },
    /// `fndtch` or `fndevtch` returned 0 at the tech-year probe.
    /// Caller adds a [`GeographyWarning::MissingExhaustTech`] /
    /// [`GeographyWarning::MissingEvapTech`] and bails.
    MissingTech {
        hp_level: f32,
        side: TechMissingSide,
    },
    /// `fndact` returned 0. Caller adds a
    /// [`GeographyWarning::MissingActivity`] and emits one
    /// `RMISS`-filled record per output unit.
    MissingActivity { hp_level: f32 },
}

/// Which side of the tech lookup failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TechMissingSide {
    /// `fndtch` returned 0.
    Exhaust,
    /// `fndevtch` returned 0.
    Evap,
}

/// Aggregated per-state result of the model-year loop.
///
/// The Fortran source uses these as the per-state state for the
/// final `wrtdat` call (`prc1st.f`) or the per-county allocation
/// loop (`prcsta.f`). The Rust port exposes them as data so the
/// county-allocation step can scale them by `popcty / popsta`.
#[derive(Debug, Clone)]
pub struct StateAggregate {
    /// HP-category level (`hplev`).
    pub hp_level: f32,
    /// State population after `agedist` (`popsta` post-growth).
    pub state_population: f32,
    /// Activity-record fields the writer needs (load_factor, hp_avg).
    pub activity: ActivityLookup,
    /// Per-pollutant daily emission totals (`emsday`). Length [`MXPOL`].
    pub emissions_day: Vec<f32>,
    /// Aggregate population (`poptot`).
    pub population_total: f32,
    /// Aggregate activity (`acttot`).
    pub activity_total: f32,
    /// Aggregate fuel consumption (`fulcsm`).
    pub fuel_consumption: f32,
    /// Aggregate fraction retrofitted (`fracretro`).
    pub fraction_retrofitted: f32,
    /// Aggregate units retrofitted (`unitsretro`).
    pub units_retrofitted: f32,
    /// Per-(model-year, exhaust-tech) BMY records collected during
    /// the loop. Empty when `RunOptions::emit_bmy` is false.
    pub exhaust_bmy: Vec<StateBmyCell>,
    /// Per-(model-year, evap-tech) BMY records. Empty when
    /// `RunOptions::emit_bmy_evap` is false.
    pub evap_bmy: Vec<StateBmyCell>,
    /// Per-(model-year, tech) SI accumulations. Empty when
    /// `RunOptions::emit_si` is false. The `channel` field of
    /// [`StateBmyCell`] distinguishes exhaust (1) vs evap (2).
    pub si_cells: Vec<StateBmyCell>,
}

/// One state-level by-model-year cell. Carries the values needed to
/// build a downstream [`super::ByModelYearOutput`] or
/// [`super::SiAggregate`] after county allocation.
#[derive(Debug, Clone, PartialEq)]
pub struct StateBmyCell {
    /// Absolute model year.
    pub model_year: i32,
    /// Technology code.
    pub tech_code: String,
    /// Population for this cell at the state level.
    pub population: f32,
    /// Activity for this cell at the state level.
    pub activity: f32,
    /// Fuel consumption for this cell at the state level.
    pub fuel_consumption: f32,
    /// Per-pollutant emissions for this cell at the state level.
    pub emissions: Vec<f32>,
    /// Fraction retrofitted for this cell (1 for exhaust; `RMISS`
    /// for evap — see the Fortran `wrtbmy` evap branch).
    pub frac_retrofitted: f32,
    /// Units retrofitted for this cell (or `RMISS` for evap).
    pub units_retrofitted: f32,
    /// 1 for exhaust, 2 for evap.
    pub channel: u8,
}

/// Run the shared per-state computation that backs both `prcsta.f`
/// and `prc1st.f`.
///
/// The Fortran source for both routines runs the same model-year ×
/// tech-type loop; only the post-loop step differs (one emits at
/// state level, the other allocates to counties). This function
/// captures the loop and surfaces the aggregates needed for either
/// downstream path.
///
/// The structure mirrors [`super::prcus::process_us_total_record`]
/// (`prcus.f`) with the FIPS swapped for `state_fips` and the
/// population taken directly from `equipment.population` (rather
/// than allocated from a parent).
pub fn compute_state_aggregate(
    ctx: &StateContext<'_>,
    callbacks: &mut dyn StateCallbacks,
) -> Result<StateAggregateOutcome> {
    let eq = &ctx.equipment;
    let opt = &ctx.run_options;
    let scc = ctx.scc;
    let fipsta = ctx.state_fips;
    let hpval = eq.hp_avg;

    // --- HP level (prcsta.f :273–:285 / prc1st.f :222–:234) ---
    let hpmid = (eq.hp_range_min + eq.hp_range_max) / 2.0;
    let hplev = hp_level_for_midpoint(hpmid, ctx.hp_levels);

    // --- zero-population early return (prcsta.f :300–:311 / prc1st.f :239–:246) ---
    if eq.population <= 0.0 {
        return Ok(StateAggregateOutcome::ZeroPopulation { hp_level: hplev });
    }

    // --- tech presence checks at tech_year (prcsta.f :335–:360 / prc1st.f :251–:278) ---
    if callbacks
        .find_exhaust_tech(scc, hpval, opt.tech_year)
        .is_none()
    {
        return Ok(StateAggregateOutcome::MissingTech {
            hp_level: hplev,
            side: TechMissingSide::Exhaust,
        });
    }
    if callbacks
        .find_evap_tech(scc, hpval, opt.tech_year)
        .is_none()
    {
        return Ok(StateAggregateOutcome::MissingTech {
            hp_level: hplev,
            side: TechMissingSide::Evap,
        });
    }

    // --- fuel density (prcsta.f :364–:373 / prc1st.f :282–:291) ---
    let denful = fuel_density(opt.fuel);

    // --- daymthf, tplfac, tplful, adjtime (prcsta.f :390–:405 / prc1st.f :303–:318) ---
    let dmf = callbacks.day_month_factor(scc, fipsta);
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

    // --- growth-file packet must be loaded (prcsta.f :419 / prc1st.f :332) ---
    if !opt.growth_loaded {
        return Err(Error::Config(GeographyError::GrowthFileMissing.to_string()));
    }

    // --- growth Xref / activity (prcsta.f :420–:444 / prc1st.f :333–:354) ---
    let Some(indcod) = callbacks.find_growth_xref(fipsta, scc, hpval) else {
        return Err(Error::Config(
            GeographyError::GrowthIndicatorNotFound {
                fips: fipsta.to_string(),
                scc: scc.to_string(),
                hp_avg: hpval,
                hp_min: eq.hp_range_min,
                hp_max: eq.hp_range_max,
            }
            .to_string(),
        ));
    };
    let Some(activity) = callbacks.find_activity(scc, fipsta, hpval) else {
        return Ok(StateAggregateOutcome::MissingActivity { hp_level: hplev });
    };

    // --- load growth + per-state growth factor (prcsta.f :449–:460 / prc1st.f :358–:369) ---
    callbacks.load_growth(indcod)?;
    let grwsta = callbacks.growth_factor(eq.pop_year, eq.pop_year + 1, fipsta, indcod)?;

    // --- modyr (prcsta.f :466–:469 / prc1st.f :374–:377) ---
    let my_out = callbacks.model_year(eq, &activity, grwsta)?;
    let nyrlif = my_out.nyrlif;

    // --- agedist (prcsta.f :475–:477 / prc1st.f :383–:385) ---
    let popsta = callbacks.age_distribution(
        eq.population,
        &my_out.modfrc,
        eq.pop_year,
        opt.growth_year,
        &my_out.yryrfrcscrp,
        fipsta,
        indcod,
    )?;
    let popsta = popsta.max(0.0);

    // --- accumulators (prcsta.f :381–:500 / prc1st.f :295–:408) ---
    let mut emsday = zero_emissions();
    let mut poptot: f32 = 0.0;
    let mut acttot: f32 = 0.0;
    let mut strtot: f32 = 0.0;
    let mut fulcsm: f32 = 0.0;
    let mut unitsretro: f32 = 0.0;
    let mut evpoptot: f32 = 0.0;
    let mut evacttot: f32 = 0.0;
    let mut evstrtot: f32 = 0.0;

    let mut exhaust_bmy: Vec<StateBmyCell> = Vec::new();
    let mut evap_bmy: Vec<StateBmyCell> = Vec::new();
    let mut si_cells: Vec<StateBmyCell> = Vec::new();

    // --- filter retrofits to (SCC, HP) (prcsta.f :491–:494 / prc1st.f :399–:402) ---
    if opt.retrofit_loaded {
        callbacks.filter_retrofits_by_scc_hp(scc, hpval)?;
    }

    // --- model-year loop (prcsta.f :504–:790 / prc1st.f :412–:719) ---
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

        // --- exhaust tech for this model year (prcsta.f :532 / prc1st.f :440) ---
        let Some(tech) = callbacks.find_exhaust_tech(scc, hpval, tchmdyr) else {
            // The Fortran source's outer `fndtch(itchyr)` already
            // succeeded; per-year miss is rare but possible. Match
            // prcus.f's symmetric warning + early-return.
            return Ok(StateAggregateOutcome::MissingTech {
                hp_level: hplev,
                side: TechMissingSide::Exhaust,
            });
        };

        // --- filter retrofits by model year (prcsta.f :549–:552 / prc1st.f :457–:460) ---
        if opt.retrofit_loaded {
            callbacks.filter_retrofits_by_year(iyr)?;
        }

        let mut fulbmytot: f32 = 0.0;

        // --- exhaust tech-type loop (prcsta.f :556–:639 / prc1st.f :464–:556) ---
        for (tech_i, tech_name) in tech.tech_names.iter().enumerate() {
            let tfrac = tech.fractions[tech_i];
            if tfrac <= 0.0 {
                continue;
            }

            let popbmy = popsta * modfrc * tfrac;

            // --- retrofit reduction (prcsta.f :583–:593 / prc1st.f :490–:498) ---
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

            // --- clcems (prcsta.f :607–:613 / prc1st.f :512–:518) ---
            let er = callbacks.calculate_exhaust(&ExhaustCallInputs {
                scc,
                activity: &activity,
                hp_avg: hpval,
                fuel_density: denful,
                year_index: idxyr,
                tech_index: tech_i,
                tech_name,
                tech_fraction: tfrac,
                population: popsta,
                model_year_fraction: modfrc,
                activity_adjustment: actadj,
                starts_adjustment: stradj,
                deterioration_age: detage,
                temporal_adjustment: tpltmp,
                adjustment_time: adjtime,
                n_days: ndays,
            })?;

            accumulate_emissions(&mut emsday, &er.ems_day_delta);

            // --- bookkeeping (prcsta.f :620–:635 / prc1st.f :522–:535) ---
            let actbmy = actadj * popsta * modfrc * tplful * tfrac * adjtime;
            let fulbmy = tplful
                * popsta
                * actadj
                * modfrc
                * tfrac
                * (hpval * activity.load_factor * 1.0 / denful.max(f32::MIN_POSITIVE))
                * adjtime;

            fulcsm += fulbmy;
            fulbmytot += fulbmy;

            // --- collect BMY / SI cells (prcsta.f :880–:894 / prc1st.f :539–:551) ---
            if opt.emit_bmy {
                exhaust_bmy.push(StateBmyCell {
                    model_year: iyr,
                    tech_code: tech_name.clone(),
                    population: popbmy,
                    activity: actbmy,
                    fuel_consumption: fulbmy,
                    emissions: er.ems_bmy.clone(),
                    frac_retrofitted: frac_retro_bmy,
                    units_retrofitted: units_retro_bmy,
                    channel: 1,
                });
            }
            if opt.emit_si {
                si_cells.push(StateBmyCell {
                    model_year: iyr,
                    tech_code: tech_name.clone(),
                    population: popbmy,
                    activity: actbmy,
                    fuel_consumption: fulbmy,
                    emissions: er.ems_bmy,
                    frac_retrofitted: frac_retro_bmy,
                    units_retrofitted: units_retro_bmy,
                    channel: 1,
                });
            }
        }

        // --- per-model-year exhaust totals (prcsta.f :643–:647 / prc1st.f :560–:564) ---
        poptot += popsta * modfrc;
        acttot += actadj * popsta * modfrc * tplful * adjtime;
        strtot += stradj * popsta * modfrc * tplful * adjtime;

        // --- evap tech for this tech-year-capped year (prcsta.f :657 / prc1st.f :574) ---
        let Some(evtech) = callbacks.find_evap_tech(scc, hpval, tchmdyr) else {
            return Ok(StateAggregateOutcome::MissingTech {
                hp_level: hplev,
                side: TechMissingSide::Evap,
            });
        };

        // --- evap tech-type loop (prcsta.f :722–:778 / prc1st.f :639–:707) ---
        for (evtech_i, evtech_name) in evtech.tech_names.iter().enumerate() {
            let evfrac = evtech.fractions[evtech_i];
            if evfrac <= 0.0 {
                continue;
            }

            let tpltmp = temporal_adjustment_for_unit(activity.units, tplfac);
            let fulbmy_evap = fulbmytot * evfrac;

            // --- clcevems (prcsta.f :751–:761 / prc1st.f :667–:677) ---
            let er = callbacks.calculate_evap(&EvapCallInputs {
                scc,
                activity: &activity,
                hp_avg: hpval,
                year_index: idxyr,
                evap_tech_index: evtech_i,
                evap_tech_name: evtech_name,
                evap_tech_fraction: evfrac,
                population: popsta,
                model_year_fraction: modfrc,
                activity_adjustment: actadj,
                starts_adjustment: stradj,
                deterioration_age: detage,
                temporal_adjustment: tpltmp,
                adjustment_time: adjtime,
                n_days: ndays,
                fips: fipsta,
                fuel_consumption: fulbmy_evap,
            })?;

            accumulate_emissions(&mut emsday, &er.ems_day_delta);

            let popbmy = popsta * modfrc * evfrac;
            let actbmy = actadj * popsta * modfrc * tplful * evfrac * adjtime;

            if opt.emit_bmy_evap {
                evap_bmy.push(StateBmyCell {
                    model_year: iyr,
                    tech_code: evtech_name.clone(),
                    population: popbmy,
                    activity: actbmy,
                    fuel_consumption: fulbmy_evap,
                    emissions: er.ems_bmy.clone(),
                    // RMISS for evap per prcsta.f :928 / prc1st.f :694.
                    frac_retrofitted: RMISS,
                    units_retrofitted: RMISS,
                    channel: 2,
                });
            }
            if opt.emit_si {
                si_cells.push(StateBmyCell {
                    model_year: iyr,
                    tech_code: evtech_name.clone(),
                    population: popbmy,
                    activity: actbmy,
                    fuel_consumption: fulbmy_evap,
                    emissions: er.ems_bmy,
                    frac_retrofitted: RMISS,
                    units_retrofitted: RMISS,
                    channel: 2,
                });
            }
        }

        // --- per-model-year evap totals (prcsta.f :782–:786 / prc1st.f :711–:715) ---
        evpoptot += popsta * modfrc;
        evacttot += actadj * popsta * modfrc * tplful * adjtime;
        evstrtot += stradj * popsta * modfrc * tplful * adjtime;
    }

    // --- fraction retrofitted (prcsta.f :803 / prc1st.f :732) ---
    let fracretro = if poptot > 0.0 {
        unitsretro / poptot
    } else {
        0.0
    };

    // Suppress unused warnings; the strtot / evpoptot / evacttot /
    // evstrtot counters mirror the Fortran tallies kept for the SI
    // report, but `wrtsi` (Task 114) consumes them separately.
    let _ = (grwsta, strtot, evpoptot, evacttot, evstrtot);

    Ok(StateAggregateOutcome::Ok(StateAggregate {
        hp_level: hplev,
        state_population: popsta,
        activity,
        emissions_day: emsday,
        population_total: poptot,
        activity_total: acttot,
        fuel_consumption: fulcsm,
        fraction_retrofitted: fracretro,
        units_retrofitted: unitsretro,
        exhaust_bmy,
        evap_bmy,
        si_cells,
    }))
}

fn accumulate_emissions(target: &mut [f32], delta: &[f32]) {
    let n = target.len().min(delta.len()).min(MXPOL);
    for i in 0..n {
        target[i] += delta[i];
    }
}

// =============================================================================
// prc1st.f — state-from-national (state-as-leaf) writer
// =============================================================================

/// Process one state record at the state level — `prc1st.f`
/// equivalent.
///
/// Builds the per-state aggregate via [`compute_state_aggregate`]
/// and emits one [`StateOutput`] at the state FIPS, plus any
/// `wrtbmy` / `sitot` records and warnings.
///
/// Errors:
///
/// - [`GeographyError::GrowthFileMissing`] — when
///   [`RunOptions::growth_loaded`] is false.
/// - [`GeographyError::GrowthIndicatorNotFound`] — when no growth
///   cross-reference matches.
/// - any error propagated from a callback.
pub fn process_state_from_national_record(
    ctx: &StateContext<'_>,
    callbacks: &mut dyn StateCallbacks,
) -> Result<GeographyOutput> {
    let mut output = GeographyOutput::default();
    let subcur = blank_subcounty();
    let opt = &ctx.run_options;
    let eq = &ctx.equipment;
    let scc = ctx.scc;
    let fipsta = ctx.state_fips;

    let outcome = compute_state_aggregate(ctx, callbacks)?;

    match outcome {
        StateAggregateOutcome::ZeroPopulation { hp_level } => {
            output.state_outputs.push(StateOutput {
                fips: fipsta.to_string(),
                subcounty: subcur,
                scc: scc.to_string(),
                hp_level,
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
        StateAggregateOutcome::MissingTech { hp_level, side } => {
            let _ = hp_level;
            output.warnings.push(match side {
                TechMissingSide::Exhaust => GeographyWarning::MissingExhaustTech {
                    scc: scc.to_string(),
                    hp_avg: eq.hp_avg,
                    year: opt.tech_year,
                },
                TechMissingSide::Evap => GeographyWarning::MissingEvapTech {
                    scc: scc.to_string(),
                    hp_avg: eq.hp_avg,
                    year: opt.tech_year,
                },
            });
        }
        StateAggregateOutcome::MissingActivity { hp_level } => {
            output.warnings.push(GeographyWarning::MissingActivity {
                scc: scc.to_string(),
                fips: fipsta.to_string(),
                hp_min: eq.hp_range_min,
                hp_max: eq.hp_range_max,
            });
            output.state_outputs.push(StateOutput {
                fips: fipsta.to_string(),
                subcounty: subcur,
                scc: scc.to_string(),
                hp_level,
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
        }
        StateAggregateOutcome::Ok(agg) => {
            // BMY exhaust + evap emit (prc1st.f :539–:545, :688–:694).
            for cell in &agg.exhaust_bmy {
                output.bmy_outputs.push(ByModelYearOutput {
                    fips: fipsta.to_string(),
                    subcounty: subcur.clone(),
                    scc: scc.to_string(),
                    hp_level: agg.hp_level,
                    tech_type: cell.tech_code.clone(),
                    model_year: cell.model_year,
                    population: cell.population,
                    emissions: cell.emissions.clone(),
                    fuel_consumption: cell.fuel_consumption,
                    activity: cell.activity,
                    load_factor: agg.activity.load_factor,
                    hp_avg: eq.hp_avg,
                    frac_retrofitted: cell.frac_retrofitted,
                    units_retrofitted: cell.units_retrofitted,
                    channel: 1,
                });
            }
            for cell in &agg.evap_bmy {
                output.bmy_outputs.push(ByModelYearOutput {
                    fips: fipsta.to_string(),
                    subcounty: subcur.clone(),
                    scc: scc.to_string(),
                    hp_level: agg.hp_level,
                    tech_type: cell.tech_code.clone(),
                    model_year: cell.model_year,
                    population: cell.population,
                    emissions: cell.emissions.clone(),
                    fuel_consumption: cell.fuel_consumption,
                    activity: cell.activity,
                    load_factor: cell.frac_retrofitted, // RMISS pass-through
                    hp_avg: cell.units_retrofitted,     // RMISS pass-through
                    frac_retrofitted: cell.frac_retrofitted,
                    units_retrofitted: cell.units_retrofitted,
                    channel: 2,
                });
            }
            for cell in &agg.si_cells {
                output.si_aggregates.push(SiAggregate {
                    fips: fipsta.to_string(),
                    scc: scc.to_string(),
                    tech_type: cell.tech_code.clone(),
                    population: cell.population,
                    activity: cell.activity,
                    fuel_consumption: cell.fuel_consumption,
                    emissions: cell.emissions.clone(),
                    channel: cell.channel,
                });
            }
            // Final wrtdat (prc1st.f :736–:737).
            output.state_outputs.push(StateOutput {
                fips: fipsta.to_string(),
                subcounty: subcur,
                scc: scc.to_string(),
                hp_level: agg.hp_level,
                population: agg.population_total,
                activity: agg.activity_total,
                fuel_consumption: agg.fuel_consumption,
                load_factor: agg.activity.load_factor,
                hp_avg: eq.hp_avg,
                frac_retrofitted: agg.fraction_retrofitted,
                units_retrofitted: agg.units_retrofitted,
                emissions_day: agg.emissions_day,
                missing: false,
            });
        }
    }

    Ok(output)
}

// =============================================================================
// prcsta.f — state-to-county allocation writer
// =============================================================================

/// Process one state record with state-to-county allocation —
/// `prcsta.f` equivalent.
///
/// Builds the per-state aggregate via [`compute_state_aggregate`]
/// and then allocates it to the supplied counties by
/// `popcty(idxfip) / popsta`. Mirrors the Fortran `do idxfip = ibegj,
/// iendj` loop at `prcsta.f` :811–:980. Skipped counties are dropped
/// (no record emitted); zero-population counties get a zero record;
/// non-zero counties receive every aggregate scaled by `popctyfrac`.
///
/// Retrofit fraction is state-level and copied through unchanged
/// (Fortran `prcsta.f` :954).
pub fn process_state_to_county_record(
    ctx: &StateContext<'_>,
    counties: &[CountyInput],
    callbacks: &mut dyn StateCallbacks,
) -> Result<GeographyOutput> {
    let mut output = GeographyOutput::default();
    let subcur = blank_subcounty();
    let opt = &ctx.run_options;
    let eq = &ctx.equipment;
    let scc = ctx.scc;

    let outcome = compute_state_aggregate(ctx, callbacks)?;

    match outcome {
        StateAggregateOutcome::ZeroPopulation { hp_level } => {
            // Emit a zero record for each selected county
            // (prcsta.f :300–:311).
            for county in counties.iter().filter(|c| c.selected) {
                output.state_outputs.push(StateOutput {
                    fips: county.fips.clone(),
                    subcounty: subcur.clone(),
                    scc: scc.to_string(),
                    hp_level,
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
        }
        StateAggregateOutcome::MissingTech { hp_level, side } => {
            let _ = hp_level;
            output.warnings.push(match side {
                TechMissingSide::Exhaust => GeographyWarning::MissingExhaustTech {
                    scc: scc.to_string(),
                    hp_avg: eq.hp_avg,
                    year: opt.tech_year,
                },
                TechMissingSide::Evap => GeographyWarning::MissingEvapTech {
                    scc: scc.to_string(),
                    hp_avg: eq.hp_avg,
                    year: opt.tech_year,
                },
            });
        }
        StateAggregateOutcome::MissingActivity { hp_level } => {
            output.warnings.push(GeographyWarning::MissingActivity {
                scc: scc.to_string(),
                fips: ctx.state_fips.to_string(),
                hp_min: eq.hp_range_min,
                hp_max: eq.hp_range_max,
            });
            // Emit one RMISS-filled record per selected county
            // (prcsta.f :430–:444).
            for county in counties.iter().filter(|c| c.selected) {
                output.state_outputs.push(StateOutput {
                    fips: county.fips.clone(),
                    subcounty: subcur.clone(),
                    scc: scc.to_string(),
                    hp_level,
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
            }
        }
        StateAggregateOutcome::Ok(agg) => {
            // Loop over counties allocating proportionally
            // (prcsta.f :811–:980).
            for county in counties {
                if !county.selected {
                    continue;
                }
                if county.population <= 0.0 {
                    // Zero-pop county — emit zero record but no BMY.
                    output.state_outputs.push(StateOutput {
                        fips: county.fips.clone(),
                        subcounty: subcur.clone(),
                        scc: scc.to_string(),
                        hp_level: agg.hp_level,
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

                let popctyfrac = if agg.state_population > 0.0 {
                    county.population / agg.state_population
                } else {
                    0.0
                };

                // BMY exhaust + evap records, scaled per county.
                for cell in &agg.exhaust_bmy {
                    let mut emissions = Vec::with_capacity(MXPOL);
                    for e in cell.emissions.iter().take(MXPOL) {
                        emissions.push(*e * popctyfrac);
                    }
                    output.bmy_outputs.push(ByModelYearOutput {
                        fips: county.fips.clone(),
                        subcounty: subcur.clone(),
                        scc: scc.to_string(),
                        hp_level: agg.hp_level,
                        tech_type: cell.tech_code.clone(),
                        model_year: cell.model_year,
                        population: cell.population * popctyfrac,
                        emissions,
                        fuel_consumption: cell.fuel_consumption * popctyfrac,
                        activity: cell.activity * popctyfrac,
                        load_factor: agg.activity.load_factor,
                        hp_avg: eq.hp_avg,
                        // Retrofit fraction is state-level (unscaled).
                        frac_retrofitted: cell.frac_retrofitted,
                        units_retrofitted: cell.units_retrofitted * popctyfrac,
                        channel: 1,
                    });
                }
                for cell in &agg.evap_bmy {
                    let mut emissions = Vec::with_capacity(MXPOL);
                    for e in cell.emissions.iter().take(MXPOL) {
                        emissions.push(*e * popctyfrac);
                    }
                    output.bmy_outputs.push(ByModelYearOutput {
                        fips: county.fips.clone(),
                        subcounty: subcur.clone(),
                        scc: scc.to_string(),
                        hp_level: agg.hp_level,
                        tech_type: cell.tech_code.clone(),
                        model_year: cell.model_year,
                        population: cell.population * popctyfrac,
                        emissions,
                        fuel_consumption: cell.fuel_consumption * popctyfrac,
                        activity: cell.activity * popctyfrac,
                        load_factor: cell.frac_retrofitted, // RMISS pass-through
                        hp_avg: cell.units_retrofitted,     // RMISS pass-through
                        frac_retrofitted: cell.frac_retrofitted,
                        units_retrofitted: cell.units_retrofitted,
                        channel: 2,
                    });
                }
                for cell in &agg.si_cells {
                    let mut emissions = Vec::with_capacity(MXPOL);
                    for e in cell.emissions.iter().take(MXPOL) {
                        emissions.push(*e * popctyfrac);
                    }
                    output.si_aggregates.push(SiAggregate {
                        fips: county.fips.clone(),
                        scc: scc.to_string(),
                        tech_type: cell.tech_code.clone(),
                        population: cell.population * popctyfrac,
                        activity: cell.activity * popctyfrac,
                        fuel_consumption: cell.fuel_consumption * popctyfrac,
                        emissions,
                        channel: cell.channel,
                    });
                }

                // Allocated state-level record for this county
                // (prcsta.f :962–:964).
                let mut emsday_cty = Vec::with_capacity(MXPOL);
                for e in agg.emissions_day.iter().take(MXPOL) {
                    emsday_cty.push(*e * popctyfrac);
                }
                output.state_outputs.push(StateOutput {
                    fips: county.fips.clone(),
                    subcounty: subcur.clone(),
                    scc: scc.to_string(),
                    hp_level: agg.hp_level,
                    population: agg.population_total * popctyfrac,
                    activity: agg.activity_total * popctyfrac,
                    fuel_consumption: agg.fuel_consumption * popctyfrac,
                    load_factor: agg.activity.load_factor,
                    hp_avg: eq.hp_avg,
                    // Retrofit fraction same for state and county.
                    frac_retrofitted: agg.fraction_retrofitted,
                    units_retrofitted: agg.units_retrofitted * popctyfrac,
                    emissions_day: emsday_cty,
                    missing: false,
                });
            }
        }
    }

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::emissions::exhaust::{ActivityUnit, FuelKind};

    fn default_options() -> RunOptions {
        RunOptions {
            episode_year: 2020,
            growth_year: 2020,
            tech_year: 2020,
            fuel: FuelKind::Gasoline4Stroke,
            total_mode: true,
            daily_output: false,
            emit_bmy: true,
            emit_bmy_evap: true,
            emit_si: true,
            growth_loaded: true,
            retrofit_loaded: false,
            spillage_loaded: false,
        }
    }

    fn default_equipment() -> EquipmentRecord {
        EquipmentRecord {
            hp_range_min: 10.0,
            hp_range_max: 20.0,
            hp_avg: 15.0,
            population: 1000.0,
            pop_year: 2010,
            use_hours: 100.0,
            discharge_code: 0,
            starts_hours: 1.0,
        }
    }

    fn default_activity() -> ActivityLookup {
        ActivityLookup {
            load_factor: 0.5,
            units: ActivityUnit::HoursPerYear,
            activity_level: 100.0,
            starts_value: 1.0,
            age_curve_id: "DEFAULT".to_string(),
        }
    }

    // ---------- Stub callbacks ----------

    /// A simple stub `StateCallbacks` impl. Configure fields to
    /// drive different test paths.
    struct StubCallbacks {
        exhaust_tech: Option<ExhaustTechLookup>,
        evap_tech: Option<EvapTechLookup>,
        growth_xref: Option<i32>,
        activity: Option<ActivityLookup>,
        growth_factor_value: f32,
        nyrlif: usize,
        modfrc: Vec<f32>,
        actadj: Vec<f32>,
        stradj: Vec<f32>,
        detage: Vec<f32>,
        yryrfrcscrp: Vec<f32>,
        exhaust_calls: std::cell::Cell<i32>,
        evap_calls: std::cell::Cell<i32>,
        /// THC emission delta produced by each exhaust call.
        exhaust_thc: f32,
        /// Diurnal emission delta per evap call.
        evap_diurnal: f32,
    }

    impl StubCallbacks {
        fn ok_single_year() -> Self {
            let mut modfrc = vec![0.0; 51];
            modfrc[0] = 1.0;
            Self {
                exhaust_tech: Some(ExhaustTechLookup {
                    tech_names: vec!["T0".to_string()],
                    fractions: vec![1.0],
                }),
                evap_tech: Some(EvapTechLookup {
                    tech_names: vec!["E0".to_string()],
                    fractions: vec![1.0],
                }),
                growth_xref: Some(42),
                activity: Some(default_activity()),
                growth_factor_value: 0.0,
                nyrlif: 1,
                modfrc,
                actadj: vec![1.0; 51],
                stradj: vec![1.0; 51],
                detage: vec![0.0; 51],
                yryrfrcscrp: vec![0.0; 51],
                exhaust_calls: std::cell::Cell::new(0),
                evap_calls: std::cell::Cell::new(0),
                exhaust_thc: 100.0,
                evap_diurnal: 50.0,
            }
        }
    }

    impl StateCallbacks for StubCallbacks {
        fn find_exhaust_tech(
            &mut self,
            _scc: &str,
            _hp_avg: f32,
            _year: i32,
        ) -> Option<ExhaustTechLookup> {
            self.exhaust_tech.clone()
        }
        fn find_evap_tech(
            &mut self,
            _scc: &str,
            _hp_avg: f32,
            _year: i32,
        ) -> Option<EvapTechLookup> {
            self.evap_tech.clone()
        }
        fn find_growth_xref(&mut self, _fips: &str, _scc: &str, _hp_avg: f32) -> Option<i32> {
            self.growth_xref
        }
        fn find_activity(
            &mut self,
            _scc: &str,
            _fips: &str,
            _hp_avg: f32,
        ) -> Option<ActivityLookup> {
            self.activity.clone()
        }
        fn day_month_factor(&mut self, _scc: &str, _fips: &str) -> DayMonthFactor {
            DayMonthFactor {
                day_month_fac: vec![1.0; 365],
                mthf: 1.0,
                dayf: 1.0,
                n_days: 365,
            }
        }
        fn growth_factor(&mut self, _y1: i32, _y2: i32, _fips: &str, _indcod: i32) -> Result<f32> {
            Ok(self.growth_factor_value)
        }
        fn model_year(
            &mut self,
            _eq: &EquipmentRecord,
            _activity: &ActivityLookup,
            _growth_factor: f32,
        ) -> Result<ModelYearOutput> {
            Ok(ModelYearOutput {
                yryrfrcscrp: self.yryrfrcscrp.clone(),
                modfrc: self.modfrc.clone(),
                stradj: self.stradj.clone(),
                actadj: self.actadj.clone(),
                detage: self.detage.clone(),
                nyrlif: self.nyrlif,
            })
        }
        fn age_distribution(
            &mut self,
            base_pop: f32,
            _modfrc: &[f32],
            _base_year: i32,
            _growth_year: i32,
            _yryrfrcscrp: &[f32],
            _fips: &str,
            _indcod: i32,
        ) -> Result<f32> {
            Ok(base_pop)
        }
        fn filter_retrofits_by_scc_hp(&mut self, _scc: &str, _hp: f32) -> Result<()> {
            Ok(())
        }
        fn filter_retrofits_by_year(&mut self, _year: i32) -> Result<()> {
            Ok(())
        }
        fn filter_retrofits_by_tech(&mut self, _tech: &str) -> Result<()> {
            Ok(())
        }
        fn calculate_retrofit(
            &mut self,
            _pop: f32,
            _scc: &str,
            _hp: f32,
            _model_year: i32,
            _tech: &str,
        ) -> Result<RetrofitResult> {
            Ok(RetrofitResult::default())
        }
        fn calculate_exhaust(&mut self, _inputs: &ExhaustCallInputs<'_>) -> Result<ExhaustResult> {
            self.exhaust_calls.set(self.exhaust_calls.get() + 1);
            let mut emsday = vec![0.0; MXPOL];
            let mut emsbmy = vec![0.0; MXPOL];
            emsday[0] = self.exhaust_thc;
            emsbmy[0] = self.exhaust_thc;
            Ok(ExhaustResult {
                ems_day_delta: emsday,
                ems_bmy: emsbmy,
            })
        }
        fn calculate_evap(&mut self, _inputs: &EvapCallInputs<'_>) -> Result<EvapResult> {
            self.evap_calls.set(self.evap_calls.get() + 1);
            let mut emsday = vec![0.0; MXPOL];
            let mut emsbmy = vec![0.0; MXPOL];
            emsday[7] = self.evap_diurnal; // diurnal slot
            emsbmy[7] = self.evap_diurnal;
            Ok(EvapResult {
                ems_day_delta: emsday,
                ems_bmy: emsbmy,
            })
        }
    }

    fn default_ctx<'a>(state_fips: &'a str, hp_levels: &'a [f32]) -> StateContext<'a> {
        StateContext {
            equipment: default_equipment(),
            run_options: default_options(),
            scc: "2270000000",
            state_fips,
            hp_levels,
        }
    }

    // ---------- prc1st.f path ----------

    #[test]
    fn prc1st_zero_population_emits_zero_record() {
        let hp = [1.0, 11.0, 25.0, 50.0];
        let mut ctx = default_ctx("06000", &hp);
        ctx.equipment.population = 0.0;
        let mut cb = StubCallbacks::ok_single_year();
        let out = process_state_from_national_record(&ctx, &mut cb).unwrap();
        assert_eq!(out.state_outputs.len(), 1);
        assert_eq!(out.state_outputs[0].fips, "06000");
        assert_eq!(out.state_outputs[0].population, 0.0);
        assert!(!out.state_outputs[0].missing);
    }

    #[test]
    fn prc1st_missing_exhaust_tech_warns() {
        let hp = [1.0, 11.0, 25.0, 50.0];
        let ctx = default_ctx("06000", &hp);
        let mut cb = StubCallbacks::ok_single_year();
        cb.exhaust_tech = None;
        let out = process_state_from_national_record(&ctx, &mut cb).unwrap();
        assert!(matches!(
            out.warnings[0],
            GeographyWarning::MissingExhaustTech { .. }
        ));
        assert!(out.state_outputs.is_empty());
    }

    #[test]
    fn prc1st_missing_evap_tech_warns() {
        let hp = [1.0, 11.0, 25.0, 50.0];
        let ctx = default_ctx("06000", &hp);
        let mut cb = StubCallbacks::ok_single_year();
        cb.evap_tech = None;
        let out = process_state_from_national_record(&ctx, &mut cb).unwrap();
        assert!(matches!(
            out.warnings[0],
            GeographyWarning::MissingEvapTech { .. }
        ));
    }

    #[test]
    fn prc1st_missing_growth_xref_errors() {
        let hp = [1.0, 11.0, 25.0, 50.0];
        let ctx = default_ctx("06000", &hp);
        let mut cb = StubCallbacks::ok_single_year();
        cb.growth_xref = None;
        let err = process_state_from_national_record(&ctx, &mut cb).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("growth-indicator"), "got: {msg}");
    }

    #[test]
    fn prc1st_missing_activity_emits_rmiss_record() {
        let hp = [1.0, 11.0, 25.0, 50.0];
        let ctx = default_ctx("06000", &hp);
        let mut cb = StubCallbacks::ok_single_year();
        cb.activity = None;
        let out = process_state_from_national_record(&ctx, &mut cb).unwrap();
        assert!(matches!(
            out.warnings[0],
            GeographyWarning::MissingActivity { .. }
        ));
        assert_eq!(out.state_outputs.len(), 1);
        let rec = &out.state_outputs[0];
        assert!(rec.missing);
        assert_eq!(rec.population, RMISS);
        assert!(rec.emissions_day.iter().all(|&v| v == RMISS));
    }

    #[test]
    fn prc1st_growth_file_missing_errors() {
        let hp = [1.0, 11.0, 25.0, 50.0];
        let mut ctx = default_ctx("06000", &hp);
        ctx.run_options.growth_loaded = false;
        let mut cb = StubCallbacks::ok_single_year();
        let err = process_state_from_national_record(&ctx, &mut cb).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("GROWTH FILES"), "got: {msg}");
    }

    #[test]
    fn prc1st_full_path_emits_state_record_and_bmy() {
        let hp = [1.0, 11.0, 25.0, 50.0];
        let ctx = default_ctx("06000", &hp);
        let mut cb = StubCallbacks::ok_single_year();
        let out = process_state_from_national_record(&ctx, &mut cb).unwrap();
        // 1 state record + 1 exhaust BMY + 1 evap BMY
        assert_eq!(out.state_outputs.len(), 1);
        assert_eq!(out.bmy_outputs.len(), 2);
        // SI aggregates: 1 exhaust + 1 evap = 2.
        assert_eq!(out.si_aggregates.len(), 2);

        let st = &out.state_outputs[0];
        assert_eq!(st.fips, "06000");
        assert!(!st.missing);
        // poptot = popsta * modfrc(0) = 1000 * 1 = 1000.
        assert!((st.population - 1000.0).abs() < 1e-3);
        // emsday[THC] = 100, emsday[diurnal] = 50.
        assert!((st.emissions_day[0] - 100.0).abs() < 1e-3);
        assert!((st.emissions_day[7] - 50.0).abs() < 1e-3);

        // Exhaust BMY has channel=1.
        let exhaust_bmy = out.bmy_outputs.iter().find(|b| b.channel == 1).unwrap();
        assert_eq!(exhaust_bmy.fips, "06000");
        assert_eq!(exhaust_bmy.tech_type, "T0");
        assert!((exhaust_bmy.population - 1000.0).abs() < 1e-3);

        // Evap BMY has channel=2 with RMISS frac/units retrofitted.
        let evap_bmy = out.bmy_outputs.iter().find(|b| b.channel == 2).unwrap();
        assert_eq!(evap_bmy.fips, "06000");
        assert_eq!(evap_bmy.tech_type, "E0");
        assert_eq!(evap_bmy.frac_retrofitted, RMISS);
        assert_eq!(evap_bmy.units_retrofitted, RMISS);
    }

    // ---------- prcsta.f path ----------

    #[test]
    fn prcsta_zero_population_emits_zero_record_per_selected_county() {
        let hp = [1.0, 11.0, 25.0, 50.0];
        let mut ctx = default_ctx("06000", &hp);
        ctx.equipment.population = 0.0;
        let counties = vec![
            CountyInput {
                fips: "06001".to_string(),
                selected: true,
                population: 100.0,
            },
            CountyInput {
                fips: "06037".to_string(),
                selected: false,
                population: 200.0,
            },
            CountyInput {
                fips: "06059".to_string(),
                selected: true,
                population: 50.0,
            },
        ];
        let mut cb = StubCallbacks::ok_single_year();
        let out = process_state_to_county_record(&ctx, &counties, &mut cb).unwrap();
        // 2 zero records for the 2 selected counties.
        assert_eq!(out.state_outputs.len(), 2);
        assert_eq!(out.state_outputs[0].fips, "06001");
        assert_eq!(out.state_outputs[1].fips, "06059");
        assert!(out.state_outputs.iter().all(|r| r.population == 0.0));
    }

    #[test]
    fn prcsta_missing_activity_emits_rmiss_per_selected_county() {
        let hp = [1.0, 11.0, 25.0, 50.0];
        let ctx = default_ctx("06000", &hp);
        let counties = vec![CountyInput {
            fips: "06001".to_string(),
            selected: true,
            population: 100.0,
        }];
        let mut cb = StubCallbacks::ok_single_year();
        cb.activity = None;
        let out = process_state_to_county_record(&ctx, &counties, &mut cb).unwrap();
        assert!(matches!(
            out.warnings[0],
            GeographyWarning::MissingActivity { .. }
        ));
        assert_eq!(out.state_outputs.len(), 1);
        assert!(out.state_outputs[0].missing);
        assert_eq!(out.state_outputs[0].population, RMISS);
    }

    #[test]
    fn prcsta_full_path_allocates_proportionally() {
        let hp = [1.0, 11.0, 25.0, 50.0];
        let ctx = default_ctx("06000", &hp);
        let counties = vec![
            CountyInput {
                fips: "06037".to_string(),
                selected: true,
                population: 250.0,
            },
            CountyInput {
                fips: "06059".to_string(),
                selected: true,
                population: 750.0,
            },
        ];
        let mut cb = StubCallbacks::ok_single_year();
        let out = process_state_to_county_record(&ctx, &counties, &mut cb).unwrap();

        // 2 county state records + 2 (1 exhaust + 1 evap) BMY per county.
        assert_eq!(out.state_outputs.len(), 2);
        assert_eq!(out.bmy_outputs.len(), 4);

        // First county fraction = 250/1000 = 0.25.
        let c1 = &out.state_outputs[0];
        assert_eq!(c1.fips, "06037");
        assert!((c1.population - 250.0).abs() < 1e-3); // 1000 * 0.25
        assert!((c1.emissions_day[0] - 25.0).abs() < 1e-3); // 100 THC * 0.25
        assert!((c1.emissions_day[7] - 12.5).abs() < 1e-3); // 50 diurnal * 0.25
                                                            // Retrofit fraction is unscaled.
        assert_eq!(c1.frac_retrofitted, 0.0); // No retrofit in default stub.

        // Second county fraction = 750/1000 = 0.75.
        let c2 = &out.state_outputs[1];
        assert_eq!(c2.fips, "06059");
        assert!((c2.population - 750.0).abs() < 1e-3);
        assert!((c2.emissions_day[0] - 75.0).abs() < 1e-3);

        // BMY scaling.
        let bmy_county1: Vec<_> = out
            .bmy_outputs
            .iter()
            .filter(|b| b.fips == "06037")
            .collect();
        assert_eq!(bmy_county1.len(), 2);
        let exhaust_c1 = bmy_county1.iter().find(|b| b.channel == 1).unwrap();
        assert!((exhaust_c1.population - 250.0).abs() < 1e-3);
    }

    #[test]
    fn prcsta_skips_unselected_counties() {
        let hp = [1.0, 11.0, 25.0, 50.0];
        let ctx = default_ctx("06000", &hp);
        let counties = vec![
            CountyInput {
                fips: "06037".to_string(),
                selected: false,
                population: 250.0,
            },
            CountyInput {
                fips: "06059".to_string(),
                selected: true,
                population: 750.0,
            },
        ];
        let mut cb = StubCallbacks::ok_single_year();
        let out = process_state_to_county_record(&ctx, &counties, &mut cb).unwrap();
        // 1 state record for the only selected county.
        assert_eq!(out.state_outputs.len(), 1);
        assert_eq!(out.state_outputs[0].fips, "06059");
    }

    #[test]
    fn prcsta_emits_zero_record_for_zero_pop_county() {
        let hp = [1.0, 11.0, 25.0, 50.0];
        let ctx = default_ctx("06000", &hp);
        let counties = vec![
            CountyInput {
                fips: "06037".to_string(),
                selected: true,
                population: 0.0,
            },
            CountyInput {
                fips: "06059".to_string(),
                selected: true,
                population: 100.0,
            },
        ];
        let mut cb = StubCallbacks::ok_single_year();
        let out = process_state_to_county_record(&ctx, &counties, &mut cb).unwrap();
        // 2 records. First is the zero-pop county, second is allocated.
        assert_eq!(out.state_outputs.len(), 2);
        assert_eq!(out.state_outputs[0].fips, "06037");
        assert_eq!(out.state_outputs[0].population, 0.0);
        assert!(!out.state_outputs[0].missing);
        assert_eq!(out.state_outputs[1].fips, "06059");
        // 100 / 1000 = 0.1 of state population.
        assert!((out.state_outputs[1].population - 100.0).abs() < 1e-3);
    }

    // ---------- compute_state_aggregate ----------

    #[test]
    fn aggregate_multi_year_multi_tech_accumulates() {
        let hp = [1.0, 11.0, 25.0, 50.0];
        let ctx = default_ctx("06000", &hp);
        let mut cb = StubCallbacks::ok_single_year();
        // Set up 2 model years, each with 0.5 modfrc.
        cb.nyrlif = 2;
        let mut modfrc = vec![0.0; 51];
        modfrc[0] = 0.5;
        modfrc[1] = 0.5;
        cb.modfrc = modfrc;
        // Two exhaust techs: 0.6 + 0.4.
        cb.exhaust_tech = Some(ExhaustTechLookup {
            tech_names: vec!["T0".to_string(), "T1".to_string()],
            fractions: vec![0.6, 0.4],
        });
        let outcome = compute_state_aggregate(&ctx, &mut cb).unwrap();
        let agg = match outcome {
            StateAggregateOutcome::Ok(a) => a,
            other => panic!("expected Ok, got {:?}", state_outcome_label(&other)),
        };
        // 2 years * 2 techs * 1 evap tech: 4 exhaust BMY cells + 2 evap BMY cells.
        assert_eq!(agg.exhaust_bmy.len(), 4);
        assert_eq!(agg.evap_bmy.len(), 2);
        // poptot = 1000*0.5 + 1000*0.5 = 1000.
        assert!((agg.population_total - 1000.0).abs() < 1e-3);
    }

    fn state_outcome_label(o: &StateAggregateOutcome) -> &'static str {
        match o {
            StateAggregateOutcome::Ok(_) => "Ok",
            StateAggregateOutcome::ZeroPopulation { .. } => "ZeroPopulation",
            StateAggregateOutcome::MissingTech { .. } => "MissingTech",
            StateAggregateOutcome::MissingActivity { .. } => "MissingActivity",
        }
    }
}

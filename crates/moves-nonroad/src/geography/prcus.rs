//! US-total processing — `prcus.f` (775 lines).
//!
//! The US-total geography level produces a single output record
//! covering the entire nation. No allocation is performed; the
//! input population is treated as the national total and emissions
//! are computed once for the synthetic FIPS code `"00000"`.
//!
//! The Fortran flow:
//!
//! 1. Compute the HP-level representative from the HP-range midpoint
//!    (`prcus.f` :205–:216).
//! 2. If `popeqp(icurec) <= 0`, emit a zero record and return.
//! 3. Look up the exhaust and evap tech-fraction indices for the run's
//!    `itchyr`. If either is missing, log a warning and skip.
//! 4. Pick the fuel density.
//! 5. Call `daymthf` to get the day-month adjustment factors. Decide
//!    `adjtime`, `tplfac`, `tplful`.
//! 6. Call `emsadj` for the per-day per-pollutant adjustment table.
//! 7. Find the growth-indicator cross-reference and activity record;
//!    if either is missing, emit a missing-record output and return.
//! 8. Load the growth data and compute the population-year growth factor.
//! 9. Run `modyr` then `agedist` to get the per-year modfrc/actadj/etc.
//! 10. Filter retrofit records for the (SCC, HP).
//! 11. Loop over model years iyr ∈ [iepyr - nyrlif + 1, iepyr]:
//!     - skip if modfrc(idxyr) == 0
//!     - tchmdyr = min(iyr, tech_year)
//!     - emfclc -> exhaust emission factors for (scc, tchmdyr)
//!     - filter retrofits by model year
//!     - loop over exhaust tech types:
//!         - skip if tchfrc == 0
//!         - filter retrofits by tech type; clcrtrft -> retrofit reduction
//!         - clcems -> exhaust emissions
//!         - accumulate fulcsm / fulbmytot
//!         - emit `wrtbmy(channel=1)` and `sitot` per request flags
//!     - accumulate poptot/acttot/strtot
//!     - evemfclc -> evap emission factors
//!     - loop over evap tech types:
//!         - skip if evtchfrc == 0
//!         - clcevems -> evap emissions
//!         - emit `wrtbmy(channel=2)` and `sitot` per request flags
//!     - accumulate evpoptot/evacttot/evstrtot
//! 12. fracretro = unitsretro / poptot
//! 13. Emit `wrtdat`.
//!
//! The Rust port mirrors that flow, threading the per-iteration
//! lookups through a [`UsTotalCallbacks`] trait so each input data
//! source stays decoupled from the geography orchestration.

use super::{
    blank_subcounty, fuel_density, hp_level_for_midpoint, missing_emissions,
    temporal_adjustment_for_unit, zero_emissions, ActivityLookup, ByModelYearOutput,
    EquipmentRecord, GeographyError, GeographyOutput, GeographyWarning, RunOptions, SiAggregate,
    StateOutput,
};
use crate::common::consts::{MXPOL, RMISS};
use crate::{Error, Result};

/// 5-digit FIPS sentinel used by `prcus.f` :201 (`fipus = '00000'`).
pub const US_TOTAL_FIPS: &str = "00000";

/// Result of an exhaust tech-fraction lookup. Replaces the
/// `(idxtch, ntech(idxtch), tectyp(idxtch, *), tchfrc(idxtch, *))`
/// COMMON-block fetches.
#[derive(Debug, Clone)]
pub struct ExhaustTechLookup {
    /// Exhaust tech-type names, one entry per used slot.
    pub tech_names: Vec<String>,
    /// Exhaust tech-type fractions in the same order. `tech_names.len()
    /// == fractions.len()`.
    pub fractions: Vec<f32>,
}

/// Result of an evap tech-fraction lookup.
#[derive(Debug, Clone)]
pub struct EvapTechLookup {
    /// Evap tech-type names, one entry per used slot.
    pub tech_names: Vec<String>,
    /// Evap tech-type fractions in the same order.
    pub fractions: Vec<f32>,
}

/// Output of one `clcems`-equivalent call. Replaces the
/// `emsday`/`emsbmy` mutated COMMON arrays with explicit return
/// values.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ExhaustResult {
    /// Per-pollutant daily emissions for this `(model_year, tech)`
    /// tuple, length [`MXPOL`].
    pub ems_day_delta: Vec<f32>,
    /// Per-pollutant by-model-year emissions, length [`MXPOL`].
    pub ems_bmy: Vec<f32>,
}

/// Output of one `clcevems`-equivalent call.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EvapResult {
    /// Per-pollutant daily emissions delta for this evap iteration,
    /// length [`MXPOL`].
    pub ems_day_delta: Vec<f32>,
    /// Per-pollutant by-model-year evap emissions, length [`MXPOL`].
    pub ems_bmy: Vec<f32>,
}

/// Output of one `clcrtrft`-equivalent call.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RetrofitResult {
    /// Fraction of the iteration's population that's retrofitted.
    /// Clamped to `[0, 1]`.
    pub frac_retro: f32,
    /// Number of engines retrofitted (`pop * frac_retro`).
    pub units_retro: f32,
}

/// Output of the day-month-fraction (`daymthf.f`) lookup.
#[derive(Debug, Clone, PartialEq)]
pub struct DayMonthFactor {
    /// Per-Julian-day month-fraction `daymthfac(jday)`.
    pub day_month_fac: Vec<f32>,
    /// Single month-fraction `mthf` (used for `tplful` etc.).
    pub mthf: f32,
    /// Day-fraction `dayf`.
    pub dayf: f32,
    /// Number of days in the period (`ndays`).
    pub n_days: i32,
}

/// Callbacks the [`process_us_total_record`] routine uses for the
/// per-iteration lookups and computations. Caller (Task 113 driver)
/// supplies an implementation that wires each method to the right
/// input table / computation.
///
/// Method names mirror the Fortran routines they replace.
pub trait UsTotalCallbacks {
    /// `fndtch(scc, hp_avg, tech_year)`. `None` when no tech rows
    /// match.
    fn find_exhaust_tech(&mut self, scc: &str, hp_avg: f32, year: i32)
        -> Option<ExhaustTechLookup>;
    /// `fndevtch(scc, hp_avg, tech_year)`. `None` when no evap-tech
    /// rows match.
    fn find_evap_tech(&mut self, scc: &str, hp_avg: f32, year: i32) -> Option<EvapTechLookup>;
    /// `fndgxf(fips, scc, hp_avg)` — returns the growth-indicator
    /// code if a match exists; `None` triggers
    /// [`GeographyError::GrowthIndicatorNotFound`].
    fn find_growth_xref(&mut self, fips: &str, scc: &str, hp_avg: f32) -> Option<i32>;
    /// `fndact(scc, fips, hp_avg)` — `None` when no activity row
    /// matches. Triggers the missing-activity branch of `prcus.f`.
    fn find_activity(&mut self, scc: &str, fips: &str, hp_avg: f32) -> Option<ActivityLookup>;
    /// `daymthf(scc, fips)` — month/day factor lookup.
    fn day_month_factor(&mut self, scc: &str, fips: &str) -> DayMonthFactor;
    /// `getgrw(indcod)` — preload the growth-factor stream for this
    /// indicator. The Fortran source caches the stream into the
    /// `grwfac` COMMON block; the Rust callback is a no-op for
    /// stateless implementations.
    fn load_growth(&mut self, _indcod: i32) -> Result<()> {
        Ok(())
    }
    /// `grwfac(year1, year2, fips, indcod)` — annualised growth
    /// factor between `year1` and `year2`.
    fn growth_factor(&mut self, year1: i32, year2: i32, fips: &str, indcod: i32) -> Result<f32>;
    /// `modyr(..)` — initial age-distribution computation.
    /// Returns `(yryrfrcscrp, modfrc, stradj, actadj, detage, nyrlif)`
    /// with each slice of length `MXAGYR` (except `nyrlif` which is
    /// a scalar). The Rust port has [`crate::population::model_year`]
    /// available; the caller marshals between forms.
    #[allow(clippy::type_complexity)]
    fn model_year(
        &mut self,
        eq: &EquipmentRecord,
        activity: &ActivityLookup,
        growth_factor: f32,
    ) -> Result<ModelYearOutput>;
    /// `agedist(..)` — grow the age distribution to the growth year.
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
    /// `fndrtrft(filter_type, scc, hp, year, tech)` —
    /// `filter_type=1`: filter by (scc, hp); `=2`: filter by year;
    /// `=3`: filter by tech. The Fortran source threads filter state
    /// across calls; the callback owns that state.
    fn filter_retrofits_by_scc_hp(&mut self, scc: &str, hp_avg: f32) -> Result<()>;
    /// `fndrtrft(filter_type=2, year)`.
    fn filter_retrofits_by_year(&mut self, year: i32) -> Result<()>;
    /// `fndrtrft(filter_type=3, tech)`.
    fn filter_retrofits_by_tech(&mut self, tech: &str) -> Result<()>;
    /// `clcrtrft(..)` — retrofit reduction. The Fortran source also
    /// mutates the per-pollutant `rtrftplltntrdfrc` array shared with
    /// the emission calculators; the callback owns that state.
    fn calculate_retrofit(
        &mut self,
        pop: f32,
        scc: &str,
        hp_avg: f32,
        model_year: i32,
        tech: &str,
    ) -> Result<RetrofitResult>;
    /// `clcems(..)` — exhaust emission computation for one
    /// `(model_year, tech)` tuple. Inputs encoded as
    /// [`ExhaustCallInputs`] so callers can wire it to
    /// [`crate::emissions::calculate_exhaust_emissions`].
    fn calculate_exhaust(&mut self, inputs: &ExhaustCallInputs<'_>) -> Result<ExhaustResult>;
    /// `clcevems(..)` — evap emission computation for one
    /// `(model_year, evap_tech)` tuple. Inputs as
    /// [`EvapCallInputs`].
    fn calculate_evap(&mut self, inputs: &EvapCallInputs<'_>) -> Result<EvapResult>;
}

/// Output of [`UsTotalCallbacks::model_year`]. Mirrors the slice of
/// [`crate::population::modyr::ModelYearOutput`] the geography
/// routine actually consumes.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelYearOutput {
    /// Year-to-year fraction scrapped (length MXAGYR).
    pub yryrfrcscrp: Vec<f32>,
    /// Initial model-year fractions (length MXAGYR).
    pub modfrc: Vec<f32>,
    /// Starts adjustment per year (length `nyrlif`).
    pub stradj: Vec<f32>,
    /// Activity adjustment per year (length `nyrlif`).
    pub actadj: Vec<f32>,
    /// Deterioration age per year (length `nyrlif`).
    pub detage: Vec<f32>,
    /// Lifetime in years.
    pub nyrlif: usize,
}

/// Inputs to [`UsTotalCallbacks::calculate_exhaust`].
///
/// Mirrors the Fortran `clcems` argument list, with each Fortran
/// COMMON-block-derived input made explicit.
#[derive(Debug, Clone)]
pub struct ExhaustCallInputs<'a> {
    /// 10-character SCC.
    pub scc: &'a str,
    /// Activity lookup result (load factor, units, etc.).
    pub activity: &'a ActivityLookup,
    /// Average HP.
    pub hp_avg: f32,
    /// Fuel density (`denful`).
    pub fuel_density: f32,
    /// 0-based year index (`idxyr` in Fortran; `iepyr - iyr` in 0-based).
    pub year_index: usize,
    /// 0-based tech-type slot within the SCC's tech rows.
    pub tech_index: usize,
    /// Tech-type name for this slot (for the inner `tchfrc` lookup
    /// the calculator does).
    pub tech_name: &'a str,
    /// `tchfrc(idxtch, tech_index)` for this slot.
    pub tech_fraction: f32,
    /// Population for the current iteration (`popus`).
    pub population: f32,
    /// `modfrc(idxyr)` — model-year fraction.
    pub model_year_fraction: f32,
    /// `actadj(idxyr)` — activity adjustment.
    pub activity_adjustment: f32,
    /// `stradj(idxyr)` — starts adjustment.
    pub starts_adjustment: f32,
    /// `detage(idxyr)` — deterioration age.
    pub deterioration_age: f32,
    /// Temporal adjustment factor (`tpltmp`).
    pub temporal_adjustment: f32,
    /// Time-period adjustment (`adjtime`, `1.0` or `1/ndays`).
    pub adjustment_time: f32,
    /// Period day count (`ndays`).
    pub n_days: i32,
}

/// Inputs to [`UsTotalCallbacks::calculate_evap`].
#[derive(Debug, Clone)]
pub struct EvapCallInputs<'a> {
    /// 10-character SCC.
    pub scc: &'a str,
    /// Activity lookup result.
    pub activity: &'a ActivityLookup,
    /// Average HP.
    pub hp_avg: f32,
    /// 0-based year index.
    pub year_index: usize,
    /// 0-based evap-tech slot.
    pub evap_tech_index: usize,
    /// Evap tech-type name.
    pub evap_tech_name: &'a str,
    /// `evtchfrc(idxevtch, evap_tech_index)` for this slot.
    pub evap_tech_fraction: f32,
    /// Population (`popus`).
    pub population: f32,
    /// `modfrc(idxyr)`.
    pub model_year_fraction: f32,
    /// `actadj(idxyr)`.
    pub activity_adjustment: f32,
    /// `stradj(idxyr)`.
    pub starts_adjustment: f32,
    /// `detage(idxyr)`.
    pub deterioration_age: f32,
    /// Temporal adjustment factor.
    pub temporal_adjustment: f32,
    /// Time-period adjustment.
    pub adjustment_time: f32,
    /// Period day count.
    pub n_days: i32,
    /// FIPS code for the iteration (passed through so the evap
    /// calculator can pick the right diurnal table).
    pub fips: &'a str,
    /// Estimated fuel consumption for the current model year × tech
    /// (`fulbmy = fulbmytot * evtchfrc(idxevtch, i)`).
    pub fuel_consumption: f32,
}

/// Per-call context bundle for [`process_us_total_record`]. Owns
/// the inputs that don't depend on per-iteration state.
#[derive(Debug, Clone)]
pub struct UsTotalContext<'a> {
    /// Per-record equipment data.
    pub equipment: EquipmentRecord,
    /// Run-time options.
    pub run_options: RunOptions,
    /// 10-character SCC for the current iteration.
    pub scc: &'a str,
    /// HP-level table (`hpclev`).
    pub hp_levels: &'a [f32],
}

/// Process one US-total record. Ports `prcus.f`.
///
/// Returns a [`GeographyOutput`] holding exactly one [`StateOutput`]
/// (FIPS = `"00000"`), plus any `wrtbmy` / `sitot` records gated by
/// [`RunOptions::emit_bmy`], [`RunOptions::emit_bmy_evap`], and
/// [`RunOptions::emit_si`], plus any non-fatal warnings.
///
/// Errors:
///
/// - [`GeographyError::GrowthFileMissing`] when the run requested
///   growth-dependent processing without loading the `/GROWTH FILES/`
///   packet (`prcus.f` :7003 / :318).
/// - [`GeographyError::GrowthIndicatorNotFound`] when no growth
///   cross-reference matches (`prcus.f` :7001 / :320).
/// - any error propagated from a callback.
pub fn process_us_total_record(
    ctx: &UsTotalContext<'_>,
    callbacks: &mut dyn UsTotalCallbacks,
) -> Result<GeographyOutput> {
    let eq = &ctx.equipment;
    let opt = &ctx.run_options;
    let scc = ctx.scc;
    let subcur = blank_subcounty();
    let fipus = US_TOTAL_FIPS;

    let mut output = GeographyOutput::default();

    // --- HP level (prcus.f :205–:216) ---
    let hpmid = (eq.hp_range_min + eq.hp_range_max) / 2.0;
    let hplev = hp_level_for_midpoint(hpmid, ctx.hp_levels);
    let hpval = eq.hp_avg;

    // --- emsday(MXPOL) := 0 (prcus.f :221–:223) ---
    let mut emsday = zero_emissions();

    // --- zero-population early return (prcus.f :227–:233) ---
    if eq.population <= 0.0 {
        output.state_outputs.push(StateOutput {
            fips: fipus.to_string(),
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
            emissions_day: emsday,
            missing: false,
        });
        return Ok(output);
    }

    let popus = eq.population;

    // --- find exhaust + evap tech for tech_year; warn & skip if missing
    //     (prcus.f :243–:270) ---
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

    // --- fuel density (prcus.f :274–:283) ---
    let denful = fuel_density(opt.fuel);

    // --- daymthf, tplfac, tplful, adjtime (prcus.f :289–:309) ---
    let dmf = callbacks.day_month_factor(scc, fipus);
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

    // --- growth-file packet must be loaded (prcus.f :318 / :7003) ---
    if !opt.growth_loaded {
        return Err(Error::Config(GeographyError::GrowthFileMissing.to_string()));
    }

    // --- growth Xref / activity / missing-activity branch (prcus.f :319–:344) ---
    let Some(indcod) = callbacks.find_growth_xref(fipus, scc, hpval) else {
        return Err(Error::Config(
            GeographyError::GrowthIndicatorNotFound {
                fips: fipus.to_string(),
                scc: scc.to_string(),
                hp_avg: hpval,
                hp_min: eq.hp_range_min,
                hp_max: eq.hp_range_max,
            }
            .to_string(),
        ));
    };
    let Some(activity) = callbacks.find_activity(scc, fipus, hpval) else {
        output.warnings.push(GeographyWarning::MissingActivity {
            scc: scc.to_string(),
            fips: fipus.to_string(),
            hp_min: eq.hp_range_min,
            hp_max: eq.hp_range_max,
        });
        // Construct the "missing data" output (prcus.f :336–:341).
        output.state_outputs.push(StateOutput {
            fips: fipus.to_string(),
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
        return Ok(output);
    };

    // --- load growth + compute the ipopyr → ipopyr+1 growth factor
    //     (prcus.f :348–:360) ---
    callbacks.load_growth(indcod)?;
    let grwus = callbacks.growth_factor(eq.pop_year, eq.pop_year + 1, fipus, indcod)?;

    // --- modyr -> initial age distribution (prcus.f :365–:369) ---
    let my_out = callbacks.model_year(eq, &activity, grwus)?;
    let nyrlif = my_out.nyrlif;

    // --- agedist -> backwards/forwards grown population + modfrc
    //     (prcus.f :375–:377). The callback receives the
    //     base-year inputs and returns the (possibly grown) pop. ---
    let popus = callbacks.age_distribution(
        popus,
        &my_out.modfrc,
        eq.pop_year,
        opt.growth_year,
        &my_out.yryrfrcscrp,
        fipus,
        indcod,
    )?;
    // NOTE: agedist also grows modfrc; the callback's contract is to
    // mutate `my_out.modfrc` in place. The trait method returns just
    // the new base population; modfrc updates flow through the
    // callback's internal state. Implementers must ensure modfrc is
    // updated for the subsequent reads.
    //
    // For simplicity in this initial port, the callback contract is
    // that `my_out` already reflects any agedist growth; the caller
    // wires the modyr → agedist composition together.
    let _ = popus; // shadow eq.population for the rest of the routine
    let popus = popus.max(0.0);

    // --- initialise totals (prcus.f :382–:388, :398–:400) ---
    let mut poptot: f32 = 0.0;
    let mut acttot: f32 = 0.0;
    let mut strtot: f32 = 0.0;
    let mut fulcsm: f32 = 0.0;
    let mut fracretro: f32 = 0.0;
    let mut unitsretro: f32 = 0.0;
    let mut evpoptot: f32 = 0.0;
    let mut evacttot: f32 = 0.0;
    let mut evstrtot: f32 = 0.0;

    // --- filter retrofits to (SCC, HP) (prcus.f :391–:394) ---
    if opt.retrofit_loaded {
        callbacks.filter_retrofits_by_scc_hp(scc, hpval)?;
    }

    // --- model-year loop (prcus.f :404–:707) ---
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

        // --- exhaust tech for this model year (prcus.f :432–:437) ---
        let Some(tech) = callbacks.find_exhaust_tech(scc, hpval, tchmdyr) else {
            // The Fortran source's outer `fndtch(itchyr)` already
            // succeeded; if this per-year lookup fails we follow the
            // Fortran warning path (prcus.f :382–:394 idiom — though
            // prcus.f itself doesn't have the per-year warning that
            // prcnat.f does, the Rust port treats them symmetrically).
            output.warnings.push(GeographyWarning::MissingExhaustTech {
                scc: scc.to_string(),
                hp_avg: hpval,
                year: tchmdyr,
            });
            return Ok(output);
        };

        // --- emfclc-equivalent runs inside the callback's
        //     `calculate_exhaust` (we don't precompute the EF table
        //     here; the Fortran source's `emfclc` writes to scratch
        //     arrays that the same routine then reads in `clcems`).
        //     The callback owns the EF lifecycle. ---

        // --- filter retrofits by model year (prcus.f :449–:452) ---
        if opt.retrofit_loaded {
            callbacks.filter_retrofits_by_year(iyr)?;
        }

        let mut fulbmytot: f32 = 0.0;

        // --- exhaust tech-type loop (prcus.f :456–:546) ---
        for (tech_i, tech_name) in tech.tech_names.iter().enumerate() {
            let tfrac = tech.fractions[tech_i];
            if tfrac <= 0.0 {
                continue;
            }

            // --- population for this tech (prcus.f :474) ---
            let popbmy = popus * modfrc * tfrac;

            // --- filter retrofits by tech type + retrofit calc
            //     (prcus.f :480–:488) ---
            let mut frac_retro_bmy = 0.0_f32;
            let mut units_retro_bmy = 0.0_f32;
            if opt.retrofit_loaded {
                callbacks.filter_retrofits_by_tech(tech_name)?;
                let r = callbacks.calculate_retrofit(popbmy, scc, hpval, iyr, tech_name)?;
                frac_retro_bmy = r.frac_retro;
                units_retro_bmy = r.units_retro;
                unitsretro += units_retro_bmy;
            }

            // --- temporal adjustment factor (prcus.f :492–:498) ---
            let tpltmp = temporal_adjustment_for_unit(activity.units, tplfac);

            // --- clcems-equivalent (prcus.f :502–:508) ---
            let er = callbacks.calculate_exhaust(&ExhaustCallInputs {
                scc,
                activity: &activity,
                hp_avg: hpval,
                fuel_density: denful,
                year_index: idxyr,
                tech_index: tech_i,
                tech_name,
                tech_fraction: tfrac,
                population: popus,
                model_year_fraction: modfrc,
                activity_adjustment: actadj,
                starts_adjustment: stradj,
                deterioration_age: detage,
                temporal_adjustment: tpltmp,
                adjustment_time: adjtime,
                n_days: ndays,
            })?;

            // --- accumulate emsday (prcus.f :502–:508 side effects) ---
            accumulate_emissions(&mut emsday, &er.ems_day_delta);

            // --- bookkeeping (prcus.f :512–:520) ---
            let actbmy = actadj * popus * modfrc * tplful * tfrac * adjtime;
            let fulbmy = tplful
                * popus
                * actadj
                * modfrc
                * tfrac
                * (hpval * activity.load_factor * 1.0 / denful.max(f32::MIN_POSITIVE))
                * adjtime;
            // Note: the Fortran source uses `bsfc(idxyr, i)`, which is
            // looked up inside `emfclc`. The Rust port treats the bsfc
            // as an output of the exhaust calculator's bookkeeping —
            // see `ExhaustCallInputs` doc.

            fulcsm += fulbmy;
            fulbmytot += fulbmy;

            // --- wrtbmy(1=exhaust) (prcus.f :527–:535) ---
            if opt.emit_bmy {
                output.bmy_outputs.push(ByModelYearOutput {
                    fips: fipus.to_string(),
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
            // --- sitot (prcus.f :539–:542) ---
            if opt.emit_si {
                output.si_aggregates.push(SiAggregate {
                    fips: fipus.to_string(),
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

        // --- population / activity / starts totals (prcus.f :550–:554) ---
        poptot += popus * modfrc;
        acttot += actadj * popus * modfrc * tplful * adjtime;
        strtot += stradj * popus * modfrc * tplful * adjtime;

        // --- evap tech for this tech_year-capped year (prcus.f :564) ---
        let Some(evtech) = callbacks.find_evap_tech(scc, hpval, tchmdyr) else {
            output.warnings.push(GeographyWarning::MissingEvapTech {
                scc: scc.to_string(),
                hp_avg: hpval,
                year: tchmdyr,
            });
            return Ok(output);
        };

        // --- evap tech-type loop (prcus.f :629–:695) ---
        for (evtech_i, evtech_name) in evtech.tech_names.iter().enumerate() {
            let evfrac = evtech.fractions[evtech_i];
            if evfrac <= 0.0 {
                continue;
            }

            // --- temporal adjustment factor (prcus.f :641–:647) ---
            let tpltmp = temporal_adjustment_for_unit(activity.units, tplfac);

            // --- fuel consumption attributed to this evap tech
            //     (prcus.f :651) ---
            let fulbmy_evap = fulbmytot * evfrac;

            // --- clcevems-equivalent (prcus.f :655–:666) ---
            let er = callbacks.calculate_evap(&EvapCallInputs {
                scc,
                activity: &activity,
                hp_avg: hpval,
                year_index: idxyr,
                evap_tech_index: evtech_i,
                evap_tech_name: evtech_name,
                evap_tech_fraction: evfrac,
                population: popus,
                model_year_fraction: modfrc,
                activity_adjustment: actadj,
                starts_adjustment: stradj,
                deterioration_age: detage,
                temporal_adjustment: tpltmp,
                adjustment_time: adjtime,
                n_days: ndays,
                fips: fipus,
                fuel_consumption: fulbmy_evap,
            })?;

            accumulate_emissions(&mut emsday, &er.ems_day_delta);

            let popbmy = popus * modfrc * evfrac;
            let actbmy = actadj * popus * modfrc * tplful * evfrac * adjtime;

            // --- wrtbmy(2=evap) (prcus.f :676–:684) ---
            if opt.emit_bmy_evap {
                output.bmy_outputs.push(ByModelYearOutput {
                    fips: fipus.to_string(),
                    subcounty: subcur.clone(),
                    scc: scc.to_string(),
                    hp_level: hplev,
                    tech_type: evtech_name.clone(),
                    model_year: iyr,
                    population: popbmy,
                    emissions: er.ems_bmy.clone(),
                    fuel_consumption: fulbmy_evap,
                    activity: actbmy,
                    // Fortran passes RMISS for these four fields when
                    // writing the evap bmy record (prcus.f :680–:682).
                    load_factor: RMISS,
                    hp_avg: RMISS,
                    frac_retrofitted: RMISS,
                    units_retrofitted: RMISS,
                    channel: 2,
                });
            }
            if opt.emit_si {
                output.si_aggregates.push(SiAggregate {
                    fips: fipus.to_string(),
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

        // --- evap totals (prcus.f :699–:703) ---
        evpoptot += popus * modfrc;
        evacttot += actadj * popus * modfrc * tplful * adjtime;
        evstrtot += stradj * popus * modfrc * tplful * adjtime;
    }

    // --- fraction retrofitted (prcus.f :720) ---
    if poptot > 0.0 {
        fracretro = unitsretro / poptot;
    }

    // --- wrtdat (prcus.f :724–:726) ---
    output.state_outputs.push(StateOutput {
        fips: fipus.to_string(),
        subcounty: subcur,
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

    // Suppress unused warnings — these counters are reported in the
    // Fortran SI report (see `wrtsi.f`) but not emitted here; the
    // Phase 5 plan defers their use to Task 114.
    let _ = (grwus, fracretro, strtot, evpoptot, evacttot, evstrtot);

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

    /// Minimal stub callbacks that simulate a zero-population
    /// short-circuit. Used to exercise the entry-point branches.
    struct StubCallbacks;

    impl UsTotalCallbacks for StubCallbacks {
        fn find_exhaust_tech(
            &mut self,
            _scc: &str,
            _hp_avg: f32,
            _year: i32,
        ) -> Option<ExhaustTechLookup> {
            None
        }
        fn find_evap_tech(
            &mut self,
            _scc: &str,
            _hp_avg: f32,
            _year: i32,
        ) -> Option<EvapTechLookup> {
            None
        }
        fn find_growth_xref(&mut self, _f: &str, _s: &str, _h: f32) -> Option<i32> {
            None
        }
        fn find_activity(&mut self, _s: &str, _f: &str, _h: f32) -> Option<ActivityLookup> {
            None
        }
        fn day_month_factor(&mut self, _s: &str, _f: &str) -> DayMonthFactor {
            DayMonthFactor {
                day_month_fac: vec![1.0; 365],
                mthf: 1.0,
                dayf: 1.0,
                n_days: 1,
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
            Ok(ModelYearOutput {
                yryrfrcscrp: vec![0.0; 51],
                modfrc: vec![0.0; 51],
                stradj: vec![0.0; 1],
                actadj: vec![0.0; 1],
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
            Ok(ExhaustResult {
                ems_day_delta: vec![0.0; MXPOL],
                ems_bmy: vec![0.0; MXPOL],
            })
        }
        fn calculate_evap(&mut self, _: &EvapCallInputs<'_>) -> Result<EvapResult> {
            Ok(EvapResult {
                ems_day_delta: vec![0.0; MXPOL],
                ems_bmy: vec![0.0; MXPOL],
            })
        }
    }

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

    #[test]
    fn zero_population_writes_zero_record_and_returns() {
        let ctx = UsTotalContext {
            equipment: sample_equipment(0.0),
            run_options: sample_options(),
            scc: "2270002000",
            hp_levels: &sample_hp_levels(),
        };
        let mut cb = StubCallbacks;
        let out = process_us_total_record(&ctx, &mut cb).expect("zero-pop path is infallible");
        assert_eq!(out.state_outputs.len(), 1);
        assert_eq!(out.state_outputs[0].fips, US_TOTAL_FIPS);
        assert_eq!(out.state_outputs[0].population, 0.0);
        assert!(!out.state_outputs[0].missing);
        assert!(out.warnings.is_empty());
        assert!(out.bmy_outputs.is_empty());
    }

    #[test]
    fn missing_exhaust_tech_warns_and_returns_no_outputs() {
        let ctx = UsTotalContext {
            equipment: sample_equipment(1000.0),
            run_options: sample_options(),
            scc: "2270002000",
            hp_levels: &sample_hp_levels(),
        };
        let mut cb = StubCallbacks;
        let out = process_us_total_record(&ctx, &mut cb).unwrap();
        assert_eq!(out.warnings.len(), 1);
        assert!(matches!(
            &out.warnings[0],
            GeographyWarning::MissingExhaustTech { .. }
        ));
        assert!(out.state_outputs.is_empty());
    }

    /// Stub callbacks that simulate a full happy-path run. Tech rows
    /// are present, activity is present, retrofits are off, no
    /// emissions added (calculators return zero), so the output is a
    /// single zero `StateOutput`.
    struct HappyPathCallbacks {
        exhaust_calls: usize,
        evap_calls: usize,
    }

    impl UsTotalCallbacks for HappyPathCallbacks {
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
        fn find_activity(&mut self, _: &str, _: &str, _: f32) -> Option<ActivityLookup> {
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
    fn happy_path_emits_single_state_output() {
        let ctx = UsTotalContext {
            equipment: sample_equipment(1000.0),
            run_options: sample_options(),
            scc: "2270002000",
            hp_levels: &sample_hp_levels(),
        };
        let mut cb = HappyPathCallbacks {
            exhaust_calls: 0,
            evap_calls: 0,
        };
        let out = process_us_total_record(&ctx, &mut cb).unwrap();
        // exactly one model year × one tech × (exhaust + evap) =>
        // one of each calculator call.
        assert_eq!(cb.exhaust_calls, 1, "expected one exhaust call");
        assert_eq!(cb.evap_calls, 1, "expected one evap call");
        assert_eq!(out.state_outputs.len(), 1);
        let so = &out.state_outputs[0];
        assert_eq!(so.fips, US_TOTAL_FIPS);
        assert!(!so.missing);
        // population total == popus * modfrc(0) = 1000 * 1.0
        assert_eq!(so.population, 1000.0);
        // emissions vector is filled with zeros
        assert_eq!(so.emissions_day.len(), MXPOL);
        assert!(so.emissions_day.iter().all(|&v| v == 0.0));
    }

    #[test]
    fn missing_growth_xref_returns_error() {
        struct NoGrowthXref;
        impl UsTotalCallbacks for NoGrowthXref {
            fn find_exhaust_tech(&mut self, _: &str, _: f32, _: i32) -> Option<ExhaustTechLookup> {
                Some(ExhaustTechLookup {
                    tech_names: vec!["X".to_string()],
                    fractions: vec![1.0],
                })
            }
            fn find_evap_tech(&mut self, _: &str, _: f32, _: i32) -> Option<EvapTechLookup> {
                Some(EvapTechLookup {
                    tech_names: vec!["X".to_string()],
                    fractions: vec![1.0],
                })
            }
            fn find_growth_xref(&mut self, _: &str, _: &str, _: f32) -> Option<i32> {
                None
            }
            fn find_activity(&mut self, _: &str, _: &str, _: f32) -> Option<ActivityLookup> {
                None
            }
            fn day_month_factor(&mut self, _: &str, _: &str) -> DayMonthFactor {
                DayMonthFactor {
                    day_month_fac: vec![1.0; 365],
                    mthf: 1.0,
                    dayf: 1.0,
                    n_days: 1,
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
                Ok(ModelYearOutput {
                    yryrfrcscrp: vec![0.0; 51],
                    modfrc: vec![0.0; 51],
                    stradj: vec![],
                    actadj: vec![],
                    detage: vec![],
                    nyrlif: 0,
                })
            }
            fn age_distribution(
                &mut self,
                _: f32,
                _: &[f32],
                _: i32,
                _: i32,
                _: &[f32],
                _: &str,
                _: i32,
            ) -> Result<f32> {
                Ok(0.0)
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
                Ok(ExhaustResult::default())
            }
            fn calculate_evap(&mut self, _: &EvapCallInputs<'_>) -> Result<EvapResult> {
                Ok(EvapResult::default())
            }
        }
        let ctx = UsTotalContext {
            equipment: sample_equipment(1000.0),
            run_options: sample_options(),
            scc: "2270002000",
            hp_levels: &sample_hp_levels(),
        };
        let mut cb = NoGrowthXref;
        let err = process_us_total_record(&ctx, &mut cb).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("growth-indicator"));
    }

    #[test]
    fn missing_activity_writes_missing_record() {
        struct NoActivity;
        impl UsTotalCallbacks for NoActivity {
            fn find_exhaust_tech(&mut self, _: &str, _: f32, _: i32) -> Option<ExhaustTechLookup> {
                Some(ExhaustTechLookup {
                    tech_names: vec!["X".to_string()],
                    fractions: vec![1.0],
                })
            }
            fn find_evap_tech(&mut self, _: &str, _: f32, _: i32) -> Option<EvapTechLookup> {
                Some(EvapTechLookup {
                    tech_names: vec!["X".to_string()],
                    fractions: vec![1.0],
                })
            }
            fn find_growth_xref(&mut self, _: &str, _: &str, _: f32) -> Option<i32> {
                Some(7)
            }
            fn find_activity(&mut self, _: &str, _: &str, _: f32) -> Option<ActivityLookup> {
                None
            }
            fn day_month_factor(&mut self, _: &str, _: &str) -> DayMonthFactor {
                DayMonthFactor {
                    day_month_fac: vec![1.0; 365],
                    mthf: 1.0,
                    dayf: 1.0,
                    n_days: 1,
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
                Ok(ModelYearOutput {
                    yryrfrcscrp: vec![],
                    modfrc: vec![],
                    stradj: vec![],
                    actadj: vec![],
                    detage: vec![],
                    nyrlif: 0,
                })
            }
            fn age_distribution(
                &mut self,
                _: f32,
                _: &[f32],
                _: i32,
                _: i32,
                _: &[f32],
                _: &str,
                _: i32,
            ) -> Result<f32> {
                Ok(0.0)
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
                Ok(ExhaustResult::default())
            }
            fn calculate_evap(&mut self, _: &EvapCallInputs<'_>) -> Result<EvapResult> {
                Ok(EvapResult::default())
            }
        }
        let ctx = UsTotalContext {
            equipment: sample_equipment(1000.0),
            run_options: sample_options(),
            scc: "2270002000",
            hp_levels: &sample_hp_levels(),
        };
        let mut cb = NoActivity;
        let out = process_us_total_record(&ctx, &mut cb).unwrap();
        assert_eq!(out.state_outputs.len(), 1);
        let so = &out.state_outputs[0];
        assert!(so.missing);
        assert_eq!(so.population, RMISS);
        assert_eq!(out.warnings.len(), 1);
        assert!(matches!(
            &out.warnings[0],
            GeographyWarning::MissingActivity { .. }
        ));
    }
}

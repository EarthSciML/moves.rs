//! Parameterised geography processing — the merged core of
//! `prccty.f` (county) and `prcsub.f` (subcounty).
//!
//! Task 109 ported `prccty.f` and `prcsub.f` as the separate
//! [`process_county`] and [`process_subcounty`] functions for
//! fidelity. Task 112 merges their orchestration here: the two
//! Fortran routines consume the same record shape
//! ([`PopulationRecord`]), the same run options ([`RunOptions`]), the
//! same callback surface ([`GeographyCallbacks`]), and produce the
//! same output ([`ProcessOutput`]). They differ in exactly one place
//! — how the per-record population is resolved before the model-year
//! loop:
//!
//! - **County** uses [`PopulationRecord::population`] directly.
//! - **Subcounty** runs an extra `fndasc` + subcounty-marker +
//!   `alosub` step (`prcsub.f` :240–:266) that subdivides the
//!   state-level population, and tags every output record with the
//!   resolved subcounty marker.
//!
//! [`process_geography`] takes a [`ProcessLevel`] selector to pick
//! between the two; the model-year / tech-type orchestration that
//! follows is shared. [`process_county`] and [`process_subcounty`]
//! are now thin wrappers that pick the selector — the ~700 lines of
//! near-duplicate orchestration that used to live in `prccty.f`'s and
//! `prcsub.f`'s Rust ports are gone.
//!
//! [`process_county`]: super::process_county
//! [`process_subcounty`]: super::process_subcounty

use super::common::{
    accumulate_evap_iteration, accumulate_exhaust_iteration, fuel_density, hp_level_lookup,
    temporal_adjustment, time_period_setup, ActivityRecord, BmyKind, BmyRecord, DatRecord,
    EmissionsIterationResult, GeographyCallbacks, ModelYearAgedistResult, PopulationRecord,
    ProcessOutcome, ProcessOutput, ProcessWarning, RefuelingData, RetrofitFilter, RunOptions,
    SiRecord, WarningKind,
};
use crate::common::consts::{MXPOL, RMISS};
use crate::{Error, Result};

/// Which of the two Task 109 routines [`process_geography`] runs.
///
/// `prccty.f` and `prcsub.f` share the entire model-year / tech-type
/// core; they differ only in how the per-record population is
/// resolved before the loop runs.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProcessLevel {
    /// County-level processing — `prccty.f`. The population comes
    /// straight from the record's `population` field; output records
    /// carry a blank subcounty marker.
    County,
    /// Subcounty-level processing — `prcsub.f`. An extra `fndasc` +
    /// subcounty-marker + `alosub` step (`prcsub.f` :240–:266)
    /// subdivides the population; output records carry the resolved
    /// subcounty marker.
    Subcounty {
        /// Original record index (`icurec`), threaded into `alosub`.
        record_index: usize,
        /// Cached growth value (the `growth` argument to `prcsub.f`).
        /// `None` is the Fortran `-9` "not yet computed" sentinel;
        /// the callback's `allocate_subcounty` produces a real value
        /// in that case.
        cached_growth: Option<f32>,
    },
}

/// Process one county- or subcounty-level population record. Merged
/// port of `prccty.f` and `prcsub.f` (Task 112).
///
/// The `level` selector picks between the two Fortran routines — see
/// [`ProcessLevel`]. Everything after the population-resolution step
/// is identical for both.
///
/// The Rust port turns the Fortran `IFAIL` / `ISUCES` / `ISKIP`
/// trinity into `ProcessOutcome`: `ISUCES` → `Ok(Success(…))`,
/// `ISKIP` → `Ok(Skipped(…))`, `IFAIL` (or any I/O-style fatal) →
/// `Err(…)`. The `emsday`, `emsbmy`, and tally side effects the
/// Fortran source performs in-place are surfaced as `ProcessOutput`
/// fields.
///
/// # Arguments
///
/// - `level`: county vs. subcounty, plus the subcounty-only
///   `icurec` / `growth` inputs.
/// - `record`: per-record COMMON-block reads (`/popdat/` slot).
/// - `options`: run-level settings (`/optdat/`, `/eqpdat/`, `/io/`).
/// - `callbacks`: dependency surface that supplies the helpers the
///   Fortran sources call (`fndtch`, `fndevtch`, `emfclc`,
///   `evemfclc`, `daymthf`, `emsadj`, `getgrw`, `grwfac`, `modyr`,
///   `agedist`, `clcems`, `clcevems`, `fndrtrft`, `fndrfm`, `fndchr`,
///   plus the subcounty-only `fndasc` / `alosub`).
///
/// # Failures
///
/// - [`Error::Config`] when `options.growth_enabled` is `false` — the
///   Fortran source's `7003` error path triggers an explicit I/O
///   message and exits.
/// - [`Error::Config`] when a subcounty record's SCC has no
///   allocation coefficients (`prcsub.f` :240 — the `7000` path).
/// - Errors propagated from any callback method (`clcems`-style
///   numerics, growth-data I/O, etc.).
pub fn process_geography<C: GeographyCallbacks + ?Sized>(
    level: ProcessLevel,
    record: &PopulationRecord<'_>,
    options: &RunOptions,
    callbacks: &mut C,
) -> Result<ProcessOutcome> {
    let mut output = ProcessOutput {
        fips: record.region_code.to_string(),
        subcounty: String::new(),
        emissions_day: vec![0.0; MXPOL],
        emsams_delta: Vec::new(),
        emsams_fips_index: None,
        ..ProcessOutput::default()
    };

    let fipin = record.region_code;

    // ---- prccty.f :204–:209 / prcsub.f :208–:212 — county-FIPS
    //      lookup. ISKIP early-out. ----
    let Some(fips_idx) = callbacks.find_fips(fipin) else {
        return Ok(ProcessOutcome::Skipped(output));
    };
    callbacks.tally_county_record(fips_idx);

    // ---- prccty.f :214–:225 / prcsub.f :217–:228 — HP-level lookup. ----
    let hplev = hp_level_lookup(record.hp_range, &options.hp_levels);
    let hpval = record.hp_avg;
    output.hp_level = hplev;

    // ---- Population resolution — the one place the two routines
    //      diverge before the shared core. County reads the record
    //      population directly; subcounty runs the `fndasc` +
    //      subcounty-marker + `alosub` chain (`prcsub.f` :240–:266). ----
    let (population, subcur) = match level {
        ProcessLevel::County => (record.population, String::new()),
        ProcessLevel::Subcounty {
            record_index,
            cached_growth,
        } => {
            // ---- prcsub.f :240–:241 — fndasc. Missing → 7000 error
            //      path: the Fortran source writes an ERROR message
            //      and falls through to 9999 with `ierr = IFAIL`. ----
            let idxasc = match callbacks.find_allocation(record.scc) {
                Some(idx) => idx,
                None => {
                    return Err(Error::Config(format!(
                        "ERROR:  Could not find any allocation coefficients for SCC code {scc}",
                        scc = record.scc,
                    )));
                }
            };

            // ---- prcsub.f :247–:258 — find the subcounty marker. ----
            let subcur = match callbacks.find_subcounty(fipin) {
                Some(s) => s,
                None => {
                    return Ok(ProcessOutcome::Skipped(output));
                }
            };
            if subcur.trim().is_empty() {
                return Ok(ProcessOutcome::Skipped(output));
            }
            output.subcounty = subcur.clone();

            // ---- prcsub.f :263–:266 — alosub. ----
            let alloc = callbacks.allocate_subcounty(
                record_index,
                idxasc,
                fips_idx,
                &subcur,
                record.base_pop_year,
                record.population,
                cached_growth,
            )?;
            if !alloc.use_record {
                return Ok(ProcessOutcome::Skipped(output));
            }
            (alloc.population, subcur)
        }
    };

    // ---- prccty.f :236–:242 / prcsub.f :271–:277 — zero-population
    //      early-out. ----
    if population <= 0.0 {
        output.dat_records.push(DatRecord {
            fips: fipin.to_string(),
            subcounty: subcur.clone(),
            scc: record.scc.to_string(),
            hp_level: hplev,
            population_total: 0.0,
            activity_total: 0.0,
            fuel_consumption: 0.0,
            load_factor: 0.0,
            hp_avg: 0.0,
            frac_retrofitted: 0.0,
            units_retrofitted: 0.0,
            emissions: output.emissions_day.clone(),
        });
        return Ok(ProcessOutcome::Success(output));
    }

    // ---- prccty.f :251–:262 / prcsub.f :282–:293 — exhaust tech-type
    //      lookup. Validated up-front so we can ISKIP early; the
    //      per-model-year loop re-queries with `tchmdyr`
    //      (`prccty.f` :440) to match the Fortran semantics. ----
    if callbacks
        .find_exhaust_tech(record.scc, hpval, options.tech_year)
        .is_none()
    {
        // `prccty.f` and `prcsub.f` word this warning differently —
        // preserve each verbatim.
        let message = match level {
            ProcessLevel::County => format!(
                "WARNING: Could not find any exhaust technology fractions for equipment: \
                 SCC code {scc} Average HP {hp:7.1} Skipping...",
                scc = record.scc,
                hp = hpval,
            ),
            ProcessLevel::Subcounty { .. } => format!(
                "WARNING:  Could not find any technology fractions for equipment: \
                 SCC code {scc} Average HP {hp:7.1} Skipping...",
                scc = record.scc,
                hp = hpval,
            ),
        };
        output.warnings.push(ProcessWarning {
            kind: WarningKind::MissingTechType,
            message,
        });
        return Ok(ProcessOutcome::Skipped(output));
    }

    // ---- prccty.f :267–:278 / prcsub.f :298–:309 — evap tech-type
    //      lookup. Validated up-front; the per-model-year loop
    //      re-queries with `tchmdyr`. ----
    if callbacks
        .find_evap_tech(record.scc, hpval, options.tech_year)
        .is_none()
    {
        let message = match level {
            ProcessLevel::County => format!(
                "WARNING: Could not find any evap technology fractions for equipment: \
                 SCC code {scc} Average HP {hp:7.1} Skipping...",
                scc = record.scc,
                hp = hpval,
            ),
            ProcessLevel::Subcounty { .. } => format!(
                "WARNING:  Could not find any evap technology fractions for equipment: \
                 SCC code {scc} Average HP {hp:7.1} Skipping...",
                scc = record.scc,
                hp = hpval,
            ),
        };
        output.warnings.push(ProcessWarning {
            kind: WarningKind::MissingTechType,
            message,
        });
        return Ok(ProcessOutcome::Skipped(output));
    }

    // ---- prccty.f :282–:291 / prcsub.f :313–:322 — fuel density. ----
    let denful = fuel_density(options.fuel);

    // ---- prccty.f :297 / prcsub.f :328 — daymthf. ----
    let (daymthfac, mthf, dayf, ndays) = callbacks.day_month_factors(record.scc, fipin);

    // ---- prccty.f :301–:312 / prcsub.f :332–:343 — time-period factors. ----
    let tp = time_period_setup(options.sum_type, ndays, options.daily_mode, mthf, dayf);

    // ---- prccty.f :317 / prcsub.f :348 — emsadj. ----
    let adjems = callbacks.emission_adjustments(record.scc, fipin, &daymthfac);

    // ---- prccty.f :326–:328 / prcsub.f :357–:359 — growth file
    //      required for any non-base-year run. ----
    if !options.growth_enabled {
        return Err(Error::Config(
            "Could not find the /GROWTH FILES/ packet of options file. \
             This packet is required for future year projections or backcasting."
                .into(),
        ));
    }
    let idxgrw = callbacks
        .find_growth_xref(fipin, record.scc, hpval)
        .ok_or_else(|| {
            Error::Config(format!(
                "ERROR: Could not find match in growth indicator cross reference for: \
                 County {fipin} SCC {scc} HP range {lo:6.1} {hi:6.1}",
                fipin = fipin,
                scc = record.scc,
                lo = record.hp_range.0,
                hi = record.hp_range.1,
            ))
        })?;

    // ---- prccty.f :332–:352 / prcsub.f :363–:383 — activity lookup.
    //      Missing → RMISS path. ----
    let idxact = match callbacks.find_activity(record.scc, fipin, hpval) {
        Some(idx) => idx,
        None => {
            output.warnings.push(ProcessWarning {
                kind: WarningKind::MissingActivity,
                message: format!(
                    "WARNING:  Could not find any activity data for: \
                     County {fipin} SCC {scc} HP range {lo:.1} {hi:.1}",
                    fipin = fipin,
                    scc = record.scc,
                    lo = record.hp_range.0,
                    hi = record.hp_range.1,
                ),
            });
            // Fortran :344–:346: emsday(j) = RMISS for j outside
            // [IDXDIU, IDXRLS]. IDXDIU = 8 (slot 7), IDXRLS = 17 (slot
            // 16). The exclusion gates are inclusive in Fortran
            // indices; mirrored as 1-based here.
            for (j, slot) in output.emissions_day.iter_mut().enumerate() {
                let j1 = j + 1; // back to 1-based for the comparison.
                if !(8..=17).contains(&j1) {
                    *slot = RMISS;
                }
            }
            output.dat_records.push(DatRecord {
                fips: fipin.to_string(),
                subcounty: subcur.clone(),
                scc: record.scc.to_string(),
                hp_level: hplev,
                population_total: RMISS,
                activity_total: RMISS,
                fuel_consumption: RMISS,
                load_factor: RMISS,
                hp_avg: RMISS,
                frac_retrofitted: RMISS,
                units_retrofitted: RMISS,
                emissions: output.emissions_day.clone(),
            });
            return Ok(ProcessOutcome::Success(output));
        }
    };

    // ---- prccty.f :356–:357 / prcsub.f :388–:389 — getgrw. ----
    // The Fortran source loads the growth-factor data into the
    // growth-state COMMON; the Rust port lets the callback handle it
    // inside `model_year_and_agedist`.
    //
    // ---- prccty.f :373–:385 / prcsub.f :404–:416 — modyr + agedist. ----
    let modyr_agedist = callbacks.model_year_and_agedist(
        idxact,
        record,
        fipin,
        idxgrw,
        options.episode_year,
        options.growth_year,
        population,
    )?;

    // Retrofit filter type 1: SCC + HP.
    if options.retrofit_enabled {
        callbacks.filter_retrofits(RetrofitFilter::SccHp, record.scc, hpval, 0, "")?;
    }

    // Running accumulators.
    let mut poptot: f32 = 0.0;
    let mut acttot: f32 = 0.0;
    let mut strtot: f32 = 0.0;
    let mut fulcsm: f32 = 0.0;
    let mut unitsretro: f32 = 0.0;
    let mut evpoptot: f32 = 0.0;
    let mut evacttot: f32 = 0.0;
    let mut evstrtot: f32 = 0.0;

    let activity = callbacks.activity_record(idxact);

    // ---- prccty.f :412–:716 / prcsub.f :443–:747 — model-year loop. ----
    let iepyr = options.episode_year;
    let nyrlif = modyr_agedist.nyrlif;
    // Fortran loop: `do 60 iyr = iepyr - nyrlif + 1, iepyr`.
    for iyr in (iepyr - nyrlif as i32 + 1)..=iepyr {
        // `idxyr = iepyr - iyr + 1` (1-based). Convert to 0-based.
        let idxyr_one_based = iepyr - iyr + 1;
        let idxyr0 = (idxyr_one_based - 1) as usize;

        // Skip model years with zero/negative fractions.
        if modyr_agedist.modfrc.get(idxyr0).copied().unwrap_or(0.0) <= 0.0 {
            continue;
        }

        let mut fulbmytot: f32 = 0.0;

        // ---- prccty.f :431 — tchmdyr = min(iyr, itchyr). ----
        let tchmdyr = iyr.min(options.tech_year);

        // ---- Exhaust ----
        run_exhaust_block(
            record,
            options,
            callbacks,
            &subcur,
            iyr,
            tchmdyr,
            idxyr0,
            &modyr_agedist,
            &activity,
            &adjems,
            tp.tplfac,
            tp.tplful,
            tp.adjtime,
            denful,
            population,
            hplev,
            ndays,
            idxact,
            &mut poptot,
            &mut acttot,
            &mut strtot,
            &mut fulcsm,
            &mut unitsretro,
            &mut fulbmytot,
            &mut output,
        )?;

        // ---- Evap ----
        run_evap_block(
            record,
            options,
            callbacks,
            &subcur,
            iyr,
            tchmdyr,
            idxyr0,
            &modyr_agedist,
            &activity,
            &adjems,
            tp.tplfac,
            tp.tplful,
            tp.adjtime,
            population,
            hplev,
            ndays,
            &mut evpoptot,
            &mut evacttot,
            &mut evstrtot,
            fulbmytot,
            &mut output,
        )?;
    }

    // ---- prccty.f :720–:724 / prcsub.f :751–:755 — emsams fold-in. ----
    let mut emsams_delta = vec![0.0_f32; MXPOL];
    for (i, &v) in output.emissions_day.iter().enumerate() {
        if v > 0.0 {
            emsams_delta[i] = v;
        }
    }
    output.emsams_delta = emsams_delta;
    output.emsams_fips_index = Some(fips_idx);

    // ---- prccty.f :737 / prcsub.f :768 — fracretro = unitsretro / poptot. ----
    let fracretro = if poptot > 0.0 {
        unitsretro / poptot
    } else {
        0.0
    };

    // ---- prccty.f :741–:743 / prcsub.f :772–:774 — wrtdat. ----
    output.dat_records.push(DatRecord {
        fips: fipin.to_string(),
        subcounty: subcur,
        scc: record.scc.to_string(),
        hp_level: hplev,
        population_total: poptot,
        activity_total: acttot,
        fuel_consumption: fulcsm,
        load_factor: activity.load_factor,
        hp_avg: hpval,
        frac_retrofitted: fracretro,
        units_retrofitted: unitsretro,
        emissions: output.emissions_day.clone(),
    });

    // strtot / evpoptot / evacttot / evstrtot are accumulated in
    // Fortran but the source code does not surface them to the wrtdat
    // record. They are used by the SI report (wrtsi.f) which ports
    // separately in Task 114. We discard them here.
    let _ = (strtot, evpoptot, evacttot, evstrtot);

    Ok(ProcessOutcome::Success(output))
}

/// Body of the exhaust block (`prccty.f` :433–:562 / `prcsub.f`
/// :444–:566). Split out so [`process_geography`] reads top-to-bottom
/// without one routine spilling past three screens.
#[allow(clippy::too_many_arguments)]
fn run_exhaust_block<C: GeographyCallbacks + ?Sized>(
    record: &PopulationRecord<'_>,
    options: &RunOptions,
    callbacks: &mut C,
    subcur: &str,
    iyr: i32,
    tchmdyr: i32,
    idxyr0: usize,
    modyr_agedist: &ModelYearAgedistResult,
    activity: &ActivityRecord,
    adjems: &crate::emissions::exhaust::AdjustmentTable,
    tplfac: f32,
    tplful: f32,
    adjtime: f32,
    denful: f32,
    population: f32,
    hplev: f32,
    ndays: i32,
    idxact: usize,
    poptot: &mut f32,
    acttot: &mut f32,
    strtot: &mut f32,
    fulcsm: &mut f32,
    unitsretro: &mut f32,
    fulbmytot: &mut f32,
    output: &mut ProcessOutput,
) -> Result<()> {
    // ---- prccty.f :440–:445 — re-resolve idxtch for tchmdyr. ----
    let exhaust_tech = match callbacks.find_exhaust_tech(record.scc, record.hp_avg, tchmdyr) {
        Some(t) => t,
        // Fortran source does not error on a per-year miss — it would
        // silently fall through. The up-front lookup already accepted
        // the SCC, so a miss here is a data bug; we still skip the
        // year to avoid a hard crash.
        None => return Ok(()),
    };

    let n_tech = exhaust_tech.tech_names.len();

    // ---- prccty.f :450–:453 — emfclc. ----
    let factors = callbacks.compute_exhaust_factors(
        record.scc,
        &exhaust_tech.tech_names,
        &exhaust_tech.tech_fractions,
        tchmdyr,
        idxyr0,
        // record_index is used only by error messages; pass 0 since
        // we do not yet track the original `icurec` here.
        0,
    )?;

    // ---- prccty.f :456–:460 — retrofit filter type 2: model year. ----
    if options.retrofit_enabled {
        callbacks.filter_retrofits(RetrofitFilter::ModelYear, "", 0.0, iyr, "")?;
    }

    let activity_unit = activity.activity_unit;
    let tpltmp_exhaust = temporal_adjustment(activity_unit, tplfac);

    // ---- prccty.f :464–:554 — per-tech-type loop. ----
    for tech_idx in 0..n_tech {
        let tchfrc = exhaust_tech.tech_fractions[tech_idx];
        if tchfrc <= 0.0 {
            continue;
        }
        let tech_name = exhaust_tech.tech_names[tech_idx].clone();

        let popbmy = population * modyr_agedist.modfrc[idxyr0] * tchfrc;

        let mut fracretrobmy: f32 = 0.0;
        let mut unitsretrobmy: f32 = 0.0;

        // ---- prccty.f :488–:496 — retrofit type 3 filter + clcrtrft. ----
        if options.retrofit_enabled {
            callbacks.filter_retrofits(RetrofitFilter::TechType, "", 0.0, 0, &tech_name)?;
            let surviving = callbacks.surviving_retrofits();
            // Run the already-ported retrofit calculator. The
            // accumulator pollutant_reduction_fraction is owned by the
            // callback's RetrofitState; we just call to populate
            // fracretrobmy + unitsretrobmy.
            let mut pollutant_rdfrc = vec![0.0_f32; MXPOL];
            let ctx = crate::emissions::retrofit::RetrofitCalcContext {
                scc: record.scc,
                hp_avg: record.hp_avg,
                model_year: iyr,
                tech_type: &tech_name,
            };
            let outcome = crate::emissions::retrofit::calculate_retrofit_reduction(
                &surviving,
                popbmy,
                options.episode_year,
                &mut pollutant_rdfrc,
                &ctx,
            )?;
            fracretrobmy = outcome.frac_retro;
            unitsretrobmy = outcome.units_retro;
            *unitsretro += unitsretrobmy;
        }

        // ---- prccty.f :510–:516 — clcems. ----
        let iter = callbacks.compute_exhaust_iteration(
            record,
            options,
            &factors,
            adjems,
            exhaust_tech.scc_tech_index,
            tech_idx,
            idxyr0,
            modyr_agedist.detage[idxyr0],
            tchfrc,
            tpltmp_exhaust,
            modyr_agedist.stradj[idxyr0],
            modyr_agedist.modfrc[idxyr0],
            modyr_agedist.actadj[idxyr0],
            population,
            ndays,
            idxact,
        )?;
        accumulate_iteration_into_output(&iter, output);

        // ---- prccty.f :520–:524 — actbmy / fulbmy ----
        // Fortran: actbmy = actadj(idxyr) * popcty * modfrc(idxyr) * tplful * tchfrc(idxtch,i) * adjtime
        let actbmy = modyr_agedist.actadj[idxyr0]
            * population
            * modyr_agedist.modfrc[idxyr0]
            * tplful
            * tchfrc
            * adjtime;
        let fulbmy = tplful
            * population
            * modyr_agedist.actadj[idxyr0]
            * modyr_agedist.modfrc[idxyr0]
            * tchfrc
            * (record.hp_avg
                * activity.load_factor
                * factors
                    .bsfc
                    .get(idxyr0 * exhaust_tech.tech_names.len() + tech_idx)
                    .copied()
                    .unwrap_or(0.0)
                / denful)
            * adjtime;
        *fulcsm += fulbmy;
        *fulbmytot += fulbmy;

        // ---- prccty.f :537–:543 — wrtbmy (exhaust). ----
        if options.write_bmy_exhaust {
            output.bmy_records.push(BmyRecord {
                fips: record.region_code.to_string(),
                subcounty: subcur.to_string(),
                scc: record.scc.to_string(),
                hp_level: hplev,
                tech_name: tech_name.clone(),
                model_year: iyr,
                population: popbmy,
                emissions: iter.emsbmy.clone(),
                fuel: fulbmy,
                activity: actbmy,
                load_factor: activity.load_factor,
                hp_avg: record.hp_avg,
                frac_retrofitted: fracretrobmy,
                units_retrofitted: unitsretrobmy,
                kind: BmyKind::Exhaust,
            });
        }

        // ---- prccty.f :547–:550 — sitot. ----
        if options.write_si {
            output.si_records.push(SiRecord {
                tech_name: tech_name.clone(),
                population: popbmy,
                activity: actbmy,
                fuel: fulbmy,
                emissions: iter.emsbmy.clone(),
            });
        }
    }

    // ---- prccty.f :558–:562 — exhaust pop/act/starts accumulator. ----
    accumulate_exhaust_iteration(
        poptot,
        acttot,
        strtot,
        population,
        modyr_agedist.modfrc[idxyr0],
        modyr_agedist.actadj[idxyr0],
        modyr_agedist.stradj[idxyr0],
        tplful,
        adjtime,
    );

    Ok(())
}

/// Body of the evap block (`prccty.f` :566–:716 / `prcsub.f`
/// :572–:730). Same shape as the exhaust block, but with the evap
/// helpers and the refueling lookup at the front.
#[allow(clippy::too_many_arguments)]
fn run_evap_block<C: GeographyCallbacks + ?Sized>(
    record: &PopulationRecord<'_>,
    options: &RunOptions,
    callbacks: &mut C,
    subcur: &str,
    iyr: i32,
    tchmdyr: i32,
    idxyr0: usize,
    modyr_agedist: &ModelYearAgedistResult,
    activity: &ActivityRecord,
    adjems: &crate::emissions::exhaust::AdjustmentTable,
    tplfac: f32,
    tplful: f32,
    adjtime: f32,
    population: f32,
    hplev: f32,
    ndays: i32,
    evpoptot: &mut f32,
    evacttot: &mut f32,
    evstrtot: &mut f32,
    fulbmytot: f32,
    output: &mut ProcessOutput,
) -> Result<()> {
    // ---- prccty.f :573 — evap tech-type for tchmdyr. ----
    let evap_tech = match callbacks.find_evap_tech(record.scc, record.hp_avg, tchmdyr) {
        Some(t) => t,
        None => return Ok(()),
    };

    let n_evtech = evap_tech.tech_names.len();

    // ---- prccty.f :575–:626 — per-evtech refueling data assembly. ----
    let mut refueling_data: Vec<RefuelingData> = vec![RefuelingData::default(); n_evtech];
    #[allow(clippy::needless_range_loop)] // multiple parallel arrays indexed by i
    for i in 0..n_evtech {
        if !options.spillage_enabled {
            continue;
        }
        // Fortran: idxall = fndrfm(asccod, hpval, TECDEF) — fallback
        // to the default tech for when the per-tech lookup misses.
        let default_rfm = callbacks.find_refueling(record.scc, record.hp_avg, "ALL       ");
        // The Fortran source builds a probe tech name from the
        // evap-tech code: `tname = 'E' // evtecnam(i)(j:j)` where
        // `j = IDXSPL - IDXDIU + 2 - 3`. IDXSPL=16, IDXDIU=8, so
        // `j = 16 - 8 + 2 - 3 = 7`. We use the 7th character of the
        // evap tech name (0-based offset 6).
        let evname = &evap_tech.tech_names[i];
        let probe = if evname.len() >= 7 {
            format!("E{}", &evname[6..7])
        } else {
            String::from("E")
        };
        let primary = callbacks.find_refueling(record.scc, record.hp_avg, &probe);
        let resolved = primary.or(default_rfm);
        if let Some(r) = resolved {
            refueling_data[i] = r;
        } else {
            // Fortran source warns + sets tank = -9 (i.e. mode is
            // empty and tank is sentinel). We surface the warning.
            output.warnings.push(ProcessWarning {
                kind: WarningKind::MissingEmissionFactor,
                message: format!(
                    "WARNING:  No Spillage data found for: {scc} {hp:6.1} {evname} {probe}",
                    scc = record.scc,
                    hp = record.hp_avg,
                    evname = evname,
                    probe = probe,
                ),
            });
            refueling_data[i].tank = -9.0;
        }
    }

    // ---- prccty.f :631–:634 — evemfclc. ----
    let evap_factors = callbacks.compute_evap_factors(
        record.scc,
        &evap_tech.tech_names,
        &evap_tech.tech_fractions,
        tchmdyr,
        idxyr0,
        0,
    )?;

    let activity_unit = activity.activity_unit;
    let tpltmp_evap = temporal_adjustment(activity_unit, tplfac);

    // ---- prccty.f :638–:704 — per-evtech-type loop. ----
    #[allow(clippy::needless_range_loop)] // multiple parallel arrays indexed by tech_idx
    for tech_idx in 0..n_evtech {
        let evtchfrc = evap_tech.tech_fractions[tech_idx];
        if evtchfrc <= 0.0 {
            continue;
        }
        let tech_name = evap_tech.tech_names[tech_idx].clone();

        // ---- prccty.f :660 — fulbmy = fulbmytot * evtecfrc(i). ----
        let fulbmy = fulbmytot * evtchfrc;

        // ---- prccty.f :664–:674 — clcevems. ----
        let iter = callbacks.compute_evap_iteration(
            record,
            options,
            &evap_factors,
            adjems,
            &refueling_data[tech_idx],
            evap_tech.scc_tech_index,
            tech_idx,
            idxyr0,
            modyr_agedist.detage[idxyr0],
            evtchfrc,
            &tech_name,
            tpltmp_evap,
            modyr_agedist.stradj[idxyr0],
            modyr_agedist.modfrc[idxyr0],
            modyr_agedist.actadj[idxyr0],
            population,
            ndays,
            fulbmy,
        )?;
        accumulate_iteration_into_output(&iter, output);

        // ---- prccty.f :679–:681 — popbmy / actbmy. ----
        let popbmy = population * modyr_agedist.modfrc[idxyr0] * evtchfrc;
        let actbmy = modyr_agedist.actadj[idxyr0]
            * population
            * modyr_agedist.modfrc[idxyr0]
            * tplful
            * evtchfrc
            * adjtime;

        // ---- prccty.f :685–:693 — wrtbmy (evap). ----
        if options.write_bmy_evap {
            output.bmy_records.push(BmyRecord {
                fips: record.region_code.to_string(),
                subcounty: subcur.to_string(),
                scc: record.scc.to_string(),
                hp_level: hplev,
                tech_name: tech_name.clone(),
                model_year: iyr,
                population: popbmy,
                emissions: iter.emsbmy.clone(),
                fuel: fulbmy,
                activity: actbmy,
                load_factor: RMISS,
                hp_avg: RMISS,
                frac_retrofitted: RMISS,
                units_retrofitted: RMISS,
                kind: BmyKind::Evaporative,
            });
        }

        // ---- prccty.f :697–:700 — sitot. ----
        if options.write_si {
            output.si_records.push(SiRecord {
                tech_name: tech_name.clone(),
                population: popbmy,
                activity: actbmy,
                fuel: fulbmy,
                emissions: iter.emsbmy.clone(),
            });
        }
    }

    // ---- prccty.f :708–:712 — evap pop/act/starts accumulator. ----
    accumulate_evap_iteration(
        evpoptot,
        evacttot,
        evstrtot,
        population,
        modyr_agedist.modfrc[idxyr0],
        modyr_agedist.actadj[idxyr0],
        modyr_agedist.stradj[idxyr0],
        tplful,
        adjtime,
    );

    Ok(())
}

/// Fold one [`EmissionsIterationResult`] into the running
/// `output.emissions_day` accumulator.
fn accumulate_iteration_into_output(iter: &EmissionsIterationResult, output: &mut ProcessOutput) {
    for (i, &delta) in iter.emsday_delta.iter().enumerate() {
        if i >= output.emissions_day.len() {
            break;
        }
        // Fortran semantics: when the iteration produces RMISS for a
        // pollutant slot, the day-level slot becomes RMISS too. The
        // `compute_*_iteration` callback decides this on its end — we
        // just add.
        output.emissions_day[i] += delta;
    }
}

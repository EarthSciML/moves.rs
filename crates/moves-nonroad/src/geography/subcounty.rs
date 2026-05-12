//! Subcounty-level processing — port of `prcsub.f` (Task 109).
//!
//! Ports `prcsub.f` (829 lines) — the county-to-subregion allocation
//! of populations and the application of emission factors +
//! seasonality factors. Structure mirrors [`super::county`]
//! ([`prccty.f`]) closely; the differences are:
//!
//! 1. An extra `fndasc` step finds the allocation-record slot for
//!    the SCC (`prcsub.f` :240).
//! 2. A linear search over `reglst(idxreg)` finds the subcounty
//!    marker for the current record (`prcsub.f` :247–:258).
//! 3. `alosub` allocates the state population to the subcounty
//!    (`prcsub.f` :263–:266) — the resulting `popsub` replaces the
//!    `popcty` of `prccty.f`.
//! 4. Output records carry the subcounty marker (`subcur`).
//!
//! Everything else — fuel-density branch, time-period setup, model-
//! year + tech-type loops — is line-for-line equivalent.
//!
//! [`prccty.f`]: super::county

use super::common::{
    accumulate_evap_iteration, accumulate_exhaust_iteration, fuel_density, hp_level_lookup,
    temporal_adjustment, time_period_setup, ActivityRecord, BmyKind, BmyRecord, DatRecord,
    EmissionsIterationResult, GeographyCallbacks, ModelYearAgedistResult, PopulationRecord,
    ProcessOutcome, ProcessOutput, ProcessWarning, RefuelingData, RetrofitFilter, RunOptions,
    SiRecord, TechLookup, WarningKind,
};
use crate::common::consts::{MXPOL, RMISS};
use crate::{Error, Result};

/// Per-subcounty record index passed to [`process_subcounty`].
///
/// `prcsub.f` accepts a single `icurec` argument; the allocation
/// callback (`alosub`) uses it to look up the per-record population
/// data. The Rust port carries the index alongside the record so
/// the allocation method can resolve it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubcountyRecordIndex(pub usize);

/// Process one subcounty-level population record. Ports `prcsub.f`.
///
/// See [`super::process_county`] for the overall design notes — this
/// function adds the subcounty-specific allocation step and otherwise
/// follows the same shape.
///
/// # Arguments
///
/// - `record_index`: original record index `icurec`, threaded
///   through to `alosub` in the callback. The Fortran source uses
///   it as an index into the population COMMON arrays.
/// - `record`: per-record COMMON-block reads. For subcounty
///   processing, `region_code` is the 5-character FIPS prefix of the
///   matched `reglst` entry (the trailing 5 hold the subcounty
///   marker).
/// - `cached_growth`: the `growth` argument to `prcsub.f`. A
///   sentinel value `< 0` (Fortran's `-9`) means "not yet computed";
///   the callback's `allocate_subcounty` must produce a real value
///   in that case.
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
    let mut output = ProcessOutput {
        fips: record.region_code.to_string(),
        subcounty: String::new(),
        emissions_day: vec![0.0; MXPOL],
        emsams_delta: Vec::new(),
        emsams_fips_index: None,
        ..ProcessOutput::default()
    };

    let fipin = record.region_code;

    // ---- prcsub.f :208–:212 — county-FIPS lookup. ----
    let Some(fips_idx) = callbacks.find_fips(fipin) else {
        return Ok(ProcessOutcome::Skipped(output));
    };
    callbacks.tally_county_record(fips_idx);

    // ---- prcsub.f :217–:228 — HP-level lookup. ----
    let hplev = hp_level_lookup(record.hp_range, &options.hp_levels);
    let hpval = record.hp_avg;
    output.hp_level = hplev;

    // ---- prcsub.f :240–:241 — fndasc. Missing → 7000 error path.
    //      The Fortran source writes an ERROR message to IOWSTD/IOWMSG
    //      and immediately falls through to 9999 with `ierr = IFAIL`
    //      (never reset). The Rust port surfaces this as a fatal
    //      Config error so callers can surface the same message. ----
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
        record_index.0,
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
    let popsub = alloc.population;

    // ---- prcsub.f :271–:277 — zero-population early-out. ----
    if popsub <= 0.0 {
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

    // ---- prcsub.f :282–:293 — exhaust tech-type lookup. ----
    let exhaust_tech = match callbacks.find_exhaust_tech(record.scc, hpval, options.tech_year) {
        Some(t) => t,
        None => {
            output.warnings.push(ProcessWarning {
                kind: WarningKind::MissingTechType,
                message: format!(
                    "WARNING:  Could not find any technology fractions for equipment: \
                     SCC code {scc} Average HP {hp:7.1} Skipping...",
                    scc = record.scc,
                    hp = hpval,
                ),
            });
            return Ok(ProcessOutcome::Skipped(output));
        }
    };

    // ---- prcsub.f :298–:309 — evap tech-type lookup. ----
    // Validated up-front; per-model-year loop re-queries with `tchmdyr`.
    let _evap_tech = match callbacks.find_evap_tech(record.scc, hpval, options.tech_year) {
        Some(t) => t,
        None => {
            output.warnings.push(ProcessWarning {
                kind: WarningKind::MissingTechType,
                message: format!(
                    "WARNING:  Could not find any evap technology fractions for equipment: \
                     SCC code {scc} Average HP {hp:7.1} Skipping...",
                    scc = record.scc,
                    hp = hpval,
                ),
            });
            return Ok(ProcessOutcome::Skipped(output));
        }
    };

    // ---- prcsub.f :313–:322 — fuel density. ----
    let denful = fuel_density(options.fuel);

    // ---- prcsub.f :328 — daymthf. ----
    let (daymthfac, mthf, dayf, ndays) = callbacks.day_month_factors(record.scc, fipin);

    // ---- prcsub.f :332–:343 — time-period factors. ----
    let tp = time_period_setup(options.sum_type, ndays, options.daily_mode, mthf, dayf);

    // ---- prcsub.f :348 — emsadj. ----
    let adjems = callbacks.emission_adjustments(record.scc, fipin, &daymthfac);

    // ---- prcsub.f :357–:359 — growth file required. ----
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

    // ---- prcsub.f :363–:383 — activity lookup. Missing → RMISS path. ----
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
            for (j, slot) in output.emissions_day.iter_mut().enumerate() {
                let j1 = j + 1;
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

    // ---- prcsub.f :404–:416 — modyr + agedist. ----
    let modyr_agedist = callbacks.model_year_and_agedist(
        idxact,
        record,
        fipin,
        idxgrw,
        options.episode_year,
        options.growth_year,
        popsub,
    )?;

    if options.retrofit_enabled {
        callbacks.filter_retrofits(RetrofitFilter::SccHp, record.scc, hpval, 0, "")?;
    }

    let mut poptot: f32 = 0.0;
    let mut acttot: f32 = 0.0;
    let mut strtot: f32 = 0.0;
    let mut fulcsm: f32 = 0.0;
    let mut unitsretro: f32 = 0.0;
    let mut evpoptot: f32 = 0.0;
    let mut evacttot: f32 = 0.0;
    let mut evstrtot: f32 = 0.0;

    let activity = callbacks.activity_record(idxact);

    // ---- prcsub.f :443–:747 — model-year loop. ----
    let iepyr = options.episode_year;
    let nyrlif = modyr_agedist.nyrlif;
    for iyr in (iepyr - nyrlif as i32 + 1)..=iepyr {
        let idxyr_one_based = iepyr - iyr + 1;
        let idxyr0 = (idxyr_one_based - 1) as usize;

        if modyr_agedist.modfrc.get(idxyr0).copied().unwrap_or(0.0) <= 0.0 {
            continue;
        }

        let mut fulbmytot: f32 = 0.0;
        let tchmdyr = iyr.min(options.tech_year);

        run_exhaust_block(
            record,
            options,
            callbacks,
            &exhaust_tech,
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
            popsub,
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
            popsub,
            hplev,
            ndays,
            &mut evpoptot,
            &mut evacttot,
            &mut evstrtot,
            fulbmytot,
            &mut output,
        )?;
    }

    // ---- prcsub.f :751–:755 — emsams fold-in. ----
    let mut emsams_delta = vec![0.0_f32; MXPOL];
    for (i, &v) in output.emissions_day.iter().enumerate() {
        if v > 0.0 {
            emsams_delta[i] = v;
        }
    }
    output.emsams_delta = emsams_delta;
    output.emsams_fips_index = Some(fips_idx);

    // ---- prcsub.f :768 — fracretro. ----
    let fracretro = if poptot > 0.0 {
        unitsretro / poptot
    } else {
        0.0
    };

    // ---- prcsub.f :772–:774 — wrtdat. ----
    output.dat_records.push(DatRecord {
        fips: fipin.to_string(),
        subcounty: subcur.clone(),
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

    let _ = (strtot, evpoptot, evacttot, evstrtot);

    Ok(ProcessOutcome::Success(output))
}

#[allow(clippy::too_many_arguments)]
fn run_exhaust_block<C: GeographyCallbacks + ?Sized>(
    record: &PopulationRecord<'_>,
    options: &RunOptions,
    callbacks: &mut C,
    // Validated up-front; per-year re-query happens inside.
    _base_exhaust_tech: &TechLookup,
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
    popsub: f32,
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
    let _ = idxact;

    let exhaust_tech = match callbacks.find_exhaust_tech(record.scc, record.hp_avg, tchmdyr) {
        Some(t) => t,
        None => return Ok(()),
    };

    let n_tech = exhaust_tech.tech_names.len();

    let factors = callbacks.compute_exhaust_factors(
        record.scc,
        &exhaust_tech.tech_names,
        &exhaust_tech.tech_fractions,
        tchmdyr,
        idxyr0,
        0,
    )?;

    if options.retrofit_enabled {
        callbacks.filter_retrofits(RetrofitFilter::ModelYear, "", 0.0, iyr, "")?;
    }

    let activity_unit = activity.activity_unit;
    let tpltmp_exhaust = temporal_adjustment(activity_unit, tplfac);

    for tech_idx in 0..n_tech {
        let tchfrc = exhaust_tech.tech_fractions[tech_idx];
        if tchfrc <= 0.0 {
            continue;
        }
        let tech_name = exhaust_tech.tech_names[tech_idx].clone();

        let popbmy = popsub * modyr_agedist.modfrc[idxyr0] * tchfrc;

        let mut fracretrobmy: f32 = 0.0;
        let mut unitsretrobmy: f32 = 0.0;

        if options.retrofit_enabled {
            callbacks.filter_retrofits(RetrofitFilter::TechType, "", 0.0, 0, &tech_name)?;
            let surviving = callbacks.surviving_retrofits();
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
            popsub,
            ndays,
            idxact,
        )?;
        accumulate_iteration_into_output(&iter, output);

        // ---- prcsub.f :552–:556 — actbmy / fulbmy. ----
        let actbmy = modyr_agedist.actadj[idxyr0]
            * popsub
            * modyr_agedist.modfrc[idxyr0]
            * tplful
            * tchfrc
            * adjtime;
        let fulbmy = tplful
            * popsub
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

    accumulate_exhaust_iteration(
        poptot,
        acttot,
        strtot,
        popsub,
        modyr_agedist.modfrc[idxyr0],
        modyr_agedist.actadj[idxyr0],
        modyr_agedist.stradj[idxyr0],
        tplful,
        adjtime,
    );

    Ok(())
}

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
    popsub: f32,
    hplev: f32,
    ndays: i32,
    evpoptot: &mut f32,
    evacttot: &mut f32,
    evstrtot: &mut f32,
    fulbmytot: f32,
    output: &mut ProcessOutput,
) -> Result<()> {
    let evap_tech = match callbacks.find_evap_tech(record.scc, record.hp_avg, tchmdyr) {
        Some(t) => t,
        None => return Ok(()),
    };

    let n_evtech = evap_tech.tech_names.len();

    let mut refueling_data: Vec<RefuelingData> = vec![RefuelingData::default(); n_evtech];
    #[allow(clippy::needless_range_loop)] // multiple parallel arrays indexed by i
    for i in 0..n_evtech {
        if !options.spillage_enabled {
            continue;
        }
        let default_rfm = callbacks.find_refueling(record.scc, record.hp_avg, "ALL       ");
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

    #[allow(clippy::needless_range_loop)] // multiple parallel arrays indexed by tech_idx
    for tech_idx in 0..n_evtech {
        let evtchfrc = evap_tech.tech_fractions[tech_idx];
        if evtchfrc <= 0.0 {
            continue;
        }
        let tech_name = evap_tech.tech_names[tech_idx].clone();

        let fulbmy = fulbmytot * evtchfrc;

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
            popsub,
            ndays,
            fulbmy,
        )?;
        accumulate_iteration_into_output(&iter, output);

        let popbmy = popsub * modyr_agedist.modfrc[idxyr0] * evtchfrc;
        let actbmy = modyr_agedist.actadj[idxyr0]
            * popsub
            * modyr_agedist.modfrc[idxyr0]
            * tplful
            * evtchfrc
            * adjtime;

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

    accumulate_evap_iteration(
        evpoptot,
        evacttot,
        evstrtot,
        popsub,
        modyr_agedist.modfrc[idxyr0],
        modyr_agedist.actadj[idxyr0],
        modyr_agedist.stradj[idxyr0],
        tplful,
        adjtime,
    );

    Ok(())
}

fn accumulate_iteration_into_output(iter: &EmissionsIterationResult, output: &mut ProcessOutput) {
    for (i, &delta) in iter.emsday_delta.iter().enumerate() {
        if i >= output.emissions_day.len() {
            break;
        }
        output.emissions_day[i] += delta;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::consts::MXHPC;
    use crate::emissions::exhaust::FuelKind;
    use crate::geography::common::{
        ActivityUnit, EvapFactorsLookup, ExhaustFactorsLookup, NoopCallbacks, SumType,
    };

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

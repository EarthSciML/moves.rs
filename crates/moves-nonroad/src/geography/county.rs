//! County-level processing — port of `prccty.f` (Task 109).
//!
//! Ports `prccty.f` (790 lines) — the state-to-county allocation of
//! populations and the application of emission factors + seasonality
//! factors. The Rust port stays line-by-line close to the Fortran
//! source so the numerical-fidelity validation (Task 115) can compare
//! the structure as well as the outputs.
//!
//! The orchestration is broken into three phases:
//!
//! 1. **Record validation** — FIPS lookup, zero-population check,
//!    tech-type discovery, fuel density.
//! 2. **Per-record setup** — time-period factors, daily adjustments,
//!    growth + model-year + age-distribution, retrofit filter
//!    initialization.
//! 3. **Model-year loop** — for each absolute year `iyr` in
//!    `iepyr - nyrlif + 1 ..= iepyr`, compute the exhaust and evap
//!    contributions and update the running accumulators.
//!
//! Each phase translates a contiguous block of Fortran lines; the
//! line numbers are quoted in inline comments.

use super::common::{
    accumulate_evap_iteration, accumulate_exhaust_iteration, fuel_density, hp_level_lookup,
    temporal_adjustment, time_period_setup, ActivityRecord, BmyKind, BmyRecord, DatRecord,
    EmissionsIterationResult, GeographyCallbacks, ModelYearAgedistResult, PopulationRecord,
    ProcessOutcome, ProcessOutput, ProcessWarning, RefuelingData, RetrofitFilter, RunOptions,
    SiRecord, TechLookup, WarningKind,
};
use crate::common::consts::{MXPOL, RMISS};
use crate::{Error, Result};

/// Process one county-level population record. Ports `prccty.f`.
///
/// The Rust port turns the Fortran `IFAIL` / `ISUCES` / `ISKIP`
/// trinity into [`ProcessOutcome`]: `ISUCES` → `Ok(Success(…))`,
/// `ISKIP` → `Ok(Skipped(…))`, `IFAIL` (or any I/O-style fatal) →
/// `Err(…)`. The `emsday`, `emsbmy`, and tally side effects the
/// Fortran source performs in-place are surfaced as
/// [`ProcessOutput`] fields.
///
/// # Arguments
///
/// - `record`: per-record COMMON-block reads (`/popdat/` slot).
/// - `options`: run-level settings (`/optdat/`, `/eqpdat/`, `/io/`).
/// - `callbacks`: dependency surface that supplies the helpers
///   `prccty.f` calls (`fndtch`, `fndevtch`, `emfclc`, `evemfclc`,
///   `daymthf`, `emsadj`, `getgrw`, `grwfac`, `modyr`, `agedist`,
///   `clcems`, `clcevems`, `fndrtrft`, `fndrfm`, `fndchr`, …).
///
/// # Failures
///
/// - [`Error::Config`] when `options.growth_enabled` is `false` —
///   the Fortran source's `7003` error path triggers an explicit
///   I/O message and exits.
/// - Errors propagated from any callback method (`clcems`-style
///   numerics, growth-data I/O, etc.).
pub fn process_county<C: GeographyCallbacks + ?Sized>(
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

    // ---- prccty.f :204–:209 — county-FIPS lookup. ISKIP early-out. ----
    let fipin = record.region_code;
    let Some(fips_idx) = callbacks.find_fips(fipin) else {
        return Ok(ProcessOutcome::Skipped(output));
    };
    callbacks.tally_county_record(fips_idx);

    // ---- prccty.f :214–:225 — HP-level lookup. ----
    let hplev = hp_level_lookup(record.hp_range, &options.hp_levels);
    let hpval = record.hp_avg;
    output.hp_level = hplev;

    // ---- prccty.f :236–:242 — zero-population early-out. ----
    if record.population <= 0.0 {
        output.dat_records.push(DatRecord {
            fips: fipin.to_string(),
            subcounty: String::new(),
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

    let popcty = record.population;

    // ---- prccty.f :251–:262 — exhaust tech-type lookup. ----
    let exhaust_tech = match callbacks.find_exhaust_tech(record.scc, hpval, options.tech_year) {
        Some(t) => t,
        None => {
            output.warnings.push(ProcessWarning {
                kind: WarningKind::MissingTechType,
                message: format!(
                    "WARNING: Could not find any exhaust technology fractions for equipment: \
                     SCC code {scc} Average HP {hp:7.1} Skipping...",
                    scc = record.scc,
                    hp = hpval,
                ),
            });
            return Ok(ProcessOutcome::Skipped(output));
        }
    };

    // ---- prccty.f :267–:278 — evap tech-type lookup. ----
    // Validated up-front so we can ISKIP early; the per-model-year
    // loop re-queries with `tchmdyr` (`prccty.f` :573) to match the
    // Fortran semantics, so the validated lookup is intentionally
    // unused beyond this branch.
    let _evap_tech = match callbacks.find_evap_tech(record.scc, hpval, options.tech_year) {
        Some(t) => t,
        None => {
            output.warnings.push(ProcessWarning {
                kind: WarningKind::MissingTechType,
                message: format!(
                    "WARNING: Could not find any evap technology fractions for equipment: \
                     SCC code {scc} Average HP {hp:7.1} Skipping...",
                    scc = record.scc,
                    hp = hpval,
                ),
            });
            return Ok(ProcessOutcome::Skipped(output));
        }
    };

    // ---- prccty.f :282–:291 — fuel density. ----
    let denful = fuel_density(options.fuel);

    // ---- prccty.f :297 — daymthf. ----
    let (daymthfac, mthf, dayf, ndays) = callbacks.day_month_factors(record.scc, fipin);

    // ---- prccty.f :301–:312 — time-period factors. ----
    let tp = time_period_setup(options.sum_type, ndays, options.daily_mode, mthf, dayf);

    // ---- prccty.f :317 — emsadj. ----
    let adjems = callbacks.emission_adjustments(record.scc, fipin, &daymthfac);

    // ---- prccty.f :326–:328 — growth file required for any non-base-year run. ----
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

    // ---- prccty.f :332–:352 — activity lookup. Missing → RMISS path. ----
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
            // Fortran :344–:346: emsday(j) = RMISS for j outside [IDXDIU, IDXRLS].
            // IDXDIU = 8 (slot 7), IDXRLS = 17 (slot 16). The exclusion gates
            // are inclusive in Fortran indices; mirrored as 1-based here.
            for (j, slot) in output.emissions_day.iter_mut().enumerate() {
                let j1 = j + 1; // back to 1-based for the comparison.
                if !(8..=17).contains(&j1) {
                    *slot = RMISS;
                }
            }
            output.dat_records.push(DatRecord {
                fips: fipin.to_string(),
                subcounty: String::new(),
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

    // ---- prccty.f :356–:357 — getgrw. ----
    // The Fortran source loads the growth-factor data into the
    // growth-state COMMON; the Rust port lets the callback handle it.
    // We don't have a separate `load_growth_data` trait method —
    // `model_year_and_agedist` is expected to load whatever it needs
    // internally. (If the production impl needs a separate hook, it
    // can extend the trait.)
    //
    // ---- prccty.f :373–:385 — modyr + agedist. ----
    let modyr_agedist = callbacks.model_year_and_agedist(
        idxact,
        record,
        fipin,
        idxgrw,
        options.episode_year,
        options.growth_year,
        popcty,
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

    // ---- prccty.f :412–:716 — model-year loop. ----
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
            &exhaust_tech,
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
            popcty,
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
            iyr,
            tchmdyr,
            idxyr0,
            &modyr_agedist,
            &activity,
            &adjems,
            tp.tplfac,
            tp.tplful,
            tp.adjtime,
            popcty,
            hplev,
            ndays,
            &mut evpoptot,
            &mut evacttot,
            &mut evstrtot,
            fulbmytot,
            &mut output,
        )?;
    }

    // ---- prccty.f :720–:724 — emsams fold-in. ----
    let mut emsams_delta = vec![0.0_f32; MXPOL];
    for (i, &v) in output.emissions_day.iter().enumerate() {
        if v > 0.0 {
            emsams_delta[i] = v;
        }
    }
    output.emsams_delta = emsams_delta;
    output.emsams_fips_index = Some(fips_idx);

    // ---- prccty.f :737 — fracretro = unitsretro / poptot ----
    let fracretro = if poptot > 0.0 {
        unitsretro / poptot
    } else {
        0.0
    };

    // ---- prccty.f :741–:743 — wrtdat. ----
    output.dat_records.push(DatRecord {
        fips: fipin.to_string(),
        subcounty: String::new(),
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
    // Fortran but the source code does not surface them to the
    // wrtdat record. They are used by the SI report (wrtsi.f) which
    // ports separately in Task 114. We discard them here.
    let _ = (strtot, evpoptot, evacttot, evstrtot);

    Ok(ProcessOutcome::Success(output))
}

/// Body of the exhaust block (`prccty.f` :433–:562). Split out so
/// `process_county` reads top-to-bottom without one routine spilling
/// past three screens.
#[allow(clippy::too_many_arguments)]
fn run_exhaust_block<C: GeographyCallbacks + ?Sized>(
    record: &PopulationRecord<'_>,
    options: &RunOptions,
    callbacks: &mut C,
    // Validated up-front; the per-year loop re-queries with `tchmdyr`,
    // so the early lookup is intentionally unused inside this helper.
    _base_exhaust_tech: &TechLookup,
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
    popcty: f32,
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
    let _ = denful;
    let _ = idxact;

    // ---- prccty.f :440–:445 — re-resolve idxtch for tchmdyr. ----
    let exhaust_tech = match callbacks.find_exhaust_tech(record.scc, record.hp_avg, tchmdyr) {
        Some(t) => t,
        // Fortran source does not error on a per-year miss — it
        // would silently fall through. The base lookup above
        // already accepted the SCC, so a miss here is a data bug;
        // we still skip the year to avoid a hard crash.
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

        let popbmy = popcty * modyr_agedist.modfrc[idxyr0] * tchfrc;

        let mut fracretrobmy: f32 = 0.0;
        let mut unitsretrobmy: f32 = 0.0;

        // ---- prccty.f :488–:496 — retrofit type 3 filter + clcrtrft. ----
        if options.retrofit_enabled {
            callbacks.filter_retrofits(RetrofitFilter::TechType, "", 0.0, 0, &tech_name)?;
            let surviving = callbacks.surviving_retrofits();
            // Run the already-ported retrofit calculator. The
            // accumulator pollutant_reduction_fraction is owned by
            // the callback's RetrofitState; we just call to populate
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
            popcty,
            ndays,
            idxact,
        )?;
        accumulate_iteration_into_output(&iter, output);

        // ---- prccty.f :520–:524 — actbmy / fulbmy ----
        // Fortran: actbmy = actadj(idxyr) * popcty * modfrc(idxyr) * tplful * tchfrc(idxtch,i) * adjtime
        // (the extra `*` at line 521 is a continuation glitch — the
        // line `* tplful * tchfrc(idxtch,i) * adjtime` is the
        // continuation of the previous expression.)
        let actbmy = modyr_agedist.actadj[idxyr0]
            * popcty
            * modyr_agedist.modfrc[idxyr0]
            * tplful
            * tchfrc
            * adjtime;
        let fulbmy = tplful
            * popcty
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
                subcounty: String::new(),
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
        popcty,
        modyr_agedist.modfrc[idxyr0],
        modyr_agedist.actadj[idxyr0],
        modyr_agedist.stradj[idxyr0],
        tplful,
        adjtime,
    );

    Ok(())
}

/// Body of the evap block (`prccty.f` :566–:716). Same shape as the
/// exhaust block, but with the evap helpers and the refueling
/// lookup at the front.
#[allow(clippy::too_many_arguments)]
fn run_evap_block<C: GeographyCallbacks + ?Sized>(
    record: &PopulationRecord<'_>,
    options: &RunOptions,
    callbacks: &mut C,
    _iyr: i32,
    tchmdyr: i32,
    idxyr0: usize,
    modyr_agedist: &ModelYearAgedistResult,
    activity: &ActivityRecord,
    adjems: &crate::emissions::exhaust::AdjustmentTable,
    tplfac: f32,
    tplful: f32,
    adjtime: f32,
    popcty: f32,
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
        // `j = 16 - 8 + 2 - 3 = 7`. We use the 7th character of
        // the evap tech name (0-based offset 6).
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
            popcty,
            ndays,
            fulbmy,
        )?;
        accumulate_iteration_into_output(&iter, output);

        // ---- prccty.f :679–:681 — popbmy / actbmy. ----
        let popbmy = popcty * modyr_agedist.modfrc[idxyr0] * evtchfrc;
        let actbmy = modyr_agedist.actadj[idxyr0]
            * popcty
            * modyr_agedist.modfrc[idxyr0]
            * tplful
            * evtchfrc
            * adjtime;

        // ---- prccty.f :685–:693 — wrtbmy (evap). ----
        if options.write_bmy_evap {
            output.bmy_records.push(BmyRecord {
                fips: record.region_code.to_string(),
                subcounty: String::new(),
                scc: record.scc.to_string(),
                hp_level: hplev,
                tech_name: tech_name.clone(),
                model_year: _iyr,
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
        popcty,
        modyr_agedist.modfrc[idxyr0],
        modyr_agedist.actadj[idxyr0],
        modyr_agedist.stradj[idxyr0],
        tplful,
        adjtime,
    );

    Ok(())
}

/// Fold one `EmissionsIterationResult` into the running
/// `output.emissions_day` accumulator.
fn accumulate_iteration_into_output(iter: &EmissionsIterationResult, output: &mut ProcessOutput) {
    for (i, &delta) in iter.emsday_delta.iter().enumerate() {
        if i >= output.emissions_day.len() {
            break;
        }
        // Fortran semantics: when the iteration produces RMISS for a
        // pollutant slot, the day-level slot becomes RMISS too. The
        // `compute_*_iteration` callback decides this on its end —
        // we just add.
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

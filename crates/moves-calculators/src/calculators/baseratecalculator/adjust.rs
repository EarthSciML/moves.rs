//! The per-fuel-block rate adjustment — the heart of the Base Rate Calculator.
//!
//! Ports the Go `streamBaseRateByAge` / `streamBaseRate` row expansion, the
//! `calculateAndAccumulate` adjustment sequence, and the three pure
//! adjustment equations `startTempAdjust`, `generalTempAdjust`,
//! `calculateNOxK`.
//!
//! # The adjustment sequence
//!
//! [`process_fuel_block`] applies, in order: extended-idle / APU / shorepower
//! hourly scaling, start-temperature adjustment, general-fuel-ratio scaling,
//! criteria-ratio scaling, temperature + humidity adjustment, air-conditioning
//! addition, I/M blending, the emission-rate adjustment, the E85 THC
//! duplication, and the EV-efficiency divisor — exactly the order of the Go
//! `calculateAndAccumulate` loop body.
//!
//! # Fidelity note — the shorepower process rewrite
//!
//! The shorepower step rewrites `process_id` `91 → 93` but the Go does **not**
//! re-run `CalcIDs`, so `pol_process_id` stays at its process-91 value for
//! the rest of the sequence. Every later table lookup keyed on
//! `pol_process_id` therefore uses the process-91 key while every branch
//! gated on `process_id` sees `93`. The port reproduces this exactly: it
//! mutates `process_id` without recomputing `pol_process_id`.

use super::model::{
    BaseRate, BlockKey, FuelBlock, GeneralFuelRatioKey, ImCoverageKey, ModelYearFuelKey,
    ModuleFlags, NoxHumidityAdjustDetail, OpModeRates, PolProcSourceRegFuelMyKey,
    PollutantProcessMappedModelYearKey, RunConstants, StartTempAdjustmentDetail,
    StartTempAdjustmentKey, TemperatureAdjustmentDetail, TemperatureAdjustmentKey, ZoneAcFactorKey,
    ZoneMonthHourDetail, ZoneMonthHourKey,
};
use super::setup::{BaseRateRow, FuelSupplyKey, PreparedTables};
use moves_framework::Error;

/// Apply the start-temperature adjustment equations — the Go
/// `StartTempAdjustmentDetail.startTempAdjust`.
///
/// * `polProcessID` `11202` / `11802` — a multiplicative PM form.
/// * `is_log` — an additive logarithmic form weighted by `weight_fraction`.
/// * otherwise — an additive cubic-polynomial form.
///
/// The Go's `POLY` branch and its fallback branch are byte-identical, so the
/// equation depends only on `is_log`; `is_poly` is parsed for fidelity but
/// never changes the result.
#[must_use]
pub fn start_temp_adjust(
    detail: &StartTempAdjustmentDetail,
    base_value: f64,
    pol_process_id: i32,
    weight_fraction: f64,
    temperature: f64,
) -> f64 {
    if pol_process_id == 11202 || pol_process_id == 11802 {
        // rate = rate * B * exp(A * (72 - least(temp, 72))) + C
        base_value * detail.term_b * (detail.term_a * (72.0 - temperature.min(72.0))).exp()
            + detail.term_c
    } else if detail.is_log {
        // rate = rate + weight * (B * exp(A * (least(temp, 75) - 75)) + C)
        base_value
            + weight_fraction
                * (detail.term_b * (detail.term_a * (temperature.min(75.0) - 75.0)).exp()
                    + detail.term_c)
    } else {
        // rate = rate + weight * d * (A + d * (B + d * C)), d = least(temp,75) - 75
        let d = temperature.min(75.0) - 75.0;
        base_value + weight_fraction * d * (detail.term_a + d * (detail.term_b + d * detail.term_c))
    }
}

/// Apply the general temperature + humidity adjustment — the Go
/// `TemperatureAdjustmentDetail.generalTempAdjust`.
///
/// Four cases keyed on the row's process / pollutant / fuel type:
///
/// * PM (process 1/2, pollutant 118/112) — a multiplicative exponential
/// capped at `1.0` above 72 °F.
/// * EV running energy (process 1, fuel type 9, pollutant 91) — a quadratic
/// cold-temperature adjustment, suppressed for warm light-duty conditions.
/// * NOx (process 1/90/91, pollutant 3) — a fuel-type-specific temperature
/// term multiplied by the humidity factor `k`.
/// * everything else — the standard quadratic temperature term.
///
/// `key` supplies the process / pollutant / fuel-type / source-type ids;
/// `baserate` is the pre-adjustment mean base rate (the EV branch reads its
/// sign).
#[must_use]
pub fn general_temp_adjust(
    detail: &TemperatureAdjustmentDetail,
    key: &BlockKey,
    k: f64,
    temperature: f64,
    heat_index: f64,
    baserate: f64,
    is_project: bool,
) -> f64 {
    let process_id = key.process_id;
    let pollutant_id = key.pollutant_id;

    // PM: process 1/2, pollutant 118/112.
    if (process_id == 1 || process_id == 2) && (pollutant_id == 118 || pollutant_id == 112) {
        if temperature <= 72.0 {
            return (detail.term_a * (72.0 - temperature)).exp();
        }
        return 1.0;
    }

    // EV running energy: process 1, fuel type 9, pollutant 91.
    if process_id == 1 && key.fuel_type_id == 9 && pollutant_id == 91 {
        let mut adj = (temperature - 72.0) * (detail.term_a + detail.term_b * (temperature - 72.0));
        if adj < 0.0 {
            adj = 0.0;
        }
        // Light-duty AC usage is set by the heat index, not this cold term.
        if key.source_type_id < 40 && heat_index > 67.0 {
            adj = 0.0;
        }
        // At project scale a negative base rate flips the adjustment sign so
        // regen braking is not assumed more effective when cold.
        if is_project && baserate < 0.0 {
            return 1.0 - adj;
        }
        return 1.0 + adj;
    }

    // NOx: process 1/90/91, pollutant 3.
    if (process_id == 1 || process_id == 90 || process_id == 91) && pollutant_id == 3 {
        let temp_adjust = if key.fuel_type_id == 2 {
            // No diesel adjustment above 25 °C (77 °F).
            if temperature > 77.0 {
                0.0
            } else {
                (77.0 - temperature) * detail.term_a
            }
        } else {
            (temperature - 75.0) * (detail.term_a + (temperature - 75.0) * detail.term_b)
        };
        return (1.0 + temp_adjust) * k;
    }

    // Standard quadratic temperature term.
    1.0 + (temperature - 75.0) * (detail.term_a + (temperature - 75.0) * detail.term_b)
}

/// Bound `value` into `[low, up]` by the Go `calculateNOxK` two-step
/// (`if v < low { v = low }; if v > up { v = up }`).
///
/// Unlike [`f64::clamp`] this does not panic when `low > up` — it yields
/// `up`, exactly as the Go's two sequential assignments do. (Valid
/// `NOxHumidityAdjust` rows always satisfy `low <= up`; the faithful form is
/// kept regardless.)
fn bound_humidity(value: f64, low: f64, up: f64) -> f64 {
    let value = if value < low { low } else { value };
    if value > up {
        up
    } else {
        value
    }
}

/// Compute the NOx humidity-adjustment factor `k` — the Go `calculateNOxK`.
///
/// `"CFR 86"` bounds the gram-per-kilogram specific humidity and applies a
/// linear correction; `"CFR 1065"` bounds the water mole fraction and applies
/// a reciprocal correction. Any other equation name yields `1.0` (no
/// adjustment).
#[must_use]
pub fn calculate_nox_k(zmh: &ZoneMonthHourDetail, nha: &NoxHumidityAdjustDetail) -> f64 {
    match nha.humidity_nox_eq.as_str() {
        "CFR 86" => {
            let spec_hum = bound_humidity(
                zmh.specific_humidity,
                nha.humidity_low_bound,
                nha.humidity_up_bound,
            );
            1.0 - nha.humidity_term_a * (spec_hum - 10.71)
        }
        "CFR 1065" => {
            let mole_frac = bound_humidity(
                zmh.mol_water_fraction,
                nha.humidity_low_bound,
                nha.humidity_up_bound,
            );
            1.0 / (nha.humidity_term_a * mole_frac + nha.humidity_term_b)
        }
        _ => 1.0,
    }
}

/// Expand the `BaseRate` / `BaseRateByAge` rows into [`FuelBlock`]s.
///
/// Ports `streamBaseRateByAge` / `streamBaseRate`. Each row becomes one fuel
/// block whose [`OpModeRates::base_rates`] holds one [`BaseRate`] per fuel
/// formulation supplied to the row's `(county, year, month, fuel type)` cell.
///
/// When `panic_on_missing_supply` is `false` (the age path, `streamBaseRateByAge`),
/// a row with no matching fuel supply is silently dropped — matching the Go
/// `return` on a nil lookup. When `panic_on_missing_supply` is `true` (the
/// non-age path, `streamBaseRate`), a missing supply is a fatal data error and
/// [`Error::MissingContext`] is returned — matching the Go `panic` there.
#[must_use]
pub fn build_fuel_blocks(
    rows: &[BaseRateRow],
    prepared: &PreparedTables,
    constants: &RunConstants,
    panic_on_missing_supply: bool,
) -> Result<Vec<FuelBlock>, Error> {
    let mut blocks = Vec::with_capacity(rows.len());
    for row in rows {
        let fuel_supply = prepared.fuel_supply.get(&FuelSupplyKey {
            county_id: constants.county_id,
            year_id: constants.year_id,
            month_id: constants.month_id,
            fuel_type_id: row.fuel_type_id,
        });
        let Some(fuel_supply) = fuel_supply else {
            if panic_on_missing_supply {
                return Err(Error::MissingContext {
                    what: format!(
                        "fuel supply for county_id={} year_id={} month_id={} fuel_type_id={}",
                        constants.county_id,
                        constants.year_id,
                        constants.month_id,
                        row.fuel_type_id
                    ),
                });
            }
            continue;
        };

        let mut key = BlockKey {
            year_id: constants.year_id,
            month_id: constants.month_id,
            day_id: row.hour_day_id % 10,
            hour_id: row.hour_day_id / 10,
            state_id: constants.state_id,
            county_id: constants.county_id,
            zone_id: constants.zone_id,
            link_id: constants.link_id,
            road_type_id: row.road_type_id,
            source_type_id: row.source_type_id,
            reg_class_id: row.reg_class_id,
            fuel_type_id: row.fuel_type_id,
            model_year_id: row.model_year_id,
            avg_speed_bin_id: row.avg_speed_bin_id,
            pollutant_id: row.pollutant_id,
            process_id: row.process_id,
            pol_process_id: 0,
            hour_day_id: 0,
            age_id: 0,
        };
        key.calc_ids();

        let base_rates = fuel_supply
            .iter()
            .map(|fsd| BaseRate {
                fuel_sub_type_id: fsd.fuel_sub_type_id,
                fuel_formulation_id: fsd.fuel_formulation_id,
                market_share: fsd.market_share,
                mean_base_rate: row.mean_base_rate,
                mean_base_rate_im: row.mean_base_rate_im,
                emission_rate: row.emission_rate,
                emission_rate_im: row.emission_rate_im,
                mean_base_rate_ac_adj: row.mean_base_rate_ac_adj,
                mean_base_rate_im_ac_adj: row.mean_base_rate_im_ac_adj,
                emission_rate_ac_adj: row.emission_rate_ac_adj,
                emission_rate_im_ac_adj: row.emission_rate_im_ac_adj,
            })
            .collect();

        blocks.push(FuelBlock {
            key,
            op_mode: Some(OpModeRates {
                op_mode_id: row.op_mode_id,
                general_fraction: row.op_mode_fraction,
                general_fraction_rate: row.op_mode_fraction_rate,
                base_rates,
            }),
            emissions: Vec::new(),
        });
    }
    Ok(blocks)
}

/// Scale the four mean-base-rate fields of every base rate in place — the Go
/// extended-idle / APU / shorepower hourly adjustment, which never touches the
/// `EmissionRate*` fields ("a quirk of the APU calculations").
fn scale_mean_base_rates(op_mode: &mut OpModeRates, factor: f64) {
    for br in &mut op_mode.base_rates {
        br.mean_base_rate *= factor;
        br.mean_base_rate_im *= factor;
        br.mean_base_rate_ac_adj *= factor;
        br.mean_base_rate_im_ac_adj *= factor;
    }
}

/// Scale all eight rate fields of one base rate in place — the per-base-rate
/// factor of the general-fuel-ratio, criteria-ratio and temperature
/// adjustments, each of which computes a distinct factor per fuel formulation.
fn scale_base_rate(br: &mut BaseRate, factor: f64) {
    br.mean_base_rate *= factor;
    br.mean_base_rate_im *= factor;
    br.emission_rate *= factor;
    br.emission_rate_im *= factor;
    br.mean_base_rate_ac_adj *= factor;
    br.mean_base_rate_im_ac_adj *= factor;
    br.emission_rate_ac_adj *= factor;
    br.emission_rate_im_ac_adj *= factor;
}

/// Apply the full adjustment sequence to one fuel block — the body of the Go
/// `calculateAndAccumulate` loop.
///
/// Returns the block plus, when the E85 THC step fires, one extra block
/// carrying the `10000`-offset pollutant. The returned blocks are ready for
/// accumulation into the unique-key map.
#[must_use]
pub fn process_fuel_block(
    mut fb: FuelBlock,
    prepared: &PreparedTables,
    flags: &ModuleFlags,
    gpa_fract: f64,
) -> Vec<FuelBlock> {
    let ppmy = prepared
        .pollutant_process_mapped_model_year
        .get(&PollutantProcessMappedModelYearKey {
            pol_process_id: fb.key.pol_process_id,
            model_year_id: fb.key.model_year_id,
        })
        .copied();
    let zmh = prepared
        .zone_month_hour
        .get(&ZoneMonthHourKey {
            month_id: fb.key.month_id,
            zone_id: fb.key.zone_id,
            hour_id: fb.key.hour_id,
        })
        .copied();
    let nha = prepared.nox_humidity_adjust.get(&fb.key.fuel_type_id);
    let fuel_type_known = prepared.fuel_types.contains(&fb.key.fuel_type_id);

    let op_mode = fb
        .op_mode
        .as_mut()
        .expect("build_fuel_blocks always sets op_mode");
    let op_mode_id = op_mode.op_mode_id;
    let general_fraction = op_mode.general_fraction;
    let general_fraction_rate = op_mode.general_fraction_rate;
    let my_fuel_key = ModelYearFuelKey {
        model_year_id: fb.key.model_year_id,
        fuel_type_id: fb.key.fuel_type_id,
    };

    // Extended Idle (process 90): scale mean base rates by the opMode-200
    // fraction. Emission rates are left untouched.
    if fb.key.process_id == 90 && !prepared.extended_idle_emission_rate_fraction.is_empty() {
        if let Some(&adjust) = prepared
            .extended_idle_emission_rate_fraction
            .get(&my_fuel_key)
        {
            scale_mean_base_rates(op_mode, adjust);
        }
    }
    // APU (process 91, opMode 201): scale mean base rates by the opMode-201
    // fraction.
    if fb.key.process_id == 91
        && op_mode_id == 201
        && !prepared.apu_emission_rate_fraction.is_empty()
    {
        if let Some(&adjust) = prepared.apu_emission_rate_fraction.get(&my_fuel_key) {
            scale_mean_base_rates(op_mode, adjust);
        }
    }
    // Shorepower (process 91, opMode 203): retag the process 91 -> 93 so the
    // output is not aggregated with APU, then scale mean base rates. The
    // process is retagged whether or not a fraction is found; pol_process_id
    // is deliberately not recomputed (see the module docs).
    if fb.key.process_id == 91
        && op_mode_id == 203
        && !prepared.shorepower_emission_rate_fraction.is_empty()
    {
        fb.key.process_id = 93;
        if let Some(&adjust) = prepared.shorepower_emission_rate_fraction.get(&my_fuel_key) {
            scale_mean_base_rates(op_mode, adjust);
        }
    }

    // Start temperature adjustment (process 2).
    if fb.key.process_id == 2 {
        if let (Some(zmh), true, Some(ppmy)) = (zmh, fuel_type_known, ppmy) {
            let sta = prepared
                .start_temp_adjustment
                .get(&StartTempAdjustmentKey {
                    fuel_type_id: fb.key.fuel_type_id,
                    pol_process_id: fb.key.pol_process_id,
                    model_year_group_id: ppmy.model_year_group_id,
                    op_mode_id,
                })
                .copied();
            if let Some(sta) = sta {
                let pp = fb.key.pol_process_id;
                let temp = zmh.temperature;
                for br in &mut op_mode.base_rates {
                    // Mean base rates weight by the inventory opMode fraction;
                    // emission rates weight by the rate opMode fraction.
                    br.mean_base_rate =
                        start_temp_adjust(&sta, br.mean_base_rate, pp, general_fraction, temp);
                    br.mean_base_rate_im =
                        start_temp_adjust(&sta, br.mean_base_rate_im, pp, general_fraction, temp);
                    br.emission_rate =
                        start_temp_adjust(&sta, br.emission_rate, pp, general_fraction_rate, temp);
                    br.emission_rate_im = start_temp_adjust(
                        &sta,
                        br.emission_rate_im,
                        pp,
                        general_fraction_rate,
                        temp,
                    );
                    br.mean_base_rate_ac_adj = start_temp_adjust(
                        &sta,
                        br.mean_base_rate_ac_adj,
                        pp,
                        general_fraction,
                        temp,
                    );
                    br.mean_base_rate_im_ac_adj = start_temp_adjust(
                        &sta,
                        br.mean_base_rate_im_ac_adj,
                        pp,
                        general_fraction,
                        temp,
                    );
                    br.emission_rate_ac_adj = start_temp_adjust(
                        &sta,
                        br.emission_rate_ac_adj,
                        pp,
                        general_fraction_rate,
                        temp,
                    );
                    br.emission_rate_im_ac_adj = start_temp_adjust(
                        &sta,
                        br.emission_rate_im_ac_adj,
                        pp,
                        general_fraction_rate,
                        temp,
                    );
                }
            }
        }
    }

    // General fuel ratio: blend the normal and GPA fuel-effect ratios by the
    // county GPA fraction, then scale every rate field.
    if !prepared.general_fuel_ratio.is_empty() {
        for br in &mut op_mode.base_rates {
            let gr = prepared.general_fuel_ratio.get(&GeneralFuelRatioKey {
                fuel_formulation_id: br.fuel_formulation_id,
                pol_process_id: fb.key.pol_process_id,
                source_type_id: fb.key.source_type_id,
            });
            let Some(gr) = gr else { continue };
            for grd in &gr.details {
                if fb.key.model_year_id >= grd.min_model_year_id
                    && fb.key.model_year_id <= grd.max_model_year_id
                    && fb.key.age_id >= grd.min_age_id
                    && fb.key.age_id <= grd.max_age_id
                {
                    let r = grd.fuel_effect_ratio
                        + gpa_fract * (grd.fuel_effect_ratio_gpa - grd.fuel_effect_ratio);
                    scale_base_rate(br, r);
                }
            }
        }
    }

    // Criteria ratio (running / start exhaust): blend by GPA fraction and
    // scale every rate field.
    if (fb.key.process_id == 1 || fb.key.process_id == 2) && !prepared.criteria_ratio.is_empty() {
        for br in &mut op_mode.base_rates {
            let cr = prepared
                .criteria_ratio
                .get(&super::model::CriteriaRatioKey {
                    fuel_formulation_id: br.fuel_formulation_id,
                    pol_process_id: fb.key.pol_process_id,
                    source_type_id: fb.key.source_type_id,
                    model_year_id: fb.key.model_year_id,
                    age_id: fb.key.age_id,
                });
            if let Some(cr) = cr {
                let r = cr.ratio + gpa_fract * (cr.ratio_gpa - cr.ratio);
                scale_base_rate(br, r);
            }
        }
    }

    // Temperature + humidity adjustment. The detail is looked up by the exact
    // regClassID, then by the regClassID-0 wildcard, then defaults to a
    // zero-valued detail (a no-op adjustment).
    if let (true, Some(zmh)) = (fuel_type_known, zmh) {
        let ta = prepared
            .temperature_adjustment
            .get(&TemperatureAdjustmentKey {
                pol_process_id: fb.key.pol_process_id,
                fuel_type_id: fb.key.fuel_type_id,
                reg_class_id: fb.key.reg_class_id,
                model_year_id: fb.key.model_year_id,
            })
            .or_else(|| {
                prepared
                    .temperature_adjustment
                    .get(&TemperatureAdjustmentKey {
                        pol_process_id: fb.key.pol_process_id,
                        fuel_type_id: fb.key.fuel_type_id,
                        reg_class_id: 0,
                        model_year_id: fb.key.model_year_id,
                    })
            })
            .copied()
            .unwrap_or_default();
        // The humidity factor only matters for the NOx branch, but the Go
        // computes it whenever the NOx-humidity row exists.
        let k = nha.map_or(1.0, |nha| calculate_nox_k(&zmh, nha));
        for br in &mut op_mode.base_rates {
            let factor = general_temp_adjust(
                &ta,
                &fb.key,
                k,
                zmh.temperature,
                zmh.heat_index,
                br.mean_base_rate,
                flags.is_project,
            );
            scale_base_rate(br, factor);
        }
    }

    // Air conditioning (every process except start exhaust): add the
    // AC-adjusted rate, scaled by the AC factor.
    if fb.key.process_id != 2 {
        let ac = prepared
            .zone_ac_factor
            .get(&ZoneAcFactorKey {
                hour_id: fb.key.hour_id,
                source_type_id: fb.key.source_type_id,
                model_year_id: fb.key.model_year_id,
            })
            .copied();
        if let Some(factor) = ac {
            if factor > 0.0 {
                for br in &mut op_mode.base_rates {
                    // A negative base rate (only reachable at project scale)
                    // pairs with a negative AC adjustment, so the adjustment
                    // is subtracted to move the rate toward zero.
                    if br.mean_base_rate >= 0.0 || !flags.is_project {
                        br.mean_base_rate += factor * br.mean_base_rate_ac_adj;
                        br.mean_base_rate_im += factor * br.mean_base_rate_im_ac_adj;
                        br.emission_rate += factor * br.emission_rate_ac_adj;
                        br.emission_rate_im += factor * br.emission_rate_im_ac_adj;
                    } else {
                        br.mean_base_rate -= factor * br.mean_base_rate_ac_adj;
                        br.mean_base_rate_im -= factor * br.mean_base_rate_im_ac_adj;
                        br.emission_rate -= factor * br.emission_rate_ac_adj;
                        br.emission_rate_im -= factor * br.emission_rate_im_ac_adj;
                    }
                }
            }
        }
    }

    // I/M programs: blend the I/M and non-I/M rates by the coverage fraction.
    if let Some(&im_adjust) = prepared.im_coverage.get(&ImCoverageKey {
        pol_process_id: fb.key.pol_process_id,
        model_year_id: fb.key.model_year_id,
        source_type_id: fb.key.source_type_id,
        fuel_type_id: fb.key.fuel_type_id,
    }) {
        for br in &mut op_mode.base_rates {
            br.mean_base_rate =
                (1.0 - im_adjust) * br.mean_base_rate + im_adjust * br.mean_base_rate_im;
            br.emission_rate =
                (1.0 - im_adjust) * br.emission_rate + im_adjust * br.emission_rate_im;
        }
    }

    // Emission rate adjustment — applied before the E85 duplication so the
    // duplicated records inherit the adjusted rate.
    if flags.emission_rate_adjustment && !prepared.emission_rate_adjustment.is_empty() {
        if let Some(&a) = prepared
            .emission_rate_adjustment
            .get(&PolProcSourceRegFuelMyKey {
                pol_process_id: fb.key.pol_process_id,
                source_type_id: fb.key.source_type_id,
                reg_class_id: fb.key.reg_class_id,
                fuel_type_id: fb.key.fuel_type_id,
                model_year_id: fb.key.model_year_id,
            })
        {
            for br in &mut op_mode.base_rates {
                br.mean_base_rate *= a;
                br.emission_rate *= a;
            }
        }
    }

    // E85 THC: where an E85 formulation (subtype 51/52) carries an alternate
    // criteria ratio, emit a 10000-offset pollutant scaled by altRatio/ratio.
    let e85_block = build_e85_block(&fb, prepared, gpa_fract);

    let mut blocks = vec![fb];
    blocks.extend(e85_block);

    // EV efficiency: divide the rate through the battery and charging
    // efficiencies. Applied to the E85 block as well, matching the Go loop
    // over every fuel block of the unit.
    if flags.ev_efficiency && !prepared.ev_efficiency.is_empty() {
        for block in &mut blocks {
            apply_ev_efficiency(block, prepared);
        }
    }

    blocks
}

/// Build the E85 THC duplicate block, if the row qualifies — the Go
/// `len(AltCriteriaRatio) > 0 && modelYearID >= 2001` branch.
fn build_e85_block(fb: &FuelBlock, prepared: &PreparedTables, gpa_fract: f64) -> Option<FuelBlock> {
    if !(fb.key.process_id == 1 || fb.key.process_id == 2) {
        return None;
    }
    if prepared.alt_criteria_ratio.is_empty() || fb.key.model_year_id < 2001 {
        return None;
    }
    let op_mode = fb.op_mode.as_ref()?;

    let mut new_op_mode: Option<OpModeRates> = None;
    for br in &op_mode.base_rates {
        let Some(&fuel_sub_type_id) = prepared.fuel_formulations.get(&br.fuel_formulation_id)
        else {
            continue;
        };
        if fuel_sub_type_id != 51 && fuel_sub_type_id != 52 {
            continue;
        }
        let cr_key = super::model::CriteriaRatioKey {
            fuel_formulation_id: br.fuel_formulation_id,
            pol_process_id: fb.key.pol_process_id,
            source_type_id: fb.key.source_type_id,
            model_year_id: fb.key.model_year_id,
            age_id: fb.key.age_id,
        };
        let (Some(acr), Some(cr)) = (
            prepared.alt_criteria_ratio.get(&cr_key),
            prepared.criteria_ratio.get(&cr_key),
        ) else {
            continue;
        };
        // Scale the E10-RVP-based effect to the E85-RVP-based effect.
        let ar = acr.ratio + gpa_fract * (acr.ratio_gpa - acr.ratio);
        let r = cr.ratio + gpa_fract * (cr.ratio_gpa - cr.ratio);
        let ar_to_r = if r > 0.0 { ar / r } else { 0.0 };

        let op_mode_ref = new_op_mode.get_or_insert_with(|| OpModeRates {
            op_mode_id: op_mode.op_mode_id,
            general_fraction: op_mode.general_fraction,
            general_fraction_rate: op_mode.general_fraction_rate,
            base_rates: Vec::new(),
        });
        let mut new_br = *br;
        new_br.mean_base_rate *= ar_to_r;
        new_br.emission_rate *= ar_to_r;
        op_mode_ref.base_rates.push(new_br);
    }

    new_op_mode.map(|op_mode| {
        // NewFuelBlock copies the key, then re-tags the pollutant 10000-up.
        let mut key = fb.key;
        key.pollutant_id += 10000;
        key.pol_process_id = key.pollutant_id * 100 + key.process_id;
        FuelBlock {
            key,
            op_mode: Some(op_mode),
            emissions: Vec::new(),
        }
    })
}

/// Divide a block's mean base rate and emission rate by the EV battery ×
/// charging efficiency, when an `EVEfficiency` record exists. A zero product
/// would divide by zero, so it is skipped (the Go logs an error and moves on).
fn apply_ev_efficiency(fb: &mut FuelBlock, prepared: &PreparedTables) {
    let Some(detail) = prepared.ev_efficiency.get(&PolProcSourceRegFuelMyKey {
        pol_process_id: fb.key.pol_process_id,
        source_type_id: fb.key.source_type_id,
        reg_class_id: fb.key.reg_class_id,
        fuel_type_id: fb.key.fuel_type_id,
        model_year_id: fb.key.model_year_id,
    }) else {
        return;
    };
    let divisor = detail.battery_efficiency * detail.charging_efficiency;
    if divisor == 0.0 {
        return;
    }
    if let Some(op_mode) = fb.op_mode.as_mut() {
        for br in &mut op_mode.base_rates {
            br.mean_base_rate /= divisor;
            br.emission_rate /= divisor;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn detail(a: f64, b: f64, c: f64, is_log: bool, is_poly: bool) -> StartTempAdjustmentDetail {
        StartTempAdjustmentDetail {
            term_a: a,
            term_b: b,
            term_c: c,
            is_log,
            is_poly,
        }
    }

    #[test]
    fn start_temp_adjust_pm_multiplicative_form() {
        // polProcessID 11202: rate*B*exp(A*(72-least(temp,72)))+C.
        // A=0 -> exp(0)=1, so result = base*B + C.
        let d = detail(0.0, 3.0, 7.0, false, false);
        assert_eq!(
            start_temp_adjust(&d, 2.0, 11202, 0.0, 50.0),
            2.0 * 3.0 + 7.0
        );
        // temp above 72 is clamped to 72, still exp(0).
        assert_eq!(
            start_temp_adjust(&d, 2.0, 11802, 0.0, 90.0),
            2.0 * 3.0 + 7.0
        );
    }

    #[test]
    fn start_temp_adjust_log_form_weights_by_fraction() {
        // isLog: base + weight*(B*exp(A*(least(temp,75)-75))+C). A=0 -> exp 0.
        let d = detail(0.0, 5.0, 1.0, true, false);
        // weight 0.5, temp 75 -> base + 0.5*(5*1 + 1) = base + 3.
        assert_eq!(start_temp_adjust(&d, 10.0, 301, 0.5, 75.0), 13.0);
    }

    #[test]
    fn start_temp_adjust_poly_and_default_branches_are_identical() {
        // The Go POLY branch and its fallback are byte-identical; is_poly
        // must not change the result.
        let poly = detail(1.0, 2.0, 3.0, false, true);
        let neither = detail(1.0, 2.0, 3.0, false, false);
        let got_poly = start_temp_adjust(&poly, 4.0, 301, 1.0, 60.0);
        let got_neither = start_temp_adjust(&neither, 4.0, 301, 1.0, 60.0);
        assert_eq!(got_poly, got_neither);
    }

    #[test]
    fn nox_k_cfr86_clamps_humidity() {
        let nha = NoxHumidityAdjustDetail {
            humidity_nox_eq: "CFR 86".to_string(),
            humidity_term_a: 0.01,
            humidity_term_b: 0.0,
            humidity_low_bound: 5.0,
            humidity_up_bound: 20.0,
            humidity_units: String::new(),
        };
        // specific humidity 100 clamps to 20: 1 - 0.01*(20-10.71).
        let zmh = ZoneMonthHourDetail {
            specific_humidity: 100.0,
            ..ZoneMonthHourDetail::default()
        };
        assert!((calculate_nox_k(&zmh, &nha) - (1.0 - 0.01 * (20.0 - 10.71))).abs() < 1e-12);
    }

    #[test]
    fn nox_k_unknown_equation_is_identity() {
        let nha = NoxHumidityAdjustDetail {
            humidity_nox_eq: "something else".to_string(),
            ..NoxHumidityAdjustDetail::default()
        };
        assert_eq!(calculate_nox_k(&ZoneMonthHourDetail::default(), &nha), 1.0);
    }

    #[test]
    fn general_temp_adjust_pm_caps_at_one_above_72() {
        let ta = TemperatureAdjustmentDetail {
            term_a: 0.02,
            term_b: 0.0,
            term_c: 0.0,
        };
        let key = BlockKey {
            process_id: 1,
            pollutant_id: 118,
            ..BlockKey::default()
        };
        assert_eq!(
            general_temp_adjust(&ta, &key, 1.0, 90.0, 0.0, 1.0, false),
            1.0
        );
        // At/below 72 -> exp(A*(72-temp)).
        let got = general_temp_adjust(&ta, &key, 1.0, 72.0, 0.0, 1.0, false);
        assert_eq!(got, 1.0); // exp(0)
    }

    #[test]
    fn general_temp_adjust_nox_multiplies_by_humidity_k() {
        // NOx, non-diesel: (1 + (temp-75)*(A+(temp-75)*B)) * k. temp 75 -> 1*k.
        let ta = TemperatureAdjustmentDetail {
            term_a: 0.1,
            term_b: 0.2,
            term_c: 0.0,
        };
        let key = BlockKey {
            process_id: 1,
            pollutant_id: 3,
            fuel_type_id: 1,
            ..BlockKey::default()
        };
        assert_eq!(
            general_temp_adjust(&ta, &key, 1.3, 75.0, 0.0, 1.0, false),
            1.3
        );
    }

    #[test]
    fn general_temp_adjust_ev_project_negative_baserate_flips_sign() {
        // EV running energy, project scale, negative base rate -> 1 - adj.
        let ta = TemperatureAdjustmentDetail {
            term_a: 1.0,
            term_b: 0.0,
            term_c: 0.0,
        };
        let key = BlockKey {
            process_id: 1,
            pollutant_id: 91,
            fuel_type_id: 9,
            source_type_id: 62, // >= 40 so the heat-index suppression is off
            ..BlockKey::default()
        };
        // temp 73: adj = (73-72)*(1 + 0) = 1. project + negative baserate.
        assert_eq!(
            general_temp_adjust(&ta, &key, 1.0, 73.0, 0.0, -5.0, true),
            0.0
        );
        // Non-project, same inputs -> 1 + adj.
        assert_eq!(
            general_temp_adjust(&ta, &key, 1.0, 73.0, 0.0, -5.0, false),
            2.0
        );
    }
}

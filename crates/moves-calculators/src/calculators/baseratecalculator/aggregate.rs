//! Operating-mode aggregation and activity weighting — the tail of the Base
//! Rate Calculator pipeline.
//!
//! Ports the Go `aggregateOpModes`, `calculateActivityWeight`, and
//! `aggregateAndApplyActivity`. After [`process_fuel_block`] has adjusted
//! every per-operating-mode rate, these steps collapse the operating-mode
//! detail into one [`Emission`] per fuel formulation, optionally weight the
//! result by the source/model-year/fuel/reg-class activity distribution, and
//! optionally convert rates into an inventory by multiplying through the
//! universal activity.
//!
//! [`process_fuel_block`]: super::adjust::process_fuel_block

use std::collections::BTreeMap;

use super::model::{
    ActivityWeightDetail, ActivityWeightKey, Emission, FuelBlock, ModelYearFuelKey, ModuleFlags,
    UniversalActivityKey,
};
use super::setup::{ActivityWeights, PreparedTables, SmfrSbdSummaryRow};

/// Collapse a block's per-operating-mode base rates into one [`Emission`] per
/// fuel formulation — the Go `aggregateOpModes`.
///
/// `emission_quant` sums `meanBaseRate * marketShare`; `emission_rate` sums
/// `emissionRate * marketShare`. The operating-mode detail is then dropped:
/// `op_mode` is cleared and `emissions` becomes the block's payload.
pub fn aggregate_op_modes(fb: &mut FuelBlock) {
    let mut emissions: BTreeMap<i32, Emission> = BTreeMap::new();
    if let Some(op_mode) = &fb.op_mode {
        for br in &op_mode.base_rates {
            let e = emissions.entry(br.fuel_formulation_id).or_insert(Emission {
                fuel_sub_type_id: br.fuel_sub_type_id,
                fuel_formulation_id: br.fuel_formulation_id,
                emission_quant: 0.0,
                emission_rate: 0.0,
            });
            e.emission_quant += br.mean_base_rate * br.market_share;
            e.emission_rate += br.emission_rate * br.market_share;
        }
    }
    fb.emissions = emissions.into_values().collect();
    fb.op_mode = None;
}

/// Zero the [`ActivityWeightKey`] fields the `BRC_Discard*` flags drop.
fn discard_key(mut key: ActivityWeightKey, flags: &ModuleFlags) -> ActivityWeightKey {
    if flags.discard_source_type_id {
        key.source_type_id = 0;
    }
    if flags.discard_model_year_id {
        key.model_year_id = 0;
    }
    if flags.discard_fuel_type_id {
        key.fuel_type_id = 0;
    }
    if flags.discard_reg_class_id {
        key.reg_class_id = 0;
    }
    key
}

/// Compute the activity-weight distribution — the Go `calculateActivityWeight`.
///
/// Returns an empty map unless [`ModuleFlags::aggregate_smfr`] is set (the Go
/// early-returns when `BRC_AggregateSMFR` is absent). Otherwise it:
///
/// 1. seeds each `(hourDay, modelYear, source, fuel, regClass)` cell with
/// `universalActivity * sbdTotal`, summed over the SMFR source-bin rows;
/// 2. divides the *rates* fraction by the extended-idle and/or APU usage
/// fractions, restricting the weighting activity to those hours;
/// 3. normalises each cell against the total over whichever of source /
/// model-year / fuel / reg-class the run aggregates away.
#[must_use]
pub fn calculate_activity_weight(
    smfr_sbd_summary: &[SmfrSbdSummaryRow],
    prepared: &PreparedTables,
    flags: &ModuleFlags,
) -> ActivityWeights {
    let mut activity_weight: ActivityWeights = BTreeMap::new();
    if !flags.aggregate_smfr {
        return activity_weight;
    }

    // Seed each cell with universalActivity * sbdTotal.
    for row in smfr_sbd_summary {
        for &hour_day_id in &prepared.universal_activity_hour_day_ids {
            let Some(&activity) = prepared.universal_activity.get(&UniversalActivityKey {
                hour_day_id,
                model_year_id: row.model_year_id,
                source_type_id: row.source_type_id,
            }) else {
                continue;
            };
            let detail = activity_weight
                .entry(ActivityWeightKey {
                    hour_day_id,
                    model_year_id: row.model_year_id,
                    source_type_id: row.source_type_id,
                    fuel_type_id: row.fuel_type_id,
                    reg_class_id: row.reg_class_id,
                })
                .or_default();
            detail.smfr_fraction += activity * row.sbd_total;
            detail.smfr_rates_fraction += activity * row.sbd_total;
        }
    }

    // Extended-idle rate weighting: the input activity includes all extended
    // idling, but the rate must be weighted only by diesel-APU hours.
    if flags.adjust_extended_idle_emission_rate {
        for (key, detail) in &mut activity_weight {
            let ei_adjust = prepared
                .extended_idle_emission_rate_fraction
                .get(&ModelYearFuelKey {
                    model_year_id: key.model_year_id,
                    fuel_type_id: key.fuel_type_id,
                })
                .copied()
                .unwrap_or(0.0);
            if ei_adjust != 0.0 {
                detail.smfr_rates_fraction /= ei_adjust;
            }
        }
    }
    // APU rate weighting: restrict the weighting activity to diesel-APU or
    // shorepower hours.
    if flags.adjust_apu_emission_rate {
        for (key, detail) in &mut activity_weight {
            let my_fuel = ModelYearFuelKey {
                model_year_id: key.model_year_id,
                fuel_type_id: key.fuel_type_id,
            };
            let apu_adjust = prepared
                .apu_emission_rate_fraction
                .get(&my_fuel)
                .copied()
                .unwrap_or(0.0);
            let sp_adjust = prepared
                .shorepower_emission_rate_fraction
                .get(&my_fuel)
                .copied()
                .unwrap_or(0.0);
            let adjustment = apu_adjust + sp_adjust;
            if adjustment != 0.0 {
                detail.smfr_rates_fraction /= adjustment;
            }
        }
    }

    // Total the activity over the aggregated-away dimensions.
    let mut activity_total: BTreeMap<ActivityWeightKey, ActivityWeightDetail> = BTreeMap::new();
    for (key, detail) in &activity_weight {
        let total = activity_total.entry(discard_key(*key, flags)).or_default();
        total.smfr_fraction += detail.smfr_fraction;
        total.smfr_rates_fraction += detail.smfr_rates_fraction;
    }

    // Normalise each cell into a fraction of its aggregated total.
    for (key, detail) in &mut activity_weight {
        match activity_total.get(&discard_key(*key, flags)) {
            None => {
                detail.smfr_fraction = 0.0;
                detail.smfr_rates_fraction = 0.0;
            }
            Some(total) => {
                detail.smfr_fraction = if total.smfr_fraction > 0.0 {
                    detail.smfr_fraction / total.smfr_fraction
                } else {
                    0.0
                };
                detail.smfr_rates_fraction = if total.smfr_rates_fraction > 0.0 {
                    detail.smfr_rates_fraction / total.smfr_rates_fraction
                } else {
                    0.0
                };
            }
        }
    }

    activity_weight
}

/// Aggregate one block's operating modes, then weight and convert it — the
/// per-block body of the Go `aggregateAndApplyActivity`.
///
/// `aggregate_op_modes` always runs. When [`ModuleFlags::aggregate_smfr`] or
/// [`ModuleFlags::apply_activity`] is set, the aggregated emissions are then
/// scaled: by the activity distribution (rate and/or mean-base-rate fraction)
/// and/or by the universal activity that turns a rate into an inventory.
pub fn aggregate_and_apply_activity(
    fb: &mut FuelBlock,
    prepared: &PreparedTables,
    flags: &ModuleFlags,
    activity_weights: &ActivityWeights,
) {
    aggregate_op_modes(fb);

    if !(flags.aggregate_smfr || flags.apply_activity) {
        return;
    }

    let mut mean_base_rate_scale = 1.0;
    let mut emission_rate_scale = 1.0;

    if flags.aggregate_smfr {
        if let Some(wd) = activity_weights.get(&ActivityWeightKey {
            hour_day_id: fb.key.hour_day_id,
            model_year_id: fb.key.model_year_id,
            source_type_id: fb.key.source_type_id,
            fuel_type_id: fb.key.fuel_type_id,
            reg_class_id: fb.key.reg_class_id,
        }) {
            if flags.adjust_emission_rate_only {
                emission_rate_scale *= wd.smfr_rates_fraction;
            } else if flags.adjust_mean_base_rate_and_emission_rate {
                mean_base_rate_scale *= wd.smfr_fraction;
                emission_rate_scale *= wd.smfr_rates_fraction;
            }
        }
    }

    if flags.apply_activity {
        if let Some(&activity) = prepared.universal_activity.get(&UniversalActivityKey {
            hour_day_id: fb.key.hour_day_id,
            model_year_id: fb.key.model_year_id,
            source_type_id: fb.key.source_type_id,
        }) {
            mean_base_rate_scale *= activity;
        }
    }

    // ScaleEmissions: the mean-base-rate scale weights the emission quantity,
    // the emission-rate scale weights the emission rate.
    for e in &mut fb.emissions {
        e.emission_quant *= mean_base_rate_scale;
        e.emission_rate *= emission_rate_scale;
    }
}

#[cfg(test)]
mod tests {
    use super::super::model::{BaseRate, BlockKey, OpModeRates};
    use super::*;

    fn base_rate(formulation: i32, market: f64, mean: f64, rate: f64) -> BaseRate {
        BaseRate {
            fuel_sub_type_id: 10,
            fuel_formulation_id: formulation,
            market_share: market,
            mean_base_rate: mean,
            emission_rate: rate,
            ..BaseRate::default()
        }
    }

    #[test]
    fn aggregate_op_modes_sums_market_weighted_rates_per_formulation() {
        let mut fb = FuelBlock {
            key: BlockKey::default(),
            op_mode: Some(OpModeRates {
                op_mode_id: 0,
                general_fraction: 0.0,
                general_fraction_rate: 0.0,
                base_rates: vec![
                    base_rate(100, 0.5, 4.0, 8.0),
                    base_rate(100, 0.25, 4.0, 8.0),
                    base_rate(200, 1.0, 3.0, 6.0),
                ],
            }),
            emissions: Vec::new(),
        };
        aggregate_op_modes(&mut fb);
        assert!(fb.op_mode.is_none());
        assert_eq!(fb.emissions.len(), 2);
        // Formulation 100: quant = 4*0.5 + 4*0.25 = 3; rate = 8*0.5 + 8*0.25 = 6.
        let f100 = fb
            .emissions
            .iter()
            .find(|e| e.fuel_formulation_id == 100)
            .unwrap();
        assert_eq!(f100.emission_quant, 3.0);
        assert_eq!(f100.emission_rate, 6.0);
        // Formulation 200: quant = 3*1 = 3; rate = 6*1 = 6.
        let f200 = fb
            .emissions
            .iter()
            .find(|e| e.fuel_formulation_id == 200)
            .unwrap();
        assert_eq!(f200.emission_quant, 3.0);
    }

    #[test]
    fn activity_weight_empty_without_aggregate_smfr() {
        let prepared = PreparedTables::default();
        let weights = calculate_activity_weight(&[], &prepared, &ModuleFlags::default());
        assert!(weights.is_empty());
    }

    #[test]
    fn aggregate_and_apply_activity_without_flags_only_aggregates() {
        let mut fb = FuelBlock {
            key: BlockKey::default(),
            op_mode: Some(OpModeRates {
                op_mode_id: 0,
                general_fraction: 0.0,
                general_fraction_rate: 0.0,
                base_rates: vec![base_rate(100, 1.0, 5.0, 9.0)],
            }),
            emissions: Vec::new(),
        };
        let prepared = PreparedTables::default();
        let weights = ActivityWeights::new();
        aggregate_and_apply_activity(&mut fb, &prepared, &ModuleFlags::default(), &weights);
        // No SMFR, no activity: emissions present, unscaled.
        assert_eq!(fb.emissions.len(), 1);
        assert_eq!(fb.emissions[0].emission_quant, 5.0);
        assert_eq!(fb.emissions[0].emission_rate, 9.0);
    }

    #[test]
    fn apply_activity_scales_emission_quant_by_universal_activity() {
        let mut fb = FuelBlock {
            key: BlockKey {
                hour_day_id: 85,
                model_year_id: 2018,
                source_type_id: 21,
                ..BlockKey::default()
            },
            op_mode: Some(OpModeRates {
                op_mode_id: 0,
                general_fraction: 0.0,
                general_fraction_rate: 0.0,
                base_rates: vec![base_rate(100, 1.0, 5.0, 9.0)],
            }),
            emissions: Vec::new(),
        };
        let mut prepared = PreparedTables::default();
        prepared.universal_activity.insert(
            UniversalActivityKey {
                hour_day_id: 85,
                model_year_id: 2018,
                source_type_id: 21,
            },
            3.0,
        );
        let flags = ModuleFlags {
            apply_activity: true,
            ..ModuleFlags::default()
        };
        aggregate_and_apply_activity(&mut fb, &prepared, &flags, &ActivityWeights::new());
        // emission_quant *= activity (3); emission_rate untouched.
        assert_eq!(fb.emissions[0].emission_quant, 15.0);
        assert_eq!(fb.emissions[0].emission_rate, 9.0);
    }
}

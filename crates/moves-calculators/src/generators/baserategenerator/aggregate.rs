//! Core `RatesOpModeDistribution` processing and the base-rate aggregators.
//!
//! Ports four Go functions:
//!
//! * [`core_base_rate_generator_from_romd`] — `coreBaseRateGeneratorFromRatesOpModeDistribution`
//! * [`make_base_rate_from_source_bin_rates`] — `makeBaseRateFromSourceBinRates`
//! * [`make_base_rate_by_age_from_source_bin_rates`] — `makeBaseRateByAgeFromSourceBinRates`
//! * [`make_base_rate_from_distance_rates`] — `makeBaseRateFromDistanceRates`
//!
//! The Go ran these as goroutines connected by channels; the pure port runs
//! them sequentially. The producer ([`core_base_rate_generator_from_romd`] or
//! the drive-cycle path) yields a `Vec<RomdBlock>` in the same order the Go
//! SQL `ORDER BY` / nested-loop enumeration produced, and the consumers
//! stream it with the Go's `currentKey != previousKey` flush logic — so the
//! accumulated sums match the Go bit-for-bit.

use std::collections::{BTreeMap, BTreeSet};

use super::inputs::{BaseRateInputs, PreparedTables};
use super::model::{
    AvgSpeedDistributionKey, BaseRateOutputKey, BaseRateOutputRecord, ExternalFlags, RomdBlock,
    RomdKey, SbWeightedRateDetail, SbWeightedRateKey,
};

/// Process the `RatesOpModeDistribution` table, applying the physics-mapping
/// source-type swaps and operating-mode promotions, and yield the deduplicated
/// [`RomdBlock`] stream the base-rate aggregators consume.
///
/// Ports `coreBaseRateGeneratorFromRatesOpModeDistribution`. The Go relied on
/// `ORDER BY sourceTypeID DESC, polProcessID DESC, roadTypeID DESC,
/// hourDayID DESC, opModeID DESC, avgSpeedBinID DESC` — the descending
/// `opModeID` ensures genuine extended operating modes (`opModeID >= 1000`)
/// are seen, and claim their deduplication key, before any real operating
/// mode is promoted onto the same key. The port reproduces that sort.
#[must_use]
pub fn core_base_rate_generator_from_romd(
    inputs: &BaseRateInputs,
    prepared: &PreparedTables,
    flags: &ExternalFlags,
) -> Vec<RomdBlock> {
    // WHERE: optional process and road-type restrictions.
    let mut rows: Vec<_> = inputs
        .rates_op_mode_distribution
        .iter()
        .copied()
        .filter(|r| flags.process_id <= 0 || r.pol_process_id % 100 == flags.process_id)
        .filter(|r| flags.road_type_id <= 0 || r.road_type_id == flags.road_type_id)
        .collect();
    // ORDER BY ... DESC on all six key columns.
    rows.sort_by(|a, b| {
        let ka = (
            a.source_type_id,
            a.pol_process_id,
            a.road_type_id,
            a.hour_day_id,
            a.op_mode_id,
            a.avg_speed_bin_id,
        );
        let kb = (
            b.source_type_id,
            b.pol_process_id,
            b.road_type_id,
            b.hour_day_id,
            b.op_mode_id,
            b.avg_speed_bin_id,
        );
        kb.cmp(&ka)
    });

    let mut output: Vec<RomdBlock> = Vec::new();
    // Combinations already emitted for the current (sourceType, polProcess).
    let mut romd_keys: BTreeSet<RomdKey> = BTreeSet::new();
    let mut previous_source_type_id = 0;
    let mut previous_pol_process_id = 0;

    for row in rows {
        let mut source_type_id = row.source_type_id;
        let mut op_mode_id = row.op_mode_id;
        let mut avg_speed_fraction = row.avg_speed_fraction;
        let mut avg_bin_speed = row.avg_bin_speed;
        let mut should_write = false;
        let mut did_handle = false;

        if previous_source_type_id != source_type_id
            || previous_pol_process_id != row.pol_process_id
        {
            romd_keys.clear();
        }
        previous_source_type_id = source_type_id;
        previous_pol_process_id = row.pol_process_id;

        // Resolve the average-speed information if the row did not carry it.
        if avg_speed_fraction <= 0.0 {
            if let Some(d) = prepared
                .avg_speed_distribution
                .get(&AvgSpeedDistributionKey {
                    source_type_id,
                    road_type_id: row.road_type_id,
                    hour_day_id: row.hour_day_id,
                    avg_speed_bin_id: row.avg_speed_bin_id,
                })
            {
                avg_speed_fraction = d.avg_speed_fraction;
                avg_bin_speed = d.avg_bin_speed;
            }
        }

        // Physics-mapping details are looked up by the *original* source type.
        let temp_detail = prepared
            .physics_by_temp_source_type
            .get(&source_type_id)
            .copied();
        let real_detail = prepared
            .physics_by_real_source_type
            .get(&source_type_id)
            .copied();

        // The transform branches gate on Running (1) / Brakewear (9) — or a
        // negative (wildcard) polProcessID.
        let process_eligible = row.pol_process_id < 0
            || row.pol_process_id % 100 == 1
            || row.pol_process_id % 100 == 9;

        // Delete wildcard placeholders.
        if !did_handle && row.pol_process_id < 0 {
            did_handle = true;
            should_write = false;
        }

        // Change source types for any new (already-offset) operating modes.
        if !did_handle {
            if let Some(temp) = temp_detail {
                if op_mode_id >= temp.op_mode_id_offset
                    && op_mode_id < 100 + temp.op_mode_id_offset
                    && process_eligible
                {
                    did_handle = true;
                    should_write = true;
                    source_type_id = temp.real_source_type_id;
                }
            }
        }

        // Promote old operating modes and change source types. If a genuine
        // extended-operating-mode record already claimed the deduplication
        // key, the promoted record collides and is silently dropped later.
        if !did_handle {
            if let Some(temp) = temp_detail {
                if (0..100).contains(&op_mode_id) && process_eligible {
                    did_handle = true;
                    should_write = true;
                    source_type_id = temp.real_source_type_id;
                    op_mode_id += temp.op_mode_id_offset;
                }
            }
        }

        // A temp source type with a positive offset discards its real modes.
        if !did_handle {
            if let Some(temp) = temp_detail {
                if temp.op_mode_id_offset > 0 && (0..100).contains(&op_mode_id) && process_eligible
                {
                    did_handle = true;
                    should_write = false;
                }
            }
        }

        // A real source type that has been superseded discards its modes.
        if !did_handle && temp_detail.is_none() {
            if let Some(real) = real_detail {
                if real.op_mode_id_offset > 0 && (0..100).contains(&op_mode_id) && process_eligible
                {
                    did_handle = true;
                    should_write = false;
                }
            }
        }

        // Anything not otherwise handled is kept.
        if !did_handle {
            should_write = true;
        }

        if !should_write {
            continue;
        }

        let key = RomdKey {
            source_type_id,
            pol_process_id: row.pol_process_id,
            road_type_id: row.road_type_id,
            hour_day_id: row.hour_day_id,
            op_mode_id,
            avg_speed_bin_id: row.avg_speed_bin_id,
            begin_model_year_id: 0,
            end_model_year_id: 0,
            reg_class_id: 0,
        };
        if romd_keys.contains(&key) {
            continue;
        }

        // Re-resolve the average-speed information now that the source type
        // may have changed.
        if avg_speed_fraction <= 0.0 {
            if let Some(d) = prepared
                .avg_speed_distribution
                .get(&AvgSpeedDistributionKey {
                    source_type_id,
                    road_type_id: row.road_type_id,
                    hour_day_id: row.hour_day_id,
                    avg_speed_bin_id: row.avg_speed_bin_id,
                })
            {
                avg_speed_fraction = d.avg_speed_fraction;
                avg_bin_speed = d.avg_bin_speed;
            }
        }

        romd_keys.insert(key);
        output.push(RomdBlock {
            key,
            op_mode_fraction: row.op_mode_fraction,
            avg_bin_speed,
            avg_speed_fraction,
        });
    }

    output
}

/// Drain every accumulated record into the output vector, deterministically
/// (ascending key order). Ports the Go `writeLines`; the Go iterated a hash
/// map in random order, so output-row order was never defined.
fn drain_records(
    records: &mut BTreeMap<BaseRateOutputKey, BaseRateOutputRecord>,
    out: &mut Vec<BaseRateOutputRecord>,
) {
    out.extend(std::mem::take(records).into_values());
}

/// The romd-portion of a [`BaseRateOutputKey`] / flush key, mirroring the Go
/// `currentKey` that only fills `opModeID` / `avgSpeedBinID` when the
/// corresponding flag is set.
fn current_key(romd: &RomdBlock, flags: &ExternalFlags) -> RomdKey {
    let mut key = RomdKey {
        source_type_id: romd.key.source_type_id,
        pol_process_id: romd.key.pol_process_id,
        road_type_id: romd.key.road_type_id,
        hour_day_id: romd.key.hour_day_id,
        ..RomdKey::default()
    };
    if flags.keep_op_mode_id {
        key.op_mode_id = romd.key.op_mode_id;
    }
    if flags.use_avg_speed_bin {
        key.avg_speed_bin_id = romd.key.avg_speed_bin_id;
    }
    key
}

/// Build the accumulation key for one (romd, rate) pair.
///
/// Ports `baseRateOutputKey.fromRomdKey` + `fromSBbyAge`: the romd-portion
/// fields are copied from `current` (the flush key) and the rate-portion
/// fields from `rate`.
fn output_key(current: &RomdKey, rate: &SbWeightedRateDetail) -> BaseRateOutputKey {
    BaseRateOutputKey {
        source_type_id: current.source_type_id,
        pol_process_id: current.pol_process_id,
        road_type_id: current.road_type_id,
        hour_day_id: current.hour_day_id,
        op_mode_id: current.op_mode_id,
        avg_speed_bin_id: current.avg_speed_bin_id,
        model_year_id: rate.model_year_id,
        fuel_type_id: rate.fuel_type_id,
        age_group_id: rate.age_group_id,
        reg_class_id: rate.reg_class_id,
    }
}

/// Initialise a fresh output record for one (romd, rate) pair.
fn new_output_record(
    romd: &RomdBlock,
    rate: &SbWeightedRateDetail,
    flags: &ExternalFlags,
    by_age: bool,
) -> BaseRateOutputRecord {
    let pol_process_id = romd.key.pol_process_id;
    let mut record = BaseRateOutputRecord {
        source_type_id: romd.key.source_type_id,
        road_type_id: romd.key.road_type_id,
        hour_day_id: romd.key.hour_day_id,
        pol_process_id,
        process_id: pol_process_id % 100,
        pollutant_id: pol_process_id / 100,
        model_year_id: rate.model_year_id,
        fuel_type_id: rate.fuel_type_id,
        // BaseRate carries no age dimension; BaseRateByAge carries the rate's.
        age_group_id: if by_age { rate.age_group_id } else { 0 },
        reg_class_id: rate.reg_class_id,
        ..BaseRateOutputRecord::default()
    };
    if flags.use_avg_speed_bin {
        record.avg_speed_bin_id = romd.key.avg_speed_bin_id;
    }
    // Rates mode (useAvgSpeedBin) needs the real opModeID retained; inventory
    // mode collapses it to 0. keepOpModeID is, per the Go comment, always
    // false in practice, but is honoured for completeness.
    if flags.use_avg_speed_bin || flags.keep_op_mode_id {
        record.op_mode_id = romd.key.op_mode_id;
    }
    record
}

/// Populate the `BaseRate` table from source-bin weighted emission rates.
///
/// Ports `makeBaseRateFromSourceBinRates`.
#[must_use]
pub fn make_base_rate_from_source_bin_rates(
    romd_blocks: &[RomdBlock],
    prepared: &PreparedTables,
    flags: &ExternalFlags,
) -> Vec<BaseRateOutputRecord> {
    let mut records: BTreeMap<BaseRateOutputKey, BaseRateOutputRecord> = BTreeMap::new();
    let mut output: Vec<BaseRateOutputRecord> = Vec::new();
    let mut previous_key: Option<RomdKey> = None;

    for romd in romd_blocks {
        let current = current_key(romd, flags);
        if previous_key.is_some_and(|p| p != current) {
            drain_records(&mut records, &mut output);
        }
        previous_key = Some(current);

        let sb_key = SbWeightedRateKey {
            source_type_id: romd.key.source_type_id,
            pol_process_id: romd.key.pol_process_id,
            op_mode_id: romd.key.op_mode_id,
        };
        let rates = prepared.sb_weighted_emission_rate.get(&sb_key);
        if rates.is_none() && romd.key.op_mode_id >= 1000 {
            // Offset operating modes that miss are skipped to avoid the
            // double-counting the Go comment describes.
            continue;
        }
        let Some(rates) = rates else { continue };

        for rate in rates {
            if romd.key.reg_class_id > 0 && romd.key.reg_class_id != rate.reg_class_id {
                continue;
            }
            if romd.key.begin_model_year_id > 0
                && romd.key.end_model_year_id > 0
                && (rate.model_year_id < romd.key.begin_model_year_id
                    || rate.model_year_id > romd.key.end_model_year_id)
            {
                continue;
            }
            let record = records
                .entry(output_key(&current, rate))
                .or_insert_with(|| new_output_record(romd, rate, flags, false));

            let sum_sbd = if flags.use_sum_sbd { rate.sum_sbd } else { 1.0 };
            let sum_sbd_raw = if flags.use_sum_sbd_raw {
                rate.sum_sbd_raw
            } else {
                1.0
            };
            let op_mode_fraction = romd.op_mode_fraction;
            let avg_bin_speed = romd.avg_bin_speed;
            let avg_speed_fraction = if flags.use_avg_speed_fraction {
                romd.avg_speed_fraction
            } else {
                1.0
            };

            let mut t = op_mode_fraction * avg_speed_fraction * sum_sbd;
            if flags.keep_op_mode_id {
                record.op_mode_fraction += t * sum_sbd_raw;
            } else {
                record.op_mode_fraction += t;
            }
            record.op_mode_fraction_rate += t;

            t = op_mode_fraction * avg_speed_fraction * sum_sbd_raw;
            // ONI exception: undo the source-bin weighting for total energy
            // consumption (pollutant 91) on off-network roads, so refueling
            // — which is chained to energy — agrees with the inventory.
            if flags.use_avg_speed_bin
                && record.road_type_id == 1
                && record.process_id == 1
                && record.pollutant_id == 91
            {
                t *= rate.sum_sbd_raw;
            }
            record.mean_base_rate += rate.mean_base_rate * t;
            record.mean_base_rate_im += rate.mean_base_rate_im * t;
            record.mean_base_rate_ac_adj += rate.mean_base_rate_ac_adj * t;
            record.mean_base_rate_im_ac_adj += rate.mean_base_rate_im_ac_adj * t;

            if flags.use_avg_speed_bin {
                if avg_bin_speed > 0.0 {
                    t = op_mode_fraction * avg_speed_fraction / avg_bin_speed;
                    record.emission_rate += rate.mean_base_rate * t;
                    record.emission_rate_im += rate.mean_base_rate_im * t;
                    record.emission_rate_ac_adj += rate.mean_base_rate_ac_adj * t;
                    record.emission_rate_im_ac_adj += rate.mean_base_rate_im_ac_adj * t;
                }
            } else {
                t = op_mode_fraction * avg_speed_fraction;
                record.emission_rate += rate.mean_base_rate * t;
                record.emission_rate_im += rate.mean_base_rate_im * t;
                record.emission_rate_ac_adj += rate.mean_base_rate_ac_adj * t;
                record.emission_rate_im_ac_adj += rate.mean_base_rate_im_ac_adj * t;
            }
        }
    }
    drain_records(&mut records, &mut output);
    output
}

/// Populate the `BaseRateByAge` table from source-bin weighted emission rates.
///
/// Ports `makeBaseRateByAgeFromSourceBinRates`. Differs from
/// [`make_base_rate_from_source_bin_rates`] in three ways: it reads the
/// age-resolved `SBWeightedEmissionRateByAge` table, its `opModeID >= 1000`
/// miss handling falls back to the non-offset operating mode for Brakewear
/// (process `9`) instead of skipping unconditionally, and it has no ONI
/// exception.
#[must_use]
pub fn make_base_rate_by_age_from_source_bin_rates(
    romd_blocks: &[RomdBlock],
    prepared: &PreparedTables,
    flags: &ExternalFlags,
) -> Vec<BaseRateOutputRecord> {
    let mut records: BTreeMap<BaseRateOutputKey, BaseRateOutputRecord> = BTreeMap::new();
    let mut output: Vec<BaseRateOutputRecord> = Vec::new();
    let mut previous_key: Option<RomdKey> = None;

    for romd in romd_blocks {
        let current = current_key(romd, flags);
        if previous_key.is_some_and(|p| p != current) {
            drain_records(&mut records, &mut output);
        }
        previous_key = Some(current);

        let mut sb_key = SbWeightedRateKey {
            source_type_id: romd.key.source_type_id,
            pol_process_id: romd.key.pol_process_id,
            op_mode_id: romd.key.op_mode_id,
        };
        let mut rates = prepared.sb_weighted_emission_rate_by_age.get(&sb_key);
        if rates.is_none() && romd.key.op_mode_id >= 1000 {
            // Non-brakewear offset modes are skipped to avoid double-counting;
            // brakewear keeps rates only for non-offset modes, so fall back.
            if romd.key.pol_process_id % 100 != 9 {
                continue;
            }
            sb_key.op_mode_id = romd.key.op_mode_id % 100;
            rates = prepared.sb_weighted_emission_rate_by_age.get(&sb_key);
        }
        let Some(rates) = rates else { continue };

        for rate in rates {
            if romd.key.reg_class_id > 0 && romd.key.reg_class_id != rate.reg_class_id {
                continue;
            }
            if romd.key.begin_model_year_id > 0
                && romd.key.end_model_year_id > 0
                && (rate.model_year_id < romd.key.begin_model_year_id
                    || rate.model_year_id > romd.key.end_model_year_id)
            {
                continue;
            }
            let record = records
                .entry(output_key(&current, rate))
                .or_insert_with(|| new_output_record(romd, rate, flags, true));

            let sum_sbd = if flags.use_sum_sbd { rate.sum_sbd } else { 1.0 };
            let sum_sbd_raw = if flags.use_sum_sbd_raw {
                rate.sum_sbd_raw
            } else {
                1.0
            };
            let op_mode_fraction = romd.op_mode_fraction;
            let avg_bin_speed = romd.avg_bin_speed;
            let avg_speed_fraction = if flags.use_avg_speed_fraction {
                romd.avg_speed_fraction
            } else {
                1.0
            };

            let mut t = op_mode_fraction * avg_speed_fraction * sum_sbd;
            if flags.keep_op_mode_id {
                record.op_mode_fraction += t * sum_sbd_raw;
            } else {
                record.op_mode_fraction += t;
            }
            record.op_mode_fraction_rate += t;

            t = op_mode_fraction * avg_speed_fraction * sum_sbd_raw;
            record.mean_base_rate += rate.mean_base_rate * t;
            record.mean_base_rate_im += rate.mean_base_rate_im * t;
            record.mean_base_rate_ac_adj += rate.mean_base_rate_ac_adj * t;
            record.mean_base_rate_im_ac_adj += rate.mean_base_rate_im_ac_adj * t;

            if flags.use_avg_speed_bin {
                if avg_bin_speed > 0.0 {
                    t = op_mode_fraction * avg_speed_fraction / avg_bin_speed;
                    record.emission_rate += rate.mean_base_rate * t;
                    record.emission_rate_im += rate.mean_base_rate_im * t;
                    record.emission_rate_ac_adj += rate.mean_base_rate_ac_adj * t;
                    record.emission_rate_im_ac_adj += rate.mean_base_rate_im_ac_adj * t;
                }
            } else {
                t = op_mode_fraction * avg_speed_fraction;
                record.emission_rate += rate.mean_base_rate * t;
                record.emission_rate_im += rate.mean_base_rate_im * t;
                record.emission_rate_ac_adj += rate.mean_base_rate_ac_adj * t;
                record.emission_rate_im_ac_adj += rate.mean_base_rate_im_ac_adj * t;
            }
        }
    }
    drain_records(&mut records, &mut output);
    output
}

/// Populate the `BaseRate` table from distance-based source-bin weighted
/// emission rates.
///
/// Ports `makeBaseRateFromDistanceRates`. Distance rates have a single
/// operating mode (`300`, or `0` in inventory mode) and `opModeFraction`
/// fixed at `1`, so the per-mode loop collapses to one road-type × hour-day
/// sweep per rate row.
#[must_use]
pub fn make_base_rate_from_distance_rates(
    inputs: &BaseRateInputs,
    prepared: &PreparedTables,
    flags: &ExternalFlags,
) -> Vec<BaseRateOutputRecord> {
    // WHERE mod(polProcessID,100)=processID, ORDER BY sourceTypeID,
    // polProcessID, modelYearID, fuelTypeID, regClassID, avgSpeedBinID.
    let mut rows: Vec<_> = inputs
        .sb_weighted_distance_rate
        .iter()
        .copied()
        .filter(|r| r.pol_process_id % 100 == flags.process_id)
        .collect();
    rows.sort_by(|a, b| {
        (
            a.source_type_id,
            a.pol_process_id,
            a.model_year_id,
            a.fuel_type_id,
            a.reg_class_id,
            a.avg_speed_bin_id,
        )
            .cmp(&(
                b.source_type_id,
                b.pol_process_id,
                b.model_year_id,
                b.fuel_type_id,
                b.reg_class_id,
                b.avg_speed_bin_id,
            ))
    });

    let mut records: BTreeMap<BaseRateOutputKey, BaseRateOutputRecord> = BTreeMap::new();
    let mut output: Vec<BaseRateOutputRecord> = Vec::new();
    let mut previous_key: Option<(i32, i32)> = None;

    for row in rows {
        let current = (row.source_type_id, row.pol_process_id);
        if previous_key.is_some_and(|p| p != current) {
            drain_records(&mut records, &mut output);
        }
        previous_key = Some(current);

        for &road_type_id in &prepared.run_spec_road_type {
            for &hour_day_id in &prepared.run_spec_hour_day {
                let detail = prepared
                    .avg_speed_distribution
                    .get(&AvgSpeedDistributionKey {
                        source_type_id: row.source_type_id,
                        road_type_id,
                        hour_day_id,
                        avg_speed_bin_id: row.avg_speed_bin_id,
                    });
                let (rate_avg_bin_speed, rate_avg_speed_fraction) = match detail {
                    Some(d) => (d.avg_bin_speed, d.avg_speed_fraction),
                    None => (0.0, 0.0),
                };

                let mut key = BaseRateOutputKey {
                    source_type_id: row.source_type_id,
                    road_type_id,
                    pol_process_id: row.pol_process_id,
                    hour_day_id,
                    model_year_id: row.model_year_id,
                    fuel_type_id: row.fuel_type_id,
                    reg_class_id: row.reg_class_id,
                    ..BaseRateOutputKey::default()
                };
                if flags.use_avg_speed_bin {
                    key.avg_speed_bin_id = row.avg_speed_bin_id;
                }

                let record = records.entry(key).or_insert_with(|| {
                    let mut rec = BaseRateOutputRecord {
                        source_type_id: row.source_type_id,
                        road_type_id,
                        pol_process_id: row.pol_process_id,
                        hour_day_id,
                        model_year_id: row.model_year_id,
                        fuel_type_id: row.fuel_type_id,
                        reg_class_id: row.reg_class_id,
                        age_group_id: 0,
                        process_id: row.pol_process_id % 100,
                        pollutant_id: row.pol_process_id / 100,
                        // Distance rates use operating mode 300 in rates mode.
                        op_mode_id: if flags.keep_op_mode_id { 300 } else { 0 },
                        ..BaseRateOutputRecord::default()
                    };
                    if flags.use_avg_speed_bin {
                        rec.avg_speed_bin_id = row.avg_speed_bin_id;
                    }
                    rec
                });

                let sum_sbd = if flags.use_sum_sbd { row.sum_sbd } else { 1.0 };
                let sum_sbd_raw = if flags.use_sum_sbd_raw {
                    row.sum_sbd_raw
                } else {
                    1.0
                };
                let avg_bin_speed = rate_avg_bin_speed;
                let avg_speed_fraction = if flags.use_avg_speed_fraction {
                    rate_avg_speed_fraction
                } else {
                    1.0
                };

                // opModeFraction == 1 for distance rates (single mode 300).
                let mut t = avg_speed_fraction * sum_sbd;
                record.op_mode_fraction += t;
                record.op_mode_fraction_rate += t;

                t = avg_speed_fraction * sum_sbd_raw;
                record.mean_base_rate += row.mean_base_rate * t;
                record.mean_base_rate_im += row.mean_base_rate_im * t;
                record.mean_base_rate_ac_adj += row.mean_base_rate_ac_adj * t;
                record.mean_base_rate_im_ac_adj += row.mean_base_rate_im_ac_adj * t;

                if flags.use_avg_speed_bin {
                    if avg_bin_speed > 0.0 {
                        t = avg_speed_fraction / avg_bin_speed;
                        record.emission_rate += row.mean_base_rate * t;
                        record.emission_rate_im += row.mean_base_rate_im * t;
                        record.emission_rate_ac_adj += row.mean_base_rate_ac_adj * t;
                        record.emission_rate_im_ac_adj += row.mean_base_rate_im_ac_adj * t;
                    }
                } else {
                    t = avg_speed_fraction;
                    record.emission_rate += row.mean_base_rate * t;
                    record.emission_rate_im += row.mean_base_rate_im * t;
                    record.emission_rate_ac_adj += row.mean_base_rate_ac_adj * t;
                    record.emission_rate_im_ac_adj += row.mean_base_rate_im_ac_adj * t;
                }
            }
        }
    }
    drain_records(&mut records, &mut output);
    output
}

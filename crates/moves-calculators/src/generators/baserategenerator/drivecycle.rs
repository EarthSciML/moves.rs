//! Drive-cycle operating-mode distribution computation.
//!
//! Ports three Go functions:
//!
//! * `find_drive_cycles` — `findDriveCycles`: for every source type, road
//!   type and average-speed bin, find the drive schedules that bracket the
//!   bin speed and the fraction each contributes.
//! * `calculate_drive_cycle_op_mode_distribution` —
//!   `calculateDriveCycleOpModeDistribution`: bin each second of a drive
//!   schedule into an operating mode from its speed and vehicle-specific
//!   power, then return the per-mode time fraction.
//! * [`process_drive_cycles`] — `processDriveCycles`: combine the two,
//!   compute the driving-idle fraction, and enumerate the result as the
//!   [`RomdBlock`] stream the base-rate aggregators consume.
//!
//! The Go used `*SourceUseTypePhysicsMappingDetail` pointers as map keys
//! (pointer identity). The port keys instead by the record's index into
//! [`PreparedTables::source_use_type_physics_mapping`]; because that table
//! is de-duplicated (`SELECT DISTINCT`), index identity and value identity
//! coincide.
//!
//! [`PreparedTables::source_use_type_physics_mapping`]: super::inputs::PreparedTables::source_use_type_physics_mapping

use std::collections::{BTreeMap, BTreeSet};

use super::inputs::{BaseRateInputs, PreparedTables};
use super::model::{
    AvgSpeedDistributionKey, DriveScheduleAssocKey, DrivingIdleFractionRow, ExternalFlags,
    OperatingMode, RomdBlock, RomdKey, SourceUseTypePhysicsMappingDetail,
};

/// Key for one bracketed average-speed bin — Go `DriveCycleBracketedBinKey`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct BracketedBinKey {
    /// Index into the physics-mapping table.
    physics_index: usize,
    road_type_id: i32,
    avg_speed_bin_id: i32,
}

/// Detail for one bracketed bin — Go `DriveCycleBracketedBinDetail`.
#[derive(Debug, Clone, Default)]
struct BracketedBinDetail {
    /// Drive-schedule fraction keyed by drive-schedule id.
    schedule_fractions: BTreeMap<i32, f64>,
    /// Combined operating-mode fraction keyed by operating-mode id.
    op_mode_fractions: BTreeMap<i32, f64>,
}

/// Key for one physics / drive-schedule pair — Go
/// `DriveScheduleOpModeDistributionKey`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ScheduleOpModeKey {
    /// Index into the physics-mapping table.
    physics_index: usize,
    drive_schedule_id: i32,
}

/// Fast lookup key into the bracketed bins — Go `dcbFastKey`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct FastKey {
    source_type_id: i32,
    road_type_id: i32,
    avg_speed_bin_id: i32,
}

/// Per-second state while binning a drive schedule — Go `secondDetail`.
#[derive(Debug, Clone, Copy, Default)]
struct SecondDetail {
    has_op_mode: bool,
    op_mode_id: i32,
    speed: f64,
    acceleration: f64,
    vsp: f64,
    speed_mph: f64,
    acceleration_mph: f64,
}

/// The bracketed bins and physics/schedule pairs produced by
/// [`find_drive_cycles`].
type DriveCycles = (
    BTreeMap<BracketedBinKey, BracketedBinDetail>,
    BTreeMap<ScheduleOpModeKey, ()>,
);

/// Find the drive schedules that bracket every source-type / road-type /
/// average-speed-bin combination, and the fraction each schedule contributes.
///
/// Ports `findDriveCycles`. Returns the bracketed-bin map and the set of
/// distinct (physics, drive-schedule) pairs whose operating-mode
/// distributions must be computed.
#[must_use]
fn find_drive_cycles(prepared: &PreparedTables) -> DriveCycles {
    let mut bracketed_bins: BTreeMap<BracketedBinKey, BracketedBinDetail> = BTreeMap::new();
    let mut schedule_op_modes: BTreeMap<ScheduleOpModeKey, ()> = BTreeMap::new();

    let allowed_road_types: BTreeSet<i32> = prepared.run_spec_road_type.iter().copied().collect();

    for (&assoc_key, cycles) in &prepared.drive_schedule_assoc {
        let DriveScheduleAssocKey {
            source_type_id,
            road_type_id,
        } = assoc_key;
        if !allowed_road_types.contains(&road_type_id) {
            continue;
        }
        for (&avg_speed_bin_id, &avg_bin_speed) in &prepared.avg_speed_bin {
            // Find the drive schedules that bracket this bin speed.
            let mut best_low_id: i32 = -1;
            let mut best_low_speed: f64 = -100.0;
            let mut best_high_id: i32 = -1;
            let mut best_high_speed: f64 = 100_000.0;
            for &drive_schedule_id in cycles {
                let speed = prepared
                    .drive_schedule
                    .get(&drive_schedule_id)
                    .copied()
                    .unwrap_or(0.0);
                if speed <= avg_bin_speed && speed > best_low_speed {
                    best_low_speed = speed;
                    best_low_id = drive_schedule_id;
                }
                if speed >= avg_bin_speed && speed < best_high_speed {
                    best_high_speed = speed;
                    best_high_id = drive_schedule_id;
                }
            }

            // The farther a cycle's speed is from the bin speed, the less it
            // influences the result.
            let total_span = best_high_speed - best_low_speed;
            let (low_fraction, high_fraction) = if total_span <= 0.0 {
                (1.0, 0.0)
            } else if best_low_id < 0 {
                // All cycles were too fast — extrapolate from the closest.
                (0.0, 1.0)
            } else if best_high_id < 0 {
                // All cycles were too slow — extrapolate from the closest.
                (1.0, 0.0)
            } else {
                (
                    (best_high_speed - avg_bin_speed) / total_span,
                    (avg_bin_speed - best_low_speed) / total_span,
                )
            };

            let mut cycles_to_use: Vec<(i32, f64)> = Vec::with_capacity(2);
            if low_fraction > 0.0 {
                cycles_to_use.push((best_low_id, low_fraction));
            }
            if high_fraction > 0.0 {
                cycles_to_use.push((best_high_id, high_fraction));
            }

            // Record the mapping for every physics record on this source
            // type and each bracketing cycle.
            for (physics_index, physics) in
                prepared.source_use_type_physics_mapping.iter().enumerate()
            {
                if physics.real_source_type_id != source_type_id {
                    continue;
                }
                for &(schedule_id, fraction) in &cycles_to_use {
                    bracketed_bins
                        .entry(BracketedBinKey {
                            physics_index,
                            road_type_id,
                            avg_speed_bin_id,
                        })
                        .or_default()
                        .schedule_fractions
                        .insert(schedule_id, fraction);
                    schedule_op_modes.insert(
                        ScheduleOpModeKey {
                            physics_index,
                            drive_schedule_id: schedule_id,
                        },
                        (),
                    );
                }
            }
        }
    }

    (bracketed_bins, schedule_op_modes)
}

/// Compute the operating-mode distribution for one drive schedule, given the
/// physics terms to apply.
///
/// Ports `calculateDriveCycleOpModeDistribution`. Operating mode `501` is the
/// zero-speed special case; it is kept distinct here and folded into mode `1`
/// by the caller for pollutant/processes other than `11609`.
#[must_use]
fn calculate_drive_cycle_op_mode_distribution(
    seconds: &[(i32, f64)],
    physics: &SourceUseTypePhysicsMappingDetail,
    operating_modes: &BTreeMap<i32, OperatingMode>,
    is_project: bool,
) -> BTreeMap<i32, f64> {
    let mut op_mode_fractions: BTreeMap<i32, f64> = BTreeMap::new();
    if seconds.is_empty() {
        return op_mode_fractions;
    }

    let mut details: BTreeMap<i32, SecondDetail> = BTreeMap::new();
    let mut first_second: i32 = 999_999;
    let mut last_second: i32 = -999_999;

    for &(second, speed) in seconds {
        let detail = details.entry(second).or_default();
        detail.speed = speed;
        detail.speed_mph = speed * 0.44704;
        // Assign the idle operating mode to near-zero speeds.
        if detail.speed == 0.0 {
            // EMT-443: project scale uses 501 (no emissions), national scale
            // uses 1 to match real operations.
            detail.op_mode_id = if is_project { 501 } else { 1 };
            detail.has_op_mode = true;
        } else if detail.speed < 1.0 {
            detail.op_mode_id = 1;
            detail.has_op_mode = true;
        }
        first_second = first_second.min(second);
        last_second = last_second.max(second);
    }

    // Acceleration of every second beyond the first.
    for second in (first_second + 1)..=last_second {
        let then = details.get(&(second - 1)).copied();
        if let (Some(then), true) = (then, details.contains_key(&second)) {
            let now = details.get_mut(&second).expect("checked present");
            now.acceleration = now.speed - then.speed;
            now.acceleration_mph = now.speed_mph - then.speed_mph;
        }
    }
    // The first second copies the acceleration of the second second.
    if let Some(next) = details.get(&(first_second + 1)).copied() {
        if let Some(first) = details.get_mut(&first_second) {
            first.acceleration = next.acceleration;
            first.acceleration_mph = next.acceleration_mph;
        }
    }

    let mut op_mode_totals: BTreeMap<i32, i32> = BTreeMap::new();
    let mut total_seconds: i32 = 0;

    for second in first_second..=last_second {
        let Some(mut now) = details.get(&second).copied() else {
            continue;
        };
        if !now.has_op_mode {
            let back1 = details.get(&(second - 1)).copied();
            let back2 = details.get(&(second - 2)).copied();
            // Braking events.
            if now.acceleration <= -2.0 {
                now.op_mode_id = 0;
                now.has_op_mode = true;
            } else if let (Some(b1), Some(b2)) = (back1, back2) {
                if now.acceleration < -1.0 && b1.acceleration < -1.0 && b2.acceleration < -1.0 {
                    now.op_mode_id = 0;
                    now.has_op_mode = true;
                }
            }
            // Vehicle-specific power. Go used `math.Pow(x, 2)` / `Pow(x, 3)`,
            // which for integer exponents reduce exactly to `x*x` and
            // `x*(x*x)` — reproduced here for bit-level fidelity.
            if !now.has_op_mode && physics.source_mass > 0.0 && physics.fixed_mass_factor > 0.0 {
                let sq = now.speed_mph * now.speed_mph;
                let cube = now.speed_mph * sq;
                now.vsp = (physics.rolling_term_a * now.speed_mph
                    + physics.rotating_term_b * sq
                    + physics.drag_term_c * cube
                    + physics.source_mass * now.speed_mph * now.acceleration_mph)
                    / physics.fixed_mass_factor;
            }
            // Assign an operating mode from VSP and speed bounds.
            if !now.has_op_mode {
                for (&op_mode_id, m) in operating_modes {
                    if let Some(lo) = m.vsp_lower {
                        if now.vsp < lo {
                            continue;
                        }
                    }
                    if let Some(hi) = m.vsp_upper {
                        if now.vsp >= hi {
                            continue;
                        }
                    }
                    if let Some(lo) = m.speed_lower {
                        if now.speed < lo {
                            continue;
                        }
                    }
                    if let Some(hi) = m.speed_upper {
                        if now.speed >= hi {
                            continue;
                        }
                    }
                    now.has_op_mode = true;
                    now.op_mode_id = op_mode_id;
                    break;
                }
            }
        }
        details.insert(second, now);
        // The `second > 0` guard mirrors a quirk of the Java reference code.
        if now.has_op_mode && second > 0 {
            *op_mode_totals.entry(now.op_mode_id).or_insert(0) += 1;
            total_seconds += 1;
        }
    }

    if total_seconds > 0 {
        let one_over_total = 1.0 / f64::from(total_seconds);
        for (op_mode_id, seconds_in_mode) in op_mode_totals {
            op_mode_fractions.insert(op_mode_id, f64::from(seconds_in_mode) * one_over_total);
        }
    }
    op_mode_fractions
}

/// Output of [`process_drive_cycles`]: the romd-block stream and the
/// driving-idle-fraction rows.
pub struct DriveCycleOutput {
    /// `RatesOpModeDistribution` blocks for the base-rate aggregators.
    pub romd_blocks: Vec<RomdBlock>,
    /// `DrivingIdleFraction` rows for off-network idling.
    pub driving_idle_fraction: Vec<DrivingIdleFractionRow>,
}

/// Build operating-mode distributions from drive cycles and enumerate them as
/// [`RomdBlock`]s, also computing the driving-idle fraction.
///
/// Ports `processDriveCycles`. The `SaveROMD` debug table is not reproduced —
/// it has no effect on the generator's output.
#[must_use]
pub fn process_drive_cycles(
    inputs: &BaseRateInputs,
    prepared: &PreparedTables,
    flags: &ExternalFlags,
) -> DriveCycleOutput {
    let (mut bracketed_bins, schedule_op_modes) = find_drive_cycles(prepared);

    // Index the per-second speed samples by drive schedule.
    let mut seconds_by_schedule: BTreeMap<i32, Vec<(i32, f64)>> = BTreeMap::new();
    for row in &inputs.drive_schedule_second {
        seconds_by_schedule
            .entry(row.drive_schedule_id)
            .or_default()
            .push((row.second, row.speed));
    }

    // Operating-mode distribution for each (physics, drive-schedule) pair.
    let mut schedule_distributions: BTreeMap<ScheduleOpModeKey, BTreeMap<i32, f64>> =
        BTreeMap::new();
    for &key in schedule_op_modes.keys() {
        let physics = &prepared.source_use_type_physics_mapping[key.physics_index];
        let seconds = seconds_by_schedule
            .get(&key.drive_schedule_id)
            .map_or(&[][..], Vec::as_slice);
        let distribution = calculate_drive_cycle_op_mode_distribution(
            seconds,
            physics,
            &prepared.operating_modes,
            inputs.is_project,
        );
        schedule_distributions.insert(key, distribution);
    }

    // Combine the per-schedule distributions, weighted by schedule fraction.
    for (key, detail) in &mut bracketed_bins {
        for (&drive_schedule_id, &schedule_fraction) in &detail.schedule_fractions {
            let Some(distribution) = schedule_distributions.get(&ScheduleOpModeKey {
                physics_index: key.physics_index,
                drive_schedule_id,
            }) else {
                continue;
            };
            for (&op_mode_id, &op_mode_fraction) in distribution {
                *detail.op_mode_fractions.entry(op_mode_id).or_insert(0.0) +=
                    schedule_fraction * op_mode_fraction;
            }
        }
    }

    // Operating modes to iterate: idle/brake specials first, then the binned
    // modes. The Go comment guarantees order within this list is immaterial.
    let mut op_modes_to_iterate: Vec<i32> = vec![0, 1, 501];
    op_modes_to_iterate.extend(prepared.operating_modes.keys().copied());

    // Group the bracketed bins for fast lookup by real source type.
    let mut bins_fast: BTreeMap<FastKey, Vec<BracketedBinKey>> = BTreeMap::new();
    for &key in bracketed_bins.keys() {
        let physics = &prepared.source_use_type_physics_mapping[key.physics_index];
        bins_fast
            .entry(FastKey {
                source_type_id: physics.real_source_type_id,
                road_type_id: key.road_type_id,
                avg_speed_bin_id: key.avg_speed_bin_id,
            })
            .or_default()
            .push(key);
    }

    // Driving-idle fraction, needed for off-network idling.
    let mut driving_idle_fraction: Vec<DrivingIdleFractionRow> = Vec::new();
    for &source_type_id in &prepared.run_spec_source_type {
        for &road_type_id in &prepared.run_spec_road_type {
            for &hour_day_id in &prepared.run_spec_hour_day {
                let mut idling_fraction = 0.0;
                let mut not_idling_fraction = 0.0;
                for &op_mode_id in &op_modes_to_iterate {
                    for &avg_speed_bin_id in prepared.avg_speed_bin.keys() {
                        let Some(avg_speed_detail) =
                            prepared
                                .avg_speed_distribution
                                .get(&AvgSpeedDistributionKey {
                                    source_type_id,
                                    road_type_id,
                                    hour_day_id,
                                    avg_speed_bin_id,
                                })
                        else {
                            continue;
                        };
                        if avg_speed_detail.avg_speed_fraction <= 0.0 {
                            continue;
                        }
                        let Some(keys) = bins_fast.get(&FastKey {
                            source_type_id,
                            road_type_id,
                            avg_speed_bin_id,
                        }) else {
                            continue;
                        };
                        for key in keys {
                            let detail = &bracketed_bins[key];
                            if detail.op_mode_fractions.is_empty() {
                                continue;
                            }
                            let op_mode_fraction = detail
                                .op_mode_fractions
                                .get(&op_mode_id)
                                .copied()
                                .unwrap_or(0.0);
                            if op_mode_fraction <= 0.0 {
                                continue;
                            }
                            if op_mode_id == 1 || op_mode_id == 501 {
                                idling_fraction +=
                                    op_mode_fraction * avg_speed_detail.avg_speed_fraction;
                            } else {
                                not_idling_fraction +=
                                    op_mode_fraction * avg_speed_detail.avg_speed_fraction;
                            }
                        }
                    }
                }
                let total_fraction = idling_fraction + not_idling_fraction;
                if total_fraction > 0.0 {
                    driving_idle_fraction.push(DrivingIdleFractionRow {
                        hour_day_id,
                        year_id: flags.year_id,
                        road_type_id,
                        source_type_id,
                        driving_idle_fraction: idling_fraction / total_fraction,
                    });
                }
            }
        }
    }

    // Regulatory classes per source type, discovered from the bracketed bins.
    let mut reg_classes_by_source_type: BTreeMap<i32, Vec<i32>> = BTreeMap::new();
    for &key in bracketed_bins.keys() {
        let physics = &prepared.source_use_type_physics_mapping[key.physics_index];
        let reg_classes = reg_classes_by_source_type
            .entry(physics.real_source_type_id)
            .or_default();
        if !reg_classes.contains(&physics.reg_class_id) {
            reg_classes.push(physics.reg_class_id);
        }
    }

    // Enumerate the result as RatesOpModeDistribution blocks. The nesting —
    // sourceType, polProcess, roadType, hourDay, opMode, avgSpeedBin — is the
    // ORDER BY the base-rate aggregators' flush logic depends on.
    let mut romd_blocks: Vec<RomdBlock> = Vec::new();
    for &source_type_id in &prepared.run_spec_source_type {
        for &pol_process_id in &prepared.run_spec_pol_process_id {
            for &road_type_id in &prepared.run_spec_road_type_with_off_network {
                // Only Running Exhaust uses off-network roads here.
                if road_type_id == 1 && pol_process_id % 100 != 1 {
                    continue;
                }
                for &hour_day_id in &prepared.run_spec_hour_day {
                    if road_type_id == 1 {
                        if let Some(reg_classes) = reg_classes_by_source_type.get(&source_type_id) {
                            for &reg_class_id in reg_classes {
                                romd_blocks.push(RomdBlock {
                                    key: RomdKey {
                                        source_type_id,
                                        pol_process_id,
                                        road_type_id,
                                        hour_day_id,
                                        // avgSpeedBinID 0 so workers pick up
                                        // off-network emissions.
                                        avg_speed_bin_id: 0,
                                        op_mode_id: 1, // idle
                                        begin_model_year_id: 1950,
                                        end_model_year_id: 2060,
                                        reg_class_id,
                                    },
                                    op_mode_fraction: 1.0, // 100% at idle
                                    // 1 mph so the per-distance rate equals
                                    // the per-hour rate.
                                    avg_bin_speed: 1.0,
                                    avg_speed_fraction: 1.0, // 100% lowest bin
                                });
                            }
                        }
                        continue;
                    }
                    for &op_mode_id in &op_modes_to_iterate {
                        if pol_process_id != 11609 && op_mode_id == 501 {
                            continue;
                        }
                        for &avg_speed_bin_id in prepared.avg_speed_bin.keys() {
                            let Some(avg_speed_detail) =
                                prepared
                                    .avg_speed_distribution
                                    .get(&AvgSpeedDistributionKey {
                                        source_type_id,
                                        road_type_id,
                                        hour_day_id,
                                        avg_speed_bin_id,
                                    })
                            else {
                                continue;
                            };
                            if avg_speed_detail.avg_speed_fraction <= 0.0 {
                                continue;
                            }
                            let Some(keys) = bins_fast.get(&FastKey {
                                source_type_id,
                                road_type_id,
                                avg_speed_bin_id,
                            }) else {
                                continue;
                            };
                            for key in keys {
                                let detail = &bracketed_bins[key];
                                if detail.op_mode_fractions.is_empty() {
                                    continue;
                                }
                                let mut op_mode_fraction = detail
                                    .op_mode_fractions
                                    .get(&op_mode_id)
                                    .copied()
                                    .unwrap_or(0.0);
                                if pol_process_id != 11609 {
                                    if op_mode_id == 501 {
                                        continue;
                                    } else if op_mode_id == 1 {
                                        // Fold the zero-speed mode into idle.
                                        op_mode_fraction += detail
                                            .op_mode_fractions
                                            .get(&501)
                                            .copied()
                                            .unwrap_or(0.0);
                                    }
                                }
                                if op_mode_fraction <= 0.0 {
                                    continue;
                                }
                                let physics =
                                    &prepared.source_use_type_physics_mapping[key.physics_index];
                                romd_blocks.push(RomdBlock {
                                    key: RomdKey {
                                        source_type_id,
                                        pol_process_id,
                                        road_type_id,
                                        hour_day_id,
                                        avg_speed_bin_id,
                                        op_mode_id,
                                        begin_model_year_id: physics.begin_model_year_id,
                                        end_model_year_id: physics.end_model_year_id,
                                        reg_class_id: physics.reg_class_id,
                                    },
                                    op_mode_fraction,
                                    avg_bin_speed: avg_speed_detail.avg_bin_speed,
                                    avg_speed_fraction: avg_speed_detail.avg_speed_fraction,
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    DriveCycleOutput {
        romd_blocks,
        driving_idle_fraction,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A drive schedule of all-idle seconds bins entirely into mode 1 at
    /// national scale.
    #[test]
    fn all_idle_schedule_bins_to_mode_one() {
        let seconds: Vec<(i32, f64)> = (1..=10).map(|s| (s, 0.0)).collect();
        let physics = SourceUseTypePhysicsMappingDetail::default();
        let modes = BTreeMap::new();
        let dist = calculate_drive_cycle_op_mode_distribution(&seconds, &physics, &modes, false);
        assert_eq!(dist.get(&1).copied(), Some(1.0));
    }

    /// At project scale, zero-speed seconds bin into the 501 special mode.
    #[test]
    fn project_scale_zero_speed_is_mode_501() {
        let seconds: Vec<(i32, f64)> = (1..=4).map(|s| (s, 0.0)).collect();
        let physics = SourceUseTypePhysicsMappingDetail::default();
        let modes = BTreeMap::new();
        let dist = calculate_drive_cycle_op_mode_distribution(&seconds, &physics, &modes, true);
        assert_eq!(dist.get(&501).copied(), Some(1.0));
    }

    /// A hard deceleration is binned as braking (operating mode 0).
    #[test]
    fn hard_deceleration_bins_to_braking() {
        // Second 0 establishes a high speed; second 1 drops 3 m/s.
        let seconds = vec![(0, 5.0), (1, 2.0)];
        let physics = SourceUseTypePhysicsMappingDetail::default();
        let modes = BTreeMap::new();
        let dist = calculate_drive_cycle_op_mode_distribution(&seconds, &physics, &modes, false);
        // Second 0 is excluded by the `second > 0` guard; second 1 has
        // acceleration -3 and bins to braking.
        assert_eq!(dist.get(&0).copied(), Some(1.0));
    }

    #[test]
    fn empty_schedule_yields_empty_distribution() {
        let physics = SourceUseTypePhysicsMappingDetail::default();
        let modes = BTreeMap::new();
        let dist = calculate_drive_cycle_op_mode_distribution(&[], &physics, &modes, false);
        assert!(dist.is_empty());
    }
}

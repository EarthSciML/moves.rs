//! The OMDG-1 … OMDG-7 computation pipeline.
//!
//! Ports the seven SQL stages of `OperatingModeDistributionGenerator.java`
//! (`executeLoop` → `bracketAverageSpeedBins` … `preliminaryCalculateOpModeFractions`
//! → `calculateOpModeFractions`) into pure functions over the projected input
//! tables in [`super::inputs`]. See the crate module documentation
//! ([`super`]) for the scope of the port and the numeric-fidelity notes.

use std::collections::{BTreeMap, BTreeSet};

use moves_data::{PolProcessId, RoadTypeId, SourceTypeId};

use super::inputs::{OmdgInputs, OperatingModeRow, PhysicsMappingRow};

/// Operating mode `0` — braking.
const BRAKING_OP_MODE: i16 = 0;
/// Operating mode `1` — idle.
const IDLE_OP_MODE: i16 = 1;
/// Operating mode `501` — the zero-speed special case, used only by
/// pol/process 11609.
const ZERO_SPEED_OP_MODE: i16 = 501;
/// pol/process `11609` — whose zero-speed seconds bin to [`ZERO_SPEED_OP_MODE`]
/// rather than [`IDLE_OP_MODE`]. Java OMDG-5:
/// `IF(speed=0 and polProcessID=11609,501,if(speed<1.0,1,opModeID))`.
const ZERO_SPEED_OP_MODE_POL_PROCESS: PolProcessId = PolProcessId(11609);
/// pol/process `11710` — excluded from the OMDG-7 preliminary aggregation.
/// Java OMDG-7: `omppa.polProcessID not in (11710)`.
const EXCLUDED_POL_PROCESS: PolProcessId = PolProcessId(11710);
/// Miles per hour to metres per second — the Java constant `0.44704`.
const MPH_TO_MPS: f64 = 0.44704;

/// Key into the bracketed-bin map: `(sourceType, roadType, avgSpeedBin)`.
pub type BinKey = (SourceTypeId, RoadTypeId, i16);
/// Key into a drive-schedule-fraction map:
/// `(sourceType, roadType, hourDay, driveSchedule)`.
type DriveScheduleFractionKey = (SourceTypeId, RoadTypeId, i16, i16);
/// Key into a per-schedule operating-mode-fraction map:
/// `(sourceType, driveSchedule, polProcess, opMode)`.
type ScheduleFractionKey = (SourceTypeId, i16, PolProcessId, i16);
/// Candidate `1 < opModeID < 100` operating modes per pol/process, each sorted
/// by `opModeID` — the VSP/speed-binning lookup the OMDG-5 step walks.
type OpModeCandidates = BTreeMap<PolProcessId, Vec<(i16, OperatingModeRow)>>;
/// Drive-schedule fractions grouped by their `(sourceType, driveSchedule)`
/// join key, each entry a `(roadType, hourDay, fraction)` triple.
type ScheduleFractionsByJoinKey = BTreeMap<(SourceTypeId, i16), Vec<(RoadTypeId, i16, f64)>>;

/// One bracketed average-speed bin — the drive schedules whose average speeds
/// straddle the bin's nominal speed.
///
/// Ports the `BracketScheduleLo` / `BracketScheduleHi` rows of OMDG-1. `lo_*`
/// is the fastest driving cycle no faster than the bin (or, when every cycle
/// is faster, the slowest cycle — the out-of-bounds clamp); `hi_*` is the
/// slowest cycle strictly faster than the bin (or the fastest cycle when every
/// cycle is slower). Two cycles may share a bracket speed, so each side holds
/// a `Vec`.
#[derive(Debug, Clone, PartialEq)]
pub struct BracketBin {
    /// Average speed of the lo-bracket driving cycle(s), mph.
    pub lo_speed: f64,
    /// Average speed of the hi-bracket driving cycle(s), mph.
    pub hi_speed: f64,
    /// `driveScheduleID`s whose average speed equals [`lo_speed`](Self::lo_speed).
    pub lo_schedules: Vec<i16>,
    /// `driveScheduleID`s whose average speed equals [`hi_speed`](Self::hi_speed).
    pub hi_schedules: Vec<i16>,
}

/// One `OpModeFraction2` row — the operating-mode fraction this generator
/// computes for a `(sourceType, roadType, hourDay, polProcess, opMode)`
/// combination.
///
/// This is the natural compute-stage output of OMDG: the Java's final
/// `calculateOpModeFractions` step cross-joins `OpModeFraction2` with `Link`
/// (`roadTypeID → linkID`) to populate the link-keyed `OpModeDistribution`
/// table — a data-plane projection [`Generator::execute`] performs once the
/// Task 50 `DataFrameStore` lands.
///
/// `source_type_id` may be a *temporary* source type when `SourceTypePhysics`
/// has split a source type by model-year range; see [`super`].
///
/// [`Generator::execute`]: moves_framework::Generator::execute
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OpModeFractionRow {
    /// `sourceTypeID` (real or temporary).
    pub source_type_id: SourceTypeId,
    /// `roadTypeID`.
    pub road_type_id: RoadTypeId,
    /// `hourDayID`.
    pub hour_day_id: i16,
    /// `polProcessID`.
    pub pol_process_id: PolProcessId,
    /// `opModeID`.
    pub op_mode_id: i16,
    /// `opModeFraction` — the operating-mode fraction; the fractions for one
    /// `(sourceType, roadType, hourDay, polProcess)` sum to 1 when every
    /// second of the contributing drive schedules was assigned a mode.
    pub op_mode_fraction: f64,
}

impl OpModeFractionRow {
    /// Primary-key projection, used to order the output deterministically.
    fn key(&self) -> (SourceTypeId, RoadTypeId, i16, PolProcessId, i16) {
        (
            self.source_type_id,
            self.road_type_id,
            self.hour_day_id,
            self.pol_process_id,
            self.op_mode_id,
        )
    }
}

/// Port of `validateDriveScheduleDistribution` — check that every
/// `(sourceType, roadType)` the run models has at least one non-ramp driving
/// cycle.
///
/// Returns the `(sourceType, roadType)` pairs that lack one; an empty result
/// means the drive-schedule distribution is valid. The Java method returns a
/// single boolean and aborts the generator on the first failure.
///
/// # Faithful-to-intent note
///
/// The Java `SELECT` lists two columns (`SourceTypeID`, `RoadTypeID`) yet the
/// loop reads a third — `result.getString(3)` — to test `isRamp`. That column
/// index is out of range, so the live Java throws and the method's `catch`
/// returns `false`. This port restores the method's evident intent (read the
/// `isRamp` flag, carried explicitly by [`DriveScheduleAssocRow`]) rather than
/// reproducing the column-index defect.
///
/// [`DriveScheduleAssocRow`]: super::inputs::DriveScheduleAssocRow
#[must_use]
pub fn validate_drive_schedule_distribution(
    inputs: &OmdgInputs<'_>,
) -> Vec<(SourceTypeId, RoadTypeId)> {
    let run_spec_source: BTreeSet<SourceTypeId> =
        inputs.run_spec_source_type.iter().copied().collect();
    let run_spec_road: BTreeSet<RoadTypeId> = inputs.run_spec_road_type.iter().copied().collect();

    let mut has_non_ramp: BTreeMap<(SourceTypeId, RoadTypeId), bool> = BTreeMap::new();
    for assoc in inputs.drive_schedule_assoc {
        if !run_spec_source.contains(&assoc.source_type_id)
            || !run_spec_road.contains(&assoc.road_type_id)
        {
            continue;
        }
        let entry = has_non_ramp
            .entry((assoc.source_type_id, assoc.road_type_id))
            .or_insert(false);
        *entry = *entry || !assoc.is_ramp;
    }
    has_non_ramp
        .into_iter()
        .filter(|&(_, ok)| !ok)
        .map(|(pair, _)| pair)
        .collect()
}

/// Port of `bracketAverageSpeedBins` (OMDG-1).
///
/// For every `(sourceType, roadType)` the RunSpec selects, bracket each
/// average-speed bin between the two driving cycles whose average speeds
/// straddle it. A bin faster than every cycle, or slower than every cycle, is
/// clamped to the cycle bound — the `isOutOfBounds` path in the Java.
#[must_use]
pub fn bracket_average_speed_bins(inputs: &OmdgInputs<'_>) -> BTreeMap<BinKey, BracketBin> {
    let run_spec_source: BTreeSet<SourceTypeId> =
        inputs.run_spec_source_type.iter().copied().collect();
    let run_spec_road: BTreeSet<RoadTypeId> = inputs.run_spec_road_type.iter().copied().collect();
    let speed_of: BTreeMap<i16, f64> = inputs
        .drive_schedule
        .iter()
        .map(|cycle| (cycle.drive_schedule_id, cycle.average_speed))
        .collect();

    // Driving cycles per (sourceType, roadType), restricted to the RunSpec.
    let mut cycles_by_combination: BTreeMap<(SourceTypeId, RoadTypeId), Vec<(i16, f64)>> =
        BTreeMap::new();
    for assoc in inputs.drive_schedule_assoc {
        if !run_spec_source.contains(&assoc.source_type_id)
            || !run_spec_road.contains(&assoc.road_type_id)
        {
            continue;
        }
        if let Some(&speed) = speed_of.get(&assoc.drive_schedule_id) {
            cycles_by_combination
                .entry((assoc.source_type_id, assoc.road_type_id))
                .or_default()
                .push((assoc.drive_schedule_id, speed));
        }
    }

    let mut brackets: BTreeMap<BinKey, BracketBin> = BTreeMap::new();
    for (&(source_type_id, road_type_id), cycles) in &cycles_by_combination {
        let Some(bound_lo) = cycles.iter().map(|&(_, speed)| speed).reduce(f64::min) else {
            continue;
        };
        let bound_hi = cycles
            .iter()
            .map(|&(_, speed)| speed)
            .reduce(f64::max)
            .unwrap_or(bound_lo);
        for bin in inputs.avg_speed_bin {
            let bin_speed = bin.avg_bin_speed;
            // Lo bracket: fastest cycle no faster than the bin; clamp up to
            // the slowest cycle when every cycle is faster.
            let lo_speed = cycles
                .iter()
                .map(|&(_, speed)| speed)
                .filter(|&speed| speed <= bin_speed)
                .reduce(f64::max)
                .unwrap_or(bound_lo);
            // Hi bracket: slowest cycle strictly faster than the bin; clamp
            // down to the fastest cycle when every cycle is slower.
            let hi_speed = cycles
                .iter()
                .map(|&(_, speed)| speed)
                .filter(|&speed| speed > bin_speed)
                .reduce(f64::min)
                .unwrap_or(bound_hi);
            brackets.insert(
                (source_type_id, road_type_id, bin.avg_speed_bin_id),
                BracketBin {
                    lo_speed,
                    hi_speed,
                    lo_schedules: schedules_at_speed(cycles, lo_speed),
                    hi_schedules: schedules_at_speed(cycles, hi_speed),
                },
            );
        }
    }
    brackets
}

/// The `driveScheduleID`s whose average speed equals `speed`.
///
/// `speed` is always copied verbatim from a `cycles` entry, so this exact
/// equality test is the faithful port of the SQL equi-join
/// `ds.averageSpeed = bsl.loScheduleSpeed` — not an approximate comparison.
#[allow(clippy::float_cmp)]
fn schedules_at_speed(cycles: &[(i16, f64)], speed: f64) -> Vec<i16> {
    cycles
        .iter()
        .filter(|&&(_, cycle_speed)| cycle_speed == speed)
        .map(|&(drive_schedule_id, _)| drive_schedule_id)
        .collect()
}

/// The lo-bracket cycle's share of a bin, per OMDG-2.
///
/// Java: `loScheduleFraction = (hiSpeed - binSpeed) / (hiSpeed - loSpeed)`
/// when `hiSpeed <> loSpeed`, and `1` otherwise. `lo_speed` / `hi_speed` are
/// copied from drive-schedule data, so the equality is the exact SQL `<>`
/// test.
#[allow(clippy::float_cmp)]
fn lo_bracket_share(bin_speed: f64, lo_speed: f64, hi_speed: f64) -> f64 {
    if hi_speed == lo_speed {
        1.0
    } else {
        (hi_speed - bin_speed) / (hi_speed - lo_speed)
    }
}

/// Port of `determineDriveScheduleProportions` (OMDG-2) and
/// `determineDriveScheduleDistributionNonRamp` (OMDG-3).
///
/// Splits each bin's average-speed-distribution fraction between its
/// bracketing cycles, then sums each cycle's lo and hi shares across all bins
/// to give the per-`(sourceType, roadType, hourDay, driveSchedule)` drive
/// schedule fraction. Finally copies every real source type's fractions onto
/// its temporary source types (the OMDG-3 `sourceUseTypePhysicsMapping` step).
fn drive_schedule_fractions(
    inputs: &OmdgInputs<'_>,
    brackets: &BTreeMap<BinKey, BracketBin>,
) -> BTreeMap<DriveScheduleFractionKey, f64> {
    let bin_speed: BTreeMap<i16, f64> = inputs
        .avg_speed_bin
        .iter()
        .map(|bin| (bin.avg_speed_bin_id, bin.avg_bin_speed))
        .collect();
    let mut speed_fraction: BTreeMap<(SourceTypeId, RoadTypeId, i16, i16), f64> = BTreeMap::new();
    for row in inputs.avg_speed_distribution {
        speed_fraction.insert(
            (
                row.source_type_id,
                row.road_type_id,
                row.hour_day_id,
                row.avg_speed_bin_id,
            ),
            row.avg_speed_fraction,
        );
    }
    let hour_days: BTreeSet<i16> = inputs.run_spec_hour_day.iter().copied().collect();

    // OMDG-2: weight each bracketing cycle's share by the bin's time fraction.
    let mut lo_fraction: BTreeMap<DriveScheduleFractionKey, f64> = BTreeMap::new();
    let mut hi_fraction: BTreeMap<DriveScheduleFractionKey, f64> = BTreeMap::new();
    for (&(source_type_id, road_type_id, bin_id), bracket) in brackets {
        let Some(&bin_avg_speed) = bin_speed.get(&bin_id) else {
            continue;
        };
        let lo_share = lo_bracket_share(bin_avg_speed, bracket.lo_speed, bracket.hi_speed);
        let hi_share = 1.0 - lo_share;
        for &hour_day_id in &hour_days {
            let Some(&fraction) =
                speed_fraction.get(&(source_type_id, road_type_id, hour_day_id, bin_id))
            else {
                continue;
            };
            let lo_weighted = lo_share * fraction;
            let hi_weighted = hi_share * fraction;
            for &drive_schedule_id in &bracket.lo_schedules {
                *lo_fraction
                    .entry((source_type_id, road_type_id, hour_day_id, drive_schedule_id))
                    .or_insert(0.0) += lo_weighted;
            }
            for &drive_schedule_id in &bracket.hi_schedules {
                *hi_fraction
                    .entry((source_type_id, road_type_id, hour_day_id, drive_schedule_id))
                    .or_insert(0.0) += hi_weighted;
            }
        }
    }
    // OMDG-3: a cycle's drive-schedule fraction is the sum of the lo and hi
    // shares it accumulated. A cycle that brackets bins from only one side
    // contributes that side alone.
    let mut drive_schedule_fraction = lo_fraction;
    for (key, fraction) in hi_fraction {
        *drive_schedule_fraction.entry(key).or_insert(0.0) += fraction;
    }
    // OMDG-3: copy each real source type's rows onto its temporary source
    // types. `INSERT IGNORE` — an existing key is never overwritten.
    let mut temp_rows: Vec<(DriveScheduleFractionKey, f64)> = Vec::new();
    for mapping in inputs.physics_mapping {
        if mapping.temp_source_type_id == mapping.real_source_type_id {
            continue;
        }
        for (&(source_type_id, road_type_id, hour_day_id, drive_schedule_id), &fraction) in
            &drive_schedule_fraction
        {
            if source_type_id == mapping.real_source_type_id {
                temp_rows.push((
                    (
                        mapping.temp_source_type_id,
                        road_type_id,
                        hour_day_id,
                        drive_schedule_id,
                    ),
                    fraction,
                ));
            }
        }
    }
    for (key, fraction) in temp_rows {
        drive_schedule_fraction.entry(key).or_insert(fraction);
    }
    drive_schedule_fraction
}

/// Vehicle-specific power for one second, per the OMDG-4 `VSP` insert.
///
/// Java SQL: `(rollingTermA*speed + rotatingTermB*POW(speed,2) +
/// dragTermC*POW(speed,3) + sourceMass*speed*acceleration) / fixedMassFactor`,
/// with `speed` / `acceleration` in metres per second. `POW(x,2)` / `POW(x,3)`
/// with integer exponents reduce exactly to `x*x` / `x*x*x`; written out here,
/// matching the sibling `baserategenerator::drivecycle` port.
fn vehicle_specific_power(speed_ms: f64, acceleration_ms: f64, physics: &PhysicsMappingRow) -> f64 {
    let square = speed_ms * speed_ms;
    let cube = square * speed_ms;
    (physics.rolling_term_a * speed_ms
        + physics.rotating_term_b * square
        + physics.drag_term_c * cube
        + physics.source_mass * speed_ms * acceleration_ms)
        / physics.fixed_mass_factor
}

/// Whether a second's VSP and speed fall inside an operating mode's bounds.
///
/// Java OMDG-5: `VSP >= VSPLower AND VSP < VSPUpper AND speed >= speedLower
/// AND speed < speedUpper`, with a `NULL` bound omitting its clause. `speed`
/// is in miles per hour.
fn op_mode_matches(vsp: f64, speed_mph: f64, mode: &OperatingModeRow) -> bool {
    if let Some(lower) = mode.vsp_lower {
        if vsp < lower {
            return false;
        }
    }
    if let Some(upper) = mode.vsp_upper {
        if vsp >= upper {
            return false;
        }
    }
    if let Some(lower) = mode.speed_lower {
        if speed_mph < lower {
            return false;
        }
    }
    if let Some(upper) = mode.speed_upper {
        if speed_mph >= upper {
            return false;
        }
    }
    true
}

/// Assign one second's operating mode for one pol/process — the per-second
/// logic of OMDG-5.
///
/// Applied in Java order: braking first (`acceleration <= -2`, or three
/// consecutive seconds all below `-1`), then VSP/speed binning over the
/// candidate modes (only those left unassigned), and finally the idle
/// override (`speed = 0` for pol/process 11609 → mode 501; `speed < 1` → mode
/// 1). `prev_accelerations` are the accelerations of the two preceding binned
/// seconds, each `None` when that second was not itself binned.
///
/// `candidates` must be the `1 < opModeID < 100` operating modes for this
/// pol/process, sorted by `opModeID` — the first match wins, which is exact
/// for the non-overlapping MOVES VSP/speed bins. Returns `None` when no mode
/// applies (mirrors a `NULL` `opModeID`, which the OMDG-7 join then drops).
fn assign_operating_mode(
    speed_mph: f64,
    acceleration_mph: f64,
    prev_accelerations: (Option<f64>, Option<f64>),
    vsp: f64,
    pol_process_id: PolProcessId,
    candidates: &[(i16, OperatingModeRow)],
) -> Option<i16> {
    let mut op_mode: Option<i16> = None;
    // Braking — Java OMDG-5 `OpModeIDBySecond_Temp` plus the `acceleration <=
    // -2` update.
    if acceleration_mph <= -2.0 {
        op_mode = Some(BRAKING_OP_MODE);
    } else if let (Some(prev1), Some(prev2)) = prev_accelerations {
        if acceleration_mph < -1.0 && prev1 < -1.0 && prev2 < -1.0 {
            op_mode = Some(BRAKING_OP_MODE);
        }
    }
    // VSP / speed binning — only seconds still unassigned (`opModeID IS NULL`).
    if op_mode.is_none() {
        for &(op_mode_id, mode) in candidates {
            if op_mode_matches(vsp, speed_mph, &mode) {
                op_mode = Some(op_mode_id);
                break;
            }
        }
    }
    // Idle override — applied last, so it wins over braking and VSP binning.
    if speed_mph == 0.0 && pol_process_id == ZERO_SPEED_OP_MODE_POL_PROCESS {
        op_mode = Some(ZERO_SPEED_OP_MODE);
    } else if speed_mph < 1.0 {
        op_mode = Some(IDLE_OP_MODE);
    }
    op_mode
}

/// Index the per-second speed traces by `driveScheduleID`.
fn index_drive_schedule_seconds(inputs: &OmdgInputs<'_>) -> BTreeMap<i16, BTreeMap<i16, f64>> {
    let mut seconds: BTreeMap<i16, BTreeMap<i16, f64>> = BTreeMap::new();
    for row in inputs.drive_schedule_second {
        seconds
            .entry(row.drive_schedule_id)
            .or_default()
            .insert(row.second, row.speed);
    }
    seconds
}

/// The `(sourceType, driveSchedule)` pairs OMDG-4 builds into
/// `SourceTypeDriveSchedule` — those with a non-zero summed drive-schedule
/// fraction, expanded onto temporary source types.
fn source_type_drive_schedules(
    inputs: &OmdgInputs<'_>,
    drive_schedule_fraction: &BTreeMap<DriveScheduleFractionKey, f64>,
) -> BTreeSet<(SourceTypeId, i16)> {
    let mut fraction_sum: BTreeMap<(SourceTypeId, i16), f64> = BTreeMap::new();
    for (&(source_type_id, _, _, drive_schedule_id), &fraction) in drive_schedule_fraction {
        *fraction_sum
            .entry((source_type_id, drive_schedule_id))
            .or_insert(0.0) += fraction;
    }
    let mut pairs: BTreeSet<(SourceTypeId, i16)> = fraction_sum
        .into_iter()
        .filter(|&(_, sum)| sum != 0.0)
        .map(|(pair, _)| pair)
        .collect();
    // `drive_schedule_fraction` already carries temp-source rows, so this
    // expansion adds nothing in practice — ported from OMDG-4 for fidelity.
    for mapping in inputs.physics_mapping {
        if mapping.temp_source_type_id == mapping.real_source_type_id {
            continue;
        }
        let temp_pairs: Vec<(SourceTypeId, i16)> = pairs
            .iter()
            .filter(|&&(source_type_id, _)| source_type_id == mapping.real_source_type_id)
            .map(|&(_, drive_schedule_id)| (mapping.temp_source_type_id, drive_schedule_id))
            .collect();
        pairs.extend(temp_pairs);
    }
    pairs
}

/// The OMDG-5 setup: the distinct non-represented pol/processes
/// (`OMDGPollutantProcess`) and, per pol/process, the `1 < opModeID < 100`
/// candidate operating modes (`OperatingMode` joined to
/// `OpModePolProcAssocTrimmed`), sorted by `opModeID`.
fn op_mode_candidates(inputs: &OmdgInputs<'_>) -> (BTreeSet<PolProcessId>, OpModeCandidates) {
    let represented: BTreeSet<PolProcessId> = inputs
        .pol_process_represented
        .iter()
        .map(|row| row.pol_process_id)
        .collect();
    let operating_mode_by_id: BTreeMap<i16, OperatingModeRow> = inputs
        .operating_mode
        .iter()
        .map(|mode| (mode.op_mode_id, *mode))
        .collect();
    let mut pol_processes: BTreeSet<PolProcessId> = BTreeSet::new();
    let mut candidates: OpModeCandidates = BTreeMap::new();
    for assoc in inputs.op_mode_pol_proc_assoc {
        if represented.contains(&assoc.pol_process_id) {
            continue;
        }
        pol_processes.insert(assoc.pol_process_id);
        if assoc.op_mode_id > 1 && assoc.op_mode_id < 100 {
            if let Some(&mode) = operating_mode_by_id.get(&assoc.op_mode_id) {
                candidates
                    .entry(assoc.pol_process_id)
                    .or_default()
                    .push((assoc.op_mode_id, mode));
            }
        }
    }
    for modes in candidates.values_mut() {
        modes.sort_unstable_by_key(|&(op_mode_id, _)| op_mode_id);
    }
    (pol_processes, candidates)
}

/// Bin every second of one drive schedule into operating modes, returning the
/// count of binned seconds and the per-`(polProcess, opMode)` second counts.
///
/// Ports the per-schedule core of OMDG-5/-6: build `DriveScheduleSecond3`
/// accelerations and `VSP` values, then count `OpModeIDBySecond`. A second is
/// binned only when it has both an acceleration (its predecessor is present)
/// and a VSP (it is also `second > 0`).
fn count_schedule_op_modes(
    speeds: &BTreeMap<i16, f64>,
    physics: &PhysicsMappingRow,
    pol_processes: &BTreeSet<PolProcessId>,
    candidates: &OpModeCandidates,
) -> (i32, BTreeMap<(PolProcessId, i16), i32>) {
    let mut acceleration_mph: BTreeMap<i16, f64> = BTreeMap::new();
    let mut vsp: BTreeMap<i16, f64> = BTreeMap::new();
    for (&second, &speed_mph) in speeds {
        let Some(&prev_speed_mph) = speeds.get(&(second - 1)) else {
            continue;
        };
        acceleration_mph.insert(second, speed_mph - prev_speed_mph);
        if second > 0 {
            let speed_ms = speed_mph * MPH_TO_MPS;
            let acceleration_ms = (speed_mph - prev_speed_mph) * MPH_TO_MPS;
            vsp.insert(
                second,
                vehicle_specific_power(speed_ms, acceleration_ms, physics),
            );
        }
    }
    let binned: BTreeSet<i16> = acceleration_mph
        .keys()
        .copied()
        .filter(|second| vsp.contains_key(second))
        .collect();

    let no_candidates: &[(i16, OperatingModeRow)] = &[];
    let mut second_count: i32 = 0;
    let mut mode_counts: BTreeMap<(PolProcessId, i16), i32> = BTreeMap::new();
    for &second in &binned {
        second_count += 1;
        let speed_mph = speeds[&second];
        let acceleration_mph_now = acceleration_mph[&second];
        // The three-consecutive-deceleration test needs the two preceding
        // seconds to be binned seconds too.
        let prev_accelerations = (
            binned
                .contains(&(second - 1))
                .then(|| acceleration_mph[&(second - 1)]),
            binned
                .contains(&(second - 2))
                .then(|| acceleration_mph[&(second - 2)]),
        );
        let vsp_now = vsp[&second];
        for &pol_process_id in pol_processes {
            let pol_process_candidates = candidates
                .get(&pol_process_id)
                .map_or(no_candidates, Vec::as_slice);
            if let Some(op_mode) = assign_operating_mode(
                speed_mph,
                acceleration_mph_now,
                prev_accelerations,
                vsp_now,
                pol_process_id,
                pol_process_candidates,
            ) {
                *mode_counts.entry((pol_process_id, op_mode)).or_insert(0) += 1;
            }
        }
    }
    (second_count, mode_counts)
}

/// Port of `calculateEnginePowerBySecond` (OMDG-4),
/// `determineOpModeIDPerSecond` (OMDG-5) and
/// `calculateOpModeFractionsPerDriveSchedule` (OMDG-6).
///
/// For every `(sourceType, driveSchedule)` pair, bins each second of the
/// driving cycle into an operating mode and divides the per-mode second count
/// by the total binned-second count — the `OpModeFractionBySchedule` table.
fn op_mode_fraction_by_schedule(
    inputs: &OmdgInputs<'_>,
    drive_schedule_fraction: &BTreeMap<DriveScheduleFractionKey, f64>,
) -> BTreeMap<ScheduleFractionKey, f64> {
    let pairs = source_type_drive_schedules(inputs, drive_schedule_fraction);
    let seconds_by_schedule = index_drive_schedule_seconds(inputs);
    let physics_by_temp_source: BTreeMap<SourceTypeId, &PhysicsMappingRow> = inputs
        .physics_mapping
        .iter()
        .map(|mapping| (mapping.temp_source_type_id, mapping))
        .collect();
    let (pol_processes, candidates) = op_mode_candidates(inputs);

    let mut result: BTreeMap<ScheduleFractionKey, f64> = BTreeMap::new();
    for &(source_type_id, drive_schedule_id) in &pairs {
        let Some(physics) = physics_by_temp_source.get(&source_type_id) else {
            continue;
        };
        // The Java VSP `SELECT` joins `sourceMass <> 0`; a zero
        // `fixedMassFactor` would make its division yield `NULL`. Either way
        // there is no usable VSP, so the source type contributes nothing.
        if physics.source_mass == 0.0 || physics.fixed_mass_factor == 0.0 {
            continue;
        }
        let Some(speeds) = seconds_by_schedule.get(&drive_schedule_id) else {
            continue;
        };
        let (second_count, mode_counts) =
            count_schedule_op_modes(speeds, physics, &pol_processes, &candidates);
        if second_count == 0 {
            continue;
        }
        for ((pol_process_id, op_mode), count) in mode_counts {
            result.insert(
                (source_type_id, drive_schedule_id, pol_process_id, op_mode),
                f64::from(count) / f64::from(second_count),
            );
        }
    }
    result
}

/// Port of `preliminaryCalculateOpModeFractions` (OMDG-7).
///
/// Weights each `OpModeFractionBySchedule` value by the corresponding drive
/// schedule fraction and sums over driving cycles, giving `OpModeFraction2` —
/// the operating-mode fraction per `(sourceType, roadType, hourDay, polProcess,
/// opMode)`. Keeps only `(polProcess, opMode)` pairs present in
/// `OpModePolProcAssoc` and drops the excluded pol/process 11710.
fn preliminary_op_mode_fractions(
    inputs: &OmdgInputs<'_>,
    drive_schedule_fraction: &BTreeMap<DriveScheduleFractionKey, f64>,
    op_mode_fraction_by_schedule: &BTreeMap<ScheduleFractionKey, f64>,
) -> BTreeMap<(SourceTypeId, RoadTypeId, i16, PolProcessId, i16), f64> {
    let assoc_pairs: BTreeSet<(PolProcessId, i16)> = inputs
        .op_mode_pol_proc_assoc
        .iter()
        .map(|assoc| (assoc.pol_process_id, assoc.op_mode_id))
        .collect();
    // Drive-schedule fractions indexed by their (sourceType, driveSchedule)
    // join key into `op_mode_fraction_by_schedule`.
    let mut fractions_by_source_schedule: ScheduleFractionsByJoinKey = BTreeMap::new();
    for (&(source_type_id, road_type_id, hour_day_id, drive_schedule_id), &fraction) in
        drive_schedule_fraction
    {
        fractions_by_source_schedule
            .entry((source_type_id, drive_schedule_id))
            .or_default()
            .push((road_type_id, hour_day_id, fraction));
    }

    let mut op_mode_fraction: BTreeMap<(SourceTypeId, RoadTypeId, i16, PolProcessId, i16), f64> =
        BTreeMap::new();
    for (&(source_type_id, drive_schedule_id, pol_process_id, op_mode), &mode_fraction) in
        op_mode_fraction_by_schedule
    {
        if pol_process_id == EXCLUDED_POL_PROCESS
            || !assoc_pairs.contains(&(pol_process_id, op_mode))
        {
            continue;
        }
        let Some(fractions) =
            fractions_by_source_schedule.get(&(source_type_id, drive_schedule_id))
        else {
            continue;
        };
        for &(road_type_id, hour_day_id, schedule_fraction) in fractions {
            *op_mode_fraction
                .entry((
                    source_type_id,
                    road_type_id,
                    hour_day_id,
                    pol_process_id,
                    op_mode,
                ))
                .or_insert(0.0) += mode_fraction * schedule_fraction;
        }
    }
    op_mode_fraction
}

/// Run the full OMDG-1 … OMDG-7 pipeline.
///
/// Ports `OperatingModeDistributionGenerator.executeLoop`'s setup phase: from
/// the projected input tables, produce every `OpModeFraction2` row, then apply
/// the `OMDGPolProcessRepresented` step — a represented pol/process receives a
/// copy of its representing pol/process's distribution. The result is sorted
/// by `(sourceType, roadType, hourDay, polProcess, opMode)`.
///
/// The link-keyed `OpModeDistribution` table is one cross-join away (with
/// `Link`, on `roadTypeID`); see [`OpModeFractionRow`].
#[must_use]
pub fn op_mode_distribution(inputs: &OmdgInputs<'_>) -> Vec<OpModeFractionRow> {
    let brackets = bracket_average_speed_bins(inputs);
    let drive_schedule_fraction = drive_schedule_fractions(inputs, &brackets);
    let by_schedule = op_mode_fraction_by_schedule(inputs, &drive_schedule_fraction);
    let op_mode_fraction =
        preliminary_op_mode_fractions(inputs, &drive_schedule_fraction, &by_schedule);

    let mut rows: Vec<OpModeFractionRow> = op_mode_fraction
        .iter()
        .map(
            |(
                &(source_type_id, road_type_id, hour_day_id, pol_process_id, op_mode_id),
                &fraction,
            )| {
                OpModeFractionRow {
                    source_type_id,
                    road_type_id,
                    hour_day_id,
                    pol_process_id,
                    op_mode_id,
                    op_mode_fraction: fraction,
                }
            },
        )
        .collect();
    // OMDG-7: a represented pol/process copies the rows of its representing
    // pol/process. `OpModeFraction2` never holds a represented pol/process
    // (it is excluded from `OMDGPollutantProcess`), so the copies never
    // collide with an existing row.
    for represented in inputs.pol_process_represented {
        let copies: Vec<OpModeFractionRow> = op_mode_fraction
            .iter()
            .filter(|(&(_, _, _, pol_process_id, _), _)| {
                pol_process_id == represented.representing_pol_process_id
            })
            .map(
                |(&(source_type_id, road_type_id, hour_day_id, _, op_mode_id), &fraction)| {
                    OpModeFractionRow {
                        source_type_id,
                        road_type_id,
                        hour_day_id,
                        pol_process_id: represented.pol_process_id,
                        op_mode_id,
                        op_mode_fraction: fraction,
                    }
                },
            )
            .collect();
        rows.extend(copies);
    }
    rows.sort_unstable_by_key(OpModeFractionRow::key);
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generators::operating_mode_distribution::inputs::{
        AvgSpeedBinRow, AvgSpeedDistributionRow, DriveScheduleAssocRow, DriveScheduleRow,
        DriveScheduleSecondRow, OpModePolProcAssocRow, PolProcessRepresentedRow,
    };

    /// Source / road / hour-day identifiers reused across the tests.
    const SOURCE_TYPE: SourceTypeId = SourceTypeId(21);
    const ROAD_TYPE: RoadTypeId = RoadTypeId(5);
    const HOUR_DAY: i16 = 51;
    /// A plain running-exhaust pol/process (pollutant 1, process 1) — neither
    /// the zero-speed (11609) nor the excluded (11710) special case.
    const POL_PROCESS: PolProcessId = PolProcessId(101);

    /// Physics terms that make VSP identically zero for a constant-speed
    /// second: only the (zeroed) acceleration term would contribute.
    fn flat_physics(real: SourceTypeId, temp: SourceTypeId) -> PhysicsMappingRow {
        PhysicsMappingRow {
            real_source_type_id: real,
            temp_source_type_id: temp,
            rolling_term_a: 0.0,
            rotating_term_b: 0.0,
            drag_term_c: 0.0,
            source_mass: 1000.0,
            fixed_mass_factor: 1.0,
        }
    }

    /// A constant-speed driving cycle: four seconds (0..=3) all at `speed`.
    fn constant_cycle(drive_schedule_id: i16, speed: f64) -> Vec<DriveScheduleSecondRow> {
        (0..=3)
            .map(|second| DriveScheduleSecondRow {
                drive_schedule_id,
                second,
                speed,
            })
            .collect()
    }

    #[test]
    fn validate_flags_combination_without_non_ramp_cycle() {
        let assoc = [
            DriveScheduleAssocRow {
                source_type_id: SOURCE_TYPE,
                road_type_id: ROAD_TYPE,
                drive_schedule_id: 1,
                is_ramp: true,
            },
            DriveScheduleAssocRow {
                source_type_id: SOURCE_TYPE,
                road_type_id: RoadTypeId(2),
                drive_schedule_id: 2,
                is_ramp: false,
            },
        ];
        let inputs = OmdgInputs {
            drive_schedule: &[],
            drive_schedule_assoc: &assoc,
            drive_schedule_second: &[],
            avg_speed_bin: &[],
            avg_speed_distribution: &[],
            operating_mode: &[],
            op_mode_pol_proc_assoc: &[],
            physics_mapping: &[],
            pol_process_represented: &[],
            run_spec_source_type: &[SOURCE_TYPE],
            run_spec_road_type: &[ROAD_TYPE, RoadTypeId(2)],
            run_spec_hour_day: &[HOUR_DAY],
        };
        // Road type 5 has only a ramp cycle; road type 2 has a non-ramp one.
        assert_eq!(
            validate_drive_schedule_distribution(&inputs),
            vec![(SOURCE_TYPE, ROAD_TYPE)],
        );
    }

    #[test]
    fn bracket_clamps_out_of_bounds_bins() {
        let drive_schedule = [
            DriveScheduleRow {
                drive_schedule_id: 1,
                average_speed: 10.0,
            },
            DriveScheduleRow {
                drive_schedule_id: 2,
                average_speed: 30.0,
            },
        ];
        let assoc = [
            DriveScheduleAssocRow {
                source_type_id: SOURCE_TYPE,
                road_type_id: ROAD_TYPE,
                drive_schedule_id: 1,
                is_ramp: false,
            },
            DriveScheduleAssocRow {
                source_type_id: SOURCE_TYPE,
                road_type_id: ROAD_TYPE,
                drive_schedule_id: 2,
                is_ramp: false,
            },
        ];
        let bins = [
            AvgSpeedBinRow {
                avg_speed_bin_id: 1,
                avg_bin_speed: 5.0,
            },
            AvgSpeedBinRow {
                avg_speed_bin_id: 2,
                avg_bin_speed: 20.0,
            },
            AvgSpeedBinRow {
                avg_speed_bin_id: 3,
                avg_bin_speed: 50.0,
            },
        ];
        let inputs = OmdgInputs {
            drive_schedule: &drive_schedule,
            drive_schedule_assoc: &assoc,
            drive_schedule_second: &[],
            avg_speed_bin: &bins,
            avg_speed_distribution: &[],
            operating_mode: &[],
            op_mode_pol_proc_assoc: &[],
            physics_mapping: &[],
            pol_process_represented: &[],
            run_spec_source_type: &[SOURCE_TYPE],
            run_spec_road_type: &[ROAD_TYPE],
            run_spec_hour_day: &[],
        };
        let brackets = bracket_average_speed_bins(&inputs);

        // Bin 1 (5 mph) is below every cycle: both brackets clamp to cycle 1.
        let below = &brackets[&(SOURCE_TYPE, ROAD_TYPE, 1)];
        assert_eq!((below.lo_speed, below.hi_speed), (10.0, 10.0));
        assert_eq!(
            (&below.lo_schedules[..], &below.hi_schedules[..]),
            (&[1][..], &[1][..])
        );
        // Bin 2 (20 mph) sits between the two cycles.
        let between = &brackets[&(SOURCE_TYPE, ROAD_TYPE, 2)];
        assert_eq!((between.lo_speed, between.hi_speed), (10.0, 30.0));
        assert_eq!(
            (&between.lo_schedules[..], &between.hi_schedules[..]),
            (&[1][..], &[2][..]),
        );
        // Bin 3 (50 mph) is above every cycle: both brackets clamp to cycle 2.
        let above = &brackets[&(SOURCE_TYPE, ROAD_TYPE, 3)];
        assert_eq!((above.lo_speed, above.hi_speed), (30.0, 30.0));
        assert_eq!(
            (&above.lo_schedules[..], &above.hi_schedules[..]),
            (&[2][..], &[2][..])
        );
    }

    #[test]
    fn vehicle_specific_power_matches_road_load_polynomial() {
        // rollingTermA dominates; quadratic, cubic and mass terms are zeroed.
        let physics = PhysicsMappingRow {
            real_source_type_id: SOURCE_TYPE,
            temp_source_type_id: SOURCE_TYPE,
            rolling_term_a: 2.0,
            rotating_term_b: 0.0,
            drag_term_c: 0.0,
            source_mass: 0.0,
            fixed_mass_factor: 4.0,
        };
        // VSP = (2 * 10) / 4 = 5.
        assert_eq!(vehicle_specific_power(10.0, 0.0, &physics), 5.0);
    }

    #[test]
    fn assign_operating_mode_brakes_on_hard_deceleration() {
        // A single second decelerating 2 mph/s bins to braking, before any
        // VSP/speed candidate is considered.
        let bin = OperatingModeRow {
            op_mode_id: 30,
            vsp_lower: None,
            vsp_upper: None,
            speed_lower: None,
            speed_upper: None,
        };
        let op_mode =
            assign_operating_mode(25.0, -2.0, (None, None), 0.0, POL_PROCESS, &[(30, bin)]);
        assert_eq!(op_mode, Some(BRAKING_OP_MODE));
    }

    #[test]
    fn assign_operating_mode_brakes_on_three_soft_decelerations() {
        let op_mode =
            assign_operating_mode(25.0, -1.5, (Some(-1.2), Some(-1.1)), 0.0, POL_PROCESS, &[]);
        assert_eq!(op_mode, Some(BRAKING_OP_MODE));
        // The same second without two preceding soft decelerations is not
        // braking — it falls through to (here, no) VSP candidates.
        assert_eq!(
            assign_operating_mode(25.0, -1.5, (None, None), 0.0, POL_PROCESS, &[]),
            None,
        );
    }

    #[test]
    fn assign_operating_mode_bins_by_vsp_and_speed() {
        let bin = OperatingModeRow {
            op_mode_id: 33,
            vsp_lower: Some(3.0),
            vsp_upper: Some(6.0),
            speed_lower: Some(25.0),
            speed_upper: Some(50.0),
        };
        // VSP 4, speed 30 — inside both ranges.
        assert_eq!(
            assign_operating_mode(30.0, 0.0, (None, None), 4.0, POL_PROCESS, &[(33, bin)]),
            Some(33),
        );
        // VSP 7 is above the upper bound — no candidate matches.
        assert_eq!(
            assign_operating_mode(30.0, 0.0, (None, None), 7.0, POL_PROCESS, &[(33, bin)]),
            None,
        );
    }

    #[test]
    fn assign_operating_mode_idle_override_wins() {
        let bin = OperatingModeRow {
            op_mode_id: 30,
            vsp_lower: None,
            vsp_upper: None,
            speed_lower: None,
            speed_upper: None,
        };
        // speed < 1 overrides the VSP/speed bin the second would otherwise get.
        assert_eq!(
            assign_operating_mode(0.5, 0.0, (None, None), 0.0, POL_PROCESS, &[(30, bin)]),
            Some(IDLE_OP_MODE),
        );
        // speed == 0 bins to mode 501 for pol/process 11609 only.
        assert_eq!(
            assign_operating_mode(
                0.0,
                0.0,
                (None, None),
                0.0,
                ZERO_SPEED_OP_MODE_POL_PROCESS,
                &[],
            ),
            Some(ZERO_SPEED_OP_MODE),
        );
        assert_eq!(
            assign_operating_mode(0.0, 0.0, (None, None), 0.0, POL_PROCESS, &[]),
            Some(IDLE_OP_MODE),
        );
    }

    #[test]
    fn count_schedule_op_modes_bins_hard_deceleration_as_braking() {
        // A cycle decelerating 10, 10 then 5 mph/s — every binned second has
        // an acceleration at or below -2 and so brakes.
        let speeds: BTreeMap<i16, f64> = [(0, 30.0), (1, 20.0), (2, 10.0), (3, 5.0)]
            .into_iter()
            .collect();
        let physics = flat_physics(SOURCE_TYPE, SOURCE_TYPE);
        let pol_processes: BTreeSet<PolProcessId> = [POL_PROCESS].into_iter().collect();
        let candidates: OpModeCandidates = BTreeMap::new();
        let (second_count, mode_counts) =
            count_schedule_op_modes(&speeds, &physics, &pol_processes, &candidates);
        // Seconds 1, 2 and 3 are binned; second 0 has no predecessor.
        assert_eq!(second_count, 3);
        assert_eq!(
            mode_counts.get(&(POL_PROCESS, BRAKING_OP_MODE)).copied(),
            Some(3),
        );
    }

    #[test]
    fn count_schedule_op_modes_counts_seconds_with_no_operating_mode() {
        // Constant 30 mph: no braking, no idle. A candidate whose VSP floor
        // sits above the cycle's VSP leaves every second unbinned — those
        // seconds still count toward the denominator (`secondSum`).
        let speeds: BTreeMap<i16, f64> = [(0, 30.0), (1, 30.0), (2, 30.0)].into_iter().collect();
        let physics = flat_physics(SOURCE_TYPE, SOURCE_TYPE);
        let pol_processes: BTreeSet<PolProcessId> = [POL_PROCESS].into_iter().collect();
        let unreachable_mode = OperatingModeRow {
            op_mode_id: 30,
            vsp_lower: Some(100.0),
            vsp_upper: None,
            speed_lower: None,
            speed_upper: None,
        };
        let candidates: OpModeCandidates = [(POL_PROCESS, vec![(30, unreachable_mode)])]
            .into_iter()
            .collect();
        let (second_count, mode_counts) =
            count_schedule_op_modes(&speeds, &physics, &pol_processes, &candidates);
        assert_eq!(second_count, 2);
        assert!(mode_counts.is_empty());
    }

    /// Build the two-cycle scenario shared by the end-to-end tests: an
    /// all-idle cycle (`ds1`) and a constant-30-mph cycle (`ds2`) bracketing a
    /// single 20-mph bin, each weighted 0.5.
    struct Scenario {
        drive_schedule: Vec<DriveScheduleRow>,
        drive_schedule_assoc: Vec<DriveScheduleAssocRow>,
        drive_schedule_second: Vec<DriveScheduleSecondRow>,
        avg_speed_bin: Vec<AvgSpeedBinRow>,
        avg_speed_distribution: Vec<AvgSpeedDistributionRow>,
        operating_mode: Vec<OperatingModeRow>,
        physics_mapping: Vec<PhysicsMappingRow>,
    }

    impl Scenario {
        fn new() -> Self {
            let mut drive_schedule_second = constant_cycle(1, 0.0);
            drive_schedule_second.extend(constant_cycle(2, 30.0));
            Self {
                drive_schedule: vec![
                    DriveScheduleRow {
                        drive_schedule_id: 1,
                        average_speed: 10.0,
                    },
                    DriveScheduleRow {
                        drive_schedule_id: 2,
                        average_speed: 30.0,
                    },
                ],
                drive_schedule_assoc: vec![
                    DriveScheduleAssocRow {
                        source_type_id: SOURCE_TYPE,
                        road_type_id: ROAD_TYPE,
                        drive_schedule_id: 1,
                        is_ramp: false,
                    },
                    DriveScheduleAssocRow {
                        source_type_id: SOURCE_TYPE,
                        road_type_id: ROAD_TYPE,
                        drive_schedule_id: 2,
                        is_ramp: false,
                    },
                ],
                drive_schedule_second,
                avg_speed_bin: vec![AvgSpeedBinRow {
                    avg_speed_bin_id: 1,
                    avg_bin_speed: 20.0,
                }],
                avg_speed_distribution: vec![AvgSpeedDistributionRow {
                    source_type_id: SOURCE_TYPE,
                    road_type_id: ROAD_TYPE,
                    hour_day_id: HOUR_DAY,
                    avg_speed_bin_id: 1,
                    avg_speed_fraction: 1.0,
                }],
                // VSP is identically zero here, so the cruising cycle bins
                // into mode 30.
                operating_mode: vec![OperatingModeRow {
                    op_mode_id: 30,
                    vsp_lower: Some(-100.0),
                    vsp_upper: Some(100.0),
                    speed_lower: None,
                    speed_upper: None,
                }],
                physics_mapping: vec![flat_physics(SOURCE_TYPE, SOURCE_TYPE)],
            }
        }

        fn inputs<'a>(
            &'a self,
            op_mode_pol_proc_assoc: &'a [OpModePolProcAssocRow],
            pol_process_represented: &'a [PolProcessRepresentedRow],
        ) -> OmdgInputs<'a> {
            OmdgInputs {
                drive_schedule: &self.drive_schedule,
                drive_schedule_assoc: &self.drive_schedule_assoc,
                drive_schedule_second: &self.drive_schedule_second,
                avg_speed_bin: &self.avg_speed_bin,
                avg_speed_distribution: &self.avg_speed_distribution,
                operating_mode: &self.operating_mode,
                op_mode_pol_proc_assoc,
                physics_mapping: &self.physics_mapping,
                pol_process_represented,
                run_spec_source_type: &[SOURCE_TYPE],
                run_spec_road_type: &[ROAD_TYPE],
                run_spec_hour_day: &[HOUR_DAY],
            }
        }
    }

    #[test]
    fn drive_schedule_fractions_split_bin_evenly() {
        let scenario = Scenario::new();
        let inputs = scenario.inputs(&[], &[]);
        let brackets = bracket_average_speed_bins(&inputs);
        let fractions = drive_schedule_fractions(&inputs, &brackets);
        // The 20-mph bin sits halfway between the 10- and 30-mph cycles, so
        // each cycle takes half of the (unit) bin fraction.
        assert_eq!(
            fractions
                .get(&(SOURCE_TYPE, ROAD_TYPE, HOUR_DAY, 1))
                .copied(),
            Some(0.5),
        );
        assert_eq!(
            fractions
                .get(&(SOURCE_TYPE, ROAD_TYPE, HOUR_DAY, 2))
                .copied(),
            Some(0.5),
        );
    }

    #[test]
    fn op_mode_distribution_weights_schedules_by_drive_fraction() {
        let scenario = Scenario::new();
        let assoc = [
            OpModePolProcAssocRow {
                pol_process_id: POL_PROCESS,
                op_mode_id: IDLE_OP_MODE,
            },
            OpModePolProcAssocRow {
                pol_process_id: POL_PROCESS,
                op_mode_id: 30,
            },
        ];
        let rows = op_mode_distribution(&scenario.inputs(&assoc, &[]));

        // The idle cycle (fraction 0.5) is all idle; the cruising cycle
        // (fraction 0.5) is all mode 30.
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0],
            OpModeFractionRow {
                source_type_id: SOURCE_TYPE,
                road_type_id: ROAD_TYPE,
                hour_day_id: HOUR_DAY,
                pol_process_id: POL_PROCESS,
                op_mode_id: IDLE_OP_MODE,
                op_mode_fraction: 0.5,
            },
        );
        assert_eq!(
            rows[1],
            OpModeFractionRow {
                source_type_id: SOURCE_TYPE,
                road_type_id: ROAD_TYPE,
                hour_day_id: HOUR_DAY,
                pol_process_id: POL_PROCESS,
                op_mode_id: 30,
                op_mode_fraction: 0.5,
            },
        );
        // The operating-mode fractions for the pol/process sum to one.
        let total: f64 = rows.iter().map(|r| r.op_mode_fraction).sum();
        assert!((total - 1.0).abs() < 1e-12);
    }

    #[test]
    fn op_mode_distribution_copies_represented_pol_processes() {
        let scenario = Scenario::new();
        let represented_id = PolProcessId(201);
        let assoc = [
            OpModePolProcAssocRow {
                pol_process_id: POL_PROCESS,
                op_mode_id: IDLE_OP_MODE,
            },
            OpModePolProcAssocRow {
                pol_process_id: POL_PROCESS,
                op_mode_id: 30,
            },
        ];
        let represented = [PolProcessRepresentedRow {
            pol_process_id: represented_id,
            representing_pol_process_id: POL_PROCESS,
        }];
        let rows = op_mode_distribution(&scenario.inputs(&assoc, &represented));

        // Two rows for the representing pol/process, two copied onto 201.
        assert_eq!(rows.len(), 4);
        let copied: Vec<&OpModeFractionRow> = rows
            .iter()
            .filter(|r| r.pol_process_id == represented_id)
            .collect();
        assert_eq!(copied.len(), 2);
        for row in copied {
            assert_eq!(row.op_mode_fraction, 0.5);
        }
    }

    #[test]
    fn op_mode_distribution_drops_excluded_pol_process() {
        let scenario = Scenario::new();
        // pol/process 11710 is associated with the same modes but must never
        // reach OpModeFraction2.
        let assoc = [
            OpModePolProcAssocRow {
                pol_process_id: POL_PROCESS,
                op_mode_id: IDLE_OP_MODE,
            },
            OpModePolProcAssocRow {
                pol_process_id: POL_PROCESS,
                op_mode_id: 30,
            },
            OpModePolProcAssocRow {
                pol_process_id: EXCLUDED_POL_PROCESS,
                op_mode_id: IDLE_OP_MODE,
            },
            OpModePolProcAssocRow {
                pol_process_id: EXCLUDED_POL_PROCESS,
                op_mode_id: 30,
            },
        ];
        let rows = op_mode_distribution(&scenario.inputs(&assoc, &[]));
        assert!(rows.iter().all(|r| r.pol_process_id == POL_PROCESS));
    }

    #[test]
    fn op_mode_distribution_empty_without_inputs() {
        let inputs = OmdgInputs {
            drive_schedule: &[],
            drive_schedule_assoc: &[],
            drive_schedule_second: &[],
            avg_speed_bin: &[],
            avg_speed_distribution: &[],
            operating_mode: &[],
            op_mode_pol_proc_assoc: &[],
            physics_mapping: &[],
            pol_process_represented: &[],
            run_spec_source_type: &[],
            run_spec_road_type: &[],
            run_spec_hour_day: &[],
        };
        assert!(op_mode_distribution(&inputs).is_empty());
    }

    #[test]
    fn zero_source_mass_drops_the_source_type() {
        let mut scenario = Scenario::new();
        // A zero-mass physics mapping yields no VSP rows — the Java VSP
        // `SELECT` joins `sourceMass <> 0`.
        scenario.physics_mapping = vec![PhysicsMappingRow {
            source_mass: 0.0,
            ..flat_physics(SOURCE_TYPE, SOURCE_TYPE)
        }];
        let assoc = [OpModePolProcAssocRow {
            pol_process_id: POL_PROCESS,
            op_mode_id: 30,
        }];
        assert!(op_mode_distribution(&scenario.inputs(&assoc, &[])).is_empty());
    }
}

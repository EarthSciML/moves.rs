//! Port of `MesoscaleLookupOperatingModeDistributionGenerator.java`
//! (`gov.epa.otaq.moves.master.implementation.ghg`) — the generator that
//! builds `OperatingModeDistribution` records for Mesoscale-Lookup runs.
//!
//! Migration plan: Phase 3, Task 35 (paired with [`super::total_activity`]).
//!
//! # What this generator produces
//!
//! For a run using the Mesoscale-Lookup output domain, every link's
//! `avgSpeedBinID` is encoded in its `linkID` (`avgSpeedBinID = linkID %
//! 100`). The generator computes — once per run — a
//! `(sourceType, roadType, avgSpeedBin, hourDay, polProcess, opMode)`
//! operating-mode-fraction table (`OpModeFraction2` in the Java) and, in
//! the per-link `executeLoop`, projects it onto the `OpModeDistribution`
//! table keyed by `linkID`.
//!
//! The Java class subscribes to two emission processes at `YEAR`
//! granularity / `GENERATOR` priority — Running Exhaust (process id 1) and
//! Brakewear (process id 9) — exactly as `subscribeToMe` does.
//!
//! # The algorithm — OMDG steps 1–7
//!
//! The Java runs a fixed seven-step pipeline (`OMDG-1` … `OMDG-7`) the
//! first time `executeLoop` is entered, then a per-link projection step.
//! Almost every step is a chain of `CREATE TABLE` / `INSERT … SELECT`
//! statements; the *numerically meaningful* core — what this module ports
//! as tested free functions — is:
//!
//! * **Drive-schedule bracketing** (`OMDG-1`/`-2`) — each average-speed
//!   bin lies between the average speeds of two drive schedules; the bin's
//!   travel is split between them by linear interpolation.
//!   [`bracket_speed_bin`] + [`lo_schedule_fraction`].
//! * **Vehicle-specific power** (`OMDG-4`) — the second-by-second VSP
//!   physics formula. [`vehicle_specific_power`].
//! * **Operating-mode classification** (`OMDG-5`) — each second of a drive
//!   schedule is binned into an operating mode from its speed,
//!   acceleration and VSP. [`classify_op_mode`].
//! * **Per-schedule op-mode fractions** (`OMDG-6`) — the fraction of a
//!   drive schedule's seconds spent in each operating mode.
//! * **Weighted op-mode fractions** (`OMDG-7` preliminary) — each bin's
//!   op-mode fraction is the schedule fractions weighted by the
//!   drive-schedule split.
//!
//! [`operating_mode_distribution`] composes these into the full
//! `OpModeFraction2`-shaped result.
//!
//! # Data plane (Task 50)
//!
//! The Java reads ~20 MariaDB tables and writes `OpModeDistribution`.
//! [`Generator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are Phase-2 placeholders until
//! the `DataFrameStore` lands (migration-plan Task 50), so `execute`
//! cannot yet read those tables nor write `OpModeDistribution`. The
//! numerically faithful algorithm is fully ported and unit-tested in the
//! free functions below; once the data plane exists, `execute` projects an
//! [`OpModeDistributionInputs`] out of `ctx.tables()`, runs
//! [`operating_mode_distribution`], and writes each row onto the links
//! whose `linkID % 100` matches its `avgSpeedBinID`.
//!
//! The Java per-link step additionally copies a *representing*
//! pollutant/process's rows onto the pollutant/processes it represents
//! (`OMDGPolProcessRepresented`, a scratch table populated by
//! `SourceTypePhysics`). That substitution rewrites only the
//! `polProcessID` key, never the computed fractions, so it is likewise
//! part of the Task 50 data-plane projection rather than this compute
//! core: [`operating_mode_distribution`] produces faithful rows for every
//! pollutant/process that carries its own `opModePolProcAssoc` op modes.
//!
//! # Fidelity notes
//!
//! * MOVES stores `VSP`, `speed` and `acceleration` in `FLOAT` (32-bit)
//!   columns while evaluating the arithmetic in `DOUBLE`. This port
//!   computes in `f64` throughout, matching the Task 41 / Task 33
//!   precedent; the bug-compatibility decision is deferred to Task 44
//!   (generator integration validation).
//! * `sourceUseTypePhysicsMapping` can remap a "real" source type onto a
//!   "temp" source type for the physics terms. That remap is a data-plane
//!   join owned by the `SourceTypePhysics` port (Task 37); this module
//!   takes the physics terms already keyed by the source type they apply
//!   to.

use std::collections::{BTreeMap, BTreeSet};

use moves_calculator_info::{Granularity, Priority};
use moves_data::{PolProcessId, ProcessId, RoadTypeId, SourceTypeId};
use moves_framework::{
    CalculatorContext, CalculatorOutput, CalculatorSubscription, Error, Generator,
};

/// Conversion factor from miles per hour to metres per second — the Java
/// `* 0.44704` literal applied when building `DriveScheduleSecond2`.
pub const MPH_TO_MPS: f64 = 0.447_04;

/// Running Exhaust — process id 1. First `subscribeToMe` subscription.
const RUNNING_EXHAUST: ProcessId = ProcessId(1);
/// Brakewear — process id 9. Second `subscribeToMe` subscription.
const BRAKEWEAR: ProcessId = ProcessId(9);

/// Braking operating mode — assigned to decelerating seconds (`OMDG-5`).
const BRAKING_OP_MODE: i16 = 0;
/// Idle operating mode — assigned to any second below [`IDLE_SPEED_MPH`].
const IDLE_OP_MODE: i16 = 1;
/// Op mode 501 — assigned when a stopped second carries
/// `BRAKE_PARTICULATE_POLPROCESS` (the Java `speed=0 and
/// polProcessID=11609` special case).
const STOPPED_BRAKE_OP_MODE: i16 = 501;

/// Speed (mph) below which a second is idle regardless of its VSP bin —
/// the Java `IF(speed<1.0,1,opModeID)` override.
pub const IDLE_SPEED_MPH: f64 = 1.0;

/// `polProcessID` 11609 — Brake Wear Particulate / Brakewear. A stopped
/// second (`speed = 0`) carrying this pollutant/process is op mode 501.
const BRAKE_PARTICULATE_POLPROCESS: PolProcessId = PolProcessId(11609);
/// `polProcessID` 11710 — Tire Wear Particulate / Tirewear. The Java
/// `OpModeFraction2b` step excludes it (`omppa.polProcessID not in
/// (11710)`); its op-mode distribution comes from elsewhere.
const EXCLUDED_POLPROCESS: PolProcessId = PolProcessId(11710);

/// First/lower acceleration bound for braking detection. A single second
/// at or below this (mph/s) is braking (`OMDG-5`,
/// `acceleration <= -2`).
const HARD_BRAKING_ACCEL: f64 = -2.0;
/// Sustained-deceleration bound: three consecutive seconds each strictly
/// below this (mph/s) are all braking (`OMDG-5`, the three-second window).
const SUSTAINED_BRAKING_ACCEL: f64 = -1.0;

/// One `DriveScheduleAssoc` row — which drive schedules apply to a
/// `(sourceType, roadType)` pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DriveScheduleAssoc {
    /// `sourceTypeID`.
    pub source_type_id: SourceTypeId,
    /// `roadTypeID`.
    pub road_type_id: RoadTypeId,
    /// `driveScheduleID`.
    pub drive_schedule_id: i16,
}

/// One `DriveSchedule` row — a named second-by-second speed trace and its
/// average speed (mph).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DriveSchedule {
    /// `driveScheduleID`.
    pub drive_schedule_id: i16,
    /// `averageSpeed` (mph).
    pub average_speed: f64,
}

/// One `AvgSpeedBin` row — an average-speed bin and its representative
/// speed (mph).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AvgSpeedBin {
    /// `avgSpeedBinID`.
    pub avg_speed_bin_id: i16,
    /// `avgBinSpeed` (mph).
    pub avg_bin_speed: f64,
}

/// One `DriveScheduleSecond` row — the instantaneous speed (mph) at one
/// second of a drive schedule.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DriveScheduleSecond {
    /// `driveScheduleID`.
    pub drive_schedule_id: i16,
    /// `second` — the time index within the schedule.
    pub second: i16,
    /// `speed` (mph) at this second.
    pub speed: f64,
}

/// Source-type physics terms — the `sourceUseTypePhysicsMapping` columns
/// the VSP formula reads, already keyed by the source type they apply to.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypePhysics {
    /// `sourceTypeID` these terms apply to.
    pub source_type_id: SourceTypeId,
    /// `rollingTermA` — the rolling-resistance term.
    pub rolling_term_a: f64,
    /// `rotatingTermB` — the speed² (rotating-resistance) term.
    pub rotating_term_b: f64,
    /// `dragTermC` — the speed³ (aerodynamic-drag) term.
    pub drag_term_c: f64,
    /// `sourceMass` — vehicle mass. A zero mass drops the source type
    /// from the VSP table (`sourceMass <> 0`).
    pub source_mass: f64,
    /// `fixedMassFactor` — the VSP normalisation denominator.
    pub fixed_mass_factor: f64,
}

/// One `OperatingMode` bin — the speed / VSP bounds that define a
/// running operating mode (`opModeID` strictly between 1 and 100). Each
/// bound is optional: a `NULL` column in MOVES means "unbounded on that
/// side".
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OperatingModeBin {
    /// `opModeID`.
    pub op_mode_id: i16,
    /// `VSPLower` — inclusive lower VSP bound, if any.
    pub vsp_lower: Option<f64>,
    /// `VSPUpper` — exclusive upper VSP bound, if any.
    pub vsp_upper: Option<f64>,
    /// `speedLower` — inclusive lower speed (mph) bound, if any.
    pub speed_lower: Option<f64>,
    /// `speedUpper` — exclusive upper speed (mph) bound, if any.
    pub speed_upper: Option<f64>,
}

impl OperatingModeBin {
    /// Whether a second with the given VSP and speed (mph) falls inside
    /// this bin. Mirrors the Java `whereClause`: `VSP >= VSPLower`,
    /// `VSP < VSPUpper`, `speed >= speedLower`, `speed < speedUpper`,
    /// with absent bounds contributing no constraint.
    #[must_use]
    pub fn contains(&self, vsp: f64, speed_mph: f64) -> bool {
        self.vsp_lower.map_or(true, |lo| vsp >= lo)
            && self.vsp_upper.map_or(true, |hi| vsp < hi)
            && self.speed_lower.map_or(true, |lo| speed_mph >= lo)
            && self.speed_upper.map_or(true, |hi| speed_mph < hi)
    }
}

/// One `OpModePolProcAssoc` row — an operating mode associated with a
/// pollutant/process. The set of distinct `polProcessID`s here is the set
/// of processes the generator computes op-mode fractions for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpModePolProcAssoc {
    /// `polProcessID`.
    pub pol_process_id: PolProcessId,
    /// `opModeID`.
    pub op_mode_id: i16,
}

/// The projected default-database tables [`operating_mode_distribution`]
/// reads. Each field is the Rust analogue of one MariaDB table the Java
/// `SELECT` statements reference; the run-spec filtering the Java does via
/// `RunSpecSourceType` / `RunSpecRoadType` joins is applied upstream when
/// the data plane (Task 50) projects this view from `ctx.tables()`.
#[derive(Debug, Clone, Copy)]
pub struct OpModeDistributionInputs<'a> {
    /// `driveScheduleAssoc` — drive schedules per `(sourceType, roadType)`.
    pub drive_schedule_assoc: &'a [DriveScheduleAssoc],
    /// `driveSchedule` — average speed per drive schedule.
    pub drive_schedules: &'a [DriveSchedule],
    /// `avgSpeedBin` — the average-speed bins.
    pub avg_speed_bins: &'a [AvgSpeedBin],
    /// `driveScheduleSecond` — second-by-second speed traces.
    pub drive_schedule_seconds: &'a [DriveScheduleSecond],
    /// `sourceUseTypePhysicsMapping` — VSP physics terms per source type.
    pub source_type_physics: &'a [SourceTypePhysics],
    /// `operatingMode` — the running op-mode speed / VSP bins.
    pub operating_modes: &'a [OperatingModeBin],
    /// `opModePolProcAssoc` — operating modes per pollutant/process.
    pub op_mode_pol_proc_assoc: &'a [OpModePolProcAssoc],
    /// `runSpecHourDay.hourDayID` — the hour/day combinations the run
    /// selects. Every op-mode fraction is replicated across this set
    /// (`OpModeFraction2a` crosses `OpModeFraction2b` with `RunSpecHourDay`).
    pub run_spec_hour_day: &'a [i16],
}

/// One `OpModeFraction2` row — the operating-mode fraction for a
/// `(sourceType, roadType, avgSpeedBin, hourDay, opMode, polProcess)`
/// combination. This is the per-run table the Java per-link step projects
/// onto `OpModeDistribution`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OpModeDistributionRow {
    /// `sourceTypeID`.
    pub source_type_id: SourceTypeId,
    /// `roadTypeID`.
    pub road_type_id: RoadTypeId,
    /// `avgSpeedBinID`.
    pub avg_speed_bin_id: i16,
    /// `hourDayID`.
    pub hour_day_id: i16,
    /// `opModeID`.
    pub op_mode_id: i16,
    /// `polProcessID`.
    pub pol_process_id: PolProcessId,
    /// `opModeFraction` — the fraction of activity in this operating mode.
    pub op_mode_fraction: f64,
}

/// Primary-key projection of an [`OpModeDistributionRow`] — the columns
/// `OpModeFraction2`'s unique index covers, used to order the output
/// deterministically.
type RowKey = (SourceTypeId, RoadTypeId, i16, i16, i16, PolProcessId);

impl OpModeDistributionRow {
    fn key(&self) -> RowKey {
        (
            self.source_type_id,
            self.road_type_id,
            self.avg_speed_bin_id,
            self.hour_day_id,
            self.op_mode_id,
            self.pol_process_id,
        )
    }
}

/// Compute the vehicle-specific power (VSP) for one second — the Java
/// `OMDG-4` formula.
///
/// `VSP = (rollingTermA·v + rotatingTermB·v² + dragTermC·v³ +
/// sourceMass·v·a) / fixedMassFactor`, with the speed `v` in metres per
/// second and the acceleration `a` in metres per second².
///
/// Returns `None` when `sourceMass = 0` — the Java VSP `INSERT` filters
/// `sut.sourceMass <> 0`, so a zero-mass source type contributes no VSP
/// row and its seconds fall through op-mode classification unbinned.
#[must_use]
pub fn vehicle_specific_power(
    physics: &SourceTypePhysics,
    speed_mps: f64,
    accel_mps2: f64,
) -> Option<f64> {
    if physics.source_mass == 0.0 {
        return None;
    }
    let v = speed_mps;
    Some(
        (physics.rolling_term_a * v
            + physics.rotating_term_b * v * v
            + physics.drag_term_c * v * v * v
            + physics.source_mass * v * accel_mps2)
            / physics.fixed_mass_factor,
    )
}

/// Whether the second at `index` of an acceleration trace is braking —
/// the Java `OMDG-5` three-second window plus the hard-deceleration case.
///
/// `accels` holds the per-second accelerations (mph/s) of the classified
/// seconds in time order. A second is braking when its own acceleration
/// is at or below `HARD_BRAKING_ACCEL`, **or** it and the two seconds
/// before it are each strictly below `SUSTAINED_BRAKING_ACCEL`.
#[must_use]
pub fn is_braking_second(accels: &[f64], index: usize) -> bool {
    let a = accels[index];
    if a <= HARD_BRAKING_ACCEL {
        return true;
    }
    index >= 2
        && a < SUSTAINED_BRAKING_ACCEL
        && accels[index - 1] < SUSTAINED_BRAKING_ACCEL
        && accels[index - 2] < SUSTAINED_BRAKING_ACCEL
}

/// Classify one second into an operating mode — the Java `OMDG-5`
/// precedence, evaluated last-wins exactly as the chained `UPDATE`
/// statements do:
///
/// 1. a braking second is op mode `BRAKING_OP_MODE`;
/// 2. otherwise the first [`OperatingModeBin`] (smallest `opModeID`)
///    whose speed / VSP bounds contain the second;
/// 3. the `IF(speed=0 and polProcessID=11609, 501, …)` /
///    `IF(speed<1.0, 1, …)` overrides win over steps 1–2.
///
/// Returns `None` for a second that matches no bin, is not braking and is
/// not overridden — MOVES leaves its `opModeID` `NULL`, and the later
/// `OpModeFraction2b` join drops `NULL` op modes. Such a second still
/// counts toward the schedule's denominator (see
/// [`op_mode_fractions_for_schedule`]).
#[must_use]
pub fn classify_op_mode(
    speed_mph: f64,
    vsp: Option<f64>,
    is_braking: bool,
    pol_process_id: PolProcessId,
    operating_modes: &[OperatingModeBin],
) -> Option<i16> {
    // Override: a stopped second carrying brake-particulate is op mode 501.
    if speed_mph == 0.0 && pol_process_id == BRAKE_PARTICULATE_POLPROCESS {
        return Some(STOPPED_BRAKE_OP_MODE);
    }
    // Override: any sub-1-mph second is idle.
    if speed_mph < IDLE_SPEED_MPH {
        return Some(IDLE_OP_MODE);
    }
    if is_braking {
        return Some(BRAKING_OP_MODE);
    }
    // VSP / speed bin lookup. The MOVES bins partition the plane, so at
    // most one matches; `min` keeps the choice deterministic if a
    // malformed input table overlaps two.
    let vsp = vsp?;
    operating_modes
        .iter()
        .filter(|bin| bin.contains(vsp, speed_mph))
        .map(|bin| bin.op_mode_id)
        .min()
}

/// Find the drive-schedule average speeds (mph) that bracket a bin speed
/// — the Java `OMDG-1` `BracketScheduleLo` / `BracketScheduleHi` step.
///
/// `schedule_speeds` is the set of average speeds of the drive schedules
/// associated with one `(sourceType, roadType)` pair. The bracketing
/// speeds are:
///
/// * **lo** — the greatest schedule speed `<=` the bin speed; or, for a
///   bin below every schedule, the lowest schedule speed (`scheduleBoundLo`);
/// * **hi** — the least schedule speed `>` the bin speed; or, for a bin
///   above every schedule, the highest schedule speed (`scheduleBoundHi`).
///
/// Returns `None` only when `schedule_speeds` is empty.
#[must_use]
pub fn bracket_speed_bin(bin_speed: f64, schedule_speeds: &[f64]) -> Option<(f64, f64)> {
    let min = schedule_speeds
        .iter()
        .copied()
        .fold(f64::INFINITY, f64::min);
    let max = schedule_speeds
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);
    if !min.is_finite() || !max.is_finite() {
        return None;
    }
    let lo = schedule_speeds
        .iter()
        .copied()
        .filter(|&s| s <= bin_speed)
        .fold(f64::NEG_INFINITY, f64::max);
    let lo = if lo.is_finite() { lo } else { min };
    let hi = schedule_speeds
        .iter()
        .copied()
        .filter(|&s| s > bin_speed)
        .fold(f64::INFINITY, f64::min);
    let hi = if hi.is_finite() { hi } else { max };
    Some((lo, hi))
}

/// The fraction of a bin's travel assigned to its **lower** bracketing
/// drive schedule — the Java `OMDG-2` `LoScheduleFraction` formula.
///
/// `loFraction = (hiSpeed - binSpeed) / (hiSpeed - loSpeed)` by linear
/// interpolation; when the two bracketing speeds are equal (a bin clamped
/// to a single schedule, or the schedule set has one speed) the whole bin
/// is assigned to the lower schedule, `loFraction = 1`. The upper
/// schedule receives `1 - loFraction`.
#[must_use]
pub fn lo_schedule_fraction(bin_speed: f64, lo_speed: f64, hi_speed: f64) -> f64 {
    if hi_speed == lo_speed {
        1.0
    } else {
        (hi_speed - bin_speed) / (hi_speed - lo_speed)
    }
}

/// The per-drive-schedule travel weights for one `(sourceType, roadType,
/// avgSpeedBin)` — the Java `OMDG-2`/`-3` `DriveScheduleFraction` table.
///
/// Each drive schedule whose average speed equals the bin's lower
/// bracketing speed gets `loFraction`; each at the upper speed gets
/// `hiFraction`; a schedule at both (degenerate, equal bracket) gets their
/// sum. The result maps `driveScheduleID -> fraction`.
fn drive_schedule_fractions(bin_speed: f64, schedules: &[(i16, f64)]) -> BTreeMap<i16, f64> {
    let speeds: Vec<f64> = schedules.iter().map(|&(_, s)| s).collect();
    let mut weights: BTreeMap<i16, f64> = BTreeMap::new();
    let Some((lo_speed, hi_speed)) = bracket_speed_bin(bin_speed, &speeds) else {
        return weights;
    };
    let lo_fraction = lo_schedule_fraction(bin_speed, lo_speed, hi_speed);
    let hi_fraction = 1.0 - lo_fraction;
    for &(schedule_id, speed) in schedules {
        if speed == lo_speed {
            *weights.entry(schedule_id).or_insert(0.0) += lo_fraction;
        }
        if speed == hi_speed && hi_speed != lo_speed {
            *weights.entry(schedule_id).or_insert(0.0) += hi_fraction;
        }
    }
    weights
}

/// The fraction of one drive schedule's seconds spent in each operating
/// mode, for one pollutant/process — the Java `OMDG-5`/`-6`
/// `OpModeFractionBySchedule` step.
///
/// The schedule's seconds are sorted by time; the first second is dropped
/// (MOVES's `DriveScheduleSecond3` join requires a preceding second, so
/// acceleration — and therefore classification — is undefined for it).
/// Each remaining second is classified via [`classify_op_mode`]; the
/// returned map is `opModeID -> count / classifiedSeconds`. Seconds that
/// classify to `None` are counted in the denominator but contribute no
/// entry, exactly as a `NULL` `opModeID` does in the Java.
#[must_use]
pub fn op_mode_fractions_for_schedule(
    seconds: &[DriveScheduleSecond],
    physics: &SourceTypePhysics,
    pol_process_id: PolProcessId,
    operating_modes: &[OperatingModeBin],
) -> BTreeMap<i16, f64> {
    let mut sorted: Vec<DriveScheduleSecond> = seconds.to_vec();
    sorted.sort_by_key(|s| s.second);
    let mut fractions: BTreeMap<i16, f64> = BTreeMap::new();
    if sorted.len() < 2 {
        return fractions;
    }
    // Per-second acceleration (mph/s); index 0 is the first *classified*
    // second, i.e. the schedule's second second.
    let accels: Vec<f64> = sorted.windows(2).map(|w| w[1].speed - w[0].speed).collect();
    let total = accels.len() as f64;
    let mut counts: BTreeMap<i16, usize> = BTreeMap::new();
    for (index, accel) in accels.iter().copied().enumerate() {
        let speed_mph = sorted[index + 1].speed;
        let vsp = vehicle_specific_power(physics, speed_mph * MPH_TO_MPS, accel * MPH_TO_MPS);
        let braking = is_braking_second(&accels, index);
        if let Some(op_mode) =
            classify_op_mode(speed_mph, vsp, braking, pol_process_id, operating_modes)
        {
            *counts.entry(op_mode).or_insert(0) += 1;
        }
    }
    for (op_mode, count) in counts {
        fractions.insert(op_mode, count as f64 / total);
    }
    fractions
}

/// Run the full `MesoscaleLookupOperatingModeDistributionGenerator`
/// pipeline — Java steps `OMDG-1` … `OMDG-7` (preliminary).
///
/// Produces the `OpModeFraction2`-shaped table: one row per
/// `(sourceType, roadType, avgSpeedBin, hourDay, opMode, polProcess)`.
/// The per-link projection onto `OpModeDistribution` (`avgSpeedBinID =
/// linkID % 100`) is the data-plane step done by [`Generator::execute`]
/// once Task 50 lands.
///
/// Output is sorted by primary key for deterministic comparison.
#[must_use]
pub fn operating_mode_distribution(
    inputs: &OpModeDistributionInputs<'_>,
) -> Vec<OpModeDistributionRow> {
    // Average speed per drive schedule.
    let schedule_speed: BTreeMap<i16, f64> = inputs
        .drive_schedules
        .iter()
        .map(|d| (d.drive_schedule_id, d.average_speed))
        .collect();
    // Physics terms per source type.
    let physics: BTreeMap<SourceTypeId, SourceTypePhysics> = inputs
        .source_type_physics
        .iter()
        .map(|p| (p.source_type_id, *p))
        .collect();
    // Seconds per drive schedule.
    let mut schedule_seconds: BTreeMap<i16, Vec<DriveScheduleSecond>> = BTreeMap::new();
    for second in inputs.drive_schedule_seconds {
        schedule_seconds
            .entry(second.drive_schedule_id)
            .or_default()
            .push(*second);
    }
    // Op modes per pollutant/process, and the distinct process set.
    let mut op_modes_for_polproc: BTreeMap<PolProcessId, BTreeSet<i16>> = BTreeMap::new();
    for assoc in inputs.op_mode_pol_proc_assoc {
        op_modes_for_polproc
            .entry(assoc.pol_process_id)
            .or_default()
            .insert(assoc.op_mode_id);
    }

    // OMDG-2/-3: per (sourceType, roadType, avgSpeedBin) drive-schedule
    // travel weights.
    let mut schedule_fractions: BTreeMap<(SourceTypeId, RoadTypeId, i16), BTreeMap<i16, f64>> =
        BTreeMap::new();
    let mut source_road_schedules: BTreeMap<(SourceTypeId, RoadTypeId), Vec<(i16, f64)>> =
        BTreeMap::new();
    for assoc in inputs.drive_schedule_assoc {
        if let Some(&speed) = schedule_speed.get(&assoc.drive_schedule_id) {
            source_road_schedules
                .entry((assoc.source_type_id, assoc.road_type_id))
                .or_default()
                .push((assoc.drive_schedule_id, speed));
        }
    }
    for (&(source_type, road_type), schedules) in &source_road_schedules {
        for bin in inputs.avg_speed_bins {
            let weights = drive_schedule_fractions(bin.avg_bin_speed, schedules);
            if !weights.is_empty() {
                schedule_fractions.insert((source_type, road_type, bin.avg_speed_bin_id), weights);
            }
        }
    }

    // OMDG-5/-6: per (sourceType, driveSchedule, polProcess) op-mode
    // fractions, computed only for the schedules that carry travel.
    let mut needed: BTreeSet<(SourceTypeId, i16)> = BTreeSet::new();
    for ((source_type, _, _), weights) in &schedule_fractions {
        for (&schedule_id, &weight) in weights {
            if weight != 0.0 {
                needed.insert((*source_type, schedule_id));
            }
        }
    }
    // (sourceType, driveSchedule, polProcess) -> opMode -> modeFraction.
    let mut mode_fractions: BTreeMap<(SourceTypeId, i16, PolProcessId), BTreeMap<i16, f64>> =
        BTreeMap::new();
    for &(source_type, schedule_id) in &needed {
        let Some(phys) = physics.get(&source_type) else {
            continue;
        };
        let Some(seconds) = schedule_seconds.get(&schedule_id) else {
            continue;
        };
        for &pol_process_id in op_modes_for_polproc.keys() {
            let fractions = op_mode_fractions_for_schedule(
                seconds,
                phys,
                pol_process_id,
                inputs.operating_modes,
            );
            if !fractions.is_empty() {
                mode_fractions.insert((source_type, schedule_id, pol_process_id), fractions);
            }
        }
    }

    // OMDG-7 (preliminary): weight the per-schedule fractions by the
    // drive-schedule split, then cross with the run's hour/day set.
    let mut rows: Vec<OpModeDistributionRow> = Vec::new();
    for (&(source_type, road_type, bin), weights) in &schedule_fractions {
        for (&pol_process_id, valid_op_modes) in &op_modes_for_polproc {
            if pol_process_id == EXCLUDED_POLPROCESS {
                continue;
            }
            // opMode -> weighted fraction summed over drive schedules.
            let mut weighted: BTreeMap<i16, f64> = BTreeMap::new();
            for (&schedule_id, &schedule_weight) in weights {
                let Some(fractions) =
                    mode_fractions.get(&(source_type, schedule_id, pol_process_id))
                else {
                    continue;
                };
                for (&op_mode, &mode_fraction) in fractions {
                    if valid_op_modes.contains(&op_mode) {
                        *weighted.entry(op_mode).or_insert(0.0) += mode_fraction * schedule_weight;
                    }
                }
            }
            for (op_mode, fraction) in weighted {
                for &hour_day_id in inputs.run_spec_hour_day {
                    rows.push(OpModeDistributionRow {
                        source_type_id: source_type,
                        road_type_id: road_type,
                        avg_speed_bin_id: bin,
                        hour_day_id,
                        op_mode_id: op_mode,
                        pol_process_id,
                        op_mode_fraction: fraction,
                    });
                }
            }
        }
    }
    rows.sort_by_key(OpModeDistributionRow::key);
    rows
}

/// Default-database tables the generator reads. Names are the canonical
/// MOVES table names; the registry maps them onto Parquet snapshots.
static INPUT_TABLES: &[&str] = &[
    "driveScheduleAssoc",
    "driveSchedule",
    "avgSpeedBin",
    "driveScheduleSecond",
    "sourceUseTypePhysicsMapping",
    "operatingMode",
    "opModePolProcAssoc",
    "runSpecHourDay",
];

/// Scratch-namespace table this generator writes.
static OUTPUT_TABLES: &[&str] = &["OpModeDistribution"];

/// Upstream module: `SourceTypePhysics` supplies the model-year physics
/// terms (`modelYearPhysics.setup`) the VSP step needs, and applies the
/// running-exhaust op-mode correction in `updateOperatingModeDistribution`.
static UPSTREAM: &[&str] = &["SourceTypePhysics"];

/// `OperatingModeDistribution` generator for Mesoscale-Lookup runs.
///
/// Ports `MesoscaleLookupOperatingModeDistributionGenerator.java`; see the
/// module documentation for the algorithm and the scope of the port.
#[derive(Debug, Clone)]
pub struct MesoscaleLookupOperatingModeDistributionGenerator {
    /// The two master-loop subscriptions, built once in [`Self::new`].
    subscriptions: [CalculatorSubscription; 2],
}

impl MesoscaleLookupOperatingModeDistributionGenerator {
    /// Chain-DAG name — matches the Java class name.
    pub const NAME: &'static str = "MesoscaleLookupOperatingModeDistributionGenerator";

    /// Construct the generator with its master-loop subscriptions.
    ///
    /// Mirrors `subscribeToMe`: Running Exhaust and Brakewear, both at
    /// `YEAR` granularity (year level for source bins from the
    /// SourceBinDistributionGenerator), `GENERATOR` priority.
    #[must_use]
    pub fn new() -> Self {
        let priority =
            Priority::parse("GENERATOR").expect("\"GENERATOR\" is a valid MasterLoop priority");
        let at_year = |process| CalculatorSubscription::new(process, Granularity::Year, priority);
        Self {
            subscriptions: [at_year(RUNNING_EXHAUST), at_year(BRAKEWEAR)],
        }
    }
}

impl Default for MesoscaleLookupOperatingModeDistributionGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl Generator for MesoscaleLookupOperatingModeDistributionGenerator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        &self.subscriptions
    }

    fn upstream(&self) -> &[&'static str] {
        UPSTREAM
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn output_tables(&self) -> &[&'static str] {
        OUTPUT_TABLES
    }

    /// Run the generator for the current master-loop iteration.
    ///
    /// **Data plane pending (Task 50).** [`CalculatorContext`] exposes
    /// only placeholder `ExecutionTables` / `ScratchNamespace` today, so
    /// this body cannot read the [`input_tables`](Generator::input_tables)
    /// nor write `OpModeDistribution`. The numerically faithful algorithm
    /// is fully ported and tested in [`operating_mode_distribution`];
    /// once the `DataFrameStore` lands, `execute` projects an
    /// [`OpModeDistributionInputs`] from `ctx.tables()`, runs the
    /// pipeline, and writes each row onto the links whose `linkID % 100`
    /// equals its `avgSpeedBinID`.
    fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        Ok(CalculatorOutput::empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A physics row with unit-ish terms — keeps VSP arithmetic easy to
    /// check by hand.
    fn physics(source_type: u16) -> SourceTypePhysics {
        SourceTypePhysics {
            source_type_id: SourceTypeId(source_type),
            rolling_term_a: 1.0,
            rotating_term_b: 0.1,
            drag_term_c: 0.01,
            source_mass: 2.0,
            fixed_mass_factor: 2.0,
        }
    }

    #[test]
    fn vsp_matches_hand_computed_formula() {
        let p = physics(21);
        // v = 10 m/s, a = 1 m/s²:
        // (1·10 + 0.1·100 + 0.01·1000 + 2·10·1) / 2 = (10+10+10+20)/2 = 25.
        let vsp = vehicle_specific_power(&p, 10.0, 1.0).expect("non-zero mass");
        assert!((vsp - 25.0).abs() < 1e-12);
    }

    #[test]
    fn vsp_is_none_for_zero_mass_source_type() {
        let mut p = physics(21);
        p.source_mass = 0.0;
        assert!(vehicle_specific_power(&p, 10.0, 1.0).is_none());
    }

    #[test]
    fn hard_deceleration_is_braking() {
        // A single second at -2 mph/s or below brakes regardless of context.
        assert!(is_braking_second(&[-2.0], 0));
        assert!(is_braking_second(&[-3.5], 0));
        assert!(!is_braking_second(&[-1.9], 0));
    }

    #[test]
    fn sustained_deceleration_window_is_braking() {
        // Three consecutive seconds each below -1: the third brakes.
        let accels = [-1.5, -1.5, -1.5];
        assert!(is_braking_second(&accels, 2));
        // The first two cannot — they lack two predecessors.
        assert!(!is_braking_second(&accels, 0));
        assert!(!is_braking_second(&accels, 1));
        // A break in the run (one second at exactly -1) stops the window.
        let broken = [-1.5, -1.0, -1.5];
        assert!(!is_braking_second(&broken, 2));
    }

    #[test]
    fn classify_idle_overrides_everything_below_one_mph() {
        // speed < 1 mph is idle even when the second would otherwise brake.
        assert_eq!(
            classify_op_mode(0.5, Some(50.0), true, PolProcessId(101), &[]),
            Some(IDLE_OP_MODE),
        );
    }

    #[test]
    fn classify_stopped_brake_particulate_is_op_mode_501() {
        // speed == 0 with polProcessID 11609 → op mode 501, ahead of idle.
        assert_eq!(
            classify_op_mode(0.0, None, false, BRAKE_PARTICULATE_POLPROCESS, &[]),
            Some(STOPPED_BRAKE_OP_MODE),
        );
        // A different pollutant/process at speed 0 is plain idle.
        assert_eq!(
            classify_op_mode(0.0, None, false, PolProcessId(101), &[]),
            Some(IDLE_OP_MODE),
        );
    }

    #[test]
    fn classify_braking_beats_vsp_bins() {
        let bins = [OperatingModeBin {
            op_mode_id: 33,
            vsp_lower: Some(0.0),
            vsp_upper: None,
            speed_lower: Some(1.0),
            speed_upper: None,
        }];
        // A braking second above 1 mph is op mode 0, not the matching bin.
        assert_eq!(
            classify_op_mode(25.0, Some(10.0), true, PolProcessId(101), &bins),
            Some(BRAKING_OP_MODE),
        );
    }

    #[test]
    fn classify_picks_the_matching_vsp_speed_bin() {
        let bins = [
            OperatingModeBin {
                op_mode_id: 11,
                vsp_lower: None,
                vsp_upper: Some(0.0),
                speed_lower: Some(1.0),
                speed_upper: Some(25.0),
            },
            OperatingModeBin {
                op_mode_id: 12,
                vsp_lower: Some(0.0),
                vsp_upper: Some(3.0),
                speed_lower: Some(1.0),
                speed_upper: Some(25.0),
            },
        ];
        // VSP 1.5, speed 10 → only bin 12 contains it.
        assert_eq!(
            classify_op_mode(10.0, Some(1.5), false, PolProcessId(101), &bins),
            Some(12),
        );
        // VSP 100 matches no bin and the second is not braking → unbinned.
        assert_eq!(
            classify_op_mode(10.0, Some(100.0), false, PolProcessId(101), &bins),
            None,
        );
    }

    #[test]
    fn bracket_interpolates_between_two_schedules() {
        // Bin speed 15 between schedules at 10 and 20.
        let (lo, hi) = bracket_speed_bin(15.0, &[10.0, 20.0, 30.0]).unwrap();
        assert_eq!((lo, hi), (10.0, 20.0));
        // loFraction = (20-15)/(20-10) = 0.5.
        assert!((lo_schedule_fraction(15.0, lo, hi) - 0.5).abs() < 1e-12);
    }

    #[test]
    fn bracket_clamps_bins_outside_the_schedule_range() {
        // A bin below every schedule clamps lo and hi to the lowest speed.
        let (lo, hi) = bracket_speed_bin(5.0, &[10.0, 20.0]).unwrap();
        assert_eq!((lo, hi), (10.0, 10.0));
        assert_eq!(lo_schedule_fraction(5.0, lo, hi), 1.0);
        // A bin above every schedule clamps to the highest speed.
        let (lo, hi) = bracket_speed_bin(99.0, &[10.0, 20.0]).unwrap();
        assert_eq!((lo, hi), (20.0, 20.0));
        assert_eq!(lo_schedule_fraction(99.0, lo, hi), 1.0);
    }

    #[test]
    fn bracket_on_a_schedule_speed_assigns_the_whole_bin_low() {
        // A bin speed exactly on a schedule: lo == that schedule, and the
        // hi schedule is the next one up — loFraction = 1 only if equal.
        let (lo, hi) = bracket_speed_bin(20.0, &[10.0, 20.0, 30.0]).unwrap();
        assert_eq!((lo, hi), (20.0, 30.0));
        // loFraction = (30-20)/(30-20) = 1.0 here by interpolation.
        assert!((lo_schedule_fraction(20.0, lo, hi) - 1.0).abs() < 1e-12);
    }

    #[test]
    fn bracket_none_for_empty_schedule_set() {
        assert!(bracket_speed_bin(15.0, &[]).is_none());
    }

    /// Build a contiguous-second drive schedule from a speed (mph) trace.
    fn schedule_seconds(drive_schedule_id: i16, speeds: &[f64]) -> Vec<DriveScheduleSecond> {
        speeds
            .iter()
            .enumerate()
            .map(|(i, &speed)| DriveScheduleSecond {
                drive_schedule_id,
                second: i as i16 + 1,
                speed,
            })
            .collect()
    }

    #[test]
    fn schedule_op_mode_fractions_sum_to_one_when_all_seconds_classify() {
        // A flat 10-mph cruise: every classified second is idle-or-binned;
        // here no bins, so each second falls to "speed >= 1" → unbinned →
        // contributes to the denominator only. Add a bin that catches it.
        let bins = [OperatingModeBin {
            op_mode_id: 23,
            vsp_lower: None,
            vsp_upper: None,
            speed_lower: Some(1.0),
            speed_upper: None,
        }];
        let seconds = schedule_seconds(1, &[10.0, 10.0, 10.0, 10.0]);
        let fractions =
            op_mode_fractions_for_schedule(&seconds, &physics(21), PolProcessId(101), &bins);
        // 3 classified seconds (the first is dropped), all op mode 23.
        assert_eq!(fractions.len(), 1);
        assert!((fractions[&23] - 1.0).abs() < 1e-12);
    }

    #[test]
    fn schedule_op_mode_fractions_split_idle_and_cruise() {
        // Two seconds stopped, two cruising: after dropping the first,
        // classified seconds are [0, 10, 10] → idle, cruise, cruise.
        let bins = [OperatingModeBin {
            op_mode_id: 23,
            vsp_lower: None,
            vsp_upper: None,
            speed_lower: Some(1.0),
            speed_upper: None,
        }];
        let seconds = schedule_seconds(1, &[0.0, 0.0, 10.0, 10.0]);
        let fractions =
            op_mode_fractions_for_schedule(&seconds, &physics(21), PolProcessId(101), &bins);
        assert!((fractions[&IDLE_OP_MODE] - 1.0 / 3.0).abs() < 1e-12);
        assert!((fractions[&23] - 2.0 / 3.0).abs() < 1e-12);
    }

    #[test]
    fn schedule_op_mode_fractions_empty_for_single_second() {
        // One second yields no acceleration and no classified seconds.
        let seconds = schedule_seconds(1, &[10.0]);
        assert!(
            op_mode_fractions_for_schedule(&seconds, &physics(21), PolProcessId(101), &[])
                .is_empty()
        );
    }

    /// End-to-end fixture: one source type, one road type, two drive
    /// schedules (slow + fast) and one bin between them. Holds the input
    /// tables as owned `Vec`s; [`Self::inputs`] borrows them into an
    /// [`OpModeDistributionInputs`] view.
    struct EndToEndFixture {
        drive_schedule_assoc: Vec<DriveScheduleAssoc>,
        drive_schedules: Vec<DriveSchedule>,
        avg_speed_bins: Vec<AvgSpeedBin>,
        drive_schedule_seconds: Vec<DriveScheduleSecond>,
        source_type_physics: Vec<SourceTypePhysics>,
        operating_modes: Vec<OperatingModeBin>,
        op_mode_pol_proc_assoc: Vec<OpModePolProcAssoc>,
        run_spec_hour_day: Vec<i16>,
    }

    impl EndToEndFixture {
        fn inputs(&self) -> OpModeDistributionInputs<'_> {
            OpModeDistributionInputs {
                drive_schedule_assoc: &self.drive_schedule_assoc,
                drive_schedules: &self.drive_schedules,
                avg_speed_bins: &self.avg_speed_bins,
                drive_schedule_seconds: &self.drive_schedule_seconds,
                source_type_physics: &self.source_type_physics,
                operating_modes: &self.operating_modes,
                op_mode_pol_proc_assoc: &self.op_mode_pol_proc_assoc,
                run_spec_hour_day: &self.run_spec_hour_day,
            }
        }
    }

    fn end_to_end_fixture() -> EndToEndFixture {
        let mut drive_schedule_seconds = schedule_seconds(1, &[10.0, 10.0, 10.0, 10.0]);
        drive_schedule_seconds.extend(schedule_seconds(2, &[30.0, 30.0, 30.0, 30.0]));
        EndToEndFixture {
            drive_schedule_assoc: vec![
                DriveScheduleAssoc {
                    source_type_id: SourceTypeId(21),
                    road_type_id: RoadTypeId(5),
                    drive_schedule_id: 1,
                },
                DriveScheduleAssoc {
                    source_type_id: SourceTypeId(21),
                    road_type_id: RoadTypeId(5),
                    drive_schedule_id: 2,
                },
            ],
            drive_schedules: vec![
                DriveSchedule {
                    drive_schedule_id: 1,
                    average_speed: 10.0,
                },
                DriveSchedule {
                    drive_schedule_id: 2,
                    average_speed: 30.0,
                },
            ],
            avg_speed_bins: vec![AvgSpeedBin {
                avg_speed_bin_id: 20,
                avg_bin_speed: 20.0,
            }],
            drive_schedule_seconds,
            source_type_physics: vec![physics(21)],
            operating_modes: vec![OperatingModeBin {
                op_mode_id: 23,
                vsp_lower: None,
                vsp_upper: None,
                speed_lower: Some(1.0),
                speed_upper: None,
            }],
            op_mode_pol_proc_assoc: vec![OpModePolProcAssoc {
                pol_process_id: PolProcessId(101),
                op_mode_id: 23,
            }],
            run_spec_hour_day: vec![51, 52],
        }
    }

    #[test]
    fn end_to_end_weights_op_mode_fraction_by_schedule_split() {
        let fixture = end_to_end_fixture();
        let rows = operating_mode_distribution(&fixture.inputs());
        // Both schedules spend 100% of their classified seconds in op
        // mode 23, so the weighted fraction is loFraction + hiFraction = 1
        // — and it is replicated across the two hour/day combinations.
        assert_eq!(rows.len(), 2);
        for row in &rows {
            assert_eq!(row.source_type_id, SourceTypeId(21));
            assert_eq!(row.road_type_id, RoadTypeId(5));
            assert_eq!(row.avg_speed_bin_id, 20);
            assert_eq!(row.op_mode_id, 23);
            assert_eq!(row.pol_process_id, PolProcessId(101));
            assert!((row.op_mode_fraction - 1.0).abs() < 1e-12);
        }
        let hour_days: Vec<i16> = rows.iter().map(|r| r.hour_day_id).collect();
        assert_eq!(hour_days, vec![51, 52]);
    }

    #[test]
    fn end_to_end_output_is_sorted_by_primary_key() {
        let mut fixture = end_to_end_fixture();
        // Hour/day deliberately out of order.
        fixture.run_spec_hour_day = vec![52, 51];
        let rows = operating_mode_distribution(&fixture.inputs());
        let keys: Vec<RowKey> = rows.iter().map(OpModeDistributionRow::key).collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted);
    }

    #[test]
    fn excluded_polprocess_11710_produces_no_rows() {
        let mut fixture = end_to_end_fixture();
        // Only the excluded Tirewear pollutant/process is associated.
        fixture.op_mode_pol_proc_assoc = vec![OpModePolProcAssoc {
            pol_process_id: EXCLUDED_POLPROCESS,
            op_mode_id: 23,
        }];
        assert!(operating_mode_distribution(&fixture.inputs()).is_empty());
    }

    #[test]
    fn generator_metadata_matches_java_subscribe_to_me() {
        let gen = MesoscaleLookupOperatingModeDistributionGenerator::new();
        assert_eq!(
            gen.name(),
            "MesoscaleLookupOperatingModeDistributionGenerator"
        );
        assert_eq!(gen.output_tables(), &["OpModeDistribution"]);
        assert_eq!(gen.upstream(), &["SourceTypePhysics"]);
        let subs = gen.subscriptions();
        assert_eq!(subs.len(), 2);
        let processes: Vec<ProcessId> = subs.iter().map(|s| s.process_id).collect();
        assert_eq!(processes, vec![ProcessId(1), ProcessId(9)]);
        for s in subs {
            assert_eq!(s.granularity, Granularity::Year);
            assert_eq!(s.priority.display(), "GENERATOR");
        }
    }

    #[test]
    fn generator_execute_returns_placeholder_until_data_plane() {
        let gen = MesoscaleLookupOperatingModeDistributionGenerator::new();
        let ctx = CalculatorContext::new();
        assert!(gen.execute(&ctx).is_ok());
    }

    #[test]
    fn generator_is_object_safe() {
        let gen: Box<dyn Generator> =
            Box::new(MesoscaleLookupOperatingModeDistributionGenerator::new());
        assert_eq!(
            gen.name(),
            "MesoscaleLookupOperatingModeDistributionGenerator"
        );
    }
}

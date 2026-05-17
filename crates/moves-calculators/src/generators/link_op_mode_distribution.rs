//! Port of `LinkOperatingModeDistributionGenerator.java`
//! (`gov.epa.otaq.moves.master.implementation.ghg`) — the generator that
//! builds `OpModeDistribution` records for **Project-domain** runs from
//! the user-supplied per-link drive schedules.
//!
//! Migration plan: Phase 3, Task 34.
//!
//! # What this generator produces
//!
//! A Project-domain run models individual roadway *links*, each with its
//! own second-by-second drive schedule (`driveScheduleSecondLink`: speed
//! and grade per second). For every such link this generator derives the
//! `OpModeDistribution` table — the per-`(sourceType, hourDay, polProcess,
//! opMode)` operating-mode-fraction table the running-exhaust and brakewear
//! emission calculators consume.
//!
//! The Java class subscribes at `LINK` granularity / `GENERATOR+1` priority
//! to two emission processes — Running Exhaust (process id 1) and Brakewear
//! (process id 9) — and on each link change runs `calculateOpModeFractions`.
//!
//! # The live algorithm — drive-schedule path
//!
//! `calculateOpModeFractions` (Java steps 100–199) dispatches on what the
//! link provides:
//!
//! 1. **A drive schedule** → `calculateOpModeFractionsCore` derives the
//!    operating-mode distribution from the second-by-second speeds/grades.
//! 2. **No drive schedule and `linkAvgSpeed <= 0`** → a default schedule of
//!    30 seconds of idling is synthesised, then path 1 runs on it.
//! 3. **No drive schedule but `linkAvgSpeed > 0`** → `interpolateOpModeFractions`
//!    brackets the link's average speed between standard drive cycles.
//! 4. **An input `OpModeDistribution` already present** → the link keeps its
//!    user-supplied distribution; the generator emits nothing.
//!
//! Paths 1, 2 and 4 are ported here in full — they are the
//! drive-schedule computational core named by the task ("op-mode
//! distributions per link from **user-supplied link drive schedules**").
//! `calculateOpModeFractionsCore` itself is:
//!
//! * **VSP physics** ([`second_physics`]) — for each second compute three
//!   trailing accelerations (`At0`, `At1`, `At2`) and the vehicle-specific
//!   power `VSP` from the speed/grade trace and the source type's physics
//!   terms;
//! * **op-mode assignment** ([`OpModeClassifier`]) — the data-driven
//!   `operatingMode` VSP/speed brackets plus the stopped / idle / braking
//!   special cases;
//! * **fraction counting** ([`op_mode_fractions_from_schedule`]) —
//!   `opModeFraction = secondCount / secondTotal` per source type;
//! * **distribution expansion** ([`expand_to_op_mode_distribution`]) — fan
//!   the per-source-type fractions out across `opModePolProcAssoc` and the
//!   RunSpec's hour/day set, fold stopped-mode 501 to idle-mode 1, and sum.
//!
//! # Scope — the interpolation path is deferred
//!
//! Path 3 (`interpolateOpModeFractions`, Java steps 101–105) is *not*
//! ported by this task. It is the no-drive-schedule fallback: it interpolates
//! a link's operating modes from its average speed between bracketing
//! standard drive cycles. Its interpolation arithmetic is trivial, but the
//! surrounding logic is deeply entangled with the SourceTypePhysics
//! model-year-physics expansion (`physicsOperatingMode`,
//! `sourceUseTypePhysicsMapping`, `createExpandedOperatingModesTable`) —
//! migration-plan Task 37 — and the average-speed-binned op-mode-distribution
//! concept is the subject of its own task, Task 31
//! (`AverageSpeedOperatingModeDistributionGenerator`). The reusable heart of
//! that path — `calculateOpModeFractionsCore`, which `interpolateOpModeFractions`
//! itself calls once per bracketing cycle — *is* ported here as
//! [`op_mode_fractions_from_schedule`].
//!
//! Likewise `populateRatesOpModeDistribution` (Java steps 200–299) — the
//! `DO_RATES_FIRST` per-link refresh of `ratesOpModeDistribution` — is not
//! ported here: it writes the table owned by Task 43's
//! `RatesOperatingModeDistributionGenerator` and performs a stateful SQL
//! copy with no computation.
//!
//! # Fidelity notes
//!
//! * **`FLOAT` columns held as `f64`.** Every floating-point input —
//!   `driveScheduleSecondLink.speed`/`grade`, the `operatingMode` VSP/speed
//!   bounds, the `sourceUseTypePhysicsMapping` physics terms and
//!   `link.linkAvgSpeed` — is a 32-bit `FLOAT` column in the MOVES schema.
//!   This port holds them as `f64` and evaluates the VSP/acceleration
//!   arithmetic in `f64`, matching MySQL's promotion of every arithmetic
//!   expression to `DOUBLE`, but not the `f32` truncation of the stored
//!   column *values*. `tempDriveScheduleSecondLink.speed` and
//!   `opModeDistribution.opModeFraction` are likewise `FLOAT` intermediates.
//!   Following the Task 33/41 precedent, the port computes in `f64` and
//!   leaves the bug-compatibility decision to Task 44 (generator integration
//!   validation).
//! * **`sin(atan(x))` evaluated literally.** The grade term
//!   `9.81/0.44704 * sin(atan(grade/100))` is ported as the literal
//!   `atan` followed by `sin`, not the algebraic identity
//!   `x / sqrt(1 + x^2)`, so the port calls the same two libm routines the
//!   MySQL expression does.
//! * **`secondCount * 1.0 / secondTotal`.** The Java multiplies by `1.0`
//!   precisely to force floating-point division; the port divides `f64`
//!   counts directly, which is the same operation (no MariaDB integer
//!   division rounding applies).
//!
//! # Data plane (Task 50)
//!
//! [`Generator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are Phase-2 placeholders until the
//! `DataFrameStore` lands (migration-plan Task 50), so `execute` cannot yet
//! read the input tables nor write `OpModeDistribution`. The numerically
//! faithful algorithm is fully ported and unit-tested in the free functions
//! and in
//! [`op_mode_distribution`](LinkOperatingModeDistributionGenerator::op_mode_distribution);
//! once the data plane exists, `execute` projects a [`LinkDriveScheduleInputs`]
//! out of `ctx.tables()` for the current link and stores the result in the
//! scratch namespace.

use std::collections::{HashMap, HashSet};

use moves_calculator_info::{Granularity, Priority};
use moves_data::{PolProcessId, ProcessId, SourceTypeId};
use moves_framework::{
    CalculatorContext, CalculatorOutput, CalculatorSubscription, Error, Generator,
};

/// Running Exhaust — process id 1. One of the two processes the Java
/// `subscribeToMe` registers for.
const RUNNING_EXHAUST: ProcessId = ProcessId(1);
/// Brakewear — process id 9. The other process `subscribeToMe` registers.
const BRAKEWEAR: ProcessId = ProcessId(9);

/// Conversion factor from miles/hour to metres/second — the
/// `0.44704 (metre*hour)/(mile*second)` constant the MOVES drive-cycle SQL
/// multiplies every speed by.
const MPS_PER_MPH: f64 = 0.44704;
/// Standard gravity in m/s², written `9.81` in the MOVES SQL.
const GRAVITY: f64 = 9.81;

/// Stopped pseudo-mode — assigned by the op-mode CASE when `speed = 0`.
/// Folded to [`IDLE_OP_MODE`] downstream for every polProcess except
/// [`STOPPED_POLPROCESS`].
const STOPPED_OP_MODE: i16 = 501;
/// Idle op mode — assigned when `speed < 1`, and the fold target for 501.
const IDLE_OP_MODE: i16 = 1;
/// Braking op mode — assigned by the deceleration test in the CASE.
const BRAKING_OP_MODE: i16 = 0;
/// Unassigned op mode — the `else -1` arm of the assignment CASE. A second
/// whose VSP/speed matches no bracket carries this; it is inner-joined away
/// by [`expand_to_op_mode_distribution`] unless `opModePolProcAssoc` lists
/// it, which canonically it never does.
const UNASSIGNED_OP_MODE: i16 = -1;

/// The single polProcessID for which op-mode 501 is *kept* (rather than
/// folded to op-mode 1) during distribution expansion — Java
/// `if(omppa.polProcessID=11609,501,1)`.
const STOPPED_POLPROCESS: PolProcessId = PolProcessId(11609);

/// Operating modes excluded from the data-driven bracket scan — opModes 26
/// and 36 are "redundant with others" per `buildOpModeClause`.
const REDUNDANT_OP_MODES: [i16; 2] = [26, 36];

/// Length, in seconds, of the synthesised all-idle drive schedule used for
/// a link whose `linkAvgSpeed <= 0`.
const DEFAULT_IDLE_SECONDS: i16 = 30;

/// One `operatingMode` row's VSP/speed bracket — the data-driven part of
/// the op-mode assignment CASE built by Java `buildOpModeClause`.
///
/// Each of the four bounds is a NULL-able `FLOAT` column; a `None` bound
/// contributes no condition (an open end). `buildOpModeClause` reads only
/// rows with `1 <= opModeID <= 99`, excluding the redundant modes 26 and
/// 36; [`OpModeClassifier::new`] applies that same filter.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OperatingModeBracket {
    /// `opModeID` — the operating mode this bracket assigns.
    pub op_mode_id: i16,
    /// `VSPLower` — inclusive lower VSP bound (`VSPLower <= VSP`).
    pub vsp_lower: Option<f64>,
    /// `VSPUpper` — exclusive upper VSP bound (`VSP < VSPUpper`).
    pub vsp_upper: Option<f64>,
    /// `speedLower` — inclusive lower speed bound (`speedLower <= speed`).
    pub speed_lower: Option<f64>,
    /// `speedUpper` — exclusive upper speed bound (`speed < speedUpper`).
    pub speed_upper: Option<f64>,
}

impl OperatingModeBracket {
    /// Whether `(vsp, speed)` falls inside this bracket. A `None` bound
    /// imposes no constraint, matching `buildOpModeClause` omitting the
    /// condition for a NULL column.
    #[must_use]
    fn matches(&self, vsp: f64, speed: f64) -> bool {
        // `map_or(true, …)` rather than `is_none_or` — the workspace MSRV
        // (1.78) predates `Option::is_none_or` (stable 1.82).
        self.vsp_lower.map_or(true, |lo| lo <= vsp)
            && self.vsp_upper.map_or(true, |hi| vsp < hi)
            && self.speed_lower.map_or(true, |lo| lo <= speed)
            && self.speed_upper.map_or(true, |hi| speed < hi)
    }
}

/// The prepared op-mode assignment CASE — the port of `buildOpModeClause`
/// plus the fixed stopped / idle / braking arms of the step-110 `UPDATE`.
///
/// The Java memoises `opModeAssignmentSQL`; this struct is the equivalent
/// once-built form: it holds the `operatingMode` brackets already filtered
/// to `1 <= opModeID <= 99` (minus the redundant modes) and sorted by
/// `opModeID`, so [`classify`](Self::classify) is a single ordered scan.
#[derive(Debug, Clone)]
pub struct OpModeClassifier {
    /// `operatingMode` brackets, filtered and sorted by `opModeID` — the
    /// prepared form of `buildOpModeClause`'s `ORDER BY opModeID` query.
    brackets: Vec<OperatingModeBracket>,
}

impl OpModeClassifier {
    /// Prepare the classifier from the full `operatingMode` table.
    ///
    /// Mirrors `buildOpModeClause`: keep only `1 <= opModeID <= 99`
    /// excluding the redundant modes 26 and 36, ordered by `opModeID`.
    #[must_use]
    pub fn new(operating_mode: &[OperatingModeBracket]) -> Self {
        let mut brackets: Vec<OperatingModeBracket> = operating_mode
            .iter()
            .copied()
            .filter(|b| {
                (1..=99).contains(&b.op_mode_id) && !REDUNDANT_OP_MODES.contains(&b.op_mode_id)
            })
            .collect();
        brackets.sort_by_key(|b| b.op_mode_id);
        Self { brackets }
    }

    /// Assign an operating mode to one second of the drive-cycle trace.
    ///
    /// Ports the step-110 `UPDATE … SET opModeID = CASE …`, evaluated
    /// top-down:
    ///
    /// 1. `speed = 0` → `STOPPED_OP_MODE` (501);
    /// 2. `speed < 1` → `IDLE_OP_MODE` (1);
    /// 3. `At0 <= -2` *or* (`At0 < -1` *and* `At1 < -1` *and* `At2 < -1`) →
    ///    `BRAKING_OP_MODE` (0);
    /// 4. the first `operatingMode` bracket (in `opModeID` order) whose
    ///    VSP/speed range contains the second → its `opModeID`;
    /// 5. otherwise `UNASSIGNED_OP_MODE` (-1).
    #[must_use]
    pub fn classify(&self, second: &SecondPhysics) -> i16 {
        if second.speed == 0.0 {
            return STOPPED_OP_MODE;
        }
        if second.speed < 1.0 {
            return IDLE_OP_MODE;
        }
        if second.accel_t0 <= -2.0
            || (second.accel_t0 < -1.0 && second.accel_t1 < -1.0 && second.accel_t2 < -1.0)
        {
            return BRAKING_OP_MODE;
        }
        for bracket in &self.brackets {
            if bracket.matches(second.vsp, second.speed) {
                return bracket.op_mode_id;
            }
        }
        UNASSIGNED_OP_MODE
    }
}

/// One second of a link's drive schedule — a `driveScheduleSecondLink` row.
///
/// `speed` (mph) and `grade` (percent) are `FLOAT` columns; see the module
/// fidelity notes for why they are held as `f64` here.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DriveScheduleSecond {
    /// `secondID` — the 1-based second index within the schedule.
    pub second_id: i16,
    /// `speed` — vehicle speed at this second, miles per hour.
    pub speed: f64,
    /// `grade` — road grade at this second, percent.
    pub grade: f64,
}

/// Source-type physics terms — one `sourceUseTypePhysicsMapping` row.
///
/// Produced upstream by the SourceTypePhysics generator (migration-plan
/// Task 37). The drive-cycle SQL cross-joins the per-link schedule with
/// this table (filtered to RunSpec source types) so every second is
/// evaluated under each modelled source type's physics.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypePhysics {
    /// `realSourceTypeID` — the underlying RunSpec source type; the
    /// `RunSpecSourceType` join filters on this.
    pub real_source_type_id: SourceTypeId,
    /// `tempSourceTypeID` — the physics-expanded synthetic source type the
    /// output operating-mode fractions are keyed by.
    pub temp_source_type_id: SourceTypeId,
    /// `rollingTermA` — the rolling-resistance coefficient of the
    /// road-load polynomial.
    pub rolling_term_a: f64,
    /// `rotatingTermB` — the rotating-resistance coefficient.
    pub rotating_term_b: f64,
    /// `dragTermC` — the aerodynamic-drag coefficient.
    pub drag_term_c: f64,
    /// `sourceMass` — vehicle mass, used by the inertial and grade terms.
    pub source_mass: f64,
    /// `fixedMassFactor` — the divisor converting tractive power to
    /// vehicle-specific power.
    pub fixed_mass_factor: f64,
}

/// Per-second drive-cycle physics — one `tempDriveScheduleSecondLink` row
/// before op-mode assignment.
///
/// `speed` is copied straight from the [`DriveScheduleSecond`]; the three
/// accelerations and `vsp` are derived by [`second_physics`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SecondPhysics {
    /// `secondID` — the second this row describes.
    pub second_id: i16,
    /// `speed` — the schedule speed at this second (mph).
    pub speed: f64,
    /// `At0` — the trailing acceleration over `[t-1, t]`, including the
    /// gravitational component of grade. Zero at the first second.
    pub accel_t0: f64,
    /// `At1` — the trailing acceleration over `[t-2, t-1]`.
    pub accel_t1: f64,
    /// `At2` — the trailing acceleration over `[t-3, t-2]`.
    pub accel_t2: f64,
    /// `VSP` — vehicle-specific power (kW/tonne).
    pub vsp: f64,
}

/// The grade term `sin(atan(grade / 100))`, shared by the acceleration and
/// VSP expressions. Evaluated as the literal `atan` then `sin` to match the
/// MySQL expression (see the module fidelity notes).
fn grade_sine(grade: f64) -> f64 {
    (grade / 100.0).atan().sin()
}

/// Compute the per-second accelerations and VSP for one link's drive
/// schedule under one source type's physics — the port of the
/// `tempDriveScheduleSecondLink` `SELECT` of `calculateOpModeFractionsCore`.
///
/// For each second `t` the SQL self-joins the schedule to its three
/// predecessors `t-1`, `t-2`, `t-3` (chained left joins: a missing `t-1`
/// also drops `t-2` and `t-3`). With the predecessor speeds `b`, `c`, `d`:
///
/// ```text
/// At0 = (speed[t]   - speed[t-1]) + 9.81/0.44704 * sin(atan(grade[t]  /100))   (0 if t-1 absent)
/// At1 = (speed[t-1] - speed[t-2]) + 9.81/0.44704 * sin(atan(grade[t-1]/100))   (0 if t-2 absent)
/// At2 = (speed[t-2] - speed[t-3]) + 9.81/0.44704 * sin(atan(grade[t-2]/100))   (0 if t-3 absent)
///
/// va  = speed[t] * 0.44704
/// VSP = ( va*(rollingTermA + va*(rotatingTermB + dragTermC*va))
///         + sourceMass*va*coalesce(speed[t]-speed[t-1], 0)*0.44704
///         + sourceMass*9.81*sin(atan(grade[t]/100))*va )
///       / fixedMassFactor
/// ```
///
/// The result is one [`SecondPhysics`] per schedule second, ordered by
/// `secondID`.
#[must_use]
pub fn second_physics(
    schedule: &[DriveScheduleSecond],
    physics: &SourceTypePhysics,
) -> Vec<SecondPhysics> {
    let by_second: HashMap<i16, &DriveScheduleSecond> =
        schedule.iter().map(|s| (s.second_id, s)).collect();
    let mut seconds: Vec<&DriveScheduleSecond> = schedule.iter().collect();
    seconds.sort_by_key(|s| s.second_id);

    seconds
        .into_iter()
        .map(|a| {
            // Chained predecessor lookup: `c` joins onto `b`, `d` onto `c`,
            // so a gap at `t-1` also makes `t-2`/`t-3` absent.
            let b = by_second.get(&(a.second_id - 1)).copied();
            let c = b.and_then(|b| by_second.get(&(b.second_id - 1)).copied());
            let d = c.and_then(|c| by_second.get(&(c.second_id - 1)).copied());

            // The whole acceleration expression is NULL (→ 0.0) when the
            // speed delta's predecessor is absent — the COALESCE wraps the
            // grade term too.
            let accel_t0 = match b {
                Some(b) => (a.speed - b.speed) + GRAVITY / MPS_PER_MPH * grade_sine(a.grade),
                None => 0.0,
            };
            let accel_t1 = match (b, c) {
                (Some(b), Some(c)) => {
                    (b.speed - c.speed) + GRAVITY / MPS_PER_MPH * grade_sine(b.grade)
                }
                _ => 0.0,
            };
            let accel_t2 = match (c, d) {
                (Some(c), Some(d)) => {
                    (c.speed - d.speed) + GRAVITY / MPS_PER_MPH * grade_sine(c.grade)
                }
                _ => 0.0,
            };

            let va = a.speed * MPS_PER_MPH;
            let rolling = va
                * (physics.rolling_term_a
                    + va * (physics.rotating_term_b + physics.drag_term_c * va));
            // coalesce(speed[t] - speed[t-1], 0.0): zero at the first second.
            let speed_delta = b.map_or(0.0, |b| a.speed - b.speed);
            let inertial = physics.source_mass * va * speed_delta * MPS_PER_MPH;
            let grade_power = physics.source_mass * GRAVITY * grade_sine(a.grade) * va;
            let vsp = (rolling + inertial + grade_power) / physics.fixed_mass_factor;

            SecondPhysics {
                second_id: a.second_id,
                speed: a.speed,
                accel_t0,
                accel_t1,
                accel_t2,
                vsp,
            }
        })
        .collect()
}

/// An operating-mode fraction for one (synthetic) source type — the port
/// of a `tempDriveScheduleSecondLinkFraction` row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OpModeFraction {
    /// `sourceTypeID` — the physics-expanded `tempSourceTypeID`.
    pub source_type_id: SourceTypeId,
    /// `opModeID` — the operating mode.
    pub op_mode_id: i16,
    /// `opModeFraction` — `secondCount / secondTotal`.
    pub op_mode_fraction: f64,
}

/// Derive the per-source-type operating-mode fractions for one link's drive
/// schedule — the computational core of `calculateOpModeFractionsCore`.
///
/// The Java cross-joins the schedule with `sourceUseTypePhysicsMapping`
/// (inner-joined to `RunSpecSourceType` on `realSourceTypeID`), assigns an
/// op mode to every `(second, physics row)` pair, then for each
/// `tempSourceTypeID`:
///
/// * `secondTotal` = the number of `(second, physics row)` pairs;
/// * `secondCount` = that count per `opModeID`;
/// * `opModeFraction = secondCount * 1.0 / secondTotal`.
///
/// `physics` rows whose `real_source_type_id` is absent from
/// `run_spec_source_type` are dropped — the `RunSpecSourceType` inner join.
/// Output is ordered by `(sourceTypeID, opModeID)`.
#[must_use]
pub fn op_mode_fractions_from_schedule(
    schedule: &[DriveScheduleSecond],
    physics: &[SourceTypePhysics],
    run_spec_source_type: &[SourceTypeId],
    classifier: &OpModeClassifier,
) -> Vec<OpModeFraction> {
    let run_spec: HashSet<SourceTypeId> = run_spec_source_type.iter().copied().collect();

    // One (tempSourceTypeID, opModeID) tuple per (second, applicable
    // physics row) — the assigned-op-mode `tempDriveScheduleSecondLink`.
    let mut totals: HashMap<SourceTypeId, u32> = HashMap::new();
    let mut counts: HashMap<(SourceTypeId, i16), u32> = HashMap::new();
    for p in physics {
        if !run_spec.contains(&p.real_source_type_id) {
            continue;
        }
        for second in second_physics(schedule, p) {
            let op_mode = classifier.classify(&second);
            *totals.entry(p.temp_source_type_id).or_insert(0) += 1;
            *counts.entry((p.temp_source_type_id, op_mode)).or_insert(0) += 1;
        }
    }

    let mut fractions: Vec<OpModeFraction> = counts
        .into_iter()
        .map(|((source_type_id, op_mode_id), count)| OpModeFraction {
            source_type_id,
            op_mode_id,
            // secondCount * 1.0 / secondTotal — `totals` always holds the
            // source type, since every count incremented it in lockstep.
            op_mode_fraction: f64::from(count) / f64::from(totals[&source_type_id]),
        })
        .collect();
    fractions.sort_by_key(|f| (f.source_type_id, f.op_mode_id));
    fractions
}

/// An `opModePolProcAssoc` row — which operating modes a pollutant/process
/// pairing carries. The distribution expansion fans each source-type
/// fraction out across these.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpModePolProcAssoc {
    /// `polProcessID`.
    pub pol_process_id: PolProcessId,
    /// `opModeID`.
    pub op_mode_id: i16,
}

/// One `OpModeDistribution` row produced by this generator.
///
/// Models the six columns the Java `INSERT` populates. The execution-database
/// table also has `opModeFractionCV` and `isUserInput`; this generator never
/// sets `opModeFractionCV` (it defaults `NULL`) and every row it emits is
/// generated, not user input (`isUserInput` defaults `'N'`), so neither is
/// modelled here.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OpModeDistributionRow {
    /// `sourceTypeID` — the physics-expanded `tempSourceTypeID`.
    pub source_type_id: SourceTypeId,
    /// `hourDayID` — one of the RunSpec's selected hour/day combinations.
    pub hour_day_id: i16,
    /// `linkID` — the link this distribution is for.
    pub link_id: i32,
    /// `polProcessID` — the pollutant/process the fraction applies to.
    pub pol_process_id: PolProcessId,
    /// `opModeID` — the operating mode, after the 501 → 1 fold.
    pub op_mode_id: i16,
    /// `opModeFraction` — the operating-mode fraction.
    pub op_mode_fraction: f64,
}

/// Group key for the expansion's final `GROUP BY` / sum.
type DistributionKey = (SourceTypeId, i16, i32, PolProcessId, i16);

/// Fan the per-source-type operating-mode fractions out into
/// `OpModeDistribution` rows — the port of the `opModeDistributionTemp`
/// expansion and its summing `INSERT` into `opModeDistribution`.
///
/// For every [`OpModeFraction`] and every `opModePolProcAssoc` row sharing
/// its `opModeID`:
///
/// * the output op mode is the fraction's, except a stopped-mode 501 folds
///   to idle-mode 1 — kept as 501 only for `STOPPED_POLPROCESS`
///   (`if(opModeID=501, if(polProcessID=11609,501,1), opModeID)`);
/// * the row is crossed with every `run_spec_hour_day` entry;
/// * a `(sourceTypeID, polProcessID, linkID)` triple already present in
///   `existing_op_mode_keys` (the `tempExistingOpMode` snapshot of
///   user-input distributions) is skipped — the anti-join.
///
/// Rows sharing a `(sourceType, hourDay, link, polProcess, opMode)` key —
/// which the 501 → 1 fold can create — have their fractions summed. Output
/// is ordered by that key.
///
/// An `opModeID` absent from `op_mode_pol_proc_assoc` (notably
/// `UNASSIGNED_OP_MODE`) contributes nothing — the inner join drops it.
#[must_use]
pub fn expand_to_op_mode_distribution(
    link_id: i32,
    fractions: &[OpModeFraction],
    op_mode_pol_proc_assoc: &[OpModePolProcAssoc],
    run_spec_hour_day: &[i16],
    existing_op_mode_keys: &HashSet<(SourceTypeId, PolProcessId, i32)>,
) -> Vec<OpModeDistributionRow> {
    let mut summed: HashMap<DistributionKey, f64> = HashMap::new();

    for fraction in fractions {
        for assoc in op_mode_pol_proc_assoc {
            // `using (opModeID)` joins on the *pre-fold* op mode.
            if assoc.op_mode_id != fraction.op_mode_id {
                continue;
            }
            // tempExistingOpMode anti-join: skip links/source types/processes
            // that already carry a user-supplied distribution.
            if existing_op_mode_keys.contains(&(
                fraction.source_type_id,
                assoc.pol_process_id,
                link_id,
            )) {
                continue;
            }
            let op_mode_id = if fraction.op_mode_id == STOPPED_OP_MODE {
                if assoc.pol_process_id == STOPPED_POLPROCESS {
                    STOPPED_OP_MODE
                } else {
                    IDLE_OP_MODE
                }
            } else {
                fraction.op_mode_id
            };
            for &hour_day_id in run_spec_hour_day {
                let key = (
                    fraction.source_type_id,
                    hour_day_id,
                    link_id,
                    assoc.pol_process_id,
                    op_mode_id,
                );
                *summed.entry(key).or_insert(0.0) += fraction.op_mode_fraction;
            }
        }
    }

    let mut rows: Vec<OpModeDistributionRow> = summed
        .into_iter()
        .map(
            |((source_type_id, hour_day_id, link_id, pol_process_id, op_mode_id), fraction)| {
                OpModeDistributionRow {
                    source_type_id,
                    hour_day_id,
                    link_id,
                    pol_process_id,
                    op_mode_id,
                    op_mode_fraction: fraction,
                }
            },
        )
        .collect();
    rows.sort_by_key(|r| {
        (
            r.source_type_id,
            r.hour_day_id,
            r.link_id,
            r.pol_process_id,
            r.op_mode_id,
        )
    });
    rows
}

/// The default all-idle drive schedule synthesised for a link whose
/// `linkAvgSpeed <= 0` — `DEFAULT_IDLE_SECONDS` (30) seconds of zero speed
/// and zero grade.
///
/// Ports the Java step-100 fallback: "Provide a default drive schedule of
/// all idling. Use 0 grade because with 0 speed, brakes are likely applied
/// rather than using the engine to counteract any grade." Every second has
/// `speed = 0`, so the classifier assigns `STOPPED_OP_MODE` throughout.
#[must_use]
pub fn default_idle_drive_schedule() -> Vec<DriveScheduleSecond> {
    (1..=DEFAULT_IDLE_SECONDS)
        .map(|second_id| DriveScheduleSecond {
            second_id,
            speed: 0.0,
            grade: 0.0,
        })
        .collect()
}

/// The projected per-link inputs the drive-schedule path of
/// `calculateOpModeFractions` reads.
///
/// Once the Task 50 data plane lands,
/// [`Generator::execute`] builds this view from `ctx.tables()` for the link
/// in `ctx.position()`.
#[derive(Debug, Clone, Copy)]
pub struct LinkDriveScheduleInputs<'a> {
    /// `linkID` of the link being processed. Bracketing pseudo-links use
    /// negative ids; real links are positive.
    pub link_id: i32,
    /// `driveScheduleSecondLink` rows for this link — empty if the link
    /// supplied no drive schedule.
    pub drive_schedule: &'a [DriveScheduleSecond],
    /// `link.linkAvgSpeed` for this link, if present. Consulted only when
    /// `drive_schedule` is empty.
    pub link_avg_speed: Option<f64>,
    /// Whether `opModeDistribution` already holds a running-process
    /// (`polProcessID % 100 = 1`) distribution for this link — Java's
    /// `hasRunningOpModeDistribution`. When true the link keeps its
    /// user-supplied distribution and the generator emits nothing.
    pub has_running_op_mode_distribution: bool,
    /// The full `operatingMode` table — the VSP/speed brackets.
    pub operating_mode: &'a [OperatingModeBracket],
    /// `sourceUseTypePhysicsMapping` rows — the source-type physics terms.
    pub physics: &'a [SourceTypePhysics],
    /// `runSpecSourceType.sourceTypeID` — the RunSpec's selected source
    /// types; the physics cross-join is filtered to these.
    pub run_spec_source_type: &'a [SourceTypeId],
    /// `opModePolProcAssoc` — operating modes per pollutant/process.
    pub op_mode_pol_proc_assoc: &'a [OpModePolProcAssoc],
    /// `runSpecHourDay.hourDayID` — the hour/day combinations every row is
    /// crossed with.
    pub run_spec_hour_day: &'a [i16],
    /// The `tempExistingOpMode` snapshot — `(sourceTypeID, polProcessID,
    /// linkID)` triples already in `opModeDistribution` before generation.
    pub existing_op_mode_keys: &'a HashSet<(SourceTypeId, PolProcessId, i32)>,
}

/// `OpModeDistribution` generator for Project-domain (per-link) runs.
///
/// Ports `LinkOperatingModeDistributionGenerator.java`; see the module
/// documentation for the scope of the port.
#[derive(Debug, Clone)]
pub struct LinkOperatingModeDistributionGenerator {
    /// The two master-loop subscriptions, built once in [`Self::new`].
    subscriptions: [CalculatorSubscription; 2],
}

impl LinkOperatingModeDistributionGenerator {
    /// Chain-DAG name — matches the Java class name.
    pub const NAME: &'static str = "LinkOperatingModeDistributionGenerator";

    /// Construct the generator with its master-loop subscriptions.
    ///
    /// Mirrors `subscribeToMe`: Running Exhaust and Brakewear, both at
    /// `LINK` granularity and `GENERATOR+1` priority. The Java comment
    /// explains the `+1`: the generator fills `Link.linkAvgSpeed` from the
    /// drive schedules, and that column is needed by `ProjectTAG` — which
    /// runs at the plain `GENERATOR` priority — for its SHO calculations.
    #[must_use]
    pub fn new() -> Self {
        let priority =
            Priority::parse("GENERATOR+1").expect("\"GENERATOR+1\" is a valid MasterLoop priority");
        let at_link = |process| CalculatorSubscription::new(process, Granularity::Link, priority);
        Self {
            subscriptions: [at_link(RUNNING_EXHAUST), at_link(BRAKEWEAR)],
        }
    }

    /// Compute the `OpModeDistribution` rows for one link — the
    /// drive-schedule path of the Java `calculateOpModeFractions`.
    ///
    /// Dispatches as the Java does:
    ///
    /// * an input running-process distribution is already present → emit
    ///   nothing (the link keeps its user-supplied distribution);
    /// * the link has a drive schedule → derive the distribution from it;
    /// * no drive schedule but `linkAvgSpeed <= 0` → synthesise the
    ///   [`default_idle_drive_schedule`] and derive from that;
    /// * no drive schedule and `linkAvgSpeed > 0` → the interpolation path,
    ///   which is out of scope for this task (see the module docs); emit
    ///   nothing.
    #[must_use]
    pub fn op_mode_distribution(
        &self,
        inputs: &LinkDriveScheduleInputs<'_>,
    ) -> Vec<OpModeDistributionRow> {
        if inputs.has_running_op_mode_distribution {
            return Vec::new();
        }

        let classifier = OpModeClassifier::new(inputs.operating_mode);
        let fractions = if !inputs.drive_schedule.is_empty() {
            op_mode_fractions_from_schedule(
                inputs.drive_schedule,
                inputs.physics,
                inputs.run_spec_source_type,
                &classifier,
            )
        } else if inputs.link_avg_speed.is_some_and(|s| s <= 0.0) {
            // No drive schedule and a non-positive average speed: derive the
            // distribution from a synthesised 30-second all-idle schedule.
            op_mode_fractions_from_schedule(
                &default_idle_drive_schedule(),
                inputs.physics,
                inputs.run_spec_source_type,
                &classifier,
            )
        } else {
            // No drive schedule and a positive average speed: the
            // interpolation path (Task 31 / Task 37 territory).
            return Vec::new();
        };
        expand_to_op_mode_distribution(
            inputs.link_id,
            &fractions,
            inputs.op_mode_pol_proc_assoc,
            inputs.run_spec_hour_day,
            inputs.existing_op_mode_keys,
        )
    }
}

impl Default for LinkOperatingModeDistributionGenerator {
    fn default() -> Self {
        Self::new()
    }
}

/// Default-database / scratch tables the drive-schedule path reads. Names
/// are the canonical MOVES table names; the registry maps them onto the
/// per-run snapshots.
static INPUT_TABLES: &[&str] = &[
    "driveScheduleSecondLink",
    "link",
    "operatingMode",
    "sourceUseTypePhysicsMapping",
    "runSpecSourceType",
    "opModePolProcAssoc",
    "runSpecHourDay",
    "opModeDistribution",
];

/// Scratch-namespace table this generator writes.
static OUTPUT_TABLES: &[&str] = &["OpModeDistribution"];

/// Upstream generator: `SourceTypePhysics` supplies the model-year physics
/// (`sourceUseTypePhysicsMapping`, the `modelYearPhysics` setup and the
/// op-mode-distribution corrections the Java applies after generation).
static UPSTREAM: &[&str] = &["SourceTypePhysics"];

impl Generator for LinkOperatingModeDistributionGenerator {
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
    /// **Data plane pending (Task 50).** [`CalculatorContext`] exposes only
    /// placeholder `ExecutionTables` / `ScratchNamespace` today, so this
    /// body cannot read the [`input_tables`](Generator::input_tables) nor
    /// write `OpModeDistribution`. The numerically faithful algorithm is
    /// fully ported and tested in
    /// [`op_mode_distribution`](Self::op_mode_distribution); once the
    /// `DataFrameStore` lands, `execute` will project a
    /// [`LinkDriveScheduleInputs`] from `ctx.tables()` for the link in
    /// `ctx.position()` and store the rows.
    fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        Ok(CalculatorOutput::empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a [`SecondPhysics`] for classifier tests — only the fields the
    /// CASE reads need to be meaningful.
    fn second(speed: f64, vsp: f64, at0: f64, at1: f64, at2: f64) -> SecondPhysics {
        SecondPhysics {
            second_id: 1,
            speed,
            accel_t0: at0,
            accel_t1: at1,
            accel_t2: at2,
            vsp,
        }
    }

    /// A VSP-only bracket (no speed bounds) for `op_mode_id`.
    fn vsp_bracket(
        op_mode_id: i16,
        lower: Option<f64>,
        upper: Option<f64>,
    ) -> OperatingModeBracket {
        OperatingModeBracket {
            op_mode_id,
            vsp_lower: lower,
            vsp_upper: upper,
            speed_lower: None,
            speed_upper: None,
        }
    }

    /// Physics terms with the road-load polynomial zeroed except
    /// `rollingTermA` — keeps hand-computed VSP checks simple.
    fn physics(real: u16, temp: u16) -> SourceTypePhysics {
        SourceTypePhysics {
            real_source_type_id: SourceTypeId(real),
            temp_source_type_id: SourceTypeId(temp),
            rolling_term_a: 1.0,
            rotating_term_b: 0.0,
            drag_term_c: 0.0,
            source_mass: 10.0,
            fixed_mass_factor: 2.0,
        }
    }

    #[test]
    fn classifier_assigns_stopped_mode_for_zero_speed() {
        let classifier = OpModeClassifier::new(&[vsp_bracket(13, None, None)]);
        // speed == 0 short-circuits ahead of every other arm.
        assert_eq!(
            classifier.classify(&second(0.0, 99.0, -9.0, -9.0, -9.0)),
            501
        );
    }

    #[test]
    fn classifier_assigns_idle_below_one_mph() {
        let classifier = OpModeClassifier::new(&[vsp_bracket(13, None, None)]);
        assert_eq!(classifier.classify(&second(0.5, 99.0, 0.0, 0.0, 0.0)), 1);
        // Exactly 1 mph is not idle — the test is `speed < 1`.
        assert_ne!(classifier.classify(&second(1.0, 99.0, 0.0, 0.0, 0.0)), 1);
    }

    #[test]
    fn classifier_assigns_braking_from_acceleration_tests() {
        let classifier = OpModeClassifier::new(&[vsp_bracket(13, None, None)]);
        // At0 <= -2 alone is braking.
        assert_eq!(classifier.classify(&second(30.0, 5.0, -2.0, 0.0, 0.0)), 0);
        // So is sustained mild deceleration across all three windows.
        assert_eq!(classifier.classify(&second(30.0, 5.0, -1.5, -1.5, -1.5)), 0);
        // Mild deceleration in only one window is *not* braking.
        assert_ne!(classifier.classify(&second(30.0, 5.0, -1.5, 0.0, -1.5)), 0);
    }

    #[test]
    fn classifier_scans_brackets_in_op_mode_order() {
        // Two brackets both contain VSP 5.0; the lower opModeID wins.
        let classifier = OpModeClassifier::new(&[
            vsp_bracket(35, Some(0.0), Some(10.0)),
            vsp_bracket(25, Some(0.0), Some(10.0)),
        ]);
        assert_eq!(classifier.classify(&second(30.0, 5.0, 0.0, 0.0, 0.0)), 25);
    }

    #[test]
    fn classifier_skips_redundant_op_modes_26_and_36() {
        // Modes 26 and 36 are dropped by buildOpModeClause; mode 27 remains.
        let classifier = OpModeClassifier::new(&[
            vsp_bracket(26, Some(0.0), Some(10.0)),
            vsp_bracket(36, Some(0.0), Some(10.0)),
            vsp_bracket(27, Some(0.0), Some(10.0)),
        ]);
        assert_eq!(classifier.classify(&second(30.0, 5.0, 0.0, 0.0, 0.0)), 27);
    }

    #[test]
    fn classifier_treats_null_bounds_as_open() {
        // Only an upper VSP bound: any VSP below it matches.
        let classifier = OpModeClassifier::new(&[vsp_bracket(11, None, Some(0.0))]);
        assert_eq!(classifier.classify(&second(30.0, -5.0, 0.0, 0.0, 0.0)), 11);
        // VSP at the exclusive upper bound does not match → unassigned.
        assert_eq!(classifier.classify(&second(30.0, 0.0, 0.0, 0.0, 0.0)), -1);
    }

    #[test]
    fn classifier_respects_speed_bounds() {
        let bracket = OperatingModeBracket {
            op_mode_id: 33,
            vsp_lower: None,
            vsp_upper: None,
            speed_lower: Some(25.0),
            speed_upper: Some(50.0),
        };
        let classifier = OpModeClassifier::new(&[bracket]);
        assert_eq!(classifier.classify(&second(30.0, 5.0, 0.0, 0.0, 0.0)), 33);
        // 50 mph is the exclusive upper bound — no match.
        assert_eq!(classifier.classify(&second(50.0, 5.0, 0.0, 0.0, 0.0)), -1);
    }

    #[test]
    fn classifier_returns_unassigned_when_no_bracket_matches() {
        let classifier = OpModeClassifier::new(&[vsp_bracket(13, Some(100.0), None)]);
        assert_eq!(classifier.classify(&second(30.0, 5.0, 0.0, 0.0, 0.0)), -1);
    }

    #[test]
    fn second_physics_first_second_has_zero_acceleration() {
        // A single second has no predecessor, so all three accelerations
        // and the inertial VSP term are zero.
        let schedule = [DriveScheduleSecond {
            second_id: 1,
            speed: 10.0,
            grade: 0.0,
        }];
        let rows = second_physics(&schedule, &physics(21, 21));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].accel_t0, 0.0);
        assert_eq!(rows[0].accel_t1, 0.0);
        assert_eq!(rows[0].accel_t2, 0.0);
    }

    #[test]
    fn second_physics_vsp_matches_hand_computed_value() {
        // speed 100 mph, flat grade, road-load = rollingTermA only.
        //   va    = 100 * 0.44704 = 44.704
        //   VSP   = (va * 1.0 + 0 + 0) / 2.0 = 22.352
        let schedule = [DriveScheduleSecond {
            second_id: 1,
            speed: 100.0,
            grade: 0.0,
        }];
        let rows = second_physics(&schedule, &physics(21, 21));
        assert!((rows[0].vsp - 22.352).abs() < 1e-9, "vsp = {}", rows[0].vsp);
    }

    #[test]
    fn second_physics_acceleration_uses_speed_delta_and_grade() {
        // Flat grade: At0 of the second second is the plain speed delta.
        let flat = [
            DriveScheduleSecond {
                second_id: 1,
                speed: 10.0,
                grade: 0.0,
            },
            DriveScheduleSecond {
                second_id: 2,
                speed: 15.0,
                grade: 0.0,
            },
        ];
        let rows = second_physics(&flat, &physics(21, 21));
        assert!((rows[1].accel_t0 - 5.0).abs() < 1e-9);

        // With grade and no speed change, At0 is purely the gravity term
        // 9.81/0.44704 * sin(atan(grade/100)).
        let graded = [
            DriveScheduleSecond {
                second_id: 1,
                speed: 10.0,
                grade: 0.0,
            },
            DriveScheduleSecond {
                second_id: 2,
                speed: 10.0,
                grade: 100.0,
            },
        ];
        let rows = second_physics(&graded, &physics(21, 21));
        let expected = GRAVITY / MPS_PER_MPH * (1.0_f64).atan().sin();
        assert!((rows[1].accel_t0 - expected).abs() < 1e-9);
    }

    #[test]
    fn second_physics_acceleration_chain_uses_prior_seconds() {
        // A four-second ramp: At0/At1/At2 of second 4 read the deltas of
        // [3,4], [2,3] and [1,2] respectively.
        let schedule = [
            DriveScheduleSecond {
                second_id: 1,
                speed: 0.0,
                grade: 0.0,
            },
            DriveScheduleSecond {
                second_id: 2,
                speed: 10.0,
                grade: 0.0,
            },
            DriveScheduleSecond {
                second_id: 3,
                speed: 30.0,
                grade: 0.0,
            },
            DriveScheduleSecond {
                second_id: 4,
                speed: 60.0,
                grade: 0.0,
            },
        ];
        let rows = second_physics(&schedule, &physics(21, 21));
        let fourth = &rows[3];
        assert!((fourth.accel_t0 - 30.0).abs() < 1e-9); // 60 - 30
        assert!((fourth.accel_t1 - 20.0).abs() < 1e-9); // 30 - 10
        assert!((fourth.accel_t2 - 10.0).abs() < 1e-9); // 10 - 0
    }

    #[test]
    fn op_mode_fractions_sum_to_one_per_source_type() {
        // Two seconds at distinct speeds → two op modes, fractions 1/2 each.
        let schedule = [
            DriveScheduleSecond {
                second_id: 1,
                speed: 0.0,
                grade: 0.0,
            },
            DriveScheduleSecond {
                second_id: 2,
                speed: 30.0,
                grade: 0.0,
            },
        ];
        let classifier = OpModeClassifier::new(&[vsp_bracket(13, None, None)]);
        let fractions = op_mode_fractions_from_schedule(
            &schedule,
            &[physics(21, 21)],
            &[SourceTypeId(21)],
            &classifier,
        );
        let total: f64 = fractions.iter().map(|f| f.op_mode_fraction).sum();
        assert!((total - 1.0).abs() < 1e-12);
        assert!(fractions
            .iter()
            .all(|f| (f.op_mode_fraction - 0.5).abs() < 1e-12));
    }

    #[test]
    fn op_mode_fractions_filter_by_run_spec_source_type() {
        let schedule = [DriveScheduleSecond {
            second_id: 1,
            speed: 0.0,
            grade: 0.0,
        }];
        let classifier = OpModeClassifier::new(&[vsp_bracket(13, None, None)]);
        // Physics for source types 21 and 32; only 21 is in the RunSpec.
        let fractions = op_mode_fractions_from_schedule(
            &schedule,
            &[physics(21, 21), physics(32, 32)],
            &[SourceTypeId(21)],
            &classifier,
        );
        let source_types: Vec<SourceTypeId> = fractions.iter().map(|f| f.source_type_id).collect();
        assert_eq!(source_types, vec![SourceTypeId(21)]);
    }

    #[test]
    fn op_mode_fractions_keyed_by_temp_source_type() {
        // realSourceTypeID 21 maps to tempSourceTypeID 2100 — output is
        // keyed by the temp id, while the RunSpec filter uses the real id.
        let schedule = [DriveScheduleSecond {
            second_id: 1,
            speed: 0.0,
            grade: 0.0,
        }];
        let classifier = OpModeClassifier::new(&[vsp_bracket(13, None, None)]);
        let fractions = op_mode_fractions_from_schedule(
            &schedule,
            &[physics(21, 2100)],
            &[SourceTypeId(21)],
            &classifier,
        );
        assert_eq!(fractions.len(), 1);
        assert_eq!(fractions[0].source_type_id, SourceTypeId(2100));
    }

    #[test]
    fn expand_folds_stopped_mode_to_idle_except_stopped_polprocess() {
        // A pure stopped-mode fraction, associated with two polProcesses:
        // the stopped polProcess keeps op-mode 501, the other folds to 1.
        let fractions = [OpModeFraction {
            source_type_id: SourceTypeId(21),
            op_mode_id: 501,
            op_mode_fraction: 1.0,
        }];
        let assoc = [
            OpModePolProcAssoc {
                pol_process_id: STOPPED_POLPROCESS,
                op_mode_id: 501,
            },
            OpModePolProcAssoc {
                pol_process_id: PolProcessId(101),
                op_mode_id: 501,
            },
        ];
        let rows = expand_to_op_mode_distribution(7, &fractions, &assoc, &[51], &HashSet::new());
        let modes: HashMap<PolProcessId, i16> = rows
            .iter()
            .map(|r| (r.pol_process_id, r.op_mode_id))
            .collect();
        assert_eq!(modes[&STOPPED_POLPROCESS], 501);
        assert_eq!(modes[&PolProcessId(101)], 1);
    }

    #[test]
    fn expand_crosses_with_run_spec_hour_day() {
        let fractions = [OpModeFraction {
            source_type_id: SourceTypeId(21),
            op_mode_id: 13,
            op_mode_fraction: 1.0,
        }];
        let assoc = [OpModePolProcAssoc {
            pol_process_id: PolProcessId(101),
            op_mode_id: 13,
        }];
        let rows =
            expand_to_op_mode_distribution(7, &fractions, &assoc, &[51, 52, 53], &HashSet::new());
        // One fraction × one polProcess × three hour/days = three rows.
        assert_eq!(rows.len(), 3);
        let hour_days: Vec<i16> = rows.iter().map(|r| r.hour_day_id).collect();
        assert_eq!(hour_days, vec![51, 52, 53]);
    }

    #[test]
    fn expand_excludes_existing_op_mode_keys() {
        let fractions = [OpModeFraction {
            source_type_id: SourceTypeId(21),
            op_mode_id: 13,
            op_mode_fraction: 1.0,
        }];
        let assoc = [OpModePolProcAssoc {
            pol_process_id: PolProcessId(101),
            op_mode_id: 13,
        }];
        // The (sourceType, polProcess, link) triple already has a
        // user-supplied distribution — the anti-join drops it.
        let existing: HashSet<_> = [(SourceTypeId(21), PolProcessId(101), 7)]
            .into_iter()
            .collect();
        let rows = expand_to_op_mode_distribution(7, &fractions, &assoc, &[51], &existing);
        assert!(rows.is_empty());
    }

    #[test]
    fn expand_drops_op_modes_absent_from_pol_proc_assoc() {
        // An unassigned (-1) fraction joins nothing in opModePolProcAssoc.
        let fractions = [OpModeFraction {
            source_type_id: SourceTypeId(21),
            op_mode_id: UNASSIGNED_OP_MODE,
            op_mode_fraction: 1.0,
        }];
        let assoc = [OpModePolProcAssoc {
            pol_process_id: PolProcessId(101),
            op_mode_id: 13,
        }];
        let rows = expand_to_op_mode_distribution(7, &fractions, &assoc, &[51], &HashSet::new());
        assert!(rows.is_empty());
    }

    #[test]
    fn expand_sums_colliding_folded_fractions() {
        // A stopped-mode fraction folds to op-mode 1 and collides with a
        // native op-mode-1 fraction on the same key — the two sum.
        let fractions = [
            OpModeFraction {
                source_type_id: SourceTypeId(21),
                op_mode_id: 501,
                op_mode_fraction: 0.3,
            },
            OpModeFraction {
                source_type_id: SourceTypeId(21),
                op_mode_id: 1,
                op_mode_fraction: 0.7,
            },
        ];
        let assoc = [
            OpModePolProcAssoc {
                pol_process_id: PolProcessId(101),
                op_mode_id: 501,
            },
            OpModePolProcAssoc {
                pol_process_id: PolProcessId(101),
                op_mode_id: 1,
            },
        ];
        let rows = expand_to_op_mode_distribution(7, &fractions, &assoc, &[51], &HashSet::new());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].op_mode_id, 1);
        assert!((rows[0].op_mode_fraction - 1.0).abs() < 1e-12);
    }

    #[test]
    fn default_idle_schedule_is_thirty_idle_seconds() {
        let schedule = default_idle_drive_schedule();
        assert_eq!(schedule.len(), 30);
        assert_eq!(schedule[0].second_id, 1);
        assert_eq!(schedule[29].second_id, 30);
        assert!(schedule.iter().all(|s| s.speed == 0.0 && s.grade == 0.0));
    }

    /// Shared classifier table for the orchestration tests.
    fn op_mode_table() -> Vec<OperatingModeBracket> {
        vec![vsp_bracket(13, None, None)]
    }

    #[test]
    fn op_mode_distribution_from_drive_schedule() {
        let schedule = [
            DriveScheduleSecond {
                second_id: 1,
                speed: 0.0,
                grade: 0.0,
            },
            DriveScheduleSecond {
                second_id: 2,
                speed: 30.0,
                grade: 0.0,
            },
        ];
        let operating_mode = op_mode_table();
        let physics_rows = [physics(21, 21)];
        let assoc = [
            OpModePolProcAssoc {
                pol_process_id: PolProcessId(101),
                op_mode_id: 501,
            },
            OpModePolProcAssoc {
                pol_process_id: PolProcessId(101),
                op_mode_id: 13,
            },
        ];
        let existing = HashSet::new();
        let inputs = LinkDriveScheduleInputs {
            link_id: 7,
            drive_schedule: &schedule,
            link_avg_speed: None,
            has_running_op_mode_distribution: false,
            operating_mode: &operating_mode,
            physics: &physics_rows,
            run_spec_source_type: &[SourceTypeId(21)],
            op_mode_pol_proc_assoc: &assoc,
            run_spec_hour_day: &[51],
            existing_op_mode_keys: &existing,
        };
        let gen = LinkOperatingModeDistributionGenerator::new();
        let rows = gen.op_mode_distribution(&inputs);
        // Second 1 (idle) folds 501 → op-mode 1; second 2 → op-mode 13.
        let modes: HashSet<i16> = rows.iter().map(|r| r.op_mode_id).collect();
        assert_eq!(modes, HashSet::from([1, 13]));
        let total: f64 = rows.iter().map(|r| r.op_mode_fraction).sum();
        assert!((total - 1.0).abs() < 1e-12);
    }

    #[test]
    fn op_mode_distribution_synthesises_idle_schedule_for_nonpositive_avg_speed() {
        // No drive schedule and linkAvgSpeed <= 0 → 30 idle seconds → the
        // whole distribution is op-mode 1 (idle).
        let operating_mode = op_mode_table();
        let physics_rows = [physics(21, 21)];
        let assoc = [OpModePolProcAssoc {
            pol_process_id: PolProcessId(101),
            op_mode_id: 501,
        }];
        let existing = HashSet::new();
        let inputs = LinkDriveScheduleInputs {
            link_id: 7,
            drive_schedule: &[],
            link_avg_speed: Some(0.0),
            has_running_op_mode_distribution: false,
            operating_mode: &operating_mode,
            physics: &physics_rows,
            run_spec_source_type: &[SourceTypeId(21)],
            op_mode_pol_proc_assoc: &assoc,
            run_spec_hour_day: &[51],
            existing_op_mode_keys: &existing,
        };
        let gen = LinkOperatingModeDistributionGenerator::new();
        let rows = gen.op_mode_distribution(&inputs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].op_mode_id, 1);
        assert!((rows[0].op_mode_fraction - 1.0).abs() < 1e-12);
    }

    #[test]
    fn op_mode_distribution_empty_for_link_with_input_distribution() {
        let operating_mode = op_mode_table();
        let existing = HashSet::new();
        let inputs = LinkDriveScheduleInputs {
            link_id: 7,
            drive_schedule: &[],
            link_avg_speed: Some(35.0),
            has_running_op_mode_distribution: true,
            operating_mode: &operating_mode,
            physics: &[],
            run_spec_source_type: &[],
            op_mode_pol_proc_assoc: &[],
            run_spec_hour_day: &[51],
            existing_op_mode_keys: &existing,
        };
        let gen = LinkOperatingModeDistributionGenerator::new();
        assert!(gen.op_mode_distribution(&inputs).is_empty());
    }

    #[test]
    fn op_mode_distribution_empty_for_interpolation_path() {
        // No drive schedule, positive average speed: the interpolation path
        // is out of scope and yields no rows from this module.
        let operating_mode = op_mode_table();
        let existing = HashSet::new();
        let inputs = LinkDriveScheduleInputs {
            link_id: 7,
            drive_schedule: &[],
            link_avg_speed: Some(35.0),
            has_running_op_mode_distribution: false,
            operating_mode: &operating_mode,
            physics: &[],
            run_spec_source_type: &[],
            op_mode_pol_proc_assoc: &[],
            run_spec_hour_day: &[51],
            existing_op_mode_keys: &existing,
        };
        let gen = LinkOperatingModeDistributionGenerator::new();
        assert!(gen.op_mode_distribution(&inputs).is_empty());
    }

    #[test]
    fn generator_metadata_matches_subscribe_to_me() {
        let gen = LinkOperatingModeDistributionGenerator::new();
        assert_eq!(gen.name(), "LinkOperatingModeDistributionGenerator");
        assert_eq!(gen.output_tables(), &["OpModeDistribution"]);
        assert_eq!(gen.upstream(), &["SourceTypePhysics"]);
        assert!(gen.input_tables().contains(&"driveScheduleSecondLink"));

        let subs = gen.subscriptions();
        assert_eq!(subs.len(), 2);
        let processes: Vec<ProcessId> = subs.iter().map(|s| s.process_id).collect();
        assert_eq!(processes, vec![ProcessId(1), ProcessId(9)]);
        for s in subs {
            assert_eq!(s.granularity, Granularity::Link);
            assert_eq!(s.priority.display(), "GENERATOR+1");
        }
    }

    #[test]
    fn generator_execute_returns_placeholder_until_data_plane() {
        // execute is a documented placeholder until Task 50; it must still
        // honour the trait contract and return Ok.
        let gen = LinkOperatingModeDistributionGenerator::new();
        let ctx = CalculatorContext::new();
        assert!(gen.execute(&ctx).is_ok());
    }

    #[test]
    fn generator_is_object_safe() {
        // The registry stores generators as Box<dyn Generator>.
        let gen: Box<dyn Generator> = Box::new(LinkOperatingModeDistributionGenerator::new());
        assert_eq!(gen.name(), "LinkOperatingModeDistributionGenerator");
    }
}

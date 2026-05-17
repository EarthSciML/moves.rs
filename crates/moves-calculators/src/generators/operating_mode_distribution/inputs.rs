//! Projected default-database tables the [`OperatingModeDistributionGenerator`]
//! pipeline reads.
//!
//! [`OperatingModeDistributionGenerator`]: super::OperatingModeDistributionGenerator
//!
//! Each struct is the Rust analogue of one MySQL table referenced by the
//! `SELECT` statements in `OperatingModeDistributionGenerator.java`. Once the
//! Task 50 data plane lands, [`Generator::execute`] builds an [`OmdgInputs`]
//! view from `ctx.tables()`; until then the pipeline functions in
//! [`super::pipeline`] take an [`OmdgInputs`] directly so the numerically
//! faithful algorithm can be exercised by unit tests.
//!
//! [`Generator::execute`]: moves_framework::Generator::execute
//!
//! Identifiers that have a `moves-data` newtype ([`SourceTypeId`],
//! [`RoadTypeId`], [`PolProcessId`]) use it; the remaining MOVES `SMALLINT`
//! identifiers — `driveScheduleID`, `avgSpeedBinID`, `hourDayID`, `opModeID`,
//! `second` — have no newtype and are held as [`i16`], the width of the
//! source column.

use moves_data::{PolProcessId, RoadTypeId, SourceTypeId};

/// One `DriveSchedule` row — a named driving cycle and its average speed.
///
/// Java: `DriveSchedule (driveScheduleID, averageSpeed)`. `average_speed` is
/// in miles per hour, the unit the bracketing arithmetic in
/// [`bracket_average_speed_bins`](super::pipeline::bracket_average_speed_bins)
/// compares against `avgSpeedBin.avgBinSpeed`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DriveScheduleRow {
    /// `driveScheduleID`.
    pub drive_schedule_id: i16,
    /// `averageSpeed` — the cycle's mean speed, mph.
    pub average_speed: f64,
}

/// One `DriveScheduleAssoc` row — which driving cycles apply to a
/// `(sourceType, roadType)` combination.
///
/// Java: `DriveScheduleAssoc (sourceTypeID, roadTypeID, driveScheduleID,
/// isRamp)`. `is_ramp` distinguishes ramp cycles from non-ramp cycles; it is
/// read only by [`validate_drive_schedule_distribution`].
///
/// [`validate_drive_schedule_distribution`]: super::pipeline::validate_drive_schedule_distribution
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DriveScheduleAssocRow {
    /// `sourceTypeID`.
    pub source_type_id: SourceTypeId,
    /// `roadTypeID`.
    pub road_type_id: RoadTypeId,
    /// `driveScheduleID`.
    pub drive_schedule_id: i16,
    /// `isRamp = 'Y'` — a ramp driving cycle.
    pub is_ramp: bool,
}

/// One `DriveScheduleSecond` row — the speed trace of a driving cycle.
///
/// Java: `DriveScheduleSecond (driveScheduleID, second, speed)`. `speed` is in
/// miles per hour; the VSP step converts it to metres per second.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DriveScheduleSecondRow {
    /// `driveScheduleID`.
    pub drive_schedule_id: i16,
    /// `second` — the elapsed-time index within the cycle.
    pub second: i16,
    /// `speed` — instantaneous speed at this second, mph.
    pub speed: f64,
}

/// One `AvgSpeedBin` row — the nominal speed of an average-speed bin.
///
/// Java: `AvgSpeedBin (avgSpeedBinID, avgBinSpeed)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AvgSpeedBinRow {
    /// `avgSpeedBinID`.
    pub avg_speed_bin_id: i16,
    /// `avgBinSpeed` — the bin's nominal average speed, mph.
    pub avg_bin_speed: f64,
}

/// One `AvgSpeedDistribution` row — the fraction of operating time a
/// `(sourceType, roadType, hourDay)` spends in an average-speed bin.
///
/// Java: `AvgSpeedDistribution (sourceTypeID, roadTypeID, hourDayID,
/// avgSpeedBinID, avgSpeedFraction)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AvgSpeedDistributionRow {
    /// `sourceTypeID`.
    pub source_type_id: SourceTypeId,
    /// `roadTypeID`.
    pub road_type_id: RoadTypeId,
    /// `hourDayID`.
    pub hour_day_id: i16,
    /// `avgSpeedBinID`.
    pub avg_speed_bin_id: i16,
    /// `avgSpeedFraction` — fraction of time in this bin (the bin fractions
    /// for one `(sourceType, roadType, hourDay)` sum to 1).
    pub avg_speed_fraction: f64,
}

/// One `OperatingMode` row — the VSP / speed bounds that define an operating
/// mode.
///
/// Java: `OperatingMode (opModeID, VSPLower, VSPUpper, speedLower,
/// speedUpper)`. Each bound is nullable; `None` mirrors a SQL `NULL` and means
/// "this bound does not constrain the mode" — matching the Java's
/// `result.wasNull()` handling, which omits the corresponding `WHERE` clause.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OperatingModeRow {
    /// `opModeID`. The op-mode-binning step considers only `1 < opModeID < 100`.
    pub op_mode_id: i16,
    /// `VSPLower` — inclusive lower vehicle-specific-power bound, or `None`.
    pub vsp_lower: Option<f64>,
    /// `VSPUpper` — exclusive upper vehicle-specific-power bound, or `None`.
    pub vsp_upper: Option<f64>,
    /// `speedLower` — inclusive lower speed bound (mph), or `None`.
    pub speed_lower: Option<f64>,
    /// `speedUpper` — exclusive upper speed bound (mph), or `None`.
    pub speed_upper: Option<f64>,
}

/// One `OpModePolProcAssoc` row — an operating mode associated with a
/// pollutant/process.
///
/// Java: `OpModePolProcAssoc (polProcessID, opModeID)`. The distinct
/// `polProcessID`s drive the per-second op-mode calculation; the
/// `(polProcessID, opModeID)` pairs gate which modes survive into the final
/// distribution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpModePolProcAssocRow {
    /// `polProcessID`.
    pub pol_process_id: PolProcessId,
    /// `opModeID`.
    pub op_mode_id: i16,
}

/// One `sourceUseTypePhysicsMapping` row — the road-load polynomial terms a
/// source type carries into the VSP calculation.
///
/// Built by `SourceTypePhysics` (migration-plan Task 37) and consumed here as
/// an input. `real_source_type_id` is the traditional source type;
/// `temp_source_type_id` is a temporary source type carved out for a
/// model-year-range / regulatory-class split (equal to `real_source_type_id`
/// for the identity mapping every source type always receives).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PhysicsMappingRow {
    /// `realSourceTypeID`.
    pub real_source_type_id: SourceTypeId,
    /// `tempSourceTypeID` — unique per row.
    pub temp_source_type_id: SourceTypeId,
    /// `rollingTermA` — rolling-resistance term of the road-load polynomial.
    pub rolling_term_a: f64,
    /// `rotatingTermB` — rotating-mass term of the road-load polynomial.
    pub rotating_term_b: f64,
    /// `dragTermC` — aerodynamic-drag term of the road-load polynomial.
    pub drag_term_c: f64,
    /// `sourceMass` — vehicle mass used in the VSP calculation. The Java VSP
    /// `SELECT` joins `sourceMass <> 0`, so a zero-mass mapping contributes
    /// no VSP rows.
    pub source_mass: f64,
    /// `fixedMassFactor` — the VSP denominator.
    pub fixed_mass_factor: f64,
}

/// One `OMDGPolProcessRepresented` row — a represented pollutant/process and
/// the one whose operating-mode distribution stands in for it.
///
/// Java: `OMDGPolProcessRepresented (polProcessID, representingPolProcessID)`.
/// The generator computes a distribution only for the representing
/// pollutant/process and copies it onto the represented one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PolProcessRepresentedRow {
    /// `polProcessID` — the represented pollutant/process.
    pub pol_process_id: PolProcessId,
    /// `representingPolProcessID` — whose distribution is copied onto it.
    pub representing_pol_process_id: PolProcessId,
}

/// The projected default-database tables the OMDG pipeline reads.
///
/// Each field is a borrowed slice of one input table (see the per-struct
/// docs). The three `run_spec_*` fields are the RunSpec's active selections:
/// `run_spec_hour_day` is the set of `hourDayID`s the Java derives by joining
/// `RunSpecHour`, `RunSpecDay` and `HourDay`.
#[derive(Debug, Clone, Copy)]
pub struct OmdgInputs<'a> {
    /// `DriveSchedule` — driving cycles and their average speeds.
    pub drive_schedule: &'a [DriveScheduleRow],
    /// `DriveScheduleAssoc` — driving cycles per `(sourceType, roadType)`.
    pub drive_schedule_assoc: &'a [DriveScheduleAssocRow],
    /// `DriveScheduleSecond` — the per-second speed trace of each cycle.
    pub drive_schedule_second: &'a [DriveScheduleSecondRow],
    /// `AvgSpeedBin` — average-speed bins and their nominal speeds.
    pub avg_speed_bin: &'a [AvgSpeedBinRow],
    /// `AvgSpeedDistribution` — time fraction per average-speed bin.
    pub avg_speed_distribution: &'a [AvgSpeedDistributionRow],
    /// `OperatingMode` — the VSP / speed bounds of each operating mode.
    pub operating_mode: &'a [OperatingModeRow],
    /// `OpModePolProcAssoc` — operating modes per pollutant/process.
    pub op_mode_pol_proc_assoc: &'a [OpModePolProcAssocRow],
    /// `sourceUseTypePhysicsMapping` — road-load polynomial terms.
    pub physics_mapping: &'a [PhysicsMappingRow],
    /// `OMDGPolProcessRepresented` — represented-pollutant/process mappings.
    pub pol_process_represented: &'a [PolProcessRepresentedRow],
    /// `RunSpecSourceType.sourceTypeID` — the RunSpec's selected source types.
    pub run_spec_source_type: &'a [SourceTypeId],
    /// `RunSpecRoadType.roadTypeID` — the RunSpec's selected road types.
    pub run_spec_road_type: &'a [RoadTypeId],
    /// The RunSpec's selected `hourDayID`s (`RunSpecHour` × `RunSpecDay`
    /// joined through `HourDay`).
    pub run_spec_hour_day: &'a [i16],
}

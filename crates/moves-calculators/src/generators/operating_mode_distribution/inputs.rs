//! Projected default-database tables the [`OperatingModeDistributionGenerator`]
//! pipeline reads.
//!
//! [`OperatingModeDistributionGenerator`]: super::OperatingModeDistributionGenerator
//!
//! Each struct is the Rust analogue of one MySQL table referenced by the
//! `SELECT` statements in `OperatingModeDistributionGenerator.java`. Once the
//! data plane lands, [`Generator::execute`] builds an [`OmdgInputs`]
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
use moves_framework::{Error, TableRow};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

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
/// Built by `SourceTypePhysics` () and consumed here as
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

// ============================================================================
// Data-plane wiring — TableRow impls for all input row types.
// ============================================================================

/// Build a [`Error::RowExtraction`] for a missing/bad cell.
fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::RowExtraction {
        table: table.into(),
        row,
        column: column.into(),
        message: msg,
    }
}

impl TableRow for DriveScheduleRow {
    fn table_name() -> &'static str {
        "DriveSchedule"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("driveScheduleID".into(), DataType::Int32),
            ("averageSpeed".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "driveScheduleID".into(),
                    rows.iter()
                        .map(|r| r.drive_schedule_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "averageSpeed".into(),
                    rows.iter().map(|r| r.average_speed).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "DriveSchedule";
        let drive_schedule_id = df
            .column("driveScheduleID")
            .map_err(|e| row_err(t, 0, "driveScheduleID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "driveScheduleID", e.to_string()))?;
        let average_speed = df
            .column("averageSpeed")
            .map_err(|e| row_err(t, 0, "averageSpeed", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "averageSpeed", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(DriveScheduleRow {
                    drive_schedule_id: drive_schedule_id
                        .get(i)
                        .ok_or_else(|| null("driveScheduleID"))?
                        as i16,
                    average_speed: average_speed.get(i).ok_or_else(|| null("averageSpeed"))?,
                })
            })
            .collect()
    }
}

impl TableRow for DriveScheduleAssocRow {
    fn table_name() -> &'static str {
        "DriveScheduleAssoc"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("driveScheduleID".into(), DataType::Int32),
            ("isRamp".into(), DataType::Boolean),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter()
                        .map(|r| r.source_type_id.0 as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeID".into(),
                    rows.iter()
                        .map(|r| r.road_type_id.0 as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "driveScheduleID".into(),
                    rows.iter()
                        .map(|r| r.drive_schedule_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "isRamp".into(),
                    rows.iter().map(|r| r.is_ramp).collect::<Vec<bool>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "DriveScheduleAssoc";
        let source_type_id = df
            .column("sourceTypeID")
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?;
        let road_type_id = df
            .column("roadTypeID")
            .map_err(|e| row_err(t, 0, "roadTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "roadTypeID", e.to_string()))?;
        let drive_schedule_id = df
            .column("driveScheduleID")
            .map_err(|e| row_err(t, 0, "driveScheduleID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "driveScheduleID", e.to_string()))?;
 // `isRamp` defaults to 'N' (non-ramp) in MOVES, and canonical's
 // `DriveScheduleAssoc` table declares only sourceTypeID/roadTypeID/
 // driveScheduleID (CreateDefault.sql:393-397) — it carries NO `isRamp`
 // column, so the captured snapshots legitimately omit it. Treat an absent
 // column — and any NULL within it — as non-ramp, matching the MOVES default
 // rather than erroring (the audit's strict variant demanded a column
 // canonical's table does not have, breaking previously-correct runs).
        let is_ramp = df
            .column("isRamp")
            .ok()
            .and_then(|c| c.bool().ok().cloned());
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(DriveScheduleAssocRow {
                    source_type_id: SourceTypeId(
                        source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))? as u16,
                    ),
                    road_type_id: RoadTypeId(
                        road_type_id.get(i).ok_or_else(|| null("roadTypeID"))? as u16,
                    ),
                    drive_schedule_id: drive_schedule_id
                        .get(i)
                        .ok_or_else(|| null("driveScheduleID"))?
                        as i16,
                    is_ramp: is_ramp.as_ref().and_then(|c| c.get(i)).unwrap_or(false),
                })
            })
            .collect()
    }
}

impl TableRow for DriveScheduleSecondRow {
    fn table_name() -> &'static str {
        "DriveScheduleSecond"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("driveScheduleID".into(), DataType::Int32),
            ("second".into(), DataType::Int32),
            ("speed".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "driveScheduleID".into(),
                    rows.iter()
                        .map(|r| r.drive_schedule_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "second".into(),
                    rows.iter().map(|r| r.second as i32).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "speed".into(),
                    rows.iter().map(|r| r.speed).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "DriveScheduleSecond";
        let drive_schedule_id = df
            .column("driveScheduleID")
            .map_err(|e| row_err(t, 0, "driveScheduleID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "driveScheduleID", e.to_string()))?;
        let second = df
            .column("second")
            .map_err(|e| row_err(t, 0, "second", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "second", e.to_string()))?;
        let speed = df
            .column("speed")
            .map_err(|e| row_err(t, 0, "speed", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "speed", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(DriveScheduleSecondRow {
                    drive_schedule_id: drive_schedule_id
                        .get(i)
                        .ok_or_else(|| null("driveScheduleID"))?
                        as i16,
                    second: second.get(i).ok_or_else(|| null("second"))? as i16,
                    speed: speed.get(i).ok_or_else(|| null("speed"))?,
                })
            })
            .collect()
    }
}

impl TableRow for AvgSpeedBinRow {
    fn table_name() -> &'static str {
        "AvgSpeedBin"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("avgSpeedBinID".into(), DataType::Int32),
            ("avgBinSpeed".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "avgSpeedBinID".into(),
                    rows.iter()
                        .map(|r| r.avg_speed_bin_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "avgBinSpeed".into(),
                    rows.iter().map(|r| r.avg_bin_speed).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "AvgSpeedBin";
        let avg_speed_bin_id = df
            .column("avgSpeedBinID")
            .map_err(|e| row_err(t, 0, "avgSpeedBinID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "avgSpeedBinID", e.to_string()))?;
        let avg_bin_speed = df
            .column("avgBinSpeed")
            .map_err(|e| row_err(t, 0, "avgBinSpeed", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "avgBinSpeed", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(AvgSpeedBinRow {
                    avg_speed_bin_id: avg_speed_bin_id
                        .get(i)
                        .ok_or_else(|| null("avgSpeedBinID"))?
                        as i16,
                    avg_bin_speed: avg_bin_speed.get(i).ok_or_else(|| null("avgBinSpeed"))?,
                })
            })
            .collect()
    }
}

impl TableRow for AvgSpeedDistributionRow {
    fn table_name() -> &'static str {
        "AvgSpeedDistribution"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("hourDayID".into(), DataType::Int32),
            ("avgSpeedBinID".into(), DataType::Int32),
            ("avgSpeedFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter()
                        .map(|r| r.source_type_id.0 as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeID".into(),
                    rows.iter()
                        .map(|r| r.road_type_id.0 as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourDayID".into(),
                    rows.iter()
                        .map(|r| r.hour_day_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "avgSpeedBinID".into(),
                    rows.iter()
                        .map(|r| r.avg_speed_bin_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "avgSpeedFraction".into(),
                    rows.iter()
                        .map(|r| r.avg_speed_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "AvgSpeedDistribution";
        let source_type_id = df
            .column("sourceTypeID")
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?;
        let road_type_id = df
            .column("roadTypeID")
            .map_err(|e| row_err(t, 0, "roadTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "roadTypeID", e.to_string()))?;
        let hour_day_id = df
            .column("hourDayID")
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?;
        let avg_speed_bin_id = df
            .column("avgSpeedBinID")
            .map_err(|e| row_err(t, 0, "avgSpeedBinID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "avgSpeedBinID", e.to_string()))?;
        let avg_speed_fraction = df
            .column("avgSpeedFraction")
            .map_err(|e| row_err(t, 0, "avgSpeedFraction", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "avgSpeedFraction", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(AvgSpeedDistributionRow {
                    source_type_id: SourceTypeId(
                        source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))? as u16,
                    ),
                    road_type_id: RoadTypeId(
                        road_type_id.get(i).ok_or_else(|| null("roadTypeID"))? as u16,
                    ),
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))? as i16,
                    avg_speed_bin_id: avg_speed_bin_id
                        .get(i)
                        .ok_or_else(|| null("avgSpeedBinID"))?
                        as i16,
                    avg_speed_fraction: avg_speed_fraction
                        .get(i)
                        .ok_or_else(|| null("avgSpeedFraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for OperatingModeRow {
    fn table_name() -> &'static str {
        "OperatingMode"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("opModeID".into(), DataType::Int32),
            ("VSPLower".into(), DataType::Float64),
            ("VSPUpper".into(), DataType::Float64),
            ("speedLower".into(), DataType::Float64),
            ("speedUpper".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "opModeID".into(),
                    rows.iter()
                        .map(|r| r.op_mode_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "VSPLower".into(),
                    rows.iter()
                        .map(|r| r.vsp_lower)
                        .collect::<Vec<Option<f64>>>(),
                )
                .into(),
                Series::new(
                    "VSPUpper".into(),
                    rows.iter()
                        .map(|r| r.vsp_upper)
                        .collect::<Vec<Option<f64>>>(),
                )
                .into(),
                Series::new(
                    "speedLower".into(),
                    rows.iter()
                        .map(|r| r.speed_lower)
                        .collect::<Vec<Option<f64>>>(),
                )
                .into(),
                Series::new(
                    "speedUpper".into(),
                    rows.iter()
                        .map(|r| r.speed_upper)
                        .collect::<Vec<Option<f64>>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "OperatingMode";
        let op_mode_id = df
            .column("opModeID")
            .map_err(|e| row_err(t, 0, "opModeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "opModeID", e.to_string()))?;
        let vsp_lower = df
            .column("VSPLower")
            .map_err(|e| row_err(t, 0, "VSPLower", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "VSPLower", e.to_string()))?;
        let vsp_upper = df
            .column("VSPUpper")
            .map_err(|e| row_err(t, 0, "VSPUpper", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "VSPUpper", e.to_string()))?;
        let speed_lower = df
            .column("speedLower")
            .map_err(|e| row_err(t, 0, "speedLower", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "speedLower", e.to_string()))?;
        let speed_upper = df
            .column("speedUpper")
            .map_err(|e| row_err(t, 0, "speedUpper", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "speedUpper", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(OperatingModeRow {
                    op_mode_id: op_mode_id.get(i).ok_or_else(|| null("opModeID"))? as i16,
                    vsp_lower: vsp_lower.get(i),
                    vsp_upper: vsp_upper.get(i),
                    speed_lower: speed_lower.get(i),
                    speed_upper: speed_upper.get(i),
                })
            })
            .collect()
    }
}

impl TableRow for OpModePolProcAssocRow {
    fn table_name() -> &'static str {
        "OpModePolProcAssoc"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("opModeID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "polProcessID".into(),
                    rows.iter()
                        .map(|r| r.pol_process_id.0 as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "opModeID".into(),
                    rows.iter()
                        .map(|r| r.op_mode_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "OpModePolProcAssoc";
        let pol_process_id = df
            .column("polProcessID")
            .map_err(|e| row_err(t, 0, "polProcessID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "polProcessID", e.to_string()))?;
        let op_mode_id = df
            .column("opModeID")
            .map_err(|e| row_err(t, 0, "opModeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "opModeID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(OpModePolProcAssocRow {
                    pol_process_id: PolProcessId(
                        pol_process_id.get(i).ok_or_else(|| null("polProcessID"))? as u32,
                    ),
                    op_mode_id: op_mode_id.get(i).ok_or_else(|| null("opModeID"))? as i16,
                })
            })
            .collect()
    }
}

impl TableRow for PhysicsMappingRow {
    fn table_name() -> &'static str {
        "sourceUseTypePhysicsMapping"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("realSourceTypeID".into(), DataType::Int32),
            ("tempSourceTypeID".into(), DataType::Int32),
            ("rollingTermA".into(), DataType::Float64),
            ("rotatingTermB".into(), DataType::Float64),
            ("dragTermC".into(), DataType::Float64),
            ("sourceMass".into(), DataType::Float64),
            ("fixedMassFactor".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "realSourceTypeID".into(),
                    rows.iter()
                        .map(|r| r.real_source_type_id.0 as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "tempSourceTypeID".into(),
                    rows.iter()
                        .map(|r| r.temp_source_type_id.0 as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "rollingTermA".into(),
                    rows.iter().map(|r| r.rolling_term_a).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "rotatingTermB".into(),
                    rows.iter().map(|r| r.rotating_term_b).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "dragTermC".into(),
                    rows.iter().map(|r| r.drag_term_c).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "sourceMass".into(),
                    rows.iter().map(|r| r.source_mass).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "fixedMassFactor".into(),
                    rows.iter()
                        .map(|r| r.fixed_mass_factor)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "sourceUseTypePhysicsMapping";
        let real_source_type_id = df
            .column("realSourceTypeID")
            .map_err(|e| row_err(t, 0, "realSourceTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "realSourceTypeID", e.to_string()))?;
        let temp_source_type_id = df
            .column("tempSourceTypeID")
            .map_err(|e| row_err(t, 0, "tempSourceTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "tempSourceTypeID", e.to_string()))?;
        let rolling_term_a = df
            .column("rollingTermA")
            .map_err(|e| row_err(t, 0, "rollingTermA", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "rollingTermA", e.to_string()))?;
        let rotating_term_b = df
            .column("rotatingTermB")
            .map_err(|e| row_err(t, 0, "rotatingTermB", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "rotatingTermB", e.to_string()))?;
        let drag_term_c = df
            .column("dragTermC")
            .map_err(|e| row_err(t, 0, "dragTermC", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "dragTermC", e.to_string()))?;
        let source_mass = df
            .column("sourceMass")
            .map_err(|e| row_err(t, 0, "sourceMass", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "sourceMass", e.to_string()))?;
        let fixed_mass_factor = df
            .column("fixedMassFactor")
            .map_err(|e| row_err(t, 0, "fixedMassFactor", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "fixedMassFactor", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(PhysicsMappingRow {
                    real_source_type_id: SourceTypeId(
                        real_source_type_id
                            .get(i)
                            .ok_or_else(|| null("realSourceTypeID"))?
                            as u16,
                    ),
                    temp_source_type_id: SourceTypeId(
                        temp_source_type_id
                            .get(i)
                            .ok_or_else(|| null("tempSourceTypeID"))?
                            as u16,
                    ),
                    rolling_term_a: rolling_term_a.get(i).ok_or_else(|| null("rollingTermA"))?,
                    rotating_term_b: rotating_term_b
                        .get(i)
                        .ok_or_else(|| null("rotatingTermB"))?,
                    drag_term_c: drag_term_c.get(i).ok_or_else(|| null("dragTermC"))?,
                    source_mass: source_mass.get(i).ok_or_else(|| null("sourceMass"))?,
                    fixed_mass_factor: fixed_mass_factor
                        .get(i)
                        .ok_or_else(|| null("fixedMassFactor"))?,
                })
            })
            .collect()
    }
}

impl TableRow for PolProcessRepresentedRow {
    fn table_name() -> &'static str {
        "OMDGPolProcessRepresented"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("representingPolProcessID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "polProcessID".into(),
                    rows.iter()
                        .map(|r| r.pol_process_id.0 as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "representingPolProcessID".into(),
                    rows.iter()
                        .map(|r| r.representing_pol_process_id.0 as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "OMDGPolProcessRepresented";
        let pol_process_id = df
            .column("polProcessID")
            .map_err(|e| row_err(t, 0, "polProcessID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "polProcessID", e.to_string()))?;
        let representing_pol_process_id = df
            .column("representingPolProcessID")
            .map_err(|e| row_err(t, 0, "representingPolProcessID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "representingPolProcessID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(PolProcessRepresentedRow {
                    pol_process_id: PolProcessId(
                        pol_process_id.get(i).ok_or_else(|| null("polProcessID"))? as u32,
                    ),
                    representing_pol_process_id: PolProcessId(
                        representing_pol_process_id
                            .get(i)
                            .ok_or_else(|| null("representingPolProcessID"))?
                            as u32,
                    ),
                })
            })
            .collect()
    }
}

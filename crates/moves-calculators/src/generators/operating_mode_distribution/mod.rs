//! Port of `OperatingModeDistributionGenerator.java`
//! (`gov.epa.otaq.moves.master.implementation.ghg`) — the generator that
//! builds the running-exhaust and brakewear `OpModeDistribution` table.
//!
//! //!
//! # What this generator produces
//!
//! An *operating-mode distribution* is the fraction of a vehicle's operating
//! time spent in each operating mode — idle, the cruise / acceleration VSP
//! bins, braking — broken down by source type, road type, hour/day and
//! pollutant/process. The running-emission calculators multiply an emission
//! *rate per operating mode* by these fractions, so the distribution is the
//! bridge between drive-cycle physics and the inventory.
//!
//! The Java class subscribes to two emission processes at `YEAR` granularity
//! / `GENERATOR` priority — Running Exhaust (process 1) and Brakewear
//! (process 9) — and shares one set of computed fractions across both.
//!
//! # The OMDG pipeline
//!
//! `executeLoop` runs a seven-stage SQL pipeline once per run; the pure
//! computational core is ported in [`pipeline`]:
//!
//! 1. **OMDG-1** ([`bracket_average_speed_bins`]) — bracket each average-speed
//! bin between the two driving cycles whose average speeds straddle it,
//! clamping bins outside the cycle range.
//! 2. **OMDG-2/-3** — split each bin's average-speed-distribution fraction
//! between its bracketing cycles and sum per driving cycle, giving a
//! per-`(sourceType, roadType, hourDay, driveSchedule)` drive-schedule
//! fraction.
//! 3. **OMDG-4** — compute second-by-second vehicle-specific power (VSP) from
//! the road-load polynomial in `sourceUseTypePhysicsMapping`.
//! 4. **OMDG-5** — bin every second into an operating mode from its VSP,
//! speed and acceleration (braking, idle and the VSP/speed bins).
//! 5. **OMDG-6** — divide the per-mode second counts by the total, giving an
//! operating-mode fraction per driving cycle.
//! 6. **OMDG-7** ([`op_mode_distribution`]) — weight the per-cycle fractions
//! by the drive-schedule fractions and sum, then copy each represented
//! pollutant/process its representative's distribution.
//!
//! # Scope of the port
//!
//! [`op_mode_distribution`] ports stages OMDG-1 … OMDG-7 in full and yields
//! every `OpModeFraction2` row — the operating-mode fraction per
//! `(sourceType, roadType, hourDay, polProcess, opMode)`. The Java's final
//! `calculateOpModeFractions` step is a thin data-plane projection: it
//! cross-joins `OpModeFraction2` with `Link` (`roadTypeID → linkID`) to fill
//! the link-keyed `OpModeDistribution` table, skipping link rows a user
//! supplied directly. That projection belongs to [`Generator::execute`] once
//! the data plane lands; see [`OpModeFractionRow`].
//!
//! **Deferred to `SourceTypePhysics` ().** `executeLoop`
//! also calls `modelYearPhysics.setup`, `modelYearPhysics.updateEmissionRateTables`
//! and `modelYearPhysics.updateOperatingModeDistribution`. Those belong to the
//! `SourceTypePhysics` port; this generator *consumes*
//! `sourceUseTypePhysicsMapping` (which `SourceTypePhysics.setup` populates)
//! as an input — the road-load polynomial terms it carries drive the VSP
//! calculation, and its real/temporary source-type rows let one source type
//! split by model-year range. Output rows therefore key on a *temporary*
//! source type wherever such a split applies.
//!
//! # Numeric fidelity
//!
//! The port computes in `f64` throughout, matching the
//! `baserategenerator::drivecycle` sibling. The canonical MariaDB path is
//! coarser: `OpModeFractionBySchedule` and `OpModeFraction2` are `FLOAT`
//! (`f32`) columns, and the OMDG-6 `count(*) / secondSum` is integer division,
//! which MariaDB evaluates as `DECIMAL` rounded to `div_precision_increment`
//! places. The resulting divergence is systematic, bounded and tracked by the
//! generator-validation tolerance budget.
//!
//! # Data plane
//!
//! [`Generator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are Phase-2 placeholders until the
//! `DataFrameStore` lands (), so `execute` cannot yet
//! read the input tables nor write `OpModeDistribution`. The numerically
//! faithful algorithm is fully ported and unit-tested in [`op_mode_distribution`];
//! once the data plane exists, `execute` projects an [`OmdgInputs`] out of
//! `ctx.tables()`, runs [`op_mode_distribution`], cross-joins the result with
//! `Link`, and writes `OpModeDistribution` to the scratch namespace.

pub mod inputs;
pub mod pipeline;

use moves_calculator_info::{Granularity, Priority};
use moves_data::{PolProcessId, ProcessId, RoadTypeId, SourceTypeId};
use moves_framework::{
    CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped, Error,
    Generator, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

pub use inputs::{
    AvgSpeedBinRow, AvgSpeedDistributionRow, DriveScheduleAssocRow, DriveScheduleRow,
    DriveScheduleSecondRow, OmdgInputs, OpModePolProcAssocRow, OperatingModeRow, PhysicsMappingRow,
    PolProcessRepresentedRow,
};
pub use pipeline::{
    bracket_average_speed_bins, op_mode_distribution, validate_drive_schedule_distribution, BinKey,
    BracketBin, OpModeFractionRow,
};

/// Running Exhaust — process id 1. The Java `subscribeToMe` subscribes it
/// (`MasterLoopGranularity.YEAR`, year level for source bins from the
/// `SourceBinDistributionGenerator`).
const RUNNING_EXHAUST: ProcessId = ProcessId(1);
/// Brakewear — process id 9. The Java `subscribeToMe` subscribes it at the
/// same granularity and priority as Running Exhaust.
const BRAKEWEAR: ProcessId = ProcessId(9);

/// `OpModeDistribution` generator for running-exhaust and brakewear runs.
///
/// Ports `OperatingModeDistributionGenerator.java`; see the module
/// documentation for the scope of the port.
#[derive(Debug, Clone)]
pub struct OperatingModeDistributionGenerator {
    /// The two master-loop subscriptions, built once in [`Self::new`].
    subscriptions: [CalculatorSubscription; 2],
}

impl OperatingModeDistributionGenerator {
    /// Chain-DAG name — matches the Java class name.
    pub const NAME: &'static str = "OperatingModeDistributionGenerator";

    /// Construct the generator with its master-loop subscriptions.
    ///
    /// Mirrors `subscribeToMe`: Running Exhaust and Brakewear, both at `YEAR`
    /// granularity (year level for source bins from the
    /// `SourceBinDistributionGenerator`), `GENERATOR` priority.
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

impl Default for OperatingModeDistributionGenerator {
    fn default() -> Self {
        Self::new()
    }
}

/// Default-database tables the OMDG pipeline reads. Names are the canonical
/// MOVES table names; the registry maps them onto Parquet snapshots. `Link`
/// is read only by the `calculateOpModeFractions` link projection.
static INPUT_TABLES: &[&str] = &[
    "DriveSchedule",
    "DriveScheduleAssoc",
    "DriveScheduleSecond",
    "AvgSpeedBin",
    "AvgSpeedDistribution",
    "OperatingMode",
    "OpModePolProcAssoc",
    "sourceUseTypePhysicsMapping",
    "OMDGPolProcessRepresented",
    "RunSpecSourceType",
    "RunSpecRoadType",
    "RunSpecHour",
    "RunSpecDay",
    "HourDay",
    "Link",
];

/// Scratch-namespace table this generator writes.
static OUTPUT_TABLES: &[&str] = &["OpModeDistribution"];

/// Upstream module: `SourceTypePhysics` builds `sourceUseTypePhysicsMapping`/// the road-load polynomial terms and real/temporary source-type rows the VSP
/// calculation reads.
static UPSTREAM: &[&str] = &["SourceTypePhysics"];

impl Generator for OperatingModeDistributionGenerator {
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
    /// Reads the OMDG input tables from `ctx.tables()`, runs the
    /// [`op_mode_distribution`] kernel, cross-joins the result with the
    /// `Link` table on `roadTypeID` to fill link-keyed `OpModeDistribution`
    /// rows, and writes them to the scratch namespace.
    fn execute(&self, ctx: &mut CalculatorContext) -> Result<CalculatorOutput, Error> {
        // -- Read all input tables --
        let drive_schedule: Vec<DriveScheduleRow> = ctx.tables().iter_typed("DriveSchedule")?;
        let drive_schedule_assoc: Vec<DriveScheduleAssocRow> =
            ctx.tables().iter_typed("DriveScheduleAssoc")?;
        let drive_schedule_second: Vec<DriveScheduleSecondRow> =
            ctx.tables().iter_typed("DriveScheduleSecond")?;
        let avg_speed_bin: Vec<AvgSpeedBinRow> = ctx.tables().iter_typed("AvgSpeedBin")?;
        let avg_speed_distribution: Vec<AvgSpeedDistributionRow> =
            ctx.tables().iter_typed("AvgSpeedDistribution")?;
        let operating_mode: Vec<OperatingModeRow> = ctx.tables().iter_typed("OperatingMode")?;
        let op_mode_pol_proc_assoc: Vec<OpModePolProcAssocRow> =
            ctx.tables().iter_typed("OpModePolProcAssoc")?;
        let physics_mapping: Vec<PhysicsMappingRow> =
            ctx.tables().iter_typed("sourceUseTypePhysicsMapping")?;
        let pol_process_represented: Vec<PolProcessRepresentedRow> =
            ctx.tables().iter_typed("OMDGPolProcessRepresented")?;

        // RunSpec selections — separate wrapper row types.
        let run_spec_source_type_rows: Vec<OmdgRunSpecSourceTypeRow> =
            ctx.tables().iter_typed("RunSpecSourceType")?;
        let run_spec_road_type_rows: Vec<OmdgRunSpecRoadTypeRow> =
            ctx.tables().iter_typed("RunSpecRoadType")?;
        let run_spec_hour_day_rows: Vec<OmdgHourDayRow> = ctx.tables().iter_typed("HourDay")?;

        // Link table for the cross-join step.
        let link_rows: Vec<OmdgLinkRow> = ctx.tables().iter_typed("Link")?;

        // -- Convert RunSpec wrapper rows to plain id slices --
        let run_spec_source_type: Vec<SourceTypeId> = run_spec_source_type_rows
            .iter()
            .map(|r| SourceTypeId(r.source_type_id as u16))
            .collect();
        let run_spec_road_type: Vec<RoadTypeId> = run_spec_road_type_rows
            .iter()
            .map(|r| RoadTypeId(r.road_type_id as u16))
            .collect();
        let run_spec_hour_day: Vec<i16> = run_spec_hour_day_rows
            .iter()
            .map(|r| r.hour_day_id as i16)
            .collect();

        // -- Build OmdgInputs and run the kernel --
        let omdg_inputs = OmdgInputs {
            drive_schedule: &drive_schedule,
            drive_schedule_assoc: &drive_schedule_assoc,
            drive_schedule_second: &drive_schedule_second,
            avg_speed_bin: &avg_speed_bin,
            avg_speed_distribution: &avg_speed_distribution,
            operating_mode: &operating_mode,
            op_mode_pol_proc_assoc: &op_mode_pol_proc_assoc,
            physics_mapping: &physics_mapping,
            pol_process_represented: &pol_process_represented,
            run_spec_source_type: &run_spec_source_type,
            run_spec_road_type: &run_spec_road_type,
            run_spec_hour_day: &run_spec_hour_day,
        };
        let fractions = op_mode_distribution(&omdg_inputs);

        // -- Cross-join OpModeFractionRow with Link on roadTypeID --
        // Java: INSERT IGNORE INTO OpModeDistribution SELECT linkID, ...
        // FROM OpModeFraction2 INNER JOIN Link ON link.roadTypeID = opModeFraction.roadTypeID
        let mut output_rows: Vec<OpModeDistributionRow> = Vec::new();
        for fraction in &fractions {
            for link in &link_rows {
                if link.road_type_id == fraction.road_type_id.0 as i32 {
                    output_rows.push(OpModeDistributionRow {
                        source_type_id: fraction.source_type_id,
                        road_type_id: fraction.road_type_id,
                        link_id: link.link_id,
                        hour_day_id: fraction.hour_day_id,
                        pol_process_id: fraction.pol_process_id,
                        op_mode_id: fraction.op_mode_id,
                        op_mode_fraction: fraction.op_mode_fraction,
                    });
                }
            }
        }

        crate::wiring::write_scratch_table(ctx, "OpModeDistribution", output_rows)
    }
}

// ============================================================================
// Data-plane wiring — helper row types and TableRow impls for
// the RunSpec selection tables and the Link cross-join table.
// ============================================================================

/// Build a [`Error::RowExtraction`] for a missing/bad cell in this module.
fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::RowExtraction {
        table: table.into(),
        row,
        column: column.into(),
        message: msg,
    }
}

/// One `RunSpecSourceType` row — a source type the RunSpec selects.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OmdgRunSpecSourceTypeRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
}

impl TableRow for OmdgRunSpecSourceTypeRow {
    fn table_name() -> &'static str {
        "RunSpecSourceType"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([("sourceTypeID".into(), DataType::Int32)])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![Series::new(
                "sourceTypeID".into(),
                rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
            )
            .into()],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "RunSpecSourceType";
        let source_type_id = df
            .column("sourceTypeID")
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(OmdgRunSpecSourceTypeRow {
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                })
            })
            .collect()
    }
}

/// One `RunSpecRoadType` row — a road type the RunSpec selects.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OmdgRunSpecRoadTypeRow {
    /// `roadTypeID`.
    pub road_type_id: i32,
}

impl TableRow for OmdgRunSpecRoadTypeRow {
    fn table_name() -> &'static str {
        "RunSpecRoadType"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([("roadTypeID".into(), DataType::Int32)])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![Series::new(
                "roadTypeID".into(),
                rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>(),
            )
            .into()],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "RunSpecRoadType";
        let road_type_id = df
            .column("roadTypeID")
            .map_err(|e| row_err(t, 0, "roadTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "roadTypeID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(OmdgRunSpecRoadTypeRow {
                    road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
                })
            })
            .collect()
    }
}

/// One `HourDay` row — the `hourDayID`s active in the run (derived from
/// `RunSpecHour` × `RunSpecDay` joined through `HourDay`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OmdgHourDayRow {
    /// `hourDayID`.
    pub hour_day_id: i32,
}

impl TableRow for OmdgHourDayRow {
    fn table_name() -> &'static str {
        "HourDay"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([("hourDayID".into(), DataType::Int32)])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![Series::new(
                "hourDayID".into(),
                rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
            )
            .into()],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "HourDay";
        let hour_day_id = df
            .column("hourDayID")
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(OmdgHourDayRow {
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                })
            })
            .collect()
    }
}

/// One `Link` row — the `(linkID, roadTypeID)` the cross-join uses.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OmdgLinkRow {
    /// `linkID`.
    pub link_id: i32,
    /// `roadTypeID`.
    pub road_type_id: i32,
}

impl TableRow for OmdgLinkRow {
    fn table_name() -> &'static str {
        "Link"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("linkID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "linkID".into(),
                    rows.iter().map(|r| r.link_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeID".into(),
                    rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "Link";
        let link_id = df
            .column("linkID")
            .map_err(|e| row_err(t, 0, "linkID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "linkID", e.to_string()))?;
        let road_type_id = df
            .column("roadTypeID")
            .map_err(|e| row_err(t, 0, "roadTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "roadTypeID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(OmdgLinkRow {
                    link_id: link_id.get(i).ok_or_else(|| null("linkID"))?,
                    road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
                })
            })
            .collect()
    }
}

/// One `OpModeDistribution` output row — the link-keyed operating-mode
/// fraction produced by cross-joining [`OpModeFractionRow`] with `Link`.
///
/// The Java final step populates `OpModeDistribution` with columns
/// `(linkID, sourceTypeID, hourDayID, polProcessID, opModeID, opModeFraction)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OpModeDistributionRow {
    /// `sourceTypeID` (real or temporary).
    pub source_type_id: SourceTypeId,
    /// `roadTypeID`.
    pub road_type_id: RoadTypeId,
    /// `linkID` — from the `Link` cross-join.
    pub link_id: i32,
    /// `hourDayID`.
    pub hour_day_id: i16,
    /// `polProcessID`.
    pub pol_process_id: PolProcessId,
    /// `opModeID`.
    pub op_mode_id: i16,
    /// `opModeFraction`.
    pub op_mode_fraction: f64,
}

impl TableRow for OpModeDistributionRow {
    fn table_name() -> &'static str {
        "OpModeDistribution"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("linkID".into(), DataType::Int32),
            ("hourDayID".into(), DataType::Int32),
            ("polProcessID".into(), DataType::Int32),
            ("opModeID".into(), DataType::Int32),
            ("opModeFraction".into(), DataType::Float64),
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
                    "linkID".into(),
                    rows.iter().map(|r| r.link_id).collect::<Vec<i32>>(),
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
                Series::new(
                    "opModeFraction".into(),
                    rows.iter()
                        .map(|r| r.op_mode_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "OpModeDistribution";
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
        let link_id = df
            .column("linkID")
            .map_err(|e| row_err(t, 0, "linkID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "linkID", e.to_string()))?;
        let hour_day_id = df
            .column("hourDayID")
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?;
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
        let op_mode_fraction = df
            .column("opModeFraction")
            .map_err(|e| row_err(t, 0, "opModeFraction", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "opModeFraction", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(OpModeDistributionRow {
                    source_type_id: SourceTypeId(
                        source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))? as u16,
                    ),
                    road_type_id: RoadTypeId(
                        road_type_id.get(i).ok_or_else(|| null("roadTypeID"))? as u16,
                    ),
                    link_id: link_id.get(i).ok_or_else(|| null("linkID"))?,
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))? as i16,
                    pol_process_id: PolProcessId(
                        pol_process_id.get(i).ok_or_else(|| null("polProcessID"))? as u32,
                    ),
                    op_mode_id: op_mode_id.get(i).ok_or_else(|| null("opModeID"))? as i16,
                    op_mode_fraction: op_mode_fraction
                        .get(i)
                        .ok_or_else(|| null("opModeFraction"))?,
                })
            })
            .collect()
    }
}

/// Satisfy the generator-factory signature so the calculator registry can register it.
#[must_use]
pub fn factory() -> Box<dyn Generator> {
    Box::new(OperatingModeDistributionGenerator::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generator_metadata_matches_java_subscribe_to_me() {
        let generator = OperatingModeDistributionGenerator::new();
        assert_eq!(generator.name(), "OperatingModeDistributionGenerator");
        assert_eq!(generator.output_tables(), &["OpModeDistribution"]);
        assert_eq!(generator.upstream(), &["SourceTypePhysics"]);
        assert!(generator.input_tables().contains(&"AvgSpeedDistribution"));
        assert!(generator.input_tables().contains(&"DriveScheduleSecond"));

        let subscriptions = generator.subscriptions();
        assert_eq!(subscriptions.len(), 2);
        let processes: Vec<ProcessId> = subscriptions.iter().map(|s| s.process_id).collect();
        // Running Exhaust (1) and Brakewear (9).
        assert_eq!(processes, vec![ProcessId(1), ProcessId(9)]);
        for subscription in subscriptions {
            assert_eq!(subscription.granularity, Granularity::Year);
            assert_eq!(subscription.priority.display(), "GENERATOR");
        }
    }

    #[test]
    fn generator_is_object_safe() {
        // The registry stores generators as Box<dyn Generator>.
        let generator: Box<dyn Generator> = Box::new(OperatingModeDistributionGenerator::new());
        assert_eq!(generator.name(), "OperatingModeDistributionGenerator");
    }

    #[test]
    fn generator_default_matches_new() {
        let generator = OperatingModeDistributionGenerator::default();
        assert_eq!(generator.subscriptions().len(), 2);
    }

    // ── execute() integration test ──────────────────────────────────────

    /// Populate an `InMemoryStore` with the minimum tables needed for the
    /// two-cycle scenario used in pipeline tests, plus RunSpec and Link tables.
    fn minimal_execute_store() -> moves_framework::InMemoryStore {
        use super::inputs::{
            AvgSpeedBinRow, AvgSpeedDistributionRow, DriveScheduleAssocRow, DriveScheduleRow,
            DriveScheduleSecondRow, OpModePolProcAssocRow, OperatingModeRow, PhysicsMappingRow,
            PolProcessRepresentedRow,
        };
        use moves_framework::{DataFrameStore, InMemoryStore};

        const SOURCE_TYPE: SourceTypeId = SourceTypeId(21);
        const ROAD_TYPE: RoadTypeId = RoadTypeId(5);
        const HOUR_DAY: i32 = 51;
        const POL_PROCESS: PolProcessId = PolProcessId(101);
        const LINK_ID: i32 = 1001;

        let mut store = InMemoryStore::new();

        // DriveSchedule: two cycles
        store.insert(
            "DriveSchedule",
            DriveScheduleRow::into_dataframe(vec![
                DriveScheduleRow {
                    drive_schedule_id: 1,
                    average_speed: 10.0,
                },
                DriveScheduleRow {
                    drive_schedule_id: 2,
                    average_speed: 30.0,
                },
            ])
            .unwrap(),
        );

        // DriveScheduleAssoc
        store.insert(
            "DriveScheduleAssoc",
            DriveScheduleAssocRow::into_dataframe(vec![
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
            ])
            .unwrap(),
        );

        // DriveScheduleSecond: idle cycle (ds1) and 30-mph cycle (ds2)
        let mut seconds = Vec::new();
        for s in 0i16..=3 {
            seconds.push(DriveScheduleSecondRow {
                drive_schedule_id: 1,
                second: s,
                speed: 0.0,
            });
            seconds.push(DriveScheduleSecondRow {
                drive_schedule_id: 2,
                second: s,
                speed: 30.0,
            });
        }
        store.insert(
            "DriveScheduleSecond",
            DriveScheduleSecondRow::into_dataframe(seconds).unwrap(),
        );

        // AvgSpeedBin: one bin at 20 mph
        store.insert(
            "AvgSpeedBin",
            AvgSpeedBinRow::into_dataframe(vec![AvgSpeedBinRow {
                avg_speed_bin_id: 1,
                avg_bin_speed: 20.0,
            }])
            .unwrap(),
        );

        // AvgSpeedDistribution: all weight on bin 1
        store.insert(
            "AvgSpeedDistribution",
            AvgSpeedDistributionRow::into_dataframe(vec![AvgSpeedDistributionRow {
                source_type_id: SOURCE_TYPE,
                road_type_id: ROAD_TYPE,
                hour_day_id: HOUR_DAY as i16,
                avg_speed_bin_id: 1,
                avg_speed_fraction: 1.0,
            }])
            .unwrap(),
        );

        // OperatingMode: idle (1) is a special case; mode 30 catches VSP=0, speed>=1
        store.insert(
            "OperatingMode",
            OperatingModeRow::into_dataframe(vec![OperatingModeRow {
                op_mode_id: 30,
                vsp_lower: Some(-100.0),
                vsp_upper: Some(100.0),
                speed_lower: None,
                speed_upper: None,
            }])
            .unwrap(),
        );

        // OpModePolProcAssoc: idle and mode 30 for POL_PROCESS
        store.insert(
            "OpModePolProcAssoc",
            OpModePolProcAssocRow::into_dataframe(vec![
                OpModePolProcAssocRow {
                    pol_process_id: POL_PROCESS,
                    op_mode_id: 1,
                },
                OpModePolProcAssocRow {
                    pol_process_id: POL_PROCESS,
                    op_mode_id: 30,
                },
            ])
            .unwrap(),
        );

        // sourceUseTypePhysicsMapping: identity mapping, flat physics
        store.insert(
            "sourceUseTypePhysicsMapping",
            PhysicsMappingRow::into_dataframe(vec![PhysicsMappingRow {
                real_source_type_id: SOURCE_TYPE,
                temp_source_type_id: SOURCE_TYPE,
                rolling_term_a: 0.0,
                rotating_term_b: 0.0,
                drag_term_c: 0.0,
                source_mass: 1000.0,
                fixed_mass_factor: 1.0,
            }])
            .unwrap(),
        );

        // OMDGPolProcessRepresented: empty (no represented pol/processes)
        store.insert(
            "OMDGPolProcessRepresented",
            PolProcessRepresentedRow::into_dataframe(vec![]).unwrap(),
        );

        // RunSpecSourceType
        store.insert(
            "RunSpecSourceType",
            OmdgRunSpecSourceTypeRow::into_dataframe(vec![OmdgRunSpecSourceTypeRow {
                source_type_id: SOURCE_TYPE.0 as i32,
            }])
            .unwrap(),
        );

        // RunSpecRoadType
        store.insert(
            "RunSpecRoadType",
            OmdgRunSpecRoadTypeRow::into_dataframe(vec![OmdgRunSpecRoadTypeRow {
                road_type_id: ROAD_TYPE.0 as i32,
            }])
            .unwrap(),
        );

        // HourDay: one hour/day id
        store.insert(
            "HourDay",
            OmdgHourDayRow::into_dataframe(vec![OmdgHourDayRow {
                hour_day_id: HOUR_DAY,
            }])
            .unwrap(),
        );

        // Link: one link on road type 5
        store.insert(
            "Link",
            OmdgLinkRow::into_dataframe(vec![OmdgLinkRow {
                link_id: LINK_ID,
                road_type_id: ROAD_TYPE.0 as i32,
            }])
            .unwrap(),
        );

        store
    }

    #[test]
    fn execute_writes_op_mode_distribution_to_scratch() {
        use moves_framework::{DataFrameStoreTyped, IterationPosition};

        let store = minimal_execute_store();
        let pos = IterationPosition::default();
        let mut ctx = CalculatorContext::with_position_and_tables(pos, store);

        let generator = OperatingModeDistributionGenerator::new();
        let out = generator.execute(&mut ctx).expect("execute ok");
        // Generator writes to scratch, not the primary output DataFrame.
        assert!(out.dataframe().is_none());

        let rows: Vec<OpModeDistributionRow> = ctx
            .scratch()
            .store
            .iter_typed("OpModeDistribution")
            .expect("OpModeDistribution in scratch");

        // Two OpModeFractionRows (idle=0.5, mode30=0.5) × one Link = 2 rows.
        assert_eq!(rows.len(), 2, "expected 2 rows, got: {rows:?}");

        // All rows share the same link, source type, road type and hour/day.
        for r in &rows {
            assert_eq!(r.link_id, 1001);
            assert_eq!(r.source_type_id, SourceTypeId(21));
            assert_eq!(r.road_type_id, RoadTypeId(5));
            assert_eq!(r.hour_day_id, 51);
            assert_eq!(r.pol_process_id, PolProcessId(101));
        }

        // One row for idle (op mode 1) and one for mode 30, each fraction 0.5.
        let idle_row = rows.iter().find(|r| r.op_mode_id == 1).expect("idle row");
        assert!((idle_row.op_mode_fraction - 0.5).abs() < 1e-12);
        let mode30_row = rows
            .iter()
            .find(|r| r.op_mode_id == 30)
            .expect("mode 30 row");
        assert!((mode30_row.op_mode_fraction - 0.5).abs() < 1e-12);
    }

    #[test]
    fn execute_empty_link_table_produces_no_output_rows() {
        use moves_framework::{DataFrameStore, DataFrameStoreTyped, IterationPosition};

        let mut store = minimal_execute_store();
        // Replace the Link table with an empty one.
        store.insert("Link", OmdgLinkRow::into_dataframe(vec![]).unwrap());

        let pos = IterationPosition::default();
        let mut ctx = CalculatorContext::with_position_and_tables(pos, store);

        let generator = OperatingModeDistributionGenerator::new();
        generator.execute(&mut ctx).expect("execute ok");

        let rows: Vec<OpModeDistributionRow> = ctx
            .scratch()
            .store
            .iter_typed("OpModeDistribution")
            .expect("OpModeDistribution in scratch");
        assert!(rows.is_empty(), "no output rows when Link table is empty");
    }
}

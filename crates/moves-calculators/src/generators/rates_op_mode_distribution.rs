//! Port of `RatesOperatingModeDistributionGenerator.java`
//! (`gov.epa.otaq.moves.master.implementation.ghg`) — the generator that
//! builds `RatesOpModeDistribution` records for rates-mode (`DO_RATES_FIRST`)
//! runs.
//!
//! Migration plan: Phase 3, Task 43.
//!
//! # What this generator produces
//!
//! In a rates-mode run MOVES emits emission *rates* (grams per unit of
//! activity) rather than an inventory. `RatesOpModeDistribution` is the
//! per-`(sourceType, roadType, avgSpeedBin, polProcess, hourDay, opMode)`
//! operating-mode-fraction table that the rates-mode calculators
//! (`BaseRateCalculator` and the chained criteria calculators) consume.
//!
//! The Java class subscribes to three emission processes at `YEAR`
//! granularity / `GENERATOR` priority — Running Exhaust (process id 1),
//! Extended Idle Exhaust (90) and Auxiliary Power Exhaust (91) — and its
//! `calculateOpModeFractions` dispatcher routes each to its own
//! computation.
//!
//! # Scope of this port — the live path only
//!
//! `RatesOperatingModeDistributionGenerator.java` is 2 275 lines, but two
//! `static final` flags gate most of it as dead code in the pinned EPA
//! source:
//!
//! * `USE_EXTERNAL_GENERATOR_FOR_DRIVE_CYCLES = true` — the drive-schedule
//!   bracketing / VSP / second-by-second op-mode pipeline
//!   (`bracketAverageSpeedBins`, `determineDriveScheduleProportions`,
//!   `calculateEnginePowerBySecond`, `determineOpModeIDPerSecond`,
//!   `calculateOpModeFractionsPerDriveSchedule`,
//!   `preliminaryCalculateOpModeFractions`, and the step-220 SQL inside
//!   `calculateOpModeFractions`) is never executed: the compiled external
//!   generator produces those running-exhaust op modes instead.
//! * `USE_EXTERNAL_GENERATOR = true` — the Running-Exhaust branch of
//!   `calculateOpModeFractions` delegates entirely to the external
//!   generator step `SourceTypePhysics.updateOperatingModeDistribution`;
//!   the SQL fallback (`modelYearPhysics.updateOperatingModeDistribution`)
//!   is dead.
//!
//! What remains *live and self-contained* in the class is exactly:
//!
//! * the master-loop subscription (`subscribeToMe`);
//! * `calculateExtendedIdleOpModeFractions` — Java steps 200, process 90;
//! * `calculateAuxiliaryPowerOpModeFractions` — Java steps 210, process 91;
//! * the `calculateOpModeFractions` process dispatcher.
//!
//! Those are ported here in full. The drive-cycle pipeline and the
//! Running-Exhaust SourceTypePhysics correction are deliberately *not*
//! ported by this task — they belong to the external-generator /
//! `SourceTypePhysics` port (migration-plan Task 37). Running Exhaust
//! therefore contributes no rows *from this module*:
//! [`RatesOperatingModeDistributionGenerator::op_mode_fractions`] returns
//! an empty `Vec` for it, mirroring the fact that the live Java
//! `calculateOpModeFractions` body for process 1 only calls out to the
//! external generator.
//!
//! # The live algorithm
//!
//! Extended Idle (90) and Auxiliary Power (91) are *hotelling* processes:
//! they apply only to source type 62 (combination long-haul truck) while
//! parked off-network. Their operating-mode "distribution" is degenerate —
//! every applicable operating mode is assigned `opModeFraction = 1` on road
//! type 1 (Off-Network), `avgSpeedBinID = 0`, `avgBinSpeed = 0`. The two
//! steps differ only in which operating modes they enumerate and in their
//! treatment of op-mode 200 (extended idling):
//!
//! * **Extended Idle** keeps op-mode 200 and additionally guarantees a
//!   200 row exists for every process-90 `polProcessID`.
//! * **Auxiliary Power** excludes op-mode 200 (extended idling is a
//!   separate process) and draws its hotelling op modes from
//!   `hotellingActivityDistribution`.
//!
//! Both steps emit through MySQL `INSERT IGNORE`, so a `polProcessID` /
//! `opModeID` / `hourDayID` triple that two statements both produce yields
//! a single row. Because every row this generator emits carries the same
//! constant non-key columns (`opModeFraction = 1`, `avgBinSpeed = 0`), an
//! `INSERT IGNORE` collision is always between value-identical rows — the
//! de-duplication here only avoids emitting a primary key twice; it never
//! has to choose between competing values. [`primary key`](RatesOpModeDistributionRow):
//! `(sourceTypeID, polProcessID, roadTypeID, hourDayID, opModeID, avgSpeedBinID)`,
//! per `database/CreateExecutionRates.sql`.
//!
//! # Data plane (Task 50)
//!
//! [`Generator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are Phase-2 placeholders until
//! the `DataFrameStore` lands (migration-plan Task 50), so `execute`
//! cannot yet read the input tables nor write `RatesOpModeDistribution`.
//! The numerically faithful algorithm is fully ported and unit-tested in
//! the free functions [`extended_idle_op_mode_fractions`] and
//! [`auxiliary_power_op_mode_fractions`]; once the data plane exists,
//! `execute` projects an [`OpModeFractionInputs`] out of `ctx.tables()`,
//! dispatches on `ctx.position().process_id` through
//! [`op_mode_fractions`](RatesOperatingModeDistributionGenerator::op_mode_fractions),
//! and writes the result to the scratch namespace.

use std::collections::HashSet;

use moves_calculator_info::{Granularity, Priority};
use moves_data::{
    PolProcessId, PollutantId, PollutantProcessAssociation, ProcessId, RoadTypeId, SourceTypeId,
};
use moves_framework::{
    CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped, Error,
    Generator, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Series};

/// Running Exhaust — process id 1. Subscribed to (outside Project domain)
/// but produces no rows from this module; see the module docs.
const RUNNING_EXHAUST: ProcessId = ProcessId(1);
/// Extended Idle Exhaust — process id 90. Handled by Java steps 200.
const EXTENDED_IDLE_EXHAUST: ProcessId = ProcessId(90);
/// Auxiliary Power Exhaust — process id 91. Handled by Java steps 210.
const AUXILIARY_POWER_EXHAUST: ProcessId = ProcessId(91);

/// The only source type with hotelling activity — combination long-haul
/// truck. Both Java steps hard-filter `sourceTypeID = 62`.
const HOTELLING_SOURCE_TYPE: SourceTypeId = SourceTypeId(62);
/// Off-Network road type — the road type both steps assign (`1 as roadTypeID`).
const OFF_NETWORK_ROAD_TYPE: RoadTypeId = RoadTypeId(1);
/// Extended-idling operating mode. Step 200 guarantees it is present;
/// step 210 (Auxiliary Power) explicitly excludes it.
const EXTENDED_IDLE_OP_MODE: i16 = 200;

/// One `RatesOpModeDistribution` row produced by this generator.
///
/// Models the eight columns the Java `INSERT IGNORE` statements populate.
/// The execution-database table also has `opModeFractionCV` and
/// `avgSpeedFraction`; this generator never sets them, so they take their
/// schema defaults (`NULL` and `0`) and are not modeled here.
///
/// The `FLOAT` columns `avgBinSpeed` and `opModeFraction` are held as
/// `f64` for consistency with the rest of the Rust port; the only values
/// this generator ever stores in them — `0.0` and `1.0` — are exact in
/// both `f32` and `f64`, so the widening introduces no divergence.
///
/// Primary key (the `INSERT IGNORE` de-duplication key, from
/// `database/CreateExecutionRates.sql`): `(sourceTypeID, polProcessID,
/// roadTypeID, hourDayID, opModeID, avgSpeedBinID)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RatesOpModeDistributionRow {
    /// `sourceTypeID` — always source type 62 (combination long-haul
    /// truck), the only hotelling source type.
    pub source_type_id: SourceTypeId,
    /// `roadTypeID` — always road type 1 (Off-Network).
    pub road_type_id: RoadTypeId,
    /// `avgSpeedBinID` — always `0` for hotelling rows.
    pub avg_speed_bin_id: i16,
    /// `avgBinSpeed` — always `0.0` for hotelling rows.
    pub avg_bin_speed: f64,
    /// `polProcessID` — the pollutant/process this fraction applies to.
    pub pol_process_id: PolProcessId,
    /// `hourDayID` — one of the RunSpec's selected hour/day combinations.
    pub hour_day_id: i16,
    /// `opModeID` — the operating mode this fraction applies to.
    pub op_mode_id: i16,
    /// `opModeFraction` — always `1.0` for these degenerate hotelling
    /// distributions.
    pub op_mode_fraction: f64,
}

/// Primary-key tuple of `RatesOpModeDistribution` — the columns the Java
/// `INSERT IGNORE` statements de-duplicate on, in primary-key order.
type RowKey = (SourceTypeId, PolProcessId, RoadTypeId, i16, i16, i16);

impl RatesOpModeDistributionRow {
    /// The primary-key projection used both to de-duplicate `INSERT IGNORE`
    /// collisions and to give the output a deterministic order.
    fn key(&self) -> RowKey {
        (
            self.source_type_id,
            self.pol_process_id,
            self.road_type_id,
            self.hour_day_id,
            self.op_mode_id,
            self.avg_speed_bin_id,
        )
    }
}

/// A `sourceTypePolProcess` row — which `(sourceType, polProcess)` pairs
/// the run models. Java steps 200/210 SQL 1 inner-join this table to pin
/// the source type to 62.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceTypePolProcess {
    /// `sourceTypeID`.
    pub source_type_id: SourceTypeId,
    /// `polProcessID`.
    pub pol_process_id: PolProcessId,
}

/// An `opModePolProcAssoc` row — the operating modes associated with a
/// pollutant/process. Java steps 200/210 SQL 1 enumerate their op modes
/// from this table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpModePolProcAssoc {
    /// `polProcessID`.
    pub pol_process_id: PolProcessId,
    /// `opModeID`.
    pub op_mode_id: i16,
}

/// The projected default-database tables that the Extended Idle (step 200)
/// and Auxiliary Power (step 210) computations read.
///
/// Each field is the Rust analogue of one MySQL table the Java `SELECT`
/// statements reference. Once the Task 50 data plane lands,
/// [`Generator::execute`] builds this view from `ctx.tables()`.
#[derive(Debug, Clone, Copy)]
pub struct OpModeFractionInputs<'a> {
    /// `pollutantProcessAssoc` — every modeled `(pollutant, process)` pair.
    /// The steps filter it by `processID`.
    pub pollutant_process_assoc: &'a [PollutantProcessAssociation],
    /// `sourceTypePolProcess` — which `(sourceType, polProcess)` pairs are
    /// modeled.
    pub source_type_pol_process: &'a [SourceTypePolProcess],
    /// `opModePolProcAssoc` — operating modes per pollutant/process.
    pub op_mode_pol_proc_assoc: &'a [OpModePolProcAssoc],
    /// `runSpecHourDay.hourDayID` — the hour/day combinations the RunSpec
    /// selects. Every emitted row is crossed with this set.
    pub run_spec_hour_day: &'a [i16],
    /// `runSpecSourceType.sourceTypeID` — the source types the RunSpec
    /// selects. Steps 200/210 SQL 2 emit nothing unless 62 is present.
    pub run_spec_source_type: &'a [SourceTypeId],
    /// `hotellingActivityDistribution.opModeID` — the hotelling operating
    /// modes; consumed by step 210 SQL 2. Duplicates are harmless (the
    /// Java uses `SELECT DISTINCT`; the primary-key de-duplication here
    /// has the same effect).
    pub hotelling_op_modes: &'a [i16],
}

// ============================================================================
// Data-plane helpers — `row_err`, `TableRow` impls for input and output rows
// ============================================================================

fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::RowExtraction {
        table: table.into(),
        row,
        column: column.into(),
        message: msg,
    }
}

/// Local wrapper for `pollutantProcessAssoc` rows.
///
/// The foreign type `PollutantProcessAssociation` (in `moves_data`) cannot
/// implement `TableRow` directly (orphan rule), so this private wrapper
/// carries the same two columns the Java steps read.
struct RatesPollutantProcessAssocRow {
    pollutant_id: u16,
    process_id: u16,
}

impl TableRow for RatesPollutantProcessAssocRow {
    fn table_name() -> &'static str {
        "pollutantProcessAssoc"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([
            ("pollutantID".into(), DataType::Int32),
            ("processID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "pollutantID".into(),
                    rows.iter()
                        .map(|r| r.pollutant_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "processID".into(),
                    rows.iter()
                        .map(|r| r.process_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "pollutantProcessAssoc";
        let pollutant_id_col = df
            .column("pollutantID")
            .map_err(|e| row_err(t, 0, "pollutantID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "pollutantID", e.to_string()))?;
        let process_id_col = df
            .column("processID")
            .map_err(|e| row_err(t, 0, "processID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "processID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(RatesPollutantProcessAssocRow {
                    pollutant_id: pollutant_id_col.get(i).ok_or_else(|| null("pollutantID"))?
                        as u16,
                    process_id: process_id_col.get(i).ok_or_else(|| null("processID"))? as u16,
                })
            })
            .collect()
    }
}

/// Local wrapper for `sourceTypePolProcess` rows.
///
/// Reads only `sourceTypeID` and `polProcessID`; the reg-class / MY-group
/// flag columns (present in the full table) are not needed by this generator.
struct RatesSourceTypePolProcessRow {
    source_type_id: u16,
    pol_process_id: u32,
}

impl TableRow for RatesSourceTypePolProcessRow {
    fn table_name() -> &'static str {
        "sourceTypePolProcess"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("polProcessID".into(), DataType::Int32),
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
                        .map(|r| r.source_type_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "polProcessID".into(),
                    rows.iter()
                        .map(|r| r.pol_process_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "sourceTypePolProcess";
        let source_type_id_col = df
            .column("sourceTypeID")
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?;
        let pol_process_id_col = df
            .column("polProcessID")
            .map_err(|e| row_err(t, 0, "polProcessID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "polProcessID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(RatesSourceTypePolProcessRow {
                    source_type_id: source_type_id_col
                        .get(i)
                        .ok_or_else(|| null("sourceTypeID"))?
                        as u16,
                    pol_process_id: pol_process_id_col
                        .get(i)
                        .ok_or_else(|| null("polProcessID"))?
                        as u32,
                })
            })
            .collect()
    }
}

/// Local wrapper for `opModePolProcAssoc` rows.
struct RatesOpModePolProcAssocRow {
    pol_process_id: u32,
    op_mode_id: i16,
}

impl TableRow for RatesOpModePolProcAssocRow {
    fn table_name() -> &'static str {
        "opModePolProcAssoc"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([
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
                        .map(|r| r.pol_process_id as i32)
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
        let t = "opModePolProcAssoc";
        let pol_process_id_col = df
            .column("polProcessID")
            .map_err(|e| row_err(t, 0, "polProcessID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "polProcessID", e.to_string()))?;
        let op_mode_id_col = df
            .column("opModeID")
            .map_err(|e| row_err(t, 0, "opModeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "opModeID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(RatesOpModePolProcAssocRow {
                    pol_process_id: pol_process_id_col
                        .get(i)
                        .ok_or_else(|| null("polProcessID"))?
                        as u32,
                    op_mode_id: op_mode_id_col.get(i).ok_or_else(|| null("opModeID"))? as i16,
                })
            })
            .collect()
    }
}

/// Local wrapper for `runSpecHourDay` rows (single-column: `hourDayID`).
struct RatesRunSpecHourDayRow {
    hour_day_id: i16,
}

impl TableRow for RatesRunSpecHourDayRow {
    fn table_name() -> &'static str {
        "runSpecHourDay"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([("hourDayID".into(), DataType::Int32)])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![Series::new(
                "hourDayID".into(),
                rows.iter()
                    .map(|r| r.hour_day_id as i32)
                    .collect::<Vec<i32>>(),
            )
            .into()],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "runSpecHourDay";
        let hour_day_id_col = df
            .column("hourDayID")
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(RatesRunSpecHourDayRow {
                    hour_day_id: hour_day_id_col.get(i).ok_or_else(|| null("hourDayID"))? as i16,
                })
            })
            .collect()
    }
}

/// Local wrapper for `runSpecSourceType` rows (single-column: `sourceTypeID`).
struct RatesRunSpecSourceTypeRow {
    source_type_id: u16,
}

impl TableRow for RatesRunSpecSourceTypeRow {
    fn table_name() -> &'static str {
        "runSpecSourceType"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([("sourceTypeID".into(), DataType::Int32)])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![Series::new(
                "sourceTypeID".into(),
                rows.iter()
                    .map(|r| r.source_type_id as i32)
                    .collect::<Vec<i32>>(),
            )
            .into()],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "runSpecSourceType";
        let source_type_id_col = df
            .column("sourceTypeID")
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(RatesRunSpecSourceTypeRow {
                    source_type_id: source_type_id_col
                        .get(i)
                        .ok_or_else(|| null("sourceTypeID"))?
                        as u16,
                })
            })
            .collect()
    }
}

/// Local wrapper for `hotellingActivityDistribution` rows.
///
/// Reads only `opModeID`; the fraction, model-year-range and fuel columns
/// are not needed by this generator (it only needs the set of hotelling
/// operating modes).
struct RatesHotellingActivityDistributionRow {
    op_mode_id: i16,
}

impl TableRow for RatesHotellingActivityDistributionRow {
    fn table_name() -> &'static str {
        "hotellingActivityDistribution"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([("opModeID".into(), DataType::Int32)])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![Series::new(
                "opModeID".into(),
                rows.iter()
                    .map(|r| r.op_mode_id as i32)
                    .collect::<Vec<i32>>(),
            )
            .into()],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "hotellingActivityDistribution";
        let op_mode_id_col = df
            .column("opModeID")
            .map_err(|e| row_err(t, 0, "opModeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "opModeID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(RatesHotellingActivityDistributionRow {
                    op_mode_id: op_mode_id_col.get(i).ok_or_else(|| null("opModeID"))? as i16,
                })
            })
            .collect()
    }
}

impl TableRow for RatesOpModeDistributionRow {
    fn table_name() -> &'static str {
        "RatesOpModeDistribution"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("avgSpeedBinID".into(), DataType::Int32),
            ("avgBinSpeed".into(), DataType::Float64),
            ("polProcessID".into(), DataType::Int32),
            ("hourDayID".into(), DataType::Int32),
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
                Series::new(
                    "polProcessID".into(),
                    rows.iter()
                        .map(|r| r.pol_process_id.0 as i32)
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
        let t = "RatesOpModeDistribution";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let source_type_id = get_i32("sourceTypeID")?;
        let road_type_id = get_i32("roadTypeID")?;
        let avg_speed_bin_id = get_i32("avgSpeedBinID")?;
        let avg_bin_speed = get_f64("avgBinSpeed")?;
        let pol_process_id = get_i32("polProcessID")?;
        let hour_day_id = get_i32("hourDayID")?;
        let op_mode_id = get_i32("opModeID")?;
        let op_mode_fraction = get_f64("opModeFraction")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(RatesOpModeDistributionRow {
                    source_type_id: SourceTypeId(
                        source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))? as u16,
                    ),
                    road_type_id: RoadTypeId(
                        road_type_id.get(i).ok_or_else(|| null("roadTypeID"))? as u16,
                    ),
                    avg_speed_bin_id: avg_speed_bin_id
                        .get(i)
                        .ok_or_else(|| null("avgSpeedBinID"))?
                        as i16,
                    avg_bin_speed: avg_bin_speed.get(i).ok_or_else(|| null("avgBinSpeed"))?,
                    pol_process_id: PolProcessId(
                        pol_process_id.get(i).ok_or_else(|| null("polProcessID"))? as u32,
                    ),
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))? as i16,
                    op_mode_id: op_mode_id.get(i).ok_or_else(|| null("opModeID"))? as i16,
                    op_mode_fraction: op_mode_fraction
                        .get(i)
                        .ok_or_else(|| null("opModeFraction"))?,
                })
            })
            .collect()
    }
}

/// Build a hotelling `RatesOpModeDistribution` row — the shared row shape
/// of every `INSERT` in Java steps 200 and 210: source type 62, road type
/// 1, `avgSpeedBinID = 0`, `avgBinSpeed = 0`, `opModeFraction = 1`.
fn hotelling_row(
    pol_process_id: PolProcessId,
    hour_day_id: i16,
    op_mode_id: i16,
) -> RatesOpModeDistributionRow {
    RatesOpModeDistributionRow {
        source_type_id: HOTELLING_SOURCE_TYPE,
        road_type_id: OFF_NETWORK_ROAD_TYPE,
        avg_speed_bin_id: 0,
        avg_bin_speed: 0.0,
        pol_process_id,
        hour_day_id,
        op_mode_id,
        op_mode_fraction: 1.0,
    }
}

/// Apply MySQL `INSERT IGNORE` semantics to a candidate row list: keep the
/// first row for each primary key, drop later collisions, and return the
/// result in deterministic primary-key order.
fn insert_ignore(rows: Vec<RatesOpModeDistributionRow>) -> Vec<RatesOpModeDistributionRow> {
    let mut seen: HashSet<RowKey> = HashSet::new();
    let mut out: Vec<RatesOpModeDistributionRow> =
        rows.into_iter().filter(|r| seen.insert(r.key())).collect();
    out.sort_unstable_by_key(RatesOpModeDistributionRow::key);
    out
}

/// Collect the `polProcessID`s associated with `process` in
/// `pollutantProcessAssoc` — the set the steps' `ppa.processID = …` filter
/// and `polProcessID` join select.
fn polprocs_for_process(
    pollutant_process_assoc: &[PollutantProcessAssociation],
    process: ProcessId,
) -> HashSet<PolProcessId> {
    pollutant_process_assoc
        .iter()
        .filter(|ppa| ppa.process_id == process)
        .map(|ppa| ppa.polproc_id())
        .collect()
}

/// Port of `calculateExtendedIdleOpModeFractions` — Java steps 200,
/// Extended Idle Exhaust (process 90).
///
/// Two `INSERT IGNORE` statements:
///
/// * **SQL 1** — for every process-90 `polProcessID` that is modeled for
///   source type 62 (`sourceTypePolProcess`), emit one row per operating
///   mode in `opModePolProcAssoc`, crossed with every `runSpecHourDay`.
/// * **SQL 2** — for every process-90 `polProcessID`, emit an op-mode-200
///   (extended idling) row crossed with every `runSpecHourDay`, but only
///   if source type 62 is in `runSpecSourceType`. `INSERT IGNORE` means
///   this is a no-op for any `polProcessID` whose op-mode 200 row SQL 1
///   already produced.
///
/// Every row carries `sourceTypeID = 62`, `roadTypeID = 1`,
/// `avgSpeedBinID = 0`, `avgBinSpeed = 0`, `opModeFraction = 1`.
#[must_use]
pub fn extended_idle_op_mode_fractions(
    inputs: &OpModeFractionInputs<'_>,
) -> Vec<RatesOpModeDistributionRow> {
    let process_90 = polprocs_for_process(inputs.pollutant_process_assoc, EXTENDED_IDLE_EXHAUST);
    let mut rows: Vec<RatesOpModeDistributionRow> = Vec::new();

    // @step 200, SQL 1: opModePolProcAssoc op modes for (source type 62,
    // process-90 polProcess), crossed with runSpecHourDay.
    for stpp in inputs.source_type_pol_process {
        if stpp.source_type_id != HOTELLING_SOURCE_TYPE
            || !process_90.contains(&stpp.pol_process_id)
        {
            continue;
        }
        for omppa in inputs.op_mode_pol_proc_assoc {
            if omppa.pol_process_id != stpp.pol_process_id {
                continue;
            }
            for &hour_day_id in inputs.run_spec_hour_day {
                rows.push(hotelling_row(
                    stpp.pol_process_id,
                    hour_day_id,
                    omppa.op_mode_id,
                ));
            }
        }
    }

    // @step 200, SQL 2: op-mode-200 row for every process-90 polProcess,
    // crossed with runSpecHourDay — only when source type 62 is selected.
    if inputs.run_spec_source_type.contains(&HOTELLING_SOURCE_TYPE) {
        for ppa in inputs.pollutant_process_assoc {
            if ppa.process_id != EXTENDED_IDLE_EXHAUST {
                continue;
            }
            for &hour_day_id in inputs.run_spec_hour_day {
                rows.push(hotelling_row(
                    ppa.polproc_id(),
                    hour_day_id,
                    EXTENDED_IDLE_OP_MODE,
                ));
            }
        }
    }

    insert_ignore(rows)
}

/// Port of `calculateAuxiliaryPowerOpModeFractions` — Java steps 210,
/// Auxiliary Power Exhaust (process 91).
///
/// Symmetric to [`extended_idle_op_mode_fractions`], with two differences:
///
/// * **SQL 1** excludes op-mode 200 (extended idling) — `opModeID <> 200`.
/// * **SQL 2** draws its operating modes from `hotellingActivityDistribution`
///   (the [`hotelling_op_modes`](OpModeFractionInputs::hotelling_op_modes)
///   field), again excluding op-mode 200, instead of inserting a fixed 200
///   row. The Java `SELECT DISTINCT` collapses repeated op modes; the
///   primary-key de-duplication applied to the output has the same effect.
#[must_use]
pub fn auxiliary_power_op_mode_fractions(
    inputs: &OpModeFractionInputs<'_>,
) -> Vec<RatesOpModeDistributionRow> {
    let process_91 = polprocs_for_process(inputs.pollutant_process_assoc, AUXILIARY_POWER_EXHAUST);
    let mut rows: Vec<RatesOpModeDistributionRow> = Vec::new();

    // @step 210, SQL 1: opModePolProcAssoc op modes (excluding 200) for
    // (source type 62, process-91 polProcess), crossed with runSpecHourDay.
    for stpp in inputs.source_type_pol_process {
        if stpp.source_type_id != HOTELLING_SOURCE_TYPE
            || !process_91.contains(&stpp.pol_process_id)
        {
            continue;
        }
        for omppa in inputs.op_mode_pol_proc_assoc {
            if omppa.pol_process_id != stpp.pol_process_id
                || omppa.op_mode_id == EXTENDED_IDLE_OP_MODE
            {
                continue;
            }
            for &hour_day_id in inputs.run_spec_hour_day {
                rows.push(hotelling_row(
                    stpp.pol_process_id,
                    hour_day_id,
                    omppa.op_mode_id,
                ));
            }
        }
    }

    // @step 210, SQL 2: every hotelling op mode (excluding 200) for every
    // process-91 polProcess, crossed with runSpecHourDay — only when
    // source type 62 is selected.
    if inputs.run_spec_source_type.contains(&HOTELLING_SOURCE_TYPE) {
        for ppa in inputs.pollutant_process_assoc {
            if ppa.process_id != AUXILIARY_POWER_EXHAUST {
                continue;
            }
            for &op_mode_id in inputs.hotelling_op_modes {
                if op_mode_id == EXTENDED_IDLE_OP_MODE {
                    continue;
                }
                for &hour_day_id in inputs.run_spec_hour_day {
                    rows.push(hotelling_row(ppa.polproc_id(), hour_day_id, op_mode_id));
                }
            }
        }
    }

    insert_ignore(rows)
}

/// `RatesOpModeDistribution` generator for rates-mode runs.
///
/// Ports `RatesOperatingModeDistributionGenerator.java`; see the module
/// documentation for the scope of the port.
#[derive(Debug, Clone)]
pub struct RatesOperatingModeDistributionGenerator {
    /// The three master-loop subscriptions, built once in [`Self::new`].
    subscriptions: [CalculatorSubscription; 3],
}

impl RatesOperatingModeDistributionGenerator {
    /// Chain-DAG name — matches the Java class name.
    pub const NAME: &'static str = "RatesOperatingModeDistributionGenerator";

    /// Construct the generator with its master-loop subscriptions.
    ///
    /// Mirrors `subscribeToMe`: Running Exhaust, Extended Idle Exhaust and
    /// Auxiliary Power Exhaust, all at `YEAR` granularity (year level for
    /// source bins from the SourceBinDistributionGenerator), `GENERATOR`
    /// priority.
    ///
    /// The Java `subscribeToMe` drops Running Exhaust in the Project
    /// domain; that is a runtime RunSpec decision the registry / engine
    /// applies, so the static subscription metadata here always lists all
    /// three processes.
    #[must_use]
    pub fn new() -> Self {
        let priority =
            Priority::parse("GENERATOR").expect("\"GENERATOR\" is a valid MasterLoop priority");
        let at_year = |process| CalculatorSubscription::new(process, Granularity::Year, priority);
        Self {
            subscriptions: [
                at_year(RUNNING_EXHAUST),
                at_year(EXTENDED_IDLE_EXHAUST),
                at_year(AUXILIARY_POWER_EXHAUST),
            ],
        }
    }

    /// Compute the `RatesOpModeDistribution` rows this generator
    /// contributes for `process_id`, given the projected input tables.
    ///
    /// This is the port of the Java `calculateOpModeFractions` process
    /// dispatcher:
    ///
    /// * Extended Idle Exhaust (90) → [`extended_idle_op_mode_fractions`];
    /// * Auxiliary Power Exhaust (91) → [`auxiliary_power_op_mode_fractions`];
    /// * Running Exhaust (1) and anything else → no rows. Running
    ///   Exhaust's op-mode distribution is produced by the external
    ///   generator's `SourceTypePhysics.updateOperatingModeDistribution`
    ///   step (migration-plan Task 37), not by this class.
    #[must_use]
    pub fn op_mode_fractions(
        &self,
        process_id: ProcessId,
        inputs: &OpModeFractionInputs<'_>,
    ) -> Vec<RatesOpModeDistributionRow> {
        if process_id == EXTENDED_IDLE_EXHAUST {
            extended_idle_op_mode_fractions(inputs)
        } else if process_id == AUXILIARY_POWER_EXHAUST {
            auxiliary_power_op_mode_fractions(inputs)
        } else {
            Vec::new()
        }
    }
}

impl Default for RatesOperatingModeDistributionGenerator {
    fn default() -> Self {
        Self::new()
    }
}

/// Default-database tables steps 200 and 210 read. Names are the canonical
/// MOVES table names; the registry maps them onto Parquet snapshots.
static INPUT_TABLES: &[&str] = &[
    "pollutantProcessAssoc",
    "sourceTypePolProcess",
    "opModePolProcAssoc",
    "runSpecHourDay",
    "runSpecSourceType",
    "hotellingActivityDistribution",
];

/// Scratch-namespace table this generator writes.
static OUTPUT_TABLES: &[&str] = &["RatesOpModeDistribution"];

/// Upstream module: `SourceTypePhysics` supplies the model-year physics
/// setup (`modelYearPhysics.setup`) and, for Running Exhaust, the external
/// generator's op-mode-distribution correction.
static UPSTREAM: &[&str] = &["SourceTypePhysics"];

impl Generator for RatesOperatingModeDistributionGenerator {
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
    /// Reads the six input tables from `ctx.tables()`, builds an
    /// [`OpModeFractionInputs`] view, dispatches through
    /// [`op_mode_fractions`](Self::op_mode_fractions) on
    /// `ctx.position().process_id`, and writes the result to
    /// `ctx.scratch()` under `"RatesOpModeDistribution"`.
    fn execute(&self, ctx: &mut CalculatorContext) -> Result<CalculatorOutput, Error> {
        let process_id = ctx
            .position()
            .process_id
            .ok_or_else(|| Error::Polars("no process_id in iteration position".into()))?;

        // Read the six input tables.
        let ppa_raw: Vec<RatesPollutantProcessAssocRow> =
            ctx.tables().iter_typed("pollutantProcessAssoc")?;
        let stpp_raw: Vec<RatesSourceTypePolProcessRow> =
            ctx.tables().iter_typed("sourceTypePolProcess")?;
        let omppa_raw: Vec<RatesOpModePolProcAssocRow> =
            ctx.tables().iter_typed("opModePolProcAssoc")?;
        let hour_day_raw: Vec<RatesRunSpecHourDayRow> =
            ctx.tables().iter_typed("runSpecHourDay")?;
        let source_type_raw: Vec<RatesRunSpecSourceTypeRow> =
            ctx.tables().iter_typed("runSpecSourceType")?;
        let hotelling_raw: Vec<RatesHotellingActivityDistributionRow> =
            ctx.tables().iter_typed("hotellingActivityDistribution")?;

        // Convert wrapper rows to the algorithm's input types.
        let pollutant_process_assoc: Vec<PollutantProcessAssociation> = ppa_raw
            .iter()
            .map(|r| PollutantProcessAssociation {
                pollutant_id: PollutantId(r.pollutant_id),
                process_id: ProcessId(r.process_id),
            })
            .collect();
        let source_type_pol_process: Vec<SourceTypePolProcess> = stpp_raw
            .iter()
            .map(|r| SourceTypePolProcess {
                source_type_id: SourceTypeId(r.source_type_id),
                pol_process_id: PolProcessId(r.pol_process_id),
            })
            .collect();
        let op_mode_pol_proc_assoc: Vec<OpModePolProcAssoc> = omppa_raw
            .iter()
            .map(|r| OpModePolProcAssoc {
                pol_process_id: PolProcessId(r.pol_process_id),
                op_mode_id: r.op_mode_id,
            })
            .collect();
        let run_spec_hour_day: Vec<i16> = hour_day_raw.iter().map(|r| r.hour_day_id).collect();
        let run_spec_source_type: Vec<SourceTypeId> = source_type_raw
            .iter()
            .map(|r| SourceTypeId(r.source_type_id))
            .collect();
        let hotelling_op_modes: Vec<i16> = hotelling_raw.iter().map(|r| r.op_mode_id).collect();

        let inputs = OpModeFractionInputs {
            pollutant_process_assoc: &pollutant_process_assoc,
            source_type_pol_process: &source_type_pol_process,
            op_mode_pol_proc_assoc: &op_mode_pol_proc_assoc,
            run_spec_hour_day: &run_spec_hour_day,
            run_spec_source_type: &run_spec_source_type,
            hotelling_op_modes: &hotelling_op_modes,
        };

        let rows = self.op_mode_fractions(process_id, &inputs);
        crate::wiring::write_scratch_table(ctx, OUTPUT_TABLES[0], rows)
    }
}

/// Generator factory — returns a boxed instance for registration with the
/// `CalculatorRegistry`.
#[must_use]
pub fn factory() -> Box<dyn Generator> {
    Box::new(RatesOperatingModeDistributionGenerator::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use moves_data::PollutantId;

    /// `(pollutant, process)` association helper for test inputs.
    fn ppa(pollutant: u16, process: u16) -> PollutantProcessAssociation {
        PollutantProcessAssociation {
            pollutant_id: PollutantId(pollutant),
            process_id: ProcessId(process),
        }
    }

    /// `polProcessID` for a `(pollutant, process)` pair.
    fn polproc(pollutant: u16, process: u16) -> PolProcessId {
        PolProcessId::new(PollutantId(pollutant), ProcessId(process))
    }

    /// `sourceTypePolProcess` row helper.
    fn stpp(source_type: u16, pol_process: PolProcessId) -> SourceTypePolProcess {
        SourceTypePolProcess {
            source_type_id: SourceTypeId(source_type),
            pol_process_id: pol_process,
        }
    }

    /// `opModePolProcAssoc` row helper.
    fn omppa(pol_process: PolProcessId, op_mode: i16) -> OpModePolProcAssoc {
        OpModePolProcAssoc {
            pol_process_id: pol_process,
            op_mode_id: op_mode,
        }
    }

    #[test]
    fn extended_idle_emits_assoc_op_modes_crossed_with_hour_days() {
        // One process-90 polProcess modeled for source type 62, with two
        // operating modes, crossed with two hour/day combinations.
        let pp = polproc(91, 90);
        let inputs = OpModeFractionInputs {
            pollutant_process_assoc: &[ppa(91, 90)],
            source_type_pol_process: &[stpp(62, pp)],
            op_mode_pol_proc_assoc: &[omppa(pp, 200), omppa(pp, 201)],
            run_spec_hour_day: &[51, 52],
            run_spec_source_type: &[SourceTypeId(62)],
            hotelling_op_modes: &[],
        };
        let rows = extended_idle_op_mode_fractions(&inputs);
        // 2 op modes × 2 hour/days = 4 rows; SQL 2's op-mode-200 rows
        // collide with SQL 1's and are ignored.
        assert_eq!(rows.len(), 4);
        for r in &rows {
            assert_eq!(r.source_type_id, SourceTypeId(62));
            assert_eq!(r.road_type_id, RoadTypeId(1));
            assert_eq!(r.avg_speed_bin_id, 0);
            assert_eq!(r.avg_bin_speed, 0.0);
            assert_eq!(r.pol_process_id, pp);
            assert_eq!(r.op_mode_fraction, 1.0);
        }
        // Output is primary-key sorted, and hourDayID precedes opModeID in
        // the key — so the op modes alternate per hour/day, not per mode.
        let op_modes: Vec<i16> = rows.iter().map(|r| r.op_mode_id).collect();
        assert_eq!(op_modes, vec![200, 201, 200, 201]);
        let hour_days: Vec<i16> = rows.iter().map(|r| r.hour_day_id).collect();
        assert_eq!(hour_days, vec![51, 51, 52, 52]);
    }

    #[test]
    fn extended_idle_sql2_adds_op_mode_200_when_assoc_lacks_it() {
        // opModePolProcAssoc only lists op-mode 150; SQL 2 must still add
        // an op-mode-200 row for the process-90 polProcess.
        let pp = polproc(91, 90);
        let inputs = OpModeFractionInputs {
            pollutant_process_assoc: &[ppa(91, 90)],
            source_type_pol_process: &[stpp(62, pp)],
            op_mode_pol_proc_assoc: &[omppa(pp, 150)],
            run_spec_hour_day: &[51],
            run_spec_source_type: &[SourceTypeId(62)],
            hotelling_op_modes: &[],
        };
        let rows = extended_idle_op_mode_fractions(&inputs);
        let op_modes: HashSet<i16> = rows.iter().map(|r| r.op_mode_id).collect();
        assert_eq!(op_modes, HashSet::from([150, 200]));
    }

    #[test]
    fn extended_idle_sql2_skipped_when_source_type_62_not_selected() {
        // Source type 62 absent from runSpecSourceType: SQL 2 produces
        // nothing, leaving only SQL 1's assoc-driven rows.
        let pp = polproc(91, 90);
        let inputs = OpModeFractionInputs {
            pollutant_process_assoc: &[ppa(91, 90)],
            source_type_pol_process: &[stpp(62, pp)],
            op_mode_pol_proc_assoc: &[omppa(pp, 201)],
            run_spec_hour_day: &[51],
            run_spec_source_type: &[SourceTypeId(21)],
            hotelling_op_modes: &[],
        };
        let rows = extended_idle_op_mode_fractions(&inputs);
        let op_modes: Vec<i16> = rows.iter().map(|r| r.op_mode_id).collect();
        assert_eq!(op_modes, vec![201]);
    }

    #[test]
    fn extended_idle_empty_without_any_source_type_62() {
        // No (62, ·) rows in sourceTypePolProcess and 62 not selected:
        // both statements produce nothing.
        let pp = polproc(91, 90);
        let inputs = OpModeFractionInputs {
            pollutant_process_assoc: &[ppa(91, 90)],
            source_type_pol_process: &[stpp(21, pp)],
            op_mode_pol_proc_assoc: &[omppa(pp, 201)],
            run_spec_hour_day: &[51],
            run_spec_source_type: &[SourceTypeId(21)],
            hotelling_op_modes: &[],
        };
        assert!(extended_idle_op_mode_fractions(&inputs).is_empty());
    }

    #[test]
    fn extended_idle_insert_ignore_collapses_op_mode_200_collision() {
        // opModePolProcAssoc lists op-mode 200, and SQL 2 also emits a
        // 200 row: the single output row proves INSERT IGNORE de-dups.
        let pp = polproc(91, 90);
        let inputs = OpModeFractionInputs {
            pollutant_process_assoc: &[ppa(91, 90)],
            source_type_pol_process: &[stpp(62, pp)],
            op_mode_pol_proc_assoc: &[omppa(pp, 200)],
            run_spec_hour_day: &[51],
            run_spec_source_type: &[SourceTypeId(62)],
            hotelling_op_modes: &[],
        };
        let rows = extended_idle_op_mode_fractions(&inputs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].op_mode_id, 200);
    }

    #[test]
    fn extended_idle_skips_polproc_not_modeled_for_source_type_62() {
        // sourceTypePolProcess models the polProcess for source type 21,
        // not 62: SQL 1 contributes nothing, only SQL 2's op-mode 200.
        let pp = polproc(91, 90);
        let inputs = OpModeFractionInputs {
            pollutant_process_assoc: &[ppa(91, 90)],
            source_type_pol_process: &[stpp(21, pp)],
            op_mode_pol_proc_assoc: &[omppa(pp, 201)],
            run_spec_hour_day: &[51],
            run_spec_source_type: &[SourceTypeId(62)],
            hotelling_op_modes: &[],
        };
        let rows = extended_idle_op_mode_fractions(&inputs);
        let op_modes: Vec<i16> = rows.iter().map(|r| r.op_mode_id).collect();
        assert_eq!(op_modes, vec![200]);
    }

    #[test]
    fn auxiliary_power_emits_hotelling_op_modes_excluding_200() {
        let pp = polproc(91, 91);
        let inputs = OpModeFractionInputs {
            pollutant_process_assoc: &[ppa(91, 91)],
            source_type_pol_process: &[stpp(62, pp)],
            op_mode_pol_proc_assoc: &[],
            run_spec_hour_day: &[51],
            run_spec_source_type: &[SourceTypeId(62)],
            hotelling_op_modes: &[200, 201, 203],
        };
        let rows = auxiliary_power_op_mode_fractions(&inputs);
        let op_modes: Vec<i16> = rows.iter().map(|r| r.op_mode_id).collect();
        // op-mode 200 (extended idling) is excluded from Auxiliary Power.
        assert_eq!(op_modes, vec![201, 203]);
        for r in &rows {
            assert_eq!(r.source_type_id, SourceTypeId(62));
            assert_eq!(r.op_mode_fraction, 1.0);
        }
    }

    #[test]
    fn auxiliary_power_sql1_excludes_op_mode_200() {
        // opModePolProcAssoc lists op-mode 200; step 210 SQL 1 must drop it.
        let pp = polproc(91, 91);
        let inputs = OpModeFractionInputs {
            pollutant_process_assoc: &[ppa(91, 91)],
            source_type_pol_process: &[stpp(62, pp)],
            op_mode_pol_proc_assoc: &[omppa(pp, 200), omppa(pp, 201)],
            run_spec_hour_day: &[51],
            run_spec_source_type: &[SourceTypeId(21)],
            hotelling_op_modes: &[],
        };
        let rows = auxiliary_power_op_mode_fractions(&inputs);
        let op_modes: Vec<i16> = rows.iter().map(|r| r.op_mode_id).collect();
        assert_eq!(op_modes, vec![201]);
    }

    #[test]
    fn auxiliary_power_empty_without_source_type_62() {
        let pp = polproc(91, 91);
        let inputs = OpModeFractionInputs {
            pollutant_process_assoc: &[ppa(91, 91)],
            source_type_pol_process: &[stpp(21, pp)],
            op_mode_pol_proc_assoc: &[omppa(pp, 201)],
            run_spec_hour_day: &[51],
            run_spec_source_type: &[SourceTypeId(21)],
            hotelling_op_modes: &[201, 203],
        };
        assert!(auxiliary_power_op_mode_fractions(&inputs).is_empty());
    }

    #[test]
    fn auxiliary_power_distinct_collapses_repeated_hotelling_op_modes() {
        // hotellingActivityDistribution repeats op-mode 201; the result
        // has a single 201 row per (polProcess, hourDay) — Java SELECT
        // DISTINCT / primary-key de-duplication.
        let inputs = OpModeFractionInputs {
            pollutant_process_assoc: &[ppa(91, 91)],
            source_type_pol_process: &[],
            op_mode_pol_proc_assoc: &[],
            run_spec_hour_day: &[51],
            run_spec_source_type: &[SourceTypeId(62)],
            hotelling_op_modes: &[201, 201, 201],
        };
        let rows = auxiliary_power_op_mode_fractions(&inputs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].op_mode_id, 201);
    }

    #[test]
    fn op_mode_fractions_dispatches_by_process() {
        let pp90 = polproc(91, 90);
        let pp91 = polproc(91, 91);
        let inputs = OpModeFractionInputs {
            pollutant_process_assoc: &[ppa(91, 90), ppa(91, 91)],
            source_type_pol_process: &[stpp(62, pp90), stpp(62, pp91)],
            op_mode_pol_proc_assoc: &[omppa(pp90, 201), omppa(pp91, 201)],
            run_spec_hour_day: &[51],
            run_spec_source_type: &[SourceTypeId(62)],
            hotelling_op_modes: &[203],
        };
        let gen = RatesOperatingModeDistributionGenerator::new();

        assert_eq!(
            gen.op_mode_fractions(ProcessId(90), &inputs),
            extended_idle_op_mode_fractions(&inputs),
        );
        assert_eq!(
            gen.op_mode_fractions(ProcessId(91), &inputs),
            auxiliary_power_op_mode_fractions(&inputs),
        );
        // Running Exhaust and unrelated processes contribute no rows here.
        assert!(gen.op_mode_fractions(ProcessId(1), &inputs).is_empty());
        assert!(gen.op_mode_fractions(ProcessId(2), &inputs).is_empty());
    }

    #[test]
    fn output_is_sorted_by_primary_key() {
        // Inputs deliberately out of key order; output must be sorted.
        let pp = polproc(91, 90);
        let inputs = OpModeFractionInputs {
            pollutant_process_assoc: &[ppa(91, 90)],
            source_type_pol_process: &[stpp(62, pp)],
            op_mode_pol_proc_assoc: &[omppa(pp, 203), omppa(pp, 201)],
            run_spec_hour_day: &[52, 51],
            run_spec_source_type: &[SourceTypeId(62)],
            hotelling_op_modes: &[],
        };
        let rows = extended_idle_op_mode_fractions(&inputs);
        let keys: Vec<RowKey> = rows.iter().map(RatesOpModeDistributionRow::key).collect();
        let mut sorted = keys.clone();
        sorted.sort_unstable();
        assert_eq!(keys, sorted);
    }

    #[test]
    fn generator_metadata_matches_java_subscribe_to_me() {
        let gen = RatesOperatingModeDistributionGenerator::new();
        assert_eq!(gen.name(), "RatesOperatingModeDistributionGenerator");
        assert_eq!(gen.output_tables(), &["RatesOpModeDistribution"]);
        assert_eq!(gen.upstream(), &["SourceTypePhysics"]);
        assert!(gen
            .input_tables()
            .contains(&"hotellingActivityDistribution"));

        let subs = gen.subscriptions();
        assert_eq!(subs.len(), 3);
        let processes: Vec<ProcessId> = subs.iter().map(|s| s.process_id).collect();
        assert_eq!(processes, vec![ProcessId(1), ProcessId(90), ProcessId(91)]);
        for s in subs {
            assert_eq!(s.granularity, Granularity::Year);
            assert_eq!(s.priority.display(), "GENERATOR");
        }
    }

    #[test]
    fn execute_writes_rates_op_mode_distribution_to_scratch() {
        use moves_framework::{
            DataFrameStore, DataFrameStoreTyped, ExecutionLocation, ExecutionTime, InMemoryStore,
            IterationPosition,
        };

        // One process-90 (Extended Idle) polProcess modeled for source type 62,
        // with op-mode 201 in opModePolProcAssoc and one hour/day (51).
        let pp = polproc(91, 90);

        let mut store = InMemoryStore::default();

        // pollutantProcessAssoc
        store.insert(
            "pollutantProcessAssoc",
            RatesPollutantProcessAssocRow::into_dataframe(vec![RatesPollutantProcessAssocRow {
                pollutant_id: 91,
                process_id: 90,
            }])
            .unwrap(),
        );
        // sourceTypePolProcess
        store.insert(
            "sourceTypePolProcess",
            RatesSourceTypePolProcessRow::into_dataframe(vec![RatesSourceTypePolProcessRow {
                source_type_id: 62,
                pol_process_id: pp.0,
            }])
            .unwrap(),
        );
        // opModePolProcAssoc
        store.insert(
            "opModePolProcAssoc",
            RatesOpModePolProcAssocRow::into_dataframe(vec![RatesOpModePolProcAssocRow {
                pol_process_id: pp.0,
                op_mode_id: 201,
            }])
            .unwrap(),
        );
        // runSpecHourDay
        store.insert(
            "runSpecHourDay",
            RatesRunSpecHourDayRow::into_dataframe(vec![RatesRunSpecHourDayRow {
                hour_day_id: 51,
            }])
            .unwrap(),
        );
        // runSpecSourceType: source type 62 is selected.
        store.insert(
            "runSpecSourceType",
            RatesRunSpecSourceTypeRow::into_dataframe(vec![RatesRunSpecSourceTypeRow {
                source_type_id: 62,
            }])
            .unwrap(),
        );
        // hotellingActivityDistribution: empty (not used by process 90).
        store.insert(
            "hotellingActivityDistribution",
            RatesHotellingActivityDistributionRow::into_dataframe(vec![]).unwrap(),
        );

        let position = IterationPosition {
            iteration: 0,
            process_id: Some(ProcessId(90)),
            location: ExecutionLocation::link(1, 1, 1, 1),
            time: ExecutionTime {
                year: Some(2020),
                month: None,
                day_id: None,
                hour: None,
            },
        };

        let gen = RatesOperatingModeDistributionGenerator::new();
        let mut ctx = CalculatorContext::with_position_and_tables(position, store);
        gen.execute(&mut ctx).unwrap();

        let out: Vec<RatesOpModeDistributionRow> = ctx
            .scratch()
            .store
            .iter_typed("RatesOpModeDistribution")
            .unwrap();

        // Extended idle (process 90): SQL 1 emits op-mode 201 for hour-day 51;
        // SQL 2 emits op-mode 200 (the guarantee). INSERT IGNORE de-duplication
        // leaves two distinct rows.
        assert_eq!(out.len(), 2, "expected op-mode 200 and 201 rows");
        for r in &out {
            assert_eq!(r.source_type_id, SourceTypeId(62));
            assert_eq!(r.road_type_id, RoadTypeId(1));
            assert_eq!(r.avg_speed_bin_id, 0);
            assert_eq!(r.avg_bin_speed, 0.0);
            assert_eq!(r.pol_process_id, pp);
            assert_eq!(r.hour_day_id, 51);
            assert_eq!(r.op_mode_fraction, 1.0);
        }
        let op_modes: HashSet<i16> = out.iter().map(|r| r.op_mode_id).collect();
        assert_eq!(op_modes, HashSet::from([200, 201]));
    }

    #[test]
    fn generator_is_object_safe() {
        // The registry stores generators as Box<dyn Generator>.
        let gen: Box<dyn Generator> = Box::new(RatesOperatingModeDistributionGenerator::new());
        assert_eq!(gen.name(), "RatesOperatingModeDistributionGenerator");
    }
}

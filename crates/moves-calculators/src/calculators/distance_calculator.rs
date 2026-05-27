//! Port of `DistanceCalculator.java` and `database/DistanceCalculator.sql` —
//! migration plan Phase 3, Task 72.
//!
//! `DistanceCalculator` produces the **distance** activity output MOVES
//! writes when a RunSpec requests distance in inventory mode. It is the
//! first calculator ported into [`crate::calculators`].
//!
//! # What it computes
//!
//! Distance is `SHO.distance` — the vehicle distance travelled, already
//! summed across source bins by the Total Activity Generator — split onto
//! the `(regClassID, fuelTypeID)` dimensions the output table carries:
//!
//! ```text
//! activity = SHO.distance × fuelTypeActivityFraction
//! ```
//!
//! `fuelTypeActivityFraction[sourceTypeID, modelYearID, regClassID,
//! fuelTypeID]` is the share of a `(sourceType, modelYear)` group's
//! running-exhaust activity that falls in a given `(regClass, fuelType)` —
//! the source-bin activity fractions summed over the engine-technology,
//! engine-size and model-year-group sub-dimensions of the source bin.
//!
//! # Algorithm
//!
//! [`DistanceCalculator::calculate`] ports the SQL's "Processing" section.
//! The SQL builds seven MyISAM working tables; the port folds them into two
//! index maps and one join loop:
//!
//! | SQL working table | This port |
//! |-------------------|-----------|
//! | `SBD2` | `(sourceTypeModelYearID, regClassID, fuelTypeID) → Σ sourceBinActivityFraction` |
//! | `DistFracts` | `(sourceTypeID, modelYearID) → [(regClassID, fuelTypeID, fraction)]` |
//! | `SHO2`, `Link2`, `SHO3` | folded into the final per-`SHO`-row join |
//! | `MOVESWorkerActivityOutput` | the returned `Vec<`[`DistanceActivityRow`]`>` |
//!
//! Every join in the SQL is an `INNER JOIN`, so a row with no match on the
//! join key is dropped; the port reproduces that with map lookups that skip
//! on a miss.
//!
//! # Scope of this port
//!
//! [`calculate`](DistanceCalculator::calculate) is the SQL "Processing"
//! section. The Java `doExecute` wrapper that *generates* that SQL is
//! execution wiring, not algorithm:
//!
//! * It picks one Running Exhaust `polProcessID` (`… WHERE ppa.processID = 1
//!   LIMIT 1`) to filter `SourceBinDistribution`. The source-bin activity
//!   fraction is identical across the pollutants of a process, so any one
//!   does; [`calculate`](DistanceCalculator::calculate) reproduces this by
//!   selecting the smallest Running Exhaust `polProcessID` present.
//! * It toggles the `SCCOutput` / `NoSCCOutput` SQL sections from the
//!   RunSpec's on-road-SCC flag. `MOVESWorkerActivityOutput.SCC` is not an
//!   algorithm input; the SCC column is left to the Task 50 output wiring.
//!
//! # The off-network road type
//!
//! `DistanceCalculator` implements `MasterLoopContext.IContextFilter`: its
//! `doesProcessContext` returns `false` for the off-network road type
//! (`roadTypeID == 1`), so the master loop never runs the calculator there.
//! Off-network "links" model parking, not travel, and carry no distance.
//! [`DistanceCalculator::processes_road_type`] is that predicate;
//! [`calculate`](DistanceCalculator::calculate) also drops off-network
//! `SHO` rows directly, so the pure compute is correct for any input.
//!
//! # Fidelity notes
//!
//! `DistanceCalculator.sql` writes `fuelTypeActivityFraction` to a `FLOAT`
//! (32-bit) `SBD2` column and the final `activity` to the `FLOAT`
//! `MOVESWorkerActivityOutput.activity` column, while MariaDB evaluates the
//! arithmetic in `DOUBLE`. This port sums and multiplies in `f64` end to
//! end, so it does not reproduce the `f32` truncation MOVES applies when it
//! stores those two values — a sub-`1e-7` relative drift. Reproducing it
//! bug-for-bug is the Task 73/74 calculator-integration-validation call,
//! matching the Task 41 / Task 33 precedent. `SHO.distance` is likewise a
//! `FLOAT` column, but it is a model *input* — already `f32`-quantised
//! before [`calculate`](DistanceCalculator::calculate) sees it. There are no
//! integer/integer literal divisions in the SQL, so the MariaDB
//! `div_precision_increment` rounding gotcha does not arise.
//!
//! # Data plane (Task 50)
//!
//! [`Calculator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are Phase 2 placeholders until the
//! `DataFrameStore` lands (migration-plan Task 50), so `execute` cannot yet
//! read the input tables nor emit `MOVESWorkerActivityOutput`. The numeric
//! algorithm is fully ported and unit-tested on
//! [`calculate`](DistanceCalculator::calculate); `execute` is a documented
//! shell returning an empty [`CalculatorOutput`]. Once the data plane
//! exists, `execute` materialises a [`DistanceInputs`] from `ctx.tables()`,
//! calls [`calculate`](DistanceCalculator::calculate), and writes the rows
//! to the activity output.

use std::collections::HashMap;

use moves_calculator_info::{Granularity, Priority};
use moves_data::{PollutantProcessAssociation, ProcessId};
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped,
    Error, IntoDataFrame, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

/// Stable module name — matches the Java class and the `DistanceCalculator`
/// entry in the calculator-chain DAG (`calculator-dag.json`).
const CALCULATOR_NAME: &str = "DistanceCalculator";

/// Running Exhaust process. `DistanceCalculator` subscribes to it, and a
/// `polProcessID` (`pollutantID * 100 + processID`) belongs to it when
/// `polProcessID % 100` equals [`RUNNING_EXHAUST.0`](ProcessId).
const RUNNING_EXHAUST: ProcessId = ProcessId(1);

/// Off-network road type — `RoadType` row 1, `"Off-Network"`. The Java
/// `doesProcessContext` skips it; off-network links carry no travel.
const OFF_NETWORK_ROAD_TYPE_ID: i32 = 1;

/// `activityTypeID` for distance in `MOVESWorkerActivityOutput` — the
/// `ActivityType` "Distance Traveled" row. Every [`DistanceActivityRow`]
/// carries this activity type; it is a constant, so it is not stored per
/// row. The Task 50 output wiring writes it into the `activityTypeID`
/// column.
pub const DISTANCE_ACTIVITY_TYPE_ID: i32 = 1;

// ===========================================================================
// Input tables — plain Rust mirrors of the tables `DistanceCalculator.sql`'s
// "Extract Data" section pulls. Following the Phase 3 convention, every
// `INT`/`SMALLINT` identifier is an `i32`, `sourceBinID` (`BIGINT`) is an
// `i64`, and every `FLOAT`/`DOUBLE` quantity is an `f64`. Only the columns
// the distance algorithm reads are modelled.
// ===========================================================================

/// One `SourceBin` row — the engine/fuel/regulatory-class decomposition of a
/// source bin.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceBinRow {
    /// `sourceBinID` — `BIGINT` primary key.
    pub source_bin_id: i64,
    /// `regClassID` — regulatory class. Schema-nullable, but always
    /// populated for the onroad running-exhaust bins this calculator sees.
    pub reg_class_id: i32,
    /// `fuelTypeID` — fuel type.
    pub fuel_type_id: i32,
}

/// One `SourceBinDistribution` row — a source bin's share of a
/// `(sourceTypeModelYear)` group's activity for one `polProcessID`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceBinDistributionRow {
    /// `sourceTypeModelYearID` — surrogate key for a `(sourceType, modelYear)`.
    pub source_type_model_year_id: i32,
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `sourceBinID` — joins to [`SourceBinRow::source_bin_id`].
    pub source_bin_id: i64,
    /// `sourceBinActivityFraction` — the bin's share of the group's activity.
    pub source_bin_activity_fraction: f64,
}

/// One `SourceTypeModelYear` row — resolves a `sourceTypeModelYearID`
/// surrogate key into its `(sourceTypeID, modelYearID)` components.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypeModelYearRow {
    /// `sourceTypeModelYearID` — the surrogate key.
    pub source_type_model_year_id: i32,
    /// `sourceTypeID` — MOVES source (vehicle) type.
    pub source_type_id: i32,
    /// `modelYearID` — vehicle model year.
    pub model_year_id: i32,
}

/// One `SHO` (Source Hours Operating) row — per `(hourDay, month, year,
/// age, link, sourceType)` travelled distance.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShoRow {
    /// `hourDayID` — joins to [`HourDayRow::hour_day_id`].
    pub hour_day_id: i32,
    /// `monthID` — calendar month.
    pub month_id: i32,
    /// `yearID` — calendar year.
    pub year_id: i32,
    /// `ageID` — vehicle age in years; `modelYearID = yearID - ageID`.
    pub age_id: i32,
    /// `linkID` — joins to [`LinkRow::link_id`].
    pub link_id: i32,
    /// `sourceTypeID` — MOVES source (vehicle) type.
    pub source_type_id: i32,
    /// `distance` — vehicle distance travelled. `FLOAT` in MOVES.
    pub distance: f64,
}

/// One `HourDay` row — the `hourDayID` → `(dayID, hourID)` split.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HourDayRow {
    /// `hourDayID` — the surrogate key.
    pub hour_day_id: i32,
    /// `dayID` — day-of-week type.
    pub day_id: i32,
    /// `hourID` — hour of day.
    pub hour_id: i32,
}

/// One `Link` row — a road link's geography and road type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LinkRow {
    /// `linkID` — the link primary key.
    pub link_id: i32,
    /// `countyID` — joins to [`CountyRow::county_id`].
    pub county_id: i32,
    /// `zoneID`. Schema-nullable; populated for the onroad links here.
    pub zone_id: i32,
    /// `roadTypeID` — road type; `1` is off-network (see
    /// [`DistanceCalculator::processes_road_type`]).
    pub road_type_id: i32,
}

/// One `County` row — supplies the `stateID` for a county.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CountyRow {
    /// `countyID` — the county primary key.
    pub county_id: i32,
    /// `stateID` — the state the county belongs to.
    pub state_id: i32,
}

/// Inputs to [`DistanceCalculator::calculate`] — the extracted tables the
/// SQL's "Extract Data" section produces, as plain row vectors.
///
/// A future Task 50 (`DataFrameStore`) wiring populates this from the
/// per-run filtered execution database; until then it is the explicit
/// data-plane contract the unit tests build directly.
#[derive(Debug, Clone, Default)]
pub struct DistanceInputs {
    /// `SourceBin` rows.
    pub source_bin: Vec<SourceBinRow>,
    /// `SourceBinDistribution` rows. May carry several `polProcessID`s;
    /// [`DistanceCalculator::calculate`] selects the Running Exhaust ones.
    pub source_bin_distribution: Vec<SourceBinDistributionRow>,
    /// `SourceTypeModelYear` rows.
    pub source_type_model_year: Vec<SourceTypeModelYearRow>,
    /// `SHO` rows.
    pub sho: Vec<ShoRow>,
    /// `HourDay` rows.
    pub hour_day: Vec<HourDayRow>,
    /// `Link` rows.
    pub link: Vec<LinkRow>,
    /// `County` rows.
    pub county: Vec<CountyRow>,
}

/// One `MOVESWorkerActivityOutput` row produced by the distance calculation.
///
/// `activityTypeID` is always [`DISTANCE_ACTIVITY_TYPE_ID`] and so is not
/// stored per row; `activity` carries the distance value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DistanceActivityRow {
    /// `yearID`.
    pub year_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `dayID`.
    pub day_id: i32,
    /// `hourID`.
    pub hour_id: i32,
    /// `stateID`.
    pub state_id: i32,
    /// `countyID`.
    pub county_id: i32,
    /// `zoneID`.
    pub zone_id: i32,
    /// `linkID`.
    pub link_id: i32,
    /// `regClassID`.
    pub reg_class_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `modelYearID`.
    pub model_year_id: i32,
    /// `roadTypeID`.
    pub road_type_id: i32,
    /// `activity` — distance, `SHO.distance × fuelTypeActivityFraction`.
    pub activity: f64,
}

impl DistanceActivityRow {
    /// The integer dimension tuple — every column except `activity`. Used to
    /// sort the output deterministically: MOVES leaves
    /// `MOVESWorkerActivityOutput` physically unordered (the SQL `INSERT …
    /// SELECT` has no `ORDER BY`), so the port sorts purely to make the
    /// result reproducible.
    fn dimension_key(&self) -> [i32; 13] {
        [
            self.year_id,
            self.month_id,
            self.day_id,
            self.hour_id,
            self.state_id,
            self.county_id,
            self.zone_id,
            self.link_id,
            self.reg_class_id,
            self.source_type_id,
            self.fuel_type_id,
            self.model_year_id,
            self.road_type_id,
        ]
    }
}

/// `DistFracts` indexed for the final join: each `(sourceTypeID,
/// modelYearID)` maps to its rows, a `(regClassID, fuelTypeID,
/// fuelTypeActivityFraction)` triple apiece. Built by joining `SBD2` to
/// `SourceTypeModelYear`; consumed by the per-`SHO`-row join.
type DistFractsIndex = HashMap<(i32, i32), Vec<(i32, i32, f64)>>;

// ===========================================================================
// TableRow implementations — typed DataFrame ↔ row round-trips for every
// table `DistanceCalculator` reads and for the output row type.
// ===========================================================================

/// Helper: build an `Error::RowExtraction` for a missing / wrong-type column.
fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::RowExtraction {
        table: table.into(),
        row,
        column: column.into(),
        message: msg,
    }
}

impl TableRow for ShoRow {
    fn table_name() -> &'static str {
        "SHO"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("linkID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("distance".into(), DataType::Float64),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "hourDayID".into(),
                    rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "linkID".into(),
                    rows.iter().map(|r| r.link_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "distance".into(),
                    rows.iter().map(|r| r.distance).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SHO";
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
        let hour_day = get_i32("hourDayID")?;
        let month = get_i32("monthID")?;
        let year = get_i32("yearID")?;
        let age = get_i32("ageID")?;
        let link = get_i32("linkID")?;
        let src_type = get_i32("sourceTypeID")?;
        let dist = get_f64("distance")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(ShoRow {
                    hour_day_id: hour_day.get(i).ok_or_else(|| null("hourDayID"))?,
                    month_id: month.get(i).ok_or_else(|| null("monthID"))?,
                    year_id: year.get(i).ok_or_else(|| null("yearID"))?,
                    age_id: age.get(i).ok_or_else(|| null("ageID"))?,
                    link_id: link.get(i).ok_or_else(|| null("linkID"))?,
                    source_type_id: src_type.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    distance: dist.get(i).ok_or_else(|| null("distance"))?,
                })
            })
            .collect()
    }
}

impl TableRow for SourceBinRow {
    fn table_name() -> &'static str {
        "SourceBin"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceBinID".into(), DataType::Int64),
            ("regClassID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceBinID".into(),
                    rows.iter().map(|r| r.source_bin_id).collect::<Vec<i64>>(),
                )
                .into(),
                Series::new(
                    "regClassID".into(),
                    rows.iter().map(|r| r.reg_class_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SourceBin";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_i64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let src_bin = get_i64("sourceBinID")?;
        let reg_class = get_i32("regClassID")?;
        let fuel_type = get_i32("fuelTypeID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SourceBinRow {
                    source_bin_id: src_bin.get(i).ok_or_else(|| null("sourceBinID"))?,
                    reg_class_id: reg_class.get(i).ok_or_else(|| null("regClassID"))?,
                    fuel_type_id: fuel_type.get(i).ok_or_else(|| null("fuelTypeID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for SourceBinDistributionRow {
    fn table_name() -> &'static str {
        "SourceBinDistribution"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeModelYearID".into(), DataType::Int32),
            ("polProcessID".into(), DataType::Int32),
            ("sourceBinID".into(), DataType::Int64),
            ("sourceBinActivityFraction".into(), DataType::Float64),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeModelYearID".into(),
                    rows.iter()
                        .map(|r| r.source_type_model_year_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "polProcessID".into(),
                    rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceBinID".into(),
                    rows.iter().map(|r| r.source_bin_id).collect::<Vec<i64>>(),
                )
                .into(),
                Series::new(
                    "sourceBinActivityFraction".into(),
                    rows.iter()
                        .map(|r| r.source_bin_activity_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SourceBinDistribution";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_i64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let stmy = get_i32("sourceTypeModelYearID")?;
        let pol_proc = get_i32("polProcessID")?;
        let src_bin = get_i64("sourceBinID")?;
        let frac = get_f64("sourceBinActivityFraction")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SourceBinDistributionRow {
                    source_type_model_year_id: stmy
                        .get(i)
                        .ok_or_else(|| null("sourceTypeModelYearID"))?,
                    pol_process_id: pol_proc.get(i).ok_or_else(|| null("polProcessID"))?,
                    source_bin_id: src_bin.get(i).ok_or_else(|| null("sourceBinID"))?,
                    source_bin_activity_fraction: frac
                        .get(i)
                        .ok_or_else(|| null("sourceBinActivityFraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for SourceTypeModelYearRow {
    fn table_name() -> &'static str {
        "SourceTypeModelYear"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeModelYearID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeModelYearID".into(),
                    rows.iter()
                        .map(|r| r.source_type_model_year_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "modelYearID".into(),
                    rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SourceTypeModelYear";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let stmy = get_i32("sourceTypeModelYearID")?;
        let src_type = get_i32("sourceTypeID")?;
        let model_year = get_i32("modelYearID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SourceTypeModelYearRow {
                    source_type_model_year_id: stmy
                        .get(i)
                        .ok_or_else(|| null("sourceTypeModelYearID"))?,
                    source_type_id: src_type.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    model_year_id: model_year.get(i).ok_or_else(|| null("modelYearID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for HourDayRow {
    fn table_name() -> &'static str {
        "HourDay"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "hourDayID".into(),
                    rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "dayID".into(),
                    rows.iter().map(|r| r.day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourID".into(),
                    rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "HourDay";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let hour_day = get_i32("hourDayID")?;
        let day = get_i32("dayID")?;
        let hour = get_i32("hourID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(HourDayRow {
                    hour_day_id: hour_day.get(i).ok_or_else(|| null("hourDayID"))?,
                    day_id: day.get(i).ok_or_else(|| null("dayID"))?,
                    hour_id: hour.get(i).ok_or_else(|| null("hourID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for LinkRow {
    fn table_name() -> &'static str {
        "Link"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("linkID".into(), DataType::Int32),
            ("countyID".into(), DataType::Int32),
            ("zoneID".into(), DataType::Int32),
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
                    "countyID".into(),
                    rows.iter().map(|r| r.county_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "zoneID".into(),
                    rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>(),
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
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let link = get_i32("linkID")?;
        let county = get_i32("countyID")?;
        let zone = get_i32("zoneID")?;
        let road_type = get_i32("roadTypeID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(LinkRow {
                    link_id: link.get(i).ok_or_else(|| null("linkID"))?,
                    county_id: county.get(i).ok_or_else(|| null("countyID"))?,
                    zone_id: zone.get(i).ok_or_else(|| null("zoneID"))?,
                    road_type_id: road_type.get(i).ok_or_else(|| null("roadTypeID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for CountyRow {
    fn table_name() -> &'static str {
        "County"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("countyID".into(), DataType::Int32),
            ("stateID".into(), DataType::Int32),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "countyID".into(),
                    rows.iter().map(|r| r.county_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "stateID".into(),
                    rows.iter().map(|r| r.state_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "County";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let county = get_i32("countyID")?;
        let state = get_i32("stateID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(CountyRow {
                    county_id: county.get(i).ok_or_else(|| null("countyID"))?,
                    state_id: state.get(i).ok_or_else(|| null("stateID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for DistanceActivityRow {
    fn table_name() -> &'static str {
        "MOVESWorkerActivityOutput"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("yearID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
            ("stateID".into(), DataType::Int32),
            ("countyID".into(), DataType::Int32),
            ("zoneID".into(), DataType::Int32),
            ("linkID".into(), DataType::Int32),
            ("regClassID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("activity".into(), DataType::Float64),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "dayID".into(),
                    rows.iter().map(|r| r.day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourID".into(),
                    rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "stateID".into(),
                    rows.iter().map(|r| r.state_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "countyID".into(),
                    rows.iter().map(|r| r.county_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "zoneID".into(),
                    rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "linkID".into(),
                    rows.iter().map(|r| r.link_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "regClassID".into(),
                    rows.iter().map(|r| r.reg_class_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "modelYearID".into(),
                    rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeID".into(),
                    rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "activity".into(),
                    rows.iter().map(|r| r.activity).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "MOVESWorkerActivityOutput";
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
        let year = get_i32("yearID")?;
        let month = get_i32("monthID")?;
        let day = get_i32("dayID")?;
        let hour = get_i32("hourID")?;
        let state = get_i32("stateID")?;
        let county = get_i32("countyID")?;
        let zone = get_i32("zoneID")?;
        let link = get_i32("linkID")?;
        let reg_class = get_i32("regClassID")?;
        let src_type = get_i32("sourceTypeID")?;
        let fuel_type = get_i32("fuelTypeID")?;
        let model_year = get_i32("modelYearID")?;
        let road_type = get_i32("roadTypeID")?;
        let activity = get_f64("activity")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(DistanceActivityRow {
                    year_id: year.get(i).ok_or_else(|| null("yearID"))?,
                    month_id: month.get(i).ok_or_else(|| null("monthID"))?,
                    day_id: day.get(i).ok_or_else(|| null("dayID"))?,
                    hour_id: hour.get(i).ok_or_else(|| null("hourID"))?,
                    state_id: state.get(i).ok_or_else(|| null("stateID"))?,
                    county_id: county.get(i).ok_or_else(|| null("countyID"))?,
                    zone_id: zone.get(i).ok_or_else(|| null("zoneID"))?,
                    link_id: link.get(i).ok_or_else(|| null("linkID"))?,
                    reg_class_id: reg_class.get(i).ok_or_else(|| null("regClassID"))?,
                    source_type_id: src_type.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    fuel_type_id: fuel_type.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    model_year_id: model_year.get(i).ok_or_else(|| null("modelYearID"))?,
                    road_type_id: road_type.get(i).ok_or_else(|| null("roadTypeID"))?,
                    activity: activity.get(i).ok_or_else(|| null("activity"))?,
                })
            })
            .collect()
    }
}

/// The MOVES distance calculator.
///
/// A small value type: it owns no per-run state — only its master-loop
/// subscription, built once in [`new`](Self::new). All run-varying input
/// flows through the [`DistanceInputs`] argument to
/// [`calculate`](Self::calculate).
#[derive(Debug, Clone)]
pub struct DistanceCalculator {
    /// The single master-loop subscription, built once in [`Self::new`].
    subscriptions: [CalculatorSubscription; 1],
}

impl DistanceCalculator {
    /// Stable module name — matches the Java class and the chain-DAG entry.
    pub const NAME: &'static str = CALCULATOR_NAME;

    /// Construct the calculator with its master-loop subscription.
    ///
    /// `DistanceCalculator.subscribeToMe` signs up for the Running Exhaust
    /// process at `YEAR` granularity with `EMISSION_CALCULATOR` priority
    /// (when the RunSpec requests Running Exhaust); the calculator-chain DAG
    /// records the same single subscription.
    #[must_use]
    pub fn new() -> Self {
        let priority = Priority::parse("EMISSION_CALCULATOR")
            .expect("\"EMISSION_CALCULATOR\" is a valid MasterLoop priority");
        Self {
            subscriptions: [CalculatorSubscription::new(
                RUNNING_EXHAUST,
                Granularity::Year,
                priority,
            )],
        }
    }

    /// Port of `doesProcessContext` — `DistanceCalculator` skips the
    /// off-network road type (`roadTypeID == 1`); off-network "links" model
    /// parking, not travel, and carry no distance.
    ///
    /// [`calculate`](Self::calculate) applies the same exclusion at row
    /// grain (it drops `SHO` rows on off-network links); this predicate is
    /// the master-loop context-filter form the Task 50 `execute` wiring uses
    /// to avoid invoking the calculator for off-network contexts at all.
    #[must_use]
    pub fn processes_road_type(road_type_id: i32) -> bool {
        road_type_id != OFF_NETWORK_ROAD_TYPE_ID
    }

    /// Compute the distance activity rows — the port of the
    /// `DistanceCalculator.sql` "Processing" section.
    ///
    /// Returns no rows when no Running Exhaust `polProcessID` is present in
    /// `inputs.source_bin_distribution`: the Java `doExecute` logs
    /// "Distance calculation requires Running Exhaust" and abandons the
    /// calculation in that case. The result is sorted by its integer
    /// dimension columns for deterministic output — MOVES leaves
    /// `MOVESWorkerActivityOutput` physically unordered.
    #[must_use]
    pub fn calculate(&self, inputs: &DistanceInputs) -> Vec<DistanceActivityRow> {
        // Select the Running Exhaust pollutant/process. The Java runs
        // `… WHERE ppa.processID = 1 LIMIT 1`; the source-bin activity
        // fraction is identical across a process's pollutants, so any one
        // Running Exhaust `polProcessID` gives the same result. Pick the
        // smallest present for determinism.
        let Some(pol_process_id) = inputs
            .source_bin_distribution
            .iter()
            .map(|r| r.pol_process_id)
            .filter(|id| id % 100 == i32::from(RUNNING_EXHAUST.0))
            .min()
        else {
            return Vec::new();
        };

        // SBD2: fuelTypeActivityFraction[sourceTypeModelYearID, regClassID,
        // fuelTypeID] = Σ sourceBinActivityFraction, over the Running
        // Exhaust source-bin-distribution rows joined to `SourceBin`.
        let source_bin: HashMap<i64, &SourceBinRow> = inputs
            .source_bin
            .iter()
            .map(|sb| (sb.source_bin_id, sb))
            .collect();
        let mut sbd2: HashMap<(i32, i32, i32), f64> = HashMap::new();
        for sbd in &inputs.source_bin_distribution {
            if sbd.pol_process_id != pol_process_id {
                continue;
            }
            // INNER JOIN SourceBin USING (sourceBinID).
            let Some(sb) = source_bin.get(&sbd.source_bin_id) else {
                continue;
            };
            *sbd2
                .entry((
                    sbd.source_type_model_year_id,
                    sb.reg_class_id,
                    sb.fuel_type_id,
                ))
                .or_default() += sbd.source_bin_activity_fraction;
        }

        // DistFracts: resolve sourceTypeModelYearID into its (sourceTypeID,
        // modelYearID), keyed for the final join.
        let stmy: HashMap<i32, &SourceTypeModelYearRow> = inputs
            .source_type_model_year
            .iter()
            .map(|r| (r.source_type_model_year_id, r))
            .collect();
        // (sourceTypeID, modelYearID) -> [(regClassID, fuelTypeID, fraction)]
        let mut dist_fracts: DistFractsIndex = HashMap::new();
        for (&(stmy_id, reg_class_id, fuel_type_id), &fraction) in &sbd2 {
            // INNER JOIN SourceTypeModelYear USING (sourceTypeModelYearID).
            let Some(stmy_row) = stmy.get(&stmy_id) else {
                continue;
            };
            dist_fracts
                .entry((stmy_row.source_type_id, stmy_row.model_year_id))
                .or_default()
                .push((reg_class_id, fuel_type_id, fraction));
        }

        // SHO3 join indices: HourDay (for SHO2), Link and County (for Link2).
        let hour_day: HashMap<i32, &HourDayRow> =
            inputs.hour_day.iter().map(|r| (r.hour_day_id, r)).collect();
        let link: HashMap<i32, &LinkRow> = inputs.link.iter().map(|r| (r.link_id, r)).collect();
        let county: HashMap<i32, &CountyRow> =
            inputs.county.iter().map(|r| (r.county_id, r)).collect();

        // Final join: one output row per (SHO3 row, DistFracts row) pair on
        // (sourceTypeID, modelYearID), with activity = distance × fraction.
        let mut out: Vec<DistanceActivityRow> = Vec::new();
        for sho in &inputs.sho {
            // SHO2: INNER JOIN HourDay USING (hourDayID); modelYearID adds
            // a dimension as yearID - ageID.
            let Some(hd) = hour_day.get(&sho.hour_day_id) else {
                continue;
            };
            let model_year_id = sho.year_id - sho.age_id;
            // SHO3: INNER JOIN Link2 (= Link INNER JOIN County USING
            // (countyID)) USING (linkID).
            let Some(link_row) = link.get(&sho.link_id) else {
                continue;
            };
            let Some(county_row) = county.get(&link_row.county_id) else {
                continue;
            };
            // doesProcessContext: off-network links never reach the
            // calculator in MOVES; drop them here so the compute is correct
            // for any input.
            if !Self::processes_road_type(link_row.road_type_id) {
                continue;
            }
            // INNER JOIN DistFracts USING (sourceTypeID, modelYearID).
            let Some(fracts) = dist_fracts.get(&(sho.source_type_id, model_year_id)) else {
                continue;
            };
            for &(reg_class_id, fuel_type_id, fraction) in fracts {
                out.push(DistanceActivityRow {
                    year_id: sho.year_id,
                    month_id: sho.month_id,
                    day_id: hd.day_id,
                    hour_id: hd.hour_id,
                    state_id: county_row.state_id,
                    county_id: link_row.county_id,
                    zone_id: link_row.zone_id,
                    link_id: sho.link_id,
                    reg_class_id,
                    source_type_id: sho.source_type_id,
                    fuel_type_id,
                    model_year_id,
                    road_type_id: link_row.road_type_id,
                    activity: sho.distance * fraction,
                });
            }
        }

        out.sort_unstable_by_key(DistanceActivityRow::dimension_key);
        out
    }
}

impl Default for DistanceCalculator {
    fn default() -> Self {
        Self::new()
    }
}

/// `DistanceCalculator` registers no `(pollutant, process)` pairs.
static NO_REGISTRATIONS: &[PollutantProcessAssociation] = &[];

/// Default-DB / execution-DB tables the distance computation consumes — the
/// data tables the SQL's "Extract Data" section pulls. The SQL also extracts
/// `EmissionProcess` (never read by the "Processing" section) and joins the
/// `RunSpec*` filter tables; neither feeds the algorithm, so neither is
/// listed.
static INPUT_TABLES: &[&str] = &[
    "County",
    "HourDay",
    "Link",
    "SHO",
    "SourceBin",
    "SourceBinDistribution",
    "SourceTypeModelYear",
];

impl Calculator for DistanceCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        &self.subscriptions
    }

    /// `DistanceCalculator` registers no `(pollutant, process)` pairs — the
    /// Java constructor comment: "This calculator doesn't determine
    /// pollutants in any way, so it does not register itself." Its output is
    /// the distance activity table, not an emission tally; the chain DAG
    /// records `registrations_count: 0`.
    fn registrations(&self) -> &[PollutantProcessAssociation] {
        NO_REGISTRATIONS
    }

    // `upstream` keeps the trait default (empty): the chain DAG records no
    // `depends_on` edges. `DistanceCalculator` consumes `SHO` (Total
    // Activity Generator) and `SourceBinDistribution` (Source Bin
    // Distribution Generator), but those run earlier by master-loop priority
    // ordering — `GENERATOR` before `EMISSION_CALCULATOR` — not as chain
    // dependencies.

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    /// Read the seven input tables from `ctx.tables()`, run
    /// [`DistanceCalculator::calculate`], and wrap the result in a
    /// [`CalculatorOutput`] carrying the activity `DataFrame`.
    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let tables = ctx.tables();
        let inputs = DistanceInputs {
            sho: tables.iter_typed::<ShoRow>("SHO")?,
            source_bin: tables.iter_typed::<SourceBinRow>("SourceBin")?,
            source_bin_distribution: tables
                .iter_typed::<SourceBinDistributionRow>("SourceBinDistribution")?,
            source_type_model_year: tables
                .iter_typed::<SourceTypeModelYearRow>("SourceTypeModelYear")?,
            hour_day: tables.iter_typed::<HourDayRow>("HourDay")?,
            link: tables.iter_typed::<LinkRow>("Link")?,
            county: tables.iter_typed::<CountyRow>("County")?,
        };
        let rows = self.calculate(&inputs);
        let df = rows
            .into_dataframe()
            .map_err(|e| Error::Polars(e.to_string()))?;
        Ok(CalculatorOutput::with_dataframe(df))
    }
}

/// Construct the calculator as a boxed trait object — matches the engine's
/// calculator-factory signature so the registry can register it.
#[must_use]
pub fn factory() -> Box<dyn Calculator> {
    Box::new(DistanceCalculator::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a one-bin / one-`SHO` input whose single output row has
    /// `activity == 100.0` (distance 100, fuel-type activity fraction 1).
    /// `sourceTypeModelYearID` follows the MOVES `sourceTypeID * 10000 +
    /// modelYearID` convention (`21 * 10000 + 2018`).
    fn minimal_inputs() -> DistanceInputs {
        DistanceInputs {
            source_bin: vec![SourceBinRow {
                source_bin_id: 1000,
                reg_class_id: 30,
                fuel_type_id: 1,
            }],
            source_bin_distribution: vec![SourceBinDistributionRow {
                source_type_model_year_id: 212_018,
                pol_process_id: 101, // pollutant 1, process 1 (Running Exhaust)
                source_bin_id: 1000,
                source_bin_activity_fraction: 1.0,
            }],
            source_type_model_year: vec![SourceTypeModelYearRow {
                source_type_model_year_id: 212_018,
                source_type_id: 21,
                model_year_id: 2018,
            }],
            sho: vec![ShoRow {
                hour_day_id: 85,
                month_id: 7,
                year_id: 2020,
                age_id: 2, // modelYearID = 2020 - 2 = 2018
                link_id: 5001,
                source_type_id: 21,
                distance: 100.0,
            }],
            hour_day: vec![HourDayRow {
                hour_day_id: 85,
                day_id: 5,
                hour_id: 8,
            }],
            link: vec![LinkRow {
                link_id: 5001,
                county_id: 26_161,
                zone_id: 261_610,
                road_type_id: 4, // urban unrestricted — not off-network
            }],
            county: vec![CountyRow {
                county_id: 26_161,
                state_id: 26,
            }],
        }
    }

    /// Assert `actual.activity` matches `expected` within `f64` slack — the
    /// FLOAT-column fidelity note means the port computes in `f64`.
    fn assert_activity(actual: &DistanceActivityRow, expected: f64) {
        assert!(
            (actual.activity - expected).abs() < 1e-9,
            "activity {} != expected {expected}",
            actual.activity,
        );
    }

    #[test]
    fn calculate_minimal_input_yields_one_row() {
        let rows = DistanceCalculator::new().calculate(&minimal_inputs());
        assert_eq!(rows.len(), 1);
        let r = rows[0];
        // Geography and time carried through the SHO2/Link2/SHO3 joins.
        assert_eq!(r.year_id, 2020);
        assert_eq!(r.month_id, 7);
        assert_eq!(r.day_id, 5); // from HourDay
        assert_eq!(r.hour_id, 8); // from HourDay
        assert_eq!(r.state_id, 26); // from County
        assert_eq!(r.county_id, 26_161); // from Link
        assert_eq!(r.zone_id, 261_610); // from Link
        assert_eq!(r.link_id, 5001);
        assert_eq!(r.reg_class_id, 30); // from SourceBin
        assert_eq!(r.source_type_id, 21);
        assert_eq!(r.fuel_type_id, 1); // from SourceBin
        assert_eq!(r.model_year_id, 2018); // yearID - ageID
        assert_eq!(r.road_type_id, 4); // from Link
        assert_activity(&r, 100.0); // distance 100 × fraction 1
    }

    #[test]
    fn calculate_sums_source_bin_activity_fraction() {
        // Two source bins with the same (regClass, fuelType) but distinct
        // sourceBinIDs — different engTech/engSize. SBD2 sums their activity
        // fractions into one fuelTypeActivityFraction: 0.5 + 0.25 = 0.75.
        let mut inputs = minimal_inputs();
        inputs.source_bin.push(SourceBinRow {
            source_bin_id: 1001,
            reg_class_id: 30,
            fuel_type_id: 1,
        });
        inputs.source_bin_distribution[0].source_bin_activity_fraction = 0.5;
        inputs
            .source_bin_distribution
            .push(SourceBinDistributionRow {
                source_type_model_year_id: 212_018,
                pol_process_id: 101,
                source_bin_id: 1001,
                source_bin_activity_fraction: 0.25,
            });

        let rows = DistanceCalculator::new().calculate(&inputs);
        assert_eq!(rows.len(), 1);
        assert_activity(&rows[0], 75.0); // 100 × (0.5 + 0.25)
    }

    #[test]
    fn calculate_splits_distance_across_fuel_types() {
        // A second source bin on a different fuel type adds a DistFracts row
        // for the same (sourceType, modelYear): the single SHO row's
        // distance is emitted once per fuel type.
        let mut inputs = minimal_inputs();
        inputs.source_bin.push(SourceBinRow {
            source_bin_id: 1002,
            reg_class_id: 30,
            fuel_type_id: 2,
        });
        inputs
            .source_bin_distribution
            .push(SourceBinDistributionRow {
                source_type_model_year_id: 212_018,
                pol_process_id: 101,
                source_bin_id: 1002,
                source_bin_activity_fraction: 1.0,
            });

        let rows = DistanceCalculator::new().calculate(&inputs);
        assert_eq!(rows.len(), 2);
        let fuel1 = rows.iter().find(|r| r.fuel_type_id == 1).unwrap();
        let fuel2 = rows.iter().find(|r| r.fuel_type_id == 2).unwrap();
        assert_activity(fuel1, 100.0);
        assert_activity(fuel2, 100.0);
    }

    #[test]
    fn calculate_picks_single_running_exhaust_pol_process() {
        // SourceBinDistribution carries the same bin under two Running
        // Exhaust polProcessIDs (101, 201) and one non-running one (302).
        // The calculator must use exactly one Running Exhaust polProcessID —
        // not sum across them, and not touch the non-running row.
        let mut inputs = minimal_inputs();
        inputs.source_bin_distribution[0].source_bin_activity_fraction = 0.5;
        inputs
            .source_bin_distribution
            .push(SourceBinDistributionRow {
                source_type_model_year_id: 212_018,
                pol_process_id: 201, // pollutant 2, process 1 — also Running Exhaust
                source_bin_id: 1000,
                source_bin_activity_fraction: 0.5,
            });
        inputs
            .source_bin_distribution
            .push(SourceBinDistributionRow {
                source_type_model_year_id: 212_018,
                pol_process_id: 302, // process 2 — must be ignored entirely
                source_bin_id: 1000,
                source_bin_activity_fraction: 99.0,
            });

        let rows = DistanceCalculator::new().calculate(&inputs);
        assert_eq!(rows.len(), 1);
        // Only polProcessID 101's row (fraction 0.5) is used — not 0.5 + 0.5
        // and certainly not the 99.0 from the non-running row.
        assert_activity(&rows[0], 50.0);
    }

    #[test]
    fn calculate_without_running_exhaust_yields_no_rows() {
        // Every SourceBinDistribution row is for a non-running process.
        let mut inputs = minimal_inputs();
        inputs.source_bin_distribution[0].pol_process_id = 102; // process 2
        assert!(DistanceCalculator::new().calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_excludes_off_network_road_type() {
        // The SHO row's link is off-network (roadTypeID 1): doesProcessContext
        // skips it, so no distance row is produced.
        let mut inputs = minimal_inputs();
        inputs.link[0].road_type_id = OFF_NETWORK_ROAD_TYPE_ID;
        assert!(DistanceCalculator::new().calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_drops_sho_without_matching_link_or_county() {
        // SHO references a link not in the Link table — the SHO3 inner join
        // drops it.
        let mut no_link = minimal_inputs();
        no_link.sho[0].link_id = 9999;
        assert!(DistanceCalculator::new().calculate(&no_link).is_empty());

        // Link references a county not in the County table — the Link2
        // inner join drops it, and with it the SHO row.
        let mut no_county = minimal_inputs();
        no_county.county.clear();
        assert!(DistanceCalculator::new().calculate(&no_county).is_empty());
    }

    #[test]
    fn calculate_drops_source_bin_distribution_without_matching_bin() {
        // The SBD row's sourceBinID is absent from SourceBin — the SBD2
        // inner join drops it, leaving no fuel-type activity fraction.
        let mut inputs = minimal_inputs();
        inputs.source_bin_distribution[0].source_bin_id = 7777;
        assert!(DistanceCalculator::new().calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_drops_sho_without_matching_model_year() {
        // ageID 10 in year 2020 gives modelYearID 2010; SourceTypeModelYear
        // only carries 2018, so the final (sourceTypeID, modelYearID) join
        // finds no DistFracts row.
        let mut inputs = minimal_inputs();
        inputs.sho[0].age_id = 10;
        assert!(DistanceCalculator::new().calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_drops_sho_without_matching_hour_day() {
        // SHO references an hourDayID absent from HourDay — the SHO2 inner
        // join drops it.
        let mut inputs = minimal_inputs();
        inputs.sho[0].hour_day_id = 999;
        assert!(DistanceCalculator::new().calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_empty_input_yields_no_rows() {
        assert!(DistanceCalculator::new()
            .calculate(&DistanceInputs::default())
            .is_empty());
    }

    #[test]
    fn calculate_output_is_sorted_by_dimension_key() {
        // Two fuel types and two SHO rows of different age produce four
        // output rows; the result must come back dimension-key sorted
        // regardless of the (hash-map-driven) computation order.
        let mut inputs = minimal_inputs();
        inputs.source_bin.push(SourceBinRow {
            source_bin_id: 1002,
            reg_class_id: 30,
            fuel_type_id: 2,
        });
        inputs
            .source_bin_distribution
            .push(SourceBinDistributionRow {
                source_type_model_year_id: 212_018,
                pol_process_id: 101,
                source_bin_id: 1002,
                source_bin_activity_fraction: 1.0,
            });
        // A second model year so a second SHO row joins.
        inputs.source_type_model_year.push(SourceTypeModelYearRow {
            source_type_model_year_id: 212_019,
            source_type_id: 21,
            model_year_id: 2019,
        });
        inputs
            .source_bin_distribution
            .push(SourceBinDistributionRow {
                source_type_model_year_id: 212_019,
                pol_process_id: 101,
                source_bin_id: 1000,
                source_bin_activity_fraction: 1.0,
            });
        inputs
            .source_bin_distribution
            .push(SourceBinDistributionRow {
                source_type_model_year_id: 212_019,
                pol_process_id: 101,
                source_bin_id: 1002,
                source_bin_activity_fraction: 1.0,
            });
        inputs.sho.push(ShoRow {
            hour_day_id: 85,
            month_id: 7,
            year_id: 2020,
            age_id: 1, // modelYearID 2019
            link_id: 5001,
            source_type_id: 21,
            distance: 50.0,
        });

        let rows = DistanceCalculator::new().calculate(&inputs);
        assert_eq!(rows.len(), 4);
        assert!(
            rows.windows(2)
                .all(|w| w[0].dimension_key() <= w[1].dimension_key()),
            "calculate output is not sorted by dimension key",
        );
    }

    #[test]
    fn processes_road_type_excludes_only_off_network() {
        assert!(!DistanceCalculator::processes_road_type(1)); // Off-Network
        assert!(DistanceCalculator::processes_road_type(2)); // rural restricted
        assert!(DistanceCalculator::processes_road_type(4)); // urban unrestricted
        assert!(DistanceCalculator::processes_road_type(5));
    }

    #[test]
    fn calculator_name_matches_dag_module() {
        assert_eq!(DistanceCalculator::new().name(), "DistanceCalculator");
        assert_eq!(DistanceCalculator::NAME, "DistanceCalculator");
    }

    #[test]
    fn calculator_subscribes_to_running_exhaust_at_year() {
        let calc = DistanceCalculator::new();
        let subs = calc.subscriptions();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].process_id, ProcessId(1)); // Running Exhaust
        assert_eq!(subs[0].granularity, Granularity::Year);
        assert_eq!(subs[0].priority.display(), "EMISSION_CALCULATOR");
    }

    #[test]
    fn calculator_registers_nothing() {
        // The Java constructor registers no pollutants; the chain DAG
        // records registrations_count 0.
        assert!(DistanceCalculator::new().registrations().is_empty());
    }

    #[test]
    fn calculator_declares_input_tables() {
        let calc = DistanceCalculator::new();
        let tables = calc.input_tables();
        for expected in [
            "County",
            "HourDay",
            "Link",
            "SHO",
            "SourceBin",
            "SourceBinDistribution",
            "SourceTypeModelYear",
        ] {
            assert!(tables.contains(&expected), "missing input table {expected}");
        }
        // `upstream` keeps the trait default — no chain dependency edges.
        assert!(calc.upstream().is_empty());
    }

    #[test]
    fn execute_requires_input_tables_in_context() {
        // An empty context has no tables; execute must return an error.
        let calc = DistanceCalculator::new();
        let ctx = CalculatorContext::new();
        assert!(calc.execute(&ctx).is_err());
    }

    #[test]
    fn execute_returns_dataframe_for_minimal_seeded_context() {
        use moves_framework::DataFrameStore;
        let calc = DistanceCalculator::new();
        let mut store = moves_framework::InMemoryStore::new();
        let inputs = minimal_inputs();
        // Seed via into_dataframe + insert to bypass schema-validation for
        // tables whose registry schema has more columns than the row struct
        // (SourceBin, County).
        store.insert("SHO", ShoRow::into_dataframe(inputs.sho).unwrap());
        store.insert(
            "SourceBin",
            SourceBinRow::into_dataframe(inputs.source_bin).unwrap(),
        );
        store.insert(
            "SourceBinDistribution",
            SourceBinDistributionRow::into_dataframe(inputs.source_bin_distribution).unwrap(),
        );
        store.insert(
            "SourceTypeModelYear",
            SourceTypeModelYearRow::into_dataframe(inputs.source_type_model_year).unwrap(),
        );
        store.insert(
            "HourDay",
            HourDayRow::into_dataframe(inputs.hour_day).unwrap(),
        );
        store.insert("Link", LinkRow::into_dataframe(inputs.link).unwrap());
        store.insert("County", CountyRow::into_dataframe(inputs.county).unwrap());
        let ctx = CalculatorContext::with_tables(store);
        let out = calc.execute(&ctx).expect("execute ok");
        let df = out.dataframe().expect("output should contain a DataFrame");
        assert_eq!(
            df.height(),
            1,
            "minimal inputs produce exactly one output row"
        );
        let activity = df
            .column("activity")
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();
        assert!(
            (activity - 100.0).abs() < 1e-9,
            "activity {activity} != 100.0"
        );
    }

    #[test]
    fn factory_builds_a_named_calculator() {
        assert_eq!(factory().name(), "DistanceCalculator");
    }

    #[test]
    fn calculator_is_object_safe() {
        // The registry stores calculators as Box<dyn Calculator>.
        let calc: Box<dyn Calculator> = Box::new(DistanceCalculator::new());
        assert_eq!(calc.name(), "DistanceCalculator");
    }
}

//! Output and intermediate table types for the Activity Calculator.
//!
//! Plain Rust mirrors of the rows `ActivityCalculator.sql` produces. The
//! `CREATE TABLE` / `TRUNCATE` scaffolding in the script's first two sections
//! is pure MariaDB mechanics with no algorithmic content, so it has no
//! analogue here — these structs *are* the tables.
//!
//! As in [`super::inputs`], every identifier is an [`i32`] and every quantity
//! an [`f64`].

use moves_framework::{data::TableRow, Error as MfError};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> MfError {
    MfError::RowExtraction {
        table: table.to_string(),
        row,
        column: column.to_string(),
        message: msg,
    }
}

/// One row inserted into `##ActivityTable##` (`MOVESWorkerActivityOutput`) —
/// the activity record the calculator emits for one
/// `(activity type, location, time, source-bin)` combination.
///
/// The SQL's `INSERT` column list is
/// `(yearID, monthID, dayID, hourID, stateID, countyID, zoneID, linkID,
/// sourceTypeID, regClassID, fuelTypeID, modelYearID, roadTypeID, SCC,
/// activityTypeID, activity)`. `ActivityCalculator.sql` inserts `NULL` for
/// `SCC` on every row — the calculator never classifies activity by source
/// classification code — so the column is omitted here; the Task 50 output
/// writer supplies the `NULL`.
///
/// `regClassID` is always populated: the Java force-enables the
/// `WithRegClassID` script section (see the [module docs](super)), so every
/// row carries a regulatory class.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ActivityRow {
    /// `yearID` — calendar year. The source-hours families copy it from the
    /// activity row; `Population` uses the iteration's `context.year`.
    pub year_id: i32,
    /// `monthID`. `0` for `Population` (a year-level quantity).
    pub month_id: i32,
    /// `dayID`. `0` for `Population`.
    pub day_id: i32,
    /// `hourID`. `0` for `Population`.
    pub hour_id: i32,
    /// `stateID` — from the master-loop iteration location.
    pub state_id: i32,
    /// `countyID` — from the iteration location.
    pub county_id: i32,
    /// `zoneID` — from the iteration location, except `SHP` and the hotelling
    /// families, which copy the zone of the activity row.
    pub zone_id: i32,
    /// `linkID` — from the activity row (`SourceHours`, `SHO`, `ONI`,
    /// `Population`) or the iteration location (`SHP`, `Starts`, hotelling).
    pub link_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `regClassID` — regulatory class from `RegClassSourceTypeFraction`.
    pub reg_class_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `modelYearID` — `yearID - ageID` (`context.year - ageID` for
    /// `Population`).
    pub model_year_id: i32,
    /// `roadTypeID` — from `Link` (`SourceHours`, `SHO`, `ONI`), the
    /// iteration location (`SHP`, `Starts`, hotelling), or fixed (`1` for the
    /// off-network `Population` rows).
    pub road_type_id: i32,
    /// `activityTypeID` — the kind of activity this row records:
    /// `2` source hours, `3` extended idle hours, `4` source hours operating,
    /// `5` source hours parked, `6` population, `7` starts,
    /// `13`/`14`/`15` hotelling diesel-aux / battery-or-AC / engines-off.
    pub activity_type_id: i32,
    /// `activity` — the activity quantity, the product of the base activity
    /// table value and the fuel-fraction / regulatory-class / op-mode
    /// weightings that split it across the source bin.
    pub activity: f64,
}

impl TableRow for ActivityRow {
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
            ("sourceTypeID".into(), DataType::Int32),
            ("regClassID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("activityTypeID".into(), DataType::Int32),
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
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
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
                    "activityTypeID".into(),
                    rows.iter()
                        .map(|r| r.activity_type_id)
                        .collect::<Vec<i32>>(),
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
        let year_id = get_i32("yearID")?;
        let month_id = get_i32("monthID")?;
        let day_id = get_i32("dayID")?;
        let hour_id = get_i32("hourID")?;
        let state_id = get_i32("stateID")?;
        let county_id = get_i32("countyID")?;
        let zone_id = get_i32("zoneID")?;
        let link_id = get_i32("linkID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let reg_class_id = get_i32("regClassID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        let model_year_id = get_i32("modelYearID")?;
        let road_type_id = get_i32("roadTypeID")?;
        let activity_type_id = get_i32("activityTypeID")?;
        let activity = get_f64("activity")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(ActivityRow {
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    day_id: day_id.get(i).ok_or_else(|| null("dayID"))?,
                    hour_id: hour_id.get(i).ok_or_else(|| null("hourID"))?,
                    state_id: state_id.get(i).ok_or_else(|| null("stateID"))?,
                    county_id: county_id.get(i).ok_or_else(|| null("countyID"))?,
                    zone_id: zone_id.get(i).ok_or_else(|| null("zoneID"))?,
                    link_id: link_id.get(i).ok_or_else(|| null("linkID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    reg_class_id: reg_class_id.get(i).ok_or_else(|| null("regClassID"))?,
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    model_year_id: model_year_id.get(i).ok_or_else(|| null("modelYearID"))?,
                    road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
                    activity_type_id: activity_type_id
                        .get(i)
                        .ok_or_else(|| null("activityTypeID"))?,
                    activity: activity.get(i).ok_or_else(|| null("activity"))?,
                })
            })
            .collect()
    }
}

/// One `sourceTypeFuelFraction` row — the share of a
/// `(sourceType, modelYear)` population running on a given fuel type.
///
/// Built by [`super::fuelfraction::create_source_type_fuel_fraction`] and
/// consumed by every activity section except the hotelling families (which
/// carry `fuelTypeID` directly on the activity row). Mirrors the
/// `sourceTypeFuelFraction` table the script creates and drops within one
/// execution.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypeFuelFractionRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `modelYearID`.
    pub model_year_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `fuelFraction` — `tempFuelFraction / tempTotal`, or `0` when the
    /// `(sourceType, modelYear)` total is non-positive.
    pub fuel_fraction: f64,
}

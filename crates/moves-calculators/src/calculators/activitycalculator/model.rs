//! Output and intermediate table types for the Activity Calculator.
//!
//! Plain Rust mirrors of the rows `ActivityCalculator.sql` produces. The
//! `CREATE TABLE` / `TRUNCATE` scaffolding in the script's first two sections
//! is pure MariaDB mechanics with no algorithmic content, so it has no
//! analogue here — these structs *are* the tables.
//!
//! As in [`super::inputs`], every identifier is an [`i32`] and every quantity
//! an [`f64`].

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

//! Input tables for the Activity Calculator.
//!
//! Plain Rust mirrors of the tables `ActivityCalculator.sql` reads. The Java
//! extracts these from the MariaDB execution database (the script's
//! `Extract Data` section) and the pure port instead takes them as plain row
//! vectors bundled in [`ActivityInputs`].
//!
//! Every `INT`/`SMALLINT` identifier becomes [`i32`] — every MOVES identifier
//! fits comfortably — and every `FLOAT`/`DOUBLE` quantity becomes [`f64`].
//!
//! Only the columns the calculator's `Processing` SQL actually references are
//! modelled; provenance columns the algorithm never reads are omitted. Where
//! the `Extract Data` section filters a table (by `yearID`, `zoneID`,
//! `linkID`, …) and `Processing` never reads the filtered column, that
//! column is omitted and the per-struct doc records the upstream filter.

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

// ===========================================================================
// Master-loop iteration context — the `##context.*##` placeholders.
// ===========================================================================

/// The master-loop iteration scalars the SQL substitutes for its
/// `##context.*##` placeholders — the location and time the calculator is
/// running for.
///
/// `ActivityCalculator.java` runs `doExecute` once per `(process, zone, link,
/// year)`; these are the location/time of that call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct IterationContext {
    /// `##context.year##` — the calendar year being processed.
    pub year: i32,
    /// `##context.iterLocation.stateRecordID##`.
    pub state_id: i32,
    /// `##context.iterLocation.countyRecordID##`.
    pub county_id: i32,
    /// `##context.iterLocation.zoneRecordID##`.
    pub zone_id: i32,
    /// `##context.iterLocation.linkRecordID##`.
    pub link_id: i32,
    /// `##context.iterLocation.roadTypeRecordID##`.
    pub road_type_id: i32,
    /// `##context.fuelYearID##` — the fuel year, used only by the
    /// `UseFuelUsageFraction` variant of `createSourceTypeFuelFraction`.
    pub fuel_year_id: i32,
}

// ===========================================================================
// Activity-table source rows — the base activity each section weights.
// ===========================================================================

/// One `SourceHours` row — source hours by `(hourDay, month, year, age, link,
/// sourceType)`. Extracted `WHERE yearID = context.year AND linkID =
/// context.iterLocation.linkRecordID`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceHoursRow {
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `yearID`.
    pub year_id: i32,
    /// `ageID`.
    pub age_id: i32,
    /// `linkID`.
    pub link_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `sourceHours` — the base source-hours quantity.
    pub source_hours: f64,
}

/// One `SHO` row — source hours operating by `(hourDay, month, year, age,
/// link, sourceType)`. Extracted `WHERE yearID = context.year AND linkID =
/// context.iterLocation.linkRecordID`.
///
/// Feeds both the `SHO` section (`activityTypeID` 4) and the `ONI` section
/// (off-network idle, also `activityTypeID` 4) — the SQL extracts and
/// processes `SHO` identically for both.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShoRow {
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `yearID`.
    pub year_id: i32,
    /// `ageID`.
    pub age_id: i32,
    /// `linkID`.
    pub link_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `SHO` — the base source-hours-operating quantity.
    pub sho: f64,
}

/// One `SHP` row — source hours parked by `(hourDay, month, year, age, zone,
/// sourceType)`. Extracted `WHERE yearID = context.year AND zoneID =
/// context.iterLocation.zoneRecordID`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShpRow {
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `yearID`.
    pub year_id: i32,
    /// `ageID`.
    pub age_id: i32,
    /// `zoneID` — the SQL copies this onto the output row.
    pub zone_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `SHP` — the base source-hours-parked quantity.
    pub shp: f64,
}

/// One `Starts` row — engine starts by `(hourDay, month, year, age, zone,
/// sourceType)`. Extracted `WHERE yearID = context.year AND zoneID =
/// context.iterLocation.zoneRecordID`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StartsRow {
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `yearID`.
    pub year_id: i32,
    /// `ageID`.
    pub age_id: i32,
    /// `zoneID`.
    pub zone_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `starts` — the base engine-starts quantity.
    pub starts: f64,
}

/// One `hotellingHours` row — hotelling hours by `(hourDay, month, year, age,
/// zone, sourceType, fuelType)`. Extracted `WHERE yearID = context.year AND
/// zoneID = context.iterLocation.zoneRecordID`.
///
/// Unlike the source-hours families, the hotelling sections carry
/// `fuelTypeID` on the activity row itself rather than expanding it through
/// `sourceTypeFuelFraction`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HotellingHoursRow {
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `yearID`.
    pub year_id: i32,
    /// `ageID`.
    pub age_id: i32,
    /// `zoneID` — the SQL copies this onto the output row.
    pub zone_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `hotellingHours` — the base hotelling-hours quantity.
    pub hotelling_hours: f64,
}

// ===========================================================================
// Join / lookup tables.
// ===========================================================================

/// One `HourDay` row — the `hourDayID` → `(day, hour)` decomposition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HourDayRow {
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `dayID`.
    pub day_id: i32,
    /// `hourID`.
    pub hour_id: i32,
}

/// One `link` row — a road link in the iteration zone. Extracted `WHERE
/// zoneID = context.iterLocation.zoneRecordID`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LinkRow {
    /// `linkID`.
    pub link_id: i32,
    /// `zoneID`.
    pub zone_id: i32,
    /// `roadTypeID` — `1` marks the off-network link.
    pub road_type_id: i32,
    /// `linkVolume` — vehicle volume on the link, read only by the
    /// Project-domain on-roadway `Population` allocation.
    pub link_volume: f64,
}

/// One `RegClassSourceTypeFraction` row — the regulatory-class split of a
/// `(sourceType, fuelType, modelYear)` source bin. Extracted `WHERE
/// modelYearID BETWEEN context.year - 40 AND context.year`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RegClassSourceTypeFractionRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `modelYearID`.
    pub model_year_id: i32,
    /// `regClassID` — regulatory class.
    pub reg_class_id: i32,
    /// `regClassFraction` — the bin's share of the regulatory class.
    pub reg_class_fraction: f64,
}

/// One `hotellingActivityDistribution` row — the operating-mode split of
/// hotelling activity over a model-year range. Extracted for the iteration
/// zone with `beginModelYearID <= context.year` and `endModelYearID >=
/// context.year - 40`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HotellingActivityDistributionRow {
    /// `opModeID` — `200` extended idle; `201`/`203`/`204` hotelling
    /// diesel-aux / battery-or-AC / engines-off.
    pub op_mode_id: i32,
    /// `beginModelYearID` — inclusive start of the model-year range.
    pub begin_model_year_id: i32,
    /// `endModelYearID` — inclusive end of the model-year range.
    pub end_model_year_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `opModeFraction` — the op-mode's share of hotelling hours.
    pub op_mode_fraction: f64,
}

// ===========================================================================
// `createSourceTypeFuelFraction` inputs.
// ===========================================================================

/// One `sampleVehiclePopulation` row — the sample-vehicle fleet split by
/// `(sourceTypeModelYear, fuelType)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SampleVehiclePopulationRow {
    /// `sourceTypeModelYearID` — the `(sourceType, modelYear)` surrogate key.
    pub source_type_model_year_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `stmyFraction` — the sample-vehicle fraction of the source bin.
    pub stmy_fraction: f64,
}

/// One `fuelUsageFraction` row — re-fuelling reassignment from a vehicle's
/// nominal fuel type to the fuel it actually burns.
///
/// Read only by the `UseFuelUsageFraction` variant of
/// `createSourceTypeFuelFraction`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelUsageFractionRow {
    /// `countyID`.
    pub county_id: i32,
    /// `fuelYearID`.
    pub fuel_year_id: i32,
    /// `modelYearGroupID` — the SQL keeps only `modelYearGroupID = 0`.
    pub model_year_group_id: i32,
    /// `sourceBinFuelTypeID` — the vehicle's nominal (source-bin) fuel type.
    pub source_bin_fuel_type_id: i32,
    /// `fuelSupplyFuelTypeID` — the fuel actually supplied / burned.
    pub fuel_supply_fuel_type_id: i32,
    /// `usageFraction` — the share of the nominal fuel reassigned to the
    /// supply fuel.
    pub usage_fraction: f64,
}

/// One `sourceTypeModelYear` row — resolves the `sourceTypeModelYearID`
/// surrogate key into its `(sourceType, modelYear)` pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceTypeModelYearRow {
    /// `sourceTypeModelYearID`.
    pub source_type_model_year_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `modelYearID`.
    pub model_year_id: i32,
}

/// One `runSpecSourceFuelType` row — a `(sourceType, fuelType)` pair the
/// RunSpec selected; gates the final `sourceTypeFuelFraction` join.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RunSpecSourceFuelTypeRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
}

// ===========================================================================
// `Population` inputs.
// ===========================================================================

/// One `sourceUseType` row — the calculator reads only its `sourceTypeID`,
/// to drive the Non-Project `Population` source-type fraction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceUseTypeRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
}

/// One `roadTypeDistribution` row — a source type's VMT split across road
/// types.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RoadTypeDistributionRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `roadTypeID`.
    pub road_type_id: i32,
    /// `roadTypeVMTFraction` — the road type's share of the source type's
    /// VMT.
    pub road_type_vmt_fraction: f64,
}

/// One `zoneRoadType` row, as extracted — `SHOAllocFactor` summed over source
/// type and grouped by `roadTypeID`, for the iteration zone.
///
/// The `Extract Data` section runs `SELECT zoneID, roadTypeID,
/// sum(SHOAllocFactor) ... WHERE zoneID = context... GROUP BY roadTypeID`, so
/// the rows reaching the calculator already hold one pre-summed factor per
/// road type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ZoneRoadTypeRow {
    /// `zoneID`.
    pub zone_id: i32,
    /// `roadTypeID`.
    pub road_type_id: i32,
    /// `SHOAllocFactor` — source-hours-operating allocation factor, summed
    /// over source type.
    pub sho_alloc_factor: f64,
}

/// One `sourceTypeAgePopulation` row — vehicle population by `(sourceType,
/// age)`. Extracted `WHERE yearID = context.year`; `Processing` reads only
/// the three columns below.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypeAgePopulationRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `ageID`.
    pub age_id: i32,
    /// `population` — vehicles of this type and age.
    pub population: f64,
}

/// One `runSpecSourceType` row — a source type the RunSpec selected; gates
/// the Non-Project `Population` join.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RunSpecSourceTypeRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
}

/// One `offNetworkLink` row — the off-network vehicle population of a zone,
/// used by the Project-domain `Population` allocation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OffNetworkLinkRow {
    /// `zoneID`.
    pub zone_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `vehiclePopulation` — off-network vehicle population.
    pub vehicle_population: f64,
}

/// One `linkSourceTypeHour` row — the per-source-type share of an
/// on-roadway link's hourly volume, used by the Project-domain `Population`
/// allocation. Extracted for links in the iteration zone.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LinkSourceTypeHourRow {
    /// `linkID`.
    pub link_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `sourceTypeHourFraction` — the source type's share of the link's
    /// hourly volume.
    pub source_type_hour_fraction: f64,
}

/// One `sourceTypeAgeDistribution` row — the age-fraction split of a source
/// type's population, used by the Project-domain `Population` allocation.
/// Extracted `WHERE yearID = context.year` and joined to `RunSpecSourceType`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypeAgeDistributionRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `ageID`.
    pub age_id: i32,
    /// `ageFraction` — the age cohort's share of the source-type population.
    pub age_fraction: f64,
}

// ===========================================================================
// Input bundle.
// ===========================================================================

impl TableRow for SourceHoursRow {
    fn table_name() -> &'static str {
        "SourceHours"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("linkID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("sourceHours".into(), DataType::Float64),
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
                    "sourceHours".into(),
                    rows.iter().map(|r| r.source_hours).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SourceHours";
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
        let hour_day_id = get_i32("hourDayID")?;
        let month_id = get_i32("monthID")?;
        let year_id = get_i32("yearID")?;
        let age_id = get_i32("ageID")?;
        let link_id = get_i32("linkID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let source_hours = get_f64("sourceHours")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SourceHoursRow {
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    link_id: link_id.get(i).ok_or_else(|| null("linkID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    source_hours: source_hours.get(i).ok_or_else(|| null("sourceHours"))?,
                })
            })
            .collect()
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
            ("SHO".into(), DataType::Float64),
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
                    "SHO".into(),
                    rows.iter().map(|r| r.sho).collect::<Vec<f64>>(),
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
        let hour_day_id = get_i32("hourDayID")?;
        let month_id = get_i32("monthID")?;
        let year_id = get_i32("yearID")?;
        let age_id = get_i32("ageID")?;
        let link_id = get_i32("linkID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let sho = get_f64("SHO")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(ShoRow {
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    link_id: link_id.get(i).ok_or_else(|| null("linkID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    sho: sho.get(i).ok_or_else(|| null("SHO"))?,
                })
            })
            .collect()
    }
}

impl TableRow for ShpRow {
    fn table_name() -> &'static str {
        "SHP"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("zoneID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("SHP".into(), DataType::Float64),
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
                    "zoneID".into(),
                    rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "SHP".into(),
                    rows.iter().map(|r| r.shp).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SHP";
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
        let hour_day_id = get_i32("hourDayID")?;
        let month_id = get_i32("monthID")?;
        let year_id = get_i32("yearID")?;
        let age_id = get_i32("ageID")?;
        let zone_id = get_i32("zoneID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let shp = get_f64("SHP")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(ShpRow {
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    zone_id: zone_id.get(i).ok_or_else(|| null("zoneID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    shp: shp.get(i).ok_or_else(|| null("SHP"))?,
                })
            })
            .collect()
    }
}

impl TableRow for StartsRow {
    fn table_name() -> &'static str {
        "Starts"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("zoneID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("starts".into(), DataType::Float64),
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
                    "zoneID".into(),
                    rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "starts".into(),
                    rows.iter().map(|r| r.starts).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "Starts";
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
        let hour_day_id = get_i32("hourDayID")?;
        let month_id = get_i32("monthID")?;
        let year_id = get_i32("yearID")?;
        let age_id = get_i32("ageID")?;
        let zone_id = get_i32("zoneID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let starts = get_f64("starts")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(StartsRow {
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    zone_id: zone_id.get(i).ok_or_else(|| null("zoneID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    starts: starts.get(i).ok_or_else(|| null("starts"))?,
                })
            })
            .collect()
    }
}

impl TableRow for HotellingHoursRow {
    fn table_name() -> &'static str {
        "hotellingHours"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("zoneID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("hotellingHours".into(), DataType::Float64),
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
                    "zoneID".into(),
                    rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>(),
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
                    "hotellingHours".into(),
                    rows.iter().map(|r| r.hotelling_hours).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "hotellingHours";
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
        let hour_day_id = get_i32("hourDayID")?;
        let month_id = get_i32("monthID")?;
        let year_id = get_i32("yearID")?;
        let age_id = get_i32("ageID")?;
        let zone_id = get_i32("zoneID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        let hotelling_hours = get_f64("hotellingHours")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(HotellingHoursRow {
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    zone_id: zone_id.get(i).ok_or_else(|| null("zoneID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    hotelling_hours: hotelling_hours
                        .get(i)
                        .ok_or_else(|| null("hotellingHours"))?,
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
        let hour_day_id = get_i32("hourDayID")?;
        let day_id = get_i32("dayID")?;
        let hour_id = get_i32("hourID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(HourDayRow {
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    day_id: day_id.get(i).ok_or_else(|| null("dayID"))?,
                    hour_id: hour_id.get(i).ok_or_else(|| null("hourID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for LinkRow {
    fn table_name() -> &'static str {
        "link"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("linkID".into(), DataType::Int32),
            ("zoneID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("linkVolume".into(), DataType::Float64),
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
                    "zoneID".into(),
                    rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeID".into(),
                    rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "linkVolume".into(),
                    rows.iter().map(|r| r.link_volume).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "link";
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
        let link_id = get_i32("linkID")?;
        let zone_id = get_i32("zoneID")?;
        let road_type_id = get_i32("roadTypeID")?;
        let link_volume = get_f64("linkVolume")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(LinkRow {
                    link_id: link_id.get(i).ok_or_else(|| null("linkID"))?,
                    zone_id: zone_id.get(i).ok_or_else(|| null("zoneID"))?,
                    road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
                    link_volume: link_volume.get(i).ok_or_else(|| null("linkVolume"))?,
                })
            })
            .collect()
    }
}

impl TableRow for RegClassSourceTypeFractionRow {
    fn table_name() -> &'static str {
        "RegClassSourceTypeFraction"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("regClassID".into(), DataType::Int32),
            ("regClassFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
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
                    "regClassID".into(),
                    rows.iter().map(|r| r.reg_class_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "regClassFraction".into(),
                    rows.iter()
                        .map(|r| r.reg_class_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "RegClassSourceTypeFraction";
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
        let fuel_type_id = get_i32("fuelTypeID")?;
        let model_year_id = get_i32("modelYearID")?;
        let reg_class_id = get_i32("regClassID")?;
        let reg_class_fraction = get_f64("regClassFraction")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(RegClassSourceTypeFractionRow {
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    model_year_id: model_year_id.get(i).ok_or_else(|| null("modelYearID"))?,
                    reg_class_id: reg_class_id.get(i).ok_or_else(|| null("regClassID"))?,
                    reg_class_fraction: reg_class_fraction
                        .get(i)
                        .ok_or_else(|| null("regClassFraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for HotellingActivityDistributionRow {
    fn table_name() -> &'static str {
        "hotellingActivityDistribution"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("opModeID".into(), DataType::Int32),
            ("beginModelYearID".into(), DataType::Int32),
            ("endModelYearID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("opModeFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "opModeID".into(),
                    rows.iter().map(|r| r.op_mode_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "beginModelYearID".into(),
                    rows.iter()
                        .map(|r| r.begin_model_year_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "endModelYearID".into(),
                    rows.iter()
                        .map(|r| r.end_model_year_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
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
        let t = "hotellingActivityDistribution";
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
        let op_mode_id = get_i32("opModeID")?;
        let begin_model_year_id = get_i32("beginModelYearID")?;
        let end_model_year_id = get_i32("endModelYearID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        let op_mode_fraction = get_f64("opModeFraction")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(HotellingActivityDistributionRow {
                    op_mode_id: op_mode_id.get(i).ok_or_else(|| null("opModeID"))?,
                    begin_model_year_id: begin_model_year_id
                        .get(i)
                        .ok_or_else(|| null("beginModelYearID"))?,
                    end_model_year_id: end_model_year_id
                        .get(i)
                        .ok_or_else(|| null("endModelYearID"))?,
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    op_mode_fraction: op_mode_fraction
                        .get(i)
                        .ok_or_else(|| null("opModeFraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for SampleVehiclePopulationRow {
    fn table_name() -> &'static str {
        "sampleVehiclePopulation"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeModelYearID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("stmyFraction".into(), DataType::Float64),
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
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "stmyFraction".into(),
                    rows.iter().map(|r| r.stmy_fraction).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "sampleVehiclePopulation";
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
        let source_type_model_year_id = get_i32("sourceTypeModelYearID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        let stmy_fraction = get_f64("stmyFraction")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SampleVehiclePopulationRow {
                    source_type_model_year_id: source_type_model_year_id
                        .get(i)
                        .ok_or_else(|| null("sourceTypeModelYearID"))?,
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    stmy_fraction: stmy_fraction.get(i).ok_or_else(|| null("stmyFraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for FuelUsageFractionRow {
    fn table_name() -> &'static str {
        "fuelUsageFraction"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("countyID".into(), DataType::Int32),
            ("fuelYearID".into(), DataType::Int32),
            ("modelYearGroupID".into(), DataType::Int32),
            ("sourceBinFuelTypeID".into(), DataType::Int32),
            ("fuelSupplyFuelTypeID".into(), DataType::Int32),
            ("usageFraction".into(), DataType::Float64),
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
                    "fuelYearID".into(),
                    rows.iter().map(|r| r.fuel_year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "modelYearGroupID".into(),
                    rows.iter()
                        .map(|r| r.model_year_group_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceBinFuelTypeID".into(),
                    rows.iter()
                        .map(|r| r.source_bin_fuel_type_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelSupplyFuelTypeID".into(),
                    rows.iter()
                        .map(|r| r.fuel_supply_fuel_type_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "usageFraction".into(),
                    rows.iter().map(|r| r.usage_fraction).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "fuelUsageFraction";
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
        let county_id = get_i32("countyID")?;
        let fuel_year_id = get_i32("fuelYearID")?;
        let model_year_group_id = get_i32("modelYearGroupID")?;
        let source_bin_fuel_type_id = get_i32("sourceBinFuelTypeID")?;
        let fuel_supply_fuel_type_id = get_i32("fuelSupplyFuelTypeID")?;
        let usage_fraction = get_f64("usageFraction")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(FuelUsageFractionRow {
                    county_id: county_id.get(i).ok_or_else(|| null("countyID"))?,
                    fuel_year_id: fuel_year_id.get(i).ok_or_else(|| null("fuelYearID"))?,
                    model_year_group_id: model_year_group_id
                        .get(i)
                        .ok_or_else(|| null("modelYearGroupID"))?,
                    source_bin_fuel_type_id: source_bin_fuel_type_id
                        .get(i)
                        .ok_or_else(|| null("sourceBinFuelTypeID"))?,
                    fuel_supply_fuel_type_id: fuel_supply_fuel_type_id
                        .get(i)
                        .ok_or_else(|| null("fuelSupplyFuelTypeID"))?,
                    usage_fraction: usage_fraction.get(i).ok_or_else(|| null("usageFraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for SourceTypeModelYearRow {
    fn table_name() -> &'static str {
        "sourceTypeModelYear"
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
        let t = "sourceTypeModelYear";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let source_type_model_year_id = get_i32("sourceTypeModelYearID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let model_year_id = get_i32("modelYearID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SourceTypeModelYearRow {
                    source_type_model_year_id: source_type_model_year_id
                        .get(i)
                        .ok_or_else(|| null("sourceTypeModelYearID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    model_year_id: model_year_id.get(i).ok_or_else(|| null("modelYearID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for RunSpecSourceFuelTypeRow {
    fn table_name() -> &'static str {
        "runSpecSourceFuelType"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
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
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "runSpecSourceFuelType";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let source_type_id = get_i32("sourceTypeID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(RunSpecSourceFuelTypeRow {
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for SourceUseTypeRow {
    fn table_name() -> &'static str {
        "sourceUseType"
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
        let t = "sourceUseType";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let source_type_id = get_i32("sourceTypeID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SourceUseTypeRow {
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for RoadTypeDistributionRow {
    fn table_name() -> &'static str {
        "roadTypeDistribution"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("roadTypeVMTFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeID".into(),
                    rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeVMTFraction".into(),
                    rows.iter()
                        .map(|r| r.road_type_vmt_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "roadTypeDistribution";
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
        let road_type_vmt_fraction = get_f64("roadTypeVMTFraction")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(RoadTypeDistributionRow {
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
                    road_type_vmt_fraction: road_type_vmt_fraction
                        .get(i)
                        .ok_or_else(|| null("roadTypeVMTFraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for ZoneRoadTypeRow {
    fn table_name() -> &'static str {
        "zoneRoadType"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("zoneID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("SHOAllocFactor".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
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
                Series::new(
                    "SHOAllocFactor".into(),
                    rows.iter()
                        .map(|r| r.sho_alloc_factor)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "zoneRoadType";
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
        let zone_id = get_i32("zoneID")?;
        let road_type_id = get_i32("roadTypeID")?;
        let sho_alloc_factor = get_f64("SHOAllocFactor")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(ZoneRoadTypeRow {
                    zone_id: zone_id.get(i).ok_or_else(|| null("zoneID"))?,
                    road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
                    sho_alloc_factor: sho_alloc_factor
                        .get(i)
                        .ok_or_else(|| null("SHOAllocFactor"))?,
                })
            })
            .collect()
    }
}

impl TableRow for SourceTypeAgePopulationRow {
    fn table_name() -> &'static str {
        "sourceTypeAgePopulation"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("population".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "population".into(),
                    rows.iter().map(|r| r.population).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "sourceTypeAgePopulation";
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
        let age_id = get_i32("ageID")?;
        let population = get_f64("population")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SourceTypeAgePopulationRow {
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    population: population.get(i).ok_or_else(|| null("population"))?,
                })
            })
            .collect()
    }
}

impl TableRow for RunSpecSourceTypeRow {
    fn table_name() -> &'static str {
        "runSpecSourceType"
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
        let t = "runSpecSourceType";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let source_type_id = get_i32("sourceTypeID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(RunSpecSourceTypeRow {
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for OffNetworkLinkRow {
    fn table_name() -> &'static str {
        "offNetworkLink"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("zoneID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("vehiclePopulation".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "zoneID".into(),
                    rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "vehiclePopulation".into(),
                    rows.iter()
                        .map(|r| r.vehicle_population)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "offNetworkLink";
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
        let zone_id = get_i32("zoneID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let vehicle_population = get_f64("vehiclePopulation")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(OffNetworkLinkRow {
                    zone_id: zone_id.get(i).ok_or_else(|| null("zoneID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    vehicle_population: vehicle_population
                        .get(i)
                        .ok_or_else(|| null("vehiclePopulation"))?,
                })
            })
            .collect()
    }
}

impl TableRow for LinkSourceTypeHourRow {
    fn table_name() -> &'static str {
        "linkSourceTypeHour"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("linkID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("sourceTypeHourFraction".into(), DataType::Float64),
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
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeHourFraction".into(),
                    rows.iter()
                        .map(|r| r.source_type_hour_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "linkSourceTypeHour";
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
        let link_id = get_i32("linkID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let source_type_hour_fraction = get_f64("sourceTypeHourFraction")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(LinkSourceTypeHourRow {
                    link_id: link_id.get(i).ok_or_else(|| null("linkID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    source_type_hour_fraction: source_type_hour_fraction
                        .get(i)
                        .ok_or_else(|| null("sourceTypeHourFraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for SourceTypeAgeDistributionRow {
    fn table_name() -> &'static str {
        "sourceTypeAgeDistribution"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("ageFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageFraction".into(),
                    rows.iter().map(|r| r.age_fraction).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "sourceTypeAgeDistribution";
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
        let age_id = get_i32("ageID")?;
        let age_fraction = get_f64("ageFraction")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SourceTypeAgeDistributionRow {
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    age_fraction: age_fraction.get(i).ok_or_else(|| null("ageFraction"))?,
                })
            })
            .collect()
    }
}

/// Every table [`ActivityCalculator::run`](super::ActivityCalculator::run)
/// reads, plus the iteration [`context`](Self::context).
///
/// The Java reads these rows out of the MariaDB execution database; the pure
/// port instead takes them as plain row vectors. A future Task 50
/// (`DataFrameStore`) wiring populates this from the scratch / default-DB
/// `DataFrame`s. [`Default`] yields an all-empty bundle so a test (or a
/// section that an [`ActivityConfig`](super::ActivityConfig) leaves disabled)
/// can fill in only the tables it needs.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ActivityInputs {
    /// The master-loop iteration location and time.
    pub context: IterationContext,

    /// `SourceHours` — base source hours (`activityTypeID` 2).
    pub source_hours: Vec<SourceHoursRow>,
    /// `SHO` — base source hours operating; feeds the `SHO` and `ONI`
    /// sections (both `activityTypeID` 4).
    pub sho: Vec<ShoRow>,
    /// `SHP` — base source hours parked (`activityTypeID` 5).
    pub shp: Vec<ShpRow>,
    /// `Starts` — base engine starts (`activityTypeID` 7).
    pub starts: Vec<StartsRow>,
    /// `hotellingHours` — base hotelling hours, feeding `ExtendedIdleHours`
    /// (`activityTypeID` 3) and `hotellingHours` (`activityTypeID` 13/14/15).
    pub hotelling_hours: Vec<HotellingHoursRow>,

    /// `HourDay` — the `hourDayID` → `(day, hour)` lookup.
    pub hour_day: Vec<HourDayRow>,
    /// `link` — road links in the iteration zone.
    pub link: Vec<LinkRow>,
    /// `RegClassSourceTypeFraction` — regulatory-class split of each source
    /// bin.
    pub reg_class_source_type_fraction: Vec<RegClassSourceTypeFractionRow>,
    /// `hotellingActivityDistribution` — op-mode split of hotelling activity.
    pub hotelling_activity_distribution: Vec<HotellingActivityDistributionRow>,

    /// `sampleVehiclePopulation` — sample-vehicle fleet, the basis of
    /// `sourceTypeFuelFraction`.
    pub sample_vehicle_population: Vec<SampleVehiclePopulationRow>,
    /// `fuelUsageFraction` — re-fuelling reassignment (`UseFuelUsageFraction`
    /// variant only).
    pub fuel_usage_fraction: Vec<FuelUsageFractionRow>,
    /// `sourceTypeModelYear` — `sourceTypeModelYearID` key resolution.
    pub source_type_model_year: Vec<SourceTypeModelYearRow>,
    /// `runSpecSourceFuelType` — RunSpec-selected `(sourceType, fuelType)`
    /// pairs.
    pub run_spec_source_fuel_type: Vec<RunSpecSourceFuelTypeRow>,

    /// `sourceUseType` — source types, for the Non-Project `Population`
    /// fraction.
    pub source_use_type: Vec<SourceUseTypeRow>,
    /// `roadTypeDistribution` — per-source-type road-type VMT split.
    pub road_type_distribution: Vec<RoadTypeDistributionRow>,
    /// `zoneRoadType` — pre-summed `SHOAllocFactor` per road type.
    pub zone_road_type: Vec<ZoneRoadTypeRow>,
    /// `sourceTypeAgePopulation` — population by `(sourceType, age)`.
    pub source_type_age_population: Vec<SourceTypeAgePopulationRow>,
    /// `runSpecSourceType` — RunSpec-selected source types.
    pub run_spec_source_type: Vec<RunSpecSourceTypeRow>,
    /// `offNetworkLink` — off-network population (Project domain).
    pub off_network_link: Vec<OffNetworkLinkRow>,
    /// `linkSourceTypeHour` — on-roadway source-type volume share (Project
    /// domain).
    pub link_source_type_hour: Vec<LinkSourceTypeHourRow>,
    /// `sourceTypeAgeDistribution` — age split (Project-domain `Population`).
    pub source_type_age_distribution: Vec<SourceTypeAgeDistributionRow>,
}

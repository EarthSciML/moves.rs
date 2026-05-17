//! Input tables for the Total Activity Generator.
//!
//! Plain Rust mirrors of the default-DB and RunSpec tables that
//! `TotalActivityGenerator.java` reads through its embedded SQL. Every
//! `INT`/`SMALLINT` identifier becomes [`i32`] — every MOVES identifier fits
//! comfortably — and every `FLOAT`/`DOUBLE` quantity becomes [`f64`].
//!
//! The Java reads these rows out of the MariaDB execution database; the pure
//! port instead takes them as plain row vectors. [`TotalActivityInputs`] is
//! the data-plane contract — a future Task 50 (`DataFrameStore`) wiring will
//! populate it from the scratch / default-DB `DataFrame`s, and
//! [`super::TotalActivityGenerator::run`] consumes it.
//!
//! Only the columns the generator's SQL actually references are modelled;
//! provenance columns the algorithm never reads (`*CV`, `isUserInput`, …)
//! are omitted, exactly as Task 29's `SourceBinDistributionGenerator` port
//! omitted its unread columns.

// ===========================================================================
// Steps 110-139 — population growth.
// ===========================================================================

/// One `Year` row — the calendar-year catalogue with base-year markers.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct YearRow {
    /// `yearID` — calendar year.
    pub year_id: i32,
    /// `isBaseYear` — `true` when the Java `isBaseYear IN ('Y','y')` test
    /// holds. A *base year* is one with measured `SourceTypeYear` /
    /// `HPMSVTypeYear` data the generator grows forward from.
    pub is_base_year: bool,
}

/// One `SourceTypeYear` row — per `(year, sourceType)` population basis and
/// growth factors.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypeYearRow {
    /// `yearID`.
    pub year_id: i32,
    /// `sourceTypeID` — MOVES source (vehicle) type.
    pub source_type_id: i32,
    /// `sourceTypePopulation` — total vehicles of this type in the year.
    pub source_type_population: f64,
    /// `migrationRate` — net migration multiplier.
    pub migration_rate: f64,
    /// `salesGrowthFactor` — new-vehicle (age 0) sales growth multiplier.
    pub sales_growth_factor: f64,
}

/// One `SourceTypeAgeDistribution` row — the age-fraction split of a
/// `(sourceType, year)` population.
///
/// Read as input for the base-year population (step 120) and written back
/// (`INSERT IGNORE`) for the analysis year (step 130).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypeAgeDistributionRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `yearID`.
    pub year_id: i32,
    /// `ageID` — vehicle age in years (`0` = new).
    pub age_id: i32,
    /// `ageFraction` — share of the population at this age.
    pub age_fraction: f64,
}

/// One `SourceTypeAge` row — per `(sourceType, age)` survival and
/// relative-mileage-accumulation factors.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypeAgeRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `ageID`.
    pub age_id: i32,
    /// `survivalRate` — fraction of vehicles surviving from `age-1` to `age`.
    pub survival_rate: f64,
    /// `relativeMAR` — relative mileage-accumulation rate, weighting how
    /// much an age cohort travels.
    pub relative_mar: f64,
}

// ===========================================================================
// Steps 140-159 — HPMS travel fraction and VMT growth.
// ===========================================================================

/// One `SourceUseType` row — the `sourceType` → `HPMSVType` rollup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceUseTypeRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `HPMSVTypeID` — the broader HPMS vehicle type the source type rolls
    /// up into.
    pub hpms_v_type_id: i32,
}

/// One `HPMSVTypeYear` row — per `(year, HPMSVType)` base VMT and growth.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HpmsVTypeYearRow {
    /// `yearID`.
    pub year_id: i32,
    /// `HPMSVTypeID`.
    pub hpms_v_type_id: i32,
    /// `HPMSBaseYearVMT` — measured base-year VMT for this HPMS type.
    pub hpms_base_year_vmt: f64,
    /// `VMTGrowthFactor` — year-over-year VMT growth multiplier.
    pub vmt_growth_factor: f64,
}

/// One `RunSpecSourceType` row — a source type selected by the RunSpec.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RunSpecSourceTypeRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
}

/// One `SourceTypeYearVMT` row — annual VMT supplied directly by source type
/// (an alternative to HPMS-typed VMT input).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypeYearVmtRow {
    /// `yearID`.
    pub year_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `VMT` — annual vehicle miles travelled.
    pub vmt: f64,
}

// ===========================================================================
// Steps 160-179 — VMT allocation by road type, source, age, and hour.
// ===========================================================================

/// One `RoadType` row — a modelled road type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RoadTypeRow {
    /// `roadTypeID`.
    pub road_type_id: i32,
}

/// One `RoadTypeDistribution` row — the share of a source type's VMT on a
/// given road type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RoadTypeDistributionRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `roadTypeID`.
    pub road_type_id: i32,
    /// `roadTypeVMTFraction` — fraction of the source type's VMT on this
    /// road type.
    pub road_type_vmt_fraction: f64,
}

/// One `SourceTypeDayVMT` row — daily VMT supplied by source type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypeDayVmtRow {
    /// `yearID`.
    pub year_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `dayID` — day-of-week type id.
    pub day_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `VMT` — daily vehicle miles travelled.
    pub vmt: f64,
}

/// One `HPMSVTypeDay` row — daily VMT supplied by HPMS vehicle type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HpmsVTypeDayRow {
    /// `yearID`.
    pub year_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `dayID`.
    pub day_id: i32,
    /// `HPMSVTypeID`.
    pub hpms_v_type_id: i32,
    /// `VMT` — daily vehicle miles travelled.
    pub vmt: f64,
}

/// One `MonthVMTFraction` row — the share of annual VMT in a month.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MonthVmtFractionRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `monthVMTFraction` — share of the source type's annual VMT.
    pub month_vmt_fraction: f64,
}

/// One `DayVMTFraction` row — the share of a month's VMT on a day type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DayVmtFractionRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `roadTypeID`.
    pub road_type_id: i32,
    /// `dayID`.
    pub day_id: i32,
    /// `dayVMTFraction` — share of the month's VMT on this day type.
    pub day_vmt_fraction: f64,
}

/// One `HourVMTFraction` row — the share of a day's VMT in an hour.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HourVmtFractionRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `roadTypeID`.
    pub road_type_id: i32,
    /// `dayID`.
    pub day_id: i32,
    /// `hourID`.
    pub hour_id: i32,
    /// `hourVMTFraction` — share of the day's VMT in this hour.
    pub hour_vmt_fraction: f64,
}

/// One `HourDay` row — the `(hour, day)` → `hourDay` packed-key catalogue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HourDayRow {
    /// `hourDayID` — packed `(hourID, dayID)` key.
    pub hour_day_id: i32,
    /// `hourID`.
    pub hour_id: i32,
    /// `dayID`.
    pub day_id: i32,
}

/// One `DayOfAnyWeek` row — the number of real 24-hour days a `dayID`
/// portion-of-week represents.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DayOfAnyWeekRow {
    /// `dayID`.
    pub day_id: i32,
    /// `noOfRealDays` — count of real days within the week portion.
    pub no_of_real_days: f64,
}

/// One `MonthOfAnyYear` row — supplies the day count used to derive
/// weeks-per-month.
///
/// Ports the data side of `WeeksInMonthHelper`: the helper computes
/// `weeksPerMonth = noOfDays / 7.0`. The Java reads `monthName`/`noOfDays`
/// from `monthOfAnyYear`; only `noOfDays` matters here.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MonthOfAnyYearRow {
    /// `monthID`.
    pub month_id: i32,
    /// `noOfDays` — calendar days in the month.
    pub no_of_days: i32,
}

// ===========================================================================
// Steps 180-189 — conversion to total-activity basis.
// ===========================================================================

/// One `SourceTypeHour` row — per `(sourceType, hourDay)` idle and hotelling
/// distribution factors.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypeHourRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `idleSHOFactor` — idle source-hours-operating factor (read by the
    /// Java but unused downstream of step 180; carried for fidelity).
    pub idle_sho_factor: f64,
    /// `hotellingDist` — hourly distribution weight for hotelling hours.
    pub hotelling_dist: f64,
}

/// One `RunSpecDay` row — a day type selected by the RunSpec.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RunSpecDayRow {
    /// `dayID`.
    pub day_id: i32,
}

/// One `AvgSpeedBin` row — the speed value of an average-speed bin.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AvgSpeedBinRow {
    /// `avgSpeedBinID`.
    pub avg_speed_bin_id: i32,
    /// `avgBinSpeed` — representative speed of the bin (mph).
    pub avg_bin_speed: f64,
}

/// One `AvgSpeedDistribution` row — the share of activity in a speed bin.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AvgSpeedDistributionRow {
    /// `roadTypeID`.
    pub road_type_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `avgSpeedBinID`.
    pub avg_speed_bin_id: i32,
    /// `avgSpeedFraction` — share of activity falling in this speed bin.
    pub avg_speed_fraction: f64,
}

/// One `HourOfAnyDay` row — an hour-of-day catalogue entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HourOfAnyDayRow {
    /// `hourID`.
    pub hour_id: i32,
}

/// One `ZoneRoadType` row — per `(zone, roadType)` source-hours allocation
/// factors.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ZoneRoadTypeRow {
    /// `zoneID`.
    pub zone_id: i32,
    /// `roadTypeID`.
    pub road_type_id: i32,
    /// `SHOAllocFactor` — source-hours-operating allocation factor.
    pub sho_alloc_factor: f64,
    /// `SHPAllocFactor` — source-hours-parked allocation factor.
    pub shp_alloc_factor: f64,
}

/// One `HotellingCalendarYear` row — the per-year hotelling rate.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HotellingCalendarYearRow {
    /// `yearID`.
    pub year_id: i32,
    /// `hotellingRate` — hotelling hours per unit VMT.
    pub hotelling_rate: f64,
}

/// One `SampleVehicleDay` row — a sampled vehicle observed on a day type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SampleVehicleDayRow {
    /// `vehID` — sample vehicle id.
    pub veh_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `dayID`.
    pub day_id: i32,
}

/// One `SampleVehicleTrip` row — a sampled trip; the engine-start the Java
/// counts towards `StartsPerSampleVehicle`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SampleVehicleTripRow {
    /// `vehID`.
    pub veh_id: i32,
    /// `dayID`.
    pub day_id: i32,
    /// `hourID`.
    pub hour_id: i32,
    /// `true` when `keyOnTime IS NOT NULL` — the Java's "ignore marker
    /// trips" filter keeps only trips with a real key-on time.
    pub has_key_on_time: bool,
}

/// One `StartsPerVehicle` row — engine starts per vehicle per `hourDay`.
///
/// Both an input and an output: the Java recomputes rows only for source
/// types not already present (see [`super::activity::starts_per_vehicle`]),
/// then `StartsByAgeHour` joins the full table.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StartsPerVehicleRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `startsPerVehicle` — engine starts per vehicle.
    pub starts_per_vehicle: f64,
}

// ===========================================================================
// Steps 190-209 — link/zone allocation (kernels only; see `allocation`).
// ===========================================================================

/// One `Link` row — a modelled road link within a zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LinkRow {
    /// `linkID`.
    pub link_id: i32,
    /// `zoneID`.
    pub zone_id: i32,
    /// `roadTypeID`.
    pub road_type_id: i32,
    /// `countyID`.
    pub county_id: i32,
}

/// One `County` row — the county's type and parent state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CountyRow {
    /// `countyID`.
    pub county_id: i32,
    /// `countyTypeID` — urban / rural classification.
    pub county_type_id: i32,
    /// `stateID`.
    pub state_id: i32,
}

/// One `State` row — the state's idle region.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StateRow {
    /// `stateID`.
    pub state_id: i32,
    /// `idleRegionID` — region key for `TotalIdleFraction`.
    pub idle_region_id: i32,
}

/// One `TotalIdleFraction` row — the total-idle share keyed by region,
/// county type, source type, month, day, and a model-year window.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TotalIdleFractionRow {
    /// `idleRegionID`.
    pub idle_region_id: i32,
    /// `countyTypeID`.
    pub county_type_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `dayID`.
    pub day_id: i32,
    /// `minModelYearID` — inclusive lower bound of the model-year window.
    pub min_model_year_id: i32,
    /// `maxModelYearID` — inclusive upper bound of the model-year window.
    pub max_model_year_id: i32,
    /// `totalIdleFraction` — total share of time spent idling.
    pub total_idle_fraction: f64,
}

/// One `DrivingIdleFraction` row — the share of driving time spent idling.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DrivingIdleFractionRow {
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `yearID`.
    pub year_id: i32,
    /// `roadTypeID`.
    pub road_type_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `drivingIdleFraction` — share of on-network time spent idling.
    pub driving_idle_fraction: f64,
}

/// One `Zone` row — the zone's source-hours-parked allocation factor.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ZoneRow {
    /// `zoneID`.
    pub zone_id: i32,
    /// `SHPAllocFactor` — source-hours-parked allocation factor.
    pub shp_alloc_factor: f64,
}

/// One `SampleVehiclePopulation` row — the model-year/fuel split of a
/// source type's sampled population.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SampleVehiclePopulationRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `modelYearID`.
    pub model_year_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `stmyFraction` — source-type/model-year population fraction.
    pub stmy_fraction: f64,
}

/// One `RunSpecHourDay` row — an `hourDay` selected by the RunSpec.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RunSpecHourDayRow {
    /// `hourDayID`.
    pub hour_day_id: i32,
}

/// One `LinkAverageSpeed` row — the average speed assigned to a link.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LinkAverageSpeedRow {
    /// `linkID`.
    pub link_id: i32,
    /// `averageSpeed` — average speed on the link (mph).
    pub average_speed: f64,
}

// ===========================================================================
// The `run` input bundle.
// ===========================================================================

/// Every default-DB and RunSpec table [`run`](super::TotalActivityGenerator::run)
/// reads to compute the year/zone activity tables (algorithm steps 110-189),
/// plus the analysis year and zone the run targets.
///
/// This is the data-plane contract: a future Task 50 (`DataFrameStore`)
/// `execute` wiring populates it from the scratch / default-DB `DataFrame`s.
/// The link/zone *allocation* kernels (steps 190-209, [`super::allocation`])
/// take their own arguments — they are sequenced by the master loop, not by
/// `run` — so the tables they need (`Link`, `County`, …) are not bundled here.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TotalActivityInputs {
    /// Calendar year being analysed (`MasterLoopContext.year`).
    pub analysis_year: i32,
    /// Zone being analysed (`MasterLoopContext.iterLocation.zoneRecordID`) —
    /// the zone whose `SHOAllocFactor` weights step-180 hotelling hours.
    pub zone_id: i32,
    /// `true` when the user supplied a `hotellingHoursPerDay` table — the
    /// Java `count(hotellinghoursperday) = 0` test in step 180's
    /// road-type pruning.
    pub has_hotelling_hours_per_day_input: bool,
    /// `Year`.
    pub year: Vec<YearRow>,
    /// `SourceTypeYear`.
    pub source_type_year: Vec<SourceTypeYearRow>,
    /// `SourceTypeAgeDistribution`.
    pub source_type_age_distribution: Vec<SourceTypeAgeDistributionRow>,
    /// `SourceTypeAge`.
    pub source_type_age: Vec<SourceTypeAgeRow>,
    /// `SourceUseType`.
    pub source_use_type: Vec<SourceUseTypeRow>,
    /// `HPMSVTypeYear`.
    pub hpms_v_type_year: Vec<HpmsVTypeYearRow>,
    /// `RunSpecSourceType`.
    pub run_spec_source_type: Vec<RunSpecSourceTypeRow>,
    /// `SourceTypeYearVMT`.
    pub source_type_year_vmt: Vec<SourceTypeYearVmtRow>,
    /// `RoadType`.
    pub road_type: Vec<RoadTypeRow>,
    /// `RoadTypeDistribution`.
    pub road_type_distribution: Vec<RoadTypeDistributionRow>,
    /// `SourceTypeDayVMT`.
    pub source_type_day_vmt: Vec<SourceTypeDayVmtRow>,
    /// `HPMSVTypeDay`.
    pub hpms_v_type_day: Vec<HpmsVTypeDayRow>,
    /// `MonthVMTFraction`.
    pub month_vmt_fraction: Vec<MonthVmtFractionRow>,
    /// `DayVMTFraction`.
    pub day_vmt_fraction: Vec<DayVmtFractionRow>,
    /// `HourVMTFraction`.
    pub hour_vmt_fraction: Vec<HourVmtFractionRow>,
    /// `HourDay`.
    pub hour_day: Vec<HourDayRow>,
    /// `DayOfAnyWeek`.
    pub day_of_any_week: Vec<DayOfAnyWeekRow>,
    /// `MonthOfAnyYear`.
    pub month_of_any_year: Vec<MonthOfAnyYearRow>,
    /// `SourceTypeHour`.
    pub source_type_hour: Vec<SourceTypeHourRow>,
    /// `RunSpecDay`.
    pub run_spec_day: Vec<RunSpecDayRow>,
    /// `AvgSpeedBin`.
    pub avg_speed_bin: Vec<AvgSpeedBinRow>,
    /// `AvgSpeedDistribution`.
    pub avg_speed_distribution: Vec<AvgSpeedDistributionRow>,
    /// `HourOfAnyDay`.
    pub hour_of_any_day: Vec<HourOfAnyDayRow>,
    /// `ZoneRoadType`.
    pub zone_road_type: Vec<ZoneRoadTypeRow>,
    /// `HotellingCalendarYear`.
    pub hotelling_calendar_year: Vec<HotellingCalendarYearRow>,
    /// `SampleVehicleDay`.
    pub sample_vehicle_day: Vec<SampleVehicleDayRow>,
    /// `SampleVehicleTrip`.
    pub sample_vehicle_trip: Vec<SampleVehicleTripRow>,
    /// `StartsPerVehicle` — the rows already present; [`run`](super::TotalActivityGenerator::run)
    /// computes new rows only for source types absent from it.
    pub starts_per_vehicle: Vec<StartsPerVehicleRow>,
}

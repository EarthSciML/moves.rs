//! Intermediate and output table types for the Total Activity Generator.
//!
//! Plain Rust mirrors of the working tables `TotalActivityGenerator.java`
//! builds in its execution database (`SourceTypeAgePopulation`,
//! `TravelFraction`, `VMTByAgeRoadwayHour`, …). The Java `CREATE TABLE` /
//! `TRUNCATE` scaffolding is pure database mechanics with no algorithmic
//! content, so it has no analogue here — these structs *are* the tables.
//!
//! Each step module ([`super::population`], [`super::travel`],
//! [`super::vmt`], [`super::activity`]) produces some of these rows and the
//! next step consumes them; [`TotalActivityOutput`] is the bundle
//! [`super::TotalActivityGenerator::run`] returns.
//!
//! As in [`super::inputs`], every identifier is an [`i32`] and every
//! quantity an [`f64`].

// ===========================================================================
// Step 120-139 — population.
// ===========================================================================

/// One `SourceTypeAgePopulation` row — vehicle population by `(year,
/// sourceType, age)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypeAgePopulationRow {
    /// `yearID`.
    pub year_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `ageID`.
    pub age_id: i32,
    /// `population` — vehicles of this type and age in the year.
    pub population: f64,
}

// ===========================================================================
// Step 140-149 — HPMS travel fraction.
// ===========================================================================

/// One `HPMSVTypePopulation` row — population rolled up to HPMS vehicle type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HpmsVTypePopulationRow {
    /// `yearID`.
    pub year_id: i32,
    /// `HPMSVTypeID`.
    pub hpms_v_type_id: i32,
    /// `population` — summed vehicle population for the HPMS type.
    pub population: f64,
}

/// One `FractionWithinHPMSVType` row — a source type/age's share of its
/// HPMS-type population.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FractionWithinHpmsVTypeRow {
    /// `yearID`.
    pub year_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `ageID`.
    pub age_id: i32,
    /// `fraction` — `population / HPMSVTypePopulation.population`.
    pub fraction: f64,
}

/// One `HPMSTravelFraction` row — the relative-MAR-weighted travel share of
/// an HPMS vehicle type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HpmsTravelFractionRow {
    /// `yearID`.
    pub year_id: i32,
    /// `HPMSVTypeID`.
    pub hpms_v_type_id: i32,
    /// `fraction` — `sum(FractionWithinHPMSVType * relativeMAR)`.
    pub fraction: f64,
}

/// One `TravelFraction` row — the share of an HPMS type's travel attributed
/// to a `(sourceType, age)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TravelFractionRow {
    /// `yearID`.
    pub year_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `ageID`.
    pub age_id: i32,
    /// `fraction` — `(FractionWithinHPMSVType * relativeMAR) /
    /// HPMSTravelFraction`.
    pub fraction: f64,
}

// ===========================================================================
// Step 150-159 — analysis-year VMT.
// ===========================================================================

/// One `AnalysisYearVMT` row — VMT grown to the analysis year, by HPMS type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AnalysisYearVmtRow {
    /// `yearID`.
    pub year_id: i32,
    /// `HPMSVTypeID`.
    pub hpms_v_type_id: i32,
    /// `VMT` — vehicle miles travelled.
    pub vmt: f64,
}

// ===========================================================================
// Step 160-169 — annual VMT by road type, source, and age.
// ===========================================================================

/// One `AnnualVMTByAgeRoadway` row — annual VMT split to `(roadType,
/// sourceType, age)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AnnualVmtByAgeRoadwayRow {
    /// `yearID`.
    pub year_id: i32,
    /// `roadTypeID`.
    pub road_type_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `ageID`.
    pub age_id: i32,
    /// `VMT` — annual vehicle miles travelled.
    pub vmt: f64,
}

// ===========================================================================
// Step 170-179 — hourly VMT.
// ===========================================================================

/// One `VMTByAgeRoadwayHour` row — VMT temporally allocated to an hour.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VmtByAgeRoadwayHourRow {
    /// `yearID`.
    pub year_id: i32,
    /// `roadTypeID`.
    pub road_type_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `ageID`.
    pub age_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `dayID`.
    pub day_id: i32,
    /// `hourID`.
    pub hour_id: i32,
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `VMT` — hourly vehicle miles travelled.
    pub vmt: f64,
}

/// One `vmtByMYRoadHourFraction` row — VMT fraction by model year within a
/// `(roadType, sourceType, month, hour, day)` cell.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VmtByMyRoadHourFractionRow {
    /// `yearID`.
    pub year_id: i32,
    /// `roadTypeID`.
    pub road_type_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `modelYearID` — derived as `yearID - ageID`.
    pub model_year_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `hourID`.
    pub hour_id: i32,
    /// `dayID`.
    pub day_id: i32,
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `vmtFraction` — `VMT / totalVMT` over the model-year cell.
    pub vmt_fraction: f64,
}

// ===========================================================================
// Step 180-189 — total-activity basis.
// ===========================================================================

/// One `AverageSpeed` row — activity-weighted average speed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AverageSpeedRow {
    /// `roadTypeID`.
    pub road_type_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `dayID`.
    pub day_id: i32,
    /// `hourID`.
    pub hour_id: i32,
    /// `averageSpeed` — `sum(avgBinSpeed * avgSpeedFraction)` (mph).
    pub average_speed: f64,
}

/// One `SHOByAgeRoadwayHour` row — source hours operating, with the VMT it
/// was derived from carried alongside.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShoByAgeRoadwayHourRow {
    /// `yearID`.
    pub year_id: i32,
    /// `roadTypeID`.
    pub road_type_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `ageID`.
    pub age_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `dayID`.
    pub day_id: i32,
    /// `hourID`.
    pub hour_id: i32,
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `SHO` — source hours operating (`VMT / averageSpeed`).
    pub sho: f64,
    /// `VMT` — the hourly VMT the `SHO` was derived from.
    pub vmt: f64,
}

/// One `VMTByAgeRoadwayDay` row — daily VMT with the hotelling hours derived
/// from it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VmtByAgeRoadwayDayRow {
    /// `yearID`.
    pub year_id: i32,
    /// `roadTypeID`.
    pub road_type_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `ageID`.
    pub age_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `dayID`.
    pub day_id: i32,
    /// `VMT` — daily vehicle miles travelled.
    pub vmt: f64,
    /// `hotellingHours` — hotelling hours, `VMT * SHOAllocFactor *
    /// hotellingRate`.
    pub hotelling_hours: f64,
}

/// One `IdleHoursByAgeHour` row — hotelling activity distributed to hours.
///
/// Despite the column name `idleHours`, the Java comment is explicit that
/// this holds *hotelling* activity (`hotellingHours * hotellingDist`), not
/// off-network idle.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct IdleHoursByAgeHourRow {
    /// `yearID`.
    pub year_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `ageID`.
    pub age_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `dayID`.
    pub day_id: i32,
    /// `hourID`.
    pub hour_id: i32,
    /// `idleHours` — hotelling hours allocated to this hour.
    pub idle_hours: f64,
}

/// One `StartsByAgeHour` row — engine starts by `(sourceType, year,
/// hourDay, age)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StartsByAgeHourRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `yearID`.
    pub year_id: i32,
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `ageID`.
    pub age_id: i32,
    /// `starts` — `population * startsPerVehicle`.
    pub starts: f64,
}

/// One `SHPByAgeHour` row — source hours parked by `(year, sourceType, age,
/// month, day, hour)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShpByAgeHourRow {
    /// `yearID`.
    pub year_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `ageID`.
    pub age_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `dayID`.
    pub day_id: i32,
    /// `hourID`.
    pub hour_id: i32,
    /// `SHP` — source hours parked, `(population * noOfRealDays) - sum(SHO)`.
    pub shp: f64,
}

// ===========================================================================
// Generator output bundle.
// ===========================================================================

/// The activity tables one [`run`](super::TotalActivityGenerator::run)
/// produces — the result of porting algorithm steps 110-189.
///
/// These are the year- and zone-level activity tables. Their per-link /
/// per-zone *spatial allocation* (steps 190-209: `SHO`, `SHP`,
/// `SourceHours`, `hotellingHours`, distance) is the
/// [`super::allocation`] kernel set, driven by the master loop's
/// per-`(process, zone, link)` iteration once the Task 50 data plane lands.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TotalActivityOutput {
    /// `SourceTypeAgePopulation` — population by `(year, sourceType, age)`.
    pub source_type_age_population: Vec<SourceTypeAgePopulationRow>,
    /// `SourceTypeAgeDistribution` rows the generator `INSERT IGNORE`s for
    /// the analysis year (step 130's by-product).
    pub source_type_age_distribution_additions: Vec<super::inputs::SourceTypeAgeDistributionRow>,
    /// `TravelFraction` — travel share by `(year, sourceType, age)`.
    pub travel_fraction: Vec<TravelFractionRow>,
    /// `AnalysisYearVMT` — VMT grown to the analysis year.
    pub analysis_year_vmt: Vec<AnalysisYearVmtRow>,
    /// `AnnualVMTByAgeRoadway` — annual VMT by `(roadType, sourceType, age)`.
    pub annual_vmt_by_age_roadway: Vec<AnnualVmtByAgeRoadwayRow>,
    /// `VMTByAgeRoadwayHour` — hourly VMT.
    pub vmt_by_age_roadway_hour: Vec<VmtByAgeRoadwayHourRow>,
    /// `vmtByMYRoadHourFraction` — model-year VMT fractions.
    pub vmt_by_my_road_hour_fraction: Vec<VmtByMyRoadHourFractionRow>,
    /// `SHOByAgeRoadwayHour` — source hours operating.
    pub sho_by_age_roadway_hour: Vec<ShoByAgeRoadwayHourRow>,
    /// `VMTByAgeRoadwayDay` — daily VMT and hotelling hours.
    pub vmt_by_age_roadway_day: Vec<VmtByAgeRoadwayDayRow>,
    /// `IdleHoursByAgeHour` — hotelling activity by hour.
    pub idle_hours_by_age_hour: Vec<IdleHoursByAgeHourRow>,
    /// `StartsByAgeHour` — engine starts.
    pub starts_by_age_hour: Vec<StartsByAgeHourRow>,
    /// `SHPByAgeHour` — source hours parked.
    pub shp_by_age_hour: Vec<ShpByAgeHourRow>,
}

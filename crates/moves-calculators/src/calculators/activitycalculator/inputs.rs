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

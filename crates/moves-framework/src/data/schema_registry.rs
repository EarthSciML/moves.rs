//! Canonical Polars schemas for all default-DB tables used by calculators.
//!
//! [`schema_registry`] returns a static map keyed by the table name exactly as
//! written in `Calculator::input_tables()` / `Generator::output_tables()`.
//! The value is a `fn() -> Schema` so callers get a fresh owned copy on each
//! call; the fn is a zero-capture function pointer, suitable for a static map.
//!
//! Tables whose column layout is not yet catalogued have an empty `Schema`.
//! [`DataFrameStoreTyped::insert_typed`](super::DataFrameStoreTyped::insert_typed) skips validation for those entries.
//!
//! # Naming convention
//!
//! Table names match the MariaDB table name verbatim: PascalCase for canonical
//! default-DB tables (e.g. `"SHO"`, `"SourceBin"`), camelCase for generator
//! scratch tables (e.g. `"sho"`, `"generalFuelRatio"`).
//!
//! # Type policy
//!
//! MariaDB `INT` / `SMALLINT` identifiers → `Int32`.
//! MariaDB `BIGINT` keys (notably `sourceBinID`) → `Int64`.
//! MariaDB `FLOAT` / `DOUBLE` quantities → `Float64`.
//! MariaDB `VARCHAR` / `CHAR` → `String`.

use std::collections::HashMap;
use std::sync::OnceLock;

use polars::prelude::{DataType, Schema};

/// All table names declared in any `Calculator::input_tables()` or
/// `Generator::output_tables()` call across the ported calculators.
///
/// Every entry in this list has a corresponding key in the registry returned
/// by [`schema_registry`]. If the schema has not been catalogued yet, the
/// entry maps to `fn() -> Schema::default()`.
pub const KNOWN_CALCULATOR_INPUT_TABLES: &[&str] = &[
 // ── PascalCase canonical default-DB tables ──────────────────────────────
    "ATRatio",
    "ATRatioGas2",
    "ATRatioNonGas",
    "AgeCategory",
    "AnalysisYearVMT",
    "AverageTankGasoline",
    "AverageTankTemperature",
    "AvgSpeedBin",
    "AvgSpeedDistribution",
    "BaseRate",
    "BaseRateByAge",
    "ColdSoakInitialHourFraction",
    "ColdSoakTankTemperature",
    "County",
    "CountyYear",
    "CrankcaseEmissionRatio",
    "CumTVVCoeffs",
    "DayOfAnyWeek",
    "DayVMTFraction",
    "DriveSchedule",
    "DriveScheduleAssoc",
    "DriveScheduleSecond",
    "DrivingIdleFraction",
    "ETOHBin",
    "EVEfficiency",
    "EmissionProcess",
    "EmissionRate",
    "EmissionRateAdjustment",
    "EmissionRateByAge",
    "ExtendedIdleEmissionRateFraction",
    "FuelFormulation",
    "FuelSubType",
    "FuelSubtype",
    "FuelSupply",
    "FuelType",
    "FullACAdjustment",
    "GREETWellToPump",
    "GeneralFuelRatio",
    "HCPermeationCoeff",
    "HCSpeciation",
    "HPMSVTypeDay",
    "HPMSVTypeYear",
    "HourDay",
    "HourOfAnyDay",
    "HourVMTFraction",
    "IMCoverage",
    "IMFactor",
    "Link",
    "MOVESWorkerOutput",
    "ModelYear",
    "ModelYearGroup",
    "MonthGroupHour",
    "MonthOfAnyYear",
    "MonthVMTFraction",
    "NONO2Ratio",
    "NOxHumidityAdjust",
    "OpModeDistribution",
    "OpModePolProcAssoc",
    "OperatingMode",
    "PM10EmissionRatio",
    "PMSpeciation",
    "Pollutant",
    "PollutantProcessAssoc",
    "PollutantProcessMappedModelYear",
    "PollutantProcessModelYear",
    "RatesOpModeDistribution",
    "RefuelingControlTechnology",
    "RefuelingFactors",
    "RegClassSourceTypeFraction",
    "RoadType",
    "RoadTypeDistribution",
    "RunSpecChainedTo",
    "RunSpecDay",
    "RunSpecHour",
    "RunSpecHourDay",
    "RunSpecModelYear",
    "RunSpecMonth",
    "RunSpecRoadType",
    "RunSpecSourceFuelType",
    "RunSpecSourceType",
    "SBWeightedDistanceRate",
    "SBWeightedEmissionRate",
    "SBWeightedEmissionRateByAge",
    "SHO",
    "SHP",
    "SampleVehicleDay",
    "SampleVehicleTrip",
    "ShorepowerEmissionRateFraction",
    "SoakActivityFraction",
    "SourceBin",
    "SourceBinDistribution",
    "SourceHours",
    "SourceTypeAge",
    "SourceTypeAgeDistribution",
    "SourceTypeDayVMT",
    "SourceTypeHour",
    "SourceTypeModelYear",
    "SourceTypeModelYearGroup",
    "SourceTypePhysics",
    "SourceTypePolProcess",
    "SourceTypeTechAdjustment",
    "SourceTypeYear",
    "SourceTypeYearVMT",
    "SourceUseType",
    "StartTempAdjustment",
    "Starts",
    "StartsPerVehicle",
    "SulfateEmissionRate",
    "TankTemperatureGroup",
    "TankTemperatureRise",
    "TankVaporGenCoeffs",
    "TemperatureAdjustment",
    "Year",
    "Zone",
    "ZoneMonthHour",
    "ZoneRoadType",
 // ── camelCase scratch / generator output tables ──────────────────────────
    "altCriteriaRatio",
    "apuEmissionRateFraction",
    "avgSpeedBin",
    "avgSpeedDistribution",
    "crankcaseEmissionRatio",
    "criteriaRatio",
    "dayVMTFraction",
    "dioxinEmissionRate",
    "driveSchedule",
    "driveScheduleAssoc",
    "driveScheduleSecond",
    "driveScheduleSecondLink",
    "evapRVPTemperatureAdjustment",
    "evapTemperatureAdjustment",
    "fuelUsageFraction",
    "generalFuelRatio",
    "generalFuelRatioExpression",
    "hotellingActivityDistribution",
    "hotellingCalendarYear",
    "hotellingHours",
    "hotellingHoursPerDay",
    "hourDay",
    "hourVMTFraction",
    "integratedSpeciesSet",
    "link",
    "linkAverageSpeed",
    "linkSourceTypeHour",
    "metalEmissionRate",
    "methaneTHCRatio",
    "minorHAPRatio",
    "monthVMTFraction",
    "nrATRatio",
    "nrDioxinEmissionRate",
    "nrHCSpeciation",
    "nrIntegratedSpecies",
    "nrMetalEmissionRate",
    "nrMethaneTHCRatio",
    "nrPAHGasRatio",
    "nrPAHParticleRatio",
    "offNetworkLink",
    "opModeDistribution",
    "opModePolProcAssoc",
    "operatingMode",
    "pahGasRatio",
    "pahParticleRatio",
    "pollutantProcessAssoc",
    "regionCounty",
    "roadType",
    "roadTypeDistribution",
    "runSpecHourDay",
    "runSpecPollutantProcess",
    "runSpecRoadType",
    "runSpecSourceFuelType",
    "runSpecSourceType",
    "sampleVehiclePopulation",
    "sampleVehicleSoaking",
    "sho",
    "smfrSBDSummary",
    "sourceHours",
    "sourceTypeAge",
    "sourceTypeAgeDistribution",
    "sourceTypeAgePopulation",
    "sourceTypeModelYear",
    "sourceTypePolProcess",
    "sourceTypeYear",
    "sourceUseType",
    "sourceUseTypePhysicsMapping",
    "startsOpModeDistribution",
    "stmyTVVCoeffs",
    "stmyTVVEquations",
    "sulfateFractions",
    "universalActivity",
    "vmtByMYRoadHourFraction",
    "year",
    "zoneACFactor",
    "zoneRoadType",
];

// ── Per-table schema builder functions ──────────────────────────────────────
// Each is a plain `fn() -> Schema` (no captures) so it can be stored as a
// function pointer in the static registry map.

fn sho_schema() -> Schema {
 // Columns read by the ported distance calculator's ShoRow. The full MOVES
 // SHO table also carries a "SHO" activity column (source hours operating),
 // but DistanceCalculator only reads the columns below.
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

fn source_bin_schema() -> Schema {
    Schema::from_iter([
        ("sourceBinID".into(), DataType::Int64),
        ("regClassID".into(), DataType::Int32),
        ("fuelTypeID".into(), DataType::Int32),
        ("engTechID".into(), DataType::Int32),
        ("hpID".into(), DataType::Int32),
        ("emissionConcept".into(), DataType::Int32),
    ])
}

fn source_bin_distribution_schema() -> Schema {
    Schema::from_iter([
        ("sourceTypeModelYearID".into(), DataType::Int32),
        ("polProcessID".into(), DataType::Int32),
        ("sourceBinID".into(), DataType::Int64),
        ("sourceBinActivityFraction".into(), DataType::Float64),
    ])
}

fn source_type_model_year_schema() -> Schema {
    Schema::from_iter([
        ("sourceTypeModelYearID".into(), DataType::Int32),
        ("sourceTypeID".into(), DataType::Int32),
        ("modelYearID".into(), DataType::Int32),
    ])
}

fn hour_day_schema() -> Schema {
    Schema::from_iter([
        ("hourDayID".into(), DataType::Int32),
        ("dayID".into(), DataType::Int32),
        ("hourID".into(), DataType::Int32),
    ])
}

fn link_schema() -> Schema {
    Schema::from_iter([
        ("linkID".into(), DataType::Int32),
        ("countyID".into(), DataType::Int32),
        ("zoneID".into(), DataType::Int32),
        ("roadTypeID".into(), DataType::Int32),
    ])
}

fn county_schema() -> Schema {
    Schema::from_iter([
        ("countyID".into(), DataType::Int32),
        ("stateID".into(), DataType::Int32),
        ("GPAFract".into(), DataType::Float64),
    ])
}

fn zone_schema() -> Schema {
    Schema::from_iter([
        ("zoneID".into(), DataType::Int32),
        ("countyID".into(), DataType::Int32),
        ("startAllocFactor".into(), DataType::Float64),
        ("idleAllocFactor".into(), DataType::Float64),
        ("SHPAllocFactor".into(), DataType::Float64),
    ])
}

fn zone_month_hour_schema() -> Schema {
    Schema::from_iter([
        ("monthID".into(), DataType::Int32),
        ("zoneID".into(), DataType::Int32),
        ("hourID".into(), DataType::Int32),
        ("temperature".into(), DataType::Float64),
        ("relHumidity".into(), DataType::Float64),
        ("heatIndex".into(), DataType::Float64),
        ("specificHumidity".into(), DataType::Float64),
    ])
}

fn year_schema() -> Schema {
    Schema::from_iter([
        ("yearID".into(), DataType::Int32),
        ("isBaseYear".into(), DataType::Int32),
        ("fuelYearID".into(), DataType::Int32),
    ])
}

fn emission_rate_schema() -> Schema {
    Schema::from_iter([
        ("sourceBinID".into(), DataType::Int64),
        ("polProcessID".into(), DataType::Int32),
        ("opModeID".into(), DataType::Int32),
        ("meanBaseRate".into(), DataType::Float64),
        ("meanBaseRateIM".into(), DataType::Float64),
    ])
}

fn emission_rate_by_age_schema() -> Schema {
    Schema::from_iter([
        ("sourceBinID".into(), DataType::Int64),
        ("polProcessID".into(), DataType::Int32),
        ("opModeID".into(), DataType::Int32),
        ("ageGroupID".into(), DataType::Int32),
        ("meanBaseRate".into(), DataType::Float64),
        ("meanBaseRateIM".into(), DataType::Float64),
    ])
}

fn source_hours_schema() -> Schema {
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

fn op_mode_distribution_schema() -> Schema {
    Schema::from_iter([
        ("sourceTypeID".into(), DataType::Int32),
        ("hourDayID".into(), DataType::Int32),
        ("linkID".into(), DataType::Int32),
        ("polProcessID".into(), DataType::Int32),
        ("opModeID".into(), DataType::Int32),
        ("opModeFraction".into(), DataType::Float64),
    ])
}

fn rates_op_mode_distribution_schema() -> Schema {
    Schema::from_iter([
        ("sourceTypeID".into(), DataType::Int32),
        ("roadTypeID".into(), DataType::Int32),
        ("polProcessID".into(), DataType::Int32),
        ("opModeID".into(), DataType::Int32),
        ("avgSpeedBinID".into(), DataType::Int32),
        ("opModeFraction".into(), DataType::Float64),
    ])
}

fn source_type_age_schema() -> Schema {
    Schema::from_iter([
        ("sourceTypeID".into(), DataType::Int32),
        ("yearID".into(), DataType::Int32),
        ("ageID".into(), DataType::Int32),
        ("ageFraction".into(), DataType::Float64),
    ])
}

fn source_type_year_schema() -> Schema {
    Schema::from_iter([
        ("yearID".into(), DataType::Int32),
        ("sourceTypeID".into(), DataType::Int32),
        ("salesGrowthFactor".into(), DataType::Float64),
        ("migrationRate".into(), DataType::Float64),
    ])
}

fn source_type_pol_process_schema() -> Schema {
    Schema::from_iter([
        ("sourceTypeID".into(), DataType::Int32),
        ("polProcessID".into(), DataType::Int32),
    ])
}

fn pollutant_process_assoc_schema() -> Schema {
    Schema::from_iter([
        ("polProcessID".into(), DataType::Int32),
        ("processID".into(), DataType::Int32),
        ("pollutantID".into(), DataType::Int32),
    ])
}

fn pollutant_process_mapped_model_year_schema() -> Schema {
    Schema::from_iter([
        ("polProcessID".into(), DataType::Int32),
        ("modelYearID".into(), DataType::Int32),
        ("modelYearGroupID".into(), DataType::Int32),
        ("fuelMYGroupID".into(), DataType::Int32),
        ("IMModelYearGroupID".into(), DataType::Int32),
    ])
}

fn pollutant_process_model_year_schema() -> Schema {
    Schema::from_iter([
        ("polProcessID".into(), DataType::Int32),
        ("modelYearID".into(), DataType::Int32),
        ("modelYearGroupID".into(), DataType::Int32),
    ])
}

fn fuel_supply_schema() -> Schema {
    Schema::from_iter([
        ("fuelRegionID".into(), DataType::Int32),
        ("fuelYearID".into(), DataType::Int32),
        ("monthGroupID".into(), DataType::Int32),
        ("fuelFormulationID".into(), DataType::Int32),
        ("marketShare".into(), DataType::Float64),
        ("marketShareCV".into(), DataType::Float64),
    ])
}

fn fuel_type_schema() -> Schema {
    Schema::from_iter([
        ("fuelTypeID".into(), DataType::Int32),
        ("defaultFormulationID".into(), DataType::Int32),
        ("fuelTypeDesc".into(), DataType::String),
        ("humidityCorrectionCoeff".into(), DataType::Float64),
    ])
}

fn fuel_subtype_schema() -> Schema {
    Schema::from_iter([
        ("fuelSubtypeID".into(), DataType::Int32),
        ("fuelTypeID".into(), DataType::Int32),
        ("fuelSubtypeDesc".into(), DataType::String),
        ("carbonContent".into(), DataType::Float64),
        ("oxidationFraction".into(), DataType::Float64),
        ("energyContent".into(), DataType::Float64),
    ])
}

fn full_ac_adjustment_schema() -> Schema {
    Schema::from_iter([
        ("sourceTypeID".into(), DataType::Int32),
        ("polProcessID".into(), DataType::Int32),
        ("opModeID".into(), DataType::Int32),
        ("fullACAdjustment".into(), DataType::Float64),
    ])
}

fn temperature_adjustment_schema() -> Schema {
    Schema::from_iter([
        ("polProcessID".into(), DataType::Int32),
        ("fuelTypeID".into(), DataType::Int32),
        ("minModelYearID".into(), DataType::Int32),
        ("maxModelYearID".into(), DataType::Int32),
        ("tempAdjustTermA".into(), DataType::Float64),
        ("tempAdjustTermB".into(), DataType::Float64),
    ])
}

fn model_year_schema() -> Schema {
    Schema::from_iter([("modelYearID".into(), DataType::Int32)])
}

fn im_coverage_schema() -> Schema {
    Schema::from_iter([
        ("polProcessID".into(), DataType::Int32),
        ("stateID".into(), DataType::Int32),
        ("countyID".into(), DataType::Int32),
        ("yearID".into(), DataType::Int32),
        ("sourceTypeID".into(), DataType::Int32),
        ("fuelTypeID".into(), DataType::Int32),
        ("IMProgramID".into(), DataType::Int32),
        ("begModelYearID".into(), DataType::Int32),
        ("endModelYearID".into(), DataType::Int32),
        ("inspectFreq".into(), DataType::Int32),
        ("testStandardsID".into(), DataType::Int32),
        ("useIMyn".into(), DataType::String),
        ("complianceFactor".into(), DataType::Float64),
    ])
}

fn im_factor_schema() -> Schema {
    Schema::from_iter([
        ("polProcessID".into(), DataType::Int32),
        ("inspectFreq".into(), DataType::Int32),
        ("testStandardsID".into(), DataType::Int32),
        ("sourceTypeID".into(), DataType::Int32),
        ("fuelTypeID".into(), DataType::Int32),
        ("IMModelYearGroupID".into(), DataType::Int32),
        ("ageGroupID".into(), DataType::Int32),
        ("IMFactor".into(), DataType::Float64),
    ])
}

fn avg_speed_bin_schema() -> Schema {
    Schema::from_iter([
        ("avgSpeedBinID".into(), DataType::Int32),
        ("avgBinSpeed".into(), DataType::Float64),
    ])
}

fn avg_speed_distribution_schema() -> Schema {
    Schema::from_iter([
        ("sourceTypeID".into(), DataType::Int32),
        ("roadTypeID".into(), DataType::Int32),
        ("hourDayID".into(), DataType::Int32),
        ("avgSpeedBinID".into(), DataType::Int32),
        ("avgSpeedFraction".into(), DataType::Float64),
    ])
}

fn month_of_any_year_schema() -> Schema {
    Schema::from_iter([
        ("monthID".into(), DataType::Int32),
        ("monthGroupID".into(), DataType::Int32),
        ("noOfDays".into(), DataType::Int32),
    ])
}

fn run_spec_hour_day_schema() -> Schema {
    Schema::from_iter([("hourDayID".into(), DataType::Int32)])
}

fn run_spec_source_fuel_type_schema() -> Schema {
    Schema::from_iter([
        ("fuelTypeID".into(), DataType::Int32),
        ("sourceTypeID".into(), DataType::Int32),
    ])
}

fn run_spec_source_type_schema() -> Schema {
    Schema::from_iter([("sourceTypeID".into(), DataType::Int32)])
}

fn sb_weighted_emission_rate_schema() -> Schema {
    Schema::from_iter([
        ("sourceTypeID".into(), DataType::Int32),
        ("polProcessID".into(), DataType::Int32),
        ("opModeID".into(), DataType::Int32),
        ("meanBaseRate".into(), DataType::Float64),
        ("meanBaseRateIM".into(), DataType::Float64),
    ])
}

fn sb_weighted_emission_rate_by_age_schema() -> Schema {
    Schema::from_iter([
        ("sourceTypeID".into(), DataType::Int32),
        ("polProcessID".into(), DataType::Int32),
        ("opModeID".into(), DataType::Int32),
        ("ageGroupID".into(), DataType::Int32),
        ("meanBaseRate".into(), DataType::Float64),
        ("meanBaseRateIM".into(), DataType::Float64),
    ])
}

fn sb_weighted_distance_rate_schema() -> Schema {
    Schema::from_iter([
        ("sourceTypeID".into(), DataType::Int32),
        ("roadTypeID".into(), DataType::Int32),
        ("polProcessID".into(), DataType::Int32),
        ("opModeID".into(), DataType::Int32),
        ("meanBaseRate".into(), DataType::Float64),
        ("meanBaseRateIM".into(), DataType::Float64),
    ])
}

fn driving_idle_fraction_schema() -> Schema {
    Schema::from_iter([
        ("sourceTypeID".into(), DataType::Int32),
        ("roadTypeID".into(), DataType::Int32),
        ("hourDayID".into(), DataType::Int32),
        ("drivingIdleFraction".into(), DataType::Float64),
    ])
}

fn stub_schema() -> Schema {
    Schema::default()
}

// ── Registry ─────────────────────────────────────────────────────────────────

type SchemaRegistry = HashMap<&'static str, fn() -> Schema>;
static REGISTRY: OnceLock<SchemaRegistry> = OnceLock::new();

/// Return the static schema registry.
///
/// Each entry maps a canonical table name to a `fn() -> Schema` that returns a
/// fresh owned schema. Tables with no catalogued schema map to a function that
/// returns an empty `Schema`; [`crate::data::DataFrameStoreTyped::insert_typed`]
/// skips validation for those.
pub fn schema_registry() -> &'static HashMap<&'static str, fn() -> Schema> {
    REGISTRY.get_or_init(build_registry)
}

fn build_registry() -> HashMap<&'static str, fn() -> Schema> {
    let mut m: HashMap<&'static str, fn() -> Schema> = HashMap::new();

 // ── Fully catalogued schemas ─────────────────────────────────────────────
    m.insert("SHO", sho_schema);
    m.insert("sho", sho_schema);
    m.insert("SourceBin", source_bin_schema);
    m.insert("SourceBinDistribution", source_bin_distribution_schema);
    m.insert("SourceTypeModelYear", source_type_model_year_schema);
    m.insert("sourceTypeModelYear", source_type_model_year_schema);
    m.insert("HourDay", hour_day_schema);
    m.insert("hourDay", hour_day_schema);
    m.insert("Link", link_schema);
    m.insert("link", link_schema);
    m.insert("County", county_schema);
    m.insert("Zone", zone_schema);
    m.insert("ZoneMonthHour", zone_month_hour_schema);
    m.insert("Year", year_schema);
    m.insert("year", year_schema);
    m.insert("EmissionRate", emission_rate_schema);
    m.insert("EmissionRateByAge", emission_rate_by_age_schema);
    m.insert("SourceHours", source_hours_schema);
    m.insert("sourceHours", source_hours_schema);
    m.insert("OpModeDistribution", op_mode_distribution_schema);
    m.insert("opModeDistribution", op_mode_distribution_schema);
    m.insert("RatesOpModeDistribution", rates_op_mode_distribution_schema);
    m.insert("SourceTypeAge", source_type_age_schema);
    m.insert("sourceTypeAge", source_type_age_schema);
    m.insert("SourceTypeAgeDistribution", source_type_age_schema);
    m.insert("sourceTypeAgeDistribution", source_type_age_schema);
    m.insert("SourceTypeYear", source_type_year_schema);
    m.insert("sourceTypeYear", source_type_year_schema);
    m.insert("SourceTypePolProcess", source_type_pol_process_schema);
    m.insert("sourceTypePolProcess", source_type_pol_process_schema);
    m.insert("PollutantProcessAssoc", pollutant_process_assoc_schema);
    m.insert("pollutantProcessAssoc", pollutant_process_assoc_schema);
    m.insert(
        "PollutantProcessMappedModelYear",
        pollutant_process_mapped_model_year_schema,
    );
    m.insert(
        "PollutantProcessModelYear",
        pollutant_process_model_year_schema,
    );
    m.insert("FuelSupply", fuel_supply_schema);
    m.insert("FuelType", fuel_type_schema);
    m.insert("FuelSubtype", fuel_subtype_schema);
    m.insert("FuelSubType", fuel_subtype_schema);
    m.insert("FullACAdjustment", full_ac_adjustment_schema);
    m.insert("TemperatureAdjustment", temperature_adjustment_schema);
    m.insert("ModelYear", model_year_schema);
    m.insert("IMCoverage", im_coverage_schema);
    m.insert("IMFactor", im_factor_schema);
    m.insert("AvgSpeedBin", avg_speed_bin_schema);
    m.insert("avgSpeedBin", avg_speed_bin_schema);
    m.insert("AvgSpeedDistribution", avg_speed_distribution_schema);
    m.insert("avgSpeedDistribution", avg_speed_distribution_schema);
    m.insert("MonthOfAnyYear", month_of_any_year_schema);
    m.insert("RunSpecHourDay", run_spec_hour_day_schema);
    m.insert("runSpecHourDay", run_spec_hour_day_schema);
    m.insert("RunSpecSourceFuelType", run_spec_source_fuel_type_schema);
    m.insert("runSpecSourceFuelType", run_spec_source_fuel_type_schema);
    m.insert("RunSpecSourceType", run_spec_source_type_schema);
    m.insert("runSpecSourceType", run_spec_source_type_schema);
    m.insert("SBWeightedEmissionRate", sb_weighted_emission_rate_schema);
    m.insert(
        "SBWeightedEmissionRateByAge",
        sb_weighted_emission_rate_by_age_schema,
    );
    m.insert("SBWeightedDistanceRate", sb_weighted_distance_rate_schema);
    m.insert("DrivingIdleFraction", driving_idle_fraction_schema);

 // ── Stub entries for remaining tables (schema not yet catalogued) ────────
    for &name in KNOWN_CALCULATOR_INPUT_TABLES {
        m.entry(name).or_insert(stub_schema);
    }

    m
}

#[cfg(test)]
mod tests {
    use polars::prelude::*;

    use super::*;
    use crate::data::conversions::TableRow;
    use crate::data::store::InMemoryStore;
    use crate::data::DataFrameStoreTyped;

 // ── Local ShoRow ─────────────────────────────────────────────────────────
 // Mirrors `ShoRow` in `moves-calculators` without a circular dependency.

    #[derive(Debug, Clone, PartialEq)]
    struct ShoRow {
        hour_day_id: i32,
        month_id: i32,
        year_id: i32,
        age_id: i32,
        link_id: i32,
        source_type_id: i32,
        distance: f64,
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
            let hour_day_ids: Vec<i32> = rows.iter().map(|r| r.hour_day_id).collect();
            let month_ids: Vec<i32> = rows.iter().map(|r| r.month_id).collect();
            let year_ids: Vec<i32> = rows.iter().map(|r| r.year_id).collect();
            let age_ids: Vec<i32> = rows.iter().map(|r| r.age_id).collect();
            let link_ids: Vec<i32> = rows.iter().map(|r| r.link_id).collect();
            let source_type_ids: Vec<i32> = rows.iter().map(|r| r.source_type_id).collect();
            let distances: Vec<f64> = rows.iter().map(|r| r.distance).collect();

            DataFrame::new(
                n,
                vec![
                    Series::new("hourDayID".into(), hour_day_ids).into(),
                    Series::new("monthID".into(), month_ids).into(),
                    Series::new("yearID".into(), year_ids).into(),
                    Series::new("ageID".into(), age_ids).into(),
                    Series::new("linkID".into(), link_ids).into(),
                    Series::new("sourceTypeID".into(), source_type_ids).into(),
                    Series::new("distance".into(), distances).into(),
                ],
            )
        }

        fn from_dataframe(df: &DataFrame) -> crate::Result<Vec<Self>> {
            let t = "SHO";
            macro_rules! col_i32 {
                ($col:expr) => {{
                    df.column($col)
                        .map_err(|e| crate::Error::RowExtraction {
                            table: t.into(),
                            row: 0,
                            column: $col.into(),
                            message: e.to_string(),
                        })?
                        .i32()
                        .map_err(|e| crate::Error::RowExtraction {
                            table: t.into(),
                            row: 0,
                            column: $col.into(),
                            message: e.to_string(),
                        })?
                }};
            }
            macro_rules! col_f64 {
                ($col:expr) => {{
                    df.column($col)
                        .map_err(|e| crate::Error::RowExtraction {
                            table: t.into(),
                            row: 0,
                            column: $col.into(),
                            message: e.to_string(),
                        })?
                        .f64()
                        .map_err(|e| crate::Error::RowExtraction {
                            table: t.into(),
                            row: 0,
                            column: $col.into(),
                            message: e.to_string(),
                        })?
                }};
            }

            let hour_day = col_i32!("hourDayID");
            let month = col_i32!("monthID");
            let year = col_i32!("yearID");
            let age = col_i32!("ageID");
            let link = col_i32!("linkID");
            let src_type = col_i32!("sourceTypeID");
            let dist = col_f64!("distance");
            let n = df.height();

            (0..n)
                .map(|i| {
                    let null = |col: &str| crate::Error::RowExtraction {
                        table: t.into(),
                        row: i,
                        column: col.into(),
                        message: "null value".into(),
                    };
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

 // ── WrongRow: claims to be SHO but has wrong columns ────────────────────

    #[derive(Debug, Clone)]
    struct WrongRow {
        wrong_column: i32,
    }

    impl TableRow for WrongRow {
        fn table_name() -> &'static str {
            "SHO"
        }
        fn polars_schema() -> Schema {
            Schema::from_iter([("wrongColumn".into(), DataType::Int32)])
        }
        fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
            let n = rows.len();
            let vals: Vec<i32> = rows.iter().map(|r| r.wrong_column).collect();
            DataFrame::new(n, vec![Series::new("wrongColumn".into(), vals).into()])
        }
        fn from_dataframe(_df: &DataFrame) -> crate::Result<Vec<Self>> {
            Ok(vec![])
        }
    }

 // ── Tests ────────────────────────────────────────────────────────────────

    #[test]
    fn registry_has_all_calculator_input_tables() {
        let reg = schema_registry();
        for &name in KNOWN_CALCULATOR_INPUT_TABLES {
            assert!(
                reg.contains_key(name),
                "schema_registry is missing entry for table '{name}'"
            );
        }
    }

    #[test]
    fn iter_typed_round_trips_for_sho_row() {
        let rows = vec![
            ShoRow {
                hour_day_id: 11,
                month_id: 7,
                year_id: 2020,
                age_id: 3,
                link_id: 1001,
                source_type_id: 21,
                distance: 42.5,
            },
            ShoRow {
                hour_day_id: 22,
                month_id: 8,
                year_id: 2021,
                age_id: 5,
                link_id: 1002,
                source_type_id: 31,
                distance: 17.0,
            },
        ];

        let mut store = InMemoryStore::new();
        store
            .insert_typed(rows.clone())
            .expect("insert_typed should succeed");
        let recovered: Vec<ShoRow> = store.iter_typed("SHO").expect("iter_typed should succeed");

        assert_eq!(recovered, rows);
    }

    #[test]
    fn insert_typed_rejects_schema_mismatch_with_clear_error() {
        let wrong_rows = vec![WrongRow { wrong_column: 1 }];
        let mut store = InMemoryStore::new();
        let err = store
            .insert_typed(wrong_rows)
            .expect_err("should fail on schema mismatch");

        match err {
            crate::Error::SchemaMismatch {
                table,
                expected,
                actual,
            } => {
                assert_eq!(table, "SHO");
 // Registry SHO schema has ≥ 7 columns; WrongRow declares 1
                assert!(
                    expected.len() > 1,
                    "expected should list registry SHO columns, got {expected:?}"
                );
                assert_eq!(actual, vec!["wrongColumn"]);
            }
            other => panic!("expected SchemaMismatch, got {other:?}"),
        }
    }
}

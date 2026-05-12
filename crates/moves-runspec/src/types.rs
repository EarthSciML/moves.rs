//! Data structures for a MOVES RunSpec.
//!
//! Mirrors `gov.epa.otaq.moves.master.runspec.RunSpec` and its supporting
//! enums/value classes from the canonical MOVES Java source. Field semantics
//! follow the Java; sort orders for collection-typed fields match the Java
//! `compareTo` so round-tripping through the XML form preserves order.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

// ---------- Enumerations ----------

/// `Model` (`gov.epa.otaq.moves.common.Model`). The simulation can run any
/// non-empty subset of these.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Model {
    /// On-road vehicle model.
    OnRoad,
    /// Off-road (NONROAD) vehicle model.
    NonRoad,
    /// Reserved third-model slot (`Model3` in Java).
    Model3,
}

impl Model {
    /// Canonical Java name (`ONROAD`, `NONROAD`, ...).
    pub fn as_str(self) -> &'static str {
        match self {
            Model::OnRoad => "ONROAD",
            Model::NonRoad => "NONROAD",
            Model::Model3 => "Model3",
        }
    }

    /// Parses the Java name (case-insensitive).
    pub fn from_str_ci(s: &str) -> Option<Self> {
        if s.eq_ignore_ascii_case("ONROAD") {
            Some(Model::OnRoad)
        } else if s.eq_ignore_ascii_case("NONROAD") {
            Some(Model::NonRoad)
        } else if s.eq_ignore_ascii_case("Model3") {
            Some(Model::Model3)
        } else {
            None
        }
    }
}

/// `ModelScale`. `MACROSCALE` is the inventory mode (`"Inv"`),
/// `MESOSCALE_LOOKUP` is the emission-rates mode (`"Rates"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ModelScale {
    /// Inventory-mode (Java `MACROSCALE`, serialized as `"Inv"`).
    Macroscale,
    /// Emission-rates lookup mode (Java `MESOSCALE_LOOKUP`, serialized as `"Rates"`).
    MesoscaleLookup,
}

impl ModelScale {
    /// Canonical Java serialized value (`Inv` or `Rates`).
    pub fn as_str(self) -> &'static str {
        match self {
            ModelScale::Macroscale => "Inv",
            ModelScale::MesoscaleLookup => "Rates",
        }
    }

    /// Parses the Java serialized value (case-insensitive).
    pub fn from_str_ci(s: &str) -> Option<Self> {
        if s.eq_ignore_ascii_case("Inv") || s.eq_ignore_ascii_case("MACROSCALE") {
            Some(ModelScale::Macroscale)
        } else if s.eq_ignore_ascii_case("Rates")
            || s.eq_ignore_ascii_case("MESOSCALE_LOOKUP")
            || s.eq_ignore_ascii_case("MESO")
        {
            Some(ModelScale::MesoscaleLookup)
        } else {
            None
        }
    }
}

/// `ModelDomain`. Controls how locations are sampled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ModelDomain {
    /// Java `NATIONAL_ALLOCATION`, serialized as `"DEFAULT"`.
    NationalAllocation,
    /// Java `SINGLE_COUNTY`, serialized as `"SINGLE"`.
    SingleCounty,
    /// Java `PROJECT`.
    Project,
}

impl ModelDomain {
    /// Canonical Java serialized value.
    pub fn as_str(self) -> &'static str {
        match self {
            ModelDomain::NationalAllocation => "DEFAULT",
            ModelDomain::SingleCounty => "SINGLE",
            ModelDomain::Project => "PROJECT",
        }
    }

    /// Parses the Java serialized value (case-insensitive). Accepts the
    /// deprecated `"NATIONAL"` alias for backwards compatibility with older
    /// runspecs.
    pub fn from_str_ci(s: &str) -> Option<Self> {
        if s.eq_ignore_ascii_case("DEFAULT") || s.eq_ignore_ascii_case("NATIONAL") {
            Some(ModelDomain::NationalAllocation)
        } else if s.eq_ignore_ascii_case("SINGLE") {
            Some(ModelDomain::SingleCounty)
        } else if s.eq_ignore_ascii_case("PROJECT") {
            Some(ModelDomain::Project)
        } else {
            None
        }
    }
}

/// `GeographicSelectionType`. The granularity ordering matches Java
/// (finer-to-coarser by `sortOrder`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GeographicSelectionType {
    /// Link-level selection.
    Link,
    /// Zone-level selection.
    Zone,
    /// County-level selection.
    County,
    /// State-level selection.
    State,
    /// Nation-level selection.
    Nation,
}

impl GeographicSelectionType {
    fn sort_order(self) -> i32 {
        match self {
            GeographicSelectionType::Link => 1,
            GeographicSelectionType::Zone => 2,
            GeographicSelectionType::County => 3,
            GeographicSelectionType::State => 4,
            GeographicSelectionType::Nation => 5,
        }
    }

    /// Canonical Java serialized value.
    pub fn as_str(self) -> &'static str {
        match self {
            GeographicSelectionType::Link => "LINK",
            GeographicSelectionType::Zone => "ZONE",
            GeographicSelectionType::County => "COUNTY",
            GeographicSelectionType::State => "STATE",
            GeographicSelectionType::Nation => "NATION",
        }
    }

    /// Parses the Java serialized value (case-insensitive).
    pub fn from_str_ci(s: &str) -> Option<Self> {
        if s.eq_ignore_ascii_case("LINK") {
            Some(GeographicSelectionType::Link)
        } else if s.eq_ignore_ascii_case("ZONE") {
            Some(GeographicSelectionType::Zone)
        } else if s.eq_ignore_ascii_case("COUNTY") {
            Some(GeographicSelectionType::County)
        } else if s.eq_ignore_ascii_case("STATE") {
            Some(GeographicSelectionType::State)
        } else if s.eq_ignore_ascii_case("NATION") {
            Some(GeographicSelectionType::Nation)
        } else {
            None
        }
    }
}

impl PartialOrd for GeographicSelectionType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for GeographicSelectionType {
    fn cmp(&self, other: &Self) -> Ordering {
        self.sort_order().cmp(&other.sort_order())
    }
}

/// `GeographicOutputDetailLevel`. Controls the geographic grain of output rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GeographicOutputDetailLevel {
    /// National rollup.
    Nation,
    /// State rollup.
    State,
    /// County rollup.
    County,
    /// Zone rollup.
    Zone,
    /// Road-type rollup.
    RoadType,
    /// Link-level detail.
    Link,
}

impl GeographicOutputDetailLevel {
    /// Canonical Java serialized value.
    pub fn as_str(self) -> &'static str {
        match self {
            GeographicOutputDetailLevel::Nation => "NATION",
            GeographicOutputDetailLevel::State => "STATE",
            GeographicOutputDetailLevel::County => "COUNTY",
            GeographicOutputDetailLevel::Zone => "ZONE",
            GeographicOutputDetailLevel::RoadType => "ROADTYPE",
            GeographicOutputDetailLevel::Link => "LINK",
        }
    }

    /// Parses the Java serialized value (case-insensitive).
    pub fn from_str_ci(s: &str) -> Option<Self> {
        if s.eq_ignore_ascii_case("NATION") {
            Some(GeographicOutputDetailLevel::Nation)
        } else if s.eq_ignore_ascii_case("STATE") {
            Some(GeographicOutputDetailLevel::State)
        } else if s.eq_ignore_ascii_case("COUNTY") {
            Some(GeographicOutputDetailLevel::County)
        } else if s.eq_ignore_ascii_case("ZONE") {
            Some(GeographicOutputDetailLevel::Zone)
        } else if s.eq_ignore_ascii_case("ROADTYPE") {
            Some(GeographicOutputDetailLevel::RoadType)
        } else if s.eq_ignore_ascii_case("LINK") {
            Some(GeographicOutputDetailLevel::Link)
        } else {
            None
        }
    }
}

/// `OutputTimeStep`. The temporal grain of output rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OutputTimeStep {
    /// Hour granularity.
    Hour,
    /// 24-hour day (`"24-Hour Day"`).
    ClassicalDay,
    /// Portion-of-week.
    PortionOfWeek,
    /// Month.
    Month,
    /// Year.
    Year,
}

impl OutputTimeStep {
    /// Canonical Java description, used as the value in the XML.
    pub fn as_str(self) -> &'static str {
        match self {
            OutputTimeStep::Hour => "Hour",
            OutputTimeStep::ClassicalDay => "24-Hour Day",
            OutputTimeStep::PortionOfWeek => "Portion of Week",
            OutputTimeStep::Month => "Month",
            OutputTimeStep::Year => "Year",
        }
    }

    /// Parses the Java description (case-insensitive).
    pub fn from_str_ci(s: &str) -> Option<Self> {
        if s.eq_ignore_ascii_case("Hour") {
            Some(OutputTimeStep::Hour)
        } else if s.eq_ignore_ascii_case("24-Hour Day") || s.eq_ignore_ascii_case("Day") {
            Some(OutputTimeStep::ClassicalDay)
        } else if s.eq_ignore_ascii_case("Portion of Week") {
            Some(OutputTimeStep::PortionOfWeek)
        } else if s.eq_ignore_ascii_case("Month") {
            Some(OutputTimeStep::Month)
        } else if s.eq_ignore_ascii_case("Year") {
            Some(OutputTimeStep::Year)
        } else {
            None
        }
    }
}

/// `TimeMeasurementSystem`. Units for the `<timefactors>` selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TimeMeasurementSystem {
    /// Seconds.
    Seconds,
    /// Hours.
    Hours,
    /// Days.
    Days,
    /// `"Portions of Week"`.
    PortionsOfWeek,
    /// Weeks.
    Weeks,
    /// Months.
    Months,
    /// Years.
    Years,
}

impl TimeMeasurementSystem {
    /// Canonical Java description.
    pub fn as_str(self) -> &'static str {
        match self {
            TimeMeasurementSystem::Seconds => "Seconds",
            TimeMeasurementSystem::Hours => "Hours",
            TimeMeasurementSystem::Days => "Days",
            TimeMeasurementSystem::PortionsOfWeek => "Portions of Week",
            TimeMeasurementSystem::Weeks => "Weeks",
            TimeMeasurementSystem::Months => "Months",
            TimeMeasurementSystem::Years => "Years",
        }
    }

    /// Parses the Java description (case-insensitive).
    pub fn from_str_ci(s: &str) -> Option<Self> {
        if s.eq_ignore_ascii_case("Seconds") {
            Some(TimeMeasurementSystem::Seconds)
        } else if s.eq_ignore_ascii_case("Hours") {
            Some(TimeMeasurementSystem::Hours)
        } else if s.eq_ignore_ascii_case("Days") {
            Some(TimeMeasurementSystem::Days)
        } else if s.eq_ignore_ascii_case("Portions of Week") {
            Some(TimeMeasurementSystem::PortionsOfWeek)
        } else if s.eq_ignore_ascii_case("Weeks") {
            Some(TimeMeasurementSystem::Weeks)
        } else if s.eq_ignore_ascii_case("Months") {
            Some(TimeMeasurementSystem::Months)
        } else if s.eq_ignore_ascii_case("Years") {
            Some(TimeMeasurementSystem::Years)
        } else {
            None
        }
    }
}

/// `DistanceMeasurementSystem`. Units for the `<distancefactors>` selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DistanceMeasurementSystem {
    /// Kilometers.
    Kilometers,
    /// International miles.
    Miles,
}

impl DistanceMeasurementSystem {
    /// Canonical Java description.
    pub fn as_str(self) -> &'static str {
        match self {
            DistanceMeasurementSystem::Kilometers => "Kilometers",
            DistanceMeasurementSystem::Miles => "Miles",
        }
    }

    /// Parses the Java description (case-insensitive).
    pub fn from_str_ci(s: &str) -> Option<Self> {
        if s.eq_ignore_ascii_case("Kilometers") {
            Some(DistanceMeasurementSystem::Kilometers)
        } else if s.eq_ignore_ascii_case("Miles") {
            Some(DistanceMeasurementSystem::Miles)
        } else {
            None
        }
    }
}

/// `MassMeasurementSystem`. Units for the mass column of `<massfactors>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MassMeasurementSystem {
    /// Kilograms.
    Kilograms,
    /// Grams.
    Grams,
    /// Pounds (treated as a unit of force in MOVES).
    Pounds,
    /// US tons (`"U.S. Ton"`).
    UsTon,
}

impl MassMeasurementSystem {
    /// Canonical Java description.
    pub fn as_str(self) -> &'static str {
        match self {
            MassMeasurementSystem::Kilograms => "Kilograms",
            MassMeasurementSystem::Grams => "Grams",
            MassMeasurementSystem::Pounds => "Pounds",
            MassMeasurementSystem::UsTon => "U.S. Ton",
        }
    }

    /// Parses the Java description (case-insensitive).
    pub fn from_str_ci(s: &str) -> Option<Self> {
        if s.eq_ignore_ascii_case("Kilograms") {
            Some(MassMeasurementSystem::Kilograms)
        } else if s.eq_ignore_ascii_case("Grams") {
            Some(MassMeasurementSystem::Grams)
        } else if s.eq_ignore_ascii_case("Pounds") {
            Some(MassMeasurementSystem::Pounds)
        } else if s.eq_ignore_ascii_case("U.S. Ton") {
            Some(MassMeasurementSystem::UsTon)
        } else {
            None
        }
    }
}

/// `EnergyMeasurementSystem`. Units for the energy column of `<massfactors>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum EnergyMeasurementSystem {
    /// Joules.
    Joules,
    /// Kilojoules.
    KiloJoules,
    /// Million BTU.
    MillionBtu,
}

impl EnergyMeasurementSystem {
    /// Canonical Java description.
    pub fn as_str(self) -> &'static str {
        match self {
            EnergyMeasurementSystem::Joules => "Joules",
            EnergyMeasurementSystem::KiloJoules => "KiloJoules",
            EnergyMeasurementSystem::MillionBtu => "Million BTU",
        }
    }

    /// Parses the Java description (case-insensitive).
    pub fn from_str_ci(s: &str) -> Option<Self> {
        if s.eq_ignore_ascii_case("Joules") {
            Some(EnergyMeasurementSystem::Joules)
        } else if s.eq_ignore_ascii_case("KiloJoules") {
            Some(EnergyMeasurementSystem::KiloJoules)
        } else if s.eq_ignore_ascii_case("Million BTU") {
            Some(EnergyMeasurementSystem::MillionBtu)
        } else {
            None
        }
    }
}

/// `Models.ModelCombination`. Bitmask of which models a road type applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ModelCombination {
    /// No models.
    M0,
    /// Onroad only.
    M1,
    /// Nonroad only.
    M2,
    /// Both onroad and nonroad.
    M12,
}

impl ModelCombination {
    /// Canonical Java serialized value.
    pub fn as_str(self) -> &'static str {
        match self {
            ModelCombination::M0 => "M0",
            ModelCombination::M1 => "M1",
            ModelCombination::M2 => "M2",
            ModelCombination::M12 => "M12",
        }
    }

    /// Parses the Java serialized value (case-insensitive).
    pub fn from_str_ci(s: &str) -> Self {
        if s.eq_ignore_ascii_case("M1") {
            ModelCombination::M1
        } else if s.eq_ignore_ascii_case("M2") {
            ModelCombination::M2
        } else if s.eq_ignore_ascii_case("M12") {
            ModelCombination::M12
        } else {
            ModelCombination::M0
        }
    }
}

// ---------- Value structs with custom ordering ----------

/// `GeographicSelection`. Ordering matches Java: granularity first
/// (finer-to-coarser), then `textDescription` (lex), then `databaseKey`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GeographicSelection {
    /// Granularity of the selection.
    pub type_: GeographicSelectionType,
    /// Primary key (county/zone/state/link/etc. id) in the MOVES default DB.
    pub database_key: i32,
    /// Human-readable text description.
    pub text_description: String,
}

impl PartialOrd for GeographicSelection {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for GeographicSelection {
    fn cmp(&self, other: &Self) -> Ordering {
        self.type_
            .cmp(&other.type_)
            .then_with(|| self.text_description.cmp(&other.text_description))
            .then_with(|| self.database_key.cmp(&other.database_key))
    }
}

/// `OnRoadVehicleSelection`. Ordered by (`sourceTypeID`, `fuelTypeID`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OnRoadVehicleSelection {
    /// Fuel-type primary key.
    pub fuel_type_id: i32,
    /// Fuel-type description (used only for `toString` in Java, retained here
    /// so round-trips preserve text).
    pub fuel_type_desc: String,
    /// Source-use-type primary key.
    pub source_type_id: i32,
    /// Source-use-type description.
    pub source_type_name: String,
}

impl PartialOrd for OnRoadVehicleSelection {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OnRoadVehicleSelection {
    fn cmp(&self, other: &Self) -> Ordering {
        self.source_type_id
            .cmp(&other.source_type_id)
            .then_with(|| self.fuel_type_id.cmp(&other.fuel_type_id))
    }
}

/// `OffRoadVehicleSelection`. Ordered case-insensitively by `sectorName`, then
/// `fuelTypeDesc`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OffRoadVehicleSelection {
    /// Fuel-type primary key.
    pub fuel_type_id: i32,
    /// Fuel-type description.
    pub fuel_type_desc: String,
    /// Sector primary key.
    pub sector_id: i32,
    /// Sector description (orderable text).
    pub sector_name: String,
}

impl PartialOrd for OffRoadVehicleSelection {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OffRoadVehicleSelection {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp_ignore_ascii_case(&self.sector_name, &other.sector_name)
            .then_with(|| cmp_ignore_ascii_case(&self.fuel_type_desc, &other.fuel_type_desc))
    }
}

/// `SCC`. The off-road SCC code is the only meaningful field for ordering.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Scc {
    /// The Source Classification Code string.
    pub code: String,
}

impl PartialOrd for Scc {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Scc {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp_ignore_ascii_case(&self.code, &other.code)
    }
}

/// `RoadType`. Java orders by `roadTypeID` only — entries with the same ID
/// are treated as equal regardless of name or model combination. We preserve
/// that here so a TreeSet-shaped collection round-trips.
#[derive(Debug, Clone)]
pub struct RoadType {
    /// Road-type primary key.
    pub road_type_id: i32,
    /// Road-type description.
    pub road_type_name: String,
    /// Which models the road type belongs to.
    pub model_combination: ModelCombination,
}

impl PartialEq for RoadType {
    fn eq(&self, other: &Self) -> bool {
        self.road_type_id == other.road_type_id
    }
}

impl Eq for RoadType {}

impl PartialOrd for RoadType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RoadType {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.road_type_id == other.road_type_id {
            Ordering::Equal
        } else {
            cmp_ignore_ascii_case(&self.road_type_name, &other.road_type_name)
        }
    }
}

impl std::hash::Hash for RoadType {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.road_type_id.hash(state);
    }
}

/// `PollutantProcessAssociation`. Java orders by pollutant (name, then
/// databaseKey), then by emission-process `displayKey`. Without the runtime
/// database we lack `displayKey`; we use `process_key` as a stable proxy.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PollutantProcessAssociation {
    /// Pollutant primary key.
    pub pollutant_key: i32,
    /// Pollutant display name.
    pub pollutant_name: String,
    /// Emission-process primary key.
    pub process_key: i32,
    /// Emission-process display name.
    pub process_name: String,
}

impl PartialOrd for PollutantProcessAssociation {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PollutantProcessAssociation {
    fn cmp(&self, other: &Self) -> Ordering {
        self.pollutant_name
            .cmp(&other.pollutant_name)
            .then_with(|| self.pollutant_key.cmp(&other.pollutant_key))
            .then_with(|| self.process_key.cmp(&other.process_key))
    }
}

/// `DatabaseSelection`. The userName/password fields in Java are runtime
/// concerns; the XML only carries server/database/description.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct DatabaseSelection {
    /// MariaDB server (often blank).
    pub server_name: String,
    /// Database name.
    pub database_name: String,
    /// Free-text description (escaped in the XML).
    pub description: String,
}

/// `TimeSpan`. Years, months, days, an hour range, and an optional aggregation
/// granularity.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TimeSpan {
    /// Selected years (ascending order).
    pub years: BTreeSet<i32>,
    /// Selected month IDs (1=Jan ... 12=Dec).
    pub months: BTreeSet<i32>,
    /// Selected day IDs (Java `dayID` values from the `dayOfAnyWeek` table).
    pub days: BTreeSet<i32>,
    /// Begin-of-day hour ID (1..=24).
    pub begin_hour_id: i32,
    /// End-of-day hour ID (1..=24).
    pub end_hour_id: i32,
    /// Optional aggregation granularity.
    pub aggregate_by: Option<OutputTimeStep>,
}

/// `UncertaintyParameters`. Carries the Monte-Carlo mode flag and counters
/// even though uncertainty mode is currently disabled.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct UncertaintyParameters {
    /// Whether uncertainty mode is enabled.
    pub uncertainty_mode_enabled: bool,
    /// Number of MOVES runs per simulation when uncertainty mode is enabled.
    pub number_of_runs_per_simulation: i32,
    /// Number of simulations to run.
    pub number_of_simulations: i32,
}

/// `OutputEmissionsBreakdownSelection`. Flags controlling output emission
/// breakdown columns.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OutputEmissionsBreakdownSelection {
    /// Break out by model year.
    pub model_year: bool,
    /// Break out by fuel type.
    pub fuel_type: bool,
    /// Break out by fuel sub-type.
    pub fuel_sub_type: bool,
    /// Break out by emission process.
    pub emission_process: bool,
    /// Break out by on-road vs off-road.
    pub onroad_offroad: bool,
    /// Break out by road type.
    pub road_type: bool,
    /// Break out by source-use type.
    pub source_use_type: bool,
    /// Break out by MOVES vehicle type.
    pub moves_vehicle_type: bool,
    /// Break out by on-road SCC.
    pub onroad_scc: bool,
    /// Break out by NONROAD sector / on-road segment.
    pub sector: bool,
    /// Break out by engine technology id (NONROAD).
    pub eng_tech_id: bool,
    /// Break out by horsepower class (NONROAD).
    pub hp_class: bool,
    /// Break out by regulatory class.
    pub reg_class_id: bool,
    /// Emit per-iteration uncertainty estimates.
    pub estimate_uncertainty: bool,
    /// Number of iterations for the uncertainty estimate.
    pub number_of_iterations: i32,
    /// Keep sampled data alongside aggregated output.
    pub keep_sampled_data: bool,
    /// Keep each iteration's output.
    pub keep_iterations: bool,
    /// Track whether the source XML mentioned `<fuelsubtype>`; if not, the
    /// serializer omits it for round-trip stability with older runspecs.
    pub had_fuel_sub_type: bool,
    /// Track whether `<engtechid>` was present in the source XML.
    pub had_eng_tech_id: bool,
    /// Track whether `<regclassid>` was present in the source XML.
    pub had_reg_class_id: bool,
}

/// `OutputFactors`. Units for the three output factor columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputFactors {
    /// Whether the `<timefactors>` selection is enabled.
    pub time_factors_selected: bool,
    /// Whether the `<distancefactors>` selection is enabled.
    pub distance_factors_selected: bool,
    /// Whether the `<massfactors>` selection is enabled.
    pub mass_factors_selected: bool,
    /// Time-units choice.
    pub time_measurement_system: Option<TimeMeasurementSystem>,
    /// Distance-units choice.
    pub distance_measurement_system: Option<DistanceMeasurementSystem>,
    /// Mass-units choice.
    pub mass_measurement_system: Option<MassMeasurementSystem>,
    /// Energy-units choice (paired with the mass factors).
    pub energy_measurement_system: Option<EnergyMeasurementSystem>,
}

impl Default for OutputFactors {
    fn default() -> Self {
        Self {
            time_factors_selected: true,
            distance_factors_selected: true,
            mass_factors_selected: true,
            time_measurement_system: None,
            distance_measurement_system: None,
            mass_measurement_system: None,
            energy_measurement_system: None,
        }
    }
}

/// `GenericCounty`. The optional user-defined county for project/single-county
/// runs. Stored as raw fields here since we do not run domain validation.
#[derive(Debug, Clone, PartialEq)]
pub struct GenericCounty {
    /// 1..=999 portion of the county id.
    pub short_county_id: i32,
    /// 1..=99 state FIPS.
    pub state_id: i32,
    /// Description text.
    pub description: String,
    /// True for high-altitude counties (the Java string was `"H"`).
    pub high_altitude: bool,
    /// Gas-pump fraction (0.0..=1.0).
    pub gpa_fraction: f32,
    /// Mean barometric pressure (inHg).
    pub barometric_pressure: f32,
    /// Refueling vapor-program adjustment.
    pub refueling_vapor_program_adjust: f32,
    /// Refueling spill-program adjustment.
    pub refueling_spill_program_adjust: f32,
}

impl Eq for GenericCounty {}

/// `InternalControlStrategy`. The Java code stores arbitrary control-strategy
/// blobs keyed by class name. We preserve them as opaque text so an XML round
/// trip survives even though we don't interpret them yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalControlStrategy {
    /// Java class name of the strategy.
    pub class_name: String,
    /// XML/TSV blob content as written by `RunSpecXML`.
    pub body: String,
    /// Whether the body was wrapped in CDATA in the source XML.
    pub is_cdata: bool,
}

/// Boolean flag plus an "was-it-present-in-the-XML" marker. Some fields are
/// optional in older runspecs; tracking presence lets the serializer match the
/// input shape.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct OptionalBool {
    /// The boolean value (only meaningful when `present`).
    pub value: bool,
    /// Whether the element/attribute was present in the source XML.
    pub present: bool,
}

impl OptionalBool {
    /// Construct from a value, marking it as present.
    pub fn present(value: bool) -> Self {
        Self {
            value,
            present: true,
        }
    }
}

// ---------- Top-level RunSpec ----------

/// In-memory representation of a MOVES RunSpec.
///
/// Mirrors the Java `RunSpec` plus the optional `<savedata>`, `<donotexecute>`,
/// `<generatordatabase>`, `<lookuptableflags>`, and
/// `<skipdomaindatabasevalidation>` blocks emitted by `RunSpecXML.save`.
#[derive(Debug, Clone, PartialEq)]
pub struct RunSpec {
    /// Optional version string from the `<runspec version="...">` attribute.
    pub version: String,
    /// Description text (CDATA-encoded in the XML).
    pub description: String,
    /// Which simulation models are active.
    pub models: Vec<Model>,
    /// Modeling scale (inventory vs. rates).
    pub scale: Option<ModelScale>,
    /// Modeling domain.
    pub domain: Option<ModelDomain>,
    /// Optional user-defined county.
    pub generic_county: Option<GenericCounty>,
    /// Geographic selections (sorted in TreeSet order).
    pub geographic_selections: BTreeSet<GeographicSelection>,
    /// Time span selection.
    pub time_span: TimeSpan,
    /// On-road vehicle selections.
    pub onroad_vehicle_selections: BTreeSet<OnRoadVehicleSelection>,
    /// Off-road vehicle selections.
    pub offroad_vehicle_selections: BTreeSet<OffRoadVehicleSelection>,
    /// Off-road vehicle SCC codes.
    pub offroad_vehicle_sccs: BTreeSet<Scc>,
    /// Road-type selections.
    pub road_types: BTreeSet<RoadType>,
    /// Pollutant/process associations.
    pub pollutant_process_associations: BTreeSet<PollutantProcessAssociation>,
    /// User database overrides (per Java `databaseSelectionInputSets`).
    pub database_selections: Vec<DatabaseSelection>,
    /// Internal control strategies, keyed by Java class name.
    pub internal_control_strategies: BTreeMap<String, Vec<InternalControlStrategy>>,
    /// `<inputdatabase>` reference.
    pub input_database: DatabaseSelection,
    /// `<uncertaintyparameters>` block.
    pub uncertainty_parameters: UncertaintyParameters,
    /// `<geographicoutputdetail>` value.
    pub geographic_output_detail: Option<GeographicOutputDetailLevel>,
    /// `<outputemissionsbreakdownselection>` block.
    pub output_emissions_breakdown_selection: OutputEmissionsBreakdownSelection,
    /// `<outputdatabase>` reference.
    pub output_database: DatabaseSelection,
    /// `<outputtimestep>` value.
    pub output_time_step: Option<OutputTimeStep>,
    /// `<outputvmtdata>` flag.
    pub output_vmt_data: bool,
    /// `<outputsho>` flag, with presence tracking for older runspecs.
    pub output_sho: OptionalBool,
    /// `<outputsh>` flag, with presence tracking.
    pub output_sh: OptionalBool,
    /// `<outputshp>` flag, with presence tracking.
    pub output_shp: OptionalBool,
    /// `<outputshidling>` flag, with presence tracking.
    pub output_sh_idling: OptionalBool,
    /// `<outputstarts>` flag, with presence tracking.
    pub output_starts: OptionalBool,
    /// `<outputpopulation>` flag, with presence tracking.
    pub output_population: OptionalBool,
    /// `<scaleinputdatabase>` reference.
    pub scale_input_database: DatabaseSelection,
    /// `<pmsize>` value.
    pub pm_size: i32,
    /// `<outputfactors>` block.
    pub output_factors: OutputFactors,
    /// Class names listed under `<savedata>`.
    pub classes_to_save_data: Vec<String>,
    /// Class names listed under `<donotexecute>`.
    pub classes_not_to_execute: Vec<String>,
    /// `<generatordatabase>` reference, only emitted when present in source XML.
    pub generator_database: Option<DatabaseSelection>,
    /// `shouldsave` attribute on `<generatordatabase>`.
    pub should_copy_saved_generator_data: bool,
    /// `<donotperformfinalaggregation selected="...">` flag.
    pub do_not_perform_final_aggregation: OptionalBool,
    /// `<lookuptableflags scenarioid="...">`. Empty string when absent.
    pub scenario_id: String,
    /// `<lookuptableflags truncateoutput="...">`.
    pub should_truncate_moves_output: bool,
    /// `<lookuptableflags truncateactivity="...">`.
    pub should_truncate_moves_activity_output: bool,
    /// `<lookuptableflags truncatebaserates="...">`.
    pub should_truncate_base_rate_output: bool,
    /// True when the source XML carried a `<lookuptableflags>` element.
    pub has_lookup_table_flags: bool,
    /// `<skipdomaindatabasevalidation selected="...">`.
    pub skip_domain_database_validation: OptionalBool,
    /// True when the source XML carried the deprecated Intercity-Bus source
    /// type and we auto-renamed it to `"Other Buses"` (Java sets a flag for
    /// the GUI to surface a warning).
    pub had_intercity_buses: bool,
}

impl Default for RunSpec {
    fn default() -> Self {
        RunSpec {
            version: String::new(),
            description: String::new(),
            models: vec![Model::OnRoad],
            scale: Some(ModelScale::Macroscale),
            domain: Some(ModelDomain::NationalAllocation),
            generic_county: None,
            geographic_selections: BTreeSet::new(),
            time_span: TimeSpan::default(),
            onroad_vehicle_selections: BTreeSet::new(),
            offroad_vehicle_selections: BTreeSet::new(),
            offroad_vehicle_sccs: BTreeSet::new(),
            road_types: BTreeSet::new(),
            pollutant_process_associations: BTreeSet::new(),
            database_selections: Vec::new(),
            internal_control_strategies: BTreeMap::new(),
            input_database: DatabaseSelection::default(),
            uncertainty_parameters: UncertaintyParameters::default(),
            geographic_output_detail: None,
            output_emissions_breakdown_selection: OutputEmissionsBreakdownSelection::default(),
            output_database: DatabaseSelection::default(),
            output_time_step: None,
            output_vmt_data: false,
            output_sho: OptionalBool::default(),
            output_sh: OptionalBool::default(),
            output_shp: OptionalBool::default(),
            output_sh_idling: OptionalBool::default(),
            output_starts: OptionalBool::default(),
            output_population: OptionalBool::default(),
            scale_input_database: DatabaseSelection::default(),
            pm_size: 0,
            output_factors: OutputFactors::default(),
            classes_to_save_data: Vec::new(),
            classes_not_to_execute: Vec::new(),
            generator_database: None,
            should_copy_saved_generator_data: false,
            do_not_perform_final_aggregation: OptionalBool::default(),
            scenario_id: String::new(),
            should_truncate_moves_output: true,
            should_truncate_moves_activity_output: true,
            should_truncate_base_rate_output: true,
            has_lookup_table_flags: false,
            skip_domain_database_validation: OptionalBool::default(),
            had_intercity_buses: false,
        }
    }
}

fn cmp_ignore_ascii_case(a: &str, b: &str) -> Ordering {
    let mut ai = a.bytes();
    let mut bi = b.bytes();
    loop {
        match (ai.next(), bi.next()) {
            (Some(x), Some(y)) => {
                let xl = x.to_ascii_lowercase();
                let yl = y.to_ascii_lowercase();
                match xl.cmp(&yl) {
                    Ordering::Equal => continue,
                    other => return other,
                }
            }
            (None, None) => return Ordering::Equal,
            (None, _) => return Ordering::Less,
            (_, None) => return Ordering::Greater,
        }
    }
}

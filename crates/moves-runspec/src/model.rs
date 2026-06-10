//! Canonical in-memory `RunSpec` model.
//!
//! Both the XML reader/writer (`xml_format`) and TOML reader/writer
//! (`toml_format`) project to and from this single representation; that
//! is what makes the two formats *isomorphic* in the sense asks
//! for. Conversion between the formats is always model-mediated:
//!
//! ```text
//! TOML <─ to_toml ── RunSpec ── to_xml ─> XML
//! │ ▲ │
//! └── from_toml ───────┴── from_xml ────────┘
//! ```
//!
//! The model deliberately stores `(id, name)` pairs for pollutants,
//! processes, road types, source types, and fuel types rather than
//! strongly-typed enums. (`Pollutant`/`EmissionProcess`/`SourceType`
//! /`RoadType` definitional code) will introduce the canonical enums and
//! lookup tables backed by `phf`; once that lands, the `_name` fields
//! become derivable and this module can drop them.

use serde::{Deserialize, Serialize};

/// A complete MOVES RunSpec — the inputs for one MOVES execution.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunSpec {
    pub version: Option<String>,
    pub description: Option<String>,
    pub models: Vec<Model>,
    pub scale: ModelScale,
    pub domain: Option<ModelDomain>,
    pub geographic_selections: Vec<GeographicSelection>,
    pub timespan: Timespan,
    pub onroad_vehicle_selections: Vec<OnroadVehicleSelection>,
    pub offroad_vehicle_selections: Vec<OffroadVehicleSelection>,
    pub offroad_vehicle_sccs: Vec<OffroadVehicleScc>,
    pub road_types: Vec<RoadType>,
    pub pollutant_process_associations: Vec<PollutantProcessAssociation>,
    pub database_selections: Vec<DatabaseSelection>,
    pub internal_control_strategies: Vec<InternalControlStrategy>,
    pub input_database: DatabaseRef,
    pub uncertainty: UncertaintyParameters,
    pub geographic_output_detail: GeographicOutputDetail,
    pub output_breakdown: OutputBreakdown,
    pub output_database: DatabaseRef,
    pub output_timestep: OutputTimestep,
    pub output_vmt_data: bool,
    pub output_nonroad: NonroadOutputFlags,
    pub scale_input_database: DatabaseRef,
    pub pm_size: u32,
    pub output_factors: OutputFactors,
}

/// `models > model[value]` — which engine the run drives.
///
/// MOVES supports independent ONROAD (light/heavy on-highway vehicles) and
/// NONROAD (off-highway equipment) models. A single RunSpec may select one
/// or both; the existing fixtures all select exactly one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Model {
    Onroad,
    Nonroad,
}

/// `modelscale[value]` — what kind of output the run produces.
///
/// The XML uses three distinct strings (`MACROSCALE`, `Inv`, `Rates`);
/// the TOML form uses the serde kebab-case slugs (`macro`, `inventory`,
/// `rates`). [`Self::xml_value`] converts back to the XML literal for
/// round-trip stability.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ModelScale {
    /// Legacy onroad inventory scale (`MACROSCALE`).
    #[default]
    Macro,
    /// Inventory output (`Inv`) — the standard NONROAD/onroad inventory mode.
    Inventory,
    /// Emission-rates output (`Rates`).
    Rates,
}

impl ModelScale {
    pub fn xml_value(self) -> &'static str {
        match self {
            Self::Macro => "MACROSCALE",
            Self::Inventory => "Inv",
            Self::Rates => "Rates",
        }
    }

    pub fn from_xml_value(s: &str) -> Option<Self> {
        match s {
            "MACROSCALE" => Some(Self::Macro),
            "Inv" => Some(Self::Inventory),
            "Rates" => Some(Self::Rates),
            _ => None,
        }
    }

    /// Whether this is an inventory (mass-output) scale — `Macro` (the legacy
    /// `MACROSCALE` string) or `Inventory` (`Inv`) — as opposed to `Rates`.
    /// Both produce mass inventories and share the same calculator behavior, so
    /// scale-sensitive logic must treat them identically; only `Rates` differs.
    #[must_use]
    pub fn is_inventory(self) -> bool {
        matches!(self, Self::Macro | Self::Inventory)
    }
}

/// `modeldomain[value]` — execution-domain mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ModelDomain {
    Default,
    Single,
    Project,
}

impl ModelDomain {
    pub fn xml_value(self) -> &'static str {
        match self {
            Self::Default => "DEFAULT",
            Self::Single => "SINGLE",
            Self::Project => "PROJECT",
        }
    }

    pub fn from_xml_value(s: &str) -> Option<Self> {
        match s {
            "DEFAULT" => Some(Self::Default),
            "SINGLE" => Some(Self::Single),
            "PROJECT" => Some(Self::Project),
            _ => None,
        }
    }
}

/// `geographicselection[type, key, description]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GeographicSelection {
    pub kind: GeoKind,
    pub key: u32,
    pub description: String,
}

/// `geographicselection@type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GeoKind {
    Nation,
    State,
    County,
    Zone,
    Link,
}

impl GeoKind {
    pub fn xml_value(self) -> &'static str {
        match self {
            Self::Nation => "NATION",
            Self::State => "STATE",
            Self::County => "COUNTY",
            Self::Zone => "ZONE",
            Self::Link => "LINK",
        }
    }

    pub fn from_xml_value(s: &str) -> Option<Self> {
        match s {
            "NATION" => Some(Self::Nation),
            "STATE" => Some(Self::State),
            "COUNTY" => Some(Self::County),
            "ZONE" => Some(Self::Zone),
            "LINK" => Some(Self::Link),
            _ => None,
        }
    }
}

/// `timespan` — the temporal extent of the run.
///
/// All child elements (`year`, `month`, `day`, `beginhour`, `endhour`,
/// `aggregateBy`) may appear zero or more times in canonical XML; we
/// model them as `Vec` for `year`/`month`/`day` and `Option`/scalar for
/// the others to match observed fixture patterns.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Timespan {
    pub years: Vec<u32>,
    pub months: Vec<u32>,
    pub days: Vec<u32>,
    pub begin_hour: Option<u32>,
    pub end_hour: Option<u32>,
    pub aggregate_by: Option<String>,
}

/// `onroadvehicleselection[fueltypeid, fueltypedesc, sourcetypeid, sourcetypename]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnroadVehicleSelection {
    pub fuel_type_id: u32,
    pub fuel_type_name: String,
    pub source_type_id: u32,
    pub source_type_name: String,
}

/// `offroadvehicleselection[fueltypeid, fueltypedesc, sectorid, sectorname]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OffroadVehicleSelection {
    pub fuel_type_id: u32,
    pub fuel_type_name: String,
    pub sector_id: u32,
    pub sector_name: String,
}

/// `offroadvehiclescc[scc, description]` — placeholder until non-empty
/// fixtures are added.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OffroadVehicleScc {
    pub scc: String,
    pub description: Option<String>,
}

/// `roadtype[roadtypeid, roadtypename, modelCombination?]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoadType {
    pub road_type_id: u32,
    pub road_type_name: String,
    pub model_combination: Option<String>,
}

/// `pollutantprocessassociation[pollutantkey, pollutantname, processkey, processname]`.
///
/// Stored as `(id, name)` pairs; introduces the canonical
/// pollutant/process enums and the names become derivable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PollutantProcessAssociation {
    pub pollutant_id: u32,
    pub pollutant_name: String,
    pub process_id: u32,
    pub process_name: String,
}

/// `databaseselection` — content-free in observed fixtures.
///
/// will extend this with the canonical attributes (server,
/// database, description, etc.) once a non-empty fixture appears.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseSelection {}

/// `internalcontrolstrategy` — a discriminated control-strategy declaration.
///
/// Ports `gov.epa.otaq.moves.master.runspec.RunSpec.internalControlStrategies`.
/// The Java field is a `LinkedList<InternalControlStrategy>` keyed by
/// fully-qualified class name; the enum variant here corresponds to that key.
///
/// The only variant currently relevant to the `hasRateOfProgress` gate is
/// [`InternalControlStrategy::RateOfProgress`]; all other class names are
/// captured in the [`InternalControlStrategy::Other`] variant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InternalControlStrategy {
    /// `RateOfProgressStrategy` with its `useParameters` flag.
    ///
    /// Ports `gov.epa.otaq.moves.master.implementation.ghg
    ///       .internalcontrolstrategies.rateofprogress
    ///       .RateOfProgressStrategy.useParameters`.
    /// `hasRateOfProgress()` returns `true` when this variant is present
    /// with `use_parameters: true`.
    RateOfProgress {
        /// Whether the strategy's parameter file is active (`useParameters Yes`).
        use_parameters: bool,
    },
    /// Any other class name — content preserved but not interpreted.
    Other {
        /// Fully-qualified Java class name from the `@classname` attribute.
        class_name: String,
    },
}

/// `inputdatabase`, `outputdatabase`, `scaleinputdatabase`.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseRef {
    pub server: String,
    pub database: String,
    pub description: String,
}

/// `uncertaintyparameters`.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UncertaintyParameters {
    pub enabled: bool,
    pub runs_per_simulation: u32,
    pub simulations: u32,
}

/// `geographicoutputdetail[description]` — output granularity level.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GeographicOutputDetail {
    Nation,
    State,
    #[default]
    County,
    Zone,
    Link,
}

impl GeographicOutputDetail {
    pub fn xml_value(self) -> &'static str {
        match self {
            Self::Nation => "NATION",
            Self::State => "STATE",
            Self::County => "COUNTY",
            Self::Zone => "ZONE",
            Self::Link => "LINK",
        }
    }

    pub fn from_xml_value(s: &str) -> Option<Self> {
        match s {
            "NATION" => Some(Self::Nation),
            "STATE" => Some(Self::State),
            "COUNTY" => Some(Self::County),
            "ZONE" => Some(Self::Zone),
            "LINK" => Some(Self::Link),
            _ => None,
        }
    }
}

/// `outputemissionsbreakdownselection` — boolean flags for emission-output dimensions.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputBreakdown {
    pub model_year: bool,
    pub fuel_type: bool,
    pub emission_process: bool,
    pub distinguish_particulates: bool,
    pub onroad_offroad: bool,
    pub road_type: bool,
    pub source_use_type: bool,
    pub moves_vehicle_type: bool,
    pub onroad_scc: bool,
    pub offroad_scc: bool,
    pub estimate_uncertainty: bool,
    pub segment: bool,
    pub hp_class: bool,
}

/// `outputtimestep[value]`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OutputTimestep {
    #[default]
    Hour,
    Day,
    Month,
    Year,
}

impl OutputTimestep {
    pub fn xml_value(self) -> &'static str {
        match self {
            Self::Hour => "Hour",
            Self::Day => "Day",
            Self::Month => "Month",
            Self::Year => "Year",
        }
    }

    pub fn from_xml_value(s: &str) -> Option<Self> {
        match s {
            "Hour" => Some(Self::Hour),
            "Day" => Some(Self::Day),
            "Month" => Some(Self::Month),
            "Year" => Some(Self::Year),
            _ => None,
        }
    }
}

/// NONROAD-only output toggles. Each XML element appears for NONROAD
/// fixtures and is absent for the legacy onroad sample.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NonroadOutputFlags {
    pub sho: Option<bool>,
    pub sh: Option<bool>,
    pub shp: Option<bool>,
    pub shidling: Option<bool>,
    pub starts: Option<bool>,
    pub population: Option<bool>,
}

impl NonroadOutputFlags {
    pub fn is_present(&self) -> bool {
        self.sho.is_some()
            || self.sh.is_some()
            || self.shp.is_some()
            || self.shidling.is_some()
            || self.starts.is_some()
            || self.population.is_some()
    }
}

/// `outputfactors` — units selection for the output tables.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputFactors {
    pub time: TimeFactor,
    pub distance: DistanceFactor,
    pub mass: MassFactor,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeFactor {
    pub enabled: bool,
    pub units: TimeUnit,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DistanceFactor {
    pub enabled: bool,
    pub units: DistanceUnit,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MassFactor {
    pub enabled: bool,
    pub units: MassUnit,
    pub energy_units: EnergyUnit,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TimeUnit {
    #[default]
    Seconds,
    Minutes,
    Hours,
    Days,
}

impl TimeUnit {
    pub fn xml_value(self) -> &'static str {
        match self {
            Self::Seconds => "Seconds",
            Self::Minutes => "Minutes",
            Self::Hours => "Hours",
            Self::Days => "Days",
        }
    }
    pub fn from_xml_value(s: &str) -> Option<Self> {
        match s {
            "Seconds" => Some(Self::Seconds),
            "Minutes" => Some(Self::Minutes),
            "Hours" => Some(Self::Hours),
            "Days" => Some(Self::Days),
            _ => None,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DistanceUnit {
    #[default]
    Miles,
    Kilometers,
}

impl DistanceUnit {
    pub fn xml_value(self) -> &'static str {
        match self {
            Self::Miles => "Miles",
            Self::Kilometers => "Kilometers",
        }
    }
    pub fn from_xml_value(s: &str) -> Option<Self> {
        match s {
            "Miles" => Some(Self::Miles),
            "Kilometers" => Some(Self::Kilometers),
            _ => None,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MassUnit {
    Pounds,
    Kilograms,
    #[default]
    Grams,
    /// Short tons (US, 2000 lb). XML: `Tons (short)`.
    TonsShort,
    /// Metric tonnes (1000 kg). XML: `Tons (metric)`.
    TonsMetric,
}

impl MassUnit {
    pub fn xml_value(self) -> &'static str {
        match self {
            Self::Pounds => "Pounds",
            Self::Kilograms => "Kilograms",
            Self::Grams => "Grams",
            Self::TonsShort => "Tons (short)",
            Self::TonsMetric => "Tons (metric)",
        }
    }
    pub fn from_xml_value(s: &str) -> Option<Self> {
        match s {
            "Pounds" => Some(Self::Pounds),
            "Kilograms" => Some(Self::Kilograms),
            "Grams" => Some(Self::Grams),
            "Tons (short)" => Some(Self::TonsShort),
            "Tons (metric)" => Some(Self::TonsMetric),
            _ => None,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EnergyUnit {
    #[default]
    MillionBtu,
    KiloJoules,
    Joules,
}

impl EnergyUnit {
    pub fn xml_value(self) -> &'static str {
        match self {
            Self::MillionBtu => "Million BTU",
            Self::KiloJoules => "KiloJoules",
            Self::Joules => "Joules",
        }
    }
    pub fn from_xml_value(s: &str) -> Option<Self> {
        match s {
            "Million BTU" => Some(Self::MillionBtu),
            "KiloJoules" => Some(Self::KiloJoules),
            "Joules" => Some(Self::Joules),
            _ => None,
        }
    }

    /// `EnergyMeasurementSystem.conversionToJoulesFactor` — Joules per one unit
    /// of this measurement system (`Joules` 1, `KiloJoules` 1000, `Million BTU`
    /// `1055.0559 × 1e6`).
    #[must_use]
    pub fn joules_per_unit(self) -> f64 {
        match self {
            Self::Joules => 1.0,
            Self::KiloJoules => 1000.0,
            Self::MillionBtu => 1055.0559 * 1_000_000.0,
        }
    }

    /// Factor that converts an energy quantity from MOVES' **base energy unit
    /// (kilojoules)** to this output unit. The canonical `OutputProcessor`
    /// rebases the kilojoule worker output to the run's `energyUnits`:
    /// `out = kJ × 1000 / joulesPerUnit(target)` (kJ → J, then J → target).
    #[must_use]
    pub fn factor_from_kilojoules(self) -> f64 {
        1000.0 / self.joules_per_unit()
    }
}

#[cfg(test)]
mod energy_unit_tests {
    use super::EnergyUnit;

    #[test]
    fn kilojoules_is_the_base_unit() {
        assert_eq!(EnergyUnit::KiloJoules.factor_from_kilojoules(), 1.0);
    }

    #[test]
    fn joules_scales_up_by_a_thousand() {
        assert_eq!(EnergyUnit::Joules.factor_from_kilojoules(), 1000.0);
    }

    #[test]
    fn million_btu_matches_the_canonical_constant() {
        // EnergyMeasurementSystem.MMBTU = 1055.0559 * 1e6 J per Million BTU;
        // kJ → MMBTU = 1000 / that. ~9.478e-7, the inverse of the empirically
        // observed ~1.055e6 over-emit before the conversion landed.
        let f = EnergyUnit::MillionBtu.factor_from_kilojoules();
        assert!((f - 1000.0 / (1055.0559 * 1_000_000.0)).abs() < 1e-18);
        assert!((1.0 / f - 1_055_055.9).abs() < 1.0);
    }
}

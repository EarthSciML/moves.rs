//! Replacement for the 65 Fortran COMMON blocks that hold NONROAD's
//! global state.
//!
//! The Fortran source uses 11 include files (`*.inc`) declaring 65
//! named COMMON blocks; these are imported into routines and act as
//! shared mutable state. The Rust port replaces them with typed
//! sub-structs grouped by purpose, owned by a top-level
//! [`NonroadContext`] passed explicitly between modules.
//!
//! Task 92 owns the design that splits state across the typed
//! sub-structs (one per include file is the starting heuristic);
//! Task 93 ports the parameter and chemistry constants from
//! `nonrdprm.inc` into [`consts`].
//!
//! See `DESIGN.md` for the full type architecture.

pub mod consts;
pub mod eqpcod;

use std::collections::HashMap;
use std::path::PathBuf;

/// Top-level container for all NONROAD execution state.
///
/// Replaces the implicit-global-via-COMMON pattern in the Fortran
/// source. All fields are owned here and passed explicitly between
/// modules via `&` or `&mut` references.
///
/// # Construction
///
/// The production path (Task 99) constructs this from a parsed `.opt`
/// file plus loaded input bundles. Unit tests may use [`NonroadContext::new`]
/// for "just enough" contexts.
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct NonroadContext {
    /// User options from `.opt` file
    pub options: UserOptions,

    /// Emission factor tables
    pub emissions: EmissionsState,

    /// Population, growth, and age data
    pub population: PopulationState,

    /// Allocation records
    pub allocation: AllocationState,

    /// Growth factor data
    pub growth: GrowthState,

    /// Seasonal and day-specific data
    pub seasonal: SeasonalState,

    /// Technology tables
    pub technology: TechnologyState,

    /// Equipment and SCC definitions
    pub equipment: EquipmentState,

    /// Geography and FIPS tables
    pub geography: GeographyState,

    /// Retrofit data
    pub retrofit: RetrofitState,

    /// Runtime state (updated during execution)
    pub current_year: i16,
    pub current_month: u8,
    pub current_day: u8,
    pub current_scc: i32,
    pub current_equipment_idx: usize,
    pub current_fips: i32,

    /// Output accumulators
    pub emission_outputs: Vec<EmissionOutput>,

    /// Warning messages
    pub warning_messages: Vec<WarningMessage>,
}

impl NonroadContext {
    /// Create an empty execution context.
    pub fn new() -> Self {
        Self::default()
    }
}

/// User options from `.opt` file
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct UserOptions {
    /// Input file paths
    pub pop_file: Option<PathBuf>,
    pub alo_file: Option<PathBuf>,
    pub grw_file: Option<PathBuf>,
    pub sea_file: Option<PathBuf>,
    pub emf_file: Option<PathBuf>,
    pub tch_file: Option<PathBuf>,
    pub rtrft_file: Option<PathBuf>,

    /// Output configuration
    pub output_dir: Option<PathBuf>,
    pub output_format: OutputFormat,

    /// Run parameters
    pub start_year: Option<i16>,
    pub end_year: Option<i16>,
    pub start_month: Option<u8>,
    pub end_month: Option<u8>,
    pub start_day: Option<u8>,
    pub end_day: Option<u8>,

    /// Geographic scope
    pub geography_level: GeographyLevel,
    pub state_codes: Vec<i32>,
    pub county_codes: Vec<i32>,

    /// Pollutant selection
    pub active_pollutants: Vec<i32>,

    /// Control strategy flags
    pub retrofit_enabled: bool,
    pub moves_mode: bool,

    /// Diagnostic flags
    pub debug_output: bool,
    pub verbose: bool,
}

/// Output format selection
#[derive(Debug, Default, Clone, Copy)]
pub enum OutputFormat {
    #[default]
    Legacy,
    Parquet,
    Both,
}

/// Geographic scope level
#[derive(Debug, Default, Clone, Copy)]
pub enum GeographyLevel {
    #[default]
    US,
    State,
    County,
    Subcounty,
}

/// Emission factor tables
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct EmissionsState {
    /// Exhaust emission factors
    pub exhaust_factors: HashMap<SccKey, ExhaustFactorTable>,

    /// Evaporative emission factors
    pub evaporative_factors: HashMap<SccKey, EvapFactorTable>,

    /// Deterioration factors
    pub deterioration_factors: HashMap<SccKey, DetFactorTable>,

    /// Unit conversion factors
    pub unit_conversions: UnitConversionTable,
}

/// Key for SCC-based lookups
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct SccKey {
    pub scc: i32,
}

/// Exhaust emission factor table
#[derive(Debug, Clone)]
pub struct ExhaustFactorTable {
    /// [equipment_idx][pollutant_idx][age_idx][operating_mode]
    pub rates: Vec<Vec<Vec<Vec<f64>>>>,
}

/// Evaporative emission factor table
#[derive(Debug, Clone)]
pub struct EvapFactorTable {
    pub rates: Vec<Vec<Vec<f64>>>,
}

/// Deterioration factor table
#[derive(Debug, Clone)]
pub struct DetFactorTable {
    pub factors: Vec<f64>,
}

/// Unit conversion factors
#[derive(Debug, Default, Clone)]
pub struct UnitConversionTable {
    pub mass_to_volume: HashMap<String, f64>,
    pub volume_to_mass: HashMap<String, f64>,
}

/// Population, growth, and age data
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct PopulationState {
    /// Population records
    pub populations: Vec<PopulationRecord>,

    /// Age distributions
    pub age_distributions: HashMap<AgeKey, Vec<f64>>,

    /// Growth factors
    pub growth_factors: HashMap<GrowthKey, Vec<f64>>,

    /// Base year populations
    pub base_populations: HashMap<BasePopKey, f64>,

    /// Scrappage rates
    pub scrappage_rates: Vec<f64>,

    /// Retrofit population adjustments
    pub retrofit_adjustments: Vec<RetrofitPopAdjustment>,
}

/// Population record
#[derive(Debug, Clone)]
pub struct PopulationRecord {
    pub scc: i32,
    pub equipment_idx: usize,
    pub fips: i32,
    pub population: f64,
    pub avg_hp: f64,
    pub use_hours: f64,
    pub model_year: i16,
}

/// Age distribution key
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct AgeKey {
    pub scc: i32,
    pub equipment_idx: usize,
    pub fips: i32,
}

/// Growth factor key
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct GrowthKey {
    pub scc: i32,
    pub equipment_idx: usize,
    pub fips: i32,
}

/// Base population key
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct BasePopKey {
    pub scc: i32,
    pub equipment_idx: usize,
    pub fips: i32,
    pub year: i16,
}

/// Retrofit population adjustment
#[derive(Debug, Clone)]
pub struct RetrofitPopAdjustment {
    pub scc: i32,
    pub equipment_idx: usize,
    pub fips: i32,
    pub adjustment: f64,
}

/// Allocation records
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct AllocationState {
    /// County allocation factors
    pub county_allocations: Vec<CountyAllocation>,

    /// State-to-county allocation factors
    pub state_to_county: Vec<StateCountyAllocation>,

    /// Subcounty allocation factors
    pub subcounty_allocations: Vec<SubcountyAllocation>,

    /// Zone mappings
    pub zone_mappings: HashMap<i32, Vec<ZoneRecord>>,
}

/// County allocation
#[derive(Debug, Clone)]
pub struct CountyAllocation {
    pub fips: i32,
    pub factor: f64,
}

/// State-to-county allocation
#[derive(Debug, Clone)]
pub struct StateCountyAllocation {
    pub state_code: i32,
    pub county_code: i32,
    pub factor: f64,
}

/// Subcounty allocation
#[derive(Debug, Clone)]
pub struct SubcountyAllocation {
    pub fips: i32,
    pub subcounty_id: i32,
    pub factor: f64,
}

/// Zone record
#[derive(Debug, Clone)]
pub struct ZoneRecord {
    pub zone_id: i32,
    pub zone_name: String,
}

/// Growth factor data
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct GrowthState {
    /// Growth factor tables by year
    pub growth_table: Vec<GrowthRecord>,

    /// Growth adjustment factors
    pub adjustment_factors: Vec<GrowthAdjustment>,

    /// Year indices
    pub year_indices: Vec<i16>,
}

/// Growth record
#[derive(Debug, Clone)]
pub struct GrowthRecord {
    pub scc: i32,
    pub equipment_idx: usize,
    pub fips: i32,
    pub year: i16,
    pub factor: f64,
}

/// Growth adjustment
#[derive(Debug, Clone)]
pub struct GrowthAdjustment {
    pub scc: i32,
    pub equipment_idx: usize,
    pub adjustment: f64,
}

/// Seasonal and day-specific data
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SeasonalState {
    /// Seasonal factors by month
    pub monthly_factors: Vec<MonthlyFactor>,

    /// Day-specific adjustments
    pub day_adjustments: Vec<DayAdjustment>,

    /// Day-of-year fractions
    pub day_fractions: [f64; 365],

    /// Month-to-day mappings
    pub month_days: [i32; 12],
}

impl Default for SeasonalState {
    fn default() -> Self {
        Self {
            monthly_factors: Vec::new(),
            day_adjustments: Vec::new(),
            day_fractions: [0.0; 365],
            month_days: [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
        }
    }
}

/// Monthly factor
#[derive(Debug, Clone)]
pub struct MonthlyFactor {
    pub month: u8,
    pub factor: f64,
}

/// Day adjustment
#[derive(Debug, Clone)]
pub struct DayAdjustment {
    pub day_of_year: i32,
    pub adjustment: f64,
}

/// Technology tables
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct TechnologyState {
    /// Exhaust technology tables
    pub exhaust_technologies: Vec<TechnologyRecord>,

    /// Evaporative technology tables
    pub evaporative_technologies: Vec<EvapTechnologyRecord>,

    /// Technology mappings by SCC
    pub scc_technology_map: HashMap<i32, Vec<TechMapping>>,
}

/// Technology record
#[derive(Debug, Clone)]
pub struct TechnologyRecord {
    pub scc: i32,
    pub tech_idx: usize,
    pub tech_type: TechType,
    pub control_efficiency: f64,
}

/// Technology type
#[derive(Debug, Default, Clone, Copy)]
pub enum TechType {
    #[default]
    None,
    Basic,
    Advanced,
    Certified,
}

/// Evaporative technology record
#[derive(Debug, Clone)]
pub struct EvapTechnologyRecord {
    pub scc: i32,
    pub evap_tech_idx: usize,
    pub evap_type: EvapTechType,
    pub control_efficiency: f64,
}

/// Evaporative technology type
#[derive(Debug, Default, Clone, Copy)]
pub enum EvapTechType {
    #[default]
    None,
    Stage1,
    Stage2,
    Enhanced,
}

/// Technology mapping
#[derive(Debug, Clone)]
pub struct TechMapping {
    pub tech_idx: usize,
    pub penetration: f64,
}

/// Equipment and SCC definitions
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct EquipmentState {
    /// Equipment category definitions
    pub equipment_categories: Vec<EquipmentCategory>,

    /// SCC definitions
    pub scc_definitions: Vec<SccDefinition>,

    /// SCC to equipment mappings
    pub scc_to_equipment: HashMap<i32, Vec<EquipmentMapping>>,

    /// Horsepower categories
    pub hp_categories: Vec<HpCategory>,
}

/// Equipment category
#[derive(Debug, Clone)]
pub struct EquipmentCategory {
    pub idx: usize,
    pub name: String,
    pub base_year: i16,
}

/// SCC definition
#[derive(Debug, Clone)]
pub struct SccDefinition {
    pub scc: i32,
    pub description: String,
    pub category: SccCategory,
}

/// SCC category
#[derive(Debug, Default, Clone)]
pub enum SccCategory {
    #[default]
    Unknown,
    OnRoad,
    NonRoad,
    Area,
    Point,
}

/// Equipment mapping
#[derive(Debug, Clone)]
pub struct EquipmentMapping {
    pub equipment_idx: usize,
    pub fraction: f64,
}

/// Horsepower category
#[derive(Debug, Clone)]
pub struct HpCategory {
    pub idx: usize,
    pub min_hp: f64,
    pub max_hp: f64,
    pub name: String,
}

/// Geography and FIPS tables
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct GeographyState {
    /// FIPS code tables
    pub fips_tables: FipsTables,

    /// State definitions
    pub states: Vec<StateDefinition>,

    /// County definitions
    pub counties: Vec<CountyDefinition>,

    /// Subcounty definitions
    pub subcounties: Vec<SubcountyDefinition>,

    /// Geographic allocation tables
    pub geo_allocations: Vec<GeoAllocation>,
}

/// FIPS code tables
#[derive(Debug, Default, Clone)]
pub struct FipsTables {
    /// FIPS → state mapping
    pub fips_to_state: HashMap<i32, i32>,

    /// State → FIPS list
    pub state_to_fips: HashMap<i32, Vec<i32>>,

    /// State codes
    pub state_codes: Vec<i32>,

    /// County codes
    pub county_codes: Vec<i32>,
}

/// State definition
#[derive(Debug, Clone)]
pub struct StateDefinition {
    pub code: i32,
    pub name: String,
    pub fips_start: i32,
    pub fips_end: i32,
}

/// County definition
#[derive(Debug, Clone)]
pub struct CountyDefinition {
    pub fips: i32,
    pub state_code: i32,
    pub county_code: i32,
    pub name: String,
    pub zone: i32,
}

/// Subcounty definition
#[derive(Debug, Clone)]
pub struct SubcountyDefinition {
    pub fips: i32,
    pub subcounty_id: i32,
    pub name: String,
}

/// Geographic allocation
#[derive(Debug, Clone)]
pub struct GeoAllocation {
    pub from_fips: i32,
    pub to_fips: i32,
    pub factor: f64,
}

/// Retrofit data
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct RetrofitState {
    /// Retrofit records
    pub retrofits: Vec<RetrofitRecord>,

    /// Retrofit technology mappings
    pub retrofit_tech_map: HashMap<i32, RetrofitTechMapping>,

    /// Retrofit effectiveness tables
    pub effectiveness: Vec<RetrofitEffectiveness>,

    /// Validation flags
    pub validation_errors: Vec<RetrofitValidationError>,
}

/// Retrofit record
#[derive(Debug, Clone)]
pub struct RetrofitRecord {
    pub scc: i32,
    pub equipment_idx: usize,
    pub fips: i32,
    pub retrofit_type: i32,
    pub year_installed: i16,
    pub effectiveness: f64,
}

/// Retrofit technology mapping
#[derive(Debug, Clone)]
pub struct RetrofitTechMapping {
    pub retrofit_type: i32,
    pub tech_idx: usize,
}

/// Retrofit effectiveness
#[derive(Debug, Clone)]
pub struct RetrofitEffectiveness {
    pub retrofit_type: i32,
    pub pollutant: i32,
    pub efficiency: f64,
}

/// Retrofit validation error
#[derive(Debug, Clone)]
pub struct RetrofitValidationError {
    pub record_idx: usize,
    pub error_code: i32,
    pub message: String,
}

/// Emission output record
#[derive(Debug, Clone)]
pub struct EmissionOutput {
    pub scc: i32,
    pub equipment_idx: usize,
    pub fips: i32,
    pub year: i16,
    pub month: u8,
    pub day: u8,
    pub pollutant: i32,
    pub emission_rate: f64,
    pub units: String,
}

/// Warning message
#[derive(Debug, Clone)]
pub struct WarningMessage {
    pub code: i32,
    pub message: String,
    pub context: Option<String>,
}

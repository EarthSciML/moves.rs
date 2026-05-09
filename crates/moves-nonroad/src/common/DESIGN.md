# Task 92: COMMON Block Replacement Design

## Overview

The 65 named COMMON blocks across 11 Fortran include files hold all of NONROAD's global state. This document defines the typed Rust structs that replace them.

**Design principle**: One Rust struct per include file, with `nonrdprm.inc`'s parameters folded into `common::consts`. All structs are owned by [`NonroadContext`] and passed explicitly between modules.

## The 11 Include Files

Based on the Fortran source audit (`NONROAD/NR08a/SOURCE/`):

| # | Include File | Lines | COMMON Blocks | Primary Role |
|---|--------------|-------|---------------|--------------|
| 1 | `nonrdprm.inc` | 732 | (parameters only) | Array dimensions + chemical/constants |
| 2 | `nonrdusr.inc` | ~200 | 1 | User options from `.opt` file |
| 3 | `nonrdefc.inc` | ~300 | 1 | Emission factor tables |
| 4 | `nonrdpop.inc` | ~150 | 1 | Population records |
| 5 | `nonrdalo.inc` | ~100 | 1 | Allocation records |
| 6 | `nonrdgrw.inc` | ~100 | 1 | Growth factor data |
| 7 | `nonrdsea.inc` | ~150 | 1 | Seasonal/day data |
| 8 | `nonrdtec.inc` | ~200 | 1 | Technology tables |
| 9 | `nonrdegc.inc` | ~150 | 1 | Equipment/SCC data |
| 10 | `nonrdgeo.inc` | ~250 | 1 | Geography tables |
| 11 | `nonrdrtr.inc` | ~200 | 1 | Retrofit data |

**Total**: 11 files, 65 COMMON blocks, ~2,433 lines

## Struct Mapping

### 1. `common::consts` (from `nonrdprm.inc`)

**Task 93** handles the full port. This is the constants module (already exists).

```rust
pub mod consts {
    // Array dimension parameters (documentation only)
    pub const MXEQIP: usize = 25;
    pub const MXPOL: usize = 23;
    pub const NSTATE: usize = 53;
    pub const NCNTY: usize = 3400;
    pub const MXTECH: usize = 15;
    pub const MXEVTECH: usize = 15;
    pub const MXHPC: usize = 18;
    pub const MXAGYR: usize = 51;
    pub const MXDAYS: usize = 365;
    pub const MXSUBC: usize = 300;
    pub const MXEMFC: usize = 13_000;
    pub const MXDTFC: usize = 120;
    pub const MXPOP: usize = 1_000;
    
    // Chemical and conversion constants (Task 93)
    pub const DENGAS: f64 = 6.237;  // Density of gas (lb/gal)
    // ... more constants
}
```

### 2. `UserOptions` (from `nonrdusr.inc`)

**COMMON blocks**: `USR` (user options)

```rust
#[derive(Debug, Default, Clone)]
pub struct UserOptions {
    /// Input file paths
    pub pop_file: PathBuf,
    pub alo_file: PathBuf,
    pub grw_file: PathBuf,
    pub sea_file: PathBuf,
    pub emf_file: PathBuf,
    pub tch_file: PathBuf,
    pub rtrft_file: Option<PathBuf>,
    
    /// Output configuration
    pub output_dir: PathBuf,
    pub output_format: OutputFormat,  // legacy, parquet, or both
    
    /// Run parameters
    pub start_year: u16,
    pub end_year: u16,
    pub start_month: u8,
    pub end_month: u8,
    pub start_day: u8,
    pub end_day: u8,
    
    /// Geographic scope
    pub geography_level: GeographyLevel,  // US, STATE, COUNTY, SUBCOUNTY
    pub state_codes: Vec<i32>,
    pub county_codes: Vec<i32>,
    
    /// Pollutant selection
    pub active_pollutants: Vec<Pollutant>,
    
    /// Control strategy flags
    pub retrofit_enabled: bool,
    pub moves_mode: bool,
    
    /// Diagnostic flags
    pub debug_output: bool,
    pub verbose: bool,
}
```

### 3. `EmissionsState` (from `nonrdefc.inc`)

**COMMON blocks**: `EMF` (emission factors), related tables

```rust
#[derive(Debug, Default, Clone)]
pub struct EmissionsState {
    /// Exhaust emission factors: [SCC][eqip][pollutant][age][mode]
    pub exhaust_factors: HashMap<SccKey, ExhaustFactorTable>,
    
    /// Evaporative emission factors
    pub evaporative_factors: HashMap<SccKey, EvapFactorTable>,
    
    /// Deterioration factors
    pub deterioration_factors: HashMap<SccKey, DetFactorTable>,
    
    /// Unit conversion factors
    pub unit_conversions: UnitConversionTable,
}

#[derive(Debug, Clone)]
pub struct ExhaustFactorTable {
    /// [equipment_idx][pollutant_idx][age_idx][operating_mode]
    pub rates: Vec<Vec<Vec<Vec<f64>>>>,
}

#[derive(Debug, Clone)]
pub struct EvapFactorTable {
    // Similar structure for evaporative emissions
    pub rates: Vec<Vec<Vec<f64>>>,
}

#[derive(Debug, Clone)]
pub struct DetFactorTable {
    /// Deterioration multipliers by age
    pub factors: Vec<f64>,
}
```

### 4. `PopulationState` (from `nonrdpop.inc`)

**COMMON blocks**: `POP` (population), `AGE` (age distribution), `GRO` (growth)

```rust
#[derive(Debug, Default, Clone)]
pub struct PopulationState {
    /// Population records: [SCC][equipment][FIPS]
    pub populations: Vec<PopulationRecord>,
    
    /// Age distributions: [SCC][equipment][FIPS][age]
    pub age_distributions: HashMap<AgeKey, Vec<f64>>,
    
    /// Growth factors: [SCC][equipment][FIPS][year]
    pub growth_factors: HashMap<GrowthKey, Vec<f64>>,
    
    /// Base year populations
    pub base_populations: HashMap<BasePopKey, f64>,
    
    /// Scrappage rates
    pub scrappage_rates: Vec<f64>,
    
    /// Retrofit population adjustments
    pub retrofit_adjustments: Vec<RetrofitPopAdjustment>,
}

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

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct AgeKey {
    pub scc: i32,
    pub equipment_idx: usize,
    pub fips: i32,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct GrowthKey {
    pub scc: i32,
    pub equipment_idx: usize,
    pub fips: i32,
}
```

### 5. `AllocationState` (from `nonrdalo.inc`)

**COMMON blocks**: `ALO` (allocation)

```rust
#[derive(Debug, Default, Clone)]
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

#[derive(Debug, Clone)]
pub struct CountyAllocation {
    pub fips: i32,
    pub factor: f64,
}

#[derive(Debug, Clone)]
pub struct StateCountyAllocation {
    pub state_code: i32,
    pub county_code: i32,
    pub factor: f64,
}

#[derive(Debug, Clone)]
pub struct SubcountyAllocation {
    pub fips: i32,
    pub subcounty_id: i32,
    pub factor: f64,
}
```

### 6. `GrowthState` (from `nonrdgrw.inc`)

**COMMON blocks**: `GRW` (growth), `GXRF` (growth adjustments)

```rust
#[derive(Debug, Default, Clone)]
pub struct GrowthState {
    /// Growth factor tables by year
    pub growth_table: Vec<GrowthRecord>,
    
    /// Growth adjustment factors
    pub adjustment_factors: Vec<GrowthAdjustment>,
    
    /// Year indices
    pub year_indices: Vec<i16>,
}

#[derive(Debug, Clone)]
pub struct GrowthRecord {
    pub scc: i32,
    pub equipment_idx: usize,
    pub fips: i32,
    pub year: i16,
    pub factor: f64,
}
```

### 7. `SeasonalState` (from `nonrdsea.inc`)

**COMMON blocks**: `SEA` (seasonal), `DAY` (day-specific)

```rust
#[derive(Debug, Default, Clone)]
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

#[derive(Debug, Clone)]
pub struct MonthlyFactor {
    pub month: u8,
    pub factor: f64,
}

#[derive(Debug, Clone)]
pub struct DayAdjustment {
    pub day_of_year: i32,
    pub adjustment: f64,
}
```

### 8. `TechnologyState` (from `nonrdtec.inc`)

**COMMON blocks**: `TECH` (technology), `EVTECH` (evaporative tech)

```rust
#[derive(Debug, Default, Clone)]
pub struct TechnologyState {
    /// Exhaust technology tables
    pub exhaust_technologies: Vec<TechnologyRecord>,
    
    /// Evaporative technology tables
    pub evaporative_technologies: Vec<EvapTechnologyRecord>,
    
    /// Technology mappings by SCC
    pub scc_technology_map: HashMap<i32, Vec<TechMapping>>,
}

#[derive(Debug, Clone)]
pub struct TechnologyRecord {
    pub scc: i32,
    pub tech_idx: usize,
    pub tech_type: TechType,
    pub control_efficiency: f64,
}

#[derive(Debug, Clone)]
pub struct EvapTechnologyRecord {
    pub scc: i32,
    pub evap_tech_idx: usize,
    pub evap_type: EvapTechType,
    pub control_efficiency: f64,
}
```

### 9. `EquipmentState` (from `nonrdegc.inc`)

**COMMON blocks**: `EQP` (equipment), `SCC` (source category codes)

```rust
#[derive(Debug, Default, Clone)]
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

#[derive(Debug, Clone)]
pub struct EquipmentCategory {
    pub idx: usize,
    pub name: String,
    pub base_year: i16,
}

#[derive(Debug, Clone)]
pub struct SccDefinition {
    pub scc: i32,
    pub description: String,
    pub category: SccCategory,
}
```

### 10. `GeographyState` (from `nonrdgeo.inc`)

**COMMON blocks**: `GEO` (geography), `FIPS` (FIPS codes)

```rust
#[derive(Debug, Default, Clone)]
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

#[derive(Debug, Clone)]
pub struct StateDefinition {
    pub code: i32,
    pub name: String,
    pub fips_start: i32,
    pub fips_end: i32,
}

#[derive(Debug, Clone)]
pub struct CountyDefinition {
    pub fips: i32,
    pub state_code: i32,
    pub county_code: i32,
    pub name: String,
    pub zone: i32,
}
```

### 11. `RetrofitState` (from `nonrdrtr.inc`)

**COMMON blocks**: `RTRFT` (retrofit)

```rust
#[derive(Debug, Default, Clone)]
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

#[derive(Debug, Clone)]
pub struct RetrofitRecord {
    pub scc: i32,
    pub equipment_idx: usize,
    pub fips: i32,
    pub retrofit_type: i32,
    pub year_installed: i16,
    pub effectiveness: f64,
}
```

## Top-Level `NonroadContext`

```rust
#[derive(Debug, Default, Clone)]
pub struct NonroadContext {
    pub options: UserOptions,
    pub emissions: EmissionsState,
    pub population: PopulationState,
    pub allocation: AllocationState,
    pub growth: GrowthState,
    pub seasonal: SeasonalState,
    pub technology: TechnologyState,
    pub equipment: EquipmentState,
    pub geography: GeographyState,
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
    pub warning_messages: Vec<WarningMessage>,
}
```

## Shared Variables Across Includes

Some COMMON blocks share variables. The following cross-references must be handled:

1. **Population ↔ Growth**: `PopulationState` loads base populations; `GrowthState` applies growth factors. Shared: FIPS codes, SCC, equipment indices.

2. **Equipment ↔ Technology**: `EquipmentState` defines equipment categories; `TechnologyState` references them. Shared: equipment indices.

3. **Geography ↔ All**: `GeographyState` provides FIPS lookups used by all other states.

4. **Emissions ↔ Technology**: Emission factors depend on technology selections.

## Module Structure

**Completed**: All types are defined in `src/common/mod.rs` as a single module
for now. Future refactoring may split into sub-modules per state type.

```
src/common/
├── mod.rs              # NonroadContext + all state types (Task 92 DONE)
└── consts.rs           # Parameters and constants (Task 93 pending)
```

## Implementation Status

✅ **Task 92 Complete**: COMMON block replacement design implemented

- All 10 state structs defined
- `NonroadContext` top-level container with all fields
- Supporting types (keys, records, enums) defined
- `#[non_exhaustive]` on public structs for future extension
- All types derive `Debug`, `Clone`, and `Default` where appropriate

⏳ **Task 93 Pending**: Parameter and constant translation

- Move `nonrdprm.inc` chemical constants to `consts.rs`
- Add units and provenance documentation

## Dependencies

- `thiserror` (workspace dep)
- `std::path::PathBuf` (standard library)
- `std::collections::HashMap` (standard library)

No new top-level dependencies required for this task.

## Notes

- All arrays use `Vec` or `HashMap` (dynamic sizing), not fixed-size arrays
- Original Fortran dimensions are in `consts` as documentation
- Error handling uses `Result<T, Error>` (see `error.rs`)
- All types derive `Debug` and `Clone` for testing
- `#[non_exhaustive]` on public structs to allow future field additions
- SeasonalState uses fixed arrays for day_fractions and month_days (365 and 12 elements respectively)

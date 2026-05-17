//! `TankFuelGenerator` — commingled tank-fuel RVP and ethanol volume
//! (migration-plan Task 39).
//!
//! Ports `gov/epa/otaq/moves/master/implementation/ghg/TankFuelGenerator.java`
//! (589 lines). The Java generator "builds commingled RVP information for the
//! fuels in vehicle tanks": for each `(county, calendar-year)` it derives, per
//! `(zone, fuel-type, fuel-year, month-group)`, the average ethanol volume and
//! Reid Vapor Pressure that downstream evaporative calculators read from the
//! `AverageTankGasoline` execution-database table.
//!
//! # Java structure
//!
//! `TankFuelGenerator` subscribes to the master loop at `YEAR` granularity,
//! `GENERATOR` priority, for the *Evap Fuel Vapor Venting* (process 12) and
//! *Evap Fuel Leaks* (process 13) processes. Its `executeLoop` delegates to
//! `calculateAverageTankGasoline`, a straight-line sequence of scripted SQL
//! statements with no branch points — the source comments label them
//! TFG-1a … TFG-3b. [`calculate_average_tank_gasoline`] ports that sequence:
//!
//! | Java step                                       | Rust |
//! |-------------------------------------------------|------|
//! | `regionCounty` region / fuel-year lookup        | `resolve_fuel_region` |
//! | TFG-1a `TFGUsedFuelFormulation`                 | `build_used_fuel_formulations` |
//! | TFG-1b/1c/1d/1e/3a `TFGFuelSupplyAverage`       | `build_fuel_supply_averages` |
//! | TFG-2a `TFGZone`                                | `build_zone_temperatures` |
//! | TFG-2b/2c/2d/3b `TFGZoneFuel` → output rows     | [`calculate_average_tank_gasoline`] |
//!
//! # SQL tables → typed inputs
//!
//! Each `inner join` in the Java SQL becomes an explicit Rust join over the
//! typed table-row slices on [`TankFuelInputs`]. The three RunSpec *filter*
//! tables (`RunSpecYear`, `RunSpecMonthGroup`, `RunSpecMonth`) — single-column
//! selection sets MOVES materialises in the execution database — become the
//! `runspec_*_ids` id-list fields.
//!
//! One subtlety is faithfully preserved: TFG-1a/1b/1c join
//! `FuelSupply ⋈ Year ⋈ RunSpecYear`, so each `FuelSupply` row is multiplied
//! by the number of run-selected calendar years that share the resolved fuel
//! year. The ratio aggregates (`linearAverageRVP`, `tankAverageETOHVolume`,
//! `averageGasPortionRVP`) are invariant under that uniform multiplication,
//! but `gasoholMarketShare = sum(marketShare)` scales with it — so the
//! multiplicity is folded into each row's market-share weight rather than
//! dropped.
//!
//! # Numeric precision
//!
//! All arithmetic runs in `f64`, matching MySQL, which evaluates every
//! expression in double precision. The MOVES default-DB and temp-table
//! columns are declared `float` (32-bit) / `double` (64-bit); the 32-bit
//! storage of intermediates is a data-plane concern (Phase 4 Parquet
//! conversion / Task 50) and is not emulated here. The resulting divergence
//! from canonical MOVES is bounded well within the Phase 3 tolerance budget
//! (`characterization/tolerance.toml`).
//!
//! # Data-plane deferral
//!
//! The framework data plane (`ExecutionTables` / `ScratchNamespace`, Task 50
//! `DataFrameStore`) is still a placeholder, so [`TankFuelGenerator`]'s
//! `Generator::execute` returns an empty output — the established Phase 2
//! pattern. The ported computation lives in the pure
//! [`calculate_average_tank_gasoline`] function and is fully exercised by the
//! crate tests; Task 50 wiring will read the input tables from the
//! [`CalculatorContext`], call it per `(county, year)`, and write
//! `AverageTankGasoline` into the scratch namespace.

use std::collections::{BTreeMap, HashMap, HashSet};

use moves_calculator_info::{Granularity, Priority};
use moves_data::ProcessId;
use moves_framework::{
    CalculatorContext, CalculatorOutput, CalculatorSubscription, Error, Generator,
};

/// `ethanolRVP` constant from `calculateAverageTankGasoline` (step 100).
const ETHANOL_RVP: f64 = 2.3;

/// `weatheringConstant` constant from `calculateAverageTankGasoline` (step 100).
const WEATHERING_CONSTANT: f64 = 0.049;

/// The `regionCodeID` selecting fuel regions in `regionCounty` — the Java
/// region lookup filters `where regionCodeID = 1`.
const FUEL_REGION_CODE_ID: i32 = 1;

// =============================================================================
//   Input table rows
// =============================================================================

/// One `FuelSupply` row: the market share of a fuel formulation within a
/// `(fuel region, fuel year, month group)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelSupplyRow {
    /// `fuelRegionID`.
    pub fuel_region_id: i32,
    /// `fuelYearID`.
    pub fuel_year_id: i32,
    /// `monthGroupID`.
    pub month_group_id: i32,
    /// `fuelFormulationID`.
    pub fuel_formulation_id: i32,
    /// `marketShare` — fraction of the fuel supply; rows with a non-positive
    /// (or `NaN`) share are excluded, matching the SQL `marketShare > 0`.
    pub market_share: f64,
}

/// One `FuelFormulation` row — only the columns `TankFuelGenerator` reads.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelFormulationRow {
    /// `fuelFormulationID`.
    pub fuel_formulation_id: i32,
    /// `fuelSubtypeID`.
    pub fuel_subtype_id: i32,
    /// `RVP` — Reid Vapor Pressure of the formulation.
    pub rvp: f64,
    /// `ETOHVolume` — ethanol volume percentage.
    pub etoh_volume: f64,
}

/// One `FuelSubtype` row — maps a fuel subtype to its fuel type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuelSubtypeRow {
    /// `fuelSubtypeID`.
    pub fuel_subtype_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
}

/// One `FuelType` row — carries the `subjectToEvapCalculations` flag the
/// TFG-1a join filters on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuelTypeRow {
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `subjectToEvapCalculations = 'Y'`.
    pub subject_to_evap_calculations: bool,
}

/// One `Year` row — maps a calendar year to its fuel year.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct YearRow {
    /// `yearID` — calendar year.
    pub year_id: i32,
    /// `fuelYearID`.
    pub fuel_year_id: i32,
}

/// One `MonthofAnyYear` row — maps a month to its month group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MonthOfAnyYearRow {
    /// `monthID`.
    pub month_id: i32,
    /// `monthGroupID`.
    pub month_group_id: i32,
}

/// One `Zone` row — maps a zone to its county.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ZoneRow {
    /// `zoneID`.
    pub zone_id: i32,
    /// `countyID`.
    pub county_id: i32,
}

/// One `ZoneMonthHour` row — only the columns `TankFuelGenerator` reads.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ZoneMonthHourRow {
    /// `zoneID`.
    pub zone_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `temperature` — hourly zone temperature (°F).
    pub temperature: f64,
}

/// One `regionCounty` row — associates a county with a fuel region / fuel year.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegionCountyRow {
    /// `regionID`.
    pub region_id: i32,
    /// `countyID`.
    pub county_id: i32,
    /// `regionCodeID`.
    pub region_code_id: i32,
    /// `fuelYearID`.
    pub fuel_year_id: i32,
}

// =============================================================================
//   Output
// =============================================================================

/// One `AverageTankGasoline` row — the generator's output, keyed by
/// `(zone, fuel type, fuel year, month group)`.
///
/// The same type carries pre-existing **user input** rows on
/// [`TankFuelInputs::prior_average_tank_gasoline`]: the Java `insert ignore`
/// leaves a user-supplied row untouched, so a key already present blocks the
/// generated row for that key.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AverageTankGasolineRow {
    /// `zoneID`.
    pub zone_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `fuelYearID`.
    pub fuel_year_id: i32,
    /// `monthGroupID`.
    pub month_group_id: i32,
    /// `ETOHVolume` — market-share-weighted average tank ethanol volume.
    pub etoh_volume: f64,
    /// `RVP` — commingled, weathered tank Reid Vapor Pressure.
    pub rvp: f64,
    /// `isUserInput` — `false` (`'N'`) for generated rows, `true` (`'Y'`)
    /// for user-supplied rows.
    pub is_user_input: bool,
}

/// The slice of the MOVES execution database `TankFuelGenerator` reads.
///
/// Holds whole tables; [`calculate_average_tank_gasoline`] filters to one
/// `(county, year)` internally, so the same value is reused across every
/// `(county, year)` invocation of a run.
#[derive(Debug, Clone, Default)]
pub struct TankFuelInputs {
    /// `FuelSupply`.
    pub fuel_supply: Vec<FuelSupplyRow>,
    /// `FuelFormulation`.
    pub fuel_formulation: Vec<FuelFormulationRow>,
    /// `FuelSubtype`.
    pub fuel_subtype: Vec<FuelSubtypeRow>,
    /// `FuelType`.
    pub fuel_type: Vec<FuelTypeRow>,
    /// `Year`.
    pub year: Vec<YearRow>,
    /// `MonthofAnyYear`.
    pub month_of_any_year: Vec<MonthOfAnyYearRow>,
    /// `Zone`.
    pub zone: Vec<ZoneRow>,
    /// `ZoneMonthHour`.
    pub zone_month_hour: Vec<ZoneMonthHourRow>,
    /// `regionCounty`.
    pub region_county: Vec<RegionCountyRow>,
    /// `RunSpecYear` — calendar years selected by the RunSpec.
    pub runspec_year_ids: Vec<i32>,
    /// `RunSpecMonthGroup` — month groups selected by the RunSpec.
    pub runspec_month_group_ids: Vec<i32>,
    /// `RunSpecMonth` — months selected by the RunSpec.
    pub runspec_month_ids: Vec<i32>,
    /// Pre-existing `AverageTankGasoline` rows (typically user input). A key
    /// present here blocks the generated row for that key — the Java
    /// `insert ignore` semantics.
    pub prior_average_tank_gasoline: Vec<AverageTankGasolineRow>,
}

// =============================================================================
//   Scalar formulas (step 100)
// =============================================================================

/// `kGasoline` — gasoline-portion volume-correction factor (TFG-1a, reused
/// for `averageKGasoline` in TFG-1d).
///
/// `((-7e-7)*pow(e,3)) + (0.0002*pow(e,2)) + (0.0024*e) + 1.0`.
fn k_gasoline(etoh_volume: f64) -> f64 {
    let e = etoh_volume;
    -7e-7 * e.powi(3) + 0.0002 * e.powi(2) + 0.0024 * e + 1.0
}

/// `kEthanol` — ethanol-portion volume-correction factor (TFG-1a, reused for
/// `averageKEthanol` in TFG-1d).
///
/// `case when e > 0 then 46.321*pow(e,-0.8422) else 1000.0 end`.
fn k_ethanol(etoh_volume: f64) -> f64 {
    if etoh_volume > 0.0 {
        46.321 * etoh_volume.powf(-0.8422)
    } else {
        1000.0
    }
}

/// `gasPortionRVP` — the RVP attributable to the gasoline portion of a fuel
/// formulation (TFG-1a).
///
/// `(RVP - kEthanol*e/100*ethanolRVP) / (kGasoline*(100-e)/100)`.
fn gas_portion_rvp(rvp: f64, etoh_volume: f64) -> f64 {
    let e = etoh_volume;
    let numerator = rvp - k_ethanol(e) * e / 100.0 * ETHANOL_RVP;
    let denominator = k_gasoline(e) * (100.0 - e) / 100.0;
    numerator / denominator
}

/// "Reddy RVP" — recombines the gasoline and ethanol RVP portions into a
/// commingled tank RVP. TFG-1e (`noWeatheringReddyRVP`, fed the unweathered
/// `averageGasPortionRVP`) and TFG-2d (`weatheredReddyRVP`, fed
/// `weatheredGasPortionRVP`) share this formula:
///
/// `kGasoline*(100-e)/100*gasPortionRVP + kEthanol*e/100*ethanolRVP`.
fn reddy_rvp(k_gasoline: f64, k_ethanol: f64, etoh_volume: f64, gas_portion_rvp: f64) -> f64 {
    let e = etoh_volume;
    k_gasoline * (100.0 - e) / 100.0 * gas_portion_rvp + k_ethanol * e / 100.0 * ETHANOL_RVP
}

/// `zoneEvapTemp` — the representative evaporative temperature of a zone /
/// month group from its temperature extremes (TFG-2a).
///
/// For a narrow or cool zone (`zoneMax < 40` or `zoneMax <= zoneMin`) it is
/// the midpoint; otherwise it is the MOVES regression of the extremes.
fn zone_evap_temp(zone_min: f64, zone_max: f64) -> f64 {
    if zone_max < 40.0 || zone_max - zone_min <= 0.0 {
        (zone_min + zone_max) / 2.0
    } else {
        -1.7474 + 1.029 * zone_min + 0.99202 * (zone_max - zone_min)
            - 0.0025173 * zone_min * (zone_max - zone_min)
    }
}

/// `ratioGasolineRVPLoss` — the fraction of gasoline-portion RVP weathered
/// away at a zone's evaporative temperature, clamped at zero (TFG-2b).
///
/// `greatest(0, (-2.4908 + 0.026196*t + 0.00076898*t*g) / (-0.0860 + 0.070592*g))`
/// where `t` is `zoneEvapTemp` and `g` is `averageGasPortionRVP`.
fn ratio_gasoline_rvp_loss(zone_evap_temp: f64, avg_gas_portion_rvp: f64) -> f64 {
    let t = zone_evap_temp;
    let g = avg_gas_portion_rvp;
    let numerator = -2.4908 + 0.026196 * t + 0.00076898 * t * g;
    let denominator = -0.0860 + 0.070592 * g;
    (numerator / denominator).max(0.0)
}

/// `commingledRVP` multiplier — the gasohol-commingling correction applied to
/// `linearAverageRVP` (TFG-3a), a step function of the gasohol market share.
///
/// A `None` share (no gasohol formulation contributed, so TFG-1c inserts no
/// `TFGTemp` row and `gasoholMarketShare` stays SQL `NULL`) takes the
/// `else 1.000` branch, as does any share below `0.1`.
fn commingled_factor(gasohol_market_share: Option<f64>) -> f64 {
    match gasohol_market_share {
        Some(s) if s >= 1.0 => 1.000,
        Some(s) if s >= 0.9 => 1.018,
        Some(s) if s >= 0.8 => 1.027,
        Some(s) if s >= 0.7 => 1.034,
        Some(s) if s >= 0.6 => 1.038,
        Some(s) if s >= 0.5 => 1.040,
        Some(s) if s >= 0.4 => 1.039,
        Some(s) if s >= 0.3 => 1.035,
        Some(s) if s >= 0.2 => 1.028,
        Some(s) if s >= 0.1 => 1.016,
        _ => 1.000,
    }
}

// =============================================================================
//   Intermediate (temp) tables
// =============================================================================

/// One `TFGUsedFuelFormulation` row, keyed in a map by `fuelFormulationID`.
/// Only the two columns downstream steps consume are retained — the per-row
/// `kGasoline` / `kEthanol` are folded into [`gas_portion_rvp`].
struct UsedFuelFormulation {
    fuel_type_id: i32,
    gas_portion_rvp: f64,
}

/// One `TFGFuelSupplyAverage` row — per `(fuel type, fuel year, month group)`
/// averages plus the TFG-1d/1e/3a derived columns.
struct FuelSupplyAverage {
    fuel_type_id: i32,
    fuel_year_id: i32,
    month_group_id: i32,
    tank_average_etoh_volume: f64,
    average_gas_portion_rvp: f64,
    average_k_gasoline: f64,
    average_k_ethanol: f64,
    no_weathering_reddy_rvp: f64,
    commingled_rvp: f64,
}

/// One `TFGZone` row — the representative evaporative temperature of a
/// `(zone, month group)`.
struct ZoneTemperature {
    zone_id: i32,
    month_group_id: i32,
    zone_evap_temp: f64,
}

// =============================================================================
//   Steps
// =============================================================================

/// Resolve the fuel region and fuel year for a `(county, calendar-year)`.
///
/// Ports the opening query of `calculateAverageTankGasoline`:
/// `regionCounty ⋈ Year` filtered to `regionCodeID = 1`,
/// `Year.yearID = calendar_year`, `countyID = county_id`. The Java reads the
/// first result row; this returns the first match in input order.
fn resolve_fuel_region(
    inputs: &TankFuelInputs,
    county_id: i32,
    calendar_year: i32,
) -> Option<(i32, i32)> {
    inputs
        .region_county
        .iter()
        .filter(|rc| rc.region_code_id == FUEL_REGION_CODE_ID && rc.county_id == county_id)
        .find(|rc| {
            inputs
                .year
                .iter()
                .any(|y| y.fuel_year_id == rc.fuel_year_id && y.year_id == calendar_year)
        })
        .map(|rc| (rc.region_id, rc.fuel_year_id))
}

/// TFG-1a — the fuel formulations the run's fuel supply actually uses, with
/// each one's gasoline-portion RVP.
///
/// Ports `select distinct ...` into `TFGUsedFuelFormulation`: every
/// `FuelSupply` row matching the resolved region / fuel year, with a positive
/// market share and a run-selected month group, joined through
/// `FuelFormulation → FuelSubtype → FuelType` and kept only where the fuel
/// type is `subjectToEvapCalculations`. The `Year ⋈ RunSpecYear` join is
/// already implied — the caller returns early unless its multiplicity is
/// non-zero.
fn build_used_fuel_formulations(
    inputs: &TankFuelInputs,
    fuel_region_id: i32,
    fuel_year_id: i32,
    runspec_month_groups: &HashSet<i32>,
) -> HashMap<i32, UsedFuelFormulation> {
    let formulation_by_id: HashMap<i32, &FuelFormulationRow> = inputs
        .fuel_formulation
        .iter()
        .map(|f| (f.fuel_formulation_id, f))
        .collect();
    let subtype_to_type: HashMap<i32, i32> = inputs
        .fuel_subtype
        .iter()
        .map(|s| (s.fuel_subtype_id, s.fuel_type_id))
        .collect();
    let evap_fuel_types: HashSet<i32> = inputs
        .fuel_type
        .iter()
        .filter(|t| t.subject_to_evap_calculations)
        .map(|t| t.fuel_type_id)
        .collect();

    let mut used: HashMap<i32, UsedFuelFormulation> = HashMap::new();
    for fs in &inputs.fuel_supply {
        let qualifies = fs.fuel_region_id == fuel_region_id
            && fs.fuel_year_id == fuel_year_id
            && fs.market_share > 0.0
            && runspec_month_groups.contains(&fs.month_group_id);
        if !qualifies || used.contains_key(&fs.fuel_formulation_id) {
            continue;
        }
        let Some(ff) = formulation_by_id.get(&fs.fuel_formulation_id) else {
            continue;
        };
        let Some(&fuel_type_id) = subtype_to_type.get(&ff.fuel_subtype_id) else {
            continue;
        };
        if !evap_fuel_types.contains(&fuel_type_id) {
            continue;
        }
        used.insert(
            fs.fuel_formulation_id,
            UsedFuelFormulation {
                fuel_type_id,
                gas_portion_rvp: gas_portion_rvp(ff.rvp, ff.etoh_volume),
            },
        );
    }
    used
}

/// TFG-1b/1c/1d/1e/3a — the per-`(fuel type, fuel year, month group)`
/// averages.
///
/// Aggregates the same filtered `FuelSupply` join as TFG-1a (restricted to
/// the used formulations) into market-share-weighted averages, then layers on
/// the TFG-1d K-factors, the TFG-1e `noWeatheringReddyRVP`, and the TFG-3a
/// `commingledRVP`. `weight_factor` carries the `Year ⋈ RunSpecYear`
/// multiplicity (see the module docs).
fn build_fuel_supply_averages(
    inputs: &TankFuelInputs,
    fuel_region_id: i32,
    fuel_year_id: i32,
    weight_factor: f64,
    runspec_month_groups: &HashSet<i32>,
    used: &HashMap<i32, UsedFuelFormulation>,
) -> Vec<FuelSupplyAverage> {
    let formulation_by_id: HashMap<i32, &FuelFormulationRow> = inputs
        .fuel_formulation
        .iter()
        .map(|f| (f.fuel_formulation_id, f))
        .collect();

    /// Per-group running sums; the gasohol share stays `None` until a
    /// gasohol-range formulation contributes, mirroring TFG-1c's `NULL`.
    #[derive(Default)]
    struct Accum {
        sum_rvp_ms: f64,
        sum_etoh_ms: f64,
        sum_gas_portion_rvp_ms: f64,
        sum_ms: f64,
        gasohol_market_share: Option<f64>,
    }

    let mut groups: BTreeMap<(i32, i32, i32), Accum> = BTreeMap::new();
    for fs in &inputs.fuel_supply {
        let qualifies = fs.fuel_region_id == fuel_region_id
            && fs.fuel_year_id == fuel_year_id
            && fs.market_share > 0.0
            && runspec_month_groups.contains(&fs.month_group_id);
        if !qualifies {
            continue;
        }
        let Some(uff) = used.get(&fs.fuel_formulation_id) else {
            continue;
        };
        let Some(ff) = formulation_by_id.get(&fs.fuel_formulation_id) else {
            continue;
        };
        let weight = fs.market_share * weight_factor;
        let acc = groups
            .entry((uff.fuel_type_id, fs.fuel_year_id, fs.month_group_id))
            .or_default();
        acc.sum_rvp_ms += ff.rvp * weight;
        acc.sum_etoh_ms += ff.etoh_volume * weight;
        acc.sum_gas_portion_rvp_ms += uff.gas_portion_rvp * weight;
        acc.sum_ms += weight;
        // TFG-1c: gasohol is a formulation with 4 <= ETOHVolume <= 20.
        if (4.0..=20.0).contains(&ff.etoh_volume) {
            *acc.gasohol_market_share.get_or_insert(0.0) += weight;
        }
    }

    groups
        .into_iter()
        .map(|((fuel_type_id, fuel_year_id, month_group_id), acc)| {
            // TFG-1b: market-share-weighted averages.
            let linear_average_rvp = acc.sum_rvp_ms / acc.sum_ms;
            let tank_average_etoh_volume = acc.sum_etoh_ms / acc.sum_ms;
            let average_gas_portion_rvp = acc.sum_gas_portion_rvp_ms / acc.sum_ms;
            // TFG-1d: K-factors at the average tank ethanol volume.
            let average_k_gasoline = k_gasoline(tank_average_etoh_volume);
            let average_k_ethanol = k_ethanol(tank_average_etoh_volume);
            // TFG-1e: unweathered commingled RVP.
            let no_weathering_reddy_rvp = reddy_rvp(
                average_k_gasoline,
                average_k_ethanol,
                tank_average_etoh_volume,
                average_gas_portion_rvp,
            );
            // TFG-3a: gasohol-commingling correction.
            let commingled_rvp = linear_average_rvp * commingled_factor(acc.gasohol_market_share);
            FuelSupplyAverage {
                fuel_type_id,
                fuel_year_id,
                month_group_id,
                tank_average_etoh_volume,
                average_gas_portion_rvp,
                average_k_gasoline,
                average_k_ethanol,
                no_weathering_reddy_rvp,
                commingled_rvp,
            }
        })
        .collect()
}

/// TFG-2a — the representative evaporative temperature of each
/// `(zone, month group)` in the county.
///
/// Ports `TFGZone`: the min/max `ZoneMonthHour.temperature` over the county's
/// zones and the run-selected months, reduced to `zoneEvapTemp`.
fn build_zone_temperatures(inputs: &TankFuelInputs, county_id: i32) -> Vec<ZoneTemperature> {
    let zones_in_county: HashSet<i32> = inputs
        .zone
        .iter()
        .filter(|z| z.county_id == county_id)
        .map(|z| z.zone_id)
        .collect();
    let month_to_group: HashMap<i32, i32> = inputs
        .month_of_any_year
        .iter()
        .map(|m| (m.month_id, m.month_group_id))
        .collect();
    let runspec_months: HashSet<i32> = inputs.runspec_month_ids.iter().copied().collect();

    // (zoneID, monthGroupID) -> (min temperature, max temperature)
    let mut extremes: BTreeMap<(i32, i32), (f64, f64)> = BTreeMap::new();
    for zmh in &inputs.zone_month_hour {
        if !zones_in_county.contains(&zmh.zone_id) || !runspec_months.contains(&zmh.month_id) {
            continue;
        }
        let Some(&month_group_id) = month_to_group.get(&zmh.month_id) else {
            continue;
        };
        extremes
            .entry((zmh.zone_id, month_group_id))
            .and_modify(|(lo, hi)| {
                *lo = lo.min(zmh.temperature);
                *hi = hi.max(zmh.temperature);
            })
            .or_insert((zmh.temperature, zmh.temperature));
    }

    extremes
        .into_iter()
        .map(
            |((zone_id, month_group_id), (zone_min, zone_max))| ZoneTemperature {
                zone_id,
                month_group_id,
                zone_evap_temp: zone_evap_temp(zone_min, zone_max),
            },
        )
        .collect()
}

/// Compute the `AverageTankGasoline` rows `TankFuelGenerator` produces for one
/// `(county, calendar-year)`.
///
/// Ports `TankFuelGenerator.calculateAverageTankGasoline` end to end. Rows
/// whose `(zone, fuel type, fuel year, month group)` key is already present in
/// [`TankFuelInputs::prior_average_tank_gasoline`] are skipped — the Java
/// `insert ignore` keeps user-supplied rows. The result is sorted by that key
/// for deterministic output.
///
/// Returns an empty vector when no fuel region / fuel year resolves for the
/// `(county, year)`, when no run-selected calendar year shares the resolved
/// fuel year, or when no fuel / zone data survives the joins.
#[must_use]
pub fn calculate_average_tank_gasoline(
    inputs: &TankFuelInputs,
    county_id: i32,
    calendar_year: i32,
) -> Vec<AverageTankGasolineRow> {
    let Some((fuel_region_id, fuel_year_id)) =
        resolve_fuel_region(inputs, county_id, calendar_year)
    else {
        return Vec::new();
    };

    // The TFG-1 joins pass FuelSupply through `Year ⋈ RunSpecYear`, which
    // multiplies each row by the count of run-selected calendar years sharing
    // the resolved fuel year. Folded into the market-share weight below.
    let runspec_years: HashSet<i32> = inputs.runspec_year_ids.iter().copied().collect();
    let year_multiplicity = inputs
        .year
        .iter()
        .filter(|y| y.fuel_year_id == fuel_year_id && runspec_years.contains(&y.year_id))
        .count();
    if year_multiplicity == 0 {
        return Vec::new();
    }
    let weight_factor = year_multiplicity as f64;

    let runspec_month_groups: HashSet<i32> =
        inputs.runspec_month_group_ids.iter().copied().collect();

    let used =
        build_used_fuel_formulations(inputs, fuel_region_id, fuel_year_id, &runspec_month_groups);
    let averages = build_fuel_supply_averages(
        inputs,
        fuel_region_id,
        fuel_year_id,
        weight_factor,
        &runspec_month_groups,
        &used,
    );
    let zone_temperatures = build_zone_temperatures(inputs, county_id);

    // The TFG-2b join is `TFGZone ⋈ TFGFuelSupplyAverage using (monthGroupID)`.
    let mut averages_by_month_group: HashMap<i32, Vec<&FuelSupplyAverage>> = HashMap::new();
    for average in &averages {
        averages_by_month_group
            .entry(average.month_group_id)
            .or_default()
            .push(average);
    }

    let prior_keys: HashSet<(i32, i32, i32, i32)> = inputs
        .prior_average_tank_gasoline
        .iter()
        .map(|r| (r.zone_id, r.fuel_type_id, r.fuel_year_id, r.month_group_id))
        .collect();

    let mut output: Vec<AverageTankGasolineRow> = Vec::new();
    for zone in &zone_temperatures {
        let Some(averages) = averages_by_month_group.get(&zone.month_group_id) else {
            continue;
        };
        for &fsa in averages {
            // TFG-2b: weathering ratio at this zone's evaporative temperature.
            let ratio = ratio_gasoline_rvp_loss(zone.zone_evap_temp, fsa.average_gas_portion_rvp);
            // TFG-2c: weathered gasoline-portion RVP.
            let weathered_gas_portion_rvp =
                fsa.average_gas_portion_rvp * (1.0 - ratio * WEATHERING_CONSTANT);
            // TFG-2d: weathered commingled RVP.
            let weathered_reddy_rvp = reddy_rvp(
                fsa.average_k_gasoline,
                fsa.average_k_ethanol,
                fsa.tank_average_etoh_volume,
                weathered_gas_portion_rvp,
            );
            // TFG-3b: final RVP = weathered RVP scaled by the commingling
            // correction, renormalised by the unweathered commingled RVP.
            let rvp = weathered_reddy_rvp * fsa.commingled_rvp / fsa.no_weathering_reddy_rvp;

            let key = (
                zone.zone_id,
                fsa.fuel_type_id,
                fsa.fuel_year_id,
                zone.month_group_id,
            );
            if prior_keys.contains(&key) {
                continue;
            }
            output.push(AverageTankGasolineRow {
                zone_id: zone.zone_id,
                fuel_type_id: fsa.fuel_type_id,
                fuel_year_id: fsa.fuel_year_id,
                month_group_id: zone.month_group_id,
                etoh_volume: fsa.tank_average_etoh_volume,
                rvp,
                is_user_input: false,
            });
        }
    }

    output.sort_by_key(|r| (r.zone_id, r.fuel_type_id, r.fuel_year_id, r.month_group_id));
    output
}

// =============================================================================
//   Generator
// =============================================================================

/// Default-DB tables [`TankFuelGenerator`] reads, in canonical MOVES casing.
static INPUT_TABLES: &[&str] = &[
    "FuelSupply",
    "FuelFormulation",
    "FuelSubtype",
    "FuelType",
    "Year",
    "MonthofAnyYear",
    "Zone",
    "ZoneMonthHour",
    "regionCounty",
];

/// Scratch table [`TankFuelGenerator`] writes.
static OUTPUT_TABLES: &[&str] = &["AverageTankGasoline"];

/// The Task 39 generator — the framework adapter around
/// [`calculate_average_tank_gasoline`].
///
/// Ports the master-loop surface of `TankFuelGenerator.java`: it subscribes
/// for *Evap Fuel Vapor Venting* and *Evap Fuel Leaks* at `YEAR` granularity,
/// `GENERATOR` priority, and declares the `AverageTankGasoline` scratch table
/// it produces. `Generator::execute` is an empty stand-in until the Task 50
/// data plane lands — see the module docs.
#[derive(Debug, Clone)]
pub struct TankFuelGenerator {
    subscriptions: Vec<CalculatorSubscription>,
}

impl TankFuelGenerator {
    /// Construct the generator with its two master-loop subscriptions.
    #[must_use]
    pub fn new() -> Self {
        // `MasterLoopPriority.GENERATOR` — see `TankFuelGenerator.subscribeToMe`.
        let priority =
            Priority::parse("GENERATOR").expect("\"GENERATOR\" is a canonical MasterLoopPriority");
        Self {
            subscriptions: vec![
                // Evap Fuel Vapor Venting (process 12).
                CalculatorSubscription::new(ProcessId(12), Granularity::Year, priority),
                // Evap Fuel Leaks (process 13).
                CalculatorSubscription::new(ProcessId(13), Granularity::Year, priority),
            ],
        }
    }
}

impl Default for TankFuelGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl Generator for TankFuelGenerator {
    fn name(&self) -> &'static str {
        "TankFuelGenerator"
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        &self.subscriptions
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn output_tables(&self) -> &[&'static str] {
        OUTPUT_TABLES
    }

    fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        // The data plane (Task 50 `DataFrameStore`) is not yet materialised,
        // so `ctx.tables()` / `ctx.scratch()` are placeholders. The ported
        // computation lives in `calculate_average_tank_gasoline`; once Task 50
        // lands, this body will read the input tables from `ctx`, call it per
        // (county, year), and write `AverageTankGasoline` into `ctx.scratch()`.
        Ok(CalculatorOutput::empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Assert two `f64`s agree within a tolerance comfortably tighter than any
    /// platform `libm` `pow` discrepancy yet far looser than a real algorithm
    /// bug. Reference values are computed independently (see the test docs).
    fn assert_close(got: f64, expected: f64, what: &str) {
        let diff = (got - expected).abs();
        assert!(
            diff < 1e-9,
            "{what}: got {got}, expected {expected}, diff {diff}"
        );
    }

    // --- scalar formulas ---------------------------------------------------

    #[test]
    fn k_gasoline_matches_reference() {
        // Pure mul/add — fully deterministic IEEE-754 arithmetic.
        // k_gasoline(0) = 1.0; k_gasoline(10) = -7e-4 + 0.02 + 0.024 + 1.0.
        assert_close(k_gasoline(0.0), 1.0, "k_gasoline(0)");
        assert_close(k_gasoline(10.0), 1.0433, "k_gasoline(10)");
    }

    #[test]
    fn k_ethanol_branches() {
        // etoh <= 0 takes the constant branch; > 0 the power-law branch.
        assert_close(k_ethanol(0.0), 1000.0, "k_ethanol(0)");
        assert_close(k_ethanol(-1.0), 1000.0, "k_ethanol(-1)");
        // Reference: 46.321 * 10^-0.8422, computed with Python `math`.
        assert_close(k_ethanol(10.0), 6.661_590_412_093_28, "k_ethanol(10)");
    }

    #[test]
    fn gas_portion_rvp_and_reddy_rvp_are_inverse() {
        // `reddy_rvp` recombines what `gas_portion_rvp` splits out, so feeding
        // an unweathered gas-portion RVP back through `reddy_rvp` reconstructs
        // the original formulation RVP.
        for &(rvp, etoh) in &[(12.0, 10.0), (9.0, 0.0), (13.5, 15.0), (7.8, 5.5)] {
            let gpr = gas_portion_rvp(rvp, etoh);
            let recombined = reddy_rvp(k_gasoline(etoh), k_ethanol(etoh), etoh, gpr);
            assert_close(recombined, rvp, "reddy_rvp ∘ gas_portion_rvp");
        }
        // A zero-ethanol formulation's gas portion is just its RVP.
        assert_close(gas_portion_rvp(9.0, 0.0), 9.0, "gas_portion_rvp(9, 0)");
    }

    #[test]
    fn zone_evap_temp_branches() {
        // Cool zone (zoneMax < 40): midpoint.
        assert_close(zone_evap_temp(30.0, 35.0), 32.5, "midpoint, cool");
        // Degenerate range (zoneMax <= zoneMin): midpoint.
        assert_close(zone_evap_temp(50.0, 50.0), 50.0, "midpoint, flat");
        // Warm, wide zone: the regression branch.
        // -1.7474 + 1.029*45 + 0.99202*50 - 0.0025173*45*50.
        assert_close(zone_evap_temp(45.0, 95.0), 88.494_675, "regression");
    }

    #[test]
    fn ratio_gasoline_rvp_loss_clamps_at_zero() {
        // A low evaporative temperature drives the numerator negative — the
        // `greatest(0, ...)` clamp pins the ratio at zero.
        assert_close(
            ratio_gasoline_rvp_loss(32.5, 9.969_426_630_383_43),
            0.0,
            "clamped ratio",
        );
        // A warm zone yields a positive, sub-unity ratio. Reference:
        // (-2.4908 + 0.026196*80 + 0.00076898*80*10) / (-0.0860 + 0.070592*10).
        assert_close(
            ratio_gasoline_rvp_loss(80.0, 10.0),
            0.354_987_740_353_594_04,
            "positive ratio",
        );
    }

    #[test]
    fn commingled_factor_bands() {
        // Every band boundary, plus the NULL / below-0.1 fallthrough.
        assert_close(commingled_factor(Some(1.0)), 1.000, "share >= 1.0");
        assert_close(commingled_factor(Some(0.95)), 1.018, "share >= 0.9");
        assert_close(commingled_factor(Some(0.6)), 1.038, "share >= 0.6");
        assert_close(commingled_factor(Some(0.5)), 1.040, "share >= 0.5");
        assert_close(commingled_factor(Some(0.1)), 1.016, "share >= 0.1");
        assert_close(commingled_factor(Some(0.05)), 1.000, "share < 0.1");
        assert_close(commingled_factor(None), 1.000, "no gasohol (NULL)");
    }

    // --- fixtures ----------------------------------------------------------

    /// Fixture A: one gasoline formulation (`ETOHVolume = 10`, `RVP = 12`,
    /// `marketShare = 1`) in one zone of one county, one month group, one
    /// run-selected year. The warm/wide zone (`45 °F`–`95 °F`) exercises the
    /// `zoneEvapTemp` regression branch and a non-zero weathering ratio.
    fn fixture_single_formulation() -> TankFuelInputs {
        TankFuelInputs {
            fuel_supply: vec![FuelSupplyRow {
                fuel_region_id: 50,
                fuel_year_id: 2020,
                month_group_id: 1,
                fuel_formulation_id: 100,
                market_share: 1.0,
            }],
            fuel_formulation: vec![FuelFormulationRow {
                fuel_formulation_id: 100,
                fuel_subtype_id: 10,
                rvp: 12.0,
                etoh_volume: 10.0,
            }],
            fuel_subtype: vec![FuelSubtypeRow {
                fuel_subtype_id: 10,
                fuel_type_id: 1,
            }],
            fuel_type: vec![FuelTypeRow {
                fuel_type_id: 1,
                subject_to_evap_calculations: true,
            }],
            year: vec![YearRow {
                year_id: 2020,
                fuel_year_id: 2020,
            }],
            month_of_any_year: vec![MonthOfAnyYearRow {
                month_id: 1,
                month_group_id: 1,
            }],
            zone: vec![ZoneRow {
                zone_id: 90_000,
                county_id: 1000,
            }],
            zone_month_hour: vec![
                ZoneMonthHourRow {
                    zone_id: 90_000,
                    month_id: 1,
                    temperature: 45.0,
                },
                ZoneMonthHourRow {
                    zone_id: 90_000,
                    month_id: 1,
                    temperature: 95.0,
                },
            ],
            region_county: vec![RegionCountyRow {
                region_id: 50,
                county_id: 1000,
                region_code_id: 1,
                fuel_year_id: 2020,
            }],
            runspec_year_ids: vec![2020],
            runspec_month_group_ids: vec![1],
            runspec_month_ids: vec![1],
            prior_average_tank_gasoline: Vec::new(),
        }
    }

    /// Fixture B: two formulations sharing a fuel type / month group — a
    /// gasohol (`ETOHVolume = 10`, `share 0.6`) and an `E0` (`ETOHVolume = 0`,
    /// `share 0.4`). The cool zone (`30 °F`–`35 °F`) exercises the
    /// `zoneEvapTemp` midpoint branch and the zero-weathering clamp.
    fn fixture_two_formulations() -> TankFuelInputs {
        let mut inputs = fixture_single_formulation();
        inputs.fuel_supply = vec![
            FuelSupplyRow {
                fuel_region_id: 50,
                fuel_year_id: 2020,
                month_group_id: 1,
                fuel_formulation_id: 100,
                market_share: 0.6,
            },
            FuelSupplyRow {
                fuel_region_id: 50,
                fuel_year_id: 2020,
                month_group_id: 1,
                fuel_formulation_id: 101,
                market_share: 0.4,
            },
        ];
        inputs.fuel_formulation = vec![
            FuelFormulationRow {
                fuel_formulation_id: 100,
                fuel_subtype_id: 10,
                rvp: 11.5,
                etoh_volume: 10.0,
            },
            FuelFormulationRow {
                fuel_formulation_id: 101,
                fuel_subtype_id: 10,
                rvp: 9.0,
                etoh_volume: 0.0,
            },
        ];
        inputs.zone_month_hour = vec![
            ZoneMonthHourRow {
                zone_id: 90_000,
                month_id: 1,
                temperature: 30.0,
            },
            ZoneMonthHourRow {
                zone_id: 90_000,
                month_id: 1,
                temperature: 35.0,
            },
        ];
        inputs
    }

    // --- end-to-end --------------------------------------------------------

    #[test]
    fn single_formulation_full_chain() {
        // Reference values traced through every TFG step in Python:
        //   etohVolume = tankAverageETOHVolume = 10.0
        //   rvp        = weatheredReddyRVP (noWeatheringReddyRVP == commingledRVP == 12.0)
        let rows = calculate_average_tank_gasoline(&fixture_single_formulation(), 1000, 2020);
        assert_eq!(rows.len(), 1, "one zone × one fuel-supply group");
        let row = rows[0];
        assert_eq!(row.zone_id, 90_000);
        assert_eq!(row.fuel_type_id, 1);
        assert_eq!(row.fuel_year_id, 2020);
        assert_eq!(row.month_group_id, 1);
        assert!(!row.is_user_input);
        assert_close(row.etoh_volume, 10.0, "etoh_volume");
        assert_close(row.rvp, 11.571_170_292_022_503, "rvp");
    }

    #[test]
    fn two_formulations_full_chain() {
        // Reference values from the Python trace:
        //   tankAverageETOHVolume = (10*0.6 + 0*0.4) / 1.0      = 6.0
        //   gasoholMarketShare    = 0.6  -> commingled factor 1.038
        //   zoneEvapTemp          = (30+35)/2 = 32.5 -> ratio clamped to 0
        //   rvp = commingledRVP = linearAverageRVP(10.5) * 1.038 = 10.899
        let rows = calculate_average_tank_gasoline(&fixture_two_formulations(), 1000, 2020);
        assert_eq!(rows.len(), 1);
        assert_close(rows[0].etoh_volume, 6.0, "etoh_volume");
        assert_close(rows[0].rvp, 10.899_000_000_000_001, "rvp");
    }

    #[test]
    fn prior_user_input_blocks_generated_row() {
        // A pre-existing AverageTankGasoline row on the generated key makes the
        // Java `insert ignore` a no-op — the generator emits nothing for it.
        let mut inputs = fixture_single_formulation();
        inputs.prior_average_tank_gasoline = vec![AverageTankGasolineRow {
            zone_id: 90_000,
            fuel_type_id: 1,
            fuel_year_id: 2020,
            month_group_id: 1,
            etoh_volume: 7.0,
            rvp: 9.9,
            is_user_input: true,
        }];
        let rows = calculate_average_tank_gasoline(&inputs, 1000, 2020);
        assert!(rows.is_empty(), "user-input row blocks generation");
    }

    #[test]
    fn unresolved_region_yields_no_rows() {
        let inputs = fixture_single_formulation();
        // No regionCounty row for county 9999.
        assert!(calculate_average_tank_gasoline(&inputs, 9999, 2020).is_empty());
        // No Year row maps the resolved fuel year to calendar year 1999.
        assert!(calculate_average_tank_gasoline(&inputs, 1000, 1999).is_empty());
        // Empty inputs resolve nothing.
        assert!(calculate_average_tank_gasoline(&TankFuelInputs::default(), 1, 2020).is_empty());
    }

    #[test]
    fn non_evap_fuel_type_is_excluded() {
        // A fuel type that is not subjectToEvapCalculations drops out of the
        // TFG-1a join, leaving no fuel-supply averages and no output.
        let mut inputs = fixture_single_formulation();
        inputs.fuel_type[0].subject_to_evap_calculations = false;
        assert!(calculate_average_tank_gasoline(&inputs, 1000, 2020).is_empty());
    }

    #[test]
    fn unselected_month_group_is_excluded() {
        // FuelSupply for a month group outside RunSpecMonthGroup is filtered
        // out, even though zone temperatures still exist for the run months.
        let mut inputs = fixture_single_formulation();
        inputs.runspec_month_group_ids = vec![2];
        assert!(calculate_average_tank_gasoline(&inputs, 1000, 2020).is_empty());
    }

    #[test]
    fn output_is_sorted_across_multiple_zones() {
        // Two zones in the county — output must be sorted by the
        // (zone, fuel type, fuel year, month group) key regardless of input
        // order, so the generator is deterministic.
        let mut inputs = fixture_single_formulation();
        inputs.zone.push(ZoneRow {
            zone_id: 80_000,
            county_id: 1000,
        });
        inputs.zone_month_hour.extend_from_slice(&[
            ZoneMonthHourRow {
                zone_id: 80_000,
                month_id: 1,
                temperature: 45.0,
            },
            ZoneMonthHourRow {
                zone_id: 80_000,
                month_id: 1,
                temperature: 95.0,
            },
        ]);
        let rows = calculate_average_tank_gasoline(&inputs, 1000, 2020);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].zone_id, 80_000);
        assert_eq!(rows[1].zone_id, 90_000);
        // Identical fuel supply / temperatures -> identical RVP per zone.
        assert_close(rows[0].rvp, rows[1].rvp, "per-zone rvp");
    }

    #[test]
    fn shared_fuel_year_multiplicity_scales_gasohol_share() {
        // Two run-selected calendar years share the resolved fuel year, so the
        // `Year ⋈ RunSpecYear` join doubles every FuelSupply row. The ratio
        // averages are invariant, but the gasohol share scales: a 0.6 share
        // doubles to 1.2 (>= 1.0), shifting the commingling factor 1.038 ->
        // 1.000 and the RVP from 10.899 to linearAverageRVP = 10.5.
        let mut inputs = fixture_two_formulations();
        inputs.year.push(YearRow {
            year_id: 2021,
            fuel_year_id: 2020,
        });
        inputs.runspec_year_ids = vec![2020, 2021];
        let rows = calculate_average_tank_gasoline(&inputs, 1000, 2020);
        assert_eq!(rows.len(), 1);
        assert_close(rows[0].etoh_volume, 6.0, "etoh_volume invariant");
        assert_close(rows[0].rvp, 10.5, "commingling factor collapses to 1.000");
    }

    // --- generator metadata ------------------------------------------------

    #[test]
    fn generator_metadata_matches_calculator_dag() {
        let gen = TankFuelGenerator::new();
        assert_eq!(gen.name(), "TankFuelGenerator");
        assert_eq!(gen.output_tables(), &["AverageTankGasoline"]);
        assert!(gen.input_tables().contains(&"FuelSupply"));
        assert!(gen.input_tables().contains(&"regionCounty"));
        assert!(gen.upstream().is_empty());

        let subs = gen.subscriptions();
        assert_eq!(subs.len(), 2, "Evap Fuel Vapor Venting + Evap Fuel Leaks");
        let processes: Vec<ProcessId> = subs.iter().map(|s| s.process_id).collect();
        assert_eq!(processes, vec![ProcessId(12), ProcessId(13)]);
        for sub in subs {
            assert_eq!(sub.granularity, Granularity::Year);
            assert_eq!(sub.priority.display(), "GENERATOR");
        }
    }

    #[test]
    fn generator_execute_returns_empty_until_data_plane() {
        // The Task 50 data plane is not yet wired; `execute` is a stand-in.
        let gen = TankFuelGenerator::new();
        let ctx = CalculatorContext::new();
        gen.execute(&ctx).expect("execute is infallible");
    }

    #[test]
    fn generator_is_object_safe() {
        // The CalculatorRegistry stores generators as `Box<dyn Generator>`.
        let gens: Vec<Box<dyn Generator>> = vec![Box::new(TankFuelGenerator::new())];
        assert_eq!(gens[0].name(), "TankFuelGenerator");
    }
}

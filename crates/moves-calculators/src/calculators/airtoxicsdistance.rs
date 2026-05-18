//! Port of `database/AirToxicsDistanceCalculator.sql` — the
//! `AirToxicsDistanceCalculator`, MOVES's distance-ratioed air-toxics
//! calculator.
//!
//! Migration plan: Phase 3, Task 51. The emission-ratioed sibling is Task 50
//! ([`super::airtoxics`]); the Nonroad air-toxics calculator is Task 52
//! ([`super::nrairtoxics`]).
//!
//! # What this calculator does
//!
//! `AirToxicsDistanceCalculator` produces the air-toxic pollutants whose
//! emission rates MOVES expresses *per unit distance travelled* rather than
//! as a ratio of another pollutant's mass: the dioxin/furan congeners and
//! the metallic toxics (mercury, arsenic, chromium, manganese, nickel, …),
//! all for the **Running Exhaust** process. For each one it multiplies the
//! vehicle distance travelled by a per-distance `meanBaseRate` lookup.
//!
//! # Supersession — empty registrations
//!
//! This is a **legacy** scripted-SQL calculator. The SQL script carries the
//! MOVES `@notused` tag; `AirToxicsDistanceCalculator` is absent from the
//! pinned runtime registry `CalculatorInfo.txt`, and
//! `characterization/calculator-chains/calculator-dag.json` records
//! `registrations_count: 0` for it. The distance-ratioed Running-Exhaust
//! toxics it would produce — pollutants 60–67 and 130–146, all process 1 —
//! are registered to `BaseRateCalculator` (Task 45) instead.
//! [`Calculator::registrations`] therefore returns an **empty slice**:
//! re-registering those pairs here would double-register them against the
//! already-merged `BaseRateCalculator`. The Java constructor's legacy
//! `EmissionCalculatorRegistration.register(...)` loop (25 placeholder
//! `ATRatioEntry` pairs) is intentionally not ported.
//!
//! The calculator does still subscribe directly to the master loop —
//! `calculator-dag.json` records `subscribes_directly: true` — so
//! [`Calculator::subscriptions`] is non-empty (see below).
//!
//! # Java / SQL structure
//!
//! Unlike the modern rates-path calculators, this is a legacy inventory-path
//! calculator with **no Go worker**: the Java `AirToxicsDistanceCalculator`
//! reads the runspec, picks the enabled `Use*` sections, and hands
//! `database/AirToxicsDistanceCalculator.sql` to a distributed worker via
//! `readAndHandleScriptedCalculations`. The SQL script carries the whole
//! computation. This module ports that script's `Section Processing`.
//!
//! # The processing pipeline
//!
//! `Section Processing` builds a chain of temp tables. This port mirrors it
//! as six functions, chained by [`AirToxicsDistanceCalculator::run`]:
//!
//! 1. **Fuel-type activity fractions** (`fuel_type_activity_fractions`, SQL
//!    `SBD2`) — join `SourceBinDistribution` to `SourceBin` on `sourceBinID`
//!    and `SUM` `sourceBinActivityFraction` per
//!    `(sourceTypeModelYearID, fuelTypeID)`.
//! 2. **Distance fractions** (`distance_fractions`, SQL `DistFracts`) — join
//!    `SourceTypeModelYear` to step 1 on `sourceTypeModelYearID`, resolving
//!    the `(sourceTypeID, modelYearID)` of each fraction.
//! 3. **Located vehicle distance** (`located_distances`, SQL `SHO2` ⋈
//!    `Link2` ⋈ `SHO3`) — join `SHO` to `HourDay` (resolving day/hour and
//!    `modelYearID = yearID - ageID`) and to `Link` / `County` (resolving the
//!    state / county / zone / road type of the link).
//! 4. **Activity output** (`activity_output`, SQL `ATActivityOutput`) — join
//!    steps 2 and 3 on `(sourceTypeID, modelYearID)`;
//!    `activity = distance * fuelTypeActivityFraction`.
//! 5. **Dioxin emissions** (`dioxin_emissions`) — join the activity to
//!    `dioxinEmissionRate` on `(fuelTypeID, modelYearID)`;
//!    `emissionQuant = activity * meanBaseRate`.
//! 6. **Metal emissions** (`metal_emissions`) — join the activity to
//!    `metalEmissionRate` on `(sourceTypeID, fuelTypeID, modelYearID)`;
//!    `emissionQuant = activity * meanBaseRate`.
//!
//! Steps 5 and 6 each emit [`WorkerOutputRow`]s; `run` concatenates them.
//! The SQL `activityTypeID` is the constant 1 (distance travelled);
//! `ATActivityOutput` is a worker-local temp table dropped in
//! `Section Cleanup`, so this port treats it as a pure intermediate and
//! emits only the `MOVESWorkerOutput` rows.
//!
//! # `Section Extract Data` is the data plane
//!
//! The script's `Section Extract Data` is the data-plane table load — the
//! tables [`AirToxicsDistanceInputs`] carries are its already-extracted
//! result. For most tables the extract is a filtered copy; for the two
//! emission-rate tables it is a transform, so the `dioxinEmissionRate` /
//! `metalEmissionRate` rows this port consumes are modelled at their
//! **worker-extracted** schema (the `CREATE TABLE` shapes in the script's
//! `Create Remote Tables` section), already:
//!
//! * **unit-normalised** — `meanBaseRate` is scaled to native units (grams
//!   per mile / TEQ per mile): a `g/km` or `TEQ/km` rate is multiplied by
//!   1.609344 km/mile;
//! * **`polProcessID`-split** — `pollutantProcessAssoc` splits the default-DB
//!   `polProcessID` into `(processID, pollutantID)`;
//! * **model-year-expanded** — the `modelYear` join expands each
//!   `modelYearGroupID` into the model years it covers.
//!
//! The Java `UseDioxinEmissionRate` / `UseMetalEmissionRate` section flags
//! gate whether each rate path runs; this port expresses the gate naturally
//! — an empty `dioxin_emission_rate` / `metal_emission_rate` input simply
//! yields no dioxin / metal output.
//!
//! # Fidelity notes
//!
//! * **`FLOAT` intermediate columns.** The SQL holds
//!   `SBD2.fuelTypeActivityFraction`, `ATActivityOutput.activity` and
//!   `MOVESWorkerOutput.emissionQuant` in `FLOAT` (32-bit) columns while
//!   evaluating the arithmetic in `DOUBLE`, so it truncates to `f32`
//!   precision between steps. This port computes in `f64` throughout; the
//!   bug-compatibility decision is deferred to Task 44 (calculator
//!   integration validation), matching the Task 41 / Task 33 generator
//!   precedents.
//! * **`SCC` is always `NULL`.** `ATActivityOutput` never receives an `SCC`
//!   value (the `INSERT` omits the column) and the script has no `SCCOutput`
//!   / `NoSCCOutput` section, so every `MOVESWorkerOutput` row has a `NULL`
//!   `SCC`; [`WorkerOutputRow`] omits the column.
//! * **No deduplication.** The SQL `INSERT ... SELECT` keeps every join row;
//!   so does this port. `run` returns the rows sorted by their full key for
//!   a deterministic result (the SQL insert order over MyISAM tables is
//!   undefined).
//!
//! # Data plane pending
//!
//! [`Calculator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are Phase-2 placeholders, so it
//! cannot yet materialise an [`AirToxicsDistanceInputs`] nor write the worker
//! output back. The faithful pipeline is fully ported and unit-tested on
//! [`AirToxicsDistanceCalculator::run`]; once the `DataFrameStore` lands,
//! `execute` builds the inputs from `ctx.tables()`, calls `run`, and stores
//! the [`WorkerOutputRow`]s.

use std::collections::{BTreeMap, HashMap};
use std::sync::OnceLock;

use moves_calculator_info::{Granularity, Priority};
use moves_data::{PollutantProcessAssociation, ProcessId};
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, Error,
};

/// MOVES process id for Running Exhaust — the single process the calculator
/// subscribes to and the process of every pollutant it would compute.
const RUNNING_EXHAUST_PROCESS_ID: u16 = 1;

// ===========================================================================
// Input row structs — one per table `Section Processing` reads.
//
// Every table is the already-extracted output of `Section Extract Data`. The
// two emission-rate tables are modelled at their worker-extracted schema (see
// the module-level "Section Extract Data is the data plane" note); the rest
// are filtered copies of the same-named default-DB tables.
// ===========================================================================

/// One `SourceBinDistribution` row — the source-bin activity split.
///
/// `Section Extract Data` filters this table to the run's Running-Exhaust
/// `polProcessID`, and `Section Processing`'s `SBD2` aggregate does not
/// reference `polProcessID`, so the column is not modelled.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceBinDistributionRow {
    /// `sourceTypeModelYearID` — the source-type / model-year key.
    pub source_type_model_year_id: i32,
    /// `sourceBinID` — the source bin (a `BIGINT` in MOVES).
    pub source_bin_id: i64,
    /// `sourceBinActivityFraction` — fraction of activity in this bin.
    pub source_bin_activity_fraction: f64,
}

/// One `SourceBin` row — supplies a source bin's fuel type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceBinRow {
    /// `sourceBinID`.
    pub source_bin_id: i64,
    /// `fuelTypeID` — the fuel type of the bin.
    pub fuel_type_id: i32,
}

/// One `SourceTypeModelYear` row — ties a `sourceTypeModelYearID` to its
/// `(sourceTypeID, modelYearID)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceTypeModelYearRow {
    /// `sourceTypeModelYearID` — the surrogate key.
    pub source_type_model_year_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `modelYearID` — the vehicle model year.
    pub model_year_id: i32,
}

/// One `SHO` row — the source-hours-operating activity record, carrying the
/// distance travelled.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShoRow {
    /// `hourDayID` — joined hour-of-day / day-of-week bucket.
    pub hour_day_id: i32,
    /// `yearID` — the calendar year.
    pub year_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `ageID` — the vehicle age; `modelYearID = yearID - ageID`.
    pub age_id: i32,
    /// `linkID`.
    pub link_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `distance` — distance travelled, in miles.
    pub distance: f64,
}

/// One `HourDay` row — decodes an `hourDayID` into day-of-week and hour.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HourDayRow {
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `dayID` — day-of-week bucket.
    pub day_id: i32,
    /// `hourID` — hour-of-day.
    pub hour_id: i32,
}

/// One `Link` row — the road link, supplying its location columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LinkRow {
    /// `linkID`.
    pub link_id: i32,
    /// `countyID` — resolves the link's state through `County`.
    pub county_id: i32,
    /// `zoneID`.
    pub zone_id: i32,
    /// `roadTypeID`.
    pub road_type_id: i32,
}

/// One `County` row — maps a county to its state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CountyRow {
    /// `countyID`.
    pub county_id: i32,
    /// `stateID`.
    pub state_id: i32,
}

/// One worker-extracted `dioxinEmissionRate` row — a per-distance dioxin /
/// furan emission rate.
///
/// Modelled at the worker schema the script's `Create Remote Tables` section
/// defines: `polProcessID` is already split into `(processID, pollutantID)`,
/// `modelYearGroupID` is already expanded to `modelYearID`, and `meanBaseRate`
/// is already unit-normalised to per-mile (see the module-level data-plane
/// note).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DioxinEmissionRateRow {
    /// `processID`.
    pub process_id: i32,
    /// `pollutantID`.
    pub pollutant_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `modelYearID`.
    pub model_year_id: i32,
    /// `meanBaseRate` — emission per mile travelled.
    pub mean_base_rate: f64,
}

/// One worker-extracted `metalEmissionRate` row — a per-distance metallic-toxic
/// emission rate.
///
/// Like [`DioxinEmissionRateRow`] but additionally keyed on `sourceTypeID`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MetalEmissionRateRow {
    /// `processID`.
    pub process_id: i32,
    /// `pollutantID`.
    pub pollutant_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `modelYearID`.
    pub model_year_id: i32,
    /// `meanBaseRate` — emission per mile travelled.
    pub mean_base_rate: f64,
}

/// The full set of input tables `AirToxicsDistanceCalculator.sql`'s
/// `Section Processing` reads.
#[derive(Debug, Clone, Default)]
pub struct AirToxicsDistanceInputs {
    /// `SourceBinDistribution` rows.
    pub source_bin_distribution: Vec<SourceBinDistributionRow>,
    /// `SourceBin` rows.
    pub source_bin: Vec<SourceBinRow>,
    /// `SourceTypeModelYear` rows.
    pub source_type_model_year: Vec<SourceTypeModelYearRow>,
    /// `SHO` rows.
    pub sho: Vec<ShoRow>,
    /// `HourDay` rows.
    pub hour_day: Vec<HourDayRow>,
    /// `Link` rows.
    pub link: Vec<LinkRow>,
    /// `County` rows.
    pub county: Vec<CountyRow>,
    /// Worker-extracted `dioxinEmissionRate` rows.
    pub dioxin_emission_rate: Vec<DioxinEmissionRateRow>,
    /// Worker-extracted `metalEmissionRate` rows.
    pub metal_emission_rate: Vec<MetalEmissionRateRow>,
}

// ===========================================================================
// Intermediate result rows — one per SQL temp table in `Section Processing`.
// ===========================================================================

/// Step 1 output — the SQL `SBD2` table: the activity fraction of a
/// `sourceTypeModelYearID` carried by one fuel type.
#[derive(Debug, Clone, Copy, PartialEq)]
struct FuelTypeActivityFraction {
    source_type_model_year_id: i32,
    fuel_type_id: i32,
    fuel_type_activity_fraction: f64,
}

/// Step 2 output — the SQL `DistFracts` table.
///
/// The SQL `DistFracts` also projects `sourceTypeModelYearID`, but the
/// `ATActivityOutput` join consumes only `(sourceTypeID, modelYearID)` and the
/// projected `(fuelTypeID, fuelTypeActivityFraction)`, so the unused surrogate
/// key is dropped here.
#[derive(Debug, Clone, Copy, PartialEq)]
struct DistanceFraction {
    source_type_id: i32,
    model_year_id: i32,
    fuel_type_id: i32,
    fuel_type_activity_fraction: f64,
}

/// Step 3 output — the SQL `SHO3` table: a distance record with its
/// model year and full location resolved.
#[derive(Debug, Clone, Copy, PartialEq)]
struct LocatedDistance {
    year_id: i32,
    month_id: i32,
    day_id: i32,
    hour_id: i32,
    model_year_id: i32,
    link_id: i32,
    source_type_id: i32,
    distance: f64,
    state_id: i32,
    county_id: i32,
    zone_id: i32,
    road_type_id: i32,
}

/// Step 4 output — the SQL `ATActivityOutput` table: distance apportioned to a
/// fuel type. A worker-local temp table (dropped in `Section Cleanup`), so
/// this is a pure intermediate; `activityTypeID` is the constant 1.
#[derive(Debug, Clone, Copy, PartialEq)]
struct ActivityOutputRow {
    year_id: i32,
    month_id: i32,
    day_id: i32,
    hour_id: i32,
    state_id: i32,
    county_id: i32,
    zone_id: i32,
    link_id: i32,
    source_type_id: i32,
    fuel_type_id: i32,
    model_year_id: i32,
    road_type_id: i32,
    activity: f64,
}

/// One `MOVESWorkerOutput` row — the calculator's contribution to the master
/// emission tally.
///
/// The SQL `INSERT` leaves `SCC`, `emissionRate`, `regClassID` and
/// `fuelSubTypeID` `NULL`; only the columns the dioxin / metal sections
/// populate are modelled.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WorkerOutputRow {
    /// `yearID`.
    pub year_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `dayID`.
    pub day_id: i32,
    /// `hourID`.
    pub hour_id: i32,
    /// `stateID`.
    pub state_id: i32,
    /// `countyID`.
    pub county_id: i32,
    /// `zoneID`.
    pub zone_id: i32,
    /// `linkID`.
    pub link_id: i32,
    /// `pollutantID` — from the joined emission-rate row.
    pub pollutant_id: i32,
    /// `processID` — from the joined emission-rate row.
    pub process_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `modelYearID`.
    pub model_year_id: i32,
    /// `roadTypeID`.
    pub road_type_id: i32,
    /// `emissionQuant` — `activity * meanBaseRate`.
    pub emission_quant: f64,
}

/// Total order over a [`WorkerOutputRow`]'s key columns.
///
/// `WorkerOutputRow` cannot derive `Ord` (its `emission_quant` is an `f64`);
/// `run` sorts by this 14-column key so the output is deterministic
/// regardless of input order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct WorkerOutputSortKey {
    process_id: i32,
    pollutant_id: i32,
    year_id: i32,
    month_id: i32,
    day_id: i32,
    hour_id: i32,
    state_id: i32,
    county_id: i32,
    zone_id: i32,
    link_id: i32,
    source_type_id: i32,
    fuel_type_id: i32,
    model_year_id: i32,
    road_type_id: i32,
}

/// The key columns of a [`WorkerOutputRow`], for [`AirToxicsDistanceCalculator::run`]'s
/// deterministic final sort.
fn sort_key(row: &WorkerOutputRow) -> WorkerOutputSortKey {
    WorkerOutputSortKey {
        process_id: row.process_id,
        pollutant_id: row.pollutant_id,
        year_id: row.year_id,
        month_id: row.month_id,
        day_id: row.day_id,
        hour_id: row.hour_id,
        state_id: row.state_id,
        county_id: row.county_id,
        zone_id: row.zone_id,
        link_id: row.link_id,
        source_type_id: row.source_type_id,
        fuel_type_id: row.fuel_type_id,
        model_year_id: row.model_year_id,
        road_type_id: row.road_type_id,
    }
}

// ===========================================================================
// Step 1 — fuel-type activity fractions (SQL `SBD2`).
// ===========================================================================

/// Step 1 — the SQL `SBD2` insert.
///
/// Ports
/// `SELECT sbd.sourceTypeModelYearID, sb.fuelTypeID, sum(sbd.sourceBinActivityFraction)
/// FROM sourceBinDistribution sbd INNER JOIN SourceBin sb USING (sourceBinID)
/// GROUP BY sourceTypeModelYearID, fuelTypeID`. The `INNER JOIN` drops any
/// `SourceBinDistribution` row whose `sourceBinID` is absent from `SourceBin`.
/// The `BTreeMap` accumulator keeps the result ordered by the `GROUP BY` key.
fn fuel_type_activity_fractions(inputs: &AirToxicsDistanceInputs) -> Vec<FuelTypeActivityFraction> {
    // SourceBin's fuel type, keyed by source bin id.
    let fuel_type_of: HashMap<i64, i32> = inputs
        .source_bin
        .iter()
        .map(|sb| (sb.source_bin_id, sb.fuel_type_id))
        .collect();

    let mut acc: BTreeMap<(i32, i32), f64> = BTreeMap::new();
    for sbd in &inputs.source_bin_distribution {
        let Some(&fuel_type_id) = fuel_type_of.get(&sbd.source_bin_id) else {
            continue;
        };
        *acc.entry((sbd.source_type_model_year_id, fuel_type_id))
            .or_insert(0.0) += sbd.source_bin_activity_fraction;
    }
    acc.into_iter()
        .map(
            |((source_type_model_year_id, fuel_type_id), sum)| FuelTypeActivityFraction {
                source_type_model_year_id,
                fuel_type_id,
                fuel_type_activity_fraction: sum,
            },
        )
        .collect()
}

// ===========================================================================
// Step 2 — distance fractions (SQL `DistFracts`).
// ===========================================================================

/// Step 2 — the SQL `DistFracts` table.
///
/// Ports
/// `SELECT stmy.sourceTypeID, stmy.modelYearID, sbd.fuelTypeID, sbd.fuelTypeActivityFraction
/// FROM SourceTypeModelYear stmy INNER JOIN SBD2 sbd USING (sourceTypeModelYearID)`,
/// resolving each step-1 fraction's `(sourceTypeID, modelYearID)`. The
/// `INNER JOIN` drops a fraction whose `sourceTypeModelYearID` is absent from
/// `SourceTypeModelYear`.
fn distance_fractions(
    fuel_type_fractions: &[FuelTypeActivityFraction],
    inputs: &AirToxicsDistanceInputs,
) -> Vec<DistanceFraction> {
    let stmy_by_id: HashMap<i32, &SourceTypeModelYearRow> = inputs
        .source_type_model_year
        .iter()
        .map(|stmy| (stmy.source_type_model_year_id, stmy))
        .collect();

    let mut out = Vec::new();
    for ftf in fuel_type_fractions {
        let Some(stmy) = stmy_by_id.get(&ftf.source_type_model_year_id) else {
            continue;
        };
        out.push(DistanceFraction {
            source_type_id: stmy.source_type_id,
            model_year_id: stmy.model_year_id,
            fuel_type_id: ftf.fuel_type_id,
            fuel_type_activity_fraction: ftf.fuel_type_activity_fraction,
        });
    }
    out
}

// ===========================================================================
// Step 3 — located vehicle distance (SQL `SHO2` ⋈ `Link2` ⋈ `SHO3`).
// ===========================================================================

/// Step 3 — the SQL `SHO3` table.
///
/// Fuses the script's three temp tables — `SHO2` (`SHO` ⋈ `HourDay`), `Link2`
/// (`Link` ⋈ `County`) and `SHO3` (`SHO2` ⋈ `Link2`) — into one pass, since
/// `SHO2` and `Link2` are each consumed only by `SHO3`. Each `SHO` row gains
/// its day / hour, its `modelYearID = yearID - ageID`, and the state / county
/// / zone / road type of its link. The `INNER JOIN`s drop an `SHO` row whose
/// `hourDayID` or `linkID` is unmatched, or whose link's `countyID` is absent
/// from `County`.
fn located_distances(inputs: &AirToxicsDistanceInputs) -> Vec<LocatedDistance> {
    let hour_day_by_id: HashMap<i32, &HourDayRow> = inputs
        .hour_day
        .iter()
        .map(|hd| (hd.hour_day_id, hd))
        .collect();
    let state_of_county: HashMap<i32, i32> = inputs
        .county
        .iter()
        .map(|c| (c.county_id, c.state_id))
        .collect();
    // `Link2`: a link keyed by id, carrying its location columns. The
    // `filter_map` enforces `Link2`'s `INNER JOIN County` — a link whose
    // county is unknown is dropped.
    let link_by_id: HashMap<i32, (i32, i32, i32, i32)> = inputs
        .link
        .iter()
        .filter_map(|link| {
            let state_id = *state_of_county.get(&link.county_id)?;
            Some((
                link.link_id,
                (state_id, link.county_id, link.zone_id, link.road_type_id),
            ))
        })
        .collect();

    let mut out = Vec::new();
    for sho in &inputs.sho {
        let Some(hd) = hour_day_by_id.get(&sho.hour_day_id) else {
            continue;
        };
        let Some(&(state_id, county_id, zone_id, road_type_id)) = link_by_id.get(&sho.link_id)
        else {
            continue;
        };
        out.push(LocatedDistance {
            year_id: sho.year_id,
            month_id: sho.month_id,
            day_id: hd.day_id,
            hour_id: hd.hour_id,
            model_year_id: sho.year_id - sho.age_id,
            link_id: sho.link_id,
            source_type_id: sho.source_type_id,
            distance: sho.distance,
            state_id,
            county_id,
            zone_id,
            road_type_id,
        });
    }
    out
}

// ===========================================================================
// Step 4 — activity output (SQL `ATActivityOutput`).
// ===========================================================================

/// Step 4 — the SQL `ATActivityOutput` insert.
///
/// Ports
/// `SELECT ... (sho.distance * df.fuelTypeActivityFraction)
/// FROM DistFracts df INNER JOIN SHO3 sho USING (sourceTypeID, modelYearID)`.
/// A located distance fans out to one activity row per matching distance
/// fraction — i.e. one per fuel type that source-type / model-year carries.
fn activity_output(
    distance_fractions: &[DistanceFraction],
    located: &[LocatedDistance],
) -> Vec<ActivityOutputRow> {
    // DistFracts indexed by the `(sourceTypeID, modelYearID)` join key.
    let mut df_by_key: HashMap<(i32, i32), Vec<&DistanceFraction>> = HashMap::new();
    for df in distance_fractions {
        df_by_key
            .entry((df.source_type_id, df.model_year_id))
            .or_default()
            .push(df);
    }

    let mut out = Vec::new();
    for sho in located {
        let Some(dfs) = df_by_key.get(&(sho.source_type_id, sho.model_year_id)) else {
            continue;
        };
        for df in dfs {
            out.push(ActivityOutputRow {
                year_id: sho.year_id,
                month_id: sho.month_id,
                day_id: sho.day_id,
                hour_id: sho.hour_id,
                state_id: sho.state_id,
                county_id: sho.county_id,
                zone_id: sho.zone_id,
                link_id: sho.link_id,
                source_type_id: sho.source_type_id,
                fuel_type_id: df.fuel_type_id,
                model_year_id: sho.model_year_id,
                road_type_id: sho.road_type_id,
                activity: sho.distance * df.fuel_type_activity_fraction,
            });
        }
    }
    out
}

// ===========================================================================
// Steps 5 / 6 — dioxin and metal emissions.
// ===========================================================================

/// Step 5 — the SQL `Section UseDioxinEmissionRate` `MOVESWorkerOutput`
/// insert.
///
/// Ports the join `ATActivityOutput a INNER JOIN dioxinEmissionRate r
/// ON (r.fuelTypeID = a.fuelTypeID AND r.modelYearID = a.modelYearID)` with
/// `emissionQuant = activity * meanBaseRate`. The join ignores `sourceTypeID`
/// — a dioxin rate applies to every source type at its `(fuelTypeID,
/// modelYearID)` — and an activity row fans out to one output row per
/// `(processID, pollutantID)` registered for that key.
fn dioxin_emissions(
    activity: &[ActivityOutputRow],
    inputs: &AirToxicsDistanceInputs,
) -> Vec<WorkerOutputRow> {
    let mut rates_by_key: HashMap<(i32, i32), Vec<&DioxinEmissionRateRow>> = HashMap::new();
    for r in &inputs.dioxin_emission_rate {
        rates_by_key
            .entry((r.fuel_type_id, r.model_year_id))
            .or_default()
            .push(r);
    }

    let mut out = Vec::new();
    for a in activity {
        let Some(rates) = rates_by_key.get(&(a.fuel_type_id, a.model_year_id)) else {
            continue;
        };
        for r in rates {
            out.push(WorkerOutputRow {
                year_id: a.year_id,
                month_id: a.month_id,
                day_id: a.day_id,
                hour_id: a.hour_id,
                state_id: a.state_id,
                county_id: a.county_id,
                zone_id: a.zone_id,
                link_id: a.link_id,
                pollutant_id: r.pollutant_id,
                process_id: r.process_id,
                source_type_id: a.source_type_id,
                fuel_type_id: a.fuel_type_id,
                model_year_id: a.model_year_id,
                road_type_id: a.road_type_id,
                emission_quant: a.activity * r.mean_base_rate,
            });
        }
    }
    out
}

/// Step 6 — the SQL `Section UseMetalEmissionRate` `MOVESWorkerOutput` insert.
///
/// Like [`dioxin_emissions`](fn@dioxin_emissions) but the `metalEmissionRate`
/// join additionally keys on `sourceTypeID`:
/// `r.sourceTypeID = a.sourceTypeID AND r.fuelTypeID = a.fuelTypeID
/// AND r.modelYearID = a.modelYearID`.
fn metal_emissions(
    activity: &[ActivityOutputRow],
    inputs: &AirToxicsDistanceInputs,
) -> Vec<WorkerOutputRow> {
    let mut rates_by_key: HashMap<(i32, i32, i32), Vec<&MetalEmissionRateRow>> = HashMap::new();
    for r in &inputs.metal_emission_rate {
        rates_by_key
            .entry((r.source_type_id, r.fuel_type_id, r.model_year_id))
            .or_default()
            .push(r);
    }

    let mut out = Vec::new();
    for a in activity {
        let Some(rates) = rates_by_key.get(&(a.source_type_id, a.fuel_type_id, a.model_year_id))
        else {
            continue;
        };
        for r in rates {
            out.push(WorkerOutputRow {
                year_id: a.year_id,
                month_id: a.month_id,
                day_id: a.day_id,
                hour_id: a.hour_id,
                state_id: a.state_id,
                county_id: a.county_id,
                zone_id: a.zone_id,
                link_id: a.link_id,
                pollutant_id: r.pollutant_id,
                process_id: r.process_id,
                source_type_id: a.source_type_id,
                fuel_type_id: a.fuel_type_id,
                model_year_id: a.model_year_id,
                road_type_id: a.road_type_id,
                emission_quant: a.activity * r.mean_base_rate,
            });
        }
    }
    out
}

// ===========================================================================
// The calculator.
// ===========================================================================

/// The Air Toxics Distance Calculator.
///
/// A zero-sized value type: the calculator owns no per-run state, exactly as
/// the [`Calculator`] trait requires. All run-varying input flows through
/// [`AirToxicsDistanceCalculator::run`].
#[derive(Debug, Clone, Copy, Default)]
pub struct AirToxicsDistanceCalculator;

impl AirToxicsDistanceCalculator {
    /// Chain-DAG name — matches the Java class and the `calculator-dag.json`
    /// entry.
    pub const NAME: &'static str = "AirToxicsDistanceCalculator";

    /// Construct the calculator.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Run the calculator over a fully materialised set of input tables.
    ///
    /// Chains the six processing steps of `AirToxicsDistanceCalculator.sql`
    /// and returns the `MOVESWorkerOutput` rows the dioxin and metal sections
    /// would insert. The rows are sorted by their full key for a deterministic
    /// result (the SQL `INSERT ... SELECT` order over MyISAM tables is
    /// undefined).
    #[must_use]
    pub fn run(inputs: &AirToxicsDistanceInputs) -> Vec<WorkerOutputRow> {
        let fuel_type_fractions = fuel_type_activity_fractions(inputs);
        let distance_fractions = distance_fractions(&fuel_type_fractions, inputs);
        let located = located_distances(inputs);
        let activity = activity_output(&distance_fractions, &located);

        let mut out = dioxin_emissions(&activity, inputs);
        out.extend(metal_emissions(&activity, inputs));
        out.sort_by_key(sort_key);
        out
    }
}

/// The calculator's master-loop subscription.
///
/// The Java `subscribeToMe` subscribes to **Running Exhaust** (process 1) at
/// `YEAR` granularity and `EMISSION_CALCULATOR` priority, gated on the runspec
/// carrying a Running-Exhaust pollutant. `calculator-dag.json` records the
/// subscription (`subscribes_directly: true`) but with a placeholder
/// `process_id` of 0, because the static analyser cannot resolve the
/// `EmissionProcess.findByName("Running Exhaust")` lookup — the true id (1)
/// comes from the Java.
fn subscriptions() -> &'static [CalculatorSubscription] {
    static SUBS: OnceLock<[CalculatorSubscription; 1]> = OnceLock::new();
    SUBS.get_or_init(|| {
        let priority = Priority::parse("EMISSION_CALCULATOR")
            .expect("EMISSION_CALCULATOR is a valid priority");
        [CalculatorSubscription::new(
            ProcessId(RUNNING_EXHAUST_PROCESS_ID),
            Granularity::Year,
            priority,
        )]
    })
}

/// The `(pollutant, process)` pairs the calculator registers — **none**.
///
/// `AirToxicsDistanceCalculator` is a legacy calculator superseded by
/// `BaseRateCalculator` (see the module-level supersession note): its SQL
/// script carries the MOVES `@notused` tag, it is absent from
/// `CalculatorInfo.txt`, and `calculator-dag.json` records
/// `registrations_count: 0`. The distance-ratioed Running-Exhaust toxics it
/// would produce are registered to `BaseRateCalculator` instead, so
/// registering them here too would double-register them.
static REGISTRATIONS: &[PollutantProcessAssociation] = &[];

/// Tables `AirToxicsDistanceCalculator.sql`'s `Section Processing` reads.
///
/// `dioxinEmissionRate` / `metalEmissionRate` name the default-DB rate
/// tables; the data plane applies the `Section Extract Data` transform that
/// reshapes them into the worker schema this port consumes. The extract-only
/// `EmissionProcess` table, which `Section Processing` does not read, is
/// omitted.
static INPUT_TABLES: &[&str] = &[
    "SourceBinDistribution",
    "SourceBin",
    "SourceTypeModelYear",
    "SHO",
    "HourDay",
    "Link",
    "County",
    "dioxinEmissionRate",
    "metalEmissionRate",
];

impl Calculator for AirToxicsDistanceCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        subscriptions()
    }

    /// No registrations — `AirToxicsDistanceCalculator` is superseded by
    /// `BaseRateCalculator` (see `REGISTRATIONS` and the module-level
    /// supersession note).
    fn registrations(&self) -> &[PollutantProcessAssociation] {
        REGISTRATIONS
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    /// Run the calculator for the current master-loop iteration.
    ///
    /// **Data plane pending.** [`CalculatorContext`] exposes only placeholder
    /// `ExecutionTables` / `ScratchNamespace` today, so this body cannot build
    /// an [`AirToxicsDistanceInputs`] nor write the worker output back. The
    /// faithful pipeline is ported and tested on
    /// [`AirToxicsDistanceCalculator::run`]; once the `DataFrameStore` lands,
    /// `execute` materialises the inputs from `ctx.tables()`, calls `run`, and
    /// stores the [`WorkerOutputRow`]s.
    fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        Ok(CalculatorOutput::empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal set of inputs that flows one distance record cleanly through
    /// all six steps, producing one dioxin and one metal output row.
    ///
    /// `year = 2020`, age 5 ⇒ `modelYearID = 2015`; one source type (21), one
    /// source bin (900, fuel type 2), one link (8001 in county 26161, state
    /// 26). Distance 100 mi, fuel-type activity fraction 0.8 ⇒ activity 80 mi.
    fn single_flow_inputs() -> AirToxicsDistanceInputs {
        AirToxicsDistanceInputs {
            source_bin_distribution: vec![SourceBinDistributionRow {
                source_type_model_year_id: 700,
                source_bin_id: 900,
                source_bin_activity_fraction: 0.8,
            }],
            source_bin: vec![SourceBinRow {
                source_bin_id: 900,
                fuel_type_id: 2,
            }],
            source_type_model_year: vec![SourceTypeModelYearRow {
                source_type_model_year_id: 700,
                source_type_id: 21,
                model_year_id: 2015,
            }],
            sho: vec![ShoRow {
                hour_day_id: 51,
                year_id: 2020,
                month_id: 7,
                age_id: 5,
                link_id: 8001,
                source_type_id: 21,
                distance: 100.0,
            }],
            hour_day: vec![HourDayRow {
                hour_day_id: 51,
                day_id: 5,
                hour_id: 14,
            }],
            link: vec![LinkRow {
                link_id: 8001,
                county_id: 26161,
                zone_id: 261_610,
                road_type_id: 4,
            }],
            county: vec![CountyRow {
                county_id: 26161,
                state_id: 26,
            }],
            dioxin_emission_rate: vec![DioxinEmissionRateRow {
                process_id: 1,
                pollutant_id: 130,
                fuel_type_id: 2,
                model_year_id: 2015,
                mean_base_rate: 0.05,
            }],
            metal_emission_rate: vec![MetalEmissionRateRow {
                process_id: 1,
                pollutant_id: 63,
                fuel_type_id: 2,
                source_type_id: 21,
                model_year_id: 2015,
                mean_base_rate: 0.02,
            }],
        }
    }

    #[test]
    fn metadata_matches_the_dag_entry() {
        let calc = AirToxicsDistanceCalculator::new();
        assert_eq!(calc.name(), "AirToxicsDistanceCalculator");

        // One subscription: Running Exhaust, YEAR, EMISSION_CALCULATOR.
        let subs = calc.subscriptions();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].process_id, ProcessId(1));
        assert_eq!(subs[0].granularity, Granularity::Year);
        assert_eq!(subs[0].priority.display(), "EMISSION_CALCULATOR");

        // No registrations — superseded by BaseRateCalculator
        // (calculator-dag.json: registrations_count 0).
        assert!(calc.registrations().is_empty());

        // depends_on is empty in the DAG entry.
        assert!(calc.upstream().is_empty());

        assert!(calc.input_tables().contains(&"dioxinEmissionRate"));
        assert!(calc.input_tables().contains(&"metalEmissionRate"));
        assert!(calc.input_tables().contains(&"SHO"));
    }

    #[test]
    fn execute_is_a_shell_until_the_data_plane_lands() {
        let calc = AirToxicsDistanceCalculator::new();
        let ctx = CalculatorContext::new();
        assert!(calc.execute(&ctx).is_ok());
    }

    #[test]
    fn calculator_is_object_safe() {
        let calcs: Vec<Box<dyn Calculator>> = vec![Box::new(AirToxicsDistanceCalculator::new())];
        assert_eq!(calcs[0].name(), "AirToxicsDistanceCalculator");
    }

    #[test]
    fn run_on_empty_inputs_yields_no_output() {
        assert!(AirToxicsDistanceCalculator::run(&AirToxicsDistanceInputs::default()).is_empty());
    }

    #[test]
    fn end_to_end_single_flow() {
        let out = AirToxicsDistanceCalculator::run(&single_flow_inputs());
        // One dioxin row + one metal row, sorted by key (pollutant 63 < 130).
        assert_eq!(out.len(), 2);

        let metal = out[0];
        assert_eq!(metal.pollutant_id, 63);
        assert_eq!(metal.process_id, 1);
        assert_eq!(metal.source_type_id, 21);
        assert_eq!(metal.fuel_type_id, 2);
        assert_eq!(metal.model_year_id, 2015);
        assert_eq!(metal.year_id, 2020);
        assert_eq!(metal.month_id, 7);
        assert_eq!(metal.day_id, 5);
        assert_eq!(metal.hour_id, 14);
        assert_eq!(metal.state_id, 26);
        assert_eq!(metal.county_id, 26161);
        assert_eq!(metal.zone_id, 261_610);
        assert_eq!(metal.road_type_id, 4);
        // activity 80 mi * 0.02 rate.
        assert!((metal.emission_quant - 1.6).abs() < 1e-12);

        let dioxin = out[1];
        assert_eq!(dioxin.pollutant_id, 130);
        assert_eq!(dioxin.process_id, 1);
        assert_eq!(dioxin.source_type_id, 21);
        // activity 80 mi * 0.05 rate.
        assert!((dioxin.emission_quant - 4.0).abs() < 1e-12);
    }

    #[test]
    fn fuel_type_activity_fractions_sum_over_source_bins() {
        // A second source bin, same source-type/model-year, same fuel type:
        // the SBD2 GROUP BY sums the two activity fractions.
        let mut inputs = single_flow_inputs();
        inputs.source_bin.push(SourceBinRow {
            source_bin_id: 901,
            fuel_type_id: 2,
        });
        inputs
            .source_bin_distribution
            .push(SourceBinDistributionRow {
                source_type_model_year_id: 700,
                source_bin_id: 901,
                source_bin_activity_fraction: 0.15,
            });
        let sbd2 = fuel_type_activity_fractions(&inputs);
        assert_eq!(sbd2.len(), 1);
        assert_eq!(sbd2[0].source_type_model_year_id, 700);
        assert_eq!(sbd2[0].fuel_type_id, 2);
        assert!((sbd2[0].fuel_type_activity_fraction - 0.95).abs() < 1e-12);
    }

    #[test]
    fn fuel_type_activity_fractions_split_distinct_fuel_types() {
        // Two bins of different fuel types stay in separate SBD2 groups.
        let mut inputs = single_flow_inputs();
        inputs.source_bin.push(SourceBinRow {
            source_bin_id: 902,
            fuel_type_id: 1,
        });
        inputs
            .source_bin_distribution
            .push(SourceBinDistributionRow {
                source_type_model_year_id: 700,
                source_bin_id: 902,
                source_bin_activity_fraction: 0.2,
            });
        let sbd2 = fuel_type_activity_fractions(&inputs);
        assert_eq!(sbd2.len(), 2);
        // BTreeMap key order: fuel type 1 before fuel type 2.
        assert_eq!(sbd2[0].fuel_type_id, 1);
        assert!((sbd2[0].fuel_type_activity_fraction - 0.2).abs() < 1e-12);
        assert_eq!(sbd2[1].fuel_type_id, 2);
        assert!((sbd2[1].fuel_type_activity_fraction - 0.8).abs() < 1e-12);
    }

    #[test]
    fn fuel_type_activity_fractions_drop_bins_absent_from_source_bin() {
        // A SourceBinDistribution row whose bin is not in SourceBin is dropped
        // by the INNER JOIN.
        let mut inputs = single_flow_inputs();
        inputs
            .source_bin_distribution
            .push(SourceBinDistributionRow {
                source_type_model_year_id: 700,
                source_bin_id: 999,
                source_bin_activity_fraction: 0.5,
            });
        let sbd2 = fuel_type_activity_fractions(&inputs);
        assert_eq!(sbd2.len(), 1);
        assert!((sbd2[0].fuel_type_activity_fraction - 0.8).abs() < 1e-12);
    }

    #[test]
    fn distance_fractions_resolve_source_type_and_model_year() {
        let inputs = single_flow_inputs();
        let sbd2 = fuel_type_activity_fractions(&inputs);
        let dist = distance_fractions(&sbd2, &inputs);
        assert_eq!(dist.len(), 1);
        assert_eq!(dist[0].source_type_id, 21);
        assert_eq!(dist[0].model_year_id, 2015);
        assert_eq!(dist[0].fuel_type_id, 2);
        assert!((dist[0].fuel_type_activity_fraction - 0.8).abs() < 1e-12);
    }

    #[test]
    fn distance_fractions_drop_fractions_with_unknown_source_type_model_year() {
        // No SourceTypeModelYear row for the surrogate key -> dropped.
        let mut inputs = single_flow_inputs();
        inputs.source_type_model_year.clear();
        let sbd2 = fuel_type_activity_fractions(&inputs);
        assert!(distance_fractions(&sbd2, &inputs).is_empty());
    }

    #[test]
    fn located_distances_compute_model_year_from_age() {
        let located = located_distances(&single_flow_inputs());
        assert_eq!(located.len(), 1);
        // modelYearID = yearID - ageID = 2020 - 5.
        assert_eq!(located[0].model_year_id, 2015);
        assert_eq!(located[0].day_id, 5);
        assert_eq!(located[0].hour_id, 14);
        assert_eq!(located[0].state_id, 26);
        assert_eq!(located[0].county_id, 26161);
        assert_eq!(located[0].zone_id, 261_610);
        assert_eq!(located[0].road_type_id, 4);
        assert!((located[0].distance - 100.0).abs() < 1e-12);
    }

    #[test]
    fn located_distances_drop_sho_without_an_hour_day() {
        let mut inputs = single_flow_inputs();
        inputs.hour_day.clear();
        assert!(located_distances(&inputs).is_empty());
    }

    #[test]
    fn located_distances_drop_links_whose_county_is_unknown() {
        // Link2's INNER JOIN County drops a link whose county is absent.
        let mut inputs = single_flow_inputs();
        inputs.county.clear();
        assert!(located_distances(&inputs).is_empty());
    }

    #[test]
    fn activity_output_multiplies_distance_by_fuel_fraction() {
        let inputs = single_flow_inputs();
        let sbd2 = fuel_type_activity_fractions(&inputs);
        let dist = distance_fractions(&sbd2, &inputs);
        let located = located_distances(&inputs);
        let activity = activity_output(&dist, &located);
        assert_eq!(activity.len(), 1);
        // distance 100 * fuelTypeActivityFraction 0.8.
        assert!((activity[0].activity - 80.0).abs() < 1e-12);
        assert_eq!(activity[0].fuel_type_id, 2);
        assert_eq!(activity[0].source_type_id, 21);
        assert_eq!(activity[0].model_year_id, 2015);
    }

    #[test]
    fn activity_output_fans_out_per_fuel_type() {
        // Two fuel types for the same source-type/model-year: the distance is
        // apportioned to each.
        let mut inputs = single_flow_inputs();
        inputs.source_bin.push(SourceBinRow {
            source_bin_id: 902,
            fuel_type_id: 1,
        });
        inputs
            .source_bin_distribution
            .push(SourceBinDistributionRow {
                source_type_model_year_id: 700,
                source_bin_id: 902,
                source_bin_activity_fraction: 0.2,
            });
        let sbd2 = fuel_type_activity_fractions(&inputs);
        let dist = distance_fractions(&sbd2, &inputs);
        let located = located_distances(&inputs);
        let activity = activity_output(&dist, &located);
        assert_eq!(activity.len(), 2);
        let total: f64 = activity.iter().map(|a| a.activity).sum();
        // 100 * 0.2 + 100 * 0.8.
        assert!((total - 100.0).abs() < 1e-12);
    }

    #[test]
    fn dioxin_rate_fans_out_to_multiple_pollutants() {
        // A second dioxin congener at the same (fuelTypeID, modelYearID):
        // the activity fans out to one output row per pollutant.
        let mut inputs = single_flow_inputs();
        inputs.dioxin_emission_rate.push(DioxinEmissionRateRow {
            process_id: 1,
            pollutant_id: 131,
            fuel_type_id: 2,
            model_year_id: 2015,
            mean_base_rate: 0.07,
        });
        let out = AirToxicsDistanceCalculator::run(&inputs);
        let dioxin: Vec<_> = out.iter().filter(|r| r.pollutant_id >= 130).collect();
        assert_eq!(dioxin.len(), 2);
        let total: f64 = dioxin.iter().map(|r| r.emission_quant).sum();
        // 80 * 0.05 + 80 * 0.07.
        assert!((total - (4.0 + 5.6)).abs() < 1e-12);
    }

    #[test]
    fn dioxin_join_ignores_source_type() {
        // A dioxin rate has no sourceTypeID column: it applies to whatever
        // source type the activity row carries.
        let mut inputs = single_flow_inputs();
        inputs.metal_emission_rate.clear();
        inputs.source_type_model_year[0].source_type_id = 62;
        inputs.sho[0].source_type_id = 62;
        let out = AirToxicsDistanceCalculator::run(&inputs);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].pollutant_id, 130);
        assert_eq!(out[0].source_type_id, 62);
    }

    #[test]
    fn metal_join_requires_a_matching_source_type() {
        // The metalEmissionRate join keys on sourceTypeID; a mismatch yields
        // no metal output (the dioxin row still flows).
        let mut inputs = single_flow_inputs();
        inputs.metal_emission_rate[0].source_type_id = 99;
        let out = AirToxicsDistanceCalculator::run(&inputs);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].pollutant_id, 130, "only the dioxin row survives");
    }

    #[test]
    fn output_is_deterministically_sorted_regardless_of_input_order() {
        // Reversing the input rows must not change the sorted output.
        let inputs = single_flow_inputs();
        let mut reversed = inputs.clone();
        reversed.dioxin_emission_rate.push(DioxinEmissionRateRow {
            process_id: 1,
            pollutant_id: 145,
            fuel_type_id: 2,
            model_year_id: 2015,
            mean_base_rate: 0.01,
        });
        let mut forward = reversed.clone();
        forward.dioxin_emission_rate.reverse();

        let out_a = AirToxicsDistanceCalculator::run(&reversed);
        let out_b = AirToxicsDistanceCalculator::run(&forward);
        assert_eq!(out_a, out_b);
        // Keys ascend: pollutant 63 (metal), 130, 145 (dioxin).
        let pollutants: Vec<i32> = out_a.iter().map(|r| r.pollutant_id).collect();
        assert_eq!(pollutants, vec![63, 130, 145]);
    }
}

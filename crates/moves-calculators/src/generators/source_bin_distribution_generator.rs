//! `SourceBinDistributionGenerator` — Task 29.
//!
//! Ports `gov/epa/otaq/moves/master/implementation/ghg/SourceBinDistributionGenerator.java`
//! (711 lines Java + embedded SQL). The generator apportions vehicle-population
//! fractions across *source bins* — the `(fuel type, engine technology,
//! regulatory class, model-year group)` buckets that every running-emission
//! calculator keys its rates on. Its `SourceBinDistribution` output is the
//! `sourceBinActivityFraction` weight used to blend per-bin rates into a
//! per-`sourceTypeModelYear` emission.
//!
//! # Algorithm
//!
//! The Java class is one MasterLoop generator with three phases:
//!
//! * **`doFirstTime`** (once per run) — derive the model-year window the run
//!   needs ([`model_year_range`]) and the fuels each source type is equipped
//!   with ([`fuel_types_by_source_type`]).
//! * **`doPollutantProcess`** (once per new `(pollutant, process)`) — for each
//!   not-yet-populated source type, aggregate `sampleVehiclePopulation`
//!   fractions into source bins and emit `SourceBin` + `SourceBinDistribution`
//!   rows. See [`pollutant_process_distribution`].
//! * **`doCountyYear`** (once per new `(process, county, year)`) — re-base the
//!   distribution from *equipped* fuels onto the fuels actually *used* in that
//!   county/year via `fuelUsageFraction`. See [`county_year_distribution`].
//!
//! # Source-bin identity
//!
//! A source bin is identified by a single packed [`i64`], decimal-encoded so
//! the components stay human-readable in the database. [`source_bin_id`] is
//! the pure port of the Java `update SBDGSVP set sourceBinID = …` statement.
//!
//! # Phase 2 / data-plane status
//!
//! `moves-framework`'s [`CalculatorContext`] is still a Phase 2 skeleton — the
//! slow-tier [tables](CalculatorContext::tables) and the
//! [scratch namespace](CalculatorContext::scratch) are placeholder structs
//! until Task 50 lands the `DataFrameStore`. So [`Generator::execute`] here
//! returns an empty [`CalculatorOutput`]: there is no table store to read from
//! or write to yet.
//!
//! The *algorithm* is nonetheless fully ported, as the pure functions in this
//! module ([`pollutant_process_distribution`], [`county_year_distribution`],
//! [`model_year_range`], [`fuel_types_by_source_type`], [`source_bin_id`]).
//! They operate on plain row structs ([`SampleVehiclePopulationRow`] …) and
//! are exhaustively unit-tested. When the data plane lands, wiring `execute`
//! is mechanical: read the rows out of [`CalculatorContext::tables`], call
//! these functions, write the result into [`CalculatorContext::scratch`].
//!
//! The Java instance fields that dedupe work across MasterLoop callbacks
//! (`processesDone`, `countyYearsDone`, `cleanupPriorProcess`) are run
//! orchestration, not part of the distribution math; they belong with the
//! Task 50 `execute` wiring. The `PROJECT`-domain `modelYearPhysics`
//! call-out is `SourceTypePhysics` (Task 37) and is out of scope here.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

use moves_calculator_info::{Granularity, Priority};
use moves_data::EmissionProcess;
use moves_framework::{
    CalculatorContext, CalculatorOutput, CalculatorSubscription, Error, Generator,
};

// ===========================================================================
// Input row types — plain Rust mirrors of the default-DB / RunSpec tables the
// generator reads. Every integer column is an `i64` (it losslessly holds every
// MySQL `int`/`bigint` MOVES uses); every fraction is an `f64` (`double`).
// ===========================================================================

/// One `sampleVehiclePopulation` row: the fraction of a `(sourceType,
/// modelYear)` population in a given `(fuel, engTech, regClass)` cell.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SampleVehiclePopulationRow {
    /// `sourceTypeModelYearID` — packed `sourceTypeID * 10000 + modelYearID`.
    pub source_type_model_year_id: i64,
    /// `modelYearID` — calendar model year.
    pub model_year_id: i64,
    /// `sourceTypeID` — MOVES source (vehicle) type.
    pub source_type_id: i64,
    /// `fuelTypeID`.
    pub fuel_type_id: i64,
    /// `engTechID` — engine-technology id.
    pub eng_tech_id: i64,
    /// `regClassID` — regulatory class id.
    pub reg_class_id: i64,
    /// `stmyFraction` — population fraction for this cell.
    pub stmy_fraction: f64,
}

/// One `PollutantProcessModelYear` row: maps a `(polProcessID, modelYearID)`
/// to its model-year group. Primary key is `(polProcessID, modelYearID)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PollutantProcessModelYearRow {
    /// `polProcessID` — composite `pollutantID * 100 + processID`.
    pub pol_process_id: i64,
    /// `modelYearID`.
    pub model_year_id: i64,
    /// `modelYearGroupID` for this pollutant-process and model year.
    pub model_year_group_id: i64,
}

/// One `ModelYearGroup` row: maps a full model-year group to its short id.
/// Primary key is `modelYearGroupID`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ModelYearGroupRow {
    /// `modelYearGroupID`.
    pub model_year_group_id: i64,
    /// `shortModYrGroupID` — the abbreviated group id packed into source bins.
    pub short_mod_yr_group_id: i64,
}

/// One `SourceTypePolProcess` row: declares, per `(sourceType, polProcess)`,
/// whether the source bin must resolve regulatory class and/or model-year
/// group. Primary key is `(sourceTypeID, polProcessID)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypePolProcessRow {
    /// `sourceTypeID`.
    pub source_type_id: i64,
    /// `polProcessID`.
    pub pol_process_id: i64,
    /// `isRegClassReqd` — when set, source bins keep `regClassID`; otherwise it
    /// collapses to `0` and reg classes merge.
    pub is_reg_class_required: bool,
    /// `isMYGroupReqd` — when set, source bins keep the model-year group;
    /// otherwise `modelYearGroupID` / `shortModYrGroupID` collapse to `0`.
    pub is_my_group_required: bool,
}

/// One `SourceTypeModelYear` row: the `sourceTypeModelYearID` → `sourceTypeID`
/// lookup used to attribute existing `SourceBinDistribution` rows back to a
/// source type. Primary key is `sourceTypeModelYearID`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypeModelYearRow {
    /// `sourceTypeModelYearID`.
    pub source_type_model_year_id: i64,
    /// `sourceTypeID` owning that `sourceTypeModelYearID`.
    pub source_type_id: i64,
}

/// One `RunSpecSourceFuelType` row: a `(sourceType, fuelType)` pair the RunSpec
/// selects. Feeds [`fuel_types_by_source_type`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RunSpecSourceFuelTypeRow {
    /// `sourceTypeID`.
    pub source_type_id: i64,
    /// `fuelTypeID`.
    pub fuel_type_id: i64,
}

/// One `SourceBin` row: the canonical definition of a source bin. The
/// generator both reads existing rows (to skip re-inserting known bins) and
/// appends newly discovered ones.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceBinRow {
    /// `sourceBinID` — see [`source_bin_id`].
    pub source_bin_id: i64,
    /// `engSizeID` — always `0` for the onroad source bins this generator
    /// builds.
    pub eng_size_id: i64,
    /// `fuelTypeID`.
    pub fuel_type_id: i64,
    /// `engTechID`.
    pub eng_tech_id: i64,
    /// `regClassID`.
    pub reg_class_id: i64,
    /// `modelYearGroupID`.
    pub model_year_group_id: i64,
    /// `weightClassID` — always `0` for these onroad source bins.
    pub weight_class_id: i64,
}

/// One `SourceBinDistribution` row: the activity-weighting of a source bin
/// within a `(sourceTypeModelYear, polProcess)`.
///
/// The Java table also carries `sourceBinActivityFractionCV` (always `NULL`
/// from this generator) and `isUserInput` (always `'N'`); both are
/// provenance/cleanup metadata the distribution math never reads, so they are
/// omitted from this port.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceBinDistributionRow {
    /// `sourceTypeModelYearID`.
    pub source_type_model_year_id: i64,
    /// `polProcessID`.
    pub pol_process_id: i64,
    /// `sourceBinID`.
    pub source_bin_id: i64,
    /// `sourceBinActivityFraction` — the activity weight assigned to the bin.
    pub source_bin_activity_fraction: f64,
}

/// One `fuelUsageFraction` row: the fraction of activity equipped for one fuel
/// that is, in a given county/fuel-year, actually fuelled by another.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelUsageFractionRow {
    /// `countyID`.
    pub county_id: i64,
    /// `fuelYearID`.
    pub fuel_year_id: i64,
    /// `modelYearGroupID` — `0` means "applies to every model-year group".
    pub model_year_group_id: i64,
    /// `sourceBinFuelTypeID` — the fuel the vehicle is *equipped* for.
    pub source_bin_fuel_type_id: i64,
    /// `fuelSupplyFuelTypeID` — the fuel actually *supplied*.
    pub fuel_supply_fuel_type_id: i64,
    /// `usageFraction` — share of equipped activity that uses the supply fuel.
    pub usage_fraction: f64,
}

// ===========================================================================
// Result + intermediate types.
// ===========================================================================

/// The model-year window a run needs, as computed by [`model_year_range`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelYearRange {
    /// Earliest model year — `min(calendar years) - max(AgeCategory.ageID)`.
    pub first_model_year_needed: i64,
    /// Latest model year — `max(calendar years)`.
    pub last_model_year_needed: i64,
}

/// Fallback range the Java `doFirstTime` installs if the model-year query
/// fails (`firstModelYearNeeded = 1966`, `lastModelYearNeeded = 2060`).
pub const FALLBACK_MODEL_YEAR_RANGE: ModelYearRange = ModelYearRange {
    first_model_year_needed: 1966,
    last_model_year_needed: 2060,
};

/// What one `doPollutantProcess` pass produces: bins discovered for the first
/// time, and the distribution rows weighting them.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SourceBinDistributionOutput {
    /// `SourceBin` rows whose `sourceBinID` was not already present — to be
    /// appended to the `SourceBin` table.
    pub new_source_bins: Vec<SourceBinRow>,
    /// `SourceBinDistribution` rows to insert.
    pub distribution: Vec<SourceBinDistributionRow>,
}

/// Bundle of table slices [`pollutant_process_distribution`] reads. Grouping
/// them keeps the call readable and mirrors the [`CalculatorContext`] the
/// Task 50 data plane will hand the generator.
#[derive(Debug, Clone, Copy)]
pub struct SourceBinTables<'a> {
    /// `sampleVehiclePopulation`.
    pub sample_vehicle_population: &'a [SampleVehiclePopulationRow],
    /// `PollutantProcessModelYear`.
    pub pollutant_process_model_year: &'a [PollutantProcessModelYearRow],
    /// `ModelYearGroup`.
    pub model_year_group: &'a [ModelYearGroupRow],
    /// `SourceTypePolProcess`.
    pub source_type_pol_process: &'a [SourceTypePolProcessRow],
    /// `SourceTypeModelYear`.
    pub source_type_model_year: &'a [SourceTypeModelYearRow],
    /// `SourceBin` — rows already present before this pass.
    pub source_bin: &'a [SourceBinRow],
    /// `SourceBinDistribution` — rows already present before this pass.
    pub source_bin_distribution: &'a [SourceBinDistributionRow],
}

/// One aggregated `SBDGSVP` row — `sampleVehiclePopulation` fractions summed
/// into a single source bin. Private intermediate of `aggregate_svp`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct SbdgsvpRow {
    source_type_model_year_id: i64,
    fuel_type_id: i64,
    eng_tech_id: i64,
    reg_class_id: i64,
    stmy_fraction: f64,
    model_year_group_id: i64,
    short_mod_yr_group_id: i64,
    source_bin_id: i64,
}

/// One `sourceBinFuelUsage` row — an equipped→used source-bin remapping with
/// its usage weight. Private intermediate of `source_bin_fuel_usage`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct SourceBinFuelUsageRow {
    equipped_source_bin_id: i64,
    used_source_bin_id: i64,
    usage_fraction: f64,
}

// ===========================================================================
// Pure algorithm.
// ===========================================================================

/// Decimal-pack a source bin's components into its `sourceBinID`.
///
/// Pure port of the Java `update SBDGSVP set sourceBinID = …` statement. Each
/// component owns a fixed decimal slot:
///
/// ```text
/// 1_000_000_000_000_000_000   leading marker digit
///   + fuelTypeID         * 10_000_000_000_000_000
///   + engTechID          *    100_000_000_000_000
///   + regClassID         *      1_000_000_000_000
///   + shortModYrGroupID  *         10_000_000_000
/// ```
///
/// The Java statement has two further `0 * …` terms — the `engSizeID` and
/// `weightClassID` slots — which are always `0` for the onroad source bins
/// this generator builds, so they are omitted here.
#[must_use]
pub const fn source_bin_id(
    fuel_type_id: i64,
    eng_tech_id: i64,
    reg_class_id: i64,
    short_mod_yr_group_id: i64,
) -> i64 {
    1_000_000_000_000_000_000
        + fuel_type_id * 10_000_000_000_000_000
        + eng_tech_id * 100_000_000_000_000
        + reg_class_id * 1_000_000_000_000
        + short_mod_yr_group_id * 10_000_000_000
}

/// Compute the model-year window the run needs.
///
/// Pure port of the Java `doFirstTime` model-year arithmetic:
/// `firstModelYearNeeded = min(calendar years) - max(AgeCategory.ageID)` and
/// `lastModelYearNeeded = max(calendar years)`.
///
/// Returns [`None`] when `run_spec_years` is empty (the Java reads `years`
/// from a `TreeSet` that is never empty for a valid RunSpec; callers that hit
/// the empty case should fall back to [`FALLBACK_MODEL_YEAR_RANGE`], matching
/// the Java `catch` block).
#[must_use]
pub fn model_year_range(run_spec_years: &[i64], max_age_id: i64) -> Option<ModelYearRange> {
    let first_calendar_year = run_spec_years.iter().copied().min()?;
    let last_calendar_year = run_spec_years.iter().copied().max()?;
    Some(ModelYearRange {
        first_model_year_needed: first_calendar_year - max_age_id,
        last_model_year_needed: last_calendar_year,
    })
}

/// Group the RunSpec's `(sourceType, fuelType)` selections into the set of
/// fuels each source type is equipped with.
///
/// Pure port of the Java `doFirstTime` `fuelTypesBySourceType` build. The Java
/// keeps a comma-separated string per source type for embedding in a SQL `IN`
/// clause; a [`BTreeSet`] is the order-independent, de-duplicated equivalent.
#[must_use]
pub fn fuel_types_by_source_type(
    rows: &[RunSpecSourceFuelTypeRow],
) -> BTreeMap<i64, BTreeSet<i64>> {
    let mut by_source_type: BTreeMap<i64, BTreeSet<i64>> = BTreeMap::new();
    for row in rows {
        by_source_type
            .entry(row.source_type_id)
            .or_default()
            .insert(row.fuel_type_id);
    }
    by_source_type
}

/// Source types that already hold `SourceBinDistribution` output for
/// `pol_process_id` — the Java `BlockSourceType` table.
///
/// Mirrors `SourceBinDistribution INNER JOIN SourceTypeModelYear USING
/// (sourceTypeModelYearID)`: a distribution row whose `sourceTypeModelYearID`
/// is absent from `SourceTypeModelYear` is dropped by the inner join.
fn block_source_types(
    source_bin_distribution: &[SourceBinDistributionRow],
    source_type_model_year: &[SourceTypeModelYearRow],
    pol_process_id: i64,
) -> BTreeSet<i64> {
    let source_type_of: BTreeMap<i64, i64> = source_type_model_year
        .iter()
        .map(|r| (r.source_type_model_year_id, r.source_type_id))
        .collect();
    source_bin_distribution
        .iter()
        .filter(|r| r.pol_process_id == pol_process_id)
        .filter_map(|r| source_type_of.get(&r.source_type_model_year_id).copied())
        .collect()
}

/// Source types to (re)compute for `pol_process_id`: those declared in
/// `SourceTypePolProcess` and not already blocked.
///
/// Mirrors `SourceTypePolProcess LEFT JOIN BlockSourceType … WHERE
/// ISNULL(BlockSourceType.sourceTypeID)`.
fn source_types_to_use(
    source_type_pol_process: &[SourceTypePolProcessRow],
    pol_process_id: i64,
    blocked: &BTreeSet<i64>,
) -> BTreeSet<i64> {
    source_type_pol_process
        .iter()
        .filter(|r| r.pol_process_id == pol_process_id)
        .map(|r| r.source_type_id)
        .filter(|source_type_id| !blocked.contains(source_type_id))
        .collect()
}

/// Aggregate `sampleVehiclePopulation` fractions into source bins for one
/// `(sourceType, polProcess)` — the Java `SBDGSVP` build plus the
/// `sourceBinID` update.
///
/// Mirrors the `coreSQL` join/filter/group:
///
/// * inner join to `PollutantProcessModelYear` on `(polProcessID,
///   modelYearID)` and to `ModelYearGroup` on `modelYearGroupID` — a row with
///   no match in either is dropped;
/// * keep rows for `source_type_id`, with `modelYearID` inside `year_range`,
///   a `fuelTypeID` in `fuel_type_ids`, and `stmyFraction > 0.0`;
/// * group by `(sourceTypeModelYearID, fuelTypeID, engTechID)`, additionally
///   by `regClassID` when `is_reg_class_required` and by `shortModYrGroupID`
///   when `is_my_group_required`, summing `stmyFraction`.
///
/// When a class/group is not required its id collapses to `0` in both the
/// group key and the output (so, e.g., reg classes merge). `sourceBinID` is
/// then [packed](source_bin_id) from the resolved components; the Java does
/// this as a follow-up `UPDATE`, but it is a pure function of the grouped
/// columns so it is computed in the same pass here.
fn aggregate_svp(
    tables: &SourceBinTables,
    source_type_id: i64,
    pol_process_id: i64,
    year_range: ModelYearRange,
    fuel_type_ids: &BTreeSet<i64>,
    is_reg_class_required: bool,
    is_my_group_required: bool,
) -> Vec<SbdgsvpRow> {
    // PollutantProcessModelYear PK is (polProcessID, modelYearID); ModelYearGroup
    // PK is modelYearGroupID — so each makes a unique lookup map.
    let model_year_group_of: BTreeMap<(i64, i64), i64> = tables
        .pollutant_process_model_year
        .iter()
        .map(|r| ((r.pol_process_id, r.model_year_id), r.model_year_group_id))
        .collect();
    let short_group_of: BTreeMap<i64, i64> = tables
        .model_year_group
        .iter()
        .map(|r| (r.model_year_group_id, r.short_mod_yr_group_id))
        .collect();

    // Group key: (sourceTypeModelYearID, fuelTypeID, engTechID, regClassKey,
    // shortKey). The map value accumulates the summed fraction in a fully
    // populated row.
    let mut groups: BTreeMap<(i64, i64, i64, i64, i64), SbdgsvpRow> = BTreeMap::new();

    for row in tables.sample_vehicle_population {
        let in_scope = row.source_type_id == source_type_id
            && row.model_year_id >= year_range.first_model_year_needed
            && row.model_year_id <= year_range.last_model_year_needed
            && fuel_type_ids.contains(&row.fuel_type_id)
            && row.stmy_fraction > 0.0;
        if !in_scope {
            continue;
        }
        // Inner joins: drop the row if either match is missing.
        let Some(&model_year_group_id) =
            model_year_group_of.get(&(pol_process_id, row.model_year_id))
        else {
            continue;
        };
        let Some(&short_mod_yr_group_id) = short_group_of.get(&model_year_group_id) else {
            continue;
        };

        let out_reg_class = if is_reg_class_required {
            row.reg_class_id
        } else {
            0
        };
        let out_short = if is_my_group_required {
            short_mod_yr_group_id
        } else {
            0
        };
        let out_model_year_group = if is_my_group_required {
            model_year_group_id
        } else {
            0
        };

        let key = (
            row.source_type_model_year_id,
            row.fuel_type_id,
            row.eng_tech_id,
            out_reg_class,
            out_short,
        );
        let entry = groups.entry(key).or_insert_with(|| SbdgsvpRow {
            source_type_model_year_id: row.source_type_model_year_id,
            fuel_type_id: row.fuel_type_id,
            eng_tech_id: row.eng_tech_id,
            reg_class_id: out_reg_class,
            stmy_fraction: 0.0,
            model_year_group_id: out_model_year_group,
            short_mod_yr_group_id: out_short,
            source_bin_id: source_bin_id(
                row.fuel_type_id,
                row.eng_tech_id,
                out_reg_class,
                out_short,
            ),
        });
        entry.stmy_fraction += row.stmy_fraction;
    }

    groups.into_values().collect()
}

/// `SBDGSVP` bins whose `sourceBinID` is not already known — the Java
/// `NewSourceBin2` table. `engSizeID` and `weightClassID` are `0` for onroad
/// source bins.
fn new_source_bins(sbdgsvp: &[SbdgsvpRow], known_bin_ids: &BTreeSet<i64>) -> Vec<SourceBinRow> {
    let mut seen: BTreeSet<i64> = BTreeSet::new();
    let mut new_bins = Vec::new();
    for row in sbdgsvp {
        if known_bin_ids.contains(&row.source_bin_id) || !seen.insert(row.source_bin_id) {
            continue;
        }
        new_bins.push(SourceBinRow {
            source_bin_id: row.source_bin_id,
            eng_size_id: 0,
            fuel_type_id: row.fuel_type_id,
            eng_tech_id: row.eng_tech_id,
            reg_class_id: row.reg_class_id,
            model_year_group_id: row.model_year_group_id,
            weight_class_id: 0,
        });
    }
    new_bins
}

/// Collapse `SBDGSVP` rows into `SourceBinDistribution` rows: sum
/// `stmyFraction` over `(sourceTypeModelYearID, sourceBinID)`.
fn build_source_bin_distribution(
    sbdgsvp: &[SbdgsvpRow],
    pol_process_id: i64,
) -> Vec<SourceBinDistributionRow> {
    let mut by_bin: BTreeMap<(i64, i64), f64> = BTreeMap::new();
    for row in sbdgsvp {
        *by_bin
            .entry((row.source_type_model_year_id, row.source_bin_id))
            .or_insert(0.0) += row.stmy_fraction;
    }
    by_bin
        .into_iter()
        .map(
            |((source_type_model_year_id, bin_id), fraction)| SourceBinDistributionRow {
                source_type_model_year_id,
                pol_process_id,
                source_bin_id: bin_id,
                source_bin_activity_fraction: fraction,
            },
        )
        .collect()
}

/// Build the source-bin distribution for one `(pollutant, process)`.
///
/// Pure port of the Java `doPollutantProcess`. `pol_process_id` is the
/// composite `pollutantID * 100 + processID` (a `moves_data::PolProcessId`
/// value). The Java loops the not-yet-blocked source types in ascending order,
/// rebuilding `SBDGSVP` per source type and growing `SourceBin` as it goes;
/// this port reproduces that incremental growth — a bin first emitted for
/// source type *A* is "known" when source type *B* is processed.
///
/// Distribution rows never collide across source types: `sourceTypeModelYearID`
/// embeds `sourceTypeID`, so each source type owns a disjoint key space.
///
/// A `(pollutant, process)` absent from `SourceTypePolProcess` yields an empty
/// output, matching the Java early return.
#[must_use]
pub fn pollutant_process_distribution(
    pol_process_id: i64,
    tables: &SourceBinTables,
    fuels_by_source_type: &BTreeMap<i64, BTreeSet<i64>>,
    year_range: ModelYearRange,
) -> SourceBinDistributionOutput {
    let mut known_bin_ids: BTreeSet<i64> =
        tables.source_bin.iter().map(|b| b.source_bin_id).collect();

    let blocked = block_source_types(
        tables.source_bin_distribution,
        tables.source_type_model_year,
        pol_process_id,
    );
    let to_use = source_types_to_use(tables.source_type_pol_process, pol_process_id, &blocked);

    // (sourceTypeID, polProcessID) -> (isRegClassReqd, isMYGroupReqd).
    let requirement_flags: BTreeMap<i64, (bool, bool)> = tables
        .source_type_pol_process
        .iter()
        .filter(|r| r.pol_process_id == pol_process_id)
        .map(|r| {
            (
                r.source_type_id,
                (r.is_reg_class_required, r.is_my_group_required),
            )
        })
        .collect();

    let mut output = SourceBinDistributionOutput::default();
    for source_type_id in to_use {
        // The Java skips a source type with no equipped fuels.
        let Some(fuels) = fuels_by_source_type.get(&source_type_id) else {
            continue;
        };
        if fuels.is_empty() {
            continue;
        }
        let (is_reg_class_required, is_my_group_required) = requirement_flags
            .get(&source_type_id)
            .copied()
            .unwrap_or((false, false));

        let sbdgsvp = aggregate_svp(
            tables,
            source_type_id,
            pol_process_id,
            year_range,
            fuels,
            is_reg_class_required,
            is_my_group_required,
        );

        for bin in new_source_bins(&sbdgsvp, &known_bin_ids) {
            known_bin_ids.insert(bin.source_bin_id);
            output.new_source_bins.push(bin);
        }
        output
            .distribution
            .extend(build_source_bin_distribution(&sbdgsvp, pol_process_id));
    }
    output
}

/// Build the equipped→used source-bin remapping for a county/fuel-year — the
/// Java `sourceBinFuelUsage` table.
///
/// For each `fuelUsageFraction` row matching `county_id` and `fuel_year_id`,
/// the *equipped* bin `e` matches on `fuelTypeID = sourceBinFuelTypeID` and
/// (when the fuel-usage row is model-year-group specific) on
/// `modelYearGroupID`; the *used* bin `u` matches on `fuelTypeID =
/// fuelSupplyFuelTypeID` and shares every other component with `e`.
fn source_bin_fuel_usage(
    fuel_usage_fractions: &[FuelUsageFractionRow],
    source_bins: &[SourceBinRow],
    county_id: i64,
    fuel_year_id: i64,
) -> Vec<SourceBinFuelUsageRow> {
    let mut usage = Vec::new();
    for f in fuel_usage_fractions {
        if f.county_id != county_id || f.fuel_year_id != fuel_year_id {
            continue;
        }
        for e in source_bins {
            let equipped_matches = e.fuel_type_id == f.source_bin_fuel_type_id
                && (f.model_year_group_id == 0 || e.model_year_group_id == f.model_year_group_id);
            if !equipped_matches {
                continue;
            }
            for u in source_bins {
                let used_matches = u.fuel_type_id == f.fuel_supply_fuel_type_id
                    && u.eng_tech_id == e.eng_tech_id
                    && u.reg_class_id == e.reg_class_id
                    && u.model_year_group_id == e.model_year_group_id
                    && u.eng_size_id == e.eng_size_id
                    && u.weight_class_id == e.weight_class_id;
                if !used_matches {
                    continue;
                }
                usage.push(SourceBinFuelUsageRow {
                    equipped_source_bin_id: e.source_bin_id,
                    used_source_bin_id: u.source_bin_id,
                    usage_fraction: f.usage_fraction,
                });
            }
        }
    }
    usage
}

/// Re-base a source-bin distribution from equipped fuels onto the fuels
/// actually used in one `(process, county, year)`.
///
/// Pure port of the Java `doCountyYear` (the `USE_FUELUSAGEFRACTION` branch —
/// `CompilationFlags.USE_FUELUSAGEFRACTION` is `true`). It builds the
/// equipped→used remapping with `source_bin_fuel_usage`, then converts each
/// in-scope distribution row:
/// `sourceBinActivityFraction[usedSourceBinID] = sum(usageFraction *
/// sourceBinActivityFraction)`, grouped by `(sourceTypeModelYearID,
/// polProcessID, usedSourceBinID)`.
///
/// `process_pol_process_ids` is the set of `polProcessID`s the target process
/// owns in `PollutantProcessAssoc` — the Java `INNER JOIN
/// pollutantProcessAssoc … ON ppa.processID = processID` filter, which also
/// carries the process identity (no separate `process_id` argument is
/// needed). `fuel_year_id` is the `Year.fuelYearID` for the calendar year (the
/// Java resolves it via a sub-select). The result rows populate the caller's
/// run-named `sourceBinDistributionFuelUsage_<process>_<county>_<year>` table.
#[must_use]
pub fn county_year_distribution(
    distribution: &[SourceBinDistributionRow],
    source_bins: &[SourceBinRow],
    fuel_usage_fractions: &[FuelUsageFractionRow],
    process_pol_process_ids: &BTreeSet<i64>,
    county_id: i64,
    fuel_year_id: i64,
) -> Vec<SourceBinDistributionRow> {
    let usage = source_bin_fuel_usage(fuel_usage_fractions, source_bins, county_id, fuel_year_id);

    // Index the remapping by equipped bin: equipped -> [(used, usageFraction)].
    let mut used_by_equipped: BTreeMap<i64, Vec<(i64, f64)>> = BTreeMap::new();
    for u in &usage {
        used_by_equipped
            .entry(u.equipped_source_bin_id)
            .or_default()
            .push((u.used_source_bin_id, u.usage_fraction));
    }

    let mut by_used: BTreeMap<(i64, i64, i64), f64> = BTreeMap::new();
    for d in distribution {
        if !process_pol_process_ids.contains(&d.pol_process_id) {
            continue;
        }
        let Some(remaps) = used_by_equipped.get(&d.source_bin_id) else {
            continue;
        };
        for &(used_bin_id, usage_fraction) in remaps {
            *by_used
                .entry((d.source_type_model_year_id, d.pol_process_id, used_bin_id))
                .or_insert(0.0) += usage_fraction * d.source_bin_activity_fraction;
        }
    }

    by_used
        .into_iter()
        .map(
            |((source_type_model_year_id, pol_process_id, used_bin_id), fraction)| {
                SourceBinDistributionRow {
                    source_type_model_year_id,
                    pol_process_id,
                    source_bin_id: used_bin_id,
                    source_bin_activity_fraction: fraction,
                }
            },
        )
        .collect()
}

// ===========================================================================
// Generator.
// ===========================================================================

/// The `SourceBinDistributionGenerator` MasterLoop generator.
///
/// A zero-sized value type — like every `moves-framework` generator it owns no
/// per-run state (the Task 50 `execute` wiring carries the `processesDone` /
/// `countyYearsDone` dedupe state). See the [module documentation](self) for
/// the algorithm and the data-plane status.
#[derive(Debug, Default, Clone, Copy)]
pub struct SourceBinDistributionGenerator;

impl SourceBinDistributionGenerator {
    /// Stable module name — matches the `SourceBinDistributionGenerator` entry
    /// in the Phase 1 calculator-chain DAG.
    pub const NAME: &'static str = "SourceBinDistributionGenerator";
}

/// Process names the Java `subscribeToMe` signs up for, in source order.
///
/// `EmissionProcess::find_by_name` resolves nine of these; `"Evap Non-Fuel
/// Vapors"` has no entry in the MOVES process table, so it is dropped — the
/// exact behaviour of the Java `if (process != null)` guard. The resulting
/// nine subscriptions match `SourceBinDistributionGenerator` in
/// `characterization/calculator-chains/calculator-dag.json`.
const SUBSCRIBED_PROCESS_NAMES: [&str; 10] = [
    "Running Exhaust",
    "Start Exhaust",
    "Extended Idle Exhaust",
    "Auxiliary Power Exhaust",
    "Evap Permeation",
    "Evap Fuel Vapor Venting",
    "Evap Fuel Leaks",
    "Evap Non-Fuel Vapors",
    "Brakewear",
    "Tirewear",
];

/// Default-DB / RunSpec tables the generator reads.
static INPUT_TABLES: &[&str] = &[
    "sampleVehiclePopulation",
    "PollutantProcessModelYear",
    "ModelYearGroup",
    "SourceTypePolProcess",
    "PollutantProcessAssoc",
    "RunSpecSourceFuelType",
    "AgeCategory",
    "SourceTypeModelYear",
    "SourceBin",
    "SourceBinDistribution",
    "fuelUsageFraction",
    "Year",
];

/// Scratch tables the generator writes. `SourceBin` and `SourceBinDistribution`
/// are read-modify-write. The per-county/year
/// `sourceBinDistributionFuelUsage_<process>_<county>_<year>` tables carry
/// run-derived names and so cannot appear in this static list.
static OUTPUT_TABLES: &[&str] = &["SourceBin", "SourceBinDistribution"];

/// Resolve [`SUBSCRIBED_PROCESS_NAMES`] into the generator's subscription set:
/// every resolvable process, at `YEAR` granularity, priority `GENERATOR+1`.
///
/// The `GENERATOR+1` priority places the generator just ahead of the
/// operating-mode generators that consume `SourceBinDistribution`.
fn build_subscriptions() -> Vec<CalculatorSubscription> {
    let priority = Priority::parse("GENERATOR+1")
        .expect("\"GENERATOR+1\" is a well-formed MasterLoopPriority");
    SUBSCRIBED_PROCESS_NAMES
        .iter()
        .copied()
        .filter_map(EmissionProcess::find_by_name)
        .map(|process| CalculatorSubscription::new(process.id, Granularity::Year, priority))
        .collect()
}

/// Construct the generator as a boxed trait object — matches the
/// `moves_framework::GeneratorFactory` signature (`fn() -> Box<dyn Generator>`)
/// so engine wiring can register it with the calculator registry.
#[must_use]
pub fn factory() -> Box<dyn Generator> {
    Box::new(SourceBinDistributionGenerator)
}

impl Generator for SourceBinDistributionGenerator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        static SUBSCRIPTIONS: OnceLock<Vec<CalculatorSubscription>> = OnceLock::new();
        SUBSCRIPTIONS.get_or_init(build_subscriptions).as_slice()
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn output_tables(&self) -> &[&'static str] {
        OUTPUT_TABLES
    }

    /// Phase 2 skeleton: returns an empty [`CalculatorOutput`].
    ///
    /// [`CalculatorContext`] cannot yet surface the `sampleVehiclePopulation`
    /// inputs or accept the `SourceBinDistribution` output — its table store
    /// lands with the Task 50 `DataFrameStore`. The distribution math itself
    /// is ported and tested in this module's pure functions; see the
    /// [module documentation](self).
    fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        Ok(CalculatorOutput::empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// f64 comparison tolerance for the activity-fraction sums under test.
    const EPS: f64 = 1e-9;

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() < EPS
    }

    // -- compact row constructors -------------------------------------------

    fn svp(
        stmyid: i64,
        model_year: i64,
        source_type: i64,
        fuel: i64,
        eng_tech: i64,
        reg_class: i64,
        fraction: f64,
    ) -> SampleVehiclePopulationRow {
        SampleVehiclePopulationRow {
            source_type_model_year_id: stmyid,
            model_year_id: model_year,
            source_type_id: source_type,
            fuel_type_id: fuel,
            eng_tech_id: eng_tech,
            reg_class_id: reg_class,
            stmy_fraction: fraction,
        }
    }

    fn ppmy(pol_process: i64, model_year: i64, group: i64) -> PollutantProcessModelYearRow {
        PollutantProcessModelYearRow {
            pol_process_id: pol_process,
            model_year_id: model_year,
            model_year_group_id: group,
        }
    }

    fn myg(group: i64, short: i64) -> ModelYearGroupRow {
        ModelYearGroupRow {
            model_year_group_id: group,
            short_mod_yr_group_id: short,
        }
    }

    fn stpp(
        source_type: i64,
        pol_process: i64,
        is_reg_class: bool,
        is_my_group: bool,
    ) -> SourceTypePolProcessRow {
        SourceTypePolProcessRow {
            source_type_id: source_type,
            pol_process_id: pol_process,
            is_reg_class_required: is_reg_class,
            is_my_group_required: is_my_group,
        }
    }

    fn bin(sbid: i64, fuel: i64, eng_tech: i64, reg_class: i64, group: i64) -> SourceBinRow {
        SourceBinRow {
            source_bin_id: sbid,
            eng_size_id: 0,
            fuel_type_id: fuel,
            eng_tech_id: eng_tech,
            reg_class_id: reg_class,
            model_year_group_id: group,
            weight_class_id: 0,
        }
    }

    fn sbd(stmyid: i64, pol_process: i64, sbid: i64, fraction: f64) -> SourceBinDistributionRow {
        SourceBinDistributionRow {
            source_type_model_year_id: stmyid,
            pol_process_id: pol_process,
            source_bin_id: sbid,
            source_bin_activity_fraction: fraction,
        }
    }

    fn empty_tables<'a>() -> SourceBinTables<'a> {
        SourceBinTables {
            sample_vehicle_population: &[],
            pollutant_process_model_year: &[],
            model_year_group: &[],
            source_type_pol_process: &[],
            source_type_model_year: &[],
            source_bin: &[],
            source_bin_distribution: &[],
        }
    }

    // -- source_bin_id ------------------------------------------------------

    #[test]
    fn source_bin_id_marker_digit_only_when_all_components_zero() {
        assert_eq!(source_bin_id(0, 0, 0, 0), 1_000_000_000_000_000_000);
    }

    #[test]
    fn source_bin_id_each_component_owns_its_decimal_slot() {
        assert_eq!(source_bin_id(1, 0, 0, 0), 1_010_000_000_000_000_000);
        assert_eq!(source_bin_id(0, 1, 0, 0), 1_000_100_000_000_000_000);
        assert_eq!(source_bin_id(0, 0, 1, 0), 1_000_001_000_000_000_000);
        assert_eq!(source_bin_id(0, 0, 0, 1), 1_000_000_010_000_000_000);
    }

    #[test]
    fn source_bin_id_combines_components_additively() {
        // Diesel (2), engTech 1, regClass 47, no short group.
        assert_eq!(source_bin_id(2, 1, 47, 0), 1_020_147_000_000_000_000);
        // All four slots populated.
        assert_eq!(source_bin_id(2, 1, 20, 3), 1_020_120_030_000_000_000);
    }

    #[test]
    fn source_bin_id_stays_well_within_i64() {
        // Even an out-of-band fuel id keeps the packed value positive.
        assert!(source_bin_id(90, 9, 99, 99) > 0);
        assert!(source_bin_id(90, 9, 99, 99) < i64::MAX);
    }

    // -- model_year_range ---------------------------------------------------

    #[test]
    fn model_year_range_spans_min_minus_age_to_max() {
        let range = model_year_range(&[2020, 2018, 2022], 30).unwrap();
        assert_eq!(range.first_model_year_needed, 2018 - 30);
        assert_eq!(range.last_model_year_needed, 2022);
    }

    #[test]
    fn model_year_range_single_year() {
        let range = model_year_range(&[2020], 40).unwrap();
        assert_eq!(range.first_model_year_needed, 1980);
        assert_eq!(range.last_model_year_needed, 2020);
    }

    #[test]
    fn model_year_range_none_when_no_years() {
        assert!(model_year_range(&[], 30).is_none());
    }

    #[test]
    fn fallback_model_year_range_matches_java_catch_block() {
        assert_eq!(FALLBACK_MODEL_YEAR_RANGE.first_model_year_needed, 1966);
        assert_eq!(FALLBACK_MODEL_YEAR_RANGE.last_model_year_needed, 2060);
    }

    // -- fuel_types_by_source_type -----------------------------------------

    #[test]
    fn fuel_types_grouped_and_deduplicated_per_source_type() {
        let rows = [
            RunSpecSourceFuelTypeRow {
                source_type_id: 21,
                fuel_type_id: 1,
            },
            RunSpecSourceFuelTypeRow {
                source_type_id: 21,
                fuel_type_id: 2,
            },
            RunSpecSourceFuelTypeRow {
                source_type_id: 21,
                fuel_type_id: 1,
            },
            RunSpecSourceFuelTypeRow {
                source_type_id: 31,
                fuel_type_id: 2,
            },
        ];
        let map = fuel_types_by_source_type(&rows);
        assert_eq!(map.len(), 2);
        assert_eq!(map[&21], BTreeSet::from([1, 2]));
        assert_eq!(map[&31], BTreeSet::from([2]));
    }

    // -- block_source_types / source_types_to_use --------------------------

    #[test]
    fn block_source_types_collects_source_types_with_existing_distribution() {
        let distribution = [sbd(210020, 101, 7, 0.5), sbd(310020, 202, 7, 0.5)];
        let stmy = [
            SourceTypeModelYearRow {
                source_type_model_year_id: 210020,
                source_type_id: 21,
            },
            SourceTypeModelYearRow {
                source_type_model_year_id: 310020,
                source_type_id: 31,
            },
        ];
        let blocked = block_source_types(&distribution, &stmy, 101);
        assert_eq!(blocked, BTreeSet::from([21]));
    }

    #[test]
    fn block_source_types_inner_join_drops_unmapped_rows() {
        // The distribution row's sourceTypeModelYearID is absent from
        // SourceTypeModelYear — the inner join drops it, so nothing is blocked.
        let distribution = [sbd(999999, 101, 7, 0.5)];
        let stmy = [SourceTypeModelYearRow {
            source_type_model_year_id: 210020,
            source_type_id: 21,
        }];
        assert!(block_source_types(&distribution, &stmy, 101).is_empty());
    }

    #[test]
    fn source_types_to_use_excludes_blocked() {
        let stpp_rows = [
            stpp(21, 101, false, false),
            stpp(31, 101, false, false),
            stpp(41, 202, false, false),
        ];
        let blocked = BTreeSet::from([31]);
        let to_use = source_types_to_use(&stpp_rows, 101, &blocked);
        assert_eq!(to_use, BTreeSet::from([21]));
    }

    // -- aggregate_svp ------------------------------------------------------

    #[test]
    fn aggregate_svp_merges_reg_classes_when_not_required() {
        let svp_rows = [
            svp(210020, 2020, 21, 1, 1, 10, 0.6),
            svp(210020, 2020, 21, 1, 1, 20, 0.4),
        ];
        let ppmy_rows = [ppmy(101, 2020, 5)];
        let myg_rows = [myg(5, 50)];
        let tables = SourceBinTables {
            sample_vehicle_population: &svp_rows,
            pollutant_process_model_year: &ppmy_rows,
            model_year_group: &myg_rows,
            ..empty_tables()
        };
        let range = ModelYearRange {
            first_model_year_needed: 2000,
            last_model_year_needed: 2025,
        };
        let out = aggregate_svp(&tables, 21, 101, range, &BTreeSet::from([1]), false, false);

        assert_eq!(out.len(), 1, "reg classes 10 and 20 collapse into one bin");
        assert_eq!(out[0].reg_class_id, 0);
        assert_eq!(out[0].model_year_group_id, 0);
        assert_eq!(out[0].short_mod_yr_group_id, 0);
        assert!(close(out[0].stmy_fraction, 1.0));
        assert_eq!(out[0].source_bin_id, source_bin_id(1, 1, 0, 0));
    }

    #[test]
    fn aggregate_svp_keeps_reg_classes_when_required() {
        let svp_rows = [
            svp(210020, 2020, 21, 1, 1, 10, 0.6),
            svp(210020, 2020, 21, 1, 1, 20, 0.4),
        ];
        let ppmy_rows = [ppmy(101, 2020, 5)];
        let myg_rows = [myg(5, 50)];
        let tables = SourceBinTables {
            sample_vehicle_population: &svp_rows,
            pollutant_process_model_year: &ppmy_rows,
            model_year_group: &myg_rows,
            ..empty_tables()
        };
        let range = ModelYearRange {
            first_model_year_needed: 2000,
            last_model_year_needed: 2025,
        };
        let out = aggregate_svp(&tables, 21, 101, range, &BTreeSet::from([1]), true, false);

        assert_eq!(out.len(), 2, "reg classes stay separate");
        let reg_classes: BTreeSet<i64> = out.iter().map(|r| r.reg_class_id).collect();
        assert_eq!(reg_classes, BTreeSet::from([10, 20]));
    }

    #[test]
    fn aggregate_svp_populates_model_year_group_when_required() {
        let svp_rows = [svp(210020, 2020, 21, 1, 1, 10, 0.7)];
        let ppmy_rows = [ppmy(101, 2020, 5)];
        let myg_rows = [myg(5, 50)];
        let tables = SourceBinTables {
            sample_vehicle_population: &svp_rows,
            pollutant_process_model_year: &ppmy_rows,
            model_year_group: &myg_rows,
            ..empty_tables()
        };
        let range = ModelYearRange {
            first_model_year_needed: 2000,
            last_model_year_needed: 2025,
        };
        let out = aggregate_svp(&tables, 21, 101, range, &BTreeSet::from([1]), false, true);

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].model_year_group_id, 5);
        assert_eq!(out[0].short_mod_yr_group_id, 50);
        assert_eq!(out[0].source_bin_id, source_bin_id(1, 1, 0, 50));
    }

    #[test]
    fn aggregate_svp_filters_by_scope() {
        let svp_rows = [
            svp(210020, 2020, 21, 1, 1, 10, 1.0), // kept
            svp(210099, 1999, 21, 1, 1, 10, 1.0), // model year below range
            svp(210021, 2099, 21, 1, 1, 10, 1.0), // model year above range
            svp(310020, 2020, 31, 1, 1, 10, 1.0), // wrong source type
            svp(210020, 2020, 21, 9, 1, 10, 1.0), // fuel not selected
            svp(210020, 2020, 21, 1, 1, 10, 0.0), // non-positive fraction
        ];
        let ppmy_rows = [ppmy(101, 2020, 5), ppmy(101, 1999, 5), ppmy(101, 2099, 5)];
        let myg_rows = [myg(5, 50)];
        let tables = SourceBinTables {
            sample_vehicle_population: &svp_rows,
            pollutant_process_model_year: &ppmy_rows,
            model_year_group: &myg_rows,
            ..empty_tables()
        };
        let range = ModelYearRange {
            first_model_year_needed: 2000,
            last_model_year_needed: 2025,
        };
        let out = aggregate_svp(&tables, 21, 101, range, &BTreeSet::from([1]), false, false);
        assert_eq!(out.len(), 1);
        assert!(close(out[0].stmy_fraction, 1.0));
    }

    #[test]
    fn aggregate_svp_inner_joins_drop_unmatched_rows() {
        let svp_rows = [
            svp(210020, 2020, 21, 1, 1, 10, 1.0), // no ppmy for model year 2020
            svp(210021, 2021, 21, 1, 1, 10, 1.0), // ppmy present, no myg row
        ];
        let ppmy_rows = [ppmy(101, 2021, 5)];
        let myg_rows: [ModelYearGroupRow; 0] = [];
        let tables = SourceBinTables {
            sample_vehicle_population: &svp_rows,
            pollutant_process_model_year: &ppmy_rows,
            model_year_group: &myg_rows,
            ..empty_tables()
        };
        let range = ModelYearRange {
            first_model_year_needed: 2000,
            last_model_year_needed: 2025,
        };
        let out = aggregate_svp(&tables, 21, 101, range, &BTreeSet::from([1]), false, false);
        assert!(out.is_empty(), "both rows fail an inner join");
    }

    // -- new_source_bins / build_source_bin_distribution -------------------

    #[test]
    fn pollutant_process_distribution_basic_two_source_types() {
        let svp_rows = [
            svp(210020, 2020, 21, 1, 1, 10, 0.7),
            svp(210020, 2020, 21, 1, 1, 20, 0.3),
            svp(310020, 2020, 31, 1, 1, 10, 1.0),
        ];
        let ppmy_rows = [ppmy(101, 2020, 5)];
        let myg_rows = [myg(5, 50)];
        let stpp_rows = [stpp(21, 101, false, false), stpp(31, 101, false, false)];
        let tables = SourceBinTables {
            sample_vehicle_population: &svp_rows,
            pollutant_process_model_year: &ppmy_rows,
            model_year_group: &myg_rows,
            source_type_pol_process: &stpp_rows,
            ..empty_tables()
        };
        let fuels = BTreeMap::from([(21, BTreeSet::from([1])), (31, BTreeSet::from([1]))]);
        let range = ModelYearRange {
            first_model_year_needed: 2000,
            last_model_year_needed: 2025,
        };
        let out = pollutant_process_distribution(101, &tables, &fuels, range);

        // Both source types resolve to the same bin (fuel 1, engTech 1, no
        // class/group) — it is emitted to SourceBin exactly once.
        let bin_id = source_bin_id(1, 1, 0, 0);
        assert_eq!(out.new_source_bins.len(), 1);
        assert_eq!(out.new_source_bins[0].source_bin_id, bin_id);

        // One distribution row per source type, each summing to 1.0.
        assert_eq!(out.distribution.len(), 2);
        for row in &out.distribution {
            assert_eq!(row.source_bin_id, bin_id);
            assert_eq!(row.pol_process_id, 101);
            assert!(close(row.source_bin_activity_fraction, 1.0));
        }
        let stmyids: BTreeSet<i64> = out
            .distribution
            .iter()
            .map(|r| r.source_type_model_year_id)
            .collect();
        assert_eq!(stmyids, BTreeSet::from([210020, 310020]));
    }

    #[test]
    fn pollutant_process_distribution_skips_blocked_source_type() {
        let svp_rows = [
            svp(210020, 2020, 21, 1, 1, 10, 1.0),
            svp(310020, 2020, 31, 1, 1, 10, 1.0),
        ];
        let ppmy_rows = [ppmy(101, 2020, 5)];
        let myg_rows = [myg(5, 50)];
        let stpp_rows = [stpp(21, 101, false, false), stpp(31, 101, false, false)];
        // Source type 21 already has a distribution row for polProcess 101.
        let existing_dist = [sbd(210020, 101, 42, 1.0)];
        let stmy_rows = [SourceTypeModelYearRow {
            source_type_model_year_id: 210020,
            source_type_id: 21,
        }];
        let tables = SourceBinTables {
            sample_vehicle_population: &svp_rows,
            pollutant_process_model_year: &ppmy_rows,
            model_year_group: &myg_rows,
            source_type_pol_process: &stpp_rows,
            source_type_model_year: &stmy_rows,
            source_bin_distribution: &existing_dist,
            ..empty_tables()
        };
        let fuels = BTreeMap::from([(21, BTreeSet::from([1])), (31, BTreeSet::from([1]))]);
        let range = ModelYearRange {
            first_model_year_needed: 2000,
            last_model_year_needed: 2025,
        };
        let out = pollutant_process_distribution(101, &tables, &fuels, range);

        // Only source type 31 is (re)computed.
        assert_eq!(out.distribution.len(), 1);
        assert_eq!(out.distribution[0].source_type_model_year_id, 310020);
    }

    #[test]
    fn pollutant_process_distribution_respects_existing_source_bins() {
        let svp_rows = [svp(210020, 2020, 21, 1, 1, 10, 1.0)];
        let ppmy_rows = [ppmy(101, 2020, 5)];
        let myg_rows = [myg(5, 50)];
        let stpp_rows = [stpp(21, 101, false, false)];
        let bin_id = source_bin_id(1, 1, 0, 0);
        let existing_bins = [bin(bin_id, 1, 1, 0, 0)];
        let tables = SourceBinTables {
            sample_vehicle_population: &svp_rows,
            pollutant_process_model_year: &ppmy_rows,
            model_year_group: &myg_rows,
            source_type_pol_process: &stpp_rows,
            source_bin: &existing_bins,
            ..empty_tables()
        };
        let fuels = BTreeMap::from([(21, BTreeSet::from([1]))]);
        let range = ModelYearRange {
            first_model_year_needed: 2000,
            last_model_year_needed: 2025,
        };
        let out = pollutant_process_distribution(101, &tables, &fuels, range);

        assert!(out.new_source_bins.is_empty(), "bin already known");
        assert_eq!(out.distribution.len(), 1);
    }

    #[test]
    fn pollutant_process_distribution_skips_source_type_without_fuels() {
        let svp_rows = [svp(210020, 2020, 21, 1, 1, 10, 1.0)];
        let ppmy_rows = [ppmy(101, 2020, 5)];
        let myg_rows = [myg(5, 50)];
        let stpp_rows = [stpp(21, 101, false, false)];
        let tables = SourceBinTables {
            sample_vehicle_population: &svp_rows,
            pollutant_process_model_year: &ppmy_rows,
            model_year_group: &myg_rows,
            source_type_pol_process: &stpp_rows,
            ..empty_tables()
        };
        // No fuels mapped for source type 21.
        let fuels = BTreeMap::new();
        let range = ModelYearRange {
            first_model_year_needed: 2000,
            last_model_year_needed: 2025,
        };
        let out = pollutant_process_distribution(101, &tables, &fuels, range);
        assert!(out.distribution.is_empty());
        assert!(out.new_source_bins.is_empty());
    }

    #[test]
    fn pollutant_process_distribution_empty_when_pol_process_unknown() {
        let tables = empty_tables();
        let fuels = BTreeMap::new();
        let range = ModelYearRange {
            first_model_year_needed: 2000,
            last_model_year_needed: 2025,
        };
        let out = pollutant_process_distribution(9999, &tables, &fuels, range);
        assert_eq!(out, SourceBinDistributionOutput::default());
    }

    // -- county_year_distribution ------------------------------------------

    #[test]
    fn county_year_distribution_remaps_equipped_fuel_to_used_fuel() {
        let equipped = source_bin_id(1, 1, 10, 0);
        let used = source_bin_id(2, 1, 10, 0);
        let distribution = [sbd(210020, 101, equipped, 1.0)];
        let source_bins = [bin(equipped, 1, 1, 10, 0), bin(used, 2, 1, 10, 0)];
        let fuel_usage = [FuelUsageFractionRow {
            county_id: 5,
            fuel_year_id: 2020,
            model_year_group_id: 0,
            source_bin_fuel_type_id: 1,
            fuel_supply_fuel_type_id: 2,
            usage_fraction: 0.8,
        }];
        let valid = BTreeSet::from([101]);
        let out =
            county_year_distribution(&distribution, &source_bins, &fuel_usage, &valid, 5, 2020);

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].source_bin_id, used);
        assert_eq!(out[0].pol_process_id, 101);
        assert_eq!(out[0].source_type_model_year_id, 210020);
        assert!(close(out[0].source_bin_activity_fraction, 0.8));
    }

    #[test]
    fn county_year_distribution_filters_by_county_and_pol_process() {
        let equipped = source_bin_id(1, 1, 10, 0);
        let used = source_bin_id(2, 1, 10, 0);
        let distribution = [
            sbd(210020, 101, equipped, 1.0),
            sbd(210020, 202, equipped, 1.0), // polProcess not associated with the process
        ];
        let source_bins = [bin(equipped, 1, 1, 10, 0), bin(used, 2, 1, 10, 0)];
        let fuel_usage = [
            FuelUsageFractionRow {
                county_id: 5,
                fuel_year_id: 2020,
                model_year_group_id: 0,
                source_bin_fuel_type_id: 1,
                fuel_supply_fuel_type_id: 2,
                usage_fraction: 0.8,
            },
            FuelUsageFractionRow {
                county_id: 7, // different county
                fuel_year_id: 2020,
                model_year_group_id: 0,
                source_bin_fuel_type_id: 1,
                fuel_supply_fuel_type_id: 2,
                usage_fraction: 1.0,
            },
        ];
        let valid = BTreeSet::from([101]);
        let out =
            county_year_distribution(&distribution, &source_bins, &fuel_usage, &valid, 5, 2020);

        // Only the polProcess-101 row, only the county-5 fuel-usage row.
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].pol_process_id, 101);
        assert!(close(out[0].source_bin_activity_fraction, 0.8));
    }

    #[test]
    fn county_year_distribution_sums_multiple_supply_fuels() {
        let equipped = source_bin_id(1, 1, 10, 0);
        let used_diesel = source_bin_id(2, 1, 10, 0);
        let used_e85 = source_bin_id(5, 1, 10, 0);
        let distribution = [sbd(210020, 101, equipped, 2.0)];
        let source_bins = [
            bin(equipped, 1, 1, 10, 0),
            bin(used_diesel, 2, 1, 10, 0),
            bin(used_e85, 5, 1, 10, 0),
        ];
        let fuel_usage = [
            FuelUsageFractionRow {
                county_id: 5,
                fuel_year_id: 2020,
                model_year_group_id: 0,
                source_bin_fuel_type_id: 1,
                fuel_supply_fuel_type_id: 2,
                usage_fraction: 0.75,
            },
            FuelUsageFractionRow {
                county_id: 5,
                fuel_year_id: 2020,
                model_year_group_id: 0,
                source_bin_fuel_type_id: 1,
                fuel_supply_fuel_type_id: 5,
                usage_fraction: 0.25,
            },
        ];
        let valid = BTreeSet::from([101]);
        let mut out =
            county_year_distribution(&distribution, &source_bins, &fuel_usage, &valid, 5, 2020);
        out.sort_by_key(|r| r.source_bin_id);

        assert_eq!(out.len(), 2);
        assert_eq!(out[0].source_bin_id, used_diesel);
        assert!(close(out[0].source_bin_activity_fraction, 1.5)); // 0.75 * 2.0
        assert_eq!(out[1].source_bin_id, used_e85);
        assert!(close(out[1].source_bin_activity_fraction, 0.5)); // 0.25 * 2.0
    }

    // -- Generator trait ----------------------------------------------------

    #[test]
    fn generator_name_matches_dag_module() {
        let generator = SourceBinDistributionGenerator;
        assert_eq!(generator.name(), "SourceBinDistributionGenerator");
        assert_eq!(generator.name(), SourceBinDistributionGenerator::NAME);
    }

    #[test]
    fn subscribed_process_names_resolve_to_nine_processes() {
        // "Evap Non-Fuel Vapors" has no MOVES process; the rest resolve.
        let resolved: Vec<&str> = SUBSCRIBED_PROCESS_NAMES
            .iter()
            .copied()
            .filter(|name| EmissionProcess::find_by_name(name).is_some())
            .collect();
        assert_eq!(resolved.len(), 9);
        assert!(EmissionProcess::find_by_name("Evap Non-Fuel Vapors").is_none());
    }

    #[test]
    fn generator_subscriptions_match_phase1_dag() {
        let generator = SourceBinDistributionGenerator;
        let subs = generator.subscriptions();
        assert_eq!(subs.len(), 9);

        let priority = Priority::parse("GENERATOR+1").unwrap();
        assert_eq!(priority.value(), 101);
        for sub in subs {
            assert_eq!(sub.granularity, Granularity::Year);
            assert_eq!(sub.priority, priority);
        }

        // Process ids per characterization/calculator-chains/calculator-dag.json.
        let process_ids: BTreeSet<u16> = subs.iter().map(|s| s.process_id.0).collect();
        assert_eq!(
            process_ids,
            BTreeSet::from([1, 2, 9, 10, 11, 12, 13, 90, 91])
        );
    }

    #[test]
    fn generator_subscriptions_are_cached_across_calls() {
        let generator = SourceBinDistributionGenerator;
        assert_eq!(generator.subscriptions(), generator.subscriptions());
    }

    #[test]
    fn generator_declares_input_and_output_tables() {
        let generator = SourceBinDistributionGenerator;
        assert!(generator
            .input_tables()
            .contains(&"sampleVehiclePopulation"));
        assert!(generator.input_tables().contains(&"SourceBinDistribution"));
        assert_eq!(
            generator.output_tables(),
            &["SourceBin", "SourceBinDistribution"]
        );
    }

    #[test]
    fn generator_execute_is_empty_pending_data_plane() {
        let generator = SourceBinDistributionGenerator;
        let ctx = CalculatorContext::new();
        let result = generator.execute(&ctx);
        assert!(result.is_ok());
    }

    #[test]
    fn factory_produces_a_usable_generator() {
        let generator = factory();
        assert_eq!(generator.name(), SourceBinDistributionGenerator::NAME);
        assert_eq!(generator.subscriptions().len(), 9);
    }
}

//! Port of `MesoscaleLookupTotalActivityGenerator.java`
//! (`gov.epa.otaq.moves.master.implementation.ghg`) — the generator that
//! builds the Total-Activity (`SHO` / `SourceHours`) records for
//! Mesoscale-Lookup runs.
//!
//! Migration plan: Phase 3, Task 35 (paired with [`super::op_mode_distribution`]).
//!
//! # What this generator produces
//!
//! The generator computes the **activity basis** — how vehicle activity is
//! distributed across source type, age, road type and time — for one
//! analysis year. The Java `subscribeToMe` subscribes (subject to a
//! per-process RunSpec gate) to Running Exhaust, the evaporative
//! processes, Brakewear and Tirewear, all at `YEAR` granularity /
//! `GENERATOR` priority.
//!
//! # The algorithm — TAG steps 0–9
//!
//! Each new analysis year drives a fixed pipeline (`Tag-0` … `Tag-9`).
//! The *numerically meaningful* core — ported here as tested free
//! functions — is:
//!
//! * **`Tag-0`** find the base year ([`determine_base_year`]);
//! * **`Tag-1`** base-year population = source-type population × age
//!   fraction ([`base_year_population`]);
//! * **`Tag-2`** grow the population year by year with survival,
//!   migration and sales-growth rates ([`grow_population`] /
//!   [`grow_one_year`]) — the meatiest step;
//! * **`Tag-3`** the HPMS travel-fraction chain ([`travel_fractions`]);
//! * **`Tag-5`/`-6`** apportion the annual travel fraction across road
//!   type, month, day and hour ([`apportion_to_hour`]);
//! * **`Tag-7`** SHO is set equal to VMT (a deliberate identity — see
//!   below);
//! * **`Tag-9`** distance = SHO × average speed ([`link_distance`]).
//!
//! [`total_activity_basis`] composes `Tag-0` … `Tag-3` into the
//! `TravelFraction` table — the per-`(sourceType, age)` activity
//! proportions that the rest of the pipeline redistributes.
//!
//! # Why SHO equals VMT here
//!
//! The Java `convertVMTToTotalActivityBasis` sets `SHO = VMT` verbatim,
//! with the comment: *"Because Distance is calculated from SHO and is
//! divided out in the end, the actual SHO doesn't matter. But the
//! proportional distribution of SHO among ages, sourcetypes and times must
//! be preserved."* Likewise `allocateTotalActivityBasis` notes that, for
//! the Lookup output domain, allocation to zones and links can be uniform.
//! So the genuine activity computation ends at the travel fractions and
//! their month/day/hour apportionment; the `VMT → SHO → link →
//! SourceHours` chain is proportional bookkeeping. This port reflects that
//! — it computes the fractions and the apportionment formula, and leaves
//! the uniform link redistribution to the data-plane `execute` step.
//!
//! # Data plane (Task 50)
//!
//! The Java reads ~20 MariaDB tables and writes `SHO` / `SourceHours`.
//! [`Generator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are Phase-2 placeholders until
//! the `DataFrameStore` lands (migration-plan Task 50), so `execute`
//! cannot yet read those tables nor write the activity tables. The
//! numerically faithful algorithm is fully ported and unit-tested in the
//! free functions below.
//!
//! # Fidelity notes
//!
//! * The Java grows the population *incrementally* across `executeLoop`
//!   calls, caching `SourceTypeAgePopulation` between years. This port's
//!   [`grow_population`] recomputes from the base year on each call; the
//!   recurrence is identical, so the result is numerically the same — the
//!   Java caching is only a performance optimisation.
//! * MOVES stores the intermediate population / fraction tables in
//!   `FLOAT` (32-bit) columns while evaluating in `DOUBLE`. This port
//!   computes in `f64` throughout, matching the Task 41 / Task 33
//!   precedent; the bug-compatibility decision is deferred to Task 44.

use std::collections::BTreeMap;

use moves_calculator_info::{Granularity, Priority};
use moves_data::ProcessId;
use moves_framework::{
    CalculatorContext, CalculatorOutput, CalculatorSubscription, Error, Generator,
};

/// Running Exhaust — process id 1.
const RUNNING_EXHAUST: ProcessId = ProcessId(1);
/// Brakewear — process id 9.
const BRAKEWEAR: ProcessId = ProcessId(9);
/// Tirewear — process id 10.
const TIREWEAR: ProcessId = ProcessId(10);
/// Evap Permeation — process id 11.
const EVAP_PERMEATION: ProcessId = ProcessId(11);
/// Evap Fuel Vapor Venting — process id 12.
const EVAP_FUEL_VAPOR_VENTING: ProcessId = ProcessId(12);
/// Evap Fuel Leaks — process id 13.
const EVAP_FUEL_LEAKS: ProcessId = ProcessId(13);

/// The oldest tracked age. Ages 0‥=`OLDEST_AGE` are tracked individually;
/// age `OLDEST_AGE` is the cumulative "this age and older" bucket. The
/// Java grows ages `1..40` from their predecessor and accumulates `39` and
/// the existing `40` into the `40` bucket.
const OLDEST_AGE: i16 = 40;

/// One `SourceTypeYear` row — the per-`(year, sourceType)` population and
/// the growth / migration rates used to grow it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypeYearRow {
    /// `yearID`.
    pub year_id: i16,
    /// `sourceTypeID`.
    pub source_type_id: i16,
    /// `sourceTypePopulation` — total vehicles of this type in this year.
    pub source_type_population: f64,
    /// `salesGrowthFactor` — applied to the age-0 cohort.
    pub sales_growth_factor: f64,
    /// `migrationRate` — applied to every cohort each year.
    pub migration_rate: f64,
}

/// One `SourceTypeAgeDistribution` row — the fraction of a source type's
/// population at a given age in a given year.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypeAgeDistributionRow {
    /// `yearID`.
    pub year_id: i16,
    /// `sourceTypeID`.
    pub source_type_id: i16,
    /// `ageID`.
    pub age_id: i16,
    /// `ageFraction` — the share of the source type's population at this age.
    pub age_fraction: f64,
}

/// One `SourceTypeAge` row — the per-`(sourceType, age)` survival rate and
/// relative mileage-accumulation rate.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypeAgeRow {
    /// `sourceTypeID`.
    pub source_type_id: i16,
    /// `ageID`.
    pub age_id: i16,
    /// `survivalRate` — the fraction of a cohort surviving one year.
    pub survival_rate: f64,
    /// `relativeMAR` — relative mileage-accumulation rate (Tag-3 weight).
    pub relative_mar: f64,
}

/// One `SourceTypeAgePopulation` row — a population count for a
/// `(year, sourceType, age)` cell. Produced by [`base_year_population`]
/// and [`grow_population`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypeAgePopulationRow {
    /// `yearID`.
    pub year_id: i16,
    /// `sourceTypeID`.
    pub source_type_id: i16,
    /// `ageID`.
    pub age_id: i16,
    /// `population` — vehicle count in this cell.
    pub population: f64,
}

/// One `SourceUseType` row — the mapping from a source type to its HPMS
/// vehicle type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceUseTypeRow {
    /// `sourceTypeID`.
    pub source_type_id: i16,
    /// `HPMSVTypeID` — the HPMS vehicle-type bucket this source type
    /// rolls up into.
    pub hpms_v_type_id: i16,
}

/// One `TravelFraction` row — the share of an HPMS type's travel
/// attributed to a `(sourceType, age)` cell, for one year. This is the
/// activity basis [`total_activity_basis`] returns.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TravelFractionRow {
    /// `yearID`.
    pub year_id: i16,
    /// `sourceTypeID`.
    pub source_type_id: i16,
    /// `ageID`.
    pub age_id: i16,
    /// `fraction` — the travel fraction for this cell.
    pub fraction: f64,
}

/// One `Year` row — used by [`determine_base_year`] to find the base
/// year nearest an analysis year.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct YearRow {
    /// `yearID`.
    pub year_id: i16,
    /// `isBaseYear` — whether this year carries base population data.
    pub is_base_year: bool,
}

/// `Tag-0`: find the base year for an analysis year — the greatest
/// `isBaseYear` year not after `analysis_year`.
///
/// Ports `determineBaseYear`: `SELECT MAX(yearID) FROM Year WHERE yearID
/// <= analysisYear AND isBaseYear = 'Y'`. Returns `None` when no base year
/// qualifies (the Java throws; a pure port surfaces it as `None`).
#[must_use]
pub fn determine_base_year(years: &[YearRow], analysis_year: i16) -> Option<i16> {
    years
        .iter()
        .filter(|y| y.is_base_year && y.year_id <= analysis_year)
        .map(|y| y.year_id)
        .max()
}

/// `Tag-1`: the base-year population by `(sourceType, age)`.
///
/// Ports `calculateBaseYearPopulation`: `population = SourceTypeYear
/// .sourceTypePopulation × SourceTypeAgeDistribution.ageFraction`, joined
/// on `(sourceType, year)` at the base year.
#[must_use]
pub fn base_year_population(
    base_year: i16,
    source_type_years: &[SourceTypeYearRow],
    age_distribution: &[SourceTypeAgeDistributionRow],
) -> Vec<SourceTypeAgePopulationRow> {
    let population: BTreeMap<i16, f64> = source_type_years
        .iter()
        .filter(|s| s.year_id == base_year)
        .map(|s| (s.source_type_id, s.source_type_population))
        .collect();
    let mut rows: Vec<SourceTypeAgePopulationRow> = age_distribution
        .iter()
        .filter(|d| d.year_id == base_year)
        .filter_map(|d| {
            population
                .get(&d.source_type_id)
                .map(|&pop| SourceTypeAgePopulationRow {
                    year_id: base_year,
                    source_type_id: d.source_type_id,
                    age_id: d.age_id,
                    population: pop * d.age_fraction,
                })
        })
        .collect();
    rows.sort_by_key(|r| (r.source_type_id, r.age_id));
    rows
}

/// Grow one year of population — the inner recurrence of `Tag-2`
/// (`growPopulationToAnalysisYear`).
///
/// Given the previous year's population (`prev`, keyed `(sourceType, age)`)
/// and the `SourceTypeYear` rows for the previous and current years,
/// produces the current year's population:
///
/// * **age 0** — `(prevPop[0] / prevMigrationRate) × salesGrowthFactor ×
///   migrationRate`, skipped when `prevMigrationRate = 0` (the Java
///   `sty.migrationRate <> 0` filter);
/// * **ages 1‥39** — `prevPop[age-1] × survivalRate[age-1] ×
///   migrationRate`;
/// * **age 40** (the cumulative bucket) — `prevPop[39] × survivalRate[39]
///   × migrationRate + prevPop[40] × survivalRate[40] × migrationRate`.
///
/// `current_year` labels the produced rows.
#[must_use]
pub fn grow_one_year(
    current_year: i16,
    prev: &BTreeMap<(i16, i16), f64>,
    prev_source_type_years: &[SourceTypeYearRow],
    source_type_years: &[SourceTypeYearRow],
    source_type_ages: &[SourceTypeAgeRow],
) -> Vec<SourceTypeAgePopulationRow> {
    let prev_migration: BTreeMap<i16, f64> = prev_source_type_years
        .iter()
        .map(|s| (s.source_type_id, s.migration_rate))
        .collect();
    let survival: BTreeMap<(i16, i16), f64> = source_type_ages
        .iter()
        .map(|a| ((a.source_type_id, a.age_id), a.survival_rate))
        .collect();
    let mut rows: Vec<SourceTypeAgePopulationRow> = Vec::new();
    for sty in source_type_years {
        if sty.year_id != current_year {
            continue;
        }
        let source_type_id = sty.source_type_id;
        let mut push = |age_id: i16, population: f64| {
            rows.push(SourceTypeAgePopulationRow {
                year_id: current_year,
                source_type_id,
                age_id,
                population,
            });
        };

        // age 0 — new sales, scaled out of the previous migration rate.
        let prev_migration_rate = prev_migration.get(&source_type_id).copied().unwrap_or(0.0);
        if prev_migration_rate != 0.0 {
            if let Some(&prev_pop) = prev.get(&(source_type_id, 0)) {
                push(
                    0,
                    (prev_pop / prev_migration_rate) * sty.sales_growth_factor * sty.migration_rate,
                );
            }
        }

        // ages 1‥39 — survivors of the previous age, one year older.
        for age in 1..OLDEST_AGE {
            let prev_age = age - 1;
            if let (Some(&prev_pop), Some(&rate)) = (
                prev.get(&(source_type_id, prev_age)),
                survival.get(&(source_type_id, prev_age)),
            ) {
                push(age, prev_pop * rate * sty.migration_rate);
            }
        }

        // age 40 — the cumulative bucket: survivors of 39 plus survivors
        // of the existing 40-and-older bucket.
        let from_39 = match (
            prev.get(&(source_type_id, OLDEST_AGE - 1)),
            survival.get(&(source_type_id, OLDEST_AGE - 1)),
        ) {
            (Some(&pop), Some(&rate)) => Some(pop * rate * sty.migration_rate),
            _ => None,
        };
        let from_40 = match (
            prev.get(&(source_type_id, OLDEST_AGE)),
            survival.get(&(source_type_id, OLDEST_AGE)),
        ) {
            (Some(&pop), Some(&rate)) => Some(pop * rate * sty.migration_rate),
            _ => None,
        };
        if from_39.is_some() || from_40.is_some() {
            push(OLDEST_AGE, from_39.unwrap_or(0.0) + from_40.unwrap_or(0.0));
        }
    }
    rows.sort_by_key(|r| (r.source_type_id, r.age_id));
    rows
}

/// `Tag-2`: grow the base-year population forward to `analysis_year`.
///
/// Ports `growPopulationToAnalysisYear` by iterating [`grow_one_year`]
/// from `base_year + 1` through `analysis_year`. When `analysis_year <=
/// base_year` the base population is returned unchanged.
///
/// See the module-level fidelity note: the Java caches intermediate years
/// across `executeLoop` calls; recomputing from the base year here yields
/// the identical recurrence.
#[must_use]
pub fn grow_population(
    base_year: i16,
    analysis_year: i16,
    base_population: &[SourceTypeAgePopulationRow],
    source_type_years: &[SourceTypeYearRow],
    source_type_ages: &[SourceTypeAgeRow],
) -> Vec<SourceTypeAgePopulationRow> {
    let mut current: Vec<SourceTypeAgePopulationRow> = base_population.to_vec();
    let mut current_year = base_year;
    while current_year < analysis_year {
        let next_year = current_year + 1;
        let prev: BTreeMap<(i16, i16), f64> = current
            .iter()
            .map(|r| ((r.source_type_id, r.age_id), r.population))
            .collect();
        current = grow_one_year(
            next_year,
            &prev,
            &source_type_years
                .iter()
                .copied()
                .filter(|s| s.year_id == current_year)
                .collect::<Vec<_>>(),
            source_type_years,
            source_type_ages,
        );
        current_year = next_year;
    }
    current
}

/// `Tag-3`: the HPMS travel-fraction chain.
///
/// Ports `calculateFractionOfTravelUsingHPMS` — four dependent steps:
///
/// 1. `HPMSVTypePopulation` — total population per HPMS vehicle type;
/// 2. `FractionWithinHPMSVType` — each cell's share of its HPMS type's
///    population (computed only where the HPMS-type total is non-zero);
/// 3. `HPMSTravelFraction` — `Σ (fractionWithin × relativeMAR)` per HPMS
///    type;
/// 4. `TravelFraction` — `(fractionWithin × relativeMAR) /
///    hpmsTravelFraction` (computed only where the HPMS travel fraction
///    is non-zero).
///
/// `population` is the analysis-year population by `(sourceType, age)`.
#[must_use]
pub fn travel_fractions(
    population: &[SourceTypeAgePopulationRow],
    source_use_types: &[SourceUseTypeRow],
    source_type_ages: &[SourceTypeAgeRow],
) -> Vec<TravelFractionRow> {
    let hpms_of: BTreeMap<i16, i16> = source_use_types
        .iter()
        .map(|s| (s.source_type_id, s.hpms_v_type_id))
        .collect();
    let relative_mar: BTreeMap<(i16, i16), f64> = source_type_ages
        .iter()
        .map(|a| ((a.source_type_id, a.age_id), a.relative_mar))
        .collect();

    // Step 1: population summed per HPMS vehicle type.
    let mut hpms_population: BTreeMap<i16, f64> = BTreeMap::new();
    for row in population {
        if let Some(&hpms) = hpms_of.get(&row.source_type_id) {
            *hpms_population.entry(hpms).or_insert(0.0) += row.population;
        }
    }

    // Step 2: each cell's share of its HPMS type's population.
    struct Within {
        year_id: i16,
        source_type_id: i16,
        age_id: i16,
        hpms: i16,
        fraction: f64,
    }
    let mut within: Vec<Within> = Vec::new();
    for row in population {
        let Some(&hpms) = hpms_of.get(&row.source_type_id) else {
            continue;
        };
        let Some(&hpms_total) = hpms_population.get(&hpms) else {
            continue;
        };
        if hpms_total == 0.0 {
            continue;
        }
        within.push(Within {
            year_id: row.year_id,
            source_type_id: row.source_type_id,
            age_id: row.age_id,
            hpms,
            fraction: row.population / hpms_total,
        });
    }

    // Step 3: Σ (fractionWithin × relativeMAR) per HPMS type.
    let mut hpms_travel: BTreeMap<i16, f64> = BTreeMap::new();
    for w in &within {
        if let Some(&mar) = relative_mar.get(&(w.source_type_id, w.age_id)) {
            *hpms_travel.entry(w.hpms).or_insert(0.0) += w.fraction * mar;
        }
    }

    // Step 4: normalise each cell by its HPMS type's travel fraction.
    let mut rows: Vec<TravelFractionRow> = Vec::new();
    for w in &within {
        let Some(&mar) = relative_mar.get(&(w.source_type_id, w.age_id)) else {
            continue;
        };
        let Some(&hpms_travel_fraction) = hpms_travel.get(&w.hpms) else {
            continue;
        };
        if hpms_travel_fraction == 0.0 {
            continue;
        }
        rows.push(TravelFractionRow {
            year_id: w.year_id,
            source_type_id: w.source_type_id,
            age_id: w.age_id,
            fraction: (w.fraction * mar) / hpms_travel_fraction,
        });
    }
    rows.sort_by_key(|r| (r.source_type_id, r.age_id));
    rows
}

/// Run `Tag-0` … `Tag-3` — find the base year, build the base-year
/// population, grow it to `analysis_year`, and reduce it to the
/// `TravelFraction` activity basis.
///
/// Returns `None` when no base year qualifies for `analysis_year`.
#[must_use]
pub fn total_activity_basis(
    analysis_year: i16,
    years: &[YearRow],
    source_type_years: &[SourceTypeYearRow],
    age_distribution: &[SourceTypeAgeDistributionRow],
    source_type_ages: &[SourceTypeAgeRow],
    source_use_types: &[SourceUseTypeRow],
) -> Option<Vec<TravelFractionRow>> {
    let base_year = determine_base_year(years, analysis_year)?;
    let base = base_year_population(base_year, source_type_years, age_distribution);
    let grown = grow_population(
        base_year,
        analysis_year,
        &base,
        source_type_years,
        source_type_ages,
    );
    Some(travel_fractions(&grown, source_use_types, source_type_ages))
}

/// `Tag-6`: apportion an annual travel quantity to a single hour.
///
/// Ports the `calculateVMTByRoadwayHour` arithmetic: `VMT × monthFraction
/// × dayFraction × hourFraction / weeksInMonth`. The annual quantity is a
/// travel fraction (after `Tag-5` replicates `TravelFraction` across road
/// types into `AnnualVMTByAgeRoadway`); the month / day / hour fractions
/// come from `MonthVMTFraction`, `DayVMTFraction` and `HourVMTFraction`;
/// `weeks_in_month` is the `WeeksInMonthHelper` divisor.
///
/// Returns `0.0` when `weeks_in_month` is zero rather than dividing by it.
#[must_use]
pub fn apportion_to_hour(
    annual_vmt: f64,
    month_fraction: f64,
    day_fraction: f64,
    hour_fraction: f64,
    weeks_in_month: f64,
) -> f64 {
    if weeks_in_month == 0.0 {
        return 0.0;
    }
    annual_vmt * month_fraction * day_fraction * hour_fraction / weeks_in_month
}

/// `Tag-9`: the link distance — `SHO × averageSpeed`.
///
/// Ports `calculateDistance`: `SHO.distance = SHO.SHO × LinkAverageSpeed
/// .averageSpeed`. In a Mesoscale-Lookup run `SHO` equals VMT (see the
/// module docs), so this recovers a distance proportional to the activity.
#[must_use]
pub fn link_distance(source_hours: f64, average_speed: f64) -> f64 {
    source_hours * average_speed
}

/// Default-database tables the generator reads. Names are the canonical
/// MOVES table names; the registry maps them onto Parquet snapshots.
static INPUT_TABLES: &[&str] = &[
    "year",
    "sourceTypeYear",
    "sourceTypeAgeDistribution",
    "sourceTypeAge",
    "sourceUseType",
    "monthVMTFraction",
    "dayVMTFraction",
    "hourVMTFraction",
    "roadType",
    "link",
    "linkAverageSpeed",
    "hourDay",
];

/// Scratch-namespace tables this generator writes — the activity tables
/// the emission calculators consume.
static OUTPUT_TABLES: &[&str] = &["SHO", "SourceHours"];

/// Total-Activity generator for Mesoscale-Lookup runs.
///
/// Ports `MesoscaleLookupTotalActivityGenerator.java`; see the module
/// documentation for the algorithm and the scope of the port.
#[derive(Debug, Clone)]
pub struct MesoscaleLookupTotalActivityGenerator {
    /// The master-loop subscriptions, built once in [`Self::new`].
    subscriptions: Vec<CalculatorSubscription>,
}

impl MesoscaleLookupTotalActivityGenerator {
    /// Chain-DAG name — matches the Java class name.
    pub const NAME: &'static str = "MesoscaleLookupTotalActivityGenerator";

    /// Construct the generator with its master-loop subscriptions.
    ///
    /// Mirrors `subscribeToMe`: Running Exhaust, Evap Permeation, Evap
    /// Fuel Vapor Venting, Evap Fuel Leaks, Brakewear and Tirewear, all at
    /// `YEAR` granularity, `GENERATOR` priority. The Java additionally
    /// guards each subscription with `doesHavePollutantAndProcess` — a
    /// runtime RunSpec decision the registry / engine applies — so the
    /// static metadata lists every process unconditionally.
    ///
    /// The Java also attempts an `"Evap Non-Fuel Vapors"` subscription;
    /// that process name does not resolve in the pinned MOVES default-DB
    /// process table, so `EmissionProcess.findByName` returns `null` and
    /// the subscription is never made — it is correspondingly absent here.
    #[must_use]
    pub fn new() -> Self {
        let priority =
            Priority::parse("GENERATOR").expect("\"GENERATOR\" is a valid MasterLoop priority");
        let at_year = |process| CalculatorSubscription::new(process, Granularity::Year, priority);
        Self {
            subscriptions: vec![
                at_year(RUNNING_EXHAUST),
                at_year(EVAP_PERMEATION),
                at_year(EVAP_FUEL_VAPOR_VENTING),
                at_year(EVAP_FUEL_LEAKS),
                at_year(BRAKEWEAR),
                at_year(TIREWEAR),
            ],
        }
    }
}

impl Default for MesoscaleLookupTotalActivityGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl Generator for MesoscaleLookupTotalActivityGenerator {
    fn name(&self) -> &'static str {
        Self::NAME
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

    /// Run the generator for the current master-loop iteration.
    ///
    /// **Data plane pending (Task 50).** [`CalculatorContext`] exposes
    /// only placeholder `ExecutionTables` / `ScratchNamespace` today, so
    /// this body cannot read the [`input_tables`](Generator::input_tables)
    /// nor write `SHO` / `SourceHours`. The numerically faithful algorithm
    /// is fully ported and tested in [`total_activity_basis`] and the
    /// apportionment kernels; once the `DataFrameStore` lands, `execute`
    /// projects the input view from `ctx.tables()`, runs the pipeline for
    /// `ctx.position().time.year`, and writes the activity tables.
    fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        Ok(CalculatorOutput::empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_year_is_greatest_qualifying_base_year() {
        let years = [
            YearRow {
                year_id: 1990,
                is_base_year: true,
            },
            YearRow {
                year_id: 1999,
                is_base_year: false,
            },
            YearRow {
                year_id: 2000,
                is_base_year: true,
            },
            YearRow {
                year_id: 2010,
                is_base_year: true,
            },
        ];
        // 2005 → the greatest base year not after it is 2000.
        assert_eq!(determine_base_year(&years, 2005), Some(2000));
        // 2010 itself is a base year.
        assert_eq!(determine_base_year(&years, 2010), Some(2010));
        // Before any base year → None.
        assert_eq!(determine_base_year(&years, 1985), None);
    }

    #[test]
    fn base_year_population_is_population_times_age_fraction() {
        let years = [SourceTypeYearRow {
            year_id: 2000,
            source_type_id: 21,
            source_type_population: 1000.0,
            sales_growth_factor: 1.0,
            migration_rate: 1.0,
        }];
        let dist = [
            SourceTypeAgeDistributionRow {
                year_id: 2000,
                source_type_id: 21,
                age_id: 0,
                age_fraction: 0.25,
            },
            SourceTypeAgeDistributionRow {
                year_id: 2000,
                source_type_id: 21,
                age_id: 1,
                age_fraction: 0.75,
            },
            // A different year must be ignored.
            SourceTypeAgeDistributionRow {
                year_id: 1999,
                source_type_id: 21,
                age_id: 0,
                age_fraction: 1.0,
            },
        ];
        let pop = base_year_population(2000, &years, &dist);
        assert_eq!(pop.len(), 2);
        assert!((pop[0].population - 250.0).abs() < 1e-9);
        assert!((pop[1].population - 750.0).abs() < 1e-9);
    }

    #[test]
    fn grow_one_year_age_zero_uses_sales_growth_and_migration() {
        // prevPop[0] = 100, prevMigration = 2, salesGrowth = 1.5,
        // migration = 3 → (100/2)·1.5·3 = 225.
        let prev: BTreeMap<(i16, i16), f64> = [((21, 0), 100.0)].into_iter().collect();
        let prev_years = [SourceTypeYearRow {
            year_id: 2000,
            source_type_id: 21,
            source_type_population: 0.0,
            sales_growth_factor: 0.0,
            migration_rate: 2.0,
        }];
        let years = [SourceTypeYearRow {
            year_id: 2001,
            source_type_id: 21,
            source_type_population: 0.0,
            sales_growth_factor: 1.5,
            migration_rate: 3.0,
        }];
        let grown = grow_one_year(2001, &prev, &prev_years, &years, &[]);
        let age0 = grown.iter().find(|r| r.age_id == 0).expect("age 0 row");
        assert!((age0.population - 225.0).abs() < 1e-9);
    }

    #[test]
    fn grow_one_year_age_zero_skipped_when_prev_migration_zero() {
        // prevMigrationRate = 0 → the Java `migrationRate <> 0` filter
        // drops the age-0 row entirely (no division by zero).
        let prev: BTreeMap<(i16, i16), f64> = [((21, 0), 100.0)].into_iter().collect();
        let prev_years = [SourceTypeYearRow {
            year_id: 2000,
            source_type_id: 21,
            source_type_population: 0.0,
            sales_growth_factor: 0.0,
            migration_rate: 0.0,
        }];
        let years = [SourceTypeYearRow {
            year_id: 2001,
            source_type_id: 21,
            source_type_population: 0.0,
            sales_growth_factor: 1.5,
            migration_rate: 3.0,
        }];
        let grown = grow_one_year(2001, &prev, &prev_years, &years, &[]);
        assert!(grown.iter().all(|r| r.age_id != 0));
    }

    #[test]
    fn grow_one_year_shifts_ages_with_survival_and_migration() {
        // prevPop[age 0] = 80, survivalRate[0] = 0.9, migration = 1.1 →
        // age 1 = 80·0.9·1.1 = 79.2.
        let prev: BTreeMap<(i16, i16), f64> = [((21, 0), 80.0)].into_iter().collect();
        let years = [SourceTypeYearRow {
            year_id: 2001,
            source_type_id: 21,
            source_type_population: 0.0,
            sales_growth_factor: 1.0,
            migration_rate: 1.1,
        }];
        let ages = [SourceTypeAgeRow {
            source_type_id: 21,
            age_id: 0,
            survival_rate: 0.9,
            relative_mar: 1.0,
        }];
        // No previous-year SourceTypeYear → age 0 skipped, age 1 present.
        let grown = grow_one_year(2001, &prev, &[], &years, &ages);
        let age1 = grown.iter().find(|r| r.age_id == 1).expect("age 1 row");
        assert!((age1.population - 79.2).abs() < 1e-9);
    }

    #[test]
    fn grow_one_year_age_40_accumulates_39_and_existing_40() {
        // age 40 = prev[39]·survival[39]·mig + prev[40]·survival[40]·mig
        //        = 10·0.5·2 + 20·0.25·2 = 10 + 10 = 20.
        let prev: BTreeMap<(i16, i16), f64> =
            [((21, 39), 10.0), ((21, 40), 20.0)].into_iter().collect();
        let years = [SourceTypeYearRow {
            year_id: 2001,
            source_type_id: 21,
            source_type_population: 0.0,
            sales_growth_factor: 1.0,
            migration_rate: 2.0,
        }];
        let ages = [
            SourceTypeAgeRow {
                source_type_id: 21,
                age_id: 39,
                survival_rate: 0.5,
                relative_mar: 1.0,
            },
            SourceTypeAgeRow {
                source_type_id: 21,
                age_id: 40,
                survival_rate: 0.25,
                relative_mar: 1.0,
            },
        ];
        let grown = grow_one_year(2001, &prev, &[], &years, &ages);
        let age40 = grown.iter().find(|r| r.age_id == 40).expect("age 40 row");
        assert!((age40.population - 20.0).abs() < 1e-9);
    }

    #[test]
    fn grow_population_returns_base_unchanged_at_base_year() {
        let base = [SourceTypeAgePopulationRow {
            year_id: 2000,
            source_type_id: 21,
            age_id: 0,
            population: 500.0,
        }];
        let grown = grow_population(2000, 2000, &base, &[], &[]);
        assert_eq!(grown, base);
    }

    #[test]
    fn grow_population_iterates_multiple_years() {
        // Two-year grow of a single age-1 cohort: each year multiplies by
        // survivalRate[0]·migration. With survival 1.0 and migration 1.0,
        // age advances 0 → 1 → 2 and the count is preserved.
        let base = [SourceTypeAgePopulationRow {
            year_id: 2000,
            source_type_id: 21,
            age_id: 0,
            population: 100.0,
        }];
        let years: Vec<SourceTypeYearRow> = (2000..=2002)
            .map(|y| SourceTypeYearRow {
                year_id: y,
                source_type_id: 21,
                source_type_population: 0.0,
                sales_growth_factor: 1.0,
                migration_rate: 1.0,
            })
            .collect();
        let ages: Vec<SourceTypeAgeRow> = (0..=40)
            .map(|a| SourceTypeAgeRow {
                source_type_id: 21,
                age_id: a,
                survival_rate: 1.0,
                relative_mar: 1.0,
            })
            .collect();
        let grown = grow_population(2000, 2002, &base, &years, &ages);
        // The age-0 cohort has advanced to age 2, count unchanged.
        let age2 = grown.iter().find(|r| r.age_id == 2).expect("age 2 row");
        assert_eq!(age2.year_id, 2002);
        assert!((age2.population - 100.0).abs() < 1e-9);
    }

    #[test]
    fn travel_fractions_normalise_within_hpms_type() {
        // One HPMS type, two source types. relativeMAR = 1 everywhere, so
        // TravelFraction = fractionWithin (each cell's population share).
        let population = [
            SourceTypeAgePopulationRow {
                year_id: 2010,
                source_type_id: 21,
                age_id: 0,
                population: 30.0,
            },
            SourceTypeAgePopulationRow {
                year_id: 2010,
                source_type_id: 31,
                age_id: 0,
                population: 70.0,
            },
        ];
        let use_types = [
            SourceUseTypeRow {
                source_type_id: 21,
                hpms_v_type_id: 10,
            },
            SourceUseTypeRow {
                source_type_id: 31,
                hpms_v_type_id: 10,
            },
        ];
        let ages = [
            SourceTypeAgeRow {
                source_type_id: 21,
                age_id: 0,
                survival_rate: 1.0,
                relative_mar: 1.0,
            },
            SourceTypeAgeRow {
                source_type_id: 31,
                age_id: 0,
                survival_rate: 1.0,
                relative_mar: 1.0,
            },
        ];
        let fractions = travel_fractions(&population, &use_types, &ages);
        // fractionWithin = 0.3 / 0.7; hpmsTravel = 0.3·1 + 0.7·1 = 1.0;
        // TravelFraction = fractionWithin / 1.0.
        assert_eq!(fractions.len(), 2);
        assert!((fractions[0].fraction - 0.3).abs() < 1e-9);
        assert!((fractions[1].fraction - 0.7).abs() < 1e-9);
        // Travel fractions within an HPMS type sum to 1.
        let total: f64 = fractions.iter().map(|f| f.fraction).sum();
        assert!((total - 1.0).abs() < 1e-9);
    }

    #[test]
    fn travel_fractions_weight_by_relative_mar() {
        // Equal populations but the second cell has twice the relativeMAR
        // → it gets twice the travel fraction.
        let population = [
            SourceTypeAgePopulationRow {
                year_id: 2010,
                source_type_id: 21,
                age_id: 0,
                population: 50.0,
            },
            SourceTypeAgePopulationRow {
                year_id: 2010,
                source_type_id: 21,
                age_id: 1,
                population: 50.0,
            },
        ];
        let use_types = [SourceUseTypeRow {
            source_type_id: 21,
            hpms_v_type_id: 10,
        }];
        let ages = [
            SourceTypeAgeRow {
                source_type_id: 21,
                age_id: 0,
                survival_rate: 1.0,
                relative_mar: 1.0,
            },
            SourceTypeAgeRow {
                source_type_id: 21,
                age_id: 1,
                survival_rate: 1.0,
                relative_mar: 2.0,
            },
        ];
        let fractions = travel_fractions(&population, &use_types, &ages);
        // fractionWithin = 0.5 each; hpmsTravel = 0.5·1 + 0.5·2 = 1.5;
        // TravelFraction = {0.5·1/1.5, 0.5·2/1.5} = {1/3, 2/3}.
        assert!((fractions[0].fraction - 1.0 / 3.0).abs() < 1e-9);
        assert!((fractions[1].fraction - 2.0 / 3.0).abs() < 1e-9);
    }

    #[test]
    fn total_activity_basis_runs_the_tag0_to_tag3_pipeline() {
        let years = [YearRow {
            year_id: 2010,
            is_base_year: true,
        }];
        let source_type_years = [SourceTypeYearRow {
            year_id: 2010,
            source_type_id: 21,
            source_type_population: 100.0,
            sales_growth_factor: 1.0,
            migration_rate: 1.0,
        }];
        let dist = [SourceTypeAgeDistributionRow {
            year_id: 2010,
            source_type_id: 21,
            age_id: 0,
            age_fraction: 1.0,
        }];
        let ages = [SourceTypeAgeRow {
            source_type_id: 21,
            age_id: 0,
            survival_rate: 1.0,
            relative_mar: 1.0,
        }];
        let use_types = [SourceUseTypeRow {
            source_type_id: 21,
            hpms_v_type_id: 10,
        }];
        let basis =
            total_activity_basis(2010, &years, &source_type_years, &dist, &ages, &use_types)
                .expect("base year resolves");
        // The single cell carries the whole HPMS type's travel.
        assert_eq!(basis.len(), 1);
        assert!((basis[0].fraction - 1.0).abs() < 1e-9);
    }

    #[test]
    fn total_activity_basis_none_without_a_base_year() {
        let years = [YearRow {
            year_id: 2020,
            is_base_year: true,
        }];
        assert!(total_activity_basis(2010, &years, &[], &[], &[], &[]).is_none());
    }

    #[test]
    fn apportion_to_hour_multiplies_fractions_and_divides_by_weeks() {
        // 1000 · 0.1 · 0.2 · 0.5 / 4 = 2.5.
        let v = apportion_to_hour(1000.0, 0.1, 0.2, 0.5, 4.0);
        assert!((v - 2.5).abs() < 1e-9);
    }

    #[test]
    fn apportion_to_hour_zero_weeks_yields_zero() {
        assert_eq!(apportion_to_hour(1000.0, 0.1, 0.2, 0.5, 0.0), 0.0);
    }

    #[test]
    fn link_distance_is_source_hours_times_speed() {
        assert!((link_distance(12.0, 55.0) - 660.0).abs() < 1e-9);
    }

    #[test]
    fn generator_metadata_matches_java_subscribe_to_me() {
        let gen = MesoscaleLookupTotalActivityGenerator::new();
        assert_eq!(gen.name(), "MesoscaleLookupTotalActivityGenerator");
        assert_eq!(gen.output_tables(), &["SHO", "SourceHours"]);
        // No upstream generator dependency.
        assert!(gen.upstream().is_empty());
        let subs = gen.subscriptions();
        assert_eq!(subs.len(), 6);
        let processes: Vec<ProcessId> = subs.iter().map(|s| s.process_id).collect();
        assert_eq!(
            processes,
            vec![
                ProcessId(1),
                ProcessId(11),
                ProcessId(12),
                ProcessId(13),
                ProcessId(9),
                ProcessId(10),
            ],
        );
        for s in subs {
            assert_eq!(s.granularity, Granularity::Year);
            assert_eq!(s.priority.display(), "GENERATOR");
        }
    }

    #[test]
    fn generator_execute_returns_placeholder_until_data_plane() {
        let gen = MesoscaleLookupTotalActivityGenerator::new();
        let ctx = CalculatorContext::new();
        assert!(gen.execute(&ctx).is_ok());
    }

    #[test]
    fn generator_is_object_safe() {
        let gen: Box<dyn Generator> = Box::new(MesoscaleLookupTotalActivityGenerator::new());
        assert_eq!(gen.name(), "MesoscaleLookupTotalActivityGenerator");
    }
}

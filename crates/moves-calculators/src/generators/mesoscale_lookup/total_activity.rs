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
    CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped, Error,
    Generator, IntoDataFrame, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Series};

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

// ── Data-plane helpers ───────────────────────────────────────────────────────

/// Build a typed row-extraction error for `from_dataframe` impls.
fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::RowExtraction {
        table: table.into(),
        row,
        column: column.into(),
        message: msg,
    }
}

// ── Input TableRow impls ─────────────────────────────────────────────────────

impl TableRow for YearRow {
    fn table_name() -> &'static str {
        "year"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([
            ("yearID".into(), DataType::Int32),
            ("isBaseYear".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id as i32).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "isBaseYear".into(),
                    rows.iter()
                        .map(|r| if r.is_base_year { 1i32 } else { 0 })
                        .collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "year";
        let year_id_col = df
            .column("yearID")
            .map_err(|e| row_err(t, 0, "yearID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "yearID", e.to_string()))?;
        let is_base_year_col = df
            .column("isBaseYear")
            .map_err(|e| row_err(t, 0, "isBaseYear", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "isBaseYear", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(YearRow {
                    year_id: year_id_col.get(i).ok_or_else(|| null("yearID"))? as i16,
                    // Canonical MOVES SQL filters Year via `isBaseYear IN ('Y','y')`,
                    // so NULL is semantically "not a base year". Match that here.
                    is_base_year: is_base_year_col.get(i).unwrap_or(0) != 0,
                })
            })
            .collect()
    }
}

impl TableRow for SourceTypeYearRow {
    fn table_name() -> &'static str {
        "sourceTypeYear"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([
            ("yearID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("sourceTypePopulation".into(), DataType::Float64),
            ("salesGrowthFactor".into(), DataType::Float64),
            ("migrationRate".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id as i32).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter()
                        .map(|r| r.source_type_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypePopulation".into(),
                    rows.iter()
                        .map(|r| r.source_type_population)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "salesGrowthFactor".into(),
                    rows.iter()
                        .map(|r| r.sales_growth_factor)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "migrationRate".into(),
                    rows.iter().map(|r| r.migration_rate).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "sourceTypeYear";
        let year_id_col = df
            .column("yearID")
            .map_err(|e| row_err(t, 0, "yearID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "yearID", e.to_string()))?;
        let source_type_id_col = df
            .column("sourceTypeID")
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?;
        let source_type_population_col = df
            .column("sourceTypePopulation")
            .map_err(|e| row_err(t, 0, "sourceTypePopulation", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "sourceTypePopulation", e.to_string()))?;
        let sales_growth_factor_col = df
            .column("salesGrowthFactor")
            .map_err(|e| row_err(t, 0, "salesGrowthFactor", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "salesGrowthFactor", e.to_string()))?;
        let migration_rate_col = df
            .column("migrationRate")
            .map_err(|e| row_err(t, 0, "migrationRate", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "migrationRate", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SourceTypeYearRow {
                    year_id: year_id_col.get(i).ok_or_else(|| null("yearID"))? as i16,
                    source_type_id: source_type_id_col
                        .get(i)
                        .ok_or_else(|| null("sourceTypeID"))?
                        as i16,
                    source_type_population: source_type_population_col
                        .get(i)
                        .ok_or_else(|| null("sourceTypePopulation"))?,
                    sales_growth_factor: sales_growth_factor_col
                        .get(i)
                        .ok_or_else(|| null("salesGrowthFactor"))?,
                    migration_rate: migration_rate_col
                        .get(i)
                        .ok_or_else(|| null("migrationRate"))?,
                })
            })
            .collect()
    }
}

impl TableRow for SourceTypeAgeDistributionRow {
    fn table_name() -> &'static str {
        "sourceTypeAgeDistribution"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("ageFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter()
                        .map(|r| r.source_type_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id as i32).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id as i32).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageFraction".into(),
                    rows.iter().map(|r| r.age_fraction).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "sourceTypeAgeDistribution";
        let source_type_id_col = df
            .column("sourceTypeID")
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?;
        let year_id_col = df
            .column("yearID")
            .map_err(|e| row_err(t, 0, "yearID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "yearID", e.to_string()))?;
        let age_id_col = df
            .column("ageID")
            .map_err(|e| row_err(t, 0, "ageID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "ageID", e.to_string()))?;
        let age_fraction_col = df
            .column("ageFraction")
            .map_err(|e| row_err(t, 0, "ageFraction", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "ageFraction", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SourceTypeAgeDistributionRow {
                    source_type_id: source_type_id_col
                        .get(i)
                        .ok_or_else(|| null("sourceTypeID"))?
                        as i16,
                    year_id: year_id_col.get(i).ok_or_else(|| null("yearID"))? as i16,
                    age_id: age_id_col.get(i).ok_or_else(|| null("ageID"))? as i16,
                    age_fraction: age_fraction_col.get(i).ok_or_else(|| null("ageFraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for SourceTypeAgeRow {
    fn table_name() -> &'static str {
        "sourceTypeAge"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("survivalRate".into(), DataType::Float64),
            ("relativeMAR".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter()
                        .map(|r| r.source_type_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id as i32).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "survivalRate".into(),
                    rows.iter().map(|r| r.survival_rate).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "relativeMAR".into(),
                    rows.iter().map(|r| r.relative_mar).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "sourceTypeAge";
        let source_type_id_col = df
            .column("sourceTypeID")
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?;
        let age_id_col = df
            .column("ageID")
            .map_err(|e| row_err(t, 0, "ageID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "ageID", e.to_string()))?;
        let survival_rate_col = df
            .column("survivalRate")
            .map_err(|e| row_err(t, 0, "survivalRate", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "survivalRate", e.to_string()))?;
        let relative_mar_col = df
            .column("relativeMAR")
            .map_err(|e| row_err(t, 0, "relativeMAR", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "relativeMAR", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SourceTypeAgeRow {
                    source_type_id: source_type_id_col
                        .get(i)
                        .ok_or_else(|| null("sourceTypeID"))?
                        as i16,
                    age_id: age_id_col.get(i).ok_or_else(|| null("ageID"))? as i16,
                    survival_rate: survival_rate_col
                        .get(i)
                        .ok_or_else(|| null("survivalRate"))?,
                    relative_mar: relative_mar_col.get(i).ok_or_else(|| null("relativeMAR"))?,
                })
            })
            .collect()
    }
}

impl TableRow for SourceUseTypeRow {
    fn table_name() -> &'static str {
        "sourceUseType"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("HPMSVTypeID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter()
                        .map(|r| r.source_type_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "HPMSVTypeID".into(),
                    rows.iter()
                        .map(|r| r.hpms_v_type_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "sourceUseType";
        let source_type_id_col = df
            .column("sourceTypeID")
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?;
        let hpms_v_type_id_col = df
            .column("HPMSVTypeID")
            .map_err(|e| row_err(t, 0, "HPMSVTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "HPMSVTypeID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SourceUseTypeRow {
                    source_type_id: source_type_id_col
                        .get(i)
                        .ok_or_else(|| null("sourceTypeID"))?
                        as i16,
                    hpms_v_type_id: hpms_v_type_id_col
                        .get(i)
                        .ok_or_else(|| null("HPMSVTypeID"))?
                        as i16,
                })
            })
            .collect()
    }
}

// ── Auxiliary input row types (local to execute wiring) ───────────────────────

/// One `monthVMTFraction` row — per `(sourceType, month)` share of annual VMT.
struct MonthVmtFractionRow {
    source_type_id: i16,
    month_id: i16,
    month_vmt_fraction: f64,
}

impl TableRow for MonthVmtFractionRow {
    fn table_name() -> &'static str {
        "monthVMTFraction"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("monthVMTFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter()
                        .map(|r| r.source_type_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id as i32).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthVMTFraction".into(),
                    rows.iter()
                        .map(|r| r.month_vmt_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "monthVMTFraction";
        let source_type_id_col = df
            .column("sourceTypeID")
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?;
        let month_id_col = df
            .column("monthID")
            .map_err(|e| row_err(t, 0, "monthID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "monthID", e.to_string()))?;
        let month_vmt_fraction_col = df
            .column("monthVMTFraction")
            .map_err(|e| row_err(t, 0, "monthVMTFraction", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "monthVMTFraction", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(MonthVmtFractionRow {
                    source_type_id: source_type_id_col
                        .get(i)
                        .ok_or_else(|| null("sourceTypeID"))?
                        as i16,
                    month_id: month_id_col.get(i).ok_or_else(|| null("monthID"))? as i16,
                    month_vmt_fraction: month_vmt_fraction_col
                        .get(i)
                        .ok_or_else(|| null("monthVMTFraction"))?,
                })
            })
            .collect()
    }
}

/// One `dayVMTFraction` row — per `(sourceType, month, roadType, day)` share.
struct DayVmtFractionRow {
    source_type_id: i16,
    month_id: i16,
    road_type_id: i16,
    day_id: i16,
    day_vmt_fraction: f64,
}

impl TableRow for DayVmtFractionRow {
    fn table_name() -> &'static str {
        "dayVMTFraction"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
            ("dayVMTFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter()
                        .map(|r| r.source_type_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id as i32).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeID".into(),
                    rows.iter()
                        .map(|r| r.road_type_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "dayID".into(),
                    rows.iter().map(|r| r.day_id as i32).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "dayVMTFraction".into(),
                    rows.iter()
                        .map(|r| r.day_vmt_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "dayVMTFraction";
        let source_type_id_col = df
            .column("sourceTypeID")
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?;
        let month_id_col = df
            .column("monthID")
            .map_err(|e| row_err(t, 0, "monthID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "monthID", e.to_string()))?;
        let road_type_id_col = df
            .column("roadTypeID")
            .map_err(|e| row_err(t, 0, "roadTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "roadTypeID", e.to_string()))?;
        let day_id_col = df
            .column("dayID")
            .map_err(|e| row_err(t, 0, "dayID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "dayID", e.to_string()))?;
        let day_vmt_fraction_col = df
            .column("dayVMTFraction")
            .map_err(|e| row_err(t, 0, "dayVMTFraction", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "dayVMTFraction", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(DayVmtFractionRow {
                    source_type_id: source_type_id_col
                        .get(i)
                        .ok_or_else(|| null("sourceTypeID"))?
                        as i16,
                    month_id: month_id_col.get(i).ok_or_else(|| null("monthID"))? as i16,
                    road_type_id: road_type_id_col.get(i).ok_or_else(|| null("roadTypeID"))? as i16,
                    day_id: day_id_col.get(i).ok_or_else(|| null("dayID"))? as i16,
                    day_vmt_fraction: day_vmt_fraction_col
                        .get(i)
                        .ok_or_else(|| null("dayVMTFraction"))?,
                })
            })
            .collect()
    }
}

/// One `hourVMTFraction` row — per `(sourceType, roadType, day, hour)` share.
struct HourVmtFractionRow {
    source_type_id: i16,
    road_type_id: i16,
    day_id: i16,
    hour_id: i16,
    hour_vmt_fraction: f64,
}

impl TableRow for HourVmtFractionRow {
    fn table_name() -> &'static str {
        "hourVMTFraction"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
            ("hourVMTFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter()
                        .map(|r| r.source_type_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeID".into(),
                    rows.iter()
                        .map(|r| r.road_type_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "dayID".into(),
                    rows.iter().map(|r| r.day_id as i32).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourID".into(),
                    rows.iter().map(|r| r.hour_id as i32).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourVMTFraction".into(),
                    rows.iter()
                        .map(|r| r.hour_vmt_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "hourVMTFraction";
        let source_type_id_col = df
            .column("sourceTypeID")
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?;
        let road_type_id_col = df
            .column("roadTypeID")
            .map_err(|e| row_err(t, 0, "roadTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "roadTypeID", e.to_string()))?;
        let day_id_col = df
            .column("dayID")
            .map_err(|e| row_err(t, 0, "dayID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "dayID", e.to_string()))?;
        let hour_id_col = df
            .column("hourID")
            .map_err(|e| row_err(t, 0, "hourID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "hourID", e.to_string()))?;
        let hour_vmt_fraction_col = df
            .column("hourVMTFraction")
            .map_err(|e| row_err(t, 0, "hourVMTFraction", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "hourVMTFraction", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(HourVmtFractionRow {
                    source_type_id: source_type_id_col
                        .get(i)
                        .ok_or_else(|| null("sourceTypeID"))?
                        as i16,
                    road_type_id: road_type_id_col.get(i).ok_or_else(|| null("roadTypeID"))? as i16,
                    day_id: day_id_col.get(i).ok_or_else(|| null("dayID"))? as i16,
                    hour_id: hour_id_col.get(i).ok_or_else(|| null("hourID"))? as i16,
                    hour_vmt_fraction: hour_vmt_fraction_col
                        .get(i)
                        .ok_or_else(|| null("hourVMTFraction"))?,
                })
            })
            .collect()
    }
}

/// One `link` row — a modelled road link.
struct LinkRow {
    link_id: i32,
    road_type_id: i16,
}

impl TableRow for LinkRow {
    fn table_name() -> &'static str {
        "link"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([
            ("linkID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "linkID".into(),
                    rows.iter().map(|r| r.link_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeID".into(),
                    rows.iter()
                        .map(|r| r.road_type_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "link";
        let link_id_col = df
            .column("linkID")
            .map_err(|e| row_err(t, 0, "linkID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "linkID", e.to_string()))?;
        let road_type_id_col = df
            .column("roadTypeID")
            .map_err(|e| row_err(t, 0, "roadTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "roadTypeID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(LinkRow {
                    link_id: link_id_col.get(i).ok_or_else(|| null("linkID"))?,
                    road_type_id: road_type_id_col.get(i).ok_or_else(|| null("roadTypeID"))? as i16,
                })
            })
            .collect()
    }
}

/// One `linkAverageSpeed` row — average speed on a link (mph).
struct LinkAverageSpeedRow {
    link_id: i32,
    average_speed: f64,
}

impl TableRow for LinkAverageSpeedRow {
    fn table_name() -> &'static str {
        "linkAverageSpeed"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([
            ("linkID".into(), DataType::Int32),
            ("averageSpeed".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "linkID".into(),
                    rows.iter().map(|r| r.link_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "averageSpeed".into(),
                    rows.iter().map(|r| r.average_speed).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "linkAverageSpeed";
        let link_id_col = df
            .column("linkID")
            .map_err(|e| row_err(t, 0, "linkID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "linkID", e.to_string()))?;
        let average_speed_col = df
            .column("averageSpeed")
            .map_err(|e| row_err(t, 0, "averageSpeed", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "averageSpeed", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(LinkAverageSpeedRow {
                    link_id: link_id_col.get(i).ok_or_else(|| null("linkID"))?,
                    average_speed: average_speed_col
                        .get(i)
                        .ok_or_else(|| null("averageSpeed"))?,
                })
            })
            .collect()
    }
}

/// One `hourDay` row — packed `(hour, day)` catalogue entry.
struct HourDayRow {
    hour_day_id: i32,
    day_id: i16,
    hour_id: i16,
}

impl TableRow for HourDayRow {
    fn table_name() -> &'static str {
        "hourDay"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "hourDayID".into(),
                    rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "dayID".into(),
                    rows.iter().map(|r| r.day_id as i32).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourID".into(),
                    rows.iter().map(|r| r.hour_id as i32).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "hourDay";
        let hour_day_id_col = df
            .column("hourDayID")
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?;
        let day_id_col = df
            .column("dayID")
            .map_err(|e| row_err(t, 0, "dayID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "dayID", e.to_string()))?;
        let hour_id_col = df
            .column("hourID")
            .map_err(|e| row_err(t, 0, "hourID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "hourID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(HourDayRow {
                    hour_day_id: hour_day_id_col.get(i).ok_or_else(|| null("hourDayID"))?,
                    day_id: day_id_col.get(i).ok_or_else(|| null("dayID"))? as i16,
                    hour_id: hour_id_col.get(i).ok_or_else(|| null("hourID"))? as i16,
                })
            })
            .collect()
    }
}

// ── Output row types ──────────────────────────────────────────────────────────

/// One `SHO` output row — source-hours-operating allocated to a link and hour.
///
/// The mesoscale-lookup version uses `SHO = VMT` (the travel fraction, already
/// a proportional quantity); `distance = SHO × averageSpeed`.
pub struct ShoOutputRow {
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `monthID`.
    pub month_id: i16,
    /// `yearID`.
    pub year_id: i16,
    /// `ageID`.
    pub age_id: i16,
    /// `linkID`.
    pub link_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i16,
    /// `SHO` — source-hours operating (equals VMT for mesoscale lookup).
    pub sho: f64,
    /// `distance` — `SHO × averageSpeed`.
    pub distance: f64,
}

impl TableRow for ShoOutputRow {
    fn table_name() -> &'static str {
        "SHO"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("linkID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("SHO".into(), DataType::Float64),
            ("distance".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "hourDayID".into(),
                    rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id as i32).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id as i32).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id as i32).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "linkID".into(),
                    rows.iter().map(|r| r.link_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter()
                        .map(|r| r.source_type_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "SHO".into(),
                    rows.iter().map(|r| r.sho).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "distance".into(),
                    rows.iter().map(|r| r.distance).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SHO";
        let hour_day_id_col = df
            .column("hourDayID")
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?;
        let month_id_col = df
            .column("monthID")
            .map_err(|e| row_err(t, 0, "monthID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "monthID", e.to_string()))?;
        let year_id_col = df
            .column("yearID")
            .map_err(|e| row_err(t, 0, "yearID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "yearID", e.to_string()))?;
        let age_id_col = df
            .column("ageID")
            .map_err(|e| row_err(t, 0, "ageID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "ageID", e.to_string()))?;
        let link_id_col = df
            .column("linkID")
            .map_err(|e| row_err(t, 0, "linkID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "linkID", e.to_string()))?;
        let source_type_id_col = df
            .column("sourceTypeID")
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?;
        let sho_col = df
            .column("SHO")
            .map_err(|e| row_err(t, 0, "SHO", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "SHO", e.to_string()))?;
        let distance_col = df
            .column("distance")
            .map_err(|e| row_err(t, 0, "distance", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "distance", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(ShoOutputRow {
                    hour_day_id: hour_day_id_col.get(i).ok_or_else(|| null("hourDayID"))?,
                    month_id: month_id_col.get(i).ok_or_else(|| null("monthID"))? as i16,
                    year_id: year_id_col.get(i).ok_or_else(|| null("yearID"))? as i16,
                    age_id: age_id_col.get(i).ok_or_else(|| null("ageID"))? as i16,
                    link_id: link_id_col.get(i).ok_or_else(|| null("linkID"))?,
                    source_type_id: source_type_id_col
                        .get(i)
                        .ok_or_else(|| null("sourceTypeID"))?
                        as i16,
                    sho: sho_col.get(i).ok_or_else(|| null("SHO"))?,
                    distance: distance_col.get(i).ok_or_else(|| null("distance"))?,
                })
            })
            .collect()
    }
}

/// One `SourceHours` output row — total source-hours allocated to a link and hour.
///
/// For mesoscale-lookup runs `sourceHours = SHO` (the proportional identity).
pub struct SourceHoursOutputRow {
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `monthID`.
    pub month_id: i16,
    /// `yearID`.
    pub year_id: i16,
    /// `ageID`.
    pub age_id: i16,
    /// `linkID`.
    pub link_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i16,
    /// `sourceHours`.
    pub source_hours: f64,
}

impl TableRow for SourceHoursOutputRow {
    fn table_name() -> &'static str {
        "SourceHours"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("linkID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("sourceHours".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "hourDayID".into(),
                    rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id as i32).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id as i32).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id as i32).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "linkID".into(),
                    rows.iter().map(|r| r.link_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter()
                        .map(|r| r.source_type_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceHours".into(),
                    rows.iter().map(|r| r.source_hours).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SourceHours";
        let hour_day_id_col = df
            .column("hourDayID")
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?;
        let month_id_col = df
            .column("monthID")
            .map_err(|e| row_err(t, 0, "monthID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "monthID", e.to_string()))?;
        let year_id_col = df
            .column("yearID")
            .map_err(|e| row_err(t, 0, "yearID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "yearID", e.to_string()))?;
        let age_id_col = df
            .column("ageID")
            .map_err(|e| row_err(t, 0, "ageID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "ageID", e.to_string()))?;
        let link_id_col = df
            .column("linkID")
            .map_err(|e| row_err(t, 0, "linkID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "linkID", e.to_string()))?;
        let source_type_id_col = df
            .column("sourceTypeID")
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?;
        let source_hours_col = df
            .column("sourceHours")
            .map_err(|e| row_err(t, 0, "sourceHours", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "sourceHours", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SourceHoursOutputRow {
                    hour_day_id: hour_day_id_col.get(i).ok_or_else(|| null("hourDayID"))?,
                    month_id: month_id_col.get(i).ok_or_else(|| null("monthID"))? as i16,
                    year_id: year_id_col.get(i).ok_or_else(|| null("yearID"))? as i16,
                    age_id: age_id_col.get(i).ok_or_else(|| null("ageID"))? as i16,
                    link_id: link_id_col.get(i).ok_or_else(|| null("linkID"))?,
                    source_type_id: source_type_id_col
                        .get(i)
                        .ok_or_else(|| null("sourceTypeID"))?
                        as i16,
                    source_hours: source_hours_col.get(i).ok_or_else(|| null("sourceHours"))?,
                })
            })
            .collect()
    }
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
    /// Reads the input tables from `ctx.tables()`, runs the Tag-0…Tag-9
    /// pipeline for the current analysis year, and writes the `SHO` and
    /// `SourceHours` activity tables to `ctx.scratch()`.
    ///
    /// For mesoscale-lookup runs `SHO = VMT` (the travel fraction) and
    /// `sourceHours = SHO`; `distance = SHO × averageSpeed`.  Apportionment
    /// to months/days/hours uses `monthVMTFraction × dayVMTFraction ×
    /// hourVMTFraction / 1` (weeks-in-month defaults to 1 because
    /// `monthOfAnyYear` is not in the generator's input table set).  Links
    /// are iterated uniformly; each link's road type drives the day/hour
    /// fraction lookup.
    fn execute(&self, ctx: &mut CalculatorContext) -> Result<CalculatorOutput, Error> {
        // ── position ──────────────────────────────────────────────────────────
        let year_id = ctx
            .position()
            .time
            .year
            .ok_or_else(|| Error::Polars("no year in iteration position".into()))?
            as i16;

        // ── read input tables ─────────────────────────────────────────────────
        let years: Vec<YearRow> = ctx.tables().iter_typed("year")?;
        let source_type_years: Vec<SourceTypeYearRow> =
            ctx.tables().iter_typed("sourceTypeYear")?;
        let age_distribution: Vec<SourceTypeAgeDistributionRow> =
            ctx.tables().iter_typed("sourceTypeAgeDistribution")?;
        let source_type_ages: Vec<SourceTypeAgeRow> = ctx.tables().iter_typed("sourceTypeAge")?;
        let source_use_types: Vec<SourceUseTypeRow> = ctx.tables().iter_typed("sourceUseType")?;
        let month_vmt_fractions: Vec<MonthVmtFractionRow> =
            ctx.tables().iter_typed("monthVMTFraction")?;
        let day_vmt_fractions: Vec<DayVmtFractionRow> =
            ctx.tables().iter_typed("dayVMTFraction")?;
        let hour_vmt_fractions: Vec<HourVmtFractionRow> =
            ctx.tables().iter_typed("hourVMTFraction")?;
        let links: Vec<LinkRow> = ctx.tables().iter_typed("link")?;
        let link_avg_speeds: Vec<LinkAverageSpeedRow> =
            ctx.tables().iter_typed("linkAverageSpeed")?;
        let hour_days: Vec<HourDayRow> = ctx.tables().iter_typed("hourDay")?;

        // ── Tag-0…Tag-3: travel fractions ─────────────────────────────────────
        let travel_fracs = match total_activity_basis(
            year_id,
            &years,
            &source_type_years,
            &age_distribution,
            &source_type_ages,
            &source_use_types,
        ) {
            Some(v) => v,
            None => {
                // No qualifying base year — write empty tables and return.
                let empty_sho: Vec<ShoOutputRow> = Vec::new();
                crate::wiring::write_scratch_table(ctx, OUTPUT_TABLES[0], empty_sho)?;
                let empty_sh: Vec<SourceHoursOutputRow> = Vec::new();
                let df = empty_sh
                    .into_dataframe()
                    .map_err(|e| Error::Polars(e.to_string()))?;
                ctx.scratch_mut().insert(OUTPUT_TABLES[1], df);
                return Ok(CalculatorOutput::empty());
            }
        };

        // ── lookup maps ───────────────────────────────────────────────────────
        // (sourceTypeID, monthID) -> monthVMTFraction
        let month_frac: BTreeMap<(i16, i16), f64> = month_vmt_fractions
            .iter()
            .map(|r| ((r.source_type_id, r.month_id), r.month_vmt_fraction))
            .collect();
        // (sourceTypeID, monthID, roadTypeID, dayID) -> dayVMTFraction
        let day_frac: BTreeMap<(i16, i16, i16, i16), f64> = day_vmt_fractions
            .iter()
            .map(|r| {
                (
                    (r.source_type_id, r.month_id, r.road_type_id, r.day_id),
                    r.day_vmt_fraction,
                )
            })
            .collect();
        // (sourceTypeID, roadTypeID, dayID, hourID) -> hourVMTFraction
        let hour_frac: BTreeMap<(i16, i16, i16, i16), f64> = hour_vmt_fractions
            .iter()
            .map(|r| {
                (
                    (r.source_type_id, r.road_type_id, r.day_id, r.hour_id),
                    r.hour_vmt_fraction,
                )
            })
            .collect();
        // linkID -> averageSpeed
        let avg_speed: BTreeMap<i32, f64> = link_avg_speeds
            .iter()
            .map(|r| (r.link_id, r.average_speed))
            .collect();
        // hourDayID -> (dayID, hourID)
        let _hour_day_map: BTreeMap<i32, (i16, i16)> = hour_days
            .iter()
            .map(|r| (r.hour_day_id, (r.day_id, r.hour_id)))
            .collect();
        // collect distinct (monthID, dayID, hourID, hourDayID) combinations
        let mut month_day_hour_hd: Vec<(i16, i16, i16, i32)> = {
            // monthIDs from month_vmt_fractions; dayID/hourID/hourDayID from hourDay
            let months: std::collections::BTreeSet<i16> =
                month_vmt_fractions.iter().map(|r| r.month_id).collect();
            hour_days
                .iter()
                .flat_map(|hd| {
                    months
                        .iter()
                        .map(move |&m| (m, hd.day_id, hd.hour_id, hd.hour_day_id))
                })
                .collect()
        };
        month_day_hour_hd.sort_unstable();
        month_day_hour_hd.dedup();

        // ── Tag-5…Tag-7/Tag-9: apportion and build output rows ────────────────
        // weeks_in_month defaults to 1 (Java WeeksInMonthHelper fallback for
        // unknown months, since MonthOfAnyYear is not in INPUT_TABLES).
        let weeks_in_month = 1.0_f64;

        let mut sho_rows: Vec<ShoOutputRow> = Vec::new();
        let mut source_hours_rows: Vec<SourceHoursOutputRow> = Vec::new();

        for link in &links {
            let speed = avg_speed.get(&link.link_id).copied().unwrap_or(0.0);
            let road_type_id = link.road_type_id;

            for tf in &travel_fracs {
                let source_type_id = tf.source_type_id;
                let age_id = tf.age_id;

                for &(month_id, day_id, hour_id, hour_day_id) in &month_day_hour_hd {
                    let m_frac = month_frac
                        .get(&(source_type_id, month_id))
                        .copied()
                        .unwrap_or(0.0);
                    let d_frac = day_frac
                        .get(&(source_type_id, month_id, road_type_id, day_id))
                        .copied()
                        .unwrap_or(0.0);
                    let h_frac = hour_frac
                        .get(&(source_type_id, road_type_id, day_id, hour_id))
                        .copied()
                        .unwrap_or(0.0);

                    // Tag-5/6: apportion travel fraction to this hour.
                    let sho =
                        apportion_to_hour(tf.fraction, m_frac, d_frac, h_frac, weeks_in_month);
                    // Tag-9: distance = SHO × averageSpeed.
                    let distance = link_distance(sho, speed);

                    sho_rows.push(ShoOutputRow {
                        hour_day_id,
                        month_id,
                        year_id,
                        age_id,
                        link_id: link.link_id,
                        source_type_id,
                        sho,
                        distance,
                    });
                    // SourceHours = SHO (mesoscale-lookup identity).
                    source_hours_rows.push(SourceHoursOutputRow {
                        hour_day_id,
                        month_id,
                        year_id,
                        age_id,
                        link_id: link.link_id,
                        source_type_id,
                        source_hours: sho,
                    });
                }
            }
        }

        // ── write scratch tables ──────────────────────────────────────────────
        crate::wiring::write_scratch_table(ctx, OUTPUT_TABLES[0], sho_rows)?;
        let sh_df = source_hours_rows
            .into_dataframe()
            .map_err(|e| Error::Polars(e.to_string()))?;
        ctx.scratch_mut().insert(OUTPUT_TABLES[1], sh_df);
        Ok(CalculatorOutput::empty())
    }
}

/// Factory function for `CalculatorRegistry::register_generator`.
pub fn factory() -> Box<dyn Generator> {
    Box::new(MesoscaleLookupTotalActivityGenerator::new())
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
    fn generator_execute_errors_without_year_in_position() {
        // execute() requires a year in the iteration position; an empty
        // context (no year set) should return an error, not panic.
        let gen = MesoscaleLookupTotalActivityGenerator::new();
        let mut ctx = CalculatorContext::new();
        assert!(gen.execute(&mut ctx).is_err());
    }

    #[test]
    fn generator_is_object_safe() {
        let gen: Box<dyn Generator> = Box::new(MesoscaleLookupTotalActivityGenerator::new());
        assert_eq!(gen.name(), "MesoscaleLookupTotalActivityGenerator");
    }

    #[test]
    fn execute_writes_sho_and_source_hours_to_scratch() {
        use moves_framework::{
            DataFrameStore, DataFrameStoreTyped, ExecutionTime, InMemoryStore, IterationPosition,
        };

        // One source type, one age, one base year.
        let year_id: i16 = 2020;

        let mut store = InMemoryStore::default();

        // year table — 2020 is the base year.
        store.insert(
            "year",
            YearRow::into_dataframe(vec![YearRow {
                year_id,
                is_base_year: true,
            }])
            .unwrap(),
        );

        // sourceTypeYear — source type 21, population 1000.
        store.insert(
            "sourceTypeYear",
            SourceTypeYearRow::into_dataframe(vec![SourceTypeYearRow {
                year_id,
                source_type_id: 21,
                source_type_population: 1000.0,
                sales_growth_factor: 1.0,
                migration_rate: 1.0,
            }])
            .unwrap(),
        );

        // sourceTypeAgeDistribution — 100% in age 0.
        store.insert(
            "sourceTypeAgeDistribution",
            SourceTypeAgeDistributionRow::into_dataframe(vec![SourceTypeAgeDistributionRow {
                year_id,
                source_type_id: 21,
                age_id: 0,
                age_fraction: 1.0,
            }])
            .unwrap(),
        );

        // sourceTypeAge — relativeMAR = 1.
        store.insert(
            "sourceTypeAge",
            SourceTypeAgeRow::into_dataframe(vec![SourceTypeAgeRow {
                source_type_id: 21,
                age_id: 0,
                survival_rate: 1.0,
                relative_mar: 1.0,
            }])
            .unwrap(),
        );

        // sourceUseType — source type 21 maps to HPMS type 10.
        store.insert(
            "sourceUseType",
            SourceUseTypeRow::into_dataframe(vec![SourceUseTypeRow {
                source_type_id: 21,
                hpms_v_type_id: 10,
            }])
            .unwrap(),
        );

        // monthVMTFraction — month 1 gets all the VMT.
        store.insert(
            "monthVMTFraction",
            MonthVmtFractionRow::into_dataframe(vec![MonthVmtFractionRow {
                source_type_id: 21,
                month_id: 1,
                month_vmt_fraction: 1.0,
            }])
            .unwrap(),
        );

        // dayVMTFraction — road type 2, day 5 gets all the VMT.
        store.insert(
            "dayVMTFraction",
            DayVmtFractionRow::into_dataframe(vec![DayVmtFractionRow {
                source_type_id: 21,
                month_id: 1,
                road_type_id: 2,
                day_id: 5,
                day_vmt_fraction: 1.0,
            }])
            .unwrap(),
        );

        // hourVMTFraction — road type 2, day 5, hour 8 gets all the VMT.
        store.insert(
            "hourVMTFraction",
            HourVmtFractionRow::into_dataframe(vec![HourVmtFractionRow {
                source_type_id: 21,
                road_type_id: 2,
                day_id: 5,
                hour_id: 8,
                hour_vmt_fraction: 1.0,
            }])
            .unwrap(),
        );

        // link — one link on road type 2.
        store.insert(
            "link",
            LinkRow::into_dataframe(vec![LinkRow {
                link_id: 101,
                road_type_id: 2,
            }])
            .unwrap(),
        );

        // linkAverageSpeed — link 101 average speed 55 mph.
        store.insert(
            "linkAverageSpeed",
            LinkAverageSpeedRow::into_dataframe(vec![LinkAverageSpeedRow {
                link_id: 101,
                average_speed: 55.0,
            }])
            .unwrap(),
        );

        // hourDay — hour 8, day 5 → hourDayID = 85.
        store.insert(
            "hourDay",
            HourDayRow::into_dataframe(vec![HourDayRow {
                hour_day_id: 85,
                day_id: 5,
                hour_id: 8,
            }])
            .unwrap(),
        );

        // roadType — not consumed by execute directly, but declared in INPUT_TABLES.
        // Leave absent; iter_typed will return an empty vec for missing tables.

        let position = IterationPosition {
            iteration: 0,
            process_id: None,
            location: moves_framework::ExecutionLocation::none(),
            time: ExecutionTime {
                year: Some(year_id as u16),
                month: None,
                day_id: None,
                hour: None,
            },
        };

        let gen = MesoscaleLookupTotalActivityGenerator::new();
        let mut ctx = CalculatorContext::with_position_and_tables(position, store);
        gen.execute(&mut ctx).unwrap();

        // ── verify SHO table ──────────────────────────────────────────────────
        // travel fraction = 1.0 (single cell, all HPMS type's travel).
        // SHO = 1.0 × 1.0 × 1.0 × 1.0 / 1.0 = 1.0.
        // distance = 1.0 × 55.0 = 55.0.
        let sho_out: Vec<ShoOutputRow> = ctx.scratch().store.iter_typed("SHO").unwrap();
        assert_eq!(sho_out.len(), 1, "SHO table should have one row");
        let sho_row = &sho_out[0];
        assert_eq!(sho_row.source_type_id, 21);
        assert_eq!(sho_row.age_id, 0);
        assert_eq!(sho_row.link_id, 101);
        assert_eq!(sho_row.year_id, year_id);
        assert_eq!(sho_row.month_id, 1);
        assert_eq!(sho_row.hour_day_id, 85);
        assert!(
            (sho_row.sho - 1.0).abs() < 1e-12,
            "SHO = fraction × fracs / weeks"
        );
        assert!(
            (sho_row.distance - 55.0).abs() < 1e-12,
            "distance = SHO × avgSpeed"
        );

        // ── verify SourceHours table ──────────────────────────────────────────
        // sourceHours = SHO (mesoscale-lookup identity).
        let sh_out: Vec<SourceHoursOutputRow> =
            ctx.scratch().store.iter_typed("SourceHours").unwrap();
        assert_eq!(sh_out.len(), 1, "SourceHours table should have one row");
        let sh_row = &sh_out[0];
        assert_eq!(sh_row.source_type_id, 21);
        assert_eq!(sh_row.link_id, 101);
        assert!(
            (sh_row.source_hours - 1.0).abs() < 1e-12,
            "sourceHours = SHO"
        );
    }
}

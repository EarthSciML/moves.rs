//! Population growth — algorithm steps 110-139.
//!
//! Ports `TotalActivityGenerator.java`'s `determineBaseYear`,
//! `calculateBaseYearPopulation`, and `growPopulationToAnalysisYear`.
//!
//! The chain finds the base year nearest the analysis year, splits its
//! `SourceTypeYear` population across ages, then walks the population one
//! calendar year at a time — ageing every cohort by survival and migration
//! and seeding age 0 from sales growth — until the analysis year is reached.
//! A by-product is the analysis-year `SourceTypeAgeDistribution`.

use std::collections::BTreeMap;

use super::inputs::{SourceTypeAgeDistributionRow, SourceTypeAgeRow, SourceTypeYearRow, YearRow};
use super::model::SourceTypeAgePopulationRow;

/// The oldest age cohort. The Java grows ages `1..=39` from the cohort one
/// year younger, then folds ages 39 and 40 of the prior year into age 40 —
/// so age 40 is the terminal "40 and older" bucket.
const MAX_AGE_ID: i32 = 40;

/// Step 110 — find the base year nearest (at or below) the analysis year.
///
/// Ports `determineBaseYear`:
/// `SELECT MAX(yearId) FROM Year WHERE yearId <= ? AND isBaseYear IN ('Y','y')`.
///
/// Returns [`None`] when no base year is at or below `analysis_year`; the
/// Java throws in that case and `executeLoop` logs and abandons the year, so
/// callers treat [`None`] as "produce no population for this year".
#[must_use]
pub fn determine_base_year(years: &[YearRow], analysis_year: i32) -> Option<i32> {
    years
        .iter()
        .filter(|y| y.is_base_year && y.year_id <= analysis_year)
        .map(|y| y.year_id)
        .max()
}

/// Step 120 — split the base year's `SourceTypeYear` population across ages.
///
/// Ports `calculateBaseYearPopulation`:
/// `population = SourceTypeYear.sourceTypePopulation *
/// SourceTypeAgeDistribution.ageFraction`, joined on `(sourceTypeID, yearID)`
/// with `yearID = baseYear`.
///
/// `SourceTypeYear`'s primary key is `(yearID, sourceTypeID)`, so each
/// `SourceTypeAgeDistribution` row at the base year contributes exactly one
/// output row. A distribution row whose source type has no `SourceTypeYear`
/// entry at the base year is dropped by the inner join.
#[must_use]
pub fn calculate_base_year_population(
    source_type_year: &[SourceTypeYearRow],
    source_type_age_distribution: &[SourceTypeAgeDistributionRow],
    base_year: i32,
) -> Vec<SourceTypeAgePopulationRow> {
    let population_of: BTreeMap<i32, f64> = source_type_year
        .iter()
        .filter(|r| r.year_id == base_year)
        .map(|r| (r.source_type_id, r.source_type_population))
        .collect();

    let mut out = Vec::new();
    for stad in source_type_age_distribution {
        if stad.year_id != base_year {
            continue;
        }
        let Some(&source_type_population) = population_of.get(&stad.source_type_id) else {
            continue;
        };
        out.push(SourceTypeAgePopulationRow {
            year_id: base_year,
            source_type_id: stad.source_type_id,
            age_id: stad.age_id,
            population: source_type_population * stad.age_fraction,
        });
    }
    out.sort_by_key(|r| (r.year_id, r.source_type_id, r.age_id));
    out
}

/// What [`grow_population_to_analysis_year`] produces.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GrownPopulation {
    /// `SourceTypeAgePopulation` for every year in `base_year..=analysis_year`
    /// — the base-year rows passed in plus every grown year.
    pub population: Vec<SourceTypeAgePopulationRow>,
    /// `SourceTypeAgeDistribution` rows the Java `INSERT IGNORE`s for the
    /// analysis year (only keys absent from the existing table).
    pub age_distribution_additions: Vec<SourceTypeAgeDistributionRow>,
}

/// Step 130 — grow the population from the base year to the analysis year.
///
/// Ports `growPopulationToAnalysisYear`. Starting from `base_year_population`
/// (step 120's output), each successive year `y` is built from year `y-1`:
///
/// * **age 0** — `population[0,y] = (population[0,y-1] / migrationRate[y-1]) * salesGrowthFactor[y] * migrationRate[y]`.
///   Dropped when `migrationRate[y-1]` is `0` (the Java
///   `sty.migrationRate <> 0` guard avoids the division).
/// * **ages 1-39** — `population[a,y] = population[a-1,y-1] * survivalRate[a] * migrationRate[y]`.
/// * **age 40** — `population[40,y] = population[39,y-1] * survivalRate[39] * migrationRate[y] + population[40,y-1] * survivalRate[40] * migrationRate[y]`,
///   emitted only when both `population[39,y-1]` and `population[40,y-1]`
///   exist.
///
/// `migrationRate` and `salesGrowthFactor` come from `SourceTypeYear`
/// keyed `(yearID, sourceTypeID)`; `survivalRate` from `SourceTypeAge` keyed
/// `(sourceTypeID, ageID)`.
///
/// Once the analysis year is reached, the analysis-year
/// `SourceTypeAgeDistribution` is rebuilt: `ageFraction = population /
/// sum(population over ages)` per source type, `INSERT IGNORE`d against
/// `existing_age_distribution` (a source type whose summed population is `0`
/// contributes no rows — the Java inner join to a zero total yields a
/// `NULL` fraction that the `NOT NULL` column rejects).
#[must_use]
pub fn grow_population_to_analysis_year(
    base_year_population: &[SourceTypeAgePopulationRow],
    source_type_year: &[SourceTypeYearRow],
    source_type_age: &[SourceTypeAgeRow],
    existing_age_distribution: &[SourceTypeAgeDistributionRow],
    base_year: i32,
    analysis_year: i32,
) -> GrownPopulation {
    // (yearID, sourceTypeID) -> (migrationRate, salesGrowthFactor).
    let year_factors: BTreeMap<(i32, i32), (f64, f64)> = source_type_year
        .iter()
        .map(|r| {
            (
                (r.year_id, r.source_type_id),
                (r.migration_rate, r.sales_growth_factor),
            )
        })
        .collect();
    // (sourceTypeID, ageID) -> survivalRate.
    let survival_rate: BTreeMap<(i32, i32), f64> = source_type_age
        .iter()
        .map(|r| ((r.source_type_id, r.age_id), r.survival_rate))
        .collect();

    // (yearID, sourceTypeID, ageID) -> population. Seeded with the base year.
    let mut population: BTreeMap<(i32, i32, i32), f64> = base_year_population
        .iter()
        .map(|r| ((r.year_id, r.source_type_id, r.age_id), r.population))
        .collect();
    // Source types that have a base-year population — the universe to grow.
    let source_types: Vec<i32> = {
        let mut s: Vec<i32> = base_year_population
            .iter()
            .map(|r| r.source_type_id)
            .collect();
        s.sort_unstable();
        s.dedup();
        s
    };

    for y in (base_year + 1)..=analysis_year {
        // Each year is computed entirely from year y-1, so the new rows can
        // be staged and merged after — exactly as the Java stages them in
        // SourceTypeAgePopulation2 before copying into SourceTypeAgePopulation.
        let mut staged: Vec<((i32, i32, i32), f64)> = Vec::new();
        for &st in &source_types {
            let Some(&(migration_rate, sales_growth_factor)) = year_factors.get(&(y, st)) else {
                continue;
            };

            // Age 0 — sales growth.
            if let (Some(&prior_mr), Some(&pop_prev)) = (
                year_factors.get(&(y - 1, st)).map(|(mr, _)| mr),
                population.get(&(y - 1, st, 0)),
            ) {
                if prior_mr != 0.0 {
                    let pop = (pop_prev / prior_mr) * sales_growth_factor * migration_rate;
                    staged.push(((y, st, 0), pop));
                }
            }

            // Ages 1-39 — survival of the next-younger cohort.
            for age in 1..MAX_AGE_ID {
                let (Some(&survival), Some(&pop_prev)) = (
                    survival_rate.get(&(st, age)),
                    population.get(&(y - 1, st, age - 1)),
                ) else {
                    continue;
                };
                staged.push(((y, st, age), pop_prev * survival * migration_rate));
            }

            // Age 40 — survivors of age 39 plus survivors already at age 40.
            if let (Some(&sr39), Some(&sr40), Some(&pop39), Some(&pop40)) = (
                survival_rate.get(&(st, MAX_AGE_ID - 1)),
                survival_rate.get(&(st, MAX_AGE_ID)),
                population.get(&(y - 1, st, MAX_AGE_ID - 1)),
                population.get(&(y - 1, st, MAX_AGE_ID)),
            ) {
                let pop = pop39 * sr39 * migration_rate + pop40 * sr40 * migration_rate;
                staged.push(((y, st, MAX_AGE_ID), pop));
            }
        }
        population.extend(staged);
    }

    // Analysis-year SourceTypeAgeDistribution rebuild.
    let mut total_by_source_type: BTreeMap<i32, f64> = BTreeMap::new();
    for (&(year, st, _age), &pop) in &population {
        if year == analysis_year {
            *total_by_source_type.entry(st).or_insert(0.0) += pop;
        }
    }
    let existing: BTreeMap<(i32, i32, i32), ()> = existing_age_distribution
        .iter()
        .map(|r| ((r.source_type_id, r.year_id, r.age_id), ()))
        .collect();
    let mut age_distribution_additions = Vec::new();
    for (&(year, st, age), &pop) in &population {
        if year != analysis_year {
            continue;
        }
        let Some(&total) = total_by_source_type.get(&st) else {
            continue;
        };
        if total == 0.0 {
            continue;
        }
        if existing.contains_key(&(st, year, age)) {
            continue;
        }
        age_distribution_additions.push(SourceTypeAgeDistributionRow {
            source_type_id: st,
            year_id: year,
            age_id: age,
            age_fraction: pop / total,
        });
    }

    let population = population
        .into_iter()
        .map(
            |((year_id, source_type_id, age_id), population)| SourceTypeAgePopulationRow {
                year_id,
                source_type_id,
                age_id,
                population,
            },
        )
        .collect();

    GrownPopulation {
        population,
        age_distribution_additions,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tolerance for the population products under test.
    const EPS: f64 = 1e-9;

    fn year(year_id: i32, is_base: bool) -> YearRow {
        YearRow {
            year_id,
            is_base_year: is_base,
        }
    }

    fn sty(
        year_id: i32,
        source_type_id: i32,
        population: f64,
        migration_rate: f64,
        sales_growth_factor: f64,
    ) -> SourceTypeYearRow {
        SourceTypeYearRow {
            year_id,
            source_type_id,
            source_type_population: population,
            migration_rate,
            sales_growth_factor,
        }
    }

    fn stad(
        source_type_id: i32,
        year_id: i32,
        age_id: i32,
        age_fraction: f64,
    ) -> SourceTypeAgeDistributionRow {
        SourceTypeAgeDistributionRow {
            source_type_id,
            year_id,
            age_id,
            age_fraction,
        }
    }

    fn sta(source_type_id: i32, age_id: i32, survival_rate: f64) -> SourceTypeAgeRow {
        SourceTypeAgeRow {
            source_type_id,
            age_id,
            survival_rate,
            relative_mar: 1.0,
        }
    }

    fn pop_at(rows: &[SourceTypeAgePopulationRow], year: i32, st: i32, age: i32) -> Option<f64> {
        rows.iter()
            .find(|r| r.year_id == year && r.source_type_id == st && r.age_id == age)
            .map(|r| r.population)
    }

    #[test]
    fn base_year_is_nearest_at_or_below_analysis_year() {
        let years = [
            year(2010, true),
            year(2015, true),
            year(2018, false),
            year(2020, true),
            year(2025, true),
        ];
        // 2020 is a base year and the closest at or below 2022.
        assert_eq!(determine_base_year(&years, 2022), Some(2020));
        // Exact hit on a base year.
        assert_eq!(determine_base_year(&years, 2015), Some(2015));
        // 2018 is not a base year — fall back to 2015.
        assert_eq!(determine_base_year(&years, 2019), Some(2015));
    }

    #[test]
    fn no_base_year_below_analysis_year_is_none() {
        let years = [year(2030, true)];
        assert_eq!(determine_base_year(&years, 2020), None);
    }

    #[test]
    fn base_year_population_is_population_times_age_fraction() {
        let stys = [
            sty(2020, 21, 1000.0, 1.0, 1.0),
            sty(2020, 31, 400.0, 1.0, 1.0),
        ];
        let stads = [
            stad(21, 2020, 0, 0.25),
            stad(21, 2020, 1, 0.75),
            stad(31, 2020, 0, 1.0),
            // A different year — must be ignored.
            stad(21, 2019, 0, 1.0),
        ];
        let out = calculate_base_year_population(&stys, &stads, 2020);
        assert_eq!(out.len(), 3);
        assert!((pop_at(&out, 2020, 21, 0).unwrap() - 250.0).abs() < EPS);
        assert!((pop_at(&out, 2020, 21, 1).unwrap() - 750.0).abs() < EPS);
        assert!((pop_at(&out, 2020, 31, 0).unwrap() - 400.0).abs() < EPS);
    }

    #[test]
    fn base_year_population_drops_age_distribution_without_source_type_year() {
        // Source type 99 has an age distribution but no SourceTypeYear row.
        let stys = [sty(2020, 21, 1000.0, 1.0, 1.0)];
        let stads = [stad(21, 2020, 0, 1.0), stad(99, 2020, 0, 1.0)];
        let out = calculate_base_year_population(&stys, &stads, 2020);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].source_type_id, 21);
    }

    #[test]
    fn grow_one_year_ages_cohorts_by_survival_and_migration() {
        // Base year 2020: source type 21, ages 0, 1, 2.
        let base = [
            SourceTypeAgePopulationRow {
                year_id: 2020,
                source_type_id: 21,
                age_id: 0,
                population: 100.0,
            },
            SourceTypeAgePopulationRow {
                year_id: 2020,
                source_type_id: 21,
                age_id: 1,
                population: 80.0,
            },
            SourceTypeAgePopulationRow {
                year_id: 2020,
                source_type_id: 21,
                age_id: 2,
                population: 60.0,
            },
        ];
        // migrationRate 2020 = 1.0, 2021 = 1.1; salesGrowthFactor 2021 = 1.5.
        let stys = [sty(2020, 21, 0.0, 1.0, 1.0), sty(2021, 21, 0.0, 1.1, 1.5)];
        // survivalRate: age 1 = 0.9, age 2 = 0.8, age 3 = 0.7.
        let stas = [sta(21, 1, 0.9), sta(21, 2, 0.8), sta(21, 3, 0.7)];
        let grown = grow_population_to_analysis_year(&base, &stys, &stas, &[], 2020, 2021);

        // age 0, 2021 = (100/1.0) * 1.5 * 1.1 = 165.
        assert!((pop_at(&grown.population, 2021, 21, 0).unwrap() - 165.0).abs() < EPS);
        // age 1, 2021 = pop[0,2020] * survival[1] * migration[2021]
        //             = 100 * 0.9 * 1.1 = 99.
        assert!((pop_at(&grown.population, 2021, 21, 1).unwrap() - 99.0).abs() < EPS);
        // age 2, 2021 = 80 * 0.8 * 1.1 = 70.4.
        assert!((pop_at(&grown.population, 2021, 21, 2).unwrap() - 70.4).abs() < EPS);
        // age 3, 2021 = 60 * 0.7 * 1.1 = 46.2.
        assert!((pop_at(&grown.population, 2021, 21, 3).unwrap() - 46.2).abs() < EPS);
        // The base-year rows are carried through unchanged.
        assert!((pop_at(&grown.population, 2020, 21, 0).unwrap() - 100.0).abs() < EPS);
    }

    #[test]
    fn grow_age_zero_dropped_when_prior_migration_rate_is_zero() {
        let base = [SourceTypeAgePopulationRow {
            year_id: 2020,
            source_type_id: 21,
            age_id: 0,
            population: 100.0,
        }];
        // 2020 migrationRate is 0 — the age-0 division is skipped.
        let stys = [sty(2020, 21, 0.0, 0.0, 1.0), sty(2021, 21, 0.0, 1.0, 1.0)];
        let grown = grow_population_to_analysis_year(&base, &stys, &[], &[], 2020, 2021);
        assert_eq!(pop_at(&grown.population, 2021, 21, 0), None);
    }

    #[test]
    fn grow_age_forty_folds_two_cohorts() {
        // Base year ages 39 and 40 present.
        let base = [
            SourceTypeAgePopulationRow {
                year_id: 2020,
                source_type_id: 21,
                age_id: 39,
                population: 10.0,
            },
            SourceTypeAgePopulationRow {
                year_id: 2020,
                source_type_id: 21,
                age_id: 40,
                population: 5.0,
            },
        ];
        let stys = [sty(2020, 21, 0.0, 1.0, 1.0), sty(2021, 21, 0.0, 1.0, 1.0)];
        let stas = [sta(21, 39, 0.5), sta(21, 40, 0.4)];
        let grown = grow_population_to_analysis_year(&base, &stys, &stas, &[], 2020, 2021);
        // age 40, 2021 = 10 * 0.5 * 1.0 + 5 * 0.4 * 1.0 = 5 + 2 = 7.
        assert!((pop_at(&grown.population, 2021, 21, 40).unwrap() - 7.0).abs() < EPS);
    }

    #[test]
    fn analysis_year_age_distribution_is_population_share() {
        let base = [
            SourceTypeAgePopulationRow {
                year_id: 2020,
                source_type_id: 21,
                age_id: 0,
                population: 30.0,
            },
            SourceTypeAgePopulationRow {
                year_id: 2020,
                source_type_id: 21,
                age_id: 1,
                population: 70.0,
            },
        ];
        // analysis year == base year: no growth, distribution built directly.
        let grown = grow_population_to_analysis_year(&base, &[], &[], &[], 2020, 2020);
        let frac = |age| {
            grown
                .age_distribution_additions
                .iter()
                .find(|r| r.age_id == age)
                .map(|r| r.age_fraction)
        };
        assert!((frac(0).unwrap() - 0.30).abs() < EPS);
        assert!((frac(1).unwrap() - 0.70).abs() < EPS);
    }

    #[test]
    fn analysis_year_age_distribution_skips_existing_keys() {
        let base = [SourceTypeAgePopulationRow {
            year_id: 2020,
            source_type_id: 21,
            age_id: 0,
            population: 100.0,
        }];
        // age 0 is already present — INSERT IGNORE keeps the existing row.
        let existing = [stad(21, 2020, 0, 0.123)];
        let grown = grow_population_to_analysis_year(&base, &[], &[], &existing, 2020, 2020);
        assert!(grown.age_distribution_additions.is_empty());
    }
}

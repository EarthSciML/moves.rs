//! HPMS travel fraction and VMT growth — algorithm steps 140-159.
//!
//! Ports `TotalActivityGenerator.java`'s `calculateFractionOfTravelUsingHPMS`
//! and `growVMTToAnalysisYear`.
//!
//! Step 140 derives, per `(sourceType, age)`, the fraction of an HPMS
//! vehicle type's travel that cohort accounts for. Step 150 grows the
//! HPMS-typed VMT from the base year forward with the per-year growth
//! factors. The two feed step 160's VMT allocation.

use std::collections::BTreeMap;

use super::inputs::{HpmsVTypeYearRow, RunSpecSourceTypeRow, SourceTypeAgeRow, SourceUseTypeRow};
use super::model::{
    AnalysisYearVmtRow, FractionWithinHpmsVTypeRow, HpmsTravelFractionRow, HpmsVTypePopulationRow,
    SourceTypeAgePopulationRow, TravelFractionRow,
};

/// The four working tables step 140 builds, in dependency order.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TravelFractionTables {
    /// `HPMSVTypePopulation` — analysis-year population by HPMS type.
    pub hpms_v_type_population: Vec<HpmsVTypePopulationRow>,
    /// `FractionWithinHPMSVType` — a cohort's share of its HPMS-type
    /// population.
    pub fraction_within_hpms_v_type: Vec<FractionWithinHpmsVTypeRow>,
    /// `HPMSTravelFraction` — relative-MAR-weighted HPMS-type travel share.
    pub hpms_travel_fraction: Vec<HpmsTravelFractionRow>,
    /// `TravelFraction` — final per-cohort travel share.
    pub travel_fraction: Vec<TravelFractionRow>,
}

/// Step 140a — roll the analysis-year population up to HPMS vehicle type.
///
/// Ports the `HPMSVTypePopulation` insert: `population =
/// sum(SourceTypeAgePopulation.population)` joined to `SourceUseType` on
/// `sourceTypeID`, filtered to `yearID = analysisYear`, grouped by
/// `(yearID, HPMSVTypeID)`.
#[must_use]
pub fn hpms_v_type_population(
    source_type_age_population: &[SourceTypeAgePopulationRow],
    source_use_type: &[SourceUseTypeRow],
    analysis_year: i32,
) -> Vec<HpmsVTypePopulationRow> {
    let hpms_of: BTreeMap<i32, i32> = source_use_type
        .iter()
        .map(|r| (r.source_type_id, r.hpms_v_type_id))
        .collect();
    let mut totals: BTreeMap<i32, f64> = BTreeMap::new();
    for row in source_type_age_population {
        if row.year_id != analysis_year {
            continue;
        }
        let Some(&hpms) = hpms_of.get(&row.source_type_id) else {
            continue;
        };
        *totals.entry(hpms).or_insert(0.0) += row.population;
    }
    totals
        .into_iter()
        .map(|(hpms_v_type_id, population)| HpmsVTypePopulationRow {
            year_id: analysis_year,
            hpms_v_type_id,
            population,
        })
        .collect()
}

/// Step 140b — each cohort's share of its HPMS-type population.
///
/// Ports the `FractionWithinHPMSVType` insert: `fraction =
/// COALESCE(population / HPMSVTypePopulation.population, 0)`. The join to
/// `HPMSVTypePopulation` (analysis year only) implicitly filters the
/// population rows to the analysis year. A zero HPMS-type population yields
/// `fraction = 0` — MySQL `x/0` is `NULL` and the `COALESCE` maps it to `0`.
#[must_use]
pub fn fraction_within_hpms_v_type(
    source_type_age_population: &[SourceTypeAgePopulationRow],
    source_use_type: &[SourceUseTypeRow],
    hpms_v_type_population: &[HpmsVTypePopulationRow],
) -> Vec<FractionWithinHpmsVTypeRow> {
    let hpms_of: BTreeMap<i32, i32> = source_use_type
        .iter()
        .map(|r| (r.source_type_id, r.hpms_v_type_id))
        .collect();
    // (yearID, HPMSVTypeID) -> population.
    let hpms_population: BTreeMap<(i32, i32), f64> = hpms_v_type_population
        .iter()
        .map(|r| ((r.year_id, r.hpms_v_type_id), r.population))
        .collect();

    let mut out = Vec::new();
    for row in source_type_age_population {
        let Some(&hpms) = hpms_of.get(&row.source_type_id) else {
            continue;
        };
        let Some(&hpms_pop) = hpms_population.get(&(row.year_id, hpms)) else {
            continue;
        };
        let fraction = if hpms_pop == 0.0 {
            0.0
        } else {
            row.population / hpms_pop
        };
        out.push(FractionWithinHpmsVTypeRow {
            year_id: row.year_id,
            source_type_id: row.source_type_id,
            age_id: row.age_id,
            fraction,
        });
    }
    out.sort_by_key(|r| (r.year_id, r.source_type_id, r.age_id));
    out
}

/// Step 140c — the relative-MAR-weighted travel share of an HPMS type.
///
/// Ports the `HPMSTravelFraction` insert: `fraction =
/// sum(FractionWithinHPMSVType.fraction * SourceTypeAge.relativeMAR)`,
/// joined on `(sourceTypeID, ageID)` and grouped by `(yearID, HPMSVTypeID)`.
#[must_use]
pub fn hpms_travel_fraction(
    fraction_within_hpms_v_type: &[FractionWithinHpmsVTypeRow],
    source_use_type: &[SourceUseTypeRow],
    source_type_age: &[SourceTypeAgeRow],
) -> Vec<HpmsTravelFractionRow> {
    let hpms_of: BTreeMap<i32, i32> = source_use_type
        .iter()
        .map(|r| (r.source_type_id, r.hpms_v_type_id))
        .collect();
    let relative_mar: BTreeMap<(i32, i32), f64> = source_type_age
        .iter()
        .map(|r| ((r.source_type_id, r.age_id), r.relative_mar))
        .collect();

    let mut totals: BTreeMap<(i32, i32), f64> = BTreeMap::new();
    for row in fraction_within_hpms_v_type {
        let Some(&hpms) = hpms_of.get(&row.source_type_id) else {
            continue;
        };
        let Some(&mar) = relative_mar.get(&(row.source_type_id, row.age_id)) else {
            continue;
        };
        *totals.entry((row.year_id, hpms)).or_insert(0.0) += row.fraction * mar;
    }
    totals
        .into_iter()
        .map(
            |((year_id, hpms_v_type_id), fraction)| HpmsTravelFractionRow {
                year_id,
                hpms_v_type_id,
                fraction,
            },
        )
        .collect()
}

/// Step 140d — the final per-cohort travel fraction.
///
/// Ports the `TravelFraction` insert: `fraction =
/// COALESCE((FractionWithinHPMSVType.fraction * SourceTypeAge.relativeMAR) /
/// HPMSTravelFraction.fraction, 0)`.
///
/// When `vmt_provided_by_source_type` holds — the Java's
/// `count(SourceTypeDayVMT) + count(SourceTypeYearVMT) > 0` test — the
/// result is renormalised per `(year, sourceType)`:
/// `fraction = case when sum > 0 then fraction / sum else 0 end`.
#[must_use]
pub fn travel_fraction(
    fraction_within_hpms_v_type: &[FractionWithinHpmsVTypeRow],
    source_use_type: &[SourceUseTypeRow],
    source_type_age: &[SourceTypeAgeRow],
    hpms_travel_fraction: &[HpmsTravelFractionRow],
    vmt_provided_by_source_type: bool,
) -> Vec<TravelFractionRow> {
    let hpms_of: BTreeMap<i32, i32> = source_use_type
        .iter()
        .map(|r| (r.source_type_id, r.hpms_v_type_id))
        .collect();
    let relative_mar: BTreeMap<(i32, i32), f64> = source_type_age
        .iter()
        .map(|r| ((r.source_type_id, r.age_id), r.relative_mar))
        .collect();
    let hpms_fraction: BTreeMap<(i32, i32), f64> = hpms_travel_fraction
        .iter()
        .map(|r| ((r.year_id, r.hpms_v_type_id), r.fraction))
        .collect();

    let mut out = Vec::new();
    for row in fraction_within_hpms_v_type {
        let Some(&hpms) = hpms_of.get(&row.source_type_id) else {
            continue;
        };
        let Some(&mar) = relative_mar.get(&(row.source_type_id, row.age_id)) else {
            continue;
        };
        let Some(&hpms_frac) = hpms_fraction.get(&(row.year_id, hpms)) else {
            continue;
        };
        let fraction = if hpms_frac == 0.0 {
            0.0
        } else {
            (row.fraction * mar) / hpms_frac
        };
        out.push(TravelFractionRow {
            year_id: row.year_id,
            source_type_id: row.source_type_id,
            age_id: row.age_id,
            fraction,
        });
    }

    if vmt_provided_by_source_type {
        // Renormalise by (year, sourceType).
        let mut sums: BTreeMap<(i32, i32), f64> = BTreeMap::new();
        for row in &out {
            *sums.entry((row.year_id, row.source_type_id)).or_insert(0.0) += row.fraction;
        }
        for row in &mut out {
            let total = sums
                .get(&(row.year_id, row.source_type_id))
                .copied()
                .unwrap_or(0.0);
            row.fraction = if total > 0.0 {
                row.fraction / total
            } else {
                0.0
            };
        }
    }

    out.sort_by_key(|r| (r.year_id, r.source_type_id, r.age_id));
    out
}

/// Step 140 — build all four travel-fraction working tables in order.
///
/// Convenience orchestrator chaining [`hpms_v_type_population`],
/// [`fraction_within_hpms_v_type`], [`hpms_travel_fraction`] and
/// [`travel_fraction`].
#[must_use]
pub fn calculate_fraction_of_travel_using_hpms(
    source_type_age_population: &[SourceTypeAgePopulationRow],
    source_use_type: &[SourceUseTypeRow],
    source_type_age: &[SourceTypeAgeRow],
    analysis_year: i32,
    vmt_provided_by_source_type: bool,
) -> TravelFractionTables {
    let hvtp = hpms_v_type_population(source_type_age_population, source_use_type, analysis_year);
    let fwhvt = fraction_within_hpms_v_type(source_type_age_population, source_use_type, &hvtp);
    let hpmstf = hpms_travel_fraction(&fwhvt, source_use_type, source_type_age);
    let tf = travel_fraction(
        &fwhvt,
        source_use_type,
        source_type_age,
        &hpmstf,
        vmt_provided_by_source_type,
    );
    TravelFractionTables {
        hpms_v_type_population: hvtp,
        fraction_within_hpms_v_type: fwhvt,
        hpms_travel_fraction: hpmstf,
        travel_fraction: tf,
    }
}

/// Step 150 — grow HPMS-typed VMT from the base year to the analysis year.
///
/// Ports `growVMTToAnalysisYear` for a fresh run (`resultsYear = 0`, so the
/// base year is always seeded). `AnalysisYearVMT` starts at `HPMSVTypeYear.
/// HPMSBaseYearVMT` for every HPMS type a `RunSpecSourceType` rolls up into
/// (`INSERT IGNORE`, so one row per HPMS type even when several source types
/// share it). Each subsequent year is `VMT[y] = VMT[y-1] *
/// HPMSVTypeYear.VMTGrowthFactor[y]`.
///
/// The result holds one row per `(year, HPMSVType)` for every year in
/// `base_year..=analysis_year`.
#[must_use]
pub fn grow_vmt_to_analysis_year(
    hpms_v_type_year: &[HpmsVTypeYearRow],
    run_spec_source_type: &[RunSpecSourceTypeRow],
    source_use_type: &[SourceUseTypeRow],
    base_year: i32,
    analysis_year: i32,
) -> Vec<AnalysisYearVmtRow> {
    // HPMS types reachable from a RunSpec source type.
    let run_spec_types: BTreeMap<i32, ()> = run_spec_source_type
        .iter()
        .map(|r| (r.source_type_id, ()))
        .collect();
    let mut hpms_types: Vec<i32> = source_use_type
        .iter()
        .filter(|r| run_spec_types.contains_key(&r.source_type_id))
        .map(|r| r.hpms_v_type_id)
        .collect();
    hpms_types.sort_unstable();
    hpms_types.dedup();

    // HPMSVTypeYear lookups.
    let base_vmt: BTreeMap<(i32, i32), f64> = hpms_v_type_year
        .iter()
        .map(|r| ((r.year_id, r.hpms_v_type_id), r.hpms_base_year_vmt))
        .collect();
    let growth: BTreeMap<(i32, i32), f64> = hpms_v_type_year
        .iter()
        .map(|r| ((r.year_id, r.hpms_v_type_id), r.vmt_growth_factor))
        .collect();

    // (year, HPMSVTypeID) -> VMT, seeded at the base year.
    let mut vmt: BTreeMap<(i32, i32), f64> = BTreeMap::new();
    for &hpms in &hpms_types {
        if let Some(&base) = base_vmt.get(&(base_year, hpms)) {
            vmt.insert((base_year, hpms), base);
        }
    }

    for y in (base_year + 1)..=analysis_year {
        let mut staged: Vec<((i32, i32), f64)> = Vec::new();
        for &hpms in &hpms_types {
            let (Some(&prev), Some(&factor)) = (vmt.get(&(y - 1, hpms)), growth.get(&(y, hpms)))
            else {
                continue;
            };
            staged.push(((y, hpms), prev * factor));
        }
        vmt.extend(staged);
    }

    vmt.into_iter()
        .map(|((year_id, hpms_v_type_id), vmt)| AnalysisYearVmtRow {
            year_id,
            hpms_v_type_id,
            vmt,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPS: f64 = 1e-9;

    fn stap(year: i32, st: i32, age: i32, pop: f64) -> SourceTypeAgePopulationRow {
        SourceTypeAgePopulationRow {
            year_id: year,
            source_type_id: st,
            age_id: age,
            population: pop,
        }
    }

    fn sut(st: i32, hpms: i32) -> SourceUseTypeRow {
        SourceUseTypeRow {
            source_type_id: st,
            hpms_v_type_id: hpms,
        }
    }

    fn sta(st: i32, age: i32, mar: f64) -> SourceTypeAgeRow {
        SourceTypeAgeRow {
            source_type_id: st,
            age_id: age,
            survival_rate: 1.0,
            relative_mar: mar,
        }
    }

    #[test]
    fn hpms_population_sums_source_types_into_their_hpms_type() {
        // Source types 21 and 31 both roll up to HPMS type 10.
        let pop = [
            stap(2020, 21, 0, 100.0),
            stap(2020, 21, 1, 50.0),
            stap(2020, 31, 0, 25.0),
            // A different year — ignored.
            stap(2019, 21, 0, 999.0),
        ];
        let suts = [sut(21, 10), sut(31, 10)];
        let out = hpms_v_type_population(&pop, &suts, 2020);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].hpms_v_type_id, 10);
        assert!((out[0].population - 175.0).abs() < EPS);
    }

    #[test]
    fn fraction_within_hpms_type_is_cohort_share() {
        let pop = [stap(2020, 21, 0, 100.0), stap(2020, 21, 1, 300.0)];
        let suts = [sut(21, 10)];
        let hvtp = [HpmsVTypePopulationRow {
            year_id: 2020,
            hpms_v_type_id: 10,
            population: 400.0,
        }];
        let out = fraction_within_hpms_v_type(&pop, &suts, &hvtp);
        assert_eq!(out.len(), 2);
        assert!((out[0].fraction - 0.25).abs() < EPS);
        assert!((out[1].fraction - 0.75).abs() < EPS);
    }

    #[test]
    fn fraction_within_hpms_type_zero_population_coalesces_to_zero() {
        let pop = [stap(2020, 21, 0, 100.0)];
        let suts = [sut(21, 10)];
        let hvtp = [HpmsVTypePopulationRow {
            year_id: 2020,
            hpms_v_type_id: 10,
            population: 0.0,
        }];
        let out = fraction_within_hpms_v_type(&pop, &suts, &hvtp);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].fraction, 0.0);
    }

    #[test]
    fn travel_fraction_full_chain_normalises_to_one() {
        // One HPMS type, one source type, two ages with equal MAR.
        let pop = [stap(2020, 21, 0, 100.0), stap(2020, 21, 1, 100.0)];
        let suts = [sut(21, 10)];
        let stas = [sta(21, 0, 1.0), sta(21, 1, 1.0)];
        let tables = calculate_fraction_of_travel_using_hpms(&pop, &suts, &stas, 2020, false);
        // Each cohort is half the population; with equal MAR each travels
        // half. fraction = (0.5 * 1.0) / 1.0 = 0.5.
        assert_eq!(tables.travel_fraction.len(), 2);
        for row in &tables.travel_fraction {
            assert!((row.fraction - 0.5).abs() < EPS);
        }
        // HPMSTravelFraction = sum(0.5*1 + 0.5*1) = 1.0.
        assert_eq!(tables.hpms_travel_fraction.len(), 1);
        assert!((tables.hpms_travel_fraction[0].fraction - 1.0).abs() < EPS);
    }

    #[test]
    fn travel_fraction_weights_by_relative_mar() {
        // Age 0 travels twice as much per vehicle as age 1.
        let pop = [stap(2020, 21, 0, 100.0), stap(2020, 21, 1, 100.0)];
        let suts = [sut(21, 10)];
        let stas = [sta(21, 0, 2.0), sta(21, 1, 1.0)];
        let tables = calculate_fraction_of_travel_using_hpms(&pop, &suts, &stas, 2020, false);
        // fwhvt = 0.5 each. HPMSTravelFraction = 0.5*2 + 0.5*1 = 1.5.
        // TravelFraction age 0 = (0.5*2)/1.5 = 0.6666…; age 1 = (0.5*1)/1.5.
        let frac = |age| {
            tables
                .travel_fraction
                .iter()
                .find(|r| r.age_id == age)
                .unwrap()
                .fraction
        };
        assert!((frac(0) - (1.0 / 1.5)).abs() < EPS);
        assert!((frac(1) - (0.5 / 1.5)).abs() < EPS);
    }

    #[test]
    fn travel_fraction_renormalises_when_vmt_is_by_source_type() {
        // Without renormalisation the two ages sum to 1.0 already; force a
        // non-unit sum by giving the HPMS type a second source type so the
        // per-source-type renormalisation is observable.
        let pop = [stap(2020, 21, 0, 100.0), stap(2020, 21, 1, 100.0)];
        let suts = [sut(21, 10)];
        let stas = [sta(21, 0, 2.0), sta(21, 1, 1.0)];
        let normalised = calculate_fraction_of_travel_using_hpms(&pop, &suts, &stas, 2020, true);
        // After renormalising by (year, sourceType) the fractions sum to 1.
        let sum: f64 = normalised.travel_fraction.iter().map(|r| r.fraction).sum();
        assert!((sum - 1.0).abs() < EPS);
    }

    #[test]
    fn grow_vmt_seeds_base_year_and_compounds_growth() {
        // HPMS type 10 starts at 1000 VMT in 2020 and grows 10%/yr.
        let hvty = [
            HpmsVTypeYearRow {
                year_id: 2020,
                hpms_v_type_id: 10,
                hpms_base_year_vmt: 1000.0,
                vmt_growth_factor: 1.0,
            },
            HpmsVTypeYearRow {
                year_id: 2021,
                hpms_v_type_id: 10,
                hpms_base_year_vmt: 0.0,
                vmt_growth_factor: 1.1,
            },
            HpmsVTypeYearRow {
                year_id: 2022,
                hpms_v_type_id: 10,
                hpms_base_year_vmt: 0.0,
                vmt_growth_factor: 1.2,
            },
        ];
        let rsst = [RunSpecSourceTypeRow { source_type_id: 21 }];
        let suts = [sut(21, 10)];
        let out = grow_vmt_to_analysis_year(&hvty, &rsst, &suts, 2020, 2022);
        let vmt = |year| out.iter().find(|r| r.year_id == year).unwrap().vmt;
        assert!((vmt(2020) - 1000.0).abs() < EPS);
        assert!((vmt(2021) - 1100.0).abs() < EPS);
        // 1100 * 1.2 = 1320.
        assert!((vmt(2022) - 1320.0).abs() < EPS);
    }

    #[test]
    fn grow_vmt_skips_hpms_types_with_no_runspec_source_type() {
        let hvty = [HpmsVTypeYearRow {
            year_id: 2020,
            hpms_v_type_id: 10,
            hpms_base_year_vmt: 1000.0,
            vmt_growth_factor: 1.0,
        }];
        // RunSpec selects source type 99, which rolls up to HPMS type 20,
        // not 10 — so HPMS type 10 gets no AnalysisYearVMT row.
        let rsst = [RunSpecSourceTypeRow { source_type_id: 99 }];
        let suts = [sut(99, 20)];
        let out = grow_vmt_to_analysis_year(&hvty, &rsst, &suts, 2020, 2020);
        assert!(out.is_empty());
    }
}

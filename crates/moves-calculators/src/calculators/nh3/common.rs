//! Shared input tables and processing steps for the two ammonia (NH3)
//! calculators of Phase 3 Task 66.
//!
//! `database/NH3RunningCalculator.sql` and `database/NH3StartCalculator.sql`
//! are near-identical scripts: both extract the same emission-rate, source-bin
//! and inspection-and-maintenance (I/M) tables, and both open their
//! "Processing" section with the same two steps —
//!
//! * **`NH3REC 1` / `NH3SEC 1`** — build `IMCoverageMergedUngrouped`, the
//!   per-`(polProcess, modelYear, fuelType, sourceType)` I/M adjustment
//!   fraction. The two scripts' statements are byte-identical. Ported here as
//!   [`merge_im_coverage`].
//! * **`NH3REC-2` / `NH3SEC-2`** — weight the age-resolved base emission
//!   rates by the source-bin activity fractions. The two scripts differ only
//!   in which iteration-geography columns they stamp on (`linkID` for running,
//!   not for start); the *aggregation* — group by `(polProcess, sourceType,
//!   modelYear, fuelType, opMode)` summing `meanBaseRate × activityFraction` —
//!   is identical. Ported here as [`weight_by_source_bin`].
//!
//! Both scripts also close the same way: insert `emissionQuant` and a
//! temporary `emissionQuantIM` into `MOVESWorkerOutput`, then blend them with
//! the merged I/M fraction. That blend is [`finalize_with_im`].
//!
//! The running- and start-specific middle steps — operating-mode weighting,
//! the activity multiply (source-hours-operating vs. engine-starts) and the
//! worker-output join — live in [`super::running`] and [`super::start`].
//!
//! # The Ammonia calculator carries no fuel/temperature/AC/humidity effects
//!
//! Both SQL scripts open with the comment *"The Ammonia calculator shall not
//! have the ability to calculate fuel formulation effects, temperature
//! effects, AC on effects or humidity effects."* The pipeline is therefore
//! the bare *activity × I/M-blended base rate* — no fuel-effect ratio and no
//! temperature adjustment, unlike the exhaust-PM calculators of Tasks 53–54.
//!
//! # Data types
//!
//! Following the Phase 3 convention every `INT`/`SMALLINT` identifier is an
//! [`i32`], `sourceBinID` (a SQL `BIGINT`) is an [`i64`], and every
//! `FLOAT`/`DOUBLE` quantity is an [`f64`]. The SQL stores intermediate rates
//! in `FLOAT` (32-bit) temp columns while MariaDB evaluates the arithmetic in
//! `DOUBLE`; this port computes in `f64` end to end and does not reproduce
//! that truncation — a sub-`1e-7` relative drift left to the Task 73/74
//! calculator-integration-validation gate, matching the
//! [`super::running`]/[`super::start`] fidelity notes and the Task 65
//! `CH4N2ORunningStartCalculator` precedent.

use std::collections::HashMap;

/// Ammonia — `Pollutant` row 30. Both NH3 calculators cover this single
/// pollutant; `CalculatorInfo.txt` registers `Ammonia (NH3)` on Running
/// Exhaust and Start Exhaust to `BaseRateCalculator` (see the
/// [`super::running`] / [`super::start`] supersession notes).
pub const NH3_POLLUTANT_ID: i32 = 30;

// ===========================================================================
// Input tables — plain-Rust mirrors of the default-database tables the SQL's
// "Extract Data" section pulls, carrying only the columns the "Processing"
// section reads. A future Task 50 (`DataFrameStore`) wiring populates these
// from the per-run filtered execution database; until then they are the
// explicit data-plane contract the unit tests build directly.
// ===========================================================================

/// One `IMFactor` row — an inspection-and-maintenance benefit factor for an
/// `(polProcess, inspection program, age group)` cell.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ImFactorRow {
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `inspectFreq` — inspection frequency code.
    pub inspect_freq: i32,
    /// `testStandardsID` — the I/M test standard.
    pub test_standards_id: i32,
    /// `sourceTypeID` — MOVES source (vehicle) type.
    pub source_type_id: i32,
    /// `fuelTypeID` — fuel type.
    pub fuel_type_id: i32,
    /// `IMModelYearGroupID` — joins to
    /// [`PollutantProcessMappedModelYearRow::im_model_year_group_id`].
    pub im_model_year_group_id: i32,
    /// `ageGroupID` — joins to [`AgeCategoryRow::age_group_id`].
    pub age_group_id: i32,
    /// `IMFactor` — the I/M benefit factor (a percentage; the SQL multiplies
    /// it by `complianceFactor * 0.01`).
    pub im_factor: f64,
}

/// One `IMCoverage` row — the inspection-and-maintenance program coverage for
/// a `(county, year, sourceType, fuelType)` cell.
///
/// The SQL's "Extract Data" section already filters `IMCoverage` to
/// `useIMyn = 'Y'`, so the rows handed to [`merge_im_coverage`] are the
/// I/M-active coverage rows only; `useIMyn` is therefore not modelled.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ImCoverageRow {
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `countyID` — the county the program covers.
    pub county_id: i32,
    /// `yearID` — calendar year.
    pub year_id: i32,
    /// `sourceTypeID` — MOVES source (vehicle) type.
    pub source_type_id: i32,
    /// `fuelTypeID` — fuel type.
    pub fuel_type_id: i32,
    /// `inspectFreq` — inspection frequency code.
    pub inspect_freq: i32,
    /// `testStandardsID` — the I/M test standard.
    pub test_standards_id: i32,
    /// `begModelYearID` — first model year the program covers.
    pub beg_model_year_id: i32,
    /// `endModelYearID` — last model year the program covers.
    pub end_model_year_id: i32,
    /// `complianceFactor` — the program's compliance rate.
    pub compliance_factor: f64,
}

/// One `PollutantProcessMappedModelYear` row — maps a `(polProcess, modelYear)`
/// onto its I/M model-year group.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PollutantProcessMappedModelYearRow {
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `modelYearID` — vehicle model year.
    pub model_year_id: i32,
    /// `IMModelYearGroupID` — joins to
    /// [`ImFactorRow::im_model_year_group_id`].
    pub im_model_year_group_id: i32,
}

/// One `AgeCategory` row — maps a representative age to its age group. A
/// single `ageGroupID` spans several `ageID`s.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AgeCategoryRow {
    /// `ageID` — vehicle age in years; `modelYearID = yearID - ageID`.
    pub age_id: i32,
    /// `ageGroupID` — the age group the age belongs to.
    pub age_group_id: i32,
}

/// One `PollutantProcessAssoc` row — resolves a `polProcessID` into its
/// `(pollutantID, processID)` components.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PollutantProcessAssocRow {
    /// `polProcessID` — the surrogate key.
    pub pol_process_id: i32,
    /// `processID` — the process half.
    pub process_id: i32,
    /// `pollutantID` — the pollutant half.
    pub pollutant_id: i32,
}

/// One `EmissionRateByAge` row — an age-resolved base emission rate for a
/// `(polProcess, sourceBin, opMode, ageGroup)` cell.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EmissionRateByAgeRow {
    /// `sourceBinID` — `BIGINT` key; joins to [`SourceBinRow::source_bin_id`].
    pub source_bin_id: i64,
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `opModeID` — operating mode.
    pub op_mode_id: i32,
    /// `ageGroupID` — joins to [`AgeCategoryRow::age_group_id`].
    pub age_group_id: i32,
    /// `meanBaseRate` — the base emission rate (no I/M).
    pub mean_base_rate: f64,
    /// `meanBaseRateIM` — the base emission rate with I/M applied.
    pub mean_base_rate_im: f64,
}

/// One `SourceTypeModelYear` row — resolves a `sourceTypeModelYearID`
/// surrogate key into its `(sourceTypeID, modelYearID)` components.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypeModelYearRow {
    /// `sourceTypeModelYearID` — the surrogate key.
    pub source_type_model_year_id: i32,
    /// `modelYearID` — vehicle model year.
    pub model_year_id: i32,
    /// `sourceTypeID` — MOVES source (vehicle) type.
    pub source_type_id: i32,
}

/// One `SourceBinDistribution` row — a source bin's share of a
/// `(sourceTypeModelYear)` group's activity for one `polProcessID`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceBinDistributionRow {
    /// `sourceTypeModelYearID` — joins to
    /// [`SourceTypeModelYearRow::source_type_model_year_id`].
    pub source_type_model_year_id: i32,
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `sourceBinID` — joins to [`SourceBinRow::source_bin_id`].
    pub source_bin_id: i64,
    /// `sourceBinActivityFraction` — the bin's share of the group's activity.
    pub source_bin_activity_fraction: f64,
}

/// One `SourceBin` row — the engine/fuel decomposition of a source bin. Only
/// `fuelTypeID` is read by the NH3 pipeline.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceBinRow {
    /// `sourceBinID` — `BIGINT` primary key.
    pub source_bin_id: i64,
    /// `fuelTypeID` — fuel type.
    pub fuel_type_id: i32,
}

/// One `OpModeDistribution` row — the operating-mode fractions for a
/// `(sourceType, hourDay, link, polProcess)` cell.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OpModeDistributionRow {
    /// `sourceTypeID` — MOVES source (vehicle) type.
    pub source_type_id: i32,
    /// `hourDayID` — joins to [`HourDayRow::hour_day_id`].
    pub hour_day_id: i32,
    /// `linkID` — the road link.
    pub link_id: i32,
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `opModeID` — operating mode.
    pub op_mode_id: i32,
    /// `opModeFraction` — the fraction of activity in this operating mode.
    pub op_mode_fraction: f64,
}

/// One `HourDay` row — the `hourDayID` → `(dayID, hourID)` split.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HourDayRow {
    /// `hourDayID` — the surrogate key.
    pub hour_day_id: i32,
    /// `dayID` — day-of-week type.
    pub day_id: i32,
    /// `hourID` — hour of day.
    pub hour_id: i32,
}

// ===========================================================================
// Intermediate and output rows.
// ===========================================================================

/// One `IMCoverageMergedUngrouped` row — the I/M adjustment fraction for a
/// `(polProcess, modelYear, fuelType, sourceType)` cell, the result of
/// [`merge_im_coverage`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ImCoverageMergedRow {
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `pollutantID` — resolved from `PollutantProcessAssoc`; `0` if the
    /// `polProcessID` is absent from the supplied associations (the SQL
    /// `UPDATE` would leave the column at its `0` insert default).
    pub pollutant_id: i32,
    /// `processID` — resolved from `PollutantProcessAssoc`; `0` if absent.
    pub process_id: i32,
    /// `modelYearID` — vehicle model year.
    pub model_year_id: i32,
    /// `fuelTypeID` — fuel type.
    pub fuel_type_id: i32,
    /// `sourceTypeID` — MOVES source (vehicle) type.
    pub source_type_id: i32,
    /// `IMAdjustFract` — `Σ(IMFactor × complianceFactor × 0.01)` over the
    /// cell. The blend weight applied by [`finalize_with_im`].
    pub im_adjust_fract: f64,
    /// `weightFactor` — `Σ(complianceFactor)` over the cell. The SQL computes
    /// this column but no later step reads it; it is carried for fidelity.
    pub weight_factor: f64,
}

/// One `SourceBinEmissionRates0` row — a base emission rate weighted across
/// source bins, the result of [`weight_by_source_bin`].
///
/// Keyed by `(polProcess, sourceType, modelYear, fuelType, opMode)` — the
/// SQL's `GROUP BY`. The running and start scripts also stamp the iteration
/// geography (`zoneID`/`linkID`) onto this table; those are constant per run
/// and supplied by the calculator-specific contexts, so they are not stored
/// here.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceBinEmissionRate {
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `sourceTypeID` — MOVES source (vehicle) type.
    pub source_type_id: i32,
    /// `modelYearID` — vehicle model year.
    pub model_year_id: i32,
    /// `fuelTypeID` — fuel type.
    pub fuel_type_id: i32,
    /// `opModeID` — operating mode.
    pub op_mode_id: i32,
    /// `meanBaseRate` — `Σ(EmissionRateByAge.meanBaseRate ×
    /// sourceBinActivityFraction)` over the source bins of the cell.
    pub mean_base_rate: f64,
    /// `meanBaseRateIM` — the same sum over `meanBaseRateIM`.
    pub mean_base_rate_im: f64,
}

/// One `MOVESWorkerOutput` row produced by the running or start calculation.
///
/// The fourteen integer columns are the emission-table dimensions; the SQL
/// also writes an `SCC` column, left to the Task 50 output wiring as it is
/// not an algorithm input. `emissionQuant` is the final I/M-blended emission
/// total ([`finalize_with_im`]); the temporary `emissionQuantIM` column the
/// SQL adds and drops is not part of this struct.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EmissionRow {
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
    /// `pollutantID` — always `30` (ammonia).
    pub pollutant_id: i32,
    /// `processID` — `1` (Running Exhaust) or `2` (Start Exhaust).
    pub process_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `modelYearID`.
    pub model_year_id: i32,
    /// `roadTypeID`.
    pub road_type_id: i32,
    /// `emissionQuant` — the I/M-blended emission total for this cell.
    pub emission_quant: f64,
}

impl EmissionRow {
    /// The integer dimension tuple — every column except `emission_quant`, in
    /// `MOVESWorkerOutput` column order. Used to sort the output
    /// deterministically; MOVES leaves `MOVESWorkerOutput` physically
    /// unordered.
    fn dimension_key(&self) -> [i32; 14] {
        [
            self.year_id,
            self.month_id,
            self.day_id,
            self.hour_id,
            self.state_id,
            self.county_id,
            self.zone_id,
            self.link_id,
            self.pollutant_id,
            self.process_id,
            self.source_type_id,
            self.fuel_type_id,
            self.model_year_id,
            self.road_type_id,
        ]
    }
}

// ===========================================================================
// Processing steps shared by the running and start calculators.
// ===========================================================================

/// Build `IMCoverageMergedUngrouped` — the port of `NH3REC 1` / `NH3SEC 1`,
/// which are byte-identical between the two NH3 scripts.
///
/// For every `(polProcess, modelYear, fuelType, sourceType)` cell the SQL
/// `GROUP BY` produces, this returns one [`ImCoverageMergedRow`] with
/// `IMAdjustFract = Σ(IMFactor × complianceFactor × 0.01)` and
/// `weightFactor = Σ(complianceFactor)`.
///
/// The join chain reproduced is
/// `PollutantProcessMappedModelYear ⋈ IMFactor ⋈ AgeCategory ⋈ IMCoverage`:
///
/// * `IMFactor` joins on `(polProcessID, IMModelYearGroupID)`;
/// * `AgeCategory` joins on `ageGroupID` — one row per `ageID` in the group;
/// * the row survives only where `ppmy.modelYearID = yearID - ageID`;
/// * `IMCoverage` joins on `(polProcessID, inspectFreq, testStandardsID,
///   sourceTypeID, fuelTypeID)` and the model year falling within
///   `[begModelYearID, endModelYearID]`.
///
/// Every join is an SQL `INNER JOIN`: a tuple with no match on a join key is
/// dropped, reproduced here with map lookups that `continue` on a miss. The
/// `IMCoverage` rows are taken as already extract-filtered to the iteration
/// county, year and `useIMyn = 'Y'`; `year_id` and `county_id` re-apply the
/// county/year filter so the function is correct for any input.
///
/// `pollutantID`/`processID` are resolved from `PollutantProcessAssoc` (the
/// SQL's follow-up `UPDATE`); a `polProcessID` absent from the associations
/// keeps the SQL `0` insert default. Output is sorted by
/// `(polProcessID, modelYearID, fuelTypeID, sourceTypeID)` for determinism.
#[must_use]
pub fn merge_im_coverage(
    year_id: i32,
    county_id: i32,
    pollutant_process_mapped_model_year: &[PollutantProcessMappedModelYearRow],
    im_factor: &[ImFactorRow],
    age_category: &[AgeCategoryRow],
    im_coverage: &[ImCoverageRow],
    pollutant_process_assoc: &[PollutantProcessAssocRow],
) -> Vec<ImCoverageMergedRow> {
    // IMFactor indexed by its join key to PollutantProcessMappedModelYear.
    let mut im_factor_by: HashMap<(i32, i32), Vec<&ImFactorRow>> = HashMap::new();
    for imf in im_factor {
        im_factor_by
            .entry((imf.pol_process_id, imf.im_model_year_group_id))
            .or_default()
            .push(imf);
    }
    // AgeCategory indexed by ageGroupID — a group spans several ages.
    let mut ages_by_group: HashMap<i32, Vec<i32>> = HashMap::new();
    for ac in age_category {
        ages_by_group
            .entry(ac.age_group_id)
            .or_default()
            .push(ac.age_id);
    }
    // IMCoverage indexed by its join key to IMFactor, filtered to the
    // iteration county and year (the SQL `WHERE imc.countyID = … AND
    // imc.yearID = …`).
    let mut im_coverage_by: HashMap<(i32, i32, i32, i32, i32), Vec<&ImCoverageRow>> =
        HashMap::new();
    for imc in im_coverage {
        if imc.county_id != county_id || imc.year_id != year_id {
            continue;
        }
        im_coverage_by
            .entry((
                imc.pol_process_id,
                imc.inspect_freq,
                imc.test_standards_id,
                imc.source_type_id,
                imc.fuel_type_id,
            ))
            .or_default()
            .push(imc);
    }
    // PollutantProcessAssoc lookup — polProcessID → (pollutantID, processID).
    let ppa_by: HashMap<i32, &PollutantProcessAssocRow> = pollutant_process_assoc
        .iter()
        .map(|r| (r.pol_process_id, r))
        .collect();

    // Accumulate (IMAdjustFract, weightFactor) over the GROUP BY key.
    let mut acc: HashMap<(i32, i32, i32, i32), (f64, f64)> = HashMap::new();
    for ppmy in pollutant_process_mapped_model_year {
        // INNER JOIN IMFactor USING (polProcessID, IMModelYearGroupID).
        let Some(im_factors) =
            im_factor_by.get(&(ppmy.pol_process_id, ppmy.im_model_year_group_id))
        else {
            continue;
        };
        for imf in im_factors {
            // INNER JOIN AgeCategory ON ageGroupID — one tuple per age.
            let Some(ages) = ages_by_group.get(&imf.age_group_id) else {
                continue;
            };
            for &age_id in ages {
                let model_year_id = year_id - age_id;
                // WHERE ppmy.modelYearID = yearID - ageID.
                if ppmy.model_year_id != model_year_id {
                    continue;
                }
                // INNER JOIN IMCoverage USING (polProcessID, inspectFreq,
                // testStandardsID, sourceTypeID, fuelTypeID).
                let Some(coverages) = im_coverage_by.get(&(
                    imf.pol_process_id,
                    imf.inspect_freq,
                    imf.test_standards_id,
                    imf.source_type_id,
                    imf.fuel_type_id,
                )) else {
                    continue;
                };
                for imc in coverages {
                    // … AND imc.begModelYearID <= modelYear <=
                    // imc.endModelYearID.
                    if model_year_id < imc.beg_model_year_id
                        || model_year_id > imc.end_model_year_id
                    {
                        continue;
                    }
                    let entry = acc
                        .entry((
                            ppmy.pol_process_id,
                            ppmy.model_year_id,
                            imf.fuel_type_id,
                            imc.source_type_id,
                        ))
                        .or_insert((0.0, 0.0));
                    entry.0 += imf.im_factor * imc.compliance_factor * 0.01;
                    entry.1 += imc.compliance_factor;
                }
            }
        }
    }

    let mut out: Vec<ImCoverageMergedRow> = acc
        .into_iter()
        .map(
            |((pol_process_id, model_year_id, fuel_type_id, source_type_id), (adj, wf))| {
                // The SQL `UPDATE … SET pollutantID = …, processID = …` from
                // PollutantProcessAssoc; the `0` insert default stands if absent.
                let (pollutant_id, process_id) = ppa_by
                    .get(&pol_process_id)
                    .map_or((0, 0), |a| (a.pollutant_id, a.process_id));
                ImCoverageMergedRow {
                    pol_process_id,
                    pollutant_id,
                    process_id,
                    model_year_id,
                    fuel_type_id,
                    source_type_id,
                    im_adjust_fract: adj,
                    weight_factor: wf,
                }
            },
        )
        .collect();
    out.sort_unstable_by_key(|r| {
        (
            r.pol_process_id,
            r.model_year_id,
            r.fuel_type_id,
            r.source_type_id,
        )
    });
    out
}

/// `weight_by_source_bin`'s `GROUP BY` key — `(polProcessID, sourceTypeID,
/// modelYearID, fuelTypeID, opModeID)`, the dimension tuple of one
/// [`SourceBinEmissionRate`].
type SourceBinGroupKey = (i32, i32, i32, i32, i32);

/// Weight the age-resolved base emission rates by source-bin activity — the
/// port of `NH3REC-2` / `NH3SEC-2`'s `SourceBinEmissionRates0` insert.
///
/// For every `(polProcess, sourceType, modelYear, fuelType, opMode)` cell the
/// SQL `GROUP BY` produces, this returns one [`SourceBinEmissionRate`] with
/// `meanBaseRate = Σ(EmissionRateByAge.meanBaseRate × sourceBinActivityFraction)`
/// and the matching sum over `meanBaseRateIM`.
///
/// The join chain reproduced is
/// `EmissionRateByAge ⋈ AgeCategory ⋈ SourceTypeModelYear ⋈
/// SourceBinDistribution ⋈ SourceBin`:
///
/// * `AgeCategory` joins on `ageGroupID` — one tuple per `ageID` in the group;
/// * `SourceTypeModelYear` joins on `modelYearID = yearID - ageID`;
/// * `SourceBinDistribution` joins on `(sourceTypeModelYearID, polProcessID,
///   sourceBinID)`;
/// * `SourceBin` joins on `sourceBinID`, supplying `fuelTypeID`.
///
/// Every join is an SQL `INNER JOIN`; a row with no match is dropped. Output
/// is sorted by the `GROUP BY` key for determinism.
#[must_use]
pub fn weight_by_source_bin(
    year_id: i32,
    emission_rate_by_age: &[EmissionRateByAgeRow],
    age_category: &[AgeCategoryRow],
    source_type_model_year: &[SourceTypeModelYearRow],
    source_bin_distribution: &[SourceBinDistributionRow],
    source_bin: &[SourceBinRow],
) -> Vec<SourceBinEmissionRate> {
    // AgeCategory indexed by ageGroupID.
    let mut ages_by_group: HashMap<i32, Vec<i32>> = HashMap::new();
    for ac in age_category {
        ages_by_group
            .entry(ac.age_group_id)
            .or_default()
            .push(ac.age_id);
    }
    // SourceTypeModelYear indexed by modelYearID.
    let mut stmy_by_year: HashMap<i32, Vec<&SourceTypeModelYearRow>> = HashMap::new();
    for stmy in source_type_model_year {
        stmy_by_year
            .entry(stmy.model_year_id)
            .or_default()
            .push(stmy);
    }
    // SourceBinDistribution indexed by its join key.
    let mut sbd_by: HashMap<(i32, i32, i64), Vec<&SourceBinDistributionRow>> = HashMap::new();
    for sbd in source_bin_distribution {
        sbd_by
            .entry((
                sbd.source_type_model_year_id,
                sbd.pol_process_id,
                sbd.source_bin_id,
            ))
            .or_default()
            .push(sbd);
    }
    // SourceBin lookup — sourceBinID → fuelTypeID.
    let source_bin_fuel: HashMap<i64, i32> = source_bin
        .iter()
        .map(|r| (r.source_bin_id, r.fuel_type_id))
        .collect();

    let mut acc: HashMap<SourceBinGroupKey, (f64, f64)> = HashMap::new();
    for er in emission_rate_by_age {
        // INNER JOIN AgeCategory ON ageGroupID.
        let Some(ages) = ages_by_group.get(&er.age_group_id) else {
            continue;
        };
        for &age_id in ages {
            let model_year_id = year_id - age_id;
            // INNER JOIN SourceTypeModelYear ON modelYearID = yearID - ageID.
            let Some(stmys) = stmy_by_year.get(&model_year_id) else {
                continue;
            };
            for stmy in stmys {
                // INNER JOIN SourceBinDistribution USING
                // (sourceTypeModelYearID, polProcessID, sourceBinID).
                let Some(sbds) = sbd_by.get(&(
                    stmy.source_type_model_year_id,
                    er.pol_process_id,
                    er.source_bin_id,
                )) else {
                    continue;
                };
                for sbd in sbds {
                    // INNER JOIN SourceBin ON sourceBinID.
                    let Some(&fuel_type_id) = source_bin_fuel.get(&sbd.source_bin_id) else {
                        continue;
                    };
                    let entry = acc
                        .entry((
                            er.pol_process_id,
                            stmy.source_type_id,
                            stmy.model_year_id,
                            fuel_type_id,
                            er.op_mode_id,
                        ))
                        .or_insert((0.0, 0.0));
                    entry.0 += er.mean_base_rate * sbd.source_bin_activity_fraction;
                    entry.1 += er.mean_base_rate_im * sbd.source_bin_activity_fraction;
                }
            }
        }
    }

    let mut out: Vec<SourceBinEmissionRate> = acc
        .into_iter()
        .map(
            |(
                (pol_process_id, source_type_id, model_year_id, fuel_type_id, op_mode_id),
                (mean_base_rate, mean_base_rate_im),
            )| SourceBinEmissionRate {
                pol_process_id,
                source_type_id,
                model_year_id,
                fuel_type_id,
                op_mode_id,
                mean_base_rate,
                mean_base_rate_im,
            },
        )
        .collect();
    out.sort_unstable_by_key(|r| {
        (
            r.pol_process_id,
            r.source_type_id,
            r.model_year_id,
            r.fuel_type_id,
            r.op_mode_id,
        )
    });
    out
}

/// Blend `emissionQuant` with `emissionQuantIM` and finalise the output rows
/// — the port of both scripts' closing `-- Apply IM` `UPDATE`.
///
/// Each input pairs a worker-output row carrying its *non-I/M* emission total
/// in `emission_quant` with its I/M emission total `emission_quant_im`. Where
/// the row's `(processID, pollutantID, modelYearID, fuelTypeID, sourceTypeID)`
/// matches an [`ImCoverageMergedRow`], the SQL replaces the quantity with
/// `GREATEST(emissionQuantIM × IMAdjustFract + emissionQuant ×
/// (1 - IMAdjustFract), 0)`; a row with no I/M match keeps its non-I/M
/// quantity unchanged (and is *not* floored at zero — the SQL `UPDATE` does
/// not touch it).
///
/// The merged-row `(processID, pollutantID, modelYearID, fuelTypeID,
/// sourceTypeID)` key is unique — `polProcessID` maps one-to-one onto
/// `(pollutantID, processID)` and the [`merge_im_coverage`] `GROUP BY` already
/// resolves the other three — so a single fraction applies per matched row.
/// Output is sorted by the worker-output dimension columns for determinism.
#[must_use]
pub fn finalize_with_im(
    rows_with_im: Vec<(EmissionRow, f64)>,
    merged: &[ImCoverageMergedRow],
) -> Vec<EmissionRow> {
    let lookup: HashMap<(i32, i32, i32, i32, i32), f64> = merged
        .iter()
        .map(|m| {
            (
                (
                    m.process_id,
                    m.pollutant_id,
                    m.model_year_id,
                    m.fuel_type_id,
                    m.source_type_id,
                ),
                m.im_adjust_fract,
            )
        })
        .collect();

    let mut out: Vec<EmissionRow> = rows_with_im
        .into_iter()
        .map(|(mut row, emission_quant_im)| {
            let key = (
                row.process_id,
                row.pollutant_id,
                row.model_year_id,
                row.fuel_type_id,
                row.source_type_id,
            );
            if let Some(&im_adjust_fract) = lookup.get(&key) {
                row.emission_quant = f64::max(
                    emission_quant_im * im_adjust_fract
                        + row.emission_quant * (1.0 - im_adjust_fract),
                    0.0,
                );
            }
            row
        })
        .collect();
    out.sort_unstable_by_key(EmissionRow::dimension_key);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// NH3 Running Exhaust `polProcessID` — `pollutant 30 × 100 + process 1`.
    const NH3_RUNNING_POL_PROCESS: i32 = 3001;

    #[test]
    fn merge_im_coverage_sums_factor_and_compliance_over_the_cell() {
        // Two ages in one group; both resolve to model years the single
        // IMCoverage row covers, so the cell sums two (factor, compliance)
        // contributions.
        let ppmy = vec![
            PollutantProcessMappedModelYearRow {
                pol_process_id: NH3_RUNNING_POL_PROCESS,
                model_year_id: 2019,
                im_model_year_group_id: 7,
            },
            PollutantProcessMappedModelYearRow {
                pol_process_id: NH3_RUNNING_POL_PROCESS,
                model_year_id: 2018,
                im_model_year_group_id: 7,
            },
        ];
        let im_factor = vec![ImFactorRow {
            pol_process_id: NH3_RUNNING_POL_PROCESS,
            inspect_freq: 1,
            test_standards_id: 11,
            source_type_id: 21,
            fuel_type_id: 1,
            im_model_year_group_id: 7,
            age_group_id: 3,
            im_factor: 50.0,
        }];
        let age_category = vec![
            AgeCategoryRow {
                age_id: 1,
                age_group_id: 3,
            },
            AgeCategoryRow {
                age_id: 2,
                age_group_id: 3,
            },
        ];
        let im_coverage = vec![ImCoverageRow {
            pol_process_id: NH3_RUNNING_POL_PROCESS,
            county_id: 26_161,
            year_id: 2020,
            source_type_id: 21,
            fuel_type_id: 1,
            inspect_freq: 1,
            test_standards_id: 11,
            beg_model_year_id: 2000,
            end_model_year_id: 2025,
            compliance_factor: 80.0,
        }];
        let ppa = vec![PollutantProcessAssocRow {
            pol_process_id: NH3_RUNNING_POL_PROCESS,
            process_id: 1,
            pollutant_id: NH3_POLLUTANT_ID,
        }];

        let merged = merge_im_coverage(
            2020,
            26_161,
            &ppmy,
            &im_factor,
            &age_category,
            &im_coverage,
            &ppa,
        );

        // age 1 → model year 2019, age 2 → model year 2018 — two distinct
        // cells, each one contribution: IMAdjustFract = 50 × 80 × 0.01 = 40.
        assert_eq!(merged.len(), 2);
        for row in &merged {
            assert_eq!(row.pollutant_id, NH3_POLLUTANT_ID);
            assert_eq!(row.process_id, 1);
            assert_eq!(row.fuel_type_id, 1);
            assert_eq!(row.source_type_id, 21);
            assert!((row.im_adjust_fract - 40.0).abs() < 1e-9);
            assert!((row.weight_factor - 80.0).abs() < 1e-9);
        }
        assert_eq!(merged[0].model_year_id, 2018);
        assert_eq!(merged[1].model_year_id, 2019);
    }

    #[test]
    fn merge_im_coverage_drops_model_years_outside_coverage() {
        // The IMCoverage row covers only 2015–2017; model year 2019 (age 1 of
        // run year 2020) falls outside, so the cell is dropped.
        let ppmy = vec![PollutantProcessMappedModelYearRow {
            pol_process_id: NH3_RUNNING_POL_PROCESS,
            model_year_id: 2019,
            im_model_year_group_id: 7,
        }];
        let im_factor = vec![ImFactorRow {
            pol_process_id: NH3_RUNNING_POL_PROCESS,
            inspect_freq: 1,
            test_standards_id: 11,
            source_type_id: 21,
            fuel_type_id: 1,
            im_model_year_group_id: 7,
            age_group_id: 3,
            im_factor: 50.0,
        }];
        let age_category = vec![AgeCategoryRow {
            age_id: 1,
            age_group_id: 3,
        }];
        let im_coverage = vec![ImCoverageRow {
            pol_process_id: NH3_RUNNING_POL_PROCESS,
            county_id: 26_161,
            year_id: 2020,
            source_type_id: 21,
            fuel_type_id: 1,
            inspect_freq: 1,
            test_standards_id: 11,
            beg_model_year_id: 2015,
            end_model_year_id: 2017,
            compliance_factor: 80.0,
        }];

        let merged = merge_im_coverage(
            2020,
            26_161,
            &ppmy,
            &im_factor,
            &age_category,
            &im_coverage,
            &[],
        );
        assert!(merged.is_empty());
    }

    #[test]
    fn merge_im_coverage_filters_to_iteration_county_and_year() {
        let ppmy = vec![PollutantProcessMappedModelYearRow {
            pol_process_id: NH3_RUNNING_POL_PROCESS,
            model_year_id: 2019,
            im_model_year_group_id: 7,
        }];
        let im_factor = vec![ImFactorRow {
            pol_process_id: NH3_RUNNING_POL_PROCESS,
            inspect_freq: 1,
            test_standards_id: 11,
            source_type_id: 21,
            fuel_type_id: 1,
            im_model_year_group_id: 7,
            age_group_id: 3,
            im_factor: 50.0,
        }];
        let age_category = vec![AgeCategoryRow {
            age_id: 1,
            age_group_id: 3,
        }];
        // Coverage for a different county — no cell survives.
        let im_coverage = vec![ImCoverageRow {
            pol_process_id: NH3_RUNNING_POL_PROCESS,
            county_id: 99_999,
            year_id: 2020,
            source_type_id: 21,
            fuel_type_id: 1,
            inspect_freq: 1,
            test_standards_id: 11,
            beg_model_year_id: 2000,
            end_model_year_id: 2025,
            compliance_factor: 80.0,
        }];

        let merged = merge_im_coverage(
            2020,
            26_161,
            &ppmy,
            &im_factor,
            &age_category,
            &im_coverage,
            &[],
        );
        assert!(merged.is_empty());
    }

    #[test]
    fn weight_by_source_bin_sums_rate_times_activity_fraction() {
        // Two source bins of one fuel type contribute to one cell:
        // meanBaseRate = 2·0.25 + 4·0.75 = 3.5.
        let emission_rate_by_age = vec![
            EmissionRateByAgeRow {
                source_bin_id: 1000,
                pol_process_id: NH3_RUNNING_POL_PROCESS,
                op_mode_id: 1,
                age_group_id: 3,
                mean_base_rate: 2.0,
                mean_base_rate_im: 1.0,
            },
            EmissionRateByAgeRow {
                source_bin_id: 2000,
                pol_process_id: NH3_RUNNING_POL_PROCESS,
                op_mode_id: 1,
                age_group_id: 3,
                mean_base_rate: 4.0,
                mean_base_rate_im: 2.0,
            },
        ];
        let age_category = vec![AgeCategoryRow {
            age_id: 2,
            age_group_id: 3,
        }];
        let source_type_model_year = vec![SourceTypeModelYearRow {
            source_type_model_year_id: 212_018,
            model_year_id: 2018,
            source_type_id: 21,
        }];
        let source_bin_distribution = vec![
            SourceBinDistributionRow {
                source_type_model_year_id: 212_018,
                pol_process_id: NH3_RUNNING_POL_PROCESS,
                source_bin_id: 1000,
                source_bin_activity_fraction: 0.25,
            },
            SourceBinDistributionRow {
                source_type_model_year_id: 212_018,
                pol_process_id: NH3_RUNNING_POL_PROCESS,
                source_bin_id: 2000,
                source_bin_activity_fraction: 0.75,
            },
        ];
        let source_bin = vec![
            SourceBinRow {
                source_bin_id: 1000,
                fuel_type_id: 1,
            },
            SourceBinRow {
                source_bin_id: 2000,
                fuel_type_id: 1,
            },
        ];

        let rates = weight_by_source_bin(
            2020,
            &emission_rate_by_age,
            &age_category,
            &source_type_model_year,
            &source_bin_distribution,
            &source_bin,
        );
        assert_eq!(rates.len(), 1);
        assert_eq!(rates[0].source_type_id, 21);
        assert_eq!(rates[0].model_year_id, 2018);
        assert_eq!(rates[0].fuel_type_id, 1);
        assert!((rates[0].mean_base_rate - 3.5).abs() < 1e-9);
        assert!((rates[0].mean_base_rate_im - 1.75).abs() < 1e-9);
    }

    /// A worker row carrying the dimension columns the I/M blend keys on.
    fn quant_row(model_year_id: i32, emission_quant: f64) -> EmissionRow {
        EmissionRow {
            year_id: 2020,
            month_id: 7,
            day_id: 5,
            hour_id: 8,
            state_id: 26,
            county_id: 26_161,
            zone_id: 261_610,
            link_id: 5001,
            pollutant_id: NH3_POLLUTANT_ID,
            process_id: 1,
            source_type_id: 21,
            fuel_type_id: 1,
            model_year_id,
            road_type_id: 4,
            emission_quant,
        }
    }

    #[test]
    fn finalize_with_im_blends_matched_rows_and_passes_unmatched_through() {
        let merged = vec![ImCoverageMergedRow {
            pol_process_id: NH3_RUNNING_POL_PROCESS,
            pollutant_id: NH3_POLLUTANT_ID,
            process_id: 1,
            model_year_id: 2018,
            fuel_type_id: 1,
            source_type_id: 21,
            im_adjust_fract: 0.25,
            weight_factor: 80.0,
        }];
        // 2018 matches: 4 × 0.25 + 10 × 0.75 = 8.5. 2019 has no I/M row, so
        // its non-I/M quantity (10.0) passes through unchanged.
        let rows = vec![(quant_row(2018, 10.0), 4.0), (quant_row(2019, 10.0), 4.0)];

        let out = finalize_with_im(rows, &merged);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].model_year_id, 2018);
        assert!((out[0].emission_quant - 8.5).abs() < 1e-9);
        assert_eq!(out[1].model_year_id, 2019);
        assert!((out[1].emission_quant - 10.0).abs() < 1e-9);
    }

    #[test]
    fn finalize_with_im_floors_a_matched_blend_at_zero() {
        // An IMAdjustFract above one drives the blend negative; the SQL
        // `GREATEST(…, 0)` floors a *matched* row at zero.
        let merged = vec![ImCoverageMergedRow {
            pol_process_id: NH3_RUNNING_POL_PROCESS,
            pollutant_id: NH3_POLLUTANT_ID,
            process_id: 1,
            model_year_id: 2018,
            fuel_type_id: 1,
            source_type_id: 21,
            im_adjust_fract: 2.0,
            weight_factor: 80.0,
        }];
        // 1 × 2 + 10 × (1 - 2) = -8 → floored to 0.
        let rows = vec![(quant_row(2018, 10.0), 1.0)];

        let out = finalize_with_im(rows, &merged);
        assert_eq!(out.len(), 1);
        assert!((out[0].emission_quant - 0.0).abs() < 1e-9);
    }
}

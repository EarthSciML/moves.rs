//! VMT allocation by road type, source, age, and hour — algorithm steps
//! 160-179.
//!
//! Ports `TotalActivityGenerator.java`'s `allocateVMTByRoadTypeSourceAge`
//! and `calculateVMTByRoadwayHour`.
//!
//! Step 160 splits the analysis-year VMT across road type, source type, and
//! age. Step 170 then allocates that annual VMT temporally — to month, day,
//! and hour — and derives the per-model-year VMT fractions
//! `vmtByMYRoadHourFraction` the rate calculators key on.

use std::collections::{BTreeMap, BTreeSet};

use super::inputs::{
    DayOfAnyWeekRow, DayVmtFractionRow, HourDayRow, HourVmtFractionRow, HpmsVTypeDayRow,
    MonthOfAnyYearRow, MonthVmtFractionRow, RoadTypeDistributionRow, RoadTypeRow,
    SourceTypeDayVmtRow, SourceTypeYearVmtRow, SourceUseTypeRow,
};
use super::model::{
    AnalysisYearVmtRow, AnnualVmtByAgeRoadwayRow, TravelFractionRow, VmtByAgeRoadwayHourRow,
    VmtByMyRoadHourFractionRow,
};

/// Step 160 — split analysis-year VMT across `(roadType, sourceType, age)`.
///
/// Ports `allocateVMTByRoadTypeSourceAge`'s two `AnnualVMTByAgeRoadway`
/// inserts, both computing `VMT = annualVMT * roadTypeVMTFraction *
/// TravelFraction.fraction`:
///
/// * the **HPMS path** takes `annualVMT` from `AnalysisYearVMT`, joined to
///   the source type through `SourceUseType.HPMSVTypeID`;
/// * the **source-type path** takes it from `SourceTypeYearVMT` directly.
///
/// A run supplies VMT in exactly one of these forms, so in practice only one
/// path yields rows; the port runs both and concatenates, mirroring the two
/// unconditional Java inserts. Both require the road type to be present in
/// `RoadType` and join `RoadTypeDistribution` on `sourceTypeID`.
#[must_use]
pub fn allocate_vmt_by_road_type_source_age(
    travel_fraction: &[TravelFractionRow],
    road_type: &[RoadTypeRow],
    road_type_distribution: &[RoadTypeDistributionRow],
    analysis_year_vmt: &[AnalysisYearVmtRow],
    source_use_type: &[SourceUseTypeRow],
    source_type_year_vmt: &[SourceTypeYearVmtRow],
    analysis_year: i32,
) -> Vec<AnnualVmtByAgeRoadwayRow> {
    let road_types: BTreeSet<i32> = road_type.iter().map(|r| r.road_type_id).collect();
    let hpms_of: BTreeMap<i32, i32> = source_use_type
        .iter()
        .map(|r| (r.source_type_id, r.hpms_v_type_id))
        .collect();
    // (yearID, HPMSVTypeID) -> VMT.
    let analysis_vmt: BTreeMap<(i32, i32), f64> = analysis_year_vmt
        .iter()
        .map(|r| ((r.year_id, r.hpms_v_type_id), r.vmt))
        .collect();
    // (yearID, sourceTypeID) -> VMT.
    let source_type_vmt: BTreeMap<(i32, i32), f64> = source_type_year_vmt
        .iter()
        .filter(|r| r.year_id == analysis_year)
        .map(|r| ((r.year_id, r.source_type_id), r.vmt))
        .collect();
    // sourceTypeID -> [(roadTypeID, roadTypeVMTFraction)] for in-scope roads.
    let mut road_fractions: BTreeMap<i32, Vec<(i32, f64)>> = BTreeMap::new();
    for rtd in road_type_distribution {
        if road_types.contains(&rtd.road_type_id) {
            road_fractions
                .entry(rtd.source_type_id)
                .or_default()
                .push((rtd.road_type_id, rtd.road_type_vmt_fraction));
        }
    }

    let mut out = Vec::new();
    for tf in travel_fraction {
        let Some(roads) = road_fractions.get(&tf.source_type_id) else {
            continue;
        };
        // HPMS path: annual VMT keyed by the source type's HPMS type.
        let hpms_vmt = hpms_of
            .get(&tf.source_type_id)
            .and_then(|hpms| analysis_vmt.get(&(tf.year_id, *hpms)));
        // Source-type path: annual VMT keyed by the source type directly.
        let st_vmt = source_type_vmt.get(&(tf.year_id, tf.source_type_id));

        for &(road_type_id, road_fraction) in roads {
            for annual in [hpms_vmt, st_vmt].into_iter().flatten() {
                out.push(AnnualVmtByAgeRoadwayRow {
                    year_id: tf.year_id,
                    road_type_id,
                    source_type_id: tf.source_type_id,
                    age_id: tf.age_id,
                    vmt: annual * road_fraction * tf.fraction,
                });
            }
        }
    }
    out.sort_by_key(|r| (r.year_id, r.road_type_id, r.source_type_id, r.age_id));
    out
}

/// Weeks in a month — `MonthOfAnyYear.noOfDays / 7.0`.
///
/// Ports `WeeksInMonthHelper.getWeeksPerMonthSQLClause`: the generated SQL
/// `CASE` returns `noOfDays/7.0` for a known month and `1` (`ELSE`) for an
/// unknown one — and the whole clause collapses to `1` when the table holds
/// no rows. Both fallbacks reduce to "month not found ⇒ `1.0`". The result
/// is always positive, so it is safe to divide by.
#[must_use]
pub fn weeks_per_month(month_of_any_year: &[MonthOfAnyYearRow], month_id: i32) -> f64 {
    month_of_any_year
        .iter()
        .find(|m| m.month_id == month_id)
        .map(|m| f64::from(m.no_of_days) / 7.0)
        .unwrap_or(1.0)
}

/// Step 170, annual-VMT path — allocate `AnnualVMTByAgeRoadway` to hours.
///
/// Ports the first `VMTByAgeRoadwayHour` insert: annual VMT joined to
/// `MonthVMTFraction` (`AvarMonth`), `DayVMTFraction` (`AvarMonthDay`),
/// `HourVMTFraction` and `HourDay`, yielding
/// `VMT = annualVMT * monthVMTFraction * dayVMTFraction * hourVMTFraction /
/// weeksPerMonth`.
#[must_use]
pub fn hourly_vmt_from_annual(
    annual_vmt_by_age_roadway: &[AnnualVmtByAgeRoadwayRow],
    month_vmt_fraction: &[MonthVmtFractionRow],
    day_vmt_fraction: &[DayVmtFractionRow],
    hour_vmt_fraction: &[HourVmtFractionRow],
    hour_day: &[HourDayRow],
    month_of_any_year: &[MonthOfAnyYearRow],
) -> Vec<VmtByAgeRoadwayHourRow> {
    // sourceTypeID -> [(monthID, monthVMTFraction)].
    let mut months_by_source: BTreeMap<i32, Vec<(i32, f64)>> = BTreeMap::new();
    for m in month_vmt_fraction {
        months_by_source
            .entry(m.source_type_id)
            .or_default()
            .push((m.month_id, m.month_vmt_fraction));
    }
    // (sourceTypeID, monthID, roadTypeID) -> [(dayID, dayVMTFraction)].
    let mut days: BTreeMap<(i32, i32, i32), Vec<(i32, f64)>> = BTreeMap::new();
    for d in day_vmt_fraction {
        days.entry((d.source_type_id, d.month_id, d.road_type_id))
            .or_default()
            .push((d.day_id, d.day_vmt_fraction));
    }
    // (sourceTypeID, roadTypeID, dayID) -> [(hourID, hourVMTFraction)].
    let mut hours: BTreeMap<(i32, i32, i32), Vec<(i32, f64)>> = BTreeMap::new();
    for h in hour_vmt_fraction {
        hours
            .entry((h.source_type_id, h.road_type_id, h.day_id))
            .or_default()
            .push((h.hour_id, h.hour_vmt_fraction));
    }
    // (hourID, dayID) -> hourDayID.
    let hour_day_id: BTreeMap<(i32, i32), i32> = hour_day
        .iter()
        .map(|r| ((r.hour_id, r.day_id), r.hour_day_id))
        .collect();

    let mut out = Vec::new();
    for avar in annual_vmt_by_age_roadway {
        let Some(months) = months_by_source.get(&avar.source_type_id) else {
            continue;
        };
        for &(month_id, month_fraction) in months {
            let weeks = weeks_per_month(month_of_any_year, month_id);
            let Some(day_rows) = days.get(&(avar.source_type_id, month_id, avar.road_type_id))
            else {
                continue;
            };
            for &(day_id, day_fraction) in day_rows {
                let month_day_fraction = month_fraction * day_fraction;
                let Some(hour_rows) = hours.get(&(avar.source_type_id, avar.road_type_id, day_id))
                else {
                    continue;
                };
                for &(hour_id, hour_fraction) in hour_rows {
                    let Some(&hd_id) = hour_day_id.get(&(hour_id, day_id)) else {
                        continue;
                    };
                    out.push(VmtByAgeRoadwayHourRow {
                        year_id: avar.year_id,
                        road_type_id: avar.road_type_id,
                        source_type_id: avar.source_type_id,
                        age_id: avar.age_id,
                        month_id,
                        day_id,
                        hour_id,
                        hour_day_id: hd_id,
                        vmt: avar.vmt * month_day_fraction * hour_fraction / weeks,
                    });
                }
            }
        }
    }
    out
}

/// The shared join tables for the two daily-VMT hourly-allocation paths.
///
/// Grouping them keeps [`hourly_vmt_from_source_type_day`] and
/// [`hourly_vmt_from_hpms_day`] within a readable argument count.
#[derive(Debug, Clone, Copy)]
pub struct DailyVmtJoinTables<'a> {
    /// `RoadTypeDistribution` — the `(sourceType, roadType)` VMT split.
    pub road_type_distribution: &'a [RoadTypeDistributionRow],
    /// `HourDay` — the `(hour, day)` packed-key catalogue.
    pub hour_day: &'a [HourDayRow],
    /// `HourVMTFraction` — the hourly VMT shares.
    pub hour_vmt_fraction: &'a [HourVmtFractionRow],
    /// `TravelFraction` — the per-cohort travel shares.
    pub travel_fraction: &'a [TravelFractionRow],
    /// `DayOfAnyWeek` — the real-days-per-day-type counts.
    pub day_of_any_week: &'a [DayOfAnyWeekRow],
}

/// `hourVMTFraction` keyed `(sourceTypeID, roadTypeID, dayID, hourID)`.
fn hour_fraction_index(rows: &[HourVmtFractionRow]) -> BTreeMap<(i32, i32, i32, i32), f64> {
    rows.iter()
        .map(|r| {
            (
                (r.source_type_id, r.road_type_id, r.day_id, r.hour_id),
                r.hour_vmt_fraction,
            )
        })
        .collect()
}

/// One daily-VMT cell — the `(year, month, day, sourceType, vmt)` tuple the
/// two daily-VMT paths feed into [`expand_daily_vmt`].
#[derive(Debug, Clone, Copy)]
struct DailyVmtCell {
    year_id: i32,
    month_id: i32,
    day_id: i32,
    source_type_id: i32,
    daily_vmt: f64,
}

/// Shared inner loop of the two daily-VMT paths: given a [`DailyVmtCell`],
/// expand it across road types, hours, and ages.
fn expand_daily_vmt(
    cell: DailyVmtCell,
    tables: &DailyVmtJoinTables,
    road_fractions: &BTreeMap<i32, Vec<(i32, f64)>>,
    hour_fraction: &BTreeMap<(i32, i32, i32, i32), f64>,
    out: &mut Vec<VmtByAgeRoadwayHourRow>,
) {
    let Some(roads) = road_fractions.get(&cell.source_type_id) else {
        return;
    };
    let real_days = tables
        .day_of_any_week
        .iter()
        .find(|d| d.day_id == cell.day_id)
        .map(|d| d.no_of_real_days);
    let Some(real_days) = real_days else {
        return;
    };
    for &(road_type_id, road_fraction) in roads {
        for hd in tables.hour_day.iter().filter(|r| r.day_id == cell.day_id) {
            let Some(&hour_fraction) =
                hour_fraction.get(&(cell.source_type_id, road_type_id, cell.day_id, hd.hour_id))
            else {
                continue;
            };
            for tf in tables
                .travel_fraction
                .iter()
                .filter(|r| r.year_id == cell.year_id && r.source_type_id == cell.source_type_id)
            {
                out.push(VmtByAgeRoadwayHourRow {
                    year_id: cell.year_id,
                    road_type_id,
                    source_type_id: cell.source_type_id,
                    age_id: tf.age_id,
                    month_id: cell.month_id,
                    day_id: cell.day_id,
                    hour_id: hd.hour_id,
                    hour_day_id: hd.hour_day_id,
                    vmt: cell.daily_vmt * hour_fraction * road_fraction * tf.fraction * real_days,
                });
            }
        }
    }
}

/// Step 170, daily-VMT-by-source-type path.
///
/// Ports the `insert ignore into VMTByAgeRoadwayHour … from SourceTypeDayVMT`
/// statement: `VMT = dailyVMT * hourVMTFraction * roadTypeVMTFraction *
/// TravelFraction.fraction * DayOfAnyWeek.noOfRealDays`, filtered to the
/// analysis year. The `insert ignore` de-duplication against the
/// annual-VMT path is applied by [`combine_hourly_vmt`].
///
/// # Fidelity note
///
/// The Java joins `hourVMTFraction` on `(hourID, roadTypeID, sourceTypeID)`
/// **without** a `dayID` predicate, so every day type's hourly profile
/// matches and the `insert ignore` resolves the resulting same-key collision
/// in MySQL's (unordered) result-set order. This port constrains
/// `hourVMTFraction.dayID` to the cell's `dayID` — the deterministic,
/// evidently intended reading, since the output row's `dayID` is the
/// `SourceTypeDayVMT` day. Standard runs supply VMT by HPMS annual VMT
/// (the [`hourly_vmt_from_annual`] path), so the daily-VMT paths do not
/// reach the characterization fixtures; the choice is flagged for Task 44's
/// generator-integration validation.
#[must_use]
pub fn hourly_vmt_from_source_type_day(
    source_type_day_vmt: &[SourceTypeDayVmtRow],
    tables: &DailyVmtJoinTables,
    analysis_year: i32,
) -> Vec<VmtByAgeRoadwayHourRow> {
    let mut road_fractions: BTreeMap<i32, Vec<(i32, f64)>> = BTreeMap::new();
    for rtd in tables.road_type_distribution {
        road_fractions
            .entry(rtd.source_type_id)
            .or_default()
            .push((rtd.road_type_id, rtd.road_type_vmt_fraction));
    }
    let hour_fraction = hour_fraction_index(tables.hour_vmt_fraction);

    let mut out = Vec::new();
    for vmt in source_type_day_vmt {
        if vmt.year_id != analysis_year {
            continue;
        }
        expand_daily_vmt(
            DailyVmtCell {
                year_id: vmt.year_id,
                month_id: vmt.month_id,
                day_id: vmt.day_id,
                source_type_id: vmt.source_type_id,
                daily_vmt: vmt.vmt,
            },
            tables,
            &road_fractions,
            &hour_fraction,
            &mut out,
        );
    }
    out
}

/// Step 170, daily-VMT-by-HPMS-type path.
///
/// Ports the `insert ignore into VMTByAgeRoadwayHour … from HPMSVTypeDay`
/// statement — identical to [`hourly_vmt_from_source_type_day`] except the
/// daily VMT is keyed by HPMS vehicle type and joined to the source type
/// through `SourceUseType`.
#[must_use]
pub fn hourly_vmt_from_hpms_day(
    hpms_v_type_day: &[HpmsVTypeDayRow],
    source_use_type: &[SourceUseTypeRow],
    tables: &DailyVmtJoinTables,
    analysis_year: i32,
) -> Vec<VmtByAgeRoadwayHourRow> {
    let mut road_fractions: BTreeMap<i32, Vec<(i32, f64)>> = BTreeMap::new();
    for rtd in tables.road_type_distribution {
        road_fractions
            .entry(rtd.source_type_id)
            .or_default()
            .push((rtd.road_type_id, rtd.road_type_vmt_fraction));
    }
    let hour_fraction = hour_fraction_index(tables.hour_vmt_fraction);
    // HPMSVTypeID -> [sourceTypeID].
    let mut source_types: BTreeMap<i32, Vec<i32>> = BTreeMap::new();
    for sut in source_use_type {
        source_types
            .entry(sut.hpms_v_type_id)
            .or_default()
            .push(sut.source_type_id);
    }

    let mut out = Vec::new();
    for vmt in hpms_v_type_day {
        if vmt.year_id != analysis_year {
            continue;
        }
        let Some(sts) = source_types.get(&vmt.hpms_v_type_id) else {
            continue;
        };
        for &source_type_id in sts {
            expand_daily_vmt(
                DailyVmtCell {
                    year_id: vmt.year_id,
                    month_id: vmt.month_id,
                    day_id: vmt.day_id,
                    source_type_id,
                    daily_vmt: vmt.vmt,
                },
                tables,
                &road_fractions,
                &hour_fraction,
                &mut out,
            );
        }
    }
    out
}

/// The unique key of a `VMTByAgeRoadwayHour` row.
fn hourly_key(r: &VmtByAgeRoadwayHourRow) -> (i32, i32, i32, i32, i32, i32, i32) {
    (
        r.year_id,
        r.road_type_id,
        r.source_type_id,
        r.age_id,
        r.month_id,
        r.day_id,
        r.hour_id,
    )
}

/// Merge the three step-170 hourly-VMT paths under the Java's `insert` /
/// `insert ignore` semantics.
///
/// The annual-VMT path is an `INSERT INTO`; the two daily-VMT paths are
/// `insert ignore`, so a daily-VMT row is kept only when its
/// `(year, roadType, sourceType, age, month, day, hour)` key was not
/// already produced — first by the annual path, then by the source-type
/// path ahead of the HPMS path (statement order).
#[must_use]
pub fn combine_hourly_vmt(
    from_annual: Vec<VmtByAgeRoadwayHourRow>,
    from_source_type_day: Vec<VmtByAgeRoadwayHourRow>,
    from_hpms_day: Vec<VmtByAgeRoadwayHourRow>,
) -> Vec<VmtByAgeRoadwayHourRow> {
    let mut seen: BTreeSet<(i32, i32, i32, i32, i32, i32, i32)> = BTreeSet::new();
    let mut out = Vec::new();
    for row in from_annual {
        seen.insert(hourly_key(&row));
        out.push(row);
    }
    for row in from_source_type_day.into_iter().chain(from_hpms_day) {
        if seen.insert(hourly_key(&row)) {
            out.push(row);
        }
    }
    out.sort_by_key(|r| {
        (
            r.year_id,
            r.road_type_id,
            r.source_type_id,
            r.age_id,
            r.month_id,
            r.day_id,
            r.hour_id,
        )
    });
    out
}

/// Step 170, final — per-model-year VMT fractions.
///
/// Ports the `vmtByMYRoadHourSummary` / `vmtByMYRoadHourFraction` inserts:
/// the summary totals VMT over `(year, roadType, sourceType, month, hour,
/// day)` keeping only positive totals, and the fraction is
/// `vmtFraction = VMT / totalVMT` with `modelYearID = yearID - ageID`.
#[must_use]
pub fn vmt_by_my_road_hour_fraction(
    vmt_by_age_roadway_hour: &[VmtByAgeRoadwayHourRow],
) -> Vec<VmtByMyRoadHourFractionRow> {
    // (year, road, source, month, day, hour) -> total VMT.
    let mut totals: BTreeMap<(i32, i32, i32, i32, i32, i32), f64> = BTreeMap::new();
    for v in vmt_by_age_roadway_hour {
        *totals
            .entry((
                v.year_id,
                v.road_type_id,
                v.source_type_id,
                v.month_id,
                v.day_id,
                v.hour_id,
            ))
            .or_insert(0.0) += v.vmt;
    }

    let mut out = Vec::new();
    for v in vmt_by_age_roadway_hour {
        let key = (
            v.year_id,
            v.road_type_id,
            v.source_type_id,
            v.month_id,
            v.day_id,
            v.hour_id,
        );
        let Some(&total) = totals.get(&key) else {
            continue;
        };
        // The summary `HAVING sum(VMT) > 0` drops non-positive totals.
        if total <= 0.0 {
            continue;
        }
        out.push(VmtByMyRoadHourFractionRow {
            year_id: v.year_id,
            road_type_id: v.road_type_id,
            source_type_id: v.source_type_id,
            model_year_id: v.year_id - v.age_id,
            month_id: v.month_id,
            hour_id: v.hour_id,
            day_id: v.day_id,
            hour_day_id: v.hour_day_id,
            vmt_fraction: v.vmt / total,
        });
    }
    out.sort_by_key(|r| {
        (
            r.year_id,
            r.road_type_id,
            r.source_type_id,
            r.model_year_id,
            r.month_id,
            r.day_id,
            r.hour_id,
        )
    });
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPS: f64 = 1e-9;

    fn tf(year: i32, st: i32, age: i32, fraction: f64) -> TravelFractionRow {
        TravelFractionRow {
            year_id: year,
            source_type_id: st,
            age_id: age,
            fraction,
        }
    }

    fn rtd(st: i32, road: i32, fraction: f64) -> RoadTypeDistributionRow {
        RoadTypeDistributionRow {
            source_type_id: st,
            road_type_id: road,
            road_type_vmt_fraction: fraction,
        }
    }

    fn annual(year: i32, road: i32, st: i32, age: i32, vmt: f64) -> AnnualVmtByAgeRoadwayRow {
        AnnualVmtByAgeRoadwayRow {
            year_id: year,
            road_type_id: road,
            source_type_id: st,
            age_id: age,
            vmt,
        }
    }

    #[test]
    fn allocate_vmt_hpms_path() {
        // HPMS type 10 has 1000 VMT; source type 21 rolls up to it, with
        // 60% of VMT on road type 2 and a 0.5 travel fraction at age 0.
        let travel = [tf(2020, 21, 0, 0.5)];
        let roads = [RoadTypeRow { road_type_id: 2 }];
        let rtds = [rtd(21, 2, 0.6)];
        let ayv = [AnalysisYearVmtRow {
            year_id: 2020,
            hpms_v_type_id: 10,
            vmt: 1000.0,
        }];
        let suts = [SourceUseTypeRow {
            source_type_id: 21,
            hpms_v_type_id: 10,
        }];
        let out =
            allocate_vmt_by_road_type_source_age(&travel, &roads, &rtds, &ayv, &suts, &[], 2020);
        assert_eq!(out.len(), 1);
        // 1000 * 0.6 * 0.5 = 300.
        assert!((out[0].vmt - 300.0).abs() < EPS);
    }

    #[test]
    fn allocate_vmt_source_type_path() {
        let travel = [tf(2020, 21, 0, 0.5)];
        let roads = [RoadTypeRow { road_type_id: 2 }];
        let rtds = [rtd(21, 2, 0.6)];
        let styv = [SourceTypeYearVmtRow {
            year_id: 2020,
            source_type_id: 21,
            vmt: 2000.0,
        }];
        let out =
            allocate_vmt_by_road_type_source_age(&travel, &roads, &rtds, &[], &[], &styv, 2020);
        assert_eq!(out.len(), 1);
        // 2000 * 0.6 * 0.5 = 600.
        assert!((out[0].vmt - 600.0).abs() < EPS);
    }

    #[test]
    fn allocate_vmt_skips_road_types_not_in_road_type() {
        let travel = [tf(2020, 21, 0, 1.0)];
        // RoadType has only road 2; the distribution mentions road 9 too.
        let roads = [RoadTypeRow { road_type_id: 2 }];
        let rtds = [rtd(21, 2, 0.5), rtd(21, 9, 0.5)];
        let styv = [SourceTypeYearVmtRow {
            year_id: 2020,
            source_type_id: 21,
            vmt: 1000.0,
        }];
        let out =
            allocate_vmt_by_road_type_source_age(&travel, &roads, &rtds, &[], &[], &styv, 2020);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].road_type_id, 2);
    }

    #[test]
    fn weeks_per_month_known_and_unknown() {
        let moy = [MonthOfAnyYearRow {
            month_id: 1,
            no_of_days: 28,
        }];
        assert!((weeks_per_month(&moy, 1) - 4.0).abs() < EPS);
        // Unknown month falls back to 1.
        assert!((weeks_per_month(&moy, 7) - 1.0).abs() < EPS);
    }

    #[test]
    fn hourly_vmt_from_annual_applies_all_fractions() {
        let annual_rows = [annual(2020, 2, 21, 0, 7000.0)];
        let mvf = [MonthVmtFractionRow {
            source_type_id: 21,
            month_id: 1,
            month_vmt_fraction: 0.5,
        }];
        let dvf = [DayVmtFractionRow {
            source_type_id: 21,
            month_id: 1,
            road_type_id: 2,
            day_id: 5,
            day_vmt_fraction: 0.4,
        }];
        let hvf = [HourVmtFractionRow {
            source_type_id: 21,
            road_type_id: 2,
            day_id: 5,
            hour_id: 8,
            hour_vmt_fraction: 0.1,
        }];
        let hd = [HourDayRow {
            hour_day_id: 85,
            hour_id: 8,
            day_id: 5,
        }];
        // 7 days in the month -> weeksPerMonth = 1.0.
        let moy = [MonthOfAnyYearRow {
            month_id: 1,
            no_of_days: 7,
        }];
        let out = hourly_vmt_from_annual(&annual_rows, &mvf, &dvf, &hvf, &hd, &moy);
        assert_eq!(out.len(), 1);
        // 7000 * (0.5*0.4) * 0.1 / 1.0 = 140.
        assert!((out[0].vmt - 140.0).abs() < EPS);
        assert_eq!(out[0].hour_day_id, 85);
    }

    #[test]
    fn hourly_vmt_from_source_type_day_path() {
        let stdv = [SourceTypeDayVmtRow {
            year_id: 2020,
            month_id: 1,
            day_id: 5,
            source_type_id: 21,
            vmt: 1000.0,
        }];
        let rtds = [rtd(21, 2, 0.5)];
        let hd = [HourDayRow {
            hour_day_id: 85,
            hour_id: 8,
            day_id: 5,
        }];
        let hvf = [HourVmtFractionRow {
            source_type_id: 21,
            road_type_id: 2,
            day_id: 5,
            hour_id: 8,
            hour_vmt_fraction: 0.25,
        }];
        let travel = [tf(2020, 21, 3, 0.8)];
        let dow = [DayOfAnyWeekRow {
            day_id: 5,
            no_of_real_days: 2.0,
        }];
        let tables = DailyVmtJoinTables {
            road_type_distribution: &rtds,
            hour_day: &hd,
            hour_vmt_fraction: &hvf,
            travel_fraction: &travel,
            day_of_any_week: &dow,
        };
        let out = hourly_vmt_from_source_type_day(&stdv, &tables, 2020);
        assert_eq!(out.len(), 1);
        // 1000 * 0.25 * 0.5 * 0.8 * 2 = 200.
        assert!((out[0].vmt - 200.0).abs() < EPS);
        assert_eq!(out[0].age_id, 3);
    }

    #[test]
    fn combine_hourly_vmt_ignores_duplicate_keys() {
        let row = |vmt| VmtByAgeRoadwayHourRow {
            year_id: 2020,
            road_type_id: 2,
            source_type_id: 21,
            age_id: 0,
            month_id: 1,
            day_id: 5,
            hour_id: 8,
            hour_day_id: 85,
            vmt,
        };
        // The annual path wins; the daily path's same-key row is ignored.
        let merged = combine_hourly_vmt(vec![row(100.0)], vec![row(999.0)], vec![]);
        assert_eq!(merged.len(), 1);
        assert!((merged[0].vmt - 100.0).abs() < EPS);
    }

    #[test]
    fn vmt_by_my_road_hour_fraction_is_share_of_cell_total() {
        // Two ages in the same (year,road,source,month,day,hour) cell.
        let v = |age, vmt| VmtByAgeRoadwayHourRow {
            year_id: 2020,
            road_type_id: 2,
            source_type_id: 21,
            age_id: age,
            month_id: 1,
            day_id: 5,
            hour_id: 8,
            hour_day_id: 85,
            vmt,
        };
        let hourly = [v(0, 300.0), v(1, 100.0)];
        let out = vmt_by_my_road_hour_fraction(&hourly);
        assert_eq!(out.len(), 2);
        // age 0 -> modelYear 2020, fraction 300/400 = 0.75.
        let age0 = out.iter().find(|r| r.model_year_id == 2020).unwrap();
        assert!((age0.vmt_fraction - 0.75).abs() < EPS);
        // age 1 -> modelYear 2019, fraction 100/400 = 0.25.
        let age1 = out.iter().find(|r| r.model_year_id == 2019).unwrap();
        assert!((age1.vmt_fraction - 0.25).abs() < EPS);
    }
}

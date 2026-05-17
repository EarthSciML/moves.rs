//! Conversion of VMT to total-activity basis — algorithm steps 180-189.
//!
//! Ports `TotalActivityGenerator.java`'s `convertVMTToTotalActivityBasis`.
//!
//! This is where the generator earns its name: hourly VMT becomes **source
//! hours operating** (`SHO = VMT / averageSpeed`), the combination
//! long-haul daily VMT becomes **hotelling hours**, the sample-vehicle trip
//! counts become **starts**, and the population not accounted for by `SHO`
//! becomes **source hours parked** (`SHP`).

use std::collections::{BTreeMap, BTreeSet};

use super::inputs::{
    AvgSpeedBinRow, AvgSpeedDistributionRow, DayOfAnyWeekRow, HotellingCalendarYearRow, HourDayRow,
    HourOfAnyDayRow, RoadTypeRow, RunSpecDayRow, RunSpecSourceTypeRow, SampleVehicleDayRow,
    SampleVehicleTripRow, SourceTypeHourRow, StartsPerVehicleRow, ZoneRoadTypeRow,
};
use super::model::{
    AverageSpeedRow, IdleHoursByAgeHourRow, ShoByAgeRoadwayHourRow, ShpByAgeHourRow,
    SourceTypeAgePopulationRow, StartsByAgeHourRow, VmtByAgeRoadwayDayRow, VmtByAgeRoadwayHourRow,
};

/// MOVES `sourceTypeID` for Combination Long-haul Trucks — the only vehicle
/// type that hotels (idles overnight). The Java hard-codes `62` in the
/// `VMTByAgeRoadwayDay` aggregation.
pub const COMBINATION_LONG_HAUL_TRUCK: i32 = 62;

/// One `SourceTypeHour2` row — `SourceTypeHour` expanded with the `(day,
/// hour)` pair behind its `hourDayID`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypeHour2Row {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `dayID`.
    pub day_id: i32,
    /// `hourID`.
    pub hour_id: i32,
    /// `idleSHOFactor` — carried from `SourceTypeHour` for fidelity; step
    /// 180 itself reads only `hotellingDist`.
    pub idle_sho_factor: f64,
    /// `hotellingDist` — hourly distribution weight for hotelling hours.
    pub hotelling_dist: f64,
}

/// One `StartsPerSampleVehicle` row — engine starts counted from the trip
/// sample for a `(sourceType, hourDay)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StartsPerSampleVehicleRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `starts` — `count(trips) * noOfRealDays`.
    pub starts: f64,
    /// `dayID` — the day type behind `hourDayID`, kept for the
    /// `StartsPerVehicle` divisor join.
    pub day_id: i32,
}

/// Step 180a — expand `SourceTypeHour` to `(sourceType, day, hour)`.
///
/// Ports the `delete from sourceTypeHour …` / `CREATE TABLE SourceTypeHour2`
/// pair. The delete drops rows whose `hourDayID` resolves to a day type not
/// in `RunSpecDay`; the create joins `HourDay` to recover the `(day, hour)`
/// pair. Both reduce to: keep a `SourceTypeHour` row only when its
/// `hourDayID` belongs to a `HourDay` whose `dayID` is in `RunSpecDay`.
#[must_use]
pub fn source_type_hour_expanded(
    source_type_hour: &[SourceTypeHourRow],
    hour_day: &[HourDayRow],
    run_spec_day: &[RunSpecDayRow],
) -> Vec<SourceTypeHour2Row> {
    let run_spec_days: BTreeSet<i32> = run_spec_day.iter().map(|r| r.day_id).collect();
    // hourDayID -> (hourID, dayID) for in-RunSpec day types only.
    let hour_day_of: BTreeMap<i32, (i32, i32)> = hour_day
        .iter()
        .filter(|r| run_spec_days.contains(&r.day_id))
        .map(|r| (r.hour_day_id, (r.hour_id, r.day_id)))
        .collect();

    let mut out = Vec::new();
    for sth in source_type_hour {
        let Some(&(hour_id, day_id)) = hour_day_of.get(&sth.hour_day_id) else {
            continue;
        };
        out.push(SourceTypeHour2Row {
            source_type_id: sth.source_type_id,
            day_id,
            hour_id,
            idle_sho_factor: sth.idle_sho_factor,
            hotelling_dist: sth.hotelling_dist,
        });
    }
    out.sort_by_key(|r| (r.source_type_id, r.day_id, r.hour_id));
    out
}

/// Step 180b — activity-weighted average speed.
///
/// Ports the `AverageSpeed` insert: `averageSpeed = sum(AvgSpeedBin.
/// avgBinSpeed * AvgSpeedDistribution.avgSpeedFraction)`, grouped by
/// `(roadType, sourceType, day, hour)`. The road type must be in
/// `RoadType`, the source type in `RunSpecSourceType`, and the
/// `AvgSpeedDistribution`'s `hourDayID` must resolve to a `HourDay` whose
/// `dayID` is in `RunSpecDay` and whose `hourID` is in `HourOfAnyDay`.
#[must_use]
pub fn average_speed(
    road_type: &[RoadTypeRow],
    run_spec_source_type: &[RunSpecSourceTypeRow],
    run_spec_day: &[RunSpecDayRow],
    hour_of_any_day: &[HourOfAnyDayRow],
    avg_speed_bin: &[AvgSpeedBinRow],
    avg_speed_distribution: &[AvgSpeedDistributionRow],
    hour_day: &[HourDayRow],
) -> Vec<AverageSpeedRow> {
    let road_types: BTreeSet<i32> = road_type.iter().map(|r| r.road_type_id).collect();
    let source_types: BTreeSet<i32> = run_spec_source_type
        .iter()
        .map(|r| r.source_type_id)
        .collect();
    let run_spec_days: BTreeSet<i32> = run_spec_day.iter().map(|r| r.day_id).collect();
    let hours: BTreeSet<i32> = hour_of_any_day.iter().map(|r| r.hour_id).collect();
    let bin_speed: BTreeMap<i32, f64> = avg_speed_bin
        .iter()
        .map(|r| (r.avg_speed_bin_id, r.avg_bin_speed))
        .collect();
    // hourDayID -> (hourID, dayID), gated to RunSpec day types and known hours.
    let hour_day_of: BTreeMap<i32, (i32, i32)> = hour_day
        .iter()
        .filter(|r| run_spec_days.contains(&r.day_id) && hours.contains(&r.hour_id))
        .map(|r| (r.hour_day_id, (r.hour_id, r.day_id)))
        .collect();

    let mut totals: BTreeMap<(i32, i32, i32, i32), f64> = BTreeMap::new();
    for asd in avg_speed_distribution {
        if !road_types.contains(&asd.road_type_id) || !source_types.contains(&asd.source_type_id) {
            continue;
        }
        let Some(&(hour_id, day_id)) = hour_day_of.get(&asd.hour_day_id) else {
            continue;
        };
        let Some(&speed) = bin_speed.get(&asd.avg_speed_bin_id) else {
            continue;
        };
        *totals
            .entry((asd.road_type_id, asd.source_type_id, day_id, hour_id))
            .or_insert(0.0) += speed * asd.avg_speed_fraction;
    }
    totals
        .into_iter()
        .map(
            |((road_type_id, source_type_id, day_id, hour_id), average_speed)| AverageSpeedRow {
                road_type_id,
                source_type_id,
                day_id,
                hour_id,
                average_speed,
            },
        )
        .collect()
}

/// Step 180c — source hours operating, `SHO = VMT / averageSpeed`.
///
/// Ports the `SHOByAgeRoadwayHour` insert: `SHO = IF(averageSpeed <> 0,
/// COALESCE(VMT / averageSpeed, 0), 0)` via a `LEFT JOIN` to `AverageSpeed`
/// on `(roadType, sourceType, day, hour)`. A missing or zero average speed
/// yields `SHO = 0`; the originating `VMT` is carried through unchanged.
#[must_use]
pub fn sho_by_age_roadway_hour(
    vmt_by_age_roadway_hour: &[VmtByAgeRoadwayHourRow],
    average_speed: &[AverageSpeedRow],
) -> Vec<ShoByAgeRoadwayHourRow> {
    let speed_of: BTreeMap<(i32, i32, i32, i32), f64> = average_speed
        .iter()
        .map(|r| {
            (
                (r.road_type_id, r.source_type_id, r.day_id, r.hour_id),
                r.average_speed,
            )
        })
        .collect();

    vmt_by_age_roadway_hour
        .iter()
        .map(|v| {
            let speed = speed_of
                .get(&(v.road_type_id, v.source_type_id, v.day_id, v.hour_id))
                .copied()
                .unwrap_or(0.0);
            let sho = if speed != 0.0 { v.vmt / speed } else { 0.0 };
            ShoByAgeRoadwayHourRow {
                year_id: v.year_id,
                road_type_id: v.road_type_id,
                source_type_id: v.source_type_id,
                age_id: v.age_id,
                month_id: v.month_id,
                day_id: v.day_id,
                hour_id: v.hour_id,
                hour_day_id: v.hour_day_id,
                sho,
                vmt: v.vmt,
            }
        })
        .collect()
}

/// Step 180d — daily VMT and hotelling hours for combination long-haul
/// trucks.
///
/// Ports the `VMTByAgeRoadwayDay` insert and its two follow-up statements.
/// Hourly VMT for [`COMBINATION_LONG_HAUL_TRUCK`] is summed to a daily total;
/// `hotellingHours = dailyVMT * ZoneRoadType.SHOAllocFactor *
/// HotellingCalendarYear.hotellingRate` (`0` when either factor is absent).
///
/// The Java then deletes rows on road types other than 2 and 4 (rural and
/// urban restricted access) — unless there is *no* hotelling activity on
/// roads 2/4 *and* the user supplied a `hotellingHoursPerDay` table, the
/// case `has_hotelling_hours_per_day_input` covers.
#[must_use]
pub fn vmt_by_age_roadway_day(
    vmt_by_age_roadway_hour: &[VmtByAgeRoadwayHourRow],
    zone_road_type: &[ZoneRoadTypeRow],
    hotelling_calendar_year: &[HotellingCalendarYearRow],
    zone_id: i32,
    has_hotelling_hours_per_day_input: bool,
) -> Vec<VmtByAgeRoadwayDayRow> {
    // (zoneID, roadTypeID) -> SHOAllocFactor and yearID -> hotellingRate.
    let sho_alloc: BTreeMap<(i32, i32), f64> = zone_road_type
        .iter()
        .map(|r| ((r.zone_id, r.road_type_id), r.sho_alloc_factor))
        .collect();
    let hotelling_rate: BTreeMap<i32, f64> = hotelling_calendar_year
        .iter()
        .map(|r| (r.year_id, r.hotelling_rate))
        .collect();

    // Sum hourly VMT to daily totals for the long-haul source type.
    let mut daily: BTreeMap<(i32, i32, i32, i32, i32), f64> = BTreeMap::new();
    for v in vmt_by_age_roadway_hour {
        if v.source_type_id != COMBINATION_LONG_HAUL_TRUCK {
            continue;
        }
        *daily
            .entry((v.year_id, v.road_type_id, v.age_id, v.month_id, v.day_id))
            .or_insert(0.0) += v.vmt;
    }

    let mut rows: Vec<VmtByAgeRoadwayDayRow> = daily
        .into_iter()
        .map(|((year_id, road_type_id, age_id, month_id, day_id), vmt)| {
            let hotelling_hours = match (
                sho_alloc.get(&(zone_id, road_type_id)),
                hotelling_rate.get(&year_id),
            ) {
                (Some(&factor), Some(&rate)) => vmt * factor * rate,
                _ => 0.0,
            };
            VmtByAgeRoadwayDayRow {
                year_id,
                road_type_id,
                source_type_id: COMBINATION_LONG_HAUL_TRUCK,
                age_id,
                month_id,
                day_id,
                vmt,
                hotelling_hours,
            }
        })
        .collect();

    // Delete non-restricted-access road types unless there is no restricted-
    // access hotelling activity and the user supplied hotelling input.
    let restricted_hotelling: f64 = rows
        .iter()
        .filter(|r| r.road_type_id == 2 || r.road_type_id == 4)
        .map(|r| r.hotelling_hours)
        .sum();
    let keep_other_roads = restricted_hotelling == 0.0 && has_hotelling_hours_per_day_input;
    if !keep_other_roads {
        rows.retain(|r| r.road_type_id == 2 || r.road_type_id == 4);
    }
    rows
}

/// Step 180e — hotelling activity distributed to hours.
///
/// Ports the `IdleHoursByAgeHour` insert: `idleHours =
/// sum(VMTByAgeRoadwayDay.hotellingHours * SourceTypeHour2.hotellingDist)`,
/// joined on `(sourceType, day)` and grouped by `(year, sourceType, age,
/// month, day, hour)`.
#[must_use]
pub fn idle_hours_by_age_hour(
    vmt_by_age_roadway_day: &[VmtByAgeRoadwayDayRow],
    source_type_hour_2: &[SourceTypeHour2Row],
) -> Vec<IdleHoursByAgeHourRow> {
    // (sourceTypeID, dayID) -> [(hourID, hotellingDist)].
    let mut hours_by_source_day: BTreeMap<(i32, i32), Vec<(i32, f64)>> = BTreeMap::new();
    for sth in source_type_hour_2 {
        hours_by_source_day
            .entry((sth.source_type_id, sth.day_id))
            .or_default()
            .push((sth.hour_id, sth.hotelling_dist));
    }

    let mut totals: BTreeMap<(i32, i32, i32, i32, i32, i32), f64> = BTreeMap::new();
    for v in vmt_by_age_roadway_day {
        let Some(hours) = hours_by_source_day.get(&(v.source_type_id, v.day_id)) else {
            continue;
        };
        for &(hour_id, hotelling_dist) in hours {
            *totals
                .entry((
                    v.year_id,
                    v.source_type_id,
                    v.age_id,
                    v.month_id,
                    v.day_id,
                    hour_id,
                ))
                .or_insert(0.0) += v.hotelling_hours * hotelling_dist;
        }
    }
    totals
        .into_iter()
        .map(
            |((year_id, source_type_id, age_id, month_id, day_id, hour_id), idle_hours)| {
                IdleHoursByAgeHourRow {
                    year_id,
                    source_type_id,
                    age_id,
                    month_id,
                    day_id,
                    hour_id,
                    idle_hours,
                }
            },
        )
        .collect()
}

/// Step 180f — engine starts counted from the trip sample.
///
/// Ports the `StartsPerSampleVehicle` insert: `starts = count(trips) *
/// noOfRealDays`, counting `SampleVehicleTrip` rows with a non-null
/// `keyOnTime` (the Java's "ignore marker trips" filter), joined to
/// `SampleVehicleDay` on `vehID` and to `HourDay`/`DayOfAnyWeek`, grouped
/// by `(sourceType, hourDay)`.
#[must_use]
pub fn starts_per_sample_vehicle(
    sample_vehicle_day: &[SampleVehicleDayRow],
    sample_vehicle_trip: &[SampleVehicleTripRow],
    hour_day: &[HourDayRow],
    day_of_any_week: &[DayOfAnyWeekRow],
) -> Vec<StartsPerSampleVehicleRow> {
    // vehID -> sourceTypeID (SampleVehicleDay is keyed by (vehID, dayID); the
    // sourceTypeID is a property of the vehicle, constant across its days).
    let source_type_of_veh: BTreeMap<i32, i32> = sample_vehicle_day
        .iter()
        .map(|r| (r.veh_id, r.source_type_id))
        .collect();
    // (hourID, dayID) -> hourDayID.
    let hour_day_id: BTreeMap<(i32, i32), i32> = hour_day
        .iter()
        .map(|r| ((r.hour_id, r.day_id), r.hour_day_id))
        .collect();
    let real_days: BTreeMap<i32, f64> = day_of_any_week
        .iter()
        .map(|r| (r.day_id, r.no_of_real_days))
        .collect();

    // Count trips per (sourceTypeID, hourDayID); remember the dayID.
    let mut counts: BTreeMap<(i32, i32), (i32, i32)> = BTreeMap::new();
    for trip in sample_vehicle_trip {
        if !trip.has_key_on_time {
            continue;
        }
        let Some(&source_type_id) = source_type_of_veh.get(&trip.veh_id) else {
            continue;
        };
        let Some(&hd_id) = hour_day_id.get(&(trip.hour_id, trip.day_id)) else {
            continue;
        };
        let entry = counts
            .entry((source_type_id, hd_id))
            .or_insert((0, trip.day_id));
        entry.0 += 1;
    }

    counts
        .into_iter()
        .filter_map(|((source_type_id, hour_day_id), (count, day_id))| {
            let &no_of_real_days = real_days.get(&day_id)?;
            Some(StartsPerSampleVehicleRow {
                source_type_id,
                hour_day_id,
                // `i32` -> `f64` is lossless: trip counts are small.
                starts: f64::from(count) * no_of_real_days,
                day_id,
            })
        })
        .collect()
}

/// Step 180g — engine starts per vehicle.
///
/// Ports the `StartsPerVehicle` insert: `startsPerVehicle = starts /
/// count(SampleVehicleDay)`, where the divisor counts the sample-vehicle
/// days for the row's source type and the `StartsPerSampleVehicle`'s day
/// type. Rows are produced **only for source types absent** from
/// `existing_starts_per_vehicle` — the Java `WHERE
/// SourceTypesInStartsPerVehicle.sourceTypeID IS NULL` guard.
#[must_use]
pub fn starts_per_vehicle(
    sample_vehicle_day: &[SampleVehicleDayRow],
    starts_per_sample_vehicle: &[StartsPerSampleVehicleRow],
    existing_starts_per_vehicle: &[StartsPerVehicleRow],
) -> Vec<StartsPerVehicleRow> {
    let already_present: BTreeSet<i32> = existing_starts_per_vehicle
        .iter()
        .map(|r| r.source_type_id)
        .collect();
    // (sourceTypeID, dayID) -> count of SampleVehicleDay rows.
    let mut sample_day_count: BTreeMap<(i32, i32), i32> = BTreeMap::new();
    for sv in sample_vehicle_day {
        *sample_day_count
            .entry((sv.source_type_id, sv.day_id))
            .or_insert(0) += 1;
    }

    let mut out = Vec::new();
    for ssv in starts_per_sample_vehicle {
        if already_present.contains(&ssv.source_type_id) {
            continue;
        }
        let Some(&count) = sample_day_count.get(&(ssv.source_type_id, ssv.day_id)) else {
            continue;
        };
        if count == 0 {
            continue;
        }
        out.push(StartsPerVehicleRow {
            source_type_id: ssv.source_type_id,
            hour_day_id: ssv.hour_day_id,
            // `i32` -> `f64` is lossless: sample-vehicle counts are small.
            starts_per_vehicle: ssv.starts / f64::from(count),
        });
    }
    out.sort_by_key(|r| (r.source_type_id, r.hour_day_id));
    out
}

/// Step 180h — engine starts by age and hour.
///
/// Ports the `StartsByAgeHour` insert: `starts = SourceTypeAgePopulation.
/// population * StartsPerVehicle.startsPerVehicle`, joined on `sourceTypeID`.
/// `starts_per_vehicle` is the *full* table — the rows already present plus
/// any newly computed by [`starts_per_vehicle`].
#[must_use]
pub fn starts_by_age_hour(
    source_type_age_population: &[SourceTypeAgePopulationRow],
    starts_per_vehicle: &[StartsPerVehicleRow],
) -> Vec<StartsByAgeHourRow> {
    // sourceTypeID -> [(hourDayID, startsPerVehicle)].
    let mut starts_by_source: BTreeMap<i32, Vec<(i32, f64)>> = BTreeMap::new();
    for spv in starts_per_vehicle {
        starts_by_source
            .entry(spv.source_type_id)
            .or_default()
            .push((spv.hour_day_id, spv.starts_per_vehicle));
    }

    let mut out = Vec::new();
    for stap in source_type_age_population {
        let Some(rows) = starts_by_source.get(&stap.source_type_id) else {
            continue;
        };
        for &(hour_day_id, starts_per_vehicle) in rows {
            out.push(StartsByAgeHourRow {
                source_type_id: stap.source_type_id,
                year_id: stap.year_id,
                hour_day_id,
                age_id: stap.age_id,
                starts: stap.population * starts_per_vehicle,
            });
        }
    }
    out.sort_by_key(|r| (r.source_type_id, r.year_id, r.hour_day_id, r.age_id));
    out
}

/// Step 180i — source hours parked, `SHP = population - SHO`.
///
/// Ports the `SHPByAgeHour` insert: `SHP = (population * noOfRealDays) -
/// sum(SHO)`. Only `SHOByAgeRoadwayHour` rows with `VMT > 0` contribute; the
/// sum is over road types within each `(year, sourceType, age, month, day,
/// hour)` cell. A cell whose source type/age has no `SourceTypeAgePopulation`
/// row, or whose day type has no `DayOfAnyWeek` row, is dropped by the inner
/// joins.
#[must_use]
pub fn shp_by_age_hour(
    sho_by_age_roadway_hour: &[ShoByAgeRoadwayHourRow],
    source_type_age_population: &[SourceTypeAgePopulationRow],
    day_of_any_week: &[DayOfAnyWeekRow],
) -> Vec<ShpByAgeHourRow> {
    let population_of: BTreeMap<(i32, i32, i32), f64> = source_type_age_population
        .iter()
        .map(|r| ((r.year_id, r.source_type_id, r.age_id), r.population))
        .collect();
    let real_days: BTreeMap<i32, f64> = day_of_any_week
        .iter()
        .map(|r| (r.day_id, r.no_of_real_days))
        .collect();

    // Sum SHO over road types within each (year, source, age, month, day, hour).
    let mut sho_sum: BTreeMap<(i32, i32, i32, i32, i32, i32), f64> = BTreeMap::new();
    for sarh in sho_by_age_roadway_hour {
        if sarh.vmt <= 0.0 {
            continue;
        }
        // Drop the cell unless both inner-join partners exist.
        if !population_of.contains_key(&(sarh.year_id, sarh.source_type_id, sarh.age_id))
            || !real_days.contains_key(&sarh.day_id)
        {
            continue;
        }
        *sho_sum
            .entry((
                sarh.year_id,
                sarh.source_type_id,
                sarh.age_id,
                sarh.month_id,
                sarh.day_id,
                sarh.hour_id,
            ))
            .or_insert(0.0) += sarh.sho;
    }

    sho_sum
        .into_iter()
        .map(
            |((year_id, source_type_id, age_id, month_id, day_id, hour_id), sho_total)| {
                let population = population_of[&(year_id, source_type_id, age_id)];
                let no_of_real_days = real_days[&day_id];
                ShpByAgeHourRow {
                    year_id,
                    source_type_id,
                    age_id,
                    month_id,
                    day_id,
                    hour_id,
                    shp: population * no_of_real_days - sho_total,
                }
            },
        )
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPS: f64 = 1e-9;

    fn varh(road: i32, st: i32, age: i32, day: i32, hour: i32, vmt: f64) -> VmtByAgeRoadwayHourRow {
        VmtByAgeRoadwayHourRow {
            year_id: 2020,
            road_type_id: road,
            source_type_id: st,
            age_id: age,
            month_id: 1,
            day_id: day,
            hour_id: hour,
            hour_day_id: hour * 10 + day,
            vmt,
        }
    }

    #[test]
    fn source_type_hour_keeps_only_runspec_days() {
        let sth = [
            SourceTypeHourRow {
                source_type_id: 21,
                hour_day_id: 85,
                idle_sho_factor: 0.1,
                hotelling_dist: 0.5,
            },
            SourceTypeHourRow {
                source_type_id: 21,
                hour_day_id: 86,
                idle_sho_factor: 0.2,
                hotelling_dist: 0.6,
            },
        ];
        let hd = [
            HourDayRow {
                hour_day_id: 85,
                hour_id: 8,
                day_id: 5,
            },
            HourDayRow {
                hour_day_id: 86,
                hour_id: 8,
                day_id: 6,
            },
        ];
        // RunSpec selects day 5 only.
        let rsd = [RunSpecDayRow { day_id: 5 }];
        let out = source_type_hour_expanded(&sth, &hd, &rsd);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].day_id, 5);
        assert!((out[0].hotelling_dist - 0.5).abs() < EPS);
    }

    #[test]
    fn average_speed_is_bin_weighted_sum() {
        let road = [RoadTypeRow { road_type_id: 2 }];
        let rsst = [RunSpecSourceTypeRow { source_type_id: 21 }];
        let rsd = [RunSpecDayRow { day_id: 5 }];
        let hoad = [HourOfAnyDayRow { hour_id: 8 }];
        let asb = [
            AvgSpeedBinRow {
                avg_speed_bin_id: 1,
                avg_bin_speed: 20.0,
            },
            AvgSpeedBinRow {
                avg_speed_bin_id: 2,
                avg_bin_speed: 60.0,
            },
        ];
        let asd = [
            AvgSpeedDistributionRow {
                road_type_id: 2,
                source_type_id: 21,
                hour_day_id: 85,
                avg_speed_bin_id: 1,
                avg_speed_fraction: 0.25,
            },
            AvgSpeedDistributionRow {
                road_type_id: 2,
                source_type_id: 21,
                hour_day_id: 85,
                avg_speed_bin_id: 2,
                avg_speed_fraction: 0.75,
            },
        ];
        let hd = [HourDayRow {
            hour_day_id: 85,
            hour_id: 8,
            day_id: 5,
        }];
        let out = average_speed(&road, &rsst, &rsd, &hoad, &asb, &asd, &hd);
        assert_eq!(out.len(), 1);
        // 20*0.25 + 60*0.75 = 5 + 45 = 50.
        assert!((out[0].average_speed - 50.0).abs() < EPS);
    }

    #[test]
    fn sho_is_vmt_over_average_speed() {
        let hourly = [varh(2, 21, 0, 5, 8, 500.0)];
        let speed = [AverageSpeedRow {
            road_type_id: 2,
            source_type_id: 21,
            day_id: 5,
            hour_id: 8,
            average_speed: 50.0,
        }];
        let out = sho_by_age_roadway_hour(&hourly, &speed);
        assert_eq!(out.len(), 1);
        // 500 / 50 = 10.
        assert!((out[0].sho - 10.0).abs() < EPS);
        assert!((out[0].vmt - 500.0).abs() < EPS);
    }

    #[test]
    fn sho_is_zero_without_average_speed() {
        let hourly = [varh(2, 21, 0, 5, 8, 500.0)];
        // No AverageSpeed row -> the LEFT JOIN yields SHO = 0.
        let out = sho_by_age_roadway_hour(&hourly, &[]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].sho, 0.0);
    }

    #[test]
    fn vmt_by_age_roadway_day_sums_and_applies_hotelling_rate() {
        // Two hours of long-haul VMT on road 2, day 5.
        let hourly = [
            varh(2, COMBINATION_LONG_HAUL_TRUCK, 0, 5, 8, 300.0),
            varh(2, COMBINATION_LONG_HAUL_TRUCK, 0, 5, 9, 200.0),
        ];
        let zrt = [ZoneRoadTypeRow {
            zone_id: 100,
            road_type_id: 2,
            sho_alloc_factor: 0.5,
            shp_alloc_factor: 1.0,
        }];
        let hcy = [HotellingCalendarYearRow {
            year_id: 2020,
            hotelling_rate: 0.1,
        }];
        let out = vmt_by_age_roadway_day(&hourly, &zrt, &hcy, 100, false);
        assert_eq!(out.len(), 1);
        // dailyVMT = 500; hotellingHours = 500 * 0.5 * 0.1 = 25.
        assert!((out[0].vmt - 500.0).abs() < EPS);
        assert!((out[0].hotelling_hours - 25.0).abs() < EPS);
    }

    #[test]
    fn vmt_by_age_roadway_day_deletes_non_restricted_roads() {
        // Road 3 is non-restricted; road 2 has hotelling activity.
        let hourly = [
            varh(2, COMBINATION_LONG_HAUL_TRUCK, 0, 5, 8, 500.0),
            varh(3, COMBINATION_LONG_HAUL_TRUCK, 0, 5, 8, 100.0),
        ];
        let zrt = [ZoneRoadTypeRow {
            zone_id: 100,
            road_type_id: 2,
            sho_alloc_factor: 1.0,
            shp_alloc_factor: 1.0,
        }];
        let hcy = [HotellingCalendarYearRow {
            year_id: 2020,
            hotelling_rate: 1.0,
        }];
        let out = vmt_by_age_roadway_day(&hourly, &zrt, &hcy, 100, false);
        // Road 3 dropped — restricted-access roads carry the hotelling.
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].road_type_id, 2);
    }

    #[test]
    fn idle_hours_distribute_hotelling_by_hour() {
        let day = [VmtByAgeRoadwayDayRow {
            year_id: 2020,
            road_type_id: 2,
            source_type_id: 62,
            age_id: 0,
            month_id: 1,
            day_id: 5,
            vmt: 500.0,
            hotelling_hours: 100.0,
        }];
        let sth2 = [
            SourceTypeHour2Row {
                source_type_id: 62,
                day_id: 5,
                hour_id: 8,
                idle_sho_factor: 0.0,
                hotelling_dist: 0.3,
            },
            SourceTypeHour2Row {
                source_type_id: 62,
                day_id: 5,
                hour_id: 9,
                idle_sho_factor: 0.0,
                hotelling_dist: 0.7,
            },
        ];
        let out = idle_hours_by_age_hour(&day, &sth2);
        assert_eq!(out.len(), 2);
        let hour8 = out.iter().find(|r| r.hour_id == 8).unwrap();
        // 100 * 0.3 = 30.
        assert!((hour8.idle_hours - 30.0).abs() < EPS);
    }

    #[test]
    fn starts_per_vehicle_divides_starts_by_sample_count() {
        // Two sample vehicles of source type 21 observed on day 5.
        let svd = [
            SampleVehicleDayRow {
                veh_id: 1,
                source_type_id: 21,
                day_id: 5,
            },
            SampleVehicleDayRow {
                veh_id: 2,
                source_type_id: 21,
                day_id: 5,
            },
        ];
        let ssv = [StartsPerSampleVehicleRow {
            source_type_id: 21,
            hour_day_id: 85,
            starts: 8.0,
            day_id: 5,
        }];
        let out = starts_per_vehicle(&svd, &ssv, &[]);
        assert_eq!(out.len(), 1);
        // 8 starts / 2 sample vehicles = 4 starts per vehicle.
        assert!((out[0].starts_per_vehicle - 4.0).abs() < EPS);
    }

    #[test]
    fn starts_per_vehicle_skips_source_types_already_present() {
        let svd = [SampleVehicleDayRow {
            veh_id: 1,
            source_type_id: 21,
            day_id: 5,
        }];
        let ssv = [StartsPerSampleVehicleRow {
            source_type_id: 21,
            hour_day_id: 85,
            starts: 8.0,
            day_id: 5,
        }];
        // Source type 21 already has StartsPerVehicle rows — skip it.
        let existing = [StartsPerVehicleRow {
            source_type_id: 21,
            hour_day_id: 85,
            starts_per_vehicle: 99.0,
        }];
        let out = starts_per_vehicle(&svd, &ssv, &existing);
        assert!(out.is_empty());
    }

    #[test]
    fn starts_per_sample_vehicle_counts_real_trips() {
        let svd = [SampleVehicleDayRow {
            veh_id: 1,
            source_type_id: 21,
            day_id: 5,
        }];
        let trips = [
            SampleVehicleTripRow {
                veh_id: 1,
                day_id: 5,
                hour_id: 8,
                has_key_on_time: true,
            },
            SampleVehicleTripRow {
                veh_id: 1,
                day_id: 5,
                hour_id: 8,
                has_key_on_time: true,
            },
            // Marker trip — keyOnTime is null, so it is not counted.
            SampleVehicleTripRow {
                veh_id: 1,
                day_id: 5,
                hour_id: 8,
                has_key_on_time: false,
            },
        ];
        let hd = [HourDayRow {
            hour_day_id: 85,
            hour_id: 8,
            day_id: 5,
        }];
        let dow = [DayOfAnyWeekRow {
            day_id: 5,
            no_of_real_days: 2.0,
        }];
        let out = starts_per_sample_vehicle(&svd, &trips, &hd, &dow);
        assert_eq!(out.len(), 1);
        // 2 real trips * 2 real days = 4.
        assert!((out[0].starts - 4.0).abs() < EPS);
    }

    #[test]
    fn starts_by_age_hour_is_population_times_starts_per_vehicle() {
        let stap = [SourceTypeAgePopulationRow {
            year_id: 2020,
            source_type_id: 21,
            age_id: 0,
            population: 1000.0,
        }];
        let spv = [StartsPerVehicleRow {
            source_type_id: 21,
            hour_day_id: 85,
            starts_per_vehicle: 2.5,
        }];
        let out = starts_by_age_hour(&stap, &spv);
        assert_eq!(out.len(), 1);
        // 1000 * 2.5 = 2500.
        assert!((out[0].starts - 2500.0).abs() < EPS);
    }

    #[test]
    fn shp_is_population_days_minus_summed_sho() {
        // Two road types in the same (year,source,age,month,day,hour) cell.
        let sho = [
            ShoByAgeRoadwayHourRow {
                year_id: 2020,
                road_type_id: 2,
                source_type_id: 21,
                age_id: 0,
                month_id: 1,
                day_id: 5,
                hour_id: 8,
                hour_day_id: 85,
                sho: 6.0,
                vmt: 100.0,
            },
            ShoByAgeRoadwayHourRow {
                year_id: 2020,
                road_type_id: 3,
                source_type_id: 21,
                age_id: 0,
                month_id: 1,
                day_id: 5,
                hour_id: 8,
                hour_day_id: 85,
                sho: 4.0,
                vmt: 50.0,
            },
        ];
        let stap = [SourceTypeAgePopulationRow {
            year_id: 2020,
            source_type_id: 21,
            age_id: 0,
            population: 100.0,
        }];
        let dow = [DayOfAnyWeekRow {
            day_id: 5,
            no_of_real_days: 2.0,
        }];
        let out = shp_by_age_hour(&sho, &stap, &dow);
        assert_eq!(out.len(), 1);
        // (100 * 2) - (6 + 4) = 200 - 10 = 190.
        assert!((out[0].shp - 190.0).abs() < EPS);
    }

    #[test]
    fn shp_excludes_zero_vmt_rows() {
        let sho = [ShoByAgeRoadwayHourRow {
            year_id: 2020,
            road_type_id: 2,
            source_type_id: 21,
            age_id: 0,
            month_id: 1,
            day_id: 5,
            hour_id: 8,
            hour_day_id: 85,
            sho: 6.0,
            vmt: 0.0,
        }];
        let stap = [SourceTypeAgePopulationRow {
            year_id: 2020,
            source_type_id: 21,
            age_id: 0,
            population: 100.0,
        }];
        let dow = [DayOfAnyWeekRow {
            day_id: 5,
            no_of_real_days: 1.0,
        }];
        assert!(shp_by_age_hour(&sho, &stap, &dow).is_empty());
    }
}

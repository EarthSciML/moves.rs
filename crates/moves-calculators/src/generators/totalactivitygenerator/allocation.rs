//! Spatial allocation of activity to links — algorithm steps 190-209.
//!
//! Ports the *pure computational kernels* of
//! `TotalActivityGenerator.java`'s `allocateTotalActivityBasis` and
//! `calculateDistance`: the formulas that spread the year/zone activity
//! tables ([`super::model::TotalActivityOutput`]) onto individual road
//! links.
//!
//! # Scope
//!
//! The Java methods are driven by the master loop's per-`(process, zone,
//! link)` iteration: which kernel runs, and in what order, is decided by
//! `MasterLoopContext`, the `checkAndMark` dedup set, and `clearActivityTables`.
//! That sequencing — and the three external `database/Adjust*.sql` scripts
//! (`AdjustStarts`, `AdjustHotelling`, `AdjustTotalIdleFraction`) the Java
//! shells out to — is master-loop orchestration, not generator arithmetic.
//! It lands with the Task 50 `execute` wiring, exactly as Task 29's
//! `SourceBinDistributionGenerator` left its per-callback dedup state to that
//! wiring. This module ports the kernels as standalone pure functions; the
//! orchestration calls them.
//!
//! Every kernel here is the **inventory-domain** form. The
//! `ModelScale.MESOSCALE_LOOKUP` branches (off-network idle divided by the
//! 16 average-speed-bin rows; distance from `LinkAverageSpeed` rather than
//! `AverageSpeed`) belong to Task 35's `MesoscaleLookupTotalActivityGenerator`
//! and are deliberately not duplicated here.

use std::collections::BTreeMap;

use super::inputs::{
    CountyRow, DrivingIdleFractionRow, HourDayRow, LinkRow, RunSpecHourDayRow,
    SampleVehiclePopulationRow, StateRow, TotalIdleFractionRow, ZoneRoadTypeRow, ZoneRow,
};
use super::model::{
    AverageSpeedRow, IdleHoursByAgeHourRow, ShoByAgeRoadwayHourRow, ShpByAgeHourRow,
};

/// MOVES `roadTypeID` for the off-network road type — parking lots, driveways,
/// and the like. Off-network "source hours operating" is the off-network
/// idle (ONI) the Java derives in [`off_network_idle_sho`].
pub const OFF_NETWORK_ROAD_TYPE: i32 = 1;

/// One `ZoneRoadTypeLinkTemp` row — a link with its zone's `SHOAllocFactor`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ZoneRoadTypeLinkRow {
    /// `roadTypeID`.
    pub road_type_id: i32,
    /// `linkID`.
    pub link_id: i32,
    /// `SHOAllocFactor` — copied from `ZoneRoadType`.
    pub sho_alloc_factor: f64,
}

/// One `SHO` row — source hours operating allocated to a link.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShoRow {
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `yearID`.
    pub year_id: i32,
    /// `ageID`.
    pub age_id: i32,
    /// `linkID`.
    pub link_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `SHO` — source hours operating.
    pub sho: f64,
    /// `distance` — distance travelled, populated by [`calculate_distance`]
    /// (`0.0` until then, matching the Java's pre-step-200 state).
    pub distance: f64,
}

/// One `hotellingHours` row — hotelling activity allocated to a zone and
/// fuel type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HotellingHoursRow {
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `yearID`.
    pub year_id: i32,
    /// `ageID`.
    pub age_id: i32,
    /// `zoneID`.
    pub zone_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `hotellingHours` — total hotelling hours, including extended idle.
    pub hotelling_hours: f64,
}

/// One `SHP` row — source hours parked allocated to a zone.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShpRow {
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `yearID`.
    pub year_id: i32,
    /// `ageID`.
    pub age_id: i32,
    /// `zoneID`.
    pub zone_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `SHP` — source hours parked.
    pub shp: f64,
}

/// Step 190a — append each link's `SHOAllocFactor`.
///
/// Ports the `ZoneRoadTypeLinkTemp` insert: `ZoneRoadType` joined to `Link`
/// on `roadTypeID`, both filtered to `zone_id`.
#[must_use]
pub fn zone_road_type_link(
    zone_road_type: &[ZoneRoadTypeRow],
    link: &[LinkRow],
    zone_id: i32,
) -> Vec<ZoneRoadTypeLinkRow> {
    // roadTypeID -> SHOAllocFactor for the zone.
    let factor_of: BTreeMap<i32, f64> = zone_road_type
        .iter()
        .filter(|r| r.zone_id == zone_id)
        .map(|r| (r.road_type_id, r.sho_alloc_factor))
        .collect();

    let mut out = Vec::new();
    for l in link.iter().filter(|l| l.zone_id == zone_id) {
        if let Some(&sho_alloc_factor) = factor_of.get(&l.road_type_id) {
            out.push(ZoneRoadTypeLinkRow {
                road_type_id: l.road_type_id,
                link_id: l.link_id,
                sho_alloc_factor,
            });
        }
    }
    out.sort_by_key(|r| (r.road_type_id, r.link_id));
    out
}

/// Step 190b — allocate `SHOByAgeRoadwayHour` to links.
///
/// Ports the `SHO` insert: `SHO = SHOByAgeRoadwayHour.SHO *
/// ZoneRoadTypeLinkTemp.SHOAllocFactor`, joined on `roadTypeID`, for rows in
/// the analysis year whose `hourDayID` is selected by `RunSpecHourDay`.
#[must_use]
pub fn allocate_sho(
    sho_by_age_roadway_hour: &[ShoByAgeRoadwayHourRow],
    zone_road_type_link: &[ZoneRoadTypeLinkRow],
    run_spec_hour_day: &[RunSpecHourDayRow],
    analysis_year: i32,
) -> Vec<ShoRow> {
    let selected_hour_days: BTreeMap<i32, ()> = run_spec_hour_day
        .iter()
        .map(|r| (r.hour_day_id, ()))
        .collect();
    // roadTypeID -> [(linkID, SHOAllocFactor)].
    let mut links_by_road: BTreeMap<i32, Vec<(i32, f64)>> = BTreeMap::new();
    for zrt in zone_road_type_link {
        links_by_road
            .entry(zrt.road_type_id)
            .or_default()
            .push((zrt.link_id, zrt.sho_alloc_factor));
    }

    let mut out = Vec::new();
    for sarh in sho_by_age_roadway_hour {
        if sarh.year_id != analysis_year || !selected_hour_days.contains_key(&sarh.hour_day_id) {
            continue;
        }
        let Some(links) = links_by_road.get(&sarh.road_type_id) else {
            continue;
        };
        for &(link_id, sho_alloc_factor) in links {
            out.push(ShoRow {
                hour_day_id: sarh.hour_day_id,
                month_id: sarh.month_id,
                year_id: sarh.year_id,
                age_id: sarh.age_id,
                link_id,
                source_type_id: sarh.source_type_id,
                sho: sarh.sho * sho_alloc_factor,
                distance: 0.0,
            });
        }
    }
    out
}

/// The lookup tables [`off_network_idle_sho`] joins, bundled to keep the
/// argument count readable.
#[derive(Debug, Clone, Copy)]
pub struct OffNetworkIdleTables<'a> {
    /// `Link` — links keyed by `linkID`, carrying their road type and zone.
    pub link: &'a [LinkRow],
    /// `County` — the county type and parent state of each county.
    pub county: &'a [CountyRow],
    /// `State` — the idle region of each state.
    pub state: &'a [StateRow],
    /// `HourDay` — the `(hour, day)` catalogue.
    pub hour_day: &'a [HourDayRow],
    /// `TotalIdleFraction` — total idle shares.
    pub total_idle_fraction: &'a [TotalIdleFractionRow],
    /// `DrivingIdleFraction` — on-network idle shares.
    pub driving_idle_fraction: &'a [DrivingIdleFractionRow],
}

/// Step 190c — off-network idle (ONI), the `SHO` of the off-network link.
///
/// Ports the inventory-domain `SHO` insert that derives off-network idle.
/// For each non-off-network link's allocated `SHO`, the matching
/// off-network link `lo` in the same zone accumulates, per
/// `(hourDay, month, year, age, lo.link, sourceType)`:
///
/// ```text
/// SHO = totalIdleFraction != 1
///     ? max( sum(sho) * (totalIdleFraction - sum(sho*drivingIdleFraction)/sum(sho))
///            / (1 - totalIdleFraction), 0 )
///     : 0
/// ```
///
/// `totalIdleFraction` is the `TotalIdleFraction` row whose model-year window
/// contains `year - age`; `drivingIdleFraction` varies with the *source*
/// link's road type, so it sits inside the weighted sum. A group whose
/// `sum(sho)` is zero contributes nothing — the Java's `sum/sum` would be a
/// `NULL` the surrounding `case` propagates.
#[must_use]
pub fn off_network_idle_sho(
    allocated_sho: &[ShoRow],
    tables: &OffNetworkIdleTables,
    zone_id: i32,
    analysis_year: i32,
) -> Vec<ShoRow> {
    let link_of: BTreeMap<i32, LinkRow> = tables.link.iter().map(|l| (l.link_id, *l)).collect();
    let county_of: BTreeMap<i32, CountyRow> =
        tables.county.iter().map(|c| (c.county_id, *c)).collect();
    let idle_region_of: BTreeMap<i32, i32> = tables
        .state
        .iter()
        .map(|s| (s.state_id, s.idle_region_id))
        .collect();
    let day_of_hour_day: BTreeMap<i32, i32> = tables
        .hour_day
        .iter()
        .map(|hd| (hd.hour_day_id, hd.day_id))
        .collect();
    // (hourDayID, yearID, roadTypeID, sourceTypeID) -> drivingIdleFraction.
    let driving_idle: BTreeMap<(i32, i32, i32, i32), f64> = tables
        .driving_idle_fraction
        .iter()
        .map(|d| {
            (
                (d.hour_day_id, d.year_id, d.road_type_id, d.source_type_id),
                d.driving_idle_fraction,
            )
        })
        .collect();
    // The off-network links of the zone.
    let off_network_links: Vec<i32> = tables
        .link
        .iter()
        .filter(|l| l.zone_id == zone_id && l.road_type_id == OFF_NETWORK_ROAD_TYPE)
        .map(|l| l.link_id)
        .collect();

    // Accumulators keyed by the GROUP BY tuple.
    let mut sum_sho: BTreeMap<(i32, i32, i32, i32, i32, i32), f64> = BTreeMap::new();
    let mut sum_sho_dif: BTreeMap<(i32, i32, i32, i32, i32, i32), f64> = BTreeMap::new();
    let mut group_tif: BTreeMap<(i32, i32, i32, i32, i32, i32), f64> = BTreeMap::new();

    for s in allocated_sho {
        if s.year_id != analysis_year {
            continue;
        }
        // The source link must exist and be on the network.
        let Some(source_link) = link_of.get(&s.link_id) else {
            continue;
        };
        if source_link.road_type_id == OFF_NETWORK_ROAD_TYPE {
            continue;
        }
        let Some(&day_id) = day_of_hour_day.get(&s.hour_day_id) else {
            continue;
        };
        let Some(&driving_idle_fraction) = driving_idle.get(&(
            s.hour_day_id,
            s.year_id,
            source_link.road_type_id,
            s.source_type_id,
        )) else {
            continue;
        };
        let model_year = s.year_id - s.age_id;

        for &off_link_id in &off_network_links {
            // county / state / idle region of the off-network link.
            let Some(off_link) = link_of.get(&off_link_id) else {
                continue;
            };
            let Some(county) = county_of.get(&off_link.county_id) else {
                continue;
            };
            let Some(&idle_region_id) = idle_region_of.get(&county.state_id) else {
                continue;
            };
            let Some(tif) = tables.total_idle_fraction.iter().find(|t| {
                t.idle_region_id == idle_region_id
                    && t.county_type_id == county.county_type_id
                    && t.source_type_id == s.source_type_id
                    && t.month_id == s.month_id
                    && t.day_id == day_id
                    && t.min_model_year_id <= model_year
                    && t.max_model_year_id >= model_year
            }) else {
                continue;
            };
            let key = (
                s.hour_day_id,
                s.month_id,
                s.year_id,
                s.age_id,
                off_link_id,
                s.source_type_id,
            );
            *sum_sho.entry(key).or_insert(0.0) += s.sho;
            *sum_sho_dif.entry(key).or_insert(0.0) += s.sho * driving_idle_fraction;
            group_tif.insert(key, tif.total_idle_fraction);
        }
    }

    sum_sho
        .into_iter()
        .map(|(key, total_sho)| {
            let (hour_day_id, month_id, year_id, age_id, link_id, source_type_id) = key;
            let tif = group_tif[&key];
            let total_sho_dif = sum_sho_dif[&key];
            let sho = if tif != 1.0 && total_sho != 0.0 {
                let oni = total_sho * (tif - total_sho_dif / total_sho) / (1.0 - tif);
                oni.max(0.0)
            } else {
                0.0
            };
            ShoRow {
                hour_day_id,
                month_id,
                year_id,
                age_id,
                link_id,
                source_type_id,
                sho,
                distance: 0.0,
            }
        })
        .collect()
}

/// Step 190d — hotelling hours allocated to zone and fuel type.
///
/// Ports the `hotellingHours` insert: `hotellingHours =
/// sum(IdleHoursByAgeHour.idleHours * SampleVehiclePopulation.stmyFraction)`.
/// Both tables are filtered to source type
/// [`COMBINATION_LONG_HAUL_TRUCK`](super::activity::COMBINATION_LONG_HAUL_TRUCK)
/// — the only vehicle that hotels — and joined on `modelYearID = yearID -
/// ageID`, grouped by `(hourDay, month, year, age, zone, sourceType,
/// fuelType)`.
#[must_use]
pub fn hotelling_hours(
    idle_hours_by_age_hour: &[IdleHoursByAgeHourRow],
    sample_vehicle_population: &[SampleVehiclePopulationRow],
    hour_day: &[HourDayRow],
    zone_id: i32,
    analysis_year: i32,
) -> Vec<HotellingHoursRow> {
    use super::activity::COMBINATION_LONG_HAUL_TRUCK;

    // (hourID, dayID) -> hourDayID.
    let hour_day_id: BTreeMap<(i32, i32), i32> = hour_day
        .iter()
        .map(|hd| ((hd.hour_id, hd.day_id), hd.hour_day_id))
        .collect();
    // modelYearID -> [(fuelTypeID, stmyFraction)] for long-haul trucks.
    let mut fuels_by_model_year: BTreeMap<i32, Vec<(i32, f64)>> = BTreeMap::new();
    for svp in sample_vehicle_population {
        if svp.source_type_id == COMBINATION_LONG_HAUL_TRUCK {
            fuels_by_model_year
                .entry(svp.model_year_id)
                .or_default()
                .push((svp.fuel_type_id, svp.stmy_fraction));
        }
    }

    let mut totals: BTreeMap<(i32, i32, i32, i32, i32), f64> = BTreeMap::new();
    for ihah in idle_hours_by_age_hour {
        if ihah.source_type_id != COMBINATION_LONG_HAUL_TRUCK || ihah.year_id != analysis_year {
            continue;
        }
        let Some(&hd_id) = hour_day_id.get(&(ihah.hour_id, ihah.day_id)) else {
            continue;
        };
        let model_year = ihah.year_id - ihah.age_id;
        let Some(fuels) = fuels_by_model_year.get(&model_year) else {
            continue;
        };
        for &(fuel_type_id, stmy_fraction) in fuels {
            *totals
                .entry((
                    hd_id,
                    ihah.month_id,
                    ihah.age_id,
                    fuel_type_id,
                    ihah.year_id,
                ))
                .or_insert(0.0) += ihah.idle_hours * stmy_fraction;
        }
    }
    totals
        .into_iter()
        .map(
            |((hour_day_id, month_id, age_id, fuel_type_id, year_id), hotelling_hours)| {
                HotellingHoursRow {
                    hour_day_id,
                    month_id,
                    year_id,
                    age_id,
                    zone_id,
                    source_type_id: COMBINATION_LONG_HAUL_TRUCK,
                    fuel_type_id,
                    hotelling_hours,
                }
            },
        )
        .collect()
}

/// Step 190e — allocate `SHPByAgeHour` to a zone.
///
/// Ports the `SHP` insert: `SHP = SHPByAgeHour.SHP * Zone.SHPAllocFactor`,
/// for analysis-year rows whose `hourDayID` (resolved through `HourDay`) is
/// selected by `RunSpecHourDay`.
#[must_use]
pub fn allocate_shp(
    shp_by_age_hour: &[ShpByAgeHourRow],
    hour_day: &[HourDayRow],
    run_spec_hour_day: &[RunSpecHourDayRow],
    zone: &[ZoneRow],
    zone_id: i32,
    analysis_year: i32,
) -> Vec<ShpRow> {
    let Some(shp_alloc_factor) = zone
        .iter()
        .find(|z| z.zone_id == zone_id)
        .map(|z| z.shp_alloc_factor)
    else {
        return Vec::new();
    };
    // (hourID, dayID) -> hourDayID, gated to RunSpec-selected hourDays.
    let selected: BTreeMap<i32, ()> = run_spec_hour_day
        .iter()
        .map(|r| (r.hour_day_id, ()))
        .collect();
    let hour_day_id: BTreeMap<(i32, i32), i32> = hour_day
        .iter()
        .filter(|hd| selected.contains_key(&hd.hour_day_id))
        .map(|hd| ((hd.hour_id, hd.day_id), hd.hour_day_id))
        .collect();

    let mut out = Vec::new();
    for sah in shp_by_age_hour {
        if sah.year_id != analysis_year {
            continue;
        }
        let Some(&hd_id) = hour_day_id.get(&(sah.hour_id, sah.day_id)) else {
            continue;
        };
        out.push(ShpRow {
            hour_day_id: hd_id,
            month_id: sah.month_id,
            year_id: sah.year_id,
            age_id: sah.age_id,
            zone_id,
            source_type_id: sah.source_type_id,
            shp: sah.shp * shp_alloc_factor,
        });
    }
    out
}

/// Step 200 — distance travelled, `distance = SHO * averageSpeed`.
///
/// Ports the inventory-domain `calculateDistance`: each allocated `SHO` row
/// is joined to `AverageSpeed` through its link's road type and its
/// `hourDayID`'s `(day, hour)`. A row with a matching average speed gets
/// `distance = SHO * averageSpeed`; one without (an off-network ONI row,
/// whose link has no `AverageSpeed`) keeps `distance = 0`, mirroring the
/// Java's `insert ignore … 0 as distance` recovery of the unmatched rows.
#[must_use]
pub fn calculate_distance(
    allocated_sho: &[ShoRow],
    link: &[LinkRow],
    average_speed: &[AverageSpeedRow],
    hour_day: &[HourDayRow],
) -> Vec<ShoRow> {
    let road_type_of: BTreeMap<i32, i32> =
        link.iter().map(|l| (l.link_id, l.road_type_id)).collect();
    let day_hour_of: BTreeMap<i32, (i32, i32)> = hour_day
        .iter()
        .map(|hd| (hd.hour_day_id, (hd.day_id, hd.hour_id)))
        .collect();
    // (roadTypeID, sourceTypeID, dayID, hourID) -> averageSpeed.
    let speed_of: BTreeMap<(i32, i32, i32, i32), f64> = average_speed
        .iter()
        .map(|a| {
            (
                (a.road_type_id, a.source_type_id, a.day_id, a.hour_id),
                a.average_speed,
            )
        })
        .collect();

    allocated_sho
        .iter()
        .map(|s| {
            let distance = (|| {
                let &road_type_id = road_type_of.get(&s.link_id)?;
                let &(day_id, hour_id) = day_hour_of.get(&s.hour_day_id)?;
                let &speed = speed_of.get(&(road_type_id, s.source_type_id, day_id, hour_id))?;
                Some(s.sho * speed)
            })()
            .unwrap_or(0.0);
            ShoRow { distance, ..*s }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPS: f64 = 1e-9;

    fn link(link_id: i32, zone: i32, road: i32, county: i32) -> LinkRow {
        LinkRow {
            link_id,
            zone_id: zone,
            road_type_id: road,
            county_id: county,
        }
    }

    fn sho_arh(road: i32, st: i32, age: i32, sho: f64) -> ShoByAgeRoadwayHourRow {
        ShoByAgeRoadwayHourRow {
            year_id: 2020,
            road_type_id: road,
            source_type_id: st,
            age_id: age,
            month_id: 1,
            day_id: 5,
            hour_id: 8,
            hour_day_id: 85,
            sho,
            vmt: 1.0,
        }
    }

    #[test]
    fn zone_road_type_link_joins_links_to_alloc_factor() {
        let zrt = [ZoneRoadTypeRow {
            zone_id: 100,
            road_type_id: 2,
            sho_alloc_factor: 0.5,
            shp_alloc_factor: 1.0,
        }];
        let links = [
            link(1001, 100, 2, 9),
            // A link in another zone — excluded.
            link(2001, 200, 2, 9),
        ];
        let out = zone_road_type_link(&zrt, &links, 100);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].link_id, 1001);
        assert!((out[0].sho_alloc_factor - 0.5).abs() < EPS);
    }

    #[test]
    fn allocate_sho_scales_by_alloc_factor() {
        let sarh = [sho_arh(2, 21, 0, 10.0)];
        let zrtl = [ZoneRoadTypeLinkRow {
            road_type_id: 2,
            link_id: 1001,
            sho_alloc_factor: 0.5,
        }];
        let rshd = [RunSpecHourDayRow { hour_day_id: 85 }];
        let out = allocate_sho(&sarh, &zrtl, &rshd, 2020);
        assert_eq!(out.len(), 1);
        // 10 * 0.5 = 5.
        assert!((out[0].sho - 5.0).abs() < EPS);
        assert_eq!(out[0].link_id, 1001);
    }

    #[test]
    fn allocate_sho_drops_unselected_hour_days() {
        let sarh = [sho_arh(2, 21, 0, 10.0)];
        let zrtl = [ZoneRoadTypeLinkRow {
            road_type_id: 2,
            link_id: 1001,
            sho_alloc_factor: 1.0,
        }];
        // hourDay 85 is not in RunSpecHourDay.
        let out = allocate_sho(&sarh, &zrtl, &[], 2020);
        assert!(out.is_empty());
    }

    #[test]
    fn off_network_idle_applies_the_oni_formula() {
        // One on-network SHO row on road 2, link 1001.
        let allocated = [ShoRow {
            hour_day_id: 85,
            month_id: 1,
            year_id: 2020,
            age_id: 0,
            link_id: 1001,
            source_type_id: 21,
            sho: 100.0,
            distance: 0.0,
        }];
        let links = [
            link(1001, 100, 2, 9),
            // Off-network link in the same zone.
            link(1000, 100, OFF_NETWORK_ROAD_TYPE, 9),
        ];
        let counties = [CountyRow {
            county_id: 9,
            county_type_id: 1,
            state_id: 4,
        }];
        let states = [StateRow {
            state_id: 4,
            idle_region_id: 7,
        }];
        let hd = [HourDayRow {
            hour_day_id: 85,
            hour_id: 8,
            day_id: 5,
        }];
        let tif = [TotalIdleFractionRow {
            idle_region_id: 7,
            county_type_id: 1,
            source_type_id: 21,
            month_id: 1,
            day_id: 5,
            min_model_year_id: 1990,
            max_model_year_id: 2030,
            total_idle_fraction: 0.5,
        }];
        let dif = [DrivingIdleFractionRow {
            hour_day_id: 85,
            year_id: 2020,
            road_type_id: 2,
            source_type_id: 21,
            driving_idle_fraction: 0.1,
        }];
        let tables = OffNetworkIdleTables {
            link: &links,
            county: &counties,
            state: &states,
            hour_day: &hd,
            total_idle_fraction: &tif,
            driving_idle_fraction: &dif,
        };
        let out = off_network_idle_sho(&allocated, &tables, 100, 2020);
        assert_eq!(out.len(), 1);
        // sum(sho)=100, sum(sho*dif)=10, tif=0.5.
        // oni = 100 * (0.5 - 10/100) / (1 - 0.5) = 100 * 0.4 / 0.5 = 80.
        assert!((out[0].sho - 80.0).abs() < EPS);
        assert_eq!(out[0].link_id, 1000);
    }

    #[test]
    fn off_network_idle_is_zero_when_total_idle_fraction_is_one() {
        let allocated = [ShoRow {
            hour_day_id: 85,
            month_id: 1,
            year_id: 2020,
            age_id: 0,
            link_id: 1001,
            source_type_id: 21,
            sho: 100.0,
            distance: 0.0,
        }];
        let links = [
            link(1001, 100, 2, 9),
            link(1000, 100, OFF_NETWORK_ROAD_TYPE, 9),
        ];
        let counties = [CountyRow {
            county_id: 9,
            county_type_id: 1,
            state_id: 4,
        }];
        let states = [StateRow {
            state_id: 4,
            idle_region_id: 7,
        }];
        let hd = [HourDayRow {
            hour_day_id: 85,
            hour_id: 8,
            day_id: 5,
        }];
        let tif = [TotalIdleFractionRow {
            idle_region_id: 7,
            county_type_id: 1,
            source_type_id: 21,
            month_id: 1,
            day_id: 5,
            min_model_year_id: 1990,
            max_model_year_id: 2030,
            total_idle_fraction: 1.0,
        }];
        let dif = [DrivingIdleFractionRow {
            hour_day_id: 85,
            year_id: 2020,
            road_type_id: 2,
            source_type_id: 21,
            driving_idle_fraction: 0.1,
        }];
        let tables = OffNetworkIdleTables {
            link: &links,
            county: &counties,
            state: &states,
            hour_day: &hd,
            total_idle_fraction: &tif,
            driving_idle_fraction: &dif,
        };
        let out = off_network_idle_sho(&allocated, &tables, 100, 2020);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].sho, 0.0);
    }

    #[test]
    fn hotelling_hours_weights_idle_by_fuel_fraction() {
        let ihah = [IdleHoursByAgeHourRow {
            year_id: 2020,
            source_type_id: 62,
            age_id: 0,
            month_id: 1,
            day_id: 5,
            hour_id: 8,
            idle_hours: 200.0,
        }];
        // Model year 2020 split 70% diesel / 30% gasoline.
        let svp = [
            SampleVehiclePopulationRow {
                source_type_id: 62,
                model_year_id: 2020,
                fuel_type_id: 2,
                stmy_fraction: 0.7,
            },
            SampleVehiclePopulationRow {
                source_type_id: 62,
                model_year_id: 2020,
                fuel_type_id: 1,
                stmy_fraction: 0.3,
            },
        ];
        let hd = [HourDayRow {
            hour_day_id: 85,
            hour_id: 8,
            day_id: 5,
        }];
        let out = hotelling_hours(&ihah, &svp, &hd, 100, 2020);
        assert_eq!(out.len(), 2);
        let diesel = out.iter().find(|r| r.fuel_type_id == 2).unwrap();
        // 200 * 0.7 = 140.
        assert!((diesel.hotelling_hours - 140.0).abs() < EPS);
    }

    #[test]
    fn allocate_shp_scales_by_zone_factor() {
        let shp = [ShpByAgeHourRow {
            year_id: 2020,
            source_type_id: 21,
            age_id: 0,
            month_id: 1,
            day_id: 5,
            hour_id: 8,
            shp: 1000.0,
        }];
        let hd = [HourDayRow {
            hour_day_id: 85,
            hour_id: 8,
            day_id: 5,
        }];
        let rshd = [RunSpecHourDayRow { hour_day_id: 85 }];
        let zone = [ZoneRow {
            zone_id: 100,
            shp_alloc_factor: 0.25,
        }];
        let out = allocate_shp(&shp, &hd, &rshd, &zone, 100, 2020);
        assert_eq!(out.len(), 1);
        // 1000 * 0.25 = 250.
        assert!((out[0].shp - 250.0).abs() < EPS);
    }

    #[test]
    fn calculate_distance_is_sho_times_average_speed() {
        let sho = [ShoRow {
            hour_day_id: 85,
            month_id: 1,
            year_id: 2020,
            age_id: 0,
            link_id: 1001,
            source_type_id: 21,
            sho: 10.0,
            distance: 0.0,
        }];
        let links = [link(1001, 100, 2, 9)];
        let speed = [AverageSpeedRow {
            road_type_id: 2,
            source_type_id: 21,
            day_id: 5,
            hour_id: 8,
            average_speed: 55.0,
        }];
        let hd = [HourDayRow {
            hour_day_id: 85,
            hour_id: 8,
            day_id: 5,
        }];
        let out = calculate_distance(&sho, &links, &speed, &hd);
        assert_eq!(out.len(), 1);
        // 10 * 55 = 550.
        assert!((out[0].distance - 550.0).abs() < EPS);
    }

    #[test]
    fn calculate_distance_is_zero_without_average_speed() {
        let sho = [ShoRow {
            hour_day_id: 85,
            month_id: 1,
            year_id: 2020,
            age_id: 0,
            link_id: 1000,
            source_type_id: 21,
            sho: 10.0,
            distance: 0.0,
        }];
        // Off-network link with no AverageSpeed — distance stays 0.
        let links = [link(1000, 100, OFF_NETWORK_ROAD_TYPE, 9)];
        let hd = [HourDayRow {
            hour_day_id: 85,
            hour_id: 8,
            day_id: 5,
        }];
        let out = calculate_distance(&sho, &links, &[], &hd);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].distance, 0.0);
    }
}

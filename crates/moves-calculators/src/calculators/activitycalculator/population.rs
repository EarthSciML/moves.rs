//! The `Population` activity section — `activityTypeID` 6.
//!
//! Ports the `Processing` section's `Population` block, which has two
//! mutually exclusive domain variants:
//!
//! * [`population_non_project`] — the default. Distributes the
//! `sourceTypeAgePopulation` of the analysis year onto the zone's
//! off-network link, weighted by a source-type fraction
//! (`fractionBySourceTypeTemp`) derived from how much each source type
//! travels on road types present in the zone.
//! * [`population_project`] — Project-domain runs. Sums an off-network term
//! (`offNetworkLink` population) and an on-roadway term (`link.linkVolume`
//! apportioned by `linkSourceTypeHour`), each split by an explicit
//! `sourceTypeAgeDistribution`.
//!
//! Both emit `activityTypeID` 6 with `monthID`, `dayID`, and `hourID` fixed
//! at `0` — population is a year-level quantity — and `modelYearID` derived
//! as `context.year - ageID`. Only the `WithRegClassID` script variant is
//! ported; see the [module docs](super).

use std::collections::{HashMap, HashSet};

use super::fuelfraction::{fuel_reg_class_weights, FuelFractionIndex, RegClassIndex};
use super::inputs::ActivityInputs;
use super::model::ActivityRow;
use super::rowbuild::{weighted, RowTemplate};

/// `Population`, Non-Project domain.
///
/// First the `fractionBySourceTypeTemp` step: a source type's `sutFraction`
/// is `sum(roadTypeVMTFraction * SHOAllocFactor) / sum(roadTypeVMTFraction)`
/// over the road types it travels that are present in the iteration zone.
/// Then `sourceTypeTempPopulation`: `sourceTypeAgePopulation.population *
/// sutFraction`, replicated onto every off-network (`roadTypeID = 1`) link.
/// Finally each population is split across the source bin.
///
/// # Fidelity
///
/// A source type whose `sum(roadTypeVMTFraction)` over the joined rows is not
/// positive is skipped: the SQL's `num / 0` would yield a `NULL` `sutFraction`
/// and propagate a `NULL`-`activity` row. This is a degenerate input — a
/// source type with no positive road-type VMT fraction in the zone — and the
/// port drops it rather than emit a `NULL`/`NaN` activity.
#[must_use]
pub fn population_non_project(
    inputs: &ActivityInputs,
    fuel: &FuelFractionIndex,
    reg: &RegClassIndex,
) -> Vec<ActivityRow> {
    let ctx = &inputs.context;

 // zoneRoadType, filtered to the iteration zone, indexed by road type.
 // The Extract step already summed SHOAllocFactor over source type and
 // grouped by road type, so there is one factor per road type.
    let sho_alloc: HashMap<i32, f64> = inputs
        .zone_road_type
        .iter()
        .filter(|z| z.zone_id == ctx.zone_id)
        .map(|z| (z.road_type_id, z.sho_alloc_factor))
        .collect();

 // roadTypeDistribution indexed by source type.
    let mut road_dist: HashMap<i32, Vec<(i32, f64)>> = HashMap::new();
    for r in &inputs.road_type_distribution {
        road_dist
            .entry(r.source_type_id)
            .or_default()
            .push((r.road_type_id, r.road_type_vmt_fraction));
    }

 // sourceTypeAgePopulation indexed by source type.
    let mut age_population: HashMap<i32, Vec<(i32, f64)>> = HashMap::new();
    for r in &inputs.source_type_age_population {
        age_population
            .entry(r.source_type_id)
            .or_default()
            .push((r.age_id, r.population));
    }

    let run_spec: HashSet<i32> = inputs
        .run_spec_source_type
        .iter()
        .map(|r| r.source_type_id)
        .collect();

 // Off-network links — the cross join `link l ON l.roadTypeID = 1`.
    let off_network_links: Vec<i32> = inputs
        .link
        .iter()
        .filter(|l| l.road_type_id == 1)
        .map(|l| l.link_id)
        .collect();

    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for sut in &inputs.source_use_type {
        let source_type_id = sut.source_type_id;
 // `GROUP BY sut.sourceTypeID` — process each source type once.
        if !seen.insert(source_type_id) {
            continue;
        }

 // fractionBySourceTypeTemp: sum over (roadTypeDistribution ⋈
 // zoneRoadType) for this source type.
        let mut numerator = 0.0;
        let mut denominator = 0.0;
        for &(road_type_id, vmt_fraction) in road_dist
            .get(&source_type_id)
            .map_or(&[][..], Vec::as_slice)
        {
            if let Some(&factor) = sho_alloc.get(&road_type_id) {
                numerator += vmt_fraction * factor;
                denominator += vmt_fraction;
            }
        }
        if denominator <= 0.0 {
            continue;
        }
        let sut_fraction = numerator / denominator;

 // INNER JOIN runSpecSourceType rsst.
        if !run_spec.contains(&source_type_id) {
            continue;
        }
 // INNER JOIN sourceTypeAgePopulation stap.
        let Some(ages) = age_population.get(&source_type_id) else {
            continue;
        };
        for &(age_id, population) in ages {
            let model_year_id = ctx.year - age_id;
            let weights = fuel_reg_class_weights(fuel, reg, source_type_id, model_year_id);
            if weights.is_empty() {
                continue;
            }
            let population = population * sut_fraction;
 // CROSS JOIN link l ON l.roadTypeID = 1.
            for &link_id in &off_network_links {
                let template = RowTemplate {
                    year_id: ctx.year,
                    month_id: 0,
                    day_id: 0,
                    hour_id: 0,
                    state_id: ctx.state_id,
                    county_id: ctx.county_id,
                    zone_id: ctx.zone_id,
                    link_id,
                    source_type_id,
                    model_year_id,
                    road_type_id: 1,
                    activity_type_id: 6,
                };
                weighted(&template, population, &weights, &mut out);
            }
        }
    }
    out
}

/// `Population`, Project domain.
///
/// Returns the off-network insert followed by the on-roadway insert, the
/// order the SQL runs them.
///
/// * Off-network: for each `roadTypeID = 1` link in the zone, `offNetworkLink`
/// vehicle population × `sourceTypeAgeDistribution` age fraction × source
/// bin.
/// * On-roadway: for each non-off-network link, `link.linkVolume` ×
/// `linkSourceTypeHour` source-type share × age fraction × source bin.
#[must_use]
pub fn population_project(
    inputs: &ActivityInputs,
    fuel: &FuelFractionIndex,
    reg: &RegClassIndex,
) -> Vec<ActivityRow> {
    let ctx = &inputs.context;

 // sourceTypeAgeDistribution indexed by source type.
    let mut age_dist: HashMap<i32, Vec<(i32, f64)>> = HashMap::new();
    for r in &inputs.source_type_age_distribution {
        age_dist
            .entry(r.source_type_id)
            .or_default()
            .push((r.age_id, r.age_fraction));
    }

    let mut out = Vec::new();

 // --- Off-network insert: link ⋈ offNetworkLink USING (zoneID). ---
    let mut off_network: HashMap<i32, Vec<(i32, f64)>> = HashMap::new();
    for r in &inputs.off_network_link {
        off_network
            .entry(r.zone_id)
            .or_default()
            .push((r.source_type_id, r.vehicle_population));
    }
    for l in &inputs.link {
        if l.zone_id != ctx.zone_id || l.road_type_id != 1 {
            continue;
        }
        for &(source_type_id, vehicle_population) in
            off_network.get(&l.zone_id).map_or(&[][..], Vec::as_slice)
        {
            for &(age_id, age_fraction) in
                age_dist.get(&source_type_id).map_or(&[][..], Vec::as_slice)
            {
                let model_year_id = ctx.year - age_id;
                let weights = fuel_reg_class_weights(fuel, reg, source_type_id, model_year_id);
                let template = RowTemplate {
                    year_id: ctx.year,
                    month_id: 0,
                    day_id: 0,
                    hour_id: 0,
                    state_id: ctx.state_id,
                    county_id: ctx.county_id,
                    zone_id: ctx.zone_id,
                    link_id: l.link_id,
                    source_type_id,
                    model_year_id,
                    road_type_id: 1,
                    activity_type_id: 6,
                };
                weighted(
                    &template,
                    vehicle_population * age_fraction,
                    &weights,
                    &mut out,
                );
            }
        }
    }

 // --- On-roadway insert: link ⋈ linkSourceTypeHour ON linkID. ---
    let mut link_source_type_hour: HashMap<i32, Vec<(i32, f64)>> = HashMap::new();
    for r in &inputs.link_source_type_hour {
        link_source_type_hour
            .entry(r.link_id)
            .or_default()
            .push((r.source_type_id, r.source_type_hour_fraction));
    }
    for l in &inputs.link {
        if l.road_type_id == 1 {
            continue;
        }
        for &(source_type_id, hour_fraction) in link_source_type_hour
            .get(&l.link_id)
            .map_or(&[][..], Vec::as_slice)
        {
            for &(age_id, age_fraction) in
                age_dist.get(&source_type_id).map_or(&[][..], Vec::as_slice)
            {
                let model_year_id = ctx.year - age_id;
                let weights = fuel_reg_class_weights(fuel, reg, source_type_id, model_year_id);
                let template = RowTemplate {
                    year_id: ctx.year,
                    month_id: 0,
                    day_id: 0,
                    hour_id: 0,
                    state_id: ctx.state_id,
                    county_id: ctx.county_id,
                    zone_id: ctx.zone_id,
                    link_id: l.link_id,
                    source_type_id,
                    model_year_id,
                    road_type_id: l.road_type_id,
                    activity_type_id: 6,
                };
                weighted(
                    &template,
                    l.link_volume * hour_fraction * age_fraction,
                    &weights,
                    &mut out,
                );
            }
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::super::inputs::{
        IterationContext, LinkRow, LinkSourceTypeHourRow, OffNetworkLinkRow,
        RegClassSourceTypeFractionRow, RoadTypeDistributionRow, RunSpecSourceTypeRow,
        SourceTypeAgeDistributionRow, SourceTypeAgePopulationRow, SourceUseTypeRow,
        ZoneRoadTypeRow,
    };
    use super::super::model::SourceTypeFuelFractionRow;
    use super::*;

    fn ctx() -> IterationContext {
        IterationContext {
            year: 2020,
            state_id: 26,
            county_id: 26161,
            zone_id: 261610,
            link_id: 2616100,
            road_type_id: 1,
            fuel_year_id: 2020,
        }
    }

 /// A whole-bin source type 21, model year 2018 (age 2 of analysis year
 /// 2020): one fuel type, one regulatory class, both fraction 1.
    fn whole_bin() -> (FuelFractionIndex, RegClassIndex) {
        let fuel = FuelFractionIndex::new(&[SourceTypeFuelFractionRow {
            source_type_id: 21,
            model_year_id: 2018,
            fuel_type_id: 1,
            fuel_fraction: 1.0,
        }]);
        let reg = RegClassIndex::new(&[RegClassSourceTypeFractionRow {
            source_type_id: 21,
            fuel_type_id: 1,
            model_year_id: 2018,
            reg_class_id: 30,
            reg_class_fraction: 1.0,
        }]);
        (fuel, reg)
    }

    #[test]
    fn non_project_weights_population_by_source_type_fraction() {
        let (fuel, reg) = whole_bin();
        let inputs = ActivityInputs {
            context: ctx(),
            source_use_type: vec![SourceUseTypeRow { source_type_id: 21 }],
 // Source type 21 travels 75% on road 2, 25% on road 4.
            road_type_distribution: vec![
                RoadTypeDistributionRow {
                    source_type_id: 21,
                    road_type_id: 2,
                    road_type_vmt_fraction: 0.75,
                },
                RoadTypeDistributionRow {
                    source_type_id: 21,
                    road_type_id: 4,
                    road_type_vmt_fraction: 0.25,
                },
            ],
 // SHOAllocFactor 0.8 on road 2, 0.4 on road 4.
            zone_road_type: vec![
                ZoneRoadTypeRow {
                    zone_id: 261610,
                    road_type_id: 2,
                    sho_alloc_factor: 0.8,
                },
                ZoneRoadTypeRow {
                    zone_id: 261610,
                    road_type_id: 4,
                    sho_alloc_factor: 0.4,
                },
            ],
            source_type_age_population: vec![SourceTypeAgePopulationRow {
                source_type_id: 21,
                age_id: 2,
                population: 1000.0,
            }],
            run_spec_source_type: vec![RunSpecSourceTypeRow { source_type_id: 21 }],
            link: vec![LinkRow {
                link_id: 9,
                zone_id: 261610,
                road_type_id: 1,
                link_volume: 0.0,
            }],
            ..ActivityInputs::default()
        };
        let rows = population_non_project(&inputs, &fuel, &reg);
        assert_eq!(rows.len(), 1);
 // sutFraction = (0.75*0.8 + 0.25*0.4) / (0.75 + 0.25) = 0.7.
 // activity = 1000 * 0.7 * 1.0 * 1.0.
        assert!((rows[0].activity - 700.0).abs() < 1e-9);
        assert_eq!(rows[0].activity_type_id, 6);
        assert_eq!(rows[0].road_type_id, 1);
        assert_eq!(rows[0].link_id, 9);
        assert_eq!(rows[0].month_id, 0);
        assert_eq!(rows[0].model_year_id, 2018); // context.year - ageID
    }

    #[test]
    fn non_project_run_spec_gate_drops_unselected_source_types() {
        let (fuel, reg) = whole_bin();
        let mut inputs = ActivityInputs {
            context: ctx(),
            source_use_type: vec![SourceUseTypeRow { source_type_id: 21 }],
            road_type_distribution: vec![RoadTypeDistributionRow {
                source_type_id: 21,
                road_type_id: 2,
                road_type_vmt_fraction: 1.0,
            }],
            zone_road_type: vec![ZoneRoadTypeRow {
                zone_id: 261610,
                road_type_id: 2,
                sho_alloc_factor: 1.0,
            }],
            source_type_age_population: vec![SourceTypeAgePopulationRow {
                source_type_id: 21,
                age_id: 2,
                population: 1000.0,
            }],
            run_spec_source_type: vec![RunSpecSourceTypeRow { source_type_id: 21 }],
            link: vec![LinkRow {
                link_id: 9,
                zone_id: 261610,
                road_type_id: 1,
                link_volume: 0.0,
            }],
            ..ActivityInputs::default()
        };
        assert_eq!(population_non_project(&inputs, &fuel, &reg).len(), 1);
 // Drop source type 21 from the RunSpec selection.
        inputs.run_spec_source_type.clear();
        assert!(population_non_project(&inputs, &fuel, &reg).is_empty());
    }

    #[test]
    fn non_project_replicates_onto_every_off_network_link() {
        let (fuel, reg) = whole_bin();
        let inputs = ActivityInputs {
            context: ctx(),
            source_use_type: vec![SourceUseTypeRow { source_type_id: 21 }],
            road_type_distribution: vec![RoadTypeDistributionRow {
                source_type_id: 21,
                road_type_id: 2,
                road_type_vmt_fraction: 1.0,
            }],
            zone_road_type: vec![ZoneRoadTypeRow {
                zone_id: 261610,
                road_type_id: 2,
                sho_alloc_factor: 1.0,
            }],
            source_type_age_population: vec![SourceTypeAgePopulationRow {
                source_type_id: 21,
                age_id: 2,
                population: 1000.0,
            }],
            run_spec_source_type: vec![RunSpecSourceTypeRow { source_type_id: 21 }],
 // Two off-network links plus one on-roadway link that must not match.
            link: vec![
                LinkRow {
                    link_id: 9,
                    zone_id: 261610,
                    road_type_id: 1,
                    link_volume: 0.0,
                },
                LinkRow {
                    link_id: 10,
                    zone_id: 261610,
                    road_type_id: 1,
                    link_volume: 0.0,
                },
                LinkRow {
                    link_id: 11,
                    zone_id: 261610,
                    road_type_id: 4,
                    link_volume: 0.0,
                },
            ],
            ..ActivityInputs::default()
        };
        let rows = population_non_project(&inputs, &fuel, &reg);
        assert_eq!(rows.len(), 2);
        let mut links: Vec<i32> = rows.iter().map(|r| r.link_id).collect();
        links.sort_unstable();
        assert_eq!(links, vec![9, 10]);
    }

    #[test]
    fn project_sums_off_network_and_on_roadway() {
        let (fuel, reg) = whole_bin();
        let inputs = ActivityInputs {
            context: ctx(),
            link: vec![
                LinkRow {
                    link_id: 1,
                    zone_id: 261610,
                    road_type_id: 1,
                    link_volume: 0.0,
                },
                LinkRow {
                    link_id: 2,
                    zone_id: 261610,
                    road_type_id: 4,
                    link_volume: 500.0,
                },
            ],
            off_network_link: vec![OffNetworkLinkRow {
                zone_id: 261610,
                source_type_id: 21,
                vehicle_population: 200.0,
            }],
            link_source_type_hour: vec![LinkSourceTypeHourRow {
                link_id: 2,
                source_type_id: 21,
                source_type_hour_fraction: 0.5,
            }],
            source_type_age_distribution: vec![SourceTypeAgeDistributionRow {
                source_type_id: 21,
                age_id: 2,
                age_fraction: 0.3,
            }],
            ..ActivityInputs::default()
        };
        let rows = population_project(&inputs, &fuel, &reg);
        assert_eq!(rows.len(), 2);
 // Off-network insert runs first: 200 vehicles * 0.3 age fraction.
        assert_eq!(rows[0].link_id, 1);
        assert_eq!(rows[0].road_type_id, 1);
        assert!((rows[0].activity - 60.0).abs() < 1e-9);
 // On-roadway insert: 500 linkVolume * 0.5 hour fraction * 0.3 age.
        assert_eq!(rows[1].link_id, 2);
        assert_eq!(rows[1].road_type_id, 4);
        assert!((rows[1].activity - 75.0).abs() < 1e-9);
        assert!(rows.iter().all(|r| r.activity_type_id == 6));
    }

    #[test]
    fn project_off_network_ignores_on_roadway_links() {
        let (fuel, reg) = whole_bin();
 // Only an on-roadway link is present; the off-network insert is empty.
        let inputs = ActivityInputs {
            context: ctx(),
            link: vec![LinkRow {
                link_id: 2,
                zone_id: 261610,
                road_type_id: 4,
                link_volume: 500.0,
            }],
            off_network_link: vec![OffNetworkLinkRow {
                zone_id: 261610,
                source_type_id: 21,
                vehicle_population: 200.0,
            }],
            source_type_age_distribution: vec![SourceTypeAgeDistributionRow {
                source_type_id: 21,
                age_id: 2,
                age_fraction: 1.0,
            }],
            ..ActivityInputs::default()
        };
 // No linkSourceTypeHour, so the on-roadway insert is empty too.
        assert!(population_project(&inputs, &fuel, &reg).is_empty());
    }
}

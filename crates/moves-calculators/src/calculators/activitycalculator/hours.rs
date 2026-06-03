//! The source-hours activity sections: `SourceHours`, `SHO`, `ONI`, `SHP`,
//! and `Starts`.
//!
//! Each ports one `Processing` section of `ActivityCalculator.sql`. They
//! share a shape: join the base activity table to `HourDay` for the
//! `(day, hour)` split, expand the source bin through
//! [`fuel_reg_class_weights`], and emit `baseValue * fuelFraction *
//! regClassFraction`.
//!
//! Two placement families differ only in where the output row sits:
//!
//! * **Link-located** — `SourceHours` (`activityTypeID` 2) and `SHO` /`ONI`
//! (`activityTypeID` 4): the row's own `linkID`, the joined `Link`'s
//! `roadTypeID`, and the iteration zone.
//! * **Zone-located** — `SHP` (`activityTypeID` 5) and `Starts`
//! (`activityTypeID` 7): the iteration link and road type, with no `Link`
//! join. `SHP` copies the activity row's `zoneID`; `Starts` uses the
//! iteration zone.
//!
//! Only the `WithRegClassID` script variant is ported — see the
//! [module docs](super) for why `NoRegClassID` is dead in MOVES.

use std::collections::HashMap;

use super::fuelfraction::{fuel_reg_class_weights, FuelFractionIndex, RegClassIndex};
use super::inputs::{ActivityInputs, HourDayRow, LinkRow};
use super::model::ActivityRow;
use super::rowbuild::{weighted, RowTemplate};

/// Index `HourDay` by `hourDayID` (a primary key — one row each).
fn hour_day_index(rows: &[HourDayRow]) -> HashMap<i32, HourDayRow> {
    rows.iter().map(|r| (r.hour_day_id, *r)).collect()
}

/// Index `link` by `linkID` (a primary key — one row each).
fn link_index(rows: &[LinkRow]) -> HashMap<i32, LinkRow> {
    rows.iter().map(|r| (r.link_id, *r)).collect()
}

/// `SourceHours` section — `activityTypeID` 2.
///
/// `activity = sourceHours * fuelFraction * regClassFraction`, placed at the
/// activity row's `linkID` and the joined `Link`'s `roadTypeID`.
#[must_use]
pub fn source_hours(
    inputs: &ActivityInputs,
    fuel: &FuelFractionIndex,
    reg: &RegClassIndex,
) -> Vec<ActivityRow> {
    let hour_day = hour_day_index(&inputs.hour_day);
    let link = link_index(&inputs.link);
    let ctx = &inputs.context;
    let mut out = Vec::new();
    for s in &inputs.source_hours {
        // INNER JOIN HourDay h, INNER JOIN Link l.
        let (Some(h), Some(l)) = (hour_day.get(&s.hour_day_id), link.get(&s.link_id)) else {
            continue;
        };
        let model_year_id = s.year_id - s.age_id;
        let template = RowTemplate {
            year_id: s.year_id,
            month_id: s.month_id,
            day_id: h.day_id,
            hour_id: h.hour_id,
            state_id: ctx.state_id,
            county_id: ctx.county_id,
            zone_id: ctx.zone_id,
            link_id: s.link_id,
            source_type_id: s.source_type_id,
            model_year_id,
            road_type_id: l.road_type_id,
            activity_type_id: 2,
        };
        let weights = fuel_reg_class_weights(fuel, reg, s.source_type_id, model_year_id);
        weighted(&template, s.source_hours, &weights, &mut out);
    }
    out
}

/// `SHO` section — `activityTypeID` 4.
///
/// `activity = SHO * fuelFraction * regClassFraction`, placed at the activity
/// row's `linkID` and the joined `Link`'s `roadTypeID`. The `ONI`
/// (off-network idle) section's SQL is byte-identical — it extracts and
/// processes the same `SHO` table — so the calculator routes `ONI` here too;
/// the master loop keeps the two from both firing on one link by gating
/// `SHO` to on-network and `ONI` to off-network links.
#[must_use]
pub fn sho(
    inputs: &ActivityInputs,
    fuel: &FuelFractionIndex,
    reg: &RegClassIndex,
) -> Vec<ActivityRow> {
    let hour_day = hour_day_index(&inputs.hour_day);
    let link = link_index(&inputs.link);
    let ctx = &inputs.context;
    let mut out = Vec::new();
    for s in &inputs.sho {
        let (Some(h), Some(l)) = (hour_day.get(&s.hour_day_id), link.get(&s.link_id)) else {
            continue;
        };
        let model_year_id = s.year_id - s.age_id;
        let template = RowTemplate {
            year_id: s.year_id,
            month_id: s.month_id,
            day_id: h.day_id,
            hour_id: h.hour_id,
            state_id: ctx.state_id,
            county_id: ctx.county_id,
            zone_id: ctx.zone_id,
            link_id: s.link_id,
            source_type_id: s.source_type_id,
            model_year_id,
            road_type_id: l.road_type_id,
            activity_type_id: 4,
        };
        let weights = fuel_reg_class_weights(fuel, reg, s.source_type_id, model_year_id);
        weighted(&template, s.sho, &weights, &mut out);
    }
    out
}

/// `SHP` section — `activityTypeID` 5.
///
/// `activity = SHP * fuelFraction * regClassFraction`. Zone-located: the
/// output row keeps the activity row's `zoneID` but takes its `linkID` and
/// `roadTypeID` from the iteration context, with no `Link` join.
#[must_use]
pub fn shp(
    inputs: &ActivityInputs,
    fuel: &FuelFractionIndex,
    reg: &RegClassIndex,
) -> Vec<ActivityRow> {
    let hour_day = hour_day_index(&inputs.hour_day);
    let ctx = &inputs.context;
    let mut out = Vec::new();
    for s in &inputs.shp {
        let Some(h) = hour_day.get(&s.hour_day_id) else {
            continue;
        };
        let model_year_id = s.year_id - s.age_id;
        let template = RowTemplate {
            year_id: s.year_id,
            month_id: s.month_id,
            day_id: h.day_id,
            hour_id: h.hour_id,
            state_id: ctx.state_id,
            county_id: ctx.county_id,
            zone_id: s.zone_id,
            link_id: ctx.link_id,
            source_type_id: s.source_type_id,
            model_year_id,
            road_type_id: ctx.road_type_id,
            activity_type_id: 5,
        };
        let weights = fuel_reg_class_weights(fuel, reg, s.source_type_id, model_year_id);
        weighted(&template, s.shp, &weights, &mut out);
    }
    out
}

/// `Starts` section — `activityTypeID` 7.
///
/// `activity = starts * fuelFraction * regClassFraction`. Zone-located, like
/// [`shp`], but the output row's `zoneID` is the iteration zone.
#[must_use]
pub fn starts(
    inputs: &ActivityInputs,
    fuel: &FuelFractionIndex,
    reg: &RegClassIndex,
) -> Vec<ActivityRow> {
    let hour_day = hour_day_index(&inputs.hour_day);
    let ctx = &inputs.context;
    let mut out = Vec::new();
    for s in &inputs.starts {
        let Some(h) = hour_day.get(&s.hour_day_id) else {
            continue;
        };
        let model_year_id = s.year_id - s.age_id;
        let template = RowTemplate {
            year_id: s.year_id,
            month_id: s.month_id,
            day_id: h.day_id,
            hour_id: h.hour_id,
            state_id: ctx.state_id,
            county_id: ctx.county_id,
            zone_id: ctx.zone_id,
            link_id: ctx.link_id,
            source_type_id: s.source_type_id,
            model_year_id,
            road_type_id: ctx.road_type_id,
            activity_type_id: 7,
        };
        let weights = fuel_reg_class_weights(fuel, reg, s.source_type_id, model_year_id);
        weighted(&template, s.starts, &weights, &mut out);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::super::inputs::{
        HourDayRow, IterationContext, LinkRow, RegClassSourceTypeFractionRow, ShoRow, ShpRow,
        SourceHoursRow, StartsRow,
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
            road_type_id: 5,
            fuel_year_id: 2020,
        }
    }

    /// One fuel type (share 1.0) split across two regulatory classes 60/40.
    fn fuel_and_reg() -> (FuelFractionIndex, RegClassIndex) {
        let fuel = FuelFractionIndex::new(&[SourceTypeFuelFractionRow {
            source_type_id: 21,
            model_year_id: 2015,
            fuel_type_id: 1,
            fuel_fraction: 1.0,
        }]);
        let reg = RegClassIndex::new(&[
            RegClassSourceTypeFractionRow {
                source_type_id: 21,
                fuel_type_id: 1,
                model_year_id: 2015,
                reg_class_id: 30,
                reg_class_fraction: 0.6,
            },
            RegClassSourceTypeFractionRow {
                source_type_id: 21,
                fuel_type_id: 1,
                model_year_id: 2015,
                reg_class_id: 40,
                reg_class_fraction: 0.4,
            },
        ]);
        (fuel, reg)
    }

    #[test]
    fn source_hours_splits_across_the_source_bin() {
        let (fuel, reg) = fuel_and_reg();
        let inputs = ActivityInputs {
            context: ctx(),
            hour_day: vec![HourDayRow {
                hour_day_id: 85,
                day_id: 5,
                hour_id: 8,
            }],
            link: vec![LinkRow {
                link_id: 900,
                zone_id: 261610,
                road_type_id: 4,
                link_volume: 0.0,
            }],
            source_hours: vec![SourceHoursRow {
                hour_day_id: 85,
                month_id: 7,
                year_id: 2020,
                age_id: 5,
                link_id: 900,
                source_type_id: 21,
                source_hours: 100.0,
            }],
            ..ActivityInputs::default()
        };
        let rows = source_hours(&inputs, &fuel, &reg);
        assert_eq!(rows.len(), 2); // one per regulatory class
                                   // 100 * 1.0 * 0.6 and 100 * 1.0 * 0.4.
        assert!((rows[0].activity - 60.0).abs() < 1e-9);
        assert!((rows[1].activity - 40.0).abs() < 1e-9);
        let r = &rows[0];
        assert_eq!(r.activity_type_id, 2);
        assert_eq!(r.day_id, 5);
        assert_eq!(r.hour_id, 8);
        assert_eq!(r.month_id, 7);
        assert_eq!(r.link_id, 900);
        assert_eq!(r.road_type_id, 4); // from the joined Link, not the context
        assert_eq!(r.zone_id, 261610); // from the context
        assert_eq!(r.model_year_id, 2015); // yearID - ageID
        assert_eq!(r.reg_class_id, 30);
        assert_eq!(r.fuel_type_id, 1);
    }

    #[test]
    fn source_hours_drops_rows_with_no_link_or_hourday_match() {
        let (fuel, reg) = fuel_and_reg();
        let base = SourceHoursRow {
            hour_day_id: 85,
            month_id: 7,
            year_id: 2020,
            age_id: 5,
            link_id: 900,
            source_type_id: 21,
            source_hours: 100.0,
        };
        // HourDay present but the link is missing.
        let inputs = ActivityInputs {
            context: ctx(),
            hour_day: vec![HourDayRow {
                hour_day_id: 85,
                day_id: 5,
                hour_id: 8,
            }],
            link: vec![],
            source_hours: vec![base],
            ..ActivityInputs::default()
        };
        assert!(source_hours(&inputs, &fuel, &reg).is_empty());
    }

    #[test]
    fn sho_is_link_located_and_type_4() {
        let (fuel, reg) = fuel_and_reg();
        let inputs = ActivityInputs {
            context: ctx(),
            hour_day: vec![HourDayRow {
                hour_day_id: 85,
                day_id: 5,
                hour_id: 8,
            }],
            link: vec![LinkRow {
                link_id: 900,
                zone_id: 261610,
                road_type_id: 4,
                link_volume: 0.0,
            }],
            sho: vec![ShoRow {
                hour_day_id: 85,
                month_id: 7,
                year_id: 2020,
                age_id: 5,
                link_id: 900,
                source_type_id: 21,
                sho: 50.0,
            }],
            ..ActivityInputs::default()
        };
        let rows = sho(&inputs, &fuel, &reg);
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r.activity_type_id == 4));
        let total: f64 = rows.iter().map(|r| r.activity).sum();
        assert!((total - 50.0).abs() < 1e-9); // regclass fractions sum to 1
    }

    #[test]
    fn shp_is_zone_located_with_the_rows_own_zone() {
        let (fuel, reg) = fuel_and_reg();
        let inputs = ActivityInputs {
            context: ctx(),
            hour_day: vec![HourDayRow {
                hour_day_id: 85,
                day_id: 5,
                hour_id: 8,
            }],
            shp: vec![ShpRow {
                hour_day_id: 85,
                month_id: 7,
                year_id: 2020,
                age_id: 5,
                zone_id: 777,
                source_type_id: 21,
                shp: 10.0,
            }],
            ..ActivityInputs::default()
        };
        let rows = shp(&inputs, &fuel, &reg);
        assert_eq!(rows.len(), 2);
        let r = &rows[0];
        assert_eq!(r.activity_type_id, 5);
        assert_eq!(r.zone_id, 777); // the SHP row's zone
        assert_eq!(r.link_id, 2616100); // the iteration link
        assert_eq!(r.road_type_id, 5); // the iteration road type
    }

    #[test]
    fn starts_is_zone_located_with_the_iteration_zone() {
        let (fuel, reg) = fuel_and_reg();
        let inputs = ActivityInputs {
            context: ctx(),
            hour_day: vec![HourDayRow {
                hour_day_id: 85,
                day_id: 5,
                hour_id: 8,
            }],
            starts: vec![StartsRow {
                hour_day_id: 85,
                month_id: 7,
                year_id: 2020,
                age_id: 5,
                zone_id: 777,
                source_type_id: 21,
                starts: 8.0,
            }],
            ..ActivityInputs::default()
        };
        let rows = starts(&inputs, &fuel, &reg);
        assert_eq!(rows.len(), 2);
        let r = &rows[0];
        assert_eq!(r.activity_type_id, 7);
        assert_eq!(r.zone_id, 261610); // the iteration zone, not the row's 777
        let total: f64 = rows.iter().map(|r| r.activity).sum();
        assert!((total - 8.0).abs() < 1e-9);
    }
}

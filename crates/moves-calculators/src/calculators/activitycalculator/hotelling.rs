//! The hotelling activity sections: `ExtendedIdleHours` and `hotellingHours`.
//!
//! Both port a `Processing` section of `ActivityCalculator.sql` that splits
//! the `hotellingHours` base table by operating mode:
//!
//! `activity = hotellingHours * opModeFraction * regClassFraction`
//!
//! Unlike the source-hours sections, the hotelling families carry `fuelTypeID`
//! directly on the activity row — they never expand through
//! `sourceTypeFuelFraction`. They join `RegClassSourceTypeFraction` on that
//! fuel type and `hotellingActivityDistribution` on a model-year range:
//!
//! * [`extended_idle_hours`] keeps `opModeID = 200`, emitting `activityTypeID`
//! 3 (extended idle).
//! * [`hotelling_hours`] keeps `opModeID ∈ {201, 203, 204}`, emitting
//! `activityTypeID` 13 / 14 / 15 (hotelling diesel-aux / battery-or-AC /
//! engines-off).
//!
//! Only the `WithRegClassID` script variant is ported — see the
//! [module docs](super).

use std::collections::HashMap;

use super::fuelfraction::{FuelRegClassWeight, RegClassIndex};
use super::inputs::{ActivityInputs, HotellingActivityDistributionRow, HourDayRow};
use super::model::ActivityRow;
use super::rowbuild::{weighted, RowTemplate};

/// Index `HourDay` by `hourDayID` (a primary key — one row each).
fn hour_day_index(rows: &[HourDayRow]) -> HashMap<i32, HourDayRow> {
    rows.iter().map(|r| (r.hour_day_id, *r)).collect()
}

/// Index `hotellingActivityDistribution` by `fuelTypeID`; the model-year
/// range and `opModeID` are filtered per source row.
fn hotelling_dist_index(
    rows: &[HotellingActivityDistributionRow],
) -> HashMap<i32, Vec<HotellingActivityDistributionRow>> {
    let mut map: HashMap<i32, Vec<HotellingActivityDistributionRow>> = HashMap::new();
    for r in rows {
        map.entry(r.fuel_type_id).or_default().push(*r);
    }
    map
}

/// Shared body of the two hotelling sections.
///
/// `op_mode_matches` selects the `hotellingActivityDistribution` op modes the
/// section processes; `activity_type_id` maps a matched op mode to the
/// output `activityTypeID`.
fn hotelling_section<F, G>(
    inputs: &ActivityInputs,
    reg: &RegClassIndex,
    op_mode_matches: F,
    activity_type_id: G,
) -> Vec<ActivityRow>
where
    F: Fn(i32) -> bool,
    G: Fn(i32) -> i32,
{
    let hour_day = hour_day_index(&inputs.hour_day);
    let dist = hotelling_dist_index(&inputs.hotelling_activity_distribution);
    let ctx = &inputs.context;
    let mut out = Vec::new();
    for s in &inputs.hotelling_hours {
 // INNER JOIN HourDay h.
        let Some(h) = hour_day.get(&s.hour_day_id) else {
            continue;
        };
        let model_year_id = s.year_id - s.age_id;
 // INNER JOIN RegClassSourceTypeFraction stf — keyed by the activity
 // row's own fuel type. Phrased as `FuelRegClassWeight`s with that
 // fixed fuel type so [`weighted`] can complete the rows.
        let reg_weights: Vec<FuelRegClassWeight> = reg
            .reg_classes(s.source_type_id, s.fuel_type_id, model_year_id)
            .iter()
            .map(|&(reg_class_id, reg_class_fraction)| FuelRegClassWeight {
                fuel_type_id: s.fuel_type_id,
                reg_class_id,
                weight: reg_class_fraction,
            })
            .collect();
        if reg_weights.is_empty() {
            continue;
        }
 // INNER JOIN hotellingActivityDistribution ha — same fuel type, an
 // op mode the section processes, and a model-year range covering the
 // row's model year.
        let Some(candidates) = dist.get(&s.fuel_type_id) else {
            continue;
        };
        for ha in candidates {
            if !op_mode_matches(ha.op_mode_id) {
                continue;
            }
            if model_year_id < ha.begin_model_year_id || model_year_id > ha.end_model_year_id {
                continue;
            }
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
                activity_type_id: activity_type_id(ha.op_mode_id),
            };
            weighted(
                &template,
                s.hotelling_hours * ha.op_mode_fraction,
                &reg_weights,
                &mut out,
            );
        }
    }
    out
}

/// `ExtendedIdleHours` section — `activityTypeID` 3.
///
/// Splits `hotellingHours` by the extended-idle operating mode (`opModeID`
/// 200): `activity = hotellingHours * opModeFraction * regClassFraction`.
#[must_use]
pub fn extended_idle_hours(inputs: &ActivityInputs, reg: &RegClassIndex) -> Vec<ActivityRow> {
    hotelling_section(inputs, reg, |op_mode| op_mode == 200, |_| 3)
}

/// `hotellingHours` section — `activityTypeID` 13 / 14 / 15.
///
/// Splits `hotellingHours` by the hotelling operating modes: `opModeID` 201
/// (diesel auxiliary power → 13), 203 (battery or shore AC → 14), and 204
/// (all engines off → 15). `activity = hotellingHours * opModeFraction *
/// regClassFraction`.
#[must_use]
pub fn hotelling_hours(inputs: &ActivityInputs, reg: &RegClassIndex) -> Vec<ActivityRow> {
    hotelling_section(
        inputs,
        reg,
        |op_mode| matches!(op_mode, 201 | 203 | 204),
        |op_mode| match op_mode {
            201 => 13,
            203 => 14,
            204 => 15,
 // The SQL `CASE` falls through to 8; unreachable here because the
 // `opModeID IN (201,203,204)` join already excluded everything else.
            _ => 8,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::super::inputs::{
        HotellingActivityDistributionRow, HotellingHoursRow, HourDayRow, IterationContext,
        RegClassSourceTypeFractionRow,
    };
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

 /// One diesel (fuelType 2) hotelling-hours row, model year 2015, with a
 /// single regulatory class covering the whole bin.
    fn base_inputs() -> ActivityInputs {
        ActivityInputs {
            context: ctx(),
            hour_day: vec![HourDayRow {
                hour_day_id: 85,
                day_id: 5,
                hour_id: 8,
            }],
            hotelling_hours: vec![HotellingHoursRow {
                hour_day_id: 85,
                month_id: 7,
                year_id: 2020,
                age_id: 5,
                zone_id: 261610,
                source_type_id: 62,
                fuel_type_id: 2,
                hotelling_hours: 200.0,
            }],
            reg_class_source_type_fraction: vec![RegClassSourceTypeFractionRow {
                source_type_id: 62,
                fuel_type_id: 2,
                model_year_id: 2015,
                reg_class_id: 47,
                reg_class_fraction: 1.0,
            }],
            ..ActivityInputs::default()
        }
    }

    #[test]
    fn extended_idle_keeps_only_op_mode_200() {
        let mut inputs = base_inputs();
        inputs.hotelling_activity_distribution = vec![
            HotellingActivityDistributionRow {
                op_mode_id: 200,
                begin_model_year_id: 1990,
                end_model_year_id: 2025,
                fuel_type_id: 2,
                op_mode_fraction: 0.25,
            },
 // opMode 201 is a hotelling mode — extended idle must ignore it.
            HotellingActivityDistributionRow {
                op_mode_id: 201,
                begin_model_year_id: 1990,
                end_model_year_id: 2025,
                fuel_type_id: 2,
                op_mode_fraction: 0.75,
            },
        ];
        let reg = RegClassIndex::new(&inputs.reg_class_source_type_fraction);
        let rows = extended_idle_hours(&inputs, &reg);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].activity_type_id, 3);
 // 200 hotelling hours * 0.25 op-mode fraction * 1.0 reg-class fraction.
        assert!((rows[0].activity - 50.0).abs() < 1e-9);
        assert_eq!(rows[0].fuel_type_id, 2);
        assert_eq!(rows[0].model_year_id, 2015);
    }

    #[test]
    fn hotelling_maps_op_modes_to_activity_types() {
        let mut inputs = base_inputs();
        inputs.hotelling_activity_distribution = vec![
            HotellingActivityDistributionRow {
                op_mode_id: 201,
                begin_model_year_id: 1990,
                end_model_year_id: 2025,
                fuel_type_id: 2,
                op_mode_fraction: 0.5,
            },
            HotellingActivityDistributionRow {
                op_mode_id: 203,
                begin_model_year_id: 1990,
                end_model_year_id: 2025,
                fuel_type_id: 2,
                op_mode_fraction: 0.3,
            },
            HotellingActivityDistributionRow {
                op_mode_id: 204,
                begin_model_year_id: 1990,
                end_model_year_id: 2025,
                fuel_type_id: 2,
                op_mode_fraction: 0.2,
            },
 // opMode 200 is extended idle — the hotelling section ignores it.
            HotellingActivityDistributionRow {
                op_mode_id: 200,
                begin_model_year_id: 1990,
                end_model_year_id: 2025,
                fuel_type_id: 2,
                op_mode_fraction: 9.9,
            },
        ];
        let reg = RegClassIndex::new(&inputs.reg_class_source_type_fraction);
        let rows = hotelling_hours(&inputs, &reg);
        assert_eq!(rows.len(), 3);
        let by_type: Vec<(i32, f64)> = rows
            .iter()
            .map(|r| (r.activity_type_id, r.activity))
            .collect();
        assert!(by_type.contains(&(13, 100.0))); // 200 * 0.5
        assert!(by_type.contains(&(14, 60.0))); // 200 * 0.3
        assert!(by_type.contains(&(15, 40.0))); // 200 * 0.2
    }

    #[test]
    fn model_year_outside_the_distribution_range_is_dropped() {
        let mut inputs = base_inputs();
 // Row model year is 2015; this range starts in 2018.
        inputs.hotelling_activity_distribution = vec![HotellingActivityDistributionRow {
            op_mode_id: 200,
            begin_model_year_id: 2018,
            end_model_year_id: 2025,
            fuel_type_id: 2,
            op_mode_fraction: 0.25,
        }];
        let reg = RegClassIndex::new(&inputs.reg_class_source_type_fraction);
        assert!(extended_idle_hours(&inputs, &reg).is_empty());
    }

    #[test]
    fn fuel_type_mismatch_drops_the_distribution_join() {
        let mut inputs = base_inputs();
 // Distribution is for gasoline (fuelType 1); the row is diesel (2).
        inputs.hotelling_activity_distribution = vec![HotellingActivityDistributionRow {
            op_mode_id: 200,
            begin_model_year_id: 1990,
            end_model_year_id: 2025,
            fuel_type_id: 1,
            op_mode_fraction: 0.25,
        }];
        let reg = RegClassIndex::new(&inputs.reg_class_source_type_fraction);
        assert!(extended_idle_hours(&inputs, &reg).is_empty());
    }

    #[test]
    fn no_regulatory_class_drops_the_row() {
        let mut inputs = base_inputs();
        inputs.reg_class_source_type_fraction.clear();
        inputs.hotelling_activity_distribution = vec![HotellingActivityDistributionRow {
            op_mode_id: 200,
            begin_model_year_id: 1990,
            end_model_year_id: 2025,
            fuel_type_id: 2,
            op_mode_fraction: 0.25,
        }];
        let reg = RegClassIndex::new(&inputs.reg_class_source_type_fraction);
        assert!(extended_idle_hours(&inputs, &reg).is_empty());
    }
}

//! Output-row assembly shared by the activity sections.
//!
//! Every `INSERT ... SELECT` in the script's `Processing` section emits one
//! `##ActivityTable##` row per source-bin leaf of a base activity quantity.
//! The columns that do not vary across the leaves — the location, time, and
//! source-type identifiers — are the same for every row a single base record
//! produces; [`RowTemplate`] captures them once and [`weighted`] completes
//! the per-leaf `(regClassID, fuelTypeID, activity)` columns.

use super::fuelfraction::FuelRegClassWeight;
use super::model::ActivityRow;

/// The non-source-bin columns of an [`ActivityRow`] — everything fixed for
/// all rows a single base activity record (one `SourceHours`, `SHO`, …, or
/// `Population` quantity) expands into.
#[derive(Debug, Clone, Copy)]
pub struct RowTemplate {
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
 /// `sourceTypeID`.
    pub source_type_id: i32,
 /// `modelYearID`.
    pub model_year_id: i32,
 /// `roadTypeID`.
    pub road_type_id: i32,
 /// `activityTypeID`.
    pub activity_type_id: i32,
}

/// Expand one base activity quantity into output rows: one [`ActivityRow`]
/// per `(fuelType, regClass)` leaf, each carrying `base_value * weight`.
///
/// `weight` is `fuelFraction * regClassFraction` (the non-hotelling sections)
/// or `regClassFraction` alone (the hotelling sections, which fold
/// `opModeFraction` into `base_value`). An empty `weights` slice — the
/// source bin had no `sourceTypeFuelFraction` / `RegClassSourceTypeFraction`
/// match — emits nothing, matching the SQL inner joins.
pub fn weighted(
    template: &RowTemplate,
    base_value: f64,
    weights: &[FuelRegClassWeight],
    out: &mut Vec<ActivityRow>,
) {
    out.reserve(weights.len());
    for w in weights {
        out.push(ActivityRow {
            year_id: template.year_id,
            month_id: template.month_id,
            day_id: template.day_id,
            hour_id: template.hour_id,
            state_id: template.state_id,
            county_id: template.county_id,
            zone_id: template.zone_id,
            link_id: template.link_id,
            source_type_id: template.source_type_id,
            reg_class_id: w.reg_class_id,
            fuel_type_id: w.fuel_type_id,
            model_year_id: template.model_year_id,
            road_type_id: template.road_type_id,
            activity_type_id: template.activity_type_id,
            activity: base_value * w.weight,
        });
    }
}

//! Plan-driven aggregation of MOVES output rows — ports the roll-up half
//! of `OutputProcessor.java` (migration plan Task 26).
//!
//! `OutputProcessor.java` runs the `INSERT … SELECT … GROUP BY` statements
//! built by `AggregationSQLGenerator` against the per-iteration worker
//! output, then writes the rolled-up rows into the final `MOVESOutput` /
//! `MOVESActivityOutput` tables. The Rust port splits that into three
//! pieces:
//!
//! * **Task 25** ([`crate::aggregation`]) — derives the *column-shape
//!   plan*: which dimensions are `GROUP BY` keys, which collapse to
//!   `NULL`, and which metric is `SUM`-ed (with optional temporal
//!   rescaling).
//! * **Task 26** (this module) — *applies* an [`AggregationPlan`] to a
//!   batch of strongly-typed [`EmissionRecord`] / [`ActivityRecord`] rows:
//!   groups by the plan's keys, sums the metric, and emits the rolled-up
//!   rows.
//! * **Task 89** ([`crate::output_processor`]) — writes the rolled-up rows
//!   to partitioned Parquet.
//!
//! [`OutputProcessor`](crate::OutputProcessor) ties the last two together
//! with
//! [`write_aggregated_emissions`](crate::OutputProcessor::write_aggregated_emissions)
//! and
//! [`write_aggregated_activity`](crate::OutputProcessor::write_aggregated_activity).
//!
//! Aggregation runs over strongly-typed record vectors rather than a
//! Polars `DataFrame`: the data plane (Task 50) has not landed, and the
//! group-by / sum mechanics are identical whichever row representation the
//! calculators ultimately deliver. When Task 50 lands, the same plan can
//! drive a `LazyFrame` `group_by`/`agg`; this module's tests pin the
//! reference semantics that port must reproduce.
//!
//! # `SUM` and SQL `NULL`
//!
//! The metric column is summed with SQL `SUM` semantics: `NULL` inputs are
//! skipped, and a group whose every input metric is `NULL` produces a
//! `NULL` (`None`) metric rather than `0.0`.
//!
//! # Temporal rescaling
//!
//! When the plan's `SUM` carries a [`TemporalScaling`] other than
//! [`TemporalScaling::None`], each row's metric is multiplied by a factor
//! before being added to its group. The factor is supplied by a caller
//! [`TemporalScalingFactors`] implementation, because the concrete factor
//! values (weeks per calendar month, weekday/weekend portion-of-week
//! split) come from the `monthOfAnyYear` / `dayOfAnyWeek` reference tables
//! the framework reads at run time. [`UnitScaling`] is the identity
//! implementation — correct for any [`TemporalScaling::None`] plan and a
//! placeholder until the reference-table-backed factors are wired in Task
//! 27 (`MOVESEngine`).
//!
//! # Determinism
//!
//! Output rows are emitted in group-key sort order, independent of input
//! order. The metric sum accumulates in input-slice order; as with the
//! [`OutputProcessor`](crate::OutputProcessor) writer, byte-identical
//! output is guaranteed for any *fixed* input ordering.
//!
//! # Nonroad activity weighting
//!
//! This module does **not** run the Nonroad activity-weighting pre-pass.
//! When [`AggregationPlan::needs_nonroad_activity_weight`] is `true` the
//! caller must apply that weighting to the records first (see
//! `AggregationSQLGenerator.nrActivityWeightSQL` for the reference
//! algorithm).

use std::collections::BTreeMap;

use moves_data::output_schema::{ActivityRecord, EmissionRecord};

use crate::aggregation::{AggregationPlan, AggregationTable, TemporalScaling};
use crate::error::{Error, Result};

/// Per-row temporal scaling factors for metric aggregation.
///
/// When an [`AggregationPlan`]'s `SUM` column carries a [`TemporalScaling`]
/// other than [`TemporalScaling::None`], each input row's metric is
/// multiplied by a factor that depends on the row's time keys before the
/// group sum is taken. The MOVES Worker SQL applies the same factor via a
/// `SUM(metric * factor)` term; the factor values come from the
/// `monthOfAnyYear` / `dayOfAnyWeek` reference tables.
///
/// Implementors decide how to handle `None` time keys (a row missing the
/// `yearID`/`monthID`/`dayID` it would be scaled by). [`UnitScaling`]
/// returns `1.0` unconditionally.
pub trait TemporalScalingFactors {
    /// Weeks-per-month factor applied to a row landing in
    /// (`year_id`, `month_id`) when the plan's [`TemporalScaling`] is
    /// [`TemporalScaling::WeeksPerMonth`].
    fn weeks_per_month(&self, year_id: Option<i16>, month_id: Option<i16>) -> f64;

    /// Portion-of-week-per-day factor applied to a row with day key
    /// `day_id` when the plan's [`TemporalScaling`] is
    /// [`TemporalScaling::PortionOfWeekPerDay`].
    fn portion_of_week_per_day(&self, day_id: Option<i16>) -> f64;
}

/// A [`TemporalScalingFactors`] that always returns `1.0`.
///
/// Use this for any plan whose `SUM` carries [`TemporalScaling::None`]
/// (where no factor is consulted at all) and as the Phase 2 placeholder
/// until the `monthOfAnyYear` / `dayOfAnyWeek`-backed factors are wired in
/// by Task 27 (`MOVESEngine`).
#[derive(Debug, Clone, Copy, Default)]
pub struct UnitScaling;

impl TemporalScalingFactors for UnitScaling {
    fn weeks_per_month(&self, _year_id: Option<i16>, _month_id: Option<i16>) -> f64 {
        1.0
    }

    fn portion_of_week_per_day(&self, _day_id: Option<i16>) -> f64 {
        1.0
    }
}

/// Aggregate a batch of [`EmissionRecord`]s with an emission
/// [`AggregationPlan`].
///
/// Input rows are grouped by the plan's [`group_by`](AggregationPlan::group_by)
/// key columns; within each group `emissionQuant` is summed (with the
/// plan's [`TemporalScaling`] applied per row) and every non-key dimension
/// column collapses to `NULL`. `emissionRate` is not a metric the emission
/// plan tracks, so the rolled-up rows carry `emission_rate = None`.
/// `runHash` flows through unchanged — it is `1:1` with the `MOVESRunID`
/// group key.
///
/// Output rows are returned in group-key sort order. The result feeds
/// straight into [`OutputProcessor::write_emissions`](crate::OutputProcessor::write_emissions);
/// [`OutputProcessor::write_aggregated_emissions`](crate::OutputProcessor::write_aggregated_emissions)
/// composes the two.
///
/// # Errors
///
/// Returns [`Error::AggregationPlanMismatch`] if `plan` does not target
/// [`AggregationTable::Emission`], if it does not `SUM` exactly the
/// `emissionQuant` column, or if a group-by key names a column that is not
/// part of the `MOVESOutput` schema.
pub fn aggregate_emissions(
    plan: &AggregationPlan,
    records: &[EmissionRecord],
    factors: &impl TemporalScalingFactors,
) -> Result<Vec<EmissionRecord>> {
    if plan.table != AggregationTable::Emission {
        return Err(Error::AggregationPlanMismatch(format!(
            "aggregate_emissions needs an Emission plan, got {:?}",
            plan.table
        )));
    }
    let scaling = plan_sum_scaling(plan, "emissionQuant")?;
    let keys = plan.group_by();

    let mut groups: BTreeMap<Vec<KeyValue>, Accum<'_, EmissionRecord>> = BTreeMap::new();
    for rec in records {
        let mut key = Vec::with_capacity(keys.len());
        for &column in &keys {
            key.push(emission_key_value(rec, column)?);
        }
        let factor = row_factor(scaling, factors, rec.year_id, rec.month_id, rec.day_id);
        groups
            .entry(key)
            .or_insert_with(|| Accum::new(rec))
            .add(rec.emission_quant, factor);
    }

    let mut out = Vec::with_capacity(groups.len());
    for accum in groups.into_values() {
        let metric = accum.metric();
        out.push(build_emission_output(&keys, accum.rep, metric));
    }
    Ok(out)
}

/// Aggregate a batch of [`ActivityRecord`]s with an activity
/// [`AggregationPlan`].
///
/// Behaves like [`aggregate_emissions`] but sums the `activity` column.
/// `activityTypeID` is always a group key (the canonical activity schema
/// keeps activity rows separated by type), and `runHash` flows through
/// unchanged.
///
/// # Errors
///
/// Returns [`Error::AggregationPlanMismatch`] if `plan` does not target
/// [`AggregationTable::Activity`], if it does not `SUM` exactly the
/// `activity` column, or if a group-by key names a column that is not part
/// of the `MOVESActivityOutput` schema.
pub fn aggregate_activity(
    plan: &AggregationPlan,
    records: &[ActivityRecord],
    factors: &impl TemporalScalingFactors,
) -> Result<Vec<ActivityRecord>> {
    if plan.table != AggregationTable::Activity {
        return Err(Error::AggregationPlanMismatch(format!(
            "aggregate_activity needs an Activity plan, got {:?}",
            plan.table
        )));
    }
    let scaling = plan_sum_scaling(plan, "activity")?;
    let keys = plan.group_by();

    let mut groups: BTreeMap<Vec<KeyValue>, Accum<'_, ActivityRecord>> = BTreeMap::new();
    for rec in records {
        let mut key = Vec::with_capacity(keys.len());
        for &column in &keys {
            key.push(activity_key_value(rec, column)?);
        }
        let factor = row_factor(scaling, factors, rec.year_id, rec.month_id, rec.day_id);
        groups
            .entry(key)
            .or_insert_with(|| Accum::new(rec))
            .add(rec.activity, factor);
    }

    let mut out = Vec::with_capacity(groups.len());
    for accum in groups.into_values() {
        let metric = accum.metric();
        out.push(build_activity_output(&keys, accum.rep, metric));
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

/// One component of a group-by key. Integer columns (`i16` and `i32`
/// alike) widen to `i64`; `SCC` is the lone text key. `Ord` drives the
/// `BTreeMap` that gives aggregation its deterministic output order —
/// `None` sorts before `Some`, matching the writer's NULL-partition-first
/// convention.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum KeyValue {
    Int(Option<i64>),
    Text(Option<String>),
}

/// Running aggregate for one group: the first record seen (source of the
/// preserved key columns and `runHash`) and the metric sum.
struct Accum<'a, R> {
    rep: &'a R,
    sum: f64,
    saw_value: bool,
}

impl<'a, R> Accum<'a, R> {
    fn new(rep: &'a R) -> Self {
        Self {
            rep,
            sum: 0.0,
            saw_value: false,
        }
    }

    /// Fold one row's metric into the group. `None` (SQL `NULL`) is
    /// skipped; `factor` carries the per-row temporal rescaling.
    fn add(&mut self, metric: Option<f64>, factor: f64) {
        if let Some(value) = metric {
            self.sum += value * factor;
            self.saw_value = true;
        }
    }

    /// SQL-`SUM` result: `None` when the group held no non-`NULL` input.
    fn metric(&self) -> Option<f64> {
        self.saw_value.then_some(self.sum)
    }
}

/// Resolve the per-row temporal rescaling factor for one input record.
fn row_factor(
    scaling: TemporalScaling,
    factors: &impl TemporalScalingFactors,
    year_id: Option<i16>,
    month_id: Option<i16>,
    day_id: Option<i16>,
) -> f64 {
    match scaling {
        TemporalScaling::None => 1.0,
        TemporalScaling::WeeksPerMonth => factors.weeks_per_month(year_id, month_id),
        TemporalScaling::PortionOfWeekPerDay => factors.portion_of_week_per_day(day_id),
    }
}

/// Extract the single expected `SUM` column's [`TemporalScaling`], or fail
/// if the plan's `SUM` shape does not match what the table writer expects.
fn plan_sum_scaling(plan: &AggregationPlan, metric: &str) -> Result<TemporalScaling> {
    let sums = plan.sum_columns();
    match sums.as_slice() {
        [(column, scaling)] if *column == metric => Ok(*scaling),
        _ => Err(Error::AggregationPlanMismatch(format!(
            "expected exactly one SUM column '{metric}', plan has {:?}",
            sums.iter().map(|(c, _)| *c).collect::<Vec<_>>()
        ))),
    }
}

/// `value` when `keep` is set, `None` otherwise — the per-column rule for a
/// dimension that may or may not survive aggregation.
fn keep_opt<T>(keep: bool, value: Option<T>) -> Option<T> {
    if keep {
        value
    } else {
        None
    }
}

/// Read one `MOVESOutput` column off an [`EmissionRecord`] as a sortable
/// [`KeyValue`].
fn emission_key_value(rec: &EmissionRecord, column: &str) -> Result<KeyValue> {
    Ok(match column {
        "MOVESRunID" => KeyValue::Int(Some(i64::from(rec.moves_run_id))),
        "iterationID" => KeyValue::Int(rec.iteration_id.map(i64::from)),
        "yearID" => KeyValue::Int(rec.year_id.map(i64::from)),
        "monthID" => KeyValue::Int(rec.month_id.map(i64::from)),
        "dayID" => KeyValue::Int(rec.day_id.map(i64::from)),
        "hourID" => KeyValue::Int(rec.hour_id.map(i64::from)),
        "stateID" => KeyValue::Int(rec.state_id.map(i64::from)),
        "countyID" => KeyValue::Int(rec.county_id.map(i64::from)),
        "zoneID" => KeyValue::Int(rec.zone_id.map(i64::from)),
        "linkID" => KeyValue::Int(rec.link_id.map(i64::from)),
        "pollutantID" => KeyValue::Int(rec.pollutant_id.map(i64::from)),
        "processID" => KeyValue::Int(rec.process_id.map(i64::from)),
        "sourceTypeID" => KeyValue::Int(rec.source_type_id.map(i64::from)),
        "regClassID" => KeyValue::Int(rec.reg_class_id.map(i64::from)),
        "fuelTypeID" => KeyValue::Int(rec.fuel_type_id.map(i64::from)),
        "fuelSubTypeID" => KeyValue::Int(rec.fuel_sub_type_id.map(i64::from)),
        "modelYearID" => KeyValue::Int(rec.model_year_id.map(i64::from)),
        "roadTypeID" => KeyValue::Int(rec.road_type_id.map(i64::from)),
        "SCC" => KeyValue::Text(rec.scc.clone()),
        "engTechID" => KeyValue::Int(rec.eng_tech_id.map(i64::from)),
        "sectorID" => KeyValue::Int(rec.sector_id.map(i64::from)),
        "hpID" => KeyValue::Int(rec.hp_id.map(i64::from)),
        other => {
            return Err(Error::AggregationPlanMismatch(format!(
                "emission aggregation: group-by column '{other}' is not a MOVESOutput column"
            )))
        }
    })
}

/// Read one `MOVESActivityOutput` column off an [`ActivityRecord`] as a
/// sortable [`KeyValue`].
fn activity_key_value(rec: &ActivityRecord, column: &str) -> Result<KeyValue> {
    Ok(match column {
        "MOVESRunID" => KeyValue::Int(Some(i64::from(rec.moves_run_id))),
        "iterationID" => KeyValue::Int(rec.iteration_id.map(i64::from)),
        "yearID" => KeyValue::Int(rec.year_id.map(i64::from)),
        "monthID" => KeyValue::Int(rec.month_id.map(i64::from)),
        "dayID" => KeyValue::Int(rec.day_id.map(i64::from)),
        "hourID" => KeyValue::Int(rec.hour_id.map(i64::from)),
        "stateID" => KeyValue::Int(rec.state_id.map(i64::from)),
        "countyID" => KeyValue::Int(rec.county_id.map(i64::from)),
        "zoneID" => KeyValue::Int(rec.zone_id.map(i64::from)),
        "linkID" => KeyValue::Int(rec.link_id.map(i64::from)),
        "sourceTypeID" => KeyValue::Int(rec.source_type_id.map(i64::from)),
        "regClassID" => KeyValue::Int(rec.reg_class_id.map(i64::from)),
        "fuelTypeID" => KeyValue::Int(rec.fuel_type_id.map(i64::from)),
        "fuelSubTypeID" => KeyValue::Int(rec.fuel_sub_type_id.map(i64::from)),
        "modelYearID" => KeyValue::Int(rec.model_year_id.map(i64::from)),
        "roadTypeID" => KeyValue::Int(rec.road_type_id.map(i64::from)),
        "SCC" => KeyValue::Text(rec.scc.clone()),
        "engTechID" => KeyValue::Int(rec.eng_tech_id.map(i64::from)),
        "sectorID" => KeyValue::Int(rec.sector_id.map(i64::from)),
        "hpID" => KeyValue::Int(rec.hp_id.map(i64::from)),
        "activityTypeID" => KeyValue::Int(rec.activity_type_id.map(i64::from)),
        other => {
            return Err(Error::AggregationPlanMismatch(format!(
                "activity aggregation: group-by column '{other}' is not a \
                 MOVESActivityOutput column"
            )))
        }
    })
}

/// Build one rolled-up [`EmissionRecord`]: key columns copied from the
/// group representative, every other dimension `NULL`-ed, `emissionQuant`
/// set to the group sum.
fn build_emission_output(
    keys: &[&str],
    rep: &EmissionRecord,
    metric: Option<f64>,
) -> EmissionRecord {
    let kept = |column: &str| keys.contains(&column);
    EmissionRecord {
        moves_run_id: rep.moves_run_id,
        iteration_id: keep_opt(kept("iterationID"), rep.iteration_id),
        year_id: keep_opt(kept("yearID"), rep.year_id),
        month_id: keep_opt(kept("monthID"), rep.month_id),
        day_id: keep_opt(kept("dayID"), rep.day_id),
        hour_id: keep_opt(kept("hourID"), rep.hour_id),
        state_id: keep_opt(kept("stateID"), rep.state_id),
        county_id: keep_opt(kept("countyID"), rep.county_id),
        zone_id: keep_opt(kept("zoneID"), rep.zone_id),
        link_id: keep_opt(kept("linkID"), rep.link_id),
        pollutant_id: keep_opt(kept("pollutantID"), rep.pollutant_id),
        process_id: keep_opt(kept("processID"), rep.process_id),
        source_type_id: keep_opt(kept("sourceTypeID"), rep.source_type_id),
        reg_class_id: keep_opt(kept("regClassID"), rep.reg_class_id),
        fuel_type_id: keep_opt(kept("fuelTypeID"), rep.fuel_type_id),
        fuel_sub_type_id: keep_opt(kept("fuelSubTypeID"), rep.fuel_sub_type_id),
        model_year_id: keep_opt(kept("modelYearID"), rep.model_year_id),
        road_type_id: keep_opt(kept("roadTypeID"), rep.road_type_id),
        scc: if kept("SCC") { rep.scc.clone() } else { None },
        eng_tech_id: keep_opt(kept("engTechID"), rep.eng_tech_id),
        sector_id: keep_opt(kept("sectorID"), rep.sector_id),
        hp_id: keep_opt(kept("hpID"), rep.hp_id),
        emission_quant: metric,
        // emissionRate is not a metric the emission plan tracks; the
        // rolled-up row drops it (rate-mode output goes to BaseRateOutput).
        emission_rate: None,
        run_hash: rep.run_hash.clone(),
    }
}

/// Build one rolled-up [`ActivityRecord`]: key columns copied from the
/// group representative, every other dimension `NULL`-ed, `activity` set to
/// the group sum.
fn build_activity_output(
    keys: &[&str],
    rep: &ActivityRecord,
    metric: Option<f64>,
) -> ActivityRecord {
    let kept = |column: &str| keys.contains(&column);
    ActivityRecord {
        moves_run_id: rep.moves_run_id,
        iteration_id: keep_opt(kept("iterationID"), rep.iteration_id),
        year_id: keep_opt(kept("yearID"), rep.year_id),
        month_id: keep_opt(kept("monthID"), rep.month_id),
        day_id: keep_opt(kept("dayID"), rep.day_id),
        hour_id: keep_opt(kept("hourID"), rep.hour_id),
        state_id: keep_opt(kept("stateID"), rep.state_id),
        county_id: keep_opt(kept("countyID"), rep.county_id),
        zone_id: keep_opt(kept("zoneID"), rep.zone_id),
        link_id: keep_opt(kept("linkID"), rep.link_id),
        source_type_id: keep_opt(kept("sourceTypeID"), rep.source_type_id),
        reg_class_id: keep_opt(kept("regClassID"), rep.reg_class_id),
        fuel_type_id: keep_opt(kept("fuelTypeID"), rep.fuel_type_id),
        fuel_sub_type_id: keep_opt(kept("fuelSubTypeID"), rep.fuel_sub_type_id),
        model_year_id: keep_opt(kept("modelYearID"), rep.model_year_id),
        road_type_id: keep_opt(kept("roadTypeID"), rep.road_type_id),
        scc: if kept("SCC") { rep.scc.clone() } else { None },
        eng_tech_id: keep_opt(kept("engTechID"), rep.eng_tech_id),
        sector_id: keep_opt(kept("sectorID"), rep.sector_id),
        hp_id: keep_opt(kept("hpID"), rep.hp_id),
        activity_type_id: keep_opt(kept("activityTypeID"), rep.activity_type_id),
        activity: metric,
        run_hash: rep.run_hash.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregation::{
        activity_aggregation, emission_aggregation, AggregationColumn, AggregationInputs,
    };
    use moves_runspec::model::{
        GeographicOutputDetail, Model, ModelScale, OutputBreakdown, OutputTimestep,
    };

    /// `weeks_per_month` returns the `monthID` itself as the factor, so a
    /// test can read the scaling back off the summed metric.
    struct MonthAsFactor;

    impl TemporalScalingFactors for MonthAsFactor {
        fn weeks_per_month(&self, _year_id: Option<i16>, month_id: Option<i16>) -> f64 {
            f64::from(month_id.unwrap_or(1))
        }
        fn portion_of_week_per_day(&self, day_id: Option<i16>) -> f64 {
            f64::from(day_id.unwrap_or(1))
        }
    }

    fn breakdown_all_false() -> OutputBreakdown {
        OutputBreakdown::default()
    }

    fn inputs<'a>(
        timestep: OutputTimestep,
        geo: GeographicOutputDetail,
        models: &'a [Model],
        breakdown: &'a OutputBreakdown,
    ) -> AggregationInputs<'a> {
        AggregationInputs {
            timestep,
            geographic_output_detail: geo,
            scale: ModelScale::Macro,
            domain: None,
            models,
            breakdown,
            output_population: false,
            reg_class_id: false,
            fuel_sub_type: false,
            eng_tech_id: false,
            sector: false,
        }
    }

    fn emission(pollutant: i16, month: i16, quant: Option<f64>) -> EmissionRecord {
        EmissionRecord {
            moves_run_id: 1,
            iteration_id: Some(1),
            year_id: Some(2020),
            month_id: Some(month),
            day_id: Some(5),
            hour_id: Some(12),
            state_id: Some(17),
            county_id: Some(17031),
            zone_id: Some(170310),
            link_id: Some(1),
            pollutant_id: Some(pollutant),
            process_id: Some(1),
            source_type_id: Some(21),
            reg_class_id: Some(20),
            fuel_type_id: Some(1),
            fuel_sub_type_id: Some(10),
            model_year_id: Some(2018),
            road_type_id: Some(3),
            scc: Some("2201001110".to_string()),
            eng_tech_id: Some(1),
            sector_id: None,
            hp_id: None,
            emission_quant: quant,
            // A non-null rate so the "rate is dropped on aggregation"
            // assertion is meaningful.
            emission_rate: Some(99.0),
            run_hash: "run-hash".to_string(),
        }
    }

    fn activity(activity_type: i16, month: i16, value: Option<f64>) -> ActivityRecord {
        ActivityRecord {
            moves_run_id: 1,
            iteration_id: Some(1),
            year_id: Some(2020),
            month_id: Some(month),
            day_id: Some(5),
            hour_id: Some(12),
            state_id: Some(17),
            county_id: Some(17031),
            zone_id: Some(170310),
            link_id: Some(1),
            source_type_id: Some(21),
            reg_class_id: Some(20),
            fuel_type_id: Some(1),
            fuel_sub_type_id: Some(10),
            model_year_id: Some(2018),
            road_type_id: Some(3),
            scc: Some("2201001110".to_string()),
            eng_tech_id: Some(1),
            sector_id: None,
            hp_id: None,
            activity_type_id: Some(activity_type),
            activity: value,
            run_hash: "run-hash".to_string(),
        }
    }

    #[test]
    fn unit_scaling_is_identity() {
        let u = UnitScaling;
        assert_eq!(u.weeks_per_month(Some(2020), Some(7)), 1.0);
        assert_eq!(u.portion_of_week_per_day(Some(5)), 1.0);
        assert_eq!(u.weeks_per_month(None, None), 1.0);
    }

    #[test]
    fn emission_year_nation_collapses_to_one_row_per_pollutant() {
        // Year + Nation + empty breakdown: only MOVESRunID, iterationID,
        // yearID, pollutantID survive. Rows that share a pollutant collapse.
        let b = breakdown_all_false();
        let plan = emission_aggregation(&inputs(
            OutputTimestep::Year,
            GeographicOutputDetail::Nation,
            &[Model::Onroad],
            &b,
        ));
        let rows = vec![
            emission(2, 1, Some(1.0)),
            emission(2, 7, Some(2.0)),
            emission(3, 1, Some(4.0)),
        ];
        let out = aggregate_emissions(&plan, &rows, &UnitScaling).unwrap();

        assert_eq!(out.len(), 2, "one row per distinct pollutantID");
        // BTreeMap key order: pollutantID 2 before 3.
        assert_eq!(out[0].pollutant_id, Some(2));
        assert_eq!(out[0].emission_quant, Some(3.0), "1.0 + 2.0");
        assert_eq!(out[1].pollutant_id, Some(3));
        assert_eq!(out[1].emission_quant, Some(4.0));
    }

    #[test]
    fn non_key_dimensions_collapse_to_null() {
        // Year + Nation drops month/day/hour, all geography, and every
        // breakdown dimension. yearID + pollutantID stay.
        let b = breakdown_all_false();
        let plan = emission_aggregation(&inputs(
            OutputTimestep::Year,
            GeographicOutputDetail::Nation,
            &[Model::Onroad],
            &b,
        ));
        let out = aggregate_emissions(&plan, &[emission(2, 7, Some(5.0))], &UnitScaling).unwrap();
        let row = &out[0];

        // Surviving keys.
        assert_eq!(row.moves_run_id, 1);
        assert_eq!(row.iteration_id, Some(1));
        assert_eq!(row.year_id, Some(2020));
        assert_eq!(row.pollutant_id, Some(2));
        // Collapsed dimensions.
        assert_eq!(row.month_id, None);
        assert_eq!(row.day_id, None);
        assert_eq!(row.hour_id, None);
        assert_eq!(row.county_id, None);
        assert_eq!(row.source_type_id, None);
        assert_eq!(row.scc, None);
        // emissionRate is always dropped; runHash always flows through.
        assert_eq!(row.emission_rate, None);
        assert_eq!(row.run_hash, "run-hash");
    }

    #[test]
    fn month_timestep_keeps_month_key() {
        // Month timestep keeps monthID, so rows in different months stay
        // distinct even though everything else aggregates away.
        let b = breakdown_all_false();
        let plan = emission_aggregation(&inputs(
            OutputTimestep::Month,
            GeographicOutputDetail::Nation,
            &[Model::Onroad],
            &b,
        ));
        let rows = vec![
            emission(2, 1, Some(1.0)),
            emission(2, 1, Some(2.0)),
            emission(2, 3, Some(8.0)),
        ];
        let out = aggregate_emissions(&plan, &rows, &UnitScaling).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].month_id, Some(1));
        assert_eq!(out[0].emission_quant, Some(3.0));
        assert_eq!(out[1].month_id, Some(3));
        assert_eq!(out[1].emission_quant, Some(8.0));
    }

    #[test]
    fn sum_skips_null_metric_values() {
        let b = breakdown_all_false();
        let plan = emission_aggregation(&inputs(
            OutputTimestep::Year,
            GeographicOutputDetail::Nation,
            &[Model::Onroad],
            &b,
        ));
        // One NULL row in the group — SQL SUM ignores it.
        let rows = vec![
            emission(2, 1, Some(10.0)),
            emission(2, 1, None),
            emission(2, 1, Some(5.0)),
        ];
        let out = aggregate_emissions(&plan, &rows, &UnitScaling).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].emission_quant, Some(15.0));
    }

    #[test]
    fn all_null_metric_group_yields_null() {
        let b = breakdown_all_false();
        let plan = emission_aggregation(&inputs(
            OutputTimestep::Year,
            GeographicOutputDetail::Nation,
            &[Model::Onroad],
            &b,
        ));
        let rows = vec![emission(2, 1, None), emission(2, 1, None)];
        let out = aggregate_emissions(&plan, &rows, &UnitScaling).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].emission_quant, None,
            "SUM over all-NULL group is NULL, not 0.0"
        );
    }

    #[test]
    fn temporal_scaling_multiplies_each_row_before_summing() {
        // Month timestep on an onroad run carries WeeksPerMonth scaling.
        let b = breakdown_all_false();
        let plan = emission_aggregation(&inputs(
            OutputTimestep::Month,
            GeographicOutputDetail::Nation,
            &[Model::Onroad],
            &b,
        ));
        assert_eq!(
            plan.sum_columns(),
            vec![("emissionQuant", TemporalScaling::WeeksPerMonth)]
        );
        // MonthAsFactor: factor == monthID. Both rows are month 3, so each
        // metric is tripled: (1.0 + 2.0) * 3 = 9.0.
        let rows = vec![emission(2, 3, Some(1.0)), emission(2, 3, Some(2.0))];
        let out = aggregate_emissions(&plan, &rows, &MonthAsFactor).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].emission_quant, Some(9.0));
    }

    #[test]
    fn temporal_scaling_uses_each_rows_own_month_factor() {
        // Year timestep collapses monthID, so rows from different months
        // land in one group — each must still be scaled by its OWN month's
        // factor before the sum, not by a single group-wide factor.
        let b = breakdown_all_false();
        let plan = emission_aggregation(&inputs(
            OutputTimestep::Year,
            GeographicOutputDetail::Nation,
            &[Model::Onroad],
            &b,
        ));
        assert_eq!(
            plan.sum_columns(),
            vec![("emissionQuant", TemporalScaling::WeeksPerMonth)]
        );
        // MonthAsFactor: factor == monthID. 2.0 * 1 + 3.0 * 7 = 23.0.
        let rows = vec![emission(2, 1, Some(2.0)), emission(2, 7, Some(3.0))];
        let out = aggregate_emissions(&plan, &rows, &MonthAsFactor).unwrap();
        assert_eq!(out.len(), 1, "Year collapses both months into one row");
        assert_eq!(out[0].emission_quant, Some(2.0 * 1.0 + 3.0 * 7.0));
        assert_eq!(out[0].month_id, None);
    }

    #[test]
    fn scc_text_key_groups_distinctly() {
        // onroad_scc keeps the SCC text column as a group key.
        let mut b = breakdown_all_false();
        b.onroad_scc = true;
        let plan = emission_aggregation(&inputs(
            OutputTimestep::Year,
            GeographicOutputDetail::Nation,
            &[Model::Onroad],
            &b,
        ));
        let mut a = emission(2, 1, Some(1.0));
        a.scc = Some("AAA".to_string());
        let mut a2 = emission(2, 1, Some(3.0));
        a2.scc = Some("AAA".to_string());
        let mut c = emission(2, 1, Some(7.0));
        c.scc = Some("ZZZ".to_string());

        let out = aggregate_emissions(&plan, &[c, a, a2], &UnitScaling).unwrap();
        assert_eq!(out.len(), 2);
        // Group-key sort: "AAA" before "ZZZ".
        assert_eq!(out[0].scc.as_deref(), Some("AAA"));
        assert_eq!(out[0].emission_quant, Some(4.0));
        assert_eq!(out[1].scc.as_deref(), Some("ZZZ"));
        assert_eq!(out[1].emission_quant, Some(7.0));
    }

    #[test]
    fn output_order_is_independent_of_input_order() {
        let mut b = breakdown_all_false();
        b.emission_process = true;
        let plan = emission_aggregation(&inputs(
            OutputTimestep::Month,
            GeographicOutputDetail::Nation,
            &[Model::Onroad],
            &b,
        ));
        // Powers of two so the sum is exact regardless of fold order.
        let forward = vec![
            emission(2, 1, Some(1.0)),
            emission(2, 2, Some(2.0)),
            emission(3, 1, Some(4.0)),
            emission(3, 2, Some(8.0)),
        ];
        let mut reversed = forward.clone();
        reversed.reverse();

        let a = aggregate_emissions(&plan, &forward, &UnitScaling).unwrap();
        let b = aggregate_emissions(&plan, &reversed, &UnitScaling).unwrap();
        assert_eq!(a, b, "aggregation output must not depend on input order");
        assert_eq!(a.len(), 4);
    }

    #[test]
    fn activity_aggregation_sums_activity_and_keeps_type() {
        let b = breakdown_all_false();
        let plan = activity_aggregation(&inputs(
            OutputTimestep::Year,
            GeographicOutputDetail::Nation,
            &[Model::Onroad],
            &b,
        ));
        assert_eq!(plan.table, AggregationTable::Activity);
        // activityTypeID is always a key — types stay separated.
        let rows = vec![
            activity(1, 1, Some(100.0)),
            activity(1, 7, Some(200.0)),
            activity(2, 1, Some(50.0)),
        ];
        let out = aggregate_activity(&plan, &rows, &UnitScaling).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].activity_type_id, Some(1));
        assert_eq!(out[0].activity, Some(300.0));
        assert_eq!(out[1].activity_type_id, Some(2));
        assert_eq!(out[1].activity, Some(50.0));
        // Geography collapsed away.
        assert_eq!(out[0].county_id, None);
    }

    #[test]
    fn empty_input_yields_empty_output() {
        let b = breakdown_all_false();
        let plan = emission_aggregation(&inputs(
            OutputTimestep::Year,
            GeographicOutputDetail::Nation,
            &[Model::Onroad],
            &b,
        ));
        assert!(aggregate_emissions(&plan, &[], &UnitScaling)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn aggregate_emissions_rejects_non_emission_plan() {
        let b = breakdown_all_false();
        let activity_plan = activity_aggregation(&inputs(
            OutputTimestep::Year,
            GeographicOutputDetail::Nation,
            &[Model::Onroad],
            &b,
        ));
        let err = aggregate_emissions(&activity_plan, &[], &UnitScaling).unwrap_err();
        assert!(
            matches!(err, Error::AggregationPlanMismatch(_)),
            "got {err:?}"
        );
    }

    #[test]
    fn aggregate_activity_rejects_non_activity_plan() {
        let b = breakdown_all_false();
        let emission_plan = emission_aggregation(&inputs(
            OutputTimestep::Year,
            GeographicOutputDetail::Nation,
            &[Model::Onroad],
            &b,
        ));
        let err = aggregate_activity(&emission_plan, &[], &UnitScaling).unwrap_err();
        assert!(
            matches!(err, Error::AggregationPlanMismatch(_)),
            "got {err:?}"
        );
    }

    #[test]
    fn plan_with_wrong_sum_column_is_rejected() {
        // Hand-build an Emission-tabled plan whose SUM names the wrong
        // metric — the writer must refuse it rather than silently sum the
        // wrong column.
        let bad = AggregationPlan {
            table: AggregationTable::Emission,
            columns: vec![
                AggregationColumn::Key("MOVESRunID"),
                AggregationColumn::Sum {
                    column: "activity",
                    scaling: TemporalScaling::None,
                },
            ],
            needs_nonroad_activity_weight: false,
        };
        let err = aggregate_emissions(&bad, &[], &UnitScaling).unwrap_err();
        assert!(
            matches!(err, Error::AggregationPlanMismatch(_)),
            "got {err:?}"
        );
    }

    #[test]
    fn unknown_group_by_column_is_rejected() {
        // A plan whose group-by names a column absent from the MOVESOutput
        // schema must fail loudly rather than panic.
        let bad = AggregationPlan {
            table: AggregationTable::Emission,
            columns: vec![
                AggregationColumn::Key("not_a_real_column"),
                AggregationColumn::Sum {
                    column: "emissionQuant",
                    scaling: TemporalScaling::None,
                },
            ],
            needs_nonroad_activity_weight: false,
        };
        let err =
            aggregate_emissions(&bad, &[emission(2, 1, Some(1.0))], &UnitScaling).unwrap_err();
        assert!(
            matches!(err, Error::AggregationPlanMismatch(_)),
            "got {err:?}"
        );
    }
}

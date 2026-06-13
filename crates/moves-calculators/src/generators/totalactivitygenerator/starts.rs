//! Port of `TotalActivityGenerator.adjustStarts` / `database/AdjustStarts.sql`
//! — populates the execution-DB `Starts` table for non-project (inventory)
//! runs.
//!
//! The canonical procedure computes, per `(sourceType, day)`, a daily start
//! count and allocates it across age, month and hour:
//!
//! ```text
//! starts = startsPerDay
//!        * combinedAgeEffectFraction   -- age distribution × normalised age adjustment
//!        * monthAdjustment             -- StartsMonthAdjust
//!        * hourAllocationFraction      -- StartsHourFraction.allocationFraction
//!        * startAllocFactor            -- Zone.startAllocFactor (default 1)
//!        * noOfRealDays                -- DayOfAnyWeek (weekday days count as 5, etc.)
//! ```
//!
//! where, with no user-imported `StartsPerDay`/`Starts`,
//!
//! ```text
//! startsPerDay                = sourceTypePopulation * startsPerDayPerVehicle
//! normalizedAgeAdjustment     = ageAdjustment / sum(ageAdjustment over ages)        -- per sourceType
//! combinedAgeEffectFraction   = ageFraction * normalizedAgeAdjustment / sumproduct  -- per sourceType
//! sumproduct                  = sum(ageFraction * normalizedAgeAdjustment over ages) -- per sourceType
//! ```
//!
//! The default DB ships an empty `StartsPerDay`/`Starts` (no user input), so
//! only the calculated branch is ported; the user-import passthrough is a
//! no-op here. `dayID` is part of the data (`StartsPerDayPerVehicle.dayID` /
//! `StartsHourFraction.dayID`), so the canonical `spAdjustStarts(0/2/5)` day
//! loop is replaced by iterating every `(sourceType, day)` cell present.

use std::collections::HashMap;

use moves_framework::{Error, TableRow};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

use super::inputs::{DayOfAnyWeekRow, SourceTypeAgeDistributionRow, SourceTypeYearRow};

fn row_err_starts(table: &'static str, row: usize, col: &'static str, msg: String) -> Error {
    Error::RowExtraction {
        table: table.to_string(),
        row,
        column: col.to_string(),
        message: msg,
    }
}

/// One `StartsPerDayPerVehicle` row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StartsPerDayPerVehicleRow {
    /// `dayID`.
    pub day_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `startsPerDayPerVehicle`.
    pub starts_per_day_per_vehicle: f64,
}

/// One `startsAgeAdjustment` row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StartsAgeAdjustmentRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `ageID`.
    pub age_id: i32,
    /// `ageAdjustment`.
    pub age_adjustment: f64,
}

/// One `startsMonthAdjust` row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StartsMonthAdjustRow {
    /// `monthID`.
    pub month_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `monthAdjustment`.
    pub month_adjustment: f64,
}

/// One `startsHourFraction` row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StartsHourFractionRow {
    /// `dayID`.
    pub day_id: i32,
    /// `hourID`.
    pub hour_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `allocationFraction`.
    pub allocation_fraction: f64,
}

/// One `Zone` row — only `zoneID` and the `startAllocFactor` `AdjustStarts`
/// reads (`ifnull(z.startAllocFactor, 1)`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StartsZoneRow {
    /// `zoneID`.
    pub zone_id: i32,
    /// `startAllocFactor`.
    pub start_alloc_factor: f64,
}

/// One `Starts` row — the activity table the start calculators read.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StartsRow {
    /// `hourDayID` — `hourID * 10 + dayID`.
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
    /// `starts`.
    pub starts: f64,
}

/// Inputs to [`adjust_starts`].
#[derive(Debug, Clone)]
pub struct AdjustStartsInputs {
    /// The run's analysis year (`##yearID##`).
    pub analysis_year: i32,
    /// The current zone (`##zoneID##`).
    pub zone_id: i32,
    /// `SourceTypeYear` — supplies `sourceTypePopulation` for the run year.
    pub source_type_year: Vec<SourceTypeYearRow>,
    /// `SourceTypeAgeDistribution` — supplies `ageFraction` for the run year.
    pub source_type_age_distribution: Vec<SourceTypeAgeDistributionRow>,
    /// `DayOfAnyWeek` — supplies `noOfRealDays`.
    pub day_of_any_week: Vec<DayOfAnyWeekRow>,
    /// `StartsPerDayPerVehicle`.
    pub starts_per_day_per_vehicle: Vec<StartsPerDayPerVehicleRow>,
    /// `startsAgeAdjustment`.
    pub starts_age_adjustment: Vec<StartsAgeAdjustmentRow>,
    /// `startsMonthAdjust`.
    pub starts_month_adjust: Vec<StartsMonthAdjustRow>,
    /// `startsHourFraction`.
    pub starts_hour_fraction: Vec<StartsHourFractionRow>,
    /// `Zone` rows — `startAllocFactor` lookup (`ifnull(…, 1)`).
    pub zone: Vec<StartsZoneRow>,
}

/// Compute the `Starts` activity table per `AdjustStarts.sql`.
#[must_use]
pub fn adjust_starts(inputs: &AdjustStartsInputs) -> Vec<StartsRow> {
    let year = inputs.analysis_year;

    // population[sourceType] from SourceTypeYear at the run year.
    let population: HashMap<i32, f64> = inputs
        .source_type_year
        .iter()
        .filter(|r| r.year_id == year)
        .map(|r| (r.source_type_id, r.source_type_population))
        .collect();

    // startsPerDay[(sourceType, day)] = population * startsPerDayPerVehicle.
    // Carries the day so the hour-fraction join can match on it.
    let mut starts_per_day: Vec<(i32, i32, f64)> = Vec::new(); // (sourceType, day, startsPerDay)
    for spdpv in &inputs.starts_per_day_per_vehicle {
        if let Some(&pop) = population.get(&spdpv.source_type_id) {
            starts_per_day.push((
                spdpv.source_type_id,
                spdpv.day_id,
                pop * spdpv.starts_per_day_per_vehicle,
            ));
        }
    }

    // normalizedAgeAdjustment[(sourceType, age)] = ageAdjustment / sum(ageAdjustment).
    let mut total_age_adjust: HashMap<i32, f64> = HashMap::new();
    for r in &inputs.starts_age_adjustment {
        *total_age_adjust.entry(r.source_type_id).or_insert(0.0) += r.age_adjustment;
    }
    let normalized_age_adjust: HashMap<(i32, i32), f64> = inputs
        .starts_age_adjustment
        .iter()
        .filter_map(|r| {
            let total = *total_age_adjust.get(&r.source_type_id)?;
            (total != 0.0).then(|| ((r.source_type_id, r.age_id), r.age_adjustment / total))
        })
        .collect();

    // sumproduct[sourceType] = sum(ageFraction * normalizedAgeAdjustment), inner
    // join SourceTypeAgeDistribution (run year) × normalizedAgeAdjustment.
    let age_dist: Vec<&SourceTypeAgeDistributionRow> = inputs
        .source_type_age_distribution
        .iter()
        .filter(|r| r.year_id == year)
        .collect();
    let mut sumproduct: HashMap<i32, f64> = HashMap::new();
    for r in &age_dist {
        if let Some(&naa) = normalized_age_adjust.get(&(r.source_type_id, r.age_id)) {
            *sumproduct.entry(r.source_type_id).or_insert(0.0) += r.age_fraction * naa;
        }
    }

    // combinedAgeEffectFraction[sourceType] -> [(age, fraction)].
    let mut combined_age_effect: HashMap<i32, Vec<(i32, f64)>> = HashMap::new();
    for r in &age_dist {
        let (Some(&naa), Some(&sp)) = (
            normalized_age_adjust.get(&(r.source_type_id, r.age_id)),
            sumproduct.get(&r.source_type_id),
        ) else {
            continue;
        };
        if sp == 0.0 {
            continue;
        }
        combined_age_effect
            .entry(r.source_type_id)
            .or_default()
            .push((r.age_id, r.age_fraction * naa / sp));
    }

    // month adjust[sourceType] -> [(month, adjustment)].
    let mut month_adjust: HashMap<i32, Vec<(i32, f64)>> = HashMap::new();
    for r in &inputs.starts_month_adjust {
        month_adjust
            .entry(r.source_type_id)
            .or_default()
            .push((r.month_id, r.month_adjustment));
    }

    // hour fraction[(sourceType, day)] -> [(hour, allocationFraction)].
    let mut hour_fraction: HashMap<(i32, i32), Vec<(i32, f64)>> = HashMap::new();
    for r in &inputs.starts_hour_fraction {
        hour_fraction
            .entry((r.source_type_id, r.day_id))
            .or_default()
            .push((r.hour_id, r.allocation_fraction));
    }

    let no_of_real_days: HashMap<i32, f64> = inputs
        .day_of_any_week
        .iter()
        .map(|r| (r.day_id, r.no_of_real_days))
        .collect();

    // startAllocFactor for the current zone (ifnull(…, 1)).
    let start_alloc_factor = inputs
        .zone
        .iter()
        .find(|z| z.zone_id == inputs.zone_id)
        .map_or(1.0, |z| z.start_alloc_factor);

    let mut out: Vec<StartsRow> = Vec::new();
    for &(source_type_id, day_id, starts_per_day_val) in &starts_per_day {
        let (Some(months), Some(hours), Some(ages)) = (
            month_adjust.get(&source_type_id),
            hour_fraction.get(&(source_type_id, day_id)),
            combined_age_effect.get(&source_type_id),
        ) else {
            continue;
        };
        // The final canonical `update` multiplies by noOfRealDays for the day.
        let real_days = no_of_real_days.get(&day_id).copied().unwrap_or(1.0);

        for &(month_id, month_adjustment) in months {
            for &(hour_id, allocation_fraction) in hours {
                for &(age_id, combined_age_effect_fraction) in ages {
                    let starts = starts_per_day_val
                        * combined_age_effect_fraction
                        * month_adjustment
                        * allocation_fraction
                        * start_alloc_factor
                        * real_days;
                    out.push(StartsRow {
                        hour_day_id: hour_id * 10 + day_id,
                        month_id,
                        year_id: year,
                        age_id,
                        zone_id: inputs.zone_id,
                        source_type_id,
                        starts,
                    });
                }
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// TableRow impls — input readers + the Starts output.
// ---------------------------------------------------------------------------

macro_rules! get_f64 {
    ($df:expr, $t:expr, $col:literal) => {
        $df.column($col)
            .map_err(|e| row_err_starts($t, 0, $col, e.to_string()))?
            .cast(&DataType::Float64)
            .map_err(|e| row_err_starts($t, 0, $col, e.to_string()))?
            .f64()
            .map_err(|e| row_err_starts($t, 0, $col, e.to_string()))?
            .clone()
    };
}
macro_rules! get_i32 {
    ($df:expr, $t:expr, $col:literal) => {
        $df.column($col)
            .map_err(|e| row_err_starts($t, 0, $col, e.to_string()))?
            .cast(&DataType::Int32)
            .map_err(|e| row_err_starts($t, 0, $col, e.to_string()))?
            .i32()
            .map_err(|e| row_err_starts($t, 0, $col, e.to_string()))?
            .clone()
    };
}

impl TableRow for StartsPerDayPerVehicleRow {
    fn table_name() -> &'static str {
        "StartsPerDayPerVehicle"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("dayID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("startsPerDayPerVehicle".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "dayID".into(),
                    rows.iter().map(|r| r.day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "startsPerDayPerVehicle".into(),
                    rows.iter()
                        .map(|r| r.starts_per_day_per_vehicle)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "StartsPerDayPerVehicle";
        let day = get_i32!(df, t, "dayID");
        let st = get_i32!(df, t, "sourceTypeID");
        let spdpv = get_f64!(df, t, "startsPerDayPerVehicle");
        (0..df.height())
            .map(|i| {
                let null = |c: &'static str| row_err_starts(t, i, c, "null value".into());
                Ok(StartsPerDayPerVehicleRow {
                    day_id: day.get(i).ok_or_else(|| null("dayID"))?,
                    source_type_id: st.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    starts_per_day_per_vehicle: spdpv
                        .get(i)
                        .ok_or_else(|| null("startsPerDayPerVehicle"))?,
                })
            })
            .collect()
    }
}

impl TableRow for StartsAgeAdjustmentRow {
    fn table_name() -> &'static str {
        "startsAgeAdjustment"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("ageAdjustment".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageAdjustment".into(),
                    rows.iter().map(|r| r.age_adjustment).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "startsAgeAdjustment";
        let st = get_i32!(df, t, "sourceTypeID");
        let age = get_i32!(df, t, "ageID");
        let adj = get_f64!(df, t, "ageAdjustment");
        (0..df.height())
            .map(|i| {
                let null = |c: &'static str| row_err_starts(t, i, c, "null value".into());
                Ok(StartsAgeAdjustmentRow {
                    source_type_id: st.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    age_id: age.get(i).ok_or_else(|| null("ageID"))?,
                    age_adjustment: adj.get(i).ok_or_else(|| null("ageAdjustment"))?,
                })
            })
            .collect()
    }
}

impl TableRow for StartsMonthAdjustRow {
    fn table_name() -> &'static str {
        "startsMonthAdjust"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("monthID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("monthAdjustment".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthAdjustment".into(),
                    rows.iter()
                        .map(|r| r.month_adjustment)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "startsMonthAdjust";
        let month = get_i32!(df, t, "monthID");
        let st = get_i32!(df, t, "sourceTypeID");
        let adj = get_f64!(df, t, "monthAdjustment");
        (0..df.height())
            .map(|i| {
                let null = |c: &'static str| row_err_starts(t, i, c, "null value".into());
                Ok(StartsMonthAdjustRow {
                    month_id: month.get(i).ok_or_else(|| null("monthID"))?,
                    source_type_id: st.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    month_adjustment: adj.get(i).ok_or_else(|| null("monthAdjustment"))?,
                })
            })
            .collect()
    }
}

impl TableRow for StartsHourFractionRow {
    fn table_name() -> &'static str {
        "startsHourFraction"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("dayID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("allocationFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "dayID".into(),
                    rows.iter().map(|r| r.day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourID".into(),
                    rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "allocationFraction".into(),
                    rows.iter()
                        .map(|r| r.allocation_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "startsHourFraction";
        let day = get_i32!(df, t, "dayID");
        let hour = get_i32!(df, t, "hourID");
        let st = get_i32!(df, t, "sourceTypeID");
        let frac = get_f64!(df, t, "allocationFraction");
        (0..df.height())
            .map(|i| {
                let null = |c: &'static str| row_err_starts(t, i, c, "null value".into());
                Ok(StartsHourFractionRow {
                    day_id: day.get(i).ok_or_else(|| null("dayID"))?,
                    hour_id: hour.get(i).ok_or_else(|| null("hourID"))?,
                    source_type_id: st.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    allocation_fraction: frac.get(i).ok_or_else(|| null("allocationFraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for StartsZoneRow {
    fn table_name() -> &'static str {
        "Zone"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("zoneID".into(), DataType::Int32),
            ("startAllocFactor".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "zoneID".into(),
                    rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "startAllocFactor".into(),
                    rows.iter()
                        .map(|r| r.start_alloc_factor)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "Zone";
        let zone = get_i32!(df, t, "zoneID");
        let saf = get_f64!(df, t, "startAllocFactor");
        (0..df.height())
            .map(|i| {
                let null = |c: &'static str| row_err_starts(t, i, c, "null value".into());
                Ok(StartsZoneRow {
                    zone_id: zone.get(i).ok_or_else(|| null("zoneID"))?,
                    start_alloc_factor: saf.get(i).ok_or_else(|| null("startAllocFactor"))?,
                })
            })
            .collect()
    }
}

impl TableRow for StartsRow {
    fn table_name() -> &'static str {
        "Starts"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("zoneID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("starts".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "hourDayID".into(),
                    rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "zoneID".into(),
                    rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "starts".into(),
                    rows.iter().map(|r| r.starts).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "Starts";
        let hd = get_i32!(df, t, "hourDayID");
        let month = get_i32!(df, t, "monthID");
        let year = get_i32!(df, t, "yearID");
        let age = get_i32!(df, t, "ageID");
        let zone = get_i32!(df, t, "zoneID");
        let st = get_i32!(df, t, "sourceTypeID");
        let starts = get_f64!(df, t, "starts");
        (0..df.height())
            .map(|i| {
                let null = |c: &'static str| row_err_starts(t, i, c, "null value".into());
                Ok(StartsRow {
                    hour_day_id: hd.get(i).ok_or_else(|| null("hourDayID"))?,
                    month_id: month.get(i).ok_or_else(|| null("monthID"))?,
                    year_id: year.get(i).ok_or_else(|| null("yearID"))?,
                    age_id: age.get(i).ok_or_else(|| null("ageID"))?,
                    zone_id: zone.get(i).ok_or_else(|| null("zoneID"))?,
                    source_type_id: st.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    starts: starts.get(i).ok_or_else(|| null("starts"))?,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // One source type (21), one day (5), two ages (0,1), two months (1,2),
    // two hours (6,7). Hand-computed end to end.
    fn inputs() -> AdjustStartsInputs {
        AdjustStartsInputs {
            analysis_year: 2020,
            zone_id: 100,
            // population = 1000.
            source_type_year: vec![SourceTypeYearRow {
                year_id: 2020,
                source_type_id: 21,
                source_type_population: 1000.0,
                migration_rate: 1.0,
                sales_growth_factor: 1.0,
            }],
            // ageFraction: age0=0.6, age1=0.4.
            source_type_age_distribution: vec![
                SourceTypeAgeDistributionRow {
                    source_type_id: 21,
                    year_id: 2020,
                    age_id: 0,
                    age_fraction: 0.6,
                },
                SourceTypeAgeDistributionRow {
                    source_type_id: 21,
                    year_id: 2020,
                    age_id: 1,
                    age_fraction: 0.4,
                },
            ],
            // weekday day 5 counts as 5 real days.
            day_of_any_week: vec![DayOfAnyWeekRow {
                day_id: 5,
                no_of_real_days: 5.0,
            }],
            // startsPerDayPerVehicle = 2 → startsPerDay = 1000 * 2 = 2000.
            starts_per_day_per_vehicle: vec![StartsPerDayPerVehicleRow {
                day_id: 5,
                source_type_id: 21,
                starts_per_day_per_vehicle: 2.0,
            }],
            // ageAdjustment: age0=3, age1=1 → total=4 → normalized: 0.75, 0.25.
            starts_age_adjustment: vec![
                StartsAgeAdjustmentRow {
                    source_type_id: 21,
                    age_id: 0,
                    age_adjustment: 3.0,
                },
                StartsAgeAdjustmentRow {
                    source_type_id: 21,
                    age_id: 1,
                    age_adjustment: 1.0,
                },
            ],
            // monthAdjust: m1=1.0, m2=0.5.
            starts_month_adjust: vec![
                StartsMonthAdjustRow {
                    month_id: 1,
                    source_type_id: 21,
                    month_adjustment: 1.0,
                },
                StartsMonthAdjustRow {
                    month_id: 2,
                    source_type_id: 21,
                    month_adjustment: 0.5,
                },
            ],
            // hourFraction: h6=0.3, h7=0.7 (day 5).
            starts_hour_fraction: vec![
                StartsHourFractionRow {
                    day_id: 5,
                    hour_id: 6,
                    source_type_id: 21,
                    allocation_fraction: 0.3,
                },
                StartsHourFractionRow {
                    day_id: 5,
                    hour_id: 7,
                    source_type_id: 21,
                    allocation_fraction: 0.7,
                },
            ],
            // startAllocFactor = 2.0 for zone 100.
            zone: vec![StartsZoneRow {
                zone_id: 100,
                start_alloc_factor: 2.0,
            }],
        }
    }

    #[test]
    fn adjust_starts_matches_hand_computed_allocation() {
        let out = adjust_starts(&inputs());
        // 2 months × 2 hours × 2 ages = 8 rows.
        assert_eq!(out.len(), 8);

        // combinedAgeEffectFraction:
        //   normalizedAgeAdjust: age0=0.75, age1=0.25.
        //   sumproduct = 0.6*0.75 + 0.4*0.25 = 0.45 + 0.10 = 0.55.
        //   caef age0 = 0.6*0.75/0.55 = 0.45/0.55; age1 = 0.4*0.25/0.55 = 0.10/0.55.
        let caef0 = 0.45 / 0.55;
        let caef1 = 0.10 / 0.55;
        // starts = startsPerDay(2000) * caef * monthAdj * hourFrac * startAlloc(2) * realDays(5).
        let base = 2000.0 * 2.0 * 5.0;
        // month1 (1.0), hour6 (0.3), age0 → hourDayID = 6*10+5 = 65.
        let r = out
            .iter()
            .find(|r| r.hour_day_id == 65 && r.month_id == 1 && r.age_id == 0)
            .unwrap();
        assert!((r.starts - base * caef0 * 1.0 * 0.3).abs() < 1e-9);
        assert_eq!(r.zone_id, 100);
        assert_eq!(r.year_id, 2020);
        assert_eq!(r.source_type_id, 21);
        // month2 (0.5), hour7 (0.7), age1 → hourDayID = 75.
        let r = out
            .iter()
            .find(|r| r.hour_day_id == 75 && r.month_id == 2 && r.age_id == 1)
            .unwrap();
        assert!((r.starts - base * caef1 * 0.5 * 0.7).abs() < 1e-9);

        // The combinedAgeEffectFraction sums to 1 per source type, so summing
        // over ages for a fixed (month, hour) gives startsPerDay×monthAdj×
        // hourFrac×startAlloc×realDays (no age split).
        let sum_age: f64 = out
            .iter()
            .filter(|r| r.hour_day_id == 65 && r.month_id == 1)
            .map(|r| r.starts)
            .sum();
        assert!((sum_age - base * 1.0 * 0.3).abs() < 1e-9);
    }

    #[test]
    fn missing_zone_defaults_start_alloc_factor_to_one() {
        let mut inp = inputs();
        inp.zone.clear(); // no zone row → ifnull(…, 1)
        let out = adjust_starts(&inp);
        let base = 2000.0 * 1.0 * 5.0; // startAlloc defaults to 1
        let caef0 = 0.45 / 0.55;
        let r = out
            .iter()
            .find(|r| r.hour_day_id == 65 && r.month_id == 1 && r.age_id == 0)
            .unwrap();
        assert!((r.starts - base * caef0 * 1.0 * 0.3).abs() < 1e-9);
    }

    #[test]
    fn no_population_for_source_type_yields_no_rows() {
        let mut inp = inputs();
        inp.source_type_year.clear(); // no population → startsPerDay empty
        assert!(adjust_starts(&inp).is_empty());
    }
}

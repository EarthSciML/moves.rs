//! `TankTemperatureGenerator` — soak-mode tank temperatures and activity
//! fractions (migration-plan Task 38).
//!
//! Ports `gov/epa/otaq/moves/master/implementation/ghg/TankTemperatureGenerator.java`
//! (2,645 lines). The Java generator "builds average vehicle fuel tank
//! temperature and soak mode activity fraction records": for each execution
//! `zone` it derives, from county meteorology and the sample-vehicle trip
//! activity, the hourly tank-temperature trajectories and the cold-/hot-soak
//! activity split that downstream evaporative calculators read.
//!
//! # Java structure
//!
//! `TankTemperatureGenerator` subscribes to the master loop at `ZONE`
//! granularity, `GENERATOR` priority, for the *Evap Permeation* (process 11),
//! *Evap Fuel Vapor Venting* (process 12) and *Evap Fuel Leaks* (process 13)
//! processes. Its `executeLoop` runs a fixed sequence of scripted SQL/Java
//! steps the source labels TTG-1 … TTG-7. [`generate_tank_temperatures`]
//! ports that sequence:
//!
//! | Java step                                            | Rust |
//! |------------------------------------------------------|------|
//! | TTG-1 `calculateColdSoakTankTemperature`             | [`calculate_cold_soak_tank_temperature`] |
//! | `flagMarkerTrips` + TTG-2 `createSampleVehicleTripByHour` | `flag_and_split_trips` |
//! | TTG-3 `createHotSoakEventByHour`                     | `build_hot_soak_events` |
//! | TTG-4 `calculateHotSoakAndOperatingTankTemperatures` | `calculate_operating_and_hot_soak_temperatures` |
//! | TTG-5 `calculateAverageTankTemperature`              | `calculate_average_tank_temperature` |
//! | `buildTTGeMinutes` + TTG-6 `calculateSoakActivityFraction` | `calculate_soak_activity_fraction` |
//! | TTG-7 `calculateColdSoakInitialHourFractions`        | `calculate_cold_soak_initial_hour_fractions` |
//!
//! # SQL tables → typed inputs
//!
//! Each `inner join` in the Java SQL becomes an explicit Rust join over the
//! typed table-row slices on [`TankTemperatureInputs`]. The Java step 100
//! `TempVehAndTTG` temp table (`SampleVehicleDay ⋈ SourceTypeModelYearGroup`)
//! and the per-zone `TempColdSoakTankTemperature` are reconstructed in memory.
//! The two RunSpec *filter* tables (`RunSpecMonth`, `RunSpecHourDay`) —
//! single-column selection sets MOVES materialises in the execution database —
//! become the `runspec_*_ids` id-list fields.
//!
//! The Java cursor machinery in TTG-7 (`openSvthCursor` / `openHotSoakCursor`
//! and the `SVTHCursor.txt` / `HotSoakCursor.txt` temp files) is a
//! memory-footprint workaround; this port groups the same rows with in-memory
//! maps, so the file I/O is dropped while the grouping it performs —
//! `min(keyOnTime)`, `sum(keyOffTime − keyOnTime)`, hot-soak `min`/`count` —
//! is preserved exactly.
//!
//! # Numeric precision
//!
//! MOVES performs TTG-1b's quarter-hour tank-temperature recurrence and
//! TTG-4's operating / hot-soak temperature updates in Java 32-bit `float`;
//! the `ColdSoakTankTemperature`, `OperatingTemperature` and
//! `HotSoakTemperature` columns those land in are mixed `FLOAT` / `DOUBLE`.
//! This port runs every step in `f64`, matching the established Phase 3
//! choice (see `TankFuelGenerator`, Task 39): the `f32`→`f64` widening of the
//! recurrences is a harmless numerical artifact bounded well within the
//! Phase 3 tolerance budget (`characterization/tolerance.toml`). Task 44
//! generator-integration validation decides, against canonical captures,
//! whether any step must instead be made bit-compatible.
//!
//! # Data-plane deferral
//!
//! The framework data plane (`ExecutionTables` / `ScratchNamespace`, Task 50
//! `DataFrameStore`) is still a placeholder, so [`TankTemperatureGenerator`]'s
//! `Generator::execute` returns an empty output — the established Phase 2/3
//! pattern. The ported computation lives in the pure
//! [`generate_tank_temperatures`] function and is fully exercised by the
//! crate tests; Task 50 wiring will read the input tables from the
//! [`CalculatorContext`], call it per `zone`, and write the four output
//! tables into the scratch namespace.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use moves_calculator_info::{Granularity, Priority};
use moves_data::ProcessId;
use moves_framework::{
    CalculatorContext, CalculatorOutput, CalculatorSubscription, Error, Generator,
};

/// Tank-temperature smoothing coefficient — the `1.4` multiplier applied to
/// the running temperature-delta sum in TTG-1b and TTG-4b.
const TANK_TEMP_SMOOTHING: f64 = 1.4;

/// Upper bound (°F) on operating tank temperature — the EPA cap applied to
/// `keyOffTemp` in TTG-4a.
const MAX_OPERATING_TANK_TEMP: f64 = 140.0;

/// Op-mode for a hot-soaking vehicle — every `HotSoakTemperature` minute and
/// the TTG-5 hot-soak `AverageTankTemperature` aggregate.
const OP_MODE_HOT_SOAKING: i32 = 150;

/// Op-mode for a cold-soaking (parked) vehicle — the TTG-5 cold-soak
/// `AverageTankTemperature` aggregate; also the TTG-4b loop terminator.
const OP_MODE_COLD_SOAKING: i32 = 151;

/// Op-mode for the "all running" TTG-5 `AverageTankTemperature` aggregate.
const OP_MODE_RUNNING: i32 = 300;

/// Sentinel `keyOnTemp` meaning "the prior trip's hot soak ended cold" — set
/// by TTG-4b when `opModeID` drops to cold soaking. A TTG-4a trip seeing this
/// starts from `coldSoakTankTemperature` instead.
const HOT_SOAK_ENDED_SENTINEL: f64 = -1000.0;

// =============================================================================
//   Input table rows
// =============================================================================

/// One `ZoneMonthHour` row — hourly ambient temperature for a zone.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ZoneMonthHourRow {
    /// `zoneID`.
    pub zone_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `hourID` — 1‥=24.
    pub hour_id: i32,
    /// `temperature` — hourly ambient temperature (°F).
    pub temperature: f64,
}

/// One `HourDay` row — maps a packed `hourDayID` to its hour and day.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HourDayRow {
    /// `hourDayID` — the packed `hourID`/`dayID` key.
    pub hour_day_id: i32,
    /// `hourID` — 1‥=24.
    pub hour_id: i32,
    /// `dayID`.
    pub day_id: i32,
}

/// One `SampleVehicleDay` row — a sample vehicle's source type and day.
/// `vehID` is unique within the table (the Java relies on this).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SampleVehicleDayRow {
    /// `vehID`.
    pub veh_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `dayID`.
    pub day_id: i32,
}

/// One `SampleVehicleTrip` row — a sample drive-cycle trip.
///
/// A *marker* trip — both `keyOnTime` and `priorTripID` absent — is a sentinel
/// MOVES inserts between days; `flagMarkerTrips` drops it and re-flags the
/// trips that referenced it as first-of-day trips.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SampleVehicleTripRow {
    /// `vehID`.
    pub veh_id: i32,
    /// `dayID`.
    pub day_id: i32,
    /// `tripID`.
    pub trip_id: i32,
    /// `hourID` — the hour the trip starts.
    pub hour_id: i32,
    /// `priorTripID` — `None` for a trip with no recorded predecessor.
    pub prior_trip_id: Option<i32>,
    /// `keyOnTime` — start minute-of-day; `None` for a marker trip or a trip
    /// continued without a key-on event.
    pub key_on_time: Option<i32>,
    /// `keyOffTime` — end minute-of-day.
    pub key_off_time: i32,
}

/// One `SourceTypeModelYearGroup` row — only the two columns step 100 reads,
/// mapping a source type to a tank-temperature group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceTypeModelYearGroupRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `tankTemperatureGroupID`.
    pub tank_temperature_group_id: i32,
}

/// One `TankTemperatureRise` row — the per-group operating-temperature rise
/// coefficients TTG-4a applies.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TankTemperatureRiseRow {
    /// `tankTemperatureGroupID`.
    pub tank_temperature_group_id: i32,
    /// `tankTemperatureRiseTermA`.
    pub tank_temperature_rise_term_a: f64,
    /// `tankTemperatureRiseTermB`.
    pub tank_temperature_rise_term_b: f64,
}

// =============================================================================
//   Output table rows
// =============================================================================

/// One `ColdSoakTankTemperature` row — TTG-1's hourly parked-vehicle tank
/// temperature for a zone.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ColdSoakTankTemperatureRow {
    /// `zoneID`.
    pub zone_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `hourID` — 1‥=24.
    pub hour_id: i32,
    /// `coldSoakTankTemperature` — the hourly cold-soak tank temperature (°F).
    pub cold_soak_tank_temperature: f64,
}

/// One `AverageTankTemperature` row — TTG-5's per-`opModeID` average tank
/// temperature, keyed by `(zone, month, hour-day, tank-temperature group)`.
///
/// The same type carries pre-existing **user input** rows on
/// [`TankTemperatureInputs::prior_average_tank_temperature`]: a row with
/// `is_user_input` true blocks every generated row sharing its
/// `(tank-temperature group, zone, month)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AverageTankTemperatureRow {
    /// `zoneID`.
    pub zone_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `tankTemperatureGroupID`.
    pub tank_temperature_group_id: i32,
    /// `opModeID` — 150 hot soaking, 151 cold soaking, 300 all running.
    pub op_mode_id: i32,
    /// `averageTankTemperature` (°F).
    pub average_tank_temperature: f64,
    /// `isUserInput` — `false` (`'N'`) for generated rows.
    pub is_user_input: bool,
}

/// One `SoakActivityFraction` row — TTG-6's per-`opModeID` fraction of
/// soak-eligible minutes, keyed by `(source type, zone, month, hour-day)`.
///
/// A row with `is_user_input` true on
/// [`TankTemperatureInputs::prior_soak_activity_fraction`] blocks every
/// generated row sharing its `(source type, zone, month)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SoakActivityFractionRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `zoneID`.
    pub zone_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `opModeID` — 150 hot soaking, 151 cold soaking.
    pub op_mode_id: i32,
    /// `soakActivityFraction`.
    pub soak_activity_fraction: f64,
    /// `isUserInput` — `false` (`'N'`) for generated rows.
    pub is_user_input: bool,
}

/// One `ColdSoakInitialHourFraction` row — TTG-7's fraction of a vehicle's
/// cold-soak minutes attributable to soaking that began in each prior hour.
///
/// A row with `is_user_input` true on
/// [`TankTemperatureInputs::prior_cold_soak_initial_hour_fraction`] blocks
/// every generated row sharing its `(source type, zone, month)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ColdSoakInitialHourFractionRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `zoneID`.
    pub zone_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `initialHourDayID` — the hour-day the cold soak began in.
    pub initial_hour_day_id: i32,
    /// `coldSoakInitialHourFraction`.
    pub cold_soak_initial_hour_fraction: f64,
    /// `isUserInput` — `false` (`'N'`) for generated rows.
    pub is_user_input: bool,
}

// =============================================================================
//   Inputs / outputs
// =============================================================================

/// The slice of the MOVES execution database `TankTemperatureGenerator` reads.
///
/// Holds whole tables; [`generate_tank_temperatures`] filters to one `zone`
/// internally, so the same value is reused across every `zone` of a run.
#[derive(Debug, Clone, Default)]
pub struct TankTemperatureInputs {
    /// `ZoneMonthHour`.
    pub zone_month_hour: Vec<ZoneMonthHourRow>,
    /// `HourDay`.
    pub hour_day: Vec<HourDayRow>,
    /// `SampleVehicleDay`.
    pub sample_vehicle_day: Vec<SampleVehicleDayRow>,
    /// `SampleVehicleTrip`.
    pub sample_vehicle_trip: Vec<SampleVehicleTripRow>,
    /// `SourceTypeModelYearGroup`.
    pub source_type_model_year_group: Vec<SourceTypeModelYearGroupRow>,
    /// `TankTemperatureRise`.
    pub tank_temperature_rise: Vec<TankTemperatureRiseRow>,
    /// `TankTemperatureGroup` — the full set of tank-temperature group ids.
    pub tank_temperature_group_ids: Vec<i32>,
    /// `RunSpecMonth` — months selected by the RunSpec.
    pub runspec_month_ids: Vec<i32>,
    /// `RunSpecHourDay` — hour-days selected by the RunSpec.
    pub runspec_hour_day_ids: Vec<i32>,
    /// Pre-existing user-input `AverageTankTemperature` rows.
    pub prior_average_tank_temperature: Vec<AverageTankTemperatureRow>,
    /// Pre-existing user-input `SoakActivityFraction` rows.
    pub prior_soak_activity_fraction: Vec<SoakActivityFractionRow>,
    /// Pre-existing user-input `ColdSoakInitialHourFraction` rows.
    pub prior_cold_soak_initial_hour_fraction: Vec<ColdSoakInitialHourFractionRow>,
}

/// The four execution-database tables `TankTemperatureGenerator` produces for
/// one `zone`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TankTemperatureOutput {
    /// `ColdSoakTankTemperature` (TTG-1).
    pub cold_soak_tank_temperature: Vec<ColdSoakTankTemperatureRow>,
    /// `AverageTankTemperature` (TTG-5).
    pub average_tank_temperature: Vec<AverageTankTemperatureRow>,
    /// `SoakActivityFraction` (TTG-6).
    pub soak_activity_fraction: Vec<SoakActivityFractionRow>,
    /// `ColdSoakInitialHourFraction` (TTG-7).
    pub cold_soak_initial_hour_fraction: Vec<ColdSoakInitialHourFractionRow>,
}

// =============================================================================
//   Intermediate (temp) tables
// =============================================================================

/// One `SampleVehicleTripByHour` row — a trip segment confined to a single
/// hour, produced by TTG-2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SvthRow {
    veh_id: i32,
    trip_id: i32,
    day_id: i32,
    hour_day_id: i32,
    hour_id: i32,
    /// Resolved `priorTripID`: `0` for a first-of-day trip.
    prior_trip_id: i32,
    key_on_time: i32,
    key_off_time: i32,
    end_of_hour: i32,
    start_of_trip: bool,
    end_of_trip: bool,
}

/// One `HotSoakEventByHour` row — an hour-confined slice of the hot soak that
/// follows a trip, produced by TTG-3. `day_id` is not a Java column; it is
/// `HourDay[hour_day_id].day_id`, retained so TTG-4b can key by trip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HotSoakEventRow {
    veh_id: i32,
    trip_id: i32,
    day_id: i32,
    hour_day_id: i32,
    end_of_hour: i32,
    hot_soak_begin: i32,
    hot_soak_end: i32,
    start_of_hot_soak: bool,
    end_of_hot_soak: bool,
}

/// One `OperatingTemperature` row — the tank temperature across a trip
/// segment, produced by TTG-4a.
#[derive(Debug, Clone, Copy, PartialEq)]
struct OperatingTemperatureRow {
    month_id: i32,
    tank_temperature_group_id: i32,
    veh_id: i32,
    trip_id: i32,
    hour_day_id: i32,
    prior_trip_id: i32,
    key_on_time: i32,
    key_off_time: i32,
    end_of_hour: i32,
    start_of_trip: bool,
    end_of_trip: bool,
    key_on_temp: f64,
    key_off_temp: f64,
}

/// One `HotSoakTemperature` row — a minute of post-trip hot soaking, produced
/// by TTG-4b.
#[derive(Debug, Clone, Copy, PartialEq)]
struct HotSoakTemperatureRow {
    month_id: i32,
    tank_temperature_group_id: i32,
    veh_id: i32,
    trip_id: i32,
    hour_day_id: i32,
    hot_soak_time: i32,
    initial_tank_temp: f64,
    soak_tank_temp: f64,
    temp_delta: f64,
    temperature: f64,
    cold_soak_temp: f64,
    op_mode_id: i32,
}

// =============================================================================
//   TTG-1 — cold soak tank temperature
// =============================================================================

/// TTG-1 — compute the hourly cold-soak (parked-vehicle) tank temperature for
/// one zone.
///
/// Ports `calculateColdSoakTankTemperature`. The Java step has three parts:
///
/// * **TTG-1a** interpolates the hourly `ZoneMonthHour.temperature` to
///   quarter-hour resolution — for output hour `h` and quarter-hour step
///   `ts ∈ 1‥=4`,
///   `qhTemp = temp[h] + (ts−1)·0.25·(temp[next(h)] − temp[h])`,
///   where `next(24) = 1`. An hour produces output only when both it and its
///   successor have a temperature (the Java self-join requires both).
/// * **TTG-1b** runs a smoothing recurrence over the quarter-hour series, per
///   month, in `(hour, step)` order: with `firstQHTankTemp = qhTemp[h=1,ts=1]`
///   and `sumTempDelta` reset to 0 per month,
///   `qhTankTemp = 1.4·sumTempDelta + firstQHTankTemp`,
///   `tempDelta = qhTemp − qhTankTemp`, then `sumTempDelta += tempDelta`.
/// * **TTG-1c** keeps the `ts = 1` value as `coldSoakTankTemperature`.
///
/// A month with no `(hour 1, step 1)` quarter-hour temperature produces no
/// rows — the Java `firstQuarterHourTankTemperature` query excludes it.
/// Output is sorted by `(zone, month, hour)`.
#[must_use]
pub fn calculate_cold_soak_tank_temperature(
    zone_month_hour: &[ZoneMonthHourRow],
    zone_id: i32,
) -> Vec<ColdSoakTankTemperatureRow> {
    // (monthID, hourID) -> temperature, for this zone.
    let mut temp: BTreeMap<(i32, i32), f64> = BTreeMap::new();
    for zmh in zone_month_hour {
        if zmh.zone_id == zone_id {
            temp.insert((zmh.month_id, zmh.hour_id), zmh.temperature);
        }
    }
    let months: BTreeSet<i32> = temp.keys().map(|&(m, _)| m).collect();

    let mut output: Vec<ColdSoakTankTemperatureRow> = Vec::new();
    for month in months {
        // TTG-1a: quarter-hour temperatures, keyed (hourID, timeStepID).
        let mut quarter_hour_temp: BTreeMap<(i32, i32), f64> = BTreeMap::new();
        for hour in 1..=24 {
            let next_hour = if hour == 24 { 1 } else { hour + 1 };
            let (Some(&t_here), Some(&t_next)) =
                (temp.get(&(month, hour)), temp.get(&(month, next_hour)))
            else {
                continue;
            };
            for time_step in 1..=4 {
                let q = t_here + f64::from(time_step - 1) * 0.25 * (t_next - t_here);
                quarter_hour_temp.insert((hour, time_step), q);
            }
        }
        // TTG-1b: the smoothing recurrence. With no (hour 1, step 1) entry the
        // Java skips the month entirely.
        let Some(&first_quarter_hour_tank_temp) = quarter_hour_temp.get(&(1, 1)) else {
            continue;
        };
        let mut sum_temp_delta = 0.0_f64;
        // The BTreeMap iterates (hour, step)-ascending — the Java
        // `ORDER BY hourID, timeStepID`.
        for (&(hour, time_step), &quarter_hour_temperature) in &quarter_hour_temp {
            let quarter_hour_tank_temp =
                TANK_TEMP_SMOOTHING * sum_temp_delta + first_quarter_hour_tank_temp;
            let temp_delta = quarter_hour_temperature - quarter_hour_tank_temp;
            sum_temp_delta += temp_delta;
            // TTG-1c: the step-1 value is the cold-soak tank temperature.
            if time_step == 1 {
                output.push(ColdSoakTankTemperatureRow {
                    zone_id,
                    month_id: month,
                    hour_id: hour,
                    cold_soak_tank_temperature: quarter_hour_tank_temp,
                });
            }
        }
    }
    output.sort_by_key(|r| (r.zone_id, r.month_id, r.hour_id));
    output
}

// =============================================================================
//   flagMarkerTrips + TTG-2 — split trips into per-hour segments
// =============================================================================

/// `flagMarkerTrips` + TTG-2 — classify the sample trips and split each
/// non-marker trip into the hours it spans.
///
/// `flagMarkerTrips` finds *marker* trips (`keyOnTime` and `priorTripID` both
/// absent) and re-flags as first-of-day any trip whose predecessor is a marker
/// or that simply has no predecessor; a first-of-day trip's resolved
/// `priorTripID` becomes `0`. Marker trips themselves are excluded from TTG-2.
///
/// TTG-2 (`createSampleVehicleTripByHour`) then walks each surviving trip
/// hour by hour from `keyOnTime` to `keyOffTime`: the first segment is flagged
/// `startOfTrip`, the segment containing `keyOffTime` is flagged `endOfTrip`,
/// and `endOfHour = (⌊keyOnTime / 60⌋ + 1)·60` advances by 60 each hour. A
/// segment whose `(hour, day)` has no `HourDay` row is dropped, matching the
/// Java `INSERT … SELECT … FROM HourDay`.
fn flag_and_split_trips(trips: &[SampleVehicleTripRow], hour_day: &[HourDayRow]) -> Vec<SvthRow> {
    let hour_day_id: HashMap<(i32, i32), i32> = hour_day
        .iter()
        .map(|hd| ((hd.hour_id, hd.day_id), hd.hour_day_id))
        .collect();

    // flagMarkerTrips: a marker trip has neither a keyOnTime nor a priorTripID.
    let marker_trips: HashSet<(i32, i32, i32)> = trips
        .iter()
        .filter(|t| t.key_on_time.is_none() && t.prior_trip_id.is_none())
        .map(|t| (t.veh_id, t.day_id, t.trip_id))
        .collect();

    // TTG-2 processes non-marker trips in (vehID, dayID, tripID, keyOnTime)
    // order.
    let mut sorted: Vec<&SampleVehicleTripRow> = trips
        .iter()
        .filter(|t| !marker_trips.contains(&(t.veh_id, t.day_id, t.trip_id)))
        .collect();
    sorted.sort_by_key(|t| (t.veh_id, t.day_id, t.trip_id, t.key_on_time.unwrap_or(0)));

    let mut svth: Vec<SvthRow> = Vec::new();
    for &t in &sorted {
        // Resolve priorTripID. A marker predecessor, or no predecessor at all,
        // marks a first-of-day trip — flagMarkerTrips sets its priorTripID to
        // NULL, which `getInt` reads back as 0.
        let prior_is_marker = t
            .prior_trip_id
            .is_some_and(|p| marker_trips.contains(&(t.veh_id, t.day_id, p)));
        let effective_prior = match t.prior_trip_id {
            Some(p) if !prior_is_marker => p,
            _ => 0,
        };

        let key_on_time = t.key_on_time.unwrap_or(0);
        let key_off_time = t.key_off_time;
        let mut hour = t.hour_id;
        let mut end_of_hour = (key_on_time / 60 + 1) * 60;

        let mut emit = |hour: i32, k_on: i32, k_off: i32, eoh: i32, sot: bool, eot: bool| {
            if let Some(&hd_id) = hour_day_id.get(&(hour, t.day_id)) {
                svth.push(SvthRow {
                    veh_id: t.veh_id,
                    trip_id: t.trip_id,
                    day_id: t.day_id,
                    hour_day_id: hd_id,
                    hour_id: hour,
                    prior_trip_id: effective_prior,
                    key_on_time: k_on,
                    key_off_time: k_off,
                    end_of_hour: eoh,
                    start_of_trip: sot,
                    end_of_trip: eot,
                });
            }
        };

        if key_off_time <= end_of_hour {
            // The whole trip fits within its starting hour.
            emit(hour, key_on_time, key_off_time, end_of_hour, true, true);
        } else {
            emit(hour, key_on_time, end_of_hour, end_of_hour, true, false);
            let mut cur_key_on;
            let mut done = false;
            while !done {
                hour = if hour == 24 { 1 } else { hour + 1 };
                cur_key_on = end_of_hour + 1;
                end_of_hour += 60;
                let new_key_off = if key_off_time > end_of_hour {
                    end_of_hour
                } else {
                    done = true;
                    key_off_time
                };
                emit(hour, cur_key_on, new_key_off, end_of_hour, false, done);
            }
        }
    }
    svth
}

// =============================================================================
//   TTG-3 — hot soak events
// =============================================================================

/// TTG-3 — derive, for every trip-ending segment, the hour-confined slices of
/// the hot soak that follows the trip.
///
/// Ports `createHotSoakEventByHour`. For each `endOfTrip` segment the hot soak
/// runs from `keyOffTime` to the first segment of the *next* trip (the trip
/// whose resolved `priorTripID` is this trip's id); with no next trip it runs
/// to the end of the day. The soak is split at hour boundaries the same way
/// TTG-2 splits trips. A slice whose `(hour, day)` has no `HourDay` row is
/// dropped.
fn build_hot_soak_events(svth: &[SvthRow], hour_day: &[HourDayRow]) -> Vec<HotSoakEventRow> {
    let hour_day_id: HashMap<(i32, i32), i32> = hour_day
        .iter()
        .map(|hd| ((hd.hour_id, hd.day_id), hd.hour_day_id))
        .collect();

    // keyOnTime of the first segment of the trip that follows the keyed trip.
    let mut next_trip_key_on: HashMap<(i32, i32, i32), i32> = HashMap::new();
    for r in svth {
        if r.start_of_trip {
            next_trip_key_on
                .entry((r.veh_id, r.day_id, r.prior_trip_id))
                .or_insert(r.key_on_time);
        }
    }

    // TTG-3 walks the trip-ending segments in (vehID, dayID, tripID) order.
    let mut enders: Vec<&SvthRow> = svth.iter().filter(|r| r.end_of_trip).collect();
    enders.sort_by_key(|r| (r.veh_id, r.day_id, r.trip_id));

    let mut events: Vec<HotSoakEventRow> = Vec::new();
    for r in enders {
        let mut emit = |hour: i32, eoh: i32, begin: i32, end: i32, soh: bool, end_flag: bool| {
            if let Some(&hd_id) = hour_day_id.get(&(hour, r.day_id)) {
                events.push(HotSoakEventRow {
                    veh_id: r.veh_id,
                    trip_id: r.trip_id,
                    day_id: r.day_id,
                    hour_day_id: hd_id,
                    end_of_hour: eoh,
                    hot_soak_begin: begin,
                    hot_soak_end: end,
                    start_of_hot_soak: soh,
                    end_of_hot_soak: end_flag,
                });
            }
        };

        let mut hour = r.hour_id;
        let mut end_of_hour = r.end_of_hour;
        let mut hot_soak_begin = r.key_off_time;

        // The next trip bounds the soak; with none, it runs to end of day.
        let next_trip = next_trip_key_on
            .get(&(r.veh_id, r.day_id, r.trip_id))
            .copied();
        let run_to_end_of_day = next_trip.is_none();
        let mut next_key_on_time = next_trip.unwrap_or(end_of_hour);
        let mut end_of_hot_soak = next_trip.is_some_and(|k| k < end_of_hour);
        let mut hot_soak_end = if end_of_hot_soak {
            next_key_on_time
        } else {
            end_of_hour
        };
        emit(
            hour,
            end_of_hour,
            hot_soak_begin,
            hot_soak_end,
            true,
            end_of_hot_soak,
        );

        while !end_of_hot_soak {
            if run_to_end_of_day {
                if hour == 24 {
                    break;
                }
                next_key_on_time = end_of_hour + 60 + 1;
            }
            hour = if hour == 24 { 1 } else { hour + 1 };
            hot_soak_begin = end_of_hour + 1;
            end_of_hour += 60;
            if next_key_on_time > end_of_hour {
                hot_soak_end = end_of_hour;
            } else {
                hot_soak_end = next_key_on_time;
                end_of_hot_soak = true;
            }
            emit(
                hour,
                end_of_hour,
                hot_soak_begin,
                hot_soak_end,
                false,
                end_of_hot_soak,
            );
        }
    }
    events
}

// =============================================================================
//   TTG-4 — operating and hot-soak tank temperatures
// =============================================================================

/// One `SampleVehicleTripByHour` segment expanded across the TTG-4a join with
/// `TempVehAndTTG`, `TankTemperatureRise` and `TempColdSoakTankTemperature`.
#[derive(Debug, Clone, Copy)]
struct OperatingInput {
    trip_id: i32,
    hour_day_id: i32,
    day_id: i32,
    veh_id: i32,
    prior_trip_id: i32,
    key_on_time: i32,
    key_off_time: i32,
    end_of_hour: i32,
    start_of_trip: bool,
    end_of_trip: bool,
    month_id: i32,
    cold_soak_tank_temperature: f64,
    tank_temperature_group_id: i32,
    term_a: f64,
    term_b: f64,
}

/// Identifies one parsed trip — the key TTG-4b's hot-soak calculation runs for.
#[derive(Debug, Clone, Copy)]
struct ParsedTrip {
    month_id: i32,
    tank_temperature_group_id: i32,
    veh_id: i32,
    day_id: i32,
    trip_id: i32,
}

/// A TTG-4a work-queue entry — a request to process the trips that follow
/// `prior_trip_id`, carrying the hot-soak end temperature as the starting
/// `keyOnTemp`. The seed entry has `prior_trip_id = 0` (process all first
/// trips).
#[derive(Debug, Clone, Copy)]
struct WorkItem {
    month_id: i32,
    tank_temperature_group_id: i32,
    veh_id: i32,
    day_id: i32,
    prior_trip_id: i32,
    key_on_temp: f64,
}

/// The per-zone lookup tables TTG-4b's hot-soak calculation reads.
struct Ttg4Tables<'a> {
    /// `(monthID, hourID)` → `coldSoakTankTemperature`.
    cstt: &'a HashMap<(i32, i32), f64>,
    /// `hourDayID` → `hourID`.
    hour_of_hour_day: HashMap<i32, i32>,
    /// `(monthID, hourID)` → ambient `temperature`, for the active zone.
    zone_temp: HashMap<(i32, i32), f64>,
    /// `(vehID, dayID, tripID)` → hot-soak events, sorted by `hotSoakBegin`.
    events_by_trip: HashMap<(i32, i32, i32), Vec<HotSoakEventRow>>,
}

/// TTG-4b — generate the minute-by-minute hot-soak temperatures following one
/// trip, returning the temperature the soak ended at.
///
/// Ports `calculateHotSoakTemperaturesForTrip`. With `tempDeltaSum` starting
/// at 0 and `initialTankTemperature = keyOffTemp`, each soak minute computes
/// `soakTankTemperature = 1.4·tempDeltaSum / 60 + initialTankTemperature`,
/// `tempDelta = temperature − soakTankTemperature`, then
/// `tempDeltaSum += tempDelta`. The soak stays hot while
/// `soakTankTemperature > coldSoakTankTemperature + 3`; the first minute that
/// falls to or below it ends the soak — that minute is *not* recorded and the
/// return value is the [`HOT_SOAK_ENDED_SENTINEL`]. With no soak event the
/// passed `keyOffTemp` is returned unchanged.
fn hot_soak_for_trip(
    out: &mut Vec<HotSoakTemperatureRow>,
    tables: &Ttg4Tables,
    trip: &ParsedTrip,
    key_off_temp: f64,
) -> f64 {
    let mut soak_tank_temperature = key_off_temp;
    let Some(events) = tables
        .events_by_trip
        .get(&(trip.veh_id, trip.day_id, trip.trip_id))
    else {
        return soak_tank_temperature;
    };
    let initial_tank_temperature = key_off_temp;
    let mut temp_delta_sum = 0.0_f64;
    let mut done = false;
    for event in events {
        if done {
            break;
        }
        // Ambient temperature and cold-soak temperature for the event's hour.
        let Some(&hour) = tables.hour_of_hour_day.get(&event.hour_day_id) else {
            continue;
        };
        let (Some(&temperature), Some(&cold_soak)) = (
            tables.zone_temp.get(&(trip.month_id, hour)),
            tables.cstt.get(&(trip.month_id, hour)),
        ) else {
            continue;
        };
        for minute in event.hot_soak_begin..event.hot_soak_end {
            soak_tank_temperature =
                TANK_TEMP_SMOOTHING * temp_delta_sum / 60.0 + initial_tank_temperature;
            let temp_delta = temperature - soak_tank_temperature;
            temp_delta_sum += temp_delta;
            let op_mode_id = if soak_tank_temperature > cold_soak + 3.0 {
                OP_MODE_HOT_SOAKING
            } else {
                OP_MODE_COLD_SOAKING
            };
            if op_mode_id == OP_MODE_COLD_SOAKING {
                // The soak has cooled to cold soaking — stop without
                // recording this minute or any that would follow.
                soak_tank_temperature = HOT_SOAK_ENDED_SENTINEL;
                done = true;
                break;
            }
            out.push(HotSoakTemperatureRow {
                month_id: trip.month_id,
                tank_temperature_group_id: trip.tank_temperature_group_id,
                veh_id: trip.veh_id,
                trip_id: trip.trip_id,
                hour_day_id: event.hour_day_id,
                hot_soak_time: minute,
                initial_tank_temp: initial_tank_temperature,
                soak_tank_temp: soak_tank_temperature,
                temp_delta,
                temperature,
                cold_soak_temp: cold_soak,
                op_mode_id: OP_MODE_HOT_SOAKING,
            });
        }
    }
    soak_tank_temperature
}

/// TTG-4 — compute the operating and hot-soak tank temperatures for one zone.
///
/// Ports `calculateHotSoakAndOperatingTankTemperatures` (TTG-4a) together with
/// its `calculateHotSoakTemperaturesForTrip` callee ([`hot_soak_for_trip`],
/// TTG-4b). TTG-4a processes trips through a work queue: the seed processes
/// every first-of-day trip, and each trip, as its last segment is reached,
/// enqueues its successor with the hot-soak end temperature as the starting
/// `keyOnTemp`. Within a trip the operating temperature rises by
/// `keyOffTemp = keyOnTemp + ((termA + termB·(95 − keyOnTemp)) / 1.2)·((keyOffTime − keyOnTime) / 60)`,
/// capped at [`MAX_OPERATING_TANK_TEMP`]; a segment continued into a new hour
/// starts from the previous segment's `keyOffTemp`.
fn calculate_operating_and_hot_soak_temperatures(
    inputs: &TankTemperatureInputs,
    svth: &[SvthRow],
    events: &[HotSoakEventRow],
    cstt: &HashMap<(i32, i32), f64>,
    zone_id: i32,
) -> (Vec<OperatingTemperatureRow>, Vec<HotSoakTemperatureRow>) {
    // vehID → sourceTypeID.
    let veh_source: HashMap<i32, i32> = inputs
        .sample_vehicle_day
        .iter()
        .map(|sd| (sd.veh_id, sd.source_type_id))
        .collect();
    // sourceTypeID → tankTemperatureGroupIDs (the `TempVehAndTTG` join).
    let mut source_ttgs: HashMap<i32, BTreeSet<i32>> = HashMap::new();
    for g in &inputs.source_type_model_year_group {
        source_ttgs
            .entry(g.source_type_id)
            .or_default()
            .insert(g.tank_temperature_group_id);
    }
    // tankTemperatureGroupID → (termA, termB); a group with no rise row drops
    // out of the TTG-4a inner join.
    let rise: HashMap<i32, (f64, f64)> = inputs
        .tank_temperature_rise
        .iter()
        .map(|r| {
            (
                r.tank_temperature_group_id,
                (
                    r.tank_temperature_rise_term_a,
                    r.tank_temperature_rise_term_b,
                ),
            )
        })
        .collect();
    // hourID → [(monthID, coldSoak)] — the `svth.hourID = cstt.hourID` join.
    let mut cstt_by_hour: HashMap<i32, Vec<(i32, f64)>> = HashMap::new();
    for (&(month, hour), &cold_soak) in cstt {
        cstt_by_hour
            .entry(hour)
            .or_default()
            .push((month, cold_soak));
    }
    for months in cstt_by_hour.values_mut() {
        months.sort_by_key(|&(m, _)| m);
    }

    // Expand the TTG-4a join: every SVTH segment × its vehicle's groups ×
    // every month sharing the segment's hour.
    let mut expanded: Vec<OperatingInput> = Vec::new();
    for r in svth {
        let Some(&source_type) = veh_source.get(&r.veh_id) else {
            continue;
        };
        let Some(ttgs) = source_ttgs.get(&source_type) else {
            continue;
        };
        let Some(months) = cstt_by_hour.get(&r.hour_id) else {
            continue;
        };
        for &ttg in ttgs {
            let Some(&(term_a, term_b)) = rise.get(&ttg) else {
                continue;
            };
            for &(month, cold_soak) in months {
                expanded.push(OperatingInput {
                    trip_id: r.trip_id,
                    hour_day_id: r.hour_day_id,
                    day_id: r.day_id,
                    veh_id: r.veh_id,
                    prior_trip_id: r.prior_trip_id,
                    key_on_time: r.key_on_time,
                    key_off_time: r.key_off_time,
                    end_of_hour: r.end_of_hour,
                    start_of_trip: r.start_of_trip,
                    end_of_trip: r.end_of_trip,
                    month_id: month,
                    cold_soak_tank_temperature: cold_soak,
                    tank_temperature_group_id: ttg,
                    term_a,
                    term_b,
                });
            }
        }
    }

    // First-of-day segments, ordered as the TTG-4a first-trip query is;
    // subsequent segments indexed by trip key, ordered by keyOnTime.
    let mut first_trip_rows: Vec<&OperatingInput> =
        expanded.iter().filter(|r| r.prior_trip_id == 0).collect();
    first_trip_rows.sort_by_key(|r| {
        (
            r.month_id,
            r.tank_temperature_group_id,
            r.veh_id,
            r.day_id,
            r.key_on_time,
        )
    });
    let mut by_prior: HashMap<(i32, i32, i32, i32, i32), Vec<&OperatingInput>> = HashMap::new();
    for r in &expanded {
        if r.prior_trip_id != 0 {
            by_prior
                .entry((
                    r.month_id,
                    r.tank_temperature_group_id,
                    r.veh_id,
                    r.day_id,
                    r.prior_trip_id,
                ))
                .or_default()
                .push(r);
        }
    }
    for rows in by_prior.values_mut() {
        rows.sort_by_key(|r| r.key_on_time);
    }

    let zone_temp: HashMap<(i32, i32), f64> = inputs
        .zone_month_hour
        .iter()
        .filter(|z| z.zone_id == zone_id)
        .map(|z| ((z.month_id, z.hour_id), z.temperature))
        .collect();
    let hour_of_hour_day: HashMap<i32, i32> = inputs
        .hour_day
        .iter()
        .map(|hd| (hd.hour_day_id, hd.hour_id))
        .collect();
    let mut events_by_trip: HashMap<(i32, i32, i32), Vec<HotSoakEventRow>> = HashMap::new();
    for e in events {
        events_by_trip
            .entry((e.veh_id, e.day_id, e.trip_id))
            .or_default()
            .push(*e);
    }
    for trip_events in events_by_trip.values_mut() {
        trip_events.sort_by_key(|e| e.hot_soak_begin);
    }
    let tables = Ttg4Tables {
        cstt,
        hour_of_hour_day,
        zone_temp,
        events_by_trip,
    };

    let mut operating: Vec<OperatingTemperatureRow> = Vec::new();
    let mut hot_soak: Vec<HotSoakTemperatureRow> = Vec::new();
    let mut queue: VecDeque<WorkItem> = VecDeque::new();
    queue.push_back(WorkItem {
        month_id: 0,
        tank_temperature_group_id: 0,
        veh_id: 0,
        day_id: 0,
        prior_trip_id: 0,
        key_on_temp: 0.0,
    });
    while let Some(p) = queue.pop_front() {
        let rows: Vec<&OperatingInput> = if p.prior_trip_id <= 0 {
            first_trip_rows.clone()
        } else {
            by_prior
                .get(&(
                    p.month_id,
                    p.tank_temperature_group_id,
                    p.veh_id,
                    p.day_id,
                    p.prior_trip_id,
                ))
                .cloned()
                .unwrap_or_default()
        };
        // The recurrence carries keyOffTemp across a trip's segments.
        let mut key_off_temp = p.key_on_temp;
        for r in rows {
            let key_on_temp = if r.start_of_trip {
                if p.prior_trip_id > 0 && p.key_on_temp > HOT_SOAK_ENDED_SENTINEL {
                    p.key_on_temp
                } else {
                    r.cold_soak_tank_temperature
                }
            } else {
                key_off_temp
            };
            let mut new_key_off_temp = key_on_temp
                + ((r.term_a + r.term_b * (95.0 - key_on_temp)) / 1.2)
                    * (f64::from(r.key_off_time - r.key_on_time) / 60.0);
            if new_key_off_temp > MAX_OPERATING_TANK_TEMP {
                new_key_off_temp = MAX_OPERATING_TANK_TEMP;
            }
            key_off_temp = new_key_off_temp;
            operating.push(OperatingTemperatureRow {
                month_id: r.month_id,
                tank_temperature_group_id: r.tank_temperature_group_id,
                veh_id: r.veh_id,
                trip_id: r.trip_id,
                hour_day_id: r.hour_day_id,
                prior_trip_id: r.prior_trip_id,
                key_on_time: r.key_on_time,
                key_off_time: r.key_off_time,
                end_of_hour: r.end_of_hour,
                start_of_trip: r.start_of_trip,
                end_of_trip: r.end_of_trip,
                key_on_temp,
                key_off_temp,
            });
            if r.end_of_trip {
                let trip = ParsedTrip {
                    month_id: r.month_id,
                    tank_temperature_group_id: r.tank_temperature_group_id,
                    veh_id: r.veh_id,
                    day_id: r.day_id,
                    trip_id: r.trip_id,
                };
                let end_hot_soak_temp =
                    hot_soak_for_trip(&mut hot_soak, &tables, &trip, key_off_temp);
                queue.push_back(WorkItem {
                    month_id: r.month_id,
                    tank_temperature_group_id: r.tank_temperature_group_id,
                    veh_id: r.veh_id,
                    day_id: r.day_id,
                    prior_trip_id: r.trip_id,
                    key_on_temp: end_hot_soak_temp,
                });
            }
        }
    }
    operating.sort_by_key(|r| {
        (
            r.month_id,
            r.tank_temperature_group_id,
            r.veh_id,
            r.trip_id,
            r.hour_day_id,
        )
    });
    hot_soak.sort_by_key(|r| {
        (
            r.month_id,
            r.tank_temperature_group_id,
            r.veh_id,
            r.trip_id,
            r.hour_day_id,
            r.hot_soak_time,
        )
    });
    (operating, hot_soak)
}

// =============================================================================
//   TTG-5 — average tank temperature
// =============================================================================

/// TTG-5 — aggregate the operating, hot-soak and cold-soak temperatures into
/// the per-`opModeID` `AverageTankTemperature`.
///
/// Ports `calculateAverageTankTemperature`'s three inserts, keyed by
/// `(month, hour-day, tank-temperature group)`:
///
/// * **opMode 151** (cold soaking) — the `coldSoakTankTemperature` of the
///   group's month and hour, for every group/hour-day appearing in
///   `OperatingTemperature`.
/// * **opMode 300** (all running) — the duration-weighted mean operating
///   temperature, `Σ((keyOffTime − keyOnTime)·(keyOnTemp + keyOffTemp) / 2) /
///   Σ(keyOffTime − keyOnTime)`.
/// * **opMode 150** (hot soaking) — the mean `soakTankTemp` over the group's
///   `HotSoakTemperature` minutes.
///
/// A `(group, month)` present as a user-input row on `prior` blocks all three
/// for that key. Output is sorted by
/// `(zone, month, hour-day, group, opModeID)`.
fn calculate_average_tank_temperature(
    operating: &[OperatingTemperatureRow],
    hot_soak: &[HotSoakTemperatureRow],
    cstt: &HashMap<(i32, i32), f64>,
    hour_day: &[HourDayRow],
    prior: &[AverageTankTemperatureRow],
    zone_id: i32,
) -> Vec<AverageTankTemperatureRow> {
    let hour_of_hour_day: HashMap<i32, i32> = hour_day
        .iter()
        .map(|hd| (hd.hour_day_id, hd.hour_id))
        .collect();
    let blocked: HashSet<(i32, i32)> = prior
        .iter()
        .filter(|r| r.is_user_input && r.zone_id == zone_id)
        .map(|r| (r.tank_temperature_group_id, r.month_id))
        .collect();

    let mut output: Vec<AverageTankTemperatureRow> = Vec::new();

    // opMode 151 — cold soaking.
    let summary: BTreeSet<(i32, i32, i32)> = operating
        .iter()
        .map(|r| (r.month_id, r.hour_day_id, r.tank_temperature_group_id))
        .collect();
    for (month_id, hour_day_id, ttg) in summary {
        if blocked.contains(&(ttg, month_id)) {
            continue;
        }
        let Some(&hour) = hour_of_hour_day.get(&hour_day_id) else {
            continue;
        };
        let Some(&cold_soak) = cstt.get(&(month_id, hour)) else {
            continue;
        };
        output.push(AverageTankTemperatureRow {
            zone_id,
            month_id,
            hour_day_id,
            tank_temperature_group_id: ttg,
            op_mode_id: OP_MODE_COLD_SOAKING,
            average_tank_temperature: cold_soak,
            is_user_input: false,
        });
    }

    // opMode 300 — all running.
    let mut running: BTreeMap<(i32, i32, i32), (f64, i32)> = BTreeMap::new();
    for r in operating {
        let weight = r.key_off_time - r.key_on_time;
        let entry = running
            .entry((r.month_id, r.hour_day_id, r.tank_temperature_group_id))
            .or_default();
        entry.0 += f64::from(weight) * (r.key_on_temp + r.key_off_temp) / 2.0;
        entry.1 += weight;
    }
    for ((month_id, hour_day_id, ttg), (weighted_sum, weight)) in running {
        // A zero total duration leaves the Java SUM/SUM as NULL — no row.
        if blocked.contains(&(ttg, month_id)) || weight == 0 {
            continue;
        }
        output.push(AverageTankTemperatureRow {
            zone_id,
            month_id,
            hour_day_id,
            tank_temperature_group_id: ttg,
            op_mode_id: OP_MODE_RUNNING,
            average_tank_temperature: weighted_sum / f64::from(weight),
            is_user_input: false,
        });
    }

    // opMode 150 — hot soaking.
    let mut soaking: BTreeMap<(i32, i32, i32), (f64, i32)> = BTreeMap::new();
    for r in hot_soak {
        let entry = soaking
            .entry((r.month_id, r.hour_day_id, r.tank_temperature_group_id))
            .or_default();
        entry.0 += r.soak_tank_temp;
        entry.1 += 1;
    }
    for ((month_id, hour_day_id, ttg), (sum, count)) in soaking {
        if blocked.contains(&(ttg, month_id)) {
            continue;
        }
        output.push(AverageTankTemperatureRow {
            zone_id,
            month_id,
            hour_day_id,
            tank_temperature_group_id: ttg,
            op_mode_id: OP_MODE_HOT_SOAKING,
            average_tank_temperature: sum / f64::from(count),
            is_user_input: false,
        });
    }

    output.sort_by_key(|r| {
        (
            r.zone_id,
            r.month_id,
            r.hour_day_id,
            r.tank_temperature_group_id,
            r.op_mode_id,
        )
    });
    output
}

// =============================================================================
//   buildTTGeMinutes + TTG-6 — soak activity fraction
// =============================================================================

/// TTG-6 — compute the per-`opModeID` soak activity fractions for one zone.
///
/// Ports `buildTTGeMinutes` and `calculateSoakActivityFraction`. Three minute
/// tallies per `(source type, month, hour-day)` drive it:
///
/// * `eMinutes` — soak-eligible minutes, `60 · vehicleCount`, for every
///   RunSpec-selected `(month, hour-day)` whose day matches the source type's
///   sampled day (`buildTTGeMinutes`).
/// * `oMinutes` — operating minutes, `Σ(keyOffTime − keyOnTime)` over the
///   trip segments.
/// * `hotSoakOpModeCount` — distinct `(vehID, hotSoakTime)` hot-soak minutes.
///
/// The fractions are `hotSoakOpModeCount / (eMinutes − oMinutes)` (opMode 150,
/// hot soaking) and `(eMinutes − oMinutes − hotSoakOpModeCount) /
/// (eMinutes − oMinutes)` (opMode 151, cold soaking). A `(source type, month)`
/// present as a user-input row on the inputs blocks generation. When
/// `eMinutes = oMinutes` the Java division yields SQL `NULL` and inserts no
/// usable row — the port emits none.
fn calculate_soak_activity_fraction(
    inputs: &TankTemperatureInputs,
    svth: &[SvthRow],
    hot_soak: &[HotSoakTemperatureRow],
    zone_id: i32,
) -> Vec<SoakActivityFractionRow> {
    // vehID → (sourceTypeID, dayID); vehID is unique in SampleVehicleDay.
    let veh_day: HashMap<i32, (i32, i32)> = inputs
        .sample_vehicle_day
        .iter()
        .map(|sd| (sd.veh_id, (sd.source_type_id, sd.day_id)))
        .collect();
    let day_of_hour_day: HashMap<i32, i32> = inputs
        .hour_day
        .iter()
        .map(|hd| (hd.hour_day_id, hd.day_id))
        .collect();

    // buildTTGeMinutes: eMinutes = 60 · vehicleCount.
    let mut vehicle_count: HashMap<(i32, i32), i32> = HashMap::new();
    for sd in &inputs.sample_vehicle_day {
        *vehicle_count
            .entry((sd.source_type_id, sd.day_id))
            .or_default() += 1;
    }
    let mut e_minutes: HashMap<(i32, i32, i32), i32> = HashMap::new();
    for (&(source_type, day), &count) in &vehicle_count {
        for &hour_day_id in &inputs.runspec_hour_day_ids {
            if day_of_hour_day.get(&hour_day_id) != Some(&day) {
                continue;
            }
            for &month in &inputs.runspec_month_ids {
                e_minutes.insert((source_type, month, hour_day_id), 60 * count);
            }
        }
    }

    // TTGoMinutes: oMinutes = Σ(keyOffTime − keyOnTime).
    let mut o_minutes: HashMap<(i32, i32, i32), i32> = HashMap::new();
    for r in svth {
        let Some(&(source_type, day)) = veh_day.get(&r.veh_id) else {
            continue;
        };
        if day != r.day_id {
            continue;
        }
        for &month in &inputs.runspec_month_ids {
            *o_minutes
                .entry((source_type, month, r.hour_day_id))
                .or_default() += r.key_off_time - r.key_on_time;
        }
    }

    // HotSoakOpModeCount: count(distinct vehID, hotSoakTime).
    let mut hot_soak_keys: HashMap<(i32, i32, i32), HashSet<(i32, i32)>> = HashMap::new();
    for r in hot_soak {
        let Some(&(source_type, _)) = veh_day.get(&r.veh_id) else {
            continue;
        };
        hot_soak_keys
            .entry((source_type, r.month_id, r.hour_day_id))
            .or_default()
            .insert((r.veh_id, r.hot_soak_time));
    }

    let blocked: HashSet<(i32, i32)> = inputs
        .prior_soak_activity_fraction
        .iter()
        .filter(|r| r.is_user_input && r.zone_id == zone_id)
        .map(|r| (r.source_type_id, r.month_id))
        .collect();

    let mut output: Vec<SoakActivityFractionRow> = Vec::new();

    // opMode 150 — hot soaking. eMinutes must exist (the Java inner join).
    for (&(source_type, month, hour_day_id), set) in &hot_soak_keys {
        let Some(&e) = e_minutes.get(&(source_type, month, hour_day_id)) else {
            continue;
        };
        if blocked.contains(&(source_type, month)) {
            continue;
        }
        let o = o_minutes
            .get(&(source_type, month, hour_day_id))
            .copied()
            .unwrap_or(0);
        let denominator = e - o;
        if denominator == 0 {
            continue;
        }
        let count = set.len() as i32;
        output.push(SoakActivityFractionRow {
            source_type_id: source_type,
            zone_id,
            month_id: month,
            hour_day_id,
            op_mode_id: OP_MODE_HOT_SOAKING,
            soak_activity_fraction: f64::from(count) / f64::from(denominator),
            is_user_input: false,
        });
    }

    // opMode 151 — cold soaking. Iterates every eMinutes key.
    for (&(source_type, month, hour_day_id), &e) in &e_minutes {
        if blocked.contains(&(source_type, month)) {
            continue;
        }
        let o = o_minutes
            .get(&(source_type, month, hour_day_id))
            .copied()
            .unwrap_or(0);
        let h = hot_soak_keys
            .get(&(source_type, month, hour_day_id))
            .map_or(0, |set| set.len() as i32);
        let denominator = e - o;
        if denominator == 0 {
            continue;
        }
        output.push(SoakActivityFractionRow {
            source_type_id: source_type,
            zone_id,
            month_id: month,
            hour_day_id,
            op_mode_id: OP_MODE_COLD_SOAKING,
            soak_activity_fraction: f64::from(e - o - h) / f64::from(denominator),
            is_user_input: false,
        });
    }

    output.sort_by_key(|r| {
        (
            r.source_type_id,
            r.zone_id,
            r.month_id,
            r.hour_day_id,
            r.op_mode_id,
        )
    });
    output
}

// =============================================================================
//   TTG-7 — cold soak initial hour fractions
// =============================================================================

/// TTG-7 — apportion each vehicle's cold-soak minutes to the hour the soak
/// began in.
///
/// Ports `calculateColdSoakInitialHourFractions`. For every
/// `(vehicle, hour-day, tank-temperature group, month)` it classifies the
/// hour's first activity record — a vehicle trip or a hot-soak minute — and
/// splits the 60 minutes of the hour into those still cold-soaking from an
/// earlier *initial* hour and those whose cold soak began in the current hour:
///
/// * no record — all 60 minutes carry over from the initial hour;
/// * a trip first — `keyOnTime mod 60` minutes carry over, the rest (less the
///   trip and hot-soak minutes) start in the current hour;
/// * a hot-soak minute first — none carry over, 60 minutes (less the hot-soak
///   and trip minutes) start in the current hour.
///
/// The minutes accumulate into `coldSoakMinutes[source][month][hour][initial]`
/// and the fraction is `coldSoakMinutes / Σ coldSoakMinutes`. The Java
/// file-backed cursors are replaced by in-memory grouping; a `(source, month)`
/// already user-supplied for the zone is skipped.
fn calculate_cold_soak_initial_hour_fractions(
    inputs: &TankTemperatureInputs,
    svth: &[SvthRow],
    hot_soak: &[HotSoakTemperatureRow],
    zone_id: i32,
) -> Vec<ColdSoakInitialHourFractionRow> {
    // --- buildArrays: lookup arrays and value→index maps ---
    let mut veh_list: Vec<(i32, i32, i32)> = inputs
        .sample_vehicle_day
        .iter()
        .map(|sd| (sd.veh_id, sd.source_type_id, sd.day_id))
        .collect();
    veh_list.sort_by_key(|&(veh, _, _)| veh);

    // TempVehAndTTG — the distinct (vehID, tankTemperatureGroupID) pairs.
    let mut source_ttgs: HashMap<i32, Vec<i32>> = HashMap::new();
    for g in &inputs.source_type_model_year_group {
        source_ttgs
            .entry(g.source_type_id)
            .or_default()
            .push(g.tank_temperature_group_id);
    }
    let veh_ttgs: HashSet<(i32, i32)> = inputs
        .sample_vehicle_day
        .iter()
        .flat_map(|sd| {
            source_ttgs
                .get(&sd.source_type_id)
                .into_iter()
                .flatten()
                .map(move |&ttg| (sd.veh_id, ttg))
        })
        .collect();

    let source_type_ids: Vec<i32> = inputs
        .sample_vehicle_day
        .iter()
        .map(|sd| sd.source_type_id)
        .collect::<BTreeSet<i32>>()
        .into_iter()
        .collect();
    let mut hour_days: Vec<&HourDayRow> = inputs.hour_day.iter().collect();
    hour_days.sort_by_key(|hd| hd.hour_day_id);
    let hour_day_ids: Vec<i32> = hour_days.iter().map(|hd| hd.hour_day_id).collect();
    let hour_of: Vec<i32> = hour_days.iter().map(|hd| hd.hour_id).collect();
    let day_of: Vec<i32> = hour_days.iter().map(|hd| hd.day_id).collect();
    let month_ids: Vec<i32> = inputs
        .runspec_month_ids
        .iter()
        .copied()
        .collect::<BTreeSet<i32>>()
        .into_iter()
        .collect();
    let ttg_ids: Vec<i32> = inputs
        .tank_temperature_group_ids
        .iter()
        .copied()
        .collect::<BTreeSet<i32>>()
        .into_iter()
        .collect();

    let source_type_index: HashMap<i32, usize> = source_type_ids
        .iter()
        .enumerate()
        .map(|(i, &s)| (s, i))
        .collect();
    let month_index: HashMap<i32, usize> =
        month_ids.iter().enumerate().map(|(i, &m)| (m, i)).collect();
    let ttg_index: HashMap<i32, usize> = ttg_ids.iter().enumerate().map(|(i, &t)| (t, i)).collect();

    // --- cursors: the Java file-backed grouping, done in memory ---
    // svth: (vehID, hourDayID) → (min keyOnTime, Σ(keyOffTime − keyOnTime)).
    let mut svth_group: HashMap<(i32, i32), (i32, i32)> = HashMap::new();
    for r in svth {
        let entry = svth_group
            .entry((r.veh_id, r.hour_day_id))
            .or_insert((i32::MAX, 0));
        entry.0 = entry.0.min(r.key_on_time);
        entry.1 += r.key_off_time - r.key_on_time;
    }
    // hot soak: (vehID, hourDayID) → per (monthIdx, ttgIdx) (min time, count).
    // `HotSoakCells` is `(min hotSoakTime, count)` keyed by `(monthIdx, ttgIdx)`.
    type HotSoakCells = HashMap<(usize, usize), (i32, i32)>;
    let mut hot_soak_group: HashMap<(i32, i32), HotSoakCells> = HashMap::new();
    for r in hot_soak {
        let (Some(&m_idx), Some(&t_idx)) = (
            month_index.get(&r.month_id),
            ttg_index.get(&r.tank_temperature_group_id),
        ) else {
            continue;
        };
        let cell = hot_soak_group
            .entry((r.veh_id, r.hour_day_id))
            .or_default()
            .entry((m_idx, t_idx))
            .or_insert((i32::MAX, 0));
        cell.0 = cell.0.min(r.hot_soak_time);
        cell.1 += 1;
    }

    let n_source = source_type_ids.len();
    let n_month = month_ids.len();
    let n_hour_day = hour_day_ids.len();
    let n_ttg = ttg_ids.len();

    // coldSoakMinutes[source][month][hour][initialHour] and its per-hour sum.
    let mut cold_soak_minutes: Vec<Vec<Vec<Vec<i32>>>> =
        vec![vec![vec![vec![0; n_hour_day]; n_hour_day]; n_month]; n_source];
    let mut cold_soak_minutes_sum: Vec<Vec<Vec<i32>>> =
        vec![vec![vec![0; n_hour_day]; n_month]; n_source];
    // initialHourDayIndex[month][ttg] — persists across vehicles, reset to the
    // first hour-day of each day.
    let mut initial_hour_day_index: Vec<Vec<usize>> = vec![vec![0; n_ttg]; n_month];

    for &(veh_id, source_type, veh_day) in &veh_list {
        let Some(&s_idx) = source_type_index.get(&source_type) else {
            continue;
        };
        for h_idx in 0..n_hour_day {
            if day_of[h_idx] != veh_day {
                continue;
            }
            let svth_cell = svth_group.get(&(veh_id, hour_day_ids[h_idx]));
            let has_svth = svth_cell.is_some();
            let (svth_min_key_on, svth_sum) = svth_cell.copied().unwrap_or((0, 0));
            let hot_soak_cell = hot_soak_group.get(&(veh_id, hour_day_ids[h_idx]));

            // Reset the initial-hour tracker at the first hour of the day.
            if hour_of[h_idx] == 1 {
                for month_row in &mut initial_hour_day_index {
                    for slot in month_row.iter_mut() {
                        *slot = h_idx;
                    }
                }
            }

            for t_idx in 0..n_ttg {
                if !veh_ttgs.contains(&(veh_id, ttg_ids[t_idx])) {
                    continue;
                }
                for m_idx in 0..n_month {
                    let (hot_soak_min_time, hot_soak_count) = hot_soak_cell
                        .and_then(|per| per.get(&(m_idx, t_idx)).copied())
                        .unwrap_or((999_999, 0));
                    let has_hot_soak = hot_soak_count > 0;

                    let mut should_reset = false;
                    let (from_initial, in_current): (i32, i32) = if !has_svth && !has_hot_soak {
                        // No first record because there are no records.
                        (60, 0)
                    } else if has_svth && (!has_hot_soak || svth_min_key_on < hot_soak_min_time) {
                        // The first record is a vehicle trip.
                        should_reset = true;
                        let from_initial = svth_min_key_on % 60;
                        let mut in_current = 60 - from_initial - svth_sum;
                        if has_hot_soak {
                            in_current -= hot_soak_count;
                        }
                        (from_initial, in_current)
                    } else {
                        // The first record is a hot-soak minute.
                        should_reset = true;
                        let mut in_current = 60;
                        if has_hot_soak {
                            in_current -= hot_soak_count;
                        }
                        if has_svth {
                            in_current -= svth_sum;
                        }
                        (0, in_current)
                    };

                    let initial = initial_hour_day_index[m_idx][t_idx];
                    cold_soak_minutes[s_idx][m_idx][h_idx][initial] += from_initial;
                    cold_soak_minutes[s_idx][m_idx][h_idx][h_idx] += in_current;
                    cold_soak_minutes_sum[s_idx][m_idx][h_idx] += from_initial + in_current;

                    if should_reset {
                        initial_hour_day_index[m_idx][t_idx] = h_idx;
                    }
                }
            }
        }
    }

    // getUserInputKeys — (source type, month) already supplied for the zone.
    let user_inputs: HashSet<(i32, i32)> = inputs
        .prior_cold_soak_initial_hour_fraction
        .iter()
        .filter(|r| r.is_user_input && r.zone_id == zone_id)
        .map(|r| (r.source_type_id, r.month_id))
        .collect();

    let mut output: Vec<ColdSoakInitialHourFractionRow> = Vec::new();
    for s_idx in 0..n_source {
        for m_idx in 0..n_month {
            if user_inputs.contains(&(source_type_ids[s_idx], month_ids[m_idx])) {
                continue;
            }
            for h_idx in 0..n_hour_day {
                let sum = cold_soak_minutes_sum[s_idx][m_idx][h_idx];
                if sum <= 0 {
                    continue;
                }
                for i_idx in 0..n_hour_day {
                    let minutes = cold_soak_minutes[s_idx][m_idx][h_idx][i_idx];
                    if minutes <= 0 {
                        continue;
                    }
                    output.push(ColdSoakInitialHourFractionRow {
                        source_type_id: source_type_ids[s_idx],
                        zone_id,
                        month_id: month_ids[m_idx],
                        hour_day_id: hour_day_ids[h_idx],
                        initial_hour_day_id: hour_day_ids[i_idx],
                        cold_soak_initial_hour_fraction: f64::from(minutes) / f64::from(sum),
                        is_user_input: false,
                    });
                }
            }
        }
    }
    output.sort_by_key(|r| {
        (
            r.source_type_id,
            r.zone_id,
            r.month_id,
            r.hour_day_id,
            r.initial_hour_day_id,
        )
    });
    output
}

// =============================================================================
//   Orchestrator
// =============================================================================

/// Run `TankTemperatureGenerator`'s full TTG-1 … TTG-7 sequence for one zone.
///
/// Ports `executeLoop`: TTG-1 builds the cold-soak tank temperatures, the trip
/// steps split the sample trips and their hot soaks, TTG-4 derives the
/// operating and hot-soak temperatures, and TTG-5 … TTG-7 aggregate them into
/// the four output tables. The intermediate tables
/// (`SampleVehicleTripByHour`, `HotSoakEventByHour`, `OperatingTemperature`,
/// `HotSoakTemperature`) are computed once and threaded through the steps
/// that consume them.
#[must_use]
pub fn generate_tank_temperatures(
    inputs: &TankTemperatureInputs,
    zone_id: i32,
) -> TankTemperatureOutput {
    // TTG-1.
    let cold_soak_tank_temperature =
        calculate_cold_soak_tank_temperature(&inputs.zone_month_hour, zone_id);
    // `TempColdSoakTankTemperature` — (month, hour) → coldSoak, for this zone.
    let cstt: HashMap<(i32, i32), f64> = cold_soak_tank_temperature
        .iter()
        .map(|r| ((r.month_id, r.hour_id), r.cold_soak_tank_temperature))
        .collect();
    // flagMarkerTrips + TTG-2.
    let svth = flag_and_split_trips(&inputs.sample_vehicle_trip, &inputs.hour_day);
    // TTG-3.
    let events = build_hot_soak_events(&svth, &inputs.hour_day);
    // TTG-4.
    let (operating, hot_soak) =
        calculate_operating_and_hot_soak_temperatures(inputs, &svth, &events, &cstt, zone_id);
    // TTG-5.
    let average_tank_temperature = calculate_average_tank_temperature(
        &operating,
        &hot_soak,
        &cstt,
        &inputs.hour_day,
        &inputs.prior_average_tank_temperature,
        zone_id,
    );
    // buildTTGeMinutes + TTG-6.
    let soak_activity_fraction =
        calculate_soak_activity_fraction(inputs, &svth, &hot_soak, zone_id);
    // TTG-7.
    let cold_soak_initial_hour_fraction =
        calculate_cold_soak_initial_hour_fractions(inputs, &svth, &hot_soak, zone_id);

    TankTemperatureOutput {
        cold_soak_tank_temperature,
        average_tank_temperature,
        soak_activity_fraction,
        cold_soak_initial_hour_fraction,
    }
}

// =============================================================================
//   Generator
// =============================================================================

/// Default-DB tables [`TankTemperatureGenerator`] reads, in canonical MOVES
/// casing.
static INPUT_TABLES: &[&str] = &[
    "ZoneMonthHour",
    "HourDay",
    "SampleVehicleDay",
    "SampleVehicleTrip",
    "SourceTypeModelYearGroup",
    "TankTemperatureRise",
    "TankTemperatureGroup",
    "RunSpecMonth",
    "RunSpecHourDay",
];

/// Scratch tables [`TankTemperatureGenerator`] writes.
static OUTPUT_TABLES: &[&str] = &[
    "ColdSoakTankTemperature",
    "AverageTankTemperature",
    "SoakActivityFraction",
    "ColdSoakInitialHourFraction",
];

/// The Task 38 generator — the framework adapter around
/// [`generate_tank_temperatures`].
///
/// Ports the master-loop surface of `TankTemperatureGenerator.java`: it
/// subscribes for *Evap Permeation*, *Evap Fuel Vapor Venting* and *Evap Fuel
/// Leaks* at `ZONE` granularity, `GENERATOR` priority, and declares the four
/// scratch tables it produces. `Generator::execute` is an empty stand-in
/// until the Task 50 data plane lands — see the module docs.
#[derive(Debug, Clone)]
pub struct TankTemperatureGenerator {
    subscriptions: Vec<CalculatorSubscription>,
}

impl TankTemperatureGenerator {
    /// Construct the generator with its three master-loop subscriptions.
    #[must_use]
    pub fn new() -> Self {
        // `MasterLoopPriority.GENERATOR` — see `TankTemperatureGenerator.subscribeToMe`.
        let priority =
            Priority::parse("GENERATOR").expect("\"GENERATOR\" is a canonical MasterLoopPriority");
        Self {
            subscriptions: vec![
                // Evap Permeation (process 11).
                CalculatorSubscription::new(ProcessId(11), Granularity::Zone, priority),
                // Evap Fuel Vapor Venting (process 12).
                CalculatorSubscription::new(ProcessId(12), Granularity::Zone, priority),
                // Evap Fuel Leaks (process 13).
                CalculatorSubscription::new(ProcessId(13), Granularity::Zone, priority),
            ],
        }
    }
}

impl Default for TankTemperatureGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl Generator for TankTemperatureGenerator {
    fn name(&self) -> &'static str {
        "TankTemperatureGenerator"
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        &self.subscriptions
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn output_tables(&self) -> &[&'static str] {
        OUTPUT_TABLES
    }

    fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        // The data plane (Task 50 `DataFrameStore`) is not yet materialised,
        // so `ctx.tables()` / `ctx.scratch()` are placeholders. The ported
        // computation lives in `generate_tank_temperatures`; once Task 50
        // lands, this body will read the input tables from `ctx`, call it per
        // zone, and write the four output tables into `ctx.scratch()`.
        Ok(CalculatorOutput::empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Assert two `f64`s agree within a tolerance comfortably tighter than any
    /// platform `libm` discrepancy yet far looser than a real algorithm bug.
    /// Reference values are traced through the TTG steps independently (see
    /// each test's comments).
    fn assert_close(got: f64, expected: f64, what: &str) {
        let diff = (got - expected).abs();
        assert!(
            diff < 1e-9,
            "{what}: got {got}, expected {expected}, diff {diff}"
        );
    }

    /// 24 `HourDay` rows for one day, with MOVES' `hourDayID = hourID·10 + dayID`
    /// packing — `hourDayID % 10` recovers the day, as TTG-4a relies on.
    fn hour_days(day_id: i32) -> Vec<HourDayRow> {
        (1..=24)
            .map(|hour_id| HourDayRow {
                hour_day_id: hour_id * 10 + day_id,
                hour_id,
                day_id,
            })
            .collect()
    }

    // --- TTG-1: cold soak tank temperature --------------------------------

    #[test]
    fn cold_soak_constant_temperature() {
        // A flat ambient temperature interpolates to a flat quarter-hour
        // series, so every tempDelta is 0, sumTempDelta never moves, and every
        // cold-soak temperature equals the ambient temperature.
        let zmh: Vec<ZoneMonthHourRow> = (1..=24)
            .map(|hour_id| ZoneMonthHourRow {
                zone_id: 1,
                month_id: 1,
                hour_id,
                temperature: 75.0,
            })
            .collect();
        let rows = calculate_cold_soak_tank_temperature(&zmh, 1);
        assert_eq!(rows.len(), 24, "one row per hour");
        for row in &rows {
            assert_close(row.cold_soak_tank_temperature, 75.0, "flat cold soak");
        }
    }

    #[test]
    fn cold_soak_recurrence_traced() {
        // Hours 1‥3 only: hour 1 (→2) and hour 2 (→3) produce quarter-hour
        // temperatures; hour 3 (→4 absent) and hour 24 (→1, 24 absent) do not.
        // temp[1]=50, temp[2]=70, temp[3]=70.
        //   qhTemp[1,ts] = 50 + (ts-1)·5  -> 50, 55, 60, 65
        //   qhTemp[2,ts] = 70             -> 70, 70, 70, 70
        // Recurrence (first = qhTemp[1,1] = 50), in (hour, step) order:
        //   (1,1) qhTT=50.0   delta=0.0   sum=0.0
        //   (1,2) qhTT=50.0   delta=5.0   sum=5.0
        //   (1,3) qhTT=57.0   delta=3.0   sum=8.0
        //   (1,4) qhTT=61.2   delta=3.8   sum=11.8
        //   (2,1) qhTT=1.4·11.8+50 = 66.52
        // Cold soak keeps the step-1 values: hour 1 -> 50.0, hour 2 -> 66.52.
        let zmh = vec![
            ZoneMonthHourRow {
                zone_id: 7,
                month_id: 1,
                hour_id: 1,
                temperature: 50.0,
            },
            ZoneMonthHourRow {
                zone_id: 7,
                month_id: 1,
                hour_id: 2,
                temperature: 70.0,
            },
            ZoneMonthHourRow {
                zone_id: 7,
                month_id: 1,
                hour_id: 3,
                temperature: 70.0,
            },
        ];
        let rows = calculate_cold_soak_tank_temperature(&zmh, 7);
        assert_eq!(rows.len(), 2, "hours 1 and 2 produce output");
        assert_eq!(rows[0].hour_id, 1);
        assert_close(rows[0].cold_soak_tank_temperature, 50.0, "hour 1");
        assert_eq!(rows[1].hour_id, 2);
        assert_close(rows[1].cold_soak_tank_temperature, 66.52, "hour 2");
    }

    #[test]
    fn cold_soak_skips_month_without_hour_one() {
        // With no hour 1, the Java `firstQuarterHourTankTemperature` query
        // returns nothing for the month and it produces no rows.
        let zmh = vec![
            ZoneMonthHourRow {
                zone_id: 1,
                month_id: 1,
                hour_id: 2,
                temperature: 60.0,
            },
            ZoneMonthHourRow {
                zone_id: 1,
                month_id: 1,
                hour_id: 3,
                temperature: 61.0,
            },
        ];
        assert!(calculate_cold_soak_tank_temperature(&zmh, 1).is_empty());
        // A different zone is not selected.
        assert!(calculate_cold_soak_tank_temperature(&zmh, 999).is_empty());
    }

    // --- flagMarkerTrips + TTG-2: trip splitting --------------------------

    #[test]
    fn single_hour_trip_is_one_segment() {
        // keyOnTime 65 -> endOfHour = (⌊65/60⌋+1)·60 = 120; keyOffTime 90 ≤ 120,
        // so the whole trip is a single start+end segment. No predecessor
        // resolves priorTripID to 0.
        let trips = vec![SampleVehicleTripRow {
            veh_id: 1,
            day_id: 5,
            trip_id: 1,
            hour_id: 2,
            prior_trip_id: None,
            key_on_time: Some(65),
            key_off_time: 90,
        }];
        let svth = flag_and_split_trips(&trips, &hour_days(5));
        assert_eq!(svth.len(), 1);
        let r = svth[0];
        assert_eq!(r.hour_id, 2);
        assert_eq!(r.hour_day_id, 25);
        assert_eq!(r.prior_trip_id, 0);
        assert_eq!(r.key_on_time, 65);
        assert_eq!(r.key_off_time, 90);
        assert_eq!(r.end_of_hour, 120);
        assert!(r.start_of_trip && r.end_of_trip);
    }

    #[test]
    fn multi_hour_trip_splits_across_hours() {
        // keyOnTime 100 -> endOfHour 120; keyOffTime 200 spills two hours:
        //   [100,120] start, [121,180], [181,200] end.
        let trips = vec![SampleVehicleTripRow {
            veh_id: 1,
            day_id: 5,
            trip_id: 2,
            hour_id: 2,
            prior_trip_id: Some(1),
            key_on_time: Some(100),
            key_off_time: 200,
        }];
        let svth = flag_and_split_trips(&trips, &hour_days(5));
        assert_eq!(svth.len(), 3);
        assert_eq!(svth[0].hour_id, 2);
        assert!(svth[0].start_of_trip && !svth[0].end_of_trip);
        assert_eq!((svth[0].key_on_time, svth[0].key_off_time), (100, 120));
        assert_eq!(svth[1].hour_id, 3);
        assert!(!svth[1].start_of_trip && !svth[1].end_of_trip);
        assert_eq!((svth[1].key_on_time, svth[1].key_off_time), (121, 180));
        assert_eq!(svth[2].hour_id, 4);
        assert!(!svth[2].start_of_trip && svth[2].end_of_trip);
        assert_eq!((svth[2].key_on_time, svth[2].key_off_time), (181, 200));
        // No matching marker, so the explicit predecessor is kept.
        assert!(svth.iter().all(|r| r.prior_trip_id == 1));
    }

    #[test]
    fn marker_trip_excluded_and_successor_becomes_first() {
        // Trip 1 is a marker (no keyOnTime, no priorTripID): excluded, and
        // trip 2 — whose predecessor is that marker — becomes a first trip
        // (priorTripID 0). Trip 3's predecessor is the real trip 2, so it is
        // kept.
        let trips = vec![
            SampleVehicleTripRow {
                veh_id: 1,
                day_id: 5,
                trip_id: 1,
                hour_id: 1,
                prior_trip_id: None,
                key_on_time: None,
                key_off_time: 0,
            },
            SampleVehicleTripRow {
                veh_id: 1,
                day_id: 5,
                trip_id: 2,
                hour_id: 2,
                prior_trip_id: Some(1),
                key_on_time: Some(70),
                key_off_time: 90,
            },
            SampleVehicleTripRow {
                veh_id: 1,
                day_id: 5,
                trip_id: 3,
                hour_id: 3,
                prior_trip_id: Some(2),
                key_on_time: Some(130),
                key_off_time: 150,
            },
        ];
        let svth = flag_and_split_trips(&trips, &hour_days(5));
        assert_eq!(svth.len(), 2, "marker trip dropped");
        assert!(svth.iter().all(|r| r.trip_id != 1));
        let t2 = svth.iter().find(|r| r.trip_id == 2).unwrap();
        assert_eq!(t2.prior_trip_id, 0, "marker successor is a first trip");
        let t3 = svth.iter().find(|r| r.trip_id == 3).unwrap();
        assert_eq!(t3.prior_trip_id, 2, "real predecessor kept");
    }

    // --- TTG-3: hot soak events -------------------------------------------

    #[test]
    fn hot_soak_runs_to_next_trip() {
        // Trip 1 ends at keyOffTime 90; the next trip (priorTripID 1) starts
        // its first segment at keyOnTime 110, before endOfHour 120, so the
        // single hot-soak slice runs [90, 110).
        let svth = vec![
            SvthRow {
                veh_id: 1,
                trip_id: 1,
                day_id: 5,
                hour_day_id: 25,
                hour_id: 2,
                prior_trip_id: 0,
                key_on_time: 65,
                key_off_time: 90,
                end_of_hour: 120,
                start_of_trip: true,
                end_of_trip: true,
            },
            SvthRow {
                veh_id: 1,
                trip_id: 2,
                day_id: 5,
                hour_day_id: 25,
                hour_id: 2,
                prior_trip_id: 1,
                key_on_time: 110,
                key_off_time: 118,
                end_of_hour: 120,
                start_of_trip: true,
                end_of_trip: true,
            },
        ];
        let events = build_hot_soak_events(&svth, &hour_days(5));
        let trip1: Vec<&HotSoakEventRow> = events.iter().filter(|e| e.trip_id == 1).collect();
        assert_eq!(trip1.len(), 1, "soak bounded by the next trip");
        let e = trip1[0];
        assert_eq!(e.hot_soak_begin, 90);
        assert_eq!(e.hot_soak_end, 110);
        assert_eq!(e.hour_day_id, 25);
        assert!(e.start_of_hot_soak && e.end_of_hot_soak);
    }

    #[test]
    fn hot_soak_runs_to_end_of_day() {
        // Trip 1 has no successor, so its hot soak runs to the end of the day:
        // a slice for hour 23 and one for hour 24, then it stops.
        let svth = vec![SvthRow {
            veh_id: 1,
            trip_id: 1,
            day_id: 5,
            hour_day_id: 235,
            hour_id: 23,
            prior_trip_id: 0,
            key_on_time: 1330,
            key_off_time: 1380,
            end_of_hour: 1380,
            start_of_trip: true,
            end_of_trip: true,
        }];
        let events = build_hot_soak_events(&svth, &hour_days(5));
        assert_eq!(events.len(), 2, "hours 23 and 24");
        assert_eq!(events[0].hour_day_id, 235);
        assert!(events[0].start_of_hot_soak && !events[0].end_of_hot_soak);
        assert_eq!(events[1].hour_day_id, 245);
        assert!(!events[1].start_of_hot_soak);
    }

    // --- TTG-4: operating and hot-soak tank temperatures ------------------

    /// A one-vehicle / one-group fixture: source type 10 → tank-temperature
    /// group 3, with the given rise coefficients, and an ambient temperature
    /// of 0 °F at `(month 1, hour 2)`.
    fn ttg4_inputs(term_a: f64, term_b: f64) -> TankTemperatureInputs {
        TankTemperatureInputs {
            zone_month_hour: vec![ZoneMonthHourRow {
                zone_id: 1,
                month_id: 1,
                hour_id: 2,
                temperature: 0.0,
            }],
            hour_day: hour_days(5),
            sample_vehicle_day: vec![SampleVehicleDayRow {
                veh_id: 1,
                source_type_id: 10,
                day_id: 5,
            }],
            source_type_model_year_group: vec![SourceTypeModelYearGroupRow {
                source_type_id: 10,
                tank_temperature_group_id: 3,
            }],
            tank_temperature_rise: vec![TankTemperatureRiseRow {
                tank_temperature_group_id: 3,
                tank_temperature_rise_term_a: term_a,
                tank_temperature_rise_term_b: term_b,
            }],
            ..TankTemperatureInputs::default()
        }
    }

    #[test]
    fn operating_temperature_formula() {
        // First trip, single segment: keyOnTemp = coldSoak = 60. With
        // termA 5, termB 0.1 over keyOffTime−keyOnTime = 25 minutes,
        //   keyOffTemp = 60 + ((5 + 0.1·(95−60)) / 1.2)·(25/60)
        //              = 60 + (8.5/1.2)·(5/12) = 60 + 425/144.
        let inputs = ttg4_inputs(5.0, 0.1);
        let svth = vec![SvthRow {
            veh_id: 1,
            trip_id: 1,
            day_id: 5,
            hour_day_id: 25,
            hour_id: 2,
            prior_trip_id: 0,
            key_on_time: 65,
            key_off_time: 90,
            end_of_hour: 120,
            start_of_trip: true,
            end_of_trip: true,
        }];
        let cstt: HashMap<(i32, i32), f64> = [((1, 2), 60.0)].into_iter().collect();
        let events: Vec<HotSoakEventRow> = Vec::new();
        let (operating, hot_soak) =
            calculate_operating_and_hot_soak_temperatures(&inputs, &svth, &events, &cstt, 1);
        assert_eq!(operating.len(), 1);
        assert_close(operating[0].key_on_temp, 60.0, "keyOnTemp");
        assert_close(
            operating[0].key_off_temp,
            60.0 + 425.0 / 144.0,
            "keyOffTemp",
        );
        assert!(hot_soak.is_empty(), "no soak events provided");
    }

    #[test]
    fn operating_temperature_capped_at_140() {
        // A huge termA drives keyOffTemp past the 140 °F EPA cap.
        let inputs = ttg4_inputs(100_000.0, 0.0);
        let svth = vec![SvthRow {
            veh_id: 1,
            trip_id: 1,
            day_id: 5,
            hour_day_id: 25,
            hour_id: 2,
            prior_trip_id: 0,
            key_on_time: 65,
            key_off_time: 90,
            end_of_hour: 120,
            start_of_trip: true,
            end_of_trip: true,
        }];
        let cstt: HashMap<(i32, i32), f64> = [((1, 2), 60.0)].into_iter().collect();
        let events: Vec<HotSoakEventRow> = Vec::new();
        let (operating, _) =
            calculate_operating_and_hot_soak_temperatures(&inputs, &svth, &events, &cstt, 1);
        assert_eq!(operating.len(), 1);
        assert_close(operating[0].key_off_temp, MAX_OPERATING_TANK_TEMP, "capped");
    }

    #[test]
    fn operating_temperature_chains_through_trips() {
        // Trip 1 (first) ends and, with no soak events, hands its keyOffTemp
        // straight to its successor trip 2 as the starting keyOnTemp — the
        // work-queue chain.
        //   trip 1: keyOnTemp 60, keyOffTemp = 60 + (8.5/1.2)·(30/60) = 1525/24
        //   trip 2: keyOnTemp = 1525/24 (carried, not the cold soak),
        //           keyOffTemp = 1525/24 + (391/48 / 1.2)·(10/60) = 111755/1728
        let inputs = ttg4_inputs(5.0, 0.1);
        let svth = vec![
            SvthRow {
                veh_id: 1,
                trip_id: 1,
                day_id: 5,
                hour_day_id: 25,
                hour_id: 2,
                prior_trip_id: 0,
                key_on_time: 60,
                key_off_time: 90,
                end_of_hour: 120,
                start_of_trip: true,
                end_of_trip: true,
            },
            SvthRow {
                veh_id: 1,
                trip_id: 2,
                day_id: 5,
                hour_day_id: 25,
                hour_id: 2,
                prior_trip_id: 1,
                key_on_time: 100,
                key_off_time: 110,
                end_of_hour: 120,
                start_of_trip: true,
                end_of_trip: true,
            },
        ];
        let cstt: HashMap<(i32, i32), f64> = [((1, 2), 60.0)].into_iter().collect();
        let events: Vec<HotSoakEventRow> = Vec::new();
        let (operating, _) =
            calculate_operating_and_hot_soak_temperatures(&inputs, &svth, &events, &cstt, 1);
        assert_eq!(
            operating.len(),
            2,
            "both trips processed via the work queue"
        );
        assert_eq!(operating[0].trip_id, 1);
        assert_close(
            operating[0].key_on_temp,
            60.0,
            "trip 1 starts from cold soak",
        );
        assert_close(
            operating[0].key_off_temp,
            1525.0 / 24.0,
            "trip 1 keyOffTemp",
        );
        assert_eq!(operating[1].trip_id, 2);
        assert_close(
            operating[1].key_on_temp,
            1525.0 / 24.0,
            "trip 2 carries trip 1's keyOffTemp",
        );
        assert_close(
            operating[1].key_off_temp,
            111_755.0 / 1728.0,
            "trip 2 keyOffTemp",
        );
    }

    #[test]
    fn hot_soak_minute_recurrence() {
        // Hot soak from initialTankTemp 100, ambient 0, cold soak 60: every
        // minute stays above 60+3, so all three are recorded.
        //   m119: 1.4·0/60 + 100        = 100
        //   m120: 1.4·(−100)/60 + 100   = 293/3
        //   m121: 1.4·(−593/3)/60 + 100 = 85849/900
        let cstt: HashMap<(i32, i32), f64> = [((1, 2), 60.0)].into_iter().collect();
        let tables = Ttg4Tables {
            cstt: &cstt,
            hour_of_hour_day: [(25, 2)].into_iter().collect(),
            zone_temp: [((1, 2), 0.0)].into_iter().collect(),
            events_by_trip: [(
                (1, 5, 1),
                vec![HotSoakEventRow {
                    veh_id: 1,
                    trip_id: 1,
                    day_id: 5,
                    hour_day_id: 25,
                    end_of_hour: 120,
                    hot_soak_begin: 119,
                    hot_soak_end: 122,
                    start_of_hot_soak: true,
                    end_of_hot_soak: true,
                }],
            )]
            .into_iter()
            .collect(),
        };
        let trip = ParsedTrip {
            month_id: 1,
            tank_temperature_group_id: 3,
            veh_id: 1,
            day_id: 5,
            trip_id: 1,
        };
        let mut out = Vec::new();
        let ended_at = hot_soak_for_trip(&mut out, &tables, &trip, 100.0);
        assert_eq!(out.len(), 3);
        assert_close(out[0].soak_tank_temp, 100.0, "minute 119");
        assert_close(out[1].soak_tank_temp, 293.0 / 3.0, "minute 120");
        assert_close(out[2].soak_tank_temp, 85_849.0 / 900.0, "minute 121");
        assert_close(ended_at, 85_849.0 / 900.0, "return value is the last soak");
        assert!(out.iter().all(|r| r.op_mode_id == OP_MODE_HOT_SOAKING));
    }

    #[test]
    fn hot_soak_terminates_when_cold() {
        // initialTankTemp 64, ambient 0, cold soak 60 (threshold 63):
        //   m119: soak 64 > 63          -> hot, recorded
        //   m120: 1.4·(−64)/60 + 64 ≈ 62.5 ≤ 63 -> cold soaking, soak ends.
        // Only minute 119 is recorded; the sentinel is returned.
        let cstt: HashMap<(i32, i32), f64> = [((1, 2), 60.0)].into_iter().collect();
        let tables = Ttg4Tables {
            cstt: &cstt,
            hour_of_hour_day: [(25, 2)].into_iter().collect(),
            zone_temp: [((1, 2), 0.0)].into_iter().collect(),
            events_by_trip: [(
                (1, 5, 1),
                vec![HotSoakEventRow {
                    veh_id: 1,
                    trip_id: 1,
                    day_id: 5,
                    hour_day_id: 25,
                    end_of_hour: 120,
                    hot_soak_begin: 119,
                    hot_soak_end: 130,
                    start_of_hot_soak: true,
                    end_of_hot_soak: true,
                }],
            )]
            .into_iter()
            .collect(),
        };
        let trip = ParsedTrip {
            month_id: 1,
            tank_temperature_group_id: 3,
            veh_id: 1,
            day_id: 5,
            trip_id: 1,
        };
        let mut out = Vec::new();
        let ended_at = hot_soak_for_trip(&mut out, &tables, &trip, 64.0);
        assert_eq!(out.len(), 1, "only the still-hot minute is recorded");
        assert_close(out[0].soak_tank_temp, 64.0, "minute 119");
        assert_close(ended_at, HOT_SOAK_ENDED_SENTINEL, "soak ended cold");
    }

    // --- TTG-5: average tank temperature ----------------------------------

    /// An `OperatingTemperature` row with the fields TTG-5 reads; the rest are
    /// placeholders.
    fn op_row(
        month_id: i32,
        tank_temperature_group_id: i32,
        hour_day_id: i32,
        key_on_time: i32,
        key_off_time: i32,
        key_on_temp: f64,
        key_off_temp: f64,
    ) -> OperatingTemperatureRow {
        OperatingTemperatureRow {
            month_id,
            tank_temperature_group_id,
            veh_id: 1,
            trip_id: 1,
            hour_day_id,
            prior_trip_id: 0,
            key_on_time,
            key_off_time,
            end_of_hour: 0,
            start_of_trip: true,
            end_of_trip: true,
            key_on_temp,
            key_off_temp,
        }
    }

    /// A `HotSoakTemperature` row with the fields TTG-5/6/7 read; the rest are
    /// placeholders.
    fn hs_row(
        month_id: i32,
        tank_temperature_group_id: i32,
        veh_id: i32,
        trip_id: i32,
        hour_day_id: i32,
        hot_soak_time: i32,
        soak_tank_temp: f64,
    ) -> HotSoakTemperatureRow {
        HotSoakTemperatureRow {
            month_id,
            tank_temperature_group_id,
            veh_id,
            trip_id,
            hour_day_id,
            hot_soak_time,
            initial_tank_temp: 0.0,
            soak_tank_temp,
            temp_delta: 0.0,
            temperature: 0.0,
            cold_soak_temp: 0.0,
            op_mode_id: OP_MODE_HOT_SOAKING,
        }
    }

    #[test]
    fn average_tank_temperature_aggregates() {
        // One operating segment and two hot-soak minutes for (month 1,
        // hour-day 25, group 3):
        //   opMode 151 = coldSoak[1, hour 2]                       = 55
        //   opMode 300 = Σ(60·(60+80)/2) / Σ60                     = 70
        //   opMode 150 = mean(90, 86)                              = 88
        let operating = vec![op_row(1, 3, 25, 60, 120, 60.0, 80.0)];
        let hot_soak = vec![
            hs_row(1, 3, 1, 1, 25, 119, 90.0),
            hs_row(1, 3, 1, 1, 25, 120, 86.0),
        ];
        let cstt: HashMap<(i32, i32), f64> = [((1, 2), 55.0)].into_iter().collect();
        let rows =
            calculate_average_tank_temperature(&operating, &hot_soak, &cstt, &hour_days(5), &[], 9);
        assert_eq!(rows.len(), 3);
        let by_mode = |m: i32| {
            *rows
                .iter()
                .find(|r| r.op_mode_id == m)
                .expect("op mode present")
        };
        assert_close(
            by_mode(OP_MODE_COLD_SOAKING).average_tank_temperature,
            55.0,
            "151",
        );
        assert_close(
            by_mode(OP_MODE_RUNNING).average_tank_temperature,
            70.0,
            "300",
        );
        assert_close(
            by_mode(OP_MODE_HOT_SOAKING).average_tank_temperature,
            88.0,
            "150",
        );
        for r in &rows {
            assert_eq!((r.zone_id, r.month_id, r.hour_day_id), (9, 1, 25));
            assert_eq!(r.tank_temperature_group_id, 3);
            assert!(!r.is_user_input);
        }
    }

    #[test]
    fn user_input_blocks_average_tank_temperature() {
        // A user-input row for (group 3, zone 9, month 1) blocks all three
        // generated op-mode rows for that key.
        let operating = vec![op_row(1, 3, 25, 60, 120, 60.0, 80.0)];
        let cstt: HashMap<(i32, i32), f64> = [((1, 2), 55.0)].into_iter().collect();
        let prior = vec![AverageTankTemperatureRow {
            zone_id: 9,
            month_id: 1,
            hour_day_id: 0,
            tank_temperature_group_id: 3,
            op_mode_id: OP_MODE_RUNNING,
            average_tank_temperature: 0.0,
            is_user_input: true,
        }];
        let rows =
            calculate_average_tank_temperature(&operating, &[], &cstt, &hour_days(5), &prior, 9);
        assert!(rows.is_empty(), "user input blocks generation");
    }

    // --- TTG-6: soak activity fraction ------------------------------------

    #[test]
    fn soak_activity_fraction_split() {
        // One vehicle (source type 10, day 5): eMinutes = 60·1 = 60. One trip
        // segment of 30 minutes: oMinutes = 30. Two hot-soak minutes:
        //   opMode 150 = 2 / (60 − 30)            = 2/30
        //   opMode 151 = (60 − 30 − 2) / (60 − 30) = 28/30
        let inputs = TankTemperatureInputs {
            hour_day: hour_days(5),
            sample_vehicle_day: vec![SampleVehicleDayRow {
                veh_id: 1,
                source_type_id: 10,
                day_id: 5,
            }],
            runspec_month_ids: vec![1],
            runspec_hour_day_ids: vec![25],
            ..TankTemperatureInputs::default()
        };
        let svth = vec![SvthRow {
            veh_id: 1,
            trip_id: 1,
            day_id: 5,
            hour_day_id: 25,
            hour_id: 2,
            prior_trip_id: 0,
            key_on_time: 60,
            key_off_time: 90,
            end_of_hour: 120,
            start_of_trip: true,
            end_of_trip: true,
        }];
        let hot_soak = vec![
            hs_row(1, 3, 1, 1, 25, 119, 0.0),
            hs_row(1, 3, 1, 1, 25, 120, 0.0),
        ];
        let rows = calculate_soak_activity_fraction(&inputs, &svth, &hot_soak, 7);
        assert_eq!(rows.len(), 2);
        let by_mode = |m: i32| {
            *rows
                .iter()
                .find(|r| r.op_mode_id == m)
                .expect("op mode present")
        };
        assert_close(
            by_mode(OP_MODE_HOT_SOAKING).soak_activity_fraction,
            2.0 / 30.0,
            "150",
        );
        assert_close(
            by_mode(OP_MODE_COLD_SOAKING).soak_activity_fraction,
            28.0 / 30.0,
            "151",
        );
        for r in &rows {
            assert_eq!((r.source_type_id, r.zone_id, r.month_id), (10, 7, 1));
            assert_eq!(r.hour_day_id, 25);
        }
    }

    // --- TTG-7: cold soak initial hour fractions --------------------------

    #[test]
    fn cold_soak_initial_hour_fraction_apportions_minutes() {
        // One vehicle, one group, one month, two hour-days (hour 1, hour 2).
        // Hour 1 has no activity — 60 minutes carry over from its own initial
        // hour. Hour 2 has a trip: keyOnTime 130 → 130 mod 60 = 10 minutes
        // carry over from the initial hour (hour 1), and 60 − 10 − 20 = 30
        // start in hour 2.
        //   hour 1: coldSoakMinutes[initial 1] = 60, sum 60 -> fraction 1.0
        //   hour 2: coldSoakMinutes[initial 1] = 10, [initial 2] = 30,
        //           sum 40 -> fractions 0.25 and 0.75
        let inputs = TankTemperatureInputs {
            hour_day: vec![
                HourDayRow {
                    hour_day_id: 15,
                    hour_id: 1,
                    day_id: 5,
                },
                HourDayRow {
                    hour_day_id: 25,
                    hour_id: 2,
                    day_id: 5,
                },
            ],
            sample_vehicle_day: vec![SampleVehicleDayRow {
                veh_id: 1,
                source_type_id: 10,
                day_id: 5,
            }],
            source_type_model_year_group: vec![SourceTypeModelYearGroupRow {
                source_type_id: 10,
                tank_temperature_group_id: 3,
            }],
            tank_temperature_group_ids: vec![3],
            runspec_month_ids: vec![1],
            ..TankTemperatureInputs::default()
        };
        let svth = vec![SvthRow {
            veh_id: 1,
            trip_id: 1,
            day_id: 5,
            hour_day_id: 25,
            hour_id: 2,
            prior_trip_id: 0,
            key_on_time: 130,
            key_off_time: 150,
            end_of_hour: 180,
            start_of_trip: true,
            end_of_trip: true,
        }];
        let rows = calculate_cold_soak_initial_hour_fractions(&inputs, &svth, &[], 1);
        assert_eq!(rows.len(), 3);
        assert_eq!((rows[0].hour_day_id, rows[0].initial_hour_day_id), (15, 15));
        assert_close(rows[0].cold_soak_initial_hour_fraction, 1.0, "hour 1");
        assert_eq!((rows[1].hour_day_id, rows[1].initial_hour_day_id), (25, 15));
        assert_close(rows[1].cold_soak_initial_hour_fraction, 0.25, "carryover");
        assert_eq!((rows[2].hour_day_id, rows[2].initial_hour_day_id), (25, 25));
        assert_close(rows[2].cold_soak_initial_hour_fraction, 0.75, "current");
        for r in &rows {
            assert_eq!((r.source_type_id, r.zone_id, r.month_id), (10, 1, 1));
        }
    }

    // --- end to end + generator -------------------------------------------

    #[test]
    fn generate_full_chain_is_deterministic() {
        // A constant-75 °F zone with one vehicle making one trip. The cold
        // soak is exactly 75 everywhere; every other table is populated and
        // the whole chain is order-independent.
        let zone_month_hour: Vec<ZoneMonthHourRow> = (1..=24)
            .map(|hour_id| ZoneMonthHourRow {
                zone_id: 1,
                month_id: 1,
                hour_id,
                temperature: 75.0,
            })
            .collect();
        let inputs = TankTemperatureInputs {
            zone_month_hour,
            hour_day: hour_days(5),
            sample_vehicle_day: vec![SampleVehicleDayRow {
                veh_id: 1,
                source_type_id: 10,
                day_id: 5,
            }],
            sample_vehicle_trip: vec![SampleVehicleTripRow {
                veh_id: 1,
                day_id: 5,
                trip_id: 1,
                hour_id: 2,
                prior_trip_id: None,
                key_on_time: Some(65),
                key_off_time: 90,
            }],
            source_type_model_year_group: vec![SourceTypeModelYearGroupRow {
                source_type_id: 10,
                tank_temperature_group_id: 3,
            }],
            tank_temperature_rise: vec![TankTemperatureRiseRow {
                tank_temperature_group_id: 3,
                tank_temperature_rise_term_a: 50.0,
                tank_temperature_rise_term_b: 0.0,
            }],
            tank_temperature_group_ids: vec![3],
            runspec_month_ids: vec![1],
            runspec_hour_day_ids: hour_days(5).iter().map(|hd| hd.hour_day_id).collect(),
            ..TankTemperatureInputs::default()
        };
        let out = generate_tank_temperatures(&inputs, 1);
        assert_eq!(out.cold_soak_tank_temperature.len(), 24, "one per hour");
        for row in &out.cold_soak_tank_temperature {
            assert_close(row.cold_soak_tank_temperature, 75.0, "constant cold soak");
        }
        assert!(
            !out.average_tank_temperature.is_empty(),
            "TTG-5 produced rows"
        );
        assert!(
            !out.soak_activity_fraction.is_empty(),
            "TTG-6 produced rows"
        );
        assert!(
            !out.cold_soak_initial_hour_fraction.is_empty(),
            "TTG-7 produced rows"
        );
        // Every cold-soaking average is the constant zone temperature.
        for row in &out.average_tank_temperature {
            if row.op_mode_id == OP_MODE_COLD_SOAKING {
                assert_close(row.average_tank_temperature, 75.0, "151 = cold soak");
            }
        }
        // The chain is deterministic — a second run is bit-identical.
        assert!(generate_tank_temperatures(&inputs, 1) == out);
    }

    #[test]
    fn empty_inputs_produce_no_rows() {
        let out = generate_tank_temperatures(&TankTemperatureInputs::default(), 1);
        assert_eq!(out, TankTemperatureOutput::default());
    }

    #[test]
    fn generator_metadata_matches_master_loop() {
        let generator = TankTemperatureGenerator::new();
        assert_eq!(generator.name(), "TankTemperatureGenerator");
        assert_eq!(
            generator.output_tables(),
            &[
                "ColdSoakTankTemperature",
                "AverageTankTemperature",
                "SoakActivityFraction",
                "ColdSoakInitialHourFraction",
            ]
        );
        assert!(generator.input_tables().contains(&"ZoneMonthHour"));
        assert!(generator.input_tables().contains(&"SampleVehicleTrip"));
        assert!(generator.upstream().is_empty());

        let subs = generator.subscriptions();
        assert_eq!(subs.len(), 3, "Evap Permeation + Vapor Venting + Leaks");
        let processes: Vec<ProcessId> = subs.iter().map(|s| s.process_id).collect();
        assert_eq!(processes, vec![ProcessId(11), ProcessId(12), ProcessId(13)]);
        for sub in subs {
            assert_eq!(sub.granularity, Granularity::Zone);
            assert_eq!(sub.priority.display(), "GENERATOR");
        }
    }

    #[test]
    fn generator_execute_returns_empty_until_data_plane() {
        // The Task 50 data plane is not yet wired; `execute` is a stand-in.
        let generator = TankTemperatureGenerator::new();
        let ctx = CalculatorContext::new();
        generator.execute(&ctx).expect("execute is infallible");
    }

    #[test]
    fn generator_is_object_safe() {
        // The CalculatorRegistry stores generators as `Box<dyn Generator>`.
        let generators: Vec<Box<dyn Generator>> = vec![Box::new(TankTemperatureGenerator::new())];
        assert_eq!(generators[0].name(), "TankTemperatureGenerator");
    }
}

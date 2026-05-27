//! `MeteorologyGenerator` — builds the meteorology fields of the
//! `ZoneMonthHour` execution table.
//!
//! Ports `gov.epa.otaq.moves.master.implementation.general.MeteorologyGenerator`
//! (migration plan Task 41). The Java generator runs one SQL pass —
//! `doHeatIndex` — that:
//!
//! 1. Fills a missing `County.barometricPressure` / `County.altitude` with
//!    altitude-group defaults.
//! 2. Writes `ZoneMonthHour.heatIndex` — the dry-bulb temperature below
//!    78 °F, the National Weather Service heat-index regression (capped at
//!    120 °F) at or above it.
//! 3. Writes `ZoneMonthHour.specificHumidity` and
//!    `ZoneMonthHour.molWaterFraction` from a saturation-vapour-pressure
//!    chain (intermediate `TK` / `PH2O` / `XH2O` / `PV` tables in the Java;
//!    pure functions here).
//!
//! # Structure of this port
//!
//! The Java threads its arithmetic through temporary MariaDB tables. This
//! port keeps the *numerics* as pure, individually testable functions
//! ([`heat_index`], [`saturation_vapor_pressure`], …) and the *per-row
//! pipeline* as [`compute_zone_month_hour`]. [`MeteorologyGenerator`]
//! implements the framework [`Generator`] trait around them.
//!
//! `MeteorologyGenerator::execute` returns an empty [`CalculatorOutput`]
//! until the Task 50 data plane lands the `ZoneMonthHour` / `Zone` /
//! `County` tables it would iterate; the numeric port above is complete
//! and is what `execute` will apply row by row.
//!
//! The whole `doHeatIndex` pass is idempotent — every step reads only
//! original input columns and the County fill-in conditions stop matching
//! once filled — so the Java `isFirst` flag is purely a once-per-run
//! optimisation, not a correctness requirement.

use std::collections::HashMap;
use std::sync::OnceLock;

use moves_calculator_info::{Granularity, Priority};
use moves_data::ProcessId;
use moves_framework::{
    CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped, Error,
    Generator, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

/// Inches of mercury → kilopascals. The Java multiplies barometric
/// pressure (stored in inHg) by this in every pressure expression.
const INHG_TO_KPA: f64 = 3.38639;

/// Default barometric pressure (inHg) for a high-altitude county whose
/// `barometricPressure` is missing — the average of the high group.
///
/// The Javadoc `@algorithm` comment says 24.69; the executed SQL literal
/// is 24.59. This port follows the value MOVES actually computes with.
const DEFAULT_PRESSURE_HIGH_ALTITUDE: f64 = 24.59;

/// Default barometric pressure (inHg) for every other county missing a
/// `barometricPressure` — the average of the low group.
const DEFAULT_PRESSURE_LOW_ALTITUDE: f64 = 28.94;

/// Barometric-pressure threshold (inHg) separating low (`>=`) from high
/// (`<`) altitude when a county does not record an `altitude`.
const ALTITUDE_PRESSURE_THRESHOLD: f64 = 25.8403;

/// Temperature (°F) at or above which the heat index uses the NWS
/// regression rather than the temperature itself.
const HEAT_INDEX_THRESHOLD_F: f64 = 78.0;

/// Upper bound (°F) the NWS heat index is clamped to — the SQL
/// `least(..., 120)`.
const HEAT_INDEX_CAP_F: f64 = 120.0;

/// Mixing-ratio numerator constant (g H2O / kg dry air) in the specific
/// humidity expression. MOVES uses 621.1.
const SPECIFIC_HUMIDITY_CONSTANT: f64 = 621.1;

/// A county's altitude class — `H` (high) or `L` (low) in `County.altitude`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Altitude {
    /// `'H'` — high altitude.
    High,
    /// `'L'` — low altitude.
    Low,
}

/// A county's barometric pressure and altitude after the
/// [`resolve_county_meteorology`] default-fill — both fields populated.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CountyMeteorology {
    /// Barometric pressure in inches of mercury.
    pub barometric_pressure_inhg: f64,
    /// Altitude class.
    pub altitude: Altitude,
}

/// Apply the `County` default-fill from `MeteorologyGenerator.doHeatIndex`.
///
/// Two `UPDATE County` statements, in order:
///
/// 1. A county with a missing or non-positive `barometricPressure` is
///    given 24.59 inHg when its `altitude` is `'H'`, 28.94 inHg otherwise
///    — a NULL or non-`H` altitude both take the SQL `CASE` `ELSE` branch.
/// 2. A county with a missing or non-`H`/`L` `altitude` is then classed
///    `Low` when its (now-populated) pressure is at least 25.8403 inHg,
///    `High` below that.
///
/// `pressure` is `None` for a SQL NULL; `altitude` is `None` for a NULL or
/// any value outside `('H','L')` — the SQL guard is `altitude not in
/// ('H','L')`.
///
/// ```
/// use moves_calculators::generators::meteorology::{resolve_county_meteorology, Altitude};
///
/// // NULL pressure + NULL altitude: step 1 picks the 28.94 ELSE branch,
/// // step 2 then classes 28.94 inHg as Low.
/// let resolved = resolve_county_meteorology(None, None);
/// assert_eq!(resolved.altitude, Altitude::Low);
/// ```
#[must_use]
pub fn resolve_county_meteorology(
    pressure: Option<f64>,
    altitude: Option<Altitude>,
) -> CountyMeteorology {
    // Step 1 — `UPDATE County SET barometricPressure = ...
    //           WHERE barometricPressure IS NULL OR barometricPressure <= 0`.
    // A value is kept only when present and strictly positive.
    let barometric_pressure_inhg = match pressure {
        Some(p) if p > 0.0 => p,
        _ => match altitude {
            Some(Altitude::High) => DEFAULT_PRESSURE_HIGH_ALTITUDE,
            // 'L', NULL, and out-of-domain altitudes all take the SQL ELSE.
            Some(Altitude::Low) | None => DEFAULT_PRESSURE_LOW_ALTITUDE,
        },
    };

    // Step 2 — `UPDATE County SET altitude = ...
    //           WHERE barometricPressure IS NOT NULL
    //             AND (altitude IS NULL OR altitude NOT IN ('H','L'))`.
    // Step 1 guarantees a non-NULL pressure, so the first guard always
    // holds here; only the altitude guard discriminates.
    let derived_altitude = if barometric_pressure_inhg >= ALTITUDE_PRESSURE_THRESHOLD {
        Altitude::Low
    } else {
        Altitude::High
    };
    let altitude = altitude.unwrap_or(derived_altitude);

    CountyMeteorology {
        barometric_pressure_inhg,
        altitude,
    }
}

/// Heat index (°F) — `MeteorologyGenerator`'s two `UPDATE ZoneMonthHour
/// SET heatIndex` statements.
///
/// Below 78 °F the heat index is the dry-bulb temperature unchanged. At or
/// above 78 °F it is the National Weather Service (Rothfusz) regression in
/// temperature and relative humidity (percent), capped at 120 °F by the
/// SQL `least(..., 120)`.
///
/// ```
/// use moves_calculators::generators::meteorology::heat_index;
///
/// // Below the 78 °F threshold the heat index is just the temperature.
/// assert_eq!(heat_index(70.0, 50.0), 70.0);
/// ```
#[must_use]
pub fn heat_index(temperature_f: f64, rel_humidity: f64) -> f64 {
    if temperature_f < HEAT_INDEX_THRESHOLD_F {
        return temperature_f;
    }
    let t = temperature_f;
    let rh = rel_humidity;
    // Term order mirrors the SQL expression so the left-to-right f64
    // summation matches MariaDB's.
    let hi = -42.379 + 2.04901523 * t + 10.14333127 * rh
        - 0.22475541 * t * rh
        - 0.00683783 * t * t
        - 0.05481717 * rh * rh
        + 0.00122874 * t * t * rh
        + 0.00085282 * t * rh * rh
        - 0.00000199 * t * t * rh * rh;
    hi.min(HEAT_INDEX_CAP_F)
}

/// Convert a Fahrenheit temperature to Kelvin — the Java `TK` column
/// expression `(5/9)*(temperature-32)+273.15`.
///
/// # Fidelity note
///
/// The Java writes the conversion factor as the SQL literal `(5/9)`. In
/// MariaDB `5/9` is decimal division and rounds to `div_precision_increment`
/// (default 4) places — 0.5556 — before promotion to a double. This port
/// uses the exact ratio `5.0 / 9.0`; the two differ by roughly 8e-6
/// relative, far inside any generator tolerance budget, and `5.0 / 9.0` is
/// the conversion the expression denotes. Task 44's canonical-capture
/// comparison is the place to revisit this if a divergence appears.
#[must_use]
pub fn fahrenheit_to_kelvin(temperature_f: f64) -> f64 {
    (5.0 / 9.0) * (temperature_f - 32.0) + 273.15
}

/// Saturation vapour pressure of water (kPa) at temperature `tk` (Kelvin)
/// — the Java `PH2O` table expression.
///
/// A Goff-Gratch-form polynomial in `tk / 273.15`; at the ice point
/// (`tk` of 273.15) every temperature-dependent term vanishes and the
/// result is `10` to the `-0.2138602`, about 0.611 kPa — the known
/// saturation pressure of water at 0 °C.
#[must_use]
pub fn saturation_vapor_pressure(tk: f64) -> f64 {
    // `TK/273.15` and `273.15/TK` are distinct expressions in the SQL;
    // keep them separate here too rather than reusing a reciprocal.
    let tk_over_ref = tk / 273.15;
    let ref_over_tk = 273.15 / tk;
    let exponent = 10.79574 * (1.0 - ref_over_tk) - 5.028 * tk_over_ref.log10()
        + 1.50475 * 10f64.powf(-4.0) * (1.0 - 10f64.powf(-8.2969 * (tk_over_ref - 1.0)))
        + 0.42873 * 10f64.powf(-3.0) * (10f64.powf(4.76955 * (1.0 - ref_over_tk)) - 1.0)
        - 0.2138602;
    10f64.powf(exponent)
}

/// Partial pressure of water vapour (kPa) — the Java `PV` expression
/// `relHumidity/100 * PH2O`.
#[must_use]
pub fn vapor_partial_pressure(rel_humidity: f64, saturation_pressure_kpa: f64) -> f64 {
    rel_humidity / 100.0 * saturation_pressure_kpa
}

/// H2O mole fraction (mol H2O / mol ambient air) — the Java `XH2O`
/// expression `((relHumidity/100) * PH2O) / (barometricPressure * 3.38639)`.
/// This is the value written to `ZoneMonthHour.molWaterFraction`.
#[must_use]
pub fn mole_fraction(
    rel_humidity: f64,
    saturation_pressure_kpa: f64,
    barometric_pressure_inhg: f64,
) -> f64 {
    ((rel_humidity / 100.0) * saturation_pressure_kpa) / (barometric_pressure_inhg * INHG_TO_KPA)
}

/// Specific humidity (g H2O / kg dry air) — the Java
/// `ZoneMonthHour.specificHumidity` expression
/// `(621.1 * PV) / (PB * 3.38639 - PV)`, where `PV` is the
/// [`vapor_partial_pressure`] and `PB` the barometric pressure in inHg.
#[must_use]
pub fn specific_humidity(vapor_partial_pressure_kpa: f64, barometric_pressure_inhg: f64) -> f64 {
    (SPECIFIC_HUMIDITY_CONSTANT * vapor_partial_pressure_kpa)
        / (barometric_pressure_inhg * INHG_TO_KPA - vapor_partial_pressure_kpa)
}

/// The per-row `ZoneMonthHour` inputs the generator reads.
///
/// `barometric_pressure_inhg` is the row's county pressure — the Java
/// reaches it by joining `ZoneMonthHour` → `Zone` → `County` and uses the
/// value left by [`resolve_county_meteorology`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ZoneMonthHourInputs {
    /// `ZoneMonthHour.temperature` — dry-bulb temperature, °F.
    pub temperature_f: f64,
    /// `ZoneMonthHour.relHumidity` — relative humidity, percent.
    pub rel_humidity: f64,
    /// Resolved `County.barometricPressure` for the row's zone, inHg.
    pub barometric_pressure_inhg: f64,
}

/// The three `ZoneMonthHour` columns the generator writes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ZoneMonthHourMeteorology {
    /// `ZoneMonthHour.heatIndex`, °F.
    pub heat_index: f64,
    /// `ZoneMonthHour.specificHumidity`, g H2O / kg dry air.
    pub specific_humidity: f64,
    /// `ZoneMonthHour.molWaterFraction`, mol H2O / mol ambient air.
    pub mol_water_fraction: f64,
}

/// Compute one `ZoneMonthHour` row's meteorology outputs — the whole
/// `doHeatIndex` arithmetic chain (`TK` → `PH2O` → `PV`/`XH2O` →
/// `specificHumidity`/`molWaterFraction`, plus `heatIndex`) for a single
/// row.
#[must_use]
pub fn compute_zone_month_hour(inputs: ZoneMonthHourInputs) -> ZoneMonthHourMeteorology {
    let tk = fahrenheit_to_kelvin(inputs.temperature_f);
    let saturation = saturation_vapor_pressure(tk);
    let pv = vapor_partial_pressure(inputs.rel_humidity, saturation);
    ZoneMonthHourMeteorology {
        heat_index: heat_index(inputs.temperature_f, inputs.rel_humidity),
        specific_humidity: specific_humidity(pv, inputs.barometric_pressure_inhg),
        mol_water_fraction: mole_fraction(
            inputs.rel_humidity,
            saturation,
            inputs.barometric_pressure_inhg,
        ),
    }
}

/// Process IDs the Java `MeteorologyGenerator.subscribeToMe` subscribes
/// to, in its declared order: Running Exhaust (1), Extended Idle Exhaust
/// (90), Start Exhaust (2), Auxiliary Power Exhaust (91), Tirewear (10),
/// Brakewear (9).
const SUBSCRIBED_PROCESS_IDS: [u16; 6] = [1, 90, 2, 91, 10, 9];

/// Default-DB tables [`MeteorologyGenerator`] reads — `ZoneMonthHour` for
/// temperature/humidity, `Zone` for the zone→county join, `County` for
/// barometric pressure and altitude.
static INPUT_TABLES: &[&str] = &["ZoneMonthHour", "Zone", "County"];

/// Tables [`MeteorologyGenerator`] writes — it augments `ZoneMonthHour` in
/// place with `heatIndex`, `specificHumidity`, and `molWaterFraction`.
static OUTPUT_TABLES: &[&str] = &["ZoneMonthHour"];

// ---- Input row types -------------------------------------------------------

fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::RowExtraction {
        table: table.into(),
        row,
        column: column.into(),
        message: msg,
    }
}

/// One `ZoneMonthHour` row read by [`MeteorologyGenerator`] — temperature and
/// relative humidity for a `(zone, month, hour)` cell.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MeteorologyZoneMonthHourRow {
    /// `zoneID` — the zone.
    pub zone_id: i32,
    /// `monthID` — calendar month.
    pub month_id: i32,
    /// `hourID` — hour of day.
    pub hour_id: i32,
    /// `temperature` — dry-bulb temperature, °F.
    pub temperature_f: f64,
    /// `relHumidity` — relative humidity, percent.
    pub rel_humidity: f64,
}

impl TableRow for MeteorologyZoneMonthHourRow {
    fn table_name() -> &'static str {
        "ZoneMonthHour"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("zoneID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
            ("temperature".into(), DataType::Float64),
            ("relHumidity".into(), DataType::Float64),
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
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourID".into(),
                    rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "temperature".into(),
                    rows.iter().map(|r| r.temperature_f).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "relHumidity".into(),
                    rows.iter().map(|r| r.rel_humidity).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "ZoneMonthHour";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let zone_id = get_i32("zoneID")?;
        let month_id = get_i32("monthID")?;
        let hour_id = get_i32("hourID")?;
        let temperature = get_f64("temperature")?;
        let rel_humidity = get_f64("relHumidity")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(MeteorologyZoneMonthHourRow {
                    zone_id: zone_id.get(i).ok_or_else(|| null("zoneID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    hour_id: hour_id.get(i).ok_or_else(|| null("hourID"))?,
                    temperature_f: temperature.get(i).ok_or_else(|| null("temperature"))?,
                    rel_humidity: rel_humidity.get(i).ok_or_else(|| null("relHumidity"))?,
                })
            })
            .collect()
    }
}

/// One `Zone` row read by [`MeteorologyGenerator`] — the zone→county mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MeteorologyZoneRow {
    /// `zoneID` — the zone primary key.
    pub zone_id: i32,
    /// `countyID` — the county the zone belongs to.
    pub county_id: i32,
}

impl TableRow for MeteorologyZoneRow {
    fn table_name() -> &'static str {
        "Zone"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("zoneID".into(), DataType::Int32),
            ("countyID".into(), DataType::Int32),
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
                    "countyID".into(),
                    rows.iter().map(|r| r.county_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "Zone";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let zone_id = get_i32("zoneID")?;
        let county_id = get_i32("countyID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(MeteorologyZoneRow {
                    zone_id: zone_id.get(i).ok_or_else(|| null("zoneID"))?,
                    county_id: county_id.get(i).ok_or_else(|| null("countyID"))?,
                })
            })
            .collect()
    }
}

/// One `County` row read by [`MeteorologyGenerator`] — the barometric pressure
/// and altitude class. Both fields are nullable in the MOVES default DB;
/// [`resolve_county_meteorology`] fills in defaults for `NULL` values.
#[derive(Debug, Clone, PartialEq)]
pub struct MeteorologyCountyRow {
    /// `countyID` — the county primary key.
    pub county_id: i32,
    /// `barometricPressure` — nullable `DOUBLE`, inches of mercury. `None`
    /// when the DB row has `NULL` or a non-positive value.
    pub barometric_pressure: Option<f64>,
    /// `altitude` — nullable `CHAR(1)`: `'H'` high / `'L'` low. `None` when
    /// the DB row has `NULL` or a character outside `('H','L')`.
    pub altitude: Option<char>,
}

impl TableRow for MeteorologyCountyRow {
    fn table_name() -> &'static str {
        "County"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("countyID".into(), DataType::Int32),
            ("barometricPressure".into(), DataType::Float64),
            ("altitude".into(), DataType::String),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "countyID".into(),
                    rows.iter().map(|r| r.county_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "barometricPressure".into(),
                    rows.iter()
                        .map(|r| r.barometric_pressure)
                        .collect::<Vec<Option<f64>>>(),
                )
                .into(),
                Series::new(
                    "altitude".into(),
                    rows.iter()
                        .map(|r| r.altitude.map(|c| c.to_string()))
                        .collect::<Vec<Option<String>>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "County";
        let county_id_col = df
            .column("countyID")
            .map_err(|e| row_err(t, 0, "countyID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "countyID", e.to_string()))?;
        let baro_col = df
            .column("barometricPressure")
            .map_err(|e| row_err(t, 0, "barometricPressure", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "barometricPressure", e.to_string()))?;
        let alt_col = df
            .column("altitude")
            .map_err(|e| row_err(t, 0, "altitude", e.to_string()))?
            .str()
            .map_err(|e| row_err(t, 0, "altitude", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(MeteorologyCountyRow {
                    county_id: county_id_col.get(i).ok_or_else(|| null("countyID"))?,
                    barometric_pressure: baro_col.get(i),
                    altitude: alt_col.get(i).and_then(|s| s.chars().next()),
                })
            })
            .collect()
    }
}

/// Inputs to [`MeteorologyGenerator::execute`] — the three default-DB tables
/// the generator reads.
#[derive(Debug, Clone, Default)]
pub struct MeteorologyInputs {
    /// `ZoneMonthHour` rows — one per `(zone, month, hour)` cell.
    pub zone_month_hour: Vec<MeteorologyZoneMonthHourRow>,
    /// `Zone` rows — the zone→county mapping.
    pub zone: Vec<MeteorologyZoneRow>,
    /// `County` rows — barometric pressure and altitude class.
    pub county: Vec<MeteorologyCountyRow>,
}

// ---- Output row type -------------------------------------------------------

/// One `ZoneMonthHour` row written to scratch by [`MeteorologyGenerator`] —
/// the original temperature/humidity inputs plus the three computed meteorology
/// columns.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MeteorologyOutputRow {
    /// `zoneID` — the zone.
    pub zone_id: i32,
    /// `monthID` — calendar month.
    pub month_id: i32,
    /// `hourID` — hour of day.
    pub hour_id: i32,
    /// `temperature` — dry-bulb temperature, °F (pass-through from input).
    pub temperature_f: f64,
    /// `relHumidity` — relative humidity, percent (pass-through from input).
    pub rel_humidity: f64,
    /// `heatIndex` — apparent temperature, °F.
    pub heat_index: f64,
    /// `specificHumidity` — g H2O / kg dry air.
    pub specific_humidity: f64,
    /// `molWaterFraction` — mol H2O / mol ambient air.
    pub mol_water_fraction: f64,
}

impl TableRow for MeteorologyOutputRow {
    fn table_name() -> &'static str {
        "ZoneMonthHour"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("zoneID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
            ("temperature".into(), DataType::Float64),
            ("relHumidity".into(), DataType::Float64),
            ("heatIndex".into(), DataType::Float64),
            ("specificHumidity".into(), DataType::Float64),
            ("molWaterFraction".into(), DataType::Float64),
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
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourID".into(),
                    rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "temperature".into(),
                    rows.iter().map(|r| r.temperature_f).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "relHumidity".into(),
                    rows.iter().map(|r| r.rel_humidity).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "heatIndex".into(),
                    rows.iter().map(|r| r.heat_index).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "specificHumidity".into(),
                    rows.iter()
                        .map(|r| r.specific_humidity)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "molWaterFraction".into(),
                    rows.iter()
                        .map(|r| r.mol_water_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "ZoneMonthHour";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let zone_id = get_i32("zoneID")?;
        let month_id = get_i32("monthID")?;
        let hour_id = get_i32("hourID")?;
        let temperature = get_f64("temperature")?;
        let rel_humidity = get_f64("relHumidity")?;
        let heat_index = get_f64("heatIndex")?;
        let specific_humidity = get_f64("specificHumidity")?;
        let mol_water_fraction = get_f64("molWaterFraction")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(MeteorologyOutputRow {
                    zone_id: zone_id.get(i).ok_or_else(|| null("zoneID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    hour_id: hour_id.get(i).ok_or_else(|| null("hourID"))?,
                    temperature_f: temperature.get(i).ok_or_else(|| null("temperature"))?,
                    rel_humidity: rel_humidity.get(i).ok_or_else(|| null("relHumidity"))?,
                    heat_index: heat_index.get(i).ok_or_else(|| null("heatIndex"))?,
                    specific_humidity: specific_humidity
                        .get(i)
                        .ok_or_else(|| null("specificHumidity"))?,
                    mol_water_fraction: mol_water_fraction
                        .get(i)
                        .ok_or_else(|| null("molWaterFraction"))?,
                })
            })
            .collect()
    }
}

// ---- Kernel ----------------------------------------------------------------

/// Drive the `doHeatIndex` pass over the input tables.
///
/// For each `ZoneMonthHour` row, joins to `Zone` → `County` to look up the
/// county's barometric pressure and altitude, applies
/// [`resolve_county_meteorology`] to fill defaults, then calls
/// [`compute_zone_month_hour`] to produce the three output columns.
///
/// Rows with no matching `Zone` or `County` entry are silently dropped —
/// the same behaviour as the Java's inner-join SQL.
pub fn build_meteorology_table(inputs: &MeteorologyInputs) -> Vec<MeteorologyOutputRow> {
    let zone_to_county: HashMap<i32, i32> = inputs
        .zone
        .iter()
        .map(|z| (z.zone_id, z.county_id))
        .collect();
    let county_map: HashMap<i32, &MeteorologyCountyRow> =
        inputs.county.iter().map(|c| (c.county_id, c)).collect();

    let mut rows = Vec::with_capacity(inputs.zone_month_hour.len());
    for zmh in &inputs.zone_month_hour {
        let Some(&county_id) = zone_to_county.get(&zmh.zone_id) else {
            continue;
        };
        let Some(&county) = county_map.get(&county_id) else {
            continue;
        };

        let altitude = county.altitude.and_then(|c| match c {
            'H' => Some(Altitude::High),
            'L' => Some(Altitude::Low),
            _ => None,
        });
        let resolved = resolve_county_meteorology(county.barometric_pressure, altitude);

        let out = compute_zone_month_hour(ZoneMonthHourInputs {
            temperature_f: zmh.temperature_f,
            rel_humidity: zmh.rel_humidity,
            barometric_pressure_inhg: resolved.barometric_pressure_inhg,
        });

        rows.push(MeteorologyOutputRow {
            zone_id: zmh.zone_id,
            month_id: zmh.month_id,
            hour_id: zmh.hour_id,
            temperature_f: zmh.temperature_f,
            rel_humidity: zmh.rel_humidity,
            heat_index: out.heat_index,
            specific_humidity: out.specific_humidity,
            mol_water_fraction: out.mol_water_fraction,
        });
    }
    rows
}

/// MOVES `MeteorologyGenerator` (migration plan Task 41).
///
/// Builds the meteorology fields of `ZoneMonthHour`. Holds no per-run
/// state: every input arrives through the [`CalculatorContext`] passed to
/// [`Generator::execute`].
#[derive(Debug, Default, Clone, Copy)]
pub struct MeteorologyGenerator;

/// Build the six `PROCESS`-granularity, `GENERATOR`-priority subscriptions.
///
/// `CalculatorSubscription::new` and `Priority::parse` are not `const`, so
/// this runs once behind the [`OnceLock`] in
/// [`MeteorologyGenerator::subscriptions`].
fn build_subscriptions() -> Vec<CalculatorSubscription> {
    let priority =
        Priority::parse("GENERATOR").expect("\"GENERATOR\" is a canonical MasterLoopPriority base");
    SUBSCRIBED_PROCESS_IDS
        .iter()
        .map(|&process_id| {
            CalculatorSubscription::new(ProcessId(process_id), Granularity::Process, priority)
        })
        .collect()
}

impl Generator for MeteorologyGenerator {
    fn name(&self) -> &'static str {
        "MeteorologyGenerator"
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        static SUBSCRIPTIONS: OnceLock<Vec<CalculatorSubscription>> = OnceLock::new();
        SUBSCRIPTIONS.get_or_init(build_subscriptions).as_slice()
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn output_tables(&self) -> &[&'static str] {
        OUTPUT_TABLES
    }

    fn execute(&self, ctx: &mut CalculatorContext) -> Result<CalculatorOutput, Error> {
        let inputs = MeteorologyInputs {
            zone_month_hour: ctx.tables().iter_typed("ZoneMonthHour")?,
            zone: ctx.tables().iter_typed("Zone")?,
            county: ctx.tables().iter_typed("County")?,
        };
        let rows = build_meteorology_table(&inputs);
        crate::wiring::write_scratch_table(ctx, OUTPUT_TABLES[0], rows)
    }
}

/// Construct a [`MeteorologyGenerator`] as a boxed trait object — the
/// `GeneratorFactory` shape engine wiring registers via
/// `CalculatorRegistry::register_generator`.
#[must_use]
pub fn factory() -> Box<dyn Generator> {
    Box::new(MeteorologyGenerator)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- heat_index ----

    #[test]
    fn heat_index_below_threshold_is_temperature() {
        // The `WHERE temperature < 78` UPDATE leaves heatIndex == temperature.
        assert_eq!(heat_index(70.0, 50.0), 70.0);
        assert_eq!(heat_index(32.0, 99.0), 32.0);
        assert_eq!(heat_index(-10.0, 0.0), -10.0);
    }

    #[test]
    fn heat_index_just_below_threshold_is_temperature() {
        assert_eq!(heat_index(77.999, 80.0), 77.999);
    }

    #[test]
    fn heat_index_at_threshold_uses_regression() {
        // temperature == 78 falls into the `>= 78.0` UPDATE, not the
        // passthrough, so the result is the polynomial — not 78.
        let hi = heat_index(78.0, 50.0);
        assert_ne!(hi, 78.0);
        assert!(hi.is_finite());
    }

    #[test]
    fn heat_index_matches_hand_computed_regression() {
        // T = 80 °F, RH = 40 %: the NWS regression summed term-by-term
        // left-to-right is 79.9293732 (independent hand calculation).
        let hi = heat_index(80.0, 40.0);
        assert!((hi - 79.9293732).abs() < 1e-6, "heat_index(80, 40) = {hi}");
    }

    #[test]
    fn heat_index_caps_at_120() {
        // Extreme heat drives the regression well past 120; the SQL
        // `least(..., 120)` clamps it.
        assert_eq!(heat_index(115.0, 95.0), 120.0);
        assert_eq!(heat_index(140.0, 100.0), 120.0);
    }

    // ---- fahrenheit_to_kelvin ----

    #[test]
    fn fahrenheit_to_kelvin_ice_point() {
        // 32 °F is exactly 273.15 K: (5/9)·0 + 273.15.
        assert_eq!(fahrenheit_to_kelvin(32.0), 273.15);
    }

    #[test]
    fn fahrenheit_to_kelvin_steam_point() {
        // 212 °F is 373.15 K (100 °C).
        assert!((fahrenheit_to_kelvin(212.0) - 373.15).abs() < 1e-9);
    }

    #[test]
    fn fahrenheit_to_kelvin_is_monotonic() {
        assert!(fahrenheit_to_kelvin(60.0) < fahrenheit_to_kelvin(90.0));
    }

    // ---- saturation_vapor_pressure ----

    #[test]
    fn saturation_vapor_pressure_at_ice_point() {
        // At TK = 273.15 every temperature term vanishes; the result is
        // 10^-0.2138602 ≈ 0.611 kPa, water's saturation pressure at 0 °C.
        let p = saturation_vapor_pressure(273.15);
        assert!((p - 0.611).abs() < 0.01, "PH2O(273.15) = {p}");
    }

    #[test]
    fn saturation_vapor_pressure_increases_with_temperature() {
        // Warmer air holds more water: PH2O must rise with TK.
        let cold = saturation_vapor_pressure(fahrenheit_to_kelvin(40.0));
        let warm = saturation_vapor_pressure(fahrenheit_to_kelvin(95.0));
        assert!(warm > cold);
        assert!(cold > 0.0);
    }

    // ---- vapor_partial_pressure / mole_fraction / specific_humidity ----

    #[test]
    fn vapor_partial_pressure_scales_with_humidity() {
        // PV = relHumidity/100 * PH2O.
        assert_eq!(vapor_partial_pressure(50.0, 2.0), 1.0);
        assert_eq!(vapor_partial_pressure(0.0, 2.0), 0.0);
        assert_eq!(vapor_partial_pressure(100.0, 2.0), 2.0);
    }

    #[test]
    fn mole_fraction_is_vapor_over_total_pressure() {
        // With relHumidity 100 %, XH2O = PH2O / (PB · 3.38639). Choosing
        // PH2O == PB·3.38639 makes the fraction exactly 1.
        let pb = 2.0;
        let ph2o = pb * INHG_TO_KPA;
        assert!((mole_fraction(100.0, ph2o, pb) - 1.0).abs() < 1e-12);
    }

    #[test]
    fn specific_humidity_zero_when_no_vapor() {
        assert_eq!(specific_humidity(0.0, 30.0), 0.0);
    }

    #[test]
    fn specific_humidity_is_positive_for_real_inputs() {
        // PB·3.38639 dominates PV for any realistic atmosphere, so the
        // denominator stays positive and specificHumidity is positive.
        let sh = specific_humidity(1.5, 29.0);
        assert!(sh > 0.0 && sh.is_finite(), "specific_humidity = {sh}");
    }

    // ---- resolve_county_meteorology ----

    #[test]
    fn county_keeps_valid_pressure_and_altitude() {
        let resolved = resolve_county_meteorology(Some(28.0), Some(Altitude::High));
        assert_eq!(resolved.barometric_pressure_inhg, 28.0);
        assert_eq!(resolved.altitude, Altitude::High);
    }

    #[test]
    fn county_missing_pressure_uses_high_altitude_default() {
        // NULL and non-positive pressures both trigger the fill-in.
        for missing in [None, Some(0.0), Some(-3.0)] {
            let resolved = resolve_county_meteorology(missing, Some(Altitude::High));
            assert_eq!(
                resolved.barometric_pressure_inhg,
                DEFAULT_PRESSURE_HIGH_ALTITUDE
            );
            assert_eq!(resolved.altitude, Altitude::High);
        }
    }

    #[test]
    fn county_missing_pressure_non_high_altitude_uses_low_default() {
        // SQL `CASE WHEN altitude='H' ... ELSE 28.94`: 'L' and NULL both
        // take the ELSE branch.
        let low = resolve_county_meteorology(None, Some(Altitude::Low));
        assert_eq!(low.barometric_pressure_inhg, DEFAULT_PRESSURE_LOW_ALTITUDE);
        assert_eq!(low.altitude, Altitude::Low);

        let null_altitude = resolve_county_meteorology(Some(-1.0), None);
        assert_eq!(
            null_altitude.barometric_pressure_inhg,
            DEFAULT_PRESSURE_LOW_ALTITUDE
        );
    }

    #[test]
    fn county_missing_altitude_derived_from_pressure() {
        // At least 25.8403 inHg classes Low, below it High.
        let low = resolve_county_meteorology(Some(30.0), None);
        assert_eq!(low.altitude, Altitude::Low);
        assert_eq!(low.barometric_pressure_inhg, 30.0);

        let high = resolve_county_meteorology(Some(20.0), None);
        assert_eq!(high.altitude, Altitude::High);
        assert_eq!(high.barometric_pressure_inhg, 20.0);
    }

    #[test]
    fn county_altitude_threshold_is_inclusive_low() {
        // Exactly 25.8403 inHg → Low (`barometricPressure >= 25.8403`).
        let resolved = resolve_county_meteorology(Some(ALTITUDE_PRESSURE_THRESHOLD), None);
        assert_eq!(resolved.altitude, Altitude::Low);
    }

    #[test]
    fn county_all_missing_falls_through_both_defaults() {
        // NULL pressure + NULL altitude: step 1 gives 28.94 (ELSE branch),
        // step 2 then classes 28.94 >= 25.8403 as Low.
        let resolved = resolve_county_meteorology(None, None);
        assert_eq!(
            resolved.barometric_pressure_inhg,
            DEFAULT_PRESSURE_LOW_ALTITUDE
        );
        assert_eq!(resolved.altitude, Altitude::Low);
    }

    // ---- compute_zone_month_hour ----

    #[test]
    fn compute_row_cold_uses_temperature_for_heat_index() {
        let out = compute_zone_month_hour(ZoneMonthHourInputs {
            temperature_f: 55.0,
            rel_humidity: 60.0,
            barometric_pressure_inhg: 29.0,
        });
        assert_eq!(out.heat_index, 55.0);
        assert!(out.specific_humidity > 0.0);
        assert!(out.mol_water_fraction > 0.0);
    }

    #[test]
    fn compute_row_is_consistent_with_component_functions() {
        let inputs = ZoneMonthHourInputs {
            temperature_f: 88.0,
            rel_humidity: 65.0,
            barometric_pressure_inhg: 29.92,
        };
        let out = compute_zone_month_hour(inputs);

        let tk = fahrenheit_to_kelvin(88.0);
        let ph2o = saturation_vapor_pressure(tk);
        let pv = vapor_partial_pressure(65.0, ph2o);
        assert_eq!(out.heat_index, heat_index(88.0, 65.0));
        assert_eq!(out.specific_humidity, specific_humidity(pv, 29.92));
        assert_eq!(out.mol_water_fraction, mole_fraction(65.0, ph2o, 29.92));
        // 88 °F at 65 % RH is a "feels hotter than it is" day.
        assert!(out.heat_index > 88.0);
    }

    // ---- Generator trait ----

    #[test]
    fn generator_name_matches_java_class() {
        assert_eq!(MeteorologyGenerator.name(), "MeteorologyGenerator");
    }

    #[test]
    fn generator_subscribes_at_process_granularity_generator_priority() {
        let subs = MeteorologyGenerator.subscriptions();
        assert_eq!(subs.len(), 6);
        for sub in subs {
            assert_eq!(sub.granularity, Granularity::Process);
            assert_eq!(sub.priority.display().as_str(), "GENERATOR");
        }
    }

    #[test]
    fn generator_subscription_process_ids_match_java_order() {
        // MeteorologyGenerator.subscribeToMe: Running Exhaust (1), Extended
        // Idle Exhaust (90), Start Exhaust (2), Auxiliary Power Exhaust
        // (91), Tirewear (10), Brakewear (9).
        let ids: Vec<u16> = MeteorologyGenerator
            .subscriptions()
            .iter()
            .map(|s| s.process_id.0)
            .collect();
        assert_eq!(ids, [1, 90, 2, 91, 10, 9]);
    }

    #[test]
    fn generator_declares_input_and_output_tables() {
        let gen = MeteorologyGenerator;
        assert_eq!(gen.input_tables(), &["ZoneMonthHour", "Zone", "County"]);
        assert_eq!(gen.output_tables(), &["ZoneMonthHour"]);
        // No upstream generators — MeteorologyGenerator is a root generator.
        assert!(gen.upstream().is_empty());
    }

    #[test]
    fn generator_subscriptions_are_stable_across_calls() {
        // The OnceLock-backed slice is identical on every call.
        let first = MeteorologyGenerator.subscriptions();
        let second = MeteorologyGenerator.subscriptions();
        assert_eq!(first, second);
    }

    #[test]
    fn execute_writes_computed_meteorology_to_scratch() {
        // Integration test: execute reads ZoneMonthHour/Zone/County from
        // ctx.tables(), runs the doHeatIndex kernel, and writes the augmented
        // ZoneMonthHour table to ctx.scratch(). Read it back and assert the
        // kernel's direct output matches.
        use moves_framework::{DataFrameStore, InMemoryStore};

        let zone_id = 261_610_i32;
        let county_id = 26_161_i32;
        let month_id = 7_i32;
        let hour_id = 8_i32;
        let temperature_f = 88.0_f64;
        let rel_humidity = 65.0_f64;
        let barometric_pressure = Some(29.92_f64);
        let altitude = Some("L".to_string());

        let mut store = InMemoryStore::new();

        store.insert(
            "ZoneMonthHour",
            MeteorologyZoneMonthHourRow::into_dataframe(vec![MeteorologyZoneMonthHourRow {
                zone_id,
                month_id,
                hour_id,
                temperature_f,
                rel_humidity,
            }])
            .unwrap(),
        );
        store.insert(
            "Zone",
            MeteorologyZoneRow::into_dataframe(vec![MeteorologyZoneRow { zone_id, county_id }])
                .unwrap(),
        );
        store.insert(
            "County",
            MeteorologyCountyRow::into_dataframe(vec![MeteorologyCountyRow {
                county_id,
                barometric_pressure,
                altitude: altitude.as_ref().and_then(|s| s.chars().next()),
            }])
            .unwrap(),
        );

        let mut ctx = CalculatorContext::with_tables(store);
        let out = MeteorologyGenerator.execute(&mut ctx).expect("execute ok");
        // Generator writes to scratch, not the main output.
        assert!(out.dataframe().is_none());

        // Read the scratch table back as typed rows.
        let rows: Vec<MeteorologyOutputRow> = ctx
            .scratch()
            .store
            .iter_typed("ZoneMonthHour")
            .expect("ZoneMonthHour in scratch");
        assert_eq!(rows.len(), 1);

        // Compare against what the kernel functions compute directly.
        let expected_county = resolve_county_meteorology(Some(29.92), Some(Altitude::Low));
        let expected = compute_zone_month_hour(ZoneMonthHourInputs {
            temperature_f,
            rel_humidity,
            barometric_pressure_inhg: expected_county.barometric_pressure_inhg,
        });

        let row = rows[0];
        assert_eq!(row.zone_id, zone_id);
        assert_eq!(row.month_id, month_id);
        assert_eq!(row.hour_id, hour_id);
        assert_eq!(row.temperature_f, temperature_f);
        assert_eq!(row.rel_humidity, rel_humidity);
        assert_eq!(row.heat_index, expected.heat_index);
        assert_eq!(row.specific_humidity, expected.specific_humidity);
        assert_eq!(row.mol_water_fraction, expected.mol_water_fraction);
    }

    #[test]
    fn execute_drops_zmh_rows_with_no_zone_match() {
        // ZoneMonthHour rows whose zoneID has no Zone entry are silently dropped.
        use moves_framework::{DataFrameStore, InMemoryStore};
        let mut store = InMemoryStore::new();
        store.insert(
            "ZoneMonthHour",
            MeteorologyZoneMonthHourRow::into_dataframe(vec![MeteorologyZoneMonthHourRow {
                zone_id: 99_999,
                month_id: 1,
                hour_id: 1,
                temperature_f: 55.0,
                rel_humidity: 50.0,
            }])
            .unwrap(),
        );
        // Zone table maps a different zone; County maps that county.
        store.insert(
            "Zone",
            MeteorologyZoneRow::into_dataframe(vec![MeteorologyZoneRow {
                zone_id: 1,
                county_id: 1,
            }])
            .unwrap(),
        );
        store.insert(
            "County",
            MeteorologyCountyRow::into_dataframe(vec![MeteorologyCountyRow {
                county_id: 1,
                barometric_pressure: None,
                altitude: None,
            }])
            .unwrap(),
        );
        let mut ctx = CalculatorContext::with_tables(store);
        MeteorologyGenerator.execute(&mut ctx).expect("execute ok");
        let rows: Vec<MeteorologyOutputRow> = ctx
            .scratch()
            .store
            .iter_typed("ZoneMonthHour")
            .expect("scratch table present");
        assert!(rows.is_empty(), "rows with no zone match must be dropped");
    }

    #[test]
    fn execute_applies_county_default_fill_for_null_pressure_and_altitude() {
        // When County has NULL barometricPressure and NULL altitude, the
        // default fill gives 28.94 inHg / Low — verify the output uses it.
        use moves_framework::{DataFrameStore, InMemoryStore};
        let mut store = InMemoryStore::new();
        store.insert(
            "ZoneMonthHour",
            MeteorologyZoneMonthHourRow::into_dataframe(vec![MeteorologyZoneMonthHourRow {
                zone_id: 1,
                month_id: 1,
                hour_id: 1,
                temperature_f: 55.0,
                rel_humidity: 50.0,
            }])
            .unwrap(),
        );
        store.insert(
            "Zone",
            MeteorologyZoneRow::into_dataframe(vec![MeteorologyZoneRow {
                zone_id: 1,
                county_id: 1,
            }])
            .unwrap(),
        );
        store.insert(
            "County",
            MeteorologyCountyRow::into_dataframe(vec![MeteorologyCountyRow {
                county_id: 1,
                barometric_pressure: None,
                altitude: None,
            }])
            .unwrap(),
        );
        let mut ctx = CalculatorContext::with_tables(store);
        MeteorologyGenerator.execute(&mut ctx).expect("execute ok");
        let rows: Vec<MeteorologyOutputRow> = ctx
            .scratch()
            .store
            .iter_typed("ZoneMonthHour")
            .expect("scratch table present");
        assert_eq!(rows.len(), 1);
        // 55 °F is below the 78 °F threshold so heat_index == temperature.
        assert_eq!(rows[0].heat_index, 55.0);
        // specific_humidity and mol_water_fraction must be positive.
        assert!(rows[0].specific_humidity > 0.0);
        assert!(rows[0].mol_water_fraction > 0.0);
    }

    #[test]
    fn factory_builds_a_named_generator() {
        assert_eq!(factory().name(), "MeteorologyGenerator");
    }

    #[test]
    fn generator_is_object_safe() {
        let gens: Vec<Box<dyn Generator>> = vec![factory(), Box::new(MeteorologyGenerator)];
        assert_eq!(gens.len(), 2);
        assert!(gens.iter().all(|g| g.name() == "MeteorologyGenerator"));
    }
}

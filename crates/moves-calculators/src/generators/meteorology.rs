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

use std::sync::OnceLock;

use moves_calculator_info::{Granularity, Priority};
use moves_data::ProcessId;
use moves_framework::{
    CalculatorContext, CalculatorOutput, CalculatorSubscription, Error, Generator,
};

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

    fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        // The numeric port (`compute_zone_month_hour`, `resolve_county_meteorology`)
        // is complete, but the `ZoneMonthHour` / `Zone` / `County` tables it
        // iterates do not materialise until the Task 50 data plane lands —
        // `CalculatorContext` exposes no row storage yet. Until then the
        // generator contributes no rows, matching every other Phase 2/3
        // module (Task 28's empty-output smoke test).
        Ok(CalculatorOutput::empty())
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
    fn generator_execute_is_ok() {
        // Phase 2/3 skeleton: execute returns an empty output until the
        // data plane lands. Smoke-test that it is callable and Ok.
        let ctx = CalculatorContext::new();
        assert!(MeteorologyGenerator.execute(&ctx).is_ok());
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

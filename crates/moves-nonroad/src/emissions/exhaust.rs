//! Exhaust emissions calculator (Task 106).
//!
//! Ports four tightly-coupled Fortran subroutines from
//! `NONROAD/NR08a/SOURCE/`:
//!
//! | Fortran | Rust |
//! |---|---|
//! | `unitcf.f` (80)   | [`unit_conversion_factor`]                                    |
//! | `emsadj.f` (343)  | [`calculate_emission_adjustments`]                            |
//! | `emfclc.f` (314)  | [`compute_emission_factor_for_tech`] (+ [`apply_deterioration`]) |
//! | `clcems.f` (360)  | [`calculate_exhaust_emissions`]                               |
//!
//! The Fortran code is the most numerically-sensitive piece of
//! NONROAD; the migration plan flags Task 106 for two weeks
//! including fixture-fidelity validation. The Rust port preserves
//! the order of arithmetic operations exactly so the
//! single-precision intermediate rounding stays bit-identical to
//! the Fortran reference (`f32` throughout — Fortran `real*4` is
//! IEEE 754 binary32).
//!
//! # COMMON-block decoupling
//!
//! The Fortran routines pull most of their inputs from COMMON
//! blocks (`/optdat/`, `/perdat/`, `/rfgdat/`, `/actdat/`, …). The
//! Rust port takes every value as an explicit function parameter
//! so the port is testable in isolation and so future Task 113
//! (driver loop) can wire it up against the eventual
//! `NonroadContext`. Each input bag carries the same fields the
//! Fortran COMMON would have supplied, named for clarity.
//!
//! # Numerical-fidelity policy
//!
//! All calculations use `f32` (single precision). The Fortran
//! source declares every variable `real*4`; matching the storage
//! type is required to produce the same rounding behaviour, which
//! is exactly what Task 115 validates against canonical fixtures.
//! Where the Fortran code multiplies several `real*4` quantities in
//! a specific order (e.g. `a * b * c * d` versus `(a * b) * (c * d)`),
//! the Rust port reproduces the original associativity.

use crate::common::consts::{
    CMFCNG, CMFDSL, CMFGAS, CMFLPG, CVTTON, GRMLB, MXAGYR, MXDAYS, MXPOL, MXTECH, RMISS,
};

// =============================================================================
// Pollutant indices
// =============================================================================

/// Pollutant indices used by NONROAD's exhaust calculator.
///
/// Mirrors the `IDX*` parameters in `nonrdprm.inc` (lines 299–349).
/// The numeric values match the 1-based Fortran indices; the
/// `slot()` method returns the 0-based array index used by Rust.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PollutantIndex {
    /// Total hydrocarbons (IDXTHC = 1).
    Thc = 1,
    /// Carbon monoxide (IDXCO = 2).
    Co = 2,
    /// Nitrogen oxides (IDXNOX = 3).
    Nox = 3,
    /// Carbon dioxide (IDXCO2 = 4).
    Co2 = 4,
    /// Sulfur oxides (IDXSOX = 5).
    Sox = 5,
    /// Particulate matter (IDXPM = 6).
    Pm = 6,
    /// Crankcase HC (IDXCRA = 7).
    Crankcase = 7,
    /// Diurnal evap (IDXDIU = 8). Skipped by the exhaust calc.
    Diurnal = 8,
    /// Tank permeation (IDXTKP = 9). Skipped by the exhaust calc.
    TankPerm = 9,
    /// Non-rec-marine hose permeation (IDXHOS = 10). Skipped.
    HosePerm = 10,
    /// Rec-marine fill-neck permeation (IDXNCK = 11). Skipped.
    NeckPerm = 11,
    /// Rec-marine supply/return permeation (IDXSR = 12). Skipped.
    SupplyReturnPerm = 12,
    /// Rec-marine vent permeation (IDXVNT = 13). Skipped.
    VentPerm = 13,
    /// Hot soak (IDXSOK = 14). Skipped.
    HotSoak = 14,
    /// Refueling displacement (IDXDIS = 15).
    Displacement = 15,
    /// Spillage (IDXSPL = 16). Skipped by the exhaust calc.
    Spillage = 16,
    /// Running loss (IDXRLS = 17). Skipped by the exhaust calc.
    RunningLoss = 17,
    /// Start emissions: THC (IDSTHC = 18).
    StartThc = 18,
    /// Start emissions: CO (IDSCO = 19).
    StartCo = 19,
    /// Start emissions: NOx (IDSNOX = 20).
    StartNox = 20,
    /// Start emissions: CO2 (IDSCO2 = 21).
    StartCo2 = 21,
    /// Start emissions: SOx (IDSSOX = 22).
    StartSox = 22,
    /// Start emissions: PM (IDSPM = 23).
    StartPm = 23,
}

impl PollutantIndex {
    /// 0-based slot index into per-pollutant arrays of length [`MXPOL`].
    #[inline]
    pub const fn slot(self) -> usize {
        self as usize - 1
    }

    /// 1-based Fortran index.
    #[inline]
    pub const fn fortran_index(self) -> usize {
        self as usize
    }
}

// =============================================================================
// Fuel
// =============================================================================

/// Fuel/engine types.
///
/// Mirrors the `IDXGS2..IDXCNG` parameters in `nonrdprm.inc`
/// (lines 154–162). The Fortran `ifuel` global is read from
/// `nonrdeqp.inc` `/eqpdat/`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FuelKind {
    /// 2-stroke gasoline (IDXGS2 = 1).
    Gasoline2Stroke = 1,
    /// 4-stroke gasoline (IDXGS4 = 2).
    Gasoline4Stroke = 2,
    /// Diesel (IDXDSL = 3).
    Diesel = 3,
    /// LPG (IDXLPG = 4).
    Lpg = 4,
    /// CNG (IDXCNG = 5).
    Cng = 5,
}

impl FuelKind {
    /// 1-based Fortran index, useful for indexing into per-fuel
    /// tables such as `soxbas`/`soxful`/`altfac`.
    #[inline]
    pub const fn fortran_index(self) -> usize {
        self as usize
    }
}

// =============================================================================
// Emission-factor units (mirrors IDXGHR..IDXMLT)
// =============================================================================

/// Emission-factor units code, as written by the upstream `.EMF`
/// parser into `iexhun(idxefc, idxspc)`.
///
/// Mirrors the `IDXGHR..IDXMLT` parameters in `nonrdefc.inc`
/// (lines 89–105).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EmissionUnitCode {
    /// `G/HR` — grams per hour (IDXGHR = 1).
    GramsPerHour = 1,
    /// `G/HP-HR` — grams per horsepower-hour (IDXGHP = 2).
    GramsPerHpHour = 2,
    /// `G/GALLON` — grams per gallon of fuel (IDXGAL = 3).
    GramsPerGallon = 3,
    /// `G/TANK` — grams per tank volume (IDXTNK = 4).
    GramsPerTank = 4,
    /// `G/DAY` — grams per day (IDXGDY = 5).
    GramsPerDay = 5,
    /// `G/START` — grams per engine start (IDXGST = 6).
    GramsPerStart = 6,
    /// `MULT` — unitless multiplier (IDXMLT = 7).
    Multiplier = 7,
    /// `G/M2/DAY` — grams per square metre per day (IDXGMD = 8).
    GramsPerM2Day = 8,
}

impl EmissionUnitCode {
    /// Fortran 1-based index.
    #[inline]
    pub const fn fortran_index(self) -> usize {
        self as usize
    }

    /// Construct from the Fortran 1-based index, returning `None`
    /// for values outside `1..=8`.
    pub const fn from_fortran(idx: u8) -> Option<Self> {
        match idx {
            1 => Some(Self::GramsPerHour),
            2 => Some(Self::GramsPerHpHour),
            3 => Some(Self::GramsPerGallon),
            4 => Some(Self::GramsPerTank),
            5 => Some(Self::GramsPerDay),
            6 => Some(Self::GramsPerStart),
            7 => Some(Self::Multiplier),
            8 => Some(Self::GramsPerM2Day),
            _ => None,
        }
    }
}

// =============================================================================
// unitcf.f — unit conversion factor
// =============================================================================

/// Activity-units code, as parsed by [`crate::input::activity`].
///
/// Mirrors the `IDXHRY..IDXGLD` parameters in `nonrdact.inc`
/// (lines 53–62) and the [`crate::input::activity::ActivityUnits`]
/// enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ActivityUnit {
    /// `HRS/YR` — hours per year (IDXHRY = 1).
    HoursPerYear = 1,
    /// `HRS/DAY` — hours per day (IDXHRD = 2).
    HoursPerDay = 2,
    /// `GAL/YR` — gallons per year (IDXGLY = 3).
    GallonsPerYear = 3,
    /// `GAL/DAY` — gallons per day (IDXGLD = 4).
    GallonsPerDay = 4,
}

/// Return the unit conversion factor for the given emission-factor
/// unit code. Ports `unitcf.f` (80 lines).
///
/// The Fortran function dispatches on the EF unit `index`:
///
/// - `IDXGHP` (`G/HP-HR`): convert g/hp-hr → g/hr by multiplying
///   by `hpavg * faclod(indlod)`.
/// - `IDXGAL` (`G/GALLON`): convert g/gallon → g/hr by
///   multiplying by `bsfc * faclod(indlod) * hpavg / density`.
///   The special case `iactun ∈ {IDXGLY, IDXGLD}` (activity is
///   already in gallon units) bypasses the conversion and returns
///   1.0. When `density == 0`, returns 0 to avoid divide-by-zero.
/// - `IDXGDY` (`G/DAY`): returns the days-per-year `FACGDY` (always 1.0).
/// - `IDXMLT` (`MULT`): returns 1.0 (unitless multiplier).
/// - All other codes (`G/HR`, `G/TANK`, `G/START`, `G/M2/DAY`):
///   returns 1.0 (the conversion is a no-op at this layer).
///
/// `activity_unit` corresponds to the Fortran `iactun(indlod)`
/// COMMON-block read; `load_factor` corresponds to
/// `faclod(indlod)`. The Rust port replaces `indlod`-based array
/// lookups with these two explicit parameters.
pub fn unit_conversion_factor(
    unit: EmissionUnitCode,
    hp_avg: f32,
    load_factor: f32,
    activity_unit: ActivityUnit,
    density: f32,
    bsfc: f32,
) -> f32 {
    match unit {
        EmissionUnitCode::GramsPerHpHour => hp_avg * load_factor,
        EmissionUnitCode::GramsPerGallon => match activity_unit {
            ActivityUnit::GallonsPerYear | ActivityUnit::GallonsPerDay => 1.0,
            _ => {
                if density == 0.0 {
                    0.0
                } else {
                    (bsfc * load_factor * hp_avg) / density
                }
            }
        },
        // FACGDY in nonrdefc.inc is hardcoded as 1.0; we hardcode the
        // result here to keep the port self-contained.
        EmissionUnitCode::GramsPerDay => 1.0,
        EmissionUnitCode::Multiplier => 1.0,
        // G/HR, G/TANK, G/START, G/M2/DAY: pass-through.
        _ => 1.0,
    }
}

// =============================================================================
// emsadj.f — emission adjustments
// =============================================================================

/// Season indicator used by `emsadj.f`'s RFG adjustment branch.
///
/// Mirrors `IDXWTR..IDXFAL` from `nonrdprm.inc` (lines 698–705).
/// Only Winter and Summer participate in RFG adjustments; Spring
/// and Fall are accepted for completeness but produce no RFG
/// effect (the Fortran source's `iseas ∈ {WTR, SUM}` gate).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Season {
    /// Winter season (IDXWTR = 1).
    Winter = 1,
    /// Spring season (IDXSPR = 2).
    Spring = 2,
    /// Summer season (IDXSUM = 3).
    Summer = 3,
    /// Fall season (IDXFAL = 4).
    Fall = 4,
}

/// One bin from `nonrdefc.inc`'s `rfggs2`/`rfggs4` RFG-adjustment
/// table, scoped to a single `(season, year-bin)` cell.
///
/// The Fortran reads `rfggs2(iseas, idxyr, IDX*)` for THC, CO, NOX,
/// SOX, PM; the Rust port carries those five values as named
/// fields. Year-bin selection is the caller's responsibility (the
/// Fortran scans `iyrbin(iseas, i, 1..2)` against `iepyr` to pick a
/// bin; the Rust port consumes the already-selected bin).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RfgBinFactors {
    /// THC multiplier (`rfggs*(iseas, idxyr, IDXTHC)`).
    pub thc: f32,
    /// CO multiplier.
    pub co: f32,
    /// NOx multiplier.
    pub nox: f32,
    /// SOx multiplier.
    pub sox: f32,
    /// PM multiplier.
    pub pm: f32,
}

/// Day range produced by the Fortran `dayloop` helper. The
/// `winter_skip_*` fields encode the "winter using daily values"
/// skip-rule (`dayloop` returns `lskip=.TRUE.` and the
/// `[skip_start, skip_end]` inclusive Julian day range to bypass).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DayRange {
    /// First Julian day in the period (inclusive, 1-based).
    pub begin_day: i32,
    /// Last Julian day in the period (inclusive, 1-based).
    pub end_day: i32,
    /// First Julian day of the skip window (only used when
    /// `winter_skip == true`).
    pub winter_skip_begin: i32,
    /// Last Julian day of the skip window (only used when
    /// `winter_skip == true`).
    pub winter_skip_end: i32,
    /// `true` when running a winter daily inventory; days in
    /// `[winter_skip_begin, winter_skip_end]` are skipped.
    pub winter_skip: bool,
}

/// Per-day temperature and day-of-year context.
///
/// Mirrors the Fortran block at `emsadj.f` :125–134: when
/// `ldayfl=TRUE`, the daily-temperature table `daytmp(jday, 3, ireg)`
/// is read; otherwise the constant `amtemp` is used.
#[derive(Debug, Clone)]
pub struct DailyTemperatures {
    /// `true` when daily temperature/RVP arrays are used
    /// (`ldayfl` in Fortran). When `false`, every day uses
    /// [`Self::ambient_temp`].
    pub daily_temperature_mode: bool,
    /// Per-day ambient temperature in °F when
    /// `daily_temperature_mode` is true. Length must be at least
    /// `day_range.end_day` (1-based Julian days; index `i` holds
    /// the temperature for day `i+1`).
    pub daily_ambient_temp_f: Vec<f32>,
    /// Single ambient temperature in °F used when
    /// `daily_temperature_mode` is false (Fortran `amtemp`).
    pub ambient_temp: f32,
}

/// All inputs to [`calculate_emission_adjustments`].
///
/// The Rust port collects what the Fortran reads from COMMON blocks
/// into one explicit input struct so the function is testable in
/// isolation. Each field's documentation cites the Fortran COMMON
/// variable it replaces.
#[derive(Debug, Clone)]
pub struct AdjustmentInputs<'a> {
    /// Fuel type (`ifuel` in `/eqpdat/`).
    pub fuel: FuelKind,
    /// SCC code of the equipment (`asccod`).
    pub scc: &'a str,
    /// 5-digit FIPS code (`code`).
    pub fips: &'a str,
    /// Day range (output of `dayloop`).
    pub day_range: DayRange,
    /// Daily temperature context (`/optdat/` daytmp + `ldayfl`).
    pub temperatures: &'a DailyTemperatures,
    /// Per-day month-fraction (`daymthfac`, only used when
    /// `temperatures.daily_temperature_mode` is true and the
    /// pollutant is activity-based — not diurnal, tank/hose
    /// permeation, or spillage).
    pub daily_month_fraction: &'a [f32],
    /// `true` when RFG adjustments should be applied (`lrfg`).
    pub rfg: bool,
    /// `true` when running in a high-altitude region (`lhigh`).
    pub high_altitude: bool,
    /// Oxygen weight-fraction in the fuel, percent (`oxypct`).
    pub oxygen_percent: f32,
    /// `iepyr` — episode year (used by RFG bin lookup).
    pub episode_year: i32,
    /// `imonth` (1..=12), or 0 when not in monthly mode. Drives
    /// `iseas = idseas(imonth)` for RFG.
    pub month: u8,
    /// `idseas` lookup (12 entries), produced by `read_seasonal`.
    /// Slot `i-1` holds the season for calendar month `i`.
    pub month_to_season: [Season; 12],
    /// RFG year-bin factors, pre-selected per season. The caller
    /// supplies the matching bin or `None` if the episode year is
    /// outside all bins (in which case no RFG adjustment is
    /// applied). Indexing: `[Season::Winter.slot(), Season::Summer.slot()]`
    /// — Spring and Fall are not used by the Fortran source.
    pub rfg_winter_2_stroke: Option<RfgBinFactors>,
    /// RFG winter factors for 4-stroke gasoline.
    pub rfg_winter_4_stroke: Option<RfgBinFactors>,
    /// RFG summer factors for 2-stroke gasoline.
    pub rfg_summer_2_stroke: Option<RfgBinFactors>,
    /// RFG summer factors for 4-stroke gasoline.
    pub rfg_summer_4_stroke: Option<RfgBinFactors>,
    /// Episode-fuel sulfur fraction for each fuel
    /// (`soxful(IDXGS2..IDXCNG)`).
    pub sox_fuel: [f32; 5],
    /// Base sulfur fraction for each fuel (`soxbas(IDXGS2..IDXCNG)`).
    pub sox_base: [f32; 5],
    /// Rec-marine diesel sulfur override (`soxdsm`). Only applies
    /// when the SCC's first 7 digits are `"2282020"` or
    /// `"2280002"`.
    pub sox_diesel_marine: f32,
    /// Altitude correction factors by fuel
    /// (`altfac(IDXGS2..IDXCNG)`).
    pub altitude_factor: [f32; 5],
}

/// Output of [`calculate_emission_adjustments`]: a per-(pollutant,
/// day) matrix of adjustment factors. Indexing matches Fortran's
/// `adjfac(idxspc, jday)` with both fields 1-based; the Rust slot is
/// `[pollutant.slot() * MXDAYS + day_index]`.
///
/// Wrapped in a struct rather than a bare `Vec<f32>` so the
/// dimensions are explicit and tested at construction time.
#[derive(Debug, Clone)]
pub struct AdjustmentTable {
    data: Vec<f32>,
    n_days: usize,
}

impl AdjustmentTable {
    /// Construct an all-ones table with `n_days` Julian days per
    /// pollutant. The factor `1.0` is the multiplicative identity
    /// that the per-correction passes layer on top of.
    pub fn new(n_days: usize) -> Self {
        Self {
            data: vec![1.0; MXPOL * n_days],
            n_days,
        }
    }

    /// Number of Julian days carried per pollutant.
    pub fn n_days(&self) -> usize {
        self.n_days
    }

    /// Read the adjustment factor for a `(pollutant, day_0based)`
    /// pair. `day_0based` is the 0-based offset within the day
    /// vector (i.e. Julian day minus 1).
    pub fn get(&self, pollutant: PollutantIndex, day_0based: usize) -> f32 {
        self.data[pollutant.slot() * self.n_days + day_0based]
    }

    fn set(&mut self, pollutant: PollutantIndex, day_0based: usize, value: f32) {
        self.data[pollutant.slot() * self.n_days + day_0based] = value;
    }
}

/// Calculate the per-(pollutant, day) emission-adjustment factors.
/// Ports `emsadj.f` (343 lines).
///
/// The Fortran source builds the `adjfac(MXPOL, MXDAYS)` array
/// from a stack of corrections:
///
/// 1. Initialize all entries to `1.0`.
/// 2. For activity-based pollutants (not diurnal IDX8..IDX13 and
///    not spillage IDX16), if `ldayfl`, multiply by the per-day
///    month-fraction `daymthfac(jday)`.
/// 3. Temperature corrections for THC/CO/NOx, branching on fuel
///    (4-stroke gasoline applies; 2-stroke gasoline has zero
///    coefficients — placeholder kept for future data).
/// 4. Oxygenate correction for THC/CO/NOx, only when `lrfg` is
///    false and fuel is gasoline.
/// 5. SOx sulfur correction, applied to IDXSOX; uses the
///    rec-marine diesel sulfur override for the SCC prefixes
///    `2282020*` / `2280002*`.
/// 6. Altitude correction for IDXTHC..IDXSOX, when `lhigh` and
///    fuel is one of the five known fuels.
/// 7. RFG correction for THC/CO/NOX/SOX/PM, only when `lrfg` and
///    `iseas ∈ {Winter, Summer}` and the year-bin lookup succeeded.
/// 8. Tank/hose permeation temperature corrections (IDXTKP, IDXHOS,
///    IDXNCK, IDXSR, IDXVNT).
///
/// Steps 7 and 8 fire even when fuel is non-gasoline (the Fortran
/// source does not gate them by fuel — only by `lrfg`). The Rust
/// port preserves that.
pub fn calculate_emission_adjustments(inputs: &AdjustmentInputs<'_>) -> AdjustmentTable {
    let n_days = MXDAYS;
    let mut table = AdjustmentTable::new(n_days);

    let range = inputs.day_range;
    let begin = range.begin_day.max(1);
    let end = range.end_day.min(n_days as i32);

    for jday in begin..=end {
        if range.winter_skip && jday >= range.winter_skip_begin && jday <= range.winter_skip_end {
            continue;
        }

        // Day index for the per-day month-fraction lookup
        // (0-based offset).
        let jday_idx = (jday - 1) as usize;

        let tamb: f32 = if inputs.temperatures.daily_temperature_mode {
            inputs.temperatures.daily_ambient_temp_f[jday_idx]
        } else {
            inputs.temperatures.ambient_temp
        };

        // The temperature/oxygenate/altitude/RFG/permeation logic
        // multiplies INTO the existing adjfac (which starts at 1.0).
        // Mirror that by computing each factor and folding it in.

        // --- Step 2: per-day month-fraction for activity-based EFs ---
        // emsadj.f :144–151. Skipped when ldayfl=false, when the
        // pollutant index is in IDXDIU..IDXVNT (8..=13) or equals
        // IDXSPL (16). The Fortran loops `i=1,MXPOL`; the Rust
        // port mirrors that exact loop.
        if inputs.temperatures.daily_temperature_mode {
            let factor = inputs.daily_month_fraction[jday_idx];
            for pol_one_based in 1..=MXPOL {
                if (8..=13).contains(&pol_one_based) || pol_one_based == 16 {
                    continue;
                }
                set_by_one_based(&mut table, pol_one_based, jday_idx, factor);
            }
        }

        // --- Step 3: temperature corrections for THC/CO/NOX ---
        // emsadj.f :167–194 (4-stroke) and :198–220 (2-stroke).
        match inputs.fuel {
            FuelKind::Gasoline4Stroke => {
                let (a_thc, a_co, a_nox) = if tamb <= 75.0 {
                    (-0.00240_f32, 0.0015784_f32, -0.00892_f32)
                } else {
                    (0.00132_f32, 0.00375_f32, -0.00873_f32)
                };
                let dt = tamb - 75.0;
                multiply(
                    &mut table,
                    PollutantIndex::Thc,
                    jday_idx,
                    (a_thc * dt).exp(),
                );
                multiply(&mut table, PollutantIndex::Co, jday_idx, (a_co * dt).exp());
                multiply(
                    &mut table,
                    PollutantIndex::Nox,
                    jday_idx,
                    (a_nox * dt).exp(),
                );
            }
            FuelKind::Gasoline2Stroke => {
                // Coefficients are 0.0 — exp(0) = 1, so the
                // multiply is a no-op. Mirror it exactly so a future
                // data update (changing the coefficients) only needs
                // to edit one place.
                let dt = tamb - 75.0;
                multiply(
                    &mut table,
                    PollutantIndex::Thc,
                    jday_idx,
                    (0.0_f32 * dt).exp(),
                );
                multiply(
                    &mut table,
                    PollutantIndex::Co,
                    jday_idx,
                    (0.0_f32 * dt).exp(),
                );
                if tamb >= 75.0 {
                    // The Fortran writes NOx only on the >=75 branch.
                    multiply(
                        &mut table,
                        PollutantIndex::Nox,
                        jday_idx,
                        (0.0_f32 * dt).exp(),
                    );
                }
                // Note: the <75 branch in the Fortran does not
                // adjust NOx (no `acoeff` assignment). We mirror
                // that by leaving NOx untouched in that branch.
            }
            _ => {}
        }

        // --- Step 4: oxygenate correction for THC/CO/NOX ---
        // emsadj.f :228–256. Only when !lrfg and gasoline.
        if !inputs.rfg {
            if let FuelKind::Gasoline4Stroke = inputs.fuel {
                let oxy = inputs.oxygen_percent;
                multiply(&mut table, PollutantIndex::Thc, jday_idx, 1.0 - 0.045 * oxy);
                multiply(&mut table, PollutantIndex::Co, jday_idx, 1.0 - 0.062 * oxy);
                multiply(
                    &mut table,
                    PollutantIndex::Nox,
                    jday_idx,
                    1.0 - (-0.115) * oxy,
                );
            }
            if let FuelKind::Gasoline2Stroke = inputs.fuel {
                let oxy = inputs.oxygen_percent;
                multiply(&mut table, PollutantIndex::Thc, jday_idx, 1.0 - 0.006 * oxy);
                multiply(&mut table, PollutantIndex::Co, jday_idx, 1.0 - 0.065 * oxy);
                multiply(
                    &mut table,
                    PollutantIndex::Nox,
                    jday_idx,
                    1.0 - (-0.186) * oxy,
                );
            }
        }

        // --- Step 5: sulfur correction for SOx ---
        // emsadj.f :260–271. Only when !lrfg and fuel is one of the
        // five fuels (i.e. always in this enum). The rec-marine
        // override applies to SCC prefixes "2282020" / "2280002".
        if !inputs.rfg {
            let fuel_slot = inputs.fuel.fortran_index() - 1;
            let base = inputs.sox_base[fuel_slot];
            let mut soxcor = inputs.sox_fuel[fuel_slot] / base;
            if inputs.scc.starts_with("2282020") || inputs.scc.starts_with("2280002") {
                soxcor = inputs.sox_diesel_marine / base;
            }
            multiply(&mut table, PollutantIndex::Sox, jday_idx, soxcor);
        }

        // --- Step 6: altitude correction for THC..SOX ---
        // emsadj.f :275–279.
        if inputs.high_altitude {
            let fuel_slot = inputs.fuel.fortran_index() - 1;
            let af = inputs.altitude_factor[fuel_slot];
            for &p in &[
                PollutantIndex::Thc,
                PollutantIndex::Co,
                PollutantIndex::Nox,
                PollutantIndex::Co2,
                PollutantIndex::Sox,
            ] {
                multiply(&mut table, p, jday_idx, af);
            }
        }

        // --- Step 7: RFG correction ---
        // emsadj.f :283–316.
        if inputs.rfg && inputs.month >= 1 && inputs.month <= 12 {
            let iseas = inputs.month_to_season[inputs.month as usize - 1];
            if matches!(iseas, Season::Winter | Season::Summer) {
                let bin = match (inputs.fuel, iseas) {
                    (FuelKind::Gasoline2Stroke, Season::Winter) => inputs.rfg_winter_2_stroke,
                    (FuelKind::Gasoline2Stroke, Season::Summer) => inputs.rfg_summer_2_stroke,
                    (FuelKind::Gasoline4Stroke, Season::Winter) => inputs.rfg_winter_4_stroke,
                    (FuelKind::Gasoline4Stroke, Season::Summer) => inputs.rfg_summer_4_stroke,
                    _ => None,
                };
                if let Some(b) = bin {
                    multiply(&mut table, PollutantIndex::Thc, jday_idx, b.thc);
                    multiply(&mut table, PollutantIndex::Co, jday_idx, b.co);
                    multiply(&mut table, PollutantIndex::Nox, jday_idx, b.nox);
                    multiply(&mut table, PollutantIndex::Sox, jday_idx, b.sox);
                    multiply(&mut table, PollutantIndex::Pm, jday_idx, b.pm);
                }
            }
        }

        // --- Step 8: tank/hose permeation temperature corrections ---
        // emsadj.f :321–327. Always applied (no fuel/lrfg gating).
        let tkp = 3.788519e-2_f32 * (3.850818e-2_f32 * tamb).exp();
        let hos = 6.013899e-2_f32 * (3.850818e-2_f32 * tamb).exp();
        multiply(&mut table, PollutantIndex::TankPerm, jday_idx, tkp);
        multiply(&mut table, PollutantIndex::HosePerm, jday_idx, hos);
        // The Fortran assigns NCK/SR/VNT to the *post-multiply* HOS
        // value (line 325–327: `adjfac(IDXNCK,jday) = adjfac(IDXHOS,jday)`).
        let hos_value = table.get(PollutantIndex::HosePerm, jday_idx);
        table.set(PollutantIndex::NeckPerm, jday_idx, hos_value);
        table.set(PollutantIndex::SupplyReturnPerm, jday_idx, hos_value);
        table.set(PollutantIndex::VentPerm, jday_idx, hos_value);
    }

    table
}

#[inline]
fn multiply(table: &mut AdjustmentTable, p: PollutantIndex, day_idx: usize, factor: f32) {
    let cur = table.get(p, day_idx);
    table.set(p, day_idx, cur * factor);
}

#[inline]
fn set_by_one_based(table: &mut AdjustmentTable, pol_one_based: usize, day_idx: usize, value: f32) {
    let slot = pol_one_based - 1;
    table.data[slot * table.n_days + day_idx] = value;
}

// =============================================================================
// emfclc.f / fnddet — emission-factor lookup
// =============================================================================

/// Deterioration coefficients for one `(pollutant, tech)` pair.
///
/// Mirrors the per-record (`detavl`, `detbvl`, `capdet`) triplet
/// from `nonrdefc.inc` `/detdat/`.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct DeteriorationCoefficients {
    /// A-coefficient (`detavl`). Zero means no deterioration.
    pub a: f32,
    /// B-coefficient (`detbvl`). Defaults to `1.0`.
    pub b: f32,
    /// Cap on the age used by `1 + A * age^B`.
    pub cap: f32,
}

impl DeteriorationCoefficients {
    /// Default "no deterioration" coefficients, matching the
    /// Fortran initializer at `emfclc.f` :122–125.
    pub const fn none() -> Self {
        Self {
            a: 0.0,
            b: 1.0,
            cap: 0.0,
        }
    }
}

/// Per-tech emission-factor lookup context.
///
/// Mirrors the inputs to one inner iteration of `emfclc.f`: for a
/// given `(pollutant, tech)` pair, the caller supplies the EF
/// value and its unit code (the result of `fndefc(...)`), plus the
/// deterioration coefficients (the result of `fnddet(...)`). The
/// upstream parsers and lookups land in Tasks 96 and 101.
#[derive(Debug, Clone, Copy)]
pub struct EmissionFactorContext {
    /// Emission-factor value from the table (`exhfac(idxefc, idxspc)`).
    /// `None` means no record was found — the calculator treats
    /// this as `RMISS` for tech fractions > 0, and as 0 otherwise.
    pub factor: Option<f32>,
    /// Units code (`iexhun(idxefc, idxspc)`). Ignored when `factor`
    /// is `None`.
    pub unit: EmissionUnitCode,
    /// Deterioration coefficients (`detavl`/`detbvl`/`capdet`),
    /// or [`DeteriorationCoefficients::none`] when no
    /// deterioration record exists for this `(tech, pollutant)`
    /// pair.
    pub deterioration: DeteriorationCoefficients,
}

/// Populate the per-`(tech)` emission factor and deterioration
/// outputs for a single pollutant, mirroring the inner body of
/// `emfclc.f`'s species loop (`emfclc.f` :201–294).
///
/// The Fortran routine handles the whole `(BSFC + exhaust species)`
/// fan-out and the per-tech lookup search; the Rust port factors
/// that responsibility out to two layers:
///
/// 1. The driver (Task 113) walks the tech list and performs the
///    `fndefc`/`fnddet` lookups (Task 101 + Task 96 land those
///    helpers).
/// 2. This function takes the already-resolved
///    [`EmissionFactorContext`] for one `(year, pollutant, tech)`
///    slot and writes the corresponding cells of the output
///    arrays.
///
/// Returns the missing-record flag — `true` when the lookup
/// produced no record and the tech fraction is positive (so the
/// caller should emit the same warning the Fortran writes via
/// `chkwrn(IDXWEM)`).
#[allow(clippy::too_many_arguments)]
pub fn compute_emission_factor_for_tech(
    ctx: &EmissionFactorContext,
    tech_fraction: f32,
    year_index: usize,
    pollutant: PollutantIndex,
    tech_index: usize,
    emission_factors: &mut [f32],
    unit_codes: &mut [EmissionUnitCode],
    adetcf: &mut [f32],
    bdetcf: &mut [f32],
    detcap: &mut [f32],
) -> bool {
    let cell = year_index * (MXPOL * MXTECH) + pollutant.slot() * MXTECH + tech_index;
    let det_cell = pollutant.slot() * MXTECH + tech_index;

    match ctx.factor {
        Some(value) => {
            emission_factors[cell] = value;
            unit_codes[det_cell] = ctx.unit;
            adetcf[det_cell] = ctx.deterioration.a;
            bdetcf[det_cell] = if ctx.deterioration.b == 0.0 {
                1.0
            } else {
                ctx.deterioration.b
            };
            detcap[det_cell] = ctx.deterioration.cap;
            false
        }
        None if tech_fraction > 0.0 => {
            // Mirrors emfclc.f :270–276: when no factor is found
            // and the tech fraction is positive, emit the warning.
            // The EF cell stays at its initialised value of RMISS,
            // matching the Fortran's reliance on the pre-init loop
            // (`emfclc.f` :115–127) which writes RMISS into the
            // EF cell whenever tecfrc > 0.
            emission_factors[cell] = RMISS;
            true
        }
        None => {
            // tech_fraction == 0: leave the EF at 0 (the Fortran's
            // pre-init loop writes 0.0 into the EF cell when
            // tecfrc == 0). Don't warn.
            emission_factors[cell] = 0.0;
            false
        }
    }
}

/// Apply the deterioration multiplier `1 + A * age^B`, capping the
/// `age` argument at `cap` when it exceeds.
///
/// Mirrors `clcems.f` :185–191. The capping is done on the age
/// argument, not on the resulting multiplier — important when
/// `B != 1`.
pub fn apply_deterioration(coef: &DeteriorationCoefficients, age: f32) -> f32 {
    let effective_age = if age <= coef.cap { age } else { coef.cap };
    1.0 + coef.a * effective_age.powf(coef.b)
}

// =============================================================================
// clcems.f — top-level exhaust emissions calculation
// =============================================================================

/// Per-pollutant alternate-sulfur lookup, replacing the
/// `(sultec, sulalt, sulcnv)` arrays from `nonrdusr.inc`.
///
/// The Fortran source does `fndchr(tectyp(idxtch, idxtec), 10,
/// sultec, numalt)` to find the alternate row. The Rust port lets
/// the caller pass the already-matched row (or `None` when no
/// alternate applies).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct SulfurAlternate {
    /// Alternate base sulfur level (`sulalt`), or negative when
    /// not specified.
    pub alternate_base: f32,
    /// Alternate sulfur conversion factor (`sulcnv`), or negative
    /// when not specified.
    pub alternate_conversion: f32,
}

/// All inputs to [`calculate_exhaust_emissions`].
///
/// The Rust port collects what `clcems.f` reads from COMMON blocks
/// and its argument list into one explicit struct so the function
/// is testable in isolation.
///
/// Dimensioning conventions:
///
/// - `emission_factors` is row-major `[year][pollutant][tech]`
///   matching the Fortran `emsfac(MXAGYR, MXPOL, MXTECH)` array.
///   Total length: `MXAGYR * MXPOL * MXTECH`.
/// - `adetcf`/`bdetcf`/`detcap` are row-major `[pollutant][tech]`
///   matching `adetcf(MXPOL, MXTECH)` etc. Total length:
///   `MXPOL * MXTECH`.
/// - `unit_codes` is row-major `[pollutant][tech]` matching
///   `idxunt(MXPOL, MXTECH)`.
/// - `daily_adjustments` is the output of
///   [`calculate_emission_adjustments`].
#[derive(Debug)]
pub struct ExhaustCalcInputs<'a> {
    /// Year index in the emission-factor arrays (`idxyr`, 1-based
    /// in Fortran; this field is 0-based).
    pub year_index: usize,
    /// Tech-type index in the emission-factor arrays (`idxtec`,
    /// 0-based).
    pub tech_index: usize,
    /// Index into `tchfrc` for the current SCC (`idxtch`, 0-based).
    /// Used by the SOx/PM alternate-sulfur lookup.
    pub scc_tech_index: usize,
    /// Age of the equipment for deterioration calc (`dage`).
    pub equipment_age: f32,
    /// Deterioration cap by `[pollutant][tech]` (`detcap`).
    pub detcap: &'a [f32],
    /// Deterioration A-coefficient by `[pollutant][tech]` (`adetcf`).
    pub adetcf: &'a [f32],
    /// Deterioration B-coefficient by `[pollutant][tech]` (`bdetcf`).
    pub bdetcf: &'a [f32],
    /// EF unit codes by `[pollutant][tech]` (`idxunt`).
    pub unit_codes: &'a [EmissionUnitCode],
    /// Technology-type fraction for the current `(year, tech)`
    /// (`tfrac`).
    pub tech_fraction: f32,
    /// Average horsepower (`hpval`).
    pub hp_avg: f32,
    /// Fuel density (`denful`).
    pub fuel_density: f32,
    /// BSFC value for this `(year, tech)` (`bsfval`).
    pub bsfc: f32,
    /// Activity-record index (`iact`, 0-based).
    pub activity_index: usize,
    /// Load factor `faclod(iact)`.
    pub load_factor: f32,
    /// Activity units `iactun(iact)`.
    pub activity_unit: ActivityUnit,
    /// Daily emission adjustments (output of
    /// [`calculate_emission_adjustments`]).
    pub daily_adjustments: &'a AdjustmentTable,
    /// Time-period adjustment factor (`adjtime`).
    pub adjustment_time: f32,
    /// Day range (output of `dayloop`).
    pub day_range: DayRange,
    /// Mutable EF array, indexed `[year][pollutant][tech]`. The
    /// SOx and CO2 branches REWRITE entries of this array (see
    /// `clcems.f` :226 and :256). The CRA branch reads but does
    /// not write.
    pub emission_factors: &'a mut [f32],
    /// Starts-per-hour adjustment (`sadj`).
    pub starts_adjustment: f32,
    /// Temporal adjustment factor (`tpltmp`). The Fortran source
    /// stores it in `tpltmp2` (a local copy) to avoid mutating the
    /// caller's value — the Rust port just shadows the input.
    pub temporal_adjustment: f32,
    /// Equipment population (`pop`).
    pub population: f32,
    /// Model-year fraction (`mfrac`).
    pub model_year_fraction: f32,
    /// Period day count (`ndays`).
    pub n_days: i32,
    /// Activity adjustment (`afac`).
    pub activity_adjustment: f32,
    /// Tech-fraction array for the current SCC, indexed
    /// `[scc_tech_index * MXTECH + tech_index]`. Replaces the
    /// Fortran `tchfrc(idxtch, idxtec)` COMMON-block read.
    pub tech_fractions_table: &'a [f32],
    /// Retrofit reduction fractions by pollutant slot (`MXPOL`
    /// entries). The Fortran reads `rtrftplltntrdfrc(idxspc)` from
    /// the COMMON block; the Rust port takes the array directly.
    pub retrofit_reduction: &'a [f32],
    /// Fuel kind (`ifuel`).
    pub fuel: FuelKind,
    /// SOx conversion factor by fuel (`soxfrc`).
    pub sox_conversion: [f32; 5],
    /// Base sulfur level by fuel (`soxbas`).
    pub sox_base: [f32; 5],
    /// Alternate sulfur lookup for this `(scc_tech_index, tech_index)`,
    /// or `None` for no alternate.
    pub sulfur_alternate: Option<SulfurAlternate>,
}

/// Outputs of [`calculate_exhaust_emissions`].
#[derive(Debug, Clone, PartialEq)]
pub struct ExhaustCalcOutputs {
    /// Per-pollutant emissions for the current day-range
    /// (`emsday(MXPOL)`). Length: [`MXPOL`].
    pub emissions_day: Vec<f32>,
    /// Per-pollutant emissions accumulated across model years
    /// (`emsbmy(MXPOL)`). Length: [`MXPOL`].
    pub emissions_by_model_year: Vec<f32>,
}

impl Default for ExhaustCalcOutputs {
    fn default() -> Self {
        Self {
            emissions_day: vec![0.0; MXPOL],
            emissions_by_model_year: vec![0.0; MXPOL],
        }
    }
}

/// Calculate exhaust emissions for one
/// `(SCC, year, tech, activity)` iteration. Ports `clcems.f`
/// (360 lines).
///
/// The Fortran source:
///
/// 1. Loops over Julian days in the period (skipping winter-summer
///    days when in winter-daily mode).
/// 2. For each day, loops over the pollutants `IDXTHC..MXPOL`,
///    skipping the diurnal/permeation range and pollutants without
///    an emission-factor file (except CO2, SOx, DIS — which are
///    always computed).
/// 3. Computes a deterioration multiplier `1 + A * age^B`, capped
///    at `detcap` (capping the age argument).
/// 4. Multiplies the EF by the unit conversion, the deterioration
///    multiplier, the day's emission adjustment, and the time-period
///    adjustment.
/// 5. Handles three special-case pollutants:
///    - **THC**: saves the un-adjusted product `EF * cvt * det` for
///      later use by SOx and CRA.
///    - **SOx**: rewrites the EF using
///      `hp*load * (BSFC*453.6*(1-soxcnv) - thcEF*cvtbck) *
///      0.01 * soxbas * 2`, then applies the day's adjustments.
///    - **CRA**: multiplies EF by the saved THC EF before adjustment.
///    - **CO2**: rewrites the EF using
///      `hp*load * (BSFC*453.6 - thcEF*cvtbck) * cfrac * 44/12`,
///      then applies the day's adjustments.
/// 6. For PM diesel: subtracts a sulfur correction
///    `bsfval*453.6*hp*load*7*soxcnv*0.01*adjtime * (sulbas*adjPM
///    - soxbas*adjSOx)` to account for sulfur-driven PM.
/// 7. Multiplies by `pop * mfrac * tchfrc * tpltmp2` and either
///    `sadj` (for IDSTHC..IDSPM start emissions) or
///    `ndays`/`afac` (for normal pollutants, depending on unit).
/// 8. Applies the retrofit reduction `(1 - retro(idxspc))`.
/// 9. Converts to tons via `CVTTON` and accumulates into
///    `emsday`/`emsbmy`.
pub fn calculate_exhaust_emissions(
    inputs: &mut ExhaustCalcInputs<'_>,
    pollutant_filter: &PollutantFilter,
) -> ExhaustCalcOutputs {
    let mut outputs = ExhaustCalcOutputs::default();

    let tpltmp2 = inputs.temporal_adjustment;
    let mut ems_thc: f32 = 0.0;

    let range = inputs.day_range;
    let begin = range.begin_day.max(1);
    let end = range.end_day.min(MXDAYS as i32);

    let fuel_slot = inputs.fuel.fortran_index() - 1;
    let sox_conv_base = inputs.sox_conversion[fuel_slot];
    let cfrac = match inputs.fuel {
        FuelKind::Gasoline2Stroke | FuelKind::Gasoline4Stroke => CMFGAS,
        FuelKind::Diesel => CMFDSL,
        FuelKind::Lpg => CMFLPG,
        FuelKind::Cng => CMFCNG,
    };

    for jday in begin..=end {
        if range.winter_skip && jday >= range.winter_skip_begin && jday <= range.winter_skip_end {
            continue;
        }
        let jday_idx = (jday - 1) as usize;

        for idxspc in 1..=MXPOL {
            // Skip diurnal..RLS (8..=17) — these are not exhaust.
            if (8..=17).contains(&idxspc) {
                continue;
            }
            let Some(pollutant) = pollutant_from_one_based(idxspc) else {
                continue;
            };

            // Skip if no emission-factor file was supplied AND the
            // pollutant is not one of CO2/SOX/DIS (which are always
            // computed). Mirrors clcems.f :179–181.
            if !pollutant_filter.has_factor_file(pollutant)
                && pollutant != PollutantIndex::Co2
                && pollutant != PollutantIndex::Sox
                && pollutant != PollutantIndex::Displacement
            {
                continue;
            }

            // Deterioration: cap age, then compute 1 + A * age^B.
            let det_cell = pollutant.slot() * MXTECH + inputs.tech_index;
            let det = DeteriorationCoefficients {
                a: inputs.adetcf[det_cell],
                b: inputs.bdetcf[det_cell],
                cap: inputs.detcap[det_cell],
            };
            let detrat = apply_deterioration(&det, inputs.equipment_age);

            let unit = inputs.unit_codes[det_cell];
            let cvttmp = unit_conversion_factor(
                unit,
                inputs.hp_avg,
                inputs.load_factor,
                inputs.activity_unit,
                inputs.fuel_density,
                inputs.bsfc,
            );

            let ef_cell = ef_cell(inputs.year_index, pollutant, inputs.tech_index);
            let adjems = inputs.daily_adjustments.get(pollutant, jday_idx);
            let mut emstmp = inputs.emission_factors[ef_cell]
                * cvttmp
                * detrat
                * adjems
                * inputs.adjustment_time;

            // Save the un-adjusted THC product for later SOx/CRA use.
            if pollutant == PollutantIndex::Thc {
                ems_thc = inputs.emission_factors[ef_cell] * cvttmp * detrat;
            }

            // --- SOx: rewrite EF and recompute emstmp. ---
            // clcems.f :213–232.
            if pollutant == PollutantIndex::Sox && inputs.tech_fraction > 0.0 {
                let cvtbck = if inputs.hp_avg * inputs.load_factor == 0.0 {
                    0.0
                } else {
                    1.0 / (inputs.hp_avg * inputs.load_factor)
                };
                let mut soxcnv = sox_conv_base;
                if let Some(alt) = inputs.sulfur_alternate {
                    if alt.alternate_conversion >= 0.0 {
                        soxcnv = alt.alternate_conversion;
                    }
                }
                let new_ef = inputs.hp_avg
                    * inputs.load_factor
                    * (inputs.bsfc * GRMLB as f32 * (1.0 - soxcnv) - ems_thc * cvtbck)
                    * 0.01
                    * inputs.sox_base[fuel_slot]
                    * 2.0;
                inputs.emission_factors[ef_cell] = new_ef;
                emstmp = new_ef * adjems * inputs.adjustment_time;
            }

            // --- Crankcase HC ---
            // clcems.f :236–241. Multiplies the EF (already a
            // MULT-type fraction) by ems_thc, the un-adjusted THC
            // product.
            if pollutant == PollutantIndex::Crankcase
                && inputs.tech_fraction > 0.0
                && ems_thc_factor_positive(
                    inputs.emission_factors,
                    inputs.year_index,
                    inputs.tech_index,
                )
                && inputs.emission_factors[ef_cell] > 0.0
            {
                emstmp =
                    inputs.emission_factors[ef_cell] * ems_thc * adjems * inputs.adjustment_time;
            }

            // --- CO2: rewrite EF as a function of BSFC. ---
            // clcems.f :245–261.
            if pollutant == PollutantIndex::Co2 && inputs.tech_fraction > 0.0 {
                let cvtbck = if inputs.hp_avg * inputs.load_factor == 0.0 {
                    0.0
                } else {
                    1.0 / (inputs.hp_avg * inputs.load_factor)
                };
                let new_ef = inputs.hp_avg
                    * inputs.load_factor
                    * (inputs.bsfc * GRMLB as f32 - ems_thc * cvtbck)
                    * cfrac
                    * 44.0
                    / 12.0;
                inputs.emission_factors[ef_cell] = new_ef;
                emstmp = new_ef * detrat * adjems * inputs.adjustment_time;
            }

            // --- PM diesel sulfur correction ---
            // clcems.f :267–302.
            if pollutant == PollutantIndex::Pm && inputs.fuel == FuelKind::Diesel {
                let dsl_slot = FuelKind::Diesel.fortran_index() - 1;
                let mut sulbas = inputs.sox_base[dsl_slot];
                let mut soxcnv = sox_conv_base;
                if let Some(alt) = inputs.sulfur_alternate {
                    if alt.alternate_base >= 0.0 {
                        sulbas = alt.alternate_base;
                    }
                    if alt.alternate_conversion >= 0.0 {
                        soxcnv = alt.alternate_conversion;
                    }
                }
                if sulbas != 1.0 {
                    let adj_pm = inputs.daily_adjustments.get(PollutantIndex::Pm, jday_idx);
                    let adj_sox = inputs.daily_adjustments.get(PollutantIndex::Sox, jday_idx);
                    emstmp -= inputs.bsfc
                        * GRMLB as f32
                        * inputs.hp_avg
                        * inputs.load_factor
                        * 7.0
                        * soxcnv
                        * 0.01
                        * inputs.adjustment_time
                        * (sulbas * adj_pm - inputs.sox_base[dsl_slot] * adj_sox);
                }
            }

            // --- Starts vs. activity-based emissions ---
            // clcems.f :306–320.
            let tchfrc =
                inputs.tech_fractions_table[inputs.scc_tech_index * MXTECH + inputs.tech_index];
            let emiss = if (pollutant as u8) >= (PollutantIndex::StartThc as u8) {
                emstmp
                    * inputs.starts_adjustment
                    * tpltmp2
                    * inputs.population
                    * inputs.model_year_fraction
                    * tchfrc
            } else if unit == EmissionUnitCode::GramsPerDay {
                emstmp
                    * (inputs.n_days as f32)
                    * tpltmp2
                    * inputs.population
                    * inputs.model_year_fraction
                    * tchfrc
            } else {
                emstmp
                    * inputs.activity_adjustment
                    * tpltmp2
                    * inputs.population
                    * inputs.model_year_fraction
                    * tchfrc
            };

            // --- Missing-value propagation ---
            // clcems.f :322–337. If the EF is missing OR emsday is
            // already RMISS-flagged, propagate RMISS.
            if inputs.emission_factors[ef_cell] < 0.0
                || outputs.emissions_day[pollutant.slot()] < 0.0
            {
                outputs.emissions_day[pollutant.slot()] = RMISS;
                outputs.emissions_by_model_year[pollutant.slot()] = RMISS;
                continue;
            }

            // --- Retrofit reduction (clcems.f :330–332) ---
            let retro = inputs.retrofit_reduction[pollutant.slot()];
            let mut emiss = emiss;
            if retro > 0.0 {
                emiss *= 1.0 - retro;
            }

            let temiss = emiss * CVTTON;
            outputs.emissions_day[pollutant.slot()] += temiss;
            outputs.emissions_by_model_year[pollutant.slot()] += temiss;
        }
    }

    outputs
}

/// Per-pollutant boolean: has the `(SCC, pollutant)` an EF file
/// been loaded? Replaces the Fortran COMMON-block `lfacfl(idxspc)`
/// array. Length: [`MXPOL`].
#[derive(Debug, Clone, Default)]
pub struct PollutantFilter {
    has_file: Vec<bool>,
}

impl PollutantFilter {
    /// All pollutants off — no exhaust file loaded.
    pub fn empty() -> Self {
        Self {
            has_file: vec![false; MXPOL],
        }
    }

    /// All pollutants on (useful for tests).
    pub fn all() -> Self {
        Self {
            has_file: vec![true; MXPOL],
        }
    }

    /// Set the flag for one pollutant.
    pub fn set(mut self, pollutant: PollutantIndex, on: bool) -> Self {
        self.has_file[pollutant.slot()] = on;
        self
    }

    /// Read the flag for one pollutant.
    pub fn has_factor_file(&self, pollutant: PollutantIndex) -> bool {
        self.has_file[pollutant.slot()]
    }
}

#[inline]
fn ef_cell(year_index: usize, pollutant: PollutantIndex, tech_index: usize) -> usize {
    year_index * (MXPOL * MXTECH) + pollutant.slot() * MXTECH + tech_index
}

#[inline]
fn ems_thc_factor_positive(emission_factors: &[f32], year_index: usize, tech_index: usize) -> bool {
    let cell = ef_cell(year_index, PollutantIndex::Thc, tech_index);
    emission_factors[cell] > 0.0
}

fn pollutant_from_one_based(idx: usize) -> Option<PollutantIndex> {
    match idx {
        1 => Some(PollutantIndex::Thc),
        2 => Some(PollutantIndex::Co),
        3 => Some(PollutantIndex::Nox),
        4 => Some(PollutantIndex::Co2),
        5 => Some(PollutantIndex::Sox),
        6 => Some(PollutantIndex::Pm),
        7 => Some(PollutantIndex::Crankcase),
        8 => Some(PollutantIndex::Diurnal),
        9 => Some(PollutantIndex::TankPerm),
        10 => Some(PollutantIndex::HosePerm),
        11 => Some(PollutantIndex::NeckPerm),
        12 => Some(PollutantIndex::SupplyReturnPerm),
        13 => Some(PollutantIndex::VentPerm),
        14 => Some(PollutantIndex::HotSoak),
        15 => Some(PollutantIndex::Displacement),
        16 => Some(PollutantIndex::Spillage),
        17 => Some(PollutantIndex::RunningLoss),
        18 => Some(PollutantIndex::StartThc),
        19 => Some(PollutantIndex::StartCo),
        20 => Some(PollutantIndex::StartNox),
        21 => Some(PollutantIndex::StartCo2),
        22 => Some(PollutantIndex::StartSox),
        23 => Some(PollutantIndex::StartPm),
        _ => None,
    }
}

// =============================================================================
// Tests
// =============================================================================

#[doc(hidden)]
#[allow(dead_code)]
const _MXAGYR_REFERENCED: usize = MXAGYR;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::consts::{SWTDSL, SWTGS2, SWTGS4};

    // ---- unitcf ----

    #[test]
    fn unitcf_gram_per_hp_hr_returns_hp_times_load() {
        let v = unit_conversion_factor(
            EmissionUnitCode::GramsPerHpHour,
            75.0,
            0.5,
            ActivityUnit::HoursPerYear,
            6.237,
            0.4,
        );
        assert!((v - 75.0 * 0.5).abs() < 1e-6);
    }

    #[test]
    fn unitcf_gallon_normal_uses_bsfc_load_hp_density() {
        // (bsfc * load * hp) / density = (0.4 * 0.5 * 75) / 6.237
        let v = unit_conversion_factor(
            EmissionUnitCode::GramsPerGallon,
            75.0,
            0.5,
            ActivityUnit::HoursPerYear,
            6.237,
            0.4,
        );
        let expected = (0.4_f32 * 0.5_f32 * 75.0_f32) / 6.237_f32;
        assert!((v - expected).abs() < 1e-6);
    }

    #[test]
    fn unitcf_gallon_with_gallon_activity_returns_one() {
        for au in [ActivityUnit::GallonsPerYear, ActivityUnit::GallonsPerDay] {
            let v =
                unit_conversion_factor(EmissionUnitCode::GramsPerGallon, 75.0, 0.5, au, 6.237, 0.4);
            assert_eq!(v, 1.0);
        }
    }

    #[test]
    fn unitcf_gallon_zero_density_returns_zero() {
        let v = unit_conversion_factor(
            EmissionUnitCode::GramsPerGallon,
            75.0,
            0.5,
            ActivityUnit::HoursPerYear,
            0.0,
            0.4,
        );
        assert_eq!(v, 0.0);
    }

    #[test]
    fn unitcf_mult_and_gphr_return_one() {
        for unit in [
            EmissionUnitCode::Multiplier,
            EmissionUnitCode::GramsPerHour,
            EmissionUnitCode::GramsPerDay,
            EmissionUnitCode::GramsPerStart,
            EmissionUnitCode::GramsPerTank,
            EmissionUnitCode::GramsPerM2Day,
        ] {
            let v = unit_conversion_factor(unit, 75.0, 0.5, ActivityUnit::HoursPerYear, 6.237, 0.4);
            assert_eq!(v, 1.0, "unit {:?} should produce 1.0", unit);
        }
    }

    // ---- deterioration ----

    #[test]
    fn deterioration_uncapped_evaluates_curve() {
        let coef = DeteriorationCoefficients {
            a: 0.1,
            b: 1.0,
            cap: 100.0,
        };
        let v = apply_deterioration(&coef, 5.0);
        assert!((v - 1.5).abs() < 1e-6);
    }

    #[test]
    fn deterioration_caps_at_cap_when_age_exceeds() {
        let coef = DeteriorationCoefficients {
            a: 0.1,
            b: 1.0,
            cap: 2.0,
        };
        // age=5 should be capped to age=2 → 1 + 0.1*2 = 1.2.
        let v = apply_deterioration(&coef, 5.0);
        assert!((v - 1.2).abs() < 1e-6);
    }

    #[test]
    fn deterioration_below_cap_uncapped() {
        let coef = DeteriorationCoefficients {
            a: 0.05,
            b: 1.5,
            cap: 10.0,
        };
        let v = apply_deterioration(&coef, 4.0);
        let expected = 1.0 + 0.05 * 4.0_f32.powf(1.5);
        assert!((v - expected).abs() < 1e-6);
    }

    #[test]
    fn deterioration_no_a_returns_one() {
        let coef = DeteriorationCoefficients::none();
        let v = apply_deterioration(&coef, 50.0);
        assert!((v - 1.0).abs() < 1e-6);
    }

    // ---- compute_emission_factor_for_tech ----

    fn ef_grid() -> Vec<f32> {
        // 51 years × 23 pollutants × 15 techs = 17,595 cells.
        vec![0.0; MXAGYR * MXPOL * MXTECH]
    }

    fn det_grid_f32() -> Vec<f32> {
        vec![0.0; MXPOL * MXTECH]
    }

    fn unit_grid() -> Vec<EmissionUnitCode> {
        vec![EmissionUnitCode::Multiplier; MXPOL * MXTECH]
    }

    #[test]
    fn ef_lookup_writes_value_and_no_warning() {
        let mut efs = ef_grid();
        let mut units = unit_grid();
        let mut a = det_grid_f32();
        let mut b = det_grid_f32();
        let mut cap = det_grid_f32();
        let ctx = EmissionFactorContext {
            factor: Some(2.5),
            unit: EmissionUnitCode::GramsPerHpHour,
            deterioration: DeteriorationCoefficients {
                a: 0.04,
                b: 1.0,
                cap: 1.5,
            },
        };
        let warned = compute_emission_factor_for_tech(
            &ctx,
            0.4,
            3,
            PollutantIndex::Thc,
            2,
            &mut efs,
            &mut units,
            &mut a,
            &mut b,
            &mut cap,
        );
        assert!(!warned);
        let cell = ef_cell(3, PollutantIndex::Thc, 2);
        assert!((efs[cell] - 2.5).abs() < 1e-6);
        let det_cell = PollutantIndex::Thc.slot() * MXTECH + 2;
        assert_eq!(units[det_cell], EmissionUnitCode::GramsPerHpHour);
        assert!((a[det_cell] - 0.04).abs() < 1e-6);
        assert!((b[det_cell] - 1.0).abs() < 1e-6);
        assert!((cap[det_cell] - 1.5).abs() < 1e-6);
    }

    #[test]
    fn ef_lookup_missing_with_positive_frac_warns_and_sets_rmiss() {
        let mut efs = ef_grid();
        let mut units = unit_grid();
        let mut a = det_grid_f32();
        let mut b = det_grid_f32();
        let mut cap = det_grid_f32();
        let ctx = EmissionFactorContext {
            factor: None,
            unit: EmissionUnitCode::Multiplier,
            deterioration: DeteriorationCoefficients::none(),
        };
        let warned = compute_emission_factor_for_tech(
            &ctx,
            0.5,
            0,
            PollutantIndex::Co,
            1,
            &mut efs,
            &mut units,
            &mut a,
            &mut b,
            &mut cap,
        );
        assert!(warned);
        let cell = ef_cell(0, PollutantIndex::Co, 1);
        assert_eq!(efs[cell], RMISS);
    }

    #[test]
    fn ef_lookup_missing_with_zero_frac_silently_zeros() {
        let mut efs = ef_grid();
        efs[ef_cell(0, PollutantIndex::Co, 1)] = 9.0; // pre-populate
        let mut units = unit_grid();
        let mut a = det_grid_f32();
        let mut b = det_grid_f32();
        let mut cap = det_grid_f32();
        let ctx = EmissionFactorContext {
            factor: None,
            unit: EmissionUnitCode::Multiplier,
            deterioration: DeteriorationCoefficients::none(),
        };
        let warned = compute_emission_factor_for_tech(
            &ctx,
            0.0,
            0,
            PollutantIndex::Co,
            1,
            &mut efs,
            &mut units,
            &mut a,
            &mut b,
            &mut cap,
        );
        assert!(!warned);
        let cell = ef_cell(0, PollutantIndex::Co, 1);
        assert_eq!(efs[cell], 0.0);
    }

    // ---- emsadj ----

    fn make_inputs<'a>(
        fuel: FuelKind,
        rfg: bool,
        high_altitude: bool,
        scc: &'a str,
        daily_mfrac: &'a Vec<f32>,
        temperatures: &'a DailyTemperatures,
    ) -> AdjustmentInputs<'a> {
        AdjustmentInputs {
            fuel,
            scc,
            fips: "06001",
            day_range: DayRange {
                begin_day: 1,
                end_day: 1,
                winter_skip_begin: 0,
                winter_skip_end: 0,
                winter_skip: false,
            },
            temperatures,
            daily_month_fraction: daily_mfrac,
            rfg,
            high_altitude,
            oxygen_percent: 2.0,
            episode_year: 2020,
            month: 6,
            month_to_season: [
                Season::Winter,
                Season::Winter,
                Season::Spring,
                Season::Spring,
                Season::Spring,
                Season::Summer,
                Season::Summer,
                Season::Summer,
                Season::Fall,
                Season::Fall,
                Season::Fall,
                Season::Winter,
            ],
            rfg_winter_2_stroke: None,
            rfg_winter_4_stroke: None,
            rfg_summer_2_stroke: None,
            rfg_summer_4_stroke: None,
            sox_fuel: [SWTGS2, SWTGS4, 0.05, SWTGS2, SWTGS2],
            sox_base: [SWTGS2, SWTGS4, SWTDSL, SWTGS2, SWTGS2],
            sox_diesel_marine: 0.15,
            altitude_factor: [1.1, 1.2, 1.3, 1.4, 1.5],
        }
    }

    #[test]
    fn emsadj_all_factors_default_to_one_when_nothing_applies() {
        // Use a non-gasoline fuel (LPG) with no RFG/altitude, ambient = 75 F.
        let daily_temps: Vec<f32> = vec![75.0; MXDAYS];
        let daily_mfrac: Vec<f32> = vec![1.0; MXDAYS];
        let temperatures = DailyTemperatures {
            daily_temperature_mode: false,
            daily_ambient_temp_f: daily_temps.clone(),
            ambient_temp: 75.0,
        };
        let inputs = make_inputs(
            FuelKind::Lpg,
            false,
            false,
            "2270001000",
            &daily_mfrac,
            &temperatures,
        );
        let t = calculate_emission_adjustments(&inputs);
        // THC/CO/NOx untouched (LPG): all 1.0.
        assert!((t.get(PollutantIndex::Thc, 0) - 1.0).abs() < 1e-6);
        assert!((t.get(PollutantIndex::Co, 0) - 1.0).abs() < 1e-6);
        assert!((t.get(PollutantIndex::Nox, 0) - 1.0).abs() < 1e-6);
        // SOx: sox_fuel[lpg]/sox_base[lpg] = SWTGS2/SWTGS2 = 1.0.
        assert!((t.get(PollutantIndex::Sox, 0) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn emsadj_temp_4stroke_below_75_applies_thc_co_nox() {
        let daily_temps: Vec<f32> = vec![50.0; MXDAYS];
        let daily_mfrac: Vec<f32> = vec![1.0; MXDAYS];
        let temperatures = DailyTemperatures {
            daily_temperature_mode: false,
            daily_ambient_temp_f: daily_temps.clone(),
            ambient_temp: 50.0,
        };
        let inputs = make_inputs(
            FuelKind::Gasoline4Stroke,
            true,
            // disable oxygenate path
            false,
            "2270001000",
            &daily_mfrac,
            &temperatures,
        );
        let t = calculate_emission_adjustments(&inputs);
        // dT = 50 - 75 = -25
        let expected_thc = (-0.00240_f32 * -25.0_f32).exp();
        let expected_co = (0.0015784_f32 * -25.0_f32).exp();
        let expected_nox = (-0.00892_f32 * -25.0_f32).exp();
        assert!((t.get(PollutantIndex::Thc, 0) - expected_thc).abs() < 1e-6);
        assert!((t.get(PollutantIndex::Co, 0) - expected_co).abs() < 1e-6);
        assert!((t.get(PollutantIndex::Nox, 0) - expected_nox).abs() < 1e-6);
    }

    #[test]
    fn emsadj_oxygenate_only_when_no_rfg_and_gasoline() {
        let daily_temps: Vec<f32> = vec![75.0; MXDAYS];
        let daily_mfrac: Vec<f32> = vec![1.0; MXDAYS];
        let temperatures = DailyTemperatures {
            daily_temperature_mode: false,
            daily_ambient_temp_f: daily_temps.clone(),
            ambient_temp: 75.0,
        };
        let inputs = make_inputs(
            FuelKind::Gasoline4Stroke,
            false,
            false,
            "2270001000",
            &daily_mfrac,
            &temperatures,
        );
        let t = calculate_emission_adjustments(&inputs);
        // dT=0 so temp factor = exp(0) = 1, then oxygenate at oxypct=2:
        // THC: 1 - 0.045*2 = 0.91
        // CO:  1 - 0.062*2 = 0.876
        // NOx: 1 + 0.115*2 = 1.23
        assert!((t.get(PollutantIndex::Thc, 0) - 0.91).abs() < 1e-5);
        assert!((t.get(PollutantIndex::Co, 0) - 0.876).abs() < 1e-5);
        assert!((t.get(PollutantIndex::Nox, 0) - 1.23).abs() < 1e-5);
    }

    #[test]
    fn emsadj_sox_uses_marine_override_for_rec_marine_scc() {
        let daily_temps: Vec<f32> = vec![75.0; MXDAYS];
        let daily_mfrac: Vec<f32> = vec![1.0; MXDAYS];
        let temperatures = DailyTemperatures {
            daily_temperature_mode: false,
            daily_ambient_temp_f: daily_temps.clone(),
            ambient_temp: 75.0,
        };
        let mut inputs = make_inputs(
            FuelKind::Diesel,
            false,
            false,
            "2282020001",
            &daily_mfrac,
            &temperatures,
        );
        inputs.sox_fuel[FuelKind::Diesel.fortran_index() - 1] = 0.05;
        let t = calculate_emission_adjustments(&inputs);
        // soxcor = soxdsm / soxbas[diesel] = 0.15 / SWTDSL = 0.15 / 0.33
        let expected = 0.15_f32 / SWTDSL;
        assert!((t.get(PollutantIndex::Sox, 0) - expected).abs() < 1e-5);
    }

    #[test]
    fn emsadj_altitude_applies_to_thc_through_sox() {
        let daily_temps: Vec<f32> = vec![75.0; MXDAYS];
        let daily_mfrac: Vec<f32> = vec![1.0; MXDAYS];
        let temperatures = DailyTemperatures {
            daily_temperature_mode: false,
            daily_ambient_temp_f: daily_temps.clone(),
            ambient_temp: 75.0,
        };
        let inputs = make_inputs(
            FuelKind::Diesel,
            true,
            // disable oxygenate
            true,
            "2270001000",
            &daily_mfrac,
            &temperatures,
        );
        let t = calculate_emission_adjustments(&inputs);
        // altitude_factor[diesel] = 1.3
        assert!((t.get(PollutantIndex::Thc, 0) - 1.3).abs() < 1e-5);
        assert!((t.get(PollutantIndex::Co, 0) - 1.3).abs() < 1e-5);
        assert!((t.get(PollutantIndex::Nox, 0) - 1.3).abs() < 1e-5);
        assert!((t.get(PollutantIndex::Co2, 0) - 1.3).abs() < 1e-5);
        // SOx is 1.0 (the rfg=true branch skips sulfur, so SOx ×= 1
        // before altitude). After altitude: 1.3.
        assert!((t.get(PollutantIndex::Sox, 0) - 1.3).abs() < 1e-5);
        // PM is untouched by altitude (only THC..SOX = 1..=5).
        assert!((t.get(PollutantIndex::Pm, 0) - 1.0).abs() < 1e-5);
    }

    #[test]
    fn emsadj_permeation_temp_factors_applied() {
        let daily_temps: Vec<f32> = vec![85.0; MXDAYS];
        let daily_mfrac: Vec<f32> = vec![1.0; MXDAYS];
        let temperatures = DailyTemperatures {
            daily_temperature_mode: false,
            daily_ambient_temp_f: daily_temps.clone(),
            ambient_temp: 85.0,
        };
        let inputs = make_inputs(
            FuelKind::Lpg,
            true,
            false,
            "2270001000",
            &daily_mfrac,
            &temperatures,
        );
        let t = calculate_emission_adjustments(&inputs);
        let expected_tkp = 3.788519e-2_f32 * (3.850818e-2_f32 * 85.0_f32).exp();
        let expected_hos = 6.013899e-2_f32 * (3.850818e-2_f32 * 85.0_f32).exp();
        assert!((t.get(PollutantIndex::TankPerm, 0) - expected_tkp).abs() < 1e-6);
        assert!((t.get(PollutantIndex::HosePerm, 0) - expected_hos).abs() < 1e-6);
        // NCK/SR/VNT all copy from HOS (post-multiply value).
        assert!((t.get(PollutantIndex::NeckPerm, 0) - expected_hos).abs() < 1e-6);
        assert!((t.get(PollutantIndex::SupplyReturnPerm, 0) - expected_hos).abs() < 1e-6);
        assert!((t.get(PollutantIndex::VentPerm, 0) - expected_hos).abs() < 1e-6);
    }

    #[test]
    fn emsadj_winter_skip_leaves_skipped_days_at_one() {
        let daily_temps: Vec<f32> = vec![85.0; MXDAYS];
        let daily_mfrac: Vec<f32> = vec![1.0; MXDAYS];
        let temperatures = DailyTemperatures {
            daily_temperature_mode: false,
            daily_ambient_temp_f: daily_temps.clone(),
            ambient_temp: 85.0,
        };
        let mut inputs = make_inputs(
            FuelKind::Diesel,
            false,
            false,
            "2270001000",
            &daily_mfrac,
            &temperatures,
        );
        inputs.day_range = DayRange {
            begin_day: 1,
            end_day: 10,
            winter_skip_begin: 3,
            winter_skip_end: 8,
            winter_skip: true,
        };
        let t = calculate_emission_adjustments(&inputs);
        // Days 3..=8 are skipped → remain 1.0.
        for d in 2..=7 {
            assert_eq!(t.get(PollutantIndex::TankPerm, d), 1.0);
        }
        // Days 1, 2, 9, 10 get the permeation factor.
        let expected_tkp = 3.788519e-2_f32 * (3.850818e-2_f32 * 85.0_f32).exp();
        for d in [0, 1, 8, 9] {
            assert!((t.get(PollutantIndex::TankPerm, d) - expected_tkp).abs() < 1e-6);
        }
    }

    #[test]
    fn emsadj_rfg_applies_for_winter_summer_only() {
        let daily_temps: Vec<f32> = vec![75.0; MXDAYS];
        let daily_mfrac: Vec<f32> = vec![1.0; MXDAYS];
        let temperatures = DailyTemperatures {
            daily_temperature_mode: false,
            daily_ambient_temp_f: daily_temps.clone(),
            ambient_temp: 75.0,
        };
        let mut inputs = make_inputs(
            FuelKind::Gasoline4Stroke,
            true,
            false,
            "2270001000",
            &daily_mfrac,
            &temperatures,
        );
        inputs.rfg_summer_4_stroke = Some(RfgBinFactors {
            thc: 0.9,
            co: 0.95,
            nox: 0.98,
            sox: 1.05,
            pm: 0.97,
        });
        inputs.month = 6; // June → Summer per month_to_season.
        let t = calculate_emission_adjustments(&inputs);
        assert!((t.get(PollutantIndex::Thc, 0) - 0.9).abs() < 1e-6);
        assert!((t.get(PollutantIndex::Pm, 0) - 0.97).abs() < 1e-6);

        // Switch to spring → no RFG bin applies.
        inputs.month = 4;
        let t2 = calculate_emission_adjustments(&inputs);
        assert_eq!(t2.get(PollutantIndex::Thc, 0), 1.0);
        assert_eq!(t2.get(PollutantIndex::Pm, 0), 1.0);
    }

    // ---- calculate_exhaust_emissions ----

    #[allow(clippy::too_many_arguments)]
    fn make_calc_inputs<'a>(
        emission_factors: &'a mut [f32],
        adetcf: &'a [f32],
        bdetcf: &'a [f32],
        detcap: &'a [f32],
        unit_codes: &'a [EmissionUnitCode],
        adj: &'a AdjustmentTable,
        tch_table: &'a [f32],
        retro: &'a [f32],
        day_range: DayRange,
    ) -> ExhaustCalcInputs<'a> {
        ExhaustCalcInputs {
            year_index: 0,
            tech_index: 0,
            scc_tech_index: 0,
            equipment_age: 1.0,
            detcap,
            adetcf,
            bdetcf,
            unit_codes,
            tech_fraction: 1.0,
            hp_avg: 75.0,
            fuel_density: 6.237,
            bsfc: 0.4,
            activity_index: 0,
            load_factor: 0.5,
            activity_unit: ActivityUnit::HoursPerYear,
            daily_adjustments: adj,
            adjustment_time: 1.0,
            day_range,
            emission_factors,
            starts_adjustment: 1.0,
            temporal_adjustment: 1.0,
            population: 10.0,
            model_year_fraction: 0.1,
            n_days: 1,
            activity_adjustment: 1.0,
            tech_fractions_table: tch_table,
            retrofit_reduction: retro,
            fuel: FuelKind::Diesel,
            sox_conversion: [0.0, 0.0, 0.02247, 0.0, 0.0],
            sox_base: [SWTGS2, SWTGS4, SWTDSL, SWTGS2, SWTGS2],
            sulfur_alternate: None,
        }
    }

    #[test]
    fn clcems_thc_with_unit_ghp_hr_produces_emission() {
        // Single day, single tech, single year. THC with G/HP-HR.
        let mut efs = ef_grid();
        // Year 0, THC (slot 0), tech 0:
        efs[ef_cell(0, PollutantIndex::Thc, 0)] = 2.5;
        let adetcf = det_grid_f32();
        let bdetcf = det_grid_f32();
        let detcap = det_grid_f32();
        let mut units = unit_grid();
        units[PollutantIndex::Thc.slot() * MXTECH] = EmissionUnitCode::GramsPerHpHour;
        let adj = AdjustmentTable::new(MXDAYS);
        let mut tch = vec![0.0; MXTECH];
        tch[0] = 1.0;
        let retro = vec![0.0; MXPOL];
        let day_range = DayRange {
            begin_day: 1,
            end_day: 1,
            winter_skip_begin: 0,
            winter_skip_end: 0,
            winter_skip: false,
        };
        let mut inputs = make_calc_inputs(
            &mut efs, &adetcf, &bdetcf, &detcap, &units, &adj, &tch, &retro, day_range,
        );
        let filter = PollutantFilter::empty().set(PollutantIndex::Thc, true);
        let out = calculate_exhaust_emissions(&mut inputs, &filter);
        // emstmp = 2.5 * (75*0.5) * 1.0 * 1.0 * 1.0 = 93.75 g
        // emiss = 93.75 * 1.0 * 1.0 * 10.0 * 0.1 * 1.0 = 93.75 (afac branch)
        // CVTTON = 1.102311e-6
        let expected = 93.75_f32 * 1.102311e-6_f32;
        assert!((out.emissions_day[PollutantIndex::Thc.slot()] - expected).abs() < 1e-10);
    }

    #[test]
    fn clcems_co2_rewrites_ef_using_bsfc() {
        let mut efs = ef_grid();
        // THC needs a value too — CO2 uses ems_thc.
        efs[ef_cell(0, PollutantIndex::Thc, 0)] = 1.0;
        // CO2 starting EF is overwritten — value irrelevant.
        let adetcf = det_grid_f32();
        let bdetcf = det_grid_f32();
        let detcap = det_grid_f32();
        let mut units = unit_grid();
        units[PollutantIndex::Thc.slot() * MXTECH] = EmissionUnitCode::GramsPerHpHour;
        let adj = AdjustmentTable::new(MXDAYS);
        let mut tch = vec![0.0; MXTECH];
        tch[0] = 1.0;
        let retro = vec![0.0; MXPOL];
        let day_range = DayRange {
            begin_day: 1,
            end_day: 1,
            winter_skip_begin: 0,
            winter_skip_end: 0,
            winter_skip: false,
        };
        let mut inputs = make_calc_inputs(
            &mut efs, &adetcf, &bdetcf, &detcap, &units, &adj, &tch, &retro, day_range,
        );
        // CO2 is always computed even without an EF file.
        let filter = PollutantFilter::empty().set(PollutantIndex::Thc, true);
        let _ = calculate_exhaust_emissions(&mut inputs, &filter);
        // CO2 EF was rewritten to
        //   hp*load * (bsfc*GRMLB - ems_thc*cvtbck) * CMFDSL * 44/12
        // where ems_thc = THC_factor * cvttmp * detrat (no day adj!) = 1.0 * 37.5 * 1.0 = 37.5
        // cvttmp = hp*load = 75*0.5 = 37.5 (G/HP-HR)
        // cvtbck = 1 / (hp*load) = 1/37.5 ≈ 0.02667
        // new_ef = 37.5 * (0.4*453.6 - 37.5*0.02667) * 0.87 * 44/12
        //        = 37.5 * (181.44 - 1.0) * 0.87 * 3.66667
        //        = 37.5 * 180.44 * 0.87 * 3.66667
        let new_ef = 37.5_f32
            * (0.4_f32 * 453.6_f32 - 37.5_f32 * (1.0_f32 / 37.5_f32))
            * 0.87_f32
            * 44.0_f32
            / 12.0_f32;
        let cell = ef_cell(0, PollutantIndex::Co2, 0);
        assert!(
            (efs[cell] - new_ef).abs() < 1e-2,
            "got {}, expected {}",
            efs[cell],
            new_ef
        );
    }

    #[test]
    fn clcems_retrofit_reduction_applied() {
        let mut efs = ef_grid();
        efs[ef_cell(0, PollutantIndex::Thc, 0)] = 1.0;
        let adetcf = det_grid_f32();
        let bdetcf = det_grid_f32();
        let detcap = det_grid_f32();
        let mut units = unit_grid();
        units[PollutantIndex::Thc.slot() * MXTECH] = EmissionUnitCode::GramsPerHpHour;
        let adj = AdjustmentTable::new(MXDAYS);
        let mut tch = vec![0.0; MXTECH];
        tch[0] = 1.0;
        let mut retro = vec![0.0; MXPOL];
        retro[PollutantIndex::Thc.slot()] = 0.5; // 50% reduction.
        let day_range = DayRange {
            begin_day: 1,
            end_day: 1,
            winter_skip_begin: 0,
            winter_skip_end: 0,
            winter_skip: false,
        };
        let mut inputs = make_calc_inputs(
            &mut efs, &adetcf, &bdetcf, &detcap, &units, &adj, &tch, &retro, day_range,
        );
        let filter = PollutantFilter::empty().set(PollutantIndex::Thc, true);
        let out = calculate_exhaust_emissions(&mut inputs, &filter);
        // Same as first test but × 0.5
        let expected =
            0.5_f32 * (1.0_f32 * 37.5_f32 * 1.0 * 1.0 * 1.0 * 10.0 * 0.1) * 1.102311e-6_f32;
        assert!((out.emissions_day[PollutantIndex::Thc.slot()] - expected).abs() < 1e-10);
    }

    #[test]
    fn clcems_negative_ef_propagates_rmiss() {
        let mut efs = ef_grid();
        efs[ef_cell(0, PollutantIndex::Thc, 0)] = -1.0;
        let adetcf = det_grid_f32();
        let bdetcf = det_grid_f32();
        let detcap = det_grid_f32();
        let mut units = unit_grid();
        units[PollutantIndex::Thc.slot() * MXTECH] = EmissionUnitCode::GramsPerHpHour;
        let adj = AdjustmentTable::new(MXDAYS);
        let mut tch = vec![0.0; MXTECH];
        tch[0] = 1.0;
        let retro = vec![0.0; MXPOL];
        let day_range = DayRange {
            begin_day: 1,
            end_day: 1,
            winter_skip_begin: 0,
            winter_skip_end: 0,
            winter_skip: false,
        };
        let mut inputs = make_calc_inputs(
            &mut efs, &adetcf, &bdetcf, &detcap, &units, &adj, &tch, &retro, day_range,
        );
        let filter = PollutantFilter::empty().set(PollutantIndex::Thc, true);
        let out = calculate_exhaust_emissions(&mut inputs, &filter);
        assert_eq!(out.emissions_day[PollutantIndex::Thc.slot()], RMISS);
        assert_eq!(
            out.emissions_by_model_year[PollutantIndex::Thc.slot()],
            RMISS
        );
    }

    #[test]
    fn clcems_skips_diurnal_range() {
        // Even if we mark IDXDIU's EF positive, the calculator
        // must skip the 8..=17 range.
        let mut efs = ef_grid();
        efs[ef_cell(0, PollutantIndex::Diurnal, 0)] = 100.0;
        let adetcf = det_grid_f32();
        let bdetcf = det_grid_f32();
        let detcap = det_grid_f32();
        let units = unit_grid();
        let adj = AdjustmentTable::new(MXDAYS);
        let mut tch = vec![0.0; MXTECH];
        tch[0] = 1.0;
        let retro = vec![0.0; MXPOL];
        let day_range = DayRange {
            begin_day: 1,
            end_day: 1,
            winter_skip_begin: 0,
            winter_skip_end: 0,
            winter_skip: false,
        };
        let mut inputs = make_calc_inputs(
            &mut efs, &adetcf, &bdetcf, &detcap, &units, &adj, &tch, &retro, day_range,
        );
        let filter = PollutantFilter::all();
        let out = calculate_exhaust_emissions(&mut inputs, &filter);
        assert_eq!(out.emissions_day[PollutantIndex::Diurnal.slot()], 0.0);
    }

    #[test]
    fn clcems_g_per_day_unit_multiplies_by_ndays() {
        let mut efs = ef_grid();
        efs[ef_cell(0, PollutantIndex::Thc, 0)] = 2.0;
        let adetcf = det_grid_f32();
        let bdetcf = det_grid_f32();
        let detcap = det_grid_f32();
        let mut units = unit_grid();
        units[PollutantIndex::Thc.slot() * MXTECH] = EmissionUnitCode::GramsPerDay;
        let adj = AdjustmentTable::new(MXDAYS);
        let mut tch = vec![0.0; MXTECH];
        tch[0] = 1.0;
        let retro = vec![0.0; MXPOL];
        let day_range = DayRange {
            begin_day: 1,
            end_day: 1,
            winter_skip_begin: 0,
            winter_skip_end: 0,
            winter_skip: false,
        };
        let mut inputs = make_calc_inputs(
            &mut efs, &adetcf, &bdetcf, &detcap, &units, &adj, &tch, &retro, day_range,
        );
        inputs.n_days = 30;
        let filter = PollutantFilter::empty().set(PollutantIndex::Thc, true);
        let out = calculate_exhaust_emissions(&mut inputs, &filter);
        // G/DAY: emstmp = 2.0 (no conversion), then * ndays(30) * tpltmp(1)
        // * pop(10) * mfrac(0.1) * tchfrc(1) = 60
        let expected = 60.0_f32 * 1.102311e-6_f32;
        assert!((out.emissions_day[PollutantIndex::Thc.slot()] - expected).abs() < 1e-10);
    }

    #[test]
    fn pollutant_filter_indexing() {
        let f = PollutantFilter::empty().set(PollutantIndex::Co, true);
        assert!(!f.has_factor_file(PollutantIndex::Thc));
        assert!(f.has_factor_file(PollutantIndex::Co));
        assert!(!f.has_factor_file(PollutantIndex::Pm));
    }
}

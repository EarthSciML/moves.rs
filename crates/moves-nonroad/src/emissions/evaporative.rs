//! Evaporative-emission calculation (Task 107).
//!
//! Ports `clcevems.f` (721 lines, the largest single file in NONROAD)
//! and `evemfclc.f` (370 lines). The Rust port keeps the same per-day,
//! per-evap-species branching as the Fortran source so that
//! Task 115 (numerical-fidelity validation) can diff the two
//! implementations on canonical fixtures.
//!
//! Two public entry points:
//!
//! - [`calculate_evaporative_emissions`] — ports `clcevems`. Iterates
//!   the active day-of-year range and, for each evap species, dispatches
//!   one of ten species-specific branches (spillage, displacement, tank
//!   permeation, four hose-permeation variants, hot soak, running loss,
//!   diurnal). Accumulates into per-pollutant `emsday` / `emsbmy`
//!   buffers and returns any non-fatal warnings.
//!
//! - [`calculate_evaporative_factors`] — ports `evemfclc`. Looks up the
//!   emission factor and deterioration coefficients for one
//!   (SCC, HP-average, model-year) iteration and emits per-species
//!   results suitable for caching across many `clcevems` calls.
//!
//! # Globals → explicit inputs
//!
//! The Fortran source threads roughly seventeen COMMON-block scalars
//! and a handful of arrays through these routines. Per the
//! cross-cutting policy in `ARCHITECTURE.md` § 4.1 the Rust port
//! collects them into typed structs (`EvapEmissionsCalcContext`,
//! `EvapFactorsCalcContext`) so each call's data flow is explicit:
//! no shared mutable state, every dependency is a parameter.
//!
//! Fortran-to-Rust mapping of COMMON-block names:
//!
//! | Fortran (file)                | Rust (this module) |
//! |---|---|
//! | `ifuel`        (`nonrdusr.inc`) | [`EvapEmissionsCalcContext::fuel_type`] |
//! | `iprtyp`, `iseasn`, `imonth`, `ldayfl` (`nonrdusr.inc`) | [`EvapEmissionsCalcContext::day_range`] (the caller resolves via [`day_range_for_period`]) |
//! | `tempmx`, `tempmn`, `amtemp`, `fulrvp` (`nonrdusr.inc`) | [`Meteorology::Static`] |
//! | `daytmp`, `dayrvp` (`nonrdusr.inc`) | [`Meteorology::Daily`] |
//! | `ethmkt`, `ethvpct` (`nonrdusr.inc`) | [`EvapEmissionsCalcContext::ethanol`] |
//! | `stg2fac` (`nonrdusr.inc`) | [`EvapEmissionsCalcContext::stage2_pump_factor`] |
//! | `lfacfl` (`nonrdefc.inc`) | [`EvapEmissionsCalcContext::has_factor_file`] |
//! | `evtchfrc`, `evtectyp` (`nonrdeqp.inc`) | [`EvapEmissionsCalcContext::tech_fraction`] / [`tech_type_code`](EvapEmissionsCalcContext::tech_type_code) |
//! | `emsfac` (`nonrdefc.inc`) | [`EvapEmissionsCalcContext::emission_factor`] |
//! | `adjems`, `adjtime` (`nonrdact.inc`) | [`EvapEmissionsCalcContext::day_adjustment`] / [`time_adjustment`](EvapEmissionsCalcContext::time_adjustment) |
//! | `tnke10fac`, `hose10fac`, `ncke10fac`, `sre10fac`, `vnte10fac` (call args) | [`EvapEmissionsCalcContext::e10_factor`] |
//!
//! Arguments declared on `clcevems`/`evemfclc` but not actually read in
//! the Fortran body (`idxunt`, `tfrac`, `sadj`) are omitted from the
//! Rust signatures; reviewers can audit the omission by grepping the
//! source.

use crate::common::consts::{CNTFAC, CVTTON, DIUMIN, MXPOL, PMPFAC, RMISS};
use crate::input::deterioration::DeteriorationRecord;
use crate::input::evemfc::EvapEmissionFactorRecord;
use crate::input::period::{PeriodConfig, PeriodType, Season};
use crate::input::spillage::RefuelingMode;
use crate::output::find::{find_deterioration, find_evap_emission_factor, TECH_DEFAULT};
use crate::output::strutil::wadeeq;
use crate::{Error, Result};

// =============================================================================
//   Public types — inputs
// =============================================================================

/// Equipment fuel type. Fortran indices: `IDXGS2`..`IDXCNG` in
/// `nonrdprm.inc`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FuelType {
    /// 2-stroke gasoline (`IDXGS2 = 1`).
    Gasoline2Stroke,
    /// 4-stroke gasoline (`IDXGS4 = 2`).
    Gasoline4Stroke,
    /// Diesel (`IDXDSL = 3`).
    Diesel,
    /// LPG (`IDXLPG = 4`).
    Lpg,
    /// CNG (`IDXCNG = 5`).
    Cng,
}

impl FuelType {
    /// Whether this fuel emits the full evap-species mix.
    ///
    /// In `clcevems.f` :220-238 the evap-species loop is restricted to
    /// `IDXSPL` (spillage only) for diesel/LPG/CNG; only gasoline
    /// burns through every evap species (`IDXDIU..=IDXRLS`).
    pub fn has_full_evap_species(self) -> bool {
        matches!(self, Self::Gasoline2Stroke | Self::Gasoline4Stroke)
    }
}

/// One of the ten evap species processed by `clcevems`.
///
/// The discriminants are the Fortran 1-based pollutant indices from
/// `nonrdprm.inc` so that callers (and `find_evap_emission_factor`)
/// can use the same `usize` slot in the per-species arrays the Fortran
/// source indexes by these constants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EvapSpecies {
    /// Diurnal evaporation (`IDXDIU = 8`).
    Diurnal = 8,
    /// Tank permeation (`IDXTKP = 9`).
    TankPermeation = 9,
    /// Non-rec-marine hose permeation (`IDXHOS = 10`).
    HosePermeation = 10,
    /// Rec-marine fill-neck permeation (`IDXNCK = 11`).
    NeckPermeation = 11,
    /// Rec-marine supply/return permeation (`IDXSR = 12`).
    SupplyReturnPermeation = 12,
    /// Rec-marine vent permeation (`IDXVNT = 13`).
    VentPermeation = 13,
    /// Hot soak (`IDXSOK = 14`).
    HotSoak = 14,
    /// Vapor displacement (`IDXDIS = 15`).
    Displacement = 15,
    /// Spillage (`IDXSPL = 16`).
    Spillage = 16,
    /// Running loss (`IDXRLS = 17`).
    RunningLoss = 17,
}

impl EvapSpecies {
    /// All evap species the Fortran loop iterates for gasoline runs
    /// (`IDXDIU..=IDXRLS`).
    pub const ALL: [Self; 10] = [
        Self::Diurnal,
        Self::TankPermeation,
        Self::HosePermeation,
        Self::NeckPermeation,
        Self::SupplyReturnPermeation,
        Self::VentPermeation,
        Self::HotSoak,
        Self::Displacement,
        Self::Spillage,
        Self::RunningLoss,
    ];

    /// 1-based Fortran pollutant index.
    pub fn pollutant_index(self) -> usize {
        self as usize
    }

    /// 0-based slot into the [`MXPOL`]-sized accumulator arrays
    /// (`emsday`, `emsbmy`, `lfacfl`, …).
    pub fn slot(self) -> usize {
        self.pollutant_index() - 1
    }

    /// Character position in the `E00000000` evap-tech-type string
    /// that encodes the control-technology slot for this species.
    ///
    /// `clcevems.f` decodes the control-tech slot from a fixed offset
    /// in the tech-type code:
    ///
    /// * tank → char 3
    /// * non-rec hose, neck, supply/return, vent → char 4
    ///
    /// (See `evemfclc.f` :177-183 for the position-to-species table.)
    /// Returns `None` for species that do not consult this code.
    fn tech_code_slot(self) -> Option<usize> {
        match self {
            Self::TankPermeation => Some(2), // char 3 in 1-based, 0-based 2
            Self::HosePermeation
            | Self::NeckPermeation
            | Self::SupplyReturnPermeation
            | Self::VentPermeation => Some(3), // char 4
            _ => None,
        }
    }

    /// Position of this species' slot in the `E00000000` evap tech
    /// code, expressed as a 1-based column offset matching the Fortran
    /// `j=idxspc-IDXDIU+2` formula from `evemfclc.f` :177-183.
    /// Used by [`calculate_evaporative_factors`] when looking up
    /// the per-species sub-tech code.
    fn ev_tech_char_offset(self) -> Option<usize> {
        // Mirrors `j=idxspc-IDXDIU+2` (no shift, hose-mapping, etc.):
        // * IDXDIU..IDXHOS → col idxspc - IDXDIU + 2
        // * IDXNCK..IDXVNT → mapped to IDXHOS column (4)
        // * IDXSOK..IDXRLS → shifted by 3
        let idxdiu = Self::Diurnal.pollutant_index() as i32;
        let idxhos = Self::HosePermeation.pollutant_index() as i32;
        let idxnck = Self::NeckPermeation.pollutant_index() as i32;
        let idxvnt = Self::VentPermeation.pollutant_index() as i32;
        let idxspc = self.pollutant_index() as i32;
        let offset = if idxspc <= idxhos {
            idxspc - idxdiu + 2
        } else if (idxnck..=idxvnt).contains(&idxspc) {
            idxhos - idxdiu + 2
        } else {
            idxspc - idxdiu + 2 - 3
        };
        if offset >= 2 {
            Some(offset as usize)
        } else {
            None
        }
    }

    /// Whether `clcevems` may compute a non-zero value for this
    /// species even without an `lfacfl(idxspc)` file. (`IDXDIS` and
    /// `IDXSPL` are computed-from-other-inputs; `IDXSOX` and `IDXCO2`
    /// likewise but they are not in this enum's domain.)
    fn computed_without_factor_file(self) -> bool {
        matches!(self, Self::Displacement | Self::Spillage)
    }
}

/// Ethanol-blend parameters (`ethmkt` / `ethvpct` from
/// `nonrdusr.inc`).
#[derive(Debug, Clone, Copy)]
pub struct EthanolBlend {
    /// `ethmkt` — ethanol-blend market share (percent of all
    /// gasoline that contains ethanol).
    pub market_percent: f32,
    /// `ethvpct` — volume percent of ethanol within ethanol
    /// blends.
    pub volume_percent: f32,
}

impl EthanolBlend {
    /// Fortran sentinel: blend market and volume both 0 → no ethanol
    /// adjustment applied (`clcevems.f` :354).
    pub fn none() -> Self {
        Self {
            market_percent: 0.0,
            volume_percent: 0.0,
        }
    }
}

/// Day-of-year iteration range, as resolved by `dayloop.f`.
///
/// All days are 1-based julian days, 1..=365 (NONROAD assumes 365-day
/// years; `dayloop.f` has commented-out leap-year handling that EPA
/// disabled).
#[derive(Debug, Clone, Copy)]
pub struct DayRange {
    /// First (inclusive) day-of-year to process.
    pub begin: i32,
    /// Last (inclusive) day-of-year to process.
    pub end: i32,
    /// Inclusive begin of the winter skip-over range, or 0 when no
    /// skip applies. Days in `[skip_begin, skip_end]` are bypassed
    /// inside the loop.
    pub skip_begin: i32,
    /// Inclusive end of the winter skip-over range, or 0 when no
    /// skip applies.
    pub skip_end: i32,
    /// Whether a skip range is active.
    pub skip: bool,
}

impl DayRange {
    /// Single-day range, the default when `ldayfl = .FALSE.`. Matches
    /// `dayloop.f` :65-69 where `jbday=jeday=1`.
    pub const SINGLE: Self = Self {
        begin: 1,
        end: 1,
        skip_begin: 0,
        skip_end: 0,
        skip: false,
    };

    fn covers(self, day: i32) -> bool {
        self.skip && day >= self.skip_begin && day <= self.skip_end
    }
}

/// Compute the day-range covered by the configured period.
///
/// Ports `dayloop.f`. The Fortran routine returns
/// `jbday=jeday=1` when daily-temperature mode is off; the Rust
/// counterpart returns [`DayRange::SINGLE`] in that case.
///
/// `daily_temperatures` mirrors Fortran's `ldayfl` flag — `true` only
/// when a daily `.tmp`/`.rvp` file was loaded.
pub fn day_range_for_period(period: &PeriodConfig, daily_temperatures: bool) -> DayRange {
    if !daily_temperatures {
        return DayRange::SINGLE;
    }
    const DAYNUM: [i32; 13] = [1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366];
    let max_days: i32 = crate::common::consts::MXDAYS as i32;
    match period.period_type {
        PeriodType::Annual => DayRange {
            begin: 1,
            end: max_days,
            skip_begin: 0,
            skip_end: 0,
            skip: false,
        },
        PeriodType::Seasonal => match period.season.unwrap_or(Season::Summer) {
            Season::Winter => DayRange {
                begin: 1,
                end: max_days,
                skip_begin: DAYNUM[2],
                skip_end: DAYNUM[11] - 1,
                skip: true,
            },
            Season::Spring => DayRange {
                begin: DAYNUM[2],
                end: DAYNUM[5] - 1,
                skip_begin: 0,
                skip_end: 0,
                skip: false,
            },
            Season::Summer => DayRange {
                begin: DAYNUM[5],
                end: DAYNUM[8] - 1,
                skip_begin: 0,
                skip_end: 0,
                skip: false,
            },
            Season::Fall => DayRange {
                begin: DAYNUM[8],
                end: DAYNUM[11] - 1,
                skip_begin: 0,
                skip_end: 0,
                skip: false,
            },
        },
        PeriodType::Monthly => {
            let month = period.month.unwrap_or(1) as usize;
            DayRange {
                begin: DAYNUM[month - 1],
                end: DAYNUM[month] - 1,
                skip_begin: 0,
                skip_end: 0,
                skip: false,
            }
        }
    }
}

/// Meteorology inputs.
///
/// `clcevems.f` reads either per-day arrays (`daytmp`, `dayrvp`) when
/// `ldayfl=.TRUE.`, or the four scalars (`tempmx`, `tempmn`,
/// `amtemp`, `fulrvp`) otherwise. The variants below mirror these two
/// modes so the rest of the code path is the same.
#[derive(Debug, Clone)]
pub enum Meteorology<'a> {
    /// Single-temperature mode. Used when `ldayfl=.FALSE.` and the
    /// day-loop collapses to a single iteration.
    Static {
        /// Maximum daily temperature (°F).
        max_temp: f32,
        /// Minimum daily temperature (°F).
        min_temp: f32,
        /// Representative ambient temperature (°F).
        ambient_temp: f32,
        /// Fuel RVP (psi).
        rvp: f32,
    },
    /// Per-day arrays. `temps[(day, kind)]` with
    /// `kind ∈ {0=max, 1=min, 2=ambient}`. `rvps[day]` is the per-day
    /// fuel RVP. Days are 1-based to match the Fortran `daytmp(jday,
    /// kind, ireg)` and `dayrvp(jday, ireg)` access pattern.
    Daily {
        /// 365 × 3 temperature triples (1-based day index, max/min/amb
        /// rows).
        temps: &'a [[f32; 3]],
        /// 365 fuel-RVP values (1-based day index).
        rvps: &'a [f32],
    },
}

impl Meteorology<'_> {
    /// Resolve the (max, min, ambient, rvp) tuple for one day.
    fn for_day(&self, day: i32) -> (f32, f32, f32, f32) {
        match self {
            Self::Static {
                max_temp,
                min_temp,
                ambient_temp,
                rvp,
            } => (*max_temp, *min_temp, *ambient_temp, *rvp),
            Self::Daily { temps, rvps } => {
                let idx = (day - 1).max(0) as usize;
                let temps_row = temps.get(idx).copied().unwrap_or([0.0, 0.0, 0.0]);
                let rvp = rvps.get(idx).copied().unwrap_or(0.0);
                (temps_row[0], temps_row[1], temps_row[2], rvp)
            }
        }
    }
}

/// Refueling-mode inputs needed by the spillage and displacement
/// branches.
#[derive(Debug, Clone)]
pub struct RefuelingContext {
    /// Refueling mode for this iteration, or `None` if no spillage
    /// record applied (Fortran `refmod = '         '`, nine blanks).
    pub mode: Option<RefuelingMode>,
    /// Tank volume (gallons), needed for spillage scaling.
    pub tank_volume_gallons: f32,
    /// Fuel consumption (gallons), used in spillage and displacement
    /// emissions.
    pub fuel_consumption_gallons: f32,
    /// Stage-II vapor-recovery percent reduction applied to pump-mode
    /// displacement (Fortran `stg2fac` from `nonrdusr.inc`).
    pub stage2_pump_factor: f32,
}

/// Per-iteration inputs to [`calculate_evaporative_emissions`].
///
/// One context drives one outer loop call; the function iterates the
/// day-range and the evap-species list internally.
#[derive(Debug, Clone)]
pub struct EvapEmissionsCalcContext<'a> {
    /// 10-character SCC of the current model iteration (used in error
    /// messages).
    pub scc: &'a str,
    /// 5-character FIPS code of the current model iteration (used by
    /// the daily-meteorology lookup; we keep the raw string so the
    /// `Daily` arm of [`Meteorology`] can be indexed by the caller).
    pub fips_code: &'a str,
    /// HP-average (passed only for error messages, like the Fortran
    /// source).
    pub hp_avg: f32,
    /// Equipment fuel type.
    pub fuel_type: FuelType,
    /// Day-of-year iteration range (resolve once with
    /// [`day_range_for_period`]).
    pub day_range: DayRange,
    /// Period type — needed by the displacement branch's `iprtyp`
    /// check (`clcevems.f` :322-328).
    pub period_type: PeriodType,
    /// Whether daily temperature/RVP data was provided. Mirrors
    /// `ldayfl` from `nonrdusr.inc`. When `false` the function
    /// multiplies most species' per-day emissions by `ndays` to
    /// scale up to the period total; when `true` the day-loop
    /// produces the period total directly.
    pub daily_mode: bool,
    /// Meteorology data (single-value or per-day).
    pub meteorology: Meteorology<'a>,
    /// Number of days in the period (Fortran `ndays`). When
    /// `daily_mode = false` this multiplies the per-day emissions
    /// to produce the period total.
    pub ndays: f32,
    /// Evap technology fraction for this (`idxtch`, `idxtec`) slot
    /// (Fortran `evtchfrc(idxtch,idxtec)`). All species in the loop
    /// are scaled by this single factor.
    pub tech_fraction: f32,
    /// Evap tech-type code for this slot (Fortran
    /// `evtectyp(idxtch,idxtec)`, the `E` + 8-digit identifier).
    /// Used by the ethanol-blend correction to detect controlled
    /// tech types.
    pub tech_type_code: &'a str,
    /// Engine population for the current iteration (`pop`).
    pub population: f32,
    /// Model-year fraction within the iteration (`mfrac`).
    pub model_year_fraction: f32,
    /// Equipment deterioration age (`dage`).
    pub deterioration_age: f32,
    /// Deterioration cap on age per species
    /// (`detcap(idxspc,idxtec)`); slot is [`EvapSpecies::slot()`].
    /// Length must be at least [`MXPOL`].
    pub deterioration_cap: &'a [f32],
    /// Deterioration A-coefficient per species
    /// (`adetcf(idxspc,idxtec)`).
    pub deterioration_a: &'a [f32],
    /// Deterioration B-coefficient per species
    /// (`bdetcf(idxspc,idxtec)`).
    pub deterioration_b: &'a [f32],
    /// Emission-factor per species for this model-year slot
    /// (`emsfac(idxyr,idxspc,idxtec)`). Length ≥ [`MXPOL`].
    pub emission_factor: &'a [f32],
    /// Per-day emissions-adjustment factor matrix
    /// (`adjems(idxspc,day)`). Indexed by `species_slot * 365 + (day -
    /// 1)`. Length must be `species × 365` ≥ [`MXPOL`] × 365.
    pub day_adjustment: &'a [f32],
    /// Time-period adjustment factor (`adjtime`).
    pub time_adjustment: f32,
    /// Temporal adjustment factor (`tpltmp` — preserved unchanged for
    /// downstream PRC* routines, hence `tpltmp2 = tpltmp` in the
    /// Fortran source).
    pub temporal_factor: f32,
    /// Activity adjustment (`afac`).
    pub activity: f32,
    /// Hot soaks per hour of operation (`hsstrt`).
    pub hot_soaks_per_hour: f32,
    /// Five diurnal fractions for this iteration's evap tech type
    /// (`diufrac(1..5, idxtec)`).
    pub diurnal_fractions: [f32; 5],
    /// Refueling-mode inputs (spillage + displacement).
    pub refueling: RefuelingContext,
    /// Tank metal fraction (`tmfrac`).
    pub tank_metal_fraction: f32,
    /// Tank fill fraction (`tfull`).
    pub tank_fill_fraction: f32,
    /// Tank volume (gallons, `tvol`). Same as
    /// `refueling.tank_volume_gallons` and kept here to make the
    /// tank-permeation branch's reading explicit.
    pub tank_volume_gallons: f32,
    /// Non-rec-marine hose metal fraction (`hmfrac`).
    pub hose_metal_fraction: f32,
    /// Non-rec-marine hose length (metres, `hoselen`).
    pub hose_length_m: f32,
    /// Non-rec-marine hose diameter (metres, `hosedia`).
    pub hose_diameter_m: f32,
    /// Rec-marine fill-neck hose length (metres, `necklen`).
    pub neck_length_m: f32,
    /// Rec-marine fill-neck hose diameter (metres, `neckdia`).
    pub neck_diameter_m: f32,
    /// Rec-marine supply/return hose length (metres, `supretlen`).
    pub supply_return_length_m: f32,
    /// Rec-marine supply/return hose diameter (metres, `supretdia`).
    pub supply_return_diameter_m: f32,
    /// Rec-marine vent hose length (metres, `ventlen`).
    pub vent_length_m: f32,
    /// Rec-marine vent hose diameter (metres, `ventdia`).
    pub vent_diameter_m: f32,
    /// Per-species E10 adjustment factor for permeation pollutants
    /// (`tnke10fac`, `hose10fac`, `ncke10fac`, `sre10fac`,
    /// `vnte10fac`). Indexed by [`EvapSpecies::slot()`]; non-permeation
    /// species' values are unused.
    pub e10_factor: &'a [f32],
    /// Ethanol-blend parameters.
    pub ethanol: EthanolBlend,
    /// Whether an emission-factor file was supplied for the given
    /// species (Fortran `lfacfl(idxspc)`). Index by
    /// [`EvapSpecies::slot()`].
    pub has_factor_file: &'a [bool],
}

impl<'a> EvapEmissionsCalcContext<'a> {
    fn fips_state_index(&self) -> Option<usize> {
        self.fips_code
            .get(..2)
            .and_then(|s| s.parse::<usize>().ok())
    }
}

/// Outcome of [`calculate_evaporative_emissions`].
///
/// The per-pollutant accumulators (`emsday`, `emsbmy`) live in
/// caller-owned slices and are mutated in place — mirroring the
/// Fortran source's `emsday(MXPOL)` / `emsbmy(MXPOL)` arguments.
/// Non-fatal warnings collected during the call appear in
/// [`Self::warnings`].
#[derive(Debug, Clone, PartialEq, Default)]
pub struct EvapEmissionsOutcome {
    /// Non-fatal warnings.
    pub warnings: Vec<EvapEmissionsWarning>,
}

/// Non-fatal warnings produced by [`calculate_evaporative_emissions`].
///
/// The Fortran source writes equivalent diagnostics via `IOWSTD` /
/// `IOWMSG`; the Rust port surfaces them as data so callers can log,
/// count, or surface them as the wider system requires. The
/// post-clamp accumulator value (`RMISS`) is still written into
/// `emsday`/`emsbmy` to match the Fortran behaviour.
#[derive(Debug, Clone, PartialEq)]
pub enum EvapEmissionsWarning {
    /// Hot-soak EF subtracted-permeation residual was negative,
    /// mirroring `clcevems.f` :675-683 (`7000`).
    NegativeHotSoakEf {
        /// SCC of the iteration.
        scc: String,
        /// HP-average of the iteration.
        hp_avg: f32,
        /// `emstmp` (the species EF before the subtraction).
        emstmp: f32,
        /// `permef` (the permeation residual subtracted). With current
        /// Fortran this is always 0.0 but is kept for parity with the
        /// warning message.
        permef: f32,
    },
    /// Running-loss EF subtracted-permeation residual was negative,
    /// `clcevems.f` :685-693 (`7002`).
    NegativeRunningLossEf {
        /// SCC of the iteration.
        scc: String,
        /// HP-average of the iteration.
        hp_avg: f32,
        /// `emstmp`.
        emstmp: f32,
        /// `permef`.
        permef: f32,
    },
    /// Per-tech-type diurnal accumulator went negative, `clcevems.f`
    /// :695-703 (`7004`).
    NegativeDiurnalEmiss {
        /// SCC of the iteration.
        scc: String,
        /// HP-average of the iteration.
        hp_avg: f32,
        /// `tmax` at the time of the failure.
        tmax: f32,
        /// `emiss` (accumulator value when the check fired).
        emiss: f32,
    },
}

// =============================================================================
//   clcevems — entry point
// =============================================================================

/// Compute per-day evap emissions for one model-iteration tuple.
///
/// Ports `clcevems.f`. The caller pre-resolves the day range via
/// [`day_range_for_period`] and supplies an [`EvapEmissionsCalcContext`]
/// describing all per-iteration inputs (replaces the Fortran COMMON
/// blocks). `emsday` and `emsbmy` are caller-owned accumulators of
/// length at least [`MXPOL`]; the function adds into the appropriate
/// pollutant slots and (matching the Fortran source) clamps both
/// accumulators to [`RMISS`] for any pollutant whose cumulative
/// `emsbmy` goes negative.
///
/// Errors:
///
/// * [`Error::Config`] when input slice lengths are too short for
///   [`MXPOL`].
/// * [`Error::Config`] when daily-mode is on but the FIPS code can't
///   be parsed.
///
/// Non-fatal warnings are returned in [`EvapEmissionsOutcome::warnings`].
pub fn calculate_evaporative_emissions(
    ctx: &EvapEmissionsCalcContext<'_>,
    emsday: &mut [f32],
    emsbmy: &mut [f32],
) -> Result<EvapEmissionsOutcome> {
    if emsday.len() < MXPOL || emsbmy.len() < MXPOL {
        return Err(Error::Config(format!(
            "emsday/emsbmy must have length >= MXPOL ({MXPOL}); got {} / {}",
            emsday.len(),
            emsbmy.len()
        )));
    }
    let pol_len = ctx
        .has_factor_file
        .len()
        .min(ctx.emission_factor.len())
        .min(ctx.deterioration_cap.len())
        .min(ctx.deterioration_a.len())
        .min(ctx.deterioration_b.len())
        .min(ctx.e10_factor.len());
    if pol_len < MXPOL {
        return Err(Error::Config(format!(
            "evap input slices must have length >= MXPOL ({MXPOL}); got {pol_len}"
        )));
    }

    let mut outcome = EvapEmissionsOutcome::default();

    let species_iter: &[EvapSpecies] = if ctx.fuel_type.has_full_evap_species() {
        &EvapSpecies::ALL
    } else {
        // Non-gasoline fuels only compute spillage.
        &[EvapSpecies::Spillage]
    };

    for jday in ctx.day_range.begin..=ctx.day_range.end {
        if ctx.day_range.covers(jday) {
            continue;
        }

        let (tmax, tmin, tamb, trvp) = if ctx.daily_mode {
            // Daily mode requires a parseable FIPS state code so
            // `Meteorology::Daily` can be indexed (the Fortran source
            // parses `read(code(1:2),*) ireg`). For the Rust port we
            // surface the parsed state index here so callers building
            // the meteorology slice can mirror it; failure is a
            // configuration error.
            if ctx.fips_state_index().is_none() {
                return Err(Error::Config(format!(
                    "daily_mode set but FIPS code {:?} does not parse as a state index",
                    ctx.fips_code
                )));
            }
            ctx.meteorology.for_day(jday)
        } else {
            // Single-day mode (the `else` branch in clcevems.f :235);
            // the day index is irrelevant.
            match ctx.meteorology {
                Meteorology::Static {
                    max_temp,
                    min_temp,
                    ambient_temp,
                    rvp,
                } => (max_temp, min_temp, ambient_temp, rvp),
                Meteorology::Daily { .. } => {
                    return Err(Error::Config(
                        "daily_mode=false but Meteorology::Daily supplied; \
                         pass Meteorology::Static for single-day mode"
                            .to_string(),
                    ));
                }
            }
        };

        for &species in species_iter {
            let slot = species.slot();
            if !ctx.has_factor_file[slot] && !species.computed_without_factor_file() {
                continue;
            }
            // The deterioration ratio is the same `1 + a * age^b` /
            // `1 + a * cap^b` shape regardless of species (clcevems.f
            // :252-258).
            let det_ratio = deterioration_ratio(
                ctx.deterioration_age,
                ctx.deterioration_cap[slot],
                ctx.deterioration_a[slot],
                ctx.deterioration_b[slot],
            );
            let emstmp = ctx.emission_factor[slot];
            let adjems_idx = slot * crate::common::consts::MXDAYS + ((jday - 1).max(0) as usize);
            let adjems = ctx.day_adjustment.get(adjems_idx).copied().unwrap_or(1.0);

            let temiss = match species {
                EvapSpecies::Spillage => spillage_branch(ctx),
                EvapSpecies::Displacement => displacement_branch(ctx, tamb, trvp),
                EvapSpecies::TankPermeation => tank_branch(ctx, emstmp, adjems, det_ratio),
                EvapSpecies::HosePermeation => hose_branch(ctx, emstmp, adjems, det_ratio),
                EvapSpecies::NeckPermeation => neck_branch(ctx, emstmp, adjems, det_ratio),
                EvapSpecies::SupplyReturnPermeation => {
                    supply_return_branch(ctx, emstmp, adjems, det_ratio)
                }
                EvapSpecies::VentPermeation => vent_branch(ctx, emstmp, adjems, det_ratio),
                EvapSpecies::HotSoak => {
                    hot_soak_branch(ctx, emstmp, adjems, det_ratio, &mut outcome)?
                }
                EvapSpecies::RunningLoss => {
                    running_loss_branch(ctx, emstmp, adjems, det_ratio, &mut outcome)?
                }
                EvapSpecies::Diurnal => diurnal_branch(
                    ctx,
                    emstmp,
                    adjems,
                    det_ratio,
                    tmax,
                    tmin,
                    trvp,
                    &mut outcome,
                )?,
            };

            emsday[slot] += temiss;
            emsbmy[slot] += temiss;

            // Negative-accumulator clamp (clcevems.f :659-662).
            if emsbmy[slot] < 0.0 {
                emsday[slot] = RMISS;
                emsbmy[slot] = RMISS;
            }
        }
    }

    Ok(outcome)
}

fn deterioration_ratio(age: f32, cap: f32, a: f32, b: f32) -> f32 {
    let effective_age = if age <= cap { age } else { cap };
    1.0 + a * effective_age.powf(b)
}

// =============================================================================
//   Per-species branches
// =============================================================================

fn spillage_branch(ctx: &EvapEmissionsCalcContext<'_>) -> f32 {
    if !ctx.fuel_type.has_full_evap_species() {
        return 0.0;
    }
    let spillage_slot = EvapSpecies::Spillage.slot();
    if !ctx.has_factor_file[spillage_slot] {
        return 0.0;
    }
    // Matches `clcevems.f` :272-281 — divides by `tvol` unconditionally
    // when the refueling mode is set. A zero `tvol` would propagate
    // an infinite (or NaN-multiplied) value, mirroring the Fortran
    // behaviour; callers are expected to guarantee `tvol > 0` before
    // entry, the same precondition the Fortran source assumes.
    let emiss = match ctx.refueling.mode {
        Some(RefuelingMode::Pump) => PMPFAC / ctx.refueling.tank_volume_gallons,
        Some(RefuelingMode::Container) => CNTFAC / ctx.refueling.tank_volume_gallons,
        None => 0.0,
    };
    let mut temiss = emiss * CVTTON * ctx.refueling.fuel_consumption_gallons;
    if ctx.daily_mode {
        temiss /= ctx.ndays;
    }
    temiss
}

fn displacement_branch(ctx: &EvapEmissionsCalcContext<'_>, tamb: f32, trvp: f32) -> f32 {
    let spillage_slot = EvapSpecies::Spillage.slot();
    let emiss = if ctx.has_factor_file[spillage_slot] {
        match ctx.refueling.mode {
            Some(RefuelingMode::Pump) => {
                let tempds = 62.0 + 0.6 * (tamb - 62.0);
                (-1.2798 - 0.0049 * (tempds - tamb) + 0.0203 * tempds + 0.1315 * trvp).exp()
            }
            Some(RefuelingMode::Container) => {
                let tempds = tamb;
                (-1.2798 - 0.0049 * (tempds - tamb) + 0.0203 * tempds + 0.1315 * trvp).exp()
            }
            None => 0.0,
        }
    } else {
        0.0
    };
    let stg2 = if matches!(ctx.refueling.mode, Some(RefuelingMode::Pump)) {
        ctx.refueling.stage2_pump_factor
    } else {
        1.0
    };
    let mut temiss = emiss * CVTTON * ctx.refueling.fuel_consumption_gallons * stg2;
    if ctx.daily_mode {
        if matches!(ctx.period_type, PeriodType::Annual) {
            // Annual + daily mode: scale by the day's adjustment
            // (clcevems.f :322-324). Note: the Fortran indexes the
            // adjustment by the current `jday`, but the displacement
            // branch is called once per day at row offset
            // `slot*MXDAYS + (jday-1)`; we mirror that here.
            let dis_slot = EvapSpecies::Displacement.slot();
            let adjems_idx = dis_slot * crate::common::consts::MXDAYS;
            let adjems = ctx.day_adjustment.get(adjems_idx).copied().unwrap_or(1.0);
            temiss *= adjems;
        } else {
            // Mirrors `clcevems.f` :326 — divides by `ndays` to spread
            // the period total over its days. Callers must supply
            // `ndays > 0` for monthly/seasonal/typical-day runs.
            temiss /= ctx.ndays;
        }
    }
    temiss
}

fn tank_branch(
    ctx: &EvapEmissionsCalcContext<'_>,
    emstmp: f32,
    adjems: f32,
    det_ratio: f32,
) -> f32 {
    let species = EvapSpecies::TankPermeation;
    let slot = species.slot();
    let spillage_slot = EvapSpecies::Spillage.slot();
    let emiss = if ctx.has_factor_file[slot] && ctx.has_factor_file[spillage_slot] {
        let tv = ctx.tank_volume_gallons;
        // sfcarea = 0.15 * sqrt(((tvol+2)^2 / 2^2) - 1)
        let inner = (((tv + 2.0).powi(2)) / 4.0) - 1.0;
        let sfcarea = 0.15 * inner.max(0.0).sqrt();
        emstmp * (1.0 - ctx.tank_metal_fraction) * sfcarea
    } else {
        0.0
    };
    let mut temiss = emiss
        * CVTTON
        * adjems
        * ctx.time_adjustment
        * det_ratio
        * ctx.population
        * ctx.model_year_fraction
        * ctx.tech_fraction;
    temiss = apply_ethanol_correction(ctx, species, temiss, ctx.e10_factor[slot]);
    if !ctx.daily_mode {
        temiss *= ctx.ndays;
    }
    temiss
}

fn hose_branch(
    ctx: &EvapEmissionsCalcContext<'_>,
    emstmp: f32,
    adjems: f32,
    det_ratio: f32,
) -> f32 {
    let species = EvapSpecies::HosePermeation;
    let slot = species.slot();
    let spillage_slot = EvapSpecies::Spillage.slot();
    let emiss = if ctx.has_factor_file[slot] && ctx.has_factor_file[spillage_slot] {
        let sfcarea = std::f32::consts::PI * ctx.hose_length_m * ctx.hose_diameter_m;
        emstmp * (1.0 - ctx.hose_metal_fraction) * sfcarea
    } else {
        0.0
    };
    let mut temiss = emiss
        * CVTTON
        * det_ratio
        * adjems
        * ctx.time_adjustment
        * ctx.population
        * ctx.model_year_fraction
        * ctx.tech_fraction;
    temiss = apply_ethanol_correction(ctx, species, temiss, ctx.e10_factor[slot]);
    if !ctx.daily_mode {
        temiss *= ctx.ndays;
    }
    temiss
}

fn neck_branch(
    ctx: &EvapEmissionsCalcContext<'_>,
    emstmp: f32,
    adjems: f32,
    det_ratio: f32,
) -> f32 {
    let species = EvapSpecies::NeckPermeation;
    let slot = species.slot();
    let spillage_slot = EvapSpecies::Spillage.slot();
    let emiss = if ctx.has_factor_file[slot] && ctx.has_factor_file[spillage_slot] {
        let sfcarea = std::f32::consts::PI * ctx.neck_length_m * ctx.neck_diameter_m;
        // rec-marine hoses are 100% non-metal
        emstmp * sfcarea
    } else {
        0.0
    };
    let mut temiss = emiss
        * CVTTON
        * det_ratio
        * adjems
        * ctx.time_adjustment
        * ctx.population
        * ctx.model_year_fraction
        * ctx.tech_fraction;
    temiss = apply_ethanol_correction(ctx, species, temiss, ctx.e10_factor[slot]);
    if !ctx.daily_mode {
        temiss *= ctx.ndays;
    }
    temiss
}

fn supply_return_branch(
    ctx: &EvapEmissionsCalcContext<'_>,
    emstmp: f32,
    adjems: f32,
    det_ratio: f32,
) -> f32 {
    let species = EvapSpecies::SupplyReturnPermeation;
    let slot = species.slot();
    let spillage_slot = EvapSpecies::Spillage.slot();
    let emiss = if ctx.has_factor_file[slot] && ctx.has_factor_file[spillage_slot] {
        let sfcarea =
            std::f32::consts::PI * ctx.supply_return_length_m * ctx.supply_return_diameter_m;
        emstmp * sfcarea
    } else {
        0.0
    };
    let mut temiss = emiss
        * CVTTON
        * det_ratio
        * adjems
        * ctx.time_adjustment
        * ctx.population
        * ctx.model_year_fraction
        * ctx.tech_fraction;
    temiss = apply_ethanol_correction(ctx, species, temiss, ctx.e10_factor[slot]);
    if !ctx.daily_mode {
        temiss *= ctx.ndays;
    }
    temiss
}

fn vent_branch(
    ctx: &EvapEmissionsCalcContext<'_>,
    emstmp: f32,
    adjems: f32,
    det_ratio: f32,
) -> f32 {
    let species = EvapSpecies::VentPermeation;
    let slot = species.slot();
    let spillage_slot = EvapSpecies::Spillage.slot();
    let emiss = if ctx.has_factor_file[slot] && ctx.has_factor_file[spillage_slot] {
        let sfcarea = std::f32::consts::PI * ctx.vent_length_m * ctx.vent_diameter_m;
        emstmp * sfcarea
    } else {
        0.0
    };
    let mut temiss = emiss
        * CVTTON
        * det_ratio
        * adjems
        * ctx.time_adjustment
        * ctx.population
        * ctx.model_year_fraction
        * ctx.tech_fraction;
    temiss = apply_ethanol_correction(ctx, species, temiss, ctx.e10_factor[slot]);
    if !ctx.daily_mode {
        temiss *= ctx.ndays;
    }
    temiss
}

fn hot_soak_branch(
    ctx: &EvapEmissionsCalcContext<'_>,
    emstmp: f32,
    adjems: f32,
    det_ratio: f32,
    outcome: &mut EvapEmissionsOutcome,
) -> Result<f32> {
    let species = EvapSpecies::HotSoak;
    let slot = species.slot();
    let spillage_slot = EvapSpecies::Spillage.slot();
    let permef: f32 = 0.0; // clcevems.f :548-549, the (tank+hose) bypass was disabled
    let emiss = if ctx.has_factor_file[slot] && ctx.has_factor_file[spillage_slot] {
        if emstmp >= permef {
            let base = emstmp - permef;
            ctx.activity * ctx.hot_soaks_per_hour * base
        } else {
            outcome
                .warnings
                .push(EvapEmissionsWarning::NegativeHotSoakEf {
                    scc: ctx.scc.to_string(),
                    hp_avg: ctx.hp_avg,
                    emstmp,
                    permef,
                });
            // Fortran returns ISUCES after warning and falls through;
            // the `emstmp - permef` value would be negative, so we
            // skip the accumulation by returning 0 here. The
            // accumulator-clamp at the end of the day-loop further
            // forces RMISS if `emsbmy` ends up negative.
            return Ok(0.0);
        }
    } else {
        0.0
    };
    Ok(emiss
        * CVTTON
        * det_ratio
        * ctx.temporal_factor
        * adjems
        * ctx.time_adjustment
        * ctx.population
        * ctx.model_year_fraction
        * ctx.tech_fraction)
}

fn running_loss_branch(
    ctx: &EvapEmissionsCalcContext<'_>,
    emstmp: f32,
    adjems: f32,
    det_ratio: f32,
    outcome: &mut EvapEmissionsOutcome,
) -> Result<f32> {
    let species = EvapSpecies::RunningLoss;
    let slot = species.slot();
    let permef: f32 = 0.0; // clcevems.f :573-574, like hot soak the (tank+hose) bypass was disabled
    let emiss = if ctx.has_factor_file[slot] {
        if emstmp >= permef {
            let base = emstmp - permef;
            base * ctx.activity
        } else {
            outcome
                .warnings
                .push(EvapEmissionsWarning::NegativeRunningLossEf {
                    scc: ctx.scc.to_string(),
                    hp_avg: ctx.hp_avg,
                    emstmp,
                    permef,
                });
            return Ok(0.0);
        }
    } else {
        0.0
    };
    Ok(emiss
        * CVTTON
        * det_ratio
        * ctx.temporal_factor
        * adjems
        * ctx.time_adjustment
        * ctx.population
        * ctx.model_year_fraction
        * ctx.tech_fraction)
}

#[allow(clippy::too_many_arguments)]
fn diurnal_branch(
    ctx: &EvapEmissionsCalcContext<'_>,
    emstmp: f32,
    adjems: f32,
    det_ratio: f32,
    tmax: f32,
    tmin: f32,
    trvp: f32,
    outcome: &mut EvapEmissionsOutcome,
) -> Result<f32> {
    let species = EvapSpecies::Diurnal;
    let slot = species.slot();
    let mut emiss: f32 = 0.0;
    if ctx.has_factor_file[slot] && tmax > DIUMIN {
        let diutmax = tmax.max(DIUMIN);
        let diutmin = tmin.max(DIUMIN);
        let tavg = (diutmax + diutmin) / 2.0;
        let swing = (diutmax - diutmin) / 2.0;

        // The Fortran source iterates `j=1,5` and short-circuits on
        // `diufrac(j) == 1.0` (the "this is the entire bin" sentinel).
        for j in 0..5usize {
            let frac = ctx.diurnal_fractions[j];
            if frac == 0.0 {
                continue;
            }
            let contrib = if j == 0 {
                // Non-rec-marine + rec-marine portable plastic: full
                // swing, 0.78 Wade correction (clcevems.f :613-618).
                wadeeq(
                    ctx.tank_fill_fraction,
                    ctx.tank_volume_gallons,
                    trvp,
                    diutmin,
                    diutmax,
                ) * 0.78
                    * frac
            } else {
                // Rec-marine tank variants. Slots 1 & 3 = 50% swing,
                // slots 2 & 4 = 20% swing (clcevems.f :620-628). Note
                // the Fortran source uses `j.eq.2.or.j.eq.4` for the
                // 50% case — slots 2 and 4 in 1-based, i.e. 1 and 3
                // in 0-based.
                let (dtmax, dtmin) = if j == 1 || j == 3 {
                    (tavg + swing * 0.5, tavg - swing * 0.5)
                } else {
                    (tavg + swing * 0.2, tavg - swing * 0.2)
                };
                wadeeq(
                    ctx.tank_fill_fraction,
                    ctx.tank_volume_gallons,
                    trvp,
                    dtmin,
                    dtmax,
                ) * 0.78
                    * frac
            };
            emiss += contrib;
            if emiss < 0.0 {
                outcome
                    .warnings
                    .push(EvapEmissionsWarning::NegativeDiurnalEmiss {
                        scc: ctx.scc.to_string(),
                        hp_avg: ctx.hp_avg,
                        tmax,
                        emiss,
                    });
                return Ok(0.0);
            }
            if frac == 1.0 {
                break;
            }
        }
        // The Fortran multiplies by emstmp (a multiplicative correction
        // factor for diurnal) at the end of the inner loop.
        emiss *= emstmp;
    }
    let mut temiss = emiss
        * CVTTON
        * det_ratio
        * adjems
        * ctx.time_adjustment
        * ctx.population
        * ctx.model_year_fraction
        * ctx.tech_fraction;
    if !ctx.daily_mode {
        temiss *= ctx.ndays;
    }
    Ok(temiss)
}

// =============================================================================
//   Ethanol-blend correction
// =============================================================================

const ETHANOL_POWER: f32 = 0.40;
const ETHANOL_E10_VOL: f32 = 0.10;
const ETHANOL_E20_VOL_CAP: f32 = 0.20;
const ETHANOL_E85_VOL_CAP: f32 = 0.85;

/// Apply the E10/E85 permeation-effect correction shared by all five
/// permeation species (`clcevems.f` :353-368 and four near-identical
/// copies for hose/neck/sr/vent).
///
/// `e10_factor_raw` is the raw `tnke10fac` / `hose10fac` / etc.
/// supplied by the caller (often 1.0 for default tech types). The
/// correction:
///
/// 1. Inflate the E10 factor to 2.0 when the species' control-tech
///    slot in the evap tech code is non-zero AND the raw factor is
///    not 1.0 (the Fortran source comment notes this is a
///    floating-point-equal check that's intentional — the values come
///    from input files, not runtime computation).
/// 2. Compute the per-mass adjustment via the documented power-law
///    against volume fraction, capped at E85.
/// 3. Blend back via market fraction:
///    `temiss * (1 - mkt) + temeth * mkt`.
fn apply_ethanol_correction(
    ctx: &EvapEmissionsCalcContext<'_>,
    species: EvapSpecies,
    temiss: f32,
    e10_factor_raw: f32,
) -> f32 {
    let eth_mkt = 0.01 * ctx.ethanol.market_percent;
    let eth_vol = 0.01 * ctx.ethanol.volume_percent;
    if !(eth_mkt > 0.0 && eth_vol > 0.0) {
        return temiss;
    }
    let mut e10fac = e10_factor_raw;
    if let Some(slot_idx) = species.tech_code_slot() {
        let ch = ctx
            .tech_type_code
            .as_bytes()
            .get(slot_idx)
            .copied()
            .unwrap_or(b'0');
        if ch != b'0' && e10fac != 1.0 {
            e10fac = 2.0;
        }
    }
    let temeth = if eth_vol <= ETHANOL_E20_VOL_CAP {
        temiss * (1.0 + (e10fac - 1.0) * (10.0 * eth_vol).powf(ETHANOL_POWER))
    } else {
        temiss
            * (1.0 + (e10fac - 1.0) * (10.0 * ETHANOL_E20_VOL_CAP).powf(ETHANOL_POWER))
            * (1.0
                - ((eth_vol.min(ETHANOL_E85_VOL_CAP) - ETHANOL_E20_VOL_CAP) / 0.8)
                    .powf(1.0 / ETHANOL_POWER))
    };
    let _ = ETHANOL_E10_VOL; // documented constant; the runtime formula uses eth_vol directly
    temiss * (1.0 - eth_mkt) + temeth * eth_mkt
}

// =============================================================================
//   evemfclc — entry point
// =============================================================================

/// Per-iteration inputs to [`calculate_evaporative_factors`].
///
/// Ports the COMMON-block reads of `evemfclc.f` (`ascevp`, `tecevp`,
/// `evhpcb`/`evhpce`, `iyrevp`, `evpfac`, `nevpfc`, `ldetfl`,
/// `lfacfl`, `ifuel`, and the evap tech-type table).
#[derive(Debug, Clone)]
pub struct EvapFactorsCalcContext<'a> {
    /// 10-character SCC code (Fortran `asccod`).
    pub scc: &'a str,
    /// Average HP for the current HP category (Fortran `hpavga`,
    /// i.e. `avghpc(icurec)`).
    pub hp_avg: f32,
    /// Episode year being evaluated (Fortran `iyrin`).
    pub year: i32,
    /// Equipment fuel type (Fortran `ifuel`). Diesel/CNG/LPG zero
    /// every evap species and return early.
    pub fuel_type: FuelType,
    /// Evap tech-type codes for this iteration's tech slots. Slot 0
    /// is unused (Fortran `i=0` is the "global" slot), `[1..]` holds
    /// the per-tech-type codes. The Rust port stores them in a Vec
    /// and treats index 0 as the global fallback, mirroring the
    /// Fortran `evtecnam(0)` convention.
    pub evap_tech_codes: &'a [&'a str],
    /// Evap tech-type fractions for this iteration (`evtecfrc`).
    /// Same indexing as `evap_tech_codes` (slot 0 → global).
    pub evap_tech_fractions: &'a [f32],
    /// Per-species emission-factor records (one slice per species
    /// slot). The slice for each species replaces the
    /// `evpfac(:,idxspc)` / `tecevp(:,idxspc)` columns the Fortran
    /// indexes directly. Slots not relevant for this run (e.g. the
    /// non-evap pollutants) hold an empty slice.
    pub evap_records: &'a [&'a [EvapEmissionFactorRecord]],
    /// Whether an emission-factor file was supplied for the given
    /// species (`lfacfl`).
    pub has_factor_file: &'a [bool],
    /// Whether a deterioration-factor file was supplied for the
    /// given species (`ldetfl`).
    pub has_deterioration_file: &'a [bool],
    /// Per-species deterioration records (the global flat
    /// [`DeteriorationRecord`] vector). The lookup in
    /// [`calculate_evaporative_factors`] filters by `(tech_type,
    /// pollutant)` per [`find_deterioration`].
    pub deterioration_records: &'a [DeteriorationRecord],
}

/// Per-species output of [`calculate_evaporative_factors`].
#[derive(Debug, Clone, PartialEq)]
pub struct EvapFactorsForSpecies {
    /// Species this row describes.
    pub species: EvapSpecies,
    /// Per-tech-slot emission factor. Slot 0 unused (global slot
    /// fallback is folded in already). [`RMISS`] indicates "no
    /// record found and tech-fraction non-zero" (mirrors
    /// `evemfclc.f` :110-111). 0.0 indicates "tech-fraction was
    /// zero so the factor is not consulted downstream".
    pub factors: Vec<f32>,
    /// Per-tech-slot A-coefficient of the deterioration equation.
    pub deterioration_a: Vec<f32>,
    /// Per-tech-slot B-coefficient of the deterioration equation.
    pub deterioration_b: Vec<f32>,
    /// Per-tech-slot deterioration cap (max age).
    pub deterioration_cap: Vec<f32>,
}

/// Outcome of [`calculate_evaporative_factors`].
#[derive(Debug, Clone, PartialEq, Default)]
pub struct EvapFactorsOutcome {
    /// One [`EvapFactorsForSpecies`] per evap species in
    /// `IDXDIU..=IDXRLS`. Other slots are not populated.
    pub per_species: Vec<EvapFactorsForSpecies>,
    /// "No emission factors found" warnings, mirroring
    /// `evemfclc.f` :243-249.
    pub warnings: Vec<String>,
}

/// Compute the per-species evaporative emission factors and
/// deterioration coefficients for one model-iteration tuple.
///
/// Ports `evemfclc.f`. For each evap species (`IDXDIU..=IDXRLS`) the
/// function:
///
/// 1. Initialises the per-tech-slot factor to [`RMISS`] when the
///    tech fraction is non-zero, 0 otherwise (`evemfclc.f` :108-119).
/// 2. Zeroes the deterioration coefficients.
/// 3. Skips diesel/CNG/LPG fuels (sets factors to zero).
/// 4. Skips species without an emission-factor file (`lfacfl`).
/// 5. Looks up the global-tech-slot factor via
///    [`find_evap_emission_factor`] with `tech = TECH_DEFAULT`
///    (Fortran `TECDEF`).
/// 6. For each non-global tech slot, derives the per-species sub-tech
///    code from the evap tech-type string (`E` + 8 digits) and looks
///    up the matching record; falls back to the global slot when no
///    sub-tech-specific record matches.
/// 7. Pulls deterioration coefficients from [`find_deterioration`].
pub fn calculate_evaporative_factors(
    ctx: &EvapFactorsCalcContext<'_>,
) -> Result<EvapFactorsOutcome> {
    if ctx.has_factor_file.len() < MXPOL
        || ctx.has_deterioration_file.len() < MXPOL
        || ctx.evap_records.len() < MXPOL
    {
        return Err(Error::Config(format!(
            "evap factor input slices must have length >= MXPOL ({MXPOL})"
        )));
    }
    if ctx.evap_tech_codes.len() != ctx.evap_tech_fractions.len() {
        return Err(Error::Config(format!(
            "evap_tech_codes/evap_tech_fractions length mismatch: {} vs {}",
            ctx.evap_tech_codes.len(),
            ctx.evap_tech_fractions.len()
        )));
    }

    let mut outcome = EvapFactorsOutcome::default();
    let n_tech = ctx.evap_tech_codes.len();

    for &species in &EvapSpecies::ALL {
        let slot = species.slot();
        let mut row = EvapFactorsForSpecies {
            species,
            factors: vec![0.0; n_tech],
            deterioration_a: vec![0.0; n_tech],
            deterioration_b: vec![1.0; n_tech],
            deterioration_cap: vec![0.0; n_tech],
        };

        // (1) initial sentinel: RMISS where fraction != 0
        for i in 0..n_tech {
            if ctx.evap_tech_fractions[i] != 0.0 {
                row.factors[i] = RMISS;
            } else {
                row.factors[i] = 0.0;
            }
        }

        // (3) diesel/CNG/LPG zero everything and continue.
        if !ctx.fuel_type.has_full_evap_species() {
            for slot_v in row.factors.iter_mut() {
                *slot_v = 0.0;
            }
            outcome.per_species.push(row);
            continue;
        }

        // (4) skip if no EF file or spillage species.
        if !ctx.has_factor_file[slot] || species == EvapSpecies::Spillage {
            outcome.per_species.push(row);
            continue;
        }

        let records = ctx.evap_records[slot];

        // (5) global-tech lookup. Tech slot 0 = TECDEF.
        let global_match =
            find_evap_emission_factor(ctx.scc, TECH_DEFAULT, ctx.hp_avg, ctx.year, records);

        // Per-tech-slot resolution.
        let Some(ev_offset) = species.ev_tech_char_offset() else {
            outcome.per_species.push(row);
            continue;
        };
        for i in 1..n_tech {
            let code = ctx.evap_tech_codes[i];
            // tname = "E" + ev tech code[ev_offset-1]
            let sub_letter = code
                .as_bytes()
                .get(ev_offset - 1)
                .map(|b| *b as char)
                .unwrap_or('0');
            let tname = format!("E{sub_letter}");
            let match_idx = if tname != TECH_DEFAULT {
                find_evap_emission_factor(ctx.scc, &tname, ctx.hp_avg, ctx.year, records)
            } else {
                None
            };

            let final_idx = match_idx.or(global_match);
            if let Some(idx) = final_idx {
                if ctx.evap_tech_fractions[i] > 0.0 {
                    row.factors[i] = records[idx].factor;
                }
            } else if ctx.evap_tech_fractions[i] > 0.0 {
                outcome.warnings.push(format!(
                    "WARNING:  No evap emission factors found for: \
                     scc={} tech={} sub={} hp={} year={} species={:?}",
                    ctx.scc, code, tname, ctx.hp_avg, ctx.year, species
                ));
            }

            // (7) deterioration coefficients.
            if ctx.has_deterioration_file[slot] {
                let all_idx = find_deterioration(
                    TECH_DEFAULT,
                    &species_pollutant_name(species),
                    ctx.deterioration_records,
                );
                let det_idx = find_deterioration(
                    &tname,
                    &species_pollutant_name(species),
                    ctx.deterioration_records,
                );
                let chosen = det_idx.or(all_idx);
                if let Some(idx) = chosen {
                    row.deterioration_a[i] = ctx.deterioration_records[idx].a;
                    row.deterioration_b[i] = ctx.deterioration_records[idx].b;
                    row.deterioration_cap[i] = ctx.deterioration_records[idx].cap;
                }
            }
        }

        outcome.per_species.push(row);
    }
    Ok(outcome)
}

/// Canonical pollutant-name string used by deterioration records for
/// each evap species. Matches the strings the Fortran writes when it
/// indexes `polnam(idxspc)` (rdnrper.f / blknon.f).
fn species_pollutant_name(species: EvapSpecies) -> String {
    match species {
        EvapSpecies::Diurnal => "DIU".to_string(),
        EvapSpecies::TankPermeation => "TKP".to_string(),
        EvapSpecies::HosePermeation => "HOS".to_string(),
        EvapSpecies::NeckPermeation => "NCK".to_string(),
        EvapSpecies::SupplyReturnPermeation => "SR".to_string(),
        EvapSpecies::VentPermeation => "VNT".to_string(),
        EvapSpecies::HotSoak => "HSK".to_string(),
        EvapSpecies::Displacement => "DIS".to_string(),
        EvapSpecies::Spillage => "SPL".to_string(),
        EvapSpecies::RunningLoss => "RLS".to_string(),
    }
}

// =============================================================================
//   Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::consts::MXDAYS;
    use crate::input::deterioration::DeteriorationRecord;
    use crate::input::evemfc::{EvapEmissionFactorRecord, EvapEmissionUnits};

    fn pol_bools(slots: &[usize]) -> Vec<bool> {
        let mut v = vec![false; MXPOL];
        for &s in slots {
            v[s] = true;
        }
        v
    }

    #[allow(clippy::too_many_arguments)]
    fn make_context<'a>(
        scc: &'a str,
        fuel: FuelType,
        emission_factor: &'a [f32],
        has_factor_file: &'a [bool],
        e10_factor: &'a [f32],
        day_adjustment: &'a [f32],
        deterioration_cap: &'a [f32],
        deterioration_a: &'a [f32],
        deterioration_b: &'a [f32],
    ) -> EvapEmissionsCalcContext<'a> {
        EvapEmissionsCalcContext {
            scc,
            fips_code: "06037",
            hp_avg: 75.0,
            fuel_type: fuel,
            day_range: DayRange::SINGLE,
            period_type: PeriodType::Annual,
            daily_mode: false,
            meteorology: Meteorology::Static {
                max_temp: 80.0,
                min_temp: 50.0,
                ambient_temp: 65.0,
                rvp: 9.0,
            },
            ndays: 30.0,
            tech_fraction: 1.0,
            tech_type_code: "E00000000",
            population: 100.0,
            model_year_fraction: 1.0,
            deterioration_age: 0.0,
            deterioration_cap,
            deterioration_a,
            deterioration_b,
            emission_factor,
            day_adjustment,
            time_adjustment: 1.0,
            temporal_factor: 1.0,
            activity: 100.0,
            hot_soaks_per_hour: 0.5,
            diurnal_fractions: [1.0, 0.0, 0.0, 0.0, 0.0],
            refueling: RefuelingContext {
                mode: Some(RefuelingMode::Pump),
                tank_volume_gallons: 10.0,
                fuel_consumption_gallons: 100.0,
                stage2_pump_factor: 1.0,
            },
            tank_metal_fraction: 0.0,
            tank_fill_fraction: 0.5,
            tank_volume_gallons: 10.0,
            hose_metal_fraction: 0.0,
            hose_length_m: 1.0,
            hose_diameter_m: 0.02,
            neck_length_m: 0.5,
            neck_diameter_m: 0.04,
            supply_return_length_m: 2.0,
            supply_return_diameter_m: 0.025,
            vent_length_m: 0.5,
            vent_diameter_m: 0.015,
            e10_factor,
            ethanol: EthanolBlend::none(),
            has_factor_file,
        }
    }

    #[test]
    fn diesel_only_runs_spillage_branch() {
        // Diesel runs SPILLAGE only — with refmod=None, that yields 0.
        let ef = vec![0.0; MXPOL];
        let no_file = vec![false; MXPOL];
        let e10 = vec![1.0; MXPOL];
        let day_adj = vec![1.0; MXPOL * MXDAYS];
        let det_cap = vec![100.0; MXPOL];
        let det_a = vec![0.0; MXPOL];
        let det_b = vec![1.0; MXPOL];
        let mut ctx = make_context(
            "2270002003",
            FuelType::Diesel,
            &ef,
            &no_file,
            &e10,
            &day_adj,
            &det_cap,
            &det_a,
            &det_b,
        );
        ctx.refueling.mode = None;
        let mut emsday = vec![0.0; MXPOL];
        let mut emsbmy = vec![0.0; MXPOL];
        let outcome = calculate_evaporative_emissions(&ctx, &mut emsday, &mut emsbmy).unwrap();
        assert!(outcome.warnings.is_empty());
        for v in emsday.iter() {
            assert_eq!(*v, 0.0);
        }
    }

    #[test]
    fn spillage_pump_mode_uses_pmpfac() {
        // SPILLAGE, gasoline, PUMP, with spillage file present.
        let mut ef = vec![0.0; MXPOL];
        ef[EvapSpecies::Spillage.slot()] = 0.0; // not consulted for spillage
        let has = pol_bools(&[EvapSpecies::Spillage.slot()]);
        let e10 = vec![1.0; MXPOL];
        let day_adj = vec![1.0; MXPOL * MXDAYS];
        let det_cap = vec![100.0; MXPOL];
        let det_a = vec![0.0; MXPOL];
        let det_b = vec![1.0; MXPOL];
        let mut ctx = make_context(
            "2270002003",
            FuelType::Gasoline2Stroke,
            &ef,
            &has,
            &e10,
            &day_adj,
            &det_cap,
            &det_a,
            &det_b,
        );
        // Disable every species except SPILLAGE so the single-day
        // loop only fires the spillage branch.
        let mut has_only_spl = vec![false; MXPOL];
        has_only_spl[EvapSpecies::Spillage.slot()] = true;
        ctx.has_factor_file = &has_only_spl;
        let mut emsday = vec![0.0; MXPOL];
        let mut emsbmy = vec![0.0; MXPOL];
        calculate_evaporative_emissions(&ctx, &mut emsday, &mut emsbmy).unwrap();
        // expected: PMPFAC / 10 * CVTTON * 100 = 3.6/10 * 1.102311e-6 * 100
        let expected = (PMPFAC / 10.0) * CVTTON * 100.0;
        let actual = emsday[EvapSpecies::Spillage.slot()];
        assert!(
            (actual - expected).abs() < 1e-10,
            "spillage emsday {actual} vs expected {expected}"
        );
    }

    #[test]
    fn spillage_container_mode_uses_cntfac() {
        let ef = vec![0.0; MXPOL];
        let mut has = vec![false; MXPOL];
        has[EvapSpecies::Spillage.slot()] = true;
        let e10 = vec![1.0; MXPOL];
        let day_adj = vec![1.0; MXPOL * MXDAYS];
        let det_cap = vec![100.0; MXPOL];
        let det_a = vec![0.0; MXPOL];
        let det_b = vec![1.0; MXPOL];
        let mut ctx = make_context(
            "2270002003",
            FuelType::Gasoline4Stroke,
            &ef,
            &has,
            &e10,
            &day_adj,
            &det_cap,
            &det_a,
            &det_b,
        );
        ctx.refueling.mode = Some(RefuelingMode::Container);
        let mut emsday = vec![0.0; MXPOL];
        let mut emsbmy = vec![0.0; MXPOL];
        calculate_evaporative_emissions(&ctx, &mut emsday, &mut emsbmy).unwrap();
        let expected = (CNTFAC / 10.0) * CVTTON * 100.0;
        assert!(
            (emsday[EvapSpecies::Spillage.slot()] - expected).abs() < 1e-10,
            "container spillage {} vs expected {expected}",
            emsday[EvapSpecies::Spillage.slot()]
        );
    }

    #[test]
    fn tank_permeation_uses_sfcarea_formula() {
        let mut ef = vec![0.0; MXPOL];
        ef[EvapSpecies::TankPermeation.slot()] = 0.5;
        let mut has = vec![false; MXPOL];
        has[EvapSpecies::Spillage.slot()] = true; // sentinel required for permeation
        has[EvapSpecies::TankPermeation.slot()] = true;
        let e10 = vec![1.0; MXPOL];
        let day_adj = vec![1.0; MXPOL * MXDAYS];
        let det_cap = vec![100.0; MXPOL];
        let det_a = vec![0.0; MXPOL];
        let det_b = vec![1.0; MXPOL];
        let mut ctx = make_context(
            "2270002003",
            FuelType::Gasoline4Stroke,
            &ef,
            &has,
            &e10,
            &day_adj,
            &det_cap,
            &det_a,
            &det_b,
        );
        ctx.ndays = 1.0;
        let mut emsday = vec![0.0; MXPOL];
        let mut emsbmy = vec![0.0; MXPOL];
        calculate_evaporative_emissions(&ctx, &mut emsday, &mut emsbmy).unwrap();
        // sfcarea = 0.15 * sqrt((144/4) - 1) = 0.15 * sqrt(35)
        let sfcarea = 0.15_f32 * 35.0_f32.sqrt();
        let emiss = 0.5 * 1.0 * sfcarea;
        let expected = emiss * CVTTON * 1.0 * 1.0 * 1.0 * 100.0 * 1.0 * 1.0 * 1.0; // ndays multiplier
        let actual = emsday[EvapSpecies::TankPermeation.slot()];
        assert!(
            (actual - expected).abs() / expected < 1e-5,
            "tank perm {actual} vs expected {expected}"
        );
    }

    #[test]
    fn hose_permeation_uses_pi_length_diameter() {
        let mut ef = vec![0.0; MXPOL];
        ef[EvapSpecies::HosePermeation.slot()] = 1.0;
        let mut has = vec![false; MXPOL];
        has[EvapSpecies::Spillage.slot()] = true;
        has[EvapSpecies::HosePermeation.slot()] = true;
        let e10 = vec![1.0; MXPOL];
        let day_adj = vec![1.0; MXPOL * MXDAYS];
        let det_cap = vec![100.0; MXPOL];
        let det_a = vec![0.0; MXPOL];
        let det_b = vec![1.0; MXPOL];
        let mut ctx = make_context(
            "2270002003",
            FuelType::Gasoline4Stroke,
            &ef,
            &has,
            &e10,
            &day_adj,
            &det_cap,
            &det_a,
            &det_b,
        );
        ctx.ndays = 1.0;
        let mut emsday = vec![0.0; MXPOL];
        let mut emsbmy = vec![0.0; MXPOL];
        calculate_evaporative_emissions(&ctx, &mut emsday, &mut emsbmy).unwrap();
        let sfcarea = std::f32::consts::PI * 1.0 * 0.02;
        let emiss = 1.0 * 1.0 * sfcarea;
        let expected = emiss * CVTTON * 1.0 * 1.0 * 1.0 * 100.0 * 1.0 * 1.0;
        let actual = emsday[EvapSpecies::HosePermeation.slot()];
        assert!(
            (actual - expected).abs() / expected < 1e-5,
            "hose perm {actual} vs expected {expected}"
        );
    }

    #[test]
    fn neck_supply_vent_use_pi_formula_without_metal_fraction() {
        let mut ef = vec![0.0; MXPOL];
        ef[EvapSpecies::NeckPermeation.slot()] = 0.3;
        let mut has = vec![false; MXPOL];
        has[EvapSpecies::Spillage.slot()] = true;
        has[EvapSpecies::NeckPermeation.slot()] = true;
        let e10 = vec![1.0; MXPOL];
        let day_adj = vec![1.0; MXPOL * MXDAYS];
        let det_cap = vec![100.0; MXPOL];
        let det_a = vec![0.0; MXPOL];
        let det_b = vec![1.0; MXPOL];
        let mut ctx = make_context(
            "2270002003",
            FuelType::Gasoline4Stroke,
            &ef,
            &has,
            &e10,
            &day_adj,
            &det_cap,
            &det_a,
            &det_b,
        );
        ctx.ndays = 1.0;
        let mut emsday = vec![0.0; MXPOL];
        let mut emsbmy = vec![0.0; MXPOL];
        calculate_evaporative_emissions(&ctx, &mut emsday, &mut emsbmy).unwrap();
        let sfcarea = std::f32::consts::PI * 0.5 * 0.04;
        let emiss = 0.3 * sfcarea;
        let expected = emiss * CVTTON * 100.0;
        let actual = emsday[EvapSpecies::NeckPermeation.slot()];
        assert!(
            (actual - expected).abs() / expected < 1e-5,
            "neck perm {actual} vs expected {expected}"
        );
    }

    #[test]
    fn diurnal_uses_wadeeq_for_full_swing_when_tmax_above_diumin() {
        let mut ef = vec![0.0; MXPOL];
        ef[EvapSpecies::Diurnal.slot()] = 1.0; // multiplicative correction
        let mut has = vec![false; MXPOL];
        has[EvapSpecies::Diurnal.slot()] = true;
        let e10 = vec![1.0; MXPOL];
        let day_adj = vec![1.0; MXPOL * MXDAYS];
        let det_cap = vec![100.0; MXPOL];
        let det_a = vec![0.0; MXPOL];
        let det_b = vec![1.0; MXPOL];
        let mut ctx = make_context(
            "2270002003",
            FuelType::Gasoline4Stroke,
            &ef,
            &has,
            &e10,
            &day_adj,
            &det_cap,
            &det_a,
            &det_b,
        );
        ctx.ndays = 1.0;
        // Make wadeeq fire: tmax=80, tmin=50 (both > DIUMIN), and
        // diufrac=[1,0,0,0,0] selects the j=1 branch with 0.78 mult.
        ctx.diurnal_fractions = [1.0, 0.0, 0.0, 0.0, 0.0];
        let mut emsday = vec![0.0; MXPOL];
        let mut emsbmy = vec![0.0; MXPOL];
        calculate_evaporative_emissions(&ctx, &mut emsday, &mut emsbmy).unwrap();
        let wade = wadeeq(0.5, 10.0, 9.0, 50.0, 80.0);
        let emiss = wade * 0.78 * 1.0; // emiss = wadeeq * 0.78 * diufrac[0]; then * emstmp(1.0)
        let expected = emiss * CVTTON * 100.0;
        let actual = emsday[EvapSpecies::Diurnal.slot()];
        assert!(
            (actual - expected).abs() / expected.abs().max(1e-12) < 1e-4,
            "diurnal {actual} vs expected {expected}"
        );
    }

    #[test]
    fn diurnal_skips_when_tmax_at_or_below_diumin() {
        let mut ef = vec![0.0; MXPOL];
        ef[EvapSpecies::Diurnal.slot()] = 1.0;
        let mut has = vec![false; MXPOL];
        has[EvapSpecies::Diurnal.slot()] = true;
        let e10 = vec![1.0; MXPOL];
        let day_adj = vec![1.0; MXPOL * MXDAYS];
        let det_cap = vec![100.0; MXPOL];
        let det_a = vec![0.0; MXPOL];
        let det_b = vec![1.0; MXPOL];
        let mut ctx = make_context(
            "2270002003",
            FuelType::Gasoline4Stroke,
            &ef,
            &has,
            &e10,
            &day_adj,
            &det_cap,
            &det_a,
            &det_b,
        );
        ctx.meteorology = Meteorology::Static {
            max_temp: 35.0, // < DIUMIN=40
            min_temp: 20.0,
            ambient_temp: 28.0,
            rvp: 9.0,
        };
        ctx.diurnal_fractions = [1.0, 0.0, 0.0, 0.0, 0.0];
        let mut emsday = vec![0.0; MXPOL];
        let mut emsbmy = vec![0.0; MXPOL];
        calculate_evaporative_emissions(&ctx, &mut emsday, &mut emsbmy).unwrap();
        assert_eq!(emsday[EvapSpecies::Diurnal.slot()], 0.0);
    }

    #[test]
    fn hot_soak_uses_activity_times_soaks_per_hour() {
        let mut ef = vec![0.0; MXPOL];
        ef[EvapSpecies::HotSoak.slot()] = 2.0; // grams/event
        let mut has = vec![false; MXPOL];
        has[EvapSpecies::Spillage.slot()] = true;
        has[EvapSpecies::HotSoak.slot()] = true;
        let e10 = vec![1.0; MXPOL];
        let day_adj = vec![1.0; MXPOL * MXDAYS];
        let det_cap = vec![100.0; MXPOL];
        let det_a = vec![0.0; MXPOL];
        let det_b = vec![1.0; MXPOL];
        let mut ctx = make_context(
            "2270002003",
            FuelType::Gasoline4Stroke,
            &ef,
            &has,
            &e10,
            &day_adj,
            &det_cap,
            &det_a,
            &det_b,
        );
        ctx.ndays = 1.0;
        ctx.activity = 100.0;
        ctx.hot_soaks_per_hour = 0.5;
        let mut emsday = vec![0.0; MXPOL];
        let mut emsbmy = vec![0.0; MXPOL];
        calculate_evaporative_emissions(&ctx, &mut emsday, &mut emsbmy).unwrap();
        let emiss = 100.0 * 0.5 * 2.0;
        let expected = emiss * CVTTON * 100.0;
        let actual = emsday[EvapSpecies::HotSoak.slot()];
        assert!(
            (actual - expected).abs() / expected < 1e-5,
            "hot soak {actual} vs expected {expected}"
        );
    }

    #[test]
    fn running_loss_uses_emstmp_times_activity() {
        let mut ef = vec![0.0; MXPOL];
        ef[EvapSpecies::RunningLoss.slot()] = 0.4;
        let mut has = vec![false; MXPOL];
        has[EvapSpecies::RunningLoss.slot()] = true;
        let e10 = vec![1.0; MXPOL];
        let day_adj = vec![1.0; MXPOL * MXDAYS];
        let det_cap = vec![100.0; MXPOL];
        let det_a = vec![0.0; MXPOL];
        let det_b = vec![1.0; MXPOL];
        let mut ctx = make_context(
            "2270002003",
            FuelType::Gasoline4Stroke,
            &ef,
            &has,
            &e10,
            &day_adj,
            &det_cap,
            &det_a,
            &det_b,
        );
        ctx.ndays = 1.0;
        ctx.activity = 25.0;
        let mut emsday = vec![0.0; MXPOL];
        let mut emsbmy = vec![0.0; MXPOL];
        calculate_evaporative_emissions(&ctx, &mut emsday, &mut emsbmy).unwrap();
        let emiss = 0.4 * 25.0;
        let expected = emiss * CVTTON * 100.0;
        let actual = emsday[EvapSpecies::RunningLoss.slot()];
        assert!(
            (actual - expected).abs() / expected < 1e-5,
            "running loss {actual} vs expected {expected}"
        );
    }

    #[test]
    fn ethanol_correction_blends_market_fraction_when_volume_below_e20() {
        // Tank permeation with default tech (E00000000) and e10 raw
        // factor = 2 → no inflate (slot already non-default? no — the
        // E00000000 means all-zeros, so the check ch != '0' is false,
        // and the raw 2.0 stays as 2.0). With ethmkt=100, ethvpct=10,
        // temeth = temiss * (1 + 1 * 1.0^0.40) = 2 * temiss.
        let mut ef = vec![0.0; MXPOL];
        ef[EvapSpecies::TankPermeation.slot()] = 0.5;
        let mut has = vec![false; MXPOL];
        has[EvapSpecies::Spillage.slot()] = true;
        has[EvapSpecies::TankPermeation.slot()] = true;
        let mut e10 = vec![1.0; MXPOL];
        e10[EvapSpecies::TankPermeation.slot()] = 2.0;
        let day_adj = vec![1.0; MXPOL * MXDAYS];
        let det_cap = vec![100.0; MXPOL];
        let det_a = vec![0.0; MXPOL];
        let det_b = vec![1.0; MXPOL];
        let mut ctx = make_context(
            "2270002003",
            FuelType::Gasoline4Stroke,
            &ef,
            &has,
            &e10,
            &day_adj,
            &det_cap,
            &det_a,
            &det_b,
        );
        ctx.ndays = 1.0;
        ctx.ethanol = EthanolBlend {
            market_percent: 100.0,
            volume_percent: 10.0,
        };
        let mut emsday = vec![0.0; MXPOL];
        let mut emsbmy = vec![0.0; MXPOL];
        calculate_evaporative_emissions(&ctx, &mut emsday, &mut emsbmy).unwrap();
        let sfcarea = 0.15_f32 * 35.0_f32.sqrt();
        let emiss = 0.5 * 1.0 * sfcarea;
        let base = emiss * CVTTON * 1.0 * 1.0 * 1.0 * 100.0 * 1.0 * 1.0;
        // ethmktfrc=1.0, ethvfrc=0.1, e10fac=2.0 → temeth = base * (1 + (2-1) * (10*0.1)^0.4) = base * 2
        // blended = base * 0 + 2*base * 1 = 2 * base
        let expected = 2.0 * base;
        let actual = emsday[EvapSpecies::TankPermeation.slot()];
        assert!(
            (actual - expected).abs() / expected < 1e-4,
            "ethanol-corrected tank {actual} vs expected {expected}"
        );
    }

    #[test]
    fn ethanol_correction_caps_volume_at_e85() {
        // Volume = 60% (> 20%): the second branch of apply_ethanol_correction
        // fires. Expected formula:
        //   temeth = temiss * (1 + (e10fac-1) * (10*0.2)^pwr)
        //                   * (1 - ((min(ethvfrc, 0.85) - 0.2) / 0.8)^(1/pwr))
        let mut ef = vec![0.0; MXPOL];
        ef[EvapSpecies::TankPermeation.slot()] = 0.5;
        let mut has = vec![false; MXPOL];
        has[EvapSpecies::Spillage.slot()] = true;
        has[EvapSpecies::TankPermeation.slot()] = true;
        let mut e10 = vec![1.0; MXPOL];
        e10[EvapSpecies::TankPermeation.slot()] = 2.0;
        let day_adj = vec![1.0; MXPOL * MXDAYS];
        let det_cap = vec![100.0; MXPOL];
        let det_a = vec![0.0; MXPOL];
        let det_b = vec![1.0; MXPOL];
        let mut ctx = make_context(
            "2270002003",
            FuelType::Gasoline4Stroke,
            &ef,
            &has,
            &e10,
            &day_adj,
            &det_cap,
            &det_a,
            &det_b,
        );
        ctx.ndays = 1.0;
        ctx.ethanol = EthanolBlend {
            market_percent: 100.0,
            volume_percent: 60.0,
        };
        let mut emsday = vec![0.0; MXPOL];
        let mut emsbmy = vec![0.0; MXPOL];
        calculate_evaporative_emissions(&ctx, &mut emsday, &mut emsbmy).unwrap();
        let sfcarea = 0.15_f32 * 35.0_f32.sqrt();
        let emiss = 0.5 * 1.0 * sfcarea;
        let base = emiss * CVTTON * 100.0;
        let pow = ETHANOL_POWER;
        let f1 = 1.0 + (2.0 - 1.0) * (10.0_f32 * 0.2).powf(pow);
        let f2 = 1.0 - ((0.6_f32 - 0.2) / 0.8).powf(1.0 / pow);
        let expected = base * f1 * f2;
        let actual = emsday[EvapSpecies::TankPermeation.slot()];
        assert!(
            (actual - expected).abs() / expected.abs() < 1e-4,
            "high-vol ethanol {actual} vs expected {expected}"
        );
    }

    #[test]
    fn ethanol_correction_skipped_for_zero_market_or_volume() {
        let mut ef = vec![0.0; MXPOL];
        ef[EvapSpecies::TankPermeation.slot()] = 0.5;
        let mut has = vec![false; MXPOL];
        has[EvapSpecies::Spillage.slot()] = true;
        has[EvapSpecies::TankPermeation.slot()] = true;
        let mut e10 = vec![1.0; MXPOL];
        e10[EvapSpecies::TankPermeation.slot()] = 2.0;
        let day_adj = vec![1.0; MXPOL * MXDAYS];
        let det_cap = vec![100.0; MXPOL];
        let det_a = vec![0.0; MXPOL];
        let det_b = vec![1.0; MXPOL];
        let mut ctx = make_context(
            "2270002003",
            FuelType::Gasoline4Stroke,
            &ef,
            &has,
            &e10,
            &day_adj,
            &det_cap,
            &det_a,
            &det_b,
        );
        ctx.ndays = 1.0;
        ctx.ethanol = EthanolBlend {
            market_percent: 0.0,
            volume_percent: 10.0,
        };
        let mut emsday = vec![0.0; MXPOL];
        let mut emsbmy = vec![0.0; MXPOL];
        calculate_evaporative_emissions(&ctx, &mut emsday, &mut emsbmy).unwrap();
        let sfcarea = 0.15_f32 * 35.0_f32.sqrt();
        let emiss = 0.5 * 1.0 * sfcarea;
        let expected = emiss * CVTTON * 100.0;
        let actual = emsday[EvapSpecies::TankPermeation.slot()];
        assert!(
            (actual - expected).abs() / expected < 1e-5,
            "no-ethanol tank {actual} vs expected {expected}"
        );
    }

    #[test]
    fn deterioration_ratio_below_cap_uses_age() {
        let r = deterioration_ratio(2.0, 10.0, 0.1, 1.0);
        // 1 + 0.1 * 2^1 = 1.2
        assert!((r - 1.2).abs() < 1e-6);
    }

    #[test]
    fn deterioration_ratio_above_cap_clamps_to_cap() {
        let r = deterioration_ratio(20.0, 10.0, 0.1, 1.0);
        // 1 + 0.1 * 10^1 = 2.0
        assert!((r - 2.0).abs() < 1e-6);
    }

    #[test]
    fn day_range_for_period_static_when_not_daily() {
        let period = PeriodConfig {
            period_type: PeriodType::Annual,
            summary_type: crate::input::period::SummaryType::TotalPeriod,
            episode_year: 2020,
            season: None,
            month: None,
            day_kind: None,
            growth_year: 2020,
            technology_year: 2020,
            warnings: Vec::new(),
        };
        let range = day_range_for_period(&period, false);
        assert_eq!(range.begin, 1);
        assert_eq!(range.end, 1);
        assert!(!range.skip);
    }

    #[test]
    fn day_range_for_period_annual_daily_covers_year() {
        let period = PeriodConfig {
            period_type: PeriodType::Annual,
            summary_type: crate::input::period::SummaryType::TotalPeriod,
            episode_year: 2020,
            season: None,
            month: None,
            day_kind: None,
            growth_year: 2020,
            technology_year: 2020,
            warnings: Vec::new(),
        };
        let range = day_range_for_period(&period, true);
        assert_eq!(range.begin, 1);
        assert_eq!(range.end, 365);
    }

    #[test]
    fn day_range_for_period_winter_seasonal_skips_summer() {
        let period = PeriodConfig {
            period_type: PeriodType::Seasonal,
            summary_type: crate::input::period::SummaryType::TotalPeriod,
            episode_year: 2020,
            season: Some(Season::Winter),
            month: None,
            day_kind: None,
            growth_year: 2020,
            technology_year: 2020,
            warnings: Vec::new(),
        };
        let range = day_range_for_period(&period, true);
        assert_eq!(range.begin, 1);
        assert_eq!(range.end, 365);
        assert!(range.skip);
        assert_eq!(range.skip_begin, 60); // March 1
        assert_eq!(range.skip_end, 334); // November 30
    }

    #[test]
    fn day_range_for_period_summer_covers_jun_aug() {
        let period = PeriodConfig {
            period_type: PeriodType::Seasonal,
            summary_type: crate::input::period::SummaryType::TotalPeriod,
            episode_year: 2020,
            season: Some(Season::Summer),
            month: None,
            day_kind: None,
            growth_year: 2020,
            technology_year: 2020,
            warnings: Vec::new(),
        };
        let range = day_range_for_period(&period, true);
        assert_eq!(range.begin, 152); // Jun 1
        assert_eq!(range.end, 243); // Aug 31
    }

    #[test]
    fn day_range_for_period_monthly_jan_covers_first_31_days() {
        let period = PeriodConfig {
            period_type: PeriodType::Monthly,
            summary_type: crate::input::period::SummaryType::TotalPeriod,
            episode_year: 2020,
            season: None,
            month: Some(1),
            day_kind: None,
            growth_year: 2020,
            technology_year: 2020,
            warnings: Vec::new(),
        };
        let range = day_range_for_period(&period, true);
        assert_eq!(range.begin, 1);
        assert_eq!(range.end, 31);
    }

    #[test]
    fn calculate_evaporative_factors_returns_one_row_per_species() {
        let ctx = EvapFactorsCalcContext {
            scc: "2270002003",
            hp_avg: 75.0,
            year: 2020,
            fuel_type: FuelType::Gasoline4Stroke,
            evap_tech_codes: &["", "E11111111"],
            evap_tech_fractions: &[0.0, 1.0],
            evap_records: &[
                &[], // slot 0
                &[], // slot 1
                &[], // slot 2
                &[], // slot 3
                &[], // slot 4
                &[], // slot 5
                &[], // slot 6
                &[], // slot 7 (IDXDIU 8 → slot 7)
                &[], // 8 (IDXTKP)
                &[], // 9 (IDXHOS)
                &[], // 10 (IDXNCK)
                &[], // 11 (IDXSR)
                &[], // 12 (IDXVNT)
                &[], // 13 (IDXSOK)
                &[], // 14 (IDXDIS)
                &[], // 15 (IDXSPL)
                &[], // 16 (IDXRLS)
                &[],
                &[],
                &[],
                &[],
                &[],
                &[],
            ],
            has_factor_file: &[false; MXPOL],
            has_deterioration_file: &[false; MXPOL],
            deterioration_records: &[],
        };
        let outcome = calculate_evaporative_factors(&ctx).unwrap();
        assert_eq!(outcome.per_species.len(), 10);
    }

    #[test]
    fn calculate_evaporative_factors_diesel_zeros_all_species() {
        let mut has_file = vec![true; MXPOL];
        has_file[EvapSpecies::Spillage.slot()] = true;
        let empty_records: Vec<&[EvapEmissionFactorRecord]> = vec![&[]; MXPOL];
        let ctx = EvapFactorsCalcContext {
            scc: "2270002003",
            hp_avg: 75.0,
            year: 2020,
            fuel_type: FuelType::Diesel,
            evap_tech_codes: &["", "E11111111"],
            evap_tech_fractions: &[0.0, 1.0],
            evap_records: &empty_records,
            has_factor_file: &has_file,
            has_deterioration_file: &[false; MXPOL],
            deterioration_records: &[],
        };
        let outcome = calculate_evaporative_factors(&ctx).unwrap();
        for row in &outcome.per_species {
            for v in &row.factors {
                assert_eq!(*v, 0.0);
            }
        }
    }

    #[test]
    fn calculate_evaporative_factors_uses_evap_records_for_diurnal() {
        // One DIU record at scc=2270002003, year=2020, tech=E10000000
        let diu_records = [EvapEmissionFactorRecord {
            scc: "2270002003".to_string(),
            tech_type: "E1".to_string(),
            hp_min: 0.0,
            hp_max: 100.0,
            year: 2020,
            units: EvapEmissionUnits::Multiplier,
            factor: 0.42,
        }];
        let global_records = [EvapEmissionFactorRecord {
            scc: "2270002003".to_string(),
            tech_type: "ALL".to_string(),
            hp_min: 0.0,
            hp_max: 100.0,
            year: 2020,
            units: EvapEmissionUnits::Multiplier,
            factor: 0.10,
        }];
        // species_records[slot] = &[…]
        let mut species_records: Vec<&[EvapEmissionFactorRecord]> = vec![&[]; MXPOL];
        let merged: Vec<EvapEmissionFactorRecord> = diu_records
            .iter()
            .cloned()
            .chain(global_records.iter().cloned())
            .collect();
        species_records[EvapSpecies::Diurnal.slot()] = &merged;
        let mut has_file = vec![false; MXPOL];
        has_file[EvapSpecies::Diurnal.slot()] = true;
        let ctx = EvapFactorsCalcContext {
            scc: "2270002003",
            hp_avg: 50.0,
            year: 2020,
            fuel_type: FuelType::Gasoline4Stroke,
            evap_tech_codes: &["", "E10000000"],
            evap_tech_fractions: &[0.0, 1.0],
            evap_records: &species_records,
            has_factor_file: &has_file,
            has_deterioration_file: &[false; MXPOL],
            deterioration_records: &[],
        };
        let outcome = calculate_evaporative_factors(&ctx).unwrap();
        let diu = outcome
            .per_species
            .iter()
            .find(|r| r.species == EvapSpecies::Diurnal)
            .unwrap();
        // Tech slot 1 should resolve to the E1-record (0.42), not the global (0.10).
        assert!(
            (diu.factors[1] - 0.42).abs() < 1e-6,
            "got {:?}",
            diu.factors
        );
    }

    #[test]
    fn calculate_evaporative_factors_falls_back_to_global_when_subtech_missing() {
        let global_records = vec![EvapEmissionFactorRecord {
            scc: "2270002003".to_string(),
            tech_type: "ALL".to_string(),
            hp_min: 0.0,
            hp_max: 100.0,
            year: 2020,
            units: EvapEmissionUnits::Multiplier,
            factor: 0.10,
        }];
        let mut species_records: Vec<&[EvapEmissionFactorRecord]> = vec![&[]; MXPOL];
        species_records[EvapSpecies::Diurnal.slot()] = &global_records;
        let mut has_file = vec![false; MXPOL];
        has_file[EvapSpecies::Diurnal.slot()] = true;
        let ctx = EvapFactorsCalcContext {
            scc: "2270002003",
            hp_avg: 50.0,
            year: 2020,
            fuel_type: FuelType::Gasoline4Stroke,
            evap_tech_codes: &["", "E10000000"],
            evap_tech_fractions: &[0.0, 1.0],
            evap_records: &species_records,
            has_factor_file: &has_file,
            has_deterioration_file: &[false; MXPOL],
            deterioration_records: &[],
        };
        let outcome = calculate_evaporative_factors(&ctx).unwrap();
        let diu = outcome
            .per_species
            .iter()
            .find(|r| r.species == EvapSpecies::Diurnal)
            .unwrap();
        assert!(
            (diu.factors[1] - 0.10).abs() < 1e-6,
            "got {:?}",
            diu.factors
        );
    }

    #[test]
    fn calculate_evaporative_factors_warns_when_no_record_matches() {
        // No records, fraction > 0, EF file flagged: expect a warning.
        let mut species_records: Vec<&[EvapEmissionFactorRecord]> = vec![&[]; MXPOL];
        let _ = &mut species_records;
        let mut has_file = vec![false; MXPOL];
        has_file[EvapSpecies::Diurnal.slot()] = true;
        let ctx = EvapFactorsCalcContext {
            scc: "2270002003",
            hp_avg: 50.0,
            year: 2020,
            fuel_type: FuelType::Gasoline4Stroke,
            evap_tech_codes: &["", "E10000000"],
            evap_tech_fractions: &[0.0, 1.0],
            evap_records: &species_records,
            has_factor_file: &has_file,
            has_deterioration_file: &[false; MXPOL],
            deterioration_records: &[],
        };
        let outcome = calculate_evaporative_factors(&ctx).unwrap();
        assert!(
            outcome
                .warnings
                .iter()
                .any(|w| w.contains("DIU") || w.contains("Diurnal")),
            "{:?}",
            outcome.warnings
        );
        let diu = outcome
            .per_species
            .iter()
            .find(|r| r.species == EvapSpecies::Diurnal)
            .unwrap();
        // factors[1] stays at RMISS when fraction was non-zero and no
        // record was found (Fortran behaviour mirrored).
        assert_eq!(diu.factors[1], RMISS);
    }

    #[test]
    fn calculate_evaporative_factors_loads_deterioration_when_file_present() {
        let mut species_records: Vec<&[EvapEmissionFactorRecord]> = vec![&[]; MXPOL];
        let diu_records = vec![EvapEmissionFactorRecord {
            scc: "2270002003".to_string(),
            tech_type: "ALL".to_string(),
            hp_min: 0.0,
            hp_max: 100.0,
            year: 2020,
            units: EvapEmissionUnits::Multiplier,
            factor: 0.10,
        }];
        species_records[EvapSpecies::Diurnal.slot()] = &diu_records;
        let mut has_file = vec![false; MXPOL];
        has_file[EvapSpecies::Diurnal.slot()] = true;
        let mut has_det = vec![false; MXPOL];
        has_det[EvapSpecies::Diurnal.slot()] = true;
        let det_records = vec![DeteriorationRecord {
            pollutant: "DIU".to_string(),
            tech_type: "ALL".to_string(),
            a: 0.05,
            b: 0.8,
            cap: 10.0,
        }];
        let ctx = EvapFactorsCalcContext {
            scc: "2270002003",
            hp_avg: 50.0,
            year: 2020,
            fuel_type: FuelType::Gasoline4Stroke,
            evap_tech_codes: &["", "E10000000"],
            evap_tech_fractions: &[0.0, 1.0],
            evap_records: &species_records,
            has_factor_file: &has_file,
            has_deterioration_file: &has_det,
            deterioration_records: &det_records,
        };
        let outcome = calculate_evaporative_factors(&ctx).unwrap();
        let diu = outcome
            .per_species
            .iter()
            .find(|r| r.species == EvapSpecies::Diurnal)
            .unwrap();
        assert!((diu.deterioration_a[1] - 0.05).abs() < 1e-6);
        assert!((diu.deterioration_b[1] - 0.8).abs() < 1e-6);
        assert!((diu.deterioration_cap[1] - 10.0).abs() < 1e-6);
    }

    #[test]
    fn ev_tech_char_offset_matches_fortran_mapping() {
        assert_eq!(EvapSpecies::Diurnal.ev_tech_char_offset(), Some(2));
        assert_eq!(EvapSpecies::TankPermeation.ev_tech_char_offset(), Some(3));
        assert_eq!(EvapSpecies::HosePermeation.ev_tech_char_offset(), Some(4));
        // Three rec-marine hoses all map to the HOS column (4)
        assert_eq!(EvapSpecies::NeckPermeation.ev_tech_char_offset(), Some(4));
        assert_eq!(
            EvapSpecies::SupplyReturnPermeation.ev_tech_char_offset(),
            Some(4)
        );
        assert_eq!(EvapSpecies::VentPermeation.ev_tech_char_offset(), Some(4));
        // HotSoak (14) → 14 - 8 + 2 - 3 = 5
        assert_eq!(EvapSpecies::HotSoak.ev_tech_char_offset(), Some(5));
        assert_eq!(EvapSpecies::Displacement.ev_tech_char_offset(), Some(6));
        assert_eq!(EvapSpecies::Spillage.ev_tech_char_offset(), Some(7));
        assert_eq!(EvapSpecies::RunningLoss.ev_tech_char_offset(), Some(8));
    }

    #[test]
    fn fuel_type_has_full_evap_species_matches_clcevems_branch() {
        assert!(FuelType::Gasoline2Stroke.has_full_evap_species());
        assert!(FuelType::Gasoline4Stroke.has_full_evap_species());
        assert!(!FuelType::Diesel.has_full_evap_species());
        assert!(!FuelType::Lpg.has_full_evap_species());
        assert!(!FuelType::Cng.has_full_evap_species());
    }
}

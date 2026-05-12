//! Geography processing — county, state, subcounty, US-total,
//! state-from-national, and national.
//!
//! Cluster 2 of the seven functional groups (see
//! `ARCHITECTURE.md` § 2.2). The bulk of the spatial-allocation
//! logic, currently spread across six near-duplicate "process"
//! routines that handle different geography levels.
//!
//! # Fortran source files this module ports
//!
//! | File | Lines | Role | Task |
//! |---|---|---|---|
//! | `prccty.f` |   790 | County-level processing       | 109 |
//! | `prcsta.f` | 1,034 | State-level processing        | 110 |
//! | `prcsub.f` |   829 | Subcounty-level processing    | 109 |
//! | `prcus.f`  |   775 | US-total processing           | 111 |
//! | `prc1st.f` |   785 | State-from-national derivation | 110 |
//! | `prcnat.f` |   943 | National-level processing     | 111 |
//!
//! Tasks 109–111 port the routines as separate functions for
//! fidelity. Task 112 then refactors them into a single parameterised
//! routine, removing ~3,000 lines of duplication. The refactor is
//! gated on characterization-fixture parity (Phase 0).
//!
//! # Status
//!
//! - Task 111 ([`prcnat`], [`prcus`]) — ported. National and US-total
//!   processing.
//! - Tasks 109 and 110 — pending; their submodules will sit beside
//!   these.
//!
//! # Design overview
//!
//! The Fortran routines are top-of-stack orchestrators that read
//! roughly ten COMMON-block include files for state, then loop over
//! geographies × model-years × technology-types issuing emission and
//! adjustment calls. The Rust port preserves that orchestration shape
//! but:
//!
//! 1. Replaces COMMON-block reads with explicit fields on the
//!    per-record [`EquipmentRecord`] and per-run [`RunOptions`] inputs.
//! 2. Replaces dynamic lookup-and-table state with caller-supplied
//!    callback closures (`find_*`, `growth_factor_fn`, etc.) so the
//!    routines stay testable without dragging in every input parser.
//! 3. Replaces Fortran-style `IOWSTD`/`IOWMSG` writes plus the
//!    `wrtdat`/`wrtbmy`/`sitot` output routines with structured
//!    [`StateOutput`], [`ByModelYearOutput`], and [`SiAggregate`]
//!    records returned in [`GeographyOutput`]. Task 114 owns the
//!    actual writers; this module is format-agnostic.
//! 4. Replaces `chkwrn` non-fatal-warning side effects with explicit
//!    [`GeographyWarning`] entries on the output.
//!
//! The numerical core (deterioration, EF lookup, emission
//! accumulation, retrofit reduction) lives in the existing
//! [`crate::emissions`] and [`crate::population`] modules. This
//! module's job is the geographic loop and the cross-module wiring,
//! not the inner math.

pub mod prcnat;
pub mod prcus;

pub use prcnat::{process_national_record, NationalContext};
pub use prcus::{process_us_total_record, UsTotalContext};

use crate::common::consts::MXPOL;
use crate::emissions::exhaust::{ActivityUnit, FuelKind};

/// Run-time options pulled from `nonrdusr.inc` and friends. The
/// Fortran source reads these from COMMON blocks set by the option-file
/// parser ([`crate::input::options`]); the Rust port surfaces them as
/// an explicit parameter so the geography routines stay testable in
/// isolation.
#[derive(Debug, Clone, Copy)]
pub struct RunOptions {
    /// Episode year — `iepyr` in `nonrdusr.inc`. Anchors the
    /// MXAGYR-deep model-year loop.
    pub episode_year: i32,
    /// Growth year — `igryr` in `nonrdusr.inc`. Used by
    /// [`crate::population::age_distribution`].
    pub growth_year: i32,
    /// Technology year — `itchyr` in `nonrdusr.inc`. Capped via
    /// `min(model_year, itchyr)` when looking up tech-type data.
    pub tech_year: i32,
    /// Fuel kind of the current equipment record — `ifuel` from
    /// `nonrdeqp.inc`.
    pub fuel: FuelKind,
    /// `true` when the run is in "total" mode (`ismtyp == IDXTOT`);
    /// `false` when in typical-day mode.
    pub total_mode: bool,
    /// `true` when day-of-year output is requested (`ldayfl`).
    pub daily_output: bool,
    /// `true` when by-model-year exhaust output is requested
    /// (`lbmyfl`).
    pub emit_bmy: bool,
    /// `true` when by-model-year evap output is requested
    /// (`levbmyfl`).
    pub emit_bmy_evap: bool,
    /// `true` when the SI report is requested (`lsifl`).
    pub emit_si: bool,
    /// `true` when the growth-file packet was loaded (`lgrwfl`).
    pub growth_loaded: bool,
    /// `true` when retrofit records were loaded (`lrtrftfl`).
    pub retrofit_loaded: bool,
    /// `true` when the spillage / refueling-mode packet was loaded
    /// (`lfacfl(IDXSPL)`). Drives evap setup.
    pub spillage_loaded: bool,
}

/// One state in the run, mirroring the slot of the Fortran
/// `statcd`/`lstacd`/`lstlev` parallel COMMON arrays.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateDescriptor {
    /// 5-character state FIPS code (`statcd(i)`).
    pub fips: String,
    /// `true` iff the state is requested for the current run
    /// (`lstacd(i)`).
    pub selected: bool,
    /// `true` iff the state has its own state-level population
    /// records and should not be allocated from the national total
    /// (`lstlev(i)`).
    pub has_state_records: bool,
}

/// Per-equipment / per-record inputs. Replaces what `prcnat.f` /
/// `prcus.f` read from `nonrdeqp.inc` arrays indexed by `icurec`.
#[derive(Debug, Clone, Copy)]
pub struct EquipmentRecord {
    /// HP-range minimum (`hprang(1, icurec)`).
    pub hp_range_min: f32,
    /// HP-range maximum (`hprang(2, icurec)`).
    pub hp_range_max: f32,
    /// Average HP for this HP category (`avghpc(icurec)`).
    pub hp_avg: f32,
    /// Equipment population for the record (`popeqp(icurec)`).
    pub population: f32,
    /// Population-input year (`ipopyr(icurec)`).
    pub pop_year: i32,
    /// Average use-hours from population input (`usehrs(icurec)`).
    pub use_hours: f32,
    /// Discharge / scrappage code (`discod(icurec)`).
    pub discharge_code: i32,
    /// Starts hours from population input (`starts(idxact)` is
    /// independent, but the population record carries it
    /// alongside).
    pub starts_hours: f32,
}

/// Activity-lookup outcome. Replaces the `idxact` index plus the
/// activity-record fields the Fortran source reads downstream
/// (`iactun(idxact)`, `faclod(idxact)`, `actage(idxact)`,
/// `actlev(idxact)`, `starts(idxact)`).
#[derive(Debug, Clone)]
pub struct ActivityLookup {
    /// `faclod(idxact)`.
    pub load_factor: f32,
    /// `iactun(idxact)`.
    pub units: ActivityUnit,
    /// `actlev(idxact)`.
    pub activity_level: f32,
    /// `starts(idxact)` — supplied via the activity-file's parallel
    /// stream (the Fortran source loads it from `actrcd` extension
    /// arrays).
    pub starts_value: f32,
    /// `actage(idxact)` — the alternate-curve identifier.
    pub age_curve_id: String,
}

/// Per-call output records. Replaces the Fortran routines'
/// `wrtdat`/`wrtbmy`/`sitot` side effects with a structured
/// collection the driver (Task 113) can hand to the writer (Task 114).
#[derive(Debug, Default, Clone)]
pub struct GeographyOutput {
    /// One [`StateOutput`] per state / FIPS the routine produced.
    /// `prcnat` produces one per state in `NSTATE`; `prcus`
    /// produces one with `fips == "00000"`.
    pub state_outputs: Vec<StateOutput>,
    /// One [`ByModelYearOutput`] entry per `(state, model_year,
    /// tech_type, channel)` tuple the run requested. Empty when
    /// `emit_bmy`/`emit_bmy_evap` are both false.
    pub bmy_outputs: Vec<ByModelYearOutput>,
    /// One [`SiAggregate`] entry per `(state, tech_type, channel)`
    /// tuple. Empty when `emit_si` is false.
    pub si_aggregates: Vec<SiAggregate>,
    /// Non-fatal warnings emitted during the call. Mirrors the
    /// `chkwrn` warning channel.
    pub warnings: Vec<GeographyWarning>,
    /// Number of national-level records the routine processed
    /// (Fortran `nnatrc`, incremented once per `prcnat` call where
    /// the state index is national). Returned for parity with the
    /// Fortran counter; the driver typically aggregates these.
    pub national_record_count: i32,
    /// Per-state national-allocation record counts (Fortran
    /// `nstarc(idx)`). Same length as `state_outputs`; ordered to
    /// match.
    pub state_record_counts: Vec<i32>,
}

/// Structured equivalent of one `wrtdat` call (`output/data` writer).
///
/// All quantities are in tons (matching the Fortran post-`CVTTON`
/// scaling).
#[derive(Debug, Clone, PartialEq)]
pub struct StateOutput {
    /// 5-character FIPS code (state for `prcnat`; `"00000"` for
    /// `prcus`).
    pub fips: String,
    /// 5-character subcounty code (blank `"     "` at the
    /// state / national level).
    pub subcounty: String,
    /// 10-character SCC code.
    pub scc: String,
    /// HP-level representative (`hplev` in the Fortran source).
    pub hp_level: f32,
    /// Total population over all model years
    /// (`poptot * modfrc(...)` sum).
    pub population: f32,
    /// Total activity (`acttot`).
    pub activity: f32,
    /// Total fuel consumption (`fulcsm`).
    pub fuel_consumption: f32,
    /// Load factor (`faclod(idxact)`).
    pub load_factor: f32,
    /// HP average (`hpval`).
    pub hp_avg: f32,
    /// Fraction of fleet that's retrofitted (`fracretro`).
    pub frac_retrofitted: f32,
    /// Units retrofitted (`unitsretro`).
    pub units_retrofitted: f32,
    /// Per-pollutant daily totals (`emsday`). Length [`MXPOL`].
    /// Set to all `RMISS` when no activity record was found for the
    /// state — preserves the Fortran missing-data semantics.
    pub emissions_day: Vec<f32>,
    /// `true` when the routine had to emit a "missing data"
    /// (all-`RMISS`) record for this state.
    pub missing: bool,
}

/// Structured equivalent of one `wrtbmy` call (`output/bmy` writer).
#[derive(Debug, Clone, PartialEq)]
pub struct ByModelYearOutput {
    /// State or national FIPS code.
    pub fips: String,
    /// Subcounty code (`"     "` at state / national level).
    pub subcounty: String,
    /// SCC.
    pub scc: String,
    /// HP level representative.
    pub hp_level: f32,
    /// Technology type identifier.
    pub tech_type: String,
    /// Model year.
    pub model_year: i32,
    /// Population for this `(model_year, tech)` bucket.
    pub population: f32,
    /// Per-pollutant emissions for this bucket. Length [`MXPOL`].
    pub emissions: Vec<f32>,
    /// Fuel consumption for this bucket.
    pub fuel_consumption: f32,
    /// Activity for this bucket.
    pub activity: f32,
    /// Load factor (`faclod(idxact)`) or [`crate::common::consts::RMISS`]
    /// for evap bucket — the Fortran source passes `RMISS` to the
    /// evap branch of `wrtbmy`.
    pub load_factor: f32,
    /// HP average or `RMISS` for evap bucket (same convention).
    pub hp_avg: f32,
    /// Fraction retrofitted for this bucket, or `RMISS` for evap.
    pub frac_retrofitted: f32,
    /// Units retrofitted for this bucket, or `RMISS` for evap.
    pub units_retrofitted: f32,
    /// 1 for exhaust, 2 for evap. Matches the Fortran `iexev`
    /// argument of `wrtbmy`.
    pub channel: u8,
}

/// Aggregated per-`(state, tech_type, channel)` total for the SI
/// report. Mirrors one `sitot` call.
#[derive(Debug, Clone, PartialEq)]
pub struct SiAggregate {
    /// FIPS for the geographic bucket.
    pub fips: String,
    /// SCC for the bucket.
    pub scc: String,
    /// Technology type.
    pub tech_type: String,
    /// Bucket's accumulated population.
    pub population: f32,
    /// Bucket's accumulated activity.
    pub activity: f32,
    /// Bucket's accumulated fuel consumption.
    pub fuel_consumption: f32,
    /// Bucket's accumulated emissions (one slot per pollutant).
    pub emissions: Vec<f32>,
    /// 1 for exhaust, 2 for evap.
    pub channel: u8,
}

/// Non-fatal warning emitted by the geography routines.
///
/// Each variant maps to a specific `chkwrn(*, IDXW*)` site in the
/// Fortran source. The variant payload carries the context needed to
/// reproduce the corresponding Fortran log entry; formatting itself
/// is up to the caller (the writers in `output/` will format them in
/// Task 114).
#[derive(Debug, Clone, PartialEq)]
pub enum GeographyWarning {
    /// No exhaust technology fractions for `(SCC, HP, year)`.
    /// `chkwrn(*, IDXWTC)`.
    MissingExhaustTech { scc: String, hp_avg: f32, year: i32 },
    /// No evap technology fractions for `(SCC, HP, year)`.
    /// `chkwrn(*, IDXWTC)`.
    MissingEvapTech { scc: String, hp_avg: f32, year: i32 },
    /// No activity records for `(SCC, FIPS, HP range)`.
    /// `chkwrn(*, IDXWAC)`.
    MissingActivity {
        scc: String,
        fips: String,
        hp_min: f32,
        hp_max: f32,
    },
    /// No spillage data for `(SCC, HP, tech_type, evap_tech_type)`.
    /// `chkwrn(*, IDXWEM)`.
    MissingSpillage {
        scc: String,
        hp_avg: f32,
        evap_tech_type: String,
        ref_name: String,
    },
}

/// Errors specific to this module. Wrapped into [`crate::Error`]
/// at the function boundary; defined as their own type here so unit
/// tests can match on them without parsing message strings.
#[derive(Debug, Clone, PartialEq)]
pub enum GeographyError {
    /// `prcnat.f` :7000 — couldn't find any allocation coefficients
    /// for the SCC code while doing a national-to-state allocation.
    AllocationNotFound { scc: String },
    /// `prcus.f` / `prcnat.f` :7001 — couldn't find a growth indicator
    /// cross-reference match for `(FIPS, SCC, HP)`.
    GrowthIndicatorNotFound {
        fips: String,
        scc: String,
        hp_avg: f32,
        hp_min: f32,
        hp_max: f32,
    },
    /// `prcus.f` / `prcnat.f` :7003 — growth-file packet missing
    /// from the options file (gated by `lgrwfl`).
    GrowthFileMissing,
}

impl std::fmt::Display for GeographyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GeographyError::AllocationNotFound { scc } => {
                write!(
                    f,
                    "could not find any allocation coefficients for SCC code {scc}",
                )
            }
            GeographyError::GrowthIndicatorNotFound {
                fips,
                scc,
                hp_avg,
                hp_min,
                hp_max,
            } => {
                write!(
                    f,
                    "could not find growth-indicator cross-reference match for FIPS={fips} \
                     SCC={scc} HP_avg={hp_avg} HP_range=({hp_min}, {hp_max})",
                )
            }
            GeographyError::GrowthFileMissing => {
                write!(
                    f,
                    "/GROWTH FILES/ packet missing from options file — required for \
                     future-year projection or backcasting",
                )
            }
        }
    }
}

impl std::error::Error for GeographyError {}

/// Map the equipment record's HP range midpoint to one of the
/// `hp_levels` slots.
///
/// Mirrors the `hpclev` lookup at `prcnat.f` :252–:265 and
/// `prcus.f` :205–:216.
///
/// - Mid HP ≤ `hp_levels[0]` → `hp_levels[0]`.
/// - Mid HP > `hp_levels[MXHPC-1]` → `9999.0` sentinel.
/// - Otherwise: the first `hp_levels[i]` (for `i >= 1`) that exceeds the mid.
///
/// Returns `9999.0` when `hp_levels` is empty (defensive — production
/// inputs always supply MXHPC entries; this matches the Fortran
/// "mid above the top level" branch).
pub fn hp_level_for_midpoint(midpoint: f32, hp_levels: &[f32]) -> f32 {
    let Some(first) = hp_levels.first().copied() else {
        return 9999.0;
    };
    if midpoint <= first {
        return first;
    }
    let last = *hp_levels.last().expect("non-empty checked above");
    if midpoint > last {
        return 9999.0;
    }
    let mut found: f32 = -9.0;
    for &lvl in &hp_levels[1..] {
        if found < 0.0 && midpoint < lvl {
            found = lvl;
        }
    }
    found
}

/// Pick the fuel density used by `prcnat.f` :350–:358 and
/// `prcus.f` :274–:283.
///
/// `1.0` is the default. Specific gas/CNG/LPG/diesel constants are
/// `DENGAS`, `DENCNG`, `DENLPG`, `DENDSL` from
/// [`crate::common::consts`].
pub fn fuel_density(fuel: FuelKind) -> f32 {
    use crate::common::consts::{DENCNG, DENDSL, DENGAS, DENLPG};
    match fuel {
        FuelKind::Gasoline2Stroke | FuelKind::Gasoline4Stroke => DENGAS as f32,
        FuelKind::Cng => DENCNG as f32,
        FuelKind::Lpg => DENLPG as f32,
        FuelKind::Diesel => DENDSL as f32,
    }
}

/// Activity-unit branching for the temporal-adjustment factor used
/// in the inner `(model_year, tech)` loop.
///
/// `prcnat.f` :700–:706 / `prcus.f` :492–:498:
///
/// ```text
/// if iactun in {IDXHRY, IDXGLY}: tpltmp = tplfac
/// elif iactun in {IDXHRD, IDXGLD}: tpltmp = 1.0
/// ```
pub fn temporal_adjustment_for_unit(unit: ActivityUnit, tplfac: f32) -> f32 {
    match unit {
        ActivityUnit::HoursPerYear | ActivityUnit::GallonsPerYear => tplfac,
        ActivityUnit::HoursPerDay | ActivityUnit::GallonsPerDay => 1.0,
    }
}

/// Helper: produce a zero-emissions slot the size of the pollutant
/// vector. The Fortran `zeroems(MXPOL)` array.
pub fn zero_emissions() -> Vec<f32> {
    vec![0.0; MXPOL]
}

/// Helper: produce an all-`RMISS` slot the size of the pollutant
/// vector. The Fortran `missems(MXPOL)` array.
pub fn missing_emissions() -> Vec<f32> {
    use crate::common::consts::RMISS;
    vec![RMISS; MXPOL]
}

/// Helper: convert the Fortran `subcur` "blank subcounty" sentinel
/// into a 5-space [`String`]. Both `prcnat.f` :246 and `prcus.f` :200
/// initialise `subcur = ' '` (Fortran's blank-padded character
/// variables; a single blank in source padded out to length 5). The
/// Rust port uses `"     "` consistently — five spaces — so callers
/// matching against the output can do byte-for-byte equality with
/// the Fortran writer.
pub fn blank_subcounty() -> String {
    "     ".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hp_level_clamps_low() {
        let levels = [11.0, 25.0, 50.0, 100.0];
        assert_eq!(hp_level_for_midpoint(5.0, &levels), 11.0);
        assert_eq!(hp_level_for_midpoint(11.0, &levels), 11.0);
    }

    #[test]
    fn hp_level_clamps_high() {
        let levels = [11.0, 25.0, 50.0, 100.0];
        assert_eq!(hp_level_for_midpoint(500.0, &levels), 9999.0);
    }

    #[test]
    fn hp_level_picks_first_exceeding() {
        let levels = [11.0, 25.0, 50.0, 100.0];
        // midpoint 30 is > 11 (first), so we go to the "found" loop.
        // First `lvl > 30` from `levels[1..]` is 50.
        assert_eq!(hp_level_for_midpoint(30.0, &levels), 50.0);
        // midpoint 24 — first lvl > 24 is 25.
        assert_eq!(hp_level_for_midpoint(24.0, &levels), 25.0);
    }

    #[test]
    fn hp_level_empty_returns_9999() {
        assert_eq!(hp_level_for_midpoint(50.0, &[]), 9999.0);
    }

    #[test]
    fn fuel_density_matches_fortran() {
        use crate::common::consts::{DENCNG, DENDSL, DENGAS, DENLPG};
        assert_eq!(fuel_density(FuelKind::Gasoline2Stroke), DENGAS as f32);
        assert_eq!(fuel_density(FuelKind::Gasoline4Stroke), DENGAS as f32);
        assert_eq!(fuel_density(FuelKind::Cng), DENCNG as f32);
        assert_eq!(fuel_density(FuelKind::Lpg), DENLPG as f32);
        assert_eq!(fuel_density(FuelKind::Diesel), DENDSL as f32);
    }

    #[test]
    fn temporal_adjustment_branches() {
        assert_eq!(
            temporal_adjustment_for_unit(ActivityUnit::HoursPerYear, 0.5),
            0.5
        );
        assert_eq!(
            temporal_adjustment_for_unit(ActivityUnit::HoursPerDay, 0.5),
            1.0
        );
        assert_eq!(
            temporal_adjustment_for_unit(ActivityUnit::GallonsPerYear, 0.7),
            0.7
        );
        assert_eq!(
            temporal_adjustment_for_unit(ActivityUnit::GallonsPerDay, 0.7),
            1.0
        );
    }

    #[test]
    fn zero_emissions_has_length_mxpol() {
        assert_eq!(zero_emissions().len(), MXPOL);
        assert!(zero_emissions().iter().all(|&v| v == 0.0));
    }

    #[test]
    fn missing_emissions_uses_rmiss() {
        use crate::common::consts::RMISS;
        let m = missing_emissions();
        assert_eq!(m.len(), MXPOL);
        assert!(m.iter().all(|&v| v == RMISS));
    }

    #[test]
    fn blank_subcounty_is_five_spaces() {
        assert_eq!(blank_subcounty(), "     ");
        assert_eq!(blank_subcounty().len(), 5);
    }
}

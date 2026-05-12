//! Shared types and helpers for the [Task 109] geography-processing
//! routines.
//!
//! Both [`process_county`] (`prccty.f`) and [`process_subcounty`]
//! (`prcsub.f`) consume the same record shape, the same run options,
//! and the same callback trait. They produce the same kind of
//! output. The pieces live here so [`county`] and [`subcounty`] only
//! carry the routine-specific orchestration.
//!
//! [Task 109]: super
//! [`process_county`]: super::process_county
//! [`process_subcounty`]: super::process_subcounty
//! [`county`]: super::county
//! [`subcounty`]: super::subcounty

use crate::common::consts::{DENCNG, DENDSL, DENGAS, DENLPG, MXDAYS, MXHPC};
use crate::emissions::exhaust::{AdjustmentTable, EmissionUnitCode, FuelKind};
use crate::population::retrofit::RetrofitRecord;
use crate::{Error, Result};

// =============================================================================
// Inputs: population record + run options
// =============================================================================

/// One record from the population array (`/popdat/`) being processed.
///
/// Mirrors the per-`icurec` reads `prccty.f` and `prcsub.f` perform
/// against the population COMMON arrays (`regncd`, `popeqp`,
/// `hprang`, `avghpc`, `usehrs`, `discod`, `ipopyr`). The string
/// fields use `&str` for zero-copy access; lifetimes flow through
/// to the borrowed fields in [`ProcessOutput`].
#[derive(Debug, Clone, PartialEq)]
pub struct PopulationRecord<'a> {
    /// 5-character region code (Fortran `regncd(icurec)(1:5)`).
    /// For prccty this is the county FIPS directly; for prcsub it
    /// is the 5-character county FIPS prefix of the 10-character
    /// region code (the trailing 5 hold the subcounty marker).
    pub region_code: &'a str,
    /// Equipment population (Fortran `popeqp(icurec)`). For prcsub
    /// this is the state-level population that `alosub` will further
    /// subdivide.
    pub population: f32,
    /// HP range — `(lower, upper)` (Fortran
    /// `hprang(1,icurec)`, `hprang(2,icurec)`).
    pub hp_range: (f32, f32),
    /// Average HP for this record (Fortran `avghpc(icurec)`).
    pub hp_avg: f32,
    /// Use hours per year (Fortran `usehrs(icurec)`).
    pub use_hours: f32,
    /// Discode/alternate-curve picker (Fortran `discod(icurec)`).
    pub disc_code: &'a str,
    /// Base population year (Fortran `ipopyr(icurec)`).
    pub base_pop_year: i32,
    /// 10-character SCC code (Fortran `asccod`).
    pub scc: &'a str,
}

/// Run-level options. Mirrors the COMMON-block reads `prccty.f` and
/// `prcsub.f` perform against `/optdat/`, `/eqpdat/`, `/perdat/` and
/// `/io/`.
#[derive(Debug, Clone)]
pub struct RunOptions {
    /// Tech year for fndtch/fndevtch lookups (Fortran `itchyr`).
    pub tech_year: i32,
    /// Episode year (Fortran `iepyr`) — top of the model-year loop.
    pub episode_year: i32,
    /// Growth year (Fortran `igryr`) — passed to `agedist`.
    pub growth_year: i32,
    /// Fuel kind (Fortran `ifuel`) — picks the density constant.
    pub fuel: FuelKind,
    /// Total-vs-typical-day mode (Fortran `ismtyp`).
    pub sum_type: SumType,
    /// Daily-mode flag (Fortran `ldayfl`) — drops the monthly
    /// factor from `tplfac`.
    pub daily_mode: bool,
    /// Exhaust by-model-year output flag (Fortran `lbmyfl`).
    pub write_bmy_exhaust: bool,
    /// Evaporative by-model-year output flag (Fortran `levbmyfl`).
    pub write_bmy_evap: bool,
    /// SI-report output flag (Fortran `lsifl`).
    pub write_si: bool,
    /// Retrofit-file present (Fortran `lrtrftfl`).
    pub retrofit_enabled: bool,
    /// Spillage-EF file present (Fortran `lfacfl(IDXSPL)`).
    pub spillage_enabled: bool,
    /// Growth-file present (Fortran `lgrwfl`). When `false`, the
    /// Fortran source's `goto 7003` error path triggers — surfaced
    /// here as [`Error::Config`].
    pub growth_enabled: bool,
    /// HP-category midpoints (Fortran `hpclev(1..MXHPC)`).
    pub hp_levels: [f32; MXHPC],
}

/// Sum / typical-day indicator from `prccty.f` :301 (`ismtyp`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SumType {
    /// Period totals (Fortran `IDXTOT` — `ismtyp == IDXTOT`).
    Total,
    /// Per-typical-day rates — anything other than `IDXTOT`.
    Typical,
}

/// Activity-units indicator (Fortran `iactun(idxact)`).
///
/// Used by [`temporal_adjustment`] to decide whether `tpltmp` uses
/// the per-period `tplfac` (yearly activity) or `1.0` (daily activity).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityUnit {
    /// Hours per year (Fortran `IDXHRY = 1`).
    HoursPerYear,
    /// Hours per day (Fortran `IDXHRD = 2`).
    HoursPerDay,
    /// Gallons per year (Fortran `IDXGLY = 3`).
    GallonsPerYear,
    /// Gallons per day (Fortran `IDXGLD = 4`).
    GallonsPerDay,
}

// =============================================================================
// Outputs: warnings, output records, processed-record summary
// =============================================================================

/// Non-fatal warning emitted during geography processing.
///
/// The Fortran routines call `chkwrn(jerr, IDX*)` to increment a
/// per-warning counter and possibly fail the run when a limit is
/// exceeded. The Rust port returns the warnings as data so callers
/// decide whether to log, count, or abort.
#[derive(Debug, Clone, PartialEq)]
pub struct ProcessWarning {
    /// Kind of warning emitted.
    pub kind: WarningKind,
    /// Human-readable description, preserved verbatim from the
    /// Fortran `write(IOWMSG, …)` calls so log diffs against the
    /// reference can be exact.
    pub message: String,
}

/// Discriminant for [`ProcessWarning::kind`].
///
/// Variants share a `Missing*` prefix because every Fortran
/// `chkwrn` site in `prccty.f`/`prcsub.f` raises a "missing data"
/// condition; the prefix encodes that shared semantic up-front.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)]
pub enum WarningKind {
    /// Could not find tech-type fractions (`IDXWTC`).
    MissingTechType,
    /// Could not find activity data (`IDXWAC`).
    MissingActivity,
    /// Could not find emission-factor / spillage data (`IDXWEM`).
    MissingEmissionFactor,
    /// Could not find allocation coefficients (used by prcsub).
    MissingAllocation,
}

/// A `wrtdat`-bound output record (one per SCC × HP × geography cell).
///
/// Mirrors the argument list of `wrtdat(jerr, fipin, subcur,
/// asccod, hplev, poptot, acttot, fulcsm, faclod(idxact), hpval,
/// fracretro, unitsretro, emsday)` and the abbreviated zero-record
/// `wrtdat(…, 0., 0., 0., 0., 0., 0., 0., emsday)` paths.
#[derive(Debug, Clone, PartialEq)]
pub struct DatRecord {
    /// 5-character county FIPS (Fortran `fipin`).
    pub fips: String,
    /// 5-character subcounty marker — empty for county records,
    /// the subcounty ID for subcounty records (Fortran `subcur`).
    pub subcounty: String,
    /// 10-character SCC (Fortran `asccod`).
    pub scc: String,
    /// HP level (Fortran `hplev`).
    pub hp_level: f32,
    /// Total population for the period (Fortran `poptot`).
    pub population_total: f32,
    /// Total activity for the period (Fortran `acttot`).
    pub activity_total: f32,
    /// Total fuel consumption (Fortran `fulcsm`).
    pub fuel_consumption: f32,
    /// Load factor (Fortran `faclod(idxact)`).
    pub load_factor: f32,
    /// HP-average (Fortran `hpval`).
    pub hp_avg: f32,
    /// Fraction retrofitted (Fortran `fracretro`).
    pub frac_retrofitted: f32,
    /// Units retrofitted (Fortran `unitsretro`).
    pub units_retrofitted: f32,
    /// Per-pollutant emissions (Fortran `emsday(MXPOL)`).
    pub emissions: Vec<f32>,
}

/// Indicator on whether a [`BmyRecord`] is an exhaust (`1`) or evap (`2`) row.
///
/// The Fortran `wrtbmy` takes a trailing integer flag for this; the
/// Rust port uses an enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BmyKind {
    /// Exhaust by-model-year row (Fortran flag `1`).
    Exhaust,
    /// Evaporative by-model-year row (Fortran flag `2`).
    Evaporative,
}

/// A `wrtbmy`-bound record (one per model year × tech-type cell).
///
/// Mirrors the argument list of `wrtbmy(jerr, fipin, subcur,
/// asccod, hplev, tecnam, iyr, popbmy, emsbmy, fulbmy, actbmy,
/// faclod, hpval, fracretrobmy, unitsretrobmy, kind)`.
#[derive(Debug, Clone, PartialEq)]
pub struct BmyRecord {
    /// 5-character county FIPS (Fortran `fipin`).
    pub fips: String,
    /// 5-character subcounty marker (Fortran `subcur`).
    pub subcounty: String,
    /// 10-character SCC (Fortran `asccod`).
    pub scc: String,
    /// HP level (Fortran `hplev`).
    pub hp_level: f32,
    /// Tech type for this row (Fortran `tecnam(i)` or `evtecnam(i)`).
    pub tech_name: String,
    /// Absolute model year (Fortran `iyr`).
    pub model_year: i32,
    /// Population at this (year, tech) (Fortran `popbmy`).
    pub population: f32,
    /// Per-pollutant emissions at this (year, tech) (Fortran
    /// `emsbmy(MXPOL)`).
    pub emissions: Vec<f32>,
    /// Fuel consumption at this (year, tech) (Fortran `fulbmy`).
    pub fuel: f32,
    /// Activity at this (year, tech) (Fortran `actbmy`).
    pub activity: f32,
    /// Load factor (Fortran `faclod(idxact)`) or
    /// [`crate::common::consts::RMISS`] for the evap path.
    pub load_factor: f32,
    /// HP-average (Fortran `hpval`) or
    /// [`crate::common::consts::RMISS`] for the evap path.
    pub hp_avg: f32,
    /// Fraction retrofitted for this (year, tech) (Fortran
    /// `fracretrobmy`) or [`crate::common::consts::RMISS`] for the
    /// evap path.
    pub frac_retrofitted: f32,
    /// Units retrofitted for this (year, tech) (Fortran
    /// `unitsretrobmy`) or [`crate::common::consts::RMISS`] for the
    /// evap path.
    pub units_retrofitted: f32,
    /// Exhaust vs evap (Fortran flag `1` or `2`).
    pub kind: BmyKind,
}

/// An `sitot`-bound record (Fortran `sitot(jerr, tecnam, popbmy,
/// actbmy, fulbmy, emsbmy)` — populates the SI-report buffers).
#[derive(Debug, Clone, PartialEq)]
pub struct SiRecord {
    /// Tech type for this row (Fortran `tecnam(i)` or `evtecnam(i)`).
    pub tech_name: String,
    /// Population at this row (Fortran `popbmy`).
    pub population: f32,
    /// Activity at this row (Fortran `actbmy`).
    pub activity: f32,
    /// Fuel consumption at this row (Fortran `fulbmy`).
    pub fuel: f32,
    /// Per-pollutant emissions at this row.
    pub emissions: Vec<f32>,
}

/// Outcome of [`super::process_county`] / [`super::process_subcounty`].
///
/// `prccty.f` and `prcsub.f` return one of three flags via the
/// `ierr` argument:
///
/// - `ISUCES` (0): processed normally — see [`Self::Success`].
/// - `ISKIP`  (5): processed but skipped (e.g. county not in run);
///   the routine may still have written a zero record — see
///   [`Self::Skipped`].
/// - `IFAIL`  (1): fatal error, returned via `Err(Error::…)` rather
///   than as a variant.
#[derive(Debug, Clone, PartialEq)]
pub enum ProcessOutcome {
    /// Normal completion (Fortran `ierr == ISUCES`).
    Success(ProcessOutput),
    /// Record skipped (Fortran `ierr == ISKIP`).
    ///
    /// Carries the output anyway because the Fortran source may
    /// have written a zero record (e.g. `popcty == 0` path).
    Skipped(ProcessOutput),
}

impl ProcessOutcome {
    /// Reference to the inner [`ProcessOutput`] regardless of variant.
    pub fn output(&self) -> &ProcessOutput {
        match self {
            ProcessOutcome::Success(o) | ProcessOutcome::Skipped(o) => o,
        }
    }

    /// True when this outcome is the `ISKIP` variant.
    pub fn is_skipped(&self) -> bool {
        matches!(self, ProcessOutcome::Skipped(_))
    }

    /// Destructure into the inner [`ProcessOutput`], discarding the
    /// skip flag.
    pub fn into_output(self) -> ProcessOutput {
        match self {
            ProcessOutcome::Success(o) | ProcessOutcome::Skipped(o) => o,
        }
    }
}

/// Successful (or zero-record-emitting) output of geography
/// processing.
///
/// Holds the typed records the Fortran source would have written
/// via `wrtdat`/`wrtbmy`/`sitot`, plus the per-county emissions
/// total that the caller folds into `emsams(NCNTY, MXPOL)`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ProcessOutput {
    /// 5-character county FIPS (`fipin`).
    pub fips: String,
    /// 5-character subcounty marker (`subcur`) — empty for
    /// county-level outputs.
    pub subcounty: String,
    /// HP level for the processed record (`hplev`).
    pub hp_level: f32,
    /// Per-pollutant emissions for the period (`emsday(MXPOL)`).
    pub emissions_day: Vec<f32>,
    /// Cumulative county-level emissions to fold into `emsams` —
    /// only populated when at least one pollutant emitted (Fortran
    /// `if (emsday(i) > 0)` gate at `prccty.f` :721).
    pub emsams_delta: Vec<f32>,
    /// FIPS slot the caller should fold `emsams_delta` into. The
    /// Fortran source indexes `emsams(idxfip, …)`; the Rust port
    /// returns the slot so callers don't have to re-resolve it.
    /// `None` when no fold-in is needed (skipped paths).
    pub emsams_fips_index: Option<usize>,
    /// `dat`-record(s) the Fortran source would emit via `wrtdat`.
    /// Usually one; the early-return paths emit a zero record.
    pub dat_records: Vec<DatRecord>,
    /// `bmy`-records emitted via `wrtbmy`.
    pub bmy_records: Vec<BmyRecord>,
    /// `si`-records emitted via `sitot`.
    pub si_records: Vec<SiRecord>,
    /// Warnings collected during processing.
    pub warnings: Vec<ProcessWarning>,
}

// =============================================================================
// Callbacks: dependencies on yet-to-be-ported subroutines
// =============================================================================

/// Lookup result for `fndtch`-style queries.
///
/// `prccty.f` :251 / :440 calls `fndtch(asccod, hpval, year)` which
/// returns a single integer slot into the tech-fraction COMMON
/// table. The Rust port carries the matched tech-type list with the
/// slot, so callers don't have to expose the COMMON-block layout.
#[derive(Debug, Clone, PartialEq)]
pub struct TechLookup {
    /// Index into the SCC-level tech table (`idxtch`, 0-based).
    pub scc_tech_index: usize,
    /// Per-tech-slot names in this SCC's row (`tectyp(idxtch, 1..n)`).
    pub tech_names: Vec<String>,
    /// Per-tech-slot fractions in this SCC's row (`tchfrc(idxtch, 1..n)`).
    pub tech_fractions: Vec<f32>,
}

/// Result of a `fndrfm` lookup — refueling/spillage data for one
/// `(SCC, HP, tech)` slot. `prccty.f` :582–:624 reads these out of
/// the spillage COMMON when `lfacfl(IDXSPL)` is set.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RefuelingData {
    /// Refueling-mode string (`modspl(idxref)`).
    pub mode: String,
    /// Tank volume in gallons (`volspl`, then capped/scaled by `untspl`).
    pub tank: f32,
    /// Tank-full flag (`tnkful`).
    pub tank_full: f32,
    /// Tank-metal flag (`tnkmtl`).
    pub tank_metal: f32,
    /// Hose length (`hoslen`).
    pub hose_length: f32,
    /// Hose diameter (`hosdia`).
    pub hose_dia: f32,
    /// Hose-metal flag (`hosmtl`).
    pub hose_metal: f32,
    /// Hot-soak start probability (`hssph`).
    pub hot_soak_start: f32,
    /// Neck length (`ncklen`).
    pub neck_length: f32,
    /// Neck diameter (`nckdia`).
    pub neck_dia: f32,
    /// Supply length (`srlen`).
    pub supply_length: f32,
    /// Supply diameter (`srdia`).
    pub supply_dia: f32,
    /// Vent length (`vntlen`).
    pub vent_length: f32,
    /// Vent diameter (`vntdia`).
    pub vent_dia: f32,
    /// Per-day-of-week diurnal fractions (`diufrc(1..5, idxref)`).
    pub diurnal_fractions: [f32; 5],
    /// E10 permeation factor for tanks (`tnke10`).
    pub tnk_e10_factor: f32,
    /// E10 permeation factor for hoses (`hose10`).
    pub hose_e10_factor: f32,
    /// E10 permeation factor for fill necks (`ncke10`).
    pub neck_e10_factor: f32,
    /// E10 permeation factor for supply/return (`sre10`).
    pub supply_e10_factor: f32,
    /// E10 permeation factor for vents (`vnte10`).
    pub vent_e10_factor: f32,
}

/// Exhaust emission-factor table produced by [`GeographyCallbacks::compute_exhaust_factors`].
///
/// Mirrors the outputs `emfclc.f` writes into its `emsfac`, `bsfc`,
/// `idxunt`, `adetcf`, `bdetcf`, `detcap` arguments. The Rust port
/// returns them as one struct so the caller only carries one
/// dependency.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ExhaustFactorsLookup {
    /// Emission factors `[year][pollutant][tech]`, row-major.
    pub emission_factors: Vec<f32>,
    /// BSFC `[year][tech]`, row-major.
    pub bsfc: Vec<f32>,
    /// Unit codes `[pollutant][tech]`, row-major.
    pub unit_codes: Vec<EmissionUnitCode>,
    /// Deterioration A coefficient `[pollutant][tech]`.
    pub adetcf: Vec<f32>,
    /// Deterioration B coefficient `[pollutant][tech]`.
    pub bdetcf: Vec<f32>,
    /// Deterioration cap `[pollutant][tech]`.
    pub detcap: Vec<f32>,
}

/// Evap emission-factor table produced by [`GeographyCallbacks::compute_evap_factors`].
///
/// Mirrors the outputs `evemfclc.f` writes into its `evemsfac`,
/// `idxevunt`, `aevdetcf`, `bevdetcf`, `evdetcap` arguments.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct EvapFactorsLookup {
    /// Evap emission factors `[year][pollutant][evtech]`, row-major.
    pub emission_factors: Vec<f32>,
    /// Unit codes `[pollutant][evtech]`, row-major.
    pub unit_codes: Vec<EmissionUnitCode>,
    /// Evap deterioration A coefficient `[pollutant][evtech]`.
    pub adetcf: Vec<f32>,
    /// Evap deterioration B coefficient `[pollutant][evtech]`.
    pub bdetcf: Vec<f32>,
    /// Evap deterioration cap `[pollutant][evtech]`.
    pub detcap: Vec<f32>,
}

/// Retrofit-filter mode (Fortran `fltrtyp` argument of `fndrtrft`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetrofitFilter {
    /// `fltrtyp=1`: keep records whose `(SCC, HP)` matches.
    SccHp,
    /// `fltrtyp=2`: keep records whose model-year span matches.
    ModelYear,
    /// `fltrtyp=3`: keep records whose tech type matches.
    TechType,
}

/// Combined output of the `modyr` + `agedist` sequence — what the
/// Fortran source produces in the block of calls at `prccty.f`
/// :373–:385 / `prcsub.f` :404–:416.
///
/// Both Fortran calls feed into the model-year loop that begins at
/// :412 / :443; the Rust port collects everything into one struct
/// so the orchestration doesn't have to plumb the closure-based
/// callbacks of [`crate::population::modyr::model_year`] and
/// [`crate::population::agedist::age_distribution`].
#[derive(Debug, Clone, PartialEq)]
pub struct ModelYearAgedistResult {
    /// Year-to-year fraction scrapped by age — `yryrfrcscrp`.
    pub yryrfrcscrp: Vec<f32>,
    /// Model-year fractions, grown to `growth_year` (`modfrc`).
    pub modfrc: Vec<f32>,
    /// Starts adjustment per year (`stradj`).
    pub stradj: Vec<f32>,
    /// Activity adjustment per year (`actadj`).
    pub actadj: Vec<f32>,
    /// Deterioration age per year (`detage`).
    pub detage: Vec<f32>,
    /// Lifetime in years (`nyrlif`).
    pub nyrlif: usize,
    /// Population after agedist (may be backward-grown).
    pub population: f32,
}

/// One iteration's per-pollutant emissions, returned by
/// [`GeographyCallbacks::compute_exhaust_iteration`] and
/// [`GeographyCallbacks::compute_evap_iteration`].
///
/// Mirrors the `emsday` and `emsbmy` accumulators that `clcems.f` /
/// `clcevems.f` mutate. The Rust port returns the deltas as
/// independent vectors so the orchestrator can fold them into its
/// running totals (`emsday` is record-level; `emsbmy` is per (year,
/// tech)).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct EmissionsIterationResult {
    /// Per-pollutant additions to `emsday(MXPOL)`. Equivalent to
    /// the difference in `emsday` after the call: positive for
    /// computed emissions, [`crate::common::consts::RMISS`] for
    /// missing-factor slots (`emsday(j) = RMISS`).
    pub emsday_delta: Vec<f32>,
    /// Per-pollutant `emsbmy(MXPOL)` for this iteration.
    pub emsbmy: Vec<f32>,
    /// Fuel consumption for this exhaust iteration (Fortran
    /// `fulbmy`). The evap branch does not compute fuel; it
    /// reuses the exhaust value.
    pub fulbmy: f32,
}

/// Per-activity-record data carried out of [`GeographyCallbacks::activity_record`].
///
/// Mirrors the COMMON-block reads `prccty.f`/`prcsub.f` perform
/// against `/actdat/` via the matched `idxact` slot.
#[derive(Debug, Clone, PartialEq)]
pub struct ActivityRecord {
    /// Starts per period (Fortran `starts(idxact)`).
    pub starts: f32,
    /// Activity level (Fortran `actlev(idxact)`).
    pub activity_level: f32,
    /// Activity-units indicator (Fortran `iactun(idxact)`).
    pub activity_unit: ActivityUnit,
    /// Load factor (Fortran `faclod(idxact)`).
    pub load_factor: f32,
    /// Age code for the alternate-curve lookup (Fortran `actage(idxact)`).
    pub age_code: String,
}

/// Allocated population for the subcounty branch.
///
/// `prcsub.f` :263–:266 calls `alosub(jerr, popsub, grwsub, …)` to
/// split the state-level population across the subcounty. The
/// Fortran call also yields `growth` (the cached growth value) and
/// `luse` (skip flag); the Rust port carries them as fields.
#[derive(Debug, Clone, PartialEq)]
pub struct SubcountyAllocation {
    /// Allocated population for this subcounty (`popsub`).
    pub population: f32,
    /// Pre-computed growth, or `None` when not yet computed
    /// (Fortran sentinel `growth == -9`).
    pub growth: Option<f32>,
    /// `false` ⇒ skip this subcounty (Fortran `luse`).
    pub use_record: bool,
}

/// Dependencies of [`super::process_county`] / [`super::process_subcounty`]
/// that come from subroutines outside this module.
///
/// These mirror the helper calls `prccty.f`/`prcsub.f` make:
/// `fndchr`, `fndtch`, `fndevtch`, `fndasc` (prcsub only),
/// `fndgxf`, `fndact`, `fndrfm`, `fndrtrft`, `getgrw`, `grwfac`,
/// `modyr`, `agedist`, `daymthf`, `emsadj`, `emfclc`, `evemfclc`.
///
/// `modyr` + `agedist` are bundled together into [`Self::model_year_and_agedist`]
/// because their closure-based callbacks would otherwise have to be
/// threaded through trait methods.
///
/// Output sinks (`wrtdat`/`wrtbmy`/`sitot`) are NOT in the trait:
/// they are returned as `Vec<…>` on [`ProcessOutput`] so callers
/// (production or tests) collect them as data. Same for `chkwrn`,
/// which lives in [`ProcessOutput::warnings`].
///
/// `clcems`, `clcevems`, and `clcrtrft` are **already ported** in
/// [`crate::emissions`]; the orchestration calls them directly
/// rather than going through the trait.
pub trait GeographyCallbacks {
    /// Find the county FIPS slot in the run's county list (Fortran
    /// `fndchr(fipin, 5, fipcod, NCNTY)`). Returns `None` when the
    /// county is not in the run (Fortran `idxfip .LE. 0` → `ISKIP`).
    fn find_fips(&self, fips: &str) -> Option<usize>;

    /// Record the per-county processed-record-count update (Fortran
    /// `nctyrc(idxfip) = nctyrc(idxfip) + 1`).
    fn tally_county_record(&mut self, fips_idx: usize);

    /// Find the exhaust-tech slot and accompanying tech-name +
    /// tech-fraction arrays (Fortran `fndtch(asccod, hpval, year)`).
    fn find_exhaust_tech(&self, scc: &str, hp_avg: f32, year: i32) -> Option<TechLookup>;

    /// Find the evap-tech slot (Fortran `fndevtch`).
    fn find_evap_tech(&self, scc: &str, hp_avg: f32, year: i32) -> Option<TechLookup>;

    /// Look up the refueling mode for `(SCC, HP, tech_name)`
    /// (Fortran `fndrfm`). Returns `None` for no match — the caller
    /// then re-queries with the default tech (`TECDEF = 'ALL'`).
    fn find_refueling(&self, scc: &str, hp_avg: f32, tech_name: &str) -> Option<RefuelingData>;

    /// Look up the growth cross-reference slot (Fortran `fndgxf`).
    fn find_growth_xref(&self, fips: &str, scc: &str, hp_avg: f32) -> Option<usize>;

    /// Find the activity-data slot (Fortran `fndact`).
    fn find_activity(&self, scc: &str, fips: &str, hp_avg: f32) -> Option<usize>;

    /// Find the allocation-record slot for the subcounty branch
    /// (Fortran `fndasc(asccod, ascalo, nalorc)`). Default impl
    /// returns `None` — only the subcounty processor needs to
    /// override.
    fn find_allocation(&self, _scc: &str) -> Option<usize> {
        None
    }

    /// Compute the subcounty allocation (Fortran `alosub`). Default
    /// impl returns a zero-population skip — only the subcounty
    /// processor needs to override.
    #[allow(clippy::too_many_arguments)]
    fn allocate_subcounty(
        &mut self,
        _record_index: usize,
        _allocation_index: usize,
        _fips_index: usize,
        _subcounty: &str,
        _base_pop_year: i32,
        _population: f32,
        _cached_growth: Option<f32>,
    ) -> Result<SubcountyAllocation> {
        Ok(SubcountyAllocation {
            population: 0.0,
            growth: None,
            use_record: false,
        })
    }

    /// Find the subcounty marker for the current record (Fortran
    /// `prcsub.f` :246–:258 — `reglst(idxreg)(6:10)` after a
    /// linear search). Returns `None` when the region code is
    /// blank (Fortran `subcur == '     '` → `ISKIP`).
    ///
    /// Default impl returns `None` so the county path is a no-op.
    fn find_subcounty(&self, _region_code: &str) -> Option<String> {
        None
    }

    /// Apply a retrofit filter mutation (Fortran
    /// `fndrtrft(fltrtyp, …)`). The implementation maintains the
    /// running filter state; subsequent calls progressively narrow
    /// the surviving records.
    fn filter_retrofits(
        &mut self,
        filter: RetrofitFilter,
        scc: &str,
        hp_avg: f32,
        model_year: i32,
        tech_name: &str,
    ) -> Result<()>;

    /// Yield the currently-surviving retrofit records — Fortran
    /// `rtrftfltr3` (after the type-3 filter call). The Rust port
    /// requests this once per tech-type iteration to feed
    /// `clcrtrft`.
    fn surviving_retrofits(&self) -> Vec<&RetrofitRecord>;

    /// Compute the daily/monthly factor table (Fortran
    /// `daymthf(asccod, fipin, daymthfac, mthf, dayf, ndays)`).
    /// Returns `(daymthfac[365], mthf, dayf, ndays)`.
    fn day_month_factors(&self, scc: &str, fips: &str) -> ([f32; MXDAYS], f32, f32, i32);

    /// Compute the daily emission-adjustment table (Fortran
    /// `emsadj(adjems, asccod, fipin, daymthfac)`).
    fn emission_adjustments(
        &self,
        scc: &str,
        fips: &str,
        daymthfac: &[f32; MXDAYS],
    ) -> AdjustmentTable;

    /// Bundled `modyr` + `agedist` invocation.
    ///
    /// The Fortran routines call `modyr` then `agedist` back-to-back
    /// (with the latter consuming the former's `modfrc` and
    /// `yryrfrcscrp` outputs). Both Fortran calls use closure-style
    /// dispatch to scrappage / growth-factor helpers; the Rust ports
    /// expose those via `FnOnce` / `FnMut` closures. Bundling the two
    /// here lets the trait method own the closures and hand back a
    /// single result, avoiding the borrow-conflict trap of letting
    /// the caller pass closures that also borrow `self`.
    #[allow(clippy::too_many_arguments)]
    fn model_year_and_agedist(
        &mut self,
        activity_index: usize,
        record: &PopulationRecord<'_>,
        fips: &str,
        growth_index: usize,
        episode_year: i32,
        growth_year: i32,
        base_population: f32,
    ) -> Result<ModelYearAgedistResult>;

    /// Compute the exhaust EF table for the current
    /// `(SCC, model_year, tech_index)` slot (Fortran `emfclc`).
    #[allow(clippy::too_many_arguments)]
    fn compute_exhaust_factors(
        &mut self,
        scc: &str,
        tech_names: &[String],
        tech_fractions: &[f32],
        model_year: i32,
        year_index: usize,
        record_index: usize,
    ) -> Result<ExhaustFactorsLookup>;

    /// Compute the evap EF table for the current
    /// `(SCC, model_year, evap_tech_index)` slot (Fortran `evemfclc`).
    #[allow(clippy::too_many_arguments)]
    fn compute_evap_factors(
        &mut self,
        scc: &str,
        evap_tech_names: &[String],
        evap_tech_fractions: &[f32],
        model_year: i32,
        year_index: usize,
        record_index: usize,
    ) -> Result<EvapFactorsLookup>;

    /// Run one `(model_year, exhaust_tech_index)` iteration of the
    /// exhaust calculator (`clcems.f`).
    ///
    /// The orchestrator passes the slot-and-iteration context; the
    /// implementation assembles the full
    /// [`crate::emissions::exhaust::ExhaustCalcInputs`] from the
    /// SCC-level + run-level state it carries and invokes
    /// [`crate::emissions::exhaust::calculate_exhaust_emissions`].
    /// Test fakes can stub the trait method directly to return
    /// canned `EmissionsIterationResult`s without setting up the
    /// full ~30-field input.
    #[allow(clippy::too_many_arguments)]
    fn compute_exhaust_iteration(
        &mut self,
        record: &PopulationRecord<'_>,
        options: &RunOptions,
        factors: &ExhaustFactorsLookup,
        adjustments: &AdjustmentTable,
        scc_tech_index: usize,
        tech_index: usize,
        year_index: usize,
        equipment_age: f32,
        tech_fraction: f32,
        temporal_adjustment: f32,
        starts_adjustment: f32,
        model_year_fraction: f32,
        activity_adjustment: f32,
        population: f32,
        n_days: i32,
        activity_index: usize,
    ) -> Result<EmissionsIterationResult>;

    /// Run one `(model_year, evap_tech_index)` iteration of the evap
    /// calculator (`clcevems.f`).
    #[allow(clippy::too_many_arguments)]
    fn compute_evap_iteration(
        &mut self,
        record: &PopulationRecord<'_>,
        options: &RunOptions,
        factors: &EvapFactorsLookup,
        adjustments: &AdjustmentTable,
        refueling: &RefuelingData,
        scc_tech_index: usize,
        tech_index: usize,
        year_index: usize,
        equipment_age: f32,
        evap_tech_fraction: f32,
        evap_tech_name: &str,
        temporal_adjustment: f32,
        starts_adjustment: f32,
        model_year_fraction: f32,
        activity_adjustment: f32,
        population: f32,
        n_days: i32,
        fulbmy: f32,
    ) -> Result<EmissionsIterationResult>;

    /// Get the activity-record-derived inputs (`starts(idxact)`,
    /// `actlev(idxact)`, `iactun(idxact)`, `faclod(idxact)`,
    /// `actage(idxact)`) for the matched slot.
    fn activity_record(&self, activity_index: usize) -> ActivityRecord;
}

/// Sink-only [`GeographyCallbacks`] implementation that errors on every
/// data-producing call.
///
/// Useful for testing the early-return paths (FIPS-not-in-run,
/// zero-population) without wiring all the dependencies.
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct NoopCallbacks;

impl GeographyCallbacks for NoopCallbacks {
    fn find_fips(&self, _fips: &str) -> Option<usize> {
        None
    }
    fn tally_county_record(&mut self, _fips_idx: usize) {}
    fn find_exhaust_tech(&self, _: &str, _: f32, _: i32) -> Option<TechLookup> {
        None
    }
    fn find_evap_tech(&self, _: &str, _: f32, _: i32) -> Option<TechLookup> {
        None
    }
    fn find_refueling(&self, _: &str, _: f32, _: &str) -> Option<RefuelingData> {
        None
    }
    fn find_growth_xref(&self, _: &str, _: &str, _: f32) -> Option<usize> {
        None
    }
    fn find_activity(&self, _: &str, _: &str, _: f32) -> Option<usize> {
        None
    }
    fn filter_retrofits(
        &mut self,
        _: RetrofitFilter,
        _: &str,
        _: f32,
        _: i32,
        _: &str,
    ) -> Result<()> {
        Ok(())
    }
    fn surviving_retrofits(&self) -> Vec<&RetrofitRecord> {
        Vec::new()
    }
    fn day_month_factors(&self, _: &str, _: &str) -> ([f32; MXDAYS], f32, f32, i32) {
        ([0.0; MXDAYS], 0.0, 0.0, 0)
    }
    fn emission_adjustments(&self, _: &str, _: &str, _: &[f32; MXDAYS]) -> AdjustmentTable {
        AdjustmentTable::new(MXDAYS)
    }
    fn model_year_and_agedist(
        &mut self,
        _: usize,
        _: &PopulationRecord<'_>,
        _: &str,
        _: usize,
        _: i32,
        _: i32,
        _: f32,
    ) -> Result<ModelYearAgedistResult> {
        Err(Error::Config(
            "NoopCallbacks::model_year_and_agedist".into(),
        ))
    }
    fn compute_exhaust_factors(
        &mut self,
        _: &str,
        _: &[String],
        _: &[f32],
        _: i32,
        _: usize,
        _: usize,
    ) -> Result<ExhaustFactorsLookup> {
        Err(Error::Config(
            "NoopCallbacks::compute_exhaust_factors".into(),
        ))
    }
    fn compute_evap_factors(
        &mut self,
        _: &str,
        _: &[String],
        _: &[f32],
        _: i32,
        _: usize,
        _: usize,
    ) -> Result<EvapFactorsLookup> {
        Err(Error::Config("NoopCallbacks::compute_evap_factors".into()))
    }
    fn compute_exhaust_iteration(
        &mut self,
        _: &PopulationRecord<'_>,
        _: &RunOptions,
        _: &ExhaustFactorsLookup,
        _: &AdjustmentTable,
        _: usize,
        _: usize,
        _: usize,
        _: f32,
        _: f32,
        _: f32,
        _: f32,
        _: f32,
        _: f32,
        _: f32,
        _: i32,
        _: usize,
    ) -> Result<EmissionsIterationResult> {
        Err(Error::Config(
            "NoopCallbacks::compute_exhaust_iteration".into(),
        ))
    }
    fn compute_evap_iteration(
        &mut self,
        _: &PopulationRecord<'_>,
        _: &RunOptions,
        _: &EvapFactorsLookup,
        _: &AdjustmentTable,
        _: &RefuelingData,
        _: usize,
        _: usize,
        _: usize,
        _: f32,
        _: f32,
        _: &str,
        _: f32,
        _: f32,
        _: f32,
        _: f32,
        _: f32,
        _: i32,
        _: f32,
    ) -> Result<EmissionsIterationResult> {
        Err(Error::Config(
            "NoopCallbacks::compute_evap_iteration".into(),
        ))
    }
    fn activity_record(&self, _: usize) -> ActivityRecord {
        ActivityRecord {
            starts: 0.0,
            activity_level: 0.0,
            activity_unit: ActivityUnit::HoursPerYear,
            load_factor: 0.0,
            age_code: String::new(),
        }
    }
}

// =============================================================================
// Helpers — pure functions used by both county and subcounty processors
// =============================================================================

/// Categorize a piece of equipment's HP range into one of the run's
/// HP buckets.
///
/// Mirrors `prccty.f` :214–225 / `prcsub.f` :217–228. The "level" is
/// the upper boundary of the bucket the range midpoint falls into,
/// with two edge cases:
/// - Midpoint `<= hp_levels[0]` → return `hp_levels[0]`.
/// - Midpoint `> hp_levels[MXHPC-1]` → return `9999.0`.
pub fn hp_level_lookup(hp_range: (f32, f32), hp_levels: &[f32]) -> f32 {
    let hp_mid = (hp_range.0 + hp_range.1) / 2.0;
    if hp_mid <= hp_levels[0] {
        return hp_levels[0];
    }
    if hp_mid > hp_levels[hp_levels.len() - 1] {
        return 9999.0;
    }
    // Fortran: do 10 i=2,MXHPC ; if hplev < 0 .AND. hpmid < hpclev(i) then hplev = hpclev(i)
    // The "first match wins" semantics come from the
    // `hplev .LT. 0` guard: once set, subsequent matches are
    // ignored.
    let mut hplev: f32 = -9.0;
    for &boundary in hp_levels.iter().skip(1) {
        if hplev < 0.0 && hp_mid < boundary {
            hplev = boundary;
        }
    }
    hplev
}

/// Return the fuel density for `fuel`.
///
/// Mirrors `prccty.f` :282–291 / `prcsub.f` :313–322. The density
/// defaults to `1.0` for fuels outside the recognized set; this
/// matches the Fortran source's `denful = 1.0` initialization.
pub fn fuel_density(fuel: FuelKind) -> f32 {
    match fuel {
        FuelKind::Gasoline2Stroke | FuelKind::Gasoline4Stroke => DENGAS as f32,
        FuelKind::Cng => DENCNG as f32,
        FuelKind::Lpg => DENLPG as f32,
        FuelKind::Diesel => DENDSL as f32,
    }
}

/// Time-period adjustment factors used by every per-iteration call
/// to `clcems` / `clcevems`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TimePeriodFactors {
    /// `1.0` for total mode, `1.0/ndays` otherwise (Fortran `adjtime`).
    pub adjtime: f32,
    /// `dayf` in daily mode, `mthf * dayf` otherwise (Fortran `tplfac`).
    pub tplfac: f32,
    /// Always `mthf * dayf` (Fortran `tplful`) — used by the fuel
    /// accumulation branch.
    pub tplful: f32,
}

/// Compute `(adjtime, tplfac, tplful)` from the run mode and the
/// `daymthf` outputs.
///
/// Mirrors `prccty.f` :301–312 / `prcsub.f` :332–343.
///
/// `n_days` is clamped to `>= 1` to avoid divide-by-zero.
pub fn time_period_setup(
    sum_type: SumType,
    n_days: i32,
    daily_mode: bool,
    mthf: f32,
    dayf: f32,
) -> TimePeriodFactors {
    let adjtime = match sum_type {
        SumType::Total => 1.0,
        SumType::Typical => 1.0 / (n_days.max(1) as f32),
    };
    let tplfac = if daily_mode { dayf } else { mthf * dayf };
    let tplful = mthf * dayf;
    TimePeriodFactors {
        adjtime,
        tplfac,
        tplful,
    }
}

/// Return the per-iteration `tpltmp` factor based on the activity-
/// units indicator, mirroring the two-branch `if` at `prccty.f`
/// :500–:506 / `prcsub.f` :531–:537.
///
/// Years-per-X units use the period `tplfac`; days-per-X units use
/// `1.0` (the daily branch already folds the per-day scale into the
/// EF).
pub fn temporal_adjustment(units: ActivityUnit, tplfac: f32) -> f32 {
    match units {
        ActivityUnit::HoursPerYear | ActivityUnit::GallonsPerYear => tplfac,
        ActivityUnit::HoursPerDay | ActivityUnit::GallonsPerDay => 1.0,
    }
}

/// Convenience accumulator: add one exhaust-iteration's contribution
/// to the running `(poptot, acttot, strtot)` triple.
///
/// Mirrors `prccty.f` :558–:562 / `prcsub.f` :590–:594.
#[allow(clippy::too_many_arguments)]
pub fn accumulate_exhaust_iteration(
    poptot: &mut f32,
    acttot: &mut f32,
    strtot: &mut f32,
    population: f32,
    modfrc: f32,
    actadj: f32,
    stradj: f32,
    tplful: f32,
    adjtime: f32,
) {
    *poptot += population * modfrc;
    *acttot += actadj * population * modfrc * tplful * adjtime;
    *strtot += stradj * population * modfrc * tplful * adjtime;
}

/// Convenience accumulator: add one evap-iteration's contribution
/// to the running `(evpoptot, evacttot, evstrtot)` triple.
///
/// Mirrors `prccty.f` :708–:712 / `prcsub.f` :739–:743 (identical to
/// the exhaust accumulator).
#[allow(clippy::too_many_arguments)]
pub fn accumulate_evap_iteration(
    evpoptot: &mut f32,
    evacttot: &mut f32,
    evstrtot: &mut f32,
    population: f32,
    modfrc: f32,
    actadj: f32,
    stradj: f32,
    tplful: f32,
    adjtime: f32,
) {
    accumulate_exhaust_iteration(
        evpoptot, evacttot, evstrtot, population, modfrc, actadj, stradj, tplful, adjtime,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::consts::MXPOL;

    fn hp_levels_default() -> [f32; MXHPC] {
        let vs: [f32; MXHPC] = [
            3.0, 6.0, 11.0, 16.0, 25.0, 40.0, 50.0, 75.0, 100.0, 175.0, 300.0, 600.0, 750.0,
            1000.0, 1200.0, 1500.0, 1800.0, 2000.0,
        ];
        let mut levels = [0.0_f32; MXHPC];
        levels.copy_from_slice(&vs);
        levels
    }

    #[test]
    fn hp_level_lookup_picks_first_boundary_above_midpoint() {
        let levels = hp_levels_default();
        assert_eq!(hp_level_lookup((30.0, 50.0), &levels), 50.0);
        assert_eq!(hp_level_lookup((50.0, 60.0), &levels), 75.0);
        assert_eq!(hp_level_lookup((110.0, 130.0), &levels), 175.0);
    }

    #[test]
    fn hp_level_lookup_floor_and_overflow() {
        let levels = hp_levels_default();
        // Midpoint strictly below first boundary → first boundary
        // (Fortran `.LE.` at :215 matches both `<` and `==`).
        assert_eq!(hp_level_lookup((0.0, 2.0), &levels), 3.0);
        // Midpoint exactly equal to the first boundary → first
        // boundary (the `.LE.` branch fires, before the strict-less
        // loop runs).
        assert_eq!(hp_level_lookup((0.0, 6.0), &levels), 3.0);
        // Midpoint above first boundary, below second → the
        // strict-less loop returns the second boundary.
        assert_eq!(hp_level_lookup((1.5, 8.5), &levels), 6.0);
        // Above last boundary → 9999.0.
        assert_eq!(hp_level_lookup((3000.0, 4000.0), &levels), 9999.0);
    }

    #[test]
    fn fuel_density_matches_fortran_constants() {
        assert_eq!(fuel_density(FuelKind::Gasoline2Stroke), DENGAS as f32);
        assert_eq!(fuel_density(FuelKind::Gasoline4Stroke), DENGAS as f32);
        assert_eq!(fuel_density(FuelKind::Cng), DENCNG as f32);
        assert_eq!(fuel_density(FuelKind::Lpg), DENLPG as f32);
        assert_eq!(fuel_density(FuelKind::Diesel), DENDSL as f32);
    }

    #[test]
    fn time_period_setup_total_mode_zeros_adjtime() {
        let tp = time_period_setup(SumType::Total, 31, false, 0.5, 1.0);
        assert_eq!(tp.adjtime, 1.0);
        assert_eq!(tp.tplfac, 0.5);
        assert_eq!(tp.tplful, 0.5);
    }

    #[test]
    fn time_period_setup_typical_uses_one_over_ndays() {
        let tp = time_period_setup(SumType::Typical, 31, false, 0.5, 1.0);
        assert_eq!(tp.adjtime, 1.0 / 31.0);
        assert_eq!(tp.tplfac, 0.5);
        assert_eq!(tp.tplful, 0.5);
    }

    #[test]
    fn time_period_setup_daily_drops_monthly_factor_from_tplfac() {
        let tp = time_period_setup(SumType::Typical, 1, true, 0.5, 2.0);
        assert_eq!(tp.tplfac, 2.0);
        // tplful keeps both factors regardless of daily_mode.
        assert_eq!(tp.tplful, 0.5 * 2.0);
    }

    #[test]
    fn time_period_setup_clamps_zero_ndays() {
        let tp = time_period_setup(SumType::Typical, 0, false, 1.0, 1.0);
        assert_eq!(tp.adjtime, 1.0);
    }

    #[test]
    fn temporal_adjustment_yearly_uses_tplfac() {
        assert_eq!(temporal_adjustment(ActivityUnit::HoursPerYear, 0.5), 0.5);
        assert_eq!(temporal_adjustment(ActivityUnit::GallonsPerYear, 0.5), 0.5);
    }

    #[test]
    fn temporal_adjustment_daily_returns_one() {
        assert_eq!(temporal_adjustment(ActivityUnit::HoursPerDay, 0.5), 1.0);
        assert_eq!(temporal_adjustment(ActivityUnit::GallonsPerDay, 0.5), 1.0);
    }

    #[test]
    fn accumulate_exhaust_iteration_matches_fortran() {
        let mut p = 0.0_f32;
        let mut a = 0.0_f32;
        let mut s = 0.0_f32;
        // Fortran: poptot = popcty * modfrc(idxyr)
        //          acttot = actadj * popcty * modfrc * tplful * adjtime
        //          strtot = stradj * popcty * modfrc * tplful * adjtime
        accumulate_exhaust_iteration(&mut p, &mut a, &mut s, 100.0, 0.5, 2.0, 3.0, 0.5, 0.25);
        assert_eq!(p, 50.0);
        assert_eq!(a, 2.0 * 100.0 * 0.5 * 0.5 * 0.25);
        assert_eq!(s, 3.0 * 100.0 * 0.5 * 0.5 * 0.25);
    }

    #[test]
    fn noop_callbacks_skip_path_returns_none() {
        let nc = NoopCallbacks;
        assert!(nc.find_fips("17001").is_none());
        assert!(nc.find_exhaust_tech("2270001000", 100.0, 2020).is_none());
        assert!(nc.find_evap_tech("2270001000", 100.0, 2020).is_none());
    }

    #[test]
    fn process_output_default_is_empty() {
        let o = ProcessOutput::default();
        assert!(o.dat_records.is_empty());
        assert!(o.bmy_records.is_empty());
        assert!(o.si_records.is_empty());
        assert!(o.warnings.is_empty());
        assert!(o.emissions_day.is_empty());
        assert_eq!(o.emsams_delta.len(), 0);
        assert_eq!(o.emsams_fips_index, None);
    }

    #[test]
    fn process_outcome_into_output_round_trips() {
        let out = ProcessOutput {
            fips: "17001".into(),
            emissions_day: vec![0.0; MXPOL],
            ..ProcessOutput::default()
        };
        let success = ProcessOutcome::Success(out.clone());
        let skip = ProcessOutcome::Skipped(out.clone());
        assert!(!success.is_skipped());
        assert!(skip.is_skipped());
        assert_eq!(success.into_output().fips, "17001");
        assert_eq!(skip.into_output().fips, "17001");
    }
}

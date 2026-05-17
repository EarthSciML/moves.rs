//! Run configuration for [`run_simulation`](super::run_simulation) —
//! the in-memory replacement for the NONROAD `.opt` file.
//!
//! The Fortran source reads the `.opt` file into ten-plus COMMON
//! blocks (`nonrdusr.inc` and friends); MOVES, in the Java↔Fortran
//! bridge this task replaces, *generated* that `.opt` file on disk and
//! shipped it to a worker. The Rust orchestrator instead builds a
//! [`NonroadOptions`] value directly and hands it to
//! [`run_simulation`](super::run_simulation) — no scratch file, no
//! subprocess.
//!
//! [`NonroadOptions`] carries the *run-global* settings the driver
//! loop and the geography executor need: the geographic level, the
//! episode / growth / technology years, and the output-channel and
//! loaded-packet flags. It deliberately does **not** carry per-record
//! state (population, emission factors, …) — that is the job of
//! [`NonroadInputs`](super::NonroadInputs) — nor the per-SCC-group
//! `fuel`, which [`fuel_for_scc`](crate::driver::fuel_for_scc) derives
//! from each group's SCC.
//!
//! This is distinct from the `geography` module's per-routine
//! `RunOptions` despite the overlap: that type is a *derived*,
//! per-SCC-group view (it adds the resolved `fuel`, the HP-level
//! table, and other routine-specific state) that a production
//! [`GeographyExecutor`](super::GeographyExecutor) builds from this
//! run-global value. [`NonroadOptions`] is the single value the
//! orchestrator sets.

use crate::driver::RegionLevel;
use crate::{Error, Result};

/// Lowest episode / growth / technology year [`NonroadOptions::validate`]
/// accepts. A sanity floor, not a NONROAD domain limit — the
/// authoritative range check belongs to the option-file parser
/// (`rdnropt.f`, Task 99 / [`crate::input::options`]).
pub const MIN_YEAR: i32 = 1990;

/// Highest episode / growth / technology year [`NonroadOptions::validate`]
/// accepts. See [`MIN_YEAR`] for the "sanity, not domain" caveat.
pub const MAX_YEAR: i32 = 2099;

/// Run-global configuration for a single NONROAD simulation.
///
/// Built by the orchestrator (or, for the standalone CLI, by the
/// option-file parser) and passed by `&` to
/// [`run_simulation`](super::run_simulation). Every field maps to a
/// `nonrdusr.inc` COMMON-block variable; the rustdoc names the Fortran
/// counterpart.
///
/// # Construction
///
/// [`NonroadOptions::new`] sets the run-global years and leaves every
/// output channel disabled — the common starting point. Callers then
/// flip the flags they need. All fields are `pub`, so a fully-specified
/// struct literal is equally valid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NonroadOptions {
    /// Geographic level the run operates at — Fortran `reglvl`.
    /// Drives [`dispatch_for`](crate::driver::dispatch_for): the same
    /// population record dispatches to different geography routines at
    /// different levels.
    pub region_level: RegionLevel,

    /// Episode year — Fortran `iepyr`. Anchors the model-year loop in
    /// every geography routine.
    pub episode_year: i32,

    /// Growth year — Fortran `igryr`. The year population is grown /
    /// back-cast to before the age distribution is applied.
    pub growth_year: i32,

    /// Technology year — Fortran `itchyr`. Caps the model year when
    /// looking up technology-fraction data (`min(model_year, itchyr)`).
    pub tech_year: i32,

    /// `true` for a "total" (period-summed) run, `false` for a
    /// typical-day run — Fortran `ismtyp == IDXTOT`.
    pub total_mode: bool,

    /// `true` when day-of-year output is requested — Fortran `ldayfl`.
    pub daily_output: bool,

    /// `true` when by-model-year *exhaust* output is requested —
    /// Fortran `lbmyfl`.
    pub emit_bmy_exhaust: bool,

    /// `true` when by-model-year *evaporative* output is requested —
    /// Fortran `levbmyfl`.
    pub emit_bmy_evap: bool,

    /// `true` when the SI (state-import) report is requested —
    /// Fortran `lsifl`.
    pub emit_si: bool,

    /// `true` when the growth-file packet was loaded — Fortran
    /// `lgrwfl`. A future-year or back-cast run with this `false` is
    /// the Fortran `7003` fatal-error path.
    pub growth_loaded: bool,

    /// `true` when retrofit records were loaded — Fortran `lrtrftfl`.
    pub retrofit_loaded: bool,

    /// `true` when the spillage / refueling-mode packet was loaded —
    /// Fortran `lfacfl(IDXSPL)`. Gates evaporative refueling setup.
    pub spillage_loaded: bool,

    /// Free-text run title — Fortran `title1`. Echoed into the
    /// completion banner and output headers; not otherwise load-bearing.
    pub title: String,
}

impl NonroadOptions {
    /// Create a [`NonroadOptions`] for a run at `region_level` whose
    /// episode, growth, and technology years are all `year`.
    ///
    /// Every output channel ([`emit_bmy_exhaust`](Self::emit_bmy_exhaust),
    /// [`emit_si`](Self::emit_si), …) starts disabled and every
    /// loaded-packet flag starts `false`; the title is empty. This is
    /// the minimal single-year run — callers enable channels and set
    /// the growth/tech years they need afterwards.
    pub fn new(region_level: RegionLevel, year: i32) -> Self {
        Self {
            region_level,
            episode_year: year,
            growth_year: year,
            tech_year: year,
            total_mode: false,
            daily_output: false,
            emit_bmy_exhaust: false,
            emit_bmy_evap: false,
            emit_si: false,
            growth_loaded: false,
            retrofit_loaded: false,
            spillage_loaded: false,
            title: String::new(),
        }
    }

    /// `true` when *any* by-model-year channel (exhaust or evap) is
    /// requested. The geography routines build the by-model-year
    /// accumulators only when this holds.
    pub fn emit_bmy(&self) -> bool {
        self.emit_bmy_exhaust || self.emit_bmy_evap
    }

    /// Validate the run-global years.
    ///
    /// Each of [`episode_year`](Self::episode_year),
    /// [`growth_year`](Self::growth_year), and
    /// [`tech_year`](Self::tech_year) must fall within
    /// `[MIN_YEAR, MAX_YEAR]`. The bounds are a sanity check that
    /// catches an unset (`0`) or transposed field before the driver
    /// loop runs — the authoritative range is the option-file parser's
    /// (`rdnropt.f`).
    ///
    /// Cross-field relationships (e.g. growth year vs. episode year)
    /// are *not* checked: a back-cast run legitimately has
    /// `growth_year < episode_year`, and the geography routines accept
    /// either ordering.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] naming the first out-of-range field.
    pub fn validate(&self) -> Result<()> {
        check_year("episode_year", self.episode_year)?;
        check_year("growth_year", self.growth_year)?;
        check_year("tech_year", self.tech_year)?;
        Ok(())
    }
}

/// Range-check one year field for [`NonroadOptions::validate`].
fn check_year(field: &str, year: i32) -> Result<()> {
    if !(MIN_YEAR..=MAX_YEAR).contains(&year) {
        return Err(Error::Config(format!(
            "NonroadOptions.{field} = {year} is outside the sanity range \
             [{MIN_YEAR}, {MAX_YEAR}]"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_sets_all_three_years_and_disables_channels() {
        let opts = NonroadOptions::new(RegionLevel::County, 2020);
        assert_eq!(opts.region_level, RegionLevel::County);
        assert_eq!(opts.episode_year, 2020);
        assert_eq!(opts.growth_year, 2020);
        assert_eq!(opts.tech_year, 2020);
        assert!(!opts.total_mode);
        assert!(!opts.emit_bmy_exhaust);
        assert!(!opts.emit_bmy_evap);
        assert!(!opts.emit_si);
        assert!(!opts.daily_output);
        assert!(!opts.growth_loaded);
        assert!(!opts.retrofit_loaded);
        assert!(!opts.spillage_loaded);
        assert!(opts.title.is_empty());
    }

    #[test]
    fn emit_bmy_is_the_or_of_the_two_channels() {
        let mut opts = NonroadOptions::new(RegionLevel::State, 2020);
        assert!(!opts.emit_bmy());
        opts.emit_bmy_exhaust = true;
        assert!(opts.emit_bmy());
        opts.emit_bmy_exhaust = false;
        opts.emit_bmy_evap = true;
        assert!(opts.emit_bmy());
    }

    #[test]
    fn validate_accepts_a_well_formed_run() {
        let opts = NonroadOptions::new(RegionLevel::Nation, 2020);
        assert!(opts.validate().is_ok());
    }

    #[test]
    fn validate_accepts_the_inclusive_bounds() {
        assert!(NonroadOptions::new(RegionLevel::County, MIN_YEAR)
            .validate()
            .is_ok());
        assert!(NonroadOptions::new(RegionLevel::County, MAX_YEAR)
            .validate()
            .is_ok());
    }

    #[test]
    fn validate_accepts_a_backcast_run() {
        // growth_year < episode_year is a legitimate back-cast — not
        // a cross-field error.
        let mut opts = NonroadOptions::new(RegionLevel::County, 2020);
        opts.growth_year = 2005;
        assert!(opts.validate().is_ok());
    }

    #[test]
    fn validate_rejects_unset_episode_year() {
        // The most common mistake: a struct built field-by-field with
        // episode_year left at its 0 default.
        let mut opts = NonroadOptions::new(RegionLevel::County, 2020);
        opts.episode_year = 0;
        let err = opts.validate().unwrap_err();
        match err {
            Error::Config(msg) => assert!(msg.contains("episode_year")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn validate_rejects_out_of_range_growth_year() {
        let mut opts = NonroadOptions::new(RegionLevel::County, 2020);
        opts.growth_year = MAX_YEAR + 1;
        let err = opts.validate().unwrap_err();
        match err {
            Error::Config(msg) => {
                assert!(msg.contains("growth_year"));
                assert!(msg.contains(&(MAX_YEAR + 1).to_string()));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn validate_rejects_out_of_range_tech_year() {
        let mut opts = NonroadOptions::new(RegionLevel::County, 2020);
        opts.tech_year = MIN_YEAR - 1;
        let err = opts.validate().unwrap_err();
        match err {
            Error::Config(msg) => assert!(msg.contains("tech_year")),
            other => panic!("unexpected error: {other:?}"),
        }
    }
}

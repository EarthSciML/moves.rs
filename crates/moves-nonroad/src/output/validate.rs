//! Output-side validators and the file-close routine (Task 114).
//!
//! Three small Fortran routines:
//!
//! | Fortran | Lines | Rust |
//! |---|---|---|
//! | `chkasc.f` | 115 | [`chkasc`] |
//! | `chkwrn.f` | 102 | [`WarningCounters::record`] |
//! | `clsnon.f` |  97 | [`close_nonroad_files`] |
//!
//! `chkasc.f` decides whether an SCC code is requested for (or simply
//! valid against) the run's equipment list; `chkwrn.f` tallies
//! warnings and flags when one warning class overflows its cap;
//! `clsnon.f` closed every Fortran I/O unit and deleted the scratch
//! files — a no-op under the Rust port's RAII I/O model.

/// Whether an SCC code matches one equipment code — `chkasc.f`
/// :82–99.
///
/// Three match shapes, tried in order:
///
/// * an exact 10-character match;
/// * a *4-digit global* — `scc` has `"000000"` in positions 5–10 —
///   matching any equipment code with the same first four digits;
/// * a *7-digit global* — `scc` has `"000"` in positions 8–10 —
///   matching any equipment code with the same first seven digits.
fn scc_matches(scc: &str, code: &str) -> bool {
    if scc == code {
        return true;
    }
    // chkasc.f :89 — a 4-digit global SCC ends in six zeros.
    if scc.get(4..10) == Some("000000") {
        return match (scc.get(..4), code.get(..4)) {
            (Some(a), Some(b)) => a == b,
            _ => false,
        };
    }
    // chkasc.f :94 — a 7-digit global SCC ends in three zeros.
    if scc.get(7..10) == Some("000") {
        return match (scc.get(..7), code.get(..7)) {
            (Some(a), Some(b)) => a == b,
            _ => false,
        };
    }
    false
}

/// Test an SCC code against the run's equipment list — `chkasc.f`.
///
/// `equipment_codes` is the equipment-code table (Fortran `eqpcod`,
/// e.g. [`crate::common::eqpcod`]). `requested` is the parallel
/// per-code "selected for this run" flag array (Fortran `lascat`).
///
/// * With `skip_unrequested` **true** the function answers *"is this
///   SCC requested?"* — unrequested equipment codes are skipped, so
///   only a selected code can produce a match. This is `chkasc.f`'s
///   primary use.
/// * With `skip_unrequested` **false** it answers *"is this SCC
///   valid?"* — every equipment code is considered and `requested`
///   is not consulted (pass an empty slice).
///
/// Returns `true` on the first matching equipment code, mirroring the
/// Fortran's early return.
pub fn chkasc(
    scc: &str,
    equipment_codes: &[&str],
    requested: &[bool],
    skip_unrequested: bool,
) -> bool {
    for (i, &code) in equipment_codes.iter().enumerate() {
        // chkasc.f :78 — skip an unrequested code when filtering.
        if skip_unrequested && !requested.get(i).copied().unwrap_or(false) {
            continue;
        }
        if scc_matches(scc, code) {
            return true;
        }
    }
    false
}

// ===========================================================================
// chkwrn — warning tally
// ===========================================================================

/// Maximum count for a single warning class before the run is
/// considered to have a data problem — Fortran `MXWARN = 5000000`
/// in `nonrdprm.inc`.
pub const MXWARN: i32 = 5_000_000;

/// The six warning classes NONROAD tallies separately — the
/// `IDXWEM..IDXWSE` indices of `nonrdprm.inc` (:109–114).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarningKind {
    /// `IDXWEM` — emissions warnings.
    Emissions = 1,
    /// `IDXWAL` — allocation warnings.
    Allocation = 2,
    /// `IDXWTC` — technology-fraction warnings.
    TechFraction = 3,
    /// `IDXWAC` — activity warnings.
    Activity = 4,
    /// `IDXWPP` — population warnings.
    Population = 5,
    /// `IDXWSE` — seasonality warnings.
    Seasonality = 6,
}

/// Result of recording a warning — `chkwrn.f`'s `ISUCES`/`IFAIL`
/// return codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarningOutcome {
    /// The warning was counted and no class is over [`MXWARN`]
    /// (`ISUCES`).
    WithinLimit,
    /// The warning was counted and its class has now exceeded
    /// [`MXWARN`] — the run's input data is suspect (`IFAIL`,
    /// `chkwrn.f`'s `7000` path).
    LimitExceeded,
}

/// NONROAD's warning tallies — the `nwarn`/`nwrnct` COMMON-block
/// counters (`nonrdio.inc`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WarningCounters {
    /// Total warnings across every class (Fortran `nwarn`).
    pub total: i32,
    /// Per-class warning counts (Fortran `nwrnct`), indexed by
    /// `WarningKind as usize - 1`.
    pub by_kind: [i32; 6],
}

impl WarningCounters {
    /// Record one warning of `kind` — `chkwrn.f`.
    ///
    /// Increments the overall counter and the per-class counter, then
    /// reports whether that class has passed [`MXWARN`]. The Fortran
    /// `chkwrn` also writes a diagnostic to the message and standard
    /// output files when the cap is exceeded; emitting that message
    /// is left to the caller, which owns the output streams (the I/O
    /// policy of `ARCHITECTURE.md` § 4.3).
    pub fn record(&mut self, kind: WarningKind) -> WarningOutcome {
        self.total += 1;
        let slot = kind as usize - 1;
        self.by_kind[slot] += 1;
        if self.by_kind[slot] > MXWARN {
            WarningOutcome::LimitExceeded
        } else {
            WarningOutcome::WithinLimit
        }
    }
}

// ===========================================================================
// clsnon — release output resources
// ===========================================================================

/// Release the NONROAD output resources — `clsnon.f`.
///
/// `clsnon.f` `CLOSE`d every Fortran I/O unit and deleted the four
/// scratch files ([`SCRATCH_POP_FILE`](crate::output::statics::SCRATCH_POP_FILE)
/// and friends). The Rust port has neither: per `ARCHITECTURE.md`
/// § 4.3 each reader and writer is owned by the orchestrating layer
/// and released — flushed and closed — when it is dropped, and
/// reference data is held in memory rather than in scratch files.
///
/// This function is therefore a deliberate no-op. It is retained so
/// the driver loop (`nonroad.f`, Task 113), which calls `clsnon` as
/// its final step, has a faithful call site; a caller that needs an
/// output stream flushed before the program ends does so explicitly
/// with [`std::io::Write::flush`].
pub fn close_nonroad_files() {}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- chkasc ----

    const CODES: [&str; 3] = ["2260001010", "2260001020", "2265001010"];

    #[test]
    fn chkasc_exact_match() {
        let requested = [true, true, true];
        assert!(chkasc("2260001020", &CODES, &requested, true));
    }

    #[test]
    fn chkasc_no_match() {
        let requested = [true, true, true];
        assert!(!chkasc("2270009999", &CODES, &requested, true));
    }

    #[test]
    fn chkasc_four_digit_global_matches_prefix() {
        // "2260000000" — six trailing zeros — matches any "2260…".
        let requested = [true, true, true];
        assert!(chkasc("2260000000", &CODES, &requested, true));
        // "2280000000" matches none of the codes.
        assert!(!chkasc("2280000000", &CODES, &requested, true));
    }

    #[test]
    fn chkasc_seven_digit_global_matches_prefix() {
        // "2260001000" — three trailing zeros — matches "2260001…".
        let requested = [true, true, true];
        assert!(chkasc("2260001000", &CODES, &requested, true));
        // "2265002000" shares no 7-digit prefix with the codes.
        assert!(!chkasc("2265002000", &CODES, &requested, true));
    }

    #[test]
    fn chkasc_skips_unrequested_codes_when_filtering() {
        // Only the third code is requested.
        let requested = [false, false, true];
        // The 2265 code is requested ⇒ matches.
        assert!(chkasc("2265001010", &CODES, &requested, true));
        // The 2260 codes are not requested ⇒ no match when filtering.
        assert!(!chkasc("2260001010", &CODES, &requested, true));
    }

    #[test]
    fn chkasc_validation_mode_ignores_requested_flags() {
        // skip_unrequested = false ⇒ every code is considered even
        // though `requested` is empty.
        assert!(chkasc("2260001010", &CODES, &[], false));
        assert!(!chkasc("9999999999", &CODES, &[], false));
    }

    // ---- chkwrn ----

    #[test]
    fn warning_counters_tally_total_and_per_kind() {
        let mut counters = WarningCounters::default();
        assert_eq!(
            counters.record(WarningKind::Emissions),
            WarningOutcome::WithinLimit
        );
        counters.record(WarningKind::Emissions);
        counters.record(WarningKind::Population);
        assert_eq!(counters.total, 3);
        assert_eq!(counters.by_kind[WarningKind::Emissions as usize - 1], 2);
        assert_eq!(counters.by_kind[WarningKind::Population as usize - 1], 1);
        assert_eq!(counters.by_kind[WarningKind::Allocation as usize - 1], 0);
    }

    #[test]
    fn warning_counters_flag_the_class_cap() {
        let mut counters = WarningCounters::default();
        // Seed the class to one below the cap, then trip it.
        counters.by_kind[WarningKind::Activity as usize - 1] = MXWARN - 1;
        assert_eq!(
            counters.record(WarningKind::Activity),
            WarningOutcome::WithinLimit
        );
        assert_eq!(
            counters.record(WarningKind::Activity),
            WarningOutcome::LimitExceeded
        );
    }

    // ---- clsnon ----

    #[test]
    fn close_nonroad_files_is_a_noop() {
        // Documented no-op; the call simply must compile and return.
        close_nonroad_files();
    }
}

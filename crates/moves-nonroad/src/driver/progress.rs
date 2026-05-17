//! Progress reporting — `dispit.f`, `mspinit.f`, `spinit.f`.
//!
//! Three tiny routines that the Fortran source uses to show progress
//! on the console:
//!
//! - `dispit.f` (50 lines) — counts processed records and computes a
//!   percent-complete figure. Ported as [`Progress`].
//! - `mspinit.f` (41 lines) — advances a 1-character "spinner"
//!   animation. Ported as [`Spinner`].
//! - `spinit.f` (38 lines) — every executable statement is commented
//!   out (`cgwilson` prefix); the routine is an inert no-op in the
//!   upstream and has nothing to port. The Rust port reproduces no
//!   dead code, consistent with the rest of the crate.
//!
//! # The terminal writes are intentionally dropped
//!
//! `dispit.f` and `mspinit.f` both end in a `write` to the console
//! unit (`IOWSTD`) — and in `dispit.f`'s case even that write is
//! commented out (`CDFK` prefix). Per the I/O policy in
//! `ARCHITECTURE.md` § 4.3, library code performs no console I/O: a
//! caller that wants a progress display reads [`Progress::percent`]
//! / [`Spinner::current`] and renders them itself. The Rust port
//! therefore keeps the *state* the two routines maintain (record
//! count, spinner frame) and the *value* they compute, and leaves
//! rendering to the caller.

/// Record-processing progress counter — `dispit.f`.
///
/// `dispit.f` maintains two `nonrdio.inc` COMMON-block integers,
/// `nrecds` (records processed so far) and `ntotrc` (total records
/// expected), and on each call bumps `nrecds` and recomputes a
/// percent-complete figure. [`Progress`] owns the same two counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Progress {
    /// Records processed so far — Fortran `nrecds`.
    records_done: i32,
    /// Total records expected — Fortran `ntotrc`.
    records_total: i32,
}

impl Progress {
    /// Create a counter for a run of `total` records.
    ///
    /// Mirrors `nonroad.f` :84 (`nrecds = 0`) plus the `ntotrc`
    /// established once the population records are counted.
    pub fn new(total: i32) -> Self {
        Progress {
            records_done: 0,
            records_total: total,
        }
    }

    /// Register one processed record and return the new
    /// percent-complete — the `dispit.f` call.
    ///
    /// `dispit.f` :28–33: `nrecds = nrecds + 1`, then
    /// `ipct = INT(100.0 * FLOAT(nrecds) / FLOAT(ntotrc))` when
    /// `ntotrc > 0`, else `ipct = 100`.
    pub fn tick(&mut self) -> i32 {
        self.records_done += 1;
        self.percent()
    }

    /// Percent complete, `0..=100`-ish — the value `dispit.f`
    /// computes for the (commented-out) console write.
    ///
    /// Returns `100` when the total is non-positive, matching
    /// `dispit.f` :29–33. The `INT()` truncation toward zero is
    /// reproduced by the `as i32` cast.
    pub fn percent(&self) -> i32 {
        if self.records_total > 0 {
            (100.0_f32 * self.records_done as f32 / self.records_total as f32) as i32
        } else {
            100
        }
    }

    /// Records processed so far (`nrecds`).
    pub fn records_done(&self) -> i32 {
        self.records_done
    }

    /// Total records expected (`ntotrc`).
    pub fn records_total(&self) -> i32 {
        self.records_total
    }

    /// Set the processed-record count directly.
    ///
    /// `nonroad.f` :339 assigns `nrecds = ntotrc - 1` just before the
    /// final `dispit` so the closing display reads 100%. This is the
    /// Rust equivalent of that direct COMMON-block write.
    pub fn set_records_done(&mut self, done: i32) {
        self.records_done = done;
    }
}

/// The 12 spinner frames initialised in `nonroad.f` :72–83.
///
/// `nonroad.f` fills `spin(0..11)` with the repeating cycle
/// `| | / - - \`. `mspinit.f` only ever indexes `spin(0..7)` (its
/// counter is taken mod 8), so the last four frames are unused by the
/// animation; the full table is preserved here for fidelity with the
/// source.
pub const SPIN_FRAMES: [char; 12] = ['|', '|', '/', '-', '-', '\\', '|', '|', '/', '-', '-', '\\'];

/// Number of distinct spinner frames `mspinit.f` cycles through.
///
/// `mspinit.f` :23: `icall = MOD(icall+1, 8)`.
const SPIN_CYCLE: usize = 8;

/// Console "spinner" animation state — `mspinit.f`.
///
/// `mspinit.f` keeps a single `nonrdio.inc` COMMON integer `icall`
/// and, on each call, advances it `icall = MOD(icall+1, 8)` and
/// writes `spin(icall)`. [`Spinner`] owns `icall` as its private
/// `frame` field and exposes the current frame character; see the
/// module docs for why the write itself is dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Spinner {
    /// Current frame index — Fortran `icall`, always `0..SPIN_CYCLE`.
    frame: usize,
}

impl Spinner {
    /// Create a spinner at frame 0.
    ///
    /// Matches `icall`'s zero-initialised COMMON-block state before
    /// the first `mspinit` call.
    pub fn new() -> Self {
        Spinner::default()
    }

    /// Advance the spinner one frame and return the new frame
    /// character — the `mspinit.f` call.
    ///
    /// `mspinit.f` :23: `icall = MOD(icall + 1, 8)`.
    pub fn advance(&mut self) -> char {
        self.frame = (self.frame + 1) % SPIN_CYCLE;
        self.current()
    }

    /// The current frame character without advancing.
    pub fn current(&self) -> char {
        SPIN_FRAMES[self.frame]
    }

    /// The current frame index (`icall`).
    pub fn frame(&self) -> usize {
        self.frame
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_increments_and_reports_percent() {
        let mut p = Progress::new(4);
        assert_eq!(p.tick(), 25); // 1 / 4
        assert_eq!(p.tick(), 50); // 2 / 4
        assert_eq!(p.tick(), 75); // 3 / 4
        assert_eq!(p.tick(), 100); // 4 / 4
        assert_eq!(p.records_done(), 4);
    }

    #[test]
    fn percent_truncates_toward_zero() {
        // 1 / 3 = 33.33% ⇒ INT() ⇒ 33.
        let mut p = Progress::new(3);
        assert_eq!(p.tick(), 33);
        // 2 / 3 = 66.66% ⇒ 66.
        assert_eq!(p.tick(), 66);
    }

    #[test]
    fn percent_is_100_when_total_non_positive() {
        let mut zero = Progress::new(0);
        assert_eq!(zero.percent(), 100);
        assert_eq!(zero.tick(), 100);

        let negative = Progress::new(-5);
        assert_eq!(negative.percent(), 100);
    }

    #[test]
    fn percent_can_exceed_100_when_overshooting() {
        // The Fortran formula is not clamped; a record count above the
        // total yields > 100. Reproduce that rather than clamping.
        let mut p = Progress::new(2);
        p.tick();
        p.tick();
        assert_eq!(p.tick(), 150); // 3 / 2
    }

    #[test]
    fn set_records_done_matches_the_final_dispit_pattern() {
        // nonroad.f :339–340: nrecds = ntotrc - 1; dispit() ⇒ 100%.
        let mut p = Progress::new(100);
        p.set_records_done(p.records_total() - 1);
        assert_eq!(p.tick(), 100);
        assert_eq!(p.records_done(), 100);
    }

    #[test]
    fn spinner_starts_at_frame_zero() {
        let s = Spinner::new();
        assert_eq!(s.frame(), 0);
        assert_eq!(s.current(), '|');
    }

    #[test]
    fn spinner_advances_through_the_eight_frame_cycle() {
        let mut s = Spinner::new();
        // mspinit: icall = MOD(icall+1, 8) ⇒ first call lands on 1.
        let seen: Vec<char> = (0..8).map(|_| s.advance()).collect();
        assert_eq!(seen, vec!['|', '/', '-', '-', '\\', '|', '|', '|']);
    }

    #[test]
    fn spinner_wraps_at_the_cycle_length() {
        let mut s = Spinner::new();
        for _ in 0..SPIN_CYCLE {
            s.advance();
        }
        // After a full cycle of 8 advances the frame returns to 0.
        assert_eq!(s.frame(), 0);
    }
}

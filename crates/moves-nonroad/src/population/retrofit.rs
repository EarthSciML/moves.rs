//! Retrofit-population helpers — `cmprrtrft.f`, `srtrtrft.f`,
//! `swaprtrft.f`, `rtrftengovrlp.f`, `initrtrft.f`.
//!
//! These routines underpin the retrofit input pipeline:
//!
//! - [`init_retrofit_state`] (`initrtrft.f`) — set up the empty
//!   retrofit-arrays state with the four valid pollutants
//!   (`HC`/`CO`/`NOX`/`PM`) and their index map.
//! - [`compare_retrofits`] (`cmprrtrft.f`) — three-way comparison
//!   used by sorting, with two comparison modes.
//! - [`swap_retrofits`] (`swaprtrft.f`) — in-place swap of two
//!   retrofit slots (in Rust this collapses to [`<[T]>::swap`], but
//!   we expose a typed wrapper for the Fortran cross-reference).
//! - [`sort_retrofits`] (`srtrtrft.f`) — quicksort variant that
//!   orders retrofit records via [`compare_retrofits`].
//! - [`engine_overlap`] (`rtrftengovrlp.f`) — predicate on two
//!   retrofit records: do their engine sets intersect?
//!
//! # State shape
//!
//! The Fortran source pre-dimensions parallel arrays in
//! `nonrdrtrft.inc` (one array per field, sized `MXRTRFT = 500`).
//! Per the array-size policy (`ARCHITECTURE.md` § 4.1), the Rust
//! port uses a `Vec<RetrofitRecord>` instead — but the comparison
//! and overlap functions take 0-based indices into a slice to
//! mirror the Fortran-call-site flavor (callers manipulate
//! "records `a` and `b`" by position).

use crate::common::consts::{IDXCO, IDXNOX, IDXPM, IDXTHC, MXPOL};

/// Sentinel for "all SCCs" in a retrofit record's SCC field.
///
/// Fortran parameter: `RTRFTSCCALL = 'ALL'` in `nonrdrtrft.inc`.
pub const RTRFTSCC_ALL: &str = "ALL";

/// Sentinel for "all tech types" in a retrofit record's tech-type
/// field.
///
/// Fortran parameter: `RTRFTTCHTYPALL = 'ALL'` in `nonrdrtrft.inc`.
pub const RTRFTTECHTYPE_ALL: &str = "ALL";

/// One retrofit record.
///
/// Replaces the parallel arrays `rtrftrec`, `rtrftryst`, …,
/// `rtrftid` from `nonrdrtrft.inc`. The Fortran source uses
/// 1-based indices throughout; the Rust port stores zero-based
/// `record` numbers and exposes them via `record_index`.
#[derive(Debug, Clone, PartialEq)]
pub struct RetrofitRecord {
    /// 0-based record number from the input file (`rtrftrec`).
    pub record_index: usize,
    /// Retrofit ID (same number for sibling rows that affect
    /// different pollutants of one retrofit; `rtrftid`).
    pub id: i32,
    /// First calendar year of retrofitting (`rtrftryst`).
    pub year_retrofit_start: i32,
    /// Last calendar year of retrofitting (`rtrftryen`).
    pub year_retrofit_end: i32,
    /// First model year affected (`rtrftmyst`).
    pub year_model_start: i32,
    /// Last model year affected (`rtrftmyen`).
    pub year_model_end: i32,
    /// SCC code or [`RTRFTSCC_ALL`] (`rtrftscc`).
    pub scc: String,
    /// Tech type or [`RTRFTTECHTYPE_ALL`] (`rtrfttechtype`).
    pub tech_type: String,
    /// Minimum HP, non-inclusive (`rtrfthpmn`).
    pub hp_min: f32,
    /// Maximum HP, inclusive (`rtrfthpmx`).
    pub hp_max: f32,
    /// Annual retrofit fraction OR total retrofitted count
    /// (`rtrftannualfracorn`). Values `> 1.0` are interpreted as
    /// absolute counts; `0.0..=1.0` as a fraction.
    pub annual_frac_or_count: f32,
    /// Retrofit effectiveness, `0.0..=1.0` (`rtrfteffect`).
    pub effectiveness: f32,
    /// Pollutant name (`HC` / `CO` / `NOX` / `PM`;
    /// `rtrftpollutant`).
    pub pollutant: String,
    /// Index into the main pollutant table for the named pollutant
    /// (`rtrftplltntidx`). Use [`RetrofitPollutant::for_name`] to
    /// derive this from `pollutant`.
    pub pollutant_idx: i32,
}

/// Comparison mode for [`compare_retrofits`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Comparison {
    /// Compare retrofit ID, then pollutant, then record number.
    /// Used by callers that need a stable ordering keyed on
    /// retrofit identity (`cmprrtrft.f` :69–102).
    IdPollutantRecord,
    /// Compare model year end, then maximum HP.
    /// Used by `fndrtrft` — an optimization there depends on
    /// model-year-end being the first criterion (`cmprrtrft.f`
    /// :108–112). Do not reorder.
    ModelYearMaxHp,
}

/// One of the four pollutants that may appear in a retrofit record.
///
/// Mirrors the `rtrftplltnt` / `rtrftplltntidxmp` arrays populated
/// by `initrtrft.f` (HC=`IDXTHC=1`, CO=`IDXCO=2`, NOX=`IDXNOX=3`,
/// PM=`IDXPM=6`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetrofitPollutant {
    Hc,
    Co,
    Nox,
    Pm,
}

impl RetrofitPollutant {
    /// Look up a pollutant by its 10-char field value (case-
    /// insensitive, trimmed). Returns `None` for unknown
    /// pollutants.
    pub fn for_name(name: &str) -> Option<Self> {
        match name.trim().to_ascii_uppercase().as_str() {
            "HC" => Some(Self::Hc),
            "CO" => Some(Self::Co),
            "NOX" => Some(Self::Nox),
            "PM" => Some(Self::Pm),
            _ => None,
        }
    }

    /// Canonical 10-character field value
    /// (matches the strings stored in `rtrftplltnt`).
    pub fn canonical_name(self) -> &'static str {
        match self {
            Self::Hc => "HC",
            Self::Co => "CO",
            Self::Nox => "NOX",
            Self::Pm => "PM",
        }
    }

    /// 1-based pollutant-array index, matching `rtrftplltntidxmp`
    /// (`initrtrft.f` :64–67).
    pub fn pollutant_index(self) -> i32 {
        match self {
            Self::Hc => IDXTHC as i32,
            Self::Co => IDXCO as i32,
            Self::Nox => IDXNOX as i32,
            Self::Pm => IDXPM as i32,
        }
    }
}

/// Retrofit runtime state — `initrtrft.f` initializer output.
///
/// Holds the empty/zeroed retrofit data and the lookup tables
/// (`rtrftplltnt`, `rtrftplltntidxmp`, `rtrftplltntrdfrc`).
#[derive(Debug, Clone)]
pub struct RetrofitState {
    /// Retrofit records, populated by `rdrtrft.f` (Task 98).
    pub records: Vec<RetrofitRecord>,
    /// Pollutants valid in retrofit records, in `rtrftplltnt` order
    /// (HC, CO, NOX, PM).
    pub valid_pollutants: [RetrofitPollutant; 4],
    /// Pollutant reduction fractions for the current iteration
    /// (`rtrftplltntrdfrc`). Indexed by pollutant index; sized to
    /// [`MXPOL`].
    pub pollutant_reduction_fraction: Vec<f32>,
}

/// `initrtrft.f` — produce a freshly-initialized retrofit state.
///
/// Mirrors the Fortran subroutine: empties the retrofit record
/// arrays, populates the valid-pollutant lookup, and zeroes the
/// pollutant-reduction-fraction accumulator.
pub fn init_retrofit_state() -> RetrofitState {
    RetrofitState {
        records: Vec::new(),
        valid_pollutants: [
            RetrofitPollutant::Hc,
            RetrofitPollutant::Co,
            RetrofitPollutant::Nox,
            RetrofitPollutant::Pm,
        ],
        pollutant_reduction_fraction: vec![0.0; MXPOL],
    }
}

/// `cmprrtrft.f` — three-way comparison of two retrofit records.
///
/// Returns negative if `records[a] < records[b]`, zero if equal,
/// positive if greater, under the requested [`Comparison`] mode.
/// Panics if `a` or `b` is out of bounds.
pub fn compare_retrofits(
    records: &[RetrofitRecord],
    mode: Comparison,
    a: usize,
    b: usize,
) -> i32 {
    let ra = &records[a];
    let rb = &records[b];
    match mode {
        Comparison::IdPollutantRecord => {
            if ra.id < rb.id {
                -1
            } else if ra.id > rb.id {
                1
            } else if ra.pollutant_idx < rb.pollutant_idx {
                -1
            } else if ra.pollutant_idx > rb.pollutant_idx {
                1
            } else if ra.record_index < rb.record_index {
                -1
            } else if ra.record_index > rb.record_index {
                1
            } else {
                0
            }
        }
        Comparison::ModelYearMaxHp => {
            if ra.year_model_end < rb.year_model_end {
                -1
            } else if ra.year_model_end > rb.year_model_end {
                1
            } else if ra.hp_max < rb.hp_max {
                -1
            } else if ra.hp_max > rb.hp_max {
                1
            } else {
                0
            }
        }
    }
}

/// `swaprtrft.f` — swap retrofit slots `a` and `b`.
///
/// The Fortran subroutine spells out the per-field swap because it
/// operates on parallel arrays; in Rust the typed wrapper around
/// [`<[T]>::swap`] preserves the cross-reference flavor while
/// being a one-liner under the hood.
#[inline]
pub fn swap_retrofits(records: &mut [RetrofitRecord], a: usize, b: usize) {
    records.swap(a, b);
}

/// `srtrtrft.f` — quicksort `records[start..=stop]` by
/// [`compare_retrofits`] under `mode`.
///
/// Bounds are inclusive on both ends, matching the Fortran call
/// convention. Calling with `start > stop` or with an empty range
/// is a no-op.
///
/// # Algorithm fidelity
///
/// Reproduces the Lomuto-style partition the Fortran source uses
/// (`srtrtrft.f` :65–98): pick the leftmost element as pivot, scan
/// inward from both ends, swap out-of-order pairs, recurse on the
/// two partitions. This preserves the *exact* element ordering the
/// Fortran code produces on equal-key records (which matters when
/// the secondary criterion in [`Comparison::IdPollutantRecord`] is
/// the record number — equal keys keep their input order).
pub fn sort_retrofits(
    records: &mut [RetrofitRecord],
    mode: Comparison,
    start: usize,
    stop: usize,
) {
    if stop <= start {
        return;
    }
    if stop - start > 1 {
        let mut l = start + 1;
        let mut r = stop;

        while l < r {
            while l <= stop && compare_retrofits(records, mode, l, start) <= 0 {
                l += 1;
            }
            while r > start && compare_retrofits(records, mode, r, start) >= 0 {
                r -= 1;
            }

            if l < r {
                swap_retrofits(records, l, r);
            } else {
                swap_retrofits(records, start, r);
            }
        }

        // Recurse on the two partitions. The pivot lives at `r`
        // after partitioning; the Fortran source guards `r - 1`
        // and `r + 1` against the partition bounds via the
        // outer `stop - start > 1` check, so we mirror that.
        if r > start {
            sort_retrofits(records, mode, start, r - 1);
        }
        sort_retrofits(records, mode, r + 1, stop);
    } else if stop - start == 1 {
        if compare_retrofits(records, mode, start, stop) > 0 {
            swap_retrofits(records, stop, start);
        }
    }
}

/// `rtrftengovrlp.f` — do retrofits `a` and `b` affect overlapping
/// engine sets?
///
/// Two retrofit records' engine sets overlap iff *all three* of
/// these conditions hold:
///
/// 1. SCC ranges overlap (per the all-inclusive / 4-digit-global /
///    7-digit-global / exact matching rules in `rtrftengovrlp.f`
///    :60–80);
/// 2. tech-type ranges overlap (either is `ALL`, or they match);
/// 3. HP ranges overlap (`hp_min` is non-inclusive, so `hp_max(a)
///    <= hp_min(b)` is *not* an overlap).
pub fn engine_overlap(records: &[RetrofitRecord], a: usize, b: usize) -> bool {
    let ra = &records[a];
    let rb = &records[b];

    // --- SCC overlap (rtrftengovrlp.f :60–80) ---
    if ra.scc != RTRFTSCC_ALL && rb.scc != RTRFTSCC_ALL && ra.scc != rb.scc {
        let a_4global = scc_segment(&ra.scc, 5, 10) == Some("000000");
        let b_4global = scc_segment(&rb.scc, 5, 10) == Some("000000");
        if a_4global || b_4global {
            if scc_segment(&ra.scc, 1, 4) != scc_segment(&rb.scc, 1, 4) {
                return false;
            }
        } else {
            let a_7global = scc_segment(&ra.scc, 8, 10) == Some("000");
            let b_7global = scc_segment(&rb.scc, 8, 10) == Some("000");
            if a_7global || b_7global {
                if scc_segment(&ra.scc, 1, 7) != scc_segment(&rb.scc, 1, 7) {
                    return false;
                }
            } else {
                return false;
            }
        }
    }

    // --- tech-type overlap (rtrftengovrlp.f :84–89) ---
    if ra.tech_type != RTRFTTECHTYPE_ALL
        && rb.tech_type != RTRFTTECHTYPE_ALL
        && ra.tech_type != rb.tech_type
    {
        return false;
    }

    // --- HP overlap (rtrftengovrlp.f :93–96): hp_min is non-inclusive,
    //     so equality at the boundary does not count as overlap ---
    if ra.hp_max <= rb.hp_min || rb.hp_max <= ra.hp_min {
        return false;
    }

    true
}

/// Slice `s` by 1-based inclusive column range, returning `None`
/// if the slice would go past `s`'s end.
fn scc_segment(s: &str, start_1based: usize, end_1based: usize) -> Option<&str> {
    let start = start_1based.checked_sub(1)?;
    if end_1based < start_1based || end_1based > s.len() {
        return None;
    }
    Some(&s[start..end_1based])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(id: i32, pollutant_idx: i32, record_index: usize) -> RetrofitRecord {
        RetrofitRecord {
            record_index,
            id,
            year_retrofit_start: 2020,
            year_retrofit_end: 2030,
            year_model_start: 2000,
            year_model_end: 2010,
            scc: "ALL".to_string(),
            tech_type: "ALL".to_string(),
            hp_min: 0.0,
            hp_max: 100.0,
            annual_frac_or_count: 0.5,
            effectiveness: 0.5,
            pollutant: "HC".to_string(),
            pollutant_idx,
        }
    }

    fn rec_my(myend: i32, hpmax: f32) -> RetrofitRecord {
        let mut r = rec(0, 1, 0);
        r.year_model_end = myend;
        r.hp_max = hpmax;
        r
    }

    #[test]
    fn init_state_has_four_pollutants_and_zeroed_arrays() {
        let s = init_retrofit_state();
        assert!(s.records.is_empty());
        assert_eq!(s.valid_pollutants.len(), 4);
        assert_eq!(s.valid_pollutants[0], RetrofitPollutant::Hc);
        assert_eq!(s.valid_pollutants[3], RetrofitPollutant::Pm);
        assert_eq!(s.pollutant_reduction_fraction.len(), MXPOL);
        assert!(s.pollutant_reduction_fraction.iter().all(|&v| v == 0.0));
    }

    #[test]
    fn pollutant_lookup_and_indices() {
        assert_eq!(RetrofitPollutant::for_name("HC"), Some(RetrofitPollutant::Hc));
        assert_eq!(RetrofitPollutant::for_name("Co"), Some(RetrofitPollutant::Co));
        assert_eq!(RetrofitPollutant::for_name("nox"), Some(RetrofitPollutant::Nox));
        assert_eq!(RetrofitPollutant::for_name("PM "), Some(RetrofitPollutant::Pm));
        assert_eq!(RetrofitPollutant::for_name("CH4"), None);
        // pollutant indices match the Fortran IDXTHC/CO/NOX/PM mapping
        assert_eq!(RetrofitPollutant::Hc.pollutant_index(), 1);
        assert_eq!(RetrofitPollutant::Co.pollutant_index(), 2);
        assert_eq!(RetrofitPollutant::Nox.pollutant_index(), 3);
        assert_eq!(RetrofitPollutant::Pm.pollutant_index(), 6);
    }

    #[test]
    fn compare_id_pollutant_record_ordering() {
        let recs = vec![rec(1, 2, 5), rec(1, 2, 5)];
        assert_eq!(compare_retrofits(&recs, Comparison::IdPollutantRecord, 0, 1), 0);

        let recs = vec![rec(1, 2, 5), rec(2, 1, 0)];
        assert_eq!(compare_retrofits(&recs, Comparison::IdPollutantRecord, 0, 1), -1);
        assert_eq!(compare_retrofits(&recs, Comparison::IdPollutantRecord, 1, 0), 1);

        let recs = vec![rec(1, 1, 0), rec(1, 2, 0)];
        assert_eq!(compare_retrofits(&recs, Comparison::IdPollutantRecord, 0, 1), -1);

        let recs = vec![rec(1, 1, 3), rec(1, 1, 5)];
        assert_eq!(compare_retrofits(&recs, Comparison::IdPollutantRecord, 0, 1), -1);
    }

    #[test]
    fn compare_modelyear_maxhp() {
        let recs = vec![rec_my(2010, 50.0), rec_my(2010, 50.0)];
        assert_eq!(compare_retrofits(&recs, Comparison::ModelYearMaxHp, 0, 1), 0);

        let recs = vec![rec_my(2005, 100.0), rec_my(2010, 50.0)];
        assert_eq!(compare_retrofits(&recs, Comparison::ModelYearMaxHp, 0, 1), -1);

        let recs = vec![rec_my(2010, 25.0), rec_my(2010, 75.0)];
        assert_eq!(compare_retrofits(&recs, Comparison::ModelYearMaxHp, 0, 1), -1);
        assert_eq!(compare_retrofits(&recs, Comparison::ModelYearMaxHp, 1, 0), 1);
    }

    #[test]
    fn swap_swaps_two_slots() {
        let mut recs = vec![rec(1, 1, 0), rec(2, 2, 1)];
        swap_retrofits(&mut recs, 0, 1);
        assert_eq!(recs[0].id, 2);
        assert_eq!(recs[1].id, 1);
    }

    #[test]
    fn sort_orders_records_by_model_year_then_hp() {
        let mut recs = vec![
            rec_my(2015, 200.0),
            rec_my(2010, 100.0),
            rec_my(2010, 50.0),
            rec_my(2020, 25.0),
        ];
        let stop = recs.len() - 1;
        sort_retrofits(&mut recs, Comparison::ModelYearMaxHp, 0, stop);
        assert_eq!((recs[0].year_model_end, recs[0].hp_max), (2010, 50.0));
        assert_eq!((recs[1].year_model_end, recs[1].hp_max), (2010, 100.0));
        assert_eq!((recs[2].year_model_end, recs[2].hp_max), (2015, 200.0));
        assert_eq!((recs[3].year_model_end, recs[3].hp_max), (2020, 25.0));
    }

    #[test]
    fn sort_orders_by_id_pollutant_record() {
        let mut recs = vec![
            rec(2, 1, 0),
            rec(1, 2, 3),
            rec(1, 1, 1),
            rec(1, 1, 0),
        ];
        let stop = recs.len() - 1;
        sort_retrofits(&mut recs, Comparison::IdPollutantRecord, 0, stop);
        assert_eq!((recs[0].id, recs[0].pollutant_idx, recs[0].record_index), (1, 1, 0));
        assert_eq!((recs[1].id, recs[1].pollutant_idx, recs[1].record_index), (1, 1, 1));
        assert_eq!((recs[2].id, recs[2].pollutant_idx, recs[2].record_index), (1, 2, 3));
        assert_eq!((recs[3].id, recs[3].pollutant_idx, recs[3].record_index), (2, 1, 0));
    }

    #[test]
    fn sort_is_noop_on_empty_or_single() {
        let mut recs: Vec<RetrofitRecord> = vec![];
        sort_retrofits(&mut recs, Comparison::IdPollutantRecord, 0, 0);
        assert!(recs.is_empty());

        let mut recs = vec![rec(1, 1, 0)];
        // length 1 → stop = 0, start = 0, stop - start = 0 → no-op
        sort_retrofits(&mut recs, Comparison::IdPollutantRecord, 0, 0);
        assert_eq!(recs.len(), 1);
    }

    fn overlap_rec(scc: &str, tech: &str, hpmin: f32, hpmax: f32) -> RetrofitRecord {
        let mut r = rec(0, 1, 0);
        r.scc = scc.to_string();
        r.tech_type = tech.to_string();
        r.hp_min = hpmin;
        r.hp_max = hpmax;
        r
    }

    #[test]
    fn overlap_all_sccs_and_all_techs_overlaps_when_hp_overlaps() {
        let recs = vec![
            overlap_rec("ALL", "ALL", 0.0, 100.0),
            overlap_rec("ALL", "ALL", 50.0, 200.0),
        ];
        assert!(engine_overlap(&recs, 0, 1));
    }

    #[test]
    fn overlap_no_when_hp_disjoint() {
        let recs = vec![
            overlap_rec("ALL", "ALL", 0.0, 50.0),
            overlap_rec("ALL", "ALL", 50.0, 100.0),
        ];
        // hp_max(a) == hp_min(b) → non-inclusive → no overlap
        assert!(!engine_overlap(&recs, 0, 1));
    }

    #[test]
    fn overlap_no_when_tech_differs() {
        let recs = vec![
            overlap_rec("ALL", "BASE", 0.0, 100.0),
            overlap_rec("ALL", "T2", 0.0, 100.0),
        ];
        assert!(!engine_overlap(&recs, 0, 1));
    }

    #[test]
    fn overlap_yes_when_one_tech_is_all() {
        let recs = vec![
            overlap_rec("ALL", "ALL", 0.0, 100.0),
            overlap_rec("ALL", "T2", 0.0, 100.0),
        ];
        assert!(engine_overlap(&recs, 0, 1));
    }

    #[test]
    fn overlap_no_when_scc_differs_with_no_global() {
        let recs = vec![
            overlap_rec("2270002003", "ALL", 0.0, 100.0),
            overlap_rec("2270002004", "ALL", 0.0, 100.0),
        ];
        assert!(!engine_overlap(&recs, 0, 1));
    }

    #[test]
    fn overlap_yes_when_one_scc_is_4digit_global() {
        // 2270000000 = 4-digit global for "22700"-family
        let recs = vec![
            overlap_rec("2270000000", "ALL", 0.0, 100.0),
            overlap_rec("2270002003", "ALL", 0.0, 100.0),
        ];
        assert!(engine_overlap(&recs, 0, 1));
    }

    #[test]
    fn overlap_no_when_4digit_global_does_not_share_prefix() {
        let recs = vec![
            overlap_rec("2270000000", "ALL", 0.0, 100.0),
            overlap_rec("2265002003", "ALL", 0.0, 100.0),
        ];
        assert!(!engine_overlap(&recs, 0, 1));
    }

    #[test]
    fn overlap_yes_when_one_scc_is_7digit_global() {
        // 2270002000 = 7-digit global for "2270002"-family
        let recs = vec![
            overlap_rec("2270002000", "ALL", 0.0, 100.0),
            overlap_rec("2270002003", "ALL", 0.0, 100.0),
        ];
        assert!(engine_overlap(&recs, 0, 1));
    }

    #[test]
    fn overlap_no_when_7digit_global_does_not_share_prefix() {
        let recs = vec![
            overlap_rec("2270002000", "ALL", 0.0, 100.0),
            overlap_rec("2270003003", "ALL", 0.0, 100.0),
        ];
        assert!(!engine_overlap(&recs, 0, 1));
    }

    #[test]
    fn overlap_yes_when_both_sccs_are_all() {
        let recs = vec![
            overlap_rec("ALL", "ALL", 0.0, 100.0),
            overlap_rec("ALL", "ALL", 0.0, 100.0),
        ];
        assert!(engine_overlap(&recs, 0, 1));
    }
}

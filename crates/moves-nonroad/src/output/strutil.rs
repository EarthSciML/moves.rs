//! String utilities and small helpers (Task 102).
//!
//! Ports `strlen.f`, `strmin.f`, `lftjst.f`, `rgtjst.f`, `low2up.f`,
//! `chrsrt.f`, `wadeeq.f`, and `cnthpcat.f`. Most of the string
//! routines reduce to one- or two-line Rust functions that wrap
//! `&str` slice methods.
//!
//! The Fortran originals operate on fixed-width blank-padded
//! `character*(*)` variables. The Rust ports take `&str` and treat
//! the slice's byte length as the Fortran "declared length"; when
//! the original mutated the buffer in place to shift content while
//! preserving width, the Rust port returns an owned `String` of the
//! same byte length.
//!
//! `wadeeq` and `cnthpcat` are not strictly string utilities — the
//! first is an evaporative-emissions formula and the second a
//! horsepower-category counter — but the migration plan groups them
//! here as miscellaneous small helpers (Task 102). Both are
//! self-contained enough to live alongside the string routines
//! rather than carry a one-function module of their own.

/// Length of `s` with no trailing ASCII blanks. All-blank or empty
/// input returns 0.
///
/// Ports `strlen.f`. The Fortran routine takes a fixed-width
/// `character*(*)` and scans from the right end for the last
/// non-blank byte. The Rust port treats the slice's byte length as
/// the declared length and only matches the ASCII space `' '` (the
/// Fortran routine's `BLANK` parameter is the same character).
pub fn strlen(s: &str) -> usize {
    s.trim_end_matches(' ').len()
}

/// Length of `s` with no trailing ASCII blanks, with a minimum of 1.
/// All-blank or empty input returns 1.
///
/// Ports `strmin.f`. Identical to [`strlen`] except that it never
/// returns zero — useful when a caller relies on `&s[..n]` or
/// `s(1:n)` being a non-empty slice.
pub fn strmin(s: &str) -> usize {
    let n = strlen(s);
    if n == 0 {
        1
    } else {
        n
    }
}

/// Removes leading ASCII blanks from `s`, padding the right with
/// blanks to keep the original byte length.
///
/// Ports `lftjst.f`. The Fortran routine mutates a fixed-width
/// buffer in place; the Rust port returns a new owned `String` of
/// the same byte length as `s`, with non-blank content shifted to
/// the start and trailing blanks filling the original width.
/// Internal blanks are preserved (the Fortran loop only re-positions
/// the leading-blank prefix).
pub fn lftjst(s: &str) -> String {
    let total = s.len();
    let trimmed = s.trim_start_matches(' ');
    format!("{:<width$}", trimmed, width = total)
}

/// Removes trailing ASCII blanks from `s`, padding the left with
/// blanks to keep the original byte length.
///
/// Ports `rgtjst.f`. The Fortran routine mutates a fixed-width
/// buffer in place; the Rust port returns a new owned `String` of
/// the same byte length as `s`, with non-blank content shifted to
/// the end and leading blanks filling the original width. Internal
/// blanks are preserved.
pub fn rgtjst(s: &str) -> String {
    let total = s.len();
    let trimmed = s.trim_end_matches(' ');
    format!("{:>width$}", trimmed, width = total)
}

/// Returns `s` with ASCII lowercase letters (`a..=z`) converted to
/// uppercase (`A..=Z`). Non-letter and non-ASCII bytes pass through
/// unchanged.
///
/// Ports `low2up.f` with a deliberate deviation. The Fortran routine
/// has an off-by-one bug — its boundary check is
/// `idiff .GE. 0 .AND. idiff .LE. 26`, which catches one byte past
/// `'z'` (ASCII `'{'` at code 123) and incorrectly maps it to
/// `'A' + 26` (ASCII `'['` at code 91). The bug is latent because
/// NONROAD inputs never contain `'{'`, but the Rust port restricts
/// to the canonical `a..=z` range via [`char::to_ascii_uppercase`]
/// rather than reproducing the bug.
pub fn low2up(s: &str) -> String {
    s.to_ascii_uppercase()
}

/// Sorts an array of strings lexicographically by their first
/// `key_len` bytes and returns the permutation of indices that
/// produces sorted order.
///
/// Ports `chrsrt.f`. The Fortran routine fills `itglst` with 1-based
/// indices into `array`; the Rust port returns 0-based indices, so
/// callers apply the result as `array[indices[i]]`. The sort is
/// stable: equal keys retain their input order, which matches the
/// behavior of the Fortran insertion-sort tiebreaker (it iterates
/// from `ncount-1` down to 1 and only displaces the sorted prefix
/// on a strict-greater-than comparison).
///
/// `key_len` must be a valid byte boundary in every element of
/// `array`. NONROAD inputs are ASCII so byte and char boundaries
/// coincide; mixed UTF-8 input panics like ordinary slice indexing.
pub fn chrsrt(array: &[&str], key_len: usize) -> Vec<usize> {
    let mut indices: Vec<usize> = (0..array.len()).collect();
    indices.sort_by(|&a, &b| array[a][..key_len].cmp(&array[b][..key_len]));
    indices
}

/// Vapor generation in g/day from the WADE evaporative-loss
/// equation.
///
/// Ports `wadeeq.f`. Despite its placement in this module, this is a
/// numerical formula, not a string utility. Inputs:
///
/// * `frc_ful` — fraction of tank full of fuel (0..=1)
/// * `tnk_siz` — tank size, gallons
/// * `rvp` — fuel Reid Vapor Pressure, psi
/// * `min_tmp` — minimum ambient temperature, °F
/// * `max_tmp` — maximum ambient temperature, °F
///
/// Returns 0.0 when `max_tmp <= min_tmp`. The 2005-07-18 EPA fix
/// that nudges the maximum temperature by a 0.992 factor —
/// `adj_max = (max_tmp - min_tmp) * 0.992 + min_tmp` — to avoid
/// near-zero floating-point artifacts when the two temperatures are
/// nearly equal is preserved. Float precision matches the Fortran
/// `REAL` type (`f32`).
pub fn wadeeq(frc_ful: f32, tnk_siz: f32, rvp: f32, min_tmp: f32, max_tmp: f32) -> f32 {
    let vap_spc = ((1.0 - frc_ful) * tnk_siz + 0.15 * tnk_siz) / 7.841;

    if max_tmp > min_tmp {
        let adj_max = (max_tmp - min_tmp) * 0.992 + min_tmp;

        let vap_prs = 1.0223 * rvp + (0.0119 * 3.0 * rvp) / (1.0 - 0.0368 * rvp);

        let pct_evp = 66.401 - 12.718 * vap_prs + 1.3067 * vap_prs.powi(2)
            - 0.077934 * vap_prs.powi(3)
            + 0.0018407 * vap_prs.powi(4);

        let d_min_tp = pct_evp + ((262.0 / ((pct_evp / 6.0) + 560.0)) - 0.0113) * (100.0 - min_tmp);

        let d_max_tp = pct_evp + ((262.0 / ((pct_evp / 6.0) + 560.0)) - 0.0113) * (100.0 - adj_max);

        let int_prs = 14.697 - 0.53089 * d_min_tp + 0.0077215 * d_min_tp.powi(2)
            - 0.000055631 * d_min_tp.powi(3)
            + 0.0000001769 * d_min_tp.powi(4);

        let fin_prs = 14.697 - 0.53089 * d_max_tp + 0.0077215 * d_max_tp.powi(2)
            - 0.000055631 * d_max_tp.powi(3)
            + 0.0000001769 * d_max_tp.powi(4);

        let fl_dens = 6.386 - 0.0186 * rvp;

        let mol_wt = (73.23 - 1.274 * rvp) + (((min_tmp + adj_max) / 2.0) - 60.0) * 0.059;

        vap_spc
            * 454.0
            * fl_dens
            * (520.0 / (690.0 - 4.0 * mol_wt))
            * ((int_prs / (14.7 - int_prs) + fin_prs / (14.7 - fin_prs)) / 2.0)
            * (((14.7 - int_prs) / (min_tmp + 460.0)) - ((14.7 - fin_prs) / (adj_max + 460.0)))
    } else {
        0.0
    }
}

/// Counts the number of horsepower categories spanned by the range
/// `[hpmn, hpmx]`.
///
/// Ports `cnthpcat.f`. `hpmn` and `hpmx` must each be either `0.0`,
/// `9999.0`, or one of the values present in `hpclev` (the
/// hard-coded HP-level table from `nonrdeqp.inc`). The function
/// returns 0 if either bound is non-zero, non-9999, and not present
/// in `hpclev`.
///
/// The Fortran original reads `hpclev` and `MXHPC` from the
/// `nonrdeqp.inc` COMMON block and delegates the table search to
/// `fndhpc` (Task 101). The Rust port takes `hpclev` as an explicit
/// slice parameter, which avoids dragging in either Task 101's
/// lookup module or Task 92's `NonroadContext` for what is ~30
/// lines of category arithmetic. The lookup is inlined here as a
/// linear scan, matching the Fortran `fndhpc` algorithm.
///
/// Floating-point equality on `hpmn`, `hpmx`, and `hpclev` entries
/// is intentional: per the upstream comments, all three come from
/// hard-coded values or input files that contain exact decimals
/// from a fixed list, not values computed at runtime.
pub fn cnthpcat(hpmn: f32, hpmx: f32, hpclev: &[f32]) -> i32 {
    let mxhpc = hpclev.len() as i32;

    let hpmn_idx = match resolve_hp_index(hpmn, hpclev, mxhpc) {
        Some(idx) => idx,
        None => return 0,
    };

    let hpmx_idx = match resolve_hp_index(hpmx, hpclev, mxhpc) {
        Some(idx) => idx,
        None => return 0,
    };

    hpmx_idx - hpmn_idx
}

fn resolve_hp_index(hp: f32, hpclev: &[f32], mxhpc: i32) -> Option<i32> {
    if hp == 0.0 {
        Some(0)
    } else if hp == 9999.0 {
        Some(mxhpc + 1)
    } else {
        hpclev.iter().position(|&x| x == hp).map(|i| (i as i32) + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strlen_trims_trailing_blanks() {
        assert_eq!(strlen("abc"), 3);
        assert_eq!(strlen("abc   "), 3);
        assert_eq!(strlen("a b c   "), 5);
        assert_eq!(strlen("   abc"), 6); // leading blanks counted
        assert_eq!(strlen(""), 0);
        assert_eq!(strlen("     "), 0);
    }

    #[test]
    fn strmin_returns_at_least_one() {
        assert_eq!(strmin("abc"), 3);
        assert_eq!(strmin("abc   "), 3);
        assert_eq!(strmin(""), 1);
        assert_eq!(strmin("     "), 1);
    }

    #[test]
    fn lftjst_preserves_total_width() {
        assert_eq!(lftjst("  ab  "), "ab    ");
        assert_eq!(lftjst("  a   b"), "a   b  ");
        assert_eq!(lftjst("abc"), "abc"); // already left-justified
        assert_eq!(lftjst("     "), "     "); // all blank — unchanged
        assert_eq!(lftjst(""), "");
    }

    #[test]
    fn rgtjst_preserves_total_width() {
        assert_eq!(rgtjst("ab    "), "    ab");
        assert_eq!(rgtjst("a   b  "), "  a   b");
        assert_eq!(rgtjst("   abc"), "   abc"); // already right-justified
        assert_eq!(rgtjst("     "), "     "); // all blank — unchanged
        assert_eq!(rgtjst(""), "");
    }

    #[test]
    fn low2up_converts_ascii_letters() {
        assert_eq!(low2up("hello WORLD 42"), "HELLO WORLD 42");
        assert_eq!(low2up(""), "");
        assert_eq!(low2up("MixedCase"), "MIXEDCASE");
    }

    #[test]
    fn low2up_does_not_replicate_fortran_off_by_one() {
        // Fortran low2up.f maps '{' (123) to '[' (91) due to its
        // `idiff .LE. 26` bound. The Rust port leaves '{' alone.
        assert_eq!(low2up("{"), "{");
        assert_eq!(low2up("`"), "`"); // one byte before 'a'; both leave alone
    }

    #[test]
    fn chrsrt_returns_stable_sorted_indices() {
        let arr = ["BB", "AA", "CC", "AA"];
        let order = chrsrt(&arr, 2);
        // Sorted: AA(idx 1), AA(idx 3), BB(idx 0), CC(idx 2)
        assert_eq!(order, vec![1, 3, 0, 2]);

        let sorted: Vec<&str> = order.iter().map(|&i| arr[i]).collect();
        assert_eq!(sorted, vec!["AA", "AA", "BB", "CC"]);
    }

    #[test]
    fn chrsrt_uses_first_key_len_bytes_only() {
        let arr = ["AAxx", "AByy", "AAzz"];
        let order = chrsrt(&arr, 2);
        // Sort by first 2: AA(0), AA(2), AB(1) — stable so 0 before 2.
        assert_eq!(order, vec![0, 2, 1]);
    }

    #[test]
    fn chrsrt_single_and_empty() {
        assert_eq!(chrsrt(&["only"], 4), vec![0]);
        let empty: Vec<&str> = vec![];
        let order = chrsrt(&empty, 0);
        assert!(order.is_empty());
    }

    #[test]
    fn wadeeq_zero_when_max_not_above_min() {
        assert_eq!(wadeeq(0.5, 10.0, 9.0, 70.0, 70.0), 0.0);
        assert_eq!(wadeeq(0.5, 10.0, 9.0, 70.0, 60.0), 0.0);
    }

    #[test]
    fn wadeeq_positive_for_warming_day() {
        // Representative 60→90°F day, half-full 10-gal tank, RVP=9
        // psi. Exact value is what the Fortran formula evaluates to;
        // we just check sign and a sanity bound.
        let g = wadeeq(0.5, 10.0, 9.0, 60.0, 90.0);
        assert!(g > 0.0, "expected positive vapor generation, got {g}");
        assert!(g.is_finite());
    }

    #[test]
    fn wadeeq_matches_known_inputs() {
        // Reproduces the formula by hand for a fixed input so a
        // future refactor cannot silently drift. Inputs:
        // frc_ful=0.5, tnk_siz=10, rvp=9, min=60, max=90.
        let frc_ful: f32 = 0.5;
        let tnk_siz: f32 = 10.0;
        let rvp: f32 = 9.0;
        let min_tmp: f32 = 60.0;
        let max_tmp: f32 = 90.0;

        let vap_spc = ((1.0 - frc_ful) * tnk_siz + 0.15 * tnk_siz) / 7.841_f32;
        let adj_max = (max_tmp - min_tmp) * 0.992 + min_tmp;
        let vap_prs = 1.0223 * rvp + (0.0119 * 3.0 * rvp) / (1.0 - 0.0368 * rvp);
        let pct_evp = 66.401 - 12.718 * vap_prs + 1.3067 * vap_prs.powi(2)
            - 0.077934 * vap_prs.powi(3)
            + 0.0018407 * vap_prs.powi(4);
        let d_min_tp = pct_evp + ((262.0 / ((pct_evp / 6.0) + 560.0)) - 0.0113) * (100.0 - min_tmp);
        let d_max_tp = pct_evp + ((262.0 / ((pct_evp / 6.0) + 560.0)) - 0.0113) * (100.0 - adj_max);
        let int_prs = 14.697 - 0.53089 * d_min_tp + 0.0077215 * d_min_tp.powi(2)
            - 0.000055631 * d_min_tp.powi(3)
            + 0.0000001769 * d_min_tp.powi(4);
        let fin_prs = 14.697 - 0.53089 * d_max_tp + 0.0077215 * d_max_tp.powi(2)
            - 0.000055631 * d_max_tp.powi(3)
            + 0.0000001769 * d_max_tp.powi(4);
        let fl_dens = 6.386 - 0.0186 * rvp;
        let mol_wt = (73.23 - 1.274 * rvp) + (((min_tmp + adj_max) / 2.0) - 60.0) * 0.059;
        let expected = vap_spc
            * 454.0
            * fl_dens
            * (520.0 / (690.0 - 4.0 * mol_wt))
            * ((int_prs / (14.7 - int_prs) + fin_prs / (14.7 - fin_prs)) / 2.0)
            * (((14.7 - int_prs) / (min_tmp + 460.0)) - ((14.7 - fin_prs) / (adj_max + 460.0)));

        let actual = wadeeq(frc_ful, tnk_siz, rvp, min_tmp, max_tmp);
        assert_eq!(actual, expected);
    }

    fn sample_hpclev() -> Vec<f32> {
        // Three boundaries → four implied categories. Realistic
        // NONROAD has 18 entries; three is enough to exercise the
        // index arithmetic.
        vec![10.0, 25.0, 50.0]
    }

    #[test]
    fn cnthpcat_full_range() {
        let hpclev = sample_hpclev();
        // 0 → 9999 spans all four categories: 0+1+2+3+(MXHPC+1=4) − 0 = 4
        assert_eq!(cnthpcat(0.0, 9999.0, &hpclev), 4);
    }

    #[test]
    fn cnthpcat_inner_range() {
        let hpclev = sample_hpclev();
        // 10 → 50 spans two categories (10–25 and 25–50).
        assert_eq!(cnthpcat(10.0, 50.0, &hpclev), 2);
        // 25 → 50 spans one category.
        assert_eq!(cnthpcat(25.0, 50.0, &hpclev), 1);
    }

    #[test]
    fn cnthpcat_open_lower() {
        let hpclev = sample_hpclev();
        // 0 → 25 spans two categories: below-10, 10–25.
        assert_eq!(cnthpcat(0.0, 25.0, &hpclev), 2);
    }

    #[test]
    fn cnthpcat_open_upper() {
        let hpclev = sample_hpclev();
        // 50 → 9999 spans one category: above-50.
        assert_eq!(cnthpcat(50.0, 9999.0, &hpclev), 1);
    }

    #[test]
    fn cnthpcat_unknown_value_returns_zero() {
        let hpclev = sample_hpclev();
        // 30 is not in hpclev → 0.
        assert_eq!(cnthpcat(30.0, 50.0, &hpclev), 0);
        assert_eq!(cnthpcat(10.0, 30.0, &hpclev), 0);
    }
}

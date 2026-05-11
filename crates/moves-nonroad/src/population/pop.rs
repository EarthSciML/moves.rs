//! Population apportionment — `getpop.f` (285 lines).
//!
//! Selects population records for the current SCC and target year
//! from a sorted population list. The Fortran source streams
//! through a sorted scratch file (`spopfl`, produced by `rdpop.f`)
//! and stops when the SCC changes; the Rust port operates on an
//! in-memory [`Vec<PopulationRecord>`] sorted by the same key.
//!
//! # Sort-key contract
//!
//! Input records must be sorted by:
//!
//! 1. SCC code (10 chars, lexicographic);
//! 2. HP-average (rounded to nearest integer per `rdpop.f` :248 —
//!    `nint(hpavg)` written as `I5`);
//! 3. FIPS region (5 chars);
//! 4. Subregion (5 chars);
//! 5. Year (integer, ascending).
//!
//! This is the key produced by `rdpop.f`'s sort step. The selection
//! algorithm relies on records with the same `(scc, hp_avg_int,
//! region, subregion)` being contiguous and year-ascending — same
//! as the Fortran loop assumes after reading from `IOSPOP`.
//!
//! # Selection algorithm (matches `getpop.f`)
//!
//! For each `(region, subregion, hp_avg_int)` group within the SCC:
//!
//! - Keep overwriting the slot with each record whose year is `<=
//!   target_year` — this leaves the latest pre-target record.
//! - When the first year `> target_year` is seen:
//!   - If the slot already holds `year == target_year`, drop the
//!     newer record;
//!   - else, push a *new* slot for that newer year (so callers get
//!     two records straddling the target).
//! - Any further years `> target_year` are skipped (the `lupper`
//!   latch in `getpop.f` :155).
//!
//! Records for SCCs other than the requested one are ignored.

use crate::input::pop::PopulationRecord;

/// One selected population slot.
///
/// Mirrors the per-slot state populated by `getpop.f` into the
/// `popeqp`, `avghpc`, `ipopyr`, `usehrs`, `hprang`, `discod`, and
/// `regncd` arrays.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectedPopulation {
    /// FIPS region (5 chars, left-justified).
    pub fips: String,
    /// Subregion (5 chars, upper-cased per `getpop.f` :139).
    pub subregion: String,
    /// Year of this population record.
    pub year: i32,
    /// HP-category midpoint.
    pub hp_avg: f32,
    /// HP-category lower bound.
    pub hp_min: f32,
    /// HP-category upper bound.
    pub hp_max: f32,
    /// Usage factor (annual hours).
    pub usage: f32,
    /// Technology/distribution code (10 chars, left-justified,
    /// upper-cased per `getpop.f` :141).
    pub tech_code: String,
    /// Equipment population.
    pub population: f64,
}

/// Select population records for `scc` and `target_year` from a
/// pre-sorted list. See module-level docs for the sort-key contract
/// and selection algorithm.
///
/// Returns an empty `Vec` if no records match the SCC. The result
/// preserves input order (so consumers that want them grouped by
/// region+HP can rely on it).
pub fn select_for_scc(
    records: &[PopulationRecord],
    scc: &str,
    target_year: i32,
) -> Vec<SelectedPopulation> {
    let mut out: Vec<SelectedPopulation> = Vec::new();
    let mut group_region: Option<String> = None;
    let mut group_hp_int: Option<i32> = None;
    let mut group_locked = false;

    for record in records {
        if record.scc.trim() != scc.trim() {
            // Records outside our SCC are skipped — caller-supplied
            // ordering ensures matching records are contiguous, but
            // we don't assume; we just filter.
            continue;
        }

        let region = format!("{:<5}{:<5}", record.fips, record.subregion.to_ascii_uppercase());
        let hp_int = (record.hp_avg).round() as i32;

        let same_group = group_region.as_deref() == Some(region.as_str())
            && group_hp_int == Some(hp_int)
            && !out.is_empty();

        if !same_group {
            // New group: emit the record, reset the latch.
            group_region = Some(region);
            group_hp_int = Some(hp_int);
            group_locked = record.year >= target_year;
            out.push(slot_from(record));
            continue;
        }

        if group_locked {
            // Already captured a year >= target_year for this group.
            continue;
        }

        if record.year > target_year {
            // Either we already have an exact-year slot (skip the
            // newer record) or we push a new slot for the post-year
            // bracket and lock the latch.
            let last = out.last().expect("same_group implies out is non-empty");
            if last.year == target_year {
                group_locked = true;
                continue;
            }
            out.push(slot_from(record));
            group_locked = true;
        } else {
            // year <= target_year: overwrite the current slot. The
            // Fortran `goto 111` after the lupper block falls through
            // to load the same `npoprc` index.
            let last = out.last_mut().expect("same_group implies out is non-empty");
            *last = slot_from(record);
        }
    }

    out
}

fn slot_from(r: &PopulationRecord) -> SelectedPopulation {
    SelectedPopulation {
        fips: r.fips.clone(),
        subregion: r.subregion.to_ascii_uppercase(),
        year: r.year,
        hp_avg: r.hp_avg,
        hp_min: r.hp_min,
        hp_max: r.hp_max,
        usage: r.usage,
        tech_code: r.tech_code.to_ascii_uppercase(),
        population: r.population,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(scc: &str, fips: &str, sub: &str, hp: f32, year: i32, pop: f64) -> PopulationRecord {
        PopulationRecord {
            fips: fips.to_string(),
            subregion: sub.to_string(),
            year,
            scc: scc.to_string(),
            hp_min: 0.0,
            hp_max: 25.0,
            hp_avg: hp,
            usage: 100.0,
            tech_code: "BASE".to_string(),
            population: pop,
        }
    }

    #[test]
    fn picks_latest_year_le_target_when_no_exact_match() {
        // Sorted by (scc, hp, fips, sub, year). Same group at
        // (SCC1, hp=11, 06000, 00000). Years 2015, 2018, 2025.
        // Target 2020 → keep 2018 (latest <=), then add 2025 (next >).
        let records = vec![
            rec("SCC1", "06000", "00000", 11.0, 2015, 100.0),
            rec("SCC1", "06000", "00000", 11.0, 2018, 200.0),
            rec("SCC1", "06000", "00000", 11.0, 2025, 300.0),
        ];
        let out = select_for_scc(&records, "SCC1", 2020);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].year, 2018);
        assert_eq!(out[0].population, 200.0);
        assert_eq!(out[1].year, 2025);
        assert_eq!(out[1].population, 300.0);
    }

    #[test]
    fn picks_exact_year_and_drops_post_target() {
        let records = vec![
            rec("SCC1", "06000", "00000", 11.0, 2015, 100.0),
            rec("SCC1", "06000", "00000", 11.0, 2020, 250.0),
            rec("SCC1", "06000", "00000", 11.0, 2025, 300.0),
        ];
        let out = select_for_scc(&records, "SCC1", 2020);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].year, 2020);
        assert_eq!(out[0].population, 250.0);
    }

    #[test]
    fn keeps_first_post_target_when_no_pre_target_data() {
        // Only post-target records exist for the group: the first
        // becomes the slot, the next is dropped (lupper latch).
        let records = vec![
            rec("SCC1", "06000", "00000", 11.0, 2025, 300.0),
            rec("SCC1", "06000", "00000", 11.0, 2030, 400.0),
        ];
        let out = select_for_scc(&records, "SCC1", 2020);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].year, 2025);
    }

    #[test]
    fn separate_groups_by_region_and_hp() {
        let records = vec![
            rec("SCC1", "06000", "00000", 11.0, 2018, 100.0),
            rec("SCC1", "06000", "00000", 11.0, 2025, 200.0),
            rec("SCC1", "06000", "00000", 50.0, 2018, 300.0),
            rec("SCC1", "17031", "00000", 11.0, 2018, 400.0),
        ];
        let out = select_for_scc(&records, "SCC1", 2020);
        assert_eq!(out.len(), 4);
        assert_eq!(out[0].fips, "06000");
        assert_eq!(out[0].hp_avg, 11.0);
        assert_eq!(out[0].year, 2018);
        assert_eq!(out[1].year, 2025);
        assert_eq!(out[2].fips, "06000");
        assert_eq!(out[2].hp_avg, 50.0);
        assert_eq!(out[3].fips, "17031");
    }

    #[test]
    fn hp_grouping_uses_rounded_int() {
        // hp_avg 11.4 and 10.7 both round to 11 (nint) and 11.5 rounds to 12.
        // f32 banker's rounding nuance: use values away from .5.
        let records = vec![
            rec("SCC1", "06000", "00000", 10.7, 2018, 100.0),
            rec("SCC1", "06000", "00000", 11.4, 2025, 200.0),
        ];
        let out = select_for_scc(&records, "SCC1", 2020);
        // Both round to 11 → same group. Latest pre is 2018, post is 2025.
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].year, 2018);
        assert_eq!(out[1].year, 2025);
    }

    #[test]
    fn filters_out_other_sccs() {
        let records = vec![
            rec("SCC1", "06000", "00000", 11.0, 2020, 100.0),
            rec("SCC2", "06000", "00000", 11.0, 2020, 200.0),
            rec("SCC1", "06000", "00000", 50.0, 2020, 300.0),
        ];
        let out = select_for_scc(&records, "SCC1", 2020);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].population, 100.0);
        assert_eq!(out[1].population, 300.0);
    }

    #[test]
    fn empty_input_returns_empty() {
        let out = select_for_scc(&[], "SCC1", 2020);
        assert!(out.is_empty());
    }

    #[test]
    fn subregion_is_upper_cased() {
        let records = vec![rec("SCC1", "06000", "abcde", 11.0, 2020, 100.0)];
        let out = select_for_scc(&records, "SCC1", 2020);
        assert_eq!(out[0].subregion, "ABCDE");
    }
}

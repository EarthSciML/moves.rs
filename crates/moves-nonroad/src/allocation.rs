//! County, state-to-county, and subcounty allocation logic.
//!
//! Cluster 5 of the seven functional groups (see
//! `ARCHITECTURE.md` § 2.5). Distributes a parent geography's
//! population across its child geographies using regression
//! coefficients from `.ALO` records ([`crate::input::alo`]) and
//! spatial-indicator data from `.IND` files
//! ([`crate::input::indicator`]).
//!
//! # Fortran source files this module ports
//!
//! | File | Lines | Role | Rust wrapper |
//! |---|---|---|---|
//! | `alocty.f` | 181 | State → county allocation        | [`allocate_state_to_counties`] |
//! | `alosta.f` | 176 | Nation → state allocation        | [`allocate_nation_to_states`] |
//! | `alosub.f` | 170 | County → subcounty allocation    | [`allocate_county_to_subcounty`] |
//!
//! All three routines share the same body: for the parent geography,
//! look up an indicator value per allocation code; for each child,
//! look up the indicator values and form
//!
//! ```text
//! ratio = Σ (child[i] / parent[i]) × coeff[i]   for slots with parent[i] > 0
//! child_population = parent_population × ratio
//! ```
//!
//! In Rust the three Fortran routines become one general
//! [`allocate`] plus three thin wrappers that document intent at
//! call sites. The shared math is in [`apportionment_ratio`].
//!
//! # `getind` semantics
//!
//! [`IndicatorTable::lookup`] mirrors the closest-prior-year search
//! in `getind.f`: among records matching `(code, fips, subregion)`,
//! return the value for the latest year ≤ target; if none, the
//! earliest year > target; if no records match at all, `None`.
//! [`IndicatorTable::lookup_required`] raises [`Error::IndicatorNotFound`]
//! on a miss, matching the `IEOF` error path that `alocty.f` / `alosta.f`
//! / `alosub.f` treat as fatal at label `7002`.
//!
//! # Subregion handling
//!
//! Fortran encodes "no subregion" as a 5-blank field. The Rust port
//! trims subregion values (both at index time and at query time) so
//! a query with `subregion=""` matches table entries whose
//! `subcounty` field is blank, and a non-empty `subregion` matches
//! the corresponding non-empty entry.
//!
//! # Numerical fidelity
//!
//! The Fortran source carries all values in `real*4` and computes
//! the ratio in `real*4` arithmetic. The Rust port reads indicator
//! values as `f64` (`IndicatorRecord::value`) and coefficients as
//! `f32` (`AllocationRecord::coefficients`); the per-slot
//! `child / parent` division and the accumulated `valalo` are
//! computed in `f64`, then cast back to `f32` for the population
//! output. This widens the intermediate precision relative to the
//! Fortran source — a known direction-of-difference that surfaces
//! (if anywhere) in Task 115 characterization runs and is triaged
//! under Task 116.

use std::collections::HashMap;

use crate::input::indicator::IndicatorRecord;
use crate::{Error, Result};

/// Maximum allocation coefficients per SCC. Matches `MXCOEF = 3` in
/// `nonrdalo.inc` and re-exports the same value as
/// [`crate::input::alo::MAX_COEF`].
pub const MAX_COEF: usize = crate::input::alo::MAX_COEF;

// ============================================================
// IndicatorTable — `getind` lookup over parsed indicator records
// ============================================================

/// Indexed view of a set of parsed [`IndicatorRecord`] values
/// supporting the `getind.f` closest-prior-year lookup.
///
/// Built once from a flat record list and queried many times during
/// the allocation pass. The table groups records by
/// `(code, fips, subregion)` and keeps each group sorted by year so
/// lookups are `O(log n)` per query rather than the linear file
/// scan-with-rewind in the Fortran source.
///
/// Codes and FIPS strings are stored verbatim; subregion is trimmed
/// (see module-level docs).
#[derive(Debug, Default, Clone)]
pub struct IndicatorTable {
    /// `(code, fips, subregion-trimmed) -> (year, value)` ascending in year.
    entries: HashMap<(String, String, String), Vec<(i32, f64)>>,
}

impl IndicatorTable {
    /// Create an empty table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a table from a flat list of [`IndicatorRecord`] values.
    ///
    /// Duplicate `(code, fips, subregion, year)` tuples retain the
    /// last value seen — same behaviour as the Fortran sorted-scan,
    /// which keeps marching past earlier records of the same key.
    pub fn from_records<I>(records: I) -> Self
    where
        I: IntoIterator<Item = IndicatorRecord>,
    {
        let mut table = Self::new();
        for record in records {
            table.insert(record);
        }
        table.sort();
        table
    }

    /// Insert a single record. After bulk inserts call [`Self::sort`]
    /// once before issuing lookups.
    pub fn insert(&mut self, record: IndicatorRecord) {
        let year: i32 = match record.year.trim().parse() {
            Ok(y) => y,
            Err(_) => return,
        };
        let key = (
            record.code.trim().to_ascii_uppercase(),
            record.fips,
            record.subcounty.trim().to_string(),
        );
        let bucket = self.entries.entry(key).or_default();
        if let Some(slot) = bucket.iter_mut().find(|(y, _)| *y == year) {
            slot.1 = record.value;
        } else {
            bucket.push((year, record.value));
        }
    }

    /// Sort every per-key bucket by year (ascending). Idempotent.
    pub fn sort(&mut self) {
        for bucket in self.entries.values_mut() {
            bucket.sort_by_key(|(y, _)| *y);
        }
    }

    /// `getind`-style lookup: closest year ≤ `year` for the given
    /// `(code, fips, subregion)`. Falls back to the earliest year
    /// strictly greater than `year` if none qualify. Returns `None`
    /// if the key has no records at all (`IEOF` in the Fortran source).
    pub fn lookup(&self, code: &str, fips: &str, subregion: &str, year: i32) -> Option<f64> {
        let key = (
            code.trim().to_ascii_uppercase(),
            fips.trim().to_string(),
            subregion.trim().to_string(),
        );
        let bucket = self.entries.get(&key)?;
        if bucket.is_empty() {
            return None;
        }
        // Closest year ≤ target. Bucket is sorted ascending, so the
        // partition point gives the first year > target.
        let cutoff = bucket.partition_point(|(y, _)| *y <= year);
        if cutoff > 0 {
            // The record at cutoff-1 has the largest year ≤ target.
            Some(bucket[cutoff - 1].1)
        } else {
            // No year ≤ target; fall through to earliest year > target.
            Some(bucket[0].1)
        }
    }

    /// Lookup that converts the `None` case into [`Error::IndicatorNotFound`].
    /// Used by [`allocate`] for the parent / non-blank-coefficient
    /// slots that must resolve.
    pub fn lookup_required(
        &self,
        code: &str,
        fips: &str,
        subregion: &str,
        year: i32,
    ) -> Result<f64> {
        self.lookup(code, fips, subregion, year)
            .ok_or_else(|| Error::IndicatorNotFound {
                code: code.trim().to_ascii_uppercase(),
                fips: fips.trim().to_string(),
                subregion: subregion.trim().to_string(),
                year,
            })
    }

    /// Number of distinct `(code, fips, subregion)` keys.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True if the table has no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

// ============================================================
// Allocation primitive
// ============================================================

/// One child geography passed to [`allocate`].
///
/// `fips` is the 5-character FIPS code (state, county, etc.).
/// `subregion` is the 5-character subcounty key (blank for state /
/// county scope). `skip` indicates a child the caller wants
/// short-circuited to zero population without lookups — mirrors the
/// `.NOT. lfipcd(idxfip)` / `lctlev(idxfip)` / `lstacd` / `lstlev`
/// gate in the Fortran source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocationChild {
    /// FIPS code of the child geography.
    pub fips: String,
    /// Subcounty / subregion key. Empty string when not applicable.
    pub subregion: String,
    /// If `true`, the child's outputs are forced to zero and no
    /// indicator lookups are performed.
    pub skip: bool,
}

impl AllocationChild {
    /// Convenience constructor for a non-skipped child.
    pub fn new(fips: impl Into<String>, subregion: impl Into<String>) -> Self {
        Self {
            fips: fips.into(),
            subregion: subregion.into(),
            skip: false,
        }
    }

    /// Convenience constructor for a skipped child.
    pub fn skipped(fips: impl Into<String>, subregion: impl Into<String>) -> Self {
        Self {
            fips: fips.into(),
            subregion: subregion.into(),
            skip: true,
        }
    }
}

/// Per-child output produced by [`allocate`].
///
/// `population` is `parent_population × ratio`; `growth` carries the
/// caller-supplied growth factor through to the geography arrays
/// (the Fortran source's `grwcty(idxfip) = growth` assignment).
/// Skipped children get `population = 0`, `growth = 0`, and
/// `skipped = true`. (The Fortran source leaves `grwcty` untouched
/// for skipped counties, relying on the caller's pre-initialisation;
/// the Rust port zeroes it for clarity and flags the skip
/// explicitly so callers can re-impose their own initialisation if
/// they need to.)
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct ChildAllocation {
    /// Allocated population. `f32` to match the Fortran `real*4` array.
    pub population: f32,
    /// Growth factor carried through to this child.
    pub growth: f32,
    /// `true` if the child was skipped by the caller's predicate.
    pub skipped: bool,
}

/// Compute the apportionment ratio
///
/// ```text
/// ratio = Σ_i  (child[i] / parent[i]) × coeff[i]    for parent[i] > 0
/// ```
///
/// Slots where `parent[i] <= 0` contribute nothing — the Fortran
/// guard at label `40` (`alocty.f` :138, `alosta.f` :138,
/// `alosub.f` :128). All three input slices must have the same
/// length; that length is `≤ MAX_COEF`.
///
/// Pure function — exposed for testing.
pub fn apportionment_ratio(coefficients: &[f32], parent: &[f64], child: &[f64]) -> f64 {
    debug_assert_eq!(coefficients.len(), parent.len());
    debug_assert_eq!(coefficients.len(), child.len());

    let mut valalo: f64 = 0.0;
    for ((coeff, parent_v), child_v) in coefficients.iter().zip(parent).zip(child) {
        if *parent_v > 0.0 {
            valalo += (child_v / parent_v) * (*coeff as f64);
        }
    }
    valalo
}

/// Look up indicator values for the given codes at one
/// `(fips, subregion, year)` tuple. Returns one entry per code, in
/// the same order as `codes`.
fn lookup_indicators(
    table: &IndicatorTable,
    codes: &[String],
    fips: &str,
    subregion: &str,
    year: i32,
) -> Result<Vec<f64>> {
    let mut out = Vec::with_capacity(codes.len());
    for code in codes {
        let v = table.lookup_required(code, fips, subregion, year)?;
        out.push(v);
    }
    Ok(out)
}

/// Distribute a parent geography's population across child
/// geographies using indicator-based apportionment.
///
/// Generalises `alocty.f`, `alosta.f`, and `alosub.f`. The three
/// Fortran routines differ only in their input shape (children
/// supplied by index range, by state list, or as a single
/// subcounty); the Rust port factors that out and lets the caller
/// pass an arbitrary child list.
///
/// # Arguments
///
/// * `coefficients` — regression coefficients for one SCC (length
///   ≤ `MAX_COEF`).
/// * `indicator_codes` — same length as `coefficients`; each entry
///   is the 3-character allocation code (`POP`, `HHS`, etc.) used
///   for that slot. Non-empty by contract — the `.ALO` parser
///   already drops trailing blank slots so the lengths line up.
/// * `parent_fips`, `parent_subregion` — FIPS / subregion key for
///   the parent geography. The indicator lookups for the parent
///   yield the denominator in the apportionment ratio.
/// * `children` — list of child geographies to allocate to. Order
///   is preserved in the output.
/// * `parent_population` — total population at the parent level.
///   Multiplied by the per-child ratio to produce the per-child
///   population.
/// * `growth_factor` — growth factor stamped into every non-skipped
///   child's [`ChildAllocation::growth`] field. Skipped children get
///   zero (see [`ChildAllocation`]).
/// * `indicators` — indexed indicator data (typically built from a
///   parsed `.IND` file).
/// * `year` — evaluation year for the indicator lookup
///   (`iepyr` in the Fortran source).
///
/// # Errors
///
/// Returns [`Error::IndicatorNotFound`] if any required indicator
/// lookup misses — both the parent lookups (always required if the
/// coefficient list is non-empty) and the per-child lookups for
/// non-skipped children.
#[allow(clippy::too_many_arguments)]
pub fn allocate(
    coefficients: &[f32],
    indicator_codes: &[String],
    parent_fips: &str,
    parent_subregion: &str,
    children: &[AllocationChild],
    parent_population: f32,
    growth_factor: f32,
    indicators: &IndicatorTable,
    year: i32,
) -> Result<Vec<ChildAllocation>> {
    if coefficients.len() != indicator_codes.len() {
        return Err(Error::Config(format!(
            "allocation coefficient count ({}) != indicator-code count ({})",
            coefficients.len(),
            indicator_codes.len()
        )));
    }
    if coefficients.len() > MAX_COEF {
        return Err(Error::Config(format!(
            "allocation coefficient count {} exceeds MAX_COEF={}",
            coefficients.len(),
            MAX_COEF
        )));
    }

    let mut out = Vec::with_capacity(children.len());

    // Empty coefficient list -> no allocation possible. Mirrors
    // the Fortran "branch to label 111 on first blank slot" with
    // no slots filled: every child gets ratio = 0.
    if coefficients.is_empty() {
        for child in children {
            out.push(if child.skip {
                ChildAllocation::default().with_skipped()
            } else {
                ChildAllocation {
                    population: 0.0,
                    growth: growth_factor,
                    skipped: false,
                }
            });
        }
        return Ok(out);
    }

    // Look up parent (denominator) values once.
    let parent_vals = lookup_indicators(
        indicators,
        indicator_codes,
        parent_fips,
        parent_subregion,
        year,
    )?;

    let parent_pop_64 = parent_population as f64;

    for child in children {
        if child.skip {
            out.push(ChildAllocation::default().with_skipped());
            continue;
        }

        let child_vals =
            lookup_indicators(indicators, indicator_codes, &child.fips, &child.subregion, year)?;
        let ratio = apportionment_ratio(coefficients, &parent_vals, &child_vals);
        let population = (parent_pop_64 * ratio) as f32;
        out.push(ChildAllocation {
            population,
            growth: growth_factor,
            skipped: false,
        });
    }

    Ok(out)
}

impl ChildAllocation {
    fn with_skipped(mut self) -> Self {
        self.skipped = true;
        self
    }
}

// ============================================================
// Three thin Fortran-named wrappers
// ============================================================

/// Distribute a state's population across the counties in that
/// state. Ports `alocty.f` (`NONROAD/NR08a/SOURCE/alocty.f`).
///
/// `state_fips` is the 5-character FIPS for the state (its
/// indicator data is the denominator); `counties` lists the
/// counties to allocate to. Each county's subregion is blank
/// because state→county allocation operates at county granularity.
/// Set [`AllocationChild::skip`] to skip a county the caller has
/// already populated via county-specific data (the
/// `.NOT. lfipcd(idxfip)` / `lctlev(idxfip)` gates in the Fortran
/// source).
#[allow(clippy::too_many_arguments)]
pub fn allocate_state_to_counties(
    coefficients: &[f32],
    indicator_codes: &[String],
    state_fips: &str,
    counties: &[AllocationChild],
    state_population: f32,
    growth_factor: f32,
    indicators: &IndicatorTable,
    year: i32,
) -> Result<Vec<ChildAllocation>> {
    allocate(
        coefficients,
        indicator_codes,
        state_fips,
        "",
        counties,
        state_population,
        growth_factor,
        indicators,
        year,
    )
}

/// Distribute a national-level population across states.
/// Ports `alosta.f` (`NONROAD/NR08a/SOURCE/alosta.f`).
///
/// `nation_fips` is the FIPS key used for nation-level indicator
/// data (the Fortran source pulls it from `regncd(icurec)(1:5)` —
/// typically the all-zero sentinel `"00000"` for a national
/// record). The subregion is blank at both ends.
#[allow(clippy::too_many_arguments)]
pub fn allocate_nation_to_states(
    coefficients: &[f32],
    indicator_codes: &[String],
    nation_fips: &str,
    states: &[AllocationChild],
    nation_population: f32,
    growth_factor: f32,
    indicators: &IndicatorTable,
    year: i32,
) -> Result<Vec<ChildAllocation>> {
    allocate(
        coefficients,
        indicator_codes,
        nation_fips,
        "",
        states,
        nation_population,
        growth_factor,
        indicators,
        year,
    )
}

/// Distribute a county's population to a single subcounty.
/// Ports `alosub.f` (`NONROAD/NR08a/SOURCE/alosub.f`).
///
/// Equivalent to calling [`allocate`] with a one-element child list
/// whose `subregion` carries the subcounty key. Returns a scalar
/// for ergonomics at the call site — the Fortran source's `popsub`
/// / `grwsub` outputs are scalars too.
#[allow(clippy::too_many_arguments)]
pub fn allocate_county_to_subcounty(
    coefficients: &[f32],
    indicator_codes: &[String],
    county_fips: &str,
    subcounty: &str,
    county_population: f32,
    growth_factor: f32,
    indicators: &IndicatorTable,
    year: i32,
) -> Result<ChildAllocation> {
    let children = [AllocationChild::new(county_fips, subcounty)];
    let mut out = allocate(
        coefficients,
        indicator_codes,
        county_fips,
        "",
        &children,
        county_population,
        growth_factor,
        indicators,
        year,
    )?;
    Ok(out.pop().expect("allocate returns one entry per child"))
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(code: &str, fips: &str, sub: &str, year: &str, value: f64) -> IndicatorRecord {
        IndicatorRecord {
            code: code.to_string(),
            fips: fips.to_string(),
            subcounty: sub.to_string(),
            year: year.to_string(),
            value,
        }
    }

    // ------- IndicatorTable -------

    #[test]
    fn lookup_finds_exact_year() {
        let table = IndicatorTable::from_records(vec![
            rec("POP", "17031", "", "2020", 5_180_000.0),
            rec("POP", "17031", "", "2025", 5_200_000.0),
        ]);
        let v = table.lookup("POP", "17031", "", 2020).unwrap();
        assert!((v - 5_180_000.0).abs() < 1e-6);
    }

    #[test]
    fn lookup_uses_closest_prior_year() {
        // getind.f: closest year ≤ target.
        let table = IndicatorTable::from_records(vec![
            rec("POP", "17031", "", "2010", 5_000_000.0),
            rec("POP", "17031", "", "2020", 5_180_000.0),
            rec("POP", "17031", "", "2030", 5_500_000.0),
        ]);
        let v = table.lookup("POP", "17031", "", 2024).unwrap();
        assert!((v - 5_180_000.0).abs() < 1e-6);
    }

    #[test]
    fn lookup_falls_back_to_earliest_after_target() {
        // getind.f: when no year ≤ target, take the earliest year > target.
        let table = IndicatorTable::from_records(vec![
            rec("POP", "17031", "", "2020", 5_180_000.0),
            rec("POP", "17031", "", "2030", 5_500_000.0),
        ]);
        let v = table.lookup("POP", "17031", "", 2010).unwrap();
        assert!((v - 5_180_000.0).abs() < 1e-6);
    }

    #[test]
    fn lookup_missing_key_returns_none() {
        let table = IndicatorTable::from_records(vec![rec(
            "POP", "17031", "", "2020", 5_180_000.0,
        )]);
        assert!(table.lookup("HHS", "17031", "", 2020).is_none());
        assert!(table.lookup("POP", "99999", "", 2020).is_none());
        assert!(table.lookup("POP", "17031", "ABCDE", 2020).is_none());
    }

    #[test]
    fn lookup_required_errors_on_miss() {
        let table = IndicatorTable::new();
        let err = table
            .lookup_required("POP", "17031", "", 2020)
            .expect_err("empty table should miss");
        assert!(matches!(err, Error::IndicatorNotFound { .. }));
    }

    #[test]
    fn lookup_normalises_code_and_subregion() {
        let table = IndicatorTable::from_records(vec![rec(
            "POP", "17031", "00001", "2020", 100.0,
        )]);
        // Lowercase / padded query matches the stored record.
        let v = table.lookup("pop", "17031", "  00001 ", 2020).unwrap();
        assert!((v - 100.0).abs() < 1e-9);
    }

    #[test]
    fn lookup_empty_subregion_matches_empty_record() {
        // Fortran encodes state/county-scope queries with a 5-blank
        // subregion. After trimming, the Rust port keys on the
        // empty string both ways.
        let table = IndicatorTable::from_records(vec![rec(
            "POP", "17000", "", "2020", 6_000_000.0,
        )]);
        let v = table.lookup("POP", "17000", "", 2020).unwrap();
        assert!((v - 6_000_000.0).abs() < 1e-6);
        let v = table.lookup("POP", "17000", "     ", 2020).unwrap();
        assert!((v - 6_000_000.0).abs() < 1e-6);
    }

    #[test]
    fn insert_overwrites_duplicate_year() {
        let mut table = IndicatorTable::new();
        table.insert(rec("POP", "17031", "", "2020", 1.0));
        table.insert(rec("POP", "17031", "", "2020", 2.0));
        table.sort();
        let v = table.lookup("POP", "17031", "", 2020).unwrap();
        assert!((v - 2.0).abs() < 1e-9);
    }

    #[test]
    fn lookup_ignores_unparseable_year() {
        // Years that don't parse as integers are silently dropped;
        // surrounding records are unaffected.
        let table = IndicatorTable::from_records(vec![
            rec("POP", "17031", "", "BAD!", 999.0),
            rec("POP", "17031", "", "2020", 5_180_000.0),
        ]);
        let v = table.lookup("POP", "17031", "", 2020).unwrap();
        assert!((v - 5_180_000.0).abs() < 1e-6);
    }

    // ------- apportionment_ratio -------

    #[test]
    fn apportionment_ratio_single_slot() {
        // ratio = (child/parent) * coeff
        let r = apportionment_ratio(&[1.0], &[100.0], &[25.0]);
        assert!((r - 0.25).abs() < 1e-12);
    }

    #[test]
    fn apportionment_ratio_zero_parent_skipped() {
        // Parent zero -> slot contributes nothing (Fortran's guard).
        let r = apportionment_ratio(&[0.5, 0.5], &[0.0, 100.0], &[50.0, 50.0]);
        assert!((r - 0.25).abs() < 1e-12);
    }

    #[test]
    fn apportionment_ratio_negative_parent_skipped() {
        // Fortran uses `.GT. 0` — negatives also skipped.
        let r = apportionment_ratio(&[1.0], &[-1.0], &[50.0]);
        assert!(r.abs() < 1e-12);
    }

    #[test]
    fn apportionment_ratio_sums_three_slots() {
        // Even split: each slot 1/3, child uniformly half of parent.
        let coeffs = [1.0_f32 / 3.0, 1.0_f32 / 3.0, 1.0_f32 / 3.0];
        let parent = [100.0, 200.0, 400.0];
        let child = [50.0, 100.0, 200.0];
        let r = apportionment_ratio(&coeffs, &parent, &child);
        // (0.5 + 0.5 + 0.5) * (1/3) summed = 0.5. Tolerance reflects
        // the f32 representation of 1/3 (3 × 0.33333334 ≠ 1.0 exactly).
        assert!((r - 0.5).abs() < 1e-6, "got r = {r}");
    }

    // ------- allocate -------

    fn alo_table() -> IndicatorTable {
        // Two counties (17031, 17043) in state 17000; nation = 00000.
        IndicatorTable::from_records(vec![
            // State-level totals
            rec("POP", "17000", "", "2020", 12_000_000.0),
            rec("POP", "00000", "", "2020", 330_000_000.0),
            // Counties
            rec("POP", "17031", "", "2020", 5_180_000.0),
            rec("POP", "17043", "", "2020", 932_000.0),
            // Subcounty for 17031
            rec("POP", "17031", "TRACT1", "2020", 1_500_000.0),
        ])
    }

    #[test]
    fn allocate_state_to_counties_basic() {
        let table = alo_table();
        let counties = [
            AllocationChild::new("17031", ""),
            AllocationChild::new("17043", ""),
        ];
        let result = allocate_state_to_counties(
            &[1.0],
            &["POP".to_string()],
            "17000",
            &counties,
            1_000.0,
            1.5,
            &table,
            2020,
        )
        .unwrap();
        // 17031 share = 5_180_000 / 12_000_000 ≈ 0.43166...
        let expected_31 = 1_000.0_f64 * (5_180_000.0 / 12_000_000.0);
        let expected_43 = 1_000.0_f64 * (932_000.0 / 12_000_000.0);
        assert!((result[0].population as f64 - expected_31).abs() < 1e-2);
        assert!((result[1].population as f64 - expected_43).abs() < 1e-2);
        assert_eq!(result[0].growth, 1.5);
        assert_eq!(result[1].growth, 1.5);
        assert!(!result[0].skipped);
        assert!(!result[1].skipped);
    }

    #[test]
    fn allocate_skipped_children_get_zeros() {
        let table = alo_table();
        let counties = [
            AllocationChild::skipped("17031", ""),
            AllocationChild::new("17043", ""),
        ];
        let result = allocate_state_to_counties(
            &[1.0],
            &["POP".to_string()],
            "17000",
            &counties,
            1_000.0,
            1.5,
            &table,
            2020,
        )
        .unwrap();
        assert_eq!(result[0].population, 0.0);
        assert_eq!(result[0].growth, 0.0);
        assert!(result[0].skipped);
        assert!(result[1].population > 0.0);
        assert!(!result[1].skipped);
    }

    #[test]
    fn allocate_propagates_missing_parent_indicator() {
        let table = alo_table();
        let counties = [AllocationChild::new("17031", "")];
        // Parent FIPS 99999 has no POP data.
        let err = allocate_state_to_counties(
            &[1.0],
            &["POP".to_string()],
            "99999",
            &counties,
            1_000.0,
            1.0,
            &table,
            2020,
        )
        .expect_err("missing parent data should error");
        match err {
            Error::IndicatorNotFound { fips, .. } => assert_eq!(fips, "99999"),
            other => panic!("expected IndicatorNotFound, got {other:?}"),
        }
    }

    #[test]
    fn allocate_propagates_missing_child_indicator() {
        let table = alo_table();
        // Add a county that has no POP data.
        let counties = [
            AllocationChild::new("17031", ""),
            AllocationChild::new("99999", ""),
        ];
        let err = allocate_state_to_counties(
            &[1.0],
            &["POP".to_string()],
            "17000",
            &counties,
            1_000.0,
            1.0,
            &table,
            2020,
        )
        .expect_err("missing child data should error");
        assert!(matches!(err, Error::IndicatorNotFound { .. }));
    }

    #[test]
    fn allocate_skipped_child_does_not_trigger_lookup() {
        // Even though FIPS 99999 has no data, skipping it should not
        // raise IndicatorNotFound.
        let table = alo_table();
        let counties = [
            AllocationChild::new("17031", ""),
            AllocationChild::skipped("99999", ""),
        ];
        let result = allocate_state_to_counties(
            &[1.0],
            &["POP".to_string()],
            "17000",
            &counties,
            1_000.0,
            1.0,
            &table,
            2020,
        )
        .unwrap();
        assert!(!result[0].skipped);
        assert!(result[1].skipped);
        assert_eq!(result[1].population, 0.0);
    }

    #[test]
    fn allocate_empty_coefficients_returns_zeros() {
        let table = IndicatorTable::new();
        let counties = [AllocationChild::new("17031", "")];
        let result =
            allocate_state_to_counties(&[], &[], "17000", &counties, 1_000.0, 2.0, &table, 2020)
                .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].population, 0.0);
        assert_eq!(result[0].growth, 2.0);
        assert!(!result[0].skipped);
    }

    #[test]
    fn allocate_rejects_length_mismatch() {
        let table = IndicatorTable::new();
        let counties = [AllocationChild::new("17031", "")];
        let err = allocate_state_to_counties(
            &[1.0, 1.0],
            &["POP".to_string()],
            "17000",
            &counties,
            1_000.0,
            1.0,
            &table,
            2020,
        )
        .expect_err("length mismatch should error");
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn allocate_rejects_too_many_coefficients() {
        let table = IndicatorTable::new();
        let counties = [AllocationChild::new("17031", "")];
        let codes = vec!["POP".to_string(); 4];
        let err = allocate_state_to_counties(
            &[1.0; 4],
            &codes,
            "17000",
            &counties,
            1_000.0,
            1.0,
            &table,
            2020,
        )
        .expect_err("oversize coefficients should error");
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn allocate_nation_to_states_basic() {
        let table = alo_table();
        let states = [AllocationChild::new("17000", "")];
        let result = allocate_nation_to_states(
            &[1.0],
            &["POP".to_string()],
            "00000",
            &states,
            1_000_000.0,
            1.0,
            &table,
            2020,
        )
        .unwrap();
        let expected = 1_000_000.0_f64 * (12_000_000.0 / 330_000_000.0);
        assert!((result[0].population as f64 - expected).abs() < 5e-1);
    }

    #[test]
    fn allocate_county_to_subcounty_basic() {
        let table = alo_table();
        let result = allocate_county_to_subcounty(
            &[1.0],
            &["POP".to_string()],
            "17031",
            "TRACT1",
            1_000.0,
            1.25,
            &table,
            2020,
        )
        .unwrap();
        let expected = 1_000.0_f64 * (1_500_000.0 / 5_180_000.0);
        assert!((result.population as f64 - expected).abs() < 1e-2);
        assert_eq!(result.growth, 1.25);
        assert!(!result.skipped);
    }

    #[test]
    fn allocate_three_slot_weighted_blend() {
        // Two indicator codes weighted 0.6 / 0.4 over two counties.
        let table = IndicatorTable::from_records(vec![
            rec("POP", "17000", "", "2020", 10.0),
            rec("HHS", "17000", "", "2020", 20.0),
            rec("POP", "17031", "", "2020", 4.0),
            rec("HHS", "17031", "", "2020", 6.0),
            rec("POP", "17043", "", "2020", 6.0),
            rec("HHS", "17043", "", "2020", 14.0),
        ]);
        let counties = [
            AllocationChild::new("17031", ""),
            AllocationChild::new("17043", ""),
        ];
        let result = allocate_state_to_counties(
            &[0.6, 0.4],
            &["POP".to_string(), "HHS".to_string()],
            "17000",
            &counties,
            1_000.0,
            1.0,
            &table,
            2020,
        )
        .unwrap();
        // 17031: 0.6 * 4/10 + 0.4 * 6/20 = 0.24 + 0.12 = 0.36
        // 17043: 0.6 * 6/10 + 0.4 * 14/20 = 0.36 + 0.28 = 0.64
        // Population shares: 360, 640.
        let r0 = result[0].population as f64;
        let r1 = result[1].population as f64;
        assert!((r0 - 360.0).abs() < 1e-2);
        assert!((r1 - 640.0).abs() < 1e-2);
        // Two slots summing to 1.0 -> populations should add up to the parent.
        assert!((r0 + r1 - 1_000.0).abs() < 1e-2);
    }

    #[test]
    fn allocate_year_fallback_uses_closest_prior() {
        // Indicator data only at year 2015, asked for 2025 -> use 2015.
        let table = IndicatorTable::from_records(vec![
            rec("POP", "17000", "", "2015", 10.0),
            rec("POP", "17031", "", "2015", 4.0),
        ]);
        let counties = [AllocationChild::new("17031", "")];
        let result = allocate_state_to_counties(
            &[1.0],
            &["POP".to_string()],
            "17000",
            &counties,
            100.0,
            1.0,
            &table,
            2025,
        )
        .unwrap();
        // 4/10 * 100 = 40
        assert!((result[0].population as f64 - 40.0).abs() < 1e-3);
    }
}

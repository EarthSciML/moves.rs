//! County, state-to-county, and subcounty allocation logic.
//!
//! Cluster 5 of the seven functional groups (see
//! `ARCHITECTURE.md` § 2.5). Smallest calculation cluster; the
//! three routines share the parent/child ratio computation.
//!
//! # Fortran source files this module ports
//!
//! | File | Lines | Role |
//! |---|---|---|
//! | `alocty.f` | 181 | State-to-county allocation |
//! | `alosta.f` | 176 | Nation-to-state allocation |
//! | `alosub.f` | 170 | County-to-subcounty allocation |
//!
//! Each Fortran routine performs the same operation at a different
//! geography level: gather the parent geography's indicator values
//! for up to [`MAX_COEFFICIENTS`] regression terms, gather the same
//! indicators for each requested child geography, and apportion the
//! parent population to each child by
//! `child_population = parent_population * sum_i (child_i / parent_i) * coeff_i`
//! for each term where the parent indicator is positive.
//!
//! # Indicator lookup
//!
//! The Fortran routines call `getind` to look up an indicator value
//! given (code, FIPS, subregion, year). `getind` reads from a
//! sorted on-disk indicator file and applies a closest-year-≤-target
//! (fallback: earliest year > target) selection. The Rust port
//! abstracts this via the [`IndicatorLookup`] trait so production
//! code (Task 113 driver) plugs in a real implementation backed by
//! parsed `IndicatorRecord`s while unit tests use an in-memory
//! mock. [`SliceIndicatorLookup`] is the production-ready
//! implementation over a sorted slice.
//!
//! # The unused `luse` argument
//!
//! `alocty.f`/`alosta.f`/`alosub.f` declare a `logical*4 luse`
//! argument in their interfaces but never reference it inside the
//! body. The Rust port omits it.

use crate::input::alo::AllocationRecord;
use crate::input::indicator::IndicatorRecord;
use crate::{Error, Result};

/// Maximum number of (indicator_code, coefficient) terms per SCC,
/// matching `MXCOEF = 3` in `nonrdalo.inc`.
pub const MAX_COEFFICIENTS: usize = 3;

/// Width of FIPS strings used by the indicator file (5 characters).
const FIPS_WIDTH: usize = 5;

/// Width of subregion strings used by the indicator file (5 characters).
const SUBREGION_WIDTH: usize = 5;

/// Lookup of spatial-indicator values, abstracting `getind.f`.
///
/// Implementations select the closest year ≤ `year` for the given
/// (code, fips, subregion) triple, falling back to the earliest
/// year > `year` if no earlier records exist. Returns:
///
/// - `Ok(Some(v))` — a record matched and supplied value `v`.
/// - `Ok(None)` — no record exists for the (code, fips, subregion)
///   triple (matches the `IEOF` branch in `getind.f`).
/// - `Err(_)` — an I/O or parse error that should abort the
///   calculation (matches the `ISUCES`-not-returned branch).
pub trait IndicatorLookup {
    /// Look up an indicator value.
    fn lookup(&mut self, code: &str, fips: &str, subregion: &str, year: i32)
        -> Result<Option<f64>>;
}

/// In-memory [`IndicatorLookup`] backed by a sorted slice.
///
/// Records must be sorted by (code, fips, subregion, year) — the same
/// order `chrsrt` produces in the Fortran source (and what
/// [`sort_indicators`] yields). Each lookup performs a linear scan
/// constrained to records with the matching (code, fips, subregion).
///
/// [`sort_indicators`]: crate::input::indicator::sort_indicators
pub struct SliceIndicatorLookup<'a> {
    records: &'a [IndicatorRecord],
}

impl<'a> SliceIndicatorLookup<'a> {
    /// Create a lookup over the given sorted slice of indicator records.
    ///
    /// The caller is responsible for ensuring `records` is sorted
    /// (use [`sort_indicators`]).
    ///
    /// [`sort_indicators`]: crate::input::indicator::sort_indicators
    pub fn new(records: &'a [IndicatorRecord]) -> Self {
        Self { records }
    }
}

impl IndicatorLookup for SliceIndicatorLookup<'_> {
    fn lookup(
        &mut self,
        code: &str,
        fips: &str,
        subregion: &str,
        year: i32,
    ) -> Result<Option<f64>> {
        let code = code.trim();
        let fips = fips.trim();
        let subregion = subregion.trim();
        let mut best_low: Option<(i32, f64)> = None;
        let mut best_high: Option<(i32, f64)> = None;
        for r in self.records {
            if r.code.trim() != code || r.fips.trim() != fips || r.subcounty.trim() != subregion {
                continue;
            }
            let yr: i32 = match r.year.trim().parse() {
                Ok(y) => y,
                Err(_) => continue,
            };
            if yr <= year {
                if best_low.map_or(true, |(prev, _)| yr > prev) {
                    best_low = Some((yr, r.value));
                }
            } else if best_high.map_or(true, |(prev, _)| yr < prev) {
                best_high = Some((yr, r.value));
            }
        }
        match (best_low, best_high) {
            (Some((_, v)), _) => Ok(Some(v)),
            (None, Some((_, v))) => Ok(Some(v)),
            (None, None) => Ok(None),
        }
    }
}

/// A child geography that may receive part of the parent population.
///
/// Captures the `lfipcd`/`lstacd` (requested) and `lctlev`/`lstlev`
/// (already has its own population records) flags from the Fortran
/// COMMON blocks. Children where `requested` is false or
/// `has_own_records` is true are zeroed by the Fortran source and
/// skipped here.
#[derive(Debug, Clone)]
pub struct ChildGeography {
    /// 5-character FIPS-style code identifying the child geography.
    /// For [`allocate_state_to_county`] this is the county FIPS
    /// (`fipcod`); for [`allocate_nation_to_state`] it is the state
    /// FIPS (`statcd`); for [`allocate_county_to_subcounty`] it is
    /// the parent county's FIPS (the child differs only in subregion).
    pub fips: String,
    /// Whether this geography was selected by the run (`lfipcd` /
    /// `lstacd`).
    pub requested: bool,
    /// Whether this geography already has its own population records
    /// loaded (`lctlev` / `lstlev`) and therefore should be skipped.
    pub has_own_records: bool,
}

/// One child geography's allocated population.
///
/// Returned by all three allocation routines. `population` is zero
/// for children that were skipped (not requested, or had their own
/// data). `growth` is the growth value passed by the caller, stored
/// alongside the population to mirror the Fortran `grwcty` / `grwsta`
/// / `grwsub` arrays. Stored even for skipped children, matching the
/// Fortran behavior of writing into the same shared arrays as the
/// state/national-level routines.
#[derive(Debug, Clone, PartialEq)]
pub struct AllocatedRegion {
    /// FIPS-style identifier of the child geography.
    pub fips: String,
    /// For [`allocate_county_to_subcounty`], the subregion code; empty
    /// for the state/county routines.
    pub subregion: String,
    /// Allocated population for the child.
    ///
    /// Stored as `f64` to match [`input::pop::PopulationRecord::population`].
    /// The Fortran source uses `real*4`, but the moves-rs port widens
    /// population counts to `f64` throughout.
    ///
    /// [`input::pop::PopulationRecord::population`]: crate::input::pop::PopulationRecord::population
    pub population: f64,
    /// Growth value passed through from the parent record.
    pub growth: f64,
}

/// State-to-county allocation (ports `alocty.f`).
///
/// Given an `AllocationRecord` (parsed `.ALO` entry) for an SCC, a
/// state-level population that needs to be apportioned to its
/// counties, and the list of counties belonging to that state,
/// computes each county's share via indicator ratios. Counties where
/// `requested == false` or `has_own_records == true` receive
/// `population = 0.0` (matching the Fortran zero-and-skip behavior).
///
/// # Parameters
///
/// - `spec`: the `.ALO` record for the current SCC, providing up to
///   `MAX_COEFFICIENTS` (indicator_code, coefficient) pairs.
/// - `state_fips`: FIPS of the state whose population is being
///   distributed (the `regncd(icurec)(1:5)` from the Fortran caller).
/// - `counties`: the counties in this state (`fipcod` entries for
///   `idxcty(idxsta) .. idxcty(idxsta)+nconty(idxsta)-1`).
/// - `parent_population`: state population to distribute (`popyr`).
/// - `growth`: growth factor to store alongside each county's
///   allocation.
/// - `episode_year`: year for indicator lookup (`iepyr`).
/// - `lookup`: indicator-value source.
///
/// # Errors
///
/// Returns [`Error::IndicatorMissing`] when the lookup returns
/// `Ok(None)` for any indicator that is required (mirrors the
/// Fortran `IEOF` → error-7002 path). Propagates lookup errors.
pub fn allocate_state_to_county<L: IndicatorLookup>(
    spec: &AllocationRecord,
    state_fips: &str,
    counties: &[ChildGeography],
    parent_population: f64,
    growth: f64,
    episode_year: i32,
    lookup: &mut L,
) -> Result<Vec<AllocatedRegion>> {
    let parent_values = gather_parent_indicators(spec, state_fips, "", episode_year, lookup)?;
    let mut out = Vec::with_capacity(counties.len());
    for county in counties {
        let allocated = if !county.requested || county.has_own_records {
            0.0
        } else {
            let child_values =
                gather_parent_indicators(spec, &county.fips, "", episode_year, lookup)?;
            apportion(
                parent_population,
                &parent_values,
                &child_values,
                &spec.coefficients,
            )
        };
        out.push(AllocatedRegion {
            fips: county.fips.clone(),
            subregion: String::new(),
            population: allocated,
            growth,
        });
    }
    Ok(out)
}

/// Nation-to-state allocation (ports `alosta.f`).
///
/// Given an `AllocationRecord` for an SCC, a national-level
/// population, and the list of states, computes each state's share
/// via indicator ratios. States where `requested == false` or
/// `has_own_records == true` receive `population = 0.0`.
///
/// # Parameters
///
/// - `spec`: the `.ALO` record for the current SCC.
/// - `nation_fips`: FIPS of the national record (`regncd(icurec)(1:5)`,
///   typically `"00000"`).
/// - `states`: the list of states (`statcd` entries `1..=NSTATE`).
/// - `parent_population`: national population to distribute (`popyr`).
/// - `growth`: growth factor stored alongside each state's
///   allocation.
/// - `episode_year`: year for indicator lookup (`iepyr`).
/// - `lookup`: indicator-value source.
///
/// # Errors
///
/// Returns [`Error::IndicatorMissing`] when the lookup returns
/// `Ok(None)` for any indicator (Fortran `IEOF` → error-7002 path).
pub fn allocate_nation_to_state<L: IndicatorLookup>(
    spec: &AllocationRecord,
    nation_fips: &str,
    states: &[ChildGeography],
    parent_population: f64,
    growth: f64,
    episode_year: i32,
    lookup: &mut L,
) -> Result<Vec<AllocatedRegion>> {
    let parent_values = gather_parent_indicators(spec, nation_fips, "", episode_year, lookup)?;
    let mut out = Vec::with_capacity(states.len());
    for state in states {
        let allocated = if !state.requested || state.has_own_records {
            0.0
        } else {
            let child_values =
                gather_parent_indicators(spec, &state.fips, "", episode_year, lookup)?;
            apportion(
                parent_population,
                &parent_values,
                &child_values,
                &spec.coefficients,
            )
        };
        out.push(AllocatedRegion {
            fips: state.fips.clone(),
            subregion: String::new(),
            population: allocated,
            growth,
        });
    }
    Ok(out)
}

/// County-to-subcounty allocation (ports `alosub.f`).
///
/// Given an `AllocationRecord` for an SCC, a county-level population,
/// and a single subcounty (subregion) identifier, computes the
/// subcounty's share via the same parent/child ratio. Unlike the
/// state/county routines this returns a single allocation because
/// the Fortran source operates on one subregion per call.
///
/// # Parameters
///
/// - `spec`: the `.ALO` record for the current SCC.
/// - `county_fips`: FIPS of the parent county (`regncd(icurec)(1:5)`).
/// - `subregion`: 5-character subregion identifier; left-justified
///   to match the Fortran `lftjst` call at `alosub.f:115`.
/// - `parent_population`: county population to distribute (`popyr`).
/// - `growth`: growth factor stored alongside the allocation.
/// - `episode_year`: year for indicator lookup.
/// - `lookup`: indicator-value source.
///
/// # Errors
///
/// Returns [`Error::IndicatorMissing`] when the lookup returns
/// `Ok(None)` for any indicator.
pub fn allocate_county_to_subcounty<L: IndicatorLookup>(
    spec: &AllocationRecord,
    county_fips: &str,
    subregion: &str,
    parent_population: f64,
    growth: f64,
    episode_year: i32,
    lookup: &mut L,
) -> Result<AllocatedRegion> {
    let parent_values = gather_parent_indicators(spec, county_fips, "", episode_year, lookup)?;
    let subregion = subregion.trim_start();
    let child_values =
        gather_parent_indicators(spec, county_fips, subregion, episode_year, lookup)?;
    let allocated = apportion(
        parent_population,
        &parent_values,
        &child_values,
        &spec.coefficients,
    );
    Ok(AllocatedRegion {
        fips: county_fips.to_string(),
        subregion: subregion.to_string(),
        population: allocated,
        growth,
    })
}

/// Look up one indicator value per term of `spec`, returning a vector
/// of indicator values parallel to `spec.coefficients`.
///
/// Mirrors the `do 10 i=1,MXCOEF ... getind ... valsta(i) = valout`
/// loop in the Fortran source. `Ok(None)` (Fortran `IEOF`) is
/// surfaced as [`Error::IndicatorMissing`].
fn gather_parent_indicators<L: IndicatorLookup>(
    spec: &AllocationRecord,
    fips: &str,
    subregion: &str,
    year: i32,
    lookup: &mut L,
) -> Result<Vec<f64>> {
    debug_assert!(
        spec.coefficients.len() == spec.indicator_codes.len(),
        "AllocationRecord invariant: coefficients and indicator_codes \
         have the same length",
    );
    debug_assert!(spec.coefficients.len() <= MAX_COEFFICIENTS);
    let fips = pad_or_truncate(fips, FIPS_WIDTH);
    let subregion = pad_or_truncate(subregion, SUBREGION_WIDTH);
    let mut values = Vec::with_capacity(spec.coefficients.len());
    for code in &spec.indicator_codes {
        let trimmed = code.trim();
        if trimmed.is_empty() {
            // Empty indicator code stops the Fortran `do 10` loop via
            // `goto 111`; the remaining slots contribute 0 to the
            // ratio. Surfacing zero here is equivalent because
            // `apportion` skips terms with non-positive parents.
            values.push(0.0);
            continue;
        }
        match lookup.lookup(trimmed, &fips, &subregion, year)? {
            Some(v) => values.push(v),
            None => {
                return Err(Error::IndicatorMissing {
                    code: trimmed.to_string(),
                    fips: fips.clone(),
                    subregion: subregion.clone(),
                    year,
                });
            }
        }
    }
    Ok(values)
}

/// Compute `parent_population * sum_i (child_i / parent_i) * coeff_i`
/// over terms where `parent_i > 0`.
///
/// Mirrors `alocty.f:137–141`, `alosta.f:130–134`,
/// `alosub.f:128–133`. The Fortran source stores intermediates in
/// `real*4`; the Rust port computes in `f64` to match the moves-rs
/// convention of carrying population counts as `f64`.
fn apportion(
    parent_population: f64,
    parent_values: &[f64],
    child_values: &[f64],
    coefficients: &[f32],
) -> f64 {
    let n = parent_values
        .len()
        .min(child_values.len())
        .min(coefficients.len());
    let mut ratio: f64 = 0.0;
    for i in 0..n {
        if parent_values[i] > 0.0 {
            ratio += (child_values[i] / parent_values[i]) * coefficients[i] as f64;
        }
    }
    parent_population * ratio
}

/// Pad with trailing spaces or truncate to `width` characters,
/// matching Fortran fixed-width `CHARACTER*N` storage.
fn pad_or_truncate(s: &str, width: usize) -> String {
    if s.len() >= width {
        s[..width].to_string()
    } else {
        let mut out = String::with_capacity(width);
        out.push_str(s);
        for _ in s.len()..width {
            out.push(' ');
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::indicator::IndicatorRecord;
    use std::collections::HashMap;

    /// Mock lookup keyed by (code, fips, subregion); year resolution
    /// is the same closest-≤ logic used by the production
    /// implementation, but the storage is a HashMap of Vec<(year,
    /// value)> so tests don't depend on slice ordering.
    #[derive(Default)]
    struct MockLookup {
        by_key: HashMap<(String, String, String), Vec<(i32, f64)>>,
    }

    impl MockLookup {
        fn insert(&mut self, code: &str, fips: &str, sub: &str, year: i32, value: f64) {
            self.by_key
                .entry((code.into(), fips.into(), sub.into()))
                .or_default()
                .push((year, value));
        }
    }

    impl IndicatorLookup for MockLookup {
        fn lookup(
            &mut self,
            code: &str,
            fips: &str,
            subregion: &str,
            year: i32,
        ) -> Result<Option<f64>> {
            let key = (
                code.trim().to_string(),
                fips.trim().to_string(),
                subregion.trim().to_string(),
            );
            let Some(entries) = self.by_key.get(&key) else {
                return Ok(None);
            };
            let mut best_low: Option<(i32, f64)> = None;
            let mut best_high: Option<(i32, f64)> = None;
            for &(yr, v) in entries {
                if yr <= year {
                    if best_low.map_or(true, |(prev, _)| yr > prev) {
                        best_low = Some((yr, v));
                    }
                } else if best_high.map_or(true, |(prev, _)| yr < prev) {
                    best_high = Some((yr, v));
                }
            }
            Ok(best_low.or(best_high).map(|(_, v)| v))
        }
    }

    fn spec(coeffs: &[f32], codes: &[&str]) -> AllocationRecord {
        AllocationRecord {
            scc: "2270002003".into(),
            coefficients: coeffs.to_vec(),
            indicator_codes: codes.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn child(fips: &str, requested: bool, has_own: bool) -> ChildGeography {
        ChildGeography {
            fips: fips.into(),
            requested,
            has_own_records: has_own,
        }
    }

    #[test]
    fn state_to_county_distributes_by_pop_ratio() {
        let spec = spec(&[1.0], &["POP"]);
        let mut lookup = MockLookup::default();
        // State 17 (Illinois) and its two requested counties.
        lookup.insert("POP", "17000", "", 2020, 10_000.0);
        lookup.insert("POP", "17031", "", 2020, 6_000.0); // Cook
        lookup.insert("POP", "17043", "", 2020, 4_000.0); // DuPage

        let counties = vec![child("17031", true, false), child("17043", true, false)];
        let out =
            allocate_state_to_county(&spec, "17000", &counties, 1000.0, 1.5, 2020, &mut lookup)
                .unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].fips, "17031");
        assert!((out[0].population - 600.0).abs() < 1e-3);
        assert!((out[1].population - 400.0).abs() < 1e-3);
        for r in &out {
            assert_eq!(r.growth, 1.5);
            assert_eq!(r.subregion, "");
        }
    }

    #[test]
    fn unrequested_county_gets_zero_population_but_keeps_growth() {
        let spec = spec(&[1.0], &["POP"]);
        let mut lookup = MockLookup::default();
        lookup.insert("POP", "17000", "", 2020, 10_000.0);
        lookup.insert("POP", "17031", "", 2020, 6_000.0);
        // 17043 is not requested → never looked up.

        let counties = vec![child("17031", true, false), child("17043", false, false)];
        let out =
            allocate_state_to_county(&spec, "17000", &counties, 1000.0, 2.0, 2020, &mut lookup)
                .unwrap();
        assert!((out[0].population - 600.0).abs() < 1e-3);
        assert_eq!(out[1].population, 0.0);
        assert_eq!(out[1].growth, 2.0);
    }

    #[test]
    fn county_with_own_records_is_skipped() {
        let spec = spec(&[1.0], &["POP"]);
        let mut lookup = MockLookup::default();
        lookup.insert("POP", "17000", "", 2020, 10_000.0);
        lookup.insert("POP", "17031", "", 2020, 6_000.0);

        // Cook is requested but already has its own population records.
        let counties = vec![child("17031", true, true)];
        let out =
            allocate_state_to_county(&spec, "17000", &counties, 1000.0, 1.0, 2020, &mut lookup)
                .unwrap();
        assert_eq!(out[0].population, 0.0);
    }

    #[test]
    fn multiple_indicators_blend_by_coefficients() {
        let spec = spec(&[0.6, 0.4], &["POP", "EMP"]);
        let mut lookup = MockLookup::default();
        // State: 10 of POP, 100 of EMP.
        lookup.insert("POP", "17000", "", 2020, 10.0);
        lookup.insert("EMP", "17000", "", 2020, 100.0);
        // County: 4 of POP (40%), 30 of EMP (30%).
        lookup.insert("POP", "17031", "", 2020, 4.0);
        lookup.insert("EMP", "17031", "", 2020, 30.0);

        let counties = vec![child("17031", true, false)];
        let out =
            allocate_state_to_county(&spec, "17000", &counties, 1000.0, 1.0, 2020, &mut lookup)
                .unwrap();
        // ratio = 0.6 * (4/10) + 0.4 * (30/100) = 0.24 + 0.12 = 0.36
        assert!(
            (out[0].population - 360.0).abs() < 1e-3,
            "got {}",
            out[0].population
        );
    }

    #[test]
    fn zero_parent_indicator_skips_term() {
        let spec = spec(&[0.5, 0.5], &["POP", "EMP"]);
        let mut lookup = MockLookup::default();
        // POP is zero at the parent level → that term contributes 0.
        lookup.insert("POP", "17000", "", 2020, 0.0);
        lookup.insert("EMP", "17000", "", 2020, 100.0);
        lookup.insert("POP", "17031", "", 2020, 50.0);
        lookup.insert("EMP", "17031", "", 2020, 60.0);

        let counties = vec![child("17031", true, false)];
        let out =
            allocate_state_to_county(&spec, "17000", &counties, 1000.0, 1.0, 2020, &mut lookup)
                .unwrap();
        // ratio = 0.5 * (60/100) = 0.30 (POP skipped); 0.5 * 0/0 ignored.
        assert!((out[0].population - 300.0).abs() < 1e-3);
    }

    #[test]
    fn missing_parent_indicator_errors_with_indicator_missing() {
        let spec = spec(&[1.0], &["XYZ"]);
        let mut lookup = MockLookup::default();
        let counties = vec![child("17031", true, false)];
        let err = allocate_state_to_county(&spec, "17000", &counties, 1.0, 1.0, 2020, &mut lookup)
            .unwrap_err();
        match err {
            Error::IndicatorMissing {
                code, fips, year, ..
            } => {
                assert_eq!(code, "XYZ");
                assert_eq!(fips.trim(), "17000");
                assert_eq!(year, 2020);
            }
            other => panic!("expected IndicatorMissing, got {other:?}"),
        }
    }

    #[test]
    fn nation_to_state_uses_state_loop() {
        let spec = spec(&[1.0], &["POP"]);
        let mut lookup = MockLookup::default();
        lookup.insert("POP", "00000", "", 2020, 1000.0);
        lookup.insert("POP", "06000", "", 2020, 400.0); // CA
        lookup.insert("POP", "17000", "", 2020, 300.0); // IL
        lookup.insert("POP", "48000", "", 2020, 300.0); // TX
        let states = vec![
            child("06000", true, false),
            child("17000", true, false),
            child("48000", true, false),
        ];
        let out =
            allocate_nation_to_state(&spec, "00000", &states, 10_000.0, 1.0, 2020, &mut lookup)
                .unwrap();
        assert!((out[0].population - 4_000.0).abs() < 1e-2);
        assert!((out[1].population - 3_000.0).abs() < 1e-2);
        assert!((out[2].population - 3_000.0).abs() < 1e-2);
    }

    #[test]
    fn county_to_subcounty_is_single_region() {
        let spec = spec(&[1.0], &["POP"]);
        let mut lookup = MockLookup::default();
        lookup.insert("POP", "17031", "", 2020, 5000.0);
        lookup.insert("POP", "17031", "Z01", 2020, 1000.0);
        let out =
            allocate_county_to_subcounty(&spec, "17031", "Z01", 5000.0, 1.0, 2020, &mut lookup)
                .unwrap();
        assert_eq!(out.fips, "17031");
        assert_eq!(out.subregion, "Z01");
        assert!((out.population - 1000.0).abs() < 1e-3);
    }

    #[test]
    fn subcounty_left_justifies_subregion() {
        let spec = spec(&[1.0], &["POP"]);
        let mut lookup = MockLookup::default();
        lookup.insert("POP", "17031", "", 2020, 5000.0);
        lookup.insert("POP", "17031", "Z01", 2020, 1000.0);
        // Pass with leading whitespace; should match the same record.
        let out =
            allocate_county_to_subcounty(&spec, "17031", "  Z01", 5000.0, 1.0, 2020, &mut lookup)
                .unwrap();
        assert_eq!(out.subregion, "Z01");
        assert!((out.population - 1000.0).abs() < 1e-3);
    }

    #[test]
    fn coefficients_truncated_at_first_empty_slot() {
        // The .ALO parser already enforces the "stop at first empty"
        // rule (`rdalo.f:112`), so AllocationRecord never carries
        // empty trailing slots. This test documents that the
        // allocator handles the case where the parser produced fewer
        // than MAX_COEFFICIENTS terms.
        let spec = spec(&[1.0], &["POP"]);
        assert_eq!(spec.coefficients.len(), 1);
        let mut lookup = MockLookup::default();
        lookup.insert("POP", "17000", "", 2020, 10.0);
        lookup.insert("POP", "17031", "", 2020, 5.0);
        let counties = vec![child("17031", true, false)];
        let out =
            allocate_state_to_county(&spec, "17000", &counties, 100.0, 1.0, 2020, &mut lookup)
                .unwrap();
        assert!((out[0].population - 50.0).abs() < 1e-3);
    }

    #[test]
    fn empty_indicator_code_slot_contributes_zero() {
        // A blank indicator code mid-record (rare but tolerated by
        // the data model) is treated as a zero contribution — mirrors
        // the Fortran `goto 111` early-exit on `strlen == 0`.
        let spec = AllocationRecord {
            scc: "2270002003".into(),
            coefficients: vec![0.5, 0.5],
            indicator_codes: vec!["POP".into(), "   ".into()],
        };
        let mut lookup = MockLookup::default();
        lookup.insert("POP", "17000", "", 2020, 10.0);
        lookup.insert("POP", "17031", "", 2020, 5.0);
        let counties = vec![child("17031", true, false)];
        let out =
            allocate_state_to_county(&spec, "17000", &counties, 100.0, 1.0, 2020, &mut lookup)
                .unwrap();
        // Only POP contributes: ratio = 0.5 * (5/10) = 0.25.
        assert!((out[0].population - 25.0).abs() < 1e-3);
    }

    #[test]
    fn slice_lookup_picks_closest_year_le() {
        let mut recs = vec![
            IndicatorRecord {
                code: "POP".into(),
                fips: "17031".into(),
                subcounty: "00000".into(),
                year: "2015".into(),
                value: 1.0,
            },
            IndicatorRecord {
                code: "POP".into(),
                fips: "17031".into(),
                subcounty: "00000".into(),
                year: "2020".into(),
                value: 2.0,
            },
            IndicatorRecord {
                code: "POP".into(),
                fips: "17031".into(),
                subcounty: "00000".into(),
                year: "2030".into(),
                value: 3.0,
            },
        ];
        crate::input::indicator::sort_indicators(&mut recs);
        let mut lookup = SliceIndicatorLookup::new(&recs);
        // Query 2025 → closest ≤ is 2020.
        let v = lookup.lookup("POP", "17031", "00000", 2025).unwrap();
        assert_eq!(v, Some(2.0));
        // Query 2010 → no ≤, fall back to earliest > (2015).
        let v = lookup.lookup("POP", "17031", "00000", 2010).unwrap();
        assert_eq!(v, Some(1.0));
        // Query 2020 exactly → 2020.
        let v = lookup.lookup("POP", "17031", "00000", 2020).unwrap();
        assert_eq!(v, Some(2.0));
    }

    #[test]
    fn slice_lookup_misses_return_none() {
        let recs: Vec<IndicatorRecord> = Vec::new();
        let mut lookup = SliceIndicatorLookup::new(&recs);
        let v = lookup.lookup("POP", "17031", "00000", 2020).unwrap();
        assert!(v.is_none());
    }

    #[test]
    fn pad_or_truncate_pads_and_truncates() {
        assert_eq!(pad_or_truncate("17", 5), "17   ");
        assert_eq!(pad_or_truncate("17000", 5), "17000");
        // Truncation (defensive — production inputs already fit).
        assert_eq!(pad_or_truncate("170000", 5), "17000");
    }
}

//! County, state-to-county, and subcounty allocation logic.
//!
//! Cluster 5 of the seven functional groups (see
//! `ARCHITECTURE.md` § 2.5). The three routines are similar in
//! structure: a "parent" geography's population is split across
//! its "children" by a regression on per-region spatial-indicator
//! values, with coefficients supplied by the SCC's `.ALO` record.
//!
//! For each indicator code attached to the SCC, the parent and
//! child indicator values are looked up via [`IndicatorTable`] (the
//! Rust replacement for `getind.f`'s streaming-file search). The
//! child's allocation share is then
//!
//! ```text
//! valalo = Σ_i (val_child[i] / val_parent[i]) * coeff[i]
//! ```
//!
//! over indicator slots where `val_parent[i] > 0` (terms with
//! non-positive parent values are silently dropped, matching the
//! `if( valsta(i) .GT. 0 )` guard at `alocty.f` :138 / `alosta.f`
//! :133 / `alosub.f` :131).
//!
//! # Fortran source files this module ports
//!
//! | File | Lines | Role |
//! |---|---|---|
//! | `alocty.f` | 181 | County allocation (state → counties) |
//! | `alosta.f` | 176 | State-to-county allocation (national → states) |
//! | `alosub.f` | 170 | Subcounty allocation (county → subcounty) |
//!
//! # Error handling
//!
//! When a referenced indicator code has no record at the requested
//! geography the Fortran routines write to `IOWSTD`/`IOWMSG` and
//! return `IEOF` (`alocty.f` :160). The Rust port surfaces the same
//! failure as [`Error::IndicatorMissing`] carrying the offending
//! `code`, `fips`, `subcounty`, and `year`.
//!
//! # Skipped regions
//!
//! Both `alocty.f` and `alosta.f` skip child regions flagged by
//! `lfipcd`/`lstacd` (region not in the run) or `lctlev`/`lstlev`
//! (region carries its own population records and is computed
//! directly). The Rust port honours the same skips via
//! [`CountyDescriptor::selected`] / [`CountyDescriptor::has_county_records`]
//! and the [`StateDescriptor`] equivalents. Skipped children appear
//! in the output vector with zero population and `growth` carried
//! through unchanged.
//!
//! # Subcounty marker
//!
//! State- and national-level lookups need a placeholder subcounty
//! string. The Fortran source uses 5 blanks (`alocty.f` :91); the
//! Rust port uses [`STATE_LEVEL_SUBCOUNTY`] (the empty string)
//! consistently. Callers must keep their indicator records in the
//! same convention — the lookup compares the strings exactly
//! (case-insensitive).

use crate::input::alo::AllocationRecord;
use crate::input::indicator::IndicatorTable;
use crate::{Error, Result};

/// Marker subcounty value used by state- and national-level
/// indicator lookups (parent in `alocty`/`alosta`; child blank
/// before subcounty assignment in `alosub`).
///
/// Mirrors `subtmp = '     '` in the Fortran source. Callers and
/// indicator records must use the same marker for state-level
/// lookups to match.
pub const STATE_LEVEL_SUBCOUNTY: &str = "";

/// One county descriptor consumed by [`allocate_county`].
///
/// Replaces the per-county slots of the Fortran COMMON arrays
/// `fipcod` (FIPS code), `lfipcd` (county-in-run flag), and
/// `lctlev` (county-has-county-level-records flag).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CountyDescriptor {
    /// 5-character county FIPS code.
    pub fips: String,
    /// `true` iff the county is requested for the current run
    /// (Fortran `lfipcd`).
    pub selected: bool,
    /// `true` iff the county has its own county-level population
    /// records and therefore should not be allocated from the state
    /// total (Fortran `lctlev`).
    pub has_county_records: bool,
}

/// One state descriptor consumed by [`allocate_state`].
///
/// Replaces the per-state slots of `statcd` (state FIPS code),
/// `lstacd` (state-in-run flag), and `lstlev`
/// (state-has-state-level-records flag).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateDescriptor {
    /// 5-character state FIPS code, e.g. `"17000"` for Illinois.
    pub fips: String,
    /// `true` iff the state is requested for the current run
    /// (Fortran `lstacd`).
    pub selected: bool,
    /// `true` iff the state has its own state-level records and
    /// therefore should not be allocated from the national total
    /// (Fortran `lstlev`).
    pub has_state_records: bool,
}

/// Per-county result from [`allocate_county`]. Mirrors the
/// `(popcty(idxfip), grwcty(idxfip))` pair the Fortran writes back
/// into the COMMON population arrays.
///
/// The Fortran source zeroes `popcty(idxfip)` for every county in
/// the loop, including skipped ones (`alocty.f` :111). It only
/// assigns `grwcty(idxfip)` when the county is *not* skipped
/// (`alocty.f` :145), so skipped counties retain whatever growth
/// value was previously in the global slot. The Rust port models
/// this with `growth: Option<f32>` — `None` signals "do not update
/// the growth state from this entry."
#[derive(Debug, Clone, PartialEq)]
pub struct CountyAllocation {
    /// County FIPS code (copied from [`CountyDescriptor::fips`]).
    pub fips: String,
    /// Allocated county population (`popcty(idxfip)` in `alocty.f`).
    /// Always zero when the county was skipped.
    pub population: f32,
    /// Growth factor to store in the county slot
    /// (`grwcty(idxfip) = growth` at `alocty.f` :145), or `None`
    /// when the county was skipped and the caller should leave any
    /// pre-existing growth value alone.
    pub growth: Option<f32>,
}

/// Per-state result from [`allocate_state`]. The
/// `Option<f32>` growth mirrors the Fortran asymmetry: `popsta` is
/// zeroed unconditionally, `grwsta` is only assigned for
/// non-skipped states (`alosta.f` :106 / :140).
#[derive(Debug, Clone, PartialEq)]
pub struct StateAllocation {
    /// State FIPS code (copied from [`StateDescriptor::fips`]).
    pub fips: String,
    /// Allocated state population (`popsta(idxsta)` in `alosta.f`).
    /// Always zero when the state was skipped.
    pub population: f32,
    /// Growth factor (`grwsta(idxsta)` in `alosta.f`), or `None`
    /// when the state was skipped.
    pub growth: Option<f32>,
}

/// Result of [`allocate_subcounty`]. Mirrors the `(popsub, grwsub)`
/// output pair of `alosub.f`.
#[derive(Debug, Clone, PartialEq)]
pub struct SubcountyAllocation {
    /// Subcounty identifier, after left-justification (Fortran
    /// `subtmp = subreg; lftjst(subtmp)` at `alosub.f` :114).
    pub subcounty: String,
    /// Allocated subcounty population (`popsub` in `alosub.f`).
    pub population: f32,
    /// Growth factor (`grwsub` in `alosub.f`).
    pub growth: f32,
}

/// Allocate a state's population to its counties via spatial-indicator
/// regression. Ports `alocty.f`.
///
/// For each indicator code in `record`:
///
/// 1. The state-level indicator value is looked up at `state_fips`
///    with [`STATE_LEVEL_SUBCOUNTY`] (`valsta` in the Fortran).
/// 2. For each selected county, the county-level value is looked up
///    at the county's FIPS with [`STATE_LEVEL_SUBCOUNTY`] (`valcty`).
/// 3. The county receives `state_population * Σ_i (valcty[i] /
///    valsta[i]) * coeff[i]`, summed over slots with
///    `valsta[i] > 0` (`alocty.f` :137–:140).
///
/// Counties with `selected = false` or `has_county_records = true`
/// receive zero population (Fortran `goto 20`, `alocty.f` :116–:117).
/// The output vector is parallel to `counties` — every input entry
/// produces exactly one output entry, in the same order.
///
/// Returns `Err(Error::IndicatorMissing)` when any referenced
/// indicator code has no data at the requested geography (mirrors
/// the `IEOF` → `7002` error path in the Fortran source).
pub fn allocate_county(
    state_fips: &str,
    counties: &[CountyDescriptor],
    record: &AllocationRecord,
    indicators: &IndicatorTable,
    year: i32,
    state_population: f32,
    growth: f32,
) -> Result<Vec<CountyAllocation>> {
    let parent_vals =
        lookup_indicators(record, state_fips, STATE_LEVEL_SUBCOUNTY, indicators, year)?;

    let mut out = Vec::with_capacity(counties.len());
    for county in counties {
        if !county.selected || county.has_county_records {
            out.push(CountyAllocation {
                fips: county.fips.clone(),
                population: 0.0,
                growth: None,
            });
            continue;
        }
        let child_vals = lookup_indicators(
            record,
            &county.fips,
            STATE_LEVEL_SUBCOUNTY,
            indicators,
            year,
        )?;
        let valalo = compute_alloc_factor(&parent_vals, &child_vals, &record.coefficients);
        out.push(CountyAllocation {
            fips: county.fips.clone(),
            population: state_population * valalo,
            growth: Some(growth),
        });
    }
    Ok(out)
}

/// Allocate the national population to its states via spatial-indicator
/// regression. Ports `alosta.f`.
///
/// Mirrors [`allocate_county`] one level up: the parent is the
/// nation (looked up at `national_fips`, which is the FIPS-style
/// placeholder the indicator file uses for the country, typically
/// `"00000"`), and the children are the entries of `states`.
///
/// States with `selected = false` or `has_state_records = true`
/// receive zero population (Fortran `lstacd`/`lstlev` skips at
/// `alosta.f` :111–:112). The output vector is parallel to
/// `states`.
pub fn allocate_state(
    national_fips: &str,
    states: &[StateDescriptor],
    record: &AllocationRecord,
    indicators: &IndicatorTable,
    year: i32,
    national_population: f32,
    growth: f32,
) -> Result<Vec<StateAllocation>> {
    let parent_vals = lookup_indicators(
        record,
        national_fips,
        STATE_LEVEL_SUBCOUNTY,
        indicators,
        year,
    )?;

    let mut out = Vec::with_capacity(states.len());
    for state in states {
        if !state.selected || state.has_state_records {
            out.push(StateAllocation {
                fips: state.fips.clone(),
                population: 0.0,
                growth: None,
            });
            continue;
        }
        let child_vals =
            lookup_indicators(record, &state.fips, STATE_LEVEL_SUBCOUNTY, indicators, year)?;
        let valalo = compute_alloc_factor(&parent_vals, &child_vals, &record.coefficients);
        out.push(StateAllocation {
            fips: state.fips.clone(),
            population: national_population * valalo,
            growth: Some(growth),
        });
    }
    Ok(out)
}

/// Allocate a county's population to a single subcounty via
/// spatial-indicator regression. Ports `alosub.f`.
///
/// The parent indicator values are looked up at the county FIPS
/// with [`STATE_LEVEL_SUBCOUNTY`] (`alosub.f` :92–:105); the child
/// values use the same FIPS with the supplied `subcounty` after
/// left-justification (`alosub.f` :114–:124).
///
/// Returns one [`SubcountyAllocation`]. The Fortran source does not
/// have per-subcounty skipping flags at this level — the caller is
/// expected to invoke this routine only for subcounties that are in
/// the run.
pub fn allocate_subcounty(
    county_fips: &str,
    subcounty: &str,
    record: &AllocationRecord,
    indicators: &IndicatorTable,
    year: i32,
    county_population: f32,
    growth: f32,
) -> Result<SubcountyAllocation> {
    let parent_vals =
        lookup_indicators(record, county_fips, STATE_LEVEL_SUBCOUNTY, indicators, year)?;
    let subcounty_key = left_justify(subcounty);
    let child_vals = lookup_indicators(record, county_fips, &subcounty_key, indicators, year)?;
    let valalo = compute_alloc_factor(&parent_vals, &child_vals, &record.coefficients);
    Ok(SubcountyAllocation {
        subcounty: subcounty_key,
        population: county_population * valalo,
        growth,
    })
}

/// Look up every indicator code in `record` at the given geography.
///
/// Returns one value per `record.indicator_codes` slot, in slot
/// order. Any blank slots in the Fortran source were already
/// dropped by `rdalo.f` so the vector is already trimmed.
fn lookup_indicators(
    record: &AllocationRecord,
    fips: &str,
    subcounty: &str,
    indicators: &IndicatorTable,
    year: i32,
) -> Result<Vec<f32>> {
    let mut out = Vec::with_capacity(record.indicator_codes.len());
    for code in &record.indicator_codes {
        let trimmed = code.trim();
        let value = indicators
            .lookup(trimmed, fips, subcounty, year)
            .ok_or_else(|| Error::IndicatorMissing {
                code: trimmed.to_string(),
                fips: fips.to_string(),
                subcounty: subcounty.to_string(),
                year,
            })?;
        out.push(value);
    }
    Ok(out)
}

/// Compute the regression-weighted allocation factor.
///
/// Mirrors the inner `valalo` accumulation at `alocty.f` :136–:140
/// (and the matching loops in `alosta.f` / `alosub.f`): terms with
/// non-positive parent values are silently dropped, the rest sum
/// `(child[i] / parent[i]) * coeff[i]`.
fn compute_alloc_factor(parent_vals: &[f32], child_vals: &[f32], coeffs: &[f32]) -> f32 {
    let n = parent_vals.len().min(child_vals.len()).min(coeffs.len());
    let mut acc = 0.0_f32;
    for i in 0..n {
        if parent_vals[i] > 0.0 {
            acc += (child_vals[i] / parent_vals[i]) * coeffs[i];
        }
    }
    acc
}

/// Left-justify a string in place (Fortran `lftjst`).
///
/// Strips leading whitespace; trailing whitespace is preserved
/// (it's the Fortran character-length padding). Matches the
/// semantics expected by `alosub.f` :115.
fn left_justify(s: &str) -> String {
    s.trim_start().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::alo::AllocationRecord;
    use crate::input::indicator::IndicatorRecord;

    fn alo_record(scc: &str, pairs: &[(&str, f32)]) -> AllocationRecord {
        AllocationRecord {
            scc: scc.to_string(),
            coefficients: pairs.iter().map(|(_, c)| *c).collect(),
            indicator_codes: pairs.iter().map(|(code, _)| (*code).to_string()).collect(),
        }
    }

    fn ind_record(code: &str, fips: &str, sub: &str, year: i32, value: f32) -> IndicatorRecord {
        IndicatorRecord {
            code: code.to_string(),
            fips: fips.to_string(),
            subcounty: sub.to_string(),
            year: year.to_string(),
            value: value as f64,
        }
    }

    fn county(fips: &str, selected: bool, has_records: bool) -> CountyDescriptor {
        CountyDescriptor {
            fips: fips.to_string(),
            selected,
            has_county_records: has_records,
        }
    }

    fn state(fips: &str, selected: bool, has_records: bool) -> StateDescriptor {
        StateDescriptor {
            fips: fips.to_string(),
            selected,
            has_state_records: has_records,
        }
    }

    #[test]
    fn allocate_county_splits_state_population_by_indicator_ratio() {
        // One indicator with coefficient 1.0 → county share is just
        // the value ratio.
        let record = alo_record("2270002003", &[("POP", 1.0)]);
        let indicators = IndicatorTable::new(vec![
            ind_record("POP", "17000", "", 2020, 1000.0),
            ind_record("POP", "17031", "", 2020, 400.0),
            ind_record("POP", "17043", "", 2020, 600.0),
        ]);
        let counties = vec![county("17031", true, false), county("17043", true, false)];
        let out =
            allocate_county("17000", &counties, &record, &indicators, 2020, 100.0, 1.5).unwrap();
        assert_eq!(out.len(), 2);
        assert!((out[0].population - 40.0).abs() < 1e-4);
        assert!((out[1].population - 60.0).abs() < 1e-4);
        assert_eq!(out[0].growth, Some(1.5));
        assert_eq!(out[1].growth, Some(1.5));
    }

    #[test]
    fn allocate_county_multiple_indicators_weighted_sum() {
        // Two indicators with coefficients summing to 1.
        // valalo = (cty_pop / sta_pop) * 0.7 + (cty_emp / sta_emp) * 0.3
        //        = (10/100)*0.7 + (5/20)*0.3 = 0.07 + 0.075 = 0.145
        let record = alo_record("AAA", &[("POP", 0.7), ("EMP", 0.3)]);
        let indicators = IndicatorTable::new(vec![
            ind_record("POP", "17000", "", 2020, 100.0),
            ind_record("EMP", "17000", "", 2020, 20.0),
            ind_record("POP", "17031", "", 2020, 10.0),
            ind_record("EMP", "17031", "", 2020, 5.0),
        ]);
        let counties = vec![county("17031", true, false)];
        let out =
            allocate_county("17000", &counties, &record, &indicators, 2020, 200.0, 1.0).unwrap();
        let expected = 200.0_f32 * ((10.0_f32 / 100.0) * 0.7 + (5.0_f32 / 20.0) * 0.3);
        assert!((out[0].population - expected).abs() < 1e-4);
    }

    #[test]
    fn allocate_county_skips_unselected_and_self_reporting_counties() {
        let record = alo_record("AAA", &[("POP", 1.0)]);
        let indicators = IndicatorTable::new(vec![
            ind_record("POP", "17000", "", 2020, 100.0),
            ind_record("POP", "17031", "", 2020, 30.0),
            // No record for 17043 and 17097 — the skip path must not
            // call the lookup, otherwise we'd see an error.
        ]);
        let counties = vec![
            county("17031", true, false),  // computed
            county("17043", false, false), // not in run → skip
            county("17097", true, true),   // own records → skip
        ];
        let out =
            allocate_county("17000", &counties, &record, &indicators, 2020, 100.0, 2.0).unwrap();
        assert_eq!(out.len(), 3);
        assert!((out[0].population - 30.0).abs() < 1e-4);
        assert_eq!(out[0].growth, Some(2.0));
        assert_eq!(out[1].population, 0.0);
        assert_eq!(out[1].growth, None);
        assert_eq!(out[2].population, 0.0);
        assert_eq!(out[2].growth, None);
    }

    #[test]
    fn allocate_county_drops_terms_with_zero_state_indicator() {
        // POP exists at 17000 but is 0; EMP is positive. Only EMP
        // contributes (matching `alocty.f` :137–:140).
        let record = alo_record("AAA", &[("POP", 0.5), ("EMP", 0.5)]);
        let indicators = IndicatorTable::new(vec![
            ind_record("POP", "17000", "", 2020, 0.0),
            ind_record("EMP", "17000", "", 2020, 20.0),
            ind_record("POP", "17031", "", 2020, 30.0),
            ind_record("EMP", "17031", "", 2020, 5.0),
        ]);
        let counties = vec![county("17031", true, false)];
        let out =
            allocate_county("17000", &counties, &record, &indicators, 2020, 100.0, 1.0).unwrap();
        // Only the EMP term: 100 * (5/20) * 0.5 = 12.5
        assert!((out[0].population - 12.5).abs() < 1e-4);
    }

    #[test]
    fn allocate_county_missing_state_indicator_errors() {
        let record = alo_record("AAA", &[("POP", 1.0)]);
        let indicators = IndicatorTable::new(vec![
            // State POP missing entirely.
            ind_record("POP", "17031", "", 2020, 10.0),
        ]);
        let counties = vec![county("17031", true, false)];
        let err = allocate_county("17000", &counties, &record, &indicators, 2020, 100.0, 1.0)
            .unwrap_err();
        match err {
            Error::IndicatorMissing {
                code, fips, year, ..
            } => {
                assert_eq!(code, "POP");
                assert_eq!(fips, "17000");
                assert_eq!(year, 2020);
            }
            other => panic!("expected IndicatorMissing, got {other:?}"),
        }
    }

    #[test]
    fn allocate_county_missing_county_indicator_errors() {
        let record = alo_record("AAA", &[("POP", 1.0)]);
        let indicators = IndicatorTable::new(vec![
            ind_record("POP", "17000", "", 2020, 100.0),
            // County 17031 missing.
        ]);
        let counties = vec![county("17031", true, false)];
        let err = allocate_county("17000", &counties, &record, &indicators, 2020, 100.0, 1.0)
            .unwrap_err();
        match err {
            Error::IndicatorMissing { fips, .. } => assert_eq!(fips, "17031"),
            other => panic!("expected IndicatorMissing, got {other:?}"),
        }
    }

    #[test]
    fn allocate_state_splits_national_population_by_indicator_ratio() {
        let record = alo_record("AAA", &[("POP", 1.0)]);
        let indicators = IndicatorTable::new(vec![
            ind_record("POP", "00000", "", 2020, 1000.0),
            ind_record("POP", "17000", "", 2020, 200.0),
            ind_record("POP", "06000", "", 2020, 800.0),
        ]);
        let states = vec![state("17000", true, false), state("06000", true, false)];
        let out = allocate_state("00000", &states, &record, &indicators, 2020, 100.0, 1.0).unwrap();
        assert_eq!(out.len(), 2);
        assert!((out[0].population - 20.0).abs() < 1e-4);
        assert!((out[1].population - 80.0).abs() < 1e-4);
    }

    #[test]
    fn allocate_state_skips_unselected_and_self_reporting_states() {
        let record = alo_record("AAA", &[("POP", 1.0)]);
        let indicators = IndicatorTable::new(vec![
            ind_record("POP", "00000", "", 2020, 1000.0),
            ind_record("POP", "17000", "", 2020, 200.0),
        ]);
        let states = vec![
            state("17000", true, false),  // included
            state("06000", false, false), // skip: not in run
            state("48000", true, true),   // skip: has its own data
        ];
        let out = allocate_state("00000", &states, &record, &indicators, 2020, 100.0, 1.0).unwrap();
        assert!((out[0].population - 20.0).abs() < 1e-4);
        assert_eq!(out[0].growth, Some(1.0));
        assert_eq!(out[1].population, 0.0);
        assert_eq!(out[1].growth, None);
        assert_eq!(out[2].population, 0.0);
        assert_eq!(out[2].growth, None);
    }

    #[test]
    fn allocate_subcounty_splits_county_population_by_indicator_ratio() {
        let record = alo_record("AAA", &[("POP", 1.0)]);
        let indicators = IndicatorTable::new(vec![
            ind_record("POP", "17031", "", 2020, 100.0),
            ind_record("POP", "17031", "DOWN", 2020, 25.0),
        ]);
        let out =
            allocate_subcounty("17031", "DOWN", &record, &indicators, 2020, 200.0, 1.25).unwrap();
        assert_eq!(out.subcounty, "DOWN");
        assert!((out.population - 50.0).abs() < 1e-4);
        assert_eq!(out.growth, 1.25);
    }

    #[test]
    fn allocate_subcounty_left_justifies_subregion_argument() {
        let record = alo_record("AAA", &[("POP", 1.0)]);
        let indicators = IndicatorTable::new(vec![
            ind_record("POP", "17031", "", 2020, 100.0),
            ind_record("POP", "17031", "DOWN", 2020, 40.0),
        ]);
        // Leading whitespace must be stripped before lookup.
        let out =
            allocate_subcounty("17031", "  DOWN", &record, &indicators, 2020, 200.0, 1.0).unwrap();
        assert_eq!(out.subcounty, "DOWN");
        assert!((out.population - 80.0).abs() < 1e-4);
    }

    #[test]
    fn allocate_subcounty_missing_subcounty_indicator_errors() {
        let record = alo_record("AAA", &[("POP", 1.0)]);
        let indicators = IndicatorTable::new(vec![
            ind_record("POP", "17031", "", 2020, 100.0),
            // Subcounty DOWN missing.
        ]);
        let err = allocate_subcounty("17031", "DOWN", &record, &indicators, 2020, 200.0, 1.0)
            .unwrap_err();
        match err {
            Error::IndicatorMissing {
                fips, subcounty, ..
            } => {
                assert_eq!(fips, "17031");
                assert_eq!(subcounty, "DOWN");
            }
            other => panic!("expected IndicatorMissing, got {other:?}"),
        }
    }

    #[test]
    fn allocate_county_uses_year_selection_rule() {
        let record = alo_record("AAA", &[("POP", 1.0)]);
        let indicators = IndicatorTable::new(vec![
            ind_record("POP", "17000", "", 2010, 1000.0),
            ind_record("POP", "17000", "", 2020, 800.0),
            ind_record("POP", "17031", "", 2010, 300.0),
            ind_record("POP", "17031", "", 2020, 250.0),
        ]);
        let counties = vec![county("17031", true, false)];

        // Mid-year 2015: closest-earlier rule -> use 2010 records.
        let out =
            allocate_county("17000", &counties, &record, &indicators, 2015, 100.0, 1.0).unwrap();
        let expected = 100.0_f32 * (300.0_f32 / 1000.0); // 30.0
        assert!((out[0].population - expected).abs() < 1e-4);

        // After last year: still 2020.
        let out =
            allocate_county("17000", &counties, &record, &indicators, 2030, 100.0, 1.0).unwrap();
        let expected = 100.0_f32 * (250.0_f32 / 800.0);
        assert!((out[0].population - expected).abs() < 1e-4);
    }

    #[test]
    fn compute_alloc_factor_skips_nonpositive_parent_terms() {
        assert_eq!(
            compute_alloc_factor(&[0.0, 2.0], &[5.0, 4.0], &[0.5, 0.5]),
            (4.0 / 2.0) * 0.5
        );
        // Negative parent values also drop.
        assert_eq!(
            compute_alloc_factor(&[-1.0, 4.0], &[5.0, 8.0], &[0.5, 0.5]),
            (8.0 / 4.0) * 0.5
        );
    }

    #[test]
    fn left_justify_strips_leading_whitespace_only() {
        assert_eq!(left_justify("  abc"), "abc");
        assert_eq!(left_justify("abc"), "abc");
        assert_eq!(left_justify("abc  "), "abc  ");
        assert_eq!(left_justify("  abc  "), "abc  ");
        assert_eq!(left_justify(""), "");
    }
}

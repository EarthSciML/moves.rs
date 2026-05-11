//! County, state-to-county, and subcounty allocation logic.
//!
//! Cluster 5 of the seven functional groups (see
//! `ARCHITECTURE.md` § 2.5). Smallest calculation cluster; the
//! three routines share their inner ratio/sum loop via the
//! private [`allocation_factor`] helper.
//!
//! # Fortran source files this module ports
//!
//! | File | Lines | Role |
//! |---|---|---|
//! | `alocty.f` | 181 | County allocation |
//! | `alosta.f` | 176 | State-to-county allocation |
//! | `alosub.f` | 170 | Subcounty allocation |
//!
//! # The allocation arithmetic
//!
//! Each `.ALO` record (one per SCC) carries up to `MXCOEF` (3)
//! regression coefficients and parallel 3-character indicator
//! codes that sum to 1.0 (validated by [`crate::input::alo`]).
//!
//! Given a source-region population and a per-coefficient set of
//! indicator values for the source and target regions, the
//! allocation factor is
//!
//! ```text
//! factor = Σᵢ (target_i / source_i) · coeff_i        for source_i > 0
//! ```
//!
//! and the target-region population is `pop_year · factor`. The
//! growth value rides along unchanged. Coefficient slots with a
//! zero source indicator drop out (matches the `if( valsta(i) .GT.
//! 0 )` guards in `alocty.f` :138, `alosta.f` :133, `alosub.f`
//! :131).
//!
//! # The indicator lookup
//!
//! [`IndicatorIndex`] replaces the sequential `getind.f` march
//! through a sorted scratch file. The year-fallback rule is the
//! one `getind.f` settled on after the 2005 EPA change (line 31
//! of `getind.f`): use the value for the largest year ≤ the
//! episode year; if no record at or before, fall back to the
//! smallest year strictly after.

use std::collections::HashMap;

use crate::input::alo::AllocationRecord;
use crate::input::indicator::IndicatorRecord;
use crate::{Error, Result};

/// Subregion sentinel passed when looking up a state- or county-level
/// indicator. Matches the blank `subtmp = '     '` the Fortran
/// allocation routines set before calling `getind`.
pub const BLANK_SUBREGION: &str = "     ";

/// Year-indexed lookup over `(code, fips, subregion)` spatial
/// indicators. Replaces the sequential `getind.f` scan.
///
/// Records that share the same `(code, fips, subregion)` are
/// grouped and sorted ascending by year; [`Self::lookup`] returns
/// the value at the largest year ≤ the requested year, falling back
/// to the smallest year strictly after when no earlier record
/// exists. Matches `getind.f` :246–258, post-2005 (no
/// interpolation; closest earlier year wins).
#[derive(Debug, Default, Clone)]
pub struct IndicatorIndex {
    groups: HashMap<IndicatorKey, Vec<(i32, f32)>>,
}

type IndicatorKey = (String, String, String);

impl IndicatorIndex {
    /// Build an index from parsed indicator records.
    pub fn new<I>(records: I) -> Self
    where
        I: IntoIterator<Item = IndicatorRecord>,
    {
        let mut groups: HashMap<IndicatorKey, Vec<(i32, f32)>> = HashMap::new();
        for record in records {
            let year: i32 = record.year.trim().parse().unwrap_or(0);
            let key = normalize_key(&record.code, &record.fips, &record.subcounty);
            groups
                .entry(key)
                .or_default()
                .push((year, record.value as f32));
        }
        for values in groups.values_mut() {
            values.sort_by_key(|&(year, _)| year);
        }
        Self { groups }
    }

    /// Look up the indicator value for `(code, fips, subregion)` at
    /// `year`.
    ///
    /// Returns the value for the largest year ≤ `year`. If no record
    /// at or before `year` exists, falls back to the smallest year
    /// strictly after. Returns `None` when no record at all matches
    /// the `(code, fips, subregion)` key.
    pub fn lookup(&self, code: &str, fips: &str, subregion: &str, year: i32) -> Option<f32> {
        let key = normalize_key(code, fips, subregion);
        let entries = self.groups.get(&key)?;
        if entries.is_empty() {
            return None;
        }
        let mut best_le: Option<(i32, f32)> = None;
        let mut best_gt: Option<(i32, f32)> = None;
        for &(record_year, value) in entries {
            if record_year <= year {
                if best_le.map_or(true, |(by, _)| record_year > by) {
                    best_le = Some((record_year, value));
                }
            } else if best_gt.map_or(true, |(by, _)| record_year < by) {
                best_gt = Some((record_year, value));
            }
        }
        best_le.or(best_gt).map(|(_, value)| value)
    }

    /// Number of distinct `(code, fips, subregion)` groups in the
    /// index. Useful for sanity checks in callers.
    pub fn len(&self) -> usize {
        self.groups.len()
    }

    /// Whether the index has no records.
    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }
}

fn normalize_key(code: &str, fips: &str, subregion: &str) -> IndicatorKey {
    (
        code.trim().to_ascii_uppercase(),
        fips.trim().to_string(),
        subregion.trim().to_ascii_uppercase(),
    )
}

/// One element of a state-to-counties or nation-to-states target
/// list.
///
/// `include` mirrors the Fortran `lfipcd`/`lstacd` flags (was this
/// region selected by the user?). `has_specific_data` mirrors
/// `lctlev`/`lstlev` (does the region already carry its own
/// population record, in which case the parent record's
/// allocation skips it).
#[derive(Debug, Clone)]
pub struct Target<'a> {
    /// 5-character FIPS code for the target region.
    pub fips: &'a str,
    /// Whether the region is selected by the run.
    pub include: bool,
    /// Whether the region already has its own population record.
    pub has_specific_data: bool,
}

/// Allocated population and growth for a single target region.
///
/// `region` carries the FIPS code for the county
/// ([`allocate_state_to_counties`]) or state
/// ([`allocate_nation_to_states`]) the value was allocated to;
/// for [`allocate_county_to_subregion`] it carries the
/// left-justified 5-character subregion code, not a FIPS.
#[derive(Debug, Clone, PartialEq)]
pub struct Allocation {
    /// Identifier for the target region (FIPS or subregion code).
    pub region: String,
    /// Allocated population (`pop_year · factor`).
    pub population: f32,
    /// Growth value, passed through from the source record.
    pub growth: f32,
}

/// Allocate a state-level population/growth across the state's
/// counties. Ports `alocty.f`.
///
/// `state_fips` is the source region (e.g. `"17000"` for Illinois,
/// taken from `regncd(icurec)(1:5)` in the Fortran caller).
/// `targets` lists every county the state-record covers; entries
/// with `include == false` or `has_specific_data == true` are
/// skipped (matches `alocty.f` :116–117). The returned vector
/// contains one entry per non-skipped county.
pub fn allocate_state_to_counties(
    rule: &AllocationRecord,
    indicators: &IndicatorIndex,
    state_fips: &str,
    targets: &[Target<'_>],
    year: i32,
    pop_year: f32,
    growth: f32,
) -> Result<Vec<Allocation>> {
    let state_values = collect_indicators(rule, indicators, state_fips, BLANK_SUBREGION, year)?;

    let mut out = Vec::with_capacity(targets.len());
    for target in targets {
        if !target.include || target.has_specific_data {
            continue;
        }
        let county_values =
            collect_indicators(rule, indicators, target.fips, BLANK_SUBREGION, year)?;
        let factor = allocation_factor(&state_values, &county_values, &rule.coefficients);
        out.push(Allocation {
            region: target.fips.to_string(),
            population: pop_year * factor,
            growth,
        });
    }
    Ok(out)
}

/// Allocate a national population/growth across states. Ports
/// `alosta.f`.
///
/// `nation_fips` is the source region (e.g. `"00000"`, taken from
/// `regncd(icurec)(1:5)` in the Fortran caller). `targets` lists
/// every state in the run; entries with `include == false` or
/// `has_specific_data == true` are skipped (matches `alosta.f`
/// :111–112).
pub fn allocate_nation_to_states(
    rule: &AllocationRecord,
    indicators: &IndicatorIndex,
    nation_fips: &str,
    targets: &[Target<'_>],
    year: i32,
    pop_year: f32,
    growth: f32,
) -> Result<Vec<Allocation>> {
    let nation_values = collect_indicators(rule, indicators, nation_fips, BLANK_SUBREGION, year)?;

    let mut out = Vec::with_capacity(targets.len());
    for target in targets {
        if !target.include || target.has_specific_data {
            continue;
        }
        let state_values =
            collect_indicators(rule, indicators, target.fips, BLANK_SUBREGION, year)?;
        let factor = allocation_factor(&nation_values, &state_values, &rule.coefficients);
        out.push(Allocation {
            region: target.fips.to_string(),
            population: pop_year * factor,
            growth,
        });
    }
    Ok(out)
}

/// Allocate a county-level population/growth to a single subregion.
/// Ports `alosub.f`.
///
/// `county_fips` is the source region (5-character FIPS).
/// `subregion` is the 5-character subcounty code; the Fortran
/// left-justifies it before lookup (`alosub.f` :115), which the
/// [`IndicatorIndex`] key normalisation handles.
pub fn allocate_county_to_subregion(
    rule: &AllocationRecord,
    indicators: &IndicatorIndex,
    county_fips: &str,
    subregion: &str,
    year: i32,
    pop_year: f32,
    growth: f32,
) -> Result<Allocation> {
    let county_values = collect_indicators(rule, indicators, county_fips, BLANK_SUBREGION, year)?;
    let sub_values = collect_indicators(rule, indicators, county_fips, subregion, year)?;
    let factor = allocation_factor(&county_values, &sub_values, &rule.coefficients);
    Ok(Allocation {
        region: subregion.trim().to_string(),
        population: pop_year * factor,
        growth,
    })
}

/// Sum indicator values for each coefficient slot of `rule` at
/// `(fips, subregion, year)`.
///
/// Matches the Fortran loop bodies (e.g. `alocty.f` :95–103):
/// initialise each slot to 0, break out on the first blank `indcod`,
/// otherwise add the `getind` result to the slot. Slots past a
/// blank are left at zero in Fortran; here, the returned vector is
/// simply shorter than `coefficients.len()` and
/// [`allocation_factor`] treats missing slots as 0.
fn collect_indicators(
    rule: &AllocationRecord,
    indicators: &IndicatorIndex,
    fips: &str,
    subregion: &str,
    year: i32,
) -> Result<Vec<f32>> {
    let mut values = Vec::with_capacity(rule.coefficients.len());
    for code in &rule.indicator_codes {
        if code.trim().is_empty() {
            break;
        }
        let value = indicators
            .lookup(code, fips, subregion, year)
            .ok_or_else(|| Error::IndicatorMissing {
                code: code.trim().to_string(),
                fips: fips.trim().to_string(),
                subregion: subregion.trim().to_string(),
                year,
            })?;
        values.push(value);
    }
    Ok(values)
}

/// Compute `Σᵢ (target_i / source_i) · coeff_i` over slots with
/// `source_i > 0`. Slots missing from either vector default to 0,
/// matching Fortran's pre-initialised local arrays.
fn allocation_factor(source: &[f32], target: &[f32], coefficients: &[f32]) -> f32 {
    let mut factor = 0.0_f32;
    for (i, &coefficient) in coefficients.iter().enumerate() {
        let source_value = source.get(i).copied().unwrap_or(0.0);
        if source_value > 0.0 {
            let target_value = target.get(i).copied().unwrap_or(0.0);
            factor += (target_value / source_value) * coefficient;
        }
    }
    factor
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ind(code: &str, fips: &str, sub: &str, year: &str, value: f64) -> IndicatorRecord {
        IndicatorRecord {
            code: code.to_string(),
            fips: fips.to_string(),
            subcounty: sub.to_string(),
            year: year.to_string(),
            value,
        }
    }

    fn rule(coefficients: &[f32], codes: &[&str]) -> AllocationRecord {
        AllocationRecord {
            scc: "2270002003".to_string(),
            coefficients: coefficients.to_vec(),
            indicator_codes: codes.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn lookup_returns_closest_year_le_target() {
        let idx = IndicatorIndex::new(vec![
            ind("POP", "17000", "", "2010", 100.0),
            ind("POP", "17000", "", "2020", 200.0),
            ind("POP", "17000", "", "2030", 300.0),
        ]);
        // Target = 2025 → closest ≤ 2025 is 2020.
        assert_eq!(idx.lookup("POP", "17000", "", 2025), Some(200.0));
        assert_eq!(idx.lookup("POP", "17000", "", 2020), Some(200.0));
        // Target past every record → still uses the largest ≤.
        assert_eq!(idx.lookup("POP", "17000", "", 2050), Some(300.0));
    }

    #[test]
    fn lookup_falls_back_to_smallest_gt_when_no_le() {
        let idx = IndicatorIndex::new(vec![
            ind("POP", "17000", "", "2020", 200.0),
            ind("POP", "17000", "", "2030", 300.0),
        ]);
        assert_eq!(idx.lookup("POP", "17000", "", 2010), Some(200.0));
    }

    #[test]
    fn lookup_returns_none_for_unknown_key() {
        let idx = IndicatorIndex::new(vec![ind("POP", "17000", "", "2020", 200.0)]);
        assert_eq!(idx.lookup("POP", "06000", "", 2020), None);
        assert_eq!(idx.lookup("EMP", "17000", "", 2020), None);
    }

    #[test]
    fn lookup_normalises_case_and_whitespace() {
        let idx = IndicatorIndex::new(vec![ind("pop", "17000", "abc", "2020", 5.0)]);
        assert_eq!(idx.lookup("POP", "17000", "ABC", 2020), Some(5.0));
        assert_eq!(idx.lookup(" pop ", "17000", " abc ", 2020), Some(5.0));
    }

    #[test]
    fn lookup_treats_blank_and_empty_subregion_alike() {
        let idx = IndicatorIndex::new(vec![ind("POP", "17000", "", "2020", 5.0)]);
        assert_eq!(idx.lookup("POP", "17000", BLANK_SUBREGION, 2020), Some(5.0));
        assert_eq!(idx.lookup("POP", "17000", "     ", 2020), Some(5.0));
    }

    #[test]
    fn state_to_counties_basic_allocation() {
        // Single coefficient = 1.0, indicator POP.
        // State total POP = 100; counties have POP = 60 and 40.
        // pop_year = 100 → expect allocations 60 and 40.
        let idx = IndicatorIndex::new(vec![
            ind("POP", "17000", "", "2020", 100.0),
            ind("POP", "17031", "", "2020", 60.0),
            ind("POP", "17043", "", "2020", 40.0),
        ]);
        let r = rule(&[1.0], &["POP"]);
        let targets = [
            Target {
                fips: "17031",
                include: true,
                has_specific_data: false,
            },
            Target {
                fips: "17043",
                include: true,
                has_specific_data: false,
            },
        ];
        let result =
            allocate_state_to_counties(&r, &idx, "17000", &targets, 2020, 100.0, 1.5).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].region, "17031");
        assert!((result[0].population - 60.0).abs() < 1e-5);
        assert!((result[0].growth - 1.5).abs() < 1e-5);
        assert_eq!(result[1].region, "17043");
        assert!((result[1].population - 40.0).abs() < 1e-5);
    }

    #[test]
    fn state_to_counties_multi_coefficient_blends() {
        // 0.7·POP + 0.3·EMP. State POP=100, EMP=50. County POP=80,
        // EMP=30 → factor = 0.7·0.8 + 0.3·0.6 = 0.74.
        let idx = IndicatorIndex::new(vec![
            ind("POP", "17000", "", "2020", 100.0),
            ind("EMP", "17000", "", "2020", 50.0),
            ind("POP", "17031", "", "2020", 80.0),
            ind("EMP", "17031", "", "2020", 30.0),
        ]);
        let r = rule(&[0.7, 0.3], &["POP", "EMP"]);
        let targets = [Target {
            fips: "17031",
            include: true,
            has_specific_data: false,
        }];
        let result =
            allocate_state_to_counties(&r, &idx, "17000", &targets, 2020, 1000.0, 1.0).unwrap();
        assert_eq!(result.len(), 1);
        assert!((result[0].population - 740.0).abs() < 1e-3);
    }

    #[test]
    fn state_to_counties_drops_zero_source_coefficient() {
        // POP source = 0 → that coefficient is dropped (matches
        // `if( valsta(i) .GT. 0 )` in alocty.f :138). Remaining EMP
        // coefficient (0.3) carries the load:
        // factor = 0.3 · (30 / 50) = 0.18.
        let idx = IndicatorIndex::new(vec![
            ind("POP", "17000", "", "2020", 0.0),
            ind("EMP", "17000", "", "2020", 50.0),
            ind("POP", "17031", "", "2020", 1000.0),
            ind("EMP", "17031", "", "2020", 30.0),
        ]);
        let r = rule(&[0.7, 0.3], &["POP", "EMP"]);
        let targets = [Target {
            fips: "17031",
            include: true,
            has_specific_data: false,
        }];
        let result =
            allocate_state_to_counties(&r, &idx, "17000", &targets, 2020, 1000.0, 1.0).unwrap();
        assert!((result[0].population - 180.0).abs() < 1e-3);
    }

    #[test]
    fn state_to_counties_skips_unselected_and_specific() {
        let idx = IndicatorIndex::new(vec![
            ind("POP", "17000", "", "2020", 100.0),
            ind("POP", "17031", "", "2020", 60.0),
            ind("POP", "17043", "", "2020", 30.0),
            ind("POP", "17097", "", "2020", 10.0),
        ]);
        let r = rule(&[1.0], &["POP"]);
        let targets = [
            // Selected, no specific: should appear.
            Target {
                fips: "17031",
                include: true,
                has_specific_data: false,
            },
            // Has specific data: skip.
            Target {
                fips: "17043",
                include: true,
                has_specific_data: true,
            },
            // Not selected: skip.
            Target {
                fips: "17097",
                include: false,
                has_specific_data: false,
            },
        ];
        let result =
            allocate_state_to_counties(&r, &idx, "17000", &targets, 2020, 100.0, 1.0).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].region, "17031");
    }

    #[test]
    fn state_to_counties_errors_on_missing_indicator() {
        let idx = IndicatorIndex::new(vec![
            ind("POP", "17000", "", "2020", 100.0),
            // Missing POP for 17031.
        ]);
        let r = rule(&[1.0], &["POP"]);
        let targets = [Target {
            fips: "17031",
            include: true,
            has_specific_data: false,
        }];
        let err =
            allocate_state_to_counties(&r, &idx, "17000", &targets, 2020, 100.0, 1.0).unwrap_err();
        match err {
            Error::IndicatorMissing {
                code, fips, year, ..
            } => {
                assert_eq!(code, "POP");
                assert_eq!(fips, "17031");
                assert_eq!(year, 2020);
            }
            other => panic!("expected IndicatorMissing, got {other:?}"),
        }
    }

    #[test]
    fn state_to_counties_skips_indicator_lookup_for_skipped_targets() {
        // No POP record for 17043, but target is excluded → no lookup,
        // no error.
        let idx = IndicatorIndex::new(vec![
            ind("POP", "17000", "", "2020", 100.0),
            ind("POP", "17031", "", "2020", 60.0),
        ]);
        let r = rule(&[1.0], &["POP"]);
        let targets = [
            Target {
                fips: "17031",
                include: true,
                has_specific_data: false,
            },
            Target {
                fips: "17043",
                include: true,
                has_specific_data: true,
            },
        ];
        let result =
            allocate_state_to_counties(&r, &idx, "17000", &targets, 2020, 100.0, 1.0).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].region, "17031");
    }

    #[test]
    fn nation_to_states_basic_allocation() {
        let idx = IndicatorIndex::new(vec![
            ind("POP", "00000", "", "2020", 1000.0),
            ind("POP", "17000", "", "2020", 200.0),
            ind("POP", "06000", "", "2020", 300.0),
        ]);
        let r = rule(&[1.0], &["POP"]);
        let targets = [
            Target {
                fips: "17000",
                include: true,
                has_specific_data: false,
            },
            Target {
                fips: "06000",
                include: true,
                has_specific_data: false,
            },
        ];
        let result =
            allocate_nation_to_states(&r, &idx, "00000", &targets, 2020, 1000.0, 1.0).unwrap();
        assert_eq!(result.len(), 2);
        assert!((result[0].population - 200.0).abs() < 1e-3);
        assert!((result[1].population - 300.0).abs() < 1e-3);
    }

    #[test]
    fn county_to_subregion_basic_allocation() {
        // County 17031 has POP=100 total; subregion "ABCDE" has POP=25.
        // pop_year=400 → 400 · (25/100) = 100.
        let idx = IndicatorIndex::new(vec![
            ind("POP", "17031", "", "2020", 100.0),
            ind("POP", "17031", "ABCDE", "2020", 25.0),
        ]);
        let r = rule(&[1.0], &["POP"]);
        let result =
            allocate_county_to_subregion(&r, &idx, "17031", "ABCDE", 2020, 400.0, 2.0).unwrap();
        assert!((result.population - 100.0).abs() < 1e-3);
        assert!((result.growth - 2.0).abs() < 1e-5);
        assert_eq!(result.region, "ABCDE");
    }

    #[test]
    fn county_to_subregion_errors_on_missing_subregion() {
        let idx = IndicatorIndex::new(vec![
            ind("POP", "17031", "", "2020", 100.0),
            // No record for subregion ABCDE.
        ]);
        let r = rule(&[1.0], &["POP"]);
        let err =
            allocate_county_to_subregion(&r, &idx, "17031", "ABCDE", 2020, 400.0, 1.0).unwrap_err();
        match err {
            Error::IndicatorMissing { subregion, .. } => {
                assert_eq!(subregion, "ABCDE");
            }
            other => panic!("expected IndicatorMissing, got {other:?}"),
        }
    }

    #[test]
    fn allocation_factor_with_blank_trailing_indicator_uses_only_filled_slots() {
        // Rule has one filled coefficient slot; trailing indicator
        // code is blank, so lookup short-circuits and the blank
        // slot is treated as zero.
        let idx = IndicatorIndex::new(vec![
            ind("POP", "17000", "", "2020", 100.0),
            ind("POP", "17031", "", "2020", 50.0),
        ]);
        let mut r = rule(&[1.0], &["POP"]);
        r.indicator_codes.push("   ".to_string());
        let targets = [Target {
            fips: "17031",
            include: true,
            has_specific_data: false,
        }];
        let result =
            allocate_state_to_counties(&r, &idx, "17000", &targets, 2020, 100.0, 1.0).unwrap();
        assert!((result[0].population - 50.0).abs() < 1e-3);
    }

    #[test]
    fn year_fallback_picks_earlier_record_through_allocation() {
        // Indicators stored only for 2010; target year is 2030 → use 2010.
        let idx = IndicatorIndex::new(vec![
            ind("POP", "17000", "", "2010", 100.0),
            ind("POP", "17031", "", "2010", 40.0),
        ]);
        let r = rule(&[1.0], &["POP"]);
        let targets = [Target {
            fips: "17031",
            include: true,
            has_specific_data: false,
        }];
        let result =
            allocate_state_to_counties(&r, &idx, "17000", &targets, 2030, 100.0, 1.0).unwrap();
        assert!((result[0].population - 40.0).abs() < 1e-3);
    }
}

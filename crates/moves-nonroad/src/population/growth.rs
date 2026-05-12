//! Growth retrieval and factor calculation — `getgrw.f` (200
//! lines) and `grwfac.f` (281 lines).
//!
//! Two responsibilities:
//!
//! 1. [`select_for_indicator`] — `getgrw.f` equivalent. Filters a
//!    sorted growth-indicator list to records carrying the requested
//!    indicator code.
//! 2. [`growth_factor`] — `grwfac.f` equivalent. Computes the
//!    growth factor between a base population year and a target
//!    growth year for a given FIPS region, by interpolating or
//!    extrapolating the indicator-vs-year curve.
//!
//! # Sort-key contract for [`select_for_indicator`]
//!
//! Input records must be sorted by:
//!
//! 1. Indicator code (4 chars);
//! 2. FIPS region (5 chars);
//! 3. Year (ascending).
//!
//! This is the sort `rdgrow.f` writes to its scratch file
//! (`grwdir.txt` then `IOSGRW`). The Fortran `getgrw.f` walks
//! `IOSGRW`, skipping records whose indicator is lexically less
//! than the requested one and stopping when it exceeds the
//! requested one. The Rust port performs the same range scan on
//! the in-memory slice.
//!
//! # `growth_factor` algorithm
//!
//! `grwfac.f` searches `fipgrw` for the *most specific* FIPS match
//! against `infips`, in priority order:
//!
//! - county (`infips` exact, e.g. `06037`);
//! - state (`infips[0..2] || "000"`, e.g. `06000`);
//! - national (`00000`).
//!
//! It then finds the year range `[ibeg, iend]` for that FIPS and
//! either returns the matching-year value, interpolates between
//! the two bracketing years (forward differencing), or
//! extrapolates beyond the range using the boundary year-to-year
//! change. Special cases:
//!
//! - If `growth_year == base_year`, the factor is 0 (no growth).
//! - If both interpolated indicators are zero, the factor is 0.
//! - If only the base-year indicator is zero, it is bumped up to
//!   `MINGRWIND` to avoid divide-by-zero, and a warning is
//!   produced (returned alongside the factor — the Fortran source
//!   writes to `IOWMSG` and bumps the global `nwarn` counter).
//!
//! The factor formula matches `grwfac.f` :244–245:
//!
//! ```text
//! factor = (growthyearind - baseyearind) / (baseyearind * (growth_year - base_year))
//! ```
//!
//! This is the *annualized* year-to-year fractional change between
//! the base and growth years (not the cumulative ratio).

use crate::common::consts::MINGRWIND;
use crate::{Error, Result};

/// One parsed growth-indicator record (the in-memory analogue of
/// `fipgrw`/`subgrw`/`iyrgrw`/`valgrw` in `nonrdgrw.inc`).
#[derive(Debug, Clone, PartialEq)]
pub struct GrowthIndicatorRecord {
    /// 4-character indicator code.
    pub indicator: String,
    /// 5-character FIPS region (`00000` for national, `XX000` for
    /// state, `XXYYY` for county).
    pub fips: String,
    /// 5-character subregion (often blank when not subcounty).
    pub subregion: String,
    /// Calendar year.
    pub year: i32,
    /// Indicator value.
    pub value: f32,
}

/// Diagnostic produced by [`growth_factor`] when it had to clamp
/// the base-year indicator to [`MINGRWIND`] to avoid divide-by-zero.
///
/// The Fortran source writes this as a warning to `IOWMSG` and
/// bumps the global `nwarn` counter. The Rust port surfaces the
/// detail so the caller can route it to a structured log /
/// `WarningMessage` accumulator.
#[derive(Debug, Clone, PartialEq)]
pub struct GrowthFactorWarning {
    /// Base population year that was passed in.
    pub base_year: i32,
    /// The interpolated base-year indicator value (zero, in this case).
    pub base_indicator: f32,
    /// The adjusted indicator used to avoid divide-by-zero
    /// ([`MINGRWIND`]).
    pub adjusted_base_indicator: f32,
    /// Growth (target) year that was passed in.
    pub growth_year: i32,
    /// Interpolated growth-year indicator value.
    pub growth_indicator: f32,
}

/// Result of a successful [`growth_factor`] call.
#[derive(Debug, Clone, PartialEq)]
pub struct GrowthFactor {
    /// The annualized year-to-year fractional change between base
    /// and growth years.
    pub factor: f32,
    /// Indicator value used for the base population year.
    pub base_indicator: f32,
    /// Indicator value used for the growth (target) year.
    pub growth_indicator: f32,
    /// Set when the base indicator was bumped to [`MINGRWIND`].
    pub warning: Option<GrowthFactorWarning>,
}

/// Filter to records matching `indicator` — `getgrw.f` equivalent.
///
/// Returns the contiguous slice of records whose `indicator` field
/// equals the requested code. Records on either side of that range
/// are dropped. See module-level docs for the sort-key contract.
///
/// Returned records preserve input order (so consumers can use the
/// `(fips, year)` ordering for the [`growth_factor`] lookup
/// directly).
pub fn select_for_indicator<'a>(
    records: &'a [GrowthIndicatorRecord],
    indicator: &str,
) -> Vec<&'a GrowthIndicatorRecord> {
    let needle = indicator.trim_end();
    records
        .iter()
        .filter(|r| r.indicator.trim_end() == needle)
        .collect()
}

/// Compute the growth factor between `base_year` and `growth_year`
/// for the supplied FIPS code — `grwfac.f` equivalent.
///
/// `records` must already be filtered to a single indicator code
/// (use [`select_for_indicator`] or pre-filter equivalently) and
/// sorted by `(fips, year)`. See module-level docs for the
/// algorithm.
///
/// Returns:
/// - `Ok(GrowthFactor { factor, .. })` on success;
/// - `Err(Error::Config { .. })` when there is no growth data for
///   the FIPS at the requested specificity, or only a single year
///   is available (cannot compute a slope).
pub fn growth_factor(
    records: &[&GrowthIndicatorRecord],
    base_year: i32,
    growth_year: i32,
    fips: &str,
) -> Result<GrowthFactor> {
    // --- if episode year equals base year, no growth (grwfac.f :95) ---
    if growth_year == base_year {
        return Ok(GrowthFactor {
            factor: 0.0,
            base_indicator: 0.0,
            growth_indicator: 0.0,
            warning: None,
        });
    }

    let st_fips = format!("{}000", &fips.get(..2).unwrap_or("00"));

    // --- loop looking for national, state, or county match — keep
    //     the last (most-specific) hit (grwfac.f :115–119) ---
    let mut iend: Option<usize> = None;
    for (i, r) in records.iter().enumerate() {
        if r.fips == "00000" || r.fips == st_fips || r.fips == fips {
            iend = Some(i);
        }
    }
    let iend = match iend {
        Some(i) => i,
        None => {
            return Err(Error::Config(format!(
                "could not find any valid growth data for FIPS {} growth_year {}",
                fips, growth_year
            )));
        }
    };

    // --- back up to the first occurrence of records.fips[iend] (grwfac.f :128–133) ---
    let target_fips = &records[iend].fips;
    let mut ibeg = 0usize;
    if iend > 0 {
        for i in (0..iend).rev() {
            if records[i].fips != *target_fips {
                ibeg = i + 1;
                break;
            }
            if i == 0 {
                ibeg = 0;
            }
        }
    }

    // --- need at least two years to compute a slope (grwfac.f :143) ---
    if ibeg == iend || records[ibeg].year == records[iend].year {
        return Err(Error::Config(format!(
            "growth indicator data for FIPS {} growth_year {} has fewer than two distinct years; \
             cannot compute growth factor",
            fips, growth_year
        )));
    }

    // --- boundary year-to-year change for extrapolation (grwfac.f :148–157) ---
    let lower_change = if base_year < records[ibeg].year || growth_year < records[ibeg].year {
        Some(
            (records[ibeg + 1].value - records[ibeg].value)
                / (records[ibeg + 1].year - records[ibeg].year) as f32,
        )
    } else {
        None
    };
    let upper_change = if base_year > records[iend].year || growth_year > records[iend].year {
        Some(
            (records[iend].value - records[iend - 1].value)
                / (records[iend].year - records[iend - 1].year) as f32,
        )
    } else {
        None
    };

    let base_indicator =
        interpolate_indicator(records, ibeg, iend, base_year, lower_change, upper_change);
    let growth_indicator =
        interpolate_indicator(records, ibeg, iend, growth_year, lower_change, upper_change);

    // --- if both indicators are zero, default growth factor of zero (grwfac.f :224) ---
    if base_indicator == 0.0 && growth_indicator == 0.0 {
        return Ok(GrowthFactor {
            factor: 0.0,
            base_indicator: 0.0,
            growth_indicator: 0.0,
            warning: None,
        });
    }

    // --- bump base-year indicator to MINGRWIND when it is zero
    //     (grwfac.f :230–241) ---
    let mut warning = None;
    let effective_base = if base_indicator == 0.0 {
        let adjusted = MINGRWIND.max(base_indicator);
        warning = Some(GrowthFactorWarning {
            base_year,
            base_indicator,
            adjusted_base_indicator: adjusted,
            growth_year,
            growth_indicator,
        });
        adjusted
    } else {
        base_indicator
    };

    let factor =
        (growth_indicator - effective_base) / (effective_base * (growth_year - base_year) as f32);

    Ok(GrowthFactor {
        factor,
        base_indicator,
        growth_indicator,
        warning,
    })
}

/// Interpolate / extrapolate the indicator value at `year` against
/// the slice `records[ibeg..=iend]`. Mirrors the per-year branch
/// `grwfac.f` runs twice (once for base, once for growth).
fn interpolate_indicator(
    records: &[&GrowthIndicatorRecord],
    ibeg: usize,
    iend: usize,
    year: i32,
    lower_change: Option<f32>,
    upper_change: Option<f32>,
) -> f32 {
    if year < records[ibeg].year {
        // extrapolate backward (grwfac.f :160–164)
        let slope = lower_change.expect("lower_change is Some when year < records[ibeg].year");
        (records[ibeg].value + slope * (year - records[ibeg].year) as f32).max(0.0)
    } else if year > records[iend].year {
        // extrapolate forward (grwfac.f :165–169)
        let slope = upper_change.expect("upper_change is Some when year > records[iend].year");
        (records[iend].value + slope * (year - records[iend].year) as f32).max(0.0)
    } else if year == records[iend].year {
        // matched last (grwfac.f :170–172)
        records[iend].value
    } else {
        // interior — find bracket and interpolate (grwfac.f :174–188)
        for i in ibeg..iend {
            if year == records[i].year {
                return records[i].value;
            } else if year < records[i + 1].year {
                let slope = (records[i + 1].value - records[i].value)
                    / (records[i + 1].year - records[i].year) as f32;
                return records[i].value + slope * (year - records[i].year) as f32;
            }
        }
        // Unreachable given ibeg <= iend invariants; fall back to last.
        records[iend].value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(indicator: &str, fips: &str, year: i32, value: f32) -> GrowthIndicatorRecord {
        GrowthIndicatorRecord {
            indicator: indicator.to_string(),
            fips: fips.to_string(),
            subregion: "     ".to_string(),
            year,
            value,
        }
    }

    fn refs<'a>(v: &'a [GrowthIndicatorRecord]) -> Vec<&'a GrowthIndicatorRecord> {
        v.iter().collect()
    }

    #[test]
    fn select_filters_by_indicator() {
        let records = vec![
            rec("POP", "06000", 2000, 100.0),
            rec("POP", "06000", 2010, 150.0),
            rec("HHS", "06000", 2000, 50.0),
            rec("HHS", "06000", 2010, 75.0),
        ];
        let selected = select_for_indicator(&records, "POP");
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].year, 2000);
        assert_eq!(selected[1].year, 2010);
    }

    #[test]
    fn select_ignores_unknown_indicator() {
        let records = vec![rec("POP", "06000", 2000, 100.0)];
        assert!(select_for_indicator(&records, "NOPE").is_empty());
    }

    #[test]
    fn equal_years_yields_zero_factor() {
        let records = vec![
            rec("POP", "06000", 2010, 100.0),
            rec("POP", "06000", 2020, 120.0),
        ];
        let r = growth_factor(&refs(&records), 2015, 2015, "06000").unwrap();
        assert_eq!(r.factor, 0.0);
        assert!(r.warning.is_none());
    }

    #[test]
    fn interpolation_within_range() {
        // Base 2010 -> 100; growth 2020 -> 120. Target base=2010, growth=2020.
        // factor = (120 - 100) / (100 * (2020 - 2010)) = 0.02 per year.
        let records = vec![
            rec("POP", "06000", 2010, 100.0),
            rec("POP", "06000", 2020, 120.0),
        ];
        let r = growth_factor(&refs(&records), 2010, 2020, "06000").unwrap();
        assert!((r.factor - 0.02).abs() < 1e-6);
        assert_eq!(r.base_indicator, 100.0);
        assert_eq!(r.growth_indicator, 120.0);
    }

    #[test]
    fn interpolation_between_years() {
        // Linear between 2010=100 and 2020=120 → 2015 = 110.
        let records = vec![
            rec("POP", "06000", 2010, 100.0),
            rec("POP", "06000", 2020, 120.0),
        ];
        let r = growth_factor(&refs(&records), 2015, 2020, "06000").unwrap();
        assert!((r.base_indicator - 110.0).abs() < 1e-4);
        assert_eq!(r.growth_indicator, 120.0);
        // factor = (120 - 110) / (110 * (2020 - 2015)) = 10 / 550 ≈ 0.01818
        assert!((r.factor - 10.0 / 550.0).abs() < 1e-6);
    }

    #[test]
    fn county_match_wins_over_state_and_national() {
        // National POP for fips 00000: 100→120 over 2010→2020
        // State (06000): 200→240
        // County (06037): 300→360
        // Lookup for 06037 should use county data.
        let records = vec![
            rec("POP", "00000", 2010, 100.0),
            rec("POP", "00000", 2020, 120.0),
            rec("POP", "06000", 2010, 200.0),
            rec("POP", "06000", 2020, 240.0),
            rec("POP", "06037", 2010, 300.0),
            rec("POP", "06037", 2020, 360.0),
        ];
        let r = growth_factor(&refs(&records), 2010, 2020, "06037").unwrap();
        assert_eq!(r.base_indicator, 300.0);
        assert_eq!(r.growth_indicator, 360.0);
    }

    #[test]
    fn state_fallback_when_no_county() {
        let records = vec![
            rec("POP", "00000", 2010, 100.0),
            rec("POP", "00000", 2020, 120.0),
            rec("POP", "06000", 2010, 200.0),
            rec("POP", "06000", 2020, 240.0),
        ];
        let r = growth_factor(&refs(&records), 2010, 2020, "06037").unwrap();
        assert_eq!(r.base_indicator, 200.0);
        assert_eq!(r.growth_indicator, 240.0);
    }

    #[test]
    fn national_fallback_when_no_state_or_county() {
        let records = vec![
            rec("POP", "00000", 2010, 100.0),
            rec("POP", "00000", 2020, 120.0),
        ];
        let r = growth_factor(&refs(&records), 2010, 2020, "06037").unwrap();
        assert_eq!(r.base_indicator, 100.0);
        assert_eq!(r.growth_indicator, 120.0);
    }

    #[test]
    fn forward_extrapolation_clamps_at_zero() {
        // Linear extrapolation 2010→2020: 100→50 (slope -5/yr).
        // base=2020 → 50; growth=2030 → 0 (clamped, not -5).
        let records = vec![
            rec("POP", "06000", 2010, 100.0),
            rec("POP", "06000", 2020, 50.0),
        ];
        let r = growth_factor(&refs(&records), 2020, 2030, "06000").unwrap();
        assert_eq!(r.base_indicator, 50.0);
        assert_eq!(r.growth_indicator, 0.0); // clamped via max(0., ...)
    }

    #[test]
    fn backward_extrapolation_clamps_at_zero() {
        // 2010→2020: 100→150, slope +5/yr → backward to 1980 gives -50,
        // clamped to 0.
        let records = vec![
            rec("POP", "06000", 2010, 100.0),
            rec("POP", "06000", 2020, 150.0),
        ];
        let r = growth_factor(&refs(&records), 1980, 2010, "06000").unwrap();
        assert_eq!(r.base_indicator, 0.0); // clamped
        assert_eq!(r.growth_indicator, 100.0);
        // base==0 and growth!=0 → warning + adjustment
        assert!(r.warning.is_some());
    }

    #[test]
    fn missing_fips_data_errors() {
        let records = vec![
            rec("POP", "06000", 2010, 100.0),
            rec("POP", "06000", 2020, 120.0),
        ];
        let err = growth_factor(&refs(&records), 2010, 2020, "17031").unwrap_err();
        match err {
            Error::Config(msg) => assert!(msg.contains("17031")),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn single_year_errors() {
        let records = vec![rec("POP", "06000", 2010, 100.0)];
        let err = growth_factor(&refs(&records), 2010, 2020, "06000").unwrap_err();
        match err {
            Error::Config(msg) => assert!(msg.contains("two distinct years")),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn both_indicators_zero_yields_zero_factor() {
        let records = vec![
            rec("POP", "06000", 2010, 0.0),
            rec("POP", "06000", 2020, 0.0),
        ];
        let r = growth_factor(&refs(&records), 2010, 2020, "06000").unwrap();
        assert_eq!(r.factor, 0.0);
        assert!(r.warning.is_none());
    }

    #[test]
    fn zero_base_indicator_produces_warning() {
        let records = vec![
            rec("POP", "06000", 2010, 0.0),
            rec("POP", "06000", 2020, 50.0),
        ];
        let r = growth_factor(&refs(&records), 2010, 2020, "06000").unwrap();
        assert!(r.warning.is_some());
        let w = r.warning.unwrap();
        assert_eq!(w.base_indicator, 0.0);
        assert_eq!(w.adjusted_base_indicator, MINGRWIND);
        // factor = (50 - MINGRWIND) / (MINGRWIND * 10)
        let expected = (50.0 - MINGRWIND) / (MINGRWIND * 10.0);
        assert!((r.factor - expected).abs() < 1.0); // large positive; precise sanity
    }

    #[test]
    fn matched_last_year_returns_endpoint_value() {
        let records = vec![
            rec("POP", "06000", 2010, 100.0),
            rec("POP", "06000", 2020, 200.0),
        ];
        let r = growth_factor(&refs(&records), 2010, 2020, "06000").unwrap();
        assert_eq!(r.growth_indicator, 200.0);
    }
}

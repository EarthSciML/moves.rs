//! Age-distribution growth — `agedist.f` (193 lines).
//!
//! Grows the model-year fractions (age distribution) from the
//! base-population year to the growth year. When the growth year
//! precedes the base year, the base-year population is instead grown
//! backward; the age distribution is left untouched (the Fortran
//! source documents that there is no way to grow it backward).
//!
//! # Algorithm (forward growth, `agedist.f` :112–146)
//!
//! For each year between `base_year + 1` and `growth_year`
//! (inclusive):
//!
//! 1. Snapshot the current `mdyrfrc` array.
//! 2. Call the growth-factor callback for `(year - 1, year)`.
//! 3. If the growth factor is non-zero, clamp `totpop` up to
//!    [`MINGRWIND`] — `grwfac` performs the same clamp on its
//!    base-year indicator and the population must follow.
//! 4. `totpop = max(0, totpop * (1 + grwthfc))` (compound growth;
//!    the Fortran comment notes this is the simplified form of
//!    `totpop * (1 + grwthfc * (year2 - year1))` valid because
//!    `year2 = year1 + 1`).
//! 5. For ages 2..=MXAGYR:
//!    `mdyrfrc[age] = max(0, tmpfrc[age - 1] * (1 - yryrfrcscrp[age]))`
//!    accumulating their sum into `frcsum`.
//! 6. `mdyrfrc[1] = totpopfrc - frcsum` where
//!    `totpopfrc = totpop / base_population`.
//!
//! # Algorithm (backward growth, `agedist.f` :151–170)
//!
//! Single `grwfac` call from `base_year` back to `growth_year`,
//! then `baspop = max(0, baspop * (1 + (growth_year - base_year) *
//! grwthfc))`. Same `MINGRWIND` clamp applies when the growth
//! factor is non-zero.
//!
//! # Numerical-fidelity note
//!
//! The forward-growth loop is the per-year accumulator the
//! migration plan flags as the most likely source of numerical
//! divergence vs. the Windows Fortran reference (see Task 115/116).
//! The Rust port preserves Fortran's evaluation order exactly:
//! `tmpfrc` snapshot before any age update, age loop ascending,
//! `mdyrfrc[1]` written *after* the higher-age fractions have been
//! summed. All quantities stay in `f32` to match Fortran's `real*4`.

use crate::common::consts::{MINGRWIND, MXAGYR};
use crate::population::growth::{GrowthFactor, GrowthFactorWarning};
use crate::{Error, Result};

/// Output of [`age_distribution`].
#[derive(Debug, Clone, PartialEq)]
pub struct AgeDistributionResult {
    /// Base-year population. Equal to the input when
    /// `growth_year >= base_year`; otherwise grown backward.
    pub base_population: f32,
    /// Model-year fractions grown forward to `growth_year`. Equal
    /// to the input when `growth_year <= base_year`.
    pub mdyrfrc: Vec<f32>,
    /// Per-iteration warnings emitted by `grwfac` when it had to
    /// clamp the base-year indicator to [`MINGRWIND`]. The Fortran
    /// source surfaces these via `IOWMSG` and the global `nwarn`
    /// counter; the Rust port hands them back to the caller.
    pub warnings: Vec<GrowthFactorWarning>,
}

/// Grow the age distribution and/or base-year population from
/// `base_year` to `growth_year` — `agedist.f` equivalent.
///
/// `growth_fn` is the per-edge growth-factor callback. It receives
/// `(prev_year, next_year)` and must return the annualized
/// fractional change between those years (i.e. the `factor` field
/// of [`crate::population::growth::GrowthFactor`]). Typical use:
///
/// ```ignore
/// let growth_records = select_for_indicator(&records, indcod);
/// let res = age_distribution(
///     baspop,
///     &mdyrfrc,
///     ibaspopyr,
///     igrwthyr,
///     &yryrfrcscrp,
///     |y1, y2| growth_factor(&growth_records, y1, y2, infips),
/// )?;
/// ```
///
/// # Inputs
///
/// - `base_population`: base-year population. Adjusted in place
///   conceptually (returned in the result struct) when
///   `growth_year < base_year`.
/// - `mdyrfrc`: model-year fractions for the first full-scrappage
///   year. Must have length [`MXAGYR`].
/// - `yryrfrcscrp`: year-to-year fraction scrapped by age. Must
///   have length [`MXAGYR`].
///
/// # Errors
///
/// Forwards any [`Error`] returned by `growth_fn`. No other failure
/// modes — the Fortran source `goto 9999` only on `grwfac` error.
pub fn age_distribution<F>(
    base_population: f32,
    mdyrfrc: &[f32],
    base_year: i32,
    growth_year: i32,
    yryrfrcscrp: &[f32],
    mut growth_fn: F,
) -> Result<AgeDistributionResult>
where
    F: FnMut(i32, i32) -> Result<GrowthFactor>,
{
    if mdyrfrc.len() != MXAGYR {
        return Err(Error::Config(format!(
            "agedist: mdyrfrc must have length {MXAGYR}, got {}",
            mdyrfrc.len()
        )));
    }
    if yryrfrcscrp.len() != MXAGYR {
        return Err(Error::Config(format!(
            "agedist: yryrfrcscrp must have length {MXAGYR}, got {}",
            yryrfrcscrp.len()
        )));
    }

    let mut result_baspop = base_population;
    let mut mdyrfrc_out: Vec<f32> = mdyrfrc.to_vec();
    let mut warnings: Vec<GrowthFactorWarning> = Vec::new();

    if growth_year > base_year {
        // Forward growth (agedist.f :112–146).
        let mut totpop = base_population;
        for iyear in (base_year + 1)..=growth_year {
            // Snapshot current fractions (agedist.f :116–118).
            let tmpfrc: Vec<f32> = mdyrfrc_out.clone();

            // grwfac(iyear - 1, iyear).
            let gf = growth_fn(iyear - 1, iyear)?;
            let grwthfc = gf.factor;
            if let Some(w) = gf.warning {
                warnings.push(w);
            }

            // MINGRWIND clamp on totpop when growth factor non-zero
            // (agedist.f :124–131).
            if grwthfc != 0.0 {
                totpop = totpop.max(MINGRWIND);
            }

            // totpop = max(0, totpop * (1 + grwthfc)) (agedist.f :132).
            totpop = (totpop * (1.0 + grwthfc)).max(0.0);
            let totpopfrc = totpop / base_population;

            // Forward-shift fractions, accumulating frcsum
            // (agedist.f :139–143). Ages are 1-indexed in Fortran;
            // here index 0 corresponds to age 1.
            let mut frcsum: f32 = 0.0;
            for iage in 1..MXAGYR {
                let updated = (tmpfrc[iage - 1] * (1.0 - yryrfrcscrp[iage])).max(0.0);
                mdyrfrc_out[iage] = updated;
                frcsum += updated;
            }

            // New-year fraction is whatever total-population fraction
            // is left after the existing ages account for theirs
            // (agedist.f :144). May go negative if scrappage left
            // residual; Fortran does not clamp this slot.
            mdyrfrc_out[0] = totpopfrc - frcsum;
        }
    } else if growth_year < base_year {
        // Backward growth of the base population (agedist.f :151–170).
        let gf = growth_fn(base_year, growth_year)?;
        let grwthfc = gf.factor;
        if let Some(w) = gf.warning {
            warnings.push(w);
        }

        if grwthfc != 0.0 {
            result_baspop = result_baspop.max(MINGRWIND);
        }
        let delta_years = (growth_year - base_year) as f32;
        result_baspop = (result_baspop * (1.0 + delta_years * grwthfc)).max(0.0);
    }
    // growth_year == base_year: untouched (Fortran fall-through).

    Ok(AgeDistributionResult {
        base_population: result_baspop,
        mdyrfrc: mdyrfrc_out,
        warnings,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn zero_scrap() -> Vec<f32> {
        vec![0.0; MXAGYR]
    }

    fn init_mdyrfrc(values: &[f32]) -> Vec<f32> {
        let mut v = vec![0.0; MXAGYR];
        for (i, &x) in values.iter().enumerate() {
            v[i] = x;
        }
        v
    }

    fn const_growth(factor: f32) -> impl FnMut(i32, i32) -> Result<GrowthFactor> {
        move |_, _| {
            Ok(GrowthFactor {
                factor,
                base_indicator: 1.0,
                growth_indicator: 1.0 + factor,
                warning: None,
            })
        }
    }

    #[test]
    fn equal_years_leaves_inputs_untouched() {
        let mdyrfrc = init_mdyrfrc(&[0.1, 0.2, 0.3, 0.4]);
        let scrap = zero_scrap();
        let baspop = 1000.0;
        let res = age_distribution(baspop, &mdyrfrc, 2020, 2020, &scrap, |_, _| {
            panic!("growth_fn should not be called when years are equal")
        })
        .unwrap();
        assert_eq!(res.base_population, baspop);
        assert_eq!(res.mdyrfrc, mdyrfrc);
        assert!(res.warnings.is_empty());
    }

    #[test]
    fn backward_growth_adjusts_baspop_only() {
        let mdyrfrc = init_mdyrfrc(&[0.5, 0.5]);
        let scrap = zero_scrap();
        // grwfac(base=2025, growth=2020) returns factor=0.1
        // baspop = max(0, baspop * (1 + (2020-2025) * 0.1)) = baspop * 0.5
        let res = age_distribution(100.0, &mdyrfrc, 2025, 2020, &scrap, const_growth(0.1)).unwrap();
        assert!((res.base_population - 50.0).abs() < 1e-4);
        assert_eq!(res.mdyrfrc, mdyrfrc);
    }

    #[test]
    fn backward_growth_clamps_at_zero() {
        let mdyrfrc = init_mdyrfrc(&[0.5, 0.5]);
        let scrap = zero_scrap();
        // Large positive grwthfc with negative delta yields negative product → clamped.
        // baspop * (1 + (-10) * 0.5) = baspop * -4 → max(0, ...) = 0.
        let res = age_distribution(100.0, &mdyrfrc, 2030, 2020, &scrap, const_growth(0.5)).unwrap();
        assert_eq!(res.base_population, 0.0);
    }

    #[test]
    fn backward_growth_bumps_below_mingrwind_when_factor_nonzero() {
        let mdyrfrc = init_mdyrfrc(&[0.5, 0.5]);
        let scrap = zero_scrap();
        // baspop tiny → clamped to MINGRWIND first.
        let tiny = MINGRWIND / 10.0;
        let res = age_distribution(tiny, &mdyrfrc, 2025, 2020, &scrap, const_growth(0.1)).unwrap();
        // base_population is bumped to MINGRWIND, then scaled by (1 - 0.5) = 0.5.
        let expected = MINGRWIND * (1.0 + (2020.0 - 2025.0) * 0.1);
        assert!((res.base_population - expected.max(0.0)).abs() < 1e-9);
    }

    #[test]
    fn backward_growth_zero_factor_skips_clamp() {
        let mdyrfrc = init_mdyrfrc(&[0.5, 0.5]);
        let scrap = zero_scrap();
        let tiny = MINGRWIND / 10.0;
        // Zero factor → no clamp → baspop unchanged from initial small value.
        let res = age_distribution(tiny, &mdyrfrc, 2025, 2020, &scrap, const_growth(0.0)).unwrap();
        assert_eq!(res.base_population, tiny);
    }

    #[test]
    fn forward_growth_single_year_zero_scrappage() {
        // One year forward, zero scrappage, growth factor 0.10.
        // totpop: 100 → 100 * 1.10 = 110.
        // totpopfrc: 110 / 100 = 1.10.
        // For each age `iage` in 2..MXAGYR (1-based Fortran): mdyrfrc(iage)
        // gets tmpfrc(iage - 1) — i.e. ages shift up by one slot. With initial
        // mdyrfrc = [0.5, 0.5, 0, ...] (Rust 0-indexed: ages 1..2 in Fortran):
        //   mdyrfrc[1] = tmpfrc[0] = 0.5   (Fortran age 2 ← age 1)
        //   mdyrfrc[2] = tmpfrc[1] = 0.5   (Fortran age 3 ← age 2)
        // frcsum = 1.0; mdyrfrc[0] = totpopfrc - frcsum = 1.10 - 1.0 = 0.10.
        let mdyrfrc = init_mdyrfrc(&[0.5, 0.5]);
        let scrap = zero_scrap();
        let res =
            age_distribution(100.0, &mdyrfrc, 2020, 2021, &scrap, const_growth(0.10)).unwrap();
        assert_eq!(res.base_population, 100.0);
        assert!((res.mdyrfrc[0] - 0.10).abs() < 1e-6);
        assert!((res.mdyrfrc[1] - 0.50).abs() < 1e-6);
        assert!((res.mdyrfrc[2] - 0.50).abs() < 1e-6);
        assert_eq!(res.mdyrfrc[3], 0.0);
    }

    #[test]
    fn forward_growth_clamps_population_at_zero() {
        // Large negative growth factor → totpop * (1 + grwthfc) goes negative.
        // After max(0, ...), totpop = 0, so totpopfrc = 0.
        // mdyrfrc[0] = 0 - frcsum (which is non-negative); may be negative.
        let mdyrfrc = init_mdyrfrc(&[0.5, 0.5]);
        let scrap = zero_scrap();
        let res =
            age_distribution(100.0, &mdyrfrc, 2020, 2021, &scrap, const_growth(-2.0)).unwrap();
        // After the shift: mdyrfrc[1] = tmpfrc[0] = 0.5, mdyrfrc[2] = tmpfrc[1] = 0.5,
        // frcsum = 1.0. mdyrfrc[0] = 0 - 1.0 = -1.0 (Fortran does not clamp the
        // youngest-age slot).
        assert!((res.mdyrfrc[0] + 1.0).abs() < 1e-6);
        assert!((res.mdyrfrc[1] - 0.5).abs() < 1e-6);
        assert!((res.mdyrfrc[2] - 0.5).abs() < 1e-6);
    }

    #[test]
    fn forward_growth_scrappage_drops_age_fractions() {
        // 100% scrappage at every age → all shifted fractions go to zero.
        let mdyrfrc = init_mdyrfrc(&[0.5, 0.5]);
        let mut scrap = vec![0.0; MXAGYR];
        for s in scrap.iter_mut() {
            *s = 1.0;
        }
        // grwthfc 0, totpop=100, totpopfrc=1.0.
        // After shift+scrap, all higher ages are zero; mdyrfrc[0] = 1.0 - 0 = 1.0.
        let res = age_distribution(100.0, &mdyrfrc, 2020, 2021, &scrap, const_growth(0.0)).unwrap();
        assert!((res.mdyrfrc[0] - 1.0).abs() < 1e-6);
        for v in &res.mdyrfrc[1..] {
            assert_eq!(*v, 0.0);
        }
    }

    #[test]
    fn forward_growth_propagates_warning() {
        let mdyrfrc = init_mdyrfrc(&[0.5, 0.5]);
        let scrap = zero_scrap();
        let warning = GrowthFactorWarning {
            base_year: 2020,
            base_indicator: 0.0,
            adjusted_base_indicator: MINGRWIND,
            growth_year: 2021,
            growth_indicator: 1.0,
        };
        let mut call_count = 0;
        let res = age_distribution(100.0, &mdyrfrc, 2020, 2022, &scrap, |y1, y2| {
            call_count += 1;
            Ok(GrowthFactor {
                factor: 0.05,
                base_indicator: 0.0,
                growth_indicator: 1.0,
                warning: if call_count == 1 {
                    Some(GrowthFactorWarning {
                        base_year: y1,
                        base_indicator: 0.0,
                        adjusted_base_indicator: MINGRWIND,
                        growth_year: y2,
                        growth_indicator: 1.0,
                    })
                } else {
                    None
                },
            })
        })
        .unwrap();
        assert_eq!(res.warnings.len(), 1);
        assert_eq!(res.warnings[0].base_year, warning.base_year);
        assert_eq!(call_count, 2);
    }

    #[test]
    fn forward_growth_iterates_per_year() {
        // 3-year forward span → 3 grwfac calls. Initial mdyrfrc = [1.0, 0, 0, ...].
        // No growth, no scrappage. Each iteration shifts the mass one slot:
        //   year 1: [0, 1, 0, 0, ...]   (mdyrfrc[0] = 1.0 - 1.0 = 0)
        //   year 2: [0, 0, 1, 0, ...]
        //   year 3: [0, 0, 0, 1, ...]
        let mdyrfrc = init_mdyrfrc(&[1.0]);
        let scrap = zero_scrap();
        let mut call_count = 0;
        let res = age_distribution(50.0, &mdyrfrc, 2020, 2023, &scrap, |y1, y2| {
            call_count += 1;
            assert_eq!(y2, y1 + 1);
            Ok(GrowthFactor {
                factor: 0.0,
                base_indicator: 1.0,
                growth_indicator: 1.0,
                warning: None,
            })
        })
        .unwrap();
        assert_eq!(call_count, 3);
        assert!((res.mdyrfrc[0] - 0.0).abs() < 1e-6);
        assert!((res.mdyrfrc[1] - 0.0).abs() < 1e-6);
        assert!((res.mdyrfrc[2] - 0.0).abs() < 1e-6);
        assert!((res.mdyrfrc[3] - 1.0).abs() < 1e-6);
        assert_eq!(res.mdyrfrc[4], 0.0);
    }

    #[test]
    fn forward_growth_clamps_small_population_with_nonzero_factor() {
        let mdyrfrc = init_mdyrfrc(&[1.0]);
        let scrap = zero_scrap();
        // baspop is below MINGRWIND. With non-zero factor → clamp totpop to MINGRWIND
        // before multiplying. Without clamp, the next iteration would never recover.
        let tiny = MINGRWIND / 100.0;
        let res = age_distribution(tiny, &mdyrfrc, 2020, 2021, &scrap, const_growth(0.10)).unwrap();
        // totpop = max(MINGRWIND, tiny) = MINGRWIND; then * 1.10.
        // totpopfrc = (MINGRWIND * 1.10) / tiny -- a large ratio (~110000).
        let expected_totpop = MINGRWIND * 1.10;
        let expected_frc = expected_totpop / tiny;
        // mdyrfrc[1] = max(0, 1.0 * (1 - 0)) = 1.0
        // mdyrfrc[0] = expected_frc - 1.0
        assert!((res.mdyrfrc[0] - (expected_frc - 1.0)).abs() / expected_frc < 1e-5);
    }

    #[test]
    fn rejects_wrong_length_mdyrfrc() {
        let res = age_distribution(100.0, &[0.5, 0.5], 2020, 2021, &zero_scrap(), |_, _| {
            Ok(GrowthFactor {
                factor: 0.0,
                base_indicator: 1.0,
                growth_indicator: 1.0,
                warning: None,
            })
        });
        match res {
            Err(Error::Config(msg)) => assert!(msg.contains("mdyrfrc")),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn rejects_wrong_length_yryrfrcscrp() {
        let mdyrfrc = init_mdyrfrc(&[1.0]);
        let res = age_distribution(100.0, &mdyrfrc, 2020, 2021, &[0.0, 0.1], |_, _| {
            Ok(GrowthFactor {
                factor: 0.0,
                base_indicator: 1.0,
                growth_indicator: 1.0,
                warning: None,
            })
        });
        match res {
            Err(Error::Config(msg)) => assert!(msg.contains("yryrfrcscrp")),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn growth_fn_error_propagates() {
        let mdyrfrc = init_mdyrfrc(&[1.0]);
        let scrap = zero_scrap();
        let res = age_distribution(100.0, &mdyrfrc, 2020, 2021, &scrap, |_, _| {
            Err(Error::Config("no growth data".into()))
        });
        match res {
            Err(Error::Config(msg)) => assert!(msg.contains("no growth data")),
            other => panic!("unexpected {other:?}"),
        }
    }
}

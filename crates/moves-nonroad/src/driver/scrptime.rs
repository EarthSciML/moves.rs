//! Scrappage-time accounting — `scrptime.f` (212 lines).
//!
//! Computes, for one equipment population, the three age-indexed
//! scrappage outputs the model-year routine ([`crate::population::modyr`])
//! consumes:
//!
//! - `yryrfrcscrp` — the year-to-year fraction scrapped at each age;
//! - `nyrlif` — the lifetime in years (the first age at which the
//!   cumulative scrappage reaches 100%);
//! - `modfrc` — the initial model-year fractions (age distribution)
//!   for the first full-scrappage year.
//!
//! The algorithm is based on `ScrapDir.doc` and the `ScrpTime`
//! worksheet of `scrap-2.xls` (see `scrptime.f` :8–9).
//!
//! # Relationship to `modyr`
//!
//! [`crate::population::modyr::model_year`] takes its scrappage data
//! through a `scrappage_fn: FnOnce(f32) -> Result<ScrappageTime>`
//! callback (see that module's docs). [`scrptime`] *is* that callback:
//! its return type is exactly [`ScrappageTime`], and the single `f32`
//! the callback receives — `acttmp`, the annual activity hours — is
//! [`scrptime`]'s `activity_hours_per_year` argument. A caller wires
//! the two together by capturing the remaining inputs in a closure:
//!
//! ```ignore
//! let out = model_year(
//!     strhrs, acthrs, units, eload, uselif, &age_table, agecod,
//!     |acttmp| scrptime(median_life_hours, load_factor, acttmp,
//!                       pop_growth_factor, &scrappage_curve),
//! )?;
//! ```
//!
//! # `getscrp` is resolved by the caller
//!
//! `scrptime.f` :126 calls `getscrp` to pick the scrappage curve by
//! name, once per `scrptime` call, so the per-age `fndscrp` walk does
//! not repeat the name lookup. The Rust port keeps that "resolve
//! once" property by taking the **already-selected** curve as the
//! `curve` argument — the caller runs
//! [`crate::population::scrappage::select_scrappage`] (the `getscrp`
//! port) once and passes the result in. This also keeps [`scrptime`]
//! free of the curve-name fallback warning, which the caller surfaces.
//!
//! # Degenerate `nyrlif == 0`
//!
//! When the cumulative scrappage never reaches 100% within
//! [`MXAGYR`] ages, `scrptime.f` leaves `nyrlif` at `0`. Every
//! `survunits` entry then stays `0`, `srvunttot` is `0`, and the
//! final `mdyrfrc(iage) = survunits(iage) / srvunttot` divides
//! `0.0 / 0.0`. The Fortran source still returns `ISUCES` in that
//! case, so the Rust port likewise returns `Ok` with a `modfrc`
//! vector of `NaN`s rather than erroring — the divergence, if any,
//! surfaces in the Task 115 fidelity gate. In practice production
//! scrappage curves always reach 100% well inside `MXAGYR`.

use crate::common::consts::MXAGYR;
use crate::input::scrappage::ScrappagePoint;
use crate::output::find::find_scrappage_percent;
use crate::population::modyr::ScrappageTime;
use crate::{Error, Result};

/// Compute the scrappage-time outputs — `scrptime.f` equivalent.
///
/// # Inputs
///
/// - `median_life_hours`: median life in hours at full load
///   (`mdlfhrs`).
/// - `load_factor`: equipment load factor (`ldfctr`).
/// - `activity_hours_per_year`: annual activity hours (`acthpy`) —
///   the `acttmp` value [`crate::population::modyr::model_year`]
///   passes to its scrappage callback.
/// - `pop_growth_factor`: population growth factor (`popgrwfac`).
/// - `curve`: the scrappage curve already resolved by name via
///   [`crate::population::scrappage::select_scrappage`] (`getscrp`).
///
/// # Errors
///
/// Returns [`Error::Config`] when `curve` is empty — `fndscrp` needs
/// at least one point to interpolate. (`scrptime.f` cannot hit this:
/// `getscrp` always fills the full `MXSCRP`-entry Fortran arrays.)
pub fn scrptime(
    median_life_hours: f32,
    load_factor: f32,
    activity_hours_per_year: f32,
    pop_growth_factor: f32,
    curve: &[ScrappagePoint],
) -> Result<ScrappageTime> {
    if curve.is_empty() {
        return Err(Error::Config(
            "scrptime: scrappage curve is empty (getscrp produced no points)".into(),
        ));
    }

    // --- median life in years, capped at int(MXAGYR / 2) ---
    // scrptime.f :118: mdlfyrs = min(real(int(MXAGYR/2.)),
    //                                mdlfhrs / ldfctr / acthpy).
    // int(51 / 2.) == 25. The division is evaluated left-to-right,
    // matching Fortran; degenerate zero divisors yield IEEE inf/NaN
    // and `min` then keeps the finite cap (Rust's f32::min returns the
    // non-NaN argument), exactly as the Fortran MIN intrinsic does.
    let life_cap = (MXAGYR / 2) as f32;
    let median_life_years = life_cap.min(median_life_hours / load_factor / activity_hours_per_year);

    // --- median life used per year (scrptime.f :122) ---
    let median_life_per_year = 1.0_f32 / median_life_years;

    // --- per-age cumulative-scrappage walk (scrptime.f :132–155) ---
    //
    // `yryrfrcscrp[0]` and `pct_scrapped[0]` stay 0.0 — the Fortran
    // source sets element 1 explicitly and only the do-loop (iage from
    // 2) writes the rest.
    let mut yryrfrcscrp = vec![0.0_f32; MXAGYR];
    let mut pct_scrapped = vec![0.0_f32; MXAGYR];
    let mut nyrlif: usize = 0;

    for iage in 2..=MXAGYR {
        let cur = iage - 1; // pctscrp(iage),   0-based
        let prev = iage - 2; // pctscrp(iage-1), 0-based
        let frac_life_used = (iage as f32 - 1.0) * median_life_per_year;
        // fndscrp — always Some: the curve was checked non-empty above.
        pct_scrapped[cur] = find_scrappage_percent(frac_life_used, curve)
            .expect("curve is non-empty (checked above)");
        let year_frac_scrapped = (pct_scrapped[cur] - pct_scrapped[prev]) / 100.0;
        if pct_scrapped[prev] >= 100.0 {
            if nyrlif == 0 {
                nyrlif = iage - 1;
            }
            yryrfrcscrp[cur] = 0.0;
        } else {
            yryrfrcscrp[cur] = 100.0 * year_frac_scrapped / (100.0 - pct_scrapped[prev]);
        }
    }
    // scrptime.f :151–155 — the do-loop exits with the index one past
    // MXAGYR, so this re-checks the final bin (pctscrp(MXAGYR)).
    if pct_scrapped[MXAGYR - 1] >= 100.0 && nyrlif == 0 {
        nyrlif = MXAGYR;
    }

    // --- sales growth factor (scrptime.f :159–160) ---
    let sales_growth = pop_growth_factor
        / ((-1.4306_f32 * pop_growth_factor) * median_life_years
            + (-0.24_f32 * pop_growth_factor)
            + 1.0);

    // --- sales by sales year (scrptime.f :164–168) ---
    // sales(isaleyr) = initsales + initsales * salesgrwfac * (isaleyr-1).
    let initial_sales = 1000.0_f32;
    let mut sales = vec![0.0_f32; MXAGYR];
    for (i, slot) in sales.iter_mut().enumerate() {
        // Fortran (isaleyr - 1) with isaleyr = i + 1 ⇒ i.
        *slot = initial_sales + (initial_sales * sales_growth * i as f32);
    }

    // --- relative surviving units for the first full-scrappage year
    //     (scrptime.f :173–182) ---
    let mut surviving = vec![0.0_f32; MXAGYR];
    let mut surviving_total = 0.0_f32;
    for iage in 1..=MXAGYR {
        let idx = iage - 1;
        if iage <= nyrlif {
            // sales(nyrlif - iage + 1), 1-based ⇒ index nyrlif - iage.
            surviving[idx] = sales[nyrlif - iage] * (1.0 - pct_scrapped[idx] / 100.0);
        }
        // else: stays 0.0 (scrptime.f :179).
        surviving_total += surviving[idx];
    }

    // --- initial model-year fractions (scrptime.f :187–189) ---
    let mut modfrc = vec![0.0_f32; MXAGYR];
    for (idx, slot) in modfrc.iter_mut().enumerate() {
        *slot = surviving[idx] / surviving_total;
    }

    Ok(ScrappageTime {
        yryrfrcscrp,
        modfrc,
        nyrlif,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::population::modyr::{model_year, ActivityUnits, AgeAdjustmentTable};

    /// Three-point curve on the fraction-of-median-life scale:
    /// 0% scrapped at 0, 50% at 1×, 100% at 2× median life.
    fn ramp_curve() -> Vec<ScrappagePoint> {
        vec![
            ScrappagePoint {
                bin: 0.0,
                percent: 0.0,
            },
            ScrappagePoint {
                bin: 1.0,
                percent: 50.0,
            },
            ScrappagePoint {
                bin: 2.0,
                percent: 100.0,
            },
        ]
    }

    #[test]
    fn empty_curve_is_an_error() {
        let err = scrptime(1000.0, 0.5, 100.0, 0.0, &[]).unwrap_err();
        match err {
            Error::Config(msg) => assert!(msg.contains("scrappage curve is empty")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn output_arrays_have_mxagyr_length() {
        let out = scrptime(1000.0, 0.5, 100.0, 0.0, &ramp_curve()).unwrap();
        assert_eq!(out.yryrfrcscrp.len(), MXAGYR);
        assert_eq!(out.modfrc.len(), MXAGYR);
    }

    #[test]
    fn first_yryrfrcscrp_slot_is_zero() {
        // scrptime.f :135 — yryrfrcscrp(1) is set explicitly to 0.
        let out = scrptime(1000.0, 0.5, 100.0, 0.05, &ramp_curve()).unwrap();
        assert_eq!(out.yryrfrcscrp[0], 0.0);
    }

    #[test]
    fn nyrlif_is_the_first_full_scrappage_age() {
        // mdlfhrs/ldfctr/acthpy = 200 / 1 / 100 = 2 ⇒ mdlfyrs = 2,
        // mdlfupy = 0.5. frcmlusd(iage) = (iage-1) * 0.5 reaches 2.0
        // (⇒ 100% scrapped) at iage-1 = 4, i.e. iage = 5. The loop
        // records nyrlif at the iteration where pctscrp(iage-1) first
        // hits 100, which is iage = 6 ⇒ nyrlif = 5.
        let out = scrptime(200.0, 1.0, 100.0, 0.0, &ramp_curve()).unwrap();
        assert_eq!(out.nyrlif, 5);
    }

    #[test]
    fn nyrlif_set_for_a_long_lived_population() {
        // mdlfyrs = min(25, 1000/0.5/100) = 20, mdlfupy = 0.05.
        // frcmlusd reaches 2.0 at iage-1 = 40 ⇒ pctscrp hits 100 at
        // iage = 41, nyrlif recorded at iage = 42 ⇒ nyrlif = 41.
        let out = scrptime(1000.0, 0.5, 100.0, 0.0, &ramp_curve()).unwrap();
        assert_eq!(out.nyrlif, 41);
    }

    #[test]
    fn modfrc_sums_to_one_when_nyrlif_positive() {
        // With nyrlif > 0 every modfrc entry is survunits / srvunttot,
        // so the whole vector sums to exactly 1 (up to f32 rounding).
        let out = scrptime(200.0, 1.0, 100.0, 0.0, &ramp_curve()).unwrap();
        assert!(out.nyrlif > 0);
        let total: f32 = out.modfrc.iter().sum();
        assert!((total - 1.0).abs() < 1e-5, "modfrc sum = {total}");
    }

    #[test]
    fn modfrc_is_zero_past_the_lifetime() {
        // Ages beyond nyrlif have no surviving units ⇒ modfrc 0.
        let out = scrptime(200.0, 1.0, 100.0, 0.0, &ramp_curve()).unwrap();
        for &frac in &out.modfrc[out.nyrlif..] {
            assert_eq!(frac, 0.0);
        }
    }

    #[test]
    fn modfrc_front_loaded_for_zero_growth() {
        // popgrwfac = 0 ⇒ flat sales ⇒ surviving units shrink with age
        // as the cumulative scrappage rises, so the youngest age holds
        // the largest model-year fraction.
        let out = scrptime(200.0, 1.0, 100.0, 0.0, &ramp_curve()).unwrap();
        assert!(out.nyrlif >= 2);
        assert!(out.modfrc[0] >= out.modfrc[1]);
    }

    #[test]
    fn nonzero_growth_runs_without_panicking() {
        // A realistic small growth rate must not panic and must still
        // yield a normalized age distribution.
        let out = scrptime(1000.0, 0.5, 100.0, 0.05, &ramp_curve()).unwrap();
        let total: f32 = out.modfrc.iter().sum();
        assert!((total - 1.0).abs() < 1e-4, "modfrc sum = {total}");
    }

    #[test]
    fn degenerate_nyrlif_zero_yields_nan_modfrc() {
        // A curve that never reaches 100% leaves nyrlif at 0; the
        // documented Fortran behavior is a 0/0 NaN age distribution.
        let curve = vec![
            ScrappagePoint {
                bin: 0.0,
                percent: 0.0,
            },
            ScrappagePoint {
                bin: 100.0, // far past any reachable frac-of-life
                percent: 90.0,
            },
        ];
        let out = scrptime(1000.0, 0.5, 100.0, 0.0, &curve).unwrap();
        assert_eq!(out.nyrlif, 0);
        assert!(out.modfrc.iter().all(|v| v.is_nan()));
    }

    #[test]
    fn zero_activity_does_not_panic() {
        // acttmp can legitimately be 0 (modyr's gallons-units branch
        // with uselif <= 0). The division yields IEEE inf/NaN and the
        // cap keeps mdlfyrs finite — no panic.
        let out = scrptime(1000.0, 0.5, 0.0, 0.0, &ramp_curve()).unwrap();
        assert_eq!(out.yryrfrcscrp.len(), MXAGYR);
    }

    #[test]
    fn plugs_into_model_year_as_the_scrappage_callback() {
        // The whole point of the modyr `scrappage_fn` indirection:
        // scrptime drops straight in once the curve + scalars are
        // captured.
        let curve = ramp_curve();
        let empty_age_table = AgeAdjustmentTable::default();
        let out = model_year(
            5.0,   // strhrs
            120.0, // acthrs
            ActivityUnits::HoursPerYear,
            0.8,    // eload
            1500.0, // uselif
            &empty_age_table,
            "DEFAULT",
            |acttmp| scrptime(2000.0, 0.5, acttmp, 0.0, &curve),
        )
        .unwrap();
        // modyr loops 0..nyrlif; a populated scrappage time gives it a
        // non-empty per-year schedule.
        assert_eq!(out.nyrlif, out.actadj.len());
        assert!(out.nyrlif > 0);
    }
}

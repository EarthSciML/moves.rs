//! Model-year-fraction computation — `modyr.f` (216 lines).
//!
//! Computes per-year activity, starts, and deterioration-age
//! adjustments for an equipment population, given a scrappage-time
//! computation (`scrptime.f`, Task 113) that fills in the
//! year-to-year scrappage fractions, lifetime in years (`nyrlif`),
//! and initial model-year fractions.
//!
//! # The `scrappage_fn` callback
//!
//! `modyr.f` calls `scrptime` mid-routine to get `yryrfrcscrp`,
//! `nyrlif`, and `modfrc`. `scrptime` is grouped with the driver
//! (Task 113); this port takes the result via a callback so that
//! callers (or tests) can plug in a real `scrptime` once it lands
//! while preserving the surrounding adjustment logic now. The
//! callback receives the single value modyr produces for scrptime
//! (`acttmp` — annual activity hours after unit conversion).
//!
//! # Algorithm (`modyr.f` :116–204)
//!
//! 1. Initialize `adjfac[1..MXAGYR] = 1.0` (constant in the original
//!    source — never updated; folded into [`stradj`](ModelYearOutput::stradj)
//!    via `stradj = strhrs`).
//! 2. If `agecod != "DEFAULT"`, look up the alternate-curve index
//!    in `agenam`. Missing → emit warning, fall back to DEFAULT.
//! 3. Convert activity units to annual hours (`acttmp`).
//! 4. Invoke `scrappage_fn(acttmp)` for the scrappage-time outputs.
//! 5. Per-year loop `i in 1..=nyrlif`:
//!    - DEFAULT: `actadj[i] = acttmp`.
//!    - Alternate: bin-search `accum/uselif` against `agebin` and
//!      take the matching `agepct[curve][bin]/100 * acttmp`. The
//!      `accum/uselif >= 2` early-out caps activity at zero
//!      (equipment past 2× useful-life is considered scrapped).
//!    - `stradj[i] = strhrs`.
//!    - `accum += actadj[i] * eload`.
//! 6. Per-year loop `i in 1..=nyrlif` (separate accumulator):
//!    - `accum += actadj[i] * eload`.
//!    - `detage[i] = accum / uselif` (clamped to zero).
//!
//! # `uselif` mutation
//!
//! When `uselif <= 0`, the Fortran source bumps it to `1.0` *after*
//! calling scrptime to avoid divide-by-zero in the deterioration-age
//! calculation. The Rust port matches this exactly — the scrappage
//! callback receives the original `uselif` (captured by the caller),
//! and only the post-scrptime adjustments use the bumped value. The
//! actual value used appears in [`ModelYearOutput::uselif_used`].
//!
//! # `acttmp` when no unit branch matches
//!
//! Fortran's `acttmp` is uninitialized in branches like
//! `IDXGLY .AND. uselif <= 0`. Under the production build flags
//! (`-finit-real=zero`, see `characterization/nonroad-build/flags.env`),
//! that uninitialized value is zero. The Rust port mirrors that:
//! `acttmp` defaults to `0.0` and only the matching branch overwrites it.
//!
//! # `agepct` bin-loop OOB
//!
//! `modyr.f` :178–184 walks `j in 1..MXUSE` and accesses
//! `agebin(j+1)`, which reads out of bounds when `j == MXUSE`. The
//! out-of-bounds read is unreachable in practice because the outer
//! `accum/uselif >= 2` guard combined with `rdact.f`'s
//! pre-initialization of `agebin = 2.5` ensures `accum/uselif <= 2`
//! at every iteration that reaches the j-loop and the loaded bins
//! plus the 2.5 default tail always contain a matching pair. The
//! Rust port iterates `j in 1..MXUSE - 1` (inclusive) to avoid the
//! UB cleanly; the practical behavior is unchanged.

use crate::common::consts::MXAGYR;
use crate::{Error, Result};

/// Activity-unit indicator (matches `nonrdact.inc` parameters
/// `IDXHRY=1`, `IDXHRD=2`, `IDXGLY=3`, `IDXGLD=4`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityUnits {
    /// Hours per year — `IDXHRY = 1`. `acttmp = acthrs`.
    HoursPerYear,
    /// Hours per day — `IDXHRD = 2`. `acttmp = acthrs * 365`.
    HoursPerDay,
    /// Gallons per year — `IDXGLY = 3`. `acttmp = 1 / (2*uselif)`
    /// when `uselif > 0`, else `0`.
    GallonsPerYear,
    /// Gallons per day — `IDXGLD = 4`. Same conversion as
    /// `GallonsPerYear` (see the Fortran source).
    GallonsPerDay,
}

/// Output of the scrappage-time computation (`scrptime.f`, Task 113).
///
/// Supplied to [`model_year`] by the caller's `scrappage_fn` callback.
#[derive(Debug, Clone, PartialEq)]
pub struct ScrappageTime {
    /// Year-to-year fraction scrapped by age. Length [`MXAGYR`].
    pub yryrfrcscrp: Vec<f32>,
    /// Model-year fractions by age (initial age distribution).
    /// Length [`MXAGYR`].
    pub modfrc: Vec<f32>,
    /// Lifetime in years (number of years until full scrappage).
    /// `1 <= nyrlif <= MXAGYR`.
    pub nyrlif: usize,
}

/// Alternate `/AGE ADJUSTMENT/` curve table — `agebin`, `agepct`,
/// `agenam`, and `nagenm` from `nonrdact.inc` combined.
///
/// Layout: `bins[k]` and `pcts[k][curve_idx]` correspond to Fortran
/// `agebin(k+1)` and `agepct(curve_idx+1, k+1)` (Fortran indices are
/// 1-based; the table itself stores them 0-based).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AgeAdjustmentTable {
    /// Curve names from the `/AGE ADJUSTMENT/` header; upper-cased,
    /// left-justified. Length `<= MXAGE`. `rdact.f` :240–243 trims
    /// + upper-cases when loading.
    pub names: Vec<String>,
    /// Bin boundaries. Length [`MXUSE`]; `rdact.f` :217–222
    /// pre-initializes every slot to `2.5` before reading the file,
    /// so unloaded tails are `2.5` (not zero / not garbage).
    pub bins: Vec<f32>,
    /// Per-bin percent values, indexed `pcts[bin_idx][curve_idx]`.
    /// Outer length must equal `bins.len()`. Unloaded slots default
    /// to `100.0` per `rdact.f` :219–221.
    pub pcts: Vec<Vec<f32>>,
}

/// Output of [`model_year`].
#[derive(Debug, Clone, PartialEq)]
pub struct ModelYearOutput {
    /// Year-to-year fraction scrapped by age — pass-through from
    /// the scrappage callback. Length [`MXAGYR`].
    pub yryrfrcscrp: Vec<f32>,
    /// Initial model-year fractions — pass-through from the
    /// scrappage callback. Length [`MXAGYR`].
    pub modfrc: Vec<f32>,
    /// Starts adjustment per year. Length `nyrlif`. Each slot is
    /// `strhrs` (Fortran's `adjfac` is constant 1.0).
    pub stradj: Vec<f32>,
    /// Activity adjustment per year. Length `nyrlif`. DEFAULT
    /// uses `acttmp`; alternate curves scale `acttmp` by per-bin
    /// percentages.
    pub actadj: Vec<f32>,
    /// Deterioration age per year. Length `nyrlif`. Cumulative
    /// `accum/uselif`, clamped to zero.
    pub detage: Vec<f32>,
    /// Lifetime in years — echoed from the scrappage callback.
    pub nyrlif: usize,
    /// `uselif` actually used in the post-scrptime computations.
    /// Equals the input `uselif` unless that was `<= 0`, in which
    /// case it was bumped to `1.0`.
    pub uselif_used: f32,
    /// Annual activity hours (`acttmp`) used for the scrappage
    /// callback and as the unscaled default activity adjustment.
    pub acttmp: f32,
    /// Set when the alternate-curve lookup failed and the routine
    /// fell back to DEFAULT. Mirrors the warning the Fortran source
    /// writes to `IOWMSG` plus the `nwarn` bump.
    pub age_curve_warning: Option<String>,
}

/// Run the model-year-fraction computation — `modyr.f` equivalent.
///
/// `scrappage_fn` is the `scrptime.f` callback. It receives the
/// annual activity hours (`acttmp`) computed from the unit
/// conversion and must return the scrappage outputs. Callers
/// capture `uselif`, `eload`, `disin`, and `popgrwfac` in the
/// closure.
///
/// # Inputs
///
/// - `strhrs`: starts hours from the activity file.
/// - `acthrs`: activity hours from the activity file (units per
///   `units`).
/// - `units`: activity-unit indicator.
/// - `eload`: equipment load factor.
/// - `uselif`: expected lifespan in hours.
/// - `age_table`: alternate `/AGE ADJUSTMENT/` curves
///   (`rdact.f` output).
/// - `agecod`: 10-char code that picks a curve; `"DEFAULT"` (case-
///   insensitive, whitespace-padded ok) bypasses the lookup.
///
/// # Errors
///
/// Forwards any [`Error`] from `scrappage_fn`. Also returns
/// [`Error::Config`] if `age_table` is malformed
/// (`pcts.len() != bins.len()` or a row is too short for the
/// matched curve index) or if the scrappage callback returns
/// arrays of the wrong length.
pub fn model_year<F>(
    strhrs: f32,
    acthrs: f32,
    units: ActivityUnits,
    eload: f32,
    uselif: f32,
    age_table: &AgeAdjustmentTable,
    agecod: &str,
    scrappage_fn: F,
) -> Result<ModelYearOutput>
where
    F: FnOnce(f32) -> Result<ScrappageTime>,
{
    // --- search for the alternate activity curve (modyr.f :121–132) ---
    let agecod_trim = agecod.trim().to_ascii_uppercase();
    let mut age_curve_warning: Option<String> = None;
    let curve_idx: Option<usize> = if agecod_trim == "DEFAULT" {
        None
    } else {
        let hit = age_table
            .names
            .iter()
            .position(|n| n.trim().eq_ignore_ascii_case(&agecod_trim));
        if hit.is_none() {
            age_curve_warning = Some(format!(
                "Cannot find /AGE ADJUSTMENT/ curve '{}'; using DEFAULT, no adjustment.",
                agecod.trim()
            ));
        }
        hit
    };

    // --- compute acttmp from the unit indicator (modyr.f :136–144).
    //     Uninitialized branches default to 0.0 to match the Fortran
    //     binary built with -finit-real=zero. ---
    let mut acttmp: f32 = 0.0;
    match units {
        ActivityUnits::HoursPerYear => {
            acttmp = acthrs;
        }
        ActivityUnits::HoursPerDay => {
            acttmp = acthrs * 365.0;
        }
        ActivityUnits::GallonsPerYear | ActivityUnits::GallonsPerDay => {
            if uselif > 0.0 {
                acttmp = 1.0 / (2.0 * uselif);
            }
        }
    }

    // --- call the scrappage-time computation (scrptime.f, modyr.f :153–154).
    //     The callback captures uselif, eload, disin, popgrwfac from the
    //     caller's scope. ---
    let scrappage = scrappage_fn(acttmp)?;
    if scrappage.yryrfrcscrp.len() != MXAGYR {
        return Err(Error::Config(format!(
            "modyr: scrappage_fn returned yryrfrcscrp length {} (expected {MXAGYR})",
            scrappage.yryrfrcscrp.len()
        )));
    }
    if scrappage.modfrc.len() != MXAGYR {
        return Err(Error::Config(format!(
            "modyr: scrappage_fn returned modfrc length {} (expected {MXAGYR})",
            scrappage.modfrc.len()
        )));
    }
    if scrappage.nyrlif > MXAGYR {
        return Err(Error::Config(format!(
            "modyr: scrappage_fn returned nyrlif={} (max {MXAGYR})",
            scrappage.nyrlif
        )));
    }

    // --- validate alternate-curve table when in use ---
    if let Some(cidx) = curve_idx {
        if age_table.pcts.len() != age_table.bins.len() {
            return Err(Error::Config(format!(
                "modyr: age_table.pcts.len() = {} but bins.len() = {}",
                age_table.pcts.len(),
                age_table.bins.len()
            )));
        }
        for (bin_idx, row) in age_table.pcts.iter().enumerate() {
            if row.len() <= cidx {
                return Err(Error::Config(format!(
                    "modyr: age_table.pcts[{}] has length {} but curve_idx {}",
                    bin_idx,
                    row.len(),
                    cidx
                )));
            }
        }
        if age_table.bins.len() < 2 {
            return Err(Error::Config(format!(
                "modyr: age_table.bins.len() = {} (need at least 2 for bin search)",
                age_table.bins.len()
            )));
        }
    }

    let nyrlif = scrappage.nyrlif;

    // --- bump uselif to 1.0 if it was non-positive (modyr.f :165).
    //     Only used in the post-scrptime loops below. ---
    let uselif_used = if uselif <= 0.0 { 1.0 } else { uselif };

    // --- activity & starts adjustment loop (modyr.f :164–191) ---
    let mut actadj: Vec<f32> = Vec::with_capacity(nyrlif);
    let mut stradj: Vec<f32> = Vec::with_capacity(nyrlif);
    let mut accum: f32 = 0.0;
    for _i in 0..nyrlif {
        let value: f32 = match curve_idx {
            None => acttmp,
            Some(cidx) => alternate_actadj(accum, uselif_used, acttmp, cidx, age_table),
        };
        actadj.push(value);
        stradj.push(strhrs); // adjfac is constant 1.0 in the Fortran source
        accum += value * eload;
    }

    // --- deterioration-age loop (modyr.f :196–204) ---
    let mut detage: Vec<f32> = Vec::with_capacity(nyrlif);
    let mut accum: f32 = 0.0;
    for i in 0..nyrlif {
        accum += actadj[i] * eload;
        let v = if accum > 0.0 {
            accum / uselif_used
        } else {
            0.0
        };
        detage.push(v);
    }

    Ok(ModelYearOutput {
        yryrfrcscrp: scrappage.yryrfrcscrp,
        modfrc: scrappage.modfrc,
        stradj,
        actadj,
        detage,
        nyrlif,
        uselif_used,
        acttmp,
        age_curve_warning,
    })
}

/// Alternate-curve actadj evaluation — `modyr.f` :172–187.
fn alternate_actadj(
    accum: f32,
    uselif_used: f32,
    acttmp: f32,
    curve_idx: usize,
    age_table: &AgeAdjustmentTable,
) -> f32 {
    let ratio = accum / uselif_used;
    if ratio >= 2.0 {
        return 0.0;
    }
    // bins[0] corresponds to Fortran agebin(1)
    let bin0 = age_table.bins[0];
    if ratio <= bin0 {
        return age_table.pcts[0][curve_idx] / 100.0 * acttmp;
    }
    // Walk pairs (bins[k], bins[k+1]) for k in 0..bins.len() - 1.
    // The Fortran loop `j = 1..MXUSE` reads agebin(j+1) which is OOB
    // when j == MXUSE; we drop that final iteration since `rdact.f`
    // initializes the trailing bins to 2.5 and the outer ratio < 2
    // guard makes the OOB read unreachable in practice.
    let end = age_table.bins.len().saturating_sub(1);
    for k in 0..end {
        let lo = age_table.bins[k];
        let hi = age_table.bins[k + 1];
        if ratio > lo && ratio <= hi {
            return age_table.pcts[k][curve_idx] / 100.0 * acttmp;
        }
    }
    // Unreached given the data invariants above; preserve the
    // implicit Fortran "actadj untouched" semantics by returning the
    // -finit-real=zero value.
    0.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::consts::MXUSE;

    fn full(values: Vec<f32>) -> Vec<f32> {
        let mut out = values;
        out.resize(MXAGYR, 0.0);
        out
    }

    fn default_scrappage(nyrlif: usize) -> ScrappageTime {
        ScrappageTime {
            yryrfrcscrp: full(vec![0.0; nyrlif]),
            modfrc: full(vec![1.0 / nyrlif as f32; nyrlif]),
            nyrlif,
        }
    }

    fn empty_table() -> AgeAdjustmentTable {
        AgeAdjustmentTable {
            names: vec![],
            bins: vec![2.5; MXUSE],
            pcts: vec![vec![100.0; 0]; MXUSE],
        }
    }

    #[test]
    fn default_curve_uses_acttmp() {
        let scrap = default_scrappage(3);
        let out = model_year(
            10.0,
            500.0,
            ActivityUnits::HoursPerYear,
            0.5,
            1000.0,
            &empty_table(),
            "DEFAULT",
            |_acttmp| Ok(scrap.clone()),
        )
        .unwrap();
        assert_eq!(out.nyrlif, 3);
        assert_eq!(out.acttmp, 500.0);
        for v in &out.actadj {
            assert_eq!(*v, 500.0);
        }
        for v in &out.stradj {
            assert_eq!(*v, 10.0);
        }
        assert!(out.age_curve_warning.is_none());
    }

    #[test]
    fn default_curve_case_insensitive_and_whitespace_tolerant() {
        let scrap = default_scrappage(1);
        let out = model_year(
            0.0,
            10.0,
            ActivityUnits::HoursPerYear,
            1.0,
            100.0,
            &empty_table(),
            "  default ",
            |_| Ok(scrap.clone()),
        )
        .unwrap();
        assert!(out.age_curve_warning.is_none());
    }

    #[test]
    fn unknown_curve_warns_and_falls_back_to_default() {
        let mut table = empty_table();
        table.names = vec!["EARLY".into(), "LATE".into()];
        table.pcts = vec![vec![100.0, 100.0]; MXUSE];
        let scrap = default_scrappage(2);
        let out = model_year(
            0.0,
            8.0,
            ActivityUnits::HoursPerYear,
            1.0,
            10.0,
            &table,
            "MISSING",
            |_| Ok(scrap.clone()),
        )
        .unwrap();
        assert!(out.age_curve_warning.is_some());
        assert!(out.age_curve_warning.unwrap().contains("MISSING"));
        // DEFAULT behavior: actadj == acttmp.
        for v in &out.actadj {
            assert_eq!(*v, 8.0);
        }
    }

    #[test]
    fn units_hours_per_day_multiplies_by_365() {
        let scrap = default_scrappage(1);
        let out = model_year(
            0.0,
            2.0,
            ActivityUnits::HoursPerDay,
            1.0,
            100.0,
            &empty_table(),
            "DEFAULT",
            |acttmp| {
                assert_eq!(acttmp, 2.0 * 365.0);
                Ok(scrap.clone())
            },
        )
        .unwrap();
        assert_eq!(out.acttmp, 730.0);
    }

    #[test]
    fn units_gallons_per_year_uses_one_over_2x_uselif() {
        let scrap = default_scrappage(1);
        let out = model_year(
            0.0,
            999.0, // ignored for gallons-based units
            ActivityUnits::GallonsPerYear,
            1.0,
            100.0,
            &empty_table(),
            "DEFAULT",
            |acttmp| {
                assert!((acttmp - 1.0 / 200.0).abs() < 1e-9);
                Ok(scrap.clone())
            },
        )
        .unwrap();
        assert!((out.acttmp - 0.005).abs() < 1e-9);
    }

    #[test]
    fn units_gallons_per_year_zero_uselif_yields_zero_acttmp() {
        let scrap = default_scrappage(1);
        let out = model_year(
            0.0,
            999.0,
            ActivityUnits::GallonsPerYear,
            1.0,
            0.0, // uselif == 0 → acttmp stays at the -finit-real=zero default
            &empty_table(),
            "DEFAULT",
            |acttmp| {
                assert_eq!(acttmp, 0.0);
                Ok(scrap.clone())
            },
        )
        .unwrap();
        assert_eq!(out.acttmp, 0.0);
        // The bump-to-1 also applies for non-positive uselif.
        assert_eq!(out.uselif_used, 1.0);
    }

    #[test]
    fn units_gallons_per_day_same_conversion() {
        let scrap = default_scrappage(1);
        let out = model_year(
            0.0,
            999.0,
            ActivityUnits::GallonsPerDay,
            1.0,
            50.0,
            &empty_table(),
            "DEFAULT",
            |acttmp| {
                assert!((acttmp - 1.0 / 100.0).abs() < 1e-9);
                Ok(scrap.clone())
            },
        )
        .unwrap();
        assert!((out.acttmp - 0.01).abs() < 1e-9);
    }

    #[test]
    fn uselif_negative_bumps_to_one() {
        let scrap = default_scrappage(1);
        let out = model_year(
            0.0,
            100.0,
            ActivityUnits::HoursPerYear,
            1.0,
            -5.0,
            &empty_table(),
            "DEFAULT",
            |_| Ok(scrap.clone()),
        )
        .unwrap();
        assert_eq!(out.uselif_used, 1.0);
    }

    #[test]
    fn stradj_equals_strhrs_for_each_year() {
        let scrap = default_scrappage(4);
        let out = model_year(
            42.0,
            100.0,
            ActivityUnits::HoursPerYear,
            1.0,
            10.0,
            &empty_table(),
            "DEFAULT",
            |_| Ok(scrap.clone()),
        )
        .unwrap();
        assert_eq!(out.stradj, vec![42.0; 4]);
    }

    #[test]
    fn detage_accumulates_over_years() {
        // DEFAULT, acttmp=10, eload=1, uselif=10, nyrlif=3.
        // detage[i] = sum(actadj[0..=i] * eload) / uselif = (10*(i+1))/10 = i+1.
        let scrap = default_scrappage(3);
        let out = model_year(
            0.0,
            10.0,
            ActivityUnits::HoursPerYear,
            1.0,
            10.0,
            &empty_table(),
            "DEFAULT",
            |_| Ok(scrap.clone()),
        )
        .unwrap();
        assert_eq!(out.detage, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn detage_clamped_at_zero_when_acttmp_zero() {
        // acttmp=0, eload=1 → accum stays 0 → detage all zero.
        let scrap = default_scrappage(2);
        let out = model_year(
            0.0,
            0.0,
            ActivityUnits::HoursPerYear,
            1.0,
            10.0,
            &empty_table(),
            "DEFAULT",
            |_| Ok(scrap.clone()),
        )
        .unwrap();
        assert_eq!(out.detage, vec![0.0, 0.0]);
    }

    #[test]
    fn alternate_curve_bin_below_first_returns_pcts0() {
        // One alternate curve "CRV", 3 loaded bins (1.0, 1.5, 2.0)
        // with percents 50, 80, 100 for that curve.
        let mut bins = vec![2.5; MXUSE];
        bins[0] = 1.0;
        bins[1] = 1.5;
        bins[2] = 2.0;
        let mut pcts: Vec<Vec<f32>> = vec![vec![100.0]; MXUSE];
        pcts[0] = vec![50.0];
        pcts[1] = vec![80.0];
        pcts[2] = vec![100.0];
        let table = AgeAdjustmentTable {
            names: vec!["CRV".into()],
            bins,
            pcts,
        };
        // acttmp=100, eload=0 → accum stays 0 → ratio = 0/uselif = 0,
        // which is <= bins[0]=1.0 → actadj = 50/100 * 100 = 50.
        let scrap = default_scrappage(2);
        let out = model_year(
            0.0,
            100.0,
            ActivityUnits::HoursPerYear,
            0.0,
            10.0,
            &table,
            "CRV",
            |_| Ok(scrap.clone()),
        )
        .unwrap();
        assert_eq!(out.actadj, vec![50.0, 50.0]);
        assert!(out.age_curve_warning.is_none());
    }

    #[test]
    fn alternate_curve_searches_bin_pair() {
        let mut bins = vec![2.5; MXUSE];
        bins[0] = 0.5;
        bins[1] = 1.0;
        bins[2] = 1.5;
        let mut pcts: Vec<Vec<f32>> = vec![vec![100.0]; MXUSE];
        pcts[0] = vec![25.0];
        pcts[1] = vec![60.0];
        pcts[2] = vec![90.0];
        let table = AgeAdjustmentTable {
            names: vec!["CRV".into()],
            bins,
            pcts,
        };
        // acttmp=100, eload=1, uselif=1 →
        // i=0: ratio=0  → <= bins[0]=0.5 → actadj=25.
        // accum += 25*1 = 25.
        // i=1: ratio=25 → >= 2 → actadj=0.
        // accum += 0.
        // i=2: ratio=25 → >= 2 → actadj=0.
        let scrap = default_scrappage(3);
        let out = model_year(
            0.0,
            100.0,
            ActivityUnits::HoursPerYear,
            1.0,
            1.0,
            &table,
            "CRV",
            |_| Ok(scrap.clone()),
        )
        .unwrap();
        assert_eq!(out.actadj, vec![25.0, 0.0, 0.0]);
    }

    #[test]
    fn alternate_curve_walks_bin_pairs_as_accum_grows() {
        // bins=[0.5, 1.0, 1.5, 2.5, ...], pcts[0]=25, pcts[1]=60, pcts[2]=90.
        // The Fortran maps:
        //   ratio <= bins[0]              → pcts[0]   (early-branch in modyr.f :175)
        //   bins[0] < ratio <= bins[1]    → pcts[0]   (j=1 in modyr.f :178–183)
        //   bins[1] < ratio <= bins[2]    → pcts[1]   (j=2)
        //   bins[2] < ratio <= bins[3]    → pcts[2]   (j=3)
        // With uselif=1, eload=0.1, acttmp=10, the accum walks past the bin
        // boundaries within a few iterations.
        let mut bins = vec![2.5; MXUSE];
        bins[0] = 0.5;
        bins[1] = 1.0;
        bins[2] = 1.5;
        let mut pcts: Vec<Vec<f32>> = vec![vec![100.0]; MXUSE];
        pcts[0] = vec![25.0];
        pcts[1] = vec![60.0];
        pcts[2] = vec![90.0];
        let table = AgeAdjustmentTable {
            names: vec!["CRV".into()],
            bins,
            pcts,
        };
        let scrap = default_scrappage(8);
        let out = model_year(
            0.0,
            10.0,
            ActivityUnits::HoursPerYear,
            0.1,
            1.0,
            &table,
            "CRV",
            |_| Ok(scrap.clone()),
        )
        .unwrap();
        // Expected trace:
        // i=0: ratio=0    → ≤ 0.5 → pcts[0]=25 → actadj=2.5; accum=0.25
        // i=1: ratio=0.25 → ≤ 0.5 → pcts[0]=25 → actadj=2.5; accum=0.50
        // i=2: ratio=0.50 → ≤ 0.5 → pcts[0]=25 → actadj=2.5; accum=0.75
        // i=3: ratio=0.75 → (0.5,1.0] → pcts[0]=25 → actadj=2.5; accum=1.00
        // i=4: ratio=1.00 → (0.5,1.0] → pcts[0]=25 → actadj=2.5; accum=1.25
        // i=5: ratio=1.25 → (1.0,1.5] → pcts[1]=60 → actadj=6.0; accum=1.85
        // i=6: ratio=1.85 → (1.5,2.5] → pcts[2]=90 → actadj=9.0; accum=2.75
        // i=7: ratio=2.75 → ≥ 2      → 0
        for (i, &v) in out.actadj.iter().enumerate() {
            let expected = match i {
                0..=4 => 2.5,
                5 => 6.0,
                6 => 9.0,
                7 => 0.0,
                _ => unreachable!(),
            };
            assert!(
                (v - expected).abs() < 1e-5,
                "actadj[{i}] = {v} (expected {expected})"
            );
        }
    }

    #[test]
    fn detage_uses_bumped_uselif() {
        // uselif=0 → bumped to 1.0 → detage = accum, where accum = actadj*eload.
        let scrap = default_scrappage(2);
        let out = model_year(
            0.0,
            10.0,
            ActivityUnits::HoursPerYear,
            1.0,
            0.0,
            &empty_table(),
            "DEFAULT",
            |_| Ok(scrap.clone()),
        )
        .unwrap();
        assert_eq!(out.uselif_used, 1.0);
        assert_eq!(out.detage, vec![10.0, 20.0]);
    }

    #[test]
    fn scrappage_fn_error_propagates() {
        let res = model_year(
            0.0,
            10.0,
            ActivityUnits::HoursPerYear,
            1.0,
            10.0,
            &empty_table(),
            "DEFAULT",
            |_| Err(Error::Config("scrptime unavailable".into())),
        );
        match res {
            Err(Error::Config(msg)) => assert!(msg.contains("scrptime")),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn rejects_short_yryrfrcscrp() {
        let res = model_year(
            0.0,
            10.0,
            ActivityUnits::HoursPerYear,
            1.0,
            10.0,
            &empty_table(),
            "DEFAULT",
            |_| {
                Ok(ScrappageTime {
                    yryrfrcscrp: vec![0.0; 5],
                    modfrc: vec![0.0; MXAGYR],
                    nyrlif: 1,
                })
            },
        );
        match res {
            Err(Error::Config(msg)) => assert!(msg.contains("yryrfrcscrp")),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn rejects_oversized_nyrlif() {
        let res = model_year(
            0.0,
            10.0,
            ActivityUnits::HoursPerYear,
            1.0,
            10.0,
            &empty_table(),
            "DEFAULT",
            |_| {
                Ok(ScrappageTime {
                    yryrfrcscrp: vec![0.0; MXAGYR],
                    modfrc: vec![0.0; MXAGYR],
                    nyrlif: MXAGYR + 1,
                })
            },
        );
        match res {
            Err(Error::Config(msg)) => assert!(msg.contains("nyrlif")),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn rejects_malformed_age_table() {
        let mut bins = vec![2.5; MXUSE];
        bins[0] = 0.5;
        let pcts: Vec<Vec<f32>> = vec![vec![100.0]; MXUSE - 1]; // wrong length
        let table = AgeAdjustmentTable {
            names: vec!["CRV".into()],
            bins,
            pcts,
        };
        let res = model_year(
            0.0,
            10.0,
            ActivityUnits::HoursPerYear,
            1.0,
            10.0,
            &table,
            "CRV",
            |_| {
                Ok(ScrappageTime {
                    yryrfrcscrp: vec![0.0; MXAGYR],
                    modfrc: vec![0.0; MXAGYR],
                    nyrlif: 1,
                })
            },
        );
        match res {
            Err(Error::Config(msg)) => {
                assert!(msg.contains("pcts") && msg.contains("bins"))
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn nyrlif_zero_yields_empty_arrays() {
        let scrap = ScrappageTime {
            yryrfrcscrp: vec![0.0; MXAGYR],
            modfrc: vec![0.0; MXAGYR],
            nyrlif: 0,
        };
        let out = model_year(
            5.0,
            10.0,
            ActivityUnits::HoursPerYear,
            1.0,
            10.0,
            &empty_table(),
            "DEFAULT",
            |_| Ok(scrap.clone()),
        )
        .unwrap();
        assert_eq!(out.nyrlif, 0);
        assert!(out.actadj.is_empty());
        assert!(out.stradj.is_empty());
        assert!(out.detage.is_empty());
    }

    #[test]
    fn modfrc_and_yryrfrcscrp_pass_through() {
        let mut modfrc = vec![0.0; MXAGYR];
        modfrc[0] = 0.1;
        modfrc[1] = 0.2;
        let mut yry = vec![0.0; MXAGYR];
        yry[1] = 0.05;
        let scrap = ScrappageTime {
            yryrfrcscrp: yry.clone(),
            modfrc: modfrc.clone(),
            nyrlif: 2,
        };
        let out = model_year(
            0.0,
            10.0,
            ActivityUnits::HoursPerYear,
            1.0,
            10.0,
            &empty_table(),
            "DEFAULT",
            |_| Ok(scrap.clone()),
        )
        .unwrap();
        assert_eq!(out.modfrc, modfrc);
        assert_eq!(out.yryrfrcscrp, yry);
    }
}

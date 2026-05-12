//! Sulfur and RFG adjustment-factor table initialiser (`intadj.f`).
//!
//! Task 99. Populates the per-fuel sulfur arrays and the per-season /
//! per-year-bin / per-pollutant RFG adjustment factor arrays. The
//! values are mostly hardcoded constants from `nonrdprm.inc`
//! (`SWTGS2`, `SFCGS2`, `ALTGS2`, etc.); the per-run sulfur fractions
//! come from the parsed `/OPTIONS/` packet.
//!
//! In the Fortran source this routine writes into the `iyrbin`,
//! `rfggs2`, `rfggs4`, `soxbas`, `soxfrc`, `soxful`, `altfac` COMMON
//! arrays declared in `nonrdefc.inc`. The Rust port returns the same
//! tables as an owned [`AdjustmentTables`] struct.
//!
//! # Fortran source
//!
//! Ports `intadj.f` (141 lines).

use crate::input::options::OptionsConfig;

/// Number of fuel/engine type indices indexed by [`FuelIndex`].
pub const N_FUEL_TYPES: usize = 5;

/// Number of RFG year-range bins (`NRFGBIN = 3` in `nonrdefc.inc`).
pub const N_RFG_BINS: usize = 3;

/// Number of seasonal slices used by the RFG arrays. The Fortran
/// source dimensions these by `IDXFAL = 4`, matching the four
/// seasons (`IDXWTR=1`, `IDXSPR=2`, `IDXSUM=3`, `IDXFAL=4`).
pub const N_SEASONS: usize = 4;

/// Fuel/engine type, preserving the Fortran 1-based `IDX*` constants
/// from `nonrdprm.inc` lines 149–165.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FuelIndex {
    /// 2-stroke gasoline (`IDXGS2 = 1`).
    Gs2 = 1,
    /// 4-stroke gasoline (`IDXGS4 = 2`).
    Gs4 = 2,
    /// Diesel (`IDXDSL = 3`).
    Dsl = 3,
    /// LPG (`IDXLPG = 4`).
    Lpg = 4,
    /// CNG (`IDXCNG = 5`).
    Cng = 5,
}

impl FuelIndex {
    /// Zero-based index suitable for array access.
    pub fn idx(self) -> usize {
        self as usize - 1
    }
}

/// Season, preserving the Fortran 1-based `IDX*` constants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeasonIndex {
    /// Winter (`IDXWTR = 1`).
    Winter = 1,
    /// Spring (`IDXSPR = 2`).
    Spring = 2,
    /// Summer (`IDXSUM = 3`).
    Summer = 3,
    /// Fall (`IDXFAL = 4`).
    Fall = 4,
}

impl SeasonIndex {
    /// Zero-based index suitable for array access.
    pub fn idx(self) -> usize {
        self as usize - 1
    }
}

/// Pollutant subset addressed by the RFG adjustment factor arrays
/// (`rfggs2`, `rfggs4`). The Fortran source indexes by the broader
/// pollutant table (`MXPOL = 23`), but `intadj.f` only sets values for
/// the five entries below; everything else is left at zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RfgPollutant {
    /// Total HC (`IDXTHC = 1`).
    Thc = 0,
    /// CO (`IDXCO = 2`).
    Co = 1,
    /// NOx (`IDXNOX = 3`).
    Nox = 2,
    /// SOx (`IDXSOX = 5`).
    Sox = 3,
    /// PM (`IDXPM = 6`).
    Pm = 4,
}

/// Baseline sulfur weight content (`SWTGS2`/`SWTGS4`/`SWTLPG`/`SWTCNG`/`SWTDSL`
/// from `nonrdprm.inc`), per [`FuelIndex`].
pub const SOX_BASELINE: [f32; N_FUEL_TYPES] = [
    0.0339, // GS2 — SWTGS2
    0.0339, // GS4 — SWTGS4
    0.33,   // DSL — SWTDSL
    0.008,  // LPG — SWTLPG
    0.008,  // CNG — SWTCNG
];

/// Fraction of fuel-sulfur that becomes PM (`SFCGS2`/.../`SFCDSL`).
pub const SOX_PM_FRACTION: [f32; N_FUEL_TYPES] = [
    0.03,    // GS2 — SFCGS2
    0.03,    // GS4 — SFCGS4
    0.02247, // DSL — SFCDSL
    0.03,    // LPG — SFCLPG
    0.03,    // CNG — SFCCNG
];

/// Altitude correction factor (`ALTGS2`/.../`ALTDSL`). All values are
/// 1.0 in `nonrdprm.inc` and that constancy is preserved here.
pub const ALT_FACTOR: [f32; N_FUEL_TYPES] = [1.0; N_FUEL_TYPES];

/// Year-range bin endpoints (`iyrbin`) and per-season RFG adjustment
/// factor arrays (`rfggs2`, `rfggs4`).
#[derive(Debug, Clone)]
pub struct AdjustmentTables {
    /// Baseline sulfur content per fuel (5 entries).
    pub sox_baseline: [f32; N_FUEL_TYPES],
    /// PM-conversion fraction per fuel (5 entries).
    pub sox_pm_fraction: [f32; N_FUEL_TYPES],
    /// Episode sulfur content per fuel (5 entries). Sourced from the
    /// parsed `/OPTIONS/` packet.
    pub sox_full: [f32; N_FUEL_TYPES],
    /// Altitude correction factor per fuel (5 entries).
    pub altitude_factor: [f32; N_FUEL_TYPES],
    /// Year-range bin endpoints `[season][bin]` → `(begin_year, end_year)`.
    /// Bins outside what `intadj.f` populates are zeroed.
    pub year_bins: [[(i32, i32); N_RFG_BINS]; N_SEASONS],
    /// 2-stroke gasoline RFG adjustment factors `[season][bin][pollutant]`.
    pub rfg_gs2: [[[f32; 5]; N_RFG_BINS]; N_SEASONS],
    /// 4-stroke gasoline RFG adjustment factors `[season][bin][pollutant]`.
    pub rfg_gs4: [[[f32; 5]; N_RFG_BINS]; N_SEASONS],
}

impl AdjustmentTables {
    /// Initialise the adjustment tables from a parsed `/OPTIONS/`
    /// packet. Mirrors `intadj.f` lines 49–129 verbatim.
    pub fn from_options(opts: &OptionsConfig) -> Self {
        let mut tables = Self {
            sox_baseline: SOX_BASELINE,
            sox_pm_fraction: SOX_PM_FRACTION,
            sox_full: [0.0; N_FUEL_TYPES],
            altitude_factor: ALT_FACTOR,
            year_bins: [[(0, 0); N_RFG_BINS]; N_SEASONS],
            rfg_gs2: [[[0.0; 5]; N_RFG_BINS]; N_SEASONS],
            rfg_gs4: [[[0.0; 5]; N_RFG_BINS]; N_SEASONS],
        };

        // Episode sulfur content — `intadj.f` :66-71. Note that the
        // Fortran source maps GS2/GS4 to `soxgas`, LPG/CNG to
        // `soxcng`, and DSL to `soxdsl`; marine diesel (`soxdsm`)
        // is commented out and not used.
        tables.sox_full[FuelIndex::Gs2.idx()] = opts.sulfur_gasoline;
        tables.sox_full[FuelIndex::Gs4.idx()] = opts.sulfur_gasoline;
        tables.sox_full[FuelIndex::Lpg.idx()] = opts.sulfur_cng;
        tables.sox_full[FuelIndex::Cng.idx()] = opts.sulfur_cng;
        tables.sox_full[FuelIndex::Dsl.idx()] = opts.sulfur_diesel_land;

        // Summer year-range bins + adjustment factors — `intadj.f`
        // :79-116. Each bin has all five RFG pollutants set to 1.0
        // (the Fortran source uses 1.0 across the board as a
        // placeholder for future RFG adjustments).
        let summer = SeasonIndex::Summer.idx();
        let summer_bins: [(i32, i32); N_RFG_BINS] = [
            (1995, 1996), // bin 1
            (1997, 1999), // bin 2
            (2000, 9999), // bin 3
        ];
        for (bin, range) in summer_bins.iter().enumerate() {
            tables.year_bins[summer][bin] = *range;
            // Pollutant order: THC, CO, NOX, SOX, PM — same five
            // indices initialised in `intadj.f`.
            for pollutant_slot in 0..5 {
                tables.rfg_gs2[summer][bin][pollutant_slot] = 1.0;
                tables.rfg_gs4[summer][bin][pollutant_slot] = 1.0;
            }
        }

        // Winter year-range bin 1 — `intadj.f` :118-119. Note that
        // the Fortran source sets `iyrbin(IDXWTR,1,*)` but writes the
        // adjustment factors into `rfggs2(IDXWTR,3,*)` / `rfggs4(IDXWTR,3,*)`
        // — bin 3 for the factors, bin 1 for the year range. The
        // mismatch is in the original source; we preserve it
        // bit-for-bit.
        let winter = SeasonIndex::Winter.idx();
        tables.year_bins[winter][0] = (1995, 1996);
        for pollutant_slot in 0..5 {
            tables.rfg_gs2[winter][2][pollutant_slot] = 1.0;
            tables.rfg_gs4[winter][2][pollutant_slot] = 1.0;
        }

        tables
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::options::AltitudeFlag;

    fn sample_options() -> OptionsConfig {
        OptionsConfig {
            title1: "T1".into(),
            title2: "T2".into(),
            fuel_rvp: 9.0,
            oxygen_pct: 2.7,
            sulfur_gasoline: 0.030,
            sulfur_diesel_land: 0.0015,
            sulfur_diesel_marine: 0.0015,
            sulfur_cng: 0.0001,
            temp_min: 60.0,
            temp_max: 84.0,
            temp_mean: 72.0,
            altitude: AltitudeFlag::Low,
            ethanol_market_share: None,
            ethanol_vol_pct: None,
        }
    }

    #[test]
    fn baseline_sulfur_matches_fortran_constants() {
        let opts = sample_options();
        let t = AdjustmentTables::from_options(&opts);
        // SWTGS2 = SWTGS4 = 0.0339, SWTDSL = 0.33, SWTLPG = SWTCNG = 0.008
        assert!((t.sox_baseline[FuelIndex::Gs2.idx()] - 0.0339).abs() < 1e-6);
        assert!((t.sox_baseline[FuelIndex::Gs4.idx()] - 0.0339).abs() < 1e-6);
        assert!((t.sox_baseline[FuelIndex::Dsl.idx()] - 0.33).abs() < 1e-6);
        assert!((t.sox_baseline[FuelIndex::Lpg.idx()] - 0.008).abs() < 1e-6);
        assert!((t.sox_baseline[FuelIndex::Cng.idx()] - 0.008).abs() < 1e-6);
    }

    #[test]
    fn pm_fraction_matches_fortran_constants() {
        let opts = sample_options();
        let t = AdjustmentTables::from_options(&opts);
        // SFCGS2 = 0.03, SFCDSL = 0.02247
        assert!((t.sox_pm_fraction[FuelIndex::Gs2.idx()] - 0.03).abs() < 1e-6);
        assert!((t.sox_pm_fraction[FuelIndex::Dsl.idx()] - 0.02247).abs() < 1e-6);
    }

    #[test]
    fn altitude_factor_is_unity() {
        let opts = sample_options();
        let t = AdjustmentTables::from_options(&opts);
        for v in t.altitude_factor {
            assert!((v - 1.0).abs() < 1e-6);
        }
    }

    #[test]
    fn sox_full_maps_options() {
        let opts = sample_options();
        let t = AdjustmentTables::from_options(&opts);
        // Gasoline (GS2/GS4) -> soxgas
        assert!((t.sox_full[FuelIndex::Gs2.idx()] - opts.sulfur_gasoline).abs() < 1e-6);
        assert!((t.sox_full[FuelIndex::Gs4.idx()] - opts.sulfur_gasoline).abs() < 1e-6);
        // Diesel -> soxdsl
        assert!((t.sox_full[FuelIndex::Dsl.idx()] - opts.sulfur_diesel_land).abs() < 1e-6);
        // LPG/CNG -> soxcng
        assert!((t.sox_full[FuelIndex::Lpg.idx()] - opts.sulfur_cng).abs() < 1e-6);
        assert!((t.sox_full[FuelIndex::Cng.idx()] - opts.sulfur_cng).abs() < 1e-6);
    }

    #[test]
    fn summer_bins_match_fortran() {
        let opts = sample_options();
        let t = AdjustmentTables::from_options(&opts);
        let s = SeasonIndex::Summer.idx();
        assert_eq!(t.year_bins[s][0], (1995, 1996));
        assert_eq!(t.year_bins[s][1], (1997, 1999));
        assert_eq!(t.year_bins[s][2], (2000, 9999));
        for bin in 0..N_RFG_BINS {
            for p in 0..5 {
                assert!((t.rfg_gs2[s][bin][p] - 1.0).abs() < 1e-6);
                assert!((t.rfg_gs4[s][bin][p] - 1.0).abs() < 1e-6);
            }
        }
    }

    #[test]
    fn winter_bins_preserve_fortran_quirk() {
        // intadj.f sets `iyrbin(IDXWTR, 1, *)` but writes the
        // adjustment factors into `rfggs2(IDXWTR, 3, *)` — a year-range
        // for bin 1 but factors only at bin 3. Preserve the quirk.
        let opts = sample_options();
        let t = AdjustmentTables::from_options(&opts);
        let w = SeasonIndex::Winter.idx();
        assert_eq!(t.year_bins[w][0], (1995, 1996));
        // Bins 2 and 3 remain zero-initialised for the year range.
        assert_eq!(t.year_bins[w][1], (0, 0));
        assert_eq!(t.year_bins[w][2], (0, 0));
        // Adjustment factors live at bin index 2 (the Fortran "3").
        for p in 0..5 {
            assert!((t.rfg_gs2[w][2][p] - 1.0).abs() < 1e-6);
            assert!((t.rfg_gs4[w][2][p] - 1.0).abs() < 1e-6);
        }
        // Bin 0 and bin 1 stay at zero.
        for p in 0..5 {
            assert!(t.rfg_gs2[w][0][p] == 0.0);
            assert!(t.rfg_gs2[w][1][p] == 0.0);
            assert!(t.rfg_gs4[w][0][p] == 0.0);
            assert!(t.rfg_gs4[w][1][p] == 0.0);
        }
    }

    #[test]
    fn other_seasons_stay_zero() {
        let opts = sample_options();
        let t = AdjustmentTables::from_options(&opts);
        for s in [SeasonIndex::Spring, SeasonIndex::Fall] {
            let i = s.idx();
            for bin in 0..N_RFG_BINS {
                assert_eq!(t.year_bins[i][bin], (0, 0));
                for p in 0..5 {
                    assert!(t.rfg_gs2[i][bin][p] == 0.0);
                    assert!(t.rfg_gs4[i][bin][p] == 0.0);
                }
            }
        }
    }
}

//! Scrappage-curve retrieval — `getscrp.f` (107 lines).
//!
//! Returns either the default scrappage curve or one of the named
//! alternates loaded from the `/ALTERNATE SCRAPPAGE/` packet
//! (`rdalt.f`). If the requested curve name is not found in the
//! alternate set, the default is returned and a diagnostic is
//! produced (mirroring the warning the Fortran source writes to
//! `IOWMSG` and the `nwarn` bump).
//!
//! # Inputs
//!
//! - `default_curve`: the curve loaded from the `/SCRAPPAGE/`
//!   packet (`rdscrp.f`).
//! - `alternates`: optional alternate-curve table (`rdalt.f`).
//! - `name`: 10-character curve name. The reserved value
//!   `"DEFAULT"` always returns the default curve and never
//!   produces a warning.
//!
//! # Numerical-fidelity note
//!
//! The Fortran source copies the default values into the output
//! arrays *before* checking for an alternate match, so on alternate
//! lookup failure the caller receives the default curve. The Rust
//! port preserves that ordering exactly.

use crate::input::alt_scrap::AlternateScrappage;
use crate::input::scrappage::ScrappagePoint;

/// Re-export so callers don't have to thread through `crate::input`.
pub type ScrappageCurve = Vec<ScrappagePoint>;

/// Re-export so callers don't have to thread through `crate::input`.
pub type AlternateCurves = AlternateScrappage;

/// Selected curve + diagnostic.
#[derive(Debug, Clone)]
pub struct SelectedScrappage {
    /// The (bin, percent) curve to use.
    pub curve: ScrappageCurve,
    /// Non-empty when the requested name fell back to default
    /// because it was not present in the alternates.
    pub fallback_warning: Option<String>,
}

/// Reserved curve name that always returns the default curve.
///
/// Matches `getscrp.f` :71.
pub const DEFAULT_CURVE_NAME: &str = "DEFAULT";

/// Select a scrappage curve by name. See module-level docs.
pub fn select_scrappage(
    default_curve: &[ScrappagePoint],
    alternates: Option<&AlternateScrappage>,
    name: &str,
) -> SelectedScrappage {
    let trimmed = name.trim();

    // The Fortran path always populates the output arrays with the
    // default curve first (getscrp.f :67–70). Reproduce that here so
    // the fallback path returns the default without additional work.
    let default = default_curve.to_vec();

    if trimmed.eq_ignore_ascii_case(DEFAULT_CURVE_NAME) {
        return SelectedScrappage {
            curve: default,
            fallback_warning: None,
        };
    }

    let alts = match alternates {
        Some(a) => a,
        None => {
            return SelectedScrappage {
                curve: default,
                fallback_warning: Some(format!(
                    "alternate scrappage curve '{}' requested but no alternates loaded; \
                     using DEFAULT curve",
                    trimmed
                )),
            };
        }
    };

    let upper = trimmed.to_ascii_uppercase();
    let idx = alts
        .names
        .iter()
        .position(|n| n.eq_ignore_ascii_case(&upper));

    let idx = match idx {
        Some(i) => i,
        None => {
            return SelectedScrappage {
                curve: default,
                fallback_warning: Some(format!(
                    "alternate scrappage curve '{}' not found in /ALTERNATE SCRAPPAGE/; \
                     using DEFAULT curve",
                    trimmed
                )),
            };
        }
    };

    // Build the alternate curve from the column at `idx`. The
    // alternate-table bins are shared across all columns; each
    // column carries its own percent values per bin.
    let mut curve = Vec::with_capacity(alts.rows.len());
    for row in &alts.rows {
        let percent = row.percents.get(idx).copied().unwrap_or(0.0);
        curve.push(ScrappagePoint {
            bin: row.bin,
            percent,
        });
    }

    SelectedScrappage {
        curve,
        fallback_warning: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::alt_scrap::{AlternateScrappage, AlternateScrappageRow};

    fn default_curve() -> Vec<ScrappagePoint> {
        vec![
            ScrappagePoint { bin: 25.0, percent: 10.0 },
            ScrappagePoint { bin: 50.0, percent: 40.0 },
            ScrappagePoint { bin: 75.0, percent: 75.0 },
            ScrappagePoint { bin: 100.0, percent: 100.0 },
        ]
    }

    fn alts() -> AlternateScrappage {
        AlternateScrappage {
            names: vec!["EARLY".to_string(), "LATE".to_string()],
            rows: vec![
                AlternateScrappageRow { bin: 25.0, percents: vec![20.0, 5.0] },
                AlternateScrappageRow { bin: 50.0, percents: vec![60.0, 25.0] },
                AlternateScrappageRow { bin: 75.0, percents: vec![90.0, 60.0] },
                AlternateScrappageRow { bin: 100.0, percents: vec![100.0, 100.0] },
            ],
        }
    }

    #[test]
    fn default_name_returns_default_curve() {
        let r = select_scrappage(&default_curve(), Some(&alts()), "DEFAULT");
        assert!(r.fallback_warning.is_none());
        assert_eq!(r.curve, default_curve());
    }

    #[test]
    fn default_case_insensitive() {
        let r = select_scrappage(&default_curve(), Some(&alts()), "default");
        assert!(r.fallback_warning.is_none());
        assert_eq!(r.curve, default_curve());
    }

    #[test]
    fn named_alternate_returns_alternate_column() {
        let r = select_scrappage(&default_curve(), Some(&alts()), "EARLY");
        assert!(r.fallback_warning.is_none());
        assert_eq!(r.curve.len(), 4);
        assert_eq!(r.curve[0].bin, 25.0);
        assert_eq!(r.curve[0].percent, 20.0);
        assert_eq!(r.curve[3].percent, 100.0);
    }

    #[test]
    fn second_alternate_column() {
        let r = select_scrappage(&default_curve(), Some(&alts()), "LATE");
        assert_eq!(r.curve[1].percent, 25.0);
        assert_eq!(r.curve[2].percent, 60.0);
    }

    #[test]
    fn unknown_name_falls_back_to_default() {
        let r = select_scrappage(&default_curve(), Some(&alts()), "MISSING");
        assert!(r.fallback_warning.is_some());
        assert!(r.fallback_warning.as_ref().unwrap().contains("MISSING"));
        assert_eq!(r.curve, default_curve());
    }

    #[test]
    fn no_alternates_falls_back_to_default() {
        let r = select_scrappage(&default_curve(), None, "EARLY");
        assert!(r.fallback_warning.is_some());
        assert_eq!(r.curve, default_curve());
    }

    #[test]
    fn name_matching_case_insensitive() {
        let r = select_scrappage(&default_curve(), Some(&alts()), "early");
        assert!(r.fallback_warning.is_none());
        assert_eq!(r.curve[0].percent, 20.0);
    }
}

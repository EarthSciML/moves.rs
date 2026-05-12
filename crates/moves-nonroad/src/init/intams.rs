//! AMS output-parameter initialiser (`intams.f`).
//!
//! Task 99. Derives the EPS2-AMS output-file parameters from the
//! parsed `/PERIOD/` packet: report type, network type, reference and
//! base years, period code, period conversion factor, and the
//! `BBYYDDHH` begin/end date stamps.
//!
//! In the Fortran source the values land in COMMON variables
//! (`itype`, `inetyp`, `irefyr`, `ibasyr`, `cvtams`, `iperod`,
//! `ibegdt`, `ienddt` in `nonrdtpl.inc`). The Rust port returns them
//! as an owned [`AmsParams`] struct alongside any warnings produced
//! during the derivation.
//!
//! # Fortran source
//!
//! Ports `intams.f` (134 lines).

use crate::input::period::{DayKind, PeriodConfig, PeriodType, Season, SummaryType};

/// Per-month day count used to derive monthly period totals.
/// Matches the `data nodays` block at `intams.f` :50.
const DAYS_PER_MONTH: [i32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

/// Period code emitted into the AMS file header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeriodCode {
    /// Annual or month — empty string (`' '`).
    None,
    /// Seasonal (typical day not on summer/winter) — `S`.
    Season,
    /// Photochemical Ozone (summer typical day) — `PO`.
    PhotochemicalOzone,
    /// Photochemical CO (winter typical day) — `PC`.
    PhotochemicalCo,
}

impl PeriodCode {
    /// AMS code as a `'A1'` (one-character) string. Two-character
    /// codes return their full text.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => " ",
            Self::Season => "S",
            Self::PhotochemicalOzone => "PO",
            Self::PhotochemicalCo => "PC",
        }
    }
}

/// Begin/end date stamp in the Fortran `BBYYMMDDHH` packed format
/// (`yy*1000000 + mm*10000 + dd*100 + hh`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AmsDateStamp {
    /// Two-digit year (`yy`).
    pub yy: i32,
    /// Month, 1..=12.
    pub month: i32,
    /// Day of month.
    pub day: i32,
    /// Hour (`00` for begin, `24` for end).
    pub hour: i32,
}

impl AmsDateStamp {
    /// Pack the stamp into the Fortran wire format used in
    /// `ibegdt`/`ienddt`.
    pub fn packed(self) -> i32 {
        self.yy * 1_000_000 + self.month * 10_000 + self.day * 100 + self.hour
    }
}

/// AMS output parameters derived from the `/PERIOD/` packet.
#[derive(Debug, Clone)]
pub struct AmsParams {
    /// Report-type flag (`itype` in Fortran). Always `'B'`.
    pub report_type: char,
    /// Network-type flag (`inetyp` in Fortran). Always `'AC'`.
    pub network_type: &'static str,
    /// Two-digit reference year (`irefyr`).
    pub reference_year: i32,
    /// Two-digit base year (`ibasyr`); always equal to
    /// [`Self::reference_year`].
    pub base_year: i32,
    /// Period conversion factor (`cvtams`). Multiplies typical-day
    /// emissions to obtain the period total when [`SummaryType::TypicalDay`]
    /// is selected.
    pub conversion_factor: f32,
    /// Period code (`iperod`).
    pub period_code: PeriodCode,
    /// Begin date stamp (`ibegdt`).
    pub begin: AmsDateStamp,
    /// End date stamp (`ienddt`).
    pub end: AmsDateStamp,
    /// Non-fatal warnings produced during the derivation.
    pub warnings: Vec<String>,
}

/// Compute the AMS parameters for a parsed `/PERIOD/` packet.
///
/// Returns [`AmsParams::warnings`] populated with any of the
/// "typical-day output mismatched with period total" warnings the
/// Fortran source emits at the `6000` label of `intams.f`.
pub fn initialize_ams_params(period: &PeriodConfig) -> AmsParams {
    let reference_year = period.episode_year.rem_euclid(100);
    let base_year = reference_year;
    let mut params = AmsParams {
        report_type: 'B',
        network_type: "AC",
        reference_year,
        base_year,
        conversion_factor: 1.0,
        period_code: PeriodCode::Season,
        begin: AmsDateStamp {
            yy: reference_year,
            month: 1,
            day: 1,
            hour: 0,
        },
        end: AmsDateStamp {
            yy: reference_year,
            month: 12,
            day: 31,
            hour: 24,
        },
        warnings: Vec::new(),
    };

    let is_typical_day = matches!(period.summary_type, SummaryType::TypicalDay);

    match period.period_type {
        PeriodType::Annual => {
            // Empty period code, full year date range.
            params.period_code = PeriodCode::None;
            if is_typical_day {
                params.conversion_factor = 365.0;
            }
            params.begin = AmsDateStamp {
                yy: reference_year,
                month: 1,
                day: 1,
                hour: 0,
            };
            params.end = AmsDateStamp {
                yy: reference_year,
                month: 12,
                day: 31,
                hour: 24,
            };
            if is_typical_day {
                params.warnings.push(typical_day_warning());
            }
        }
        PeriodType::Seasonal => {
            if is_typical_day {
                params.conversion_factor = 91.21;
            }
            let season = period.season.unwrap_or(Season::Summer);
            let (begin, end) = seasonal_date_range(reference_year, season);
            params.begin = begin;
            params.end = end;
            if is_typical_day {
                match season {
                    Season::Summer => {
                        params.conversion_factor = 1.0;
                        params.period_code = PeriodCode::PhotochemicalOzone;
                    }
                    Season::Winter => {
                        params.conversion_factor = 1.0;
                        params.period_code = PeriodCode::PhotochemicalCo;
                    }
                    Season::Spring | Season::Fall => {
                        params.warnings.push(typical_day_warning());
                    }
                }
            }
        }
        PeriodType::Monthly => {
            let month = period.month.unwrap_or(1).clamp(1, 12) as i32;
            let days = DAYS_PER_MONTH[(month - 1) as usize];
            if is_typical_day {
                params.conversion_factor = days as f32;
            }
            params.begin = AmsDateStamp {
                yy: reference_year,
                month,
                day: 1,
                hour: 0,
            };
            params.end = AmsDateStamp {
                yy: reference_year,
                month,
                day: days,
                hour: 24,
            };
            if is_typical_day {
                params.warnings.push(typical_day_warning());
            }
        }
    }

    // Day-of-week is parsed but unused by AMS; reference it so
    // unused-field warnings stay quiet and the intent is documented.
    let _ = period.day_kind.unwrap_or(DayKind::Weekday);

    params
}

fn seasonal_date_range(yy: i32, season: Season) -> (AmsDateStamp, AmsDateStamp) {
    match season {
        // Winter spans the previous calendar year's December through
        // February — the Fortran source bumps `yy-1` for the begin
        // date stamp.
        Season::Winter => (
            AmsDateStamp {
                yy: yy - 1,
                month: 12,
                day: 1,
                hour: 0,
            },
            AmsDateStamp {
                yy,
                month: 2,
                day: 28,
                hour: 24,
            },
        ),
        Season::Spring => (
            AmsDateStamp {
                yy,
                month: 3,
                day: 1,
                hour: 0,
            },
            AmsDateStamp {
                yy,
                month: 5,
                day: 31,
                hour: 24,
            },
        ),
        Season::Summer => (
            AmsDateStamp {
                yy,
                month: 6,
                day: 1,
                hour: 0,
            },
            AmsDateStamp {
                yy,
                month: 8,
                day: 31,
                hour: 24,
            },
        ),
        Season::Fall => (
            AmsDateStamp {
                yy,
                month: 9,
                day: 1,
                hour: 0,
            },
            AmsDateStamp {
                yy,
                month: 11,
                day: 30,
                hour: 24,
            },
        ),
    }
}

fn typical_day_warning() -> String {
    // Matches the multiline warning emitted at intams.f label 6000.
    "WARNING: The output EPS file contains period total emissions. This does \
     NOT correspond to the user specified typical day emissions written to \
     the NONROAD output file."
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::period::{PeriodConfig, PeriodType, Season, SummaryType};

    fn base_period() -> PeriodConfig {
        PeriodConfig {
            period_type: PeriodType::Annual,
            summary_type: SummaryType::TotalPeriod,
            episode_year: 2025,
            season: None,
            month: None,
            day_kind: None,
            growth_year: 2025,
            technology_year: 2025,
            warnings: Vec::new(),
        }
    }

    #[test]
    fn annual_total_uses_unit_factor_and_no_warning() {
        let p = base_period();
        let r = initialize_ams_params(&p);
        assert_eq!(r.report_type, 'B');
        assert_eq!(r.network_type, "AC");
        assert_eq!(r.reference_year, 25);
        assert_eq!(r.base_year, 25);
        assert!((r.conversion_factor - 1.0).abs() < 1e-6);
        assert_eq!(r.period_code.as_str(), " ");
        assert_eq!(r.begin.packed(), 25_010_100);
        assert_eq!(r.end.packed(), 25_123_124);
        assert!(r.warnings.is_empty());
    }

    #[test]
    fn annual_typical_day_emits_warning_and_scales() {
        let mut p = base_period();
        p.summary_type = SummaryType::TypicalDay;
        let r = initialize_ams_params(&p);
        assert!((r.conversion_factor - 365.0).abs() < 1e-6);
        assert_eq!(r.warnings.len(), 1);
    }

    #[test]
    fn summer_typical_day_is_ozone_code() {
        let mut p = base_period();
        p.period_type = PeriodType::Seasonal;
        p.summary_type = SummaryType::TypicalDay;
        p.season = Some(Season::Summer);
        let r = initialize_ams_params(&p);
        // Summer typical day -> cvtams snaps to 1.0 and PO code.
        assert!((r.conversion_factor - 1.0).abs() < 1e-6);
        assert_eq!(r.period_code, PeriodCode::PhotochemicalOzone);
        assert!(r.warnings.is_empty());
        assert_eq!(r.begin.packed(), 25_060_100);
        assert_eq!(r.end.packed(), 25_083_124);
    }

    #[test]
    fn winter_typical_day_is_co_code_and_spans_prior_year() {
        let mut p = base_period();
        p.period_type = PeriodType::Seasonal;
        p.summary_type = SummaryType::TypicalDay;
        p.season = Some(Season::Winter);
        let r = initialize_ams_params(&p);
        assert_eq!(r.period_code, PeriodCode::PhotochemicalCo);
        assert_eq!(r.begin.yy, 24);
        assert_eq!(r.begin.month, 12);
        assert_eq!(r.end.yy, 25);
        assert_eq!(r.end.month, 2);
    }

    #[test]
    fn spring_typical_day_warns_but_keeps_factor() {
        let mut p = base_period();
        p.period_type = PeriodType::Seasonal;
        p.summary_type = SummaryType::TypicalDay;
        p.season = Some(Season::Spring);
        let r = initialize_ams_params(&p);
        // Conversion factor sticks at 91.21 for non-summer/winter typical days.
        assert!((r.conversion_factor - 91.21).abs() < 1e-3);
        assert_eq!(r.period_code, PeriodCode::Season);
        assert_eq!(r.warnings.len(), 1);
    }

    #[test]
    fn monthly_uses_days_in_month_for_typical_day() {
        let mut p = base_period();
        p.period_type = PeriodType::Monthly;
        p.summary_type = SummaryType::TypicalDay;
        p.month = Some(7); // July
        let r = initialize_ams_params(&p);
        assert!((r.conversion_factor - 31.0).abs() < 1e-6);
        assert_eq!(r.begin.packed(), 25_070_100);
        assert_eq!(r.end.packed(), 25_073_124);
        assert_eq!(r.warnings.len(), 1);
    }

    #[test]
    fn monthly_total_uses_unit_factor() {
        let mut p = base_period();
        p.period_type = PeriodType::Monthly;
        p.summary_type = SummaryType::TotalPeriod;
        p.month = Some(2);
        let r = initialize_ams_params(&p);
        assert!((r.conversion_factor - 1.0).abs() < 1e-6);
        // Feb 28 with 24-hour end.
        assert_eq!(r.end.packed(), 25_022_824);
    }

    #[test]
    fn handles_episode_year_with_century_remainder() {
        let mut p = base_period();
        p.episode_year = 1999;
        let r = initialize_ams_params(&p);
        assert_eq!(r.reference_year, 99);
        assert_eq!(r.base_year, 99);
    }
}

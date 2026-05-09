//! Period-packet parser (`rdnrper.f`).
//!
//! Task 97. Parses the `/PERIOD/` packet from the options file. The
//! packet defines:
//!
//! - period type (annual / monthly / seasonal),
//! - sum type (typical-day / total-period),
//! - episode year (mandatory),
//! - season (only for seasonal),
//! - month (for monthly or RFG runs),
//! - day-of-week (only for typical-day),
//! - growth year (defaults to episode year),
//! - technology year (defaults to episode year, must be ≤ episode year).
//!
//! Years specified with two digits map per the Fortran convention
//! (`> 50` → 1900s, otherwise 2000s) and must lie in `MINYEAR..=MAXYEAR`
//! (defined here as `1970..=2050`). The Rust port surfaces the
//! parsed values; cross-field warnings ("growth year doesn't match
//! episode year") are returned in [`PeriodConfig::warnings`].
//!
//! # Format (one keyword per line, value in column 21+)
//!
//! ```text
//! /PERIOD/
//! Period Type        : ANNUAL
//! Summary Type       : TYPICAL DAY
//! Episode Year       : 2020
//! Season             : SUMMER
//! Month              : JANUARY
//! Day                : WEEKDAY
//! Growth Year        : 2020
//! Technology Year    : 2020
//! /END/
//! ```
//!
//! # Fortran source
//!
//! Ports `rdnrper.f` (494 lines).

use crate::{Error, Result};
use std::io::BufRead;
use std::path::PathBuf;

/// Period type (annual / monthly / seasonal).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeriodType {
    /// Annual inventory.
    Annual,
    /// Single-month inventory.
    Monthly,
    /// Seasonal inventory.
    Seasonal,
}

/// Summary type (typical-day vs. total-period).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SummaryType {
    /// Typical-day inventory.
    TypicalDay,
    /// Total-period inventory.
    TotalPeriod,
}

/// Season (only meaningful for [`PeriodType::Seasonal`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Season {
    /// December–February.
    Winter,
    /// March–May.
    Spring,
    /// June–August.
    Summer,
    /// September–November.
    Fall,
}

/// Day-of-week selector (only meaningful for typical-day).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DayKind {
    /// Weekday.
    Weekday,
    /// Weekend.
    Weekend,
}

/// Inclusive year range allowed by the Fortran source.
pub const MIN_YEAR: i32 = 1970;
/// Inclusive year range allowed by the Fortran source.
pub const MAX_YEAR: i32 = 2050;

/// Parsed `/PERIOD/` packet.
#[derive(Debug, Clone)]
pub struct PeriodConfig {
    /// Period type.
    pub period_type: PeriodType,
    /// Summary type.
    pub summary_type: SummaryType,
    /// Episode year (4-digit).
    pub episode_year: i32,
    /// Season (only when `period_type == Seasonal`).
    pub season: Option<Season>,
    /// Month index 1..=12 (only when `period_type == Monthly`).
    pub month: Option<u8>,
    /// Day-of-week kind (only for typical-day summaries).
    pub day_kind: Option<DayKind>,
    /// Growth year (defaults to episode year).
    pub growth_year: i32,
    /// Technology year (defaults to episode year).
    pub technology_year: i32,
    /// Non-fatal warnings produced during parsing.
    pub warnings: Vec<String>,
}

/// Parse a period packet.
pub fn read_period<R: BufRead>(reader: R) -> Result<PeriodConfig> {
    let path = PathBuf::from(".OPT");
    let mut in_packet = false;
    let mut values: Vec<(String, usize)> = Vec::new();
    let mut line_num = 0;

    for line_result in reader.lines() {
        line_num += 1;
        let line = line_result.map_err(|e| Error::Io {
            path: path.clone(),
            source: e,
        })?;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let upper = trimmed.to_ascii_uppercase();
        if upper.starts_with("/PERIOD/") {
            in_packet = true;
            continue;
        }
        if upper.starts_with("/END/") {
            in_packet = false;
            continue;
        }
        if !in_packet {
            continue;
        }

        // The Fortran source reads the value field starting at column 21.
        // Any text after a `:` separator works equally well in practice.
        let value = extract_value(&line);
        values.push((value, line_num));
    }

    if !in_packet && values.is_empty() {
        return Err(Error::Parse {
            file: path,
            line: line_num,
            message: "missing /PERIOD/ packet".to_string(),
        });
    }

    if values.len() < 8 {
        return Err(Error::Parse {
            file: path,
            line: line_num,
            message: format!(
                "expected 8 records in /PERIOD/ packet, got {}",
                values.len()
            ),
        });
    }

    let (period_raw, period_line) = &values[0];
    let period_type = match period_raw.to_ascii_uppercase().as_str() {
        "ANNUAL" => PeriodType::Annual,
        "MONTHLY" => PeriodType::Monthly,
        "SEASONAL" => PeriodType::Seasonal,
        other => {
            return Err(Error::Parse {
                file: path,
                line: *period_line,
                message: format!("invalid period type: {}", other),
            });
        }
    };

    let (sum_raw, sum_line) = &values[1];
    let summary_type = match sum_raw.to_ascii_uppercase().as_str() {
        "TYPICAL DAY" | "TYPICAL_DAY" | "TYPICAL" => SummaryType::TypicalDay,
        "TOTAL PERIOD" | "TOTAL_PERIOD" | "TOTAL" => SummaryType::TotalPeriod,
        other => {
            return Err(Error::Parse {
                file: path,
                line: *sum_line,
                message: format!("invalid summary type: {}", other),
            });
        }
    };

    let (year_raw, year_line) = &values[2];
    let episode_year = parse_year(year_raw, "episode_year", *year_line, &path)?;

    let season = match (period_type, &values[3]) {
        (PeriodType::Seasonal, (raw, l)) => Some(parse_season(raw, *l, &path)?),
        _ => None,
    };
    let month = match (period_type, &values[4]) {
        (PeriodType::Monthly, (raw, l)) => Some(parse_month(raw, *l, &path)?),
        _ => None,
    };
    let day_kind = match (summary_type, &values[5]) {
        (SummaryType::TypicalDay, (raw, l)) => Some(parse_day_kind(raw, *l, &path)?),
        _ => None,
    };

    let (growth_raw, growth_line) = &values[6];
    let growth_year = if growth_raw.trim().is_empty() {
        episode_year
    } else {
        parse_year(growth_raw, "growth_year", *growth_line, &path)?
    };

    let (tech_raw, tech_line) = &values[7];
    let technology_year = if tech_raw.trim().is_empty() {
        episode_year
    } else {
        parse_year(tech_raw, "technology_year", *tech_line, &path)?
    };

    if technology_year > episode_year {
        return Err(Error::Parse {
            file: path,
            line: *tech_line,
            message: format!(
                "technology year ({}) cannot exceed episode year ({})",
                technology_year, episode_year
            ),
        });
    }

    let mut warnings = Vec::new();
    if growth_year != episode_year {
        warnings.push(format!(
            "growth year ({}) differs from episode year ({})",
            growth_year, episode_year
        ));
    }
    if technology_year != episode_year {
        warnings.push(format!(
            "technology year ({}) differs from episode year ({})",
            technology_year, episode_year
        ));
    }

    Ok(PeriodConfig {
        period_type,
        summary_type,
        episode_year,
        season,
        month,
        day_kind,
        growth_year,
        technology_year,
        warnings,
    })
}

fn extract_value(line: &str) -> String {
    if let Some(idx) = line.find(':') {
        line[idx + 1..].trim().to_string()
    } else if line.len() > 20 {
        line[20..].trim().to_string()
    } else {
        line.trim().to_string()
    }
}

fn parse_year(raw: &str, name: &str, line: usize, path: &std::path::Path) -> Result<i32> {
    let trimmed = raw.trim();
    let v: i32 = trimmed.parse().map_err(|_| Error::Parse {
        file: path.to_path_buf(),
        line,
        message: format!("invalid {}: {}", name, raw),
    })?;
    let year = if v < 100 {
        if v > 50 {
            v + 1900
        } else {
            v + 2000
        }
    } else {
        v
    };
    if !(MIN_YEAR..=MAX_YEAR).contains(&year) {
        return Err(Error::Parse {
            file: path.to_path_buf(),
            line,
            message: format!(
                "{} {} outside valid range [{}, {}]",
                name, year, MIN_YEAR, MAX_YEAR
            ),
        });
    }
    Ok(year)
}

fn parse_season(raw: &str, line: usize, path: &std::path::Path) -> Result<Season> {
    match raw.to_ascii_uppercase().as_str() {
        "WINTER" => Ok(Season::Winter),
        "SPRING" => Ok(Season::Spring),
        "SUMMER" => Ok(Season::Summer),
        "FALL" | "AUTUMN" => Ok(Season::Fall),
        other => Err(Error::Parse {
            file: path.to_path_buf(),
            line,
            message: format!("invalid season: {}", other),
        }),
    }
}

fn parse_month(raw: &str, line: usize, path: &std::path::Path) -> Result<u8> {
    let m = match raw.to_ascii_uppercase().as_str() {
        "JANUARY" | "JAN" => 1,
        "FEBRUARY" | "FEB" => 2,
        "MARCH" | "MAR" => 3,
        "APRIL" | "APR" => 4,
        "MAY" => 5,
        "JUNE" | "JUN" => 6,
        "JULY" | "JUL" => 7,
        "AUGUST" | "AUG" => 8,
        "SEPTEMBER" | "SEP" => 9,
        "OCTOBER" | "OCT" => 10,
        "NOVEMBER" | "NOV" => 11,
        "DECEMBER" | "DEC" => 12,
        other => {
            return Err(Error::Parse {
                file: path.to_path_buf(),
                line,
                message: format!("invalid month: {}", other),
            });
        }
    };
    Ok(m)
}

fn parse_day_kind(raw: &str, line: usize, path: &std::path::Path) -> Result<DayKind> {
    match raw.to_ascii_uppercase().as_str() {
        "WEEKDAY" | "WEEKDAYS" => Ok(DayKind::Weekday),
        "WEEKEND" => Ok(DayKind::Weekend),
        other => Err(Error::Parse {
            file: path.to_path_buf(),
            line,
            message: format!("invalid day kind: {}", other),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_annual() {
        let input = "\
/PERIOD/
Period Type        : ANNUAL
Summary Type       : TOTAL PERIOD
Episode Year       : 2025
Season             :
Month              :
Day                :
Growth Year        :
Technology Year    :
/END/
";
        let cfg = read_period(input.as_bytes()).unwrap();
        assert_eq!(cfg.period_type, PeriodType::Annual);
        assert_eq!(cfg.summary_type, SummaryType::TotalPeriod);
        assert_eq!(cfg.episode_year, 2025);
        assert_eq!(cfg.growth_year, 2025);
        assert_eq!(cfg.technology_year, 2025);
        assert!(cfg.warnings.is_empty());
    }

    #[test]
    fn parses_seasonal_typical_day() {
        let input = "\
/PERIOD/
Period Type        : SEASONAL
Summary Type       : TYPICAL DAY
Episode Year       : 2020
Season             : SUMMER
Month              :
Day                : WEEKDAY
Growth Year        : 2020
Technology Year    : 2018
/END/
";
        let cfg = read_period(input.as_bytes()).unwrap();
        assert_eq!(cfg.season, Some(Season::Summer));
        assert_eq!(cfg.day_kind, Some(DayKind::Weekday));
        assert_eq!(cfg.technology_year, 2018);
        assert_eq!(cfg.warnings.len(), 1);
    }

    #[test]
    fn two_digit_year_expands() {
        let input = "\
/PERIOD/
Period Type        : ANNUAL
Summary Type       : TOTAL PERIOD
Episode Year       : 25
Season             :
Month              :
Day                :
Growth Year        :
Technology Year    :
/END/
";
        let cfg = read_period(input.as_bytes()).unwrap();
        assert_eq!(cfg.episode_year, 2025);
    }

    #[test]
    fn rejects_tech_year_after_episode() {
        let input = "\
/PERIOD/
Period Type        : ANNUAL
Summary Type       : TOTAL PERIOD
Episode Year       : 2020
Season             :
Month              :
Day                :
Growth Year        :
Technology Year    : 2030
/END/
";
        let err = read_period(input.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("cannot exceed")),
            other => panic!("unexpected error: {other:?}"),
        }
    }
}

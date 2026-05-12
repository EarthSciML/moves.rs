//! Activity-file parser (`rdact.f`).
//!
//! Task 97. Parses the activity-data input file. It contains two
//! packets: `/ACTIVITY/` records (one per SCC/HP-range) carrying
//! load factor, units, activity level, and an age-curve identifier;
//! and `/AGE ADJUSTMENT/` records expressing percent of new activity
//! versus useful-life bins.
//!
//! # Format (whitespace-delimited)
//!
//! ```text
//! /ACTIVITY/
//! <scc> <equipment> <sub> <hp_min> <hp_max> <load_factor> <units> <activity> <age_id>
//! ...
//! /END/
//! /AGE ADJUSTMENT/
//! <bin> <pct1> <pct2> ... <pctN>
//! ...
//! /END/
//! ```
//!
//! Units: `HRY`, `HRD`, `GLY`, or `GLD` (hours/year, hours/day,
//! gallons/year, gallons/day).
//!
//! # Fortran source
//!
//! Ports `rdact.f` (434 lines).

use crate::{Error, Result};
use std::io::BufRead;
use std::path::PathBuf;

/// One `/ACTIVITY/` packet record.
#[derive(Debug, Clone, PartialEq)]
pub struct ActivityRecord {
    /// SCC (Source Classification Code), 10 characters left-justified.
    pub scc: String,
    /// Subcategory string (5 characters).
    pub sub: String,
    /// Horsepower range minimum.
    pub hp_min: f32,
    /// Horsepower range maximum.
    pub hp_max: f32,
    /// Load factor (fraction).
    pub load_factor: f32,
    /// Activity-units indicator: `HRY`, `HRD`, `GLY`, or `GLD`.
    pub units: ActivityUnits,
    /// Activity level value.
    pub activity_level: f32,
    /// Activity-vs-age curve identifier (10 characters).
    pub age_curve_id: String,
}

/// Activity units encoded in the `/ACTIVITY/` packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityUnits {
    /// Hours per year.
    HoursPerYear,
    /// Hours per day.
    HoursPerDay,
    /// Gallons per year.
    GallonsPerYear,
    /// Gallons per day.
    GallonsPerDay,
}

impl ActivityUnits {
    fn from_token(s: &str) -> Option<Self> {
        match s.to_ascii_uppercase().as_str() {
            "HRY" => Some(Self::HoursPerYear),
            "HRD" => Some(Self::HoursPerDay),
            "GLY" => Some(Self::GallonsPerYear),
            "GLD" => Some(Self::GallonsPerDay),
            _ => None,
        }
    }
}

/// One `/AGE ADJUSTMENT/` packet record.
#[derive(Debug, Clone, PartialEq)]
pub struct AgeAdjustmentRecord {
    /// Useful-life bin (e.g., 25.0 for 25%).
    pub bin: f32,
    /// Percent-of-new-activity values for each named age column.
    pub percents: Vec<f32>,
}

/// Parsed `.ACT` content.
#[derive(Debug, Default, Clone)]
pub struct ActivityFile {
    /// `/ACTIVITY/` records.
    pub activity: Vec<ActivityRecord>,
    /// Column labels from the `/AGE ADJUSTMENT/` packet.
    pub age_columns: Vec<String>,
    /// `/AGE ADJUSTMENT/` records.
    pub age_adjustment: Vec<AgeAdjustmentRecord>,
}

/// Parse the activity input file.
pub fn read_act<R: BufRead>(reader: R) -> Result<ActivityFile> {
    let mut out = ActivityFile::default();
    let path = PathBuf::from(".ACT");
    let mut state = Section::None;
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
        if upper.starts_with("/ACTIVITY/") {
            state = Section::Activity;
            continue;
        }
        if upper.starts_with("/AGE ADJUSTMENT/") {
            state = Section::AgeHeader;
            continue;
        }
        if upper.starts_with("/END/") {
            state = Section::None;
            continue;
        }

        match state {
            Section::None => continue,
            Section::Activity => {
                let parts: Vec<&str> = trimmed.split_whitespace().collect();
                if parts.len() < 9 {
                    return Err(Error::Parse {
                        file: path.clone(),
                        line: line_num,
                        message: format!(
                            "expected 9 fields in /ACTIVITY/ record, got {}",
                            parts.len()
                        ),
                    });
                }
                let units = ActivityUnits::from_token(parts[6]).ok_or_else(|| Error::Parse {
                    file: path.clone(),
                    line: line_num,
                    message: format!("unknown activity units: {}", parts[6]),
                })?;
                out.activity.push(ActivityRecord {
                    scc: parts[0].to_string(),
                    sub: parts[2].to_string(),
                    hp_min: parse_field(parts[3], "hp_min", line_num, &path)?,
                    hp_max: parse_field(parts[4], "hp_max", line_num, &path)?,
                    load_factor: parse_field(parts[5], "load_factor", line_num, &path)?,
                    units,
                    activity_level: parse_field(parts[7], "activity_level", line_num, &path)?,
                    age_curve_id: parts[8].to_string(),
                });
            }
            Section::AgeHeader => {
                out.age_columns = trimmed
                    .split_whitespace()
                    .map(|s| s.to_ascii_uppercase())
                    .collect();
                state = Section::AgeAdjustment;
            }
            Section::AgeAdjustment => {
                let parts: Vec<&str> = trimmed.split_whitespace().collect();
                if parts.is_empty() {
                    continue;
                }
                let bin: f32 = parse_field(parts[0], "bin", line_num, &path)?;
                let percents: Vec<f32> = parts[1..]
                    .iter()
                    .map(|t| parse_field(t, "percent", line_num, &path))
                    .collect::<Result<_>>()?;
                if let Some(prev) = out.age_adjustment.last() {
                    if bin < prev.bin {
                        return Err(Error::Parse {
                            file: path.clone(),
                            line: line_num,
                            message: "age bins must be monotonically non-decreasing".to_string(),
                        });
                    }
                }
                out.age_adjustment
                    .push(AgeAdjustmentRecord { bin, percents });
            }
        }
    }

    Ok(out)
}

#[derive(Debug, Clone, Copy)]
enum Section {
    None,
    Activity,
    AgeHeader,
    AgeAdjustment,
}

fn parse_field(token: &str, name: &str, line_num: usize, path: &std::path::Path) -> Result<f32> {
    token.parse::<f32>().map_err(|_| Error::Parse {
        file: path.to_path_buf(),
        line: line_num,
        message: format!("invalid {}: {}", name, token),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_activity_packet() {
        let input = "\
/ACTIVITY/
2270001000 LawnEquip BASE 0.0 25.0 0.68 HRY 65.0 DEFAULT
2270002000 Pumps    BASE 25.0 50.0 0.43 GLY 100.0 PUMPS
/END/
/AGE ADJUSTMENT/
   USEFUL    SCRAP
   25.0   100.0
   50.0    80.0
   75.0    50.0
/END/
";
        let parsed = read_act(input.as_bytes()).unwrap();
        assert_eq!(parsed.activity.len(), 2);
        assert_eq!(parsed.activity[0].scc, "2270001000");
        assert_eq!(parsed.activity[0].units, ActivityUnits::HoursPerYear);
        assert!((parsed.activity[0].load_factor - 0.68).abs() < 1e-6);
        assert_eq!(parsed.activity[1].units, ActivityUnits::GallonsPerYear);

        assert_eq!(parsed.age_columns, vec!["USEFUL", "SCRAP"]);
        assert_eq!(parsed.age_adjustment.len(), 3);
        assert!((parsed.age_adjustment[2].percents[0] - 50.0).abs() < 1e-6);
    }

    #[test]
    fn rejects_unknown_units() {
        let input = "\
/ACTIVITY/
2270001000 LawnEquip BASE 0.0 25.0 0.68 XYZ 65.0 DEFAULT
/END/
";
        let err = read_act(input.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("unknown activity units")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn rejects_decreasing_bin() {
        let input = "\
/AGE ADJUSTMENT/
   USEFUL
   50.0   80.0
   25.0   100.0
/END/
";
        let err = read_act(input.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("monotonically")),
            other => panic!("unexpected error: {other:?}"),
        }
    }
}

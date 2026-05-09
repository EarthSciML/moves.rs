//! Deterioration-factor parser (`rddetr.f`).
//!
//! Task 97. Parses a deterioration-factor file used to model emissions
//! drift with engine age. Each record provides coefficients to the
//! equation `DF = 1 + A * age^B`, with a cap on the result.
//!
//! # Format
//!
//! ```text
//! /DETFAC/
//! <tech_type> <a> <b> <cap> <pollutant_name>
//! ...
//! /END/
//! ```
//!
//! - `tech_type`: 10-character technology identifier
//! - `a`, `b`: coefficients
//! - `cap`: maximum deterioration multiplier
//! - `pollutant_name`: must match the file's pollutant
//!
//! # Fortran source
//!
//! Ports `rddetr.f` (209 lines).

use crate::{Error, Result};
use std::io::BufRead;
use std::path::PathBuf;

/// One deterioration-factor record.
#[derive(Debug, Clone, PartialEq)]
pub struct DeteriorationRecord {
    /// Technology type (10-character key, upper-cased).
    pub tech_type: String,
    /// Coefficient `A` in `DF = 1 + A * age^B`.
    pub a: f32,
    /// Coefficient `B`.
    pub b: f32,
    /// Cap on the deterioration multiplier.
    pub cap: f32,
    /// Pollutant name associated with the file.
    pub pollutant: String,
}

/// Parse a deterioration-factor file.
pub fn read_detr<R: BufRead>(reader: R) -> Result<Vec<DeteriorationRecord>> {
    let mut records = Vec::new();
    let path = PathBuf::from(".DET");
    let mut in_packet = false;
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
        if upper.starts_with("/DETFAC/") {
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

        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        if parts.len() < 5 {
            return Err(Error::Parse {
                file: path.clone(),
                line: line_num,
                message: format!(
                    "expected 5 fields (tech, a, b, cap, pollutant), got {}",
                    parts.len()
                ),
            });
        }
        records.push(DeteriorationRecord {
            tech_type: parts[0].to_ascii_uppercase(),
            a: parse_f32(parts[1], "a", line_num, &path)?,
            b: parse_f32(parts[2], "b", line_num, &path)?,
            cap: parse_f32(parts[3], "cap", line_num, &path)?,
            pollutant: parts[4].to_ascii_uppercase(),
        });
    }

    Ok(records)
}

/// Evaluate `1 + A * age^B`, clamped at `cap`.
pub fn deterioration_multiplier(record: &DeteriorationRecord, age: f32) -> f32 {
    let raw = 1.0 + record.a * age.max(0.0).powf(record.b);
    raw.min(record.cap)
}

fn parse_f32(token: &str, name: &str, line_num: usize, path: &std::path::Path) -> Result<f32> {
    token.parse().map_err(|_| Error::Parse {
        file: path.to_path_buf(),
        line: line_num,
        message: format!("invalid {}: {}", name, token),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_detfac_packet() {
        let input = "\
/DETFAC/
BASE      0.04 1.0 1.5 HC
ADV       0.02 1.0 1.3 HC
/END/
";
        let records = read_detr(input.as_bytes()).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].tech_type, "BASE");
        assert!((records[0].a - 0.04).abs() < 1e-6);
        assert_eq!(records[1].tech_type, "ADV");
    }

    #[test]
    fn computes_capped_multiplier() {
        let r = DeteriorationRecord {
            tech_type: "BASE".into(),
            a: 0.1,
            b: 1.0,
            cap: 1.5,
            pollutant: "HC".into(),
        };
        assert!((deterioration_multiplier(&r, 0.0) - 1.0).abs() < 1e-6);
        assert!((deterioration_multiplier(&r, 1.0) - 1.1).abs() < 1e-6);
        // capped
        assert!((deterioration_multiplier(&r, 100.0) - 1.5).abs() < 1e-6);
    }

    #[test]
    fn rejects_short_record() {
        let input = "\
/DETFAC/
BASE 0.04 1.0
/END/
";
        let err = read_detr(input.as_bytes()).unwrap_err();
        assert!(matches!(err, Error::Parse { .. }));
    }
}

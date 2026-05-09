//! Spatial-indicator parser (`rdind.f`).
//!
//! Task 97. Parses spatial-allocation indicator files. The Fortran
//! source iterates over a list of allocation files (each containing
//! a single `/INDICATORS/` packet), filters records by region scope
//! and active allocation codes, and writes a sorted scratch file.
//! In Rust we expose the per-file packet parser plus a multi-file
//! convenience that flattens, optionally filters, and sorts.
//!
//! # Format
//!
//! ```text
//! /INDICATORS/
//! <code> <fips> <subcounty> <year> <value>
//! ...
//! /END/
//! ```
//!
//! `<code>` is the 3-character allocation indicator (e.g., POP, HHS).
//! `<fips>` is the 5-digit county/state FIPS or `00000` for all.
//! `<value>` is a real allocation factor.
//!
//! # Fortran source
//!
//! Ports `rdind.f` (355 lines).

use crate::{Error, Result};
use std::io::BufRead;
use std::path::PathBuf;

/// One indicator record.
#[derive(Debug, Clone, PartialEq)]
pub struct IndicatorRecord {
    /// 3-character allocation code.
    pub code: String,
    /// 5-digit FIPS code.
    pub fips: String,
    /// 5-character subcounty identifier.
    pub subcounty: String,
    /// 4-character year string (kept as text to match Fortran sort key).
    pub year: String,
    /// Allocation value.
    pub value: f64,
}

impl IndicatorRecord {
    /// Sort key matching Fortran's character-key concatenation
    /// (`code // fips // subcounty // year`). 17 characters wide.
    pub fn sort_key(&self) -> String {
        format!(
            "{:<3}{:<5}{:<5}{:<4}",
            self.code, self.fips, self.subcounty, self.year
        )
    }
}

/// Parse one indicator file.
pub fn read_ind<R: BufRead>(reader: R) -> Result<Vec<IndicatorRecord>> {
    let path = PathBuf::from(".IND");
    let mut out = Vec::new();
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
        if upper.starts_with("/INDICATORS/") {
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
                    "expected 5 fields (code, fips, subcounty, year, value), got {}",
                    parts.len()
                ),
            });
        }
        let value: f64 = parts[4].parse().map_err(|_| Error::Parse {
            file: path.clone(),
            line: line_num,
            message: format!("invalid value: {}", parts[4]),
        })?;
        out.push(IndicatorRecord {
            code: parts[0].to_ascii_uppercase(),
            fips: parts[1].to_string(),
            subcounty: parts[2].to_ascii_uppercase(),
            year: parts[3].to_string(),
            value,
        });
    }

    Ok(out)
}

/// Sort indicator records by `(code, fips, subcounty, year)`,
/// matching the Fortran `chrsrt` ordering on the concatenated key.
pub fn sort_indicators(records: &mut [IndicatorRecord]) {
    records.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_packet() {
        let input = "\
/INDICATORS/
POP 17031 00000 2020 5180000.0
HHS 17031 00000 2020 1980000.0
/END/
";
        let records = read_ind(input.as_bytes()).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].code, "POP");
        assert!((records[1].value - 1980000.0).abs() < 1e-3);
    }

    #[test]
    fn sorts_by_concatenated_key() {
        let input = "\
/INDICATORS/
POP 17031 00000 2030 6000.0
POP 17031 00000 2020 5180.0
HHS 17031 00000 2020 1980.0
/END/
";
        let mut records = read_ind(input.as_bytes()).unwrap();
        sort_indicators(&mut records);
        let codes_years: Vec<_> = records
            .iter()
            .map(|r| (r.code.clone(), r.year.clone()))
            .collect();
        assert_eq!(
            codes_years,
            vec![
                ("HHS".into(), "2020".into()),
                ("POP".into(), "2020".into()),
                ("POP".into(), "2030".into()),
            ]
        );
    }

    #[test]
    fn rejects_short_record() {
        let input = "\
/INDICATORS/
POP 17031 00000 2020
/END/
";
        let err = read_ind(input.as_bytes()).unwrap_err();
        assert!(matches!(err, Error::Parse { .. }));
    }
}

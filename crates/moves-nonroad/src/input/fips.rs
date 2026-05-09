//! FIPS-county parser (`rdfips.f`).
//!
//! Task 97. Parses the `/FIPS/` packet from a FIPS data file. Each
//! record carries a county FIPS code, an optional start/end year
//! span, and a county name. Records whose year span excludes the
//! current episode year are filtered out (callers supply the
//! episode year via [`read_fips_for_year`]).
//!
//! # Format
//!
//! ```text
//! /FIPS/
//! <fips> <yr_start> <yr_end> <county_name>
//! ...
//! /END/
//! ```
//!
//! 4-digit FIPS codes are zero-padded to 5 digits; shorter codes are
//! rejected. Year `0` means "no bound on that side."
//!
//! # Fortran source
//!
//! Ports `rdfips.f` (238 lines).

use crate::{Error, Result};
use std::io::BufRead;
use std::path::PathBuf;

/// One county record from the FIPS file.
#[derive(Debug, Clone, PartialEq)]
pub struct FipsRecord {
    /// 5-digit FIPS code (zero-padded).
    pub fips: String,
    /// Start year, or `None` if `0` in the file (open-ended).
    pub year_start: Option<i32>,
    /// End year, or `None` if `0` in the file (open-ended).
    pub year_end: Option<i32>,
    /// County name.
    pub name: String,
}

impl FipsRecord {
    /// Two-character state-FIPS prefix.
    pub fn state_prefix(&self) -> &str {
        &self.fips[..2]
    }

    /// Whether this record applies in `episode_year`.
    pub fn applies_in(&self, episode_year: i32) -> bool {
        if let Some(start) = self.year_start {
            if episode_year < start {
                return false;
            }
        }
        if let Some(end) = self.year_end {
            if episode_year > end {
                return false;
            }
        }
        true
    }
}

/// Parse a FIPS file, returning every record.
pub fn read_fips<R: BufRead>(reader: R) -> Result<Vec<FipsRecord>> {
    let path = PathBuf::from(".FIPS");
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
        if upper.starts_with("/FIPS/") {
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

        let mut tokens = trimmed.split_whitespace();
        let fips_raw = tokens.next().ok_or_else(|| Error::Parse {
            file: path.clone(),
            line: line_num,
            message: "expected fips field, got empty line".to_string(),
        })?;
        let yr_start_raw = tokens.next().ok_or_else(|| Error::Parse {
            file: path.clone(),
            line: line_num,
            message: "expected yr_start field".to_string(),
        })?;
        let yr_end_raw = tokens.next().ok_or_else(|| Error::Parse {
            file: path.clone(),
            line: line_num,
            message: "expected yr_end field".to_string(),
        })?;
        // The remainder of the line (after the third whitespace run) is the name.
        // Reconstruct it from the remaining iterator collected verbatim — the
        // Fortran reads `A50` so internal whitespace is preserved.
        let name_remainder: String = tokens.collect::<Vec<_>>().join(" ");
        let fips = match fips_raw.len() {
            5 => fips_raw.to_string(),
            4 => format!("0{}", fips_raw),
            n => {
                return Err(Error::Parse {
                    file: path.clone(),
                    line: line_num,
                    message: format!("FIPS must be 4 or 5 digits, got {} ({:?})", n, fips_raw),
                });
            }
        };
        let yr_start = parse_optional_year(yr_start_raw, "yr_start", line_num, &path)?;
        let yr_end = parse_optional_year(yr_end_raw, "yr_end", line_num, &path)?;
        let name = name_remainder.trim().to_string();
        if name.is_empty() {
            return Err(Error::Parse {
                file: path.clone(),
                line: line_num,
                message: "FIPS county name cannot be blank".to_string(),
            });
        }
        out.push(FipsRecord {
            fips,
            year_start: yr_start,
            year_end: yr_end,
            name,
        });
    }

    Ok(out)
}

/// Parse FIPS data and keep only records that apply to `episode_year`.
pub fn read_fips_for_year<R: BufRead>(reader: R, episode_year: i32) -> Result<Vec<FipsRecord>> {
    let mut all = read_fips(reader)?;
    all.retain(|r| r.applies_in(episode_year));
    Ok(all)
}

fn parse_optional_year(
    token: &str,
    name: &str,
    line_num: usize,
    path: &std::path::Path,
) -> Result<Option<i32>> {
    let v: i32 = token.parse().map_err(|_| Error::Parse {
        file: path.to_path_buf(),
        line: line_num,
        message: format!("invalid {}: {}", name, token),
    })?;
    Ok(if v <= 0 { None } else { Some(v) })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_fips_packet() {
        let input = "\
/FIPS/
17031 0    0    Cook
17000 2000 0    Illinois (statewide)
1234  0    1990 Old County
06037 2010 2030 LA County
/END/
";
        let records = read_fips(input.as_bytes()).unwrap();
        assert_eq!(records.len(), 4);
        assert_eq!(records[2].fips, "01234"); // 4-digit zero-padded
        assert_eq!(records[3].year_start, Some(2010));
        assert_eq!(records[2].year_end, Some(1990));
        assert_eq!(records[0].state_prefix(), "17");
    }

    #[test]
    fn filters_by_year() {
        let input = "\
/FIPS/
17031 0    0    Cook
1234  0    1990 Old County
06037 2010 2030 LA County
/END/
";
        let in_2020 = read_fips_for_year(input.as_bytes(), 2020).unwrap();
        let kept: Vec<_> = in_2020.iter().map(|r| r.fips.as_str()).collect();
        assert_eq!(kept, vec!["17031", "06037"]);
    }

    #[test]
    fn rejects_short_fips() {
        let input = "\
/FIPS/
123 0 0 Foo
/END/
";
        let err = read_fips(input.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("4 or 5 digits")),
            other => panic!("unexpected error: {other:?}"),
        }
    }
}

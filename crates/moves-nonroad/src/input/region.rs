//! Region-packet parser (`rdnrreg.f`).
//!
//! Task 97. Parses the `/REGION/` packet from the options file. The
//! first record carries the region level (`USTOTAL`, `NATION`,
//! `STATE`, `COUNTY`, or `SUBCOUNTY`); subsequent records list the
//! requested FIPS codes (or subcounty codes when level is
//! `SUBCOUNTY`). For levels `USTOTAL` and `NATION` no further
//! records are required.
//!
//! # Format
//!
//! ```text
//! /REGION/
//! Level              : COUNTY
//! Region             : 17031
//! Region             : 06037
//! /END/
//! ```
//!
//! # Fortran source
//!
//! Ports `rdnrreg.f` (278 lines).

use crate::{Error, Result};
use std::io::BufRead;
use std::path::PathBuf;

/// Region scope level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionLevel {
    /// US-total inventory.
    UsTotal,
    /// National inventory (per-state).
    Nation,
    /// State-level inventory.
    State,
    /// County-level inventory.
    County,
    /// Subcounty-level inventory.
    Subcounty,
}

/// Parsed `/REGION/` packet.
#[derive(Debug, Clone)]
pub struct RegionConfig {
    /// Region level.
    pub level: RegionLevel,
    /// Region codes (FIPS or subcounty IDs). Empty for `UsTotal`/`Nation`.
    pub regions: Vec<String>,
}

/// Parse a `/REGION/` packet.
pub fn read_region<R: BufRead>(reader: R) -> Result<RegionConfig> {
    let path = PathBuf::from(".OPT");
    let mut in_packet = false;
    let mut level: Option<(RegionLevel, usize)> = None;
    let mut regions = Vec::new();
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
        if upper.starts_with("/REGION/") {
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

        let value = extract_value(&line);
        if level.is_none() {
            let lvl = match value.to_ascii_uppercase().as_str() {
                "US TOTAL" | "USTOTAL" | "US_TOTAL" => RegionLevel::UsTotal,
                "NATION" | "NATIONAL" => RegionLevel::Nation,
                "STATE" => RegionLevel::State,
                "COUNTY" => RegionLevel::County,
                "SUBCOUNTY" | "SUB-COUNTY" | "SUB_COUNTY" => RegionLevel::Subcounty,
                other => {
                    return Err(Error::Parse {
                        file: path,
                        line: line_num,
                        message: format!("invalid region level: {}", other),
                    });
                }
            };
            level = Some((lvl, line_num));
            continue;
        }
        if !value.is_empty() {
            regions.push(value);
        }
    }

    let (level_value, level_line) = level.ok_or_else(|| Error::Parse {
        file: path.clone(),
        line: line_num,
        message: "missing /REGION/ packet or level record".to_string(),
    })?;

    if matches!(level_value, RegionLevel::UsTotal | RegionLevel::Nation) {
        // Region list is ignored for these scopes; drop quietly.
        return Ok(RegionConfig {
            level: level_value,
            regions: Vec::new(),
        });
    }
    if regions.is_empty() {
        return Err(Error::Parse {
            file: path,
            line: level_line,
            message: format!(
                "region level {:?} requires at least one region",
                level_value
            ),
        });
    }

    Ok(RegionConfig {
        level: level_value,
        regions,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_county_region() {
        let input = "\
/REGION/
Level              : COUNTY
Region             : 17031
Region             : 06037
/END/
";
        let cfg = read_region(input.as_bytes()).unwrap();
        assert_eq!(cfg.level, RegionLevel::County);
        assert_eq!(cfg.regions, vec!["17031", "06037"]);
    }

    #[test]
    fn parses_us_total_no_regions() {
        let input = "\
/REGION/
Level              : US TOTAL
/END/
";
        let cfg = read_region(input.as_bytes()).unwrap();
        assert_eq!(cfg.level, RegionLevel::UsTotal);
        assert!(cfg.regions.is_empty());
    }

    #[test]
    fn rejects_state_without_regions() {
        let input = "\
/REGION/
Level              : STATE
/END/
";
        let err = read_region(input.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("requires at least")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn rejects_unknown_level() {
        let input = "\
/REGION/
Level              : PLANET
/END/
";
        let err = read_region(input.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("invalid region level")),
            other => panic!("unexpected error: {other:?}"),
        }
    }
}

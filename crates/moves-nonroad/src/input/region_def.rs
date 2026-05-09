//! Region-definition parser (`rdrgndf.f`).
//!
//! Task 97. Parses the `/REGIONS/` packet from a region-definition
//! file. Each line associates a region code with a state FIPS code.
//! A region accumulates the state FIPS codes of every record that
//! references it.
//!
//! # Format
//!
//! ```text
//! /REGIONS/
//! <region_code> <state_fips>
//! <region_code> <state_fips>
//! ...
//! /END/
//! ```
//!
//! # Fortran source
//!
//! Ports `rdrgndf.f` (200 lines).

use crate::{Error, Result};
use std::collections::HashMap;
use std::io::BufRead;
use std::path::PathBuf;

/// Region-to-states mapping.
#[derive(Debug, Default, Clone)]
pub struct RegionDefinitions {
    /// Insertion order of region codes.
    pub region_order: Vec<String>,
    /// Map from region code to its list of state FIPS codes.
    pub regions: HashMap<String, Vec<String>>,
}

impl RegionDefinitions {
    /// Number of distinct region codes.
    pub fn region_count(&self) -> usize {
        self.region_order.len()
    }

    /// State FIPS codes for `region`, if any.
    pub fn states_for(&self, region: &str) -> Option<&[String]> {
        self.regions.get(region).map(|v| v.as_slice())
    }
}

/// Parse a region-definition file.
pub fn read_rgndf<R: BufRead>(reader: R) -> Result<RegionDefinitions> {
    let mut out = RegionDefinitions::default();
    let path = PathBuf::from(".RGN");
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
        if upper.starts_with("/REGIONS/") {
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
        if parts.len() < 2 {
            return Err(Error::Parse {
                file: path.clone(),
                line: line_num,
                message: format!(
                    "expected 2 fields (region, state_fips), got {}",
                    parts.len()
                ),
            });
        }
        let region = parts[0].to_ascii_uppercase();
        let state = parts[1].to_string();
        if !out.regions.contains_key(&region) {
            out.region_order.push(region.clone());
        }
        out.regions.entry(region).or_default().push(state);
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_regions_packet() {
        let input = "\
/REGIONS/
EAST 17000
EAST 18000
WEST 06000
WEST 32000
WEST 41000
/END/
";
        let parsed = read_rgndf(input.as_bytes()).unwrap();
        assert_eq!(parsed.region_count(), 2);
        assert_eq!(parsed.region_order, vec!["EAST", "WEST"]);
        assert_eq!(parsed.states_for("EAST").unwrap().len(), 2);
        assert_eq!(parsed.states_for("WEST").unwrap().len(), 3);
    }

    #[test]
    fn rejects_short_record() {
        let input = "\
/REGIONS/
EAST
/END/
";
        let err = read_rgndf(input.as_bytes()).unwrap_err();
        assert!(matches!(err, Error::Parse { .. }));
    }
}

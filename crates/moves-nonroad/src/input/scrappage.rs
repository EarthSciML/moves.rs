//! Scrappage-curve parser (`rdscrp.f`).
//!
//! Task 97. Parses the `/SCRAPPAGE/` packet defining a discretized
//! scrappage curve: pairs of `(useful_life_bin, percent_scrapped)`.
//! Bins must be monotonically non-decreasing; bins beyond the
//! supplied curve default to 100% scrapped.
//!
//! # Format
//!
//! ```text
//! /SCRAPPAGE/
//! <bin> <percent>
//! ...
//! /END/
//! ```
//!
//! # Fortran source
//!
//! Ports `rdscrp.f` (177 lines).

use crate::{Error, Result};
use std::io::BufRead;
use std::path::PathBuf;

/// One discretized scrappage point.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScrappagePoint {
    /// Useful-life bin (e.g., 25.0 = 25% of useful life).
    pub bin: f32,
    /// Percent of equipment scrapped at this bin.
    pub percent: f32,
}

/// Parse a scrappage-curve file.
pub fn read_scrp<R: BufRead>(reader: R) -> Result<Vec<ScrappagePoint>> {
    let mut points = Vec::new();
    let path = PathBuf::from(".SCR");
    let mut in_packet = false;

    for (idx, line_result) in reader.lines().enumerate() {
        let line_num = idx + 1;
        let line = line_result.map_err(|e| Error::Io {
            path: path.clone(),
            source: e,
        })?;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let upper = trimmed.to_ascii_uppercase();
        if upper.starts_with("/SCRAPPAGE/") {
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
                message: format!("expected 2 fields (bin, percent), got {}", parts.len()),
            });
        }
        let bin: f32 = parts[0].parse().map_err(|_| Error::Parse {
            file: path.clone(),
            line: line_num,
            message: format!("invalid bin: {}", parts[0]),
        })?;
        let percent: f32 = parts[1].parse().map_err(|_| Error::Parse {
            file: path.clone(),
            line: line_num,
            message: format!("invalid percent: {}", parts[1]),
        })?;

        if let Some(prev) = points.last() {
            let prev: &ScrappagePoint = prev;
            if bin < prev.bin {
                return Err(Error::Parse {
                    file: path.clone(),
                    line: line_num,
                    message: "scrappage bins must be monotonically non-decreasing".to_string(),
                });
            }
        }
        points.push(ScrappagePoint { bin, percent });
    }

    Ok(points)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_scrappage_packet() {
        let input = "\
/SCRAPPAGE/
  25.0   10.0
  50.0   40.0
  75.0   75.0
 100.0  100.0
/END/
";
        let pts = read_scrp(input.as_bytes()).unwrap();
        assert_eq!(pts.len(), 4);
        assert!((pts[1].bin - 50.0).abs() < 1e-6);
        assert!((pts[3].percent - 100.0).abs() < 1e-6);
    }

    #[test]
    fn rejects_decreasing_bin() {
        let input = "\
/SCRAPPAGE/
  50.0   40.0
  25.0   10.0
/END/
";
        let err = read_scrp(input.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("monotonically")),
            other => panic!("unexpected error: {other:?}"),
        }
    }
}

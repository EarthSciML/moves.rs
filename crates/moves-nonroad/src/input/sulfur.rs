//! PM-base-sulfur parser (`rdsulf.f`).
//!
//! Task 97. Parses the optional `/PM BASE SULFUR/` packet from the
//! options file. Each record specifies a per-tech-type alternate base
//! sulfur fraction and a sulfate conversion ratio. Blank values
//! signal "use default" (mapped here to [`f32::NAN`]).
//!
//! # Format
//!
//! ```text
//! /PM BASE SULFUR/
//! <tech_type> <base_sulfur> <conversion>
//! ...
//! /END/
//! ```
//!
//! `base_sulfur` and `conversion` must lie in `[0.0, 1.0]` when present.
//!
//! # Fortran source
//!
//! Ports `rdsulf.f` (220 lines).

use crate::{Error, Result};
use std::io::BufRead;
use std::path::PathBuf;

/// Sentinel for "not specified" — matches `RMISS` in the Fortran source.
pub const SULFUR_MISSING: f32 = f32::NAN;

/// One `/PM BASE SULFUR/` record.
#[derive(Debug, Clone, PartialEq)]
pub struct SulfurRecord {
    /// Technology type (10-character key, upper-cased).
    pub tech_type: String,
    /// Alternate base sulfur fraction (`0..=1`, or NaN if blank).
    pub base_sulfur: f32,
    /// Sulfate conversion ratio (`0..=1`, or NaN if blank).
    pub conversion: f32,
}

/// Parse a `/PM BASE SULFUR/` packet. The packet is optional;
/// returning an empty vec means "no override".
pub fn read_sulf<R: BufRead>(reader: R) -> Result<Vec<SulfurRecord>> {
    let mut records = Vec::new();
    let path = PathBuf::from(".OPT");
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
        if upper.starts_with("/PM BASE SULFUR/") {
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
        if parts.is_empty() {
            continue;
        }
        let tech_type = parts[0].to_ascii_uppercase();
        let base_sulfur = parse_optional(parts.get(1), "base_sulfur", line_num, &path)?;
        let conversion = parse_optional(parts.get(2), "conversion", line_num, &path)?;
        records.push(SulfurRecord {
            tech_type,
            base_sulfur,
            conversion,
        });
    }

    Ok(records)
}

fn parse_optional(
    raw: Option<&&str>,
    name: &str,
    line_num: usize,
    path: &std::path::Path,
) -> Result<f32> {
    let Some(token) = raw.copied() else {
        return Ok(SULFUR_MISSING);
    };
    if token.is_empty() {
        return Ok(SULFUR_MISSING);
    }
    let v: f32 = token.parse().map_err(|_| Error::Parse {
        file: path.to_path_buf(),
        line: line_num,
        message: format!("invalid {}: {}", name, token),
    })?;
    if !(0.0..=1.0).contains(&v) {
        return Err(Error::Parse {
            file: path.to_path_buf(),
            line: line_num,
            message: format!("{} must be in [0.0, 1.0], got {}", name, v),
        });
    }
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_packet() {
        let input = "\
/PM BASE SULFUR/
BASE 0.05 0.02
ADV  0.03 0.01
/END/
";
        let records = read_sulf(input.as_bytes()).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].tech_type, "BASE");
        assert!((records[0].base_sulfur - 0.05).abs() < 1e-6);
        assert!((records[1].conversion - 0.01).abs() < 1e-6);
    }

    #[test]
    fn allows_missing_packet() {
        // No packet at all — empty result.
        let records = read_sulf(b"" as &[u8]).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn rejects_out_of_range() {
        let input = "\
/PM BASE SULFUR/
BASE 1.5 0.5
/END/
";
        let err = read_sulf(input.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("[0.0, 1.0]")),
            other => panic!("unexpected error: {other:?}"),
        }
    }
}

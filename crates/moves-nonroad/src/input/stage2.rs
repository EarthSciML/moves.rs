//! Stage-II VOC reduction parser (`rdstg2.f`).
//!
//! Task 97. Parses the optional `/STAGE II/` packet from the options
//! file. The packet contains a single VOC reduction percentage
//! (`0..=100`) and the model converts it to a multiplicative
//! retention factor `1 - pct/100`. If the packet is absent, the
//! retention factor defaults to `1.0` (no reduction).
//!
//! # Format
//!
//! ```text
//! /STAGE II/                           <pct>
//! ```
//!
//! # Fortran source
//!
//! Ports `rdstg2.f` (136 lines).

use crate::{Error, Result};
use std::io::BufRead;
use std::path::PathBuf;

/// Stage-II VOC reduction outcome.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Stage2Factor {
    /// Reduction percentage as written in the file (`0..=100`).
    pub reduction_pct: f32,
    /// Retention multiplier applied to VOC emissions (`1 - pct/100`).
    pub retention_factor: f32,
}

impl Stage2Factor {
    /// Default factor when the packet is absent (no reduction).
    pub const PASSTHROUGH: Self = Self {
        reduction_pct: 0.0,
        retention_factor: 1.0,
    };
}

/// Parse a `/STAGE II/` packet. Returns [`Stage2Factor::PASSTHROUGH`]
/// when the packet is missing.
pub fn read_stg2<R: BufRead>(reader: R) -> Result<Stage2Factor> {
    let path = PathBuf::from(".OPT");
    let mut found = false;
    let mut value: Option<f32> = None;
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
        if upper.starts_with("/STAGE II/") {
            found = true;
            // The Fortran source reads format `(20x,F7.0)` from the same line.
            // Take the first numeric token after the keyword.
            let after = trimmed
                .splitn(2, |c: char| c.is_ascii_whitespace())
                .nth(1)
                .map(str::trim)
                .unwrap_or("");
            for tok in after.split_whitespace() {
                if let Ok(v) = tok.parse::<f32>() {
                    value = Some(v);
                    break;
                }
            }
            continue;
        }
        if found && value.is_none() {
            // Value may appear on the next non-blank line.
            for tok in trimmed.split_whitespace() {
                if let Ok(v) = tok.parse::<f32>() {
                    value = Some(v);
                    break;
                }
            }
        }
    }

    if !found {
        return Ok(Stage2Factor::PASSTHROUGH);
    }
    let pct = value.ok_or_else(|| Error::Parse {
        file: path.clone(),
        line: line_num,
        message: "missing Stage II value".to_string(),
    })?;
    if !(0.0..=100.0).contains(&pct) {
        return Err(Error::Parse {
            file: path,
            line: line_num,
            message: format!("Stage II value must be in [0, 100], got {}", pct),
        });
    }
    Ok(Stage2Factor {
        reduction_pct: pct,
        retention_factor: 1.0 - pct / 100.0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_value_inline() {
        let input = "/STAGE II/                           75.0\n";
        let f = read_stg2(input.as_bytes()).unwrap();
        assert!((f.reduction_pct - 75.0).abs() < 1e-6);
        assert!((f.retention_factor - 0.25).abs() < 1e-6);
    }

    #[test]
    fn parses_value_next_line() {
        let input = "/STAGE II/\n50.0\n";
        let f = read_stg2(input.as_bytes()).unwrap();
        assert!((f.retention_factor - 0.5).abs() < 1e-6);
    }

    #[test]
    fn missing_packet_is_passthrough() {
        let f = read_stg2(b"" as &[u8]).unwrap();
        assert_eq!(f, Stage2Factor::PASSTHROUGH);
    }

    #[test]
    fn rejects_out_of_range() {
        let input = "/STAGE II/ 110.0\n";
        let err = read_stg2(input.as_bytes()).unwrap_err();
        assert!(matches!(err, Error::Parse { .. }));
    }
}

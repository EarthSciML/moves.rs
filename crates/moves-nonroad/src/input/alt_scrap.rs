//! Alternate-scrappage-curves parser (`rdalt.f`).
//!
//! Task 97. Parses the optional `/ALTERNATE SCRAPPAGE/` packet. Unlike
//! [`scrappage`](super::scrappage), this packet carries multiple
//! named curves: a header line lists curve names, and each subsequent
//! row gives an age bin followed by one percent value per named
//! curve. Bins must be monotonically non-decreasing; bins beyond the
//! curve default to 100% scrapped.
//!
//! # Format
//!
//! ```text
//! /ALTERNATE SCRAPPAGE/
//! <name1> <name2> ... <nameN>
//! <bin> <pct1> <pct2> ... <pctN>
//! ...
//! /END/
//! ```
//!
//! # Fortran source
//!
//! Ports `rdalt.f` (202 lines).

use crate::{Error, Result};
use std::io::BufRead;
use std::path::PathBuf;

/// One row of alternate-scrappage data.
#[derive(Debug, Clone, PartialEq)]
pub struct AlternateScrappageRow {
    /// Useful-life bin.
    pub bin: f32,
    /// Percent values, one per named curve.
    pub percents: Vec<f32>,
}

/// Parsed alternate-scrappage packet.
#[derive(Debug, Default, Clone)]
pub struct AlternateScrappage {
    /// Names of each curve column.
    pub names: Vec<String>,
    /// Rows of (bin, percents).
    pub rows: Vec<AlternateScrappageRow>,
}

/// Parse an alternate-scrappage file. Returns an empty result when
/// the packet is absent (matching the Fortran `ISKIP` behavior).
pub fn read_alt<R: BufRead>(reader: R) -> Result<AlternateScrappage> {
    let mut out = AlternateScrappage::default();
    let path = PathBuf::from(".ALT");
    let mut state = AltState::None;
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
        if upper.starts_with("/ALTERNATE SCRAPPAGE") {
            state = AltState::Header;
            continue;
        }
        if upper.starts_with("/END/") {
            state = AltState::None;
            continue;
        }

        match state {
            AltState::None => continue,
            AltState::Header => {
                out.names = trimmed
                    .split_whitespace()
                    .map(|s| s.to_ascii_uppercase())
                    .collect();
                if out.names.is_empty() {
                    return Err(Error::Parse {
                        file: path.clone(),
                        line: line_num,
                        message: "/ALTERNATE SCRAPPAGE/ header has no curve names".to_string(),
                    });
                }
                state = AltState::Rows;
            }
            AltState::Rows => {
                let parts: Vec<&str> = trimmed.split_whitespace().collect();
                if parts.len() < 1 + out.names.len() {
                    return Err(Error::Parse {
                        file: path.clone(),
                        line: line_num,
                        message: format!(
                            "expected bin + {} percent values, got {}",
                            out.names.len(),
                            parts.len()
                        ),
                    });
                }
                let bin: f32 = parts[0].parse().map_err(|_| Error::Parse {
                    file: path.clone(),
                    line: line_num,
                    message: format!("invalid bin: {}", parts[0]),
                })?;
                if let Some(prev) = out.rows.last() {
                    if bin < prev.bin {
                        return Err(Error::Parse {
                            file: path.clone(),
                            line: line_num,
                            message:
                                "alternate-scrappage bins must be monotonically non-decreasing"
                                    .to_string(),
                        });
                    }
                }
                let mut percents = Vec::with_capacity(out.names.len());
                for (i, raw) in parts[1..=out.names.len()].iter().enumerate() {
                    let v: f32 = raw.parse().map_err(|_| Error::Parse {
                        file: path.clone(),
                        line: line_num,
                        message: format!("invalid percent for column {}: {}", i, raw),
                    })?;
                    percents.push(v);
                }
                out.rows.push(AlternateScrappageRow { bin, percents });
            }
        }
    }

    Ok(out)
}

#[derive(Debug, Clone, Copy)]
enum AltState {
    None,
    Header,
    Rows,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_alt_packet() {
        let input = "\
/ALTERNATE SCRAPPAGE/
EARLY  LATE
  25.0  10.0  5.0
  50.0  40.0  30.0
  75.0  90.0  85.0
/END/
";
        let parsed = read_alt(input.as_bytes()).unwrap();
        assert_eq!(parsed.names, vec!["EARLY", "LATE"]);
        assert_eq!(parsed.rows.len(), 3);
        assert!((parsed.rows[1].bin - 50.0).abs() < 1e-6);
        assert_eq!(parsed.rows[2].percents, vec![90.0, 85.0]);
    }

    #[test]
    fn allows_missing_packet() {
        let parsed = read_alt(b"" as &[u8]).unwrap();
        assert!(parsed.names.is_empty());
        assert!(parsed.rows.is_empty());
    }

    #[test]
    fn rejects_decreasing_bin() {
        let input = "\
/ALTERNATE SCRAPPAGE/
EARLY
  50.0  40.0
  25.0  10.0
/END/
";
        let err = read_alt(input.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("monotonically")),
            other => panic!("unexpected error: {other:?}"),
        }
    }
}

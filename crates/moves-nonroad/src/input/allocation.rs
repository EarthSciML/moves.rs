//! Allocation cross-reference parser (`rdalo.f`).
//!
//! Task 94. Parses the `/ALLOC XREF/` packet of NONROAD `.ALO`
//! files. The packet maps an SCC to a regression of up to
//! [`MXCOEF`] spatial-allocation indicator codes (`POP`, `HHS`,
//! etc., resolved later via [`super::indicator`]) plus the
//! coefficients applied to each.
//!
//! # Multi-record format
//!
//! Each entry occupies **two consecutive non-blank lines** that
//! share the same 10-character SCC at columns 1–10 — the
//! "multi-record" pattern several allocation files reuse. Both
//! lines have identical column layouts; the first carries
//! coefficients in the three 10-character fields starting at
//! column 11, and the second carries the corresponding indicator
//! codes in the same byte positions (only the first 3 characters
//! of each field are kept).
//!
//! ```text
//! /ALLOC XREF/
//! 2270001000        0.500     0.500     0.000
//! 2270001000        POP       HHS
//! 2270002000        1.000
//! 2270002000        POP
//! /END/
//! ```
//!
//! Coefficients must sum to `1.0` within `0.001`, matching the
//! Fortran tolerance check (`rdalo.f` line 120). A blank
//! coefficient slot terminates the row early; the matching
//! indicator slot is skipped on the continuation line as well.
//!
//! # Fortran source
//!
//! Ports `rdalo.f` (278 lines). The Fortran version skips entries
//! whose SCC is not active for the current run via `chkasc`; that
//! filter requires Task 99's option-file state, so this parser
//! emits every entry and leaves filtering to the caller.

use crate::{Error, Result};
use std::io::BufRead;
use std::path::PathBuf;

/// Maximum number of regression coefficients per SCC. Matches the
/// Fortran parameter `MXCOEF` from `nonrdalo.inc`.
pub const MXCOEF: usize = 3;

/// One `/ALLOC XREF/` entry: an SCC mapped to coefficients and
/// indicator codes. Unused slots carry a `0.0` coefficient and an
/// empty indicator string.
#[derive(Debug, Clone, PartialEq)]
pub struct AllocationRecord {
    /// 10-character SCC code (left-justified, blank-stripped).
    pub scc: String,
    /// Regression coefficients; unused slots are `0.0`.
    pub coefficients: [f32; MXCOEF],
    /// 3-character indicator codes (e.g., `"POP"`, `"HHS"`); unused
    /// slots are empty strings.
    pub indicators: [String; MXCOEF],
}

/// Parsed contents of an `.ALO` file.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AllocationFile {
    /// SCC entries in input order.
    pub records: Vec<AllocationRecord>,
    /// Distinct indicator codes seen across all records, in
    /// first-encountered order. Mirrors the Fortran `alocod` array
    /// built up by `rdalo.f`.
    pub unique_codes: Vec<String>,
}

/// Parse a `.ALO` reader into an [`AllocationFile`].
pub fn read_alo<R: BufRead>(reader: R) -> Result<AllocationFile> {
    let path = PathBuf::from(".ALO");
    let mut lines = reader.lines();
    let mut line_num = 0usize;

    if !find_packet(&mut lines, &mut line_num, &path)? {
        return Err(Error::Parse {
            file: path,
            line: line_num,
            message: "no /ALLOC XREF/ packet found".to_string(),
        });
    }

    let mut out = AllocationFile::default();

    loop {
        let coef_line = match next_significant(&mut lines, &mut line_num, &path)? {
            Some(s) => s,
            None => {
                return Err(Error::Parse {
                    file: path,
                    line: line_num,
                    message: "unexpected EOF in /ALLOC XREF/ packet".to_string(),
                });
            }
        };

        if coef_line.trim().to_ascii_uppercase().starts_with("/END/") {
            break;
        }

        let coef_line_num = line_num;
        let scc = column_string(coef_line.as_bytes(), 0, 10)
            .trim()
            .to_string();
        if scc.is_empty() {
            return Err(Error::Parse {
                file: path,
                line: coef_line_num,
                message: "missing SCC on /ALLOC XREF/ coefficients line".to_string(),
            });
        }

        let mut coefficients = [0.0f32; MXCOEF];
        let mut filled = [false; MXCOEF];
        let mut sum = 0.0f32;
        for (i, slot) in coefficients.iter_mut().enumerate() {
            let start = 10 + i * 10;
            let end = start + 10;
            let raw = column_string(coef_line.as_bytes(), start, end);
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                break;
            }
            let value: f32 = trimmed.parse().map_err(|_| Error::Parse {
                file: path.clone(),
                line: coef_line_num,
                message: format!("invalid coefficient {}: {:?}", i + 1, raw),
            })?;
            *slot = value;
            filled[i] = true;
            sum += value;
        }

        if (sum - 1.0).abs() > 0.001 {
            return Err(Error::Parse {
                file: path,
                line: coef_line_num,
                message: format!("coefficients for SCC {} do not sum to 1 (got {})", scc, sum),
            });
        }

        let ind_line =
            next_significant(&mut lines, &mut line_num, &path)?.ok_or_else(|| Error::Parse {
                file: path.clone(),
                line: line_num,
                message: format!(
                    "expected indicator-code line for SCC {} but reached EOF",
                    scc
                ),
            })?;

        let ind_line_num = line_num;
        let ind_scc = column_string(ind_line.as_bytes(), 0, 10).trim().to_string();
        if ind_scc != scc {
            return Err(Error::Parse {
                file: path,
                line: ind_line_num,
                message: format!(
                    "SCC mismatch in /ALLOC XREF/ continuation: expected {}, got {}",
                    scc, ind_scc
                ),
            });
        }

        let mut indicators: [String; MXCOEF] = Default::default();
        for (i, slot) in indicators.iter_mut().enumerate() {
            if !filled[i] {
                continue;
            }
            let start = 10 + i * 10;
            let end = start + 10;
            let raw = column_string(ind_line.as_bytes(), start, end);
            // The Fortran left-justifies and keeps the first three
            // characters; reproduce that here.
            let code: String = raw.trim_start().chars().take(3).collect();
            if code.is_empty() {
                return Err(Error::Parse {
                    file: path.clone(),
                    line: ind_line_num,
                    message: format!("missing indicator code {} for SCC {}", i + 1, scc),
                });
            }
            if !out.unique_codes.iter().any(|c| c == &code) {
                out.unique_codes.push(code.clone());
            }
            *slot = code;
        }

        out.records.push(AllocationRecord {
            scc,
            coefficients,
            indicators,
        });
    }

    Ok(out)
}

fn find_packet<R: BufRead>(
    lines: &mut std::io::Lines<R>,
    line_num: &mut usize,
    path: &std::path::Path,
) -> Result<bool> {
    for line_result in lines.by_ref() {
        *line_num += 1;
        let line = line_result.map_err(|e| Error::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if trimmed.to_ascii_uppercase().starts_with("/ALLOC XREF/") {
            return Ok(true);
        }
    }
    Ok(false)
}

fn next_significant<R: BufRead>(
    lines: &mut std::io::Lines<R>,
    line_num: &mut usize,
    path: &std::path::Path,
) -> Result<Option<String>> {
    for line_result in lines.by_ref() {
        *line_num += 1;
        let line = line_result.map_err(|e| Error::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        return Ok(Some(line));
    }
    Ok(None)
}

fn column_string(bytes: &[u8], start: usize, end: usize) -> String {
    if start >= bytes.len() {
        return String::new();
    }
    let end = end.min(bytes.len());
    String::from_utf8_lossy(&bytes[start..end]).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_line(scc: &str, fields: &[&str]) -> String {
        // Build a 100-byte ASCII line: SCC at cols 1–10, three
        // 10-character fields starting at col 11.
        let mut buf = vec![b' '; 100];
        let put = |buf: &mut Vec<u8>, col0: usize, value: &str, width: usize| {
            for (i, &b) in value.as_bytes().iter().take(width).enumerate() {
                buf[col0 + i] = b;
            }
        };
        put(&mut buf, 0, scc, 10);
        for (i, field) in fields.iter().enumerate() {
            put(&mut buf, 10 + i * 10, field, 10);
        }
        String::from_utf8(buf).unwrap()
    }

    fn pack(rows: &[String]) -> String {
        let mut s = String::from("/ALLOC XREF/\n");
        for r in rows {
            s.push_str(r);
            s.push('\n');
        }
        s.push_str("/END/\n");
        s
    }

    #[test]
    fn reads_one_record_with_two_indicators() {
        let coef = make_line("2270001000", &["    0.500 ", "    0.500 ", "          "]);
        let ind = make_line("2270001000", &["POP       ", "HHS       ", "          "]);
        let input = pack(&[coef, ind]);

        let alo = read_alo(input.as_bytes()).unwrap();
        assert_eq!(alo.records.len(), 1);
        assert_eq!(alo.records[0].scc, "2270001000");
        assert!((alo.records[0].coefficients[0] - 0.5).abs() < 1e-6);
        assert!((alo.records[0].coefficients[1] - 0.5).abs() < 1e-6);
        assert_eq!(alo.records[0].coefficients[2], 0.0);
        assert_eq!(alo.records[0].indicators[0], "POP");
        assert_eq!(alo.records[0].indicators[1], "HHS");
        assert_eq!(alo.records[0].indicators[2], "");
        assert_eq!(alo.unique_codes, vec!["POP".to_string(), "HHS".to_string()]);
    }

    #[test]
    fn reads_multiple_records_and_dedups_codes() {
        let r1c = make_line("2270001000", &["    0.500 ", "    0.500 ", "          "]);
        let r1i = make_line("2270001000", &["POP       ", "HHS       ", "          "]);
        let r2c = make_line("2270002000", &["    1.000 ", "          ", "          "]);
        let r2i = make_line("2270002000", &["POP       ", "          ", "          "]);
        let r3c = make_line("2270003000", &["    0.300 ", "    0.700 ", "          "]);
        let r3i = make_line("2270003000", &["NMM       ", "POP       ", "          "]);
        let input = pack(&[r1c, r1i, r2c, r2i, r3c, r3i]);

        let alo = read_alo(input.as_bytes()).unwrap();
        assert_eq!(alo.records.len(), 3);
        assert_eq!(
            alo.unique_codes,
            vec!["POP".to_string(), "HHS".to_string(), "NMM".to_string()]
        );
    }

    #[test]
    fn rejects_coefficient_sum_off_by_more_than_tolerance() {
        let coef = make_line("2270001000", &["    0.400 ", "    0.500 ", "          "]);
        let ind = make_line("2270001000", &["POP       ", "HHS       ", "          "]);
        let input = pack(&[coef, ind]);
        let err = read_alo(input.as_bytes()).unwrap_err();
        assert!(format!("{}", err).contains("do not sum to 1"));
    }

    #[test]
    fn accepts_coefficient_sum_within_tolerance() {
        let coef = make_line("2270001000", &["    0.4995", "    0.5005", "          "]);
        let ind = make_line("2270001000", &["POP       ", "HHS       ", "          "]);
        let input = pack(&[coef, ind]);
        read_alo(input.as_bytes()).unwrap();
    }

    #[test]
    fn rejects_scc_mismatch_on_continuation_line() {
        let coef = make_line("2270001000", &["    1.000 ", "          ", "          "]);
        let ind = make_line("2270002000", &["POP       ", "          ", "          "]);
        let input = pack(&[coef, ind]);
        let err = read_alo(input.as_bytes()).unwrap_err();
        assert!(format!("{}", err).contains("SCC mismatch"));
    }

    #[test]
    fn rejects_missing_indicator_for_filled_coefficient() {
        let coef = make_line("2270001000", &["    1.000 ", "          ", "          "]);
        let ind = make_line("2270001000", &["          ", "          ", "          "]);
        let input = pack(&[coef, ind]);
        let err = read_alo(input.as_bytes()).unwrap_err();
        assert!(format!("{}", err).contains("indicator code"));
    }

    #[test]
    fn rejects_missing_packet() {
        let input = "no packet here\n";
        let err = read_alo(input.as_bytes()).unwrap_err();
        assert!(format!("{}", err).contains("/ALLOC XREF/"));
    }

    #[test]
    fn rejects_unterminated_packet() {
        let coef = make_line("2270001000", &["    1.000 ", "          ", "          "]);
        let input = format!("/ALLOC XREF/\n{}\n", coef);
        let err = read_alo(input.as_bytes()).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("EOF") || msg.contains("indicator-code line"));
    }

    #[test]
    fn skips_blank_lines_between_continuation_pairs() {
        let r1c = make_line("2270001000", &["    1.000 ", "          ", "          "]);
        let r1i = make_line("2270001000", &["POP       ", "          ", "          "]);
        let mut input = String::from("/ALLOC XREF/\n");
        input.push_str(&r1c);
        input.push_str("\n\n# comment between coef and indicator lines\n");
        input.push_str(&r1i);
        input.push_str("\n/END/\n");
        let alo = read_alo(input.as_bytes()).unwrap();
        assert_eq!(alo.records.len(), 1);
    }

    #[test]
    fn truncates_indicator_codes_to_three_chars() {
        let coef = make_line("2270001000", &["    1.000 ", "          ", "          "]);
        let ind = make_line("2270001000", &["POPULATION", "          ", "          "]);
        let input = pack(&[coef, ind]);
        let alo = read_alo(input.as_bytes()).unwrap();
        assert_eq!(alo.records[0].indicators[0], "POP");
    }
}

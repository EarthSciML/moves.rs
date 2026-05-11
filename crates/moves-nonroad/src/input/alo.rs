//! Spatial-allocation cross-reference (`.ALO`) parser (`rdalo.f`).
//!
//! Task 94. Parses the regression-coefficient file that maps SCC
//! equipment codes to spatial allocation surrogates. The format is
//! line-pair: every SCC contributes two consecutive records — one
//! line of regression coefficients, one line of the indicator
//! codes those coefficients apply to.
//!
//! # Format
//!
//! Records live inside a `/ALLOC XREF/` packet, terminated by an
//! `/END/` marker. Each SCC entry is two lines:
//!
//! ```text
//! /ALLOC XREF/
//! <SCC      ><coeff1   ><coeff2   ><coeff3   >
//! <SCC      ><indcd1   ><indcd2   ><indcd3   >
//! ...
//! /END/
//! ```
//!
//! - Cols 1–10: SCC code (must match between the two lines).
//! - Cols 11–20, 21–30, 31–40: up to `MXCOEF = 3` ten-character
//!   fields. On the coefficient line each is an `F10.0`; on the
//!   indicator line each is a left-justified character field whose
//!   leading 3 characters form the indicator code.
//!
//! Trailing blank fields signal "no more coefficients for this
//! SCC" — the Fortran source breaks the loop at the first blank
//! field (`rdalo.f` :112). The coefficients for an SCC must sum
//! to 1.0 within 0.001 (`rdalo.f` :120).
//!
//! # Filtering (deferred)
//!
//! `rdalo.f` calls `chkasc` to check whether an SCC is required by
//! the current run and, if not, skips both the coefficient and
//! indicator lines. That filtering depends on COMMON-block state
//! set up by `getind.f` / `iniasc.f` and is deferred to higher-
//! level callers (Tasks 99 and 105). This module returns every
//! well-formed allocation pair.

use crate::{Error, Result};
use std::io::BufRead;
use std::path::PathBuf;

/// Maximum number of coefficient/indicator pairs per SCC (matches
/// `MXCOEF` in `nonrdalo.inc`).
pub const MAX_COEF: usize = 3;

/// Tolerance for the coefficient-sum check (matches the literal
/// `0.001` in `rdalo.f` :120).
pub const COEF_SUM_TOLERANCE: f32 = 0.001;

/// One parsed allocation cross-reference record.
#[derive(Debug, Clone, PartialEq)]
pub struct AllocationRecord {
    /// SCC equipment code (10 chars).
    pub scc: String,
    /// Up to `MAX_COEF` regression coefficients, in column order.
    pub coefficients: Vec<f32>,
    /// Indicator codes (3 chars each, left-justified), one per
    /// coefficient. Always the same length as `coefficients`.
    pub indicator_codes: Vec<String>,
}

/// Parse an `.ALO` file into a vector of [`AllocationRecord`].
///
/// Skips lines before the `/ALLOC XREF/` packet and stops at
/// `/END/`. Returns an error if the packet marker or trailing
/// `/END/` is missing, the two lines of an SCC pair don't match,
/// any coefficient is malformed, or the coefficient sum drifts
/// further than [`COEF_SUM_TOLERANCE`] from 1.0.
pub fn read_alo<R: BufRead>(reader: R) -> Result<Vec<AllocationRecord>> {
    let path = PathBuf::from(".ALO");
    let mut records = Vec::new();

    let mut iter = reader.lines().enumerate().map(|(idx, res)| {
        res.map(|line| (idx + 1, line)).map_err(|e| Error::Io {
            path: path.clone(),
            source: e,
        })
    });

    // Skip until /ALLOC XREF/.
    let mut in_packet = false;
    let mut last_line_num: usize = 0;
    for next in iter.by_ref() {
        let (line_num, line) = next?;
        last_line_num = line_num;
        if is_keyword(&line, "/ALLOC XREF/") {
            in_packet = true;
            break;
        }
    }
    if !in_packet {
        return Err(Error::Parse {
            file: path,
            line: last_line_num,
            message: "missing /ALLOC XREF/ packet marker".to_string(),
        });
    }

    let mut found_end = false;
    while let Some(next) = iter.next() {
        let (coef_line_num, coef_line) = next?;
        last_line_num = coef_line_num;

        if is_keyword(&coef_line, "/END/") {
            found_end = true;
            break;
        }
        if coef_line.trim().is_empty() {
            continue;
        }

        let scc = column(&coef_line, 1, 10).trim().to_string();
        if scc.is_empty() {
            return Err(Error::Parse {
                file: path,
                line: coef_line_num,
                message: format!("blank SCC code on coefficient line {:?}", coef_line),
            });
        }

        let coefficients =
            parse_coefficient_fields(&coef_line, coef_line_num, &path)?;
        if coefficients.is_empty() {
            return Err(Error::Parse {
                file: path,
                line: coef_line_num,
                message: format!(
                    "no coefficients for SCC {scc}: line {:?}",
                    coef_line
                ),
            });
        }

        let coef_sum: f32 = coefficients.iter().sum();
        if (coef_sum - 1.0).abs() > COEF_SUM_TOLERANCE {
            return Err(Error::Parse {
                file: path,
                line: coef_line_num,
                message: format!(
                    "coefficients for SCC {scc} sum to {coef_sum}, expected 1.0 \
                     (tolerance {COEF_SUM_TOLERANCE})"
                ),
            });
        }

        // The matching indicator-code line must immediately follow.
        let (ind_line_num, ind_line) = match iter.next() {
            Some(next) => next?,
            None => {
                return Err(Error::Parse {
                    file: path,
                    line: coef_line_num,
                    message: format!(
                        "missing indicator-code line after coefficients for SCC {scc}"
                    ),
                });
            }
        };
        last_line_num = ind_line_num;

        let ind_scc = column(&ind_line, 1, 10).trim();
        if ind_scc != scc {
            return Err(Error::Parse {
                file: path,
                line: ind_line_num,
                message: format!(
                    "SCC mismatch: coefficient line {coef_line_num} has {scc:?}, \
                     indicator line {ind_line_num} has {ind_scc:?}"
                ),
            });
        }

        let indicator_codes = parse_indicator_fields(&ind_line, coefficients.len());

        records.push(AllocationRecord {
            scc,
            coefficients,
            indicator_codes,
        });
    }

    if !found_end {
        return Err(Error::Parse {
            file: path,
            line: last_line_num,
            message: "missing /END/ marker after /ALLOC XREF/ packet".to_string(),
        });
    }

    Ok(records)
}

/// Extract the unique 3-character indicator codes referenced by
/// `records`, preserving first-seen order. Mirrors the
/// `alocod` / `nalocd` book-keeping in `rdalo.f` :151–157.
pub fn unique_indicator_codes(records: &[AllocationRecord]) -> Vec<String> {
    let mut seen: Vec<String> = Vec::new();
    for record in records {
        for code in &record.indicator_codes {
            if !seen.iter().any(|existing| existing == code) {
                seen.push(code.clone());
            }
        }
    }
    seen
}

fn parse_coefficient_fields(
    line: &str,
    line_num: usize,
    path: &PathBuf,
) -> Result<Vec<f32>> {
    let mut coefficients = Vec::with_capacity(MAX_COEF);
    for slot in 0..MAX_COEF {
        let start = 11 + slot * 10;
        let end = start + 9;
        let field = column(line, start, end);
        if field.trim().is_empty() {
            break;
        }
        let value = field.trim().parse::<f32>().map_err(|_| Error::Parse {
            file: path.clone(),
            line: line_num,
            message: format!(
                "invalid coefficient value {:?} (slot {}): line {:?}",
                field,
                slot + 1,
                line
            ),
        })?;
        coefficients.push(value);
    }
    Ok(coefficients)
}

fn parse_indicator_fields(line: &str, n_coeffs: usize) -> Vec<String> {
    let mut codes = Vec::with_capacity(n_coeffs);
    for slot in 0..n_coeffs {
        let start = 11 + slot * 10;
        let end = start + 9;
        let field = column(line, start, end);
        let trimmed = field.trim_start();
        // The Fortran source stores `indtmp(i)(1:3)` — the first
        // three characters of the left-justified 10-char field.
        let mut code: String = trimmed.chars().take(3).collect();
        // Pad to 3 chars if shorter, matching Fortran's blank-padded
        // CHARACTER*3 storage.
        while code.len() < 3 {
            code.push(' ');
        }
        codes.push(code);
    }
    codes
}

fn is_keyword(line: &str, keyword: &str) -> bool {
    line.trim_start()
        .get(..keyword.len())
        .map(|s| s.eq_ignore_ascii_case(keyword))
        .unwrap_or(false)
}

fn column(line: &str, start_1based: usize, end_1based: usize) -> &str {
    let start = start_1based.saturating_sub(1);
    let end = end_1based.min(line.len());
    if start >= end {
        return "";
    }
    &line[start..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build one column-aligned 40-char line: SCC in cols 1–10,
    /// then up to 3 ten-char fields starting at col 11.
    fn line(scc: &str, fields: &[&str]) -> String {
        let mut buf = vec![b' '; 40];
        let put = |buf: &mut [u8], start_1based: usize, value: &str, width: usize| {
            let start = start_1based - 1;
            let bytes = value.as_bytes();
            let n = bytes.len().min(width);
            buf[start..start + n].copy_from_slice(&bytes[..n]);
        };
        put(&mut buf, 1, scc, 10);
        for (slot, field) in fields.iter().enumerate().take(MAX_COEF) {
            // right-justify numeric / left-justify will just place
            // the value at the start of the slot
            put(&mut buf, 11 + slot * 10, field, 10);
        }
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn reads_single_record() {
        let coef = line("2270002003", &["0.5       ", "0.3       ", "0.2       "]);
        let ind = line("2270002003", &["POP       ", "EMP       ", "FRT       "]);
        let input = format!("/ALLOC XREF/\n{coef}\n{ind}\n/END/\n");

        let records = read_alo(input.as_bytes()).unwrap();
        assert_eq!(records.len(), 1);
        let r = &records[0];
        assert_eq!(r.scc, "2270002003");
        assert_eq!(r.coefficients, vec![0.5, 0.3, 0.2]);
        assert_eq!(r.indicator_codes, vec!["POP", "EMP", "FRT"]);
    }

    #[test]
    fn handles_partial_coefficients() {
        // Only one coefficient = 1.0; remaining slots blank.
        let coef = line("2270002003", &["1.0       ", "          ", "          "]);
        let ind = line("2270002003", &["POP       ", "          ", "          "]);
        let input = format!("/ALLOC XREF/\n{coef}\n{ind}\n/END/\n");

        let records = read_alo(input.as_bytes()).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].coefficients, vec![1.0]);
        assert_eq!(records[0].indicator_codes, vec!["POP"]);
    }

    #[test]
    fn errors_when_packet_marker_missing() {
        let coef = line("2270002003", &["1.0       ", "", ""]);
        let ind = line("2270002003", &["POP       ", "", ""]);
        let input = format!("{coef}\n{ind}\n/END/\n");

        let err = read_alo(input.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => {
                assert!(message.contains("/ALLOC XREF/"));
            }
            other => panic!("expected Error::Parse, got {other:?}"),
        }
    }

    #[test]
    fn errors_when_end_marker_missing() {
        let coef = line("2270002003", &["1.0       ", "", ""]);
        let ind = line("2270002003", &["POP       ", "", ""]);
        let input = format!("/ALLOC XREF/\n{coef}\n{ind}\n");

        let err = read_alo(input.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => {
                assert!(message.contains("/END/"));
            }
            other => panic!("expected Error::Parse, got {other:?}"),
        }
    }

    #[test]
    fn errors_on_scc_mismatch_between_lines() {
        let coef = line("2270002003", &["1.0       ", "", ""]);
        let ind = line("9999999999", &["POP       ", "", ""]);
        let input = format!("/ALLOC XREF/\n{coef}\n{ind}\n/END/\n");

        let err = read_alo(input.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => {
                assert!(message.contains("SCC mismatch"));
            }
            other => panic!("expected Error::Parse, got {other:?}"),
        }
    }

    #[test]
    fn errors_when_coefficient_sum_far_from_one() {
        let coef = line("2270002003", &["0.5       ", "0.3       ", "          "]);
        let ind = line("2270002003", &["POP       ", "EMP       ", "          "]);
        let input = format!("/ALLOC XREF/\n{coef}\n{ind}\n/END/\n");

        let err = read_alo(input.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => {
                assert!(message.contains("sum to"), "got {message}");
            }
            other => panic!("expected Error::Parse, got {other:?}"),
        }
    }

    #[test]
    fn allows_coefficient_sum_within_tolerance() {
        // Within 0.001 of 1.0
        let coef = line("2270002003", &["0.5005    ", "0.5       ", "          "]);
        let ind = line("2270002003", &["POP       ", "EMP       ", "          "]);
        let input = format!("/ALLOC XREF/\n{coef}\n{ind}\n/END/\n");

        let records = read_alo(input.as_bytes()).unwrap();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn errors_when_indicator_line_missing() {
        let coef = line("2270002003", &["1.0       ", "", ""]);
        // Coefficient line followed immediately by /END/.
        let input = format!("/ALLOC XREF/\n{coef}\n/END/\n");

        let err = read_alo(input.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => {
                assert!(
                    message.contains("SCC mismatch") || message.contains("missing"),
                    "got {message}"
                );
            }
            other => panic!("expected Error::Parse, got {other:?}"),
        }
    }

    #[test]
    fn unique_indicator_codes_preserves_first_seen_order() {
        let records = vec![
            AllocationRecord {
                scc: "A".to_string(),
                coefficients: vec![0.5, 0.5],
                indicator_codes: vec!["POP".into(), "EMP".into()],
            },
            AllocationRecord {
                scc: "B".to_string(),
                coefficients: vec![1.0],
                indicator_codes: vec!["POP".into()],
            },
            AllocationRecord {
                scc: "C".to_string(),
                coefficients: vec![0.5, 0.5],
                indicator_codes: vec!["FRT".into(), "EMP".into()],
            },
        ];
        assert_eq!(
            unique_indicator_codes(&records),
            vec!["POP".to_string(), "EMP".into(), "FRT".into()]
        );
    }

    #[test]
    fn returns_empty_when_packet_has_no_records() {
        let input = "/ALLOC XREF/\n/END/\n";
        let records = read_alo(input.as_bytes()).unwrap();
        assert!(records.is_empty());
    }
}

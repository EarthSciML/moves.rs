//! Evaporative technology-fraction parser (`rdevtech.f`).
//!
//! Task 96. Parses an evap technology-fraction file's
//! `/EVAP TECH FRAC/` packet. Same column layout as the exhaust
//! [`super::tech`] parser, but with two differences:
//!
//! 1. Each tech-type code is an evap-group identifier of the
//!    form `E` + 8 digits (one per evap species), validated
//!    here per `rdevtech.f` :117-137.
//! 2. The header keyword is `/EVAP TECH FRAC/`.
//!
//! After reading, fractions are renormalised per group so they
//! sum to 1.0 (`rdevtech.f` :203-230).
//!
//! # Fortran source
//!
//! Ports `rdevtech.f` (334 lines).

use std::io::BufRead;
use std::path::{Path, PathBuf};

use crate::{Error, Result};

/// One (SCC, HP-range, year) group of evap tech fractions.
///
/// Each `tech_type` matches the `E + 8 digits` encoding documented
/// at `rdevtech.f` :117-134.
#[derive(Debug, Clone, PartialEq)]
pub struct EvapTechFractionGroup {
    /// SCC code.
    pub scc: String,
    /// HP-range minimum.
    pub hp_min: f32,
    /// HP-range maximum.
    pub hp_max: f32,
    /// Model year.
    pub year: i32,
    /// Tech-type → fraction, in header order.
    pub fractions: Vec<(String, f32)>,
}

/// Parse a `/EVAP TECH FRAC/` packet.
///
/// Returns an empty vector if the marker is missing
/// (`rdevtech.f`'s `ISKIP` return).
pub fn read_evtech<R: BufRead>(reader: R) -> Result<Vec<EvapTechFractionGroup>> {
    let path = PathBuf::from(".EVT");
    let mut groups: Vec<EvapTechFractionGroup> = Vec::new();
    let mut in_packet = false;
    let mut line_num = 0usize;
    let mut header: Option<HeaderContext> = None;

    for line_result in reader.lines() {
        line_num += 1;
        let line = line_result.map_err(|e| Error::Io {
            path: path.clone(),
            source: e,
        })?;

        if !in_packet {
            if is_keyword(&line, "/EVAP TECH FRAC/") {
                in_packet = true;
            }
            continue;
        }

        if is_keyword(&line, "/END/") {
            renormalise(&mut groups);
            return Ok(groups);
        }

        if line.trim().is_empty() {
            continue;
        }

        if !column(&line, 6, 15).trim().is_empty() {
            header = Some(parse_header(&line, line_num, &path)?);
        } else {
            let Some(ctx) = header.as_ref() else {
                return Err(Error::Parse {
                    file: path.clone(),
                    line: line_num,
                    message: "data record before any /EVAP TECH FRAC/ header".to_string(),
                });
            };
            groups.push(parse_data_line(&line, line_num, &path, ctx)?);
        }
    }

    if !in_packet {
        return Ok(Vec::new());
    }

    Err(Error::Parse {
        file: path,
        line: line_num,
        message: "missing /END/ marker after /EVAP TECH FRAC/ packet".to_string(),
    })
}

#[derive(Debug, Clone)]
struct HeaderContext {
    scc: String,
    hp_min: f32,
    hp_max: f32,
    tech_types: Vec<String>,
}

fn parse_header(line: &str, line_num: usize, path: &Path) -> Result<HeaderContext> {
    let scc = column(line, 6, 15).trim().to_string();
    let hp_min = parse_numeric(column(line, 21, 25), "hp_min", line_num, path)?;
    let hp_max = parse_numeric(column(line, 26, 30), "hp_max", line_num, path)?;

    let mut tech_types: Vec<String> = Vec::new();
    let mut start = 35usize;
    loop {
        let field = column(line, start, start + 9);
        if field.trim().is_empty() {
            break;
        }
        if tech_types.len() >= crate::common::consts::MXEVTECH {
            return Err(Error::Parse {
                file: path.to_path_buf(),
                line: line_num,
                message: format!(
                    "more than {} evap tech types on /EVAP TECH FRAC/ header",
                    crate::common::consts::MXEVTECH
                ),
            });
        }
        let code = field.trim().to_ascii_uppercase();
        validate_evap_code(&code, line_num, path)?;
        tech_types.push(code);
        start += 10;
    }

    if tech_types.is_empty() {
        return Err(Error::Parse {
            file: path.to_path_buf(),
            line: line_num,
            message: "/EVAP TECH FRAC/ header has no tech-type columns".to_string(),
        });
    }

    Ok(HeaderContext {
        scc,
        hp_min,
        hp_max,
        tech_types,
    })
}

/// `rdevtech.f` :136-137 — the code must start with `E` and be
/// exactly 9 characters after trimming. The eight following
/// characters are per-species tech indices (`En` per species).
fn validate_evap_code(code: &str, line_num: usize, path: &Path) -> Result<()> {
    let bytes = code.as_bytes();
    if bytes.len() != 9 || bytes[0] != b'E' {
        return Err(Error::Parse {
            file: path.to_path_buf(),
            line: line_num,
            message: format!("invalid evap tech code {code:?}; expected 'E' followed by 8 chars"),
        });
    }
    Ok(())
}

fn parse_data_line(
    line: &str,
    line_num: usize,
    path: &Path,
    ctx: &HeaderContext,
) -> Result<EvapTechFractionGroup> {
    let year = parse_i5(column(line, 1, 5), "year", line_num, path)?;
    let mut start = 35usize;
    let mut fractions = Vec::with_capacity(ctx.tech_types.len());
    for tech in &ctx.tech_types {
        let field = column(line, start, start + 9);
        let value = parse_numeric(field, "evap fraction", line_num, path)?;
        fractions.push((tech.clone(), value));
        start += 10;
    }
    Ok(EvapTechFractionGroup {
        scc: ctx.scc.clone(),
        hp_min: ctx.hp_min,
        hp_max: ctx.hp_max,
        year,
        fractions,
    })
}

fn renormalise(groups: &mut [EvapTechFractionGroup]) {
    for group in groups.iter_mut() {
        let sum: f32 = group.fractions.iter().map(|(_, f)| *f).sum();
        if sum > 0.0 {
            for (_, f) in group.fractions.iter_mut() {
                *f /= sum;
            }
        }
    }
}

fn is_keyword(line: &str, keyword: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed
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

fn parse_numeric(field: &str, name: &str, line_num: usize, path: &Path) -> Result<f32> {
    let trimmed = field.trim();
    if trimmed.is_empty() {
        return Err(Error::Parse {
            file: path.to_path_buf(),
            line: line_num,
            message: format!("empty {name} field"),
        });
    }
    trimmed.parse::<f32>().map_err(|_| Error::Parse {
        file: path.to_path_buf(),
        line: line_num,
        message: format!("invalid {name} value {trimmed:?}"),
    })
}

fn parse_i5(field: &str, name: &str, line_num: usize, path: &Path) -> Result<i32> {
    let trimmed = field.trim();
    if trimmed.is_empty() {
        return Err(Error::Parse {
            file: path.to_path_buf(),
            line: line_num,
            message: format!("empty {name} field"),
        });
    }
    trimmed.parse::<i32>().map_err(|_| Error::Parse {
        file: path.to_path_buf(),
        line: line_num,
        message: format!("invalid {name} value {trimmed:?}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(spec: &[(usize, &str)]) -> String {
        let mut out = String::new();
        for (col, value) in spec {
            let col0 = col.saturating_sub(1);
            while out.len() < col0 {
                out.push(' ');
            }
            out.push_str(value);
        }
        out
    }

    #[test]
    fn returns_empty_when_marker_missing() {
        let body = "no marker here\n/END/\n";
        let groups = read_evtech(body.as_bytes()).unwrap();
        assert!(groups.is_empty());
    }

    #[test]
    fn parses_and_renormalises() {
        let header = at(&[
            (6, "2270001000"),
            (21, " 25.0"),
            (26, " 50.0"),
            (35, "E00000000 "),
            (45, "E11111111 "),
        ]);
        let data = at(&[(1, " 2010"), (35, "      0.60"), (45, "      0.20")]);
        let body = format!("/EVAP TECH FRAC/\n{header}\n{data}\n/END/\n");
        let groups = read_evtech(body.as_bytes()).unwrap();
        assert_eq!(groups.len(), 1);
        let g = &groups[0];
        assert_eq!(g.fractions[0].0, "E00000000");
        assert_eq!(g.fractions[1].0, "E11111111");
        // 0.60 + 0.20 = 0.80; renormalised → 0.75 + 0.25
        assert!((g.fractions[0].1 - 0.75).abs() < 1e-6);
        assert!((g.fractions[1].1 - 0.25).abs() < 1e-6);
    }

    #[test]
    fn rejects_non_e_tech_code() {
        let header = at(&[
            (6, "2270001000"),
            (21, " 25.0"),
            (26, " 50.0"),
            (35, "BASE      "),
        ]);
        let body = format!("/EVAP TECH FRAC/\n{header}\n/END/\n");
        let err = read_evtech(body.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("evap tech code")),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn rejects_wrong_length_tech_code() {
        // E followed by only 4 chars (5 total instead of 9)
        let header = at(&[
            (6, "2270001000"),
            (21, " 25.0"),
            (26, " 50.0"),
            (35, "E0000     "),
        ]);
        let body = format!("/EVAP TECH FRAC/\n{header}\n/END/\n");
        let err = read_evtech(body.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("evap tech code")),
            other => panic!("unexpected: {other:?}"),
        }
    }
}

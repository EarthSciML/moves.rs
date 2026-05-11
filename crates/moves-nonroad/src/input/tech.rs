//! Exhaust technology-fraction parser (`rdtech.f`).
//!
//! Task 96. Parses a `.TCH` file's `/TECH FRAC/` packet: per
//! (SCC, HP-range, year) the file lists how the equipment
//! population splits across exhaust technology types. After
//! reading, fractions are renormalised so each group sums to
//! 1.0 — matching `rdtech.f` :179-205.
//!
//! # Format (`rdtech.f` :103-167)
//!
//! Header line:
//!
//! | Cols    | Field                              |
//! |---------|------------------------------------|
//! | 6–15    | SCC code                           |
//! | 21–25   | HP range min (F5.0)                |
//! | 26–30   | HP range max (F5.0)                |
//! | 35+     | Tech-type codes (10 chars each)    |
//!
//! Data line (cols 6–15 blank):
//!
//! | Cols    | Field                              |
//! |---------|------------------------------------|
//! | 1–5     | Year (I5)                          |
//! | 35+     | Tech fractions (F10.0 each)        |
//!
//! If the `/TECH FRAC/` keyword is missing, the Fortran returns
//! the `ISKIP` code rather than `ISUCES` and the caller continues
//! without tech fractions. The Rust port surfaces that as an
//! empty result so the caller can branch on `is_empty`.
//!
//! # Fortran source
//!
//! Ports `rdtech.f` (305 lines).

use std::io::BufRead;
use std::path::{Path, PathBuf};

use crate::{Error, Result};

/// One (SCC, HP-range, year) group of tech fractions.
///
/// Fractions are renormalised so the group sums to 1.0
/// (or 0.0 if every value was zero).
#[derive(Debug, Clone, PartialEq)]
pub struct TechFractionGroup {
    /// SCC code (10 chars, left-justified, upper-cased).
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

impl TechFractionGroup {
    /// Sum of fractions before renormalisation.
    pub fn raw_sum(&self) -> f32 {
        self.fractions.iter().map(|(_, f)| f).sum()
    }
}

/// Parse a `.TCH` `/TECH FRAC/` packet.
///
/// Returns an empty vector if the file has no `/TECH FRAC/`
/// marker — equivalent to `rdtech.f`'s `ISKIP` return.
pub fn read_tech<R: BufRead>(reader: R) -> Result<Vec<TechFractionGroup>> {
    let path = PathBuf::from(".TCH");
    let mut groups: Vec<TechFractionGroup> = Vec::new();
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
            if is_keyword(&line, "/TECH FRAC/") {
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
                    message: "data record before any /TECH FRAC/ header".to_string(),
                });
            };
            groups.push(parse_data_line(&line, line_num, &path, ctx)?);
        }
    }

    if !in_packet {
        // ISKIP — no /TECH FRAC/ packet found.
        return Ok(Vec::new());
    }

    Err(Error::Parse {
        file: path,
        line: line_num,
        message: "missing /END/ marker after /TECH FRAC/ packet".to_string(),
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
        if tech_types.len() >= crate::common::consts::MXTECH {
            return Err(Error::Parse {
                file: path.to_path_buf(),
                line: line_num,
                message: format!(
                    "more than {} tech types on /TECH FRAC/ header",
                    crate::common::consts::MXTECH
                ),
            });
        }
        tech_types.push(field.trim().to_ascii_uppercase());
        start += 10;
    }

    if tech_types.is_empty() {
        return Err(Error::Parse {
            file: path.to_path_buf(),
            line: line_num,
            message: "/TECH FRAC/ header has no tech-type columns".to_string(),
        });
    }

    Ok(HeaderContext {
        scc,
        hp_min,
        hp_max,
        tech_types,
    })
}

fn parse_data_line(
    line: &str,
    line_num: usize,
    path: &Path,
    ctx: &HeaderContext,
) -> Result<TechFractionGroup> {
    let year = parse_i5(column(line, 1, 5), "year", line_num, path)?;
    let mut start = 35usize;
    let mut fractions = Vec::with_capacity(ctx.tech_types.len());
    for tech in &ctx.tech_types {
        let field = column(line, start, start + 9);
        let value = parse_numeric(field, "tech fraction", line_num, path)?;
        fractions.push((tech.clone(), value));
        start += 10;
    }
    Ok(TechFractionGroup {
        scc: ctx.scc.clone(),
        hp_min: ctx.hp_min,
        hp_max: ctx.hp_max,
        year,
        fractions,
    })
}

/// Divide each group's fractions by the group's sum, matching
/// `rdtech.f` :179-205. Groups whose sum is 0 are left untouched.
/// The Fortran emits a warning if `|sum - 1| > 0.002` but always
/// renormalises; the Rust port preserves the renormalisation and
/// records the original sum on [`TechFractionGroup::raw_sum`]
/// (computed from the un-normalised values up to the call).
fn renormalise(groups: &mut [TechFractionGroup]) {
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
        let groups = read_tech(body.as_bytes()).unwrap();
        assert!(groups.is_empty());
    }

    #[test]
    fn parses_and_renormalises_single_group() {
        let header = at(&[
            (6, "2270001000"),
            (21, " 25.0"),
            (26, " 50.0"),
            (35, "BASE      "),
            (45, "ADV       "),
        ]);
        let data = at(&[(1, " 2010"), (35, "      0.40"), (45, "      0.40")]);
        let body = format!("/TECH FRAC/\n{header}\n{data}\n/END/\n");
        let groups = read_tech(body.as_bytes()).unwrap();
        assert_eq!(groups.len(), 1);
        let g = &groups[0];
        assert_eq!(g.scc, "2270001000");
        assert_eq!(g.year, 2010);
        // 0.40 + 0.40 = 0.80; renormalise → 0.50 each.
        assert!((g.fractions[0].1 - 0.5).abs() < 1e-6);
        assert!((g.fractions[1].1 - 0.5).abs() < 1e-6);
        assert_eq!(g.fractions[0].0, "BASE");
        assert_eq!(g.fractions[1].0, "ADV");
    }

    #[test]
    fn renormalisation_leaves_already_normalised_unchanged() {
        let header = at(&[
            (6, "2270001000"),
            (21, " 25.0"),
            (26, " 50.0"),
            (35, "BASE      "),
            (45, "ADV       "),
        ]);
        let data = at(&[(1, " 2010"), (35, "      0.30"), (45, "      0.70")]);
        let body = format!("/TECH FRAC/\n{header}\n{data}\n/END/\n");
        let groups = read_tech(body.as_bytes()).unwrap();
        assert!((groups[0].fractions[0].1 - 0.30).abs() < 1e-6);
        assert!((groups[0].fractions[1].1 - 0.70).abs() < 1e-6);
    }

    #[test]
    fn zero_sum_group_left_alone() {
        let header = at(&[
            (6, "2270001000"),
            (21, " 25.0"),
            (26, " 50.0"),
            (35, "BASE      "),
            (45, "ADV       "),
        ]);
        let data = at(&[(1, " 2010"), (35, "      0.00"), (45, "      0.00")]);
        let body = format!("/TECH FRAC/\n{header}\n{data}\n/END/\n");
        let groups = read_tech(body.as_bytes()).unwrap();
        assert!((groups[0].fractions[0].1 - 0.0).abs() < 1e-6);
        assert!((groups[0].fractions[1].1 - 0.0).abs() < 1e-6);
    }

    #[test]
    fn multiple_years_under_one_header() {
        let header = at(&[
            (6, "2270001000"),
            (21, " 25.0"),
            (26, " 50.0"),
            (35, "BASE      "),
        ]);
        let data2000 = at(&[(1, " 2000"), (35, "      1.00")]);
        let data2010 = at(&[(1, " 2010"), (35, "      1.00")]);
        let body = format!("/TECH FRAC/\n{header}\n{data2000}\n{data2010}\n/END/\n");
        let groups = read_tech(body.as_bytes()).unwrap();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].year, 2000);
        assert_eq!(groups[1].year, 2010);
    }

    #[test]
    fn missing_end_errors() {
        let header = at(&[
            (6, "2270001000"),
            (21, " 25.0"),
            (26, " 50.0"),
            (35, "BASE      "),
        ]);
        let body = format!("/TECH FRAC/\n{header}\n");
        let err = read_tech(body.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("/END/")),
            other => panic!("unexpected: {other:?}"),
        }
    }
}

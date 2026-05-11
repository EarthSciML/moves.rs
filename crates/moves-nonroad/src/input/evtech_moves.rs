//! MOVES-format evaporative technology-fraction parser
//! (`rdevtech_moves.f`).
//!
//! Task 96. Reads the `/MOVES EVAP TECH FRAC/` packet — same
//! two-line-per-entry layout as [`super::tech_moves`] but for
//! evap groups. Tech codes use the `E + 8 digits` encoding
//! documented at `rdevtech.f` :117-134 and validated here per
//! `rdevtech_moves.f` :140-141.
//!
//! After accumulation, fractions are renormalised per group to
//! sum to 1.0 (`rdevtech_moves.f` :197-224).
//!
//! # Fortran source
//!
//! Ports `rdevtech_moves.f` (337 lines).

use std::io::BufRead;
use std::path::{Path, PathBuf};

use crate::{Error, Result};

pub use super::evtech::EvapTechFractionGroup;

/// Parse a `/MOVES EVAP TECH FRAC/` packet.
///
/// Returns an empty vector if the marker is missing
/// (`rdevtech_moves.f`'s `ISKIP` return).
pub fn read_evtech_moves<R: BufRead>(reader: R) -> Result<Vec<EvapTechFractionGroup>> {
    let path = PathBuf::from(".EVT");
    let mut groups: Vec<EvapTechFractionGroup> = Vec::new();
    let mut in_packet = false;
    let mut found_end = false;
    let mut line_num = 0usize;
    let mut pending_id: Option<IdentLine> = None;

    for line_result in reader.lines() {
        line_num += 1;
        let line = line_result.map_err(|e| Error::Io {
            path: path.clone(),
            source: e,
        })?;

        if !in_packet {
            if is_keyword(&line, "/MOVES EVAP TECH FRAC/") {
                in_packet = true;
            }
            continue;
        }

        if is_keyword(&line, "/END/") {
            found_end = true;
            break;
        }

        if line.trim().is_empty() {
            continue;
        }

        match pending_id.take() {
            None => {
                if column(&line, 6, 15).trim().is_empty() {
                    continue;
                }
                pending_id = Some(parse_ident_line(&line, line_num, &path)?);
            }
            Some(ident) => {
                let (year, value) = parse_data_line(&line, line_num, &path)?;
                merge_into_group(&mut groups, &ident, year, value);
            }
        }
    }

    if !in_packet {
        return Ok(Vec::new());
    }
    if !found_end {
        return Err(Error::Parse {
            file: path,
            line: line_num,
            message: "missing /END/ marker after /MOVES EVAP TECH FRAC/ packet".to_string(),
        });
    }

    renormalise(&mut groups);
    Ok(groups)
}

#[derive(Debug, Clone)]
struct IdentLine {
    scc: String,
    hp_min: f32,
    hp_max: f32,
    tech_type: String,
}

fn parse_ident_line(line: &str, line_num: usize, path: &Path) -> Result<IdentLine> {
    let scc = column(line, 6, 15).trim().to_string();
    let hp_min = parse_numeric(column(line, 21, 25), "hp_min", line_num, path)?;
    let hp_max = parse_numeric(column(line, 26, 30), "hp_max", line_num, path)?;
    let tech_type = column(line, 35, 44).trim().to_ascii_uppercase();
    validate_evap_code(&tech_type, line_num, path)?;
    Ok(IdentLine {
        scc,
        hp_min,
        hp_max,
        tech_type,
    })
}

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

fn parse_data_line(line: &str, line_num: usize, path: &Path) -> Result<(i32, f32)> {
    let year = parse_i5(column(line, 1, 5), "year", line_num, path)?;
    let value = parse_numeric(column(line, 35, 44), "evap fraction", line_num, path)?;
    Ok((year, value))
}

fn merge_into_group(
    groups: &mut Vec<EvapTechFractionGroup>,
    ident: &IdentLine,
    year: i32,
    value: f32,
) {
    if let Some(existing) = groups.iter_mut().find(|g| {
        g.scc == ident.scc && g.year == year && g.hp_min == ident.hp_min && g.hp_max == ident.hp_max
    }) {
        if let Some(slot) = existing
            .fractions
            .iter_mut()
            .find(|(t, _)| t == &ident.tech_type)
        {
            slot.1 = value;
        } else {
            existing.fractions.push((ident.tech_type.clone(), value));
        }
        return;
    }
    groups.push(EvapTechFractionGroup {
        scc: ident.scc.clone(),
        hp_min: ident.hp_min,
        hp_max: ident.hp_max,
        year,
        fractions: vec![(ident.tech_type.clone(), value)],
    });
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

    fn ident(scc: &str, tech: &str) -> String {
        at(&[
            (6, scc),
            (21, " 25.0"),
            (26, " 50.0"),
            (35, &format!("{tech:<10}")),
        ])
    }

    fn data(year: &str, value: &str) -> String {
        at(&[(1, year), (35, value)])
    }

    #[test]
    fn returns_empty_when_marker_missing() {
        let body = "no marker here\n/END/\n";
        let groups = read_evtech_moves(body.as_bytes()).unwrap();
        assert!(groups.is_empty());
    }

    #[test]
    fn pairs_ident_and_data_lines() {
        let body = format!(
            "/MOVES EVAP TECH FRAC/\n{}\n{}\n{}\n{}\n/END/\n",
            ident("2270001000", "E00000000"),
            data(" 2010", "      0.60"),
            ident("2270001000", "E11111111"),
            data(" 2010", "      0.40"),
        );
        let groups = read_evtech_moves(body.as_bytes()).unwrap();
        assert_eq!(groups.len(), 1);
        let g = &groups[0];
        assert_eq!(g.fractions.len(), 2);
        assert_eq!(g.fractions[0].0, "E00000000");
        assert!((g.fractions[0].1 - 0.6).abs() < 1e-6);
        assert!((g.fractions[1].1 - 0.4).abs() < 1e-6);
    }

    #[test]
    fn rejects_non_e_tech_code() {
        let body = format!(
            "/MOVES EVAP TECH FRAC/\n{}\n{}\n/END/\n",
            ident("2270001000", "BASE"),
            data(" 2010", "      1.00"),
        );
        let err = read_evtech_moves(body.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("evap tech code")),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn duplicate_tech_overwrites() {
        let body = format!(
            "/MOVES EVAP TECH FRAC/\n{}\n{}\n{}\n{}\n/END/\n",
            ident("2270001000", "E00000000"),
            data(" 2010", "      0.20"),
            ident("2270001000", "E00000000"),
            data(" 2010", "      0.80"),
        );
        let groups = read_evtech_moves(body.as_bytes()).unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].fractions.len(), 1);
        assert!((groups[0].fractions[0].1 - 1.0).abs() < 1e-6);
    }
}

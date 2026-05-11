//! Population (`.POP`) parser (`rdpop.f`, `getpop.f`).
//!
//! Task 94. Parses the NONROAD `.POP` (population) input format,
//! which lists base-year equipment populations by FIPS region,
//! subregion, year, SCC, and horsepower category.
//!
//! # Format
//!
//! `.POP` files are line-oriented text with column-positional
//! fields. The data block starts after a `/POPULATION/` packet
//! marker and ends at an `/END/` marker. Each data line is up to
//! 160 characters (`2*MXSTR` in the Fortran source); fields are
//! parsed from fixed columns (1-based, inclusive — preserved here
//! so the layout matches `rdpop.f` and `getpop.f` directly):
//!
//! | Cols    | Field                          | Source ref      |
//! |---------|--------------------------------|-----------------|
//! | 1–5     | FIPS code                      | `rdpop.f` :139  |
//! | 7–11    | Subregion                      | `rdpop.f` :140  |
//! | 13–16   | Year (I4)                      | `rdpop.f` :141  |
//! | 18–27   | SCC code                       | `rdpop.f` :138  |
//! | 70–74   | HP min (F5.0)                  | `rdpop.f` :142  |
//! | 76–80   | HP max (F5.0)                  | `rdpop.f` :144  |
//! | 82–86   | HP avg (F5.0, optional)        | `rdpop.f` :146  |
//! | 88–92   | Usage factor (F5.0)            | `getpop.f` :128 |
//! | 93–102  | Tech / dist code               | `getpop.f` :129 |
//! | 108–122 | Population value (digits, may  | `getpop.f` :201 |
//! |         | contain commas)                |                 |
//!
//! If `hp_avg` is blank, it defaults to `(hp_min + hp_max) / 2`,
//! matching `rdpop.f` :151.
//!
//! # Filtering (deferred)
//!
//! `rdpop.f` performs region-level filtering (national-only,
//! sub-county, state-only) and equipment-list filtering against
//! the COMMON-block state set up by `getind.f` / `iniasc.f`.
//! That filtering is deferred to the higher-level driver
//! (Tasks 99 and 103); this module returns every well-formed
//! population record.
//!
//! # Sorting (deferred)
//!
//! `rdpop.f` writes a sorted scratch file (`popdir.txt` →
//! `spopfl`) keyed by `SCC || HP_avg || FIPS || subregion ||
//! year`. The Rust port does not need a scratch file — callers
//! sort the returned `Vec<PopulationRecord>` if needed.

use crate::{Error, Result};
use std::io::BufRead;
use std::path::PathBuf;

/// One parsed population record from a `.POP` file.
#[derive(Debug, Clone, PartialEq)]
pub struct PopulationRecord {
    /// FIPS region code (state || county; 5 chars, left-justified).
    pub fips: String,
    /// Subregion code (5 chars).
    pub subregion: String,
    /// Calendar year.
    pub year: i32,
    /// SCC equipment code (10 chars).
    pub scc: String,
    /// Horsepower-category lower bound.
    pub hp_min: f32,
    /// Horsepower-category upper bound.
    pub hp_max: f32,
    /// Horsepower-category midpoint (`(hp_min + hp_max) / 2` if
    /// blank in the file).
    pub hp_avg: f32,
    /// Usage factor (annual hours, etc.).
    pub usage: f32,
    /// Technology/distribution code (10 chars, left-justified).
    pub tech_code: String,
    /// Equipment population.
    pub population: f64,
}

/// Parse a `.POP` file into a vector of [`PopulationRecord`].
///
/// Skips lines before the `/POPULATION/` packet marker and stops
/// at the `/END/` marker. Returns an error if the packet is
/// missing, the file ends before `/END/`, or any record has
/// malformed numeric fields.
pub fn read_pop<R: BufRead>(reader: R) -> Result<Vec<PopulationRecord>> {
    let path = PathBuf::from(".POP");
    let mut records = Vec::new();
    let mut in_packet = false;
    let mut found_end = false;
    let mut line_num: usize = 0;

    for line_result in reader.lines() {
        line_num += 1;
        let line = line_result.map_err(|e| Error::Io {
            path: path.clone(),
            source: e,
        })?;

        if !in_packet {
            if is_keyword(&line, "/POPULATION/") {
                in_packet = true;
            }
            continue;
        }

        if is_keyword(&line, "/END/") {
            found_end = true;
            break;
        }

        // Blank lines inside the packet are tolerated (the Fortran
        // parser would treat them as records with empty fields and
        // bail out on the numeric reads; we skip them to avoid
        // confusing error messages on whitespace).
        if line.trim().is_empty() {
            continue;
        }

        records.push(parse_pop_line(&line, line_num, &path)?);
    }

    if !in_packet {
        return Err(Error::Parse {
            file: path,
            line: line_num,
            message: "missing /POPULATION/ packet marker".to_string(),
        });
    }
    if !found_end {
        return Err(Error::Parse {
            file: path,
            line: line_num,
            message: "missing /END/ marker after /POPULATION/ packet".to_string(),
        });
    }

    Ok(records)
}

fn parse_pop_line(line: &str, line_num: usize, path: &PathBuf) -> Result<PopulationRecord> {
    let fips = column(line, 1, 5).trim().to_string();
    let subregion = column(line, 7, 11).trim().to_string();
    let year = parse_int(column(line, 13, 16), "year", line, line_num, path)?;
    let scc = column(line, 18, 27).trim().to_string();
    let hp_min = parse_f5(column(line, 70, 74), "hp_min", line, line_num, path)?;
    let hp_max = parse_f5(column(line, 76, 80), "hp_max", line, line_num, path)?;

    let hp_avg_field = column(line, 82, 86);
    let hp_avg = if hp_avg_field.trim().is_empty() {
        (hp_min + hp_max) / 2.0
    } else {
        parse_f5(hp_avg_field, "hp_avg", line, line_num, path)?
    };

    let usage = parse_f5(column(line, 88, 92), "usage", line, line_num, path)?;
    let tech_code = column(line, 93, 102).trim().to_string();
    let population = parse_pop_value(column(line, 108, 122), line, line_num, path)?;

    Ok(PopulationRecord {
        fips,
        subregion,
        year,
        scc,
        hp_min,
        hp_max,
        hp_avg,
        usage,
        tech_code,
        population,
    })
}

/// Returns true if the trimmed start of `line` matches `keyword`
/// case-insensitively. Mirrors `lftjst` + `low2up` + comparison
/// against `KEYEND` in the Fortran source.
fn is_keyword(line: &str, keyword: &str) -> bool {
    line.trim_start()
        .get(..keyword.len())
        .map(|s| s.eq_ignore_ascii_case(keyword))
        .unwrap_or(false)
}

/// Extract a 1-based, inclusive column range from `line`.
///
/// Returns an empty string slice if the line is shorter than the
/// requested range — matching Fortran's blank-padded character
/// substring semantics for short records.
fn column(line: &str, start_1based: usize, end_1based: usize) -> &str {
    let start = start_1based.saturating_sub(1);
    let end = end_1based.min(line.len());
    if start >= end {
        return "";
    }
    &line[start..end]
}

fn parse_int(
    field: &str,
    name: &str,
    line: &str,
    line_num: usize,
    path: &PathBuf,
) -> Result<i32> {
    field.trim().parse::<i32>().map_err(|_| Error::Parse {
        file: path.clone(),
        line: line_num,
        message: format!("invalid {name} value {:?}: line {:?}", field, line),
    })
}

fn parse_f5(
    field: &str,
    name: &str,
    line: &str,
    line_num: usize,
    path: &PathBuf,
) -> Result<f32> {
    let trimmed = field.trim();
    if trimmed.is_empty() {
        return Err(Error::Parse {
            file: path.clone(),
            line: line_num,
            message: format!("empty {name} field on line {:?}", line),
        });
    }
    trimmed.parse::<f32>().map_err(|_| Error::Parse {
        file: path.clone(),
        line: line_num,
        message: format!("invalid {name} value {:?}: line {:?}", field, line),
    })
}

/// Parse the population value field. The Fortran code (`getpop.f`
/// :201–209) strips spaces and commas from columns 108–122 before
/// reading the result as a number.
fn parse_pop_value(
    field: &str,
    line: &str,
    line_num: usize,
    path: &PathBuf,
) -> Result<f64> {
    let cleaned: String = field
        .chars()
        .filter(|c| *c != ' ' && *c != ',')
        .collect();
    if cleaned.is_empty() {
        return Err(Error::Parse {
            file: path.clone(),
            line: line_num,
            message: format!("empty population field on line {:?}", line),
        });
    }
    cleaned.parse::<f64>().map_err(|_| Error::Parse {
        file: path.clone(),
        line: line_num,
        message: format!("invalid population value {:?}: line {:?}", field, line),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_pop_line(
        fips: &str,
        sub: &str,
        year: &str,
        scc: &str,
        hp_min: &str,
        hp_max: &str,
        hp_avg: &str,
        usage: &str,
        tech: &str,
        pop: &str,
    ) -> String {
        // Build a 122-char fixed-width record matching rdpop.f /
        // getpop.f column layout.
        let mut buf = vec![b' '; 130];
        let put = |buf: &mut [u8], start_1based: usize, value: &str, width: usize| {
            let start = start_1based - 1;
            let bytes = value.as_bytes();
            let n = bytes.len().min(width);
            buf[start..start + n].copy_from_slice(&bytes[..n]);
        };
        put(&mut buf, 1, fips, 5);
        put(&mut buf, 7, sub, 5);
        put(&mut buf, 13, year, 4);
        put(&mut buf, 18, scc, 10);
        // right-justify HP fields in 5-char slot
        let put_right = |buf: &mut [u8], start_1based: usize, value: &str, width: usize| {
            let pad = width.saturating_sub(value.len());
            let start = start_1based - 1 + pad;
            let bytes = value.as_bytes();
            let n = bytes.len().min(width - pad);
            buf[start..start + n].copy_from_slice(&bytes[..n]);
        };
        put_right(&mut buf, 70, hp_min, 5);
        put_right(&mut buf, 76, hp_max, 5);
        put_right(&mut buf, 82, hp_avg, 5);
        put_right(&mut buf, 88, usage, 5);
        put(&mut buf, 93, tech, 10);
        put_right(&mut buf, 108, pop, 15);
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn reads_single_population_record() {
        let line = build_pop_line(
            "06000", "00000", "2020", "2270002003", "0", "25", "11", "500",
            "PERS_TRUC", "1000",
        );
        let input = format!("/POPULATION/\n{line}\n/END/\n");

        let records = read_pop(input.as_bytes()).unwrap();
        assert_eq!(records.len(), 1);
        let r = &records[0];
        assert_eq!(r.fips, "06000");
        assert_eq!(r.subregion, "00000");
        assert_eq!(r.year, 2020);
        assert_eq!(r.scc, "2270002003");
        assert_eq!(r.hp_min, 0.0);
        assert_eq!(r.hp_max, 25.0);
        assert_eq!(r.hp_avg, 11.0);
        assert_eq!(r.usage, 500.0);
        assert_eq!(r.tech_code, "PERS_TRUC");
        assert_eq!(r.population, 1000.0);
    }

    #[test]
    fn defaults_hp_avg_when_blank() {
        let line = build_pop_line(
            "17031", "00000", "2020", "2270002003", "25", "75", "", "100",
            "FOO", "5,000",
        );
        let input = format!("/POPULATION/\n{line}\n/END/\n");

        let records = read_pop(input.as_bytes()).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].hp_avg, 50.0);
        assert_eq!(records[0].population, 5000.0);
    }

    #[test]
    fn skips_lines_before_population_marker() {
        let line = build_pop_line(
            "06000", "00000", "2020", "2270002003", "0", "25", "", "100",
            "X", "1",
        );
        let input = format!("# header\nignored garbage\n/POPULATION/\n{line}\n/END/\n");

        let records = read_pop(input.as_bytes()).unwrap();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn errors_when_population_marker_missing() {
        let line = build_pop_line(
            "06000", "00000", "2020", "2270002003", "0", "25", "", "100",
            "X", "1",
        );
        let input = format!("{line}\n");

        let err = read_pop(input.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => {
                assert!(message.contains("/POPULATION/"));
            }
            other => panic!("expected Error::Parse, got {other:?}"),
        }
    }

    #[test]
    fn errors_when_end_marker_missing() {
        let line = build_pop_line(
            "06000", "00000", "2020", "2270002003", "0", "25", "", "100",
            "X", "1",
        );
        let input = format!("/POPULATION/\n{line}\n");

        let err = read_pop(input.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => {
                assert!(message.contains("/END/"));
            }
            other => panic!("expected Error::Parse, got {other:?}"),
        }
    }

    #[test]
    fn errors_on_invalid_year() {
        let line = build_pop_line(
            "06000", "00000", "abcd", "2270002003", "0", "25", "", "100",
            "X", "1",
        );
        let input = format!("/POPULATION/\n{line}\n/END/\n");

        let err = read_pop(input.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, line, .. } => {
                assert!(message.contains("year"), "got {message}");
                assert_eq!(line, 2);
            }
            other => panic!("expected Error::Parse, got {other:?}"),
        }
    }

    #[test]
    fn parses_population_with_commas() {
        let line = build_pop_line(
            "06000", "00000", "2020", "2270002003", "25", "75", "50", "100",
            "X", "1,234,567",
        );
        let input = format!("/POPULATION/\n{line}\n/END/\n");

        let records = read_pop(input.as_bytes()).unwrap();
        assert_eq!(records[0].population, 1_234_567.0);
    }

    #[test]
    fn returns_empty_packet_with_no_records() {
        let input = "/POPULATION/\n/END/\n";
        let records = read_pop(input.as_bytes()).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn keyword_match_is_case_insensitive() {
        let line = build_pop_line(
            "06000", "00000", "2020", "2270002003", "0", "25", "", "100",
            "X", "1",
        );
        let input = format!("/Population/\n{line}\n/end/\n");

        let records = read_pop(input.as_bytes()).unwrap();
        assert_eq!(records.len(), 1);
    }
}

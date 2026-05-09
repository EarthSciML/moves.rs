//! Population-file parser (`rdpop.f`).
//!
//! Task 94. Parses the `/POPULATION/` packet of NONROAD `.POP`
//! files. Each record carries a county FIPS, a subregion code, an
//! episode year, an SCC, and a horsepower range; downstream
//! consumers (Tasks 103-104) re-extract the equipment description
//! and population value from byte positions past column 86, so the
//! verbatim line is preserved alongside the parsed fields.
//!
//! # Format
//!
//! Column-aligned text lines (Fortran reads them as `character*160`;
//! shorter lines are treated as blank-padded). Field columns are
//! 1-based, matching the Fortran source.
//!
//! | Cols  | Field      | Fortran format |
//! |-------|------------|----------------|
//! | 1–5   | FIPS       | `A5`           |
//! | 7–11  | Subregion  | `A5`           |
//! | 13–16 | Year       | `A4`           |
//! | 18–27 | SCC        | `A10`          |
//! | 70–74 | HP min     | `F5.0`         |
//! | 76–80 | HP max     | `F5.0`         |
//! | 82–86 | HP avg     | `F5.0`         |
//!
//! The Fortran source fills a blank `HP avg` field with
//! `(HP min + HP max) / 2`; this port matches that behavior. The
//! HP-range validation against the internal `hpclev[]` table and
//! the region-level / SCC-allowlist filters live in `getpop.f`
//! (Task 103) — this parser surfaces the raw records and lets the
//! caller decide.
//!
//! Records are framed by a `/POPULATION/` opening keyword and an
//! `/END/` closing keyword. Lines outside the packet (including
//! blank lines and `#`-prefixed comments) are skipped.
//!
//! # Fortran source
//!
//! Ports `rdpop.f` (446 lines). The Fortran routine also handles
//! the per-record sort and writes a sorted scratch file; this port
//! emits records in input order and exposes the sort key via
//! [`PopulationRecord::sort_key`] for callers reproducing the
//! Fortran output ordering.

use crate::{Error, Result};
use std::io::BufRead;
use std::path::PathBuf;

/// One record from the `/POPULATION/` packet.
#[derive(Debug, Clone, PartialEq)]
pub struct PopulationRecord {
    /// 5-character FIPS code (no padding adjustment beyond `trim`;
    /// `"00000"` denotes a national-totals record).
    pub fips: String,
    /// 5-character subregion code (blank for whole-county records).
    pub subregion: String,
    /// 4-character episode-year string (kept as text to match the
    /// Fortran sort key).
    pub year: String,
    /// 10-character SCC code (left-justified, blank-stripped).
    pub scc: String,
    /// Minimum horsepower bound for this record's HP bin.
    pub hp_min: f32,
    /// Maximum horsepower bound for this record's HP bin.
    pub hp_max: f32,
    /// Average horsepower; falls back to `(hp_min + hp_max) / 2`
    /// when the source field is blank.
    pub hp_avg: f32,
    /// The verbatim line, preserved so downstream consumers can
    /// re-extract the description and population value from the
    /// same byte positions the Fortran code expects.
    pub line: String,
}

impl PopulationRecord {
    /// Sort key matching the Fortran concatenation
    /// `scc(10) // nint(hp_avg)(5) // fips(5) // subregion(5) // year(4)`.
    /// 30 characters wide, total.
    pub fn sort_key(&self) -> String {
        format!(
            "{:<10}{:>5}{:<5}{:<5}{:<4}",
            self.scc,
            self.hp_avg.round() as i32,
            self.fips,
            self.subregion,
            self.year,
        )
    }
}

/// Parse the `/POPULATION/` packet from a `.POP` reader.
pub fn read_pop<R: BufRead>(reader: R) -> Result<Vec<PopulationRecord>> {
    let path = PathBuf::from(".POP");
    let mut out = Vec::new();
    let mut in_packet = false;
    let mut found_end = false;
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

        if !in_packet {
            if upper.starts_with("/POPULATION/") {
                in_packet = true;
            }
            continue;
        }
        if upper.starts_with("/END/") {
            found_end = true;
            break;
        }
        out.push(parse_record(&line, line_num, &path)?);
    }

    if !in_packet {
        return Err(Error::Parse {
            file: path,
            line: line_num,
            message: "no /POPULATION/ packet found".to_string(),
        });
    }
    if !found_end {
        return Err(Error::Parse {
            file: path,
            line: line_num,
            message: "missing /END/ marker for /POPULATION/ packet".to_string(),
        });
    }
    Ok(out)
}

/// Parse the packet and return records sorted by [`PopulationRecord::sort_key`].
///
/// Reproduces the Fortran ordering: `scc, hp_avg, fips, subregion, year`.
pub fn read_pop_sorted<R: BufRead>(reader: R) -> Result<Vec<PopulationRecord>> {
    let mut records = read_pop(reader)?;
    records.sort_by_key(|a| a.sort_key());
    Ok(records)
}

fn parse_record(line: &str, line_num: usize, path: &std::path::Path) -> Result<PopulationRecord> {
    let bytes = line.as_bytes();

    let fips = column_string(bytes, 0, 5);
    let subregion = column_string(bytes, 6, 11);
    let year = column_string(bytes, 12, 16);
    let scc = column_string(bytes, 17, 27);
    let hp_min_raw = column_string(bytes, 69, 74);
    let hp_max_raw = column_string(bytes, 75, 80);
    let hp_avg_raw = column_string(bytes, 81, 86);

    if scc.trim().is_empty() {
        return Err(Error::Parse {
            file: path.to_path_buf(),
            line: line_num,
            message: "missing SCC code".to_string(),
        });
    }
    if fips.trim().is_empty() {
        return Err(Error::Parse {
            file: path.to_path_buf(),
            line: line_num,
            message: "missing FIPS code".to_string(),
        });
    }
    if year.trim().is_empty() {
        return Err(Error::Parse {
            file: path.to_path_buf(),
            line: line_num,
            message: "missing year".to_string(),
        });
    }

    let hp_min = parse_hp(&hp_min_raw, "hp_min", line_num, path)?;
    let hp_max = parse_hp(&hp_max_raw, "hp_max", line_num, path)?;
    let hp_avg = if hp_avg_raw.trim().is_empty() {
        (hp_min + hp_max) / 2.0
    } else {
        parse_hp(&hp_avg_raw, "hp_avg", line_num, path)?
    };

    if hp_avg < hp_min || hp_avg > hp_max {
        return Err(Error::Parse {
            file: path.to_path_buf(),
            line: line_num,
            message: format!(
                "hp_avg ({}) outside [hp_min={}, hp_max={}] for SCC {}",
                hp_avg,
                hp_min,
                hp_max,
                scc.trim()
            ),
        });
    }

    Ok(PopulationRecord {
        fips: fips.trim().to_string(),
        subregion: subregion.trim().to_string(),
        year: year.trim().to_string(),
        scc: scc.trim().to_string(),
        hp_min,
        hp_max,
        hp_avg,
        line: line.to_string(),
    })
}

fn column_string(bytes: &[u8], start: usize, end: usize) -> String {
    if start >= bytes.len() {
        return String::new();
    }
    let end = end.min(bytes.len());
    String::from_utf8_lossy(&bytes[start..end]).into_owned()
}

fn parse_hp(s: &str, name: &str, line_num: usize, path: &std::path::Path) -> Result<f32> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(0.0);
    }
    s.parse::<f32>().map_err(|_| Error::Parse {
        file: path.to_path_buf(),
        line: line_num,
        message: format!("invalid {}: {:?}", name, s),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // Header alignment cheat-sheet (1-based columns):
    //          1    6    11   16   21   26   31   36   41   46   51   56   61   66   71   76   81   86
    //          |    |    |    |    |    |    |    |    |    |    |    |    |    |    |    |    |    |
    // example: 17031 ALL  2020 2270001000Lawn & Garden                                  0.0  6.0  3.0  1234.5

    fn make_line(
        fips: &str,
        sub: &str,
        year: &str,
        scc: &str,
        hp_min: &str,
        hp_max: &str,
        hp_avg: &str,
        tail: &str,
    ) -> String {
        // Build a 160-byte ASCII line with each field at its
        // documented column position.
        let mut buf = vec![b' '; 160];
        let put = |buf: &mut Vec<u8>, col0: usize, value: &str, width: usize| {
            let bytes = value.as_bytes();
            for (i, &b) in bytes.iter().take(width).enumerate() {
                buf[col0 + i] = b;
            }
        };
        put(&mut buf, 0, fips, 5);
        put(&mut buf, 6, sub, 5);
        put(&mut buf, 12, year, 4);
        put(&mut buf, 17, scc, 10);
        put(&mut buf, 69, hp_min, 5);
        put(&mut buf, 75, hp_max, 5);
        put(&mut buf, 81, hp_avg, 5);
        put(&mut buf, 87, tail, tail.len().min(160 - 87));
        String::from_utf8(buf).unwrap()
    }

    fn pack(rows: &[String]) -> String {
        let mut s = String::from("/POPULATION/\n");
        for r in rows {
            s.push_str(r);
            s.push('\n');
        }
        s.push_str("/END/\n");
        s
    }

    #[test]
    fn reads_a_basic_record() {
        let row = make_line(
            "17031",
            "00000",
            "2020",
            "2270001000",
            "  0.0",
            "  6.0",
            "  3.0",
            "Lawn",
        );
        let input = pack(&[row]);

        let recs = read_pop(input.as_bytes()).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].fips, "17031");
        assert_eq!(recs[0].subregion, "00000");
        assert_eq!(recs[0].year, "2020");
        assert_eq!(recs[0].scc, "2270001000");
        assert!((recs[0].hp_min - 0.0).abs() < 1e-6);
        assert!((recs[0].hp_max - 6.0).abs() < 1e-6);
        assert!((recs[0].hp_avg - 3.0).abs() < 1e-6);
    }

    #[test]
    fn fills_blank_hp_avg() {
        let row = make_line(
            "17031",
            "",
            "2020",
            "2270001000",
            "  0.0",
            " 11.0",
            "     ",
            "",
        );
        let input = pack(&[row]);
        let recs = read_pop(input.as_bytes()).unwrap();
        assert!((recs[0].hp_avg - 5.5).abs() < 1e-6);
        assert_eq!(recs[0].subregion, "");
    }

    #[test]
    fn rejects_hp_avg_outside_range() {
        let row = make_line(
            "17031",
            "",
            "2020",
            "2270001000",
            "  0.0",
            "  6.0",
            " 25.0",
            "",
        );
        let input = pack(&[row]);
        let err = read_pop(input.as_bytes()).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("hp_avg"), "{}", msg);
    }

    #[test]
    fn skips_lines_before_packet_and_comments() {
        let row = make_line(
            "06037",
            "",
            "2020",
            "2270002000",
            "  6.0",
            " 11.0",
            "  9.0",
            "Mower",
        );
        let mut input = String::from("# header comment\n\nNot a packet line\n");
        input.push_str(&pack(&[row]));
        let recs = read_pop(input.as_bytes()).unwrap();
        assert_eq!(recs.len(), 1);
    }

    #[test]
    fn errors_when_packet_missing() {
        let input = "no packet here\n";
        let err = read_pop(input.as_bytes()).unwrap_err();
        assert!(format!("{}", err).contains("/POPULATION/"));
    }

    #[test]
    fn errors_when_end_missing() {
        let row = make_line(
            "17031",
            "",
            "2020",
            "2270001000",
            "  0.0",
            "  6.0",
            "  3.0",
            "",
        );
        let input = format!("/POPULATION/\n{}\n", row);
        let err = read_pop(input.as_bytes()).unwrap_err();
        assert!(format!("{}", err).contains("/END/"));
    }

    #[test]
    fn errors_on_blank_scc() {
        let row = make_line(
            "17031",
            "",
            "2020",
            "          ",
            "  0.0",
            "  6.0",
            "  3.0",
            "",
        );
        let input = pack(&[row]);
        let err = read_pop(input.as_bytes()).unwrap_err();
        assert!(format!("{}", err).contains("SCC"));
    }

    #[test]
    fn sort_key_reproduces_fortran_ordering() {
        let r1 = PopulationRecord {
            fips: "06037".into(),
            subregion: "".into(),
            year: "2020".into(),
            scc: "2270002000".into(),
            hp_min: 6.0,
            hp_max: 11.0,
            hp_avg: 9.0,
            line: String::new(),
        };
        let r2 = PopulationRecord {
            fips: "17031".into(),
            subregion: "".into(),
            year: "2020".into(),
            scc: "2270001000".into(),
            hp_min: 0.0,
            hp_max: 6.0,
            hp_avg: 3.0,
            line: String::new(),
        };
        assert!(r2.sort_key() < r1.sort_key());
    }

    #[test]
    fn read_pop_sorted_reorders_records() {
        let high = make_line(
            "06037",
            "",
            "2020",
            "2270002000",
            "  6.0",
            " 11.0",
            "  9.0",
            "B",
        );
        let low = make_line(
            "17031",
            "",
            "2020",
            "2270001000",
            "  0.0",
            "  6.0",
            "  3.0",
            "A",
        );
        // Insert in reverse order; sorted output should put SCC 2270001000 first.
        let input = pack(&[high, low]);
        let recs = read_pop_sorted(input.as_bytes()).unwrap();
        assert_eq!(recs[0].scc, "2270001000");
        assert_eq!(recs[1].scc, "2270002000");
    }

    #[test]
    fn handles_short_lines_gracefully() {
        // Only fields up to col 27 are present; HP fields are blank.
        // hp_min/hp_max default to 0 which satisfies the bounds check.
        let mut input = String::from("/POPULATION/\n");
        input.push_str("17031 00000 2020 2270001000\n");
        input.push_str("/END/\n");
        let recs = read_pop(input.as_bytes()).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].scc, "2270001000");
        assert!((recs[0].hp_min - 0.0).abs() < 1e-6);
    }
}

//! Exhaust emission-factor parser (`rdemfc.f`).
//!
//! Task 96. Parses an exhaust emission-factor file (`.EMF`)
//! containing per-pollutant emission factors keyed by SCC,
//! horsepower range, technology type, and model year.
//!
//! Also serves the BSFC dispatcher (`rdbsfc.f`): brake-specific
//! fuel-consumption files share the `.EMF` format and call
//! `rdemfc` with a `BSFC` pseudo-pollutant. The Fortran lets a BSFC
//! file omit the per-tech-type columns by checking `iounit .EQ.
//! IORBSF` when a blank field appears at position 35. The Rust port
//! exposes that fallback through the [`Variant::Bsfc`] flag.
//!
//! # Format
//!
//! `/EMSFAC/`-delimited packet of column-positional records. Each
//! `(SCC, HP-range)` group is introduced by a header line that
//! lists the technology-type codes carried by the following data
//! lines. Header columns (1-based, inclusive — matching `rdemfc.f`
//! lines 141–179):
//!
//! | Cols    | Field                                       |
//! |---------|---------------------------------------------|
//! | 6–15    | SCC code (10 chars)                         |
//! | 21–25   | HP range min (F5.0)                         |
//! | 26–30   | HP range max (F5.0)                         |
//! | 35+     | Tech-type codes (10 chars each), then       |
//! |         | a units keyword (`G/HR`, `G/HP-HR`, …),     |
//! |         | then a pollutant code (10 chars).           |
//!
//! Data lines (cols 6–15 blank):
//!
//! | Cols    | Field                                       |
//! |---------|---------------------------------------------|
//! | 1–5     | Year (I5)                                   |
//! | 35+     | One F10.0 factor per tech-type column       |
//!
//! Units keywords are `G/HR`, `G/HP-HR`, `G/GALLON`, `G/TANK`,
//! `G/DAY`, `G/START`, `MULT` (see `nonrdefc.inc`).
//!
//! For pollutant `CRA` (crankcase HC, `PollutantIndex::Crankcase`),
//! units must be `MULT` — mirrors the check at `rdemfc.f` :173.
//!
//! # Fortran source
//!
//! Ports `rdemfc.f` (354 lines).

use std::io::BufRead;
use std::path::{Path, PathBuf};

use crate::{Error, Result};

/// Units keyword used in an `.EMF` header.
///
/// Mirrors the `IDXGHR..IDXMLT` constants in `nonrdefc.inc` and
/// the `KEYGHR..KEYMLT` strings recognised by the Fortran parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmissionUnits {
    /// `G/HR` — grams per hour.
    GramsPerHour,
    /// `G/HP-HR` — grams per horsepower-hour.
    GramsPerHpHour,
    /// `G/GALLON` — grams per gallon of fuel.
    GramsPerGallon,
    /// `G/TANK` — grams per tank volume.
    GramsPerTank,
    /// `G/DAY` — grams per day.
    GramsPerDay,
    /// `G/START` — grams per engine start.
    GramsPerStart,
    /// `MULT` — unitless multiplier (used for crankcase HC).
    Multiplier,
}

impl EmissionUnits {
    /// Match a left-justified, upper-cased 10-char field against
    /// the keyword set. Returns `None` for tech-type columns.
    fn from_keyword(field: &str) -> Option<Self> {
        let trimmed = field.trim_end();
        match trimmed {
            "G/HR" => Some(Self::GramsPerHour),
            "G/HP-HR" => Some(Self::GramsPerHpHour),
            "G/GALLON" => Some(Self::GramsPerGallon),
            "G/TANK" => Some(Self::GramsPerTank),
            "G/DAY" => Some(Self::GramsPerDay),
            "G/START" => Some(Self::GramsPerStart),
            "MULT" => Some(Self::Multiplier),
            _ => None,
        }
    }
}

/// File variant — drives the BSFC-specific lenience documented at
/// `rdemfc.f` :150 (allow ntch=0 if the first tech-type column is
/// blank).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Variant {
    /// Regular `.EMF` file. Tech-type columns are required.
    Emf,
    /// BSFC file (same syntax, but the units keyword may sit at
    /// col 35 with no tech-types preceding it).
    Bsfc,
}

/// One emission-factor record (one tech-type × year × HP-range).
#[derive(Debug, Clone, PartialEq)]
pub struct EmissionFactorRecord {
    /// SCC code (10 chars, left-justified, upper-cased).
    pub scc: String,
    /// Technology-type code (10 chars, left-justified,
    /// upper-cased). Empty for BSFC records that omit the
    /// tech-type column.
    pub tech_type: String,
    /// Horsepower-range minimum.
    pub hp_min: f32,
    /// Horsepower-range maximum.
    pub hp_max: f32,
    /// Model year.
    pub year: i32,
    /// Units keyword (shared per HP-range block).
    pub units: EmissionUnits,
    /// Emission-factor value, in [`Self::units`].
    pub factor: f32,
}

/// Parse an `.EMF` (`/EMSFAC/`) packet.
///
/// `expected_pollutant` is the 10-char left-justified pollutant
/// code the file must announce (e.g. `"THC"`, `"CO"`, `"CRA"`,
/// `"BSFC"`). The comparison is case-insensitive and trim-aware.
///
/// `enforce_mult_for_crank` mirrors the `idxpol .EQ. IDXCRA`
/// branch at `rdemfc.f` :173 — when the caller is parsing the
/// crankcase HC pollutant, the parser refuses any non-`MULT`
/// units. The flag is decoupled from `expected_pollutant` so that
/// integrators (e.g. `efls`) decide the pollutant-index policy
/// once, here in Rust, rather than relying on a column literal.
pub fn read_emf<R: BufRead>(
    reader: R,
    expected_pollutant: &str,
    enforce_mult_for_crank: bool,
) -> Result<Vec<EmissionFactorRecord>> {
    parse(
        reader,
        expected_pollutant,
        enforce_mult_for_crank,
        Variant::Emf,
    )
}

/// Parse a BSFC file (same `.EMF` syntax but with the
/// `Variant::Bsfc` lenience — see [`Variant`]).
///
/// Mirrors `rdbsfc.f`: the caller passes the BSFC pseudo-pollutant
/// `"BSFC"` and the crank-units enforcement is implicitly off
/// (the Fortran calls `rdemfc` with `idxpol = 0`).
pub fn read_bsfc<R: BufRead>(reader: R) -> Result<Vec<EmissionFactorRecord>> {
    parse(reader, "BSFC", false, Variant::Bsfc)
}

fn parse<R: BufRead>(
    reader: R,
    expected_pollutant: &str,
    enforce_mult_for_crank: bool,
    variant: Variant,
) -> Result<Vec<EmissionFactorRecord>> {
    let path = match variant {
        Variant::Emf => PathBuf::from(".EMF"),
        Variant::Bsfc => PathBuf::from(".BSF"),
    };
    let expected_upper = expected_pollutant.trim().to_ascii_uppercase();

    let mut records = Vec::new();
    let mut in_packet = false;
    let mut found_end = false;
    let mut line_num = 0usize;

    // Currently-active header block.
    let mut block: Option<HeaderBlock> = None;

    for line_result in reader.lines() {
        line_num += 1;
        let line = line_result.map_err(|e| Error::Io {
            path: path.clone(),
            source: e,
        })?;

        if !in_packet {
            if is_keyword(&line, "/EMSFAC/") {
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

        if !column(&line, 6, 15).trim().is_empty() {
            // Header line — new (SCC, HP-range) block.
            block = Some(parse_header(
                &line,
                line_num,
                &path,
                &expected_upper,
                enforce_mult_for_crank,
                variant,
            )?);
        } else {
            // Data line — needs an active block.
            let Some(block) = block.as_ref() else {
                return Err(Error::Parse {
                    file: path.clone(),
                    line: line_num,
                    message: "data record before any /EMSFAC/ header".to_string(),
                });
            };
            parse_data_line(&line, line_num, &path, block, &mut records)?;
        }
    }

    if !in_packet {
        return Err(Error::Parse {
            file: path,
            line: line_num,
            message: "missing /EMSFAC/ packet marker".to_string(),
        });
    }
    if !found_end {
        return Err(Error::Parse {
            file: path,
            line: line_num,
            message: "missing /END/ marker after /EMSFAC/ packet".to_string(),
        });
    }

    Ok(records)
}

#[derive(Debug, Clone)]
struct HeaderBlock {
    scc: String,
    hp_min: f32,
    hp_max: f32,
    tech_types: Vec<String>,
    units: EmissionUnits,
}

fn parse_header(
    line: &str,
    line_num: usize,
    path: &Path,
    expected_pollutant_upper: &str,
    enforce_mult_for_crank: bool,
    variant: Variant,
) -> Result<HeaderBlock> {
    let scc = column(line, 6, 15).trim().to_string();
    let hp_min = parse_f5(column(line, 21, 25), "hp_min", line_num, path)?;
    let hp_max = parse_f5(column(line, 26, 30), "hp_max", line_num, path)?;

    let mut tech_types: Vec<String> = Vec::new();
    let mut units: Option<EmissionUnits> = None;
    let mut start = 35usize;

    loop {
        let field = column(line, start, start + 9);
        if field.trim().is_empty() {
            // Fortran `rdemfc.f` :150 — for BSFC files, a blank
            // field at the current position means "no more tech
            // columns, proceed to units handling". The Fortran's
            // label 444 then advances `istrt += 10` before reading
            // the pollutant code, so we mirror that here.
            // For regular `.EMF`, this is an error (7005).
            if matches!(variant, Variant::Bsfc) {
                start += 10;
                break;
            }
            return Err(Error::Parse {
                file: path.to_path_buf(),
                line: line_num,
                message: format!("missing or invalid tech-type or units at col {}", start),
            });
        }

        let normalised = field.trim().to_ascii_uppercase();
        if let Some(u) = EmissionUnits::from_keyword(&normalised) {
            units = Some(u);
            start += 10;
            break;
        }

        if tech_types.len() >= crate::common::consts::MXTECH {
            return Err(Error::Parse {
                file: path.to_path_buf(),
                line: line_num,
                message: format!(
                    "more than {} tech types on header line",
                    crate::common::consts::MXTECH
                ),
            });
        }
        tech_types.push(normalised);
        start += 10;
    }

    // For BSFC, units may be absent if we broke on a blank
    // tech-type column. The Fortran leaves `idxunt`/`iuntmp` with
    // their previous (undefined) values; we default to `MULT`
    // because BSFC data is unitless in practice. Callers that care
    // can re-validate downstream.
    let units = units.unwrap_or(EmissionUnits::Multiplier);

    // Pollutant code, 10-char field at `start`, then `lftjst` +
    // `low2up` and compared against `polin`.
    let pol_field = column(line, start, start + 9);
    let pol_upper = pol_field.trim().to_ascii_uppercase();
    if pol_upper != expected_pollutant_upper {
        return Err(Error::Parse {
            file: path.to_path_buf(),
            line: line_num,
            message: format!(
                "expected pollutant {expected_pollutant_upper:?} on header line, got {pol_upper:?}"
            ),
        });
    }

    if enforce_mult_for_crank && units != EmissionUnits::Multiplier {
        return Err(Error::Parse {
            file: path.to_path_buf(),
            line: line_num,
            message: "crankcase emission factor must use MULT units".to_string(),
        });
    }

    Ok(HeaderBlock {
        scc,
        hp_min,
        hp_max,
        tech_types,
        units,
    })
}

fn parse_data_line(
    line: &str,
    line_num: usize,
    path: &Path,
    block: &HeaderBlock,
    out: &mut Vec<EmissionFactorRecord>,
) -> Result<()> {
    let year = parse_i5(column(line, 1, 5), "year", line_num, path)?;
    let mut start = 35usize;
    for tech in &block.tech_types {
        let field = column(line, start, start + 9);
        let factor = parse_f10(field, "factor", line_num, path)?;
        out.push(EmissionFactorRecord {
            scc: block.scc.clone(),
            tech_type: tech.clone(),
            hp_min: block.hp_min,
            hp_max: block.hp_max,
            year,
            units: block.units,
            factor,
        });
        start += 10;
    }
    Ok(())
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

fn parse_f5(field: &str, name: &str, line_num: usize, path: &Path) -> Result<f32> {
    parse_numeric(field, name, line_num, path)
}

fn parse_f10(field: &str, name: &str, line_num: usize, path: &Path) -> Result<f32> {
    parse_numeric(field, name, line_num, path)
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

#[cfg(test)]
mod tests {
    use super::*;

    // Helper: build a fixed-width line by placing strings/values at
    // specific 1-based columns. Padded with spaces.
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

    fn one_block_emf() -> String {
        let header = at(&[
            (6, "2270001000"),
            (21, " 25.0"),
            (26, " 50.0"),
            (35, "BASE      "),
            (45, "G/HP-HR   "),
            (55, "THC       "),
        ]);
        let data2000 = at(&[(1, " 2000"), (35, "      0.50")]);
        let data2010 = at(&[(1, " 2010"), (35, "      0.30")]);
        format!("/EMSFAC/\n{header}\n{data2000}\n{data2010}\n/END/\n")
    }

    #[test]
    fn parses_simple_block() {
        let body = one_block_emf();
        let records = read_emf(body.as_bytes(), "THC", false).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].scc, "2270001000");
        assert_eq!(records[0].tech_type, "BASE");
        assert!((records[0].hp_min - 25.0).abs() < 1e-6);
        assert!((records[0].hp_max - 50.0).abs() < 1e-6);
        assert_eq!(records[0].year, 2000);
        assert_eq!(records[0].units, EmissionUnits::GramsPerHpHour);
        assert!((records[0].factor - 0.50).abs() < 1e-6);
        assert_eq!(records[1].year, 2010);
        assert!((records[1].factor - 0.30).abs() < 1e-6);
    }

    #[test]
    fn parses_multi_tech_columns() {
        let header = at(&[
            (6, "2270001000"),
            (21, " 25.0"),
            (26, " 50.0"),
            (35, "BASE      "),
            (45, "T1        "),
            (55, "T2        "),
            (65, "G/HP-HR   "),
            (75, "THC       "),
        ]);
        let data = at(&[
            (1, " 2010"),
            (35, "      0.10"),
            (45, "      0.20"),
            (55, "      0.30"),
        ]);
        let body = format!("/EMSFAC/\n{header}\n{data}\n/END/\n");
        let records = read_emf(body.as_bytes(), "THC", false).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(
            records
                .iter()
                .map(|r| r.tech_type.as_str())
                .collect::<Vec<_>>(),
            vec!["BASE", "T1", "T2"]
        );
        let factors: Vec<f32> = records.iter().map(|r| r.factor).collect();
        assert!((factors[0] - 0.10).abs() < 1e-6);
        assert!((factors[1] - 0.20).abs() < 1e-6);
        assert!((factors[2] - 0.30).abs() < 1e-6);
    }

    #[test]
    fn pollutant_mismatch_errors() {
        let body = one_block_emf();
        let err = read_emf(body.as_bytes(), "CO", false).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("expected pollutant")),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn crankcase_requires_mult_units() {
        // Header advertises G/HR for a CRA pollutant — should fail
        // when the caller asks us to enforce the CRA→MULT rule.
        let header = at(&[
            (6, "2270001000"),
            (21, " 25.0"),
            (26, " 50.0"),
            (35, "BASE      "),
            (45, "G/HR      "),
            (55, "CRA       "),
        ]);
        let data = at(&[(1, " 2010"), (35, "      0.10")]);
        let body = format!("/EMSFAC/\n{header}\n{data}\n/END/\n");
        let err = read_emf(body.as_bytes(), "CRA", true).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("MULT")),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn crankcase_with_mult_passes() {
        let header = at(&[
            (6, "2270001000"),
            (21, " 25.0"),
            (26, " 50.0"),
            (35, "BASE      "),
            (45, "MULT      "),
            (55, "CRA       "),
        ]);
        let data = at(&[(1, " 2010"), (35, "      0.05")]);
        let body = format!("/EMSFAC/\n{header}\n{data}\n/END/\n");
        let records = read_emf(body.as_bytes(), "CRA", true).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].units, EmissionUnits::Multiplier);
    }

    #[test]
    fn missing_emsfac_marker_errors() {
        let body = "no marker here\n/END/\n";
        let err = read_emf(body.as_bytes(), "THC", false).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("/EMSFAC/")),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn missing_end_marker_errors() {
        let header = at(&[
            (6, "2270001000"),
            (21, " 25.0"),
            (26, " 50.0"),
            (35, "BASE      "),
            (45, "G/HR      "),
            (55, "THC       "),
        ]);
        let body = format!("/EMSFAC/\n{header}\n");
        let err = read_emf(body.as_bytes(), "THC", false).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("/END/")),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn bsfc_allows_blank_first_field() {
        // BSFC files put units directly at col 35 (no tech-type
        // column). The parser should accept this — emitting zero
        // tech-types means zero records per data line.
        let header = at(&[
            (6, "2270001000"),
            (21, " 25.0"),
            (26, " 50.0"),
            // col 35 left blank
            (45, "BSFC      "),
        ]);
        let data = at(&[(1, " 2010")]);
        let body = format!("/EMSFAC/\n{header}\n{data}\n/END/\n");
        // No tech-types -> no factor records produced.
        let records = read_bsfc(body.as_bytes()).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn bsfc_with_units_at_col_35() {
        // Conventional BSFC layout: units keyword at col 35, BSFC
        // pollutant at col 45, single F10.0 value per data line.
        let header = at(&[
            (6, "2270001000"),
            (21, " 25.0"),
            (26, " 50.0"),
            (35, "G/HP-HR   "),
            (45, "BSFC      "),
        ]);
        let data = at(&[(1, " 2010")]);
        let body = format!("/EMSFAC/\n{header}\n{data}\n/END/\n");
        // With no tech-type column, ntch=0 → no data records emitted.
        let records = read_bsfc(body.as_bytes()).unwrap();
        assert!(records.is_empty());
    }
}

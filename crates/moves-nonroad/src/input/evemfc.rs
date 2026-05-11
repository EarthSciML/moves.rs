//! Evaporative emission-factor parser (`rdevemfc.f`).
//!
//! Task 96. Parses an evap emission-factor file (`.EMF`) — same
//! syntax as the exhaust [`super::emfc`] reader, but with an extra
//! `G/M2/DAY` units keyword and per-pollutant unit constraints:
//!
//! - Diurnal (`IDXDIU`) — units must be `MULT`
//!   (`rdevemfc.f` :177).
//! - Tank Permeation (`IDXTKP`) — units must be `G/M2/DAY`
//!   (`rdevemfc.f` :178).
//! - Hose / Fill-neck / Supply-return / Vent permeation
//!   (`IDXHOS..IDXVNT`) — units must be `G/M2/DAY`
//!   (`rdevemfc.f` :179-180).
//!
//! The Rust port models the pollutant-kind policy as
//! [`EvapPollutantKind`] so the caller (the `efls` dispatcher) can
//! decide once which rule applies to each input pollutant.
//!
//! # Fortran source
//!
//! Ports `rdevemfc.f` (383 lines).

use std::io::BufRead;
use std::path::{Path, PathBuf};

use crate::{Error, Result};

/// Units keyword used in an evap `.EMF` header.
///
/// Same as [`super::emfc::EmissionUnits`] plus the
/// `G/M2/DAY` permeation unit (`IDXGMD` in `nonrdefc.inc`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvapEmissionUnits {
    /// `G/HR`.
    GramsPerHour,
    /// `G/HP-HR`.
    GramsPerHpHour,
    /// `G/GALLON`.
    GramsPerGallon,
    /// `G/TANK`.
    GramsPerTank,
    /// `G/DAY`.
    GramsPerDay,
    /// `G/START`.
    GramsPerStart,
    /// `MULT` — unitless multiplier (diurnal).
    Multiplier,
    /// `G/M2/DAY` — grams per square metre per day
    /// (permeation pollutants).
    GramsPerM2Day,
}

impl EvapEmissionUnits {
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
            "G/M2/DAY" => Some(Self::GramsPerM2Day),
            _ => None,
        }
    }
}

/// Evap-pollutant unit policy.
///
/// The Fortran checks the pollutant index against `IDXDIU`,
/// `IDXTKP`, and the `IDXHOS..IDXVNT` range. The Rust port models
/// those as named cases. Callers translate their input pollutant
/// (typically a string code) into the right [`EvapPollutantKind`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvapPollutantKind {
    /// Diurnal (IDXDIU) — units must be `MULT`.
    Diurnal,
    /// Tank permeation (IDXTKP) — units must be `G/M2/DAY`.
    TankPermeation,
    /// Hose / fill-neck / supply-return / vent permeation
    /// (IDXHOS..IDXVNT) — units must be `G/M2/DAY`.
    HosePermeation,
    /// No unit constraint (other evap pollutants such as hot soak,
    /// running loss, resting loss, displacement, spillage).
    Unconstrained,
}

impl EvapPollutantKind {
    fn required_units(self) -> Option<EvapEmissionUnits> {
        match self {
            Self::Diurnal => Some(EvapEmissionUnits::Multiplier),
            Self::TankPermeation | Self::HosePermeation => Some(EvapEmissionUnits::GramsPerM2Day),
            Self::Unconstrained => None,
        }
    }
}

/// One evap emission-factor record.
#[derive(Debug, Clone, PartialEq)]
pub struct EvapEmissionFactorRecord {
    /// SCC code.
    pub scc: String,
    /// Evap technology-type code (10 chars; see the `E00000000`
    /// encoding documented at `rdevtech.f` :117-134).
    pub tech_type: String,
    /// Horsepower-range minimum.
    pub hp_min: f32,
    /// Horsepower-range maximum.
    pub hp_max: f32,
    /// Model year.
    pub year: i32,
    /// Units keyword.
    pub units: EvapEmissionUnits,
    /// Emission-factor value.
    pub factor: f32,
}

/// Parse an evap `.EMF` (`/EMSFAC/`) packet.
///
/// `expected_pollutant` must match the pollutant code announced
/// in each header line (case-insensitive, trim-aware).
/// `pollutant_kind` selects which unit constraint applies.
pub fn read_evemf<R: BufRead>(
    reader: R,
    expected_pollutant: &str,
    pollutant_kind: EvapPollutantKind,
) -> Result<Vec<EvapEmissionFactorRecord>> {
    let path = PathBuf::from(".EMF");
    let expected_upper = expected_pollutant.trim().to_ascii_uppercase();
    let required_units = pollutant_kind.required_units();

    let mut records = Vec::new();
    let mut in_packet = false;
    let mut found_end = false;
    let mut line_num = 0usize;
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
            block = Some(parse_header(
                &line,
                line_num,
                &path,
                &expected_upper,
                required_units,
                pollutant_kind,
            )?);
        } else {
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
    units: EvapEmissionUnits,
}

fn parse_header(
    line: &str,
    line_num: usize,
    path: &Path,
    expected_pollutant_upper: &str,
    required_units: Option<EvapEmissionUnits>,
    pollutant_kind: EvapPollutantKind,
) -> Result<HeaderBlock> {
    let scc = column(line, 6, 15).trim().to_string();
    let hp_min = parse_f5(column(line, 21, 25), "hp_min", line_num, path)?;
    let hp_max = parse_f5(column(line, 26, 30), "hp_max", line_num, path)?;

    let mut tech_types: Vec<String> = Vec::new();
    let mut start = 35usize;

    let units = loop {
        let field = column(line, start, start + 9);
        if field.trim().is_empty() {
            return Err(Error::Parse {
                file: path.to_path_buf(),
                line: line_num,
                message: format!("missing or invalid tech-type or units at col {start}"),
            });
        }

        let normalised = field.trim().to_ascii_uppercase();
        if let Some(u) = EvapEmissionUnits::from_keyword(&normalised) {
            start += 10;
            break u;
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
    };

    if let Some(required) = required_units {
        if units != required {
            return Err(Error::Parse {
                file: path.to_path_buf(),
                line: line_num,
                message: format!(
                    "{pollutant_kind:?} pollutant requires units {required:?}, got {units:?}"
                ),
            });
        }
    }

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
    out: &mut Vec<EvapEmissionFactorRecord>,
) -> Result<()> {
    let year = parse_i5(column(line, 1, 5), "year", line_num, path)?;
    let mut start = 35usize;
    for tech in &block.tech_types {
        let field = column(line, start, start + 9);
        let factor = parse_f10(field, "factor", line_num, path)?;
        out.push(EvapEmissionFactorRecord {
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
    fn parses_unconstrained_pollutant() {
        let header = at(&[
            (6, "2270001000"),
            (21, " 25.0"),
            (26, " 50.0"),
            (35, "E00000000 "),
            (45, "G/DAY     "),
            (55, "HSK       "),
        ]);
        let data = at(&[(1, " 2010"), (35, "      0.75")]);
        let body = format!("/EMSFAC/\n{header}\n{data}\n/END/\n");
        let records = read_evemf(body.as_bytes(), "HSK", EvapPollutantKind::Unconstrained).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].tech_type, "E00000000");
        assert_eq!(records[0].units, EvapEmissionUnits::GramsPerDay);
    }

    #[test]
    fn diurnal_requires_mult() {
        // Diurnal pollutant with G/DAY → rejected.
        let header = at(&[
            (6, "2270001000"),
            (21, " 25.0"),
            (26, " 50.0"),
            (35, "E00000000 "),
            (45, "G/DAY     "),
            (55, "DIU       "),
        ]);
        let data = at(&[(1, " 2010"), (35, "      0.10")]);
        let body = format!("/EMSFAC/\n{header}\n{data}\n/END/\n");
        let err = read_evemf(body.as_bytes(), "DIU", EvapPollutantKind::Diurnal).unwrap_err();
        match err {
            Error::Parse { message, .. } => {
                assert!(message.contains("Diurnal") || message.contains("MULT"));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn diurnal_with_mult_passes() {
        let header = at(&[
            (6, "2270001000"),
            (21, " 25.0"),
            (26, " 50.0"),
            (35, "E00000000 "),
            (45, "MULT      "),
            (55, "DIU       "),
        ]);
        let data = at(&[(1, " 2010"), (35, "      0.10")]);
        let body = format!("/EMSFAC/\n{header}\n{data}\n/END/\n");
        let records = read_evemf(body.as_bytes(), "DIU", EvapPollutantKind::Diurnal).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].units, EvapEmissionUnits::Multiplier);
    }

    #[test]
    fn tank_permeation_requires_gmd() {
        let header = at(&[
            (6, "2270001000"),
            (21, " 25.0"),
            (26, " 50.0"),
            (35, "E00000000 "),
            (45, "MULT      "),
            (55, "TKP       "),
        ]);
        let data = at(&[(1, " 2010"), (35, "      0.10")]);
        let body = format!("/EMSFAC/\n{header}\n{data}\n/END/\n");
        let err =
            read_evemf(body.as_bytes(), "TKP", EvapPollutantKind::TankPermeation).unwrap_err();
        match err {
            Error::Parse { message, .. } => {
                assert!(message.contains("G/M2/DAY") || message.contains("TankPermeation"));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn hose_permeation_with_gmd_passes() {
        let header = at(&[
            (6, "2270001000"),
            (21, " 25.0"),
            (26, " 50.0"),
            (35, "E00000000 "),
            (45, "G/M2/DAY  "),
            (55, "HOS       "),
        ]);
        let data = at(&[(1, " 2010"), (35, "      0.30")]);
        let body = format!("/EMSFAC/\n{header}\n{data}\n/END/\n");
        let records =
            read_evemf(body.as_bytes(), "HOS", EvapPollutantKind::HosePermeation).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].units, EvapEmissionUnits::GramsPerM2Day);
    }
}

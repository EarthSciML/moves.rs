//! Spillage / permeation parser (`rdspil.f`).
//!
//! Parses the `/EMSFAC/` packet from a spillage file (e.g. `SPILLAGE.EMF`).
//! Each record carries refueling-mode and range indicators, HP range, tech
//! type, units, tank/hose/fill-neck/supply-return/vent dimensions,
//! hot-soak starts-per-hour, five diurnal fractions, and E10 base-case
//! adjustment factors.
//!
//! # Fixed-width column layout
//!
//! Matches `rdspil.f` column positions exactly (Fortran 1-based → Rust
//! 0-based inclusive end):
//!
//! | Field | Fortran | Rust \[start..end) |
//! |---|---|---|
//! | SCC code | 1–10 | \[0..10) |
//! | Equipment name (skipped) | 12–51 | skipped |
//! | Fill method (`PUMP`/`CONTAINER`) | 54–62 | \[53..62) |
//! | Indicator (`HP`/`TANK`) | 64–67 | \[63..67) |
//! | HP range min | 69–73 | \[68..73) |
//! | HP range max | 74–78 | \[73..78) |
//! | Tech type | 79–88 | \[78..88) |
//! | Units (`GALLONS`/`GAL/HP`) | 90–99 | \[89..99) |
//! | Tank volume | 103–112 | \[102..112) |
//! | Tank fullness fraction | 113–120 | \[112..120) |
//! | Tank metal fraction | 121–130 | \[120..130) |
//! | Non-RM hose length | 131–140 | \[130..140) |
//! | Non-RM hose diameter | 141–150 | \[140..150) |
//! | Non-RM hose metal fraction | 151–160 | \[150..160) |
//! | RM fill-neck length | 161–170 | \[160..170) |
//! | RM fill-neck diameter | 171–180 | \[170..180) |
//! | RM supply/return length | 181–190 | \[180..190) |
//! | RM supply/return diameter | 191–200 | \[190..200) |
//! | RM vent length | 201–210 | \[200..210) |
//! | RM vent diameter | 211–220 | \[210..220) |
//! | Hot-soak starts/hr | 221–230 | \[220..230) |
//! | Diurnal fractions ×5 | 231–280 | \[230..280) |
//! | Tank E10 factor | 281–290 | \[280..290) |
//! | Hose E10 factor | 291–300 | \[290..300) |
//! | Fill-neck E10 factor | 301–310 | \[300..310) |
//! | Supply/return E10 factor | 311–320 | \[310..320) |
//! | Vent E10 factor | 321–330 | \[320..330) |
//!
//! E10 factors default to `1.0` when read as `0.0` (matches `rdspil.f`).
//!
//! # Fortran source
//!
//! Ports `rdspil.f` (311 lines).

use crate::{Error, Result};
use std::io::BufRead;
use std::path::{Path, PathBuf};

/// Refueling mode (Fortran `PUMP = "PUMP     "`, `CNTR = "CONTAINER"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefuelingMode {
    /// Pump refueling.
    Pump,
    /// Container (portable-can) refueling.
    Container,
}

/// Range indicator (Fortran `HP = "HP  "`, `TNKTYP = "TANK"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeIndicator {
    /// Tank-volume ranges (`TANK`).
    Tank,
    /// Horsepower ranges (`HP`).
    Horsepower,
}

/// Spillage volume units (Fortran `GALLON = "GALLONS   "`, `GALHP = "GAL/HP    "`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillageUnits {
    /// Gallons.
    Gallons,
    /// Gallons per horsepower.
    GallonsPerHp,
}

/// One `/EMSFAC/` spillage record (`nsplar` entry in Fortran COMMON).
#[derive(Debug, Clone, PartialEq)]
pub struct SpillageRecord {
    /// SCC code (`ascspl`).
    pub scc: String,
    /// Refueling mode (`modspl`).
    pub mode: RefuelingMode,
    /// HP/tank range indicator (`indspl`).
    pub indicator: RangeIndicator,
    /// HP range minimum (`splpcb`).
    pub hp_min: f32,
    /// HP range maximum (`splpce`).
    pub hp_max: f32,
    /// Technology type (`tecspl`).
    pub tech_type: String,
    /// Volume units (`untspl`).
    pub units: SpillageUnits,
    /// Tank volume in gallons or gal/HP (`volspl`).
    pub tank_volume: f32,
    /// Fraction of tank filled with fuel (`tnkful`).
    pub tank_full: f32,
    /// Fraction of tank that is metal (`tnkmtl`).
    pub tank_metal_pct: f32,
    /// Non-rec-marine hose length in metres (`hoslen`).
    pub hose_len: f32,
    /// Non-rec-marine hose diameter in metres (`hosdia`).
    pub hose_dia: f32,
    /// Fraction of hose that is metal (`hosmtl`).
    pub hose_metal_pct: f32,
    /// Rec-marine fill-neck length in metres (`ncklen`).
    pub neck_len: f32,
    /// Rec-marine fill-neck diameter in metres (`nckdia`).
    pub neck_dia: f32,
    /// Rec-marine supply/return hose length (`srlen`).
    pub sr_len: f32,
    /// Rec-marine supply/return hose diameter (`srdia`).
    pub sr_dia: f32,
    /// Rec-marine vent hose length (`vntlen`).
    pub vent_len: f32,
    /// Rec-marine vent hose diameter (`vntdia`).
    pub vent_dia: f32,
    /// Hot-soak starts per hour of operation (`hssph`).
    pub hot_soak_per_hr: f32,
    /// Diurnal fractions (5 values: portable-plastic, RM-plastic-trailer,
    /// RM-plastic-water, RM-metal-trailer, RM-metal-water) (`diufrc`).
    pub diurnal: [f32; 5],
    /// Tank E10 permeation adjustment factor (`tnke10`; defaults 1.0 when 0).
    pub tank_e10: f32,
    /// Non-RM hose E10 permeation adjustment factor (`hose10`).
    pub hose_e10: f32,
    /// RM fill-neck E10 permeation adjustment factor (`ncke10`).
    pub neck_e10: f32,
    /// RM supply/return E10 permeation adjustment factor (`sre10`).
    pub sr_e10: f32,
    /// RM vent E10 permeation adjustment factor (`vnte10`).
    pub vent_e10: f32,
}

/// Parse a spillage file, returning one [`SpillageRecord`] per `/EMSFAC/` data row.
///
/// Lines outside the `/EMSFAC/` … `/END/` packet, blank lines, and lines
/// starting with `#` are silently skipped, matching `rdspil.f` behaviour.
pub fn read_spil<R: BufRead>(reader: R) -> Result<Vec<SpillageRecord>> {
    let mut records = Vec::new();
    let path = PathBuf::from(".SPL");
    let mut in_packet = false;
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
        if upper.starts_with("/EMSFAC/") {
            in_packet = true;
            continue;
        }
        if upper.starts_with("/END/") {
            in_packet = false;
            continue;
        }
        if !in_packet {
            continue;
        }

        let bytes = line.as_bytes();

        let mode_str = col(bytes, 53, 62).to_ascii_uppercase();
        let mode = match mode_str.as_str() {
            "PUMP" => RefuelingMode::Pump,
            "CONTAINER" => RefuelingMode::Container,
            other => {
                return Err(Error::Parse {
                    file: path.clone(),
                    line: line_num,
                    message: format!("invalid refueling mode: {other}"),
                });
            }
        };

        let ind_str = col(bytes, 63, 67).to_ascii_uppercase();
        let indicator = match ind_str.as_str() {
            "TANK" => RangeIndicator::Tank,
            "HP" => RangeIndicator::Horsepower,
            other => {
                return Err(Error::Parse {
                    file: path.clone(),
                    line: line_num,
                    message: format!("invalid range indicator: {other}"),
                });
            }
        };

        let units_str = col(bytes, 89, 99).to_ascii_uppercase();
        let units = match units_str.as_str() {
            "GALLONS" => SpillageUnits::Gallons,
            "GAL/HP" => SpillageUnits::GallonsPerHp,
            other => {
                return Err(Error::Parse {
                    file: path.clone(),
                    line: line_num,
                    message: format!("invalid spillage units: {other}"),
                });
            }
        };

        // E10 factors at end of line — default to 1.0 when zero (matches rdspil.f).
        let e10 = |s: usize, e: usize, name: &str| -> Result<f32> {
            let v = parse_col(bytes, s, e, name, line_num, &path)?;
            Ok(if v == 0.0 { 1.0 } else { v })
        };

        records.push(SpillageRecord {
            scc: col(bytes, 0, 10).to_string(),
            mode,
            indicator,
            hp_min: parse_col(bytes, 68, 73, "hp_min", line_num, &path)?,
            hp_max: parse_col(bytes, 73, 78, "hp_max", line_num, &path)?,
            tech_type: col(bytes, 78, 88).to_ascii_uppercase(),
            units,
            tank_volume: parse_col(bytes, 102, 112, "tank_volume", line_num, &path)?,
            tank_full: parse_col(bytes, 112, 120, "tank_full", line_num, &path)?,
            tank_metal_pct: parse_col(bytes, 120, 130, "tank_metal_pct", line_num, &path)?,
            hose_len: parse_col(bytes, 130, 140, "hose_len", line_num, &path)?,
            hose_dia: parse_col(bytes, 140, 150, "hose_dia", line_num, &path)?,
            hose_metal_pct: parse_col(bytes, 150, 160, "hose_metal_pct", line_num, &path)?,
            neck_len: parse_col(bytes, 160, 170, "neck_len", line_num, &path)?,
            neck_dia: parse_col(bytes, 170, 180, "neck_dia", line_num, &path)?,
            sr_len: parse_col(bytes, 180, 190, "sr_len", line_num, &path)?,
            sr_dia: parse_col(bytes, 190, 200, "sr_dia", line_num, &path)?,
            vent_len: parse_col(bytes, 200, 210, "vent_len", line_num, &path)?,
            vent_dia: parse_col(bytes, 210, 220, "vent_dia", line_num, &path)?,
            hot_soak_per_hr: parse_col(bytes, 220, 230, "hot_soak_per_hr", line_num, &path)?,
            diurnal: [
                parse_col(bytes, 230, 240, "diurnal[0]", line_num, &path)?,
                parse_col(bytes, 240, 250, "diurnal[1]", line_num, &path)?,
                parse_col(bytes, 250, 260, "diurnal[2]", line_num, &path)?,
                parse_col(bytes, 260, 270, "diurnal[3]", line_num, &path)?,
                parse_col(bytes, 270, 280, "diurnal[4]", line_num, &path)?,
            ],
            tank_e10: e10(280, 290, "tank_e10")?,
            hose_e10: e10(290, 300, "hose_e10")?,
            neck_e10: e10(300, 310, "neck_e10")?,
            sr_e10: e10(310, 320, "sr_e10")?,
            vent_e10: e10(320, 330, "vent_e10")?,
        });
    }

    Ok(records)
}

/// Extract a fixed-width field from `bytes[start..end)`, trim whitespace.
fn col(bytes: &[u8], start: usize, end: usize) -> &str {
    let end = end.min(bytes.len());
    if start >= bytes.len() {
        return "";
    }
    std::str::from_utf8(&bytes[start..end])
        .unwrap_or("")
        .trim()
}

/// Parse a fixed-width column as `f32`, returning a parse error on failure.
fn parse_col(
    bytes: &[u8],
    start: usize,
    end: usize,
    name: &str,
    line_num: usize,
    path: &Path,
) -> Result<f32> {
    let token = col(bytes, start, end);
    token.parse().map_err(|_| Error::Parse {
        file: path.to_path_buf(),
        line: line_num,
        message: format!("invalid {name}: {token:?}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // First data record from SPILLAGE.EMF (330 chars, fixed-width).
    const SAMPLE_LINE: &str =
        "2260001010 2-Str Offroad Motorcycles                 CONTAINER HP       0 9999    ALL    GALLONS         3.00000 0.50000   0.00000   0.45750  0.006354   0.00000   0.00000  0.000000   0.00000  0.000000   0.00000  0.000000   0.05000     1.000     0.000     0.000     0.000     0.000     1.000     1.000     0.000     0.000     0.000";

    #[test]
    fn parses_emsfac_packet() {
        let input = format!("/EMSFAC/\n{SAMPLE_LINE}\n/END/\n");
        let records = read_spil(input.as_bytes()).unwrap();
        assert_eq!(records.len(), 1);
        let r = &records[0];
        assert_eq!(r.scc, "2260001010");
        assert_eq!(r.mode, RefuelingMode::Container);
        assert_eq!(r.indicator, RangeIndicator::Horsepower);
        assert_eq!(r.tech_type, "ALL");
        assert_eq!(r.units, SpillageUnits::Gallons);
        assert!((r.hp_min - 0.0).abs() < 1e-6);
        assert!((r.hp_max - 9999.0).abs() < 1e-6);
        assert!((r.tank_volume - 3.0).abs() < 1e-4);
        assert!((r.tank_full - 0.5).abs() < 1e-6);
        assert!((r.hose_len - 0.45750).abs() < 1e-5);
        assert!((r.hot_soak_per_hr - 0.05).abs() < 1e-6);
        assert!((r.diurnal[0] - 1.0).abs() < 1e-6);
        // tank_e10 is 1.0 in the file (non-zero, kept)
        assert!((r.tank_e10 - 1.0).abs() < 1e-6);
        // neck_e10 is 0.0 in the file → defaults to 1.0
        assert!((r.neck_e10 - 1.0).abs() < 1e-6);
    }

    #[test]
    fn rejects_invalid_mode() {
        // Replace "CONTAINER" (bytes 53-61) with "BLEHBLEHB"
        let bad: String = SAMPLE_LINE
            .chars()
            .enumerate()
            .map(|(i, c)| if (53..62).contains(&i) { "BLEHBLEHB".chars().nth(i - 53).unwrap_or(' ') } else { c })
            .collect();
        let input = format!("/EMSFAC/\n{bad}\n/END/\n");
        let err = read_spil(input.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("refueling mode")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn skips_lines_outside_packet() {
        let input = "some header\n2260001010 ignored line\n/EMSFAC/\n";
        let records = read_spil(input.as_bytes()).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn reads_multiple_records() {
        let line2 =
            "2260001020 2-Str Snowmobiles                         PUMP      HP       1  175    ALL    GALLONS        11.00000 0.50000   0.00000   1.06750  0.006354   0.00000   0.00000  0.000000   0.00000  0.000000   0.00000  0.000000   1.00000     1.000     0.000     0.000     0.000     0.000     1.000     1.000     0.000     0.000     0.000";
        let input = format!("/EMSFAC/\n{SAMPLE_LINE}\n{line2}\n/END/\n");
        let records = read_spil(input.as_bytes()).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].scc, "2260001010");
        assert_eq!(records[1].scc, "2260001020");
        assert_eq!(records[1].mode, RefuelingMode::Pump);
        assert!((records[1].hp_min - 1.0).abs() < 1e-6);
        assert!((records[1].hp_max - 175.0).abs() < 1e-6);
    }
}

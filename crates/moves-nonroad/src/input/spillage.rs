//! Spillage / permeation parser (`rdspil.f`).
//!
//! Task 97. Parses the `/EMSFAC/` packet from a spillage file. Each
//! record carries refueling-mode and tank-type indicators, HP range,
//! tech type, units, tank/hose/fill-neck/supply-return/vent
//! dimensions, hot-soak starts-per-hour, five diurnal fractions, and
//! E10 base-case adjustment factors.
//!
//! # Format (whitespace-delimited summary)
//!
//! ```text
//! /EMSFAC/
//! <scc> <mode> <indicator> <hp_min> <hp_max> <tech> <units> \
//!     <tank_vol> <tank_full> <tank_metal_pct> \
//!     <hose_len> <hose_dia> <hose_metal_pct> \
//!     <neck_len> <neck_dia> <sr_len> <sr_dia> <vent_len> <vent_dia> \
//!     <hot_soak_per_hr> <diu1> <diu2> <diu3> <diu4> <diu5> \
//!     <tnk_e10> <hose_e10> <neck_e10> <sr_e10> <vent_e10>
//! ...
//! /END/
//! ```
//!
//! Refueling-mode is `PUMP` or `CNTR`; indicator is `TNK` or `HP`.
//! Units must be `GALLONS` or `GAL/HP`.
//!
//! # Fortran source
//!
//! Ports `rdspil.f` (311 lines).

use crate::{Error, Result};
use std::io::BufRead;
use std::path::PathBuf;

/// Refueling mode (Fortran constants `PUMP` and `CNTR`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefuelingMode {
    /// Pump refueling.
    Pump,
    /// Container refueling.
    Container,
}

/// Range indicator (Fortran constants `TNK` and `HP`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeIndicator {
    /// HP range applies to tank volume buckets (`TNK`).
    Tank,
    /// HP range applies to engine horsepower (`HP`).
    Horsepower,
}

/// Spillage units (`GALLONS` or `GAL/HP`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillageUnits {
    /// Gallons.
    Gallons,
    /// Gallons per horsepower.
    GallonsPerHp,
}

/// One `/EMSFAC/` spillage record.
#[derive(Debug, Clone, PartialEq)]
pub struct SpillageRecord {
    /// SCC code.
    pub scc: String,
    /// Refueling mode.
    pub mode: RefuelingMode,
    /// HP/Tank range indicator.
    pub indicator: RangeIndicator,
    /// HP range minimum.
    pub hp_min: f32,
    /// HP range maximum.
    pub hp_max: f32,
    /// Technology type identifier.
    pub tech_type: String,
    /// Units indicator.
    pub units: SpillageUnits,
    /// Tank volume.
    pub tank_volume: f32,
    /// Tank fullness fraction.
    pub tank_full: f32,
    /// Percentage of tank that is metal.
    pub tank_metal_pct: f32,
    /// Non-rec-marine hose length.
    pub hose_len: f32,
    /// Non-rec-marine hose diameter.
    pub hose_dia: f32,
    /// Percentage of hose that is metal.
    pub hose_metal_pct: f32,
    /// Rec-marine fill-neck length.
    pub neck_len: f32,
    /// Rec-marine fill-neck diameter.
    pub neck_dia: f32,
    /// Rec-marine supply/return length.
    pub sr_len: f32,
    /// Rec-marine supply/return diameter.
    pub sr_dia: f32,
    /// Rec-marine vent length.
    pub vent_len: f32,
    /// Rec-marine vent diameter.
    pub vent_dia: f32,
    /// Hot-soak starts per hour.
    pub hot_soak_per_hr: f32,
    /// Diurnal fractions (5 values).
    pub diurnal: [f32; 5],
    /// Tank E10 adjustment factor (defaults 1.0 if missing/zero).
    pub tank_e10: f32,
    /// Hose E10 adjustment factor.
    pub hose_e10: f32,
    /// Fill-neck E10 adjustment factor.
    pub neck_e10: f32,
    /// Supply/return E10 adjustment factor.
    pub sr_e10: f32,
    /// Vent E10 adjustment factor.
    pub vent_e10: f32,
}

/// Parse a spillage file.
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

        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        if parts.len() < 30 {
            return Err(Error::Parse {
                file: path.clone(),
                line: line_num,
                message: format!(
                    "expected 30 fields in /EMSFAC/ record, got {}",
                    parts.len()
                ),
            });
        }
        let mode = match parts[1].to_ascii_uppercase().as_str() {
            "PUMP" => RefuelingMode::Pump,
            "CNTR" => RefuelingMode::Container,
            other => {
                return Err(Error::Parse {
                    file: path.clone(),
                    line: line_num,
                    message: format!("invalid refueling mode: {}", other),
                });
            }
        };
        let indicator = match parts[2].to_ascii_uppercase().as_str() {
            "TNK" => RangeIndicator::Tank,
            "HP" => RangeIndicator::Horsepower,
            other => {
                return Err(Error::Parse {
                    file: path.clone(),
                    line: line_num,
                    message: format!("invalid range indicator: {}", other),
                });
            }
        };
        let units = match parts[6].to_ascii_uppercase().as_str() {
            "GALLONS" => SpillageUnits::Gallons,
            "GAL/HP" => SpillageUnits::GallonsPerHp,
            other => {
                return Err(Error::Parse {
                    file: path.clone(),
                    line: line_num,
                    message: format!("invalid spillage units: {}", other),
                });
            }
        };
        // E10 factors default to 1.0 when zero (matches rdspil.f).
        let mut e10 = [0f32; 5];
        for (slot, raw) in e10.iter_mut().zip(parts[25..30].iter()) {
            let v: f32 = parse_f32(raw, "e10", line_num, &path)?;
            *slot = if v == 0.0 { 1.0 } else { v };
        }

        records.push(SpillageRecord {
            scc: parts[0].to_string(),
            mode,
            indicator,
            hp_min: parse_f32(parts[3], "hp_min", line_num, &path)?,
            hp_max: parse_f32(parts[4], "hp_max", line_num, &path)?,
            tech_type: parts[5].to_ascii_uppercase(),
            units,
            tank_volume: parse_f32(parts[7], "tank_volume", line_num, &path)?,
            tank_full: parse_f32(parts[8], "tank_full", line_num, &path)?,
            tank_metal_pct: parse_f32(parts[9], "tank_metal_pct", line_num, &path)?,
            hose_len: parse_f32(parts[10], "hose_len", line_num, &path)?,
            hose_dia: parse_f32(parts[11], "hose_dia", line_num, &path)?,
            hose_metal_pct: parse_f32(parts[12], "hose_metal_pct", line_num, &path)?,
            neck_len: parse_f32(parts[13], "neck_len", line_num, &path)?,
            neck_dia: parse_f32(parts[14], "neck_dia", line_num, &path)?,
            sr_len: parse_f32(parts[15], "sr_len", line_num, &path)?,
            sr_dia: parse_f32(parts[16], "sr_dia", line_num, &path)?,
            vent_len: parse_f32(parts[17], "vent_len", line_num, &path)?,
            vent_dia: parse_f32(parts[18], "vent_dia", line_num, &path)?,
            hot_soak_per_hr: parse_f32(parts[19], "hot_soak_per_hr", line_num, &path)?,
            diurnal: [
                parse_f32(parts[20], "diurnal[0]", line_num, &path)?,
                parse_f32(parts[21], "diurnal[1]", line_num, &path)?,
                parse_f32(parts[22], "diurnal[2]", line_num, &path)?,
                parse_f32(parts[23], "diurnal[3]", line_num, &path)?,
                parse_f32(parts[24], "diurnal[4]", line_num, &path)?,
            ],
            tank_e10: e10[0],
            hose_e10: e10[1],
            neck_e10: e10[2],
            sr_e10: e10[3],
            vent_e10: e10[4],
        });
    }

    Ok(records)
}

fn parse_f32(token: &str, name: &str, line_num: usize, path: &std::path::Path) -> Result<f32> {
    token.parse().map_err(|_| Error::Parse {
        file: path.to_path_buf(),
        line: line_num,
        message: format!("invalid {}: {}", name, token),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_record() -> &'static str {
        // 30 fields: scc mode ind hp_min hp_max tech units
        //            vol full mtl  hose_len hose_dia hose_mtl  neck_len neck_dia
        //            sr_len sr_dia  vent_len vent_dia  hot_soak  d1 d2 d3 d4 d5
        //            tnk_e10 hose_e10 neck_e10 sr_e10 vent_e10
        "2270001000 PUMP TNK 0.0 25.0 BASE GALLONS \
0.5 0.6 70.0 1.0 0.5 50.0 0.0 0.0 \
0.0 0.0 0.0 0.0 0.05 0.2 0.2 0.2 0.2 0.2 \
0.0 1.5 1.0 1.0 1.0"
    }

    #[test]
    fn parses_emsfac_packet() {
        let input = format!("/EMSFAC/\n{}\n/END/\n", sample_record());
        let records = read_spil(input.as_bytes()).unwrap();
        assert_eq!(records.len(), 1);
        let r = &records[0];
        assert_eq!(r.mode, RefuelingMode::Pump);
        assert_eq!(r.indicator, RangeIndicator::Tank);
        assert_eq!(r.units, SpillageUnits::Gallons);
        // tank_e10 was 0.0 in input, defaults to 1.0
        assert!((r.tank_e10 - 1.0).abs() < 1e-6);
        // hose_e10 was 1.5 (non-zero), kept
        assert!((r.hose_e10 - 1.5).abs() < 1e-6);
    }

    #[test]
    fn rejects_invalid_mode() {
        let bad = sample_record().replace("PUMP", "BLEH");
        let input = format!("/EMSFAC/\n{}\n/END/\n", bad);
        let err = read_spil(input.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("refueling mode")),
            other => panic!("unexpected error: {other:?}"),
        }
    }
}

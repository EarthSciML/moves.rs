//! `/OPTIONS/` packet parser (`rdnropt.f`).
//!
//! Task 97. Parses the main `/OPTIONS/` packet from the options file:
//! titles, fuel parameters (RVP, oxygen, sulfur fractions for
//! gasoline / land diesel / marine diesel / CNG-LPG), temperature
//! window (min/max/mean), altitude flag, and optional ethanol-blend
//! market share + vol-percent records.
//!
//! Records are line-oriented; each begins with a label (column 1–20)
//! followed by a value (column 21+). The Rust parser accepts either
//! that fixed-column layout or `key: value` form.
//!
//! Three records are conditional:
//!
//! - the marine-diesel sulfur record appears only when its label
//!   starts with `MARINE`,
//! - the two ethanol records appear only when the first label starts
//!   with `ETOH`.
//!
//! # Fortran source
//!
//! Ports `rdnropt.f` (438 lines).

use crate::{Error, Result};
use std::io::BufRead;
use std::path::PathBuf;

/// Altitude scope flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AltitudeFlag {
    /// `HIGH` — high-altitude run.
    High,
    /// `LOW` — low-altitude run.
    Low,
}

/// Parsed `/OPTIONS/` packet.
#[derive(Debug, Clone, PartialEq)]
pub struct OptionsConfig {
    /// First descriptive title.
    pub title1: String,
    /// Second descriptive title.
    pub title2: String,
    /// Fuel RVP in psi (`6..=16`).
    pub fuel_rvp: f32,
    /// Fuel oxygen weight percent (`0..=5`).
    pub oxygen_pct: f32,
    /// Sulfur fraction for gasoline (`0..=0.5`).
    pub sulfur_gasoline: f32,
    /// Sulfur fraction for land-based diesel (`0..=0.5`).
    pub sulfur_diesel_land: f32,
    /// Sulfur fraction for marine diesel (`0..=0.5`); defaults to
    /// `sulfur_diesel_land` when the optional record is absent.
    pub sulfur_diesel_marine: f32,
    /// Sulfur fraction for CNG/LPG (`0..=0.5`).
    pub sulfur_cng: f32,
    /// Minimum daily temperature in °F (`-40..=120`).
    pub temp_min: f32,
    /// Maximum daily temperature in °F (`-40..=120`).
    pub temp_max: f32,
    /// Representative daily ambient temperature (within `[temp_min, temp_max]`).
    pub temp_mean: f32,
    /// Altitude flag.
    pub altitude: AltitudeFlag,
    /// Ethanol market share (`0..=100`); `None` if the optional record is absent.
    pub ethanol_market_share: Option<f32>,
    /// Ethanol volume percent (`0..=100`); `None` when the record is absent.
    pub ethanol_vol_pct: Option<f32>,
}

/// Parse an `/OPTIONS/` packet.
pub fn read_options<R: BufRead>(reader: R) -> Result<OptionsConfig> {
    let path = PathBuf::from(".OPT");
    let mut in_packet = false;
    let mut entries: Vec<(String, String, usize)> = Vec::new();
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
        if upper.starts_with("/OPTIONS/") {
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
        let (label, value) = split_label_value(&line);
        entries.push((label, value, line_num));
    }

    if entries.is_empty() {
        return Err(Error::Parse {
            file: path,
            line: line_num,
            message: "missing /OPTIONS/ packet".to_string(),
        });
    }

    let mut iter = entries.into_iter();
    let title1 = next_value(&mut iter, &path, line_num)?;
    let title2 = next_value(&mut iter, &path, line_num)?;
    let rvp = next_f32(&mut iter, &path, line_num)?;
    range_check("fuel_rvp", rvp, 6.0, 16.0, line_num, &path)?;
    let oxy = next_f32(&mut iter, &path, line_num)?;
    range_check("oxygen_pct", oxy, 0.0, 5.0, line_num, &path)?;
    let sox_gas = next_f32(&mut iter, &path, line_num)?;
    range_check("sulfur_gasoline", sox_gas, 0.0, 0.5, line_num, &path)?;
    let sox_dsl = next_f32(&mut iter, &path, line_num)?;
    range_check("sulfur_diesel_land", sox_dsl, 0.0, 0.5, line_num, &path)?;

    let mut peeked: Option<(String, String, usize)> = iter.next();
    let mut sox_dsm = sox_dsl;
    if let Some((ref label, ref value, ref l)) = peeked {
        if label.to_ascii_uppercase().starts_with("MARINE") {
            let v: f32 = value.trim().parse().map_err(|_| Error::Parse {
                file: path.clone(),
                line: *l,
                message: format!("invalid marine diesel sulfur: {}", value),
            })?;
            range_check("sulfur_diesel_marine", v, 0.0, 0.5, *l, &path)?;
            sox_dsm = v;
            peeked = iter.next();
        }
    }
    let sox_cng = match peeked {
        Some((_, v, l)) => parse_required_f32(&v, "sulfur_cng", l, &path)?,
        None => return Err(missing_record(&path, "sulfur_cng", line_num)),
    };
    range_check("sulfur_cng", sox_cng, 0.0, 0.5, line_num, &path)?;

    let temp_mn = next_f32(&mut iter, &path, line_num)?;
    range_check("temp_min", temp_mn, -40.0, 120.0, line_num, &path)?;
    let temp_mx = next_f32(&mut iter, &path, line_num)?;
    range_check("temp_max", temp_mx, -40.0, 120.0, line_num, &path)?;
    let temp_avg = next_f32(&mut iter, &path, line_num)?;
    if temp_avg < temp_mn || temp_avg > temp_mx {
        return Err(Error::Parse {
            file: path,
            line: line_num,
            message: format!(
                "temp_mean {} not within [temp_min={}, temp_max={}]",
                temp_avg, temp_mn, temp_mx
            ),
        });
    }
    let altitude_raw = next_value(&mut iter, &path, line_num)?;
    let altitude = match altitude_raw.to_ascii_uppercase().as_str() {
        "HIGH" => AltitudeFlag::High,
        "LOW" => AltitudeFlag::Low,
        other => {
            return Err(Error::Parse {
                file: path,
                line: line_num,
                message: format!("invalid altitude flag: {}", other),
            });
        }
    };

    // Optional ethanol records: first label starts with ETOH.
    let mut ethanol_share: Option<f32> = None;
    let mut ethanol_vol: Option<f32> = None;
    if let Some((label, value, l)) = iter.next() {
        if label.to_ascii_uppercase().starts_with("ETOH") {
            let v: f32 = parse_required_f32(&value, "ethanol_market_share", l, &path)?;
            range_check("ethanol_market_share", v, 0.0, 100.0, l, &path)?;
            ethanol_share = Some(v);
            if let Some((_, vol_raw, l2)) = iter.next() {
                let v2 = parse_required_f32(&vol_raw, "ethanol_vol_pct", l2, &path)?;
                range_check("ethanol_vol_pct", v2, 0.0, 100.0, l2, &path)?;
                ethanol_vol = Some(v2);
            }
        }
    }

    Ok(OptionsConfig {
        title1: title1.trim_matches('"').to_string(),
        title2: title2.trim_matches('"').to_string(),
        fuel_rvp: rvp,
        oxygen_pct: oxy,
        sulfur_gasoline: sox_gas,
        sulfur_diesel_land: sox_dsl,
        sulfur_diesel_marine: sox_dsm,
        sulfur_cng: sox_cng,
        temp_min: temp_mn,
        temp_max: temp_mx,
        temp_mean: temp_avg,
        altitude,
        ethanol_market_share: ethanol_share,
        ethanol_vol_pct: ethanol_vol,
    })
}

fn split_label_value(line: &str) -> (String, String) {
    if let Some(idx) = line.find(':') {
        (
            line[..idx].trim().to_string(),
            line[idx + 1..].trim().to_string(),
        )
    } else if line.len() > 20 {
        (line[..20].trim().to_string(), line[20..].trim().to_string())
    } else {
        (line.trim().to_string(), String::new())
    }
}

fn next_value<I>(iter: &mut I, path: &std::path::Path, fallback_line: usize) -> Result<String>
where
    I: Iterator<Item = (String, String, usize)>,
{
    iter.next()
        .map(|(_, v, _)| v)
        .ok_or_else(|| missing_record(path, "next /OPTIONS/ record", fallback_line))
}

fn next_f32<I>(iter: &mut I, path: &std::path::Path, fallback_line: usize) -> Result<f32>
where
    I: Iterator<Item = (String, String, usize)>,
{
    let (_, value, line) = iter
        .next()
        .ok_or_else(|| missing_record(path, "numeric record", fallback_line))?;
    parse_required_f32(&value, "value", line, path)
}

fn parse_required_f32(
    raw: &str,
    name: &str,
    line: usize,
    path: &std::path::Path,
) -> Result<f32> {
    raw.trim().parse().map_err(|_| Error::Parse {
        file: path.to_path_buf(),
        line,
        message: format!("invalid {}: {:?}", name, raw),
    })
}

fn range_check(
    name: &str,
    v: f32,
    lo: f32,
    hi: f32,
    line: usize,
    path: &std::path::Path,
) -> Result<()> {
    if v < lo || v > hi {
        return Err(Error::Parse {
            file: path.to_path_buf(),
            line,
            message: format!("{} {} outside valid range [{}, {}]", name, v, lo, hi),
        });
    }
    Ok(())
}

fn missing_record(path: &std::path::Path, what: &str, line: usize) -> Error {
    Error::Parse {
        file: path.to_path_buf(),
        line,
        message: format!("unexpected end of /OPTIONS/ packet — missing {}", what),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn full_packet() -> &'static str {
        // Minimal complete packet: 11 records + altitude.
        "\
/OPTIONS/
Title 1            : Sample run
Title 2            : Defaults
Fuel RVP           : 9.0
Oxygen Pct         : 2.7
Gas Sulfur         : 0.030
Diesel Sulfur      : 0.0015
CNG Sulfur         : 0.0001
Temp Min           : 60.0
Temp Max           : 84.0
Temp Mean          : 72.0
Altitude           : LOW
/END/
"
    }

    #[test]
    fn parses_full_packet() {
        let cfg = read_options(full_packet().as_bytes()).unwrap();
        assert_eq!(cfg.title1, "Sample run");
        assert!((cfg.fuel_rvp - 9.0).abs() < 1e-6);
        assert!((cfg.sulfur_diesel_marine - cfg.sulfur_diesel_land).abs() < 1e-6);
        assert_eq!(cfg.altitude, AltitudeFlag::Low);
        assert!(cfg.ethanol_market_share.is_none());
    }

    #[test]
    fn parses_with_marine_diesel_and_ethanol() {
        let input = "\
/OPTIONS/
Title 1            : Run
Title 2            : Comment
Fuel RVP           : 7.0
Oxygen Pct         : 3.5
Gas Sulfur         : 0.030
Diesel Sulfur      : 0.0015
Marine Sulfur      : 0.0050
CNG Sulfur         : 0.0001
Temp Min           : 30.0
Temp Max           : 95.0
Temp Mean          : 70.0
Altitude           : HIGH
ETOH Mkt Share     : 80.0
ETOH Vol Pct       : 10.0
/END/
";
        let cfg = read_options(input.as_bytes()).unwrap();
        assert_eq!(cfg.altitude, AltitudeFlag::High);
        assert!((cfg.sulfur_diesel_marine - 0.0050).abs() < 1e-6);
        assert_eq!(cfg.ethanol_market_share, Some(80.0));
        assert_eq!(cfg.ethanol_vol_pct, Some(10.0));
    }

    #[test]
    fn rejects_out_of_range_rvp() {
        let bad = full_packet().replace("9.0", "20.0");
        let err = read_options(bad.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("fuel_rvp")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn rejects_temp_mean_outside_window() {
        let bad = full_packet().replace("Temp Mean          : 72.0", "Temp Mean          : 100.0");
        let err = read_options(bad.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("temp_mean")),
            other => panic!("unexpected error: {other:?}"),
        }
    }
}

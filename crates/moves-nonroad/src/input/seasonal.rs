//! Seasonal-factor parser (`rdseas.f`, `rdday.f`).
//!
//! Task 95. Parses the `.DAT` and `.DAY` input files that contain
//! seasonal and day-of-year adjustment factors for equipment emissions.
//!
//! # Formats
//!
//! ## `.DAT` file (seasonal factors)
//!
//! The `.DAT` file is a whitespace-delimited text file with the
//! following structure:
//!
//! ```text
//! <n_equipment>
//! <equipment_idx> <month1> <month2> ... <month12>
//! <equipment_idx> <month1> <month2> ... <month12>
//! ...
//! ```
//!
//! - `n_equipment`: Number of equipment categories
//! - `equipment_idx`: 1-based equipment index
//! - `month1` through `month12`: Monthly seasonal factors
//!
//! ## `.DAY` file (day-of-year factors)
//!
//! The `.DAY` file contains 365 day-of-year adjustment factors:
//!
//! ```text
//! <day1> <day2> ... <day365>
//! ```
//!
//! Values can span multiple lines; whitespace separates values.
//!
//! # Fortran source
//!
//! This module ports:
//! - `rdseas.f` (280 lines) - seasonal factor parsing
//! - `rdday.f` (255 lines) - day-of-year factor parsing

use crate::{Error, Result};
use ndarray::Array2;
use std::io::BufRead;
use std::path::PathBuf;

/// Seasonal factor record for one equipment category.
#[derive(Debug, Clone)]
pub struct SeasonalRecord {
    /// Equipment index (0-based).
    pub equipment_idx: usize,
    /// Monthly seasonal factors (12 values, one per month).
    pub monthly_factors: [f64; 12],
}

/// Parse a `.DAT` file and return seasonal factors.
///
/// Returns a 2D array indexed by `[equipment][month]` where month is
/// 0-based (0 = January).
pub fn read_dat<R: BufRead>(reader: R, n_equipment: usize) -> Result<Array2<f64>> {
    let mut seasonal = Array2::from_elem((n_equipment, 12), 1.0);
    let mut lines = reader.lines();
    let mut line_num = 0;

    // Read header (skip blank and comment lines)
    let header_line = loop {
        line_num += 1;
        let line = lines
            .next()
            .ok_or_else(|| Error::Parse {
                file: PathBuf::from(".DAT"),
                line: line_num,
                message: "empty seasonal file".to_string(),
            })?
            .map_err(|e| Error::Io {
                path: PathBuf::from(".DAT"),
                source: e,
            })?;
        let trimmed = line.trim();
        if !trimmed.is_empty() && !trimmed.starts_with('#') {
            break line;
        }
    };

    let file_n_equipment: usize = match header_line.split_whitespace().next() {
        Some(tok) => match tok.parse::<usize>() {
            Ok(v) => v,
            Err(_) => {
                return Err(Error::Parse {
                    file: PathBuf::from(".DAT"),
                    line: line_num,
                    message: format!("invalid equipment count: {}", header_line),
                });
            }
        },
        None => {
            return Err(Error::Parse {
                file: PathBuf::from(".DAT"),
                line: line_num,
                message: "invalid header: expected equipment count".to_string(),
            });
        }
    };

    if file_n_equipment != n_equipment {
        return Err(Error::Parse {
            file: PathBuf::from(".DAT"),
            line: line_num,
            message: format!(
                "equipment count mismatch: file has {}, expected {}",
                file_n_equipment, n_equipment
            ),
        });
    }

    // Read seasonal factor records
    for line_result in lines {
        line_num += 1;
        let line = line_result.map_err(|e| Error::Io {
            path: PathBuf::from(".DAT"),
            source: e,
        })?;

        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 13 {
            return Err(Error::Parse {
                file: PathBuf::from(".DAT"),
                line: line_num,
                message: format!(
                    "invalid record: expected 13 values (idx + 12 months), got {}",
                    parts.len()
                ),
            });
        }

        let equipment_idx: usize = match parts[0].parse::<usize>() {
            Ok(v) => v - 1,
            Err(_) => {
                return Err(Error::Parse {
                    file: PathBuf::from(".DAT"),
                    line: line_num,
                    message: format!("invalid equipment index: {}", parts[0]),
                });
            }
        };

        if equipment_idx >= n_equipment {
            return Err(Error::Parse {
                file: PathBuf::from(".DAT"),
                line: line_num,
                message: format!(
                    "equipment index out of bounds: {} (max: {})",
                    equipment_idx + 1,
                    n_equipment
                ),
            });
        }

        for (month_idx, val_str) in parts[1..].iter().enumerate() {
            if month_idx >= 12 {
                break;
            }
            let factor: f64 = match val_str.parse::<f64>() {
                Ok(v) => v,
                Err(_) => {
                    return Err(Error::Parse {
                        file: PathBuf::from(".DAT"),
                        line: line_num,
                        message: format!("invalid seasonal factor: {}", val_str),
                    });
                }
            };
            seasonal[[equipment_idx, month_idx]] = factor;
        }
    }

    Ok(seasonal)
}

/// Parse a `.DAT` file and return seasonal records.
///
/// This is a lower-level parsing function that returns raw records.
pub fn read_dat_records<R: BufRead>(reader: R) -> Result<Vec<SeasonalRecord>> {
    let mut records = Vec::new();
    let mut lines = reader.lines();
    let mut line_num = 0;

    // Skip header (first non-blank, non-comment line)
    loop {
        line_num += 1;
        let line = lines
            .next()
            .ok_or_else(|| Error::Parse {
                file: PathBuf::from(".DAT"),
                line: line_num,
                message: "empty seasonal file".to_string(),
            })?
            .map_err(|e| Error::Io {
                path: PathBuf::from(".DAT"),
                source: e,
            })?;
        let trimmed = line.trim();
        if !trimmed.is_empty() && !trimmed.starts_with('#') {
            break;
        }
    }

    // Read seasonal factor records
    for line_result in lines {
        let line = line_result.map_err(|e| Error::Io {
            path: PathBuf::from(".DAT"),
            source: e,
        })?;

        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 13 {
            continue; // Skip malformed lines
        }

        let equipment_idx: usize = match parts[0].parse::<usize>() {
            Ok(v) => v - 1,
            Err(_) => continue,
        };

        let mut monthly_factors = [1.0; 12];
        for (month_idx, val_str) in parts[1..].iter().enumerate() {
            if month_idx >= 12 {
                break;
            }
            monthly_factors[month_idx] = val_str.parse().unwrap_or(1.0);
        }

        records.push(SeasonalRecord {
            equipment_idx,
            monthly_factors,
        });
    }

    Ok(records)
}

/// Parse a `.DAY` file and return day-of-year factors.
///
/// Returns a vector of 365 day-of-year adjustment factors.
/// If fewer than 365 values are present, remaining days default to 1.0.
pub fn read_day<R: BufRead>(reader: R) -> Result<Vec<f64>> {
    let mut factors = Vec::with_capacity(365);

    for (idx, line_result) in reader.lines().enumerate() {
        let line_num = idx + 1;
        let line = line_result.map_err(|e| Error::Io {
            path: PathBuf::from(".DAY"),
            source: e,
        })?;

        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        for val_str in parts {
            if factors.len() >= 365 {
                break;
            }
            let factor: f64 = val_str.parse().map_err(|_| Error::Parse {
                file: PathBuf::from(".DAY"),
                line: line_num,
                message: format!("invalid day factor: {}", val_str),
            })?;
            factors.push(factor);
        }
    }

    // Pad to 365 days if needed
    while factors.len() < 365 {
        factors.push(1.0);
    }

    Ok(factors)
}

/// Get seasonal factor for a specific equipment and month.
///
/// # Arguments
/// * `seasonal` - 2D array of seasonal factors `[equipment][month]`
/// * `equipment_idx` - 0-based equipment index
/// * `month` - 0-based month index (0 = January)
pub fn get_seasonal_factor(seasonal: &Array2<f64>, equipment_idx: usize, month: usize) -> f64 {
    *seasonal.get([equipment_idx, month.min(11)]).unwrap_or(&1.0)
}

/// Get day-of-year factor.
///
/// # Arguments
/// * `day_factors` - Vector of 365 day-of-year factors
/// * `day_of_year` - 0-based day index (0 = January 1)
pub fn get_day_factor(day_factors: &[f64], day_of_year: usize) -> f64 {
    *day_factors.get(day_of_year.min(364)).unwrap_or(&1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_dat() {
        let input = r#"
2
1 1.0 1.0 1.0 1.0 1.0 1.0 1.0 1.0 1.0 1.0 1.0 1.0
2 0.8 0.9 1.0 1.1 1.2 1.3 1.2 1.1 1.0 0.9 0.8 0.7
"#;

        let seasonal = read_dat(input.as_bytes(), 2).unwrap();

        assert_eq!(seasonal.shape(), &[2, 12]);
        assert!((seasonal[[0, 0]] - 1.0).abs() < 1e-10);
        assert!((seasonal[[1, 4]] - 1.2).abs() < 1e-10); // May
        assert!((seasonal[[1, 11]] - 0.7).abs() < 1e-10); // December
    }

    #[test]
    fn test_read_dat_records() {
        let input = r#"
2
1 1.0 1.0 1.0 1.0 1.0 1.0 1.0 1.0 1.0 1.0 1.0 1.0
2 0.8 0.9 1.0 1.1 1.2 1.3 1.2 1.1 1.0 0.9 0.8 0.7
"#;

        let records = read_dat_records(input.as_bytes()).unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].equipment_idx, 0);
        assert_eq!(records[1].equipment_idx, 1);
        assert!((records[1].monthly_factors[4] - 1.2).abs() < 1e-10);
    }

    #[test]
    fn test_read_day() {
        let input = r#"
1.0 1.0 1.0 1.0 1.0 1.0 1.0 1.0 1.0 1.0
1.0 1.0 1.0 1.0 1.0 1.0 1.0 1.0 1.0 1.0
"#;

        let factors = read_day(input.as_bytes()).unwrap();

        assert_eq!(factors.len(), 365);
        assert!((factors[0] - 1.0).abs() < 1e-10);
        assert!((factors[364] - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_read_day_partial() {
        let input = "1.0 1.0 1.0\n";

        let factors = read_day(input.as_bytes()).unwrap();

        assert_eq!(factors.len(), 365);
        assert!((factors[0] - 1.0).abs() < 1e-10);
        assert!((factors[3] - 1.0).abs() < 1e-10); // Default
    }

    #[test]
    fn test_get_seasonal_factor() {
        let seasonal = Array2::from_shape_vec((2, 12), vec![1.0; 24]).unwrap();

        assert!((get_seasonal_factor(&seasonal, 0, 0) - 1.0).abs() < 1e-10);
        assert!((get_seasonal_factor(&seasonal, 1, 11) - 1.0).abs() < 1e-10);
        assert!((get_seasonal_factor(&seasonal, 0, 15) - 1.0).abs() < 1e-10); // Clamped
    }

    #[test]
    fn test_get_day_factor() {
        let factors: Vec<f64> = (0..365).map(|i| i as f64).collect();

        assert!((get_day_factor(&factors, 0) - 0.0).abs() < 1e-10);
        assert!((get_day_factor(&factors, 100) - 100.0).abs() < 1e-10);
        assert!((get_day_factor(&factors, 400) - 364.0).abs() < 1e-10); // Clamped
    }
}

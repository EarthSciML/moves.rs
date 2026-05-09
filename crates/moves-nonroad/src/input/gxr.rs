//! Growth-extrapolation parser (`rdgxrf.f`).
//!
//! Task 95. Parses the `.GXR` input file that contains growth
//! extrapolation factors for projecting equipment populations beyond
//! the base year.
//!
//! # Format
//!
//! The `.GXR` file is a whitespace-delimited text file with the
//! following structure:
//!
//! ```text
//! <n_counties> <n_equipment> <n_years>
//! <county_idx> <equipment_idx> <year1_factor> <year2_factor> ...
//! <county_idx> <equipment_idx> <year1_factor> <year2_factor> ...
//! ...
//! ```
//!
//! - `n_counties`: Number of counties in the dataset
//! - `n_equipment`: Number of equipment categories
//! - `n_years`: Number of extrapolation years
//! - `county_idx`: 1-based county index
//! - `equipment_idx`: 1-based equipment index
//! - `yearN_factor`: Growth factor for year N
//!
//! # Fortran source
//!
//! This module ports `rdgxrf.f` (169 lines) from the NONROAD2008a
//! source tree.

use crate::{Error, Result};
use ndarray::Array3;
use std::io::BufRead;
use std::path::PathBuf;

/// Growth extrapolation record for one county/equipment combination.
#[derive(Debug, Clone)]
pub struct GrowthExtrapolationRecord {
    /// County index (0-based).
    pub county_idx: usize,
    /// Equipment index (0-based).
    pub equipment_idx: usize,
    /// Year factors (one per extrapolation year).
    pub year_factors: Vec<f64>,
}

/// Parse a `.GXR` file and return growth extrapolation factors.
///
/// Returns a 3D array indexed by [county][equipment][year].
pub fn read_gxr<R: BufRead>(
    reader: R,
) -> Result<Array3<f64>> {
    let mut lines = reader.lines();
    let mut line_num = 0;

    // Read header (skip blank and comment lines)
    let header_line = loop {
        line_num += 1;
        let line = lines
            .next()
            .ok_or_else(|| Error::Parse {
                file: PathBuf::from(".GXR"),
                line: line_num,
                message: "empty growth extrapolation file".to_string(),
            })?
            .map_err(|e| Error::Io {
                path: PathBuf::from(".GXR"),
                source: e,
            })?;
        let trimmed = line.trim();
        if !trimmed.is_empty() && !trimmed.starts_with('#') {
            break line;
        }
    };

    let parts: Vec<&str> = header_line.split_whitespace().collect();
    if parts.len() < 3 {
        return Err(Error::Parse {
            file: PathBuf::from(".GXR"),
            line: 1,
            message: format!(
                "invalid header: expected 3 values, got {}",
                parts.len()
            ),
        });
    }

    let n_counties: usize = parts[0].parse().map_err(|_| Error::Parse {
        file: PathBuf::from(".GXR"),
        line: 1,
        message: format!("invalid county count: {}", parts[0]),
    })?;

    let n_equipment: usize = parts[1].parse().map_err(|_| Error::Parse {
        file: PathBuf::from(".GXR"),
        line: 1,
        message: format!("invalid equipment count: {}", parts[1]),
    })?;

    let n_years: usize = parts[2].parse().map_err(|_| Error::Parse {
        file: PathBuf::from(".GXR"),
        line: 1,
        message: format!("invalid year count: {}", parts[2]),
    })?;

    if n_counties == 0 || n_equipment == 0 || n_years == 0 {
        return Err(Error::Parse {
            file: PathBuf::from(".GXR"),
            line: 1,
            message: "zero dimension in growth extrapolation file".to_string(),
        });
    }

    let mut gxr = Array3::from_elem((n_counties, n_equipment, n_years), 1.0);
    line_num = 1;

    // Read growth extrapolation records
    for line_result in lines {
        line_num += 1;
        let line = line_result.map_err(|e| Error::Io {
            path: PathBuf::from(".GXR"),
            source: e,
        })?;

        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 3 {
            continue; // Skip malformed lines
        }

        let county_idx: usize = match parts[0].parse::<usize>() {
            Ok(v) => v - 1,
            Err(_) => {
                return Err(Error::Parse {
                    file: PathBuf::from(".GXR"),
                    line: line_num,
                    message: format!("invalid county index: {}", parts[0]),
                });
            }
        };

        let equipment_idx: usize = match parts[1].parse::<usize>() {
            Ok(v) => v - 1,
            Err(_) => {
                return Err(Error::Parse {
                    file: PathBuf::from(".GXR"),
                    line: line_num,
                    message: format!("invalid equipment index: {}", parts[1]),
                });
            }
        };

        if county_idx >= n_counties || equipment_idx >= n_equipment {
            return Err(Error::Parse {
                file: PathBuf::from(".GXR"),
                line: line_num,
                message: format!(
                    "index out of bounds: county={} equipment={} (max: county={}, equipment={})",
                    county_idx + 1,
                    equipment_idx + 1,
                    n_counties,
                    n_equipment
                ),
            });
        }

        // Read year factors
        for (year_idx, val_str) in parts[2..].iter().enumerate() {
            if year_idx >= n_years {
                break;
            }
            let factor: f64 = match val_str.parse::<f64>() {
                Ok(v) => v,
                Err(_) => {
                    return Err(Error::Parse {
                        file: PathBuf::from(".GXR"),
                        line: line_num,
                        message: format!("invalid year factor: {}", val_str),
                    });
                }
            };
            gxr[[county_idx, equipment_idx, year_idx]] = factor;
        }
    }

    Ok(gxr)
}

/// Parse a `.GXR` file and return growth extrapolation records.
///
/// This is a lower-level parsing function that returns raw records
/// without validating against dimensions.
pub fn read_gxr_records<R: BufRead>(reader: R) -> Result<Vec<GrowthExtrapolationRecord>> {
    let mut records = Vec::new();
    let mut lines = reader.lines();
    let mut line_num = 0;

    // Skip header (first non-blank, non-comment line)
    loop {
        line_num += 1;
        let line = lines
            .next()
            .ok_or_else(|| Error::Parse {
                file: PathBuf::from(".GXR"),
                line: line_num,
                message: "empty growth extrapolation file".to_string(),
            })?
            .map_err(|e| Error::Io {
                path: PathBuf::from(".GXR"),
                source: e,
            })?;
        let trimmed = line.trim();
        if !trimmed.is_empty() && !trimmed.starts_with('#') {
            break;
        }
    }

    // Read growth extrapolation records
    for line_result in lines {
        line_num += 1;
        let line = line_result.map_err(|e| Error::Io {
            path: PathBuf::from(".GXR"),
            source: e,
        })?;

        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 3 {
            continue; // Skip malformed lines
        }

        let county_idx: usize = match parts[0].parse::<usize>() {
            Ok(v) => v - 1,
            Err(_) => continue,
        };

        let equipment_idx: usize = match parts[1].parse::<usize>() {
            Ok(v) => v - 1,
            Err(_) => continue,
        };

        let mut year_factors = Vec::new();
        for val_str in parts[2..].iter() {
            let factor: f64 = val_str.parse().unwrap_or(1.0);
            year_factors.push(factor);
        }

        records.push(GrowthExtrapolationRecord {
            county_idx,
            equipment_idx,
            year_factors,
        });
    }

    Ok(records)
}

/// Get growth extrapolation factor for a specific county, equipment, and year.
///
/// # Arguments
/// * `gxr` - 3D array of growth factors [county][equipment][year]
/// * `county_idx` - 0-based county index
/// * `equipment_idx` - 0-based equipment index
/// * `year_idx` - 0-based year index
pub fn get_gxr_factor(
    gxr: &Array3<f64>,
    county_idx: usize,
    equipment_idx: usize,
    year_idx: usize,
) -> f64 {
    *gxr
        .get([county_idx, equipment_idx, year_idx])
        .unwrap_or(&1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_gxr() {
        let input = r#"
2 2 3
1 1 1.0 1.05 1.10
1 2 1.0 1.02 1.04
2 1 1.0 1.03 1.06
2 2 1.0 1.01 1.02
"#;

        let gxr = read_gxr(input.as_bytes()).unwrap();

        assert_eq!(gxr.shape(), &[2, 2, 3]);
        assert!((gxr[[0, 0, 0]] - 1.0).abs() < 1e-10);
        assert!((gxr[[0, 0, 1]] - 1.05).abs() < 1e-10);
        assert!((gxr[[0, 0, 2]] - 1.10).abs() < 1e-10);
        assert!((gxr[[1, 1, 2]] - 1.02).abs() < 1e-10);
    }

    #[test]
    fn test_read_gxr_records() {
        let input = r#"
2 2 3
1 1 1.0 1.05 1.10
2 2 1.0 1.01 1.02
"#;

        let records = read_gxr_records(input.as_bytes()).unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].county_idx, 0);
        assert_eq!(records[0].equipment_idx, 0);
        assert_eq!(records[0].year_factors.len(), 3);
        assert!((records[0].year_factors[1] - 1.05).abs() < 1e-10);
    }

    #[test]
    fn test_empty_gxr() {
        let input = "";

        let result = read_gxr(input.as_bytes());
        assert!(result.is_err());
    }
}

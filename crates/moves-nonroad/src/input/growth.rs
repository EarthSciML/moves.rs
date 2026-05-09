//! Growth-factor parser (`rdgrow.f`).
//!
//! Task 95. Parses the `.GRW` input file that contains growth factors
//! for equipment populations by county and equipment category.
//!
//! # Format
//!
//! The `.GRW` file is a whitespace-delimited text file with the
//! following structure:
//!
//! ```text
//! <n_counties> <n_equipment>
//! <county_idx> <equipment_idx> <growth_factor>
//! <county_idx> <equipment_idx> <growth_factor>
//! ...
//! ```
//!
//! - `n_counties`: Number of counties in the dataset
//! - `n_equipment`: Number of equipment categories
//! - `county_idx`: 1-based county index
//! - `equipment_idx`: 1-based equipment index
//! - `growth_factor`: Multiplicative growth factor (typically ~1.0)
//!
//! # Fortran source
//!
//! This module ports `rdgrow.f` (382 lines) from the NONROAD2008a
//! source tree.

use crate::common::PopulationState;
use crate::{Error, Result};
use ndarray::Array2;
use std::io::BufRead;
use std::path::PathBuf;

/// Growth factor record.
#[derive(Debug, Clone)]
pub struct GrowthRecord {
    /// County index (0-based).
    pub county_idx: usize,
    /// Equipment index (0-based).
    pub equipment_idx: usize,
    /// Growth factor (multiplicative).
    pub growth_factor: f64,
}

/// Parse a `.GRW` file and return growth factors.
///
/// This function reads the growth factor file and returns a 2D array
/// indexed by [county][equipment]. The array is sized based on the
/// dimensions found in the file header.
pub fn read_grw<R: BufRead>(reader: R) -> Result<Array2<f64>> {
    let mut lines = reader.lines();
    let mut line_num = 0;

    // Read header
    let header_line = lines
        .next()
        .ok_or_else(|| Error::Parse {
            file: PathBuf::from(".GRW"),
            line: 0,
            message: "empty growth file".to_string(),
        })?
        .map_err(|e| Error::Io {
            path: PathBuf::from(".GRW"),
            source: e,
        })?;

    let parts: Vec<&str> = header_line.split_whitespace().collect();
    if parts.len() < 2 {
        return Err(Error::Parse {
            file: PathBuf::from(".GRW"),
            line: 1,
            message: format!("invalid header: expected 2 values, got {}", parts.len()),
        });
    }

    let n_counties: usize = parts[0].parse().map_err(|_| Error::Parse {
        file: PathBuf::from(".GRW"),
        line: 1,
        message: format!("invalid county count: {}", parts[0]),
    })?;

    let n_equipment: usize = parts[1].parse().map_err(|_| Error::Parse {
        file: PathBuf::from(".GRW"),
        line: 1,
        message: format!("invalid equipment count: {}", parts[1]),
    })?;

    if n_counties == 0 || n_equipment == 0 {
        return Err(Error::Parse {
            file: PathBuf::from(".GRW"),
            line: 1,
            message: "zero dimension in growth file".to_string(),
        });
    }

    let mut growth = Array2::from_elem((n_counties, n_equipment), 1.0);
    line_num = 1;

    // Read growth factor records
    for line_result in lines {
        line_num += 1;
        let line = line_result.map_err(|e| Error::Io {
            path: PathBuf::from(".GRW"),
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

        let county_idx: usize = parts[0].parse().map_err(|_| Error::Parse {
            file: PathBuf::from(".GRW"),
            line: line_num,
            message: format!("invalid county index: {}", parts[0]),
        })? - 1; // Convert from 1-based to 0-based

        let equipment_idx: usize = parts[1].parse().map_err(|_| Error::Parse {
            file: PathBuf::from(".GRW"),
            line: line_num,
            message: format!("invalid equipment index: {}", parts[1]),
        })? - 1; // Convert from 1-based to 0-based

        let growth_factor: f64 = parts[2].parse().map_err(|_| Error::Parse {
            file: PathBuf::from(".GRW"),
            line: line_num,
            message: format!("invalid growth factor: {}", parts[2]),
        })?;

        if county_idx >= n_counties || equipment_idx >= n_equipment {
            return Err(Error::Parse {
                file: PathBuf::from(".GRW"),
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

        growth[[county_idx, equipment_idx]] = growth_factor;
    }

    Ok(growth)
}

/// Parse a `.GRW` file and return a vector of growth records.
///
/// This is a lower-level parsing function that returns raw records
/// without validating against a population state.
pub fn read_grw_records<R: BufRead>(reader: R) -> Result<Vec<GrowthRecord>> {
    let mut records = Vec::new();
    let mut lines = reader.lines();
    let mut line_num = 0;

    // Skip header
    let _header = lines
        .next()
        .ok_or_else(|| Error::Parse {
            file: PathBuf::from(".GRW"),
            line: 0,
            message: "empty growth file".to_string(),
        })?
        .map_err(|e| Error::Io {
            path: PathBuf::from(".GRW"),
            source: e,
        })?;

    line_num = 1;

    // Read growth factor records
    for line_result in lines {
        line_num += 1;
        let line = line_result.map_err(|e| Error::Io {
            path: PathBuf::from(".GRW"),
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

        let county_idx: usize = parts[0].parse().map_err(|_| Error::Parse {
            file: PathBuf::from(".GRW"),
            line: line_num,
            message: format!("invalid county index: {}", parts[0]),
        })? - 1; // Convert from 1-based to 0-based

        let equipment_idx: usize = parts[1].parse().map_err(|_| Error::Parse {
            file: PathBuf::from(".GRW"),
            line: line_num,
            message: format!("invalid equipment index: {}", parts[1]),
        })? - 1; // Convert from 1-based to 0-based

        let growth_factor: f64 = parts[2].parse().map_err(|_| Error::Parse {
            file: PathBuf::from(".GRW"),
            line: line_num,
            message: format!("invalid growth factor: {}", parts[2]),
        })?;

        records.push(GrowthRecord {
            county_idx,
            equipment_idx,
            growth_factor,
        });
    }

    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_grw() {
        let input = r#"
3 2
1 1 1.05
1 2 1.02
2 1 1.03
2 2 1.01
3 1 1.04
3 2 1.00
"#;

        let growth = read_grw(input.as_bytes()).unwrap();

        assert_eq!(growth.shape(), &[3, 2]);
        assert!((growth[[0, 0]] - 1.05).abs() < 1e-10);
        assert!((growth[[1, 1]] - 1.01).abs() < 1e-10);
        assert!((growth[[2, 1]] - 1.00).abs() < 1e-10);
    }

    #[test]
    fn test_read_grw_records() {
        let input = r#"
2 2
1 1 1.05
2 2 1.02
"#;

        let records = read_grw_records(input.as_bytes()).unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].county_idx, 0);
        assert_eq!(records[0].equipment_idx, 0);
        assert!((records[0].growth_factor - 1.05).abs() < 1e-10);
        assert_eq!(records[1].county_idx, 1);
        assert_eq!(records[1].equipment_idx, 1);
    }

    #[test]
    fn test_empty_grw() {
        let input = "";

        let result = read_grw(input.as_bytes());
        assert!(result.is_err());
    }
}

//! Day-of-year parser (`rdday.f`).
//!
//! Task 95. Parses the `.DAY` input file that contains day-of-year
//! adjustment factors for equipment emissions.
//!
//! # Format
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
//! This module ports `rdday.f` (255 lines) from the NONROAD2008a
//! source tree.

use crate::{Error, Result};
use std::io::BufRead;
use std::path::PathBuf;

/// Parse a `.DAY` file and return day-of-year factors.
///
/// Returns a vector of 365 day-of-year adjustment factors.
/// If fewer than 365 values are present, remaining days default to 1.0.
pub fn read_day<R: BufRead>(reader: R) -> Result<Vec<f64>> {
    let mut factors = Vec::with_capacity(365);
    let mut line_num = 0;

    for line_result in reader.lines() {
        line_num += 1;
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
    fn test_get_day_factor() {
        let factors: Vec<f64> = (0..365).map(|i| i as f64).collect();

        assert!((get_day_factor(&factors, 0) - 0.0).abs() < 1e-10);
        assert!((get_day_factor(&factors, 100) - 100.0).abs() < 1e-10);
        assert!((get_day_factor(&factors, 400) - 364.0).abs() < 1e-10); // Clamped
    }
}

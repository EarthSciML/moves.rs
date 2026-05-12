//! Spatial-indicator parser (`rdind.f`).
//!
//! Task 97. Parses spatial-allocation indicator files. The Fortran
//! source iterates over a list of allocation files (each containing
//! a single `/INDICATORS/` packet), filters records by region scope
//! and active allocation codes, and writes a sorted scratch file.
//! In Rust we expose the per-file packet parser plus a multi-file
//! convenience that flattens, optionally filters, and sorts.
//!
//! # Format
//!
//! ```text
//! /INDICATORS/
//! <code> <fips> <subcounty> <year> <value>
//! ...
//! /END/
//! ```
//!
//! `<code>` is the 3-character allocation indicator (e.g., POP, HHS).
//! `<fips>` is the 5-digit county/state FIPS or `00000` for all.
//! `<value>` is a real allocation factor.
//!
//! # Fortran source
//!
//! Ports `rdind.f` (355 lines). [`IndicatorTable`] additionally
//! replaces `getind.f` (313 lines): the Fortran routine performs a
//! streaming-file search with rewinds against the sorted scratch
//! file; the Rust port collapses it to an in-memory hash lookup that
//! preserves the year-selection rule documented on [`IndicatorTable`]
//! (Task 99).

use crate::{Error, Result};
use std::collections::HashMap;
use std::io::BufRead;
use std::path::PathBuf;

/// One indicator record.
#[derive(Debug, Clone, PartialEq)]
pub struct IndicatorRecord {
    /// 3-character allocation code.
    pub code: String,
    /// 5-digit FIPS code.
    pub fips: String,
    /// 5-character subcounty identifier.
    pub subcounty: String,
    /// 4-character year string (kept as text to match Fortran sort key).
    pub year: String,
    /// Allocation value.
    pub value: f64,
}

impl IndicatorRecord {
    /// Sort key matching Fortran's character-key concatenation
    /// (`code // fips // subcounty // year`). 17 characters wide.
    pub fn sort_key(&self) -> String {
        format!(
            "{:<3}{:<5}{:<5}{:<4}",
            self.code, self.fips, self.subcounty, self.year
        )
    }
}

/// Parse one indicator file.
pub fn read_ind<R: BufRead>(reader: R) -> Result<Vec<IndicatorRecord>> {
    let path = PathBuf::from(".IND");
    let mut out = Vec::new();
    let mut in_packet = false;

    for (idx, line_result) in reader.lines().enumerate() {
        let line_num = idx + 1;
        let line = line_result.map_err(|e| Error::Io {
            path: path.clone(),
            source: e,
        })?;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let upper = trimmed.to_ascii_uppercase();
        if upper.starts_with("/INDICATORS/") {
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
        if parts.len() < 5 {
            return Err(Error::Parse {
                file: path.clone(),
                line: line_num,
                message: format!(
                    "expected 5 fields (code, fips, subcounty, year, value), got {}",
                    parts.len()
                ),
            });
        }
        let value: f64 = parts[4].parse().map_err(|_| Error::Parse {
            file: path.clone(),
            line: line_num,
            message: format!("invalid value: {}", parts[4]),
        })?;
        out.push(IndicatorRecord {
            code: parts[0].to_ascii_uppercase(),
            fips: parts[1].to_string(),
            subcounty: parts[2].to_ascii_uppercase(),
            year: parts[3].to_string(),
            value,
        });
    }

    Ok(out)
}

/// Sort indicator records by `(code, fips, subcounty, year)`,
/// matching the Fortran `chrsrt` ordering on the concatenated key.
pub fn sort_indicators(records: &mut [IndicatorRecord]) {
    records.sort_by_key(|r| r.sort_key());
}

/// Fast lookup over a set of [`IndicatorRecord`]s, replacing the
/// streaming-file search in `getind.f`.
///
/// Built once from the parsed indicator records (typically from
/// [`read_ind`] concatenated across every active indicator file),
/// then queried per allocation operation. Records are grouped by
/// `(code, fips, subcounty)`; within each group the year/value
/// pairs are kept in ascending year order.
///
/// # Year-selection rule (from `getind.f`)
///
/// [`IndicatorTable::lookup`] picks a value from the matching group
/// using the same priority as the Fortran routine:
///
/// 1. The latest record whose year is `<= year_target` (closest
///    earlier year wins).
/// 2. Otherwise, the earliest record whose year is `> year_target`.
/// 3. Otherwise (no records match `code`/`fips`/`subcounty`),
///    `None`.
///
/// The 2005 EPA revision removed year interpolation; the lookup
/// returns the closest-earlier value verbatim (`getind.f` :138).
///
/// # Key normalization
///
/// `code` and `subcounty` are upper-cased and `fips` is left as-is
/// (the parser already keeps them in the canonical form). Lookups
/// are case-insensitive on `code` and `subcounty`; callers may
/// pass either form.
#[derive(Debug, Default, Clone)]
pub struct IndicatorTable {
    /// `(code, fips, subcounty)` → year-ascending `(year, value)` pairs.
    by_region: HashMap<(String, String, String), Vec<(i32, f32)>>,
}

impl IndicatorTable {
    /// Build a lookup table from a sequence of records.
    ///
    /// Records whose `year` field does not parse as an integer are
    /// skipped — production indicator files store 4-digit years, so
    /// this is defensive against malformed inputs that the streaming
    /// Fortran reader would have surfaced via its own format-error
    /// path.
    pub fn new<I: IntoIterator<Item = IndicatorRecord>>(records: I) -> Self {
        let mut table = Self::default();
        for record in records {
            let Ok(year) = record.year.trim().parse::<i32>() else {
                continue;
            };
            let key = Self::normalize_key(&record.code, &record.fips, &record.subcounty);
            table
                .by_region
                .entry(key)
                .or_default()
                .push((year, record.value as f32));
        }
        for years in table.by_region.values_mut() {
            years.sort_by_key(|(year, _)| *year);
        }
        table
    }

    /// Number of distinct `(code, fips, subcounty)` groups.
    pub fn group_count(&self) -> usize {
        self.by_region.len()
    }

    /// Whether any record exists for `(code, fips, subcounty)`.
    pub fn has_region(&self, code: &str, fips: &str, subcounty: &str) -> bool {
        let key = Self::normalize_key(code, fips, subcounty);
        self.by_region.contains_key(&key)
    }

    /// Look up the indicator value for `code` at `(fips, subcounty)`
    /// for `year_target`, applying the year-selection rule
    /// documented on [`IndicatorTable`].
    ///
    /// Returns `None` only when no record exists for the
    /// `(code, fips, subcounty)` triple. If a triple has any
    /// records at all, this method always returns `Some(_)` — the
    /// selection rule guarantees a hit either at or after the
    /// target year.
    pub fn lookup(&self, code: &str, fips: &str, subcounty: &str, year_target: i32) -> Option<f32> {
        let key = Self::normalize_key(code, fips, subcounty);
        let years = self.by_region.get(&key)?;
        select_year(years, year_target)
    }

    fn normalize_key(code: &str, fips: &str, subcounty: &str) -> (String, String, String) {
        (
            code.to_ascii_uppercase(),
            fips.to_string(),
            subcounty.to_ascii_uppercase(),
        )
    }
}

/// Year-selection helper used by [`IndicatorTable::lookup`]; exposed
/// for direct unit-testing.
///
/// `years` must already be sorted ascending by year (the table
/// invariant).
fn select_year(years: &[(i32, f32)], year_target: i32) -> Option<f32> {
    let mut latest_le: Option<(i32, f32)> = None;
    let mut earliest_gt: Option<(i32, f32)> = None;
    for (year, value) in years {
        if *year <= year_target {
            // Sorted ascending → keep overwriting; final survivor is
            // the closest earlier year.
            latest_le = Some((*year, *value));
        } else if earliest_gt.is_none() {
            earliest_gt = Some((*year, *value));
            // No need to keep scanning — subsequent entries are
            // further from the target.
            break;
        }
    }
    latest_le
        .map(|(_, v)| v)
        .or_else(|| earliest_gt.map(|(_, v)| v))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_packet() {
        let input = "\
/INDICATORS/
POP 17031 00000 2020 5180000.0
HHS 17031 00000 2020 1980000.0
/END/
";
        let records = read_ind(input.as_bytes()).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].code, "POP");
        assert!((records[1].value - 1980000.0).abs() < 1e-3);
    }

    #[test]
    fn sorts_by_concatenated_key() {
        let input = "\
/INDICATORS/
POP 17031 00000 2030 6000.0
POP 17031 00000 2020 5180.0
HHS 17031 00000 2020 1980.0
/END/
";
        let mut records = read_ind(input.as_bytes()).unwrap();
        sort_indicators(&mut records);
        let codes_years: Vec<_> = records
            .iter()
            .map(|r| (r.code.clone(), r.year.clone()))
            .collect();
        assert_eq!(
            codes_years,
            vec![
                ("HHS".into(), "2020".into()),
                ("POP".into(), "2020".into()),
                ("POP".into(), "2030".into()),
            ]
        );
    }

    #[test]
    fn rejects_short_record() {
        let input = "\
/INDICATORS/
POP 17031 00000 2020
/END/
";
        let err = read_ind(input.as_bytes()).unwrap_err();
        assert!(matches!(err, Error::Parse { .. }));
    }

    fn rec(code: &str, fips: &str, sub: &str, year: &str, value: f64) -> IndicatorRecord {
        IndicatorRecord {
            code: code.to_string(),
            fips: fips.to_string(),
            subcounty: sub.to_string(),
            year: year.to_string(),
            value,
        }
    }

    #[test]
    fn select_year_picks_closest_earlier() {
        let years = vec![(2000, 1.0), (2010, 2.0), (2020, 3.0)];
        assert_eq!(select_year(&years, 2015), Some(2.0));
        assert_eq!(select_year(&years, 2010), Some(2.0));
        assert_eq!(select_year(&years, 2020), Some(3.0));
        assert_eq!(select_year(&years, 2030), Some(3.0));
    }

    #[test]
    fn select_year_falls_back_to_earliest_later() {
        let years = vec![(2010, 1.5), (2020, 2.5)];
        assert_eq!(select_year(&years, 2005), Some(1.5));
        assert_eq!(select_year(&years, 1999), Some(1.5));
    }

    #[test]
    fn select_year_returns_none_for_empty() {
        let years: Vec<(i32, f32)> = Vec::new();
        assert_eq!(select_year(&years, 2020), None);
    }

    #[test]
    fn indicator_table_groups_by_region() {
        let records = vec![
            rec("POP", "17000", "", "2010", 100.0),
            rec("POP", "17000", "", "2020", 110.0),
            rec("POP", "17031", "", "2020", 50.0),
            rec("EMP", "17000", "", "2020", 80.0),
        ];
        let table = IndicatorTable::new(records);
        assert_eq!(table.group_count(), 3);
        assert!(table.has_region("POP", "17000", ""));
        assert!(table.has_region("pop", "17000", ""));
        assert!(!table.has_region("POP", "06000", ""));
    }

    #[test]
    fn indicator_table_lookup_applies_year_rule() {
        let records = vec![
            rec("POP", "17000", "", "2010", 1.0),
            rec("POP", "17000", "", "2020", 2.0),
        ];
        let table = IndicatorTable::new(records);
        assert_eq!(table.lookup("POP", "17000", "", 2015), Some(1.0));
        assert_eq!(table.lookup("POP", "17000", "", 2020), Some(2.0));
        assert_eq!(table.lookup("POP", "17000", "", 2025), Some(2.0));
        assert_eq!(table.lookup("POP", "17000", "", 2005), Some(1.0));
        assert_eq!(table.lookup("POP", "17000", "", 1900), Some(1.0));
    }

    #[test]
    fn indicator_table_lookup_missing_region_returns_none() {
        let records = vec![rec("POP", "17000", "", "2020", 1.0)];
        let table = IndicatorTable::new(records);
        assert_eq!(table.lookup("POP", "06000", "", 2020), None);
        assert_eq!(table.lookup("EMP", "17000", "", 2020), None);
    }

    #[test]
    fn indicator_table_lookup_is_case_insensitive_on_code_and_subcounty() {
        let records = vec![rec("pop", "17031", "abc", "2020", 9.0)];
        let table = IndicatorTable::new(records);
        assert_eq!(table.lookup("POP", "17031", "ABC", 2020), Some(9.0));
        assert_eq!(table.lookup("pop", "17031", "abc", 2020), Some(9.0));
    }

    #[test]
    fn indicator_table_skips_records_with_malformed_year() {
        let records = vec![
            rec("POP", "17000", "", "20XX", 1.0),
            rec("POP", "17000", "", "2020", 2.0),
        ];
        let table = IndicatorTable::new(records);
        assert_eq!(table.lookup("POP", "17000", "", 2030), Some(2.0));
    }

    #[test]
    fn indicator_table_lookup_returns_f32() {
        // Indicator values are REAL*4 in the Fortran source; the
        // table stores f32 even though the parsed records carry f64.
        let records = vec![rec("POP", "17000", "", "2020", 0.123_456_789_f64)];
        let table = IndicatorTable::new(records);
        let v = table.lookup("POP", "17000", "", 2020).unwrap();
        assert_eq!(v, 0.123_456_789_f64 as f32);
    }
}

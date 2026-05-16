//! AVFT input importer — validation logic ported from
//! `database/AVFTImporter.sql` (canonical MOVES).
//!
//! Three checks, matching the canonical SQL:
//!
//! 1. **ERROR** — for any `(sourceTypeID, modelYearID)` group whose
//!    `SUM(fuelEngFraction)` rounded to four decimals exceeds 1.0.
//! 2. **ERROR** — for any individual row with
//!    `fuelEngFraction < 0` (rounded to four decimals).
//! 3. **WARNING** — for any `(sourceTypeID, modelYearID)` group whose
//!    `SUM(fuelEngFraction)` rounded to four decimals is strictly
//!    between 0 and 1. The Java importer treats this as recoverable
//!    (the tool will renormalize). We surface it as a non-fatal
//!    [`Warning::FractionSumBelowOne`] entry on the [`Report`].
//!
//! The four-decimal rounding mirrors the SQL: `round(sum(...), 4) > 1`,
//! `round(fuelEngFraction, 4) < 0`. Performing it in fixed-precision i64
//! arithmetic (× 10_000) keeps the floating-point boundary check
//! reproducible across architectures.

use std::collections::BTreeMap;

use crate::error::{Error, Result};
use crate::model::{AvftRecord, AvftTable, ModelYearId, SourceTypeId};

pub use crate::csv_io::{read_csv, read_reader, ReadReport};

/// Non-fatal observation about the imported table.
#[derive(Debug, Clone, PartialEq)]
pub enum Warning {
    /// Sum of fractions for a (source type, model year) group is between
    /// 0 and 1 — the tool will renormalize, but the user is told.
    FractionSumBelowOne {
        source_type_id: SourceTypeId,
        model_year_id: ModelYearId,
        sum: f64,
    },
    /// Duplicate primary key seen during CSV parse — the later row wins.
    DuplicateKey(AvftRecord),
}

/// Result of [`validate`].
#[derive(Debug, Default)]
pub struct Report {
    pub warnings: Vec<Warning>,
}

/// Validate an [`AvftTable`] against `AVFTImporter.sql` rules.
///
/// Returns `Ok(report)` even if `report.warnings` is non-empty —
/// the Java importer treats those as `WARNING:` log lines, not
/// `ERROR:` lines. Returns `Err(Error::NegativeFraction)` or
/// `Err(Error::FractionSumExceedsOne)` on the two genuine error
/// conditions; the [`crate::error::Error`] variant captures the
/// offending row so the caller can render a useful diagnostic.
pub fn validate(table: &AvftTable) -> Result<Report> {
    let mut report = Report::default();

    // 1) Per-row negative fraction check.
    for r in table.iter() {
        if round4(r.fuel_eng_fraction) < 0 {
            return Err(Error::NegativeFraction {
                source_type_id: r.source_type_id as i64,
                model_year_id: r.model_year_id as i64,
                fuel_type_id: r.fuel_type_id as i64,
                eng_tech_id: r.eng_tech_id as i64,
                fuel_eng_fraction: r.fuel_eng_fraction,
            });
        }
    }

    // 2) Per-group sum checks. Group by (sourceTypeID, modelYearID).
    // Match the canonical SQL: SUM the raw f64 values, *then* round.
    let mut sums: BTreeMap<(SourceTypeId, ModelYearId), f64> = BTreeMap::new();
    for r in table.iter() {
        let bucket = sums
            .entry((r.source_type_id, r.model_year_id))
            .or_insert(0.0);
        *bucket += r.fuel_eng_fraction;
    }
    for ((st, my), sum_raw) in sums {
        let sum_q = round4(sum_raw);
        match sum_q.cmp(&10_000) {
            std::cmp::Ordering::Greater => {
                return Err(Error::FractionSumExceedsOne {
                    source_type_id: st as i64,
                    model_year_id: my as i64,
                    sum: (sum_q as f64) / 10_000.0,
                });
            }
            std::cmp::Ordering::Less if sum_q > 0 => {
                report.warnings.push(Warning::FractionSumBelowOne {
                    source_type_id: st,
                    model_year_id: my,
                    sum: (sum_q as f64) / 10_000.0,
                });
            }
            _ => {}
        }
    }

    Ok(report)
}

/// Round to four decimals using the same semantics as the canonical
/// `ROUND(..., 4)` — banker's-tie-round-to-even is *not* what MySQL
/// uses; MySQL's `ROUND` is half-away-from-zero. We reproduce that here.
fn round4(v: f64) -> i64 {
    let scaled = v * 10_000.0;
    // half-away-from-zero rounding
    if scaled >= 0.0 {
        (scaled + 0.5) as i64
    } else {
        (scaled - 0.5) as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round4_matches_mysql_semantics() {
        assert_eq!(round4(0.99995), 10_000); // rounds up to 1.0000
        assert_eq!(round4(0.99994), 9_999);
        assert_eq!(round4(-0.00005), -1); // half-away-from-zero
        assert_eq!(round4(1.0), 10_000);
        assert_eq!(round4(0.0), 0);
    }

    #[test]
    fn accepts_exactly_summing_groups() {
        let t: AvftTable = [
            AvftRecord::new(11, 2020, 1, 1, 0.6),
            AvftRecord::new(11, 2020, 2, 1, 0.4),
        ]
        .into_iter()
        .collect();
        let r = validate(&t).unwrap();
        assert!(r.warnings.is_empty());
    }

    #[test]
    fn rejects_negative_fraction() {
        let t: AvftTable = [AvftRecord::new(11, 2020, 1, 1, -0.5)]
            .into_iter()
            .collect();
        match validate(&t) {
            Err(Error::NegativeFraction { source_type_id, .. }) => assert_eq!(source_type_id, 11),
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn rejects_sum_exceeding_one() {
        let t: AvftTable = [
            AvftRecord::new(11, 2020, 1, 1, 0.7),
            AvftRecord::new(11, 2020, 2, 1, 0.5),
        ]
        .into_iter()
        .collect();
        match validate(&t) {
            Err(Error::FractionSumExceedsOne { sum, .. }) => assert!((sum - 1.2).abs() < 1e-9),
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn warns_on_partial_sum() {
        let t: AvftTable = [AvftRecord::new(11, 2020, 1, 1, 0.5)].into_iter().collect();
        let r = validate(&t).unwrap();
        assert_eq!(r.warnings.len(), 1);
        match &r.warnings[0] {
            Warning::FractionSumBelowOne {
                source_type_id,
                sum,
                ..
            } => {
                assert_eq!(*source_type_id, 11);
                assert!((sum - 0.5).abs() < 1e-9);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn does_not_warn_on_zero_sum_group() {
        // A (source type, model year) group whose every row is exactly
        // 0 is treated as "no input for this group at all" rather than
        // as a partial sum to renormalize — matches the SQL's
        // `HAVING sum > 0` filter on the WARNING insert.
        let t: AvftTable = [AvftRecord::new(11, 2020, 1, 1, 0.0)].into_iter().collect();
        let r = validate(&t).unwrap();
        assert!(r.warnings.is_empty());
    }

    #[test]
    fn four_decimal_rounding_at_boundary() {
        // 0.50004 + 0.50003 = 1.00007; rounded to 4 decimals = 1.0001 > 1.
        // But 0.50004 rounds individually to 0.5000, so the per-row
        // floor would say sum=1.0000. The Java SQL rounds the SUM, not
        // the addends; we follow the same convention.
        let t: AvftTable = [
            AvftRecord::new(11, 2020, 1, 1, 0.50004),
            AvftRecord::new(11, 2020, 2, 1, 0.50003),
        ]
        .into_iter()
        .collect();
        // Sum is 1.00007 → round4 = 10_001 > 10_000 → ERROR.
        match validate(&t) {
            Err(Error::FractionSumExceedsOne { .. }) => {}
            other => panic!("got {other:?}"),
        }
    }
}

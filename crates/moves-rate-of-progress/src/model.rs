//! Typed data model for the Rate-of-Progress reduction table.
//!
//! The ROP table is keyed by
//! `(pollutantID, sourceTypeID, regClassID, modelYearID)` and carries one
//! payload column, `reductionFraction`, representing the fraction of emissions
//! to be removed by the control strategy (0.0 = no change, 1.0 = 100%
//! reduction). The resulting emission scaling factor applied downstream is
//! `1.0 - reductionFraction`.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Pollutant identifier (`pollutantID`, `smallint`).
pub type PollutantId = i32;
/// Source-use-type identifier (`sourceTypeID`, `smallint`).
pub type SourceTypeId = i32;
/// Regulatory-class identifier (`regClassID`, `smallint`).
pub type RegClassId = i32;
/// Model-year identifier (`modelYearID`, `smallint unsigned`).
pub type ModelYearId = i32;

/// One row of the Rate-of-Progress reduction table.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct RopRecord {
    #[serde(rename = "pollutantID")]
    pub pollutant_id: PollutantId,
    #[serde(rename = "sourceTypeID")]
    pub source_type_id: SourceTypeId,
    #[serde(rename = "regClassID")]
    pub reg_class_id: RegClassId,
    #[serde(rename = "modelYearID")]
    pub model_year_id: ModelYearId,
    /// Fraction of emissions to remove: 0.0 = no change, 1.0 = eliminate
    /// entirely. The downstream scaling factor is `1.0 - reductionFraction`.
    #[serde(rename = "reductionFraction")]
    pub reduction_fraction: f64,
}

impl RopRecord {
    /// Construct a record from typed values.
    pub fn new(
        pollutant_id: PollutantId,
        source_type_id: SourceTypeId,
        reg_class_id: RegClassId,
        model_year_id: ModelYearId,
        reduction_fraction: f64,
    ) -> Self {
        Self {
            pollutant_id,
            source_type_id,
            reg_class_id,
            model_year_id,
            reduction_fraction,
        }
    }

    /// `(pollutantID, sourceTypeID, regClassID, modelYearID)` — the
    /// canonical primary key.
    pub fn key(&self) -> RopKey {
        RopKey {
            pollutant_id: self.pollutant_id,
            source_type_id: self.source_type_id,
            reg_class_id: self.reg_class_id,
            model_year_id: self.model_year_id,
        }
    }

    /// The emission scale factor `1.0 - reductionFraction`.
    pub fn emission_scale_factor(&self) -> f64 {
        1.0 - self.reduction_fraction
    }
}

/// Rate-of-Progress primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RopKey {
    pub pollutant_id: PollutantId,
    pub source_type_id: SourceTypeId,
    pub reg_class_id: RegClassId,
    pub model_year_id: ModelYearId,
}

/// In-memory Rate-of-Progress reduction table.
///
/// Stored as a `BTreeMap` so iteration order is deterministic
/// (lexicographic on the key tuple).
#[derive(Debug, Default, Clone)]
pub struct RopTable {
    records: BTreeMap<RopKey, f64>,
}

impl RopTable {
    /// Create an empty table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of rows in the table.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// `true` if the table has no rows.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Insert a record. If the key already exists the new fraction replaces it
    /// (last-write-wins; callers surface duplicate-key warnings separately).
    pub fn insert(&mut self, record: RopRecord) {
        self.records
            .insert(record.key(), record.reduction_fraction);
    }

    /// Look up the reduction fraction for the given key, if any.
    pub fn get(&self, key: &RopKey) -> Option<f64> {
        self.records.get(key).copied()
    }

    /// Whether the table contains an entry for the given key.
    pub fn contains_key(&self, key: &RopKey) -> bool {
        self.records.contains_key(key)
    }

    /// Iterate the table in canonical key-lexicographic order.
    pub fn iter(&self) -> impl Iterator<Item = RopRecord> + '_ {
        self.records.iter().map(|(k, &v)| RopRecord {
            pollutant_id: k.pollutant_id,
            source_type_id: k.source_type_id,
            reg_class_id: k.reg_class_id,
            model_year_id: k.model_year_id,
            reduction_fraction: v,
        })
    }

    /// Materialize the table as a `Vec<RopRecord>` in canonical order.
    pub fn to_vec(&self) -> Vec<RopRecord> {
        self.iter().collect()
    }

    /// Look up the emission scale factor (`1.0 - reductionFraction`) for the
    /// given key. Returns `1.0` (no change) when no entry is found.
    pub fn scale_factor(&self, key: &RopKey) -> f64 {
        self.records
            .get(key)
            .map(|&r| 1.0 - r)
            .unwrap_or(1.0)
    }

    /// Set of distinct `sourceTypeID`s present (ascending order).
    pub fn source_types(&self) -> Vec<SourceTypeId> {
        self.records
            .keys()
            .map(|k| k.source_type_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// Set of distinct `pollutantID`s present (ascending order).
    pub fn pollutants(&self) -> Vec<PollutantId> {
        self.records
            .keys()
            .map(|k| k.pollutant_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
}

impl FromIterator<RopRecord> for RopTable {
    fn from_iter<I: IntoIterator<Item = RopRecord>>(iter: I) -> Self {
        let mut t = Self::new();
        for r in iter {
            t.insert(r);
        }
        t
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iteration_is_key_sorted() {
        let mut t = RopTable::new();
        t.insert(RopRecord::new(2, 21, 10, 2020, 0.10));
        t.insert(RopRecord::new(1, 21, 10, 2020, 0.05));
        t.insert(RopRecord::new(1, 11, 10, 2020, 0.15));

        let recs: Vec<_> = t.iter().collect();
        assert_eq!(recs[0].pollutant_id, 1);
        assert_eq!(recs[0].source_type_id, 11);
        assert_eq!(recs[1].pollutant_id, 1);
        assert_eq!(recs[1].source_type_id, 21);
        assert_eq!(recs[2].pollutant_id, 2);
    }

    #[test]
    fn scale_factor_no_entry_returns_one() {
        let t = RopTable::new();
        let key = RopKey {
            pollutant_id: 3,
            source_type_id: 11,
            reg_class_id: 10,
            model_year_id: 2020,
        };
        assert_eq!(t.scale_factor(&key), 1.0);
    }

    #[test]
    fn scale_factor_with_entry() {
        let mut t = RopTable::new();
        t.insert(RopRecord::new(3, 11, 10, 2020, 0.25));
        let key = RopKey {
            pollutant_id: 3,
            source_type_id: 11,
            reg_class_id: 10,
            model_year_id: 2020,
        };
        assert!((t.scale_factor(&key) - 0.75).abs() < 1e-15);
    }

    #[test]
    fn emission_scale_factor_method() {
        let r = RopRecord::new(1, 11, 10, 2020, 0.3);
        assert!((r.emission_scale_factor() - 0.7).abs() < 1e-15);
    }

    #[test]
    fn last_write_wins_on_duplicate_key() {
        let mut t = RopTable::new();
        t.insert(RopRecord::new(1, 11, 10, 2020, 0.1));
        t.insert(RopRecord::new(1, 11, 10, 2020, 0.5));
        assert_eq!(t.len(), 1);
        let key = RopKey {
            pollutant_id: 1,
            source_type_id: 11,
            reg_class_id: 10,
            model_year_id: 2020,
        };
        assert_eq!(t.get(&key), Some(0.5));
    }

    #[test]
    fn source_types_returns_ascending_distinct() {
        let t: RopTable = [
            RopRecord::new(1, 62, 10, 2020, 0.1),
            RopRecord::new(1, 11, 10, 2020, 0.2),
            RopRecord::new(2, 11, 10, 2020, 0.3),
            RopRecord::new(1, 21, 10, 2020, 0.1),
        ]
        .into_iter()
        .collect();
        assert_eq!(t.source_types(), vec![11, 21, 62]);
    }

    #[test]
    fn pollutants_returns_ascending_distinct() {
        let t: RopTable = [
            RopRecord::new(3, 11, 10, 2020, 0.1),
            RopRecord::new(1, 11, 10, 2020, 0.2),
            RopRecord::new(2, 11, 10, 2020, 0.3),
        ]
        .into_iter()
        .collect();
        assert_eq!(t.pollutants(), vec![1, 2, 3]);
    }
}

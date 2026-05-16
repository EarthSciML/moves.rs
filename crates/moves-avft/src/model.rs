//! Typed data model for the AVFT table.
//!
//! The canonical `avft` MOVES table is keyed by
//! `(sourceTypeID, modelYearID, fuelTypeID, engTechID)` and carries one
//! payload column, `fuelEngFraction`. We carry every key as a typed
//! `i32` (no overflow risk — MOVES uses `smallint`) and the fraction as
//! `f64`, matching `double` in the SQL.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Source-use-type identifier (`avft.sourceTypeID`, `smallint`).
pub type SourceTypeId = i32;
/// Model-year identifier (`avft.modelYearID`, `smallint unsigned`).
pub type ModelYearId = i32;
/// Fuel-type identifier (`avft.fuelTypeID`, `smallint`).
pub type FuelTypeId = i32;
/// Engine-technology identifier (`avft.engTechID`, `smallint`).
pub type EngTechId = i32;

/// One row of the AVFT table.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct AvftRecord {
    #[serde(rename = "sourceTypeID")]
    pub source_type_id: SourceTypeId,
    #[serde(rename = "modelYearID")]
    pub model_year_id: ModelYearId,
    #[serde(rename = "fuelTypeID")]
    pub fuel_type_id: FuelTypeId,
    #[serde(rename = "engTechID")]
    pub eng_tech_id: EngTechId,
    #[serde(rename = "fuelEngFraction")]
    pub fuel_eng_fraction: f64,
}

impl AvftRecord {
    /// Construct a record from typed values.
    pub fn new(
        source_type_id: SourceTypeId,
        model_year_id: ModelYearId,
        fuel_type_id: FuelTypeId,
        eng_tech_id: EngTechId,
        fuel_eng_fraction: f64,
    ) -> Self {
        Self {
            source_type_id,
            model_year_id,
            fuel_type_id,
            eng_tech_id,
            fuel_eng_fraction,
        }
    }

    /// `(sourceTypeID, modelYearID, fuelTypeID, engTechID)` — the
    /// canonical primary key.
    pub fn key(&self) -> AvftKey {
        AvftKey {
            source_type_id: self.source_type_id,
            model_year_id: self.model_year_id,
            fuel_type_id: self.fuel_type_id,
            eng_tech_id: self.eng_tech_id,
        }
    }
}

/// AVFT primary key — `(sourceTypeID, modelYearID, fuelTypeID, engTechID)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AvftKey {
    pub source_type_id: SourceTypeId,
    pub model_year_id: ModelYearId,
    pub fuel_type_id: FuelTypeId,
    pub eng_tech_id: EngTechId,
}

/// In-memory AVFT table.
///
/// Stored as a `BTreeMap` so iteration order is deterministic
/// (lexicographic on the key tuple) — the canonical MOVES
/// `AVFTTool_OrderResults` procedure orders by the same key. The
/// `Vec`-shaped accessors materialize that order.
#[derive(Debug, Default, Clone)]
pub struct AvftTable {
    records: BTreeMap<AvftKey, f64>,
}

impl AvftTable {
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

    /// Insert a record. If the key already exists the new fraction
    /// replaces it (the canonical SQL importer uses `INSERT IGNORE`
    /// against a primary-key constraint, but for the in-memory model
    /// the simplest semantics is last-write-wins; the CSV reader
    /// surfaces duplicate keys to the caller separately).
    pub fn insert(&mut self, record: AvftRecord) {
        self.records.insert(record.key(), record.fuel_eng_fraction);
    }

    /// Look up the fraction for the given key, if any.
    pub fn get(&self, key: &AvftKey) -> Option<f64> {
        self.records.get(key).copied()
    }

    /// Whether the table contains an entry for the given key.
    pub fn contains_key(&self, key: &AvftKey) -> bool {
        self.records.contains_key(key)
    }

    /// Iterate the table in canonical order (key-lexicographic).
    pub fn iter(&self) -> impl Iterator<Item = AvftRecord> + '_ {
        self.records.iter().map(|(k, v)| AvftRecord {
            source_type_id: k.source_type_id,
            model_year_id: k.model_year_id,
            fuel_type_id: k.fuel_type_id,
            eng_tech_id: k.eng_tech_id,
            fuel_eng_fraction: *v,
        })
    }

    /// Materialize the table as a `Vec<AvftRecord>` in canonical order.
    pub fn to_vec(&self) -> Vec<AvftRecord> {
        self.iter().collect()
    }

    /// Iterate only the rows that match a given `sourceTypeID`.
    pub fn rows_for_source_type(
        &self,
        source_type_id: SourceTypeId,
    ) -> impl Iterator<Item = AvftRecord> + '_ {
        self.iter()
            .filter(move |r| r.source_type_id == source_type_id)
    }

    /// Set of `sourceTypeID`s present in the table (in ascending order).
    pub fn source_types(&self) -> Vec<SourceTypeId> {
        let mut s: Vec<SourceTypeId> = self.records.keys().map(|k| k.source_type_id).collect();
        s.dedup();
        // BTreeMap iteration is sorted by full key, so source_type_id
        // values appear non-decreasingly but with duplicates from the
        // (my, fuel, eng) cartesian — dedup leaves the ascending set.
        s
    }

    /// Remove every row whose `sourceTypeID` matches `id`.
    pub fn remove_source_type(&mut self, id: SourceTypeId) {
        self.records.retain(|k, _| k.source_type_id != id);
    }

    /// Number of rows in the table for a given `sourceTypeID`.
    pub fn count_for_source_type(&self, id: SourceTypeId) -> usize {
        self.records
            .keys()
            .filter(|k| k.source_type_id == id)
            .count()
    }
}

impl FromIterator<AvftRecord> for AvftTable {
    fn from_iter<I: IntoIterator<Item = AvftRecord>>(iter: I) -> Self {
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
        let mut t = AvftTable::new();
        t.insert(AvftRecord::new(21, 2020, 1, 1, 0.6));
        t.insert(AvftRecord::new(11, 2020, 1, 1, 0.4));
        t.insert(AvftRecord::new(11, 2010, 1, 1, 0.5));

        let recs: Vec<_> = t.iter().collect();
        assert_eq!(recs[0].source_type_id, 11);
        assert_eq!(recs[0].model_year_id, 2010);
        assert_eq!(recs[1].source_type_id, 11);
        assert_eq!(recs[1].model_year_id, 2020);
        assert_eq!(recs[2].source_type_id, 21);
    }

    #[test]
    fn source_types_returns_ascending_distinct() {
        let t: AvftTable = [
            AvftRecord::new(62, 2020, 2, 1, 1.0),
            AvftRecord::new(11, 2020, 1, 1, 0.5),
            AvftRecord::new(11, 2020, 2, 1, 0.5),
            AvftRecord::new(21, 2020, 1, 1, 1.0),
        ]
        .into_iter()
        .collect();
        assert_eq!(t.source_types(), vec![11, 21, 62]);
    }

    #[test]
    fn remove_source_type_strips_only_matching() {
        let mut t: AvftTable = [
            AvftRecord::new(62, 2020, 2, 1, 1.0),
            AvftRecord::new(11, 2020, 1, 1, 1.0),
        ]
        .into_iter()
        .collect();
        t.remove_source_type(11);
        assert_eq!(t.len(), 1);
        assert_eq!(t.iter().next().unwrap().source_type_id, 62);
    }

    #[test]
    fn last_write_wins_on_duplicate_key() {
        let mut t = AvftTable::new();
        t.insert(AvftRecord::new(11, 2020, 1, 1, 0.5));
        t.insert(AvftRecord::new(11, 2020, 1, 1, 0.9));
        assert_eq!(t.len(), 1);
        assert_eq!(
            t.get(&AvftKey {
                source_type_id: 11,
                model_year_id: 2020,
                fuel_type_id: 1,
                eng_tech_id: 1
            }),
            Some(0.9)
        );
    }
}

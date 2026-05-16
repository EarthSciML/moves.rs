//! Table and column descriptors — the Rust shape of an importer's
//! `dataTableDescriptor`.
//!
//! Java's descriptor is an interleaved `String[]` keyed by sentinel
//! values (`BEGIN_TABLE`, then triples of column/decode/filter), see
//! `gov/epa/otaq/moves/master/implementation/importers/*Importer.java`.
//! The Rust port flattens to a normal struct of structs because we
//! don't share the descriptor with a GUI-side editor that needs to walk
//! it incrementally.

use arrow::datatypes::DataType;

use crate::filter::Filter;

/// One column in an importer's table.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDescriptor {
    /// Column name as it appears in the user's CSV header and in the
    /// default-DB schema (`tables.json`). MOVES is case-sensitive about
    /// some column names (`SHOAllocFactor`, `HPMSVtypeID`) so the
    /// descriptor stores them verbatim.
    pub name: &'static str,
    /// Per-column validation constraint.
    pub filter: Filter,
}

impl ColumnDescriptor {
    /// Build a descriptor with name + filter.
    pub const fn new(name: &'static str, filter: Filter) -> Self {
        Self { name, filter }
    }

    /// Arrow type derived from the filter.
    pub fn arrow_type(&self) -> DataType {
        self.filter.arrow_type()
    }
}

/// One table managed by an importer. Composite importers (e.g.
/// `ZoneImporter` which manages both `zone` and `zoneRoadType`)
/// declare two [`TableDescriptor`]s.
#[derive(Debug, Clone)]
pub struct TableDescriptor {
    /// Canonical table name (matches `tables.json` casing).
    pub name: &'static str,
    /// Columns, in declaration order.
    pub columns: &'static [ColumnDescriptor],
    /// Primary-key column names. Output Parquet is sorted by these for
    /// byte-determinism.
    pub primary_key: &'static [&'static str],
}

impl TableDescriptor {
    /// Look up a column by its descriptor name. Case-sensitive on
    /// purpose: the descriptor encodes the canonical casing.
    pub fn column(&self, name: &str) -> Option<&ColumnDescriptor> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Return all columns whose filter is a foreign-key constraint.
    pub fn foreign_key_columns(&self) -> impl Iterator<Item = &ColumnDescriptor> {
        self.columns
            .iter()
            .filter(|c| c.filter.decode_table().is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_lookup_is_case_sensitive() {
        const COLS: &[ColumnDescriptor] = &[
            ColumnDescriptor::new("zoneID", Filter::Zone),
            ColumnDescriptor::new("SHOAllocFactor", Filter::NonNegative),
        ];
        let t = TableDescriptor {
            name: "ZoneRoadType",
            columns: COLS,
            primary_key: &["zoneID"],
        };
        assert!(t.column("zoneID").is_some());
        assert!(t.column("shoallocfactor").is_none());
        assert!(t.column("SHOAllocFactor").is_some());
    }

    #[test]
    fn foreign_key_iteration_filters_to_decode_table_filters() {
        const COLS: &[ColumnDescriptor] = &[
            ColumnDescriptor::new("yearID", Filter::Year),
            ColumnDescriptor::new("sourceTypeID", Filter::SourceType),
            ColumnDescriptor::new("sourceTypePopulation", Filter::NonNegative),
        ];
        let t = TableDescriptor {
            name: "SourceTypeYear",
            columns: COLS,
            primary_key: &["yearID", "sourceTypeID"],
        };
        let fk: Vec<_> = t.foreign_key_columns().map(|c| c.name).collect();
        assert_eq!(fk, vec!["yearID", "sourceTypeID"]);
    }
}

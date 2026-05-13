//! Filter predicates used to drive partition pruning.
//!
//! A [`TableFilter`] carries optional equality / set-membership predicates
//! per partition column. The scan layer walks the manifest's
//! `partitions[]` array and keeps only those whose `values` satisfy every
//! predicate. Predicates against non-partition columns are out of scope
//! for the filter — apply those to the returned `LazyFrame` directly.
//!
//! ## Value representation
//!
//! Partition values are stored as strings in the manifest (the exact byte
//! sequence the TSV cell contained, e.g. `"17"` or `"2020"`). The filter
//! holds the same string representation so equality is a byte comparison.
//! `From<i64>`, `From<&str>`, etc. conversions cover the common call
//! sites without forcing the caller to format ids by hand.

use std::collections::HashMap;

/// Predicate applied to a single partition column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionPredicate {
    /// Keep partitions whose value equals this string.
    Eq(String),
    /// Keep partitions whose value is in this set.
    InSet(Vec<String>),
}

impl PartitionPredicate {
    /// Test whether `value` (the manifest's partition string) matches.
    pub fn matches(&self, value: &str) -> bool {
        match self {
            PartitionPredicate::Eq(v) => v == value,
            PartitionPredicate::InSet(vs) => vs.iter().any(|v| v == value),
        }
    }
}

/// Per-table filter passed to [`crate::DefaultDb::scan`].
///
/// Empty (`TableFilter::default()`) means "load every partition" — the
/// scan returns a `LazyFrame` over the union of all files.
#[derive(Debug, Clone, Default)]
pub struct TableFilter {
    predicates: HashMap<String, PartitionPredicate>,
}

impl TableFilter {
    /// Create an empty filter (no predicates → no partition pruning).
    pub fn new() -> Self {
        Self::default()
    }

    /// Restrict the scan to partitions where `column` equals `value`.
    ///
    /// `column` must match (case-sensitively) the column name in the
    /// table manifest's `partition_columns`. The scan layer validates
    /// this and returns [`crate::Error::UnknownPartitionColumn`] if the
    /// column is not partitioning the table.
    pub fn partition_eq(mut self, column: impl Into<String>, value: impl PartitionValue) -> Self {
        self.predicates.insert(
            column.into(),
            PartitionPredicate::Eq(value.to_partition_string()),
        );
        self
    }

    /// Restrict the scan to partitions where `column` is in `values`.
    ///
    /// An empty iterator means "no partitions match" — the resulting scan
    /// returns an empty frame.
    pub fn partition_in<V, I>(mut self, column: impl Into<String>, values: I) -> Self
    where
        V: PartitionValue,
        I: IntoIterator<Item = V>,
    {
        let values: Vec<String> = values
            .into_iter()
            .map(|v| v.to_partition_string())
            .collect();
        self.predicates
            .insert(column.into(), PartitionPredicate::InSet(values));
        self
    }

    /// Whether this filter has no predicates.
    pub fn is_empty(&self) -> bool {
        self.predicates.is_empty()
    }

    /// Look up the predicate for a partition column (if any).
    pub fn predicate(&self, column: &str) -> Option<&PartitionPredicate> {
        self.predicates.get(column)
    }

    /// Iterate the columns this filter constrains.
    pub fn columns(&self) -> impl Iterator<Item = &str> {
        self.predicates.keys().map(String::as_str)
    }

    /// Test whether a partition row (one value per `columns` entry, in
    /// order) satisfies every constrained predicate.
    ///
    /// Partition columns that the filter does not constrain are ignored —
    /// they accept any value, which is the right default for "load
    /// everything matching `countyID=17` regardless of year".
    pub fn matches(&self, columns: &[String], values: &[String]) -> bool {
        debug_assert_eq!(
            columns.len(),
            values.len(),
            "partition columns/values length mismatch"
        );
        for (col, val) in columns.iter().zip(values.iter()) {
            if let Some(p) = self.predicates.get(col) {
                if !p.matches(val) {
                    return false;
                }
            }
        }
        true
    }
}

/// Anything that can be rendered into the manifest's partition-string form.
///
/// The default-DB conversion writes partition values as the raw decimal
/// representation of integer ids (no leading zeros, no thousands
/// separators) and that's what the manifest carries. `i64` / `i32` / `u32`
/// — the common id types — round-trip cleanly through `i64::to_string`,
/// so [`PartitionValue::to_partition_string`] just defers to `ToString`
/// for those, and the same for `&str` / `String`.
pub trait PartitionValue {
    fn to_partition_string(self) -> String;
}

impl PartitionValue for String {
    fn to_partition_string(self) -> String {
        self
    }
}

impl PartitionValue for &str {
    fn to_partition_string(self) -> String {
        self.to_string()
    }
}

impl PartitionValue for i64 {
    fn to_partition_string(self) -> String {
        self.to_string()
    }
}

impl PartitionValue for i32 {
    fn to_partition_string(self) -> String {
        self.to_string()
    }
}

impl PartitionValue for u32 {
    fn to_partition_string(self) -> String {
        self.to_string()
    }
}

impl PartitionValue for u16 {
    fn to_partition_string(self) -> String {
        self.to_string()
    }
}

impl PartitionValue for i16 {
    fn to_partition_string(self) -> String {
        self.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_filter_matches_everything() {
        let f = TableFilter::new();
        assert!(f.is_empty());
        assert!(f.matches(&["countyID".into()], &["17".into()]));
        assert!(f.matches(&["countyID".into()], &["6037".into()]));
    }

    #[test]
    fn partition_eq_filters_to_one_value() {
        let f = TableFilter::new().partition_eq("countyID", 17i64);
        assert!(f.matches(&["countyID".into()], &["17".into()]));
        assert!(!f.matches(&["countyID".into()], &["6037".into()]));
    }

    #[test]
    fn partition_in_filters_to_set() {
        let f = TableFilter::new().partition_in("yearID", [2020i64, 2025]);
        assert!(f.matches(&["yearID".into()], &["2020".into()]));
        assert!(f.matches(&["yearID".into()], &["2025".into()]));
        assert!(!f.matches(&["yearID".into()], &["2024".into()]));
    }

    #[test]
    fn partition_in_with_empty_set_rejects_all() {
        let f = TableFilter::new().partition_in::<i64, _>("yearID", []);
        assert!(!f.matches(&["yearID".into()], &["2020".into()]));
    }

    #[test]
    fn unconstrained_columns_are_unbounded() {
        // year_x_county table: filter only sets countyID, all years
        // should pass.
        let f = TableFilter::new().partition_eq("countyID", 17i64);
        let cols = vec!["yearID".to_string(), "countyID".to_string()];
        assert!(f.matches(&cols, &["1990".into(), "17".into()]));
        assert!(f.matches(&cols, &["2099".into(), "17".into()]));
        assert!(!f.matches(&cols, &["2020".into(), "18".into()]));
    }

    #[test]
    fn predicate_lookup_returns_constraints() {
        let f = TableFilter::new()
            .partition_eq("countyID", 17i64)
            .partition_in("yearID", [2020i64, 2025]);
        assert_eq!(
            f.predicate("countyID"),
            Some(&PartitionPredicate::Eq("17".into()))
        );
        assert_eq!(
            f.predicate("yearID"),
            Some(&PartitionPredicate::InSet(vec![
                "2020".into(),
                "2025".into()
            ]))
        );
        assert!(f.predicate("monthID").is_none());
    }

    #[test]
    fn string_and_integer_values_normalize_to_same_string() {
        let f_int = TableFilter::new().partition_eq("countyID", 17i64);
        let f_str = TableFilter::new().partition_eq("countyID", "17");
        assert_eq!(f_int.predicate("countyID"), f_str.predicate("countyID"));
    }
}

//! In-memory [`DataFrameStore`] implementation.

use std::collections::BTreeMap;
use std::sync::Arc;

use polars::prelude::DataFrame;

use super::DataFrameStore;

/// A [`DataFrameStore`] backed by a `BTreeMap<String, Arc<DataFrame>>`.
///
/// Used for both the slow (default-DB) tier ([`crate::ExecutionTables`]) and
/// the scratch (inter-calculator) tier ([`crate::ScratchNamespace`]). The two
/// tiers share the same concrete type; their ownership and mutability rules are
/// enforced by the [`crate::CalculatorContext`] accessors (read-only `tables`,
/// read-write `tables_mut` / `scratch_mut`).
///
/// Validation against [`crate::ExecutionDatabaseSchema`] is deferred to T3.
#[derive(Debug, Default, Clone)]
pub struct InMemoryStore {
    map: BTreeMap<String, Arc<DataFrame>>,
}

impl InMemoryStore {
    /// Construct an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl DataFrameStore for InMemoryStore {
    fn get(&self, name: &str) -> Option<Arc<DataFrame>> {
        self.map.get(name).cloned()
    }

    fn insert(&mut self, name: impl Into<String>, df: DataFrame) {
        self.map.insert(name.into(), Arc::new(df));
    }

    fn contains(&self, name: &str) -> bool {
        self.map.contains_key(name)
    }

    fn names(&self) -> Vec<&str> {
        self.map.keys().map(String::as_str).collect()
    }
}

#[cfg(test)]
mod tests {
    use polars::prelude::*;

    use super::*;
    use crate::CalculatorContext;

    fn one_col_df(name: &str) -> DataFrame {
        let s = Series::new(name.into(), [1i32, 2, 3]);
        DataFrame::new(3, vec![s.into()]).unwrap()
    }

    #[test]
    fn store_insert_then_get_round_trips() {
        let mut store = InMemoryStore::new();
        let df = one_col_df("sourceUseTypePopulation");
        let shape = df.shape();
        store.insert("sourceUseTypePopulation", df);
        let got = store.get("sourceUseTypePopulation").expect("should be present");
        assert_eq!(got.shape(), shape);
    }

    #[test]
    fn store_get_unknown_returns_none() {
        let store = InMemoryStore::new();
        assert!(store.get("notATable").is_none());
    }

    #[test]
    fn store_insert_duplicate_replaces() {
        let mut store = InMemoryStore::new();
        store.insert("t", one_col_df("a"));
        store.insert("t", one_col_df("b"));
        let got = store.get("t").unwrap();
        // The second insert replaced the first: column name is "b".
        assert_eq!(got.get_column_names(), vec!["b"]);
    }

    #[test]
    fn store_can_be_held_inside_calculator_context() {
        let mut store = InMemoryStore::new();
        store.insert("sourceUseTypePopulation", one_col_df("col"));
        let ctx = CalculatorContext::with_tables(store);
        assert!(ctx
            .tables()
            .store
            .contains("sourceUseTypePopulation"));
    }
}

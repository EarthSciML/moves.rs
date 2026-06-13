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

    /// Return a mutable reference to the DataFrame stored under `name`, or
    /// `None` if absent. Uses `Arc::make_mut` to ensure exclusive ownership;
    /// if the Arc has multiple owners the value is cloned first.
    pub fn get_mut(&mut self, name: &str) -> Option<&mut DataFrame> {
        let lower = name.to_ascii_lowercase();
        self.map.get_mut(lower.as_str()).map(Arc::make_mut)
    }

    /// Whether the store holds no tables.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Copy every table into `dest` (cheap `Arc` clones — no DataFrame deep
    /// copy), replacing any same-named entry. Used to promote generator
    /// scratch output into the slow tier so calculators that read via
    /// [`crate::CalculatorContext::tables`] observe it.
    pub fn copy_into(&self, dest: &mut InMemoryStore) {
        for (name, df) in &self.map {
            dest.map.insert(name.clone(), Arc::clone(df));
        }
    }

    /// Like [`copy_into`](Self::copy_into), but never replaces a **non-empty**
    /// `dest` table with an **empty** source table.
    ///
    /// A captured snapshot supplies the authoritative activity/rate tables
    /// (`SHO`, `Starts`, …) in the slow tier. When a generator that re-derives
    /// one of these tables is co-chunked with a downstream calculator (because
    /// some *other* output of the generator links them), `promote_scratch`
    /// would otherwise overwrite the captured value with the generator's
    /// snapshot-incomplete recomputation — often an empty frame — and the
    /// calculator then sees zero activity. The chunk-graph already skips
    /// producer→consumer edges for slow-tier tables to avoid exactly this
    /// (see `chunk_chains`), but that does not prevent the clobber when the
    /// generator co-chunks via a different table. Guarding the promotion here
    /// closes that gap: an empty generic recomputation can never destroy a
    /// non-empty authoritative table. A non-empty recomputation still wins (it
    /// is the legitimate default-DB path where the generator *is* the source).
    pub fn copy_into_preserving_nonempty(&self, dest: &mut InMemoryStore) {
        for (name, df) in &self.map {
            if df.height() == 0 {
                if let Some(existing) = dest.map.get(name) {
                    if existing.height() > 0 {
                        continue;
                    }
                }
            }
            dest.map.insert(name.clone(), Arc::clone(df));
        }
    }
}

impl DataFrameStore for InMemoryStore {
    fn get(&self, name: &str) -> Option<Arc<DataFrame>> {
        let lower = name.to_ascii_lowercase();
        self.map.get(lower.as_str()).cloned()
    }

    fn insert(&mut self, name: impl Into<String>, df: DataFrame) {
        let mut key = name.into();
        key.make_ascii_lowercase();
        self.map.insert(key, Arc::new(df));
    }

    fn contains(&self, name: &str) -> bool {
        let lower = name.to_ascii_lowercase();
        self.map.contains_key(lower.as_str())
    }

    fn names(&self) -> Vec<&str> {
        self.map.keys().map(String::as_str).collect()
    }
}

#[cfg(test)]
mod tests {
    use polars::prelude::*;

    use super::*;
    use crate::{data::DataFrameStore, CalculatorContext};

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
        let got = store
            .get("sourceUseTypePopulation")
            .expect("should be present");
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
        assert!(ctx.tables().contains("sourceUseTypePopulation"));
    }

    #[test]
    fn column_views_returns_arc_backed_series_without_deep_copy() {
        use crate::data::DataFrameStoreTyped;

        let mut store = InMemoryStore::new();
        let df = DataFrame::new(
            3,
            vec![
                Series::new("a".into(), [1i32, 2, 3]).into(),
                Series::new("b".into(), [4i32, 5, 6]).into(),
                Series::new("c".into(), [7i32, 8, 9]).into(),
            ],
        )
        .unwrap();
        store.insert("t", df);

        let views = store
            .column_views("t", &["a", "c"])
            .expect("column_views failed");
        assert_eq!(views.len(), 2);
        assert_eq!(views[0].name().as_str(), "a");
        assert_eq!(views[1].name().as_str(), "c");
        let a = views[0].i32().unwrap();
        assert_eq!(a.get(0), Some(1));
        assert_eq!(a.get(2), Some(3));
    }

    #[test]
    fn column_views_case_insensitive_lookup() {
        use crate::data::DataFrameStoreTyped;

        let mut store = InMemoryStore::new();
        let df =
            DataFrame::new(2, vec![Series::new("hourDayID".into(), [85i32, 86]).into()]).unwrap();
        store.insert("SHO", df);

        // Request with lowercase; should find the mixed-case column.
        let views = store
            .column_views("SHO", &["hourdayid"])
            .expect("column_views failed");
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].i32().unwrap().get(0), Some(85));
    }

    #[test]
    fn column_views_missing_column_returns_error() {
        use crate::data::DataFrameStoreTyped;

        let mut store = InMemoryStore::new();
        store.insert("t", one_col_df("x"));
        let err = store.column_views("t", &["y"]).unwrap_err();
        assert!(err.to_string().contains("'y'"), "got: {err}");
    }
}

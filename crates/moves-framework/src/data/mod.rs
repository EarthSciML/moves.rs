//! DataFrame-backed storage for the execution database.
//!
//! Task 50 (`DataFrameStore`) — replaces the placeholder
//! `ExecutionTables { _private: () }` and `ScratchNamespace { _private: () }`
//! shapes with concrete [`InMemoryStore`]-backed types.
//!
//! # Types
//!
//! * [`TableSchema`] — static name + column schema; used by Task 24
//!   (`InputDataManager`) to declare which tables may appear in the slow tier.
//! * [`TableHandle`] — a resolved `(name, Arc<DataFrame>)` pair returned from
//!   store lookups.
//! * [`DataFrameStore`] — the trait that both the slow and scratch tiers
//!   implement. Single-method generic so implementations can be swapped for
//!   testing without touching calling code.
//! * [`InMemoryStore`] — the concrete `BTreeMap`-backed implementation used by
//!   [`crate::ExecutionTables`] and [`crate::ScratchNamespace`].

pub mod store;

pub use store::InMemoryStore;

use std::sync::Arc;

use polars::prelude::{DataFrame, SchemaRef};

/// Static declaration of one DataFrame table's schema.
///
/// A schema entry records the *contract* — the table's canonical name and the
/// column types it must carry — not the data itself. Task 24
/// (`InputDataManager`) reads these to validate loaded DataFrames; Task 19
/// (`CalculatorRegistry`) checks calculator `input_tables` declarations against
/// registered schemas to catch typos at startup.
///
/// Schema validation against store contents is deferred to T3.
#[derive(Debug, Clone)]
pub struct TableSchema {
    /// Canonical name matching the default-DB table or scratch-table declaration
    /// (e.g. `"sourceUseTypePopulation"`).
    pub name: &'static str,
    /// Expected column names and types. Built once at startup from the
    /// characterization catalogue; individual store inserts are not yet
    /// validated against this (T3).
    pub schema: SchemaRef,
}

impl TableSchema {
    /// Construct a schema entry.
    #[must_use]
    pub fn new(name: &'static str, schema: SchemaRef) -> Self {
        Self { name, schema }
    }
}

/// A resolved `(name, data)` pair returned from a [`DataFrameStore`] lookup.
///
/// The `Arc<DataFrame>` allows cheap cloning so multiple calculators can hold
/// references to the same loaded table without copying data.
#[derive(Debug, Clone)]
pub struct TableHandle {
    /// Canonical table name.
    pub name: String,
    /// The table's data, shared behind an `Arc` to avoid copies.
    pub data: Arc<DataFrame>,
}

impl TableHandle {
    /// Construct a handle from a name and a shared DataFrame.
    #[must_use]
    pub fn new(name: impl Into<String>, data: Arc<DataFrame>) -> Self {
        Self {
            name: name.into(),
            data,
        }
    }
}

/// Trait for name-keyed DataFrame stores.
///
/// Both the slow tier ([`crate::ExecutionTables`]) and the scratch tier
/// ([`crate::ScratchNamespace`]) implement this trait via their inner
/// [`InMemoryStore`]. The trait surface is intentionally narrow —
/// `get`, `insert`, `contains`, `names` — so alternative implementations
/// (e.g. a read-only Parquet-backed store for tests) are easy to write.
pub trait DataFrameStore {
    /// Return the DataFrame stored under `name`, or `None` if absent.
    fn get(&self, name: &str) -> Option<Arc<DataFrame>>;

    /// Insert `df` under `name`, replacing any existing entry with the same
    /// name. The store takes ownership of `df` and wraps it in an `Arc`.
    fn insert(&mut self, name: impl Into<String>, df: DataFrame);

    /// Return `true` when a table named `name` is present in the store.
    fn contains(&self, name: &str) -> bool;

    /// All table names currently in the store, in sorted order.
    fn names(&self) -> Vec<&str>;
}

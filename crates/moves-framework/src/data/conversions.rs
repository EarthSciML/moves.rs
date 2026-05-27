//! Typed DataFrame conversions for [`DataFrameStore`].
//!
//! # `TableRow` trait
//!
//! [`TableRow`] is the contract that typed row structs implement to participate
//! in the [`DataFrameStoreTyped`] extension. Each implementor declares:
//!
//! - its canonical table name ([`TableRow::table_name`])
//! - its Polars column schema ([`TableRow::polars_schema`])
//! - how to serialise a `Vec<Self>` into a [`DataFrame`]
//!   ([`TableRow::into_dataframe`])
//! - how to deserialise a [`DataFrame`] back into `Vec<Self>`
//!   ([`TableRow::from_dataframe`])
//!
//! # `DataFrameStoreTyped` extension
//!
//! [`DataFrameStoreTyped`] is a blanket extension over any [`DataFrameStore`]
//! implementation. It adds:
//!
//! - [`DataFrameStoreTyped::insert_typed`] — validates the row type's schema
//!   against the [`schema_registry`] entry (if the entry is non-empty), then
//!   serialises and inserts the DataFrame.
//! - [`DataFrameStoreTyped::iter_typed`] — retrieves the DataFrame for a table
//!   and deserialises it to `Vec<R>`.
//!
//! # `IntoDataFrame` helper
//!
//! The blanket impl of [`IntoDataFrame`] for `Vec<R: TableRow>` provides a
//! one-call path:
//! ```rust,ignore
//! let df: polars::prelude::DataFrame = rows.into_dataframe()?;
//! ```

use polars::prelude::{DataFrame, PolarsResult, Schema};

use crate::data::schema_registry::schema_registry;
use crate::data::DataFrameStore;
use crate::error::{Error, Result};

/// Contract for typed row structs that can round-trip through a
/// [`DataFrameStore`].
///
/// Implementors live in `moves-calculators` (one per calculator's `*Row`
/// type); the test in [`crate::data::schema_registry`] provides a minimal
/// in-framework example.
pub trait TableRow: Sized {
    /// Canonical table name matching the schema registry key and the
    /// `Calculator::input_tables()` declaration.
    fn table_name() -> &'static str;

    /// Polars schema — column names and `DataType`s in the order
    /// [`into_dataframe`](Self::into_dataframe) writes them.
    ///
    /// Must be consistent with the registry entry for
    /// [`table_name`](Self::table_name) when the registry entry is
    /// non-empty (i.e., the schema has been catalogued). Validated by
    /// [`DataFrameStoreTyped::insert_typed`].
    fn polars_schema() -> Schema;

    /// Serialise a row collection into a Polars `DataFrame`.
    ///
    /// Column order, names, and types must match [`polars_schema`](Self::polars_schema).
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame>;

    /// Deserialise every row from `df`.
    ///
    /// Implementors should return [`Error::RowExtraction`] when a column is
    /// missing or a value is null.
    fn from_dataframe(df: &DataFrame) -> Result<Vec<Self>>;
}

/// One-call conversion from `Vec<R>` to [`DataFrame`].
pub trait IntoDataFrame {
    /// Convert `self` into a Polars `DataFrame`.
    fn into_dataframe(self) -> PolarsResult<DataFrame>;
}

impl<R: TableRow> IntoDataFrame for Vec<R> {
    fn into_dataframe(self) -> PolarsResult<DataFrame> {
        R::into_dataframe(self)
    }
}

/// Typed insert/iter extension over any [`DataFrameStore`].
///
/// Blanket-implemented for all `DataFrameStore` implementations; call-sites
/// need `use crate::data::DataFrameStoreTyped` or the re-exported path.
pub trait DataFrameStoreTyped: DataFrameStore {
    /// Serialise `rows` into a `DataFrame` and store it under
    /// `R::table_name()`.
    ///
    /// # Schema validation
    ///
    /// If the schema registry contains a **non-empty** schema for
    /// `R::table_name()`, the column names in the registry schema are compared
    /// with those in `R::polars_schema()`. A mismatch returns
    /// [`Error::SchemaMismatch`] before any insert is attempted.
    ///
    /// Tables with an empty registry schema (not yet catalogued) bypass
    /// validation.
    fn insert_typed<R: TableRow>(&mut self, rows: Vec<R>) -> Result<()> {
        let name = R::table_name();
        let row_schema = R::polars_schema();

        // Schema validation against registry (skip if registry entry is empty)
        let registry = schema_registry();
        if let Some(schema_fn) = registry.get(name) {
            let reg_schema = schema_fn();
            if !reg_schema.is_empty() {
                let expected: Vec<String> =
                    reg_schema.iter().map(|(col, _)| col.to_string()).collect();
                let actual: Vec<String> =
                    row_schema.iter().map(|(col, _)| col.to_string()).collect();
                if expected != actual {
                    return Err(Error::SchemaMismatch {
                        table: name.to_string(),
                        expected,
                        actual,
                    });
                }
            }
        }

        let df = R::into_dataframe(rows).map_err(|e| Error::Polars(e.to_string()))?;
        self.insert(name, df);
        Ok(())
    }

    /// Retrieve the DataFrame for `name` and deserialise it as `Vec<R>`.
    ///
    /// Returns an error if no table named `name` exists in the store.
    fn iter_typed<R: TableRow>(&self, name: &str) -> Result<Vec<R>> {
        let arc_df = self
            .get(name)
            .ok_or_else(|| Error::Polars(format!("table '{name}' not found in store")))?;
        R::from_dataframe(&arc_df)
    }
}

impl<S: DataFrameStore> DataFrameStoreTyped for S {}

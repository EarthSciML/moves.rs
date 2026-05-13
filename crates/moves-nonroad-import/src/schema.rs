//! Column schema and per-cell validation rules.
//!
//! Each importer describes its table as a slice of [`Column`] descriptors.
//! The descriptors are static (`&'static str` names, no allocation) so the
//! schema definition reads close to the underlying MariaDB DDL — e.g. the
//! population table in `CreateNRDefault.sql` is reproduced as a 4-element
//! slice in [`crate::tables::population`].
//!
//! `required` and `rule` are independent concerns:
//!
//! * `required: true` rejects empty cells outright. Used for primary-key
//!   columns and `NOT NULL` columns in the DDL.
//! * `rule` constrains the *value* and is only applied when the cell is
//!   non-empty. This keeps "nullable but non-negative when present" — a
//!   common Nonroad pattern (`annualFractionRetrofit float DEFAULT NULL`
//!   with a [0, 1] business rule) — expressible without a combinatorial
//!   rule explosion.
//!
//! The arrow-type set is intentionally narrow (Int64 / Float64 / Utf8). The
//! Nonroad input tables use `smallint(6)`, `int`, `float`, and `char(N)` —
//! all of which map to one of those three. Single-character flag columns
//! (e.g. `NRSourceUseType.isPumpFilled char(1)`) stay Utf8 here; the
//! downstream consumer interprets the character.

use arrow::datatypes::DataType;

/// One column in a Nonroad input table.
#[derive(Debug, Clone)]
pub struct Column {
    /// Column name. Case-matched against the user's CSV header and
    /// preserved verbatim into Parquet so the schema matches the
    /// Nonroad default-DB layout (`CreateNRDefault.sql`).
    pub name: &'static str,
    /// Original MariaDB type. Carried for the manifest; not used during
    /// conversion.
    pub mysql_type: &'static str,
    pub arrow_type: DataType,
    pub primary_key: bool,
    /// Reject NULL outright. Set on every PK column and every column
    /// declared `NOT NULL` in the DDL.
    pub required: bool,
    /// Constraint applied to the *value* once parsed. Bypassed for NULL
    /// cells (`required: false` columns can opt out of value checks
    /// entirely by leaving the cell blank).
    pub rule: Rule,
}

/// Per-cell value rule. Applied after the CSV cell has been parsed to
/// the declared `arrow_type`; range checks therefore see typed values
/// rather than strings.
#[derive(Debug, Clone, Copy)]
pub enum Rule {
    /// No value constraint.
    None,
    /// Integer in `[lo, hi]` (inclusive). Used for `monthID ∈ [1, 12]`
    /// and similar bounded identifiers.
    IntRange { lo: i64, hi: i64 },
    /// Float in `[lo, hi]` (inclusive). Used for fractional columns
    /// (`monthFraction ∈ [0, 1]`).
    FloatRange { lo: f64, hi: f64 },
    /// Numeric ≥ 0. `population`, `growthIndex`, `surrogateQuant`.
    NonNegative,
}

/// Optional cross-row invariant applied after all rows are parsed. Today
/// the only invariant is "fractions sum to 1.0 per group", which models
/// MOVES's per-(NREquipTypeID, stateID) month-fraction summation rule.
///
/// The default tolerance is 1e-3 because float CSV → f64 round-trips
/// pick up the same noise the MariaDB-side summation does; tighter
/// tolerances reject hand-edited templates that visually sum to 1.0
/// but accumulate per-row representation error.
#[derive(Debug, Clone)]
pub enum CrossRowInvariant {
    FractionSum {
        fraction_column: &'static str,
        group_columns: &'static [&'static str],
        tolerance: f64,
    },
}

/// Full table descriptor: name + columns + invariants.
#[derive(Debug, Clone)]
pub struct TableSchema {
    /// Lower-case table name, matching the Nonroad default-DB convention.
    pub name: &'static str,
    pub columns: &'static [Column],
    pub invariants: &'static [CrossRowInvariant],
}

impl TableSchema {
    /// Names of the columns flagged `primary_key: true`, in declaration order.
    pub fn primary_key(&self) -> Vec<&'static str> {
        self.columns
            .iter()
            .filter(|c| c.primary_key)
            .map(|c| c.name)
            .collect()
    }

    /// Index of `name` in `columns`, if present.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }
}

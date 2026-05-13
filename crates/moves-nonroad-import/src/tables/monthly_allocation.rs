//! `nrmonthallocation` importer (the Phase 4 "monthly throttle").
//!
//! Schema mirrors `CreateNRDefault.sql`:
//!
//! ```sql
//! CREATE TABLE NRMonthAllocation (
//!   NREquipTypeID smallint(6) NOT NULL,
//!   stateID       smallint(6) NOT NULL,
//!   monthID       smallint(6) NOT NULL,
//!   monthFraction float       NOT NULL,
//!   PRIMARY KEY (NREquipTypeID, stateID, monthID)
//! )
//! ```
//!
//! Validation rules:
//!
//! * `monthID` is `[1, 12]`.
//! * `stateID` is `[1, 99]`.
//! * `monthFraction` is `[0, 1]` per cell.
//! * **Cross-row invariant:** the per-`(NREquipTypeID, stateID)` sum of
//!   `monthFraction` must equal 1.0 within 1e-3. NEIQA flags violations
//!   of this invariant as a warning; we promote it to a hard error
//!   because downstream apportionment quietly miscounts when the sum
//!   drifts.

use arrow::datatypes::DataType;

use crate::schema::{Column, CrossRowInvariant, Rule, TableSchema};

static COLUMNS: &[Column] = &[
    Column {
        name: "NREquipTypeID",
        mysql_type: "smallint(6)",
        arrow_type: DataType::Int64,
        primary_key: true,
        required: true,
        rule: Rule::None,
    },
    Column {
        name: "stateID",
        mysql_type: "smallint(6)",
        arrow_type: DataType::Int64,
        primary_key: true,
        required: true,
        rule: Rule::IntRange { lo: 1, hi: 99 },
    },
    Column {
        name: "monthID",
        mysql_type: "smallint(6)",
        arrow_type: DataType::Int64,
        primary_key: true,
        required: true,
        rule: Rule::IntRange { lo: 1, hi: 12 },
    },
    Column {
        name: "monthFraction",
        mysql_type: "float",
        arrow_type: DataType::Float64,
        primary_key: false,
        required: true,
        rule: Rule::FloatRange { lo: 0.0, hi: 1.0 },
    },
];

static INVARIANTS: &[CrossRowInvariant] = &[CrossRowInvariant::FractionSum {
    fraction_column: "monthFraction",
    group_columns: &["NREquipTypeID", "stateID"],
    tolerance: 1e-3,
}];

pub static SCHEMA: TableSchema = TableSchema {
    name: "nrmonthallocation",
    columns: COLUMNS,
    invariants: INVARIANTS,
};

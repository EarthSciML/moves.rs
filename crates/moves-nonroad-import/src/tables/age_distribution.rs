//! `nrengtechfraction` importer (the Nonroad analogue of the on-road
//! `AgeDistribution` table).
//!
//! Schema mirrors `CreateNRDefault.sql`:
//!
//! ```sql
//! CREATE TABLE NREngTechFraction (
//!   sourceTypeID    smallint(6) NOT NULL,
//!   modelYearID     smallint(6) NOT NULL,
//!   processID       smallint(6) NOT NULL,
//!   engTechID       smallint(6) NOT NULL,
//!   NREngTechFraction float     NOT NULL,
//!   PRIMARY KEY (sourceTypeID, modelYearID, processID, engTechID)
//! )
//! ```
//!
//! "Age distribution" in the Phase 4 task description maps onto this
//! table because a Nonroad equipment cohort is identified by its
//! `modelYearID`; the per-(sourceTypeID, modelYearID, processID) total
//! over `engTechID` is the engine-technology distribution at that age.
//!
//! Validation rules:
//!
//! * `modelYearID` is bounded `[1950, 2099]`. The wider bound matches
//!   default-DB coverage (model-year fidelity tests use `2024..=2031`).
//! * `processID` is bounded `[1, 99]` (MOVES process IDs).
//! * `NREngTechFraction` is `[0, 1]` per cell.
//! * **Cross-row invariant:** the engTechID fractions must sum to 1.0
//!   per (sourceTypeID, modelYearID, processID) tuple — this matches
//!   the MOVES NREngTechFraction summation rule that NEIQA assumes
//!   downstream.

use arrow::datatypes::DataType;

use crate::schema::{Column, CrossRowInvariant, Rule, TableSchema};

static COLUMNS: &[Column] = &[
    Column {
        name: "sourceTypeID",
        mysql_type: "smallint(6)",
        arrow_type: DataType::Int64,
        primary_key: true,
        required: true,
        rule: Rule::None,
    },
    Column {
        name: "modelYearID",
        mysql_type: "smallint(6)",
        arrow_type: DataType::Int64,
        primary_key: true,
        required: true,
        rule: Rule::IntRange { lo: 1950, hi: 2099 },
    },
    Column {
        name: "processID",
        mysql_type: "smallint(6)",
        arrow_type: DataType::Int64,
        primary_key: true,
        required: true,
        rule: Rule::IntRange { lo: 1, hi: 99 },
    },
    Column {
        name: "engTechID",
        mysql_type: "smallint(6)",
        arrow_type: DataType::Int64,
        primary_key: true,
        required: true,
        rule: Rule::None,
    },
    Column {
        name: "NREngTechFraction",
        mysql_type: "float",
        arrow_type: DataType::Float64,
        primary_key: false,
        required: true,
        rule: Rule::FloatRange { lo: 0.0, hi: 1.0 },
    },
];

static INVARIANTS: &[CrossRowInvariant] = &[CrossRowInvariant::FractionSum {
    fraction_column: "NREngTechFraction",
    group_columns: &["sourceTypeID", "modelYearID", "processID"],
    tolerance: 1e-3,
}];

pub static SCHEMA: TableSchema = TableSchema {
    name: "nrengtechfraction",
    columns: COLUMNS,
    invariants: INVARIANTS,
};

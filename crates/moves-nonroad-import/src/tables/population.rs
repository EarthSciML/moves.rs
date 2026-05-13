//! `nrbaseyearequippopulation` importer.
//!
//! Schema mirrors `CreateNRDefault.sql`:
//!
//! ```sql
//! CREATE TABLE NRBaseYearEquipPopulation (
//!   sourceTypeID smallint(6) NOT NULL,
//!   stateID      smallint(6) NOT NULL,
//!   population   float       DEFAULT NULL,
//!   NRBaseYearID smallint(6) NOT NULL,
//!   PRIMARY KEY (sourceTypeID, stateID)
//! )
//! ```
//!
//! Validation rules:
//!
//! * `sourceTypeID`, `stateID`, `NRBaseYearID` are NOT NULL.
//! * `stateID` falls in `[1, 99]` (FIPS state codes; NEIQA's per-state
//!   joins reject values outside this range).
//! * `population` is nullable (`DEFAULT NULL`); when present it must be
//!   ≥ 0. Java MOVES treats NULL and 0 differently downstream, so we
//!   preserve NULL rather than coercing to 0.

use arrow::datatypes::DataType;

use crate::schema::{Column, Rule, TableSchema};

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
        name: "stateID",
        mysql_type: "smallint(6)",
        arrow_type: DataType::Int64,
        primary_key: true,
        required: true,
        rule: Rule::IntRange { lo: 1, hi: 99 },
    },
    Column {
        name: "population",
        mysql_type: "float",
        arrow_type: DataType::Float64,
        primary_key: false,
        required: false,
        rule: Rule::NonNegative,
    },
    Column {
        name: "NRBaseYearID",
        mysql_type: "smallint(6)",
        arrow_type: DataType::Int64,
        primary_key: false,
        required: true,
        rule: Rule::None,
    },
];

pub static SCHEMA: TableSchema = TableSchema {
    name: "nrbaseyearequippopulation",
    columns: COLUMNS,
    invariants: &[],
};

//! `nrretrofitfactors` importer.
//!
//! Schema mirrors `CreateDefault.sql`:
//!
//! ```sql
//! CREATE TABLE  nrRetrofitFactors (
//!   retrofitStartYear         smallint(6) NOT NULL,
//!   retrofitEndYear           smallint(6) NOT NULL,
//!   StartModelYear            smallint(6) NOT NULL,
//!   EndModelYear              smallint(6) NOT NULL,
//!   SCC                       char(10)    NOT NULL,
//!   engTechID                 smallint(6) NOT NULL,
//!   hpMin                     smallint(6) NOT NULL,
//!   hpMax                     smallint(6) NOT NULL,
//!   pollutantID               smallint(6) NOT NULL,
//!   retrofitID                smallint(6) NOT NULL,
//!   annualFractionRetrofit    float       DEFAULT NULL,
//!   retrofitEffectiveFraction float       DEFAULT NULL,
//!   PRIMARY KEY (SCC, engTechID, hpMin, hpMax, pollutantID, retrofitID)
//! )
//! ```
//!
//! Validation rules:
//!
//! * Year columns are bounded `[1990, 2099]`. The lower bound matches
//!   the earliest retrofit fixtures shipped in EPA's reference inputs.
//! * `hpMin`, `hpMax` are NonNegative.
//! * `annualFractionRetrofit`, `retrofitEffectiveFraction` are nullable
//!   but `[0, 1]` when present (matching NEIQA's per-row range warning).
//! * Cross-row invariants: none. The retrofit table accumulates rather
//!   than partitions, so per-key fraction sums need not equal 1.0; that
//!   would over-constrain templates that genuinely allocate < 100 % of
//!   the population.

use arrow::datatypes::DataType;

use crate::schema::{Column, Rule, TableSchema};

static COLUMNS: &[Column] = &[
    Column {
        name: "retrofitStartYear",
        mysql_type: "smallint(6)",
        arrow_type: DataType::Int64,
        primary_key: false,
        required: true,
        rule: Rule::IntRange { lo: 1990, hi: 2099 },
    },
    Column {
        name: "retrofitEndYear",
        mysql_type: "smallint(6)",
        arrow_type: DataType::Int64,
        primary_key: false,
        required: true,
        rule: Rule::IntRange { lo: 1990, hi: 2099 },
    },
    Column {
        name: "StartModelYear",
        mysql_type: "smallint(6)",
        arrow_type: DataType::Int64,
        primary_key: false,
        required: true,
        rule: Rule::IntRange { lo: 1950, hi: 2099 },
    },
    Column {
        name: "EndModelYear",
        mysql_type: "smallint(6)",
        arrow_type: DataType::Int64,
        primary_key: false,
        required: true,
        rule: Rule::IntRange { lo: 1950, hi: 2099 },
    },
    Column {
        name: "SCC",
        mysql_type: "char(10)",
        arrow_type: DataType::Utf8,
        primary_key: true,
        required: true,
        rule: Rule::None,
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
        name: "hpMin",
        mysql_type: "smallint(6)",
        arrow_type: DataType::Int64,
        primary_key: true,
        required: true,
        rule: Rule::NonNegative,
    },
    Column {
        name: "hpMax",
        mysql_type: "smallint(6)",
        arrow_type: DataType::Int64,
        primary_key: true,
        required: true,
        rule: Rule::NonNegative,
    },
    Column {
        name: "pollutantID",
        mysql_type: "smallint(6)",
        arrow_type: DataType::Int64,
        primary_key: true,
        required: true,
        rule: Rule::None,
    },
    Column {
        name: "retrofitID",
        mysql_type: "smallint(6)",
        arrow_type: DataType::Int64,
        primary_key: true,
        required: true,
        rule: Rule::None,
    },
    Column {
        name: "annualFractionRetrofit",
        mysql_type: "float",
        arrow_type: DataType::Float64,
        primary_key: false,
        required: false,
        rule: Rule::FloatRange { lo: 0.0, hi: 1.0 },
    },
    Column {
        name: "retrofitEffectiveFraction",
        mysql_type: "float",
        arrow_type: DataType::Float64,
        primary_key: false,
        required: false,
        rule: Rule::FloatRange { lo: 0.0, hi: 1.0 },
    },
];

pub static SCHEMA: TableSchema = TableSchema {
    name: "nrretrofitfactors",
    columns: COLUMNS,
    invariants: &[],
};

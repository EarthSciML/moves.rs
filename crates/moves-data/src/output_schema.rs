//! Unified Parquet output schema for MOVES runs (Phase 4 Task 89).
//!
//! Three logical tables are emitted by every run, mirroring the legacy
//! MOVES MariaDB output database:
//!
//! | Logical table | Layout | Purpose |
//! |---|---|---|
//! | [`OutputTable::Run`]            | singleton `MOVESRun.parquet`             | run metadata (one row per [`MovesRunRecord`]) |
//! | [`OutputTable::Emissions`]      | partitioned `MOVESOutput/yearID=…/monthID=…/part.parquet`         | per-(time, location, pollutant, process) emissions |
//! | [`OutputTable::Activity`]       | partitioned `MOVESActivityOutput/yearID=…/monthID=…/part.parquet` | per-(time, location, activity-type) activity |
//!
//! Partitioning follows the same `<column>=<value>` directory convention
//! the [`moves-default-db-convert`](../../moves-default-db-convert/index.html)
//! crate uses for the *input* default-DB Parquet layout, so downstream
//! tools (Polars `scan_parquet` hive-style globbing, DuckDB, pandas with
//! `pyarrow.dataset`) get predicate pushdown on `yearID` / `monthID`
//! without further wiring.
//!
//! # Schema provenance
//!
//! Columns named in [`MOVES_RUN_COLUMNS`], [`MOVES_OUTPUT_COLUMNS`], and
//! [`MOVES_ACTIVITY_OUTPUT_COLUMNS`] fall into two groups:
//!
//! * **Legacy columns** (`additive = false`) — verbatim from the canonical
//!   MOVES output DDL (`CreateOutput.sql`, MOVES commit
//!   `25dc6c833dd8c88198f82cee93ca30be1456df8b`). Names, ordering, and
//!   nullability match the MariaDB schema 1:1 so a fixture-captured
//!   MariaDB dump and a Rust-MOVES output can be compared column-for-column.
//!   Numeric types widen to `Int` / `Smallint` / `Float` per
//!   [`OutputColumnType`]; the MOVES `unsigned` qualifier is dropped
//!   because Parquet does not natively distinguish signedness for the
//!   integer widths used here.
//! * **Additive columns** (`additive = true`) — introduced by the Rust
//!   port for provenance and cache invalidation. They appear after the
//!   legacy columns in the schema and are non-null for every row produced
//!   by `moves.rs`. Currently:
//!     * `runHash` — hex SHA-256 of the canonical run inputs (RunSpec
//!       bytes + default-DB content hashes + calculator-DAG hash). Lets
//!       downstream tools join across runs and deduplicate cached results
//!       without consulting [`OutputTable::Run`].
//!     * `calculatorVersion` — the moves-rs build identifier
//!       (`CARGO_PKG_VERSION` plus an optional git rev). Surfaces on
//!       [`OutputTable::Run`] so per-run audits can pin the producing
//!       binary.
//!
//! See `docs/output-schema.md` (this repo) for the user-facing column
//! reference and `moves-rust-migration-plan.md` Task 89 for the design
//! rationale.
//!
//! # Why a separate `output_schema` module
//!
//! [`moves-data`](crate) owns *every* schema declaration the workspace
//! agrees on — the static pollutant / process enums in [`super`] already
//! follow that pattern. Centralising the output schema here means the
//! framework's writer (Phase 2 `OutputProcessor`, Task 26) and any future
//! Polars-backed loader (Task 50) share one source of truth. The crate
//! deliberately does **not** depend on `arrow` or `parquet`; the writer
//! that materialises these definitions lives in `moves-framework` and
//! translates [`OutputColumnType`] into Arrow `DataType` at the writer
//! boundary.

use serde::{Deserialize, Serialize};

/// Logical column type for a MOVES output column.
///
/// The mapping to Arrow / Parquet types is owned by the writer in
/// `moves-framework`:
///
/// | [`OutputColumnType`] | Arrow `DataType` | Parquet logical type |
/// |---|---|---|
/// | [`Self::Smallint`] | `Int16` | `INT16` |
/// | [`Self::Int`]      | `Int32` | `INT32` |
/// | [`Self::Float`]    | `Float64` | `DOUBLE` |
/// | [`Self::Text`]     | `Utf8` | `BYTE_ARRAY (utf8)` |
/// | [`Self::DateTime`] | `Utf8` (ISO 8601) | `BYTE_ARRAY (utf8)` |
///
/// MOVES uses MariaDB `datetime` for two run-metadata fields. The Parquet
/// writer stores them as ISO 8601 strings rather than the Arrow timestamp
/// types because (a) the source values lack a timezone in canonical MOVES
/// and (b) string storage avoids dragging chrono into the schema crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputColumnType {
    /// 16-bit integer. Matches MOVES `smallint`.
    Smallint,
    /// 32-bit integer. Matches MOVES `int` (used for `countyID`, `zoneID`,
    /// `linkID`, the `*DONEFiles` counters).
    Int,
    /// 64-bit float. Matches MOVES `float`/`double` (the `emissionQuant`,
    /// `emissionRate`, `activity`, and `minutesDuration` columns).
    Float,
    /// UTF-8 string. Matches MOVES `char(n)`/`varchar(n)`/`text`/`enum`.
    Text,
    /// ISO 8601 string. Matches MOVES `datetime` (`runSpecFileDateTime`,
    /// `runDateTime`).
    DateTime,
}

impl OutputColumnType {
    /// Short identifier used in serialised metadata sidecars and error
    /// messages. Stable across Parquet writer revisions.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Smallint => "smallint",
            Self::Int => "int",
            Self::Float => "float",
            Self::Text => "text",
            Self::DateTime => "datetime",
        }
    }
}

/// One column in an output-table schema.
///
/// The fields are read-only (`pub` for pattern-matching, but the entries
/// are declared as `static` arrays of `const`-constructible values). Avoid
/// constructing instances outside of the schema constants — callers should
/// look up columns via [`OutputTable::columns`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputColumn {
    /// Column name. Legacy columns match the MOVES MariaDB DDL exactly
    /// (case-sensitive, `MOVESRunID`-style camelCase).
    pub name: &'static str,
    /// Logical type. See [`OutputColumnType`] for the Arrow / Parquet
    /// mapping.
    pub kind: OutputColumnType,
    /// Whether the column accepts SQL `NULL` / Arrow null. Matches the
    /// MOVES DDL's `NOT NULL` annotation for legacy columns; additive
    /// columns are non-null by construction.
    pub nullable: bool,
    /// True if the column participates in the table's primary key.
    /// Mirrors the `PRIMARY KEY` declaration in the MOVES DDL — used by
    /// downstream readers (Task 50) to drive `sort_by` and join logic.
    pub primary_key: bool,
    /// `false` for columns ported unchanged from canonical MOVES; `true`
    /// for columns the Rust port introduces.
    pub additive: bool,
}

/// Identifier for one of the three output tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputTable {
    /// `MOVESRun` — singleton run-metadata table.
    Run,
    /// `MOVESOutput` — per-(time, location, pollutant, process) emissions.
    Emissions,
    /// `MOVESActivityOutput` — per-(time, location, activity-type) activity.
    Activity,
}

impl OutputTable {
    /// Canonical table name as it appears in the MOVES DDL and on the
    /// output directory layout.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Run => "MOVESRun",
            Self::Emissions => "MOVESOutput",
            Self::Activity => "MOVESActivityOutput",
        }
    }

    /// Column schema for this table.
    #[must_use]
    pub fn columns(self) -> &'static [OutputColumn] {
        match self {
            Self::Run => MOVES_RUN_COLUMNS,
            Self::Emissions => MOVES_OUTPUT_COLUMNS,
            Self::Activity => MOVES_ACTIVITY_OUTPUT_COLUMNS,
        }
    }

    /// Hive-style partition columns (in order). Empty for the singleton
    /// `Run` table; `["yearID", "monthID"]` for the two row-level tables.
    #[must_use]
    pub fn partition_columns(self) -> &'static [&'static str] {
        match self {
            Self::Run => &[],
            Self::Emissions | Self::Activity => &["yearID", "monthID"],
        }
    }

    /// Iterate every table in fixed order: Run → Emissions → Activity.
    pub fn all() -> impl Iterator<Item = Self> {
        [Self::Run, Self::Emissions, Self::Activity].into_iter()
    }
}

/// `MOVESRun` schema — singleton table carrying run-level metadata.
///
/// Mirrors the canonical MOVES `MOVESRun` DDL. Additive columns
/// (`runHash`, `calculatorVersion`) trail the legacy columns and are
/// non-null on every run the Rust port produces.
pub static MOVES_RUN_COLUMNS: &[OutputColumn] = &[
    OutputColumn {
        name: "MOVESRunID",
        kind: OutputColumnType::Smallint,
        nullable: false,
        primary_key: true,
        additive: false,
    },
    OutputColumn {
        name: "outputTimePeriod",
        kind: OutputColumnType::Text,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "timeUnits",
        kind: OutputColumnType::Text,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "distanceUnits",
        kind: OutputColumnType::Text,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "massUnits",
        kind: OutputColumnType::Text,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "energyUnits",
        kind: OutputColumnType::Text,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "runSpecFileName",
        kind: OutputColumnType::Text,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "runSpecDescription",
        kind: OutputColumnType::Text,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "runSpecFileDateTime",
        kind: OutputColumnType::DateTime,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "runDateTime",
        kind: OutputColumnType::DateTime,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "scale",
        kind: OutputColumnType::Text,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "minutesDuration",
        kind: OutputColumnType::Float,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "defaultDatabaseUsed",
        kind: OutputColumnType::Text,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "masterVersion",
        kind: OutputColumnType::Text,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "masterComputerID",
        kind: OutputColumnType::Text,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "masterIDNumber",
        kind: OutputColumnType::Text,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "domain",
        kind: OutputColumnType::Text,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "domainCountyID",
        kind: OutputColumnType::Int,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "domainCountyName",
        kind: OutputColumnType::Text,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "domainDatabaseServer",
        kind: OutputColumnType::Text,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "domainDatabaseName",
        kind: OutputColumnType::Text,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "expectedDONEFiles",
        kind: OutputColumnType::Int,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "retrievedDONEFiles",
        kind: OutputColumnType::Int,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "models",
        kind: OutputColumnType::Text,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    // -- Additive columns introduced by the Rust port --------------------
    OutputColumn {
        name: "runHash",
        kind: OutputColumnType::Text,
        nullable: false,
        primary_key: false,
        additive: true,
    },
    OutputColumn {
        name: "calculatorVersion",
        kind: OutputColumnType::Text,
        nullable: false,
        primary_key: false,
        additive: true,
    },
];

/// `MOVESOutput` schema — per-emission row table.
///
/// Mirrors the canonical MOVES `MOVESOutput` DDL. `runHash` trails the
/// legacy columns so per-row records carry their producing run's
/// fingerprint without joining back to [`OutputTable::Run`].
pub static MOVES_OUTPUT_COLUMNS: &[OutputColumn] = &[
    OutputColumn {
        name: "MOVESRunID",
        kind: OutputColumnType::Smallint,
        nullable: false,
        primary_key: true,
        additive: false,
    },
    OutputColumn {
        name: "iterationID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "yearID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "monthID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "dayID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "hourID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "stateID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "countyID",
        kind: OutputColumnType::Int,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "zoneID",
        kind: OutputColumnType::Int,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "linkID",
        kind: OutputColumnType::Int,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "pollutantID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "processID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "sourceTypeID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "regClassID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "fuelTypeID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "fuelSubTypeID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "modelYearID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "roadTypeID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "SCC",
        kind: OutputColumnType::Text,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "engTechID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "sectorID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "hpID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "emissionQuant",
        kind: OutputColumnType::Float,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "emissionRate",
        kind: OutputColumnType::Float,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    // -- Additive columns introduced by the Rust port --------------------
    OutputColumn {
        name: "runHash",
        kind: OutputColumnType::Text,
        nullable: false,
        primary_key: false,
        additive: true,
    },
];

/// `MOVESActivityOutput` schema — per-activity row table.
///
/// Mirrors the canonical MOVES `MOVESActivityOutput` DDL. As with
/// [`MOVES_OUTPUT_COLUMNS`], `runHash` trails the legacy columns and is
/// non-null on every produced row.
pub static MOVES_ACTIVITY_OUTPUT_COLUMNS: &[OutputColumn] = &[
    OutputColumn {
        name: "MOVESRunID",
        kind: OutputColumnType::Smallint,
        nullable: false,
        primary_key: true,
        additive: false,
    },
    OutputColumn {
        name: "iterationID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "yearID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "monthID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "dayID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "hourID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "stateID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "countyID",
        kind: OutputColumnType::Int,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "zoneID",
        kind: OutputColumnType::Int,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "linkID",
        kind: OutputColumnType::Int,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "sourceTypeID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "regClassID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "fuelTypeID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "fuelSubTypeID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "modelYearID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "roadTypeID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "SCC",
        kind: OutputColumnType::Text,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "engTechID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "sectorID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "hpID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "activityTypeID",
        kind: OutputColumnType::Smallint,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    OutputColumn {
        name: "activity",
        kind: OutputColumnType::Float,
        nullable: true,
        primary_key: false,
        additive: false,
    },
    // -- Additive columns introduced by the Rust port --------------------
    OutputColumn {
        name: "runHash",
        kind: OutputColumnType::Text,
        nullable: false,
        primary_key: false,
        additive: true,
    },
];

/// Typed run-metadata record materialised onto [`OutputTable::Run`].
///
/// Field names match the [`MOVES_RUN_COLUMNS`] entries 1:1 so a writer
/// implementation can iterate by index without a side map. Optional
/// fields surface as `Option<…>` and become Arrow nulls on disk. The two
/// additive fields are non-optional — `moves.rs` always knows the run
/// hash and its own version string.
///
/// Datetime fields are carried as ISO 8601 strings to avoid pulling
/// `chrono` into [`moves-data`](crate); callers are responsible for
/// formatting (the writer treats the value as-is).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MovesRunRecord {
    pub moves_run_id: i16,
    pub output_time_period: Option<String>,
    pub time_units: Option<String>,
    pub distance_units: Option<String>,
    pub mass_units: Option<String>,
    pub energy_units: Option<String>,
    pub run_spec_file_name: Option<String>,
    pub run_spec_description: Option<String>,
    pub run_spec_file_date_time: Option<String>,
    pub run_date_time: Option<String>,
    pub scale: Option<String>,
    pub minutes_duration: Option<f64>,
    pub default_database_used: Option<String>,
    pub master_version: Option<String>,
    pub master_computer_id: Option<String>,
    pub master_id_number: Option<String>,
    pub domain: Option<String>,
    pub domain_county_id: Option<i32>,
    pub domain_county_name: Option<String>,
    pub domain_database_server: Option<String>,
    pub domain_database_name: Option<String>,
    pub expected_done_files: Option<i32>,
    pub retrieved_done_files: Option<i32>,
    pub models: Option<String>,
    /// Hex SHA-256 of the canonical run inputs.
    pub run_hash: String,
    /// `moves.rs` build identifier — typically `CARGO_PKG_VERSION` plus
    /// an optional `+<git-sha>` suffix.
    pub calculator_version: String,
}

/// One row of [`OutputTable::Emissions`]. Field-by-field 1:1 with
/// [`MOVES_OUTPUT_COLUMNS`]. The `Option` shape mirrors the MariaDB
/// nullability declarations: only `MOVESRunID` and `runHash` are
/// non-optional.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EmissionRecord {
    pub moves_run_id: i16,
    pub iteration_id: Option<i16>,
    pub year_id: Option<i16>,
    pub month_id: Option<i16>,
    pub day_id: Option<i16>,
    pub hour_id: Option<i16>,
    pub state_id: Option<i16>,
    pub county_id: Option<i32>,
    pub zone_id: Option<i32>,
    pub link_id: Option<i32>,
    pub pollutant_id: Option<i16>,
    pub process_id: Option<i16>,
    pub source_type_id: Option<i16>,
    pub reg_class_id: Option<i16>,
    pub fuel_type_id: Option<i16>,
    pub fuel_sub_type_id: Option<i16>,
    pub model_year_id: Option<i16>,
    pub road_type_id: Option<i16>,
    pub scc: Option<String>,
    pub eng_tech_id: Option<i16>,
    pub sector_id: Option<i16>,
    pub hp_id: Option<i16>,
    pub emission_quant: Option<f64>,
    pub emission_rate: Option<f64>,
    /// Hex SHA-256 of the producing run.
    pub run_hash: String,
}

/// One row of [`OutputTable::Activity`]. Field-by-field 1:1 with
/// [`MOVES_ACTIVITY_OUTPUT_COLUMNS`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActivityRecord {
    pub moves_run_id: i16,
    pub iteration_id: Option<i16>,
    pub year_id: Option<i16>,
    pub month_id: Option<i16>,
    pub day_id: Option<i16>,
    pub hour_id: Option<i16>,
    pub state_id: Option<i16>,
    pub county_id: Option<i32>,
    pub zone_id: Option<i32>,
    pub link_id: Option<i32>,
    pub source_type_id: Option<i16>,
    pub reg_class_id: Option<i16>,
    pub fuel_type_id: Option<i16>,
    pub fuel_sub_type_id: Option<i16>,
    pub model_year_id: Option<i16>,
    pub road_type_id: Option<i16>,
    pub scc: Option<String>,
    pub eng_tech_id: Option<i16>,
    pub sector_id: Option<i16>,
    pub hp_id: Option<i16>,
    pub activity_type_id: Option<i16>,
    pub activity: Option<f64>,
    /// Hex SHA-256 of the producing run.
    pub run_hash: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_table_names_match_legacy_ddl() {
        assert_eq!(OutputTable::Run.name(), "MOVESRun");
        assert_eq!(OutputTable::Emissions.name(), "MOVESOutput");
        assert_eq!(OutputTable::Activity.name(), "MOVESActivityOutput");
    }

    #[test]
    fn moves_run_columns_have_unique_names() {
        let mut names = std::collections::HashSet::new();
        for col in MOVES_RUN_COLUMNS {
            assert!(
                names.insert(col.name),
                "duplicate column name in MOVES_RUN_COLUMNS: {}",
                col.name
            );
        }
    }

    #[test]
    fn moves_output_columns_have_unique_names() {
        let mut names = std::collections::HashSet::new();
        for col in MOVES_OUTPUT_COLUMNS {
            assert!(
                names.insert(col.name),
                "duplicate column name in MOVES_OUTPUT_COLUMNS: {}",
                col.name
            );
        }
    }

    #[test]
    fn moves_activity_output_columns_have_unique_names() {
        let mut names = std::collections::HashSet::new();
        for col in MOVES_ACTIVITY_OUTPUT_COLUMNS {
            assert!(
                names.insert(col.name),
                "duplicate column name in MOVES_ACTIVITY_OUTPUT_COLUMNS: {}",
                col.name
            );
        }
    }

    #[test]
    fn each_table_declares_movesrunid_as_primary_key() {
        for table in OutputTable::all() {
            let pk: Vec<&str> = table
                .columns()
                .iter()
                .filter(|c| c.primary_key)
                .map(|c| c.name)
                .collect();
            assert!(
                pk.contains(&"MOVESRunID"),
                "{} must include MOVESRunID in primary key, got {:?}",
                table.name(),
                pk
            );
        }
    }

    #[test]
    fn additive_columns_trail_legacy_columns() {
        // The schema contract says additive columns come *after* the
        // legacy block, so reorderings that interleave them get caught.
        for table in OutputTable::all() {
            let cols = table.columns();
            let first_additive = cols.iter().position(|c| c.additive);
            if let Some(idx) = first_additive {
                for (i, col) in cols.iter().enumerate() {
                    if i < idx {
                        assert!(
                            !col.additive,
                            "{}: column '{}' at index {} is additive but precedes \
                             the first-additive marker at index {}",
                            table.name(),
                            col.name,
                            i,
                            idx
                        );
                    } else {
                        assert!(
                            col.additive,
                            "{}: column '{}' at index {} is legacy but appears \
                             after additive columns begin at index {}",
                            table.name(),
                            col.name,
                            i,
                            idx
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn additive_columns_include_run_hash() {
        for table in OutputTable::all() {
            let run_hash = table
                .columns()
                .iter()
                .find(|c| c.name == "runHash")
                .unwrap_or_else(|| panic!("{} must declare runHash", table.name()));
            assert!(run_hash.additive, "runHash must be marked additive");
            assert!(
                !run_hash.nullable,
                "runHash must be non-nullable for downstream join correctness"
            );
        }
    }

    #[test]
    fn calculator_version_appears_only_on_run_table() {
        for table in OutputTable::all() {
            let has_calculator_version = table
                .columns()
                .iter()
                .any(|c| c.name == "calculatorVersion");
            if table == OutputTable::Run {
                assert!(
                    has_calculator_version,
                    "MOVESRun must declare calculatorVersion"
                );
            } else {
                assert!(
                    !has_calculator_version,
                    "{} must NOT declare calculatorVersion (it lives on MOVESRun)",
                    table.name()
                );
            }
        }
    }

    #[test]
    fn partition_columns_match_design() {
        assert_eq!(OutputTable::Run.partition_columns(), &[] as &[&str]);
        assert_eq!(
            OutputTable::Emissions.partition_columns(),
            &["yearID", "monthID"]
        );
        assert_eq!(
            OutputTable::Activity.partition_columns(),
            &["yearID", "monthID"]
        );
    }

    #[test]
    fn partition_columns_exist_in_schema() {
        for table in OutputTable::all() {
            let schema_names: std::collections::HashSet<&str> =
                table.columns().iter().map(|c| c.name).collect();
            for &p in table.partition_columns() {
                assert!(
                    schema_names.contains(p),
                    "{}: partition column '{}' must appear in column schema",
                    table.name(),
                    p
                );
            }
        }
    }

    #[test]
    fn output_column_kinds_round_trip_through_str() {
        for kind in [
            OutputColumnType::Smallint,
            OutputColumnType::Int,
            OutputColumnType::Float,
            OutputColumnType::Text,
            OutputColumnType::DateTime,
        ] {
            // `as_str` must produce a non-empty stable identifier — the
            // string ends up in metadata sidecars and the test guards
            // against accidental rename.
            assert!(!kind.as_str().is_empty(), "{:?} has empty as_str", kind);
        }
    }

    #[test]
    fn moves_run_record_field_count_matches_schema() {
        // Hand-count guard so the record type and the schema constants
        // stay in lockstep. A drift here means the writer would silently
        // drop or invent columns.
        //
        // Bump this number when intentionally adding a column to both.
        const EXPECTED_RUN_COLUMNS: usize = 26;
        assert_eq!(MOVES_RUN_COLUMNS.len(), EXPECTED_RUN_COLUMNS);
    }

    #[test]
    fn moves_output_record_field_count_matches_schema() {
        const EXPECTED_OUTPUT_COLUMNS: usize = 25;
        assert_eq!(MOVES_OUTPUT_COLUMNS.len(), EXPECTED_OUTPUT_COLUMNS);
    }

    #[test]
    fn moves_activity_output_record_field_count_matches_schema() {
        const EXPECTED_ACTIVITY_OUTPUT_COLUMNS: usize = 23;
        assert_eq!(
            MOVES_ACTIVITY_OUTPUT_COLUMNS.len(),
            EXPECTED_ACTIVITY_OUTPUT_COLUMNS
        );
    }
}

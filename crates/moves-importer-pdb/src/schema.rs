//! Per-table schema descriptors.
//!
//! Mirrors the Java `dataTableDescriptor` arrays declared on each
//! importer (`LinkImporter`, `LinkSourceTypeHourImporter`, …). Each
//! [`TableSchema`] knows the table's MOVES name, its column order
//! (matching the user-facing CSV template header), each column's
//! Arrow type, whether it's nullable, and which [`Filter`] (if any)
//! the importer applies.
//!
//! ## Type widening parity with the default-DB convert pipeline
//!
//! The Phase 4 Task 80 converter (`moves-default-db-convert::types`)
//! widens every MariaDB integer flavor to `Int64` and every floating-point
//! flavor to `Float64` so cross-stage joins line up. We use the same
//! widening here so importer Parquet matches default-DB Parquet for the
//! identically-named tables (`Link`, `linkSourceTypeHour`, etc).

use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use std::sync::Arc;

use crate::filter::Filter;

/// One column on the user-facing CSV template, with everything the
/// reader needs to coerce, validate, and emit it.
#[derive(Debug, Clone)]
pub struct Column {
    /// MOVES column name. Case is *display* — header matching is
    /// case-insensitive (Java's `equalsIgnoreCase`).
    pub name: &'static str,
    /// Arrow type emitted in Parquet output. We use widened types
    /// (`Int64`, `Float64`, `Utf8`) per the default-DB convention.
    pub data_type: DataType,
    /// `true` if the column is allowed to be empty in the input. Mirrors
    /// `metaData.isNullable` from `BasicDataHandler.doImport`.
    pub nullable: bool,
    /// Per-cell filter to apply after parsing. `None` is the
    /// empty-string sentinel Java uses for "no filter".
    pub filter: Option<Filter>,
}

/// Schema for one importer-managed table.
#[derive(Debug, Clone)]
pub struct TableSchema {
    /// MOVES table name as written to the manifest and Parquet path.
    /// Case matches `tables.json` so importer output and default-DB
    /// output sit at identical relative paths.
    pub name: &'static str,
    /// Columns in the order the CSV template lists them (also the
    /// order written to Parquet — we don't re-order columns).
    pub columns: &'static [Column],
}

impl TableSchema {
    /// Build the Arrow schema (every field is nullable so missing CSV
    /// cells flow through as Arrow null — matches the default-DB
    /// converter, which marks every Parquet field nullable to absorb
    /// MariaDB NULLs verbatim).
    pub fn arrow_schema(&self) -> SchemaRef {
        let fields: Vec<Field> = self
            .columns
            .iter()
            .map(|c| Field::new(c.name, c.data_type.clone(), true))
            .collect();
        Arc::new(ArrowSchema::new(fields))
    }

    /// Find a column by case-insensitive name. Returns `(index,
    /// &Column)`. Mirrors the header-matching pass in
    /// `BasicDataHandler.doImport` (around line 837 in
    /// `BasicDataHandler.java`).
    pub fn find_column(&self, name: &str) -> Option<(usize, &Column)> {
        self.columns
            .iter()
            .enumerate()
            .find(|(_, c)| c.name.eq_ignore_ascii_case(name))
    }
}

// -----------------------------------------------------------------------
// Project-only table descriptors. Each mirrors the Java importer's
// `dataTableDescriptor` triple-array: (column, lookup-table, filter).
// We drop the lookup-table entry — that was only used by the Java GUI
// to build dropdown picklists from the default DB and isn't load-path
// state.
// -----------------------------------------------------------------------

/// `Link` — primary table for `LinkImporter`.
///
/// Java descriptor: `LinkImporter.dataTableDescriptor` (lines 43-54 of
/// `LinkImporter.java`). MariaDB types from `tables.json`: `linkID
/// integer`, `countyID integer`, `zoneID integer`, `roadTypeID
/// smallint`, `linkLength float`, `linkVolume float`, `linkAvgSpeed
/// float`, `linkDescription varchar(50)`, `linkAvgGrade float`.
pub const LINK: TableSchema = TableSchema {
    name: "Link",
    columns: &[
        Column {
            name: "linkID",
            data_type: DataType::Int64,
            nullable: false,
            filter: None,
        },
        Column {
            name: "countyID",
            data_type: DataType::Int64,
            nullable: false,
            filter: Some(Filter::County),
        },
        Column {
            name: "zoneID",
            data_type: DataType::Int64,
            nullable: false,
            filter: Some(Filter::Zone),
        },
        Column {
            name: "roadTypeID",
            data_type: DataType::Int64,
            nullable: false,
            filter: Some(Filter::RoadType),
        },
        Column {
            name: "linkLength",
            data_type: DataType::Float64,
            nullable: true,
            filter: Some(Filter::NonNegativeFloat),
        },
        Column {
            name: "linkVolume",
            data_type: DataType::Float64,
            nullable: true,
            filter: Some(Filter::NonNegativeFloat),
        },
        Column {
            name: "linkAvgSpeed",
            data_type: DataType::Float64,
            nullable: true,
            filter: Some(Filter::NonNegativeFloat),
        },
        Column {
            name: "linkDescription",
            data_type: DataType::Utf8,
            nullable: true,
            filter: None,
        },
        Column {
            name: "linkAvgGrade",
            data_type: DataType::Float64,
            nullable: true,
            filter: None,
        },
    ],
};

/// `linkSourceTypeHour` — primary table for `LinkSourceTypeHourImporter`.
///
/// Java descriptor: `LinkSourceTypeHourImporter.dataTableDescriptor`
/// (lines 43-48 of `LinkSourceTypeHourImporter.java`). The
/// `sourceTypeHourFraction` column carries the
/// `FILTER_NON_NEGATIVE` Java declares — Java itself enforces the
/// "must sum to 1.0 per linkID" rule in `getProjectDataStatus`, not
/// in the cell filter.
pub const LINK_SOURCE_TYPE_HOUR: TableSchema = TableSchema {
    name: "linkSourceTypeHour",
    columns: &[
        Column {
            name: "linkID",
            data_type: DataType::Int64,
            nullable: false,
            filter: None,
        },
        Column {
            name: "sourceTypeID",
            data_type: DataType::Int64,
            nullable: false,
            filter: Some(Filter::SourceType),
        },
        Column {
            name: "sourceTypeHourFraction",
            data_type: DataType::Float64,
            nullable: false,
            filter: Some(Filter::NonNegativeFloat),
        },
    ],
};

/// `driveScheduleSecondLink` — primary table for
/// `DriveScheduleSecondLinkImporter`.
///
/// Java descriptor: `DriveScheduleSecondLinkImporter.dataTableDescriptor`
/// (lines 42-48 of `DriveScheduleSecondLinkImporter.java`). The Java
/// importer leaves grade unfiltered and only enforces `secondID >= 0`
/// and `speed >= 0` via `FILTER_NON_NEGATIVE`.
pub const DRIVE_SCHEDULE_SECOND_LINK: TableSchema = TableSchema {
    name: "driveScheduleSecondLink",
    columns: &[
        Column {
            name: "linkID",
            data_type: DataType::Int64,
            nullable: false,
            filter: None,
        },
        Column {
            name: "secondID",
            data_type: DataType::Int64,
            nullable: false,
            filter: Some(Filter::NonNegativeFloat),
        },
        Column {
            name: "speed",
            data_type: DataType::Float64,
            nullable: false,
            filter: Some(Filter::NonNegativeFloat),
        },
        Column {
            name: "grade",
            data_type: DataType::Float64,
            nullable: true,
            filter: None,
        },
    ],
};

/// `offNetworkLink` — primary table for `OffNetworkLinkImporter`.
///
/// Java descriptor: `OffNetworkLinkImporter.dataTableDescriptor`
/// (lines 45-53 of `OffNetworkLinkImporter.java`).
pub const OFF_NETWORK_LINK: TableSchema = TableSchema {
    name: "offNetworkLink",
    columns: &[
        Column {
            name: "zoneID",
            data_type: DataType::Int64,
            nullable: false,
            filter: Some(Filter::Zone),
        },
        Column {
            name: "sourceTypeID",
            data_type: DataType::Int64,
            nullable: false,
            filter: Some(Filter::SourceType),
        },
        Column {
            name: "vehiclePopulation",
            data_type: DataType::Float64,
            nullable: false,
            filter: Some(Filter::NonNegativeFloat),
        },
        Column {
            name: "startFraction",
            data_type: DataType::Float64,
            nullable: false,
            filter: Some(Filter::NonNegativeFloat),
        },
        Column {
            name: "extendedIdleFraction",
            data_type: DataType::Float64,
            nullable: false,
            filter: Some(Filter::NonNegativeFloat),
        },
        Column {
            name: "parkedVehicleFraction",
            data_type: DataType::Float64,
            nullable: false,
            filter: Some(Filter::NonNegativeFloat),
        },
    ],
};

/// `OpModeDistribution` — primary table for `LinkOpmodeDistributionImporter`.
///
/// Java descriptor: `LinkOpmodeDistributionImporter.dataTableDescriptor`
/// (lines 44-52 of `LinkOpmodeDistributionImporter.java`). The Java
/// importer's table filename in the project domain is
/// `opModeDistribution`, but the Parquet table name we emit matches
/// the default-DB capitalization (`OpModeDistribution`) for cross-stage
/// schema parity — the default-DB lazy reader uses the
/// `OpModeDistribution` directory.
pub const OP_MODE_DISTRIBUTION: TableSchema = TableSchema {
    name: "OpModeDistribution",
    columns: &[
        Column {
            name: "sourceTypeID",
            data_type: DataType::Int64,
            nullable: false,
            filter: Some(Filter::SourceType),
        },
        Column {
            name: "hourDayID",
            data_type: DataType::Int64,
            nullable: false,
            filter: Some(Filter::HourDay),
        },
        Column {
            name: "linkID",
            data_type: DataType::Int64,
            nullable: false,
            filter: None,
        },
        Column {
            name: "polProcessID",
            data_type: DataType::Int64,
            nullable: false,
            filter: Some(Filter::PolProcess),
        },
        Column {
            name: "opModeID",
            data_type: DataType::Int64,
            nullable: false,
            filter: Some(Filter::OpMode),
        },
        Column {
            name: "opModeFraction",
            data_type: DataType::Float64,
            nullable: false,
            filter: Some(Filter::NonNegativeFloat),
        },
    ],
};

/// All project-only table schemas, in the order
/// `ImporterInstantiator` lists them at the top of `idInfo` (Java
/// puts project-domain importers first so the GUI tabs render in
/// project-workflow order).
pub const ALL_PROJECT_TABLES: &[&TableSchema] = &[
    &LINK,
    &LINK_SOURCE_TYPE_HOUR,
    &DRIVE_SCHEDULE_SECOND_LINK,
    &OFF_NETWORK_LINK,
    &OP_MODE_DISTRIBUTION,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schemas_have_unique_column_names() {
        for table in ALL_PROJECT_TABLES {
            let mut names: Vec<&str> = table.columns.iter().map(|c| c.name).collect();
            names.sort();
            let original_len = names.len();
            names.dedup();
            assert_eq!(
                names.len(),
                original_len,
                "table {} has duplicate column names",
                table.name
            );
        }
    }

    #[test]
    fn arrow_schema_field_order_matches_columns() {
        let schema = LINK.arrow_schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        let expected: Vec<&str> = LINK.columns.iter().map(|c| c.name).collect();
        assert_eq!(names, expected);
    }

    #[test]
    fn find_column_is_case_insensitive() {
        let (i, col) = LINK.find_column("LINKID").expect("case-insensitive lookup");
        assert_eq!(col.name, "linkID");
        assert_eq!(i, 0);
        let (i, col) = LINK.find_column("RoadTypeID").expect("mixed case lookup");
        assert_eq!(col.name, "roadTypeID");
        assert_eq!(i, 3);
    }

    #[test]
    fn link_required_columns_are_first_four() {
        for col in &LINK.columns[0..4] {
            assert!(!col.nullable, "{} should be NOT NULL", col.name);
        }
        for col in &LINK.columns[4..] {
            assert!(col.nullable, "{} should be NULL-tolerant", col.name);
        }
    }

    #[test]
    fn off_network_columns_match_java_descriptor() {
        let names: Vec<&str> = OFF_NETWORK_LINK.columns.iter().map(|c| c.name).collect();
        assert_eq!(
            names,
            vec![
                "zoneID",
                "sourceTypeID",
                "vehiclePopulation",
                "startFraction",
                "extendedIdleFraction",
                "parkedVehicleFraction",
            ]
        );
    }
}

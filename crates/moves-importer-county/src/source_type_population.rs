//! `SourceTypePopulationImporter` — ports
//! `gov/epa/otaq/moves/master/implementation/importers/SourceTypePopulationImporter.java`.
//!
//! Java declares a single table `SourceTypeYear`. The user-facing
//! columns are:
//!
//! | column | filter | Java line |
//! |---|---|---|
//! | `yearID` | [`Filter::Year`] | `SourceTypePopulationImporter.java:46` |
//! | `sourceTypeID` | [`Filter::SourceType`] | `SourceTypePopulationImporter.java:47` |
//! | `sourceTypePopulation` | [`Filter::NonNegative`] | `SourceTypePopulationImporter.java:48` |
//!
//! `salesGrowthFactor` and `migrationRate` are zeroed by the SQL
//! script (`database/SourceTypePopulationImporter.sql` lines 56-58)
//! and are not user-supplied; we don't include them in the descriptor
//! and we don't materialize them in the output Parquet.
//!
//! The Java SQL script also surfaces:
//! * `ERROR: Year ... is outside the range of 1990-2060` — covered by
//!   [`Filter::Year`]'s built-in range check in
//!   [`moves_importer::validate_table`].
//! * `ERROR: Missing sourceTypePopulation value for sourceTypeID: ...`
//!   — covered by [`Filter::NonNegative`]'s null check.
//! * Default-zero rows for source types not provided by the user —
//!   not enforced here; downstream merge in the InputDataManager
//!   (Task 24) is the place for that synthesis.
//!
//! [`Filter::Year`]: moves_importer::Filter::Year
//! [`Filter::SourceType`]: moves_importer::Filter::SourceType
//! [`Filter::NonNegative`]: moves_importer::Filter::NonNegative

use moves_importer::{ColumnDescriptor, Filter, Importer, TableDescriptor};

const COLUMNS: &[ColumnDescriptor] = &[
    ColumnDescriptor::new("yearID", Filter::Year),
    ColumnDescriptor::new("sourceTypeID", Filter::SourceType),
    ColumnDescriptor::new("sourceTypePopulation", Filter::NonNegative),
];

const TABLE: TableDescriptor = TableDescriptor {
    name: "SourceTypeYear",
    columns: COLUMNS,
    primary_key: &["yearID", "sourceTypeID"],
};

const TABLES: &[TableDescriptor] = &[TABLE];

/// Source-type-year population importer.
#[derive(Debug, Default)]
pub struct SourceTypePopulationImporter;

impl Importer for SourceTypePopulationImporter {
    fn name(&self) -> &'static str {
        "Source Type Population"
    }
    fn xml_node_type(&self) -> &'static str {
        "sourcetypepopulation"
    }
    fn tables(&self) -> &'static [TableDescriptor] {
        TABLES
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_lists_user_supplied_columns_only() {
        let cols: Vec<_> = TABLE.columns.iter().map(|c| c.name).collect();
        assert_eq!(cols, vec!["yearID", "sourceTypeID", "sourceTypePopulation"]);
        assert!(TABLE.column("salesGrowthFactor").is_none());
        assert!(TABLE.column("migrationRate").is_none());
    }

    #[test]
    fn primary_key_matches_movesdb_natural_key() {
        assert_eq!(TABLE.primary_key, &["yearID", "sourceTypeID"]);
    }

    #[test]
    fn xml_node_type_matches_java_super_call() {
        assert_eq!(
            SourceTypePopulationImporter.xml_node_type(),
            "sourcetypepopulation"
        );
    }
}

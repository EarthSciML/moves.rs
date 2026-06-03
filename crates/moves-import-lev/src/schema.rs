//! `EmissionRateByAgeLEV` / `EmissionRateByAgeNLEV` schema.
//!
//! Both tables share an identical column shape — the only thing that
//! distinguishes them is the destination Parquet path. See
//! `characterization/default-db-schema/tables.json` for the source-of-truth
//! MariaDB schema.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};

/// Which alternative-rate table this row set targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LevKind {
    /// `EmissionRateByAgeLEV` — Low-Emission Vehicle alternative rates.
    Lev,
    /// `EmissionRateByAgeNLEV` — National-LEV alternative rates.
    Nlev,
}

impl LevKind {
    /// The default-DB table name this kind targets.
    #[must_use]
    pub const fn table_name(self) -> &'static str {
        match self {
            Self::Lev => "EmissionRateByAgeLEV",
            Self::Nlev => "EmissionRateByAgeNLEV",
        }
    }

    /// Parse a user-supplied table name (case-insensitive). Accepts the
    /// canonical names and the short forms `LEV` / `NLEV`.
    #[must_use]
    pub fn from_table_name(name: &str) -> Option<Self> {
        let n = name.trim();
        if n.eq_ignore_ascii_case("EmissionRateByAgeLEV") || n.eq_ignore_ascii_case("LEV") {
            Some(Self::Lev)
        } else if n.eq_ignore_ascii_case("EmissionRateByAgeNLEV") || n.eq_ignore_ascii_case("NLEV")
        {
            Some(Self::Nlev)
        } else {
            None
        }
    }
}

/// One column of the LEV/NLEV row schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Column {
    /// Header text as it appears in the CSV (and in the default-DB).
    pub name: &'static str,
    /// Logical kind — drives parsing and Arrow encoding.
    pub kind: ColumnKind,
    /// Required columns must have a non-null value in every row.
    pub required: bool,
    /// Primary-key columns participate in the uniqueness check.
    pub primary_key: bool,
}

/// Column logical kinds. The default-DB stores ids as integer families
/// (`bigint`, `int`, `smallint`) and rates as `float`; we widen the
/// integer family to `Int64` and floats to `Float64`, mirroring the
/// `mysql_to_arrow` mapping in `moves-default-db-convert`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnKind {
    /// Signed 64-bit integer.
    Integer,
    /// 64-bit float; non-negative for rate columns (enforced by the
    /// validator, not the schema).
    Float,
}

impl ColumnKind {
    /// The Arrow [`DataType`] for this kind.
    #[must_use]
    pub fn arrow_type(self) -> DataType {
        match self {
            Self::Integer => DataType::Int64,
            Self::Float => DataType::Float64,
        }
    }
}

/// The shared column layout for `EmissionRateByAgeLEV` and
/// `EmissionRateByAgeNLEV`. The order is canonical — Parquet output is
/// written in this order, and the CSV reader matches headers
/// case-insensitively rather than positionally so callers may supply
/// columns in any order.
pub const COLUMNS: &[Column] = &[
    Column {
        name: "sourceBinID",
        kind: ColumnKind::Integer,
        required: true,
        primary_key: true,
    },
    Column {
        name: "polProcessID",
        kind: ColumnKind::Integer,
        required: true,
        primary_key: true,
    },
    Column {
        name: "opModeID",
        kind: ColumnKind::Integer,
        required: true,
        primary_key: true,
    },
    Column {
        name: "ageGroupID",
        kind: ColumnKind::Integer,
        required: true,
        primary_key: true,
    },
    Column {
        name: "meanBaseRate",
        kind: ColumnKind::Float,
        required: true,
        primary_key: false,
    },
    Column {
        name: "meanBaseRateCV",
        kind: ColumnKind::Float,
        required: false,
        primary_key: false,
    },
    Column {
        name: "meanBaseRateIM",
        kind: ColumnKind::Float,
        required: false,
        primary_key: false,
    },
    Column {
        name: "meanBaseRateIMCV",
        kind: ColumnKind::Float,
        required: false,
        primary_key: false,
    },
    Column {
        name: "dataSourceId",
        kind: ColumnKind::Integer,
        required: false,
        primary_key: false,
    },
];

/// Build the Arrow schema for the LEV/NLEV row format. Every column is
/// nullable on the wire even though required columns may not contain
/// nulls in valid input — the validator enforces presence before encoding.
#[must_use]
pub fn arrow_schema() -> SchemaRef {
    let fields: Vec<Field> = COLUMNS
        .iter()
        .map(|c| Field::new(c.name, c.kind.arrow_type(), true))
        .collect();
    Arc::new(ArrowSchema::new(fields))
}

/// Column index of `name`, case-insensitively. Returns `None` for headers
/// that don't appear in [`COLUMNS`].
#[must_use]
pub fn column_index(name: &str) -> Option<usize> {
    COLUMNS
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_round_trips_through_table_name() {
        for kind in [LevKind::Lev, LevKind::Nlev] {
            assert_eq!(LevKind::from_table_name(kind.table_name()), Some(kind));
        }
    }

    #[test]
    fn kind_accepts_short_forms() {
        assert_eq!(LevKind::from_table_name("LEV"), Some(LevKind::Lev));
        assert_eq!(LevKind::from_table_name("lev"), Some(LevKind::Lev));
        assert_eq!(LevKind::from_table_name("NLEV"), Some(LevKind::Nlev));
        assert_eq!(LevKind::from_table_name("nlev"), Some(LevKind::Nlev));
    }

    #[test]
    fn kind_rejects_unknown_names() {
        assert_eq!(LevKind::from_table_name(""), None);
        assert_eq!(LevKind::from_table_name("EmissionRateByAge"), None);
        assert_eq!(LevKind::from_table_name("nlev2"), None);
    }

    #[test]
    fn column_index_is_case_insensitive() {
        assert_eq!(column_index("sourceBinID"), Some(0));
        assert_eq!(column_index("sourcebinid"), Some(0));
        assert_eq!(column_index("SOURCEBINID"), Some(0));
        assert_eq!(column_index("dataSourceId"), Some(8));
        assert!(column_index("nope").is_none());
    }

    #[test]
    fn arrow_schema_has_nine_nullable_fields_in_canonical_order() {
        let schema = arrow_schema();
        assert_eq!(schema.fields().len(), COLUMNS.len());
        for (i, col) in COLUMNS.iter().enumerate() {
            let field = schema.field(i);
            assert_eq!(field.name(), col.name);
            assert_eq!(field.data_type(), &col.kind.arrow_type());
            assert!(field.is_nullable());
        }
    }

    #[test]
    fn primary_key_columns_match_default_db() {
        let pk: Vec<&'static str> = COLUMNS
            .iter()
            .filter(|c| c.primary_key)
            .map(|c| c.name)
            .collect();
        assert_eq!(
            pk,
            vec!["sourceBinID", "polProcessID", "opModeID", "ageGroupID"]
        );
    }
}

//! Column [`Filter`] — the per-column validation constraint a MOVES
//! importer attaches to each `dataTableDescriptor` triple.
//!
//! The variants port the `FILTER_*` constants from
//! `gov/epa/otaq/moves/master/framework/importers/ImporterManager.java`
//! one-for-one. Two kinds of constraint show up:
//!
//! * **Foreign-key (decode-table) filters** — the value must exist in a
//!   named lookup table in the MOVES default DB. Examples: [`Filter::Year`],
//!   [`Filter::SourceType`], [`Filter::RoadType`], [`Filter::Age`]. We
//!   carry the table name so the validator can scan the default-DB
//!   Parquet at validation time.
//! * **Numeric range filters** — the value must lie in a fixed numeric
//!   range. Examples: [`Filter::NonNegative`], [`Filter::ZeroToOne`].
//!   These are checked against the column directly without a DB read.
//!
//! Java's `ImporterManager` reuses the column-name string itself as the
//! filter constant for foreign-key cases — `"yearID"` is both the
//! column name AND the filter id. The Rust port separates the two so
//! the descriptor's column name doesn't need to match the canonical
//! foreign-key column name verbatim (and so a single column can pick a
//! more-specific filter, e.g. [`Filter::RoadTypeNotOffNetwork`] vs
//! plain [`Filter::RoadType`]).

use arrow::datatypes::DataType;

/// Per-column validation constraint.
///
/// See [`Filter::arrow_type`] for the canonical Arrow type a column
/// carrying this filter maps to, and [`Filter::decode_table`] for the
/// default-DB lookup table a foreign-key filter resolves against.
#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    // ----- Foreign-key (decode-table) filters -----
    /// Calendar year. Decode table `Year`, valid range 1990-2060
    /// (inclusive) per `SourceTypePopulationImporter.sql` and the
    /// default-DB `year` table contents.
    Year,
    /// MOVES source-use type id (1-99 per `SourceUseType` table).
    SourceType,
    /// Age category id (0-30 per `AgeCategory` table).
    Age,
    /// Zone id (decoded as `countyID * 10` historically; we don't
    /// enforce the encoding, just that the id appears in the user's
    /// `Zone` table).
    Zone,
    /// County id (5-digit FIPS, e.g. 06037 = Los Angeles County).
    County,
    /// State id (1-2-digit FIPS, 1-56).
    State,
    /// Road type id (1-5 inclusive per `RoadType` table).
    RoadType,
    /// Road type excluding off-network (i.e. 2-5).
    RoadTypeNotOffNetwork,
    /// Pollutant id.
    Pollutant,
    /// Emission process id.
    Process,
    /// Fuel type id.
    FuelType,
    /// Month id (1-12).
    Month,
    /// Day id (1-7 per `DayOfAnyWeek`).
    Day,
    /// Hour id (1-24 per `HourOfAnyDay`).
    Hour,
    /// Model year id (>=1950, <=2060 per `ImporterManager.FILTER_MODELYEARID`).
    ModelYear,

    // ----- Numeric range filters -----
    /// Non-negative float (`>= 0.0`). Nulls are NOT permitted by the
    /// SQL importer (see `SourceTypePopulationImporter.sql` line 67-71
    /// which surfaces an ERROR on missing values), so the framework
    /// treats null as a validation error here.
    NonNegative,
    /// Non-negative float, default 1.0 when null is allowed.
    NonNegativeDefault1,
    /// Fraction in `[0.0, 1.0]` inclusive.
    ZeroToOne,
    /// Percentage in `[0.0, 100.0]` inclusive.
    ZeroToOneHundred,
    /// Yes/No flag — the column type is `char(1)` per MOVES convention;
    /// allowed values are `Y` and `N` (case-insensitive).
    YesNo,
    /// Boolean (`0` / `1`), accepted by some importers in place of
    /// `Y`/`N`.
    Boolean,
    /// Free-form text — no constraint beyond non-null when listed as a
    /// required column. Used for description fields.
    Text,
}

impl Filter {
    /// The canonical Arrow type for a column carrying this filter.
    ///
    /// Foreign-key filters all map to [`DataType::Int64`] (matching the
    /// `mysql_to_arrow` widening in `moves-default-db-convert`).
    /// Fractions and population counts map to [`DataType::Float64`].
    /// Flags map to [`DataType::Utf8`] for `Y`/`N` and [`DataType::Boolean`]
    /// for `0`/`1`.
    pub fn arrow_type(&self) -> DataType {
        match self {
            Filter::Year
            | Filter::SourceType
            | Filter::Age
            | Filter::Zone
            | Filter::County
            | Filter::State
            | Filter::RoadType
            | Filter::RoadTypeNotOffNetwork
            | Filter::Pollutant
            | Filter::Process
            | Filter::FuelType
            | Filter::Month
            | Filter::Day
            | Filter::Hour
            | Filter::ModelYear => DataType::Int64,

            Filter::NonNegative
            | Filter::NonNegativeDefault1
            | Filter::ZeroToOne
            | Filter::ZeroToOneHundred => DataType::Float64,

            Filter::YesNo | Filter::Text => DataType::Utf8,
            Filter::Boolean => DataType::Boolean,
        }
    }

    /// If this filter is a foreign-key constraint into the MOVES default
    /// DB, return the decode-table name (case-sensitive, as used in
    /// `tables.json`). Returns `None` for pure numeric / text filters.
    ///
    /// The validator uses this to fetch the set of valid ids from the
    /// `moves-data-default::DefaultDb` and check the imported column
    /// against it.
    pub fn decode_table(&self) -> Option<&'static str> {
        match self {
            Filter::Year => Some("Year"),
            Filter::SourceType => Some("SourceUseType"),
            Filter::Age => Some("AgeCategory"),
            Filter::Zone => Some("Zone"),
            Filter::County => Some("County"),
            Filter::State => Some("State"),
            Filter::RoadType | Filter::RoadTypeNotOffNetwork => Some("RoadType"),
            Filter::Pollutant => Some("Pollutant"),
            Filter::Process => Some("EmissionProcess"),
            Filter::FuelType => Some("FuelType"),
            Filter::Month => Some("MonthOfAnyYear"),
            Filter::Day => Some("DayOfAnyWeek"),
            Filter::Hour => Some("HourOfAnyDay"),
            Filter::ModelYear => Some("Year"),
            _ => None,
        }
    }

    /// The canonical primary-key column name in the decode table, if
    /// any. Caller uses this to pull the id column out of the
    /// decode-table DataFrame returned by
    /// `moves-data-default::DefaultDb`.
    pub fn decode_column(&self) -> Option<&'static str> {
        match self {
            Filter::Year => Some("yearID"),
            Filter::SourceType => Some("sourceTypeID"),
            Filter::Age => Some("ageID"),
            Filter::Zone => Some("zoneID"),
            Filter::County => Some("countyID"),
            Filter::State => Some("stateID"),
            Filter::RoadType | Filter::RoadTypeNotOffNetwork => Some("roadTypeID"),
            Filter::Pollutant => Some("pollutantID"),
            Filter::Process => Some("processID"),
            Filter::FuelType => Some("fuelTypeID"),
            Filter::Month => Some("monthID"),
            Filter::Day => Some("dayID"),
            Filter::Hour => Some("hourID"),
            Filter::ModelYear => Some("yearID"),
            _ => None,
        }
    }

    /// Whether null values are permitted. Most filters reject null; the
    /// `*Default1` variant allows null because the importer fills in
    /// 1.0 on its behalf.
    pub fn nullable(&self) -> bool {
        matches!(self, Filter::NonNegativeDefault1 | Filter::Text)
    }

    /// For numeric range filters, the inclusive lower bound. `None`
    /// means "no constraint."
    pub fn numeric_min(&self) -> Option<f64> {
        match self {
            Filter::NonNegative | Filter::NonNegativeDefault1 => Some(0.0),
            Filter::ZeroToOne => Some(0.0),
            Filter::ZeroToOneHundred => Some(0.0),
            _ => None,
        }
    }

    /// For numeric range filters, the inclusive upper bound.
    pub fn numeric_max(&self) -> Option<f64> {
        match self {
            Filter::ZeroToOne => Some(1.0),
            Filter::ZeroToOneHundred => Some(100.0),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn foreign_key_filters_carry_decode_tables() {
        assert_eq!(Filter::Year.decode_table(), Some("Year"));
        assert_eq!(Filter::SourceType.decode_table(), Some("SourceUseType"));
        assert_eq!(Filter::Age.decode_table(), Some("AgeCategory"));
        assert_eq!(Filter::RoadType.decode_table(), Some("RoadType"));
        // Off-network variant uses the same decode table but a
        // narrower numeric range — validator enforces the narrowing.
        assert_eq!(
            Filter::RoadTypeNotOffNetwork.decode_table(),
            Some("RoadType")
        );
    }

    #[test]
    fn numeric_filters_have_no_decode_table() {
        assert_eq!(Filter::NonNegative.decode_table(), None);
        assert_eq!(Filter::ZeroToOne.decode_table(), None);
        assert_eq!(Filter::ZeroToOneHundred.decode_table(), None);
    }

    #[test]
    fn arrow_types_align_with_default_db_conversion() {
        assert_eq!(Filter::Year.arrow_type(), DataType::Int64);
        assert_eq!(Filter::SourceType.arrow_type(), DataType::Int64);
        assert_eq!(Filter::NonNegative.arrow_type(), DataType::Float64);
        assert_eq!(Filter::ZeroToOne.arrow_type(), DataType::Float64);
        assert_eq!(Filter::YesNo.arrow_type(), DataType::Utf8);
        assert_eq!(Filter::Boolean.arrow_type(), DataType::Boolean);
    }

    #[test]
    fn nullability_defaults_to_required() {
        assert!(!Filter::NonNegative.nullable());
        assert!(!Filter::Year.nullable());
        assert!(Filter::NonNegativeDefault1.nullable());
        assert!(Filter::Text.nullable());
    }

    #[test]
    fn numeric_bounds_match_filter_semantics() {
        assert_eq!(Filter::NonNegative.numeric_min(), Some(0.0));
        assert_eq!(Filter::NonNegative.numeric_max(), None);
        assert_eq!(Filter::ZeroToOne.numeric_min(), Some(0.0));
        assert_eq!(Filter::ZeroToOne.numeric_max(), Some(1.0));
        assert_eq!(Filter::ZeroToOneHundred.numeric_max(), Some(100.0));
    }
}

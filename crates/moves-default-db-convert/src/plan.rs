//! Partitioning-plan loader. Parses `characterization/default-db-schema/tables.json`
//! (schema tag `moves-default-db-schema/v1`) into a typed plan structure.
//!
//! Only the fields the conversion pipeline needs are deserialized; unknown
//! fields are tolerated so the audit can grow without breaking conversions.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

pub const SCHEMA_VERSION: &str = "moves-default-db-schema/v1";

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PartitionPlan {
    pub schema_version: String,
    pub moves_commit: String,
    pub table_count: usize,
    pub tables: Vec<TableEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct TableEntry {
    pub name: String,
    pub primary_key: Vec<String>,
    pub columns: Vec<ColumnEntry>,
    #[serde(default)]
    pub indexes: Vec<IndexEntry>,
    pub size_bucket: String,
    pub partition: PartitionInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ColumnEntry {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct IndexEntry {
    pub unique: bool,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PartitionInfo {
    pub strategy: PartitionStrategy,
    #[serde(default)]
    pub rationale: String,
}

/// One of the five partition strategies emitted by `audit-schema.py`. Anything
/// else parses into [`PartitionStrategy::Unknown`] so a future audit revision
/// surfaces as a clear error in [`crate::partition::resolve`] rather than a
/// JSON parse failure halfway through a conversion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionStrategy {
    Monolithic,
    SchemaOnly,
    County,
    Year,
    YearXCounty,
    ModelYear,
    #[serde(other)]
    Unknown,
}

impl PartitionStrategy {
    pub fn as_str(self) -> &'static str {
        match self {
            PartitionStrategy::Monolithic => "monolithic",
            PartitionStrategy::SchemaOnly => "schema_only",
            PartitionStrategy::County => "county",
            PartitionStrategy::Year => "year",
            PartitionStrategy::YearXCounty => "year_x_county",
            PartitionStrategy::ModelYear => "model_year",
            PartitionStrategy::Unknown => "unknown",
        }
    }
}

impl PartitionPlan {
    pub fn from_file(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path).map_err(|source| Error::Io {
            path: path.to_path_buf(),
            source,
        })?;
        Self::from_bytes(path, &bytes)
    }

    pub fn from_bytes(path: &Path, bytes: &[u8]) -> Result<Self> {
        let plan: PartitionPlan = serde_json::from_slice(bytes).map_err(|source| Error::Json {
            path: path.to_path_buf(),
            source,
        })?;
        if plan.schema_version != SCHEMA_VERSION {
            return Err(Error::Plan(format!(
                "expected schema_version='{}', got '{}'",
                SCHEMA_VERSION, plan.schema_version
            )));
        }
        if plan.tables.len() != plan.table_count {
            return Err(Error::Plan(format!(
                "table_count={} but tables array has {} entries",
                plan.table_count,
                plan.tables.len()
            )));
        }
        Ok(plan)
    }

    pub fn get(&self, name: &str) -> Option<&TableEntry> {
        let needle = name.to_ascii_lowercase();
        self.tables
            .iter()
            .find(|t| t.name.eq_ignore_ascii_case(&needle))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_plan() -> &'static [u8] {
        br#"{
            "schema_version": "moves-default-db-schema/v1",
            "moves_commit": "deadbeef",
            "sources": {},
            "table_count": 2,
            "tables": [
                {
                    "name": "Year",
                    "primary_key": ["yearID"],
                    "columns": [
                        {"name": "yearID", "type": "smallint"},
                        {"name": "isBaseYear", "type": "char"}
                    ],
                    "indexes": [],
                    "estimated_rows_upper_bound": 100,
                    "size_bucket": "small",
                    "filter_columns": ["yearID"],
                    "partition": {"strategy": "monolithic", "rationale": "lookup"}
                },
                {
                    "name": "SHO",
                    "primary_key": ["hourDayID", "linkID", "sourceTypeID"],
                    "columns": [{"name": "hourDayID", "type": "int"}],
                    "indexes": [],
                    "estimated_rows_upper_bound": 1000000,
                    "size_bucket": "empty",
                    "filter_columns": [],
                    "partition": {"strategy": "schema_only", "rationale": "empty in default DB"}
                }
            ]
        }"#
    }

    #[test]
    fn parse_minimal_plan() {
        let plan = PartitionPlan::from_bytes(Path::new("tables.json"), minimal_plan()).unwrap();
        assert_eq!(plan.schema_version, SCHEMA_VERSION);
        assert_eq!(plan.tables.len(), 2);
        assert_eq!(plan.tables[0].name, "Year");
        assert_eq!(
            plan.tables[0].partition.strategy,
            PartitionStrategy::Monolithic
        );
        assert_eq!(
            plan.tables[1].partition.strategy,
            PartitionStrategy::SchemaOnly
        );
    }

    #[test]
    fn case_insensitive_lookup() {
        let plan = PartitionPlan::from_bytes(Path::new("tables.json"), minimal_plan()).unwrap();
        assert!(plan.get("year").is_some());
        assert!(plan.get("Year").is_some());
        assert!(plan.get("YEAR").is_some());
        assert!(plan.get("missing").is_none());
    }

    #[test]
    fn rejects_wrong_schema_version() {
        let body = br#"{
            "schema_version": "moves-default-db-schema/v2",
            "moves_commit": "x",
            "sources": {},
            "table_count": 0,
            "tables": []
        }"#;
        let err = PartitionPlan::from_bytes(Path::new("tables.json"), body).unwrap_err();
        assert!(matches!(err, Error::Plan(_)));
    }

    #[test]
    fn rejects_count_mismatch() {
        let body = br#"{
            "schema_version": "moves-default-db-schema/v1",
            "moves_commit": "x",
            "sources": {},
            "table_count": 5,
            "tables": []
        }"#;
        let err = PartitionPlan::from_bytes(Path::new("tables.json"), body).unwrap_err();
        assert!(matches!(err, Error::Plan(_)));
    }

    #[test]
    fn unknown_strategy_parses_to_variant() {
        let body = br#"{
            "schema_version": "moves-default-db-schema/v1",
            "moves_commit": "x",
            "sources": {},
            "table_count": 1,
            "tables": [{
                "name": "Future",
                "primary_key": [],
                "columns": [],
                "indexes": [],
                "estimated_rows_upper_bound": 0,
                "size_bucket": "tiny",
                "filter_columns": [],
                "partition": {"strategy": "by_quarter_moon", "rationale": ""}
            }]
        }"#;
        let plan = PartitionPlan::from_bytes(Path::new("tables.json"), body).unwrap();
        assert_eq!(
            plan.tables[0].partition.strategy,
            PartitionStrategy::Unknown
        );
    }
}

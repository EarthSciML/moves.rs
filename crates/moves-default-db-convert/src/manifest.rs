//! Output manifest written next to the per-table Parquet files.
//!
//! Schema tag: `moves-default-db-manifest/v1`. Downstream consumers (the
//! lazy-loading reader, Task 82) read this to discover which files belong
//! to which table, validate row counts at load time, and detect drift.

use serde::{Deserialize, Serialize};

pub const SCHEMA_VERSION: &str = "moves-default-db-manifest/v1";
pub const MANIFEST_FILENAME: &str = "manifest.json";

/// Top-level manifest. Tables are sorted by case-folded `name` so the JSON
/// itself is deterministic. Each table records its partition strategy and
/// the per-partition Parquet files.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Manifest {
    pub schema_version: String,
    pub moves_db_version: String,
    pub moves_commit: String,
    pub plan_sha256: String,
    pub generated_at_utc: String,
    /// Sorted by case-folded `name`.
    pub tables: Vec<TableManifest>,
}

/// Per-table manifest entry. For monolithic tables, `partitions` has a
/// single entry. For schema-only tables, `partitions` is empty and
/// `schema_only_path` points at the sidecar JSON.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct TableManifest {
    pub name: String,
    pub partition_strategy: String,
    pub partition_columns: Vec<String>,
    pub row_count: u64,
    pub columns: Vec<ColumnManifest>,
    pub primary_key: Vec<String>,
    pub partitions: Vec<PartitionManifest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_only_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ColumnManifest {
    pub name: String,
    pub mysql_type: String,
    pub arrow_type: String,
    pub primary_key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PartitionManifest {
    /// Relative path under the output root.
    pub path: String,
    /// Partition key values in the same order as `TableManifest.partition_columns`.
    #[serde(default)]
    pub values: Vec<String>,
    pub row_count: u64,
    pub sha256: String,
    pub bytes: u64,
}

/// Schema-only sidecar (`<table>.schema.json`) written alongside the
/// Parquet tree for `schema_only` tables. Records the column types so a
/// runtime that populates these tables in execution-DB mode can validate
/// inserts.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct SchemaOnlySidecar {
    pub schema_version: String,
    pub name: String,
    pub columns: Vec<ColumnManifest>,
    pub primary_key: Vec<String>,
}

pub const SCHEMA_ONLY_VERSION: &str = "moves-default-db-schema-only/v1";

impl Manifest {
    pub fn new(
        moves_db_version: String,
        moves_commit: String,
        plan_sha256: String,
        generated_at_utc: String,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION.to_string(),
            moves_db_version,
            moves_commit,
            plan_sha256,
            generated_at_utc,
            tables: Vec::new(),
        }
    }

    pub fn push(&mut self, entry: TableManifest) {
        self.tables.push(entry);
    }

    pub fn finalize(&mut self) {
        self.tables.sort_by(|a, b| {
            a.name
                .to_ascii_lowercase()
                .cmp(&b.name.to_ascii_lowercase())
        });
    }

    pub fn to_pretty_json(&self) -> serde_json::Result<String> {
        serde_json::to_string_pretty(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_serialises_pretty_and_finalises_sorted() {
        let mut m = Manifest::new(
            "movesdb20241112".into(),
            "deadbeef".into(),
            "0".repeat(64),
            "1970-01-01T00:00:00Z".into(),
        );
        m.push(TableManifest {
            name: "ZZZ".into(),
            partition_strategy: "monolithic".into(),
            partition_columns: vec![],
            row_count: 1,
            columns: vec![],
            primary_key: vec![],
            partitions: vec![PartitionManifest {
                path: "ZZZ.parquet".into(),
                values: vec![],
                row_count: 1,
                sha256: "00".into(),
                bytes: 100,
            }],
            schema_only_path: None,
        });
        m.push(TableManifest {
            name: "aaa".into(),
            partition_strategy: "schema_only".into(),
            partition_columns: vec![],
            row_count: 0,
            columns: vec![],
            primary_key: vec![],
            partitions: vec![],
            schema_only_path: Some("aaa.schema.json".into()),
        });
        m.finalize();
        assert_eq!(m.tables[0].name, "aaa");
        let json = m.to_pretty_json().unwrap();
        assert!(json.contains("\"schema_version\""));
    }

    #[test]
    fn schema_only_sidecar_round_trips() {
        let s = SchemaOnlySidecar {
            schema_version: SCHEMA_ONLY_VERSION.to_string(),
            name: "Link".into(),
            columns: vec![ColumnManifest {
                name: "linkID".into(),
                mysql_type: "int".into(),
                arrow_type: "Int64".into(),
                primary_key: true,
            }],
            primary_key: vec!["linkID".into()],
        };
        let json = serde_json::to_string(&s).unwrap();
        let parsed: SchemaOnlySidecar = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, s);
    }
}

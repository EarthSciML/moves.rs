//! Output manifest written next to the imported Parquet tree.
//!
//! Schema tag: `moves-nonroad-import-manifest/v1`. The shape mirrors the
//! default-DB converter's manifest (Phase 4 Task 80) so a downstream reader
//! that already understands one can be taught the other with a single
//! schema-version check; the user-input importer evolves on its own
//! lifecycle, so the schema tag is distinct.

use serde::{Deserialize, Serialize};

pub const SCHEMA_VERSION: &str = "moves-nonroad-import-manifest/v1";
pub const MANIFEST_FILENAME: &str = "manifest.json";

/// Top-level manifest. Tables are sorted by lower-cased `name` so the
/// JSON itself is deterministic across runs that produce the same set of
/// tables.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Manifest {
    pub schema_version: String,
    pub generated_at_utc: String,
    pub tables: Vec<TableManifest>,
}

/// Per-table manifest entry. Always monolithic for user-input tables —
/// they are smaller than the partitioned default-DB tables and partitioning
/// is not justified.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct TableManifest {
    pub name: String,
    pub row_count: u64,
    pub columns: Vec<ColumnManifest>,
    pub primary_key: Vec<String>,
    pub path: String,
    pub sha256: String,
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ColumnManifest {
    pub name: String,
    pub mysql_type: String,
    pub arrow_type: String,
    pub primary_key: bool,
}

impl Manifest {
    pub fn new(generated_at_utc: String) -> Self {
        Self {
            schema_version: SCHEMA_VERSION.to_string(),
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
    fn manifest_round_trips_via_json() {
        let mut m = Manifest::new("1970-01-01T00:00:00Z".into());
        m.push(TableManifest {
            name: "nrbaseyearequippopulation".into(),
            row_count: 1,
            columns: vec![ColumnManifest {
                name: "sourceTypeID".into(),
                mysql_type: "smallint(6)".into(),
                arrow_type: "Int64".into(),
                primary_key: true,
            }],
            primary_key: vec!["sourceTypeID".into()],
            path: "nrbaseyearequippopulation.parquet".into(),
            sha256: "0".repeat(64),
            bytes: 256,
        });
        m.finalize();
        let json = m.to_pretty_json().unwrap();
        let back: Manifest = serde_json::from_str(&json).unwrap();
        assert_eq!(back, m);
        assert_eq!(back.schema_version, SCHEMA_VERSION);
    }

    #[test]
    fn finalize_sorts_by_lowercased_name() {
        let mut m = Manifest::new("now".into());
        for n in ["ZZZ", "aaa", "Mmm"] {
            m.push(TableManifest {
                name: n.into(),
                row_count: 0,
                columns: vec![],
                primary_key: vec![],
                path: format!("{n}.parquet"),
                sha256: String::new(),
                bytes: 0,
            });
        }
        m.finalize();
        let names: Vec<&str> = m.tables.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, vec!["aaa", "Mmm", "ZZZ"]);
    }
}

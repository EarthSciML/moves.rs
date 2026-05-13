//! Per-import manifest, mirroring `moves-default-db-convert::manifest`.
//!
//! The default-DB pipeline writes a `manifest.json` so the lazy
//! reader can find files by table name and verify their content
//! hashes. We follow the same convention: an importer run produces a
//! `manifest.json` listing every table that was loaded, the source
//! CSV path, the output Parquet path, the SHA-256, the row count,
//! and any warnings emitted. Downstream tooling (the importer
//! validation suite at Task 88) reads it to compare against the
//! canonical-MOVES MariaDB dump.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::csv_reader::ImportWarning;

/// Schema tag — bump on incompatible changes.
pub const EXPECTED_SCHEMA_VERSION: &str = "moves-importer-pdb/v1";

/// Filename written into the output directory.
pub const MANIFEST_FILENAME: &str = "manifest.json";

/// Top-level manifest written by [`crate::ImportSession::write_to_disk`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Manifest {
    pub schema_version: String,
    /// One entry per loaded table.
    pub tables: Vec<TableManifest>,
}

impl Manifest {
    pub fn new(tables: Vec<TableManifest>) -> Self {
        Self {
            schema_version: EXPECTED_SCHEMA_VERSION.to_string(),
            tables,
        }
    }
}

/// Per-table manifest entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableManifest {
    /// MOVES table name (e.g. "Link", "OpModeDistribution").
    pub name: String,
    /// CSV file the importer read from.
    pub source_path: PathBuf,
    /// Parquet file the importer wrote to (relative to the manifest's
    /// directory if the writer placed it under the same root).
    pub output_path: PathBuf,
    /// Hex SHA-256 of the Parquet bytes.
    pub sha256: String,
    /// Number of rows imported.
    pub row_count: u64,
    /// Filter-rejected cells (Java-style WARNING entries).
    pub warnings: Vec<ImportWarning>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_round_trips_through_json() {
        let m = Manifest::new(vec![TableManifest {
            name: "Link".into(),
            source_path: PathBuf::from("/tmp/link.csv"),
            output_path: PathBuf::from("/tmp/Link.parquet"),
            sha256: "deadbeef".repeat(8),
            row_count: 42,
            warnings: vec![],
        }]);
        let json = serde_json::to_string(&m).unwrap();
        let back: Manifest = serde_json::from_str(&json).unwrap();
        assert_eq!(m, back);
    }

    #[test]
    fn manifest_includes_warnings() {
        let m = Manifest::new(vec![TableManifest {
            name: "Link".into(),
            source_path: PathBuf::from("/tmp/link.csv"),
            output_path: PathBuf::from("/tmp/Link.parquet"),
            sha256: "0".repeat(64),
            row_count: 10,
            warnings: vec![ImportWarning {
                line: 5,
                column: "countyID".into(),
                message: "countyID 999 is not used.".into(),
            }],
        }]);
        let json = serde_json::to_string(&m).unwrap();
        assert!(json.contains("999 is not used"));
    }
}

//! Manifest loader.
//!
//! The default-DB conversion crate (Phase 4 Task 80) writes a
//! `manifest.json` (schema tag `moves-default-db-manifest/v1`) alongside
//! the Parquet tree. The reader uses the manifest to discover which
//! files belong to which table, which partition values they carry, and
//! which tables are schema-only sidecars rather than data files.
//!
//! The on-disk schema is owned by `moves-default-db-convert`; this
//! module re-exports the relevant types so callers depend on one
//! definition rather than maintaining a parallel struct here. The reader
//! adds a minimal [`load`] helper that pins the schema version and
//! returns crate-local errors.

use std::path::Path;

use crate::error::{Error, Result};

pub use moves_default_db_convert::manifest::{
    ColumnManifest, Manifest, PartitionManifest, SchemaOnlySidecar, TableManifest,
    MANIFEST_FILENAME, SCHEMA_ONLY_VERSION,
};

/// The manifest schema version this reader understands. Bump this and
/// any compatibility-shim logic when the converter's manifest schema
/// changes.
pub const EXPECTED_SCHEMA_VERSION: &str = "moves-default-db-manifest/v1";

/// Read `manifest.json` from `path`. Validates `schema_version` so a
/// future converter revision can't silently ship a layout the reader
/// doesn't understand.
pub fn load(path: &Path) -> Result<Manifest> {
    let bytes = std::fs::read(path).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let manifest: Manifest =
        serde_json::from_slice(&bytes).map_err(|source| Error::ManifestParse {
            path: path.to_path_buf(),
            source,
        })?;
    if manifest.schema_version != EXPECTED_SCHEMA_VERSION {
        return Err(Error::ManifestVersion {
            path: path.to_path_buf(),
            expected: EXPECTED_SCHEMA_VERSION.to_string(),
            found: manifest.schema_version,
        });
    }
    Ok(manifest)
}

/// Look up a table manifest by case-insensitive name. MariaDB on Linux
/// lower-cases table names while the audit + Java code use CamelCase, so
/// the reader matches in a case-insensitive way to insulate callers.
pub fn find_table<'m>(manifest: &'m Manifest, name: &str) -> Option<&'m TableManifest> {
    let needle = name.to_ascii_lowercase();
    manifest
        .tables
        .iter()
        .find(|t| t.name.eq_ignore_ascii_case(&needle))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_tmp(body: &[u8]) -> tempfile::NamedTempFile {
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(body).unwrap();
        f
    }

    #[test]
    fn load_accepts_valid_manifest() {
        let body = br#"{
            "schema_version": "moves-default-db-manifest/v1",
            "moves_db_version": "movesdb20241112",
            "moves_commit": "deadbeef",
            "plan_sha256": "0",
            "generated_at_utc": "1970-01-01T00:00:00Z",
            "tables": []
        }"#;
        let tmp = write_tmp(body);
        let m = load(tmp.path()).unwrap();
        assert_eq!(m.moves_db_version, "movesdb20241112");
    }

    #[test]
    fn load_rejects_wrong_schema_version() {
        let body = br#"{
            "schema_version": "moves-default-db-manifest/v2",
            "moves_db_version": "x",
            "moves_commit": "x",
            "plan_sha256": "0",
            "generated_at_utc": "1970-01-01T00:00:00Z",
            "tables": []
        }"#;
        let tmp = write_tmp(body);
        let err = load(tmp.path()).unwrap_err();
        assert!(matches!(err, Error::ManifestVersion { .. }), "got {err:?}");
    }

    #[test]
    fn load_reports_io_error_for_missing_file() {
        let err = load(Path::new("/nonexistent/manifest.json")).unwrap_err();
        assert!(matches!(err, Error::Io { .. }), "got {err:?}");
    }

    #[test]
    fn find_table_is_case_insensitive() {
        let mut m = Manifest::new(
            "v".into(),
            "c".into(),
            "p".into(),
            "1970-01-01T00:00:00Z".into(),
        );
        m.push(TableManifest {
            name: "SourceUseType".into(),
            partition_strategy: "monolithic".into(),
            partition_columns: vec![],
            row_count: 0,
            columns: vec![],
            primary_key: vec![],
            partitions: vec![],
            schema_only_path: None,
        });
        assert!(find_table(&m, "SourceUseType").is_some());
        assert!(find_table(&m, "sourceusetype").is_some());
        assert!(find_table(&m, "SOURCEUSETYPE").is_some());
        assert!(find_table(&m, "missing").is_none());
    }
}

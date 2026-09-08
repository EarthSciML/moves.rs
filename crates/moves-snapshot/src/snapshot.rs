//! Snapshot directory: write tables to parquet + manifest, read them back.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;

use crate::error::{Error, Result};
use crate::format::{ColumnSpec, FormatProfile, PARQUET_CREATED_BY, SUPPORTED_FORMAT_VERSIONS};
use crate::manifest::{compute_aggregate_hash, sha256_hex, Manifest, ManifestEntry, TableMetadata};
use crate::table::Table;

const TABLES_SUBDIR: &str = "tables";
const MANIFEST_FILE: &str = "manifest.json";

/// In-memory collection of normalized tables that maps to a snapshot directory.
///
/// Tables are stored keyed by name in lexicographic order, so iteration/// and the resulting on-disk manifest — is deterministic.
///
/// A snapshot also carries the [`FormatProfile`] it belongs to. A freshly
/// built one gets the current profile; one produced by [`Snapshot::load`]
/// keeps the version it was read from, so loading an older snapshot and
/// writing it back reproduces the original bytes rather than relabelling
/// v1-encoded cells as v2.
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    tables: BTreeMap<String, Table>,
    profile: FormatProfile,
}

impl Snapshot {
    pub fn new() -> Self {
        Self::default()
    }

    /// The format version + float encoding this snapshot's cells use.
    pub fn profile(&self) -> &FormatProfile {
        &self.profile
    }

    /// The format version string that will be written to `manifest.json`.
    pub fn format_version(&self) -> &str {
        &self.profile.version
    }

    /// Build an empty snapshot pinned to a specific format profile.
    ///
    /// Only useful for rewriting a snapshot in the version it came from, and
    /// for tests that need to produce an older-format fixture. Tables added to
    /// it must already carry cells encoded under that profile's float rule —
    /// [`crate::TableBuilder`] always normalizes with the *current* rule, so
    /// an older-profile snapshot has to be assembled from
    /// [`crate::Table::from_normalized`].
    pub fn with_profile(profile: FormatProfile) -> Self {
        Self {
            tables: BTreeMap::new(),
            profile,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    pub fn len(&self) -> usize {
        self.tables.len()
    }

    pub fn tables(&self) -> impl Iterator<Item = &Table> {
        self.tables.values()
    }

    pub fn table_names(&self) -> impl Iterator<Item = &str> {
        self.tables.keys().map(String::as_str)
    }

    pub fn table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    pub fn add_table(&mut self, table: Table) -> Result<()> {
        if self.tables.contains_key(table.name()) {
            return Err(Error::DuplicateTable {
                table: table.name().to_string(),
            });
        }
        self.tables.insert(table.name().to_string(), table);
        Ok(())
    }

    /// Write the snapshot to `dir`, creating it if absent. Any existing
    /// `manifest.json` and `tables/` are removed first so the resulting bytes
    /// only depend on `self`.
    pub fn write(&self, dir: &Path) -> Result<()> {
        fs::create_dir_all(dir).map_err(|source| Error::Io {
            path: dir.to_path_buf(),
            source,
        })?;
        let tables_dir = dir.join(TABLES_SUBDIR);
        if tables_dir.exists() {
            fs::remove_dir_all(&tables_dir).map_err(|source| Error::Io {
                path: tables_dir.clone(),
                source,
            })?;
        }
        fs::create_dir_all(&tables_dir).map_err(|source| Error::Io {
            path: tables_dir.clone(),
            source,
        })?;

        let manifest_path = dir.join(MANIFEST_FILE);
        if manifest_path.exists() {
            fs::remove_file(&manifest_path).map_err(|source| Error::Io {
                path: manifest_path.clone(),
                source,
            })?;
        }

        let mut entries = Vec::with_capacity(self.tables.len());
        for table in self.tables.values() {
            let parquet_path = tables_dir.join(format!("{}.parquet", table.name()));
            let meta_path = tables_dir.join(format!("{}.meta.json", table.name()));

            let parquet_bytes = encode_parquet(table)?;
            write_bytes(&parquet_path, &parquet_bytes)?;
            let content_sha256 = sha256_hex(&parquet_bytes);

            let meta = TableMetadata::new(
                &self.profile,
                table.name().to_string(),
                table.schema().to_vec(),
                table.natural_key().to_vec(),
                table.row_count() as u64,
                content_sha256.clone(),
            );
            let meta_bytes = serialize_json(&meta, &meta_path)?;
            write_bytes(&meta_path, &meta_bytes)?;
            let metadata_sha256 = sha256_hex(&meta_bytes);

            entries.push(ManifestEntry {
                name: table.name().to_string(),
                row_count: table.row_count() as u64,
                content_sha256,
                metadata_sha256,
            });
        }

        // BTreeMap iteration is already sorted; sort defensively in case a
        // later refactor swaps the storage type.
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        let aggregate_sha256 = compute_aggregate_hash(&entries);

        let manifest = Manifest {
            format_version: self.profile.version.clone(),
            tables: entries,
            aggregate_sha256,
        };
        let manifest_bytes = serialize_json(&manifest, &manifest_path)?;
        write_bytes(&manifest_path, &manifest_bytes)?;

        // Write execution-DB bundle for fast loading by `moves run --snapshot`.
        crate::bundle::write_execution_bundle(dir, &self.tables)?;

        Ok(())
    }

    /// Load a snapshot from `dir`, validating manifest hashes.
    pub fn load(dir: &Path) -> Result<Self> {
        let manifest_path = dir.join(MANIFEST_FILE);
        let manifest_bytes = fs::read(&manifest_path).map_err(|source| {
            if source.kind() == std::io::ErrorKind::NotFound {
                Error::ManifestMissing {
                    path: manifest_path.clone(),
                }
            } else {
                Error::Io {
                    path: manifest_path.clone(),
                    source,
                }
            }
        })?;
        let manifest: Manifest =
            serde_json::from_slice(&manifest_bytes).map_err(|source| Error::Json {
                path: manifest_path.clone(),
                source,
            })?;
        // Read every version this crate knows, not just the one it writes:
        // the committed corpus stays on the older format until a scheduled
        // recapture sweep migrates it, and the regression gates diff against
        // it in the meantime.
        let Some(profile) = FormatProfile::for_version(&manifest.format_version) else {
            return Err(Error::UnsupportedFormatVersion {
                path: manifest_path,
                actual: manifest.format_version,
                expected: SUPPORTED_FORMAT_VERSIONS.join(", "),
            });
        };

        // Verify the aggregate hash before doing any expensive parquet
        // decoding. If the manifest itself was tampered with this catches it
        // up front.
        let recomputed_aggregate = compute_aggregate_hash(&manifest.tables);
        if recomputed_aggregate != manifest.aggregate_sha256 {
            return Err(Error::AggregateHashMismatch {
                path: manifest_path.clone(),
                manifest_hash: manifest.aggregate_sha256,
                computed_hash: recomputed_aggregate,
            });
        }

        let tables_dir = dir.join(TABLES_SUBDIR);
        let mut snapshot = Snapshot::new();
        snapshot.profile = profile;

        for entry in &manifest.tables {
            let parquet_path = tables_dir.join(format!("{}.parquet", entry.name));
            let meta_path = tables_dir.join(format!("{}.meta.json", entry.name));

            let meta_bytes = fs::read(&meta_path).map_err(|source| Error::Io {
                path: meta_path.clone(),
                source,
            })?;
            let computed_meta = sha256_hex(&meta_bytes);
            if computed_meta != entry.metadata_sha256 {
                return Err(Error::ContentHashMismatch {
                    path: meta_path,
                    table: entry.name.clone(),
                    manifest_hash: entry.metadata_sha256.clone(),
                    computed_hash: computed_meta,
                });
            }
            let meta: TableMetadata =
                serde_json::from_slice(&meta_bytes).map_err(|source| Error::Json {
                    path: meta_path.clone(),
                    source,
                })?;

            let parquet_bytes = fs::read(&parquet_path).map_err(|source| Error::Io {
                path: parquet_path.clone(),
                source,
            })?;
            let computed_content = sha256_hex(&parquet_bytes);
            if computed_content != entry.content_sha256 {
                return Err(Error::ContentHashMismatch {
                    path: parquet_path,
                    table: entry.name.clone(),
                    manifest_hash: entry.content_sha256.clone(),
                    computed_hash: computed_content,
                });
            }

            let table = decode_parquet(
                entry.name.clone(),
                meta.schema,
                meta.natural_key,
                parquet_bytes,
            )?;
            snapshot.add_table(table)?;
        }

        Ok(snapshot)
    }

    /// Return the snapshot's aggregate hash by re-deriving it from the in-memory
    /// tables. Equivalent to writing the snapshot and reading
    /// `manifest.aggregate_sha256` — useful for callers that want the
    /// content-address without touching disk.
    pub fn aggregate_hash(&self) -> Result<String> {
        let mut entries = Vec::with_capacity(self.tables.len());
        for table in self.tables.values() {
            let parquet_bytes = encode_parquet(table)?;
            let content_sha256 = sha256_hex(&parquet_bytes);
            let meta = TableMetadata::new(
                &self.profile,
                table.name().to_string(),
                table.schema().to_vec(),
                table.natural_key().to_vec(),
                table.row_count() as u64,
                content_sha256.clone(),
            );
            // Use the same JSON encoding the writer uses so the hash matches.
            let meta_bytes = serialize_json_inmem(&meta)?;
            let metadata_sha256 = sha256_hex(&meta_bytes);
            entries.push(ManifestEntry {
                name: table.name().to_string(),
                row_count: table.row_count() as u64,
                content_sha256,
                metadata_sha256,
            });
        }
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(compute_aggregate_hash(&entries))
    }
}

fn encode_parquet(table: &Table) -> Result<Vec<u8>> {
    let (schema, batch) = table.to_record_batch()?;
    let props = WriterProperties::builder()
        .set_created_by(PARQUET_CREATED_BY.to_string())
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .build();
    let mut buf: Vec<u8> = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(buf)
}

fn decode_parquet(
    name: String,
    schema_spec: Vec<ColumnSpec>,
    natural_key: Vec<String>,
    bytes: Vec<u8>,
) -> Result<Table> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))?.build()?;
    let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<Vec<_>, _>>()?;

    if batches.is_empty() {
        let cols = empty_columns_for(&schema_spec);
        return Table::from_normalized(name, schema_spec, natural_key, cols);
    }

    let batch = if batches.len() == 1 {
        batches.into_iter().next().unwrap()
    } else {
        let schema = batches[0].schema();
        concat_batches(&schema, batches.iter())?
    };

    Table::from_record_batch(name, schema_spec, natural_key, &batch)
}

fn empty_columns_for(spec: &[ColumnSpec]) -> Vec<crate::table::NormalizedColumn> {
    use crate::table::NormalizedColumn;
    spec.iter()
        .map(|s| match s.kind {
            crate::format::ColumnKind::Int64 => NormalizedColumn::Int64(Vec::new()),
            crate::format::ColumnKind::Float64 => NormalizedColumn::Float64String(Vec::new()),
            crate::format::ColumnKind::Utf8 => NormalizedColumn::Utf8(Vec::new()),
            crate::format::ColumnKind::Boolean => NormalizedColumn::Boolean(Vec::new()),
        })
        .collect()
}

fn write_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    fs::write(path, bytes).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn serialize_json<T: serde::Serialize>(value: &T, path: &Path) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec_pretty(value).map_err(|source| Error::Json {
        path: path.to_path_buf(),
        source,
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn serialize_json_inmem<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec_pretty(value).map_err(|source| Error::Json {
        path: std::path::PathBuf::from("<in-memory>"),
        source,
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::ColumnKind;
    use crate::table::{TableBuilder, Value};

    use tempfile::tempdir;

    fn sample_snapshot() -> Snapshot {
        let mut tb_a = TableBuilder::new(
            "alpha",
            [
                ("id".to_string(), ColumnKind::Int64),
                ("value".to_string(), ColumnKind::Float64),
                ("label".to_string(), ColumnKind::Utf8),
                ("flag".to_string(), ColumnKind::Boolean),
            ],
        )
        .unwrap()
        .with_natural_key(["id"])
        .unwrap();
        tb_a.push_row([
            Value::Int64(2),
            Value::Float64(2.5),
            Value::Utf8("two".into()),
            Value::Boolean(true),
        ])
        .unwrap();
        tb_a.push_row([
            Value::Int64(1),
            Value::Float64(1.0 + 1e-13),
            Value::Utf8("one".into()),
            Value::Boolean(false),
        ])
        .unwrap();
        tb_a.push_row([Value::Int64(3), Value::Null, Value::Null, Value::Null])
            .unwrap();
        let table_a = tb_a.build().unwrap();

        let mut tb_b = TableBuilder::new("beta", [("k".to_string(), ColumnKind::Utf8)])
            .unwrap()
            .with_natural_key(["k"])
            .unwrap();
        tb_b.push_row([Value::Utf8("z".into())]).unwrap();
        tb_b.push_row([Value::Utf8("a".into())]).unwrap();
        let table_b = tb_b.build().unwrap();

        let mut s = Snapshot::new();
        s.add_table(table_a).unwrap();
        s.add_table(table_b).unwrap();
        s
    }

    #[test]
    fn write_and_load_round_trips() {
        let dir = tempdir().unwrap();
        let s1 = sample_snapshot();
        s1.write(dir.path()).unwrap();
        let s2 = Snapshot::load(dir.path()).unwrap();

        // Same table set, same content.
        let names1: Vec<_> = s1.table_names().collect();
        let names2: Vec<_> = s2.table_names().collect();
        assert_eq!(names1, names2);
        for name in names1 {
            assert_eq!(s1.table(name).unwrap(), s2.table(name).unwrap());
        }
    }

    #[test]
    fn write_is_byte_deterministic() {
        let s = sample_snapshot();
        let dir1 = tempdir().unwrap();
        let dir2 = tempdir().unwrap();
        s.write(dir1.path()).unwrap();
        s.write(dir2.path()).unwrap();

        for rel in [
            "manifest.json",
            "tables/alpha.parquet",
            "tables/alpha.meta.json",
            "tables/beta.parquet",
            "tables/beta.meta.json",
        ] {
            let a = fs::read(dir1.path().join(rel)).unwrap();
            let b = fs::read(dir2.path().join(rel)).unwrap();
            assert_eq!(a, b, "file {rel} differs across writes");
        }
    }

    #[test]
    fn write_load_write_is_byte_identical() {
        let dir1 = tempdir().unwrap();
        let dir2 = tempdir().unwrap();
        let s1 = sample_snapshot();
        s1.write(dir1.path()).unwrap();
        let s2 = Snapshot::load(dir1.path()).unwrap();
        s2.write(dir2.path()).unwrap();

        for rel in [
            "manifest.json",
            "tables/alpha.parquet",
            "tables/alpha.meta.json",
            "tables/beta.parquet",
            "tables/beta.meta.json",
        ] {
            let a = fs::read(dir1.path().join(rel)).unwrap();
            let b = fs::read(dir2.path().join(rel)).unwrap();
            assert_eq!(a, b, "round-trip differs at {rel}");
        }
    }

    #[test]
    fn aggregate_hash_matches_manifest() {
        let dir = tempdir().unwrap();
        let s = sample_snapshot();
        s.write(dir.path()).unwrap();
        let in_memory = s.aggregate_hash().unwrap();
        let manifest_bytes = fs::read(dir.path().join(MANIFEST_FILE)).unwrap();
        let manifest: Manifest = serde_json::from_slice(&manifest_bytes).unwrap();
        assert_eq!(in_memory, manifest.aggregate_sha256);
    }

    #[test]
    fn manifest_missing_is_diagnosed() {
        let dir = tempdir().unwrap();
        let err = Snapshot::load(dir.path()).unwrap_err();
        assert!(matches!(err, Error::ManifestMissing { .. }));
    }

    #[test]
    fn content_hash_mismatch_detected() {
        let dir = tempdir().unwrap();
        let s = sample_snapshot();
        s.write(dir.path()).unwrap();
        let parquet = dir.path().join("tables/alpha.parquet");
        // Tamper with one byte. Pick the last byte to avoid header magic.
        let mut bytes = fs::read(&parquet).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        fs::write(&parquet, bytes).unwrap();
        let err = Snapshot::load(dir.path()).unwrap_err();
        assert!(matches!(err, Error::ContentHashMismatch { .. }));
    }

    #[test]
    fn write_creates_execution_bundle_when_execdb_tables_present() {
        use crate::bundle::BUNDLE_FILE;

        let dir = tempdir().unwrap();
        let mut s = Snapshot::new();

        // Add an execution-DB table (prefix matches db__movesexecution).
        let mut tb = TableBuilder::new(
            "db__movesexecution1__activitytype",
            [("id".to_string(), ColumnKind::Int64)],
        )
        .unwrap()
        .with_natural_key(["id"])
        .unwrap();
        tb.push_row([Value::Int64(1)]).unwrap();
        s.add_table(tb.build().unwrap()).unwrap();

        s.write(dir.path()).unwrap();

        let bundle_path = dir.path().join("tables").join(BUNDLE_FILE);
        assert!(
            bundle_path.exists(),
            "execution-db.bundle should be written when execution-DB tables are present"
        );
    }

    #[test]
    fn write_skips_execution_bundle_when_no_execdb_tables() {
        use crate::bundle::BUNDLE_FILE;

        let dir = tempdir().unwrap();
        let s = sample_snapshot(); // only alpha/beta, no db__movesexecution prefix
        s.write(dir.path()).unwrap();

        let bundle_path = dir.path().join("tables").join(BUNDLE_FILE);
        assert!(
            !bundle_path.exists(),
            "execution-db.bundle should not be created when there are no execution-DB tables"
        );
    }

    #[test]
    fn duplicate_table_rejected() {
        let mut s = Snapshot::new();
        let mut tb1 = TableBuilder::new("t", [("a".to_string(), ColumnKind::Int64)]).unwrap();
        tb1.push_row([Value::Int64(1)]).unwrap();
        let t1 = tb1.build().unwrap();
        s.add_table(t1).unwrap();

        let mut tb2 = TableBuilder::new("t", [("a".to_string(), ColumnKind::Int64)]).unwrap();
        tb2.push_row([Value::Int64(2)]).unwrap();
        let t2 = tb2.build().unwrap();
        let err = s.add_table(t2).unwrap_err();
        assert!(matches!(err, Error::DuplicateTable { .. }));
    }
    // ---- format version / float encoding --------------------------------

    /// A v1 fixture, assembled by hand: v1-encoded cells in a v1-profile
    /// snapshot. Nothing in the repo can produce these any more, so the test
    /// builds one to prove the reader still handles the committed corpus.
    fn v1_snapshot() -> Snapshot {
        use crate::format::{float_to_fixed_decimal, V1_FLOAT_DECIMALS};
        use crate::table::NormalizedColumn;

        let schema = vec![
            ColumnSpec {
                name: "id".into(),
                kind: ColumnKind::Int64,
            },
            ColumnSpec {
                name: "rate".into(),
                kind: ColumnKind::Float64,
            },
        ];
        let cells: Vec<Option<String>> = [1.105e-9, 1.9e-11, 1.5]
            .iter()
            .map(|&v| Some(float_to_fixed_decimal(v, V1_FLOAT_DECIMALS)))
            .collect();
        let table = Table::from_normalized(
            "db__movesexecutionx__nrdioxinemissionrate".to_string(),
            schema,
            vec!["id".to_string()],
            vec![
                NormalizedColumn::Int64(vec![Some(1), Some(2), Some(3)]),
                NormalizedColumn::Float64String(cells),
            ],
        )
        .unwrap();

        let mut s = Snapshot::with_profile(
            crate::format::FormatProfile::for_version("moves-snapshot/v1").unwrap(),
        );
        s.add_table(table).unwrap();
        s
    }

    #[test]
    fn manifest_distinguishes_v1_from_v2() {
        // --- v1 ---
        let d1 = tempdir().unwrap();
        v1_snapshot().write(d1.path()).unwrap();
        let m1: Manifest =
            serde_json::from_slice(&fs::read(d1.path().join("manifest.json")).unwrap()).unwrap();
        assert_eq!(m1.format_version, "moves-snapshot/v1");
        let meta1: TableMetadata = serde_json::from_slice(
            &fs::read(
                d1.path()
                    .join("tables/db__movesexecutionx__nrdioxinemissionrate.meta.json"),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(meta1.format_version, "moves-snapshot/v1");
        assert_eq!(meta1.float_decimals, Some(12));
        assert_eq!(meta1.float_encoding, None);
        assert_eq!(
            meta1.float_encoding(),
            crate::format::FloatEncoding::FixedDecimals { decimals: 12 }
        );
        // The v1 rule's absolute floor, which is what a consumer derived from
        // `float_decimals`.
        assert_eq!(meta1.float_encoding().absolute_quantum(1.0), 0.5e-12);

        // --- v2 ---
        let mut tb = TableBuilder::new(
            "db__movesexecutionx__nrdioxinemissionrate",
            [
                ("id".to_string(), ColumnKind::Int64),
                ("rate".to_string(), ColumnKind::Float64),
            ],
        )
        .unwrap()
        .with_natural_key(["id"])
        .unwrap();
        for (i, v) in [1.105e-9, 1.9e-11, 1.5].iter().enumerate() {
            tb.push_row([Value::Int64(i as i64 + 1), Value::Float64(*v)])
                .unwrap();
        }
        let mut s2 = Snapshot::new();
        s2.add_table(tb.build().unwrap()).unwrap();
        let d2 = tempdir().unwrap();
        s2.write(d2.path()).unwrap();

        let m2: Manifest =
            serde_json::from_slice(&fs::read(d2.path().join("manifest.json")).unwrap()).unwrap();
        assert_eq!(m2.format_version, "moves-snapshot/v2");
        let meta2: TableMetadata = serde_json::from_slice(
            &fs::read(
                d2.path()
                    .join("tables/db__movesexecutionx__nrdioxinemissionrate.meta.json"),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(meta2.format_version, "moves-snapshot/v2");
        assert_eq!(meta2.float_decimals, None);
        assert_eq!(
            meta2.float_encoding,
            Some(crate::format::FloatEncoding::CURRENT)
        );
        // Lossless: nothing to floor a tolerance at.
        assert_eq!(meta2.float_encoding().absolute_quantum(1.0), 0.0);

        // And the cells really do differ: v1 kept 2 significant digits of
        // 1.9e-11, v2 keeps all of them.
        let t1 = Snapshot::load(d1.path()).unwrap();
        let t2 = Snapshot::load(d2.path()).unwrap();
        let cell = |s: &Snapshot| {
            let tbl = s
                .table("db__movesexecutionx__nrdioxinemissionrate")
                .unwrap();
            let i = tbl.column_index("rate").unwrap();
            tbl.columns()[i].cell_string(1)
        };
        assert_eq!(cell(&t1).as_deref(), Some("0.000000000019"));
        assert_eq!(cell(&t2).as_deref(), Some("1.9e-11"));
    }

    #[test]
    fn v1_snapshot_loads_and_rewrites_byte_identically() {
        let d1 = tempdir().unwrap();
        let d2 = tempdir().unwrap();
        v1_snapshot().write(d1.path()).unwrap();
        let loaded = Snapshot::load(d1.path()).unwrap();
        assert_eq!(loaded.format_version(), "moves-snapshot/v1");
        assert_eq!(
            loaded.profile().float_encoding,
            crate::format::FloatEncoding::V1
        );
        loaded.write(d2.path()).unwrap();
        for rel in [
            "manifest.json",
            "tables/db__movesexecutionx__nrdioxinemissionrate.parquet",
            "tables/db__movesexecutionx__nrdioxinemissionrate.meta.json",
        ] {
            assert_eq!(
                fs::read(d1.path().join(rel)).unwrap(),
                fs::read(d2.path().join(rel)).unwrap(),
                "{rel}"
            );
        }
        // A loaded v1 snapshot is not silently relabelled v2.
        assert_eq!(loaded.aggregate_hash().unwrap(), {
            let m: Manifest =
                serde_json::from_slice(&fs::read(d1.path().join("manifest.json")).unwrap())
                    .unwrap();
            m.aggregate_sha256
        });
    }

    #[test]
    fn unknown_format_version_is_rejected_with_the_supported_list() {
        let dir = tempdir().unwrap();
        sample_snapshot().write(dir.path()).unwrap();
        let mp = dir.path().join("manifest.json");
        let mut m: Manifest = serde_json::from_slice(&fs::read(&mp).unwrap()).unwrap();
        m.format_version = "moves-snapshot/v99".into();
        fs::write(&mp, serde_json::to_vec_pretty(&m).unwrap()).unwrap();
        let err = Snapshot::load(dir.path()).unwrap_err();
        match err {
            Error::UnsupportedFormatVersion {
                actual, expected, ..
            } => {
                assert_eq!(actual, "moves-snapshot/v99");
                assert_eq!(expected, "moves-snapshot/v1, moves-snapshot/v2");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    /// Extreme magnitudes survive a real write/load cycle bit-for-bit, which
    /// the v1 format could not do for anything below ~1e-12.
    #[test]
    fn extreme_magnitudes_round_trip_through_a_snapshot() {
        let values = [
            0.0_f64,
            1.5,
            -1.5,
            std::f64::consts::PI,
            1e-9,
            1.105e-9,
            1.9e-11,
            4.7e-13,
            1e-300,
            f64::MIN_POSITIVE,
            f64::from_bits(1),
            f64::from_bits(0x000f_ffff_ffff_ffff),
            1e300,
            f64::MAX,
            f64::MIN,
        ];
        let mut tb = TableBuilder::new(
            "t",
            [
                ("id".to_string(), ColumnKind::Int64),
                ("v".to_string(), ColumnKind::Float64),
            ],
        )
        .unwrap()
        .with_natural_key(["id"])
        .unwrap();
        for (i, v) in values.iter().enumerate() {
            tb.push_row([Value::Int64(i as i64), Value::Float64(*v)])
                .unwrap();
        }
        let mut s = Snapshot::new();
        s.add_table(tb.build().unwrap()).unwrap();
        let dir = tempdir().unwrap();
        s.write(dir.path()).unwrap();

        let back = Snapshot::load(dir.path()).unwrap();
        let tbl = back.table("t").unwrap();
        let vi = tbl.column_index("v").unwrap();
        for (i, expect) in values.iter().enumerate() {
            let cell = tbl.columns()[vi].cell_string(i).unwrap();
            let got = crate::format::parse_canonical_float(&cell).unwrap();
            assert_eq!(
                got.to_bits(),
                expect.to_bits(),
                "row {i}: {expect:?} stored as {cell}"
            );
        }
    }
}

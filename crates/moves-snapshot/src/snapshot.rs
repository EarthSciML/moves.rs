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
use crate::format::{ColumnSpec, FORMAT_VERSION, PARQUET_CREATED_BY};
use crate::manifest::{compute_aggregate_hash, sha256_hex, Manifest, ManifestEntry, TableMetadata};
use crate::table::Table;

const TABLES_SUBDIR: &str = "tables";
const MANIFEST_FILE: &str = "manifest.json";

/// In-memory collection of normalized tables that maps to a snapshot directory.
///
/// Tables are stored keyed by name in lexicographic order, so iteration —
/// and the resulting on-disk manifest — is deterministic.
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    tables: BTreeMap<String, Table>,
}

impl Snapshot {
    pub fn new() -> Self {
        Self::default()
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
            format_version: FORMAT_VERSION.to_string(),
            tables: entries,
            aggregate_sha256,
        };
        let manifest_bytes = serialize_json(&manifest, &manifest_path)?;
        write_bytes(&manifest_path, &manifest_bytes)?;

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
        if manifest.format_version != FORMAT_VERSION {
            return Err(Error::UnsupportedFormatVersion {
                path: manifest_path,
                actual: manifest.format_version,
                expected: FORMAT_VERSION.to_string(),
            });
        }

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
}

//! Parquet writer for PDB-imported tables.
//!
//! Output convention matches `moves-default-db-convert::parquet_writer`
//! so importer-emitted Parquet sits next to default-DB Parquet and the
//! Phase 4 Task 82 lazy reader can scan either:
//!
//! * Uncompressed; no dictionary; no statistics; `PARQUET_1_0` writer
//!   version. Byte-stable for a fixed input.
//! * Atomic write via `.tmp` rename so a crash mid-write never leaves
//!   a torn Parquet file in the output tree.
//! * SHA-256 over the file bytes, returned alongside row count for the
//!   import manifest.

use std::path::{Path, PathBuf};

use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};

/// `created_by` string written into the Parquet file footer. Matches
/// the "moves-default-db-convert v0.1" naming so cross-tool readers
/// can identify which producer wrote a file.
pub const PARQUET_CREATED_BY: &str = "moves-importer-pdb v0.1";

/// One Parquet output: bytes, row count, and SHA-256.
#[derive(Debug, Clone)]
pub struct ParquetOutput {
    pub bytes: Vec<u8>,
    pub row_count: u64,
    pub sha256: String,
}

/// Encode an Arrow [`RecordBatch`] to Parquet bytes using the
/// importer's deterministic settings.
pub fn encode(batch: &RecordBatch) -> Result<ParquetOutput> {
    let props = WriterProperties::builder()
        .set_created_by(PARQUET_CREATED_BY.to_string())
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .build();
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))?;
        if batch.num_rows() > 0 {
            writer.write(batch)?;
        }
        writer.close()?;
    }
    let row_count = batch.num_rows() as u64;
    let sha256 = sha256_hex(&buf);
    Ok(ParquetOutput {
        bytes: buf,
        row_count,
        sha256,
    })
}

/// Atomic write: encode, write to `<path>.tmp`, fsync (skipped — match
/// the default-DB convert pipeline), rename. Creates parent dirs as
/// needed.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| Error::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let tmp = with_extension_suffix(path, ".tmp");
    std::fs::write(&tmp, bytes).map_err(|source| Error::Io {
        path: tmp.clone(),
        source,
    })?;
    std::fs::rename(&tmp, path).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(())
}

/// Hex SHA-256 of the input bytes. Lowercase, 64 chars.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    let digest = h.finalize();
    let mut out = String::with_capacity(64);
    for b in digest {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

fn with_extension_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(suffix);
    PathBuf::from(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::sync::Arc;

    fn small_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("linkID", DataType::Int64, false),
            Field::new("speed", DataType::Float64, true),
        ]));
        let link_ids = Int64Array::from(vec![1_i64, 2, 3]);
        let speeds = Float64Array::from(vec![Some(55.0), None, Some(45.0)]);
        RecordBatch::try_new(schema, vec![Arc::new(link_ids), Arc::new(speeds)]).unwrap()
    }

    #[test]
    fn encode_round_trips() {
        let batch = small_batch();
        let out = encode(&batch).unwrap();
        assert_eq!(out.row_count, 3);
        let bytes = Bytes::from(out.bytes);
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .unwrap();
        let read = reader.next().unwrap().unwrap();
        assert_eq!(read.num_rows(), 3);
        assert_eq!(read.num_columns(), 2);
    }

    #[test]
    fn encoding_is_deterministic() {
        let batch = small_batch();
        let a = encode(&batch).unwrap();
        let b = encode(&batch).unwrap();
        assert_eq!(a.sha256, b.sha256);
        assert_eq!(a.bytes, b.bytes);
    }

    #[test]
    fn atomic_write_creates_parent_dirs_and_renames_tmp() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("nested/out/foo.parquet");
        let batch = small_batch();
        let out = encode(&batch).unwrap();
        write_atomic(&target, &out.bytes).unwrap();
        assert!(target.exists());
        assert!(!with_extension_suffix(&target, ".tmp").exists());
        let written = std::fs::read(&target).unwrap();
        assert_eq!(written, out.bytes);
    }

    #[test]
    fn empty_batch_writes_valid_parquet() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let empty = RecordBatch::new_empty(schema.clone());
        let out = encode(&empty).unwrap();
        assert_eq!(out.row_count, 0);
        let bytes = Bytes::from(out.bytes);
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .unwrap();
        let count: usize = reader.into_iter().map(|b| b.unwrap().num_rows()).sum();
        assert_eq!(count, 0);
    }
}

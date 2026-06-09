//! Encode validated rows as Arrow record batches and write Parquet.
//!
//! Determinism contract: same pinning as `moves-default-db-convert`'s
//! Parquet writer (uncompressed, no dictionary, no statistics,
//! `PARQUET_1_0` writer version, fixed `created_by` string). Combined
//! with the validator preserving input row order, re-importing the
//! same CSV produces byte-identical Parquet output.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Builder, Int64Builder};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};
use crate::schema::{arrow_schema, ColumnKind, COLUMNS};
use crate::validate::{TypedRow, TypedValue};

/// Stable `created_by` string baked into every Parquet file we write.
/// Keep in sync with the converter so all default-DB-shaped Parquet
/// produced by `moves.rs` carries one of two known signatures.
pub const PARQUET_CREATED_BY: &str = "moves-import-lev v0.1";

/// Result of encoding rows to Parquet.
#[derive(Debug, Clone)]
pub struct ParquetOutput {
    /// Raw Parquet bytes. The encoder buffers in memory so callers can
    /// hash without re-reading from disk.
    pub bytes: Vec<u8>,
    /// Number of rows actually written.
    pub row_count: u64,
    /// Lower-case hex SHA-256 of [`Self::bytes`]. Distinct CSVs hash to
    /// distinct outputs; identical CSVs (modulo cell whitespace / NULL
    /// spellings) hash identically.
    pub sha256: String,
}

/// Encode validated rows into a Parquet byte buffer.
pub fn encode(rows: &[TypedRow]) -> Result<ParquetOutput> {
    let schema = arrow_schema();
    let arrays = build_arrays(rows)?;
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;

    let props = WriterProperties::builder()
        .set_created_by(PARQUET_CREATED_BY.to_string())
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .build();
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props))?;
        if batch.num_rows() > 0 {
            writer.write(&batch)?;
        }
        writer.close()?;
    }
    let sha = sha256_hex(&buf);
    Ok(ParquetOutput {
        bytes: buf,
        row_count: rows.len() as u64,
        sha256: sha,
    })
}

/// Write `bytes` to `path` atomically via a `.tmp` sibling rename so a
/// crash mid-write does not leave a partial Parquet on disk.
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

fn with_extension_suffix(path: &Path, suffix: &str) -> std::path::PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(suffix);
    s.into()
}

fn build_arrays(rows: &[TypedRow]) -> Result<Vec<ArrayRef>> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(COLUMNS.len());
    for (col_idx, column) in COLUMNS.iter().enumerate() {
        let array: ArrayRef = match column.kind {
            ColumnKind::Integer => {
                let mut builder = Int64Builder::with_capacity(rows.len());
                for row in rows {
                    match row.values[col_idx] {
                        TypedValue::Integer(v) => builder.append_value(v),
                        TypedValue::Null => builder.append_null(),
                        TypedValue::Float(_) => {
                            // The validator guarantees this can't happen.
                            unreachable!(
                                "column {} expects Integer, validator produced Float",
                                column.name
                            );
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            ColumnKind::Float => {
                let mut builder = Float64Builder::with_capacity(rows.len());
                for row in rows {
                    match row.values[col_idx] {
                        TypedValue::Float(v) => builder.append_value(v),
                        TypedValue::Null => builder.append_null(),
                        TypedValue::Integer(_) => {
                            unreachable!(
                                "column {} expects Float, validator produced Integer",
                                column.name
                            );
                        }
                    }
                }
                Arc::new(builder.finish())
            }
        };
        arrays.push(array);
    }
    Ok(arrays)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    hex_lower(&digest)
}

fn hex_lower(bytes: &[u8]) -> String {
    const TABLE: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(TABLE[(b >> 4) as usize] as char);
        out.push(TABLE[(b & 0xF) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validate::{TypedRow, TypedValue};
    use arrow::array::Array;
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    #[allow(clippy::too_many_arguments)]
    fn row(
        line: usize,
        sb: i64,
        pp: i64,
        om: i64,
        ag: i64,
        rate: f64,
        cv: Option<f64>,
        im: Option<f64>,
        im_cv: Option<f64>,
        ds: Option<i64>,
    ) -> TypedRow {
        let mut values = vec![
            TypedValue::Integer(sb),
            TypedValue::Integer(pp),
            TypedValue::Integer(om),
            TypedValue::Integer(ag),
            TypedValue::Float(rate),
            cv.map_or(TypedValue::Null, TypedValue::Float),
            im.map_or(TypedValue::Null, TypedValue::Float),
            im_cv.map_or(TypedValue::Null, TypedValue::Float),
            ds.map_or(TypedValue::Null, TypedValue::Integer),
        ];
        // Force size match — tests would catch a schema length change.
        values.truncate(COLUMNS.len());
        TypedRow { line, values }
    }

    fn read_back(bytes: &[u8]) -> RecordBatch {
        let bytes = Bytes::from(bytes.to_vec());
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .unwrap();
        reader.next().unwrap().unwrap()
    }

    #[test]
    fn round_trip_one_row() {
        let rows = vec![row(
            2,
            1000,
            101,
            1,
            1,
            0.5,
            Some(0.05),
            None,
            None,
            Some(7),
        )];
        let out = encode(&rows).unwrap();
        assert_eq!(out.row_count, 1);
        let batch = read_back(&out.bytes);
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), COLUMNS.len());
        // Spot-check: meanBaseRate column is index 4, value 0.5.
        let rate = batch
            .column(4)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(rate.value(0), 0.5);
        // meanBaseRateIM (index 6) is null.
        let im = batch
            .column(6)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert!(im.is_null(0));
        // dataSourceId (index 8) is 7.
        let ds = batch
            .column(8)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(ds.value(0), 7);
    }

    #[test]
    fn empty_rows_yields_empty_parquet() {
        let out = encode(&[]).unwrap();
        assert_eq!(out.row_count, 0);
        let bytes = Bytes::from(out.bytes.clone());
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .unwrap();
        // Empty-rows path may or may not emit an empty batch — the
        // important contract is "no panic, schema preserved".
        if let Some(batch) = reader.next() {
            let batch = batch.unwrap();
            assert_eq!(batch.num_rows(), 0);
        }
    }

    #[test]
    fn encoding_is_byte_deterministic() {
        let rows = vec![
            row(2, 1000, 101, 1, 1, 0.5, None, None, None, None),
            row(3, 1001, 101, 1, 1, 0.7, Some(0.05), None, None, Some(7)),
        ];
        let out1 = encode(&rows).unwrap();
        let out2 = encode(&rows).unwrap();
        assert_eq!(out1.bytes, out2.bytes);
        assert_eq!(out1.sha256, out2.sha256);
    }

    #[test]
    fn distinct_inputs_hash_distinctly() {
        let rows_a = vec![row(2, 1000, 101, 1, 1, 0.5, None, None, None, None)];
        let rows_b = vec![row(2, 1000, 101, 1, 1, 0.7, None, None, None, None)];
        let a = encode(&rows_a).unwrap();
        let b = encode(&rows_b).unwrap();
        assert_ne!(a.sha256, b.sha256);
    }

    #[test]
    fn write_atomic_creates_parent_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("nested/dir/out.parquet");
        write_atomic(&path, b"abc").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"abc");
    }
}

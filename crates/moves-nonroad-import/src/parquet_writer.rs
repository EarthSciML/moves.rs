//! Encode validated rows into Arrow record batches and Parquet bytes.
//!
//! Mirrors the determinism contract of `moves-default-db-convert`:
//! uncompressed, no dictionary, no statistics, `PARQUET_1_0` writer
//! version. Combined with the importer's deterministic row ordering
//! (CSV reads rows in user-supplied order; the schema-driven encoder
//! never reorders) this gives byte-stable Parquet output: re-running
//! against the same CSV yields identical content hashes.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};
use crate::schema::Column;

pub const PARQUET_CREATED_BY: &str = "moves-nonroad-import v0.1";

/// One row of typed cells. `None` is the user-input equivalent of NULL.
/// Variants match the per-column [`arrow::datatypes::DataType`] declared
/// in the table's [`Column`] slice.
#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    Null,
    Int(i64),
    Float(f64),
    Str(String),
}

pub type TypedRow = Vec<Cell>;

/// Result of encoding a set of rows to Parquet bytes.
#[derive(Debug, Clone)]
pub struct ParquetOutput {
    pub bytes: Vec<u8>,
    pub row_count: u64,
    pub sha256: String,
}

/// Build the Arrow schema from the column slice. Every column is nullable
/// because the user may legitimately leave non-PK columns blank.
pub fn arrow_schema(columns: &[Column]) -> SchemaRef {
    let fields: Vec<Field> = columns
        .iter()
        .map(|c| Field::new(c.name, c.arrow_type.clone(), true))
        .collect();
    Arc::new(ArrowSchema::new(fields))
}

/// Encode the rows into a Parquet byte buffer.
///
/// Returns [`Error::Internal`] if a [`Cell`] variant does not match the
/// column's declared `arrow_type` — that is a coercion bug in the table
/// importer, not a user-input error.
pub fn encode_parquet(columns: &[Column], rows: &[TypedRow]) -> Result<ParquetOutput> {
    let schema = arrow_schema(columns);
    let batch = build_record_batch(columns, rows, schema.clone())?;
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
    let sha256 = sha256_hex(&buf);
    Ok(ParquetOutput {
        bytes: buf,
        row_count: rows.len() as u64,
        sha256,
    })
}

/// Write `bytes` to `path`, creating parent directories. The write is
/// atomic via a `.tmp` sibling rename so a crash mid-write does not leave
/// a partial Parquet file in the output tree.
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

fn with_extension_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(suffix);
    PathBuf::from(s)
}

fn build_record_batch(
    columns: &[Column],
    rows: &[TypedRow],
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let row_count = rows.len();
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    for (col_idx, col) in columns.iter().enumerate() {
        let array: ArrayRef = match &col.arrow_type {
            DataType::Int64 => {
                let mut b = Int64Builder::with_capacity(row_count);
                for row in rows {
                    match &row[col_idx] {
                        Cell::Null => b.append_null(),
                        Cell::Int(v) => b.append_value(*v),
                        other => {
                            return Err(Error::Internal {
                                message: format!(
                                    "column '{}' declared Int64 but cell is {:?}",
                                    col.name, other
                                ),
                            });
                        }
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Float64 => {
                let mut b = Float64Builder::with_capacity(row_count);
                for row in rows {
                    match &row[col_idx] {
                        Cell::Null => b.append_null(),
                        Cell::Float(v) => b.append_value(*v),
                        other => {
                            return Err(Error::Internal {
                                message: format!(
                                    "column '{}' declared Float64 but cell is {:?}",
                                    col.name, other
                                ),
                            });
                        }
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Utf8 => {
                let mut b = StringBuilder::with_capacity(row_count, row_count * 16);
                for row in rows {
                    match &row[col_idx] {
                        Cell::Null => b.append_null(),
                        Cell::Str(s) => b.append_value(s),
                        other => {
                            return Err(Error::Internal {
                                message: format!(
                                    "column '{}' declared Utf8 but cell is {:?}",
                                    col.name, other
                                ),
                            });
                        }
                    }
                }
                Arc::new(b.finish())
            }
            other => {
                return Err(Error::Internal {
                    message: format!(
                        "column '{}' uses unsupported arrow type {other:?}",
                        col.name
                    ),
                });
            }
        };
        arrays.push(array);
    }
    Ok(RecordBatch::try_new(schema, arrays)?)
}

/// Hex-encoded SHA-256 of the bytes. Used for the manifest content hash.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Column, Rule};
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    fn cols() -> Vec<Column> {
        vec![
            Column {
                name: "id",
                mysql_type: "smallint(6)",
                arrow_type: DataType::Int64,
                primary_key: true,
                required: true,
                rule: Rule::None,
            },
            Column {
                name: "value",
                mysql_type: "float",
                arrow_type: DataType::Float64,
                primary_key: false,
                required: false,
                rule: Rule::None,
            },
            Column {
                name: "name",
                mysql_type: "char(10)",
                arrow_type: DataType::Utf8,
                primary_key: false,
                required: false,
                rule: Rule::None,
            },
        ]
    }

    fn rows() -> Vec<TypedRow> {
        vec![
            vec![Cell::Int(1), Cell::Float(1.5), Cell::Str("alpha".into())],
            vec![Cell::Int(2), Cell::Null, Cell::Str("beta".into())],
        ]
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
    fn encode_roundtrips_supported_types() {
        let out = encode_parquet(&cols(), &rows()).unwrap();
        assert_eq!(out.row_count, 2);
        assert_eq!(out.sha256.len(), 64);
        let batch = read_back(&out.bytes);
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);
        assert!(batch.column(1).is_null(1));
    }

    #[test]
    fn encoding_is_deterministic() {
        let a = encode_parquet(&cols(), &rows()).unwrap();
        let b = encode_parquet(&cols(), &rows()).unwrap();
        assert_eq!(a.sha256, b.sha256);
        assert_eq!(a.bytes, b.bytes);
    }

    #[test]
    fn empty_rows_produces_valid_parquet() {
        let out = encode_parquet(&cols(), &[]).unwrap();
        assert_eq!(out.row_count, 0);
        let bytes = Bytes::from(out.bytes);
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .unwrap();
        let n: usize = reader.map(|b| b.unwrap().num_rows()).sum();
        assert_eq!(n, 0);
    }

    #[test]
    fn type_mismatch_is_internal_error() {
        let bad: Vec<TypedRow> = vec![vec![
            Cell::Float(1.0),
            Cell::Float(2.0),
            Cell::Str("x".into()),
        ]];
        let err = encode_parquet(&cols(), &bad).unwrap_err();
        match err {
            Error::Internal { message } => {
                assert!(message.contains("Int64"));
                assert!(message.contains("'id'"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn write_atomic_renames_through_tmp() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.bin");
        write_atomic(&path, b"hello").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"hello");
        // No leftover .tmp file.
        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(entries, vec!["out.bin".to_string()]);
    }
}

//! Convert in-memory rows into Arrow record batches and Parquet bytes.
//!
//! ## Determinism contract
//!
//! Parquet writer settings are pinned for byte-stable output: uncompressed,
//! no dictionary, no statistics, `PARQUET_1_0` writer version. The source
//! TSV is `ORDER BY` every column on the way out of MariaDB, so rows are
//! delivered in a deterministic order. As long as the partition grouping
//! is stable (it is — we sort by the partition value before emitting),
//! re-running the pipeline against the same dump yields byte-identical
//! files and therefore identical content hashes.
//!
//! ## Type coercion
//!
//! TSV cells are `Option<String>`. We coerce per column:
//!
//! * `Int64`  — `String::parse::<i64>` → returns [`Error::Parse`] on miss.
//! * `Float64` — `String::parse::<f64>` → likewise.
//! * `Boolean` — `0`/`1`/`true`/`false` (case-insensitive); other values
//!   error.
//! * `Utf8`   — passthrough.
//!
//! Nulls (TSV `NULL` literal) flow through every kind as Arrow null.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};
use crate::tsv::SchemaColumn;

pub const PARQUET_CREATED_BY: &str = "moves-default-db-convert v0.1";

/// One row from the source TSV, decoded but not yet typed. `None` is SQL NULL.
pub type Row = Vec<Option<String>>;

/// Result of encoding a set of rows to a Parquet file.
#[derive(Debug, Clone)]
pub struct ParquetOutput {
    pub bytes: Vec<u8>,
    pub row_count: u64,
    pub sha256: String,
}

/// Build the Arrow schema (no nulls override — every column is nullable to
/// accept TSV NULLs verbatim).
pub fn arrow_schema(columns: &[SchemaColumn]) -> SchemaRef {
    let fields: Vec<Field> = columns
        .iter()
        .map(|c| Field::new(&c.name, c.arrow_type.clone(), true))
        .collect();
    Arc::new(ArrowSchema::new(fields))
}

/// Encode the rows into a Parquet byte buffer. Rows must be a vector of
/// rows of the same width as `columns`.
pub fn encode_parquet(columns: &[SchemaColumn], rows: &[Row]) -> Result<ParquetOutput> {
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
    let sha = sha256_hex(&buf);
    Ok(ParquetOutput {
        bytes: buf,
        row_count: rows.len() as u64,
        sha256: sha,
    })
}

/// Write `bytes` to `path`, creating parent directories as needed. The
/// write is atomic via a `.tmp` sibling rename so a crash mid-write does
/// not leave a partial Parquet file in the output tree.
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
    columns: &[SchemaColumn],
    rows: &[Row],
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let row_count = rows.len();
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());

    for (col_idx, col) in columns.iter().enumerate() {
        let array: ArrayRef = match &col.arrow_type {
            DataType::Int64 => {
                let mut b = Int64Builder::with_capacity(row_count);
                for (row_idx, row) in rows.iter().enumerate() {
                    match &row[col_idx] {
                        None => b.append_null(),
                        Some(s) => {
                            let v = s.parse::<i64>().map_err(|e| Error::Parse {
                                path: PathBuf::new(),
                                line: row_idx + 1,
                                message: format!(
                                    "column '{}' expected int64, got '{}': {}",
                                    col.name, s, e
                                ),
                            })?;
                            b.append_value(v);
                        }
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Float64 => {
                let mut b = Float64Builder::with_capacity(row_count);
                for (row_idx, row) in rows.iter().enumerate() {
                    match &row[col_idx] {
                        None => b.append_null(),
                        Some(s) => {
                            let v = s.parse::<f64>().map_err(|e| Error::Parse {
                                path: PathBuf::new(),
                                line: row_idx + 1,
                                message: format!(
                                    "column '{}' expected float64, got '{}': {}",
                                    col.name, s, e
                                ),
                            })?;
                            b.append_value(v);
                        }
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Boolean => {
                let mut b = BooleanBuilder::with_capacity(row_count);
                for (row_idx, row) in rows.iter().enumerate() {
                    match &row[col_idx] {
                        None => b.append_null(),
                        Some(s) => {
                            let v = match s.as_str() {
                                "0" => false,
                                "1" => true,
                                other => match other.to_ascii_lowercase().as_str() {
                                    "true" => true,
                                    "false" => false,
                                    _ => {
                                        return Err(Error::Parse {
                                            path: PathBuf::new(),
                                            line: row_idx + 1,
                                            message: format!(
                                                "column '{}' expected boolean, got '{}'",
                                                col.name, other
                                            ),
                                        });
                                    }
                                },
                            };
                            b.append_value(v);
                        }
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Utf8 => {
                let mut b = StringBuilder::with_capacity(row_count, row_count * 16);
                for row in rows.iter() {
                    match &row[col_idx] {
                        None => b.append_null(),
                        Some(s) => b.append_value(s),
                    }
                }
                Arc::new(b.finish())
            }
            other => {
                return Err(Error::Parse {
                    path: PathBuf::new(),
                    line: 0,
                    message: format!(
                        "internal: column '{}' has unsupported arrow type {other:?}",
                        col.name
                    ),
                });
            }
        };
        arrays.push(array);
    }

    Ok(RecordBatch::try_new(schema, arrays)?)
}

/// Hex-encoded SHA-256 of the given bytes.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    let digest = h.finalize();
    let mut out = String::with_capacity(64);
    for b in digest {
        out.push_str(&format!("{:02x}", b));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    fn col(name: &str, ty: DataType, pk: bool) -> SchemaColumn {
        SchemaColumn {
            name: name.to_string(),
            mysql_type: match &ty {
                DataType::Int64 => "int",
                DataType::Float64 => "double",
                DataType::Boolean => "bool",
                _ => "varchar",
            }
            .to_string(),
            arrow_type: ty,
            primary_key: pk,
        }
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
    fn encode_roundtrips_basic_types() {
        let cols = vec![
            col("id", DataType::Int64, true),
            col("v", DataType::Float64, false),
            col("name", DataType::Utf8, false),
            col("flag", DataType::Boolean, false),
        ];
        let rows: Vec<Row> = vec![
            vec![
                Some("1".to_string()),
                Some("1.5".to_string()),
                Some("alpha".to_string()),
                Some("1".to_string()),
            ],
            vec![
                Some("2".to_string()),
                None,
                Some("beta".to_string()),
                Some("0".to_string()),
            ],
        ];
        let out = encode_parquet(&cols, &rows).unwrap();
        assert_eq!(out.row_count, 2);
        assert_eq!(out.sha256.len(), 64);
        let batch = read_back(&out.bytes);
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);
    }

    #[test]
    fn encoding_is_deterministic() {
        let cols = vec![col("id", DataType::Int64, true)];
        let rows: Vec<Row> = vec![vec![Some("1".into())], vec![Some("2".into())]];
        let a = encode_parquet(&cols, &rows).unwrap();
        let b = encode_parquet(&cols, &rows).unwrap();
        assert_eq!(a.sha256, b.sha256);
        assert_eq!(a.bytes, b.bytes);
    }

    #[test]
    fn nulls_round_trip() {
        let cols = vec![
            col("id", DataType::Int64, true),
            col("x", DataType::Float64, false),
        ];
        let rows: Vec<Row> = vec![vec![None, None]];
        let out = encode_parquet(&cols, &rows).unwrap();
        let batch = read_back(&out.bytes);
        assert!(batch.column(0).is_null(0));
        assert!(batch.column(1).is_null(0));
    }

    #[test]
    fn parse_errors_carry_column_name() {
        let cols = vec![col("id", DataType::Int64, true)];
        let rows: Vec<Row> = vec![vec![Some("not a number".into())]];
        let err = encode_parquet(&cols, &rows).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("column 'id'")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn empty_rows_produces_valid_parquet() {
        let cols = vec![col("id", DataType::Int64, true)];
        let rows: Vec<Row> = vec![];
        let out = encode_parquet(&cols, &rows).unwrap();
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
    fn boolean_accepts_text_and_numeric() {
        let cols = vec![col("flag", DataType::Boolean, false)];
        let rows: Vec<Row> = vec![
            vec![Some("0".to_string())],
            vec![Some("1".to_string())],
            vec![Some("true".to_string())],
            vec![Some("False".to_string())],
        ];
        let out = encode_parquet(&cols, &rows).unwrap();
        assert_eq!(out.row_count, 4);
    }
}

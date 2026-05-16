//! Write a validated [`RecordBatch`] to byte-deterministic Parquet.
//!
//! The writer settings match
//! `moves-default-db-convert::parquet_writer` so a re-import of a
//! default-DB-derived CSV produces a Parquet file with the same
//! SHA-256 the converter wrote. Settings:
//!
//! * `Compression::UNCOMPRESSED`
//! * dictionary encoding disabled
//! * statistics disabled
//! * `PARQUET_1_0` writer version
//!
//! Rows are sorted lexicographically by primary-key columns before
//! writing. This is the same ordering MOVES' MariaDB dump produces
//! when the `dump-default-db.sh` driver runs `ORDER BY` every column.
//!
//! [`RecordBatch`]: arrow::record_batch::RecordBatch

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder, UInt32Array,
};
use arrow::compute::{lexsort_to_indices, take, SortColumn, SortOptions};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use sha2::{Digest, Sha256};

use crate::descriptor::TableDescriptor;
use crate::error::{Error, Result};

/// The `created_by` string written into every Parquet footer the
/// importer produces. Distinct from the converter's string so the
/// origin of a Parquet file is unambiguous.
pub const PARQUET_CREATED_BY: &str = "moves-importer v0.1";

/// One row group's worth of typed data plus the bytes that backed it.
#[derive(Debug, Clone)]
pub struct TableOutput {
    pub table_name: &'static str,
    pub bytes: Vec<u8>,
    pub row_count: u64,
    pub sha256: String,
}

/// Sort rows by `primary_key`, encode to Parquet bytes, and (optionally)
/// write to `output_path` atomically.
///
/// Pass `None` for `output_path` if the caller only needs the bytes
/// (e.g., for SHA-256 comparison against the converter's output).
pub fn write_table_parquet(
    descriptor: &TableDescriptor,
    batch: &RecordBatch,
    output_path: Option<&Path>,
) -> Result<TableOutput> {
    let sorted = sort_by_primary_key(descriptor, batch)?;

    let props = WriterProperties::builder()
        .set_created_by(PARQUET_CREATED_BY.to_string())
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .build();

    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, sorted.schema(), Some(props))?;
        if sorted.num_rows() > 0 {
            writer.write(&sorted)?;
        }
        writer.close()?;
    }

    let sha = {
        let mut hasher = Sha256::new();
        hasher.update(&buf);
        format!("{:x}", hasher.finalize())
    };

    if let Some(path) = output_path {
        write_atomic(path, &buf)?;
    }

    Ok(TableOutput {
        table_name: descriptor.name,
        bytes: buf,
        row_count: sorted.num_rows() as u64,
        sha256: sha,
    })
}

/// Sort a `RecordBatch` lexicographically by the descriptor's
/// primary-key columns. Columns are sorted ascending with nulls
/// first (matching `ORDER BY` semantics in MariaDB).
fn sort_by_primary_key(descriptor: &TableDescriptor, batch: &RecordBatch) -> Result<RecordBatch> {
    if descriptor.primary_key.is_empty() || batch.num_rows() == 0 {
        return Ok(batch.clone());
    }

    let mut sort_columns: Vec<SortColumn> = Vec::with_capacity(descriptor.primary_key.len());
    for &name in descriptor.primary_key {
        let col = batch
            .column_by_name(name)
            .expect("primary-key columns must be present in the descriptor");
        sort_columns.push(SortColumn {
            values: col.clone(),
            options: Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
        });
    }

    let indices: UInt32Array = lexsort_to_indices(&sort_columns, None)?;
    let new_columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(batch.schema(), new_columns)?)
}

/// Write `bytes` to `path` via a `.tmp` sibling rename — matches the
/// atomic-write convention in `moves-default-db-convert::parquet_writer`.
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
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

/// Build a `RecordBatch` from typed columns. Helper for tests and
/// callers that build batches programmatically. The columns must
/// match the descriptor in name, order, and Arrow type.
pub fn build_record_batch_from_columns(
    descriptor: &TableDescriptor,
    columns: Vec<ArrayRef>,
) -> Result<RecordBatch> {
    use arrow::datatypes::{Field, Schema};
    let fields: Vec<Field> = descriptor
        .columns
        .iter()
        .map(|c| Field::new(c.name, c.arrow_type(), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    Ok(RecordBatch::try_new(schema, columns)?)
}

/// Construct a non-null `Int64` array from a slice. Convenience for
/// tests that build descriptor-shaped batches by hand.
pub fn int64_array(values: impl IntoIterator<Item = i64>) -> ArrayRef {
    let mut b = Int64Builder::new();
    for v in values {
        b.append_value(v);
    }
    Arc::new(b.finish())
}

/// Construct a non-null `Float64` array from a slice.
pub fn float64_array(values: impl IntoIterator<Item = f64>) -> ArrayRef {
    let mut b = Float64Builder::new();
    for v in values {
        b.append_value(v);
    }
    Arc::new(b.finish())
}

/// Construct a non-null `Boolean` array from a slice.
pub fn bool_array(values: impl IntoIterator<Item = bool>) -> ArrayRef {
    let mut b = BooleanBuilder::new();
    for v in values {
        b.append_value(v);
    }
    Arc::new(b.finish())
}

/// Construct a non-null `Utf8` array from a slice.
pub fn string_array<S: AsRef<str>>(values: impl IntoIterator<Item = S>) -> ArrayRef {
    let mut b = StringBuilder::new();
    for v in values {
        b.append_value(v.as_ref());
    }
    Arc::new(b.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::descriptor::ColumnDescriptor;
    use crate::filter::Filter;

    static ZONE_ROAD_TYPE: TableDescriptor = TableDescriptor {
        name: "ZoneRoadType",
        columns: &[
            ColumnDescriptor::new("zoneID", Filter::Zone),
            ColumnDescriptor::new("roadTypeID", Filter::RoadType),
            ColumnDescriptor::new("SHOAllocFactor", Filter::NonNegative),
        ],
        primary_key: &["zoneID", "roadTypeID"],
    };

    #[test]
    fn rows_are_sorted_by_primary_key_columns_before_writing() {
        let batch = build_record_batch_from_columns(
            &ZONE_ROAD_TYPE,
            vec![
                int64_array([60371, 60371, 60371, 60371]),
                int64_array([4, 2, 3, 5]),
                float64_array([0.10, 0.25, 0.30, 0.35]),
            ],
        )
        .unwrap();

        let sorted = sort_by_primary_key(&ZONE_ROAD_TYPE, &batch).unwrap();
        let road_type = sorted
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(
            (0..sorted.num_rows())
                .map(|i| road_type.value(i))
                .collect::<Vec<_>>(),
            vec![2, 3, 4, 5]
        );
    }

    #[test]
    fn output_is_byte_deterministic_for_a_given_input() {
        let batch = build_record_batch_from_columns(
            &ZONE_ROAD_TYPE,
            vec![
                int64_array([60371, 60371, 60371, 60371]),
                int64_array([2, 3, 4, 5]),
                float64_array([0.25, 0.30, 0.10, 0.35]),
            ],
        )
        .unwrap();

        let a = write_table_parquet(&ZONE_ROAD_TYPE, &batch, None).unwrap();
        let b = write_table_parquet(&ZONE_ROAD_TYPE, &batch, None).unwrap();
        assert_eq!(a.sha256, b.sha256);
        assert_eq!(a.bytes, b.bytes);
        assert_eq!(a.row_count, 4);
    }

    #[test]
    fn output_path_is_written_atomically_when_provided() {
        let batch = build_record_batch_from_columns(
            &ZONE_ROAD_TYPE,
            vec![int64_array([60371]), int64_array([2]), float64_array([1.0])],
        )
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("ZoneRoadType.parquet");
        let out = write_table_parquet(&ZONE_ROAD_TYPE, &batch, Some(&path)).unwrap();
        assert!(path.exists());
        let on_disk = std::fs::read(&path).unwrap();
        assert_eq!(on_disk, out.bytes);
    }

    #[test]
    fn empty_table_writes_a_zero_row_parquet() {
        let batch = build_record_batch_from_columns(
            &ZONE_ROAD_TYPE,
            vec![int64_array([]), int64_array([]), float64_array([])],
        )
        .unwrap();
        let out = write_table_parquet(&ZONE_ROAD_TYPE, &batch, None).unwrap();
        assert_eq!(out.row_count, 0);
        assert!(
            !out.bytes.is_empty(),
            "must still emit a valid Parquet file"
        );
    }
}

//! Parquet writer for the AVFT table.
//!
//! Output mirrors the canonical `avft` table schema (`smallint`/`smallint`/
//! `smallint`/`smallint`/`double`) but widens every integer column to
//! [`DataType::Int64`] in line with `moves-default-db-convert`'s width
//! policy — see `crates/moves-default-db-convert/src/types.rs`. The
//! writer settings (uncompressed, no dictionary, no statistics,
//! `PARQUET_1_0`) mirror that crate too, so AVFT Parquet files are byte
//! deterministic and can be content-hashed alongside the converted
//! default DB.
//!
//! Row order follows [`AvftTable::iter`] (key-lexicographic), matching
//! `AVFTTool_OrderResults` in canonical MOVES.

use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Float64Builder, Int64Array, Int64Builder};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};

use crate::csv_io::COLUMNS;
use crate::error::{Error, Result};
use crate::model::AvftTable;

/// `created_by` string embedded in the Parquet file footer.
pub const PARQUET_CREATED_BY: &str = "moves-avft v0.1";

/// Build the Arrow schema for an AVFT Parquet file.
///
/// All four key columns widen `smallint` → `Int64` (matches
/// `moves-default-db-convert`). `fuelEngFraction` is `Float64`. Every
/// column is non-nullable: the AVFT table's primary-key invariant
/// rules out nulls in the key columns, and the user-facing semantic
/// for `fuelEngFraction` is a fraction in `[0, 1]`, not a missing
/// observation.
pub fn arrow_schema() -> SchemaRef {
    Arc::new(ArrowSchema::new(vec![
        Field::new(COLUMNS[0], DataType::Int64, false),
        Field::new(COLUMNS[1], DataType::Int64, false),
        Field::new(COLUMNS[2], DataType::Int64, false),
        Field::new(COLUMNS[3], DataType::Int64, false),
        Field::new(COLUMNS[4], DataType::Float64, false),
    ]))
}

/// Build a single Arrow [`RecordBatch`] holding every row of `table`.
pub fn build_record_batch(table: &AvftTable) -> Result<RecordBatch> {
    let mut st = Int64Builder::with_capacity(table.len());
    let mut my = Int64Builder::with_capacity(table.len());
    let mut fuel = Int64Builder::with_capacity(table.len());
    let mut eng = Int64Builder::with_capacity(table.len());
    let mut frac = Float64Builder::with_capacity(table.len());
    for r in table.iter() {
        st.append_value(r.source_type_id as i64);
        my.append_value(r.model_year_id as i64);
        fuel.append_value(r.fuel_type_id as i64);
        eng.append_value(r.eng_tech_id as i64);
        frac.append_value(r.fuel_eng_fraction);
    }
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(st.finish()),
        Arc::new(my.finish()),
        Arc::new(fuel.finish()),
        Arc::new(eng.finish()),
        Arc::new(frac.finish()),
    ];
    let batch = RecordBatch::try_new(arrow_schema(), arrays)?;
    Ok(batch)
}

/// Build the deterministic Parquet writer properties used for AVFT
/// output. Exposed so downstream callers (the snapshot/characterization
/// pipeline, integration tests) can configure their own writers
/// identically.
pub fn writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_created_by(PARQUET_CREATED_BY.to_string())
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .build()
}

/// Encode `table` as a Parquet byte buffer (in memory).
pub fn encode_parquet(table: &AvftTable) -> Result<Vec<u8>> {
    let batch = build_record_batch(table)?;
    let props = writer_properties();
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, arrow_schema(), Some(props))?;
        if batch.num_rows() > 0 {
            writer.write(&batch)?;
        }
        writer.close()?;
    }
    Ok(buf)
}

/// Write `table` to a Parquet file at `path`. The write is atomic: the
/// bytes go to `<path>.tmp` first, then rename. Parent directories are
/// created if needed.
pub fn write_parquet(table: &AvftTable, path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|source| Error::Io {
                path: parent.to_path_buf(),
                source,
            })?;
        }
    }
    let bytes = encode_parquet(table)?;
    let tmp = with_tmp_suffix(path);
    {
        let mut f = File::create(&tmp).map_err(|e| Error::io(&tmp, e))?;
        f.write_all(&bytes).map_err(|e| Error::io(&tmp, e))?;
        f.sync_all().map_err(|e| Error::io(&tmp, e))?;
    }
    std::fs::rename(&tmp, path).map_err(|e| Error::io(path, e))?;
    Ok(())
}

fn with_tmp_suffix(path: &Path) -> std::path::PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".tmp");
    s.into()
}

/// Read an [`AvftTable`] back from a Parquet file on disk.
///
/// Complements [`write_parquet`]. Columns are matched by name and
/// narrowed from the stored `Int64`/`Float64` types back to the
/// `i32`/`f64` values [`crate::model::AvftRecord`] carries.
///
/// # Errors
///
/// Returns [`crate::Error::Io`] if the file cannot be opened, or
/// [`crate::Error::Parquet`] / [`crate::Error::Arrow`] if the content
/// is not valid AVFT Parquet.
pub fn read_parquet(path: impl AsRef<Path>) -> crate::error::Result<AvftTable> {
    let path = path.as_ref();
    let file = File::open(path).map_err(|e| crate::error::Error::io(path, e))?;
    read_chunk_reader(file)
}

fn read_chunk_reader<R>(reader: R) -> crate::error::Result<AvftTable>
where
    R: parquet::file::reader::ChunkReader + 'static,
{
    let builder = ParquetRecordBatchReaderBuilder::try_new(reader)?;
    let record_reader = builder.build()?;

    let mut table = AvftTable::new();
    for batch_result in record_reader {
        let batch = batch_result?;
        let st = col_i64(&batch, "sourceTypeID")?;
        let my = col_i64(&batch, "modelYearID")?;
        let fuel = col_i64(&batch, "fuelTypeID")?;
        let eng = col_i64(&batch, "engTechID")?;
        let frac = col_f64(&batch, "fuelEngFraction")?;
        for i in 0..batch.num_rows() {
            table.insert(crate::model::AvftRecord::new(
                st.value(i) as i32,
                my.value(i) as i32,
                fuel.value(i) as i32,
                eng.value(i) as i32,
                frac.value(i),
            ));
        }
    }
    Ok(table)
}

fn col_i64<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> std::result::Result<&'a Int64Array, arrow::error::ArrowError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| {
            arrow::error::ArrowError::InvalidArgumentError(format!(
                "AVFT Parquet missing column '{name}'"
            ))
        })?
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            arrow::error::ArrowError::InvalidArgumentError(format!(
                "AVFT Parquet column '{name}' is not Int64"
            ))
        })
}

fn col_f64<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> std::result::Result<&'a Float64Array, arrow::error::ArrowError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| {
            arrow::error::ArrowError::InvalidArgumentError(format!(
                "AVFT Parquet missing column '{name}'"
            ))
        })?
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| {
            arrow::error::ArrowError::InvalidArgumentError(format!(
                "AVFT Parquet column '{name}' is not Float64"
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::AvftRecord;
    use parquet::file::reader::{FileReader, SerializedFileReader};

    fn sample_table() -> AvftTable {
        [
            AvftRecord::new(11, 2020, 1, 1, 0.6),
            AvftRecord::new(11, 2020, 2, 1, 0.4),
            AvftRecord::new(21, 2020, 1, 1, 1.0),
        ]
        .into_iter()
        .collect()
    }

    #[test]
    fn encode_parquet_has_correct_row_count() {
        let bytes = encode_parquet(&sample_table()).unwrap();
        let reader = SerializedFileReader::new(bytes::Bytes::from(bytes)).unwrap();
        assert_eq!(reader.metadata().file_metadata().num_rows(), 3);
    }

    #[test]
    fn encode_parquet_is_byte_stable() {
        let a = encode_parquet(&sample_table()).unwrap();
        let b = encode_parquet(&sample_table()).unwrap();
        assert_eq!(a, b, "AVFT Parquet writer must be byte-deterministic");
    }

    #[test]
    fn writer_emits_canonical_column_order() {
        let bytes = encode_parquet(&sample_table()).unwrap();
        let reader = SerializedFileReader::new(bytes::Bytes::from(bytes)).unwrap();
        let schema = reader.metadata().file_metadata().schema_descr();
        // `column(i)` hands back an owned `ColumnDescPtr`; hold the
        // descriptors in a `Vec` so the `&str` names borrowed from them
        // outlive the closure.
        let columns: Vec<_> = (0..schema.num_columns())
            .map(|i| schema.column(i))
            .collect();
        let names: Vec<&str> = columns.iter().map(|c| c.name()).collect();
        assert_eq!(names, COLUMNS.to_vec());
    }

    #[test]
    fn write_parquet_is_atomic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested/dir/out.parquet");
        write_parquet(&sample_table(), &path).unwrap();
        assert!(path.exists());
        // The .tmp sibling must be gone after rename.
        let mut tmp = path.clone();
        tmp.set_extension("parquet.tmp");
        assert!(!tmp.exists(), "tmp file should have been renamed");
    }

    #[test]
    fn empty_table_round_trips() {
        let bytes = encode_parquet(&AvftTable::new()).unwrap();
        let reader = SerializedFileReader::new(bytes::Bytes::from(bytes)).unwrap();
        assert_eq!(reader.metadata().file_metadata().num_rows(), 0);
    }

    #[test]
    fn read_parquet_round_trips_sample_table() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("avft.parquet");
        let original = sample_table();
        write_parquet(&original, &path).unwrap();
        let loaded = read_parquet(&path).unwrap();
        let orig_rows: Vec<_> = original.iter().collect();
        let loaded_rows: Vec<_> = loaded.iter().collect();
        assert_eq!(orig_rows, loaded_rows, "round-trip must reproduce identical rows");
    }

    #[test]
    fn read_parquet_round_trips_empty_table() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.parquet");
        write_parquet(&AvftTable::new(), &path).unwrap();
        let loaded = read_parquet(&path).unwrap();
        assert!(loaded.is_empty());
    }
}

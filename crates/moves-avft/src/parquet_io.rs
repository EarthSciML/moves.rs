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

use arrow::array::{ArrayRef, Float64Builder, Int64Builder};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
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
}

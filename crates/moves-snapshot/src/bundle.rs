//! Arrow-IPC execution-DB bundle: write all `db__movesexecution*` tables from
//! a snapshot into a single file for fast loading by `moves run --snapshot`.
//!
//! # Bundle layout
//!
//! ```text
//! [0..8] magic: b"MXDB\x00\x00\x00\x01"
//! [8..12] count: u32 LE (number of tables)
//! [12..] TOC: count × entry
//! name_len: u16 LE
//! name: [u8; name_len] (full snapshot table name, UTF-8)
//! offset: u64 LE (absolute byte offset from file start)
//! length: u64 LE
//! [..] Data: concatenated Arrow IPC file bytes (one per table)
//! ```
//!
//! Tables are stored in lexicographic name order (inherited from
//! `BTreeMap` iteration). Only tables with the `db__movesexecution` prefix
//! are included — output-DB and temporary tables are excluded.

use std::collections::BTreeMap;
use std::path::Path;

use arrow::array::RecordBatchReader;
use arrow::ipc::writer::FileWriter as ArrowIpcFileWriter;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::error::{Error, Result};
use crate::table::Table;

/// Magic bytes identifying a valid bundle file.
pub const BUNDLE_MAGIC: &[u8; 8] = b"MXDB\x00\x00\x00\x01";

/// Filename written inside `<snapshot-dir>/tables/`.
pub const BUNDLE_FILE: &str = "execution-db.bundle";

const EXECDB_PREFIX: &str = "db__movesexecution";

/// Write `tables/execution-db.bundle` inside `snapshot_dir`.
///
/// Only tables whose name starts with the execution-DB prefix are included.
/// If no such tables exist, the file is not created and the function returns
/// successfully.
pub fn write_execution_bundle(snapshot_dir: &Path, tables: &BTreeMap<String, Table>) -> Result<()> {
    let mut entries: Vec<(String, Vec<u8>)> = Vec::new();
    for (name, table) in tables {
        if !name.starts_with(EXECDB_PREFIX) {
            continue;
        }
        entries.push((name.clone(), encode_ipc(table)?));
    }
    if entries.is_empty() {
        return Ok(());
    }

    let buf = build_bundle_bytes(entries.iter().map(|(n, b)| (n.as_str(), b.as_slice())));
    let bundle_path = snapshot_dir.join("tables").join(BUNDLE_FILE);
    std::fs::write(&bundle_path, &buf).map_err(|source| Error::Io {
        path: bundle_path,
        source,
    })
}

/// Serialise a sequence of `(name, data)` pairs into bundle bytes.
///
/// Exposed for testing; callers outside this module should use
/// [`write_execution_bundle`] instead.
pub fn build_bundle_bytes<'a>(entries: impl IntoIterator<Item = (&'a str, &'a [u8])>) -> Vec<u8> {
    let entries: Vec<(&str, &[u8])> = entries.into_iter().collect();

 // header_size = magic (8) + count (4) = 12
 // toc_entry_size per entry = name_len (2) + name (n) + offset (8) + length (8)
    let toc_size: usize = entries.iter().map(|(name, _)| 2 + name.len() + 16).sum();
    let data_start: usize = 12 + toc_size;
    let total_data: usize = entries.iter().map(|(_, b)| b.len()).sum();

    let mut buf: Vec<u8> = Vec::with_capacity(data_start + total_data);

    buf.extend_from_slice(BUNDLE_MAGIC);
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());

    let mut data_offset: u64 = data_start as u64;
    for (name, data) in &entries {
        let name_bytes = name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(name_bytes);
        buf.extend_from_slice(&data_offset.to_le_bytes());
        buf.extend_from_slice(&(data.len() as u64).to_le_bytes());
        data_offset += data.len() as u64;
    }

    for (_, data) in &entries {
        buf.extend_from_slice(data);
    }

    buf
}

/// Write `tables/execution-db.bundle` from existing per-table Parquet files.
///
/// Reads every `db__movesexecution*.parquet` file from `<snapshot_dir>/tables/`,
/// converts each to Arrow IPC, and writes the bundle. Used to upgrade snapshots
/// written before the bundle format was introduced.
///
/// Returns the number of tables included in the bundle.
pub fn update_execution_bundle_from_parquets(snapshot_dir: &Path) -> Result<usize> {
    let tables_dir = snapshot_dir.join("tables");
    let mut dir_entries: Vec<_> = std::fs::read_dir(&tables_dir)
        .map_err(|source| Error::Io {
            path: tables_dir.clone(),
            source,
        })?
        .filter_map(|r| r.ok())
        .collect();
 // Sort so the bundle is in deterministic order.
    dir_entries.sort_by_key(|e| e.file_name());

    let mut entries: Vec<(String, Vec<u8>)> = Vec::new();
    for entry in dir_entries {
        let file_name = entry.file_name();
        let name_str = file_name.to_string_lossy();
        if !name_str.starts_with(EXECDB_PREFIX) || !name_str.ends_with(".parquet") {
            continue;
        }
 // Full snapshot table name: strip `.parquet`.
        let table_name = name_str.trim_end_matches(".parquet").to_string();
        let parquet_bytes = std::fs::read(entry.path()).map_err(|source| Error::Io {
            path: entry.path(),
            source,
        })?;
        let ipc_bytes = parquet_to_ipc(&parquet_bytes)?;
        entries.push((table_name, ipc_bytes));
    }

    let count = entries.len();
    if count == 0 {
        return Ok(0);
    }

    let buf = build_bundle_bytes(entries.iter().map(|(n, b)| (n.as_str(), b.as_slice())));
    let bundle_path = tables_dir.join(BUNDLE_FILE);
    std::fs::write(&bundle_path, &buf).map_err(|source| Error::Io {
        path: bundle_path,
        source,
    })?;
    Ok(count)
}

fn parquet_to_ipc(parquet_bytes: &[u8]) -> Result<Vec<u8>> {
    let reader =
        ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(parquet_bytes))?.build()?;
    let schema = reader.schema().clone();

    let mut buf: Vec<u8> = Vec::new();
    let mut writer = ArrowIpcFileWriter::try_new(&mut buf, &schema)?;
    for batch in reader {
        writer.write(&batch?)?;
    }
    writer.finish()?;
    Ok(buf)
}

fn encode_ipc(table: &Table) -> Result<Vec<u8>> {
    let (schema, batch) = table.to_record_batch()?;
    let mut buf: Vec<u8> = Vec::new();
    let mut writer = ArrowIpcFileWriter::try_new(&mut buf, &schema)?;
    writer.write(&batch)?;
    writer.finish()?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::ColumnKind;
    use crate::table::{TableBuilder, Value};

    fn make_table(name: &str) -> Table {
        let mut tb = TableBuilder::new(
            name,
            [
                ("id".to_string(), ColumnKind::Int64),
                ("val".to_string(), ColumnKind::Utf8),
            ],
        )
        .unwrap()
        .with_natural_key(["id"])
        .unwrap();
        tb.push_row([Value::Int64(1), Value::Utf8("a".into())])
            .unwrap();
        tb.push_row([Value::Int64(2), Value::Null]).unwrap();
        tb.build().unwrap()
    }

    #[test]
    fn build_bundle_bytes_round_trips_toc() {
        let data_a: Vec<u8> = vec![0xAA, 0xBB, 0xCC];
        let data_b: Vec<u8> = vec![0xDD];
        let buf = build_bundle_bytes([("alpha", data_a.as_slice()), ("beta", data_b.as_slice())]);

 // Magic.
        assert_eq!(&buf[0..8], BUNDLE_MAGIC);

 // Count.
        let count = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        assert_eq!(count, 2);

 // First TOC entry.
        let name_len_a = u16::from_le_bytes(buf[12..14].try_into().unwrap()) as usize;
        assert_eq!(name_len_a, 5); // "alpha"
        let name_a = std::str::from_utf8(&buf[14..14 + 5]).unwrap();
        assert_eq!(name_a, "alpha");
        let offset_a = u64::from_le_bytes(buf[19..27].try_into().unwrap());
        let length_a = u64::from_le_bytes(buf[27..35].try_into().unwrap());
        assert_eq!(length_a, 3);

 // Second TOC entry.
        let name_len_b = u16::from_le_bytes(buf[35..37].try_into().unwrap()) as usize;
        assert_eq!(name_len_b, 4); // "beta"
        let name_b = std::str::from_utf8(&buf[37..41]).unwrap();
        assert_eq!(name_b, "beta");
        let offset_b = u64::from_le_bytes(buf[41..49].try_into().unwrap());
        let length_b = u64::from_le_bytes(buf[49..57].try_into().unwrap());
        assert_eq!(length_b, 1);

 // Data section.
        assert_eq!(
            &buf[offset_a as usize..offset_a as usize + 3],
            &[0xAA, 0xBB, 0xCC]
        );
        assert_eq!(&buf[offset_b as usize..offset_b as usize + 1], &[0xDD]);
    }

    #[test]
    fn encode_ipc_round_trips_column_count() {
        let table = make_table("test");
        let ipc = encode_ipc(&table).unwrap();
        assert!(!ipc.is_empty());
 // IPC file starts with the Arrow magic.
        assert_eq!(&ipc[0..6], b"ARROW1");
    }

    #[test]
    fn write_execution_bundle_only_includes_execdb_tables() {
        use std::collections::BTreeMap;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        std::fs::create_dir(dir.path().join("tables")).unwrap();

        let mut tables = BTreeMap::new();
        tables.insert(
            "db__movesexecution1__activitytype".to_string(),
            make_table("db__movesexecution1__activitytype"),
        );
        tables.insert(
            "db__movesoutput__movesoutput".to_string(),
            make_table("db__movesoutput__movesoutput"),
        );

        write_execution_bundle(dir.path(), &tables).unwrap();

        let bundle_path = dir.path().join("tables").join(BUNDLE_FILE);
        assert!(bundle_path.exists());
        let buf = std::fs::read(&bundle_path).unwrap();
        let count = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        assert_eq!(count, 1, "only execution-DB tables should be bundled");
    }
}

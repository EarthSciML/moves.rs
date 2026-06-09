//! Read the Arrow-IPC execution-DB bundle written by `moves-snapshot`.
//!
//! See `moves-snapshot::bundle` for the format specification. This module
//! only implements the reader side; the writer lives in `moves-snapshot` so
//! the `moves-framework` crate does not need to depend on Polars at write
//! time.

use std::collections::BTreeSet;
use std::io::Cursor;
use std::path::Path;

use polars::prelude::{IpcReader, SerReader};

use crate::data::store::InMemoryStore;
use crate::data::DataFrameStore;
use crate::error::{Error, Result};

const MAGIC: &[u8; 8] = b"MXDB\x00\x00\x00\x01";

/// Read the execution-DB bundle at `path` and return a populated
/// [`InMemoryStore`].
///
/// Table names in the bundle are full snapshot names (e.g.
/// `db__movesexecution1ccc0232_campuscluster_illinois_edu__activitytype`).
/// Each table is stored in the returned store under its *short name*: the
/// last `__`-separated segment, lower-cased (e.g. `activitytype`). This
/// matches the key convention used by the per-file Parquet loader in
/// `moves-cli`.
pub fn read_execution_bundle(path: &Path) -> Result<InMemoryStore> {
    let bytes = std::fs::read(path).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    parse_bundle(&bytes, path, None)
}

/// Like [`read_execution_bundle`] but skips tables whose lowercased short name
/// is not in `allowed`.
///
/// Pass the set returned by
/// [`CalculatorRegistry::required_input_tables`](crate::calculator::CalculatorRegistry::required_input_tables)
/// to avoid materialising tables that no registered calculator consumes.
pub fn read_execution_bundle_filtered(
    path: &Path,
    allowed: &BTreeSet<String>,
) -> Result<InMemoryStore> {
    let bytes = std::fs::read(path).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    parse_bundle(&bytes, path, Some(allowed))
}

/// If `name` ends with exactly four ASCII decimal digits (a year suffix like `2020`),
/// return the prefix without them; otherwise return `name` unchanged.
fn strip_year_suffix(name: &str) -> &str {
    let bytes = name.as_bytes();
    if bytes.len() > 4 && bytes[bytes.len() - 4..].iter().all(|b| b.is_ascii_digit()) {
        &name[..name.len() - 4]
    } else {
        name
    }
}

/// Strip all trailing `_<digits>` segments, returning the canonical base name.
///
/// Matches the same helper in `moves-cli/src/run.rs`; duplicated here because
/// `moves-framework` cannot depend on `moves-cli`. See that copy for details.
fn strip_numeric_index_suffix(name: &str) -> &str {
    let mut end = name.len();
    while let Some(pos) = name[..end].rfind('_') {
        let suffix = &name[pos + 1..end];
        if !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()) {
            end = pos;
        } else {
            break;
        }
    }
    &name[..end]
}

fn parse_bundle(
    src: &[u8],
    path: &Path,
    allowed: Option<&BTreeSet<String>>,
) -> Result<InMemoryStore> {
    if src.len() < 12 {
        return Err(Error::InvalidBundle(format!(
            "{}: too short ({} bytes)",
            path.display(),
            src.len()
        )));
    }
    if &src[0..8] != MAGIC {
        return Err(Error::InvalidBundle(format!(
            "{}: unrecognised magic bytes",
            path.display()
        )));
    }
    let count = u32::from_le_bytes([src[8], src[9], src[10], src[11]]) as usize;

    // Parse TOC.
    let mut cursor = 12usize;
    let mut toc: Vec<(String, u64, u64)> = Vec::with_capacity(count);
    for i in 0..count {
        if cursor + 2 > src.len() {
            return Err(Error::InvalidBundle(format!(
                "{}: TOC entry {i} name_len field truncated",
                path.display()
            )));
        }
        let name_len = u16::from_le_bytes([src[cursor], src[cursor + 1]]) as usize;
        cursor += 2;
        if cursor + name_len + 16 > src.len() {
            return Err(Error::InvalidBundle(format!(
                "{}: TOC entry {i} truncated",
                path.display()
            )));
        }
        let name = std::str::from_utf8(&src[cursor..cursor + name_len]).map_err(|_| {
            Error::InvalidBundle(format!(
                "{}: TOC entry {i} has invalid UTF-8 table name",
                path.display()
            ))
        })?;
        let name = name.to_string();
        cursor += name_len;
        let offset = u64::from_le_bytes(src[cursor..cursor + 8].try_into().unwrap());
        let length = u64::from_le_bytes(src[cursor + 8..cursor + 16].try_into().unwrap());
        cursor += 16;
        toc.push((name, offset, length));
    }

    // Decode tables.
    let mut store = InMemoryStore::new();
    for (full_name, offset, length) in toc {
        let start = offset as usize;
        let end = start.checked_add(length as usize).ok_or_else(|| {
            Error::InvalidBundle(format!(
                "{}: overflow in data range for {full_name:?}",
                path.display()
            ))
        })?;
        if end > src.len() {
            return Err(Error::InvalidBundle(format!(
                "{}: data for {full_name:?} extends beyond bundle end",
                path.display()
            )));
        }
        let ipc_bytes = &src[start..end];
        let df = IpcReader::new(Cursor::new(ipc_bytes))
            .set_rechunk(true)
            .finish()
            .map_err(|e| Error::Polars(e.to_string()))?;

        let short_name = full_name
            .rsplit("__")
            .next()
            .unwrap_or(&full_name)
            .to_ascii_lowercase();
        if let Some(allowed) = allowed {
            // Year-suffixed tables (e.g. `stmyTVVCoeffs2020`) are admitted when
            // their base name (e.g. `stmytvvcoeffs`) appears in the allowed set.
            // Process/year-indexed tables (e.g. `baserate_1_2001`) are admitted when
            // their canonical base name (e.g. `baserate`) appears in the allowed set.
            if !allowed.contains(&short_name)
                && !allowed.contains(strip_year_suffix(&short_name))
                && !allowed.contains(strip_numeric_index_suffix(&short_name))
            {
                continue;
            }
        }
        store.insert(short_name, df);
    }
    Ok(store)
}

#[cfg(test)]
mod tests {
    use super::*;
    use moves_snapshot::bundle::build_bundle_bytes;
    use moves_snapshot::format::ColumnKind;
    use moves_snapshot::table::{TableBuilder, Value};
    use tempfile::tempdir;

    fn ipc_for_table(name: &str, rows: usize) -> Vec<u8> {
        let mut tb = TableBuilder::new(
            name,
            [
                ("id".to_string(), ColumnKind::Int64),
                ("label".to_string(), ColumnKind::Utf8),
            ],
        )
        .unwrap()
        .with_natural_key(["id"])
        .unwrap();
        for i in 0..rows as i64 {
            tb.push_row([Value::Int64(i), Value::Utf8(format!("row{i}"))])
                .unwrap();
        }
        let table = tb.build().unwrap();
        let (schema, batch) = table.to_record_batch().unwrap();
        let mut buf = Vec::new();
        let mut w = arrow::ipc::writer::FileWriter::try_new(&mut buf, &schema).unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
        buf
    }

    #[test]
    fn round_trip_single_table() {
        let ipc = ipc_for_table("db__movesexecution1__activitytype", 3);
        let bundle = build_bundle_bytes([("db__movesexecution1__activitytype", ipc.as_slice())]);

        let dir = tempdir().unwrap();
        let bundle_path = dir.path().join("exec.bundle");
        std::fs::write(&bundle_path, &bundle).unwrap();

        let store = read_execution_bundle(&bundle_path).unwrap();
        let df = store.get("activitytype").expect("short name lookup");
        assert_eq!(df.height(), 3);
        assert_eq!(df.width(), 2);
    }

    #[test]
    fn short_name_extraction() {
        let long = "db__movesexecution1ccc0232_campuscluster_illinois_edu__samplevehicletrip";
        let short: String = long
            .rsplit("__")
            .next()
            .unwrap_or(long)
            .to_ascii_lowercase();
        assert_eq!(short, "samplevehicletrip");
    }

    #[test]
    fn bad_magic_is_rejected() {
        let mut buf = vec![0u8; 20];
        buf[0..4].copy_from_slice(b"XXXX");
        let dir = tempdir().unwrap();
        let p = dir.path().join("bad.bundle");
        std::fs::write(&p, &buf).unwrap();
        let err = read_execution_bundle(&p).unwrap_err();
        assert!(matches!(err, Error::InvalidBundle(_)));
    }

    #[test]
    fn empty_bundle_returns_empty_store() {
        // Build a valid bundle with count=0.
        let buf = build_bundle_bytes(std::iter::empty::<(&str, &[u8])>());
        let dir = tempdir().unwrap();
        let p = dir.path().join("empty.bundle");
        std::fs::write(&p, &buf).unwrap();
        let store = read_execution_bundle(&p).unwrap();
        assert!(store.names().is_empty());
    }

    #[test]
    fn filtered_bundle_skips_tables_not_in_allowed_set() {
        // Bundle with two tables; allowed set contains only one.
        let ipc_a = ipc_for_table("db__movesexecution1__tablea", 2);
        let ipc_b = ipc_for_table("db__movesexecution1__tableb", 5);
        let bundle = build_bundle_bytes([
            ("db__movesexecution1__tablea", ipc_a.as_slice()),
            ("db__movesexecution1__tableb", ipc_b.as_slice()),
        ]);
        let dir = tempdir().unwrap();
        let p = dir.path().join("multi.bundle");
        std::fs::write(&p, &bundle).unwrap();

        let mut allowed = BTreeSet::new();
        allowed.insert("tablea".to_string());

        let store = read_execution_bundle_filtered(&p, &allowed).unwrap();
        assert!(store.get("tablea").is_some(), "tablea must be loaded");
        assert!(
            store.get("tableb").is_none(),
            "tableb must be skipped — not in allowed set"
        );
        assert_eq!(store.get("tablea").unwrap().height(), 2);
    }

    #[test]
    fn filtered_bundle_with_empty_allowed_loads_nothing() {
        let ipc = ipc_for_table("db__movesexecution1__activitytype", 1);
        let bundle = build_bundle_bytes([("db__movesexecution1__activitytype", ipc.as_slice())]);
        let dir = tempdir().unwrap();
        let p = dir.path().join("one.bundle");
        std::fs::write(&p, &bundle).unwrap();

        let store = read_execution_bundle_filtered(&p, &BTreeSet::new()).unwrap();
        assert!(
            store.names().is_empty(),
            "empty allowed set must produce empty store"
        );
    }

    // --- strip_year_suffix unit tests ---

    #[test]
    fn strip_year_suffix_strips_four_digits() {
        assert_eq!(strip_year_suffix("stmytvvcoeffs2020"), "stmytvvcoeffs");
        assert_eq!(
            strip_year_suffix("stmytvvequations2020"),
            "stmytvvequations"
        );
    }

    #[test]
    fn strip_year_suffix_no_change_without_digits() {
        assert_eq!(strip_year_suffix("activitytype"), "activitytype");
        assert_eq!(strip_year_suffix("stmytvvcoeffs"), "stmytvvcoeffs");
    }

    #[test]
    fn strip_year_suffix_no_change_partial_digits() {
        // Fewer than 4 trailing digits: no change.
        assert_eq!(strip_year_suffix("table202"), "table202");
        // 5 trailing digits: last 4 ("0200") are stripped, leaving "table2".
        assert_eq!(strip_year_suffix("table20200"), "table2");
    }

    #[test]
    fn strip_year_suffix_no_change_mixed_suffix() {
        assert_eq!(strip_year_suffix("table20a0"), "table20a0");
    }

    // --- strip_numeric_index_suffix unit tests ---

    #[test]
    fn strip_numeric_index_suffix_strips_process_year() {
        assert_eq!(strip_numeric_index_suffix("baserate_1_2001"), "baserate");
        assert_eq!(
            strip_numeric_index_suffix("baseratebyage_90_2020"),
            "baseratebyage"
        );
    }

    #[test]
    fn strip_numeric_index_suffix_strips_three_segments() {
        assert_eq!(
            strip_numeric_index_suffix("sourcebindistributionfuelusage_1_26161_2001"),
            "sourcebindistributionfuelusage"
        );
    }

    #[test]
    fn strip_numeric_index_suffix_no_change_plain_name() {
        assert_eq!(strip_numeric_index_suffix("baserate"), "baserate");
        assert_eq!(strip_numeric_index_suffix("activitytype"), "activitytype");
    }

    #[test]
    fn strip_numeric_index_suffix_no_change_year_only() {
        // A bare year suffix (no underscore separator) is NOT stripped — that's
        // `strip_year_suffix`'s job.
        assert_eq!(
            strip_numeric_index_suffix("stmytvvcoeffs2020"),
            "stmytvvcoeffs2020"
        );
    }

    // --- filtered bundle admits numeric-indexed tables ---

    #[test]
    fn filtered_bundle_admits_indexed_table_when_base_in_allowed() {
        // Simulates the baserate_1_2001 scenario: the bundle has a process/year-indexed
        // table, and the allowed set contains only the base (canonical) name.
        let ipc_indexed = ipc_for_table("db__movesexecution1__baserate_1_2001", 4);
        let ipc_other = ipc_for_table("db__movesexecution1__unrelated", 1);
        let bundle = build_bundle_bytes([
            (
                "db__movesexecution1__baserate_1_2001",
                ipc_indexed.as_slice(),
            ),
            ("db__movesexecution1__unrelated", ipc_other.as_slice()),
        ]);
        let dir = tempdir().unwrap();
        let p = dir.path().join("indexed.bundle");
        std::fs::write(&p, &bundle).unwrap();

        let mut allowed = BTreeSet::new();
        allowed.insert("baserate".to_string()); // canonical name — no index suffix

        let store = read_execution_bundle_filtered(&p, &allowed).unwrap();
        assert!(
            store.get("baserate_1_2001").is_some(),
            "indexed table must be admitted when base name is in allowed"
        );
        assert_eq!(store.get("baserate_1_2001").unwrap().height(), 4);
        assert!(
            store.get("unrelated").is_none(),
            "unrelated table must still be skipped"
        );
    }

    // --- year-suffixed table admission test ---

    #[test]
    fn filtered_bundle_admits_year_suffixed_table_when_base_in_allowed() {
        // Simulates the stmyTVVCoeffs2020 scenario: the bundle has a year-suffixed
        // table, and the allowed set contains only the base (unsuffixed) name.
        let ipc_year = ipc_for_table("db__movesexecution1__stmytvvcoeffs2020", 3);
        let ipc_other = ipc_for_table("db__movesexecution1__unrelated", 1);
        let bundle = build_bundle_bytes([
            (
                "db__movesexecution1__stmytvvcoeffs2020",
                ipc_year.as_slice(),
            ),
            ("db__movesexecution1__unrelated", ipc_other.as_slice()),
        ]);
        let dir = tempdir().unwrap();
        let p = dir.path().join("year.bundle");
        std::fs::write(&p, &bundle).unwrap();

        let mut allowed = BTreeSet::new();
        allowed.insert("stmytvvcoeffs".to_string()); // base name — no year suffix

        let store = read_execution_bundle_filtered(&p, &allowed).unwrap();
        assert!(
            store.get("stmytvvcoeffs2020").is_some(),
            "year-suffixed table must be admitted when base name is in allowed"
        );
        assert_eq!(store.get("stmytvvcoeffs2020").unwrap().height(), 3);
        assert!(
            store.get("unrelated").is_none(),
            "unrelated table must still be skipped"
        );
    }
}

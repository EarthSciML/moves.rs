//! MariaDB `-B -N` batch-format TSV reader.
//!
//! The dumper (`characterization/default-db-conversion/dump-default-db.sh`
//! and the apptainer-side `dump-databases.sh`) writes:
//!
//! * `<table>.tsv` — tab-separated rows, no header. NULL → literal `NULL`.
//!   Escape sequences `\0 \b \n \r \t \Z \\` are emitted by the mariadb
//!   client and decoded here.
//! * `<table>.schema.tsv` — three columns per row (`name`, `mysql_type`,
//!   `column_key`). `column_key` is the `INFORMATION_SCHEMA.COLUMNS.COLUMN_KEY`
//!   value (`PRI`, `MUL`, `UNI`, or empty).
//!
//! The reader streams the body line-by-line so per-partition writes can
//! buffer one partition at a time without materialising the full file.

use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::types::{mysql_to_arrow, normalize_mysql_type};
use arrow::datatypes::DataType;

/// One row from `<table>.schema.tsv`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaColumn {
    pub name: String,
    pub mysql_type: String,
    pub arrow_type: DataType,
    pub primary_key: bool,
}

/// Parse a `<table>.schema.tsv` file.
pub fn read_schema_tsv(path: &Path) -> Result<Vec<SchemaColumn>> {
    let bytes = std::fs::read(path).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    parse_schema_tsv(path, &bytes)
}

pub fn parse_schema_tsv(path: &Path, bytes: &[u8]) -> Result<Vec<SchemaColumn>> {
    let text = std::str::from_utf8(bytes).map_err(|source| Error::Parse {
        path: path.to_path_buf(),
        line: 0,
        message: format!("schema is not utf-8: {source}"),
    })?;
    let mut out = Vec::new();
    for (i, line) in text.lines().enumerate() {
        if line.is_empty() {
            continue;
        }
        let cols: Vec<&str> = line.split('\t').collect();
        if cols.len() < 2 {
            return Err(Error::Parse {
                path: path.to_path_buf(),
                line: i + 1,
                message: format!(
                    "expected `<name>\\t<type>[\\t<key>]`, got {} fields",
                    cols.len()
                ),
            });
        }
        let name = cols[0].to_string();
        let mysql_type = normalize_mysql_type(cols[1]);
        let arrow_type = mysql_to_arrow(&mysql_type);
        let column_key = cols.get(2).copied().unwrap_or("");
        out.push(SchemaColumn {
            name,
            mysql_type,
            arrow_type,
            primary_key: column_key.eq_ignore_ascii_case("PRI"),
        });
    }
    Ok(out)
}

/// Decode mariadb batch-mode escape encoding.
///
/// Recognised: `\0` → NUL, `\b` → BS, `\n` → LF, `\r` → CR, `\t` → TAB,
/// `\Z` → 0x1A, `\\` → backslash. Anything else is preserved as-is so a
/// truly literal backslash run is not silently lost.
pub fn decode_mariadb_field(field: &str) -> String {
    let mut out = String::with_capacity(field.len());
    let mut chars = field.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('0') => out.push('\0'),
            Some('b') => out.push('\u{0008}'),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('t') => out.push('\t'),
            Some('Z') => out.push('\u{001A}'),
            Some('\\') => out.push('\\'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => {
                out.push('\\');
            }
        }
    }
    out
}

/// Iterator over the decoded rows of a `<table>.tsv` file.
///
/// Each yielded row is `Vec<Option<String>>`; `None` represents SQL NULL
/// (the literal four-character string `NULL` in the file). Row width
/// mismatches against the expected schema are reported as
/// [`Error::RowWidthMismatch`].
pub struct TsvRows {
    path: PathBuf,
    text: String,
    expected_cols: usize,
    next_byte: usize,
}

impl TsvRows {
    pub fn read(path: &Path, expected_cols: usize) -> Result<Self> {
        let bytes = std::fs::read(path).map_err(|source| Error::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let text = String::from_utf8(bytes).map_err(|source| Error::Parse {
            path: path.to_path_buf(),
            line: 0,
            message: format!("body is not utf-8: {source}"),
        })?;
        Ok(Self {
            path: path.to_path_buf(),
            text,
            expected_cols,
            next_byte: 0,
        })
    }

    fn next_line(&mut self) -> Option<String> {
        if self.next_byte >= self.text.len() {
            return None;
        }
        let remainder = &self.text[self.next_byte..];
        let (line, advance) = match remainder.find('\n') {
            Some(i) => (&remainder[..i], i + 1),
            None => (remainder, remainder.len()),
        };
        self.next_byte += advance;
        Some(line.strip_suffix('\r').unwrap_or(line).to_string())
    }
}

impl Iterator for TsvRows {
    type Item = Result<Vec<Option<String>>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let line = self.next_line()?;
            if line.is_empty() {
                continue;
            }
            let fields: Vec<&str> = line.split('\t').collect();
            if fields.len() != self.expected_cols {
                return Some(Err(Error::RowWidthMismatch {
                    path: self.path.clone(),
                    expected: self.expected_cols,
                    actual: fields.len(),
                }));
            }
            let row: Vec<Option<String>> = fields
                .into_iter()
                .map(|raw| {
                    if raw == "NULL" {
                        None
                    } else {
                        Some(decode_mariadb_field(raw))
                    }
                })
                .collect();
            return Some(Ok(row));
        }
    }
}

/// Count the data rows in a TSV file without parsing them. Used by the
/// pipeline to cross-check Parquet row counts against the source dump
/// without paying the full parse cost twice.
pub fn count_rows(path: &Path) -> Result<u64> {
    let bytes = std::fs::read(path).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let mut count: u64 = 0;
    let mut at_line_start = true;
    for &b in bytes.iter() {
        if at_line_start && b != b'\n' && b != b'\r' {
            count += 1;
            at_line_start = false;
        }
        if b == b'\n' {
            at_line_start = true;
        }
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    fn write_tmp(name: &str, body: &[u8]) -> PathBuf {
        let dir = tempdir().unwrap();
        let path = dir.path().join(name);
        std::fs::create_dir_all(dir.path()).unwrap();
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(body).unwrap();
        // Leak the tempdir to keep the file alive; tests rely on the path
        // surviving past the helper. Inside #[cfg(test)] this is fine.
        std::mem::forget(dir);
        path
    }

    #[test]
    fn decode_escapes() {
        assert_eq!(decode_mariadb_field("plain"), "plain");
        assert_eq!(decode_mariadb_field("a\\tb"), "a\tb");
        assert_eq!(decode_mariadb_field("a\\nb"), "a\nb");
        assert_eq!(decode_mariadb_field("a\\\\b"), "a\\b");
        assert_eq!(decode_mariadb_field("a\\xb"), "a\\xb");
        assert_eq!(decode_mariadb_field("trail\\"), "trail\\");
    }

    #[test]
    fn parse_schema_records_pk_and_types() {
        let body = b"id\tint(11)\tPRI\nname\tvarchar(64)\t\nval\tdouble\t\n";
        let cols = parse_schema_tsv(Path::new("t.schema.tsv"), body).unwrap();
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].mysql_type, "int");
        assert_eq!(cols[0].arrow_type, DataType::Int64);
        assert!(cols[0].primary_key);
        assert_eq!(cols[1].mysql_type, "varchar");
        assert_eq!(cols[1].arrow_type, DataType::Utf8);
        assert!(!cols[1].primary_key);
        assert_eq!(cols[2].arrow_type, DataType::Float64);
    }

    #[test]
    fn parse_schema_rejects_underdocumented_lines() {
        let body = b"id\nname\tvarchar\n";
        let err = parse_schema_tsv(Path::new("t.schema.tsv"), body).unwrap_err();
        assert!(matches!(err, Error::Parse { .. }));
    }

    #[test]
    fn tsv_rows_streams_with_nulls_and_escapes() {
        let path = write_tmp("t.tsv", b"1\thello\t1.5\n2\tNULL\t2.5\n3\ta\\tb\tNULL\n");
        let rows: Vec<_> = TsvRows::read(&path, 3).unwrap().collect();
        assert_eq!(rows.len(), 3);
        let r0 = rows[0].as_ref().unwrap();
        assert_eq!(r0[0].as_deref(), Some("1"));
        assert_eq!(r0[1].as_deref(), Some("hello"));
        let r1 = rows[1].as_ref().unwrap();
        assert_eq!(r1[1], None);
        let r2 = rows[2].as_ref().unwrap();
        assert_eq!(r2[1].as_deref(), Some("a\tb"));
        assert_eq!(r2[2], None);
    }

    #[test]
    fn tsv_rows_rejects_width_mismatch() {
        let path = write_tmp("t.tsv", b"1\thello\n");
        let mut iter = TsvRows::read(&path, 3).unwrap();
        let err = iter.next().unwrap().unwrap_err();
        assert!(matches!(
            err,
            Error::RowWidthMismatch {
                expected: 3,
                actual: 2,
                ..
            }
        ));
    }

    #[test]
    fn tsv_rows_skips_blank_lines() {
        let path = write_tmp("t.tsv", b"1\ta\n\n\n2\tb\n");
        let rows: Vec<_> = TsvRows::read(&path, 2).unwrap().collect();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn count_rows_matches_iteration() {
        let path = write_tmp("t.tsv", b"a\nb\nc\n");
        assert_eq!(count_rows(&path).unwrap(), 3);
        // No trailing newline
        let path2 = write_tmp("t2.tsv", b"a\nb\nc");
        assert_eq!(count_rows(&path2).unwrap(), 3);
        // Empty file
        let path3 = write_tmp("empty.tsv", b"");
        assert_eq!(count_rows(&path3).unwrap(), 0);
        // Blank lines aren't rows
        let path4 = write_tmp("blanks.tsv", b"a\n\nb\n");
        assert_eq!(count_rows(&path4).unwrap(), 2);
    }
}

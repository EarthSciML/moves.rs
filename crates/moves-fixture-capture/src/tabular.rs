//! Tab-separated value parsers for MOVES fixture captures.
//!
//! Two formats are handled:
//!
//! 1. **MariaDB-dumped tables** — written by `mariadb -B -N -e "SELECT ..."`.
//!    Each row is tab-separated; embedded tabs/newlines are escaped via
//!    `\t`, `\n`, etc. NULL renders as the literal four-character string
//!    `NULL`. A sidecar `<table>.schema.tsv` carries column types.
//!
//! 2. **Worker bundle `.tbl` files** — written by MOVES Java/Go worker code.
//!    Tab-separated, no schema sidecar, no escape encoding. We treat every
//!    column as `Utf8` (literal-byte preservation); type-aware normalization
//!    applies only to (1).
//!
//! ## Determinism
//!
//! The MariaDB dumper is expected to issue `SELECT ... ORDER BY <every column>`
//! so the on-disk row order is deterministic, then the snapshot crate
//! re-sorts by natural key during `TableBuilder::build` — so the final
//! snapshot is invariant under either source ordering.
//!
//! NULL ambiguity: a `varchar` column holding the literal string `"NULL"` is
//! indistinguishable from SQL NULL in this format. MOVES output tables do not
//! contain the literal `"NULL"` as a varchar value, so this is documented as
//! a known caveat rather than worked around.

use std::path::{Path, PathBuf};

use moves_snapshot::{ColumnKind, Table, TableBuilder, Value};

use crate::error::{Error, Result};

/// Schema sidecar — one entry per column, in declared order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnHint {
    pub name: String,
    pub kind: ColumnKind,
    /// Whether this column is part of the table's PRIMARY KEY (any position).
    pub primary_key: bool,
}

/// Parse a `<table>.schema.tsv` sidecar.
///
/// Each line: `<name>\t<mysql_type>\t<column_key>` (column_key is the
/// `INFORMATION_SCHEMA.COLUMNS.COLUMN_KEY` value, e.g. `PRI`, `MUL`, `UNI`,
/// or empty).
pub fn parse_schema_tsv(path: &Path, bytes: &[u8]) -> Result<Vec<ColumnHint>> {
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
        let mysql_type = cols[1];
        let column_key = cols.get(2).copied().unwrap_or("");
        let kind = mysql_type_to_kind(mysql_type);
        out.push(ColumnHint {
            name,
            kind,
            primary_key: column_key.eq_ignore_ascii_case("PRI"),
        });
    }
    Ok(out)
}

/// Map a MariaDB / MySQL `INFORMATION_SCHEMA.COLUMNS.DATA_TYPE` value to a
/// snapshot column kind.
///
/// The mapping intentionally widens fixed-width integer types (`tinyint`,
/// `smallint`, `int`) to `Int64` and `decimal`/`float`/`double` to `Float64`.
/// `tinyint(1)` would naturally map to Boolean, but `INFORMATION_SCHEMA`
/// reports the type as `tinyint` regardless of display width — without the
/// width annotation we cannot distinguish, so tinyint is widened to Int64
/// for safety. MOVES tables don't have boolean columns of clinical interest.
pub fn mysql_type_to_kind(mysql_type: &str) -> ColumnKind {
    let t = mysql_type.trim().to_ascii_lowercase();
    match t.as_str() {
        // Integer family.
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" => ColumnKind::Int64,
        "year" => ColumnKind::Int64,
        // Float family — anything that prints with a decimal point.
        "decimal" | "numeric" | "float" | "double" | "real" => ColumnKind::Float64,
        // Boolean (rare in MOVES tables, but supported defensively).
        "bool" | "boolean" => ColumnKind::Boolean,
        // Everything else — strings, blobs, dates, enums — captured as utf8.
        _ => ColumnKind::Utf8,
    }
}

/// Decode mariadb `-B` (batch) escape encoding.
///
/// Recognized escapes:
/// `\0` → NUL, `\b` → backspace, `\n` → LF, `\r` → CR, `\t` → TAB,
/// `\Z` → 0x1A (CTRL-Z), `\\` → backslash.
/// Any other backslash sequence is preserved literally (defensive — the
/// MariaDB client emits only the sequences above).
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

/// Parse a MariaDB-batch-format dump into a [`Table`], using the schema hints
/// to drive type interpretation.
///
/// `table_name` becomes the table's identity in the snapshot (so callers
/// should namespace it, e.g. `output_db__movesactivityoutput`).
///
/// Rows whose column count does not match the schema are an error.
pub fn parse_mariadb_table(
    path: &Path,
    table_name: &str,
    schema: &[ColumnHint],
    body: &[u8],
) -> Result<Table> {
    let text = std::str::from_utf8(body).map_err(|source| Error::Parse {
        path: path.to_path_buf(),
        line: 0,
        message: format!("table body is not utf-8: {source}"),
    })?;

    let schema_pairs: Vec<(String, ColumnKind)> =
        schema.iter().map(|c| (c.name.clone(), c.kind)).collect();
    let mut tb = TableBuilder::new(table_name.to_string(), schema_pairs)?;

    let natural_key: Vec<String> = schema
        .iter()
        .filter(|c| c.primary_key)
        .map(|c| c.name.clone())
        .collect();
    if !natural_key.is_empty() {
        tb = tb.with_natural_key(natural_key)?;
    }

    for (i, line) in text.split_inclusive('\n').enumerate() {
        // Strip the trailing \n that split_inclusive preserves so the final
        // line (which may not have a trailing \n) is processed identically.
        let line = line.strip_suffix('\n').unwrap_or(line);
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.is_empty() {
            continue;
        }

        let fields: Vec<&str> = line.split('\t').collect();
        if fields.len() != schema.len() {
            return Err(Error::RowWidthMismatch {
                path: path.to_path_buf(),
                expected: schema.len(),
                actual: fields.len(),
            });
        }

        let mut row: Vec<Value> = Vec::with_capacity(schema.len());
        for (col, raw) in schema.iter().zip(fields.iter()) {
            row.push(field_to_value(path, i + 1, col, raw)?);
        }
        tb.push_row(row)?;
    }

    Ok(tb.build()?)
}

fn field_to_value(path: &Path, line: usize, col: &ColumnHint, raw: &str) -> Result<Value> {
    if raw == "NULL" {
        return Ok(Value::Null);
    }
    let decoded = decode_mariadb_field(raw);
    match col.kind {
        ColumnKind::Int64 => decoded
            .parse::<i64>()
            .map(Value::Int64)
            .map_err(|e| Error::Parse {
                path: path.to_path_buf(),
                line,
                message: format!(
                    "column '{}' expected int64, got '{}': {}",
                    col.name, decoded, e
                ),
            }),
        ColumnKind::Float64 => {
            decoded
                .parse::<f64>()
                .map(Value::Float64)
                .map_err(|e| Error::Parse {
                    path: path.to_path_buf(),
                    line,
                    message: format!(
                        "column '{}' expected float64, got '{}': {}",
                        col.name, decoded, e
                    ),
                })
        }
        ColumnKind::Boolean => match decoded.as_str() {
            "0" => Ok(Value::Boolean(false)),
            "1" => Ok(Value::Boolean(true)),
            "true" => Ok(Value::Boolean(true)),
            "false" => Ok(Value::Boolean(false)),
            other => Err(Error::Parse {
                path: path.to_path_buf(),
                line,
                message: format!("column '{}' expected boolean, got '{}'", col.name, other),
            }),
        },
        ColumnKind::Utf8 => Ok(Value::Utf8(decoded)),
    }
}

/// Parse a worker `.tbl` / `.csv` file.
///
/// Heuristic: if the first line contains only ASCII identifier characters
/// (letters, digits, underscores) separated by tabs, it's treated as a
/// header line and column names come from it. Otherwise the columns are
/// named `col_0`, `col_1`, etc.
///
/// All columns are typed `Utf8`. No escape decoding is performed — bytes are
/// preserved verbatim so a regression in the worker output's text format is
/// detected as a content change rather than silently absorbed.
///
/// Empty files produce an empty table with a single `col_0` Utf8 column —
/// MOVES sometimes writes a stub `.tbl` to flag "this stage ran but produced
/// no rows," and we want that to surface as an entry with row_count=0.
pub fn parse_worker_tbl(path: &Path, table_name: &str, body: &[u8]) -> Result<Table> {
    let text = std::str::from_utf8(body).map_err(|source| Error::Parse {
        path: path.to_path_buf(),
        line: 0,
        message: format!("file is not utf-8: {source}"),
    })?;

    let mut lines = text.split_inclusive('\n').peekable();
    let raw_first = lines.peek().copied();

    let (header, mut data_lines): (Vec<String>, Vec<&str>) = match raw_first {
        Some(first) => {
            let first_clean = first
                .strip_suffix('\n')
                .unwrap_or(first)
                .strip_suffix('\r')
                .unwrap_or_else(|| first.strip_suffix('\n').unwrap_or(first));
            if first_clean.is_empty() {
                // Empty file — single stub column, no rows.
                (vec!["col_0".to_string()], Vec::new())
            } else if looks_like_header(first_clean) {
                let header = first_clean
                    .split('\t')
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>();
                lines.next();
                let data: Vec<&str> = lines.collect();
                (header, data)
            } else {
                // Generate names from the first row's column count.
                let n = first_clean.split('\t').count();
                let header: Vec<String> = (0..n).map(|i| format!("col_{i}")).collect();
                let data: Vec<&str> = lines.collect();
                (header, data)
            }
        }
        None => (vec!["col_0".to_string()], Vec::new()),
    };

    // Defensive: if any data line has more columns than the header, widen
    // the header so we don't lose data. This shouldn't happen with
    // well-formed worker output but surfaces malformed input clearly.
    let mut max_cols = header.len();
    for line in &data_lines {
        let line = line
            .strip_suffix('\n')
            .unwrap_or(line)
            .strip_suffix('\r')
            .unwrap_or_else(|| line.strip_suffix('\n').unwrap_or(line));
        if line.is_empty() {
            continue;
        }
        max_cols = max_cols.max(line.split('\t').count());
    }
    let mut header = header;
    while header.len() < max_cols {
        header.push(format!("col_{}", header.len()));
    }

    let schema: Vec<(String, ColumnKind)> = header
        .iter()
        .map(|n| (n.clone(), ColumnKind::Utf8))
        .collect();
    let mut tb = TableBuilder::new(table_name.to_string(), schema)?;

    // Drop trailing empty lines so a final \n doesn't appear as a row.
    while let Some(last) = data_lines.last() {
        let trimmed = last.strip_suffix('\n').unwrap_or(last);
        let trimmed = trimmed.strip_suffix('\r').unwrap_or(trimmed);
        if trimmed.is_empty() {
            data_lines.pop();
        } else {
            break;
        }
    }

    for line in data_lines {
        let line = line.strip_suffix('\n').unwrap_or(line);
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split('\t').collect();
        let mut row: Vec<Value> = Vec::with_capacity(header.len());
        for i in 0..header.len() {
            match fields.get(i) {
                Some(s) => row.push(Value::Utf8((*s).to_string())),
                None => row.push(Value::Null),
            }
        }
        tb.push_row(row)?;
    }

    Ok(tb.build()?)
}

fn looks_like_header(line: &str) -> bool {
    if line.is_empty() {
        return false;
    }
    line.split('\t').all(|cell| {
        if cell.is_empty() {
            return false;
        }
        let mut has_letter = false;
        for c in cell.chars() {
            if c.is_ascii_alphabetic() {
                has_letter = true;
            } else if !(c.is_ascii_digit() || c == '_' || c == '-') {
                return false;
            }
        }
        has_letter
    })
}

/// Sanitize a path-derived component into a snapshot table-name segment.
/// Lowercases ASCII; replaces non-`[a-z0-9_-]` characters with `_`.
pub fn sanitize_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    out
}

/// Build a snapshot-friendly table name from a hierarchy of path segments.
/// Empty segments are ignored, and segments are joined with `__`.
pub fn join_table_name<I, S>(segments: I) -> String
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut parts: Vec<String> = Vec::new();
    for seg in segments {
        let s = sanitize_segment(seg.as_ref());
        if !s.is_empty() {
            parts.push(s);
        }
    }
    parts.join("__")
}

/// Helper to read a file and produce an [`Error::Io`] on failure with the
/// exact path attached.
pub(crate) fn read_file(path: &Path) -> Result<Vec<u8>> {
    std::fs::read(path).map_err(|source| Error::Io {
        path: PathBuf::from(path),
        source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use moves_snapshot::NormalizedColumn;

    #[test]
    fn type_mapping() {
        assert_eq!(mysql_type_to_kind("int"), ColumnKind::Int64);
        assert_eq!(mysql_type_to_kind("BIGINT"), ColumnKind::Int64);
        assert_eq!(mysql_type_to_kind(" smallint "), ColumnKind::Int64);
        assert_eq!(mysql_type_to_kind("decimal"), ColumnKind::Float64);
        assert_eq!(mysql_type_to_kind("DOUBLE"), ColumnKind::Float64);
        assert_eq!(mysql_type_to_kind("float"), ColumnKind::Float64);
        assert_eq!(mysql_type_to_kind("varchar"), ColumnKind::Utf8);
        assert_eq!(mysql_type_to_kind("text"), ColumnKind::Utf8);
        assert_eq!(mysql_type_to_kind("date"), ColumnKind::Utf8);
        assert_eq!(mysql_type_to_kind("blob"), ColumnKind::Utf8);
    }

    #[test]
    fn decode_known_escapes() {
        assert_eq!(decode_mariadb_field(""), "");
        assert_eq!(decode_mariadb_field("hello"), "hello");
        assert_eq!(decode_mariadb_field("a\\tb"), "a\tb");
        assert_eq!(decode_mariadb_field("a\\nb"), "a\nb");
        assert_eq!(decode_mariadb_field("a\\\\b"), "a\\b");
        assert_eq!(decode_mariadb_field("a\\0b"), "a\0b");
        assert_eq!(decode_mariadb_field("a\\Zb"), "a\u{001A}b");
        // Unknown escape → preserved
        assert_eq!(decode_mariadb_field("a\\xb"), "a\\xb");
        // Trailing backslash → preserved
        assert_eq!(decode_mariadb_field("a\\"), "a\\");
    }

    #[test]
    fn schema_tsv_roundtrip() {
        let body = b"id\tint\tPRI\nname\tvarchar\t\nval\tdouble\t\n";
        let hints = parse_schema_tsv(Path::new("t.schema.tsv"), body).unwrap();
        assert_eq!(hints.len(), 3);
        assert_eq!(hints[0].name, "id");
        assert_eq!(hints[0].kind, ColumnKind::Int64);
        assert!(hints[0].primary_key);
        assert_eq!(hints[1].kind, ColumnKind::Utf8);
        assert!(!hints[1].primary_key);
        assert_eq!(hints[2].kind, ColumnKind::Float64);
    }

    #[test]
    fn schema_tsv_rejects_too_few_fields() {
        let body = b"id\nname\tvarchar\n";
        let err = parse_schema_tsv(Path::new("t.schema.tsv"), body).unwrap_err();
        assert!(matches!(err, Error::Parse { .. }));
    }

    #[test]
    fn parse_mariadb_table_basic() {
        let schema = vec![
            ColumnHint {
                name: "id".into(),
                kind: ColumnKind::Int64,
                primary_key: true,
            },
            ColumnHint {
                name: "value".into(),
                kind: ColumnKind::Float64,
                primary_key: false,
            },
            ColumnHint {
                name: "label".into(),
                kind: ColumnKind::Utf8,
                primary_key: false,
            },
        ];
        let body = b"3\t3.0\tthree\n1\t1.0\tone\n2\tNULL\ttwo\n";
        let table = parse_mariadb_table(Path::new("t.tsv"), "t", &schema, body).unwrap();
        // Sorted by `id` (the natural key from PRI).
        assert_eq!(table.row_count(), 3);
        let NormalizedColumn::Int64(ids) = &table.columns()[0] else {
            panic!()
        };
        assert_eq!(ids, &[Some(1), Some(2), Some(3)]);
        let NormalizedColumn::Float64String(vs) = &table.columns()[1] else {
            panic!()
        };
        assert_eq!(vs[0].as_deref(), Some("1.000000000000"));
        assert_eq!(vs[1].as_deref(), None); // NULL
        assert_eq!(vs[2].as_deref(), Some("3.000000000000"));
    }

    #[test]
    fn parse_mariadb_table_rejects_width_mismatch() {
        let schema = vec![ColumnHint {
            name: "a".into(),
            kind: ColumnKind::Utf8,
            primary_key: false,
        }];
        let body = b"x\ty\n";
        let err = parse_mariadb_table(Path::new("t.tsv"), "t", &schema, body).unwrap_err();
        assert!(matches!(
            err,
            Error::RowWidthMismatch {
                expected: 1,
                actual: 2,
                ..
            }
        ));
    }

    #[test]
    fn parse_mariadb_table_decodes_escapes() {
        let schema = vec![ColumnHint {
            name: "s".into(),
            kind: ColumnKind::Utf8,
            primary_key: false,
        }];
        let body = b"a\\tb\nc\\nd\n";
        let table = parse_mariadb_table(Path::new("t.tsv"), "t", &schema, body).unwrap();
        assert_eq!(table.row_count(), 2);
        let NormalizedColumn::Utf8(vs) = &table.columns()[0] else {
            panic!()
        };
        // Sorted lexicographically (no natural key on this schema).
        assert_eq!(vs[0].as_deref(), Some("a\tb"));
        assert_eq!(vs[1].as_deref(), Some("c\nd"));
    }

    #[test]
    fn parse_worker_tbl_with_header() {
        let body = b"colA\tcolB\n1\t2\n3\t4\n";
        let table = parse_worker_tbl(Path::new("t.tbl"), "t", body).unwrap();
        assert_eq!(table.schema().len(), 2);
        assert_eq!(table.schema()[0].name, "colA");
        assert_eq!(table.row_count(), 2);
    }

    #[test]
    fn parse_worker_tbl_without_header() {
        // First row contains a numeric/punctuation cell — not header-like.
        let body = b"1.5\thello world\n2.5\tfoo\n";
        let table = parse_worker_tbl(Path::new("t.tbl"), "t", body).unwrap();
        assert_eq!(table.schema().len(), 2);
        assert_eq!(table.schema()[0].name, "col_0");
        assert_eq!(table.schema()[1].name, "col_1");
        assert_eq!(table.row_count(), 2);
    }

    #[test]
    fn parse_worker_tbl_empty() {
        let table = parse_worker_tbl(Path::new("t.tbl"), "t", b"").unwrap();
        assert_eq!(table.schema().len(), 1);
        assert_eq!(table.row_count(), 0);
    }

    #[test]
    fn parse_worker_tbl_widens_short_rows() {
        // Header has 2 cols, second row has 3.
        let body = b"a\tb\n1\t2\n3\t4\t5\n";
        let table = parse_worker_tbl(Path::new("t.tbl"), "t", body).unwrap();
        assert_eq!(table.schema().len(), 3);
        assert_eq!(table.schema()[2].name, "col_2");
    }

    #[test]
    fn join_table_name_sanitizes() {
        assert_eq!(join_table_name(["MOVESOutput"]), "movesoutput");
        assert_eq!(
            join_table_name(["WorkerFolder", "WorkerTemp00", "Output.tbl"]),
            "workerfolder__workertemp00__output_tbl"
        );
        assert_eq!(join_table_name(["a", "", "b"]), "a__b");
    }

    #[test]
    fn looks_like_header_examples() {
        assert!(looks_like_header("colA\tcolB"));
        assert!(looks_like_header("a_b\tc-d\te123"));
        assert!(!looks_like_header(""));
        assert!(!looks_like_header("1\t2"));
        assert!(!looks_like_header("a\t1.5"));
        assert!(!looks_like_header("hello world\tfoo"));
    }
}

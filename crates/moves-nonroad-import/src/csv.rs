//! Minimal CSV reader for the Nonroad input templates.
//!
//! The user-facing input format is CSV with a header row whose column names
//! match the table schema (mixed case, no leading/trailing whitespace). NULL
//! is encoded as the empty cell — there is no `\N`-style sentinel. We
//! intentionally do not pull in the `csv` crate: the templates are tabular
//! ASCII with no embedded newlines or quoted commas that we know of, and a
//! hand-written parser keeps the dependency surface and the diagnostic
//! quality under our control.
//!
//! Supported syntax (RFC 4180 subset):
//!
//! * Fields separated by `,` on a single line.
//! * Optional UTF-8 BOM at the start of file.
//! * Optional Windows-style `\r\n` line endings.
//! * Double-quoted fields: outer `"`s stripped, internal `""` collapsed
//!   to one `"`. Cells with embedded commas or quotes require quoting.
//! * Empty cells → `None`. Whitespace-only cells with surrounding quotes
//!   are preserved; bare whitespace cells are also passed through (the
//!   schema layer rejects them where typing requires non-empty).

use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

/// Decoded contents of a CSV file: the header row plus all body rows.
///
/// Body rows are `Vec<Option<String>>` where `None` is "the cell was
/// empty" — i.e. the user-facing equivalent of MariaDB NULL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsvFile {
    pub path: PathBuf,
    pub header: Vec<String>,
    pub rows: Vec<CsvRow>,
}

/// One body row plus its 1-based source line number (for error messages).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsvRow {
    pub line: usize,
    pub cells: Vec<Option<String>>,
}

/// Read and parse a CSV file from disk. Errors carry the path so callers
/// don't need to thread it through themselves.
pub fn read_csv(path: &Path) -> Result<CsvFile> {
    let bytes = std::fs::read(path).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    parse_csv(path, &bytes)
}

/// Parse CSV bytes against the given source path.
pub fn parse_csv(path: &Path, bytes: &[u8]) -> Result<CsvFile> {
    let text = std::str::from_utf8(bytes).map_err(|source| Error::Parse {
        path: path.to_path_buf(),
        line: 0,
        message: format!("not valid UTF-8: {source}"),
    })?;
    let text = text.strip_prefix('\u{FEFF}').unwrap_or(text);

    let mut lines = text.split('\n').enumerate();
    let (_, raw_header) = match lines.next() {
        Some(pair) => pair,
        None => {
            return Err(Error::Parse {
                path: path.to_path_buf(),
                line: 0,
                message: "file is empty (no header row)".to_string(),
            });
        }
    };
    let header_line = strip_cr(raw_header);
    if header_line.is_empty() {
        return Err(Error::Parse {
            path: path.to_path_buf(),
            line: 1,
            message: "header row is empty".to_string(),
        });
    }
    let header = split_line(path, 1, header_line)?
        .into_iter()
        .map(|c| c.unwrap_or_default())
        .collect::<Vec<_>>();

    let mut rows = Vec::new();
    for (idx, raw) in lines {
        let line_no = idx + 1;
        let line = strip_cr(raw);
        if line.is_empty() {
            continue;
        }
        let cells = split_line(path, line_no, line)?;
        rows.push(CsvRow {
            line: line_no,
            cells,
        });
    }

    Ok(CsvFile {
        path: path.to_path_buf(),
        header,
        rows,
    })
}

fn strip_cr(s: &str) -> &str {
    s.strip_suffix('\r').unwrap_or(s)
}

/// Split a single CSV line into cells. Returns `None` for cells the user
/// left empty (zero characters between surrounding commas). Quoted cells
/// strip the outer `"`s and collapse `""` to a single `"`.
fn split_line(path: &Path, line_no: usize, line: &str) -> Result<Vec<Option<String>>> {
    let mut out = Vec::new();
    let bytes = line.as_bytes();
    let n = bytes.len();
    let mut i = 0;
    loop {
        if i >= n {
            // Either the line is empty or we just consumed a trailing ','.
            // Either way, the implicit final cell is empty.
            out.push(None);
            return Ok(out);
        }
        let cell = if bytes[i] == b'"' {
            i += 1;
            let mut buf = String::new();
            loop {
                if i >= n {
                    return Err(Error::Parse {
                        path: path.to_path_buf(),
                        line: line_no,
                        message: "unterminated quoted field".to_string(),
                    });
                }
                let b = bytes[i];
                if b == b'"' {
                    if i + 1 < n && bytes[i + 1] == b'"' {
                        buf.push('"');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                buf.push(b as char);
                i += 1;
            }
            if i < n && bytes[i] != b',' {
                return Err(Error::Parse {
                    path: path.to_path_buf(),
                    line: line_no,
                    message: format!(
                        "unexpected character {:?} after closing quote (cell {})",
                        bytes[i] as char,
                        out.len() + 1
                    ),
                });
            }
            if buf.is_empty() {
                None
            } else {
                Some(buf)
            }
        } else {
            let start = i;
            while i < n && bytes[i] != b',' {
                i += 1;
            }
            let raw = &line[start..i];
            if raw.is_empty() {
                None
            } else {
                Some(raw.to_string())
            }
        };
        out.push(cell);
        if i >= n {
            return Ok(out);
        }
        // Consume the field separator.
        debug_assert_eq!(bytes[i], b',');
        i += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn p() -> PathBuf {
        PathBuf::from("test.csv")
    }

    #[test]
    fn parses_simple_header_and_rows() {
        let body = b"a,b,c\n1,2,3\n4,5,6\n";
        let csv = parse_csv(&p(), body).unwrap();
        assert_eq!(csv.header, vec!["a", "b", "c"]);
        assert_eq!(csv.rows.len(), 2);
        assert_eq!(
            csv.rows[0].cells,
            vec![Some("1".into()), Some("2".into()), Some("3".into())]
        );
        assert_eq!(csv.rows[0].line, 2);
        assert_eq!(csv.rows[1].line, 3);
    }

    #[test]
    fn strips_utf8_bom() {
        let body = b"\xEF\xBB\xBFa,b\n1,2\n";
        let csv = parse_csv(&p(), body).unwrap();
        assert_eq!(csv.header, vec!["a", "b"]);
    }

    #[test]
    fn handles_crlf_line_endings() {
        let body = b"a,b\r\n1,2\r\n";
        let csv = parse_csv(&p(), body).unwrap();
        assert_eq!(csv.header, vec!["a", "b"]);
        assert_eq!(csv.rows[0].cells, vec![Some("1".into()), Some("2".into())]);
    }

    #[test]
    fn empty_cell_is_none() {
        let body = b"a,b,c\n1,,3\n,5,\n";
        let csv = parse_csv(&p(), body).unwrap();
        assert_eq!(
            csv.rows[0].cells,
            vec![Some("1".into()), None, Some("3".into())]
        );
        assert_eq!(csv.rows[1].cells, vec![None, Some("5".into()), None]);
    }

    #[test]
    fn skips_blank_body_lines() {
        let body = b"a\n1\n\n2\n";
        let csv = parse_csv(&p(), body).unwrap();
        assert_eq!(csv.rows.len(), 2);
        assert_eq!(csv.rows[0].line, 2);
        assert_eq!(csv.rows[1].line, 4);
    }

    #[test]
    fn quoted_field_with_comma() {
        let body = b"a,b\n\"hello, world\",1\n";
        let csv = parse_csv(&p(), body).unwrap();
        assert_eq!(
            csv.rows[0].cells,
            vec![Some("hello, world".into()), Some("1".into())]
        );
    }

    #[test]
    fn quoted_field_with_escaped_quote() {
        let body = b"a\n\"he said \"\"hi\"\"\"\n";
        let csv = parse_csv(&p(), body).unwrap();
        assert_eq!(csv.rows[0].cells, vec![Some("he said \"hi\"".into())]);
    }

    #[test]
    fn unterminated_quote_errors() {
        let body = b"a\n\"unterminated\n";
        let err = parse_csv(&p(), body).unwrap_err();
        match err {
            Error::Parse { line, .. } => assert_eq!(line, 2),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn empty_file_errors() {
        let err = parse_csv(&p(), b"").unwrap_err();
        matches!(err, Error::Parse { .. });
    }

    #[test]
    fn missing_trailing_newline_still_yields_row() {
        let body = b"a,b\n1,2";
        let csv = parse_csv(&p(), body).unwrap();
        assert_eq!(csv.rows.len(), 1);
        assert_eq!(csv.rows[0].cells, vec![Some("1".into()), Some("2".into())]);
    }

    #[test]
    fn trailing_comma_yields_extra_empty_cell() {
        let body = b"a,b\n1,2,\n";
        let csv = parse_csv(&p(), body).unwrap();
        assert_eq!(
            csv.rows[0].cells,
            vec![Some("1".into()), Some("2".into()), None]
        );
    }
}

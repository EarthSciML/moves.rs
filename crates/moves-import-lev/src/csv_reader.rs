//! Minimal CSV reader tailored to the LEV/NLEV input format.
//!
//! Why hand-roll instead of using the `csv` crate: the LEV/NLEV input
//! format is a fixed nine-column shape with one row per primary-key
//! tuple, no quoting (numeric data only), and we want precise
//! line-number errors for the validator. The reader fits in ~50 lines
//! and pulls in no new dependencies.
//!
//! Format contract:
//!
//! * Comma-separated. The first non-empty line is the header.
//! * One row per line; trailing `\r` is stripped.
//! * Blank lines and lines whose first non-whitespace character is `#`
//!   are skipped (header `#` comments are not significant).
//! * A cell is "empty" if it parses to the empty string, the literal
//!   `NULL` (case-insensitive), or whitespace only — all three map to
//!   SQL NULL on output.
//! * Quoting is *not* supported. The validator's `ParseCell` error
//!   surfaces any embedded comma as a width mismatch or parse failure,
//!   which is the desired behavior for a tabular numeric format.

use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

/// A parsed input CSV: header + body rows, both with original line
/// numbers attached so the validator can quote them in errors.
#[derive(Debug, Clone)]
pub struct Csv {
    pub path: PathBuf,
    pub header: Vec<String>,
    pub rows: Vec<CsvRow>,
}

/// One body row of the CSV.
#[derive(Debug, Clone)]
pub struct CsvRow {
    /// 1-based line number in the source file.
    pub line: usize,
    /// Cells in the order they appeared. `None` represents an empty cell
    /// (blank, `NULL`, or whitespace-only).
    pub cells: Vec<Option<String>>,
}

/// Read a CSV file from disk.
pub fn read(path: &Path) -> Result<Csv> {
    let bytes = std::fs::read(path).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    parse(path, &bytes)
}

/// Parse a CSV from raw bytes. Validates header shape; row-content
/// validation is the validator's responsibility.
pub fn parse(path: &Path, bytes: &[u8]) -> Result<Csv> {
    let text = std::str::from_utf8(bytes).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source: std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("file is not utf-8: {source}"),
        ),
    })?;

    let mut header: Option<Vec<String>> = None;
    let mut rows = Vec::new();

    for (zero_based, raw_line) in text.lines().enumerate() {
        let line_no = zero_based + 1;
        let line = raw_line.strip_suffix('\r').unwrap_or(raw_line);

        let trimmed = line.trim_start();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        let fields: Vec<&str> = line.split(',').collect();

        if header.is_none() {
            let h: Vec<String> = fields.iter().map(|s| s.trim().to_string()).collect();
            check_header_shape(path, &h)?;
            header = Some(h);
            continue;
        }

        let header_width = header.as_ref().map_or(0, |h| h.len());
        if fields.len() != header_width {
            return Err(Error::RowWidthMismatch {
                path: path.to_path_buf(),
                line: line_no,
                expected: header_width,
                actual: fields.len(),
            });
        }
        let cells: Vec<Option<String>> = fields
            .into_iter()
            .map(|raw| {
                let trimmed = raw.trim();
                if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("NULL") {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            })
            .collect();
        rows.push(CsvRow {
            line: line_no,
            cells,
        });
    }

    let header = header.unwrap_or_default();
    Ok(Csv {
        path: path.to_path_buf(),
        header,
        rows,
    })
}

/// Reject duplicate header names so the row→column mapping is
/// unambiguous. Unknown / missing column checks happen in the validator
/// because they need the schema definition.
fn check_header_shape(path: &Path, header: &[String]) -> Result<()> {
    for (i, name) in header.iter().enumerate() {
        for prior in &header[..i] {
            if prior.eq_ignore_ascii_case(name) {
                return Err(Error::DuplicateColumn {
                    path: path.to_path_buf(),
                    column: name.clone(),
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p() -> &'static Path {
        Path::new("input.csv")
    }

    #[test]
    fn parses_header_and_one_row() {
        let body = b"sourceBinID,polProcessID,opModeID,ageGroupID,meanBaseRate\n\
                     1000,101,1,1,0.5\n";
        let csv = parse(p(), body).unwrap();
        assert_eq!(csv.header.len(), 5);
        assert_eq!(csv.header[0], "sourceBinID");
        assert_eq!(csv.rows.len(), 1);
        assert_eq!(csv.rows[0].line, 2);
        assert_eq!(csv.rows[0].cells[0].as_deref(), Some("1000"));
        assert_eq!(csv.rows[0].cells[4].as_deref(), Some("0.5"));
    }

    #[test]
    fn empty_and_null_cells_become_none() {
        let body = b"sourceBinID,polProcessID,opModeID,ageGroupID,meanBaseRate,meanBaseRateCV\n\
                     1000,101,1,1,0.5,\n\
                     1001,101,1,1,0.5,NULL\n\
                     1002,101,1,1,0.5,null\n";
        let csv = parse(p(), body).unwrap();
        assert_eq!(csv.rows.len(), 3);
        for row in &csv.rows {
            assert_eq!(row.cells[5], None);
        }
    }

    #[test]
    fn comments_and_blank_lines_skipped() {
        let body = b"# this is a comment header\n\
                     \n\
                     sourceBinID,polProcessID,opModeID,ageGroupID,meanBaseRate\n\
                     # mid-file comment\n\
                     \n\
                     1000,101,1,1,0.5\n";
        let csv = parse(p(), body).unwrap();
        assert_eq!(csv.header[0], "sourceBinID");
        assert_eq!(csv.rows.len(), 1);
        // Line 6 in the file is the data row.
        assert_eq!(csv.rows[0].line, 6);
    }

    #[test]
    fn header_must_be_unique() {
        let body = b"sourceBinID,sourceBinID,opModeID,ageGroupID,meanBaseRate\n";
        let err = parse(p(), body).unwrap_err();
        match err {
            Error::DuplicateColumn { column, .. } => assert_eq!(column, "sourceBinID"),
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn duplicate_column_check_is_case_insensitive() {
        let body = b"sourceBinID,SOURCEBINID,opModeID,ageGroupID,meanBaseRate\n";
        let err = parse(p(), body).unwrap_err();
        assert!(matches!(err, Error::DuplicateColumn { .. }));
    }

    #[test]
    fn row_width_must_match_header() {
        let body = b"sourceBinID,polProcessID,opModeID,ageGroupID,meanBaseRate\n\
                     1000,101,1,1\n";
        let err = parse(p(), body).unwrap_err();
        match err {
            Error::RowWidthMismatch {
                expected, actual, ..
            } => {
                assert_eq!(expected, 5);
                assert_eq!(actual, 4);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn crlf_line_endings_are_stripped() {
        let body =
            b"sourceBinID,polProcessID,opModeID,ageGroupID,meanBaseRate\r\n1000,101,1,1,0.5\r\n";
        let csv = parse(p(), body).unwrap();
        assert_eq!(csv.header[4], "meanBaseRate");
        assert_eq!(csv.rows[0].cells[4].as_deref(), Some("0.5"));
    }

    #[test]
    fn whitespace_around_cells_is_trimmed() {
        let body =
            b"sourceBinID, polProcessID, opModeID, ageGroupID, meanBaseRate\n  1000 , 101 ,1,1,0.5\n";
        let csv = parse(p(), body).unwrap();
        assert_eq!(csv.header[1], "polProcessID");
        assert_eq!(csv.rows[0].cells[0].as_deref(), Some("1000"));
        assert_eq!(csv.rows[0].cells[1].as_deref(), Some("101"));
    }

    #[test]
    fn empty_file_returns_empty_csv() {
        let csv = parse(p(), b"").unwrap();
        assert!(csv.header.is_empty());
        assert!(csv.rows.is_empty());
    }

    #[test]
    fn rejects_non_utf8_input() {
        // 0xFF is not a valid UTF-8 start byte.
        let body: Vec<u8> = vec![b'a', b',', b'b', 0xFF, b'\n'];
        let err = parse(p(), &body).unwrap_err();
        assert!(matches!(err, Error::Io { .. }));
    }
}

//! CSV read/write for the AVFT table.
//!
//! The CSV shape mirrors the canonical SQL `avft` table:
//!
//! ```text
//! sourceTypeID,modelYearID,fuelTypeID,engTechID,fuelEngFraction
//! 11,2022,1,1,0.95
//! 11,2022,2,1,0.05
//! ```
//!
//! Column order in the input file is not significant — the header line
//! is consulted to map column names to fields. Output is always written
//! in the canonical order shown above.

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use crate::error::{Error, Result};
use crate::model::{AvftRecord, AvftTable};

/// Canonical AVFT column names, in canonical write order.
pub const COLUMNS: [&str; 5] = [
    "sourceTypeID",
    "modelYearID",
    "fuelTypeID",
    "engTechID",
    "fuelEngFraction",
];

/// Report returned by [`read_csv`].
///
/// Holds the parsed table plus any cosmetic warnings the reader picked
/// up that aren't fatal (currently only "duplicate primary key seen").
/// Hard errors (negative fractions, header mismatch, unparseable rows)
/// short-circuit and surface as [`Error`] instead.
#[derive(Debug, Default)]
pub struct ReadReport {
    pub table: AvftTable,
    pub duplicate_keys: Vec<AvftRecord>,
}

/// Read a CSV file into an [`AvftTable`].
pub fn read_csv(path: impl AsRef<Path>) -> Result<ReadReport> {
    let path = path.as_ref();
    let file = File::open(path).map_err(|e| Error::io(path, e))?;
    parse_reader(BufReader::new(file), path)
}

/// Read a CSV from any [`Read`] source (handy for in-memory test
/// fixtures). The `display_path` is used only for error messages.
pub fn read_reader(reader: impl Read, display_path: impl AsRef<Path>) -> Result<ReadReport> {
    parse_reader(reader, display_path.as_ref())
}

fn parse_reader(reader: impl Read, path: &Path) -> Result<ReadReport> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(false)
        .from_reader(reader);

    let headers = rdr
        .headers()
        .map_err(|e| Error::CsvParse {
            path: path.to_path_buf(),
            line: 1,
            message: format!("could not read header: {e}"),
        })?
        .clone();

    let header_strings: Vec<String> = headers.iter().map(|s| s.to_string()).collect();
    let positions = match resolve_positions(&header_strings) {
        Some(p) => p,
        None => {
            return Err(Error::BadCsvHeader {
                path: path.to_path_buf(),
                got: header_strings,
                want: COLUMNS.to_vec(),
            });
        }
    };

    let mut table = AvftTable::new();
    let mut duplicates: Vec<AvftRecord> = Vec::new();
    let mut record = csv::StringRecord::new();
    while rdr.read_record(&mut record).map_err(|e| Error::CsvParse {
        path: path.to_path_buf(),
        line: record_line(&record),
        message: format!("could not read record: {e}"),
    })? {
        if record.len() != COLUMNS.len() {
            return Err(Error::CsvParse {
                path: path.to_path_buf(),
                line: record_line(&record),
                message: format!("expected {} columns, got {}", COLUMNS.len(), record.len()),
            });
        }
        let parsed = parse_record(&record, &positions).map_err(|message| Error::CsvParse {
            path: path.to_path_buf(),
            line: record_line(&record),
            message,
        })?;
        if table.contains_key(&parsed.key()) {
            duplicates.push(parsed);
        }
        table.insert(parsed);
    }

    Ok(ReadReport {
        table,
        duplicate_keys: duplicates,
    })
}

fn record_line(record: &csv::StringRecord) -> u64 {
    record.position().map(|p| p.line()).unwrap_or(0)
}

/// Positions of canonical fields within the source CSV's actual column
/// order. Returns `None` if any required column is missing.
fn resolve_positions(headers: &[String]) -> Option<[usize; 5]> {
    let find = |want: &str| headers.iter().position(|h| h.eq_ignore_ascii_case(want));
    Some([
        find("sourceTypeID")?,
        find("modelYearID")?,
        find("fuelTypeID")?,
        find("engTechID")?,
        find("fuelEngFraction")?,
    ])
}

fn parse_record(
    record: &csv::StringRecord,
    positions: &[usize; 5],
) -> std::result::Result<AvftRecord, String> {
    let s = |i: usize| -> &str { &record[positions[i]] };
    let st = parse_i32(s(0), COLUMNS[0])?;
    let my = parse_i32(s(1), COLUMNS[1])?;
    let fuel = parse_i32(s(2), COLUMNS[2])?;
    let eng = parse_i32(s(3), COLUMNS[3])?;
    let frac = parse_f64(s(4), COLUMNS[4])?;
    Ok(AvftRecord {
        source_type_id: st,
        model_year_id: my,
        fuel_type_id: fuel,
        eng_tech_id: eng,
        fuel_eng_fraction: frac,
    })
}

fn parse_i32(raw: &str, field: &str) -> std::result::Result<i32, String> {
    raw.trim()
        .parse::<i32>()
        .map_err(|e| format!("{field}: could not parse {raw:?} as integer: {e}"))
}

fn parse_f64(raw: &str, field: &str) -> std::result::Result<f64, String> {
    raw.trim()
        .parse::<f64>()
        .map_err(|e| format!("{field}: could not parse {raw:?} as number: {e}"))
}

/// Write an [`AvftTable`] as CSV to disk.
///
/// Output is in canonical column order (see [`COLUMNS`]) and canonical
/// row order (key-lexicographic, matching `AVFTTool_OrderResults`).
/// Fractions are written as the shortest decimal that round-trips to
/// the same `f64`, so output is byte stable across platforms.
pub fn write_csv(table: &AvftTable, path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();
    let file = File::create(path).map_err(|e| Error::io(path, e))?;
    write_writer(table, BufWriter::new(file))
}

/// Write an [`AvftTable`] as CSV into any [`Write`] sink.
pub fn write_writer(table: &AvftTable, writer: impl Write) -> Result<()> {
    let mut wtr = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(writer);
    wtr.write_record(COLUMNS).map_err(map_csv_write)?;
    for r in table.iter() {
        wtr.write_record([
            r.source_type_id.to_string(),
            r.model_year_id.to_string(),
            r.fuel_type_id.to_string(),
            r.eng_tech_id.to_string(),
            format_fraction(r.fuel_eng_fraction),
        ])
        .map_err(map_csv_write)?;
    }
    wtr.flush().map_err(|e| Error::Io {
        path: std::path::PathBuf::from("<csv writer>"),
        source: e,
    })?;
    Ok(())
}

fn map_csv_write(e: csv::Error) -> Error {
    let message = e.to_string();
    let io_kind = e.into_kind();
    match io_kind {
        csv::ErrorKind::Io(io) => Error::Io {
            path: std::path::PathBuf::from("<csv writer>"),
            source: io,
        },
        _ => Error::Io {
            path: std::path::PathBuf::from("<csv writer>"),
            source: std::io::Error::other(message),
        },
    }
}

/// Format a fraction so it round-trips with full `f64` precision while
/// staying compact for human-readable diffs. Integers come out as
/// `"0"`/`"1"`; finite non-integers use Rust's default `{}` formatter
/// (round-trippable). Non-finite values pass through as `inf`/`nan`,
/// which downstream code rejects in [`crate::import::validate`].
fn format_fraction(v: f64) -> String {
    if v == 0.0 {
        return "0".to_string();
    }
    if v == 1.0 {
        return "1".to_string();
    }
    format!("{}", v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_with_canonical_order() {
        let t: AvftTable = [
            AvftRecord::new(21, 2020, 1, 1, 0.7),
            AvftRecord::new(11, 2020, 2, 1, 0.3),
            AvftRecord::new(11, 2020, 1, 1, 0.7),
        ]
        .into_iter()
        .collect();

        let mut buf: Vec<u8> = Vec::new();
        write_writer(&t, &mut buf).unwrap();
        let text = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(
            lines[0],
            "sourceTypeID,modelYearID,fuelTypeID,engTechID,fuelEngFraction"
        );
        // canonical row order: (11, 2020, 1, 1), (11, 2020, 2, 1), (21, 2020, 1, 1)
        assert_eq!(lines[1], "11,2020,1,1,0.7");
        assert_eq!(lines[2], "11,2020,2,1,0.3");
        assert_eq!(lines[3], "21,2020,1,1,0.7");

        let parsed = read_reader(text.as_bytes(), Path::new("test")).unwrap();
        assert_eq!(parsed.table.len(), 3);
        let recs: Vec<_> = parsed.table.iter().collect();
        assert_eq!(recs[0].source_type_id, 11);
        assert_eq!(recs[0].fuel_type_id, 1);
        assert_eq!(recs[2].source_type_id, 21);
    }

    #[test]
    fn header_can_be_in_any_order() {
        let text =
            "engTechID,fuelTypeID,modelYearID,fuelEngFraction,sourceTypeID\n1,2,2020,0.5,11\n";
        let r = read_reader(text.as_bytes(), Path::new("test")).unwrap();
        let rec = r.table.iter().next().unwrap();
        assert_eq!(rec.source_type_id, 11);
        assert_eq!(rec.fuel_type_id, 2);
        assert_eq!(rec.eng_tech_id, 1);
        assert_eq!(rec.fuel_eng_fraction, 0.5);
    }

    #[test]
    fn header_is_case_insensitive() {
        let text =
            "SOURCETYPEID,MODELYEARID,FUELTYPEID,ENGTECHID,FUELENGFRACTION\n11,2020,1,1,0.5\n";
        let r = read_reader(text.as_bytes(), Path::new("test")).unwrap();
        assert_eq!(r.table.len(), 1);
    }

    #[test]
    fn missing_required_column_is_error() {
        let text = "sourceTypeID,modelYearID,fuelTypeID,fuelEngFraction\n11,2020,1,0.5\n";
        let err = read_reader(text.as_bytes(), Path::new("missing.csv")).unwrap_err();
        match err {
            Error::BadCsvHeader { .. } => {}
            other => panic!("expected BadCsvHeader, got {other:?}"),
        }
    }

    #[test]
    fn non_numeric_cell_is_csv_parse_error() {
        let text =
            "sourceTypeID,modelYearID,fuelTypeID,engTechID,fuelEngFraction\n11,2020,1,1,not-a-number\n";
        let err = read_reader(text.as_bytes(), Path::new("bad.csv")).unwrap_err();
        match err {
            Error::CsvParse { message, .. } => assert!(message.contains("fuelEngFraction")),
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn duplicate_keys_are_reported_not_error() {
        let text = "sourceTypeID,modelYearID,fuelTypeID,engTechID,fuelEngFraction\n11,2020,1,1,0.4\n11,2020,1,1,0.6\n";
        let r = read_reader(text.as_bytes(), Path::new("dup.csv")).unwrap();
        assert_eq!(r.table.len(), 1);
        assert_eq!(r.duplicate_keys.len(), 1);
    }

    #[test]
    fn formats_zero_and_one_compactly() {
        assert_eq!(format_fraction(0.0), "0");
        assert_eq!(format_fraction(1.0), "1");
        assert_eq!(format_fraction(0.5), "0.5");
    }
}

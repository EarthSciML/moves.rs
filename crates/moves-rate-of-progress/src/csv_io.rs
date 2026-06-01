//! CSV read/write for the Rate-of-Progress reduction table.
//!
//! The CSV shape mirrors the logical table keyed by
//! `(pollutantID, sourceTypeID, regClassID, modelYearID)`:
//!
//! ```text
//! pollutantID,sourceTypeID,regClassID,modelYearID,reductionFraction
//! 3,11,10,2022,0.25
//! 3,21,10,2022,0.10
//! ```
//!
//! Column order in the input file is not significant — the header line is
//! consulted to map column names to fields. Output is always written in the
//! canonical column order shown above.

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use crate::error::{Error, Result};
use crate::model::{RopRecord, RopTable};

/// Canonical ROP column names, in canonical write order.
pub const COLUMNS: [&str; 5] = [
    "pollutantID",
    "sourceTypeID",
    "regClassID",
    "modelYearID",
    "reductionFraction",
];

/// Report returned by [`read_csv`].
///
/// Holds the parsed table plus any non-fatal warnings. Hard errors
/// short-circuit as [`Error`].
#[derive(Debug, Default)]
pub struct ReadReport {
    pub table: RopTable,
 /// Records whose key was already present in the table when they were
 /// encountered (last-write-wins, but duplicates are reported here).
    pub duplicate_keys: Vec<RopRecord>,
 /// Records with `reductionFraction` outside `[0.0, 1.0]` that were
 /// rejected.
    pub invalid_fractions: Vec<RopRecord>,
}

/// Read a ROP CSV file from disk.
pub fn read_csv(path: impl AsRef<Path>) -> Result<ReadReport> {
    let path = path.as_ref();
    let file = File::open(path).map_err(|e| Error::io(path, e))?;
    parse_reader(BufReader::new(file), path)
}

/// Read a ROP CSV from any [`Read`] source (useful in tests).
///
/// `display_path` is used only for error messages.
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

    let mut table = RopTable::new();
    let mut duplicates: Vec<RopRecord> = Vec::new();
    let mut invalids: Vec<RopRecord> = Vec::new();
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
        if !(0.0..=1.0).contains(&parsed.reduction_fraction) {
            invalids.push(parsed);
            continue;
        }
        if table.contains_key(&parsed.key()) {
            duplicates.push(parsed);
        }
        table.insert(parsed);
    }

    Ok(ReadReport {
        table,
        duplicate_keys: duplicates,
        invalid_fractions: invalids,
    })
}

fn record_line(record: &csv::StringRecord) -> u64 {
    record.position().map(|p| p.line()).unwrap_or(0)
}

fn resolve_positions(headers: &[String]) -> Option<[usize; 5]> {
    let find = |want: &str| headers.iter().position(|h| h.eq_ignore_ascii_case(want));
    Some([
        find("pollutantID")?,
        find("sourceTypeID")?,
        find("regClassID")?,
        find("modelYearID")?,
        find("reductionFraction")?,
    ])
}

fn parse_record(
    record: &csv::StringRecord,
    positions: &[usize; 5],
) -> std::result::Result<RopRecord, String> {
    let s = |i: usize| -> &str { &record[positions[i]] };
    let pollutant = parse_i32(s(0), COLUMNS[0])?;
    let source_type = parse_i32(s(1), COLUMNS[1])?;
    let reg_class = parse_i32(s(2), COLUMNS[2])?;
    let model_year = parse_i32(s(3), COLUMNS[3])?;
    let reduction = parse_f64(s(4), COLUMNS[4])?;
    Ok(RopRecord {
        pollutant_id: pollutant,
        source_type_id: source_type,
        reg_class_id: reg_class,
        model_year_id: model_year,
        reduction_fraction: reduction,
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

/// Write a [`RopTable`] as CSV to disk.
///
/// Output is in canonical column order (see [`COLUMNS`]) and canonical
/// row order (key-lexicographic). Fractions are written as the shortest
/// decimal that round-trips to the same `f64`.
pub fn write_csv(table: &RopTable, path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();
    let file = File::create(path).map_err(|e| Error::io(path, e))?;
    write_writer(table, BufWriter::new(file))
}

/// Write a [`RopTable`] as CSV into any [`Write`] sink.
pub fn write_writer(table: &RopTable, writer: impl Write) -> Result<()> {
    let mut wtr = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(writer);
    wtr.write_record(COLUMNS).map_err(map_csv_write)?;
    for r in table.iter() {
        wtr.write_record([
            r.pollutant_id.to_string(),
            r.source_type_id.to_string(),
            r.reg_class_id.to_string(),
            r.model_year_id.to_string(),
            format!("{}", r.reduction_fraction),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_with_canonical_order() {
        let t: RopTable = [
            RopRecord::new(2, 21, 10, 2020, 0.10),
            RopRecord::new(1, 11, 10, 2020, 0.25),
            RopRecord::new(1, 21, 20, 2020, 0.15),
        ]
        .into_iter()
        .collect();

        let mut buf: Vec<u8> = Vec::new();
        write_writer(&t, &mut buf).unwrap();
        let text = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(
            lines[0],
            "pollutantID,sourceTypeID,regClassID,modelYearID,reductionFraction"
        );
 // canonical row order is key-lexicographic
        assert_eq!(lines[1], "1,11,10,2020,0.25");
        assert_eq!(lines[2], "1,21,20,2020,0.15");
        assert_eq!(lines[3], "2,21,10,2020,0.1");

        let parsed = read_reader(text.as_bytes(), Path::new("test")).unwrap();
        assert_eq!(parsed.table.len(), 3);
    }

    #[test]
    fn header_can_be_in_any_order() {
        let text = "reductionFraction,modelYearID,regClassID,sourceTypeID,pollutantID\n\
                    0.2,2022,10,11,3\n";
        let r = read_reader(text.as_bytes(), Path::new("test")).unwrap();
        let rec = r.table.iter().next().unwrap();
        assert_eq!(rec.pollutant_id, 3);
        assert_eq!(rec.source_type_id, 11);
        assert_eq!(rec.reg_class_id, 10);
        assert_eq!(rec.model_year_id, 2022);
        assert!((rec.reduction_fraction - 0.2).abs() < 1e-15);
    }

    #[test]
    fn header_is_case_insensitive() {
        let text = "POLLUTANTID,SOURCETYPEID,REGCLASSID,MODELYEARID,REDUCTIONFRACTION\n\
                    1,11,10,2020,0.1\n";
        let r = read_reader(text.as_bytes(), Path::new("test")).unwrap();
        assert_eq!(r.table.len(), 1);
    }

    #[test]
    fn missing_required_column_is_error() {
        let text = "pollutantID,sourceTypeID,regClassID,modelYearID\n1,11,10,2020\n";
        let err = read_reader(text.as_bytes(), Path::new("missing.csv")).unwrap_err();
        match err {
            Error::BadCsvHeader { .. } => {}
            other => panic!("expected BadCsvHeader, got {other:?}"),
        }
    }

    #[test]
    fn non_numeric_cell_is_csv_parse_error() {
        let text = "pollutantID,sourceTypeID,regClassID,modelYearID,reductionFraction\n\
                    1,11,10,2020,bad\n";
        let err = read_reader(text.as_bytes(), Path::new("bad.csv")).unwrap_err();
        match err {
            Error::CsvParse { message, .. } => assert!(message.contains("reductionFraction")),
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn duplicate_keys_are_reported_not_error() {
        let text = "pollutantID,sourceTypeID,regClassID,modelYearID,reductionFraction\n\
                    1,11,10,2020,0.1\n\
                    1,11,10,2020,0.2\n";
        let r = read_reader(text.as_bytes(), Path::new("dup.csv")).unwrap();
        assert_eq!(r.table.len(), 1);
        assert_eq!(r.duplicate_keys.len(), 1);
    }

    #[test]
    fn out_of_range_fractions_are_skipped_and_reported() {
        let text = "pollutantID,sourceTypeID,regClassID,modelYearID,reductionFraction\n\
                    1,11,10,2020,0.5\n\
                    2,11,10,2020,1.5\n\
                    3,11,10,2020,-0.1\n";
        let r = read_reader(text.as_bytes(), Path::new("oob.csv")).unwrap();
        assert_eq!(r.table.len(), 1);
        assert_eq!(r.invalid_fractions.len(), 2);
    }
}

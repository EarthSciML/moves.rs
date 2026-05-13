//! CSV → Arrow `RecordBatch` reader for one PDB table.
//!
//! Mirrors the file-handling slice of `BasicDataHandler.doImport`
//! (lines 728-993 of `BasicDataHandler.java`):
//!
//! 1. Skip blank/skippable leading rows; first non-blank row is the
//!    header. (Java: lines 797-803.)
//! 2. Build a per-file column-name → schema-column-index map,
//!    case-insensitively. Reject the file if any required schema
//!    column is missing. (Java: lines 837-857.)
//! 3. For each subsequent row: read each cell into the column's
//!    declared type. If a non-nullable cell is empty, emit a
//!    `MissingRequiredCell` error. (Java: lines 862-916.)
//! 4. Apply the column's [`Filter`](crate::filter::Filter) to the parsed value. Java keeps
//!    the row and emits a WARNING; we surface those as
//!    [`ImportWarning`] entries on the [`ImportReport`].
//!    (Java: lines 922-958.)
//!
//! ## What we don't replicate
//!
//! * **`xls/xlsx` worksheets.** Java's `CellFileReader` transparently
//!   handles Excel via Apache POI. We support CSV only — the file
//!   format that 95% of real-world PDB users actually use.
//! * **String-length truncation.** Java truncates string cells to the
//!   target column's max length (line 882). The only project-only
//!   string column is `Link.linkDescription` (`varchar(50)`); we leave
//!   long strings as-is and let downstream consumers truncate if they
//!   care. Parquet has no width limit.
//! * **Wildcards (`manager.setupWildcards`).** Java lets users put
//!   `0` in `monthID`, `hourDayID`, etc. as a "applies to every value"
//!   marker. None of the project-only tables declare a wildcard
//!   column on the Java side, so we don't expand any.

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use crate::error::{Error, Result};
use crate::filter::{CellValue, FilterOutcome, RunSpecFilter};
use crate::schema::TableSchema;

/// One row that survived parsing but tripped a column filter. Mirrors
/// Java's `WARNING: <column> <value> is not used.` lines.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ImportWarning {
    pub line: u64,
    pub column: String,
    pub message: String,
}

/// Outcome of one CSV → RecordBatch conversion.
#[derive(Debug)]
pub struct ImportReport {
    /// The parsed table data, schema-aligned with [`TableSchema::arrow_schema`].
    pub batch: RecordBatch,
    /// Filter-rejected cells, in encounter order.
    pub warnings: Vec<ImportWarning>,
    /// Total data rows seen (excluding the header). Useful for the
    /// "Imported N rows" message Java's `BasicDataHandler` writes.
    pub rows_read: u64,
    /// Path of the file that was read (for downstream error context).
    pub source_path: PathBuf,
}

/// Read a CSV file into one [`RecordBatch`] following `schema`.
pub fn read_csv(
    path: &Path,
    schema: &TableSchema,
    runspec: &RunSpecFilter,
) -> Result<ImportReport> {
    let file = File::open(path).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    read_csv_from_reader(BufReader::new(file), path, schema, runspec)
}

/// Same as [`read_csv`], but reads from any [`Read`] source — used by
/// in-memory unit tests so we don't have to round-trip through the
/// filesystem.
pub fn read_csv_from_reader<R: Read>(
    reader: R,
    source_path: &Path,
    schema: &TableSchema,
    runspec: &RunSpecFilter,
) -> Result<ImportReport> {
    let mut csv_reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true) // tolerate trailing blank cells (Java tolerates them)
        .trim(csv::Trim::All)
        .from_reader(reader);

    // ------------------------------------------------------------------
    // Header → schema-column-index map.
    // ------------------------------------------------------------------
    let headers = csv_reader.headers().map_err(|e| Error::Csv {
        path: source_path.to_path_buf(),
        line: 1,
        message: format!("could not read header row: {e}"),
    })?;
    let mut column_to_schema: Vec<Option<usize>> = Vec::with_capacity(headers.len());
    for header in headers.iter() {
        let header_trimmed = header.trim();
        if header_trimmed.is_empty() {
            column_to_schema.push(None);
            continue;
        }
        column_to_schema.push(schema.find_column(header_trimmed).map(|(i, _)| i));
    }
    // Java rejects the file if any *required* schema column is missing
    // from the header (line 853 of `BasicDataHandler.java`). Required
    // here = listed in the descriptor — every descriptor column must
    // have a header, even nullable ones, otherwise Java emits
    // `ERROR: Missing column ... for ... table.`
    for (schema_idx, col) in schema.columns.iter().enumerate() {
        if !column_to_schema.contains(&Some(schema_idx)) {
            return Err(Error::MissingColumn {
                table: schema.name.to_string(),
                column: col.name.to_string(),
                path: source_path.to_path_buf(),
            });
        }
    }

    // ------------------------------------------------------------------
    // Builders, one per schema column. Pre-grow to a small initial
    // capacity; csv::Reader doesn't expose row count up front.
    // ------------------------------------------------------------------
    let mut builders: Vec<ColumnBuilder> = schema
        .columns
        .iter()
        .map(|c| ColumnBuilder::new(&c.data_type))
        .collect();

    let mut warnings: Vec<ImportWarning> = Vec::new();
    let mut rows_read: u64 = 0;
    let mut record = csv::StringRecord::new();
    while csv_reader
        .read_record(&mut record)
        .map_err(|e| Error::Csv {
            path: source_path.to_path_buf(),
            line: record.position().map(|p| p.line()).unwrap_or(0),
            message: format!("could not read row: {e}"),
        })?
    {
        // Java skips blank lines silently (lines 862-866). The csv
        // crate already drops fully-empty records, but a row that's
        // all-comma still arrives as a record of N empty fields —
        // detect that here.
        if record.iter().all(|f| f.trim().is_empty()) {
            continue;
        }

        rows_read += 1;
        let line_no = record.position().map(|p| p.line()).unwrap_or(rows_read + 1);

        // Per-row scratch: parsed cell values for filter checks.
        let mut parsed: Vec<Option<CellValue>> = vec![None; schema.columns.len()];

        // Walk the file's columns in declared order so unmapped
        // columns stay skipped without disturbing index alignment.
        for (file_col_idx, raw) in record.iter().enumerate() {
            let Some(schema_idx) = column_to_schema.get(file_col_idx).copied().flatten() else {
                continue;
            };
            let col = &schema.columns[schema_idx];
            let raw = raw.trim();
            if raw.is_empty() {
                if !col.nullable {
                    return Err(Error::MissingRequiredCell {
                        table: schema.name.to_string(),
                        line: line_no,
                        column: col.name.to_string(),
                        path: source_path.to_path_buf(),
                    });
                }
                builders[schema_idx].append_null();
                continue;
            }
            match &col.data_type {
                DataType::Int64 => {
                    // Java accepts strings like "1.0" for integer
                    // columns by going through `readDoubleCell` →
                    // toString → parseInt; mirror that by trying
                    // parse::<i64> first, then float-then-truncate.
                    let parsed_i64 = raw
                        .parse::<i64>()
                        .or_else(|_| raw.parse::<f64>().map(|f| f as i64))
                        .map_err(|_| Error::Parse {
                            table: schema.name.to_string(),
                            line: line_no,
                            column: col.name.to_string(),
                            expected: "integer",
                            value: raw.to_string(),
                            path: source_path.to_path_buf(),
                        })?;
                    builders[schema_idx].append_int(parsed_i64);
                    parsed[schema_idx] = Some(CellValue::Int(parsed_i64));
                }
                DataType::Float64 => {
                    let parsed_f64 = raw.parse::<f64>().map_err(|_| Error::Parse {
                        table: schema.name.to_string(),
                        line: line_no,
                        column: col.name.to_string(),
                        expected: "float",
                        value: raw.to_string(),
                        path: source_path.to_path_buf(),
                    })?;
                    builders[schema_idx].append_float(parsed_f64);
                    parsed[schema_idx] = Some(CellValue::Float(parsed_f64));
                }
                DataType::Utf8 => {
                    builders[schema_idx].append_string(raw);
                }
                other => {
                    return Err(Error::Parse {
                        table: schema.name.to_string(),
                        line: line_no,
                        column: col.name.to_string(),
                        expected: "supported arrow type",
                        value: format!("{other:?}"),
                        path: source_path.to_path_buf(),
                    });
                }
            }
        }

        // Filter pass — Java keeps the row but emits a warning. We
        // do the same; downstream callers can choose to hard-fail
        // by inspecting `ImportReport::warnings`.
        for (idx, col) in schema.columns.iter().enumerate() {
            let Some(filter) = &col.filter else {
                continue;
            };
            let Some(value) = parsed[idx] else {
                continue;
            };
            if let FilterOutcome::Filtered { reason } = filter.check(value, runspec) {
                warnings.push(ImportWarning {
                    line: line_no,
                    column: col.name.to_string(),
                    message: reason,
                });
            }
        }
    }

    let arrays: Vec<ArrayRef> = builders.into_iter().map(ColumnBuilder::finish).collect();
    let batch = RecordBatch::try_new(schema.arrow_schema(), arrays)?;

    Ok(ImportReport {
        batch,
        warnings,
        rows_read,
        source_path: source_path.to_path_buf(),
    })
}

enum ColumnBuilder {
    Int(Int64Builder),
    Float(Float64Builder),
    Utf8(StringBuilder),
}

impl ColumnBuilder {
    fn new(ty: &DataType) -> Self {
        match ty {
            DataType::Int64 => ColumnBuilder::Int(Int64Builder::new()),
            DataType::Float64 => ColumnBuilder::Float(Float64Builder::new()),
            DataType::Utf8 => ColumnBuilder::Utf8(StringBuilder::new()),
            other => panic!("unsupported arrow type for PDB importer: {other:?}"),
        }
    }

    fn append_null(&mut self) {
        match self {
            ColumnBuilder::Int(b) => b.append_null(),
            ColumnBuilder::Float(b) => b.append_null(),
            ColumnBuilder::Utf8(b) => b.append_null(),
        }
    }

    fn append_int(&mut self, v: i64) {
        if let ColumnBuilder::Int(b) = self {
            b.append_value(v);
        } else {
            panic!("append_int on non-int builder");
        }
    }

    fn append_float(&mut self, v: f64) {
        if let ColumnBuilder::Float(b) = self {
            b.append_value(v);
        } else {
            panic!("append_float on non-float builder");
        }
    }

    fn append_string(&mut self, s: &str) {
        if let ColumnBuilder::Utf8(b) = self {
            b.append_value(s);
        } else {
            panic!("append_string on non-string builder");
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            ColumnBuilder::Int(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Float(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Utf8(mut b) => Arc::new(b.finish()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{LINK, LINK_SOURCE_TYPE_HOUR, OFF_NETWORK_LINK};
    use std::io::Cursor;

    fn read(csv: &str, schema: &TableSchema, runspec: &RunSpecFilter) -> Result<ImportReport> {
        read_csv_from_reader(
            Cursor::new(csv.as_bytes()),
            Path::new("(test)"),
            schema,
            runspec,
        )
    }

    #[test]
    fn link_csv_round_trip() {
        let csv = "linkID,countyID,zoneID,roadTypeID,linkLength,linkVolume,linkAvgSpeed,linkDescription,linkAvgGrade
1,26161,261610,4,0.5,1000,55,downtown,0.0
2,26161,261610,4,0.25,500,45,bypass,
";
        let report = read(csv, &LINK, &RunSpecFilter::default()).expect("parse");
        assert_eq!(report.rows_read, 2);
        assert_eq!(report.batch.num_rows(), 2);
        assert!(report.warnings.is_empty());
        // grade column 8 should have a null on row 1
        assert!(report.batch.column(8).is_null(1));
    }

    #[test]
    fn missing_required_column_errors() {
        let csv = "linkID,countyID,zoneID\n1,26161,261610\n";
        let err = read(csv, &LINK, &RunSpecFilter::default()).unwrap_err();
        match err {
            Error::MissingColumn { column, .. } => assert_eq!(column, "roadTypeID"),
            other => panic!("wanted MissingColumn, got {other:?}"),
        }
    }

    #[test]
    fn missing_required_cell_errors() {
        let csv = "linkID,countyID,zoneID,roadTypeID,linkLength,linkVolume,linkAvgSpeed,linkDescription,linkAvgGrade
1,26161,,4,0.5,1000,55,,
";
        let err = read(csv, &LINK, &RunSpecFilter::default()).unwrap_err();
        match err {
            Error::MissingRequiredCell { column, .. } => assert_eq!(column, "zoneID"),
            other => panic!("wanted MissingRequiredCell, got {other:?}"),
        }
    }

    #[test]
    fn header_match_is_case_insensitive() {
        let csv = "LINKID,COUNTYID,ZONEID,ROADTYPEID,LINKLENGTH,LINKVOLUME,LINKAVGSPEED,LINKDESCRIPTION,LINKAVGGRADE
1,26161,261610,4,0.5,1000,55,x,0
";
        let report = read(csv, &LINK, &RunSpecFilter::default()).expect("parse");
        assert_eq!(report.rows_read, 1);
    }

    #[test]
    fn membership_filter_emits_warning_keeps_row() {
        let csv = "linkID,countyID,zoneID,roadTypeID,linkLength,linkVolume,linkAvgSpeed,linkDescription,linkAvgGrade
1,26161,261610,4,0.5,1000,55,a,0
2,99999,999990,4,0.5,1000,55,b,0
";
        let runspec = RunSpecFilter::default().with_counties([26161]);
        let report = read(csv, &LINK, &runspec).expect("parse");
        assert_eq!(report.batch.num_rows(), 2, "Java keeps filtered rows");
        assert_eq!(report.warnings.len(), 1);
        assert_eq!(report.warnings[0].column, "countyID");
        assert!(report.warnings[0].message.contains("99999"));
    }

    #[test]
    fn extra_columns_in_file_are_ignored() {
        // CSV has a trailing column the importer doesn't know about.
        // Java's `BasicDataHandler` reads it via `skipCell()` (line 874).
        let csv = "linkID,countyID,zoneID,roadTypeID,linkLength,linkVolume,linkAvgSpeed,linkDescription,linkAvgGrade,extraJunk
1,26161,261610,4,0.5,1000,55,a,0,trailing
";
        let report = read(csv, &LINK, &RunSpecFilter::default()).expect("parse");
        assert_eq!(report.rows_read, 1);
        assert_eq!(report.batch.num_columns(), 9);
    }

    #[test]
    fn float_fraction_round_trips_for_link_source_type_hour() {
        let csv = "linkID,sourceTypeID,sourceTypeHourFraction
1,21,0.4
1,32,0.6
";
        let report = read(csv, &LINK_SOURCE_TYPE_HOUR, &RunSpecFilter::default()).expect("parse");
        assert_eq!(report.rows_read, 2);
        let frac = report
            .batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("float64 column");
        assert!((frac.value(0) - 0.4).abs() < 1e-12);
        assert!((frac.value(1) - 0.6).abs() < 1e-12);
    }

    #[test]
    fn off_network_link_round_trip() {
        let csv = "zoneID,sourceTypeID,vehiclePopulation,startFraction,extendedIdleFraction,parkedVehicleFraction
261610,21,1000,0.1,0.0,0.9
261610,32,500,0.05,0.2,0.75
";
        let report = read(csv, &OFF_NETWORK_LINK, &RunSpecFilter::default()).expect("parse");
        assert_eq!(report.rows_read, 2);
        let pop = report
            .batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(pop.value(0), 1000.0);
    }

    #[test]
    fn integer_column_accepts_dot_zero_form() {
        let csv = "linkID,countyID,zoneID,roadTypeID,linkLength,linkVolume,linkAvgSpeed,linkDescription,linkAvgGrade
1.0,26161.0,261610.0,4.0,0.5,1000,55,a,0
";
        let report = read(csv, &LINK, &RunSpecFilter::default()).expect("parse");
        let link_id = report
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(link_id.value(0), 1);
    }

    #[test]
    fn rejects_non_numeric_int_column() {
        let csv = "linkID,countyID,zoneID,roadTypeID,linkLength,linkVolume,linkAvgSpeed,linkDescription,linkAvgGrade
abc,26161,261610,4,0.5,1000,55,a,0
";
        let err = read(csv, &LINK, &RunSpecFilter::default()).unwrap_err();
        match err {
            Error::Parse { column, .. } => assert_eq!(column, "linkID"),
            other => panic!("wanted Parse error, got {other:?}"),
        }
    }
}

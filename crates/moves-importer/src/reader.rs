//! Read a user-supplied CSV into an Arrow [`RecordBatch`] typed against a
//! [`TableDescriptor`].
//!
//! MOVES' Java importers accept both `.csv` and `.xls` / `.xlsx`. The
//! Rust port handles CSV here; Excel support belongs in a follow-up
//! task that wires `calamine` or a similar crate. The `.txt` extension
//! is accepted as a synonym for CSV (MOVES does the same).
//!
//! ## Header matching
//!
//! MOVES is case-insensitive about column headers in user CSVs — the
//! Java reader compares headers with `equalsIgnoreCase`. We do the same
//! and emit [`Error::MissingColumn`] when a descriptor column isn't
//! found. Extra columns in the CSV are ignored (this matches the Java
//! `BasicDataHandler` which only reads columns named in the
//! descriptor).
//!
//! ## Null handling
//!
//! An empty cell becomes a null. A literal `NULL` token (case-insensitive)
//! also becomes a null. Anything else is fed to the type coercion path.
//! The validator decides whether a null is fatal — see
//! [`Filter::nullable`](crate::Filter::nullable).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::descriptor::TableDescriptor;
use crate::error::{Error, Result};

/// One CSV-derived table.
///
/// `source_path` is carried through so validation messages can quote
/// the file the user provided. `batch.num_rows()` is the number of data
/// rows (header row not included).
#[derive(Debug, Clone)]
pub struct ImportedRows {
    /// User CSV path (for error messages).
    pub source_path: PathBuf,
    /// Typed data, in descriptor column order.
    pub batch: RecordBatch,
}

/// Read a CSV file into a [`RecordBatch`] typed by `descriptor`.
///
/// The returned batch has exactly the columns listed in
/// `descriptor.columns`, in descriptor order, regardless of the CSV
/// header order. Extra columns in the CSV are dropped.
pub fn read_csv_table(path: &Path, descriptor: &TableDescriptor) -> Result<ImportedRows> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(false)
        .from_path(path)
        .map_err(|e| Error::CsvParse {
            path: path.to_path_buf(),
            message: e.to_string(),
        })?;

    let header = reader
        .headers()
        .map_err(|e| Error::CsvParse {
            path: path.to_path_buf(),
            message: format!("header read failed: {e}"),
        })?
        .clone();

    // Map descriptor column index → CSV column index. Case-insensitive.
    let mut indices: Vec<usize> = Vec::with_capacity(descriptor.columns.len());
    for col in descriptor.columns {
        let csv_idx = header
            .iter()
            .position(|h| h.eq_ignore_ascii_case(col.name))
            .ok_or_else(|| Error::MissingColumn {
                path: path.to_path_buf(),
                column: col.name.to_string(),
            })?;
        indices.push(csv_idx);
    }

    // Collect rows: Vec<Vec<Option<String>>> where outer is rows,
    // inner is descriptor columns. Each cell is None if the CSV value
    // was empty or the literal `NULL`.
    let mut rows: Vec<Vec<Option<String>>> = Vec::new();
    for (row_idx, record) in reader.records().enumerate() {
        let record = record.map_err(|e| Error::CsvParse {
            path: path.to_path_buf(),
            message: format!("row {} read failed: {e}", row_idx + 2),
        })?;
        let mut row = Vec::with_capacity(descriptor.columns.len());
        for &csv_idx in &indices {
            let cell = record.get(csv_idx).unwrap_or("").trim();
            if cell.is_empty() || cell.eq_ignore_ascii_case("NULL") {
                row.push(None);
            } else {
                row.push(Some(cell.to_string()));
            }
        }
        rows.push(row);
    }

    let batch = build_record_batch(path, descriptor, &rows)?;
    Ok(ImportedRows {
        source_path: path.to_path_buf(),
        batch,
    })
}

/// Build the Arrow schema for a descriptor.
pub fn arrow_schema(descriptor: &TableDescriptor) -> SchemaRef {
    let fields: Vec<Field> = descriptor
        .columns
        .iter()
        .map(|c| Field::new(c.name, c.arrow_type(), true))
        .collect();
    Arc::new(ArrowSchema::new(fields))
}

/// Build a `RecordBatch` from typed Vec-of-Vec rows. Type-coercion
/// errors are reported with 1-based row indices that match the user's
/// CSV (header row is row 1, first data row is row 2).
fn build_record_batch(
    path: &Path,
    descriptor: &TableDescriptor,
    rows: &[Vec<Option<String>>],
) -> Result<RecordBatch> {
    let schema = arrow_schema(descriptor);
    let row_count = rows.len();
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(descriptor.columns.len());

    for (col_idx, col) in descriptor.columns.iter().enumerate() {
        let array: ArrayRef = match col.arrow_type() {
            DataType::Int64 => {
                let mut b = Int64Builder::with_capacity(row_count);
                for (row_idx, row) in rows.iter().enumerate() {
                    match &row[col_idx] {
                        None => b.append_null(),
                        Some(s) => {
                            let v = s.parse::<i64>().map_err(|e| Error::TypeCoercion {
                                path: path.to_path_buf(),
                                column: col.name.to_string(),
                                message: format!(
                                    "row {} value '{}' is not an integer: {}",
                                    row_idx + 2,
                                    s,
                                    e
                                ),
                            })?;
                            b.append_value(v);
                        }
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Float64 => {
                let mut b = Float64Builder::with_capacity(row_count);
                for (row_idx, row) in rows.iter().enumerate() {
                    match &row[col_idx] {
                        None => b.append_null(),
                        Some(s) => {
                            let v = s.parse::<f64>().map_err(|e| Error::TypeCoercion {
                                path: path.to_path_buf(),
                                column: col.name.to_string(),
                                message: format!(
                                    "row {} value '{}' is not a number: {}",
                                    row_idx + 2,
                                    s,
                                    e
                                ),
                            })?;
                            b.append_value(v);
                        }
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Boolean => {
                let mut b = BooleanBuilder::with_capacity(row_count);
                for (row_idx, row) in rows.iter().enumerate() {
                    match &row[col_idx] {
                        None => b.append_null(),
                        Some(s) => {
                            let v = match s.to_ascii_lowercase().as_str() {
                                "0" | "false" | "n" | "no" => false,
                                "1" | "true" | "y" | "yes" => true,
                                _ => {
                                    return Err(Error::TypeCoercion {
                                        path: path.to_path_buf(),
                                        column: col.name.to_string(),
                                        message: format!(
                                            "row {} value '{}' is not a boolean (expected 0/1, true/false, Y/N)",
                                            row_idx + 2,
                                            s
                                        ),
                                    })
                                }
                            };
                            b.append_value(v);
                        }
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Utf8 => {
                let mut b = StringBuilder::with_capacity(row_count, row_count * 16);
                for row in rows.iter() {
                    match &row[col_idx] {
                        None => b.append_null(),
                        Some(s) => b.append_value(s),
                    }
                }
                Arc::new(b.finish())
            }
            other => {
                return Err(Error::TypeCoercion {
                    path: path.to_path_buf(),
                    column: col.name.to_string(),
                    message: format!("unsupported Arrow type {other:?}"),
                })
            }
        };
        arrays.push(array);
    }

    RecordBatch::try_new(schema, arrays).map_err(Error::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::descriptor::ColumnDescriptor;
    use crate::filter::Filter;

    static AGE_DIST: TableDescriptor = TableDescriptor {
        name: "SourceTypeAgeDistribution",
        columns: &[
            ColumnDescriptor::new("sourceTypeID", Filter::SourceType),
            ColumnDescriptor::new("yearID", Filter::Year),
            ColumnDescriptor::new("ageID", Filter::Age),
            ColumnDescriptor::new("ageFraction", Filter::NonNegative),
        ],
        primary_key: &["sourceTypeID", "yearID", "ageID"],
    };

    fn write_temp_csv(contents: &str) -> tempfile::NamedTempFile {
        use std::io::Write;
        let mut f = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn reads_header_case_insensitively_and_keeps_descriptor_order() {
        let csv = "yearID,sourcetypeid,AGEID,ageFraction\n\
                   2020,21,0,0.05\n\
                   2020,21,1,0.04\n";
        let f = write_temp_csv(csv);
        let rows = read_csv_table(f.path(), &AGE_DIST).unwrap();
        // The descriptor lists sourceTypeID first, then yearID, then
        // ageID, then ageFraction. The CSV order is different.
        assert_eq!(rows.batch.num_rows(), 2);
        assert_eq!(rows.batch.num_columns(), 4);
        let schema = rows.batch.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec!["sourceTypeID", "yearID", "ageID", "ageFraction"]
        );
    }

    #[test]
    fn parses_integer_and_float_columns_to_arrow_types() {
        use arrow::array::{Float64Array, Int64Array};
        let csv = "sourceTypeID,yearID,ageID,ageFraction\n\
                   21,2020,0,0.05\n\
                   21,2020,1,0.04\n";
        let f = write_temp_csv(csv);
        let rows = read_csv_table(f.path(), &AGE_DIST).unwrap();
        let source_type = rows
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let age_fraction = rows
            .batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(source_type.value(0), 21);
        assert!((age_fraction.value(0) - 0.05).abs() < 1e-12);
        assert!((age_fraction.value(1) - 0.04).abs() < 1e-12);
    }

    #[test]
    fn empty_and_null_cells_become_arrow_nulls() {
        let csv = "sourceTypeID,yearID,ageID,ageFraction\n\
                   21,2020,0,\n\
                   21,2020,1,NULL\n\
                   21,2020,2,0.04\n";
        let f = write_temp_csv(csv);
        let rows = read_csv_table(f.path(), &AGE_DIST).unwrap();
        let frac = rows.batch.column(3);
        assert!(frac.is_null(0));
        assert!(frac.is_null(1));
        assert!(!frac.is_null(2));
    }

    #[test]
    fn missing_column_in_csv_errors() {
        let csv = "sourceTypeID,yearID,ageFraction\n21,2020,0.05\n";
        let f = write_temp_csv(csv);
        let err = read_csv_table(f.path(), &AGE_DIST).unwrap_err();
        match err {
            Error::MissingColumn { column, .. } => assert_eq!(column, "ageID"),
            other => panic!("expected MissingColumn, got {other:?}"),
        }
    }

    #[test]
    fn extra_columns_in_csv_are_ignored() {
        let csv = "sourceTypeID,yearID,ageID,ageFraction,salesGrowthFactor,note\n\
                   21,2020,0,0.05,0,first\n";
        let f = write_temp_csv(csv);
        let rows = read_csv_table(f.path(), &AGE_DIST).unwrap();
        assert_eq!(rows.batch.num_columns(), 4);
        assert_eq!(rows.batch.num_rows(), 1);
    }

    #[test]
    fn non_integer_value_in_int_column_surfaces_type_coercion_error() {
        let csv = "sourceTypeID,yearID,ageID,ageFraction\n\
                   not-a-number,2020,0,0.05\n";
        let f = write_temp_csv(csv);
        let err = read_csv_table(f.path(), &AGE_DIST).unwrap_err();
        match err {
            Error::TypeCoercion {
                column, message, ..
            } => {
                assert_eq!(column, "sourceTypeID");
                assert!(message.contains("row 2"));
            }
            other => panic!("expected TypeCoercion, got {other:?}"),
        }
    }
}

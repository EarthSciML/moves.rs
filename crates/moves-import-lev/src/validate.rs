//! Validate a parsed CSV against the LEV/NLEV schema and produce typed
//! rows ready for Parquet encoding.
//!
//! Validation rules:
//!
//! * Every column marked `required` in [`COLUMNS`] must be present in
//!   the header.
//! * Every header entry must match a [`COLUMNS`] entry — unknown columns
//!   are rejected rather than silently dropped, to catch typos.
//! * Cells in `required` columns must be non-empty in every row.
//! * Cells must parse as the column's declared type.
//! * Rate cells (any [`ColumnKind::Float`]) must be finite and
//!   non-negative.
//! * The four primary-key columns
//!   (`sourceBinID`, `polProcessID`, `opModeID`, `ageGroupID`) must
//!   form a unique tuple across all rows. Duplicates would silently
//!   overwrite on default-DB insert.

use std::collections::HashMap;

use crate::csv_reader::Csv;
use crate::error::{Error, Result};
use crate::schema::{column_index, Column, ColumnKind, COLUMNS};

/// One validated, typed row. Cells appear in [`COLUMNS`] canonical
/// order; `None` is SQL NULL (only allowed for non-required columns).
#[derive(Debug, Clone, PartialEq)]
pub struct TypedRow {
    pub line: usize,
    pub values: Vec<TypedValue>,
}

/// A typed cell value. Integer/Float/Null mirror the three Arrow output
/// flavors used by [`crate::schema::COLUMNS`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TypedValue {
    /// SQL NULL — only legal for non-required columns.
    Null,
    /// Signed integer parsed from [`ColumnKind::Integer`].
    Integer(i64),
    /// Finite, non-negative float parsed from [`ColumnKind::Float`].
    Float(f64),
}

/// Validate a parsed [`Csv`] against the LEV/NLEV schema. Returns the
/// rows in input order (preserving input order keeps Parquet byte-stable
/// when the same CSV is re-imported).
pub fn validate(csv: &Csv) -> Result<Vec<TypedRow>> {
    let header_map = build_header_map(csv)?;

    let pk_indices = primary_key_indices();
    let mut seen: HashMap<(i64, i64, i64, i64), usize> = HashMap::new();
    let mut typed_rows = Vec::with_capacity(csv.rows.len());

    for row in &csv.rows {
        let mut values: Vec<TypedValue> = vec![TypedValue::Null; COLUMNS.len()];

        for (header_pos, target) in header_map.iter().enumerate() {
            let raw = row.cells.get(header_pos).and_then(|c| c.as_deref());
            let column = COLUMNS[*target];
            values[*target] = parse_cell(&csv.path, row.line, &column, raw)?;
        }

        for (col_idx, column) in COLUMNS.iter().enumerate() {
            if column.required && matches!(values[col_idx], TypedValue::Null) {
                return Err(Error::MissingRequiredValue {
                    path: csv.path.clone(),
                    line: row.line,
                    column: column.name.to_string(),
                });
            }
        }

        let pk = (
            integer_at(&values, pk_indices[0]),
            integer_at(&values, pk_indices[1]),
            integer_at(&values, pk_indices[2]),
            integer_at(&values, pk_indices[3]),
        );
        if let Some(&prior_line) = seen.get(&pk) {
            return Err(Error::DuplicatePrimaryKey {
                path: csv.path.clone(),
                source_bin_id: pk.0,
                pol_process_id: pk.1,
                op_mode_id: pk.2,
                age_group_id: pk.3,
                first: prior_line,
                second: row.line,
            });
        }
        seen.insert(pk, row.line);

        typed_rows.push(TypedRow {
            line: row.line,
            values,
        });
    }

    Ok(typed_rows)
}

/// Map each header position to its [`COLUMNS`] index. Rejects unknown
/// columns and missing required columns up front.
fn build_header_map(csv: &Csv) -> Result<Vec<usize>> {
    let mut header_map: Vec<usize> = Vec::with_capacity(csv.header.len());
    let mut present: Vec<bool> = vec![false; COLUMNS.len()];

    for header_name in &csv.header {
        let idx = column_index(header_name).ok_or_else(|| Error::UnknownColumn {
            path: csv.path.clone(),
            column: header_name.clone(),
        })?;
        header_map.push(idx);
        present[idx] = true;
    }

    for (col_idx, column) in COLUMNS.iter().enumerate() {
        if column.required && !present[col_idx] {
            return Err(Error::MissingRequiredColumn {
                path: csv.path.clone(),
                column: column.name.to_string(),
            });
        }
    }

    Ok(header_map)
}

/// Indices into [`COLUMNS`] for the four PK columns, in PK order.
fn primary_key_indices() -> [usize; 4] {
    let mut out = [0usize; 4];
    let mut i = 0;
    for (idx, col) in COLUMNS.iter().enumerate() {
        if col.primary_key {
            out[i] = idx;
            i += 1;
        }
    }
    debug_assert_eq!(i, 4, "schema must declare four primary-key columns");
    out
}

fn integer_at(values: &[TypedValue], idx: usize) -> i64 {
    // Primary-key columns are required, so this is unreachable for
    // valid inputs that survived the `required` check.
    match values[idx] {
        TypedValue::Integer(v) => v,
        _ => unreachable!("primary-key cell at index {idx} is not an integer"),
    }
}

fn parse_cell(
    path: &std::path::Path,
    line: usize,
    column: &Column,
    raw: Option<&str>,
) -> Result<TypedValue> {
    let Some(text) = raw else {
        return Ok(TypedValue::Null);
    };

    match column.kind {
        ColumnKind::Integer => {
            let v = text.parse::<i64>().map_err(|_| Error::ParseCell {
                path: path.to_path_buf(),
                line,
                column: column.name.to_string(),
                expected_type: "integer",
                value: text.to_string(),
            })?;
            Ok(TypedValue::Integer(v))
        }
        ColumnKind::Float => {
            let v = text.parse::<f64>().map_err(|_| Error::ParseCell {
                path: path.to_path_buf(),
                line,
                column: column.name.to_string(),
                expected_type: "float",
                value: text.to_string(),
            })?;
            if !v.is_finite() {
                return Err(Error::NonFiniteRate {
                    path: path.to_path_buf(),
                    line,
                    column: column.name.to_string(),
                    value: v,
                });
            }
            if v < 0.0 {
                return Err(Error::NegativeRate {
                    path: path.to_path_buf(),
                    line,
                    column: column.name.to_string(),
                    value: v,
                });
            }
            Ok(TypedValue::Float(v))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csv_reader::parse;
    use std::path::Path;

    fn p() -> &'static Path {
        Path::new("input.csv")
    }

    fn full_csv(extra_rows: &[&str]) -> Vec<u8> {
        let header = "sourceBinID,polProcessID,opModeID,ageGroupID,meanBaseRate,meanBaseRateCV,meanBaseRateIM,meanBaseRateIMCV,dataSourceId\n";
        let mut out = header.as_bytes().to_vec();
        for r in extra_rows {
            out.extend_from_slice(r.as_bytes());
            out.push(b'\n');
        }
        out
    }

    #[test]
    fn validates_minimal_row() {
        // Only the five required columns.
        let body = b"sourceBinID,polProcessID,opModeID,ageGroupID,meanBaseRate\n\
                     1000,101,1,1,0.5\n";
        let csv = parse(p(), body).unwrap();
        let typed = validate(&csv).unwrap();
        assert_eq!(typed.len(), 1);
        assert_eq!(typed[0].values[0], TypedValue::Integer(1000));
        assert_eq!(typed[0].values[1], TypedValue::Integer(101));
        assert_eq!(typed[0].values[4], TypedValue::Float(0.5));
        // Optional columns absent from the header default to Null.
        assert_eq!(typed[0].values[5], TypedValue::Null);
        assert_eq!(typed[0].values[8], TypedValue::Null);
    }

    #[test]
    fn validates_full_row_with_optionals() {
        let body = full_csv(&["1000,101,1,1,0.5,0.05,0.6,0.06,7"]);
        let csv = parse(p(), &body).unwrap();
        let typed = validate(&csv).unwrap();
        assert_eq!(typed.len(), 1);
        assert_eq!(typed[0].values[5], TypedValue::Float(0.05));
        assert_eq!(typed[0].values[6], TypedValue::Float(0.6));
        assert_eq!(typed[0].values[7], TypedValue::Float(0.06));
        assert_eq!(typed[0].values[8], TypedValue::Integer(7));
    }

    #[test]
    fn header_can_be_in_any_order() {
        let body = b"meanBaseRate,ageGroupID,opModeID,polProcessID,sourceBinID\n\
                     0.5,1,1,101,1000\n";
        let csv = parse(p(), body).unwrap();
        let typed = validate(&csv).unwrap();
        assert_eq!(typed[0].values[0], TypedValue::Integer(1000));
        assert_eq!(typed[0].values[4], TypedValue::Float(0.5));
    }

    #[test]
    fn rejects_missing_required_column() {
        // Drop ageGroupID.
        let body = b"sourceBinID,polProcessID,opModeID,meanBaseRate\n\
                     1000,101,1,0.5\n";
        let csv = parse(p(), body).unwrap();
        let err = validate(&csv).unwrap_err();
        match err {
            Error::MissingRequiredColumn { column, .. } => assert_eq!(column, "ageGroupID"),
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn rejects_unknown_column() {
        let body = b"sourceBinID,polProcessID,opModeID,ageGroupID,meanBaseRate,extraColumn\n\
                     1000,101,1,1,0.5,0\n";
        let csv = parse(p(), body).unwrap();
        let err = validate(&csv).unwrap_err();
        match err {
            Error::UnknownColumn { column, .. } => assert_eq!(column, "extraColumn"),
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn rejects_blank_required_value() {
        let body = b"sourceBinID,polProcessID,opModeID,ageGroupID,meanBaseRate\n\
                     1000,101,1,,0.5\n";
        let csv = parse(p(), body).unwrap();
        let err = validate(&csv).unwrap_err();
        match err {
            Error::MissingRequiredValue { column, line, .. } => {
                assert_eq!(column, "ageGroupID");
                assert_eq!(line, 2);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn rejects_non_integer_id() {
        let body = b"sourceBinID,polProcessID,opModeID,ageGroupID,meanBaseRate\n\
                     1000,101,1.5,1,0.5\n";
        let csv = parse(p(), body).unwrap();
        let err = validate(&csv).unwrap_err();
        match err {
            Error::ParseCell {
                column,
                expected_type,
                value,
                ..
            } => {
                assert_eq!(column, "opModeID");
                assert_eq!(expected_type, "integer");
                assert_eq!(value, "1.5");
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn rejects_non_float_rate() {
        let body = b"sourceBinID,polProcessID,opModeID,ageGroupID,meanBaseRate\n\
                     1000,101,1,1,not-a-number\n";
        let csv = parse(p(), body).unwrap();
        let err = validate(&csv).unwrap_err();
        match err {
            Error::ParseCell { column, .. } => assert_eq!(column, "meanBaseRate"),
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn rejects_negative_rate() {
        let body = b"sourceBinID,polProcessID,opModeID,ageGroupID,meanBaseRate\n\
                     1000,101,1,1,-0.1\n";
        let csv = parse(p(), body).unwrap();
        let err = validate(&csv).unwrap_err();
        match err {
            Error::NegativeRate { value, column, .. } => {
                assert!(value < 0.0);
                assert_eq!(column, "meanBaseRate");
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn rejects_non_finite_rate() {
        let body = b"sourceBinID,polProcessID,opModeID,ageGroupID,meanBaseRate\n\
                     1000,101,1,1,nan\n";
        let csv = parse(p(), body).unwrap();
        let err = validate(&csv).unwrap_err();
        match err {
            Error::NonFiniteRate { column, .. } => assert_eq!(column, "meanBaseRate"),
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn rejects_negative_optional_rate() {
        let body = b"sourceBinID,polProcessID,opModeID,ageGroupID,meanBaseRate,meanBaseRateCV\n\
                     1000,101,1,1,0.5,-0.01\n";
        let csv = parse(p(), body).unwrap();
        let err = validate(&csv).unwrap_err();
        match err {
            Error::NegativeRate { column, .. } => assert_eq!(column, "meanBaseRateCV"),
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn rejects_duplicate_primary_key() {
        let body = b"sourceBinID,polProcessID,opModeID,ageGroupID,meanBaseRate\n\
                     1000,101,1,1,0.5\n\
                     1000,101,1,1,0.7\n";
        let csv = parse(p(), body).unwrap();
        let err = validate(&csv).unwrap_err();
        match err {
            Error::DuplicatePrimaryKey {
                source_bin_id,
                pol_process_id,
                op_mode_id,
                age_group_id,
                first,
                second,
                ..
            } => {
                assert_eq!(source_bin_id, 1000);
                assert_eq!(pol_process_id, 101);
                assert_eq!(op_mode_id, 1);
                assert_eq!(age_group_id, 1);
                assert_eq!(first, 2);
                assert_eq!(second, 3);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn distinct_keys_with_one_pk_diff_ok() {
        // Differing only in ageGroupID is a legitimate distinct key.
        let body = b"sourceBinID,polProcessID,opModeID,ageGroupID,meanBaseRate\n\
                     1000,101,1,1,0.5\n\
                     1000,101,1,2,0.7\n";
        let csv = parse(p(), body).unwrap();
        let typed = validate(&csv).unwrap();
        assert_eq!(typed.len(), 2);
    }

    #[test]
    fn empty_csv_yields_empty_result() {
        let body = b"sourceBinID,polProcessID,opModeID,ageGroupID,meanBaseRate\n";
        let csv = parse(p(), body).unwrap();
        let typed = validate(&csv).unwrap();
        assert!(typed.is_empty());
    }

    #[test]
    fn zero_rate_is_accepted() {
        // 0 is a legal rate (e.g., zero-emission technology).
        let body = b"sourceBinID,polProcessID,opModeID,ageGroupID,meanBaseRate\n\
                     1000,101,1,1,0\n";
        let csv = parse(p(), body).unwrap();
        let typed = validate(&csv).unwrap();
        assert_eq!(typed[0].values[4], TypedValue::Float(0.0));
    }
}

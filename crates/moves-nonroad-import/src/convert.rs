//! Per-table conversion: CSV → typed rows + validation.
//!
//! Each call processes one [`CsvFile`] against one [`TableSchema`]:
//!
//! 1. **Header check.** Column names must match the schema in declared
//!    order. Reordered or missing columns are rejected with a precise
//!    diagnostic — silently re-mapping by name would mask a malformed
//!    template.
//! 2. **Type coercion + per-cell rule.** Each cell is parsed to the
//!    declared `arrow_type` and then run through its [`Rule`]. Failures
//!    carry the source line, column name, and offending value.
//! 3. **Duplicate primary-key detection.** Rows are grouped by their PK
//!    tuple; the second occurrence is an error. The Nonroad CDB tables
//!    are small enough that an in-memory `HashSet` is fine.
//! 4. **Cross-row invariants.** Currently only [`CrossRowInvariant::FractionSum`]
//!    (sum-to-1 by group). The summation is applied after the duplicate
//!    check so an in-range duplicate doesn't double-count.
//!
//! The result is a `Vec<TypedRow>` ready for the parquet writer plus the
//! per-column metadata for the manifest.

use std::collections::HashSet;

use arrow::datatypes::DataType;

use crate::csv::CsvFile;
use crate::error::{Error, Result};
use crate::manifest::ColumnManifest;
use crate::parquet_writer::{Cell, TypedRow};
use crate::schema::{Column, CrossRowInvariant, Rule, TableSchema};

/// Successful conversion product: rows ready for [`encode_parquet`](crate::parquet_writer::encode_parquet),
/// plus the per-column manifest entries.
#[derive(Debug, Clone)]
pub struct Converted {
    pub rows: Vec<TypedRow>,
    pub columns: Vec<ColumnManifest>,
    pub primary_key: Vec<String>,
}

/// Convert one CSV file against one table schema. The CSV's `path` is
/// embedded in every diagnostic, so callers don't need to re-thread it.
pub fn convert_table(schema: &TableSchema, csv: &CsvFile) -> Result<Converted> {
    check_header(schema, csv)?;

    let mut typed_rows: Vec<TypedRow> = Vec::with_capacity(csv.rows.len());
    let mut seen_keys: HashSet<String> = HashSet::with_capacity(csv.rows.len());

    for row in &csv.rows {
        if row.cells.len() != schema.columns.len() {
            return Err(Error::RowWidth {
                path: csv.path.clone(),
                line: row.line,
                expected: schema.columns.len(),
                actual: row.cells.len(),
            });
        }
        let typed = coerce_row(schema, csv, row.line, &row.cells)?;
        let pk_key = primary_key_signature(schema, &typed);
        if !pk_key.is_empty() && !seen_keys.insert(pk_key.clone()) {
            return Err(Error::DuplicateKey {
                path: csv.path.clone(),
                line: row.line,
                table: schema.name.to_string(),
                key: pk_key,
            });
        }
        typed_rows.push(typed);
    }

    apply_cross_row_invariants(schema, csv, &typed_rows)?;

    let columns: Vec<ColumnManifest> = schema
        .columns
        .iter()
        .map(|c| ColumnManifest {
            name: c.name.to_string(),
            mysql_type: c.mysql_type.to_string(),
            arrow_type: format!("{:?}", c.arrow_type),
            primary_key: c.primary_key,
        })
        .collect();

    Ok(Converted {
        rows: typed_rows,
        columns,
        primary_key: schema
            .primary_key()
            .into_iter()
            .map(|s| s.to_string())
            .collect(),
    })
}

fn check_header(schema: &TableSchema, csv: &CsvFile) -> Result<()> {
    if csv.header.len() != schema.columns.len() {
        return Err(Error::HeaderShape {
            path: csv.path.clone(),
            table: schema.name.to_string(),
            expected: schema.columns.len(),
            actual: csv.header.len(),
        });
    }
    for (idx, expected) in schema.columns.iter().enumerate() {
        let got = csv.header[idx].trim();
        if got != expected.name {
            return Err(Error::MissingColumn {
                path: csv.path.clone(),
                column: format!(
                    "expected '{}' at position {} but found '{}'",
                    expected.name,
                    idx + 1,
                    got
                ),
            });
        }
    }
    Ok(())
}

fn coerce_row(
    schema: &TableSchema,
    csv: &CsvFile,
    line: usize,
    cells: &[Option<String>],
) -> Result<TypedRow> {
    let mut out = Vec::with_capacity(schema.columns.len());
    for (col, raw) in schema.columns.iter().zip(cells.iter()) {
        out.push(coerce_cell(csv, line, col, raw.as_deref())?);
    }
    Ok(out)
}

fn coerce_cell(csv: &CsvFile, line: usize, col: &Column, raw: Option<&str>) -> Result<Cell> {
    let stripped = raw.map(str::trim).filter(|s| !s.is_empty());
    match (stripped, &col.arrow_type) {
        (None, _) => {
            if col.required {
                return Err(Error::Parse {
                    path: csv.path.clone(),
                    line,
                    message: format!("column '{}' is required but cell is empty", col.name),
                });
            }
            Ok(Cell::Null)
        }
        (Some(s), DataType::Int64) => {
            let v = s.parse::<i64>().map_err(|e| Error::Parse {
                path: csv.path.clone(),
                line,
                message: format!("column '{}' expected integer, got '{}': {}", col.name, s, e),
            })?;
            check_int_rule(csv, line, col, v)?;
            Ok(Cell::Int(v))
        }
        (Some(s), DataType::Float64) => {
            let v = s.parse::<f64>().map_err(|e| Error::Parse {
                path: csv.path.clone(),
                line,
                message: format!("column '{}' expected number, got '{}': {}", col.name, s, e),
            })?;
            if !v.is_finite() {
                return Err(Error::Parse {
                    path: csv.path.clone(),
                    line,
                    message: format!("column '{}' is {} (must be finite)", col.name, v),
                });
            }
            check_float_rule(csv, line, col, v)?;
            Ok(Cell::Float(v))
        }
        (Some(s), DataType::Utf8) => Ok(Cell::Str(s.to_string())),
        (Some(_), other) => Err(Error::Internal {
            message: format!("column '{}' has unsupported arrow type {other:?}", col.name),
        }),
    }
}

fn check_int_rule(csv: &CsvFile, line: usize, col: &Column, v: i64) -> Result<()> {
    match col.rule {
        Rule::IntRange { lo, hi } => {
            if v < lo || v > hi {
                return Err(Error::Parse {
                    path: csv.path.clone(),
                    line,
                    message: format!(
                        "column '{}' value {} out of range [{}, {}]",
                        col.name, v, lo, hi
                    ),
                });
            }
        }
        Rule::FloatRange { lo, hi } => {
            let vf = v as f64;
            if vf < lo || vf > hi {
                return Err(Error::Parse {
                    path: csv.path.clone(),
                    line,
                    message: format!(
                        "column '{}' value {} out of range [{}, {}]",
                        col.name, v, lo, hi
                    ),
                });
            }
        }
        Rule::NonNegative => {
            if v < 0 {
                return Err(Error::Parse {
                    path: csv.path.clone(),
                    line,
                    message: format!(
                        "column '{}' value {} is negative (must be ≥ 0)",
                        col.name, v
                    ),
                });
            }
        }
        Rule::None => {}
    }
    Ok(())
}

fn check_float_rule(csv: &CsvFile, line: usize, col: &Column, v: f64) -> Result<()> {
    match col.rule {
        Rule::FloatRange { lo, hi } => {
            if v < lo || v > hi {
                return Err(Error::Parse {
                    path: csv.path.clone(),
                    line,
                    message: format!(
                        "column '{}' value {} out of range [{}, {}]",
                        col.name, v, lo, hi
                    ),
                });
            }
        }
        Rule::NonNegative => {
            if v < 0.0 {
                return Err(Error::Parse {
                    path: csv.path.clone(),
                    line,
                    message: format!(
                        "column '{}' value {} is negative (must be ≥ 0)",
                        col.name, v
                    ),
                });
            }
        }
        Rule::IntRange { .. } | Rule::None => {}
    }
    Ok(())
}

fn primary_key_signature(schema: &TableSchema, row: &TypedRow) -> String {
    let mut parts = Vec::new();
    for (col, cell) in schema.columns.iter().zip(row.iter()) {
        if !col.primary_key {
            continue;
        }
        parts.push(cell_signature(cell));
    }
    parts.join("|")
}

fn cell_signature(cell: &Cell) -> String {
    match cell {
        Cell::Null => "\0NULL".to_string(),
        Cell::Int(v) => format!("i:{v}"),
        Cell::Float(v) => format!("f:{}", v.to_bits()),
        Cell::Str(s) => format!("s:{s}"),
    }
}

fn apply_cross_row_invariants(
    schema: &TableSchema,
    csv: &CsvFile,
    rows: &[TypedRow],
) -> Result<()> {
    for invariant in schema.invariants {
        match invariant {
            CrossRowInvariant::FractionSum {
                fraction_column,
                group_columns,
                tolerance,
            } => check_fraction_sum(
                schema,
                csv,
                rows,
                fraction_column,
                group_columns,
                *tolerance,
            )?,
        }
    }
    Ok(())
}

fn check_fraction_sum(
    schema: &TableSchema,
    csv: &CsvFile,
    rows: &[TypedRow],
    fraction_column: &str,
    group_columns: &[&str],
    tolerance: f64,
) -> Result<()> {
    let fraction_idx = schema
        .column_index(fraction_column)
        .ok_or_else(|| Error::Internal {
            message: format!(
                "FractionSum invariant on '{}' references missing column '{}'",
                schema.name, fraction_column
            ),
        })?;
    let group_idx: Vec<usize> = group_columns
        .iter()
        .map(|g| {
            schema.column_index(g).ok_or_else(|| Error::Internal {
                message: format!(
                    "FractionSum invariant on '{}' references missing group column '{}'",
                    schema.name, g
                ),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    use std::collections::BTreeMap;
    let mut sums: BTreeMap<String, f64> = BTreeMap::new();
    for row in rows {
        let key = group_idx
            .iter()
            .map(|i| cell_signature(&row[*i]))
            .collect::<Vec<_>>()
            .join("|");
        let v = match &row[fraction_idx] {
            Cell::Float(v) => *v,
            Cell::Int(v) => *v as f64,
            Cell::Null => 0.0,
            Cell::Str(_) => {
                return Err(Error::Internal {
                    message: format!(
                        "FractionSum invariant on '{}' references non-numeric column '{}'",
                        schema.name, fraction_column
                    ),
                });
            }
        };
        *sums.entry(key).or_insert(0.0) += v;
    }
    for (key, sum) in sums {
        if (sum - 1.0).abs() > tolerance {
            return Err(Error::AllocationSum {
                path: csv.path.clone(),
                table: schema.name.to_string(),
                column: fraction_column.to_string(),
                key,
                actual: sum,
                expected: 1.0,
                tolerance,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csv::{CsvFile, CsvRow};
    use crate::schema::Column;
    use std::path::PathBuf;

    fn pop_schema() -> TableSchema {
        static COLS: &[Column] = &[
            Column {
                name: "sourceTypeID",
                mysql_type: "smallint(6)",
                arrow_type: DataType::Int64,
                primary_key: true,
                required: true,
                rule: Rule::None,
            },
            Column {
                name: "stateID",
                mysql_type: "smallint(6)",
                arrow_type: DataType::Int64,
                primary_key: true,
                required: true,
                rule: Rule::IntRange { lo: 1, hi: 99 },
            },
            Column {
                name: "population",
                mysql_type: "float",
                arrow_type: DataType::Float64,
                primary_key: false,
                required: true,
                rule: Rule::NonNegative,
            },
        ];
        static INVARIANTS: &[CrossRowInvariant] = &[];
        TableSchema {
            name: "nrbaseyearequippopulation",
            columns: COLS,
            invariants: INVARIANTS,
        }
    }

    fn csv(rows: Vec<Vec<Option<&str>>>) -> CsvFile {
        let header = vec!["sourceTypeID".into(), "stateID".into(), "population".into()];
        let rows: Vec<CsvRow> = rows
            .into_iter()
            .enumerate()
            .map(|(i, r)| CsvRow {
                line: i + 2,
                cells: r.into_iter().map(|c| c.map(|s| s.to_string())).collect(),
            })
            .collect();
        CsvFile {
            path: PathBuf::from("pop.csv"),
            header,
            rows,
        }
    }

    #[test]
    fn rejects_misordered_header() {
        let bad = CsvFile {
            path: PathBuf::from("pop.csv"),
            header: vec!["stateID".into(), "sourceTypeID".into(), "population".into()],
            rows: vec![],
        };
        let err = convert_table(&pop_schema(), &bad).unwrap_err();
        matches!(err, Error::MissingColumn { .. });
    }

    #[test]
    fn rejects_wrong_column_count() {
        let bad = CsvFile {
            path: PathBuf::from("pop.csv"),
            header: vec!["sourceTypeID".into(), "stateID".into()],
            rows: vec![],
        };
        let err = convert_table(&pop_schema(), &bad).unwrap_err();
        match err {
            Error::HeaderShape {
                expected, actual, ..
            } => {
                assert_eq!(expected, 3);
                assert_eq!(actual, 2);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn parses_typical_row() {
        let c = csv(vec![vec![Some("1"), Some("26"), Some("100.5")]]);
        let out = convert_table(&pop_schema(), &c).unwrap();
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0][0], Cell::Int(1));
        assert_eq!(out.rows[0][1], Cell::Int(26));
        assert_eq!(out.rows[0][2], Cell::Float(100.5));
    }

    #[test]
    fn rejects_negative_population() {
        let c = csv(vec![vec![Some("1"), Some("26"), Some("-1.0")]]);
        let err = convert_table(&pop_schema(), &c).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("negative")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn rejects_state_id_out_of_range() {
        let c = csv(vec![vec![Some("1"), Some("100"), Some("100.0")]]);
        let err = convert_table(&pop_schema(), &c).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("out of range")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn rejects_missing_pk() {
        let c = csv(vec![vec![None, Some("26"), Some("1.0")]]);
        let err = convert_table(&pop_schema(), &c).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("required")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn allows_null_in_non_required_column() {
        let header = vec!["sourceTypeID".into(), "stateID".into(), "population".into()];
        static COLS: &[Column] = &[
            Column {
                name: "sourceTypeID",
                mysql_type: "smallint(6)",
                arrow_type: DataType::Int64,
                primary_key: true,
                required: true,
                rule: Rule::None,
            },
            Column {
                name: "stateID",
                mysql_type: "smallint(6)",
                arrow_type: DataType::Int64,
                primary_key: true,
                required: true,
                rule: Rule::None,
            },
            // Nullable but range-checked when present — the common
            // "DEFAULT NULL with a business-rule range" pattern.
            Column {
                name: "population",
                mysql_type: "float",
                arrow_type: DataType::Float64,
                primary_key: false,
                required: false,
                rule: Rule::NonNegative,
            },
        ];
        let nullable = TableSchema {
            name: "nrbaseyearequippopulation",
            columns: COLS,
            invariants: &[],
        };
        let file = CsvFile {
            path: PathBuf::from("pop.csv"),
            header,
            rows: vec![
                CsvRow {
                    line: 2,
                    cells: vec![Some("1".into()), Some("26".into()), None],
                },
                CsvRow {
                    line: 3,
                    cells: vec![Some("2".into()), Some("26".into()), Some("100.0".into())],
                },
            ],
        };
        let out = convert_table(&nullable, &file).unwrap();
        assert_eq!(out.rows[0][2], Cell::Null);
        assert_eq!(out.rows[1][2], Cell::Float(100.0));
    }

    #[test]
    fn detects_duplicate_primary_key() {
        let c = csv(vec![
            vec![Some("1"), Some("26"), Some("1.0")],
            vec![Some("1"), Some("26"), Some("2.0")],
        ]);
        let err = convert_table(&pop_schema(), &c).unwrap_err();
        match err {
            Error::DuplicateKey { table, .. } => assert_eq!(table, "nrbaseyearequippopulation"),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn fraction_sum_invariant_detects_drift() {
        static COLS: &[Column] = &[
            Column {
                name: "groupID",
                mysql_type: "smallint(6)",
                arrow_type: DataType::Int64,
                primary_key: true,
                required: true,
                rule: Rule::None,
            },
            Column {
                name: "monthID",
                mysql_type: "smallint(6)",
                arrow_type: DataType::Int64,
                primary_key: true,
                required: true,
                rule: Rule::IntRange { lo: 1, hi: 12 },
            },
            Column {
                name: "frac",
                mysql_type: "float",
                arrow_type: DataType::Float64,
                primary_key: false,
                required: true,
                rule: Rule::FloatRange { lo: 0.0, hi: 1.0 },
            },
        ];
        static INV: &[CrossRowInvariant] = &[CrossRowInvariant::FractionSum {
            fraction_column: "frac",
            group_columns: &["groupID"],
            tolerance: 1e-3,
        }];
        let s = TableSchema {
            name: "test",
            columns: COLS,
            invariants: INV,
        };
        let file = CsvFile {
            path: PathBuf::from("t.csv"),
            header: vec!["groupID".into(), "monthID".into(), "frac".into()],
            rows: vec![
                CsvRow {
                    line: 2,
                    cells: vec![Some("1".into()), Some("1".into()), Some("0.4".into())],
                },
                CsvRow {
                    line: 3,
                    cells: vec![Some("1".into()), Some("2".into()), Some("0.4".into())],
                },
            ],
        };
        let err = convert_table(&s, &file).unwrap_err();
        match err {
            Error::AllocationSum { actual, .. } => assert!((actual - 0.8).abs() < 1e-9),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn fraction_sum_invariant_passes_within_tolerance() {
        static COLS: &[Column] = &[
            Column {
                name: "groupID",
                mysql_type: "smallint(6)",
                arrow_type: DataType::Int64,
                primary_key: true,
                required: true,
                rule: Rule::None,
            },
            Column {
                name: "monthID",
                mysql_type: "smallint(6)",
                arrow_type: DataType::Int64,
                primary_key: true,
                required: true,
                rule: Rule::IntRange { lo: 1, hi: 12 },
            },
            Column {
                name: "frac",
                mysql_type: "float",
                arrow_type: DataType::Float64,
                primary_key: false,
                required: true,
                rule: Rule::FloatRange { lo: 0.0, hi: 1.0 },
            },
        ];
        static INV: &[CrossRowInvariant] = &[CrossRowInvariant::FractionSum {
            fraction_column: "frac",
            group_columns: &["groupID"],
            tolerance: 1e-3,
        }];
        let s = TableSchema {
            name: "test",
            columns: COLS,
            invariants: INV,
        };
        // Two groups each summing to exactly 1.0.
        let rows = vec![
            (1, 1, "0.5"),
            (1, 2, "0.5"),
            (2, 1, "0.999"),
            (2, 2, "0.001"),
        ];
        let file = CsvFile {
            path: PathBuf::from("t.csv"),
            header: vec!["groupID".into(), "monthID".into(), "frac".into()],
            rows: rows
                .into_iter()
                .enumerate()
                .map(|(i, (g, m, f))| CsvRow {
                    line: i + 2,
                    cells: vec![
                        Some(g.to_string()),
                        Some(m.to_string()),
                        Some(f.to_string()),
                    ],
                })
                .collect(),
        };
        convert_table(&s, &file).unwrap();
    }
}

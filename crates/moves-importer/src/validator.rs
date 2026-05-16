//! Run per-column [`Filter`] checks plus per-importer
//! cross-row hooks against a typed [`RecordBatch`].
//!
//! ## Error severity
//!
//! MOVES' Java importers split their SQL output by message prefix:
//! `ERROR: ...` is fatal, anything else is informational. We carry the
//! same notion as [`Severity::Error`] (fatal) vs [`Severity::Warning`]
//! (informational). A caller treats `Error` as "do not write Parquet"
//! and forwards both severities to the user.
//!
//! ## Foreign-key validation
//!
//! When a column carries a foreign-key filter (e.g.
//! [`Filter::SourceType`]), the validator
//! pulls the decode-table id column from a
//! [`moves_data_default::DefaultDb`] handle on the
//! [`ValidationContext`]. The set of valid ids is cached on the context
//! after the first lookup so cross-table validation across an importer
//! with multiple tables shares the same fetch.
//!
//! [`RecordBatch`]: arrow::record_batch::RecordBatch

use std::collections::HashMap;
use std::path::PathBuf;

use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;

use crate::descriptor::TableDescriptor;
use crate::error::Result;
use crate::filter::Filter;

/// Fatal vs. informational.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    /// Block the import; output Parquet must not be written.
    Error,
    /// Surface to the user but don't block.
    Warning,
}

/// One validation finding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationMessage {
    pub severity: Severity,
    /// Table name (matches the descriptor's `name`).
    pub table: &'static str,
    /// Column name. `None` for cross-row / cross-table messages.
    pub column: Option<&'static str>,
    /// 1-based row in the user's CSV (header is row 1, first data row is row 2).
    /// `None` for whole-column or cross-row messages.
    pub row: Option<usize>,
    /// Human-readable description.
    pub message: String,
}

impl ValidationMessage {
    /// Construct an error.
    pub fn error(
        table: &'static str,
        column: Option<&'static str>,
        row: Option<usize>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            severity: Severity::Error,
            table,
            column,
            row,
            message: message.into(),
        }
    }

    /// Construct a warning.
    pub fn warning(
        table: &'static str,
        column: Option<&'static str>,
        row: Option<usize>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            severity: Severity::Warning,
            table,
            column,
            row,
            message: message.into(),
        }
    }

    /// True if [`Self::severity`] is [`Severity::Error`].
    pub fn is_error(&self) -> bool {
        matches!(self.severity, Severity::Error)
    }
}

/// One read-and-typed table ready for validation / output.
///
/// Created by the reader, carried through the validator, fed to the
/// writer. The descriptor reference is kept so the validator can match
/// columns by name and the writer can sort by primary key without an
/// extra lookup.
#[derive(Debug, Clone)]
pub struct ImportedTable<'a> {
    pub descriptor: &'a TableDescriptor,
    pub source_path: PathBuf,
    pub batch: RecordBatch,
}

impl<'a> ImportedTable<'a> {
    pub fn new(descriptor: &'a TableDescriptor, source_path: PathBuf, batch: RecordBatch) -> Self {
        Self {
            descriptor,
            source_path,
            batch,
        }
    }
}

/// Carries cross-table resources used during validation.
///
/// Holds an optional [`moves_data_default::DefaultDb`] handle for
/// foreign-key lookups. When the handle is absent, FK validators emit
/// a warning rather than an error — useful for unit-testing the
/// numeric-range and cross-row paths in isolation. The full importer
/// CLI in `moves-cli` always wires a real `DefaultDb`.
pub struct ValidationContext<'a> {
    default_db: Option<&'a moves_data_default::DefaultDb>,
    /// Cached id sets: `decode_table_name → set of valid ids`.
    /// `Vec` is cheap to construct and we use it for membership via a
    /// linear scan — decode tables are small (<200 rows for the ones
    /// we touch), so a `HashSet` would be overkill.
    cache: std::cell::RefCell<HashMap<&'static str, Vec<i64>>>,
}

impl<'a> ValidationContext<'a> {
    /// Build a context bound to a default-DB handle. The handle must
    /// be alive for the lifetime of the context.
    pub fn new(default_db: &'a moves_data_default::DefaultDb) -> Self {
        Self {
            default_db: Some(default_db),
            cache: std::cell::RefCell::new(HashMap::new()),
        }
    }

    /// Build a context with no default-DB handle — FK filters degrade
    /// to warnings. Intended for unit tests that only exercise
    /// numeric-range or cross-row validation.
    pub fn without_default_db() -> Self {
        Self {
            default_db: None,
            cache: std::cell::RefCell::new(HashMap::new()),
        }
    }

    /// Fetch (and cache) the set of valid ids for a decode-table /
    /// column pair. Returns `Ok(None)` if no default-DB handle is
    /// attached.
    fn decode_table_ids(&self, table: &'static str, column: &str) -> Result<Option<Vec<i64>>> {
        if let Some(cached) = self.cache.borrow().get(table) {
            return Ok(Some(cached.clone()));
        }
        let Some(db) = self.default_db else {
            return Ok(None);
        };
        let lf = db.scan(table, &moves_data_default::TableFilter::new())?;
        let df = lf.select([polars::prelude::col(column)]).collect()?;
        let series = df.column(column)?.as_materialized_series();
        let ids = series_to_i64_vec(series)?;
        self.cache.borrow_mut().insert(table, ids.clone());
        Ok(Some(ids))
    }
}

fn series_to_i64_vec(series: &polars::prelude::Series) -> Result<Vec<i64>> {
    // The `moves-default-db-convert` pipeline widens every MariaDB
    // integer flavor (`tinyint`, `smallint`, `int`, `bigint`) to
    // `Int64` per `crates/moves-default-db-convert/src/types.rs`. The
    // `moves-data-default` reader preserves that. So decode-table id
    // columns are always `Int64` here; if Polars reports otherwise the
    // pipeline has drifted and the validator should surface the
    // mismatch rather than silently truncate.
    let ca = series.i64().map_err(|e| crate::Error::DefaultDb {
        table: series.name().to_string(),
        message: format!(
            "expected Int64 id column (default-DB convention), got dtype {:?}: {e}",
            series.dtype()
        ),
    })?;
    Ok(ca.into_iter().flatten().collect())
}

/// Run per-column filter checks for one imported table.
///
/// Per-importer cross-row checks (allocation sums, coverage) are
/// dispatched separately via [`crate::Importer::validate_imported`].
pub fn validate_table(
    imported: &ImportedTable<'_>,
    ctx: &ValidationContext<'_>,
) -> Result<Vec<ValidationMessage>> {
    let mut messages = Vec::new();
    let descriptor = imported.descriptor;
    let batch = &imported.batch;

    for col_desc in descriptor.columns {
        let arr = batch
            .column_by_name(col_desc.name)
            .expect("reader populates every descriptor column");
        // 1. Null check.
        check_nulls(
            descriptor.name,
            col_desc.name,
            &col_desc.filter,
            arr,
            &mut messages,
        );
        // 2. Numeric range check (skipped for non-numeric filters).
        check_numeric_range(
            descriptor.name,
            col_desc.name,
            &col_desc.filter,
            arr,
            &mut messages,
        );
        // 3. RoadType narrowing (specific to RoadTypeNotOffNetwork).
        check_road_type_narrowing(
            descriptor.name,
            col_desc.name,
            &col_desc.filter,
            arr,
            &mut messages,
        );
        // 4. Year-range check (1990-2060 per SourceTypePopulationImporter.sql).
        check_year_range(
            descriptor.name,
            col_desc.name,
            &col_desc.filter,
            arr,
            &mut messages,
        );
        // 5. Model-year range check (1950-2060 per ImporterManager FILTER_MODELYEARID).
        check_model_year_range(
            descriptor.name,
            col_desc.name,
            &col_desc.filter,
            arr,
            &mut messages,
        );
        // 6. Foreign-key membership.
        if let Some(decode_table) = col_desc.filter.decode_table() {
            if let Some(decode_column) = col_desc.filter.decode_column() {
                let ids = ctx.decode_table_ids(decode_table, decode_column)?;
                match ids {
                    Some(ids) => check_fk(
                        descriptor.name,
                        col_desc.name,
                        decode_table,
                        &ids,
                        arr,
                        &mut messages,
                    ),
                    None => messages.push(ValidationMessage::warning(
                        descriptor.name,
                        Some(col_desc.name),
                        None,
                        format!(
                            "no default-DB attached; foreign-key check against {decode_table} skipped"
                        ),
                    )),
                }
            }
        }
        // 7. YesNo flag check.
        check_yesno(
            descriptor.name,
            col_desc.name,
            &col_desc.filter,
            arr,
            &mut messages,
        );
    }

    Ok(messages)
}

fn check_nulls(
    table: &'static str,
    column: &'static str,
    filter: &Filter,
    arr: &dyn Array,
    out: &mut Vec<ValidationMessage>,
) {
    if filter.nullable() {
        return;
    }
    for row in 0..arr.len() {
        if arr.is_null(row) {
            out.push(ValidationMessage::error(
                table,
                Some(column),
                Some(row + 2),
                format!("required column '{column}' is missing"),
            ));
        }
    }
}

fn check_numeric_range(
    table: &'static str,
    column: &'static str,
    filter: &Filter,
    arr: &dyn Array,
    out: &mut Vec<ValidationMessage>,
) {
    let min = filter.numeric_min();
    let max = filter.numeric_max();
    if min.is_none() && max.is_none() {
        return;
    }
    let arr = match arr.as_any().downcast_ref::<Float64Array>() {
        Some(a) => a,
        None => return,
    };
    for row in 0..arr.len() {
        if arr.is_null(row) {
            continue;
        }
        let v = arr.value(row);
        if v.is_nan() {
            out.push(ValidationMessage::error(
                table,
                Some(column),
                Some(row + 2),
                format!("column '{column}' value is NaN"),
            ));
            continue;
        }
        if let Some(lo) = min {
            if v < lo {
                out.push(ValidationMessage::error(
                    table,
                    Some(column),
                    Some(row + 2),
                    format!("column '{column}' value {v} is below minimum {lo}"),
                ));
            }
        }
        if let Some(hi) = max {
            if v > hi {
                out.push(ValidationMessage::error(
                    table,
                    Some(column),
                    Some(row + 2),
                    format!("column '{column}' value {v} exceeds maximum {hi}"),
                ));
            }
        }
    }
}

fn check_road_type_narrowing(
    table: &'static str,
    column: &'static str,
    filter: &Filter,
    arr: &dyn Array,
    out: &mut Vec<ValidationMessage>,
) {
    if !matches!(filter, Filter::RoadTypeNotOffNetwork) {
        return;
    }
    let arr = match arr.as_any().downcast_ref::<Int64Array>() {
        Some(a) => a,
        None => return,
    };
    for row in 0..arr.len() {
        if arr.is_null(row) {
            continue;
        }
        let v = arr.value(row);
        // Off-network is roadTypeID 1; valid not-off-network are 2-5.
        if v == 1 {
            out.push(ValidationMessage::error(
                table,
                Some(column),
                Some(row + 2),
                "roadTypeID 1 (off-network) is not allowed here",
            ));
        }
    }
}

fn check_year_range(
    table: &'static str,
    column: &'static str,
    filter: &Filter,
    arr: &dyn Array,
    out: &mut Vec<ValidationMessage>,
) {
    if !matches!(filter, Filter::Year) {
        return;
    }
    let arr = match arr.as_any().downcast_ref::<Int64Array>() {
        Some(a) => a,
        None => return,
    };
    for row in 0..arr.len() {
        if arr.is_null(row) {
            continue;
        }
        let v = arr.value(row);
        if !(1990..=2060).contains(&v) {
            out.push(ValidationMessage::error(
                table,
                Some(column),
                Some(row + 2),
                format!("year {v} is outside the MOVES-supported range 1990-2060"),
            ));
        }
    }
}

fn check_model_year_range(
    table: &'static str,
    column: &'static str,
    filter: &Filter,
    arr: &dyn Array,
    out: &mut Vec<ValidationMessage>,
) {
    if !matches!(filter, Filter::ModelYear) {
        return;
    }
    let arr = match arr.as_any().downcast_ref::<Int64Array>() {
        Some(a) => a,
        None => return,
    };
    for row in 0..arr.len() {
        if arr.is_null(row) {
            continue;
        }
        let v = arr.value(row);
        if !(1950..=2060).contains(&v) {
            out.push(ValidationMessage::error(
                table,
                Some(column),
                Some(row + 2),
                format!("model year {v} is outside the MOVES-supported range 1950-2060"),
            ));
        }
    }
}

fn check_fk(
    table: &'static str,
    column: &'static str,
    decode_table: &str,
    ids: &[i64],
    arr: &dyn Array,
    out: &mut Vec<ValidationMessage>,
) {
    let arr = match arr.as_any().downcast_ref::<Int64Array>() {
        Some(a) => a,
        None => return,
    };
    for row in 0..arr.len() {
        if arr.is_null(row) {
            continue;
        }
        let v = arr.value(row);
        if !ids.contains(&v) {
            out.push(ValidationMessage::error(
                table,
                Some(column),
                Some(row + 2),
                format!("column '{column}' value {v} is not present in {decode_table}"),
            ));
        }
    }
}

fn check_yesno(
    table: &'static str,
    column: &'static str,
    filter: &Filter,
    arr: &dyn Array,
    out: &mut Vec<ValidationMessage>,
) {
    if !matches!(filter, Filter::YesNo) {
        return;
    }
    let arr = match arr.as_any().downcast_ref::<StringArray>() {
        Some(a) => a,
        None => return,
    };
    for row in 0..arr.len() {
        if arr.is_null(row) {
            continue;
        }
        let v = arr.value(row);
        if !matches!(v.to_ascii_uppercase().as_str(), "Y" | "N") {
            out.push(ValidationMessage::error(
                table,
                Some(column),
                Some(row + 2),
                format!("column '{column}' value '{v}' is not Y or N"),
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::descriptor::ColumnDescriptor;
    use crate::reader::read_csv_table;
    use std::io::Write;

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

    fn write_temp(contents: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    fn import(csv: &str) -> ImportedTable<'static> {
        let f = write_temp(csv);
        let rows = read_csv_table(f.path(), &AGE_DIST).unwrap();
        ImportedTable::new(&AGE_DIST, rows.source_path, rows.batch)
    }

    #[test]
    fn non_negative_filter_rejects_negative_value() {
        let t = import(
            "sourceTypeID,yearID,ageID,ageFraction\n\
             21,2020,0,-0.01\n\
             21,2020,1,0.04\n",
        );
        let ctx = ValidationContext::without_default_db();
        let msgs = validate_table(&t, &ctx).unwrap();
        let errors: Vec<_> = msgs.iter().filter(|m| m.is_error()).collect();
        assert!(errors
            .iter()
            .any(|m| m.column == Some("ageFraction") && m.row == Some(2)));
    }

    #[test]
    fn year_outside_1990_2060_is_an_error() {
        let t = import(
            "sourceTypeID,yearID,ageID,ageFraction\n\
             21,1980,0,0.05\n\
             21,2061,1,0.04\n\
             21,2020,2,0.03\n",
        );
        let ctx = ValidationContext::without_default_db();
        let msgs = validate_table(&t, &ctx).unwrap();
        let year_errors: Vec<_> = msgs
            .iter()
            .filter(|m| m.column == Some("yearID") && m.is_error())
            .collect();
        assert_eq!(year_errors.len(), 2);
    }

    #[test]
    fn missing_required_column_is_an_error_per_row() {
        let t = import(
            "sourceTypeID,yearID,ageID,ageFraction\n\
             21,2020,0,\n\
             21,2020,1,0.04\n",
        );
        let ctx = ValidationContext::without_default_db();
        let msgs = validate_table(&t, &ctx).unwrap();
        assert!(msgs
            .iter()
            .any(|m| m.column == Some("ageFraction") && m.row == Some(2) && m.is_error()));
    }

    #[test]
    fn fk_filter_warns_without_default_db() {
        let t = import(
            "sourceTypeID,yearID,ageID,ageFraction\n\
             21,2020,0,0.05\n",
        );
        let ctx = ValidationContext::without_default_db();
        let msgs = validate_table(&t, &ctx).unwrap();
        // Three FK columns (sourceTypeID, yearID, ageID) — each emits
        // one column-level warning.
        let warnings: Vec<_> = msgs
            .iter()
            .filter(|m| matches!(m.severity, Severity::Warning))
            .collect();
        assert_eq!(warnings.len(), 3);
        assert!(warnings.iter().all(|m| m.row.is_none()));
    }
}

//! `moves-importer-validation` — the MOVES importer validation suite
//! (Phase 4 Task 88).
//!
//! ## What this crate is for
//!
//! Phase 4 ports five MOVES input-database importers to Rust:
//!
//! | Crate                   | Importer                       | Task |
//! |-------------------------|--------------------------------|------|
//! | `moves-importer-county` | County Database (CDB)          | 83   |
//! | `moves-importer-pdb`    | Project Database (PDB)         | 84   |
//! | `moves-nonroad-import`  | Nonroad input database         | 85   |
//! | `moves-avft`            | Alternative Vehicle Fuel Tech  | 86   |
//! | `moves-import-lev`      | LEV / NLEV alternative rates   | 87   |
//!
//! Task 88 closes the loop: it *validates* those importers by running
//! them against representative user source files and comparing the
//! resulting Parquet against the tables canonical MOVES loads into
//! MariaDB for the same inputs. A difference is a candidate importer
//! bug.
//!
//! ## How the comparison works
//!
//! Canonical MOVES loads a user CDB/PDB into a MariaDB scratch database;
//! the Phase 0 capture pipeline (`moves-fixture-capture`) dumps every
//! such table into a snapshot as `db__<database>__<table>` — see
//! `characterization/snapshots/README.md`. This crate:
//!
//! 1. Runs a Rust importer on the user source files.
//! 2. Normalizes the importer's Parquet output into a
//!    [`moves_snapshot::Table`] — the *same* normalization the canonical
//!    snapshot applies (rows sorted by the natural key, floats rounded
//!    to a fixed-decimal string). [`parquet_to_table`] does this.
//! 3. Diffs the normalized importer table against the canonical
//!    `db__…` table. [`compare_importer_output`] wraps
//!    [`moves_snapshot::diff_snapshots`] and classifies the result into
//!    a [`ComparisonReport`].
//!
//! ## Snapshot gating
//!
//! The canonical-MOVES snapshots are produced on an HPC compute node
//! (Apptainer + the patched MOVES SIF); they are not committed to the
//! repository. When a fixture's snapshot is absent, the comparison
//! reports [`ComparisonReport::canonical_missing`] rather than failing —
//! the same way the `fixture-suite-weekly` workflow skips fixtures with
//! no committed baseline.
//!
//! The `tests/` of this crate therefore run in two modes:
//!
//! * **Always (CI):** run each importer against the committed fixtures
//!   under `fixtures/`, normalize the output, and verify it is a
//!   well-formed, snapshot-comparable table. The comparison harness
//!   itself is unit-tested here with synthetic canonical data.
//! * **When snapshots are present:** additionally diff importer output
//!   against the canonical `db__…` tables and fail on genuine drift.
//!
//! See this crate's `README.md` for the operator procedure that
//! produces the canonical snapshots the gated tests consume.

use std::path::{Path, PathBuf};

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use moves_snapshot::{
    diff_snapshots, ColumnKind, DiffOptions, Snapshot, Table, TableBuilder, Value,
};

// Re-exported so callers can pattern-match diff detail without a direct
// `moves-snapshot` dependency.
pub use moves_snapshot::{RowDiff, SchemaDiff};

/// Errors raised while normalizing or comparing importer output.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("reading {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("decoding importer Parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("decoding importer Parquet: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("snapshot: {0}")]
    Snapshot(#[from] moves_snapshot::Error),

    #[error("table {table:?}: column {column:?} has unsupported Arrow type {dtype}")]
    UnsupportedColumnType {
        table: String,
        column: String,
        dtype: String,
    },
}

/// Result alias for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Normalize importer-produced Parquet into a [`moves_snapshot::Table`].
///
/// `name` becomes the table's name (use [`canonical_table_name`] when the
/// table will be diffed against a canonical snapshot). `natural_key` lists
/// the primary-key columns, in order — rows are sorted by them, exactly as
/// the canonical snapshot writer sorts. Every `natural_key` entry must name
/// a column present in the Parquet schema.
///
/// The importer crates emit `Int64`, `Float64`, `Utf8`, and `Boolean`
/// Arrow columns; any other column type is rejected with
/// [`Error::UnsupportedColumnType`]. `Float64` cells are rounded to the
/// snapshot format's fixed-decimal precision, so a normalized importer
/// table compares cell-for-cell with a canonical snapshot table.
pub fn parquet_to_table(name: &str, parquet: &[u8], natural_key: &[&str]) -> Result<Table> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(parquet))?;
    let schema = builder.schema().clone();
    let reader = builder.build()?;
    let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<_, _>>()?;

    let mut columns: Vec<(String, ColumnKind)> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        columns.push((
            field.name().clone(),
            arrow_kind(name, field.name(), field.data_type())?,
        ));
    }

    let mut builder =
        TableBuilder::new(name, columns)?.with_natural_key(natural_key.iter().copied())?;
    for batch in &batches {
        let value_columns = batch_to_value_columns(name, batch)?;
        for row in 0..batch.num_rows() {
            builder.push_row(value_columns.iter().map(|col| col[row].clone()))?;
        }
    }
    Ok(builder.build()?)
}

/// [`parquet_to_table`] reading the Parquet from a file on disk.
pub fn read_importer_table(name: &str, path: &Path, natural_key: &[&str]) -> Result<Table> {
    let bytes = std::fs::read(path).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    parquet_to_table(name, &bytes, natural_key)
}

/// Map an Arrow data type to a snapshot [`ColumnKind`].
fn arrow_kind(table: &str, column: &str, dtype: &DataType) -> Result<ColumnKind> {
    match dtype {
        DataType::Int64 => Ok(ColumnKind::Int64),
        DataType::Float64 => Ok(ColumnKind::Float64),
        DataType::Utf8 => Ok(ColumnKind::Utf8),
        DataType::Boolean => Ok(ColumnKind::Boolean),
        other => Err(Error::UnsupportedColumnType {
            table: table.to_string(),
            column: column.to_string(),
            dtype: format!("{other:?}"),
        }),
    }
}

/// Extract a [`RecordBatch`] into column-major [`Value`] vectors so the
/// row-oriented [`TableBuilder`] can be fed without re-downcasting per cell.
fn batch_to_value_columns(table: &str, batch: &RecordBatch) -> Result<Vec<Vec<Value>>> {
    let mut out = Vec::with_capacity(batch.num_columns());
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        let array = batch.column(idx);
        let len = array.len();
        let mut column = Vec::with_capacity(len);
        match array.data_type() {
            DataType::Int64 => {
                let typed = downcast::<Int64Array>(array);
                for i in 0..len {
                    column.push(if typed.is_null(i) {
                        Value::Null
                    } else {
                        Value::Int64(typed.value(i))
                    });
                }
            }
            DataType::Float64 => {
                let typed = downcast::<Float64Array>(array);
                for i in 0..len {
                    column.push(if typed.is_null(i) {
                        Value::Null
                    } else {
                        Value::Float64(typed.value(i))
                    });
                }
            }
            DataType::Utf8 => {
                let typed = downcast::<StringArray>(array);
                for i in 0..len {
                    column.push(if typed.is_null(i) {
                        Value::Null
                    } else {
                        Value::Utf8(typed.value(i).to_string())
                    });
                }
            }
            DataType::Boolean => {
                let typed = downcast::<BooleanArray>(array);
                for i in 0..len {
                    column.push(if typed.is_null(i) {
                        Value::Null
                    } else {
                        Value::Boolean(typed.value(i))
                    });
                }
            }
            other => {
                return Err(Error::UnsupportedColumnType {
                    table: table.to_string(),
                    column: field.name().clone(),
                    dtype: format!("{other:?}"),
                });
            }
        }
        out.push(column);
    }
    Ok(out)
}

fn downcast<T: 'static>(array: &dyn Array) -> &T {
    array
        .as_any()
        .downcast_ref::<T>()
        .expect("array data type was matched immediately above")
}

/// The snapshot table id for a MariaDB table loaded by canonical MOVES.
///
/// Canonical MOVES captures every non-system database table as
/// `db__<database>__<table>`, lower-cased — see
/// `characterization/snapshots/README.md`. For a project-scale run the
/// `<database>` is the run's `scale_input_database` (recorded in the
/// fixture's `provenance.json`).
pub fn canonical_table_name(database: &str, table: &str) -> String {
    format!("db__{}__{}", database.to_lowercase(), table.to_lowercase())
}

/// Find the `db__<database>__<table>` entry in a snapshot for a MOVES
/// table, regardless of which scratch database MOVES loaded it into.
///
/// A scale-county / scale-project snapshot has exactly one scale-input
/// database, so matching on the `__<table>` suffix uniquely identifies
/// the captured table without having to parse `provenance.json`.
pub fn find_canonical_table(snapshot: &Snapshot, table: &str) -> Option<String> {
    let suffix = format!("__{}", table.to_lowercase());
    snapshot
        .table_names()
        .find(|name| name.starts_with("db__") && name.ends_with(&suffix))
        .map(str::to_string)
}

/// Locate the repository's `characterization/` directory by walking up
/// from this crate. Returns `None` if the crate has been vendored away
/// from the moves.rs tree.
pub fn characterization_dir() -> Option<PathBuf> {
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    loop {
        let candidate = dir.join("characterization");
        if candidate.is_dir() {
            return Some(candidate);
        }
        if !dir.pop() {
            return None;
        }
    }
}

/// Load the canonical-MOVES snapshot named `snapshot`, if it has been
/// captured under `characterization/snapshots/`.
///
/// Returns `Ok(None)` when the snapshot directory (or its `manifest.json`)
/// is absent — the capture suite runs on HPC and its output is not
/// committed (see `characterization/snapshots/README.md` and this crate's
/// `README.md`).
pub fn load_canonical_snapshot(snapshot: &str) -> Result<Option<Snapshot>> {
    let Some(root) = characterization_dir() else {
        return Ok(None);
    };
    let dir = root.join("snapshots").join(snapshot);
    if !dir.join("manifest.json").is_file() {
        return Ok(None);
    }
    Ok(Some(Snapshot::load(&dir)?))
}

/// Outcome of comparing one importer-produced table against canonical MOVES.
///
/// The report separates *genuine* importer bugs from differences that are
/// expected by design: a Rust importer legitimately omits columns that
/// canonical MOVES synthesizes in its SQL load script (`salesGrowthFactor`
/// and `migrationRate` on `SourceTypeYear`, for example). Those land in
/// [`columns_omitted_by_importer`](Self::columns_omitted_by_importer) and
/// are *not* counted by [`has_importer_bug`](Self::has_importer_bug).
#[derive(Debug, Clone)]
pub struct ComparisonReport {
    /// Canonical `db__…` table id that was looked up.
    pub table: String,
    /// `false` when no canonical table was available to compare against.
    pub canonical_present: bool,
    /// Per-row and per-cell differences. Each is a candidate importer bug.
    pub row_diffs: Vec<RowDiff>,
    /// Columns canonical MOVES has that the importer did not emit. Often
    /// benign — importers omit columns the MOVES SQL load script derives —
    /// so these are surfaced but not treated as bugs.
    pub columns_omitted_by_importer: Vec<String>,
    /// Schema differences that *do* indicate a bug: a column the importer
    /// emits that canonical MOVES lacks, or a column-type mismatch.
    pub schema_bugs: Vec<SchemaDiff>,
}

impl ComparisonReport {
    /// A report for a table with no canonical baseline available.
    pub fn canonical_missing(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            canonical_present: false,
            row_diffs: Vec::new(),
            columns_omitted_by_importer: Vec::new(),
            schema_bugs: Vec::new(),
        }
    }

    /// `true` when importer output diverged from canonical MOVES in a way
    /// that indicates a bug — any row/cell difference or suspicious schema
    /// difference. The suite fails on this.
    pub fn has_importer_bug(&self) -> bool {
        !self.row_diffs.is_empty() || !self.schema_bugs.is_empty()
    }

    /// `true` when a canonical baseline existed and the importer output
    /// matched it (benign omitted columns aside).
    pub fn is_validated(&self) -> bool {
        self.canonical_present && !self.has_importer_bug()
    }
}

/// Compare one importer-produced Parquet table against canonical MOVES.
///
/// `canonical_table` is the full `db__…` table id (see
/// [`canonical_table_name`] / [`find_canonical_table`]). The importer
/// Parquet is normalized with the *canonical* table's own natural key, so
/// the diff is a keyed merge-join rather than a positional walk. When the
/// canonical snapshot has no such table the report is
/// [`ComparisonReport::canonical_missing`].
///
/// `opts` carries the float tolerance — pass `&DiffOptions::default()` for
/// strict bit-for-bit equality, or a configured one for known-harmless
/// numerical drift.
pub fn compare_importer_output(
    canonical_table: &str,
    importer_parquet: &[u8],
    canonical: &Snapshot,
    opts: &DiffOptions,
) -> Result<ComparisonReport> {
    let Some(canon) = canonical.table(canonical_table) else {
        return Ok(ComparisonReport::canonical_missing(canonical_table));
    };

    // Build the importer table with the canonical table's own name and
    // natural key so the diff is keyed and pairs the two tables.
    let key: Vec<&str> = canon.natural_key().iter().map(String::as_str).collect();
    let importer = parquet_to_table(canonical_table, importer_parquet, &key)?;

    let mut canonical_side = Snapshot::new();
    canonical_side.add_table(canon.clone())?;
    let mut importer_side = Snapshot::new();
    importer_side.add_table(importer)?;
    let diff = diff_snapshots(&canonical_side, &importer_side, opts);

    let mut report = ComparisonReport {
        table: canonical_table.to_string(),
        canonical_present: true,
        row_diffs: Vec::new(),
        columns_omitted_by_importer: Vec::new(),
        schema_bugs: Vec::new(),
    };

    if let Some(change) = diff
        .table_changes
        .into_iter()
        .find(|tc| tc.table == canonical_table)
    {
        report.row_diffs = change.row_diffs;
        // `canonical_side` is the diff's lhs, so `ColumnRemoved` is a
        // column present in canonical MOVES but missing from importer
        // output — a deliberate omission, not a bug. Everything else is.
        for schema_diff in change.schema_diffs {
            match schema_diff {
                SchemaDiff::ColumnRemoved(spec) => {
                    report.columns_omitted_by_importer.push(spec.name)
                }
                other => report.schema_bugs.push(other),
            }
        }
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema as ArrowSchema};
    use parquet::arrow::ArrowWriter;

    /// Encode an importer-style Parquet buffer (real `Int64` / `Float64` /
    /// `Utf8` columns — *not* the snapshot's fixed-decimal encoding).
    fn importer_parquet(columns: Vec<(&str, arrow::array::ArrayRef)>) -> Vec<u8> {
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, arr)| Field::new(*name, arr.data_type().clone(), true))
            .collect();
        let schema = Arc::new(ArrowSchema::new(fields));
        let batch = RecordBatch::try_new(
            schema.clone(),
            columns.into_iter().map(|(_, a)| a).collect(),
        )
        .unwrap();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        buf
    }

    fn int_col(values: &[i64]) -> arrow::array::ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }
    fn float_col(values: &[f64]) -> arrow::array::ArrayRef {
        Arc::new(Float64Array::from(values.to_vec()))
    }
    fn str_col(values: &[&str]) -> arrow::array::ArrayRef {
        Arc::new(StringArray::from(values.to_vec()))
    }

    #[test]
    fn canonical_table_name_lowercases_both_components() {
        assert_eq!(
            canonical_table_name("ProjectDB_26161", "Link"),
            "db__projectdb_26161__link"
        );
    }

    #[test]
    fn parquet_to_table_normalizes_and_sorts() {
        // Rows out of natural-key order; the builder must sort them.
        let parquet = importer_parquet(vec![
            ("yearID", int_col(&[2021, 2020, 2020])),
            ("sourceTypeID", int_col(&[21, 31, 21])),
            ("sourceTypePopulation", float_col(&[10.0, 20.0, 30.0])),
        ]);
        let table =
            parquet_to_table("SourceTypeYear", &parquet, &["yearID", "sourceTypeID"]).unwrap();
        assert_eq!(table.row_count(), 3);
        assert_eq!(table.natural_key(), &["yearID", "sourceTypeID"]);
        // First row after sorting is (2020, 21).
        let year = table.column_index("yearID").unwrap();
        let source = table.column_index("sourceTypeID").unwrap();
        assert_eq!(
            table.columns()[year].cell_string(0).as_deref(),
            Some("2020")
        );
        assert_eq!(
            table.columns()[source].cell_string(0).as_deref(),
            Some("21")
        );
    }

    #[test]
    fn parquet_to_table_rejects_unknown_natural_key_column() {
        let parquet = importer_parquet(vec![("a", int_col(&[1]))]);
        let err = parquet_to_table("t", &parquet, &["missing"]).unwrap_err();
        assert!(matches!(err, Error::Snapshot(_)), "got {err:?}");
    }

    /// Build a tiny canonical snapshot holding one `db__…` table.
    fn canonical_with(table: &str, parquet: &[u8], natural_key: &[&str]) -> Snapshot {
        let mut snap = Snapshot::new();
        snap.add_table(parquet_to_table(table, parquet, natural_key).unwrap())
            .unwrap();
        snap
    }

    #[test]
    fn compare_reports_match_when_identical() {
        let id = canonical_table_name("cdb", "Link");
        let parquet = importer_parquet(vec![
            ("linkID", int_col(&[1, 2])),
            ("linkLength", float_col(&[0.5, 1.0])),
        ]);
        let canonical = canonical_with(&id, &parquet, &["linkID"]);
        let report =
            compare_importer_output(&id, &parquet, &canonical, &DiffOptions::default()).unwrap();
        assert!(report.is_validated(), "{report:?}");
        assert!(!report.has_importer_bug());
    }

    #[test]
    fn compare_flags_changed_cell_as_importer_bug() {
        let id = canonical_table_name("cdb", "Link");
        let canonical_parquet = importer_parquet(vec![
            ("linkID", int_col(&[1, 2])),
            ("linkLength", float_col(&[0.5, 1.0])),
        ]);
        let canonical = canonical_with(&id, &canonical_parquet, &["linkID"]);

        // Importer output: linkID 2's length drifted 0.5 → 0.9.
        let importer_parquet_bytes = importer_parquet(vec![
            ("linkID", int_col(&[1, 2])),
            ("linkLength", float_col(&[0.5, 0.9])),
        ]);
        let report = compare_importer_output(
            &id,
            &importer_parquet_bytes,
            &canonical,
            &DiffOptions::default(),
        )
        .unwrap();
        assert!(report.has_importer_bug(), "{report:?}");
        assert_eq!(report.row_diffs.len(), 1);
    }

    #[test]
    fn compare_treats_omitted_canonical_column_as_benign() {
        let id = canonical_table_name("cdb", "SourceTypeYear");
        // Canonical has the SQL-derived salesGrowthFactor column.
        let canonical_parquet = importer_parquet(vec![
            ("yearID", int_col(&[2020])),
            ("sourceTypeID", int_col(&[21])),
            ("sourceTypePopulation", float_col(&[100.0])),
            ("salesGrowthFactor", float_col(&[0.0])),
        ]);
        let canonical = canonical_with(&id, &canonical_parquet, &["yearID", "sourceTypeID"]);

        // Importer omits salesGrowthFactor (matches the real importer).
        let importer_parquet_bytes = importer_parquet(vec![
            ("yearID", int_col(&[2020])),
            ("sourceTypeID", int_col(&[21])),
            ("sourceTypePopulation", float_col(&[100.0])),
        ]);
        let report = compare_importer_output(
            &id,
            &importer_parquet_bytes,
            &canonical,
            &DiffOptions::default(),
        )
        .unwrap();
        assert!(!report.has_importer_bug(), "{report:?}");
        assert!(report.is_validated());
        assert_eq!(
            report.columns_omitted_by_importer,
            vec!["salesGrowthFactor".to_string()]
        );
    }

    #[test]
    fn compare_flags_extra_importer_column_as_schema_bug() {
        let id = canonical_table_name("cdb", "Link");
        let canonical_parquet = importer_parquet(vec![("linkID", int_col(&[1]))]);
        let canonical = canonical_with(&id, &canonical_parquet, &["linkID"]);

        // Importer emits a stray column canonical MOVES never has.
        let importer_parquet_bytes = importer_parquet(vec![
            ("linkID", int_col(&[1])),
            ("strayColumn", str_col(&["x"])),
        ]);
        let report = compare_importer_output(
            &id,
            &importer_parquet_bytes,
            &canonical,
            &DiffOptions::default(),
        )
        .unwrap();
        assert!(report.has_importer_bug(), "{report:?}");
        assert_eq!(report.schema_bugs.len(), 1);
    }

    #[test]
    fn compare_reports_missing_canonical_table() {
        let canonical = Snapshot::new();
        let parquet = importer_parquet(vec![("linkID", int_col(&[1]))]);
        let report = compare_importer_output(
            "db__cdb__link",
            &parquet,
            &canonical,
            &DiffOptions::default(),
        )
        .unwrap();
        assert!(!report.canonical_present);
        assert!(!report.is_validated());
        assert!(!report.has_importer_bug());
    }

    #[test]
    fn find_canonical_table_matches_by_suffix() {
        let id = canonical_table_name("projectdb_26161_xyz", "Link");
        let parquet = importer_parquet(vec![("linkID", int_col(&[1]))]);
        let snapshot = canonical_with(&id, &parquet, &["linkID"]);
        assert_eq!(
            find_canonical_table(&snapshot, "Link").as_deref(),
            Some(id.as_str())
        );
        assert_eq!(find_canonical_table(&snapshot, "Zone"), None);
    }
}

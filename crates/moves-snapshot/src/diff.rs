//! Snapshot-to-snapshot diff.
//!
//! Diffs are produced at table-row-cell granularity. Float columns may use a
//! per-(table, column) absolute tolerance — values whose magnitudes differ by
//! at most the tolerance are considered equal.
//!
//! Row matching strategy:
//! * If both tables declare the **same** natural-key columns, rows are
//!   merge-joined on the key (both tables are sorted by the same lex order, so
//!   the walk is O(n)).
//! * Otherwise the matcher falls back to row-index matching and surfaces the
//!   schema-level disagreement via [`SchemaDiff::NaturalKeyChanged`].

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::format::{parse_fixed_decimal, ColumnKind, ColumnSpec};
use crate::snapshot::Snapshot;
use crate::table::Table;

/// Configuration knobs for [`diff_snapshots`].
///
/// `default_float_tolerance` applies to every Float64 column unless overridden
/// by `per_column_tolerance`. Both tolerances are absolute: two cells are
/// considered equal when `|lhs - rhs| <= tolerance`.
#[derive(Debug, Clone, Default)]
pub struct DiffOptions {
    pub default_float_tolerance: f64,
    pub per_column_tolerance: BTreeMap<(String, String), f64>,
}

impl DiffOptions {
    pub fn with_default_float_tolerance(mut self, t: f64) -> Self {
        self.default_float_tolerance = t;
        self
    }

    pub fn with_column_tolerance(mut self, table: &str, column: &str, t: f64) -> Self {
        self.per_column_tolerance
            .insert((table.to_string(), column.to_string()), t);
        self
    }

    fn tolerance_for(&self, table: &str, column: &str) -> f64 {
        self.per_column_tolerance
            .get(&(table.to_string(), column.to_string()))
            .copied()
            .unwrap_or(self.default_float_tolerance)
    }
}

/// Top-level diff result. A snapshot is unchanged iff [`Diff::is_empty`].
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Diff {
    /// Tables present in `rhs` but not `lhs`, lexicographically sorted.
    pub tables_added: Vec<String>,
    /// Tables present in `lhs` but not `rhs`, lexicographically sorted.
    pub tables_removed: Vec<String>,
    /// Per-table changes for tables present in both, lexicographically sorted
    /// by table name.
    pub table_changes: Vec<TableChange>,
}

impl Diff {
    pub fn is_empty(&self) -> bool {
        self.tables_added.is_empty()
            && self.tables_removed.is_empty()
            && self.table_changes.iter().all(|t| t.is_empty())
    }

    /// Aggregate counts. Useful for CI summaries and human-readable output.
    pub fn summary(&self) -> DiffSummary {
        let mut s = DiffSummary {
            tables_added: self.tables_added.len(),
            tables_removed: self.tables_removed.len(),
            tables_changed: self.table_changes.len(),
            schema_diffs: 0,
            rows_added: 0,
            rows_removed: 0,
            cells_changed: 0,
        };
        for tc in &self.table_changes {
            s.schema_diffs += tc.schema_diffs.len();
            for rd in &tc.row_diffs {
                match rd {
                    RowDiff::Added { .. } => s.rows_added += 1,
                    RowDiff::Removed { .. } => s.rows_removed += 1,
                    RowDiff::Cell { .. } => s.cells_changed += 1,
                }
            }
        }
        s
    }
}

/// Aggregate counts derived from a [`Diff`]. Stable order — designed to be
/// JSON-serialized as part of CI output.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct DiffSummary {
    pub tables_added: usize,
    pub tables_removed: usize,
    pub tables_changed: usize,
    pub schema_diffs: usize,
    pub rows_added: usize,
    pub rows_removed: usize,
    pub cells_changed: usize,
}

/// Per-table diff with both schema- and row-level changes.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TableChange {
    pub table: String,
    pub schema_diffs: Vec<SchemaDiff>,
    pub row_diffs: Vec<RowDiff>,
}

impl TableChange {
    pub fn is_empty(&self) -> bool {
        self.schema_diffs.is_empty() && self.row_diffs.is_empty()
    }
}

/// Schema-level disagreement between two same-named tables.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaDiff {
    ColumnAdded(ColumnSpec),
    ColumnRemoved(ColumnSpec),
    ColumnTypeChanged {
        name: String,
        lhs: ColumnKind,
        rhs: ColumnKind,
    },
    NaturalKeyChanged {
        lhs: Vec<String>,
        rhs: Vec<String>,
    },
}

/// Row-level diff event.
///
/// `key` carries the stringified natural-key cells that identify the row
/// (or the row index, formatted, when the table has no natural key).
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RowDiff {
    Added {
        key: Vec<String>,
    },
    Removed {
        key: Vec<String>,
    },
    Cell {
        key: Vec<String>,
        column: String,
        lhs: Option<String>,
        rhs: Option<String>,
    },
}

/// Compute the structured difference between `lhs` and `rhs`.
pub fn diff_snapshots(lhs: &Snapshot, rhs: &Snapshot, opts: &DiffOptions) -> Diff {
    let lhs_names: BTreeSet<&str> = lhs.table_names().collect();
    let rhs_names: BTreeSet<&str> = rhs.table_names().collect();

    let tables_added: Vec<String> = rhs_names
        .difference(&lhs_names)
        .map(|s| s.to_string())
        .collect();
    let tables_removed: Vec<String> = lhs_names
        .difference(&rhs_names)
        .map(|s| s.to_string())
        .collect();

    let mut table_changes = Vec::new();
    for name in lhs_names.intersection(&rhs_names) {
        let l = lhs.table(name).expect("table exists in lhs");
        let r = rhs.table(name).expect("table exists in rhs");
        let change = diff_table(l, r, opts);
        if !change.is_empty() {
            table_changes.push(change);
        }
    }

    Diff {
        tables_added,
        tables_removed,
        table_changes,
    }
}

fn diff_table(lhs: &Table, rhs: &Table, opts: &DiffOptions) -> TableChange {
    let mut schema_diffs = compute_schema_diffs(lhs, rhs);

    // Drop the NaturalKeyChanged sentinel before deciding the matching mode —
    // we still want to surface it, but its presence drives the fallback path.
    let same_key = lhs.natural_key() == rhs.natural_key();
    if !same_key {
        schema_diffs.push(SchemaDiff::NaturalKeyChanged {
            lhs: lhs.natural_key().to_vec(),
            rhs: rhs.natural_key().to_vec(),
        });
    }

    // Cells are only compared for columns present in both with the same kind.
    let comparable_columns = comparable_columns(lhs, rhs);

    let row_diffs = if same_key && !lhs.natural_key().is_empty() {
        diff_rows_keyed(lhs, rhs, &comparable_columns, opts)
    } else {
        diff_rows_positional(lhs, rhs, &comparable_columns, opts)
    };

    TableChange {
        table: lhs.name().to_string(),
        schema_diffs,
        row_diffs,
    }
}

/// Surface the schema-level differences between two same-named tables.
fn compute_schema_diffs(lhs: &Table, rhs: &Table) -> Vec<SchemaDiff> {
    let mut diffs = Vec::new();
    let lhs_by_name: BTreeMap<&str, &ColumnSpec> =
        lhs.schema().iter().map(|c| (c.name.as_str(), c)).collect();
    let rhs_by_name: BTreeMap<&str, &ColumnSpec> =
        rhs.schema().iter().map(|c| (c.name.as_str(), c)).collect();

    // Iterate by union of names, sorted, so the diff is stable.
    let names: BTreeSet<&str> = lhs_by_name
        .keys()
        .chain(rhs_by_name.keys())
        .copied()
        .collect();
    for name in names {
        match (lhs_by_name.get(name), rhs_by_name.get(name)) {
            (Some(l), Some(r)) if l.kind != r.kind => {
                diffs.push(SchemaDiff::ColumnTypeChanged {
                    name: name.to_string(),
                    lhs: l.kind,
                    rhs: r.kind,
                });
            }
            (Some(_), Some(_)) => {}
            (None, Some(r)) => diffs.push(SchemaDiff::ColumnAdded((*r).clone())),
            (Some(l), None) => diffs.push(SchemaDiff::ColumnRemoved((*l).clone())),
            (None, None) => unreachable!(),
        }
    }
    diffs
}

/// Pairs `(name, lhs_column_index, rhs_column_index, kind)` for every column
/// that is present in both tables with the same logical kind.
fn comparable_columns(lhs: &Table, rhs: &Table) -> Vec<(String, usize, usize, ColumnKind)> {
    let mut out = Vec::new();
    for (li, l) in lhs.schema().iter().enumerate() {
        if let Some(ri) = rhs.column_index(&l.name) {
            if rhs.schema()[ri].kind == l.kind {
                out.push((l.name.clone(), li, ri, l.kind));
            }
        }
    }
    out
}

/// Merge-join walk over the natural-key-sorted tables.
fn diff_rows_keyed(
    lhs: &Table,
    rhs: &Table,
    comparable: &[(String, usize, usize, ColumnKind)],
    opts: &DiffOptions,
) -> Vec<RowDiff> {
    let mut out = Vec::new();
    let lhs_keys: Vec<Vec<Option<String>>> =
        (0..lhs.row_count()).map(|i| key_cells(lhs, i)).collect();
    let rhs_keys: Vec<Vec<Option<String>>> =
        (0..rhs.row_count()).map(|i| key_cells(rhs, i)).collect();

    let mut i = 0usize;
    let mut j = 0usize;
    while i < lhs.row_count() && j < rhs.row_count() {
        match compare_keys(&lhs_keys[i], &rhs_keys[j]) {
            Ordering::Equal => {
                emit_cell_diffs(lhs, rhs, i, j, comparable, opts, &mut out);
                i += 1;
                j += 1;
            }
            Ordering::Less => {
                out.push(RowDiff::Removed {
                    key: stringify_key(&lhs_keys[i]),
                });
                i += 1;
            }
            Ordering::Greater => {
                out.push(RowDiff::Added {
                    key: stringify_key(&rhs_keys[j]),
                });
                j += 1;
            }
        }
    }
    while i < lhs.row_count() {
        out.push(RowDiff::Removed {
            key: stringify_key(&lhs_keys[i]),
        });
        i += 1;
    }
    while j < rhs.row_count() {
        out.push(RowDiff::Added {
            key: stringify_key(&rhs_keys[j]),
        });
        j += 1;
    }
    out
}

/// Positional walk used when natural keys differ or are absent.
fn diff_rows_positional(
    lhs: &Table,
    rhs: &Table,
    comparable: &[(String, usize, usize, ColumnKind)],
    opts: &DiffOptions,
) -> Vec<RowDiff> {
    let mut out = Vec::new();
    let common = lhs.row_count().min(rhs.row_count());
    for i in 0..common {
        emit_cell_diffs(lhs, rhs, i, i, comparable, opts, &mut out);
    }
    for i in common..lhs.row_count() {
        out.push(RowDiff::Removed {
            key: vec![format!("row[{i}]")],
        });
    }
    for j in common..rhs.row_count() {
        out.push(RowDiff::Added {
            key: vec![format!("row[{j}]")],
        });
    }
    out
}

fn emit_cell_diffs(
    lhs: &Table,
    rhs: &Table,
    li: usize,
    ri: usize,
    comparable: &[(String, usize, usize, ColumnKind)],
    opts: &DiffOptions,
    out: &mut Vec<RowDiff>,
) {
    let key = if lhs.natural_key().is_empty() {
        vec![format!("row[{li}]")]
    } else {
        stringify_key(&key_cells(lhs, li))
    };

    for (col_name, l_idx, r_idx, kind) in comparable {
        let l_col = &lhs.columns()[*l_idx];
        let r_col = &rhs.columns()[*r_idx];
        let l_val = l_col.cell_string(li);
        let r_val = r_col.cell_string(ri);

        if cells_equal(*kind, &l_val, &r_val, lhs.name(), col_name, opts) {
            continue;
        }

        out.push(RowDiff::Cell {
            key: key.clone(),
            column: col_name.clone(),
            lhs: l_val,
            rhs: r_val,
        });
    }
}

fn cells_equal(
    kind: ColumnKind,
    lhs: &Option<String>,
    rhs: &Option<String>,
    table: &str,
    column: &str,
    opts: &DiffOptions,
) -> bool {
    match (lhs, rhs) {
        (None, None) => true,
        (None, Some(_)) | (Some(_), None) => false,
        (Some(l), Some(r)) => {
            if l == r {
                return true;
            }
            if kind == ColumnKind::Float64 {
                let tol = opts.tolerance_for(table, column);
                if tol > 0.0 {
                    if let (Some(lf), Some(rf)) = (parse_fixed_decimal(l), parse_fixed_decimal(r)) {
                        return floats_within_tolerance(lf, rf, tol);
                    }
                }
            }
            false
        }
    }
}

fn floats_within_tolerance(a: f64, b: f64, tol: f64) -> bool {
    if a.is_nan() || b.is_nan() {
        return a.is_nan() && b.is_nan();
    }
    if a.is_infinite() || b.is_infinite() {
        return a == b;
    }
    (a - b).abs() <= tol
}

fn key_cells(table: &Table, row: usize) -> Vec<Option<String>> {
    if table.natural_key().is_empty() {
        return vec![Some(format!("row[{row}]"))];
    }
    table
        .natural_key()
        .iter()
        .map(|k| {
            let idx = table
                .column_index(k)
                .expect("natural_key columns validated at build");
            table.columns()[idx].cell_string(row)
        })
        .collect()
}

fn stringify_key(cells: &[Option<String>]) -> Vec<String> {
    cells
        .iter()
        .map(|c| c.clone().unwrap_or_else(|| "<null>".to_string()))
        .collect()
}

fn compare_keys(a: &[Option<String>], b: &[Option<String>]) -> Ordering {
    for (x, y) in a.iter().zip(b.iter()) {
        // None sorts before Some — same convention as the table builder.
        let ord = match (x, y) {
            (None, None) => Ordering::Equal,
            (None, _) => Ordering::Less,
            (_, None) => Ordering::Greater,
            (Some(xs), Some(ys)) => xs.cmp(ys),
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    a.len().cmp(&b.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::ColumnKind;
    use crate::table::{TableBuilder, Value};

    fn build_simple(name: &str, rows: &[(i64, f64)]) -> Table {
        let mut tb = TableBuilder::new(
            name,
            [
                ("id".to_string(), ColumnKind::Int64),
                ("v".to_string(), ColumnKind::Float64),
            ],
        )
        .unwrap()
        .with_natural_key(["id"])
        .unwrap();
        for (id, v) in rows {
            tb.push_row([Value::Int64(*id), Value::Float64(*v)])
                .unwrap();
        }
        tb.build().unwrap()
    }

    #[test]
    fn no_diff_when_equal() {
        let mut s1 = Snapshot::new();
        let mut s2 = Snapshot::new();
        s1.add_table(build_simple("t", &[(1, 1.0), (2, 2.0)]))
            .unwrap();
        s2.add_table(build_simple("t", &[(1, 1.0), (2, 2.0)]))
            .unwrap();
        let diff = diff_snapshots(&s1, &s2, &DiffOptions::default());
        assert!(diff.is_empty(), "{:?}", diff);
    }

    #[test]
    fn detects_added_and_removed_tables() {
        let mut s1 = Snapshot::new();
        let mut s2 = Snapshot::new();
        s1.add_table(build_simple("only_lhs", &[(1, 0.0)])).unwrap();
        s2.add_table(build_simple("only_rhs", &[(1, 0.0)])).unwrap();
        let diff = diff_snapshots(&s1, &s2, &DiffOptions::default());
        assert_eq!(diff.tables_removed, vec!["only_lhs".to_string()]);
        assert_eq!(diff.tables_added, vec!["only_rhs".to_string()]);
        assert!(diff.table_changes.is_empty());
    }

    #[test]
    fn detects_added_and_removed_rows() {
        let mut s1 = Snapshot::new();
        let mut s2 = Snapshot::new();
        s1.add_table(build_simple("t", &[(1, 1.0), (2, 2.0), (3, 3.0)]))
            .unwrap();
        s2.add_table(build_simple("t", &[(2, 2.0), (3, 3.0), (4, 4.0)]))
            .unwrap();
        let diff = diff_snapshots(&s1, &s2, &DiffOptions::default());
        assert_eq!(diff.table_changes.len(), 1);
        let row_diffs = &diff.table_changes[0].row_diffs;
        let added: Vec<_> = row_diffs
            .iter()
            .filter_map(|d| match d {
                RowDiff::Added { key } => Some(key.clone()),
                _ => None,
            })
            .collect();
        let removed: Vec<_> = row_diffs
            .iter()
            .filter_map(|d| match d {
                RowDiff::Removed { key } => Some(key.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(added, vec![vec!["4".to_string()]]);
        assert_eq!(removed, vec![vec!["1".to_string()]]);
    }

    #[test]
    fn detects_cell_changes() {
        let mut s1 = Snapshot::new();
        let mut s2 = Snapshot::new();
        s1.add_table(build_simple("t", &[(1, 1.0), (2, 2.0)]))
            .unwrap();
        s2.add_table(build_simple("t", &[(1, 1.5), (2, 2.0)]))
            .unwrap();
        let diff = diff_snapshots(&s1, &s2, &DiffOptions::default());
        let row_diffs = &diff.table_changes[0].row_diffs;
        assert_eq!(row_diffs.len(), 1);
        match &row_diffs[0] {
            RowDiff::Cell {
                key,
                column,
                lhs,
                rhs,
            } => {
                assert_eq!(key, &vec!["1".to_string()]);
                assert_eq!(column, "v");
                assert_eq!(lhs.as_deref(), Some("1.000000000000"));
                assert_eq!(rhs.as_deref(), Some("1.500000000000"));
            }
            other => panic!("unexpected diff: {other:?}"),
        }
    }

    #[test]
    fn float_tolerance_suppresses_small_diffs() {
        let mut s1 = Snapshot::new();
        let mut s2 = Snapshot::new();
        // Values differ by 1e-6 — within tolerance only when configured.
        s1.add_table(build_simple("t", &[(1, 1.000000)])).unwrap();
        s2.add_table(build_simple("t", &[(1, 1.000001)])).unwrap();

        let strict = diff_snapshots(&s1, &s2, &DiffOptions::default());
        assert!(!strict.is_empty(), "expected diff under strict mode");

        let loose = DiffOptions::default().with_default_float_tolerance(1e-5);
        let diff = diff_snapshots(&s1, &s2, &loose);
        assert!(diff.is_empty(), "{:?}", diff);
    }

    #[test]
    fn per_column_tolerance_overrides_default() {
        let mut s1 = Snapshot::new();
        let mut s2 = Snapshot::new();
        s1.add_table(build_simple("t", &[(1, 1.0)])).unwrap();
        s2.add_table(build_simple("t", &[(1, 1.05)])).unwrap();

        let opts = DiffOptions::default()
            .with_default_float_tolerance(0.0)
            .with_column_tolerance("t", "v", 0.1);
        let diff = diff_snapshots(&s1, &s2, &opts);
        assert!(diff.is_empty(), "{:?}", diff);
    }

    #[test]
    fn schema_diff_added_column() {
        let mut tb1 = TableBuilder::new("t", [("a".to_string(), ColumnKind::Int64)])
            .unwrap()
            .with_natural_key(["a"])
            .unwrap();
        tb1.push_row([Value::Int64(1)]).unwrap();
        let t1 = tb1.build().unwrap();

        let mut tb2 = TableBuilder::new(
            "t",
            [
                ("a".to_string(), ColumnKind::Int64),
                ("b".to_string(), ColumnKind::Utf8),
            ],
        )
        .unwrap()
        .with_natural_key(["a"])
        .unwrap();
        tb2.push_row([Value::Int64(1), Value::Utf8("x".into())])
            .unwrap();
        let t2 = tb2.build().unwrap();

        let mut s1 = Snapshot::new();
        let mut s2 = Snapshot::new();
        s1.add_table(t1).unwrap();
        s2.add_table(t2).unwrap();
        let diff = diff_snapshots(&s1, &s2, &DiffOptions::default());
        let change = &diff.table_changes[0];
        assert_eq!(change.schema_diffs.len(), 1);
        assert!(matches!(
            &change.schema_diffs[0],
            SchemaDiff::ColumnAdded(c) if c.name == "b"
        ));
        // b is not comparable, so no cell diffs.
        assert!(change.row_diffs.is_empty());
    }

    #[test]
    fn natural_key_change_falls_back_to_positional() {
        let mut tb1 = TableBuilder::new(
            "t",
            [
                ("a".to_string(), ColumnKind::Int64),
                ("b".to_string(), ColumnKind::Int64),
            ],
        )
        .unwrap()
        .with_natural_key(["a"])
        .unwrap();
        tb1.push_row([Value::Int64(1), Value::Int64(10)]).unwrap();
        tb1.push_row([Value::Int64(2), Value::Int64(20)]).unwrap();
        let t1 = tb1.build().unwrap();

        let mut tb2 = TableBuilder::new(
            "t",
            [
                ("a".to_string(), ColumnKind::Int64),
                ("b".to_string(), ColumnKind::Int64),
            ],
        )
        .unwrap()
        .with_natural_key(["b"])
        .unwrap();
        tb2.push_row([Value::Int64(1), Value::Int64(10)]).unwrap();
        tb2.push_row([Value::Int64(2), Value::Int64(20)]).unwrap();
        let t2 = tb2.build().unwrap();

        let mut s1 = Snapshot::new();
        let mut s2 = Snapshot::new();
        s1.add_table(t1).unwrap();
        s2.add_table(t2).unwrap();
        let diff = diff_snapshots(&s1, &s2, &DiffOptions::default());
        let change = &diff.table_changes[0];
        assert!(change
            .schema_diffs
            .iter()
            .any(|d| matches!(d, SchemaDiff::NaturalKeyChanged { .. })));
    }

    #[test]
    fn null_vs_value_is_a_diff() {
        let mut tb1 = TableBuilder::new(
            "t",
            [
                ("a".to_string(), ColumnKind::Int64),
                ("v".to_string(), ColumnKind::Int64),
            ],
        )
        .unwrap()
        .with_natural_key(["a"])
        .unwrap();
        tb1.push_row([Value::Int64(1), Value::Null]).unwrap();
        let t1 = tb1.build().unwrap();

        let mut tb2 = TableBuilder::new(
            "t",
            [
                ("a".to_string(), ColumnKind::Int64),
                ("v".to_string(), ColumnKind::Int64),
            ],
        )
        .unwrap()
        .with_natural_key(["a"])
        .unwrap();
        tb2.push_row([Value::Int64(1), Value::Int64(5)]).unwrap();
        let t2 = tb2.build().unwrap();

        let mut s1 = Snapshot::new();
        let mut s2 = Snapshot::new();
        s1.add_table(t1).unwrap();
        s2.add_table(t2).unwrap();
        let diff = diff_snapshots(&s1, &s2, &DiffOptions::default());
        let change = &diff.table_changes[0];
        assert_eq!(change.row_diffs.len(), 1);
        match &change.row_diffs[0] {
            RowDiff::Cell { lhs, rhs, .. } => {
                assert_eq!(lhs.as_deref(), None);
                assert_eq!(rhs.as_deref(), Some("5"));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
}

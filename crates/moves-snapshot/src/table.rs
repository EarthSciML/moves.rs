//! In-memory representation of a normalized snapshot table.

use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::error::{Error, Result};
use crate::format::{float_to_fixed_decimal, ColumnKind, ColumnSpec, FLOAT_DECIMALS};

/// Tagged value used by the row-based builder API. Variants must match the
/// declared `ColumnKind` for the target column. `Null` is accepted for any
/// kind.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Boolean(bool),
    Null,
}

impl Value {
    fn type_name(&self) -> &'static str {
        match self {
            Value::Int64(_) => "int64",
            Value::Float64(_) => "float64",
            Value::Utf8(_) => "utf8",
            Value::Boolean(_) => "boolean",
            Value::Null => "null",
        }
    }
}

/// Columnar storage for one column after normalization. Floats are stored as
/// the fixed-decimal strings that will land in the parquet file — the f64
/// form is gone after `TableBuilder::build`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NormalizedColumn {
    Int64(Vec<Option<i64>>),
    Float64String(Vec<Option<String>>),
    Utf8(Vec<Option<String>>),
    Boolean(Vec<Option<bool>>),
}

impl NormalizedColumn {
    pub fn kind(&self) -> ColumnKind {
        match self {
            NormalizedColumn::Int64(_) => ColumnKind::Int64,
            NormalizedColumn::Float64String(_) => ColumnKind::Float64,
            NormalizedColumn::Utf8(_) => ColumnKind::Utf8,
            NormalizedColumn::Boolean(_) => ColumnKind::Boolean,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            NormalizedColumn::Int64(v) => v.len(),
            NormalizedColumn::Float64String(v) => v.len(),
            NormalizedColumn::Utf8(v) => v.len(),
            NormalizedColumn::Boolean(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Stringified view of cell `i` for diffing and sort. Returns `None` when
    /// the cell is null.
    pub fn cell_string(&self, i: usize) -> Option<String> {
        match self {
            NormalizedColumn::Int64(v) => v[i].map(|n| n.to_string()),
            NormalizedColumn::Float64String(v) => v[i].clone(),
            NormalizedColumn::Utf8(v) => v[i].clone(),
            NormalizedColumn::Boolean(v) => {
                v[i].map(|b| if b { "true" } else { "false" }.to_string())
            }
        }
    }
}

/// A normalized table: schema + natural-key columns + columnar data, all
/// sorted lexicographically by the natural-key columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    name: String,
    schema: Vec<ColumnSpec>,
    natural_key: Vec<String>,
    columns: Vec<NormalizedColumn>,
}

impl Table {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn schema(&self) -> &[ColumnSpec] {
        &self.schema
    }

    pub fn natural_key(&self) -> &[String] {
        &self.natural_key
    }

    pub fn row_count(&self) -> usize {
        self.columns.first().map(|c| c.len()).unwrap_or(0)
    }

    pub fn columns(&self) -> &[NormalizedColumn] {
        &self.columns
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.schema.iter().position(|c| c.name == name)
    }

    /// Construct from already-normalized columnar data (e.g., when loading
    /// from disk). Validates lengths and kind agreement.
    pub fn from_normalized(
        name: String,
        schema: Vec<ColumnSpec>,
        natural_key: Vec<String>,
        columns: Vec<NormalizedColumn>,
    ) -> Result<Self> {
        if schema.len() != columns.len() {
            return Err(Error::RowWidthMismatch {
                table: name,
                row: 0,
                expected: schema.len(),
                actual: columns.len(),
            });
        }
        for (spec, col) in schema.iter().zip(columns.iter()) {
            if spec.kind != col.kind() {
                return Err(Error::ColumnTypeMismatch {
                    table: name,
                    column: spec.name.clone(),
                    declared: spec.kind.as_str().to_string(),
                    actual: col.kind().as_str().to_string(),
                });
            }
        }
        let row_count = columns.first().map(|c| c.len()).unwrap_or(0);
        for col in &columns {
            if col.len() != row_count {
                return Err(Error::RowWidthMismatch {
                    table: name,
                    row: col.len(),
                    expected: row_count,
                    actual: col.len(),
                });
            }
        }
        validate_natural_key(&name, &schema, &natural_key)?;
        Ok(Self {
            name,
            schema,
            natural_key,
            columns,
        })
    }

    /// Convert to an Arrow RecordBatch for parquet writing. Float columns
    /// surface as Utf8 in the arrow schema, since that's what gets written
    /// to disk.
    pub fn to_record_batch(&self) -> Result<(SchemaRef, RecordBatch)> {
        let mut fields = Vec::with_capacity(self.schema.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.schema.len());
        for (spec, col) in self.schema.iter().zip(self.columns.iter()) {
            let (dtype, array) = column_to_arrow(col);
            fields.push(Field::new(&spec.name, dtype, true));
            arrays.push(array);
        }
        let schema: SchemaRef = Arc::new(ArrowSchema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), arrays)?;
        Ok((schema, batch))
    }

    /// Reconstruct a Table from a parquet-derived RecordBatch plus the
    /// per-table metadata recovered from the sidecar.
    pub fn from_record_batch(
        name: String,
        schema_spec: Vec<ColumnSpec>,
        natural_key: Vec<String>,
        batch: &RecordBatch,
    ) -> Result<Self> {
        if batch.num_columns() != schema_spec.len() {
            return Err(Error::RowWidthMismatch {
                table: name,
                row: 0,
                expected: schema_spec.len(),
                actual: batch.num_columns(),
            });
        }
        let mut columns = Vec::with_capacity(schema_spec.len());
        for (i, spec) in schema_spec.iter().enumerate() {
            let array = batch.column(i);
            let col = arrow_to_column(&name, spec, array.as_ref())?;
            columns.push(col);
        }
        Self::from_normalized(name, schema_spec, natural_key, columns)
    }
}

/// Row-oriented builder. Use `push_row` to add rows in any order; `build`
/// normalizes floats to fixed-decimal strings and sorts rows lexicographically
/// on the natural-key columns.
#[derive(Debug)]
pub struct TableBuilder {
    name: String,
    schema: Vec<ColumnSpec>,
    natural_key: Vec<String>,
    rows: Vec<Vec<Value>>,
}

impl TableBuilder {
    pub fn new<N, S>(name: N, schema: S) -> Result<Self>
    where
        N: Into<String>,
        S: IntoIterator<Item = (String, ColumnKind)>,
    {
        let name = name.into();
        let schema: Vec<ColumnSpec> = schema
            .into_iter()
            .map(|(n, k)| ColumnSpec { name: n, kind: k })
            .collect();
        let mut seen: HashSet<&str> = HashSet::new();
        for spec in &schema {
            if !seen.insert(spec.name.as_str()) {
                return Err(Error::DuplicateColumn {
                    table: name,
                    column: spec.name.clone(),
                });
            }
        }
        Ok(Self {
            name,
            schema,
            natural_key: Vec::new(),
            rows: Vec::new(),
        })
    }

    /// Set the natural-key columns. Order matters: rows are sorted first by
    /// `natural_key[0]`, then `natural_key[1]`, etc.
    pub fn with_natural_key<I, S>(mut self, key: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.natural_key = key.into_iter().map(|s| s.into()).collect();
        validate_natural_key(&self.name, &self.schema, &self.natural_key)?;
        Ok(self)
    }

    pub fn push_row<I>(&mut self, row: I) -> Result<()>
    where
        I: IntoIterator<Item = Value>,
    {
        let row: Vec<Value> = row.into_iter().collect();
        if row.len() != self.schema.len() {
            return Err(Error::RowWidthMismatch {
                table: self.name.clone(),
                row: self.rows.len(),
                expected: self.schema.len(),
                actual: row.len(),
            });
        }
        for (i, (spec, val)) in self.schema.iter().zip(row.iter()).enumerate() {
            if !value_matches_kind(spec.kind, val) {
                return Err(Error::ColumnTypeMismatch {
                    table: self.name.clone(),
                    column: spec.name.clone(),
                    declared: spec.kind.as_str().to_string(),
                    actual: val.type_name().to_string(),
                });
            }
            // i is unused beyond bounds-pairing; keep as a stable iterator
            // anchor in case we add per-row diagnostics.
            let _ = i;
        }
        self.rows.push(row);
        Ok(())
    }

    pub fn build(mut self) -> Result<Table> {
        // Sort first (Vec<Vec<Value>> form makes this straightforward).
        sort_rows(&mut self.rows, &self.schema, &self.natural_key);

        // Pivot row-major to columnar, normalizing floats as we go.
        let n_cols = self.schema.len();
        let n_rows = self.rows.len();
        let mut columns: Vec<NormalizedColumn> = Vec::with_capacity(n_cols);
        for spec in &self.schema {
            columns.push(empty_normalized_column(spec.kind, n_rows));
        }
        for row in &self.rows {
            for (i, val) in row.iter().enumerate() {
                push_value(&mut columns[i], val);
            }
        }
        Table::from_normalized(self.name, self.schema, self.natural_key, columns)
    }
}

fn validate_natural_key(table: &str, schema: &[ColumnSpec], key: &[String]) -> Result<()> {
    let mut seen: HashSet<&str> = HashSet::new();
    for k in key {
        if !schema.iter().any(|s| &s.name == k) {
            return Err(Error::NaturalKeyColumnMissing {
                table: table.to_string(),
                column: k.clone(),
            });
        }
        if !seen.insert(k.as_str()) {
            return Err(Error::DuplicateColumn {
                table: table.to_string(),
                column: k.clone(),
            });
        }
    }
    Ok(())
}

fn value_matches_kind(kind: ColumnKind, val: &Value) -> bool {
    matches!(
        (kind, val),
        (_, Value::Null)
            | (ColumnKind::Int64, Value::Int64(_))
            | (ColumnKind::Float64, Value::Float64(_))
            | (ColumnKind::Utf8, Value::Utf8(_))
            | (ColumnKind::Boolean, Value::Boolean(_))
    )
}

fn empty_normalized_column(kind: ColumnKind, capacity: usize) -> NormalizedColumn {
    match kind {
        ColumnKind::Int64 => NormalizedColumn::Int64(Vec::with_capacity(capacity)),
        ColumnKind::Float64 => NormalizedColumn::Float64String(Vec::with_capacity(capacity)),
        ColumnKind::Utf8 => NormalizedColumn::Utf8(Vec::with_capacity(capacity)),
        ColumnKind::Boolean => NormalizedColumn::Boolean(Vec::with_capacity(capacity)),
    }
}

fn push_value(col: &mut NormalizedColumn, val: &Value) {
    match (col, val) {
        (NormalizedColumn::Int64(v), Value::Int64(x)) => v.push(Some(*x)),
        (NormalizedColumn::Int64(v), Value::Null) => v.push(None),
        (NormalizedColumn::Float64String(v), Value::Float64(x)) => {
            v.push(Some(float_to_fixed_decimal(*x, FLOAT_DECIMALS)));
        }
        (NormalizedColumn::Float64String(v), Value::Null) => v.push(None),
        (NormalizedColumn::Utf8(v), Value::Utf8(s)) => v.push(Some(s.clone())),
        (NormalizedColumn::Utf8(v), Value::Null) => v.push(None),
        (NormalizedColumn::Boolean(v), Value::Boolean(b)) => v.push(Some(*b)),
        (NormalizedColumn::Boolean(v), Value::Null) => v.push(None),
        _ => unreachable!("type mismatch should be caught in push_row"),
    }
}

fn sort_rows(rows: &mut [Vec<Value>], schema: &[ColumnSpec], natural_key: &[String]) {
    if natural_key.is_empty() {
        return;
    }
    let key_indices: Vec<(usize, ColumnKind)> = natural_key
        .iter()
        .map(|k| {
            let idx = schema.iter().position(|c| &c.name == k).expect("validated");
            (idx, schema[idx].kind)
        })
        .collect();

    rows.sort_by(|a, b| {
        for &(idx, kind) in &key_indices {
            let cmp = compare_values(kind, &a[idx], &b[idx]);
            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    });
}

fn compare_values(kind: ColumnKind, a: &Value, b: &Value) -> Ordering {
    // Nulls sort first, deterministically.
    match (a, b) {
        (Value::Null, Value::Null) => return Ordering::Equal,
        (Value::Null, _) => return Ordering::Less,
        (_, Value::Null) => return Ordering::Greater,
        _ => {}
    }
    match (kind, a, b) {
        (ColumnKind::Int64, Value::Int64(x), Value::Int64(y)) => x.cmp(y),
        (ColumnKind::Float64, Value::Float64(x), Value::Float64(y)) => {
            // Sort by the canonical fixed-decimal string so the order is
            // identical before and after normalization.
            let xs = float_to_fixed_decimal(*x, FLOAT_DECIMALS);
            let ys = float_to_fixed_decimal(*y, FLOAT_DECIMALS);
            xs.cmp(&ys)
        }
        (ColumnKind::Utf8, Value::Utf8(x), Value::Utf8(y)) => x.cmp(y),
        (ColumnKind::Boolean, Value::Boolean(x), Value::Boolean(y)) => x.cmp(y),
        _ => unreachable!("type mismatch should be caught in push_row"),
    }
}

fn column_to_arrow(col: &NormalizedColumn) -> (DataType, ArrayRef) {
    match col {
        NormalizedColumn::Int64(v) => {
            let arr = Int64Array::from(v.clone());
            (DataType::Int64, Arc::new(arr) as ArrayRef)
        }
        NormalizedColumn::Float64String(v) => {
            let arr = StringArray::from(v.clone());
            (DataType::Utf8, Arc::new(arr) as ArrayRef)
        }
        NormalizedColumn::Utf8(v) => {
            let arr = StringArray::from(v.clone());
            (DataType::Utf8, Arc::new(arr) as ArrayRef)
        }
        NormalizedColumn::Boolean(v) => {
            let arr = BooleanArray::from(v.clone());
            (DataType::Boolean, Arc::new(arr) as ArrayRef)
        }
    }
}

fn arrow_to_column(table: &str, spec: &ColumnSpec, array: &dyn Array) -> Result<NormalizedColumn> {
    let n = array.len();
    let unsupported = |dtype: &DataType| Error::UnsupportedColumnType {
        table: table.to_string(),
        column: spec.name.clone(),
        dtype: format!("{dtype:?}"),
    };
    match spec.kind {
        ColumnKind::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                Error::ColumnTypeMismatch {
                    table: table.to_string(),
                    column: spec.name.clone(),
                    declared: ColumnKind::Int64.as_str().to_string(),
                    actual: format!("{:?}", array.data_type()),
                }
            })?;
            let mut v = Vec::with_capacity(n);
            for i in 0..n {
                v.push(if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i))
                });
            }
            Ok(NormalizedColumn::Int64(v))
        }
        ColumnKind::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::ColumnTypeMismatch {
                    table: table.to_string(),
                    column: spec.name.clone(),
                    declared: ColumnKind::Float64.as_str().to_string(),
                    actual: format!("{:?}", array.data_type()),
                })?;
            let mut v = Vec::with_capacity(n);
            for i in 0..n {
                v.push(if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i).to_string())
                });
            }
            Ok(NormalizedColumn::Float64String(v))
        }
        ColumnKind::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::ColumnTypeMismatch {
                    table: table.to_string(),
                    column: spec.name.clone(),
                    declared: ColumnKind::Utf8.as_str().to_string(),
                    actual: format!("{:?}", array.data_type()),
                })?;
            let mut v = Vec::with_capacity(n);
            for i in 0..n {
                v.push(if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i).to_string())
                });
            }
            Ok(NormalizedColumn::Utf8(v))
        }
        ColumnKind::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| Error::ColumnTypeMismatch {
                    table: table.to_string(),
                    column: spec.name.clone(),
                    declared: ColumnKind::Boolean.as_str().to_string(),
                    actual: format!("{:?}", array.data_type()),
                })?;
            let mut v = Vec::with_capacity(n);
            for i in 0..n {
                v.push(if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i))
                });
            }
            Ok(NormalizedColumn::Boolean(v))
        }
    }
    .map_err(|err| {
        // Surface a richer message if the array's data type doesn't even
        // correspond to any supported kind.
        if matches!(err, Error::ColumnTypeMismatch { .. }) {
            match array.data_type() {
                DataType::Int64 | DataType::Utf8 | DataType::Boolean => err,
                other => unsupported(other),
            }
        } else {
            err
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ic(name: &str) -> (String, ColumnKind) {
        (name.to_string(), ColumnKind::Int64)
    }
    fn fc(name: &str) -> (String, ColumnKind) {
        (name.to_string(), ColumnKind::Float64)
    }

    #[test]
    fn build_sorts_by_natural_key() {
        let mut tb = TableBuilder::new("t", [ic("a"), fc("b")])
            .unwrap()
            .with_natural_key(["a"])
            .unwrap();
        tb.push_row([Value::Int64(3), Value::Float64(0.3)]).unwrap();
        tb.push_row([Value::Int64(1), Value::Float64(0.1)]).unwrap();
        tb.push_row([Value::Int64(2), Value::Float64(0.2)]).unwrap();
        let t = tb.build().unwrap();
        let NormalizedColumn::Int64(a) = &t.columns[0] else {
            panic!()
        };
        assert_eq!(a, &[Some(1), Some(2), Some(3)]);
    }

    #[test]
    fn build_normalizes_floats() {
        let mut tb = TableBuilder::new("t", [fc("x")])
            .unwrap()
            .with_natural_key::<[&str; 0], &str>([])
            .unwrap();
        tb.push_row([Value::Float64(1.0 + 1e-13)]).unwrap();
        let t = tb.build().unwrap();
        let NormalizedColumn::Float64String(x) = &t.columns[0] else {
            panic!()
        };
        assert_eq!(x[0].as_deref(), Some("1.000000000000"));
    }

    #[test]
    fn duplicate_column_rejected() {
        let err = TableBuilder::new("t", [ic("a"), ic("a")]).unwrap_err();
        assert!(matches!(err, Error::DuplicateColumn { .. }));
    }

    #[test]
    fn natural_key_must_exist() {
        let err = TableBuilder::new("t", [ic("a")])
            .unwrap()
            .with_natural_key(["b"])
            .unwrap_err();
        assert!(matches!(err, Error::NaturalKeyColumnMissing { .. }));
    }

    #[test]
    fn type_mismatch_in_row_rejected() {
        let mut tb = TableBuilder::new("t", [ic("a")]).unwrap();
        let err = tb.push_row([Value::Float64(1.0)]).unwrap_err();
        assert!(matches!(err, Error::ColumnTypeMismatch { .. }));
    }

    #[test]
    fn arrow_round_trip() {
        let mut tb = TableBuilder::new("t", [ic("a"), fc("b")])
            .unwrap()
            .with_natural_key(["a"])
            .unwrap();
        tb.push_row([Value::Int64(1), Value::Float64(1.5)]).unwrap();
        tb.push_row([Value::Int64(2), Value::Null]).unwrap();
        let t1 = tb.build().unwrap();
        let (_schema, batch) = t1.to_record_batch().unwrap();
        let t2 = Table::from_record_batch(
            t1.name().to_string(),
            t1.schema().to_vec(),
            t1.natural_key().to_vec(),
            &batch,
        )
        .unwrap();
        assert_eq!(t1, t2);
    }
}

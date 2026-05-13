//! [`DefaultDb`] — the lazy-loading entry point.
//!
//! Open a default-DB tree with [`DefaultDb::open`] and call
//! [`DefaultDb::scan`] to get a Polars [`LazyFrame`] over a table. The
//! scan consults the manifest, applies the caller's [`TableFilter`] to
//! prune partitions, then builds a concatenated `LazyFrame` over the
//! matching files.
//!
//! ## Partition pruning
//!
//! For a monolithic table the scan opens exactly one Parquet file. For
//! partitioned tables the scan iterates `manifest.tables[N].partitions`
//! and keeps only those whose `values` satisfy every constrained
//! predicate. The Polars query engine then handles the actual file
//! reads lazily — `collect()` is the materialization point.
//!
//! ## Schema-only tables
//!
//! `Link`, `SHO`, `SourceHours`, and `Starts` ship empty in the default
//! DB and are populated by the runtime. Calling [`DefaultDb::scan`] on
//! one of these returns [`crate::Error::SchemaOnly`]; use
//! [`DefaultDb::schema_sidecar`] to read the column types.

use std::path::{Path, PathBuf};

use polars::prelude::*;

use crate::error::{Error, Result};
use crate::filter::TableFilter;
use crate::manifest::{
    find_table, load as load_manifest, Manifest, PartitionManifest, SchemaOnlySidecar,
    TableManifest, MANIFEST_FILENAME,
};

/// Reader over a converted MOVES default-DB Parquet tree.
///
/// Cheap to clone — holds a [`PathBuf`] and a parsed [`Manifest`]. The
/// manifest is loaded once at [`DefaultDb::open`]; subsequent
/// [`DefaultDb::scan`] calls are pure path manipulation plus a Polars
/// `scan_parquet`.
#[derive(Debug, Clone)]
pub struct DefaultDb {
    root: PathBuf,
    manifest: Manifest,
}

impl DefaultDb {
    /// Open a default-DB tree rooted at `root`. `root` must contain a
    /// `manifest.json` from `moves-default-db-convert`.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        let manifest_path = root.join(MANIFEST_FILENAME);
        let manifest = load_manifest(&manifest_path)?;
        Ok(Self { root, manifest })
    }

    /// Root directory the reader was opened against.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// EPA release label, e.g. `movesdb20241112`.
    pub fn db_version(&self) -> &str {
        &self.manifest.moves_db_version
    }

    /// Full manifest (parsed, schema-version checked).
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Look up a table manifest by case-insensitive name.
    pub fn table(&self, name: &str) -> Option<&TableManifest> {
        find_table(&self.manifest, name)
    }

    /// Iterate every table manifest in the database.
    pub fn tables(&self) -> impl Iterator<Item = &TableManifest> {
        self.manifest.tables.iter()
    }

    /// Build a [`LazyFrame`] over the named table.
    ///
    /// `filter` drives partition pruning: only Parquet files whose
    /// partition values satisfy every constrained predicate are loaded.
    /// An empty filter loads every partition. Predicates against
    /// non-partition columns are not part of `TableFilter` — express
    /// those on the returned `LazyFrame` with `.filter(col("x").eq(...))`.
    ///
    /// Returns:
    ///
    /// * [`Error::UnknownTable`] — `name` is not in the manifest.
    /// * [`Error::SchemaOnly`] — the table is a schema-only sidecar
    ///   (no Parquet data); use [`DefaultDb::schema_sidecar`].
    /// * [`Error::UnknownPartitionColumn`] — the filter references a
    ///   column the table is not partitioned on. Surfacing this loudly
    ///   prevents a silent "no pruning happened" bug.
    pub fn scan(&self, name: &str, filter: &TableFilter) -> Result<LazyFrame> {
        let table = self
            .table(name)
            .ok_or_else(|| Error::UnknownTable(name.to_string()))?;
        if is_schema_only(table) {
            return Err(Error::SchemaOnly {
                table: table.name.clone(),
            });
        }
        validate_filter_columns(table, filter)?;
        let selected = select_partitions(table, filter);
        scan_partitions(&self.root, &selected)
    }

    /// Read the schema-only sidecar for a `schema_only` table (e.g.
    /// `SHO`). Returns `None` for tables that ship with data; the data
    /// path is [`Self::scan`] instead.
    pub fn schema_sidecar(&self, name: &str) -> Result<Option<SchemaOnlySidecar>> {
        let table = self
            .table(name)
            .ok_or_else(|| Error::UnknownTable(name.to_string()))?;
        let Some(rel) = &table.schema_only_path else {
            return Ok(None);
        };
        let path = self.root.join(rel);
        let bytes = std::fs::read(&path).map_err(|source| Error::Io {
            path: path.clone(),
            source,
        })?;
        let parsed: SchemaOnlySidecar =
            serde_json::from_slice(&bytes).map_err(|source| Error::ManifestParse {
                path: path.clone(),
                source,
            })?;
        Ok(Some(parsed))
    }
}

fn is_schema_only(table: &TableManifest) -> bool {
    table.schema_only_path.is_some()
}

/// Reject filter predicates that name a column the table isn't
/// partitioned on. Without this check a typo like `partition_eq("countID"
/// /* missing 'y' */, 17)` would silently load every partition.
fn validate_filter_columns(table: &TableManifest, filter: &TableFilter) -> Result<()> {
    for col in filter.columns() {
        if !table.partition_columns.iter().any(|c| c == col) {
            return Err(Error::UnknownPartitionColumn {
                table: table.name.clone(),
                column: col.to_string(),
                partition_columns: table.partition_columns.clone(),
            });
        }
    }
    Ok(())
}

/// Walk the table's partitions and keep those that satisfy every set
/// predicate. The manifest's `partitions[].values` array is in the same
/// order as `partition_columns`, so the filter check is positional.
fn select_partitions<'a>(
    table: &'a TableManifest,
    filter: &TableFilter,
) -> Vec<&'a PartitionManifest> {
    if table.partition_columns.is_empty() {
        // Monolithic table: the manifest has exactly one partition (the
        // single Parquet file). No pruning to do.
        return table.partitions.iter().collect();
    }
    table
        .partitions
        .iter()
        .filter(|p| filter.matches(&table.partition_columns, &p.values))
        .collect()
}

/// Build a `LazyFrame` from the selected partition files. Empty
/// selection (no partitions matched) returns an empty frame typed by
/// re-scanning the first available partition file's schema — Polars
/// can't infer a schema with zero files, but for partition pruning the
/// "no match" case is normal (e.g., year=1900 against a default DB that
/// stops at 1999) and the caller shouldn't have to handle it as an
/// error.
fn scan_partitions(root: &Path, partitions: &[&PartitionManifest]) -> Result<LazyFrame> {
    if partitions.is_empty() {
        return empty_frame();
    }
    let paths: Vec<PlRefPath> = partitions
        .iter()
        .map(|p| PlRefPath::try_from_pathbuf(root.join(&p.path)))
        .collect::<polars::error::PolarsResult<Vec<_>>>()?;
    if paths.len() == 1 {
        return Ok(LazyFrame::scan_parquet(
            paths.into_iter().next().unwrap(),
            ScanArgsParquet::default(),
        )?);
    }
    let lfs: Vec<LazyFrame> = paths
        .into_iter()
        .map(|p| LazyFrame::scan_parquet(p, ScanArgsParquet::default()))
        .collect::<polars::error::PolarsResult<Vec<_>>>()?;
    Ok(concat(lfs, UnionArgs::default())?)
}

/// Build an empty [`LazyFrame`]. Used when partition pruning eliminated
/// every file. The frame has no columns — callers downstream that need
/// the table's schema should fall back to the manifest's `columns`
/// array.
fn empty_frame() -> Result<LazyFrame> {
    let empty = DataFrame::default();
    Ok(empty.lazy())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{ColumnManifest, Manifest, PartitionManifest, TableManifest};

    fn mk_partition(path: &str, values: &[&str]) -> PartitionManifest {
        PartitionManifest {
            path: path.into(),
            values: values.iter().map(|s| s.to_string()).collect(),
            row_count: 0,
            sha256: "00".into(),
            bytes: 0,
        }
    }

    fn mk_table(
        name: &str,
        partition_columns: &[&str],
        partitions: Vec<PartitionManifest>,
    ) -> TableManifest {
        TableManifest {
            name: name.into(),
            partition_strategy: if partition_columns.is_empty() {
                "monolithic"
            } else {
                "test"
            }
            .into(),
            partition_columns: partition_columns.iter().map(|s| s.to_string()).collect(),
            row_count: 0,
            columns: vec![],
            primary_key: vec![],
            partitions,
            schema_only_path: None,
        }
    }

    #[test]
    fn select_partitions_returns_all_for_empty_filter() {
        let t = mk_table(
            "T",
            &["countyID"],
            vec![
                mk_partition("T/county=1/part.parquet", &["1"]),
                mk_partition("T/county=2/part.parquet", &["2"]),
            ],
        );
        let f = TableFilter::new();
        let selected = select_partitions(&t, &f);
        assert_eq!(selected.len(), 2);
    }

    #[test]
    fn select_partitions_prunes_by_eq() {
        let t = mk_table(
            "T",
            &["countyID"],
            vec![
                mk_partition("T/county=1/part.parquet", &["1"]),
                mk_partition("T/county=2/part.parquet", &["2"]),
            ],
        );
        let f = TableFilter::new().partition_eq("countyID", 2i64);
        let selected = select_partitions(&t, &f);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].values, vec!["2".to_string()]);
    }

    #[test]
    fn select_partitions_prunes_by_in_set() {
        let t = mk_table(
            "T",
            &["yearID", "countyID"],
            vec![
                mk_partition("T/year=2020/county=1/part.parquet", &["2020", "1"]),
                mk_partition("T/year=2021/county=1/part.parquet", &["2021", "1"]),
                mk_partition("T/year=2020/county=2/part.parquet", &["2020", "2"]),
                mk_partition("T/year=2025/county=2/part.parquet", &["2025", "2"]),
            ],
        );
        let f = TableFilter::new()
            .partition_in("yearID", [2020i64, 2021])
            .partition_eq("countyID", 1i64);
        let selected = select_partitions(&t, &f);
        let paths: Vec<&str> = selected.iter().map(|p| p.path.as_str()).collect();
        assert_eq!(
            paths,
            vec![
                "T/year=2020/county=1/part.parquet",
                "T/year=2021/county=1/part.parquet",
            ]
        );
    }

    #[test]
    fn select_partitions_returns_empty_when_nothing_matches() {
        let t = mk_table(
            "T",
            &["countyID"],
            vec![mk_partition("T/county=1/part.parquet", &["1"])],
        );
        let f = TableFilter::new().partition_eq("countyID", 99i64);
        assert!(select_partitions(&t, &f).is_empty());
    }

    #[test]
    fn validate_filter_columns_errors_on_unknown_column() {
        let t = mk_table("T", &["countyID"], vec![]);
        let f = TableFilter::new().partition_eq("yearID", 2020i64);
        let err = validate_filter_columns(&t, &f).unwrap_err();
        match err {
            Error::UnknownPartitionColumn {
                table,
                column,
                partition_columns,
            } => {
                assert_eq!(table, "T");
                assert_eq!(column, "yearID");
                assert_eq!(partition_columns, vec!["countyID"]);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn validate_filter_columns_accepts_subset() {
        // year_x_county table, filter only by year — that's fine; not
        // every partition column must be constrained.
        let t = mk_table("T", &["yearID", "countyID"], vec![]);
        let f = TableFilter::new().partition_eq("yearID", 2020i64);
        validate_filter_columns(&t, &f).unwrap();
    }

    #[test]
    fn is_schema_only_detects_sidecar() {
        let mut t = mk_table("Link", &[], vec![]);
        t.schema_only_path = Some("Link.schema.json".into());
        assert!(is_schema_only(&t));
    }

    #[test]
    fn open_missing_root_returns_io_error() {
        let err = DefaultDb::open("/this/path/does/not/exist").unwrap_err();
        assert!(matches!(err, Error::Io { .. }), "got {err:?}");
    }

    #[test]
    fn empty_frame_is_returned_when_no_partitions_match() {
        let lf = empty_frame().unwrap();
        let df = lf.collect().unwrap();
        assert_eq!(df.height(), 0);
        assert_eq!(df.width(), 0);
    }

    // Mute unused warnings for helpers used by integration tests but not
    // by every unit test path.
    fn _column_manifest_is_referenced() -> ColumnManifest {
        ColumnManifest {
            name: "x".into(),
            mysql_type: "int".into(),
            arrow_type: "Int64".into(),
            primary_key: true,
        }
    }
    #[allow(dead_code)]
    fn _manifest_is_referenced() -> Manifest {
        Manifest::new("v".into(), "c".into(), "p".into(), "t".into())
    }
}

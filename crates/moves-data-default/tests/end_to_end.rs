//! End-to-end tests against a fixture default-DB tree produced by the
//! companion `moves-default-db-convert` crate.
//!
//! The fixture is built fresh per test via the converter's public API so
//! the reader exercises the real on-disk format — manifest, monolithic
//! Parquet, partitioned Parquet, and a schema-only sidecar.

use std::path::{Path, PathBuf};

use moves_data_default::{DefaultDb, Error, TableFilter};
use moves_default_db_convert::{convert, ConvertOptions};
use polars::prelude::*;

fn write(path: &Path, body: &[u8]) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, body).unwrap();
}

fn build_fixture() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let tsv_dir = dir.path().join("dump");
    let out_dir = dir.path().join("out");
    let plan_path = dir.path().join("tables.json");

    // Plan covers all three pruning-relevant shapes: monolithic
    // (SourceUseType), schema-only (SHO), county-partitioned (Surrogate),
    // year_x_county-partitioned (Coverage), and model_year-partitioned
    // (EmRate). Surrogate exercises the stateID PK fallback so the
    // reader's pruning is checked against the converter's actual path
    // labels.
    let plan = br#"{
        "schema_version": "moves-default-db-schema/v1",
        "moves_commit": "deadbeefcafe",
        "sources": {},
        "table_count": 5,
        "tables": [
            {
                "name": "SourceUseType",
                "primary_key": ["sourceTypeID"],
                "columns": [
                    {"name": "sourceTypeID", "type": "smallint"},
                    {"name": "HPMSVtypeID", "type": "smallint"},
                    {"name": "sourceTypeName", "type": "varchar"}
                ],
                "indexes": [],
                "estimated_rows_upper_bound": 13,
                "size_bucket": "tiny",
                "filter_columns": [],
                "partition": {"strategy": "monolithic", "rationale": "lookup"}
            },
            {
                "name": "SHO",
                "primary_key": ["hourDayID", "linkID"],
                "columns": [
                    {"name": "hourDayID", "type": "smallint"},
                    {"name": "linkID", "type": "int"},
                    {"name": "sho", "type": "double"}
                ],
                "indexes": [],
                "estimated_rows_upper_bound": 0,
                "size_bucket": "empty",
                "filter_columns": [],
                "partition": {"strategy": "schema_only", "rationale": "empty"}
            },
            {
                "name": "Surrogate",
                "primary_key": ["stateID", "metric"],
                "columns": [
                    {"name": "stateID", "type": "smallint"},
                    {"name": "metric", "type": "varchar"},
                    {"name": "value", "type": "double"}
                ],
                "indexes": [],
                "estimated_rows_upper_bound": 1000000,
                "size_bucket": "large",
                "filter_columns": [],
                "partition": {"strategy": "county", "rationale": ""}
            },
            {
                "name": "Coverage",
                "primary_key": ["countyID", "yearID"],
                "columns": [
                    {"name": "countyID", "type": "int"},
                    {"name": "yearID", "type": "smallint"},
                    {"name": "factor", "type": "double"}
                ],
                "indexes": [],
                "estimated_rows_upper_bound": 1000000,
                "size_bucket": "large",
                "filter_columns": [],
                "partition": {"strategy": "year_x_county", "rationale": ""}
            },
            {
                "name": "EmRate",
                "primary_key": ["modelYearID", "scc"],
                "columns": [
                    {"name": "modelYearID", "type": "smallint"},
                    {"name": "scc", "type": "varchar"},
                    {"name": "rate", "type": "double"}
                ],
                "indexes": [],
                "estimated_rows_upper_bound": 1000000,
                "size_bucket": "large",
                "filter_columns": [],
                "partition": {"strategy": "model_year", "rationale": ""}
            }
        ]
    }"#;
    write(&plan_path, plan);

    // SourceUseType: three rows covering the three columns.
    write(
        &tsv_dir.join("SourceUseType.schema.tsv"),
        b"sourceTypeID\tsmallint\tPRI\nHPMSVtypeID\tsmallint\t\nsourceTypeName\tvarchar\t\n",
    );
    write(
        &tsv_dir.join("SourceUseType.tsv"),
        b"11\t10\tMotorcycle\n21\t25\tPassenger Car\n62\t60\tCombination Long-haul Truck\n",
    );

    // SHO: schema_only, no data file needed (schema can come from synth
    // if no TSV pair is shipped). Provide schema only so the converter
    // writes the sidecar.
    write(
        &tsv_dir.join("SHO.schema.tsv"),
        b"hourDayID\tsmallint\tPRI\nlinkID\tint\tPRI\nsho\tdouble\t\n",
    );

    // Surrogate: 3 partitions (06, 17, 36), 2 rows each — exercises the
    // state= label and partition pruning.
    write(
        &tsv_dir.join("Surrogate.schema.tsv"),
        b"stateID\tsmallint\tPRI\nmetric\tvarchar\tPRI\nvalue\tdouble\t\n",
    );
    write(
        &tsv_dir.join("Surrogate.tsv"),
        b"6\talpha\t1.5\n6\tbeta\t2.5\n17\talpha\t3.5\n17\tbeta\t4.5\n36\talpha\t5.5\n36\tbeta\t6.5\n",
    );

    // Coverage: 4 partitions (year_x_county). 2020/1, 2020/2, 2021/1, 2025/2.
    write(
        &tsv_dir.join("Coverage.schema.tsv"),
        b"countyID\tint\tPRI\nyearID\tsmallint\tPRI\nfactor\tdouble\t\n",
    );
    write(
        &tsv_dir.join("Coverage.tsv"),
        b"1\t2020\t1.0\n2\t2020\t2.0\n1\t2021\t3.0\n2\t2025\t4.0\n",
    );

    // EmRate: model_year-partitioned. 3 years.
    write(
        &tsv_dir.join("EmRate.schema.tsv"),
        b"modelYearID\tsmallint\tPRI\nscc\tvarchar\tPRI\nrate\tdouble\t\n",
    );
    write(
        &tsv_dir.join("EmRate.tsv"),
        b"1990\t2202000000\t10.0\n2000\t2202000000\t20.0\n2024\t2202000000\t40.0\n",
    );

    let opts = ConvertOptions {
        tsv_dir,
        plan_path,
        output_root: out_dir.clone(),
        moves_db_version: "movesdb20991231".into(),
        generated_at_utc: Some("1970-01-01T00:00:00Z".into()),
        require_every_table: true,
    };
    let (_manifest, report) = convert(&opts).unwrap();
    assert!(
        report.warnings.is_empty(),
        "converter emitted warnings on the fixture: {:?}",
        report.warnings
    );

    (dir, out_dir)
}

fn collect_lazy(lf: LazyFrame) -> DataFrame {
    lf.collect().unwrap()
}

#[test]
fn open_reads_manifest_metadata() {
    let (_tmp, out_dir) = build_fixture();
    let db = DefaultDb::open(&out_dir).unwrap();
    assert_eq!(db.db_version(), "movesdb20991231");
    // 5 tables — including the schema-only SHO sidecar.
    assert_eq!(db.tables().count(), 5);
}

#[test]
fn scan_monolithic_returns_all_rows() {
    let (_tmp, out_dir) = build_fixture();
    let db = DefaultDb::open(&out_dir).unwrap();
    let df = collect_lazy(db.scan("SourceUseType", &TableFilter::new()).unwrap());
    assert_eq!(df.height(), 3);
    let names: Vec<String> = df
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert!(names.contains(&"sourceTypeID".to_string()));
    assert!(names.contains(&"sourceTypeName".to_string()));
}

#[test]
fn typed_source_use_type_returns_materialized_dataframe() {
    use moves_data_default::typed::source_use_type::{SOURCE_TYPE_ID, SOURCE_TYPE_NAME};
    let (_tmp, out_dir) = build_fixture();
    let db = DefaultDb::open(&out_dir).unwrap();
    let df = db.source_use_type().unwrap();
    assert_eq!(df.height(), 3);
    let ids = df.column(SOURCE_TYPE_ID).unwrap().i64().unwrap();
    let mut ids: Vec<i64> = ids.into_iter().flatten().collect();
    ids.sort();
    assert_eq!(ids, vec![11, 21, 62]);
    // Name column is Utf8 / String.
    assert!(df.column(SOURCE_TYPE_NAME).unwrap().str().is_ok());
}

#[test]
fn case_insensitive_table_lookup() {
    let (_tmp, out_dir) = build_fixture();
    let db = DefaultDb::open(&out_dir).unwrap();
    // Mixed case + lowercase both resolve to the same Parquet.
    let upper = collect_lazy(db.scan("SourceUseType", &TableFilter::new()).unwrap());
    let lower = collect_lazy(db.scan("sourceusetype", &TableFilter::new()).unwrap());
    assert_eq!(upper.height(), lower.height());
}

#[test]
fn scan_partitioned_county_prunes_by_state_label() {
    let (_tmp, out_dir) = build_fixture();
    let db = DefaultDb::open(&out_dir).unwrap();

    // Filter on stateID even though the path label is `state=` — the
    // reader matches against partition_columns (the SQL column name),
    // which is `stateID`.
    let filter = TableFilter::new().partition_eq("stateID", 17i64);
    let df = collect_lazy(db.scan("Surrogate", &filter).unwrap());
    assert_eq!(df.height(), 2, "expected 2 rows for stateID=17, got {df}");
    let states = df.column("stateID").unwrap().i64().unwrap();
    for v in states {
        assert_eq!(v.unwrap(), 17);
    }
}

#[test]
fn scan_partitioned_unfiltered_returns_all_partitions() {
    let (_tmp, out_dir) = build_fixture();
    let db = DefaultDb::open(&out_dir).unwrap();
    let df = collect_lazy(db.scan("Surrogate", &TableFilter::new()).unwrap());
    assert_eq!(df.height(), 6);
}

#[test]
fn scan_year_x_county_with_two_predicates() {
    let (_tmp, out_dir) = build_fixture();
    let db = DefaultDb::open(&out_dir).unwrap();
    let filter = TableFilter::new()
        .partition_eq("countyID", 2i64)
        .partition_in("yearID", [2020i64, 2025]);
    let df = collect_lazy(db.scan("Coverage", &filter).unwrap());
    // Two partitions match: year=2020/county=2 and year=2025/county=2.
    assert_eq!(df.height(), 2);
    let counties = df.column("countyID").unwrap().i64().unwrap();
    for v in counties {
        assert_eq!(v.unwrap(), 2);
    }
}

#[test]
fn scan_year_x_county_partition_pruning_excludes_other_files() {
    let (_tmp, out_dir) = build_fixture();
    let db = DefaultDb::open(&out_dir).unwrap();
    // Asking for a year that isn't present yields zero rows but doesn't
    // error — pruning excluded every file before any disk read.
    let filter = TableFilter::new().partition_in("yearID", [1900i64]);
    let df = collect_lazy(db.scan("Coverage", &filter).unwrap());
    assert_eq!(df.height(), 0);
}

#[test]
fn scan_model_year_filters_to_single_year() {
    let (_tmp, out_dir) = build_fixture();
    let db = DefaultDb::open(&out_dir).unwrap();
    let filter = TableFilter::new().partition_eq("modelYearID", 2000i64);
    let df = collect_lazy(db.scan("EmRate", &filter).unwrap());
    assert_eq!(df.height(), 1);
    let mys = df.column("modelYearID").unwrap().i64().unwrap();
    assert_eq!(mys.get(0), Some(2000));
}

#[test]
fn scan_rejects_unknown_table() {
    let (_tmp, out_dir) = build_fixture();
    let db = DefaultDb::open(&out_dir).unwrap();
    // LazyFrame doesn't implement Debug, so unwrap_err() can't render
    // the Ok branch — match on the Result manually instead.
    let err = match db.scan("DoesNotExist", &TableFilter::new()) {
        Ok(_) => panic!("expected UnknownTable error"),
        Err(e) => e,
    };
    assert!(matches!(err, Error::UnknownTable(name) if name == "DoesNotExist"));
}

#[test]
fn scan_rejects_schema_only_table() {
    let (_tmp, out_dir) = build_fixture();
    let db = DefaultDb::open(&out_dir).unwrap();
    let err = match db.scan("SHO", &TableFilter::new()) {
        Ok(_) => panic!("expected SchemaOnly error"),
        Err(e) => e,
    };
    match err {
        Error::SchemaOnly { table } => assert_eq!(table, "SHO"),
        other => panic!("got {other:?}"),
    }
}

#[test]
fn scan_rejects_filter_on_unknown_partition_column() {
    let (_tmp, out_dir) = build_fixture();
    let db = DefaultDb::open(&out_dir).unwrap();
    // Surrogate is partitioned on stateID; asking to prune by countyID
    // is a programming error, not a silent no-op.
    let filter = TableFilter::new().partition_eq("countyID", 17i64);
    let err = match db.scan("Surrogate", &filter) {
        Ok(_) => panic!("expected UnknownPartitionColumn error"),
        Err(e) => e,
    };
    match err {
        Error::UnknownPartitionColumn {
            table,
            column,
            partition_columns,
        } => {
            assert_eq!(table, "Surrogate");
            assert_eq!(column, "countyID");
            assert_eq!(partition_columns, vec!["stateID"]);
        }
        other => panic!("got {other:?}"),
    }
}

#[test]
fn schema_sidecar_loads_for_schema_only_table() {
    let (_tmp, out_dir) = build_fixture();
    let db = DefaultDb::open(&out_dir).unwrap();
    let sidecar = db.schema_sidecar("SHO").unwrap().expect("SHO sidecar");
    assert_eq!(sidecar.name, "SHO");
    let names: Vec<&str> = sidecar.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, vec!["hourDayID", "linkID", "sho"]);
    assert_eq!(sidecar.primary_key, vec!["hourDayID", "linkID"]);
}

#[test]
fn schema_sidecar_returns_none_for_data_table() {
    let (_tmp, out_dir) = build_fixture();
    let db = DefaultDb::open(&out_dir).unwrap();
    assert!(db.schema_sidecar("SourceUseType").unwrap().is_none());
}

#[test]
fn scan_predicate_on_lazyframe_composes_with_partition_pruning() {
    // Demonstrates the recommended pattern: partition pruning via
    // `TableFilter`, column-level filtering via Polars expressions.
    let (_tmp, out_dir) = build_fixture();
    let db = DefaultDb::open(&out_dir).unwrap();
    let filter = TableFilter::new().partition_eq("countyID", 2i64);
    let lf = db.scan("Coverage", &filter).unwrap();
    let df = lf.filter(col("yearID").eq(lit(2025))).collect().unwrap();
    assert_eq!(df.height(), 1);
    let factors = df.column("factor").unwrap().f64().unwrap();
    assert_eq!(factors.get(0), Some(4.0));
}

#[test]
fn scan_returns_consistent_row_count_with_manifest() {
    // Sanity: the manifest's row_count equals what we observe via the
    // reader. Guards against accidental joins/duplicates from concat.
    let (_tmp, out_dir) = build_fixture();
    let db = DefaultDb::open(&out_dir).unwrap();
    for tbl in db.tables() {
        if tbl.schema_only_path.is_some() {
            continue;
        }
        let df = collect_lazy(db.scan(&tbl.name, &TableFilter::new()).unwrap());
        assert_eq!(
            df.height() as u64,
            tbl.row_count,
            "row-count mismatch for {}",
            tbl.name
        );
    }
}

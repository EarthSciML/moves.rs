//! Parquet round-trip determinism on the DataFrame boundary (Task b2-t4).
//!
//! Verifies that `Vec<ShoRow> → IntoDataFrame → write_parquet → read_parquet
//! → iter_typed::<ShoRow>` is lossless, and that two writes of the same
//! [`DataFrame`] produce byte-identical files.

use std::io::Cursor;

use moves_framework::{
    DataFrameStore, DataFrameStoreParquet, DataFrameStoreTyped, InMemoryStore, IntoDataFrame,
    TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};
use sha2::{Digest as _, Sha256};

// ── Local ShoRow ──────────────────────────────────────────────────────────────
// Mirrors the ShoRow in crate::data::schema_registry unit tests without
// creating a circular dependency. Both implement the same schema so the
// registry validation passes.

#[derive(Debug, Clone, PartialEq)]
struct ShoRow {
    hour_day_id: i32,
    month_id: i32,
    year_id: i32,
    age_id: i32,
    link_id: i32,
    source_type_id: i32,
    distance: f64,
}

impl TableRow for ShoRow {
    fn table_name() -> &'static str {
        "SHO"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("linkID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("distance".into(), DataType::Float64),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "hourDayID".into(),
                    rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "linkID".into(),
                    rows.iter().map(|r| r.link_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "distance".into(),
                    rows.iter().map(|r| r.distance).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SHO";
        macro_rules! col_i32 {
            ($col:expr) => {{
                df.column($col)
                    .map_err(|e| moves_framework::Error::RowExtraction {
                        table: t.into(),
                        row: 0,
                        column: $col.into(),
                        message: e.to_string(),
                    })?
                    .i32()
                    .map_err(|e| moves_framework::Error::RowExtraction {
                        table: t.into(),
                        row: 0,
                        column: $col.into(),
                        message: e.to_string(),
                    })?
            }};
        }
        macro_rules! col_f64 {
            ($col:expr) => {{
                df.column($col)
                    .map_err(|e| moves_framework::Error::RowExtraction {
                        table: t.into(),
                        row: 0,
                        column: $col.into(),
                        message: e.to_string(),
                    })?
                    .f64()
                    .map_err(|e| moves_framework::Error::RowExtraction {
                        table: t.into(),
                        row: 0,
                        column: $col.into(),
                        message: e.to_string(),
                    })?
            }};
        }

        let hour_day = col_i32!("hourDayID");
        let month = col_i32!("monthID");
        let year = col_i32!("yearID");
        let age = col_i32!("ageID");
        let link = col_i32!("linkID");
        let src_type = col_i32!("sourceTypeID");
        let dist = col_f64!("distance");
        let n = df.height();

        (0..n)
            .map(|i| {
                let null = |col: &str| moves_framework::Error::RowExtraction {
                    table: t.into(),
                    row: i,
                    column: col.into(),
                    message: "null value".into(),
                };
                Ok(ShoRow {
                    hour_day_id: hour_day.get(i).ok_or_else(|| null("hourDayID"))?,
                    month_id: month.get(i).ok_or_else(|| null("monthID"))?,
                    year_id: year.get(i).ok_or_else(|| null("yearID"))?,
                    age_id: age.get(i).ok_or_else(|| null("ageID"))?,
                    link_id: link.get(i).ok_or_else(|| null("linkID"))?,
                    source_type_id: src_type.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    distance: dist.get(i).ok_or_else(|| null("distance"))?,
                })
            })
            .collect()
    }
}

// ── Fixtures ─────────────────────────────────────────────────────────────────

fn make_rows(n: usize) -> Vec<ShoRow> {
    (0..n)
        .map(|i| ShoRow {
            hour_day_id: (i % 24) as i32 + 1,
            month_id: (i % 12) as i32 + 1,
            year_id: 2020 + (i % 5) as i32,
            age_id: (i % 10) as i32,
            link_id: 1000 + i as i32,
            source_type_id: 21,
            distance: (i as f64) * 1.5 + 0.1,
        })
        .collect()
}

fn sha256_bytes(data: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(data);
    h.finalize().into()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[test]
fn vec_round_trips_element_for_element() {
    let rows = make_rows(100);

 // IntoDataFrame: Vec<ShoRow> → DataFrame
    let df = rows.clone().into_dataframe().expect("into_dataframe");

 // write_parquet: store DataFrame → Parquet bytes
    let mut store = InMemoryStore::new();
    store.insert("SHO", df);
    let mut buf = Vec::new();
    store.write_parquet("SHO", &mut buf).expect("write_parquet");

 // read_parquet: Parquet bytes → DataFrame in store
    let mut store2 = InMemoryStore::new();
    store2
        .read_parquet("SHO", Cursor::new(&buf))
        .expect("read_parquet");

 // iter_typed: DataFrame → Vec<ShoRow>
    let recovered: Vec<ShoRow> = store2.iter_typed("SHO").expect("iter_typed");

    assert_eq!(recovered.len(), rows.len(), "row count must be preserved");
    for (i, (got, want)) in recovered.iter().zip(rows.iter()).enumerate() {
        assert_eq!(got, want, "row {i} must round-trip identically");
    }
}

#[test]
fn two_writes_of_same_dataframe_are_byte_identical() {
    let rows = make_rows(100);
    let df = rows.into_dataframe().expect("into_dataframe");

    let mut store = InMemoryStore::new();
    store.insert("SHO", df);

    let mut buf1 = Vec::new();
    let mut buf2 = Vec::new();
    store.write_parquet("SHO", &mut buf1).expect("write 1");
    store.write_parquet("SHO", &mut buf2).expect("write 2");

    assert_eq!(buf1.len(), buf2.len(), "byte lengths must match");
    assert_eq!(
        sha256_bytes(&buf1),
        sha256_bytes(&buf2),
        "sha256 must match: two writes of the same DataFrame must be byte-identical"
    );
}

//! Wasm32-compatible execution-DB store setup for `moves-wasm`.
//!
//! Provides:
//! * [`parse_bundle_to_store`] — read an Arrow-IPC execution-DB bundle from
//!   bytes (no filesystem).  Uses the `arrow` crate's IPC reader (pure Rust,
//!   wasm32-safe) and converts each table to a polars [`DataFrame`] using
//!   column-by-column extraction.
//! * [`setup_execution_store`] — post-load synthesis: merge variant tables,
//!   synthesise Link + RunSpec* tables, fill meteorology derived columns,
//!   and build the [`GeographyTables`] needed by the engine.
//!
//! The populate helpers are wasm32-compatible translations of the private
//! functions in `moves-cli/src/run.rs`.  All polars operations are
//! polars-core only (no lazy / parquet → no mio chain).

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::DataType as ArrowDT;
use arrow::ipc::reader::FileReader as ArrowFileReader;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::BTreeMap;
use std::io::Cursor;

use moves_framework::{CountyRow, DataFrameStore, GeographyTables, InMemoryStore, LinkRow};
use polars::prelude::{Column, DataFrame, DataType, NamedFrom, Series};

// The post-load execution-store synthesis (merge variants, prune geography,
// synthesise Link + RunSpec* tables, fill meteorology, …) now lives in the
// shared `moves_calculators::default_db_setup` module — the single source of
// truth for both this wasm path and the native CLI `build_default_db_store`, so
// the two can no longer drift. Re-exported so existing callers keep using
// `default_db::setup_execution_store`.
pub use moves_calculators::default_db_setup::setup_execution_store;

const BUNDLE_MAGIC: &[u8; 8] = b"MXDB\x00\x00\x00\x01";

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/// Parse an Arrow-IPC execution-DB bundle from raw bytes into an [`InMemoryStore`].
///
/// This is the wasm32 equivalent of `moves_framework::read_execution_bundle`
/// for the browser path — no `std::fs` calls are made.
///
/// Each table in the bundle is stored under its *short name*: the last
/// `__`-separated segment of the full snapshot name, lower-cased.
pub fn parse_bundle_to_store(bundle_bytes: &[u8]) -> Result<InMemoryStore, String> {
    if bundle_bytes.len() < 12 {
        return Err(format!("bundle too short: {} bytes", bundle_bytes.len()));
    }
    if &bundle_bytes[0..8] != BUNDLE_MAGIC {
        return Err("unrecognised bundle magic bytes".to_string());
    }
    let count = u32::from_le_bytes([
        bundle_bytes[8],
        bundle_bytes[9],
        bundle_bytes[10],
        bundle_bytes[11],
    ]) as usize;

    // Parse TOC.
    let mut cursor = 12usize;
    let mut toc: Vec<(String, usize, usize)> = Vec::with_capacity(count);
    for i in 0..count {
        if cursor + 2 > bundle_bytes.len() {
            return Err(format!("TOC entry {i} name_len field truncated"));
        }
        let name_len =
            u16::from_le_bytes([bundle_bytes[cursor], bundle_bytes[cursor + 1]]) as usize;
        cursor += 2;
        if cursor + name_len + 16 > bundle_bytes.len() {
            return Err(format!("TOC entry {i} truncated"));
        }
        let name = std::str::from_utf8(&bundle_bytes[cursor..cursor + name_len])
            .map_err(|_| format!("TOC entry {i} has invalid UTF-8 table name"))?
            .to_string();
        cursor += name_len;
        let offset =
            u64::from_le_bytes(bundle_bytes[cursor..cursor + 8].try_into().unwrap()) as usize;
        let length =
            u64::from_le_bytes(bundle_bytes[cursor + 8..cursor + 16].try_into().unwrap()) as usize;
        cursor += 16;
        toc.push((name, offset, length));
    }

    // Decode tables.
    let mut store = InMemoryStore::new();
    for (full_name, offset, length) in toc {
        let end = offset
            .checked_add(length)
            .ok_or_else(|| format!("overflow in data range for {full_name:?}"))?;
        if end > bundle_bytes.len() {
            return Err(format!("data for {full_name:?} extends beyond bundle end"));
        }
        let ipc_bytes = &bundle_bytes[offset..end];

        let df = ipc_bytes_to_polars_df(ipc_bytes)
            .map_err(|e| format!("converting {full_name:?}: {e}"))?;

        let short_name = full_name
            .rsplit("__")
            .next()
            .unwrap_or(&full_name)
            .to_ascii_lowercase();

        store.insert(short_name, df);
    }
    Ok(store)
}

/// Build [`GeographyTables`] from `Link` and `County` tables in the store.
pub fn load_geography_from_store(store: &InMemoryStore) -> Result<GeographyTables, String> {
    let cast_i32 = |df: &DataFrame, name: &str| -> Result<polars::prelude::Column, String> {
        df.column(name)
            .and_then(|c| c.cast(&DataType::Int32))
            .map_err(|e| format!("{name}: {e}"))
    };

    let links: Vec<LinkRow> = if let Some(arc_df) = store.get("link") {
        let df = &*arc_df;
        let link_id_s = cast_i32(df, "linkID")?;
        let county_id_s = cast_i32(df, "countyID")?;
        let zone_id_s = cast_i32(df, "zoneID")?;
        let road_type_id_s = cast_i32(df, "roadTypeID")?;
        let lids = link_id_s.i32().map_err(|e| format!("{e}"))?;
        let cids = county_id_s.i32().map_err(|e| format!("{e}"))?;
        let zids = zone_id_s.i32().map_err(|e| format!("{e}"))?;
        let rtids = road_type_id_s.i32().map_err(|e| format!("{e}"))?;

        let county_state: std::collections::HashMap<i32, i32> =
            if let Some(arc) = store.get("county") {
                let cdf = &*arc;
                let cid_s = cast_i32(cdf, "countyID").ok();
                let sid_s = cast_i32(cdf, "stateID").ok();
                match (cid_s, sid_s) {
                    (Some(cs), Some(ss)) => {
                        let cids2 = cs.i32().unwrap();
                        let sids = ss.i32().unwrap();
                        (0..cdf.height())
                            .filter_map(|i| Some((cids2.get(i)?, sids.get(i)?)))
                            .collect()
                    }
                    _ => Default::default(),
                }
            } else {
                Default::default()
            };

        (0..df.height())
            .filter_map(|i| {
                let lid = lids.get(i)? as u32;
                let cid = cids.get(i)? as u32;
                let zid = zids.get(i)? as u32;
                let rtid = rtids.get(i)? as u32;
                let sid = *county_state.get(&(cid as i32))? as u32;
                Some(LinkRow {
                    state_id: sid,
                    county_id: cid,
                    zone_id: zid,
                    link_id: lid,
                    road_type_id: rtid,
                })
            })
            .collect()
    } else {
        Vec::new()
    };

    let counties: Vec<CountyRow> = if let Some(arc_df) = store.get("county") {
        let df = &*arc_df;
        let cid_s = cast_i32(df, "countyID").ok();
        let sid_s = cast_i32(df, "stateID").ok();
        match (cid_s, sid_s) {
            (Some(cs), Some(ss)) => {
                let cids = cs.i32().unwrap();
                let sids = ss.i32().unwrap();
                (0..df.height())
                    .filter_map(|i| {
                        Some(CountyRow {
                            state_id: sids.get(i)? as u32,
                            county_id: cids.get(i)? as u32,
                        })
                    })
                    .collect()
            }
            _ => Vec::new(),
        }
    } else {
        Vec::new()
    };

    Ok(GeographyTables::new(links, counties))
}

// ---------------------------------------------------------------------------
// Partition-file loading (Parquet from Pages)
// ---------------------------------------------------------------------------

/// Parse a Parquet file (from the default-DB Pages tree) into a polars
/// [`DataFrame`].
///
/// Uses `parquet::arrow::arrow_reader` (pure Rust, wasm32-compatible) to
/// produce Arrow `RecordBatch`es, then converts each column via the same
/// typed extraction used by [`ipc_bytes_to_polars_df`].
pub fn parquet_bytes_to_polars_df(parquet_bytes: &[u8]) -> Result<DataFrame, String> {
    let bytes = bytes::Bytes::copy_from_slice(parquet_bytes);
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|e| format!("Parquet open: {e}"))?;
    let schema = builder.schema().clone();
    let n_cols = schema.fields().len();
    let col_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let col_types: Vec<ArrowDT> = schema
        .fields()
        .iter()
        .map(|f| f.data_type().clone())
        .collect();

    let reader = builder
        .build()
        .map_err(|e| format!("Parquet reader build: {e}"))?;

    struct ColumnAccum {
        arrays: Vec<ArrayRef>,
    }
    let mut cols: Vec<ColumnAccum> = (0..n_cols)
        .map(|_| ColumnAccum { arrays: Vec::new() })
        .collect();
    let mut total_rows = 0usize;

    for batch_result in reader {
        let batch = batch_result.map_err(|e| format!("reading Parquet batch: {e}"))?;
        total_rows += batch.num_rows();
        for (i, col) in batch.columns().iter().enumerate() {
            cols[i].arrays.push(col.clone());
        }
    }

    let mut series_vec: Vec<Column> = Vec::with_capacity(n_cols);
    for (col_idx, (name, dt)) in col_names.into_iter().zip(col_types.iter()).enumerate() {
        let s = arrow_arrays_to_polars_series(&name, dt, &cols[col_idx].arrays, total_rows)?;
        series_vec.push(s.into());
    }

    DataFrame::new_infer_height(series_vec)
        .map_err(|e| format!("building DataFrame from Parquet: {e}"))
}

/// Load a set of `(relative_path, parquet_bytes)` pairs into an
/// [`InMemoryStore`].
///
/// Each path must follow the default-DB Pages-tree layout:
/// - Monolithic: `<TableName>.parquet`
/// - County-partitioned: `<TableName>/county=<id>/part.parquet`
/// - Year×county: `<TableName>/year=<y>/county=<id>/part.parquet`
/// - Model-year: `<TableName>/modelYear=<y>/part.parquet`
///
/// The table name is the first path segment (with `.parquet` stripped for
/// monolithic files). Multiple partitions for the same table are vstacked
/// into a single DataFrame. The store uses lowercased keys (matches
/// [`InMemoryStore`]'s internal convention).
pub fn load_partitions_to_store(
    partition_files: &[(String, Vec<u8>)],
) -> Result<InMemoryStore, String> {
    // Group Parquet files by table name.
    let mut by_table: BTreeMap<String, Vec<DataFrame>> = BTreeMap::new();

    for (path, bytes) in partition_files {
        // Table name = first path segment, strip ".parquet" for monolithic files.
        let table_name = path
            .split('/')
            .next()
            .unwrap_or(path.as_str())
            .trim_end_matches(".parquet")
            .to_string();
        if table_name.is_empty() {
            continue;
        }
        let df = parquet_bytes_to_polars_df(bytes)
            .map_err(|e| format!("parsing partition '{path}': {e}"))?;
        by_table.entry(table_name).or_default().push(df);
    }

    let mut store = InMemoryStore::new();
    for (table_name, mut dfs) in by_table {
        if dfs.is_empty() {
            continue;
        }
        let merged = if dfs.len() == 1 {
            dfs.remove(0)
        } else {
            let mut base = dfs[0].clone();
            for extra in &dfs[1..] {
                base = base
                    .vstack(extra)
                    .map_err(|e| format!("vstacking {table_name}: {e}"))?;
            }
            base
        };
        store.insert(table_name, merged);
    }
    Ok(store)
}

// ---------------------------------------------------------------------------
// Bundle / IPC parsing
// ---------------------------------------------------------------------------

/// Decode one Arrow-IPC FILE-format byte slice into a polars [`DataFrame`].
///
/// Uses `arrow::ipc::reader::FileReader` (pure Rust, wasm32-compatible) then
/// converts each column via typed extraction into polars [`Series`].
fn ipc_bytes_to_polars_df(ipc_bytes: &[u8]) -> Result<DataFrame, String> {
    let reader = ArrowFileReader::try_new(Cursor::new(ipc_bytes), None)
        .map_err(|e| format!("IPC parse: {e}"))?;

    let schema = reader.schema();
    let n_cols = schema.fields().len();
    let col_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let col_types: Vec<ArrowDT> = schema
        .fields()
        .iter()
        .map(|f| f.data_type().clone())
        .collect();

    // Accumulate raw values per column across batches.
    struct ColumnAccum {
        arrays: Vec<ArrayRef>,
    }
    let mut cols: Vec<ColumnAccum> = (0..n_cols)
        .map(|_| ColumnAccum { arrays: Vec::new() })
        .collect();
    let mut total_rows = 0usize;

    for batch_result in reader {
        let batch = batch_result.map_err(|e| format!("reading batch: {e}"))?;
        total_rows += batch.num_rows();
        for (i, col) in batch.columns().iter().enumerate() {
            cols[i].arrays.push(col.clone());
        }
    }

    let mut series_vec: Vec<Column> = Vec::with_capacity(n_cols);
    for (col_idx, (name, dt)) in col_names.into_iter().zip(col_types.iter()).enumerate() {
        let arrays = &cols[col_idx].arrays;
        let s = arrow_arrays_to_polars_series(&name, dt, arrays, total_rows)?;
        series_vec.push(s.into());
    }

    DataFrame::new_infer_height(series_vec).map_err(|e| format!("building DataFrame: {e}"))
}

/// Convert a list of same-typed arrow arrays (one per batch) into a polars [`Series`].
fn arrow_arrays_to_polars_series(
    name: &str,
    dt: &ArrowDT,
    arrays: &[ArrayRef],
    _total_rows: usize,
) -> Result<Series, String> {
    // Macro for primitive typed arrays: downcast, iterate with null handling.
    macro_rules! extract_primitive {
        ($ArrowTy:ident, $rust_ty:ty) => {{
            let vals: Vec<Option<$rust_ty>> = arrays
                .iter()
                .flat_map(|a| {
                    let arr = a
                        .as_any()
                        .downcast_ref::<arrow::array::$ArrowTy>()
                        .expect(concat!("downcast to ", stringify!($ArrowTy)));
                    (0..arr.len()).map(move |i| {
                        if arr.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i) as $rust_ty)
                        }
                    })
                })
                .collect();
            Ok(Series::new(name.into(), vals))
        }};
    }

    match dt {
        ArrowDT::Boolean => {
            let vals: Vec<Option<bool>> = arrays
                .iter()
                .flat_map(|a| {
                    let arr = a
                        .as_any()
                        .downcast_ref::<arrow::array::BooleanArray>()
                        .expect("downcast to BooleanArray");
                    (0..arr.len()).map(move |i| {
                        if arr.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i))
                        }
                    })
                })
                .collect();
            Ok(Series::new(name.into(), vals))
        }
        // Smaller integer types are widened to i32 (polars uses i32/i64 for IDs).
        ArrowDT::Int8 => extract_primitive!(Int8Array, i32),
        ArrowDT::Int16 => extract_primitive!(Int16Array, i32),
        ArrowDT::Int32 => extract_primitive!(Int32Array, i32),
        ArrowDT::Int64 => extract_primitive!(Int64Array, i64),
        // Unsigned integer types widened to the next signed type.
        ArrowDT::UInt8 => extract_primitive!(UInt8Array, i32),
        ArrowDT::UInt16 => extract_primitive!(UInt16Array, i32),
        ArrowDT::UInt32 => extract_primitive!(UInt32Array, i64),
        ArrowDT::UInt64 => extract_primitive!(UInt64Array, i64),
        ArrowDT::Float32 => extract_primitive!(Float32Array, f32),
        ArrowDT::Float64 => extract_primitive!(Float64Array, f64),
        ArrowDT::Utf8 => {
            let vals: Vec<Option<String>> = arrays
                .iter()
                .flat_map(|a| {
                    let arr = a
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .unwrap();
                    (0..arr.len()).map(move |i| {
                        if arr.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i).to_string())
                        }
                    })
                })
                .collect();
            Ok(Series::new(name.into(), vals))
        }
        ArrowDT::LargeUtf8 => {
            let vals: Vec<Option<String>> = arrays
                .iter()
                .flat_map(|a| {
                    let arr = a
                        .as_any()
                        .downcast_ref::<arrow::array::LargeStringArray>()
                        .unwrap();
                    (0..arr.len()).map(move |i| {
                        if arr.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i).to_string())
                        }
                    })
                })
                .collect();
            Ok(Series::new(name.into(), vals))
        }
        other => Err(format!(
            "unsupported arrow column type {other:?} for column '{name}'"
        )),
    }
}

// ---------------------------------------------------------------------------
// Store synthesis helpers (polars-core only — wasm32 compatible)
// ---------------------------------------------------------------------------

/// Strip all trailing `_<digits>` segments from a table name.
///
/// E.g. `"baserate_1_2001"` → `"baserate"`, `"baserate"` → `"baserate"`.
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    // Synthesis functions moved to the shared crate; their unit tests stay here.
    use moves_calculators::default_db_setup::{
        build_runspec_tables, merge_store_variants_eager, prune_geographic_tables_to_runspec,
        scope_pollutant_process_model_year_to_runspec,
    };

    // Minimal Arrow-IPC FILE-format bytes for a table with one Int32 column.
    fn make_ipc_bytes(name: &str, values: &[i32]) -> Vec<u8> {
        use arrow::array::Int32Array;
        use arrow::datatypes::{Field, Schema};
        use arrow::ipc::writer::FileWriter as ArrowFileWriter;
        use arrow::record_batch::RecordBatch;

        let schema = Arc::new(Schema::new(vec![Field::new(
            name,
            arrow::datatypes::DataType::Int32,
            false,
        )]));
        let arr = Arc::new(Int32Array::from(values.to_vec())) as ArrayRef;
        let batch = RecordBatch::try_new(schema.clone(), vec![arr]).unwrap();

        let mut buf = Vec::new();
        let mut writer = ArrowFileWriter::try_new(&mut buf, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        buf
    }

    // Build a minimal MXDB bundle containing one table.
    fn make_minimal_bundle(table_name: &str, ipc_bytes: &[u8]) -> Vec<u8> {
        let mut bundle = Vec::new();
        bundle.extend_from_slice(BUNDLE_MAGIC);
        let count: u32 = 1;
        bundle.extend_from_slice(&count.to_le_bytes());

        // TOC entry.
        let name_bytes = table_name.as_bytes();
        let name_len = name_bytes.len() as u16;
        bundle.extend_from_slice(&name_len.to_le_bytes());
        bundle.extend_from_slice(name_bytes);

        // offset comes after the TOC (magic + count + TOC)
        // TOC size = 2 + name_len + 16
        let toc_size: u64 = 12 + 2 + name_len as u64 + 16;
        let offset = toc_size;
        let length = ipc_bytes.len() as u64;
        bundle.extend_from_slice(&offset.to_le_bytes());
        bundle.extend_from_slice(&length.to_le_bytes());

        // Data.
        bundle.extend_from_slice(ipc_bytes);
        bundle
    }

    #[test]
    fn parse_bundle_to_store_round_trips_int32_column() {
        let ipc = make_ipc_bytes("myID", &[10, 20, 30]);
        let bundle = make_minimal_bundle("db__test__mytable", &ipc);

        let store = parse_bundle_to_store(&bundle).expect("parse must succeed");
        assert!(
            store.contains("mytable"),
            "short name 'mytable' must be in store"
        );

        let df = store.get("mytable").unwrap();
        assert_eq!(df.height(), 3);
        let col = df.column("myID").unwrap();
        let vals: Vec<i32> = col.i32().unwrap().into_no_null_iter().collect();
        assert_eq!(vals, vec![10, 20, 30]);
    }

    #[test]
    fn build_runspec_tables_inserts_expected_tables() {
        use moves_runspec::{
            OnroadVehicleSelection, PollutantProcessAssociation, RoadType, RunSpec, Timespan,
        };

        let runspec = RunSpec {
            onroad_vehicle_selections: vec![OnroadVehicleSelection {
                source_type_id: 21,
                fuel_type_id: 1,
                source_type_name: String::new(),
                fuel_type_name: String::new(),
            }],
            pollutant_process_associations: vec![PollutantProcessAssociation {
                pollutant_id: 3,
                pollutant_name: String::new(),
                process_id: 1,
                process_name: String::new(),
            }],
            timespan: Timespan {
                years: vec![2020],
                months: vec![1],
                days: vec![5],
                begin_hour: Some(1),
                end_hour: Some(2),
                aggregate_by: None,
            },
            road_types: vec![RoadType {
                road_type_id: 2,
                road_type_name: String::new(),
                model_combination: None,
            }],
            ..RunSpec::default()
        };

        let mut store = InMemoryStore::new();
        build_runspec_tables(&runspec, &mut store).expect("build must succeed");

        for tname in &[
            "RunSpecSourceType",
            "RunSpecPollutantProcess",
            "RunSpecDay",
            "RunSpecHour",
            "RunSpecHourDay",
            "RunSpecMonth",
            "RunSpecYear",
            "RunSpecRoadType",
            "RunSpecMonthGroup",
            "RunSpecSourceFuelType",
        ] {
            assert!(
                store.contains(tname),
                "RunSpec table '{tname}' must be in store"
            );
        }
    }

    #[test]
    fn merge_store_variants_eager_unions_rows() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{Field, Schema};
        use arrow::ipc::writer::FileWriter as ArrowFileWriter;
        use arrow::record_batch::RecordBatch;

        // Build two variant IPC tables.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "val",
            arrow::datatypes::DataType::Int32,
            false,
        )]));
        let make_df = |vals: &[i32]| {
            let arr = Arc::new(Int32Array::from(vals.to_vec())) as ArrayRef;
            let batch = RecordBatch::try_new(schema.clone(), vec![arr]).unwrap();
            let mut buf = Vec::new();
            let mut w = ArrowFileWriter::try_new(&mut buf, &schema).unwrap();
            w.write(&batch).unwrap();
            w.finish().unwrap();
            ipc_bytes_to_polars_df(&buf).unwrap()
        };

        let mut store = InMemoryStore::new();
        store.insert("baserate_1_2001".to_string(), make_df(&[1, 2]));
        store.insert("baserate_2_2001".to_string(), make_df(&[3, 4]));

        merge_store_variants_eager(&mut store).expect("merge must succeed");
        let merged = store.get("baserate").expect("merged table must exist");
        assert_eq!(merged.height(), 4, "two variants × 2 rows each = 4");
    }

    #[test]
    fn prune_geographic_tables_filters_to_selected_county() {
        use moves_runspec::{GeoKind, GeographicSelection, RunSpec};

        // ZoneMonthHour with two zones (60370 belongs to county 6037, 99990 to
        // county 9999); the run selects only county 6037.
        let zmh = DataFrame::new(
            4,
            vec![
                Series::new("zoneID".into(), &[60370_i32, 60370, 99990, 99990]).into(),
                Series::new("monthID".into(), &[1_i32, 2, 1, 2]).into(),
            ],
        )
        .unwrap();
        let zone = DataFrame::new(
            2,
            vec![
                Series::new("zoneID".into(), &[60370_i32, 99990]).into(),
                Series::new("countyID".into(), &[6037_i32, 9999]).into(),
            ],
        )
        .unwrap();
        let county_year = DataFrame::new(
            2,
            vec![
                Series::new("countyID".into(), &[6037_i32, 9999]).into(),
                Series::new("yearID".into(), &[2020_i32, 2020]).into(),
            ],
        )
        .unwrap();

        let mut store = InMemoryStore::new();
        store.insert("ZoneMonthHour".to_string(), zmh);
        store.insert("Zone".to_string(), zone);
        store.insert("CountyYear".to_string(), county_year);

        let runspec = RunSpec {
            geographic_selections: vec![GeographicSelection {
                kind: GeoKind::County,
                key: 6037,
                description: String::new(),
            }],
            ..RunSpec::default()
        };

        prune_geographic_tables_to_runspec(&runspec, &mut store).expect("prune must succeed");

        assert_eq!(
            store.get("ZoneMonthHour").unwrap().height(),
            2,
            "only county 6037's zone (60370) rows kept"
        );
        assert_eq!(
            store.get("CountyYear").unwrap().height(),
            1,
            "only county 6037 kept"
        );
    }

    #[test]
    fn prune_geographic_tables_is_noop_for_non_county_scope() {
        use moves_runspec::{GeoKind, GeographicSelection, RunSpec};

        let zmh = DataFrame::new(
            2,
            vec![Series::new("zoneID".into(), &[60370_i32, 99990]).into()],
        )
        .unwrap();
        let mut store = InMemoryStore::new();
        store.insert("ZoneMonthHour".to_string(), zmh);

        // State-level selection: zones aren't enumerable here, so leave intact.
        let runspec = RunSpec {
            geographic_selections: vec![GeographicSelection {
                kind: GeoKind::State,
                key: 6,
                description: String::new(),
            }],
            ..RunSpec::default()
        };

        prune_geographic_tables_to_runspec(&runspec, &mut store).expect("prune must succeed");
        assert_eq!(
            store.get("ZoneMonthHour").unwrap().height(),
            2,
            "state scope: ZoneMonthHour untouched"
        );
    }

    #[test]
    fn prune_keeps_full_table_when_no_zone_matches() {
        use moves_runspec::{GeoKind, GeographicSelection, RunSpec};

        // Zone maps the only zone to a different county than the run selects, so
        // the keep-set wouldn't match any ZoneMonthHour row — the guard must
        // leave the table full (correct, if slow) rather than empty it.
        let zmh = DataFrame::new(
            2,
            vec![Series::new("zoneID".into(), &[60370_i32, 60370]).into()],
        )
        .unwrap();
        let zone = DataFrame::new(
            1,
            vec![
                Series::new("zoneID".into(), &[60370_i32]).into(),
                Series::new("countyID".into(), &[6037_i32]).into(),
            ],
        )
        .unwrap();
        let mut store = InMemoryStore::new();
        store.insert("ZoneMonthHour".to_string(), zmh);
        store.insert("Zone".to_string(), zone);

        let runspec = RunSpec {
            geographic_selections: vec![GeographicSelection {
                kind: GeoKind::County,
                key: 1234, // no zone maps to this county
                description: String::new(),
            }],
            ..RunSpec::default()
        };

        prune_geographic_tables_to_runspec(&runspec, &mut store).expect("prune must succeed");
        assert_eq!(
            store.get("ZoneMonthHour").unwrap().height(),
            2,
            "no match → keep full table, never empty it"
        );
    }

    #[test]
    fn scope_ppmy_filters_to_runspec_pol_processes() {
        // PollutantProcessModelYear with rows for 301, 202 (in the run) and 999
        // (not). Scope keeps only the run's pol-process rows.
        let ppmy = DataFrame::new(
            3,
            vec![
                Series::new("polProcessID".into(), &[301_i32, 202, 999]).into(),
                Series::new("modelYearID".into(), &[2020_i32, 2020, 2020]).into(),
            ],
        )
        .unwrap();
        let rspp = DataFrame::new(
            2,
            vec![Series::new("polProcessID".into(), &[301_i32, 202]).into()],
        )
        .unwrap();
        let mut store = InMemoryStore::new();
        store.insert("PollutantProcessModelYear".to_string(), ppmy);
        store.insert("RunSpecPollutantProcess".to_string(), rspp);

        scope_pollutant_process_model_year_to_runspec(&mut store).expect("scope must succeed");

        let kept = store.get("PollutantProcessModelYear").unwrap();
        assert_eq!(kept.height(), 2, "only the run's pol-process rows kept");
        let pps: std::collections::BTreeSet<i32> = kept
            .column("polProcessID")
            .unwrap()
            .i32()
            .unwrap()
            .into_iter()
            .flatten()
            .collect();
        assert_eq!(
            pps,
            [202, 301]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
        );
    }
}

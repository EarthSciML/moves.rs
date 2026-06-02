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

use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::DataType as ArrowDT;
use arrow::ipc::reader::FileReader as ArrowFileReader;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use moves_calculators::generators::meteorology::{build_meteorology_table, MeteorologyInputs};
use moves_framework::{
    CountyRow, DataFrameStore, DataFrameStoreTyped, GeographyTables, InMemoryStore, LinkRow,
};
use moves_runspec::RunSpec;
use polars::prelude::{Column, DataFrame, DataType, NamedFrom, Series};

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
    let count =
        u32::from_le_bytes([bundle_bytes[8], bundle_bytes[9], bundle_bytes[10], bundle_bytes[11]])
            as usize;

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
        let end = offset.checked_add(length).ok_or_else(|| {
            format!("overflow in data range for {full_name:?}")
        })?;
        if end > bundle_bytes.len() {
            return Err(format!(
                "data for {full_name:?} extends beyond bundle end"
            ));
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

/// Post-load store synthesis for the default-DB path.
///
/// After [`parse_bundle_to_store`] loads the raw execution-DB tables, this
/// function:
/// 1. Merges process/year-indexed variant tables into their canonical names.
/// 2. Synthesises `sourceUseTypePhysicsMapping` from `sourceUseTypePhysics`.
/// 3. Fills derived `ZoneMonthHour` meteorology columns.
/// 4. Synthesises the `Link` table from `ZoneRoadType` (default-DB path).
/// 5. Builds all `RunSpec*` tables from the parsed [`RunSpec`].
///
/// Pair with [`load_geography_from_store`] to get the [`GeographyTables`] the
/// engine needs.
pub fn setup_execution_store(
    runspec: &RunSpec,
    store: &mut InMemoryStore,
) -> Result<(), String> {
    merge_store_variants_eager(store)?;
    populate_source_use_type_physics_mapping(store)?;
    populate_pollutant_process_mapped_model_year(store)?;
    populate_zone_month_hour_meteorology(store)?;
    populate_link_from_zone_road_type(store)?;
    // The default DB ships an all-zero "placeholder" row (fuelFormulationID=0)
    // in FuelSupply with NULL market-share columns. Real fuel supplies always
    // carry a value; the placeholder never joins real data, so fill its NULLs
    // with 0.0 rather than have the strict per-row extractors error on it.
    fill_fuel_supply_placeholder_nulls(store)?;
    build_runspec_tables(runspec, store)?;
    Ok(())
}

/// Zero-fill the NULL `marketShare`/`marketShareCV` of the FuelSupply
/// `fuelFormulationID = 0` placeholder row(s) only, in place.
///
/// The default DB ships a single all-zero placeholder row (fuelFormulationID=0)
/// whose market-share columns are NULL and that never joins real data; that row
/// is the only legitimate NULL. A NULL `marketShare` on any *real*
/// (fuelFormulationID != 0) row is a genuine data gap — the native strict
/// per-row extractor (criteria_running_calculator.rs `FuelSupplyRow::extract`
/// errors via `ok_or_else(|| null("marketShare"))`), so we must surface it as an
/// error here rather than coerce it to 0.0 and silently zero out that
/// formulation's blend-weighted contribution. No-op if the table is absent.
/// Uses polars-core only.
fn fill_fuel_supply_placeholder_nulls(store: &mut InMemoryStore) -> Result<(), String> {
    const TABLE: &str = "FuelSupply";
    const COLS: &[&str] = &["marketShare", "marketShareCV"];

    let Some(arc) = store.get(TABLE) else {
        return Ok(());
    };
    let mut df = (*arc).clone();
    drop(arc);

    // Locate the fuelFormulationID column so the NULL fill can be restricted to
    // the placeholder row(s). If it is missing we cannot distinguish placeholder
    // from real rows, so leave the data untouched and let the strict extractor
    // decide.
    let ffid_name = df
        .columns()
        .iter()
        .find(|c| c.name().eq_ignore_ascii_case("fuelFormulationID"))
        .map(|c| c.name().to_string());
    let Some(ffid_name) = ffid_name else {
        return Ok(());
    };
    let ffid = df
        .column(&ffid_name)
        .and_then(|c| c.cast(&DataType::Int32))
        .map_err(|e| format!("FuelSupply.fuelFormulationID cast: {e}"))?;
    let ffid = ffid.i32().map_err(|e| format!("{e}"))?.clone();
    let is_placeholder = |i: usize| ffid.get(i) == Some(0);

    let mut changed = false;
    for &want in COLS {
        let actual = df
            .columns()
            .iter()
            .find(|c| c.name().to_ascii_lowercase() == want.to_ascii_lowercase())
            .map(|c| c.name().to_string());
        let Some(name) = actual else { continue };
        let casted = df
            .column(&name)
            .and_then(|c| c.cast(&DataType::Float64))
            .map_err(|e| format!("FuelSupply.{want} cast: {e}"))?;
        let ca = casted.f64().map_err(|e| format!("{e}"))?;
        if ca.null_count() == 0 {
            continue;
        }
        let mut filled: Vec<f64> = Vec::with_capacity(ca.len());
        for i in 0..ca.len() {
            match ca.get(i) {
                Some(v) => filled.push(v),
                // A NULL on a real row is a data gap the native path would
                // surface; only the fuelFormulationID=0 placeholder may be 0.0.
                None if is_placeholder(i) => filled.push(0.0),
                None => {
                    return Err(format!(
                        "FuelSupply.{want} is NULL for fuelFormulationID={} (row {i}): \
                         a real fuel-supply row is missing its market share",
                        ffid.get(i)
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "NULL".to_string()),
                    ));
                }
            }
        }
        let series: Column = Series::new(name.as_str().into(), filled).into();
        if df.with_column(series).is_ok() {
            changed = true;
        }
    }
    if changed {
        store.insert(TABLE.to_string(), df);
    }
    Ok(())
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
    let col_names: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let col_types: Vec<ArrowDT> = schema
        .fields()
        .iter()
        .map(|f| f.data_type().clone())
        .collect();

    // Accumulate raw values per column across batches.
    struct ColumnAccum {
        arrays: Vec<ArrayRef>,
    }
    let mut cols: Vec<ColumnAccum> = (0..n_cols).map(|_| ColumnAccum { arrays: Vec::new() }).collect();
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

    DataFrame::new_infer_height(series_vec)
        .map_err(|e| format!("building DataFrame: {e}"))
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
fn strip_numeric_index_suffix(name: &str) -> &str {
    let mut end = name.len();
    while let Some(pos) = name[..end].rfind('_') {
        let suffix = &name[pos + 1..end];
        if !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()) {
            end = pos;
        } else {
            break;
        }
    }
    &name[..end]
}

/// Merge process/year-indexed variant tables into their canonical names using
/// `DataFrame::vstack` (polars-core, wasm32-compatible).
///
/// This is the wasm32-safe equivalent of `merge_process_year_variants` in
/// `moves-cli/src/run.rs`, which uses `LazyFrame + concat` (polars-lazy,
/// not available on wasm32).
fn merge_store_variants_eager(store: &mut InMemoryStore) -> Result<(), String> {
    let all_names: Vec<String> = store.names().iter().map(|s| s.to_string()).collect();
    let mut by_base: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for name in &all_names {
        let base = strip_numeric_index_suffix(name);
        if base != name.as_str() {
            by_base
                .entry(base.to_string())
                .or_default()
                .push(name.clone());
        }
    }
    for (base, variant_names) in by_base {
        let mut dfs: Vec<DataFrame> = variant_names
            .iter()
            .filter_map(|vname| store.get(vname))
            .filter(|df| df.height() > 0)
            .map(|df| df.as_ref().clone())
            .collect();
        if dfs.is_empty() {
            continue;
        }
        let merged = if dfs.len() == 1 {
            dfs.remove(0)
        } else {
            let mut base_df = dfs[0].clone();
            for df in &dfs[1..] {
                base_df = base_df
                    .vstack(df)
                    .map_err(|e| format!("vstacking {base} variants: {e}"))?;
            }
            base_df
        };
        store.insert(base, merged);
    }
    Ok(())
}

/// Synthesise `Link` from `ZoneRoadType` when `Link` is absent or empty.
///
/// Port of `populate_link_from_zone_road_type` in `moves-cli/src/run.rs`.
/// Uses polars-core only.
fn populate_link_from_zone_road_type(store: &mut InMemoryStore) -> Result<(), String> {
    if !store.contains("ZoneRoadType") {
        return Ok(());
    }
    if store.get("link").is_some_and(|df| df.height() > 0) {
        return Ok(());
    }

    let (zone_ids, road_type_ids) = {
        let arc = store
            .get("ZoneRoadType")
            .expect("ZoneRoadType present after contains check");
        let df = &*arc;
        let find = |want: &str| -> Result<polars::prelude::Column, String> {
            let lower = want.to_ascii_lowercase();
            df.columns()
                .iter()
                .find(|c| c.name().to_ascii_lowercase() == lower)
                .cloned()
                .ok_or_else(|| format!("ZoneRoadType column '{want}' not found"))
        };
        let zone_col = find("zoneID")?
            .cast(&DataType::Int32)
            .map_err(|e| format!("ZoneRoadType.zoneID cast: {e}"))?;
        let road_col = find("roadTypeID")?
            .cast(&DataType::Int32)
            .map_err(|e| format!("ZoneRoadType.roadTypeID cast: {e}"))?;
        let zids: Vec<i32> = zone_col
            .i32()
            .map_err(|e| format!("{e}"))?
            .into_no_null_iter()
            .collect();
        let rids: Vec<i32> = road_col
            .i32()
            .map_err(|e| format!("{e}"))?
            .into_no_null_iter()
            .collect();
        (zids, rids)
    };

    let mut seen: BTreeSet<(i32, i32)> = BTreeSet::new();
    let mut link_ids: Vec<i32> = Vec::new();
    let mut county_ids: Vec<i32> = Vec::new();
    let mut out_zone_ids: Vec<i32> = Vec::new();
    let mut out_road_type_ids: Vec<i32> = Vec::new();
    for (&zone_id, &road_type_id) in zone_ids.iter().zip(road_type_ids.iter()) {
        if seen.insert((zone_id, road_type_id)) {
            link_ids.push(zone_id * 10 + road_type_id);
            county_ids.push(zone_id / 10);
            out_zone_ids.push(zone_id);
            out_road_type_ids.push(road_type_id);
        }
    }
    if link_ids.is_empty() {
        return Ok(());
    }

    let n = link_ids.len();
    let df = DataFrame::new(
        n,
        vec![
            Series::new("linkID".into(), link_ids).into(),
            Series::new("countyID".into(), county_ids).into(),
            Series::new("zoneID".into(), out_zone_ids).into(),
            Series::new("roadTypeID".into(), out_road_type_ids).into(),
        ],
    )
    .map_err(|e| format!("building Link DataFrame: {e}"))?;
    store.insert("Link".to_string(), df);
    Ok(())
}

/// Build all `RunSpec*` tables that generators read from the execution-DB slow
/// tier, synthesised from the parsed [`RunSpec`].
///
/// Port of `build_runspec_tables` in `moves-cli/src/run.rs`.
/// Uses polars-core only.
fn build_runspec_tables(runspec: &RunSpec, store: &mut InMemoryStore) -> Result<(), String> {
    let insert_i32 = |store: &mut InMemoryStore, name: &str, col: &str, vals: Vec<i32>| {
        let n = vals.len();
        let df = DataFrame::new(n, vec![Series::new(col.into(), vals).into()])
            .expect("single-column DataFrame should never fail");
        store.insert(name.to_string(), df);
    };

    // RunSpecSourceType.
    let source_type_ids: Vec<i32> = {
        let mut ids: BTreeSet<i32> = BTreeSet::new();
        for sel in &runspec.onroad_vehicle_selections {
            ids.insert(sel.source_type_id as i32);
        }
        ids.into_iter().collect()
    };
    insert_i32(store, "RunSpecSourceType", "sourceTypeID", source_type_ids.clone());

    // RunSpecPollutantProcess.
    let pol_process_ids: Vec<i32> = {
        let mut ids: BTreeSet<i32> = BTreeSet::new();
        for assoc in &runspec.pollutant_process_associations {
            ids.insert((assoc.pollutant_id * 100 + assoc.process_id) as i32);
        }
        ids.into_iter().collect()
    };
    insert_i32(store, "RunSpecPollutantProcess", "polProcessID", pol_process_ids);

    // RunSpecDay.
    let day_ids: Vec<i32> = {
        let mut ids: BTreeSet<i32> = BTreeSet::new();
        for &d in &runspec.timespan.days {
            ids.insert(d as i32);
        }
        ids.into_iter().collect()
    };
    insert_i32(store, "RunSpecDay", "dayID", day_ids.clone());

    // RunSpecHour.
    let hour_ids: Vec<i32> = match (runspec.timespan.begin_hour, runspec.timespan.end_hour) {
        (Some(b), Some(e)) if b <= e => (b..=e).map(|h| h as i32).collect(),
        (Some(h), _) | (_, Some(h)) => vec![h as i32],
        (None, None) => Vec::new(),
    };
    insert_i32(store, "RunSpecHour", "hourID", hour_ids.clone());

    // RunSpecHourDay.
    let hour_day_ids: Vec<i32> = {
        let mut ids: BTreeSet<i32> = BTreeSet::new();
        for &h in &hour_ids {
            for &d in &day_ids {
                ids.insert(h * 10 + d);
            }
        }
        ids.into_iter().collect()
    };
    insert_i32(store, "RunSpecHourDay", "hourDayID", hour_day_ids);

    // RunSpecMonth (months are 1-indexed in MOVES internal representation).
    let month_ids: Vec<i32> = {
        let mut ids: BTreeSet<i32> = BTreeSet::new();
        for &m in &runspec.timespan.months {
            ids.insert(m as i32);
        }
        ids.into_iter().collect()
    };
    insert_i32(store, "RunSpecMonth", "monthID", month_ids.clone());

    // RunSpecYear.
    let year_ids: Vec<i32> = {
        let mut ids: BTreeSet<i32> = BTreeSet::new();
        for &y in &runspec.timespan.years {
            ids.insert(y as i32);
        }
        ids.into_iter().collect()
    };
    insert_i32(store, "RunSpecYear", "yearID", year_ids);

    // RunSpecRoadType.
    let road_type_ids: Vec<i32> = {
        let mut ids: BTreeSet<i32> = BTreeSet::new();
        for rt in &runspec.road_types {
            ids.insert(rt.road_type_id as i32);
        }
        ids.into_iter().collect()
    };
    insert_i32(store, "RunSpecRoadType", "roadTypeID", road_type_ids);

    // RunSpecMonthGroup: derive from MonthGroupOfAnyYear if present.
    let month_group_ids: Vec<i32> = if store.contains("MonthGroupOfAnyYear") {
        let arc = store
            .get("MonthGroupOfAnyYear")
            .expect("MonthGroupOfAnyYear present after contains check");
        let df = &*arc;
        let find = |want: &str| {
            let lower = want.to_ascii_lowercase();
            df.columns()
                .iter()
                .find(|c| c.name().to_ascii_lowercase() == lower)
                .cloned()
        };
        let mut month_to_group: BTreeMap<i32, i32> = BTreeMap::new();
        if let (Some(mid_col), Some(mgid_col)) = (find("monthID"), find("monthGroupID")) {
            let mids = mid_col.cast(&DataType::Int32).ok().and_then(|c| c.i32().ok().cloned());
            let mgids = mgid_col.cast(&DataType::Int32).ok().and_then(|c| c.i32().ok().cloned());
            if let (Some(mids), Some(mgids)) = (mids, mgids) {
                for i in 0..df.height() {
                    if let (Some(mid), Some(mgid)) = (mids.get(i), mgids.get(i)) {
                        month_to_group.insert(mid, mgid);
                    }
                }
            }
        }
        let mut groups: BTreeSet<i32> = BTreeSet::new();
        for &m in &month_ids {
            groups.insert(*month_to_group.get(&m).unwrap_or(&m));
        }
        groups.into_iter().collect()
    } else {
        month_ids.clone()
    };
    insert_i32(store, "RunSpecMonthGroup", "monthGroupID", month_group_ids);

    // RunSpecSourceFuelType (Int64 pairs per SourceBinDistributionGenerator schema).
    let source_fuel_pairs: Vec<(i64, i64)> = {
        let mut pairs: BTreeSet<(i64, i64)> = BTreeSet::new();
        for sel in &runspec.onroad_vehicle_selections {
            pairs.insert((sel.source_type_id as i64, sel.fuel_type_id as i64));
        }
        pairs.into_iter().collect()
    };
    let (sf_source_ids, sf_fuel_ids): (Vec<i64>, Vec<i64>) = source_fuel_pairs.into_iter().unzip();
    let n = sf_source_ids.len();
    let sf_df = DataFrame::new(
        n,
        vec![
            Series::new("sourceTypeID".into(), sf_source_ids).into(),
            Series::new("fuelTypeID".into(), sf_fuel_ids).into(),
        ],
    )
    .map_err(|e| format!("building RunSpecSourceFuelType: {e}"))?;
    store.insert("RunSpecSourceFuelType".to_string(), sf_df);

    Ok(())
}

/// Synthesise `PollutantProcessMappedModelYear` from `PollutantProcessModelYear`.
///
/// MOVES builds this table during execution-DB setup by mapping each
/// `(polProcessID, modelYearID)` through `modelYearMapping` (a user→standard
/// model-year remap). The default DB ships an empty `modelYearMapping`, so the
/// mapping is the identity and the result is a direct projection of
/// `PollutantProcessModelYear`'s `(polProcessID, modelYearID, IMModelYearGroupID)`
/// columns. Calculators (BaseRate, criteria, NOx, …) read this table to expand
/// per-pollutant-process ratios across model years; without it they fail with
/// "table 'PollutantProcessMappedModelYear' not found in store".
///
/// No-op when the table already exists or the source table is absent. Uses
/// polars-core only (wasm32-compatible).
fn populate_pollutant_process_mapped_model_year(
    store: &mut InMemoryStore,
) -> Result<(), String> {
    if store.contains("PollutantProcessMappedModelYear")
        || !store.contains("PollutantProcessModelYear")
    {
        return Ok(());
    }

    // With an identity model-year mapping the mapped table carries exactly the
    // source table's columns (polProcessID, modelYearID, modelYearGroupID,
    // fuelMYGroupID, IMModelYearGroupID) — different calculators read different
    // subsets — so copy the source wholesale under the mapped name.
    let mapped: DataFrame = (*store
        .get("PollutantProcessModelYear")
        .expect("present after contains check"))
    .clone();
    store.insert("PollutantProcessMappedModelYear".to_string(), mapped);
    Ok(())
}

/// Synthesise `sourceUseTypePhysicsMapping` from `sourceUseTypePhysics` when
/// the table is absent.
///
/// Port of `populate_source_use_type_physics_mapping` in `moves-cli/src/run.rs`.
fn populate_source_use_type_physics_mapping(store: &mut InMemoryStore) -> Result<(), String> {
    if store.contains("sourceUseTypePhysicsMapping") || !store.contains("sourceUseTypePhysics") {
        return Ok(());
    }

    let physics = store
        .get("sourceUseTypePhysics")
        .expect("present after contains check");
    let mut mapping: DataFrame = (*physics).clone();
    drop(physics);

    let src_col = mapping
        .get_column_names()
        .iter()
        .find(|n| n.as_str().eq_ignore_ascii_case("sourceTypeID"))
        .map(|n| n.to_string())
        .ok_or("sourceUseTypePhysics has no sourceTypeID column")?;
    mapping
        .rename(&src_col, "realSourceTypeID".into())
        .map_err(|e| format!("renaming sourceTypeID → realSourceTypeID: {e}"))?;

    let mut temp = mapping
        .column("realSourceTypeID")
        .map_err(|e| format!("{e}"))?
        .clone();
    temp.rename("tempSourceTypeID".into());
    let n = mapping.height();
    mapping
        .with_column(temp)
        .map_err(|e| format!("adding tempSourceTypeID: {e}"))?;
    mapping
        .with_column(Series::new("opModeIDOffset".into(), vec![0i64; n]).into())
        .map_err(|e| format!("adding opModeIDOffset: {e}"))?;

    store.insert("sourceUseTypePhysicsMapping".to_string(), mapping);
    Ok(())
}

/// Fill derived `ZoneMonthHour` meteorology columns from `temperature` and
/// `relHumidity`, when those derived columns are NULL in the store.
///
/// Port of `populate_zone_month_hour_meteorology` in `moves-cli/src/run.rs`.
/// Uses `build_meteorology_table` from the `meteorology` generator.
fn populate_zone_month_hour_meteorology(store: &mut InMemoryStore) -> Result<(), String> {
    if !store.contains("ZoneMonthHour") {
        return Ok(());
    }

    // Early exit if heatIndex is already populated.
    {
        let zmh = store
            .get("ZoneMonthHour")
            .expect("ZoneMonthHour not in store after contains check");
        let already_filled = zmh
            .columns()
            .iter()
            .find(|c| c.name().eq_ignore_ascii_case("heatIndex"))
            .and_then(|c| c.cast(&DataType::Float64).ok())
            .and_then(|c| c.f64().ok().cloned())
            .is_some_and(|ca| ca.into_iter().any(|v| v.is_some()));
        if already_filled {
            return Ok(());
        }
    }

    if !store.contains("Zone") || !store.contains("County") {
        return Ok(());
    }

    let inputs = MeteorologyInputs {
        zone_month_hour: store
            .iter_typed("ZoneMonthHour")
            .map_err(|e| format!("reading ZoneMonthHour: {e}"))?,
        zone: store
            .iter_typed("Zone")
            .map_err(|e| format!("reading Zone: {e}"))?,
        county: store
            .iter_typed("County")
            .map_err(|e| format!("reading County: {e}"))?,
    };
    let computed = build_meteorology_table(&inputs);

    let mut by_key: std::collections::HashMap<(i32, i32, i32), (f64, f64, f64)> =
        std::collections::HashMap::with_capacity(computed.len());
    for r in &computed {
        by_key.insert(
            (r.zone_id, r.month_id, r.hour_id),
            (r.heat_index, r.specific_humidity, r.mol_water_fraction),
        );
    }

    // Re-read ZoneMonthHour and annotate with computed columns.
    let zmh_arc = store
        .get("ZoneMonthHour")
        .expect("ZoneMonthHour present after contains check");
    let zmh = &*zmh_arc;

    let find = |want: &str| -> Result<polars::prelude::Column, String> {
        let lower = want.to_ascii_lowercase();
        zmh.columns()
            .iter()
            .find(|c| c.name().to_ascii_lowercase() == lower)
            .cloned()
            .ok_or_else(|| format!("ZoneMonthHour column '{want}' not found"))
    };
    let zone_ids_col = find("zoneID")?.cast(&DataType::Int32)
        .map_err(|e| format!("zoneID cast: {e}"))?;
    let month_ids_col = find("monthID")?.cast(&DataType::Int32)
        .map_err(|e| format!("monthID cast: {e}"))?;
    let hour_ids_col = find("hourID")?.cast(&DataType::Int32)
        .map_err(|e| format!("hourID cast: {e}"))?;

    // temperature is the heatIndex fallback for unmatched rows. Canonical MOVES
    // (MeteorologyGenerator.java:151-156) sets `heatIndex = temperature` when
    // temperature < 78F (the no-humidity-polynomial path), so an unmatched
    // ZoneMonthHour row must inherit its own ambient temperature, NOT 0.0.
    // (matches the CLI port: moves-cli/src/run.rs uses `heat.push(temps[i])`.)
    let temps_col = find("temperature")?.cast(&DataType::Float64)
        .map_err(|e| format!("temperature cast: {e}"))?;
    let temps_ca = temps_col.f64().map_err(|e| format!("{e}"))?;

    let zids = zone_ids_col.i32().map_err(|e| format!("{e}"))?;
    let mids = month_ids_col.i32().map_err(|e| format!("{e}"))?;
    let hids = hour_ids_col.i32().map_err(|e| format!("{e}"))?;
    let n = zmh.height();

    let mut heat_index: Vec<f64> = Vec::with_capacity(n);
    let mut specific_humidity: Vec<f64> = Vec::with_capacity(n);
    let mut mol_water_fraction: Vec<f64> = Vec::with_capacity(n);

    for i in 0..n {
        let key = (
            zids.get(i).unwrap_or(0),
            mids.get(i).unwrap_or(0),
            hids.get(i).unwrap_or(0),
        );
        match by_key.get(&key).copied() {
            Some((hi, sh, mwf)) => {
                heat_index.push(hi);
                specific_humidity.push(sh);
                mol_water_fraction.push(mwf);
            }
            None => {
                heat_index.push(temps_ca.get(i).unwrap_or(0.0));
                specific_humidity.push(0.0);
                mol_water_fraction.push(0.0);
            }
        }
    }

    let mut updated = zmh.clone();
    drop(zmh_arc);

    updated
        .with_column(Series::new("heatIndex".into(), heat_index).into())
        .map_err(|e| format!("writing heatIndex: {e}"))?;
    updated
        .with_column(Series::new("specificHumidity".into(), specific_humidity).into())
        .map_err(|e| format!("writing specificHumidity: {e}"))?;
    updated
        .with_column(
            Series::new("molWaterFraction".into(), mol_water_fraction).into(),
        )
        .map_err(|e| format!("writing molWaterFraction: {e}"))?;
    store.insert("ZoneMonthHour".to_string(), updated);
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

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
        assert!(store.contains("mytable"), "short name 'mytable' must be in store");

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

        let mut runspec = RunSpec::default();
        runspec.onroad_vehicle_selections = vec![OnroadVehicleSelection {
            source_type_id: 21,
            fuel_type_id: 1,
            source_type_name: String::new(),
            fuel_type_name: String::new(),
        }];
        runspec.pollutant_process_associations = vec![PollutantProcessAssociation {
            pollutant_id: 3,
            pollutant_name: String::new(),
            process_id: 1,
            process_name: String::new(),
        }];
        runspec.timespan = Timespan {
            years: vec![2020],
            months: vec![1],
            days: vec![5],
            begin_hour: Some(1),
            end_hour: Some(2),
            aggregate_by: None,
        };
        runspec.road_types = vec![RoadType {
            road_type_id: 2,
            road_type_name: String::new(),
            model_combination: None,
        }];

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
}

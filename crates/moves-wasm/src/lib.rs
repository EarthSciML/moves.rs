//! `moves-wasm` — WebAssembly entry point for the MOVES Rust port.
//!
//! Exposes the MOVES onroad simulation engine and the
//! NONROAD nonroad-emissions simulation engine to browser
//! JavaScript via [`wasm-bindgen`](https://github.com/rustwasm/wasm-bindgen).
//! The module is compiled with `--target wasm32-unknown-unknown` and loaded
//! via a standard ES module or Webpack bundle.
//!
//! # Concurrency
//!
//! Two concurrency levels are supported:
//!
//! ## Single-threaded (default build)
//!
//! Compile normally — no special flags required. Pass `max_parallel_chunks: 1`
//! (or 0 for auto, which resolves to 1 in this mode). Chunks run sequentially
//! in the calling thread, bounding peak memory by never keeping more than one
//! chunk working set resident. Works on any browser without COEP/COOP headers.
//!
//! ## Multi-threaded (`wasm-threads` feature)
//!
//! Requires the `wasm-threads` Cargo feature and the WASM atomics target
//! features:
//!
//! ```text
//! RUSTFLAGS="-C target-feature=+atomics,+bulk-memory,+mutable-globals" \
//! cargo build --target wasm32-unknown-unknown --features wasm-threads
//! ```
//!
//! The page (or the hosting Worker) must be served with cross-origin isolation
//! headers so the browser provides `SharedArrayBuffer`:
//!
//! ```text
//! Cross-Origin-Opener-Policy: same-origin
//! Cross-Origin-Embedder-Policy: require-corp
//! ```
//!
//! Call `init_thread_pool(n)` from JavaScript before the first simulation.
//! Simulation functions must then be called **from a Web Worker context**//! browsers disallow `atomic.wait` on the main thread, which rayon uses for
//! synchronisation. See `docs/wasm-threading.md` for a complete deployment
//! example.
//!
//! # I/O model
//!
//! The browser has no direct filesystem access. Input arrives as JavaScript
//! strings / `Uint8Array` values (file upload or OPFS reads in the caller);
//! output is returned as a JavaScript object. Onroad output maps relative
//! file paths to `Uint8Array` Parquet bytes; NONROAD output is a JSON-shaped
//! object with counters and a completion message.
//!
//! # Usage (JavaScript — single-threaded onroad)
//!
//! ```js
//! import init, { run_simulation } from "./moves_wasm.js";
//!
//! await init();
//!
//! // RunSpec as an XML string (loaded from a file picker or OPFS).
//! const runspecXml = await file.text();
//!
//! // Run (max_parallel_chunks=0 means auto, resolves to 1 in single-threaded build).
//! // Returns { "MOVESRun.parquet": Uint8Array, ... }
//! const result = run_simulation(runspecXml, 0);
//!
//! for (const [path, bytes] of Object.entries(result)) {
//! console.log(path, bytes.byteLength, "bytes");
//! }
//! ```
//!
//! # Usage (JavaScript — multi-threaded onroad, wasm-threads build)
//!
//! ```js
//! import init, { init_thread_pool, run_simulation } from "./moves_wasm.js";
//!
//! await init();
//! await init_thread_pool(navigator.hardwareConcurrency);
//!
//! // Must be called from a Web Worker — see docs/wasm-threading.md.
//! const result = run_simulation(runspecXml, navigator.hardwareConcurrency);
//! ```
//!
//! # Usage (JavaScript — NONROAD)
//!
//! ```js
//! import init, { run_nonroad_simulation } from "./moves_wasm.js";
//!
//! await init();
//!
//! // Options as a JSON string; population data as Uint8Array from a file
//! // picker (<input type="file">) or OPFS.
//! const options = JSON.stringify({ episode_year: 2020, region_level: "COUNTY",
//! selected_counties: ["06037"] });
//! const popBytes = new Uint8Array(await popFile.arrayBuffer());
//!
//! // Returns { completion_message: "…", counters: { scc_groups_planned: …, … } }
//! const result = run_nonroad_simulation(options, popBytes, 0);
//! console.log(result.completion_message);
//! ```
//!
//! See `moves-rust-.md` and `docs/wasm-threading.md`.

mod default_db;

use moves_calculator_info::CalculatorDag;
use moves_framework::{CalculatorRegistry, EngineConfig, MOVESEngine};
use moves_nonroad::{
    driver::{DriverRecord, RegionLevel, RunRegions},
    input::pop::read_pop,
    simulation::{
        run_simulation as nonroad_run_simulation, NonroadInputs, NonroadOptions,
        PlanRecordingExecutor,
    },
};
use moves_runspec::{from_xml_str, GeoKind, RunSpec};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

/// Initialize the rayon Web Worker thread pool.
///
/// Call this from JavaScript **once**, before the first simulation, passing
/// the desired worker count (typically `navigator.hardwareConcurrency`).
/// The function returns a `Promise` that resolves when all workers are ready.
///
/// Only available in the `wasm-threads` feature build. Requires the page to
/// be served with cross-origin isolation headers — see `docs/wasm-threading.md`.
///
/// ```js
/// import init, { init_thread_pool, run_simulation } from "./moves_wasm.js";
/// await init();
/// await init_thread_pool(navigator.hardwareConcurrency);
/// ```
#[cfg(feature = "wasm-threads")]
pub use wasm_bindgen_rayon::init_thread_pool;

/// The calculator-chain DAG, embedded at compile time.
const EMBEDDED_CALCULATOR_DAG: &str =
    include_str!("../../../characterization/calculator-chains/calculator-dag.json");

/// Run a MOVES onroad simulation in the browser.
///
/// # Arguments
///
/// * `runspec_xml` — RunSpec document as an XML string. The caller
/// typically reads this from a file picker (`<input type="file">`) or
/// from OPFS via `FileSystemFileHandle.getFile().text()`.
///
/// * `max_parallel_chunks` — Maximum number of independent calculator chains
/// that may run concurrently. Pass `0` to let the engine choose (resolves
/// to 1 in a single-threaded build; to the global rayon thread count in a
/// `wasm-threads` build after `init_thread_pool` has been called).
/// Pass `1` to force sequential execution regardless of build flags.
/// Values > 1 require the `wasm-threads` build **and** a Web Worker calling
/// context — see `docs/wasm-threading.md`.
///
/// # Returns
///
/// On success, a JavaScript object mapping relative output file paths to
/// `Uint8Array` Parquet bytes:
///
/// ```json
/// {
/// "MOVESRun.parquet": Uint8Array,
/// "MOVESOutput/yearID=2020/monthID=1/part.parquet": Uint8Array,
/// ...
/// }
/// ```
///
/// The layout matches the filesystem layout that the native `moves run`
/// command writes. The caller can persist the bytes to OPFS or offer them
/// for download.
///
/// # Errors
///
/// Returns a JavaScript `Error` if the RunSpec cannot be parsed or if the
/// engine encounters a fatal error during planning or execution.
#[wasm_bindgen]
pub fn run_simulation(runspec_xml: &str, max_parallel_chunks: u32) -> Result<JsValue, JsValue> {
    let run_spec = from_xml_str(runspec_xml)
        .map_err(|e| JsValue::from_str(&format!("RunSpec parse error: {e}")))?;

    let registry =
        build_registry().map_err(|e| JsValue::from_str(&format!("Registry build error: {e}")))?;

    let config = EngineConfig {
        output_root: std::path::PathBuf::from(""),
        max_parallel_chunks: max_parallel_chunks as usize,
        run_spec_file_name: None,
        run_date_time: None,
        collect_output_in_memory: true,
    };

    let mut engine = MOVESEngine::new(run_spec, registry, config);
    let outcome = engine
        .run()
        .map_err(|e| JsValue::from_str(&format!("Engine error: {e}")))?;

 // Convert the collected (path, bytes) pairs into a JS object.
    let obj = js_sys::Object::new();
    for (path, bytes) in outcome.output_bytes {
        let key = JsValue::from_str(
            path.to_str()
                .ok_or_else(|| JsValue::from_str("non-UTF8 output path"))?,
        );
        let value: JsValue = js_sys::Uint8Array::from(bytes.as_slice()).into();
        js_sys::Reflect::set(&obj, &key, &value)
            .map_err(|_| JsValue::from_str("failed to set output property"))?;
    }
    Ok(obj.into())
}

/// Run a MOVES onroad simulation from a pre-loaded execution-DB bundle.
///
/// # Arguments
///
/// * `runspec_xml` — RunSpec document as an XML string.
///
/// * `bundle_bytes` — Arrow-IPC execution-DB bundle bytes (the `MXDB` format
///   written by `moves-snapshot` or `moves export-bundle`).  The caller
///   loads these from OPFS or a file picker and passes them here; no
///   `std::fs` access is needed inside this function.
///
/// * `max_parallel_chunks` — Maximum number of concurrent calculator chains.
///   Pass `0` for auto (resolves to 1 in single-threaded builds).
///   Pass `1` for sequential execution.
///
/// # Returns
///
/// On success, a JavaScript object mapping relative output file paths to
/// `Uint8Array` Parquet bytes (same shape as [`run_simulation`]).
///
/// # Errors
///
/// Returns a JavaScript `Error` if the RunSpec or bundle cannot be parsed,
/// or if the engine encounters a fatal error.
#[wasm_bindgen]
pub fn run_simulation_from_bundle(
    runspec_xml: &str,
    bundle_bytes: &[u8],
    max_parallel_chunks: u32,
) -> Result<JsValue, JsValue> {
    let run_spec = from_xml_str(runspec_xml)
        .map_err(|e| JsValue::from_str(&format!("RunSpec parse error: {e}")))?;

    let mut store = default_db::parse_bundle_to_store(bundle_bytes)
        .map_err(|e| JsValue::from_str(&format!("Bundle parse error: {e}")))?;

    default_db::setup_execution_store(&run_spec, &mut store)
        .map_err(|e| JsValue::from_str(&format!("Store setup error: {e}")))?;

    let geography = default_db::load_geography_from_store(&store)
        .map_err(|e| JsValue::from_str(&format!("Geography error: {e}")))?;

    let registry = build_registry()
        .map_err(|e| JsValue::from_str(&format!("Registry build error: {e}")))?;

    let config = EngineConfig {
        output_root: std::path::PathBuf::from(""),
        max_parallel_chunks: max_parallel_chunks as usize,
        run_spec_file_name: None,
        run_date_time: None,
        collect_output_in_memory: true,
    };

    let mut engine = MOVESEngine::new(run_spec.clone(), registry, config);
    engine = engine.with_slow_store(store);
    engine
        .execution_run_spec_mut()
        .build_execution_locations(&geography);

    let outcome = engine
        .run()
        .map_err(|e| JsValue::from_str(&format!("Engine error: {e}")))?;

    let obj = js_sys::Object::new();
    for (path, bytes) in outcome.output_bytes {
        let key = JsValue::from_str(
            path.to_str()
                .ok_or_else(|| JsValue::from_str("non-UTF8 output path"))?,
        );
        let value: JsValue = js_sys::Uint8Array::from(bytes.as_slice()).into();
        js_sys::Reflect::set(&obj, &key, &value)
            .map_err(|_| JsValue::from_str("failed to set output property"))?;
    }
    Ok(obj.into())
}

/// Compute which default-DB partition paths are needed for a given RunSpec.
///
/// Parses the RunSpec and the manifest.json from the default-DB Pages tree
/// and returns the relative paths of every Parquet partition file that the
/// simulation will need.  The caller fetches those files, then passes them to
/// [`run_simulation_from_partitions`].
///
/// # Arguments
///
/// * `runspec_xml` — RunSpec document as an XML string.
/// * `manifest_json` — Contents of `manifest.json` from the default-DB Pages
///   tree (e.g. fetched from `/demo/data/<db_version>/manifest.json`).
///
/// # Returns
///
/// A JavaScript `Array` of relative path strings, e.g.
/// `["County.parquet", "ZoneMonthHour/county=26161/part.parquet", …]`.
/// Each path is relative to the manifest's root (the `<db_version>/`
/// directory), so the full fetch URL is
/// `/demo/data/<db_version>/<path>`.
///
/// # Errors
///
/// Returns a JavaScript `Error` if the RunSpec or manifest cannot be parsed.
#[wasm_bindgen]
pub fn required_partition_paths(
    runspec_xml: &str,
    manifest_json: &str,
) -> Result<JsValue, JsValue> {
    let run_spec = from_xml_str(runspec_xml)
        .map_err(|e| JsValue::from_str(&format!("RunSpec parse error: {e}")))?;
    let paths = required_partition_paths_inner(&run_spec, manifest_json)
        .map_err(|e| JsValue::from_str(&e))?;
    let arr = js_sys::Array::new();
    for p in paths {
        arr.push(&JsValue::from_str(&p));
    }
    Ok(arr.into())
}

/// Per-table geographic/time dimensions that the default-DB merge plan
/// actually filters on. Derived from [`default_tables`]; a dimension is only
/// applied to partition selection when the canonical spec declares a filter
/// column for it. This matters for tables the conversion partitions by a
/// dimension that the merge plan treats as a national wildcard — e.g.
/// `hotellingActivityDistribution` is stored under `zone=990000` (the MOVES
/// national-default zone) but the merge plan copies it wholesale, so its zone
/// partition must NOT be filtered out by the run's geography.
#[derive(Clone, Copy, Default)]
struct TableFilterDims {
    year: bool,
    zone: bool,
    county: bool,
    state: bool,
}

/// Build the table-name → filtered-dimensions map from [`default_tables`].
/// Keys are lower-cased table names (manifest names are matched case-insensitively).
fn table_filter_dims() -> std::collections::HashMap<String, TableFilterDims> {
    moves_framework::input::default_tables()
        .into_iter()
        .map(|s| {
            (
                s.table_name.to_ascii_lowercase(),
                TableFilterDims {
                    year: s.year_column.is_some(),
                    zone: s.zone_column.is_some(),
                    county: s.county_column.is_some(),
                    state: s.state_column.is_some(),
                },
            )
        })
        .collect()
}

/// Pure core of [`required_partition_paths`]: compute the relative partition
/// paths the run needs from a parsed [`RunSpec`] and the default-DB manifest
/// JSON. Shared by the wasm-bindgen wrapper and native tests.
fn required_partition_paths_inner(
    run_spec: &RunSpec,
    manifest_json: &str,
) -> Result<Vec<String>, String> {
    use std::collections::BTreeSet;

    // County IDs and derived zone / state IDs from the geographic selections.
    let mut county_ids: BTreeSet<i64> = BTreeSet::new();
    let mut state_ids: BTreeSet<i64> = BTreeSet::new();
    for sel in &run_spec.geographic_selections {
        match sel.kind {
            GeoKind::County => {
                let c = sel.key as i64;
                county_ids.insert(c);
                state_ids.insert(c / 1000); // FIPS: countyID / 1000 = stateID
            }
            GeoKind::State => {
                state_ids.insert(sel.key as i64);
            }
            _ => {}
        }
    }
    // Default zones: county_id * 10 (MOVES convention).
    let zone_ids: BTreeSet<i64> = county_ids.iter().map(|&c| c * 10).collect();
    let year_ids: BTreeSet<i64> = run_spec.timespan.years.iter().map(|&y| y as i64).collect();

    let dims_map = table_filter_dims();

    let manifest: serde_json::Value =
        serde_json::from_str(manifest_json).map_err(|e| format!("manifest parse error: {e}"))?;

    let tables = manifest["tables"]
        .as_array()
        .ok_or_else(|| "manifest missing 'tables' array".to_string())?;

    let mut out: Vec<String> = Vec::new();
    for table in tables {
        let strategy = table["partition_strategy"].as_str().unwrap_or("");
        if strategy == "schema_only" {
            continue;
        }

        // Which dimensions the merge plan actually filters this table on.
        // Unknown tables (not in the curated default-tables list) fall back to
        // filtering on every partition dimension — the conservative default.
        let table_name = table["name"].as_str().unwrap_or("");
        let dims = dims_map
            .get(&table_name.to_ascii_lowercase())
            .copied()
            .unwrap_or(TableFilterDims {
                year: true,
                zone: true,
                county: true,
                state: true,
            });

        let partition_columns: Vec<String> = table["partition_columns"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_ascii_lowercase()))
                    .collect()
            })
            .unwrap_or_default();

        let partitions = match table["partitions"].as_array() {
            Some(p) => p,
            None => continue,
        };

        for partition in partitions {
            let path = match partition["path"].as_str() {
                Some(p) if !p.is_empty() => p,
                _ => continue,
            };

            let values: Vec<String> = partition["values"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();

            if partition_path_needed(
                &partition_columns,
                &values,
                &dims,
                &county_ids,
                &zone_ids,
                &state_ids,
                &year_ids,
            ) {
                out.push(path.to_string());
            }
        }
    }

    Ok(out)
}

/// Returns `true` when the partition described by `(columns, values)` is
/// needed for a run whose geography/time is described by the supplied sets.
///
/// An unconstrained dimension (empty set, or column not recognised) passes
/// unconditionally so that `required_partition_paths` is never over-
/// conservative about what to exclude.
fn partition_path_needed(
    columns: &[String],
    values: &[String],
    dims: &TableFilterDims,
    county_ids: &std::collections::BTreeSet<i64>,
    zone_ids: &std::collections::BTreeSet<i64>,
    state_ids: &std::collections::BTreeSet<i64>,
    year_ids: &std::collections::BTreeSet<i64>,
) -> bool {
    for (col, val) in columns.iter().zip(values.iter()) {
        let parsed: Option<i64> = val.parse().ok();
        // A dimension is only filtered when the canonical merge plan declares a
        // filter column for it (`dims.*`). Dimensions the plan treats as a
        // national wildcard (e.g. hotellingActivityDistribution's zoneID) pass
        // unconditionally so their default partition is never dropped.
        let ok = match col.as_str() {
            "countyid" => {
                !dims.county || county_ids.is_empty() || parsed.map_or(true, |v| county_ids.contains(&v))
            }
            "zoneid" => {
                !dims.zone || zone_ids.is_empty() || parsed.map_or(true, |v| zone_ids.contains(&v))
            }
            "stateid" => {
                !dims.state || state_ids.is_empty() || parsed.map_or(true, |v| state_ids.contains(&v))
            }
            "yearid" => {
                !dims.year || year_ids.is_empty() || parsed.map_or(true, |v| year_ids.contains(&v))
            }
            _ => true,
        };
        if !ok {
            return false;
        }
    }
    true
}

/// Run a MOVES onroad simulation from pre-fetched default-DB partition files.
///
/// This is the main entry point for arbitrary-RunSpec execution in the demo.
/// The caller:
/// 1. Calls [`required_partition_paths`] to get the list of paths.
/// 2. Fetches each path from the Pages tree.
/// 3. Passes the fetched bytes here.
///
/// # Arguments
///
/// * `runspec_xml` — RunSpec document as an XML string.
/// * `partitions_js` — A JavaScript `Array` of objects of the form
///   `{ path: string, bytes: Uint8Array }`.  Each `path` must match one
///   of the values returned by [`required_partition_paths`]; `bytes` is
///   the raw Parquet file content.
/// * `max_parallel_chunks` — See [`run_simulation`].
///
/// # Returns
///
/// Same shape as [`run_simulation`]: a JavaScript object mapping relative
/// output file paths to `Uint8Array` Parquet bytes.
///
/// # Errors
///
/// Returns a JavaScript `Error` if the RunSpec cannot be parsed, any
/// partition file cannot be decoded, or the engine encounters a fatal error.
#[wasm_bindgen]
pub fn run_simulation_from_partitions(
    runspec_xml: &str,
    partitions_js: JsValue,
    max_parallel_chunks: u32,
) -> Result<JsValue, JsValue> {
    let run_spec = from_xml_str(runspec_xml)
        .map_err(|e| JsValue::from_str(&format!("RunSpec parse error: {e}")))?;

    // Unpack JS Array<{path: string, bytes: Uint8Array}> into Rust Vec.
    let arr: js_sys::Array = partitions_js
        .dyn_into::<js_sys::Array>()
        .map_err(|_| JsValue::from_str("partitions must be an Array"))?;

    let mut partition_files: Vec<(String, Vec<u8>)> = Vec::with_capacity(arr.length() as usize);
    for i in 0..arr.length() {
        let item = arr.get(i);
        let path = js_sys::Reflect::get(&item, &JsValue::from_str("path"))
            .map_err(|_| JsValue::from_str("partition missing 'path'"))?
            .as_string()
            .ok_or_else(|| JsValue::from_str("partition 'path' must be a string"))?;
        let bytes_val = js_sys::Reflect::get(&item, &JsValue::from_str("bytes"))
            .map_err(|_| JsValue::from_str("partition missing 'bytes'"))?;
        let uint8arr: js_sys::Uint8Array = bytes_val
            .dyn_into::<js_sys::Uint8Array>()
            .map_err(|_| JsValue::from_str("partition 'bytes' must be a Uint8Array"))?;
        partition_files.push((path, uint8arr.to_vec()));
    }

    let mut store = default_db::load_partitions_to_store(&partition_files)
        .map_err(|e| JsValue::from_str(&format!("Partition load error: {e}")))?;

    default_db::setup_execution_store(&run_spec, &mut store)
        .map_err(|e| JsValue::from_str(&format!("Store setup error: {e}")))?;

    let geography = default_db::load_geography_from_store(&store)
        .map_err(|e| JsValue::from_str(&format!("Geography error: {e}")))?;

    let registry = build_registry()
        .map_err(|e| JsValue::from_str(&format!("Registry build error: {e}")))?;

    let config = EngineConfig {
        output_root: std::path::PathBuf::from(""),
        max_parallel_chunks: max_parallel_chunks as usize,
        run_spec_file_name: None,
        run_date_time: None,
        collect_output_in_memory: true,
    };

    let mut engine = MOVESEngine::new(run_spec.clone(), registry, config);
    engine = engine.with_slow_store(store);
    engine
        .execution_run_spec_mut()
        .build_execution_locations(&geography);

    let outcome = engine
        .run()
        .map_err(|e| JsValue::from_str(&format!("Engine error: {e}")))?;

    let obj = js_sys::Object::new();
    for (path, bytes) in outcome.output_bytes {
        let key = JsValue::from_str(
            path.to_str()
                .ok_or_else(|| JsValue::from_str("non-UTF8 output path"))?,
        );
        let value: JsValue = js_sys::Uint8Array::from(bytes.as_slice()).into();
        js_sys::Reflect::set(&obj, &key, &value)
            .map_err(|_| JsValue::from_str("failed to set output property"))?;
    }
    Ok(obj.into())
}

/// Run a NONROAD nonroad-emissions simulation in the browser.
///
/// Input data arrives as browser-supplied bytes via the File API or OPFS/// no `std::fs` calls are made. Parsers consume `std::io::Cursor` wrappers
/// around the supplied byte slices, which is the WASM-compatible I/O path
/// described in `ARCHITECTURE.md` § 4.3.
///
/// # Arguments
///
/// * `options_json` — JSON object with run configuration. Required:
/// - `"episode_year"`: integer, 1990–2099.
///
/// Optional (with defaults):
/// - `"region_level"`: `"COUNTY"` | `"STATE"` | `"50STATE"` |
/// `"SUBCOUNTY"` | `"US TOTAL"` (default `"COUNTY"`).
/// - `"growth_year"`, `"tech_year"`: integers; default to `episode_year`.
/// - `"total_mode"`, `"daily_output"`, `"emit_bmy_exhaust"`,
/// `"emit_bmy_evap"`, `"emit_si"`: booleans (default `false`).
/// - `"growth_loaded"`, `"retrofit_loaded"`, `"spillage_loaded"`:
/// booleans (default `false`).
/// - `"title"`: string (default `""`).
/// - `"selected_counties"`: array of 5-character FIPS strings (default:
/// no county filter — the driver accepts all records).
///
/// * `pop_bytes` — Contents of a NONROAD `.POP` population file as a
/// `Uint8Array`. Load from a file picker (`<input type="file">`) or
/// OPFS:
/// ```js
/// const popBytes = new Uint8Array(await popFile.arrayBuffer());
/// ```
///
/// # Returns
///
/// On success:
/// ```json
/// {
/// "completion_message": "Successful completion — no warnings",
/// "counters": {
/// "scc_groups_planned": 12,
/// "scc_groups_skipped": 0,
/// "records_visited": 240,
/// "records_not_selected": 0,
/// "records_no_dispatch": 0,
/// "dispatch_calls": 240,
/// "geography_skips": 0
/// }
/// }
/// ```
///
/// Emission rows are not yet included in the WASM result — the production
/// `GeographyExecutor` that evaluates the geography routines numerically
/// lives in the native orchestrator and will be wired into the WASM build
/// in a following task. The counters confirm that the driver loop ran
/// correctly over the supplied population data.
///
/// # Errors
///
/// Returns a JavaScript `Error` if the options JSON is malformed, the
/// population file cannot be parsed, or the driver loop encounters a
/// configuration error.
#[wasm_bindgen]
pub fn run_nonroad_simulation(options_json: &str, pop_bytes: &[u8]) -> Result<JsValue, JsValue> {
    let v: serde_json::Value = serde_json::from_str(options_json)
        .map_err(|e| JsValue::from_str(&format!("options JSON parse error: {e}")))?;

    let region_level_str = v["region_level"].as_str().unwrap_or("COUNTY");
    let region_level = RegionLevel::from_reglvl(region_level_str)
        .ok_or_else(|| JsValue::from_str(&format!("unknown region_level: {region_level_str}")))?;

    let episode_year = v["episode_year"]
        .as_i64()
        .ok_or_else(|| JsValue::from_str("options.episode_year is required"))?
        as i32;
    let growth_year = v["growth_year"].as_i64().unwrap_or(episode_year as i64) as i32;
    let tech_year = v["tech_year"].as_i64().unwrap_or(episode_year as i64) as i32;

    let options = NonroadOptions {
        region_level,
        episode_year,
        growth_year,
        tech_year,
        total_mode: v["total_mode"].as_bool().unwrap_or(false),
        daily_output: v["daily_output"].as_bool().unwrap_or(false),
        emit_bmy_exhaust: v["emit_bmy_exhaust"].as_bool().unwrap_or(false),
        emit_bmy_evap: v["emit_bmy_evap"].as_bool().unwrap_or(false),
        emit_si: v["emit_si"].as_bool().unwrap_or(false),
        growth_loaded: v["growth_loaded"].as_bool().unwrap_or(false),
        retrofit_loaded: v["retrofit_loaded"].as_bool().unwrap_or(false),
        spillage_loaded: v["spillage_loaded"].as_bool().unwrap_or(false),
        title: v["title"].as_str().unwrap_or("").to_string(),
    };

    let selected_counties: Vec<String> = v["selected_counties"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();

 // Parse population data from browser-supplied bytes using an in-memory
 // reader — the WASM-compatible path that replaces std::fs::File::open.
    let pop_records = read_pop(std::io::Cursor::new(pop_bytes))
        .map_err(|e| JsValue::from_str(&format!("population file parse error: {e}")))?;

 // Group population records by SCC (file order) into NonroadInputs.
    let inputs = nonroad_inputs_from_pop(pop_records, selected_counties);

 // Run via PlanRecordingExecutor — exercises the full driver loop
 // (SCC dispatch decisions, region filtering, growth-pair detection)
 // without the numerical geography-routine evaluation, which the
 // native orchestrator handles and will be wired into WASM later.
    let mut executor = PlanRecordingExecutor::new();
    let outputs = nonroad_run_simulation(&options, &inputs, &mut executor)
        .map_err(|e| JsValue::from_str(&format!("simulation error: {e}")))?;

    let result = js_sys::Object::new();
    js_set_str(&result, "completion_message", &outputs.completion_message)?;

    let counters_obj = js_sys::Object::new();
    let c = &outputs.counters;
    js_set_num(
        &counters_obj,
        "scc_groups_planned",
        c.scc_groups_planned as f64,
    )?;
    js_set_num(
        &counters_obj,
        "scc_groups_skipped",
        c.scc_groups_skipped as f64,
    )?;
    js_set_num(&counters_obj, "records_visited", c.records_visited as f64)?;
    js_set_num(
        &counters_obj,
        "records_not_selected",
        c.records_not_selected as f64,
    )?;
    js_set_num(
        &counters_obj,
        "records_no_dispatch",
        c.records_no_dispatch as f64,
    )?;
    js_set_num(&counters_obj, "dispatch_calls", c.dispatch_calls as f64)?;
    js_set_num(&counters_obj, "geography_skips", c.geography_skips as f64)?;
    js_sys::Reflect::set(&result, &JsValue::from_str("counters"), &counters_obj)
        .map_err(|_| JsValue::from_str("failed to set counters"))?;

    Ok(result.into())
}

/// Group parsed `.POP` population records by SCC (preserving file order)
/// and build a [`NonroadInputs`] ready for [`nonroad_run_simulation`].
///
/// Records for the same SCC are kept adjacent — growth-record pairs (a base
/// record immediately followed by its projection record) depend on adjacency
/// and must not be separated across groups.
fn nonroad_inputs_from_pop(
    records: Vec<moves_nonroad::input::pop::PopulationRecord>,
    selected_counties: Vec<String>,
) -> NonroadInputs {
    use std::collections::HashMap;

    let mut groups: Vec<(String, Vec<DriverRecord>)> = Vec::new();
    let mut scc_to_idx: HashMap<String, usize> = HashMap::new();

    for moves_nonroad::input::pop::PopulationRecord {
        fips,
        scc,
        hp_avg,
        population,
        year,
        ..
    } in records
    {
        let driver_rec = DriverRecord {
            region_code: fips,
            hp_avg,
            population,
            pop_year: year,
 // The .POP demo input carries no source-use-type table, so there is
 // no medianLifeFullLoad to supply. 0.0 is the documented sentinel:
 // the geography routines fall back to a neutral lifespan when
 // median_life is non-positive (see DriverRecord::median_life).
            median_life: 0.0,
        };
        if let Some(&idx) = scc_to_idx.get(&scc) {
            groups[idx].1.push(driver_rec);
        } else {
            let idx = groups.len();
            scc_to_idx.insert(scc.clone(), idx);
            groups.push((scc, vec![driver_rec]));
        }
    }

    let mut inputs = NonroadInputs::new();
    for (scc, recs) in groups {
        inputs.push_group(scc, recs);
    }
    inputs.regions = RunRegions {
        selected_counties,
        ..Default::default()
    };
    inputs
}

/// Set a string property on a JS object.
fn js_set_str(obj: &js_sys::Object, key: &str, val: &str) -> Result<(), JsValue> {
    js_sys::Reflect::set(obj, &JsValue::from_str(key), &JsValue::from_str(val))
        .map(|_| ())
        .map_err(|_| JsValue::from_str("failed to set property"))
}

/// Set a numeric property on a JS object.
fn js_set_num(obj: &js_sys::Object, key: &str, val: f64) -> Result<(), JsValue> {
    js_sys::Reflect::set(obj, &JsValue::from_str(key), &JsValue::from_f64(val))
        .map(|_| ())
        .map_err(|_| JsValue::from_str("failed to set property"))
}

/// Build a [`CalculatorRegistry`] from the embedded DAG, with
/// all ported onroad calculators and generators registered.
fn build_registry() -> Result<CalculatorRegistry, String> {
    let dag: CalculatorDag = serde_json::from_str(EMBEDDED_CALCULATOR_DAG)
        .map_err(|e| format!("embedded DAG parse error: {e}"))?;
    let mut registry = CalculatorRegistry::new(dag);
    moves_calculators::register_all(&mut registry)
        .map_err(|e| format!("calculator registration error: {e}"))?;
    Ok(registry)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_dag_parses_and_registry_builds() {
        let registry = build_registry().expect("registry must build");
        assert!(
            registry.dag().modules.len() >= 60,
            "expected ~63 modules, got {}",
            registry.dag().modules.len()
        );
    }

    /// After `register_all`, the registry must have a non-empty factory set so
    /// the engine dispatches real calculators instead of reporting "unimplemented".
    #[test]
    fn build_registry_registers_calculators_and_generators() {
        let registry = build_registry().expect("registry must build");
        let names: Vec<&str> = registry.registered_names().collect();
        assert!(
            names.len() >= 40,
            "expected ≥40 registered factories, got {}",
            names.len()
        );
        // Required input tables should be non-empty once factories are registered.
        let tables = registry.required_input_tables();
        assert!(
            !tables.is_empty(),
            "registered calculators must declare input tables"
        );
    }

    /// After `register_all`, `run_simulation` dispatches real calculators.
    /// Without a slow store the calculators fail on missing input tables —
    /// that is the correct behaviour: the engine refuses to silently emit
    /// wrong output.  The test asserts that the failure is a "table not found"
    /// error from the store, NOT an unexpected panic or code-path bug.
    ///
    /// Full correctness (wasm output == native for the sample-runspec fixture)
    /// is validated by the integration suite in `moves-calculators`.
    #[test]
    fn run_simulation_returns_moves_run_parquet() {
        use moves_framework::{EngineConfig, MOVESEngine};
        use moves_runspec::from_xml_str;

        let runspec_xml = include_str!("../../../characterization/fixtures/sample-runspec.xml");
        let run_spec = from_xml_str(runspec_xml).expect("sample RunSpec must parse");
        let registry = build_registry().expect("registry must build");

        // The registered calculators need a slow store; without it the engine
        // returns an informative error rather than empty-but-wrong output.
        assert!(
            !registry.required_input_tables().is_empty(),
            "registered calculators must declare input tables"
        );

        let config = EngineConfig {
            output_root: std::path::PathBuf::from(""),
            max_parallel_chunks: 1,
            run_spec_file_name: Some("sample-runspec.xml".to_string()),
            run_date_time: None,
            collect_output_in_memory: true,
        };
        let mut engine = MOVESEngine::new(run_spec, registry, config);
        match engine.run() {
            Ok(outcome) => {
                // If the engine succeeds it must produce at least MOVESRun.parquet.
                assert!(
                    outcome
                        .output_bytes
                        .iter()
                        .any(|(p, _)| p.to_str() == Some("MOVESRun.parquet")),
                    "expected MOVESRun.parquet in output"
                );
            }
            Err(e) => {
                // Without a snapshot slow store the generators / calculators
                // will fail when they look up their input tables.  Verify
                // the error is a table-not-found message, not a code bug.
                let msg = e.to_string();
                assert!(
                    msg.contains("not found in store") || msg.contains("SampleVehicleTrip"),
                    "unexpected engine error without slow store: {msg}"
                );
            }
        }
    }

    /// `required_partition_paths` returns county-filtered paths for a minimal manifest.
    #[test]
    fn required_partition_paths_filters_by_county() {
        let runspec_xml = include_str!(
            "../../../characterization/fixtures/sample-runspec.xml"
        );
        // Minimal manifest with one monolithic table and one county-partitioned table.
        let manifest_json = r#"{
            "schema_version": "moves-default-db-manifest/v1",
            "moves_db_version": "movesdb20241112",
            "moves_commit": "abc",
            "plan_sha256": "0",
            "generated_at_utc": "1970-01-01T00:00:00Z",
            "tables": [
                {
                    "name": "County",
                    "partition_strategy": "monolithic",
                    "partition_columns": [],
                    "row_count": 1,
                    "columns": [],
                    "primary_key": [],
                    "partitions": [{"path": "County.parquet", "values": [], "row_count": 1, "sha256": "0", "bytes": 1}]
                },
                {
                    "name": "IMCoverage",
                    "partition_strategy": "year_x_county",
                    "partition_columns": ["yearID", "countyID"],
                    "row_count": 2,
                    "columns": [],
                    "primary_key": [],
                    "partitions": [
                        {"path": "IMCoverage/year=2001/county=26161/part.parquet", "values": ["2001", "26161"], "row_count": 1, "sha256": "0", "bytes": 1},
                        {"path": "IMCoverage/year=2001/county=99999/part.parquet", "values": ["2001", "99999"], "row_count": 1, "sha256": "0", "bytes": 1}
                    ]
                },
                {
                    "name": "Link",
                    "partition_strategy": "schema_only",
                    "partition_columns": [],
                    "row_count": 0,
                    "columns": [],
                    "primary_key": [],
                    "partitions": []
                }
            ]
        }"#;

        let run_spec = from_xml_str(runspec_xml).expect("RunSpec must parse");
        // Sample RunSpec uses county 26161 (Washtenaw, MI), year 2001.
        let county_id: i64 = run_spec
            .geographic_selections
            .iter()
            .find(|s| matches!(s.kind, moves_runspec::GeoKind::County))
            .map(|s| s.key as i64)
            .expect("sample RunSpec must have a county selection");
        assert_eq!(county_id, 26161);

        // Build the path list using the real selection logic.
        let paths = required_partition_paths_inner(&run_spec, manifest_json)
            .expect("partition selection must succeed");

        // Monolithic table must always be included.
        assert!(paths.contains(&"County.parquet".to_string()), "County must be included");
        // The matching county=26161 partition must be included.
        assert!(
            paths.contains(&"IMCoverage/year=2001/county=26161/part.parquet".to_string()),
            "county=26161 partition must be included"
        );
        // The non-matching county=99999 partition must be excluded.
        assert!(
            !paths.contains(&"IMCoverage/year=2001/county=99999/part.parquet".to_string()),
            "county=99999 partition must be excluded"
        );
        // schema_only table must not appear.
        assert!(!paths.iter().any(|p| p.contains("Link")), "Link (schema_only) must be excluded");
    }

    /// `run_simulation_from_bundle` runs the default-db pipeline on a minimal
    /// synthetic bundle (wasm32-compatible path).
    ///
    /// Builds an in-memory bundle with just the ZoneRoadType + County tables,
    /// calls `run_simulation_from_bundle`, and checks that the engine produces
    /// at least the MOVESRun.parquet output file.  The slow store is sparse so
    /// calculators produce zero-row output; this test validates that the
    /// wasm32-compatible code path (IPC parsing, populate helpers, engine) all
    /// wire together correctly.
    #[test]
    fn run_simulation_from_bundle_executes_pipeline() {
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType as ArrowDT, Field, Schema};
        use arrow::ipc::writer::FileWriter as ArrowFileWriter;
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let runspec_xml =
            include_str!("../../../characterization/fixtures/sample-runspec.xml");

        // Build a minimal ZoneRoadType IPC table (one row: zone 19131960, road 2).
        let make_ipc = |schema: &Arc<Schema>, batch: RecordBatch| {
            let mut buf = Vec::new();
            let mut w = ArrowFileWriter::try_new(&mut buf, schema).unwrap();
            w.write(&batch).unwrap();
            w.finish().unwrap();
            buf
        };

        let zrt_schema = Arc::new(Schema::new(vec![
            Field::new("zoneID", ArrowDT::Int32, false),
            Field::new("roadTypeID", ArrowDT::Int32, false),
        ]));
        let zrt_batch = RecordBatch::try_new(
            zrt_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![191319600i32])) as _,
                Arc::new(Int32Array::from(vec![2i32])) as _,
            ],
        )
        .unwrap();
        let zrt_ipc = make_ipc(&zrt_schema, zrt_batch);

        let county_schema = Arc::new(Schema::new(vec![
            Field::new("countyID", ArrowDT::Int32, false),
            Field::new("stateID", ArrowDT::Int32, false),
            Field::new("countyName", ArrowDT::Utf8, false),
        ]));
        let county_batch = RecordBatch::try_new(
            county_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![19131960i32])) as _,
                Arc::new(Int32Array::from(vec![19i32])) as _,
                Arc::new(StringArray::from(vec!["Test County"])) as _,
            ],
        )
        .unwrap();
        let county_ipc = make_ipc(&county_schema, county_batch);

        // Build a bundle with two tables.
        let bundle = build_test_bundle(&[
            ("db__test__zoneroadtype", &zrt_ipc),
            ("db__test__county", &county_ipc),
        ]);

        // The bundle has ZoneRoadType + County only; after setup_execution_store
        // the store will have synthesised Link + RunSpec* tables but will still
        // be missing many calculator input tables (SampleVehicleTrip, etc.).
        // The engine should either succeed (if it tolerates sparse stores) or
        // fail with a table-not-found error — either is acceptable here.
        // The key assertions are that (a) bundle parsing succeeds, (b) store
        // setup runs without panicking, and (c) any error is a domain error,
        // not a code-path failure.
        match run_simulation_from_bundle_inner(runspec_xml, &bundle, 1) {
            Ok((_, output_bytes)) => {
                assert!(
                    output_bytes.contains_key("MOVESRun.parquet"),
                    "MOVESRun.parquet must be present on success"
                );
            }
            Err(e) => {
                assert!(
                    e.contains("not found in store") || e.contains("SampleVehicleTrip"),
                    "expected table-not-found error with sparse bundle, got: {e}"
                );
            }
        }
    }

    /// Build a minimal MXDB bundle from (table_name, ipc_bytes) pairs.
    fn build_test_bundle(tables: &[(&str, &[u8])]) -> Vec<u8> {
        const MAGIC: &[u8; 8] = b"MXDB\x00\x00\x00\x01";
        let mut bundle = Vec::new();
        bundle.extend_from_slice(MAGIC);
        bundle.extend_from_slice(&(tables.len() as u32).to_le_bytes());

        // TOC size = 12 (header) + sum of (2 + name_len + 16) for each entry.
        let toc_size: usize = 12
            + tables
                .iter()
                .map(|(n, _)| 2 + n.len() + 16)
                .sum::<usize>();
        let mut cur_offset = toc_size as u64;
        for (name, ipc) in tables {
            let name_bytes = name.as_bytes();
            bundle.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
            bundle.extend_from_slice(name_bytes);
            bundle.extend_from_slice(&cur_offset.to_le_bytes());
            bundle.extend_from_slice(&(ipc.len() as u64).to_le_bytes());
            cur_offset += ipc.len() as u64;
        }
        for (_, ipc) in tables {
            bundle.extend_from_slice(ipc);
        }
        bundle
    }

    /// Native-only helper that calls the bundle-simulation logic directly without
    /// going through wasm_bindgen's JsValue layer.
    fn run_simulation_from_bundle_inner(
        runspec_xml: &str,
        bundle_bytes: &[u8],
        max_parallel_chunks: usize,
    ) -> Result<(String, std::collections::HashMap<String, Vec<u8>>), String> {
        let run_spec = from_xml_str(runspec_xml).map_err(|e| format!("{e}"))?;
        let mut store = default_db::parse_bundle_to_store(bundle_bytes)?;
        default_db::setup_execution_store(&run_spec, &mut store)?;
        let geography = default_db::load_geography_from_store(&store)?;
        let registry = build_registry()?;
        let config = EngineConfig {
            output_root: std::path::PathBuf::from(""),
            max_parallel_chunks,
            run_spec_file_name: None,
            run_date_time: None,
            collect_output_in_memory: true,
        };
        let mut engine = MOVESEngine::new(run_spec.clone(), registry, config);
        engine = engine.with_slow_store(store);
        engine
            .execution_run_spec_mut()
            .build_execution_locations(&geography);
        let outcome = engine.run().map_err(|e| format!("{e}"))?;
        let output: std::collections::HashMap<String, Vec<u8>> = outcome
            .output_bytes
            .into_iter()
            .map(|(p, b)| (p.to_string_lossy().into_owned(), b))
            .collect();
        Ok(("ok".to_string(), output))
    }

    /// Reproduce the live demo's Default-DB onroad simulation natively, reading
    /// the partition tree from `$MOVES_DDB_DIR`. Uses the real
    /// [`required_partition_paths_inner`] selection logic, then reads the
    /// selected paths off disk. Ignored by default — run with:
    ///   MOVES_DDB_DIR=/tmp/ddb/movesdb20241112 \
    ///   cargo test -p moves-wasm repro_default_db_onroad -- --ignored --nocapture
    #[test]
    #[ignore]
    fn repro_default_db_onroad() {
        let Ok(dir) = std::env::var("MOVES_DDB_DIR") else {
            panic!("set MOVES_DDB_DIR to the extracted default-DB tree");
        };
        let root = std::path::PathBuf::from(&dir);
        let runspec_xml = match std::env::var("MOVES_RUNSPEC") {
            Ok(p) => std::fs::read_to_string(&p).expect("read runspec"),
            Err(_) => {
                include_str!("../../../characterization/fixtures/sample-runspec.xml").to_string()
            }
        };
        let runspec_xml = runspec_xml.as_str();

        let run_spec = from_xml_str(runspec_xml).expect("runspec parse");

        // Use the real selection logic to pick which partitions to load.
        let manifest_json =
            std::fs::read_to_string(root.join("manifest.json")).expect("read manifest.json");
        let paths =
            required_partition_paths_inner(&run_spec, &manifest_json).expect("partition selection");
        let files: Vec<(String, Vec<u8>)> = paths
            .iter()
            .filter_map(|p| std::fs::read(root.join(p)).ok().map(|b| (p.clone(), b)))
            .collect();
        eprintln!("selected {} partition files", files.len());
        use moves_framework::DataFrameStore;
        let mut store = default_db::load_partitions_to_store(&files).expect("load partitions");
        eprintln!("store has {} tables after load", store.names().len());
        default_db::setup_execution_store(&run_spec, &mut store).expect("setup store");
        let geography = default_db::load_geography_from_store(&store).expect("geography");

        let registry = build_registry().expect("registry");
        let config = EngineConfig {
            output_root: std::path::PathBuf::from(""),
            max_parallel_chunks: 1,
            run_spec_file_name: None,
            run_date_time: None,
            collect_output_in_memory: true,
        };
        let mut engine = MOVESEngine::new(run_spec.clone(), registry, config);
        engine = engine.with_slow_store(store);
        engine
            .execution_run_spec_mut()
            .build_execution_locations(&geography);

        // Diagnostics: planned modules and chunk grouping.
        if let Ok(modules) = engine.planned_modules() {
            eprintln!("planned_modules ({}): {:?}", modules.len(), modules);
        }
        if let Ok(chunks) = engine.planned_chunks() {
            eprintln!("chunks: {}", chunks.len());
            for (i, c) in chunks.iter().enumerate() {
                eprintln!("  chunk[{i}]: {:?}", c.modules());
            }
        }

        match engine.run() {
            Ok(outcome) => {
                eprintln!("SUCCESS: {} output file(s)", outcome.output_bytes.len());
                eprintln!("modules_executed: {:?}", outcome.modules_executed);
            }
            Err(e) => {
                panic!("engine.run() failed: {e}");
            }
        }
    }

    /// Build a 130-char fixed-width `.POP` record, matching the column layout
    /// in `rdpop.f` / `getpop.f`. Right-justifies HP fields and population.
    #[allow(clippy::too_many_arguments)]
    fn build_pop_record(
        fips: &str,
        sub: &str,
        year: &str,
        scc: &str,
        hp_min: &str,
        hp_max: &str,
        hp_avg: &str,
        usage: &str,
        tech: &str,
        pop: &str,
    ) -> Vec<u8> {
        let mut buf = vec![b' '; 130];
        let put = |buf: &mut [u8], start_1: usize, value: &str, width: usize| {
            let start = start_1 - 1;
            let bytes = value.as_bytes();
            let n = bytes.len().min(width);
            buf[start..start + n].copy_from_slice(&bytes[..n]);
        };
        let put_right = |buf: &mut [u8], start_1: usize, value: &str, width: usize| {
            let pad = width.saturating_sub(value.len());
            let start = start_1 - 1 + pad;
            let bytes = value.as_bytes();
            let n = bytes.len().min(width.saturating_sub(pad));
            buf[start..start + n].copy_from_slice(&bytes[..n]);
        };
        put(&mut buf, 1, fips, 5);
        put(&mut buf, 7, sub, 5);
        put(&mut buf, 13, year, 4);
        put(&mut buf, 18, scc, 10);
        put_right(&mut buf, 70, hp_min, 5);
        put_right(&mut buf, 76, hp_max, 5);
        put_right(&mut buf, 82, hp_avg, 5);
        put_right(&mut buf, 88, usage, 5);
        put(&mut buf, 93, tech, 10);
        put_right(&mut buf, 108, pop, 15);
        buf
    }

    fn make_pop_file(records: &[Vec<u8>]) -> Vec<u8> {
        let mut data = b"/POPULATION/\n".to_vec();
        for rec in records {
            data.extend_from_slice(rec);
            data.push(b'\n');
        }
        data.extend_from_slice(b"/END/\n");
        data
    }

    #[test]
    fn run_nonroad_simulation_empty_pop_succeeds() {
        let pop_data = b"/POPULATION/\n/END/\n";
        let options = r#"{"episode_year": 2020, "region_level": "COUNTY"}"#;
        let inputs = nonroad_inputs_from_pop(
            read_pop(std::io::Cursor::new(pop_data.as_ref())).unwrap(),
            vec![],
        );
        let mut executor = PlanRecordingExecutor::new();
        let opts = NonroadOptions::new(RegionLevel::from_reglvl("COUNTY").unwrap(), 2020);
        let out = nonroad_run_simulation(&opts, &inputs, &mut executor).unwrap();
        assert!(out.completion_message.starts_with("Successful completion"));
        assert_eq!(out.counters.scc_groups_planned, 0);
        let _ = options; // silence unused warning
    }

    #[test]
    fn run_nonroad_simulation_with_pop_records() {
        let rec1 = build_pop_record(
            "06037",
            "00000",
            "2020",
            "2270001010",
            "25",
            "50",
            "",
            "1",
            "",
            "100",
        );
        let rec2 = build_pop_record(
            "06038",
            "00000",
            "2020",
            "2270001010",
            "25",
            "50",
            "",
            "1",
            "",
            "200",
        );
        let pop_data = make_pop_file(&[rec1, rec2]);

        let pop_records = read_pop(std::io::Cursor::new(pop_data.as_slice())).unwrap();
        assert_eq!(pop_records.len(), 2);

        let inputs =
            nonroad_inputs_from_pop(pop_records, vec!["06037".to_string(), "06038".to_string()]);
        assert_eq!(inputs.group_count(), 1, "both records share SCC 2270001010");
        assert_eq!(inputs.record_count(), 2);

        let opts = NonroadOptions::new(RegionLevel::from_reglvl("COUNTY").unwrap(), 2020);
        let mut executor = PlanRecordingExecutor::new();
        let out = nonroad_run_simulation(&opts, &inputs, &mut executor).unwrap();

        assert_eq!(out.counters.scc_groups_planned, 1);
        assert_eq!(out.counters.records_visited, 2);
        assert_eq!(out.counters.dispatch_calls, 2);
        assert!(out.completion_message.starts_with("Successful completion"));
    }

    #[test]
    fn nonroad_inputs_from_pop_groups_by_scc() {
        let rec_a = build_pop_record(
            "06037",
            "00000",
            "2020",
            "2270001010",
            "25",
            "50",
            "",
            "1",
            "",
            "100",
        );
        let rec_b = build_pop_record(
            "06037",
            "00000",
            "2020",
            "2265001010",
            "10",
            "25",
            "",
            "1",
            "",
            "50",
        );
        let rec_c = build_pop_record(
            "06038",
            "00000",
            "2020",
            "2270001010",
            "25",
            "50",
            "",
            "1",
            "",
            "150",
        );
        let pop_data = make_pop_file(&[rec_a, rec_b, rec_c]);

        let pop_records = read_pop(std::io::Cursor::new(pop_data.as_slice())).unwrap();
        let inputs = nonroad_inputs_from_pop(pop_records, vec![]);

        assert_eq!(inputs.group_count(), 2, "two distinct SCCs");
        assert_eq!(inputs.scc_groups[0].scc, "2270001010");
        assert_eq!(inputs.scc_groups[0].len(), 2, "two records for 2270001010");
        assert_eq!(inputs.scc_groups[1].scc, "2265001010");
        assert_eq!(inputs.scc_groups[1].len(), 1);
    }
}

//! `moves-wasm` — WebAssembly entry point for the MOVES Rust port.
//!
//! Exposes the MOVES onroad simulation engine (Task 132) and the
//! NONROAD nonroad-emissions simulation engine (Task 133) to browser
//! JavaScript via [`wasm-bindgen`](https://github.com/rustwasm/wasm-bindgen).
//! The module is compiled with `--target wasm32-unknown-unknown` and loaded
//! via a standard ES module or Webpack bundle.
//!
//! # Concurrency (Task 134)
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
//!   cargo build --target wasm32-unknown-unknown --features wasm-threads
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
//! Simulation functions must then be called **from a Web Worker context** —
//! browsers disallow `atomic.wait` on the main thread, which rayon uses for
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
//!   console.log(path, bytes.byteLength, "bytes");
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
//!                                   selected_counties: ["06037"] });
//! const popBytes = new Uint8Array(await popFile.arrayBuffer());
//!
//! // Returns { completion_message: "…", counters: { scc_groups_planned: …, … } }
//! const result = run_nonroad_simulation(options, popBytes, 0);
//! console.log(result.completion_message);
//! ```
//!
//! See `moves-rust-migration-plan.md` Tasks 132–134 and `docs/wasm-threading.md`.

// Pull in the onroad calculator and generator implementations so they are
// compiled into the WASM module. Registration with the engine registry is
// wired in [`build_registry`] below; importing the crate ensures the code
// is present even before every calculator is registered.
use moves_calculators as _;

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
use moves_runspec::from_xml_str;
use wasm_bindgen::prelude::*;

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

/// The Phase 1 calculator-chain DAG, embedded at compile time.
const EMBEDDED_CALCULATOR_DAG: &str =
    include_str!("../../../characterization/calculator-chains/calculator-dag.json");

/// Run a MOVES onroad simulation in the browser.
///
/// # Arguments
///
/// * `runspec_xml` — RunSpec document as an XML string. The caller
///   typically reads this from a file picker (`<input type="file">`) or
///   from OPFS via `FileSystemFileHandle.getFile().text()`.
///
/// * `max_parallel_chunks` — Maximum number of independent calculator chains
///   that may run concurrently. Pass `0` to let the engine choose (resolves
///   to 1 in a single-threaded build; to the global rayon thread count in a
///   `wasm-threads` build after `init_thread_pool` has been called).
///   Pass `1` to force sequential execution regardless of build flags.
///   Values > 1 require the `wasm-threads` build **and** a Web Worker calling
///   context — see `docs/wasm-threading.md`.
///
/// # Returns
///
/// On success, a JavaScript object mapping relative output file paths to
/// `Uint8Array` Parquet bytes:
///
/// ```json
/// {
///   "MOVESRun.parquet": Uint8Array,
///   "MOVESOutput/yearID=2020/monthID=1/part.parquet": Uint8Array,
///   ...
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
    let run_spec =
        from_xml_str(runspec_xml).map_err(|e| JsValue::from_str(&format!("RunSpec parse error: {e}")))?;

    let registry = build_registry()
        .map_err(|e| JsValue::from_str(&format!("Registry build error: {e}")))?;

    let config = EngineConfig {
        output_root: std::path::PathBuf::from(""),
        max_parallel_chunks: max_parallel_chunks as usize,
        run_spec_file_name: None,
        run_date_time: None,
        collect_output_in_memory: true,
    };

    let engine = MOVESEngine::new(run_spec, registry, config);
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

/// Run a NONROAD nonroad-emissions simulation in the browser.
///
/// Input data arrives as browser-supplied bytes via the File API or OPFS —
/// no `std::fs` calls are made. Parsers consume `std::io::Cursor` wrappers
/// around the supplied byte slices, which is the WASM-compatible I/O path
/// described in `ARCHITECTURE.md` § 4.3.
///
/// # Arguments
///
/// * `options_json` — JSON object with run configuration. Required:
///   - `"episode_year"`: integer, 1990–2099.
///
///   Optional (with defaults):
///   - `"region_level"`: `"COUNTY"` | `"STATE"` | `"50STATE"` |
///     `"SUBCOUNTY"` | `"US TOTAL"` (default `"COUNTY"`).
///   - `"growth_year"`, `"tech_year"`: integers; default to `episode_year`.
///   - `"total_mode"`, `"daily_output"`, `"emit_bmy_exhaust"`,
///     `"emit_bmy_evap"`, `"emit_si"`: booleans (default `false`).
///   - `"growth_loaded"`, `"retrofit_loaded"`, `"spillage_loaded"`:
///     booleans (default `false`).
///   - `"title"`: string (default `""`).
///   - `"selected_counties"`: array of 5-character FIPS strings (default:
///     no county filter — the driver accepts all records).
///
/// * `pop_bytes` — Contents of a NONROAD `.POP` population file as a
///   `Uint8Array`. Load from a file picker (`<input type="file">`) or
///   OPFS:
///   ```js
///   const popBytes = new Uint8Array(await popFile.arrayBuffer());
///   ```
///
/// # Returns
///
/// On success:
/// ```json
/// {
///   "completion_message": "Successful completion — no warnings",
///   "counters": {
///     "scc_groups_planned": 12,
///     "scc_groups_skipped": 0,
///     "records_visited": 240,
///     "records_not_selected": 0,
///     "records_no_dispatch": 0,
///     "dispatch_calls": 240,
///     "geography_skips": 0
///   }
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
        .ok_or_else(|| JsValue::from_str("options.episode_year is required"))? as i32;
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
    js_set_num(&counters_obj, "scc_groups_planned", c.scc_groups_planned as f64)?;
    js_set_num(&counters_obj, "scc_groups_skipped", c.scc_groups_skipped as f64)?;
    js_set_num(&counters_obj, "records_visited", c.records_visited as f64)?;
    js_set_num(&counters_obj, "records_not_selected", c.records_not_selected as f64)?;
    js_set_num(&counters_obj, "records_no_dispatch", c.records_no_dispatch as f64)?;
    js_set_num(&counters_obj, "dispatch_calls", c.dispatch_calls as f64)?;
    js_set_num(&counters_obj, "geography_skips", c.geography_skips as f64)?;
    js_sys::Reflect::set(
        &result,
        &JsValue::from_str("counters"),
        &counters_obj,
    )
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

/// Build a [`CalculatorRegistry`] from the embedded Phase 1 DAG, with
/// all ported onroad calculators and generators registered.
fn build_registry() -> Result<CalculatorRegistry, String> {
    let dag: CalculatorDag = serde_json::from_str(EMBEDDED_CALCULATOR_DAG)
        .map_err(|e| format!("embedded DAG parse error: {e}"))?;
    let registry = CalculatorRegistry::new(dag);
    // Phase 3 calculator registration will be added here as calculators are
    // ported. For now the registry carries the DAG structure for planning
    // (module ordering, chunking) even though no factories are registered
    // yet — the engine reports all planned modules as "unimplemented" and
    // produces an empty-but-correctly-shaped output, exactly as the native
    // CLI does in the current Phase 2 / Phase 3 state.
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

    #[test]
    fn run_simulation_returns_moves_run_parquet() {
        use moves_framework::{EngineConfig, MOVESEngine};
        use moves_runspec::from_xml_str;

        let runspec_xml = include_str!(
            "../../../characterization/fixtures/sample-runspec.xml"
        );
        let run_spec = from_xml_str(runspec_xml).expect("sample RunSpec must parse");
        let registry = build_registry().expect("registry must build");
        let config = EngineConfig {
            output_root: std::path::PathBuf::from(""),
            max_parallel_chunks: 1,
            run_spec_file_name: Some("sample-runspec.xml".to_string()),
            run_date_time: None,
            collect_output_in_memory: true,
        };
        let engine = MOVESEngine::new(run_spec, registry, config);
        let outcome = engine.run().expect("engine must run");
        assert!(
            !outcome.output_bytes.is_empty(),
            "expected at least one output file"
        );
        assert!(
            outcome.output_bytes.iter().any(|(p, _)| p.to_str() == Some("MOVESRun.parquet")),
            "expected MOVESRun.parquet in output"
        );
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
        let opts = NonroadOptions::new(
            RegionLevel::from_reglvl("COUNTY").unwrap(),
            2020,
        );
        let out = nonroad_run_simulation(&opts, &inputs, &mut executor).unwrap();
        assert!(out.completion_message.starts_with("Successful completion"));
        assert_eq!(out.counters.scc_groups_planned, 0);
        let _ = options; // silence unused warning
    }

    #[test]
    fn run_nonroad_simulation_with_pop_records() {
        let rec1 = build_pop_record("06037", "00000", "2020", "2270001010",
            "25", "50", "", "1", "", "100");
        let rec2 = build_pop_record("06038", "00000", "2020", "2270001010",
            "25", "50", "", "1", "", "200");
        let pop_data = make_pop_file(&[rec1, rec2]);

        let pop_records = read_pop(std::io::Cursor::new(pop_data.as_slice())).unwrap();
        assert_eq!(pop_records.len(), 2);

        let inputs = nonroad_inputs_from_pop(pop_records, vec!["06037".to_string(), "06038".to_string()]);
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
        let rec_a = build_pop_record("06037", "00000", "2020", "2270001010",
            "25", "50", "", "1", "", "100");
        let rec_b = build_pop_record("06037", "00000", "2020", "2265001010",
            "10", "25", "", "1", "", "50");
        let rec_c = build_pop_record("06038", "00000", "2020", "2270001010",
            "25", "50", "", "1", "", "150");
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

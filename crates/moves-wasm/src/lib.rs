//! `moves-wasm` — WebAssembly entry point for the MOVES Rust port.
//!
//! Exposes the MOVES onroad simulation engine to browser JavaScript via
//! [`wasm-bindgen`](https://github.com/rustwasm/wasm-bindgen). The module
//! is compiled with `--target wasm32-unknown-unknown` and loaded via a
//! standard ES module or Webpack bundle.
//!
//! # Concurrency note (Task 132)
//!
//! `wasm32-unknown-unknown` has no threads until Task 134 enables the
//! threads proposal (SharedArrayBuffer + cross-origin isolation). The
//! [`BoundedExecutor`](moves_framework::BoundedExecutor) therefore
//! defaults `max_parallel_chunks` to 1 here — chunks run sequentially,
//! which still bounds peak memory by sequencing chunk working sets rather
//! than running them concurrently. Task 134 will re-enable
//! `max_parallel_chunks > 1` once rayon's Web Worker pool is wired up.
//!
//! # I/O model
//!
//! The browser has no direct filesystem access. Input arrives as JavaScript
//! strings / `Uint8Array` values (file upload or OPFS reads in the caller);
//! output is returned as a JSON object mapping relative file paths to
//! `Uint8Array` Parquet bytes, which the caller writes to OPFS or offers
//! for download.
//!
//! # Usage (JavaScript)
//!
//! ```js
//! import init, { run_simulation } from "./moves_wasm.js";
//!
//! await init();
//!
//! // RunSpec as an XML string (loaded from a file picker or OPFS).
//! const runspecXml = await file.text();
//!
//! // Run. Returns { "MOVESRun.parquet": Uint8Array, ... }
//! const result = run_simulation(runspecXml);
//!
//! // Persist to OPFS or offer as a download.
//! for (const [path, bytes] of Object.entries(result)) {
//!   console.log(path, bytes.byteLength, "bytes");
//! }
//! ```
//!
//! See `moves-rust-migration-plan.md` Task 132.

// Pull in the onroad calculator and generator implementations so they are
// compiled into the WASM module. Registration with the engine registry is
// wired in [`build_registry`] below; importing the crate ensures the code
// is present even before every calculator is registered.
use moves_calculators as _;

use moves_calculator_info::CalculatorDag;
use moves_framework::{CalculatorRegistry, EngineConfig, MOVESEngine};
use moves_runspec::from_xml_str;
use wasm_bindgen::prelude::*;

/// The Phase 1 calculator-chain DAG, embedded at compile time.
const EMBEDDED_CALCULATOR_DAG: &str =
    include_str!("../../../characterization/calculator-chains/calculator-dag.json");

/// Default parallelism cap for the WASM build.
///
/// `wasm32-unknown-unknown` is single-threaded until Task 134 enables the
/// threads proposal, so the bounded executor runs at most one chunk at a
/// time. The semaphore still bounds peak memory — with limit 1, no two
/// chunk working sets are ever co-resident.
///
/// Task 134 will replace this constant with a runtime-settable option
/// backed by `navigator.hardwareConcurrency`.
const WASM_MAX_PARALLEL_CHUNKS: usize = 1;

/// Run a MOVES simulation in the browser.
///
/// # Arguments
///
/// * `runspec_xml` — RunSpec document as an XML string. The caller
///   typically reads this from a file picker (`<input type="file">`) or
///   from OPFS via `FileSystemFileHandle.getFile().text()`.
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
pub fn run_simulation(runspec_xml: &str) -> Result<JsValue, JsValue> {
    let run_spec =
        from_xml_str(runspec_xml).map_err(|e| JsValue::from_str(&format!("RunSpec parse error: {e}")))?;

    let registry = build_registry()
        .map_err(|e| JsValue::from_str(&format!("Registry build error: {e}")))?;

    let config = EngineConfig {
        output_root: std::path::PathBuf::from(""),
        max_parallel_chunks: WASM_MAX_PARALLEL_CHUNKS,
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
        use moves_runspec::from_xml_str;
        use moves_framework::{CalculatorRegistry, EngineConfig, MOVESEngine};

        // Use the sample RunSpec fixture for a minimal smoke test.
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
        // Should produce at least MOVESRun.parquet in memory.
        assert!(
            !outcome.output_bytes.is_empty(),
            "expected at least one output file"
        );
        assert!(
            outcome.output_bytes.iter().any(|(p, _)| p.to_str() == Some("MOVESRun.parquet")),
            "expected MOVESRun.parquet in output"
        );
    }
}

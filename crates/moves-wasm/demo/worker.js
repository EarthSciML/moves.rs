/**
 * MOVES WebAssembly worker — runs onroad or NONROAD simulation off the main
 * thread so the page stays responsive.
 *
 * Message protocol (main → worker):
 *   { type: "onroad",  runspecXml: string, maxParallelChunks: number }
 *   { type: "nonroad", optionsJson: string, popBytes: Uint8Array }
 *
 * Message protocol (worker → main):
 *   { type: "log",     message: string }
 *   { type: "result",  files: [{ name: string, bytes: Uint8Array }] }  // onroad
 *   { type: "result",  completion_message: string, counters: object }   // nonroad
 *   { type: "error",   message: string }
 *
 * The worker expects the wasm-bindgen JS glue at ./pkg/moves_wasm.js relative
 * to the worker script URL.  Build the package first — see demo/README.md.
 */

import init, { run_simulation, run_nonroad_simulation }
    from "./pkg/moves_wasm.js";

let ready = false;

async function ensureInit() {
    if (!ready) {
        self.postMessage({ type: "log", message: "Initialising WASM module…" });
        await init();
        ready = true;
        self.postMessage({ type: "log", message: "WASM module ready." });
    }
}

self.onmessage = async (event) => {
    const msg = event.data;
    try {
        await ensureInit();

        if (msg.type === "onroad") {
            self.postMessage({ type: "log", message: "Running onroad simulation…" });
            const result = run_simulation(msg.runspecXml, msg.maxParallelChunks ?? 0);

            // result is a plain JS object: { "path": Uint8Array, … }
            const files = Object.entries(result).map(([name, bytes]) => ({ name, bytes }));
            self.postMessage({ type: "result", mode: "onroad", files });

        } else if (msg.type === "nonroad") {
            self.postMessage({ type: "log", message: "Running NONROAD simulation…" });
            const result = run_nonroad_simulation(msg.optionsJson, msg.popBytes);
            self.postMessage({
                type: "result",
                mode: "nonroad",
                completion_message: result.completion_message,
                counters: result.counters,
            });

        } else {
            self.postMessage({ type: "error", message: `Unknown message type: ${msg.type}` });
        }

    } catch (err) {
        self.postMessage({ type: "error", message: String(err) });
    }
};

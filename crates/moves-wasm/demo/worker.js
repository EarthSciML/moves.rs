/**
 * MOVES WebAssembly worker — runs onroad or NONROAD simulation off the main
 * thread so the page stays responsive.
 *
 * Message protocol (main → worker):
 *   { type: "onroad",     runspecXml: string, maxParallelChunks: number }
 *   { type: "nonroad",    optionsJson: string, popBytes: Uint8Array }
 *   { type: "default-db", runspecXml: string, dbBaseUrl: string,
 *                         maxParallelChunks: number }
 *     dbBaseUrl — absolute or relative URL of the default-DB directory,
 *     e.g. "./data/movesdb20241112" (no trailing slash).
 *
 * Message protocol (worker → main):
 *   { type: "log",     message: string }
 *   { type: "result",  mode: "onroad"|"nonroad"|"default-db",
 *                      files: [{name,bytes}] }  // onroad / default-db
 *   { type: "result",  mode: "nonroad",
 *                      completion_message, counters }
 *   { type: "progress", fetched: number, total: number }  // default-db fetch
 *   { type: "error",   message: string }
 *
 * The worker expects the wasm-bindgen JS glue at ./pkg/moves_wasm.js relative
 * to the worker script URL.  Build the package first — see demo/README.md.
 */

import init, {
    run_simulation,
    run_nonroad_simulation,
    run_simulation_from_partitions,
    required_partition_paths,
} from "./pkg/moves_wasm.js";

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

        } else if (msg.type === "default-db") {
            await runDefaultDb(msg);

        } else {
            self.postMessage({ type: "error", message: `Unknown message type: ${msg.type}` });
        }

    } catch (err) {
        self.postMessage({ type: "error", message: String(err) });
    }
};

/**
 * Fetch the needed default-DB partitions and run the simulation.
 *
 * @param {object} msg
 * @param {string} msg.runspecXml
 * @param {string} msg.dbBaseUrl  e.g. "./data/movesdb20241112"
 * @param {number} [msg.maxParallelChunks]
 */
async function runDefaultDb({ runspecXml, dbBaseUrl, maxParallelChunks }) {
    // 1. Fetch manifest.
    self.postMessage({ type: "log", message: "Fetching default-DB manifest…" });
    const manifestResp = await fetch(`${dbBaseUrl}/manifest.json`);
    if (!manifestResp.ok) {
        throw new Error(`Failed to fetch manifest: ${manifestResp.status} ${manifestResp.statusText}`);
    }
    const manifestJson = await manifestResp.text();

    // 2. Compute required partition paths.
    self.postMessage({ type: "log", message: "Computing required partitions…" });
    const paths = required_partition_paths(runspecXml, manifestJson);
    self.postMessage({
        type: "log",
        message: `Fetching ${paths.length} partition file(s)…`,
    });

    // 3. Fetch each partition file (sequentially to stay within memory budget;
    //    parallel fetches of many large files can OOM a wasm32 sandbox).
    const partitions = [];
    for (let i = 0; i < paths.length; i++) {
        const path = paths[i];
        const url  = `${dbBaseUrl}/${path}`;
        const resp = await fetch(url);
        if (!resp.ok) {
            throw new Error(`Failed to fetch ${path}: ${resp.status} ${resp.statusText}`);
        }
        const buf  = await resp.arrayBuffer();
        partitions.push({ path, bytes: new Uint8Array(buf) });
        self.postMessage({ type: "progress", fetched: i + 1, total: paths.length });
    }

    // 4. Run simulation.
    self.postMessage({ type: "log", message: "Running default-DB simulation…" });
    const result = run_simulation_from_partitions(
        runspecXml,
        partitions,
        maxParallelChunks ?? 0,
    );

    const files = Object.entries(result).map(([name, bytes]) => ({ name, bytes }));
    self.postMessage({ type: "result", mode: "default-db", files });
}

# Embedding the moves.rs WASM module in third-party tools

This guide explains how to integrate the `moves-wasm` package into your own
JavaScript/TypeScript application. It covers installation, API reference,
bundler integration, and deployment requirements.

## Prerequisites

- A modern browser or a Node.js/Deno runtime that can execute WASM.
- The `moves_wasm.js` JS glue and `moves_wasm_bg.wasm` binary, produced by
 `wasm-pack build --target web crates/moves-wasm` from the repo root.

## Installation

The package is not yet published to npm. Copy the `pkg/` directory (produced
by `wasm-pack`) into your project, or reference it from a local path:

```bash
# Build the package
wasm-pack build --target web crates/moves-wasm --out-dir /path/to/your-app/moves-wasm
```

For bundler-based projects (see [Bundler integration](#bundler-integration)):

```bash
wasm-pack build --target bundler crates/moves-wasm --out-dir /path/to/your-app/moves-wasm
```

## API reference

The WASM module exposes two main functions and one optional threading initialiser.

### `init(input?)`

Initialises the WASM binary. Must be called (and awaited) once before any
other function.

```js
import init from "./moves-wasm/moves_wasm.js";
await init();
```

### `run_simulation(runspecXml, maxParallelChunks)`

Runs the onroad MOVES simulation.

| Parameter | Type | Description |
|-----------|------|-------------|
| `runspecXml` | `string` | RunSpec document as an XML string |
| `maxParallelChunks` | `number` | Concurrency level: 0 = auto (1 for single-thread build, thread count for multi-thread build); 1 = sequential; &gt;1 = multi-thread (requires `wasm-threads` build + Worker context) |

**Returns:** A plain JS object mapping relative output paths to `Uint8Array`
Parquet bytes:

```js
{
 "MOVESRun.parquet": Uint8Array,
 "MOVESOutput/yearID=2020/monthID=1/part.parquet": Uint8Array,
 ...
}
```

**Throws:** A JavaScript `Error` if the RunSpec cannot be parsed or if the
engine encounters a fatal error.

**Example:**

```js
import init, { run_simulation } from "./moves-wasm/moves_wasm.js";

await init();

const runspecXml = await fetch("sample-runspec.xml").then(r => r.text());
const result = run_simulation(runspecXml, 0);

for (const [path, bytes] of Object.entries(result)) {
 console.log(`${path}: ${bytes.byteLength} bytes`);
}
```

### `run_nonroad_simulation(optionsJson, popBytes)`

Runs the NONROAD simulation.

| Parameter | Type | Description |
|-----------|------|-------------|
| `optionsJson` | `string` | JSON string with run configuration (see below) |
| `popBytes` | `Uint8Array` | Contents of a NONROAD `.POP` population file |

**`optionsJson` fields:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `episode_year` | `number` | **Yes** | — | Simulation year (1990–2099) |
| `region_level` | `string` | No | `"COUNTY"` | `"COUNTY"` \| `"STATE"` \| `"50STATE"` \| `"SUBCOUNTY"` \| `"US TOTAL"` |
| `growth_year` | `number` | No | `episode_year` | Growth projection year |
| `tech_year` | `number` | No | `episode_year` | Technology year |
| `total_mode` | `boolean` | No | `false` | Aggregate to totals |
| `daily_output` | `boolean` | No | `false` | Daily output mode |
| `selected_counties` | `string[]` | No | `[]` (all) | Array of 5-character FIPS codes |
| `title` | `string` | No | `""` | Run title |

**Returns:** A plain JS object:

```js
{
 "completion_message": "Successful completion — no warnings",
 "counters": {
 "scc_groups_planned": 12,
 "scc_groups_skipped": 0,
 "records_visited": 240,
 "records_not_selected": 0,
 "records_no_dispatch": 0,
 "dispatch_calls": 240,
 "geography_skips": 0
 }
}
```

**Throws:** A JavaScript `Error` if the options JSON is malformed or the
population file cannot be parsed.

**Example:**

```js
import init, { run_nonroad_simulation } from "./moves-wasm/moves_wasm.js";

await init();

const popBytes = new Uint8Array(await fetch("equipment.POP").then(r => r.arrayBuffer()));
const options = JSON.stringify({
 episode_year: 2020,
 region_level: "COUNTY",
 selected_counties: ["06037", "06038"],
});

const result = run_nonroad_simulation(options, popBytes);
console.log(result.completion_message);
console.log(result.counters);
```

### `init_thread_pool(numThreads)` — multi-thread build only

Initialises the rayon Web Worker thread pool. Only available in the
`wasm-threads` feature build. Call once, after `init()`, before the first
simulation. Returns a `Promise` that resolves when all workers are ready.

```js
await init();
await init_thread_pool(navigator.hardwareConcurrency);
```

See [`docs/wasm-threading.md`](wasm-threading.md) for full multi-thread
deployment instructions.

---

## Running off the main thread (recommended)

Simulation functions are synchronous and CPU-intensive. They block the
calling thread for the duration of the run. To keep the browser UI
responsive, run them inside a **Web Worker**:

**simulation-worker.js**

```js
import init, { run_simulation } from "./moves-wasm/moves_wasm.js";

let ready = false;

self.onmessage = async ({ data }) => {
 if (!ready) {
 await init();
 ready = true;
 }

 try {
 const result = run_simulation(data.runspecXml, data.maxParallelChunks ?? 0);
 const files = Object.entries(result).map(([name, bytes]) => ({ name, bytes }));
 self.postMessage({ ok: true, files }, files.map(f => f.bytes.buffer));
 } catch (err) {
 self.postMessage({ ok: false, error: String(err) });
 }
};
```

**main.js**

```js
const worker = new Worker(new URL("simulation-worker.js", import.meta.url),
 { type: "module" });

worker.postMessage({ runspecXml, maxParallelChunks: 0 });

worker.onmessage = ({ data }) => {
 if (data.ok) {
 for (const { name, bytes } of data.files) {
 console.log(`${name}: ${bytes.byteLength} bytes`);
 }
 } else {
 console.error(data.error);
 }
};
```

The `files.map(f => f.bytes.buffer)` transfer list moves the underlying
`ArrayBuffer` objects to the main thread without copying, which is important
for large outputs.

---

## Downloading Parquet output

Convert a `Uint8Array` output buffer to a download link:

```js
function downloadParquet(bytes, filename) {
 const blob = new Blob([bytes], { type: "application/octet-stream" });
 const url = URL.createObjectURL(blob);
 const a = Object.assign(document.createElement("a"), {
 href: url, download: filename, textContent: `Download ${filename}`,
 });
 document.body.appendChild(a);
 // Release the object URL when no longer needed
 a.addEventListener("click", () => setTimeout(() => URL.revokeObjectURL(url), 60_000));
}
```

---

## Saving to OPFS (Origin Private File System)

For large outputs, write directly to the browser's OPFS instead of holding
everything in memory:

```js
const root = await navigator.storage.getDirectory();
const dir = await root.getDirectoryHandle("moves-output", { create: true });
const handle = await dir.getFileHandle("MOVESRun.parquet", { create: true });
const writable = await handle.createWritable();
await writable.write(bytes);
await writable.close();
```

OPFS is available in all modern browsers and does not require cross-origin
isolation headers, unlike `SharedArrayBuffer`.

---

## Bundler integration

### Webpack 5

```js
// webpack.config.js
module.exports = {
 experiments: { asyncWebAssembly: true },
};
```

Build the package with `--target bundler` instead of `--target web`:

```bash
wasm-pack build --target bundler crates/moves-wasm --out-dir moves-wasm
```

Then import normally — Webpack handles WASM loading automatically:

```js
import init, { run_simulation } from "./moves-wasm";
await init();
```

### Vite

No special configuration needed for the default `--target web` build. Vite
resolves the WASM file automatically.

```bash
wasm-pack build --target web crates/moves-wasm --out-dir src/moves-wasm
```

```js
import init, { run_simulation } from "./moves-wasm/moves_wasm.js?url";
```

For a dedicated Vite WASM plugin, see
[vite-plugin-wasm](https://github.com/Menci/vite-plugin-wasm).

### Rollup / esbuild

Use the bundler target:

```bash
wasm-pack build --target bundler crates/moves-wasm
```

Configure your bundler to treat `.wasm` files as assets (Rollup:
`@rollup/plugin-wasm`; esbuild: `--loader:.wasm=file`).

---

## Deployment checklist

| Requirement | Notes |
|-------------|-------|
| Static hosting | Any CDN or server that can serve `.js` and `.wasm` files. |
| MIME type for `.wasm` | Server must respond with `Content-Type: application/wasm`. Most CDNs do this automatically; some require configuration. |
| COEP/COOP headers | **Only** required for the multi-thread `wasm-threads` build. Not required for the default single-thread build. |
| HTTPS | Required in production for Service Workers and OPFS; not required for `localhost` testing. |

---

## Troubleshooting

**"Failed to fetch" / 404 on `.wasm` file** 
The WASM binary must be co-located with the JS glue file, or the path
passed to `init()` must point to the `.wasm` file explicitly:

```js
await init(new URL("./moves_wasm_bg.wasm", import.meta.url));
```

**`SharedArrayBuffer is not defined`** 
The page is missing COEP/COOP headers. This is only needed for the
multi-thread build. The default single-thread build does not require these
headers.

**Worker uses ES module but browser doesn't support it** 
Set `{ type: "module" }` when constructing the Worker. If the browser
doesn't support module workers, use a bundler to produce a classic-format
worker bundle.

**`atomics.wait` error on main thread** 
Move the `run_simulation(…, n > 1)` call into a Web Worker. Browsers
prohibit `atomics.wait` on the main thread to prevent freezing the UI.

**Simulation hangs or takes very long** 
A large RunSpec (many pollutants, many county-year combinations) can take
minutes on a single thread in WASM. Use `maxParallelChunks > 1` with the
multi-thread build, or pre-filter the RunSpec to a smaller scope.

---

## See also

- [`docs/wasm-threading.md`](wasm-threading.md) — multi-thread deployment and
 COEP/COOP header configuration
- [`crates/moves-wasm/demo/`](../crates/moves-wasm/demo/) — minimal demo page
 showing the API in use
- [`docs/output-schema.md`](output-schema.md) — Parquet output column reference
- [`docs/downstream-tools.md`](downstream-tools.md) — reading MOVES output in
 Python, R, DuckDB, Polars, and Spark

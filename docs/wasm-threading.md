# WASM Multi-Threading (Task 134)

The `moves-wasm` crate ships in two build variants:

| Build | Feature flag | Threads | `SharedArrayBuffer` required |
|---|---|---|---|
| Default | *(none)* | No — sequential | No |
| Threaded | `wasm-threads` | Yes — Web Workers | **Yes** |

---

## Cross-origin isolation requirement

The threaded build relies on `SharedArrayBuffer`, which browsers gate behind
**cross-origin isolation**. The page (or the Worker that loads the WASM module)
must be served with:

```
Cross-Origin-Opener-Policy: same-origin
Cross-Origin-Embedder-Policy: require-corp
```

Every resource the page loads (images, scripts, fonts, data files) must either
be same-origin or carry a `Cross-Origin-Resource-Policy: cross-origin` header.
CDN-hosted resources without that header will be blocked.

Test that isolation is active in browser DevTools:

```js
console.log(crossOriginIsolated); // must print true
```

If this prints `false`, `SharedArrayBuffer` is `undefined` and the threaded
WASM module will fail to instantiate.

---

## Building the threaded WASM binary

### Toolchain requirement

WASM atomics (`SharedArrayBuffer`-backed mutexes and thread synchronisation)
are a nightly-only Rust feature because they depend on the standard library
being recompiled with atomics support (`-Zbuild-std`). The `wasm-threads`
feature therefore requires a **nightly** Rust toolchain:

```bash
rustup install nightly
rustup target add --toolchain nightly wasm32-unknown-unknown
```

### Build command

```bash
RUSTFLAGS="-C target-feature=+atomics,+bulk-memory,+mutable-globals" \
  cargo +nightly build \
    -Z build-std=std,panic_abort \
    -Z build-std-features=panic_immediate_abort \
    --target wasm32-unknown-unknown \
    --package moves-wasm \
    --features wasm-threads \
    --release
```

Then run `wasm-bindgen` to generate the JS glue:

```bash
wasm-bindgen \
  --target web \
  --out-dir pkg/ \
  target/wasm32-unknown-unknown/release/moves_wasm.wasm
```

The `+atomics,+bulk-memory,+mutable-globals` LLVM target features and
`-Zbuild-std` are **required** — without them the standard library is not
compiled with atomic-wait support and the Web Worker thread pool cannot
start. The stable toolchain default WASM build (no `wasm-threads` feature)
does not need nightly and does not require these flags.

---

## JavaScript usage

### Initialise once, then simulate from a Web Worker

Because browsers forbid `atomics.wait` on the main thread (it would freeze
the UI), simulation functions that use `max_parallel_chunks > 1` must be
called **from inside a Web Worker**:

**worker.js**

```js
import init, { init_thread_pool, run_simulation } from "./pkg/moves_wasm.js";

// Step 1: load the WASM binary.
await init();

// Step 2: spin up the rayon Web Worker pool.
//   init_thread_pool returns a Promise that resolves when all workers are
//   ready. Call it once per Worker lifetime.
await init_thread_pool(navigator.hardwareConcurrency);

// Step 3: run a simulation with full parallelism.
//   max_parallel_chunks=0 → auto (equals navigator.hardwareConcurrency after
//   init_thread_pool has been called).
self.onmessage = async (event) => {
  const { runspecXml } = event.data;
  try {
    const result = run_simulation(runspecXml, navigator.hardwareConcurrency);
    self.postMessage({ ok: true, result });
  } catch (err) {
    self.postMessage({ ok: false, error: err.message });
  }
};
```

**main.js** (main browser thread)

```js
const worker = new Worker("./worker.js", { type: "module" });

worker.postMessage({ runspecXml: await runspecFile.text() });

worker.onmessage = ({ data }) => {
  if (data.ok) {
    for (const [path, bytes] of Object.entries(data.result)) {
      console.log(path, bytes.byteLength, "bytes");
    }
  }
};
```

### Single-threaded fallback (default build)

No Worker, no COEP/COOP headers, no feature flag needed:

```js
import init, { run_simulation } from "./pkg/moves_wasm.js";
await init();
const result = run_simulation(runspecXml, 1); // max_parallel_chunks=1 → sequential
```

Pass `0` for `max_parallel_chunks` to use the auto-resolved limit (also 1 in
this build).

---

## `max_parallel_chunks` parameter

`run_simulation(runspecXml, max_parallel_chunks)`:

| Value | Behaviour |
|---|---|
| `0` | Auto: resolves to `1` in the default build; to the rayon global pool thread count in the `wasm-threads` build (set by `init_thread_pool`). |
| `1` | Sequential — safe to call from the main thread. |
| `n > 1` | `n` chunks may run concurrently. Requires `wasm-threads` build + Web Worker context. |

---

## Memory model

Peak RSS ≈ `max_parallel_chunks × max(chunk working set)`. Doubling
`max_parallel_chunks` roughly doubles peak resident memory. Start with a low
value and sweep upward if memory headroom allows — the same guidance as the
native `--max-parallel-chunks` flag documented in `docs/user-guide.md`.

---

## Nginx / Caddy server header examples

**Nginx**

```nginx
add_header Cross-Origin-Opener-Policy "same-origin";
add_header Cross-Origin-Embedder-Policy "require-corp";
```

**Caddy**

```caddyfile
header {
    Cross-Origin-Opener-Policy "same-origin"
    Cross-Origin-Embedder-Policy "require-corp"
}
```

**Vite dev server** (`vite.config.ts`)

```ts
export default {
  server: {
    headers: {
      "Cross-Origin-Opener-Policy": "same-origin",
      "Cross-Origin-Embedder-Policy": "require-corp",
    },
  },
};
```

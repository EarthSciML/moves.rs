# 3-way default-DB end-to-end benchmark — canonical vs native vs wasm

_Generated 2026-06-15T02:53:29Z · branch fix/default-db-ev-sales-fraction @ 07e4406_


## default-DB end-to-end benchmark — canonical vs native vs wasm

All three run the **same** end-to-end simulation reading the **same** `movesdb20241112` default DB (MySQL form for canonical; converted parquet tree for native/wasm), serial config (canonical `main1worker`; native/wasm `max_parallel_chunks=1`).

- **Runtime (s)** — wall clock. Canonical includes per-run MariaDB start/stop + dual-JVM startup (fixed overhead the single-process Rust paths don't pay). `wasm eng` = compute-only `engine.run()` phase.
- **Memory (MB)** — canonical = peak aggregate RSS of the whole process tree (master JVM + worker JVM + mariadbd + Go calc, sampled); native/wasm = single-process peak RSS (`/usr/bin/time -v`).
- **rel_diff** — max per-pollutant relative error vs the canonical MOVES snapshot (canonical = reference ≡ 0); gate-verified.

| fixture | canon s | native s | wasm s | wasm eng s | canon MB | native MB | wasm MB | native rel_diff | wasm rel_diff |
|---|---|---|---|---|---|---|---|---|---|
| chain-nonhaptog | 31.97 | 0.24 | 1.43 | 0.09 | 395 | 206 | 621 | -8.92e-08 | -8.92e-08 |
| chain-tog-speciation | 33.03 | 0.22 | 1.39 | 0.09 | 418 | 206 | 621 | -8.92e-08 | -8.92e-08 |
| expand-counties | 36.97 | 0.39 | 1.60 | 0.24 | 365 | 116 | 618 | -3.42e-04 | -3.42e-04 |
| expand-criteria | 44.89 | 0.38 | 1.61 | 0.25 | 616 | 247 | 661 | -3.46e-07 | -3.46e-07 |
| expand-day | 41.11 | 0.22 | 1.42 | 0.11 | 373 | 130 | 609 | -3.47e-04 | -3.47e-04 |
| expand-fueltype-diesel | 36.60 | 0.32 | 1.56 | 0.17 | 365 | 124 | 611 | -2.96e-04 | -2.96e-04 |
| expand-month | 43.63 | 0.25 | 1.57 | 0.11 | 381 | 131 | 610 | -3.80e-04 | -3.80e-04 |
| expand-sourcetype | 42.12 | 0.54 | 1.74 | 0.36 | 355 | 124 | 611 | -1.29e-04 | -1.29e-04 |
| mixed-onroad | 42.66 | 0.22 | 1.37 | 0.08 | 346 | 120 | 610 | -3.29e-04 | -3.29e-04 |
| process-airtoxics | 29.88 | 0.26 | 1.45 | 0.11 | 565 | 200 | 621 | 1.74e-07 | 1.74e-07 |
| process-brakewear | 31.87 | 0.20 | 1.33 | 0.08 | 369 | 125 | 617 | -3.47e-04 | -3.47e-04 |
| process-crankcase-running | 35.76 | 0.27 | 1.43 | 0.14 | 512 | 233 | 654 | -3.46e-07 | -3.46e-07 |
| process-evap-fvv | 55.15 | 1.17 | 1.91 | 0.37 | 530 | 146 | 606 | -1.12e-07 | -1.12e-07 |
| process-evap-leaks | 52.09 | 0.26 | 1.39 | 0.14 | 515 | 150 | 607 | -5.16e-07 | -5.16e-07 |
| process-evap-permeation | 59.90 | 0.26 | 1.42 | 0.17 | 529 | 131 | 607 | 1.58e-07 | 1.58e-07 |
| process-nox-speciation | 32.69 | 0.20 | 1.38 | 0.07 | 387 | 207 | 620 | -4.88e-07 | -4.88e-07 |
| process-pm-exhaust | 34.76 | 0.25 | 1.43 | 0.11 | 361 | 220 | 635 | -4.58e-07 | -4.58e-07 |
| process-refueling | 37.89 | 0.34 | 1.43 | 0.18 | 375 | 129 | 610 | -2.77e-07 | -2.77e-07 |
| process-tirewear | 35.02 | 0.19 | 1.37 | 0.08 | 338 | 118 | 617 | -3.47e-04 | -3.47e-04 |
| sample-runspec | 32.89 | 0.23 | 1.42 | 0.10 | 353 | 110 | 610 | -1.58e-06 | -1.58e-06 |

### Aggregates (20 fixtures)

| metric | mean | min | max |
|---|---|---|---|
| canonical wall s | 39.5 | 29.9 | 59.9 |
| native wall s | 0.32 | 0.19 | 1.17 |
| wasm wall s | 1.48 | 1.33 | 1.91 |
| wasm engine.run s | 0.15 | 0.07 | 0.37 |
| canonical tree-peak MB | 422 | 338 | 616 |
| native peak MB | 159 | 110 | 247 |
| wasm peak MB | 619 | 606 | 661 |

Mean wall-clock speedup vs canonical: native **123×**, wasm **27×**.

CSV: /tmp/ddb-bench/benchmark.csv

## Key findings

- **Numerical: native and wasm are correct and identical.** Every fixture's
  `rel_diff` vs the canonical MOVES snapshot is identical between native and
  wasm (the wasm partition data-plane mirrors the native InputDataManager
  path exactly). Worst case is ~3.8e-4 (the `expand-*` / wear fixtures, a
  preaggregation rounding floor); most are ~1e-7. Both gates: **28/28
  asserted-pass** on this exact regenerated tree.
- **Runtime: the Rust paths are 1–2 orders of magnitude faster end-to-end.**
  Mean wall ~39.5 s (canonical) vs ~0.32 s (native) vs ~1.48 s (wasm). The bulk
  of canonical's cost is fixed per-run overhead — MariaDB start/stop + building
  a fresh execution DB + dual-JVM startup — that the single-process Rust paths
  don't pay; the actual `engine.run()` compute is ~0.15 s mean.
- **native faster than wasm here** because the native CLI run reuses the
  warm OS page cache for the parquet tree, while the wasm harness re-reads the
  required partitions and pays ~1.2 s of fixed test-binary + load overhead each
  invocation (`wasm eng` isolates the compute at ~0.15 s, on par with native).
- **Memory:** native ~159 MB, canonical tree-peak ~422 MB (summed across
  master JVM + worker JVM + mariadbd + Go calc), wasm ~619 MB (the wasm path
  vstacks all required partitions + polars in one process; higher but well
  within a browser tab budget).

## Caveats

- **Not a strict parallelism apples-to-apples.** Canonical `main1worker` is
  single-worker but inherently multi-process; native/wasm are `chunks=1`
  single-process. There is no exact knob equivalence.
- **Canonical wall includes JVM+MariaDB startup**, so the speedup overstates
  the pure-compute gap; `wasm eng` / native give the closer compute-only view.
- **Shared host.** Measured on a 20-core node under concurrent unrelated load
  (load avg ~24); absolute numbers carry noise, but native/wasm ran back-to-back
  per fixture and canonical runs were isolated, so the relative picture holds.
- Memory metrics differ by construction (canonical = process-tree sampled;
  native/wasm = single-process `/usr/bin/time -v` peak).

## Reproduce

```sh
# 1. canonical SIF (already built): characterization/apptainer/build-sif.sh
# 2. regenerate the parquet default-DB tree (persistent):
characterization/default-db-conversion/convert-default-db.sh --output /scratch.local/$USER/moves-defaultdb
# 3. canonical timings (tree-RSS sampled):
characterization/audit/canonical-benchmark.sh
# 4. native+wasm timings (release, chunks=1):
MOVES_DEFAULT_DB_DIR=/scratch.local/$USER/moves-defaultdb/movesdb20241112 \
  characterization/audit/default-db-benchmark.sh
# 5. numerical rel_diff (both gates, 28/28):
MOVES_DEFAULT_DB_DIR=... cargo test -p moves-cli --test full_suite_regression default_db_snapshot_diff -- --nocapture
MOVES_DEFAULT_DB_DIR=... cargo test -p moves-wasm default_db_canonical_diff -- --nocapture
# 6. assemble:
python3 characterization/audit/default-db-benchmark-report.py /tmp/ddb-bench \
  --canonical-dir /tmp/ddb-bench-canonical --native-gate /tmp/native-gate.log --wasm-gate /tmp/wasm-gate.log
```

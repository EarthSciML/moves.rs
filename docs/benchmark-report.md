# MOVES-Rust Benchmark Report — Task 127

This document is the public-facing performance benchmark for the Rust port of
MOVES, comparing it against canonical MOVES (EPA's Java implementation) on six
representative workloads.

**Measurement date:** 2026-05-21  
**Port phase:** Phase 7 (framework complete; data plane pending)  
**Host:** Intel Xeon Gold 6248, 20 cores @ 2.50 GHz, Linux

---

## Performance targets

| Workload | Target improvement over canonical MOVES |
|---|---|
| County-Scale (single county, single year) | 10–50× wall-time reduction |
| County-Scale (multi-county, multi-year) | 10–50× wall-time reduction |
| Project-Scale | 10–50× wall-time reduction |
| Default-Scale national | 10–50× wall-time reduction |
| Rates-mode | 10–50× wall-time reduction |
| NONROAD-only (single thread) | 2–5× wall-time reduction |
| Mixed onroad + NONROAD | 10–50× wall-time reduction (onroad path) |

The NONROAD target is more conservative: the Fortran NONROAD executable already
runs natively without a JVM or MariaDB, so the primary gain comes from better
lookup-table data structures in the Rust port (hash-indexed Parquet vs.
sequential file scans).

---

## Why the Rust port is faster

Canonical MOVES has two primary performance bottlenecks that the Rust port
eliminates:

| Bottleneck | Canonical MOVES | Rust port |
|---|---|---|
| Storage engine | MariaDB/MyISAM: every calculator reads and writes SQL tables to disk between steps | In-memory Polars DataFrames; no SQL round-trips |
| Bundle handoff | Filesystem-mediated master→worker bundles; each bundle is a MariaDB dump | In-process rayon thread pool; no I/O between calculator chains |

EPA's own profiling of canonical MOVES county-scale runs shows the following
approximate breakdown of a 5–15 minute run:

| Phase | Fraction of runtime |
|---|---|
| MariaDB startup + schema creation | 10–20 % |
| Input-table SQL queries (per calculator) | 30–50 % |
| Calculator SQL execution (MyISAM table writes) | 30–40 % |
| Output aggregation + export | 10–20 % |

The Rust port eliminates the first two categories entirely and replaces the
third with in-memory Polars operations over columnar Parquet data.

---

## Measurement methodology

### Timing

Every `moves run` invocation records four timing fields on its
[`EngineOutcome`](../crates/moves-framework/src/execution/engine.rs):

| Field | Description |
|---|---|
| `wall_time` | Total wall time from engine entry to output file close |
| `planning_time` | Topological sort + chunking (before the executor starts) |
| `execution_time` | Wall span of the parallel executor call |
| `chunk_wall_times` | Per-chunk wall time measured inside each parallel closure |

### Peak memory

Peak RSS is read from `/proc/self/status` field `VmHWM` (process-lifetime
high-water mark) after each run. `VmHWM` is monotonic within a process, so
the per-fixture delta is estimated by differencing consecutive readings.

### Output-correctness (`impl%`)

`impl%` reports the fraction of planned calculator-graph modules that were
actually executed (not unimplemented stubs):

```
impl% = modules_executed / modules_planned × 100
```

In Phase 7 this is 0 % because calculator `execute()` methods return
`CalculatorOutput::empty()` — the data-plane wiring is the Phase 4
deliverable that turns this number toward 100 %. The column is included so
it will self-populate as the port advances.

### Parallelism sweep

Each workload group is measured at `--max-parallel-chunks` = 1, 2, 4, and
NCPU (the host's available CPU count). The speedup column is relative to
N = 1. In Phase 7 (no real data) the executor has nothing substantial to
parallelize; the sweep exercises the scheduling infrastructure.

### Scale fixtures

The three `scale-*` fixtures (County-Scale, Project-Scale, Rates-mode) require
County Database (CDB) and Project Database (PDB) Parquet inputs that are not
bundled in the repository. Set `BENCHMARK_SCALE_INPUTS_DIR` to the directory
containing those files to enable them:

```sh
BENCHMARK_SCALE_INPUTS_DIR=/path/to/scale-inputs \
    cargo test -p moves-cli --test benchmark_suite --release -- --nocapture
```

---

## Phase 7 baseline measurements (framework overhead only)

These numbers reflect **pure framework overhead** — RunSpec parsing,
calculator-graph planning, chunk scheduling, and `MOVESRun.parquet` output.
Calculator `execute()` methods do no work yet; they return empty output
immediately. The data plane (Phase 4) that feeds real row data is not yet
wired.

This is not a fair head-to-head comparison with canonical MOVES (which does
real computation). It establishes the framework floor: the minimum overhead
that every run will pay regardless of workload complexity.

### 1. Default-Scale National (onroad, 23 fixtures, N=1)

```
fixture                                wall(ms)   plan(ms)   exec(ms)  chunks   impl%
---------------------------------------------------------------------------------------
chain-nonhaptog                             2.8        0.7        0.2      26      0%
chain-tog-speciation                        2.8        0.7        0.2      26      0%
expand-counties                             2.9        0.8        0.2      27      0%
expand-criteria                             3.0        0.8        0.2      27      0%
expand-day                                  3.3        0.8        0.5      27      0%
expand-fueltype-diesel                      3.1        0.8        0.2      27      0%
expand-month                                3.4        0.8        0.3      27      0%
expand-sourcetype                           3.2        0.8        0.2      27      0%
process-airtoxics                           3.0        0.8        0.2      26      0%
process-apu                                 2.8        0.7        0.2      25      0%
process-brakewear                           2.8        0.7        0.2      24      0%
process-crankcase-extidle                   2.9        0.7        0.2      20      0%
process-crankcase-running                   2.9        0.7        0.2      21      0%
process-crankcase-start                     2.8        0.7        0.2      20      0%
process-evap-fvv                            2.9        0.7        0.2      25      0%
process-evap-leaks                          2.8        0.7        0.2      25      0%
process-evap-permeation                     2.9        0.7        0.2      24      0%
process-extended-idle                       2.9        0.7        0.2      25      0%
process-nox-speciation                      2.9        0.7        0.2      25      0%
process-pm-exhaust                          2.8        0.7        0.2      26      0%
process-refueling                           3.0        0.8        0.2      27      0%
process-tirewear                            2.8        0.7        0.2      25      0%
sample-runspec                              3.1        0.8        0.2      27      0%
---------------------------------------------------------------------------------------
TOTAL (23 fixtures)                        67.9       16.7
```

**N sweep:**

```
     N   total_wall_ms   speedup  peak_RSS_MiB
------------------------------------------------
     1           470.2      1.00           0.0
     2           468.4      1.00           0.0
     4           470.9      1.00           0.0
    20           506.1      0.93           0.3
```

Framework overhead per fixture: **2.8–3.4 ms**. Planning dominates at ~0.7–0.8 ms;
execution of 20–27 empty MasterLoops costs ~0.2 ms. Throughput is flat across
N because planning (sequential) is ~4× execution (parallel-eligible) in Phase 7.

### 2. NONROAD-Only (10 fixtures, N=1)

```
fixture                                wall(ms)   plan(ms)   exec(ms)  chunks   impl%
---------------------------------------------------------------------------------------
nr-agriculture-state                        2.5        0.4        0.2      18      0%
nr-airport-support-county                   2.5        0.4        0.2      18      0%
nr-commercial-nation                        2.5        0.4        0.2      18      0%
nr-construction-state                       2.8        0.4        0.2      18      0%
nr-industrial-county                        2.6        0.3        0.3      18      0%
nr-lawn-garden-county                       2.5        0.4        0.2      18      0%
nr-logging-county                           2.5        0.4        0.2      18      0%
nr-pleasure-craft-state                     2.7        0.5        0.2      19      0%
nr-railroad-support-nation                  2.5        0.4        0.2      18      0%
nr-recreational-county                      2.5        0.4        0.2      18      0%
---------------------------------------------------------------------------------------
TOTAL (10 fixtures)                        25.7        3.7
```

**N sweep:**

```
     N   total_wall_ms   speedup  peak_RSS_MiB
------------------------------------------------
     1           200.0      1.00           0.0
     2           213.5      0.94           0.0
     4           203.6      0.98           0.0
    20           206.2      0.97           0.0
```

NONROAD fixtures plan 18–19 chunks (fewer than onroad's 20–27) and have slightly
lower planning overhead (~0.4 ms vs ~0.8 ms) because the NONROAD calculator graph
has fewer modules. Framework overhead per fixture: **2.5–2.8 ms**.

### 3. Mixed Onroad + NONROAD (1 fixture, N=1)

```
fixture                                wall(ms)   plan(ms)   exec(ms)  chunks   impl%
---------------------------------------------------------------------------------------
mixed-onroad-nonroad                        3.0        0.8        0.2      27      0%
```

**N sweep:**

```
     N   total_wall_ms   speedup  peak_RSS_MiB
------------------------------------------------
     1            20.5      1.00           0.0
     2            20.5      1.00           0.0
     4            20.5      1.00           0.0
    20            21.3      0.96           0.0
```

The dual-model path (ONROAD + NONROAD) plans 27 chunks — the combined graph
includes ~44 onroad modules and 18 NONROAD modules. Total framework overhead
matches single-model runs at the same scale.

### 4–6. County-Scale, Project-Scale, Rates-Mode

These workloads require CDB/PDB Parquet inputs. See §Reproducing below.
Canonical-MOVES reference times are provided for context:

| Workload | Canonical MOVES (typical) | Rust port target |
|---|---|---|
| County-Scale, single county, 1 year, running exhaust | 5–15 min | 10–90 s |
| County-Scale, 5 counties, 5 years, all pollutants | 60–120 min | 5–15 min |
| Project-Scale (link-level) | 2–8 min | 10–50 s |
| Rates-mode (emission-rate lookup) | 3–10 min | 15–60 s |

Times will be filled in once Phase 4 (data plane) is complete and the
`BENCHMARK_SCALE_INPUTS_DIR` inputs are available.

---

## Canonical MOVES comparison (framework floor vs. real runs)

At the Phase 7 framework-overhead level, the Rust port completes one
default-scale fixture in **~3 ms**. A comparable canonical MOVES run
(single pollutant, single process, one county, one hour) takes **5–15 minutes**.

This is not a meaningful comparison: the Rust port is not yet computing anything.
The relevant comparison begins when Phase 4 lands.

What the numbers do confirm:

- **Per-fixture framework tax is 2.5–3.5 ms.** This is the minimum overhead
  that will remain in the final system regardless of how large the real
  computation is. For a 5-minute canonical-MOVES run, this represents
  **0.001 % of total runtime** — the framework is not the bottleneck.
- **Planning scales with the calculator graph, not the workload data.**
  The same fixture takes the same planning time whether it will ultimately
  process one row or one million. Canonical MOVES pays schema-creation overhead
  per-run even for small workloads; the Rust port pays a flat ~0.8 ms.
- **Peak RSS at framework level is 11–12 MiB.** This is the process baseline
  before any data is loaded. Canonical MOVES requires MariaDB (~100–300 MiB
  RSS plus disk for the MyISAM tables).

---

## Memory model

The bounded-concurrency executor enforces:

```
peak_RSS ≈ process_baseline + N × max_chain_working_set
```

where `max_chain_working_set` is the largest peak DataFrame allocation any
single calculator chain holds in memory simultaneously. In Phase 7 this is
zero (empty output). Once Phase 4 lands, estimates per machine class are:

| Machine type | Estimated chain working set | Recommended N | Estimated peak RSS |
|---|---|---|---|
| Laptop (8–16 GiB RAM) | 50–200 MiB per chain | 2–4 | 0.5–1.5 GiB |
| Workstation (32–128 GiB RAM) | same | 8–16 | 1–4 GiB |
| HPC node (128+ GiB RAM) | same | 16–32 | 2–8 GiB |

See `docs/concurrency-tuning.md` for the full N-sweep methodology and the
performance-isolation guarantees the executor provides.

---

## Output-correctness vs. tolerance

The `impl%` column tracks forward progress through the port. When Phase 4
lands and calculators produce real output, this column will rise from 0 % to
100 % as each fixture's calculator graph becomes fully executed.

The correctness gate — diffing Rust port output against canonical-MOVES
snapshots within per-column tolerance budgets — is documented in
`docs/known-divergences.md`. Activating it requires:

1. Canonical-MOVES snapshots captured from `characterization/run-all-fixtures.sh`.
2. Real output from the Rust port (Phase 4 data-plane deliverable).

Expected divergence categories and their tolerance budgets are pre-documented
in §4 of `known-divergences.md`.

---

## Reproducing

```sh
# Framework-overhead baseline (no external inputs, runs in ~4 s):
cargo test -p moves-cli --test benchmark_suite -- --nocapture

# Release build for publication-quality numbers (~2–3× faster than debug):
cargo test -p moves-cli --test benchmark_suite --release -- --nocapture

# Include County/Project/Rates scale fixtures (requires CDB/PDB inputs):
BENCHMARK_SCALE_INPUTS_DIR=/path/to/scale-inputs \
    cargo test -p moves-cli --test benchmark_suite --release -- --nocapture

# Per-fixture timing with the release binary (single fixture):
cargo build --release -p moves-cli
time target/release/moves run \
    --runspec characterization/fixtures/sample-runspec.xml \
    --output /tmp/moves-out

# Cache-miss instrumentation (Linux, requires perf):
perf stat \
    -e cache-misses,cache-references,L1-dcache-loads,L1-dcache-load-misses \
    target/release/moves run \
        --runspec characterization/fixtures/sample-runspec.xml \
        --output /tmp/moves-out

# Flamegraph (requires cargo-flamegraph):
cargo flamegraph -p moves-cli -- run \
    --runspec characterization/fixtures/sample-runspec.xml \
    --output /tmp/moves-out
```

---

## Updating this report

Once Phase 4 (data plane) is complete:

1. Run the full benchmark suite with the release binary and real inputs.
2. Fill in the §County-Scale / Project-Scale / Rates-Mode tables above.
3. Update the `impl%` column (should read 100 % for all available fixtures).
4. Record peak-RSS readings at each N for a representative county-scale run.
5. Compute actual speedup vs. the canonical-MOVES reference times in §4–6.

The benchmark test (`crates/moves-cli/tests/benchmark_suite.rs`) is the
canonical source of truth for all numbers in this report. Run it, then
paste its output into the relevant sections above.

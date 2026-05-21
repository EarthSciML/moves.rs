# Performance Baseline — Task 75

This document records the performance baseline for the Rust port of MOVES,
as established by migration-plan Task 75.  It covers methodology, current
(Phase 3, pre-data-plane) numbers, and the expected comparison to canonical
MOVES once the data plane is connected.

---

## Background

Canonical MOVES (Java) has two primary sources of overhead that the Rust
port eliminates:

| Overhead source | Canonical MOVES | Rust port |
|-----------------|-----------------|-----------|
| Storage engine | MariaDB/MyISAM: every calculator reads/writes SQL tables on disk | In-memory Polars DataFrames; no SQL |
| Bundle handoff | Filesystem-mediated master→worker bundles; each bundle is a MariaDB dump | In-process rayon thread pool; no I/O |

The migration plan expects a **5–20× wall-time improvement** on identical
hardware from eliminating these two bottlenecks alone, before any
algorithmic improvements.

---

## Measurement methodology

### Timing

The engine records three `std::time::Duration` fields on every
[`EngineOutcome`](../crates/moves-framework/src/execution/engine.rs):

| Field | Description |
|-------|-------------|
| `wall_time` | Total wall time from entry of `MOVESEngine::run` to return, including output-file I/O |
| `planning_time` | Topological-sort + chunking (before the executor starts) |
| `execution_time` | Wall span of the `BoundedExecutor::execute` call; parallel chunks overlap |
| `chunk_wall_times` | Per-chunk wall time measured within each parallel closure |

The `moves run` subcommand prints these to stdout.

### Peak memory

Peak RSS is read from `/proc/self/status` field `VmHWM` (the
process-lifetime high-water mark) after the run completes.  `VmHWM` is
monotonic, so a reading at run-end captures the peak from within the run.
Returns `None` on non-Linux hosts.

### Cache miss rate

Hardware performance counters require an external tool.  To collect cache
statistics for a single fixture:

```
perf stat \
  -e cache-misses,cache-references,L1-dcache-loads,L1-dcache-load-misses \
  target/release/moves run \
    --runspec characterization/fixtures/process-airtoxics.xml \
    --output /tmp/out
```

The full fixture suite can be driven with the release binary via
`characterization/run-all-fixtures.sh` (requires Apptainer + the canonical
MOVES SIF for comparison runs).

---

## Phase 3 baseline (pre-data-plane, 2026-05-21)

Measured with `cargo test -p moves-cli --test perf_baseline -- --nocapture`
on the CI host (`--max-parallel-chunks 1` for reproducible serialised
timing).  All 23 onroad fixtures (excludes NONROAD `nr-*` and the
`scale-*` fixtures that need supplementary input databases).

**What this measures today:** Calculators return `CalculatorOutput::empty()`
— the compute cores are ported but the data plane that feeds real row data
is not yet wired in. These numbers represent **pure framework overhead**:
RunSpec parsing, calculator-graph planning and chunking, MasterLoop setup
and iteration (zero rows per iteration), and `MOVESRun.parquet` output.

```
fixture                                  wall(ms)   plan(ms)   exec(ms)     chunks RSS(MiB)
--------------------------------------------------------------------------------------------
chain-nonhaptog                               4.7        0.9        0.2         26     11.6
chain-tog-speciation                          3.1        0.8        0.2         26     11.6
expand-counties                               3.2        0.8        0.2         27     11.6
expand-criteria                               3.2        0.9        0.2         27     11.6
expand-day                                    3.4        0.8        0.5         27     11.6
expand-fueltype-diesel                        3.1        0.8        0.2         27     11.6
expand-month                                  3.1        0.8        0.2         27     11.6
expand-sourcetype                             3.1        0.8        0.2         27     11.6
process-airtoxics                             2.8        0.7        0.2         26     11.6
process-apu                                   2.9        0.7        0.2         25     11.6
process-brakewear                             3.1        0.7        0.2         24     11.6
process-crankcase-extidle                     2.8        0.7        0.2         20     11.6
process-crankcase-running                     2.9        0.7        0.2         21     11.6
process-crankcase-start                       2.8        0.6        0.2         20     11.6
process-evap-fvv                              2.9        0.7        0.2         25     11.6
process-evap-leaks                            3.5        0.7        0.2         25     11.6
process-evap-permeation                       2.8        0.7        0.2         24     11.6
process-extended-idle                         2.8        0.7        0.2         25     11.6
process-nox-speciation                        2.9        0.7        0.2         25     11.6
process-pm-exhaust                            2.9        0.7        0.2         26     11.6
process-refueling                             3.2        0.8        0.2         27     11.6
process-tirewear                              2.9        0.7        0.2         25     11.6
sample-runspec                                3.1        0.8        0.2         27     11.6
--------------------------------------------------------------------------------------------
TOTAL (23 fixtures)                          70.9       17.0        4.9
```

Key observations:

- **Total framework overhead per fixture: 2.8–4.7 ms.** Planning dominates
  (~0.7–0.9 ms); execution of 20–27 empty MasterLoops is ~0.2 ms.
- **Peak RSS: 11.6 MiB.** This is the process baseline; each calculator
  chain's working set adds to this when the data plane lands.
- **Planning time scales with chunk count**, not fixture complexity —
  the topological sort and DAG traversal is the dominant term.

---

## Comparison to canonical MOVES

Canonical MOVES county-scale runs (single county, single year, running
exhaust only) typically take **5–15 minutes** on the same class of hardware.
The Java profiling breakdown (from EPA's own benchmarks and community
reports) is approximately:

| Phase | Fraction of runtime |
|-------|---------------------|
| MariaDB startup + schema creation | 10–20% |
| Input-table SQL queries (per calculator) | 30–50% |
| Calculator SQL execution (MyISAM table writes) | 30–40% |
| Output aggregation + export | 10–20% |

At the Phase 3 framework-overhead level the Rust port completes **one
full fixture in under 5 ms** versus **5–15 minutes** for canonical MOVES
— nominally 60,000–180,000× faster. This is not a fair comparison: the
calculators do no real work. The correct comparison comes once the data
plane (Phase 4) is connected:

- Input table reads (Parquet vs MyISAM SQL) should be 10–50× faster.
- Per-calculator computation (Polars in-memory vs MyISAM write-read) should
  be 5–20× faster.
- No MariaDB startup or schema creation overhead.

The migration-plan target of **5–20× overall improvement** is conservative
given the MariaDB elimination; the real gain may be higher for I/O-bound
workloads.

---

## Reproducing the measurements

```bash
# Framework-overhead baseline (no real data):
cargo test -p moves-cli --test perf_baseline -- --nocapture

# Single-fixture timing with the release binary:
cargo build --release -p moves-cli
target/release/moves run \
  --runspec characterization/fixtures/process-airtoxics.xml \
  --output /tmp/moves-out

# Cache miss rate (Linux, requires perf):
perf stat \
  -e cache-misses,cache-references,L1-dcache-loads,L1-dcache-load-misses \
  target/release/moves run \
    --runspec characterization/fixtures/process-airtoxics.xml \
    --output /tmp/moves-out

# Profiling with flamegraph (requires cargo-flamegraph):
cargo flamegraph -p moves-cli -- run \
  --runspec characterization/fixtures/process-airtoxics.xml \
  --output /tmp/moves-out
```

---

## Next steps

- **Task 76** — Concurrency tuning: sweep `--max-parallel-chunks` from 1
  to NCPU and document the throughput/RSS tradeoff.
- **Task 77** — Concurrency correctness: verify byte-identical output at
  all parallelism levels.
- **Task 127** — Public benchmark suite: full comparison against canonical
  MOVES once the data plane is connected in Phase 4.

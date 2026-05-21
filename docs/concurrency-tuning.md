# Concurrency Tuning — Task 76

This document records the `--max-parallel-chunks` sweep results and recommended
settings for the Rust MOVES port, as established by migration-plan Task 76.

---

## Background

The bounded-concurrency executor (Task 27) runs independent calculator chains
in parallel, capping concurrent chains at `--max-parallel-chunks`.  Peak memory
scales linearly with the limit:

```
peak_RSS ≈ process_baseline + N × max_chain_working_set
```

where `max_chain_working_set` is the largest peak DataFrame allocation any
single chain needs.  Setting `N` too high wastes memory; setting it too low
leaves CPU cores idle and reduces throughput.

The executor guarantees the bound deterministically — each chain holds a
semaphore permit for its entire execution, so at most `N` working sets are
resident at once.  The unit tests in `crates/moves-framework/src/execution/executor.rs`
(the barrier-based concurrency test) and the integration test in
`crates/moves-framework/tests/memory_pressure.rs` (real 8 MiB buffers + VmHWM)
both pin this relationship.

---

## Phase 3 measurement (pre-data-plane, 2026-05-21)

**Method:** `cargo test -p moves-cli --test concurrency_tuning -- --nocapture`
on the CI host (20-core Intel Xeon Gold 6248 @ 2.50 GHz).  Sweeps N from 1 to
NCPU on all 23 onroad fixtures.  Each cell reports total wall time across all
23 fixtures and the process peak RSS (VmHWM) after that sweep.

**Phase 3 caveat:** Calculators return `CalculatorOutput::empty()` —
no data-plane working set exists yet.  These numbers reflect **framework
overhead** only: RunSpec parsing, calculator-graph planning, MasterLoop setup,
and `MOVESRun.parquet` output.  The per-chain working set is essentially zero.

```
           N  total_wall_ms  speedup   RSS_MiB  delta_RSS_MiB
--------------------------------------------------------------
           1          480.8     1.00      10.6           10.6
           2          488.3     0.98      10.6            0.0
           4          483.1     1.00      10.6            0.0
           8          513.9     0.94      10.6            0.0
          16          500.5     0.96      10.6            0.0
   20 (NCPU)          507.8     0.95      10.6            0.0
```

Key observations:

- **Throughput is flat** — speedup ≈ 1.0 at all N.  In Phase 3, planning
  (~0.8 ms per fixture) dominates over execution (~0.2 ms), and planning is
  sequential.  There is nothing to parallelize across.

- **Peak RSS is flat at 10.6 MiB** — delta is 0.0 MiB beyond N=1.  This
  confirms the chain-isolation model: with `max_chain_working_set ≈ 0`, memory
  is constant regardless of N.  No unexpected state sharing exists between
  calculator chains.

- **N has no measurable effect on Phase 3 runs.**  Any value from 1 to NCPU
  produces equivalent performance and memory footprint.

---

## Memory model validation

The measurement validates the invariant from `executor.rs`:

```
peak_RSS(N=20) = 10.6 MiB = peak_RSS(N=1)
```

With empty working sets, `N × 0 = 0` additional memory is expected.  The
integration test in `crates/moves-cli/tests/concurrency_tuning.rs` asserts this
programmatically:

```
rss(N=NCPU) ≤ rss(N=1) + (NCPU−1) × 2 MiB/slot
```

A violation of this bound means calculator chains are sharing unexpected mutable
state — a correctness bug masquerading as a memory issue, not a tuning problem.

---

## Phase 4 projections (once data plane lands)

When Phase 4 connects the Parquet data plane, each calculator chain holds a
working set proportional to the DataFrame columns it reads and writes.  The
memory model then becomes load-bearing:

| Machine type | Estimated chain working set | Recommended N | Est. peak RSS |
|---|---|---|---|
| Laptop (8–16 GiB RAM) | 50–200 MiB per chain (county-scale run) | 2–4 | 0.5–1.5 GiB |
| Workstation (32–128 GiB RAM) | same | 8–16 | 1–4 GiB |
| Server (256+ GiB RAM) | same | 32+ | 3–12 GiB |

The recommended N values are soft: the right choice depends on actual working
set size (fixture scale, pollutant count), available RAM, and whether the run is
memory-bound or CPU-bound.

**Tuning procedure (Phase 4):**

1. Run a representative fixture with `--max-parallel-chunks 1` and record peak
   RSS.  This is `process_baseline + 1 × max_chain_working_set`.
2. Run the same fixture with `--max-parallel-chunks 4`.  Check that peak RSS ≈
   `baseline + 4 × working_set`.  If it is, the model holds and you can predict
   RSS at any N.
3. Choose N so that predicted peak RSS stays under 50–70% of available RAM
   (leave headroom for OS page cache and other processes).

---

## Reproducing the measurements

```bash
# Sweep all N values, print table (unoptimized / debug build):
cargo test -p moves-cli --test concurrency_tuning -- --nocapture

# Single-fixture timing across N=1, 4, NCPU:
cargo test -p moves-cli --test concurrency_tuning all_parallelism_levels

# Executor memory-pressure regression (real 8 MiB buffers, isolated process):
cargo test -p moves-framework --test memory_pressure -- --nocapture
```

---

## Next steps

- **Task 77** — Concurrency correctness: verify byte-identical output at N=1,
  4, NCPU (completed; see `crates/moves-cli/tests/concurrency_correctness.rs`).
- **Task 78** — Phase 3 closing checkpoint.
- **Phase 4** — Data plane landing; re-run this sweep with real working sets,
  update the table and recommended-N guidance above.

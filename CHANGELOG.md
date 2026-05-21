# Changelog

## v0.1.0 — 2026-05-21

First public release of `moves.rs`, a pure-Rust port of EPA's MOVES
on-road and NONROAD emissions model.

### Scope — what's in this release

#### Onroad model

All ~70 onroad emission calculators are implemented as Rust crates in the
`moves-calculators` workspace member, covering every emission process in
canonical MOVES:

- Running exhaust, start exhaust, extended idle
- Brakewear, tirewear
- Evaporative emissions: permeation, fuel vapor venting, fuel leaks,
  refueling displacement, refueling spillage
- Crankcase variants (running, start, extended idle)
- Auxiliary power unit (APU)
- HC/TOG speciation and NOx speciation chains
- Air toxics

All calculator chains from `CalculatorInfo.txt` are reconstructed; the
calculator-graph planner correctly plans 40–44 modules per fixture across
the 34-fixture characterization suite.

#### NONROAD model

The 29k-line Fortran NONROAD2008a model is ported to pure Rust in
`crates/moves-nonroad`. All equipment categories are covered. The port
uses `f64` throughout (vs. Fortran `real*4`), producing slightly more
accurate results with documented, bounded divergence from the canonical
Fortran reference.

#### Control strategies (Phase 6)

All four canonical control strategies are implemented:

| Strategy | Crate |
|---------|-------|
| Alternative Vehicle Fuels & Technologies (AVFT) | `moves-avft` |
| Rate-of-Progress (ROP) | `moves-rate-of-progress` |
| OnRoadRetrofit | `moves-onroad-retrofit` |
| Low-Emission Vehicle (LEV) | `moves-import-lev` |

#### RunSpec formats

Both the canonical MOVES XML format (`.xml` / `.mrs`) and a new TOML
format are fully supported. An existing RunSpec that runs in canonical
MOVES can be passed directly to `moves run --runspec <path>` without
modification.

#### Input database importers

- County Database (CDB) importer (`moves-importer-county`)
- Project Database (PDB) importer (`moves-importer-pdb`)
- NONROAD input importer (`moves-nonroad-import`)

#### Default-database conversion

`moves-default-db-convert` converts an EPA MOVES default-DB MariaDB dump
(e.g., `movesdb20241112.zip`) to the partitioned Parquet layout the port
uses, with no MariaDB installation required.

#### WebAssembly

The full onroad + NONROAD port compiles to `wasm32-unknown-unknown` and
runs in modern browsers. A minimal browser demo is included at
`crates/moves-wasm/demo/`. Multi-threaded WASM (via the Threads proposal
+ `rayon` on Web Workers) is supported with the `wasm-threads` feature flag.

#### Characterization infrastructure

- 37-fixture characterization suite exercising every emission process,
  every NONROAD equipment category, and mixed onroad+NONROAD runs
- `moves-snapshot` crate for producing and diffing fixture outputs
- `characterization/tolerance.toml` for per-column divergence budgets
- Weekly CI workflow running the full fixture suite against a pinned
  canonical-MOVES container image

#### Upstream-tracking automation

`scripts/upstream-update.sh` and `.github/workflows/upstream-update.yml`
automate detection and incorporation of EPA's annual MOVES default-DB
updates. See [docs/upstream-tracking.md](docs/upstream-tracking.md).

---

### Known divergences and limitations

#### Data-plane gap (most significant)

**`moves run` does not yet produce emission rows.** The calculator
implementations are complete, but the Phase 4 data plane that feeds
per-row default-database lookups into the calculator context is not yet
wired. `moves run` will:

1. Parse your RunSpec correctly
2. Plan the full calculator graph (reporting planned module count)
3. Write `MOVESRun.parquet` with run metadata
4. Create empty output Parquet partitions

The executed-module count will read 0 (or a small number of stub modules).
Emission numbers require the Phase 4 data-plane wiring, which is the next
major milestone.

**Practical use of v0.1:** validate RunSpec parsing, inspect planned
calculator graphs, exercise control-strategy configuration, test WASM
browser integration.

#### County Scale and Project Scale

`import-cdb` and `import-pdb` produce validated Parquet from user-supplied
input files, but the data-plane wiring to feed CDB/PDB Parquet into the
calculator context is part of Phase 4. The three scale fixtures
(`scale-county`, `scale-project`, `scale-rates`) are excluded from the
standard regression suite.

#### Canonical-snapshot regression gate (dormant)

The canonical-snapshot diff gate in `full_suite_regression` is dormant:
activating it requires both real Rust port output (Phase 4) and canonical
MOVES snapshot captures. Known divergence categories are pre-documented
in [docs/known-divergences.md](docs/known-divergences.md) §4.

#### NONROAD numerical fidelity

The Rust port uses `f64` throughout; canonical NONROAD uses Fortran
`real*4`. This can produce results that are more accurate but differ
numerically from canonical captures. Per-variable tolerance budgets are
documented in `docs/known-divergences.md`.

#### Not supported (out of scope)

- MOVES GUI (Swing application)
- Distributed execution (`amazon/`, multi-host worker protocol)
- Uncertainty / Monte Carlo simulation
- Regulatory validation (this port is not for SIP submissions — see below)
- Disk-spill fallback for memory pressure

---

### Performance

Framework-overhead baseline measured on 2026-05-21, Intel Xeon Gold 6248,
`--max-parallel-chunks 1`:

| Fixture | Wall (ms) | Plan (ms) | Exec (ms) | Peak RSS (MiB) |
|---------|-----------|-----------|-----------|----------------|
| sample-runspec | 4.7 | 0.9 | 0.2 | 11.6 |
| process-airtoxics | 2.8 | 0.7 | 0.2 | 11.6 |
| nr-commercial-nation | ~3 | ~0.7 | ~0.2 | 11.6 |

These numbers represent **pure framework overhead** — RunSpec parsing,
calculator-graph planning, and output-file creation with no data rows.
The emission-calculation pass (Phase 4) is where the 10–50× wall-time
improvement over canonical MOVES will be realised.

The projected improvement comes from eliminating canonical MOVES's two
primary bottlenecks:

- **MariaDB/MyISAM I/O** (30–50% of canonical wall time) → replaced by
  in-memory Polars DataFrames over mmapped Parquet
- **Filesystem-mediated bundle handoff** (~20% of canonical wall time) →
  replaced by an in-process `rayon` thread pool

See [docs/benchmark-report.md](docs/benchmark-report.md) for full
methodology, projected memory model, and instructions for reproducing
the numbers.

---

### Regulatory caveat

> **This port is not approved for regulatory use.** Do not use outputs
> for SIP submissions, transportation conformity determinations, NEPA
> analyses, NAAQS-related filings, or any other regulatory purpose.

The port is for research and policy analysis. Regulatory validity requires
formal EPA approval and a passing EPA validation suite. Researchers
requiring regulatory-grade output must continue using the official
[MOVES Java application](https://www.epa.gov/moves).

---

### Contribution guidelines

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full guide. The short version:

- Each calculator maps to a task in `moves-rust-migration-plan.md` and a
  crate in `crates/moves-calculators/`
- Every behavioral change must be detected by a characterization test before
  merging
- Add `[from]` error conversions only for errors you actually surface
- Commit types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`
- File real bugs as beads; don't fix unrelated issues in a PR

---

### Browser demo

A minimal demo running the full onroad + NONROAD simulation in the browser
is included at `crates/moves-wasm/demo/`. To run it locally:

```bash
# 1. Install wasm-pack
cargo install wasm-pack

# 2. Build the WASM package
wasm-pack build --target web crates/moves-wasm

# 3. Serve
python3 -m http.server 8080 --directory crates/moves-wasm
# Open http://localhost:8080/demo/
```

See [crates/moves-wasm/demo/README.md](crates/moves-wasm/demo/README.md)
and [docs/wasm-embedding.md](docs/wasm-embedding.md).

A hosted demo URL will be added in a future release once CI publishes the
WASM artifacts to GitHub Pages.

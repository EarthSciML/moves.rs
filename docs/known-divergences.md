# Known Divergences — Phase 7 Regression Baseline (Task 126, `mo-uj3ke`)

This document is the "known divergences" record required by Phase 7 Task 126.
It describes the regression methodology for the full-suite pass and catalogues
the current state of the port against the 34-fixture characterization suite.

---

## 1. Methodology

### The fixture suite

`characterization/fixtures/` holds 37 RunSpec XML files. The full-suite pass
covers 34 of them — the 3 `scale-*` fixtures (County-Scale, Project-Scale,
Rates) require additional input databases not present in the default test
environment and are excluded.

| Set | Count | Pattern |
|-----|-------|---------|
| Onroad (default-scale) | 23 | `chain-*`, `expand-*`, `process-*`, `sample-runspec` |
| Mixed onroad + NONROAD | 1 | `mixed-onroad-nonroad` |
| NONROAD | 10 | `nr-*` |
| Excluded (need extra input DB) | 3 | `scale-county`, `scale-project`, `scale-rates` |

### The regression gate

The regression test lives in `crates/moves-cli/tests/full_suite_regression.rs`.
It has two layers:

**Always active** — runs on every `cargo test`:
- Every fixture calls `moves run` and must return `Ok`.
- `MOVESRun.parquet` must be produced.
- At least one calculator-graph module must be planned.

**Canonical-diff gate** — activated by `REGRESSION_SNAPSHOTS_DIR=<path>`:
- Loads Phase 0 canonical-MOVES snapshots from the supplied path.
- Diffs each fixture's port output against the canonical snapshot using
  `moves_snapshot::diff_snapshots`.
- Applies the per-(table, column) tolerance budget from
  `characterization/tolerance.toml`.
- Differences within tolerance are reported but not failing.
- Differences beyond tolerance fail the test.

The gate is currently **dormant** — see §3 for what is needed to activate it.

### Tolerance budget

`characterization/tolerance.toml` governs what counts as a "within tolerance"
divergence. The Phase 0 default is `default_float_tolerance = 0.0` (strict
byte-identity). As divergences are triaged in the canonical-diff phase, the
budget file grows per-column overrides with explanatory comments.

---

## 2. Phase 7 baseline — all fixtures run without error

Recorded on 2026-05-21 against the `polecat/mo-uj3ke` branch (Phase 7 entry).
All 34 fixtures complete without error. All plan > 0 modules. All execute 0
modules (expected — see §3).

```
fixture                                     planned executed   unimpl
------------------------------------------------------------------------
chain-nonhaptog                                  43        0       43
chain-tog-speciation                             43        0       43
expand-counties                                  44        0       44
expand-criteria                                  44        0       44
expand-day                                       44        0       44
expand-fueltype-diesel                           44        0       44
expand-month                                     44        0       44
expand-sourcetype                                44        0       44
mixed-onroad-nonroad                             44        0       44
nr-agriculture-state                             18        0       18
nr-airport-support-county                        18        0       18
nr-commercial-nation                             18        0       18
nr-construction-state                            18        0       18
nr-industrial-county                             18        0       18
nr-lawn-garden-county                            18        0       18
nr-logging-county                                18        0       18
nr-pleasure-craft-state                          21        0       21
nr-railroad-support-nation                       18        0       18
nr-recreational-county                           18        0       18
process-airtoxics                                43        0       43
process-apu                                      40        0       40
process-brakewear                                39        0       39
process-crankcase-extidle                        35        0       35
process-crankcase-running                        38        0       38
process-crankcase-start                          35        0       35
process-evap-fvv                                 40        0       40
process-evap-leaks                               40        0       40
process-evap-permeation                          39        0       39
process-extended-idle                            40        0       40
process-nox-speciation                           40        0       40
process-pm-exhaust                               43        0       43
process-refueling                                44        0       44
process-tirewear                                 40        0       40
sample-runspec                                   44        0       44
------------------------------------------------------------------------
34 fixtures
```

**What "0 executed" means:** The calculator `execute()` methods return
`CalculatorOutput::empty()` because the `CalculatorContext` does not yet carry
real row data — that is Phase 4's `DataFrameStore` deliverable. The numerical
implementations are complete (all Phase 3 calculator unit tests pass), but the
per-fixture materialisation path is not yet wired. This is the expected Phase 7
entry state; wiring the data plane is what turns "0 executed" into real
emission outputs.

---

## 3. Activating the canonical-diff gate

The canonical-diff gate requires two inputs that do not yet exist:

### Input 1: Canonical MOVES snapshots

The Phase 0 snapshot captures require running canonical MOVES in an Apptainer
SIF on an HPC node with root-capable namespacing:

```sh
# Build the SIF (one-time, ~1–2 hours):
characterization/apptainer/build-sif.sh
characterization/apptainer/build-fixture-sif.sh

# Run all fixtures and capture snapshots (~30–60 minutes total):
characterization/run-all-fixtures.sh --fakeroot --keep-going
```

This populates `characterization/snapshots/<fixture-name>/` for each fixture.
The snapshot format (`manifest.json` + `tables/*.parquet`) is defined in
`crates/moves-snapshot`.

### Input 2: Real calculator output from the Rust port

The port currently writes only `MOVESRun.parquet` (run metadata). Real
emission output tables appear once:
1. The Phase 4 `DataFrameStore` is wired into `CalculatorContext`.
2. The `OutputProcessor` writes output tables in `moves-snapshot` format
   alongside `MOVESRun.parquet`.

When both inputs exist, enable the diff gate:

```sh
REGRESSION_SNAPSHOTS_DIR=characterization/snapshots \
    cargo test --test full_suite_regression -- --nocapture
```

---

## 4. Expected divergence categories

Based on the migration plan and the Phase 3 calculator-validation harness
experience, divergences are expected to fall into four categories:

### 4.1 Within tolerance: ordering differences in tied-row aggregates

MOVES output tables accumulate rows across calculator threads in
non-deterministic order. When two rows tie on all natural-key columns, their
order in the canonical snapshot and the Rust port output may differ. The
`moves-snapshot` format normalises rows by natural key before writing, so
this category of divergence should be zero after normalisation — but if any
output table lacks a natural key, ordering differences will appear as
`rows_added` + `rows_removed` pairs in the diff.

**Resolution:** add natural-key declarations to any output table that lacks
them in the `OutputProcessor`.

### 4.2 Within tolerance: sub-tolerance numerical drift

Float summation order differs between the Java/Go original and the Rust port.
For most calculators the difference is sub-1e-9, within the default tolerance
budget. The calculator-validation harness (Task 73/74) documented no
divergences beyond 1e-9 for the 26 onroad fixtures it covers.

NONROAD arithmetic uses Fortran single-precision (`real*4`) in the original;
the Rust port uses `f64` throughout. This can produce results that are more
accurate but differ numerically from the canonical captures. The
`nonroad-fidelity` gate (Task 115) characterised per-variable tolerance
budgets for the intermediate NONROAD quantities; those budgets carry over to
the end-to-end output tables.

**Resolution:** widen per-column tolerances in `characterization/tolerance.toml`
for columns where the Rust port's higher-precision arithmetic produces
documented, acceptable differences.

### 4.3 Within tolerance: log-message and metadata format differences

`MOVESRun.parquet` carries a `description` column that may include
Java-class-name references (`gov.epa.otaq.moves.…`) absent from the Rust
port. These are in metadata columns, not emission quantities, and are accepted
as a known structural difference.

**Resolution:** exclude these columns from the diff or accept them via
`characterization/tolerance.toml` string-match exclusions once that feature
is implemented.

### 4.4 Beyond tolerance: real bugs

A small number of divergences may represent genuine port errors: incorrect
sign, wrong factor, missed edge case. These are identified by being large
(>> 1e-9), reproducible, and present in specific (pollutant, process) cells
that the corresponding unit test did not cover.

**Resolution:** fix the bug in the calculator, update the unit test to cover
the case, and verify the divergence disappears.

---

## 5. Regression workflow once the gate is active

```sh
# 1. Run the full suite with snapshots.
REGRESSION_SNAPSHOTS_DIR=characterization/snapshots \
    cargo test --test full_suite_regression -- --nocapture 2>&1 | tee /tmp/regression.log

# 2. Inspect divergences for a specific fixture.
target/release/moves-snapshot diff \
    characterization/snapshots/process-airtoxics/ \
    /tmp/port-output/process-airtoxics/ \
    --tolerance characterization/tolerance.toml \
    --format json | jq '.diff.table_changes[] | {table, cells: (.row_diffs | length)}'

# 3. Accept a characterised artifact — edit characterization/tolerance.toml:
#    [tables."db__movesoutput__movesoutput"]
#    emissionQuant = 1e-7   # artifact: Fortran real*4 vs Rust f64 for nr-* fixtures

# 4. Re-run gate to confirm the divergence is now within budget.
REGRESSION_SNAPSHOTS_DIR=characterization/snapshots \
    cargo test --test full_suite_regression canonical_snapshot_diff -- --nocapture
```

---

## 6. Scale fixtures (deferred)

The three excluded `scale-*` fixtures require additional input databases:

| Fixture | Requires |
|---------|----------|
| `scale-county` | County Database (CDB) Parquet inputs |
| `scale-project` | Project Database (PDB) Parquet inputs |
| `scale-rates` | Rates-mode setup database |

These fixtures will be added to the regression suite after the CDB/PDB
importers (Phase 4 Tasks 83–84) and a matching test fixture set are in place.
The `run-all-fixtures.sh` script has the same exclusion: pass
`--include scale-county` to opt in once the inputs are available.

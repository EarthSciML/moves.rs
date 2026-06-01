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

**Canonical-diff gate** — `canonical_snapshot_diff`, **active** (the in-repo
`characterization/snapshots/` tree is populated for all 34 non-scale fixtures;
override the tree with `REGRESSION_SNAPSHOTS_DIR=<path>`):
- Runs each fixture with `--snapshot`, so the calculators execute against the
  captured execution DB and the engine writes the real `MOVESOutput/` tree
  (not just `MOVESRun.parquet`).
- Sums `emissionQuant` per `pollutantID` from both the canonical `MOVESOutput`
  table and the port's `MOVESOutput/` tree, then compares the per-pollutant
  totals (`moves_snapshot::compare_pollutant_sums`).
- **Hard-asserts** on the fixtures whose data plane matches canonical within a
  documented precision-only tolerance (§4.2 below).
- **Hard-fails** (operator decision) on fixtures with a known, reported
  data-plane bug (§4.4 below) — it is OK for CI to be red while results are
  wrong. Masking a divergence with a widened tolerance is worse than no gate,
  so a quarantined fixture stays in the gate (failing CI) and graduates to the
  asserted set only once its data plane is actually fixed.

### Why per-pollutant sums, not a cell-level diff

A byte/cell-level `moves_snapshot::diff_snapshots` of `MOVESOutput` is unusable
here: even when the port reproduces canonical to `f64` precision, the two tables
disagree on metadata/labeling columns that do **not** affect emitted mass —
`iterationID` (port NULL vs canonical 1), `roadTypeID` (port 0 vs the link road
type), and the `SCC` road-type subfield (which therefore differs, e.g.
`2201210412` vs `2201210012`) — and canonical carries
`emissionQuantMean`/`emissionQuantSigma` (always NULL with uncertainty off)
where the port carries `emissionRate`/`runHash`. A cell diff fails on those for
*every* fixture. The per-pollutant `emissionQuant` total is the quantity that
must agree and cleanly isolates real divergences in emitted mass; it is the same
metric `characterization/audit/regression_gate.sh` uses.

### Tolerance budget

The per-pollutant relative tolerances live in the gate
(`crates/moves-cli/tests/full_suite_regression.rs`): `ONROAD_REL_TOL = 1e-3`
and `NONROAD_REL_TOL = 1e-2`, justified in §4.2. (`characterization/tolerance.toml`
remains the per-(table, column) budget for `moves_snapshot diff` of the full
snapshot, unchanged at `default_float_tolerance = 0.0`.)

---

## 1b. Canonical-diff gate state (2026-05-31)

The gate was activated against the re-captured snapshots (commit `a1d4314`,
onroad Go calculators). Of the 34 non-scale fixtures, **8 are asserted** and
**26 are known data-plane bugs that hard-fail CI** (operator decision: it is OK
for CI to be red while results are wrong). The gate pins the 8 working fixtures
against regression and keeps the 26 bugs failing on the record (never masked by
a tolerance); each graduates to the asserted set once its data plane is fixed.

### Triage table

| Fixture(s) | canon→port rows | max rel. diff | Verdict |
|---|---|---|---|
| `process-evap-fvv` | 128→128 | 8.2e-5 | **precision-only — asserted** (`ONROAD_REL_TOL`) |
| `process-evap-leaks` | 128→128 | 1.6e-7 | **precision-only — asserted** |
| `process-evap-permeation` | 128→128 | 2.1e-7 | **precision-only — asserted** |
| `nr-commercial-nation` | 908→908 | 3.5e-3 | **precision-only (real\*4) — asserted** (`NONROAD_REL_TOL`) |
| `process-apu`, `process-crankcase-extidle`, `process-crankcase-start`, `process-extended-idle` | 0→0 | — | **vacuous — asserted** (canonical has no `MOVESOutput`; port emits none either) |
| `chain-nonhaptog`, `chain-tog-speciation`, `expand-counties`, `expand-criteria`, `expand-day`, `expand-fueltype-diesel`, `expand-month`, `expand-sourcetype`, `mixed-onroad-nonroad`, `process-airtoxics`, `process-brakewear`, `process-crankcase-running`, `process-nox-speciation`, `process-pm-exhaust`, `process-refueling`, `process-tirewear`, `sample-runspec` | N→0 (was →8,632) | −100 % (was ~1e40 %) | **NONROAD-garbage root-cause FIXED; residual onroad-emits-0 gap — quarantined** (see §4.4 reported bug 1) |
| `nr-agriculture-state`, `nr-airport-support-county`, `nr-industrial-county`, `nr-railroad-support-nation` | N→0 | −100 % | **reported bug (NONROAD emits nothing) — quarantined** |
| `nr-construction-state`, `nr-lawn-garden-county`, `nr-logging-county`, `nr-pleasure-craft-state`, `nr-recreational-county` | mismatched | 1e3–1e6 % | **reported bug (NONROAD population/coverage) — quarantined** |

No tolerance was widened to absorb a bug. The only tolerances applied
(`ONROAD_REL_TOL = 1e-3`, `NONROAD_REL_TOL = 1e-2`) cover the four genuinely
matching fixtures, whose divergences are sub-tolerance float-accumulation /
`real*4` artifacts (§4.2).

### Reported bug 1 — onroad-exhaust path emitted fixed NONROAD-coded garbage — ROOT-CAUSE FIXED

**Original symptom.** Every onroad fixture, run against its own snapshot, wrote
a **byte-identical** ~8,632-row `MOVESOutput` block regardless of the RunSpec
(verified: the part files for `expand-criteria`, `chain-nonhaptog`, and
`expand-fueltype-diesel` were identical, `Σ emissionQuant = 43520901035.30757`).
The rows carried NONROAD SCC codes (`2260…/2265…/2282…/2285…`) with
`sourceTypeID`/`fuelTypeID`/`sectorID` all NULL, and emitted ~7 orders of
magnitude more mass than canonical.

**Root cause (found 2026-05-31).** It was never the onroad calculators emitting
garbage — they emit nothing (see the residual gap below). The whole block came
from `NonroadEmissionCalculator` firing on **onroad-only** RunSpecs. The MOVES
NONROAD emission processes (1, 15, 18–21, 30–32) share the process-ID namespace
with onroad, and `CalculatorRegistry::modules_for_runspec` selected modules
purely by `(pollutant, process)` with **no model filter**. So for any onroad run
that selects process 1 (Running Exhaust) — i.e. every onroad fixture — the
planner pulled in `NonroadEmissionCalculator` (plus its NONROAD-only downstream
`NRHCSpeciationCalculator`/`NRAirToxicsCalculator`). Its `execute` then ran a
full NONROAD simulation against the `nr*` execution-DB tables, which are
default-DB content captured **identically** in every snapshot — hence the
byte-identical, RunSpec-independent block (only `runHash` differed). Canonical
MOVES gates this chain on the model selection (`Models.evaluateModels`); the
Rust planner had dropped that dimension.

**Fix.** `CalculatorRegistry::execution_order_for_models` (new) drops the
NONROAD-only module set — computed from the DAG: the `.../master/nonroad/`
package module plus its transitive `chained_downstream` closure — when the
RunSpec does not select the NONROAD model. `MOVESEngine::planned_modules` now
calls it with the run's model flags. After the fix, all 17 onroad fixtures emit
**0** NONROAD-coded rows (the garbage is gone); `nr-commercial-nation` and the
mixed/NONROAD fixtures are unaffected (NONROAD still selected → calculator still
runs).

**Residual gap (separate bug, still quarantined).** With the garbage removed,
the onroad fixtures now diverge the *other* way: the onroad emission data plane
(`BaseRateGenerator` → `BaseRateCalculator` → criteria/PM/etc.) executes but
emits **0** `MOVESOutput` rows against the captured execution DB, where canonical
has the real onroad SCC `2201…` rows (e.g. `expand-criteria` 744, `sample-runspec`
84, `process-pm-exhaust` 1,456). That is a distinct onroad data-plane gap — not
the NONROAD-garbage bug — so these fixtures stay in `QUARANTINED_FIXTURES`
(−100 % vs a fixed catastrophic ~1e40 %) until the onroad output wiring lands.
`mixed-onroad-nonroad` also stays quarantined: its captured canonical
`MOVESOutput` is empty (0 rows) while the port's NONROAD half legitimately emits
~8,632 rows.

### Reported bug 2 — several NONROAD fixtures emit nothing or a wrong row count

`nr-agriculture-state`, `nr-airport-support-county`, `nr-industrial-county`,
and `nr-railroad-support-nation` produce **0** `MOVESOutput` rows against a
populated canonical (−100 %). `nr-construction-state`, `nr-lawn-garden-county`,
`nr-logging-county`, `nr-pleasure-craft-state`, and `nr-recreational-county`
produce the wrong row count and diverge by 10³–10⁶ %. These are NONROAD
population / sector-coverage gaps in the data plane, to be fixed there. Only
`nr-commercial-nation` reproduces canonical (all four pollutants within 0.35 %).

As each fixture's data plane is fixed it should graduate from
`QUARANTINED_FIXTURES` into `asserted_fixtures` in the gate.

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

## 3. How the canonical-diff gate was activated

The gate is now active (§1b). It required two inputs, both of which now exist:

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

Running with `--snapshot` wires the captured execution DB into the calculators,
so the engine writes the real `MOVESOutput/` partitioned Parquet tree (Hive
layout) alongside `MOVESRun.parquet`. The gate reads that tree directly (it does
**not** require the port to write `moves-snapshot` format), summing
`emissionQuant` per `pollutantID`.

Run the gate:

```sh
cargo test --test full_suite_regression canonical_snapshot_diff -- --nocapture
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

**Resolution:** the gate's per-pollutant relative tolerances absorb this drift
on the matching fixtures: `ONROAD_REL_TOL = 1e-3` (the three onroad evap
fixtures land at 1.6e-7 … 8.2e-5) and `NONROAD_REL_TOL = 1e-2`
(`nr-commercial-nation`'s `real*4`-vs-`f64` totals land at ≤ 3.5e-3 across all
four pollutants). These are the only tolerances applied; they cover precision
artifacts only, never a structural/wiring divergence (§4.4).

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

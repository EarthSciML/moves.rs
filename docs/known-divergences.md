# Known Divergences — Regression Baseline

This document is the "known divergences" record for the regression methodology.
It describes the regression methodology for the full-suite pass and catalogues
the current state of the port against the characterization suite. As of
2026-06-15 the canonical-diff gate is **green**: every non-scale fixture is
asserted against canonical MOVES and **zero** are quarantined.

---

## 1. Methodology

### The fixture suite

`characterization/fixtures/` holds 51 RunSpec XML files. The canonical-diff gate
asserts the **39** non-scale fixtures that have a populated snapshot directory
(one per fixture under `characterization/snapshots/` carrying a `manifest.json`);
the 3 `scale-*` fixtures (County-Scale, Project-Scale, Rates) require additional
input databases not present in the default test environment and are excluded.

| Set | Pattern |
|-----|---------|
| Onroad (default-scale) | `chain-*`, `expand-*`, `process-*`, `sample-runspec`, `mixed-onroad` |
| NONROAD | `nr-*` (including `nr-mixed-nonroad`) |
| Excluded from `all_fixtures()` (`scale-*`) | `scale-county`, `scale-project`, `scale-rates` — see §6 |

### The regression gate

The regression test lives in `crates/moves-cli/tests/full_suite_regression.rs`.
It has two layers:

**Always active** — runs on every `cargo test`:
- Every fixture calls `moves run` and must return `Ok`.
- `MOVESRun.parquet` must be produced.
- At least one calculator-graph module must be planned.

**Canonical-diff gate** — `canonical_snapshot_diff`, **active** (the in-repo
`characterization/snapshots/` tree is populated for all 39 asserted non-scale
fixtures; override the tree with `REGRESSION_SNAPSHOTS_DIR=<path>`):
- Runs each fixture with `--snapshot`, so the calculators execute against the
 captured execution DB and the engine writes the real `MOVESOutput/` tree
 (not just `MOVESRun.parquet`).
- Sums `emissionQuant` per `pollutantID` from both the canonical `MOVESOutput`
 table and the port's `MOVESOutput/` tree, then compares the per-pollutant
 totals (`moves_snapshot::compare_pollutant_sums`).
- **Hard-asserts** on the fixtures whose data plane matches canonical within a
 documented precision-only tolerance (§4.2 below).
- **Hard-fails** (operator decision) on any fixture with a known, reported
 data-plane bug — it is OK for CI to be red while results are wrong. Masking a
 divergence with a widened tolerance is worse than no gate, so a quarantined
 fixture stays in the gate (failing CI) and graduates to the asserted set only
 once its data plane is actually fixed. As of 2026-06-15 the quarantine list
 (`QUARANTINED_FIXTURES`) is **empty** — every non-scale fixture has graduated
 (see §1b).

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

## 1b. Canonical-diff gate state (current — 2026-06-15)

The gate is **green**: all **39** non-scale fixtures with a populated snapshot
directory are **asserted** against canonical MOVES within the documented
precision-only tolerances (§4.2), and `QUARANTINED_FIXTURES` is **empty** — there
are no fixtures failing CI on a known data-plane bug. No tolerance was ever
widened to absorb a bug; the only tolerances applied (`ONROAD_REL_TOL = 1e-3`,
`NONROAD_REL_TOL = 1e-2`) cover sub-tolerance float-accumulation / `real*4`
artifacts (§4.2). The authoritative list of asserted fixtures (with each one's
per-pollutant residual) lives in `asserted_fixtures()` in
`crates/moves-cli/tests/full_suite_regression.rs`; that file's per-fixture
comments are the canonical, kept-current record of how each one was graduated.

A handful of the start/idle/hotelling fixtures (`process-apu`,
`process-crankcase-extidle`/`-start`, `process-extended-idle`, and their
`-single` variants) are asserted **vacuous**: canonical's captured execution DB
holds the base rate but its activity and output tables are empty, so canonical's
authoritative output for the process is zero rows, and the port — gated by the
same activity weighting — reproduces that (canon 0 == port 0). The `vacuous`
flag makes the gate fail loudly if a recapture ever gives either side a nonzero
row.

### Resolved (historical)

Earlier revisions of this document (state dated 2026-05-31) catalogued **8
asserted / 26 quarantined** fixtures and three classes of "reported bug":

1. **Onroad over-emit / activity weighting** — a fixed NONROAD-coded garbage
   block (NONROAD calculators firing on onroad-only RunSpecs), a month
   off-by-one zeroing onroad output, an off-network start-row over-emit, and the
   missing inventory activity weighting (`universalActivity = SHO / noOfRealDays`)
   plus the kJ→Million-BTU energy-unit conversion, E85 fuel-effects and
   multi-county expansion residuals.
2. **NONROAD emit-nothing / wrong-row-count** — the empty-`/SOURCE CATEGORY/`
   quirk, surrogate allocation, state-scoped lookups, the SFC-vs-SWT sox fix, the
   `/PM BASE SULFUR/` alternates, the Tier-4-era diesel gap (MXTECH=15 truncation
   + the 7-digit SCC fallback step), per-HP-bin activity, and the `.POP`
   one-decimal population rounding.
3. **Onroad under-emit / chained-calculator coverage** — the regClass-collapse
   round-trip drop, the SulfatePM pass-through doubling, the NO/NO2 species
   doubling, and the PM-speciation / air-toxics / refueling chains not producing
   their full pollutant/process set.

**All of these have been fixed and the affected fixtures graduated to
`asserted_fixtures`.** None were resolved by widening a tolerance. The detailed
root-cause-and-fix narrative for each is recorded in the git history and in the
per-fixture comments of `full_suite_regression.rs`; it is no longer reproduced
here because none of it describes a current failure. (`mixed-onroad-nonroad` was
not "fixed" but **retired** — canonical MOVES 5.0.1 does not implement a combined
ONROAD+NONROAD run, the `M12` case in `ExecutionRunSpec.buildVehicleSelections`
being an unfinished stub — and split into the asserted `mixed-onroad` and
`nr-mixed-nonroad` halves.)

---

## 2. Initial baseline — all fixtures run without error

Recorded on 2026-05-21. All 34 fixtures complete without error. All plan > 0
modules. All execute 0 modules (expected — see §3).

```
fixture planned executed unimpl
------------------------------------------------------------------------
chain-nonhaptog 43 0 43
chain-tog-speciation 43 0 43
expand-counties 44 0 44
expand-criteria 44 0 44
expand-day 44 0 44
expand-fueltype-diesel 44 0 44
expand-month 44 0 44
expand-sourcetype 44 0 44
mixed-onroad-nonroad 44 0 44
nr-agriculture-state 18 0 18
nr-airport-support-county 18 0 18
nr-commercial-nation 18 0 18
nr-construction-state 18 0 18
nr-industrial-county 18 0 18
nr-lawn-garden-county 18 0 18
nr-logging-county 18 0 18
nr-pleasure-craft-state 21 0 21
nr-railroad-support-nation 18 0 18
nr-recreational-county 18 0 18
process-airtoxics 43 0 43
process-apu 40 0 40
process-brakewear 39 0 39
process-crankcase-extidle 35 0 35
process-crankcase-running 38 0 38
process-crankcase-start 35 0 35
process-evap-fvv 40 0 40
process-evap-leaks 40 0 40
process-evap-permeation 39 0 39
process-extended-idle 40 0 40
process-nox-speciation 40 0 40
process-pm-exhaust 43 0 43
process-refueling 44 0 44
process-tirewear 40 0 40
sample-runspec 44 0 44
------------------------------------------------------------------------
34 fixtures
```

**What "0 executed" means:** The calculator `execute()` methods return
`CalculatorOutput::empty()` because the `CalculatorContext` does not yet carry
real row data — that is the `DataFrameStore` deliverable. The numerical
implementations are complete (all calculator unit tests pass), but the
per-fixture materialisation path is not yet wired. This is the expected entry
state; wiring the data plane is what turns "0 executed" into real emission
outputs.

---

## 3. How the canonical-diff gate was activated

The gate is now active (§1b). It required two inputs, both of which now exist:

### Input 1: Canonical MOVES snapshots

The snapshot captures require running canonical MOVES in an Apptainer SIF on an
HPC node with root-capable namespacing:

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

Divergences are expected to fall into four categories:

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
budget. The calculator-validation harness documented no divergences beyond 1e-9
for the 26 onroad fixtures it covers.

NONROAD arithmetic uses Fortran single-precision (`real*4`) in the original;
the Rust port uses `f64` throughout. This can produce results that are more
accurate but differ numerically from the canonical captures. The
`nonroad-fidelity` gate characterised per-variable tolerance budgets for the
intermediate NONROAD quantities; those budgets carry over to
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
# [tables."db__movesoutput__movesoutput"]
# emissionQuant = 1e-7 # artifact: Fortran real*4 vs Rust f64 for nr-* fixtures

# 4. Re-run gate to confirm the divergence is now within budget.
REGRESSION_SNAPSHOTS_DIR=characterization/snapshots \
 cargo test --test full_suite_regression canonical_snapshot_diff -- --nocapture
```

---

## 6. Scale fixtures

`all_fixtures()` in `crates/moves-cli/tests/full_suite_regression.rs`
hard-excludes every `scale-*.xml` (the `!name.starts_with("scale-")` filter),
so no `scale-*` fixture reaches `all_fixtures_run_without_error`,
`canonical_snapshot_diff`, or the catalogue-count assertion. The purpose-built
home for them is the dormant `SCALE_INPUTS_DIR_ENV` gate, which wants
CDB/PDB Parquet inputs:

| Fixture | Canonical snapshot | Requires for the port |
|---------|--------------------|-----------------------|
| `scale-county` | not captured | County Database (CDB) Parquet inputs |
| `scale-project` | **captured 2026-09-10** | Project Database (PDB) Parquet inputs, or `--snapshot` |
| `scale-rates` | not captured | Rates-mode setup database |

`run-all-fixtures.sh` keeps `scale-county` and `scale-rates` in
`SKIP_BY_DEFAULT` for want of an input DB. `scale-project` stays there only
for the cost of the extra MariaDB seed pass: `--include scale-project` now
captures it, routed through `apptainer/capture-county-snapshot.sh` with
`characterization/county-inputs/washtenaw-project/setup-project.sql`.

### 6.1 `scale-project`: the port over-emits by ~50× (unfiled, measured 2026-09-10)

The snapshot carries the execution database, so the port does not need a PDB
importer to be measured against it — `--snapshot` supplies the slow tier.
Measured by temporarily lifting the `scale-` exclusion in `all_fixtures()`
and running `canonical_snapshot_diff`, and independently by running the CLI:

```sh
moves run --runspec characterization/fixtures/scale-project.xml \
          --snapshot characterization/snapshots/scale-project \
          --output /tmp/portout
```

| | canonical | port |
|---|---|---|
| `MOVESOutput` rows | 125 | 125 |
| pollutants emitted | 91 only | 91 only |
| process / roadType / link / day / hour / month | 1 / 4 / 1 / 5 / 9 / 8 | identical |
| Σ `emissionQuant` (Million BTU) | 4.227043523010997 | 214.257191021672 |

`max_rel_diff = +4.969e1`; the port's total is **50.69×** canonical's. Every
key column agrees and the row count agrees exactly, so this is a magnitude
error in the PROJECT-domain activity or rate path, not a shape or coverage
error — the shape agreeing on all six dimensions is what makes it worth
chasing.

This fixture is **not** wired into `canonical_snapshot_diff`. Doing so means
editing `all_fixtures()`, which also feeds `all_fixtures_run_without_error`
and the "exactly 48 non-scale non-error fixtures" catalogue assertion, so it
is a deliberate three-test change and an operator call. The numbers above are
recorded here so that call can be made on evidence. Once made, `scale-project`
belongs in `QUARANTINED_FIXTURES` until the magnitude error is fixed.

Note also that across two independent captures of this fixture on 2026-09-10
(same SIF, input DB differing only in its month filter) all 360 table Parquets
were byte-identical except `db__out_scale_project__movestablesused`;
`db__out_scale_project__movesoutput` in particular did not change a byte. Only
`manifest.json` and `provenance.json` otherwise differ. That is a stronger
result than the corpus generally holds — see
`characterization/snapshots/README.md` §"Measured limits of the determinism
contract" for the two known sources of run-to-run drift.

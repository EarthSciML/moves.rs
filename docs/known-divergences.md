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
covers the **42** non-scale fixtures that have a populated snapshot directory
(one per fixture under `characterization/snapshots/` carrying a `manifest.json`):
**41 asserted**, **1 quarantined** (§4.4). The 3 `scale-*` fixtures
(County-Scale, Project-Scale, Rates) require additional input databases not
present in the default test environment and are excluded.

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
`characterization/snapshots/` tree is populated for all 42 covered non-scale
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
 once its data plane is actually fixed. As of 2026-09-10 the quarantine list
 (`QUARANTINED_FIXTURES`) holds exactly one fixture,
 `nr-airtoxics-lawn-garden-county` (§4.4) — so `canonical_snapshot_diff` is
 **red, deliberately**.
- **Hard-fails as UNCLASSIFIED** on a fixture that is in neither list. This is
 the state a newly captured snapshot lands in: it is a *triage gap*, not a
 measurement. UNCLASSIFIED is never an acceptable resting state — classify the
 fixture into one list or the other, with the evidence written into its
 comment.

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

## 1b. Canonical-diff gate state (current — 2026-09-10)

The gate is **red, by design, on exactly one fixture**. Of the **42** non-scale
fixtures with a populated snapshot directory:

| | count | |
|---|---|---|
| asserted (pass within §4.2 precision tolerance) | **41** | `asserted_fixtures()` |
| quarantined (known data-plane bug, hard-fails CI) | **1** | `nr-airtoxics-lawn-garden-county`, §4.4 |
| unclassified | **0** | — |

`cargo test -p moves-cli --test full_suite_regression` therefore reports
**8 passed, 1 failed**, the failure being `canonical_snapshot_diff` on the one
quarantined fixture. Per the §1 policy that is the correct outcome: the port
does not compute 27 of that fixture's 29 pollutants, so there is nothing a
tolerance could legitimately absorb, and masking it would turn the gate into
decoration.

No tolerance was ever widened to absorb a bug; the only tolerances applied
(`ONROAD_REL_TOL = 1e-3`, `NONROAD_REL_TOL = 1e-2`) cover sub-tolerance
float-accumulation / `real*4` artifacts (§4.2). The authoritative list of
asserted fixtures (with each one's per-pollutant residual) lives in
`asserted_fixtures()` in `crates/moves-cli/tests/full_suite_regression.rs`;
that file's per-fixture comments are the canonical, kept-current record of how
each one was graduated.

### Triage of 2026-09-08/09-10 snapshot additions

Three snapshots were captured after the 2026-06-15 state above and were never
classified, so the gate printed them as UNCLASSIFIED (a triage gap, not a
measurement — see §1). All three were triaged on 2026-09-10:

| fixture | canon/port rows | max_rel_diff | verdict |
|---|---|---|---|
| `chain-so2-co2e-mechanism` | 2767 / 2767 | -3.408e-4 | **asserted** — precision only |
| `chain-so2-co2e-mechanism-control` | 2309 / 2309 | -3.408e-4 | **asserted** — precision only |
| `nr-airtoxics-lawn-garden-county` | 14036 / 968 | -1.000e0 | **quarantined** — §4.4 |

For the two `chain-so2-*` fixtures the pollutant key sets and the per-pollutant
row counts are identical on both sides, and every pollutant agrees to ≤ 5e-7 —
including the three trigger pollutants those fixtures exist to prove reachable:
SO2 (31) -3.8e-7, Atmospheric CO2 (90) +1.5e-7, CO2 Equivalent (98) +1.2e-7.
The whole of the reported -3.408e-4 is Total Energy Consumption (91), the same
energy summation-drift class already carried by `expand-day` (-3.5e-4),
`expand-month` (-3.8e-4) and `process-tirewear` (-3.4e-4).

A handful of the start/idle/hotelling fixtures (`process-apu`,
`process-crankcase-extidle`/`-start`, `process-extended-idle`, and their
`-single` variants) are asserted **vacuous**: canonical's captured execution DB
holds the base rate but its activity and output tables are empty, so canonical's
authoritative output for the process is zero rows, and the port — gated by the
same activity weighting — reproduces that (canon 0 == port 0). The `vacuous`
flag makes the gate fail loudly if a recapture ever gives either side a nonzero
row.

### 1b.1 `process-crankcase-start-single` recaptured 2026-09-10 (input-data fix)

`characterization/county-inputs/washtenaw-county/setup-starts.sql` carried
`zoneRoadType(261610, roadTypeID 5).SHOAllocFactor = 0.002012975189` from
30a3a528 (2026-06-03) until 2026-09-10. The `movesdb20241112` default is
`0.0010453931260005`; the committed value was **1.93x** that and occurs
nowhere in the default table (`ABS(SHOAllocFactor - 0.002012975189) < 1e-12`
returns no rows), so it was not a mis-copy from another zone. The sibling
`setup-hotelling.sql` had the correct default for the same key and road
types 2/3/4 agreed in both files. 30a3a528's message records that the work
was "Salvaged from polecat furiosa (LLM context-gated at 1M before final
verification)". Filed as EarthSciML/moves.rs#66, fixed and recaptured.

Corrected to **`0.001045393126`** — the default truncated to 12 decimal
places, which is the convention every other literal in that file follows
(see the file's own header on why the literals are not `SELECT`ed from the
default DB). Writing the full 17-digit default would have introduced a new
divergence rather than removed one: the truncation reaches the execution
database, where SINGLE fixtures read `1.122533244e-03` for
`zone.startAllocFactor` against `1.12253324381788e-03` in DEFAULT-scale
fixtures.

**Blast radius: exactly one snapshot.** All 43 `…__zoneroadtype.parquet`
files in `characterization/snapshots/` were scanned; 31 contain zone
261610, and `process-crankcase-start-single` was the only one reading
`2.012975189e-03`. The other 30 (including `mixed-onroad`,
`process-extended-idle-single` and the three other `-single` fixtures) read
the default.

**Nothing propagated downstream, and that is the finding.** The recapture
(same SIF `4f92c593`, same RunSpec bytes) changed **2 of 316** tables:

| table | change |
|---|---|
| `db__movesexecution…__zoneroadtype` | the corrected input itself, 1 cell |
| `db__out_…__movestablesused` | `dataFileModificationDate` on the 8 `washtenaw_cdb` rows — wall-clock metadata |

The other 314 are byte-identical, including every output table.
`SHOAllocFactor` is consumed by `TotalActivityGenerator`'s SHO allocation,
and this fixture generates **0 bundles** with `sho`, `sourcehours`,
`starts`, `startspervehicle`, `movesworkeractivityoutput`,
`baserateoutput`, `movesactivityoutput` and `movesoutput` all at 0 rows —
so the value is copied into the execution database and read by nothing.
A 1.93x error on it was invisible to every output-side gate, which is how
it survived three months. The fixture remains asserted-**vacuous**; no
test changed.

Snapshot aggregate `a3af712b` -> `dc60a36e`.

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

A small number of divergences represent genuine port errors: incorrect sign,
wrong factor, missed edge case, or a calculator that never runs at all. These
are identified by being large (>> 1e-9), reproducible, and present in specific
(pollutant, process) cells that the corresponding unit test did not cover.

**Resolution:** fix the bug in the calculator, update the unit test to cover
the case, and verify the divergence disappears. Never a widened tolerance.

#### 4.4.1 `nr-airtoxics-lawn-garden-county` — the NONROAD air-toxics chain emits nothing (open, measured 2026-09-10)

The one fixture currently in `QUARANTINED_FIXTURES`, and the reason
`canonical_snapshot_diff` is red.

**Symptom.** The port emits **968 of canonical's 14036 rows** — 2 of 29
pollutants, at 484 rows each:

| | pollutants emitted | rows |
|---|---|---|
| canonical | 1, 5, 20, 21, 23, 24, 25, 26, 27, 45, 46, 60, 63, 65, 66, 67, 69, 79, 80, 86, 87, 88, 99, 100, 110, 131, 142, 169, 185 | 14036 |
| port | 1, 100 | 968 |

The two it does emit are right: THC (1) -8.6e-7, Primary Exhaust PM10 (100)
-1.2e-6. Everything `NRHCSpeciationCalculator` and `NRAirToxicsCalculator`
should contribute is absent, and so are the two `NonroadEmissionCalculator`
outputs those calculators consume — total fuel consumption (99) and PM2.5
(110). A pollutant that is missing entirely scores a relative difference of
-1.0, which is the `max_rel_diff = -1.000e0` the gate prints.

**Layer 1 — planner gap (isolated and confirmed).**
`CalculatorRegistry::rates_first_excluded_calculators`
(`crates/moves-framework/src/calculator/registry.rs`) mirrors canonical's
`MOVESInstantiator` `DO_RATES_FIRST` behaviour by dropping every selected
*calculator* that is not on a hard-coded `KEEP` whitelist. That whitelist
carries `NonroadEmissionCalculator` but **not** `NRHCSpeciationCalculator` or
`NRAirToxicsCalculator`, so neither is ever planned — confirmed by dumping
`EngineOutcome::modules_planned` for this fixture, which lists neither name,
and by adding both to `KEEP`, after which both appear in `modules_planned` and
`modules_executed`.

Canonical does instantiate both for this RunSpec. The captured
`characterization/snapshots/nr-airtoxics-lawn-garden-county/execution-trace.json`
lists `gov.epa.otaq.moves.master.implementation.ghg.NRAirToxicsCalculator` and
`...NRHCSpeciationCalculator` (plus their `$ATRatioEntry` / `$HCEntry` inner
classes) among the java classes MOVES loaded. Note the package: both live under
`implementation.ghg`, not `master.nonroad`, so they are whitelist-governed the
same way the onroad chained calculators are — and the onroad members of that
chain (`HCSpeciationCalculator`, `AirToxicsCalculator`, `TOGSpeciationCalculator`)
*are* on `KEEP`. The omission of the two NR names looks like an oversight in
that list rather than a modelled exclusion.

**Layer 2 — the calculators emit nothing anyway (data plane).** Adding both
names to `KEEP` is *not* sufficient: with both planned and executed, output is
still 968 rows and the same 2 pollutants. So there is a second, independent
defect in the NONROAD chain — `NonroadEmissionCalculator` withholds pollutants
99 and 110 for this RunSpec (it produces them for no fixture in the suite), and
with no VOC (87) / PM2.5 (110) / fuel (99) inputs the two downstream NR
calculators have nothing to scale. This is the fixture's whole purpose: it is
the only fixture in the corpus that reaches `NRHCSpeciationCalculator` and
`NRAirToxicsCalculator` at all (see `characterization/fixtures/coverage-matrix.md`),
so nothing else in the suite guards them.

**Explicitly not a tolerance question.** The port is not computing 27 of the 29
pollutants, so no tolerance, `excluded_pollutants` scope exception, or storage
quantum allowance can honestly clear this. Under the §1 policy the fixture stays
in the gate and CI stays red until the data plane is fixed. When it is fixed,
move the entry from `QUARANTINED_FIXTURES` to `asserted_fixtures()` at
`NONROAD_REL_TOL` and delete this subsection.

**Reproduce:**

```sh
cargo run -p moves-cli --bin moves -- run \
  --runspec characterization/fixtures/nr-airtoxics-lawn-garden-county.xml \
  --snapshot characterization/snapshots/nr-airtoxics-lawn-garden-county \
  --output /tmp/nrat --max-parallel-chunks 1
# port: 968 rows, pollutants {1, 100}
# canonical: characterization/snapshots/nr-airtoxics-lawn-garden-county/tables/
#            db__out_nr_airtoxics_lawn_garden_county__movesoutput.parquet
#            14036 rows, 29 pollutants
```

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
| `scale-project` | **captured 2026-09-10** | Project Database (PDB) Parquet inputs, or `--snapshot`. Matches canonical since 2026-09-10 (§6.1) |
| `scale-rates` | not captured | Rates-mode setup database |

`run-all-fixtures.sh` keeps `scale-county` and `scale-rates` in
`SKIP_BY_DEFAULT` for want of an input DB. `scale-project` stays there only
for the cost of the extra MariaDB seed pass: `--include scale-project` now
captures it, routed through `apptainer/capture-county-snapshot.sh` with
`characterization/county-inputs/washtenaw-project/setup-project.sql`.

### 6.1 `scale-project`: was ~50x high, now matches (measured and fixed 2026-09-10)

The snapshot carries the execution database, so the port does not need a PDB
importer to be measured against it — `--snapshot` supplies the slow tier:

```sh
moves run --runspec characterization/fixtures/scale-project.xml \
          --snapshot characterization/snapshots/scale-project \
          --output /tmp/portout
```

| Sigma `emissionQuant` (Million BTU) | value |
|---|---|
| canonical | 4.227043523011 |
| port, before | 214.257191021672 — **50.687x** canonical |
| port, after | 4.2270374987767 — ratio **0.9999986** |

Row count agreed exactly (125/125) before and after, and so did every key
column: pollutant 91 only, process 1, one link, one hour, one day.

Three defects, all in the port, none a canonical quirk.

#### 1. The PROJECT domain ran two total-activity generators

The "~50x" was never a scalar. Joined per row the 125 ratios ran from
**7.095x to 73.594x**, monotonically increasing with model year; dividing by
`SourceTypeAge.relativeMAR` flattened them to **77.1678 +/- 0.0016** over ages
9-40 (0.002% spread; ages 0-8 drift down to 77.093, about 0.1%). That reads
like two independent errors — "one spurious `relativeMAR`" plus "one constant
of 77.167". It is one.

`MOVESInstantiator.instantiate`'s `M1` block *replaces* `TotalActivityGenerator`
(and `MesoscaleLookupTotalActivityGenerator`) with `ProjectTAG` when
`isProjectDomain`. The port had ported the sibling swap for the three
`OpModeDistribution` generators
(`CalculatorRegistry::domain_scale_excluded_omd_modules`) but not this one, so
the `(pollutant, process)` module filter selected both — every total-activity
producer subscribes to the same processes. They collide on the single `SHO`
scratch table: `ProjectTAG` *appends* its link-volume rows, then
`TotalActivityGenerator` *overwrites* the whole table with
`store.insert("SHO", ...)`.

The surviving activity is therefore the county HPMS/VMT allocation weighted by
`TravelFraction`, which is `ageFraction x relativeMAR / sum(ageFraction x
relativeMAR)`, where the PROJECT `SHO` is `linkVolume x sourceTypeHourFraction
x noOfRealDays x ageFraction x min(length/speed, 1)` — `ageFraction` alone.
Their ratio is exactly `relativeMAR x (a constant activity ratio)`. Both
halves of the divergence fall out of the one missing swap; there was no stray
scalar in an expression to find, and 77.1678 is not a quantity that appears
anywhere in the code — it is the ratio of Washtenaw's county activity to the
fixture link's.

Fixed by `CalculatorRegistry::domain_excluded_total_activity_modules`, applied
in `MOVESEngine::planned_modules` next to the OMD swap. The fixture's planned
module count drops 32 -> 30.

#### 2. `sourceTypeID` was NULL where canonical emits 21

Not a magnitude error, but it meant a full-key join of the two `MOVESOutput`
tables matched **0 of 125 rows** (distinct from the `iterationID` NULL in
Section 1, which is expected and unchanged).

Root cause: `RunSpecXML.enforceConsistency()`, which canonical runs at RunSpec
load, promotes `fuelType`, `sourceUseType`, `roadType` and `emissionProcess`
to selected whenever a non-NONROAD run selects `onRoadSCC` — the SCC is
`concat('22', fuelTypeID, sourceTypeID, roadTypeID, processID)`
(`AggregationSQLGenerator.java`), so those four columns have to survive
aggregation. The port never ported that rule, and
`AggregationSQLGenerator`'s `null as sourceTypeID` branch therefore fired.

Fixed as `RunSpec::enforce_consistency`, applied in `ExecutionRunSpec::new` —
deliberately *not* in the XML/TOML parsers, so surface-format round-trips stay
byte-stable.

This is corpus-wide, not `scale-project`-specific: every onroad fixture ships
`<onroadscc selected="true"/>` with `<sourceusetype selected="false"/>`, and
every canonical snapshot has `sourceTypeID` populated. Row counts are
unaffected — the SCC key already encodes all four subfields, so the group-by
is unchanged and only the emitted column values move from NULL to the real
value. Measured: `canonical_snapshot_diff` row counts are identical
fixture-for-fixture before and after.

#### 3. The `evefficiency` section was never enabled

With defects 1 and 2 fixed, 104 of the 125 rows (fuel types 1/2/5) agreed with
canonical to a worst relative error of 9.3e-06, but the 21 electricity rows
(`fuelTypeID` 9) were **10.7% to 22.1% low**, monotone in model year. That is
`BaseRateCalculator.sql`'s `evefficiency` section:

```sql
-- @algorithm emissionRate=emissionRate/(batteryEfficiency*chargingEfficiency),
--            meanbaserate=meanbaserate/(batteryEfficiency*chargingEfficiency)
```

`BaseRateCalculator.java` enables it unconditionally (`// always run
evefficiency section`). The port implements the arithmetic
(`baseratecalculator::adjust::apply_ev_efficiency`) and loads the table, but
built its `ModuleFlags` with `..Default::default()`, leaving `ev_efficiency`
**false**. The snapshot's `evefficiency` carries `polProcessID` 9101 only,
with `batteryEfficiency x chargingEfficiency` running from `0.95 x 0.94 =
0.8930` for the newest age group down to `0.828273 x 0.94 = 0.7786` for the
oldest — exactly the observed 10.7%-22.1% deficit.

Fixed by `BaseRateCalculator::module_flags`. This is shared onroad code:
`fuelTypeID` 9 is about 0.25%-0.3% of pollutant 91 in the default-scale
fixtures, so the defect showed up there as a ~-3.4e-04 per-pollutant residual.
Enabling the section moved `chain-so2-co2e-mechanism`,
`chain-so2-co2e-mechanism-control` from **-3.408e-04 to +5.366e-07** and
`process-tirewear` from **-3.403e-04 to +2.514e-07** — see §6.1.1.

#### Residual

**None beyond output precision.** All 125 rows join on the full key, and the
worst relative error over them is **9.32e-06** (median 2.21e-06), with no
structure by fuel type (fuel 1: 7.7e-06, fuel 2: 9.3e-06, fuel 5: 7.2e-06,
fuel 9: 4.8e-06) or by age. That is the `real*4` intermediate-table class: the
captured `SHO` carries six significant digits (`5.47084`, `6.76147`, ...),
about 2e-06 of relative granularity per value, and the emission is a product
of several such tables. It is the same class as, and no worse than, the
asserted default-scale fixtures (6e-08 to 8e-05).

#### Wiring it into the gate

This fixture is still **not** wired into `canonical_snapshot_diff`. Doing so
means editing `all_fixtures()`, which also feeds
`all_fixtures_run_without_error` and the "exactly 48 non-scale non-error
fixtures" catalogue assertion, so it is a deliberate three-test change and an
operator call. On the evidence above it now belongs in `asserted_fixtures`
(`ONROAD_REL_TOL`, non-vacuous), not in `QUARANTINED_FIXTURES`.

#### Capture determinism

Note also that across two independent captures of this fixture on 2026-09-10
(same SIF, input DB differing only in its month filter) all 360 table Parquets
were byte-identical except `db__out_scale_project__movestablesused`;
`db__out_scale_project__movesoutput` in particular did not change a byte. Only
`manifest.json` and `provenance.json` otherwise differ. That is a stronger
result than the corpus generally holds — see
`characterization/snapshots/README.md` §"Measured limits of the determinism
contract" for the two known sources of run-to-run drift.

### 6.1.1 Regression evidence for the §6.1 fixes

`cargo test --release -p moves-cli --test full_suite_regression`, same machine,
same snapshot tree, before (`2eb13772`) and after:

| | before | after |
|---|---|---|
| test result | 8 passed, 1 failed | 8 passed, 1 failed |
| `canonical_snapshot_diff` | 39 asserted-pass, 3 UNCLASSIFIED | 39 asserted-pass, 3 UNCLASSIFIED |
| row counts | — | identical, fixture for fixture |

No fixture changed verdict and no tolerance was touched. Three fixtures'
residuals improved by roughly 635x:

| fixture | before | after |
|---|---|---|
| `chain-so2-co2e-mechanism` | -3.408e-04 (UNCLASS) | +5.366e-07 (UNCLASS) |
| `chain-so2-co2e-mechanism-control` | -3.408e-04 (UNCLASS) | +5.366e-07 (UNCLASS) |
| `process-tirewear` | -3.403e-04 (PASS) | +2.514e-07 (PASS) |

The two `chain-so2-co2e-mechanism*` fixtures stay UNCLASSIFIED only because
they are absent from `asserted_fixtures()` — a catalogue entry, not a
numerical problem; they are now well inside `ONROAD_REL_TOL`.
`nr-airtoxics-lawn-garden-county` is unchanged (`-1.000e0`, 968 port rows
against 14036 canonical) — a NONROAD air-toxics coverage gap, untouched here.

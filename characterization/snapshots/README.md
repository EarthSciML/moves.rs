# Fixture snapshots

This directory holds the canonical-MOVES snapshots produced by Phase 0
Task 4 (`mo-kbjl`). One sub-directory per fixture; each is the
deterministic, content-addressed regression baseline that every other
phase verifies against.

## Acceptance status

As of 2026-09-09 `characterization/fixtures/` holds **54** RunSpec XML
fixtures, and **42** of them have a populated snapshot directory here (one
sub-directory per fixture carrying a `manifest.json`). All 42 are
`moves-snapshot/v2` — see "Float encoding" below. The three `scale-*`
fixtures are skipped (require an additional input DB; see
`characterization/fixtures/README.md`). The canonical-diff regression gate
asserts them against canonical MOVES (see
`docs/known-divergences.md` §1b).

Four of the 42 are `<modeldomain value="SINGLE"/>` county-domain runs
(`process-apu-single`, `process-crankcase-extidle-single`,
`process-crankcase-start-single`, `process-extended-idle-single`) and
**cannot** be captured with `run-fixture.sh` alone — MOVES fails with
"The database does not have the required county." They go through
`apptainer/capture-county-snapshot.sh`, which seeds `washtenaw_cdb` from
`../county-inputs/washtenaw-county/setup-{starts,hotelling}.sql` first;
each SQL file's header names the fixtures it serves. `scale-county.xml` is
a fifth SINGLE-domain fixture but has never been captured and is out of
scope here.

Counts measured with `ls characterization/fixtures/*.xml | wc -l` (54),
`ls -d characterization/snapshots/*/ | wc -l` (42) and
`ls characterization/snapshots/*/manifest.json | wc -l` (42). The two
2026-09-08 additions are `chain-so2-co2e-mechanism` and
`chain-so2-co2e-mechanism-control`.

The original acceptance pass was T7 (`mo-o785i`) on 2026-05-26 — at that
time the suite was 34 non-scale fixtures, **34/34 succeeded, 0 failed**, and
T8 (`mo-zt8nk`) triage found no failures requiring re-run; the suite has
since grown to the counts above.

| Fixture | Status | Notes |
|---------|--------|-------|
| 42 non-scale fixtures | OK | Populated; see sub-directories |
| scale-county | skipped | Requires additional input DB |
| scale-project | skipped | Requires additional input DB |
| scale-rates | skipped | Requires additional input DB |

## Layout

```
characterization/snapshots/
├── README.md                       (this file)
└── <fixture-name>/                 (one per RunSpec)
    ├── manifest.json               aggregate hash, table list
    ├── provenance.json             SIF SHA + RunSpec SHA + fixture name
    ├── execution-trace.json        Java classes / SQL files / Go calcs touched
    └── tables/
        ├── db__movesoutput__movesactivityoutput.parquet
        ├── db__movesoutput__movesactivityoutput.meta.json
        ├── db__movesoutput__movesoutput.parquet
        ├── ...
        ├── moves_temporary__sourcetypeyearvmt_2020_tbl.parquet
        ├── ...
        ├── worker_folder__workertemp00__output_tbl.parquet
        └── ...
```

The `manifest.json` and `tables/` layout is the canonical
`moves-snapshot/v1` format defined in `crates/moves-snapshot`. The
`provenance.json` sidecar is added by `moves-fixture-capture` and lists:

| Field | Source |
|-------|--------|
| `fixture_name` | filename-derived from the RunSpec (sanitized, lowercased) |
| `sif_sha256` | from `characterization/fixture-image.lock` |
| `runspec_sha256` | sha256 over the RunSpec file's bytes |
| `snapshot_aggregate_sha256` | manifest's aggregate hash, mirrored for cross-reference |
| `output_database` / `scale_input_database` | parsed from the RunSpec |

## Execution trace (`execution-trace.json`)

Phase 0 Task 8 (bead `mo-d7or`) deliverable. Where the snapshot answers
"what numbers did MOVES produce?", the execution trace answers "which
pieces of MOVES were exercised producing them?" — making the snapshot
the regression baseline AND a coverage/migration-ordering input for
Phase 1 Task 9 (bead `mo-55l0`). The per-fixture traces here are rolled
up into a suite-wide coverage map at `characterization/coverage/` by
the `moves-coverage` CLI — see that directory's `README.md` for the
schema and `jq` recipes.

The trace is assembled post-run by `moves-fixture-capture` from the
same captures directory the snapshot is built from:

| Source | Contributes |
|--------|-------------|
| `worker-folder/<bundle>/worker.sql` | Java classes (FQN scan), SQL macro paths, Go-calc references, statement count |
| `worker-folder/<bundle>/*.go.input` / `*.go.output` / `*.go.txt` | Go calculator names from filename markers |
| `moves-temporary/instrumentation/class-load-*.log` | Java classes loaded by the JVM (filtered to `gov.epa.otaq.moves.*`) |

Top-level fields:

| Field | Description |
|-------|-------------|
| `trace_version` | `moves-fixture-capture/v1`. Bumped on incompatible schema changes. |
| `fixture_name` / `sif_sha256` / `runspec_sha256` | Mirrored from provenance for self-contained reading. |
| `java_classes` | `[{name, kind}]` sorted by `name`. `kind` is one of `calculator`, `generator`, `framework`, `worker`, `master`, `common`, `utils`, `other` based on the package path. |
| `sql_files` | `[{path, consumed_by}]` sorted by `path`. `consumed_by` lists worker bundle ids that referenced the file. |
| `go_calculators` | `[{name, invoked_in}]` sorted by `name`. |
| `worker_bundles` | `[{id, java_classes, sql_files, go_calculators, statement_count}]` sorted by `id`. |
| `sources` | `{worker_sql_files, class_load_log_files}` — count of inputs consumed. |

Determinism: every list is sorted, the captures walk is sorted, and the
JSON is pretty-printed with a trailing newline. Two runs with the same
captures directory produce a byte-identical `execution-trace.json`.

The JVM class-load log is populated by
`characterization/apptainer/run-fixture.sh`, which sets
`JAVA_TOOL_OPTIONS=-Xlog:class+load=info:file=/opt/moves/MOVESTemporary/instrumentation/class-load-%p.log`
before invoking MOVES. Every JVM that runs (ant + the forked MOVES JVM)
writes a per-PID `class-load-<pid>.log` into the bind-mounted
instrumentation directory. The trace builder filters those down to the
`gov.epa.otaq.moves.*` package space. Class-load events approximate
"this code was reached" for coverage purposes — JVMs load classes
lazily on first reference, so a class in the trace is one that the
fixture's execution path touched.

Inspect the trace:

```sh
# Top-level summary:
jq '{fixture_name, classes: (.java_classes | length), sql_files: (.sql_files | length), go: (.go_calculators | length)}' \
    characterization/snapshots/<fixture>/execution-trace.json

# Calculators only:
jq '.java_classes | map(select(.kind == "calculator"))' \
    characterization/snapshots/<fixture>/execution-trace.json

# Which SQL macros did this fixture touch?
jq '.sql_files[].path' \
    characterization/snapshots/<fixture>/execution-trace.json
```

## Determinism contract

> [!IMPORTANT]
> **This guarantee is measurably false, and the wording below has not yet
> been amended — that is a pending decision, not an oversight.** See
> "Measured limits of the determinism contract" immediately after this
> section. The two failures were both present under v1; v1's rounding hid
> one of them and nobody had captured the same fixture twice to notice the
> other.

Two runs with the same SIF SHA256 + same RunSpec bytes produce
**byte-identical** files in this directory. The pieces that uphold the
contract:

1. The patched MOVES SIF (`moves-fixture.sif`) is deterministic given
   the same MOVES_COMMIT + MOVESDB hash + patch — `fixture-image.lock`
   pins the resulting SIF SHA.
2. `dump-databases.sh` (run inside the SIF) lists databases, tables, and
   columns via `INFORMATION_SCHEMA` `ORDER BY` clauses, and dumps each
   table with `SELECT ... ORDER BY 1, 2, ..., N`.
3. `moves-fixture-capture`'s directory walk is sorted lexicographically.
4. The `moves-snapshot` crate normalizes floats to canonical decimal
   strings, sorts rows by the natural key, and writes parquet files with
   `compression=UNCOMPRESSED`, `dictionary_enabled=false`,
   `statistics_enabled=None`, and a fixed `created_by` stamp.

If a snapshot file's bytes change, the underlying MOVES output changed —
that's the regression-detection signal Phase 0 is designed to provide.

## Measured limits of the determinism contract

The recapture sweep (2026-09-08/09) captured two fixtures twice, from the
same SIF (`4f92c593`), the same RunSpec bytes and the same host
(`ccc0232`). Neither pair was byte-identical, for two independent reasons.

**1. Worker-temp table names are assigned per run.** MOVES's master hands
work to `MOVESTemporary/manyworkers/workerfolder/workertempN/`, and both
*N* and how many workers it uses vary between runs of the same fixture:

| fixture | capture A | capture B |
|---|---|---|
| `process-brakewear` | `workertemp2`, `workertemp` | `workertemp8`, `workertemp9` |
| `nr-airtoxics-lawn-garden-county` | (none) | `workertemp2` |

That is 6 of 366 table *names* for `process-brakewear` and a 324-vs-327
table count for `nr-airtoxics-lawn-garden-county`. Row totals reconcile
exactly in both cases — the per-worker `output_tbl` row counts sum to
`MOVESOutput` either way — so no data is lost or gained; the partition is
simply not reproducible. **This has nothing to do with float encoding and
was equally true of every v1 snapshot.**

**2. MOVES itself produces a value that is not bit-reproducible.**
`drivingIdleFraction` in `db__movesexecution…__drivingidlefraction`
(1 row, present in all 42 fixtures, no float in its natural key) came out
as

    3.0299686857752525e-02      and      3.0299686857752545e-02

in the two `process-brakewear` captures — 6 ULP apart, relative difference
6.9e-16, almost certainly a floating-point aggregation-order effect in the
MariaDB query that computes it. Under v1 both rendered as
`0.030299686858` at twelve decimal places, which is exactly what the
committed v1 snapshot holds, so the difference was invisible. **v2 does not
introduce this; it discloses it.**

Scope of the measurement: n = 2 fixtures, the only ones captured twice.
`nr-airtoxics-lawn-garden-county` showed **0 of 324** common tables
differing in content, so value-level nondeterminism is not universal — one
cell in one table is all that has been observed. A wider estimate needs
repeat captures across the suite, which this sweep did not perform.

### What this affects

* `.github/workflows/fixture-suite-weekly.yml` runs `moves-snapshot diff`
  in strict byte-identity mode (exit 0 = match). Against a v2 corpus that
  gate will now report drift on any fixture whose re-capture lands a
  different worker partition, and on `drivingidlefraction` whenever the
  aggregation order differs — neither being a real regression.
* `characterization/tolerance.toml` sets `default_float_tolerance = 0.0`,
  which cannot absorb a 6.9e-16 relative difference. A per-column
  tolerance on `drivingidlefraction.drivingIdleFraction` (or a small
  global relative floor, order 1e-12) would; so would excluding the
  `moves_temporary__manyworkers__*` tables from the strict table-set
  comparison.

Both changes are deliberately **not** made in the sweep that found the
problem: retracting a stated guarantee and loosening the regression gate
are decisions for a reviewer, and folding them into a 42-fixture recapture
would make the diff impossible to reason about.

## Float encoding, and the v1 → v2 format change

**The v1 → v2 recapture sweep ran on 2026-09-08/09: all 42 snapshots here
are now `moves-snapshot/v2`, and all 14 766 table sidecars carry
`float_encoding` with no `float_decimals`.** The rest of this section
describes what changed and why; it is kept because the reasoning is the
justification for the corpus you are reading.

`moves-snapshot/v1` stored each float as a fixed-decimal string with
**twelve places after the point**. Twelve decimal places is not twelve significant digits: a value
near 1e-9 kept four significant digits, one near 1e-11 kept two, and
anything below 5e-13 was stored as `0.000000000000`. The measurement is
in
[`../audit-results/20260908T0948-float-precision-blast-radius.md`](../audit-results/20260908T0948-float-precision-blast-radius.md)
— across the 40 populated snapshots, 60 226 821 of 64 656 397 non-zero
float cells carry fewer significant digits than an f64 holds, and
17 234 of them carry too few to support a 1e-6 relative comparison.

`moves-snapshot/v2` replaces the rule: a float is stored as the shortest
correctly-rounded decimal that parses back to a **bit-identical** f64,
written in normalized scientific notation (`1.9e-11`, `1.5e+00`,
`0e+00`). It is lossless at every magnitude, and the rule is defined as a
property of the number rather than of any formatter, so it holds the same
determinism guarantee.

Two consequences for anything reading this directory:

* Each `tables/<name>.meta.json` says which rule produced it. v1 sidecars
  carry `"float_decimals": 12`; v2 sidecars carry
  `"float_encoding": {"kind": "shortest_round_trip",
  "max_significant_digits": 17}` and **no** `float_decimals` field,
  because there is no fixed decimal count any more. A consumer that
  floored its tolerance at `0.5 * 10^-float_decimals` should floor at
  **zero** when it sees `float_encoding`.
* Rows whose natural key includes a float column sort by numeric (IEEE
  total) order in v2, where v1 sorted them by the fixed-decimal string —
  which put `10.0` before `9.0`. 120 of the 14 055 committed tables have
  a float in the natural key, so their row order will change on
  recapture.

`moves-snapshot` reads both versions and remembers which one it read, so a
v1 snapshot restored from history stays diffable and byte-stable. See
[`../../docs/snapshot-v2-migration.md`](../../docs/snapshot-v2-migration.md)
for what the sweep measured against what it predicted.

## Producing a snapshot

```sh
# From the repo root, with characterization/apptainer/moves-fixture.sif
# already built (see characterization/apptainer/README.md):
characterization/apptainer/run-fixture.sh \
    --fakeroot \
    --runspec /opt/moves/testdata/SampleRunSpec.xml
```

The wrapper:

1. Sets up the host scratch layout under
   `/scratch/$USER/moves-fixture/<fixture-name>/`
   (`mariadb-data/`, `run-mysqld/`, `MOVESTemporary/`, `WorkerFolder/`,
   `captures/`).
2. Invokes `run-moves.sh -f --runspec <path>` to execute the patched
   MOVES against `moves-fixture.sif`. The patch retains
   `MOVESTemporary/`, `WorkerFolder/WorkerTempXX/`, and external
   generator outputs.
3. Re-enters the SIF with `dump-databases.sh` bind-mounted to dump every
   non-system MariaDB database to `captures/databases/<db>/<table>.tsv`
   plus a `<table>.schema.tsv` sidecar.
4. Mirrors `MOVESTemporary/` and `WorkerFolder/` into `captures/`.
5. Runs `moves-fixture-capture`, which converts the captures into a
   deterministic snapshot at `characterization/snapshots/<fixture-name>/`.

The full set of options, including `--workdir`, `--output-dir`,
`--keep-captures`, and `--skip-run`, is documented at the head of
`run-fixture.sh`.

## Producing the full Phase 0 fixture suite

`characterization/fixtures/` ships 51 RunSpec XML fixtures (Phase 0 Task 5/6,
bead `mo-n2yg`, plus subsequent additions). To populate the matching snapshots
end-to-end:

```sh
# Once, on an HPC compute node with Apptainer + fakeroot:
characterization/apptainer/build-sif.sh           # canonical-moves.sif
characterization/apptainer/build-fixture-sif.sh   # moves-fixture.sif

# Then run the whole suite (default skips the three scale-* fixtures
# that require additional supporting input databases — see
# characterization/fixtures/README.md):
characterization/run-all-fixtures.sh --fakeroot --keep-going
```

Each fixture writes to `characterization/snapshots/<fixture-name>/`. The
runner is idempotent: re-running with the same SIF SHA + RunSpec bytes
produces byte-identical snapshot files (the determinism contract above
applies suite-wide, not just per-fixture).

A typical suite takes roughly N × 5–10 minutes wall-clock plus the
one-time SIF build. Reserve a few CPU-hours for a full pass and treat the
output as a content-addressed regression baseline pinned by the SIF lock
file.

## Inspecting a snapshot

```sh
# Top-level manifest with aggregate hash and table list:
jq . characterization/snapshots/<fixture>/manifest.json

# Provenance — SIF + RunSpec identity:
jq . characterization/snapshots/<fixture>/provenance.json

# Per-table schema:
jq . characterization/snapshots/<fixture>/tables/<table>.meta.json

# Read parquet (Polars / pandas / DuckDB):
duckdb -c "SELECT * FROM read_parquet(
  'characterization/snapshots/<fixture>/tables/db__movesoutput__movesoutput.parquet'
) LIMIT 10"
```

## Comparing snapshots

The `moves-snapshot diff` CLI (Phase 0 Task 7, bead `mo-obyw`) ships in the
workspace as a binary in the `moves-snapshot` crate. Build with
`cargo build --release` and use:

```sh
# Strict byte-identity check. Exit 0 = match, 1 = drift, 2 = error.
target/release/moves-snapshot diff \
    characterization/snapshots/samplerunspec/ \
    /tmp/fresh/samplerunspec/

# Per-(table, column) tolerance, JSON output for CI / jq.
target/release/moves-snapshot diff \
    characterization/snapshots/samplerunspec/ \
    /tmp/fresh/samplerunspec/ \
    --tolerance characterization/tolerance.toml \
    --format json | jq '.summary'
```

The TOML tolerance config (see `characterization/tolerance.toml` and the
crate-level docs in `crates/moves-snapshot/src/tolerance.rs`) lets you
absorb harmless numerical artifacts on a per-column basis without losing
the regression-detection signal on every other column.

The `.github/workflows/fixture-suite-weekly.yml` workflow exercises this
diff against the pinned canonical-MOVES SIF on a weekly cron, so any
upstream drift (or determinism break) fires within a week even when
nobody is actively touching the repo.

For quick out-of-band checks:

```sh
# Different fixture-image SHA → different snapshots → audit which.
diff <(jq -S . a/manifest.json) <(jq -S . b/manifest.json)
```

## What is *not* in the snapshot

* The **default DB** (`movesdb20241112`) — it's read-only during a run
  and pinned by the SIF SHA already; capturing it would just bloat
  every snapshot with the same content.
* `mysql`, `information_schema`, `performance_schema`, `sys` — system
  databases, not produced by MOVES.
* Non-tabular files in `MOVESTemporary/` and `WorkerFolder/` (`.log`,
  `.txt`, `.sql`). The snapshot format stores tables; non-table
  forensic artifacts stay in the source scratch area, retained when
  `--keep-captures` is passed.

## Caveats

* MariaDB batch-mode output renders SQL `NULL` as the four-character
  string `"NULL"`. A `varchar` column whose value is the literal
  `"NULL"` is therefore indistinguishable from SQL `NULL` in the
  capture. MOVES output schemas don't use such values, but a future
  fixture set should keep this in mind.
* Worker `.tbl` files have no schema sidecar, so every column is stored
  as `Utf8` in the snapshot. Float-tolerance diffs (Phase 0 Task 7)
  apply only to database tables, not worker bundles. The byte-stable
  snapshot semantics still hold for `.tbl` content.
* Per-run database names that vary by clock (e.g.
  `MOVESExecution_<timestamp>` if MOVES injects a timestamp) would
  break determinism. The MOVES build we patch in `mo-1s9o` uses
  RunSpec-derived names, so this isn't an issue today; if it becomes
  one, the dumper should normalize the database name before writing
  the snapshot.

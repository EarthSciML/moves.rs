# Fixture snapshots

This directory holds the canonical-MOVES snapshots produced by Phase 0
Task 4 (`mo-kbjl`). One sub-directory per fixture; each is the
deterministic, content-addressed regression baseline that every other
phase verifies against.

## Layout

```
characterization/snapshots/
├── README.md                       (this file)
└── <fixture-name>/                 (one per RunSpec)
    ├── manifest.json               aggregate hash, table list
    ├── provenance.json             SIF SHA + RunSpec SHA + fixture name
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

## Determinism contract

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
4. The `moves-snapshot` crate normalizes floats to fixed-decimal strings,
   sorts rows by the natural key, and writes parquet files with
   `compression=UNCOMPRESSED`, `dictionary_enabled=false`,
   `statistics_enabled=None`, and a fixed `created_by` stamp.

If a snapshot file's bytes change, the underlying MOVES output changed —
that's the regression-detection signal Phase 0 is designed to provide.

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

Phase 0 Task 5/6 (bead `mo-n2yg`) ships 33 RunSpec XML fixtures under
`characterization/fixtures/`. To populate the matching snapshots end-to-end:

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

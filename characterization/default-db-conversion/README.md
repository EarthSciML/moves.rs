# Phase 4 default-DB conversion pipeline

Phase 4 Task 80 (bead `mo-yj9w`) deliverable. Converts a MOVES default
database from a MariaDB dump into the versioned Parquet layout the
lazy-loading reader (Task 82) will consume.

Drives off the partitioning plan from Task 79
(`characterization/default-db-schema/tables.json`) and produces:

```text
<output-root>/<db-version>/
├── manifest.json
├── <Table>.parquet                                 # monolithic strategy
├── <Table>.schema.json                             # schema_only strategy
├── <Table>/county=<id>/part.parquet                # county strategy
├── <Table>/year=<y>/county=<id>/part.parquet       # year_x_county strategy
└── <Table>/modelYear=<y>/part.parquet              # model_year strategy
```

The pipeline is re-runnable for future EPA default-DB releases: bump the
SIF and `tables.json`, run `convert-default-db.sh`, get a fresh
`<db-version>/` tree.

## Files

| File | Purpose |
|------|---------|
| `convert-default-db.sh` | Top-level orchestrator. Stages 1+2 in one call, or `--tsv-dir` to skip stage 1. |
| `dump-default-db.sh` | Stage 1: runs **inside** the canonical-moves SIF; starts MariaDB, dumps every BASE TABLE of the default DB to TSV + schema-TSV, writes `dump-manifest.json`. |
| `README.md` | This file. |

The TSV → Parquet conversion (stage 2) lives in the Rust crate
[`crates/moves-default-db-convert`](../../crates/moves-default-db-convert).
It depends on `tables.json` for the per-table partition strategy and on
the dumper's TSV pair for the data. Tested with unit + end-to-end suites.

## Two-stage pipeline

Stage 1 needs MariaDB; stage 2 is pure Rust. Splitting them keeps the
Rust converter fully testable on the host (no Docker / Apptainer) while
the SIF-bound dumper handles the MariaDB load + dump.

```text
┌──────────────────────────────────────┐    ┌──────────────────────────────────────┐
│ Stage 1: dump-default-db.sh          │    │ Stage 2: moves-default-db-convert    │
│ ───────────────────────────────────  │    │ ───────────────────────────────────  │
│ • runs inside canonical-moves.sif    │    │ • runs on the host                   │
│ • starts MariaDB on seeded data dir  │    │ • reads <Table>.tsv pairs            │
│ • SELECT * ORDER BY 1..N per table   │ -> │ • applies partition plan             │
│ • writes <Table>.tsv +               │    │ • writes Parquet per strategy        │
│   <Table>.schema.tsv + manifest      │    │ • computes SHA-256, row counts       │
└──────────────────────────────────────┘    │ • writes manifest.json               │
                                            └──────────────────────────────────────┘
```

## Inputs

* **SIF** — `characterization/apptainer/canonical-moves.sif`. Carries
  the MariaDB instance with `movesdb<DATE>` pre-loaded by
  `characterization/apptainer/build-sif.sh`. The SIF is pinned by
  `characterization/canonical-image.lock`.
* **`tables.json`** — `characterization/default-db-schema/tables.json`
  from Task 79. Names every default-DB table with its partition strategy.
* **(Optional) source dump path** — `--source-dump`. The orchestrator
  computes its SHA-256 and records it in the conversion manifest so the
  output is provenance-traceable to the input artifact.

## Outputs

`<output-root>/<db-version>/manifest.json` (schema tag
`moves-default-db-manifest/v1`) records:

* `moves_db_version` — the EPA release label (e.g. `movesdb20241112`).
* `moves_commit` — canonical MOVES commit copied from `tables.json`.
* `plan_sha256` — hash of the `tables.json` input.
* `generated_at_utc` — ISO-8601 timestamp.
* `tables[]` — sorted by case-folded name, with per-table:
  * `partition_strategy`, `partition_columns`, `primary_key`, `columns`
    (name + MySQL/Arrow types + PK flag).
  * `partitions[]` — relative path, partition values, row count, SHA-256,
    bytes-on-disk.
  * `row_count` — sum across partitions.
  * `schema_only_path` for the four empty tables (`Link`, `SHO`,
    `SourceHours`, `Starts`); their Parquet body is replaced by a JSON
    schema sidecar.

## Running the pipeline

The expected invocation on an HPC compute node, against the canonical SIF:

```bash
characterization/default-db-conversion/convert-default-db.sh \
    --sif       characterization/apptainer/canonical-moves.sif \
    --db        movesdb20241112 \
    --db-version movesdb20241112 \
    --plan      characterization/default-db-schema/tables.json \
    --output    default-db
```

The orchestrator stages output under `default-db/movesdb20241112/`,
matching the layout Task 82 will consume.

## Dry-run / dev iteration

For host-side iteration without rebuilding the SIF, pre-dump TSVs once on
an HPC node and feed them back to the converter:

```bash
# On the compute node, dump only:
characterization/default-db-conversion/convert-default-db.sh \
    --sif characterization/apptainer/canonical-moves.sif \
    --db  movesdb20241112 \
    --output /tmp/dump

# On any host, run stage 2 only:
cargo run --release -p moves-default-db-convert -- \
    --tsv-dir /tmp/dump/movesdb20241112/_tsv \
    --plan    characterization/default-db-schema/tables.json \
    --output  default-db/movesdb20241112 \
    --moves-db-version movesdb20241112
```

## Determinism contract

The converter pins Parquet writer settings (uncompressed, no dictionary,
no statistics, `PARQUET_1_0` writer version) so identical inputs yield
byte-identical output, and the SHA-256s in the manifest are stable.
The dumper's `ORDER BY` clauses guarantee deterministic row order, and
the converter sorts partition groups by their partition-value tuple.

## Partition strategies

Resolved from `tables.json` × the table's primary key. See
`partitioning-plan.md` for rationale.

| Strategy | Layout |
|----------|--------|
| `monolithic`    | `<Table>.parquet` |
| `schema_only`   | `<Table>.schema.json` (sidecar; no Parquet) |
| `county`        | `<Table>/<label>=<v>/part.parquet` — `label` is `county` (for `countyID` PK), `zone` (for `zoneID` PK), or `state` (for `stateID` PK). The plan's `zoneID` ⇄ `countyID` 1:1 join at the default scale is deferred to the reader; the converter labels partitions by the actual column present in the PK. |
| `year_x_county` | `<Table>/year=<y>/<label>=<v>/part.parquet` |
| `model_year`    | `<Table>/modelYear=<y>/part.parquet` |

If a table's PK doesn't carry a usable partition column for its strategy,
the converter errors out. This catches audit drift before silently
writing the wrong layout.

## Row-count validation

For every partitioned table the converter sums the per-partition Parquet
row counts and cross-checks against the source TSV's line count. A
mismatch aborts the run with `RowCountMismatch`. Schema-only tables
report a `0` expected count; if a future EPA release populates one of
the historically-empty tables, the converter emits a warning instead of
silently dropping data — the audit must be updated to reclassify it.

## Memory model

The current implementation loads each table fully into memory before
writing. The largest default-DB tables (≤50M rows per the audit caveats)
fit comfortably on a workstation with 8 GiB RAM. Tables that grow past
that threshold in future releases should be re-bucketed in `tables.json`
(see `partitioning-plan.md`'s "large-monolithic re-review queue") so
their partition layout drops the per-table memory footprint by
construction.

## Open items / follow-up

These are deliberately out of scope for Task 80 and tracked elsewhere:

1. **zone → county join.** Tables with `zoneID` PK are partitioned by
   zone. The lazy-loading reader (Task 82) will join through the `Zone`
   dimension when callers filter by county.
2. **Validation pass.** Task 81 runs this pipeline on the real
   `movesdb20241112.zip` and verifies that every row from the SQL dump
   appears exactly once in the Parquet output and that `DOUBLE`/`DATE`
   types round-trip via Polars/Arrow.
3. **`large` monolithic re-review.** The partitioning plan flags tables
   whose measured row count may exceed 50M. Task 80 records true row
   counts in the manifest; Task 81's validation step rebases the
   partition decisions if the measured counts demand it.
4. **Row-group statistics.** Currently disabled for byte-stable hashes.
   Task 82 (the reader) may flip this once the determinism contract is
   relaxed to "content equivalence" rather than "byte equivalence".

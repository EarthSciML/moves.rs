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
| `convert-default-db.sh` | Top-level orchestrator for conversion. Stages 1+2 in one call, or `--tsv-dir` to skip stage 1. |
| `validate-default-db.sh` | Phase 4 Task 81 orchestrator. Runs the conversion **and** validates the result against the source TSV dump. |
| `dump-default-db.sh` | Stage 1: runs **inside** the canonical-moves SIF; starts MariaDB, dumps every BASE TABLE of the default DB to TSV + schema-TSV, writes `dump-manifest.json`. |
| `README.md` | This file. |

The TSV → Parquet conversion (stage 2) lives in the Rust crate
[`crates/moves-default-db-convert`](../../crates/moves-default-db-convert).
It depends on `tables.json` for the per-table partition strategy and on
the dumper's TSV pair for the data. Tested with unit + end-to-end suites.

The validation tool — Task 81's deliverable — is the
`moves-default-db-validate` binary in the same crate. It is independent
of MariaDB at validation time: the source TSV is the authoritative
artifact (emitted by `mariadb -B -N` immediately before the dumper
exits), so anything that round-trips TSV → Parquet → readback equals
the MariaDB content modulo the documented escape encoding.

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

## Validation (Task 81)

`validate-default-db.sh` drives the full pipeline + cross-check:

```bash
characterization/default-db-conversion/validate-default-db.sh \
    --sif        characterization/apptainer/canonical-moves.sif \
    --db         movesdb20241112 \
    --plan       characterization/default-db-schema/tables.json \
    --output     default-db \
    --source-dump path/to/movesdb20241112.zip
```

The validator (`moves-default-db-validate`) reads the manifest and, for
every table:

| Check | What it asserts |
|-------|-----------------|
| Manifest drift | Each `partitions[*]` file exists on disk; its SHA-256 matches the manifest. |
| Parquet schema | Column count, names, and Arrow types in the readback match `manifest.tables[*].columns`. |
| Row totals | `sum(partitions.row_count) == source.tsv line count == manifest.row_count`. |
| Per-column aggregates | For every Int64/Float64 column: `count_non_null`, `min`, `max`, and a scaled-decimal sum match between TSV parse and Parquet readback. Float64 equality is exact (bit-pattern) because both sides parse via `str::parse::<f64>`. |
| First-row spot check | Monolithic tables: field-by-field comparison of the first source TSV row vs the first Parquet row. |

Findings are reported per-kind with a counts summary; exit code `1`
signals validation errors so CI can gate on this binary directly.

### Task 81 audit reconciliation

A side effect of validating against the real `movesdb20241112` dump is
that several drifts between Task 79's audit (parsed from
`CreateDefault.sql`) and the actual EPA release surfaced. The audit was
patched as part of Task 81:

| Change | Reason |
|--------|--------|
| Added 8 tables: `fuelAdjustment`, `fuelEngFraction`, `importStartsOpModeDistribution`, `imTestType`, `nrProcessGroup`, `pollutantDisplayGroup`, `regClassFraction`, `startsSourceTypeFraction` | Present in the dump, absent from the canonical DDL Task 79 parsed. |
| Removed 11 NR* tables: `NRCrankCaseEmissionRatio`, `NRExhaustEmissionRate`, `NRFuelOxyAdjustment`, `NRPollutantProcessModelYear`, `NRProcessEmissionRate`, `NRSourceBin`, `NRStateSurrogateTotal`, `NRTemperatureAdjustment`, `NRTransientAdjustFactor`, `NRYear`, `NRZoneAllocation` | Declared by the canonical DDL but not materialised by the `movesdb20241112` release. |
| Promoted `Link` from `schema_only` to `monolithic` | Task 79 assumed Link ships empty; the dump carries 22,610 rows. The schema-only optimisation was unsound. |
| Added `isUserInput` column to `AverageTankTemperature`, `OpModeDistribution`, `SoakActivityFraction`, `SourceBinDistribution`, `startsOpModeDistribution` | The canonical DDL omitted this metadata column; the EPA release carries it. |

### Converter changes (Task 81)

Two robustness improvements to `moves-default-db-convert`:

1. **Case-insensitive TSV lookup.** MariaDB on Linux normalises the
   original Windows dump's CamelCase table names to lowercase on disk
   (`lower_case_table_names=1`), but the audit preserves the CamelCase
   from the MOVES Java schema. Without case-insensitive matching,
   ~90% of tables silently skipped. The converter now falls back to a
   case-insensitive directory scan when the exact-case path miss.
2. **Column drift demoted to warning.** When the audit's column list
   doesn't match the dump's `<Table>.schema.tsv`, the converter records
   a warning in the report and proceeds using the dump's schema (which
   is authoritative for data). Strict mode (`--require-every-table`)
   escalates the warning to an error so a CI gate catches drift in the
   audit.

## End-to-end run on `movesdb20241112.zip`

Captured by Task 81, [bead `mo-eq5d`](#):

| Metric | Value |
|--------|-------|
| Tables | 240 |
| Partitions | 31,677 |
| Total rows | 8,436,056 |
| Parquet bytes (uncompressed) | ~658 MiB |
| Conversion runtime | 30s (8-core workstation, warm cache) |
| Validation runtime | 36s |
| Validation errors | 0 |
| Validation warnings | 0 |

## Open items / follow-up

These are deliberately out of scope and tracked elsewhere:

1. **zone → county join.** Tables with `zoneID` PK are partitioned by
   zone. The lazy-loading reader (Task 82) will join through the `Zone`
   dimension when callers filter by county.
2. **`large` monolithic re-review.** The partitioning plan flags tables
   whose measured row count may exceed 50M. Task 80 records true row
   counts in the manifest; if any future EPA release pushes a table
   past that band, the audit needs to be reclassified.
3. **Row-group statistics.** Currently disabled for byte-stable hashes.
   Task 82 (the reader) may flip this once the determinism contract is
   relaxed to "content equivalence" rather than "byte equivalence".
4. **Polars round-trip.** Task 82 brings in Polars and will exercise
   the lazy-loaded read path. The Arrow-based readback in
   `moves-default-db-validate` is functionally equivalent (Polars'
   Parquet reader is built on arrow-rs) so the byte-pattern equality we
   already check carries over.

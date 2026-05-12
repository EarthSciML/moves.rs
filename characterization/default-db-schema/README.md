# Phase 4 default-DB schema audit

Phase 4 Task 79 (bead `mo-r5sz`) deliverable. Inventory of every table in
the MOVES default database (`movesdb20241112`) with a per-table Parquet
partition decision feeding Phase 4 Task 80 (the conversion pipeline) and
Task 82 (the lazy-loading reader).

## Files in this directory

| File | Purpose |
|------|---------|
| `audit-schema.py` | Parser/classifier. Reads the canonical MOVES `CreateDefault.sql` and `CreateNRDefault.sql` and emits `tables.json`. Byte-deterministic for a fixed input pair. |
| `tables.json` | Machine-readable inventory. Schema tag `moves-default-db-schema/v1`. Consumed by Task 80 to drive per-table Parquet writes. |
| `partitioning-plan.md` | Human-readable plan: which tables stay monolithic, which partition by year, by county, or by both, and why. |
| `README.md` | This file. |

## Regenerating

The schema sources live in the canonical MOVES tree pinned by
`characterization/canonical-image.lock` — they are **not vendored** in
this repository. Fetch them at the pinned commit and run the parser:

```bash
PINNED=25dc6c833dd8c88198f82cee93ca30be1456df8b  # canonical-image.lock moves_commit
curl -fsSL "https://raw.githubusercontent.com/USEPA/EPA_MOVES_Model/${PINNED}/database/CreateDefault.sql" \
    -o /tmp/CreateDefault.sql
curl -fsSL "https://raw.githubusercontent.com/USEPA/EPA_MOVES_Model/${PINNED}/database/CreateNRDefault.sql" \
    -o /tmp/CreateNRDefault.sql

python3 characterization/default-db-schema/audit-schema.py \
    --default-sql    /tmp/CreateDefault.sql \
    --nr-default-sql /tmp/CreateNRDefault.sql \
    --moves-commit   "${PINNED}" \
    --output         characterization/default-db-schema/tables.json
```

The parser captures every CREATE TABLE body plus the out-of-band
`CREATE UNIQUE INDEX XPK<Table>` and `ALTER TABLE ... ADD KEY ...`
fragments MOVES uses for several large rate tables (`SHO`,
`SourceHours`, `EmissionRate*`, etc.). The SIF runs MariaDB with
`lower_case_table_names=1`, so the parser dedupes case-variant table
names (`nrAgeCategory` and `NRAgeCategory` collapse to one physical
table); see `characterization/apptainer/files/my.cnf` for the setting.

## What `tables.json` contains

Top-level fields:

| Field | Type | Description |
|-------|------|-------------|
| `schema_version`  | string | `moves-default-db-schema/v1`. Bumped on incompatible changes. |
| `moves_commit`    | string | Canonical-MOVES commit the inputs were drawn from. |
| `sources`         | object | SHA-256 of each input SQL file. Detect upstream drift. |
| `table_count`     | int    | Number of distinct tables after case-fold dedup. |
| `tables`          | array  | Per-table records, sorted by case-folded table name. |

Each table record carries:

| Field | Type | Description |
|-------|------|-------------|
| `name`                       | string | Original-case table name as written in the DDL (first occurrence). |
| `primary_key`                | array  | Ordered PK column list. Empty when the DDL declares only secondary indexes. |
| `columns`                    | array  | `{name, type}` per column, in ordinal order. |
| `indexes`                    | array  | `{unique, columns}` per secondary index (`KEY`, `INDEX`, and out-of-band `CREATE INDEX`). |
| `estimated_rows_upper_bound` | int    | Upper bound from PK cardinality product with a sparsity prior; **not** the actual count. Task 80 measures and overwrites. |
| `size_bucket`                | string | `empty`, `tiny` (<100), `small` (<10k), `medium` (<1M), `large` (<50M), `huge` (≥50M), or `unknown` (no PK). |
| `filter_columns`             | array  | PK + index columns, deduped — the schema-encoded proxy for "frequently filtered on" until the Phase 1 coverage map populates. |
| `partition`                  | object | `{strategy, rationale}`. `strategy` ∈ `monolithic`, `schema_only`, `year`, `county`, `year_x_county`, `model_year`. |

## Estimator caveats

`estimated_rows_upper_bound` is the product of dimension cardinalities for
each PK column, attenuated by a sparsity prior for known sparse patterns
(e.g. `beginModelYearID`/`endModelYearID` pairs). It overshoots reality
because MOVES rate tables carry sparse coverage in practice — a table
with a 7-column PK rarely has 10⁹ rows. The estimator's job is to sort
tables into coarse buckets (`tiny` … `huge`), not to predict exact
counts. Task 80 emits true row counts during conversion; downstream
consumers should trust those over the upper bound.

The Phase 1 coverage map (`characterization/coverage/coverage-map.json`)
is empty in the current checkout (no fixture suite captured yet), so the
per-column "frequently filtered on" signal falls back to the schema's
own primary-key + index column list. When the coverage map populates,
the audit can be re-run with a smarter `filter_columns` proxy that
reads bundle SQL.

## Update frequency

Every table in the default DB ships as part of the EPA release artifact
(`movesdb<DATE>.zip`). EPA cuts a new release every ~1–2 years. The
"update frequency" axis the migration plan asks for is therefore
uniform — every table updates per-release — so it does not factor into
per-table partition decisions. The relevant axis is per-table row count
and partition-column composition.

A subset of default-DB tables (`avft`, `FuelSupply`, `HPMSVtype*`,
`SourceTypeYearVMT`, etc.) can be overridden by user input databases on
a per-run basis. The conversion pipeline still treats those as
default-DB tables; per-run overrides are imported separately by Phase 4
Task 83/84.

## Task 81 reconciliation

The audit was originally generated by parsing the canonical DDL
(`CreateDefault.sql` + `CreateNRDefault.sql`). Phase 4 Task 81 ran the
conversion pipeline on the actual `movesdb20241112.zip` and surfaced
four kinds of drift between the DDL and the released dump:

| Drift | Resolution |
|-------|------------|
| 11 NR* tables (`NRCrankCaseEmissionRatio`, `NRExhaustEmissionRate`, `NRFuelOxyAdjustment`, `NRPollutantProcessModelYear`, `NRProcessEmissionRate`, `NRSourceBin`, `NRStateSurrogateTotal`, `NRTemperatureAdjustment`, `NRTransientAdjustFactor`, `NRYear`, `NRZoneAllocation`) declared in the DDL but absent from the release. | Dropped from `tables.json`. |
| 8 tables (`fuelAdjustment`, `fuelEngFraction`, `importStartsOpModeDistribution`, `imTestType`, `nrProcessGroup`, `pollutantDisplayGroup`, `regClassFraction`, `startsSourceTypeFraction`) present in the release but not declared in the canonical DDL. | Added to `tables.json` with `monolithic` strategy (all are tiny/empty). |
| `Link` marked `schema_only` based on the assumption it ships empty. | Promoted to `monolithic`; the actual release carries 22,610 rows. |
| 5 tables (`AverageTankTemperature`, `OpModeDistribution`, `SoakActivityFraction`, `SourceBinDistribution`, `startsOpModeDistribution`) missing the `isUserInput` column the release adds. | Column list extended. |

After these fixes the audit has 240 tables matching the actual release,
and `moves-default-db-validate` reports zero discrepancies between the
Parquet output and the source TSV dump.

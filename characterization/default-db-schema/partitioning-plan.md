# Default-DB partitioning plan

Per-table Parquet layout for converting `movesdb20241112`. Drives the
conversion pipeline (`crates/moves-default-db-convert`) and its
round-trip validation. The machine-readable inventory is
[`tables.json`](tables.json); companion: [`README.md`](README.md)
(regeneration, estimator caveats).

**The policy is: one Parquet file per table.** Nothing partitions.

## Why nothing partitions

The original plan (Phase 4 Task 79) sharded the large tables by county,
by year × county, or by model year, so that a run could read only the
geography it named. That was the right answer to the question as it
stood, and the question has since changed.

**The equi-join gate removed the premise.** The naive objection to a
national table is that selecting one county out of 3,232 means paying
for all of them. `join.on` in EarthSciAST `28bda86ac`
(`CONFORMANCE_SPEC.md` §5.5.8) drives enumeration from the *match set*,
so cost tracks matches rather than the product.

Measured on this database's own `IMCoverage`, national, 2,024,874 rows,
joined on `countyID` against a county selection:

| selection | matches | cartesian pairs | wall | peak RSS |
|---|---|---|---|---|
| 1 county | 3,884 | 2.02 M | 0.17 s | 145 MB |
| 10 counties | 79,424 | 20.2 M | 0.26 s | 145 MB |
| 100 counties | 669,186 | 202.5 M | 0.72 s | 237 MB |
| all 405 counties in the table | 2,024,874 | 820.1 M | 2.23 s | 508 MB |

The cartesian product grows 40.5× from the 10-county arm to the
405-county arm; wall clock grows 8.6×, tracking the match set (25.5×,
sub-linear only because of a fixed ~0.15 s scan floor). Marginal cost
is about 1 µs per match. Each arm asserts an exact match count computed
independently, so none can pass on a partial read.

**The join is cheaper than not joining.** For the single-county case
the join arm runs in 0.17 s against 0.39 s for the same selection
expressed as a filter over all 2 M rows: the gate skips rows the scan
has to touch. Reading a national table and joining is not a cost
tolerated for legibility, it is the faster spelling.

One caveat for later: peak RSS tracks the *match set*, which is
materialised (145 MB at 79 K matches, 508 MB at 2.02 M). At county
scale that is free. Joining two multi-million-row MOVES tables on a
low-cardinality key would not be.

**And partitioning was not free — it was the dominant cost.** Every
Parquet file carries a footer: schema, row-group metadata, column
chunk descriptors. Sharded to one file per (county, year), `IMCoverage`
became 21,625 files averaging 17 KB, of which almost all is footer:

| `IMCoverage` | files | on disk | Parquet bytes | rows |
|---|---|---|---|---|
| partitioned by year × county | 21,625 | 378 MB | 262,910,040 | 2,024,874 |
| monolithic | 1 | 196 MB | 204,574,768 | 2,024,874 |

and the same table written by pyarrow with Snappy plus dictionary
encoding is 874,011 bytes. So the 378 MB that this document once cited
as the reason to partition `IMCoverage` was, in order of magnitude:
Parquet footers, then a writer configured for byte-determinism rather
than size, and only then data. The data is under a megabyte.

Across the whole database the layout change alone is:

| | files | directories | on disk | Parquet bytes |
|---|---|---|---|---|
| partitioned (11 tables sharded) | 31,681 | 31,531 | 781 MB | 620,566,894 |
| monolithic | 241 | 0 | 517 MB | 540,886,214 |

The 264 MB saved on disk is footers, 4 KB block slack on 31,681 tiny
files, and 4 KB per directory inode on 31,531 directories. The
conversion takes 15.6 s wall for all 240 tables either way.

## Strategies

The converter still implements all five strategies and
`partition.rs`'s unit tests still exercise them. What changed is the
*policy*, not the capability: a future table that genuinely needs
sharding can be assigned one in `tables.json`. Nothing in the shipped
audit does.

| Strategy | Layout | Currently used by |
|----------|--------|-------------------|
| `monolithic`     | `<table>.parquet` | **237 tables — everything with data** |
| `schema_only`    | `<table>.schema.json` (no Parquet) | 3 tables that ship empty |
| `county`         | `<table>/county=<id>/part.parquet` | none |
| `year`           | `<table>/year=<y>/part.parquet` | none |
| `year_x_county`  | `<table>/year=<y>/county=<id>/part.parquet` | none |
| `model_year`     | `<table>/modelYear=<y>/part.parquet` | none |

## Selection rule

`audit-schema.py::_classify_partition` applies one rule:

1. **`empty` bucket** → `schema_only`. The table ships empty in the
   default DB and the runtime populates it; the pipeline records the
   schema and writes no Parquet body.
2. **everything else** → `monolithic`.

`size_bucket` is still recorded per table. It no longer steers the
layout.

### `tables.json` is edited, not regenerated

`audit-schema.py` parses the canonical MOVES DDL, and the shipped
`tables.json` has been hand-reconciled against the actual
`movesdb20241112` dump since it was first emitted: `Link` was
reclassified out of `schema_only` when Task 81 found 22,610 rows in it,
and the table set differs from a fresh DDL parse by 8 additions and 11
removals (243 tables parsed from the DDL, 240 in the dump). Re-running
the script would throw that reconciliation away. The classifier is kept
in step with the policy so a *future* regeneration lands in the right
place, but the current file was updated in place — only the 17
`partition` blocks that were not already monolithic, a 34-line diff.

`crates/moves-default-db-convert/tests/end_to_end.rs::shipped_audit_is_entirely_monolithic_or_schema_only`
asserts the shipped composition (237 + 3), so a partition strategy
cannot reappear in `tables.json` without a test change and a change to
this document.

## The 17 tables that were partitioned

Six of them ship empty, and the old code wrote **no file at all** for
them — a partitioned table with zero rows produces zero partitions.
Monolithic writes an empty Parquet carrying the schema, so they are
readable now.

| table | old strategy | old files | old Parquet bytes | rows | new bytes |
|---|---|---|---|---|---|
| `IMCoverage` | year_x_county | 21,625 | 262,910,040 | 2,024,874 | 204,574,768 |
| `fuelUsageFraction` | county | 3,230 | 73,094,900 | 1,424,430 | 68,393,510 |
| `nrStateSurrogate` | county | 3,285 | 6,343,150 | 62,821 | 2,514,750 |
| `regionCounty` | county | 3,233 | 16,128,634 | 407,233 | 13,035,902 |
| `nrMonthAllocation` | county (by state) | 53 | 1,815,674 | 46,428 | 1,765,625 |
| `nrEngtechFraction` | model_year | 26 | 632,522 | 9,554 | 593,957 |
| `nrEmissionRate` | model_year | 1 | 4,424,850 | 55,471 | 4,424,850 |
| `nrEvapEmissionRate` | model_year | 1 | 59,520 | 718 | 59,520 |
| `nrCrankcaseEmissionRate` | model_year | 1 | 11,714 | 126 | 11,714 |
| `nrUSMonthAllocation` | county (by state) | 1 | 32,890 | 840 | 32,890 |
| `hotellingActivityDistribution` | county (by zone) | 1 | 3,096 | 36 | 3,096 |
| `AverageTankGasoline` | county | 0 | 0 | 0 | 865 |
| `AverageTankTemperature` | county | 0 | 0 | 0 | 1,050 |
| `ColdSoakInitialHourFraction` | county | 0 | 0 | 0 | 935 |
| `SoakActivityFraction` | county | 0 | 0 | 0 | 1,020 |
| `hotellingHours` | year_x_county | 0 | 0 | 0 | 1,059 |
| `GREETManfAndDisposal` | model_year | 0 | 0 | 0 | 744 |

Note the tail: five of the eleven populated tables were "partitioned"
into a single file, because the partition key had one distinct value in
the default DB. The classifier had assigned them a strategy on an
estimated upper-bound row count that the real data never approached.

## Schema-only — populated at runtime

`SHO`, `SourceHours`, and `Starts`: the activity tables the migration
plan singled out as needing partitioning, but only in the *execution*
database. In the default DB they are empty shells. The pipeline records
the schema (column names, types, primary key) as a `*.schema.json`
sidecar so downstream Rust can validate inserts, but writes no Parquet.

`Link` was on this list until Task 81 measured 22,610 rows in the
shipped dump; it is monolithic.

## Row counts, measured

The estimator in `audit-schema.py` multiplies primary-key cardinalities
and attenuates by a sparsity prior. It is an upper bound and it is
loose — that looseness is what put five single-file tables into a
partitioned strategy. The conversion measures the truth: **8,436,056
rows across 240 tables**, the largest being `IMCoverage` (2,024,874),
`EmissionRateByAge` (1,590,830), and `fuelUsageFraction` (1,424,430).
Nothing is within two orders of magnitude of the 50M-row threshold the
old "large-monolithic re-review queue" set for revisiting a decision,
so that queue is closed.

## `_tsv/` — the intermediate dump, and where it should live

`convert-default-db.sh` stage 1 runs MariaDB inside the canonical SIF and
writes `<Table>.tsv` + `<Table>.schema.tsv` for every table into
`${OUTPUT_DIR}/_tsv` — *inside* the versioned tree. Stage 2 reads it.
It is not part of the database; it is the conversion's input, and it is
also the only thing that makes a shipped database re-validatable, since
`moves-default-db-validate` cross-checks Parquet against the TSV rather
than against MariaDB.

It is inside the release asset. `default-db-movesdb20241112.tar.gz` is
64.5 MB, and 38.3 MB of that — 59% — is `_tsv/` (337 MB raw, 481 files).
Every consumer then excludes it on the way out: `ci.yml` and
`package-default-db.yml` each carry their own `--exclude`/`--exclude`
of `_tsv/`, and `default-db-gate.yml` a third.

Recommendation, not yet done: keep shipping the TSVs — dropping them
would make the released database unverifiable by its own validator —
but stop nesting them inside the thing they are not part of. Write them
to a sibling (`${OUTPUT_ROOT}/_tsv-${DB_VERSION}/`) and package them as
a second asset. Then the database asset is just the database, three
workflows stop re-implementing the same exclusion, and the asset drops
to 14.6 MB gzipped (monolithic, measured).

The monolithic tree produced for Phase 7 has no `_tsv/` in it: the
conversion was run with `--tsv-dir` pointing at the existing dump rather
than re-dumping into the output. That is the layout this section
recommends, arrived at by accident.

## Open question for the reader phase: writer settings, not layout

The Parquet writer is pinned uncompressed, no dictionary, no
statistics, `PARQUET_1_0`, for a byte-determinism contract (see
`parquet_writer.rs`) — two conversion runs produce byte-identical files
and therefore identical manifest hashes, which is verified.

That pinning, not the layout, is now what the database's size is made
of. Re-encoding the shipped output with Snappy plus dictionary
encoding, content unchanged:

| | Parquet bytes | |
|---|---|---|
| as shipped | 540,886,214 | 515.8 MiB |
| Snappy + dictionary | 16,597,230 | 15.8 MiB — 32.6× smaller |
| Zstd + dictionary | 12,614,911 | 12.0 MiB — 42.9× smaller |

and reading is *faster* compressed, not slower: `IMCoverage` reads back
in 0.058 s at 0.83 MiB versus 0.102 s at 195 MiB (pyarrow, best of 3,
warm). The MOVES tables are overwhelmingly small repeated integers,
which is the case dictionary encoding is for.

This is left as it is. Changing it trades away the byte-determinism
contract for a compressor's version-stability, and it needs a check
that the reader decompresses Snappy — neither is a layout question.
Recorded here because it is the next 30× on the table and the layout
work is what surfaced it.

# moves-importer-validation

The MOVES **importer validation suite**.

This crate ports five MOVES input-database importers to Rust:

| Crate | Importer |
|-------------------------|-------------------------------|
| `moves-importer-county` | County Database (CDB) |
| `moves-importer-pdb` | Project Database (PDB) |
| `moves-nonroad-import` | Nonroad input database |
| `moves-avft` | Alternative Vehicle Fuel Tech |
| `moves-import-lev` | LEV / NLEV alternative rates |

This crate closes the loop. It runs those importers against representative
user source files and compares the resulting Parquet against the tables
**canonical MOVES** loads into MariaDB for the same inputs. A difference is
a candidate importer bug.

## How the comparison works

Canonical MOVES loads a user CDB/PDB into a MariaDB scratch database. The
capture pipeline (`moves-fixture-capture`) dumps every such table
into a snapshot as `db__<database>__<table>` — see
`characterization/snapshots/README.md`.

The harness in `src/lib.rs`:

1. Runs a Rust importer on the user source files.
2. Normalizes the importer's Parquet output into a `moves_snapshot::Table`
 with [`parquet_to_table`] — the *same* normalization the canonical
 snapshot applies (rows sorted by the natural key, floats rounded to a
 fixed-decimal string).
3. Diffs the normalized importer table against the canonical `db__…`
 table with [`compare_importer_output`], which wraps
 `moves_snapshot::diff_snapshots` and classifies the result.

[`compare_importer_output`] distinguishes genuine importer bugs (changed
cells, added/removed rows, stray columns, type mismatches) from
differences expected by design — a Rust importer legitimately omits
columns canonical MOVES synthesizes in its SQL load script (for example
`salesGrowthFactor` and `migrationRate` on `SourceTypeYear`). Omitted
columns are surfaced but not counted as bugs.

## Two modes

The `tests/` run in two modes:

* **Always (CI).** Run each importer against the committed fixtures under
 `fixtures/`, normalize the output, and verify it is a well-formed,
 snapshot-stable table (`assert_snapshot_stable`). The harness itself is
 unit-tested in `src/lib.rs` with synthetic canonical data, so the
 comparison logic is fully exercised in CI.
* **When canonical snapshots are present.** Additionally diff importer
 output against the canonical `db__…` tables and fail on genuine drift.

The canonical-MOVES snapshots are produced on an HPC compute node
(Apptainer + the patched MOVES SIF) and are **not committed** to the
repository. When a snapshot is absent the comparison reports a skip
rather than failing — the same way `.github/workflows/fixture-suite-weekly.yml`
skips fixtures without a committed baseline.

## Fixtures

`fixtures/` holds representative importer source files — small but
schema-complete inputs anchored on Washtenaw County, Michigan (county
`26161`, zone `261610`):

```
fixtures/
├── cdb/ County importer inputs (SourceTypeYear, ZoneRoadType,
│ SourceTypeAgeDistribution, Zone)
├── pdb/ Project importer inputs (Link, linkSourceTypeHour,
│ driveScheduleSecondLink, offNetworkLink, OpModeDistribution)
├── nonroad/ Nonroad importer inputs (nrbaseyearequippopulation,
│ nrengtechfraction)
├── lev/ LEV alternative-rate input (EmissionRateByAgeLEV)
└── avft/ AVFT input (avft)
```

These same files are the input to the canonical-MOVES comparison: the
operator runs canonical MOVES with them as the scale-input database, so
the importer and canonical MOVES see identical inputs.

## Producing the canonical baseline (operator procedure)

To enable the gated comparison for the CDB importers:

1. On an HPC compute node with the built `moves-fixture.sif`, run
 canonical MOVES for a County-scale RunSpec whose County data manager
 input database is loaded from `fixtures/cdb/`.
2. Capture the run with `moves-fixture-capture`, writing the snapshot to
 `characterization/snapshots/importer-validation-cdb/`.
3. Re-run `cargo test -p moves-importer-validation`. The `cdb` tests now
 diff importer output against the captured `db__…__sourcetypeyear`,
 `db__…__zoneroadtype`, etc. tables.

The PDB importers follow the same procedure with a Project-scale RunSpec
and the snapshot directory `characterization/snapshots/importer-validation-pdb/`.

`load_canonical_snapshot` locates the snapshot by name under
`characterization/snapshots/`; `find_canonical_table` matches a MOVES
table to its `db__<database>__<table>` id regardless of the scratch
database name.

## Running

```sh
cargo test -p moves-importer-validation
```

With no canonical snapshots staged, every importer is still exercised and
its output verified snapshot-comparable; the canonical diffs print a
`[skip]` line. With snapshots staged, the canonical diffs run and any
drift fails the suite.

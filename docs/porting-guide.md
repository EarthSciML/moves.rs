# Porting guide — moves.rs for MOVES users

This guide is for users who already have working MOVES workflows and want to
evaluate or adopt the Rust port. It covers:

- [Regulatory caveat](#regulatory-caveat) — read this first
- [What the port supports today](#what-the-port-supports-today)
- [What is not yet supported](#what-is-not-yet-supported)
- [Porting your RunSpec](#porting-your-runspec)
- [What differs from canonical MOVES](#what-differs-from-canonical-moves)
- [How to compare outputs](#how-to-compare-outputs)

---

## Regulatory caveat

> **This port is not approved for regulatory use.** It must not be used for
> SIP submissions, transportation conformity determinations, NEPA analyses,
> NAAQS-related filings, or any other regulatory purpose.

The Rust port targets the **research and policy community**, not EPA regulatory
workflows. Until EPA formally approves the port and it passes EPA's own
validation suite, any emission inventory produced by `moves.rs` carries no
regulatory standing. Researchers who need regulatory-grade output must continue
to use the official MOVES Java application.

This caveat applies even where the port produces numerically identical results
to canonical MOVES. Regulatory validity is a formal process, not just a
question of numerical fidelity.

---

## What the port supports today

The port covers the following canonical-MOVES capabilities:

### RunSpec formats

Both the canonical XML format (`.xml` / `.mrs`) and the Rust-port TOML format
(`.toml`) are fully supported. A RunSpec that works in canonical MOVES can be
passed directly to `moves run --runspec <path>` without modification. See
[`docs/runspec-toml.md`](runspec-toml.md) for the TOML alternative.

### Model paths

| Model path | Status |
|-----------|--------|
| Onroad (Default Scale / Macroscale) | Supported — all ~70 calculators implemented |
| NONROAD | Supported — full Fortran→Rust port |
| Mixed onroad + NONROAD | Supported |

### Emission processes (onroad)

All MOVES emission processes are ported: running exhaust, start exhaust,
extended idle, brakewear, tirewear, evap permeation, evap fuel vapor venting,
evap fuel leaks, refueling displacement, refueling spillage, crankcase variants,
auxiliary power, and HC/NOx speciation chains.

### Control strategies

All four control strategies are implemented and available via RunSpec
`<internalcontrolstrategy>` blocks. See
[`docs/control-strategies.md`](control-strategies.md) for input file formats
and worked examples.

| Strategy | Canonical name | Status |
|---------|---------------|--------|
| AVFT | `AVFTControlStrategy` | Supported |
| Rate of Progress | `RateOfProgressControlStrategy` | Supported |
| On-Road Retrofit | `OnRoadRetrofitStrategy` | Supported |
| NONROAD Retrofit | `NonRoadRetrofitStrategy` | Supported |

### Input database import

County-scale input CSVs can be imported with:

```sh
moves import-cdb --input <csv-dir> --output <parquet-dir> [--default-db <default-db-dir>]
```

The importer validates column types, value ranges, and (when `--default-db` is
supplied) foreign-key constraints. See `moves import-cdb --help` for details.

### RunSpec conversion

Convert an existing canonical-MOVES RunSpec from XML to TOML (or back):

```sh
moves convert-runspec --input my_run.xml # writes my_run.toml
moves convert-runspec --input my_run.toml # writes my_run.xml
moves convert-runspec --input my_run.xml --output other_name.toml
```

---

## What is not yet supported

### Emission output data (v0.1 state)

**In the current release, `moves run` produces the `MOVESRun.parquet` metadata
file and plans the full calculator graph, but calculator `execute()` methods
return empty output.** The `MOVESOutput/` and `MOVESActivityOutput/` partitions
are created but contain no emission rows.

This is the expected v0.1 state. The data plane that materialises per-row
default-database lookups into `CalculatorContext` is not yet wired. You will see output like:

```
[moves run] my_run.xml
 calculator graph : 44 module(s) planned across 1 chunk(s)
 executed : 0
 not yet ported : 44 module(s)
 ...
```

The "not yet ported" label here is misleading — the calculators are
implemented, but without the data plane they produce no output. Once the data
plane is wired, the executed count will equal the planned count and emission
rows will appear in the output Parquet.

**Practical implication:** use the port today to validate that your RunSpec
parses correctly, that the calculator graph is planned as expected, and that
your control-strategy inputs load without error. Do not expect emission numbers
until the data plane is live.

### County Scale (CDB) and Project Scale (PDB)

County-Scale and Project-Scale runs require user-supplied County Databases
(CDB) or Project Databases (PDB). The `import-cdb` command is available and
produces validated Parquet. However, the data-plane wiring to feed CDB-sourced
Parquet into the calculator context is not yet complete.

The three scale fixtures (`scale-county.xml`, `scale-project.xml`,
`scale-rates.xml`) are excluded from the standard regression suite for this
reason. They will be added once the CDB/PDB data plane is in place.

### Rates mode

`<modelscale value="Rates"/>` RunSpecs are parsed without error, but the
rates-mode generator chain that produces per-link emission-rate output rather
than inventory output is not yet wired into the execution path. Rates-mode
RunSpecs will plan the graph but produce no output rows.

### Uncertainty / Monte Carlo simulation

`<uncertaintyparameters>` in the RunSpec is parsed and round-trips correctly
through TOML, but the Monte Carlo execution path
(`ExecutionRunSpec.estimateUncertainty()` in the Java original) is not
implemented. Uncertainty runs are out of scope for v0.1.

### Distributed execution

Multi-machine worker dispatch (`amazon/`, multi-host worker protocol) is out
of scope. `moves.rs` is a single-process model; all parallelism is bounded by
`--max-parallel-chunks` on one machine.

### MOVES GUI

The Swing-based GUI is not ported. All interaction is via the `moves` CLI.

---

## Porting your RunSpec

### Step 1 — Use your existing XML RunSpec as-is

`moves run` accepts canonical MOVES `.xml` and `.mrs` files directly. No
format conversion is required:

```sh
moves run --runspec SampleRunSpec.xml --output ./port-output
```

The parser is tested against all 36 characterization fixtures including the
EPA-standard `SampleRunSpec.xml`. If your RunSpec was authored for MOVES5 or
later, it will parse without modification.

### Step 2 — (optional) Convert to TOML

The TOML format is easier to edit by hand and supports comments. Convert once:

```sh
moves convert-runspec --input my_run.xml # -> my_run.toml
```

Then edit `my_run.toml` as needed. See [`docs/runspec-toml.md`](runspec-toml.md)
for the full field-by-field mapping.

### Step 3 — Prepare input databases

**Default-Scale runs with the default database:** no action needed. The port
ships with the MOVES default database pre-converted to Parquet (based on
MOVES commit `25dc6c8`, MOVES5.0.1).

**County-Scale runs (CDB):** convert your County Database CSVs:

```sh
moves import-cdb \
 --input my_cdb_csvs/ \
 --output my_cdb_parquet/ \
 --default-db /path/to/default-db-parquet/
```

Fix any validation errors the importer reports before proceeding.
`--default-db` is optional but enables foreign-key checks; without it,
FK mismatches are warnings only.

**Project-Scale runs (PDB):** the `moves-importer-pdb` crate exists but is
not yet exposed as a CLI subcommand. Project-Scale runs are not supported
at the CLI level in v0.1.

### Step 4 — Run

```sh
moves run \
 --runspec my_run.toml \
 --output ./my-output \
 --max-parallel-chunks 4 # optional; 0 = use all logical CPUs
```

On completion the output directory contains:

```
my-output/
├── MOVESRun.parquet # run metadata (always written)
├── MOVESOutput/ # emission rows (empty until data plane is wired)
└── MOVESActivityOutput/ # activity rows (empty until data plane is wired)
```

---

## What differs from canonical MOVES

### No JDK, no MariaDB

Canonical MOVES requires a Java runtime (JDK 11+) and a MariaDB 10.x server.
`moves.rs` is a single static binary with no runtime dependencies.

### Output format: Parquet, not MariaDB

Canonical MOVES writes results into MariaDB output-database tables.
`moves.rs` writes Parquet files. The column names and types follow the
canonical schema exactly (see [`docs/output-schema.md`](output-schema.md));
two additive columns are appended:

| Column | Table | Description |
|--------|-------|-------------|
| `runHash` | all three tables | SHA-256 of canonical run inputs; joinable across runs without consulting `MOVESRun` |
| `calculatorVersion` | `MOVESRun` only | `moves.rs` build identifier |

Downstream tools that query MariaDB by column name will work unchanged once
pointed at the Parquet files. See [`docs/downstream-tools.md`](downstream-tools.md)
for pandas, R, Polars, DuckDB, and Spark loader recipes.

### NONROAD: f64 throughout (Fortran used real\*4)

The Fortran NONROAD source uses single-precision (`real*4`) arithmetic
extensively. The Rust port uses `f64` throughout. Results are typically
closer to the true mathematical value but will differ from canonical NONROAD
at the level of single-precision rounding — see
[`docs/known-divergences.md §4.2`](known-divergences.md) for the expected
tolerance budget.

### Output row order

Canonical MOVES accumulates rows across calculator threads in
non-deterministic order. `moves.rs` produces a deterministic output row
order (calculators deliver rows in DAG-topological order, partitioned by
`(yearID, monthID)`). Queries that `ORDER BY` the natural key will see the
same results; raw row-for-row comparison against canonical output may show
reordered rows even when the values are identical.

### `masterVersion` / `masterComputerID` / `masterIDNumber` metadata

These `MOVESRun.parquet` columns hold Java-class-name strings in canonical
MOVES (e.g. `gov.epa.otaq.moves.master.runspec.RunSpec`). `moves.rs` writes
`moves.rs/<version>` for `masterVersion` and leaves `masterComputerID` /
`masterIDNumber` as empty strings. Downstream code that hard-codes the Java
class-name format should be updated to accept either form.

### No intermediate database tables

Canonical MOVES writes intermediate `workerFolder/` tables to MariaDB during
execution. The Rust port keeps all intermediate data in memory (Polars
`DataFrame`). There is no per-calculator database artefact to inspect.

### Performance

`moves.rs` targets a 10–50× wall-time improvement over canonical MOVES for
County-Scale and Project-Scale workloads. Default-Scale runs benefit
proportionally. Memory usage scales with `--max-parallel-chunks`; if peak RSS
is too high, lower the parallelism setting.

---

## How to compare outputs

Once the default-database data-plane wiring is live, you can compare `moves.rs`
output against canonical MOVES snapshots with the `moves-snapshot` tool.

### Run canonical MOVES and capture a snapshot

```sh
# On an HPC node with Apptainer (see characterization/apptainer/README.md):
characterization/run-all-fixtures.sh --fakeroot --keep-going
```

This populates `characterization/snapshots/<fixture-name>/` in the
`manifest.json` + `tables/*.parquet` snapshot format.

### Run the port against the same RunSpec

```sh
moves run \
 --runspec characterization/fixtures/sample-runspec.xml \
 --output /tmp/port-output/sample-runspec/
```

### Diff the two

```sh
# Produce a JSON diff within the tolerance budget:
target/release/moves-snapshot diff \
 characterization/snapshots/sample-runspec/ \
 /tmp/port-output/sample-runspec/ \
 --tolerance characterization/tolerance.toml \
 --format json \
 | jq '.diff.table_changes[] | {table, cells: (.row_diffs | length)}'
```

### Enable the regression gate

Set `REGRESSION_SNAPSHOTS_DIR` to run the diff as part of `cargo test`:

```sh
REGRESSION_SNAPSHOTS_DIR=characterization/snapshots \
 cargo test --test full_suite_regression -- --nocapture
```

Divergences within the budget in `characterization/tolerance.toml` are
reported but non-failing. Divergences beyond the budget fail the test.

### Expected divergence categories

Four categories of divergence are expected and documented in
[`docs/known-divergences.md`](known-divergences.md):

| Category | Within tolerance? | Notes |
|----------|-------------------|-------|
| Row ordering in tied aggregates | Yes (after natural-key normalisation) | See §4.1 |
| Sub-tolerance numerical drift (float summation order) | Yes | See §4.2 |
| Metadata column format differences (`masterVersion`, etc.) | Yes (excluded from diff) | See §4.3 |
| Real port bugs (large, reproducible divergences) | No — fix these | See §4.4 |

If you observe a divergence not in one of these categories, please file an
issue.

---

## Quick-reference: canonical MOVES → moves.rs

| Canonical MOVES | moves.rs equivalent |
|----------------|---------------------|
| MOVES GUI → File → Run | `moves run --runspec <path>` |
| Export RunSpec XML | Use existing `.xml` or `.mrs` directly |
| Author RunSpec by hand | Write TOML (see `docs/runspec-toml.md`) |
| Import County Database | `moves import-cdb --input <csv-dir> --output <parquet-dir>` |
| XML ↔ TOML conversion | `moves convert-runspec --input <path>` |
| Output: MariaDB tables | Output: Parquet files under `<output>/MOVESOutput/` |
| `MOVESOutput.emissionQuant` | Same column name, same semantics |
| Results in database browser | Load Parquet with pandas / DuckDB / Polars / R / Spark |
| `moves --help` | `moves --help` / `moves <subcommand> --help` |

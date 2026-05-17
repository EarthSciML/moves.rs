# generator-validation — generator integration-validation gate

This directory documents the Phase 3 Task 44 (`mo-zstr`) generator
integration-validation gate: the harness that runs the Phase 0 onroad
fixtures through the Rust generators (Tasks 29–43, calculators still
stubbed) and diffs each generator's output against the canonical-MOVES
intermediate captures, within an explicit tolerance budget.

The gate's **code** lives in the `moves-calculators` crate's tests:

```
crates/moves-calculators/tests/
├── generator_integration.rs        # the harness — runs under `cargo test`
└── generator_validation/
    ├── mod.rs                      # harness overview + the snapshots-dir hook
    ├── fixtures.rs                 # the 23 Phase 0 onroad RunSpec fixtures
    ├── generators.rs               # the 16 Phase 3 generator implementations
    ├── coverage.rs                 # the fixture × generator coverage matrix
    ├── adapter.rs                  # generator compute-core output → snapshot table
    └── compare.rs                  # diff produced tables vs canonical snapshots
```

It runs on every `cargo test` (the fast `.github/workflows/ci.yml`
gate) — no Apptainer, no MOVES runtime required.

## Scope: onroad only

Tasks 29–43 port the MOVES **onroad** generators. Phase 0 ships 33
fixtures; the ten `nr-*.xml` NONROAD fixtures drive a separate
calculation path (the `moves-nonroad` Fortran port) and never
instantiate these generators — they are covered by the Task 115
NONROAD numerical-fidelity gate (`../nonroad-fidelity/`). This gate
therefore scopes to the **23 onroad fixtures** and the **16
generators** (Tasks 29–43, counting Task 35's paired
`MesoscaleLookup…` generators separately).

## The comparison

The canonical-MOVES captures are `moves_snapshot`-format snapshots
(Phase 0 Task 4) — so the gate diffs with the same engine
(`crates/moves-snapshot`) Phase 0's own regression detection uses,
rather than reinventing one. For each exercised `(fixture, generator)`
coverage cell, the harness:

1. runs the generator's numeric compute core for the fixture;
2. shapes its output rows into a `moves_snapshot` table (`adapter`);
3. resolves the matching table in the fixture's canonical snapshot;
4. diffs the two under the tolerance budget (`compare`).

## Tolerance budget

`tolerance.toml` holds the per-(table, column) absolute tolerances.
The default is a strict `1e-9`; a divergence past it is a port bug to
fix before Phase 3's calculators proceed.

One *expected* divergence is already recorded — `MeteorologyGenerator`
routes `specificHumidity` and `molWaterFraction` through
`fahrenheit_to_kelvin`, whose exact `5.0/9.0` ratio differs from the
canonical SQL's MariaDB-evaluated `(5/9)` literal. The port source
(`crates/moves-calculators/src/generators/meteorology.rs`) explicitly
defers that call here. Those two columns are widened; `heatIndex`,
which does not pass through the conversion, keeps the strict default.
The widened values are pre-calibration placeholders — see the comments
in `tolerance.toml`.

## What runs today

- **Machinery validation** — the fixture-catalogue parse, the
  generator catalogue, the coverage-matrix derivation, and the
  snapshot-diff engine are exercised end to end on the real fixtures
  and on synthetic snapshots (the co-located module tests).
- **Live-port exercise** — the harness calls the real
  `MeteorologyGenerator` compute core (`compute_zone_month_hour`),
  routes its output through `adapter` into a `moves_snapshot` table,
  and confirms `compare` composes with genuine generator output.
- **Fixture + generator catalogues** — all 23 onroad fixtures are
  confirmed present and well-formed; all 16 generators are confirmed
  registered with unique names.
- **Coverage matrix** — confirmed to reach every fixture. Print it
  with `cargo test -p moves-calculators --test generator_integration
  -- --nocapture` (the `harness_status` test renders it).

## What is gated, and how to activate it

The end-to-end canonical-capture diff needs two inputs the repository
does not hold yet:

1. **The canonical-MOVES intermediate captures.** Phase 0 ships the
   fixture RunSpecs but the snapshots are "pending compute-node run"
   (`../fixtures/README.md`). Populate `../snapshots/<fixture>/` per
   `../snapshots/README.md` § "Producing the full Phase 0 fixture
   suite".
2. **The Rust generators' per-fixture output.** Every generator's
   `Generator::execute` returns `CalculatorOutput::empty()` today
   because `CalculatorContext` exposes no row storage until the
   Task 50 data plane (`DataFrameStore`) lands. The generators'
   *numeric compute cores* are complete and callable — that is what
   `adapter` and the live-port exercise drive — but the
   materialisation that feeds them per-fixture inputs and collects
   their output rows does not exist.

When the snapshots are populated, the harness picks them up at
`../snapshots/` automatically. Point it at an out-of-repo capture run
instead with:

```sh
GENERATOR_VALIDATION_SNAPSHOTS=/path/to/snapshots \
    cargo test -p moves-calculators --test generator_integration
```

With snapshots present the harness loads each one and validates the
generator output it can produce. The full per-fixture diff activates
with no further harness change once the Task 50 data plane lands the
generator-output side — `adapter` is the contract that wiring builds
to, and `compare::compare_table` is the diff.

This split mirrors the Task 115 NONROAD fidelity gate
(`../nonroad-fidelity/`): Task 44 builds the *gate*; Phase 0's
compute-node run and Task 50's data plane supply its two inputs.

## Relationship to other tasks

- **Phase 0 Tasks 4–6** supply the fixture RunSpecs and (pending) the
  canonical snapshots this gate diffs against.
- **Tasks 29–43** are the 16 generators under validation.
- **Task 50** (`DataFrameStore`) lands the data plane that lets the
  generators run per-fixture rather than only through their compute
  cores.
- **Tasks 45–88** are the Phase 3 calculators that consume generator
  output; this gate is the checkpoint that the generators are sound
  before that work proceeds.

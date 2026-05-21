# calculator-validation — calculator integration-validation gate

This directory documents the Phase 3 Task 73 (`mo-fvuf`) calculator
integration-validation gate: the harness that runs the Phase 0 onroad
fixtures through the Rust calculators (Tasks 45–72) and diffs each
calculator's output against the canonical-MOVES captures, within an
explicit tolerance budget.

The gate's **code** lives in the `moves-calculators` crate's tests:

```
crates/moves-calculators/tests/
├── calculator_integration.rs       # the harness — runs under `cargo test`
└── calculator_validation/
    ├── mod.rs                      # harness overview + the snapshots-dir hook
    ├── fixtures.rs                 # the 23 Phase 0 onroad RunSpec fixtures
    ├── calculators.rs              # the 38 Phase 3 calculator implementations
    ├── coverage.rs                 # the fixture × calculator coverage matrix
    └── compare.rs                  # diff produced tables vs canonical snapshots
```

It runs on every `cargo test` (the fast `.github/workflows/ci.yml`
gate) — no Apptainer, no MOVES runtime required.

## Scope: onroad hot-path

Tasks 45–72 port the MOVES onroad **emission calculators** for the
hot-path processes. Phase 0 ships 33 fixtures; the ten `nr-*.xml`
NONROAD fixtures drive a separate calculation path (the `moves-nonroad`
Fortran port) and are covered by the Task 115 NONROAD numerical-fidelity
gate (`../nonroad-fidelity/`). This gate scopes to the **23 onroad
fixtures** and the **38 calculators** from Tasks 45–72.

## The coverage matrix

The fixture × calculator coverage matrix is the join of each fixture's
`(pollutant_id, process_id)` pairs (from its RunSpec
`<pollutantprocessassociations>`) against each calculator's
`registrations()` (the `(pollutant, process)` pairs it declares in
the chain DAG). A calculator is exercised by a fixture when they share
at least one `(pollutant_id, process_id)` pair.

Chained-only calculators — those with empty `registrations()` because
they are invoked by their chain parent rather than by the master-loop
scheduler directly — appear as not-directly-exercised in the matrix.
The matrix records their actual activation path (via chain parent)
in the cell annotation.

## The comparison

The canonical-MOVES captures are `moves_snapshot`-format snapshots
(Phase 0 Task 4) — so the gate diffs with the same engine
(`crates/moves-snapshot`) Phase 0's own regression detection uses.
For each exercised `(fixture, calculator)` coverage cell, the harness
compares the Rust port's output table against the canonical snapshot
under the tolerance budget.

## Tolerance budget

`tolerance.toml` holds the per-(table, column) absolute tolerances.
The default is `0.0` (byte-identical within fixed-decimal
canonicalization). Known numerical artifacts — port-vs-canonical
floating-point divergences that are intentional, not bugs — are
widened here with a comment explaining the source.

## What runs today

- **Machinery validation** — the fixture-catalogue parse, the
  calculator catalogue, the coverage-matrix derivation, and the
  snapshot-diff engine are exercised end to end on real fixtures and
  synthetic snapshots (the co-located module tests).
- **Fixture + calculator catalogues** — all 23 onroad fixtures are
  confirmed present and well-formed; all 38 calculators are confirmed
  registered with unique names.
- **Coverage matrix** — confirmed to reach every fixture. Print it
  with `cargo test -p moves-calculators --test calculator_integration
  -- --nocapture` (the `harness_status` test renders it).

## What is gated, and how to activate it

The end-to-end canonical-capture diff needs two inputs the repository
does not hold yet:

1. **The canonical-MOVES captures.** Phase 0 ships the fixture
   RunSpecs but the snapshots are "pending compute-node run"
   (`../fixtures/README.md`). Populate `../snapshots/<fixture>/` per
   `../snapshots/README.md` § "Producing the full Phase 0 fixture
   suite".
2. **The Rust calculators' per-fixture output.** Every calculator's
   `Calculator::execute` returns `CalculatorOutput::empty()` today
   because `CalculatorContext` exposes no row storage until the
   data-plane lands. The calculators' *numeric compute cores* are
   complete and callable — the unit tests (`tests/baseratecalculator.rs`
   etc.) exercise them — but the materialisation that feeds them
   per-fixture inputs and collects their output rows does not exist yet.

When the snapshots are populated, the harness picks them up at
`../snapshots/` automatically. Point it at an out-of-repo capture run
instead with:

```sh
CALCULATOR_VALIDATION_SNAPSHOTS=/path/to/snapshots \
    cargo test -p moves-calculators --test calculator_integration
```

## Relationship to other tasks

- **Phase 0 Tasks 4–6** supply the fixture RunSpecs and (pending) the
  canonical snapshots this gate diffs against.
- **Tasks 45–72** are the 38 calculators under validation.
- **Task 73** (this bead, `mo-fvuf`) builds the gate; the data plane
  and Phase 0 compute-node run supply its two inputs.
- **Task 74** extends coverage to the full fixture suite including
  non-hot-path calculators.

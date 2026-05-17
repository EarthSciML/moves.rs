# nonroad-fidelity — NONROAD numerical-fidelity validation gate

This directory documents the Phase 5 Task 115 (`mo-065ko`)
numerical-fidelity gate: the harness that runs the Phase 0 NONROAD
fixtures through the Rust port and diffs the result against the
locally-fixed gfortran NONROAD reference, within a fixed tolerance
budget.

The gate's **code** lives in the `moves-nonroad` crate's tests:

```
crates/moves-nonroad/tests/
├── nonroad_fidelity.rs        # the harness — runs under `cargo test`
└── fidelity/
    ├── mod.rs                 # harness overview + the reference-dir env hook
    ├── reference.rs           # dbgemit capture-TSV parser
    ├── tolerance.rs           # the 1e-9 / 1e-12 / exact tolerance policy
    ├── divergence.rs          # diff engine + DivergenceReport (Task 116 input)
    ├── fixtures.rs            # the ten Phase 0 nr-*.xml fixtures
    └── adapter.rs             # moves-nonroad output types → reference records
```

It runs on every `cargo test` (the fast `.github/workflows/ci.yml`
gate) — no Apptainer, no MOVES runtime required.

## The four instrumented phases

The fidelity baseline is the intermediate state captured by the four
`dbgemit` instrumentation patches in `../nonroad-build/`. Each maps
to one `moves-nonroad` module and a fixed set of emitted variables:

| Phase     | Fortran     | Rust module           | Emitted labels                          |
|-----------|-------------|-----------------------|-----------------------------------------|
| `GETPOP`  | `getpop.f`  | `population::pop`     | `popeqp` `avghpc` `usehrs` `ipopyr`      |
| `AGEDIST` | `agedist.f` | `population::agedist` | `mdyrfrc` `baspop`                       |
| `GRWFAC`  | `grwfac.f`  | `population::growth`  | `factor` `baseyearind` `growthyearind`   |
| `CLCEMS`  | `clcems.f`  | `emissions::exhaust`  | `emsday` `emsbmy` `pop` `mfrac` `afac` `dage` |

## Tolerance budget

The bead fixes three rules (`tests/fidelity/tolerance.rs`):

| Class            | Rule              | Applies to                          |
|------------------|-------------------|-------------------------------------|
| Energy quantity  | `1e-9` relative   | emissions, populations, factors     |
| Count / index    | `1e-12` absolute  | year indices (`ipopyr`)             |
| SCC/eqp/year key | exact             | the `key=val` context that pairs records |

NONROAD's reals are Fortran `real*4`, whose epsilon (≈`1.19e-7`) is
*larger* than the `1e-9` relative bound — so the gate reports any
energy quantity that is not bit-identical to the reference. That is
deliberate: Task 115 surfaces every divergence; Task 116 (`mo-490cm`)
triages each and, where a divergence is a tolerable artifact, widens
the budget for that pollutant/equipment class. The tolerance
constants in `tolerance.rs` are the knobs Task 116 turns.

## What runs today

- **Machinery validation** — the parser, tolerance rules, and
  divergence engine are exercised end to end on synthetic captures.
- **Live-port exercise** — the harness calls real `moves-nonroad`
  functions (`age_distribution`, `growth_factor`), routes their
  output through the adapter and divergence engine, and confirms the
  machinery composes with genuine port output.
- **Fixture catalogue** — all ten Phase 0 `nr-*.xml` NONROAD
  fixtures are confirmed present and well-formed.

## What is gated, and how to activate it

The end-to-end gfortran-reference diff needs two inputs the
repository does not hold yet:

1. **A captured gfortran baseline per fixture.** Build the
   instrumented NONROAD (`../nonroad-build/`), then run it inside the
   canonical-MOVES Apptainer SIF with `NRDBG_FILE` set, once per
   fixture. Collect the resulting TSVs into one directory, named
   `<fixture>.tsv` (e.g. `nr-construction-state.tsv`).
2. **The Rust port's own intermediate-state capture**, produced once
   Task 117 wires up `run_simulation` with port-side instrumentation
   that emits the same labels. `tests/fidelity/adapter.rs` is the
   contract that instrumentation builds to.

Point the harness at the baseline directory with:

```sh
NONROAD_FIDELITY_REFERENCE=/path/to/baselines \
    cargo test -p moves-nonroad --test nonroad_fidelity
```

When the variable is set, the harness loads and structurally
validates every `<fixture>.tsv` it finds. The reference-vs-port diff
itself (`divergence::compare_runs`) activates with no further harness
change once Task 117 lands the port side.

## Handoff to Task 116

`divergence::DivergenceReport` is the artifact Task 116 (`mo-490cm`,
NONROAD numerical-divergence triage) consumes. It records every
out-of-tolerance value with its phase, context, label, expected and
actual values, and absolute/relative differences; `to_json()`
serialises it for a CI artifact, and its `Display` form is the
human-readable triage view.

## Known divergences

Empty for now — Task 116 populates this table as triage finds
pollutant/equipment classes that fall outside the budget.

| Phase | Label | Context | Tolerance | Reason |
|-------|-------|---------|-----------|--------|
| ...   | ...   | ...     | ...       | ...    |

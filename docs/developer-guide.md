# Developer Guide — moves.rs

This guide is for contributors who want to add or modify components in the
Rust MOVES port. It covers the architecture, the calculator and generator
extension points, the default-DB data layer, control strategies, and the
NONROAD module.

---

## Architecture overview

### Workspace layout

`moves.rs` is a Cargo workspace. The crates divide into three tiers:

| Layer | Crates | Role |
|-------|--------|------|
| Data types | `moves-data`, `moves-calculator-info`, `moves-runspec` | Shared domain types: pollutants, processes, RunSpec model, calculator DAG metadata |
| Framework | `moves-framework` | Engine, MasterLoop, registry, executor, control-strategy lifecycle |
| Implementations | `moves-calculators`, `moves-avft`, `moves-onroad-retrofit`, `moves-nonroad`, `moves-importer*` | Calculator/generator bodies, control strategies, importers |
| CLI | `moves-cli` | The `moves` binary — `run`, `import` subcommands |

No crate in the "Implementations" layer may depend on another implementation
crate; they share only the framework and data-type layers.

### The execution pipeline

`moves-cli run` invokes [`MOVESEngine::run`] (in `moves-framework/src/execution/engine.rs`):

```
RunSpec (XML or TOML)
        │
        ▼
 ExecutionRunSpec    ← filters selections from RunSpec (target processes, timespan, geography)
        │
        ▼
 CalculatorRegistry  ← filters chain DAG to RunSpec selections, topological order
        │
        ▼
 chunk_chains        ← split independent calculator chains into parallel chunks
        │
        ▼
 BoundedExecutor     ← run up to --max-parallel-chunks chains concurrently via rayon
  (per chunk)
        │
        ▼
 MasterLoop          ← iterate over (process × state × county × zone × link × year × month × day × hour)
  (per iteration)
        │
        ▼
 InternalControlStrategy.execute  ← priority 1000, runs before calculators
 Calculator / Generator .execute  ← priority 10 / 100
        │
        ▼
 OutputProcessor     ← aggregate and write MOVESRun.parquet
```

### Concurrency model

The `BoundedExecutor` implements the "at most N chunks resident" memory bound:

```
peak_RSS ≈ process_baseline + N × max_chain_working_set
```

`--max-parallel-chunks` is the `N` lever. Each chain holds a semaphore permit
for the full duration of its working-set allocation. The `chunk_chains` function
splits the calculator DAG into weakly-connected components — chains that share
no data dependency — so distinct chunks can interleave freely.

Within a chunk, calculators run in **topological order**: every upstream
producer executes before any downstream consumer.

See `docs/concurrency-tuning.md` for the empirical N-sweep table and
recommended settings.

### Data plane

Calculator bodies read from [`CalculatorContext`]:

```rust
fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
    let tables   = ctx.tables();    // ExecutionTables: per-run filtered default-DB DataFrames
    let scratch  = ctx.scratch();   // ScratchNamespace: inter-calculator intermediate tables
    let position = ctx.position();  // IterationPosition: current loop iteration coordinates
    // ...
}
```

`ExecutionTables` is backed by the `DefaultDb` reader (`moves-data-default`):
a Parquet tree with a `manifest.json` index. The `InputDataManager`
(Task 24) slices the manifest by the RunSpec's active selections before
handing the filtered view to each calculator.

`ScratchNamespace` is an in-memory Polars DataFrame map. Generators write
here; downstream calculators read. Table names in the scratch are the
generator's `output_tables()` declarations.

---

## Calculator-implementation guide

### What is a calculator?

A **calculator** consumes activity data and emission rates, produces
`(pollutant, process)` emission quantities, and registers those pairs in
`CalculatorInfo.txt`. A **generator** produces upstream intermediate data
(operating-mode distributions, fuel-effect multipliers, etc.) that calculators
consume from the scratch namespace. Neither is aware of the iteration loop
structure — the framework calls `execute` once per subscribed granularity
bucket.

Both implement parallel traits in `moves-framework/src/calculator/traits.rs`:
`Calculator` and `Generator`. The difference is:

- Calculators declare `registrations()` — `(pollutant, process)` output pairs.
- Generators declare `output_tables()` — scratch-namespace table names they write.

### Implementing a calculator

There are two common shapes, depending on whether the calculator subscribes
directly to the master loop or runs as a chained (downstream) calculator.

**Chained calculator** — most Phase 3 calculators. No direct master-loop
subscription; runs when the upstream modules it depends on produce output.
`subscriptions()` returns an empty slice. Static arrays work fine for all
metadata because nothing non-const is needed:

```rust
use moves_data::{PollutantId, PollutantProcessAssociation, ProcessId};
use moves_framework::calculator::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription,
};
use moves_framework::error::Error;

#[derive(Debug)]
pub struct MyChainedCalculator;

static NO_SUBS: &[CalculatorSubscription] = &[];

static MY_REGS: &[PollutantProcessAssociation] = &[
    PollutantProcessAssociation { pollutant_id: PollutantId(91), process_id: ProcessId(1) },
];

static MY_UPSTREAM: &[&str] = &["TotalActivityGenerator"];
static MY_INPUTS: &[&str] = &["emissionRateByAge", "sourceUseTypePopulation"];

impl Calculator for MyChainedCalculator {
    fn name(&self) -> &'static str { "MyChainedCalculator" }
    fn subscriptions(&self) -> &[CalculatorSubscription] { NO_SUBS }
    fn registrations(&self) -> &[PollutantProcessAssociation] { MY_REGS }
    fn upstream(&self) -> &[&'static str] { MY_UPSTREAM }
    fn input_tables(&self) -> &[&'static str] { MY_INPUTS }

    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let _tables = ctx.tables();
        let _pos    = ctx.position();
        Ok(CalculatorOutput::empty())
    }
}
```

**Direct-subscriber calculator** — subscribes to the master loop explicitly.
Because `Priority::parse` is not a const function, the subscription is built
at construction time and stored in the struct:

```rust
use moves_calculator_info::{Granularity, Priority};
use moves_data::{PollutantId, PollutantProcessAssociation, ProcessId};
use moves_framework::calculator::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription,
};
use moves_framework::error::Error;

#[derive(Debug)]
pub struct MyDirectCalculator {
    subscriptions: [CalculatorSubscription; 1],
}

static MY_REGS: &[PollutantProcessAssociation] = &[
    PollutantProcessAssociation { pollutant_id: PollutantId(91), process_id: ProcessId(1) },
];

static MY_INPUTS: &[&str] = &["emissionRateByAge", "sourceUseTypePopulation"];

impl MyDirectCalculator {
    pub fn new() -> Self {
        let priority = Priority::parse("EMISSION_CALCULATOR")
            .expect("known-good priority constant");
        Self {
            subscriptions: [CalculatorSubscription::new(
                ProcessId(1),          // Running Exhaust
                Granularity::Hour,
                priority,
            )],
        }
    }
}

impl Calculator for MyDirectCalculator {
    fn name(&self) -> &'static str { "MyDirectCalculator" }
    fn subscriptions(&self) -> &[CalculatorSubscription] { &self.subscriptions }
    fn registrations(&self) -> &[PollutantProcessAssociation] { MY_REGS }
    fn input_tables(&self) -> &[&'static str] { MY_INPUTS }

    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let _tables = ctx.tables();
        let _pos    = ctx.position();
        Ok(CalculatorOutput::empty())
    }
}
```

Key points:

- **`name()`** must exactly match the `ModuleEntry::name` in
  `characterization/calculator-chains/calculator-dag.json`. This is the
  key the registry uses to wire chain edges.
- **`subscriptions()`** maps `(process, granularity, priority)` from the
  corresponding `Subscribe` directives in `CalculatorInfo.txt`. Most
  Phase 3 calculators are chained and return an empty slice.
- **`registrations()`** maps the `Registration` directives. Return an empty
  slice for calculators superseded by a successor (like `CriteriaRunningCalculator`,
  which was replaced by `BaseRateCalculator`).
- **`upstream()`** lists the `name()` of every calculator or generator whose
  output this calculator reads from the scratch namespace. The registry
  validates DAG closure at startup.
- **`input_tables()`** lists canonical default-DB table names this calculator
  reads via `ctx.tables()`. Task 24's `InputDataManager` uses this list for
  lazy loading.
- Calculators are **value types** — they hold no per-run state. The registry
  instantiates one per chain-DAG entry and reuses it across all iterations.

### Registering the calculator

In `crates/moves-calculators/src/lib.rs` (or in the engine wiring), call:

```rust
registry.register_calculator("MyDirectCalculator", || Box::new(MyDirectCalculator::new()))?;
```

The first argument is `name()` from the trait (must match the DAG); the second
is a factory closure. `register_calculator` returns `Result<()>` — it errors if
the name is not found in the DAG.

### Implementing a generator

Generators follow the same pattern but implement `Generator` instead of
`Calculator`. Replace `registrations()` with `output_tables()`:

```rust
impl Generator for MyGenerator {
    fn name(&self) -> &'static str { "MyGenerator" }
    fn subscriptions(&self) -> &[CalculatorSubscription] { MY_GEN_SUBS }
    fn output_tables(&self) -> &[&'static str] { &["myGeneratedTable"] }
    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        // write to ctx.scratch()["myGeneratedTable"]
        Ok(CalculatorOutput::empty())
    }
}
```

Register with `registry.register_generator`.

### Writing characterization tests

The integration-validation gate lives in
`crates/moves-calculators/tests/calculator_integration.rs`. It runs every
onroad fixture through the calculator chain and diffs output against
canonical-MOVES captures stored in `characterization/snapshots/`.

To add coverage for a new calculator:

1. **Confirm fixture coverage.** Check
   `characterization/calculator-validation/README.md` for which fixtures
   exercise which calculators. If no existing fixture hits your new
   `(pollutant, process)` pair, add a minimal RunSpec fixture to
   `characterization/fixtures/` following the shape of existing ones.

2. **Capture the canonical baseline.** Run canonical MOVES (via Apptainer
   and the `fixture-image.lock` SIF) on the new fixture and capture its
   output table:

   ```bash
   characterization/run-all-fixtures.sh --canonical-only --fixture my-new-fixture.xml
   ```

   This writes a snapshot under `characterization/snapshots/`.

3. **Update the tolerance budget.** If the new calculator produces results
   with known numerical divergences (floating-point summation order,
   transcendental-function differences), add an entry in
   `characterization/calculator-validation/tolerance.toml`. Follow the
   structure of existing entries — per-pollutant absolute and relative
   tolerances.

4. **Run the gate:**

   ```bash
   cargo test -p moves-calculators --test calculator_integration
   ```

   The gate reports pass/fail per `(fixture, calculator, pollutant)`.

For **unit tests** within the calculator module itself, write standard
`#[test]` functions in the same file. Keep them pure (no Parquet I/O) and
test individual helper functions — join logic, rate-application math —
not the full `execute` path.

---

## Data-layer extension guide

### DefaultDb structure

The default-DB reader (`moves-data-default`) wraps a Parquet tree
produced by `moves-default-db-convert`. The tree layout is:

```
default-db/movesdb20241112/
├── manifest.json               ← lists every file with partition values
├── SourceUseType.parquet       ← monolithic table
├── emissionRateByAge/          ← model-year-partitioned table
│   ├── modelYear=2010/part.parquet
│   ├── modelYear=2015/part.parquet
│   └── ...
└── IMCoverage/                 ← county-partitioned table
    ├── county=6037/part.parquet
    └── ...
```

Three partition strategies are used:

| Strategy | Directory layout | When |
|----------|-----------------|------|
| `monolithic` | `<table>.parquet` | < 1 M rows, no natural partition axis |
| `county` | `<table>/county=<id>/part.parquet` | county/zone-keyed activity tables |
| `model_year` | `<table>/modelYear=<y>/part.parquet` | model-year-dominated rate tables |
| `year_x_county` | `<table>/year=<y>/county=<id>/part.parquet` | both axes, large table |

### Adding a new default-DB table

1. **Add the table to the conversion pipeline.**
   The `moves-default-db-convert` crate reads `CreateDefault.sql` (or
   `CreateNRDefault.sql` for NONROAD tables) and writes Parquet. If the new
   table already exists in the source SQL but was omitted from the manifest,
   add it to `characterization/default-db-schema/tables.json` with the
   appropriate partition strategy.

2. **Regenerate the manifest:**

   ```bash
   cargo run -p moves-default-db-convert -- \
     --mariadb-dump /path/to/movesdb20241112.sql \
     --output default-db/movesdb20241112
   ```

3. **Expose it via `DefaultDb::scan`:**

   ```rust
   let lf = db.scan("MyNewTable", &TableFilter::new()
       .partition_eq("countyID", 6037i64))?;
   let df = lf.collect()?;
   ```

   `TableFilter` drives partition pruning. Pass `.partition_eq("countyID", N)`
   for county-partitioned tables; for monolithic tables a bare
   `TableFilter::new()` is fine.

4. **Add a typed accessor (optional).**
   For high-traffic lookup tables, add a typed wrapper in
   `crates/moves-data-default/src/typed.rs` following the
   `DefaultDb::source_use_type` pattern:

   ```rust
   pub mod my_new_table {
       pub const MY_ID_COL: &str = "myID";
   }

   impl DefaultDb {
       pub fn my_new_table(&self) -> Result<DataFrame> {
           let lf = self.scan("MyNewTable", &TableFilter::new())?;
           Ok(lf.collect()?)
       }
   }
   ```

5. **Declare it in `input_tables()`.**
   Any calculator that reads the new table must list it in
   `Calculator::input_tables()` so `InputDataManager` (Task 24) loads it.

### Schema-only tables

Four tables (`Link`, `SHO`, `SourceHours`, `Starts`) ship empty in the
default DB — they are populated per-run. Calling `DefaultDb::scan` on them
returns `Error::SchemaOnly`. Use `DefaultDb::schema_sidecar` to obtain the
column types for building an empty DataFrame with the correct schema.

---

## Control-strategy authoring guide

### What is a control strategy?

Internal control strategies modify default-DB input tables before emission
calculators see them. They run at priority 1000 (`INTERNAL_CONTROL_STRATEGY`),
which fires before generators (100) and calculators (10) in every granularity
bucket. Each strategy declares which tables it modifies via `modified_tables()`
so the `InputDataManager` can invalidate and reload those tables after
`pre_run` completes.

### Lifecycle

The engine calls three lifecycle hooks in order:

| Hook | Thread | When |
|------|--------|------|
| `pre_run` | Single | Once before the first MasterLoop iteration — use for global table transforms |
| `execute` | Parallel | Once per subscribed master-loop iteration — use for per-location/time modifications |
| `post_run` | Single | Once after all iterations — use for cleanup |

`pre_run` and `post_run` run outside the parallel-chunk section.
`execute` can be called concurrently; any shared mutable state must use
interior mutability with appropriate synchronisation (`Mutex`, `RwLock`,
or atomics).

### Implementation pattern

Create a new crate (e.g. `crates/moves-my-strategy/`) following the shape
of `moves-avft` or `moves-onroad-retrofit`:

```rust
use moves_framework::{CalculatorContext, InternalControlStrategy};
use moves_framework::control_strategy::StrategySubscription;
use moves_calculator_info::Granularity;
use moves_data::ProcessId;

#[derive(Debug)]
pub struct MyControlStrategy {
    // completed input data loaded once at construction time
}

static MY_STRATEGY_SUBS: &[StrategySubscription] = &[
    StrategySubscription {
        process_id: ProcessId(1),
        granularity: Granularity::County,
        priority_offset: 0,
    },
];

static MY_MODIFIED_TABLES: &[&str] = &["myTargetTable"];

impl InternalControlStrategy for MyControlStrategy {
    fn name(&self) -> &'static str { "MyControlStrategy" }

    fn subscriptions(&self) -> &[StrategySubscription] {
        MY_STRATEGY_SUBS
    }

    fn modified_tables(&self) -> &[&'static str] {
        MY_MODIFIED_TABLES
    }

    fn pre_run(&self, ctx: &CalculatorContext) -> Result<(), Error> {
        // Apply global table modifications here.
        // ctx.tables() contains the current execution tables.
        Ok(())
    }

    fn execute(&self, ctx: &CalculatorContext) -> Result<(), Error> {
        // Apply per-location/time modifications here.
        // ctx.position() gives the current (process, county, year, hour).
        Ok(())
    }
}
```

Key points:

- **`modified_tables()`** is the integration contract with `InputDataManager`.
  List every table your strategy writes or modifies in `pre_run` or `execute`.
- For strategies that only use `pre_run` (like AVFT, which applies a global
  fleet-composition replacement), leave `subscriptions()` returning an empty
  slice — no per-iteration `execute` call is needed.
- Strategies that need per-location modifications (like OnRoadRetrofit, which
  applies per-county reduction factors) register a `County`-granularity
  subscription.

### Registering the strategy

In the engine wiring, call:

```rust
registry.register(|| Box::new(MyControlStrategy::new()));
```

The `ControlStrategyRegistry` (in `moves-framework/src/control_strategy/registry.rs`)
instantiates strategies in registration order. Order matters only when two
strategies write the same table; use `priority_offset` on the subscription
to control ordering within the `INTERNAL_CONTROL_STRATEGY` band.

### Testing

Write unit tests in the strategy crate for the transformation logic in
isolation. For integration tests, add a RunSpec fixture that exercises the
strategy and verify the output table modifications through the calculator
output. See the control-strategy validation section of the characterization
suite (`crates/moves-calculators/tests/`) for examples.

---

## NONROAD module guide

### Overview

`moves-nonroad` is a pure-Rust port of EPA's NONROAD2008a Fortran model
(118 `.f` files, ~29.4k lines, plus 11 `.inc` files defining 65 named
COMMON blocks). The crate exposes a single in-process entry point:

```rust
pub fn run_simulation(
    opts: &NonroadOptions,
    inputs: &NonroadInputs,
) -> Result<NonroadOutputs>;
```

No subprocess, no scratch files, no MariaDB ingestion. The output
joins the unified Parquet schema from Phase 4 Task 89.

The full Fortran-to-module map lives in
`crates/moves-nonroad/ARCHITECTURE.md`. This section summarizes the
key design decisions a contributor needs.

### Module structure

| Module | Fortran source cluster | Role |
|--------|----------------------|------|
| `common` | 11 `.inc` files, 65 COMMON blocks | Shared context replacing Fortran global state |
| `driver` | `nonroad.f`, `dayloop.f`, `daymthf.f`, `dispit.f` | Top-level SCC-group × geography × year loop |
| `geography` | `prccty.f`, `prcsta.f`, + 4 more | Spatial-allocation routines for county/state/national/project scales |
| `population` | 5 `.f` files | Population growth and age-distribution logic |
| `emissions` | `clc*.f` and related | Emission-factor application per SCC × pollutant × model year |
| `allocation` | `alo*.f` | Spatial apportionment and summing |
| `input` | ~30 `rd*.f` files | Fixed-width input-file parsers |
| `output` | ~50 `wrt*.f`, `fnd*.f`, and helpers | Output writers and small helpers |

### COMMON-block replacement (`common`)

The Fortran model uses 65 named COMMON blocks as global mutable state.
The Rust port replaces all of them with a single `NonroadContext` struct
that is threaded explicitly through all function calls:

```rust
pub struct NonroadContext {
    // fields corresponding to the 65 COMMON blocks
}
```

**Never add new global or static mutable state.** Every piece of
per-run state must live in `NonroadContext` or in a local variable.
This is what makes in-process concurrency and WASM compatibility possible.

### Adding or modifying a NONROAD component

1. **Identify the Fortran source.** The rustdoc in each module lists the
   Fortran filenames it ports. Start by reading the Fortran to understand
   the algorithm, then look at the existing Rust for the transcription
   pattern.

2. **Numerical fidelity.** NONROAD has 30+ years of accumulated numerical
   conventions. Where Fortran and Rust differ (transcendental functions,
   floating-point summation order, integer overflow semantics), the Rust
   port must match the Fortran output within the tolerance budget in
   `characterization/tolerance.toml`. The NONROAD tolerance budget is
   deliberately wider than the onroad budget because of documented
   compiler-dependent behavior in the Fortran (see the gfortran-on-Linux
   warning in the migration plan).

3. **Array sizes.** The Fortran uses fixed-size arrays declared in COMMON
   blocks (e.g. `REAL EMFAC(MXPOL, MXTECH, MXSCC)`). In Rust these are
   Vecs or arrays on `NonroadContext`. Use the same symbolic size constants
   as the Fortran where the size has a documented meaning; use runtime-sized
   Vecs where the Fortran uses a compile-time upper bound for safety.

4. **No platform-specific I/O.** All I/O paths must work on Linux, macOS,
   Windows, and `wasm32-unknown-unknown`. Use `std::io::Read`/`Write`
   abstractions rather than `std::fs::File` directly. The `input` and
   `output` modules use this pattern throughout.

5. **Test against the canonical reference.** The NONROAD characterization
   gate lives in `crates/moves-nonroad/tests/nonroad_fidelity.rs` and the
   fixtures in `characterization/fixtures/nr-*.xml`. Run:

   ```bash
   cargo test -p moves-nonroad --test nonroad_fidelity -- --nocapture
   ```

   The canonical reference is the Windows-compiled `nonroad.exe` run via
   the `characterization/nonroad-build/` Apptainer image. Any numerical
   divergence beyond the tolerance in `characterization/tolerance.toml`
   must be fixed or explicitly added to `docs/known-divergences.md` with
   a note explaining why the divergence is acceptable.

### WASM constraints

The NONROAD port targets `wasm32-unknown-unknown`. This imposes two
constraints that are harder to work around than on native:

- **No Fortran FFI.** The FFI escape hatch available on native Linux (to
  call `gfortran`-compiled code for a handful of hard-to-port routines)
  is unavailable in WASM. Every NONROAD subroutine must be ported in pure
  Rust.
- **Single-threaded by default.** Until Task 134 (WASM multi-threading
  via the threads proposal), `run_simulation` must be callable from a
  single-threaded WASM context. Do not add unconditional `rayon` parallelism
  inside `moves-nonroad`; use `#[cfg(not(target_arch = "wasm32"))]` guards
  or the `rayon`-optional pattern if parallelism is needed for native
  performance.

---

## Quick-reference

```bash
# Build everything
cargo build --workspace

# Run all tests
cargo test --workspace

# Run the onroad calculator integration gate
cargo test -p moves-calculators --test calculator_integration

# Run the NONROAD fidelity gate (requires Apptainer)
cargo test -p moves-nonroad --test nonroad_fidelity

# Run the full fixture suite (requires Apptainer + canonical MOVES SIF)
characterization/run-all-fixtures.sh

# Performance baseline (onroad fixtures, --max-parallel-chunks 1)
cargo test -p moves-cli --test perf_baseline -- --nocapture
```

See `docs/performance-baseline.md` for the Phase 3 numbers and methodology,
and `docs/concurrency-tuning.md` for the N-sweep table.
